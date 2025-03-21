
import os
from uuid import uuid4
from datetime import datetime
import hashlib
import json
import time

from bson.objectid import ObjectId
from flask_login import UserMixin
from pymongo import ReturnDocument
from toposort import toposort, CircularDependencyError

from werkzeug.security import generate_password_hash

import logging
log = logging.getLogger(__name__)


class DependencyException(Exception):
    pass


class MalformedFieldException(Exception):
    pass


class Field(object):
    PRIMITIVES = ['int', 'str', 'float', 'bool', 'datetime']

    def __init__(self, name, field_type, target_spec_name=None, reverse_link_field=None, default=None, required=None, indexed=None, unique=None, unique_global=None):
        self.name = name
        self.field_type = field_type
        self.default = default
        self.required = required or False
        self.indexed = indexed or False
        self.unique = unique or False
        self.unique_global = unique_global or False
        self.target_spec_name = target_spec_name
        self.reverse_link_field = reverse_link_field  # only used for reverse links
        self._comparable_types= {
            'str': ['str', 'NoneType'],
            'int': ['float', 'int', 'NoneType'],
            'float': ['float', 'int', 'NoneType'],
            'bool': ['bool', 'NoneType'],
            'datetime': ['datetime'],
        }
        self.spec = None
        self.mutation_id = None

    def is_primitive(self):
        return self.field_type in Field.PRIMITIVES

    def is_collection(self):
        return self.field_type in ['collection', 'orderedcollection', 'linkcollection', 'reverse_link', 'reverse_link_collection',]

    def is_field(self):
        return True

    def is_reverse(self):
        return self.reverse_link_field is not None

    def check_comparable_type(self, ctype):
        return ctype in self._comparable_types.get(self.field_type, [])

    def __repr__(self):
        return "<Field %s %s %s>" % (self.name, self.field_type, self.target_spec_name or '')


class CalcField(Field):
    def __init__(self, field_name, calc_str, background, indexed=False, unique=False, unique_global=False):
        super().__init__(field_name, 'calc', indexed=indexed, unique=unique, unique_global=unique_global)
        self.name = field_name
        self.calc_str = calc_str
        self.field_type = 'calc'
        self.spec = None
        self.default = None
        self.background = background

    def __repr__(self):
        return "<Calc %s = %s>" % (self.name, self.calc_str)

    def infer_type(self):
        calc_tree = self.spec.schema.calc_trees[self.spec.name, self.name]
        return calc_tree.infer_type()

    def get_resource_dependencies(self):
        calc_tree = self.spec.schema.calc_trees[self.spec.name, self.name]
        return calc_tree.get_resource_dependencies()

    def check_comparable_type(self, ctype):
        return ctype in self._comparable_types.get(self.infer_type().field_type, [])

    def is_primitive(self):
        return self.infer_type().is_primitive()

    def is_collection(self):
        calc_tree = self.spec.schema.calc_trees[self.spec.name, self.name]
        return calc_tree.is_collection()


class Spec(object):
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self.fields = {}

    @property
    def field_type(self):
        return self.name

    def __repr__(self):
        return "<Spec %s>" % (self.name,)

    def build_child_spec(self, name):
        if name not in self.fields:
            raise SyntaxError("No such field %s in %s" % (name, self.name))
        if self.fields[name].is_primitive():
            return self.fields[name]
        elif self.fields[name].field_type in ('link', 'reverse_link', 'parent_collection', 'collection', 'linkcollection', 'reverse_link_collection', 'orderedcollection'):
            return self.schema.specs[self.fields[name].target_spec_name]
        elif self.fields[name].field_type == 'calc':
            from metaphor.lrparse.lrparse import parse
            field = self.fields[name]
            tree = parse(field.calc_str, self)
            return tree.infer_type()
        else:
            raise SyntaxError('Unrecognised field type')

    def is_collection(self):
        # specs map to resources and are not collections
        return False

    def is_primitive(self):
        return False

    def is_field(self):
        return False

    def linked_from(self):
        parents = []
        for ref_spec_name, ref_spec in self.schema.specs.items():
            if ref_spec_name != self.name:
                for ref_field_name, ref_field in ref_spec.fields.items():
                    if ref_field.target_spec_name == self.name:
                        parents.append("%s.%s" % (ref_spec_name, ref_field_name))
        return parents


class User:
    def __init__(self, user_id, grants, admin=False):
        self.user_id = user_id
        self.grants = grants
        self.admin = admin

    def is_admin(self):
        return self.admin

    def __repr__(self):
        return "<User %s>" % self.user_id


class Identity(UserMixin):
    def __init__(self, identity_id, user_id, identity_type, email, name, profile_url, session_id=None, password=None):
        self.identity_id = identity_id
        self.user_id = user_id
        self.identity_type = identity_type
        self.email = email
        self.name = name
        self.profile_url = profile_url
        self.session_id = session_id
        self.password = password

    def get_id(self):
        return self.session_id

    @staticmethod
    def from_data(identity_data):
        return Identity(
            identity_data["_id"],
            identity_data["user_id"],
            identity_data["type"],
            identity_data["email"],
            identity_data["name"],
            identity_data["profile_url"],
            identity_data.get("session_id"),
            identity_data.get("password"))


class Schema(object):
    def __init__(self, db):
        self.db = db

        self.name = ""
        self.description = ""

        self.specs = {}
        self.groups = {}
        self.root = Spec('root', self)
        self._id = None
        self.current = None
        self.calc_trees = {}

    @property
    def schema_id(self):
        return str(self._id)

    @staticmethod
    def create_schema(db):
        inserted = db.metaphor_schema.insert_one({"specs": {}, "root": {}, "groups": {}, "created": datetime.now().isoformat()})
        schema = Schema(db)
        schema._id = inserted.inserted_id
        schema.update_version()
        return schema

    def get_spec(self, spec_name):
        return self.root if spec_name == 'root' else self.specs[spec_name]

    def update_version(self):
        self.db.metaphor_schema.find_one_and_update({'_id': self._id}, {"$set": {"version": self.calculate_short_hash()}})

    def calculate_short_hash(self):
        schema_data = self._load_schema_data()
        schema_data = {
            "specs": schema_data["specs"],
            "root": schema_data["root"],
            "groups": schema_data["groups"],
        }
        schema_str = json.dumps(schema_data, sort_keys=True)
        return hashlib.sha1(schema_str.encode("UTF-8")).hexdigest()[:8]

    def _load_schema_data(self):
        return self.db.metaphor_schema.find_one({"_id": self._id})

    def _build_specs(self, schema_data):
        for spec_name, spec_data in schema_data['specs'].items():
            spec = self.add_spec(spec_name)
            for field_name, field_data in spec_data['fields'].items():
                if field_data['type'] != 'calc':
                    self._add_field(spec,
                        field_name,
                        field_data['type'],
                        target_spec_name=field_data.get('target_spec_name'),
                        default=field_data.get('default'),
                        required=field_data.get('required'),
                        indexed=field_data.get('indexed'),
                        unique=field_data.get('unique'),
                        unique_global=field_data.get('unique_global'),
                    )
        self._add_reverse_links()

    def _collect_calcs(self, schema_data):
        calcs = {}
        for spec_name, spec_data in schema_data['specs'].items():
            for field_name, field_data in spec_data['fields'].items():
                if field_data['type'] == 'calc':
                    calcs[spec_name + '.' + field_name] = field_data
        return calcs

    def set_as_current(self):
        # we'll transact this later
        self.db.metaphor_schema.update_one({"current": True}, {"$set": {"current": False}})
        self.db.metaphor_schema.update_one({"_id": self._id}, {"$set": {"current": True}})
        self.current = True

    def _build_schema(self, schema_data):
        from metaphor.lrparse.lrparse import parse

        self._id = schema_data.get('_id') or ObjectId(schema_data['id'])
        self.name = schema_data.get('name', '')
        self.description = schema_data.get('description', '')
        self.current = schema_data.get('current', False)
        self._build_specs(schema_data)

        calcs = self._collect_calcs(schema_data)
        calc_deps = dict((key, set(calc.get('deps', {}))) for key, calc in calcs.items())

        sorted_calcs = list(toposort(calc_deps))

        for root_name, root_data in schema_data.get('root', {}).items():
            self._add_field(
                self.root,
                root_name,
                root_data['type'],
                target_spec_name=root_data.get('target_spec_name'),
                default=root_data.get('default'),
                required=root_data.get('required'),
            )

        for line in sorted_calcs:
            for field_str in line:
                spec_name, field_name = field_str.split('.')

                if spec_name == 'root':
                    field_data = schema_data['root'][field_name]
                else:
                    field_data = schema_data['specs'][spec_name]['fields'][field_name]
                if field_data['type'] == 'calc':
                    spec = self.specs[spec_name]
                    self._add_calc(spec, field_name, field_data['calc_str'])
                    self.calc_trees[(spec.name, field_name)] = parse(field_data['calc_str'], spec)

        self.version = schema_data['version']
        self.name = self.name or self.version
        self.groups = schema_data['groups']
        self.mutation_id = schema_data.get('mutation_id')

    def create_spec(self, spec_name):
        if spec_name in self.specs:
            raise Exception("Spec already exists: %s" % spec_name)
        self.db['metaphor_schema'].update_one(
            {'_id': self._id},
            {"$set": {'specs.%s' % spec_name: {'fields': {}}}})
        self.update_version()
        return self.add_spec(spec_name)

    def create_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None, default=None, required=None, background=None, indexed=None, unique=None, unique_global=None):
        if spec_name != 'root' and field_name in self.specs[spec_name].fields:
            raise MalformedFieldException('Field already exists: %s.%s' % (spec_name, field_name))
        if spec_name == 'root' and field_name in self.root.fields:
            raise MalformedFieldException('Field already exists: root.%s' % field_name)
        if (unique_global and not unique) or (unique and not indexed):
            raise MalformedFieldException('Index specified incorrectly')
        self._check_field_name(field_name)
        self._update_field(spec_name, field_name, field_type, field_target, calc_str, default, required, indexed, unique, unique_global)
        if spec_name == 'root':
            spec = self.root
        else:
            spec = self.specs[spec_name]
        if field_type == 'calc':
            self.add_calc(spec, field_name, calc_str, background, indexed, unique, unique_global)
        else:
            self.add_field(spec, field_name, field_type, field_target, default, required, indexed, unique, unique_global)

    def update_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None, default=None, required=None, indexed=None, unique=None, unique_global=None):
        if spec_name != 'root' and field_name not in self.specs[spec_name].fields:
            raise MalformedFieldException('Field does not exist: %s' % field_name)
        self._update_field(spec_name, field_name, field_type, field_target, calc_str, default, required, indexed, unique, unique_global)

    def _update_field(self, spec_name, field_name, field_type, field_target, calc_str, default, required, indexed, unique, unique_global):
        if calc_str:
            self._check_calc_syntax(spec_name, calc_str)
            self._check_circular_dependencies(spec_name, field_name, calc_str)
            self._check_indexes_for_calc(spec_name, calc_str, indexed, unique, unique_global)

            from metaphor.lrparse.lrparse import parse
            parsed = parse(calc_str, self.specs[spec_name])
            deps = list(parsed.get_resource_dependencies())
            deps.sort()
            field_data = {'type': 'calc', 'calc_str': calc_str, 'deps': deps}
        elif field_type in ('int', 'str', 'float', 'bool'):
            field_data = {'type': field_type}
            field_data['default'] = default
            field_data['required'] = required
            field_data['indexed'] = indexed
            field_data['unique'] = unique
            field_data['unique_global'] = unique_global
        else:
            field_data = {'type': field_type, 'target_spec_name': field_target}

        if spec_name == 'root':
            self.db['metaphor_schema'].update_one(
                {'_id': self._id},
                {"$set": {'root.%s' % (field_name,): field_data}},
                upsert=True)
            self.update_version()
        else:
            self.db['metaphor_schema'].update_one(
                {'_id': self._id},
                {"$set": {'specs.%s.fields.%s' % (spec_name, field_name): field_data}},
                upsert=True)
            self.update_version()

    def _check_field_name(self, field_name):
        if not field_name:
            raise MalformedFieldException('Field name cannot be blank')
        for start in ('link_', 'parent_', '_'):
            if field_name.startswith(start):
                raise MalformedFieldException('Field name cannot begin with "%s"' % (start,))
        if field_name in ('self', 'id'):
            raise MalformedFieldException('Field name cannot be reserverd word "%s"' % (field_name,))
        if not field_name[0].isalpha():
            raise MalformedFieldException('First character must be letter')

    def _check_calc_syntax(self, spec_name, calc_str):
        try:
            from metaphor.lrparse.lrparse import parse
            spec = self.specs[spec_name]
            tree = parse(calc_str, spec)
        except SyntaxError as se:
            raise MalformedFieldException('SyntaxError in calc: %s' % str(se))

    def _check_circular_dependencies(self, spec_name, field_name, calc_str):
        from metaphor.lrparse.lrparse import parse
        spec = self.specs[spec_name]
        tree = parse(calc_str, spec)
        deps = tree.get_resource_dependencies()
        dep_tree = {}
        for name, spec in self.specs.items():
            for fname, field in spec.fields.items():
                if field.field_type == 'calc':
                    calc = self.calc_trees[(name, fname)]
                    dep_tree["%s.%s" % (name, fname)] = calc.get_resource_dependencies()
        dep_tree["%s.%s" % (spec_name, field_name)] = deps
        try:
            list(toposort(dep_tree))
        except CircularDependencyError as cde:
            raise MalformedFieldException('%s.%s has circular dependencies: %s' % (spec_name, field_name, cde.data))

    def _check_indexes_for_calc(self, spec_name, calc_str, indexed, unique, unique_global):
        from metaphor.lrparse.lrparse import parse
        spec = self.specs[spec_name]
        tree = parse(calc_str, spec)
        if indexed and not tree.infer_type().is_primitive():
            raise MalformedFieldException("Index not allowed for non-primitive calc field")
        if tree.infer_type().is_primitive() and (unique or unique_global):
            raise MalformedFieldException("Unique index not allowed for calc field")

    def add_spec(self, spec_name):
        spec = Spec(spec_name, self)
        self.specs[spec_name] = spec
        return spec

    def delete_field(self, spec_name, field_name):
        self._check_field_dependencies(spec_name, field_name)

        if spec_name == 'root':
            self._delete_root_field(field_name)
        else:
            self._delete_spec_field(spec_name, field_name)
        self.update_version()

    def create_index_for_spec(self, spec_name):
        self.db['resource_%s' % spec_name].create_index(
            ["_parent_id", "_parent_field_name"],
            name=f"specindex-{spec_name}")

    def create_index_for_field(self, spec_name, field_name, unique=False, unique_global=False):
        if unique_global:
            self.db['resource_%s' % spec_name].create_index(
                [field_name],
                unique=unique,
                name=f"field-global-{spec_name}-{field_name}")
        elif unique:
            self.db['resource_%s' % spec_name].create_index(
                ["_parent_id", "_parent_field_name", field_name],
                unique=unique,
                name=f"field-unique-{spec_name}-{field_name}")
        else:
            self.db['resource_%s' % spec_name].create_index(
                [field_name],
                name=f"field-index-{spec_name}-{field_name}")

    def drop_index_for_field(self, index_type, spec_name, field_name):
        index_name = f"field-{index_type}-{spec_name}-{field_name}"
        if index_name in self.db['resource_%s' % spec_name].index_information():
            self.db['resource_%s' % spec_name].drop_index(index_name)

    def _delete_root_field(self, field_name):
        self.root.fields.pop(field_name)
        dep = ('root', field_name)
        if dep in self.calc_trees:
            self.calc_trees.pop(dep)

        self.db['metaphor_schema'].update_one(
            {'_id': self._id},
            {"$unset": {'root.%s' % (field_name,): ''}})

    def _delete_spec_field(self, spec_name, field_name):
        self._do_delete_field(spec_name, field_name)

        self.db['metaphor_schema'].update_one(
            {'_id': self._id},
            {"$unset": {'specs.%s.fields.%s' % (spec_name, field_name): ''}})

    def rename_field(self, spec_name, from_field_name, to_field_name):
        spec = self.get_spec(spec_name)
        field = spec.fields[from_field_name]
        if field.field_type == 'collection':
            self.db['resource_%s' % field.target_spec_name].update_many({"_type": field.target_spec_name, "_parent_field_name": from_field_name}, {"$set": {"_parent_field_name": to_field_name}})
        else:
            self.db['resource_%s' % spec_name].update_many({"_type": spec_name}, {"$rename": {from_field_name: to_field_name}})
        if spec_name == 'root':
            self.db['metaphor_schema'].update_one(
                {'_id': self._id},
                {'$rename': {f'root.{from_field_name}': f'root.{to_field_name}'}},
            )
        else:
            self.db['metaphor_schema'].update_one(
                {'_id': self._id},
                {'$rename': {f'specs.{spec_name}.{from_field_name}': f'specs.{spec_name}.{to_field_name}'}},
            )
        spec.fields[to_field_name] = spec.fields[from_field_name]
        spec.fields.pop(from_field_name)

    def rename_spec(self, from_spec_name, to_spec_name):
        # TODO: do for each child:
        for spec_name in self.specs:
            self.db['resource_%s' % spec_name].update_many({"_parent_type": from_spec_name}, {"$set": {"_parent_type": to_spec_name}})
        self.db['metaphor_schema'].update_one(
            {'_id': self._id},
            {'$rename': {f'specs.{from_spec_name}': f'specs.{to_spec_name}'}},
        )
        for spec_name, spec in self.specs.items():
            for field_name, field in spec.fields.items():
                if field.field_type in ['collection', 'link_collection']:
                    self.db['metaphor_schema'].update_one(
                        {'_id': self._id},
                        {"$set": {f"specs.{spec_name}.fields.{field_name}.target_spec_name": to_spec_name}}
                    )
        if 'resource_%s' % from_spec_name in self.db.list_collection_names():
            self.db['resource_%s' % from_spec_name].rename("resource_%s" % to_spec_name)

    def _do_delete_field(self, spec_name, field_name):
        spec = self.specs[spec_name]
        field = spec.fields.pop(field_name)
        self._remove_reverse_link_for_field(field, spec)
        dep = (spec_name, field_name)
        if dep in self.calc_trees:
            self.calc_trees.pop(dep)

    def delete_spec(self, spec_name):
        spec = self.specs[spec_name]

        for field_name in spec.fields:
            self._check_field_dependencies(spec_name, field_name)

        links = spec.linked_from()
        if links:
            raise DependencyException("%s is linked from %s" % (spec_name, ", ".join(links)))

        for field_name in list(spec.fields):
            self._do_delete_field(spec_name, field_name)
        self.specs.pop(spec_name)

        self.db['metaphor_schema'].update_one(
            {'_id': self._id},
            {"$unset": {'specs.%s' % (spec_name,): ''}})
        self.update_version()

    def _check_field_dependencies(self, spec_name, field_name):
        all_deps = self._get_field_dependencies(spec_name, field_name)
        if all_deps:
            raise DependencyException('%s.%s referenced by %s' % (spec_name, field_name, all_deps))

    def _get_field_dependencies(self, spec_name, field_name):
        all_deps = []
        for name, spec in self.specs.items():
            for fname, field in spec.fields.items():
                if field.field_type == 'calc':
                    calc = self.calc_trees[(name, fname)]
                    if "%s.%s" % (spec_name, field_name) in calc.get_resource_dependencies():
                        all_deps.append('%s.%s' % (name, fname))
        return all_deps

    def _add_field(self, spec, field_name, field_type, target_spec_name=None, default=None, required=None, indexed=None, unique=None, unique_global=None):
        field = Field(field_name, field_type, target_spec_name=target_spec_name, default=default, required=required, indexed=indexed, unique=unique, unique_global=unique_global)
        spec.fields[field_name] = field
        field.spec = spec
        return field

    def add_field(self, spec, field_name, field_type, target_spec_name=None, default=None, required=None, indexed=None, unique=None, unique_global=None):
        field = self._add_field(spec, field_name, field_type, target_spec_name, default, required, indexed, unique, unique_global)
        self._add_reverse_link_for_field(field, spec)
        return field

    def add_calc(self, spec, field_name, calc_str, background=False, indexed=False, unique=False, unique_global=False):
        from metaphor.lrparse.lrparse import parse
        calc_field = self._add_calc(spec, field_name, calc_str, background, indexed, unique, unique_global)
        self.calc_trees[(spec.name, field_name)] = parse(calc_str, spec)
        return calc_field

    def _add_calc(self, spec, field_name, calc_str, background=False, indexed=False, unique=False, unique_global=False):
        calc_field = CalcField(field_name, calc_str=calc_str, background=background, indexed=indexed, unique=unique, unique_global=unique_global)
        spec.fields[field_name] = calc_field
        spec.fields[field_name].spec = spec
        return calc_field

    def _add_reverse_links(self):
        for spec in self.specs.values():
            for field in list(spec.fields.values()):
                self._add_reverse_link_for_field(field, spec)

    def _add_reverse_link_for_field(self, field, spec):
        if spec.name == 'root':
            return
        if field.field_type == 'link':
            reverse_field_name = "link_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields[reverse_field_name] = Field(reverse_field_name, "reverse_link", spec.name, field.name)
        if field.field_type in ['collection', 'orderedcollection']:
            parent_field_name = "parent_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields[parent_field_name] = Field(parent_field_name, "parent_collection", spec.name, field.name)
        if field.field_type == 'linkcollection':
            parent_field_name = "link_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields[parent_field_name] = Field(parent_field_name, "reverse_link_collection", spec.name, field.name)

    def _remove_reverse_link_for_field(self, field, spec):
        if spec.name == 'root':
            return
        if field.field_type == 'link':
            reverse_field_name = "link_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields.pop(reverse_field_name)
        if field.field_type in ['collection', 'orderedcollection']:
            parent_field_name = "parent_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields.pop(parent_field_name)
        if field.field_type == 'linkcollection':
            parent_field_name = "link_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields.pop(parent_field_name)

    def encodeid(self, mongo_id):
        return "ID" + str(mongo_id)

    def decodeid(self, str_id):
        return ObjectId(str_id[2:])

    def load_parent_data(self, parent_type, parent_id):
        return self.db['resource_%s' % parent_type].find_one({'_id': self.decodeid(parent_id)})

    def check_field_types(self, spec_name, data):
        pass

    def _parse_fields(self, spec_name, resource_data):
        parsed_data = {}
        spec = self.specs[spec_name]
        for field_name, field_value in resource_data.items():
            field = spec.fields[field_name]
            if field.field_type == 'link' and field_value is not None:
                parsed_data[field_name] = self.decodeid(field_value)
            elif field.field_type == 'linkcollection' and field_value is not None:
                raise Exception("Do this")
            elif field.field_type == 'datetime' and field_value is not None:
                parsed_data[field_name] = datetime.fromisoformat(field_value.replace('Z', '+00:00'))
            else:
                parsed_data[field_name] = field_value
        for field_name, field in spec.fields.items():
            if field.default is not None and field_name not in resource_data:
                parsed_data[field_name] = field.default
        return parsed_data

    def _fields_with_dependant_calcs(self, spec_name):
        fields = set()
        spec_fields = set([
            '%s.%s' % (spec_name, field_name)
            for field_name in self.specs[spec_name].fields])
        for (spec_name, field_name), calc in self.calc_trees.items():
            deps = calc.get_resource_dependencies()
            fields = fields.union(spec_fields.intersection(deps))
        return [dep.split('.')[1] for dep in fields]

    def all_dependent_calcs_for(self, spec_name, field_names):
        calcs = self._dependent_calcs_for_resource(spec_name)
        for field_name in field_names:
            calcs.update(self._dependent_calcs_for_field(spec_name, field_name))
        return calcs

    def _dependent_calcs_for_field(self, spec_name, field_name):
        calcs = {}
        for (calc_spec_name, calc_field_name), calc in self.calc_trees.items():
            if "%s.%s" % (spec_name, field_name) in calc.get_resource_dependencies():
                calcs[(calc_spec_name, calc_field_name)] = calc
        return calcs

    def _dependent_calcs_for_resource(self, spec_name):
        calcs = {}
        for (calc_spec_name, calc_field_name), calc in self.calc_trees.items():
            for dep in calc.get_resource_dependencies():
                sname, fname = dep.split('.')
                if sname == 'root':
                    if self.root.fields[fname].target_spec_name == spec_name:
                        calcs[(calc_spec_name, calc_field_name)] = calc
                elif self.specs[sname].fields[fname].target_spec_name == spec_name:
                    calcs[(calc_spec_name, calc_field_name)] = calc
        return calcs

    def create_update(self):
        return self.db['metaphor_updates'].insert_one({}).inserted_id

    def cleanup_update(self, update_id):
        # remove dirty flags
        for spec_name in self.specs:
            self.db["resource_%s" % spec_name].update_many(
                {"_dirty.%s" % update_id: {"$exists": True}},
                {"$unset": {"_dirty.%s" % update_id: ""}}
            )
        return self.db['metaphor_updates'].delete_one({"_id": ObjectId(update_id)})

    def update_error(self, update_id, message):
        self.db["metaphor_updates"].update_one(
            {"_id": ObjectId(update_id)},
            {"$set": {"error": message}})

    def insert_resource(self, spec_name, data, parent_field_name, parent_type=None, parent_id=None, extra_fields=None):
        data = self._parse_fields(spec_name, data)

        data['_type'] = spec_name
        data['_parent_type'] = parent_type or 'root'
        data['_parent_id'] = self.decodeid(parent_id) if parent_id else None
        data['_parent_field_name'] = parent_field_name

        if extra_fields:
            data.update(extra_fields)

        insert_result = self.db['resource_%s' % spec_name].insert_one(data)
        return self.encodeid(insert_result.inserted_id)

    def save_details(self):
        self.db['metaphor_schema'].update_one({"_id": self._id}, {"$set": {"name": self.name, "description": self.description}})

    def mark_resource_deleted(self, spec_name, resource_id):
        return self.db['resource_%s' % spec_name].find_one_and_update({'_id': self.decodeid(resource_id)}, {"$set": {"_deleted": True}})

    def delete_resource(self, spec_name, resource_id):
        return self.db['resource_%s' % spec_name].find_one_and_delete({'_id': self.decodeid(resource_id)})

    def delete_resources_of_type(self, spec_name):
        return self.db['resource_%s' % spec_name].delete_many({'_type': spec_name})

    def mark_link_collection_item_deleted(self, spec_name, parent_id, field_name, resource_id):
        self.db['resource_%s' % spec_name].update_one({
            "_id": parent_id,
            field_name: {"$elemMatch": {"_id": self.decodeid(resource_id)}},
        }, {
            "$set": {"%s.$._deleted" % field_name: True
        }})

    def delete_linkcollection_entry(self, spec_name, parent_id, field_name, resource_id):
        self.db['resource_%s' % spec_name].update_one({"_id": parent_id}, {"$pull": {field_name: {"_id": self.decodeid(resource_id)}}})

    def update_resource_fields(self, spec_name, resource_id, field_data):
        save_data = self._parse_fields(spec_name, field_data)

        new_resource = self.db['resource_%s' % spec_name].find_one_and_update(
            {"_id": self.decodeid(resource_id)},
            {"$set": save_data},
            return_document=ReturnDocument.AFTER)

    def default_field_value(self, spec_name, field_name, default_value):
        save_data = self._parse_fields(spec_name, {field_name: default_value})

        self.db['resource_%s' % spec_name].update_many(
            {'_type': spec_name, field_name: {"$exists": False}},
            {"$set": {field_name: default_value}})

    def delete_field_value(self, spec_name, field_name):
        self.db['resource_%s' % spec_name].update_many(
            {'_type': spec_name, field_name: {"$exists": True}},
            {"$unset": {field_name: ""}})

    def alter_field_convert_type(self, spec_name, field_name, new_type):

        type_map = {
            'str': 'string',
            'int': 'int',
            'float': 'double',
            'bool': 'bool',
            'datetime': 'date',
        }


        self.db['resource_%s' % spec_name].aggregate([
            {"$match": {field_name: {"$exists": True}}},
            {"$addFields": {
                field_name: {"$convert": {
                    "input": "$%s"%field_name,
                    "to": type_map[new_type],
                    "onError": None,
                    "onNull": None,
                }},
            }},
            {"$merge": {
                "into": 'resource_%s' % spec_name,
                "whenNotMatched": "discard",
            }}
        ])

    def create_linkcollection_entry(self, spec_name, parent_id, parent_field, link_id):
        self.db['resource_%s' % spec_name].update_one({'_id': self.decodeid(parent_id)}, {'$addToSet': {parent_field: {'_id': self.decodeid(link_id)}}})
        return link_id

    def create_orderedcollection_entry(self, spec_name, parent_spec_name, parent_field, parent_id, data, extra_fields=None):
        resource_id = self.insert_resource(spec_name, data, parent_field, parent_spec_name, parent_id, extra_fields)
        self.create_linkcollection_entry(parent_spec_name, parent_id, parent_field, resource_id)
        return resource_id

    def resolve_calc_metadata(self, calc_str, spec_name=None):
        if spec_name is None or spec_name == 'root':
            spec = self.root
        else:
            spec = self.specs[spec_name]
        from metaphor.lrparse.lrparse import parse
        parsed = parse(calc_str, spec)

        return parsed.infer_type(), parsed.is_collection()

    def resolve_url(self, url):
        from metaphor.lrparse.lrparse import parse_url
        parsed = parse_url(url, self.root)

        return parsed.infer_type(), parsed.is_collection()

    def resolve_canonical_url(self, url):
        from metaphor.lrparse.lrparse import parse_canonical_url
        parsed = parse_canonical_url(url, self.root)

        return parsed.infer_type(), parsed.is_collection()

    def validate_spec(self, spec_name, data):
        spec = self.specs[spec_name]
        errors = []
        for field_name, field_data in data.items():
            if field_name not in spec.fields:
                errors.append({'error': "Nonexistant field: '%s'" % field_name})
                continue

            field = spec.fields[field_name]
            field_type = type(field_data).__name__
            if field.field_type == 'datetime':
                try:
                    datetime.fromisoformat(field_data)
                except TypeError as ve:
                    errors.append({'error': "Invalid type for field '%s' (expected 'str')" % (field_name,)})
                except ValueError as ve:
                    errors.append({'error': "Invalid date string for field '%s' (expected ISO format)" % (field_name,)})
            elif not field.check_comparable_type(field_type):
                errors.append({'error': "Invalid type: %s for field '%s' of '%s' (expected '%s')" % (field_type, field_name, spec_name, field.field_type)})
        for field_name, field in spec.fields.items():
            if field.required and field_name not in data:
                errors.append({"error": "Missing required field: '%s'" % field_name})
        return errors

    def load_identity(self, identity_type, email):
        identity_data = self.db['metaphor_identity'].find_one({
            "type": identity_type,
            "email": email,
        })
        return Identity.from_data(identity_data)

    def update_identity_session_id(self, identity):
        identity.session_id = str(uuid4())
        self.db['metaphor_identity'].update_one(
            {"_id": identity.identity_id},
            {"$set": {"session_id": identity.session_id}})

    def delete_identity_session_id(self, identity):
        identity.session_id = str(uuid4())
        self.db['metaphor_identity'].update_one(
            {"_id": identity.identity_id},
            {"$unset": {"session_id": ""}})

    def load_user_by_id(self, user_id):
        return self._load_user_with_aggregate({'_id': user_id})

    def load_user_by_email(self, email):
        return self._load_user_with_aggregate({'email': email})

    def _load_user_with_aggregate(self, match):
        user_data = self.db['resource_user'].find_one(match)
        if user_data:
            group_grants = []
            user_groups = [g['group_name'] for g in self.get_user_groups(user_data['_id'])]
            for group_name, group in self.groups.items():
                if group_name in user_groups:
                    group_grants.extend(group['grants'])
            grants = {
                'read': [g['url'] for g in group_grants if g['grant_type'] == 'read'],
                'create': [g['url'] for g in group_grants if g['grant_type'] == 'create'],
                'update': [g['url'] for g in group_grants if g['grant_type'] == 'update'],
                'delete': [g['url'] for g in group_grants if g['grant_type'] == 'delete'],
                'put': [g['url'] for g in group_grants if g['grant_type'] == 'put'],
            }
            user = User(user_data['_id'],
                        grants,
                        user_data.get('admin'))
            return user
        else:
            return None

    def create_initial_schema(self):
        self.name = "Default"
        self.description = "Default Schema"

        self.create_spec('user')
        self.create_field('user', 'email', 'str', required=True, indexed=True, unique=True, unique_global=True)
        self.create_field('user', 'admin', 'bool')

        self.create_field('root', 'users', 'collection', 'user')

        self.create_index_for_field('user', 'email')

        self.create_group("admin")
        self.create_grant("admin", "read", "/")
        self.create_grant("admin", "create", "/")
        self.create_grant("admin", "update", "/")
        self.create_grant("admin", "delete", "/")

    def has_global_duplicates(self, resource_name, field_name):
        try:
            result = self.db['resource_%s' % resource_name].aggregate([
                {"$group" : { "_id": f"${field_name}", "count": { "$sum": 1 } } },
                {"$match": {"_id" :{ "$ne" : None} , "count" : {"$gt": 1} } },
                {"$limit": 1},
            ])
            return next(result)['count'] > 1
        except StopIteration as si:
            return False

    def create_basic_identity(self, user_id, email, password):
        pw_hash = generate_password_hash(password)
        identity_data = self.db['metaphor_identity'].find_one_and_update(
            {"email": email, "type": "basic"},
            {"$set": {
                "password": pw_hash,
                "user_id": user_id,
                "name": email,
                "profile_url": None,
            }},
            upsert=True,
            return_document=ReturnDocument.AFTER)
        return Identity.from_data(identity_data)

    def get_or_create_identity(self, provider, user_id, email, name, profile_url):
        identity_data = self.db['metaphor_identity'].find_one_and_update(
            {"email": email, "type": provider},
            {"$set": {
                "name": name,
                "profile_url": profile_url,
                "user_id": user_id,
            }},
            upsert=True,
            return_document=ReturnDocument.AFTER)
        return Identity.from_data(identity_data)

    def load_identity_by_session_id(self, session_id):
        identity_data = self.db['metaphor_identity'].find_one({
            "session_id": session_id})
        if identity_data:
            return Identity.from_data(identity_data)
        else:
            return None

    def create_group(self, group_name):
        if group_name in self.groups:
            raise Exception("Group already exists: %s" % group_name)
        self.groups[group_name] = {"grants": []}
        self.save_groups()

    def delete_group(self, group_name):
        self.groups.pop(group_name)
        self.save_groups()

    def save_groups(self):
        self.db.metaphor_schema.update_one({"_id": self._id}, {"$set": {"groups": self.groups}})

    def create_grant(self, group_name, grant_type, url):
        allowed_grant_types = ['read', 'create', 'delete', 'update']
        if grant_type not in allowed_grant_types:
            raise Exception("Invalid grant type, must be one of %s" % (allowed_grant_types,))

        self.groups[group_name]["grants"].append({"grant_type": grant_type, "url": url})
        self.save_groups()

    def delete_grant(self, group_name, grant_type, url):
        self.groups[group_name]["grants"].pop(self.groups[group_name]["grants"].index({"grant_type": grant_type, "url": url}))
        self.save_groups()

    def add_user_to_group(self, group_name, user_id):
        self.db.metaphor_usergroup.insert_one({"group_name": group_name, "user_id": user_id})

    def remove_user_from_group(self, group_name, user_id):
        self.db.metaphor_usergroup.delete_one({"group_name": group_name, "user_id": user_id})

    def get_user_groups(self, user_id):
        return self.db.metaphor_usergroup.find({"user_id": user_id})

    def delete(self):
        self.db.metaphor_schema.delete_one({"_id": self._id})
