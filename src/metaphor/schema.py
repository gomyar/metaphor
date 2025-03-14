
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


class User(UserMixin):
    def __init__(self, user_id, username, password, grants, user_hash, admin=False):
        self._id = user_id
        self.username = username
        self.password = password
        self.grants = grants
        self.admin = admin
        self.user_hash = user_hash

    def get_id(self):
        return self.user_hash

    def is_admin(self):
        return self.admin

    def __repr__(self):
        return "<User %s>" % self.username


class Schema(object):
    def __init__(self, db):
        self.db = db

        self.name = ""
        self.description = ""

        self.specs = {}
        self.root = Spec('root', self)
        self._id = None
        self.current = None
        self.calc_trees = {}

    @property
    def schema_id(self):
        return str(self._id)

    @staticmethod
    def create_schema(db):
        inserted = db.metaphor_schema.insert_one({"specs": {}, "root": {}, "created": datetime.now().isoformat()})
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

    def insert_resource(self, spec_name, data, parent_field_name, parent_type=None, parent_id=None, grants=None, extra_fields=None):
        data = self._parse_fields(spec_name, data)

        new_id = ObjectId()  # doing this to be able to construct a canonical url without 2 writes
        data['_id'] = new_id
        data['_schema_id'] = self._id
        data['_type'] = spec_name
        data['_parent_type'] = parent_type or 'root'
        data['_parent_id'] = self.decodeid(parent_id) if parent_id else None
        data['_parent_field_name'] = parent_field_name

        if extra_fields:
            data.update(extra_fields)

        if spec_name == 'user':
            data['_user_hash'] = str(uuid4())

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

        if spec_name == 'user' and field_data.get('password'):
            save_data['_user_hash'] = str(uuid4())

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

    def create_orderedcollection_entry(self, spec_name, parent_spec_name, parent_field, parent_id, data, grants=None, extra_fields=None):
        resource_id = self.insert_resource(spec_name, data, parent_field, parent_spec_name, parent_id, grants, extra_fields)
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

#    def remove_spec_field(self, spec_name, field_name):
#        self.db['resource_%s' % spec_name].update_many({'_type': spec_name}, {'$unset': {field_name: ''}})

    def load_user_by_username(self, username, load_hash=False):
        return self._load_user_with_aggregate({'username': username}, load_hash)

    def load_user_by_user_hash(self, user_hash):
        return self._load_user_with_aggregate({'_user_hash': user_hash})

    def _load_user_with_aggregate(self, match, load_hash=False):
        user_data = self.db['resource_user'].aggregate([
            {"$match": match},
            {"$limit": 1},
            {"$lookup": {
                "foreignField": "_id",
                "localField": "groups._id",
                "as": "groups",
                "from": "resource_group",
            }},
            {"$lookup": {
                "foreignField": "_parent_id",
                "localField": "groups._id",
                "as": "group_grants",
                "from": "resource_grant",
            }},
            {"$limit": 1},
            {"$project": {
                '_id': 1,
                'username': 1,
                'password': 1,
                'read_grants._id': 1,
                'read_grants.url': 1,
                'create_grants._id': 1,
                'create_grants.url': 1,
                'update_grants._id': 1,
                'update_grants.url': 1,
                'delete_grants._id': 1,
                'delete_grants.url': 1,
                'put_grants._id': 1,
                'put_grants.url': 1,
                '_user_hash': 1,
                'admin': 1,
                'group_grants.type': 1,
                'group_grants.url': 1,
            }},
        ])
        user_data = list(user_data)
        if user_data:
            user_data = user_data[0]
            grants = {
                'read': [g['url'] for g in user_data['group_grants'] if g['type'] == 'read'],
                'create': [g['url'] for g in user_data['group_grants'] if g['type'] == 'create'],
                'update': [g['url'] for g in user_data['group_grants'] if g['type'] == 'update'],
                'delete': [g['url'] for g in user_data['group_grants'] if g['type'] == 'delete'],
                'put': [g['url'] for g in user_data['group_grants'] if g['type'] == 'put'],
            }
            user = User(user_data['_id'],
                        user_data['username'],
                        user_data['password'],
                        grants,
                        user_data['_user_hash'],
                        user_data.get('admin'))
            if load_hash:
                user.password = user_data['password']
            return user
        else:
            return None

    def create_initial_schema(self):
        self.name = "Default"
        self.description = "Default Schema"

        self.create_spec('user')
        self.create_field('user', 'username', 'str')
        self.create_field('user', 'password', 'str')
        self.create_field('user', 'admin', 'bool')

        self.create_spec('group')
        self.create_field('group', 'name', 'str')

        self.create_spec('grant')
        self.create_field('grant', 'type', 'str')
        self.create_field('grant', 'url', 'str')

        self.create_field('user', 'groups', 'linkcollection', 'group')
        self.create_field('group', 'grants', 'collection', 'grant')

        self.create_field('user', 'read_grants', 'calc', calc_str="self.groups.grants[type='read']")
        self.create_field('user', 'create_grants', 'calc', calc_str="self.groups.grants[type='create']")
        self.create_field('user', 'update_grants', 'calc', calc_str="self.groups.grants[type='update']")
        self.create_field('user', 'delete_grants', 'calc', calc_str="self.groups.grants[type='delete']")
        self.create_field('user', 'put_grants', 'calc', calc_str="self.groups.grants[type='put']")

        self.create_field('root', 'users', 'collection', 'user')
        self.create_field('root', 'groups', 'collection', 'group')

    def read_root_grants(self, path):
        or_clause = [{'url': '/'}]
        segments = path.split('/')
        while segments:
            or_clause.append({'url': '/' + '/'.join(segments)})
            segments = segments[:-1]
        query = {
            "$and": [
                {"_type": "grant"},
                {"$or": or_clause},
            ]
        }
        return [g['_id'] for g in self.db['resource_grant'].find(query, {'_id': True})]

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
