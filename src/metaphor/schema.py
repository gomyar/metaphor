
import os
from datetime import datetime
from bson.objectid import ObjectId
from pymongo import ReturnDocument
from flask_login import UserMixin


class Field(object):
    PRIMITIVES = ['int', 'str', 'float', 'bool']

    def __init__(self, name, field_type, target_spec_name=None, reverse_link_field=None):
        self.name = name
        self.field_type = field_type
        self.target_spec_name = target_spec_name
        self.reverse_link_field = reverse_link_field  # only used for reverse links
        self._comparable_types= {
            'str': [str],
            'int': [float, int],
            'float': [float, int],
            'bool': [bool],
        }
        self.spec = None

    def is_primitive(self):
        return self.field_type in Field.PRIMITIVES

    def is_collection(self):
        return self.field_type in ['collection', 'linkcollection', 'reverse_link', 'reverse_link_collection']

    def is_field(self):
        return True

    def check_comparable_type(self, value):
        return type(value) in self._comparable_types.get(self.field_type, [])

    def __repr__(self):
        return "<Field %s %s %s>" % (self.name, self.field_type, self.target_spec_name or '')


class CalcField(Field):
    def __init__(self, field_name, calc_str):
        self.field_name = field_name
        self.calc_str = calc_str
        self.field_type = 'calc'
        self.spec = None

    def __repr__(self):
        return "<Calc %s = %s>" % (self.field_name, self.calc_str)

    def infer_type(self):
        calc_tree = self.spec.schema.calc_trees[self.spec.name, self.field_name]
        return calc_tree.infer_type()

    def is_primitive(self):
        return self.infer_type().is_primitive()

    def is_collection(self):
        calc_tree = self.spec.schema.calc_trees[self.spec.name, self.field_name]
        return calc_tree.is_collection()


# field types:
# int str float bool
# link -> + target_spec_name
# reverse_link -> implied by link / linkcollection
# parent -> implied by collection
# collection -> + target_spec_name
# linkcollection -> + target_spec_name
# calc -> + calculated_spec_name + calculated_spec_type (int str float bool link linkcollection)


class Spec(object):
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self.fields = {}

    def __repr__(self):
        return "<Spec %s>" % (self.name,)

    def build_child_spec(self, name):
        if name not in self.fields:
            raise SyntaxError("No such field %s in %s" % (name, self.name))
        if self.fields[name].is_primitive():
            return self.fields[name]
        elif self.fields[name].field_type in ('link', 'reverse_link', 'parent_collection', 'collection', 'linkcollection', 'reverse_link_collection'):
            return self.schema.specs[self.fields[name].target_spec_name]
        elif self.fields[name].field_type == 'calc':
            from metaphor.lrparse.lrparse import parse
            field = self.fields[name]
            tree = parse(field.calc_str, self)
            return tree.infer_type()
        else:
            raise SyntaxError('Unrecognised field type')

    def resolve_child(self, child_path):
        ''' child_path "self.division.name" dot-separated child specs '''
        children = child_path.split('.')
        if 'self' == children[0]:
            child = self
            children.pop(0)
        else:
            child = self.schema.root

        for field_name in children:
            child = child.build_child_spec(field_name)
        return child

    def is_collection(self):
        # specs map to resources and are not collections
        return False

    def is_primitive(self):
        return False

    def is_field(self):
        return False


class User(UserMixin):
    def __init__(self, username, read_grants, create_grants, update_grants, delete_grants, admin=False):
        self.username = username
        self.read_grants = read_grants
        self.create_grants = create_grants
        self.update_grants = update_grants
        self.delete_grants = delete_grants
        self.admin = admin

    def get_id(self):
        return self.username

    def is_admin(self):
        return self.admin


class Schema(object):
    def __init__(self, db):
        self.db = db
        self.specs = {}
        self.root = Spec('root', self)
        self._id = None
        self.calc_trees = {}

    def load_schema(self):
        self.specs = {}
        self.root = Spec('root', self)

        schema_data = self.db.metaphor_schema.find_one_and_update(
            {"_id": {"$exists": True}},
            {
                "$set": {"loaded": datetime.now()},
                "$setOnInsert": {"specs": {}, "root": {}, "created": datetime.now()},
            }, upsert=True, return_document=ReturnDocument.AFTER)
        self._id = schema_data['_id']
        for spec_name, spec_data in schema_data['specs'].items():
            spec = self.add_spec(spec_name)
            for field_name, field_data in spec_data['fields'].items():
                if field_data['type'] == 'calc':
                    self._add_calc(spec, field_name, field_data['calc_str'])
                else:
                    self._add_field(spec, field_name, field_data['type'], target_spec_name=field_data.get('target_spec_name'))
        self._add_reverse_links()
        for root_name, root_data in schema_data.get('root', {}).items():
            if root_data['type'] == 'calc':
                self._add_calc(self.root, field_name, root_data['calc_str'])
            else:
                self._add_field(self.root, root_name, root_data['type'], target_spec_name=root_data.get('target_spec_name'))

        # pre-parse calcs
        from metaphor.lrparse.lrparse import parse
        for spec_name, spec_data in schema_data['specs'].items():
            spec = self.specs[spec_name]
            for field_name, field_data in spec_data['fields'].items():
                if field_data['type'] == 'calc':
                    self.calc_trees[(spec.name, field_name)] = parse(field_data['calc_str'], spec)
        for root_name, root_data in schema_data.get('root', {}).items():
            if root_data['type'] == 'calc':
                self.calc_trees[('root', root_name)] = parse(root_data['calc_str'], self.root)

    def add_spec(self, spec_name):
        spec = Spec(spec_name, self)
        self.specs[spec_name] = spec
        return spec

    def _add_field(self, spec, field_name, field_type, target_spec_name=None):
        field = Field(field_name, field_type, target_spec_name=target_spec_name)
        spec.fields[field_name] = field
        field.spec = spec
        return field

    def add_field(self, spec, field_name, field_type, target_spec_name=None):
        field = self._add_field(spec, field_name, field_type, target_spec_name)
        self._add_reverse_link_for_field(field, spec)
        return field

    def add_calc(self, spec, field_name, calc_str):
        from metaphor.lrparse.lrparse import parse
        calc_field = self._add_calc(spec, field_name, calc_str)
        self.calc_trees[(spec.name, field_name)] = parse(calc_str, spec)
        return calc_field

    def _add_calc(self, spec, field_name, calc_str):
        calc_field = CalcField(field_name, calc_str=calc_str)
        spec.fields[field_name] = calc_field
        spec.fields[field_name].spec = spec
        return calc_field

    def _add_reverse_links(self):
        for spec in self.specs.values():
            for field in spec.fields.values():
                self._add_reverse_link_for_field(field, spec)

    def _add_reverse_link_for_field(self, field, spec):
        if field.field_type == 'link':
            reverse_field_name = "link_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields[reverse_field_name] = Field(reverse_field_name, "reverse_link", spec.name, field.name)
        if field.field_type == 'collection':
            parent_field_name = "parent_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields[parent_field_name] = Field(parent_field_name, "parent_collection", spec.name, field.name)
        if field.field_type == 'linkcollection':
            parent_field_name = "link_%s_%s" % (spec.name, field.name)
            self.specs[field.target_spec_name].fields[parent_field_name] = Field(parent_field_name, "reverse_link_collection", spec.name, field.name)

    def encodeid(self, mongo_id):
        return "ID" + str(mongo_id)

    def decodeid(self, str_id):
        return ObjectId(str_id[2:])

    def load_canonical_parent_url(self, parent_type, parent_id):
        if parent_id:
            parent_data = self.load_parent_data(parent_type, parent_id)
            return os.path.join(parent_data['_parent_canonical_url'], parent_data['_parent_field_name'], parent_id)
        else:
            return '/'

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
                parsed_data['_canonical_url_%s' % field_name] = self.load_canonical_parent_url(field.target_spec_name, field_value)
            else:
                parsed_data[field_name] = field_value
        return parsed_data

    def insert_resource(self, spec_name, data, parent_field_name, parent_type=None, parent_id=None, grants=None):
        data = self._parse_fields(spec_name, data)

        new_id = ObjectId()  # doing this to be able to construct a canonical url without 2 writes
        if parent_id:
            parent_data = self.load_parent_data(parent_type, parent_id)
            parent_canonical_url = os.path.join(parent_data['_parent_canonical_url'], parent_data['_parent_field_name'], parent_id)
            grants = parent_data['_grants']
        else:
            # assume grants taken from root
            parent_canonical_url = '/'
            grants = grants or []
        data['_id'] = new_id
        data['_parent_type'] = parent_type or 'root'
        data['_parent_id'] = self.decodeid(parent_id) if parent_id else None
        data['_parent_field_name'] = parent_field_name
        data['_parent_canonical_url'] = parent_canonical_url
        data['_canonical_url'] = os.path.join(parent_canonical_url, parent_field_name, self.encodeid(new_id))
        data['_grants'] = grants
        new_resource_id = self.db['resource_%s' % spec_name].insert(data)
        return self.encodeid(new_resource_id)

    def delete_resource(self, spec_name, resource_id):
        self.db['resource_%s' % spec_name].delete_one({'_id': self.decodeid(resource_id)})

    def delete_linkcollection_entry(self, spec_name, parent_id, field_name, resource_id):
        self.db['resource_%s' % spec_name].update({"_id": parent_id} ,{"$pull": {'parttimers': {"_id": self.decodeid(resource_id)}}})

    def update_resource_fields(self, spec_name, resource_id, field_data):
        save_data = self._parse_fields(spec_name, field_data)
        new_resource = self.db['resource_%s' % spec_name].find_one_and_update(
            {"_id": self.decodeid(resource_id)},
            {"$set": save_data},
            return_document=ReturnDocument.AFTER)

    def create_linkcollection_entry(self, spec_name, parent_id, parent_field, link_id):
        self.db['resource_%s' % spec_name].update({'_id': self.decodeid(parent_id)}, {'$addToSet': {parent_field: {'_id': self.decodeid(link_id)}}})
        return link_id

    def validate_spec(self, spec_name, data):
        spec = self.specs[spec_name]
        errors = []
        for field_name, field_data in data.items():
            field = spec.fields[field_name]
            field_type = type(field_data).__name__
            if field_type in Field.PRIMITIVES:
                if field.field_type != field_type:
                    errors.append({'error': "Invalid type: %s for field '%s' of '%s'" % (field_type, field_name, spec_name)})
        return errors

    def remove_spec_field(self, spec_name, field_name):
        self.db['resource_%s' % spec_name].update_many({}, {'$unset': {field_name: ''}})

    def load_user(self, username, load_hash=False):
        user_data = self.db['resource_user'].find_one({'username': username})
        if user_data:
            user = User(username,
                        user_data['read_grants'],
                        user_data['create_grants'],
                        user_data['update_grants'],
                        user_data['delete_grants'],
                        user_data.get('admin'))
            if load_hash:
                user.pw_hash = user_data['pw_hash']
            return user
        else:
            return None

    def create_initial_schema(self):
        self.db.metaphor_schema.insert_one({
            "specs": {
                "user" : {
                    "fields" : {
                        "username" : {
                            "type" : "str"
                        },
                        "pw_hash" : {
                            "type" : "str"
                        },
                        "groups": {
                            "type": "linkcollection",
                            "target_spec_name": "group"
                        },
                        "read_grants": {
                            "type": "calc",
                            "calc_str": "self.groups.grants[type='read'].url",
                        },
                        "create_grants": {
                            "type": "calc",
                            "calc_str": "self.groups.grants[type='create'].url",
                        },
                        "update_grants": {
                            "type": "calc",
                            "calc_str": "self.groups.grants[type='update'].url",
                        },
                        "delete_grants": {
                            "type": "calc",
                            "calc_str": "self.groups.grants[type='delete'].url",
                        },
                        "admin": {
                            "type": "bool",
                        },
                    }
                },
                "group" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "grants": {
                            "type": "collection",
                            "target_spec_name": "grant"
                        },
                    }
                },
                "grant" : {
                    "fields" : {
                        "type" : {
                            "type" : "str"
                        },
                        "url" : {
                            "type" : "str"
                        },
                    }
                },

            },
            "root": {
                "users" : {
                    "type" : "collection",
                    "target_spec_name" : "user"
                },
                "groups" : {
                    "type" : "collection",
                    "target_spec_name" : "group"
                },
            },
            "created": datetime.now()
        })

        self.load_schema()
#        group_id = self.insert_resource('group', {'name': 'admin'}, '/groups')

#        for grant_type in ['read', 'create', 'update', 'delete']:
#            self.insert_resource('grant', {'type': grant_type, 'url': '/groups'}, 'grants', 'group', group_id)
#            self.insert_resource('grant', {'type': grant_type, 'url': '/users'}, 'grants', 'group', group_id)
#            self.insert_resource('grant', {'type': grant_type, 'url': '/'}, 'grants', 'group', group_id)

    def read_root_grants(self, path):
        return [g['_id'] for g in self.db['resource_grant'].find({'url': '/%s' % path, 'type': 'read'}, {'_id': True})]

