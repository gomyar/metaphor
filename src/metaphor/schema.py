
import os
from datetime import datetime
from bson.objectid import ObjectId
from pymongo import ReturnDocument


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
            'bool': [bool, float, int, str],
        }

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

    def __repr__(self):
        return "<Calc %s = %s>" % (self.field_name, self.calc_str)


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
            raise Exception('Unrecognised field type')

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
                    self.add_calc(spec, field_name, field_data['calc_str'])
                else:
                    self._add_field(spec, field_name, field_data['type'], target_spec_name=field_data.get('target_spec_name'))
        self._add_reverse_links()
        for root_name, root_data in schema_data.get('root', {}).items():
            if root_data['type'] == 'calc':
                self.add_calc(self.root, root_name, root_data['calc_str'])
            else:
                self._add_field(self.root, root_name, root_data['type'], target_spec_name=root_data.get('target_spec_name'))

    def add_spec(self, spec_name):
        spec = Spec(spec_name, self)
        self.specs[spec_name] = spec
        return spec

    def _add_field(self, spec, field_name, field_type, target_spec_name=None):
        field = Field(field_name, field_type, target_spec_name=target_spec_name)
        spec.fields[field_name] = field
        return field

    def add_field(self, spec, field_name, field_type, target_spec_name=None):
        field = self._add_field(spec, field_name, field_type, target_spec_name)
        self._add_reverse_link_for_field(field, spec)

    def add_calc(self, spec, field_name, calc_str):
        from metaphor.lrparse.lrparse import parse
        spec.fields[field_name] =  CalcField(field_name, calc_str=calc_str)
        self.calc_trees[(spec.name, field_name)] = parse(calc_str, spec)

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
            parent_data = self.db['resource_%s' % parent_type].find_one({'_id': self.decodeid(parent_id)})
            return os.path.join(parent_data['_parent_canonical_url'], parent_data['_parent_field_name'], parent_id)
        else:
            return "/"

    def insert_resource(self, spec_name, data, parent_field_name, parent_type=None, parent_id=None):
        data['_parent_type'] = parent_type or 'root'
        data['_parent_id'] = self.decodeid(parent_id) if parent_id else None
        data['_parent_field_name'] = parent_field_name
        data['_parent_canonical_url'] = self.load_canonical_parent_url(parent_type, parent_id)
        new_resource_id = self.db['resource_%s' % spec_name].insert(data)
        self._update_dependencies(spec_name, data)
        return self.encodeid(new_resource_id)

    def update_resource_fields(self, spec_name, resource_id, field_data):
        spec = self.specs[spec_name]
        save_data = {}
        for field_name, field_value in field_data.items():
            field = spec.fields[field_name]
            if field.field_type == 'link':
                save_data[field_name] = self.decodeid(field_value)
                save_data['_canonical_url_%s' % field_name] = self.load_canonical_parent_url(field.target_spec_name, field_value)
            else:
                save_data[field_name] = field_value
        new_resource = self.db['resource_%s' % spec_name].find_one_and_update(
            {"_id": self.decodeid(resource_id)},
            {"$set": save_data},
            return_document=ReturnDocument.AFTER)
        self._update_dependencies(spec_name, save_data)

    def _update_dependencies(self, spec_name, save_data):
        spec = self.specs[spec_name]
        for field_name in save_data:
            # find dependencies and update
            pass

    def create_linkcollection_entry(self, spec_name, parent_id, parent_field, link_id):
        self.db['resource_%s' % spec_name].update({'_id': self.decodeid(parent_id)}, {'$push': {parent_field: {'_id': self.decodeid(link_id)}}})
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
