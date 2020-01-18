
import os
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
    def __init__(self, field_name, calc_str, spec):
        self.field_name = field_name
        self.calc_str = calc_str
        self.spec = spec
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
        elif self.fields[name].field_type in ('link', 'reverse_link', 'parent_collection', 'collection', 'linkcollection'):
            return self.schema.specs[self.fields[name].target_spec_name]
        else:
            raise Exception('Unrecognised field type')

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

    def load_schema(self):
        schema_data = self.db.metaphor_schema.find_one()
        for spec_name, spec_data in schema_data['specs'].items():
            spec = Spec(spec_name, self)
            for field_name, field_data in spec_data['fields'].items():
                spec.fields[field_name] = self._create_field(field_name, field_data, spec)
            self.specs[spec_name] = spec
        self._add_reverse_links()
        for root_name, root_data in schema_data.get('root', {}).items():
            self.root.fields[root_name] = self._create_field(root_name, root_data, self.root)

    def _create_field(self, field_name, field_data, spec):
        if field_data['type'] == 'calc':
            return CalcField(field_name, calc_str=field_data['calc_str'], spec=spec)
        else:
            return Field(field_name, field_data['type'], target_spec_name=field_data.get('target_spec_name'))

    def _add_reverse_links(self):
        for spec in self.specs.values():
            for field in spec.fields.values():
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
