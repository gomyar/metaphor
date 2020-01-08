

from bson.objectid import ObjectId


class Field(object):
    PRIMITIVES = ['int', 'str', 'float', 'bool']

    def __init__(self, name, field_type, target_spec_name=None):
        self.name = name
        self.field_type = field_type
        self.target_spec_name = target_spec_name
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
        elif self.fields[name].field_type in ('link', 'reverse_link'):
            return self.schema.specs[self.fields[name].target_spec_name]
        else:
            return self.schema.specs[name]

    def is_collection(self):
        # specs map to resources and are not collections
        return False

    def is_field(self):
        return False


class Schema(object):
    def __init__(self, db):
        self.db = db
        self.specs = {}

    def load_schema(self):
        schema_data = self.db.metaphor_schema.find_one()
        for spec_name, spec_data in schema_data['specs'].items():
            spec = Spec(spec_name, self)
            for field_name, field_data in spec_data['fields'].items():
                spec.fields[field_name] = self._create_field(field_name, field_data)
            self.specs[spec_name] = spec
        self._add_reverse_links()

    def _create_field(self, field_name, field_data):
        if field_data['type'] == 'calc':
            return CalcField(field_name, calc_str=field_data['calc_str'])
        else:
            return Field(field_name, field_data['type'], target_spec_name=field_data.get('target_spec_name'))

    def _add_reverse_links(self):
        for spec in self.specs.values():
            for field in spec.fields.values():
                if field.field_type == 'link':
                    reverse_field_name = "link_%s_%s" % (spec.name, field.name)
                    self.specs[field.target_spec_name].fields[reverse_field_name] = Field(reverse_field_name, "reverse_link", spec.name)
                if field.field_type == 'collection':
                    parent_field_name = "parent_%s_%s" % (spec.name, field.name)
                    self.specs[field.target_spec_name].fields[parent_field_name] = Field(parent_field_name, "parent_collection", spec.name)
                if field.field_type == 'linkcollection':
                    parent_field_name = "link_%s_%s" % (spec.name, field.name)
                    self.specs[field.target_spec_name].fields[parent_field_name] = Field(parent_field_name, "reverse_link_collection", spec.name)

    def encodeid(self, mongo_id):
        return "ID" + str(mongo_id)

    def decodeid(self, str_id):
        return ObjectId(str_id[2:])

    def encode_resource(self, spec, resource_data):
        encoded = {
            'id': self.encodeid(resource_data['_id'])
        }
        for field_name, field in spec.fields.items():
            field_value = resource_data.get(field_name)
            if field_value:
                if field.field_type == 'link':
                    encoded[field_name] = self.encodeid(field_value)
                else:
                    encoded[field_name] = field_value
        return encoded

    def insert_resource(self, spec_name, data):
        new_resource_id = self.db['resource_%s' % spec_name].insert(data)
        return self.encodeid(new_resource_id)

    def update_resource_fields(self, spec_name, resource_id, field_data):
        spec = self.specs[spec_name]
        for field_name, field_value in field_data.items():
            if spec.fields[field_name].field_type == 'link':
                field_data[field_name] = self.decodeid(field_value)
        self.db['resource_%s' % spec_name].update({"_id": self.decodeid(resource_id)}, {"$set": field_data})

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
