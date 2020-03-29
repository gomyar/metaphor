
from metaphor.updater import Updater


class AdminApi(object):
    def __init__(self, schema):
        self.schema = schema
        self.updater = Updater(schema)

    def format_schema(self):
        schema_db = self.schema.db['metaphor_schema'].find_one()
        schema_json = {
            'version': 'tbd',
            'specs': schema_db['specs'] if schema_db else {},
            'root': schema_db['root'] if schema_db else {},
        }
        return schema_json

    def create_spec(self, spec_name):
        self.schema.db['metaphor_schema'].update(
            {'_id': self.schema._id},
            {"$set": {'specs.%s' % spec_name: {'fields': {}}}})
        self.schema.load_schema()

    def create_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None):
        if field_type == 'calc':
            field_data = {'type': 'calc', 'calc_str': calc_str}
        elif field_type in ('int', 'str', 'float', 'bool'):
            field_data = {'type': field_type}
        else:
            field_data = {'type': field_type, 'target_spec_name': field_target}

        if spec_name == 'root':
            self.schema.db['metaphor_schema'].update(
                {'_id': self.schema._id},
                {"$set": {'root.%s' % (field_name,): field_data}})
        else:
            self.schema.db['metaphor_schema'].update(
                {'_id': self.schema._id},
                {"$set": {'specs.%s.fields.%s' % (spec_name, field_name): field_data}})
        self.schema.load_schema()
#        if field_type == 'calc':
