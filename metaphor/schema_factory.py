
import importlib

from metaphor.resource import ResourceSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.resource import FieldSpec
from metaphor.resource import CollectionSpec
from metaphor.schema import Schema


class SchemaFactory(object):
    def __init__(self):
        self.field_builders = {
            'link': self._build_link,
            'str': self._build_field,
            'int': self._build_field,
            'calc': self._build_calc,
            'collection': self._build_collection_field,
        }
        self.field_serializers = {
            'link': self._serialize_link,
            'str': self._serialize_field,
            'int': self._serialize_field,
            'calc': self._serialize_calc,
            'collection': self._serialize_collection_field,
        }

    def create_schema(self, db, version, data):
        schema = Schema(db, version)
        for resource_name, resource_data in data['specs'].items():
            self.add_resource_to_schema(schema, resource_name, resource_data.get('fields', {}))
        for root_name, resource_data in data['roots'].items():
            schema.add_root(root_name, self._build_collection(None, resource_data))
        for name, import_path in data.get('registered_functions', {}).items():
            func_module, func_name = import_path.rsplit('.', 1)
            mod = importlib.import_module(func_module)
            schema.register_function(name, getattr(mod, func_name))
        return schema

    def add_resource_to_schema(self, schema, resource_name, resource_fields):
        spec = ResourceSpec(resource_name)
        schema.add_resource_spec(spec)
        for field_name, field_data in resource_fields.items():
            self.add_field_to_spec(schema, resource_name, field_name, field_data)

    def add_field_to_spec(self, schema, resource_name, field_name, field_data):
        spec = schema.specs[resource_name]
        spec.add_field(field_name, self.field_builders[field_data['type']](field_name, field_data))

    def _build_collection(self, type_name, data=None):
        return CollectionSpec(data['target'])

    def _build_link(self, type_name, data=None):
        return ResourceLinkSpec(data['target'])

    def _build_field(self, type_name, data=None):
        return FieldSpec(data.get('type'))

    def _build_calc(self, type_name, data=None):
        return CalcSpec(data['calc'])

    def _build_collection_field(self, type_name, data=None):
        return CollectionSpec(data['target'])

    def serialize_schema(self, schema):
        specs = dict([(name, self._serialize_spec(data)) for (name, data) in schema.specs.items() if name != 'root'])
        roots = dict([(name, {'type': 'collection', 'target': spec.target_spec_name}) for (name, spec) in schema.specs['root'].fields.items()])
        registered_functions = dict([(name, "%s.%s" % (func.__module__, func.__name__)) for (name, func) in schema._functions.items()])
        return {'specs': specs, 'roots': roots, 'version': schema.version, 'registered_functions': registered_functions}

    def _serialize_spec(self, spec):
        fields = dict([(name, self.field_serializers[field.field_type](field)) for (name, field) in spec.fields.items()])
        return {'type': 'resource', 'fields': fields}

    def _serialize_collection(self, collection):
        return {'type': 'collection', 'target': collection.target_spec_name}

    def _serialize_link(self, link):
        return {'type': 'link', 'target': link.name}

    def _serialize_field(self, field):
        return {'type': field.field_type}

    def _serialize_calc(self, calc):
        return {'calc': calc.calc_str}

    def _serialize_collection_field(self, collection):
        return {'type': 'collection', 'target': collection.target_spec_name}

    def save_schema(self, schema):
        save_data = SchemaFactory().serialize_schema(schema)
        schema.db['metaphor_schema'].find_and_modify(
            sort={'version': 1}, update=save_data, upsert=True)

    def load_schema(self, db):
        schema_data = db['metaphor_schema'].find_one({})
        if schema_data:
            return self.create_schema(db, "0.1", schema_data)
        else:
            return Schema(db, "0.1")
