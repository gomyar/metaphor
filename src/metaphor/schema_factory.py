
import importlib

from metaphor.resource import ResourceSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import ReverseLinkSpec
from metaphor.resource import CalcSpec
from metaphor.resource import FieldSpec
from metaphor.resource import CollectionSpec
from metaphor.resource import LinkCollectionSpec
from metaphor.schema import Schema


class SchemaFactory(object):
    def __init__(self):
        self.field_builders = {
            'link': self._build_link,
            'str': self._build_field,
            'int': self._build_field,
            'float': self._build_field,
            'bool': self._build_field,
            'calc': self._build_calc,
            'collection': self._build_collection_field,
            'linkcollection': self._build_link_collection_field,
        }
        self.field_serializers = {
            'link': self._serialize_link,
            'str': self._serialize_field,
            'int': self._serialize_field,
            'float': self._serialize_field,
            'bool': self._serialize_field,
            'calc': self._serialize_calc,
            'collection': self._serialize_collection_field,
            'linkcollection': self._serialize_link_collection_field,
        }

    def create_schema(self, db, version, data):
        schema = Schema(db, version)
        self._add_specs_from_data(schema, data['specs'])
        self._add_reverse_links(schema)
        for root_name, resource_data in data['roots'].items():
            schema.add_root(root_name, self._build_collection(None, resource_data))
        return schema

    def _add_specs_from_data(self, schema, spec_data):
        for resource_name, resource_data in spec_data.items():
            self._add_spec(schema, resource_name, resource_data.get('fields', {}))

    def add_resource_to_schema(self, schema, resource_name, resource_fields):
        self._add_spec(schema, resource_name, resource_fields)
        self._add_reverse_links_for_fields(schema, resource_name, resource_fields)

    def _add_reverse_links(self, schema):
        for name, spec in schema.specs.items():
            for field_name, field_spec in spec.fields.items():
                spec._link_field(field_name, field_spec)

    def _add_spec(self, schema, resource_name, resource_fields):
        spec = ResourceSpec(resource_name)
        schema.add_resource_spec(spec)
        spec.schema = schema
        for field_name, field_data in resource_fields.items():
            spec = schema.specs[resource_name]
            spec._add_field(field_name, self.field_builders[field_data['type']](field_name, field_data))

    def _add_reverse_links_for_fields(self, schema, resource_name, resource_fields):
        for field_name, field_data in resource_fields.items():
            spec = schema.specs[resource_name]
            spec._link_field(field_name, spec.fields[field_name])

    def add_field_to_spec(self, schema, resource_name, field_name, field_data):
        spec = schema.specs[resource_name]
        spec.add_field(field_name, self.field_builders[field_data['type']](field_name, field_data))

    def validate_field_spec(self, schema, resource_name, field_name, field_data):
        if field_name.startswith('link_'):
            raise Exception("Fields cannot start with 'link_' (reserved for interal use)")
        calc = self.field_builders[field_data['type']](field_name, field_data)
        parent_spec = schema.specs[resource_name]
        if field_data['type'] == 'calc':
            calc.schema = schema
            calc.parent = parent_spec
            calc.field_name = field_name
            for resource_ref in calc.all_resource_refs():
                calc.resolve_spec(resource_ref)

    def _build_collection(self, type_name, data=None):
        return CollectionSpec(data['target'])

    def _build_link(self, type_name, data=None):
        return ResourceLinkSpec(data['target'])

    def _build_field(self, type_name, data=None):
        return FieldSpec(data.get('type'))

    def _build_calc(self, type_name, data=None):
        return CalcSpec(data['calc'], data['calc_type'])

    def _build_collection_field(self, type_name, data=None):
        return CollectionSpec(data['target'])

    def _build_link_collection_field(self, type_name, data=None):
        return LinkCollectionSpec(data['target'])

    def serialize_schema(self, schema):
        specs = dict([(name, self._serialize_spec(data)) for (name, data) in schema.specs.items() if name != 'root'])
        roots = dict([(name, {'type': 'collection', 'target': spec.target_spec_name}) for (name, spec) in schema.specs['root'].fields.items()])
        return {'specs': specs, 'roots': roots, 'version': schema.version}

    def _serialize_spec(self, spec):
        fields = dict([(name, self.field_serializers[field.field_type](field)) for (name, field) in spec.fields.items() if type(field) != ReverseLinkSpec])
        return {'type': 'resource', 'fields': fields}

    def _serialize_collection(self, collection):
        return {'type': 'collection', 'target': collection.target_spec_name}

    def _serialize_link(self, link):
        return {'type': 'link', 'target': link.name}

    def _serialize_field(self, field):
        return {'type': field.field_type}

    def _serialize_calc(self, calc):
        return {'type': 'calc', 'calc': calc.calc_str, 'calc_type': calc.calc_type}

    def _serialize_collection_field(self, collection):
        return {'type': 'collection', 'target': collection.target_spec_name}

    def _serialize_link_collection_field(self, collection):
        return {'type': 'linkcollection', 'target': collection.target_spec_name}

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
