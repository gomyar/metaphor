

from metaphor.resource import ResourceSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.resource import FieldSpec
from metaphor.resource import CollectionSpec
from metaphor.schema import Schema


class SchemaFactory(object):
    def __init__(self):
        self.field_builders ={
            'link': self._build_link,
            'str': self._build_field,
            'int': self._build_field,
            'calc': self._build_calc,
            'collection': self._build_collection_field,
        }

    def create_schema(self, db, version, data):
        schema = Schema(db, version)
        for resource_name, resource_data in data['specs'].items():
            self.add_resource_to_schema(schema, resource_name, resource_data.get('fields', {}))
        for root_name, resource_data in data['roots'].items():
            schema.add_root(root_name, self._build_collection(None, resource_data))
        return schema

    def add_resource_to_schema(self, schema, resource_name, resource_fields):
        spec = ResourceSpec(resource_name)
        schema.add_resource_spec(spec)
        for field_name, field_data in resource_fields.items():
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
