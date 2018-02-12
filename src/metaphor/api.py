
import os

from metaphor.schema_factory import SchemaFactory
from metaphor.resource import CollectionSpec


class MongoApi(object):
    def __init__(self, root_url, schema, db):
        self.root_url = root_url
        self.schema = schema
        self.db = db

    def post(self, path, data):
        resource = self.build_resource(path)
        return resource.create(data)

    def patch(self, path, data):
        resource = self.build_resource(path)
        return resource.update(data)

    def get(self, path):
        path = path.strip('/')
        if path:
            resource = self.build_resource(path)
            return resource.serialize(os.path.join(self.root_url, 'api', path))
        else:
            return self.schema.root.serialize(os.path.join(self.root_url, 'api'))

    def unlink(self, path):
        resource = self.build_resource(path)
        resource.unlink()
        return resource._id

    def build_resource(self, path):
        return self.schema.root.build_child(path)


class SchemaApi(object):
    def __init__(self, root_url, schema, db):
        self.root_url = root_url
        self.schema = schema
        self.db = db

    def post(self, path, data):
        SchemaFactory().add_resource_to_schema(self.schema, data['name'], data.get('fields', {}))
        SchemaFactory().save_schema(self.schema)

    def patch(self, spec_name, data):
        for field_name, field_data in data.items():
            SchemaFactory().add_field_to_spec(self.schema, spec_name, field_name, field_data)

    def get(self, path):
        pass

    def unlink(self, path):
        pass


class RootsApi(object):
    def __init__(self, root_url, schema, db):
        self.root_url = root_url
        self.schema = schema
        self.db = db

    def post(self, root_name, spec_name):
        self.schema.add_root(root_name, CollectionSpec(spec_name))
        SchemaFactory().save_schema(self.schema)
