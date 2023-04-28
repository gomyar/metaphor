
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.lrparse.lrparse import parse


class SchemaFactory:
    def __init__(self, db):
        self.db = db

    def load_current_schema(self):
        data = self.db.metaphor_schema.find_one({"current": True})
        schema = Schema(self.db)
        schema._build_schema(data)
        return schema

    def load_schema(self, schema_id):
        data = self.db.metaphor_schema.find_one({"_id": ObjectId(schema_id)})
        schema = Schema(self.db)
        schema._build_schema(data)
        return schema

    def create_schema(self):
        schema = Schema(self.db)
        schema._id = ObjectId()
        schema.create_initial_schema()
        return schema

    def list_schemas(self):
        schema_data = self.db.metaphor_schema.find()
        schema_list = []
        for data in schema_data:
            schema = Schema(self.db)
            schema._build_schema(data)
            schema_list.append(schema)
        return schema_list
