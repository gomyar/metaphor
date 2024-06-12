
from datetime import datetime

from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.mutation import Mutation
from metaphor.lrparse.lrparse import parse


class SchemaFactory:
    def __init__(self, db):
        self.db = db

    def load_current_schema(self):
        data = self.db.metaphor_schema.find_one({"current": True})
        data['id'] = str(data['_id'])
        schema = Schema(self.db)
        schema._build_schema(data)
        return schema

    def load_schema(self, schema_id, include_mutations=False):
        schema = Schema(self.db)
        schema._build_schema(self.load_schema_data(schema_id, include_mutations))
        return schema

    def _schema_aggregation(self):
        return [
            {"$lookup": {
                "from": "metaphor_mutation",
                "as": "mutations",
                "let": {"id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {
                        "$eq": ["$to_schema_id", "$$id"],
                    }}},
                    {"$addFields": {
                        "id": {"$toString": "$_id"},
                        "from_schema_id": {"$toString": "$from_schema_id"},
                        "to_schema_id": {"$toString": "$to_schema_id"},
                    }},
                    {"$project": {
                        "_id": 0,
                    }},
                ]
            }},
            {"$addFields": {
                "id": {"$toString": "$_id"},
            }},
            {"$project": {
                "_id": 0,
            }},
            {"$sort": {
                "created": -1
            }},
        ]

    def load_schema_data(self, schema_id, include_mutations=False):
        aggregate = [
            {"$match": {"_id": ObjectId(schema_id)}},
        ] + self._schema_aggregation()
        return next(self.db.metaphor_schema.aggregate(aggregate))

    def create_schema(self):
        schema = Schema.create_schema(self.db)
        schema.create_initial_schema()
        schema.set_as_current()
        return schema

    def delete_schema(self, schema_id):
        result = self.db.metaphor_schema.delete_one({"_id": ObjectId(schema_id), "current": {"$ne": True}})
        return result.acknowledged

    def list_schemas(self):
        return list(self.db.metaphor_schema.aggregate(self._schema_aggregation()))

    def create_schema_from_import(self, schema_data):
        saved = {
            "root": schema_data['root']['fields'],
            "specs": schema_data['specs'],
            "version": schema_data['version'],
            "created": schema_data['created'],
        }
        self.db.metaphor_schema.insert_one(saved)

    def copy_schema_from_id(self, schema_id):
        to_copy = self.db.metaphor_schema.find_one({"_id": ObjectId(schema_id)})
        to_copy['current'] = False
        to_copy.pop('_id')
        to_copy['created'] = datetime.now().isoformat()
        self.db.metaphor_schema.insert_one(to_copy)

    def load_mutation(self, mutation_id):
        mutation_id = ObjectId(mutation_id)
        data = self.db.metaphor_mutation.find_one({
            "_id": mutation_id
        })
        from_schema = self.load_schema(data['from_schema_id'])
        to_schema = self.load_schema(data['to_schema_id'])
        mutation = Mutation(from_schema, to_schema)
        mutation._id = mutation_id
        mutation.data_steps = data.get('data_steps') or []
        return mutation

    def create_mutation(self, mutation):
        self.db.metaphor_mutation.insert_one({
            "from_schema_id": mutation.from_schema._id,
            "to_schema_id": mutation.to_schema._id,
            "data_steps": [],
        })

    def save_mutation(self, mutation):
        self.db.metaphor_mutation.update_one({"_id": ObjectId(mutation.id)}, {
            "from_schema_id": mutation.from_schema._id,
            "to_schema_id": mutation.to_schema._id,
            "data_steps": mutation.data_steps,
        })
