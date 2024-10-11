
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

    def load_schema(self, schema_id):
        schema = Schema(self.db)
        schema._build_schema(self.load_schema_data(schema_id))
        return schema

    def _schema_aggregation(self):
        return [
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

    def load_schema_data(self, schema_id):
        aggregate = [
            {"$match": {"_id": ObjectId(schema_id)}},
        ] + self._schema_aggregation() + [
            {"$limit": 1},
        ]
        results = list(self.db.metaphor_schema.aggregate(aggregate))
        return results[0] if results else None

    def create_schema(self):
        schema = Schema.create_schema(self.db)
        schema.create_initial_schema()
        schema.set_as_current()
        return schema

    def delete_schema(self, schema_id):
        result = self.db.metaphor_schema.delete_one({"_id": ObjectId(schema_id), "current": {"$ne": True}})
        return result.acknowledged

    def delete_mutation(self, mutation_id):
        result = self.db.metaphor_mutation.delete_one({"_id": ObjectId(mutation_id), "current": {"$ne": True}})
        return result.acknowledged

    def list_schemas(self):
        for data in self.db.metaphor_schema.aggregate(self._schema_aggregation()):
            schema = Schema(self.db)
            schema._build_schema(data)
            yield schema

    def create_schema_from_import(self, schema_data):
        saved = {
            "root": schema_data['root'],
            "specs": schema_data['specs'],
            "version": schema_data['version'],
            "created": schema_data['created'],
        }
        self.db.metaphor_schema.insert_one(saved)

    def copy_schema_from_id(self, schema_id, name):
        to_copy = self.db.metaphor_schema.find_one({"_id": ObjectId(schema_id)})
        to_copy['current'] = False
        to_copy.pop('_id')
        to_copy['created'] = datetime.now().isoformat()
        to_copy['name'] = name
        self.db.metaphor_schema.insert_one(to_copy)

    def load_mutation(self, mutation_id):
        mutation_id = ObjectId(mutation_id)
        data = self.db.metaphor_mutation.find_one({
            "_id": mutation_id
        })
        return self._create_mutation(data)

    def _create_mutation(self, data):
        from_schema = self.load_schema(data['from_schema_id'])
        to_schema = self.load_schema(data['to_schema_id'])
        mutation = Mutation(from_schema, to_schema)
        mutation._id = data['_id']
        mutation.data_steps = data.get('data_steps') or []
        mutation.steps = data.get('steps') or []
        mutation.state = data.get('state') or 'ready'
        mutation.error = data.get('error')
        return mutation

    def list_ready_mutations(self):
        data = self.db.metaphor_mutation.find()#{"$or": [{"state": "ready"}, {"state": "running"}]})
        return [self._create_mutation(m) for m in data]

    def save_mutation(self, mutation, object_id):
        update = self.db.metaphor_mutation.update_one({"_id": object_id}, {
            "$set": {
                "from_schema_id": mutation.from_schema._id,
                "to_schema_id": mutation.to_schema._id,
                "steps": mutation.steps,
            }
        }, upsert=True)
        mutation._id = str(update.upserted_id)
        return update.upserted_id

