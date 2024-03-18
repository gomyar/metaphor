
import unittest

import pymongo
from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()
        self.updater = Updater(self.schema)

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('root', 'employees', 'collection', 'employee')

        self.api = Api(self.db)

        self.db.delete_calc.create_index([
            ('update_id', pymongo.ASCENDING),
            ('resource_id', pymongo.ASCENDING),
        ], unique=True)

    def test_aggregation_merge(self):
        employee_id_1 = self.api.post('employees', {'name': 'ned'})
        employee_id_2 = self.api.post('employees', {'name': 'bob'})

        cursor = self.db.metaphor_resource.aggregate([
            {"$match": {"_type": "employee"}},
            {"$set": {"update_id": 1}},
            {"$project": {"resource_id": "$_id", "update_id": True, "_id": False}},
            {"$merge": {"into": "delete_calc", "on": ["update_id", "resource_id"], "whenNotMatched": "insert"}},
        ])

        cursor = self.db.metaphor_resource.aggregate([
            {"$match": {"_type": "employee"}},
            {"$set": {"update_id": 2}},
            {"$project": {"resource_id": "$_id", "update_id": True, "_id": False}},
            {"$merge": {"into": "delete_calc", "on": ["update_id", "resource_id"], "whenNotMatched": "insert"}},
        ])

        employee_id_3 = self.api.post('employees', {'name': 'fred'})

        cursor = self.db.metaphor_resource.aggregate([
            {"$match": {"_type": "employee"}},
            {"$set": {"update_id": 2}},
            {"$project": {"resource_id": "$_id", "update_id": True, "_id": False}},
            {"$merge": {"into": "delete_calc", "on": ["update_id", "resource_id"], "whenNotMatched": "insert"}},
        ])

        self.assertEqual([], list(cursor))
