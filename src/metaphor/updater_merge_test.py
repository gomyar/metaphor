
import unittest

import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.updater import Updater


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)
        self.updater = Updater(self.schema)

        self.employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(self.employee_spec, 'name', 'str')
        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        self.api = Api(self.schema)

        self.db.delete_calc.create_index([
            ('update_id', pymongo.ASCENDING),
            ('resource_id', pymongo.ASCENDING),
        ], unique=True)

    def test_aggregation_merge(self):
        employee_id_1 = self.api.post('employees', {'name': 'ned'})
        employee_id_2 = self.api.post('employees', {'name': 'bob'})

        cursor = self.db.resource_employee.aggregate([
            {"$set": {"update_id": 1}},
            {"$project": {"resource_id": "$_id", "update_id": True, "_id": False}},
            {"$merge": {"into": "delete_calc", "on": ["update_id", "resource_id"], "whenNotMatched": "insert"}},
        ])

        cursor = self.db.resource_employee.aggregate([
            {"$set": {"update_id": 2}},
            {"$project": {"resource_id": "$_id", "update_id": True, "_id": False}},
            {"$merge": {"into": "delete_calc", "on": ["update_id", "resource_id"], "whenNotMatched": "insert"}},
        ])

        employee_id_3 = self.api.post('employees', {'name': 'fred'})

        cursor = self.db.resource_employee.aggregate([
            {"$set": {"update_id": 2}},
            {"$project": {"resource_id": "$_id", "update_id": True, "_id": False}},
            {"$merge": {"into": "delete_calc", "on": ["update_id", "resource_id"], "whenNotMatched": "insert"}},
        ])

        self.assertEqual([], list(cursor))
