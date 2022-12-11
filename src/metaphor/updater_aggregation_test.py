
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.updater import Updater
from metaphor.lrparse.lrparse import parse


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
        self.schema.add_field(self.employee_spec, 'age', 'int')

        self.division_spec = self.schema.add_spec('division')
        self.schema.add_field(self.division_spec, 'name', 'str')
        self.schema.add_field(self.division_spec, 'employees', 'linkcollection', 'employee')
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.employees[age>9]')

        self.schema.add_field(self.schema.root, 'divisions', 'collection', 'division')
        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

    def test_update_only_linked_resources(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 10}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 10}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales'}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting'}, 'divisions')

        self.updater.create_linkcollection_entry('division', division_id_1, 'employees', employee_id_1)
        self.updater.create_linkcollection_entry('division', division_id_2, 'employees', employee_id_1)
        self.updater.create_linkcollection_entry('division', division_id_2, 'employees', employee_id_2)

        employee_1 = self.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_1)})
        employee_2 = self.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_2)})

        division_1 = self.db['resource_division'].find_one({"_id": self.schema.decodeid(division_id_1)})
        division_2 = self.db['resource_division'].find_one({"_id": self.schema.decodeid(division_id_2)})

        self.assertEqual([{"_id": self.schema.decodeid(employee_id_1)}], division_1['older_employees'])
        self.assertEqual([{"_id": self.schema.decodeid(employee_id_1)}, {"_id": self.schema.decodeid(employee_id_2)}], division_2['older_employees'])
