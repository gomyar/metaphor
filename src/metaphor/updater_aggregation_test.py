
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
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.employees[age>30]')

        self.schema.add_field(self.schema.root, 'divisions', 'collection', 'division')
        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

    def test_update_only_linked_resources(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 10}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 10}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales'}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting'}, 'divisions')

        self.schema.create_linkcollection_entry('division', division_id_1, 'employees', employee_id_1)
        self.schema.create_linkcollection_entry('division', division_id_2, 'employees', employee_id_1)
        self.schema.create_linkcollection_entry('division', division_id_2, 'employees', employee_id_2)

        self.assertEqual([
            self.schema.decodeid(division_id_1),
            self.schema.decodeid(division_id_2)], self.updater.get_affected_ids_for_resource('division', 'older_employees', self.employee_spec, employee_id_1))
        self.assertEqual([
            self.schema.decodeid(division_id_2)], self.updater.get_affected_ids_for_resource('division', 'older_employees', self.employee_spec, employee_id_2))
