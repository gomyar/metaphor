
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.updater import Updater


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.updater = Updater(self.schema)

    def test_updater(self):
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')
        self.schema.add_field(employee_spec, 'age', 'int')

        division_spec = self.schema.add_spec('division')
        self.schema.add_field(division_spec, 'name', 'str')
        self.schema.add_field(division_spec, 'employees', 'collection', 'employee')
        self.schema.add_calc(division_spec, 'older_employees', 'self.employees[age>30]')

        self.schema.add_field(self.schema.root, 'divisions', 'collection', 'division')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'Sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'Bob', 'age': 31}, 'employees', 'division', division_id_1)

        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            'name': 'Sales',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            'older_employees': [ObjectId(employee_id_1[2:])],
        }, division_data)

        employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'Ned', 'age': 41}, 'employees', 'division', division_id_1)

        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            'name': 'Sales',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            'older_employees': [ObjectId(employee_id_1[2:]), ObjectId(employee_id_2[2:])],
        }, division_data)
