
import unittest
from datetime import datetime

from metaphor.lrparse.lrparse import parse, parse_filter
from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.schema import Field

from metaphor.updater import Updater


class LRParseTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)
        self.updater = Updater(self.schema)

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')
        self.schema.create_field('employee', 'duration', 'int')
        self.schema.create_field('employee', 'boss', 'link', 'employee')

        self.section_spec = self.schema.create_spec('section')
        self.schema.create_field('section', 'name', 'str')
        self.schema.create_field('section', 'employees', 'linkcollection', 'employee')

        self.division_spec = self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'sections', 'collection', 'section')

        self.schema.create_field('root', 'divisions', 'collection', 'division')
        self.schema.create_field('root', 'employees', 'collection', 'employee')

        self.division_id_1 = self.schema.insert_resource(
            'division', {'name': 'Sales'}, 'divisions')
        self.division_id_2 = self.schema.insert_resource(
            'division', {'name': 'Marketting'}, 'divisions')

        self.employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'Bob', 'age': 44, 'duration': 12}, 'employees')
        self.employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'Ned', 'age': 34, 'duration': 8, 'boss' :self.employee_id_1}, 'employees')
        self.employee_id_3 = self.schema.insert_resource(
            'employee', {'name': 'Ted', 'age': 24, 'duration': 6, 'boss' :self.employee_id_1}, 'employees')

    def test_field(self):
        tree = parse("self.name", self.employee_spec)

        self.assertEqual('Bob', self.updater._calculate_resource(tree, self.employee_id_1))

    def test_calc(self):
        tree = parse("self.age + self.duration", self.employee_spec)

        self.assertEqual(56, self.updater._calculate_resource(tree, self.employee_id_1))
        self.assertEqual(42, self.updater._calculate_resource(tree, self.employee_id_2))

    def test_linked_resource(self):
        tree = parse("self.boss", self.employee_spec)

        boss = self.updater._calculate_resource(tree, self.employee_id_1)
        self.assertEqual('Bob', boss['name'])

    def test_linked_calc(self):
        tree = parse("self.age + (self.boss.duration)", self.employee_spec)

        self.assertEqual(44, self.updater._calculate_resource(tree, self.employee_id_1))
        self.assertEqual(46, self.updater._calculate_resource(tree, self.employee_id_1))

    def test_root_collection(self):
        tree = parse("employees", self.employee_spec)

        employees = self.updater._calculate_resource(tree, self.employee_id_1)
        self.assertEqual(3, len(employees))

    def test_root_collection_filtered(self):
        tree = parse("employees[age>30]", self.employee_spec)

        employees = self.updater._calculate_resource(tree, self.employee_id_1)
        self.assertEqual(2, len(employees))

    def test_ternary(self):
        tree = parse("self.name == 'Bob' -> 12 : 14", self.employee_spec)

        self.assertEqual(12, self.updater._calculate_resource(tree, self.employee_id_1))
        self.assertEqual(14, self.updater._calculate_resource(tree, self.employee_id_2))
        self.assertEqual(14, self.updater._calculate_resource(tree, self.employee_id_3))

    def test_ternary_calcs(self):
        tree = parse("self.boss.name == 'Bob' -> (self.boss.duration) : 99", self.employee_spec)

        self.assertEqual(99, self.updater._calculate_resource(tree, self.employee_id_1))
        self.assertEqual(12, self.updater._calculate_resource(tree, self.employee_id_2))
        self.assertEqual(12, self.updater._calculate_resource(tree, self.employee_id_3))

    def test_switch(self):
        tree = parse("self.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        self.assertEqual(22, self.updater._calculate_resource(tree, self.employee_id_1))
        self.assertEqual(11, self.updater._calculate_resource(tree, self.employee_id_2))
        self.assertEqual(None, self.updater._calculate_resource(tree, self.employee_id_3))

    def test_switch_calc(self):
        tree = parse("self.boss.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        self.assertEqual(None, self.updater._calculate_resource(tree, self.employee_id_1))
        self.assertEqual(22, self.updater._calculate_resource(tree, self.employee_id_2))
        self.assertEqual(22, self.updater._calculate_resource(tree, self.employee_id_3))

    def test_switch_calc_fields(self):
        tree = parse("self.boss.name -> ('Bob': (self.boss.duration), 'Ned': (self.duration), 'Ted': (self.age))", self.employee_spec)

        self.assertEqual(12, self.updater._calculate_resource(tree, self.employee_id_1))
        self.assertEqual(8, self.updater._calculate_resource(tree, self.employee_id_2))
        self.assertEqual(24, self.updater._calculate_resource(tree, self.employee_id_3))

    def _test_function_first(self):
        tree = parse("first(employees)", self.employee_spec)

        first = self.updater._calculate_resource(tree, self.employee_id_1)
        self.assertEqual('Bob', first['name'])
