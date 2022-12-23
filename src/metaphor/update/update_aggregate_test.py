
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

        self.section_id_1 = self.schema.insert_resource(
            'section', {'name': 'alpha'}, 'sections', 'division', self.division_id_1)
        self.section_id_2 = self.schema.insert_resource(
            'section', {'name': 'beta'}, 'sections', 'division', self.division_id_2)
        self.section_id_3 = self.schema.insert_resource(
            'section', {'name': 'gamma'}, 'sections', 'division', self.division_id_2)

        self.employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'Bob', 'age': 44, 'duration': 12}, 'employees')
        self.employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'Ned', 'age': 34, 'duration': 8, 'boss' :self.employee_id_1}, 'employees')
        self.employee_id_3 = self.schema.insert_resource(
            'employee', {'name': 'Ted', 'age': 24, 'duration': 6, 'boss' :self.employee_id_1}, 'employees')

        self.schema.create_linkcollection_entry('section', self.section_id_1, 'employees', self.employee_id_1)
        self.schema.create_linkcollection_entry('section', self.section_id_2, 'employees', self.employee_id_2)
        self.schema.create_linkcollection_entry('section', self.section_id_2, 'employees', self.employee_id_3)

    def perform_simple_calc(self, collection, resource_id, calc):
        val = list(collection.aggregate([{"$match": {"_id": self.schema.decodeid(resource_id)}}] + calc.create_aggregation()))
        return val[0]['_val'] if val else None

    def perform_resource_calc(self, collection, resource_id, calc):
        val = list(collection.aggregate([{"$match": {"_id": self.schema.decodeid(resource_id)}}] + calc.create_aggregation()))
        return val

    def test_field(self):
        tree = parse("self.name", self.employee_spec)

        self.assertEqual('Bob', self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree))

    def test_calc(self):
        tree = parse("self.age + self.duration", self.employee_spec)

        self.assertEqual(56, self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree))
        self.assertEqual(42, self.perform_simple_calc(self.db.resource_employee, self.employee_id_2, tree))

    def test_linked_resource(self):
        tree = parse("self.boss", self.employee_spec)

        val = self.perform_resource_calc(self.db.resource_employee, self.employee_id_2, tree)
        self.assertEqual(val[0]['_id'], self.schema.decodeid(self.employee_id_1))

    def test_linked_calc(self):
        tree = parse("self.age + (self.boss.duration)", self.employee_spec)

        self.assertEqual(46, self.perform_simple_calc(self.db.resource_employee, self.employee_id_2, tree))
        self.assertEqual(36, self.perform_simple_calc(self.db.resource_employee, self.employee_id_3, tree))

    def test_root_collection(self):
        tree = parse("employees", self.employee_spec)

        val = self.perform_resource_calc(self.db.resource_employee, self.employee_id_1, tree)
        self.assertEqual(3, len(val))

    def test_root_collection_filtered(self):
        tree = parse("employees[age>30]", self.employee_spec)

        val = self.perform_resource_calc(self.db.resource_employee, self.employee_id_1, tree)
        self.assertEqual(2, len(val))

    def test_multi_collection_filtered(self):
        tree = parse("divisions.sections.employees[age>30]", self.employee_spec)

        val = self.perform_resource_calc(self.db.resource_employee, self.employee_id_1, tree)
        self.assertEqual(2, len(val))

    def test_multi_collection_filtered_at_sections(self):
        tree = parse("divisions.sections[name='alpha'].employees", self.employee_spec)

        val = self.perform_resource_calc(self.db.resource_employee, self.employee_id_1, tree)
        self.assertEqual(1, len(val))
        self.assertEqual('Bob', val[0]['name'])

    def test_ternary(self):
        tree = parse("self.name = 'Bob' -> 12 : 14", self.employee_spec)

        self.assertEqual(12, self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree))

        self.assertEqual(14, self.perform_simple_calc(self.db.resource_employee, self.employee_id_2, tree))

        self.assertEqual(14, self.perform_simple_calc(self.db.resource_employee, self.employee_id_3, tree))

    def test_ternary_calcs(self):
        tree = parse("self.boss.name = 'Bob' -> (self.boss.duration) : 99", self.employee_spec)
        val = self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree)
        self.assertEqual(99, val)

        val = self.perform_simple_calc(self.db.resource_employee, self.employee_id_2, tree)
        self.assertEqual(12, val)

        val = self.perform_simple_calc(self.db.resource_employee, self.employee_id_3, tree)
        self.assertEqual(12, val)

    def test_switch(self):
        tree = parse("self.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        self.assertEqual(22, self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree))

        self.assertEqual(11, self.perform_simple_calc(self.db.resource_employee, self.employee_id_2, tree))

        self.assertEqual(None, self.perform_simple_calc(self.db.resource_employee, self.employee_id_3, tree))

    def test_switch_calc(self):
        tree = parse("self.boss.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        self.assertEqual(None, self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree))

        self.assertEqual(22, self.perform_simple_calc(self.db.resource_employee, self.employee_id_2, tree))

        self.assertEqual(22, self.perform_simple_calc(self.db.resource_employee, self.employee_id_3, tree))

    def test_switch_calc_fields(self):
        tree = parse("self.boss.name -> ('Bob': (self.boss.duration), 'Ned': (self.duration), 'Ted': (self.age))", self.employee_spec)

        self.assertEqual(None, self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree))

        self.assertEqual(12, self.perform_simple_calc(self.db.resource_employee, self.employee_id_2, tree))

        self.assertEqual(12, self.perform_simple_calc(self.db.resource_employee, self.employee_id_3, tree))

    def test_function_first(self):
        tree = parse("first(employees)", self.employee_spec)

        val = self.perform_resource_calc(self.db.resource_employee, self.employee_id_1, tree)
        self.assertEqual(1, len(val))
        self.assertEqual('Bob', val[0]['name'])

    def test_function_sum(self):
        tree = parse("sum(employees.age)", self.employee_spec)

        self.assertEqual(102, self.perform_simple_calc(self.db.resource_employee, self.employee_id_1, tree))
