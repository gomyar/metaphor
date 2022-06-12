
import unittest
from datetime import datetime

from .lrparse import parse, parse_filter
from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.schema import Field

from .lrparse import FieldRef, ResourceRef


class LRParseTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

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

    def test_field(self):
        tree = parse("self.name", self.employee_spec)

        self.assertEqual([{"$project": {"_val": "$name"}}], tree.create_aggregation(None))

    def test_calc(self):
        tree = parse("self.age + self.duration", self.employee_spec)

        expected = [
            {"$addFields": {"_val": "$age"}},
            {"$addFields": {"_lhs": "$_val"}},

            {"$addFields": {"_val": "$age"}},
            {"$addFields": {"_rhs": "$_val"}},

            {"$project": {"_val": {"$add": ["$_lhs", "$_rhs"]}}},
        ]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_linked_resource(self):
        tree = parse("self.boss", self.employee_spec)

        expected = [
            {"$lookup": {
                "from": 'resource_employee',
                "as": '_val',
                "let": {"v_boss": "$boss"},
                "pipeline":[
                    {"$match": {"$expr": {"$eq": ["$_id", "$$v_boss"]}}}
                ]
            }},
            {"$set": {"_val": {"$arrayElemAt": ["$_val.age", 0]}}},
        ]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_linked_calc(self):
        tree = parse("self.age + (self.boss.duration)", self.employee_spec)

        expected = [
            {"$addFields": {"_val": "$age"}},
            {"$addFields": {"_lhs": "$_val"}},

            {"$lookup": {
                "from": 'resource_employee',
                "as": '_val',
                "let": {"v_boss": "$boss"},
                "pipeline":[
                    {"$match": {"$expr": {"$eq": ["$_id", "$$v_boss"]}}}
                ]
            }},
            {"$set": {"_val": {"$arrayElemAt": ["$_val", 0]}}},

            {"$addFields": {"_val": "$duration"}},
            {"$addFields": {"_rhs": "$_val"}},

            {"$project": {"_val": {"$add": ["$_lhs", "$_rhs"]}}},
        ]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_root_collection(self):
        tree = parse("employees", self.employee_spec)

        expected = [
            {"$lookup": {
                "from": 'resource_employee',
                "as": '_val',
                "pipeline":[
                    {"$match": {
                        "_parent_field_name": "employees",
                        "_parent_canonical_url": "/",
                    }}
                ]
            }},
#            {"$group": {"_id": "$_val"}},
#            {"$unwind": "$_id"},
#            {"$replaceRoot": {"newRoot": "$_id"}},
        ]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_root_collection_filtered(self):
        tree = parse("employees[age>4]", self.employee_spec)

        expected = [
            {"$lookup": {
                "from": 'resource_employee',
                "as": '_val',
                "pipeline":[
                    {"$match": {
                        "_parent_field_name": "employees",
                        "_parent_canonical_url": "/",
                    }},
                ]
            }},

            {"$match": {
                "_val.age": {"$gt": 4},
            }},
        ]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_ternary(self):
        tree = parse("self.name == 'Bob' -> 12 : 14", self.employee_spec)

        resource_tree = tree._create_calc_agg_tree()
        agg_tree = tree._create_agg_tree(resource_tree, user)


        self.assertEqual({}, resource_tree)
        self.assertEqual({}, agg_tree)
        expected = []
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_ternary_calcs(self):
        tree = parse("self.boss.name == 'Bob' -> (self.boss.duration) : 99", self.employee_spec)

        expected = []
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_switch(self):
        tree = parse("self.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        expected = []
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_switch_calc(self):
        tree = parse("self.boss.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        expected = []
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_switch_calc_fields(self):
        tree = parse("self.boss.name -> ('Bob': (self.boss.duration), 'Ned': (self.duration), 'Ted': (self.age))", self.employee_spec)

        expected = []
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_function(self):
        tree = parse("first(employees.age)", self.employee_spec)

        expected = [
            {"$lookup": {
                "from": 'resource_employee',
                "as": '_val',
                "pipeline":[
                    {"$match": {
                        "_parent_field_name": "employees",
                        "_parent_canonical_url": "/",
                    }},
                ]
            }},

            {"$group": {"_id": "$_val"}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},

            {"$project": {"_val": "$name"}},

            {"$set": {"_val": {"$arrayElemAt": ["$_val", 0]}}},
        ]
        self.assertEqual(expected, tree.create_aggregation(None))
