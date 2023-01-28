
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

        self.assertEqual([{'$addFields': {'_val': '$name'}}], tree.create_aggregation(None))

    def test_calc(self):
        tree = parse("self.age + self.duration", self.employee_spec)

        expected = [{'$addFields': {'_val': '$age'}},
            {'$addFields': {'_v_self_age': '$_val'}},
            {'$addFields': {'_val': '$duration'}},
            {'$addFields': {'_v_self_duration': '$_val'}},
            {'$addFields': {'_val': {'$add': [{'$ifNull': ['$_v_self_age', 0]},
                                            {'$ifNull': ['$_v_self_duration', 0]}]}}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_linked_resource(self):
        tree = parse("self.boss", self.employee_spec)

        expected = [{'$lookup': {'as': '_val',
                        'from': 'resource_employee',
                        'let': {'s_id': '$boss'},
                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$s_id']}}},
                                    {'$match': {'_deleted': {'$exists': False}}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_linked_calc(self):
        tree = parse("self.age + (self.boss.duration)", self.employee_spec)


        expected = [{'$addFields': {'_val': '$age'}},
            {'$addFields': {'_v_self_age': '$_val'}},
            {'$lookup': {'as': '_lookup_val',
                        'from': 'resource_employee',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$id']}}},
                                    {'$lookup': {'as': '_val',
                                                    'from': 'resource_employee',
                                                    'let': {'s_id': '$boss'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$s_id']}}},
                                                                {'$match': {'_deleted': {'$exists': False}}}]}},
                                    {'$group': {'_id': '$_val'}},
                                    {'$unwind': '$_id'},
                                    {'$replaceRoot': {'newRoot': '$_id'}},
                                    {'$addFields': {'_val': '$duration'}}]}},
            {'$set': {'_v_self_boss_duration': {'$arrayElemAt': ['$_lookup_val._val',
                                                                0]}}},
            {'$addFields': {'_val': {'$add': [{'$ifNull': ['$_v_self_age', 0]},
                                            {'$ifNull': ['$_v_self_boss_duration',
                                                            0]}]}}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_root_collection(self):
        tree = parse("employees", self.employee_spec)

        expected = [{'$lookup': {'as': '_val',
                        'from': 'resource_employee',
                        'pipeline': [{'$match': {'_deleted': {'$exists': False},
                                                '_parent_canonical_url': '/',
                                                '_parent_field_name': 'employees'}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_root_collection_filtered(self):
        tree = parse("employees[age>4]", self.employee_spec)

        expected = [{'$lookup': {'as': '_val',
                        'from': 'resource_employee',
                        'pipeline': [{'$match': {'_deleted': {'$exists': False},
                                                '_parent_canonical_url': '/',
                                                '_parent_field_name': 'employees'}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'age': {'$gt': 4}}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_ternary(self):
        tree = parse("self.name = 'Bob' -> 12 : 14", self.employee_spec)

        expected = [
            {'$addFields': {'_val': '$name'}},
            {'$addFields': {'_v_self_name': '$_val'}},
            {'$addFields': {'_val': 'Bob'}},
            {'$addFields': {'_v_0': '$_val'}},
            {'$addFields': {'_val': {'$eq': ['$_v_self_name', 'Bob']}}},
            {'$addFields': {'_if': '$_val'}},
            {'$addFields': {'_val': 12}},
            {'$addFields': {'_then': '$_val'}},
            {'$addFields': {'_val': 14}},
            {'$addFields': {'_else': '$_val'}},
            {'$addFields': {'_val': {'$cond': {'else': '$_else',
                                                'if': '$_if',
                                                'then': '$_then'}}}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$addFields': {'_val': '$_id'}},
        ]

        self.assertEqual(expected, tree.create_aggregation(None))

    def test_ternary_calcs(self):
        tree = parse("self.boss.name = 'Bob' -> (self.boss.duration) : 99", self.employee_spec)

        expected = [{'$lookup': {'as': '_lookup_val',
                        'from': 'resource_employee',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$id']}}},
                                    {'$lookup': {'as': '_val',
                                                    'from': 'resource_employee',
                                                    'let': {'s_id': '$boss'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$s_id']}}},
                                                                {'$match': {'_deleted': {'$exists': False}}}]}},
                                    {'$group': {'_id': '$_val'}},
                                    {'$unwind': '$_id'},
                                    {'$replaceRoot': {'newRoot': '$_id'}},
                                    {'$addFields': {'_val': '$name'}}]}},
            {'$set': {'_v_self_boss_name': {'$arrayElemAt': ['$_lookup_val._val', 0]}}},
            {'$addFields': {'_val': 'Bob'}},
            {'$addFields': {'_v_0': '$_val'}},
            {'$addFields': {'_val': {'$eq': ['$_v_self_boss_name', 'Bob']}}},
            {'$addFields': {'_if': '$_val'}},
            {'$lookup': {'as': '_lookup_val',
                        'from': 'resource_employee',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$id']}}},
                                    {'$lookup': {'as': '_val',
                                                    'from': 'resource_employee',
                                                    'let': {'s_id': '$boss'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$s_id']}}},
                                                                {'$match': {'_deleted': {'$exists': False}}}]}},
                                    {'$group': {'_id': '$_val'}},
                                    {'$unwind': '$_id'},
                                    {'$replaceRoot': {'newRoot': '$_id'}},
                                    {'$addFields': {'_val': '$duration'}}]}},
            {'$addFields': {'_then': {'$arrayElemAt': ['$_lookup_val._val', 0]}}},
            {'$addFields': {'_val': 99}},
            {'$addFields': {'_else': '$_val'}},
            {'$addFields': {'_val': {'$cond': {'else': '$_else',
                                                'if': '$_if',
                                                'then': '$_then'}}}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$addFields': {'_val': '$_id'}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_switch(self):
        tree = parse("self.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        expected = [{'$addFields': {'_val': '$name'}},
            {'$addFields': {'_switch_val': '$_val'}},
            {'$addFields': {'_val': 22}},
            {'$addFields': {'_case_0': '$_val'}},
            {'$addFields': {'_val': 11}},
            {'$addFields': {'_case_1': '$_val'}},
            {'$addFields': {'_val': 4}},
            {'$addFields': {'_case_2': '$_val'}},
            {'$addFields': {'_val': {'$switch': {'branches': [{'case': {'$eq': ['Bob',
                                                                                '$_switch_val']},
                                                                'then': '$_case_0'},
                                                            {'case': {'$eq': ['Ned',
                                                                                '$_switch_val']},
                                                                'then': '$_case_1'},
                                                            {'case': {'$eq': ['Fred',
                                                                                '$_switch_val']},
                                                                'then': '$_case_2'}],
                                                'default': None}}}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$addFields': {'_val': '$_id'}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_switch_calc(self):
        tree = parse("self.boss.name -> ('Bob': 22, 'Ned': 11, 'Fred': 4)", self.employee_spec)

        expected = [{'$lookup': {'as': '_switch_lookup_val',
                        'from': 'resource_employee',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$id']}}},
                                    {'$lookup': {'as': '_val',
                                                    'from': 'resource_employee',
                                                    'let': {'s_id': '$boss'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$s_id']}}},
                                                                {'$match': {'_deleted': {'$exists': False}}}]}},
                                    {'$group': {'_id': '$_val'}},
                                    {'$unwind': '$_id'},
                                    {'$replaceRoot': {'newRoot': '$_id'}},
                                    {'$addFields': {'_val': '$name'}}]}},
            {'$addFields': {'_switch_val': {'$arrayElemAt': ['$_switch_lookup_val._val',
                                                            0]}}},
            {'$addFields': {'_val': 22}},
            {'$addFields': {'_case_0': '$_val'}},
            {'$addFields': {'_val': 11}},
            {'$addFields': {'_case_1': '$_val'}},
            {'$addFields': {'_val': 4}},
            {'$addFields': {'_case_2': '$_val'}},
            {'$addFields': {'_val': {'$switch': {'branches': [{'case': {'$eq': ['Bob',
                                                                                '$_switch_val']},
                                                                'then': '$_case_0'},
                                                            {'case': {'$eq': ['Ned',
                                                                                '$_switch_val']},
                                                                'then': '$_case_1'},
                                                            {'case': {'$eq': ['Fred',
                                                                                '$_switch_val']},
                                                                'then': '$_case_2'}],
                                                'default': None}}}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$addFields': {'_val': '$_id'}}]
        self.assertEqual(expected, tree.create_aggregation(None))

    def test_switch_calc_fields(self):
        tree = parse("self.boss.name -> ('Bob': (self.boss.duration), 'Ned': (self.duration), 'Ted': (self.age))", self.employee_spec)

        expected = [{'$lookup': {'as': '_switch_lookup_val',
                        'from': 'resource_employee',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$id']}}},
                                    {'$lookup': {'as': '_val',
                                                    'from': 'resource_employee',
                                                    'let': {'s_id': '$boss'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$s_id']}}},
                                                                {'$match': {'_deleted': {'$exists': False}}}]}},
                                    {'$group': {'_id': '$_val'}},
                                    {'$unwind': '$_id'},
                                    {'$replaceRoot': {'newRoot': '$_id'}},
                                    {'$addFields': {'_val': '$name'}}]}},
            {'$addFields': {'_switch_val': {'$arrayElemAt': ['$_switch_lookup_val._val',
                                                            0]}}},
            {'$lookup': {'as': '_case_lookup_val',
                        'from': 'resource_employee',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$id']}}},
                                    {'$lookup': {'as': '_val',
                                                    'from': 'resource_employee',
                                                    'let': {'s_id': '$boss'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$s_id']}}},
                                                                {'$match': {'_deleted': {'$exists': False}}}]}},
                                    {'$group': {'_id': '$_val'}},
                                    {'$unwind': '$_id'},
                                    {'$replaceRoot': {'newRoot': '$_id'}},
                                    {'$addFields': {'_val': '$duration'}}]}},
            {'$addFields': {'_case_0': {'$arrayElemAt': ['$_case_lookup_val._val', 0]}}},
            {'$addFields': {'_val': '$duration'}},
            {'$addFields': {'_case_1': '$_val'}},
            {'$addFields': {'_val': '$age'}},
            {'$addFields': {'_case_2': '$_val'}},
            {'$addFields': {'_val': {'$switch': {'branches': [{'case': {'$eq': ['Bob',
                                                                                '$_switch_val']},
                                                                'then': '$_case_0'},
                                                            {'case': {'$eq': ['Ned',
                                                                                '$_switch_val']},
                                                                'then': '$_case_1'},
                                                            {'case': {'$eq': ['Ted',
                                                                                '$_switch_val']},
                                                                'then': '$_case_2'}],
                                                'default': None}}}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$addFields': {'_val': '$_id'}}]
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
                        '_deleted': {'$exists': False},
                    }},
                ]
            }},

            {"$group": {"_id": "$_val"}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},

            {'$addFields': {'_val': '$age'}},

            {"$set": {"_val": {"$arrayElemAt": ["$_val", 0]}}},
        ]
        self.assertEqual(expected, tree.create_aggregation(None))
