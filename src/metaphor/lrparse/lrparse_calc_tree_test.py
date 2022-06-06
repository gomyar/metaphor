
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

    def test_single_level(self):
        tree = parse("(self.age + (self.boss.duration)) - (self.boss.age + (self.boss.duration))", self.employee_spec)

        res_tree = tree._create_calc_agg_tree()
        calc_tree = tree._create_agg_tree(res_tree)

        self.assertEqual({
            "_v_self_age": [
                {"$addFields": {"_val": "$_val.age"}},
                {"$addFields": {"_v_self_age": "$_val"}},
            ],
            "_v_self_boss_duration": [
                {"$lookup": {
                    # forced lookup to enable separate pipeline
                    "from": "resource_employee",
                    "as": "_lookup_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        # match self
                        {"$match": {"_id": "$$id"}},

                        # lookup link
                        {"$lookup": {
                            "from": "resource_employee",
                            "localField": "boss",
                            "foreignField": "_id",
                            "as": "_val",
                        }},
                        {"$set": {"_val": {"$arrayElemAt": ["$_val", 0]}}},

                        # field lookup
                        {"$addFields": {"_val": "$_val.duration"}},
                    ]
                }},
                {"$set": {"_v_self_boss_duration": {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
            ],
            "_v_self_boss_age": [
                {"$lookup": {
                    # forced lookup to enable separate pipeline
                    "from": "resource_employee",
                    "as": "_lookup_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        # match self
                        {"$match": {"_id": "$$id"}},

                        # lookup link
                        {"$lookup": {
                            "from": "resource_employee",
                            "localField": "boss",
                            "foreignField": "_id",
                            "as": "_val",
                        }},
                        {"$set": {"_val": {"$arrayElemAt": ["$_val", 0]}}},

                        # field lookup
                        {"$addFields": {"_val": "$_val.age"}},
                    ]
                }},
                {"$set": {"_v_self_boss_age": {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
            ],
            # no repeat for self.boss.duration
        }, calc_tree)

        calc_expr = tree._create_calc_expr()

        self.assertEqual({
            "$subtract": [
                {"$add": ["$_v_self_age", "$_v_self_boss_duration"]},
                {"$add": ["$_v_self_boss_age", "$_v_self_boss_duration"]}
            ]
        }, calc_expr)

    def test_two_levels(self):
        tree = parse("(self.age + (self.boss.duration)) - (self.boss.age + (self.duration - (self.boss.duration)))", self.employee_spec)

        res_tree = tree._create_calc_agg_tree()
        calc_tree = tree._create_agg_tree(res_tree)

        self.assertEqual({
            '_v_self_age': [{'$addFields': {'_val': '$_val.age'}},
                            {'$addFields': {'_v_self_age': '$_val'}}],
            '_v_self_boss_age': [{'$lookup': {'as': '_lookup_val',
                                                'from': 'resource_employee',
                                                'let': {'id': '$_id'},
                                                'pipeline': [{'$match': {'_id': '$$id'}},
                                                            {'$lookup': {'as': '_val',
                                                                        'foreignField': '_id',
                                                                        'from': 'resource_employee',
                                                                        'localField': 'boss'}},
                                                            {'$set': {'_val': {'$arrayElemAt': ['$_val',
                                                                                                0]}}},
                                                            {'$addFields': {'_val': '$_val.age'}}]}},
                                {'$set': {'_v_self_boss_age': {'$arrayElemAt': ['$_lookup_val._val',
                                                                                0]}}}],
            '_v_self_boss_duration': [{'$lookup': {'as': '_lookup_val',
                                                    'from': 'resource_employee',
                                                    'let': {'id': '$_id'},
                                                    'pipeline': [{'$match': {'_id': '$$id'}},
                                                                {'$lookup': {'as': '_val',
                                                                            'foreignField': '_id',
                                                                            'from': 'resource_employee',
                                                                            'localField': 'boss'}},
                                                                {'$set': {'_val': {'$arrayElemAt': ['$_val',
                                                                                                    0]}}},
                                                                {'$addFields': {'_val': '$_val.duration'}}]}},
                                        {'$set': {'_v_self_boss_duration': {'$arrayElemAt': ['$_lookup_val._val',
                                                                                            0]}}}],
            '_v_self_duration': [{'$addFields': {'_val': '$_val.duration'}},
                                {'$addFields': {'_v_self_duration': '$_val'}}]}, calc_tree)

        calc_expr = tree._create_calc_expr()

        self.assertEqual({
            "$subtract": [
                {"$add": ["$_v_self_age", "$_v_self_boss_duration"]},
                {"$add": ["$_v_self_boss_age", {"$subtract": [
                    "$_v_self_duration", "$_v_self_boss_duration",
                ]}]}
            ]
        }, calc_expr)
