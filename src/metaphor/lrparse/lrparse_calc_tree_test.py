
import unittest
from datetime import datetime

from .lrparse import parse, parse_filter
from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.schema import Field

from .lrparse import FieldRef, ResourceRef


class LRParseTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

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

        self.assertEqual({'_v_self_age': [{'$addFields': {'_val': '$age'}},
                            {'$addFields': {'_v_self_age': '$_val'}}],
            '_v_self_boss_age': [{'$lookup': {'as': '_lookup_val',
                                            'from': 'resource_employee',
                                            'let': {'id': '$_id'},
                                            'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                        '$$id']}}},
                                                            {'$lookup': {'as': '_val',
                                                                        'from': 'resource_employee',
                                                                        'let': {'s_id': '$boss'},
                                                                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                                                    '$$s_id']}}},
                                                                                    {'$match': {'_deleted': {'$exists': False}}}]}},
                                                            {'$group': {'_id': '$_val'}},
                                                            {'$unwind': '$_id'},
                                                            {'$replaceRoot': {'newRoot': '$_id'}},
                                                            {'$addFields': {'_val': '$age'}}]}},
                                {'$set': {'_v_self_boss_age': {'$arrayElemAt': ['$_lookup_val._val',
                                                                                0]}}}],
            '_v_self_boss_duration': [{'$lookup': {'as': '_lookup_val',
                                                    'from': 'resource_employee',
                                                    'let': {'id': '$_id'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$id']}}},
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
                                                                                            0]}}}]}, calc_tree)

        calc_expr = tree._create_calc_expr()

        self.assertEqual({'$subtract': [{'$ifNull': [{'$add': [{'$ifNull': ['$_v_self_age', 0]},
                                      {'$ifNull': ['$_v_self_boss_duration',
                                                   0]}]},
                            0]},
               {'$ifNull': [{'$add': [{'$ifNull': ['$_v_self_boss_age', 0]},
                                      {'$ifNull': ['$_v_self_boss_duration',
                                                   0]}]},
                            0]}]}, calc_expr)

    def test_two_levels(self):
        tree = parse("(self.age + (self.boss.duration)) - (self.boss.age + (self.duration - (self.boss.duration)))", self.employee_spec)

        res_tree = tree._create_calc_agg_tree()
        calc_tree = tree._create_agg_tree(res_tree)

        self.assertEqual({'_v_self_age': [{'$addFields': {'_val': '$age'}},
                            {'$addFields': {'_v_self_age': '$_val'}}],
            '_v_self_boss_age': [{'$lookup': {'as': '_lookup_val',
                                            'from': 'resource_employee',
                                            'let': {'id': '$_id'},
                                            'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                        '$$id']}}},
                                                            {'$lookup': {'as': '_val',
                                                                        'from': 'resource_employee',
                                                                        'let': {'s_id': '$boss'},
                                                                        'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                                                    '$$s_id']}}},
                                                                                    {'$match': {'_deleted': {'$exists': False}}}]}},
                                                            {'$group': {'_id': '$_val'}},
                                                            {'$unwind': '$_id'},
                                                            {'$replaceRoot': {'newRoot': '$_id'}},
                                                            {'$addFields': {'_val': '$age'}}]}},
                                {'$set': {'_v_self_boss_age': {'$arrayElemAt': ['$_lookup_val._val',
                                                                                0]}}}],
            '_v_self_boss_duration': [{'$lookup': {'as': '_lookup_val',
                                                    'from': 'resource_employee',
                                                    'let': {'id': '$_id'},
                                                    'pipeline': [{'$match': {'$expr': {'$eq': ['$_id',
                                                                                            '$$id']}}},
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
                                                                                            0]}}}],
            '_v_self_duration': [{'$addFields': {'_val': '$duration'}},
                                {'$addFields': {'_v_self_duration': '$_val'}}]}, calc_tree)
        calc_expr = tree._create_calc_expr()

        self.assertEqual({'$subtract': [{'$ifNull': [{'$add': [{'$ifNull': ['$_v_self_age', 0]},
                                      {'$ifNull': ['$_v_self_boss_duration',
                                                   0]}]},
                            0]},
               {'$ifNull': [{'$add': [{'$ifNull': ['$_v_self_boss_age', 0]},
                                      {'$ifNull': [{'$subtract': [{'$ifNull': ['$_v_self_duration',
                                                                               0]},
                                                                  {'$ifNull': ['$_v_self_boss_duration',
                                                                               0]}]},
                                                   0]}]},
                            0]}]}, calc_expr)

