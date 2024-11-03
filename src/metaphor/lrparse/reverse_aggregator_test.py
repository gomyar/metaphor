
import unittest

from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater
from .lrparse import parse
from .reverse_aggregator import ReverseAggregator


class AggregatorTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.updater = Updater(self.schema)

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')

        self.section_spec = self.schema.create_spec('section')
        self.schema.create_field('section', 'name', 'str')
        self.schema.create_field('section', 'members', 'linkcollection', 'employee')

        self.division_spec = self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'employees', 'collection', 'employee')
        self.schema.create_field('division', 'sections', 'collection', 'section')

        self.schema.create_field('root', 'divisions', 'collection', 'division')

        self.aggregator = ReverseAggregator(self.schema)

    def test_simple(self):
        tree = parse('self.employees.age', self.schema.specs['division'])

        employee_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'employee', employee_id, 'division', 'all_employees_age')

        self.assertEqual([
            [{'$lookup': {'as': '_field_employees',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_parent_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$match': {'_type': 'division'}}]], aggregations)

    def test_middle_of_calc(self):
        tree = parse('divisions.sections.members.age', self.schema.specs['division'])

        section_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'section', section_id, 'division', 'all_ages')

        self.assertEqual([
            [{'$lookup': {'as': '_field_sections',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_parent_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_sections'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$lookup': {'as': '_field_all_ages',
                        'from': 'metaphor_resource',
                        'pipeline': [{'$match': {'_type': 'division'}}]}},
            {'$group': {'_id': '$_field_all_ages'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]], aggregations)

    def test_double(self):
        tree = parse('self.employees.parent_division_employees.sections.members.age', self.schema.specs['division'])

        employee_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'employee', employee_id)

        self.assertEqual([
            [{'$lookup': {'as': '_field_members',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$in': [{'_id': '$$id'},
                                                                                {'$ifNull': ['$members',
                                                                                            []]}]},
                                                                    {'$eq': ['$_type',
                                                                                'section']}]}}}]}},
            {'$group': {'_id': '$_field_members'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$lookup': {'as': '_field_sections',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_parent_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_sections'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$lookup': {'as': '_field_parent_division_employees',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_parent_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'employee']}]}}}]}},
            {'$group': {'_id': '$_field_parent_division_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$lookup': {'as': '_field_employees',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_parent_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$lookup': {'as': '_field_employees',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_parent_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$match': {'_type': None}}]], aggregations)

    def test_calc(self):
        tree = parse('max(self.employees.age) + min(divisions.employees.age) + 10', self.schema.specs['division'])

        employee_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'employee', employee_id, 'section', 'age_calc')

        self.assertEqual([
            [{'$lookup': {'as': '_field_employees',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_parent_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$match': {'_type': 'section'}}],
            [{'$lookup': {'as': '_field_employees',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_parent_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$_id',
                                                                                '$$id']},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$lookup': {'as': '_field_age_calc',
                        'from': 'metaphor_resource',
                        'pipeline': [{'$match': {'_type': 'section'}}]}},
            {'$group': {'_id': '$_field_age_calc'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]], aggregations)

    def test_double_aggregate(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        section_id_1 = self.schema.insert_resource('section', {'name': 'Sales'}, 'sections', 'division', division_id_1)

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 10}, 'employees', 'division', division_id_1)
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 10}, 'employees', 'division', division_id_1)

        self.schema.create_linkcollection_entry('section', section_id_1, 'members', employee_id_1)

        tree = parse('self.employees.parent_division_employees.sections.members.age', self.schema.specs['division'])

        aggregations = self.aggregator.get_for_resource(tree, 'employee', self.schema.decodeid(employee_id_1))

        result = self.schema.db['metaphor_resource'].aggregate(aggregations[0])
        self.assertEqual({
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'name': 'Sales'}, next(result))

    def test_ternary_aggregate(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        section_id_1 = self.schema.insert_resource('section', {'name': 'Sales'}, 'sections', 'division', division_id_1)

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 10}, 'employees', 'division', division_id_1)
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 10}, 'employees', 'division', division_id_1)

        self.schema.create_linkcollection_entry('section', section_id_1, 'members', employee_id_1)

        tree = parse('max(self.employees.age) > 20 -> max(self.employees.age) : min(divisions.sections.members.age)', self.schema.specs['division'])

        aggregations = self.aggregator.get_for_resource(tree, 'employee', self.schema.decodeid(employee_id_1))

        result = self.schema.db['metaphor_resource'].aggregate(aggregations[0])
        self.assertEqual({
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'name': 'Sales'}, next(result))

    def test_simple_root(self):
        # uncertain what case this is testing exactly - section has no link to division
        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        tree = parse('max(divisions.name)', self.schema.root)

        aggregations = self.aggregator.get_for_resource(tree, 'division', self.schema.decodeid(division_id_1), 'division', 'max_divisions_name')

        self.assertEqual([[{'$lookup': {'as': '_field_max_divisions_name',
                        'from': 'metaphor_resource',
                        'pipeline': [{'$match': {'_type': 'division'}}]}},
            {'$group': {'_id': '$_field_max_divisions_name'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]], aggregations)

