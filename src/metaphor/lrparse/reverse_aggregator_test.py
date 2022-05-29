
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.updater import Updater
from .lrparse import parse
from .reverse_aggregator import ReverseAggregator


class AggregatorTest(unittest.TestCase):
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

        self.section_spec = self.schema.add_spec('section')
        self.schema.add_field(self.section_spec, 'name', 'str')
        self.schema.add_field(self.section_spec, 'members', 'linkcollection', 'employee')

        self.division_spec = self.schema.add_spec('division')
        self.schema.add_field(self.division_spec, 'name', 'str')
        self.schema.add_field(self.division_spec, 'employees', 'collection', 'employee')
        self.schema.add_field(self.division_spec, 'sections', 'collection', 'section')

        self.schema.add_field(self.schema.root, 'divisions', 'collection', 'division')

        self.aggregator = ReverseAggregator(self.schema)

    def test_simple(self):
        tree = parse('self.employees.age', self.schema.specs['division'])

        employee_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'employee', employee_id, 'division', 'all_employees_age')

        self.assertEqual([[
            {'$match': {'_id': employee_id}},
            {'$lookup': {'as': '_field_employees',
             'foreignField': '_id',
             'from': 'resource_division',
             'localField': '_parent_id'}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]], aggregations)

    def test_middle_of_calc(self):
        tree = parse('divisions.sections.members.age', self.schema.specs['division'])

        section_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'section', section_id, 'division', 'all_ages')

        self.assertEqual([[
            {'$match': {'_id': section_id}},
            {'$lookup': {'as': '_field_sections',
             'foreignField': '_id',
             'from': 'resource_division',
             'localField': '_parent_id'}},
            {'$group': {'_id': '$_field_sections'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
           [{'$lookup': {'as': '_field_all_ages',
                            'from': 'resource_division',
                            'pipeline': []}},
            {'$group': {'_id': '$_field_all_ages'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]], aggregations)

    def test_double(self):
        tree = parse('self.employees.parent_division_employees.sections.members.age', self.schema.specs['division'])

        employee_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'employee', employee_id)

        self.assertEqual([
            [
                {'$match': {'_id': employee_id}},

                {'$lookup': {'as': '_field_members',
                'foreignField': 'members._id',
                'from': 'resource_section',
                'localField': '_id'}},
                {'$group': {'_id': '$_field_members'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},

                {'$lookup': {'as': '_field_sections',
                'foreignField': '_id',
                'from': 'resource_division',
                'localField': '_parent_id'}},
                {'$group': {'_id': '$_field_sections'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},

                {'$lookup': {'as': '_field_parent_division_employees',
                'foreignField': '_parent_id',
                'from': 'resource_employee',
                'localField': '_id'}},
                {'$group': {'_id': '$_field_parent_division_employees'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},

                {'$lookup': {'as': '_field_employees',
                'foreignField': '_id',
                'from': 'resource_division',
                'localField': '_parent_id'}},
                {'$group': {'_id': '$_field_employees'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},
            ],
            [
                {'$match': {'_id': employee_id}},

                {'$lookup': {'as': '_field_employees',
                'foreignField': '_id',
                'from': 'resource_division',
                'localField': '_parent_id'}},
                {'$group': {'_id': '$_field_employees'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},
            ],
        ], aggregations)

    def test_calc(self):
        tree = parse('max(self.employees.age) + min(divisions.employees.age) + 10', self.schema.specs['division'])

        employee_id = ObjectId()
        aggregations = self.aggregator.get_for_resource(tree, 'employee', employee_id, 'section', 'age_calc')

        self.assertEqual([
            [
                {'$match': {'_id': employee_id}},
                {'$lookup': {'as': '_field_employees',
                'foreignField': '_id',
                'from': 'resource_division',
                'localField': '_parent_id'}},
                {'$group': {'_id': '$_field_employees'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},

            ],
            # TODO: if two of the aggregations are the same, remove the second one:
            [
                {'$match': {'_id': employee_id}},

                {'$lookup': {'as': '_field_employees',
                'foreignField': '_id',
                'from': 'resource_division',
                'localField': '_parent_id'}},
                {'$group': {'_id': '$_field_employees'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},

            ],
            [
                {'$lookup': {'as': '_field_age_calc',
                             'from': 'resource_section',
                             'pipeline': []}},
                {'$group': {'_id': '$_field_age_calc'}},
                {'$unwind': '$_id'},
                {'$replaceRoot': {'newRoot': '$_id'}},
            ]
        ], aggregations)

    def test_double_aggregate(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        section_id_1 = self.schema.insert_resource('section', {'name': 'Sales'}, 'sections', 'division', division_id_1)

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 10}, 'employees', 'division', division_id_1)
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 10}, 'employees', 'division', division_id_1)

        self.schema.create_linkcollection_entry('section', section_id_1, 'members', employee_id_1)

        tree = parse('self.employees.parent_division_employees.sections.members.age', self.schema.specs['division'])

        aggregations = self.aggregator.get_for_resource(tree, 'employee', self.schema.decodeid(employee_id_1))

        result = self.schema.db['resource_employee'].aggregate(aggregations[0])
        self.assertEqual({
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_grants': [],
            '_id': self.schema.decodeid(division_id_1),
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            'name': 'Sales'}, next(result))

    def test_ternary_aggregate(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        section_id_1 = self.schema.insert_resource('section', {'name': 'Sales'}, 'sections', 'division', division_id_1)

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 10}, 'employees', 'division', division_id_1)
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 10}, 'employees', 'division', division_id_1)

        self.schema.create_linkcollection_entry('section', section_id_1, 'members', employee_id_1)

        tree = parse('max(self.employees.age) > 20 -> max(self.employees.age) : min(divisions.sections.members.age)', self.schema.specs['division'])

        aggregations = self.aggregator.get_for_resource(tree, 'employee', self.schema.decodeid(employee_id_1))

        result = self.schema.db['resource_employee'].aggregate(aggregations[0])
        self.assertEqual({
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_grants': [],
            '_id': self.schema.decodeid(division_id_1),
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            'name': 'Sales'}, next(result))

    def test_simple_root(self):
        # uncertain what case this is testing exactly - section has no link to division
        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        tree = parse('max(divisions.name)', self.schema.root)

        aggregations = self.aggregator.get_for_resource(tree, 'division', self.schema.decodeid(division_id_1), 'division', 'max_divisions_name')

        self.assertEqual([[{'$lookup': {'as': '_field_max_divisions_name',
                        'from': 'resource_division',
                        'pipeline': []}},
            {'$group': {'_id': '$_field_max_divisions_name'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}]], aggregations)

