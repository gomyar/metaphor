
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.updater import Updater

from metaphor.update.move_resource import MoveResourceUpdate


class MoveResourceTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.updater = Updater(self.schema)

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')

        self.schema.create_field('root', 'current_employees', 'collection', 'employee')
        self.schema.create_field('root', 'former_employees', 'collection', 'employee')

        self.calcs_spec = self.schema.create_spec('calcs')

    def test_move_from_root(self):
        self.schema.create_field('calcs',  'sum_employee_age', 'calc', calc_str= 'sum(current_employees.age)')
        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        # add root resources
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'Bob', 'age': 10}, 'current_employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'Ned', 'age': 14}, 'current_employees')

        calcs_id_1 = self.schema.insert_resource('calcs', {}, 'calcs')

        # create move update
        self.move_resource = MoveResourceUpdate(
            self.updater,
            self.schema,
            None,
            'root',
            'root',
            'former_employees',
            'current_employees')

        from_path_agg, from_path_spec, from_path_is_coll = self.move_resource.from_path_agg()

        self.assertEqual([
            {'$match': {
                '$and': [
                    {'_parent_field_name': 'current_employees'},
                    {'_parent_canonical_url': '/'}
                ]
            }},
            {'$match': {'_deleted': {'$exists': False}}},
        ], from_path_agg)
        self.assertEqual(self.schema.specs['employee'], from_path_spec)
        self.assertTrue(from_path_is_coll)

        affected_ids_agg_before = self.move_resource.affected_aggs()
        self.assertEqual([
            ('calcs', 'sum_employee_age', [
            {'$lookup': {'as': '_field_sum_employee_age',
               'from': 'resource_calcs',
               'pipeline': []}},
            {'$group': {'_id': '$_field_sum_employee_age'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}])
        ], affected_ids_agg_before)

        # check affected agg to path
        affected_ids_agg_after = self.move_resource.affected_aggs_to_path()
        self.assertEqual([
        ], affected_ids_agg_after)

        # check affected ids
        self.assertEqual(set([
            ('calcs', 'sum_employee_age', self.schema.decodeid(calcs_id_1)),
        ]), self.move_resource.affected_ids())

        # check affected ids for to path
        self.assertEqual(set([
        ]), self.move_resource.affected_ids_to_path())

    def test_move_from_root_after_aggs(self):
        self.schema.create_field('calcs',  'sum_employee_age', 'calc', calc_str= 'sum(former_employees.age)')
        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        # add root resources
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'Bob', 'age': 10}, 'current_employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'Ned', 'age': 14}, 'current_employees')

        calcs_id_1 = self.schema.insert_resource('calcs', {}, 'calcs')

        # create move update
        self.move_resource = MoveResourceUpdate(
            self.updater,
            self.schema,
            None,
            'root',
            'former_employees',
            'former_employees',
            'current_employees')

        from_path_agg, from_path_spec, from_path_is_coll = self.move_resource.from_path_agg()

        self.assertEqual([
            {'$match': {
                '$and': [
                    {'_parent_field_name': 'current_employees'},
                    {'_parent_canonical_url': '/'}
                ]
            }},
            {'$match': {'_deleted': {'$exists': False}}}
        ], from_path_agg)
        self.assertEqual(self.schema.specs['employee'], from_path_spec)
        self.assertTrue(from_path_is_coll)

        affected_ids_agg_before = self.move_resource.affected_aggs()
        self.assertEqual([
        ], affected_ids_agg_before)

        # check affected agg to path
        affected_ids_agg_after = self.move_resource.affected_aggs_to_path()
        self.assertEqual([
            ('calcs', 'sum_employee_age',
            [{'$lookup': {'as': '_field_sum_employee_age',
                            'from': 'resource_calcs',
                            'pipeline': []}},
            {'$group': {'_id': '$_field_sum_employee_age'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}])], affected_ids_agg_after)

        # check affected ids
        self.assertEqual(set([
        ]), self.move_resource.affected_ids())

        # need to perform the move before the after ids will show up
        self.move_resource.perform_move('111')

        # check affected ids for to path
        self.assertEqual(set([
            ('calcs', 'sum_employee_age', self.schema.decodeid(calcs_id_1)),
        ]), self.move_resource.affected_ids_to_path())

        # check canonical_url
        employee = self.db['resource_employee'].find_one({'_id': self.schema.decodeid(employee_id_1 )})
        self.assertEqual('/', employee['_parent_canonical_url'])
        self.assertEqual('former_employees', employee['_parent_field_name'])

    def test_move_from_root_more_resources(self):
        self.schema.create_field('calcs',  'sum_employee_age', 'calc', calc_str= 'sum(current_employees.age)')
        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        # add root resources
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'Bob', 'age': 10}, 'current_employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'Ned', 'age': 14}, 'current_employees')

        calcs_id_1 = self.schema.insert_resource('calcs', {}, 'calcs')
        calcs_id_2 = self.schema.insert_resource('calcs', {}, 'calcs')

        # create move update
        self.move_resource = MoveResourceUpdate(
            self.updater,
            self.schema,
            None,
            'root',
            'root',
            'former_employees',
            'current_employees')

        # check affected ids
        self.assertEqual(set([
            ('calcs', 'sum_employee_age', self.schema.decodeid(calcs_id_1)),
            ('calcs', 'sum_employee_age', self.schema.decodeid(calcs_id_2)),
        ]), self.move_resource.affected_ids())

    def test_move_from_child_collection(self):
        self.division_spec = self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')

        self.schema.create_field('root', 'divisions', 'collection', 'division')
        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        self.schema.create_field('division', 'employees', 'collection', 'employee')

        self.schema.create_field('calcs', 'division', 'link', 'division')
        self.schema.create_field('calcs',  'sum_division_employee_age', 'calc', calc_str= 'sum(self.division.employees.age)')

        # add root resources
        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'Marketting'}, 'divisions')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'Bob', 'age': 10}, 'employees', 'division', division_id_1)
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'Ned', 'age': 14}, 'employees', 'division', division_id_1)
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'Fred', 'age': 16}, 'employees', 'division', division_id_2)

        calcs_id_1 = self.schema.insert_resource('calcs', {"division": division_id_1}, 'calcs')
        calcs_id_2 = self.schema.insert_resource('calcs', {"division": division_id_2}, 'calcs')

        # create move update
        self.move_resource = MoveResourceUpdate(
            self.updater,
            self.schema,
            division_id_2,
            'division',
            'employees',
            'divisions/%s/employees' % division_id_2,
            'divisions/%s/employees' % division_id_1)

        # check from agg
        from_path_agg, from_path_spec, from_path_is_coll = self.move_resource.from_path_agg()
        self.assertEqual([
            {'$match': {'$and': [{'_parent_field_name': 'divisions'},
                                 {'_parent_canonical_url': '/'}]}},
            {'$match': {'_deleted': {'$exists': False}}},
            {'$match': {'_id': self.schema.decodeid(division_id_1)}},
            {'$match': {'_deleted': {'$exists': False}}},
            {'$lookup': {'as': '_field_employees',
                        'foreignField': '_parent_id',
                        'from': 'resource_employee',
                        'localField': '_id'}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'_deleted': {'$exists': False}}},
            ], from_path_agg)
        self.assertEqual(self.schema.specs['employee'], from_path_spec)
        self.assertTrue(from_path_is_coll)

        # check affected ids agg
        affected_ids_agg_before = self.move_resource.affected_aggs()
        self.assertEqual([
            ('calcs', 'sum_division_employee_age', [
            # lookup to division
            {'$lookup': {'as': '_field_employees',
                        'foreignField': '_id',
                        'from': 'resource_division',
                        'localField': '_parent_id'}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            # lookup to calcs
            {'$lookup': {'as': '_field_division',
                        'foreignField': 'division',
                        'from': 'resource_calcs',
                        'localField': '_id'}},
            {'$group': {'_id': '$_field_division'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}])], affected_ids_agg_before)

        # check affected ids
        self.assertEqual(set([
            ('calcs', 'sum_division_employee_age', self.schema.decodeid(calcs_id_1)),
        ]), self.move_resource.affected_ids())
