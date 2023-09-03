
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.updater import Updater

from metaphor.update.copy_resource import CopyResourceUpdate


class CopyResourceTest(unittest.TestCase):
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

    def test_copy_from_root(self):
        self.schema.create_field('calcs',  'sum_employee_age', 'calc', calc_str= 'sum(current_employees.age)')
        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        # add root resources
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'Bob', 'age': 10}, 'current_employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'Ned', 'age': 14}, 'current_employees')

        calcs_id_1 = self.schema.insert_resource('calcs', {}, 'calcs')

        # create copy update
        self.copy_resource = CopyResourceUpdate(
            self.updater,
            self.schema,
            None,
            'root',
            'root',
            'former_employees',
            'current_employees')

    def test_copy_from_root_after_aggs(self):
        self.schema.create_field('calcs',  'sum_employee_age', 'calc', calc_str= 'sum(former_employees.age)')
        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        # add root resources
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'Bob', 'age': 10}, 'current_employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'Ned', 'age': 14}, 'current_employees')

        calcs_id_1 = self.schema.insert_resource('calcs', {}, 'calcs')

        # create copy update
        self.copy_resource = CopyResourceUpdate(
            self.updater,
            self.schema,
            None,
            'root',
            'former_employees',
            'former_employees',
            'current_employees')

        # need to perform the copy before the after ids will show up
        self.copy_resource.perform_copy('111')

        # check affected ids for to path
        self.assertEqual(set([
            ('calcs', 'sum_employee_age', self.schema.decodeid(calcs_id_1)),
        ]), self.copy_resource.affected_ids_to_path())

        # check canonical_url
        employee = self.db['metaphor_resource'].find_one({'_id': self.schema.decodeid(employee_id_1 )})
        self.assertEqual('/', employee['_parent_canonical_url'])
        self.assertEqual('former_employees', employee['_parent_field_name'])

    def test_copy_from_root_more_resources(self):
        self.schema.create_field('calcs',  'sum_employee_age', 'calc', calc_str= 'sum(current_employees.age)')
        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        # add root resources
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'Bob', 'age': 10}, 'current_employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'Ned', 'age': 14}, 'current_employees')

        calcs_id_1 = self.schema.insert_resource('calcs', {}, 'calcs')
        calcs_id_2 = self.schema.insert_resource('calcs', {}, 'calcs')

        # create copy update
        self.copy_resource = CopyResourceUpdate(
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
        ]), self.copy_resource.affected_ids())

    def test_copy_from_child_collection(self):
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

        # create copy update
        self.copy_resource = CopyResourceUpdate(
            self.updater,
            self.schema,
            division_id_2,
            'division',
            'employees',
            'divisions/%s/employees' % division_id_2,
            'divisions/%s/employees' % division_id_1)

