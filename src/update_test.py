
import unittest
from datetime import datetime
from mock import patch

from pymongo import MongoClient

from metaphor.update import Update
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource
from metaphor.resource import LinkCollectionSpec
from metaphor.resource import AggregateField, CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi




class UpdateTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.employee_spec = ResourceSpec('employee')
        self.employee_spec.add_field("name", FieldSpec("str"))
        self.employee_spec.add_field("age", FieldSpec("int"))

        self.department_spec = ResourceSpec('department')

        self.schema.add_resource_spec(self.employee_spec)
        self.schema.add_resource_spec(self.department_spec)
        self.schema.add_root('employees', CollectionSpec('employee'))

        self.department_spec.add_field("employees",
                                       LinkCollectionSpec("employee"))
        self.department_spec.add_field(
            "averageAge", CalcSpec("average(self.employees.age)", 'int'))

        self.api = MongoApi('http://server', self.schema, self.db)

        self.update = Update(self.schema)

    @patch('metaphor.update.datetime')
    def test_update(self, dt):
        dt.now.return_value = datetime(2018, 1, 1, 1, 1, 1)

        employee_id = self.db['resource_employee'].insert({'name': 'Bob'})

        self.employee = self.api.build_resource('employees/%s' % (employee_id))

        updated_data = self.update._initiate_update_processing(
            self.employee, {'name': 'Ned'})
        self.assertEquals({
            '_id': employee_id,
            'name': 'Ned',
            '_updated': None,
            '_processing': datetime(2018, 1, 1, 1, 1, 1),
            '_updated_fields': ['name'],
        }, updated_data)

        employee = self.db['resource_employee'].find_one(
            {'_id': employee_id}, {'_id': 0})

        self.assertEquals({
            'name': 'Ned',
            '_updated': None,
            '_processing': datetime(2018, 1, 1, 1, 1, 1),
            '_updated_fields': ['name'],
        }, employee)

    @patch('metaphor.update.datetime')
    def test_update_ongoing_update(self, dt):
        dt.now.return_value = datetime(2018, 1, 1, 1, 1, 1)

        employee_id = self.db['resource_employee'].insert(
            {'name': 'Bob',
             '_updated': None,
             '_processing': datetime(2017, 2, 2, 2, 2, 2)})

        self.employee = self.api.build_resource('employees/%s' % (employee_id))

        updated_data = self.update._initiate_update_processing(
            self.employee, {'name': 'Ned'})
        self.assertEquals(None, updated_data)

        self.update._initiate_update_processing(
            self.employee, {'name': 'Ned'})

        employee = self.db['resource_employee'].find_one(
            {'_id': employee_id}, {'_id': 0})

        # document has not changed, since another update is processing
        self.assertEquals({
            'name': 'Bob',
            '_updated': None,
            '_processing': datetime(2017, 2, 2, 2, 2, 2),
        }, employee)

    def test_finalize_update(self):
        employee_id = self.db['resource_employee'].insert(
            {'name': 'Bob', '_processing': datetime(2018, 1, 1, 1, 1, 1),
             '_updated_fields': ['name']})

        self.employee = self.api.build_resource('employees/%s' % (employee_id))

        updated_data = self.update._finalize_update(self.employee)
        self.assertEquals({
            '_id': employee_id,
            'name': 'Bob',
            '_updated_fields': None,
            '_processing': None,
        }, updated_data)

        employee = self.db['resource_employee'].find_one(
            {'_id': employee_id}, {'_id': 0})

        self.assertEquals({
            'name': 'Bob',
            '_updated_fields': None,
            '_processing': None,
        }, employee)

    def test_finalize_update_another_update_has_begun(self):
        employee_id = self.db['resource_employee'].insert(
            {'name': 'Bob', '_processing': datetime(2018, 2, 2, 2, 2, 2),
             '_updated': True,
             '_updated_fields': ['name']})

        self.employee = self.api.build_resource('employees/%s' % (employee_id))

        updated_data = self.update._finalize_update(self.employee)
        self.assertEquals(None, updated_data)

        employee = self.db['resource_employee'].find_one(
            {'_id': employee_id}, {'_id': 0})

        # document has not changed
        self.assertEquals({
            'name': 'Bob',
            '_updated_fields': ['name'],
            '_updated': True,
            '_processing': datetime(2018, 2, 2, 2, 2, 2),
        }, employee)

    def test_update_dependents(self):
        department_id_1 = self.db['resource_department'].insert(
            {'name': 'Engineering',
            })
        department_id_2 = self.db['resource_department'].insert(
            {'name': 'Marketting',
            })

        employee_id = self.db['resource_employee'].insert(
            {'name': 'Bob',
             'age': 40,
             '_owners': [
                {'owner_spec': 'department',
                 'owner_field': 'employees',
                 'owner_id': department_id_1},
                {'owner_spec': 'department',
                 'owner_field': 'employees',
                 'owner_id': department_id_2},
             ],
            })

        self.employee = self.api.build_resource('employees/%s' % (employee_id))

        updated_ids = self.update._update_dependents(self.employee, ['age'])
        self.assertEquals(set([department_id_1, department_id_2]), updated_ids)

        department_data_1 = self.db['resource_department'].find_one(
            {'_id': department_id_1}, {'_id': 0})
        department_data_2 = self.db['resource_department'].find_one(
            {'_id': department_id_2}, {'_id': 0})

        self.assertEquals({
            'name': 'Engineering',
            '_updated': True,
            '_updated_fields': ['averageAge']}, department_data_1)

        self.assertEquals({
            'name': 'Marketting',
            '_updated': True,
            '_updated_fields': ['averageAge']}, department_data_2)

    def test_update_dependents_unlinked_dependents_unchanged(self):
        department_id_1 = self.db['resource_department'].insert(
            {'name': 'Engineering',
            })
        department_id_2 = self.db['resource_department'].insert(
            {'name': 'Marketting',
            })

        employee_id = self.db['resource_employee'].insert(
            {'name': 'Bob',
             'age': 40,
             '_owners': [
                {'owner_spec': 'department',
                 'owner_field': 'employees',
                 'owner_id': department_id_1}
                 # no department 2
             ],
            })

        self.employee = self.api.build_resource('employees/%s' % (employee_id))

        updated_ids = self.update._update_dependents(self.employee, ['age'])
        self.assertEquals(set([department_id_1]), updated_ids)

        department_data_1 = self.db['resource_department'].find_one(
            {'_id': department_id_1}, {'_id': 0})
        department_data_2 = self.db['resource_department'].find_one(
            {'_id': department_id_2}, {'_id': 0})

        self.assertEquals({
            'name': 'Engineering',
            '_updated': True,
            '_updated_fields': ['averageAge']}, department_data_1)

        # unlinked dependent unchanged
        self.assertEquals({
            'name': 'Marketting',
            }, department_data_2)


    def test_update_on_create(self):
        department_id_1 = self.db['resource_department'].insert(
            {'name': 'Engineering',
            })

        employee_id = self.db['resource_employee'].insert(
            {'name': 'Bob',
             'age': 40,
             '_owners': [
                {'owner_spec': 'department',
                 'owner_field': 'employees',
                 'owner_id': department_id_1}
             ],
            })

        self.employee = self.api.build_resource('employees/%s' % (employee_id))

        self.update._update_resource_dependents(self.employee)

        department_data_1 = self.db['resource_department'].find_one(
            {'_id': department_id_1}, {'_id': 0})
        self.assertEquals({
            'name': 'Engineering',
            '_updated': True,
            '_updated_fields': ['averageAge']}, department_data_1)
