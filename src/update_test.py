
import unittest
from datetime import datetime
from mock import patch

from bson.objectid import ObjectId
from pymongo import MongoClient
from gevent import Greenlet

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

        self.employee_spec.add_field("seniority", CalcSpec('self.age / 10', "int"))

        self.department_spec.add_field("employees",
                                       LinkCollectionSpec("employee"))
        self.department_spec.add_field(
            "averageAge", CalcSpec("average(self.employees.age)", 'int'))

        self.api = MongoApi('http://server', self.schema, self.db)

        self.update = Update(self.schema)

    @patch('metaphor.update.datetime')
    def test_update(self, dt):
        dt.now.return_value = datetime(2018, 1, 1, 1, 1, 1)

        self.update.init()
        employee_id = self.db['resource_employee'].insert({'name': 'Bob'})

        self.update.init_update_resource(
            'employee', employee_id, {'name': 'Ned'})

        employee = self.db['resource_employee'].find_one(
            {'_id': employee_id}, {'_id': 0})

        self.assertEquals({
            'name': 'Ned',
        }, employee)

    def test_zip_altered_dependents(self):
        dept_id_1 = ObjectId()
        dept_id_2 = ObjectId()
        empl_id_1 = ObjectId()
        empl_id_2 = ObjectId()

        altered = set([
            ('department', 'averageAge', (dept_id_1, dept_id_2)),
            ('department', 'anotherCalc', (dept_id_1,)),
            ('employee', 'seniority', (empl_id_1,)),
            ('employee', 'emplCalc', (empl_id_1, empl_id_2)),
        ])

        dependents = self.update._zip_altered(altered)

        self.assertEquals([
            ('department', dept_id_1, ['averageAge', 'anotherCalc']),
            ('department', dept_id_2, ['averageAge']),
            ('employee', empl_id_1, ['seniority', 'emplCalc']),
            ('employee', empl_id_2, ['emplCalc']),
        ], sorted(dependents))

    @patch('metaphor.update.datetime')
    def test_update_object(self, dt):
        dt.now.return_value = datetime(2018, 1, 1, 1)

        # setup
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

        self.update.init()

        # must have own id
        update_obj = self.db['metaphor_update'].find_one({'_id': self.update.update_id})
        self.assertEquals({'_id': self.update.update_id}, update_obj)

        # on init update - must have spec_name + resource_id
        # must have altered fields
        # will have list of dependent's resource_ids
        # resource fields have changed
        self.update.init_update_resource('employee', employee_id, {'age': 30})

        update_obj = self.db['metaphor_update'].find_one(
            {'_id': self.update.update_id})
        employee_obj = self.db['resource_employee'].find_one(
            {'_id': employee_id})

        self.assertEquals({
            '_id': self.update.update_id,
            'spec_name': 'employee',
            'resource_id': employee_id,
            'fields': ['age'],
            'dependents': [],
            'processing': datetime(2018, 1, 1, 1)}, update_obj)

        self.assertEquals(30, employee_obj['age'])
        self.assertEquals(3, employee_obj['seniority'])

        # on dependencies update
        self.update.init_dependency_update()

        update_obj = self.db['metaphor_update'].find_one(self.update.update_id)
        # update obj contains dependency ids

        self.assertEquals('employee', update_obj['spec_name'])
        self.assertEquals(employee_id, update_obj['resource_id'])
        self.assertEquals(['age'], update_obj['fields'])
        self.assertEquals(datetime(2018, 1, 1, 1), update_obj['processing'])
        self.assertTrue(['department', department_id_1, ['averageAge']] in update_obj['dependents'])
        self.assertTrue(['department', department_id_2, ['averageAge']] in update_obj['dependents'])

        # spawn updates for each dependent resource
        self.update.spawn_dependent_updates()
        expected = sorted([
            ('department', department_id_1, ['averageAge']),
            ('department', department_id_2, ['averageAge']),
        ])
        actual = sorted([g.args for g in self.update.gthreads])
        self.assertEquals(expected, actual)

        # wait for all gthread to finish
        self.update.wait_for_dependent_updates()

        # remove db entries
        self.update.finalize_update()

        self.assertIsNone(self.db['metaphor_update'].find_one(self.update.update_id))

    def test_resource_created(self):
        pass
