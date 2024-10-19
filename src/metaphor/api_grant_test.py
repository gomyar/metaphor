
import unittest
from datetime import datetime
from urllib.error import HTTPError

from metaphor.mongoclient_testutils import mongo_connection

from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api, create_expand_dict


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.tasks_spec = self.schema.create_spec('task')
        self.schema.create_field('task', 'name', 'str')

        self.job_spec = self.schema.create_spec('job')
        self.schema.create_field('job', 'name', 'str')
        self.schema.create_field('job', 'tasks', 'collection', 'task')

        self.harddrive_spec = self.schema.create_spec('harddrive')
        self.schema.create_field('harddrive', 'name', 'str')

        self.laptop_spec = self.schema.create_spec('laptop')
        self.schema.create_field('laptop', 'name', 'str')
        self.schema.create_field('laptop', 'harddrives', 'collection', 'harddrive')

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'jobs', 'linkcollection', 'job')
        self.schema.create_field('employee', 'laptops', 'collection', 'laptop')

        self.schema.create_field('root', 'employees', 'collection', 'employee')
        self.schema.create_field('root', 'jobs', 'collection', 'job')

        self.api = Api(self.db)
        self.user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})


    def test_grants(self):
        group_id_1 = self.api.post('/groups', {'name': 'test'})
        self.api.post('/users/%s/groups' % self.user_id, {'id': group_id_1})

        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'employees'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'employees.laptops'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'employees.laptops.harddrives'})

        self.user = self.schema.load_user_by_username("bob")

        self.assertTrue(self.api.can_access(self.user, 'read', '/employees'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs/ID7890'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/laptops'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/laptops/ID4567/harddrives'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks'))

        # add linkcollection grant
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'employees.jobs'})

        self.user = self.schema.load_user_by_username("bob")

        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs/ID7890'))

        # add collection grant
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'jobs'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'jobs.tasks'})

        self.user = self.schema.load_user_by_username("bob")

        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs/ID7890'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs/ID7890/tasks'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs/ID7890/tasks/ID4321'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks/ID4321'))

    def test_grant_methods(self):
        group_id_1 = self.api.post('/groups', {'name': 'test'})
        self.api.post('/users/%s/groups' % self.user_id, {'id': group_id_1})

        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'employees'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'create', 'url': 'jobs'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'update', 'url': 'employees'})

        self.user = self.schema.load_user_by_username("bob")

        self.assertTrue(self.api.can_access(self.user, 'read', '/employees'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234'))
        self.assertFalse(self.api.can_access(self.user, 'create', '/employees'))
        self.assertFalse(self.api.can_access(self.user, 'create', '/employees/ID1234'))
        self.assertTrue(self.api.can_access(self.user, 'update', '/employees'))
        self.assertTrue(self.api.can_access(self.user, 'update', '/employees/ID1234'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs/ID7890'))
        self.assertTrue(self.api.can_access(self.user, 'create', '/jobs'))
        self.assertTrue(self.api.can_access(self.user, 'create', '/jobs/ID7890'))
        self.assertFalse(self.api.can_access(self.user, 'update', '/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'update', '/jobs/ID7890'))

    def test_url_to_path(self):
        self.assertEqual(self.api._url_to_path('/employees'),
                         'employees')
        self.assertEqual(self.api._url_to_path('/employees/ID1234'),
                         'employees')
        self.assertEqual(self.api._url_to_path('/employees[name="Bob"]'),
                         'employees')
        self.assertEqual(self.api._url_to_path('/employees[name="Bob"]/ID1234'),
                         'employees')
        self.assertEqual(self.api._url_to_path('/jobs'),
                         'jobs')
        self.assertEqual(self.api._url_to_path('/jobs/ID7890'),
                         'jobs')
        self.assertEqual(self.api._url_to_path('/employees/ID1234/laptops'),
                         'employees.laptops')
        self.assertEqual(self.api._url_to_path('/employees/ID1234/laptops/ID4567/harddrives'),
                         'employees.laptops.harddrives')
        self.assertEqual(self.api._url_to_path('/employees[name="Bob"]/ID1234/laptops[age>10]/ID4567/harddrives[storage<=15]'),
                         'employees.laptops.harddrives')
        self.assertEqual(self.api._url_to_path('/employees/ID1234/jobs'),
                         'employees.jobs')
        self.assertEqual(self.api._url_to_path('/employees[name="Ned"|age<16]/ID1234/jobs'),
                         'employees.jobs')
        self.assertEqual(self.api._url_to_path('/employees/ID1234/jobs/ID7890/tasks'),
                         'employees.jobs.tasks')
        self.assertEqual(self.api._url_to_path("/employees/ID1234/jobs/ID7890/tasks[due_date~'2024',type='TBD']"),
                         'employees.jobs.tasks')
