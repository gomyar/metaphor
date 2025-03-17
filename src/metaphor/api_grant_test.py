
import unittest
from datetime import datetime
from urllib.error import HTTPError
from server import create_app

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

        self.app = create_app(self.db)
        self.client = self.app.test_client()

        self.user_id = self.api.updater.create_basic_user("bob", "password", [], True)

        response = self.client.post('/login', json={
            "email": "bob",
            "password": "password",
        }, follow_redirects=True)
        self.assertEqual(200, response.status_code)


    def test_grants(self):
        self.schema.create_group("test")
        self.schema.create_grant("test", "read", "employees")
        self.schema.create_grant("test", "read", "employees.laptops")
        self.schema.create_grant("test", "read", "employees.laptops.harddrives")

        response = self.client.post('/api/schema/groups/test/users', json={'id': self.user_id})
        self.assertEqual(200, response.status_code)

        self.user = self.schema.load_user_by_email("bob")

        self.assertTrue(self.api.can_access(self.user, 'read', '/employees'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs/ID7890'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/laptops'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/laptops/ID4567/harddrives'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks'))

        # add linkcollection grant
        self.schema.create_grant("test", "read", "employees.jobs")

        self.user = self.schema.load_user_by_email("bob")

        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/jobs/ID7890'))

        # add collection grant
        self.schema.create_grant("test", "read", "jobs")
        self.schema.create_grant("test", "read", "jobs.tasks")

        self.user = self.schema.load_user_by_email("bob")

        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs/ID7890'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs/ID7890/tasks'))
        self.assertTrue(self.api.can_access(self.user, 'read', '/jobs/ID7890/tasks/ID4321'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks'))
        self.assertFalse(self.api.can_access(self.user, 'read', '/employees/ID1234/jobs/ID7890/tasks/ID4321'))

    def test_grant_methods(self):
        self.schema.create_group("test")
        self.schema.create_grant("test", "read", "employees")
        self.schema.create_grant("test", "create", "jobs")
        self.schema.create_grant("test", "update", "employees")

        self.schema.add_user_to_group("test", self.schema.decodeid(self.user_id))
        self.user = self.schema.load_user_by_email("bob")

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
