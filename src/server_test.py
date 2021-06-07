
from unittest import TestCase
from server import create_app
from pymongo import MongoClient
from metaphor.schema import Schema


class ServerTest(TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

        Schema(self.db).create_initial_schema()

        self.app = create_app(self.db)
        self.schema = self.app.config['api'].schema

        self.client = self.app.test_client()

        user_id = self.schema.create_user('bob', 'password')
        group_id_1 = self.schema.insert_resource('group', {'name': 'managers'}, 'groups')
        self.schema.create_linkcollection_entry('user', user_id, 'groups', group_id_1)
        self.client.post('/login', data={
            "username": "bob",
            "password": "password",
        }, follow_redirects=True)

    def test_get(self):
        response = self.client.get('/api/')
        self.assertEqual({'groups': '/groups', 'users': '/users', 'ego': '/ego'}, response.json)

    def test_ego(self):
        response = self.client.get('/api/ego/')

        self.assertEqual('bob', response.json['username'])
        self.assertEqual(False, response.json['is_admin'])

    def test_group_access(self):
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')

        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'fred'}, 'employees')

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(404, response.status_code)

#        self.schema.grant_read_to_group('employee', employee_id_1, 'managers')
        self.schema.grant_read('managers', '/employees/%s' % employee_id_1)

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(200, response.status_code)
        self.assertEqual('fred', response.json['name'])

    def test_grant_group_permissions(self):
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')

        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'fred'}, 'employees')

        response = self.client.post('/api/groups', {'name': 'managers', 'read_urls': ['/employees']})
        self.assertEqual(201, response.status_code)

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(200, response.status_code)
