
import json
from unittest import TestCase
from server import create_app
from pymongo import MongoClient
from metaphor.schema import Schema
from werkzeug.security import generate_password_hash


class ServerTest(TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

        Schema(self.db).create_initial_schema()

        self.app = create_app(self.db)
        self.api = self.app.config['api']
        self.schema = self.api.schema

        self.client = self.app.test_client()

        pw_hash = generate_password_hash('password')
        self.user_id = self.api.post('/users', {'username': 'bob', 'pw_hash': pw_hash})
        self.group_id = self.api.post('/groups', {'name': 'manager'})
        self.grant_id_1 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': '/employees'})
        self.api.post('/users/%s/groups' % self.user_id, {'id': self.group_id})

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

    def test_group_access(self):
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')

        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'fred'}, 'employees')

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(404, response.status_code)

        grant_id = self.api.post('/groups/%s/grants' % self.group_id, {'url': '/employees', 'type': 'read'})

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(200, response.status_code)
        self.assertEqual('fred', response.json['name'])

    def test_grant_group_permissions(self):
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')

        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        grant_id = self.api.post('/groups/%s/grants' % self.group_id, {'url': '/employees', 'type': 'read'})

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(200, response.status_code)

    def test_posting_new_grant_updates_resources(self):
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')

        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        grant_id_2 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': '/employees'})

        employee_data = self.db.resource_employee.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(employee_id_1),
            '_canonical_url': '/employees/%s' % employee_id_1,
            'name': 'fred',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
            '_grants': [
                self.schema.decodeid(self.grant_id_1),
                self.schema.decodeid(grant_id_2),
            ]
        }, employee_data)

    def test_can_post_with_grant(self):
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')

        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        no_response = self.client.post('/api/employees', data=json.dumps({'name': 'wontblend'}), content_type='application/json')
        self.assertEqual(403, no_response.status_code)

        grant_id_2 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'create', 'url': '/employees'})

        yes_response = self.client.post('/api/employees', data=json.dumps({'name': 'willblend'}), content_type='application/json')
        self.assertEqual(201, yes_response.status_code)
