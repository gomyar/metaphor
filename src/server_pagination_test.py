
import json
from unittest import TestCase
from server import create_app
from pymongo import MongoClient
from metaphor.schema import Schema
from werkzeug.security import generate_password_hash, check_password_hash


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

        self.user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})
        self.group_id = self.api.post('/groups', {'name': 'manager'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': '/employees'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'create', 'url': '/employees'})
        self.api.post('/users/%s/groups' % self.user_id, {'id': self.group_id})

        # add test data
        employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(employee_spec, 'name', 'str')

        self.schema.add_field(self.schema.root, 'employees', 'collection', 'employee')

        for i in range(12):
            self.api.post('/employees', {'name': 'fred %s' % i})

        # login
        self.client.post('/login', json={
            "username": "bob",
            "password": "password",
        }, follow_redirects=True)

    def test_get_page(self):
        # Zero-based pages you heathens
        response = self.client.get('/api/employees')
        self.assertEqual(10, len(response.json['results']))
        self.assertEqual("fred 0", response.json['results'][0]['name'])
        self.assertEqual(12, response.json['count'])
        self.assertEqual(None, response.json['previous'])
        self.assertEqual("http://localhost/api/employees?page=1&page_size=10", response.json['next'])

    def test_get_next_page(self):
        response = self.client.get('/api/employees?page=1')
        self.assertEqual(2, len(response.json['results']))
        self.assertEqual("fred 10", response.json['results'][0]['name'])
        self.assertEqual(12, response.json['count'])
        self.assertEqual("http://localhost/api/employees?page=0&page_size=10", response.json['previous'])
        self.assertEqual(None, response.json['next'])

    def test_get_any_page(self):
        response = self.client.get('/api/employees?page=1&page_size=5')
        self.assertEqual(5, len(response.json['results']))
        self.assertEqual("fred 5", response.json['results'][0]['name'])
        self.assertEqual(12, response.json['count'])
        self.assertEqual("http://localhost/api/employees?page=0&page_size=5", response.json['previous'])
        self.assertEqual("http://localhost/api/employees?page=2&page_size=5", response.json['next'])
