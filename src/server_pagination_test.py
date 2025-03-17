
import json
from unittest import TestCase
from server import create_app
from metaphor.mongoclient_testutils import mongo_connection
from metaphor.schema import Schema
from werkzeug.security import generate_password_hash, check_password_hash


class ServerTest(TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

        schema = Schema.create_schema(self.db)
        schema.create_initial_schema()
        schema.set_as_current()

        self.app = create_app(self.db)
        self.api = self.app.config['api']
        self.schema = self.api.schema

        self.client = self.app.test_client()

        self.schema.create_group("manager")
        self.schema.create_grant("manager", "read", "employees")
        self.schema.create_grant("manager", "create", "employees")

        self.user_id = self.api.updater.create_basic_user("bob", "password", ["manager"])

        # add test data
        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        for i in range(12):
            self.api.post('/employees', {'name': 'fred %s' % i})

        # login
        self.client.post('/login', json={
            "email": "bob",
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
