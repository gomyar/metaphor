
import unittest
import json

from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi
from metaphor.api import SchemaApi
from metaphor.api import RootsApi
from metaphor.api_bp import api_bp
from metaphor.schema_bp import schema_bp

from flask import Flask


class PaginationTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db

        self.schema = Schema(self.db, "1.1")

        self.app = Flask(__name__)
        self.app.secret_key = "1234test"

        self.app.config['api'] = MongoApi('server', self.schema, self.db)
        self.app.config['schema'] = self.schema

        self.app.register_blueprint(api_bp)
        self.app.register_blueprint(schema_bp)

        self.client = self.app.test_client()

        self.client.post('/schema/specs', data=json.dumps(
            {'name': 'skill', 'fields': {
                'name': {'type': 'str'},
            }}), content_type='application/json')

        self.client.post('/schema/specs', data=json.dumps(
            {'name': 'employee', 'fields': {
                'name': {'type': 'str'},
                'skills': {'type': 'collection', 'target': 'skill'},
            }}), content_type='application/json')

        self.client.post('/schema/root', data=json.dumps({'name': 'employees', 'target': 'employee'}), content_type='application/json')

        for index in range(120):
            response = self.client.post('/api/employees', data=json.dumps({
                'name': 'Bob',
            }), content_type='application/json')
            employee_id = json.loads(response.data)['id']
            self.client.post('/api/employees/%s/skills' % (employee_id,), data=json.dumps({
                'name': 'Carpentry',
            }), content_type='application/json')

    def test_paging(self):
        page1 = self.client.get('/api/employees', content_type='application/json')
        data = json.loads(page1.data)
        self.assertEquals(120, data['count'])
        self.assertEquals(100, len(data['results']))
        self.assertEquals('http://server/api/employees?page=2', data['next'])

    def test_paging_page_1(self):
        page1 = self.client.get('/api/employees?page=1', content_type='application/json')
        data = json.loads(page1.data)
        self.assertEquals(120, data['count'])
        self.assertEquals(100, len(data['results']))
        self.assertEquals('http://server/api/employees?page=2', data['next'])

    def test_paging_page_2(self):
        page2 = self.client.get('/api/employees?page=2', content_type='application/json')
        data = json.loads(page2.data)
        self.assertEquals(120, data['count'])
        self.assertEquals(20, len(data['results']))
        self.assertEquals(None, data['next'])

    def test_aggregate_paging(self):
        page1 = self.client.get('/api/employees/skills', content_type='application/json')
        data = json.loads(page1.data)
        self.assertEquals(100, len(data['results']))
        self.assertEquals('http://server/api/employees/skills?page=2', data['next'])

    def test_aggregate_paging_page_1(self):
        page1 = self.client.get('/api/employees/skills?page=1', content_type='application/json')
        data = json.loads(page1.data)
        self.assertEquals(100, len(data['results']))
        self.assertEquals('http://server/api/employees/skills?page=2', data['next'])

    def test_aggregate_paging_page_2(self):
        page2 = self.client.get('/api/employees/skills?page=2', content_type='application/json')
        data = json.loads(page2.data)
        self.assertEquals(20, len(data['results']))
        self.assertEquals(None, data['next'])
