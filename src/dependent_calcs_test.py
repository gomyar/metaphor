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


class ResourceCalcTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db

        self.schema = Schema(self.db, "1.1")

        self.app = Flask(__name__)
        self.app.secret_key = "1234test"

        self.app.config['api'] = MongoApi('http://server', self.schema, self.db)
        self.app.config['schema'] = self.schema

        self.app.register_blueprint(api_bp)
        self.app.register_blueprint(schema_bp)

        self.client = self.app.test_client()

    def test_dependent_calcs(self):
        resp = self.client.post('/schema/specs', data=json.dumps({
            'name': 'employee', 'fields': {
            "first_name": {'type': "str"},
            "last_name": {'type': "str"},
            "full_name": {'type': 'calc', 'calc': "self.first_name + self.last_name", 'calc_type': "str"},
            "position": {'type': "str"},
            "address_name": {'type': 'calc', 'calc': "self.full_name", "calc_type": "str"},
            "address_city": {'type': "str"},
            "address_full": {'type': 'calc', 'calc': "self.address_name + self.address_city", "calc_type": "str"},
        }}), content_type='application/json')

        resp = self.client.patch('/schema/specs/employee', data=json.dumps({
            "myself": {'type': 'calc', 'calc': 'self', 'calc_type': 'employee'},
        }), content_type='application/json')

        resp = self.client.post('/schema/root', data=json.dumps({
            'name': 'employees', 'target': 'employee'
        }), content_type='application/json')

        response = self.client.post('/api/employees', data=json.dumps({
            'first_name': 'Bob',
            'last_name': 'Mac',
            'position': 'Boss',
            'address_city': 'New York',
        }), content_type='application/json')
        employee_id = json.loads(response.data)['id']

        response = self.client.get('api/employees/%s' % (employee_id,))
        employee = json.loads(response.data)
        self.assertEquals('BobMac', employee['address_name'])
        self.assertEquals('BobMacNew York', employee['address_full'])
