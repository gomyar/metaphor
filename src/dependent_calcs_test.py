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

        resp = self.client.post('/schema/root', data=json.dumps({
            'name': 'employees', 'target': 'employee'
        }), content_type='application/json')
        self.assertEquals(200, resp.status_code)

        response = self.client.post('/api/employees', data=json.dumps({
            'first_name': 'Bob',
            'last_name': 'Mac',
            'position': 'Boss',
            'address_city': 'New York',
        }), content_type='application/json')
        self.assertEquals(200, response.status_code)

        employee_id = json.loads(response.data)['id']

        response = self.client.get('api/employees/%s' % (employee_id,))
        employee = json.loads(response.data)
        self.assertEquals('BobMac', employee['address_name'])
        self.assertEquals('BobMacNew York', employee['address_full'])

    def _test_local_field_dependencies(self):
        ''' dont think this is necessary after all'''
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

        first_name = self.schema.specs['employee'].fields['first_name']
        full_name = self.schema.specs['employee'].fields['full_name']
        address_name = self.schema.specs['employee'].fields['address_name']
        address_full = self.schema.specs['employee'].fields['address_full']

        dependents = self.schema.updater.find_affected_calcs_for_field(first_name)
        self.assertEquals(set([(full_name, 'self.first_name', 'self')]), dependents)

        dependents = self.schema.updater.find_affected_calcs_for_field(full_name)
        self.assertEquals(set([(address_name, 'self.full_name', 'self')]), dependents)

        dependents = self.schema.updater.find_affected_calcs_for_field(address_name)
        self.assertEquals(set([(address_full, 'self.address_name', 'self')]), dependents)

        dependents = self.schema.updater.find_affected_calcs_for_field(address_full)
        self.assertEquals(set(), dependents)

        dependents = self.schema.updater.ordered_local_dependencies(first_name)
        self.assertEquals([
            (full_name, 'self.first_name', 'self'),
            (address_full, 'self.full_name', 'self'),
        ], dependents)

    def test_dependent_calcs_on_delete(self):
        self.client.post('/schema/specs', data=json.dumps({
            'name': 'employee', 'fields': {
            "first_name": {'type': "str"},
            "last_name": {'type': "str"},
            "full_name": {'type': 'calc', 'calc': "self.first_name + self.last_name", 'calc_type': "str"},
            "position": {'type': "str"},
            "address_name": {'type': 'calc', 'calc': "self.full_name", "calc_type": "str"},
            "address_city": {'type': "str"},
            "address_full": {'type': 'calc', 'calc': "self.address_name + self.address_city", "calc_type": "str"},
        }}), content_type='application/json')

        resp = self.client.delete('/schema/specs/employee/address_name')
        self.assertEquals(400, resp.status_code)
        self.assertEquals({
            "error": "employee.address_name depended upon by employee.address_full"},
            json.loads(resp.data))

    def test_dependent_calcs_on_field_delete(self):
        self.client.post('/schema/specs', data=json.dumps({
            'name': 'employee', 'fields': {
            "first_name": {'type': "str"},
            "last_name": {'type': "str"},
            "full_name": {'type': 'calc', 'calc': "self.first_name + self.last_name", 'calc_type': "str"},
            "position": {'type': "str"},
            "address_name": {'type': 'calc', 'calc': "self.full_name", "calc_type": "str"},
            "address_city": {'type': "str"},
            "address_full": {'type': 'calc', 'calc': "self.address_name + self.address_city", "calc_type": "str"},
        }}), content_type='application/json')

        resp = self.client.delete('/schema/specs/employee/address_city')
        self.assertEquals(400, resp.status_code)
        self.assertEquals({
            "error": "employee.address_city depended upon by employee.address_full"},
            json.loads(resp.data))
