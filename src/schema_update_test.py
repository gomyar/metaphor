
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


class SchemaUpdateTest(unittest.TestCase):
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

    def test_add_resource(self):
        self.assertEquals(None, self.db['metaphor_schema'].find_one())
        self.assertEquals(1, len(self.schema.specs))
        self.client.post('schema/specs', data=json.dumps({'name': 'company'}), content_type='application/json')
        self.assertEquals(2, len(self.schema.specs))

        self.assertEquals('company', self.schema.specs['company'].name)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals({
            'company': {'fields': {}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_add_resource_with_fields(self):
        self.assertEquals(1, len(self.schema.specs))
        self.client.post('schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        self.assertEquals(2, len(self.schema.specs))

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('str', self.schema.specs['company'].fields['name'].field_type)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_update_resource_spec(self):
        self.client.post('schema/specs', data=json.dumps({'name': 'company'}), content_type='application/json')
        self.client.patch('schema/specs/company', data=json.dumps({'assets': {'type': 'int'}}), content_type='application/json')

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('int', self.schema.specs['company'].fields['assets'].field_type)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'assets': {'type': 'int'}}, 'type': 'resource'}
            }, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_server_default(self):
        res = self.client.get('/schema/')
        self.assertEquals(200, res.status_code)
        expected = {'root': 'http://localhost/schema/root',
                    'specs': 'http://localhost/schema/specs'}
        self.assertEquals(expected, json.loads(res.data))

    def test_add_root_and_post(self):
        resp = self.client.post('/schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        resp = self.client.post('api/companies', data=json.dumps({'name': 'Fred'}), content_type='application/json')

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
             {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'}},
             saved_schema['specs'])
        self.assertEquals({'companies': {'type': 'collection', 'target': 'company'}}, saved_schema['roots'])

    def test_patch_and_put(self):
        resp = self.client.post('/schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        resp = self.client.post('api/companies', data=json.dumps({'name': 'Fred'}), content_type='application/json')
        company_id = json.loads(resp.data)['id']
        resp = self.client.put('api/companies/%s' % (company_id,), data=json.dumps({'name': 'Ned'}), content_type='application/json')

        ned = self.db['resource_company'].find_one()
        self.assertEquals(
            'Ned',
            ned['name'])

        resp = self.client.patch('api/companies/%s' % (company_id,), data=json.dumps({'name': 'Neddy'}), content_type='application/json')

        ned = self.db['resource_company'].find_one()
        self.assertEquals(
            'Neddy',
            ned['name'])

    def test_recalc_on_update(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.post('/api/companies', data=json.dumps({'name': 'Bobs Burgers', 'assets': 100, 'liabilities': 80}), content_type='application/json')
        company_id_1 = json.loads(response.data)['id']
        response = self.client.post('/api/companies', data=json.dumps({'name': 'Neds Fries', 'assets': 50, 'liabilities': 40}), content_type='application/json')
        company_id_2 = json.loads(response.data)['id']

        self.client.patch('/schema/specs/company', data=json.dumps(
            {'profit': {'type': 'calc', 'calc': 'self.assets - self.liabilities', 'calc_type': 'int'}}), content_type='application/json')

        response = self.client.get('/api/companies/%s' % (company_id_1,))
        company_1 = json.loads(response.data)
        response = self.client.get('/api/companies/%s' % (company_id_2,))
        company_2 = json.loads(response.data)

        self.assertEquals(20, company_1['profit'])
        self.assertEquals(10, company_2['profit'])

    def test_cannot_add_invalid_calc(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'profit': {'type': 'calc', 'calc': 'self.NONEXISTANT - self.liabilities', 'calc_type': 'int'}}), content_type='application/json')

        self.assertEquals(400, response.status_code)
        self.assertEquals({"error": "u'NONEXISTANT'"}, json.loads(response.data))

    def test_cannot_add_invalid_type(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'profit': {'type': 'nonexistant'}}), content_type='application/json')

        self.assertEquals(400, response.status_code)
        self.assertEquals({"error": "u'nonexistant'"}, json.loads(response.data))

    def test_cannot_add_reserved_word(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'self': {'type': 'nonexistant'}}), content_type='application/json')
        self.assertEquals(400, response.status_code)
        self.assertEquals({"error": "u'nonexistant'"}, json.loads(response.data))

    def test_cannot_add_link_prefixed_field(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'link_something': {'type': 'company'}}), content_type='application/json')
        self.assertEquals(400, response.status_code)
        self.assertEquals({u'error': u"Fields cannot start with 'link_' (reserved for interal use)"}, json.loads(response.data))
