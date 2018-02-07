
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
            'company': {'fields': {}, 'type': 'resource'},
            'root': {'fields': {}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_add_resource_with_fields(self):
        self.assertEquals(1, len(self.schema.specs))
        self.client.post('schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        self.assertEquals(2, len(self.schema.specs))

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('str', self.schema.specs['company'].fields['name'].field_type)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'},
             'root': {'fields': {}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'},
             'root': {'fields': {}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_update_resource_spec(self):
        self.client.post('schema/specs', data=json.dumps({'name': 'company'}), content_type='application/json')
        self.client.patch('schema/specs/company', data=json.dumps({'assets': {'type': 'int'}}), content_type='application/json')

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('int', self.schema.specs['company'].fields['assets'].field_type)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {u'company': {u'fields': {u'assets': {u'type': u'int'}}, u'type': u'resource'},
             u'root': {u'fields': {}, u'type': u'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_server_default(self):
        res = self.client.get('/schema/')
        self.assertEquals(200, res.status_code)
        expected = [{'fields': {}, 'name': 'root', 'spec': 'resource'}]
        self.assertEquals(expected, json.loads(res.data))

    def test_add_root_and_post(self):
        resp = self.client.post('/schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        resp = self.client.post('api/companies', data=json.dumps({'name': 'Fred'}), content_type='application/json')

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
             {u'company': {u'fields': {u'name': {u'type': u'str'}}, u'type': u'resource'},
              u'root': {u'fields': {u'companies': {u'target': u'company',
                                                   u'type': u'collection'}},
                                                            u'type': u'resource'}},
             saved_schema['specs'])
        self.assertEquals({'companies': {'type': 'collection', 'target': 'company'}}, saved_schema['roots'])
