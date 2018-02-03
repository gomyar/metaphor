
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
from flask import Flask
from server import app
from server import schema


class SchemaUpdateTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        schema.reset()
        schema.updater.start_updater()
        self.db = client.metaphor_test_db

        self.api = MongoApi('http://server/api', schema, self.db)
        self.schema_api = SchemaApi('http://server/schema', schema, self.db)
        self.roots_api = RootsApi('http://server/roots', schema, self.db)

        self.client = app.test_client()

    def test_add_resource(self):
        self.assertEquals(1, len(schema.specs))
        self.schema_api.post('specs', {'name': 'company'})
        self.assertEquals(2, len(schema.specs))

        self.assertEquals('company', schema.specs['company'].name)

    def test_add_resource_with_fields(self):
        self.assertEquals(1, len(schema.specs))
        self.schema_api.post('specs', {'name': 'company', 'fields': {'name': {'type': 'str'}}})
        self.assertEquals(2, len(schema.specs))

        self.assertEquals('company', schema.specs['company'].name)
        self.assertEquals('str', schema.specs['company'].fields['name'].field_type)

    def test_update_resource_spec(self):
        self.schema_api.post('specs', {'name': 'company'})
        self.schema_api.patch('company', {'assets': {'type': 'int'}})

        self.assertEquals('company', schema.specs['company'].name)
        self.assertEquals('int', schema.specs['company'].fields['assets'].field_type)

    def test_server_default(self):
        res = self.client.get('/schema')
        self.assertEquals(200, res.status_code)
        expected = [{u'fields': {}, u'name': u'root', u'spec': u'resource'}]
        self.assertEquals(expected, json.loads(res.data))

    def test_add_root_and_post(self):
        self.schema_api.post('specs', {'name': 'company', 'fields': {'name': {'type': 'str'}}})
        self.roots_api.post('companies', 'company')

        self.api.post('companies', {'name': 'Fred'})
