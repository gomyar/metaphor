
import unittest
import json

from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi
from metaphor.api import SchemaApi
from flask import Flask
from server import app
from server import schema


class SchemaUpdateTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, "1.1")

        self.api = MongoApi('http://server/api', self.schema, self.db)
        self.schema_api = SchemaApi('http://server/schema', self.schema, self.db)

#        self.app = Flask(__name__)
#        self.app.secret_key = "1234test"
        self.client = app.test_client()

    def test_add_resource(self):
        self.assertEquals(1, len(self.schema.specs))
        self.schema_api.post('specs', {'name': 'company'})
        self.assertEquals(2, len(self.schema.specs))

        self.assertEquals('company', self.schema.specs['company'].name)

    def test_add_resource_with_fields(self):
        self.assertEquals(1, len(self.schema.specs))
        self.schema_api.post('specs', {'name': 'company', 'fields': {'name': {'type': 'str'}}})
        self.assertEquals(2, len(self.schema.specs))

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('str', self.schema.specs['company'].fields['name'].field_type)

    def test_update_resource_spec(self):
        self.schema_api.post('specs', {'name': 'company'})
        self.schema_api.patch('company', {'assets': {'type': 'int'}})

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('int', self.schema.specs['company'].fields['assets'].field_type)

    def test_server_default(self):
        res = self.client.get('/schema')
        self.assertEquals(200, res.status_code)
        expected = [{u'fields': {}, u'name': u'root', u'spec': u'resource'}]
        self.assertEquals(expected, json.loads(res.data))
