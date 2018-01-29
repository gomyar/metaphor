
import unittest

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

        self.api = MongoApi('http://server/api', schema, self.db)
        self.schema_api = SchemaApi('http://server/schema', schema, self.db)

#        self.app = Flask(__name__)
#        self.app.secret_key = "1234test"
        self.client = app.test_client()


    def test_add_root(self):
        self.assertEquals(0, len(schema.specs))
        self.schema_api.post('specs', {'name': 'company'})
        self.assertEquals(1, len(schema.specs))

        self.assertEquals('company', schema.specs['company'].name)

    def test_server(self):
        res = self.client.get('/schema')
        self.assertEquals(200, res.status_code)
        self.assertEquals('[{"name": "company", "fields": {"name": {"type": "str"}}}]', res.data)
