
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
        expected = [{u'fields': {u'portfolios': {u'target_spec': u'portfolio', u'spec': u'collection'}, u'name': {u'type': u'string', u'spec': u'field'}}, u'name': u'org', u'spec': u'resource'}, {u'fields': {u'maxAssets': {u'calc': u'max(self.periods.totalAssets)', u'spec': u'calc'}, u'averageGrossProfit': {u'calc': u'average(self.periods.grossProfit)', u'spec': u'calc'}, u'name': {u'type': u'string', u'spec': u'field'}, u'minAssets': {u'calc': u'min(self.periods.totalAssets)', u'spec': u'calc'}, u'periods': {u'target_spec': u'period', u'spec': u'collection'}}, u'name': u'company', u'spec': u'resource'}, {u'fields': {u'organizations': {u'target_spec': u'org', u'spec': u'collection'}, u'companies': {u'target_spec': u'company', u'spec': u'collection'}, u'portfolios': {u'target_spec': u'portfolio', u'spec': u'collection'}}, u'name': u'root', u'spec': u'resource'}, {u'fields': {u'grossProfit': {u'calc': u'self.totalAssets - self.totalLiabilities', u'spec': u'calc'}, u'totalLiabilities': {u'type': u'int', u'spec': u'field'}, u'period': {u'type': u'string', u'spec': u'field'}, u'totalAssets': {u'type': u'int', u'spec': u'field'}, u'year': {u'type': u'int', u'spec': u'field'}}, u'name': u'period', u'spec': u'resource'}, {u'fields': {u'companies': {u'target_spec': u'company', u'spec': u'collection'}, u'name': {u'type': u'string', u'spec': u'field'}}, u'name': u'portfolio', u'spec': u'resource'}]
        self.assertEquals(expected, json.loads(res.data))
