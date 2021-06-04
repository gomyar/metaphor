
import unittest
from urllib.error import HTTPError

from pymongo import MongoClient

from metaphor.schema import Schema
from metaphor.api import Api


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.schema.create_initial_schema()
        self.schema.load_schema()
        self.user_spec = self.schema.specs['user']

        self.api = Api(self.schema)

    def test_get(self):
        self.schema.create_user('bob', 'password')

        self.assertEqual({
            'name': 'bob',
        }, self.api.get('/ego'))

        company = self.schema.add_spec('company')
        self.schema.add_field(company, 'name', 'str')

        self.schema.add_field(self.user_spec, 'company', 'link', 'company')

        self.assertEqual({
            'name': 'bob',
            'company': None,
        }, self.api.get('/ego'))
