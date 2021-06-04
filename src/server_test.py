
from unittest import TestCase
from server import create_app
from pymongo import MongoClient
from metaphor.schema import Schema


class ServerTest(TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.schema.create_initial_schema()
        self.schema.load_schema()

        self.app = create_app(self.db)
        self.client = self.app.test_client()

    def test_get(self):
        response = self.client.get('/api/')
        self.assertEqual({'groups': '/groups', 'users': '/users'}, response.json)
