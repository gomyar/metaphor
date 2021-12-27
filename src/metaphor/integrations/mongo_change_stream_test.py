
import unittest
from unittest import mock

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api

from metaphor.integrations.mongo_change_stream import MongoChangeStreamIntegration


class Stream:
    def __init__(self, events):
        self.events = events

    def __enter__(self):
        return self.events

    def __exit__(self, *args):
        pass

    def __iter__(self):
        for event in self.events:
            yield event


class MongoTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        #client.drop_database('mataphor2_test_db_integration')
        self.db = client.metaphor2_test_db
        #self.source_db = client.metaphor2_test_db_integration

        self.schema = Schema(self.db)
        self.schema.load_schema()

        self.api = Api(self.schema)

        self.events = [
            {"operationType": "insert"},
        ]

        watched_collection = mock.Mock()
        watched_collection.watch.return_value = Stream(self.events)

        self.source_db = {'external_collection': watched_collection}

        self.integration = MongoChangeStreamIntegration(
            self.source_db, 'external_collection', [], self.api, self._process_change_func)

        self._call_log = []

    def _process_change_func(self, change_doc, source_db, api):
        self._call_log.append((change_doc, source_db, api))

    def test_run_integration(self):
        self.source_db['external_collection'].insert({'a': 1})
        self.integration.process()

        self.assertEqual(1, len(self._call_log))

        self.assertEqual((self.events[0], self.source_db, self.api), self._call_log[0])

    def test_multiple_events(self):
        self.events.append({'operationType': 'update'})

        self.source_db['external_collection'].insert({'a': 1})
        self.integration.process()

        self.assertEqual(2, len(self._call_log))

        self.assertEqual((self.events[0], self.source_db, self.api), self._call_log[0])
        self.assertEqual((self.events[1], self.source_db, self.api), self._call_log[1])
