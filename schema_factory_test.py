

import unittest

from metaphor.schema_factory import SchemaFactory


class SchemaFactoryTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db

        self.factory = SchemaFactory()

    def test_create_schema(self):
        schema = self.factory.create_schema(self.db, "1.1", {})

        self.assertEquals(0, len(schema.spec))
