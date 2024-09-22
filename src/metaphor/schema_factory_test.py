
import unittest

from metaphor.mongoclient_testutils import mongo_connection

from metaphor.schema_factory import SchemaFactory
from metaphor.mutation import Mutation


class SchemaFactoryTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.factory = SchemaFactory(self.db)

    def test_create(self):
        schema_1 = self.factory.create_schema()
        schema_2 = self.factory.create_schema()

        self.assertEqual(3, len(schema_1.specs))
        self.assertEqual(3, len(schema_2.specs))

        schema_1.create_spec('org')

#        schema_1.load_schema()
#        schema_2.load_schema()

        self.assertEqual(4, len(schema_1.specs))
        self.assertEqual(3, len(schema_2.specs))

        schemas = self.factory.list_schemas()

        self.assertEqual(2, len(list(schemas)))
