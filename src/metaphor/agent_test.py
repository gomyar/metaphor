
import unittest

from bson.objectid import ObjectId
from pymongo import MongoClient

from metaphor.mongoclient_testutils import mongo_connection
from metaphor.schema import Schema
from metaphor.agent import SchemaEditorAgent
from metaphor.schema_factory import SchemaFactory
from metaphor.schema_serializer import serialize_schema


class AgentTest(unittest.TestCase):

    def setUp(self):
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.maxDiff = None

    def _create_test_schema(self, data):
        data['current'] = True
        data['version'] = 'test'
        data['root'] = data.get('root', {})
        data['name'] = data.get('name')
        data['description'] = data.get('description')
        data['groups'] = {}
        inserted = self.db.metaphor_schema.insert_one(data)
        self.schema = SchemaFactory(self.db).load_current_schema()

    def test_basic_prompt(self):
        self._create_test_schema({
            "name": "Test 1",
            "description": "A description",
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "age": {
                            "type": "int"
                        }
                    },
                },
            }
        })
        self.agent = SchemaEditorAgent(self.schema)

        self.assertEqual("Test 1", self.schema.name)

        schema_json = serialize_schema(self.schema)
        self.assertEqual(['employee', 'root'], sorted(schema_json['specs'].keys()))

        self.agent.prompt("Add an egg spec")
        schema_json = serialize_schema(self.schema)
        self.assertEqual(['egg', 'employee', 'root'], sorted(schema_json['specs'].keys()))

        self.agent.prompt("Add name to egg")
        schema_json = serialize_schema(self.schema)
        egg_spec = schema_json['specs']['egg']
        self.assertEqual(1, len(egg_spec['fields']))
        self.assertEqual("str", egg_spec['fields']['name']['type'])

    def test_calc_prompt(self):
        self._create_test_schema({
            "name": "Test 1",
            "description": "A description",
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "age": {
                            "type": "int"
                        }
                    },
                },
            }
        })
        self.agent = SchemaEditorAgent(self.schema)

        self.agent.prompt("Add a field to employee called years to retirement which is 65 minus the age")
        schema_json = serialize_schema(self.schema)
        employee_spec = schema_json['specs']['employee']
        self.assertEqual(3, len(employee_spec['fields']))
        calc_field = self.schema.specs['employee'].fields['years_to_retirement']
        self.assertEqual('int', calc_field.infer_type().field_type)
