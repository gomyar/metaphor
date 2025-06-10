
import unittest

from bson.objectid import ObjectId
from pymongo import MongoClient

from metaphor.mongoclient_testutils import mongo_connection
from metaphor.schema import Schema
from metaphor.api_agent import ApiAgent
from metaphor.schema_factory import SchemaFactory
from metaphor.schema_serializer import serialize_schema
from metaphor.api import Api


class AgentTest(unittest.TestCase):

    def setUp(self):
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.maxDiff = None
        self.api = Api(self.db)

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
            "root": {
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                    "name": "employees",
                }
            },
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
        self.agent = ApiAgent(self.api, self.schema)

        self.agent.prompt("Create an employee called Bob")

        employees = self.api.get("/employees")
        self.assertEqual(1, employees['count'])
        employee = employees['results'][0]
        self.assertEqual("Bob", employee['name'])
        self.assertEqual(None, employee['age'])

    def test_link_prompt(self):
        self._create_test_schema({
            "name": "Test 1",
            "description": "A description",
            "root": {
                "jobs": {
                    "type": "collection",
                    "target_spec_name": "job",
                    "name": "jobs",
                },
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                    "name": "employees",
                },
            },
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "age": {
                            "type": "int"
                        },
                        "job": {
                            "type": "link",
                            "target_spec_name": "job"
                        },
                    },
                },
                "job": {
                    "fields": {
                        "title": {
                            "type": "str"
                        }
                    },
                },
            }
        })

        employee_id_1 = self.api.post('/employees', {'name': 'Bob', 'age': 31})
        job_id_1 = self.api.post('/jobs', {'title': 'Manager'})

        self.agent = ApiAgent(self.api, self.schema)

        self.agent.prompt("Link the employee bob to the manager job")

        employees = self.api.get("/employees")
        self.assertEqual(1, employees['count'])
        employee = employees['results'][0]
        self.assertEqual({"id": job_id_1}, employee['job'])

    def test_link_collection_prompt(self):
        self._create_test_schema({
            "name": "Test 1",
            "description": "A description",
            "root": {
                "jobs": {
                    "type": "collection",
                    "target_spec_name": "job",
                    "name": "jobs",
                },
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                    "name": "employees",
                },
            },
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "age": {
                            "type": "int"
                        },
                        "jobs": {
                            "type": "linkcollection",
                            "target_spec_name": "job"
                        },
                    },
                },
                "job": {
                    "fields": {
                        "title": {
                            "type": "str"
                        }
                    },
                },
            }
        })

        employee_id_1 = self.api.post('/employees', {'name': 'Bob', 'age': 31})
        job_id_1 = self.api.post('/jobs', {'title': 'Manager'})

        self.agent = ApiAgent(self.api, self.schema)

        self.agent.prompt("Add the manager job to Bob")

        employees = self.api.get("/employees", {"expand": "jobs"})
        self.assertEqual(1, employees['count'])
        employee = employees['results'][0]
        self.assertEqual(1, len(employee['jobs']))
        self.assertEqual(job_id_1, employee['jobs'][0]["id"])

    def test_multiple_resources(self):
        self._create_test_schema({
            "name": "Test 1",
            "description": "A description",
            "root": {
                "jobs": {
                    "type": "collection",
                    "target_spec_name": "job",
                    "name": "jobs",
                },
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                    "name": "employees",
                },
            },
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "age": {
                            "type": "int"
                        },
                        "job": {
                            "type": "link",
                            "target_spec_name": "job"
                        },
                    },
                },
                "job": {
                    "fields": {
                        "title": {
                            "type": "str"
                        }
                    },
                },
            }
        })

        job_id_1 = self.api.post('/jobs', {'title': 'Manager'})

        self.agent = ApiAgent(self.api, self.schema)

        result = self.agent.prompt("Add an Employee called Bob with the Manager job")

        employees = self.api.get("/employees")
        self.assertEqual(1, employees['count'])
        employee = employees['results'][0]
        self.assertEqual({"id": job_id_1}, employee['job'])
        self.assertEqual("Bob", employee['name'])
        self.assertEqual(None, employee['age'])


