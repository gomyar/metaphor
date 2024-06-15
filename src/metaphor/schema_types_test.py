
import unittest
from datetime import datetime

from bson.objectid import ObjectId
from metaphor.mongoclient_testutils import mongo_connection

from metaphor.schema import Schema, Spec, Field
from metaphor.schema_factory import SchemaFactory
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash


class SchemaTest(unittest.TestCase):
    def setUp(self):
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)
        self.maxDiff = None

    def _create_test_schema(self, data):
        data['current'] = True
        data['version'] = 'test'
        data['root'] = data.get('root', {})
        inserted = self.db.metaphor_schema.insert_one(data)
        self.schema = SchemaFactory(self.db).load_current_schema()

    def test_int_type(self):
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "age": {
                            "type": "int"
                        }
                    },
                },
            }
        })
        self.assertEqual(1, len(self.schema.specs))
        self.assertEqual("int", self.schema.specs['employee'].fields['age'].field_type)

        self.assertEqual([], self.schema.validate_spec('employee', {'age': 12}))

        self.assertEqual([{'error': "Nonexistant field: 'name'"}],
                          self.schema.validate_spec('employee', {'name': "12"}))

        self.assertEqual([{'error': "Invalid type: str for field 'age' of 'employee' (expected 'int')"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

    def test_str_type(self):
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name": {
                            "type": "str"
                        }
                    },
                },
            }
        })

        self.assertEqual(1, len(self.schema.specs))
        self.assertEqual("str", self.schema.specs['employee'].fields['name'].field_type)

        self.assertEqual([], self.schema.validate_spec('employee', {'name': 'Bob'}))

        self.assertEqual([{'error': "Nonexistant field: 'age'"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

        self.assertEqual([{'error': "Invalid type: int for field 'name' of 'employee' (expected 'str')"}],
                          self.schema.validate_spec('employee', {'name': 12}))

    def test_bool_type(self):
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "admin": {
                            "type": "bool"
                        }
                    },
                },
            }
        })

        self.assertEqual(1, len(self.schema.specs))
        self.assertEqual("bool", self.schema.specs['employee'].fields['admin'].field_type)

        self.assertEqual([], self.schema.validate_spec('employee', {'admin': True}))

        self.assertEqual([{'error': "Nonexistant field: 'age'"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

        self.assertEqual([{'error': "Invalid type: int for field 'admin' of 'employee' (expected 'bool')"}],
                          self.schema.validate_spec('employee', {'admin': 12}))


    def test_float_type(self):
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "salary": {
                            "type": "float"
                        }
                    },
                },
            }
        })

        self.assertEqual(1, len(self.schema.specs))
        self.assertEqual("float", self.schema.specs['employee'].fields['salary'].field_type)

        self.assertEqual([], self.schema.validate_spec('employee', {'salary': 22.33}))
        self.assertEqual([], self.schema.validate_spec('employee', {'salary': 22}))

        self.assertEqual([{'error': "Nonexistant field: 'age'"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

    def test_datetime_type(self):
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "created": {
                            "type": "datetime"
                        }
                    },
                },
            },
            "root": {
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                },
            },
        })

        self.assertEqual(1, len(self.schema.specs))
        self.assertEqual("datetime", self.schema.specs['employee'].fields['created'].field_type)

        self.assertEqual([], self.schema.validate_spec('employee', {'created': "2021-12-21T12:30:40"}))

        self.assertEqual([{'error': "Invalid type for field 'created' (expected 'str')"}],
                         self.schema.validate_spec('employee', {'created': 12}))

        self.assertEqual([{'error': "Invalid date string for field 'created' (expected ISO format)"}],
                          self.schema.validate_spec('employee', {'created': "March 12"}))

        self.schema.insert_resource('employee', {'created': "2021-12-31T12:30:20"}, 'employees')

        inserted = self.db.metaphor_resource.find_one({'_type': 'employee'})
        self.assertEqual(datetime(2021, 12, 31, 12, 30, 20), inserted['created'])

    def test_required_field(self):
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name": {
                            "type": "str",
                            "required": True
                        },
                        "address": {
                            "type": "str"
                        }
                    },
                },
            }
        })

        self.assertEqual(1, len(self.schema.specs))
        self.assertEqual(True, self.schema.specs['employee'].fields['name'].required)

        self.assertEqual([], self.schema.validate_spec('employee', {'name': 'Bob'}))

        self.assertEqual([{'error': "Missing required field: 'name'"}],
                          self.schema.validate_spec('employee', {'address': "12 Road"}))

