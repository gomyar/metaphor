
import unittest
from datetime import datetime

from bson.objectid import ObjectId
from pymongo import MongoClient

from metaphor.schema import Schema, Spec, Field
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash


class SchemaTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)
        self.maxDiff = None

    def test_int_type(self):
        self.db.metaphor_schema.insert_one({
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
        self.schema.load_schema()
        self.assertEquals(1, len(self.schema.specs))
        self.assertEquals("int", self.schema.specs['employee'].fields['age'].field_type)

        self.assertEquals([], self.schema.validate_spec('employee', {'age': 12}))

        self.assertEquals([{'error': "Nonexistant field: 'name'"}],
                          self.schema.validate_spec('employee', {'name': "12"}))

        self.assertEquals([{'error': "Invalid type: str for field 'age' of 'employee' (expected 'int')"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

    def test_str_type(self):
        self.db.metaphor_schema.insert_one({
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
        self.schema.load_schema()
        self.assertEquals(1, len(self.schema.specs))
        self.assertEquals("str", self.schema.specs['employee'].fields['name'].field_type)

        self.assertEquals([], self.schema.validate_spec('employee', {'name': 'Bob'}))

        self.assertEquals([{'error': "Nonexistant field: 'age'"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

        self.assertEquals([{'error': "Invalid type: int for field 'name' of 'employee' (expected 'str')"}],
                          self.schema.validate_spec('employee', {'name': 12}))

    def test_bool_type(self):
        self.db.metaphor_schema.insert_one({
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
        self.schema.load_schema()
        self.assertEquals(1, len(self.schema.specs))
        self.assertEquals("bool", self.schema.specs['employee'].fields['admin'].field_type)

        self.assertEquals([], self.schema.validate_spec('employee', {'admin': True}))

        self.assertEquals([{'error': "Nonexistant field: 'age'"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

        self.assertEquals([{'error': "Invalid type: int for field 'admin' of 'employee' (expected 'bool')"}],
                          self.schema.validate_spec('employee', {'admin': 12}))


    def test_float_type(self):
        self.db.metaphor_schema.insert_one({
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
        self.schema.load_schema()
        self.assertEquals(1, len(self.schema.specs))
        self.assertEquals("float", self.schema.specs['employee'].fields['salary'].field_type)

        self.assertEquals([], self.schema.validate_spec('employee', {'salary': 22.33}))
        self.assertEquals([], self.schema.validate_spec('employee', {'salary': 22}))

        self.assertEquals([{'error': "Nonexistant field: 'age'"}],
                          self.schema.validate_spec('employee', {'age': "12"}))

    def test_datetime_type(self):
        self.db.metaphor_schema.insert_one({
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
        self.schema.load_schema()
        self.assertEquals(1, len(self.schema.specs))
        self.assertEquals("datetime", self.schema.specs['employee'].fields['created'].field_type)

        self.assertEquals([], self.schema.validate_spec('employee', {'created': "2021-12-21T12:30:40"}))

        self.assertEqual([{'error': "Invalid type for field 'created' (expected 'str')"}],
                         self.schema.validate_spec('employee', {'created': 12}))

        self.assertEquals([{'error': "Invalid date string for field 'created' (expected ISO format)"}],
                          self.schema.validate_spec('employee', {'created': "March 12"}))

        self.schema.insert_resource('employee', {'created': "2021-12-31T12:30:20"}, 'employees')

        inserted = self.db.resource_employee.find_one()
        self.assertEqual(datetime(2021, 12, 31, 12, 30, 20), inserted['created'])

    def test_required_field(self):
        self.db.metaphor_schema.insert_one({
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
        self.schema.load_schema()
        self.assertEquals(1, len(self.schema.specs))
        self.assertEquals(True, self.schema.specs['employee'].fields['name'].required)

        self.assertEquals([], self.schema.validate_spec('employee', {'name': 'Bob'}))

        self.assertEquals([{'error': "Missing required field: 'name'"}],
                          self.schema.validate_spec('employee', {'address': "12 Road"}))

