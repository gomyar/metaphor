
import unittest

from bson.objectid import ObjectId
from pymongo import MongoClient

from metaphor.schema import Schema, Spec, Field


class SchemaTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

    def test_load_basic_spec(self):
        self.db.metaphor_schema.insert_one({
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
        self.schema.load_schema()
        self.assertEquals(1, len(self.schema.specs))
        self.assertEquals("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEquals("int", self.schema.specs['employee'].fields['age'].field_type)

    def test_load_basic_link_with_reverse_link(self):
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                    },
                },
                "department" : {
                    "fields" : {
                        "manager" : {
                            "type" : "link",
                            "target_spec_name": "employee",
                        },
                    },
                },

            }
        })
        self.schema.load_schema()
        self.assertEquals(2, len(self.schema.specs))
        self.assertEquals("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEquals("link", self.schema.specs['department'].fields['manager'].field_type)
        self.assertEquals("employee", self.schema.specs['department'].fields['manager'].target_spec_name)

        self.assertEquals("reverse_link", self.schema.specs['employee'].fields['link_department_manager'].field_type)
        self.assertEquals("department", self.schema.specs['employee'].fields['link_department_manager'].target_spec_name)

    def test_load_collection_with_parent_link(self):
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                    },
                },
                "department" : {
                    "fields" : {
                        "employees" : {
                            "type" : "collection",
                            "target_spec_name": "employee",
                        },
                    },
                },

            }
        })
        self.schema.load_schema()
        self.assertEquals(2, len(self.schema.specs))
        self.assertEquals("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEquals("collection", self.schema.specs['department'].fields['employees'].field_type)
        self.assertEquals("employee", self.schema.specs['department'].fields['employees'].target_spec_name)

        self.assertEquals("parent_collection", self.schema.specs['employee'].fields['parent_department_employees'].field_type)
        self.assertEquals("department", self.schema.specs['employee'].fields['parent_department_employees'].target_spec_name)

    def test_load_link_collection_with_reverse_link(self):
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                    },
                },
                "department" : {
                    "fields" : {
                        "parttimers" : {
                            "type" : "linkcollection",
                            "target_spec_name": "employee",
                        },
                    },
                },

            }
        })
        self.schema.load_schema()
        self.assertEquals(2, len(self.schema.specs))
        self.assertEquals("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEquals("linkcollection", self.schema.specs['department'].fields['parttimers'].field_type)
        self.assertEquals("employee", self.schema.specs['department'].fields['parttimers'].target_spec_name)

        self.assertEquals("reverse_link_collection", self.schema.specs['employee'].fields['link_department_parttimers'].field_type)
        self.assertEquals("department", self.schema.specs['employee'].fields['link_department_parttimers'].target_spec_name)

    def test_save_resource_encode_id(self):
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                    },
                },
            }
        })
        self.schema.load_schema()
        employee_id = self.schema.insert_resource('employee', {
            'name': 'Bob'
        })
        new_resource = self.db.resource_employee.find_one()
        self.assertEquals(ObjectId(employee_id[2:]), self.schema.decodeid(employee_id))
        self.assertEquals({
            '_id': self.schema.decodeid(employee_id),
            'name': 'Bob',
        }, new_resource)
        self.assertEquals('ID%s' % (new_resource['_id'],), employee_id)

    def test_update_field(self):
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                    },
                },
            }
        })
        self.schema.load_schema()
        employee_id = self.schema.insert_resource('employee', {
            'name': 'Bob'
        })
        self.schema.update_resource_fields('employee', employee_id, {'name': 'Ned'})
        reload_employee = self.db.resource_employee.find_one({'_id': self.schema.decodeid(employee_id)})
        self.assertEquals('Ned', reload_employee['name'])

    def test_validate_spec_data(self):
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                    },
                },
                "department" : {
                    "fields" : {
                        "employees" : {
                            "type" : "collection",
                            "target_spec_name": "employee",
                        },
                    },
                },

            }
        })
        self.schema.load_schema()

        self.assertEquals([], self.schema.validate_spec('employee', {'name': 'Bob'}))
        self.assertEquals([{'error': "Invalid type: int for field 'name' of 'employee'"}],
                          self.schema.validate_spec('employee', {'name': 12}))
