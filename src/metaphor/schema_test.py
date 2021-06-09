
import unittest

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

            },
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
        }, 'employees')
        new_resource = self.db.resource_employee.find_one()
        self.assertEquals(ObjectId(employee_id[2:]), self.schema.decodeid(employee_id))
        self.assertEquals({
            '_id': self.schema.decodeid(employee_id),
            '_grants': [],
            '_canonical_url': '/employees/%s' % employee_id,
            'name': 'Bob',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
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
        }, 'employees')
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

    def test_roots(self):
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
            },
            "root": {
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                },
                "departments": {
                    "type": "collection",
                    "target_spec_name": "department",
                }
            },
        })
        self.schema.load_schema()
        self.assertEquals(2, len(self.schema.specs))
        self.assertEquals(2, len(self.schema.root.fields))
        self.assertEquals('collection', self.schema.root.fields['employees'].field_type)
        self.assertEquals('employee', self.schema.root.fields['employees'].target_spec_name)
        self.assertEquals('collection', self.schema.root.fields['departments'].field_type)
        self.assertEquals('department', self.schema.root.fields['departments'].target_spec_name)

    def test_canonical_url(self):
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "age": {
                            "type": "int"
                        },
                        "division": {
                            "type": "link",
                            "target_spec_name": "division",
                        },
                    },
                },
                "division": {
                    "fields": {
                        "name": {
                            "type": "str",
                        },
                        "yearly_sales": {
                            "type": "int",
                        },
                        "sections": {
                            "type": "collection",
                            "target_spec_name": "section",
                        }
                    },
                },
                "section": {
                    "fields": {
                        "name": {
                            "type": "str",
                        },
                    },
                },
            },
            "root": {
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                },
                "divisions": {
                    "type": "collection",
                    "target_spec_name": "division",
                }
            },
        })
        self.schema.load_schema()

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_grants': [],
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_parent_id': None,
            '_parent_type': 'root',
            '_parent_field_name': 'divisions',
            '_parent_canonical_url': '/',
            'name': 'sales',
            'yearly_sales': 100,
        }, self.db['resource_division'].find_one({'_id': self.schema.decodeid(division_id_1)}))

        section_id_1 = self.schema.insert_resource('section', {'name': 'appropriation'}, parent_type='division', parent_id=division_id_1, parent_field_name='sections')

        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_grants': [],
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_parent_id': None,
            '_parent_type': 'root',
            '_parent_field_name': 'divisions',
            '_parent_canonical_url': '/',
            'name': 'sales',
            'yearly_sales': 100,
        }, self.db['resource_division'].find_one({'_id': self.schema.decodeid(division_id_1)}))


        self.assertEquals({
            '_id': self.schema.decodeid(section_id_1),
            '_grants': [],
            '_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            '_parent_id': self.schema.decodeid(division_id_1),
            '_parent_type': 'division',
            '_parent_field_name': 'sections',
            '_parent_canonical_url': '/divisions/%s' % division_id_1,
            'name': 'appropriation',
        }, self.db['resource_section'].find_one({'_id': self.schema.decodeid(section_id_1)}))

    def test_calc_infer_type(self):
        spec = self.schema.add_spec('employees')
        self.schema.add_field(spec, 'name', 'str')
        calc_field = self.schema.add_calc(spec, 'current_name', 'self.name')

        self.assertEquals('str', calc_field.infer_type().field_type)
        self.assertTrue(calc_field.is_primitive())
        self.assertFalse(calc_field.is_collection())

    def test_calc_infer_type_collection(self):
        spec = self.schema.add_spec('employees')
        buddy_spec = self.schema.add_spec('buddy')
        self.schema.add_field(spec, 'buddies', 'collection', 'buddy')
        calc_field = self.schema.add_calc(spec, 'all_buddies', 'self.buddies')

        self.assertEquals(buddy_spec, calc_field.infer_type())
        self.assertFalse(calc_field.is_primitive())
        self.assertTrue(calc_field.is_collection())

    def test_parse_fields_test(self):
        # add parsing and validation for field types
        pass

    def test_initialize_schema(self):
        self.schema.create_initial_schema()

        schema = self.db.metaphor_schema.find_one()
        self.assertEqual({
            "groups": {"target_spec_name": "group", "type": "collection"},
            "users": {"target_spec_name": "user", "type": "collection"}}, schema['root'])
        self.assertEqual({
            'grant': {'fields': {'type': {'type': 'str'}, 'url': {'type': 'str'}}},
            'group': {'fields': {'grants': {'target_spec_name': 'grant',
                                            'type': 'collection'},
                                'name': {'type': 'str'}}},
            'user': {'fields': {'create_grants': {'calc_str': "self.groups.grants[type='create'].url",
                                                'type': 'calc'},
                                'delete_grants': {'calc_str': "self.groups.grants[type='delete'].url",
                                                'type': 'calc'},
                                'groups': {'target_spec_name': 'group',
                                            'type': 'linkcollection'},
                                'pw_hash': {'type': 'str'},
                                'read_grants': {'calc_str': "self.groups.grants[type='read'].url",
                                                'type': 'calc'},
                                'update_grants': {'calc_str': "self.groups.grants[type='update'].url",
                                                'type': 'calc'},
                                'username': {'type': 'str'}}}}, schema['specs'])

        groups = list(self.db.resource_group.find())
        self.assertEqual(1, len(groups))
        self.assertEqual('admin', groups[0]['name'])

        grants = list(self.db.resource_grant.find())
        self.assertEqual(8, len(grants))

        pw_hash = generate_password_hash('password')
        self.user_id = self.api.post('/users', {'username': 'bob', 'pw_hash': pw_hash})

        bob = self.db.resource_user.find_one()
        self.assertEqual('bob', bob['username'])
        self.assertTrue(check_password_hash(bob['pw_hash'], 'password'))
