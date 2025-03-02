
import unittest

from bson.objectid import ObjectId
from pymongo import MongoClient

from metaphor.mongoclient_testutils import mongo_connection
from metaphor.schema import Schema, Spec, Field
from metaphor.schema import DependencyException
from metaphor.schema import MalformedFieldException
from metaphor.schema_factory import SchemaFactory
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash


class SchemaTest(unittest.TestCase):
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
        inserted = self.db.metaphor_schema.insert_one(data)
        self.schema = SchemaFactory(self.db).load_current_schema()

    def test_load_basic_spec(self):
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

        self.assertEqual("Test 1", self.schema.name)
        self.assertEqual("A description", self.schema.description)
        self.assertEqual(1, len(self.schema.specs))
        self.assertEqual("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEqual("int", self.schema.specs['employee'].fields['age'].field_type)

    def test_save_details(self):
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
        self.schema.name = "New name"
        self.schema.description = "New desc"

        self.schema.save_details()

        schema_data = self.schema.db['metaphor_schema'].find_one({"_id": self.schema._id})
        self.assertEqual("New name", schema_data['name'])
        self.assertEqual("New desc", schema_data['description'])

    def test_load_basic_link_with_reverse_link(self):
        self._create_test_schema({
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

        self.assertEqual(2, len(self.schema.specs))
        self.assertEqual("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEqual("link", self.schema.specs['department'].fields['manager'].field_type)
        self.assertEqual("employee", self.schema.specs['department'].fields['manager'].target_spec_name)

        self.assertEqual("reverse_link", self.schema.specs['employee'].fields['link_department_manager'].field_type)
        self.assertEqual("department", self.schema.specs['employee'].fields['link_department_manager'].target_spec_name)

    def test_load_collection_with_parent_link(self):
        self._create_test_schema({
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

        self.assertEqual(2, len(self.schema.specs))
        self.assertEqual("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEqual("collection", self.schema.specs['department'].fields['employees'].field_type)
        self.assertEqual("employee", self.schema.specs['department'].fields['employees'].target_spec_name)

        self.assertEqual("parent_collection", self.schema.specs['employee'].fields['parent_department_employees'].field_type)
        self.assertEqual("department", self.schema.specs['employee'].fields['parent_department_employees'].target_spec_name)

    def test_load_link_collection_with_reverse_link(self):
        self._create_test_schema({
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

        self.assertEqual(2, len(self.schema.specs))
        self.assertEqual("str", self.schema.specs['employee'].fields['name'].field_type)
        self.assertEqual("linkcollection", self.schema.specs['department'].fields['parttimers'].field_type)
        self.assertEqual("employee", self.schema.specs['department'].fields['parttimers'].target_spec_name)

        self.assertEqual("reverse_link_collection", self.schema.specs['employee'].fields['link_department_parttimers'].field_type)
        self.assertEqual("department", self.schema.specs['employee'].fields['link_department_parttimers'].target_spec_name)

    def test_save_resource_encode_id(self):
        self._create_test_schema({
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

        employee_id = self.schema.insert_resource('employee', {
            'name': 'Bob'
        }, 'employees')
        new_resource = self.db.resource_employee.find_one({'_type': 'employee'})
        self.assertEqual(ObjectId(employee_id[2:]), self.schema.decodeid(employee_id))
        self.assertEqual({
            '_id': self.schema.decodeid(employee_id),
            '_schema_id': self.schema._id,
            'name': 'Bob',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'employee',
        }, new_resource)
        self.assertEqual('ID%s' % (new_resource['_id'],), employee_id)

    def test_update_field(self):
        self._create_test_schema({
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

        employee_id = self.schema.insert_resource('employee', {
            'name': 'Bob'
        }, 'employees')
        self.schema.update_resource_fields('employee', employee_id, {'name': 'Ned'})
        reload_employee = self.db.resource_employee.find_one({'_id': self.schema.decodeid(employee_id)})
        self.assertEqual('Ned', reload_employee['name'])

    def test_roots(self):
        self._create_test_schema({
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

        self.assertEqual(2, len(self.schema.specs))
        self.assertEqual(2, len(self.schema.root.fields))
        self.assertEqual('collection', self.schema.root.fields['employees'].field_type)
        self.assertEqual('employee', self.schema.root.fields['employees'].target_spec_name)
        self.assertEqual('collection', self.schema.root.fields['departments'].field_type)
        self.assertEqual('department', self.schema.root.fields['departments'].target_spec_name)

    def test_canonical_url(self):
        self._create_test_schema({
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


        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.assertEqual({
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_parent_id': None,
            '_parent_type': 'root',
            '_parent_field_name': 'divisions',
            '_type': 'division',
            'name': 'sales',
            'yearly_sales': 100,
        }, self.db['resource_division'].find_one({'_id': self.schema.decodeid(division_id_1)}))

        section_id_1 = self.schema.insert_resource('section', {'name': 'appropriation'}, parent_type='division', parent_id=division_id_1, parent_field_name='sections')

        self.assertEqual({
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_parent_id': None,
            '_parent_type': 'root',
            '_parent_field_name': 'divisions',
            '_type': 'division',
            'name': 'sales',
            'yearly_sales': 100,
        }, self.db['resource_division'].find_one({'_id': self.schema.decodeid(division_id_1)}))


        self.assertEqual({
            '_id': self.schema.decodeid(section_id_1),
            '_schema_id': self.schema._id,
            '_parent_id': self.schema.decodeid(division_id_1),
            '_parent_type': 'division',
            '_parent_field_name': 'sections',
            '_type': 'section',
            'name': 'appropriation',
        }, self.db['resource_section'].find_one({'_id': self.schema.decodeid(section_id_1)}))

    def test_calc_infer_type(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        spec = self.schema.add_spec('employees')
        self.schema.add_field(spec, 'name', 'str')
        calc_field = self.schema.add_calc(spec, 'current_name', 'self.name')

        self.assertEqual('str', calc_field.infer_type().field_type)
        self.assertTrue(calc_field.is_primitive())
        self.assertFalse(calc_field.is_collection())

    def test_calc_infer_type_collection(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        spec = self.schema.add_spec('employees')
        buddy_spec = self.schema.add_spec('buddy')
        self.schema.add_field(spec, 'buddies', 'collection', 'buddy')
        calc_field = self.schema.add_calc(spec, 'all_buddies', 'self.buddies')

        self.assertEqual(buddy_spec, calc_field.infer_type())
        self.assertFalse(calc_field.is_primitive())
        self.assertTrue(calc_field.is_collection())

    def test_delete_linkcollection_entry(self):
        pass

    def test_parse_fields_test(self):
        # add parsing and validation for field types
        pass

    def test_initialize_schema(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_initial_schema()

        schema = self.db.metaphor_schema.find_one()
        self.assertEqual({
            "groups": {"target_spec_name": "group", "type": "collection"},
            "users": {"target_spec_name": "user", "type": "collection"}}, schema['root'])
        self.assertEqual({'grant': {'fields': {
                'type': {
                    'type': 'str',
                    'default': None,
                    'indexed': None,
                    'required': None,
                    'unique': None,
                    'unique_global': None
                },
                'url': {
                    'type': 'str',
                    'default': None,
                    'indexed': None,
                    'required': None,
                    'unique': None,
                    'unique_global': None
                }}},
            'group': {'fields': {'grants': {'target_spec_name': 'grant',
                                            'type': 'collection'},
                                'name': {
                                    'type': 'str',
                                    'default': None,
                                    'indexed': None,
                                    'required': None,
                                    'unique': None,
                                    'unique_global': None
                                }}},
            'user': {'fields': {'admin': {
                'type': 'bool',
                'default': None,
                'indexed': None,
                'required': None,
                'unique': None,
                'unique_global': None
            },
                                'create_grants': {'calc_str': "self.groups.grants[type='create']",
                                                'deps': ['grant.type',
                                                         'group.grants',
                                                         'user.groups'],
                                                'type': 'calc'},
                                'delete_grants': {'calc_str': "self.groups.grants[type='delete']",
                                                'deps': ['grant.type',
                                                         'group.grants',
                                                         'user.groups'],
                                                'type': 'calc'},
                                'groups': {'target_spec_name': 'group',
                                            'type': 'linkcollection'},
                                'password': {
                                    'type': 'str',
                                    'default': None,
                                    'indexed': None,
                                    'required': None,
                                    'unique': None,
                                    'unique_global': None
                                    },
                      'put_grants': {'calc_str': "self.groups.grants[type='put']",
                                     'deps': ['grant.type',
                                              'group.grants',
                                              'user.groups'],
                                     'type': 'calc'},
                                'read_grants': {'calc_str': "self.groups.grants[type='read']",
                                                'deps': ['grant.type',
                                                         'group.grants',
                                                         'user.groups'],
                                                'type': 'calc'},
                                'update_grants': {'calc_str': "self.groups.grants[type='update']",
                                                'deps': ['grant.type',
                                                         'group.grants',
                                                         'user.groups'],
                                                'type': 'calc'},
                                'username': {
                                    'type': 'str',
                                    'default': None,
                                    'indexed': None,
                                    'required': None,
                                    'unique': None,
                                    'unique_global': None
                                }}}}, schema['specs'])

    def test_load_calcs_by_dependency(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_spec('branch')
        self.schema.create_field('branch', 'income', 'int')
        self.schema.create_field('branch', 'employees', 'collection', 'employee')

        self.schema.create_spec('section')
        self.schema.create_field('section', 'branch', 'link', 'branch')



        self.schema.create_field('employee', 'average_section_income', 'calc', None, 'average(self.parent_branch_employees.income)')
        self.schema.create_field('branch', 'section', 'calc', None, 'self.link_section_branch')
        self.schema.create_field('section', 'employees', 'calc', None, 'self.branch.employees')

        expected ={
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str",
                            'default': None,
                            'indexed': None,
                            'required': None,
                            'type': 'str',
                            'unique': None,
                            'unique_global': None,
                        },
                        "average_section_income": {
                            "type": "calc",
                            "calc_str": "average(self.parent_branch_employees.income)",
                            "deps": ["branch.income"],
                        },
                    },
                },
                "branch" : {
                    "fields" : {
                        "income": {
                            "type": "int",
                            'default': None,
                            'indexed': None,
                            'required': None,
                            'unique': None,
                            'unique_global': None,
                        },
                        "employees" : {
                            "type" : "collection",
                            "target_spec_name": "employee",
                        },
                        "section": {
                            "type": "calc",
                            "calc_str": "self.link_section_branch",
                            "deps": ["section.branch"],
                        },
                    },
                },
                "section" : {
                    "fields" : {
                        "branch" : {
                            "type" : "link",
                            "target_spec_name": "branch",
                        },
                        "employees": {
                            "type": "calc",
                            "calc_str": "self.branch.employees",
                            "deps": ["branch.employees", "section.branch"],
                        },
                    },
                },
        }

        self.assertEqual(expected, self.db.metaphor_schema.find_one()['specs'])


        self.assertEqual(3, len(self.schema.specs))

    def test_load_calcs_by_dependency_almost_circular(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.schema.create_field('primary', 'name', 'str')
        self.schema.create_field('primary', 'calced_name', 'calc', calc_str="self.name + 'a'")

        self.schema.create_spec('secondary')
        self.schema.create_field('secondary', 'name', 'str')
        self.schema.create_field('secondary', 'calced_name', 'calc', calc_str="self.name + 'b'")

        self.schema.create_field('primary', 'secondary', 'link', 'secondary')
        self.schema.create_field('secondary', 'primary', 'link', 'primary')

        self.schema.create_field('primary', 'secondary_name', 'calc', calc_str="self.secondary.calced_name")
        self.schema.create_field('secondary', 'primary_name', 'calc', calc_str="self.primary.calced_name")


        self.assertEqual(2, len(self.schema.specs))

    def test_delete_field_with_dependencies(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.schema.create_field('primary', 'name', 'str')

        self.schema.create_spec('secondary')
        self.schema.create_field('secondary', 'name', 'str')
        self.schema.create_field('secondary', 'primary', 'link', 'primary')
        self.schema.create_field('secondary', 'primary_name', 'calc', calc_str="self.primary.name")

        self.assertEqual({('secondary', 'primary_name'): self.schema.calc_trees[('secondary', 'primary_name')]}, self.schema.all_dependent_calcs_for('primary', 'name'))

        self.schema.delete_field('secondary', 'primary_name')

        self.assertEqual({}, self.schema.all_dependent_calcs_for('primary', 'name'))

    def test_delete_spec_in_collection(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.schema.create_field('primary', 'name', 'str')

        self.schema.create_spec('secondary')
        self.schema.create_field('secondary', 'name', 'str')

        self.schema.create_field('primary', 'secondaries', 'collection', 'secondary')

        try:
            self.schema.delete_spec('secondary')
        except DependencyException as de:
            self.assertEqual("secondary is linked from primary.secondaries", str(de))

    def test_delete_root_field(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.schema.create_field('primary', 'name', 'str')
        self.schema.create_field('root', 'primaries', 'collection', 'primary')

        self.schema.delete_field('root', 'primaries')

        self.assertEqual(0, len(self.schema.root.fields))
        self.assertEqual({}, self.db.metaphor_schema.find_one()['root'])

    def test_delete_root_field_with_dependencies(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.schema.create_field('primary', 'name', 'str')
        self.schema.create_field('root', 'primaries', 'collection', 'primary')

        self.schema.create_spec('secondary')
        self.schema.create_field('secondary', 'name', 'str')
        self.schema.create_field('secondary', 'primary_name', 'calc', calc_str="primaries.name")

        self.assertEqual({('secondary', 'primary_name'): self.schema.calc_trees[('secondary', 'primary_name')]}, self.schema.all_dependent_calcs_for('primary', 'name'))

        try:
            self.schema.delete_field('root', 'primaries')
        except DependencyException as de:
            self.assertEqual("root.primaries referenced by ['secondary.primary_name']", str(de))


    def test_delete_spec(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.schema.create_field('primary', 'name', 'str')
        self.schema.create_field('root', 'primaries', 'collection', 'primary')

        self.schema.create_spec('secondary')
        self.schema.create_field('secondary', 'name', 'str')
        self.schema.create_field('root', 'secondaries', 'collection', 'secondary')

        self.schema.delete_spec("secondary")

        self.assertEqual({'primary': {'fields': {'name': {
            'default': None,
            'indexed': None,
            'required': None,
            'unique': None,
            'unique_global': None,
            'type': 'str'}}}},
                         self.db.metaphor_schema.find_one({"current": True})['specs'])
        self.assertEqual(['primary'], list(self.schema.specs.keys()))

    def test_hash(self):
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "full_name" : {
                            "type" : "str"
                        },
                    },
                },
            }
        })

        self.assertEqual("446df9b7", self.schema.calculate_short_hash())

    def test_field_default(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.schema.create_field('primary', 'name', 'str', default='ned')

        self.schema.insert_resource('primary', {}, 'employees')

        self.assertEqual('ned', self.db.resource_primary.find_one({'_type': 'primary'})['name'])

        self.assertEqual({
            "current": True,
            "root": {},
            "description": None,
            "name": None,
            "_id": self.schema._id,
            "specs" : {
                "primary" : {
                    "fields" : {
                        "name" : {
                            "type" : "str",
                            "default": "ned",
                            'indexed': None,
                            'required': None,
                            'unique': None,
                            'unique_global': None,
                        },
                    },
                },
            },
            "version": "017b77bd",
        }, self.db.metaphor_schema.find_one())

        # test load


        self.assertEqual('ned', self.schema.specs['primary'].fields['name'].default)

    def test_cannot_duplicate(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('primary')
        self.assertRaises(Exception, self.schema.create_spec, 'primary')

    def test_rename_spec(self):
        self._create_test_schema({
            "name": "Test 1",
            "description": "A description",
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

        self.schema.create_spec('organization')
        self.schema.create_field('organization', 'employees', 'collection', 'employee')

        self.schema.rename_spec('employee', 'user')

        self.schema = SchemaFactory(self.db).load_current_schema()

    def test_duplicate_index_check(self):
        self._create_test_schema({
            "name": "Test 1",
            "description": "A description",
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

        employee_id = self.schema.insert_resource('employee', {
            'name': 'Bob'
        }, 'employees')

        self.assertFalse(self.schema.has_global_duplicates("employee", "name"))

        employee_id = self.schema.insert_resource('employee', {
            'name': 'Bob'
        }, 'employees')

        self.assertTrue(self.schema.has_global_duplicates("employee", "name"))


    def test_only_basic_indexes_allowed_on_primitives_for_calc_fields(self):
        self._create_test_schema({
            "specs" : {
            }
        })

        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('root', 'employees', 'collection', 'employee')

        with self.assertRaises(MalformedFieldException) as me:
            self.schema.create_field('employee', 'title', 'calc', calc_str="'mr '+self.name", indexed=True, unique=True)
            self.assertEqual("Unique index not allowed for calc field", str(me))

        with self.assertRaises(MalformedFieldException) as me:
            self.schema.create_field('employee', 'title', 'calc', calc_str="employees", indexed=True)
            self.assertEqual("Index not allowed for non-primitive calc field", str(me))

