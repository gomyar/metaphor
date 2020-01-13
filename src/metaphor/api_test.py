
import unittest

from pymongo import MongoClient

from metaphor.schema import Schema
from metaphor.api import Api


class ApiTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

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

        self.api = Api(self.schema)

    def test_get(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_3, {'division': division_id_2})

        employee_1 = self.api.get('employees/%s' % employee_id_1)
        self.assertEquals({
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'division': '/divisions/%s' % division_id_1,
        }, employee_1)

        employee_2 = self.api.get('employees/%s' % employee_id_2)
        self.assertEquals({
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'division': '/divisions/%s' % division_id_1,
        }, employee_2)

        division_1 = self.api.get('divisions/%s' % division_id_1)
        self.assertEquals({
            'id': division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'name': 'sales',
            'yearly_sales': 100,
            'link_employee_division': '/employees/%s' % employee_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
        }, division_1)

        linked_division_1 = self.api.get('employees/%s/division' % employee_id_1)
        self.assertEquals({
            'id': division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'name': 'sales',
            'yearly_sales': 100,
            'link_employee_division': '/employees/%s' % employee_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
        }, linked_division_1)

        division_2 = self.api.get('divisions/%s' % division_id_2)
        self.assertEquals({
            'id': division_id_2,
            'self': '/divisions/%s' % division_id_2,
            'name': 'marketting',
            'yearly_sales': 20,
            'link_employee_division': '/employees/%s' % employee_id_3,
            'sections': '/divisions/%s/sections' % division_id_2,
        }, division_2)

    def test_get_reverse_link(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_3, {'division': division_id_2})

        linked_employees = self.api.get('divisions/%s/link_employee_division' % division_id_1)
        self.assertEquals([{
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'division': '/divisions/%s' % division_id_1,
        }, {
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'division': '/divisions/%s' % division_id_1,
        }
        ], linked_employees)

        linked_employees_2 = self.api.get('divisions/%s/link_employee_division' % division_id_2)
        self.assertEquals([{
            'id': employee_id_3,
            'self': '/employees/%s' % employee_id_3,
            'name': 'fred',
            'age': 21,
            'division': '/divisions/%s' % division_id_2,
        }], linked_employees_2)

    def test_collections_and_null_links(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})

        employees = self.api.get('/employees')
        self.assertEquals([{
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'division': '/divisions/%s' % division_id_1,
        },
        {
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'division': '/divisions/%s' % division_id_1,
        },
        {
            'id': employee_id_3,
            'self': '/employees/%s' % employee_id_3,
            'name': 'fred',
            'age': 21,
            'division': None,
        }], employees)

    def test_canonical_url(self):
        self.db.metaphor_schema.drop()
        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "section": {
                            "type": "link",
                            "target_spec_name": "section",
                        },
                    },
                },
                "division": {
                    "fields": {
                        "name": {
                            "type": "str",
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

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned'}, 'employees')
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales'}, 'divisions')
        section_id_1 = self.schema.insert_resource('section', {'name': 'appropriation'}, parent_type='division', parent_id=division_id_1, parent_field_name='sections')

        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEquals(None, employee['section'])

        self.schema.update_resource_fields('employee', employee_id_1, {'section': section_id_1})

        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEquals('/divisions/%s/sections/%s' % (division_id_1, section_id_1), employee['section'])

        self.assertEquals({
            '_id': self.schema.decodeid(section_id_1),
            '_parent_id': self.schema.decodeid(division_id_1),
            '_parent_type': 'division',
            '_parent_field_name': 'sections',
            '_parent_canonical_url': '/divisions/%s' % division_id_1,
            'name': 'appropriation',
        }, self.db['resource_section'].find_one({'_id': self.schema.decodeid(section_id_1)}))

        self.assertEquals({
            'id': division_id_1,
            'name': 'sales',
            'self': '/divisions/%s' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
        }, self.api.get('/divisions/%s' % division_id_1))

        self.assertEquals([{
            'id': section_id_1,
            'link_employee_section': '/employees/%s' % employee_id_1,
            'name': 'appropriation',
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % (division_id_1,)))

    def test_post(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})

        employee_id_2 = self.api.post('/employees', {'name': 'bob', 'age': 31})

        self.assertEquals(2, len(self.api.get('/employees')))
        new_employees = list(self.db['resource_employee'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 41,
             'division': self.schema.decodeid(division_id_1),
             'name': 'ned'},
            {'_id': self.schema.decodeid(employee_id_2),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 31,
             'name': 'bob'}], new_employees)

    def test_post_lower(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        section_id_1 = self.api.post('/divisions/%s/sections' % division_id_1, {'name': 'appropriation'})

        self.assertEquals([{
            'name': 'appropriation',
            'id': section_id_1,
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % division_id_1))
        new_sections = list(self.db['resource_section'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(section_id_1),
             '_parent_canonical_url': '/divisions/%s' % division_id_1,
             '_parent_field_name': 'sections',
             '_parent_id': self.schema.decodeid(division_id_1),
             '_parent_type': 'division',
             'name': 'appropriation'}], new_sections)

    def test_reserved_words(self):
        # link_*
        # parent_*
        # root
        # self
        # id
        # _*
        # [0-9]*
        pass
