
import unittest
from urllib.error import HTTPError

from pymongo import MongoClient

from metaphor.schema import Schema
from metaphor.api import Api


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
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
                        },
                        "parttimers": {
                            "type": "linkcollection",
                            "target_spec_name": "employee",
                        },
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
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
        }, employee_1)

        employee_2 = self.api.get('employees/%s' % employee_id_2)
        self.assertEquals({
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'division': '/divisions/%s' % division_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
        }, employee_2)

        division_1 = self.api.get('divisions/%s' % division_id_1)
        self.assertEquals({
            'id': division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'name': 'sales',
            'yearly_sales': 100,
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
        }, division_1)

        linked_division_1 = self.api.get('employees/%s/division' % employee_id_1)
        self.assertEquals({
            'id': division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'name': 'sales',
            'yearly_sales': 100,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
        }, linked_division_1)

        division_2 = self.api.get('divisions/%s' % division_id_2)
        self.assertEquals({
            'id': division_id_2,
            'self': '/divisions/%s' % division_id_2,
            'name': 'marketting',
            'yearly_sales': 20,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_2,
            'sections': '/divisions/%s/sections' % division_id_2,
            'parttimers': '/divisions/%s/parttimers' % division_id_2,
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
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'division': '/divisions/%s' % division_id_1,
        }, {
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
            'division': '/divisions/%s' % division_id_1,
        }
        ], linked_employees)

        linked_employees_2 = self.api.get('divisions/%s/link_employee_division' % division_id_2)
        self.assertEquals([{
            'id': employee_id_3,
            'self': '/employees/%s' % employee_id_3,
            'name': 'fred',
            'age': 21,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_3,
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
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'division': '/divisions/%s' % division_id_1,
        },
        {
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
            'division': '/divisions/%s' % division_id_1,
        },
        {
            'id': employee_id_3,
            'self': '/employees/%s' % employee_id_3,
            'name': 'fred',
            'age': 21,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_3,
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
            'name': 'appropriation',
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            'link_employee_section': '/divisions/%s/sections/%s/link_employee_section' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % (division_id_1,)))

        self.assertEquals([
            {'id': employee_id_1,
             'name': 'ned',
             'section': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
             'self': '/employees/%s' % employee_id_1,
            }
        ], self.api.get('/divisions/%s/sections/%s/link_employee_section' % (division_id_1, section_id_1)))

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
             '_canonical_url_division': '/divisions/%s' % division_id_1,
             'name': 'ned'},
            {'_id': self.schema.decodeid(employee_id_2),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 31,
             'name': 'bob'}], new_employees)

    def test_post_with_link(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        self.assertEquals(1, len(self.api.get('/employees')))
        new_employees = list(self.db['resource_employee'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 41,
             'division': self.schema.decodeid(division_id_1),
             '_canonical_url_division': '/divisions/%s' % division_id_1,
             'name': 'ned'}], new_employees)

    def test_patch(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.api.patch('employees/%s' % employee_id_1, {'division': division_id_1})

        employees = list(self.db['resource_employee'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 41,
             'division': self.schema.decodeid(division_id_1),
             '_canonical_url_division': '/divisions/%s' % division_id_1,
             'name': 'ned'}], employees)

        divisions = list(self.db['resource_division'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(division_id_1),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'divisions',
             '_parent_id': None,
             '_parent_type': 'root',
             'yearly_sales': 100,
             'name': 'sales'}], divisions)

    def test_post_lower(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        section_id_1 = self.api.post('/divisions/%s/sections' % division_id_1, {'name': 'appropriation'})

        self.assertEquals([{
            'name': 'appropriation',
            'id': section_id_1,
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % division_id_1))

        new_divisions = list(self.db['resource_division'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(division_id_1),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'divisions',
             '_parent_id': None,
             '_parent_type': 'root',
             'name': 'sales',
             'yearly_sales': 100,
             }], new_divisions)

        new_sections = list(self.db['resource_section'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(section_id_1),
             '_parent_canonical_url': '/divisions/%s' % division_id_1,
             '_parent_field_name': 'sections',
             '_parent_id': self.schema.decodeid(division_id_1),
             '_parent_type': 'division',
             'name': 'appropriation'}], new_sections)

    def test_post_linkcollection(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.assertEquals([], self.api.get('/divisions/%s/parttimers' % division_id_1))

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_1})

        parttimers = self.api.get('/divisions/%s/parttimers' % division_id_1)
        self.assertEquals(employee_id_1, parttimers[0]['id'])

        reverse_linked_employees = self.api.get('/employees/%s/link_division_parttimers' % employee_id_1)
        self.assertEquals(1, len(reverse_linked_employees))

    def test_search(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.assertEquals([{
            'id': division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}], self.api.search_resource('division', "name='sales'"))

        self.assertEquals([{
            'age': 41,
            'division': None,
            'id': employee_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'name': 'ned',
            'self': '/employees/%s' % employee_id_1},
            {'age': 31,
            'division': None,
            'id': employee_id_2,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
            'name': 'bob',
            'self': '/employees/%s' % employee_id_2}], self.api.search_resource('employee', 'age>30'))

        self.assertEquals([{
            'id': division_id_2,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_2,
            'name': 'marketting',
            'parttimers': '/divisions/%s/parttimers' % division_id_2,
            'sections': '/divisions/%s/sections' % division_id_2,
            'self': '/divisions/%s' % division_id_2,
            'yearly_sales': 20}], self.api.search_resource('division', 'yearly_sales=20'))

    def test_can_post(self):
        self.schema.add_calc(self.schema.specs['division'], 'all_employees', 'self.link_employee_division')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        self.api.post('/employees', {'name': 'Bob'})

        _, _, can_post, is_linkcollection = self.api.get_spec_for('/employees/%s' % employee_id_1)
        self.assertFalse(can_post)
        self.assertFalse(is_linkcollection)

        _, _, can_post, is_linkcollection = self.api.get_spec_for('/employees')
        self.assertTrue(can_post)
        self.assertFalse(is_linkcollection)

        _, _, can_post, is_linkcollection = self.api.get_spec_for('/divisions/%s/all_employees' % division_id_1)
        self.assertFalse(can_post)
        self.assertFalse(is_linkcollection)

        _, _, can_post, is_linkcollection = self.api.get_spec_for('/divisions/%s/sections' % division_id_1)
        self.assertTrue(can_post)
        self.assertFalse(is_linkcollection)

        _, _, can_post, is_linkcollection = self.api.get_spec_for('/divisions/%s/parttimers' % division_id_1)
        self.assertFalse(can_post)
        self.assertTrue(is_linkcollection)

    def test_delete_resource(self):
        self.schema.add_calc(self.schema.specs['division'], 'all_employees', 'self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 35}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_2})

        self.assertEquals('Sales', self.api.get('/divisions/%s' % division_id_1)['name'])

        self.api.delete('/employees/%s' % employee_id_2)

        new_employees = list(self.db['resource_employee'].find())

        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 41,
             'name': 'ned'},
            {'_id': self.schema.decodeid(employee_id_3),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 35,
             'name': 'fred'}], new_employees)
        self.assertEquals([], self.api.get('/divisions/%s/parttimers' % division_id_1))

    def test_delete_linkcollection_entry(self):
        self.schema.add_calc(self.schema.specs['division'], 'all_employees', 'self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_2})

        self.assertEquals(1, len(self.api.get('/divisions/%s/all_employees' % division_id_1)))

        self.api.delete('/divisions/%s/parttimers/%s' % (division_id_1, employee_id_2))

        self.assertEquals([], self.api.get('/divisions/%s/all_employees' % division_id_1))

    def test_delete_link(self):
        self.schema.add_calc(self.schema.specs['division'], 'all_employees', 'self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_2)

        self.api.delete('/divisions/%s/parttimers/%s' % (division_id_1, employee_id_2))

        result = self.api.get('/divisions/%s/parttimers' % (division_id_1,))
        self.assertEquals([], result)

        result = self.api.get('/divisions/%s/all_employees' % (division_id_1,))
        self.assertEquals([], result)

    def test_expand(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        self.assertEquals([{
            'id': employee_id_1,
            'age': 41,
            'division': {
                'id': division_id_1,
                'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
                'parttimers': '/divisions/%s/parttimers' % division_id_1,
                'sections': '/divisions/%s/sections' % division_id_1,
                'name': 'sales',
                'yearly_sales': 100,
                'self': '/divisions/%s' % division_id_1},
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'name': 'ned',
            'self': '/employees/%s' % employee_id_1}]
            , self.api.get('/employees', expand='division'))

        self.assertEqual({
            'id': division_id_1,
            'link_employee_division': {'age': 41,
                                        'division': '/divisions/%s' % division_id_1,
                                        'id': employee_id_1,
                                        'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
                                        'name': 'ned',
                                        'self': '/employees/%s' % employee_id_1},
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}
            , self.api.get('/divisions/%s' % division_id_1, expand='link_employee_division'))


    def test_root(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')

        self.assertEqual([{
            'age': 41,
            'division': None,
            'id': employee_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'name': 'ned',
            'self': '/employees/%s' % employee_id_1}], self.api.get('/employees'))

    def test_expand_400(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        try:
            self.api.get('/employees', expand='nonexistant')
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('nonexistant not a field of employee', he.reason)

    def test_expand_400_field_type(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        try:
            self.api.get('/employees', expand='name')
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Unable to expand field name of type str', he.reason)

    def test_404(self):
        try:
            self.api.get('/schmemployees')
        except HTTPError as he:
            self.assertEqual(404, he.code)
            self.assertEqual('Not Found', he.reason)

        try:
            self.api.post('/schmemployees', {})
        except HTTPError as he:
            self.assertEqual(404, he.code)
            self.assertEqual('Not Found', he.reason)

        try:
            self.api.patch('/schmemployees', {})
        except HTTPError as he:
            self.assertEqual(404, he.code)
            self.assertEqual('Not Found', he.reason)

        try:
            self.api.delete('/schmemployees')
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Cannot delete root resource', he.reason)
