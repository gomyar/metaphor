
import unittest
from datetime import datetime
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
                        "created": {
                            "type": "datetime"
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
                        "contractors": {
                            "type": "orderedcollection",
                            "target_spec_name": "employee",
                        }
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
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'created': None,
            'division': '/divisions/%s' % division_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
        }, employee_1)

        employee_2 = self.api.get('employees/%s' % employee_id_2)
        self.assertEquals({
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'created': None,
            'division': '/divisions/%s' % division_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
        }, employee_2)

        division_1 = self.api.get('divisions/%s' % division_id_1)
        self.assertEquals({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
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
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
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
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
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

        linked_employees = self.api.get('divisions/%s/link_employee_division' % division_id_1)['results']
        self.assertEquals([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'division': '/divisions/%s' % division_id_1,
        }, {
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
            'division': '/divisions/%s' % division_id_1,
        }
        ], linked_employees)

        linked_employees_2 = self.api.get('divisions/%s/link_employee_division' % division_id_2)['results']
        self.assertEquals([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_3,
            'self': '/employees/%s' % employee_id_3,
            'name': 'fred',
            'age': 21,
            'created': None,
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

        employees = self.api.get('/employees')['results']
        self.assertEquals([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'division': '/divisions/%s' % division_id_1,
        },
        {
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
            'division': '/divisions/%s' % division_id_1,
        },
        {
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_3,
            'self': '/employees/%s' % employee_id_3,
            'name': 'fred',
            'age': 21,
            'created': None,
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
            '_grants': [],
            '_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            '_parent_id': self.schema.decodeid(division_id_1),
            '_parent_type': 'division',
            '_parent_field_name': 'sections',
            '_parent_canonical_url': '/divisions/%s' % division_id_1,
            'name': 'appropriation',
        }, self.db['resource_section'].find_one({'_id': self.schema.decodeid(section_id_1)}))

        self.assertEquals({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'name': 'sales',
            'self': '/divisions/%s' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
        }, self.api.get('/divisions/%s' % division_id_1))

        self.assertEquals([{
            '_meta': {'is_collection': False, 'spec': {'name': 'section'}},
            'id': section_id_1,
            'name': 'appropriation',
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            'link_employee_section': '/divisions/%s/sections/%s/link_employee_section' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % (division_id_1,))['results'])

        self.assertEquals([
            {'_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
             'id': employee_id_1,
             'name': 'ned',
             'section': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
             'self': '/employees/%s' % employee_id_1,
            }
        ], self.api.get('/divisions/%s/sections/%s/link_employee_section' % (division_id_1, section_id_1))['results'])

    def test_post(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})

        employee_id_2 = self.api.post('/employees', {'name': 'bob', 'age': 31})

        self.assertEquals(2, self.api.get('/employees')['count'])
        new_employees = list(self.db['resource_employee'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 41,
             'division': self.schema.decodeid(division_id_1),
             '_canonical_url_division': '/divisions/%s' % division_id_1,
             'name': 'ned'},
            {'_id': self.schema.decodeid(employee_id_2),
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_2,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 31,
             'name': 'bob'}], new_employees)

    def test_post_datetime(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 31, 'created': '2021-12-11'})
        new_employees = list(self.db['resource_employee'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 31,
             'created': datetime(2021, 12, 11),
             'name': 'bob'}], new_employees)

    def test_post_datetime_with_z(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 31, 'created': '2021-12-11T12:11:10.123Z'})
        new_employees = list(self.db['resource_employee'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 31,
             'created': datetime(2021, 12, 11, 12, 11, 10, 123000),
             'name': 'bob'}], new_employees)

    def test_post_with_link(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        self.assertEquals(1, len(self.api.get('/employees')['results']))
        new_employees = list(self.db['resource_employee'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
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
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
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
             '_grants': [],
             '_canonical_url': '/divisions/%s' % division_id_1,
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
            '_meta': {'is_collection': False, 'spec': {'name': 'section'}},
            'contractors': '/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_1),
            'name': 'appropriation',
            'id': section_id_1,
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % division_id_1)['results'])

        new_divisions = list(self.db['resource_division'].find())
        self.assertEquals([
            {'_id': self.schema.decodeid(division_id_1),
            '_grants': [],
             '_canonical_url': '/divisions/%s' % division_id_1,
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
            '_grants': [],
             '_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
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

        self.assertEquals([], self.api.get('/divisions/%s/parttimers' % division_id_1)['results'])

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_1})

        parttimers = self.api.get('/divisions/%s/parttimers' % division_id_1)['results']
        self.assertEquals(employee_id_1, parttimers[0]['id'])

        reverse_linked_employees = self.api.get('/employees/%s/link_division_parttimers' % employee_id_1)['results']
        self.assertEquals(1, len(reverse_linked_employees))

    def test_search(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.assertEquals({
            '_meta': {'is_collection': True, 'spec': {'name': 'division'}},
            'results': [{
                '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
                'id': division_id_1,
                'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
                'name': 'sales',
                'parttimers': '/divisions/%s/parttimers' % division_id_1,
                'sections': '/divisions/%s/sections' % division_id_1,
                'self': '/divisions/%s' % division_id_1,
                'yearly_sales': 100}],
            'count': 1,
            'next': None,
            'previous': None,
            }, self.api.search_resource('division', "name='sales'"))

        self.assertEquals({
            '_meta': {'is_collection': True, 'spec': {'name': 'employee'}},
            'count': 2,
            'next': None,
            'previous': None,
            'results': [{
                '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
                'age': 41,
                'division': None,
                'created': None,
                'id': employee_id_1,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
                'name': 'ned',
                'self': '/employees/%s' % employee_id_1},
                {
                '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
                'age': 31,
                'division': None,
                'created': None,
                'id': employee_id_2,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
                'name': 'bob',
                'self': '/employees/%s' % employee_id_2}]}, self.api.search_resource('employee', 'age>30'))

        self.assertEquals({
            '_meta': {'is_collection': True, 'spec': {'name': 'division'}},
            'count': 1,
            'next': None,
            'previous': None,
            'results': [{
                '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
                'id': division_id_2,
                'link_employee_division': '/divisions/%s/link_employee_division' % division_id_2,
                'name': 'marketting',
                'parttimers': '/divisions/%s/parttimers' % division_id_2,
                'sections': '/divisions/%s/sections' % division_id_2,
                'self': '/divisions/%s' % division_id_2,
                'yearly_sales': 20}]}, self.api.search_resource('division', 'yearly_sales=20'))

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
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 41,
             'name': 'ned'},
            {'_id': self.schema.decodeid(employee_id_3),
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_3,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 35,
             'name': 'fred'}], new_employees)
        self.assertEquals([], self.api.get('/divisions/%s/parttimers' % division_id_1)['results'])

    def test_delete_linkcollection_entry(self):
        self.schema.add_calc(self.schema.specs['division'], 'all_employees', 'self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_2})

        self.assertEquals(1, len(self.api.get('/divisions/%s/all_employees' % division_id_1)['results']))
        self.assertEquals(1, self.api.get('/divisions/%s/all_employees' % division_id_1)['count'])

        self.api.delete('/divisions/%s/parttimers/%s' % (division_id_1, employee_id_2))

        self.assertEquals([], self.api.get('/divisions/%s/all_employees' % division_id_1)['results'])

    def test_delete_link(self):
        self.schema.add_calc(self.schema.specs['division'], 'all_employees', 'self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_2)

        self.api.delete('/divisions/%s/parttimers/%s' % (division_id_1, employee_id_2))

        result = self.api.get('/divisions/%s/parttimers' % (division_id_1,))['results']
        self.assertEquals([], result)

        result = self.api.get('/divisions/%s/all_employees' % (division_id_1,))['results']
        self.assertEquals([], result)

    def test_expand(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        self.assertEquals([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'age': 41,
            'created': None,
            'division': {
                '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
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
            , self.api.get('/employees', args={"expand": 'division'})['results'])

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'link_employee_division': {
                '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
                'age': 41,
                'division': '/divisions/%s' % division_id_1,
                'created': None,
                'id': employee_id_1,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
                'name': 'ned',
                'self': '/employees/%s' % employee_id_1},
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}
            , self.api.get('/divisions/%s' % division_id_1, args={"expand": 'link_employee_division'}))

    def test_expand_linkcollection(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31, 'division': division_id_1}, 'employees')

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_1})
        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_2})

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'name': 'sales',
			'parttimers': [{
				'_meta': {'is_collection': False,
                          'spec': {'name': 'employee'}},
                'age': 41,
                'created': None,
                'division': '/divisions/%s' % division_id_1,
                'id': employee_id_1,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
                'name': 'ned',
                'self': '/employees/%s' % employee_id_1},
               {'_meta': {'is_collection': False,
                          'spec': {'name': 'employee'}},
                'age': 31,
                'created': None,
                'division': '/divisions/%s' % division_id_1,
                'id': employee_id_2,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
                'name': 'bob',
                'self': '/employees/%s' % employee_id_2}],
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}
            , self.api.get('/divisions/%s' % division_id_1, args={"expand": 'parttimers'}))

    def test_expand_collection(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        section_id_1 = self.schema.insert_resource('section', {'name': 'engineering'}, 'sections', 'division', division_id_1)
        section_id_2 = self.schema.insert_resource('section', {'name': 'marketting'}, 'sections', 'division', division_id_1)

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': [{'_meta': {'is_collection': False, 'spec': {'name': 'section'}},
                        'contractors': '/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_1),
                        'id': section_id_1,
                        'name': 'engineering',
                        'parent_division_sections': '/divisions/%s' % division_id_1,
                        'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1)},
                        {'_meta': {'is_collection': False, 'spec': {'name': 'section'}},
                        'contractors': '/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_2),
                        'id': section_id_2,
                        'name': 'marketting',
                        'parent_division_sections': '/divisions/%s' % division_id_1,
                        'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_2)}],
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}
            , self.api.get('/divisions/%s' % division_id_1, args={"expand": 'sections'}))

    def test_expand_orderedcollection(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        section_id_1 = self.schema.insert_resource('section', {'name': 'engineering'}, 'sections', 'division', division_id_1)
        contractor_id_1 = self.schema.create_orderedcollection_entry('employee', 'section', 'contractors', section_id_1, {'name': 'bob'})

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'section'}},
            'contractors': [{
                '_meta': {'is_collection': False,
                'spec': {'name': 'employee'}},
                'age': None,
                'division': None,
                'created': None,
                'id': contractor_id_1,
                'link_division_parttimers': '/divisions/%s/sections/%s/contractors/%s/link_division_parttimers' % (division_id_1, section_id_1, contractor_id_1),
                'name': 'bob',
                'self': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id_1)}],
            'id': section_id_1,
            'name': 'engineering',
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1)}
            , self.api.get('/divisions/%s/sections/%s' % (division_id_1, section_id_1), args={"expand": 'contractors'}))

    def test_root(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')

        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'age': 41,
            'division': None,
            'created': None,
            'id': employee_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'name': 'ned',
            'self': '/employees/%s' % employee_id_1}], self.api.get('/employees')['results'])

    def test_expand_400(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        try:
            self.api.get('/employees', args={"expand": 'nonexistant'})
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('nonexistant not a field of employee', he.reason)

    def test_expand_400_field_type(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        try:
            self.api.get('/employees', args={"expand": 'name'})
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

    def test_get_root(self):
        root_dict = self.api.get('/')
        self.assertEquals({
            'auth': '/auth',
            'ego': '/ego',
            'users': '/users',
            'groups': '/groups',
            'employees': '/employees',
            'divisions': '/division',
        }, root_dict)

    def test_orderedcollection(self):
        self.schema.add_calc(self.schema.specs['division'], 'all_contractors', 'self.sections.contractors')

        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        section_id_1 = self.schema.insert_resource('section', {'name': 'engineering'}, 'sections', 'division', division_id_1)

        # test creation
        contractor_id = self.api.post('/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_1), {'name': 'Angus'})
        self.assertEqual({
            '_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            '_grants': [],
            '_id': self.schema.decodeid(section_id_1),
            '_parent_canonical_url': '/divisions/%s' % division_id_1,
            '_parent_field_name': 'sections',
            '_parent_id': self.schema.decodeid(division_id_1),
            '_parent_type': 'division',
            'contractors': [{'_id': self.schema.decodeid(contractor_id)}],
            'name': 'engineering'}, self.db.resource_section.find_one({'_id': self.schema.decodeid(section_id_1)}))
        self.assertEqual({
            '_canonical_url': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id),
            '_grants': [],
            '_id': self.schema.decodeid(contractor_id),
            '_parent_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            '_parent_field_name': 'contractors',
            '_parent_id': self.schema.decodeid(section_id_1),
            '_parent_type': 'section',
            'name': 'Angus'}, self.db.resource_employee.find_one(self.schema.decodeid(contractor_id)))

        # test calc update
        self.assertEqual([
            {
                '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
                'id': contractor_id,
                'name': 'Angus',
                'created': None,
                'age': None,
                'division': None,
                'link_division_parttimers': '/divisions/%s/sections/%s/contractors/%s/link_division_parttimers' % (division_id_1, section_id_1, contractor_id),
                'self': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id),
            },
        ], self.api.get('/divisions/%s/all_contractors' % (division_id_1, ))['results'])

        # test reverse link

        # test link to orderedcollection resource from another linkcollection or link
        self.schema.add_field(self.schema.specs['division'], 'manager', 'link', 'employee')
        self.api.patch('/divisions/%s' % division_id_1, {'manager': contractor_id})
        contractor = self.api.get('/divisions/%s/manager' % division_id_1)
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': contractor_id,
            'name': 'Angus',
            'age': None,
            'division': None,
            'created': None,
            'link_division_parttimers': '/divisions/%s/sections/%s/contractors/%s/link_division_parttimers' % (division_id_1, section_id_1, contractor_id),
            'link_division_manager': '/divisions/%s/sections/%s/contractors/%s/link_division_manager' % (division_id_1, section_id_1, contractor_id),
            'self': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id),
        }, contractor)

        # test deletion (with all of the above)
        self.api.delete('/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id))
        self.assertEqual([], self.api.get('/divisions/%s/all_contractors' % division_id_1)['results'])
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'all_contractors': '/divisions/%s/all_contractors' % division_id_1,
            'id': division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'manager': None,
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}, self.api.get('/divisions/%s' % division_id_1))
