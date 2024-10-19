
import unittest
from datetime import datetime
from urllib.error import HTTPError

from metaphor.mongoclient_testutils import mongo_connection

from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api, create_expand_dict


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')
        self.schema.create_field('employee', 'created', 'datetime')

        self.division_spec = self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'yearly_sales', 'int')
        self.schema.create_field('division', 'parttimers', 'linkcollection', 'employee')

        self.schema.create_field('employee', 'division', 'link', 'division')

        self.section_spec = self.schema.create_spec('section')
        self.schema.create_field('section', 'name', 'str')
        self.schema.create_field('section', 'contractors', 'orderedcollection', 'employee')

        self.schema.create_field('division', 'sections', 'collection', 'section')

        self.schema.create_field('root', 'employees', 'collection', 'employee')
        self.schema.create_field('root', 'divisions', 'collection', 'division')

        self.api = Api(self.db)

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
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'created': None,
            'division': '/divisions/%s' % division_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'parent_section_contractors': None,
        }, employee_1)

        employee_2 = self.api.get('employees/%s' % employee_id_2)
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'created': None,
            'division': '/divisions/%s' % division_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
            'parent_section_contractors': None,
        }, employee_2)

        division_1 = self.api.get('divisions/%s' % division_id_1)
        self.assertEqual({
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
        self.assertEqual({
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
        self.assertEqual({
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
        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'division': '/divisions/%s' % division_id_1,
            'parent_section_contractors': None,
        }, {
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_2,
            'self': '/employees/%s' % employee_id_2,
            'name': 'bob',
            'age': 31,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
            'division': '/divisions/%s' % division_id_1,
            'parent_section_contractors': None,
        }
        ], linked_employees)

        linked_employees_2 = self.api.get('divisions/%s/link_employee_division' % division_id_2)['results']
        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_3,
            'self': '/employees/%s' % employee_id_3,
            'name': 'fred',
            'age': 21,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_3,
            'division': '/divisions/%s' % division_id_2,
            'parent_section_contractors': None,
        }], linked_employees_2)

    def test_collections_and_null_links(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})

        employees = self.api.get('/employees')['results']
        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'self': '/employees/%s' % employee_id_1,
            'name': 'ned',
            'age': 41,
            'created': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'parent_section_contractors': None,
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
            'parent_section_contractors': None,
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
            'parent_section_contractors': None,
            'division': None,
        }], employees)

    def test_canonical_url(self):
        self.db.metaphor_schema.drop()
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()
        self.schema.create_spec("section")
        self.schema.create_field("section", "name", "str")
        self.schema.create_spec("employee")
        self.schema.create_field("employee", "name", "str")
        self.schema.create_field("employee", "section", "link", "section")
        self.schema.create_spec("division")
        self.schema.create_field("division", "name", "str")
        self.schema.create_field("division", "sections", "collection", "section")

        self.schema.create_field("root", "employees", "collection", "employee")
        self.schema.create_field("root", "divisions", "collection", "division")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned'}, 'employees')
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales'}, 'divisions')
        section_id_1 = self.schema.insert_resource('section', {'name': 'appropriation'}, parent_type='division', parent_id=division_id_1, parent_field_name='sections')

        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEqual(None, employee['section'])

        self.schema.update_resource_fields('employee', employee_id_1, {'section': section_id_1})

        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEqual('/divisions/%s/sections/%s' % (division_id_1, section_id_1), employee['section'])

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'name': 'sales',
            'self': '/divisions/%s' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
        }, self.api.get('/divisions/%s' % division_id_1))

        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'section'}},
            'id': section_id_1,
            'name': 'appropriation',
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            'link_employee_section': '/divisions/%s/sections/%s/link_employee_section' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % (division_id_1,))['results'])

        self.assertEqual([
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

        self.assertEqual(2, self.api.get('/employees')['count'])
        new_employees = list(self.db['metaphor_resource'].find({'_type': 'employee'}))
        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id_1),
            '_schema_id': self.schema._id,
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'employee',
             'age': 41,
             'division': self.schema.decodeid(division_id_1),
             '_canonical_url_division': '/divisions/%s' % division_id_1,
             'name': 'ned'},
            {'_id': self.schema.decodeid(employee_id_2),
            '_schema_id': self.schema._id,
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_2,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'employee',
             'age': 31,
             'name': 'bob'}], new_employees)

    def test_post_datetime(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 31, 'created': '2021-12-11'})
        new_employees = list(self.db['metaphor_resource'].find())
        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id_1),
            '_schema_id': self.schema._id,
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'employee',
             'age': 31,
             'created': datetime(2021, 12, 11),
             'name': 'bob'}], new_employees)

    def test_post_datetime_with_z(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 31, 'created': '2021-12-11T12:11:10.123Z'})
        new_employees = list(self.db['metaphor_resource'].find())
        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id_1),
            '_schema_id': self.schema._id,
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'employee',
             'age': 31,
             'created': datetime(2021, 12, 11, 12, 11, 10, 123000),
             'name': 'bob'}], new_employees)

    def test_post_with_link(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        self.assertEqual(1, len(self.api.get('/employees')['results']))
        new_employees = list(self.db['metaphor_resource'].find({'_type': 'employee'}))
        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id_1),
            '_schema_id': self.schema._id,
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'employee',
             'age': 41,
             'division': self.schema.decodeid(division_id_1),
             '_canonical_url_division': '/divisions/%s' % division_id_1,
             'name': 'ned'}], new_employees)

    def test_patch(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.api.patch('employees/%s' % employee_id_1, {'division': division_id_1})

        employees = list(self.db['metaphor_resource'].find({'_type': 'employee'}))
        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id_1),
            '_schema_id': self.schema._id,
             '_grants': [],
             '_canonical_url': '/employees/%s' % employee_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'employee',
             'age': 41,
             'division': self.schema.decodeid(division_id_1),
             '_canonical_url_division': '/divisions/%s' % division_id_1,
             'name': 'ned'}], employees)

        divisions = list(self.db['metaphor_resource'].find({'_type': 'division'}))
        self.assertEqual([
            {'_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
             '_grants': [],
             '_canonical_url': '/divisions/%s' % division_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'divisions',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'division',
             'yearly_sales': 100,
             'name': 'sales'}], divisions)

    def test_post_lower(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        section_id_1 = self.api.post('/divisions/%s/sections' % division_id_1, {'name': 'appropriation'})

        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'section'}},
            'contractors': '/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_1),
            'name': 'appropriation',
            'id': section_id_1,
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
        }], self.api.get('/divisions/%s/sections' % division_id_1)['results'])

        new_divisions = list(self.db['metaphor_resource'].find({'_type': 'division'}))
        self.assertEqual([
            {'_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
             '_canonical_url': '/divisions/%s' % division_id_1,
             '_parent_canonical_url': '/',
             '_parent_field_name': 'divisions',
             '_parent_id': None,
             '_parent_type': 'root',
             '_type': 'division',
             'name': 'sales',
             'yearly_sales': 100,
             }], new_divisions)

        new_sections = list(self.db['metaphor_resource'].find({'_type': 'section'}))
        self.assertEqual([
            {'_id': self.schema.decodeid(section_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
             '_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
             '_parent_canonical_url': '/divisions/%s' % division_id_1,
             '_parent_field_name': 'sections',
             '_parent_id': self.schema.decodeid(division_id_1),
             '_parent_type': 'division',
             '_type': 'section',
             'name': 'appropriation'}], new_sections)

    def test_post_linkcollection(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.assertEqual([], self.api.get('/divisions/%s/parttimers' % division_id_1)['results'])

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_1})

        parttimers = self.api.get('/divisions/%s/parttimers' % division_id_1)['results']
        self.assertEqual(employee_id_1, parttimers[0]['id'])

        reverse_linked_employees = self.api.get('/employees/%s/link_division_parttimers' % employee_id_1)['results']
        self.assertEqual(1, len(reverse_linked_employees))

    def test_search(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.assertEqual({
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

        self.assertEqual({
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
                'parent_section_contractors': None,
                'self': '/employees/%s' % employee_id_1},
                {
                '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
                'age': 31,
                'division': None,
                'created': None,
                'id': employee_id_2,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
                'name': 'bob',
                'parent_section_contractors': None,
                'self': '/employees/%s' % employee_id_2}]}, self.api.search_resource('employee', 'age>30'))

        self.assertEqual({
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
        self.schema.create_field('division', 'all_employees', 'calc', calc_str='self.link_employee_division')

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
        self.schema.create_field('division', 'all_employees', 'calc', calc_str='self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 35}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'Sales'}, 'divisions')

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_2})

        self.assertEqual('Sales', self.api.get('/divisions/%s' % division_id_1)['name'])

        self.api.delete('/employees/%s' % employee_id_2)

        new_employees = self.api.get('/employees')

        self.assertEqual([{'_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'age': 41,
            'created': None,
            'division': None,
            'id': employee_id_1,
            'link_division_parttimers': f'/employees/{employee_id_1}/link_division_parttimers',
            'name': 'ned',
            'parent_section_contractors': None,
            'self': f'/employees/{employee_id_1}'},
            {'_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'age': 35,
            'created': None,
            'division': None,
            'id': employee_id_3,
            'link_division_parttimers': f'/employees/{employee_id_3}/link_division_parttimers',
            'name': 'fred',
            'parent_section_contractors': None,
            'self': f'/employees/{employee_id_3}'}], new_employees['results'])
        self.assertEqual([], self.api.get('/divisions/%s/parttimers' % division_id_1)['results'])

    def test_delete_linkcollection_entry(self):
        self.schema.create_field('division', 'all_employees', 'calc', calc_str='self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_2})

        self.assertEqual(1, len(self.api.get('/divisions/%s/all_employees' % division_id_1)['results']))
        self.assertEqual(1, self.api.get('/divisions/%s/all_employees' % division_id_1)['count'])

        self.api.delete('/divisions/%s/parttimers/%s' % (division_id_1, employee_id_2))

        self.assertEqual([], self.api.get('/divisions/%s/all_employees' % division_id_1)['results'])

    def test_delete_link(self):
        self.schema.create_field('division', 'all_employees', 'calc', calc_str='self.parttimers')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_2)

        self.api.delete('/divisions/%s/parttimers/%s' % (division_id_1, employee_id_2))

        result = self.api.get('/divisions/%s/parttimers' % (division_id_1,))['results']
        self.assertEqual([], result)

        result = self.api.get('/divisions/%s/all_employees' % (division_id_1,))['results']
        self.assertEqual([], result)

    def test_expand(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')
        section_id_1 = self.schema.insert_resource('section', {'name': 'engineering'}, 'sections', 'division', division_id_1)
        contractor_id_1 = self.schema.create_orderedcollection_entry('employee', 'section', 'contractors', section_id_1, {'name': 'bob'})

        self.assertEqual([{
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
            'parent_section_contractors': None,
            'self': '/employees/%s' % employee_id_1}]
            , self.api.get('/employees', args={"expand": 'division'})['results'])

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'link_employee_division': [{
                '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
                'age': 41,
                'division': '/divisions/%s' % division_id_1,
                'created': None,
                'id': employee_id_1,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
                'name': 'ned',
                'parent_section_contractors': None,
                'self': '/employees/%s' % employee_id_1}],
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}
            , self.api.get('/divisions/%s' % division_id_1, args={"expand": 'link_employee_division'}))

        # nested expansion through link
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'age': 41,
            'created': None,
            'division': {'_meta': {'is_collection': False, 'spec': {'name': 'division'}},
                        'id': division_id_1,
                        'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
                        'name': 'sales',
                        'parttimers': '/divisions/%s/parttimers' % division_id_1,
                        'sections': [{'_meta': {'is_collection': False,
                                                'spec': {'name': 'section'}},
                                        'contractors': '/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_1),
                                        'id': section_id_1,
                                        'name': 'engineering',
                                        'parent_division_sections': '/divisions/%s' % division_id_1,
                                        'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1)}],
                        'self': '/divisions/%s' % division_id_1,
                        'yearly_sales': 100},
            'id': employee_id_1,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'name': 'ned',
            'parent_section_contractors': None,
            'self': '/employees/%s' % employee_id_1}
            , self.api.get('/employees/%s' % employee_id_1, args={"expand": 'division.sections'}))

    def test_expand_reverse_link_collection(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')
        self.api.post('/divisions/%s/parttimers' % division_id_1, {'id': employee_id_1})

        # reverse link collection
        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'age': 41,
            'created': None,
            'division': '/divisions/%s' % division_id_1,
            'link_division_parttimers': [
                {'_meta': {'is_collection': False,
                 'spec': {'name': 'division'}},
                 'id': division_id_1,
                 'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
                 'name': 'sales',
                 'parttimers': '/divisions/%s/parttimers' % division_id_1,
                 'sections': '/divisions/%s/sections' % division_id_1,
                 'self': '/divisions/%s' % division_id_1,
                 'yearly_sales': 100}],
            'name': 'ned',
            'parent_section_contractors': None,
            'self': '/employees/%s' % employee_id_1}]
            , self.api.get('/employees', args={"expand": 'link_division_parttimers'})['results'])

    def test_expand_null_link(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41,}, 'employees')

        self.assertEqual([{
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'id': employee_id_1,
            'age': 41,
            'created': None,
            'division': None,
            'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
            'name': 'ned',
            'parent_section_contractors': None,
            'self': '/employees/%s' % employee_id_1}]
            , self.api.get('/employees', args={"expand": 'division'})['results'])

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
                'parent_section_contractors': None,
                'self': '/employees/%s' % employee_id_1},
               {'_meta': {'is_collection': False,
                          'spec': {'name': 'employee'}},
                'age': 31,
                'created': None,
                'division': '/divisions/%s' % division_id_1,
                'id': employee_id_2,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_2,
                'name': 'bob',
                'parent_section_contractors': None,
                'self': '/employees/%s' % employee_id_2}],
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}
            , self.api.get('/divisions/%s' % division_id_1, args={"expand": 'parttimers'}))

        # expand from linkcollection
        expanded = self.api.get('/divisions/%s' % division_id_1, args={"expand": 'parttimers.division'})
        self.assertEqual('ned', expanded['parttimers'][0]['name'])
        self.assertEqual('sales', expanded['parttimers'][0]['division']['name'])
        self.assertEqual('bob', expanded['parttimers'][1]['name'])
        self.assertEqual('sales', expanded['parttimers'][1]['division']['name'])

    def test_expand_reverse_link(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41, 'division': division_id_1}, 'employees')

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'link_employee_division': [
                {
				'_meta': {'is_collection': False,
                          'spec': {'name': 'employee'}},
                'age': 41,
                'created': None,
                'division': '/divisions/%s' % division_id_1,
                'id': employee_id_1,
                'link_division_parttimers': '/employees/%s/link_division_parttimers' % employee_id_1,
                'name': 'ned',
                'parent_section_contractors': None,
                'self': '/employees/%s' % employee_id_1},
            ],
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}
            , self.api.get('/divisions/%s' % division_id_1, args={"expand": 'link_employee_division'}))

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
                'parent_section_contractors': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
                'self': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id_1)}],
            'id': section_id_1,
            'name': 'engineering',
            'parent_division_sections': '/divisions/%s' % division_id_1,
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1)}
            , self.api.get('/divisions/%s/sections/%s' % (division_id_1, section_id_1), args={"expand": 'contractors'}))

        # nested expansion
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'name': 'sales',
            'parttimers': '/divisions/%s/parttimers' % division_id_1,
            'sections': [{'_meta': {'is_collection': False, 'spec': {'name': 'section'}},
                          'contractors': [{'_meta': {'is_collection': False,
                                                     'spec': {'name': 'employee'}},
                                           'age': None,
                                           'created': None,
                                           'division': None,
                                           'id': contractor_id_1,
                                           'link_division_parttimers': '/divisions/%s/sections/%s/contractors/%s/link_division_parttimers' % (division_id_1, section_id_1, contractor_id_1),
                                           'name': 'bob',
                                           'parent_section_contractors': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
                                           'self': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id_1)}],
                          'id': section_id_1,
                          'name': 'engineering',
                          'parent_division_sections': '/divisions/%s' % division_id_1,
                          'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1)}],
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100},
            self.api.get('/divisions/%s' % (division_id_1, ), args={"expand": 'sections.contractors'}))



    def test_expand_parent(self):
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        section_id_1 = self.schema.insert_resource('section', {'name': 'engineering'}, 'sections', 'division', division_id_1)

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'section'}},
            'contractors': '/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_1),
            'id': section_id_1,
            'name': 'engineering',
            'parent_division_sections': {
                '_meta': {'is_collection': False,
                'spec': {'name': 'division'}},
                'id': division_id_1,
                'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
                'name': 'sales',
                'parttimers': '/divisions/%s/parttimers' % division_id_1,
                'sections': '/divisions/%s/sections' % division_id_1,
                'self': '/divisions/%s' % division_id_1,
                'yearly_sales': 100},
            'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1)}
            , self.api.get('/divisions/%s/sections/%s' % (division_id_1, section_id_1), args={"expand": 'parent_division_sections'}))

    def test_orderedcollection(self):
        self.schema.create_field('division', 'all_contractors', 'calc', calc_str='self.sections.contractors')

        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')
        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        section_id_1 = self.schema.insert_resource('section', {'name': 'engineering'}, 'sections', 'division', division_id_1)

        # test creation
        contractor_id = self.api.post('/divisions/%s/sections/%s/contractors' % (division_id_1, section_id_1), {'name': 'Angus'})
        self.assertEqual({
            '_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            '_grants': [],
            '_id': self.schema.decodeid(section_id_1),
            '_schema_id': self.schema._id,
            '_parent_canonical_url': '/divisions/%s' % division_id_1,
            '_parent_field_name': 'sections',
            '_parent_id': self.schema.decodeid(division_id_1),
            '_parent_type': 'division',
            '_type': 'section',
            'contractors': [{'_id': self.schema.decodeid(contractor_id)}],
            'name': 'engineering'}, self.db.metaphor_resource.find_one({'_id': self.schema.decodeid(section_id_1)}))
        self.assertEqual({
            '_canonical_url': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id),
            '_grants': [],
            '_id': self.schema.decodeid(contractor_id),
            '_schema_id': self.schema._id,
            '_parent_canonical_url': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
            '_parent_field_name': 'contractors',
            '_parent_id': self.schema.decodeid(section_id_1),
            '_parent_type': 'section',
            '_type': 'employee',
            'name': 'Angus'}, self.db.metaphor_resource.find_one({'_id': self.schema.decodeid(contractor_id)}))

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
                'parent_section_contractors': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
                'self': '/divisions/%s/sections/%s/contractors/%s' % (division_id_1, section_id_1, contractor_id),
            },
        ], self.api.get('/divisions/%s/all_contractors' % (division_id_1, ))['results'])

        # test reverse link

        # test link to orderedcollection resource from another linkcollection or link
        self.schema.create_field('division', 'manager', 'link', 'employee')
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
            'parent_section_contractors': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
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
            'parent_section_contractors': None,
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
        self.assertEqual({
            'auth': '/auth',
            'ego': '/ego',
            'users': '/users',
            'groups': '/groups',
            'employees': '/employees',
            'divisions': '/division',
        }, root_dict)

    def test_grants_set_on_nested_resources(self):
        group_id_1 = self.schema.insert_resource('group', {'name': 'test'}, 'groups')
        grant_id_1 = self.schema.insert_resource('grant', {'type': 'read', 'url': '/'}, 'grants', 'group', group_id_1)

        division_id = self.api.post('/divisions', {'name': 'EU'})
        section_id = self.api.post('/divisions/%s/sections' % division_id, {'name': 'Sales'})
        contractor_id = self.api.post('/divisions/%s/sections/%s/contractors' % (division_id, section_id), {'name': 'Bob'})

        division_data = self.db['metaphor_resource'].find_one({'_id': self.schema.decodeid(division_id)})
        section_data = self.db['metaphor_resource'].find_one({'_id': self.schema.decodeid(section_id)})
        employee_data = self.db['metaphor_resource'].find_one({'_id': self.schema.decodeid(contractor_id)})

        self.assertEqual([self.schema.decodeid(grant_id_1)], division_data['_grants'])
        self.assertEqual([self.schema.decodeid(grant_id_1)], section_data['_grants'])
        self.assertEqual([self.schema.decodeid(grant_id_1)], employee_data['_grants'])

    def test_grants_set_on_nested_resources_2(self):
        group_id_1 = self.schema.insert_resource('group', {'name': 'test'}, 'groups')

        division_id = self.api.post('/divisions', {'name': 'EU'})

        grant_id_1 = self.schema.insert_resource('grant', {'type': 'read', 'url': '/divisions/%s' % division_id}, 'grants', 'group', group_id_1)

        section_id = self.api.post('/divisions/%s/sections' % division_id, {'name': 'Sales'})
        contractor_id = self.api.post('/divisions/%s/sections/%s/contractors' % (division_id, section_id), {'name': 'Bob'})

        division_data = self.db['metaphor_resource'].find_one({'_id': self.schema.decodeid(division_id)})
        section_data = self.db['metaphor_resource'].find_one({'_id': self.schema.decodeid(section_id)})
        employee_data = self.db['metaphor_resource'].find_one({'_id': self.schema.decodeid(contractor_id)})

        self.assertEqual([], division_data['_grants'])
        self.assertEqual([self.schema.decodeid(grant_id_1)], section_data['_grants'])
        self.assertEqual([self.schema.decodeid(grant_id_1)], employee_data['_grants'])

    def test_filter(self):
        self.schema.create_field('employee', 'retirement_age', 'calc', calc_str='self.age + 20')

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        employees_by_name = self.api.get('employees[name~"ed"]')
        self.assertEqual(2, employees_by_name['count'])
        self.assertEqual('ned', employees_by_name['results'][0]['name'])
        self.assertEqual('fred', employees_by_name['results'][1]['name'])

        employees_by_age = self.api.get('employees[age>30]')
        self.assertEqual(2, employees_by_age['count'])

        self.assertEqual('ned', employees_by_age['results'][0]['name'])
        self.assertEqual('bob', employees_by_age['results'][1]['name'])

        employees_by_retirement_age = self.api.get('employees[retirement_age>50]')
        self.assertEqual(2, employees_by_retirement_age['count'])

        self.assertEqual('ned', employees_by_retirement_age['results'][0]['name'])
        self.assertEqual('bob', employees_by_retirement_age['results'][1]['name'])

    def test_grants_set_on_post_path(self):
        self.schema.create_field('user', 'references', 'collection', 'employee')

        user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})

#        group_id_1 = self.schema.insert_resource('group', {'name': 'test'}, 'groups')
#        self.schema.insert_resource('grant', {'type': 'read', 'url': '/users'}, 'grants', 'group', group_id_1)
#        self.schema.insert_resource('grant', {'type': 'read', 'url': '/users/%s/references' % user_id}, 'grants', 'group', group_id_1)
#        self.schema.insert_resource('grant', {'type': 'create', 'url': '/users/%s/references' % user_id}, 'grants', 'group', group_id_1)

        group_id_1 = self.api.post('/groups', {'name': 'test'})

        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'users'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'users.references'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'create', 'url': 'users.references'})


        self.api.post('/users/%s/groups' % user_id, {'id': group_id_1})

        user = self.schema.load_user_by_username('bob')

        # post with grants
        self.api.post('/users/%s/references' % user_id, {'name': 'fred'}, user=user)

        employees = self.api.get('/users/%s/references' % user_id, user=user)
        self.assertEqual('fred', employees['results'][0]['name'])

    def test_grants_set_on_patch_path(self):
        self.schema.create_field('user', 'reference', 'link', 'employee')

        user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})

        group_id_1 = self.api.post('/groups', {'name': 'test'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'users'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'update', 'url': 'users'})

        self.api.post('/users/%s/groups' % user_id, {'id': group_id_1})

        user = self.schema.load_user_by_username('bob')

        # patch with grants
        employee_id = self.api.post('/employees', {'name': 'fred'})

        # actually should have given a 403 as no read access to /employees/
        self.api.patch('/users/%s' % user_id, {'reference': employee_id}, user=user)

        get_user = self.api.get('/users/%s' % user_id, user=user)
        self.assertEqual('bob', get_user['username'])

    def test_grants_set_on_ego(self):
        self.schema.create_field('user', 'references', 'collection', 'employee')

        group_id_1 = self.api.post('/groups', {'name': 'test'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'ego.references'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'create', 'url': 'ego.references'})

        user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})
        self.api.post('/users/%s/groups' % user_id, {'id': group_id_1})

        user = self.schema.load_user_by_username('bob')

        self.api.post('/ego/references', {'name': 'fred'}, user=user)

        employees = self.api.get('/ego/references', user=user)
        self.assertEqual('fred', employees['results'][0]['name'])

    def test_patch_grants_set_on_ego(self):
        self.schema.create_field('user', 'reference', 'link', 'employee')

        group_id_1 = self.api.post('/groups', {'name': 'test'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'read', 'url': 'ego.reference'})
        self.api.post('/groups/%s/grants' % (group_id_1,), {'type': 'update', 'url': 'ego'})

        user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})
        self.api.post('/users/%s/groups' % user_id, {'id': group_id_1})

        employee_id = self.api.post('/employees', {'name': 'Fred'})

        user = self.schema.load_user_by_username('bob')

        self.api.patch('/ego', {'reference': employee_id}, user=user)

        employee = self.api.get('/ego/reference', user=user)
        self.assertEqual('Fred', employee['name'])

    def test_expand_further(self):
        self.assertEqual({
            'a': {
                'b': {},
                'c': {},
            },
            'c': {}
        }, create_expand_dict('a.b,c,a.c'))

        self.assertEqual({
            'a': {
                'b': {},
                'c': {},
            },
        }, create_expand_dict('a.b,a.c,a'))

