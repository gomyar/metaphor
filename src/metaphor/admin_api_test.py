
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from urllib.error import HTTPError

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.admin_api import AdminApi


class AdminApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.api = Api(self.schema)
        self.admin_api = AdminApi(self.schema)

        self.admin_api.create_spec('employee')
        self.admin_api.create_field('employee', 'name', 'str')
        self.admin_api.create_field('employee', 'age', 'int')

        self.admin_api.create_spec('branch')
        self.admin_api.create_field('branch', 'name', 'str')
        self.admin_api.create_field('branch', 'employees', 'linkcollection', 'employee')
        self.admin_api.create_field('branch', 'average_age', 'calc', calc_str='average(self.employees.age)')

        self.admin_api.create_field('root', 'employees', 'collection', 'employee')
        self.admin_api.create_field('root', 'branches', 'collection', 'branch')

    def test_add_spec(self):
        employee_id = self.api.post('/employees', {'name': 'Bob', 'age': 21})

        branch_id = self.api.post('/branches', {'name': 'Sales'})

        branch = self.api.get('/branches/%s' % branch_id)
        self.assertEquals(None, branch['average_age'])

        self.api.post('/branches/%s/employees' % branch_id, {'id': employee_id})

        branch = self.api.get('/branches/%s' % branch_id)
        self.assertEquals(21, branch['average_age'])

    def test_delete_field(self):
        self.admin_api.delete_field('branch', 'name')

        self.assertEqual({
            'branch': {
                'fields': {
                    'average_age': {
                        'calc_str': 'average(self.employees.age)',
                        'type': 'calc'
                    },
                    'employees': {
                        'target_spec_name': 'employee',
                        'type': 'linkcollection'
                    },
                }
            },
            'employee': {
                'fields': {
                    'age': {'type': 'int'},
                    'name': {'type': 'str'}
                }
            }}, self.db['metaphor_schema'].find_one()['specs'])
        self.assertEqual(['employees', 'average_age'], list(self.schema.specs['branch'].fields.keys()))

    def test_delete_field_removes_existing_data(self):
        employee_id = self.api.post('/employees', {'name': 'Bob', 'age': 21})
        employee_id_2 = self.api.post('/employees', {'name': 'Ned', 'age': 17})

        self.admin_api.delete_field('employee', 'name')

        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id),
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
            'age': 21},
            {'_id': self.schema.decodeid(employee_id_2),
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
            'age': 17}], list(self.db['resource_employee'].find()))
        self.assertEqual(['age', 'link_branch_employees'], list(self.schema.specs['employee'].fields.keys()))

    def test_delete_field_error_when_referenced_by_calc(self):
        try:
            self.admin_api.delete_field('branch', 'employees')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual("branch.employees referenced by ['branch.average_age']", he.reason)

    def test_delete_field_removes_reverse_links(self):
        # remove referencing calc first
        self.admin_api.delete_field('branch', 'average_age')
        self.admin_api.delete_field('branch', 'employees')

        self.assertEqual(['name', 'age'], list(self.schema.specs['employee'].fields.keys()))

    def test_delete_field_error_when_secondary_link_referenced(self):
        self.admin_api.create_field('employee', 'average_branch_names', 'calc', calc_str='self.link_branch_employees.name')

        # remove referencing calc first - admittedly a bit backsy-forthsy
        self.admin_api.delete_field('branch', 'average_age')

        try:
            self.admin_api.delete_field('branch', 'employees')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual("branch.employees referenced by ['employee.average_branch_names']", he.reason)

    def test_add_calc_field_updates_resources(self):
        employee_id = self.api.post('/employees', {'name': 'Bob', 'age': 21})
        employee_id_2 = self.api.post('/employees', {'name': 'Ned', 'age': 17})
        branch_id = self.api.post('/branches', {'name': 'Sales'})
        self.api.post('/branches/%s/employees' % branch_id, {'id': employee_id})
        self.api.post('/branches/%s/employees' % branch_id, {'id': employee_id_2})

        self.admin_api.create_field('branch', 'max_age', 'calc', calc_str='max(self.employees.age)')

        branch = self.api.get('/branches/%s' % branch_id)
        self.assertEquals(19, branch['average_age'])
        self.assertEquals(21, branch['max_age'])

    def test_reserved_words(self):
        # link_*
        try:
            self.admin_api.create_field('branch', 'link_something', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Field name cannot begin with "link_"', he.reason)

        # parent_*
        try:
            self.admin_api.create_field('branch', 'parent_something', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Field name cannot begin with "parent_"', he.reason)

        # self
        try:
            self.admin_api.create_field('branch', 'self', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Field name cannot be reserverd word "self"', he.reason)

        # id
        try:
            self.admin_api.create_field('branch', 'id', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Field name cannot be reserverd word "id"', he.reason)

        # _*
        try:
            self.admin_api.create_field('branch', '_name', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Field name cannot begin with "_"', he.reason)

        # [0-9]*
        try:
            self.admin_api.create_field('branch', '123name', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('First character must be letter', he.reason)

        # blank
        try:
            self.admin_api.create_field('branch', '', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('Field name cannot be blank', he.reason)
