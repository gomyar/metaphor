
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from urllib.error import HTTPError

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.admin_api import AdminApi


class AdminApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.api = Api(self.db)
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
                    'average_age': {
                        'calc_str': 'average(self.employees.age)',
                        'deps': ['branch.employees',
                                 'employee.age'],
                        'type': 'calc'
                    },
                    'employees': {
                        'target_spec_name': 'employee',
                        'type': 'linkcollection'
                    },
            }, self.db['metaphor_schema'].find_one()['specs']['branch']['fields'])
        self.assertEqual(['employees', 'average_age'], list(self.schema.specs['branch'].fields.keys()))

    def test_delete_field_removes_existing_data(self):
        employee_id = self.api.post('/employees', {'name': 'Bob', 'age': 21})
        employee_id_2 = self.api.post('/employees', {'name': 'Ned', 'age': 17})

        self.admin_api.delete_field('employee', 'name')

        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            '_canonical_url': '/employees/%s' % employee_id,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'employee',
            'age': 21},
            {'_id': self.schema.decodeid(employee_id_2),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            '_canonical_url': '/employees/%s' % employee_id_2,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'employee',
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

    def test_error_on_invalid_calc(self):
        try:
            self.admin_api.create_field('branch', 'max_age', 'calc', calc_str='max(self.i_dont_exist.age)')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual('SyntaxError in calc: No such field i_dont_exist in branch', he.reason)

        # check field was not saved to the DB
        self.assertEqual(['name', 'employees', 'average_age'], list(self.db['metaphor_schema'].find_one()['specs']['branch']['fields'].keys()))

    def test_check_spec_exists(self):
        try:
            self.admin_api.create_field('nonexistant', 'name', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(404, he.code)
            self.assertEqual("Not Found", he.reason)

    def test_check_field_already_exists(self):
        try:
            self.admin_api.create_field('employee', 'name', 'str')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual("Field already exists: name", he.reason)

    def test_check_circular_dependency(self):
        self.admin_api.create_field('employee', 'my_age', 'calc', calc_str='self.age')
        self.admin_api.create_field('employee', 'other_field', 'calc', calc_str='self.my_age')
        try:
            self.admin_api.update_field('employee', 'my_age', 'calc', calc_str='self.other_field')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual("employee.my_age has circular dependencies: {'employee.my_age': {'employee.other_field'}, 'employee.other_field': {'employee.my_age'}}", he.reason)

    def test_check_circular_dependency_between_resources(self):
        self.admin_api.create_field('employee', 'my_age', 'calc', calc_str='self.link_branch_employees.average_age')
        try:
            self.admin_api.update_field('branch', 'average_age', 'calc', calc_str='self.employees.my_age')
            self.fail("Should have thrown")
        except HTTPError as he:
            self.assertEqual(400, he.code)
            self.assertEqual("branch.average_age has circular dependencies: {'employee.my_age': {'branch.average_age'}, 'branch.average_age': {'employee.my_age'}}", he.reason)

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

    def test_create_calc_against_null_field(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob'})
        self.admin_api.create_field('employee', 'my_age', 'calc', calc_str='self.age')
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'age': None,
            'id': employee_id_1,
            'link_branch_employees': '/employees/%s/link_branch_employees' % employee_id_1,
            'my_age': None,
            'name': 'bob',
            'self': '/employees/%s' % employee_id_1}, self.api.get('/employees/%s' % employee_id_1))

    def test_create_ternary(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob'})
        self.admin_api.create_field('employee', 'my_age', 'calc', calc_str='self.name = "bob" -> 12: 14')
        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEqual(12, employee['my_age'])

    def test_create_switch(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob'})
        self.admin_api.create_field('employee', 'my_age', 'calc', calc_str='self.name -> ("bob": 12, "fred": 14)')
        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEqual(12, employee['my_age'])

    def test_create_switch_with_collections(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 25})
        employee_id_2 = self.api.post('/employees', {'name': 'ned', 'age': 35})

        branch_id_1 = self.api.post('/branches', {'name': 'sales'})
        self.api.post('/branches/%s/employees' % branch_id_1, {'id': employee_id_1})
        self.api.post('/branches/%s/employees' % branch_id_1, {'id': employee_id_2})

        self.admin_api.create_field('branch', 'older_employees', 'calc', calc_str='self.name -> ("sales": (self.employees[age>20]), "marketting": (self.employees[age>30]))')
        older_employees = self.api.get('/branches/%s/older_employees' % branch_id_1)
        self.assertEqual(2, older_employees['count'])

        # alter state, check for update
        self.api.patch('/branches/%s' % branch_id_1, {'name': 'marketting'})
        older_employees = self.api.get('/branches/%s/older_employees' % branch_id_1)
        self.assertEqual(1, older_employees['count'])

        # alter age, check for update
        self.api.patch('/employees/%s' % employee_id_1, {'age': 31})
        older_employees = self.api.get('/branches/%s/older_employees' % branch_id_1)
        self.assertEqual(2, older_employees['count'])

    def test_create_switch_field_calc(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 10})
        self.admin_api.create_field('employee', 'my_age', 'calc', calc_str='self.name -> ("bob": (self.age * 12.0), "fred": 14.0)')
        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEqual(120, employee['my_age'])

    def test_create_switch_field_calc_resource_ref(self):
        self.admin_api.create_field('employee', 'type', 'str')
        self.admin_api.create_field('employee', 'calced_initial', 'int')
        self.admin_api.create_field('employee', 'calced_value', 'calc', calc_str='self.calced_initial')

        self.admin_api.create_field('employee', 'calc_switch', 'calc', calc_str="self.type -> (  'parttime': (600 * self.calced_value),  'fulltime': self.calced_value)")

        employee_id_1 = self.api.post('/employees', {'type': 'parttime', 'name': 'bob', 'age': 10, 'calced_initial': 10})
        employee = self.api.get('/employees/%s' % employee_id_1)
        self.assertEqual(6000, employee['calc_switch'])

    def test_resolve_calc_str(self):
        spec, is_collection = self.admin_api.resolve_calc_metadata(self.schema, 'employees')
        self.assertEqual("employee", spec.name)
        self.assertTrue(is_collection)

        spec, is_collection = self.admin_api.resolve_calc_metadata(self.schema, 'branches.employees')
        self.assertEqual("employee", spec.name)
        self.assertTrue(is_collection)

    def test_resolve_calc_str_error(self):
        try:
            self.admin_api.resolve_calc_metadata(self.schema, 'branches.nope')
        except Exception as e:
            self.assertEqual("No such field nope in branch", str(e))
