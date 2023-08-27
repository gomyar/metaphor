
import unittest
from datetime import datetime
from urllib.error import HTTPError
from bson.objectid import ObjectId

from pymongo import MongoClient

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api, create_expand_dict


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')
        self.schema.create_field('employee', 'created', 'datetime')

        self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'amount', 'int')
        self.schema.create_field('division', 'employees', 'collection', 'employee')
        self.schema.create_field('division', 'parttimers', 'linkcollection', 'employee')
        self.schema.create_field('division', 'contractors', 'linkcollection', 'employee')

        self.schema.create_field('root', 'employees', 'collection', 'employee')
        self.schema.create_field('root', 'former_employees', 'collection', 'employee')
        self.schema.create_field('root', 'divisions', 'collection', 'division')
        self.schema.create_field('root', 'former_divisions', 'collection', 'division')

        self.schema.create_spec('calcs')
        self.schema.create_field('calcs', 'total_employees', 'calc', calc_str='sum(employees.age)')
        self.schema.create_field('calcs', 'total_former_employees', 'calc', calc_str='sum(former_employees.age)')
        self.schema.create_field('calcs', 'total_division_employees', 'calc', calc_str='sum(divisions.employees.age)')
        self.schema.create_field('calcs', 'total_division_parttimers', 'calc', calc_str='sum(divisions.parttimers.age)')
        self.schema.create_field('calcs', 'total_division_contractors', 'calc', calc_str='sum(divisions.contractors.age)')
        self.schema.create_field('calcs', 'total_divisions', 'calc', calc_str='sum(divisions.amount)')
        self.schema.create_field('calcs', 'total_former_divisions', 'calc', calc_str='sum(former_divisions.amount)')
        self.schema.create_field('calcs', 'total_former_division_employees', 'calc', calc_str='sum(former_divisions.employees.age)')

        self.schema.create_field('root', 'calcs', 'collection', 'calcs')

        self.api = Api(self.db)

    def test_move_resource(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 1})

        # check calc
        self.calcs_id = self.api.post('/calcs', {})
        self.assertEqual(1, self.api.get('/calcs/%s' % self.calcs_id)['total_employees'])

        self.api.put('/former_employees', {'_from': '/employees/%s' % employee_id_1})

        former_employees = self.api.get('/former_employees')
        self.assertEqual(1, former_employees['count'])

        self.assertEqual('bob', former_employees['results'][0]['name'])

        # no longer at original collection
        self.assertEqual(0, self.api.get('/employees')['count'])

        # assert calc update
        self.assertEqual(None, self.api.get('/calcs/%s' % self.calcs_id)['total_employees'])
        self.assertEqual(1, self.api.get('/calcs/%s' % self.calcs_id)['total_former_employees'])

    def test_move_collection(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 1})
        employee_id_2 = self.api.post('/employees', {'name': 'ned', 'age': 1})

        self.api.put('/former_employees', {'_from': '/employees'})

        former_employees = self.api.get('/former_employees')
        self.assertEqual(2, former_employees['count'])

        self.assertEqual('bob', former_employees['results'][0]['name'])
        self.assertEqual('ned', former_employees['results'][1]['name'])

        # no longer at original collection
        self.assertEqual(0, self.api.get('/employees')['count'])

    def test_move_between_nested_collections(self):
        division_id_1 = self.api.post('/divisions', {'name': 'sales'})
        division_id_2 = self.api.post('/divisions', {'name': 'marketting'})

        employee_id_1 = self.api.post('/divisions/%s/employees' % division_id_1, {'name': 'bob'})
        employee_id_2 = self.api.post('/divisions/%s/employees' % division_id_2, {'name': 'ned'})

        self.api.put('/divisions/%s/employees' % division_id_2, {'_from': '/divisions/%s/employees/%s' % (division_id_1, employee_id_1)})

        self.assertEqual(0, self.api.get('/divisions/%s/employees' % division_id_1)['count'])
        self.assertEqual(2, self.api.get('/divisions/%s/employees' % division_id_2)['count'])

        self.assertEqual('bob', self.api.get('/divisions/%s/employees/%s' % (division_id_2, employee_id_1))['name'])
        self.assertEqual('ned', self.api.get('/divisions/%s/employees/%s' % (division_id_2, employee_id_2))['name'])

    def test_basic_update(self):
        employee_id_1 = self.api.post('/employees', {'name': 'bob', 'age': 1})

        # check calc
        self.calcs_id = self.api.post('/calcs', {})
        self.assertEqual(1, self.api.get('/calcs/%s' % self.calcs_id)['total_employees'])

    def test_move_children(self):
        division_id_1 = self.api.post('/divisions', {'name': 'sales'})
        division_id_2 = self.api.post('/divisions', {'name': 'marketting'})

        employee_id_1 = self.api.post('/divisions/%s/employees' % division_id_1, {'name': 'bob'})
        employee_id_2 = self.api.post('/divisions/%s/employees' % division_id_2, {'name': 'ned'})

        self.api.put('/former_divisions', {'_from': '/divisions/%s' % (division_id_1,)})

        self.assertEqual('bob', self.api.get('/former_divisions/%s/employees/%s' % (division_id_1, employee_id_1))['name'])

        # check parent url for child is correct
        employee = self.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_1)})
        self.assertEqual("/former_divisions/%s" % division_id_1, employee['_parent_canonical_url'])

    def test_move_all_children(self):
        division_id_1 = self.api.post('/divisions', {'name': 'sales', 'amount': 1})
        division_id_2 = self.api.post('/divisions', {'name': 'marketting', 'amount': 1})

        employee_id_1 = self.api.post('/divisions/%s/employees' % division_id_1, {'name': 'bob', 'age': 1})
        employee_id_2 = self.api.post('/divisions/%s/employees' % division_id_2, {'name': 'ned', 'age': 1})

        # check before calcs
        self.calcs_id = self.api.post('/calcs', {})
        self.assertEqual(2, self.api.get('/calcs/%s' % self.calcs_id)['total_divisions'])
        self.assertEqual(None, self.api.get('/calcs/%s' % self.calcs_id)['total_former_divisions'])
        self.assertEqual(2, self.api.get('/calcs/%s' % self.calcs_id)['total_division_employees'])
        self.assertEqual(None, self.api.get('/calcs/%s' % self.calcs_id)['total_former_division_employees'])

        self.api.put('/former_divisions', {'_from': '/divisions'})

        self.assertEqual('bob', self.api.get('/former_divisions/%s/employees/%s' % (division_id_1, employee_id_1))['name'])
        self.assertEqual('ned', self.api.get('/former_divisions/%s/employees/%s' % (division_id_2, employee_id_2))['name'])

        # check parent url for child is correct
        employee = self.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_1)})
        self.assertEqual("/former_divisions/%s" % division_id_1, employee['_parent_canonical_url'])

        employee = self.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_2)})
        self.assertEqual("/former_divisions/%s" % division_id_2, employee['_parent_canonical_url'])

        # check after calcs
        self.assertEqual(0, self.api.get('/calcs/%s' % self.calcs_id)['total_divisions'])
        self.assertEqual(2, self.api.get('/calcs/%s' % self.calcs_id)['total_former_divisions'])
        self.assertEqual(0, self.api.get('/calcs/%s' % self.calcs_id)['total_division_employees'])
        self.assertEqual(2, self.api.get('/calcs/%s' % self.calcs_id)['total_former_division_employees'])

    def test_two_phase_delete(self):
        pass

    # update for:
    #   source collection(s)
    #      add dirty to each
    #   target collection

    def test_children_of_children(self):
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')

        self.schema.create_field('employee', 'divisions', 'collection', 'division')
        self.schema.create_field('division', 'employees', 'collection', 'employee')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        self.api = Api(self.db)


        employee_id_1 = self.api.post('/employees', {'name': 'bob'})
        division_id_1 = self.api.post(f'/employees/{employee_id_1}/divisions', {'name': 'sales'})

        employee_id_2 = self.api.post(f'/employees/{employee_id_1}/divisions/{division_id_1}/employees', {'name': 'ned'})

        result = self.api.get(f'/employees/{employee_id_1}/divisions/{division_id_1}/employees')

        self.assertEqual({}, result)

    def test_children_tree(self):
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('employee', 'employees', 'collection', 'employee')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        self.api = Api(self.db)


        employee_id_1 = self.api.post('/employees', {'name': 'bob'})
        employee_id_2 = self.api.post(f'/employees/{employee_id_1}/employees', {'name': 'fred'})
        employee_id_3 = self.api.post(f'/employees/{employee_id_1}/employees/{employee_id_2}/employees', {'name': 'ned'})

        result = self.api.get(f'/employees/{employee_id_1}/employees/{employee_id_2}/employees')

        self.assertEqual({}, result)
