
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

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
