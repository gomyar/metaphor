
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.updater import Updater

import logging
log = logging.getLogger('metaphor.test')


class UpdaterBackgroundTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.api = Api(self.schema)

        self.employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(self.employee_spec, 'name', 'str')
        self.schema.add_field(self.employee_spec, 'age', 'int')

        self.division_spec = self.schema.add_spec('division')
        self.schema.add_field(self.division_spec, 'name', 'str')
        self.schema.add_field(self.division_spec, 'employees', 'collection', 'employee')
        self.schema.add_field(self.division_spec, 'managers', 'linkcollection', 'employee')
        self.schema.add_calc(self.division_spec, 'older_managers', 'self.managers[age>30]')

        self.company_spec = self.schema.add_spec('company')
        self.schema.add_field(self.company_spec, 'name', 'str')
        self.schema.add_field(self.company_spec, 'division', 'link', 'division')
        self.schema.add_calc(self.company_spec, 'max_age', 'max(self.division.older_managers.age)')

        self.schema.add_field(self.schema.root, 'companies', 'collection', 'company')
        self.schema.add_field(self.schema.root, 'divisions', 'collection', 'division')

    def test_update(self):
        company_1_id = self.api.post('/companies', {'name': 'Bobs Burgers'})
        division_1_id = self.api.post('/divisions', {'name': 'Kitchen'})

        employee_1_id = self.api.post('/divisions/%s/employees' % division_1_id, {'name': 'Bob', 'age': 38})
        employee_2_id = self.api.post('/divisions/%s/employees' % division_1_id, {'name': 'Linda', 'age': 36})

        self.api.post('/divisions/%s/managers' % division_1_id, {'id': employee_1_id})
        self.api.post('/divisions/%s/managers' % division_1_id, {'id': employee_2_id})

        self.assertEquals(None, self.api.get('/companies/%s' % company_1_id)['max_age'])

        self.api.patch('/companies/%s' % company_1_id, {'division': division_1_id})

        self.assertEquals(38, self.api.get('/companies/%s' % company_1_id)['max_age'])
