
import unittest

from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater

import logging
log = logging.getLogger('metaphor.test')


class UpdaterBackgroundTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.api = Api(self.db)

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')

        self.division_spec = self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'employees', 'collection', 'employee')
        self.schema.create_field('division', 'managers', 'linkcollection', 'employee')
        self.schema.create_field('division', 'older_managers', 'calc', calc_str='self.managers[age>30]')

        self.company_spec = self.schema.create_spec('company')
        self.schema.create_field('company', 'name', 'str')
        self.schema.create_field('company', 'division', 'link', 'division')
        self.schema.create_field('company', 'max_age', 'calc', calc_str='max(self.division.older_managers.age)')

        self.schema.create_field('root', 'companies', 'collection', 'company')
        self.schema.create_field('root', 'divisions', 'collection', 'division')

    def test_update(self):
        company_1_id = self.api.post('/companies', {'name': 'Bobs Burgers'})
        division_1_id = self.api.post('/divisions', {'name': 'Kitchen'})

        employee_1_id = self.api.post('/divisions/%s/employees' % division_1_id, {'name': 'Bob', 'age': 38})
        employee_2_id = self.api.post('/divisions/%s/employees' % division_1_id, {'name': 'Linda', 'age': 36})

        self.api.post('/divisions/%s/managers' % division_1_id, {'id': employee_1_id})
        self.api.post('/divisions/%s/managers' % division_1_id, {'id': employee_2_id})

        self.assertEqual(None, self.api.get('/companies/%s' % company_1_id)['max_age'])

        self.api.patch('/companies/%s' % company_1_id, {'division': division_1_id})

        self.assertEqual(38, self.api.get('/companies/%s' % company_1_id)['max_age'])
