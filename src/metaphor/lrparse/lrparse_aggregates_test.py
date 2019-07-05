
import unittest

# using real connetion as mockmongo doesn't support some aggregates
from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource
from metaphor.resource import AggregateField, LinkCollectionSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class LRParseAggregatesTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.employee_spec = ResourceSpec('employee')
        self.section_spec = ResourceSpec('section')

        self.schema.add_resource_spec(self.employee_spec)
        self.schema.add_resource_spec(self.section_spec)

        self.employee_spec.add_field("name", FieldSpec("str"))
        self.section_spec.add_field("name", FieldSpec("str"))
        self.section_spec.add_field("employees", CollectionSpec("employee"))

        self.schema.add_root('employees', CollectionSpec('employee'))
        self.schema.add_root('sections', CollectionSpec('section'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_aggregate_chain(self):
        section_1 = self.api.post('sections', {'name': 'Accounting'})
        section_2 = self.api.post('sections', {'name': 'Marketting'})
        self.api.post('sections/%s/employees' % section_1, {'name': 'Bob'})
        self.api.post('sections/%s/employees' % section_1, {'name': 'Bill'})
        self.api.post('sections/%s/employees' % section_2, {'name': 'Ned'})

        result = self.api.get('sections/employees', {'pageSize': 1})

        self.assertEquals(3, len(result['results']))
        self.assertEquals('Bob', result['results'][0]['name'])
        self.assertEquals('Bill', result['results'][1]['name'])
        self.assertEquals('Ned', result['results'][2]['name'])
