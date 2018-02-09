
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.calclang import parser
from metaphor.schema import Schema
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec, CalcSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource, AggregateField
from metaphor.api import MongoApi
from metaphor.resource import Resource


class ResourceCalcTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.schema.register_function('latest_period', self._latest_period_func)

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field(
            "latestPeriod",
            CalcSpec('latest_period(self.periods)'))

        self.period_spec.add_field("year", FieldSpec("int"))

        self.schema.add_root('companies', CollectionSpec('company'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def _latest_period_func(self, periods):
        period_resources = periods.serialize('')
        if period_resources:
            return max(period_resources, key=lambda p: p['year'])
        else:
            return None

    def test_function_returns_resource(self):
        company_id = self.api.post('companies', {'name': 'Bob'})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(None, company['latestPeriod'])

        self.api.post('companies/%s/periods' % (company_id,), {'year': 2015})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(2015, company['latestPeriod']['year'])

        self.api.post('companies/%s/periods' % (company_id,), {'year': 2016})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(2016, company['latestPeriod']['year'])

    def test_function_works_with_delete(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        period_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2015})

        self.api.unlink('companies/%s/periods/%s' % (company_id, period_id))

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(None, company['latestPeriod'])
