
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

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)

        self.company_spec.add_field("name", FieldSpec("str"))

        self.company_spec.add_field("onePeriod", ResourceLinkSpec('period'))
        self.company_spec.add_field("latestPeriod", CalcSpec('self.onePeriod', 'period'))
        self.company_spec.add_field("latestPeriod_year", CalcSpec('self.latestPeriod.year', 'int'))

        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("period", FieldSpec("str"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('periods', CollectionSpec('period'))

        self.api = MongoApi('server', self.schema, self.db)

    def test_indirect_calc_links(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        period_id = self.api.post('periods', {'year': 2018, 'period': 'Q1'})
        self.api.post('companies/%s/onePeriod' % (company_id,), {'id': period_id})

        company = self.api.get('companies/%s' % (company_id,))

        self.assertEquals(2018, company['latestPeriod_year'])
