
import unittest

# using real connetion as mockmongo doesn't support some aggregates
from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource
from metaphor.resource import AggregateField
from metaphor.schema import Schema
from metaphor.api import MongoApi


class AggregateChainTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')
        self.portfolio_spec = ResourceSpec('portfolio')
        self.config_spec = ResourceSpec('config')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)
        self.schema.add_resource_spec(self.portfolio_spec)
        self.schema.add_resource_spec(self.config_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec('period'))

        self.period_spec.add_field("period", FieldSpec("string"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("totalAssets", FieldSpec("int"))

        self.portfolio_spec.add_field("name", FieldSpec("string"))
        self.portfolio_spec.add_field("companies", CollectionSpec('company'))

        self.config_spec.add_field("ppp", FieldSpec("int"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('portfolios', CollectionSpec('portfolio'))
        self.schema.add_root('config', ResourceLinkSpec('config'))

        self.api = MongoApi('http://server', self.schema, self.db)

        self.company_1 = self.api.post('companies', {'name': 'ONE'})
        self.company_2 = self.api.post('companies', {'name': 'TWO'})

        self.portfolio_1 = self.api.post('portfolios', {'name': 'ALPHA'})
        self.portfolio_2 = self.api.post('portfolios', {'name': 'BETA'})

        self.api.post('portfolios/%s/companies' % (self.portfolio_1,), {'id': self.company_1})

        self.api.post('portfolios/%s/companies' % (self.portfolio_2,), {'id': self.company_2})

        self.api.post('companies/%s/periods' % (self.company_1,), {'year': 2017, 'period': 'YE', 'totalAssets': 10})
        self.api.post('companies/%s/periods' % (self.company_2,), {'year': 2017, 'period': 'YE', 'totalAssets': 20})

    def test_aggregate_chain(self):
        periods_resource = self.api.root.build_child('portfolios/companies/periods')
        self.assertEquals(AggregateResource, type(periods_resource))

        chain = periods_resource.build_aggregate_chain("")
        self.assertEquals([
            {'$lookup': {'as': '_owners_company',
                'foreignField': '_id',
                'from': 'resource_company',
                'localField': '_owners.owner_id'}},
            {'$lookup': {'as': '_owners_portfolio',
                'foreignField': '_id',
                'from': 'resource_portfolio',
                'localField': '_owners_company._owners.owner_id'}},
            {'$match': {'_owners_portfolio': {'$ne': []}}}], chain)
