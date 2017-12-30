
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
        self.financial_spec = ResourceSpec('financial')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)
        self.schema.add_resource_spec(self.portfolio_spec)
        self.schema.add_resource_spec(self.financial_spec)
        self.schema.add_resource_spec(self.config_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec('period'))

        self.period_spec.add_field("period", FieldSpec("string"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("totalAssets", FieldSpec("int"))
        self.period_spec.add_field("financial", ResourceLinkSpec("financial"))

        self.financial_spec.add_field("grossProfit", FieldSpec("int"))

        self.portfolio_spec.add_field("name", FieldSpec("string"))
        self.portfolio_spec.add_field("companies", CollectionSpec('company'))

        self.config_spec.add_field("ppp", FieldSpec("int"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('portfolios', CollectionSpec('portfolio'))
        self.schema.add_root('config', ResourceLinkSpec('config'))
        self.schema.add_root('financials', CollectionSpec('financial'))

        self.api = MongoApi('http://server', self.schema, self.db)

        self.company_1 = self.api.post('companies', {'name': 'ONE'})
        self.company_2 = self.api.post('companies', {'name': 'TWO'})

        self.portfolio_1 = self.api.post('portfolios', {'name': 'ALPHA'})
        self.portfolio_2 = self.api.post('portfolios', {'name': 'BETA'})

        self.api.post('portfolios/%s/companies' % (self.portfolio_1,), {'id': self.company_1})

        self.api.post('portfolios/%s/companies' % (self.portfolio_2,), {'id': self.company_2})

        self.period_1 = self.api.post('companies/%s/periods' % (self.company_1,), {'year': 2017, 'period': 'YE', 'totalAssets': 10})
        self.period_2 = self.api.post('companies/%s/periods' % (self.company_2,), {'year': 2017, 'period': 'YE', 'totalAssets': 20})

        self.api.post('companies/%s/periods/%s/financial' % (self.company_1, self.period_1), {'grossProfit': 20})
        self.api.post('companies/%s/periods/%s/financial' % (self.company_1, self.period_2), {'grossProfit': 20})

    def test_aggregate_chain(self):
        periods_resource = self.api.build_resource('portfolios/companies/periods')
        self.assertEquals(AggregateResource, type(periods_resource))

        chain = periods_resource.build_aggregate_chain()
        self.assertEquals(
            [{'$unwind': '$_owners'},
            {'$match': {'_owners.owner_field': 'periods',
                        '_owners.owner_spec': 'company'}},
            {'$lookup': {'as': '__company',
                        'foreignField': '_id',
                        'from': 'resource_company',
                        'localField': '_owners.owner_id'}},
            {'$unwind': '$__company'},
            {'$unwind': '$__company._owners'},
            {'$match': {'__company._owners.owner_field': 'companies',
                        '__company._owners.owner_spec': 'portfolio'}},
            {'$lookup': {'as': '__company__portfolio',
                        'foreignField': '_id',
                        'from': 'resource_portfolio',
                        'localField': '__company._owners.owner_id'}},
            {'$unwind': '$__company__portfolio'}]
        , chain)

    def test_resource_object_aggregate_chain_for_dependencies(self):
        resource = self.api.build_resource('companies/%s/periods' % (self.company_1,))

        self.assertEquals([
            {'$unwind': '$_owners'},
            {'$match': {'_owners.owner_field': 'periods',
                        '_owners.owner_spec': 'company'}},
            {'$lookup': {'as': '__company',
                        'foreignField': '_id',
                        'from': 'resource_company',
                        'localField': '_owners.owner_id'}},
            {'$unwind': '$__company'},
            {'$match': {'__company._id': self.company_1}}]
        , resource.build_aggregate_chain())
        self.assertEquals("resource_period", resource.collection)
        self.assertEquals("companies/%s/periods" % (self.company_1,), resource.url)

    def test_resource_object_aggregate_chain_for_dependencies_2(self):
        resource = self.api.build_resource('portfolios/%s/companies/%s/periods/financial' % (self.portfolio_1, self.company_1,))

        self.assertEquals([
			{'$unwind': '$_owners'},
			{'$match': {'_owners.owner_field': 'financial',
						'_owners.owner_spec': 'period'}},
			{'$lookup': {'as': '__period',
						'foreignField': '_id',
						'from': 'resource_period',
						'localField': '_owners.owner_id'}},
			{'$unwind': '$__period'},
			{'$unwind': '$__period._owners'},
			{'$match': {'__period._owners.owner_field': 'periods',
						'__period._owners.owner_spec': 'company'}},
			{'$lookup': {'as': '__period__company',
						'foreignField': '_id',
						'from': 'resource_company',
						'localField': '__period._owners.owner_id'}},
			{'$unwind': '$__period__company'},
			{'$match': {'__period__company._id': self.company_1}},
			{'$unwind': '$__period__company._owners'},
			{'$match': {'__period__company._owners.owner_field': 'companies',
						'__period__company._owners.owner_spec': 'portfolio'}},
			{'$lookup': {'as': '__period__company__portfolio',
						'foreignField': '_id',
						'from': 'resource_portfolio',
						'localField': '__period__company._owners.owner_id'}},
			{'$unwind': '$__period__company__portfolio'},
			{'$match': {'__period__company__portfolio._id': self.portfolio_1}}]
        , resource.build_aggregate_chain())
        self.assertEquals("resource_financial", resource.collection)
        self.assertEquals("portfolios/%s/companies/%s/periods/financial" % (self.portfolio_1, self.company_1,), resource.url)

    def test_resource_object_aggregate_chain_for_calcs(self):
        pass

    def test_resource_cannot_be_first_order_aggregate(self):
        resource = self.api.build_resource('companies/%s' % (self.company_1,))
        self.assertRaises(Exception, resource.build_aggregate_chain)
