
import unittest

# using real connetion as mockmongo doesn't support some aggregates
from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource
from metaphor.resource import AggregateField
from metaphor.schema import Schema
from metaphor.api import MongoApi


class AggregatesTest(unittest.TestCase):
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

        self.company_1 = self.api.post('companies', {'name': 'Bob1'})
        self.company_2 = self.api.post('companies', {'name': 'Bob2'})
        self.company_3 = self.api.post('companies', {'name': 'Bob3'})
        self.company_4 = self.api.post('companies', {'name': 'Bob4'})
        self.company_5 = self.api.post('companies', {'name': 'Bob5'})

        self.portfolio_1 = self.api.post('portfolios', {'name': 'Portfolio1'})
        self.portfolio_2 = self.api.post('portfolios', {'name': 'Portfolio2'})
        self.portfolio_3 = self.api.post('portfolios', {'name': 'Portfolio3'})

        self.api.post('portfolios/%s/companies' % (self.portfolio_1,), {'id': self.company_1})
        self.api.post('portfolios/%s/companies' % (self.portfolio_1,), {'id': self.company_2})

        self.api.post('portfolios/%s/companies' % (self.portfolio_2,), {'id': self.company_3})
        self.api.post('portfolios/%s/companies' % (self.portfolio_2,), {'id': self.company_4})

        self.api.post('companies/%s/periods' % (self.company_1,), {'year': 2017, 'period': 'YE', 'totalAssets': 10})
        self.api.post('companies/%s/periods' % (self.company_2,), {'year': 2017, 'period': 'YE', 'totalAssets': 20})
        self.api.post('companies/%s/periods' % (self.company_3,), {'year': 2017, 'period': 'YE', 'totalAssets': 30})
        self.api.post('companies/%s/periods' % (self.company_4,), {'year': 2017, 'period': 'YE', 'totalAssets': 40})
        self.api.post('companies/%s/periods' % (self.company_5,), {'year': 2017, 'period': 'YE', 'totalAssets': 50})

    def test_portfolio_sizes(self):
        all_companies = self.api.get('companies')
        self.assertEquals(5, len(all_companies))

        self.assertEquals(2, len(self.api.get('portfolios/%s/companies' % (self.portfolio_1,))))
        self.assertEquals(2, len(self.api.get('portfolios/%s/companies' % (self.portfolio_2,))))

    def test_aggregate_portfolio_companies(self):
        all_companies = self.api.get('portfolios/companies')
        self.assertEquals(4, len(all_companies))

        self.assertEquals('Bob1', all_companies[0]['name'])
        self.assertEquals('Bob2', all_companies[1]['name'])
        self.assertEquals('Bob3', all_companies[2]['name'])
        self.assertEquals('Bob4', all_companies[3]['name'])

    def test_aggregate_portfolio_companies_periods(self):
        all_periods = self.api.get('portfolios/companies/periods')

        self.assertEquals(4, len(all_periods))

        self.assertEquals(2017, all_periods[0]['year'])
        self.assertEquals(2017, all_periods[1]['year'])
        self.assertEquals(2017, all_periods[2]['year'])
        self.assertEquals(2017, all_periods[3]['year'])

    def test_lang_parser(self):
        aggregate_resource = self.api.root.build_lang_resource('portfolios[%s].companies.periods' % (self.portfolio_1,))
        self.assertEquals(AggregateResource, type(aggregate_resource))

        aggregate_field = self.api.root.build_lang_resource('portfolios[%s].companies.periods.totalAssets')
        self.assertEquals(AggregateField, type(aggregate_resource))
