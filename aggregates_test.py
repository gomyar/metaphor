
import unittest

from mongomock import Connection

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class AggregatesTest(unittest.TestCase):
    def setUp(self):
        self.db = Connection().db
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

    def test_portfolio_sizes(self):
        all_companies = self.api.get('companies')
        self.assertEquals(5, len(all_companies))

        self.assertEquals(2, len(self.api.get('portfolios/%s/companies' % (self.portfolio_1,))))
        self.assertEquals(2, len(self.api.get('portfolios/%s/companies' % (self.portfolio_2,))))

    def test_aggregate_portfolio_companies(self):
        all_companies = self.api.get('portfolios/companies')
        self.assertEquals(4, len(all_companies))
