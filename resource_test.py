
import unittest

from mongomock import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class SpikeTest(unittest.TestCase):
    def setUp(self):
        self.db = MongoClient().db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')
        self.portfolio_spec = ResourceSpec('portfolio')
        self.financial_spec = ResourceSpec('financial')
        self.config_spec = ResourceSpec('config')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)
        self.schema.add_resource_spec(self.portfolio_spec)
        self.schema.add_resource_spec(self.financial_spec)
        self.schema.add_resource_spec(self.config_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec('period'))

        self.period_spec.add_field("period", FieldSpec("string"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("financial", ResourceLinkSpec("financial"))

        self.financial_spec.add_field("totalAssets", FieldSpec("int"))

        self.portfolio_spec.add_field("name", FieldSpec("string"))
        self.portfolio_spec.add_field("companies", CollectionSpec('company'))

        self.config_spec.add_field("ppp", FieldSpec("int"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('portfolios', CollectionSpec('portfolio'))
        self.schema.add_root('config', ResourceLinkSpec('config'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_find(self):
        company_id = self.db['resource_company'].insert(
            {'name': 'Bobs Burgers'})

        period_id = self.db['resource_period'].insert(
            {'year': 2017, 'period': 'YE', '_owners': [
                {'owner_field': 'periods',
                 'owner_id': company_id,
                 'owner_spec': 'company'}
            ]})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals("Bobs Burgers", company['name'])
        self.assertEquals("http://server/companies/%s/periods" % (company_id,),
            company['periods'])
        self.assertEquals(
            "http://server/companies/%s/periods" % (company_id,),
            company['periods'])

        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))

        self.assertEquals(2017, period['year'])

        self.assertEquals([
            {'id': str(company_id), 'name': 'Bobs Burgers', 'periods': 'http://server/companies/%s/periods' % (company_id,)}
        ], self.api.get('/companies'))

        self.assertEquals({
            'companies': 'http://server/companies',
            'portfolios': 'http://server/portfolios',
            'config': 'http://server/config',
        }, self.api.get('/'))

    def test_add_data(self):
        company_id = self.api.post('companies', dict(name='Bobs Burgers'))

        new_company = self.db['resource_company'].find_one({'_id': company_id})
        self.assertEquals("Bobs Burgers", new_company['name'])

        period_id = self.api.post('companies/%s/periods' % (company_id,),
                                    dict(year=2017, period='YE'))

        company = self.db['resource_company'].find_one({'_id': company_id})
        self.assertEquals('Bobs Burgers', company['name'])
        period = self.db['resource_period'].find_one({'_id': period_id})
        self.assertEquals('YE', period['period'])
        self.assertEquals(2017, period['year'])
        self.assertEquals([
            {'owner_field': 'periods',
             'owner_id': company_id,
             'owner_spec': 'company'}], period['_owners'])

    def test_add_resource_to_other_collection(self):
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        portfolio_1_id = self.api.post('portfolios', {'name': 'Portfolio 1'})
        portfolio_2_id = self.api.post('portfolios', {'name': 'Portfolio 2'})

        self.api.post('portfolios/%s/companies' % (portfolio_1_id,),
                        {'id': company_id})

        p1_companies = self.api.get('portfolios/%s/companies' % (portfolio_1_id,))
        self.assertEquals(1, len(p1_companies))
        self.assertEquals('Neds Fries', p1_companies[0]['name'])

        p2_companies = self.api.get('portfolios/%s/companies' % (portfolio_2_id,))
        self.assertEquals(0, len(p2_companies))

        self.assertEquals('Neds Fries', self.api.get('portfolios/%s/companies/%s' % (portfolio_1_id, company_id))['name'])

        self.api.unlink('portfolios/%s/companies/%s' % (portfolio_1_id, company_id))

        p1_companies = self.api.get('portfolios/%s/companies' % (portfolio_1_id,))
        self.assertEquals(0, len(p1_companies))

    def test_embedded_financials_default(self):
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        period_id = self.api.post('companies/%s/periods' % (company_id,),
            {'year': 2017, 'period': 'YE'})

        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))
        self.assertEquals(2017, period['year'])
        self.assertEquals('YE', period['period'])
        self.assertEquals(None, period['financial'])

    def test_patch(self):
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        self.api.patch('companies/%s' % (company_id,), {'name': 'Norman'})
        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals('Norman', company['name'])

    def test_embedded_financials_create(self):
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        period_id = self.api.post('companies/%s/periods' % (company_id,),
            {'year': 2017,
             'period': 'YE',
             'financial': {
                    "totalAssets": 100,
            }})

        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))
        self.assertEquals(2017, period['year'])
        self.assertEquals('YE', period['period'])
        self.assertEquals("http://server/companies/%s/periods/%s/financial" % (company_id, period_id), period['financial'])

        financial = self.api.get('companies/%s/periods/%s/financial' % (company_id, period_id))
        self.assertEquals(100, financial['totalAssets'])

    def test_save_schema(self):
        serialized = self.schema.serialize()
        self.assertTrue('root' in serialized)
        self.assertEquals(['company', 'config', 'financial', 'period', 'portfolio', 'root'],
                          sorted(serialized.keys()))

    def _test_resource_at_root(self):
        before = self.api.get('config')
        self.assertEquals(None, before)
        self.api.post('config', {'ppp': 100})
        after = self.api.get('config')
        self.assertEquals(100, after['ppp'])

    def test_resource_link_points_to_another_resource_link(self):
        pass

    def test_resource_equality_based_on_path_and_filters(self):
        company_1 = self.api.post('companies', {'name': 'C1'})
        company_2 = self.api.post('companies', {'name': 'C2'})

        resource_1 = self.api.root.build_child('companies/%s' % (company_1,))
        resource_2 = self.api.root.build_child('companies/%s' % (company_2,))

        self.assertFalse(resource_1 == resource_2)

        resource_3 = self.api.root.build_child('companies/%s/periods' % (company_1,))
        resource_4 = self.api.root.build_child('companies/%s/periods' % (company_2,))

        self.assertFalse(resource_3 == resource_4)
