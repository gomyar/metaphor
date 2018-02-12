
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId
from requests.exceptions import HTTPError

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi

from datetime import datetime
from mock import patch


class SpikeTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
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

        self.company_spec.add_field("name", FieldSpec("str"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field("public", FieldSpec("bool"))
        self.company_spec.add_field("totalTotalAssets", CalcSpec("sum(companies.periods.financial.totalAssets)"))
        self.company_spec.add_field("totalFinancialsAssets", CalcSpec('sum(financials.totalAssets)'))

        self.period_spec.add_field("period", FieldSpec("str"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("financial", ResourceLinkSpec("financial"))

        self.financial_spec.add_field("totalAssets", FieldSpec("int"))

        self.portfolio_spec.add_field("name", FieldSpec("str"))
        self.portfolio_spec.add_field("companies", CollectionSpec('company'))

        self.config_spec.add_field("ppp", FieldSpec("int"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('portfolios', CollectionSpec('portfolio'))
        self.schema.add_root('financials', CollectionSpec('financial'))
        self.schema.add_root('config', ResourceLinkSpec('config'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_find(self):
        company_id = self.db['resource_company'].insert(
            {'name': 'Bobs Burgers', 'public': True})

        period_id = self.db['resource_period'].insert(
            {'year': 2017, 'period': 'YE', 'public': True, '_owners': [
                {'owner_field': 'periods',
                 'owner_id': company_id,
                 'owner_spec': 'company'}
            ]})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals("Bobs Burgers", company['name'])
        self.assertEquals("http://server/api/companies/%s/periods" % (company_id,),
            company['periods'])
        self.assertEquals(
            "http://server/api/companies/%s/periods" % (company_id,),
            company['periods'])

        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))

        self.assertEquals(2017, period['year'])

        self.assertEquals([
            {'id': str(company_id), 'name': 'Bobs Burgers', 'public': True, 'periods': 'http://server/api/companies/%s/periods' % (company_id,), 'totalTotalAssets': None, 'totalFinancialsAssets': None}
        ], self.api.get('/companies'))

        self.assertEquals({
            'companies': 'http://server/api/companies',
            'portfolios': 'http://server/api/portfolios',
            'financials': 'http://server/api/financials',
            'config': 'http://server/api/config',
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

    @patch('metaphor.resource.datetime')
    def test_patch(self, dt):
        dt.now.return_value = datetime(2018, 1, 2, 3, 4, 5)
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        self.api.patch('companies/%s' % (company_id,), {'name': 'Norman'})
        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals('Norman', company['name'])
        self.assertEquals(
            {'_id': company_id,
             'name': u'Norman',
             'public': None,
             'totalTotalAssets': None,
             '_updated': {'at': datetime(2018, 1, 2, 3, 4, 5), 'fields': ['name']}
            }, self.db['resource_company'].find_one({'_id': company_id}))

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
        self.assertEquals("http://server/api/companies/%s/periods/%s/financial" % (company_id, period_id), period['financial'])

        financial = self.api.get('companies/%s/periods/%s/financial' % (company_id, period_id))
        self.assertEquals(100, financial['totalAssets'])

        # assert financial is added to root collection
        financials = self.api.get('financials')
        self.assertEquals(1, len(financials))
        self.assertEquals(100, financials[0]['totalAssets'])

        # assert aggregates
        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(100, company['totalTotalAssets'])

    def test_post_embedded_resources_separately(self):
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        period_id = self.api.post('companies/%s/periods' % (company_id,),
            {'year': 2017, 'period': 'YE'})
        financial_id = self.api.post('companies/%s/periods/%s/financial' % (company_id, period_id,), {'totalAssets': 100})

        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))

        self.assertEquals(2017, period['year'])
        self.assertEquals('YE', period['period'])
        self.assertEquals("http://server/api/companies/%s/periods/%s/financial" % (company_id, period_id), period['financial'])

        financial = self.api.get('companies/%s/periods/%s/financial' % (company_id, period_id))
        self.assertEquals(100, financial['totalAssets'])
        self.assertEquals(str(financial_id), financial['id'])

    def test_link_embedded_resources(self):
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        period_id = self.api.post('companies/%s/periods' % (company_id,),
            {'year': 2017, 'period': 'YE'})
        # add root resource
        financial_id = self.api.post('financials', {'totalAssets': 80})

        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))
        self.assertEquals(None, period['financial'])

        # link the resource
        self.api.post('companies/%s/periods/%s/financial' % (company_id, period_id), {'id': financial_id})

        # shortcut id set on resourcelink field
        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))
        self.assertEquals("http://server/api/companies/%s/periods/%s/financial" % (company_id, period_id), period['financial'])

        # parent entry set on target
        db_financial = self.db['resource_financial'].find_one({'_id': ObjectId(financial_id)})

        # aggregates still work
        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(80, company['totalTotalAssets'])
        self.assertEquals(80, company['totalFinancialsAssets'])

    def test_delete_embedded_link(self):
        company_id = self.api.post('companies', {'name': 'Neds Fries'})
        period_id = self.api.post('companies/%s/periods' % (company_id,),
            {'year': 2017, 'period': 'YE'})
        # add root resource
        financial_id = self.api.post('financials', {'totalAssets': 80})
        # link the resource
        self.api.post('companies/%s/periods/%s/financial' % (company_id, period_id), {'id': financial_id})

        # remove the link
        self.api.unlink('companies/%s/periods/%s/financial' % (company_id, period_id))

        # shortcut id null on resourcelink field
        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))
        self.assertEquals(None, period['financial'])

        # aggregates still work
        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(None, company['totalTotalAssets'])

    def test_replace_embedded_resource(self):
        # shortcut id replaces on resourcelink field
        # parent entry removed from original target
        # parent entry added to new target
        # aggregates still work
        pass

    def test_unlink_linked_resource(self):
        # assert ObjectId link is None
        pass

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

    def test_resource_equality_based_on_path_and_filters(self):
        company_1 = self.api.post('companies', {'name': 'C1'})
        company_2 = self.api.post('companies', {'name': 'C2'})

        resource_1 = self.api.build_resource('companies/%s' % (company_1,))
        resource_2 = self.api.build_resource('companies/%s' % (company_2,))

        self.assertFalse(resource_1 == resource_2)

        resource_3 = self.api.build_resource('companies/%s/periods' % (company_1,))
        resource_4 = self.api.build_resource('companies/%s/periods' % (company_2,))

        self.assertFalse(resource_3 == resource_4)

    def test_resolve_spec(self):
        pass

    def test_remove_updater_on_success(self):
        pass

    def test_remove_updater_on_error(self):
        pass

    def test_validate_fields(self):
        try:
            self.api.post('companies', {'name': 1.1})
        except HTTPError as hte:
            self.assertEquals(400, hte.status_code)

        try:
            self.api.post('companies', {'nonexistant': 'C1'})
        except HTTPError as hte:
            self.assertEquals(400, hte.status_code)

        try:
            self.api.post('companies', {'totalTotalAssets': 100})
        except HTTPError as hte:
            self.assertEquals(400, hte.status_code)

    def test_unlink_resource_from_root_collection(self):
        pass
