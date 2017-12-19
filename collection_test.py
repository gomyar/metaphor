
import unittest

from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class CollectionTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')
        self.sector_spec = ResourceSpec('sector')
        self.ratios_spec = ResourceSpec('ratios')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)
        self.schema.add_resource_spec(self.sector_spec)
        self.schema.add_resource_spec(self.ratios_spec)

        self.ratios_spec.add_field("score", FieldSpec("int"))

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field("assets", FieldSpec('int'))

        self.company_spec.add_field("averageScore", CalcSpec("average(self.periods.ratios.score)"))

        self.period_spec.add_field("period", FieldSpec("string"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("totalIncome", FieldSpec("int"))

        self.period_spec.add_field("ratios", ResourceLinkSpec("ratios"))

        self.sector_spec.add_field("name", FieldSpec("string"))
        self.sector_spec.add_field('companies', CollectionSpec('company'))
        self.sector_spec.add_field("averageCompanyAssets", CalcSpec("average(self.companies.assets)"))
        self.sector_spec.add_field("averageCompanyIncome", CalcSpec("average(self.companies.periods.totalIncome)"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('sectors', CollectionSpec('sector'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_collection_average(self):
        company_id_1 = self.api.post('companies', dict(name='Bobs Burgers', assets=50))
        company_id_2 = self.api.post('companies', dict(name='Neds Fries', assets=100))

        sector_id = self.api.post('sectors', dict(name='Marketting'))

        self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2017, period='YE', totalIncome=100))
        self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2016, period='YE', totalIncome=120))
        self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2015, period='YE', totalIncome=140))

        self.api.post('companies/%s/periods' % (company_id_2,), dict(year=2017, period='YE', totalIncome=180))
        self.api.post('companies/%s/periods' % (company_id_2,), dict(year=2016, period='YE', totalIncome=200))
        self.api.post('companies/%s/periods' % (company_id_2,), dict(year=2015, period='YE', totalIncome=220))

        # add company to sector
        self.api.post('sectors/%s/companies' % (sector_id,), {'id': company_id_1})

        sector = self.api.get('sectors/%s' % (sector_id,))
        self.assertEquals(50, sector['averageCompanyAssets'])
        self.assertEquals(120, sector['averageCompanyIncome'])

        # add other company to sector
        self.api.post('sectors/%s/companies' % (sector_id,), {'id': company_id_2})

        sector = self.api.get('sectors/%s' % (sector_id,))
        self.assertEquals(75, sector['averageCompanyAssets'])
        self.assertEquals(160, sector['averageCompanyIncome'])

        # update on delete
        self.api.unlink('sectors/%s/companies/%s' % (sector_id, company_id_2,))

        sector = self.api.get('sectors/%s' % (sector_id,))
        self.assertEquals(50, sector['averageCompanyAssets'])
        self.assertEquals(120, sector['averageCompanyIncome'])

    def test_update_on_delete(self):
        pass

    def test_filter_by_value(self):
        ''' companies[name='bob'] '''

    def test_filter_by_reference(self):
        ''' companies[sector=sectors/3] ? '''

    def test_aggregate_resource_link(self):
        company_id_1 = self.api.post('companies', dict(name='Bobs Burgers', assets=50))

        period_1 = self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2017, period='YE', totalIncome=100))
        period_2 = self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2016, period='YE', totalIncome=120))
        period_3 = self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2015, period='YE', totalIncome=140))

        self.api.post('companies/%s/periods/%s/ratios' % (company_id_1, period_1), dict({'score': 10}))
        self.api.post('companies/%s/periods/%s/ratios' % (company_id_1, period_2), dict({'score': 30}))
        self.api.post('companies/%s/periods/%s/ratios' % (company_id_1, period_3), dict({'score': 40}))

        # cannot aggregate periods.financial
        company = self.api.get('companies/%s' % (company_id_1,))

        self.assertEquals(26, company['averageScore'])

    def test_aggregate_calc(self):
        # cannot aggregate periods.averageAssets
        pass
