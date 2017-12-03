
import unittest

from mongomock import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class CollectionTest(unittest.TestCase):
    def setUp(self):
        self.db = MongoClient().db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')
        self.sector_spec = ResourceSpec('sector')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)
        self.schema.add_resource_spec(self.sector_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field("assets", FieldSpec('int'))

        self.period_spec.add_field("period", FieldSpec("string"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("totalIncome", FieldSpec("int"))

        self.sector_spec.add_field("averageCompanyAssets", CalcSpec("average(companies.assets)"))
        self.sector_spec.add_field("averageCompanyIncome", CalcSpec("average(companies.periods.totalIncome)"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('sectors', CollectionSpec('sector'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_collection_average(self):
        company_id_1 = self.api.post('companies', dict(name='Bobs Burgers', assets=50))
        company_id_2 = self.api.post('companies', dict(name='Neds Fries', assets=100))

        sector_id = self.api.post('sectors', dict(name='Marketting'))

        self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2017, period='YE', totalIncome=100))
        self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2017, period='YE', totalIncome=120))
        self.api.post('companies/%s/periods' % (company_id_1,), dict(year=2017, period='YE', totalIncome=160))

        self.api.post('companies/%s/periods' % (company_id_2,), dict(year=2017, period='YE', totalIncome=180))
        self.api.post('companies/%s/periods' % (company_id_2,), dict(year=2017, period='YE', totalIncome=200))
        self.api.post('companies/%s/periods' % (company_id_2,), dict(year=2017, period='YE', totalIncome=220))

        sector = self.api.get('sectors/%s' % (sector_id,))
        self.assertEquals(75, sector['averageCompanyAssets'])
        self.assertEquals(170, sector['averageCompanyIncome'])

    def test_filter_by_value(self):
        ''' companies[name='bob'] '''

    def test_filter_by_reference(self):
        ''' companies[sector=sectors/3] ? '''
