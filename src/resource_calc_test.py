
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
        self.schema.register_function('year_periods', self._filter_year_periods)

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)

        self.company_spec.add_field("name", FieldSpec("str"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field(
            "latestPeriod",
            CalcSpec('latest_period(self.periods)', 'period'))
        self.company_spec.add_field("yearPeriods", CalcSpec('year_periods(self.periods)', 'period', is_collection=True))
        self.company_spec.add_field(
            "latestPeriod_year",
            CalcSpec('self.latestPeriod.year', 'int'))

        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("period", FieldSpec("str"))

        self.schema.add_root('companies', CollectionSpec('company'))

        self.api = MongoApi('server', self.schema, self.db)

    def _latest_period_func(self, periods):
        period_resources = periods.load_collection_data()
        if period_resources.count():
            max_period = max(period_resources, key=lambda p: p['year'])
            return max_period['_id']
        else:
            return None

    def _filter_year_periods(self, periods):
        period_data = [p for p in periods.load_collection_data()]
        yearly_periods = [p for p in period_data if p['period'] == 'YE']
        sorted_periods = sorted(yearly_periods, key=lambda p: p['year'])
        return [p['_id'] for p in sorted_periods]

    def test_function_returns_resource(self):
        company_id = self.api.post('companies', {'name': 'Bob'})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(None, company['latestPeriod'])

        period_2015_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2015})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals('http://server/api/companies/%s/latestPeriod' % (company_id,), company['latestPeriod'])
        latest = self.api.get('companies/%s/latestPeriod' % (company_id,))
        self.assertEquals(str(period_2015_id), latest['id'])
        self.assertEquals(2015, latest['year'])

        period_2016_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2016})

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals('http://server/api/companies/%s/latestPeriod' % (company_id,), company['latestPeriod'])
        latest = self.api.get('companies/%s/latestPeriod' % (company_id,))
        self.assertEquals(str(period_2016_id), latest['id'])
        self.assertEquals(2016, latest['year'])

    def test_function_works_with_delete(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        period_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2015})

        self.api.unlink('companies/%s/periods/%s' % (company_id, period_id))

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(None, company['latestPeriod'])

    def test_link_collection_calc(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2018, 'period': 'Q1'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'YE'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'Q3'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'Q2'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'Q1'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2016, 'period': 'YE'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2016, 'period': 'Q1'})

        company = self.api.get("companies/%s" % (company_id,))
        self.assertEquals("http://server/api/companies/%s/yearPeriods" % (company_id,), company['yearPeriods'])

        year_periods = self.api.get("companies/%s/yearPeriods" % (company_id,))
        self.assertEquals(2, len(year_periods))
        self.assertEquals('YE', year_periods[0]['period'])
        self.assertEquals(2017, year_periods[0]['year'])
        self.assertEquals('YE', year_periods[1]['period'])
        self.assertEquals(2016, year_periods[1]['year'])

    def test_aggregate_link_collection_calc_field(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'YE'})
        self.api.post('companies/%s/periods' % (company_id,), {'year': 2016, 'period': 'YE'})

        company_id_2 = self.api.post('companies', {'name': 'Ned'})
        self.api.post('companies/%s/periods' % (company_id_2,), {'year': 2015, 'period': 'YE'})

        agg = self.api.get("companies/yearPeriods")
        self.assertEquals(2017, agg[0]['year'])
        self.assertEquals('YE', agg[0]['period'])
        self.assertEquals(2016, agg[1]['year'])
        self.assertEquals('YE', agg[1]['period'])
        self.assertEquals(2015, agg[2]['year'])
        self.assertEquals('YE', agg[2]['period'])
