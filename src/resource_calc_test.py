
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
        self.schema.register_function('previous_year_period', self._previous_year_period)
        self.schema.register_function('previous_quarter_period', self._previous_quarter_period)

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)

        self.company_spec.add_field("name", FieldSpec("str"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field("latestPeriod", CalcSpec('latest_period(self.periods)', 'period'))
        self.company_spec.add_field("yearPeriods", CalcSpec('year_periods(self.periods)', 'period', is_collection=True))
        self.company_spec.add_field("latestPeriod_year", CalcSpec('self.latestPeriod.year', 'int'))

        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("period", FieldSpec("str"))
        self.period_spec.add_field("score", FieldSpec("int"))

        self.period_spec.add_field("previousYear", CalcSpec('previous_year_period(self)', 'period'))
        self.period_spec.add_field("previousQuarter", CalcSpec('previous_quarter_period(self)', 'period'))

        self.period_spec.add_field("yearlyDelta_score", CalcSpec('self.score - self.previousYear.score', 'int'))
        self.period_spec.add_field("quarterlyDelta_score", CalcSpec('self.score - self.previousQuarter.score', 'int'))

        self.schema.add_root('companies', CollectionSpec('company'))

        self.api = MongoApi('server', self.schema, self.db)

    def _latest_period_func(self, periods):
        period_resources = periods.load_collection_data()
        if period_resources.count():
            max_period = max(period_resources, key=lambda p: p['year'])
            return max_period['_id']
        else:
            return None

    def _previous_year_period(self, period):
        if not period.data.get('year') or not period.data.get('period'):
            return None
        year = period.data['year']
        fperiod = period.data['period']

        previous = self._lookup_period(period, year - 1, fperiod)
        return previous['_id'] if previous else None

    def _previous_quarter_period(self, period):
        if not period.data.get('year') or not period.data.get('period'):
            return None
        year = period.data['year']
        fperiod = period.data['period']

        PERIOD_KEYS = ['Q1', 'Q2', 'Q3', 'YE']
        deltaPeriod = PERIOD_KEYS[PERIOD_KEYS.index(fperiod) - 1]

        if fperiod == 'Q1':
            year -= 1
        previous = self._lookup_period(period, year, deltaPeriod)
        return previous['_id'] if previous else previous

    def _lookup_period(self, period, year, fperiod):
        owners = [owner for owner in period.data['_owners'] if owner['owner_spec'] =='company']
        if owners:
            company_id = owners[0]['owner_id']

            found = self.schema.db['resource_period'].find_one({
                'year': year,
                'period': fperiod,
                '_owners.owner_spec': 'company',
                '_owners.owner_id': company_id,
            })
            return found
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

    def test_multiple_calcs(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        period_1_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2018, 'period': 'Q1', 'score': 50})
        period_2_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'YE', 'score': 45})
        period_3_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'Q3', 'score': 38})
        period_4_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'Q2', 'score': 43})
        period_5_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 'Q1', 'score': 40})
        period_6_id = self.api.post('companies/%s/periods' % (company_id,), {'year': 2016, 'period': 'YE', 'score': 55})

        period_1 = self.api.get('companies/%s/periods/%s' % (company_id, period_1_id))
        period_2 = self.api.get('companies/%s/periods/%s' % (company_id, period_2_id))
        period_3 = self.api.get('companies/%s/periods/%s' % (company_id, period_3_id))
        period_4 = self.api.get('companies/%s/periods/%s' % (company_id, period_4_id))
        period_5 = self.api.get('companies/%s/periods/%s' % (company_id, period_5_id))
        period_6 = self.api.get('companies/%s/periods/%s' % (company_id, period_6_id))

        self.assertEquals('http://server/api/companies/%s/periods/%s/previousQuarter' % (company_id, period_1_id), period_1['previousQuarter'])
        self.assertEquals('http://server/api/companies/%s/periods/%s/previousYear' % (company_id, period_1_id), period_1['previousYear'])
        self.assertEquals(10, period_1['yearlyDelta_score'])
        self.assertEquals(5, period_1['quarterlyDelta_score'])

        self.assertEquals('http://server/api/companies/%s/periods/%s/previousQuarter' % (company_id, period_2_id), period_2['previousQuarter'])
        self.assertEquals('http://server/api/companies/%s/periods/%s/previousYear' % (company_id, period_2_id), period_2['previousYear'])
        self.assertEquals(-10, period_2['yearlyDelta_score'])
        self.assertEquals(7, period_2['quarterlyDelta_score'])

        self.assertEquals('http://server/api/companies/%s/periods/%s/previousQuarter' % (company_id, period_3_id), period_3['previousQuarter'])
        self.assertEquals(None, period_3['previousYear'])
        self.assertEquals(None, period_3['yearlyDelta_score'])
        self.assertEquals(-5, period_3['quarterlyDelta_score'])

        self.assertEquals('http://server/api/companies/%s/periods/%s/previousQuarter' % (company_id, period_4_id), period_4['previousQuarter'])
        self.assertEquals(None, period_4['previousYear'])
        self.assertEquals(None, period_4['yearlyDelta_score'])
        self.assertEquals(3, period_4['quarterlyDelta_score'])

        self.assertEquals('http://server/api/companies/%s/periods/%s/previousQuarter' % (company_id, period_5_id), period_5['previousQuarter'])
        self.assertEquals(None, period_5['previousYear'])
        self.assertEquals(None, period_5['yearlyDelta_score'])
        self.assertEquals(-15, period_5['quarterlyDelta_score'])

        self.assertEquals(None, period_6['previousQuarter'])
        self.assertEquals(None, period_6['previousYear'])
        self.assertEquals(None, period_6['yearlyDelta_score'])
        self.assertEquals(None, period_6['quarterlyDelta_score'])

        # double check child resource
        previousYear = self.api.get('companies/%s/periods/%s/previousYear' % (company_id, period_1_id))
        self.assertEquals(2017, previousYear['year'])

        previousQuarter = self.api.get('companies/%s/periods/%s/previousQuarter' % (company_id, period_1_id))
        self.assertEquals(2017, previousQuarter['year'])
        self.assertEquals('YE', previousQuarter['period'])
        self.assertEquals('http://server/api/companies/%s/periods/%s/previousQuarter/previousQuarter' % (company_id, period_1_id), previousQuarter['previousQuarter'])

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
