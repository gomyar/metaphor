
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
        self.schema.register_function('previous_year_period', self._previous_year_period)

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)

        self.company_spec.add_field("name", FieldSpec("str"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field("latestPeriod", CalcSpec('latest_period(self.periods.period_index)', 'period'))
        self.company_spec.add_field("latestPeriod_year", CalcSpec('self.latestPeriod.year', 'int'))
#        self.company_spec.add_field("latestPeriod_previous_year", CalcSpec('self.latestPeriod.previousYear.year', 'int'))

        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("period", FieldSpec("float"))
        self.period_spec.add_field("period_index", CalcSpec("self.year + self.period / 4", "float"))

#        self.period_spec.add_field("previousYear", CalcSpec('previous_year_period(self)', 'period'))

        self.schema.add_root('companies', CollectionSpec('company'))

        self.api = MongoApi('server', self.schema, self.db)

    def _latest_period_func(self, period_field_agg):
        period_resources = period_field_agg._parent.load_collection_data()
        if period_resources.count():
            max_period = max(period_resources, key=lambda p: p.get('period_index'))
            return max_period['_id']
        else:
            return None

    def _previous_year_period(self, period):
        if not period.data.get('period_index'):
            return None
        period_index = period.data['period_index']

        previous = self._lookup_period(period, period_index - 1.0)
        return previous['_id'] if previous else None

    def _previous_quarter_period(self, period):
        if not period.data.get('period_index'):
            return None
        period_index = period.data['period_index']

        previous = self._lookup_period(period, period_index - 0.25)
        return previous['_id'] if previous else None

    def _lookup_period(self, period, period_index):
        owners = [owner for owner in period.data['_owners'] if owner['owner_spec'] =='company']
        if owners:
            company_id = owners[0]['owner_id']

            found = self.schema.db['resource_period'].find_one({
                'period_index': period_index,
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

    def _test_link_target_change(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        period_1 = self.api.post('companies/%s/periods' % (company_id,), {'year': 2017, 'period': 4})
        period_2 = self.api.post('companies/%s/periods' % (company_id,), {'year': 2016, 'period': 4})

        self.assertEquals(2017, self.api.get('companies/%s' % (company_id,))['latestPeriod_year'])

        self.api.patch('companies/%s/periods/%s' % (company_id, period_1), {'year': 2018})

        self.assertEquals(2018, self.api.get('companies/%s' % (company_id,))['latestPeriod_year'])
