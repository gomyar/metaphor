
import unittest

from mongomock import Connection

from turtleapi import ResourceSpec, FieldSpec, CollectionSpec
from turtleapi import Schema
from turtleapi import MongoApi



class SpikeTest(unittest.TestCase):
    def setUp(self):
        self.db = Connection().db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec(self.period_spec))

        self.period_spec.add_field("period", FieldSpec("string"))
        self.period_spec.add_field("year", FieldSpec("int"))

        self.schema.add_root('companies', CollectionSpec(self.company_spec))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_find(self):
        period_id = self.db['resource_period'].insert(
            {'year': 2017, 'period': 'YE'})
        company_id = self.db['resource_company'].insert(
            {'name': 'Bobs Burgers', 'periods': [period_id]})

        company = self.api.get('companies/%s' % (company_id,))
        period = self.api.get('companies/%s/periods/%s' % (company_id, period_id))

        self.assertEquals(2017, period['year'])

    def _test_add_data(self):
        company_id = self.api.create('companies', dict(name='Bobs Burgers'))
        period_id = self.api.create('companies/%s/periods' % (company_id,),
                                    dict(year=2017, period='YE'))

        company = self.db['companies'].find_one({'_id': company_id})
        self.assertEquals('Bobs Burgers', company['name'])
        period = self.db['periods'].find_one({'_id': period_id})
        self.assertEquals('YE', period['period'])
        self.assertEquals(2017, period['year'])

        api_period = self.api.get("periods/%s" % (period_id,))
        self.assertEquals('YE', api_period['period'])
        self.assertEquals(2017, api_period['year'])
        self.assertEquals({
            '_id': company_id,
            'name': 'Bobs Burgers',
            'periods': 'http://server/companies/%s/periods' % (company_id,),
        }, self.api.get("companies/%s" % (company_id,)))
