
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId
from requests.exceptions import HTTPError

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, CalcSpec
from metaphor.resource import LinkCollectionSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi
from metaphor.schema_factory import SchemaFactory

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

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)
        self.schema.add_resource_spec(self.portfolio_spec)
        self.schema.add_resource_spec(self.financial_spec)

        self.company_spec.add_field("name", FieldSpec("str"))
        self.company_spec.add_field("surname", FieldSpec("str"))
        self.company_spec.add_field("full_name", CalcSpec("self.name + self.surname", "str"))
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field("public", FieldSpec("bool"))
        self.company_spec.add_field("totalTotalAssets", CalcSpec("sum(companies.periods.financial.totalAssets)", 'int'))
        self.company_spec.add_field("totalFinancialsAssets", CalcSpec('sum(financials.totalAssets)', 'int'))
        self.company_spec.add_field("address_line_1", FieldSpec("str"))
        self.company_spec.add_field("address_line_2", FieldSpec("str"))
        self.company_spec.add_field("full_address", CalcSpec("self.address_line_1 + self.address_line_2", "str"))

        self.period_spec.add_field("period", FieldSpec("str"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("financial", ResourceLinkSpec("financial"))
        self.period_spec.add_field("companyName", CalcSpec("self.link_company_periods.name", 'str'))

        self.financial_spec.add_field("totalAssets", FieldSpec("int"))

        self.portfolio_spec.add_field("name", FieldSpec("str"))
        self.portfolio_spec.add_field("companies", LinkCollectionSpec('company'))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('portfolios', CollectionSpec('portfolio'))
        self.schema.add_root('financials', CollectionSpec('financial'))

        self.api = MongoApi('server', self.schema, self.db)

    def test_dependencies(self):
        self.assertEquals({
            'company.totalTotalAssets': set(['financial.totalAssets']),
            'company.totalFinancialsAssets': set(['financial.totalAssets']),
            'period.companyName': set(['company.name']),
            'company.full_address': set(['company.address_line_1',
                                         'company.address_line_2']),
            'company.full_name': set(['company.name', 'company.surname']),
        }, self.schema.dependency_tree())

    def test_field_dependencies_fields(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        company = self.api.build_resource('companies/%s' % (company_id,))

        expected = set([
            (self.company_spec.fields['full_address'], 'self.address_line_1', 'self')
        ])
        self.assertEquals(expected, company.field_dependencies(['address_line_1']))

        expected = set([
            (self.company_spec.fields['full_address'], 'self.address_line_1', 'self'),
            (self.company_spec.fields['full_name'], 'self.name', 'self'),
            (self.period_spec.fields['companyName'], 'self.link_company_periods.name', 'self.link_company_periods'),
        ])
        self.assertEquals(expected, company.field_dependencies(['address_line_1', 'name']))

    def test_self_dependencies(self):
        company_id = self.api.post('companies', {'name': 'Bob'})
        company = self.api.build_resource('companies/%s' % (company_id,))

        expected = set([
            (self.company_spec.fields['full_address'], 'self.address_line_1', 'self'),
            (self.company_spec.fields['full_name'], 'self.name', 'self'),
        ])
        self.assertEquals(expected, company.local_field_dependencies(['address_line_1', 'name']))


        expected = set([
            (self.period_spec.fields['companyName'], 'self.link_company_periods.name', 'self.link_company_periods'),
        ])
        self.assertEquals(expected, company.foreign_field_dependencies(['address_line_1', 'name']))

    def test_update_mongo_once_then_construct_updater(self):
        # send > 1 fields to be updated

        # assert single update sent to mongo

        # assert updater(s) refers to dependencies only
        pass
