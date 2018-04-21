
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
        self.company_spec.add_field("periods", CollectionSpec('period'))
        self.company_spec.add_field("public", FieldSpec("bool"))
        self.company_spec.add_field("totalTotalAssets", CalcSpec("sum(companies.periods.financial.totalAssets)", 'int'))
        self.company_spec.add_field("totalFinancialsAssets", CalcSpec('sum(financials.totalAssets)', 'int'))

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
        }, self.schema.dependency_tree())
