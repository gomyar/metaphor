
import unittest

from pymongo import MongoClient

from metaphor.calclang import parser
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource
from metaphor.resource import AggregateField, CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.sector_spec = ResourceSpec('sector')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.sector_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("assets", FieldSpec('int'))

        self.sector_spec.add_field("name", FieldSpec("string"))
        self.sector_spec.add_field("companies", CollectionSpec("company"))
        self.average_assets_calc = CalcSpec("average(self.companies.assets)")
        self.sector_spec.add_field("averageAssets", self.average_assets_calc)

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('sectors', CollectionSpec('sector'))

        self.api = MongoApi('http://server', self.schema, self.db)

        self.sector_1 = self.api.post('sectors', {'name': 'Marketting'})

        self.company_1 = self.api.post('companies', {'name': 'Bob1', 'totalAssets': 10})
        self.company_2 = self.api.post('companies', {'name': 'Bob2', 'totalAssets': 20})

        self.db['metaphor_updates'].drop()

    def test_updates_pieces(self):
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'id': self.company_1})

        company = self.api.build_resource('companies/%s' % (self.company_1,))
        found = self.schema.find_affected_calcs_for_resource(company)

        self.assertEquals(set([
            (self.average_assets_calc, 'self.companies.assets', 'self.companies')
        ]), found)

        resource_ids = self.schema.find_altered_resource_ids(found, company)
        self.assertEquals(set([
            ('sector', 'averageAssets', (self.sector_1,))
        ]), resource_ids)

    def test_updates_save_changed_resources(self):
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'id': self.company_1})

        self.assertEquals([
            {'spec': 'sector',
             'field_name': 'averageAssets',
             'resource_ids': [self.sector_1]}
        ], [updated for updated in self.db['metaphor_updates'].find({}, {'_id': 0})])
