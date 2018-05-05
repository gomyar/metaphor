
import unittest

from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, LinkCollectionSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class CollectionTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.organization_spec = ResourceSpec('organization')
        self.company_spec = ResourceSpec('company')
        self.section_spec = ResourceSpec('section')

        self.schema.add_resource_spec(self.organization_spec)
        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.section_spec)

        self.organization_spec.add_field("name", FieldSpec("str"))
        self.organization_spec.add_field("companies", CollectionSpec("company"))

        self.section_spec.add_field("name", FieldSpec("str"))
        self.section_spec.add_field("companies", LinkCollectionSpec("company"))

        self.company_spec.add_field("name", FieldSpec("str"))
        self.company_spec.add_field("total", FieldSpec("int"))
        self.company_spec.add_field("average_parent", CalcSpec("average(self.link_section_companies.companies.total)", 'int'))

        self.schema.add_root('organizations', CollectionSpec('organization'))
        self.schema.add_root('sections', CollectionSpec('section'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_collection_encapsulation(self):
        self.org_1 = self.api.post('organizations', dict(name='Super Org'))

        company_id_1 = self.api.post('organizations/%s/companies' % (self.org_1), dict(name='Bobs Burgers'))
        company_id_2 = self.api.post('organizations/%s/companies' % (self.org_1), dict(name='Neds Fries'))

        section_id = self.api.post('sections', dict(name='Marketting'))

        self.api.post('sections/%s/companies' % (section_id,), {'id': company_id_1})

        self.assertEquals('Bobs Burgers', self.api.get('organizations/%s/companies/%s/' % (self.org_1, company_id_1,))['name'])
        self.assertEquals('Bobs Burgers', self.api.get('sections/%s/companies/%s/' % (section_id, company_id_1,))['name'])

    def test_delete_link(self):
        self.org_1 = self.api.post('organizations', dict(name='Super Org'))

        company_id_1 = self.api.post('organizations/%s/companies' % (self.org_1), dict(name='Bobs Burgers'))
        company_id_2 = self.api.post('organizations/%s/companies' % (self.org_1), dict(name='Neds Fries'))

        section_id = self.api.post('sections', dict(name='Marketting'))

        self.api.post('sections/%s/companies' % (section_id,), {'id': company_id_1})

        self.assertEquals(2, self.api.get('organizations/%s/companies' % (self.org_1,))['count'])
        self.assertEquals(1, self.api.get('sections/%s/companies' % (section_id,))['count'])

        # delete the linked version
        self.api.unlink('sections/%s/companies/%s' % (section_id, company_id_1))

        self.assertEquals(2, self.api.get('organizations/%s/companies' % (self.org_1,))['count'])
        self.assertEquals(0, self.api.get('sections/%s/companies' % (section_id,))['count'])

    def test_delete_original(self):
        self.org_1 = self.api.post('organizations', dict(name='Super Org'))

        company_id_1 = self.api.post('organizations/%s/companies' % (self.org_1), dict(name='Bobs Burgers'))

        section_id = self.api.post('sections', dict(name='Marketting'))

        self.api.post('sections/%s/companies' % (section_id,), {'id': company_id_1})

        self.assertEquals(1, self.api.get('organizations/%s/companies' % (self.org_1,))['count'])
        self.assertEquals(1, self.api.get('sections/%s/companies' % (section_id,))['count'])

        # delete the original version
        self.api.unlink('organizations/%s/companies/%s' % (self.org_1, company_id_1))

        self.assertEquals(0, self.api.get('organizations/%s/companies' % (self.org_1,))['count'])
        self.assertEquals(0, self.api.get('sections/%s/companies' % (section_id,))['count'])

        self.assertEquals(0, self.schema.db['resource_company'].count())

    def test_calc_update_on_delete_link(self):
        pass
