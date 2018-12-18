
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.calclang import parser
from metaphor.schema import Schema
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec, CalcSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource, AggregateField
from metaphor.api import MongoApi
from metaphor.resource import Resource


class CalcLangTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.sector_spec = ResourceSpec('sector')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.sector_spec)

        self.company_spec.add_field("name", FieldSpec("str"))
        self.company_spec.add_field("totalAssets", FieldSpec('int'))
        self.company_spec.add_field("totalLiabilities", FieldSpec('int'))
        self.company_spec.add_field("totalCurrentAssets",
                                    CalcSpec('self.totalAssets', 'int'))
        self.company_spec.add_field(
            "grossProfit",
            CalcSpec('self.totalAssets - self.totalLiabilities', 'int'))

        self.sector_spec.add_field("name", FieldSpec("str"))
        self.sector_spec.add_field("companies", CollectionSpec("company"))
        self.sector_spec.add_field("averageLiabilities", CalcSpec("average(self.companies.totalLiabilities)", 'int'))

        self.schema.add_root('sectors', CollectionSpec('sector'))
        self.schema.add_root('companies', CollectionSpec('company'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_parse(self):
        exp_tree = parser.parse(self.schema, '1 + 1')
        self.assertEquals(1, exp_tree.exp.lhs.value)
        self.assertEquals(1, exp_tree.exp.rhs.value)

        self.assertEquals(2, exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, '5 - 2')
        self.assertEquals(3, exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, '2 * 6')
        self.assertEquals(12, exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, '14 / 2')
        self.assertEquals(7, exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, '(6 + (5 * 2) - 2) / 2')
        self.assertEquals(7, exp_tree.calculate(None))

#        exp_tree = parser.parse(self.schema, '-2')
#        self.assertEquals(-2, exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, '"tree"')
        self.assertEquals("tree", exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, '"tree"+"beard"')
        self.assertEquals("treebeard", exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, "'tree'+'beard'")
        self.assertEquals("treebeard", exp_tree.calculate(None))

        exp_tree = parser.parse(self.schema, "'tree'+\"beard\"")
        self.assertEquals("treebeard", exp_tree.calculate(None))

    def test_average_func(self):
        self.sector_1 = self.api.post('sectors', {'name': 'Marketting'})
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company1', 'totalAssets': 100})
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company2', 'totalAssets': 200})
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company3', 'totalAssets': 600})
        resource = self.api.build_resource("sectors/%s" % (self.sector_1,))

        exp_tree = parser.parse(self.schema, 'average(self.companies.totalAssets)')
        self.assertEquals(300, exp_tree.calculate(resource))

    def test_filter_func(self):
        self.sector_1 = self.api.post('sectors', {'name': 'Marketting'})
        c1 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company1', 'totalAssets': 100})
        c2 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company2', 'totalAssets': 200})
        c3 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company3', 'totalAssets': 600})
        resource = self.api.build_resource("sectors/%s" % (self.sector_1,))
        company1 = self.api.build_resource("companies/%s" % (c1,))
        company2 = self.api.build_resource("companies/%s" % (c2,))
        company3 = self.api.build_resource("companies/%s" % (c3,))

        exp_tree = parser.parse(self.schema, 'filter_one(companies, "totalAssets", 200)')
        self.assertEquals(company2._id, exp_tree.calculate(resource)._id)

        exp_tree = parser.parse(self.schema, 'filter_one(self.companies, "totalAssets", 100)')
        self.assertEquals(company1._id, exp_tree.calculate(resource)._id)

        exp_tree = parser.parse(self.schema, 'filter_one(companies,"totalAssets",10)')
        self.assertEquals(None, exp_tree.calculate(resource))

    def test_filter_expr(self):
        self.sector_1 = self.api.post('sectors', {'name': 'Marketting'})
        c1 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company1', 'totalAssets': 100})
        c2 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company2', 'totalAssets': 200})
        c3 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company3', 'totalAssets': 600})
        resource = self.api.build_resource("sectors/%s" % (self.sector_1,))
        company1 = self.api.build_resource("companies/%s" % (c1,))
        company2 = self.api.build_resource("companies/%s" % (c2,))
        company3 = self.api.build_resource("companies/%s" % (c3,))

        exp_tree = parser.parse(self.schema, 'companies[totalAssets=200]')
        self.assertEquals(company2._id, exp_tree.calculate(resource)._id)

        exp_tree = parser.parse(self.schema, 'self.companies[totalAssets=100]')
        self.assertEquals(company1._id, exp_tree.calculate(resource)._id)

        exp_tree = parser.parse(self.schema, 'companies[totalAssets=10]')
        self.assertEquals(None, exp_tree.calculate(resource))

    def test_average_func(self):
        self.sector_1 = self.api.post('sectors', {'name': 'Marketting'})
        c1 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company1', 'totalAssets': 100})
        c2 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company2', 'totalAssets': 200})
        c3 = self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company3', 'totalAssets': 600})
        resource = self.api.build_resource("sectors/%s" % (self.sector_1,))
        company1 = self.api.build_resource("companies/%s" % (c1,))
        company2 = self.api.build_resource("companies/%s" % (c2,))
        company3 = self.api.build_resource("companies/%s" % (c3,))

        exp_tree = parser.parse(self.schema, 'average(companies.totalAssets)')
        self.assertEquals(300, exp_tree.calculate(resource))

    def test_filter_max_min(self):
        c1 = self.api.post('companies', {'name': 'Company1', 'totalAssets': 100})
        c2 = self.api.post('companies', {'name': 'Company2', 'totalAssets': 200})
        c3 = self.api.post('companies', {'name': 'Company3', 'totalAssets': 600})
        company1 = self.api.build_resource("companies/%s" % (c1,))
        company3 = self.api.build_resource("companies/%s" % (c3,))

        exp_tree = parser.parse(self.schema, 'filter_max(companies,"totalAssets")')
        self.assertEquals(company3._id, exp_tree.calculate(company1)._id)

        exp_tree = parser.parse(self.schema, 'filter_min(companies,"totalAssets")')
        self.assertEquals(company1._id, exp_tree.calculate(company1)._id)

    def test_parse_resource(self):
        resource = Resource(self.schema.root, 'company', self.company_spec,
                            {'name': 'Bobs Burgers'})
        exp_tree = parser.parse(self.schema, 'self.name')
        self.assertEquals('Bobs Burgers', exp_tree.calculate(resource))

    def test_calc_function(self):
        self.sector_1 = self.api.post('sectors', {'name': 'Marketting'})
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company1', 'totalAssets': 110, 'totalLiabilities': 10})
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company2', 'totalAssets': 130, 'totalLiabilities': 20})
        self.api.post('sectors/%s/companies' % (self.sector_1,), {'name': 'Company3', 'totalAssets': 150, 'totalLiabilities': 30})
        resource = self.api.build_resource("sectors/%s" % (self.sector_1,))
        self.assertEquals(20, resource.data['averageLiabilities'])
        self.assertEquals("sectors/%s" % (self.sector_1,), resource.path)

    def test_expr_fields(self):
        company_id = self.api.post(
            'companies', dict(name='Bobs Burgers', totalAssets=100,
                              totalLiabilities=80))

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(100, company['totalAssets'])
        self.assertEquals(80, company['totalLiabilities'])
        self.assertEquals(100, company['totalCurrentAssets'])
        self.assertEquals(20, company['grossProfit'])

    def test_expr_fields_update(self):
        company_id = self.api.post(
            'companies', dict(name='Bobs Burgers', totalAssets=100,
                              totalLiabilities=80))
        self.api.patch('companies/%s' % (company_id,), {'totalAssets': 200})
        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(200, company['totalAssets'])
        self.assertEquals(80, company['totalLiabilities'])
        self.assertEquals(200, company['totalCurrentAssets'])
        self.assertEquals(120, company['grossProfit'])

    def test_lang_parser(self):
        # basic agg
        exp_tree = parser.parse(self.schema, 'sectors.companies')
        resource = exp_tree.exp.create_resource(self.schema.root)
        self.assertEquals(AggregateResource, type(resource))

        # agg field
        exp_tree = parser.parse(self.schema, 'sectors.companies.totalAssets')
        resource = exp_tree.exp.create_resource(self.schema.root)
        self.assertEquals(AggregateField, type(resource))

    def test_none_values(self):
        company_id = self.api.post(
            'companies', dict(name='Bobs Burgers', totalAssets=100))

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(100, company['totalAssets'])
        self.assertEquals(None, company['totalLiabilities'])
        self.assertEquals(100, company['totalCurrentAssets'])
        self.assertEquals(100, company['grossProfit'])
