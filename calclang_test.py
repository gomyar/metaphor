
import unittest

from mongomock import Connection

from calclang import parser
from metaphor.schema import Schema
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec, CalcSpec
from metaphor.api import MongoApi
from metaphor.resource import Resource


class CalcLangTest(unittest.TestCase):
    def setUp(self):
        self.db = Connection().db
        self.schema = Schema(self.db, '0.1')

        self.db = Connection().db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')

        self.schema.add_resource_spec(self.company_spec)
        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("totalAssets", FieldSpec('int'))
        self.company_spec.add_field("totalLiabilities", FieldSpec('int'))
        self.company_spec.add_field("totalCurrentAssets",
                                    CalcSpec('self.totalAssets'))
        self.company_spec.add_field(
            "grossProfit",
            CalcSpec('self.totalAssets - self.totalLiabilities'))

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

    def test_parse_resource(self):
        resource = Resource('company', self.company_spec,
                            {'name': 'Bobs Burgers'})
        exp_tree = parser.parse(self.schema, 'self.name')
        self.assertEquals('Bobs Burgers', exp_tree.calculate(resource))

    def test_expr_fields(self):
        company_id = self.api.post(
            'companies', dict(name='Bobs Burgers', totalAssets=100,
                              totalLiabilities=80))

        company = self.api.get('companies/%s' % (company_id,))
        self.assertEquals(100, company['totalAssets'])
        self.assertEquals(80, company['totalLiabilities'])
        self.assertEquals(100, company['totalCurrentAssets'])
        self.assertEquals(20, company['grossProfit'])
