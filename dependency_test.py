
import unittest

from mongomock import Connection

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class DependencyTest(unittest.TestCase):
    def setUp(self):
        self.db = Connection().db
        self.schema = Schema(self.db, '0.1')

        self.company_spec = ResourceSpec('company')
        self.period_spec = ResourceSpec('period')
        self.financial_spec = ResourceSpec('financial')
        self.fhr_spec = ResourceSpec('fhr')
        self.sector_spec = ResourceSpec('sector')

        self.schema.add_resource_spec(self.company_spec)
        self.schema.add_resource_spec(self.period_spec)
        self.schema.add_resource_spec(self.financial_spec)
        self.schema.add_resource_spec(self.fhr_spec)
        self.schema.add_resource_spec(self.sector_spec)

        self.company_spec.add_field("name", FieldSpec("string"))
        self.company_spec.add_field("periods", CollectionSpec('period'))

        self.period_spec.add_field("period", FieldSpec("string"))
        self.period_spec.add_field("year", FieldSpec("int"))
        self.period_spec.add_field("financial", ResourceLinkSpec("financial"))

        self.period_spec.add_field("profitRatio", CalcSpec("self.financial.grossProfit / self.financial.netProfit"))

        self.financial_spec.add_field("totalSales", FieldSpec("int"))
        self.financial_spec.add_field("totalExpenses", FieldSpec("int"))

        self.financial_spec.add_field(
            "grossProfit",
            CalcSpec('self.totalSales - self.totalExpenses'))

        self.sector_spec.add_field("name", FieldSpec("string"))
        self.sector_spec.add_field("companies", CollectionSpec("company"))
        self.sector_spec.add_field("average", CalcSpec("average(self.companies.period.profitRatio)"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('sectors', CollectionSpec('sector'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_resource_spec_dependencies(self):
        self.assertEquals({'period'}, self.company_spec.dependencies())
        self.assertEquals({'financial'}, self.period_spec.dependencies())
        self.assertEquals({'company', 'sector'}, self.schema.root_spec.dependencies())

    def test_build_schema_dependency_tree(self):
        self.assertEquals(['company', 'fhr', 'financial', 'period', 'root', 'sector'], self.schema.all_types())

        self.assertEquals({
            'company': set(['period']),
            'period': set(['financial']),
            'financial': set(),
            'fhr': set(),
            'root': set(['company', 'sector']),
            'sector': set(['company']),
        }, self.schema.build_dependency_tree())

    def test_mark_downstream_dependencies_dirty_on_change(self):
        # change resource

        # assert dirty flag on all dependents

        # run updater for one resource

        # assert dependent resources are marked as dirty
        pass
