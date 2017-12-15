
import unittest

from mongomock import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi


class DependencyTest(unittest.TestCase):
    def setUp(self):
        self.db = MongoClient().db
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

        self.financial_spec.add_field(
            "netProfit",
            CalcSpec('self.grossProfit / 10'))

        self.sector_spec.add_field("name", FieldSpec("string"))
        self.sector_spec.add_field("companies", CollectionSpec("company"))
        self.sector_spec.add_field("totalCompanies", CalcSpec("len(companies)"))
        self.sector_spec.add_field("allCompanies", CalcSpec("companies"))
        self.sector_spec.add_field("allPeriods", CalcSpec("companies.periods"))
        self.sector_spec.add_field("average", CalcSpec("average(self.companies.periods.profitRatio)"))

        self.schema.add_root('companies', CollectionSpec('company'))
        self.schema.add_root('sectors', CollectionSpec('sector'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_calcspec_deps(self):
        profit_ratio_spec = self.period_spec.fields['profitRatio']
        gross_profit_spec = self.financial_spec.fields['grossProfit']
        total_companies_spec = self.sector_spec.fields['totalCompanies']
        all_companies_spec = self.sector_spec.fields['allCompanies']
        all_periods_spec = self.sector_spec.fields['allPeriods']
        average_spec = self.sector_spec.fields['average']

        gross_profit_spec = self.financial_spec.fields['grossProfit']
        net_profit_spec = self.financial_spec.fields['netProfit']

        profit_ratio_deps = profit_ratio_spec.dependencies()
        self.assertEquals(set([gross_profit_spec, net_profit_spec]), profit_ratio_deps)

        total_sales_spec = self.financial_spec.fields['totalSales']
        total_expenses_spec = self.financial_spec.fields['totalExpenses']

        gross_profit_deps = gross_profit_spec.dependencies()
        self.assertEquals(set([total_sales_spec, total_expenses_spec]), gross_profit_deps)

        company_collection_spec = self.schema.specs['root'].fields['companies']
        period_collection_spec = self.schema.specs['root'].fields['companies']

        self.assertEquals(set([company_collection_spec]), total_companies_spec.dependencies())
        self.assertEquals(set([company_collection_spec]), all_companies_spec.dependencies())
        self.assertEquals(set([company_collection_spec, period_collection_spec]), all_periods_spec.dependencies())
        self.assertEquals(set([company_collection_spec, period_collection_spec, profit_ratio_spec]), average_spec.dependencies())

    def test_check_two_collections_of_same_things(self):
        # companies
        # orgs/companies
        # portfolio/companies
        # dont get mixed up somehow?
        pass

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
