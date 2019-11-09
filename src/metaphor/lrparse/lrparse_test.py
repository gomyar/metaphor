
import unittest

from .lrparse import parse
from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.resource import Resource, Field
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, CalcSpec
from metaphor.resource import LinkCollectionSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi
from metaphor.schema_factory import SchemaFactory

from .lrparse import FieldRef, ResourceRef


class LRParseTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.employee_spec = ResourceSpec('employee')
        self.division_spec = ResourceSpec('division')

        self.schema.add_resource_spec(self.employee_spec)
        self.schema.add_resource_spec(self.division_spec)

        self.employee_spec.add_field("name", FieldSpec("str"))
        self.employee_spec.add_field("division", ResourceLinkSpec("division"))
        self.employee_spec.add_field("age", FieldSpec("int"))
        self.division_spec.add_field("type", FieldSpec("str"))
        self.division_spec.add_field("yearly_sales", FieldSpec("int"))

        self.schema.add_root('employees', CollectionSpec('employee'))
        self.schema.add_root('divisions', CollectionSpec('division'))

        self.api = MongoApi('http://server', self.schema, self.db)

    def test_basic(self):
        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)

        tree = parse("self.division.yearly_sales", resource.spec)
        self.assertEquals(FieldRef, type(tree))

        self.assertEquals(self.division_spec.fields['yearly_sales'],
                          tree.result_type(resource.spec))
        self.assertEquals(10, tree.calculate(resource).data)

    def test_even_basicer(self):
        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        resource = self.api.build_resource('employees/%s' % employee_id)

        tree = parse("self.age", resource.spec)
        self.assertEquals(FieldRef, type(tree))

        self.assertEquals(self.employee_spec.fields['age'],
                          tree.result_type(resource.spec))
        self.assertEquals(41,
                          tree.calculate(resource).data)

    def test_basic_link_follow(self):
        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)

        tree = parse("self.division", resource.spec)
        self.assertEquals(ResourceRef, type(tree))

        result_type = tree.result_type(resource.spec)

        self.assertEquals(ResourceLinkSpec,
                          type(result_type))
        self.assertEquals('division', result_type.name)

        calculated = tree.calculate(resource)
        self.assertEquals(Resource, type(calculated))
        self.assertEquals('division', calculated.spec.name)

    def test_aggregate_filtered(self):
        tree = parse("employees.division[type='sales'].yearly_sales", self.employee_spec)

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        division_id_1 = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 100})
        division_id_2 = self.api.post('divisions', {'type': 'marketting', 'yearly_sales': 20})

        self.api.post('employees/%s/division' % (employee_id_1,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_2,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_3,), {'id': division_id_2})

        resource = self.api.build_resource('employees/%s' % employee_id_1)

        results = tree.calculate(resource)
        self.assertEquals(1, len(results))
        self.assertTrue(type(results[0]) is Field)
        self.assertEquals(100, results[0].data)

        self.assertEquals(self.division_spec.fields['yearly_sales'],
                          tree.result_type(resource.spec))

    def test_list(self):
        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        division_id_1 = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 100})
        division_id_2 = self.api.post('divisions', {'type': 'marketting', 'yearly_sales': 20})

        self.api.post('employees/%s/division' % (employee_id_1,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_2,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_3,), {'id': division_id_2})

        resource = self.api.build_resource('employees/%s' % employee_id_1)

        tree = parse("employees.division", resource.spec)

        result = tree.calculate(resource)

        self.assertEquals(2, len(result))
        self.assertTrue(division_id_1 in [resource._id for resource in result])
        self.assertTrue(division_id_2 in [resource._id for resource in result])

    def test_link_list(self):
        pass

    def test_reverse_list(self):
        pass  # division.employee_employees

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        division_id_1 = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 100})
        division_id_2 = self.api.post('divisions', {'type': 'marketting', 'yearly_sales': 20})

        self.api.post('employees/%s/division' % (employee_id_1,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_2,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_3,), {'id': division_id_2})

        resource = self.api.build_resource('employees/%s' % employee_id_1)
        tree = parse("self.division.link_employee_division", resource.spec)
        result = tree.calculate(resource)

        self.assertEquals(2, len(result))
        self.assertEquals('ned', result[0].data['name'])
        self.assertEquals('bob', result[1].data['name'])

    def _test_calced_resource_ref(self):
        tree = parse("self.sailors")
        resource = {'sailors': {"pay": 13.0}}
        self.assertEquals({'pay': 13.0}, tree.calculate(resource))

    def _test_calculation(self):
        tree = parse("self.sailors.pay + (10.0 * (6 / 3) - 7)")
        resource = {'sailors': {"pay": 13.0}}
        self.assertEquals(26, tree.calculate(resource))

    def _test_filter(self):
        tree = parse("self.sailors[pay=13]")
        resource = {'sailors': [{"pay": 13.0}]}
        filtered = tree.calculate(resource)
        self.assertEquals([{"pay": 13.0}], filtered)
        self.assertEquals([{}], filtered._filter)

    def _test_filter_multi(self):
        tree = parse("self.sailors[pay=13&age=40]")
        resource = {'sailors': [{"pay": 13.0, "age": 40}]}
        filtered = tree.calculate(resource)
        self.assertEquals([{"pay": 13.0, "age": 40}], filtered)
        self.assertEquals([{}], filtered._filter)

    def test_spec_hier_error(self):
        employee_id = self.api.post('employees', {'name': 'sailor'})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)

        tree = parse("self.name", resource.spec)
        aggregation, spec, is_aggregate = tree.aggregation(resource)
        # unsure how this guy fits in exactly

        self.assertEquals([{'$match': {'_id': employee_id}}, {'$project': {'name': True}}], aggregation)

    def _test_condition_nofield(self):
        try:
            parse("employees[total_nonexistant>100]")
            self.fail("should have thrown")
        except Exception as e:
            self.assertEquals("", str(e))

    def _test_const_type(self):
        try:
            parse("employees[age>'str']")
            self.fail("should have thrown")
        except Exception as e:
            self.assertEquals("", str(e))

    def test_aggregation(self):
        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)
        tree = parse("employees[age>40].division[type='sales'].yearly_sales", resource.spec)
        agg_collection = tree.root_collection(resource)
        self.assertEquals(self.db.resource_employee.name, agg_collection.name)
        aggregation, spec, is_aggregate = tree.aggregation(resource)
        self.assertEquals([
            {'$match': {'age': {'$gt': 40}}},
            {'$lookup': {'as': '_field_division',
                        'foreignField': '_id',
                        'from': 'resource_division',
                        'localField': 'division'}},
            {'$group': {'_id': '$_field_division'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'type': {'$eq': 'sales'}}},
            {'$project': {'yearly_sales': True}}], aggregation)
        self.assertEquals(self.api.schema.specs['division'].fields['yearly_sales'],
                          spec)
        self.assertEquals(set(['employees.division.yearly_sales']), tree.all_resource_refs())

    def test_aggregation_self(self):
        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)
        tree = parse("self.division[type='sales'].yearly_sales", resource.spec)
        agg_collection = tree.root_collection(resource)
        self.assertEquals(self.db.resource_employee.name, agg_collection.name)
        aggregation, spec, is_aggregate = tree.aggregation(resource)
        self.assertEquals([
            {'$match': {'_id': employee_id}},
            {'$lookup': {'as': '_field_division',
                        'foreignField': '_id',
                        'from': 'resource_division',
                        'localField': 'division'}},
            {'$group': {'_id': '$_field_division'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'type': {'$eq': 'sales'}}},
            {'$project': {'yearly_sales': True}}], aggregation)
        self.assertEquals(self.api.schema.specs['division'].fields['yearly_sales'],
                          spec)
        self.assertEquals(set(['self.division.yearly_sales']), tree.all_resource_refs())


    def test_aggregates(self):
        # entities[name=self.other[resolve='me',first=True]]
        # parents[name='ned'].entities[averagePay>average(self.children[self.type='boss'].pay)]
        pass

    def test_calc_plus(self):
        self.employee_spec.add_field("salary", FieldSpec("int"))
        self.employee_spec.add_field("tax", FieldSpec("int"))

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'salary': 10, 'tax': 2})
        resource = self.api.build_resource('employees/%s' % employee_id_1)

        tree = parse("self.salary + self.tax", resource.spec)
        self.assertEquals(12, tree.calculate(resource))

        tree = parse("self.salary - self.tax", resource.spec)
        self.assertEquals(8, tree.calculate(resource))

        tree = parse("self.salary * self.tax", resource.spec)
        self.assertEquals(20, tree.calculate(resource))

        tree = parse("self.salary / self.tax", resource.spec)
        self.assertEquals(5, tree.calculate(resource))

    def test_calc_nones(self):
        self.employee_spec.add_field("salary", FieldSpec("int"))
        self.employee_spec.add_field("tax", FieldSpec("int"))

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'salary': 10, 'tax': None})
        resource = self.api.build_resource('employees/%s' % employee_id_1)

        tree = parse("self.salary + self.tax", resource.spec)
        self.assertEquals(10, tree.calculate(resource))

        tree = parse("self.salary - self.tax", resource.spec)
        self.assertEquals(10, tree.calculate(resource))

        tree = parse("self.salary * self.tax", resource.spec)
        self.assertEquals(None, tree.calculate(resource))

        # Going with None instead of NaN for now
        tree = parse("self.salary / self.tax", resource.spec)
        self.assertEquals(None, tree.calculate(resource))

    def test_function_call_param_list(self):
        self.employee_spec.add_field("salary", FieldSpec("float"))
        self.employee_spec.add_field("tax", FieldSpec("float"))

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'salary': 10.6, 'tax': 2.4})
        resource = self.api.build_resource('employees/%s' % employee_id_1)

        tree = parse("round(self.salary + self.tax, 2)", resource.spec)
        self.assertEquals(13, tree.calculate(resource))

    def test_function_basic(self):
        employee_id_1 = self.api.post('employees', {'name': 'ned'})
        resource = self.api.build_resource('employees/%s' % employee_id_1)

        tree = parse("round(14.14)")
        self.assertEquals(14, tree.calculate(resource))

    def test_function_call_param_list_multiple_calcs(self):
        self.employee_spec.add_field("salary", FieldSpec("float"))
        self.employee_spec.add_field("tax", FieldSpec("float"))

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'salary': 10.6, 'tax': 2.4})
        resource = self.api.build_resource('employees/%s' % employee_id_1)

        tree = parse("round(self.salary) + round(self.tax)", resource.spec)
        self.assertEquals(13, tree.calculate(resource))

    def test_function_within_a_function(self):
        self.employee_spec.add_field("salary", FieldSpec("float"))
        self.employee_spec.add_field("tax", FieldSpec("float"))

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'salary': 10.12345})
        resource = self.api.build_resource('employees/%s' % employee_id_1)

        tree = parse("round(round(self.salary, 4), 3)", resource.spec)
        self.assertEquals(10.123, tree.calculate(resource))

    def test_math_functions(self):
        self.employee_spec.add_field("salary", FieldSpec("float"))

        self.api.post('employees', {'name': 'ned', 'salary': 20})
        self.api.post('employees', {'name': 'bob', 'salary': 10})
        self.api.post('employees', {'name': 'bil', 'salary': 30})

        employees = self.api.build_resource('employees')

        # max
        tree = parse("max(employees.salary)", employees.spec)
        self.assertEquals(30, tree.calculate(employees))

        # min
        tree = parse("min(employees.salary)", employees.spec)
        self.assertEquals(10, tree.calculate(employees))

        # avg
        tree = parse("average(employees.salary)", employees.spec)
        self.assertEquals(20, tree.calculate(employees))

        # sum
        tree = parse("sum(employees.salary)", employees.spec)
        self.assertEquals(60, tree.calculate(employees))

    def test_extra_math(self):
        self.employee_spec.add_field("salary", FieldSpec("float"))

        self.api.post('employees', {'name': 'ned', 'salary': 20.1234})
        self.api.post('employees', {'name': 'ned', 'salary': 10.5678})
        self.api.post('employees', {'name': 'bil', 'salary': 30.7777})

        employees = self.api.build_resource('employees')

        tree = parse("round(sum(employees.salary), 2) + round(max(employees.salary))", employees.spec)
        self.assertEquals(92.47, tree.calculate(employees))


        tree = parse("round(sum(employees[name='ned'].salary), 2) + round(max(employees.salary))", employees.spec)
        self.assertEquals(61.69, tree.calculate(employees))

        tree = parse("round(sum(employees[name='ned'].salary), 2) + round(max(employees[name='ned'].salary))", employees.spec)
        self.assertEquals(50.69, tree.calculate(employees))

        self.assertEquals(set(['employees.salary']), tree.all_resource_refs())

        # filter nones
        # filter generic aggregates (filter(aggregate, name='paul', age>20))
        # space out range
        # cap (ceil, floor) aggregates
        # min max range
