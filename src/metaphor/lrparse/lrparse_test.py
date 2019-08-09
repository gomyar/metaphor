
import unittest

from .lrparse import parse
from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.resource import Resource
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, CalcSpec
from metaphor.resource import LinkCollectionSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi
from metaphor.schema_factory import SchemaFactory


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
        tree = parse("self.division.yearly_sales")

        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)

        self.assertEquals(self.division_spec.fields['yearly_sales'],
                          tree.result_type(resource.spec))
        self.assertEquals(10, tree.calculate(resource).data)

    def test_even_basicer(self):
        tree = parse("self.age")

        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        resource = self.api.build_resource('employees/%s' % employee_id)

        self.assertEquals(self.employee_spec.fields['age'],
                          tree.result_type(resource.spec))
        self.assertEquals(41,
                          tree.calculate(resource).data)

    def test_basic_link_follow(self):
        tree = parse("self.division")

        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)

        result_type = tree.result_type(resource.spec)

        self.assertEquals(ResourceLinkSpec,
                          type(result_type))
        self.assertEquals('division', result_type.name)

        calculated = tree.calculate(resource)
        self.assertEquals(Resource, type(calculated))
        self.assertEquals('division', calculated.spec.name)

    def test_aggregate_filtered(self):
        tree = parse("employees.division[type='sales'].yearly_sales")

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        division_id_1 = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 100})
        division_id_2 = self.api.post('divisions', {'type': 'marketting', 'yearly_sales': 20})

        self.api.post('employees/%s/division' % (employee_id_1,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_2,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_3,), {'id': division_id_2})

        resource = self.api.build_resource('employees/%s' % employee_id_1)

        import ipdb; ipdb.set_trace()
        results = tree.calculate(resource)
        self.assertEquals([
            Field(None, 'yearly_sales', self.division_spec.create_child_spec('yearly_sales'), 100)
        ], results)

    def test_list(self):
        tree = parse("employees.division")

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        division_id_1 = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 100})
        division_id_2 = self.api.post('divisions', {'type': 'marketting', 'yearly_sales': 20})

        self.api.post('employees/%s/division' % (employee_id_1,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_2,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_3,), {'id': division_id_2})

        resource = self.api.build_resource('employees/%s' % employee_id_1)
        result = tree.calculate(resource)

        self.assertEquals([
            Resource(None, 'division', self.division_spec, {'type': 'sales', 'yearly_sales': 100}),
            Resource(None, 'division', self.division_spec, {'type': 'marketting', 'yearly_sales': 20}),
        ], result)

    def test_link_list(self):
        pass

    def test_reverse_list(self):
        pass  # division.employee_employees
        tree = parse("self.division.link_employees_division")

        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        division_id_1 = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 100})
        division_id_2 = self.api.post('divisions', {'type': 'marketting', 'yearly_sales': 20})

        self.api.post('employees/%s/division' % (employee_id_1,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_2,), {'id': division_id_1})
        self.api.post('employees/%s/division' % (employee_id_3,), {'id': division_id_2})

        resource = self.api.build_resource('employees/%s' % employee_id_1)
        result = tree.calculate(resource)

        self.assertEquals([
            Resource(None, 'employee', self.employee_spec, {'name': 'ned', 'age': 41}),
            Resource(None, 'employee', self.employee_spec, {'name': 'bob', 'age': 31}),
        ], result)

    def test_calced_resource_ref(self):
        tree = parse("self.sailors")
        resource = {'sailors': {"pay": 13.0}}
        self.assertEquals({'pay': 13.0}, tree.calculate(resource))

    def test_calculation(self):
        tree = parse("self.sailors.pay + (10.0 * (6 / 3) - 7)")
        resource = {'sailors': {"pay": 13.0}}
        self.assertEquals(26, tree.calculate(resource))

    def test_filter(self):
        tree = parse("self.sailors[pay=13]")
        resource = {'sailors': [{"pay": 13.0}]}
        filtered = tree.calculate(resource)
        self.assertEquals([{"pay": 13.0}], filtered)
        self.assertEquals([{}], filtered._filter)

    def test_filter_multi(self):
        tree = parse("self.sailors[pay=13&age=40]")
        resource = {'sailors': [{"pay": 13.0, "age": 40}]}
        filtered = tree.calculate(resource)
        self.assertEquals([{"pay": 13.0, "age": 40}], filtered)
        self.assertEquals([{}], filtered._filter)

    def test_spec_hier_error(self):
        tree = parse("self.name")
        employee_id = self.api.post('employees', {'name': 'sailor'})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)
        aggregation, spec = tree.aggregation(resource)
        # unsure how this guy fits in exactly

        self.assertEquals([{'$match': {'_id': employee_id}}, {'$project': {'name': True}}], aggregation)

    def test_condition_nofield(self):
        try:
            parse("employees[total_nonexistant>100]")
            self.fail("should have thrown")
        except Exception as e:
            self.assertEquals("", str(e))

    def test_const_type(self):
        try:
            parse("employees[age>'str']")
            self.fail("should have thrown")
        except Exception as e:
            self.assertEquals("", str(e))

    def test_aggregation(self):
        tree = parse("employees[age>40].division[type='sales'].yearly_sales")
        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)
        agg_collection = tree.root_collection(resource)
        self.assertEquals(self.db.resource_employee.name, agg_collection.name)
        aggregation, spec = tree.aggregation(resource)
        self.assertEquals([
            {"$match": {"age": {"$gt": 40}}},
            {"$lookup": {
                    "from": "resource_division",
                    "localField": "division",
                    "foreignField": "_id",
                    "as": "_field_division",
            }},
            {"$unwind": "$_field_division"},
            {"$replaceRoot": {"newRoot": "$_field_division"}},
            {"$match": {"type": {"$eq": "sales"}}},
            {"$project": {"yearly_sales": True}},
        ], aggregation)
        self.assertEquals(self.api.schema.specs['division'].fields['yearly_sales'],
                          spec)
        self.assertEquals(['employees.division.yearly_sales'], tree.all_resource_refs())

    def test_aggregation_self(self):
        tree = parse("self.division[type='sales'].yearly_sales")
        employee_id = self.api.post('employees', {'name': 'sailor', 'age': 41})
        division_id = self.api.post('divisions', {'type': 'sales', 'yearly_sales': 10})
        self.api.post('employees/%s/division' % (employee_id,), {'id': division_id})
        resource = self.api.build_resource('employees/%s' % employee_id)
        agg_collection = tree.root_collection(resource)
        self.assertEquals(self.db.resource_employee.name, agg_collection.name)
        aggregation, spec = tree.aggregation(resource)
        self.assertEquals([
            {"$match": {'_id': employee_id}},
            {"$lookup": {
                    "from": "resource_division",
                    "localField": "division",
                    "foreignField": "_id",
                    "as": "_field_division",
            }},
            {"$unwind": "$_field_division"},
            {"$replaceRoot": {"newRoot": "$_field_division"}},
            {"$match": {"type": {"$eq": "sales"}}},
            {"$project": {"yearly_sales": True}},
        ], aggregation)
        self.assertEquals(self.api.schema.specs['division'].fields['yearly_sales'],
                          spec)
        self.assertEquals(['self.division.yearly_sales'], tree.all_resource_refs())


    def test_aggregates(self):
        # entities[name=self.other[resolve='me',first=True]]
        # parents[name='ned'].entities[averagePay>average(self.children[self.type='boss'].pay)]
        pass
