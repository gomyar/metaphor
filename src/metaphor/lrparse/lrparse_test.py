
import unittest

from .lrparse import parse, parse_filter
from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.schema import Field

from .lrparse import FieldRef, ResourceRef


class LRParseTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "age": {
                            "type": "int"
                        },
                        "division": {
                            "type": "link",
                            "target_spec_name": "division",
                        },
                    },
                },
                "division": {
                    "fields": {
                        "name": {
                            "type": "str",
                        },
                        "yearly_sales": {
                            "type": "int",
                        },
                        "sections": {
                            "type": "collection",
                            "target_spec_name": "section",
                        },
                        "parttimers": {
                            "type": "linkcollection",
                            "target_spec_name": "employee",
                        },
                        "partners": {
                            "type": "collection",
                            "target_spec_name": "employee",
                        },
                    },
                },
                "section": {
                    "fields": {
                        "name": {
                            "type": "str",
                        },
                    },
                },
            },
            "root": {
                "employees": {
                    "type": "collection",
                    "target_spec_name": "employee",
                },
                "divisions": {
                    "type": "collection",
                    "target_spec_name": "division",
                }
            },
        })
        self.schema.load_schema()

    def test_basic(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})

        tree = parse("self.division.yearly_sales", self.schema.specs['employee'])

        self.assertEquals(self.schema.specs['division'].fields['yearly_sales'], tree.infer_type())
        self.assertFalse(tree.is_collection())

        self.assertEquals(10, tree.calculate(employee_id))

    def test_even_basicer(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')

        tree = parse("self.age", self.schema.specs['employee'])
        self.assertEquals(self.schema.specs['employee'].fields['age'], tree.infer_type())
        self.assertFalse(tree.is_collection())

        self.assertEquals(41, tree.calculate(employee_id))

    def test_basic_link_follow(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})

        tree = parse("self.division", self.schema.specs['employee'])
        self.assertEquals(self.schema.specs['division'], tree.infer_type())
        self.assertFalse(tree.is_collection())

        calculated = tree.calculate(employee_id)
        self.assertEquals({
            '_id': self.schema.decodeid(division_id),
            'name': 'sales',
            'yearly_sales': 10,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
        }, calculated)

    def test_aggregate_filtered(self):
        tree = parse("sum(employees.division[name='sales'].yearly_sales)", self.schema.specs['employee'])

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_3, {'division': division_id_2})

        result = tree.calculate(employee_id_1)
        self.assertEquals(100, result)

    def test_list(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_3, {'division': division_id_2})

        tree = parse("employees.division", self.schema.specs['employee'])

        # size and offset to be added
        result = tree.calculate(employee_id_1)

        # just making up for a lack of ordering
        division_1 = [r for r in result if r['name'] == 'sales'][0]
        division_2 = [r for r in result if r['name'] == 'marketting'][0]

        self.assertEquals('sales', division_1['name'])
        self.assertEquals('marketting', division_2['name'])

        self.assertEquals(division_id_1, self.schema.encodeid(division_1['_id']))
        self.assertEquals(division_id_2, self.schema.encodeid(division_2['_id']))

    def test_reverse_list(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_3, {'division': division_id_2})

        tree = parse("self.division.link_employee_division", self.schema.specs['employee'])
        result = tree.calculate(employee_id_1)

        self.assertEquals("ned", result[0]['name'])
        self.assertEquals("bob", result[1]['name'])

        self.assertEquals(self.schema.decodeid(employee_id_1), result[0]['_id'])
        self.assertEquals(self.schema.decodeid(employee_id_2), result[1]['_id'])

    def test_spec_hier_error(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor'}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})

        tree = parse("self.name", self.schema.specs['employee'])
        aggregation, spec, is_aggregate = tree.aggregation(employee_id)
        # unsure how this guy fits in exactly

        self.assertEquals([
            {'$match': {'_id': self.schema.decodeid(employee_id)}},
            {'$project': {'name': True}}
        ], aggregation)


    def test_nonexistant_field_in_calc(self):
        try:
            tree = parse("self.nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEquals("No such field nonexistant in employee", str(e))

    def test_validate_condition_nofield(self):
        try:
            tree = parse("employees[total_nonexistant>100]", self.schema.specs['employee'])
            tree.validate()
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEquals("Resource employee has no field total_nonexistant", str(e))

    def test_validate_const_type(self):
        try:
            tree = parse("employees[age>'str']", self.schema.specs['employee'])
            tree.validate()
            self.fail("should have thrown")
        except Exception as e:
            self.assertEquals('Cannot compare "int > str"', str(e))

    def test_validate_collection(self):
        try:
            tree = parse("employees.nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except Exception as e:
            self.assertEquals('No such field nonexistant in employee', str(e))

    def test_validate_filtered_collection(self):
        try:
            tree = parse("employees[age>21].nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except Exception as e:
            self.assertEquals('No such field nonexistant in employee', str(e))

    def test_nonexistant_root_collection(self):
        try:
            parse("nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEquals("Cannot parse expression: nonexistant", str(e))

    def test_aggregation(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')
        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})
        tree = parse("employees[age>40].division[name='sales'].yearly_sales", self.schema.specs['employee'])

        aggregation, spec, is_aggregate = tree.aggregation(employee_id)
        self.assertEquals([
            {'$match': {'$and': [{'_parent_field_name': 'employees'},
                                 {'_parent_canonical_url': '/'}]}},
            {'$match': {'age': {'$gt': 40}}},
            {'$lookup': {'as': '_field_division',
                        'foreignField': '_id',
                        'from': 'resource_division',
                        'localField': 'division'}},
            {'$group': {'_id': '$_field_division'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'name': {'$eq': 'sales'}}},
            {'$project': {'yearly_sales': True}}], aggregation)
        self.assertEquals(self.schema.specs['division'].fields['yearly_sales'],
                          spec)

    def test_conditions_multiple(self):
        employee_spec = self.schema.specs['employee']
        self.schema.add_field(employee_spec, 'salary', 'int')
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41, 'salary': 100}, 'employees')
        tree = parse("employees[age>40 & salary>99].name", self.schema.specs['employee'])

        aggregation, spec, is_aggregate = tree.aggregation(employee_id)
        self.assertEquals([
            {'$match': {'$and': [{'_parent_field_name': 'employees'},
                                 {'_parent_canonical_url': '/'}]}},
            {'$match': {'$and' : [ {'age': {'$gt': 40}}, {'salary': {'$gt': 99}}]}},
            {'$project': {'name': True}}], aggregation)
        self.assertEquals(self.schema.specs['employee'].fields['name'],
                          spec)

    def test_conditions_multiple_or(self):
        employee_spec = self.schema.specs['employee']
        self.schema.add_field(employee_spec, 'salary', 'int')
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41, 'salary': 100}, 'employees')
        tree = parse("employees[age>40 | salary>99].name", self.schema.specs['employee'])

        aggregation, spec, is_aggregate = tree.aggregation(employee_id)
        self.assertEquals([
            {'$match': {'$and': [{'_parent_field_name': 'employees'},
                                 {'_parent_canonical_url': '/'}]}},
            {'$match': {'$or' : [ {'age': {'$gt': 40}}, {'salary': {'$gt': 99}}]}},
            {'$project': {'name': True}}], aggregation)
        self.assertEquals(self.schema.specs['employee'].fields['name'],
                          spec)

    def test_aggregation_self(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')

        tree = parse("self.division[name='sales'].yearly_sales", self.schema.specs['employee'])

        aggregation, spec, is_aggregate = tree.aggregation(employee_id)
        self.assertEquals([
            {'$match': {'_id': self.schema.decodeid(employee_id)}},
            {'$lookup': {'as': '_field_division',
                        'foreignField': '_id',
                        'from': 'resource_division',
                        'localField': 'division'}},
            {'$group': {'_id': '$_field_division'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'name': {'$eq': 'sales'}}},
            {'$project': {'yearly_sales': True}}], aggregation)
        self.assertEquals(self.schema.specs['division'].fields['yearly_sales'],
                          spec)


    def test_aggregates(self):
        # entities[name=self.other[resolve='me',first=True]]
        # parents[name='ned'].entities[averagePay>average(self.children[self.type='boss'].pay)]
        pass

    def test_calc_operators(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "int")
        employee_spec.fields["tax"] = Field("tax", "int")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10, 'tax': 2}, 'employees')

        tree = parse("self.salary + self.tax", employee_spec)
        self.assertEquals(12, tree.calculate(employee_id_1))

        tree = parse("self.salary - self.tax", employee_spec)
        self.assertEquals(8, tree.calculate(employee_id_1))

        tree = parse("self.salary * self.tax", employee_spec)
        self.assertEquals(20, tree.calculate(employee_id_1))

        tree = parse("self.salary / self.tax", employee_spec)
        self.assertEquals(5, tree.calculate(employee_id_1))

        tree = parse("self.salary < self.tax", employee_spec)
        self.assertTrue(False is tree.calculate(employee_id_1))

        tree = parse("self.salary > self.tax", employee_spec)
        self.assertTrue(True is tree.calculate(employee_id_1))

        tree = parse("self.salary = self.tax", employee_spec)
        self.assertTrue(False is tree.calculate(employee_id_1))

        tree = parse("self.salary <= self.tax", employee_spec)
        self.assertTrue(False is tree.calculate(employee_id_1))

        tree = parse("self.salary >= self.tax", employee_spec)
        self.assertTrue(True is tree.calculate(employee_id_1))

    def test_calc_nones(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "int")
        employee_spec.fields["tax"] = Field("tax", "int")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10, 'tax': None}, 'employees')

        tree = parse("self.salary + self.tax", employee_spec)
        self.assertEquals(10, tree.calculate(employee_id_1))

        tree = parse("self.salary - self.tax", employee_spec)
        self.assertEquals(10, tree.calculate(employee_id_1))

        tree = parse("self.salary * self.tax", employee_spec)
        self.assertEquals(None, tree.calculate(employee_id_1))

        # Going with None instead of NaN for now
        tree = parse("self.salary / self.tax", employee_spec)
        self.assertEquals(None, tree.calculate(employee_id_1))

    def test_function_call_param_list(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("round(self.salary + self.tax, 2)", employee_spec)
        self.assertEquals(13, tree.calculate(employee_id_1))

    def test_function_basic(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned'}, 'employees')

        employee_spec = self.schema.specs['employee']
        tree = parse("round(14.14)", employee_spec)
        self.assertEquals(14, tree.calculate(employee_id_1))

    def test_function_call_param_list_multiple_calcs(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("round(self.salary) + round(self.tax)", employee_spec)
        self.assertEquals(13, tree.calculate(employee_id_1))

    def test_function_within_a_function(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.12345}, 'employees')

        tree = parse("round(round(self.salary, 4), 3)", employee_spec)
        self.assertEquals(10.123, tree.calculate(employee_id_1))

    def test_ternary_condition(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary < 2 -> 5 : 10", employee_spec)
        self.assertEquals(10, tree.calculate(employee_id_1))

        tree = parse("self.salary > 10 -> 5 : 10", employee_spec)
        self.assertEquals(5, tree.calculate(employee_id_1))

        tree = parse("(self.salary + 5) > 15 -> 5 : 10", employee_spec)
        self.assertEquals(5, tree.calculate(employee_id_1))

    def test_ternary_condition_rhs(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary > self.tax -> 'greater' : 'wrong'", employee_spec)
        self.assertEquals('greater', tree.calculate(employee_id_1))

        tree = parse("self.salary < (self.tax + 10) -> 'less' : 'wrong'", employee_spec)
        self.assertEquals('less', tree.calculate(employee_id_1))

    def test_ternary_condition_resource(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary > 10 -> self.salary : 0", employee_spec)
        self.assertEquals(10.6, tree.calculate(employee_id_1))

        tree = parse("self.salary < 10 -> 0 : self.tax", employee_spec)
        self.assertEquals(2.4, tree.calculate(employee_id_1))

    def test_ternary_resource_plus_const(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary > 10 -> 11 : self.salary", employee_spec)
        self.assertEquals(11, tree.calculate(employee_id_1))

        tree = parse("self.salary < 10 -> self.tax: 12", employee_spec)
        self.assertEquals(12, tree.calculate(employee_id_1))

    def test_math_functions(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 20}, 'employees')
        self.schema.insert_resource('employee', {'name': 'bob', 'salary': 10}, 'employees')
        self.schema.insert_resource('employee', {'name': 'bil', 'salary': 30}, 'employees')

        # max
        tree = parse("max(employees.salary)", employee_spec)
        self.assertEquals(30, tree.calculate(employee_id_1))

        # min
        tree = parse("min(employees.salary)", employee_spec)
        self.assertEquals(10, tree.calculate(employee_id_1))

        # avg
        tree = parse("average(employees.salary)", employee_spec)
        self.assertEquals(20, tree.calculate(employee_id_1))

        # sum
        tree = parse("sum(employees.salary)", employee_spec)
        self.assertEquals(60, tree.calculate(employee_id_1))

    def test_extra_math(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 20.1234}, 'employees')
        self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.5678}, 'employees')
        self.schema.insert_resource('employee', {'name': 'bil', 'salary': 30.7777}, 'employees')

        tree = parse("round(sum(employees.salary), 2) + round(max(employees.salary))", employee_spec)
        self.assertEquals(92.47, tree.calculate(employee_id_1))


        tree = parse("round(sum(employees[name='ned'].salary), 2) + round(max(employees.salary))", employee_spec)
        self.assertEquals(61.69, tree.calculate(employee_id_1))

        tree = parse("round(sum(employees[name='ned'].salary), 2) + round(max(employees[name='ned'].salary))", employee_spec)
        self.assertEquals(50.69, tree.calculate(employee_id_1))

        # filter nones
        # filter generic aggregates (filter(aggregate, name='paul', age>20))
        # space out range
        # cap (ceil, floor) aggregates
        # min max range

    def test_return_type(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age>40].division[name='sales'].yearly_sales", employee_spec)
        self.assertEquals(division_spec.fields['yearly_sales'], tree.infer_type())
        self.assertTrue(tree.is_collection())

        tree = parse("employees[age>40].division[name='sales']", employee_spec)
        # it's a link spec
        self.assertEquals(division_spec, tree.infer_type())
        self.assertTrue(tree.is_collection())

        tree = parse("self.division", employee_spec)
        self.assertEquals(division_spec, tree.infer_type())
        self.assertFalse(tree.is_collection())

    def test_root_collection_aggregates(self):
        tree = parse("employees.division", self.schema.specs['division'])

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_3, {'division': division_id_2})

        result = tree.calculate(division_id_1)
        self.assertEquals(2, len(result))

    def test_calculate_toplevel_rootresourceref(self):
        tree = parse("employees[name='bob']", self.schema.specs['division'])

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        result = tree.calculate(division_id_1)
        self.assertEquals(1, len(result))
        self.assertEquals('bob', result[0]['name'])

        # work with either resource type as starting point
        result = tree.calculate(employee_id_1)
        self.assertEquals(1, len(result))
        self.assertEquals('bob', result[0]['name'])

    def test_parent_link(self):
        tree = parse("self.parent_division_sections", self.schema.specs['section'])

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        section_id_1 = self.schema.insert_resource('section', {'name': 'appropriation'}, parent_type='division', parent_id=division_id_1, parent_field_name='sections')

        result = tree.calculate(section_id_1)
        self.assertEquals('sales', result['name'])

    def test_parse_url(self):
        tree = parse("employees", self.schema.root)

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_3, {'division': division_id_2})

        result = tree.calculate(employee_id_1)
        self.assertEquals(3, len(result))

    def test_linkcollection(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        tree = parse('self.parttimers', self.schema.specs['division'])
        self.assertEquals([], tree.calculate(division_id_1))

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_1)

        self.assertEquals([{
            '_id': self.schema.decodeid(employee_id_1),
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
            'age': 41,
            'name': 'ned'}], tree.calculate(division_id_1))

    def test_linkcollection_filter(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_1)
        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_2)
        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_3)

        tree = parse('self.parttimers[age>30]', self.schema.specs['division'])

        self.assertEquals([
            {'_id': self.schema.decodeid(employee_id_1),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 41,
             'name': 'ned'},
            {'_id': self.schema.decodeid(employee_id_2),
             '_parent_canonical_url': '/',
             '_parent_field_name': 'employees',
             '_parent_id': None,
             '_parent_type': 'root',
             'age': 31,
             'name': 'bob'},
            ], tree.calculate(division_id_1))

    def test_dependencies(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age>40].division[name='sales'].yearly_sales", employee_spec)
        self.assertEquals(
            tree.get_resource_dependencies(),
            {
                'root.employees',
                'employee.division',
                'employee.age',
                'division.name',
                'division.yearly_sales',
            })

        tree = parse("self.age", employee_spec)
        self.assertEquals(
            tree.get_resource_dependencies(),
            {
                'employee.age',
            })

        tree = parse("self.parttimers", division_spec)
        self.assertEquals(
            tree.get_resource_dependencies(),
            {
                'division.parttimers',
            })

        tree = parse("self.division.parttimers", employee_spec)
        self.assertEquals(
            tree.get_resource_dependencies(),
            {
                'employee.division',
                'division.parttimers',
            })

        # referenced calc
        self.schema.add_calc(employee_spec, 'my_division', 'self.division.link_employee_division')
        tree = parse("self.my_division", employee_spec)
        self.assertEquals(
            tree.get_resource_dependencies(),
            {
                'employee.my_division',
            })

        # reverse link
        tree = parse("self.division.link_employee_division.name", employee_spec)
        self.assertEquals(
            tree.get_resource_dependencies(),
            {
                'employee.division',
                'employee.name',
            })

    def test_gte(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age>=40]", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        calculated = tree.calculate(employee_id)
        self.assertEquals([{
            '_id': self.schema.decodeid(employee_id),
            'name': 'sailor',
            'age': 40,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
        }], calculated)

    def test_lte(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age<=40]", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        calculated = tree.calculate(employee_id)
        self.assertEquals([{
            '_id': self.schema.decodeid(employee_id),
            'name': 'sailor',
            'age': 40,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'employees',
            '_parent_id': None,
            '_parent_type': 'root',
        }], calculated)

    def test_search_filter(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual({'age': {'$lte': 40}}, tree.condition_aggregation(employee_spec))

    def test_search_filter_multiple(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40 & name='sailor'", employee_spec)

        employee_id = self.schema.insert_resource(
            'employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual(
            {'$and': [{'age': {'$lte': 40}}, {'name': {'$eq': 'sailor'}}]},
            tree.condition_aggregation(employee_spec))

    def test_search_filter_commas_are_nice(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40, name='sailor'", employee_spec)

        employee_id = self.schema.insert_resource(
            'employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual(
            {'$or': [{'age': {'$lte': 40}}, {'name': {'$eq': 'sailor'}}]},
            tree.condition_aggregation(employee_spec))

    def test_search_filter_like_string(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("name~'sam'", employee_spec)

        self.assertEqual(
            {'name': {'$options': 'i', '$regex': 'sam'}},
            tree.condition_aggregation(employee_spec))

    def test_search_filter_like_string_or(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("name~'sam',name~'bob'", employee_spec)

        self.assertEqual(
            {'$or': [{'name': {'$options': 'i', '$regex': 'sam'}},
                     {'name': {'$options': 'i', '$regex': 'bob'}}]},
            tree.condition_aggregation(employee_spec))

    def test_search_filter_commas_and_or(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40, name='sailor' | name='weaver'", employee_spec)

        employee_id = self.schema.insert_resource(
            'employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual(
            {'$or': [{'$or': [{'age': {'$lte': 40}}, {'name': {'$eq': 'sailor'}}]},
                     {'name': {'$eq': 'weaver'}}]},
            tree.condition_aggregation(employee_spec))

    def test_resources_in_different_collections(self):
        tree = parse("employees", self.schema.root)

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        employee_id_4 = self.schema.insert_resource('employee', {'name': 'pete', 'age': 60}, 'partners', 'division', division_id_1)

        result = tree.calculate(employee_id_1)
        self.assertEquals(3, len(result))

        employee_spec = self.schema.specs['employee']
        tree = parse("max(employees.age)", employee_spec)
        self.assertEquals(41, tree.calculate(employee_id_1))

    def test_validate_root(self):
        try:
            tree = parse("schmemployees", self.schema.root)
            tree.validate()
            self.fail("should have thrown")
        except SyntaxError as se:
            self.assertEqual("Cannot parse expression: schmemployees", str(se))

    def test_validate_condition(self):
        try:
            tree = parse("employees[nope>21]", self.schema.root)
            tree.validate()
            self.fail("should have thrown")
        except SyntaxError as se:
            self.assertEqual("Resource employee has no field nope", str(se))

    def test_ego(self):
        tree = parse("ego", self.schema.root)
        self.assertEqual({}, tree.calculate())
