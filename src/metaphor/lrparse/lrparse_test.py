
import unittest
from datetime import datetime

from .lrparse import parse, parse_filter, parse_canonical_url
from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory

from metaphor.schema import Field
from metaphor.update_aggregation import create_update_aggregation

from .lrparse import FieldRef, ResourceRef


class LRParseTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self._create_test_schema({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
                            "type" : "str"
                        },
                        "pseudoname" : {
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

    def _create_test_schema(self, data):
        data['current'] = True
        data['version'] = 'test'
        data['root'] = data.get('root', {})
        inserted = self.db.metaphor_schema.insert_one(data)
        self.schema = SchemaFactory(self.db).load_current_schema()

    def _calculate(self, resource_name, tree, resource_id):
        resource_id = self.schema.decodeid(resource_id)
        match_agg = [{"$match": {"_id": resource_id}}]
        agg = create_update_aggregation(resource_name, "test_field", tree, match_agg)
        self.schema.db['metaphor_resource'].aggregate(agg)
        resource = self.schema.db['metaphor_resource'].find_one({"_id": resource_id})
        return resource.get("test_field") if resource else None

    def perform_simple_calc(self, collection, resource_id, calc):
        val = list(collection.aggregate([{"$match": {"_id": self.schema.decodeid(resource_id)}}] + calc.create_aggregation()))
        return val[0]['_val'] if val else None

    def test_basic(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})

        tree = parse("self.division.yearly_sales", self.schema.specs['employee'])

        self.assertEqual(self.schema.specs['division'].fields['yearly_sales'], tree.infer_type())
        self.assertFalse(tree.is_collection())

        self.assertEqual(10, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id, tree))

    def test_even_basicer(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')

        tree = parse("self.age", self.schema.specs['employee'])
        self.assertEqual(self.schema.specs['employee'].fields['age'], tree.infer_type())
        self.assertFalse(tree.is_collection())

        self.assertEqual(41, self._calculate('employee', tree, employee_id))

    def test_basic_link_follow(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})

        tree = parse("self.division", self.schema.specs['employee'])
        self.assertEqual(self.schema.specs['division'], tree.infer_type())
        self.assertFalse(tree.is_collection())

        calculated = self._calculate('employee', tree, employee_id)
        self.assertEqual(self.schema.decodeid(division_id), calculated)

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

        result = self._calculate('employee', tree, employee_id_1)
        self.assertEqual(100, result)

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
        result = self._calculate('employee', tree, employee_id_1)

        self.assertEqual(2, len(result))
        expected = sorted([
            self.schema.decodeid(division_id_1),
            self.schema.decodeid(division_id_2),
        ])
        actual = sorted([r["_id"] for r in result])
        self.assertEqual(expected, actual)

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
        result = self._calculate('employee', tree, employee_id_1)

        self.assertEqual(self.schema.decodeid(employee_id_1), result[0]['_id'])
        self.assertEqual(self.schema.decodeid(employee_id_2), result[1]['_id'])

    def test_spec_hier_error(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor'}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})

        tree = parse("self.name", self.schema.specs['employee'])
        aggregation = tree.create_aggregation()
        # unsure how this guy fits in exactly

        self.assertEqual([
            {'$addFields': {'_val': '$name'}}
        ], aggregation)

    def test_nonexistant_field_in_calc(self):
        try:
            tree = parse("self.nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEqual("No such field nonexistant in employee", str(e))

    def test_validate_condition_nofield(self):
        try:
            tree = parse("employees[total_nonexistant>100]", self.schema.specs['employee'])
            tree.validate()
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEqual("Resource employee has no field total_nonexistant", str(e))

    def test_validate_const_type(self):
        try:
            tree = parse("employees[age>'str']", self.schema.specs['employee'])
            tree.validate()
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEqual('Cannot compare "int > str"', str(e))

    def test_validate_collection(self):
        try:
            tree = parse("employees.nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEqual('No such field nonexistant in employee', str(e))

    def test_validate_filtered_collection(self):
        try:
            tree = parse("employees[age>21].nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except Exception as e:
            self.assertEqual('No such field nonexistant in employee', str(e))

    def test_validate_ternary(self):
        try:
            tree = parse("max(employees.age) < 50 -> 'young' : 14", self.schema.specs['employee'])
            tree.validate()
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEqual('Both sides of ternary must return same type (str != int)', str(e))

    def test_validate_resource_ternary(self):
        try:
            tree = parse("max(employees.age) < 50 -> employees.age : divisions.sections", self.schema.specs['employee'])
            tree.validate()
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEqual('Both sides of ternary must return same type (int != section)', str(e))

    def test_nonexistant_root_collection(self):
        try:
            parse("nonexistant", self.schema.specs['employee'])
            self.fail("should have thrown")
        except SyntaxError as e:
            self.assertEqual("Cannot parse expression: nonexistant", str(e))

    def test_aggregation(self):
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41}, 'employees')
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')
        self.schema.update_resource_fields('employee', employee_id, {'division': division_id})
        tree = parse("employees[age>40].division[name='sales'].yearly_sales", self.schema.specs['employee'])

        aggregation = tree.create_aggregation()
        self.assertEqual([
            {'$lookup': {'as': '_val',
                        'from': 'metaphor_resource',
                        'pipeline': [{'$match': {'_deleted': {'$exists': False},
                                                '_parent_field_name': 'employees',
                                                '_type': 'employee'}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'age': {'$gt': 40}}},
            {'$lookup': {'as': '_val',
                        'from': 'metaphor_resource',
                        'let': {'id': '$division'},
                        'pipeline': [{'$match': {'$expr': {'$and': [
                                        {'$eq': ['$_id', '$$id']},
                                        {'$eq': ['$_type', 'division']},
                                    ]}}},
                                    {'$match': {'_deleted': {'$exists': False}}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'name': {'$eq': 'sales'}}},
            {'$addFields': {'_val': '$yearly_sales'}}], aggregation)
        self.assertEqual(self.schema.specs['division'].fields['yearly_sales'],
                          tree.infer_type())

    def test_conditions_multiple(self):
        employee_spec = self.schema.specs['employee']
        self.schema.add_field(employee_spec, 'salary', 'int')
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41, 'salary': 100}, 'employees')
        tree = parse("employees[age>40 & salary>99].name", self.schema.specs['employee'])

        aggregation = tree.create_aggregation()
        self.assertEqual([
            {'$lookup': {'as': '_val',
                        'from': 'metaphor_resource',
                        'pipeline': [{'$match': {'_deleted': {'$exists': False},
                                                '_parent_field_name': 'employees',
                                                '_type': 'employee'}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'$and': [{'age': {'$gt': 40}}, {'salary': {'$gt': 99}}]}},
            {'$addFields': {'_val': '$name'}}], aggregation)
        self.assertEqual(self.schema.specs['employee'].fields['name'],
                          tree.infer_type())

    def test_conditions_multiple_or(self):
        employee_spec = self.schema.specs['employee']
        self.schema.add_field(employee_spec, 'salary', 'int')
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41, 'salary': 100}, 'employees')
        tree = parse("employees[age>40 | salary>99].name", self.schema.specs['employee'])

        aggregation = tree.create_aggregation()
        self.assertEqual([
            {'$lookup': {'as': '_val',
                        'from': 'metaphor_resource',
                        'pipeline': [{'$match': {'_deleted': {'$exists': False},
                                                '_parent_field_name': 'employees',
                                                '_type': 'employee'}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'$or': [{'age': {'$gt': 40}}, {'salary': {'$gt': 99}}]}},
            {'$addFields': {'_val': '$name'}}], aggregation)
        self.assertEqual(self.schema.specs['employee'].fields['name'],
                          tree.infer_type())

    def test_aggregation_self(self):
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41, 'division': division_id}, 'employees')

        tree = parse("self.division[name='sales'].yearly_sales", self.schema.specs['employee'])

        aggregation = tree.create_aggregation()
        self.assertEqual([
            {'$lookup': {'as': '_val',
                        'from': 'metaphor_resource',
                        'let': {'id': '$division'},
                        'pipeline': [{'$match': {'$expr': {
                                        '$and': [
                                            {'$eq': ['$_id', '$$id']},
                                            {'$eq': ['$_type', 'division']},
                                        ]}}},
                                    {'$match': {'_deleted': {'$exists': False}}}]}},
            {'$group': {'_id': '$_val'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$match': {'name': {'$eq': 'sales'}}},
            {'$addFields': {'_val': '$yearly_sales'}}], aggregation)
        self.assertEqual(self.schema.specs['division'].fields['yearly_sales'],
                          tree.infer_type())

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
        self.assertEqual(12, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary - self.tax", employee_spec)
        self.assertEqual(8, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary * self.tax", employee_spec)
        self.assertEqual(20, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary / self.tax", employee_spec)
        self.assertEqual(5, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary < self.tax", employee_spec)
        self.assertTrue(False is self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary > self.tax", employee_spec)
        self.assertTrue(True is self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary = self.tax", employee_spec)
        self.assertTrue(False is self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary <= self.tax", employee_spec)
        self.assertTrue(False is self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary >= self.tax", employee_spec)
        self.assertTrue(True is self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary != self.tax", employee_spec)
        self.assertTrue(True is self._calculate('employee', tree, employee_id_1))


    def test_calc_nones(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "int")
        employee_spec.fields["tax"] = Field("tax", "int")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10, 'tax': None}, 'employees')

        tree = parse("self.salary + self.tax", employee_spec)
        self.assertEqual(10, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary - self.tax", employee_spec)
        self.assertEqual(10, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary * self.tax", employee_spec)
        self.assertEqual(None, self._calculate('employee', tree, employee_id_1))

        # Going with None instead of NaN for now
        tree = parse("self.salary / self.tax", employee_spec)
        self.assertEqual(None, self._calculate('employee', tree, employee_id_1))

    def test_function_call_param_list(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("round(self.salary + self.tax, 2)", employee_spec)
        self.assertEqual(13, self._calculate('employee', tree, employee_id_1))

    def test_function_basic(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned'}, 'employees')

        employee_spec = self.schema.specs['employee']
        tree = parse("round(14.14)", employee_spec)
        self.assertEqual(14, self._calculate('employee', tree, employee_id_1))

    def test_function_call_param_list_multiple_calcs(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("round(self.salary) + round(self.tax)", employee_spec)
        self.assertEqual(13, self._calculate('employee', tree, employee_id_1))

    def test_function_within_a_function(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.12345}, 'employees')

        tree = parse("round(round(self.salary, 4), 3)", employee_spec)
        self.assertEqual(10.123, self._calculate('employee', tree, employee_id_1))

    def test_ternary_condition(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary < 2 -> 5 : 10", employee_spec)
        self.assertEqual(10, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary > 10 -> 5 : 10", employee_spec)
        self.assertEqual(5, self._calculate('employee', tree, employee_id_1))

        tree = parse("(self.salary + 5) > 15 -> 5 : 10", employee_spec)
        self.assertEqual(5, self._calculate('employee', tree, employee_id_1))

    def test_ternary_condition_rhs(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary > self.tax -> 'greater' : 'wrong'", employee_spec)
        self.assertEqual('greater', self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary < (self.tax + 10) -> 'less' : 'wrong'", employee_spec)
        self.assertEqual('less', self._calculate('employee', tree, employee_id_1))

    def test_ternary_condition_resource(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary > 10 -> self.salary : 0", employee_spec)
        self.assertEqual(10.6, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary < 10 -> 0 : self.tax", employee_spec)
        self.assertEqual(2.4, self._calculate('employee', tree, employee_id_1))

    def test_ternary_resource_plus_const(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["tax"] = Field("tax", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.6, 'tax': 2.4}, 'employees')

        tree = parse("self.salary > 10 -> 11 : self.salary", employee_spec)
        self.assertEqual(11, self._calculate('employee', tree, employee_id_1))

        tree = parse("self.salary < 10 -> self.tax: 12", employee_spec)
        self.assertEqual(12, self._calculate('employee', tree, employee_id_1))

    def test_switch(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary_range"] = Field("salary_range", "str")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary_range': 'upper'}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'salary_range': 'lower'}, 'employees')

        tree = parse("self.salary_range -> ('upper': 20.0, 'lower': 40.0)", employee_spec)
        self.assertEqual(20.0, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))
        self.assertEqual(40.0, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_2, tree))

    def test_switch_longer_list(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary_range"] = Field("salary_range", "str")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary_range': 'upper'}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'salary_range': 'lower'}, 'employees')

        tree = parse("self.salary_range -> ('upper': 20.0, 'lower': 40.0, 'middling': 30.0)", employee_spec)

        self.assertEqual(20, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))
        self.assertEqual(40, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_2, tree))

    def test_switch_basic(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary_range"] = Field("salary_range", "str")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary_range': 'upper'}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'salary_range': 'lower'}, 'employees')

        tree = parse("self.salary_range -> ('upper': 20.0)", employee_spec)
        self.assertEqual(20.0, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))
        self.assertEqual(None, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_2, tree))

    def test_switch_field_refs(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["upper_salary"] = Field("salary", "float")
        employee_spec.fields["lower_salary"] = Field("salary", "float")
        employee_spec.fields["salary_range"] = Field("salary_range", "str")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary_range': 'upper', 'upper_salary': 50000, 'lower_salary': 40000}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'salary_range': 'lower', 'upper_salary': 30000, 'lower_salary': 20000}, 'employees')

        tree = parse("self.salary_range -> ('upper': self.upper_salary, 'lower': self.lower_salary)", employee_spec)
        self.assertEqual(50000, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))
        self.assertEqual(20000, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_2, tree))

    def test_switch_calcs(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["upper_salary_level"] = Field("salary", "float")
        employee_spec.fields["lower_salary_level"] = Field("salary", "float")
        employee_spec.fields["salary"] = Field("salary", "float")
        employee_spec.fields["salary_range"] = Field("salary_range", "str")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary_range': 'upper', 'salary': 50000, 'upper_salary_level': 0.4, 'lower_salary_level': 0.2}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'salary_range': 'lower', 'salary': 30000, 'upper_salary_level': 0.5, 'lower_salary_level': 0.3}, 'employees')

        tree = parse("self.salary_range -> ('upper': (self.salary * self.upper_salary_level), 'lower': (self.salary * self.lower_salary_level))", employee_spec)
        self.assertEqual(20000, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))
        self.assertEqual(9000, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_2, tree))

    def test_switch_collections(self):
        spec = self.schema.create_spec('branch')
        self.schema.create_field('branch', 'name', 'str')
        self.schema.create_field('branch', 'employees', 'collection', 'employee')

        branch_id = self.schema.insert_resource('branch', {'name': 'sales'}, 'branches')
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 25}, 'employees', 'branch', branch_id)
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 35}, 'employees', 'branch', branch_id)

        tree=parse('self.name -> ("sales": (self.employees[age>20]), "marketting": (self.employees[age>30]))', spec)
        val = list(self.schema.db['metaphor_resource'].aggregate([{"$match": {"_id": self.schema.decodeid(branch_id)}}] + tree.create_aggregation()))
        self.assertEqual(2, len(val))
        self.assertEqual("bob", val[0]['name'])
        self.assertEqual("ned", val[1]['name'])

        self.schema.update_resource_fields('branch', branch_id, {'name': 'marketting'})
        val = list(self.schema.db['metaphor_resource'].aggregate([{"$match": {"_id": self.schema.decodeid(branch_id)}}] + tree.create_aggregation()))
        self.assertEqual(1, len(val))
        self.assertEqual("ned", val[0]['name'])

    def test_math_functions(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 20}, 'employees')
        self.schema.insert_resource('employee', {'name': 'bob', 'salary': 10}, 'employees')
        self.schema.insert_resource('employee', {'name': 'bil', 'salary': 30}, 'employees')

        # max
        tree = parse("max(employees.salary)", employee_spec)
        self.assertEqual(30, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))

        # min
        tree = parse("min(employees.salary)", employee_spec)
        self.assertEqual(10, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))

        # avg
        tree = parse("average(employees.salary)", employee_spec)
        self.assertEqual(20, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))

        # sum
        tree = parse("sum(employees.salary)", employee_spec)
        self.assertEqual(60, self.perform_simple_calc(self.schema.db['metaphor_resource'], employee_id_1, tree))

    def test_extra_math(self):
        employee_spec = self.schema.specs['employee']
        employee_spec.fields["salary"] = Field("salary", "float")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'salary': 20.1234}, 'employees')
        self.schema.insert_resource('employee', {'name': 'ned', 'salary': 10.5678}, 'employees')
        self.schema.insert_resource('employee', {'name': 'bil', 'salary': 30.7777}, 'employees')

        tree = parse("round(sum(employees.salary), 2) + round(max(employees.salary))", employee_spec)
        self.assertEqual(92.47, self._calculate('employee', tree, employee_id_1))

        tree = parse("round(sum(employees[name='ned'].salary), 2) + round(max(employees.salary))", employee_spec)
        self.assertEqual(61.69, self._calculate('employee', tree, employee_id_1))

        tree = parse("round(sum(employees[name='ned'].salary), 2) + round(max(employees[name='ned'].salary))", employee_spec)
        self.assertEqual(50.69, self._calculate('employee', tree, employee_id_1))

        # filter nones
        self.schema.insert_resource('employee', {'name': 'sam'}, 'employees')
        tree = parse("employees[salary!=20.1234]", employee_spec)

        # filter generic aggregates (filter(aggregate, name='paul', age>20))
        # space out range
        # cap (ceil, floor) aggregates
        # min max range

    def test_return_type(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age>40].division[name='sales'].yearly_sales", employee_spec)
        self.assertEqual(division_spec.fields['yearly_sales'], tree.infer_type())
        self.assertTrue(tree.is_collection())

        tree = parse("employees[age>40].division[name='sales']", employee_spec)
        # it's a link spec
        self.assertEqual(division_spec, tree.infer_type())
        self.assertTrue(tree.is_collection())

        tree = parse("self.division", employee_spec)
        self.assertEqual(division_spec, tree.infer_type())
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

        result = self._calculate('division', tree, division_id_1)
        self.assertEqual(2, len(result))

    def test_calculate_toplevel_rootresourceref(self):
        tree = parse("employees[name='bob']", self.schema.specs['division'])

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        result = self._calculate('division', tree, division_id_1)
        self.assertEqual(1, len(result))
        self.assertEqual(self.schema.decodeid(employee_id_2), result[0]['_id'])

        # work with either resource type as starting point
        result = self._calculate('employee', tree, employee_id_1)
        self.assertEqual(1, len(result))
        self.assertEqual(self.schema.decodeid(employee_id_2), result[0]['_id'])

    def test_parent_link(self):
        tree = parse("self.parent_division_sections", self.schema.specs['section'])

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        section_id_1 = self.schema.insert_resource('section', {'name': 'appropriation'}, parent_type='division', parent_id=division_id_1, parent_field_name='sections')

        result = self._calculate('section', tree, section_id_1)
        self.assertEqual(self.schema.decodeid(division_id_1), result)

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

        result = self._calculate('employee', tree, employee_id_1)
        self.assertEqual(3, len(result))

    def test_linkcollection(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting', 'yearly_sales': 20}, 'divisions')

        tree = parse('self.parttimers', self.schema.specs['division'])
        self.assertEqual([], self._calculate('employee', tree, division_id_1))

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_1)

        self.assertEqual([{
            '_id': self.schema.decodeid(employee_id_1)}], self._calculate('division', tree, division_id_1))

    def test_linkcollection_filter(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_1)
        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_2)
        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_3)

        tree = parse('self.parttimers[age>30]', self.schema.specs['division'])

        self.assertEqual([
            {'_id': self.schema.decodeid(employee_id_1),},
            {'_id': self.schema.decodeid(employee_id_2),},
            ], self._calculate('division', tree, division_id_1))

    def test_linkcollection_reverse_aggregation(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_1)
        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_2)
        self.schema.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_3)

        tree = parse('self.parttimers.age', self.schema.specs['division'])

        # testing reverse aggregation when a resource in the "middle" of the calc is added/removed/updated
        # Note: the first aggregation is the "tracker" aggregation
        self.assertEqual([
            [{'$lookup': {'as': '_field_parttimers',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$in': [{'_id': '$$id'},
                                                                                {'$ifNull': ['$parttimers',
                                                                                            []]}]},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_parttimers'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$lookup': {'as': '_field_parttimers',
                        'from': 'metaphor_resource',
                        'let': {'id': '$_id'},
                        'pipeline': [{'$match': {'$expr': {'$and': [{'$in': [{'_id': '$$id'},
                                                                                {'$ifNull': ['$parttimers',
                                                                                            []]}]},
                                                                    {'$eq': ['$_type',
                                                                                'division']}]}}}]}},
            {'$group': {'_id': '$_field_parttimers'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            [{'$match': {'_type': 'division'}}]], tree.build_reverse_aggregations(self.schema.specs['employee'], employee_id_2, 'division', 'parttimers_age'))

    def test_dependencies(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age>40].division[name='sales'].yearly_sales", employee_spec)
        self.assertEqual(
            tree.get_resource_dependencies(),
            {
                'root.employees',
                'employee.division',
                'employee.age',
                'division.name',
                'division.yearly_sales',
            })

        tree = parse("self.age", employee_spec)
        self.assertEqual(
            tree.get_resource_dependencies(),
            {
                'employee.age',
            })

        tree = parse("self.parttimers", division_spec)
        self.assertEqual(
            tree.get_resource_dependencies(),
            {
                'division.parttimers',
            })

        tree = parse("self.division.parttimers", employee_spec)
        self.assertEqual(
            tree.get_resource_dependencies(),
            {
                'employee.division',
                'division.parttimers',
            })

        # referenced calc
        self.schema.add_calc(employee_spec, 'my_division', 'self.division.link_employee_division')
        tree = parse("self.my_division", employee_spec)
        self.assertEqual(
            tree.get_resource_dependencies(),
            {
                'employee.my_division',
            })

        # reverse link
        tree = parse("self.division.link_employee_division.name", employee_spec)
        self.assertEqual(
            tree.get_resource_dependencies(),
            {
                'employee.division',
                'employee.name',
            })

    def test_dependencies_double_barreled(self):
        spec = self.schema.create_spec('double_barreled')
        self.schema.create_field('double_barreled', 'two_words', 'str')

        employee_spec = self.schema.specs['employee']
        self.schema.create_field('employee', 'double_barreled', 'link', 'double_barreled')

        tree = parse("self.link_employee_double_barreled", spec)
        self.assertEqual(
            tree.get_resource_dependencies(),
            {
                'employee.double_barreled',
            })

    def test_gte(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age>=40]", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        calculated = self._calculate('employee', tree, employee_id)
        self.assertEqual([{
            '_id': self.schema.decodeid(employee_id),
        }], calculated)

    def test_lte(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("employees[age<=40]", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        calculated = self._calculate('employee', tree, employee_id)
        self.assertEqual([{
            '_id': self.schema.decodeid(employee_id),
        }], calculated)

    def test_search_filter(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual({'age': {'$lte': 40}}, tree.condition_aggregation(employee_spec, employee_id))

    def test_search_filter_multiple(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40 & name='sailor'", employee_spec)

        employee_id = self.schema.insert_resource(
            'employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual(
            {'$and': [{'age': {'$lte': 40}}, {'name': {'$eq': 'sailor'}}]},
            tree.condition_aggregation(employee_spec, employee_id))

    def test_search_filter_commas_are_nice(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40, name='sailor'", employee_spec)

        employee_id = self.schema.insert_resource(
            'employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual(
            {'$or': [{'age': {'$lte': 40}}, {'name': {'$eq': 'sailor'}}]},
            tree.condition_aggregation(employee_spec, employee_id))

    def test_search_filter_like_string(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("name~'sam'", employee_spec)

        self.assertEqual(
            {'name': {'$options': 'i', '$regex': 'sam'}},
            tree.condition_aggregation(employee_spec, None))

    def test_search_filter_like_string_or(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("name~'sam',name~'bob'", employee_spec)

        self.assertEqual(
            {'$or': [{'name': {'$options': 'i', '$regex': 'sam'}},
                     {'name': {'$options': 'i', '$regex': 'bob'}}]},
            tree.condition_aggregation(employee_spec, None))

    def test_search_filter_commas_and_or(self):
        employee_spec = self.schema.specs['employee']

        tree = parse_filter("age<=40, name='sailor' | name='weaver'", employee_spec)

        employee_id = self.schema.insert_resource(
            'employee', {'name': 'sailor', 'age': 40}, 'employees')

        self.assertEqual(
            {'$or': [{'$or': [{'age': {'$lte': 40}}, {'name': {'$eq': 'sailor'}}]},
                     {'name': {'$eq': 'weaver'}}]},
            tree.condition_aggregation(employee_spec, employee_id))

    def _test_search_filter_resource_ref(self):
        # I'm not sure which way this test should go
        employee_spec = self.schema.specs['employee']

        tree = parse("name=self.pseudoname", employee_spec)

        employee_id = self.schema.insert_resource(
            'employee', {'name': 'sailor', 'pseudoname': 'bob'}, 'employees')

        self.assertEqual(
            {'name': {'$eq': 'bob'}},
            tree.create_condition_aggregation())

    def test_resources_in_different_collections(self):
        tree = parse("employees", self.schema.root)

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        employee_id_4 = self.schema.insert_resource('employee', {'name': 'pete', 'age': 60}, 'partners', 'division', division_id_1)

        result = self._calculate('employee', tree, employee_id_1)
        self.assertEqual(3, len(result))

        employee_spec = self.schema.specs['employee']
        tree = parse("max(employees.age)", employee_spec)
        self.assertEqual(41, self._calculate('employee', tree, employee_id_1))

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

    def test_calc_result(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("max(employees[age>=40].age) + 15", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        calculated = self._calculate('employee', tree, employee_id)
        self.assertEqual(55, calculated)

    def test_basic_calc_result(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        tree = parse("10 + (15 / 3)", employee_spec)

        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 40}, 'employees')

        calculated = self._calculate('employee', tree, employee_id)
        self.assertEqual(15, calculated)

    def test_calc_datetime(self):
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        employee_spec.fields["created"] = Field("created", "datetime")

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'created': "2021-12-01"}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'created': "2021-12-01"}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'bil', 'created': "2021-12-01"}, 'employees')

        tree = parse("self.created - days(2)", employee_spec)

        calculated = self._calculate('employee', tree, employee_id_1)
        self.assertEqual(datetime(2021, 11, 29), calculated)

        tree = parse("self.created - hours(2)", employee_spec)

        calculated = self._calculate('employee', tree, employee_id_1)
        self.assertEqual(datetime(2021, 11, 30, 22), calculated)

        tree = parse("self.created - minutes(2)", employee_spec)

        calculated = self._calculate('employee', tree, employee_id_1)
        self.assertEqual(datetime(2021, 11, 30, 23, 58), calculated)

    def _test_calc_datetime_comparison(self):
        # TODO: support more complex comparisons
        employee_spec = self.schema.specs['employee']
        division_spec = self.schema.specs['division']

        employee_spec.fields["created"] = Field("created", "datetime")
        division_spec.fields["cutoff"] = Field("cutoff", "datetime")

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'cutoff': "2021-12-12"}, 'divisions')

        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'created': "2021-12-01", "division": division_id_1}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'created': "2021-12-14", "division": division_id_1}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'bil', 'created': "2021-12-24", "division": division_id_1}, 'employees')

        tree = parse("self.link_employee_division[created>self.cutoff]", division_spec)

        calculated = self._calculate('employee', tree, division_id_1)
        self.assertEqual(['bob', 'bil'], [e['name'] for e in calculated])

    def test_add_reverse_links(self):
        section_spec = self.schema.specs['section']

        tree = parse('self.name + (self.parent_division_sections.name)', section_spec)

    def test_replace_whitespace_with_spaces(self):
        section_spec = self.schema.specs['section']

        tree = parse('self.name + \t(\nself.parent_division_sections.name\n)', section_spec)


    def test_ternary_within_calc(self):
        # 1 + (self.a == 2 -> 'a': 'b')
        pass

    def test_parse_canonical_url(self):
        division_id = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 10}, 'divisions')
        employee_id = self.schema.insert_resource('employee', {'name': 'sailor', 'age': 41, 'division': division_id}, 'employees')

        tree = parse_canonical_url('employees/%s/division' % employee_id, self.schema.root)

        result = self._calculate('employee', tree, employee_id)
        self.assertEqual(division_id, self.schema.encodeid(result))
