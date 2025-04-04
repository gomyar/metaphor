
import unittest

from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.updater = Updater(self.schema)

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')

        self.division_spec = self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'employees', 'collection', 'employee')

        self.schema.create_field('root', 'divisions', 'collection', 'division')

    def test_updater(self):
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.employees[age>30]')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.updater.update_calc_for_single_resource_change('division', 'older_employees', 'employee', None)

        division_data = self.db.resource_division.find_one()
        self.assertEqual({
            '_id': self.schema.decodeid(division_id_1),
            'name': 'sales',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'older_employees': [{"_id": ObjectId(employee_id_1[2:])}],
        }, division_data)

        employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'Ned', 'age': 41}, 'employees', 'division', division_id_1)

        # check again
        #self.updater.update_calc('division', 'older_employees', division_id_1)
        self.updater.update_calc_for_single_resource_change('division', 'older_employees', 'employee', employee_id_2)
        division_data = self.db.resource_division.find_one()
        self.assertEqual({
            '_id': self.schema.decodeid(division_id_1),
            'name': 'sales',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'older_employees': [{"_id": ObjectId(employee_id_1[2:])}, {"_id": ObjectId(employee_id_2[2:])}],
        }, division_data)

    def test_reverse_aggregation(self):
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.employees[age>30]')
        self.schema.add_calc(self.division_spec, 'average_age', 'average(self.employees.age)')

        average_agg = self.updater.build_reverse_aggregations_to_calc('division', 'average_age', self.employee_spec, None)
        self.assertEqual([
            [{'$lookup': {'as': '_field_employees',
                        'from': 'resource_division',
                        'localField': '_parent_id',
                        'foreignField': '_id'}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            []], average_agg)

        # different calc
        older_agg = self.updater.build_reverse_aggregations_to_calc('division', 'older_employees', self.employee_spec, None)
        self.assertEqual([
            [{'$lookup': {'as': '_field_employees',
                        'from': 'resource_division',
                        'foreignField': '_id',
                        'localField': '_parent_id'}},
            {'$group': {'_id': '$_field_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            []], older_agg)

    def test_reverse_aggregation_link(self):
        self.schema.add_field(self.division_spec, 'manager', 'link', 'employee')
        self.schema.add_calc(self.division_spec, 'manager_age', 'self.manager.age')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.schema.update_resource_fields('division', division_id_1, {'manager': employee_id_1})

        agg = self.updater.build_reverse_aggregations_to_calc('division', 'manager_age', self.employee_spec, employee_id_1)
        self.assertEqual([
            [{'$lookup': {'as': '_field_manager',
                        'from': 'resource_division',
                        'localField': '_id',
                        'foreignField': 'manager'}},
            {'$group': {'_id': '$_field_manager'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            []], agg)

        # check having two links
        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        self.schema.update_resource_fields('division', division_id_2, {'manager': employee_id_1})

    def test_reverse_aggregation_link_collection(self):
        self.schema.add_field(self.division_spec, 'managers', 'linkcollection', 'employee')
        self.schema.add_calc(self.division_spec, 'average_manager_age', 'average(self.managers.age)')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.schema.create_linkcollection_entry('division', division_id_1, 'managers', employee_id_1)

        agg = self.updater.build_reverse_aggregations_to_calc('division', 'average_manager_age', self.employee_spec, employee_id_1)
        self.assertEqual([
            [{'$lookup': {'as': '_field_managers',
                        'from': 'resource_division',
                        'localField': '_id',
                        'foreignField': "managers._id"}},
            {'$group': {'_id': '$_field_managers'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            []], agg)

        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'marketting'}, 'divisions')
        self.schema.create_linkcollection_entry('division', division_id_2, 'managers', employee_id_1)

    def test_reverse_aggregation_calc_through_calc(self):
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.employees[age>30]')
        self.schema.add_calc(self.division_spec, 'older_employees_called_ned', 'self.older_employees[name="ned"]')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'marketting'}, 'divisions')

        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 21}, 'employees', 'division', division_id_1)
        employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'ned', 'age': 31}, 'employees', 'division', division_id_1)
        employee_id_3 = self.schema.insert_resource(
            'employee', {'name': 'fred', 'age': 41}, 'employees', 'division', division_id_1)

        employee_id_4 = self.schema.insert_resource(
            'employee', {'name': 'sam', 'age': 25}, 'employees', 'division', division_id_2)
        employee_id_5 = self.schema.insert_resource(
            'employee', {'name': 'ned', 'age': 35}, 'employees', 'division', division_id_2)

        self.updater.update_calc_for_single_resource_change('division', 'older_employees', 'division', None)
        self.updater.update_calc_for_single_resource_change('division', 'older_employees_called_ned', 'division', None)
        self.updater.update_calc_for_single_resource_change('division', 'older_employees', 'division', None)
        self.updater.update_calc_for_single_resource_change('division', 'older_employees_called_ned', 'division', None)

        agg = self.updater.build_reverse_aggregations_to_calc('division', 'older_employees_called_ned', self.employee_spec, employee_id_2)
        self.assertEqual([[
            {"$lookup": {
                "from": "resource_division",
                "foreignField": "older_employees._id",
                "localField": "_id",
                "as": "_field_older_employees",
            }},
            {'$group': {'_id': '$_field_older_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            []], agg)

    def test_reverse_aggregation_parent_link(self):
        self.schema.add_calc(self.employee_spec, 'division_name', 'self.parent_division_employees.name')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        agg = self.updater.build_reverse_aggregations_to_calc('employee', 'division_name', self.division_spec, division_id_1)
        self.assertEqual([[
            {"$lookup": {
                "from": "resource_employee",
                "foreignField": "_parent_id",
                "localField": "_id",
                "as": "_field_parent_division_employees",
            }},
            {'$group': {'_id': '$_field_parent_division_employees'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            []], agg)

    def test_reverse_aggregation_reverse_link(self):
        self.schema.add_field(self.division_spec, 'manager', 'link', 'employee')
        self.schema.add_calc(self.employee_spec, 'divisions_i_manage', 'self.link_division_manager')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'marketting'}, 'divisions')

        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.schema.update_resource_fields('division', division_id_1, {'manager': employee_id_1})
        self.schema.update_resource_fields('division', division_id_2, {'manager': employee_id_1})

        agg = self.updater.build_reverse_aggregations_to_calc('employee', 'divisions_i_manage', self.division_spec, division_id_1)
        self.assertEqual([[
            {"$lookup": {
                "from": "resource_employee",
                "foreignField": "_id",
                "localField": "manager",
                "as": "_field_link_division_manager",
            }},
            {'$group': {'_id': '$_field_link_division_manager'}},
            {'$unwind': '$_id'},
            {'$replaceRoot': {'newRoot': '$_id'}}],
            []], agg)

        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'marketting'}, 'divisions')
        self.schema.create_linkcollection_entry('division', division_id_2, 'managers', employee_id_1)

    def test_reverse_aggregation_loopback(self):
        self.schema.add_field(self.division_spec, 'managers', 'linkcollection', 'employee')
        self.schema.add_calc(self.employee_spec, 'all_my_subordinates', 'self.link_division_managers.employees')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 21}, 'employees', 'division', division_id_1)
        employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'ned', 'age': 31}, 'employees', 'division', division_id_1)
        employee_id_3 = self.schema.insert_resource(
            'employee', {'name': 'fred', 'age': 41}, 'employees', 'division', division_id_1)
        employee_id_4 = self.schema.insert_resource(
            'employee', {'name': 'mike', 'age': 51}, 'employees', 'division', division_id_1)

        # add manager
        self.updater.create_linkcollection_entry('division', division_id_1, 'managers', employee_id_1)

        # a little unsure of this
        agg = self.updater.build_reverse_aggregations_to_calc('employee', 'all_my_subordinates', self.division_spec, division_id_1)
        self.assertEqual([[
            {'$lookup': {'as': '_field_link_division_managers',
                            'foreignField': '_id',
                            'from': 'resource_employee',
                            'localField': 'managers._id'}},
            {'$group': {'_id': '$_field_link_division_managers'}},
            {'$unwind': '$_id'},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ], []], agg)

    def test_reverse_aggregation_simple_collection(self):
        self.schema.add_calc(self.division_spec, 'all_employees', 'self.employees')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 31})

    def test_reverse_aggregation_switch(self):
        self.schema.add_calc(self.division_spec, 'all_employees', 'self.name -> ("sales": (self.employees[age > 25]), "marketting": self.employees)')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 31})

        self.assertEqual([{"_id": self.schema.decodeid(employee_id_2)}], self.db['resource_division'].find_one()['all_employees'])

    def test_reverse_aggregation_ternary(self):
        self.schema.add_calc(self.division_spec, 'all_employees', 'self.name = "sales" -> (self.employees[age > 25]) : self.employees')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 31})

        self.assertEqual([{"_id": self.schema.decodeid(employee_id_2)}], self.db['resource_division'].find_one()['all_employees'])

    def test_delete_resource_deletes_children(self):
        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 31})

        self.assertEqual(1, self.db['resource_division'].count_documents({}))
        self.assertEqual(2, self.db['resource_employee'].count_documents({}))

        self.updater.delete_resource('division', division_id_1, None, 'divisions')

        self.assertEqual(0, self.db['resource_division'].count_documents({}))
        self.assertEqual(0, self.db['resource_employee'].count_documents({}))

        self.assertEqual(0, self.db['resource_division'].count_documents({"_deleted": {"$exists": False}}))
        self.assertEqual(0, self.db['resource_employee'].count_documents({"_deleted": {"$exists": False}}))

    def test_delete_resource_deletes_links_to_resource(self):
        self.schema.add_field(self.division_spec, 'employees', 'linkcollection', 'employee')
        self.schema.add_field(self.division_spec, 'manager', 'link', 'employee')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'Fred'}, 'employees')

        self.schema.create_linkcollection_entry('division', division_id_1, 'employees', employee_id_1)
        self.schema.update_resource_fields('division', division_id_1, {'manager': employee_id_1})

        self.updater.delete_resource('employee', employee_id_1, 'root', 'employees')

        self.assertEqual({
            '_id': self.schema.decodeid(division_id_1),
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'employees': [],
            'manager': None,
            'name': 'sales'}, self.db['resource_division'].find_one())

    def test_updates_calc_linked_to_calc(self):
        self.schema.create_field('root', 'parttimers', 'collection', 'employee')

        self.schema.create_field('employee', 'income', 'int')
        self.schema.create_field('employee', 'vat', 'int')
        self.schema.create_field('employee', 'income_after_vat', 'calc', calc_str='self.income - self.vat')
        self.schema.create_field('division', 'parttimers', 'linkcollection', 'employee')
        self.schema.create_field('division', 'employee_total', 'calc', calc_str="sum(self.employees.income_after_vat)")
        self.schema.create_field('division', 'parttime_total', 'calc', calc_str="sum(self.parttimers.income_after_vat)")

        division_id_1 = self.updater.create_resource(
            'division', 'root', 'divisions', None, {'name': 'sales'})
        employee_id_1 = self.updater.create_resource(
            'employee', 'division', 'employees', division_id_1, {'name': 'Fred', 'income': 10000, 'vat': 2000})
        employee_id_2 = self.updater.create_resource(
            'employee', 'division', 'employees', division_id_1, {'name': 'Ned', 'income': 20000, 'vat': 4000})

        employee_id_3 = self.updater.create_resource(
            'employee', 'root', 'parttimers', None, {'name': 'Bob', 'income': 40000, 'vat': 8000})

        self.updater.create_linkcollection_entry('division', division_id_1, 'parttimers', employee_id_3)

        self.assertEqual(24000, self.db['resource_division'].find_one()['employee_total'])
        self.assertEqual(32000, self.db['resource_division'].find_one()['parttime_total'])

        # assert calc change propagates
        self.updater.update_fields('employee', employee_id_3, {'vat': 9000})

        self.assertEqual(24000, self.db['resource_division'].find_one()['employee_total'])
        self.assertEqual(31000, self.db['resource_division'].find_one()['parttime_total'])

    def test_update_adjacent_calc_after_update(self):
        self.schema.create_field('employee', 'division_name', 'calc', calc_str='self.parent_division_employees.name')
        self.schema.create_field('employee', 'both_names', 'calc', calc_str='self.name + self.division_name')

        division_id_1 = self.updater.create_resource(
            'division', 'root', 'divisions', None, {'name': 'sales'})
        employee_id_1 = self.updater.create_resource(
            'employee', 'division', 'employees', division_id_1, {'name': 'Fred'})

        self.assertEqual('Fredsales', self.db['resource_employee'].find_one()['both_names'])

        self.updater.update_fields('division', division_id_1, {'name': 'marketting'})

        self.assertEqual('Fredmarketting', self.db['resource_employee'].find_one({})['both_names'])

    def test_aggregate_for_sum(self):
        self.schema.create_field('employee', 'val1', 'int')
        self.schema.create_field('employee', 'val2', 'int')
        calc = self.schema.add_calc(self.employee_spec, 'total', 'self.val1 + self.val2')
        tree = self.schema.calc_trees[('employee', 'total')]

#        aggregation = tree.aggregation(None)
#        self.assertEqual([], aggregation)

        employee_id_1 = self.updater.create_resource(
            'employee', 'root', 'employees', None, {'name': 'ned', 'val1': 1, 'val2': 1})
        employee_id_2 = self.updater.create_resource(
            'employee', 'root', 'employees', None, {'name': 'bob', 'val1': 2, 'val2': 2})
        employee_id_3 = self.updater.create_resource(
            'employee', 'root', 'employees', None, {'name': 'fred', 'val1': 3, 'val2': 3})

        employee_1 = self.db.resource_employee.find_one({"_id": self.schema.decodeid(employee_id_1)})
        employee_2 = self.db.resource_employee.find_one({"_id": self.schema.decodeid(employee_id_2)})
        employee_3 = self.db.resource_employee.find_one({"_id": self.schema.decodeid(employee_id_3)})

        self.assertEqual(2, employee_1['total'])
        self.assertEqual(4, employee_2['total'])
        self.assertEqual(6, employee_3['total'])

    def test_aggregate_for_calc(self):
        self.schema.create_field('employee', 'val1', 'int')
        self.schema.create_field('employee', 'val2', 'int')
        calc = self.schema.add_calc(self.employee_spec, 'total', 'self.val1 + (self.val1 / self.val2)')
        tree = self.schema.calc_trees[('employee', 'total')]

        employee_id_1 = self.updater.create_resource(
            'employee', 'root', 'employees', None, {'name': 'ned', 'val1': 10, 'val2': 1})
        employee_id_2 = self.updater.create_resource(
            'employee', 'root', 'employees', None, {'name': 'bob', 'val1': 10, 'val2': 2})
        employee_id_3 = self.updater.create_resource(
            'employee', 'root', 'employees', None, {'name': 'fred', 'val1': 9, 'val2': 3})

        employee_1 = self.db.resource_employee.find_one({"_id": self.schema.decodeid(employee_id_1)})
        employee_2 = self.db.resource_employee.find_one({"_id": self.schema.decodeid(employee_id_2)})
        employee_3 = self.db.resource_employee.find_one({"_id": self.schema.decodeid(employee_id_3)})

        self.assertEqual(20, employee_1['total'])
        self.assertEqual(15, employee_2['total'])
        self.assertEqual(12, employee_3['total'])

    def test_update_create_update_calc(self):
        self.schema.add_calc(self.division_spec, 'age_total', 'sum(self.employees.age)')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})

        division = self.schema.db['resource_division'].find_one()
        self.assertEqual(21, division['age_total'])

    def test_update_create_update_calc_same_resource(self):
        self.schema.add_calc(self.employee_spec, 'my_next_age', 'self.age + 10')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})

        employee = self.schema.db['resource_employee'].find_one()
        self.assertEqual(31, employee['my_next_age'])

    def test_update_create_update_subsequent_calc(self):
        self.schema.add_calc(self.division_spec, 'age_total', 'sum(self.employees.age)')
        self.schema.add_calc(self.division_spec, 'age_total_plus', 'self.age_total + 1')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})

        division = self.schema.db['resource_division'].find_one()
        self.assertEqual(21, division['age_total'])
        self.assertEqual(22, division['age_total_plus'])

    def test_update_same_resource(self):
        self.schema.add_calc(self.employee_spec, 'is_old', 'self.age > 30')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 25})
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 35})

        employee_1 = self.schema.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_1)})
        self.assertEqual(False, employee_1['is_old'])
        employee_2 = self.schema.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_2)})
        self.assertEqual(True, employee_2['is_old'])

        # change field
        self.updater.update_fields("employee", employee_id_1, {'age': 45})
        employee_1 = self.schema.db['resource_employee'].find_one({"_id": self.schema.decodeid(employee_id_1)})
        self.assertEqual(True, employee_1['is_old'])

    def test_create_switch_with_collections(self):
        print("Create calc")
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.name -> ("sales": (self.employees[age>20]), "marketting": (self.employees[age>30]))')

        print("Create division")
        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        print("Create employee 1")
        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 25})
        print("Create employee 2")
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 35})

        division = self.schema.db['resource_division'].find_one()
        self.assertEqual(2, len(division['older_employees']))

        # change field
        print("Update division")
        self.updater.update_fields("division", division_id_1, {'name': 'marketting'})
        #self.updater.update_fields("employee", employee_id_1, {'age': 15})

        division = self.schema.db['resource_division'].find_one()
        self.assertEqual(1, len(division['older_employees']))
