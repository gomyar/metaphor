
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.updater import Updater


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.updater = Updater(self.schema)

        self.employee_spec = self.schema.add_spec('employee')
        self.schema.add_field(self.employee_spec, 'name', 'str')
        self.schema.add_field(self.employee_spec, 'age', 'int')

        self.division_spec = self.schema.add_spec('division')
        self.schema.add_field(self.division_spec, 'name', 'str')
        self.schema.add_field(self.division_spec, 'employees', 'collection', 'employee')

        self.schema.add_field(self.schema.root, 'divisions', 'collection', 'division')

    def test_updater(self):
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.employees[age>30]')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.updater.update_calc('division', 'older_employees', division_id_1)

        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_grants': [],
            '_canonical_url': '/divisions/%s' % division_id_1,
            'name': 'sales',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            'older_employees': [ObjectId(employee_id_1[2:])],
        }, division_data)

        employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'Ned', 'age': 41}, 'employees', 'division', division_id_1)

        # check again
        self.updater.update_calc('division', 'older_employees', division_id_1)
        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_grants': [],
            '_canonical_url': '/divisions/%s' % division_id_1,
            'name': 'sales',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            'older_employees': [ObjectId(employee_id_1[2:]), ObjectId(employee_id_2[2:])],
        }, division_data)

    def test_reverse_aggregation(self):
        self.schema.add_calc(self.division_spec, 'older_employees', 'self.employees[age>30]')
        self.schema.add_calc(self.division_spec, 'average_age', 'average(self.employees.age)')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_2 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_2)

        average_agg = self.updater.build_reverse_aggregations_to_calc('division', 'average_age', self.employee_spec, employee_id_1)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(employee_id_1)}},
            {"$lookup": {
                "from": "resource_division",
                "localField": "_parent_id",
                "foreignField": "_id",
                "as": "_field_employees",
            }},
            {'$group': {'_id': '$_field_employees'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], average_agg)

        affected_ids = self.updater.get_affected_ids_for_resource('division', 'average_age', self.employee_spec, employee_id_1)
        self.assertEquals([self.schema.decodeid(division_id_1)], list(affected_ids))

        # check another collection
        employee_id_3 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_2)

        affected_ids = self.updater.get_affected_ids_for_resource('division', 'average_age', self.employee_spec, employee_id_3)
        self.assertEquals([self.schema.decodeid(division_id_2)], list(affected_ids))

        # different calc
        older_agg = self.updater.build_reverse_aggregations_to_calc('division', 'older_employees', self.employee_spec.fields['age'], employee_id_1)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(employee_id_1)}},
            {"$lookup": {
                "from": "resource_division",
                "localField": "_parent_id",
                "foreignField": "_id",
                "as": "_field_employees",
            }},
            {'$group': {'_id': '$_field_employees'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], average_agg)

    def test_reverse_aggregation_link(self):
        self.schema.add_field(self.division_spec, 'manager', 'link', 'employee')
        self.schema.add_calc(self.division_spec, 'manager_age', 'self.manager.age')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.schema.update_resource_fields('division', division_id_1, {'manager': employee_id_1})

        agg = self.updater.build_reverse_aggregations_to_calc('division', 'manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(employee_id_1)}},
            {"$lookup": {
                "from": "resource_division",
                "localField": "_id",
                "foreignField": "manager",
                "as": "_field_manager",
            }},
            {'$group': {'_id': '$_field_manager'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], agg)

        # check affected ids
        affected_ids = self.updater.get_affected_ids_for_resource('division', 'manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([self.schema.decodeid(division_id_1)], list(affected_ids))

        # check having two links
        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        self.schema.update_resource_fields('division', division_id_2, {'manager': employee_id_1})

        affected_ids = self.updater.get_affected_ids_for_resource('division', 'manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([self.schema.decodeid(division_id_1), self.schema.decodeid(division_id_2)], list(affected_ids))

    def test_reverse_aggregation_link_collection(self):
        self.schema.add_field(self.division_spec, 'managers', 'linkcollection', 'employee')
        self.schema.add_calc(self.division_spec, 'average_manager_age', 'average(self.managers.age)')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.schema.create_linkcollection_entry('division', division_id_1, 'managers', employee_id_1)

        agg = self.updater.build_reverse_aggregations_to_calc('division', 'average_manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(employee_id_1)}},
            {"$lookup": {
                "from": "resource_division",
                "foreignField": "managers._id",
                "localField": "_id",
                "as": "_field_managers",
            }},
            {'$group': {'_id': '$_field_managers'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], agg)

        # check affected ids
        affected_ids = self.updater.get_affected_ids_for_resource('division', 'average_manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([self.schema.decodeid(division_id_1)], list(affected_ids))

        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'marketting'}, 'divisions')
        self.schema.create_linkcollection_entry('division', division_id_2, 'managers', employee_id_1)

        affected_ids = self.updater.get_affected_ids_for_resource('division', 'average_manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([self.schema.decodeid(division_id_1), self.schema.decodeid(division_id_2)], list(affected_ids))

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

        self.updater.update_calc('division', 'older_employees', division_id_1)
        self.updater.update_calc('division', 'older_employees_called_ned', division_id_1)
        self.updater.update_calc('division', 'older_employees', division_id_2)
        self.updater.update_calc('division', 'older_employees_called_ned', division_id_2)

        agg = self.updater.build_reverse_aggregations_to_calc('division', 'older_employees_called_ned', self.employee_spec, employee_id_2)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(employee_id_2)}},
            {"$lookup": {
                "from": "resource_division",
                "foreignField": "older_employees",
                "localField": "_id",
                "as": "_field_older_employees",
            }},
            {'$group': {'_id': '$_field_older_employees'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], agg)

        # check affected ids
        affected_ids = self.updater.get_affected_ids_for_resource('division', 'older_employees_called_ned', self.employee_spec, employee_id_2)
        self.assertEquals([self.schema.decodeid(division_id_1)], list(affected_ids))

    def test_reverse_aggregation_parent_link(self):
        self.schema.add_calc(self.employee_spec, 'division_name', 'self.parent_division_employees.name')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_1)

        self.updater.update_calc('employee', 'division_name', employee_id_1)

        agg = self.updater.build_reverse_aggregations_to_calc('employee', 'division_name', self.division_spec, division_id_1)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(division_id_1)}},
            {"$lookup": {
                "from": "resource_employee",
                "foreignField": "_parent_id",
                "localField": "_id",
                "as": "_field_parent_division_employees",
            }},
            {'$group': {'_id': '$_field_parent_division_employees'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], agg)

        # check affected ids
        affected_ids = self.updater.get_affected_ids_for_resource('employee', 'division_name', self.division_spec, division_id_1)
        self.assertEquals([self.schema.decodeid(employee_id_1)], list(affected_ids))

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

        self.updater.update_calc('employee', 'divisions_i_manage', employee_id_1)

        agg = self.updater.build_reverse_aggregations_to_calc('employee', 'divisions_i_manage', self.division_spec, division_id_1)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(division_id_1)}},
            {"$lookup": {
                "from": "resource_employee",
                "foreignField": "_id",
                "localField": "manager",
                "as": "_field_link_division_manager",
            }},
            {'$group': {'_id': '$_field_link_division_manager'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], agg)

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

        # bobs addition alters bobs calc
        self.assertEquals([self.schema.decodeid(employee_id_1)],
            list(self.updater.get_affected_ids_for_resource('employee', 'all_my_subordinates', self.employee_spec, employee_id_1)))

        # a little unsure of this
        agg = self.updater.build_reverse_aggregations_to_calc('employee', 'all_my_subordinates', self.division_spec, division_id_1)
        self.assertEquals([[
            {"$match": {"_id": self.schema.decodeid(division_id_1)}},
            {"$lookup": {
                "from": "resource_division",
                "foreignField": "_id",
                "localField": "_parent_id",
                "as": "_field_employees",
            }},
            {'$group': {'_id': '$_field_employees'}},
            {"$unwind": "$_id"},
            {'$replaceRoot': {'newRoot': '$_id'}},
            {'$lookup': {'as': '_field_link_division_managers',
                            'foreignField': '_id',
                            'from': 'resource_employee',
                            'localField': 'managers._id'}},
            {'$group': {'_id': '$_field_link_division_managers'}},
            {'$unwind': '$_id'},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]], agg)

    def test_reverse_aggregation_simple_collection(self):
        self.schema.add_calc(self.division_spec, 'all_employees', 'self.employees')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 31})

        self.assertEquals(
            [self.schema.decodeid(division_id_1)],
            list(self.updater.get_affected_ids_for_resource('division', 'all_employees', self.employee_spec, employee_id_1)))
