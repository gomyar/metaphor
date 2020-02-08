
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

        affected_calcs = self.updater.get_calcs_affected_by_field('employee', 'age')
        self.assertEquals({('division', 'older_employees'), ('division', 'average_age')}, affected_calcs)

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
        self.assertEquals([self.schema.decodeid(division_id_1)], affected_ids)

        # check another collection
        employee_id_3 = self.schema.insert_resource(
            'employee', {'name': 'bob', 'age': 31}, 'employees', 'division', division_id_2)

        affected_ids = self.updater.get_affected_ids_for_resource('division', 'average_age', self.employee_spec, employee_id_3)
        self.assertEquals([self.schema.decodeid(division_id_2)], affected_ids)

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
        self.assertEquals([self.schema.decodeid(division_id_1)], affected_ids)

        # check having two links
        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        self.schema.update_resource_fields('division', division_id_2, {'manager': employee_id_1})

        affected_ids = self.updater.get_affected_ids_for_resource('division', 'manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([self.schema.decodeid(division_id_1), self.schema.decodeid(division_id_2)], affected_ids)

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
        self.assertEquals([self.schema.decodeid(division_id_1)], affected_ids)

        division_id_2 = self.schema.insert_resource(
            'division', {'name': 'marketting'}, 'divisions')
        self.schema.create_linkcollection_entry('division', division_id_2, 'managers', employee_id_1)

        affected_ids = self.updater.get_affected_ids_for_resource('division', 'average_manager_age', self.employee_spec, employee_id_1)
        self.assertEquals([self.schema.decodeid(division_id_1), self.schema.decodeid(division_id_2)], affected_ids)
