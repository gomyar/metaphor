
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater

import logging
log = logging.getLogger('metaphor.test')


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
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

    def test_update_simple_field(self):
        self.schema.create_field('division',  'older_employees', 'calc', calc_str= 'self.employees[age>30]')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob',
            'age': 31,
        })

        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            'name': 'sales',
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'older_employees': [{"_id": ObjectId(employee_id_1[2:])}],
        }, division_data)

        self.updater.update_fields('employee', employee_id_1, {"age": 20})

        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            'name': 'sales',
            '_canonical_url': '/divisions/%s' % division_id_1,
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'older_employees': [],
        }, division_data)

    def test_update_containing_collection(self):
        self.schema.create_field('division',  'older_employees', 'calc', calc_str= 'self.employees[age>30]')

        division_id_1 = self.updater.create_resource(
            'division', None, 'divisions', None, {'name': 'sales'})

        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            '_canonical_url': '/divisions/%s' % division_id_1,
            'name': 'sales',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'older_employees': [],
        }, division_data)

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'Bob',
            'age': 41,
        })

        division_data = self.db.resource_division.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            '_canonical_url': '/divisions/%s' % division_id_1,
            'name': 'sales',
            '_parent_canonical_url': '/',
            '_parent_field_name': 'divisions',
            '_parent_id': None,
            '_parent_type': 'root',
            '_type': 'division',
            'older_employees': [{"_id": ObjectId(employee_id_1[2:])}],
        }, division_data)

        employee_data = self.db.resource_employee.find_one()
        self.assertEquals({
            '_id': self.schema.decodeid(employee_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            '_canonical_url': '/divisions/%s/employees/%s' % (division_id_1, employee_id_1),
            'name': 'Bob',
            'age': 41,
            '_parent_canonical_url': '/divisions/%s' % division_id_1,
            '_parent_field_name': 'employees',
            '_parent_id': ObjectId(division_id_1[2:]),
            '_type': 'employee',
            '_parent_type': 'division',
        }, employee_data)

    def test_update_link_collection(self):
        self.schema.create_field('division', 'managers', 'linkcollection', 'employee')
        self.schema.create_field('division',  'older_managers', 'calc', calc_str= 'self.managers[age>30]')
        self.schema.create_field('division',  'older_non_retired_managers', 'calc', calc_str= 'self.older_managers[age<65]')
        log.debug("start")

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')
        log.debug("inserted")

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'Bob',
            'age': 41
        })
        log.debug("created 1")
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'Ned',
            'age': 70
        })
        log.debug("created 2")
        employee_id_3 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'Fred',
            'age': 25
        })
        log.debug("created 3")

        self.updater.create_linkcollection_entry('division', division_id_1, 'managers', employee_id_1)
        log.debug("created entry 1")
        self.updater.create_linkcollection_entry('division', division_id_1, 'managers', employee_id_2)
        log.debug("created entry 2")
        self.updater.create_linkcollection_entry('division', division_id_1, 'managers', employee_id_3)
        log.debug("created entry 3")

        division_data = self.db.resource_division.find_one()
        self.assertEquals("sales", division_data['name'])
        self.assertEquals(3, len(division_data['managers']))
        self.assertTrue({"_id" : self.schema.decodeid(employee_id_1)} in division_data['managers'])
        self.assertTrue({"_id" : self.schema.decodeid(employee_id_2)} in division_data['managers'])
        self.assertTrue({"_id" : self.schema.decodeid(employee_id_3)} in division_data['managers'])
        self.assertEquals(sorted([
            self.schema.decodeid(employee_id_1),
            self.schema.decodeid(employee_id_2),
        ]), sorted([m['_id'] for m in division_data['older_managers']]))
        self.assertEquals([
            {"_id": self.schema.decodeid(employee_id_1)}],
            division_data['older_non_retired_managers'])
        self.assertEquals({
            "_id" : self.schema.decodeid(division_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            '_canonical_url': '/divisions/%s' % division_id_1,
            "_parent_field_name" : "divisions",
            "_parent_id" : None,
            "_parent_type" : "root",
            "_parent_canonical_url" : '/',
            '_type': 'division',
            "name" : "sales",
            "managers" : [
                    {
                            "_id" : self.schema.decodeid(employee_id_1)
                    },
                    {
                            "_id" : self.schema.decodeid(employee_id_2)
                    },
                    {
                            "_id" : self.schema.decodeid(employee_id_3)
                    }
            ],
            "older_managers" : [
                    {"_id": self.schema.decodeid(employee_id_1)},
                    {"_id": self.schema.decodeid(employee_id_2)},
            ],
            "older_non_retired_managers" : [
                    {"_id": self.schema.decodeid(employee_id_1)},
            ]
        }, division_data)

    def test_reverse_aggregation_loopback(self):
        self.schema.create_field('division', 'managers', 'linkcollection', 'employee')
        self.schema.create_field('employee',  'all_my_subordinates', 'calc', calc_str= 'self.link_division_managers.employees')

        division_id_1 = self.schema.insert_resource(
            'division', {'name': 'sales'}, 'divisions')

        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'bob', 'age': 21})
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'ned', 'age': 31})
        employee_id_3 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'fred', 'age': 41})
        employee_id_4 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {
            'name': 'mike', 'age': 51})

        # add manager
        calc_spec = self.schema.calc_trees[('employee', 'all_my_subordinates')]
        self.assertEquals({'division.managers', 'division.employees'}, calc_spec.get_resource_dependencies())
        self.updater.create_linkcollection_entry('division', division_id_1, 'managers', employee_id_1)

        employee_data = self.db.resource_employee.find_one()
        self.assertEquals({
            "_id" : self.schema.decodeid(employee_id_1),
            '_schema_id': self.schema._id,
            '_grants': [],
            '_dirty': {},
            '_canonical_url': '/divisions/%s/employees/%s' % (division_id_1, employee_id_1),
            "_parent_field_name" : "employees",
            "_parent_id" : self.schema.decodeid(division_id_1),
            "_parent_type" : "division",
            "_parent_canonical_url" : "/divisions/%s" % division_id_1,
            '_type': 'employee',
            "name" : "bob",
            "age": 21,
            "all_my_subordinates" : [
                {"_id": self.schema.decodeid(employee_id_1)},
                {"_id": self.schema.decodeid(employee_id_2)},
                {"_id": self.schema.decodeid(employee_id_3)},
                {"_id": self.schema.decodeid(employee_id_4)},
            ]}, employee_data)

    def test_resource_deps_for_field(self):
        self.schema.create_field('employee',  'all_ages', 'calc', calc_str= 'divisions.employees.age + 10')

        # add manager
        calc_spec = self.schema.calc_trees[('employee', 'all_ages')]
        self.assertEquals({'division.employees', 'root.divisions', 'employee.age'}, calc_spec.get_resource_dependencies())
