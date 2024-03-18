
import unittest

from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater
from metaphor.lrparse.lrparse import parse


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
        self.schema.create_field('division', 'employees', 'linkcollection', 'employee')
        self.schema.create_field('division',  'older_employees', 'calc', calc_str= 'self.employees[age>9]')

        self.schema.create_field('root', 'divisions', 'collection', 'division')
        self.schema.create_field('root', 'employees', 'collection', 'employee')

    def test_update_only_linked_resources(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 10}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 10}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales'}, 'divisions')
        division_id_2 = self.schema.insert_resource('division', {'name': 'marketting'}, 'divisions')

        self.updater.create_linkcollection_entry('division', division_id_1, 'employees', employee_id_1)
        self.updater.create_linkcollection_entry('division', division_id_2, 'employees', employee_id_1)
        self.updater.create_linkcollection_entry('division', division_id_2, 'employees', employee_id_2)

        employee_1 = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(employee_id_1)})
        employee_2 = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(employee_id_2)})

        division_1 = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(division_id_1)})
        division_2 = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(division_id_2)})

        self.assertEqual([{"_id": self.schema.decodeid(employee_id_1)}], division_1['older_employees'])
        self.assertEqual([{"_id": self.schema.decodeid(employee_id_1)}, {"_id": self.schema.decodeid(employee_id_2)}], division_2['older_employees'])
