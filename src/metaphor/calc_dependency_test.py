
import unittest
from datetime import datetime

from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.schema import Field


class CalcDependencyTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')
        self.schema.create_field('employee', 'salary', 'int')

        self.section_spec = self.schema.create_spec('section')
        self.schema.create_field('section', 'name', 'str')
        self.schema.create_field('section', 'employees', 'collection', 'employee')
        self.schema.create_field('section', 'total_age', 'calc', calc_str="sum(self.employees.age)")

        self.org_spec = self.schema.create_spec('org')
        self.schema.create_field('org', 'name', 'str')
        self.schema.create_field('org', 'employees', 'linkcollection', 'employee')
        self.schema.create_field('org', 'total_salaries', 'calc', calc_str="sum(self.employees.salary)")

    def test_deps(self):
        self.assertEqual(['age', 'salary'], sorted(self.schema._fields_with_dependant_calcs('employee')))
