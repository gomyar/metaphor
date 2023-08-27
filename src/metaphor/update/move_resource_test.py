
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.updater import Updater

from metaphor.update.move_resource import MoveResourceUpdate


class MoveResourceTest(unittest.TestCase):
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

        self.schema.create_field('root', 'current_employees', 'collection', 'employee')
        self.schema.create_field('root', 'former_employees', 'collection', 'employee')

        self.calcs_spec = self.schema.create_spec('calcs')
