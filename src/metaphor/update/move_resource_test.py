
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.updater import Updater

from metaphor.api import Api


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

        self.career_spec = self.schema.create_spec('career')
        self.schema.create_field('career', 'name', 'str')

        self.schema.create_field('employee', 'careers', 'collection', 'career')

        self.sector_spec = self.schema.create_spec('sector')
        self.schema.create_field('sector', 'name', 'str')

        self.schema.create_field('career', 'sectors', 'collection', 'sector')

        self.schema.create_field('root', 'current_employees', 'collection', 'employee')
        self.schema.create_field('root', 'former_employees', 'collection', 'employee')

        self.calcs_spec = self.schema.create_spec('calcs')

        self.api = Api(self.db)

    def test_move(self):
        employee_id_1 = self.api.post("/current_employees", {"name": "Bob", "age": 40})
        career_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers", {"name": "salesman"})
        sector_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers/{career_id_1}/sectors", {"name": "vacuums"})

        employee_id_2 = self.api.post("/current_employees", {"name": "Ned", "age": 40})
        career_id_2 = self.api.post(f"/current_employees/{employee_id_2}/careers", {"name": "workman"})
        sector_id_2 = self.api.post(f"/current_employees/{employee_id_2}/careers/{career_id_2}/sectors", {"name": "building"})

        self.api.put("/former_employees", {"_from": "/current_employees"})

        self.assertEqual(0, self.api.get("/current_employees")['count'])
        self.assertEqual(2, self.api.get("/former_employees")['count'])
