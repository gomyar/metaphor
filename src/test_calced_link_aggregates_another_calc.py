
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.calclang import parser
from metaphor.schema import Schema
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec, LinkCollectionSpec, CalcSpec
from metaphor.resource import ResourceLinkSpec, AggregateResource, AggregateField
from metaphor.api import MongoApi
from metaphor.resource import Resource


class ResourceCalcTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.outlet_spec = ResourceSpec('outlet')
        self.employee_spec = ResourceSpec('employee')

        self.schema.add_resource_spec(self.outlet_spec)
        self.schema.add_resource_spec(self.employee_spec)

        self.outlet_spec.add_field("name", FieldSpec("str"))
        self.outlet_spec.add_field("employees", LinkCollectionSpec('employee'))
        self.outlet_spec.add_field("managers", CalcSpec('self.employees[title="manager"]', 'employee', True))
        self.outlet_spec.add_field("average_manager_years", CalcSpec('average(self.managers.years)', 'int'))

        self.employee_spec.add_field("name", FieldSpec("str"))
        self.employee_spec.add_field("title", FieldSpec("str"))
        self.employee_spec.add_field("years", FieldSpec("int"))

        self.schema.add_root('outlets', CollectionSpec('outlet'))
        self.schema.add_root('employees', CollectionSpec('employee'))

        self.api = MongoApi('server', self.schema, self.db)

    def test_indirect_calc_links(self):
        outlet_id = self.api.post('outlets', {'name': 'Tiffanys'})
        employee_id_1 = self.api.post('employees', {'name': 'Bob', 'title': 'receptionist', 'years': 10})
        employee_id_2 = self.api.post('employees', {'name': 'Ned', 'title': 'manager', 'years': 20})
        employee_id_3 = self.api.post('employees', {'name': 'Fred', 'title': 'receptionist', 'years': 9})
        employee_id_4 = self.api.post('employees', {'name': 'Bill', 'title': 'manager', 'years': 19})

        self.api.post("outlets/%s/employees" % outlet_id, {"id": employee_id_1})
        self.api.post("outlets/%s/employees" % outlet_id, {"id": employee_id_2})
        self.api.post("outlets/%s/employees" % outlet_id, {"id": employee_id_3})
        self.api.post("outlets/%s/employees" % outlet_id, {"id": employee_id_4})

        outlet = self.api.get('outlets/%s' % (outlet_id,))

        self.assertEquals(19.5, outlet['average_manager_years'])
