
import unittest

from pymongo import MongoClient

from metaphor.schema import Schema
from metaphor.api import Api


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.db.metaphor_schema.insert_one({
            "specs" : {
                "employee" : {
                    "fields" : {
                        "name" : {
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
                        "primary_sections": {
                            "type": "calc",
                            "calc_str": "self.sections[name='primary']",
                        },
                        "average_section_total": {
                            "type": "calc",
                            "calc_str": "average(self.sections.section_total)",
                        },
                        "average_bracket_calc": {
                            "type": "calc",
                            "calc_str": "average(self.sections.section_total) + ((10 + sum(self.sections.section_total)) / 5)",
                        },
                        "older_employees": {
                            "type": "calc",
                            "calc_str": "self.link_employee_division[age>40]",
                        },
                    },
                },
                "section": {
                    "fields": {
                        "name": {
                            "type": "str",
                        },
                        "section_total": {
                            "type": "int",
                        },
                        "division_name": {
                            "type": "calc",
                            "calc_str": "self.parent_division_sections.name",
                        },
                        "distance_from_average": {
                            "type": "calc",
                            "calc_str": "self.section_total - average(self.parent_division_sections.sections.section_total)",
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
        self.schema.load_schema()

        self.api = Api(self.schema)

    def test_calc_results(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')
        employee_id_2 = self.schema.insert_resource('employee', {'name': 'bob', 'age': 31}, 'employees')
        employee_id_3 = self.schema.insert_resource('employee', {'name': 'fred', 'age': 21}, 'employees')

        division_id_1 = self.schema.insert_resource('division', {'name': 'sales', 'yearly_sales': 100}, 'divisions')

        self.schema.update_resource_fields('employee', employee_id_1, {'division': division_id_1})
        self.schema.update_resource_fields('employee', employee_id_2, {'division': division_id_1})

        section_id_1 = self.api.post('/divisions/%s/sections' % division_id_1, {'name': 'primary', 'section_total': 120})
        section_id_2 = self.api.post('/divisions/%s/sections' % division_id_1, {'name': 'secondary', 'section_total': 90})

        # test simple type
        section_1 = self.api.get('/divisions/%s/sections/%s' % (division_id_1, section_id_1))
        self.assertEquals('sales', section_1['division_name'])

        # test resource type
        division_1 = self.api.get('/divisions/%s' % division_id_1)
        self.assertEquals({
            'id': division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'name': 'sales',
            'older_employees': [{
                'age': 41,
                'division': '/divisions/%s' % division_id_1,
                'id': employee_id_1,
                'name': 'ned',
                'self': '/employees/%s' % employee_id_1}],
            'average_section_total': 105.0,
            'average_bracket_calc': 149.0,
            'primary_sections': [
                {'id': section_id_1,
                 'distance_from_average': 15.0,
                 'self': '/divisions/%s/sections/%s' % (division_id_1, section_id_1),
                 'division_name': 'sales',
                 'parent_division_sections': '/divisions/%s' % division_id_1,
                 'section_total': 120,
                 'name': 'primary'}
             ],
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}, division_1)

        # test calculated resource collection endpoint
        older_employees = self.api.get('/divisions/%s/older_employees' % division_id_1)
        self.assertEquals(1, len(older_employees))
