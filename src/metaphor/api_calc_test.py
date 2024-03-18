
import unittest

from metaphor.mongoclient_testutils import mongo_connection

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

        self._create_test_schema({
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
                        "division_link": {
                            "type": "calc",
                            "calc_str": "self.division",
                        },
                        "parttime_division_name": {
                            "type": "calc",
                            "calc_str": "self.name + (self.parent_section_parttimers.parent_division_sections.name)",
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
                            "type": "orderedcollection",
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
                        "parttimers": {
                            "type": "orderedcollection",
                            "target_spec_name": "employee",
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


        self.api = Api(self.db)

    def _create_test_schema(self, data):
        data['current'] = True
        data['version'] = 'test'
        data['root'] = data.get('root', {})
        inserted = self.db.metaphor_schema.insert_one(data)
        self.schema = SchemaFactory(self.db).load_current_schema()

    def test_calc_results(self):
        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})
        employee_id_2 = self.api.post('employees', {'name': 'bob', 'age': 31})
        employee_id_3 = self.api.post('employees', {'name': 'fred', 'age': 21})

        division_id_1 = self.api.post('divisions', {'name': 'sales', 'yearly_sales': 100})

        self.api.patch('employees/%s' % employee_id_1, {'division': division_id_1})
        self.api.patch('employees/%s' % employee_id_2, {'division': division_id_1})

        section_id_1 = self.api.post('/divisions/%s/sections' % division_id_1, {'name': 'primary', 'section_total': 120})
        section_id_2 = self.api.post('/divisions/%s/sections' % division_id_1, {'name': 'secondary', 'section_total': 90})

        # test simple type
        section_1 = self.api.get('/divisions/%s/sections/%s' % (division_id_1, section_id_1))

        self.assertEqual('sales', section_1['division_name'])

        # test resource type
        division_1 = self.api.get('/divisions/%s' % division_id_1)
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'division'}},
            'id': division_id_1,
            'link_employee_division': '/divisions/%s/link_employee_division' % division_id_1,
            'name': 'sales',
            'average_section_total': 105.0,
            'average_bracket_calc': 149.0,
            'older_employees': '/divisions/%s/older_employees' % division_id_1,
            'primary_sections': '/divisions/%s/primary_sections' % division_id_1,
            'sections': '/divisions/%s/sections' % division_id_1,
            'self': '/divisions/%s' % division_id_1,
            'yearly_sales': 100}, division_1)

        # test calculated resource collection endpoint
        older_employees = self.api.get('/divisions/%s/older_employees' % division_id_1)['results']
        self.assertEqual(1, len(older_employees))

    def test_calc_link_1(self):
        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41})

    def test_calc_link(self):
        division_id_1 = self.api.post('divisions', {'name': 'sales', 'yearly_sales': 100})
        employee_id_1 = self.api.post('employees', {'name': 'ned', 'age': 41, 'division': division_id_1})

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'employee'}},
            'age': 41,
            'division': '/divisions/%s' % division_id_1,
            'division_link': {"_id": self.schema.decodeid(division_id_1)},
            'id': employee_id_1,
            'name': 'ned',
            'parent_section_parttimers': None,
            'parttime_division_name': 'ned',
            'self': '/employees/%s' % employee_id_1}
            , self.api.get('/employees/%s' % employee_id_1))

    def test_calc_parent_links(self):
        division_id_1 = self.api.post('divisions', {'name': 'sales', 'yearly_sales': 100})
        section_1 = self.api.post('/divisions/%s/sections' % (division_id_1,), {'name': 'hr'})
        parttimer_1 = self.api.post('/divisions/%s/sections/%s/parttimers' % (division_id_1, section_1), {'name': 'bob'})

        parttimer = self.api.get('/divisions/%s/sections/%s/parttimers/%s' % (division_id_1, section_1, parttimer_1))

        self.assertEqual('bobsales', parttimer['parttime_division_name'])
