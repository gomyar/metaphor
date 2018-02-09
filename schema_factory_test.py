

import unittest

from pymongo import MongoClient

from metaphor.schema_factory import SchemaFactory
from metaphor.schema import Schema
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec


def _test_func(res):
    pass


class SchemaFactoryTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db

        self.factory = SchemaFactory()

    def test_create_schema(self):
        schema = self.factory.create_schema(self.db, "1.1", {'specs': {}, 'roots': {}})

        self.assertEquals(1, len(schema.specs))
        self.assertEquals(['root'], schema.specs.keys())

    def test_basic_create_resource(self):
        schema = self.factory.create_schema(self.db, "1.1", {
            'specs': {
                'company': {'type': 'resource', 'fields': {
                            'name': {'type': 'str'},
                }},
            },
            'roots': {
                'companies': {'type': 'collection', 'target': 'company'}
            }
        })
        self.assertEquals(2, len(schema.specs))
        self.assertEquals(['company', 'root'], sorted(schema.specs.keys()))

        self.assertEquals(['companies'], schema.specs['root'].fields.keys())
        self.assertEquals('str', schema.specs['company'].fields['name'].field_type)

    def test_basic_create_collection(self):
        schema = self.factory.create_schema(self.db, "1.1", {
            'specs': {
                'company': {'type': 'resource', 'fields': {
                            'name': {'type': 'str'},
                            'periods': {'type': 'collection', 'target': 'period'},
                }},
                'period': {'type': 'resource', 'fields': {
                            'year': {'type': 'int'},
                }},
            },
            'roots': {
                'companies': {'type': 'collection', 'target': 'company'}
            }
        })
        self.assertEquals(3, len(schema.specs))
        self.assertEquals(['company', 'period', 'root'], sorted(schema.specs.keys()))

        self.assertEquals(['companies'], schema.specs['root'].fields.keys())
        self.assertEquals('str', schema.specs['company'].fields['name'].field_type)
        self.assertEquals('int', schema.specs['period'].fields['year'].field_type)
        self.assertEquals('period', schema.specs['company'].fields['periods'].target_spec_name)

    def test_basic_create_link(self):
        schema = self.factory.create_schema(self.db, "1.1", {
            'specs': {
                'company': {'type': 'resource', 'fields': {
                            'name': {'type': 'str'},
                            'latestPeriod': {'type': 'link', 'target': 'period'},
                }},
                'period': {'type': 'resource', 'fields': {
                            'year': {'type': 'int'},
                }},
            },
            'roots': {
                'companies': {'type': 'collection', 'target': 'company'}
            }
        })
        self.assertEquals(3, len(schema.specs))
        self.assertEquals(['company', 'period', 'root'], sorted(schema.specs.keys()))

        self.assertEquals(['companies'], schema.specs['root'].fields.keys())
        self.assertEquals('str', schema.specs['company'].fields['name'].field_type)
        self.assertEquals('int', schema.specs['period'].fields['year'].field_type)
        self.assertEquals('period', schema.specs['company'].fields['latestPeriod'].name)

    def test_basic_create_calc(self):
        schema = self.factory.create_schema(self.db, "1.1", {
            'specs': {
                'company': {'type': 'resource', 'fields': {
                            'name': {'type': 'str'},
                            'otherName': {'type': 'calc', 'calc': 'self.name'},
                }},
            },
            'roots': {
                'companies': {'type': 'collection', 'target': 'company'}
            }
        })
        self.assertEquals(2, len(schema.specs))
        self.assertEquals(['company', 'root'], sorted(schema.specs.keys()))

        self.assertEquals(['companies'], schema.specs['root'].fields.keys())
        self.assertEquals('str', schema.specs['company'].fields['name'].field_type)
        self.assertEquals('self.name', schema.specs['company'].fields['otherName'].calc_str)

    def test_save_load(self):
        schema = Schema(self.db, "1.2")
        test_spec = ResourceSpec('test_spec')
        test_spec.add_field("name", FieldSpec("str"))

        schema.add_resource_spec(test_spec)
        schema.add_root('specs', CollectionSpec('test_spec'))

        schema.register_function('test_func', _test_func)

        self.factory.save_schema(schema)

        schema_data = self.db['metaphor_schema'].find_one()
        self.assertEquals({'specs': {'target': 'test_spec', 'type': 'collection'}}, schema_data['roots'])
        self.assertEquals({'test_spec': {'fields': {'name': {'type': 'str'}},
                                         'type': 'resource'}}, schema_data['specs'])
        self.assertEquals({'test_func': 'schema_factory_test._test_func'}, schema_data['registered_functions'])

        schema2 = self.factory.load_schema(self.db)

        self.assertEquals("str", schema2.specs['test_spec'].fields['name'].field_type)
        self.assertEquals(_test_func, schema2._functions['test_func'])
