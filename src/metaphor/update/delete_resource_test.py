
import unittest
from datetime import datetime
from urllib.error import HTTPError

from pymongo import MongoClient

from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api, create_expand_dict


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.create_initial_schema()
        self.schema.set_as_current()

        self.schema.create_spec('country')
        self.schema.create_field('country', 'name', 'str')

        self.schema.create_spec('state')
        self.schema.create_field('state', 'name', 'str')

        self.schema.create_spec('city')
        self.schema.create_field('city', 'name', 'str')
        self.schema.create_field('city', 'population', 'int')

        self.schema.create_field('country', 'states', 'collection', 'state')
        self.schema.create_field('state', 'cities', 'collection', 'city')

        self.schema.create_spec('treaty')
        self.schema.create_field('treaty', 'name', 'str')
        self.schema.create_field('treaty', 'countries', 'linkcollection', 'country')

        self.schema.create_field('treaty', 'total_population', 'calc', calc_str='sum(self.countries.states.cities.population)')

        self.schema.create_field('root', 'countries', 'collection', 'country')
        self.schema.create_field('root', 'treaties', 'collection', 'treaty')

        self.api = Api(self.db)

    def test_delete_resource(self):
        country_1 = self.api.post('/countries', {'name': 'Latveria'})
        country_2 = self.api.post('/countries', {'name': 'Genosha'})

        treaty_1 = self.api.post('/treaties', {'name': 'Treaty'})
        self.api.post(f'/treaties/{treaty_1}/countries', {'id': country_1})
        self.api.post(f'/treaties/{treaty_1}/countries', {'id': country_2})

        state_1 = self.api.post(f'/countries/{country_1}/states', {'name': 'Doomland'})
        city_1 = self.api.post(f'/countries/{country_1}/states/{state_1}/cities', {'name': 'Doomopolis', 'population': 1})

        state_2 = self.api.post(f'/countries/{country_2}/states', {'name': 'McCoy'})
        city_2 = self.api.post(f'/countries/{country_2}/states/{state_2}/cities', {'name': 'Hanksville', 'population': 1})

        treaty = self.api.get(f'/treaties/{treaty_1}')
        self.assertEqual(2, treaty['total_population'])

        self.api.delete(f'/countries/{country_1}')

        treaty = self.api.get(f'/treaties/{treaty_1}')
        self.assertEqual(1, treaty['total_population'])
