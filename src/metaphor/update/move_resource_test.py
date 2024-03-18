
import unittest

from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from urllib.error import HTTPError

from metaphor.schema_factory import SchemaFactory
from metaphor.updater import Updater

from metaphor.api import Api


class MoveResourceTest(unittest.TestCase):
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

        self.career_spec = self.schema.create_spec('career')
        self.schema.create_field('career', 'name', 'str')

        self.schema.create_field('employee', 'careers', 'collection', 'career')

        self.sector_spec = self.schema.create_spec('sector')
        self.schema.create_field('sector', 'name', 'str')
        self.schema.create_field('sector', 'count', 'int')

        self.schema.create_field('career', 'sectors', 'collection', 'sector')

        self.schema.create_field('employee', 'count_sector', 'calc', calc_str='sum(self.careers.sectors.count)')

        self.schema.create_field('root', 'current_employees', 'collection', 'employee')
        self.schema.create_field('root', 'former_employees', 'collection', 'employee')

        self.schema.create_spec('current_sector')
        self.schema.create_field('current_sector', 'referenced_sectors', 'linkcollection', 'sector')

        self.schema.create_field('root', 'current_sectors', 'collection', 'current_sector')

        self.api = Api(self.db)

    def test_move(self):
        employee_id_1 = self.api.post("/current_employees", {"name": "Bob", "age": 40})
        career_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers", {"name": "salesman"})
        sector_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers/{career_id_1}/sectors", {"name": "vacuums", "count": 1})

        employee_id_2 = self.api.post("/current_employees", {"name": "Ned", "age": 40})
        career_id_2 = self.api.post(f"/current_employees/{employee_id_2}/careers", {"name": "workman"})
        sector_id_2 = self.api.post(f"/current_employees/{employee_id_2}/careers/{career_id_2}/sectors", {"name": "building", "count": 1})

        self.assertEqual(1, self.api.get(f"/current_employees/{employee_id_1}")['count_sector'])

        self.api.put("/former_employees", {"_from": "/current_employees"})

        self.assertEqual(0, self.api.get("/current_employees")['count'])
        self.assertEqual(2, self.api.get("/former_employees")['count'])

    def test_move_children_update(self):
        employee_id_1 = self.api.post("/current_employees", {"name": "Bob", "age": 40})
        career_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers", {"name": "salesman"})
        sector_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers/{career_id_1}/sectors", {"name": "vacuums", "count": 1})
        career_id_2 = self.api.post(f"/current_employees/{employee_id_1}/careers", {"name": "workman"})
        sector_id_2 = self.api.post(f"/current_employees/{employee_id_1}/careers/{career_id_2}/sectors", {"name": "building", "count": 1})

        employee_id_2 = self.api.post("/current_employees", {"name": "Ned", "age": 40})

        # check calc correct initially
        self.assertEqual(2, self.api.get(f"/current_employees/{employee_id_1}")['count_sector'])

        # move a resource
        self.api.put(f"/current_employees/{employee_id_2}/careers", {"_from": f"/current_employees/{employee_id_1}/careers/{career_id_1}"})

        # check resource moved
        moved_resource = self.api.get(f"/current_employees/{employee_id_2}/careers/{career_id_1}")
        self.assertEqual("salesman", moved_resource['name'])

        empty_resource = self.api.get(f"/current_employees/{employee_id_1}/careers/{career_id_1}")
        self.assertEqual(None, empty_resource)

        empty_collection = self.api.get(f"/current_employees/{employee_id_1}/careers")
        self.assertEqual(1, empty_collection['count'])
        self.assertEqual(1, len(empty_collection['results']))

        # check updates occured
        self.assertEqual(1, self.api.get(f"/current_employees/{employee_id_1}")['count_sector'])
        self.assertEqual(1, self.api.get(f"/current_employees/{employee_id_2}")['count_sector'])

        # move rest of resources in collection
        self.api.put(f"/current_employees/{employee_id_2}/careers", {"_from": f"/current_employees/{employee_id_1}/careers"})

        # check updates occured
        self.assertEqual(None, self.api.get(f"/current_employees/{employee_id_1}")['count_sector'])
        self.assertEqual(2, self.api.get(f"/current_employees/{employee_id_2}")['count_sector'])

    def test_rebuild_grants_after_move(self):
        employee_id_1 = self.api.post("/current_employees", {"name": "Bob", "age": 40})
        career_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers", {"name": "salesman"})
        sector_id_1 = self.api.post(f"/current_employees/{employee_id_1}/careers/{career_id_1}/sectors", {"name": "vacuums", "count": 1})

        self.user_id_1 = self.api.post('/users', {'username': 'bob', 'password': 'password'})
        self.user_id_2 = self.api.post('/users', {'username': 'ned', 'password': 'password'})

        # add link to sectors
        current_sector_id = self.api.post('/current_sectors', {})
        self.api.post(f'/current_sectors/{current_sector_id}/referenced_sectors', {"id": sector_id_1})

        # add source grant
        self.group_id_1 = self.api.post('/groups', {'name': 'source'})
        self.grant_id_1 = self.api.post(f'/groups/{self.group_id_1}/grants', {'type': 'read', 'url': '/current_sectors'})
        self.grant_id_2 = self.api.post(f'/groups/{self.group_id_1}/grants', {'type': 'read', 'url': '/current_employees'})
        self.api.post(f'/users/{self.user_id_1}/groups', {'id': self.group_id_1})

        user1 = self.schema.load_user_by_username("bob")

        self.api.get(f'/current_sectors/{current_sector_id}/referenced_sectors/{sector_id_1}', user=user1)

        # move resource
        self.api.put(f"/former_employees", {"_from": f"/current_employees/{employee_id_1}"})

        # check acces removed
        with self.assertRaises(HTTPError):
            self.api.get(f'/current_sectors/{current_sector_id}/referenced_sectors/{sector_id_1}', user=user1)
