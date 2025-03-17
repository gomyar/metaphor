
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

        self.grandparent_spec = self.schema.create_spec('grandparent')
        self.schema.create_field('grandparent', 'name', 'str')
        self.schema.create_field('grandparent', 'age', 'int')

        self.parent_spec = self.schema.create_spec('parent')
        self.schema.create_field('parent', 'name', 'str')
        self.schema.create_field('parent', 'age', 'int')

        self.schema.create_field('grandparent', 'parents', 'collection', 'parent')

        self.child_spec = self.schema.create_spec('child')
        self.schema.create_field('child', 'name', 'str')
        self.schema.create_field('child', 'age', 'int')

        self.schema.create_field('parent', 'childs', 'collection', 'child')

        self.schema.create_field('grandparent', 'age_child', 'calc', calc_str='sum(self.parents.childs.age)')

        self.schema.create_field('root', 'current_grandparents', 'collection', 'grandparent')
        self.schema.create_field('root', 'former_grandparents', 'collection', 'grandparent')

        self.schema.create_spec('current_child')
        self.schema.create_field('current_child', 'referenced_childs', 'linkcollection', 'child')

        self.schema.create_field('root', 'current_childs', 'collection', 'current_child')

        self.api = Api(self.db)

    def test_move(self):
        grandparent_id_1 = self.api.post("/current_grandparents", {"name": "Old Bob", "age": 60})
        parent_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents", {"name": "Dad", "age": 40})
        child_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_1}/childs", {"name": "Bobby", "age": 10})

        grandparent_id_2 = self.api.post("/current_grandparents", {"name": "Old Ned", "age": 70})
        parent_id_2 = self.api.post(f"/current_grandparents/{grandparent_id_2}/parents", {"name": "Papa"})
        child_id_2 = self.api.post(f"/current_grandparents/{grandparent_id_2}/parents/{parent_id_2}/childs", {"name": "Neddy", "age": 8})

        self.assertEqual(10, self.api.get(f"/current_grandparents/{grandparent_id_1}")['age_child'])

        self.api.put("/former_grandparents", {"_from": "/current_grandparents"})

        self.assertEqual(0, self.api.get("/current_grandparents")['count'])
        self.assertEqual(2, self.api.get("/former_grandparents")['count'])

        self.assertEqual("Old Bob", self.api.get("/former_grandparents")['results'][0]['name'])
        self.assertEqual("Old Ned", self.api.get("/former_grandparents")['results'][1]['name'])

        self.assertEqual("Dad", self.api.get(f"/former_grandparents/{grandparent_id_1}/parents")['results'][0]['name'])
        self.assertEqual("Papa", self.api.get(f"/former_grandparents/{grandparent_id_2}/parents")['results'][0]['name'])

        self.assertEqual("Bobby", self.api.get(f"/former_grandparents/{grandparent_id_1}/parents/{parent_id_1}/childs")['results'][0]['name'])
        self.assertEqual("Neddy", self.api.get(f"/former_grandparents/{grandparent_id_2}/parents/{parent_id_2}/childs")['results'][0]['name'])


        # test expand
        self.assertEqual(f"Dad", self.api.get("/former_grandparents", {"expand": "parents"})['results'][0]['parents'][0]['name'])

        self.assertEqual(f"Bobby", self.api.get("/former_grandparents", {"expand": "parents.childs"})['results'][0]['parents'][0]['childs'][0]['name'])

    def test_move_children_update(self):
        grandparent_id_1 = self.api.post("/current_grandparents", {"name": "Bob", "age": 40})
        parent_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents", {"name": "salesman"})
        child_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_1}/childs", {"name": "vacuums", "age": 1})
        parent_id_2 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents", {"name": "workman"})
        child_id_2 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_2}/childs", {"name": "building", "age": 1})

        grandparent_id_2 = self.api.post("/current_grandparents", {"name": "Ned", "age": 40})

        # check calc correct initially
        self.assertEqual(2, self.api.get(f"/current_grandparents/{grandparent_id_1}")['age_child'])

        # move a resource
        self.api.put(f"/current_grandparents/{grandparent_id_2}/parents", {"_from": f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_1}"})

        # check resource moved
        moved_resource = self.api.get(f"/current_grandparents/{grandparent_id_2}/parents/{parent_id_1}")
        self.assertEqual("salesman", moved_resource['name'])

        empty_resource = self.api.get(f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_1}")
        self.assertEqual(None, empty_resource)

        empty_collection = self.api.get(f"/current_grandparents/{grandparent_id_1}/parents")
        self.assertEqual(1, empty_collection['count'])
        self.assertEqual(1, len(empty_collection['results']))

        # check updates occured
        self.assertEqual(1, self.api.get(f"/current_grandparents/{grandparent_id_1}")['age_child'])
        self.assertEqual(1, self.api.get(f"/current_grandparents/{grandparent_id_2}")['age_child'])

        # move rest of resources in collection
        self.api.put(f"/current_grandparents/{grandparent_id_2}/parents", {"_from": f"/current_grandparents/{grandparent_id_1}/parents"})

        # check updates occured
        self.assertEqual(None, self.api.get(f"/current_grandparents/{grandparent_id_1}")['age_child'])
        self.assertEqual(2, self.api.get(f"/current_grandparents/{grandparent_id_2}")['age_child'])

    def test_rebuild_grants_after_move(self):
        grandparent_id_1 = self.api.post("/current_grandparents", {"name": "Bob", "age": 40})
        parent_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents", {"name": "salesman"})
        child_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_1}/childs", {"name": "vacuums", "age": 1})

        self.user_id_1 = self.api.post('/users', {'email': 'bob'})
        self.user_id_2 = self.api.post('/users', {'email': 'ned'})

        # add link to childs
        current_child_id = self.api.post('/current_childs', {})
        self.api.post(f'/current_childs/{current_child_id}/referenced_childs', {"id": child_id_1})

        # add source grant
        self.group_id_1 = self.api.post('/groups', {'name': 'source'})
        self.grant_id_1 = self.api.post(f'/groups/{self.group_id_1}/grants', {'type': 'read', 'url': 'current_childs.referenced_childs'})
        self.grant_id_2 = self.api.post(f'/groups/{self.group_id_1}/grants', {'type': 'read', 'url': 'current_grandparents'})
        self.api.post(f'/users/{self.user_id_1}/groups', {'id': self.group_id_1})

        user1 = self.schema.load_user_by_email("bob")

        child_1 = self.api.get(f'/current_childs/{current_child_id}/referenced_childs/{child_id_1}', user=user1)
        self.assertEqual("vacuums", child_1['name'])

        # move resource
        self.api.put(f"/former_grandparents", {"_from": f"/current_grandparents/{grandparent_id_1}"})

        # check access remains
        child_1 = self.api.get(f'/current_childs/{current_child_id}/referenced_childs/{child_id_1}', user=user1)
        self.assertEqual("vacuums", child_1['name'])

    def test_move_filter(self):
        user1 = self.schema.load_user_by_email("bob")

        grandparent_id_1 = self.api.post("/current_grandparents", {"name": "Bob", "age": 40})
        parent_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents", {"name": "salesman"})
        child_id_1 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_1}/childs", {"name": "vacuums", "age": 1})
        parent_id_2 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents", {"name": "workman"})
        child_id_2 = self.api.post(f"/current_grandparents/{grandparent_id_1}/parents/{parent_id_2}/childs", {"name": "building", "age": 1})

        grandparent_id_2 = self.api.post("/current_grandparents", {"name": "Ned", "age": 40})

        # move resource
        self.api.put(f"/current_grandparents/{grandparent_id_2}/parents", {"_from": f"/current_grandparents/{grandparent_id_1}/parents[name='workman']"})

        # test only one parent moved
        moved_parents = self.api.get(f'/current_grandparents/{grandparent_id_2}/parents', user=user1)
        self.assertEqual(1, moved_parents['count'])
        self.assertEqual("workman", moved_parents['results'][0]['name'])

