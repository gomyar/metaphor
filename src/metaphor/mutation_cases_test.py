
import unittest

from bson.objectid import ObjectId
from metaphor.mongoclient_testutils import mongo_connection

from metaphor.schema import Schema, Spec, Field
from metaphor.schema_factory import SchemaFactory
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash
from metaphor.api import Api
from server import create_app

from .mutation import MutationFactory


class MutationTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.api = Api(self.db)

        # create initial data
        self.user_id = self.api.post('/users', {'username': 'bob', 'password': 'password', 'admin': True})
        self.group_id = self.api.post('/groups', {'name': 'manager'})
        self.grant_id_1 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': '/employees'})
        self.grant_id_2 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': '/partners'})
        self.api.post('/users/%s/groups' % self.user_id, {'id': self.group_id})

        # create test clients
        self.app = create_app(self.db)
        self.client = self.app.test_client()

        # login
        response = self.client.post('/login', json={
            "username": "bob",
            "password": "password",
        }, follow_redirects=True)

    def test_mutation(self):
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'phone', 'str')

        self.schema_1.create_spec('job')
        self.schema_1.create_field('job', 'title', 'str')
        self.schema_1.create_field('job', 'description', 'str')
        self.schema_1.create_field('job', 'salary', 'int')

        self.schema_1.create_field('root', 'users', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'address', 'str', default="42 ironside")

        self.schema_2.create_spec('contract')
        self.schema_2.create_field('contract', 'title', 'str')
        self.schema_2.create_field('contract', 'description', 'str')
        self.schema_2.create_field('contract', 'salary', 'int')

        self.schema_2.create_field('root', 'users', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob", "phone": "123456"}, 'users')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned", "phone": "789654"}, 'users')

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json

        # run / promote mutation
        response = self.client.patch('/admin/api/mutations/' + mutation_data['id'], json={
            "promote": True
        })
        self.assertEqual(200, response.status_code)

        # assert results
        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("42 ironside", user_1['address'])
        self.assertEqual("42 ironside", user_2['address'])

    def test_rename_spec(self):
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'users', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('partner')
        self.schema_2.create_field('partner', 'name', 'str')

        self.schema_2.create_field('root', 'users', 'collection', 'partner')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'users')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'users')

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json
        mutation_id = mutation_data['id']

        # alter mutation to rename field
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "rename_spec",
            "from_spec_name": "client",
            "to_spec_name": "partner",
        })
        self.assertEqual(200, response.status_code)

        # check mutations
        mutations = self.client.get(f"/admin/api/mutations/{mutation_id}/steps").json
        self.assertEqual([
            {'action': 'rename_spec',
             'params': {'spec_name': 'client', 'to_spec_name': 'partner'}}], mutations)

        # run / promote mutation
        response = self.client.patch(f"/admin/api/mutations/{mutation_id}", json={
            "promote": True
        })
        self.assertEqual(200, response.status_code)

        # assert results
        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("Bob", user_1['name'])
        self.assertEqual("Ned", user_2['name'])
        self.assertEqual("partner", user_1['_type'])
        self.assertEqual("partner", user_2['_type'])

    def test_move_root_collections(self):
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')
        self.schema_1.create_field('root', 'partners', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')
        self.schema_2.create_field('root', 'partners', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json
        mutation_id = mutation_data['id']

        # add move step
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "move",
            "from_path": "clients",
            "to_path": "partners",
        })
        self.assertEqual(200, response.status_code)

        # run / promote mutation
        response = self.client.patch(f"/admin/api/mutations/{mutation_id}", json={
            "promote": True
        })
        self.assertEqual(200, response.status_code)

        self.assertEqual(0, self.client.get("/api/clients").json['count'])
        self.assertEqual(2, self.client.get("/api/partners").json['count'])

    def test_move_from_renamed_collection(self):
        pass

    def test_alter_field_to_required_include_default(self):
        pass
