
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

    def test_rename(self):
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'phone', 'str')

        self.schema_1.create_field('root', 'users', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'address', 'str', default="42 ironside")

        self.schema_2.create_field('root', 'users', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob", "phone": "123456"}, 'users')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned", "phone": "789654"}, 'users')

        # alter mutation to rename field
        #self.client.patch("/api/schemas/<schema_id>/specs/<spec_name>/fields/<field_name>",
