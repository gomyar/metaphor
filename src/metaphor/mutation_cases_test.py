
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
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'employees'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'partners'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'partners.jobs'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'clients'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'clients.jobs'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'archived'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'archived.jobs'})
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

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'address', 'str', default="42 ironside")

        self.schema_2.create_spec('contract')
        self.schema_2.create_field('contract', 'title', 'str')
        self.schema_2.create_field('contract', 'description', 'str')
        self.schema_2.create_field('contract', 'salary', 'int')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob", "phone": "123456"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned", "phone": "789654"}, 'clients')

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
        user_1 = self.db.resource_client.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_client.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("42 ironside", user_1['address'])
        self.assertEqual("42 ironside", user_2['address'])

    def test_rename_spec(self):
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('partner')
        self.schema_2.create_field('partner', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'partner')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

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
        user_1 = self.db.resource_partner.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_partner.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("Bob", user_1['name'])
        self.assertEqual("Ned", user_2['name'])

    def test_rename_field(self):
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'title', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json
        mutation_id = mutation_data['id']

        # check mutations
        mutations = self.client.get(f"/admin/api/mutations/{mutation_id}/steps").json
        self.assertEqual([
            {'action': 'create_field',
             'params': {'spec_name': 'client',
                'field_name': 'title',
                'field_type': 'str',
                'default': None,
                'indexed': False,
                'is_reverse': False,
                'spec_name': 'client',
                'unique': False,
                'unique_global': False,
                'field_target': None}},
            {'action': 'delete_field',
             'params': {'spec_name': 'client', 'field_name': 'name'}},
             ], mutations)

        # alter mutation to rename field
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "rename_field",
            "spec_name": "client",
            "from_field_name": "name",
            "to_field_name": "title",
        })
        self.assertEqual(200, response.status_code)

        # check mutations
        mutations = self.client.get(f"/admin/api/mutations/{mutation_id}/steps").json
        self.assertEqual([
            {'action': 'rename_field',
             'params': {
                'spec_name': 'client',
                'from_field_name': 'name',
                'to_field_name': 'title',
                'calc_str': None,
                'default': None,
                'field_name': 'title',
                'field_target': None,
                'field_type': 'str',
             }},
             ], mutations)

    def test_rename_root_field(self):
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')

        self.schema_2.create_field('root', 'partners', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json
        mutation_id = mutation_data['id']

        # check mutations
        mutations = self.client.get(f"/admin/api/mutations/{mutation_id}/steps").json
        self.assertEqual([
            {'action': 'create_field',
             'params': {'spec_name': 'root',
                        'field_name': 'partners',
                        'field_type': 'collection',
                        'field_target': 'client',
                        'indexed': False,
                        'unique': False,
                        'unique_global': False,
                        'is_reverse': False,
                        'default': None}},
            {'action': 'delete_field',
             'params': {'spec_name': 'root',
                        'field_name': 'clients'}},
             ], mutations)

        # alter mutation to rename field
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "rename_field",
            "spec_name": "root",
            "from_field_name": "clients",
            "to_field_name": "partners",
        })
        self.assertEqual(200, response.status_code)

        # check mutations
        mutations = self.client.get(f"/admin/api/mutations/{mutation_id}/steps").json
        self.assertEqual([
            {'action': 'rename_field',
             'params': {
                'spec_name': 'root',
                'from_field_name': 'clients',
                'to_field_name': 'partners',
                'calc_str': None,
                'default': None,
                'field_name': 'partners',
                'field_target': 'client',
                'field_type': 'collection',
             }},
             ], mutations)

        # cancel rename
        response = self.client.delete(f"/admin/api/mutations/{mutation_id}/steps/root/clients")
        self.assertEqual(200, response.status_code)

        # check mutations
        mutations = self.client.get(f"/admin/api/mutations/{mutation_id}/steps").json
        self.assertEqual([
            {'action': 'create_field',
             'params': {
                'spec_name': 'root',
                'default': None,
                'field_name': 'partners',
                'field_type': 'collection',
                'unique': False,
                'unique_global': False,
                'indexed': False,
                'is_reverse': False,
                'field_target': 'client'}},
            {'action': 'delete_field',
             'params': {'spec_name': 'root', 'field_name': 'clients'}},
             ], mutations)

    def test_move_step(self):
        # setup schema 1
        self.schema_1.create_spec('job')
        self.schema_1.create_field('job', 'name', 'str')

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'jobs', 'collection', 'job')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')
        self.schema_1.create_field('root', 'partners', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('job')
        self.schema_2.create_field('job', 'name', 'str')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'jobs', 'collection', 'job')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')
        self.schema_2.create_field('root', 'partners', 'collection', 'client')

        # insert test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        job_1_id = self.schema_1.insert_resource('job', {"name": "Sweeping"}, 'jobs', 'client', client_1_id)
        client_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'partners')
        self.assertEqual("Sweeping", self.client.get(f"/api/clients/{client_1_id}/jobs/{job_1_id}").json['name'])

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json
        mutation_id = mutation_data['id']

        # add move step
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "move",
            "from_path": "clients/jobs",
            "to_path": f"partners/{client_2_id}/jobs",
        })
        self.assertEqual(200, response.status_code)

        # run / promote mutation
        response = self.client.patch(f"/admin/api/mutations/{mutation_id}", json={
            "promote": True
        })
        self.assertEqual(200, response.status_code)

        self.assertEqual("Sweeping", self.client.get(f"/api/partners/{client_2_id}/jobs/{job_1_id}").json['name'])

    def test_move_step_filter(self):
        # setup schema 1
        self.schema_1.create_spec('job')
        self.schema_1.create_field('job', 'name', 'str')

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'jobs', 'collection', 'job')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')
        self.schema_1.create_field('root', 'partners', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('job')
        self.schema_2.create_field('job', 'name', 'str')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'jobs', 'collection', 'job')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')
        self.schema_2.create_field('root', 'partners', 'collection', 'client')

        # insert test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        job_1_id = self.schema_1.insert_resource('job', {"name": "Sweeping"}, 'jobs', 'client', client_1_id)
        job_2_id = self.schema_1.insert_resource('job', {"name": "Washing"}, 'jobs', 'client', client_1_id)
        client_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'partners')
        self.assertEqual("Sweeping", self.client.get(f"/api/clients/{client_1_id}/jobs/{job_1_id}").json['name'])

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json
        mutation_id = mutation_data['id']

        # add move step
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "move",
            "from_path": "clients/jobs[name~'wash']",
            "to_path": f"partners/{client_2_id}/jobs",
        })
        self.assertEqual(200, response.status_code)

        # run / promote mutation
        response = self.client.patch(f"/admin/api/mutations/{mutation_id}", json={
            "promote": True
        })
        self.assertEqual(200, response.status_code)

        self.assertEqual("Sweeping", self.client.get(f"/api/clients/{client_1_id}/jobs/{job_1_id}").json['name'])
        self.assertEqual("Washing", self.client.get(f"/api/partners/{client_2_id}/jobs/{job_2_id}").json['name'])

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
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        client_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

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
        # setup schema 1
        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')
        self.schema_1.create_field('root', 'archived', 'collection', 'client')

        # setup schema 2
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')

        self.schema_2.create_field('root', 'partners', 'collection', 'client')
        self.schema_2.create_field('root', 'archived', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

        # create mutation
        response = self.client.post('/admin/api/mutations', json={
            "from_schema_id": self.schema_1.schema_id, "to_schema_id": self.schema_2.schema_id})
        mutation_data = response.json
        mutation_id = mutation_data['id']

        # alter mutation to rename field
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "rename_field",
            "spec_name": "root",
            "from_field_name": "clients",
            "to_field_name": "partners",
        })
        self.assertEqual(200, response.status_code)

        # add move step
        response = self.client.post(f"/admin/api/mutations/{mutation_id}/steps", json={
            "action": "move",
            "from_path": "partners",   # use target schema field name (the renamed field)
            "to_path": "archived",
        })
        self.assertEqual(200, response.status_code)

        # run / promote mutation
        response = self.client.patch(f"/admin/api/mutations/{mutation_id}", json={
            "promote": True
        })
        self.assertEqual(200, response.status_code)

        self.assertEqual(0, self.client.get("/api/partners").json['count'])
        self.assertEqual(2, self.client.get("/api/archived").json['count'])


    def test_alter_field_to_required_include_default(self):
        pass
