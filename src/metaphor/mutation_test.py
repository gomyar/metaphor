
import unittest

from bson.objectid import ObjectId
from metaphor.mongoclient_testutils import mongo_connection

from metaphor.schema import Schema, Spec, Field
from metaphor.schema_factory import SchemaFactory
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash

from .mutation import MutationFactory


class MutationTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

    def test_create_mutation(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'address', 'str', default="42 ironside")

        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(1, len(mutation.steps))
        self.assertEqual('create_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('address', mutation.steps[0]['params']['field_name'])
        self.assertEqual('42 ironside', mutation.steps[0]['params']['default'])

        mutation.mutate()

        user_1 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("42 ironside", user_1['address'])
        self.assertEqual("42 ironside", user_2['address'])

    def test_delete_field(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'address', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob", "address": "here"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned", "address": "there"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(1, len(mutation.steps))
        self.assertEqual('delete_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('address', mutation.steps[0]['params']['field_name'])

        mutation.mutate()

        user_1 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertTrue('address' not in user_1)
        self.assertTrue('address' not in user_2)

    def test_alter_field_type_int_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'phone', 'int')
        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'phone', 'str')
        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"phone": 12345}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"phone": 67890}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(1, len(mutation.steps))
        self.assertEqual('alter_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('phone', mutation.steps[0]['params']['field_name'])

        mutation.mutate()

        user_1 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("12345", user_1['phone'])
        self.assertEqual("67890", user_2['phone'])

    def test_alter_field_type_float_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'phone', 'float')
        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'phone', 'str')
        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"phone": 12345.67}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"phone": 67890.12}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(1, len(mutation.steps))
        self.assertEqual('alter_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('phone', mutation.steps[0]['params']['field_name'])

        mutation.mutate()

        user_1 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("12345.67", user_1['phone'])
        self.assertEqual("67890.12", user_2['phone'])


    def test_alter_field_type_bool_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'phone', 'bool')
        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'phone', 'str')
        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"phone": True}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"phone": False}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(1, len(mutation.steps))
        self.assertEqual('alter_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('phone', mutation.steps[0]['params']['field_name'])

        mutation.mutate()

        user_1 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("true", user_1['phone'])
        self.assertEqual("false", user_2['phone'])

    def test_alter_field_type_datetime_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'created', 'datetime')
        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'created', 'str')
        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"created": "2023-01-02T10:11:22.000Z"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"created": None}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(1, len(mutation.steps))
        self.assertEqual('alter_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('created', mutation.steps[0]['params']['field_name'])

        mutation.mutate()

        user_1 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("2023-01-02T10:11:22.000Z", user_1['created'])
        self.assertEqual(None, user_2['created'])

    def test_move_steps_root(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('root', 'primary_clients', 'collection', 'client')
        self.schema_1.create_field('root', 'secondary_clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('root', 'primary_clients', 'collection', 'client')
        self.schema_2.create_field('root', 'secondary_clients', 'collection', 'client')

        # insert test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'primary_clients')
        client_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'primary_clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        mutation.add_move_step("primary_clients", "secondary_clients")

        mutation.mutate()

        # assert data moved
        self.assertEqual(0, self.db.resource_user.count_documents({"_parent_field_name": "primary_users"}))
        self.assertEqual(2, self.db.resource_user.count_documents({"_parent_field_name": "secondary_users"}))

    def test_move_steps(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        # create schema 1
        self.schema_1.create_spec('job')
        self.schema_1.create_field('job', 'description', 'str')

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'jobs', 'collection', 'job')

        self.schema_1.create_field('root', 'primary_clients', 'collection', 'client')
        self.schema_1.create_field('root', 'secondary_clients', 'collection', 'client')

        # create schema 2
        self.schema_2.create_spec('job')
        self.schema_2.create_field('job', 'description', 'str')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'jobs', 'collection', 'job')
        self.schema_2.create_field('root', 'primary_clients', 'collection', 'client')
        self.schema_2.create_field('root', 'secondary_clients', 'collection', 'client')

        # insert test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'primary_clients')
        client_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'secondary_clients')

        job_1_id = self.schema_1.insert_resource('job', {'description': 'Job 1'}, 'jobs', 'client', client_1_id)
        job_2_id = self.schema_1.insert_resource('job', {'description': 'Job 2'}, 'jobs', 'client', client_2_id)

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        mutation.add_move_step(f"primary_clients/{client_1_id}/jobs", f"secondary_clients/{client_2_id}/jobs")

        mutation.mutate()

        # assert data moved
        self.assertEqual(0, self.db.metaphor_client.count_documents({"_type": "job", "_parent_id": self.schema_1.decodeid(client_1_id)}))
        self.assertEqual(2, self.db.metaphor_client.count_documents({"_type": "job", "_parent_id": self.schema_1.decodeid(client_2_id)}))

    def test_validate_move_steps(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        # create schema 1
        self.schema_1.create_spec('job')
        self.schema_1.create_field('job', 'description', 'str')

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'jobs', 'collection', 'job')

        self.schema_1.create_field('root', 'primary_clients', 'collection', 'client')
        self.schema_1.create_field('root', 'secondary_clients', 'collection', 'client')

        # create schema 2
        self.schema_2.create_spec('job')
        self.schema_2.create_field('job', 'description', 'str')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'jobs', 'collection', 'job')
        self.schema_2.create_field('root', 'primary_clients', 'collection', 'client')
        self.schema_2.create_field('root', 'secondary_clients', 'collection', 'client')

        # insert test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'primary_clients')
        client_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'secondary_clients')

        job_1_id = self.schema_1.insert_resource('job', {'description': 'Job 1'}, 'jobs', 'client', client_1_id)
        job_2_id = self.schema_1.insert_resource('job', {'description': 'Job 2'}, 'jobs', 'client', client_2_id)

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        with self.assertRaises(Exception):
            # must be same type
            mutation.add_move_step(f"primary_clients/{client_1_id}", f"secondary_clients/{client_2_id}/jobs")

        with self.assertRaises(Exception):
            # target must be collection
            mutation.add_move_step(f"primary_clients/{client_1_id}", f"secondary_clients/{client_2_id}")

    def test_create_spec(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')
        self.schema_2.create_field('client', 'address', 'str', default="42 ironside")

        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(4, len(mutation.steps))
        self.assertEqual('create_spec', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])

        self.assertEqual('create_field', mutation.steps[1]['action'])
        self.assertEqual('client', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('name', mutation.steps[1]['params']['field_name'])

        self.assertEqual('create_field', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])
        self.assertEqual('address', mutation.steps[2]['params']['field_name'])
        self.assertEqual('42 ironside', mutation.steps[2]['params']['default'])

        self.assertEqual('create_field', mutation.steps[3]['action'])
        self.assertEqual('root', mutation.steps[3]['params']['spec_name'])
        self.assertEqual('clients', mutation.steps[3]['params']['field_name'])
        self.assertEqual('collection', mutation.steps[3]['params']['field_type'])
        self.assertEqual('client', mutation.steps[3]['params']['field_target'])

        mutation.mutate()

    def test_delete_spec(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')
        self.schema_1.create_field('client', 'address', 'str', default="42 ironside")

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        # create test data
        user_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"name": "Ned"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual(2, len(mutation.steps))
        self.assertEqual([{'action': 'delete_field',
          'params': {'field_name': 'clients', 'spec_name': 'root'}},
           {'action': 'delete_spec', 'params': {'spec_name': 'client'}}
           ], mutation.steps)

        mutation.mutate()

        self.assertEqual(0, self.db.metaphor_client.count_documents({"_type": "client"}))

    def test_rename_field(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'username', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"username": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # creates a delete and a create
        self.assertEqual(2, len(mutation.steps))

        self.assertEqual('create_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('name', mutation.steps[0]['params']['field_name'])

        self.assertEqual('delete_field', mutation.steps[1]['action'])
        self.assertEqual('client', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('username', mutation.steps[1]['params']['field_name'])

        # alter the mutation
        mutation.convert_delete_field_to_rename('client', 'username', 'name')

        self.assertEqual(1, len(mutation.steps))

        self.assertEqual('rename_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('username', mutation.steps[0]['params']['from_field_name'])
        self.assertEqual('name', mutation.steps[0]['params']['to_field_name'])

        mutation.mutate()

        client = self.schema_2.db['metaphor_client'].find_one({'_id': self.schema_1.decodeid(client_1_id)})
        self.assertEqual("Bob", client["name"])
        self.assertIsNone(client.get('username'))

    def test_cancel_rename_field(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'username', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"username": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # alter the mutation
        mutation.convert_delete_field_to_rename('client', 'username', 'name')

        # cancel it again
        mutation.cancel_rename_field('client', 'username')

        # creates a delete and a create
        self.assertEqual(2, len(mutation.steps))

        self.assertEqual('create_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('name', mutation.steps[0]['params']['field_name'])

        self.assertEqual('delete_field', mutation.steps[1]['action'])
        self.assertEqual('client', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('username', mutation.steps[1]['params']['field_name'])

        mutation.mutate()

        client = self.schema_2.db['metaphor_client'].find_one({'_id': self.schema_2.decodeid(client_1_id)})
        self.assertIsNone(client.get('name'))
        self.assertIsNone(client.get('username'))

    def test_rename_spec(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('customer')
        self.schema_2.create_field('customer', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'customers')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # creates a delete and a create
        self.assertEqual(3, len(mutation.steps))
        self.assertEqual('create_spec', mutation.steps[0]['action'])
        self.assertEqual('customer', mutation.steps[0]['params']['spec_name'])

        self.assertEqual('create_field', mutation.steps[1]['action'])
        self.assertEqual('customer', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('name', mutation.steps[1]['params']['field_name'])

        self.assertEqual('delete_spec', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])

        # alter the mutation
        mutation.convert_delete_spec_to_rename('client', 'customer')

        self.assertEqual(1, len(mutation.steps))

        self.assertEqual('rename_spec', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('customer', mutation.steps[0]['params']['to_spec_name'])

        mutation.mutate()

        client = self.schema_2.db['metaphor_client'].find_one({'_id': self.schema_1.decodeid(client_1_id)})
        self.assertEqual("Bob", client["name"])
        self.assertEqual("customer", client['_type'])

    def test_rename_spec_with_different_fields(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('customer')
        self.schema_2.create_field('customer', 'title', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'customers')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # creates a delete and a create
        self.assertEqual(3, len(mutation.steps))
        self.assertEqual('create_spec', mutation.steps[0]['action'])
        self.assertEqual('customer', mutation.steps[0]['params']['spec_name'])

        self.assertEqual('create_field', mutation.steps[1]['action'])
        self.assertEqual('customer', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('title', mutation.steps[1]['params']['field_name'])

        self.assertEqual('delete_spec', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])

        # alter the mutation
        mutation.convert_delete_spec_to_rename('client', 'customer')

        self.assertEqual(3, len(mutation.steps))

        # create fields on first spec before renaming spec
        self.assertEqual('create_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('title', mutation.steps[0]['params']['field_name'])

        self.assertEqual('rename_spec', mutation.steps[1]['action'])
        self.assertEqual('client', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('customer', mutation.steps[1]['params']['to_spec_name'])

        self.assertEqual('delete_field', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])
        self.assertEqual('name', mutation.steps[2]['params']['field_name'])

    def test_cancel_rename_spec(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('customer')
        self.schema_2.create_field('customer', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'customers')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # alter the mutation
        mutation.convert_delete_spec_to_rename('client', 'customer')

        # cancel it again
        mutation.cancel_rename_spec('client')

        # creates a delete and a create
        self.assertEqual(3, len(mutation.steps))

        self.assertEqual('create_spec', mutation.steps[0]['action'])
        self.assertEqual('customer', mutation.steps[0]['params']['spec_name'])

        self.assertEqual('create_field', mutation.steps[1]['action'])
        self.assertEqual('customer', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('name', mutation.steps[1]['params']['field_name'])

        self.assertEqual('delete_spec', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])

        mutation.mutate()

        self.assertEqual(0, self.db.metaphor_client.count_documents({"_type": "client"}))

    def test_cancel_rename_spec_with_different_fields(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('customer')
        self.schema_2.create_field('customer', 'title', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'customers')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # alter the mutation
        mutation.convert_delete_spec_to_rename('client', 'customer')

        # cancel it again
        mutation.cancel_rename_spec('client')

        # creates a delete and a create
        self.assertEqual(3, len(mutation.steps))

        self.assertEqual('create_spec', mutation.steps[0]['action'])
        self.assertEqual('customer', mutation.steps[0]['params']['spec_name'])

        self.assertEqual('create_field', mutation.steps[1]['action'])
        self.assertEqual('customer', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('title', mutation.steps[1]['params']['field_name'])

        self.assertEqual('delete_spec', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])

    def test_cancel_rename_spec_with_renamed_field(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('customer')
        self.schema_2.create_field('customer', 'title', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'customers')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # alter the mutation
        mutation.convert_delete_spec_to_rename('client', 'customer')
        mutation.convert_delete_field_to_rename('client', 'name', 'title')

        # cancel it again
        mutation.cancel_rename_spec('client')

        # creates a delete and a create
        self.assertEqual(3, len(mutation.steps))

        self.assertEqual('create_spec', mutation.steps[0]['action'])
        self.assertEqual('customer', mutation.steps[0]['params']['spec_name'])

        self.assertEqual('create_field', mutation.steps[1]['action'])
        self.assertEqual('customer', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('title', mutation.steps[1]['params']['field_name'])

        self.assertEqual('delete_spec', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])

    def test_cancel_rename_field_on_renamed_spec(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('customer')
        self.schema_2.create_field('customer', 'title', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'customers')

        # create test data
        client_1_id = self.schema_1.insert_resource('client', {"name": "Bob"}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        # alter the mutation
        mutation.convert_delete_spec_to_rename('client', 'customer')
        mutation.convert_delete_field_to_rename('client', 'name', 'title')

        # cancel it again
        mutation.cancel_rename_field('client', 'name')

        # creates a delete and a create
        self.assertEqual(3, len(mutation.steps))

        self.assertEqual('create_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('title', mutation.steps[0]['params']['field_name'])

        self.assertEqual('rename_spec', mutation.steps[1]['action'])
        self.assertEqual('client', mutation.steps[1]['params']['spec_name'])
        self.assertEqual('customer', mutation.steps[1]['params']['to_spec_name'])

        self.assertEqual('delete_field', mutation.steps[2]['action'])
        self.assertEqual('client', mutation.steps[2]['params']['spec_name'])
        self.assertEqual('name', mutation.steps[2]['params']['field_name'])

    def test_rename_field_add_default(self):
        pass

    def test_create_calc_fields_in_correct_order(self):
        pass

    def test_rename_root_collection_field(self):
        pass

    def test_root_field_added(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'name', 'str')

        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('customer')
        self.schema_2.create_field('customer', 'name', 'str')
        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'name', 'str')

        self.schema_2.create_field('root', 'clients', 'collection', 'client')
        self.schema_2.create_field('root', 'customers', 'collection', 'customer')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()

        self.assertEqual([
            {'action': 'create_spec', 'params': {'spec_name': 'customer'}},
            {'action': 'create_field',
                'params': {'field_name': 'name',
                    'default': None,
                    'field_type': 'str',
                    'field_target': None,
                    'spec_name': 'customer'}},
            {'action': 'create_field',
                'params': {'field_name': 'customers',
                    'default': None,
                    'field_type': 'collection',
                    'field_target': 'customer',
                    'spec_name': 'root'}},
        ], mutation.steps)

    def test_rename_and_alter_field(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('client')
        self.schema_1.create_field('client', 'phone', 'float')
        self.schema_1.create_field('root', 'clients', 'collection', 'client')

        self.schema_2.create_spec('client')
        self.schema_2.create_field('client', 'phone_number', 'str')
        self.schema_2.create_field('root', 'clients', 'collection', 'client')

        # insert test data
        user_1_id = self.schema_1.insert_resource('client', {"phone": 12345.67}, 'clients')
        user_2_id = self.schema_1.insert_resource('client', {"phone": 67890.12}, 'clients')

        mutation = MutationFactory(self.schema_1, self.schema_2).create()
        mutation.convert_delete_field_to_rename('client', 'phone', 'phone_number')

        self.assertEqual(1, len(mutation.steps))

        self.assertEqual('rename_field', mutation.steps[0]['action'])
        self.assertEqual('client', mutation.steps[0]['params']['spec_name'])
        self.assertEqual('phone', mutation.steps[0]['params']['from_field_name'])
        self.assertEqual('phone_number', mutation.steps[0]['params']['to_field_name'])

        self.assertEqual("phone_number", mutation.steps[0]["params"]["field_name"])
        self.assertEqual(None, mutation.steps[0]["params"]["default"])
        self.assertEqual("str", mutation.steps[0]["params"]["field_type"])
        self.assertEqual(None, mutation.steps[0]["params"]["calc_str"])
        self.assertEqual(None, mutation.steps[0]["params"]["field_target"])

        mutation.mutate()

        user_1 = self.db.metaphor_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("12345.67", user_1['phone_number'])
        self.assertEqual("67890.12", user_2['phone_number'])


