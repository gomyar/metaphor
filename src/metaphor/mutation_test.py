
import unittest

from bson.objectid import ObjectId
from pymongo import MongoClient

from metaphor.schema import Schema, Spec, Field
from metaphor.schema_factory import SchemaFactory
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash

from .mutation import Mutation


class MutationTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

    def test_create_mutation(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('user')
        self.schema_1.create_field('user', 'username', 'str')

        self.schema_1.create_field('root', 'users', 'collection', 'user')

        self.schema_2.create_spec('user')
        self.schema_2.create_field('user', 'username', 'str')
        self.schema_2.create_field('user', 'address', 'str', default="42 ironside")

        self.schema_2.create_field('root', 'users', 'collection', 'user')

        # insert test data
        user_1_id = self.schema_1.insert_resource('user', {"username": "Bob"}, 'users')
        user_2_id = self.schema_1.insert_resource('user', {"username": "Ned"}, 'users')

        mutation = Mutation(self.schema_1, self.schema_2)
        mutation.init()

        self.assertEqual(1, len(mutation.create_steps))
        self.assertEqual('<DefaultFieldMutation>', str(mutation.create_steps[0]))
        self.assertEqual('user', mutation.create_steps[0].spec_name)
        self.assertEqual('address', mutation.create_steps[0].field_name)

        mutation.mutate()

        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("42 ironside", user_1['address'])
        self.assertEqual("42 ironside", user_2['address'])

    def test_delete_field(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('user')
        self.schema_1.create_field('user', 'username', 'str')
        self.schema_1.create_field('user', 'address', 'str')

        self.schema_1.create_field('root', 'users', 'collection', 'user')

        self.schema_2.create_spec('user')
        self.schema_2.create_field('user', 'username', 'str')

        self.schema_2.create_field('root', 'users', 'collection', 'user')

        # insert test data
        user_1_id = self.schema_1.insert_resource('user', {"username": "Bob", "address": "here"}, 'users')
        user_2_id = self.schema_1.insert_resource('user', {"username": "Ned", "address": "there"}, 'users')

        mutation = Mutation(self.schema_1, self.schema_2)
        mutation.init()

        self.assertEqual(1, len(mutation.delete_steps))
        self.assertEqual('<DeleteFieldMutation>', str(mutation.delete_steps[0]))
        self.assertEqual('user', mutation.delete_steps[0].spec_name)
        self.assertEqual('address', mutation.delete_steps[0].field_name)

        mutation.mutate()

        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertTrue('address' not in user_1)
        self.assertTrue('address' not in user_2)

    def test_alter_field_type_int_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('user')
        self.schema_1.create_field('user', 'phone', 'int')
        self.schema_1.create_field('root', 'users', 'collection', 'user')

        self.schema_2.create_spec('user')
        self.schema_2.create_field('user', 'phone', 'str')
        self.schema_2.create_field('root', 'users', 'collection', 'user')

        # insert test data
        user_1_id = self.schema_1.insert_resource('user', {"phone": 12345}, 'users')
        user_2_id = self.schema_1.insert_resource('user', {"phone": 67890}, 'users')

        mutation = Mutation(self.schema_1, self.schema_2)
        mutation.init()

        self.assertEqual(1, len(mutation.alter_steps))
        self.assertEqual('<AlterFieldTypeConvertPrimitiveMutation>', str(mutation.alter_steps[0]))
        self.assertEqual('user', mutation.alter_steps[0].spec_name)
        self.assertEqual('phone', mutation.alter_steps[0].field_name)

        mutation.mutate()

        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("12345", user_1['phone'])
        self.assertEqual("67890", user_2['phone'])

    def test_alter_field_type_float_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('user')
        self.schema_1.create_field('user', 'phone', 'float')
        self.schema_1.create_field('root', 'users', 'collection', 'user')

        self.schema_2.create_spec('user')
        self.schema_2.create_field('user', 'phone', 'str')
        self.schema_2.create_field('root', 'users', 'collection', 'user')

        # insert test data
        user_1_id = self.schema_1.insert_resource('user', {"phone": 12345.67}, 'users')
        user_2_id = self.schema_1.insert_resource('user', {"phone": 67890.12}, 'users')

        mutation = Mutation(self.schema_1, self.schema_2)
        mutation.init()

        self.assertEqual(1, len(mutation.alter_steps))
        self.assertEqual('<AlterFieldTypeConvertPrimitiveMutation>', str(mutation.alter_steps[0]))
        self.assertEqual('user', mutation.alter_steps[0].spec_name)
        self.assertEqual('phone', mutation.alter_steps[0].field_name)

        mutation.mutate()

        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("12345.7", user_1['phone'])
        self.assertEqual("67890.1", user_2['phone'])


    def test_alter_field_type_bool_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('user')
        self.schema_1.create_field('user', 'phone', 'bool')
        self.schema_1.create_field('root', 'users', 'collection', 'user')

        self.schema_2.create_spec('user')
        self.schema_2.create_field('user', 'phone', 'str')
        self.schema_2.create_field('root', 'users', 'collection', 'user')

        # insert test data
        user_1_id = self.schema_1.insert_resource('user', {"phone": True}, 'users')
        user_2_id = self.schema_1.insert_resource('user', {"phone": False}, 'users')

        mutation = Mutation(self.schema_1, self.schema_2)
        mutation.init()

        self.assertEqual(1, len(mutation.alter_steps))
        self.assertEqual('<AlterFieldTypeConvertPrimitiveMutation>', str(mutation.alter_steps[0]))
        self.assertEqual('user', mutation.alter_steps[0].spec_name)
        self.assertEqual('phone', mutation.alter_steps[0].field_name)

        mutation.mutate()

        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("true", user_1['phone'])
        self.assertEqual("false", user_2['phone'])

    def test_alter_field_type_datetime_to_str(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('user')
        self.schema_1.create_field('user', 'created', 'datetime')
        self.schema_1.create_field('root', 'users', 'collection', 'user')

        self.schema_2.create_spec('user')
        self.schema_2.create_field('user', 'created', 'str')
        self.schema_2.create_field('root', 'users', 'collection', 'user')

        # insert test data
        user_1_id = self.schema_1.insert_resource('user', {"created": "2023-01-02T10:11:22.000Z"}, 'users')
        user_2_id = self.schema_1.insert_resource('user', {"created": None}, 'users')

        mutation = Mutation(self.schema_1, self.schema_2)
        mutation.init()

        self.assertEqual(1, len(mutation.alter_steps))
        self.assertEqual('<AlterFieldTypeConvertPrimitiveMutation>', str(mutation.alter_steps[0]))
        self.assertEqual('user', mutation.alter_steps[0].spec_name)
        self.assertEqual('created', mutation.alter_steps[0].field_name)

        mutation.mutate()

        user_1 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.metaphor_resource.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("2023-01-02T10:11:22.000Z", user_1['created'])
        self.assertEqual(None, user_2['created'])

    def test_data_steps(self):
        # given 2 schemas
        self.schema_1 = SchemaFactory(self.db).create_schema()
        self.schema_2 = SchemaFactory(self.db).create_schema()

        self.schema_1.set_as_current()

        self.schema_1.create_spec('user')
        self.schema_1.create_field('user', 'name', 'str')
        self.schema_1.create_field('root', 'primary_users', 'collection', 'user')
        self.schema_1.create_field('root', 'secondary_users', 'collection', 'user')

        self.schema_2.create_spec('user')
        self.schema_2.create_field('user', 'name', 'str')
        self.schema_2.create_field('root', 'primary_users', 'collection', 'user')
        self.schema_2.create_field('root', 'secondary_users', 'collection', 'user')

        # insert test data
        user_1_id = self.schema_1.insert_resource('user', {"name": "Bob"}, 'primary_users')
        user_2_id = self.schema_1.insert_resource('user', {"name": "Ned"}, 'primary_users')

        mutation = Mutation(self.schema_1, self.schema_2)
        mutation.init()

        mutation.add_pre_data_step("move", "primary_users", "root", "secondary_users")

        mutation.mutate()

        # assert data moved
        self.assertEqual(0, self.db.metaphor_resource.count_documents({"_type": "user", "_parent_field_name": "primary_users"}))
        self.assertEqual(2, self.db.metaphor_resource.count_documents({"_type": "user", "_parent_field_name": "secondary_users"}))

    # type changes
        # str -> int
        # int -> str
        # float -> int
        # int -> float
        # str -> datetime
        # int -> bool

    # test infer mutation add field

    # test infer delete field

    # test infer add resource

    # test infer delete resource

    # test infer alter field (default / required / type)
