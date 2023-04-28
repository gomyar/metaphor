
import unittest

from bson.objectid import ObjectId
from pymongo import MongoClient

from metaphor.schema import Schema, Spec, Field
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
        self.schema_1 = Schema(self.db)
        self.schema_1._id = ObjectId()
        self.schema_2 = Schema(self.db)
        self.schema_2._id = ObjectId()

        self.schema_1.set_as_latest()

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

        self.assertEqual(1, len(mutation.steps))
        self.assertEqual('<DefaultFieldMutation>', str(mutation.steps[0]))
        self.assertEqual('user', mutation.steps[0].spec_name)
        self.assertEqual('address', mutation.steps[0].field_name)

        mutation.mutate()

        user_1 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_1_id)})
        user_2 = self.db.resource_user.find_one({"_id": self.schema_1.decodeid(user_2_id)})

        self.assertEqual("42 ironside", user_1['address'])
        self.assertEqual("42 ironside", user_2['address'])

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
