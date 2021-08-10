
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema import Schema
from metaphor.api import Api
from metaphor.updater import Updater
from metaphor.lrparse.lrparse import parse


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema(self.db)

        self.updater = Updater(self.schema)

        self.schema.create_initial_schema()

        self.admin_group_id = self.updater.create_resource('group', 'root', 'groups', None, {'name': 'admin'}, self.schema.read_root_grants('groups'))

        self.grant_id = self.updater.create_resource('grant', 'group', 'grants', self.admin_group_id, {'type': 'read', 'url': '/companies'})

        self.user_id = self.updater.create_resource('user', 'root', 'users', None, {'username': 'bob', 'password': 'hash', 'admin': True}, self.schema.read_root_grants('users'))

        self.updater.create_linkcollection_entry('user', self.user_id, 'groups', self.admin_group_id)

        self.company_spec = self.schema.add_spec('company')
        self.schema.add_field(self.schema.root, 'companies', 'collection', 'company')

    def test_delete_group(self):
        user_db = self.db['resource_user'].find_one()
        self.assertEqual(1, len(user_db['read_grants']))

        self.updater.delete_linkcollection_entry('user', self.schema.decodeid(self.user_id), 'groups', self.admin_group_id)

        user_db = self.db['resource_user'].find_one()
        self.assertEqual(0, len(user_db['read_grants']))
