
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = MongoClient()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.create_initial_schema()
        self.schema.set_as_current()

        self.updater = Updater(self.schema)

        self.schema.create_field('user', 'age', 'int')

        self.schema.create_spec('account')
        self.schema.create_field('account', 'name', 'str')

        self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'employees', 'linkcollection', 'user')
        self.schema.create_field('division', 'accounts', 'collection', 'account')

        self.schema.create_field('root', 'divisions', 'collection', 'division')

    def test_set_grants_on_resources(self):
        # add link to user
        self.schema.create_field('user', 'managing_division', 'link', 'division')

        # add collection to user
        self.schema.create_field('user', 'subordinates', 'linkcollection', 'user')

        # add some resources
        division_id_1 = self.updater.create_resource('division', 'root', 'divisions', None, {'name': 'sales'})

        # add some users
        bob_id = self.updater.create_user('bob', 'password')
        ned_id = self.updater.create_user('ned', 'password')

        # hook up users to resources
        # bob has 1 subordinate
        self.updater.update_fields('user', bob_id, {'managing_division': division_id_1})
        self.updater.create_linkcollection_entry('user', bob_id, 'subordinates', ned_id)

        # add grants
        self.managers_group_id = self.updater.create_resource(
            'group', 'root', 'groups', None, {'name': 'managers'},
            self.schema.read_root_grants('groups'))
        self.updater.create_resource('grant', 'group', 'grants', self.managers_group_id, {'type': 'read', 'url': '/divisions/'})

        # confirm grants exist
        bob = self.db['resource_user'].find_one({'username': 'bob'})

        # add ego grants, confirm



        # /ego/subordinates/*/sections
        # /ego/subordinates/*/sections/*/
