
import unittest

from metaphor.mongoclient_testutils import mongo_connection
from bson.objectid import ObjectId

from metaphor.schema_factory import SchemaFactory
from metaphor.api import Api
from metaphor.updater import Updater
from metaphor.lrparse.lrparse import parse


class UpdaterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = SchemaFactory(self.db).create_schema()
        self.schema.set_as_current()

        self.updater = Updater(self.schema)

        self.company_spec = self.schema.create_spec('company')
        self.employee_spec = self.schema.create_spec('employee')
        self.division_spec = self.schema.create_spec('division')

        self.schema.create_field('company', 'divisions', 'collection', 'division')

        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('division', 'employees', 'collection', 'employee')

        self.schema.create_field('root', 'companies', 'collection', 'company')


    def test_delete_group(self):
        self.admin_group_id = self.updater.create_resource('group', 'root', 'groups', None, {'name': 'admin'})

        self.grant_id = self.updater.create_resource('grant', 'group', 'grants', self.admin_group_id, {'type': 'read', 'url': '/companies'})

        self.user_id = self.updater.create_resource('user', 'root', 'users', None, {'email': 'bob'})

        self.updater.create_linkcollection_entry('user', self.user_id, 'groups', self.admin_group_id)

        user_db = self.db['resource_user'].find_one()

        self.updater.delete_linkcollection_entry('user', self.schema.decodeid(self.user_id), 'groups', self.admin_group_id)

        user_db = self.db['resource_user'].find_one()

