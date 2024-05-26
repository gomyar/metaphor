
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

        self.schema.create_initial_schema()


        self.company_spec = self.schema.create_spec('company')
        self.employee_spec = self.schema.create_spec('employee')
        self.division_spec = self.schema.create_spec('division')

        self.schema.create_field('company', 'divisions', 'collection', 'division')

        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('division', 'employees', 'collection', 'employee')

        self.schema.create_field('root', 'companies', 'collection', 'company')


    def test_delete_group(self):
        self.admin_group_id = self.updater.create_resource('group', 'root', 'groups', None, {'name': 'admin'}, self.schema.read_root_grants('groups'))

        self.grant_id = self.updater.create_resource('grant', 'group', 'grants', self.admin_group_id, {'type': 'read', 'url': '/companies'})

        self.user_id = self.updater.create_resource('user', 'root', 'users', None, {'username': 'bob', 'password': 'hash', 'admin': True}, self.schema.read_root_grants('users'))

        self.updater.create_linkcollection_entry('user', self.user_id, 'groups', self.admin_group_id)

        user_db = self.db['metaphor_resource'].find_one({'_type': 'user'})
        self.assertEqual(1, len(user_db['read_grants']))

        self.updater.delete_linkcollection_entry('user', self.schema.decodeid(self.user_id), 'groups', self.admin_group_id)

        user_db = self.db['metaphor_resource'].find_one({'_type': 'user'})
        self.assertEqual(0, len(user_db['read_grants']))

    def test_root_grants(self):
        group_id = self.updater.create_resource('group', 'root', 'groups', None, {'name': 'readall'}, self.schema.read_root_grants('groups'))
        grant_id = self.updater.create_resource('grant', 'group', 'grants', group_id, {'type': 'read', 'url': '/'})

        company_id = self.updater.create_resource('company', 'root', 'companies', None, {}, self.schema.read_root_grants('companies'))

        company_data = self.db['metaphor_resource'].find_one({'_type': 'company'})
        self.assertEqual([self.schema.decodeid(grant_id)], company_data['_grants'])

    def test_nested_grants(self):
        group_id = self.updater.create_resource('group', 'root', 'groups', None, {'name': 'readall'}, self.schema.read_root_grants('groups'))
        grant_id = self.updater.create_resource('grant', 'group', 'grants', group_id, {'type': 'read', 'url': '/'})

        company_id = self.updater.create_resource('company', 'root', 'companies', None, {}, self.schema.read_root_grants('companies'))

        company_path = "companies/%s" % company_id
        division_id_1 = self.updater.create_resource('division', 'company', 'divisions', company_id, {}, self.schema.read_root_grants(company_path))
        division_id_2 = self.updater.create_resource('division', 'company', 'divisions', company_id, {}, self.schema.read_root_grants(company_path))

        division_1_path = "companies/%s/divisions/%s" % (company_id, division_id_1)
        employee_id_1 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {}, self.schema.read_root_grants(division_1_path))
        employee_id_2 = self.updater.create_resource('employee', 'division', 'employees', division_id_1, {}, self.schema.read_root_grants(division_1_path))

        division_2_path = "companies/%s/divisions/%s" % (company_id, division_id_2)
        employee_id_3 = self.updater.create_resource('employee', 'division', 'employees', division_id_2, {}, self.schema.read_root_grants(division_2_path))
        employee_id_4 = self.updater.create_resource('employee', 'division', 'employees', division_id_2, {}, self.schema.read_root_grants(division_2_path))

        # check grants
        company_data = self.db['metaphor_resource'].find_one({"_type": "company"})
        self.assertEqual([self.schema.decodeid(grant_id)], company_data['_grants'])

        division_1_data = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(division_id_1)})
        self.assertEqual([self.schema.decodeid(grant_id)], division_1_data['_grants'])

        division_2_data = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(division_id_2)})
        self.assertEqual([self.schema.decodeid(grant_id)], division_2_data['_grants'])

        employee_1_data = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(employee_id_1)})
        self.assertEqual([self.schema.decodeid(grant_id)], employee_1_data['_grants'])

        employee_2_data = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(employee_id_2)})
        self.assertEqual([self.schema.decodeid(grant_id)], employee_2_data['_grants'])

        employee_3_data = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(employee_id_3)})
        self.assertEqual([self.schema.decodeid(grant_id)], employee_3_data['_grants'])

        employee_4_data = self.db['metaphor_resource'].find_one({"_id": self.schema.decodeid(employee_id_4)})
        self.assertEqual([self.schema.decodeid(grant_id)], employee_4_data['_grants'])
