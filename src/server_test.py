
import json
from unittest import TestCase
from server import create_app
from metaphor.mongoclient_testutils import mongo_connection
from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory
from werkzeug.security import generate_password_hash, check_password_hash


class ServerTest(TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db

        schema = SchemaFactory(self.db).create_schema()
        schema.set_as_current()

        self.app = create_app(self.db)
        self.api = self.app.config['api']
        self.schema = self.api.schema

        self.client = self.app.test_client()

        self.user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})
        self.group_id = self.api.post('/groups', {'name': 'manager'})
        self.grant_id_1 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'employees'})
        self.grant_id_2 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'ego'})
        self.api.post('/users/%s/groups' % self.user_id, {'id': self.group_id})

        self.client.post('/login', json={
            "username": "bob",
            "password": "password",
        }, follow_redirects=True)

    def test_get(self):
        response = self.client.get('/api/')
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'root'}, 'resource_type': 'resource'}}, response.json)

    def test_ego(self):
        response = self.client.get('/api/ego/')

        self.assertEqual('bob', response.json['username'])

    def test_group_access(self):
        self.schema.create_spec('client')
        self.schema.create_field('client', 'name', 'str')

        self.schema.create_field('root', 'clients', 'collection', 'client')

        client_id_1 = self.schema.insert_resource('client', {'name': 'fred'}, 'clients')

        response = self.client.get('/api/clients/%s' % client_id_1)
        self.assertEqual(403, response.status_code)

        grant_id = self.api.post('/groups/%s/grants' % self.group_id, {'url': 'clients', 'type': 'read'})

        response = self.client.get('/api/clients/%s' % client_id_1)
        self.assertEqual(200, response.status_code)
        self.assertEqual('fred', response.json['name'])

    def test_grant_group_permissions(self):
        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        grant_id = self.api.post('/groups/%s/grants' % self.group_id, {'url': 'employees', 'type': 'read'})

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(200, response.status_code)

    def test_can_post_with_grant(self):
        employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        no_response = self.client.post('/api/employees', data=json.dumps({'name': 'wontblend'}), content_type='application/json')
        self.assertEqual(403, no_response.status_code)

        grant_id_2 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'create', 'url': 'employees'})

        yes_response = self.client.post('/api/employees', data=json.dumps({'name': 'willblend'}), content_type='application/json')
        self.assertEqual(201, yes_response.status_code)

    def test_can_patch_with_grant(self):
        employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        no_response = self.client.patch('/api/employees/%s' % employee_id_1, data=json.dumps({'name': 'wontblend'}), content_type='application/json')
        self.assertEqual(403, no_response.status_code)

        grant_id_2 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'update', 'url': 'employees'})

        yes_response = self.client.patch('/api/employees/%s' % employee_id_1, data=json.dumps({'name': 'willblend'}), content_type='application/json')
        self.assertEqual(200, yes_response.status_code)

    def test_delete_grant_updates_user_grants(self):
        company_spec = self.schema.create_spec('company')
        self.schema.create_field('company', 'name', 'str')

        self.schema.create_field('root', 'companies', 'collection', 'company')

        grant_1 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'create', 'url': 'companies'})

        user = self.db['resource_user'].find_one({"_id": self.schema.decodeid(self.user_id)})

        self.assertEqual([{'_id': self.schema.decodeid(grant_1)}], user['create_grants'])

        # delete link to group
        self.api.delete('/users/%s/groups/%s' % (self.user_id, self.group_id))

        # assert grants are removed
        user = self.db['resource_user'].find_one({"_id": self.schema.decodeid(self.user_id)})
        self.assertEqual([], user['create_grants'])

    def test_delete_group_updates_user_grants(self):
        company_spec = self.schema.create_spec('company')
        self.schema.create_field('company', 'name', 'str')

        self.schema.create_field('root', 'companies', 'collection', 'company')

        grant_1 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'create', 'url': 'companies'})

        user = self.db['resource_user'].find_one({"_id": self.schema.decodeid(self.user_id)})

        self.assertEqual([{'_id': self.schema.decodeid(grant_1)}], user['create_grants'])

        # delete link to group
        self.api.delete('/groups/%s' % (self.group_id))

        # assert grants are removed
        user = self.db['resource_user'].find_one({"_id": self.schema.decodeid(self.user_id)})
        self.assertEqual([], user['create_grants'])

    def test_delete_group_deletes_child_grants(self):
        pass

    def test_delete_grants_deletes_grants_in_resources(self):
        pass

    def test_serialize_password(self):
        user = self.api.get('/users/%s' % self.user_id)
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'user'}, 'resource_type': 'resource'},
            'admin': None,
            'id': self.user_id,
            'password': '<password>',
            'username': 'bob'}, user)

    def test_create_password(self):
        user_id = self.api.post('/users', {'username': 'fred', 'password': 'secret'})
        user = self.db['resource_user'].find_one({"_id": self.schema.decodeid(user_id)})
        self.assertTrue(check_password_hash(user['password'], 'secret'))

    def test_patch_password(self):
        self.api.patch('/users/%s' % self.user_id, {'password': 'secret'})
        user = self.db['resource_user'].find_one({"_id": self.schema.decodeid(self.user_id)})
        self.assertTrue(check_password_hash(user['password'], 'secret'))

    def test_patch_to_collection_returns_400(self):
        self.grant_id_1 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'update', 'url': ''})

        no_response = self.client.patch('/api/users', data=json.dumps({'name': 'fred'}), content_type='application/json')
        self.assertEqual(400, no_response.status_code)

    def test_grant_ego_permissions(self):
        employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        # add employee link to user
        user_spec = self.schema.specs['user']
        self.schema.create_field('user', 'employee', 'link', 'employee')

        # link user to employee
        self.api.patch('/users/%s' % self.user_id, {'employee': employee_id_1})

        no_response = self.client.patch('/api/ego/employee', data=json.dumps({'name': 'bob'}), content_type='application/json')
        self.assertEqual(403, no_response.status_code)

        # add grant for /ego/employee
        self.api.post('/groups/%s/grants' % self.group_id, {'url': 'ego.employee', 'type': 'update'})

        no_response = self.client.patch('/api/ego/employee', data=json.dumps({'name': 'bob'}), content_type='application/json')
        self.assertEqual(200, no_response.status_code)

        self.assertEqual('bob', self.api.get('/employees/%s' % employee_id_1)['name'])

    def test_indirect_link_access(self):
        employee_spec = self.schema.create_spec('employee')
        contract_spec = self.schema.create_spec('contract')

        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'contracts', 'linkcollection', 'employee')

        self.schema.create_field('contract', 'name', 'str')
        self.schema.create_field('contract', 'price', 'int')

        self.schema.create_field('root', 'employees', 'collection', 'employee')
        self.schema.create_field('root', 'contracts', 'collection', 'contract')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})
        contract_id_1 = self.api.post('/contracts', {'name': 'IBM', 'price': 100})

        # add linkcollection / link to other root collection with grant
        self.api.post('/employees/%s/contracts' % employee_id_1, {'id': contract_id_1})

        # assert user can access through link
        response = self.api.get('/employees/%s/contracts' % employee_id_1)
        self.assertEqual({
            '_meta': {'is_collection': True,
                'resource_type': 'linkcollection',
                'spec': {'name': 'employee'}},
            'count': 0,
            'page': 0,
            'page_size': 10,
            'next': None,
            'previous': None,
            'results': []}, response)

        # assert expand also

    def test_grant_ego_permissions_expand(self):
        organization_spec = self.schema.create_spec('organization')
        section_spec = self.schema.create_spec('section')
        employee_spec = self.schema.create_spec('employee')

        self.schema.create_field('organization', 'name', 'str')
        self.schema.create_field('organization', 'sections', 'linkcollection', 'section')
        self.schema.create_field('section', 'name', 'str')
        self.schema.create_field('section', 'employees', 'linkcollection', 'employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'organizations', 'collection', 'organization')
        self.schema.create_field('root', 'sections', 'collection', 'section')
        self.schema.create_field('root', 'employees', 'collection', 'employee')

        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'ego.organization'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'ego.organization.sections'})
        self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': 'ego.organization.sections.employees'})

        organization_id_1 = self.api.post('/organizations', {'name': 'fred'})
        section_id_1 = self.api.post('/sections', {'name': 'fred'})
        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        self.api.post('/organizations/%s/sections' % organization_id_1, {"id": section_id_1})
        self.api.post('/sections/%s/employees' % section_id_1, {"id": employee_id_1})

        # add employee link to user
        user_spec = self.schema.specs['user']
        self.schema.create_field('user', 'organization', 'link', 'organization')

        # link user to employee
        self.api.patch('/users/%s' % self.user_id, {'organization': organization_id_1})

        # add grant for /ego/employee
        self.api.post('/groups/%s/grants' % self.group_id, {'url': 'ego.organization', 'type': 'get'})
        self.api.post('/groups/%s/grants' % self.group_id, {'url': 'ego.organization.sections', 'type': 'get'})
        self.api.post('/groups/%s/grants' % self.group_id, {'url': 'ego.organization.sections.employees', 'type': 'get'})

        # request expand
        response = self.client.get('/api/ego/organization?expand=sections.employees')
        self.assertEqual(200, response.status_code)

        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'organization'}, 'resource_type': 'resource'},
            'id': organization_id_1,
            'name': 'fred',
            'sections': [
                {'_meta': {'is_collection': False, 'spec': {'name': 'section'}, 'resource_type': 'resource'},
                 'employees': [
                    {'_meta': {'is_collection': False, 'spec': {'name': 'employee'}, 'resource_type': 'resource'},
                     'id': employee_id_1,
                     'name': 'fred',}
                ],
                'id': section_id_1,
                'name': 'fred',}
            ],}
        , response.json)

        api_schema = self.client.get('/api/schema').json
        self.assertEqual(['fields', 'name', 'type'], list(api_schema['root'].keys()))
