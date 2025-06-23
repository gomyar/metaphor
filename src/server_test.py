
import json
from io import BytesIO
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

        self.schema.create_group("manager")
        self.schema.create_grant("manager", "read", "employees")
        self.schema.create_grant("manager", "read", "ego")
        self.schema.create_grant("manager", "create", "applications")
        self.schema.create_grant("manager", "create", "applications.resume")
        self.schema.create_grant("manager", "read", "applications.resume")
        self.schema.create_grant("manager", "delete", "applications.resume")

        self.user_id = self.api.updater.create_basic_user("bob", "password", ["manager"])

        response = self.client.post('/login', json={
            "email": "bob",
            "password": "password",
        }, follow_redirects=True)
        self.assertEqual(200, response.status_code)

    def test_get(self):
        response = self.client.get('/api/')
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'root'}, 'resource_type': 'resource'}}, response.json)

    def test_ego(self):
        response = self.client.get('/api/ego/')

        self.assertEqual('bob', response.json['email'])

    def test_group_access(self):
        self.schema.create_spec('client')
        self.schema.create_field('client', 'name', 'str')

        self.schema.create_field('root', 'clients', 'collection', 'client')

        client_id_1 = self.schema.insert_resource('client', {'name': 'fred'}, 'clients')

        response = self.client.get('/api/clients/%s' % client_id_1)
        self.assertEqual(403, response.status_code)

        self.schema.create_grant("manager", "read", "clients")

        response = self.client.get('/api/clients/%s' % client_id_1)
        self.assertEqual(200, response.status_code)
        self.assertEqual('fred', response.json['name'])

    def test_grant_group_permissions(self):
        self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        self.schema.create_grant("manager", "read", "employees")

        response = self.client.get('/api/employees/%s' % employee_id_1)
        self.assertEqual(200, response.status_code)

    def test_can_post_with_grant(self):
        employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        no_response = self.client.post('/api/employees', data=json.dumps({'name': 'wontblend'}), content_type='application/json')
        self.assertEqual(403, no_response.status_code)

        self.schema.create_grant("manager", "create", "employees")

        yes_response = self.client.post('/api/employees', data=json.dumps({'name': 'willblend'}), content_type='application/json')
        self.assertEqual(201, yes_response.status_code)

    def test_can_patch_with_grant(self):
        employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')

        self.schema.create_field('root', 'employees', 'collection', 'employee')

        employee_id_1 = self.api.post('/employees', {'name': 'fred'})

        no_response = self.client.patch('/api/employees/%s' % employee_id_1, data=json.dumps({'name': 'wontblend'}), content_type='application/json')
        self.assertEqual(403, no_response.status_code)

        self.schema.create_grant("manager", "update", "employees")

        yes_response = self.client.patch('/api/employees/%s' % employee_id_1, data=json.dumps({'name': 'willblend'}), content_type='application/json')
        self.assertEqual(200, yes_response.status_code)

    def test_serialize_password(self):
        user = self.api.get('/users/%s' % self.user_id)
        self.assertEqual({
            '_meta': {'is_collection': False, 'spec': {'name': 'user'}, 'resource_type': 'resource'},
            'admin': False,
            'email': 'bob',
            'id': self.user_id}, user)

    def test_patch_to_collection_returns_400(self):
        self.schema.create_grant("manager", "update", "")

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
        self.schema.create_grant("manager", "update", "ego.employee")

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

        self.schema.create_grant("manager", "read", "ego.organization")
        self.schema.create_grant("manager", "read", "ego.organization.sections")
        self.schema.create_grant("manager", "read", "ego.organization.sections.employees")

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
        self.schema.create_grant("manager", "read", "ego.organization")
        self.schema.create_grant("manager", "read", "ego.organization.sections")
        self.schema.create_grant("manager", "read", "ego.organization.sections.employees")

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

    def test_cannot_alter_current_schema(self):
        response = self.client.delete("/admin/api/schemas/{self.schema.id}")
        self.assertEqual(403, response.status_code)

    def test_upload_file(self):
        self.schema.create_spec('application')
        self.schema.create_field('application', 'name', 'str')
        self.schema.create_field('application', 'resume', 'file')
        self.schema.create_field('root', 'applications', 'collection', 'application')

        app_response = self.client.post('/api/applications',
            data=json.dumps({'name': 'test application'}),
            content_type='application/json')
        application_id = app_response.json

        response = self.client.post(
            f'/api/applications/{application_id}/resume',
            data=b'This is a test file content.',
            content_type='application/octet-stream')

        self.assertEqual(201, response.status_code)
        file_id = response.json

        # test get file contents
        file_response = self.client.get(f'/api/applications/{application_id}/resume')
        self.assertEqual(200, file_response.status_code)
        self.assertEqual("application/octet-stream", file_response.content_type)
        self.assertEqual(b'This is a test file content.', file_response.data)

        # test download file with specific filename
        file_response = self.client.get(f'/api/applications/{application_id}/resume?download=text_file.txt')
        self.assertEqual(200, file_response.status_code)
        self.assertEqual("application/octet-stream", file_response.content_type)
        self.assertEqual(b'This is a test file content.', file_response.data)
        self.assertEqual('attachment; filename="text_file.txt"', file_response.headers['Content-Disposition'])

        # test override content type
        file_response = self.client.get(f'/api/applications/{application_id}/resume?download=text_file.txt&contenttype=text/plain')
        self.assertEqual(200, file_response.status_code)
        self.assertEqual("text/plain", file_response.content_type)
        self.assertEqual(b'This is a test file content.', file_response.data)
        self.assertEqual('attachment; filename="text_file.txt"', file_response.headers['Content-Disposition'])

        # assert the size of the fs collection
        self.assertEqual(1, len(list(self.schema.db.fs.files.find({}))))

        # overwrite the file
        response = self.client.post(
            f'/api/applications/{application_id}/resume',
            data=b'This is new version.',
            content_type='application/octet-stream')

        # assert file is overridden
        file_response = self.client.get(f'/api/applications/{application_id}/resume')
        self.assertEqual(b'This is new version.', file_response.data)

        # assert the original file is deleted
        self.assertEqual(1, len(list(self.schema.db.fs.files.find({}))))

    def test_delete_uploaded_file(self):
        self.schema.create_spec('application')
        self.schema.create_field('application', 'name', 'str')
        self.schema.create_field('application', 'resume', 'file')
        self.schema.create_field('root', 'applications', 'collection', 'application')

        app_response = self.client.post('/api/applications',
            data=json.dumps({'name': 'test application'}),
            content_type='application/json')
        application_id = app_response.json

        # initially 404 when no file is uploaded
        no_response = self.client.get(f'/api/applications/{application_id}/resume')
        self.assertEqual(404, no_response.status_code)

        # upload a file
        response = self.client.post(
            f'/api/applications/{application_id}/resume',
            data=b'This is a test file content.',
            content_type='application/octet-stream')

        self.assertEqual(201, response.status_code)
        file_id = response.json

        # test delete file contents
        file_response = self.client.delete(f'/api/applications/{application_id}/resume')
        self.assertEqual(200, file_response.status_code)

        # assert the file is deleted
        no_response = self.client.get(f'/api/applications/{application_id}/resume')
        self.assertEqual(404, no_response.status_code)

        # assert the file is deleted
        self.assertEqual(0, len(list(self.schema.db.fs.files.find({}))))
