
import unittest
from datetime import datetime
from urllib.error import HTTPError
import json
import gevent

from metaphor.mongoclient_testutils import mongo_connection

from flask_socketio import SocketIO, emit

from server import create_app
from metaphor.schema import Schema
from metaphor.api import Api, create_expand_dict


class ApiWatchTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        client = mongo_connection()
        client.drop_database('metaphor2_test_db')
        self.db = client.metaphor2_test_db
        self.schema = Schema.create_schema(self.db)
        self.schema.create_initial_schema()

        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'age', 'int')
        self.schema.create_field('employee', 'created', 'datetime')

        self.division_spec = self.schema.create_spec('division')
        self.schema.create_field('division', 'name', 'str')
        self.schema.create_field('division', 'yearly_sales', 'int')
        self.schema.create_field('division', 'parttimers', 'linkcollection', 'employee')

        self.schema.create_field('employee', 'division', 'link', 'division')

        self.section_spec = self.schema.create_spec('section')
        self.schema.create_field('section', 'name', 'str')
        self.schema.create_field('section', 'contractors', 'orderedcollection', 'employee')

        self.schema.create_field('division', 'sections', 'collection', 'section')

        self.schema.create_field('root', 'employees', 'collection', 'employee')
        self.schema.create_field('root', 'divisions', 'collection', 'division')

        self.schema.set_as_current()

        # create schema
        self.app = create_app(self.db)
        self.api = self.app.config['api']
        self.schema = self.api.schema

        # create initial data
        self.user_id = self.api.post('/users', {'username': 'bob', 'password': 'password'})
        self.group_id = self.api.post('/groups', {'name': 'manager'})
        self.grant_id_1 = self.api.post('/groups/%s/grants' % self.group_id, {'type': 'read', 'url': '/employees'})
        self.api.post('/users/%s/groups' % self.user_id, {'id': self.group_id})

        # create test clients
        self.client = self.app.test_client()

        # login
        self.client.post('/login', json={
            "username": "bob",
            "password": "password",
        }, follow_redirects=True)

        # create socketio client
        self.socketio = SocketIO(self.app)
        self.socketio_client = self.socketio.test_client(self.app, flask_test_client=self.client)

    def test_watch_resource(self):
        employee_id_1 = self.schema.insert_resource('employee', {'name': 'ned', 'age': 41}, 'employees')

        self.socketio_client.emit("add_resource", {"url": "/employees/%s" % employee_id_1})

        self.client.patch('/employees/%s' % employee_id_1, data=json.dumps({'name': 'bob'}), content_type='application/json')

        gevent.sleep(0)

        self.assertEqual([], self.socketio_client.get_received())
