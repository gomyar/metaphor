#!/usr/bin/env python

import os
import json
import atexit
import pymongo
from bson.json_util import dumps

from gevent import monkey
monkey.patch_all()
import gevent

from pymongo import MongoClient

from flask import Flask
from flask import jsonify
from flask import request
from flask import redirect
from flask import url_for
from flask import current_app

from flask_socketio import SocketIO, emit
import flask_login
from flask_login import login_required

from metaphor.api import Api
from metaphor.admin_api import AdminApi
from metaphor.schema import Schema
from metaphor.api_bp import api_bp
from metaphor.api_bp import admin_bp
from metaphor.api_bp import search_bp
from metaphor.login_bp import login_bp
from metaphor.login_bp import init_login
from metaphor.client_bp import client_bp
from metaphor.integrations.runner import IntegrationRunner

import logging

logging.basicConfig(level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger()

integration_runner = None

def create_app(db):
    schema = Schema(db)
    schema.load_schema()

    api = Api(schema)

    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'
    app.config['api'] = api
    app.config['admin_api'] = AdminApi(schema)
    app.register_blueprint(api_bp)
    app.register_blueprint(client_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(search_bp)
    app.register_blueprint(login_bp)

    init_login(app)

    @app.route("/")
    def index():
        return redirect(url_for('client.client_root'))

    integration_runner = IntegrationRunner(api, db)
    integration_runner.start()

    atexit.register(integration_runner.stop)

    return app


client = MongoClient(os.environ.get('METAPHOR_MONGO_HOST', 'localhost'),
                        int(os.environ.get('METAPHOR_MONGO_PORT', 27017)))
db = client[os.environ.get('METAPHOR_DBNAME', 'metaphor')]

app = create_app(db)
socketio = SocketIO(app, async_mode='gevent')


socket_map = {}


@login_required
@socketio.on('connect')
def on_connect():
    log.debug("Connected %s %s", flask_login.current_user.username, request.sid)
    socket_map[request.sid] = {}


@login_required
@socketio.on('disconnect')
def on_disconnect():
    log.debug('Client disconnected %s', flask_login.current_user.username)


def watch_resource(watch, sid, url):
    log.debug("Listening to watch for %s: %s", sid, url)
    try:
        for change in watch:
            log.debug("Change occured: %s", change)
            socketio.emit("resource_update", {"url": url, "change": json.loads(dumps(change))}, room=sid)
    except pymongo.errors.OperationFailure as of:
        log.debug("Change stream closed for %s: %s", sid, url)
    if url in socket_map[sid]:
        log.debug("Removing stream for %s: %s", sid, url)
        mapped_socket = socket_map[sid].pop(url)


@login_required
@socketio.on('add_resource')
def add_resource(event):
    log.debug("add resource %s %s", flask_login.current_user.username, event)

    # establish watch
    api = current_app.config['api']
    watch = api.watch(event['url'], flask_login.current_user)

    # start gthread
    gthread = gevent.spawn(watch_resource, watch, request.sid, event['url'])

    socket_map[request.sid][event['url']] = {"watch": watch, "gthread": gthread}


@login_required
@socketio.on('remove_resource')
def remove_resource(event):
    log.debug("remove resource %s %s", flask_login.current_user.username, event)
    mapped_socket = socket_map[request.sid].pop(event['url'])
    mapped_socket['watch'].close()
    mapped_socket['gthread'].join()


@login_required
@socketio.on_error()        # Handles the default namespace
def error_handler(e):
    log.error("ERROR: %s", e)


if __name__ == '__main__':
    socketio.run(
        app,
        host=os.getenv('FLASK_RUN_HOST', '0.0.0.0'),
        port=int(os.getenv('FLASK_RUN_PORT', 8000)))
