#!/usr/bin/env python

import os
import sys
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
from metaphor.schema_factory import SchemaFactory
from metaphor.api_bp import api_bp
from metaphor.api_bp import admin_bp
from metaphor.api_bp import search_bp
from metaphor.login_bp import login_bp
from metaphor.login_bp import init_login
from metaphor.client_bp import client_bp

import logging

log = logging.getLogger()
log.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


def create_app(db):
    schema_factory = SchemaFactory(db)

    api = Api(db)

    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'
    app.config['api'] = api
    app.config['schema_factory'] = schema_factory
    app.register_blueprint(api_bp)
    app.register_blueprint(client_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(search_bp)
    app.register_blueprint(login_bp)

    init_login(app)

    @app.route("/")
    def index():
        return redirect(url_for('client.client_root'))

    return app


client = MongoClient(os.environ.get('METAPHOR_MONGO_HOST', 'mongodb://127.0.0.1:27017'))
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
    sockets = socket_map.pop(request.sid)
    for url in sockets:
        log.debug("Cleaning listen for: %s", url)
        sockets[url]['watch'].close()


def watch_resource(watch, sid, url):
    log.debug("Listening to watch for %s: %s", sid, url)
    try:
        for change in watch:
            log.debug("Change occured: %s", change)
            change.pop('_id')
            socketio.emit("resource_update", {"url": url, "change": json.loads(dumps(change))}, room=sid)
    except pymongo.errors.OperationFailure as of:
        log.debug("Change stream closed for %s: %s", sid, url)
    if url in socket_map.get(sid, {}):
        log.debug("Removing stream for %s: %s", sid, url)
        mapped_socket = socket_map[sid].pop(url)
        socketio.emit("lost_stream", {"url": url}, room=sid)


@login_required
@socketio.on('add_resource')
def add_resource(event):
    log.debug("add resource %s %s", flask_login.current_user.username, event)

    if event['url'] in socket_map[request.sid]:
        log.debug("Add resource already added: %s", event['url'])
        return

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
    log.error("ERROR: %s: %s", type(e), e)


if __name__ == '__main__':
    socketio.run(
        app,
        host=os.getenv('FLASK_RUN_HOST', '0.0.0.0'),
        port=int(os.getenv('FLASK_RUN_PORT', 8000)))
