#!/usr/bin/env python

import json

from gevent import monkey
monkey.patch_all()

import atexit

from flask import Flask
from flask import redirect
from flask import render_template
from flask import jsonify
from flask import request
from flask import send_from_directory

from metaphor.api import MongoApi, SchemaApi, RootsApi
from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec, CalcSpec
from metaphor.resource import ResourceLinkSpec

from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client['metaphor3']


if 'metaphor_schema' in db.collection_names():
    schema_data = db['metaphor_schema'].find_one({})
    schema = SchemaFactory().create_schema(db, "0.1", schema_data)
else:
    schema_data = {'specs': {}, 'roots': {}}
    schema = SchemaFactory().create_schema(db, "0.1", schema_data)
    db['metaphor_schema'].insert(schema_data)


def exit_app():
    schema.updater.wait_for_updates()


def create_app():
    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'
    atexit.register(exit_app)
    schema.updater.start_updater()
    return app


app = create_app()


root_url = "http://localhost:8000"
api = MongoApi(root_url, schema, db)
schema_api = SchemaApi(root_url, schema, db)
roots_api = RootsApi(root_url, schema, db)


@app.route("/api")
def api_root():
    return jsonify(api.get('/'))


@app.route("/api/<path:path>", methods=['GET', 'POST', 'DELETE', 'PATCH'])
def api_call(path):
    if request.method == 'POST':
        return jsonify({'id': str(api.post(path, request.json))})
    elif request.method == 'DELETE':
        return jsonify({'id': str(api.unlink(path))})
    elif request.method == 'PATCH':
        return jsonify({'id': str(api.patch(path, request.json))})
    else:
        resource = api.get(path)
        return jsonify(resource)


@app.route("/schema", methods=['GET', 'POST'])
def schema_list():
    if request.method == 'POST':
        schema_api.post(None, request.json)
        return jsonify({})
    if request.method == 'GET':
        return jsonify([spec.serialize() for spec in schema.specs.values()])


@app.route("/schema/<spec_name>", methods=['GET', 'PATCH'])
def schema_update(spec_name):
    if request.method == 'GET':
        return schema.specs[name].serialize()
    elif request.method == 'PATCH':
        schema_api.patch(spec_name, request.json)
        return jsonify({})


@app.route("/roots", methods=['GET', 'POST'])
def roots_list():
    if request.method == 'GET':
        return jsonify([spec.serialize for spec in schema.root_spec.fields.values()])
    if request.method == 'POST':
        roots_api.post(request.json['name'], request.json['target'])
        return jsonify({})


@app.route("/")
def root():
    return jsonify({
        "schema": api.root_url + "/schema",
        "api": api.root_url + "/api",
        "roots": api.root_url + "/roots",
    })
