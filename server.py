#!/usr/bin/env python

import os
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

from metaphor.api import MongoApi
from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec, CalcSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.api_bp import api_bp
from metaphor.schema_bp import schema_bp

from pymongo import MongoClient

def exit_app():
    schema.updater.wait_for_updates()


root_url = "http://localhost:8000"


def create_app():
    client = MongoClient(os.environ.get('METAPHOR_MONGO_HOST', 'localhost'),
                         os.environ.get('METAPHOR_MONGO_PORT', 27017))
    db = client[os.environ['METAPHOR_DBNAME']]

    schema_data = db['metaphor_schema'].find_one({})
    if schema_data:
        schema = SchemaFactory().create_schema(db, "0.1", schema_data)
    else:
        schema = Schema(db, "0.1")

    db['metaphor_schema'].insert(SchemaFactory().serialize_schema(schema))

    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'

    app.config['api'] = MongoApi(root_url, schema, db)
    app.config['schema'] = schema

    app.register_blueprint(api_bp)
    app.register_blueprint(schema_bp)

    atexit.register(exit_app)
    schema.updater.start_updater()
    return app


app = create_app()



@app.route("/")
def root():
    return jsonify({
        "schema": root_url + "/schema",
        "api": root_url + "/api",
    })
