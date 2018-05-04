

import os
from pymongo import MongoClient

import atexit

from flask import Flask
from flask import redirect
from flask import render_template
from flask import request
from flask import send_from_directory

from metaphor.api import MongoApi
from metaphor.schema import Schema
from metaphor.schema_factory import SchemaFactory
from metaphor.api_bp import api_bp
from metaphor.schema_bp import schema_bp
from metaphor.admin_bp import admin_bp


def create_app():
    client = MongoClient(os.environ.get('METAPHOR_MONGO_HOST', 'localhost'),
                         os.environ.get('METAPHOR_MONGO_PORT', 27017))
    db = client[os.environ['METAPHOR_DBNAME']]

    schema = SchemaFactory().load_schema(db)

    app = Flask(__name__)

    app.config['api'] = MongoApi(os.environ['SERVER_NAME'], schema, db)
    app.config['schema'] = schema

    app.register_blueprint(api_bp)
    app.register_blueprint(schema_bp)
    app.register_blueprint(admin_bp)

    schema.updater.start_updater()

    def exit_app():
        schema.updater.wait_for_updates()

    atexit.register(exit_app)

    return app
