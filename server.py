#!/usr/bin/env python

import os
import json

from gevent import monkey
monkey.patch_all()

from pymongo import MongoClient

from flask import Flask
from flask import jsonify
from flask import request
from flask import redirect
from flask import url_for

from metaphor.api import Api
from metaphor.admin_api import AdminApi
from metaphor.schema import Schema
from metaphor.api_bp import api_bp
from metaphor.api_bp import browser_bp
from metaphor.api_bp import admin_bp

import logging

logging.basicConfig(level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def create_app():
    client = MongoClient(os.environ.get('METAPHOR_MONGO_HOST', 'localhost'),
                         os.environ.get('METAPHOR_MONGO_PORT', 27017))
    db = client[os.environ['METAPHOR_DBNAME']]

    schema = Schema(db)
    schema.load_schema()

    app = Flask(__name__)
    app.config['api'] = Api(schema)
    app.config['admin_api'] = AdminApi(schema)
    app.register_blueprint(api_bp)
    app.register_blueprint(browser_bp)
    app.register_blueprint(admin_bp)

    return app


app = create_app()


@app.route("/")
def root():
    return redirect(url_for('browser.browser', path=''))
