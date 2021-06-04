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
from metaphor.api_bp import search_bp
from metaphor.login_bp import login_bp
from metaphor.login_bp import init_login

import logging

logging.basicConfig(level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def create_app(db):
    schema = Schema(db)
    schema.load_schema()

    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'
    app.config['api'] = Api(schema)
    app.config['admin_api'] = AdminApi(schema)
    app.register_blueprint(api_bp)
    app.register_blueprint(browser_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(search_bp)
    app.register_blueprint(login_bp)

    init_login(app)

    return app


client = MongoClient(os.environ.get('METAPHOR_MONGO_HOST', 'localhost'),
                        int(os.environ.get('METAPHOR_MONGO_PORT', 27017)))
db = client[os.environ.get('METAPHOR_DBNAME', 'metaphor')]

app = create_app(db)


@app.route("/")
def index():
    return redirect(url_for('browser.browser', path=''))


if __name__ == '__main__':
    app.run(host=os.getenv('FLASK_RUN_HOST', '0.0.0.0'),
            port=int(os.getenv('FLASK_RUN_PORT', 8000)))
