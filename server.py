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

from metaphor.api import MongoApi
from metaphor.schema import Schema
from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec, CalcSpec
from metaphor.resource import ResourceLinkSpec

from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client['metaphor3']


schema = Schema(db, "0.1")


def exit_app():
    schema.updater.wait_for_updates()


def create_app():
    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'
    atexit.register(exit_app)
    schema.updater.start_updater()
    return app


app = create_app()

company_spec = ResourceSpec('company')
period_spec = ResourceSpec('period')
org_spec = ResourceSpec('org')
portfolio_spec = ResourceSpec('portfolio')

schema.add_resource_spec(company_spec)
schema.add_resource_spec(period_spec)
schema.add_resource_spec(org_spec)
schema.add_resource_spec(portfolio_spec)

company_spec.add_field("name", FieldSpec("string"))
company_spec.add_field("periods", CollectionSpec('period'))
company_spec.add_field("maxAssets", CalcSpec("max(self.periods.totalAssets)"))
company_spec.add_field("minAssets", CalcSpec("min(self.periods.totalAssets)"))
company_spec.add_field("averageGrossProfit", CalcSpec("average(self.periods.grossProfit)"))

period_spec.add_field("period", FieldSpec("string"))
period_spec.add_field("year", FieldSpec("int"))
period_spec.add_field("totalAssets", FieldSpec("int"))
period_spec.add_field("totalLiabilities", FieldSpec("int"))
period_spec.add_field("grossProfit", CalcSpec("self.totalAssets - self.totalLiabilities"))

org_spec.add_field("name", FieldSpec("string"))
org_spec.add_field("portfolios", CollectionSpec('portfolio'))

portfolio_spec.add_field("name", FieldSpec("string"))
portfolio_spec.add_field("companies", CollectionSpec('company'))

schema.add_root('companies', CollectionSpec('company'))
schema.add_root('organizations', CollectionSpec('org'))
schema.add_root('portfolios', CollectionSpec('portfolio'))

api = MongoApi("http://localhost:8000", schema, db)


@app.route("/api")
def api_root():
    return jsonify(api.get('/'))


@app.route("/api/<path:path>", methods=['GET', 'POST', 'DELETE', 'PATCH'])
def api_call(path):
    if request.method == 'POST':
        data = json.loads(request.data)
        return jsonify({'id': str(api.post(path, data))})
    elif request.method == 'DELETE':
        return jsonify({'id': str(api.unlink(path))})
    elif request.method == 'PATCH':
        data = json.loads(request.data)
        return jsonify({'id': str(api.patch(path, data))})
    else:
        resource = api.get(path)
        return jsonify(resource)


@app.route("/schema")
def schema_list():
    return [spec.serializer() for spec in schema.specs]


@app.route("/schema/<spec_name>", methods=['GET', 'POST', 'DELETE', 'PATCH'])
def schema_update(spec_name):
    if request.method == 'GET':
        return schema.specs[name].serialize()
    elif request.method == 'POST':
        pass


@app.route("/")
def root():
    return jsonify({
        "schema": "/schema",
        "api": "/api",
    })
