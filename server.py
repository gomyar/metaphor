#!/usr/bin/env python

import json

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
db = client['metaphor']


def create_app():
    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'
    return app


app = create_app()

schema = Schema(db, "0.1")

company_spec = ResourceSpec('company')
period_spec = ResourceSpec('period')
org_spec = ResourceSpec('org')
portfolio_spec = ResourceSpec('portfolio')
financial_spec = ResourceSpec("financial")

schema.add_resource_spec(company_spec)
schema.add_resource_spec(period_spec)
schema.add_resource_spec(org_spec)
schema.add_resource_spec(portfolio_spec)
schema.add_resource_spec(financial_spec)

company_spec.add_field("name", FieldSpec("string"))
company_spec.add_field("periods", CollectionSpec('period'))

period_spec.add_field("period", FieldSpec("string"))
period_spec.add_field("year", FieldSpec("int"))
financial_spec.add_field("totalAssets", FieldSpec("int"))
financial_spec.add_field("totalLiabilities", FieldSpec("int"))
financial_spec.add_field("grossProfit", CalcSpec("self.totalAssets - self.totalLiabilities"))
period_spec.add_field("financial", ResourceLinkSpec('financial'))

org_spec.add_field("name", FieldSpec("string"))
org_spec.add_field("portfolios", CollectionSpec('portfolio'))

portfolio_spec.add_field("name", FieldSpec("string"))
portfolio_spec.add_field("companies", CollectionSpec('company'))

schema.add_root('companies', CollectionSpec('company'))
schema.add_root('organizations', CollectionSpec('org'))
schema.add_root('portfolios', CollectionSpec('portfolio'))

api = MongoApi("http://localhost:5000/api", schema, db)


#@app.route("/")
#def index():
#    return render_template("index.html")


@app.route("/schema")
def schema():
    return jsonify({
        'functions': {
            'space_periods': {'module': 'extensions.space_periods',
                              'args': ['periods'],
            },
        },
        'root': {
            'companies': {'type': 'collection', 'target': 'company'},
        },
        'resources': {
            'company': {
                'fields': {
                    'name': {'type': 'str'},
                    'address': {'type': 'str'},
                    'periods': {'type': 'collection', 'target': 'period'},
                    'spaced_periods': {'type': 'collection', 'calc': 'space_periods(self.periods)'},
                    'sector': {'type': 'link', 'target': {'type': 'sector'}},
            }},
            'period': {
                'year': {'type': 'int'},
                'period': {'type': 'str'},
                'total_assets': {'type': 'float'},
                'total_liabilities': {'type': 'float'},
                'net_assets': {'type': 'float', 'calc': 'total_assets - total_liabilities'},
                'previous_year_period': {'type': 'link', 'calc': 'self.company.spaced_periods[-4]'},
                'previous_quarter_period': {'type': 'link', 'calc': 'self.company.spaced_periods[-1]'},
            }
        }})


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
