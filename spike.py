#!/usr/bin/env python

from flask import Flask
from flask import redirect
from flask import render_template
from flask import jsonify
from flask import request
from flask import send_from_directory



def create_app():
    app = Flask(__name__)
    app.secret_key = 'keepitsecretkeepitsafe'
    return app


app = create_app()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/schema")
def api():
    return jsonify({
        'functions': [
            'space_periods': {'module': 'extensions.space_periods',
                              'args': ['periods'],
            },
        ],
        'root': {
            'companies': {'type': 'collection', 'target': 'company'},
        },
        'resources': [
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
        ]})


@app.route("/api")
def api():
    return jsonify({'api': [1,2,3]})
