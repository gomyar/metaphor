#!/usr/bin/env python

import os
import json

from gevent import monkey
monkey.patch_all()

from flask import jsonify

from metaphor.flaskapp import create_app


app = create_app()


@app.route("/")
def root():
    return jsonify({
        "schema": "http://%s/schema" % os.environ['SERVER_NAME'],
        "api": "http://%s/api" % os.environ['SERVER_NAME'],
    })
