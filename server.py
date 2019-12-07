#!/usr/bin/env python

import os
import json

from gevent import monkey
monkey.patch_all()

from flask import jsonify

from metaphor.flaskapp import create_app

import logging

#if os.path.exists(os.path.join(os.path.dirname(__file__), "logging.conf")):
#    logging.config.fileConfig(os.path.join(os.path.dirname(__file__),
#        "logging.conf"))
#else:
#    logging.basicConfig(level=logging.DEBUG,
#        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


logging.basicConfig(level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

app = create_app()


@app.route("/")
def root():
    return jsonify({
        "schema": "http://%s/schema" % os.environ['SERVER_NAME'],
        "api": "http://%s/api" % os.environ['SERVER_NAME'],
    })
