
from urllib.parse import urlparse, urljoin

import os
import base64

from pymongo import MongoClient

import flask
from flask import request
from flask import flash
from flask import url_for
from flask import Blueprint
from flask import current_app
from flask import jsonify

from flask_login import login_user
from flask_login import logout_user
from flask_login import LoginManager
from metaphor.login import login_required
from werkzeug.security import check_password_hash

login_manager = LoginManager()

login_bp = Blueprint('login', __name__,
                     template_folder=os.path.join(
                        os.path.dirname(__file__), 'templates'),
                     static_folder=os.path.join(
                        os.path.dirname(__file__), 'static/accounts'),
                     static_url_path='/static/accounts')


def load_user_from_basic_header():
    api = current_app.config['api']
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Basic '):
        encoded_credentials = auth_header.split(' ')[1]
        try:
            decoded = base64.b64decode(encoded_credentials).decode('utf-8')
            email, password = decoded.split(':', 1)

            identity = api.schema.load_identity("basic", email=email)
            if identity and check_password_hash(identity.password, password):
                return identity
        except Exception as e:
            return None
    return None


@login_manager.user_loader
def load_user(session_id):
    api = current_app.config['api']
    identity = load_user_from_basic_header()
    if not identity:
        return api.schema.load_identity_by_session_id(session_id)
    else:
        return identity


@login_bp.route('/login', methods=['GET', 'POST'])
def login():
    api = current_app.config['api']
    if request.method == 'POST':
        if not request.json:
            return "Must use application/json content", 400
        identity = api.schema.load_identity("basic", request.json['email'])
        if identity and check_password_hash(identity.password, \
                                            request.json['password']):
            if not identity.session_id:
                api.schema.update_identity_session_id(identity)
            login_user(identity)

            flash('Logged in successfully.')

            return jsonify({"ok": 1}), 200
        else:
            return jsonify({"error": "login incorrect"}), 401
    else:
        return flask.render_template('login.html')


@login_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return flask.redirect(flask.url_for('index'))


def init_login(app):
    login_manager.init_app(app)
    login_manager.login_view = 'login.login'
