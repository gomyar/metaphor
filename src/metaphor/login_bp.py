
from urllib.parse import urlparse, urljoin

import os

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


@login_manager.user_loader
def load_user(session_id):
    api = current_app.config['api']
    return api.schema.load_identity_by_session_id(session_id)


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
