
from urllib.parse import urlparse, urljoin

import os

from pymongo import MongoClient

import flask
from flask import request
from flask import flash
from flask import url_for
from flask import Blueprint
from flask import current_app

from flask_login import login_user
from flask_login import logout_user
from flask_login import LoginManager
from flask_login import login_required
from werkzeug.security import check_password_hash

login_manager = LoginManager()

login_bp = Blueprint('login', __name__,
                     template_folder=os.path.join(
                        os.path.dirname(__file__), 'templates'),
                     static_folder=os.path.join(
                        os.path.dirname(__file__), 'static/accounts'),
                     static_url_path='/static/accounts')


@login_manager.user_loader
def load_user(password_hash):
    api = current_app.config['api']
    return api.schema.load_user_by_password_hash(password_hash)


def is_safe_url(target):
    # check if target is on same server as self
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and \
           ref_url.netloc == test_url.netloc


@login_bp.route('/login', methods=['GET', 'POST'])
def login():
    api = current_app.config['api']
    if request.method == 'POST':
        user = api.schema.load_user_by_username(request.form['username'])
        if user and check_password_hash(user.password, \
                                        request.form['password']):
            login_user(user)

            flash('Logged in successfully.')

            next_url = request.form.get('next')
            if next_url and not is_safe_url(next_url):
                return flask.abort(400)

            return flask.redirect(next_url or flask.url_for('index'))
    return flask.render_template('login.html',
                                 next_url=request.args.get('next') or '')


@login_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return flask.redirect(flask.url_for('index'))


def init_login(app):
    login_manager.init_app(app)
    login_manager.login_view = 'login.login'
