
from flask import Blueprint
from flask import render_template
from flask import current_app
from flask import request
from flask import jsonify
from flask import abort
from metaphor.login import login_required
from metaphor.login import admin_required
from metaphor.schema import Schema, CalcField
from metaphor.mutation import Mutation, MutationFactory
from metaphor.schema import DependencyException

from urllib.error import HTTPError

import flask_login

from bson.objectid import ObjectId

import logging
log = logging.getLogger(__name__)



auth_bp = Blueprint('auth', __name__, template_folder='templates',
                    static_folder='static', url_prefix='/auth')


@auth_bp.route("/", methods=['GET'])
def auth_root():
    return render_template('metaphor/auth_admin.html')


@auth_bp.route("/api/usergroups", methods=['GET'])
@admin_required
def all_groups():
    api = current_app.config['api']

    users = api.schema.db.resource_user.aggregate([
        {"$lookup": {
            "from": "metaphor_usergroup",
            "as": "usergroups",
            "localField": "_id",
            "foreignField": "user_id"
        }}
    ])
    def serialize(user):
        return {
            "user_id": api.schema.encodeid(user['_id']),
            "email": user['email'],
            "groups": [g['group_name'] for g in user['usergroups']],
        }
    return jsonify([serialize(ug) for ug in users])

@auth_bp.route("/api/usergroups/<group_name>/users", methods=['POST'])
@admin_required
def all_users_for_group(group_name):
    api = current_app.config['api']

    api.schema.add_user_to_group(group_name, api.schema.decodeid(request.json['user_id']))

    return jsonify({'ok': 1})


@auth_bp.route("/api/usergroups/<group_name>/users/<user_id>", methods=['DELETE'])
@admin_required
def single_usergroup(group_name, user_id):
    api = current_app.config['api']

    api.schema.remove_user_from_group(group_name, api.schema.decodeid(user_id))
    return jsonify({'ok': 1})


@auth_bp.route("/api/identities", methods=['GET', 'POST'])
@admin_required
def all_identities():
    api = current_app.config['api']

    def serialize(identity):
        return {
            "identity_id": identity['identity_id'],
            "user_id": identity['user_id'],
            "identity_type": identity['identity_type'],
            "email": identity['email'],
            "name": identity['name'],
        }

    if request.method == 'GET':
        identities = api.schema.get_identities()
        return jsonify([serialize(i) for i in identities])
    else:
        data = request.json
        api.updater.create_user_resource(
            data['email'],
            data['groups'],
            data['admin'])
        return jsonify({'ok': 1})
