
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

@auth_bp.route("/api/oauth_clients", methods=['GET', 'POST'])
@admin_required
def oauth_clients():
    """Manage OAuth clients - list all or create new"""
    api = current_app.config['api']

    if request.method == 'GET':
        # List all OAuth clients
        clients = api.schema.get_oauth_clients()
        def serialize_client(client):
            return {
                "client_id": client.client_id,
                "name": client.name,
                "scopes": client.scopes,
                "created_at": client.created_at.isoformat() if client.created_at else None
            }
        return jsonify([serialize_client(c) for c in clients])

    else:  # POST
        # Create new OAuth client
        data = request.json
        if not data or 'name' not in data or 'scopes' not in data:
            return jsonify({'error': 'Missing required fields: name, scopes'}), 400

        name = data['name']
        scopes = data['scopes'] if isinstance(data['scopes'], list) else data['scopes'].split()

        try:
            client_id, client_secret = api.schema.create_oauth_client(name, scopes)
            return jsonify({
                'client_id': client_id,
                'client_secret': client_secret,
                'name': name,
                'scopes': scopes,
                'message': 'OAuth client created successfully. Store the client_secret securely - it will not be shown again.'
            }), 201
        except Exception as e:
            log.error(f"Failed to create OAuth client: {e}")
            return jsonify({'error': 'Failed to create OAuth client'}), 500


@auth_bp.route("/api/oauth_clients/<client_id>", methods=['DELETE'])
@admin_required
def delete_oauth_client(client_id):
    """Delete an OAuth client and all its tokens"""
    api = current_app.config['api']

    if api.schema.delete_oauth_client(client_id):
        return jsonify({'message': 'OAuth client deleted successfully'})
    else:
        return jsonify({'error': 'OAuth client not found'}), 404


@auth_bp.route("/api/oauth_clients/<client_id>/tokens", methods=['GET'])
@admin_required
def oauth_client_tokens(client_id):
    """List active tokens for an OAuth client"""
    api = current_app.config['api']

    # Verify client exists
    client = api.schema.load_oauth_client(client_id)
    if not client:
        return jsonify({'error': 'OAuth client not found'}), 404

    # Get active tokens
    tokens = api.schema.get_oauth_tokens_for_client(client_id)
    
    def serialize_token(token):
        return {
            "access_token": token.access_token[:8] + "..." if len(token.access_token) > 8 else token.access_token,
            "scopes": token.scopes,
            "expires_at": token.expires_at.isoformat(),
            "created_at": token.created_at.isoformat() if token.created_at else None
        }

    return jsonify([serialize_token(t) for t in tokens])


@auth_bp.route("/api/oauth_tokens/cleanup", methods=['POST'])
@admin_required
def cleanup_expired_tokens():
    """Clean up expired OAuth tokens"""
    api = current_app.config['api']

    deleted_count = api.schema.cleanup_expired_oauth_tokens()
    return jsonify({
        'message': f'Cleaned up {deleted_count} expired OAuth tokens'
    })


@auth_bp.route("/api/oauth_tokens/<access_token>", methods=['DELETE'])
@admin_required
def revoke_oauth_token(access_token):
    """Revoke a specific OAuth token"""
    api = current_app.config['api']

    if api.schema.revoke_oauth_token(access_token):
        return jsonify({'message': 'OAuth token revoked successfully'})
    else:
        return jsonify({'error': 'OAuth token not found'}), 404
