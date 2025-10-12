
import os
import base64
import logging

from flask import Blueprint, redirect, url_for, session, request, jsonify
from authlib.integrations.flask_client import OAuth
from authlib.integrations.base_client.errors import OAuthError
from flask import current_app
from flask_login import login_user

log = logging.getLogger(__name__)

oauth_bp = Blueprint('oauth', __name__, url_prefix='/oauth')
oauth = OAuth()


def init_oauth(app):
    oauth.init_app(app)

    if os.environ.get('GOOGLE_CLIENT_ID'):
        oauth.register(
            name='google',
            client_id=os.environ['GOOGLE_CLIENT_ID'],
            client_secret=os.environ['GOOGLE_CLIENT_SECRET'],
            access_token_url='https://oauth2.googleapis.com/token',
            authorize_url='https://accounts.google.com/o/oauth2/auth',
            jwks_uri="https://www.googleapis.com/oauth2/v3/certs",
            client_kwargs={'scope': 'openid email profile'}
        )

    if os.environ.get('GITHUB_CLIENT_ID'):
        oauth.register(
            name='github',
            client_id=os.environ['GITHUB_CLIENT_ID'],
            client_secret=os.environ['GITHUB_CLIENT_SECRET'],
            access_token_url='https://github.com/login/oauth/access_token',
            authorize_url='https://github.com/login/oauth/authorize',
            client_kwargs={'scope': 'user:email'}
        )


@oauth_bp.route('/login/<provider>')
def login(provider):
    if provider == 'google' and not os.environ.get('GOOGLE_CLIENT_ID'):
        return jsonify({"error": "Unsupported provider"}), 400
    if provider == 'github' and not os.environ.get('GITHUB_CLIENT_ID'):
        return jsonify({"error": "Unsupported provider"}), 400

    redirect_uri = url_for('oauth.callback', provider=provider, _external=True)
    return oauth.create_client(provider).authorize_redirect(redirect_uri)


@oauth_bp.route('/callback/<provider>')
def callback(provider):
    api = current_app.config['api']

    if provider == 'google' and not os.environ.get('GOOGLE_CLIENT_ID'):
        return jsonify({"error": "Unsupported provider"}), 400
    if provider == 'github' and not os.environ.get('GITHUB_CLIENT_ID'):
        return jsonify({"error": "Unsupported provider"}), 400

    client = oauth.create_client(provider)
    try:
        token = client.authorize_access_token()
    except OAuthError as oe:
        return jsonify({"error": "Access token invalid"}), 401

    if provider == 'google':
        user_info = client.get('https://www.googleapis.com/oauth2/v3/userinfo').json()
        email = user_info.get("email")
        name = user_info.get("name")
        profile_url = user_info.get("picture")

    elif provider == 'github':
        user_info = client.get('https://api.github.com/user').json()
        email_response = client.get('https://api.github.com/user/emails').json()
        email = next((e['email'] for e in email_response if e.get('primary')), None)
        name = user_info.get("name")
        profile_url = user_info.get("avatar_url")

    else:
        return jsonify({"error": "Provider not supported"}), 400

    user = api.schema.load_user_by_email(email)
    if not user:
        return jsonify({"error": "Your identity does not have access to this service"}), 403

    identity = api.schema.get_or_create_identity(provider, user.user_id, email, name, profile_url)
    if not identity.session_id:
        api.schema.update_identity_session_id(identity)
    login_user(identity)

    return redirect('/')


# OAuth 2.0 Client Credentials Flow (Machine-to-Machine Authentication)

@oauth_bp.route('/token', methods=['POST'])
def token():
    """
    OAuth 2.0 Client Credentials Grant Token Endpoint

    Handles POST /oauth/token with grant_type=client_credentials
    Expects client authentication via Authorization header (Basic Auth)
    """
    api = current_app.config['api']

    # Verify grant type
    grant_type = request.form.get('grant_type')
    if grant_type != 'client_credentials':
        return jsonify({
            "error": "unsupported_grant_type",
            "error_description": "Only client_credentials grant type is supported"
        }), 400

    # Extract client credentials from Authorization header
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Basic '):
        return jsonify({
            "error": "invalid_client",
            "error_description": "Client authentication required via Basic Auth"
        }), 401

    try:
        # Decode Basic Auth credentials
        encoded_credentials = auth_header[6:]  # Remove "Basic " prefix
        decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
        client_id, client_secret = decoded_credentials.split(':', 1)
    except (ValueError, UnicodeDecodeError):
        return jsonify({
            "error": "invalid_client",
            "error_description": "Invalid client credentials format"
        }), 401

    # Load and verify client
    client = api.schema.load_oauth_client(client_id)
    if not client:
        return jsonify({
            "error": "invalid_client",
            "error_description": "Client not found"
        }), 401

    if not client.verify_secret(client_secret):
        return jsonify({
            "error": "invalid_client",
            "error_description": "Invalid client secret"
        }), 401

    # Handle scope parameter
    requested_scopes = request.form.get('scope', '').split()
    if requested_scopes:
        # Verify all requested scopes are allowed for this client
        for scope in requested_scopes:
            if scope not in client.scopes:
                return jsonify({
                    "error": "invalid_scope",
                    "error_description": f"Scope '{scope}' not allowed for this client"
                }), 400
        granted_scopes = requested_scopes
    else:
        # Grant all client scopes if none specified
        granted_scopes = client.scopes

    # Create access token
    try:
        access_token, expires_at = api.schema.create_oauth_token(
            client_id=client_id,
            scopes=granted_scopes,
            expires_in_seconds=3600  # 1 hour default
        )

        return jsonify({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": " ".join(granted_scopes)
        })

    except Exception as e:
        log.error(f"Failed to create OAuth token: {e}")
        return jsonify({
            "error": "server_error",
            "error_description": "Failed to create access token"
        }), 500


@oauth_bp.route('/revoke', methods=['POST'])
def revoke_token():
    """
    OAuth 2.0 Token Revocation Endpoint

    Handles POST /oauth/revoke to revoke access tokens
    """
    api = current_app.config['api']

    # Get token from request
    token = request.form.get('token')
    if not token:
        return jsonify({
            "error": "invalid_request",
            "error_description": "Missing token parameter"
        }), 400

    # Revoke the token
    api.schema.revoke_oauth_token(token)

    # RFC 7009 says to return 200 regardless of whether token existed
    return '', 200


@oauth_bp.route('/introspect', methods=['POST'])
def introspect_token():
    """
    OAuth 2.0 Token Introspection Endpoint (RFC 7662)

    Handles POST /oauth/introspect to check token validity and metadata
    Requires client authentication
    """
    api = current_app.config['api']

    # Extract client credentials from Authorization header
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Basic '):
        return jsonify({
            "error": "invalid_client",
            "error_description": "Client authentication required via Basic Auth"
        }), 401

    try:
        # Decode Basic Auth credentials
        encoded_credentials = auth_header[6:]
        decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
        client_id, client_secret = decoded_credentials.split(':', 1)
    except (ValueError, UnicodeDecodeError):
        return jsonify({
            "error": "invalid_client",
            "error_description": "Invalid client credentials format"
        }), 401

    # Verify client
    client = api.schema.load_oauth_client(client_id)
    if not client or not client.verify_secret(client_secret):
        return jsonify({
            "error": "invalid_client",
            "error_description": "Invalid client credentials"
        }), 401

    # Get token to introspect
    token = request.form.get('token')
    if not token:
        return jsonify({
            "error": "invalid_request",
            "error_description": "Missing token parameter"
        }), 400

    # Load token details
    oauth_token = api.schema.load_oauth_token(token)

    if oauth_token and not oauth_token.is_expired():
        # Token is active
        return jsonify({
            "active": True,
            "client_id": oauth_token.client_id,
            "scope": " ".join(oauth_token.scopes),
            "exp": int(oauth_token.expires_at.timestamp()),
            "iat": int(oauth_token.created_at.timestamp()),
            "token_type": "Bearer"
        })
    else:
        # Token is inactive (expired, revoked, or doesn't exist)
        return jsonify({
            "active": False
        })

