
import os

from flask import Blueprint, redirect, url_for, session, request, jsonify
from authlib.integrations.flask_client import OAuth
from authlib.integrations.base_client.errors import OAuthError
from flask import current_app
from flask_login import login_user


oauth_bp = Blueprint('oauth', __name__)
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

