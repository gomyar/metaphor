
from functools import wraps

import flask_login
from flask import current_app


def login_required(func):
    @wraps(func)
    def inner(*args, **kwargs):
        if not flask_login.current_user.is_authenticated:
            return "Not logged in", 401
        return func(*args, **kwargs)
    return inner


def admin_required(func):
    @wraps(func)
    def inner(*args, **kwargs):
        api = current_app.config['api']
        if not flask_login.current_user.is_authenticated:
            return "Not logged in", 401
        identity = flask_login.current_user
        user = api.schema.load_user_by_id(identity.user_id)
        if not user:
            return jsonify({"error": "Your identity does not have access to this service"}), 403
        if not user.is_admin():
            return "Unauthorized", 403
        return func(*args, **kwargs)
    return inner
