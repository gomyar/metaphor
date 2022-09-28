
from functools import wraps

import flask_login


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
        if not flask_login.current_user.is_authenticated:
            return "Not logged in", 401
        if not flask_login.current_user.is_admin():
            return "Unauthorized", 403
        return func(*args, **kwargs)
    return inner
