
from functools import wraps

import flask_login
from flask import current_app, request, jsonify, g


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

def oauth_required(func):
    """
    Decorator that requires valid OAuth token authentication.
    Sets g.oauth_token and g.oauth_client for use in the decorated function.
    """
    @wraps(func)
    def inner(*args, **kwargs):
        api = current_app.config['api']
        
        # Check for Bearer token in Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({
                "error": "unauthorized",
                "error_description": "Bearer token required in Authorization header"
            }), 401
        
        # Extract token
        token = auth_header[7:]  # Remove "Bearer " prefix
        
        # Load and validate token
        oauth_token = api.schema.load_oauth_token(token)
        if not oauth_token:
            return jsonify({
                "error": "invalid_token",
                "error_description": "Token is invalid, expired, or revoked"
            }), 401
        
        # Load client for additional context
        oauth_client = api.schema.load_oauth_client(oauth_token.client_id)
        
        # Store in Flask's g object for access in the decorated function
        g.oauth_token = oauth_token
        g.oauth_client = oauth_client
        
        return func(*args, **kwargs)
    return inner


def scope_required(*required_scopes):
    """
    Decorator that requires specific OAuth scopes.
    Must be used with @oauth_required.
    
    Usage:
        @oauth_required
        @scope_required('read', 'write')
        def my_endpoint():
            pass
    """
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            # Check if oauth_token is available (should be set by oauth_required)
            if not hasattr(g, 'oauth_token') or not g.oauth_token:
                return jsonify({
                    "error": "unauthorized",
                    "error_description": "OAuth authentication required"
                }), 401
            
            # Check if token has all required scopes
            missing_scopes = []
            for scope in required_scopes:
                if not g.oauth_token.has_scope(scope):
                    missing_scopes.append(scope)
            
            if missing_scopes:
                return jsonify({
                    "error": "insufficient_scope",
                    "error_description": f"Missing required scopes: {', '.join(missing_scopes)}"
                }), 403
            
            return func(*args, **kwargs)
        return inner
    return decorator


def auth_required(func):
    """
    Decorator that accepts either session-based or OAuth token authentication.
    Sets g.auth_type to 'session' or 'oauth' and g.user for session auth.
    For OAuth, sets g.oauth_token and g.oauth_client.
    """
    @wraps(func)
    def inner(*args, **kwargs):
        api = current_app.config['api']
        
        # Check for OAuth token first
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]
            oauth_token = api.schema.load_oauth_token(token)
            
            if oauth_token:
                # Valid OAuth token
                oauth_client = api.schema.load_oauth_client(oauth_token.client_id)
                g.auth_type = 'oauth'
                g.oauth_token = oauth_token
                g.oauth_client = oauth_client
                return func(*args, **kwargs)
        
        # Fall back to session-based authentication
        if flask_login.current_user.is_authenticated:
            identity = flask_login.current_user
            user = api.schema.load_user_by_id(identity.user_id)
            if user:
                g.auth_type = 'session'
                g.user = user
                return func(*args, **kwargs)
        
        return jsonify({
            "error": "unauthorized",
            "error_description": "Authentication required (session or OAuth token)"
        }), 401
    
    return inner


def oauth_scope_required(*required_scopes):
    """
    Convenience decorator that combines oauth_required and scope_required.
    
    Usage:
        @oauth_scope_required('read', 'write')
        def my_endpoint():
            pass
    """
    def decorator(func):
        # Apply oauth_required and then scope_required decorators
        return oauth_required(scope_required(*required_scopes)(func))
    return decorator
