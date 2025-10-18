#!/usr/bin/env python3

import secrets
import hashlib
from uuid import uuid4
from datetime import datetime, timedelta



class AdminScript:
    """Manages OAuth client and token creation without backend identity provider dependencies"""

    def __init__(self, db):
        self.db = db

    def create_oauth_client(self, name, scopes, client_secret=None):
        """Create a new OAuth client directly in the database"""
        # Generate client ID (replicating OAuthClient.generate_client_id logic)
        client_id = 'client_' + secrets.token_urlsafe(32)

        if not client_secret:
            client_secret = str(uuid4())

        # Hash the client secret (replicating OAuthClient.hash_secret logic)
        client_secret_hash = hashlib.sha256(client_secret.encode()).hexdigest()

        client_data = {
            "client_id": client_id,
            "client_secret_hash": client_secret_hash,
            "name": name,
            "scopes": scopes if isinstance(scopes, list) else scopes.split(),
            "created_at": datetime.utcnow()
        }

        self.db['metaphor_oauth_client'].insert_one(client_data)
        return client_id, client_secret

    def create_oauth_token(self, client_id, scopes, expires_in_seconds=3600):
        """Create a new OAuth access token directly in the database"""
        # Generate access token (replicating OAuthToken.generate_access_token logic)
        access_token = 'token_' + secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(seconds=expires_in_seconds)

        token_data = {
            "access_token": access_token,
            "client_id": client_id,
            "scopes": scopes if isinstance(scopes, list) else scopes.split(),
            "expires_at": expires_at,
            "created_at": datetime.utcnow()
        }

        self.db['metaphor_oauth_token'].insert_one(token_data)
        return access_token, expires_at

    def create_complete_token_set(self, user_email, client_name, scopes, expiration_hours=24):
        """Create both OAuth client and token in one operation"""
        expires_in_seconds = expiration_hours * 3600

        # Create OAuth client
        client_id, client_secret = self.create_oauth_client(client_name, scopes)

        # Create OAuth token
        access_token, expires_at = self.create_oauth_token(client_id, scopes, expires_in_seconds)

        return {
            'user_email': user_email,
            'client_name': client_name,
            'client_id': client_id,
            'client_secret': client_secret,
            'access_token': access_token,
            'expires_at': expires_at,
            'scopes': scopes if isinstance(scopes, list) else scopes.split()
        }