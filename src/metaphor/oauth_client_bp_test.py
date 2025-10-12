import unittest
import json
import base64
from datetime import datetime, timedelta
from unittest.mock import patch

from metaphor.mongoclient_testutils import mongo_connection
from metaphor.schema import OAuthClient, OAuthToken


class OAuthClientBlueprintTest(unittest.TestCase):
    """Tests for OAuth client credentials flow blueprint"""

    def setUp(self):
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from server import create_app

        client = mongo_connection()
        client.drop_database('oauth_test_db')
        self.mongo_db = client.oauth_test_db

        self.app = create_app(self.mongo_db)
        self.client = self.app.test_client()

        # Create test schema and setup
        with self.app.app_context():
            schema_factory = self.app.config['schema_factory']
            schema = schema_factory.create_schema()
            schema.set_as_current()

            # Create employee spec for testing (avoiding user spec conflict)
            schema.create_spec('employee')
            schema.create_field('employee', 'name', 'str')
            schema.create_field('employee', 'email', 'str')

            # Create a test OAuth client
            self.test_client_id, self.test_client_secret = schema.create_oauth_client(
                "Test Client",
                ["read", "write", "admin"]
            )

    def _basic_auth_header(self, client_id, client_secret):
        """Create basic auth header for client credentials"""
        credentials = f"{client_id}:{client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return {'Authorization': f'Basic {encoded}'}

    def test_oauth_token_endpoint_success(self):
        """Test successful client credentials token request"""
        response = self.client.post('/oauth/token',
                                  data={'grant_type': 'client_credentials'},
                                  headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)

        self.assertIn('access_token', data)
        self.assertEqual(data['token_type'], 'Bearer')
        self.assertEqual(data['expires_in'], 3600)
        self.assertIn('scope', data)
        self.assertEqual(set(data['scope'].split()), {"read", "write", "admin"})

    def test_oauth_token_endpoint_invalid_grant_type(self):
        """Test token request with invalid grant type"""
        response = self.client.post('/oauth/token',
                                  data={'grant_type': 'authorization_code'},
                                  headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'unsupported_grant_type')

    def test_oauth_token_endpoint_missing_auth(self):
        """Test token request without authentication"""
        response = self.client.post('/oauth/token',
                                  data={'grant_type': 'client_credentials'})

        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'invalid_client')

    def test_oauth_token_endpoint_invalid_client(self):
        """Test token request with invalid client credentials"""
        response = self.client.post('/oauth/token',
                                  data={'grant_type': 'client_credentials'},
                                  headers=self._basic_auth_header('invalid_id', 'invalid_secret'))

        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'invalid_client')

    def test_oauth_token_endpoint_invalid_secret(self):
        """Test token request with valid client ID but invalid secret"""
        response = self.client.post('/oauth/token',
                                  data={'grant_type': 'client_credentials'},
                                  headers=self._basic_auth_header(self.test_client_id, 'wrong_secret'))

        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'invalid_client')

    def test_oauth_token_endpoint_scoped_request(self):
        """Test token request with specific scopes"""
        response = self.client.post('/oauth/token',
                                  data={
                                      'grant_type': 'client_credentials',
                                      'scope': 'read write'
                                  },
                                  headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(set(data['scope'].split()), {"read", "write"})

    def test_oauth_token_endpoint_invalid_scope(self):
        """Test token request with invalid scope"""
        response = self.client.post('/oauth/token',
                                  data={
                                      'grant_type': 'client_credentials',
                                      'scope': 'invalid_scope'
                                  },
                                  headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'invalid_scope')

    def test_oauth_revoke_endpoint(self):
        """Test token revocation"""
        # First, get a token
        token_response = self.client.post('/oauth/token',
                                        data={'grant_type': 'client_credentials'},
                                        headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        self.assertEqual(token_response.status_code, 200)
        token_data = json.loads(token_response.data)
        access_token = token_data['access_token']

        # Now revoke it
        revoke_response = self.client.post('/oauth/revoke',
                                         data={'token': access_token})

        self.assertEqual(revoke_response.status_code, 200)

    def test_oauth_introspect_endpoint_active_token(self):
        """Test token introspection for active token"""
        # First, get a token
        token_response = self.client.post('/oauth/token',
                                        data={'grant_type': 'client_credentials'},
                                        headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        token_data = json.loads(token_response.data)
        access_token = token_data['access_token']

        # Introspect the token
        introspect_response = self.client.post('/oauth/introspect',
                                             data={'token': access_token},
                                             headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        self.assertEqual(introspect_response.status_code, 200)
        data = json.loads(introspect_response.data)

        self.assertTrue(data['active'])
        self.assertEqual(data['client_id'], self.test_client_id)
        self.assertEqual(data['token_type'], 'Bearer')
        self.assertIn('exp', data)
        self.assertIn('iat', data)

    def test_oauth_introspect_endpoint_inactive_token(self):
        """Test token introspection for inactive token"""
        introspect_response = self.client.post('/oauth/introspect',
                                             data={'token': 'invalid_token'},
                                             headers=self._basic_auth_header(self.test_client_id, self.test_client_secret))

        self.assertEqual(introspect_response.status_code, 200)
        data = json.loads(introspect_response.data)
        self.assertFalse(data['active'])

    def test_oauth_introspect_endpoint_no_auth(self):
        """Test token introspection without client authentication"""
        response = self.client.post('/oauth/introspect',
                                  data={'token': 'some_token'})

        self.assertEqual(response.status_code, 401)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'invalid_client')


class OAuthModelTest(unittest.TestCase):
    """Tests for OAuth model classes"""

    def test_oauth_client_creation(self):
        """Test OAuthClient model"""
        client_id = "test_client_id"
        client_secret_hash = "hashed_secret"
        name = "Test Client"
        scopes = ["read", "write"]

        client = OAuthClient(client_id, client_secret_hash, name, scopes)

        self.assertEqual(client.client_id, client_id)
        self.assertEqual(client.client_secret_hash, client_secret_hash)
        self.assertEqual(client.name, name)
        self.assertEqual(client.scopes, scopes)
        self.assertIsInstance(client.created_at, datetime)

    def test_oauth_client_from_data(self):
        """Test OAuthClient.from_data method"""
        data = {
            "client_id": "test_id",
            "client_secret_hash": "hash",
            "name": "Test",
            "scopes": ["read"],
            "created_at": datetime.utcnow()
        }

        client = OAuthClient.from_data(data)
        self.assertEqual(client.client_id, data["client_id"])
        self.assertEqual(client.name, data["name"])

    def test_oauth_client_verify_secret(self):
        """Test OAuthClient secret verification"""
        secret = "test_secret"
        hashed = OAuthClient.hash_secret(secret)

        client = OAuthClient("id", hashed, "name", ["read"])

        self.assertTrue(client.verify_secret(secret))
        self.assertFalse(client.verify_secret("wrong_secret"))

    def test_oauth_token_creation(self):
        """Test OAuthToken model"""
        access_token = "test_token"
        client_id = "test_client"
        scopes = ["read", "write"]
        expires_at = datetime.utcnow() + timedelta(hours=1)

        token = OAuthToken(access_token, client_id, scopes, expires_at)

        self.assertEqual(token.access_token, access_token)
        self.assertEqual(token.client_id, client_id)
        self.assertEqual(token.scopes, scopes)
        self.assertEqual(token.expires_at, expires_at)

    def test_oauth_token_expiry(self):
        """Test OAuthToken expiry checking"""
        # Non-expired token
        future_time = datetime.utcnow() + timedelta(hours=1)
        token = OAuthToken("token", "client", ["read"], future_time)
        self.assertFalse(token.is_expired())

        # Expired token
        past_time = datetime.utcnow() - timedelta(hours=1)
        expired_token = OAuthToken("token", "client", ["read"], past_time)
        self.assertTrue(expired_token.is_expired())

    def test_oauth_token_scope_checking(self):
        """Test OAuthToken scope checking"""
        token = OAuthToken("token", "client", ["read", "write"], datetime.utcnow() + timedelta(hours=1))

        self.assertTrue(token.has_scope("read"))
        self.assertTrue(token.has_scope("write"))
        self.assertFalse(token.has_scope("admin"))


if __name__ == '__main__':
    unittest.main()