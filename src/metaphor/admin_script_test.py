#!/usr/bin/env python3

import unittest
from pymongo import MongoClient
from datetime import datetime, timedelta

# Import the TokenManager class
from metaphor.admin_script import AdminScript


class TokenManagerTest(unittest.TestCase):
    def setUp(self):
        # Connect to MongoDB (assuming it's running on default port)
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.test_db_name = 'metaphor_test_token_manager'
        self.db = self.mongo_client[self.test_db_name]

        # Clean up any existing test data
        self.db['metaphor_oauth_client'].delete_many({})
        self.db['metaphor_oauth_token'].delete_many({})

        # Create TokenManager instance
        self.token_manager = AdminScript(self.db)

    def tearDown(self):
        # Clean up test data
        self.db['metaphor_oauth_client'].delete_many({})
        self.db['metaphor_oauth_token'].delete_many({})
        self.mongo_client.close()

    def test_create_oauth_client(self):
        """Test creating an OAuth client"""
        client_name = "Test Client"
        scopes = ["read", "write"]

        client_id, client_secret = self.token_manager.create_oauth_client(client_name, scopes)

        # Verify the client was created with correct structure
        self.assertTrue(client_id.startswith('client_'))
        self.assertIsInstance(client_secret, str)
        self.assertTrue(len(client_secret) > 0)

        # Verify data was stored in MongoDB
        client_doc = self.db['metaphor_oauth_client'].find_one({'client_id': client_id})
        self.assertIsNotNone(client_doc)
        self.assertEqual(client_doc['name'], client_name)
        self.assertEqual(client_doc['scopes'], scopes)
        self.assertIn('client_secret_hash', client_doc)
        self.assertIsInstance(client_doc['created_at'], datetime)

    def test_create_oauth_client_with_custom_secret(self):
        """Test creating an OAuth client with custom secret"""
        client_name = "Test Client Custom"
        scopes = "read,write"  # Test string format
        custom_secret = "my_custom_secret"

        client_id, client_secret = self.token_manager.create_oauth_client(
            client_name, scopes, custom_secret
        )

        self.assertEqual(client_secret, custom_secret)

        # Verify scopes were converted from string to list
        client_doc = self.db['metaphor_oauth_client'].find_one({'client_id': client_id})
        self.assertEqual(client_doc['scopes'], ["read", "write"])

    def test_create_oauth_token(self):
        """Test creating an OAuth token"""
        # First create a client
        client_id, _ = self.token_manager.create_oauth_client("Test Client", ["read"])

        scopes = ["read"]
        expires_in = 3600

        access_token, expires_at = self.token_manager.create_oauth_token(
            client_id, scopes, expires_in
        )

        # Verify the token was created with correct structure
        self.assertTrue(access_token.startswith('token_'))
        self.assertIsInstance(expires_at, datetime)
        self.assertGreater(expires_at, datetime.utcnow())

        # Verify data was stored in MongoDB
        token_doc = self.db['metaphor_oauth_token'].find_one({'access_token': access_token})
        self.assertIsNotNone(token_doc)
        self.assertEqual(token_doc['client_id'], client_id)
        self.assertEqual(token_doc['scopes'], scopes)
        self.assertEqual(token_doc['expires_at'], expires_at)
        self.assertIsInstance(token_doc['created_at'], datetime)

    def test_create_complete_token_set(self):
        """Test creating a complete token set (client + token)"""
        user_email = "test@example.com"
        client_name = "Complete Test Client"
        scopes = ["read", "write", "admin"]
        expiration_hours = 48

        result = self.token_manager.create_complete_token_set(
            user_email, client_name, scopes, expiration_hours
        )

        # Verify the result structure
        self.assertEqual(result['user_email'], user_email)
        self.assertEqual(result['client_name'], client_name)
        self.assertEqual(result['scopes'], scopes)
        self.assertTrue(result['client_id'].startswith('client_'))
        self.assertTrue(result['access_token'].startswith('token_'))
        self.assertIsInstance(result['client_secret'], str)
        self.assertIsInstance(result['expires_at'], datetime)

        # Verify expiration is approximately correct (within 1 minute tolerance)
        expected_expires = datetime.utcnow() + timedelta(hours=expiration_hours)
        time_diff = abs((result['expires_at'] - expected_expires).total_seconds())
        self.assertLess(time_diff, 60)  # Within 1 minute

        # Verify both client and token were stored in MongoDB
        client_doc = self.db['metaphor_oauth_client'].find_one({'client_id': result['client_id']})
        token_doc = self.db['metaphor_oauth_token'].find_one({'access_token': result['access_token']})

        self.assertIsNotNone(client_doc)
        self.assertIsNotNone(token_doc)
        self.assertEqual(client_doc['name'], client_name)
        self.assertEqual(token_doc['client_id'], result['client_id'])

    def test_scopes_conversion(self):
        """Test that scopes are properly converted from string to list"""
        scopes_string = "read,write,admin"
        expected_scopes = ["read", "write", "admin"]

        result = self.token_manager.create_complete_token_set(
            "test@example.com", "Scopes Test", scopes_string, 24
        )

        self.assertEqual(result['scopes'], expected_scopes)

    def test_database_collections_created(self):
        """Test that the correct MongoDB collections are used"""
        # Create a complete token set
        result = self.token_manager.create_complete_token_set(
            "db_test@example.com", "DB Test Client", ["read"], 24
        )

        # Verify both collections have exactly one document each
        client_count = self.db['metaphor_oauth_client'].count_documents({})
        token_count = self.db['metaphor_oauth_token'].count_documents({})

        self.assertEqual(client_count, 1)
        self.assertEqual(token_count, 1)


if __name__ == '__main__':
    unittest.main()