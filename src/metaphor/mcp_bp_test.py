import unittest
import json
from unittest.mock import Mock, patch

from metaphor.mongoclient_testutils import mongo_connection
from metaphor.mcp_bp import MCPServer, MCPError
from metaphor.api import Api
from metaphor.schema_factory import SchemaFactory
from metaphor.schema import Schema


class MCPServerTest(unittest.TestCase):
    def setUp(self):
        client = mongo_connection()
        client.drop_database('mcp_test_db')
        self.mongo_db = client.mcp_test_db

        # Create API and schema factory
        self.api = Api(self.mongo_db)
        self.schema_factory = SchemaFactory(self.mongo_db)

        # Create a test schema
        self.schema = self.schema_factory.create_schema()
        self.schema.set_as_current()

        # Create employee spec
        self.employee_spec = self.schema.create_spec('employee')
        self.schema.create_field('employee', 'name', 'str')
        self.schema.create_field('employee', 'email', 'str')
        self.schema.create_field('employee', 'salary', 'int')

        # Add to root collection
        self.schema.create_field('root', 'employees', 'collection', 'employee')

        # Create MCP server
        self.mcp_server = MCPServer(self.api, self.schema_factory)

        # Create test data
        self.employee_id = self.schema.insert_resource('employee', {
            "name": "John Doe",
            "email": "john@example.com",
            "salary": 50000
        }, 'employees')

    def test_initialize(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 1)
        self.assertIn("result", response)
        result = response["result"]
        self.assertEqual(result["protocolVersion"], "2024-11-05")
        self.assertIn("capabilities", result)
        self.assertIn("serverInfo", result)

    def test_resources_list(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "resources/list",
            "params": {}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 2)
        self.assertIn("result", response)

        resources = response["result"]["resources"]
        self.assertGreater(len(resources), 0)

        # Check for schema resource
        schema_resource = next((r for r in resources if r["uri"] == "metaphor://schema"), None)
        self.assertIsNotNone(schema_resource)
        self.assertEqual(schema_resource["name"], "Current Schema")

        # Check for employee spec resource
        employee_spec_resource = next((r for r in resources if r["uri"] == "metaphor://spec/employee"), None)
        self.assertIsNotNone(employee_spec_resource)

        # Check for employee collection resource
        employee_collection_resource = next((r for r in resources if r["uri"] == "metaphor://resources/employee"), None)
        self.assertIsNotNone(employee_collection_resource)

    def test_resources_read_schema(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "resources/read",
            "params": {"uri": "metaphor://schema"}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 3)
        self.assertIn("result", response)

        contents = response["result"]["contents"]
        self.assertEqual(len(contents), 1)
        self.assertEqual(contents[0]["uri"], "metaphor://schema")
        self.assertEqual(contents[0]["mimeType"], "application/json")

        schema_data = json.loads(contents[0]["text"])
        self.assertIn("specs", schema_data)

    def test_resources_read_spec(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "resources/read",
            "params": {"uri": "metaphor://spec/employee"}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 4)
        self.assertIn("result", response)

        contents = response["result"]["contents"]
        self.assertEqual(len(contents), 1)

        spec_data = json.loads(contents[0]["text"])
        self.assertEqual(spec_data["_id"], "employee")
        self.assertIn("fields", spec_data)

    def test_resources_read_collection(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "resources/read",
            "params": {"uri": "metaphor://resources/employee"}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 5)
        self.assertIn("result", response)

        contents = response["result"]["contents"]
        self.assertEqual(len(contents), 1)

        resources_data = json.loads(contents[0]["text"])
        self.assertIsInstance(resources_data, list)
        self.assertEqual(len(resources_data), 1)
        self.assertEqual(resources_data[0]["name"], "John Doe")

    def test_resources_read_invalid_uri(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "resources/read",
            "params": {"uri": "invalid://uri"}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 6)
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32602)

    def test_tools_list(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/list",
            "params": {}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 7)
        self.assertIn("result", response)

        tools = response["result"]["tools"]
        tool_names = [tool["name"] for tool in tools]

        expected_tools = [
            "metaphor_get_resource",
            "metaphor_create_resource",
            "metaphor_update_resource",
            "metaphor_delete_resource",
            "metaphor_search_resources",
            "metaphor_create_spec",
            "metaphor_update_spec",
            "metaphor_delete_spec",
            "metaphor_add_field"
        ]

        for expected_tool in expected_tools:
            self.assertIn(expected_tool, tool_names)

    def test_tool_get_resource(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "name": "metaphor_get_resource",
                "arguments": {"path": f"/employees/{self.employee_id}"}
            }
        }

        with patch.object(self.api, 'get') as mock_get:
            mock_get.return_value = {"_id": str(self.employee_id), "name": "John Doe"}

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 8)
            self.assertIn("result", response)

            content = response["result"]["content"]
            self.assertEqual(len(content), 1)
            self.assertEqual(content[0]["type"], "text")

            result_data = json.loads(content[0]["text"])
            self.assertEqual(result_data["name"], "John Doe")

            mock_get.assert_called_once_with(f"/employees/{self.employee_id}", expand_dict={})

    def test_tool_create_resource(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {
                "name": "metaphor_create_resource",
                "arguments": {
                    "path": "/employees",
                    "data": {"name": "Jane Smith", "email": "jane@example.com", "salary": 60000}
                }
            }
        }

        with patch.object(self.api, 'post') as mock_post:
            mock_post.return_value = {"_id": "emp2", "name": "Jane Smith"}

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 9)
            self.assertIn("result", response)

            content = response["result"]["content"]
            self.assertEqual(len(content), 1)
            self.assertIn("Created resource", content[0]["text"])

            mock_post.assert_called_once()

    def test_tool_update_resource_patch(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "name": "metaphor_update_resource",
                "arguments": {
                    "path": "/employees/emp1",
                    "data": {"salary": 55000},
                    "method": "PATCH"
                }
            }
        }

        with patch.object(self.api, 'patch') as mock_patch:
            mock_patch.return_value = {"_id": "emp1", "name": "John Doe", "salary": 55000}

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 10)
            self.assertIn("result", response)

            content = response["result"]["content"]
            self.assertEqual(len(content), 1)
            self.assertIn("Updated resource", content[0]["text"])

            mock_patch.assert_called_once_with("/employees/emp1", {"salary": 55000})

    def test_tool_update_resource_put(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "metaphor_update_resource",
                "arguments": {
                    "path": "/employees/emp1",
                    "data": {"name": "John Smith", "email": "john.smith@example.com", "salary": 55000},
                    "method": "PUT"
                }
            }
        }

        with patch.object(self.api, 'put') as mock_put:
            mock_put.return_value = {"_id": "emp1", "name": "John Smith"}

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 11)
            self.assertIn("result", response)

            mock_put.assert_called_once()

    def test_tool_delete_resource(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "metaphor_delete_resource",
                "arguments": {"path": "/employees/emp1"}
            }
        }

        with patch.object(self.api, 'delete') as mock_delete:
            mock_delete.return_value = True

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 12)
            self.assertIn("result", response)

            content = response["result"]["content"]
            self.assertEqual(len(content), 1)
            self.assertIn("Deleted resource", content[0]["text"])

            mock_delete.assert_called_once_with("/employees/emp1")

    def test_tool_search_resources(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 13,
            "method": "tools/call",
            "params": {
                "name": "metaphor_search_resources",
                "arguments": {
                    "path": "/employees",
                    "filters": {"salary": {"$gte": 50000}},
                    "limit": 10
                }
            }
        }

        with patch.object(self.api, 'get') as mock_get:
            mock_get.return_value = [{"_id": "emp1", "name": "John Doe", "salary": 50000}]

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 13)
            self.assertIn("result", response)

            content = response["result"]["content"]
            self.assertEqual(len(content), 1)

            result_data = json.loads(content[0]["text"])
            self.assertIsInstance(result_data, list)

            mock_get.assert_called_once()

    @patch('metaphor.mcp_bp.current_user')
    def test_tool_create_spec_requires_admin(self, mock_current_user):
        mock_current_user.is_authenticated = False

        request_data = {
            "jsonrpc": "2.0",
            "id": 14,
            "method": "tools/call",
            "params": {
                "name": "metaphor_create_spec",
                "arguments": {
                    "spec_name": "department",
                    "spec_data": {"fields": [{"name": "name", "type": "text"}]}
                }
            }
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 14)
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32000)
        self.assertIn("Authentication required", response["error"]["message"])

    @patch('metaphor.mcp_bp.current_user')
    def test_tool_create_spec_success(self, mock_current_user):
        mock_current_user.is_authenticated = True
        mock_current_user.is_admin = True

        request_data = {
            "jsonrpc": "2.0",
            "id": 15,
            "method": "tools/call",
            "params": {
                "name": "metaphor_create_spec",
                "arguments": {
                    "spec_name": "department",
                    "spec_data": {"fields": [{"name": "name", "type": "text"}]}
                }
            }
        }

        with patch.object(self.schema_factory, 'apply_mutation') as mock_apply:
            mock_apply.return_value = {"success": True}

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 15)
            self.assertIn("result", response)

            content = response["result"]["content"]
            self.assertEqual(len(content), 1)
            self.assertIn("Created spec", content[0]["text"])

            mock_apply.assert_called_once()

    @patch('metaphor.mcp_bp.current_user')
    def test_tool_add_field_success(self, mock_current_user):
        mock_current_user.is_authenticated = True
        mock_current_user.is_admin = True

        request_data = {
            "jsonrpc": "2.0",
            "id": 16,
            "method": "tools/call",
            "params": {
                "name": "metaphor_add_field",
                "arguments": {
                    "spec_name": "employee",
                    "field_name": "department",
                    "field_type": "text",
                    "field_config": {}
                }
            }
        }

        with patch.object(self.schema_factory, 'apply_mutation') as mock_apply:
            mock_apply.return_value = {"success": True}

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 16)
            self.assertIn("result", response)

            content = response["result"]["content"]
            self.assertEqual(len(content), 1)
            self.assertIn("Added field", content[0]["text"])

            mock_apply.assert_called_once()

    def test_invalid_method(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 17,
            "method": "invalid/method",
            "params": {}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 17)
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32601)

    def test_invalid_jsonrpc_version(self):
        request_data = {
            "jsonrpc": "1.0",
            "id": 18,
            "method": "initialize",
            "params": {}
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 18)
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32600)

    def test_missing_required_parameters(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 19,
            "method": "tools/call",
            "params": {
                "name": "metaphor_get_resource"
                # Missing arguments
            }
        }

        response = self.mcp_server.handle_request(request_data)

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertEqual(response["id"], 19)
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32602)

    def test_tool_execution_error(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 20,
            "method": "tools/call",
            "params": {
                "name": "metaphor_get_resource",
                "arguments": {"path": "/employees/nonexistent"}
            }
        }

        with patch.object(self.api, 'get') as mock_get:
            mock_get.side_effect = Exception("Resource not found")

            response = self.mcp_server.handle_request(request_data)

            self.assertEqual(response["jsonrpc"], "2.0")
            self.assertEqual(response["id"], 20)
            self.assertIn("error", response)
            self.assertEqual(response["error"]["code"], -32603)

    def test_parse_error(self):
        # Test with invalid request data
        response = self.mcp_server.handle_request("invalid json")

        self.assertEqual(response["jsonrpc"], "2.0")
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32700)


class MCPBlueprintIntegrationTest(unittest.TestCase):
    """Integration tests for the MCP Flask blueprint"""

    def setUp(self):
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from server import create_app

        client = mongo_connection()
        client.drop_database('mcp_integration_test_db')
        self.mongo_db = client.mcp_integration_test_db

        self.app = create_app(self.mongo_db)
        self.client = self.app.test_client()

        # Create test schema
        with self.app.app_context():
            schema_factory = self.app.config['schema_factory']
            schema = schema_factory.create_schema()
            schema.set_as_current()

            # Create employee spec
            employee_spec = schema.create_spec('employee')
            schema.create_field('employee', 'name', 'str')
            schema.create_field('employee', 'email', 'str')

    def test_mcp_endpoint_initialize(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        }

        response = self.client.post('/mcp/',
                                   data=json.dumps(request_data),
                                   content_type='application/json')

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.data)
        self.assertEqual(response_data["jsonrpc"], "2.0")
        self.assertEqual(response_data["id"], 1)
        self.assertIn("result", response_data)

    def test_mcp_endpoint_invalid_json(self):
        response = self.client.post('/mcp/',
                                   data="invalid json",
                                   content_type='application/json')

        self.assertEqual(response.status_code, 400)

        response_data = json.loads(response.data)
        self.assertEqual(response_data["jsonrpc"], "2.0")
        self.assertIn("error", response_data)
        self.assertEqual(response_data["error"]["code"], -32700)

    def test_mcp_endpoint_resources_list(self):
        request_data = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "resources/list",
            "params": {}
        }

        response = self.client.post('/mcp/',
                                   data=json.dumps(request_data),
                                   content_type='application/json')

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.data)
        self.assertEqual(response_data["jsonrpc"], "2.0")
        self.assertIn("result", response_data)
        self.assertIn("resources", response_data["result"])


if __name__ == '__main__':
    unittest.main()