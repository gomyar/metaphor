#!/usr/bin/env python3
"""
Demo script showing MCP server functionality
This can be run without MongoDB for demonstration purposes
"""

import json
from unittest.mock import Mock, MagicMock

from metaphor.mcp_bp import MCPServer


def create_mock_api():
    """Create a mock API for demo purposes"""
    api = Mock()

    # Mock get method
    api.get.return_value = {
        "_id": "emp123",
        "name": "John Doe",
        "email": "john@example.com",
        "salary": 50000
    }

    # Mock post method
    api.post.return_value = {
        "_id": "emp124",
        "name": "Jane Smith",
        "email": "jane@example.com",
        "salary": 60000
    }

    # Mock patch method
    api.patch.return_value = {
        "_id": "emp123",
        "name": "John Doe",
        "email": "john@example.com",
        "salary": 55000
    }

    # Mock delete method
    api.delete.return_value = True

    return api


def create_mock_schema_factory():
    """Create a mock schema factory for demo purposes"""
    schema_factory = Mock()

    # Mock schema loading
    mock_schema = Mock()
    mock_schema.specs = {
        "employee": Mock(),
        "department": Mock()
    }
    mock_schema.to_dict.return_value = {
        "specs": {
            "employee": {
                "_id": "employee",
                "fields": [
                    {"name": "name", "type": "text"},
                    {"name": "email", "type": "text"},
                    {"name": "salary", "type": "int"}
                ]
            },
            "department": {
                "_id": "department",
                "fields": [
                    {"name": "name", "type": "text"},
                    {"name": "budget", "type": "int"}
                ]
            }
        }
    }

    schema_factory.load_current_schema.return_value = mock_schema
    schema_factory.apply_mutation.return_value = {"success": True}

    return schema_factory


def demo_mcp_server():
    """Demonstrate MCP server functionality"""
    print("=== Metaphor MCP Server Demo ===\n")

    # Create mock dependencies
    api = create_mock_api()
    schema_factory = create_mock_schema_factory()

    # Create MCP server
    mcp_server = MCPServer(api, schema_factory)

    # Demo 1: Initialize
    print("1. Initialize MCP Server")
    init_request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    }

    response = mcp_server.handle_request(init_request)
    print(f"Request: {json.dumps(init_request, indent=2)}")
    print(f"Response: {json.dumps(response, indent=2)}\n")

    # Demo 2: List resources
    print("2. List Available Resources")
    list_resources_request = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "resources/list",
        "params": {}
    }

    response = mcp_server.handle_request(list_resources_request)
    print(f"Request: {json.dumps(list_resources_request, indent=2)}")
    print(f"Response: {json.dumps(response, indent=2)}\n")

    # Demo 3: Read schema resource
    print("3. Read Schema Resource")
    read_schema_request = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "resources/read",
        "params": {"uri": "metaphor://schema"}
    }

    response = mcp_server.handle_request(read_schema_request)
    print(f"Request: {json.dumps(read_schema_request, indent=2)}")
    print(f"Response: {json.dumps(response, indent=2)}\n")

    # Demo 4: List available tools
    print("4. List Available Tools")
    list_tools_request = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/list",
        "params": {}
    }

    response = mcp_server.handle_request(list_tools_request)
    print(f"Request: {json.dumps(list_tools_request, indent=2)}")
    print(f"Response (first 3 tools): {json.dumps({**response, 'result': {'tools': response['result']['tools'][:3]}}, indent=2)}\n")

    # Demo 5: Get a resource
    print("5. Get Employee Resource")
    get_resource_request = {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "metaphor_get_resource",
            "arguments": {"path": "/employees/emp123"}
        }
    }

    response = mcp_server.handle_request(get_resource_request)
    print(f"Request: {json.dumps(get_resource_request, indent=2)}")
    print(f"Response: {json.dumps(response, indent=2)}\n")

    # Demo 6: Create a resource
    print("6. Create New Employee Resource")
    create_resource_request = {
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "metaphor_create_resource",
            "arguments": {
                "path": "/employees",
                "data": {
                    "name": "Jane Smith",
                    "email": "jane@example.com",
                    "salary": 60000
                }
            }
        }
    }

    response = mcp_server.handle_request(create_resource_request)
    print(f"Request: {json.dumps(create_resource_request, indent=2)}")
    print(f"Response: {json.dumps(response, indent=2)}\n")

    # Demo 7: Update a resource
    print("7. Update Employee Salary")
    update_resource_request = {
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "metaphor_update_resource",
            "arguments": {
                "path": "/employees/emp123",
                "data": {"salary": 55000},
                "method": "PATCH"
            }
        }
    }

    response = mcp_server.handle_request(update_resource_request)
    print(f"Request: {json.dumps(update_resource_request, indent=2)}")
    print(f"Response: {json.dumps(response, indent=2)}\n")

    # Demo 8: Error handling
    print("8. Error Handling (Invalid Method)")
    error_request = {
        "jsonrpc": "2.0",
        "id": 8,
        "method": "invalid/method",
        "params": {}
    }

    response = mcp_server.handle_request(error_request)
    print(f"Request: {json.dumps(error_request, indent=2)}")
    print(f"Response: {json.dumps(response, indent=2)}\n")

    print("=== Demo Complete ===")


if __name__ == "__main__":
    demo_mcp_server()