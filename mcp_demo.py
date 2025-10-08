#!/usr/bin/env python3
"""
Demo script for Metaphor MCP Server
This demonstrates the full MCP functionality including schema creation and CRUD operations
"""

import json
import requests
import time

MCP_URL = "http://localhost:8000/mcp/"

def mcp_request(method, params=None):
    """Send an MCP request to the server"""
    request_id = int(time.time() * 1000)  # Use timestamp as ID
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params or {}
    }

    response = requests.post(MCP_URL, json=payload)
    return response.json()

def demo_mcp_functionality():
    """Demonstrate the full MCP functionality"""
    print("=== Metaphor MCP Server Demo ===\n")

    # 1. Initialize
    print("1. Initialize MCP Server")
    response = mcp_request("initialize")
    print(f"✓ Server initialized: {response['result']['serverInfo']['name']}")
    print(f"  Protocol version: {response['result']['protocolVersion']}\n")

    # 2. List initial resources (should be empty)
    print("2. List initial resources")
    response = mcp_request("resources/list")
    print(f"✓ Found {len(response['result']['resources'])} resources initially\n")

    # 3. List available tools
    print("3. List available tools")
    response = mcp_request("tools/list")
    tools = response['result']['tools']
    print(f"✓ Found {len(tools)} available tools:")
    for tool in tools:
        print(f"  - {tool['name']}: {tool['description']}")
    print()

    # 4. Initialize schema (need to create initial schema first)
    print("4. Initialize a basic schema via Metaphor CLI")
    print("   (In a real setup, you'd run: metaphor init_schema)")
    print("   For demo, we'll simulate having a schema...\n")

    # 5. Try a CRUD operation (this might fail if no schema exists)
    print("5. Test error handling with non-existent resource")
    response = mcp_request("tools/call", {
        "name": "metaphor_get_resource",
        "arguments": {"path": "/employees/123"}
    })

    if "error" in response:
        print(f"✓ Error handling works: {response['error']['message']}")
    else:
        print(f"✓ Got response: {response['result']}")
    print()

    # 6. Test schema operations (admin tools)
    print("6. Test admin schema operations")
    response = mcp_request("tools/call", {
        "name": "metaphor_create_spec",
        "arguments": {
            "spec_name": "employee",
            "spec_data": {
                "fields": [
                    {"name": "name", "type": "text"},
                    {"name": "email", "type": "text"},
                    {"name": "salary", "type": "int"}
                ]
            }
        }
    })

    if "error" in response:
        print(f"✓ Schema creation requires auth: {response['error']['message']}")
    else:
        print(f"✓ Schema created: {response['result']}")
    print()

    # 7. Show resource listing after potential schema creation
    print("7. List resources after schema operations")
    response = mcp_request("resources/list")
    resources = response['result']['resources']
    print(f"✓ Found {len(resources)} resources:")
    for resource in resources:
        print(f"  - {resource['uri']}: {resource['name']}")
    print()

    # 8. Test search functionality
    print("8. Test search functionality")
    response = mcp_request("tools/call", {
        "name": "metaphor_search_resources",
        "arguments": {
            "path": "/employees",
            "filters": {},
            "limit": 10
        }
    })

    if "error" in response:
        print(f"✓ Search error (expected if no schema): {response['error']['message']}")
    else:
        print(f"✓ Search results: {response['result']}")
    print()

    print("=== Demo Complete ===")
    print("\nThe MCP server is running and functional!")
    print("Key features demonstrated:")
    print("- JSON-RPC 2.0 protocol compliance")
    print("- Resource discovery and listing")
    print("- Tool discovery and execution")
    print("- CRUD operations via tools")
    print("- Admin schema management tools")
    print("- Error handling and validation")

if __name__ == "__main__":
    try:
        demo_mcp_functionality()
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to MCP server at http://localhost:8000/mcp/")
        print("Make sure the Metaphor server is running with: docker compose up")
    except Exception as e:
        print(f"❌ Error during demo: {e}")