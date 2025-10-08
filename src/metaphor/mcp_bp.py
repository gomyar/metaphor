import json
import logging
from flask import Blueprint, request, jsonify, current_app, g
from flask_login import current_user
from metaphor.api import Api
from metaphor.schema_factory import SchemaFactory
from metaphor.schema import Schema

log = logging.getLogger(__name__)

mcp_bp = Blueprint('mcp', __name__, url_prefix='/mcp')


class MCPError(Exception):
    def __init__(self, code, message, data=None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(message)


class MCPServer:
    def __init__(self, api: Api, schema_factory: SchemaFactory):
        self.api = api
        self.schema_factory = schema_factory

    def handle_request(self, request_data):
        try:
            if not isinstance(request_data, dict):
                raise MCPError(-32700, "Parse error")

            if request_data.get("jsonrpc") != "2.0":
                raise MCPError(-32600, "Invalid Request")

            method = request_data.get("method")
            params = request_data.get("params", {})
            request_id = request_data.get("id")

            if method == "initialize":
                result = self._handle_initialize(params)
            elif method == "resources/list":
                result = self._handle_resources_list(params)
            elif method == "resources/read":
                result = self._handle_resources_read(params)
            elif method == "tools/list":
                result = self._handle_tools_list(params)
            elif method == "tools/call":
                result = self._handle_tools_call(params)
            else:
                raise MCPError(-32601, f"Method not found: {method}")

            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result
            }

        except MCPError as e:
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {
                    "code": e.code,
                    "message": e.message,
                    "data": e.data
                }
            }
        except Exception as e:
            log.exception("Unexpected error in MCP request")
            return {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": str(e)
                }
            }

    def _handle_initialize(self, params):
        return {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "resources": {},
                "tools": {}
            },
            "serverInfo": {
                "name": "metaphor-mcp-server",
                "version": "0.1.0"
            }
        }

    def _handle_resources_list(self, params):
        resources = []

        try:
            schema = self.schema_factory.load_current_schema()

            # Add schema resource
            resources.append({
                "uri": "metaphor://schema",
                "name": "Current Schema",
                "description": "The current Metaphor schema definition",
                "mimeType": "application/json"
            })

            # Add resource type definitions
            for spec_name, spec in schema.specs.items():
                resources.append({
                    "uri": f"metaphor://spec/{spec_name}",
                    "name": f"Resource Spec: {spec_name}",
                    "description": f"Schema definition for {spec_name} resources",
                    "mimeType": "application/json"
                })

            # Add actual resource collections
            for spec_name in schema.specs.keys():
                resources.append({
                    "uri": f"metaphor://resources/{spec_name}",
                    "name": f"{spec_name} Collection",
                    "description": f"Collection of all {spec_name} resources",
                    "mimeType": "application/json"
                })

        except Exception as e:
            log.warning(f"Could not load schema for resources list: {e}")

        return {"resources": resources}

    def _handle_resources_read(self, params):
        uri = params.get("uri")
        if not uri:
            raise MCPError(-32602, "Missing required parameter: uri")

        if not uri.startswith("metaphor://"):
            raise MCPError(-32602, f"Invalid URI scheme: {uri}")

        path = uri[11:]  # Remove "metaphor://" prefix

        try:
            if path == "schema":
                schema = self.schema_factory.load_current_schema()
                return {
                    "contents": [{
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(schema.to_dict(), indent=2)
                    }]
                }

            elif path.startswith("spec/"):
                spec_name = path[5:]
                schema = self.schema_factory.load_current_schema()
                if spec_name not in schema.specs:
                    raise MCPError(-32602, f"Unknown spec: {spec_name}")

                spec = schema.specs[spec_name]
                return {
                    "contents": [{
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(spec.to_dict(), indent=2)
                    }]
                }

            elif path.startswith("resources/"):
                spec_name = path[10:]
                schema = self.schema_factory.load_current_schema()
                if spec_name not in schema.specs:
                    raise MCPError(-32602, f"Unknown resource type: {spec_name}")

                # Get all resources of this type
                collection_name = f"resource_{spec_name}"
                collection = self.api.db[collection_name]
                resources = list(collection.find({}))

                # Convert ObjectIds to strings
                for resource in resources:
                    if '_id' in resource:
                        resource['_id'] = str(resource['_id'])

                return {
                    "contents": [{
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(resources, indent=2, default=str)
                    }]
                }

            else:
                raise MCPError(-32602, f"Unknown resource path: {path}")

        except MCPError:
            raise
        except Exception as e:
            raise MCPError(-32603, f"Error reading resource: {str(e)}")

    def _handle_tools_list(self, params):
        tools = [
            {
                "name": "metaphor_get_resource",
                "description": "Get a specific resource by path",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Resource path (e.g., /employees/123)"
                        }
                    },
                    "required": ["path"]
                }
            },
            {
                "name": "metaphor_create_resource",
                "description": "Create a new resource",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Collection path (e.g., /employees)"
                        },
                        "data": {
                            "type": "object",
                            "description": "Resource data"
                        }
                    },
                    "required": ["path", "data"]
                }
            },
            {
                "name": "metaphor_update_resource",
                "description": "Update an existing resource",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Resource path (e.g., /employees/123)"
                        },
                        "data": {
                            "type": "object",
                            "description": "Updated resource data"
                        },
                        "method": {
                            "type": "string",
                            "enum": ["PUT", "PATCH"],
                            "description": "Update method (PUT for replace, PATCH for partial update)"
                        }
                    },
                    "required": ["path", "data"]
                }
            },
            {
                "name": "metaphor_delete_resource",
                "description": "Delete a resource",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Resource path (e.g., /employees/123)"
                        }
                    },
                    "required": ["path"]
                }
            },
            {
                "name": "metaphor_search_resources",
                "description": "Search for resources with filters",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Collection path (e.g., /employees)"
                        },
                        "filters": {
                            "type": "object",
                            "description": "MongoDB-style filters"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results"
                        },
                        "expand": {
                            "type": "string",
                            "description": "Fields to expand (comma-separated)"
                        }
                    },
                    "required": ["path"]
                }
            },
            {
                "name": "metaphor_create_spec",
                "description": "Create a new resource specification (admin only)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "spec_name": {
                            "type": "string",
                            "description": "Name of the new resource spec"
                        },
                        "spec_data": {
                            "type": "object",
                            "description": "Spec definition with fields"
                        }
                    },
                    "required": ["spec_name", "spec_data"]
                }
            },
            {
                "name": "metaphor_update_spec",
                "description": "Update an existing resource specification (admin only)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "spec_name": {
                            "type": "string",
                            "description": "Name of the spec to update"
                        },
                        "spec_data": {
                            "type": "object",
                            "description": "Updated spec definition"
                        }
                    },
                    "required": ["spec_name", "spec_data"]
                }
            },
            {
                "name": "metaphor_delete_spec",
                "description": "Delete a resource specification (admin only)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "spec_name": {
                            "type": "string",
                            "description": "Name of the spec to delete"
                        }
                    },
                    "required": ["spec_name"]
                }
            },
            {
                "name": "metaphor_get_current_schema",
                "description": "Get the current active schema",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "metaphor_list_schemas",
                "description": "List all available schemas",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "metaphor_add_field",
                "description": "Add a field to a resource specification (admin only)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "spec_name": {
                            "type": "string",
                            "description": "Name of the spec to modify"
                        },
                        "field_name": {
                            "type": "string",
                            "description": "Name of the new field"
                        },
                        "field_type": {
                            "type": "string",
                            "description": "Type of the field (text, int, link, etc.)"
                        },
                        "field_config": {
                            "type": "object",
                            "description": "Additional field configuration"
                        }
                    },
                    "required": ["spec_name", "field_name", "field_type"]
                }
            }
        ]

        return {"tools": tools}

    def _handle_tools_call(self, params):
        name = params.get("name")
        arguments = params.get("arguments", {})

        if not name:
            raise MCPError(-32602, "Missing required parameter: name")

        try:
            if name == "metaphor_get_resource":
                return self._tool_get_resource(arguments)
            elif name == "metaphor_create_resource":
                return self._tool_create_resource(arguments)
            elif name == "metaphor_update_resource":
                return self._tool_update_resource(arguments)
            elif name == "metaphor_delete_resource":
                return self._tool_delete_resource(arguments)
            elif name == "metaphor_search_resources":
                return self._tool_search_resources(arguments)
            elif name == "metaphor_create_spec":
                return self._tool_create_spec(arguments)
            elif name == "metaphor_update_spec":
                return self._tool_update_spec(arguments)
            elif name == "metaphor_delete_spec":
                return self._tool_delete_spec(arguments)
            elif name == "metaphor_add_field":
                return self._tool_add_field(arguments)
            elif name == "metaphor_get_current_schema":
                return self._tool_get_current_schema(arguments)
            elif name == "metaphor_list_schemas":
                return self._tool_list_schemas(arguments)
            else:
                raise MCPError(-32601, f"Unknown tool: {name}")

        except MCPError:
            raise
        except Exception as e:
            raise MCPError(-32603, f"Tool execution error: {str(e)}")

    def _tool_get_resource(self, args):
        path = args.get("path")
        if not path:
            raise MCPError(-32602, "Missing path argument")

        expand = args.get("expand", "")
        api_args = {"expand": expand} if expand else {}
        result = self.api.get(path, args=api_args)

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(result, indent=2, default=str)
            }]
        }

    def _tool_create_resource(self, args):
        path = args.get("path")
        data = args.get("data")

        if not path or data is None:
            raise MCPError(-32602, "Missing path or data argument")

        result = self.api.post(path, data)

        return {
            "content": [{
                "type": "text",
                "text": f"Created resource at {path}: {json.dumps(result, indent=2, default=str)}"
            }]
        }

    def _tool_update_resource(self, args):
        path = args.get("path")
        data = args.get("data")
        method = args.get("method", "PATCH")

        if not path or data is None:
            raise MCPError(-32602, "Missing path or data argument")

        if method == "PUT":
            result = self.api.put(path, data)
        else:
            result = self.api.patch(path, data)

        return {
            "content": [{
                "type": "text",
                "text": f"Updated resource at {path}: {json.dumps(result, indent=2, default=str)}"
            }]
        }

    def _tool_delete_resource(self, args):
        path = args.get("path")
        if not path:
            raise MCPError(-32602, "Missing path argument")

        result = self.api.delete(path)

        return {
            "content": [{
                "type": "text",
                "text": f"Deleted resource at {path}"
            }]
        }

    def _tool_search_resources(self, args):
        path = args.get("path")
        if not path:
            raise MCPError(-32602, "Missing path argument")

        filters = args.get("filters", {})
        limit = args.get("limit")
        expand = args.get("expand", "")

        query_params = {}
        if filters:
            query_params.update(filters)
        if limit:
            query_params["page_size"] = limit
        if expand:
            query_params["expand"] = expand

        result = self.api.get(path, args=query_params)

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(result, indent=2, default=str)
            }]
        }

    def _check_admin_access(self):
        from flask import g
        
        # Check if we have user context from token authentication
        if not hasattr(g, 'current_user') or not g.current_user:
            raise MCPError(-32000, "Authentication required for admin operations")

        if not hasattr(g, 'current_api_user') or not g.current_api_user:
            raise MCPError(-32000, "User context not found")

        # Check if user has admin privileges
        if not getattr(g.current_api_user, 'admin', False):
            raise MCPError(-32000, "Admin privileges required")

    def _tool_create_spec(self, args):
        self._check_admin_access()

        spec_name = args.get("spec_name")
        spec_data = args.get("spec_data")

        if not spec_name or not spec_data:
            raise MCPError(-32602, "Missing spec_name or spec_data argument")

        # Use schema factory to create spec
        mutation_data = {
            "type": "create_spec",
            "spec_name": spec_name,
            "spec_data": spec_data
        }

        result = self.schema_factory.apply_mutation(mutation_data)

        return {
            "content": [{
                "type": "text",
                "text": f"Created spec '{spec_name}': {json.dumps(result, indent=2, default=str)}"
            }]
        }

    def _tool_update_spec(self, args):
        self._check_admin_access()

        spec_name = args.get("spec_name")
        spec_data = args.get("spec_data")

        if not spec_name or not spec_data:
            raise MCPError(-32602, "Missing spec_name or spec_data argument")

        mutation_data = {
            "type": "update_spec",
            "spec_name": spec_name,
            "spec_data": spec_data
        }

        result = self.schema_factory.apply_mutation(mutation_data)

        return {
            "content": [{
                "type": "text",
                "text": f"Updated spec '{spec_name}': {json.dumps(result, indent=2, default=str)}"
            }]
        }

    def _tool_delete_spec(self, args):
        self._check_admin_access()

        spec_name = args.get("spec_name")
        if not spec_name:
            raise MCPError(-32602, "Missing spec_name argument")

        mutation_data = {
            "type": "delete_spec",
            "spec_name": spec_name
        }

        result = self.schema_factory.apply_mutation(mutation_data)

        return {
            "content": [{
                "type": "text",
                "text": f"Deleted spec '{spec_name}'"
            }]
        }

    def _tool_add_field(self, args):
        self._check_admin_access()

        spec_name = args.get("spec_name")
        field_name = args.get("field_name")
        field_type = args.get("field_type")
        field_config = args.get("field_config", {})

        if not all([spec_name, field_name, field_type]):
            raise MCPError(-32602, "Missing required arguments: spec_name, field_name, field_type")

        mutation_data = {
            "type": "create_field",
            "spec_name": spec_name,
            "field_name": field_name,
            "field_type": field_type,
            "field_config": field_config
        }

        result = self.schema_factory.apply_mutation(mutation_data)

        return {
            "content": [{
                "type": "text",
                "text": f"Added field '{field_name}' to spec '{spec_name}': {json.dumps(result, indent=2, default=str)}"
            }]
        }

    def _tool_get_current_schema(self, args):
        self._check_admin_access()
        
        try:
            schema = self.schema_factory.load_current_schema()
            schema_dict = schema.to_dict()

            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps(schema_dict, indent=2, default=str)
                }]
            }
        except Exception as e:
            raise MCPError(-32603, f"Error loading current schema: {str(e)}")

    def _tool_list_schemas(self, args):
        self._check_admin_access()
        
        try:
            schemas = []
            for schema in self.schema_factory.list_schemas():
                schema_info = {
                    "id": schema.schema_id,
                    "current": getattr(schema, 'current', False),
                    "created": getattr(schema, 'created', None),
                    "name": getattr(schema, 'name', None),
                    "version": getattr(schema, 'version', None),
                    "specs_count": len(schema.specs) if hasattr(schema, 'specs') else 0
                }
                schemas.append(schema_info)

            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps(schemas, indent=2, default=str)
                }]
            }
        except Exception as e:
            raise MCPError(-32603, f"Error listing schemas: {str(e)}")


@mcp_bp.route('/', methods=['POST'])
def mcp_endpoint():
    try:
        api = current_app.config['api']
        schema_factory = current_app.config['schema_factory']

        # Debug logging
        log.info(f"MCP request headers: {dict(request.headers)}")
        log.info(f"MCP request content-type: {request.content_type}")
        log.info(f"MCP request data: {request.data}")

        # Check for Bearer token authentication
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32000,
                    "message": "Authentication required. Provide Bearer token in Authorization header."
                }
            }), 401

        token = auth_header.split(' ')[1]
        identity = api.schema.load_identity_by_token(token)
        
        if not identity:
            return jsonify({
                "jsonrpc": "2.0", 
                "id": None,
                "error": {
                    "code": -32000,
                    "message": "Invalid or expired token"
                }
            }), 401

        user = api.schema.load_user_by_id(identity.user_id)
        if not user:
            return jsonify({
                "jsonrpc": "2.0",
                "id": None, 
                "error": {
                    "code": -32000,
                    "message": "User not found"
                }
            }), 401

        # Set current user context for the MCP server
        # We'll need to modify the MCPServer to accept user context
        with current_app.test_request_context():
            # Temporarily store user in g for MCPServer access
            from flask import g
            g.current_user = identity
            g.current_api_user = user
            
            mcp_server = MCPServer(api, schema_factory)

            # Try different approaches to get JSON data
            try:
                request_data = request.get_json(force=True)
            except Exception as e:
                log.error(f"Failed to parse JSON with force=True: {e}")
                try:
                    import json
                    request_data = json.loads(request.data.decode('utf-8'))
                except Exception as e2:
                    log.error(f"Failed to parse JSON manually: {e2}")
                    return jsonify({
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32700,
                            "message": f"Parse error: {str(e)}"
                        }
                    }), 400

            if not request_data:
                return jsonify({
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": "Parse error: empty request"
                    }
                }), 400

            response = mcp_server.handle_request(request_data)

            # Return appropriate HTTP status based on response
            if "error" in response:
                return jsonify(response), 400
            else:
                return jsonify(response), 200

    except Exception as e:
        log.exception("Unexpected error in MCP endpoint")
        return jsonify({
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32603,
                "message": "Internal error",
                "data": str(e)
            }
        }), 500