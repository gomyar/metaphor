# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Metaphor** project, which includes an MCP (Model Context Protocol) server implementation. When asked to "do something active with metaphor", this typically means interacting with the locally running Metaphor MCP server that is configured and accessible through Claude Code's MCP integration.

## Technology Stack Restrictions

**Backend**: Flask only with MongoDB as the database
**Frontend**: TurtleGUI only (https://github.com/gomyar/turtlegui/)

These restrictions must be followed for all development in this repository.

## Flask + MongoDB Backend

### Environment Setup
- Create virtual environment: `python -m venv .venv`
- Activate virtual environment: `source .venv/bin/activate` (Linux/Mac) or `.venv\Scripts\activate` (Windows)
- Install dependencies: `pip install -r requirements.txt`

### Common Commands
- Start development server: `.venv/bin/python main.py` or `flask run`
- Run tests: `.venv/bin/python -m pytest -v`
- Database connection: Use PyMongo for Flask integration

### Architecture Guidelines
- Structure endpoints using Flask blueprints for organization
- Use standard synchronous MongoDB operations with PyMongo
- Use JSON schemas or dataclasses for request/response validation
- Always create a corresponding unit test for each python file

## TurtleGUI Frontend

### Core Concepts
- TurtleGUI uses declarative HTML attributes prefixed with `gui-`
- Primary update method: `turtlegui.reload()` to refresh the interface
- Data binding through expressions in GUI attributes

### Key Directives
- `gui-text`: Set element text content
- `gui-list`: Iterate through arrays/objects
- `gui-show`: Conditionally display elements
- `gui-click`: Handle click events
- `gui-val`: Bind input values
- `gui-item`: Define iteration variable in lists

### Frontend Structure
- Include `turtlegui.js` and `turtlegui.css` files
- Use data binding instead of manual DOM manipulation
- Call `turtlegui.reload()` after data updates from API calls

### API Integration Pattern
```javascript

// Specify Domain class
class Domain {
    constructor() {
        this.name = null;
        this.address = null;
    }
};

// Instantiate global domain object
var domain = Domain();

// Fetch data from API backend
fetch('/api/domain_object')
  .then(response => response.json())
  .then(data => {
    domain.name = data.name;
    domain.address = data.address;
    turtlegui.reload();
  });
```

## Development Guidelines

- No other frontend frameworks (React, Vue, Angular, etc.)
- No other backend frameworks (Django, FastAPI, Express, etc.)
- Use MongoDB with proper indexing for performance
- Follow Flask best practices for web development
- Keep TurtleGUI data objects simple and flat when possible
- Write unit tests to cover all backend code

### Unit Test Guidelines

- Each test file corresponds to a python file, is in the same directory and has a similar name as the corresponding file, with "_test" at the end, for example: the main.py test file is called main_test.py. Do not use a separate folder for tests.
- Run tests using: `.venv/bin/python -m pytest -v` for verbose output with test names and results
- Avoid mocking if possible. If not possible then try to restrict mocking to destructive calls like file writes or calls to third party APIs. Use mongomock to assert database entries.
- Use self.assertEquals() etc methods instead of using the assert keyword
- Tests should be discoverable by pytest (files matching `*_test.py` pattern)

### Unit Test Structure for mongo based tests

```
import unittest
import mongomock

from app import create_app()

class MyTest(unittest.TestCase):
    def setUp(self):
        self.mongo_client = mongomock.MongoClient()
        self.mongo_db = self.mongo_client['my_db']

        self.app = create_app(self.mongo_db)
        self.client = self.app.test_client()

    def test_api_call(self):
        # assert api call completed successfully
        response = self.client.post("/items", json={"name": "Briefcase"})
        self.assertEqual(200, response.status_code)

        # assert contents of database
        collection = self.mongo_db['known_collection']
        self.assertEqual(1, len(collection.count_documents({})))

        # assert item
        item = collection.find_one()
        self.assertEqual("Briefcase", item["name"])

```

## Metaphor Core Concepts

### Core Domain Concepts
- **Schema**: Central entity that defines the domain structure and contains specs, fields, and calculations
- **Spec**: Resource type definition (equivalent to a class/table) that can contain fields and be instantiated as resources
- **Field**: Individual data elements within specs, supporting various types (primitives, links, collections, calculations)
- **Resource**: Instance of a spec containing actual data
- **Calculation Fields**: Dynamic fields with formulas that auto-update when dependencies change

### Data Structure
- **Root Schema**: Top-level container for the entire domain model
- **Nested Resources**: Resources can contain other resources as fields
- **Link Fields**: References to other resources
- **Collection Fields**: Arrays of links or ordered collections
- **Reverse Links**: Automatically maintained bidirectional relationships

### Calculation System
- **Calc Language**: Domain-specific language for calculations starting from "self" or root
- **Aggregate Functions**: Built-in functions like `average()`, `sum()` for collections
- **Dependency Tracking**: Automatic recalculation when dependent resources change
- **Circular Dependency Detection**: Validation to prevent infinite calculation loops

### API Architecture
- **RESTful API**: Automatic generation of CRUD endpoints for all defined resources
- **Dynamic Schema**: Runtime schema modification through admin interface
- **Real-time Updates**: WebSocket support for live data synchronization
- **Authentication**: OAuth integration (Google, GitHub) with role-based permissions

### MCP Integration
- **MCP Server**: Model Context Protocol server implementation for AI assistant integration
- **HTTP Endpoint**: MCP server accessible at `/mcp/` for external tool integration
- **Local Development**: When asked to "do something active with metaphor", interact with the locally running dev server

### Mutation System
- **Schema Mutations**: Versioned schema changes (add/delete/rename specs and fields)
- **Background Updates**: Asynchronous processing of large schema changes
- **Validation**: Type checking and dependency validation before mutations

### Storage & Performance
- **MongoDB Integration**: Document-based storage with automatic indexing
- **Field Indexing**: Automatic index creation for link fields and calculations
- **Schema Versioning**: Version control for schema changes with rollback capability

### User Interface
- **Schema Editor**: Web-based interface for domain experts to build schemas at `/admin/schema_editor`
- **API Browser**: Interactive tool for testing and exploring the generated API at `/browser/`
- **Admin Interface**: User and permission management

### Local Development URLs
- **Development Server**: http://localhost:8000/
- **Schema Editor**: http://localhost:8000/admin/schema_editor
- **API Browser**: http://localhost:8000/browser/
- **Main API Endpoint**: http://localhost:8000/api/
- **MCP Endpoint**: http://localhost:8000/mcp/

### Starting the Development Server
```bash
# Start the complete development environment with MongoDB (detached mode)
docker compose up -d

# Stop the development environment
docker compose down
```

### User Management & API Authentication
- **User creation**: `docker compose exec metaphor metaphor create_superuser` or `docker compose exec metaphor metaphor create_user`
- **API token creation**: `docker compose exec metaphor metaphor create_token` (for MCP authentication)
- **MCP Authentication**: The MCP server requires Bearer token authentication via Authorization header
- **Always use docker compose exec**: All metaphor CLI commands should be run inside the container

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
