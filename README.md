# CachyDB

A lightweight document-based database with Model Context Protocol (MCP) support, similar to MongoDB but designed for simplicity and AI integration.

## Features

- **Document-based storage**: Store JSON-like documents in collections
- **Multiple databases**: Create and manage multiple databases within a single instance
- **Schema validation**: Define and enforce schemas for your collections
- **Indexing**: Automatic ID indexing plus custom hash-based indexes on any field
- **Query operations**: Find documents with filters (eq, ne, gt, lt, gte, lte, in)
- **MCP integration**: Built-in MCP server for seamless AI assistant integration
- **File-based persistence**: Simple file storage for reliability

## Database Structure

- **Database Manager**: Manages multiple databases
- **Databases**: Multiple isolated databases within the instance
- **Collections**: Multiple collections within each database
- **Documents**: JSON-like documents with automatic `_id` field
- **Schemas**: Optional field definitions with type validation
- **Indexes**: Hash-based indexes (automatic on `_id`, custom on any field)

## Installation

### From Source

```bash
git clone https://github.com/hop-/cachydb.git
cd cachydb
go build -o cachydb
```

### Using go install

```bash
go install github.com/hop-/cachydb@latest
```

## Usage

### Starting the MCP Server

```bash
./cachydb
```

The server runs in stdio mode for MCP communication.

### Configuration

Environment variables:

- `DB_NAME`: Database name (default: "main")
- `ROOT_DIR`: Data directory (default: "~/.cachydb")
- `PORT`: Port number (default: 7601)

### MCP Configuration

Add to your MCP settings (e.g., Claude Desktop config):

```json
{
  "mcpServers": {
    "cachydb": {
      "command": "/path/to/cachydb",
      "args": [],
      "env": {
        "DB_NAME": "main",
        "ROOT_DIR": "/path/to/data"
      }
    }
  }
}
```

## MCP Tools

### Database Management

#### create_database

Create a new database.

```json
{
  "name": "users_db"
}
```

#### list_databases

List all databases.

```json
{}
```

#### delete_database

Delete a database.

```json
{
  "name": "old_db"
}
```

### Collection Management

#### create_collection

Create a new collection with optional schema.

```json
{
  "database": "users_db",
  "name": "users",
  "schema": {
    "fields": {
      "name": {
        "type": "string",
        "required": true
      },
      "email": {
        "type": "string",
        "required": true
      },
      "age": {
        "type": "number",
        "required": false
      }
    }
  }
}
```

**Note**: `database` parameter is optional and defaults to the configured `DB_NAME`.

**Field Types**: `string`, `number`, `boolean`, `object`, `array`, `date`

#### list_collections

List all collections in a database.

```json
{
  "database": "users_db"
}
```

### Document Management

#### insert_document

Insert a document into a collection.

```json
{
  "database": "users_db",
  "collection": "users",
  "document": {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }
}
```

If `_id` is not provided, it will be auto-generated.

#### find_documents

Query documents in a collection.

```json
{
  "database": "users_db",
  "collection": "users",
  "query": {
    "filters": [
      {
        "field": "age",
        "operator": "gte",
        "value": 25
      }
    ],
    "limit": 10,
    "skip": 0
  }
}
```

**Operators**: `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `in`

#### update_document

Update a document by ID.

```json
{
  "database": "users_db",
  "collection": "users",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "updates": {
    "age": 31,
    "city": "New York"
  }
}
```

#### delete_document

Delete a document by ID.

```json
{
  "database": "users_db",
  "collection": "users",
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Index Management

#### create_index

Create an index on a collection field for faster queries.

```json
{
  "database": "users_db",
  "collection": "users",
  "index_name": "email_idx",
  "field_name": "email"
}
```

## Architecture

```none
cachydb/
├── main.go                 # Entry point
├── internal/
│   ├── app/               # Application setup
│   ├── cmd/               # CLI commands
│   ├── config/            # Configuration
│   └── mcp/               # MCP server
│       └── server.go      # MCP tool handlers
├── pkg/
│   └── db/                # Public database API
│       ├── types.go       # Core data structures (DatabaseManager, Database, Collection)
│       ├── schema.go      # Schema validation
│       ├── index.go       # Hash indexing system
│       ├── query.go       # Query engine (CRUD operations)
│       └── storage.go     # File-based persistence
└── examples/
    ├── basic/             # Direct library usage example
    └── mcp-client/        # MCP client example
```

## Storage Format

Data is stored in `~/.cachydb/` (or custom `ROOT_DIR`):

```none
.cachydb/
└── main/                      # Database name
    ├── db.meta.json          # Database metadata
    ├── users/                # Collection
    │   ├── collection.meta.json  # Schema & indexes
    │   └── documents.json    # All documents
    └── posts/                # Another collection
        ├── collection.meta.json
        └── documents.json
```

## Examples

### Using with AI Assistant

Once configured in your MCP client (like Claude Desktop), you can interact naturally:

```none
User: Create a users collection with name and email fields
Assistant: [calls create_collection tool]

User: Add a user named Alice with email alice@example.com
Assistant: [calls insert_document tool]

User: Find all users
Assistant: [calls find_documents tool]

User: Create an index on the email field for faster lookups
Assistant: [calls create_index tool]
```

### Schema Validation

When a schema is defined, all documents must conform:

```json
// Schema requires name and email
{
  "fields": {
    "name": { "type": "string", "required": true },
    "email": { "type": "string", "required": true }
  }
}

// ✅ Valid document
{ "name": "Alice", "email": "alice@example.com" }

// ❌ Invalid - missing required field
{ "name": "Bob" }

// ❌ Invalid - wrong type
{ "name": 123, "email": "bob@example.com" }
```

### Index Usage

Indexes speed up equality queries:

```json
// Create index on email field
{ "collection": "users", "index_name": "email_idx", "field_name": "email" }

// This query will use the index for fast lookup
{
  "collection": "users",
  "query": {
    "filters": [{ "field": "email", "operator": "eq", "value": "alice@example.com" }]
  }
}
```

## Version

```bash
./cachydb version
```

## Technology

Built with:

- **Go 1.25+** - Programming language
- **Official MCP Go SDK** - [`github.com/modelcontextprotocol/go-sdk`](https://github.com/modelcontextprotocol/go-sdk)
- Type-safe tool handlers with automatic JSON schema generation
- Standard stdio transport for MCP communication
