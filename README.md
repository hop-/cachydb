# CachyDB

A lightweight document-based database with Model Context Protocol (MCP) support, similar to MongoDB but designed for simplicity and AI integration.

## Features

- **Document-based storage**: Store JSON-like documents in collections
- **Multiple databases**: Create and manage multiple databases within a single instance
- **Schema validation**: Define and enforce schemas for your collections
- **Indexing**: Automatic ID indexing plus custom hash-based indexes on any field
- **Query operations**: Find documents with filters (eq, ne, gt, lt, gte, lte, in)
- **MCP integration**: Built-in MCP server for seamless AI assistant integration
- **Binary storage**: High-performance binary format with gzip compression
- **Write-Ahead Log (WAL)**: Crash recovery and durability guarantees
- **Persisted indexes**: Fast startup with indexes saved to disk

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

#### use_database

Switch the default database for subsequent operations. All operations without an explicit `database` parameter will use this database.

```json
{
  "name": "users_db"
}
```

#### current_database

Get the current default database name.

```json
{}
```

**Example workflow:**

```json
{"name": "create_database", "arguments": {"name": "analytics"}}
{"name": "current_database", "arguments": {}}  // Returns: "main"
{"name": "use_database", "arguments": {"name": "analytics"}}
{"name": "current_database", "arguments": {}}  // Returns: "analytics"
{"name": "create_collection", "arguments": {"name": "events"}}
// Collection created in "analytics" database
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
│   ├── cmd/               # CLI commands (including migrate)
│   ├── config/            # Configuration
│   └── mcp/               # MCP server
│       └── server.go      # MCP tool handlers
├── pkg/
│   └── db/                # Public database API
│       ├── types.go       # Core data structures (DatabaseManager, Database, Collection)
│       ├── schema.go      # Schema validation
│       ├── index.go       # Hash indexing system (with persistence)
│       ├── query.go       # Query engine (CRUD operations)
│       ├── storage.go     # Storage manager with WAL integration
│       ├── binary_storage.go  # Binary format reader/writer
│       ├── wal.go         # Write-Ahead Log implementation
│       ├── compression.go # Gzip compression utilities
│       └── migration.go   # JSON to binary migration tool
└── examples/
    ├── basic/             # Direct library usage example
    └── mcp-client/        # MCP client example
```

## Storage Architecture

CachyDB uses a modern storage architecture designed for performance, durability, and reliability:

### Write-Ahead Log (WAL)

- **Crash recovery**: All write operations are logged before being applied
- **Batch writes**: Operations are batched for performance (100 entries or 100ms)
- **Rotation**: WAL files rotate at 64MB to keep file sizes manageable
- **Retention**: Last 2 WAL files are kept for recovery
- **Checkpointing**: Periodic checkpoints mark successfully persisted data

### Binary Storage Format

- **Compression**: All documents are compressed using gzip
- **Offset index**: Fast document lookups using in-memory offset index
- **Checksums**: CRC32 checksums verify data integrity
- **File structure**:
  - `collection.data`: Binary file with compressed documents
  - `collection.idx`: Offset index mapping document IDs to file offsets
  - Header: Magic number, version, flags

### Persisted Indexes

- Indexes are saved to disk and loaded on startup
- No need to rebuild indexes from documents
- Faster database initialization

### Storage Format

Data is stored in `~/.cachydb/` (or custom `ROOT_DIR`):

```none
.cachydb/
├── wal-0000000001.bin        # WAL file (current)
├── wal-0000000002.bin        # WAL file (previous)
├── wal.checkpoint            # Checkpoint tracking
└── main/                      # Database name
    ├── db.meta.json          # Database metadata
    ├── users/                # Collection (binary format)
    │   ├── collection.meta.json  # Schema & storage format
    │   ├── collection.data   # Binary document storage (compressed)
    │   ├── collection.idx    # Offset index
    │   └── indexes/          # Persisted indexes
    │       ├── _id.json      # ID index
    │       └── email_idx.json  # Custom index
    └── posts/                # Another collection
        ├── collection.meta.json
        ├── collection.data
        ├── collection.idx
        └── indexes/
            └── _id.json
```

## Migration from JSON to Binary

If you have existing databases in JSON format, you can migrate them to the new binary format:

### Migrate a Single Database

```bash
./cachydb migrate --database mydb
```

This will:
1. Create a backup (`.backup` directory)
2. Load data from JSON format
3. Save to binary format with compression
4. Verify the migration was successful

### Migrate All Databases

```bash
./cachydb migrate --all
```

### Skip Backup (not recommended)

```bash
./cachydb migrate --database mydb --skip-backup
```

### Verify Migration

```bash
./cachydb migrate --database mydb --verify
```

### Restore from Backup

```bash
./cachydb migrate --database mydb --restore
```

## Storage Format

Legacy data is stored in JSON format (for backward compatibility):

```none
.cachydb/
└── main/                      # Database name
    ├── db.meta.json          # Database metadata
    ├── users/                # Collection (JSON format - legacy)
    │   ├── collection.meta.json  # Schema & indexes
    │   └── documents.json    # All documents
    └── posts/                # Another collection
        ├── collection.meta.json
        └── documents.json
```

New databases automatically use the binary format.

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
