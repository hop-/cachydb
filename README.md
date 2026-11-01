# CachyDB

A lightweight document-based database with Model Context Protocol (MCP) support, similar to MongoDB but designed for simplicity and AI integration.

## Features

- **Document-based storage**: Store JSON documents in collections
- **Multiple databases**: Create and manage multiple databases within a single instance
- **Schema validation**: Define and enforce schemas for your collections
- **Indexing**: Multi-value hash indexes on any top-level field (duplicate values supported)
- **Query operations**: Find documents with type-aware filters (eq, ne, gt, gte, lt, lte, in), sorted by `_id` with deterministic skip/limit
- **MCP integration**: Built-in MCP server supporting stdio and Streamable HTTP transports, with `use_database` per session
- **Durable writes**: Every acknowledged write is fsynced to a write-ahead log; concurrent writes share fsyncs (group commit)
- **Append-only storage**: zstd-compressed, append-only data segments with background compaction; only keys and indexes are kept in RAM
- **Crash recovery**: Restart always recovers, including after `kill -9`, power loss, or a torn file tail
- **Embeddable**: `pkg/db` is a Go library (`db.Open`)

## Database Structure

- **Databases**: Multiple isolated databases within the instance
- **Collections**: Multiple collections within each database
- **Documents**: JSON documents with a string `_id` (generated UUID if not provided)
- **Schemas**: Optional field definitions with type validation
- **Indexes**: Multi-value hash indexes on any top-level field (`_id` lookups are always fast)

Names of databases, collections, and indexes must match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`.

## Platform Support

CachyDB supports **Linux and macOS**; the BSDs also get the data-directory
lock. It compiles on Windows, Solaris, illumos, and AIX, but there the
data-directory lock is a no-op (cachydb logs a warning), and Windows is
not supported: there is no directory fsync either, so durability and the
single-process guarantee do not hold.

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

### Docker

```bash
docker run -d -p 7601:7601 -v cachydb-data:/data hovhannespalyan/cachydb
```

See [Docker](#docker) below.

## Usage

### Starting the MCP Server

#### stdio (default)

```bash
./cachydb
```

The server communicates over stdin/stdout — suitable for local MCP clients such as Claude Desktop.

#### HTTP (Streamable HTTP transport)

```bash
./cachydb --transport http
```

The server listens on port 7601 on all interfaces (`http://localhost:7601/mcp`) and implements the [MCP Streamable HTTP transport](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports). This is the recommended transport for networked or multi-client deployments.

Change the port with `--port`:

```bash
./cachydb --transport http --port 8080
```

### Configuration

CLI flags (override environment variables, which override the defaults):

```none
  -t, --transport            MCP transport: stdio (default) or http
  -p, --port                 Port for HTTP transport (default 7601)
  -R, --root                 Data directory (default ~/.cachydb)
  -d, --db                   Default database, created at startup (default main)
      --wal-retention-age    Keep processed WAL for this long, e.g. 24h (0 = no age limit)
      --wal-retention-size   Keep at most this much processed WAL, e.g. 1GiB (0 = no size limit)
      --cache-size           Document cache size, e.g. 64MiB (0 = cache disabled)
```

Environment variables:

- `TRANSPORT`: `stdio` or `http` (default: `stdio`)
- `PORT`: Port for the HTTP transport (default: `7601`)
- `ROOT_DIR`: Data directory (default: `~/.cachydb`)
- `DB_NAME`: Default database (default: `main`)
- `WAL_RETENTION_AGE`: How long processed WAL segments are kept, as a Go duration (default: `24h`, `0` = no age limit)
- `WAL_RETENTION_SIZE`: Maximum size of processed WAL kept (default: `1GiB`, `0` = no size limit)
- `CACHE_SIZE`: Document cache size (default: `64MiB`, `0` = disabled)

Sizes accept `B`, `KiB`, `MiB`, `GiB`, `TiB` (powers of 1024) and `KB`, `MB`, `GB`, `TB` (powers of 1000). Retention never deletes WAL that has not yet been written to the data segments.

Run `cachydb docs config` for every setting, or `cachydb --help`.

Only one process can use a data directory at a time: the server holds an exclusive lock on `<data dir>/cachydb.lock`, and a second server or a `cachydb utils` command on the same directory fails with "data directory is in use by another cachydb process".

Logs go to stderr; in stdio mode stdout carries only MCP messages. SIGINT/SIGTERM, or the client closing stdin, shut the server down gracefully (final flush, exit status 0). Errors exit with status 1.

### Built-in documentation for AI agents

The binary documents itself, so an agent does not need this repository:

```bash
cachydb docs                         # topics: overview, quickstart, mcp, tools, query, schema, config, errors
cachydb docs quickstart              # one topic (markdown)
cachydb docs tools --format json     # every MCP tool with its input/output JSON Schema and an example
cachydb version --json               # version, Go, storage/WAL format, MCP protocol
```

Over MCP, the server publishes typed tool schemas (with enums for query
operators and field types), read-only/destructive/idempotent annotations,
short usage instructions in the `initialize` result, and every docs topic
as a `cachydb://docs/<topic>` resource. The `mcp` topic has config
examples for Claude Code, Claude Desktop, Cursor, and VS Code.

### MCP Configuration

#### stdio transport

Add to your MCP client settings (e.g. Claude Desktop `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "cachydb": {
      "command": "/path/to/cachydb",
      "args": ["-R", "/path/to/data"]
    }
  }
}
```

#### HTTP transport

Start the server first:

```bash
./cachydb --transport http --port 7601
```

Then point your MCP client at the endpoint:

```json
{
  "mcpServers": {
    "cachydb": {
      "url": "http://localhost:7601/mcp"
    }
  }
}
```

The HTTP endpoint implements the MCP Streamable HTTP transport — clients POST JSON-RPC messages to `/mcp` and receive responses via SSE. Idle sessions expire after 30 minutes. There is no authentication; do not expose the port to untrusted networks.

The HTTP server also answers `GET /healthz`: `200 {"status":"ok","version":"..."}` while the database is open, `503` while it shuts down. `cachydb healthcheck` probes it and exits 0 (healthy) or 1, for container and service-manager healthchecks:

```bash
TRANSPORT=http cachydb healthcheck                    # http://127.0.0.1:$PORT/healthz (with TRANSPORT=stdio it exits 0 without probing)
cachydb healthcheck --url http://db.internal:7601/healthz
```

## Docker

The official image [`hovhannespalyan/cachydb`](https://hub.docker.com/r/hovhannespalyan/cachydb) is a small distroless image for `linux/amd64` and `linux/arm64`. It runs as the non-root user `65532`, keeps data in the `/data` volume, and is configured with the [environment variables](#configuration). Its defaults are `TRANSPORT=http`, `PORT=7601`, `ROOT_DIR=/data`, `DB_NAME=main`. Tags: `latest`, `X.Y`, `X.Y.Z`.

```bash
# HTTP server at http://localhost:7601/mcp, data in the cachydb-data volume
docker run -d --name cachydb -p 7601:7601 -v cachydb-data:/data hovhannespalyan/cachydb

# stdio, started by an MCP client (keep -i, no -t)
docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb

# other commands
docker run --rm hovhannespalyan/cachydb version --json
```

With Docker Compose, the repository's [`docker-compose.yml`](docker-compose.yml) runs the HTTP server with a named volume: `docker compose up -d`.

For an agent, point the client at the HTTP container, or let it start a stdio container:

```bash
claude mcp add --transport http cachydb http://localhost:7601/mcp
claude mcp add --transport stdio cachydb -- docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb
```

Notes:

- **Volume permissions**: named volumes work as they are. A bind mount keeps the host directory's owner, so `chown 65532:65532` it first, or run with `--user "$(id -u):$(id -g)"`.
- **One container per volume**: the data-directory lock allows one cachydb process per volume. Share data between agents through one HTTP container.
- **Healthcheck**: the image's `HEALTHCHECK` runs `cachydb healthcheck`. It reads `PORT` and `TRANSPORT` from the environment, so set those as environment variables rather than flags.
- **Shutdown**: `docker stop` sends SIGTERM; cachydb flushes and exits 0.
- **Build locally**: `docker build --build-arg VERSION=$(git describe --tags --always) -t cachydb .`

The Docker Hub overview is [`DOCKERHUB.md`](DOCKERHUB.md).

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

Switch the default database for subsequent operations in the current MCP session. All operations in this session without an explicit `database` parameter will use this database. Other sessions are not affected, and the choice is not persisted.

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

**Note**: `database` parameter is optional and defaults to the session's `use_database` choice, or `main`.

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

If `_id` is not provided, a UUID is generated. A provided `_id` must be a string. Integer values are stored as JSON numbers (Go library callers get them back as `float64`).

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

**Operators**: `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `in` (`in` takes an array of values)

- All filters must match. `field` is a top-level field name or `_id` (nested paths are not supported).
- Comparison is type-aware: numbers compare numerically, strings lexically. Values of different types never match `eq`, `gt`, `gte`, `lt`, or `lte`.
- A document that lacks the filtered field matches no operator, including `ne` (unlike MongoDB's `$ne`).
- Results are sorted by `_id`; `skip` and `limit` page deterministically. An omitted or empty `query` returns all documents.

#### update_document

Update a document by ID. Top-level fields in `updates` are replaced or added; other fields are kept. `_id` cannot be updated.

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

Create an index on a collection field for faster `eq` queries. Indexes support duplicate values. Building an index blocks writes to that database until it finishes.

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
│   ├── app/               # Application setup (opens the engine, starts the MCP server)
│   ├── cmd/               # CLI commands (server, docs, version, utils list/migrate)
│   ├── config/            # Configuration (env vars, size parsing)
│   ├── docs/              # Built-in agent docs topics (cachydb docs, MCP resources)
│   └── mcp/
│       ├── server.go      # MCP tool handlers, sessions, transports
│       ├── tools.go       # Tool definitions: typed schemas, annotations, examples
│       └── docs.go        # Server instructions, docs resources, generated tools topic
├── pkg/
│   └── db/                # Public database engine (db.Open → *Engine)
│       ├── engine.go      # Open/Close, databases
│       ├── document.go    # Write/read paths, Find
│       ├── flush.go       # Background (parallel) flush and checkpoints
│       ├── backpressure.go # Write backpressure (MaxPendingBytes)
│       ├── metacp.go      # Metadata checkpoint (cachydb.metacp)
│       ├── recovery.go    # Crash recovery and WAL replay
│       ├── compact.go     # Background compaction
│       ├── migration.go   # Schema-version migrations
│       └── internal/      # wal, segment, cache, fsutil
└── examples/
    ├── basic/             # Direct library usage example
    └── mcp-client/        # MCP client example
```

Developer documentation is in [`AGENTS.md`](AGENTS.md) and [`dev-docs/`](dev-docs/PROJECT.md).

## Storage Architecture

### Write-Ahead Log (WAL)

- **Commit point**: every change is appended to the WAL and fsynced before it is applied or acknowledged
- **Group commit**: concurrent writes share a single fsync, with no added delay
- **Checksums**: every record has a CRC32C checksum; a torn record at the end of the log is truncated on startup
- **Rotation**: WAL segments rotate at 64 MiB
- **Retention**: processed segments are deleted by age (`WAL_RETENTION_AGE`, default 24h) and/or total size (`WAL_RETENTION_SIZE`, default 1 GiB); segments not yet written to the data segments are never deleted

### Data Segments

- **Append-only**: a background flusher (every second, or when 16 MiB of changes accumulate) appends only the changed documents and deletion tombstones to the collection's active segment, followed by a checkpoint marker, with one fsync. A one-document change never rewrites a collection. Up to four collections are flushed in parallel.
- **Backpressure**: when the disk is slower than the writers, writes wait while the unflushed changes of all collections exceed `MaxPendingBytes` (by default 4 × the 16 MiB memtable size × half the CPUs, at most 1 GiB), so memory stays bounded
- **Compression**: documents are compressed with zstd
- **Keys in RAM**: an in-memory key directory maps each document id to its location on disk; documents are read on demand through an LRU cache of decoded documents (`CACHE_SIZE`, default 64 MiB), so a cached read skips the disk and the JSON decoding
- **Compaction**: a background task rewrites sealed segments with many dead records (superseded or deleted versions) to reclaim space, and merges runs of small segments
- **Hint files and index snapshots**: speed up startup; both are rebuilt automatically if missing or damaged

### Recovery

On startup CachyDB loads the key directories and indexes, then replays the WAL from each collection's last checkpoint (and, for metadata, from the `cachydb.metacp` checkpoint). Replay is idempotent, so a crash at any point — including during a flush or compaction — loses no acknowledged write. A clean shutdown flushes everything, so the next start has nothing to re-apply.

### Storage Format

Data is stored in `~/.cachydb/` (or the directory given with `-R`):

```none
.cachydb/
├── cachydb.lock                    # Exclusive lock held by the running process
├── cachydb.format                  # On-disk format version
├── cachydb.metacp                  # Metadata checkpoint (where WAL replay may start)
├── wal-00000000000000000001.log    # WAL segments, named by first offset
├── wal-00000000000004213377.log
└── main/                           # Database
    ├── db.meta                     # Database metadata (schema version)
    └── users/                      # Collection
        ├── coll.meta               # Schema and index definitions
        ├── seg-000001.data         # Sealed data segment (zstd records)
        ├── seg-000001.hint         # Hint file for fast startup
        ├── seg-000002.data         # Active data segment
        └── idx-email_idx.snap      # Index snapshot
```

The files are binary (except the small JSON metadata files). See [`dev-docs/STORAGE.md`](dev-docs/STORAGE.md) for the formats.

## Upgrading from Older Versions

This storage engine is **not compatible** with data directories created by earlier CachyDB versions (JSON or `collection.data`/`collection.idx` files, `wal.checkpoint`). Opening one fails with:

```none
data directory <dir> was created by an older cachydb format; move or delete it
```

Move the old directory aside (or delete it) and start with an empty one. There is no automatic converter.

### Schema Migrations

Databases carry a schema version. `cachydb utils` commands open the data directory directly, so stop the server first:

```bash
./cachydb utils list -c                  # databases, schema versions, collections and document counts
./cachydb utils migrate --all            # migrate every database to the current schema version
./cachydb utils migrate --database mydb  # migrate one database
./cachydb utils migrate --list           # list registered migrations
```

All `utils` commands accept `-R` for a custom data directory. See [`dev-docs/CLI.md`](dev-docs/CLI.md) for details.

## Using CachyDB as a Go Library

```go
engine, err := db.Open(ctx, db.Options{Dir: "/path/to/data"})
if err != nil {
    return err
}
defer engine.Close(ctx)

users, _ := engine.CreateDatabase(ctx, "users_db")
_ = users.CreateCollection(ctx, "users", nil) // nil = no schema
coll, _ := users.Collection("users")

id, _ := coll.Insert(ctx, &db.Document{Data: map[string]any{"name": "Alice", "age": 28}})
docs, _ := coll.Find(ctx, &db.Query{Filters: []db.QueryFilter{{Field: "age", Operator: "gte", Value: 18}}})
```

See [`examples/basic/`](examples/basic/) for a complete program and [`dev-docs/DATABASE_ENGINE.md`](dev-docs/DATABASE_ENGINE.md) for the API reference.

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
- **zstd compression** - [`github.com/klauspost/compress`](https://github.com/klauspost/compress)
- **stdio transport** — for local MCP clients
- **Streamable HTTP transport** — MCP spec 2025-03-26+, SSE-based, suitable for networked deployments
