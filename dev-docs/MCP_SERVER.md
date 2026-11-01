# MCP Server Reference

The MCP (Model Context Protocol) server lives in `internal/mcp/server.go`
and exposes CachyDB as tools for AI assistants. It uses the official Go SDK
(`github.com/modelcontextprotocol/go-sdk` v1.8.0). Handlers only call
methods of the `*db.Engine` they are given; durability is entirely the
engine's job (see [STORAGE.md](STORAGE.md)). Overview:
[PROJECT.md](PROJECT.md).

## Server Configuration

| Setting | How to set it | Default |
|---------|---------------|---------|
| Transport | `-t` / `--transport` flag, or `TRANSPORT` env var (`stdio` or `http`) | `stdio` |
| HTTP port | `-p` / `--port` flag, or `PORT` env var | `7601` |
| Data directory | `-R` / `--root` flag, or `ROOT_DIR` env var | `~/.cachydb` |
| Default database | `-d` / `--db` flag, or `DB_NAME` env var | `main` |

Precedence: flag > environment variable > default.

## Transports

### stdio (default)

One session over stdin/stdout. stdout carries only JSON-RPC; logs and
messages go to stderr. The server stops when stdin closes.

```json
{ "mcpServers": { "cachydb": { "command": "/path/to/cachydb", "args": ["-R", "/path/to/data"] } } }
```

Closing stdin is a clean shutdown: `startStdio` treats end of input (EOF,
closed pipe, connection closed, "server is closing") as success, the
engine is closed, and the process exits 0. Clients must still keep stdin
open until they have read their responses: SDK v1.8.0 cancels requests that
are in flight at EOF and does not write their responses (observed with
initialize + tools/call piped and stdin closed at once: no output, exit 0).

### Streamable HTTP

`cachydb -t http -p 7601` serves the MCP Streamable HTTP transport (spec
2025-03-26+) at `http://<host>:7601/mcp`, listening on all interfaces.
Multiple concurrent sessions are supported. Idle sessions are closed after
**30 minutes** (`httpSessionTimeout`). There is no authentication or TLS.

```json
{ "mcpServers": { "cachydb": { "url": "http://localhost:7601/mcp" } } }
```

The SDK's DNS-rebinding (localhost) protection stays on for `/mcp`. It
rejects only requests that arrive via 127.0.0.1/[::1] with a non-localhost
`Host` header, so Docker port mapping and container-to-container access
are not affected.

### `GET /healthz`

`httpHandler()` also serves `/healthz` next to `/mcp`, outside MCP session
handling (`Server.healthz`):

| Request | Status | Body |
|---------|--------|------|
| `GET`, engine open | `200` | `{"status":"ok","version":"<build version>"}` |
| `GET`, engine closing or closed (`Engine.Closed()`) | `503` | `{"status":"unavailable"}` |
| any other method | `405` | `Allow: GET` |

Responses are `application/json` with `Cache-Control: no-store`.
`cachydb healthcheck` probes it (see [CLI.md](CLI.md#cachydb-healthcheck)),
and the Docker image's `HEALTHCHECK` runs that command. The stdio
transport has no health endpoint.

## Lifecycle

1. `app.Builder.Build` opens the engine (`db.Open`: lock, recovery).
2. `NewServer(ctx, engine, defaultDBName, transport, httpAddr, version)`
   creates the default database if missing, registers the 12 tools and the
   8 docs resources, and sets the server instructions and the
   implementation version (the build version, from `cmd.getVersion`).
3. `Server.Start(ctx)` runs the transport until `ctx` is cancelled
   (SIGINT/SIGTERM), stdin closes (stdio), or the transport fails.
4. `App.Stop` (10s timeout) calls `Server.Close` (ends every session) and
   then `Engine.Close` (final flush). For HTTP, cancellation first closes
   the sessions and then shuts the HTTP server down with a 5s timeout.

## Sessions and `use_database`

The selected database is **per MCP session**. `ServerOptions.InitializedHandler`
and a receiving middleware (`trackSessionMiddleware`, needed because
clients on protocol 2026-07-28 use `server/discover` instead of the
initialize handshake) register each session in `Server.sessions`; a goroutine waits on
`ServerSession.Wait()` and forgets the session when it ends. Each handler
resolves its database in this order:

1. the tool's `database` argument, if given;
2. the session's `use_database` choice;
3. the server default (`main`).

## Tools (12)

Tools are defined once in `internal/mcp/tools.go` (`toolSpecs`): name,
description, annotations, typed input and output structs, and an example
call. `registerTools` registers exactly these, and `ToolDefinitions()`
returns them without an engine for the generated docs. The input and
output schemas are generated with `jsonschema.For` from the structs' `json`
and `jsonschema` tags; `Operator` and `FieldType` map to string enums
(`Operators`, `FieldTypes`). The SDK validates arguments against the input
schema, returns the typed output as structured content and as JSON text
content, and validates it against the output schema. Every successful
result has `"success": true`.

Annotations (all tools also set `openWorldHint: false` and a title):

| Tool(s) | readOnly | destructive | idempotent |
|---------|----------|-------------|------------|
| `list_databases`, `list_collections`, `find_documents`, `current_database` | true | — | true |
| `create_database`, `use_database` | false | false | true |
| `create_collection`, `insert_document`, `create_index`, `update_document` | false | false | false |
| `delete_document`, `delete_database` | false | true | true |

| Tool | Arguments | Result fields |
|------|-----------|---------------|
| `create_database` | `name` | `message`. An existing name succeeds (no-op) |
| `list_databases` | — | `databases` (sorted; `[]` when empty) |
| `delete_database` | `name` | `message` |
| `use_database` | `name` (must exist) | `message`, `current_database` |
| `current_database` | — | `current_database` (session choice or default) |
| `create_collection` | `name`, `database`?, `schema`? | `message` |
| `list_collections` | `database`? | `collections` (sorted; `[]` when empty), `database` |
| `insert_document` | `collection`, `document`, `database`? | `id`, `message` |
| `find_documents` | `collection`, `query`?, `database`? | `count`, `documents` |
| `update_document` | `collection`, `id`, `updates`, `database`? | `message` |
| `delete_document` | `collection`, `id`, `database`? | `message` |
| `create_index` | `collection`, `index_name`, `field_name`, `database`? | `message` |

`?` = optional. There are no tools to drop a collection or an index; the
engine supports both (`DropCollection`, `DropIndex`) but they are not
exposed.

### `create_collection` schema

```json
{
  "name": "users",
  "schema": {
    "fields": {
      "name": { "type": "string", "required": true },
      "age":  { "type": "number" }
    }
  }
}
```

Types: `string`, `number`, `boolean`, `object`, `array`, `date`. Omit
`schema` for a schemaless collection. Names (databases, collections,
indexes) must match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`.

### `insert_document`

`document` is the data object. A **string** `_id` in it becomes the
document id; without one a UUID is generated. A non-string `_id` is
rejected (schema validation error). Numbers are stored as JSON numbers:
clients read `28` back as `28`, while Go library callers get `float64`.

### `find_documents`

Filters, limit, and skip are nested under `query`:

```json
{
  "collection": "users",
  "query": {
    "filters": [
      { "field": "age",    "operator": "gte", "value": 18 },
      { "field": "status", "operator": "in",  "value": ["active", "trial"] }
    ],
    "limit": 10,
    "skip": 0
  }
}
```

- Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`. All filters must
  match. Fields are top-level names or `_id` (no dot-notation).
- Type-aware comparison: numbers compare numerically, strings lexically;
  values of different types never match `eq/gt/gte/lt/lte`.
- A document **missing the field matches no operator, including `ne`**
  (unlike MongoDB's `$ne`).
- Results are sorted by `_id`; `skip`/`limit` page deterministically.
  An omitted or empty `query` returns every document.
- Each document is returned as a flat object with `_id`.

Full semantics: [DATABASE_ENGINE.md — Queries](DATABASE_ENGINE.md#queries).

### `update_document`

`updates` is merged into the document: top-level fields are replaced or
added, others are kept. `_id` cannot be updated. The schema is validated on
the merged result.

### `create_index`

Builds a multi-value index from the existing documents. Writes to the whole
database wait while it builds. Only `eq` filters use indexes.

## Self-description

- **Instructions**: `ServerOptions.Instructions` is `Instructions()` in
  `internal/mcp/docs.go`, a condensed guide (under 2,000 characters; a test
  checks it) with the typical workflow and the `cachydb://docs/<topic>`
  URIs of every topic in `docs.Names`.
- **Resources**: one `text/markdown` resource per docs topic at
  `cachydb://docs/<topic>` (name and title = the topic title, description
  = its summary). Reads return the same content as `cachydb docs <topic>`;
  the `tools` topic is rendered from the tool definitions
  (`DocsTopics`/`ToolDocs`).
- **Implementation info**: name `cachydb`, the real build version.

## Errors

Invalid arguments are rejected by the SDK before the handler runs, as a
tool error starting `validating "arguments":` that names the property and,
for enums, lists the valid values (for example `enum: like does not equal
any of: [eq ne gt gte lt lte in]`). Unknown properties are rejected too
(`additionalProperties: false`), which catches `filter` instead of
`query.filters`.

Handler errors are returned to the client as tool errors (`isError: true`)
whose text names the action and the cause (`toolError` in `server.go`):

| Engine error | Message shape |
|--------------|---------------|
| `ErrNotFound` | `<action>: <engine message> (hint: list_databases and list_collections show what exists; …)` |
| `ErrSchemaValidation` | `<action>: <engine message> (hint: see resource cachydb://docs/schema)` |
| `ErrExists`, `ErrInvalidName` | `<action>: <engine message>` |
| `ErrClosed` | `<action>: the database engine is shut down: …` |
| `ErrWALFailed`, `ErrWALCorrupt` | `<action>: write-ahead log failure, restart the server: …` |
| context cancelled / deadline | `<action>: request cancelled: …` |

## Write consistency

A tool call that returns success has its change fsynced to the WAL. It
survives a crash (`kill -9`, power loss) and is visible to every session.
Data segments are written by the background flusher; on restart, recovery
replays any WAL records not yet in the segments.
