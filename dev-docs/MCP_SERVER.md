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
   creates the default database if missing, registers the 21 tools and the
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

## Tools (21)

Tools are defined once in `internal/mcp/tools.go` (`toolSpecs`): name,
description, annotations, typed input and output structs, and an example
call. `registerTools` registers exactly these, and `ToolDefinitions()`
returns them without an engine for the generated docs. The input and
output schemas are generated with `jsonschema.For` from the structs' `json`
and `jsonschema` tags; `Operator`, `FieldType`, and `SortOrder` map to
string enums (`Operators`, `FieldTypes`, `SortOrders`). The SDK validates
arguments against the input schema, returns the typed output as structured
content and as JSON text content, and validates it against the output
schema. Every successful result has `"success": true`. Plan 004 added nine
tools and optional parameters; every change is additive (existing calls
and outputs are unchanged, outputs only gained keys).

Annotations (all tools also set `openWorldHint: false` and a title):

| Tool(s) | readOnly | destructive | idempotent |
|---------|----------|-------------|------------|
| `list_databases`, `list_collections`, `find_documents`, `current_database`, `count_documents`, `distinct`, `describe_collection` | true | — | true |
| `create_database`, `use_database`, `replace_document`, `update_schema` | false | false | true |
| `create_collection`, `insert_document`, `create_index`, `update_document`, `rename_collection`, `copy_collection` | false | false | false |
| `delete_document`, `delete_database`, `delete_collection`, `drop_index` | false | true | true |

| Tool | Arguments | Result fields |
|------|-----------|---------------|
| `create_database` | `name` | `message`. An existing name succeeds (no-op) |
| `list_databases` | — | `databases` (sorted; `[]` when empty) |
| `delete_database` | `name` | `message` |
| `use_database` | `name` (must exist) | `message`, `current_database` |
| `current_database` | — | `current_database` (session choice or default) |
| `create_collection` | `name`, `database`?, `schema`? | `message` |
| `list_collections` | `database`? | `collections` (sorted; `[]` when empty), `database` |
| `describe_collection` | `collection`, `database`? | `database`, `name`, `schema` (or null), `indexes` [`{name, field}`, sorted], `count` |
| `update_schema` | `collection`, `schema` (object or null), `database`? | `message` |
| `rename_collection` | `collection`, `new_name`, `database`? | `message` |
| `copy_collection` | `collection`, `new_name`, `target_database`?, `database`? | `message`, `copied` |
| `delete_collection` | `collection`, `database`? | `message` (`ErrNotFound` if missing) |
| `insert_document` | `collection`, `document`, `database`? | `id`, `message` |
| `find_documents` | `collection`, `query`?, `database`? | `count`, `documents` |
| `count_documents` | `collection`, `query`? (`filters`, `any`), `database`? | `count` |
| `distinct` | `collection`, `field`, `query`? (`filters`, `any`), `database`? | `values` |
| `update_document` | `collection`, `id`, `updates`?, `unset`?, `if`?, `upsert`?, `database`? | `message`, `upserted` |
| `replace_document` | `collection`, `id`, `document`, `if`?, `database`? | `message` |
| `delete_document` | `collection`, `id`, `if`?, `database`? | `message` |
| `create_index` | `collection`, `index_name`, `field_name`, `database`? | `message` |
| `drop_index` | `collection`, `index_name`, `database`? | `message` (`ErrNotFound` if missing) |

`?` = optional. Handlers are thin: each resolves the database (see
above) and calls one engine method (`UpdateWith`, `Replace`,
`DeleteWith`, `CountDocuments`, `Distinct`, `Schema`/`Indexes`/`Count`,
`UpdateSchema`, `RenameCollection`, `CopyCollection`, `DropCollection`,
`DropIndex`).

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
indexes) must match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`. `update_schema`
changes it later (every document is checked first).

### `insert_document`

`document` is the data object. A **string** `_id` in it becomes the
document id; without one a UUID is generated. A non-string `_id` is
rejected (schema validation error). Numbers are stored as JSON numbers:
clients read `28` back as `28`, while Go library callers get `float64`.
`{"$now": true}` values (any depth) become the server time.

### `find_documents`

Filters, OR groups, projection, sort, limit, and skip are nested under
`query`:

```json
{
  "collection": "notes",
  "query": {
    "filters": [ { "field": "tags", "operator": "has", "value": "wal" } ],
    "any": [ [ { "field": "type", "operator": "eq", "value": "decision" } ],
             [ { "field": "prio", "operator": "gte", "value": 3 } ] ],
    "sort": [ { "field": "updated", "order": "desc" } ],
    "fields": [ "title" ],
    "limit": 10,
    "skip": 0
  }
}
```

- Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`,
  `has`, `has_any`, `has_all`. All `filters` must match, and with `any`
  also every filter of at least one group. Fields are top-level names or
  `_id` (no dot-notation).
- Type-aware comparison: numbers compare numerically, strings lexically;
  values of different types never match `eq/gt/gte/lt/lte`.
- A document **missing the field matches no operator, including `ne`**
  (unlike MongoDB's `$ne`).
- Results are sorted by `sort`, then `_id`; `skip`/`limit` page
  deterministically. An omitted or empty `query` returns every document.
- Each document is returned as a flat object with `_id` (only the
  `fields` listed, plus `_id`, when `fields` is set).

Full semantics: [DATABASE_ENGINE.md — Queries](DATABASE_ENGINE.md#queries).

### `update_document`, `replace_document`, `delete_document`

`updates` is merged into the document (top-level fields are replaced or
added), then the `unset` fields are removed; at least one of the two is
required, and a field in both is an error. `_id` cannot be updated or
unset. `upsert: true` creates a missing document as `{_id: id} + updates`
(`upserted: true` in the result). `replace_document` replaces the whole
document atomically (its `_id`, if given, must equal `id`). The schema is
validated on the result. `if` (on all three) is a list of filters the
stored document must match, checked under the document lock just before
the commit; a mismatch (or a missing document) fails with
`condition failed: document <id> no longer matches if; re-read it and
retry` and writes nothing.

### `create_index`, `drop_index`

Builds a multi-value index from the existing documents (array fields are
indexed per element). Writes to the whole database wait while it builds.
`eq`, `has`, `has_any`, and `has_all` filters use indexes.

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
- **HTTP handler**: `(*Server).HTTPHandler()` returns the `/mcp` +
  `/healthz` handler, so callers and tests (`internal/cmd/client_test.go`)
  can serve it on their own listener.

## Errors

Invalid arguments are rejected by the SDK before the handler runs, as a
tool error starting `validating "arguments":` that names the property and,
for enums, lists the valid values (for example `enum: like does not equal
any of: [eq ne gt gte lt lte in contains has has_any has_all]`). Unknown properties are rejected too
(`additionalProperties: false`), which catches `filter` instead of
`query.filters`.

Handler errors are returned to the client as tool errors (`isError: true`)
whose text names the action and the cause (`toolError` in `server.go`):

| Engine error | Message shape |
|--------------|---------------|
| `ErrNotFound` | `<action>: <engine message> (hint: list_databases and list_collections show what exists; …)` |
| `ErrSchemaValidation` | `<action>: <engine message> (hint: see resource cachydb://docs/schema)` |
| `ErrConditionFailed` | `<action>: condition failed: document <id> no longer matches if; re-read it and retry` |
| `ErrExists`, `ErrInvalidName` | `<action>: <engine message>` |
| `ErrClosed` | `<action>: the database engine is shut down: …` |
| `ErrWALFailed`, `ErrWALCorrupt` | `<action>: write-ahead log failure, restart the server: …` |
| context cancelled / deadline | `<action>: request cancelled: …` |

## Write consistency

A tool call that returns success has its change fsynced to the WAL. It
survives a crash (`kill -9`, power loss) and is visible to every session.
Data segments are written by the background flusher; on restart, recovery
replays any WAL records not yet in the segments.
