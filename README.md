# CachyDB

[![test](https://github.com/hop-/cachydb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/hop-/cachydb/actions/workflows/test.yml)
[![nightly](https://github.com/hop-/cachydb/actions/workflows/nightly.yml/badge.svg?branch=master)](https://github.com/hop-/cachydb/actions/workflows/nightly.yml)
[![docker](https://github.com/hop-/cachydb/actions/workflows/docker.yml/badge.svg)](https://github.com/hop-/cachydb/actions/workflows/docker.yml)
[![Docker Image Version](https://img.shields.io/docker/v/hovhannespalyan/cachydb?sort=semver&label=docker%20image)](https://hub.docker.com/r/hovhannespalyan/cachydb)
[![Docker Pulls](https://img.shields.io/docker/pulls/hovhannespalyan/cachydb)](https://hub.docker.com/r/hovhannespalyan/cachydb)
[![Latest tag](https://img.shields.io/github/v/tag/hop-/cachydb?sort=semver&label=release)](https://github.com/hop-/cachydb/tags)
[![Go Reference](https://pkg.go.dev/badge/github.com/hop-/cachydb.svg)](https://pkg.go.dev/github.com/hop-/cachydb/pkg/db)
[![Go version](https://img.shields.io/github/go-mod/go-version/hop-/cachydb)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A lightweight document-based database with Model Context Protocol (MCP) support, similar to MongoDB but designed for simplicity and AI integration.

## Features

- **Document-based storage**: Store JSON documents in collections
- **Multiple databases**: Create and manage multiple databases within a single instance
- **Schema validation**: Define and enforce schemas for your collections
- **Indexing**: Indexes on any field or dotted path, stored on disk (duplicate values supported, or `unique`; array fields indexed per element); they serve equality, `in`, ranges, `starts_with`, `exists`, array matching, and sorts
- **Query operations**: Type-aware filters (eq, ne, gt, gte, lt, lte, in), case-insensitive `contains`, array matching (`has`, `has_any`, `has_all`), `exists`, `starts_with`, nested paths, OR groups, sort by any field, projection, deterministic skip/limit, query plans (`explain`), `count_documents`, `distinct`
- **Safe writes**: Conditional writes (`if`, compare-and-set), `unset`, in-place operators (`inc`, `push`, `add_to_set`, `pull`), unique indexes, upsert, atomic `replace_document`, and `{"$now": true}` server timestamps
- **Batch writes**: `insert_documents`, `update_documents`, and `delete_documents` change many documents in one atomic call (all or none); `get_document` reads one by id
- **Schema evolution**: Every violation reported at once; `describe_collection`, `update_schema`, `rename_collection`, `copy_collection`, `delete_collection`, `drop_index`
- **Export and client**: JSON Lines export/import (`cachydb utils export|import`), offline `utils find|get`, an offline integrity check (`utils verify`, which can also rebuild index files), and `cachydb client` to call any tool of a running HTTP server from the shell
- **MCP integration**: Built-in MCP server supporting stdio and Streamable HTTP transports, with `use_database` per session
- **Users and access**: Users with tokens and `read` / `write` / `admin` access per database (plus server admins), required for HTTP that other machines can reach; HTTPS with `--tls-cert` / `--tls-key`; managed offline (`cachydb utils user`) or over MCP by server admins
- **Durable writes**: Every acknowledged write is fsynced to a write-ahead log; concurrent writes share fsyncs (group commit)
- **Append-only storage**: zstd-compressed, append-only data segments with background compaction; the key table and indexes live on disk in one index file per collection, so memory use does not grow with the number of documents (tested with 10 million)
- **Crash recovery**: Restart always recovers, including after `kill -9`, power loss, or a torn file tail
- **Embeddable**: `pkg/db` is a Go library (`db.Open`)

## Database Structure

- **Databases**: Multiple isolated databases within the instance
- **Collections**: Multiple collections within each database
- **Documents**: JSON documents with a string `_id` (generated UUID if not provided)
- **Schemas**: Optional field definitions with type validation
- **Indexes**: Indexes on any field or dotted path, optionally unique (`_id` lookups are always fast)

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

The server listens on `127.0.0.1:7601` (`http://localhost:7601/mcp`), this machine only, and implements the [MCP Streamable HTTP transport](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports). This is the recommended transport for networked or multi-client deployments.

Change the port with `--port`, the listen host with `--host`, and serve HTTPS with a certificate and key:

```bash
./cachydb --transport http --port 8080
./cachydb --transport http --host 0.0.0.0 --tls-cert cert.pem --tls-key key.pem
```

**Authentication** (`--auth` / `AUTH`): with the default `auto`, stdio and loopback HTTP (`127.0.0.1`, `::1`, `localhost`) need no token, and HTTP on any other host requires one. `required` requires tokens for all HTTP; `off` never does (on a non-loopback host it logs a warning). When tokens are required and no user exists, the server refuses to start and names the ways out: create the first admin with `cachydb utils user add NAME --admin` (server stopped), set `BOOTSTRAP_ADMIN_TOKEN`, or opt out with `AUTH=off` on a trusted network. `/healthz` never needs a token.

**Users and tokens.** A user has tokens, an optional server-admin flag, an optional default database, and an access level per database: `read`, `write` (plus document writes), or `admin` (plus collections, schemas, indexes); database `*` means every database. Only server admins create and delete databases and manage users. Clients send `Authorization: Bearer <token>`. A token is printed once when it is created; only its SHA-256 digest is stored.

```bash
cachydb utils user add admin --admin                    # server stopped; prints the token once
cachydb utils user add alice --default-database shop
cachydb utils user grant alice shop write
cachydb utils user list                                 # never shows tokens
BOOTSTRAP_ADMIN_TOKEN="$(openssl rand -hex 32)" cachydb -t http --host 0.0.0.0   # or: ensure "admin" holds this token at start
```

On a server that requires tokens every user gets the `whoami` tool, and server admins also get `list_users`, `create_user`, `set_access`, `create_token`, `revoke_token`, and `delete_user` (hidden from other users; a server with authentication off registers none of them). See `cachydb docs access`.

### Configuration

CLI flags (override environment variables, which override the defaults):

```none
  -t, --transport            MCP transport: stdio (default) or http
  -p, --port                 Port for HTTP transport (default 7601)
      --host                 HTTP listen host (default 127.0.0.1; 0.0.0.0 = every interface)
      --auth                 auto (default), required, or off
      --tls-cert, --tls-key  PEM certificate and key; with both, serve HTTPS
  -R, --root                 Data directory (default ~/.cachydb)
  -d, --db                   Default database, created at startup (default main)
      --wal-retention-age    Keep processed WAL for this long, e.g. 24h (0 = no age limit)
      --wal-retention-size   Keep at most this much processed WAL, e.g. 1GiB (0 = no size limit)
      --cache-size           Document cache size, e.g. 64MiB (0 = cache disabled)
      --default-limit        Documents find_documents returns without a limit (default 100)
      --max-result-bytes     Cap on the JSON size of one find_documents result (default 1MiB)
      --result-text          Text content of tool results: full (default; the JSON) or summary (one line)
```

Environment variables:

- `TRANSPORT`: `stdio` or `http` (default: `stdio`)
- `PORT`: Port for the HTTP transport (default: `7601`)
- `HOST`: HTTP listen host (default: `127.0.0.1`)
- `AUTH`: `auto`, `required`, or `off` (default: `auto`); any other value is a start-up error
- `TLS_CERT`, `TLS_KEY`: PEM certificate and key files; both or neither
- `BOOTSTRAP_ADMIN_TOKEN` (or `BOOTSTRAP_ADMIN_TOKEN_FILE`, a file holding it): at every start, make sure the server admin `admin` exists and holds this token; a changed value replaces the old token; never logged
- `ROOT_DIR`: Data directory (default: `~/.cachydb`)
- `DB_NAME`: Default database (default: `main`)
- `WAL_RETENTION_AGE`: How long processed WAL segments are kept, as a Go duration (default: `24h`, `0` = no age limit)
- `WAL_RETENTION_SIZE`: Maximum size of processed WAL kept (default: `1GiB`, `0` = no size limit)
- `CACHE_SIZE`: Document cache size (default: `64MiB`, `0` = disabled)
- `DEFAULT_LIMIT`: Documents `find_documents` returns at most when a call gives no `limit` (default: `100`); results report `has_more` and `next_skip` for paging
- `MAX_RESULT_BYTES`: Cap on the JSON size of the documents one `find_documents` call returns; at least one document is returned (default: `1MiB`)
- `RESULT_TEXT`: Text content of successful tool results: `full` (the result JSON, the same as the structured content) or `summary` (one line such as `12 documents, has_more: false`; the data is then only in the structured content, so use it only with a client that gives its model the structured content) (default: `full`)

Sizes accept `B`, `KiB`, `MiB`, `GiB`, `TiB` (powers of 1024) and `KB`, `MB`, `GB`, `TB` (powers of 1000). Retention never deletes WAL that has not yet been written to the data segments.

Run `cachydb docs config` for every setting, or `cachydb --help`.

Only one process can use a data directory at a time: the server holds an exclusive lock on `<data dir>/cachydb.lock`, and a second server or a `cachydb utils` command on the same directory fails with "data directory is in use by another cachydb process".

### Export, import, and the shell client

```bash
# Offline (no server running on the directory)
cachydb utils export -d kb -o kb.jsonl                 # JSON Lines: header, databases, collections, documents
cachydb utils import -i kb.jsonl --on-conflict skip -R /new/data
cachydb utils import -i seed.jsonl --on-conflict keep  # seed: create only what is missing; safe to re-run
cachydb utils find -d kb -c notes --query '{"filters":[{"field":"tags","operator":"has","value":"wal"}]}'
cachydb utils get -d kb -c notes --id wal-design
cachydb utils verify --deep                            # integrity check of the data directory; exit 1 on damage
cachydb utils verify --rebuild-index -d kb -c notes    # rebuild a damaged or stale index.db from the data segments

# Through a running HTTP server (--url or CACHYDB_URL, default http://127.0.0.1:7601/mcp;
# --token or CACHYDB_TOKEN when the server requires tokens)
cachydb client tools
cachydb client call find_documents '{"database":"kb","collection":"notes","query":{"limit":5}}'
cachydb client export -d kb -o kb.jsonl
cachydb client import -i kb.jsonl
```

`--on-conflict` decides what happens to existing data: `fail` (default) stops at an existing `_id`, `skip` keeps existing documents, `replace` overwrites them with the file's copies and updates differing schemas and indexes, and `keep` creates only what is missing and leaves existing collections (schema, indexes, documents) as they are. `client call` works for every tool, so scripts and agents without MCP support can use cachydb too. See `cachydb docs config` for the export format and flags.

Logs go to stderr; in stdio mode stdout carries only MCP messages. SIGINT/SIGTERM, or the client closing stdin, shut the server down gracefully (final flush, exit status 0). Errors exit with status 1.

### Built-in documentation for AI agents

The binary documents itself, so an agent does not need this repository:

```bash
cachydb docs                         # topics: overview, quickstart, mcp, tools, query, schema, config, access, errors
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

The HTTP endpoint implements the MCP Streamable HTTP transport — clients POST JSON-RPC messages to `/mcp` and receive responses via SSE. Idle sessions expire after 30 minutes. By default it listens on loopback only; on another host it requires tokens (see [Authentication](#http-streamable-http-transport) above) unless `AUTH=off`, and serves HTTPS with `--tls-cert` and `--tls-key`.

The HTTP server also answers `GET /healthz`: `200 {"status":"ok","version":"..."}` while the database is open, `503` while it shuts down. `cachydb healthcheck` probes it and exits 0 (healthy) or 1, for container and service-manager healthchecks:

```bash
TRANSPORT=http cachydb healthcheck                    # http://127.0.0.1:$PORT/healthz (with TRANSPORT=stdio it exits 0 without probing)
TRANSPORT=http TLS_CERT=c.pem TLS_KEY=k.pem cachydb healthcheck   # https://127.0.0.1:$PORT/healthz, certificate not verified on loopback
cachydb healthcheck --url http://db.internal:7601/healthz
```

## Docker

The official image [`hovhannespalyan/cachydb`](https://hub.docker.com/r/hovhannespalyan/cachydb) is a small distroless image for `linux/amd64` and `linux/arm64`. It runs as the non-root user `65532`, keeps data in the `/data` volume, and is configured with the [environment variables](#configuration). Its defaults are `TRANSPORT=http`, `HOST=0.0.0.0`, `PORT=7601`, `ROOT_DIR=/data`, `DB_NAME=main`. Tags: `latest`, `X.Y`, `X.Y.Z`.

Because the container listens on every interface, clients need a token under the default `AUTH=auto`, and **the container refuses to start until a user exists** (`BOOTSTRAP_ADMIN_TOKEN`, or `cachydb utils user add` on the stopped volume). For local use without tokens, publish the port on localhost and set `AUTH=off`:

```bash
# HTTP server with tokens: the server admin "admin" holds the token you choose
docker run -d --name cachydb -p 7601:7601 -v cachydb-data:/data -e BOOTSTRAP_ADMIN_TOKEN="$TOKEN" hovhannespalyan/cachydb

# HTTP server at http://localhost:7601/mcp, data in the cachydb-data volume, no tokens
docker run -d --name cachydb -p 127.0.0.1:7601:7601 -v cachydb-data:/data -e AUTH=off hovhannespalyan/cachydb

# stdio, started by an MCP client (keep -i, no -t)
docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb

# other commands
docker run --rm hovhannespalyan/cachydb version --json
```

With Docker Compose, the repository's [`docker-compose.yml`](docker-compose.yml) runs the HTTP server with a named volume, published on localhost with `AUTH=off`: `docker compose up -d`.

For an agent, point the client at the HTTP container, or let it start a stdio container:

```bash
claude mcp add --transport http cachydb http://localhost:7601/mcp
claude mcp add --transport stdio cachydb -- docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb
```

Notes:

- **Volume permissions**: named volumes work as they are. A bind mount keeps the host directory's owner, so `chown 65532:65532` it first, or run with `--user "$(id -u):$(id -g)"`.
- **One container per volume**: the data-directory lock allows one cachydb process per volume. Share data between agents through one HTTP container.
- **Healthcheck**: the image's `HEALTHCHECK` runs `cachydb healthcheck`. It reads `PORT`, `TRANSPORT`, `TLS_CERT` and `TLS_KEY` from the environment, so set those as environment variables rather than flags.
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

List the databases you can read (every database when the server requires no tokens).

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

#### describe_collection

Return a collection's schema (or `null`), its indexes, and its document count.

```json
{"collection": "users"}
```

#### update_schema

Replace a collection's schema (`null` removes it). Every existing document is checked first; if any fails, nothing changes and the error lists up to 10 failing ids.

```json
{"collection": "users", "schema": {"fields": {"name": {"type": "string", "required": true}}}}
```

#### rename_collection / copy_collection / delete_collection

```json
{"collection": "users", "new_name": "people"}
{"collection": "people", "new_name": "people_backup", "target_database": "backups"}
{"collection": "people_backup", "database": "backups"}
```

Rename keeps documents, schema, and indexes (crash-safe). Copy creates the target with the same schema and indexes and copies every document; it is **not atomic** (delete a partial copy and retry). Delete removes a collection with its documents and indexes.

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

If `_id` is not provided, a UUID is generated. A provided `_id` must be a string. Integer values are stored as JSON numbers (Go library callers get them back as `float64`). A value `{"$now": true}`, at any depth, is replaced by the server time (RFC 3339 UTC); it also works as a filter value.

#### insert_documents / get_document

`insert_documents` inserts many documents in one atomic call: all of them, or none if any fails (the error names the item as `documents[i]`). `_id` and `$now` work as in `insert_document`; the result has `ids` (in order) and `inserted`. At most 10,000 documents and 32 MiB per call, and over HTTP a request is limited to 4 MiB, so send larger loads as several calls. `get_document` returns one document by `_id`, optionally only some `fields`.

```json
{"collection": "users", "documents": [{"_id": "u2", "name": "Ann"}, {"name": "Bo"}]}
{"collection": "users", "id": "u2", "fields": ["name"]}
```

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
    "any": [
      [{"field": "tags", "operator": "has", "value": "admin"}],
      [{"field": "name", "operator": "contains", "value": "doe"}]
    ],
    "sort": [{"field": "age", "order": "desc"}],
    "fields": ["name", "email"],
    "limit": 10,
    "skip": 0
  }
}
```

**Operators**: `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `in` (`in` takes an array of values), `contains` (case-insensitive substring of a string, or of a string element of an array), `has` (an array element equals the value), `has_any` / `has_all` (some / every value of an array is an element), `exists` (`true`: the field is present; `false`: missing), `starts_with` (case-sensitive prefix)

- All `filters` must match; with `any`, at least one group must match too (OR of ANDs). `field` is a field name, a nested path (`meta.author`), or `_id`.
- Comparison is type-aware: numbers compare numerically, strings lexically. Values of different types never match `eq`, `gt`, `gte`, `lt`, or `lte`. `eq`/`in` compare arrays as whole values; use `has` for elements.
- A document that lacks the filtered field matches no operator, including `ne` (unlike MongoDB's `$ne`), except `exists` with `false`.
- `"explain": true` next to `query` adds a `plan` (driver, index, whether it gave the order, documents examined and returned).
- Results are sorted by `sort` (any fields, `asc`/`desc`; missing < null < booleans < numbers < strings < arrays < objects), then `_id`; `skip` and `limit` page deterministically. `fields` returns only the listed fields plus `_id`. An omitted or empty `query` matches all documents. Without a `limit` a call returns at most 100 documents (`DEFAULT_LIMIT`), and every result stays within 1 MiB of JSON (`MAX_RESULT_BYTES`, at least one document); when `has_more` is `true`, repeat the call with `skip` set to `next_skip`.

#### count_documents / distinct

```json
{"collection": "users", "query": {"filters": [{"field": "tags", "operator": "has", "value": "admin"}]}}
{"collection": "users", "field": "tags"}
```

`count_documents` returns `{"count": n}`; `distinct` returns the distinct values of a field (array elements counted individually), sorted.

#### update_document

Update a document by ID. Top-level fields in `updates` are replaced or added, fields in `unset` are removed, other fields are kept. `_id` cannot be updated. `inc` (add to a number), `push` / `add_to_set` (append to an array), and `pull` (remove equal elements) change fields in place, after `updates` and `unset`; a field may appear in only one of them, and an operator on a field of the wrong type fails without writing. `if` makes the write conditional on the stored document (compare-and-set); `upsert: true` creates a missing document.

```json
{
  "database": "users_db",
  "collection": "users",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "updates": {
    "age": 31,
    "version": 4,
    "updated": {"$now": true}
  },
  "unset": ["nickname"],
  "if": [{"field": "version", "operator": "eq", "value": 3}]
}
```

If the condition no longer matches, nothing is written and the error says `condition failed: … re-read it and retry`.

#### replace_document

Atomically replace a whole document (fields not in `document` are removed). Takes the same optional `if`.

```json
{"collection": "users", "id": "u1", "document": {"name": "Jane", "email": "jane@example.com"}}
```

#### delete_document

Delete a document by ID, optionally only `if` it matches filters.

```json
{
  "database": "users_db",
  "collection": "users",
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### update_documents / delete_documents

Update (`updates`, `unset`, `inc`, `push`, `add_to_set`, `pull`) or delete every document matching `query` (`filters`, `any`), atomically: all matching documents change, or none. The result has `matched` and `modified` (or `deleted`). An empty query is rejected unless `"all": true`. More than 10,000 matches without a `limit` fail with the match count and change nothing; with `limit`, the first matches by `_id` change, and repeating the call makes progress only when the change makes documents stop matching (always for a delete).

```json
{"collection": "users", "query": {"filters": [{"field": "age", "operator": "lt", "value": 18}]}, "updates": {"minor": true}}
{"collection": "users", "query": {"filters": [{"field": "status", "operator": "eq", "value": "deleted"}]}, "limit": 1000}
```

#### server_stats

Per collection the document count, data bytes, index bytes, and index names, with per-database totals, for one `database` or every database you can read. Server admins (everyone without tokens) also get the WAL size, cache size, hits and misses, version, and uptime. `cachydb utils stats [-d DB] [--format json]` prints the same offline.

```json
{"database": "users_db"}
```

#### Errors

Every tool error starts with a stable code in brackets, such as `[not_found]`, `[invalid_argument]`, `[unique_violation]`, `[limit_exceeded]`, or `[permission_denied]`, followed by the message and a hint. Only arguments that fail the input schema come back as `validating "arguments": …` without a code. `cachydb docs errors` lists every code.

### Index Management

#### create_index

Create an index on a field or nested path for faster `eq`, `in`, range, `starts_with`, and `exists` filters and sorts, and `has` / `has_any` / `has_all` on array fields (each element is indexed). Indexes allow duplicate values unless `"unique": true`, which makes a write that repeats another document's value fail. Building an index blocks writes to that database until it finishes. `drop_index` (`{"collection", "index_name"}`) removes one.

```json
{
  "database": "users_db",
  "collection": "users",
  "index_name": "email_idx",
  "field_name": "email"
}
```

### Users and access (only when the server requires tokens)

`whoami` (every user) shows the caller's access. Server admins also get `list_users`, `create_user` (returns the first token once), `set_access` (a level on one database, `none` to remove it, the server-admin flag, the default database), `create_token`, `revoke_token`, and `delete_user`; the only server admin cannot be removed or demoted. `cachydb docs access` has their schemas.

```json
{"name": "create_user", "arguments": {"name": "alice", "default_database": "shop"}}
{"name": "set_access", "arguments": {"user": "alice", "database": "shop", "level": "write"}}
{"name": "whoami", "arguments": {}}
```

## Architecture

```none
cachydb/
├── main.go                 # Entry point
├── internal/
│   ├── app/               # Application setup (opens the engine, starts the MCP server)
│   ├── cmd/               # CLI commands (server, docs, version, utils list/stats/migrate/export/import/find/get/verify, client)
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

- **Append-only**: a background flusher (every second, or when 16 MiB of changes accumulate) appends only the changed documents and deletion tombstones to the collection's active segment with one fsync, then updates the collection's index file in one atomic transaction. A one-document change never rewrites a collection. Up to four collections are flushed in parallel.
- **Backpressure**: when the disk is slower than the writers, writes wait while the unflushed changes of all collections exceed `MaxPendingBytes` (by default 4 × the 16 MiB memtable size × half the CPUs, at most 1 GiB), so memory stays bounded
- **Compression**: documents are compressed with zstd
- **Index file**: each collection's `index.db` (a B+tree file, [bbolt](https://github.com/etcd-io/bbolt)) holds the key table (document id → location on disk), every secondary index, the document count, and the checkpoint; the flusher changes them together in one transaction. Documents are read on demand through an LRU cache of decoded documents (`CACHE_SIZE`, default 64 MiB), so a cached read skips the disk and the JSON decoding
- **Compaction**: a background task rewrites sealed segments with many dead records (superseded or deleted versions) to reclaim space, and merges runs of small segments
- **Self-repairing index**: a missing or damaged `index.db` is rebuilt from the data segments on startup (logged as a warning)

### Recovery

On startup CachyDB checks the retained WAL and opens each collection's index file without reading the data segments, so startup time depends on the WAL size (bounded by `WAL_RETENTION_SIZE`), not on the number of documents; then it replays the WAL from each collection's last checkpoint (and, for metadata, from the `cachydb.metacp` checkpoint). Replay is idempotent, so a crash at any point — including during a flush or compaction — loses no acknowledged write. A clean shutdown flushes everything, so the next start has nothing to re-apply.

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
        ├── index.db                # Key table and indexes (B+tree file)
        ├── seg-000001.data         # Sealed data segment (zstd records)
        └── seg-000002.data         # Active data segment
```

The files are binary (except the small JSON metadata files). See [`dev-docs/STORAGE.md`](dev-docs/STORAGE.md) for the formats.

## Upgrading from Older Versions

### Upgrading a v0.0.4–v0.0.6 data directory

Data directories written by CachyDB v0.0.4 to v0.0.6 (on-disk format 2) are upgraded automatically, in place, the first time this version opens them: each collection gets its `index.db`, built from its data segments, and the old hint files (`seg-*.hint`) and index snapshots (`idx-*.snap`) are removed. The log shows one line per collection (`upgrading data directory format 2 -> 3; building index`); on a large collection this takes a while once. If the process stops during the upgrade, the next start runs it again from the beginning.

The upgrade is **one-way**: older versions cannot open the directory afterwards. Before the first start, stop the server and keep a copy of the data directory, or export it:

```bash
cp -a ~/.cachydb ~/.cachydb.before-upgrade         # a copy of the whole directory, or
cachydb utils export -o backup.jsonl               # a JSON Lines export (run it with the old version)
```

### Directories older than v0.0.4

This storage engine is **not compatible** with data directories created by earlier CachyDB versions (JSON or `collection.data`/`collection.idx` files, `wal.checkpoint`). Opening one fails with:

```none
data directory <dir> was created by an older cachydb format; move or delete it
```

Move the old directory aside (or delete it) and start with an empty one. There is no automatic converter.

### Schema Migrations

Databases carry a schema version. `cachydb utils` commands open the data directory directly, so stop the server first:

```bash
./cachydb utils list -c                  # databases, schema versions, collections and document counts
./cachydb utils stats                    # document counts, data and index bytes, WAL and cache figures
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

- **Go 1.27+** - Programming language
- **Official MCP Go SDK** - [`github.com/modelcontextprotocol/go-sdk`](https://github.com/modelcontextprotocol/go-sdk)
- Type-safe tool handlers with automatic JSON schema generation
- **zstd compression** - [`github.com/klauspost/compress`](https://github.com/klauspost/compress)
- **bbolt** - [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt), the B+tree file `index.db` that holds each collection's key table and indexes
- **Cobra** - [`github.com/spf13/cobra`](https://github.com/spf13/cobra), the command line
- **stdio transport** — for local MCP clients
- **Streamable HTTP transport** — MCP spec 2025-03-26+, SSE-based, suitable for networked deployments
