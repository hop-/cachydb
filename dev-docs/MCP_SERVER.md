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
| HTTP listen host | `--host` flag, or `HOST` env var | `127.0.0.1` (this machine only; the Docker image sets `0.0.0.0`) |
| HTTP port | `-p` / `--port` flag, or `PORT` env var | `7601` |
| Authentication | `--auth` flag, or `AUTH` env var: `auto`, `required`, `off` (anything else is a start-up error) | `auto` |
| TLS | `--tls-cert` and `--tls-key` flags, or `TLS_CERT` and `TLS_KEY` env vars (PEM files; both or neither) | none: plain HTTP |
| Data directory | `-R` / `--root` flag, or `ROOT_DIR` env var | `~/.cachydb` |
| Default database | `-d` / `--db` flag, or `DB_NAME` env var | `main` |
| Bootstrap admin token | `BOOTSTRAP_ADMIN_TOKEN` or `BOOTSTRAP_ADMIN_TOKEN_FILE` env var (no flag) | none |

Precedence: flag > environment variable > default.

The listener settings travel as one `mcpserver.ListenConfig` (`Host`,
`Port`, `TLSCert`, `TLSKey`; `internal/mcp/listen.go`) from
`app.Builder.WithListen` to `mcpserver.New(ctx, engine, Config)`.
`NewServer(ctx, engine, db, transport, "host:port", version)` remains as a
shorthand for a server without TLS or tokens (tests use it).

### When tokens are required

`auth.Decide(mode, http, host)` (`internal/auth/policy.go`) applies the
start-up matrix; loopback means the host is `127.0.0.1`, `::1`, or
`localhost` (`auth.IsLoopback`):

| Transport | Listener | `auto` | `required` | `off` |
| --- | --- | --- | --- | --- |
| stdio | — | no tokens | no tokens | no tokens |
| HTTP | loopback host | no tokens | tokens required | no tokens |
| HTTP | any other host | tokens required | tokens required | no tokens, WARN at start |

`app.Builder.Build` validates the mode, the TLS pair, and the bootstrap
token (`auth.ValidateBootstrapToken`) before opening the engine.
`Builder.openUsers` then opens the `auth.Store` when tokens are required,
when a bootstrap token is set, or whenever `_system` already exists
(`Engine.HasSystemDatabase`), so `delete_database` forgets grants with
authentication off too; otherwise nothing creates `_system`. Failing to
open an existing `_system` fails the start-up in every mode. Next it
applies the bootstrap token, and when tokens are required and no user
exists it returns `app.ErrNoUsers` with a message naming
`cachydb utils user add`, `BOOTSTRAP_ADMIN_TOKEN`, and `AUTH=off`; the
process exits 1 and the default database is not created. `AUTH=off` on a
non-loopback host logs a WARN through the engine logger.

**Bootstrap token.** `cmd.runApp` reads `BOOTSTRAP_ADMIN_TOKEN` or the
file named by `BOOTSTRAP_ADMIN_TOKEN_FILE` (`app.BootstrapToken`: both set
is an error, white space is trimmed) and passes it to
`Builder.WithBootstrapAdminToken`. At every start, in every mode,
`Store.EnsureAdmin("admin", token)` makes sure the user exists, is a
server admin, and holds the token with label `bootstrap`; a different
earlier `bootstrap` token of `admin` is revoked first, so after the value
changes only the current one works. Removing the setting leaves the
stored token. A value that already is another user's token is refused.
The token must be 32 to 512 characters of RFC 6750 `b64token`
(`[A-Za-z0-9._~+/-]+=*`), so it need not start with `cdb_`
(`Store.Authenticate` accepts any form). Each change is logged at INFO
with actor `BOOTSTRAP_ADMIN_TOKEN`; the token never is.

With tokens required, `httpHandler()` wraps only `/mcp` in the SDK's
`auth.RequireBearerToken` (`AllowMissingExpiration`, tokens do not expire):
a request without a valid bearer token gets 401; the verifier
(`Server.verifyToken`) calls `Store.Authenticate` and returns
`TokenInfo{UserID: name}` for any user's token, or an error wrapping the
SDK's `ErrInvalidToken`. The SDK binds each session to the user that
created it and answers 403 when another user's token reuses its
`Mcp-Session-Id`. Failed authentications (no bearer token: logged by
`logMissingToken`, since the SDK refuses those without calling the
verifier; unknown or revoked token) are logged at INFO through
`Config.Logger` (the engine logger) with the remote address and the
reason, never the token. `/healthz` never needs a token.

### Tool access

Every tool has an access level (`internal/mcp/access.go`), passed to
`newSpec` in `toolSpecs`:

| Access | Tools |
| --- | --- |
| none | `list_databases` (lists only databases the principal can read), `current_database`, `use_database` (the target must be readable; checked before existence) |
| read | `list_collections`, `describe_collection`, `find_documents`, `get_document`, `count_documents`, `distinct`; `copy_collection` on its source; `server_stats` on its `database` argument (without one it checks no database and shows only readable ones) |
| write | `insert_document`, `insert_documents`, `update_document`, `update_documents`, `replace_document`, `delete_document`, `delete_documents` |
| admin | `create_collection`, `update_schema`, `rename_collection`, `delete_collection`, `create_index`, `drop_index`; `copy_collection` on its target (`target_database`, else the source database) |
| server | `create_database`, `delete_database` (which then calls `Store.ForgetDatabase` whenever the server has a store, also with auth off), and the six admin user tools |
| none (registered only with tokens) | `whoami` |

`newSpec` wraps every handler in `Server.authorizeCall`, the one place
tools are authorized:

1. **Principal** (`Server.principal`): with a `req.Extra.TokenInfo`, the
   user's current state from `Store.Principal` (a user removed meanwhile
   fails); with tokens not required (stdio, loopback HTTP, `AUTH=off`),
   `auth.FullAccess()`, which every check allows. An HTTP server that
   requires tokens refuses a call without `TokenInfo`.
2. **Database**: inputs with a `database` argument implement
   `databaseArg` (on the pointer); the wrapper resolves it (see below) and
   writes the resolved name back, so the handler acts on exactly the
   database that was checked. An input that also implements
   `allDatabasesArg` (`server_stats`) is the exception: an empty
   argument means every readable database, so it is not resolved and no
   database is checked; the handler filters by the principal. `copy_collection` also implements
   `extraAccess` for its target.
3. **Check**: `Store.Authorize` for each requirement (it re-reads the
   user's current grants, so a revoke applies to the next call). A denied
   call is a tool error wrapping `auth.ErrPermissionDenied` that names the
   user, the database, and the level.

`toolSpec.validate` rejects a tool without an access level, a database
level (read, write, admin) on an input without a `database` argument, and
a `database` argument on a none or server tool; `registerTools` then
fails and `New` returns the error. The handler of a tool without a level
is never reached (`authorizeCall` denies it).

### User tools

`internal/mcp/users.go` defines `accessToolSpecs`: `whoami` (access
none) and the admin tools `list_users`, `create_user`, `set_access`,
`create_token`, `revoke_token`, `delete_user` (access server, marked
`adminOnly`). `registerTools` validates them always but registers them
only when `Config.TokensRequired`, so a server with authentication off
lists exactly the 26 tools every server registers (the 21 from before plan 013 plus the 4 document tools and `server_stats` of stage 5). With tokens required, the
receiving middleware `filterToolsList` sets `cacheScope: "private"` on
every `tools/list` result (the list differs per user) and removes the
admin tools unless the request's principal (`principalOf`, the same
resolution as tool calls) is a server admin; the access level stays the
real guard. `ToolDefinitions`/`ToolDocs` (the `tools` topic) cover only
the always-registered tools; `AccessToolDocs` renders the user tools at
the end of the `access` topic.

- `whoami` → `user`, `server_admin`, `default_database`, `grants`
  (database → level name).
- `list_users` → `users` [`NewUserInfo`: name, flag, default database,
  grants, created, token labels; never a token or digest].
- `create_user` (`name`, `server_admin`?, `default_database`?) → `user`,
  `label` (`default`), `token` (shown only here).
- `set_access` (`user`, and `database` + `level` (`none` removes),
  `server_admin`?, `default_database`? (empty clears)) → `Store.SetAccess`,
  all parts in one compare-and-set write; returns the updated `user`.
- `create_token` (`user`, `label`?) → `label`, `token`; `revoke_token`
  (`user`, `label`); `delete_user` (`name`).

`auth.Store` refuses to delete or demote the only server admin
(`ErrLastServerAdmin`), for the MCP tools and `cachydb utils user` alike.
Every successful change is logged at INFO via `auth.LogChange` (actor =
the calling user, action = the tool name, target = the user changed, plus
database/level/label; never a token). `userToolError` adds a
`list_users` hint to not-found errors.

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
2025-03-26+) at `http://127.0.0.1:7601/mcp` (`--host` changes the host).
Multiple concurrent sessions are supported. Idle sessions are closed after
**30 minutes** (`httpSessionTimeout`). With both TLS files, `startHTTP`
loads the key pair before listening (a bad file fails the start-up) and
serves `http.Server.ServeTLS`. The start-up line on stderr names the
scheme, the address, and whether tokens are required, for example
`CachyDB MCP server listening on https://0.0.0.0:7601/mcp (Streamable HTTP
transport, tokens required)`.

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
   creates the default database if missing, registers the 26 tools (33
   with tokens required: plus `whoami` and the six admin tools) and the
   9 docs resources, and sets the server instructions and the
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
resolves its database in this order (`Server.databaseName`, applied by
`authorizeCall` before the access check):

1. the tool's `database` argument, if given;
2. the session's `use_database` choice;
3. the user's `default_database` (only with tokens: with auth off the
   principal is the implicit full-access one, which has none);
4. the server default (`main`).

## Tools (26, plus the user tools)

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
| `list_databases`, `list_collections`, `find_documents`, `get_document`, `current_database`, `count_documents`, `distinct`, `describe_collection`, `server_stats` | true | — | true |
| `create_database`, `use_database`, `replace_document`, `update_schema` | false | false | true |
| `create_collection`, `insert_document`, `insert_documents`, `create_index`, `update_document`, `update_documents`, `rename_collection`, `copy_collection` | false | false | false |
| `delete_document`, `delete_documents`, `delete_database`, `delete_collection`, `drop_index` | false | true | true |

| Tool | Arguments | Result fields |
|------|-----------|---------------|
| `create_database` | `name` | `message`. An existing name succeeds (no-op) |
| `list_databases` | — | `databases` the caller can read (sorted; `[]` when empty; every database with auth off) |
| `delete_database` | `name` | `message` |
| `use_database` | `name` (must exist) | `message`, `current_database` |
| `current_database` | — | `current_database` (session choice or default) |
| `create_collection` | `name`, `database`?, `schema`? | `message` |
| `list_collections` | `database`? | `collections` (sorted; `[]` when empty), `database` |
| `describe_collection` | `collection`, `database`? | `database`, `name`, `schema` (or null), `indexes` [`{name, field, unique?}`, sorted; `unique: true` only on unique indexes], `count` |
| `update_schema` | `collection`, `schema` (object or null), `database`? | `message` |
| `rename_collection` | `collection`, `new_name`, `database`? | `message` |
| `copy_collection` | `collection`, `new_name`, `target_database`?, `database`? | `message`, `copied` |
| `delete_collection` | `collection`, `database`? | `message` (`ErrNotFound` if missing) |
| `insert_document` | `collection`, `document`, `database`? | `id`, `message` |
| `insert_documents` | `collection`, `documents` (array), `database`? | `ids` (in order), `inserted` |
| `get_document` | `collection`, `id`, `fields`?, `database`? | `document` (flat, with `_id`) |
| `find_documents` | `collection`, `query`?, `explain`?, `database`? | `count`, `documents`, `has_more`, `next_skip` (when `has_more`), `plan` (with `explain`) |
| `count_documents` | `collection`, `query`? (`filters`, `any`), `database`? | `count` |
| `distinct` | `collection`, `field`, `query`? (`filters`, `any`), `database`? | `values` |
| `update_document` | `collection`, `id`, `updates`?, `unset`?, `inc`?, `push`?, `add_to_set`?, `pull`?, `if`?, `upsert`?, `database`? | `message`, `upserted` |
| `update_documents` | `collection`, `query`? (`filters`, `any`), `updates`?, `unset`?, `inc`?, `push`?, `add_to_set`?, `pull`?, `limit`?, `all`?, `database`? | `matched`, `modified` |
| `replace_document` | `collection`, `id`, `document`, `if`?, `database`? | `message` |
| `delete_document` | `collection`, `id`, `if`?, `database`? | `message` |
| `delete_documents` | `collection`, `query`? (`filters`, `any`), `limit`?, `all`?, `database`? | `matched`, `deleted` |
| `create_index` | `collection`, `index_name`, `field_name`, `unique`?, `database`? | `message` |
| `drop_index` | `collection`, `index_name`, `database`? | `message` (`ErrNotFound` if missing) |
| `server_stats` | `database`? | `databases` [`{name, documents, data_bytes, index_bytes, collections: [{name, documents, data_bytes, index_bytes, indexes}]}`], `server` (admins only) |

`?` = optional. Handlers are thin: each resolves the database (see
above) and calls one engine method (`UpdateWith`, `Replace`,
`DeleteWith`, `Get`, `Batch`, `UpdateMany`, `DeleteMany`, `CountDocuments`, `Distinct`, `Schema`/`Indexes`/`Count`,
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
  `has`, `has_any`, `has_all`, `exists`, `starts_with` (the `Operators`
  enum). All `filters` must match, and with `any` also every filter of at
  least one group. Fields are names, nested paths (`meta.author`), or
  `_id`; the same holds for `sort`, `fields`, `distinct`, `if`, and
  `create_index`.
- Type-aware comparison: numbers compare numerically, strings lexically;
  values of different types never match `eq/gt/gte/lt/lte`.
- A document **missing the field matches no operator, including `ne`**
  (unlike MongoDB's `$ne`), except `exists` with `false`.
- Results are sorted by `sort`, then `_id`; `skip`/`limit` page
  deterministically. An omitted or empty `query` matches every document.
- **Bounded results** (plan 013 §2): `limit` omitted or 0 means
  `Config.DefaultLimit` (`--default-limit`/`DEFAULT_LIMIT`, 100); a
  positive `limit` is used as given. `Config.MaxResultBytes`
  (`--max-result-bytes`/`MAX_RESULT_BYTES`, 1 MiB) always caps the JSON
  size of `documents` (each document marshalled once as it is added, plus
  brackets and commas); the first document is returned even when it alone
  is larger. The handler runs `Collection.Iterate` with `limit + 1` and
  stops on the limit or the cap, so it never loads every match;
  `has_more` is true when a further match exists, and `next_skip` is then
  `skip + count`. The engine's `Find`, `utils find`, and `client export`
  (which follows `has_more`/`next_skip`) are not bounded by these.
- Each document is returned as a flat object with `_id` (only the
  `fields` listed, plus `_id`, when `fields` is set).
- **`explain: true`** (plan 013 §5) runs the same iteration through
  `Collection.IterateWithPlan` and adds `plan` (`QueryPlan` in tools.go:
  `driver`, `index` (omitted when empty), `order_provided`, `examined`,
  `returned`); without it `plan` is omitted. `returned` is the page's
  `count`; `examined` comes from the engine and can include the
  look-ahead document.

Full semantics: [DATABASE_ENGINE.md — Queries](DATABASE_ENGINE.md#queries).

### `update_document`, `replace_document`, `delete_document`

`updates` is merged into the document (top-level fields are replaced or
added), then the `unset` fields are removed, then the update operators
apply (plan 013 §4; `UpdateSpec.Inc`, `Push`, `AddToSet`, `Pull`):
`inc` (`map[string]float64` in the input, so the SDK rejects a
non-number) adds to a number, a missing field starting at 0; `push`
appends, a missing field becoming a one-element array; `add_to_set`
appends unless an equal element (`eq` equality) exists; `pull` removes
every equal element, a missing field being a no-op. At least one of
`updates` (even `{}`), `unset`, and the operators is required
(`updateSpec` in server.go). A field in both `updates` and `unset` is a
schema-validation error as before; a field named by an operator and by
anything else, an operator on `_id`, or an operator meeting the wrong
type (`inc` on a non-number, the array operators on a non-array) is
`db.ErrInvalidUpdate`, which `toolError` hints at `get_document` and the
query topic. `_id` cannot be updated or unset. `upsert: true` creates a
missing document as `{_id: id} + updates`, then applies the operators
(`upserted: true` in the result). Everything runs in the write's build
callback under the document lock, so it combines atomically with `if`. `replace_document` replaces the whole
document atomically (its `_id`, if given, must equal `id`). The schema is
validated on the result. `if` (on all three) is a list of filters the
stored document must match, checked under the document lock just before
the commit; a mismatch (or a missing document) fails with
`condition failed: document <id> no longer matches if; re-read it and
retry` and writes nothing.

### `get_document`

Without `fields`, `Collection.Get`; with `fields`, a `Find` on
`_id eq id` with that projection (so fields, including nested paths,
mean what they mean in `find_documents`). A missing id is `ErrNotFound`
(`document "<id>" in collection "<c>": not found`).

### `insert_documents`, `update_documents`, `delete_documents`

Plan 013 §3, on the stage-3 engine calls:

- `insert_documents` builds one `db.OpInsert` per document (string `_id`
  handling shared with `insert_document`, `toDBDocument`) and calls
  `Collection.Batch`: one WAL record, all or nothing; `$now` resolves to
  one time for the call. A `*db.BatchItemError` becomes
  `insert documents: documents[<i>]: <item error>` (an existing id, an id
  repeated in the call, a schema or unique-index failure). An empty
  `documents` array inserts nothing (`ids: []`, `inserted: 0`). The
  engine's `MaxBatchDocs` (10,000) and `MaxBatchBytes` (32 MiB) apply;
  over HTTP the SDK's request body limit (`DefaultMaxRequestBodyBytes`,
  4 MiB) is lower, so larger loads are several calls.
- `update_documents` and `delete_documents` call `UpdateMany` and
  `DeleteMany`. Before that, `checkBatchQuery` rejects a negative `limit`
  and an empty query without `all: true` (no `query`, or no filters and
  no OR groups, or an empty OR group, which matches every document).
  More than `MaxBatchDocs` matches without a `limit` (or with a larger
  one) fail with the engine's `ErrBatchTooLarge` message, which states
  the match count, and write nothing. A document that stops matching
  between the snapshot and its write is skipped, so `modified`/`deleted`
  can be lower than `matched`. `update_documents` takes the same
  `updates`, `unset`, and operators as `update_document` (at least one)
  and has no `upsert`; one matching document of the wrong type for an
  operator fails the whole call. Repeating a
  limited update only makes progress when the update makes documents
  stop matching the query (the tool description says so); a limited
  delete always does.

`cachydb client import` writes through `insert_documents` (see
[CLI.md](CLI.md)).

### `create_index`, `drop_index`

Builds an index from the existing documents (array fields are indexed
per element; `field_name` may be a nested path). Writes to the whole
database wait while it builds. `eq`, `in`, ranges, `starts_with`,
`exists`, `has`, `has_any`, and `has_all` filters and sorts use indexes.
`unique: true` passes `db.Unique(true)`: a duplicate value fails with the
engine's `ErrUniqueViolation` (`unique index violation`), also when
existing documents already share a value; the message then says "Unique
index '<name>' created". `describe_collection` reports `unique: true` on
such an index (omitted otherwise, so the output for existing indexes is
unchanged), and `cachydb client export`/`import` carry it (F-016).

### `server_stats`

Plan 013 §7. With `database`, that database (read access checked by
`authorizeCall`; a missing one is `not_found`); without it, every
database the principal can read (`p.Can(name, read)`; all with auth off),
sorted, skipping one dropped meanwhile. Each comes from
`Database.Stats`: per collection `Collection.Stats` (document count,
`data_bytes` = the record bytes of its data segments from the index
file's accounting, which include superseded versions until compaction and
exclude changes still only in the WAL; `index_bytes` = the `index.db`
file size; index names), and per database the sums. `server` is set only
for a server admin or the implicit principal (auth off): `version`,
`uptime_seconds` (since `New`), `wal_bytes` (`Engine.ServerStats`: the WAL
segment files), and `cache` (`bytes`, `max_bytes`, `entries`, `hits`,
`misses`; the counters of `pkg/db/internal/cache`, since `Open`).
`cachydb utils stats` prints the same offline (see [CLI.md](CLI.md)).

## Self-description

- **Instructions**: `ServerOptions.Instructions` is `Instructions()` in
  `internal/mcp/docs.go`, a condensed guide (under 2,000 characters; a test
  checks it) with the typical workflow and the `cachydb://docs/<topic>`
  URIs of every topic in `docs.Names`.
- **Resources**: one `text/markdown` resource per docs topic at
  `cachydb://docs/<topic>` (name and title = the topic title, description
  = its summary). Reads return the same content as `cachydb docs <topic>`;
  the `tools` topic is rendered from the tool definitions
  (`DocsTopics`/`ToolDocs`), and the `access` topic ends with the
  generated user-tool reference (`AccessToolDocs`).
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
`query.filters`. These are the only tool errors without a code.

Handler errors are returned to the client as tool errors (`isError: true`)
through `toolError` (or `userToolError`), whose text is
`[<code>] <action>: <cause>[ (hint: …)]` (plan 013 §6, `internal/mcp/errors.go`).
`errorCode` maps the error with `errors.Is`, in the same order as the hints,
so the code and the hint agree; the returned `*codedError` wraps the
described error, so `errors.Is` still finds the engine sentinel. Errors a
handler finds itself (an empty batch query, a negative `limit`, nothing to
update, `set_access` argument mistakes) are `invalidArgf` errors, which
match `db.ErrInvalidArgument`. `ErrorCodes` lists the codes and
`ErrorCodeOf(text)` parses one (the test helper `call` checks every tool
error of the suite with it).

| Code | From |
| --- | --- |
| `not_found` | `ErrNotFound` |
| `already_exists` | `ErrExists` |
| `invalid_name` | `ErrInvalidName` |
| `invalid_argument` | `ErrInvalidArgument` (invalid query, sort key, `distinct` field, empty or too long id, malformed batch operation; handler argument checks), `ErrInvalidUpdate`, `auth.ErrInvalidLevel`, `auth.ErrLastServerAdmin` |
| `schema_violation` | `ErrSchemaValidation` |
| `unique_violation` | `ErrUniqueViolation` |
| `condition_failed` | `ErrConditionFailed` |
| `limit_exceeded` | `ErrBatchTooLarge` (the message keeps the limit or the match count) |
| `permission_denied` | `auth.ErrPermissionDenied`, `auth.ErrUnauthenticated` |
| `unavailable` | `ErrClosed`, `ErrWALFailed`, `ErrWALCorrupt`, `ErrQueryInterrupted` |
| `cancelled` | `context.Canceled`, `context.DeadlineExceeded` |
| `internal` | anything else |

A `*db.BatchItemError` takes the code of the error it wraps;
`insert_documents` names the item as `documents[<i>]`, and
`UpdateMany`/`DeleteMany` return the item's error, which names the document.

Message shapes after the code:

| Engine error | Message shape |
|--------------|---------------|
| `ErrNotFound` | `<action>: <engine message> (hint: list_databases and list_collections show what exists; …)` |
| `ErrSchemaValidation` | `<action>: <engine message> (hint: see resource cachydb://docs/schema)` |
| `ErrInvalidUpdate` | `<action>: <engine message> (hint: get_document shows the field's current value; …)` |
| `ErrConditionFailed` | `<action>: condition failed: document <id> no longer matches if; re-read it and retry` |
| `ErrExists`, `ErrInvalidName` | `<action>: <engine message>` |
| `ErrClosed` | `<action>: the database engine is shut down: …` |
| `ErrWALFailed`, `ErrWALCorrupt` | `<action>: write-ahead log failure, restart the server: …` |
| context cancelled / deadline | `<action>: request cancelled: …` |
| `auth.ErrPermissionDenied` | `<action>: permission denied: user "<name>" needs <level> access to database "<db>" (hint: ask a server admin for access)`; server tools: `… must be a server admin …` |
| `auth.ErrUnauthenticated` | `<action>: unauthenticated: … (hint: ask a server admin for a valid token)` (the token's user was removed) |
| `auth.ErrLastServerAdmin` (user tools) | `<action>: last server admin: cannot remove\|demote user "<name>", the only server admin; make another user a server admin first` |
| `ErrNotFound` (user tools) | `<action>: user "<name>": not found (hint: list_users shows the users and their token labels)` |

## Write consistency

A tool call that returns success has its change fsynced to the WAL. It
survives a crash (`kill -9`, power loss) and is visible to every session.
Data segments are written by the background flusher; on restart, recovery
replays any WAL records not yet in the segments.
