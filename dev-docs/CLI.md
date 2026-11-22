# CLI Reference

CachyDB uses [cobra](https://github.com/spf13/cobra) (`internal/cmd/`).
Every command uses `RunE`; errors are printed once to stderr as
`Error: <message>` and the process **exits with status 1**. SIGINT and
SIGTERM cancel the command context, which stops the server and closes the
engine with a final flush. Overview: [PROJECT.md](PROJECT.md).

## Commands

### `cachydb` (root) and `cachydb app`

Open the engine and run the MCP server. `app` is the same command under an
explicit name.

```bash
cachydb [flags]
cachydb app [flags]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--transport` | `-t` | `stdio` (or `TRANSPORT`) | `stdio` or `http`; any other value fails |
| `--db` | `-d` | `main` (or `DB_NAME`) | Default database; created at startup if missing |
| `--port` | `-p` | `7601` (or `PORT`) | HTTP port (path `/mcp`) |
| `--host` | | `127.0.0.1` (or `HOST`) | HTTP listen host; `0.0.0.0` or `::` = every interface |
| `--auth` | | `auto` (or `AUTH`) | `auto`, `required`, or `off`; any other value fails |
| `--tls-cert` | | none (or `TLS_CERT`) | PEM certificate; with `--tls-key`, serve HTTPS |
| `--tls-key` | | none (or `TLS_KEY`) | PEM private key; with `--tls-cert`, serve HTTPS |
| `--root` | `-R` | `~/.cachydb` (or `ROOT_DIR`) | Data directory |
| `--wal-retention-age` | | `24h` (or `WAL_RETENTION_AGE`) | Keep processed WAL segments this long; Go duration; `0` = no age limit |
| `--wal-retention-size` | | `1GiB` (or `WAL_RETENTION_SIZE`) | Keep at most this much processed WAL; `0` = no size limit |
| `--cache-size` | | `64MiB` (or `CACHE_SIZE`) | Document cache size; `0` = cache disabled |
| `--default-limit` | | `100` (or `DEFAULT_LIMIT`) | Documents `find_documents` returns at most when a call gives no `limit`; must be > 0 (server only) |
| `--max-result-bytes` | | `1MiB` (or `MAX_RESULT_BYTES`) | Cap on the JSON size of one `find_documents` result's documents, at least one document returned; must be > 0 (server only) |
| `--result-text` | | `full` (or `RESULT_TEXT`) | Text content of successful tool results: `full` (the JSON) or `summary` (one line; data only in structured content); any other value fails (server only) |

The server takes the data-directory lock; a second server (or a `utils`
command) on the same directory fails with *"data directory is in use by
another cachydb process"*.

Unknown transports fail at startup (`invalid transport "x": use stdio or
http`), and so do an unknown `--auth` value and one TLS file without the
other (checked by `app.Builder.Build` before the engine opens).

Tokens are required for HTTP under `--auth required`, and under `auto`
when the host is not loopback (`127.0.0.1`, `::1`, `localhost`); stdio
never needs one (matrix: [MCP_SERVER.md](MCP_SERVER.md#when-tokens-are-required)).
When tokens are required and no user exists the server exits 1 with
*"refusing to start: ... no user exists. Create the first admin with
"cachydb utils user add NAME --admin" ..., or set BOOTSTRAP_ADMIN_TOKEN
...; or ... AUTH=off (--auth off)"*. `--auth off` on a non-loopback host
starts and logs a WARN.

`BOOTSTRAP_ADMIN_TOKEN`, or `BOOTSTRAP_ADMIN_TOKEN_FILE` naming a file
that holds it (env only, no flag; both set is an error), makes every
start ensure the server admin `admin` holds that token (label
`bootstrap`; a changed value replaces the old token; 32 to 512 `b64token`
characters). It is applied in every mode and never logged; see
[MCP_SERVER.md](MCP_SERVER.md#when-tokens-are-required).

### `cachydb docs`

Prints the built-in, agent-facing documentation (`internal/docs`). It never
opens the engine, so it works while a server runs.

```bash
cachydb docs                         # topic index: name — summary
cachydb docs <topic>                 # one topic (markdown)
cachydb docs --all                   # every topic, in order
cachydb docs <topic> --format json   # {"name","title","summary","content"}
cachydb docs --format json           # index: [{"name","title","summary"}]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--all` | `-a` | `false` | Print every topic (with `--format json`: an array of topic objects) |
| `--format` | `-f` | `md` | `md` or `json` |

Topics, in order: `overview`, `quickstart`, `mcp`, `tools`, `query`,
`schema`, `config`, `access`, `errors`. The `tools` topic is generated
from `mcpserver.ToolDefinitions()`, and the `access` topic ends with the
generated reference of the user tools (`mcpserver.AccessToolDocs()`,
markdown only); in JSON the `tools` topic adds
`"tools":[{"name","description","annotations","inputSchema","outputSchema","example"}]`.
An unknown topic exits 1 with the topic list on stderr and nothing on
stdout. The same topics are MCP resources `cachydb://docs/<topic>` (see
[MCP_SERVER.md](MCP_SERVER.md#self-description)).

### `cachydb version`

Prints `cachydb version <v>`. Resolution order: build-time `-ldflags`
(`cmd.Version`), then the module version from `debug.ReadBuildInfo()`, then
`v0.0.0-dev`. The MCP server reports the same version in its
implementation info.

`cachydb version --json` prints:

```json
{"version": "v0.0.3", "go": "go1.26.4", "storageFormat": 2, "walFormat": 1, "mcpProtocol": "2026-07-28"}
```

`storageFormat` is `db.FormatVersion` (`cachydb.format`), `walFormat` is
`db.WALFormatVersion`, and `mcpProtocol` is the newest protocol version the
MCP SDK supports (`mcp.SupportedProtocolVersions()[0]`).

Every command's `--help` has an `Examples` section.

### `cachydb healthcheck`

Probes the `/healthz` endpoint of a running HTTP server
(`internal/cmd/healthcheck.go`), like `pg_isready`. The official Docker
image uses it as its `HEALTHCHECK` (distroless has no shell or curl). It
never opens the engine or takes the data-directory lock.

```bash
TRANSPORT=http cachydb healthcheck                    # GET http://127.0.0.1:$PORT/healthz
TRANSPORT=http TLS_CERT=c.pem TLS_KEY=k.pem cachydb healthcheck   # GET https://127.0.0.1:$PORT/healthz
cachydb healthcheck --url http://db.internal:7601/healthz
cachydb healthcheck                                   # TRANSPORT unset (stdio): exits 0, nothing to probe
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--url` | | `http://127.0.0.1:<port>/healthz` | Endpoint to probe; an explicit URL is always probed |
| `--port` | `-p` | `7601` (or `PORT`) | Port for the default URL |
| `--transport` | `-t` | `stdio` (or `TRANSPORT`) | Without `--url`, `stdio` means nothing to probe |
| `--tls-cert`, `--tls-key` | | none (or `TLS_CERT`, `TLS_KEY`) | With both set, the default URL is `https://`; the files are not read |

An `https://` URL on a loopback host is probed without certificate
verification (`healthClient`): the certificate names the public host and
is often self-signed, and the probe sends nothing secret. `/healthz` never
needs a token.

- **Exit 0**: the endpoint answered `200`. Nothing is written.
- **Exit 1**: connection error, another status (`503` while the server
  shuts down), or no answer within 3 seconds (`healthcheckTimeout`); the
  reason goes to stderr as `Error: healthcheck: ...`. An invalid transport
  also exits 1.
- **stdio**: without `--url`, if the transport is `stdio` there is no HTTP
  server, so it prints `healthcheck: transport is stdio; nothing to probe`
  to stderr and exits 0 (a stdio container is not marked unhealthy).

### `cachydb utils`

Parent of the maintenance commands. Its persistent flags are inherited by
every subcommand: `-R/--root`, `--wal-retention-age`,
`--wal-retention-size`, `--cache-size` (same meaning as above).

`utils` commands **open the engine**: they take the data-directory lock and
run recovery (WAL replay), then close the engine with a flush. They
therefore **fail while a server is running** on the same directory. Stop
the server first, or use `cachydb client` against it: the data commands
(`export`, `import`, `find`, `get`, and `stats`) add a hint naming the `cachydb client`
command to the data-directory-in-use error (`clientHint` in `export.go`).
`utils verify` is the exception: it takes the lock but reads the files
without running recovery (`db.Verify`), and has no client equivalent.

### `cachydb utils list`

List databases with their schema versions.

```bash
cachydb utils list [-c] [-R DIR]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--collections` | `-c` | Also list each database's collections with their document counts |

Without `-c` only databases are shown. Example output with `-c`:

```
Found 2 database(s):

  main (schema version: 1)
    └─ users (2 documents)
  other (schema version: 1)
    └─ (no collections)
```

### `cachydb utils stats` (`stats.go`)

The offline twin of the `server_stats` MCP tool (plan 013 §7).

```bash
cachydb utils stats [-d DB] [--format text|json] [-R DIR]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--database` | `-d` | Only this database (a missing one fails with not found) |
| `--format` | `-f` | `text` (default) or `json` |

For every database (sorted; `_system` is not listed): document count,
data bytes, and index bytes, then per collection the same plus the index
names (`Database.Stats`, `Collection.Stats`); then a server line with the
build version, the WAL size, and the document cache (`Engine.ServerStats`:
size, limit, entries, hits, misses; a freshly opened engine has an empty
cache). `--format json` prints `{"databases": [...], "server": {"version",
"wal_bytes", "cache"}}` with the keys of `server_stats`
(`mcpserver.NewDatabaseStats`/`NewCacheTotal`), without `uptime_seconds`.
The data-directory-in-use error carries a hint naming
`cachydb client call server_stats`. `utils list -c` is unchanged.

```
main: 1 collection(s), 2 documents, data 224 bytes, index 32768 bytes
  └─ users: 2 documents, data 224 bytes, index 32768 bytes, indexes: by_name

server: version v0.1.0, WAL 331 bytes, cache 0 of 67108864 bytes (0 documents), 0 hits, 0 misses
```

### `cachydb utils migrate`

Run schema-version migrations (see
[DATABASE_ENGINE.md — Migrations](DATABASE_ENGINE.md#migrations)).

```bash
cachydb utils migrate (--database NAME | --all) [--target N]
cachydb utils migrate --show-version --database NAME
cachydb utils migrate --list
```

| Flag | Short | Description |
|------|-------|-------------|
| `--database` | `-d` | Migrate one database |
| `--all` | `-a` | Migrate every database |
| `--target` | `-t` | Target schema version; **defaults to the current schema version** (`db.CurrentSchemaVersion`, 1) |
| `--show-version` | `-v` | Print a database's schema version (needs `--database`) |
| `--list` | `-l` | List registered migrations (does not open the engine) |

Rules: exactly one of `--database` / `--all`; `--target` must be ≥ 1, and
a target above the current version needs a registered migration for every
step. Downgrades fail. No migrations are registered in this build.

### `cachydb utils export` / `import` (`export.go`, `import.go`)

```
cachydb utils export [-d DB [-c COLL]] [-o FILE]
cachydb utils import -i FILE|- [--on-conflict fail|skip|replace|keep]
```

JSON Lines in the format of `internal/exportfmt` (header with `version`
1, then database, collection, and document lines, all sorted; see `cachydb
docs config`). `-c` requires `-d`; export writes to stdout without `-o`.
Import creates missing databases and collections (schema and indexes,
unique ones as unique: the export's index lines carry `unique`, and
`client export`/`client import` read and pass it through
`describe_collection` and `create_index`);
`fail` (default) stops at the first existing `_id` and reports how many
lines were applied, `skip` keeps existing documents, `replace` overwrites
them with the file's copies and also updates a differing schema (via
`UpdateSchema`) and adds missing indexes (an index of the same name whose
field or uniqueness differs is dropped and re-created). With `fail`/`skip`
a collection whose schema or index set (names, fields, and uniqueness)
differs is an error, and its hint names `keep` and warns that `replace`
overwrites documents. `keep` (seeding, FR-23; `exportfmt.Keep`) creates
only what is missing: an existing collection's line is accepted without
comparing its schema and indexes, which stay as they are, and
documents use `skip` semantics (`OnConflict.documentMode`), so existing
ones are kept and missing ones inserted; re-running a seed exits 0. A
missing document that the existing schema rejects still fails. Offline import writes the document lines of a
collection in atomic batches of up to 1,000 documents or 4 MiB of lines
(`exportfmt.BatchTarget`, one WAL fsync each); a batch that fails is
applied not at all, so the reported count of applied lines leaves out
that batch's earlier lines. A summary goes to stderr. Export → import into an
empty directory → export gives identical output apart from `created`.

### `cachydb utils find` / `get` (`find.go`)

```
cachydb utils find -d DB -c COLL [--query JSON] [--format jsonl|json]
cachydb utils get -d DB -c COLL --id ID
```

`--query` is a `db.Query` in JSON (the `find_documents` `query` shape;
unknown keys are rejected). Output is one document per line (default) or
a JSON array. `get` exits 1 when the document does not exist.

### `cachydb utils verify` (`verify.go`)

```
cachydb utils verify [-d DB [-c COLL]] [--deep] [--rebuild-index] [--format text|json]
```

An offline integrity check of a data directory (`db.Verify`,
`pkg/db/verify.go`; storage details in
[STORAGE.md](STORAGE.md#offline-verification-utils-verify)). It takes the
data-directory lock (so stop the server first; the in-use error carries a
hint that there is no `cachydb client` equivalent), runs no recovery,
starts no background work, and writes nothing unless `--rebuild-index` is
given. It checks the WAL (headers, checksums, offset continuity, that
every `put_doc`, `delete_doc` and `batch` record meets replay's id rule
and decodes as replay decodes it, that a batch names each id once), every
data segment record (checksum, decompression, the live documents' JSON),
each `index.db` with idxstore's full `Store.Check`, that every key-table
entry points at a put of its id with its WAL offset, that `index.db` is
not older than the segments, and that the data `index.db` does not cover
(which `Open` deletes or truncates) is still in the WAL.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--database` | `-d` | all | Check (or rebuild) only this database's collections; the WAL is always checked |
| `--collection` | `-c` | all | Only this collection (requires `-d`) |
| `--deep` | | `false` | Also check every index entry against the documents, and the `complex` and `arrays` counters |
| `--rebuild-index` | | `false` | Delete and rebuild `index.db` from the data segments (the routine `Open` uses for a damaged file), then verify; refused on a format-2 directory |
| `--format` | `-f` | `text` | `text` or `json` |

- **Text output** (stdout): `rebuilt: <db>/<coll>/index.db` per rebuilt
  file, one line per problem (`<file or db/coll>: <what>`, the file
  relative to the data directory), `note: …` lines for what the next
  `Open` repairs by itself (a torn WAL tail, a leftover segment of an
  interrupted flush or compaction, a torn final write of the newest data
  segment when `index.db` is missing or the directory is format 2, an
  index that is not built, a format-2 directory), and last a one-line
  summary: `verify: ok: …` or `verify: N
  problem(s): …` with the database, collection, document, data segment,
  and WAL counts. A healthy, cleanly closed directory prints only the
  summary (the empty next segment a full segment leaves is not a note).
- **JSON** (`--format json`): the `db.VerifyReport` (`ok`, `dir`,
  `format`, the counts, `wal_first`, `wal_next`, `rebuilt`, `problems` and
  `notices` as `[{file, database, collection, message}]`,
  `omitted_problems`).
- **Exit status**: 0 when there is no problem (notices do not count); 1
  when there is one (stderr then says `Error: verify found problems: N`),
  or when the check could not run (a locked or incompatible directory, an
  unknown `-d`/`-c`). At most 1,000 problems are listed.

### `cachydb utils user` (`user.go`)

```
cachydb utils user add NAME [--admin] [--default-database DB]
cachydb utils user list [--format text|json]
cachydb utils user remove NAME
cachydb utils user grant NAME DATABASE read|write|admin
cachydb utils user revoke NAME DATABASE
cachydb utils user token add NAME [--label L]
cachydb utils user token revoke NAME LABEL
```

Offline user management (`withUsers`: `withEngine` plus `auth.Open`,
which creates `_system` if missing). `add` and `token add` print the new
token exactly once, alone on stdout (so `TOKEN="$(…)"` works), with a
note on stderr. `list` prints a table (name, server admin, default
database, grants as `db=level`, token labels) or, with `--format json`,
the `list_users` user objects (`mcpserver.NewUserInfo`); never tokens or
digests. `DATABASE` `*` is every database. `remove` (and demoting over
MCP) of the only server admin fails with `auth.ErrLastServerAdmin`;
`revoke` of a missing grant fails with not found. Every change is logged
at INFO through the engine logger (actor `cachydb utils user`).

### `cachydb client` (`client.go`)

An MCP client (go-sdk `mcp.NewClient` + `StreamableClientTransport`, no
standalone SSE, no retries) for a running `cachydb -t http` server.
`--url` (persistent), else `CACHYDB_URL`, else
`http://127.0.0.1:7601/mcp`. `--token` (persistent), else
`CACHYDB_TOKEN`, is sent as `Authorization: Bearer` by `bearerTransport`,
a `RoundTripper` in `StreamableClientTransport.HTTPClient`, only on
requests to the endpoint's scheme and host (case-insensitive host
compare). The HTTP client refuses every redirect (`refuseRedirect`; the
error names the target), so `--url` must be the final endpoint.
`bearerTransport` also records a 401 answer, and
`withClient` then prefixes the error with a hint naming `--token` /
`CACHYDB_TOKEN` (no token given, or the token was rejected).

| Subcommand | Behavior |
|------------|----------|
| `client tools [--format text\|json]` | Tool names and descriptions, or the full definitions |
| `client call <tool> [json-args \| -]` | Calls any tool with a JSON object (`-` = stdin; omitted = `{}`), prints `structuredContent` as indented JSON; a tool error is returned as an error (stderr, exit 1) |
| `client export [-d] [-c] [-o]` | `exportfmt.Export` over `clientStore` (`list_databases`, `list_collections`, `describe_collection`, `find_documents` paged by `skip`/`limit` 500 in `_id` order); not a point-in-time snapshot; `schema_version` is `db.CurrentSchemaVersion` |
| `client import -i FILE [--on-conflict]` | `exportfmt.Import` over `clientStore` (`create_database`, `create_collection`, `update_schema`, `create_index`, `drop_index`, `insert_documents`, `find_documents`, `replace_document`, `insert_document`). `clientStore` is an `exportfmt.BatchTarget`: each run of document lines is one atomic `insert_documents` call (a run whose documents exceed 3 MiB of JSON, `clientBatchBytes`, is split, staying under the server's 4 MiB HTTP request limit; a single larger document goes through `insert_document`). In `skip`/`replace` mode one `find_documents` (`_id in [...]`) first finds the existing ids; `replace` then replaces those one `replace_document` call each, so a failing replace leaves the run's inserts applied. An "already exists" tool error marks a conflict, and `documents[i]` in an `insert_documents` error names the failing line. A server without `insert_documents` (`importTarget` checks `tools/list`) gets one `insert_document` call per document, as before. Counts and the summary match `utils import` |

## Configuration

Settings come from environment variables (`internal/config/config.go`:
`Init` reads each one with `fromEnv`, the defaults are in `defaults()`)
and are overridden by flags: the flag defaults are the configured values.
A malformed value (e.g. `WAL_RETENTION_AGE=soon`) aborts startup with exit
status 1 and an error that names the variable.

| Environment variable | Default | Flag |
|----------------------|---------|------|
| `PORT` | `7601` | `-p, --port` |
| `HOST` | `127.0.0.1` | `--host` |
| `AUTH` | `auto` | `--auth` |
| `TLS_CERT` | none | `--tls-cert` (server and `healthcheck`) |
| `TLS_KEY` | none | `--tls-key` (server and `healthcheck`) |
| `ROOT_DIR` | `~/.cachydb` | `-R, --root` |
| `DB_NAME` | `main` | `-d, --db` |
| `TRANSPORT` | `stdio` | `-t, --transport` |
| `WAL_RETENTION_AGE` | `24h` | `--wal-retention-age` |
| `WAL_RETENTION_SIZE` | `1GiB` | `--wal-retention-size` |
| `CACHE_SIZE` | `64MiB` | `--cache-size` |
| `DEFAULT_LIMIT` | `100` | `--default-limit` (server only) |
| `MAX_RESULT_BYTES` | `1MiB` | `--max-result-bytes` (server only) |
| `RESULT_TEXT` | `full` | `--result-text` (server only) |
| `CACHYDB_URL` | `http://127.0.0.1:7601/mcp` | `cachydb client --url` (read by `client` only) |
| `CACHYDB_TOKEN` | none | `cachydb client --token` (read by `client` only) |
| `BOOTSTRAP_ADMIN_TOKEN` | none | none (a secret; env only) |
| `BOOTSTRAP_ADMIN_TOKEN_FILE` | none | none |

Precedence: flag > environment variable > default.

- **`0` disables**: `WAL_RETENTION_AGE=0` / `WAL_RETENTION_SIZE=0` remove
  that retention limit (both 0 keeps all processed WAL forever);
  `CACHE_SIZE=0` turns the cache off.
- **Size units** (`config.ByteSize`, case-insensitive): binary `KiB`,
  `MiB`, `GiB`, `TiB` (powers of 1024); decimal `KB`, `MB`, `GB`, `TB`
  (powers of 1000); `B` or a bare number means bytes. Fractions are allowed
  if the result is a whole number of bytes (`1.5GiB`). Negative values are
  rejected.
- **Durations**: Go syntax (`90m`, `24h`, `168h`).
- **Empty values**: an empty variable (`DB_NAME=`, `PORT=`) counts as
  unset.
- **Default root**: `$HOME/.cachydb` (`CachyDB` instead of `.cachydb` on
  Windows, which is unsupported). If the home directory cannot be found,
  `./.cachydb`.

## Known issues

- **stdio responses in flight at EOF**: closing stdin ends the session
  cleanly (exit status 0), but the MCP SDK (v1.8.0) cancels requests that
  are still in flight and does not write their responses. Keep stdin open
  until the responses arrive.

## Execution Flow

```
main.go → cmd.Execute()
  config.Init()                         # package var; error → exit 1
  signal.NotifyContext(SIGINT, SIGTERM)
  rootCmd.ExecuteContext(ctx)
    runApp                              # root / app
      newBuilder() … Build(ctx)         # db.Open + mcp.NewServer
      App.Start(ctx)                    # until signal / stdin EOF (exit 0) / error
      App.Stop(WithoutCancel(ctx))      # 10s: sessions, Engine.Close
    runList / runMigrate / utils stats  # utils: withEngine(Open … Close)
    utils user …                        # withUsers: withEngine + auth.Open (creates _system)
    utils export/import/find/get        # withEngine; exportfmt.Engine adapter
    utils verify                        # db.Verify: lock, read-only checks (or rebuild); no recovery
    client tools/call/export/import     # withClient: MCP over Streamable HTTP; never opens the engine
    runDocs / version                   # print to stdout; never open the engine
    runHealthcheck                      # GET /healthz (3s); never opens the engine
  cobra.CheckErr(err)                   # print + exit 1
```

## Build & Install

```bash
go build -o cachydb
go install github.com/hop-/cachydb@latest
go build -ldflags "-X github.com/hop-/cachydb/internal/cmd.Version=$(git describe --tags --always)" -o cachydb
docker build --build-arg VERSION=$(git describe --tags --always) -t cachydb .   # the official image, see DEVELOPMENT.md "Releasing"
```
