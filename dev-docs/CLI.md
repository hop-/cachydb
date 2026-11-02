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
| `--port` | `-p` | `7601` (or `PORT`) | HTTP port (listens on all interfaces, path `/mcp`) |
| `--root` | `-R` | `~/.cachydb` (or `ROOT_DIR`) | Data directory |
| `--wal-retention-age` | | `24h` (or `WAL_RETENTION_AGE`) | Keep processed WAL segments this long; Go duration; `0` = no age limit |
| `--wal-retention-size` | | `1GiB` (or `WAL_RETENTION_SIZE`) | Keep at most this much processed WAL; `0` = no size limit |
| `--cache-size` | | `64MiB` (or `CACHE_SIZE`) | Document cache size; `0` = cache disabled |

The server takes the data-directory lock; a second server (or a `utils`
command) on the same directory fails with *"data directory is in use by
another cachydb process"*.

Unknown transports fail at startup (`invalid transport "x": use stdio or
http`).

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
`schema`, `config`, `errors`. The `tools` topic is generated from
`mcpserver.ToolDefinitions()`; in JSON it adds
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
cachydb healthcheck --url http://db.internal:7601/healthz
cachydb healthcheck                                   # TRANSPORT unset (stdio): exits 0, nothing to probe
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--url` | | `http://127.0.0.1:<port>/healthz` | Endpoint to probe; an explicit URL is always probed |
| `--port` | `-p` | `7601` (or `PORT`) | Port for the default URL |
| `--transport` | `-t` | `stdio` (or `TRANSPORT`) | Without `--url`, `stdio` means nothing to probe |

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
(`export`, `import`, `find`, `get`) add a hint naming the `cachydb client`
command to the data-directory-in-use error (`clientHint` in `export.go`).

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
cachydb utils import -i FILE|- [--on-conflict fail|skip|replace]
```

JSON Lines in the format of `internal/exportfmt` (header with `version`
1, then database, collection, and document lines, all sorted; see `cachydb
docs config`). `-c` requires `-d`; export writes to stdout without `-o`.
Import creates missing databases and collections (schema and indexes);
`fail` (default) stops at the first existing `_id` and reports how many
lines were applied, `skip` keeps existing documents, `replace` replaces
them and also updates a differing schema (via `UpdateSchema`) and adds
missing indexes. With `fail`/`skip` a collection whose schema or index set
differs is an error. A summary goes to stderr. Export → import into an
empty directory → export gives identical output apart from `created`.

### `cachydb utils find` / `get` (`find.go`)

```
cachydb utils find -d DB -c COLL [--query JSON] [--format jsonl|json]
cachydb utils get -d DB -c COLL --id ID
```

`--query` is a `db.Query` in JSON (the `find_documents` `query` shape;
unknown keys are rejected). Output is one document per line (default) or
a JSON array. `get` exits 1 when the document does not exist.

### `cachydb client` (`client.go`)

An MCP client (go-sdk `mcp.NewClient` + `StreamableClientTransport`, no
standalone SSE, no retries) for a running `cachydb -t http` server.
`--url` (persistent), else `CACHYDB_URL`, else
`http://127.0.0.1:7601/mcp`.

| Subcommand | Behavior |
|------------|----------|
| `client tools [--format text\|json]` | Tool names and descriptions, or the full definitions |
| `client call <tool> [json-args \| -]` | Calls any tool with a JSON object (`-` = stdin; omitted = `{}`), prints `structuredContent` as indented JSON; a tool error is returned as an error (stderr, exit 1) |
| `client export [-d] [-c] [-o]` | `exportfmt.Export` over `clientStore` (`list_databases`, `list_collections`, `describe_collection`, `find_documents` paged by `skip`/`limit` 500 in `_id` order); not a point-in-time snapshot; `schema_version` is `db.CurrentSchemaVersion` |
| `client import -i FILE [--on-conflict]` | `exportfmt.Import` over `clientStore` (`create_database`, `create_collection`, `update_schema`, `create_index`, `drop_index`, `insert_document`, `replace_document`); an "already exists" tool error marks a conflict |

## Configuration

Settings come from environment variables (`internal/config/config.go`,
loaded with `envconfig` using `envconfig:"NAME"` tags) and are overridden
by flags: the flag defaults are the configured values. A malformed value
(e.g. `WAL_RETENTION_AGE=soon`) aborts startup with exit status 1.

| Environment variable | Default | Flag |
|----------------------|---------|------|
| `PORT` | `7601` | `-p, --port` |
| `ROOT_DIR` | `~/.cachydb` | `-R, --root` |
| `DB_NAME` | `main` | `-d, --db` |
| `TRANSPORT` | `stdio` | `-t, --transport` |
| `WAL_RETENTION_AGE` | `24h` | `--wal-retention-age` |
| `WAL_RETENTION_SIZE` | `1GiB` | `--wal-retention-size` |
| `CACHE_SIZE` | `64MiB` | `--cache-size` |
| `CACHYDB_URL` | `http://127.0.0.1:7601/mcp` | `cachydb client --url` (read by `client` only) |

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
- **Empty values**: `DB_NAME=` or `TRANSPORT=` count as unset.
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
    runList / runMigrate                # utils: withEngine(Open … Close)
    utils export/import/find/get        # withEngine; exportfmt.Engine adapter
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
