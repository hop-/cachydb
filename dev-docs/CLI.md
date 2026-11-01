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

### `cachydb version`

Prints `cachydb version <v>`. Resolution order: build-time `-ldflags`
(`cmd.Version`), then the module version from `debug.ReadBuildInfo()`, then
`v0.0.0-dev`.

### `cachydb utils`

Parent of the maintenance commands. Its persistent flags are inherited by
every subcommand: `-R/--root`, `--wal-retention-age`,
`--wal-retention-size`, `--cache-size` (same meaning as above).

`utils` commands **open the engine**: they take the data-directory lock and
run recovery (WAL replay), then close the engine with a flush. They
therefore **fail while a server is running** on the same directory. Stop
the server first.

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

## Configuration

Settings come from environment variables (`internal/config/config.go`,
loaded with `envconfig`) and are overridden by flags. A malformed value
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
      App.Start(ctx)                    # until signal / stdin EOF / error
      App.Stop(WithoutCancel(ctx))      # 10s: sessions, Engine.Close
    runList / runMigrate                # utils: withEngine(Open … Close)
  cobra.CheckErr(err)                   # print + exit 1
```

## Build & Install

```bash
go build -o cachydb
go install github.com/hop-/cachydb@latest
go build -ldflags "-X github.com/hop-/cachydb/internal/cmd.Version=$(git describe --tags --always)" -o cachydb
```
