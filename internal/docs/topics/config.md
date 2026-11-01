---
name: config
title: Configuration
summary: Every flag and environment variable, defaults, precedence (flag > env > default), and value formats
---
# Configuration

Settings come from flags and environment variables. **A flag wins over an
environment variable, which wins over the default.**

## Server (`cachydb` and `cachydb app`)

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `-t, --transport` | `TRANSPORT` | `stdio` | `stdio` or `http`; any other value is an error |
| `-p, --port` | `PORT` | `7601` | HTTP port; listens on all interfaces, path `/mcp` |
| `-R, --root` | `ROOT_DIR` | `~/.cachydb` | Data directory (one server per directory) |
| `-d, --db` | `DB_NAME` | `main` | Default database; created at startup if missing |

## Engine tuning (server and `cachydb utils`)

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--cache-size` | `CACHE_SIZE` | `64MiB` | Document cache size; `0` disables the cache |
| `--wal-retention-age` | `WAL_RETENTION_AGE` | `24h` | Keep processed WAL this long; `0` = no age limit |
| `--wal-retention-size` | `WAL_RETENTION_SIZE` | `1GiB` | Keep at most this much processed WAL; `0` = no size limit |

`cachydb utils` takes `-R/--root` and the engine tuning flags too.

## Health check (`cachydb healthcheck`)

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | | `http://127.0.0.1:<port>/healthz` | Endpoint to probe; always probed when given |
| `-p, --port` | `PORT` | `7601` | Port for the default URL |
| `-t, --transport` | `TRANSPORT` | `stdio` | Without `--url`, `stdio` means nothing to probe (exit 0) |

Exit 0 on HTTP 200; exit 1 otherwise (3-second timeout). See
`cachydb docs mcp`.

## Docker image

The image `hovhannespalyan/cachydb` sets `TRANSPORT=http`, `PORT=7601`,
`ROOT_DIR=/data` and `DB_NAME=main`, and passes no flags, so every
variable above works with `-e`. Keep `ROOT_DIR=/data` (the volume).
Arguments after the image name are passed to cachydb; flags still win
over variables, but the image's healthcheck reads only the variables.

```bash
docker run -d -p 8080:8080 -v cachydb-data:/data -e PORT=8080 -e DB_NAME=app hovhannespalyan/cachydb
docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb
```

## Formats

- **Sizes**: a number with an optional unit, case-insensitive. Binary
  units `KiB`, `MiB`, `GiB`, `TiB` (powers of 1024); decimal `KB`, `MB`,
  `GB`, `TB` (powers of 1000); `B` or no unit means bytes. Fractions are
  allowed if the result is a whole number of bytes (`1.5GiB`). Negative
  values are rejected.
- **Durations**: Go syntax, such as `90m`, `24h`, or `168h`.
- **`0` disables**: `--cache-size 0` turns the cache off; a retention
  limit of `0` removes that limit (both `0` keeps all processed WAL).
- An empty environment variable counts as unset.
- A malformed value (for example `WAL_RETENTION_AGE=soon`) stops cachydb
  at startup with exit status 1.

## Examples

```bash
cachydb -R /srv/cachydb -d app                 # stdio, custom directory and default database
ROOT_DIR=/srv/cachydb TRANSPORT=http cachydb   # same settings from the environment
cachydb -t http -p 8080 --cache-size 256MiB    # HTTP on port 8080 with a bigger cache
```

## Exit status and output

- Errors are printed to stderr as `Error: <message>`; the exit status is 1.
- In stdio mode, stdout carries only MCP messages.
- `cachydb docs`, `cachydb version`, and `--help` print to stdout and exit 0.
- `cachydb healthcheck` prints nothing on success (exit 0); on failure it
  prints `Error: healthcheck: ...` to stderr and exits 1.
