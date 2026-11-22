---
name: config
title: Configuration
summary: Every flag and environment variable, defaults, precedence (flag > env > default), listen host, AUTH, find_documents result limits, BOOTSTRAP_ADMIN_TOKEN and TLS, value formats, cachydb client (CACHYDB_URL, CACHYDB_TOKEN), utils user, and the export format
---
# Configuration

Settings come from flags and environment variables. **A flag wins over an
environment variable, which wins over the default.**

## Server (`cachydb` and `cachydb app`)

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `-t, --transport` | `TRANSPORT` | `stdio` | `stdio` or `http`; any other value is an error |
| `-p, --port` | `PORT` | `7601` | HTTP port, path `/mcp` |
| `--host` | `HOST` | `127.0.0.1` | HTTP listen host; `127.0.0.1` = this machine only, `0.0.0.0` = every interface |
| `--auth` | `AUTH` | `auto` | `auto`, `required`, or `off` (see below); any other value is an error |
| `--tls-cert` | `TLS_CERT` | none | PEM certificate file; with `--tls-key`, serve HTTPS |
| `--tls-key` | `TLS_KEY` | none | PEM private key file; with `--tls-cert`, serve HTTPS |
| `-R, --root` | `ROOT_DIR` | `~/.cachydb` | Data directory (one server per directory) |
| `-d, --db` | `DB_NAME` | `main` | Default database; created at startup if missing |
| | `BOOTSTRAP_ADMIN_TOKEN` | none | At every start, make sure the server admin `admin` exists and holds this token (32 to 512 characters); never logged; no flag |
| | `BOOTSTRAP_ADMIN_TOKEN_FILE` | none | A file holding the bootstrap token (for Docker secrets); not together with `BOOTSTRAP_ADMIN_TOKEN` |
| `--default-limit` | `DEFAULT_LIMIT` | `100` | Documents `find_documents` returns at most when a call gives no `limit` (or `0`); must be greater than 0 |
| `--max-result-bytes` | `MAX_RESULT_BYTES` | `1MiB` | Cap on the JSON size of the documents one `find_documents` call returns, whatever its `limit`; at least one document is always returned; must be greater than 0 |

### When clients need a token

| Transport | Listen host | `AUTH=auto` | `AUTH=required` | `AUTH=off` |
| --- | --- | --- | --- | --- |
| stdio | - | no token | no token | no token |
| HTTP | loopback (`127.0.0.1`, `::1`, `localhost`) | no token | token | no token |
| HTTP | any other host | token | token | no token (warning logged) |

When clients need a token and no user exists, cachydb refuses to start
(exit 1) and says how to go on: create the first admin with
`cachydb utils user add NAME --admin` while the server is stopped, set
`BOOTSTRAP_ADMIN_TOKEN`, or, on a trusted network only, set `AUTH=off`.
Clients send the token as `Authorization: Bearer <token>`; without a
valid one, `/mcp` answers 401. `/healthz` never needs a token. Changing
`BOOTSTRAP_ADMIN_TOKEN` and restarting replaces the old bootstrap token;
see `cachydb docs access` for users, access levels, and tokens.

TLS needs both files; setting only one is a start-up error. The
certificate and key are read once at start-up.

`--default-limit` and `--max-result-bytes` bound what one `find_documents`
call returns; its `has_more` and `next_skip` say how to get the rest (see
`cachydb docs query`). `cachydb utils find` and `cachydb client export`
are not bounded by them.

## Engine tuning (server and `cachydb utils`)

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--cache-size` | `CACHE_SIZE` | `64MiB` | Document cache size; `0` disables the cache |
| `--wal-retention-age` | `WAL_RETENTION_AGE` | `24h` | Keep processed WAL this long; `0` = no age limit |
| `--wal-retention-size` | `WAL_RETENTION_SIZE` | `1GiB` | Keep at most this much processed WAL; `0` = no size limit |

`cachydb utils` takes `-R/--root` and the engine tuning flags too.

## Offline data commands (`cachydb utils`)

They open the data directory (taking its lock), so they fail while a
server runs on it; the error suggests the `cachydb client` command to use
instead.

| Command | Flags | Output |
| --- | --- | --- |
| `utils export` | `-d` database, `-c` collection (needs `-d`), `-o` file (default stdout) | The export format below |
| `utils import` | `-i` file (`-` = stdin), `--on-conflict fail\|skip\|replace` (default `fail`) | A summary on stderr |
| `utils find` | `-d`, `-c` (required), `--query` JSON (the `find_documents` `query`), `--format jsonl\|json` (default `jsonl`) | Documents |
| `utils get` | `-d`, `-c`, `--id` (required) | One document; exit 1 if not found |
| `utils user add NAME` | `--admin`, `--default-database DB` | The first token, once, alone on stdout |
| `utils user list` | `--format text\|json` | Users, access, and token labels; never tokens |
| `utils user remove NAME` | | Removes the user and its tokens (not the only server admin) |
| `utils user grant NAME DATABASE read\|write\|admin` / `revoke NAME DATABASE` | `*` = every database | |
| `utils user token add NAME` / `token revoke NAME LABEL` | `--label L` | `add` prints the token once, alone on stdout |

`--on-conflict`: `fail` stops at the first existing `_id` (and reports how
many lines were applied); `skip` keeps existing documents; `replace`
replaces them, and also updates a differing collection schema (checked
against the documents) and adds missing indexes. With `fail` or `skip`, an
existing collection whose schema or index set differs is an error.

## Client (`cachydb client`)

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | `CACHYDB_URL` | `http://127.0.0.1:7601/mcp` | MCP endpoint of a running `cachydb -t http` server |
| `--token` | `CACHYDB_TOKEN` | none | Token for a server that requires one, sent as `Authorization: Bearer`; prefer the variable (a flag shows in the process list). After a 401 the error names the flag |

- `cachydb client tools [--format json]` lists the server's tools.
- `cachydb client call <tool> [json-args | -]` calls any tool with a JSON
  object (`-` reads stdin) and prints its structured result as JSON; a
  tool error goes to stderr with exit status 1.
- `cachydb client export [-d] [-c] [-o]` and `cachydb client import -i
  file [--on-conflict]` use the same format and rules as `utils`. A client
  export reads pages of 500 documents, so it is not a point-in-time
  snapshot.

## Export format

JSON Lines (UTF-8), one object per line: a header, then each database
(sorted) with its collections (sorted), each followed by its documents
sorted by `_id`:

```text
{"type":"cachydb-export","version":1,"cachydb":"v0.0.5","created":"2026-11-02T10:00:00Z"}
{"type":"database","name":"kb","schema_version":1}
{"type":"collection","database":"kb","name":"notes","schema":null,"indexes":[{"name":"by_tags","field":"tags"}]}
{"type":"document","database":"kb","collection":"notes","doc":{"_id":"n1","tags":["wal"],"title":"WAL design"}}
```

Importing an export into an empty directory and exporting again gives
the same file except the header's `created`. A header with another
`version` is rejected.

## Health check (`cachydb healthcheck`)

| Flag | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | | `http://127.0.0.1:<port>/healthz` | Endpoint to probe; always probed when given |
| `-p, --port` | `PORT` | `7601` | Port for the default URL |
| `-t, --transport` | `TRANSPORT` | `stdio` | Without `--url`, `stdio` means nothing to probe (exit 0) |
| `--tls-cert`, `--tls-key` | `TLS_CERT`, `TLS_KEY` | none | With both, the default URL is `https://` |

Exit 0 on HTTP 200; exit 1 otherwise (3-second timeout). An `https://`
URL on a loopback host is probed without verifying the certificate. See
`cachydb docs mcp`.

## Docker image

The image `hovhannespalyan/cachydb` sets `TRANSPORT=http`,
`HOST=0.0.0.0`, `PORT=7601`, `ROOT_DIR=/data` and `DB_NAME=main`, and
passes no flags, so every variable above works with `-e`. Keep
`ROOT_DIR=/data` (the volume). Arguments after the image name are passed
to cachydb; flags still win over variables, but the image's healthcheck
reads only the variables.

**A container needs a user or `AUTH=off`:** with `HOST=0.0.0.0` and the
default `AUTH=auto`, clients need a token, so a container without a user
exits at start with the refusal message above. `BOOTSTRAP_ADMIN_TOKEN`
(or `BOOTSTRAP_ADMIN_TOKEN_FILE`) creates the admin without a shell.

```bash
docker run -d -p 7601:7601 -v cachydb-data:/data -e BOOTSTRAP_ADMIN_TOKEN="$TOKEN" hovhannespalyan/cachydb
docker run -d -p 127.0.0.1:8080:8080 -v cachydb-data:/data -e PORT=8080 -e DB_NAME=app -e AUTH=off hovhannespalyan/cachydb
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
cachydb -t http --host 0.0.0.0 --tls-cert cert.pem --tls-key key.pem   # HTTPS on every interface; needs a user
```

## Exit status and output

- Errors are printed to stderr as `Error: <message>`; the exit status is 1.
- In stdio mode, stdout carries only MCP messages.
- `cachydb docs`, `cachydb version`, and `--help` print to stdout and exit 0.
- Data commands (`utils export/find/get`, `client call/export/tools`)
  write data to stdout and diagnostics to stderr.
- `cachydb healthcheck` prints nothing on success (exit 0); on failure it
  prints `Error: healthcheck: ...` to stderr and exits 1.
