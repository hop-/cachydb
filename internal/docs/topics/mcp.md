---
name: mcp
title: Using cachydb as an MCP server
summary: stdio vs Streamable HTTP, listen host, tokens and TLS, client configuration examples (incl. Docker and tokens), health checks, per-session databases, the data-directory lock, cachydb client, and the tools by task
---
<!--
Client config formats checked on 2026-11-01 against the official docs:
- Claude Code:    https://code.claude.com/docs/en/mcp
- Claude Desktop: https://modelcontextprotocol.io/docs/develop/connect-local-servers
- Cursor:         https://cursor.com/docs/context/mcp
- VS Code:        https://code.visualstudio.com/docs/agents/reference/mcp-configuration
Re-check them when updating this topic: the formats change.
-->
# Using cachydb as an MCP server

cachydb speaks MCP over two transports:

- **stdio** (default): the client starts `cachydb` as a child process and
  talks over stdin/stdout. One session per process. stdout carries only
  MCP messages; logs go to stderr. The server exits (status 0) when the
  client closes stdin.
- **Streamable HTTP**: `cachydb -t http -p 7601` serves
  `http://localhost:7601/mcp`, listening on `127.0.0.1` (this machine
  only; `--host 0.0.0.0` for every interface). Many clients can connect at
  once; idle sessions are closed after 30 minutes. `--tls-cert` and
  `--tls-key` serve HTTPS.

## Tokens (`AUTH`)

stdio and loopback HTTP need no token. HTTP on any other host needs one
(`AUTH=auto`, the default), as does all HTTP with `AUTH=required`;
`AUTH=off` never asks for one and logs a warning on a non-loopback host.
Clients send `Authorization: Bearer <token>`; without a valid token
`/mcp` answers 401. When a token is needed and no user exists, cachydb
refuses to start: create the first admin with
`cachydb utils user add NAME --admin` (server stopped), set
`BOOTSTRAP_ADMIN_TOKEN`, or set `AUTH=off` on a trusted network only.
With tokens, each user sees and uses only the databases it has access
to, `whoami` shows that access, and server admins also get the user
tools. See `cachydb docs config` and `cachydb docs access`.

Use absolute paths in client configs. Replace `/path/to/cachydb` with the
binary's location (`which cachydb`) and `/path/to/data` with the data
directory.

## Claude Code

```bash
# stdio (the -- separates Claude's options from the server command)
claude mcp add --transport stdio cachydb -- /path/to/cachydb -R /path/to/data

# HTTP (start `cachydb -t http -p 7601` separately)
claude mcp add --transport http cachydb http://localhost:7601/mcp

# HTTP on a server that requires tokens
claude mcp add --transport http cachydb https://db.example:7601/mcp --header "Authorization: Bearer <token>"
```

Add `--scope project` to share it through the project's `.mcp.json`, or
`--scope user` for all your projects.

## Claude Desktop

Edit `claude_desktop_config.json` (macOS:
`~/Library/Application Support/Claude/`, Windows: `%APPDATA%\Claude\`,
Linux: `~/.config/Claude/`), then restart Claude Desktop:

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

## Cursor

`.cursor/mcp.json` in a project, or `~/.cursor/mcp.json` for all
projects:

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

For HTTP, use `"cachydb": {"url": "http://localhost:7601/mcp"}`.

## VS Code

`.vscode/mcp.json` in the workspace (note the top-level `servers` key):

```json
{
  "servers": {
    "cachydb": {
      "type": "stdio",
      "command": "/path/to/cachydb",
      "args": ["-R", "/path/to/data"]
    }
  }
}
```

For HTTP: `"cachydb": {"type": "http", "url": "http://localhost:7601/mcp"}`.

## Other clients (generic JSON)

Most clients accept the `mcpServers` shape:

```json
{
  "mcpServers": {
    "cachydb": {"command": "/path/to/cachydb", "args": ["-R", "/path/to/data", "-d", "main"]}
  }
}
```

Useful arguments: `-R <dir>` data directory, `-d <name>` default database,
`-t http -p <port>` HTTP transport. See `cachydb docs config`.

## Docker

The official image `hovhannespalyan/cachydb` (linux/amd64, linux/arm64)
runs the HTTP transport on port 7601 by default, as the non-root user
65532, with its data in the `/data` volume. Configure it with environment
variables (see `cachydb docs config`).

The image listens on every interface (`HOST=0.0.0.0`), so **a container
needs a user or `AUTH=off`**: without either it exits at start with the
refusal message. To require tokens, give it a bootstrap admin token (see
`cachydb docs access`):

```bash
# HTTP server with tokens; the admin's token is the value you chose
docker run -d --name cachydb -p 7601:7601 -v cachydb-data:/data \
  -e BOOTSTRAP_ADMIN_TOKEN="$(openssl rand -hex 32)" hovhannespalyan/cachydb
```

Use TLS (`TLS_CERT`, `TLS_KEY`) or a trusted network when clients on
other machines connect. For local use, publish the port on localhost and
set `AUTH=off`:

```bash
# HTTP server: http://localhost:7601/mcp, no tokens
docker run -d --name cachydb -p 127.0.0.1:7601:7601 -v cachydb-data:/data -e AUTH=off hovhannespalyan/cachydb

# Claude Code against the HTTP container
claude mcp add --transport http cachydb http://localhost:7601/mcp

# Claude Code starting a stdio container per session (keep -i, no -t)
claude mcp add --transport stdio cachydb -- \
  docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb
```

Generic stdio JSON (Claude Desktop, Cursor and other `mcpServers`
clients):

```json
{
  "mcpServers": {
    "cachydb": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "-v", "cachydb-data:/data",
               "-e", "TRANSPORT=stdio", "hovhannespalyan/cachydb"]
    }
  }
}
```

A volume is a data directory: one container at a time (see below). A bind
mount (`-v /host/dir:/data`) must be writable by UID 65532
(`chown 65532:65532 /host/dir`), or run with `--user "$(id -u):$(id -g)"`.

## Health checks

The HTTP transport also serves `GET /healthz`: `200
{"status":"ok","version":"..."}` while the database is open, `503
{"status":"unavailable"}` while it shuts down. `cachydb healthcheck`
probes it and exits 0 (healthy) or 1, with the reason on stderr:

```bash
TRANSPORT=http cachydb healthcheck                    # http://127.0.0.1:$PORT/healthz
cachydb healthcheck --url http://db.internal:7601/healthz
```

Without `--url`, it reads `PORT` and `TRANSPORT` (or `-p`, `-t`); if the
transport is `stdio` there is nothing to probe and it exits 0. With
`TLS_CERT` and `TLS_KEY` (or `--tls-cert`, `--tls-key`) it probes
`https://` and, on loopback, does not verify the certificate.
`/healthz` never needs a token. It never opens the database. The Docker
image uses it as its `HEALTHCHECK`, so set `PORT`, `TRANSPORT`,
`TLS_CERT` and `TLS_KEY` in a container as environment variables, not
flags.

## Choosing the database

Every data tool resolves its database in this order:

1. the tool's `database` argument, if given;
2. the database this session chose with `use_database`;
3. the user's default database (only when the server requires tokens);
4. the server default (`main`, or `-d/--db`).

The `use_database` choice belongs to one MCP session: other sessions
(other HTTP clients) are not affected. `current_database` reports it.

## One server per data directory

A data directory is locked by the process that opened it. A second
`cachydb` on the same directory fails at startup with "data directory is
in use by another cachydb process". To share data between several agents,
run one HTTP server and connect them all to it, or give each stdio server
its own `-R` directory.

While an HTTP server runs, shell scripts and agents without MCP support
reach it with `cachydb client` (`--url` or `CACHYDB_URL`, default
`http://127.0.0.1:7601/mcp`):

```bash
cachydb client tools
cachydb client call find_documents '{"database":"kb","collection":"notes","query":{"limit":5}}'
cachydb client export -d kb -o kb.jsonl        # same format as cachydb utils export
```

Agents sharing documents can update safely with `if` conditions
(compare-and-set): keep a `version` field, update with
`"if": [{"field": "version", "operator": "eq", "value": 3}]` and
`"updates": {"version": 4, ...}`, and on "condition failed" re-read and
retry.

## Tools by task

| Task | Tools |
| --- | --- |
| Databases | `list_databases`, `create_database`, `delete_database`, `use_database`, `current_database` |
| Collections | `create_collection`, `list_collections`, `describe_collection`, `update_schema`, `rename_collection`, `copy_collection`, `delete_collection` |
| Indexes | `create_index`, `drop_index` |
| Writing | `insert_document`, `update_document` (`updates`, `unset`, `if`, `upsert`), `replace_document`, `delete_document` (`if`) |
| Reading | `find_documents` (`filters`, `any`, `sort`, `fields`, `limit`, `skip`; result `has_more`, `next_skip`), `count_documents`, `distinct` |
| Users (only when the server requires tokens) | `whoami` (everyone); `list_users`, `create_user`, `set_access`, `create_token`, `revoke_token`, `delete_user` (server admins; hidden from other users) |

`cachydb docs tools` has every schema and an example call; the user tools
are in `cachydb docs access`.

`find_documents` never returns an unbounded result: without a `limit` it
returns at most `--default-limit` documents (100), and the JSON of its
documents stays within `--max-result-bytes` (1 MiB), with at least one
document. Page with `"skip": next_skip` while `has_more` is `true`; see
`cachydb docs query`.

## Self-description

- `tools/list` returns typed input and output JSON Schemas, with enums for
  query operators (including `contains`, `has`, `has_any`, `has_all`),
  sort orders, and schema field types, plus read-only, destructive, and
  idempotent annotations.
- The `initialize` result carries short usage instructions.
- Every docs topic is a resource: `cachydb://docs/<topic>` (markdown),
  for example `cachydb://docs/query`.
