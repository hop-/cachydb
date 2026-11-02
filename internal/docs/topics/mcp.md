---
name: mcp
title: Using cachydb as an MCP server
summary: stdio vs Streamable HTTP, client configuration examples (incl. Docker), health checks, per-session databases, the data-directory lock, cachydb client, and the 21 tools by task
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
  `http://localhost:7601/mcp`, listening on all interfaces. Many clients
  can connect at once; idle sessions are closed after 30 minutes. There is
  no authentication or TLS, so keep it on a trusted network.

Use absolute paths in client configs. Replace `/path/to/cachydb` with the
binary's location (`which cachydb`) and `/path/to/data` with the data
directory.

## Claude Code

```bash
# stdio (the -- separates Claude's options from the server command)
claude mcp add --transport stdio cachydb -- /path/to/cachydb -R /path/to/data

# HTTP (start `cachydb -t http -p 7601` separately)
claude mcp add --transport http cachydb http://localhost:7601/mcp
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

```bash
# HTTP server: http://localhost:7601/mcp
docker run -d --name cachydb -p 7601:7601 -v cachydb-data:/data hovhannespalyan/cachydb

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
transport is `stdio` there is nothing to probe and it exits 0. It never
opens the database. The Docker image uses it as its `HEALTHCHECK`, so set
`PORT` and `TRANSPORT` in a container as environment variables, not
flags.

## Choosing the database

Every data tool resolves its database in this order:

1. the tool's `database` argument, if given;
2. the database this session chose with `use_database`;
3. the server default (`main`, or `-d/--db`).

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
| Reading | `find_documents` (`filters`, `any`, `sort`, `fields`, `limit`, `skip`), `count_documents`, `distinct` |

`cachydb docs tools` has every schema and an example call.

## Self-description

- `tools/list` returns typed input and output JSON Schemas, with enums for
  query operators (including `contains`, `has`, `has_any`, `has_all`),
  sort orders, and schema field types, plus read-only, destructive, and
  idempotent annotations.
- The `initialize` result carries short usage instructions.
- Every docs topic is a resource: `cachydb://docs/<topic>` (markdown),
  for example `cachydb://docs/query`.
