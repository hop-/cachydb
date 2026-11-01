---
name: mcp
title: Using cachydb as an MCP server
summary: stdio vs Streamable HTTP, client configuration examples, per-session databases, and the data-directory lock
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

## Self-description

- `tools/list` returns typed input and output JSON Schemas, with enums for
  query operators and schema field types, plus read-only, destructive, and
  idempotent annotations.
- The `initialize` result carries short usage instructions.
- Every docs topic is a resource: `cachydb://docs/<topic>` (markdown),
  for example `cachydb://docs/query`.
