# Testing CachyDB

## Automated Tests

```bash
go test -race ./...                                   # all tests
go test -race -count=20 -run 'Crash|Recovery|Torn|Concurrent' ./pkg/db/...   # recovery stress
test -z "$(gofmt -l .)" && go vet ./...               # format and vet
```

The suite covers the storage engine (WAL, data segments, flush, recovery
with a crash-point matrix, compaction, query and index semantics), the MCP
server (per-session `use_database`, an end-to-end tool flow), and the CLI.
Tests use only the standard `testing` package and temporary directories.

### Benchmarks

```bash
go test -run '^$' -bench . -benchmem ./pkg/db/...
```

`pkg/db/bench_test.go` measures parallel inserts (group commit), the cost
of a one-document flush at 1k and 100k documents, cached and uncached
reads, and indexed and scanning queries. Run them on a real disk: if
`$TMPDIR` is a tmpfs, fsync costs nothing and the numbers are misleading.
Point `TMPDIR` at the disk under test, e.g.
`TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench . ./pkg/db/...`.

## Using MCP Inspector (Recommended)

The easiest way to test the server interactively is with the official MCP
Inspector:

```bash
npx @modelcontextprotocol/inspector ./cachydb -R /tmp/cachydb-test
```

This will:

- Start an interactive web UI
- Connect to your server
- Let you see all 12 tools
- Call tools with arguments
- View responses in real-time

Use a scratch data directory (`-R`): only one process can open a data
directory at a time, so a server already running on `~/.cachydb` would make
this one fail with "data directory is in use by another cachydb process".

## Using Claude Desktop

1. Add to your Claude Desktop config:

   **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
   **Linux:** `~/.config/Claude/claude_desktop_config.json`

   ```json
   {
     "mcpServers": {
       "cachydb": {
         "command": "/absolute/path/to/cachydb",
         "args": ["-R", "/absolute/path/to/data"]
       }
     }
   }
   ```

   Use the `-R` flag for the data directory; the `ROOT_DIR` and `DB_NAME`
   environment variables currently have no effect (see
   [`dev-docs/CLI.md`](dev-docs/CLI.md#known-issues)).

2. Restart Claude Desktop
3. You should see CachyDB tools available in the tool panel
4. Interact naturally: "Create a users collection" or "Show me all collections"

## Using the MCP Client Example

```bash
# Build CachyDB (repository root)
go build -o cachydb

# Run the MCP client example against a fresh temporary data directory
go run ./examples/mcp-client
```

It starts `./cachydb` over stdio and exercises database management,
collections with a schema, document insert/find/update/delete, and
indexes. Flags: `-bin PATH` (binary elsewhere), `-root DIR` (use and keep
a data directory). See [`examples/README.md`](examples/README.md).

## Manual Testing over stdio

The server reads JSON-RPC lines on stdin and writes only JSON-RPC to stdout
(logs go to stderr). Two things matter when piping requests:

- **Keep stdin open** until the responses arrive. If stdin closes while
  requests are still in flight, the MCP SDK drops their responses and the
  process exits with status 1. Append a `sleep`.
- **Requests run concurrently.** Responses can arrive out of order, and a
  request does not wait for the previous one, so a `find_documents` piped
  right after an `insert_document` may not see the new document. Send
  dependent requests after the earlier response (or with a `sleep` between
  them).

```bash
(printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'; sleep 1) \
  | ./cachydb -R /tmp/cachydb-test | jq -c
```

More requests to try (send each after the previous response):

```JSON
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"current_database","arguments":{}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"create_database","arguments":{"name":"analytics"}}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"use_database","arguments":{"name":"analytics"}}}
{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"create_collection","arguments":{"name":"events"}}}
{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"insert_document","arguments":{"collection":"events","document":{"type":"page_view","user":"alice","n":9}}}}
{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"insert_document","arguments":{"collection":"events","document":{"type":"click","user":"bob","n":10}}}}
{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"find_documents","arguments":{"collection":"events","query":{"filters":[{"field":"n","operator":"gt","value":9}]}}}}
{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"find_documents","arguments":{"collection":"events","query":{}}}}
```

`use_database` applies to the current session only; a new process (or HTTP
session) starts on `main` again.

## Durability Smoke Tests

```bash
./cachydb -t http -p 7601 -R /tmp/cdb &      # 1. start, then create and insert via a client
./cachydb utils list -c -R /tmp/cdb          # 2. fails: the data directory is locked
kill -9 %1                                   # 3. crash without a flush
./cachydb utils list -c -R /tmp/cdb          # 4. the inserted documents are listed (WAL replay)
```

A clean stop (Ctrl-C / SIGTERM, or closing stdin in stdio mode) flushes
everything before exit; the next start has nothing to re-apply.

## Direct Library Usage

To use CachyDB as a Go library (without MCP), see
[`examples/basic/`](examples/basic/) and
[`dev-docs/DATABASE_ENGINE.md`](dev-docs/DATABASE_ENGINE.md).
