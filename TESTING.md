# Testing CachyDB

## Automated Tests

Run everything from the repository root.

```bash
go test -race ./...                              # all tests (about a minute)
go test -race ./pkg/db/...                       # engine only
go test -race -run TestCrashMatrix ./pkg/db/     # a single test
test -z "$(gofmt -l .)" && go vet ./...          # format and vet
```

The suite covers:

- **The storage engine:**
  - the WAL, data segments, flush, and recovery with a crash-point
    matrix;
  - compaction, the metadata checkpoint, and the query and index
    semantics;
  - conditional writes and schema operations (update, rename, copy).
- **The MCP server:** all 21 tools, per-session `use_database`, an
  end-to-end tool flow, and the docs resources.
- **The CLI:** `docs`, `healthcheck`, `utils` (including export/import
  round trips), and `client` against a live test server.
- **The examples:** `examples/basic` and `examples/mcp-client` run end to
  end as smoke tests, and check their own results. The `mcp-client` test
  builds the server binary first; `-short` skips it.

Tests use only the standard `testing` package and temporary directories.

### Stress tests

The crash, recovery, concurrency, conditional-write, rename and schema
tests are timing-sensitive. Run them many times before merging changes
to the engine or making a release:

```bash
# Quick (about 30 seconds):
go test -race -count=20 -run 'Crash|Recovery|Torn|Concurrent|Condition|Rename|Schema' ./pkg/db/...
# Full, before a release (about 10 minutes):
go test -race -count=200 -run 'Crash|Recovery|Torn|Concurrent|Condition|Rename|Schema' ./pkg/db/...
```

On a failure, the crash tests log the crash-hook trace, a listing of the
data directory, and a diff of the expected and actual state.

### Coverage

```bash
go test -coverpkg=./... -coverprofile=cover.out ./...   # run all tests, record coverage
go tool cover -func=cover.out | tail -1                 # total percentage
go tool cover -func=cover.out | grep -v examples/ | sort -k3 -n | head -30  # least-covered functions
go tool cover -html=cover.out                           # line-by-line view in the browser
go test -cover ./...                                    # quick per-package summary
```

`-coverpkg=./...` counts code exercised by tests in other packages too.
For example, the engine is also exercised by the MCP and CLI tests.
`cover.out` is a generated file; don't commit it. The example programs
under `examples/` are covered by their smoke tests (see below).

### Cross-platform builds

Linux and macOS are supported. Every other target must still compile:

```bash
for os in linux darwin freebsd openbsd netbsd dragonfly solaris illumos windows; do GOOS=$os go build ./... || echo "FAIL $os"; done
GOOS=aix GOARCH=ppc64 go build ./...    # aix exists only on ppc64
```

### Benchmarks

```bash
TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench . -benchmem ./pkg/db/...
```

`pkg/db/bench_test.go` measures:

- parallel inserts (group commit);
- the cost of a one-document flush at 1k and 100k documents;
- cached and uncached reads;
- indexed and scanning queries.

Run them on a real disk. If `$TMPDIR` is a tmpfs (as `/tmp` often is),
fsync costs nothing and the numbers are misleading, hence the `TMPDIR`
override above. To compare two versions, run both back to back on the
same machine and disk. Absolute numbers vary between runs and days, so
don't compare against numbers recorded earlier.

### Docker image

The image is tested in CI, not locally. Every pull request runs
`.github/workflows/docker.yml`, which:

- lints the Dockerfile and builds the image;
- checks that it runs as UID 65532;
- waits for it to report healthy and checks `GET /healthz`;
- sends an MCP `initialize` over HTTP and checks `version --json`;
- checks for a clean `docker stop`;
- checks the stdio mode.

### Before merging or releasing

1. `go test -race ./...`
2. The quick stress loop (the full 200-run loop before a release)
3. Format and vet
4. The cross-platform builds

Check coverage and benchmarks when you change performance-sensitive code
or add a feature.

## Using MCP Inspector (Recommended)

The easiest way to test the server interactively is with the official MCP
Inspector:

```bash
npx @modelcontextprotocol/inspector ./cachydb -R /tmp/cachydb-test
```

This will:

- Start an interactive web UI
- Connect to your server
- Let you see all 21 tools
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

   `-R` sets the data directory (or `"env": {"ROOT_DIR": "..."}`), `-d`
   the default database. `cachydb docs mcp` has config examples for
   Claude Code, Claude Desktop, Cursor, and VS Code.

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
  requests are still in flight, the MCP SDK drops their responses (the
  process still exits 0). Append a `sleep`.
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
session) starts on the default database (`main`, or `-d`) again.

Self-description to check:

```JSON
{"jsonrpc":"2.0","id":11,"method":"resources/list","params":{}}
{"jsonrpc":"2.0","id":12,"method":"resources/read","params":{"uri":"cachydb://docs/query"}}
{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"find_documents","arguments":{"collection":"events","query":{"filters":[{"field":"n","operator":"like","value":1}]}}}}
```

The `initialize` result carries `instructions`, and `tools/list` shows
typed input/output schemas with annotations; the last call fails with a
`validating "arguments"` error listing the valid operators. Without an MCP
client, `cachydb docs tools --format json` prints the same tool schemas.

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
