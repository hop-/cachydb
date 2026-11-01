# Development Guide

Guide for AI agents and developers working on CachyDB. Overview:
[PROJECT.md](PROJECT.md).

## Prerequisites

- Go 1.25+ (`go.mod`: `go 1.25.5`)
- Linux or macOS. Windows must still compile (`GOOS=windows go build ./...`)
  but is not supported at runtime.
- No external services.

## Quick Start

```bash
go build -o cachydb                       # build
./cachydb -R /tmp/cachydb-dev             # stdio transport, scratch data dir
./cachydb -t http -p 7601 -R /tmp/cachydb-dev   # HTTP transport at /mcp
go test -race ./...                       # all tests
```

## Project Layout & Ownership

| Path | Purpose | When to modify |
|------|---------|----------------|
| `main.go` | Entry point | Almost never |
| `internal/cmd/` | Commands, flags, lifecycle (`signal.NotifyContext`, `RunE`) | New commands or flags |
| `internal/config/` | Env-var settings, `ByteSize` | New settings |
| `internal/app/` | `Builder` → `db.Options`, `App.Start/Stop` | Startup/shutdown, new engine options |
| `internal/mcp/server.go` | Tools, sessions, transports | Exposing engine operations to AI clients |
| `pkg/db/engine.go`, `database.go`, `collection.go` | Public API, metadata operations | API changes |
| `pkg/db/document.go` | Write path, read path, `Find` | Document operations, query execution |
| `pkg/db/compare.go`, `index.go` | Comparison and index semantics | New operators, index behavior |
| `pkg/db/types.go`, `schema.go` | Public types, schema validation | New field types |
| `pkg/db/flush.go`, `recovery.go`, `compact.go` | Flush, recovery, compaction | Durability logic (read [STORAGE.md](STORAGE.md) first) |
| `pkg/db/meta.go`, `format.go` | Meta files, format file, old-format detection | On-disk layout changes |
| `pkg/db/migration.go` | Schema-version migrations | New migrations |
| `pkg/db/options.go` | `Options` and defaults | Tuning |
| `pkg/db/internal/wal/` | WAL format, group commit, retention | WAL changes |
| `pkg/db/internal/segment/` | Data segments, hints, index snapshots | Data format changes |
| `pkg/db/internal/fsutil/`, `cache/` | Atomic writes, dir fsync, flock; LRU | Rarely |

## Rules

Follow the Go conventions used throughout the code:

- `ctx context.Context` is the first parameter and is never stored in a
  struct. Engine methods that write take a `ctx`.
- Check every error and wrap it with context (`fmt.Errorf("…: %w", err)`).
  Return the sentinels from `pkg/db/errors.go` (wrapped) so callers can use
  `errors.Is`.
- No panics in library code.
- Every goroutine has an exit path and is joined on `Close` (engine
  background work uses `Engine.spawn` / `bgCtx`).
- **Nothing in `pkg/db` writes to stdout.** Log through `Options.Logger`
  (`slog`). In stdio mode stdout is the MCP channel; the binary logs to
  stderr.
- Never hold `Collection.mu` across file I/O. Respect the lock order in
  [STORAGE.md](STORAGE.md#concurrency-and-lock-order).
- Format with `gofmt`; lint with `go vet` (there is no golangci-lint
  config).

## Key Patterns

### Adding an MCP tool

1. Define an input struct in `internal/mcp/server.go` with `json` and
   `jsonschema` tags (the SDK generates the input schema from it).
2. Write a typed handler
   `func (s *Server) xTool(ctx, req *mcp.CallToolRequest, in XInput) (*mcp.CallToolResult, map[string]interface{}, error)`
   that calls engine methods only. Resolve the database with
   `s.databaseName(req.Session, in.Database)` or `s.getCollection(...)`.
3. Return errors through `toolError(action, err)`; return
   `map[string]interface{}{"success": true, ...}` on success; use
   `nonNil` for lists so empty results are `[]`.
4. Register it in `registerTools` with `mcp.AddTool(server, &mcp.Tool{Name, Description}, s.xTool)`.
5. Update `TestListToolsUnchanged` in `internal/mcp/server_test.go`, the
   docs, and the README.

### Adding a query operator

1. Add a case to `matchesFilter` in `pkg/db/compare.go`, using `compare` /
   `valuesEqual` for type-aware semantics.
2. Only `eq` uses indexes (`candidates` in `document.go`); decide whether
   the new operator can.
3. Add cases to `compare_test.go` / `document_test.go`.

### Adding a field type

1. Add the `FieldType` constant in `pkg/db/types.go` and a case in
   `ValidateType`; add it to `Schema.Validate` in `schema.go`.
2. Remember validation sees the document after a JSON round-trip (numbers
   are `float64`, arrays `[]any`, objects `map[string]any`).

### Changing an on-disk format

The WAL, data segment, hint, and snapshot headers carry a version; the data
directory has `cachydb.format` (`FormatVersion` in `pkg/db/format.go`).
Bump the relevant version and decide how old files are handled: old data
directories currently fail fast with `ErrIncompatibleDataDir` rather than
being converted. Never change the numeric values of WAL ops or record
types.

### Adding a migration

Register it in an `init` with `db.RegisterMigration(from, fn)` and bump
`CurrentSchemaVersion`. The function runs on an opened engine and must be
idempotent (see [DATABASE_ENGINE.md](DATABASE_ENGINE.md#migrations)).

### Adding a CLI command or setting

Add the command in `internal/cmd/` with `RunE` and register it in `init`.
Utility commands open the engine through `withEngine`. A new engine setting
needs a `Config` field with an `envconfig:"NAME"` tag (not `env:`, see
[CLI.md — Known issues](CLI.md#known-issues)), a flag in `setEngineFlags`,
and a `Builder` method that maps it into `db.Options`.

## Testing

Tests use the stdlib `testing` package: table-driven with `t.Run`, data in
`t.TempDir()`, no assertion libraries.

```bash
go test -race ./...                                          # everything
go test -race ./pkg/db/...                                   # engine only
go test -race -count=20 -run 'Crash|Recovery|Torn|Concurrent' ./pkg/db/...   # stress recovery
test -z "$(gofmt -l .)" && go vet ./...
GOOS=windows go build ./... && GOOS=darwin go build ./...    # cross-compile check
```

What exists:

- `pkg/db/*_test.go`: regression tests for the investigated defects
  (`TestRecoveryRegressionF2…`, `TestTornTailsRegressionF4`, …), the
  crash-point matrix (`crash_test.go`, via the unexported test hook set in
  `export_test.go`), flush, compaction, recovery, query/index semantics,
  names, options, format detection.
- `pkg/db/internal/{wal,segment,cache,fsutil}`: codec, torn-tail,
  retention, sidecar, LRU, and lock tests.
- `internal/mcp/server_test.go`: tool list, per-session `use_database`
  (in-memory transports and HTTP), end-to-end tool flow, errors.
- `internal/app`, `internal/cmd`, `internal/config`: builder options, lock
  behavior, utils output, flags, `ByteSize` parsing.

### Benchmarks

`pkg/db/bench_test.go` has `BenchmarkInsertParallel` (group commit),
`BenchmarkFlushOneChange` (flush cost vs. collection size),
`BenchmarkGet` (cached/uncached), and `BenchmarkFind` (indexed/scan):

```bash
TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench . -benchmem ./pkg/db/...
```

Run them on a real disk: if `$TMPDIR` is a tmpfs, fsync is free and the
numbers say nothing about durability costs.

### Manual and MCP testing

See [TESTING.md](../TESTING.md) (MCP Inspector, the example client, stdio
smoke tests, crash recovery).

## Common Tasks

- **Reset local data**: stop the server, then `rm -rf ~/.cachydb` (or the
  `-R` directory).
- **Inspect a data directory**: `cachydb utils list -c -R DIR` (fails while
  a server holds the lock). The files are binary; their layouts are in
  [STORAGE.md](STORAGE.md).
- **Debug over HTTP**: `./cachydb -t http -p 7601 -R /tmp/cdb`, then point
  the MCP Inspector or `examples/mcp-client` at it. Plain `curl` needs the
  Streamable HTTP handshake (initialize, then the `Mcp-Session-Id` header).

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `github.com/modelcontextprotocol/go-sdk` | v1.2.0 | MCP server/client SDK |
| `github.com/klauspost/compress` | v1.20.3 | zstd for data segments |
| `github.com/spf13/cobra` | v1.10.2 | CLI framework |
| `github.com/kelseyhightower/envconfig` | v1.4.0 | Env-var configuration |
| `github.com/google/uuid` | v1.6.0 | Document ids |

## Known Limitations

1. `ROOT_DIR`, `DB_NAME`, and `TRANSPORT` env vars have no effect; see
   [CLI.md — Known issues](CLI.md#known-issues).
2. `create_index` blocks writes to the whole database while it builds.
3. Only `eq` uses indexes; range filters and unindexed fields scan every
   document from disk/cache; the per-document cost is dominated by JSON
   decoding, even on a cache hit.
4. No multi-document transactions; no range, unique, or nested-field
   indexes; no dot-notation in queries.
5. Schemas cannot be changed after collection creation.
6. No MCP tools for dropping collections or indexes (engine supports both).
7. No authentication or TLS on the HTTP transport.
8. Single node only; Windows unsupported (no data-dir lock).
9. A single durable insert is fsync-bound (a few ms); throughput comes
   from concurrent writers sharing fsyncs. There is no relaxed-durability
   mode.
10. stdio: closing stdin while requests are in flight drops their responses
    and exits 1.
