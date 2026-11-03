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
| `internal/cmd/` | Commands, flags, lifecycle (`signal.NotifyContext`, `RunE`); `client.go` (MCP client), `export.go`/`import.go`/`find.go` (offline data) | New commands or flags |
| `internal/exportfmt/` | JSONL export format, `Export`/`Import` over `Source`/`Target`, engine adapter | Export format changes (bump `Version`) |
| `internal/config/` | Env-var settings, `ByteSize` | New settings |
| `internal/app/` | `Builder` → `db.Options`, `App.Start/Stop` | Startup/shutdown, new engine options |
| `internal/mcp/server.go` | Handlers, sessions, transports | Exposing engine operations to AI clients |
| `internal/mcp/tools.go`, `docs.go` | Tool definitions (typed schemas, annotations, examples); instructions, docs resources, generated tools topic | New tools, tool descriptions |
| `internal/docs/` | Embedded agent docs topics (`topics/*.md`) | Documentation an agent needs |
| `pkg/db/engine.go`, `database.go`, `collection.go` | Public API, metadata operations | API changes |
| `pkg/db/document.go` | Write path (`UpdateWith`, `Replace`, `DeleteWith`, conditions), read path, candidate selection | Document operations |
| `pkg/db/query.go` | `Find` (OR groups, sort heap, projection), `CountDocuments`, `Distinct` | Query execution |
| `pkg/db/compare.go`, `index.go` | Comparison, operators, `$now`; index keys (per element, marker keys) | New operators, index behavior |
| `pkg/db/types.go`, `schema.go` | Public types, schema validation | New field types |
| `pkg/db/flush.go`, `recovery.go`, `compact.go` | Flush, recovery, compaction | Durability logic (read [STORAGE.md](STORAGE.md) first) |
| `pkg/db/backpressure.go`, `metacp.go` | `MaxPendingBytes` write backpressure; `cachydb.metacp` metadata checkpoint | Durability logic |
| `pkg/db/meta.go`, `format.go` | Meta files, format file, old-format detection | On-disk layout changes |
| `pkg/db/migration.go` | Schema-version migrations | New migrations |
| `pkg/db/options.go` | `Options` and defaults | Tuning |
| `pkg/db/internal/wal/` | WAL format, group commit, retention | WAL changes |
| `pkg/db/internal/segment/` | Data segments, hints, index snapshots | Data format changes |
| `pkg/db/internal/fsutil/`, `cache/` | Atomic writes, dir fsync, flock; LRU | Rarely |
| `Dockerfile`, `.dockerignore` | Official image (multi-stage, distroless, non-root, `/data`) | Base-image bumps, build changes; keep the `go:embed` sources in the context |
| `.github/workflows/docker.yml` | Image lint, smoke test, multi-arch publish, Docker Hub overview sync | Release process changes |
| `DOCKERHUB.md`, `docker-compose.yml` | Docker Hub overview (< 25,000 bytes), Compose example | Docker usage changes |

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

1. Define typed input and output structs in `internal/mcp/tools.go` with
   `json` and `jsonschema` (description) tags. Use a named string type plus
   an entry in `schemaOptions` for an enum.
2. Write a typed handler in `server.go`:
   `func (s *Server) xTool(ctx, req *mcp.CallToolRequest, in XInput) (*mcp.CallToolResult, XOutput, error)`
   that calls engine methods only. Resolve the database with
   `s.databaseName(req.Session, in.Database)` or `s.getCollection(...)`.
3. Return errors through `toolError(action, err)`; return the output with
   `Success: true`; use `nonNil` for lists so empty results are `[]` (the
   output schema is validated).
4. Add a `newSpec(...)` entry to `toolSpecs` with a 1–3 sentence
   description, annotations (`readOnly(...)` or `writing(...)`), and an
   example call. Registration, `ToolDefinitions()`, the `tools` docs topic,
   and the MCP resources follow from it.
5. Update `TestListToolsUnchanged`, the annotation table in
   `selfdesc_test.go`, the example order in `TestToolExamplesAreValid`,
   the docs topics that mention tools, and the README.

### Adding a query operator

1. Add a case to `matchesFilter` in `pkg/db/compare.go`, using `compare` /
   `valuesEqual` for type-aware semantics.
2. Index use is decided in `filterCandidatesLocked` (`document.go`): `eq`,
   `has`, `has_any`, `has_all` can; candidates are always re-checked, so
   they only need to be a superset. Decide whether the new operator can.
3. Add it to `Operators` in `internal/mcp/tools.go` (the schema enum) and
   to the `query` docs topic.
4. Add cases to `compare_test.go` / `query_test.go`.

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
being converted. Sidecars that can be rebuilt (index snapshots, plan 004:
version 2) are simply rejected and rebuilt. Never change the numeric
values of WAL ops (1–10) or record types; new metadata ops need a replay
case in `replayRecord` and crash tests at `hookMetaWrite`. The upgrade
fixtures (`TestCompatFixtures`) must keep passing; a new decoder gets a
fuzz target next to it.

### Adding a migration

Register it in an `init` with `db.RegisterMigration(from, fn)` and bump
`CurrentSchemaVersion`. The function runs on an opened engine and must be
idempotent (see [DATABASE_ENGINE.md](DATABASE_ENGINE.md#migrations)).

### Adding a CLI command or setting

Add the command in `internal/cmd/` with `RunE` and register it in `init`.
Give it an `Example` (a test requires one on every command). Utility
commands open the engine through `withEngine`. A new engine setting needs a
`Config` field with an `envconfig:"NAME"` tag (`envconfig` ignores `env:`
tags), a flag in `setEngineFlags` whose default is the configured value
(flag > env > default) and whose usage names the env var, a `Builder`
method that maps it into `db.Options`, and a row in
`internal/docs/topics/config.md`.

### Changing the agent docs

Topics live in `internal/docs/topics/<name>.md` with front matter (`name`,
`title`, `summary`); the order is `docs.Names`. Tests check that every
topic parses and that every `cachydb docs <topic>` /
`cachydb://docs/<topic>` reference (in topics, help text, and the server
instructions) exists. Never hand-write tool reference content: the `tools`
topic is generated from `toolSpecs`.

## Testing

Tests use the stdlib `testing` package: table-driven with `t.Run`, data in
`t.TempDir()`, no assertion libraries.

```bash
go test -race ./...                                          # everything (about a minute)
go test -race ./pkg/db/...                                   # engine only
go test -race -count=200 -run 'Crash|Recovery|Torn|Concurrent|Condition|Rename|Schema|Model|Compat' ./pkg/db/...   # stress (slow)
test -z "$(gofmt -l .)" && go vet ./... && go mod tidy -diff
for os in linux darwin freebsd openbsd netbsd dragonfly solaris illumos windows; do GOOS=$os go build ./... || echo "FAIL $os"; done
GOOS=aix GOARCH=ppc64 go build ./...                         # aix exists only on ppc64
go test -coverpkg=./... -coverprofile=cover.out ./... && go tool cover -func=cover.out | tail -1   # CI floor: 85.0%
```

Heavy layers, opt-in (all run nightly in CI; details and commands in
[TESTING.md](../TESTING.md)):

| Layer | Where | Run | Knobs |
| --- | --- | --- | --- |
| Fuzzing | `fuzz_test.go` in `pkg/db`, `pkg/db/internal/{wal,segment}`, `internal/exportfmt` (11 targets) | `go test -run '^$' -fuzz '^FuzzX$' -fuzztime 30s ./<pkg>` | `-fuzztime`; crashers go to `testdata/fuzz/<Target>/` and are committed |
| Model test | `pkg/db/model_test.go` (`TestModel`) | `go test -run TestModel ./pkg/db/` (2 seeds × 300 ops by default) | `CACHYDB_MODEL_SEEDS`, `CACHYDB_MODEL_OPS`, `CACHYDB_MODEL_SEED` (reproduce) |
| Upgrade fixtures | `pkg/db/testdata/compat/<version>/`, `pkg/db/compat_test.go`, generator `test/compat/gen` | part of `go test ./...` | none; one fixture per release |
| Kill test | `test/e2e/kill_test.go` (build tag `e2e`) | `TMPDIR=$HOME/.cache/cachydb-e2e go test -tags e2e -run TestKill ./test/e2e/` (real disk) | `CACHYDB_E2E_ROUNDS` (default 10), `CACHYDB_E2E_ARTIFACTS` |

CI: `.github/workflows/test.yml` (every pull request and push to
`master`: lint incl. `go mod tidy -diff`, `go test -race` and the quick
stress loop on the `go.mod` minimum and stable Go, on Linux only (macOS
runners are too slow; darwin is checked by the cross-build), cross-builds,
the 85.0% coverage floor) and
`.github/workflows/nightly.yml` (03:17 UTC: 200-run stress, 100 kill
rounds, 20 × 5000 model operations, 60s per fuzz target; failure
artifacts). Keep the default `go test -race ./...` at about a minute:
put anything heavy behind the `e2e` tag or an env knob.

What exists:

- `pkg/db/*_test.go`: regression tests for the investigated defects
  (`TestRecoveryRegressionF2…`, `TestTornTailsRegressionF4`, …), the
  crash-point matrix (`crash_test.go`, via the unexported test hook set in
  `export_test.go`; on failure the crash hook trace, the data-directory
  listing, and a line diff of the state are logged), flush, compaction
  (including small-segment merges), recovery, the metadata checkpoint
  (`metacp_test.go`), backpressure and the parallel flusher
  (`backpressure_test.go`), the decoded cache (clone safety), query/index
  semantics, names, options, format detection; plan 004: `$now`,
  `SchemaError`, conditional writes (incl. 16-goroutine compare-and-set),
  unset, replace, upsert (`writes_test.go`), element operators, OR groups,
  element index keys, the old-snapshot upgrade, sort, projection, count,
  distinct (`query_test.go`), describe, `UpdateSchema`, rename (crash after
  the record, crash between the directory rename and the meta write, with
  and without earlier collections of the new name and the metadata
  checkpoint, concurrent writers), copy (`collops_test.go`).
- `internal/exportfmt`: exact format, round trip, conflict modes, bad
  input.
- `pkg/db/internal/{wal,segment,cache,fsutil}`: codec, torn-tail,
  retention, sidecar, LRU, and lock tests.
- `internal/mcp`: tool list, per-session `use_database` (in-memory
  transports and HTTP), end-to-end tool flow, errors (`server_test.go`);
  schema enums, annotations, output schemas, `ToolDefinitions` vs. the
  registered tools and the tools topic, valid examples, instructions over
  both handshakes, docs resources, helpful argument errors
  (`selfdesc_test.go`); stdio EOF handling (`stdio_test.go`).
- `internal/docs`: topic parsing, order, and cross-references.
- `internal/app`, `internal/cmd`, `internal/config`: builder options, lock
  behavior, utils output, flags, env-var precedence (flag > env >
  default), `docs` output (index, topics, `--all`, JSON, unknown topic),
  `version --json`, help examples, `ByteSize` parsing, `healthcheck`
  (healthy, 503, refused, timeout, stdio, `healthcheck_test.go`),
  `utils export/import/find/get` and the client hint on a locked
  directory (`data_test.go`), `cachydb client` against a live HTTP server
  (`client_test.go`: tools, call, export equal to `utils export`,
  import).
- `internal/mcp/features_test.go`: every plan-004 tool and parameter end
  to end.
- `/healthz` (200, 405, 503 after `Engine.Close`): `TestHealthz` in
  `internal/mcp/server_test.go`.
- Plan 005 (test hardening): fuzz targets (`fuzz_test.go`), the model
  test (`pkg/db/model_test.go`), the upgrade fixtures
  (`pkg/db/compat_test.go`, `test/compat/gen/main_test.go` checks the
  generator on the current engine), compaction failure paths and
  `mapWALErr` (`pkg/db/compact_errors_test.go`), `import --on-conflict
  replace` with a different schema and indexes (`internal/exportfmt/replace_test.go`
  offline, `TestClientImportReplace` through a server), every CLI command
  run in-process through the root command including `app` over HTTP and
  exit statuses (`internal/cmd/wrappers_test.go`; `Execute` delegates to
  `execute`, which returns the status), and the kill test
  (`test/e2e`).
- The Docker image is tested only in CI: the `docker` workflow's
  pull-request run builds it and runs the smoke test (see
  [Releasing](#releasing)).

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

## Releasing

Releases are git tags `vX.Y.Z`. Pushing one runs
`.github/workflows/docker.yml`, which publishes the official image
`hovhannespalyan/cachydb` to Docker Hub:

```bash
git tag v0.0.4 && git push origin v0.0.4
```

The workflow, in order:

1. Validates the tag (`vX.Y.Z`, optional `-prerelease`) and lints the
   `Dockerfile` with hadolint.
2. Builds `linux/amd64` with `VERSION=<tag>` (stamped into
   `cmd.Version`) and smoke-tests it without secrets: user 65532, healthy
   within 30s, `GET /healthz`, MCP `initialize` over HTTP,
   `version --json` reports the tag, `docker stop` exits 0 within 15s, and
   an `initialize` over stdio.
3. Logs in to Docker Hub and pushes `linux/amd64` + `linux/arm64` as
   `X.Y.Z`, `X.Y` and `latest` (pre-releases get only `X.Y.Z-pre`), with
   provenance (`mode=max`) and SBOM attestations.
4. Syncs `DOCKERHUB.md` to the Docker Hub overview (short description
   "Lightweight document database with a built-in MCP server").

Pull requests that touch the Dockerfile, the workflow, Go code or the
embedded topics run steps 1–2 only; nothing is pushed and no secrets are
used, so fork PRs run them too.

**Required repository secrets** (Settings > Secrets and variables >
Actions): `DOCKERHUB_USERNAME` (`hovhannespalyan`) and `DOCKERHUB_TOKEN`, a
Docker Hub personal access token with **Read, Write, Delete** scope (the
overview sync needs Delete). A missing secret fails the run before any
push, with the secret's name in the error.

**Before tagging**: CI (`test` workflow) must be green on the release
commit, and the latest `nightly` run too (or run its heavy tests
locally, see [Testing](#testing)).

**Upgrade fixture (every release)**: after tagging `vX.Y.Z`, generate the
release's fixture with its own code and commit it as
`pkg/db/testdata/compat/vX.Y.Z/`, using `test/compat/gen` in a git
worktree of the tag (the commands are in
[`pkg/db/testdata/compat/README.md`](../pkg/db/testdata/compat/README.md)).
`TestCompatFixtures` then checks that every later version opens it and
reads identical data. Never edit or regenerate an existing fixture; if a
format change makes an old fixture unreadable, that is a compatibility
break to handle (migration or an `ErrIncompatibleDataDir` message), not a
fixture to update. Keep the generator on the engine API that every
release since v0.0.4 has.

**Re-publishing**: Actions > docker > Run workflow, with input `tag` (for
example `v0.0.4`), rebuilds and pushes an existing tag. The tag must
already contain the `Dockerfile` (v0.0.4 and later).

**Maintenance**: the base images in the `Dockerfile` are pinned by digest
(the comments name the tags). Bump the digests, and the Go builder version
with `go.mod`, when updating them; the action versions in the workflow are
pinned to major tags (hadolint-action to a full version, it has no major
tag). Keep `DOCKERHUB.md` under 25,000 bytes (Docker Hub's limit).

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `github.com/modelcontextprotocol/go-sdk` | v1.8.0 | MCP server/client SDK |
| `github.com/google/jsonschema-go` | v0.4.3 | Tool input/output schemas (the SDK's schema package) |
| `github.com/klauspost/compress` | v1.20.3 | zstd for data segments |
| `github.com/spf13/cobra` | v1.10.2 | CLI framework |
| `github.com/kelseyhightower/envconfig` | v1.4.0 | Env-var configuration |
| `github.com/google/uuid` | v1.6.0 | Document ids |

## Known Limitations

1. An idle collection pins the WAL replay start at its last checkpoint
   (`cachydb.metacp` bounds only the metadata records).
2. `create_index` blocks writes to the whole database while it builds.
3. Only `eq`, `has`, `has_any`, and `has_all` use indexes; `contains`,
   range filters, `any` groups, and unindexed fields scan every document
   (from the memtable, the decoded-document cache, or disk). Sorting by a
   field other than `_id` reads every match.
4. No multi-document transactions; no range, unique, full-text, or
   nested-field indexes; no dot-notation in queries. `copy_collection` is
   not atomic.
5. `update_schema` and `create_index` hold the database's metadata lock
   while they read every document. `rename_collection` flushes the
   collection under it.
6. The exact object `{"$now": true}` cannot be stored literally (it
   becomes a timestamp).
7. No authentication or TLS on the HTTP transport (`/mcp` and `/healthz`).
   `utils export` and `client export` hold a whole collection's documents
   in memory at once (offline) or page with `skip` (client).
8. Single node only. The data-directory lock needs `flock` (Linux, macOS,
   the BSDs); elsewhere (Windows, Solaris, illumos, AIX) it is a no-op with
   a logged warning, and Windows is unsupported at runtime.
9. A single durable insert is fsync-bound (a few ms); throughput comes
   from concurrent writers sharing fsyncs. There is no relaxed-durability
   mode.
10. stdio: closing stdin exits 0, but the SDK drops the responses of
    requests still in flight.
