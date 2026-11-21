# Development Guide

Guide for AI agents and developers working on CachyDB. Overview:
[PROJECT.md](PROJECT.md).

## Prerequisites

- Go 1.27+ (`go.mod`: `go 1.27.0`)
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
| `pkg/db/document.go` | Write path (`UpdateWith`, `Replace`, `DeleteWith`, conditions), read path | Document operations |
| `pkg/db/query.go`, `plan.go` | `Find` (OR groups, sort heap, projection), `CountDocuments`, `Distinct`; snapshots, drivers, plan choice | Query execution |
| `pkg/db/compare.go`, `index.go`, `unique.go` | Compiled operators, `$now`; index entries per document version, index builds; unique indexes | New operators, index behavior |
| `pkg/db/types.go`, `schema.go` | Public types, schema validation | New field types |
| `pkg/db/flush.go`, `recovery.go`, `upgrade.go`, `compact.go` | Flush (segment append + index transaction), recovery and index-file rebuild, format 2 → 3 upgrade, compaction | Durability logic (read [STORAGE.md](STORAGE.md) first) |
| `pkg/db/backpressure.go`, `metacp.go` | `MaxPendingBytes` write backpressure; `cachydb.metacp` metadata checkpoint | Durability logic |
| `pkg/db/meta.go`, `format.go` | Meta files, format file, old-format detection | On-disk layout changes |
| `pkg/db/migration.go` | Schema-version migrations | New migrations |
| `pkg/db/options.go` | `Options` and defaults | Tuning |
| `pkg/db/internal/wal/` | WAL format, group commit, retention | WAL changes |
| `pkg/db/internal/segment/` | Data segments, full and header-only scans | Data format changes |
| `pkg/db/internal/idxstore/` | `index.db` (the only bbolt importer): buckets, key encoding, counters, `Check` | Index-file layout (bump its layout `formatVersion`) |
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
- Never hold `Collection.mu` across file I/O or an `index.db` transaction,
  and never hold an `index.db` read transaction across a WAL append or while
  starting an update. Respect the lock order in
  [STORAGE.md](STORAGE.md#concurrency-and-lock-order).
- Format with `gofmt`; lint with `go vet` and `staticcheck` (there is no
  golangci-lint config). `staticcheck` is pinned and run with `go run`, so
  it is not in `go.mod`:
  `go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...` (Staticcheck
  2026.2.2; CI runs the same command, so change the version in
  `.github/workflows/test.yml` too). Fix what it reports; suppress a
  finding only with a `//lint:ignore <check> <reason>` comment.

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

1. Add an `opCode`, its name in `operatorNames` and `operatorCodes`, and
   its compilation in `compileFilter` (`pkg/db/compare.go`): normalize the
   value once into `compiledFilter`, and return an error for a value the
   operator cannot take. If some values can match no document at all,
   report it in `compiledFilter.never`.
2. Add its case to `compiledFilter.match`, comparing through
   `filterValue.equals` / `compareTo` for type-aware semantics. Only
   `exists` looks at a missing field; keep that rule.
3. Decide whether an index can serve it in `filterOption`
   (`pkg/db/plan.go`): a value group (`probeOption`) or a value range
   (`rangeOption`, `rangeOf`). Candidates are always re-checked, so they
   only need to be a superset; the overlay rule (`planOption.cand`) must
   pick the same documents as the entries would. If counts can be exact
   from the entries, add it to `countExact` (`pkg/db/query.go`).
4. Add it to `Operators` in `internal/mcp/tools.go` (the schema enum) and
   to the `query` docs topic.
5. Add cases to `compare_test.go` / `query_test.go` and to the reference
   model (`model_test.go`: `modelOps`, `mMatch`).

### Adding a field type

1. Add the `FieldType` constant in `pkg/db/types.go` and a case in
   `ValidateType`; add it to `Schema.Validate` in `schema.go`.
2. Remember validation sees the document after a JSON round-trip (numbers
   are `float64`, arrays `[]any`, objects `map[string]any`).

### Changing an on-disk format

The WAL and data segment headers carry a version, `index.db` has a layout
version in its `meta` bucket (`formatVersion` in
`pkg/db/internal/idxstore`), and the data directory has `cachydb.format`
(`FormatVersion` in `pkg/db/format.go`, currently 3). Bump the relevant
version and decide how old files are handled:

- `index.db` is derived data: an index file with another layout version
  fails the open check and is rebuilt from the segments with a WARN, so a
  layout change needs no converter (but costs a rebuild on first open).
- A `FormatVersion` bump needs an upgrade step: `checkDataDir` reports the
  previous version as upgradable and `recover` runs the step after reading
  the metadata (`upgradeFormat` in `pkg/db/upgrade.go` is the 2 → 3 step:
  build every `index.db`, then delete the old files, then write the format
  file last, so a crash restarts it). Versions older than the one before
  fail with `ErrIncompatibleDataDir`; pre-v0.0.4 directories have no
  converter at all.

Never change the numeric values of WAL ops (1–10) or record types (a reader
still accepts type 3, the old checkpoint marker); new metadata ops need a
replay case in `replayRecord` and crash tests at `hookMetaWrite`. The
upgrade fixtures (`TestCompatFixtures`, one per release) must keep
passing, through the upgrade; a new decoder gets a fuzz target next to
it.

### Adding a migration

Register it in an `init` with `db.RegisterMigration(from, fn)` and bump
`CurrentSchemaVersion`. The function runs on an opened engine and must be
idempotent (see [DATABASE_ENGINE.md](DATABASE_ENGINE.md#migrations)).

### Adding a CLI command or setting

Add the command in `internal/cmd/` with `RunE` and register it in `init`.
Give it an `Example` (a test requires one on every command). Utility
commands open the engine through `withEngine`. A new engine setting needs a
`Config` field, its default in `defaults()` and a `fromEnv("NAME", ...)`
line in `Init` (`internal/config/config.go`; no struct tags), a flag in
`setEngineFlags` whose default is the configured value
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
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...       # pinned; also in CI
for os in linux darwin freebsd openbsd netbsd dragonfly solaris illumos windows; do GOOS=$os go build ./... || echo "FAIL $os"; done
GOOS=aix GOARCH=ppc64 go build ./...                         # aix exists only on ppc64
go test -coverpkg=./... -coverprofile=cover.out ./... && go tool cover -func=cover.out | tail -1   # CI floor: 85.0%
```

Heavy layers, opt-in (all run nightly in CI; details and commands in
[TESTING.md](../TESTING.md)):

| Layer | Where | Run | Knobs |
| --- | --- | --- | --- |
| Fuzzing | `fuzz_test.go` in `pkg/db`, `pkg/db/internal/{wal,segment}`, `internal/exportfmt` (9 targets) | `go test -run '^$' -fuzz '^FuzzX$' -fuzztime 30s ./<pkg>` | `-fuzztime`; crashers go to `testdata/fuzz/<Target>/` and are committed |
| Model test | `pkg/db/model_test.go` (`TestModel`) | `go test -run TestModel ./pkg/db/` (2 seeds × 300 ops by default) | `CACHYDB_MODEL_SEEDS`, `CACHYDB_MODEL_OPS`, `CACHYDB_MODEL_SEED` (reproduce) |
| Upgrade fixtures | `pkg/db/testdata/compat/<version>/`, `pkg/db/compat_test.go`, generator `test/compat/gen` | part of `go test ./...` | none; one fixture per release |
| Kill test | `test/e2e/kill_test.go` (build tag `e2e`) | `TMPDIR=$HOME/.cache/cachydb-e2e go test -tags e2e -run TestKill ./test/e2e/` (real disk) | `CACHYDB_E2E_ROUNDS` (default 10), `CACHYDB_E2E_ARTIFACTS` |
| Scale benchmark | `pkg/db/scale_bench_test.go` (`BenchmarkScale`, `BenchmarkScaleCollections`); nightly runs 1M, report only | see [Scale benchmark](#scale-benchmark) | `CACHYDB_SCALE_DOCS`, `CACHYDB_SCALE_UUID_IDS`, `CACHYDB_SCALE_UPDATE_SECONDS`, `CACHYDB_SCALE_COLLECTIONS` |

CI: `.github/workflows/test.yml` (every pull request and push to
`master`: lint incl. `staticcheck` and `go mod tidy -diff`, `go test -race` and the quick
stress loop on the `go.mod` minimum and stable Go, on Linux only (macOS
runners are too slow; darwin is checked by the cross-build), cross-builds,
the 85.0% coverage floor) and
`.github/workflows/nightly.yml` (03:17 UTC: 200-run stress, 100 kill
rounds, 20 × 5000 model operations, 60s per fuzz target, the 1M scale
benchmark (reported, not gated); failure artifacts). Keep the default `go test -race ./...` at about a minute:
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
  index-driven plans, sort, projection, count,
  distinct (`query_test.go`), describe, `UpdateSchema`, rename (crash after
  the record, crash between the directory rename and the meta write, with
  and without earlier collections of the new name and the metadata
  checkpoint, concurrent writers), copy (`collops_test.go`).
- `internal/exportfmt`: exact format, round trip, conflict modes, bad
  input.
- `pkg/db/internal/{wal,segment,cache,fsutil}`: codec, torn-tail,
  retention, header-only scan, LRU, and lock tests.
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

### Scale benchmark

`pkg/db/scale_bench_test.go` holds the opt-in scale benchmarks of plan 013
(on-disk indexes). They never run in the default `go test` path or in
`test.yml`; the nightly workflow runs `BenchmarkScale` at 1M documents and
only reports it. Knobs:

| Variable | Meaning |
| --- | --- |
| `CACHYDB_SCALE_DOCS=N` | run `BenchmarkScale` with N documents (unset: skipped) |
| `CACHYDB_SCALE_UUID_IDS=1` | 36-byte random (v4-shaped) ids instead of `doc-0000000`; the worst case for page fill |
| `CACHYDB_SCALE_UPDATE_SECONDS=S` | length of the sustained-update phase (default 20; 0 skips it) |
| `CACHYDB_SCALE_COLLECTIONS=K` | run `BenchmarkScaleCollections`: K collections of 1,000 documents, three indexes each |

```bash
CACHYDB_SCALE_DOCS=1000000 TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench 'Scale$' -benchtime 1x -timeout 30m ./pkg/db/
CACHYDB_SCALE_DOCS=10000000 TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench 'Scale$' -benchtime 1x -timeout 120m ./pkg/db/
CACHYDB_SCALE_COLLECTIONS=300 TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench 'ScaleCollections' -benchtime 1x -timeout 30m ./pkg/db/
```

`BenchmarkScale` loads N documents of about 330 bytes (JSON) through
`Insert` with 256 writers, with three indexes in place (`cat`: a string
with 100 values; `n`: a random number; `tags`: a two-element array, one
element of 50 values and one of `even`/`odd`), so the load goes through
the normal flusher. Then it closes, reopens, and reports: the file sizes,
the reopen time, the Go heap after the open (two forced GCs), each query
of investigation 003 table E2 (mean over up to 1,000 runs, values varied
per run, page cache warm), the heap after the queries, flushes under 64
writers doing random updates of two indexed fields (a flush every 1 s,
then every 5 s, with the background flusher held off so each flush is
timed), and, after evicting every file of the directory from the page
cache with `posix_fadvise(DONTNEED)` (no root needed; Linux amd64/arm64
only), the cold reopen and each query's first run. The report goes to
stdout; the targets of approach §11 (at 1M: each `limit 10` query by
`_id`, indexed value, range with a matching sort, and sort under 1 ms;
heap after open under 150 MiB; reopen under 1 s) print `met` or `MISSED`
but never fail the run. Data lives in `b.TempDir()` and is removed.

Recorded runs (reference laptop: i7-1165G7, 8 threads, 16 GB RAM, NVMe,
btrfs on LUKS, Go 1.27.4; single runs, 2026-11-15). "Before" is the
format-2 engine with keys and indexes in RAM (investigation 003, E2, 1M
documents of about 330 bytes, three indexes).

| Measure | Before, 1M (E2) | 1M, sequential ids | 1M, UUID ids | 10M, sequential ids |
| --- | --- | --- | --- | --- |
| Load (`Insert`, 256 writers, indexes in place) | 11.6k docs/s | 97.9 s (10,213 docs/s) | 243.9 s (4,101 docs/s) | 1,370.1 s (7,299 docs/s) |
| `index.db` after the load | (67 MiB of snapshots, keys and indexes in RAM) | 385.3 MiB (404 B/doc) | 534.3 MiB (560 B/doc) | 3,621.8 MiB (380 B/doc) |
| `index.db` after the update phase | — | 550.8 MiB (578 B/doc) | 701.3 MiB (735 B/doc) | 5,174.0 MiB (543 B/doc) |
| Data segments / WAL | 244 / 329 MiB | 267.4 / 339.9 MiB | 342.2 / 387.5 MiB | 2,683.8 / 984.9 MiB |
| Reopen after clean close | 2.2 s | 0.55 s | 0.25 s | 1.36 s |
| Go heap after open | 842 MiB | 16.5 MiB | 16.4 MiB | 16.6 MiB |
| Go heap after the queries (64 MiB cache full) | 555 MiB | 116.2 MiB | 113.6 MiB | 131.9 MiB |
| `_id eq` (point lookup) | 1 µs (at 100k) | 35 µs | 14 µs | 316 µs |
| no filter, `limit 10` (`_id` order) | 372 ms | 18 µs | 6 µs | 9 µs |
| indexed `eq` (1%), `limit 10` | 1.2 ms | 68 µs | 35 µs | 250 µs |
| array `has` (2%), `limit 10` | — | 46 µs | 26 µs | 104 µs |
| indexed 50% match (`has even`), `limit 10` | 95 ms | 43 µs | 20 µs | 39 µs |
| range with matching sort, `limit 10` | 7.3 s | 65 µs | 42 µs | 167 µs |
| sort desc, `limit 10` | 7.5 s | 36 µs | 14 µs | 19 µs |
| range (1%), `limit 10`, `_id` order | 7.3 s | 3.3 ms | 1.7 ms | 38 ms |
| count, one indexed `eq` (1%) | < 1 µs (at 100k) | 0.91 ms | 0.63 ms | 68 ms |
| count, two indexed filters | 106 ms | 93 ms | 62 ms | 6.5 s |
| `contains` (full scan, no match) | 7.3–7.5 s | 10.3 s | 6.5 s | 154 s |
| Cold cache: reopen | — | 0.64 s | 0.61 s | 1.95 s |
| Cold cache: `_id eq` / `limit 10` / `eq` / range+sort / sort | — | 1.0 / 0.7 / 5.1 / 6.4 / 4.8 ms | 1.0 / 2.5 / 5.7 / 9.4 / 5.2 ms | 2.2 / 1.2 / 8.2 / 8.8 / 7.9 ms |
| Flush, random updates, every 1 s | — | 6,755 docs in 0.97 s (150 µs/doc) | 5,747 docs in 1.61 s (285 µs/doc) | 18,011 docs in 43.0 s (1,349 µs/doc) |
| Flush, random updates, every 5 s | — | 26,030 docs in 3.04 s (96 µs/doc) | 25,332 docs in 3.48 s (127 µs/doc) | 16,359 docs in 27.8 s (1,698 µs/doc) |

Every §11 target is met at 1M with both id kinds (the five sub-millisecond
queries at 6–68 µs, heap after open 16.4–16.5 MiB, reopen 0.25–0.55 s). The
heap after open is mostly the process's own (the shared zstd encoder, the
test binary): it was 14–16 MiB before the open; the engine itself adds
1–2 MiB (bbolt's free list, per-collection state), also at 10M.

At 10M the heap stays flat (16.6 MiB after open; 131.9 MiB after the
queries, the 64 MiB document cache included) and the five queries stay
under 1 ms with a warm page cache (9–316 µs). Queries that read
documents not read before (`_id eq` on a new id each run, a new range
bound each run) cost 0.2–0.3 ms per document: the data segments (2.7 GB)
and `index.db` (3.6–5.2 GB) no longer fit this 16 GB laptop's page cache
together. Reopen took 1.36 s: the WAL had reached its 1 GiB retention cap
(a 3M run with the same WAL size reopened in 1.7 s), so a database of any
size whose WAL is at the cap reopens in about 1.2–1.7 s here; the 1 s
target holds only while the retained WAL is below about 600 MiB. A first
10M run of the same program (before a fix that made the benchmark drop
its loading engine before measuring the heap) gave: load 1,300 s,
`index.db` 4,094.7 MiB (429 B/doc) after the load and 4,775.6 MiB after
the updates, reopen 1.23 s, flushes of 5,145 and 24,506 random updates in
14.4 s and 24.2 s; its heap figure (169 MiB) included the closed loading
engine and is not valid. The UUID column comes from a run
before the heap fix described below (its heap figures may include up to
2 MiB of the closed loading engine). An earlier sequential 1M run, whose
update phase ran right after the cold-cache eviction, flushed 19,854 and
25,499 random updates in 9.3 s and 2.7 s: a cold index costs about three
times as much per update. Reopen time follows
the retained WAL, which `Open` reads and CRC-checks in full (340–390 MiB
here, at most `WALRetentionSize`, 1 GiB, by default), not the document
count. A full scan costs 6–15 µs per document (segment read, zstd, JSON;
the upper end at 10M, where the segments are read from disk). Counts that cannot be answered from one index read the
candidates of the smaller filter.

What the runs answered (plan 013 open questions):

- **Fill percent.** Kept bbolt's default 0.5 for the flush transactions,
  whose keys land at random places in the trees (random ids, random
  numbers), and 0.9 for index builds, which write sorted keys
  (`buildIndexes`). The alternative, 0.9 for the flushes too, was measured
  at 1M with UUID ids (a scratch build, not in the repository): the index
  file reached 1,014 MiB after the load (1,064 B/doc, against 534 MiB and
  560 B/doc at 0.5), the load took 359 s instead of 244 s, and flushes of
  random updates cost 320 and 231 µs per document instead of 285 and 127.
  A page filled to 90% splits again on the next random insert, so a high
  fill only pays for append-ordered keys.
- **Transaction size.** Kept one `index.db` transaction per flush, as large
  as the memtable it flushes (at most `MemtableSize`, 16 MiB, about 50,000
  documents of this size when measured, about half that since a memtable
  entry also counts the WAL payload it keeps for the flush; usually what
  one `FlushInterval` collected). The
  cost per document falls as transactions grow, because a page touched by
  several documents is written once and the fsync is shared: at 1M with
  UUID ids, 5,747 random updates took 1.61 s (285 µs each) and 25,332 took
  3.48 s (127 µs each). bbolt copies every page a transaction touches, so
  the file must hold one transaction's copies as free pages, and larger
  transactions grow it more (575 MiB after the 1 s flushes, 701 MiB after
  the 5 s flushes, from 534 MiB). Splitting a flush into several
  transactions would bound that growth but would cost more per document
  and would need a checkpoint per part; it was not needed for the targets.
- **Index bytes per document with UUID ids** (three indexes, four entries
  per document): 560 B after the load (418 B with the 11-byte sequential
  ids; the prototype of investigation 004 measured 454 B at 1M with short
  ids). Every key-table key and index entry stores the id, so the 25 extra
  bytes of a UUID cost about 28 B per entry (five per document here). After random updates the file also holds free pages: 735 B per
  document after the update phase of the UUID run.
- **A few hundred collections** (`BenchmarkScaleCollections`, 300
  collections of 1,000 documents with three indexes each): creating them
  took 74 s (900 `CreateIndex` calls, each holding the database lock and
  fsyncing), loading 300,000 documents 28 s. The 300 index files hold
  197.5 MiB (674 KiB each; a small file is mostly allocation slack). Close
  took 0.31 s and reopen 0.10 s; the heap after open was 18.3 MiB (10.5
  KiB per collection over the 15.2 MiB baseline); the process kept one
  open file per collection (its `index.db`; data segments open on first
  read) and a virtual size of 1.5 GiB from the 300 bbolt maps (address
  space, not memory). The first indexed query took 84 µs per collection;
  one flush pass with one changed document in each of the 300 collections
  took 416 ms with the default 4 flush workers. Each collection costs a
  file descriptor and a map, so thousands of collections need a raised
  `ulimit -n`.
- **Cold page cache** (measured without root: after a clean close every
  file of the directory is evicted with `posix_fadvise(DONTNEED)`, then the
  engine reopens): at 1M the reopen takes 0.5–0.6 s and the first `limit
  10` queries 0.7–9 ms instead of 6–71 µs (a disk read per B+tree level
  and per document); a 1% range in `_id` order 12–37 ms; a count of two
  indexed filters 2.3–2.4 s (it reads 10,000 documents from disk).
  At 10M: reopen 1.95 s; the first `limit 10` queries 1.0–8.8 ms; a 1%
  range in `_id` order 186 ms; a count of two indexed filters 25.6 s
  (100,000 documents read from disk).
- **Flush under sustained random updates at 10M.** With 64 writers
  updating `n` and `cat` of random documents (about 4,000 updates/s into
  the WAL), the flushes took 43.0 s for 18,011 documents and 27.8 s for
  16,359 (1.3–1.7 ms per document; 0.99–2.8 ms in the first run), about
  600–1,700 documents per second through the index, against 96–285 µs
  per document at 1M. Each random update dirties about five leaf pages
  (key table, two entries in each of two indexes) that a 10M tree no
  longer shares between updates, every page is copied and written at
  commit, and pages missing from the page cache are read one at a time
  inside the transaction. A writer faster than that fills the memtables
  until `MaxPendingBytes` makes it wait, so sustained random updates of
  indexed fields at this size run at the index's pace, well below the
  about 5,000 per second assumed when the design was accepted. Inserts in
  id order are much cheaper (the load averaged 7,300 documents per
  second).

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
| `github.com/klauspost/compress` | v1.20.4 | zstd for data segments |
| `go.etcd.io/bbolt` | v1.5.0 | B+tree file `index.db` (key table and indexes); imported only by `pkg/db/internal/idxstore`, pulls in only `golang.org/x/sys` |
| `github.com/spf13/cobra` | v1.10.2 | CLI framework |

Document ids come from the standard `uuid` package (UUID v7) and the
environment configuration from `os.Getenv`; neither needs a module.

## Known Limitations

1. An idle collection pins the WAL replay start at its last checkpoint
   (`cachydb.metacp` bounds only the metadata records).
2. `create_index` blocks writes to the whole database while it builds.
3. Indexes serve `eq`, `in`, `has*`, ranges, `starts_with`, `exists`, OR
   groups whose every group is served, and a single-key sort on an indexed
   field without arrays, objects, or cut strings. `contains`, `ne`,
   unindexed fields, and `_id` ranges walk the key table and read every
   document (a full scan: about 10 s at 1M documents, see
   [Scale benchmark](#scale-benchmark)). A multi-key sort, or a sort the
   index cannot drive, reads every match. Counts with two indexed filters
   read the candidates of the smaller one.
4. No multi-document transactions; no compound or full-text indexes; no
   array positions in field paths. `copy_collection` is not atomic.
5. `index.db` costs disk and write throughput: 380–560 bytes per document
   with three indexes after a load (more with long ids or values), more
   than the compressed documents, and up to about 750 with the free pages
   random updates leave. A flush applies its changes in one bbolt
   transaction, which copies every page it touches, so the file keeps
   about one transaction's worth of free pages and never shrinks (deleted
   documents free pages for reuse, not disk space). Random updates of indexed
   fields go through the index at a few thousand documents per second at
   1M and under 2,000 at 10M (laptop figures, see
   [Scale benchmark](#scale-benchmark)); faster writers wait on
   `MaxPendingBytes`. Query speed depends on the OS
   page cache: an uncached lookup costs a disk read per tree level.
6. Document ids are at most 31,742 bytes (65,535 before format 3).
7. `update_schema` and `create_index` hold the database's metadata lock
   while they read every document. `rename_collection` flushes the
   collection under it.
8. The exact object `{"$now": true}` cannot be stored literally (it
   becomes a timestamp).
9. No authentication or TLS on the HTTP transport (`/mcp` and `/healthz`).
   `utils export` and `client export` hold a whole collection's documents
   in memory at once (offline) or page with `skip` (client).
10. Single node only. The data-directory lock needs `flock` (Linux, macOS,
    the BSDs); elsewhere (Windows, Solaris, illumos, AIX) it is a no-op with
    a logged warning, and Windows is unsupported at runtime.
11. A single durable insert is fsync-bound (a few ms); throughput comes
    from concurrent writers sharing fsyncs. There is no relaxed-durability
    mode.
12. stdio: closing stdin exits 0, but the SDK drops the responses of
    requests still in flight.
13. `Open` trusts a valid but stale `index.db`. A copy of `index.db`
    restored on its own from an older backup, or a file whose newest bbolt
    meta page was damaged (bbolt then falls back to the previous one),
    passes the open check. `alignSegments` then deletes the segment files
    it does not register and truncates the covered segment to its old
    position. If WAL retention has already removed those records, the
    documents are lost silently. Back up and restore the whole data
    directory, never `index.db` alone; to repair a damaged `index.db`,
    delete it so `Open` rebuilds it from the segments, or run `cachydb
    utils verify --rebuild-index`. Before opening a directory whose
    `index.db` may be stale, run `cachydb utils verify`: it reports such a
    file ("older than the data segments", and "opening would discard ..."
    when the WAL no longer holds the changes). A fix in `Open` (check that
    the collection's checkpoint + 1 is still in the WAL before deleting or
    truncating, otherwise rebuild) is an open follow-up.
