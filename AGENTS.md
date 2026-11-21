# CachyDB — AI Agent Context

> Entry point for AI coding agents working in this repository.

## Project Overview

CachyDB is a lightweight document database with a built-in MCP (Model Context Protocol) server, written in Go. Single binary, no external services. `pkg/db` is an embeddable engine: a group-commit write-ahead log plus append-only, zstd-compressed data segments, with the key table and secondary indexes on disk in one bbolt file per collection (`index.db`), so the heap does not grow with the document count (data format 3; format-2 directories upgrade on first open). Supported platforms: Linux and macOS (the BSDs also get the data-directory lock; Windows, Solaris, illumos, and AIX compile, Windows is unsupported at runtime).

## Quick Reference

- **Language**: Go 1.27+ (`go.mod`: 1.27.0)
- **Module**: `github.com/hop-/cachydb`
- **Build**: `go build -o cachydb`
- **Test**: `go test -race ./...`
- **Lint**: `test -z "$(gofmt -l .)" && go vet ./... && go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...`
- **Benchmarks**: `TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench . -benchmem ./pkg/db/...` (real disk; the 1M/10M scale benchmark is opt-in: `CACHYDB_SCALE_DOCS=1000000`, see `dev-docs/DEVELOPMENT.md` "Scale benchmark")
- **Run**: `./cachydb` (stdio) or `./cachydb -t http -p 7601` (HTTP at `/mcp`, health at `/healthz`)
- **Docker**: `docker run -d -p 7601:7601 -v cachydb-data:/data -e BOOTSTRAP_ADMIN_TOKEN="$TOKEN" hovhannespalyan/cachydb` (HTTP with tokens; without a user or `-e AUTH=off` the container refuses to start; `-e TRANSPORT=stdio -i` for stdio); released by pushing a `vX.Y.Z` tag
- **Data dir**: `~/.cachydb`; override with `-R` or `ROOT_DIR` (flag > env > default; also `-d`/`DB_NAME`, `-t`/`TRANSPORT`, `-p`/`PORT`)
- **Agent-facing docs** (for *using* cachydb): `./cachydb docs` (topics: overview, quickstart, mcp, tools, query, schema, config, access, errors), `./cachydb version --json`; over MCP the same topics are `cachydb://docs/<topic>` resources. The source is `internal/docs/topics/*.md`; the `tools` topic is generated from `internal/mcp/tools.go`, and the `access` topic ends with the generated user-tool reference (`internal/mcp/users.go`).
- **Stress test**: `go test -race -count=200 -run 'Crash|Recovery|Torn|Concurrent|Condition|Rename|Schema|Model|Compat' ./pkg/db/...` (slow)
- **Heavy test layers** (opt-in, nightly in CI; see `TESTING.md`): fuzz targets (`go test -run '^$' -fuzz '^FuzzX$' -fuzztime 30s ./<pkg>`), the model test (`CACHYDB_MODEL_SEEDS`/`CACHYDB_MODEL_OPS`/`CACHYDB_MODEL_SEED`), the real-process kill test (`TMPDIR=$HOME/.cache/cachydb-e2e go test -tags e2e -run TestKill ./test/e2e/`, `CACHYDB_E2E_ROUNDS`); upgrade fixtures in `pkg/db/testdata/compat/` (one per release)
- **CI**: `.github/workflows/test.yml` (lint, race tests on min/stable Go (Linux runners only), cross-builds, 85% coverage floor) and `nightly.yml`
- **Users and access**: `./cachydb utils user add|list|remove|grant|revoke` and `utils user token add|revoke` (offline); `BOOTSTRAP_ADMIN_TOKEN`/`BOOTSTRAP_ADMIN_TOKEN_FILE` ensure the server admin `admin` at start; tokens are needed for HTTP on a non-loopback host (`AUTH=auto`) or with `AUTH=required`; with tokens, `whoami` plus six admin tools (hidden from non-admins) are registered
- **Client / offline data**: `./cachydb client call <tool> '<json>'` (also `tools`, `export`, `import`; `--url`/`CACHYDB_URL`, `--token`/`CACHYDB_TOKEN`) talks to a running HTTP server; `./cachydb utils export|import|find|get` work offline (they take the lock); `./cachydb utils verify [--deep] [--rebuild-index]` checks a stopped data directory (exit 1 on damage) and can rebuild `index.db` from the segments

## Developer Documentation

Detailed docs live in `dev-docs/`. **Read before modifying code.**

| Document | Contents |
|----------|----------|
| [`dev-docs/PROJECT.md`](dev-docs/PROJECT.md) | **Main doc**: overview, architecture, directory structure, data flow, compatibility |
| [`dev-docs/DATABASE_ENGINE.md`](dev-docs/DATABASE_ENGINE.md) | `pkg/db` public API, options, errors, documents, schemas, query and index semantics, migrations |
| [`dev-docs/STORAGE.md`](dev-docs/STORAGE.md) | On-disk layout and formats, WAL, data segments, `index.db`, flush and checkpoints, recovery, format upgrade, compaction, lock order, test hooks |
| [`dev-docs/MCP_SERVER.md`](dev-docs/MCP_SERVER.md) | The 21 MCP tools and the user tools, auth modes and the bootstrap token, tool access, typed schemas and annotations, instructions and docs resources, transports, per-session `use_database`, errors |
| [`dev-docs/CLI.md`](dev-docs/CLI.md) | Commands (incl. `docs`, `version --json`, `healthcheck`, `client`, `utils export/import/find/get/verify`), flags, environment variables, known issues |
| [`dev-docs/DEVELOPMENT.md`](dev-docs/DEVELOPMENT.md) | Rules, patterns, testing, benchmarks (incl. the recorded 1M/10M scale runs), releasing (Docker image), known limitations |

Start with `dev-docs/PROJECT.md`, then `dev-docs/DEVELOPMENT.md`. User-facing docs: [`README.md`](README.md), [`DOCKERHUB.md`](DOCKERHUB.md) (Docker Hub overview), [`TESTING.md`](TESTING.md), [`examples/README.md`](examples/README.md).

## Key Source Locations

| Path | Role |
|------|------|
| `pkg/db/engine.go` | `Engine`, `Open`/`Close`, database operations |
| `pkg/db/document.go`, `batch.go` | Document write path (WAL commit point; `UpdateWith`, `Replace`, `DeleteWith`, conditions), read path, candidate selection; `Collection.Batch` (WAL op 11, one record and fsync for many writes), `UpdateMany`/`DeleteMany` (writes by filter) |
| `pkg/db/query.go`, `plan.go`, `compare.go`, `index.go`, `unique.go` | `Find` (OR groups, sort, projection), `CountDocuments`, `Distinct`; query plans and snapshots; compiled operators and `$now`; index entries; unique indexes (field paths: `types.go` `GetPath`) |
| `pkg/db/collection.go`, `database.go` | Schema/index metadata ops incl. `UpdateSchema` (WAL op 9), `RenameCollection` (WAL op 10), `CopyCollection` (through batches) |
| `pkg/db/flush.go`, `recovery.go`, `upgrade.go`, `compact.go` | Parallel flusher (segment append, then one `index.db` transaction with the checkpoint), recovery (index file open, segment alignment, rebuild) and replay, format 2 → 3 upgrade, compaction (incl. small-segment merges) |
| `pkg/db/verify.go` | `Verify`: offline integrity check (WAL, data segments, `index.db` with `Store.Check`, key table against the segments, stale `index.db`, `Deep` index entries) and `RebuildIndex`; backs `cachydb utils verify` |
| `pkg/db/backpressure.go`, `metacp.go` | `MaxPendingBytes` write backpressure; `cachydb.metacp` metadata checkpoint |
| `pkg/db/internal/wal/`, `internal/segment/`, `internal/idxstore/` | WAL and data-segment file formats; `index.db` (the only bbolt importer: key table, index entries and key encoding, counters, checkpoint) |
| `internal/mcp/server.go` | MCP tool handlers, sessions, transports |
| `internal/mcp/access.go`, `users.go` | Per-tool access checks (`authorizeCall`); `whoami` and the admin user tools, `tools/list` filtering |
| `internal/auth/` | Users, tokens, grants in `_system` (`Store`), levels, auth modes (`Decide`), bootstrap admin (`EnsureAdmin`) |
| `internal/mcp/tools.go`, `docs.go` | Tool definitions (typed schemas, annotations), instructions, docs resources |
| `internal/docs/` | Embedded agent docs topics |
| `internal/cmd/` | CLI commands and flags (incl. `healthcheck`, which probes `GET /healthz`; `client.go`, the MCP client over HTTP; `export.go`, `import.go`, `find.go`, `verify.go`, `user.go`) |
| `internal/exportfmt/` | JSONL export format, `Export`/`Import` over a Source/Target (engine adapter here, also a `BatchTarget` that imports in batches; MCP client adapter in `internal/cmd/client.go`) |
| `internal/config/` | Environment configuration, `ByteSize` |
| `internal/app/` | Builder and application lifecycle |
| `test/e2e/`, `test/compat/` | Real-process kill test (build tag `e2e`); upgrade-fixture generator and helpers |
| `Dockerfile`, `.github/workflows/docker.yml` | Official image `hovhannespalyan/cachydb`; PR smoke test, `v*` tags publish (see `dev-docs/DEVELOPMENT.md`, "Releasing") |
