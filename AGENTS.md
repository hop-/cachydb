# CachyDB — AI Agent Context

> Entry point for AI coding agents working in this repository.

## Project Overview

CachyDB is a lightweight document database with a built-in MCP (Model Context Protocol) server, written in Go. Single binary, no external services. `pkg/db` is an embeddable engine: a group-commit write-ahead log plus append-only, zstd-compressed data segments, with keys and indexes in RAM. Supported platforms: Linux and macOS (the BSDs also get the data-directory lock; Windows, Solaris, illumos, and AIX compile, Windows is unsupported at runtime).

## Quick Reference

- **Language**: Go 1.25+ (`go.mod`: 1.25.5)
- **Module**: `github.com/hop-/cachydb`
- **Build**: `go build -o cachydb`
- **Test**: `go test -race ./...`
- **Lint**: `test -z "$(gofmt -l .)" && go vet ./...`
- **Benchmarks**: `go test -run '^$' -bench . -benchmem ./pkg/db/...`
- **Run**: `./cachydb` (stdio) or `./cachydb -t http -p 7601` (HTTP at `/mcp`, health at `/healthz`)
- **Docker**: `docker run -d -p 7601:7601 -v cachydb-data:/data hovhannespalyan/cachydb` (HTTP; `-e TRANSPORT=stdio -i` for stdio); released by pushing a `vX.Y.Z` tag
- **Data dir**: `~/.cachydb`; override with `-R` or `ROOT_DIR` (flag > env > default; also `-d`/`DB_NAME`, `-t`/`TRANSPORT`, `-p`/`PORT`)
- **Agent-facing docs** (for *using* cachydb): `./cachydb docs` (topics: overview, quickstart, mcp, tools, query, schema, config, errors), `./cachydb version --json`; over MCP the same topics are `cachydb://docs/<topic>` resources. The source is `internal/docs/topics/*.md`; the `tools` topic is generated from `internal/mcp/tools.go`.
- **Stress test**: `go test -race -count=200 -run 'Crash|Recovery|Torn|Concurrent|Condition|Rename|Schema' ./pkg/db/...` (slow)
- **Client / offline data**: `./cachydb client call <tool> '<json>'` (also `tools`, `export`, `import`; `--url`/`CACHYDB_URL`) talks to a running HTTP server; `./cachydb utils export|import|find|get` work offline (they take the lock)

## Developer Documentation

Detailed docs live in `dev-docs/`. **Read before modifying code.**

| Document | Contents |
|----------|----------|
| [`dev-docs/PROJECT.md`](dev-docs/PROJECT.md) | **Main doc**: overview, architecture, directory structure, data flow, compatibility |
| [`dev-docs/DATABASE_ENGINE.md`](dev-docs/DATABASE_ENGINE.md) | `pkg/db` public API, options, errors, documents, schemas, query and index semantics, migrations |
| [`dev-docs/STORAGE.md`](dev-docs/STORAGE.md) | On-disk layout and formats, WAL, data segments, flush and checkpoints, recovery, compaction, lock order |
| [`dev-docs/MCP_SERVER.md`](dev-docs/MCP_SERVER.md) | The 21 MCP tools, typed schemas and annotations, instructions and docs resources, transports, per-session `use_database`, errors |
| [`dev-docs/CLI.md`](dev-docs/CLI.md) | Commands (incl. `docs`, `version --json`, `healthcheck`, `client`, `utils export/import/find/get`), flags, environment variables, known issues |
| [`dev-docs/DEVELOPMENT.md`](dev-docs/DEVELOPMENT.md) | Rules, patterns, testing, benchmarks, releasing (Docker image), known limitations |

Start with `dev-docs/PROJECT.md`, then `dev-docs/DEVELOPMENT.md`. User-facing docs: [`README.md`](README.md), [`DOCKERHUB.md`](DOCKERHUB.md) (Docker Hub overview), [`TESTING.md`](TESTING.md), [`examples/README.md`](examples/README.md).

## Key Source Locations

| Path | Role |
|------|------|
| `pkg/db/engine.go` | `Engine`, `Open`/`Close`, database operations |
| `pkg/db/document.go` | Document write path (WAL commit point; `UpdateWith`, `Replace`, `DeleteWith`, conditions), read path, candidate selection |
| `pkg/db/query.go`, `compare.go`, `index.go` | `Find` (OR groups, sort, projection), `CountDocuments`, `Distinct`; operators and `$now`; hash indexes with per-element keys |
| `pkg/db/collection.go`, `database.go` | Schema/index metadata ops incl. `UpdateSchema` (WAL op 9), `RenameCollection` (WAL op 10), `CopyCollection` |
| `pkg/db/flush.go`, `recovery.go`, `compact.go` | Parallel flusher and checkpoints, recovery and replay, compaction (incl. small-segment merges) |
| `pkg/db/backpressure.go`, `metacp.go` | `MaxPendingBytes` write backpressure; `cachydb.metacp` metadata checkpoint |
| `pkg/db/internal/wal/`, `internal/segment/` | WAL and data-segment file formats |
| `internal/mcp/server.go` | MCP tool handlers, sessions, transports |
| `internal/mcp/tools.go`, `docs.go` | Tool definitions (typed schemas, annotations), instructions, docs resources |
| `internal/docs/` | Embedded agent docs topics |
| `internal/cmd/` | CLI commands and flags (incl. `healthcheck`, which probes `GET /healthz`; `client.go`, the MCP client over HTTP; `export.go`, `import.go`, `find.go`) |
| `internal/exportfmt/` | JSONL export format, `Export`/`Import` over a Source/Target (engine adapter here, MCP client adapter in `internal/cmd/client.go`) |
| `internal/config/` | Environment configuration, `ByteSize` |
| `internal/app/` | Builder and application lifecycle |
| `Dockerfile`, `.github/workflows/docker.yml` | Official image `hovhannespalyan/cachydb`; PR smoke test, `v*` tags publish (see `dev-docs/DEVELOPMENT.md`, "Releasing") |
