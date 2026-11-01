# CachyDB — AI Agent Context

> Entry point for AI coding agents working in this repository.

## Project Overview

CachyDB is a lightweight document database with a built-in MCP (Model Context Protocol) server, written in Go. Single binary, no external services. `pkg/db` is an embeddable engine: a group-commit write-ahead log plus append-only, zstd-compressed data segments, with keys and indexes in RAM. Supported platforms: Linux and macOS (Windows compiles but is unsupported).

## Quick Reference

- **Language**: Go 1.25+ (`go.mod`: 1.25.5)
- **Module**: `github.com/hop-/cachydb`
- **Build**: `go build -o cachydb`
- **Test**: `go test -race ./...`
- **Lint**: `test -z "$(gofmt -l .)" && go vet ./...`
- **Benchmarks**: `go test -run '^$' -bench . -benchmem ./pkg/db/...`
- **Run**: `./cachydb` (stdio) or `./cachydb -t http -p 7601` (HTTP at `/mcp`)
- **Data dir**: `~/.cachydb`; override with `-R` (the `ROOT_DIR` env var does not work, see `dev-docs/CLI.md`)

## Developer Documentation

Detailed docs live in `dev-docs/`. **Read before modifying code.**

| Document | Contents |
|----------|----------|
| [`dev-docs/PROJECT.md`](dev-docs/PROJECT.md) | **Main doc**: overview, architecture, directory structure, data flow, compatibility |
| [`dev-docs/DATABASE_ENGINE.md`](dev-docs/DATABASE_ENGINE.md) | `pkg/db` public API, options, errors, documents, schemas, query and index semantics, migrations |
| [`dev-docs/STORAGE.md`](dev-docs/STORAGE.md) | On-disk layout and formats, WAL, data segments, flush and checkpoints, recovery, compaction, lock order |
| [`dev-docs/MCP_SERVER.md`](dev-docs/MCP_SERVER.md) | The 12 MCP tools, transports, per-session `use_database`, errors |
| [`dev-docs/CLI.md`](dev-docs/CLI.md) | Commands, flags, environment variables, known issues |
| [`dev-docs/DEVELOPMENT.md`](dev-docs/DEVELOPMENT.md) | Rules, patterns, testing, benchmarks, known limitations |

Start with `dev-docs/PROJECT.md`, then `dev-docs/DEVELOPMENT.md`. User-facing docs: [`README.md`](README.md), [`TESTING.md`](TESTING.md), [`examples/README.md`](examples/README.md).

## Key Source Locations

| Path | Role |
|------|------|
| `pkg/db/engine.go` | `Engine`, `Open`/`Close`, database operations |
| `pkg/db/document.go` | Document write path (WAL commit point), read path, `Find` |
| `pkg/db/flush.go`, `recovery.go`, `compact.go` | Flusher and checkpoints, recovery and replay, compaction |
| `pkg/db/internal/wal/`, `internal/segment/` | WAL and data-segment file formats |
| `internal/mcp/server.go` | MCP tools, sessions, transports |
| `internal/cmd/` | CLI commands and flags |
| `internal/config/` | Environment configuration, `ByteSize` |
| `internal/app/` | Builder and application lifecycle |
