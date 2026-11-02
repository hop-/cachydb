# CachyDB — Project Overview

## Purpose

CachyDB is a lightweight document database with a built-in Model Context
Protocol (MCP) server. It stores JSON documents in collections inside
databases, supports optional schema validation and multi-value secondary
indexes, and exposes everything as MCP tools for AI assistants. It ships as
one Go binary; `pkg/db` is also usable as an embedded Go library.

Supported platforms: **Linux and macOS** (the BSDs also get the
data-directory lock). Windows, Solaris, illumos, and AIX compile; there the
lock is a no-op with a logged warning, and Windows is not supported at
runtime (no directory fsync either).

For AI agents using cachydb (not developing it), the binary documents
itself: `cachydb --help`, `cachydb docs`, and over MCP typed tool schemas,
server instructions, and `cachydb://docs/<topic>` resources.

## Related docs

- [DATABASE_ENGINE.md](DATABASE_ENGINE.md) — `pkg/db` public API, documents, schemas, query and index semantics, migrations
- [STORAGE.md](STORAGE.md) — on-disk layout and formats, WAL, data segments, flush, recovery, compaction, lock order
- [MCP_SERVER.md](MCP_SERVER.md) — the 21 MCP tools, transports, sessions, errors
- [CLI.md](CLI.md) — commands (incl. `docs`, `version --json`), flags, environment variables, known issues
- [DEVELOPMENT.md](DEVELOPMENT.md) — coding patterns, testing, benchmarks, known limitations

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ CLI (cobra)   internal/cmd                                   │
│ cachydb [app] | version | utils list | utils migrate         │
│ signal.NotifyContext → RunE → errors to stderr, exit 1       │
└───────────────┬───────────────────────────────┬──────────────┘
                │ server                        │ utils
┌───────────────▼─────────────────┐             │
│ internal/app (Builder, App)     │             │
│ opens Engine, Start / Stop      │             │
└───────────────┬─────────────────┘             │
┌───────────────▼─────────────────┐             │
│ internal/mcp (21 tools)         │             │
│ stdio | Streamable HTTP (/mcp)  │             │
│ per-session use_database        │             │
└───────────────┬─────────────────┘             │
┌───────────────▼───────────────────────────────▼──────────────┐
│ pkg/db  Engine → Database → Collection                       │
│  validate → docLock → WAL append+fsync (commit) → memtable   │
│  ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌────────────────┐  │
│  │ query /  │ │ schema    │ │ indexes  │ │ LRU cache      │  │
│  │ compare  │ │ validation│ │ (multi)  │ │ (decoded docs) │  │
│  └──────────┘ └───────────┘ └──────────┘ └────────────────┘  │
│  background: flusher · compactor · WAL retention             │
│  internal/wal       group-commit log, retention              │
│  internal/segment   append-only zstd data segments, hints,   │
│                     index snapshots                          │
│  internal/fsutil    atomic writes, dir fsync, flock          │
│  internal/cache     byte-bounded LRU                         │
└──────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
cachydb/
├── main.go                  # → cmd.Execute()
├── go.mod                   # github.com/hop-/cachydb, Go 1.25.5
├── internal/
│   ├── app/                 # Builder (options → Engine + MCP server), App Start/Stop
│   ├── cmd/                 # cobra commands: root/app, docs, version, healthcheck, utils list/migrate/export/import/find/get, client, flags
│   ├── exportfmt/           # JSONL export format, Export/Import over Source/Target (engine adapter)
│   ├── config/              # envconfig settings, ByteSize parser, Windows root dir name
│   ├── docs/                # embedded agent docs topics (cachydb docs, MCP resources)
│   └── mcp/                 # server.go: handlers, sessions, transports;
│                            # tools.go: tool definitions; docs.go: instructions, resources
├── pkg/db/
│   ├── engine.go            # Engine, Open/Close, database create/drop
│   ├── database.go          # Database, collection create/drop/rename/copy
│   ├── collection.go        # Collection state, CreateIndex/DropIndex, Schema/Indexes, UpdateSchema
│   ├── document.go          # write path (UpdateWith, Replace, DeleteWith, conditions), read path, candidates
│   ├── query.go             # Find (OR groups, sort, projection), CountDocuments, Distinct
│   ├── flush.go             # parallel flusher, checkpoint markers, low-water mark
│   ├── backpressure.go      # MaxPendingBytes write backpressure
│   ├── metacp.go            # cachydb.metacp metadata checkpoint
│   ├── recovery.go          # load at Open, idempotent WAL replay
│   ├── compact.go           # background compaction
│   ├── compare.go index.go  # operators, $now, multi-value index with per-element keys
│   ├── types.go schema.go   # Document, Schema, Query; validation
│   ├── meta.go format.go    # db.meta / coll.meta, cachydb.format
│   ├── migration.go         # schema-version migrations
│   ├── options.go errors.go names.go hooks.go
│   ├── bench_test.go        # performance benchmarks
│   └── internal/{wal,segment,fsutil,cache}/
├── examples/
│   ├── basic/main.go        # embed pkg/db as a library
│   └── mcp-client/main.go   # drive the binary over stdio MCP
├── Dockerfile .dockerignore # official image (distroless, non-root, /data volume)
├── docker-compose.yml       # example: HTTP server with a named volume
├── DOCKERHUB.md             # Docker Hub overview, synced on release
└── .github/workflows/docker.yml  # PR smoke test; v* tags publish hovhannespalyan/cachydb
```

## Design Principles

1. **Layered**: `pkg/db` is standalone and owns durability; `internal/mcp`
   maps tools to engine calls; `internal/cmd`/`internal/app` handle the
   process lifecycle.
2. **The WAL is the commit point**: a change is fsynced to the WAL before it
   is applied in memory or acknowledged. Writes are visible only once
   durable.
3. **Group commit**: concurrent writers share one fsync; no artificial delay.
4. **Append-only data**: flushes append only changed documents plus a
   checkpoint marker; the cost scales with the change, not the collection.
5. **Last writer wins by WAL offset**: every record carries its WAL offset,
   so loading and replay are order-independent and idempotent.
6. **Bounded RAM**: only keys, indexes, memtables, and the LRU cache live in
   memory; documents are read from disk. Unflushed memtable bytes are
   bounded by `MaxPendingBytes` (writers wait while over it).
7. **Clean lifecycle**: exclusive data-directory lock, graceful shutdown
   with a final flush, only MCP traffic on stdout (logs go to stderr via
   `slog`).

## Data Flow

### Write (e.g. `insert_document`)

```
MCP handler → Collection.Insert(ctx, doc)
  0. wait while unflushed bytes >= MaxPendingBytes (ctx-aware, no lock held)
  1. Database.mu (shared) → docLock(id) → read current version
  2. check the "if" condition, build after-image, JSON round-trip, schema validation
  3. WAL append; group commit fsync                 ← commit point
  4. apply under Collection.mu: memtable, indexes, cache invalidation, count
  5. return id
background flusher (every 1s, memtable > 16 MiB, or writers waiting;
up to FlushWorkers collections in parallel):
  memtable → active data segment + checkpoint marker, one fsync → keydir
```

### Read (e.g. `find_documents`)

```
Collection.Find(ctx, query)
  candidates: smallest index-backed set (eq, has, has_any, has_all), else all live ids; sorted by _id
  per id: memtable → flushing → LRU cache (decoded) → keydir → ReadAt + zstd + JSON
  filter (AND + OR groups); _id order: skip, stop at skip+limit;
  other sort: bounded heap of skip+limit; project fields; return deep copies
```

### Startup

```
cmd.Execute → config.Init → cobra → runApp
  app.Builder.Build → db.Open
     lock cachydb.lock → check cachydb.format → open WAL (torn-tail fix)
     → load meta, keydirs, indexes → replay WAL from
       min(cachydb.metacp + 1, collection floors) → write cachydb.metacp
       → start flusher, compactor, retention
  → mcp.NewServer (creates the default database, -d/--db or DB_NAME,
     default "main"; registers tools, docs resources, instructions)
  → App.Start: run stdio or HTTP transport
```

### Shutdown

```
SIGINT/SIGTERM (or stdin EOF for stdio) → transport returns
  → App.Stop (10s timeout): close MCP sessions → Engine.Close
     (final flush, index snapshots, WAL close, lock release)
```

## Compatibility

- Data directories from the earlier storage format (JSON / `.data`+`.idx`
  files, `wal.checkpoint`) are **not readable**. `Open` fails with
  `ErrIncompatibleDataDir` and asks to move or delete the directory; there
  is no converter.
- The old Go API (`DatabaseManager`, `StorageManager`, `WALManager`,
  `MigrationManager`) is gone; use `db.Open` and the `*Engine` methods.
