# CachyDB — Project Overview

## Purpose

CachyDB is a lightweight document database with a built-in Model Context
Protocol (MCP) server. It stores JSON documents in collections inside
databases, supports optional schema validation and ordered secondary
indexes kept on disk (unique and nested-path ones too), and exposes everything as MCP tools for AI assistants. It ships as
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
- [MCP_SERVER.md](MCP_SERVER.md) — the 26 MCP tools (plus the user tools with tokens), auth modes, tool access, transports, sessions, errors
- [CLI.md](CLI.md) — commands (incl. `docs`, `version --json`), flags, environment variables, known issues
- [DEVELOPMENT.md](DEVELOPMENT.md) — coding patterns, testing, benchmarks, known limitations

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ CLI (cobra)   internal/cmd                                   │
│ cachydb [app] | version | utils list|migrate|user | client   │
│ signal.NotifyContext → RunE → errors to stderr, exit 1       │
└───────────────┬───────────────────────────────┬──────────────┘
                │ server                        │ utils
┌───────────────▼─────────────────┐             │
│ internal/app (Builder, App)     │             │
│ opens Engine, auth store,       │             │
│ bootstrap token, Start / Stop   │             │
└───────────────┬─────────────────┘             │
┌───────────────▼─────────────────┐             │
│ internal/mcp (26 tools + 7 user │             │
│ tools with tokens), tool access │             │
│ stdio | Streamable HTTP (/mcp)  │             │
│ per-session use_database        │             │
├─────────────────────────────────┤             │
│ internal/auth: users, tokens,   │◄────────────┤
│ grants in _system; auth modes   │             │
└───────────────┬─────────────────┘             │
┌───────────────▼───────────────────────────────▼──────────────┐
│ pkg/db  Engine → Database → Collection                       │
│  validate → docLock → WAL append+fsync (commit) → memtable   │
│  ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌────────────────┐  │
│  │ query /  │ │ schema    │ │ plans /  │ │ LRU cache      │  │
│  │ compare  │ │ validation│ │ unique   │ │ (decoded docs) │  │
│  └──────────┘ └───────────┘ └──────────┘ └────────────────┘  │
│  background: flusher · compactor · WAL retention             │
│  internal/wal       group-commit log, retention              │
│  internal/segment   append-only zstd data segments           │
│  internal/idxstore  index.db (bbolt): key table, indexes,    │
│                     count, checkpoint (one per collection)   │
│  internal/fsutil    atomic writes, dir fsync, flock          │
│  internal/cache     byte-bounded LRU                         │
└──────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
cachydb/
├── main.go                  # → cmd.Execute()
├── go.mod                   # github.com/hop-/cachydb, Go 1.27.0
├── internal/
│   ├── app/                 # Builder (options → Engine, auth store, bootstrap token, MCP server), App Start/Stop
│   ├── auth/                # users, tokens, grants (Store over _system), levels, auth modes (Decide), bootstrap admin
│   ├── cmd/                 # cobra commands: root/app, docs, version, healthcheck, utils list/migrate/export/import/find/get/verify/user, client, flags
│   ├── exportfmt/           # JSONL export format, Export/Import over Source/Target (engine adapter)
│   ├── config/              # environment settings, ByteSize parser, Windows root dir name
│   ├── docs/                # embedded agent docs topics (cachydb docs, MCP resources)
│   └── mcp/                 # server.go: handlers, sessions, transports; access.go: tool access;
│                            # tools.go: tool definitions; users.go: whoami and admin tools;
│                            # listen.go: ListenConfig; docs.go: instructions, resources
├── pkg/db/
│   ├── engine.go            # Engine, Open/Close, database create/drop
│   ├── database.go          # Database, collection create/drop/rename/copy
│   ├── collection.go        # Collection state, CreateIndex/DropIndex, Schema/Indexes, UpdateSchema
│   ├── document.go          # write path (UpdateWith, Replace, DeleteWith, conditions), read path
│   ├── query.go plan.go     # Find (OR groups, sort, projection), CountDocuments, Distinct; snapshots, plans
│   ├── unique.go            # unique indexes (striped value locks, pending values)
│   ├── flush.go             # parallel flusher, index transaction, checkpoint, low-water mark
│   ├── backpressure.go      # MaxPendingBytes write backpressure
│   ├── metacp.go            # cachydb.metacp metadata checkpoint
│   ├── recovery.go          # load at Open (index.db, segment alignment, rebuild), idempotent WAL replay
│   ├── upgrade.go           # format 2 -> 3 upgrade at Open
│   ├── compact.go           # background compaction
│   ├── compare.go index.go  # compiled operators, $now; index entries and builds
│   ├── types.go schema.go   # Document, Schema, Query; validation
│   ├── meta.go format.go    # db.meta / coll.meta, cachydb.format
│   ├── migration.go         # schema-version migrations
│   ├── options.go errors.go names.go hooks.go
│   ├── bench_test.go        # performance benchmarks
│   ├── scale_bench_test.go  # opt-in 1M/10M scale benchmark (CACHYDB_SCALE_DOCS)
│   ├── model_test.go        # randomized test against a reference model
│   ├── testdata/compat/     # upgrade fixtures written by v0.0.4, v0.0.5, ...
│   └── internal/{wal,segment,idxstore,fsutil,cache}/
├── examples/
│   ├── basic/main.go        # embed pkg/db as a library
│   └── mcp-client/main.go   # drive the binary over stdio MCP
├── Dockerfile .dockerignore # official image (distroless, non-root, /data volume)
├── docker-compose.yml       # example: HTTP server with a named volume
├── DOCKERHUB.md             # Docker Hub overview, synced on release
├── test/
│   ├── e2e/                 # real-process kill test (build tag e2e)
│   └── compat/              # upgrade-fixture generator (gen/) and helpers
└── .github/workflows/
    ├── test.yml             # PR/master CI: lint, race tests, cross-builds, coverage floor
    ├── nightly.yml          # stress, kill test, long model run, fuzzing, 1M scale benchmark
    └── docker.yml           # PR smoke test; v* tags publish hovhannespalyan/cachydb
```

## Design Principles

1. **Layered**: `pkg/db` is standalone and owns durability; `internal/mcp`
   maps tools to engine calls; `internal/cmd`/`internal/app` handle the
   process lifecycle.
2. **The WAL is the commit point**: a change is fsynced to the WAL before it
   is applied in memory or acknowledged. Writes are visible only once
   durable.
3. **Group commit**: concurrent writers share one fsync; no artificial delay.
4. **Append-only data**: flushes append only changed documents, then
   apply the key table, index entries, and checkpoint in one `index.db`
   transaction; the cost scales with the change, not the collection.
5. **Last writer wins by WAL offset**: every record carries its WAL offset,
   so rebuilds and replay are order-independent and idempotent.
6. **Bounded RAM**: keys and indexes live on disk in `index.db` (served by
   the OS page cache); only memtables, the LRU cache, and small
   per-collection state live in memory, so the heap does not grow with the
   document count. Unflushed memtable bytes are bounded by
   `MaxPendingBytes` (writers wait while over it).
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
  3. unique indexes: stripe locks, check overlay + index.db
  4. WAL append; group commit fsync                 ← commit point
  5. apply under Collection.mu: memtable, pending unique values, cache invalidation, count
  6. return id
background flusher (every 1s, memtable > 16 MiB, or writers waiting;
up to FlushWorkers collections in parallel):
  memtable → active data segment, one fsync
  → one index.db transaction: key table, index entries, count, checkpoint
```

### Read (e.g. `find_documents`)

```
Collection.Find(ctx, query)
  compile filters once; capture the overlay (memtable + flushing), open one index.db read transaction
  driver: _id lookup | index (eq, in, has*, exists) | index range (ranges, starts_with)
          | OR union | index sort walk | key-table walk  (smallest capped estimate)
  stream ids merged with the overlay; documents from LRU cache (decoded) or ReadAt + zstd + JSON
  re-check every filter; stop at skip+limit when the driver gives the order,
  else bounded heap of skip+limit; project fields; return deep copies
```

### Startup

```
cmd.Execute → config.Init → cobra → runApp
  app.Builder.Build → db.Open
     lock cachydb.lock → check cachydb.format → open WAL (torn-tail fix)
     → load meta → (format 2: upgrade, build every index.db)
     → open each index.db, align segments (rebuild if missing/damaged) → replay WAL from
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
     (final flush, close segments, index files, and WAL, lock release)
```

## Compatibility

- Data directories of format 2 (v0.0.4–v0.0.6) are **upgraded** to format
  3 on the first `Open`: each collection's `index.db` is built from its data
  segments, then the hint files and index snapshots are deleted. No document
  is rewritten, and the upgrade is one-way (copy or export first). See
  [STORAGE.md](STORAGE.md#format-file-and-old-data-directories).
- Data directories from the earlier storage format (JSON / `.data`+`.idx`
  files, `wal.checkpoint`, before v0.0.4) are **not readable**. `Open` fails
  with `ErrIncompatibleDataDir` and asks to move or delete the directory;
  there is no converter.
- The old Go API (`DatabaseManager`, `StorageManager`, `WALManager`,
  `MigrationManager`) is gone; use `db.Open` and the `*Engine` methods.
