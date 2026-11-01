# Storage Engine Internals

How `pkg/db` stores data: on-disk layout and formats, the write-ahead log,
data segments, flush and checkpoints, recovery, compaction, and the lock
order. For the public Go API and query semantics see
[DATABASE_ENGINE.md](DATABASE_ENGINE.md); for the big picture see
[PROJECT.md](PROJECT.md).

## Design in one paragraph

Every change is appended to a **write-ahead log (WAL)** and fsynced before
it is applied or acknowledged; concurrent writers share fsyncs (**group
commit**). Applied changes sit in a per-collection **memtable**. A
background **flusher** appends only the changed documents and tombstones to
the collection's **append-only data segment**, followed by a **checkpoint
marker**, with one fsync, then repoints the in-RAM **keydir** (id → segment
location). Reads go memtable → **LRU cache** → keydir → segment. Background
**compaction** rewrites sealed segments to drop dead records, and **WAL
retention** deletes processed WAL segments by age and/or size. Every record
carries its WAL offset, and load and replay apply "last writer wins by WAL
offset", so recovery is idempotent. Only keys, indexes, memtables, and the
bounded cache live in RAM.

## On-disk layout

```
<root>/                               # Options.Dir (CLI: -R, default ~/.cachydb)
├── cachydb.lock                      # exclusive flock target (data-dir lock)
├── cachydb.format                    # {"format":"cachydb","version":2}
├── wal-00000000000000000001.log      # WAL segments, named by first offset (20 digits)
├── wal-00000000000004213377.log
├── .deleted-<offset>/                # transient: dropped database awaiting removal
└── <db>/
    ├── db.meta                       # JSON, atomic write
    ├── .deleted-<offset>/            # transient: dropped collection awaiting removal
    └── <coll>/
        ├── coll.meta                 # JSON, atomic write
        ├── seg-000001.data           # sealed data segment
        ├── seg-000001.hint           # hint file of the sealed segment
        ├── seg-000002.data           # active data segment (append-only)
        └── idx-<index>.snap          # index snapshot
```

- Root files contain a `.`; database, collection, and index names cannot
  (`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, `pkg/db/names.go`), so names never
  collide with engine files and never escape the root.
- File names: `pkg/db/engine.go` (`cachydb.lock`), `pkg/db/format.go`,
  `pkg/db/meta.go`, `pkg/db/internal/wal/segment.go`,
  `pkg/db/internal/segment/{segment,hint,snapshot}.go`.

### Format file and old data directories

`checkDataDir` (`pkg/db/format.go`) runs on every `Open`. A missing
`cachydb.format` in a root that holds legacy files (`wal.checkpoint`,
`wal-<unix>-<offset>.log`, or any `<db>/db.meta.json`) fails with
`ErrIncompatibleDataDir`: *"data directory … was created by an older
cachydb format; move or delete it"*. There is no converter. A format
version newer than `FormatVersion` (2) also fails. An empty root gets the
file written atomically.

### Metadata files

All metadata writes use `fsutil.WriteFileAtomic` (temp file → fsync →
rename → fsync directory).

| File | JSON fields |
|------|-------------|
| `db.meta` | `name`, `schema_version`, `wal_offset` |
| `coll.meta` | `name`, `schema` (or `null`), `indexes` (`[{name, field}]`, sorted), `wal_offset`, `created_offset` |

`wal_offset` is the WAL offset of the record that last wrote the file.
`created_offset` is the offset of this collection incarnation's
`create_coll` record: replay uses it to skip the records of an earlier,
dropped collection with the same name, so a recreated collection never
resurrects the old documents (0 = unknown; replay then falls back to the
database's `wal_offset` and the checkpoint).

## Write-ahead log (`pkg/db/internal/wal`)

Segment header (16 bytes): magic `CDBW` · `u16` version 1 · 10 reserved.
Records, little-endian, CRC32C (Castagnoli):

```
u32 length · u32 crc32c · u64 offset · i64 unix_nanos · u8 op
u16 db_len · db · u16 coll_len · coll · u16 id_len · id · u32 payload_len · payload
```

| op | Name | Payload |
|----|------|---------|
| 1 | `put_doc` | full document after-image (JSON, includes `_id`) |
| 2 | `delete_doc` | none |
| 3 | `create_db` | none |
| 4 | `drop_db` | none |
| 5 | `create_coll` | `{"schema": <Schema or null>}` |
| 6 | `drop_coll` | none |
| 7 | `create_index` | `{"name", "field"}` |
| 8 | `drop_index` | `{"name"}` |

Offsets start at 1 and increase by exactly 1 across the whole log. The
format is self-describing so a future WAL reader / change feed needs no
format change (no such API exists yet).

- **Group commit** (`log.go`): `Log.Append(ctx, rec)` queues the record and
  blocks until its batch is durable. One writer goroutine takes a request,
  drains whatever else is queued (cap 1024 records or 4 MiB), assigns
  offsets, does one write and one fsync, and answers every waiter. There is
  no artificial delay; batches form while the previous fsync is in flight.
  A cancelled caller's record is still written (cancellation only stops
  waiting); the engine waits for it anyway.
- **fsync failure is fatal**: the whole batch fails, the log is marked
  failed, and later appends return `ErrWALFailed` until restart. Reads keep
  working.
- **Rotation** at `WALSegmentSize` (64 MiB): a new `wal-<next offset>.log`.
- **Open**: scan every segment in order, verify CRCs and offset continuity.
  A short or bad record at the very end of the newest segment is a torn
  tail: truncated, fsynced, logged. Damage anywhere else, or an offset gap,
  fails `Open` with `ErrWALCorrupt`. Appending continues in the newest
  segment (no new file per start).
- **Retention** (`retention.go`): runs after each rotation and every
  minute. Only sealed segments whose last offset is below the engine's
  low-water mark (processed) are candidates, oldest first. One is deleted
  while its newest record is older than `WALRetentionAge` (24h) or the WAL
  is larger than `WALRetentionSize` (1 GiB). Each limit can be disabled;
  with both disabled, processed segments are kept forever. Unprocessed
  segments are never deleted; if they alone exceed the size limit, a
  warning is logged at most once a minute. Retention starts only after
  replay.

## Data segments (`pkg/db/internal/segment`)

Segment header (16 bytes): magic `CDBD` · `u16` version 1 · `u8` codec · 9
reserved. Records:

```
u32 length · u32 crc32c · u8 type · u64 wal_offset · u16 id_len · id
u32 raw_len · u8 codec · body
```

- Types: 1 = put (document after-image), 2 = tombstone, 3 = checkpoint
  marker (`wal_offset` holds the checkpoint value, no id).
- Bodies are zstd-compressed JSON (`klauspost/compress`, `SpeedDefault`,
  shared encoder/decoder). Documents under 64 bytes are stored with codec 0
  (uncompressed).
- The active segment is appended to; when it exceeds `DataSegmentSize`
  (64 MiB) it is **sealed**: fsynced, its hint file and the collection's
  index snapshots are written, and the next flush opens `seg-<n+1>.data`.
  Segment ids only grow (compaction outputs take ids from the same counter).
- Reads use `ReadAt` on files opened once and ref-counted, so compaction can
  retire a segment while a read holds it.
- **Hint file** (`seg-NNNNNN.hint`, magic `CDBH`): one entry per record (id,
  type, WAL offset, position, size) so a sealed segment loads without
  reading the data. A missing or invalid hint means the segment is scanned
  and the hint rewritten.
- **Index snapshot** (`idx-<name>.snap`, magic `CDBI`): the index's keys and
  id sets plus the data position `(seg, pos)` and WAL offset it covers.
  Written when a segment is sealed, after compaction, and on `Close` (if
  the final flush succeeded). On open, a snapshot whose field matches
  `coll.meta` and whose CRC checks is loaded and caught up from the records
  after its position; otherwise the index is rebuilt from the documents.
- Hint and snapshot files share a framing: magic · `u16` version · body ·
  CRC32C over everything before it (`sidecar.go`).

## In-memory state per collection (`pkg/db/collection.go`)

| Field | Role |
|-------|------|
| `keydir` | id → `{segment location, walOffset}` of the live on-disk version; the primary (`_id`) index |
| `memtable` | changes durable in the WAL but not yet in a segment (nil doc = tombstone) |
| `flushing` | the immutable memtable being flushed (still readable) |
| `indexes` | multi-value secondary indexes |
| `inflight` | lower bounds of WAL offsets appended but not yet applied |
| `deadBytes`, `segBytes` | per-segment byte accounting for compaction |
| `count` | live document count, O(1) |

The LRU document cache (`pkg/db/internal/cache`) is one per engine, bounded
by `CacheSize` bytes (64 MiB, can be disabled). It stores encoded JSON keyed by
`(db, coll, id)` and tagged with the WAL offset, so a stale entry never
matches a newer version.

## Write path (`pkg/db/document.go`)

```
1. Database.mu (shared, held to the end; blocks drops/index changes)
2. docLock(id): 256 striped mutexes, FNV-1a of the id
3. read current version: memtable → flushing → cache → keydir + segment
4. build after-image (insert: ErrExists if present; update: merge top-level
   fields; delete: ErrNotFound if absent), JSON round-trip, validate schema
5. register inflight; WAL append + fsync            ← commit point
6. under Collection.mu: memtable[id] = after-image@offset, update indexes,
   invalidate cache, count; remove from inflight
```

Writes become visible only after they are durable. If the append fails,
nothing is applied. Writes to different documents commit concurrently and
share fsyncs; writes to one document are serialized, so read-modify-write
updates cannot be lost. Metadata operations append their WAL record, then
write the meta file(s) and directories, then update memory. Drops rename
the directory to `.deleted-<offset>` (atomic) and remove it in the
background; `Open` removes leftovers.

## Flush, checkpoint markers, low-water mark (`pkg/db/flush.go`)

Each collection is flushed every `FlushInterval` (1s), immediately when its
memtable exceeds `MemtableSize` (16 MiB), and on `Close`:

1. Under `Collection.mu`: `flushing = memtable`, new memtable, compute the
   checkpoint `cp` (one below the smallest offset of this collection's
   records the flush does not cover).
2. Append the entries (sorted by WAL offset) and a checkpoint marker with
   `cp` to the active segment; one fsync. No collection lock is held.
3. Under `Collection.mu`: repoint keydir entries whose offset is newer,
   drop tombstoned ids, account dead bytes, clear `flushing`, store `cp`.
4. Seal and roll the segment if it is over `DataSegmentSize`.

A failed flush truncates the segment back, keeps `flushing` readable and the
old checkpoint, and retries next tick. The cost of a flush depends on the
number of changed documents, not the collection size.

The engine **low-water mark** is the smallest WAL offset not yet durable in
a data segment or metadata file: min over every collection's memtable,
flushing, and inflight offsets, in-flight metadata operations, and the
WAL's next offset. WAL segments entirely below it are processed and may be
deleted by retention.

## Recovery at `Open` (`pkg/db/engine.go`, `pkg/db/recovery.go`)

1. Take `cachydb.lock` (`ErrDataDirInUse`), check `cachydb.format`, remove
   `.deleted-*` leftovers, open the WAL (torn-tail truncation).
2. Read every `db.meta` and `coll.meta`. A newer `schema_version` fails
   with `ErrSchemaTooNew`; an older one logs a warning.
3. Per collection, in parallel (bounded by GOMAXPROCS): build the keydir
   from hint files or segment scans (highest WAL offset wins, a tombstone
   removes the id), truncate a torn tail in the newest segment, read the
   newest checkpoint marker, load or rebuild the indexes.
4. Fail with `ErrWALCorrupt` if the data references an offset at or beyond
   the end of the WAL (WAL files are missing).
5. Replay the WAL from the oldest offset any meta file or checkpoint may be
   missing. Each record is applied only if its effect is missing: document
   records iff their offset beats the current version's; create/drop
   records iff the meta file's `wal_offset` (and `created_offset`) is
   older. Replay never fails on "already exists" or "not found". Replayed
   changes land in memtables and are flushed normally.
6. Start the flusher, compactor, and WAL retention.

## Compaction (`pkg/db/compact.go`)

One goroutine checks every collection each `FlushInterval`. A collection
is compacted when its candidate segments (sealed, excluding the active
segment and the segment holding the newest checkpoint marker) number at
least 2 and their dead bytes reach `CompactionRatio` (0.5) of their total.

- **Run selection** (`pickCompactionRun`): the contiguous run of candidates
  with the most dead bytes (the oldest on ties), capped at one segment
  (`DataSegmentSize`) of live data. It is not strictly the oldest run.
- The active segment is sealed first, so every later flush goes to a newer
  segment than the output.
- A put is copied iff the keydir still points at it. Tombstones are dropped
  only when the run starts at the oldest segment on disk; otherwise they are
  copied. Checkpoint markers are not copied.
- The output (one new segment plus hint) is fsynced, then the keydir is
  repointed by compare-and-swap on `(location, walOffset)`. The old segments
  are retired: hints removed at once, data files once no read holds them.
  Index snapshots are rewritten.

## Concurrency and lock order

```
Engine.metaMu → Database.mu → compactMu → flushMu → Collection.mu
                Database.mu (shared) → docLock → Collection.mu
```

| Lock / goroutine | Guards / role |
|------------------|---------------|
| `Engine.mu` | databases map only; never held during I/O |
| `Engine.metaMu` | create/drop database, including I/O |
| `Database.mu` | collections map; exclusive for metadata ops (incl. the whole `CreateIndex` build); shared by document writes |
| `docLocks[256]` | one document's read-modify-write; held across the WAL append |
| `Collection.mu` | keydir, memtables, indexes, inflight; memory-only sections |
| `flushMu`, `compactMu` | serialize flush / compaction per collection, and against drops |
| WAL writer, flusher, compactor, retention, drop removal | background goroutines; all exit on `Close` |

`Close` waits for in-flight operations, stops the background goroutines,
runs a final flush, writes index snapshots, closes segments and the WAL,
and releases the lock. Public calls after `Close` return `ErrClosed`.

## Crash-point test hooks

`Options.testHook` is unexported and only set from `pkg/db/export_test.go`.
The engine calls it at the points in `pkg/db/hooks.go` (after the WAL fsync
before apply, after the segment write before fsync, after the segment fsync
before the keydir update, before the compaction swap, during metadata
writes). A hook can simulate a crash; the test then reopens the directory
(`pkg/db/crash_test.go`). It is always nil outside tests.
