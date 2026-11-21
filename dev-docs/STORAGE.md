# Storage Engine Internals

How `pkg/db` stores data: on-disk layout and formats, the write-ahead log,
data segments, flush and checkpoints, recovery, compaction, the lock
order, and offline verification. For the public Go API and query semantics see
[DATABASE_ENGINE.md](DATABASE_ENGINE.md); for the big picture see
[PROJECT.md](PROJECT.md).

## Design in one paragraph

Every change is appended to a **write-ahead log (WAL)** and fsynced before
it is applied or acknowledged; concurrent writers share fsyncs (**group
commit**). Applied changes sit in a per-collection **memtable**. A
background **flusher** appends only the changed documents and tombstones to
the collection's **append-only data segment** with one fsync, then applies
the **key table** (id → segment location), the secondary index entries, the
live count, the segment accounting, and the **checkpoint** in one
transaction of the collection's **index file** (`index.db`, a bbolt B+tree
file). Reads go memtable → **LRU cache** → key table → segment; queries
stream from index or key-table cursors merged with the memtable. Background
**compaction** rewrites sealed segments to drop dead records, and **WAL
retention** deletes processed WAL segments by age and/or size. Every record
carries its WAL offset, and replay applies "last writer wins by WAL
offset", so recovery is idempotent. Only memtables, the bounded cache, and
small per-collection bookkeeping live in RAM; the Go heap does not grow
with the number of documents (format version 3, plan 013).

## On-disk layout

```
<root>/                               # Options.Dir (CLI: -R, default ~/.cachydb)
├── cachydb.lock                      # exclusive flock target (data-dir lock)
├── cachydb.format                    # {"format":"cachydb","version":3}
├── cachydb.metacp                    # {"offset":N}: metadata checkpoint (see Recovery)
├── wal-00000000000000000001.log      # WAL segments, named by first offset (20 digits)
├── wal-00000000000004213377.log
├── .deleted-<offset>/                # transient: dropped database awaiting removal
└── <db>/
    ├── db.meta                       # JSON, atomic write
    ├── .deleted-<offset>/            # transient: dropped collection awaiting removal
    └── <coll>/
        ├── coll.meta                 # JSON, atomic write
        ├── index.db                  # index file (bbolt): key table, secondary indexes, count, checkpoint
        ├── seg-000001.data           # sealed data segment
        └── seg-000002.data           # active data segment (append-only)
```

Format version 2 directories (v0.0.4–v0.0.6) also had `seg-NNNNNN.hint`
(hint files) and `idx-<index>.snap` (index snapshots) in each collection
directory. Format 3 neither writes nor reads them; the upgrade deletes
them (see "Format file and old data directories").

- Root files contain a `.`; database, collection, and index names cannot
  (`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, `pkg/db/names.go`), so names never
  collide with engine files and never escape the root.
- `_system/` is the one reserved database directory
  (`db.SystemDatabaseName`, `Engine.SystemDatabase`): an ordinary database
  (users and token digests of `internal/auth`) whose name breaks the
  public rule, so `ValidateName` and every public call reject it.
  Loading, recovery, leftover removal, flush, compaction, the format
  upgrade, and `Verify` include it; `ListDatabases` (and so `utils list`,
  `list_databases`, export) and `MigrateAll` leave it out.
- File names: `pkg/db/engine.go` (`cachydb.lock`), `pkg/db/format.go`,
  `pkg/db/metacp.go`,
  `pkg/db/meta.go`, `pkg/db/internal/wal/segment.go`,
  `pkg/db/internal/segment/segment.go`, `pkg/db/internal/idxstore/store.go`
  (`index.db`).

### Format file and old data directories

`checkDataDir` (`pkg/db/format.go`) runs on every `Open`. A missing
`cachydb.format` in a root that holds legacy files (`wal.checkpoint`,
`wal-<unix>-<offset>.log`, or any `<db>/db.meta.json`) fails with
`ErrIncompatibleDataDir`: *"data directory … was created by an older
cachydb format; move or delete it"*; there is no converter for those
(pre-v0.0.4) directories. A version below 2 also fails (*"… cannot upgrade
(it upgrades version 2) …"*), and so does a version newer than
`FormatVersion` (3). An empty root gets the file written atomically.

**Upgrade from format 2** (`pkg/db/upgrade.go`, `Engine.upgradeFormat`).
Version 2 makes `checkDataDir` report `upgrade = true`. `recover` then runs
the upgrade right after reading the metadata and before loading the
collections and replaying the WAL:

1. Delete every `index.db` in the directory (in a format-2 directory it can
   only be the partial result of an interrupted upgrade).
2. One collection at a time (database, then collection name order, to bound
   memory to one id map): log INFO *"upgrading data directory format 2 ->
   3; building index"* and build `index.db` from the data segments with the
   rebuild routine (`rebuildIndex`, see Recovery). The checkpoint becomes
   the newest checkpoint marker found in the segments (0 if none).
3. After every collection succeeded, delete `seg-*.hint`, `idx-*.snap`, and
   their leftover temporary files, then write `cachydb.format` with version
   3 (atomic write).

Until step 3 the directory still says version 2, so a crash anywhere
restarts the upgrade from step 1. An upgrade that fails without a crash
(for example a document id longer than 31,742 bytes, or an I/O error)
removes the `index.db` files it built and leaves the format-2 files, so the
old release still opens the directory. No document is rewritten; the WAL
and data segments keep their format. The upgrade is **one-way**: an older
binary cannot open a format-3 directory, so copy the directory or export
first (README "Upgrading").

### Metadata files

All metadata writes use `fsutil.WriteFileAtomic` (temp file → fsync →
rename → fsync directory).

Platform notes (`pkg/db/internal/fsutil`): the data-directory lock is a
non-blocking `flock` on linux, darwin, freebsd, openbsd, netbsd, and
dragonfly (`lock_flock.go`). Every other platform (Windows, Solaris,
illumos, AIX, …) gets a no-op lock (`lock_other.go`) whose `Supported()`
is false, and `Open` logs *"data-dir locking unsupported on <GOOS>"*.
`SyncDir` treats `EINVAL` and `ENOTSUP` from a directory fsync as success
(some network and FUSE file systems reject it); on non-unix platforms it is
a no-op.

| File | JSON fields |
|------|-------------|
| `db.meta` | `name`, `schema_version`, `wal_offset` |
| `coll.meta` | `name`, `schema` (or `null`), `indexes` (`[{name, field, unique}]`, sorted; `unique` omitted when false), `wal_offset`, `created_offset` |

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
| 7 | `create_index` | `{"name", "field"}`, plus `"unique": true` for a unique index |
| 8 | `drop_index` | `{"name"}` |
| 9 | `update_schema` | `{"schema": <Schema or null>}` (plan 004) |
| 10 | `rename_coll` | `{"new_name"}`; `coll` is the old name (plan 004) |
| 11 | `batch` | binary item list (below); `id` is empty (plan 013) |

**Batch payload** (op 11, `internal/wal/batch.go`, little-endian): `u32
count`, then per item `u8 op · u16 id_len · id · u32 doc_len · doc`, where
`op` is 1 (put: `doc` is the after-image, as `put_doc`'s payload) or 2
(delete: `doc_len` 0). Ids are not empty. One record holds the changes of
one collection (`db`, `coll` in the header) and every item's version is
the record's offset, so an id appears at most once per batch. A record
larger than the group-commit cap (4 MiB) is written alone. Decoding is
strict (count, ops, lengths, no trailing bytes); a malformed payload fails
replay with `ErrWALCorrupt`.

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
  marker (`wal_offset` holds a checkpoint value, no id). Format 3 no
  longer writes markers (the checkpoint is in `index.db`); readers accept
  and ignore them in old segments, the upgrade takes the newest one as the
  initial checkpoint, and compaction does not copy them.
- Bodies are zstd-compressed JSON (`klauspost/compress`, `SpeedDefault`,
  shared encoder/decoder). Documents under 64 bytes are stored with codec 0
  (uncompressed).
- The active segment is appended to; when it exceeds `DataSegmentSize`
  (64 MiB) it is **sealed** (closed; it was fsynced by the flush) and the
  next flush opens `seg-<n+1>.data`. Sealing writes no other file. Segment
  ids only grow (compaction outputs take ids from the same counter).
- Reads use `ReadAt` on files opened once and ref-counted, so compaction can
  retire a segment while a read holds it.
- **Header-only scan** (`segment.ScanHeaders`): reads each record's length,
  type, id, and WAL offset and verifies its CRC without decompressing the
  body. Compaction uses it to find a segment's records, and the rebuild
  and upgrade use it to rebuild the key table (about 20× faster than a
  full `Scan`).

## Index file (`index.db`, `pkg/db/internal/idxstore`)

One bbolt file per collection directory, so drops and renames stay
directory operations. `idxstore` is the only package that imports
`go.etcd.io/bbolt`; no bbolt type leaves it. The file is opened with the
map free-list type, the free list synced, and a 1 s lock timeout.

| Bucket | Key | Value |
| --- | --- | --- |
| `meta` | fixed names | `format` (layout version), `complete` (absent while a rebuild is in progress), `checkpoint` (u64 WAL offset), `maxwal` (highest WAL offset any reflected change has), `count` (live documents), `covered` (segment id · end position the file reflects), `seg:<u32>` per live segment (record bytes, dead bytes), `ix:<name>` per index (`complex`, `truncated`, `arrays` counters, ready flag) |
| `keydir` | document id | segment id (u32) · position (u64) · size (u32) · WAL offset (u64): 24 bytes |
| `ix/<name>` | encoded value · `0x00 0x00` · document id | 1 flag byte: bit 0 = array element, bit 1 = value truncated |

Integers are big-endian. The value encoding is order-preserving (missing
< null < false < true < numbers < strings, the engine's sort order); a
missing field has its own entry, so every document is in every index. The
encoding, flags, and counters and the query rules built on them are in
[DATABASE_ENGINE.md](DATABASE_ENGINE.md#indexes). Document ids are at most
**31,742 bytes** (`idxstore.MaxIDLen`): a bbolt key holds 32,768 bytes and
an index key is up to 1,024 bytes of value, 2 bytes of terminator, and the
id.

Everything in `meta` changes in the same transaction as the entries it
describes, so "the file reflects the WAL up to `checkpoint` and the
segments up to `covered`" is atomic. Every change is one `Store.Update`
(one bbolt transaction, fsynced at commit). Large transactions apply their
keys in ascending order per bucket: bbolt splits leaves only at commit,
and random-order inserts into a small bucket cost quadratic time.

Readers use snapshot read transactions. bbolt has one writer at a time;
a commit that has to grow the file (remap the mmap) waits until every
open read transaction ends. A query keeps one read transaction open while
it streams its results, so a long query can delay the flush that grows
`index.db` (writers are not blocked: they only append to the WAL and the
memtable, up to `MaxPendingBytes`). Never open a read transaction inside
an update, or start an update while holding a read transaction in the
same goroutine (deadlock on remap).

`Store.Check` (bbolt's page check plus the invariants: entries well
formed, every key in a registered segment, `count` equal to the keys,
segment bytes equal to the referenced bytes, every index entry belonging
to a key, the counters) reads the whole file; it is for tests and
`cachydb utils verify`, not for `Open`. `Open` runs a cheap check only (file length against the
newest transaction, the meta pages, the `meta` bucket and layout).

## In-memory state per collection (`pkg/db/collection.go`)

| Field | Role |
|-------|------|
| `store` | the open `index.db` (`*idxstore.Store`); nil before load, after drop/detach/close, and on a renamed collection until first use (`indexStore` opens it lazily, under `storeMu`) |
| `memtable` | changes durable in the WAL but not yet in the index file (nil doc = tombstone); each `memEntry` also carries the after-image's WAL `payload` bytes (written to the segment by the flush as is, never encoded again), `base`/`baseOff` (below), and its `size` (`memEntrySize`: id + 2 × payload, for the payload bytes and the decoded document, + 64; what `MemtableSize` and `MaxPendingBytes` count) |
| `flushing` | the immutable memtable being flushed (still readable) |
| `indexes` | the secondary index definitions (`indexDef{name, field, unique}`); the entries are in `index.db` |
| `uniquePending` | unique index → encoded value → ids of unflushed documents holding it (hints for the uniqueness check) |
| `inflight` | lower bounds of WAL offsets appended but not yet applied |
| `checkpoint` | the checkpoint as of the last index commit |
| `keptTombs` | per compaction output, tombstone bytes it had to copy (planning only, not persisted) |
| `count` | live document count, O(1): the persisted count plus the overlay's delta |

There is no map of all ids or index entries: the key table and the indexes
are only in `index.db` (and the OS page cache). `memEntry.base` holds the
index keys of the on-disk version that the first write of this memtable
entry replaced, and `baseOff` that version's WAL offset (0 if there was
none); later writes to the same id keep them. The flush deletes exactly
those keys, so it reads no old document from disk. If `baseOff` no longer
matches the key table, or `base` lacks an index created since,
`oldIndexKeys` reads the replaced version from its segment instead.

The LRU document cache (`pkg/db/internal/cache`) is one per engine, bounded
by `CacheSize` bytes (64 MiB, can be disabled). It stores immutable
**decoded** documents (`cachedDoc`, charged twice their encoded length)
keyed by `(db, coll, id)` and tagged with the WAL offset, so a stale entry
never matches a newer version. The cached pointer is never handed out:
`Get` returns a deep clone, and `Find` clones only the documents it
returns. A cached `Get` is ~8.5× faster than an uncached one
(`BenchmarkGet`).

## Write path (`pkg/db/document.go`)

```
0. backpressure: while the engine's unflushed bytes >= MaxPendingBytes,
   wait (no lock held; ctx-aware; ErrClosed on Close) and kick the flusher
1. Database.mu (shared, held to the end; blocks drops/index changes)
2. docLock(id): 256 striped mutexes, FNV-1a of the id
3. read current version: memtable → flushing → key table (short read
   transaction) → cache or segment
4. check the `if` condition against the current version (ErrConditionFailed),
   build after-image (insert: ErrExists if present; update: merge top-level
   fields then unset, or upsert; replace: whole document; delete: ErrNotFound
   if absent), JSON round-trip, validate schema
5. unique indexes: for each new value of a unique index, take its
   uniqueLocks stripe (ascending) and check the value against
   uniquePending/the overlay and index.db (ErrUniqueViolation)
6. register inflight; WAL append + fsync            ← commit point
7. under Collection.mu: memtable[id] = after-image@offset (keeping the
   first base/baseOff), uniquePending, invalidate cache, count; remove
   from inflight
```

No read transaction is open across the WAL append. Index entries are not
touched on the write path: the flusher applies them.

**Batches** (`pkg/db/batch.go`, op 11) follow the same steps for many
documents of one collection: build every operation first (the single
writes' build callbacks), then 0. backpressure, 1. `Database.mu` shared,
2. the docLock stripes of all ids in ascending stripe order, each once
(a single write holds one stripe, so no deadlock), 3–4. read, build, and
validate every after-image in operation order, 5. unique values: the
batch's documents against each other, then the stripes of every value an
operation adds (ascending, each once) and the check against the overlay
and `index.db` leaving out the batch's own ids, 6. register inflight
once, one WAL append + fsync (`hookAfterWALSync`), 7. apply every item
under one `Collection.mu` section, each with the record's offset. The
memtable, flush, and segments see ordinary puts and tombstones.
`UpdateMany`/`DeleteMany` find the ids on a snapshot first
(`hookAfterMatch` follows), then run one batch whose build callbacks
re-check the query under the document locks and return `errSkipWrite` for
a document that no longer matches: `writeBatch` leaves it out (and out
of the batch's unique-check exclusions); if every write is skipped,
nothing is appended.

Writes become visible only after they are durable. If the append fails,
nothing is applied. Writes to different documents commit concurrently and
share fsyncs; writes to one document are serialized, so read-modify-write
updates cannot be lost. Metadata operations append their WAL record, then
write the meta file(s) and directories, then update memory. Drops rename
the directory to `.deleted-<offset>` (atomic) and remove it in the
background; `Open` removes leftovers.

`update_schema` (op 9) validates every document under `Database.mu`
(exclusive), appends the record, rewrites `coll.meta`, then swaps the
schema in memory.

`rename_coll` (op 10), under `Database.mu` exclusive and the collection's
`compactMu` and `flushMu`: flush the collection first (no write can run,
so afterwards every change is in its data segments and replay never needs
a record under the old name), append the record (commit point;
`hookMetaWrite`), detach the collection (flag it dropped, close its
active segment and readers, evict its cache entries), `os.Rename` the
directory and fsync the database directory (`hookRenameDir`), rewrite
`coll.meta` with the new name and `wal_offset` (keeping
`created_offset`), and register a new `*Collection` that takes over the
in-memory state (`moveState`); its active segment and `index.db` reopen
lazily. If a step after the commit fails, the collection is unavailable
until a restart replays the record. Drops close the collection's segments
and `index.db` (`markDropped`) and remove it from memory before the
directory rename; if the rename fails, the error says to restart, and the
replayed drop completes it.

**Index changes.** `CreateIndex` holds `Database.mu` exclusively, then the
collection's `compactMu` and `flushMu`, for the whole build: it reads every
document the key table lists in one read transaction, sorts the entries,
writes them in batches of 50,000 (fill percent 0.9), sets the counters and
the ready flag in the last transaction, then appends the WAL record,
writes `coll.meta`, and publishes the definition. A unique index over
duplicate values fails before anything is written. On any failure before
the publish the new bucket is dropped. `DropIndex` holds `Database.mu` and
`flushMu`, appends the record, publishes the removal, and drops the bucket.
Both remove the index from every memtable entry's `base`.

## Flush, checkpoint, low-water mark (`pkg/db/flush.go`)

Each collection is flushed every `FlushInterval` (1s), immediately when its
memtable exceeds `MemtableSize` (16 MiB), and on `Close`. One flush pass
(`flushAll`) flushes the eligible collections in parallel through a pool of
`FlushWorkers` goroutines (default `min(GOMAXPROCS, 4)`); flushes of one
collection are still serialized by its `flushMu`:

1. Under `Collection.mu`: `flushing = memtable`, new memtable, compute the
   checkpoint `cp` (one below the smallest offset of this collection's
   records the flush does not cover).
2. Append the entries (sorted by WAL offset) to the active segment, each
   put with the WAL payload bytes its entry kept (no re-encoding); one
   fsync. No collection lock is held. (`hookAfterSegWrite`,
   `hookAfterSegSync`.)
3. One `index.db` transaction (`applyFlush`), no collection lock held: for
   each entry, if the key table's offset for the id is not older, count the
   new record as dead and skip it; otherwise delete the old version's index
   entries (from `base`), write the new key-table entry and index entries
   (or delete the id for a tombstone), and adjust the `complex`, `arrays`,
   and segment byte counters. Keys are applied in ascending order per
   bucket. Then write `checkpoint = cp`, `covered = (segment, end)`, and
   `maxwal`, and commit (bbolt fsyncs). (`hookAfterIndexCommit`.)
4. Under `Collection.mu`: clear `flushing`, drop the flushed values from
   `uniquePending`, store `cp`; release the pending bytes.
5. Seal and roll the segment if it is over `DataSegmentSize`.

Readers stay correct throughout: a query captures the overlay (memtable
and `flushing`) before it opens its read transaction, and `flushing` is
cleared only after the commit, so a change is always in one of the two.

A failed flush keeps `flushing` readable and the old checkpoint, and
retries next tick. If the segment append or fsync failed, the segment is
truncated back (`abandonActive`). If the index transaction failed before
commit, the records stay unreferenced and the segment is truncated back;
if it failed during commit (bbolt may have written it), the records stay
and the writer is reopened by the next flush. The cost of a flush depends
on the number of changed documents, not the collection size (see the 10M
run in [DEVELOPMENT.md](DEVELOPMENT.md#scale-benchmark)).

**Backpressure** (`pkg/db/backpressure.go`): the engine keeps an atomic
count of the unflushed bytes of all collections (memtable plus flushing
entry sizes). A document write waits before its WAL append while the count
is at or above `MaxPendingBytes` (default `4 × MemtableSize ×
max(1, GOMAXPROCS/2)`, capped at 1 GiB; negative = no limit), so memory
stays bounded when the disk is slower than the writers. A waiting writer
kicks the flusher, which then flushes every collection rather than only
full memtables. Each completed flush and each drop releases its bytes and
wakes the waiters; `Close` wakes them with `ErrClosed`. Metadata
operations do not wait.

The engine **low-water mark** is the smallest WAL offset not yet durable in
a data segment or metadata file: min over every collection's memtable,
flushing, and inflight offsets, in-flight metadata operations, and the
WAL's next offset. WAL segments entirely below it are processed and may be
deleted by retention.

## Recovery at `Open` (`pkg/db/engine.go`, `pkg/db/recovery.go`)

1. Take `cachydb.lock` (`ErrDataDirInUse`), check `cachydb.format`, remove
   `.deleted-*` leftovers, open the WAL (torn-tail truncation).
2. Read every `db.meta` and `coll.meta`. A newer `schema_version` fails
   with `ErrSchemaTooNew`; an older one logs a warning. In a format-2
   directory, run the upgrade now (see "Format file and old data
   directories").
3. Per collection, in parallel (bounded by GOMAXPROCS; `Collection.load`):
   open `index.db` and read its checkpoint, count, covered position, and
   segment accounting; then align the segments with it (`alignSegments`):
   delete segment files it does not register (an unfinished compaction
   output, or a first flush into a new segment, that did not reach its
   index commit, or compaction inputs that did) and truncate the covered
   segment to `covered` (a flush that did not reach its index commit, or a
   torn write; WARN "torn tail"). If `index.db` is missing, fails the cheap
   open check, is incomplete, or refers to data that is not there (a
   registered segment without its file, `covered` past the segment end),
   it is **rebuilt** from the segments with a WARN (`rebuildIndex`: header
   scan of every segment, highest WAL offset per id wins, a tombstone
   removes the id; key table in sorted batches of 50,000, every index of
   `coll.meta` built from the documents, then the accounting, `covered`,
   `maxwal`, and the checkpoint: the newest old checkpoint marker, else 0;
   `complete` set last). A rebuild that meets an id over 31,742 bytes fails
   `Open` before changing anything. Last, `syncIndexes` drops index buckets
   with no definition in `coll.meta` (INFO) and builds definitions whose
   bucket is missing or not ready (WARN).
4. Fail with `ErrWALCorrupt` if the data references an offset at or beyond
   the end of the WAL (the index files' `maxwal`; WAL files are missing).
5. Replay the WAL from the oldest offset any meta file or checkpoint may be
   missing: `min(metacp+1, every collection's replay floor + 1)`, where the
   floor is `max(checkpoint, created_offset, the database's wal_offset)`.
   `cachydb.metacp` holds `{"offset": N}`: every metadata record at or
   below N is reflected in the meta files. N is the metadata-only
   low-water mark − 1, rewritten atomically (when it advances) after each
   metadata operation, after recovery, and on `Close`. A missing,
   unreadable, or implausible file (N at or past the end of the WAL) logs
   a warning and falls back to one past the minimum meta-file
   `wal_offset`. An idle collection still pins the start at its last
   checkpoint. Each record is applied only if its effect is missing: document
   records iff their offset beats the current version's (a `batch` record
   item by item, each with the record's offset, so replaying it again or
   after some of its items were flushed changes nothing more); create/drop
   records iff the meta file's `wal_offset` (and `created_offset`) is
   older; `update_schema` likewise. Replay never fails on "already exists"
   or "not found". Replayed changes land in memtables and are flushed
   normally.
   `rename_coll` (`replayRename`) handles, in order: a directory already
   renamed whose `coll.meta` still has the old name (a crash between the
   two: step 2 loads it under the old name with `pendingRename` set, and
   records about the new name before the rename are skipped as earlier,
   dropped collections of that name) → rewrite `coll.meta` and re-key; the
   new name already holding the renamed (or a later) collection → the
   rename is applied, and an old-name collection replay re-created from
   older records (`created_offset` below the record) is removed like a
   drop; the old collection predating the record with the new name free →
   rename the directory, rewrite `coll.meta`, re-key; otherwise skip.
   Replay compares a document record's offset with the memtable, then the
   key table; a replayed put on a collection with indexes reads the
   previous version from its segment (its index keys become `base`). A
   replayed put that violates a unique index is applied with a WARN
   (*"WAL replay applied a write that violates a unique index"*): replay
   never fails on committed data.
6. Write `cachydb.metacp`, then start the flusher, compactor, and WAL
   retention.

**Restore the whole data directory, never `index.db` alone.** Step 3
trusts any `index.db` that passes the open check. If the file is valid but
stale (an older copy restored on its own, or bbolt falling back to its
previous meta page after media damage), `alignSegments` deletes the
segment files that copy does not register and truncates the covered
segment to its old `covered`. Replay restores only the records still in
the WAL; if retention has already removed them, those documents are lost
without a warning. To repair a damaged `index.db`, delete it and let
`Open` rebuild it from the segments. See Known Limitations in
`DEVELOPMENT.md`. `cachydb utils verify` detects such a file before
`Open` acts on it (see "Offline verification"), and `utils verify
--rebuild-index` replaces it.

Open no longer scans the data segments (unless it rebuilds an index file
or builds an index). Its cost is the WAL open (every retained WAL segment
is read and CRC-checked, up to `WALRetentionSize`) plus the replay of the
tail, not the number of documents. The
open log line reports `recoveryStats`: `collections_upgraded` (format 2 → 3
builds), `index_files_rebuilt` (format-3 repairs only), `segments_scanned`
(by the upgrade and those rebuilds), and the replayed and skipped record
counts.

## Offline verification (`utils verify`, `pkg/db/verify.go`)

`db.Verify(ctx, Options, VerifyOptions)` checks a data directory without
`Open`: it takes the data-directory lock, reads the format file and the
metadata (`loadDatabase`; an unreadable database is a problem, not the end
of the check), runs no recovery, starts no goroutine, and writes nothing
unless `RebuildIndex` is set. It returns a `VerifyReport` with
**problems** (damage, or data `Open` would lose or fail on) and
**notices** (what `Open` repairs by itself without losing data).

1. **WAL** (`wal.Verify`, `internal/wal/verify.go`): the checks of
   `wal.Open` (header, checksums, offset continuity across segments, the
   torn-tail rule of the newest segment, which is a notice), but it reads
   on after damage, starting the next segment at its name. Every
   `put_doc` and `delete_doc` record and every item of a `batch` record
   (op 11, `wal.DecodeBatch`) must meet replay's id rule
   (`checkReplayID`, shared with `replayItem`: at most `maxIDLen` bytes),
   and every put must decode as replay decodes it (`decodeDocument`). A
   batch record that repeats an id (`repeatedBatchID`; the format allows
   each id once, replay applies the first and skips the rest) is a
   problem too. Metadata payloads must be JSON. With damage the end of
   the WAL is unknown, so the checks that need it are skipped.
2. **Per collection** (format 3): `idxstore.Open`, then the full
   `Store.Check` (a missing, damaged, or incomplete file is a problem;
   with bbolt page damage, `idxstore.PageProblemPrefix`, the file is not
   read further). The segment files are compared with the registered
   segments and `covered` as `alignSegments` would: a registered segment
   without its file, or a covered segment shorter than `covered`, is a
   problem; an unregistered file or bytes past `covered` are notices,
   except the header-only segment newer than the covered one that a
   flush filling its segment creates (`sealActive`): that is the normal
   state after a clean close and gets no finding.
3. **Every data segment record** (`segment.Scan`: checksum,
   decompression) in one read transaction. Damage in the data `index.db`
   covers is a problem (the records after it cannot be scanned); damage
   past it is a notice. Without a usable `index.db` (missing, damaged, or
   a format-2 directory) every damage is a problem except a torn final
   write of the newest segment (`segment.CheckActive`, the read-only rule
   of `OpenActive`), which the rebuild at `Open` truncates: a notice.
   Each record is looked up in the key table:
   - a put the entry points at must have the entry's size and WAL offset
     and lie in covered data; its document must decode;
   - a covered record newer than the key table's version of its id (or a
     covered put of an id the key table lacks, with no newer tombstone)
     means `index.db` is **older than the segments**: one problem per
     collection with the count and the oldest such change;
   - an uncovered record newer than the key table that replay will not
     restore (its offset is at or below the checkpoint, or no longer in
     the WAL) is data `Open` would **discard** (follow-up F-010): one
     problem per collection.
   Key-table entries the scan did not meet at their record are then read
   by location (only when there are some) and reported when the record
   is not a put of their id with their offset.
4. **`Deep`**: for every ready index of `coll.meta`, each live document's
   entries (`indexKeysOf`) must be in the bucket with the same flags, the
   bucket must hold no other entry (found by reading the documents of the
   entries only when the bucket is larger than expected), and the
   `complex` and `arrays` counters must match. Indexes that are missing,
   not ready, or not defined are notices (`syncIndexes` handles them).
5. **References**: no meta file or index file may reference a WAL offset
   at or past the end of the WAL (the check of `recover`).

`RebuildIndex` runs `rebuildIndex` (the routine of a damaged file at
`Open` and of the format upgrade) for each collection in scope through a
minimal `Engine` (metadata and a disabled cache, no WAL), closes the
files, and then verifies. It refuses a format-2 directory. The rebuilt
file's checkpoint is the newest old marker or 0, so the next `Open`
replays what the WAL still holds for the collection.

## Compaction (`pkg/db/compact.go`)

One goroutine checks every collection each `FlushInterval`. A collection
is compacted when its candidate segments (the registered segments other
than the active one) number at least 2 and their dead bytes reach
`CompactionRatio` (0.5) of their total, or, failing that, when at least 4
adjacent candidates are each smaller than `DataSegmentSize/4` (merged even
when fully live). The byte accounting comes from `index.db`'s `seg:`
entries, minus the tombstone bytes earlier outputs of this process had to
copy (`keptTombs`).

- **Run selection** (`pickCompactionRun`): the contiguous run of candidates
  with the most dead bytes (the oldest on ties), capped at one segment
  (`DataSegmentSize`) of live data. It is not strictly the oldest run.
  Small-segment merges (`pickSmallRun`) take the oldest run of ≥ 4 small
  adjacent candidates, extended while the merged live bytes fit in
  `DataSegmentSize`.
- The active segment is sealed first, so every later flush goes to a newer
  segment than the output.
- The input records come from the header-only scan; a put is copied iff its
  key-table entry points at exactly that record. Tombstones are dropped
  only when the run starts at the oldest segment on disk; otherwise they are
  copied. Checkpoint markers are not copied.
- The output (one new segment) is fsynced (file and directory), then one
  `index.db` transaction repoints each copied id whose entry still has the
  old location and WAL offset (compare-and-swap; copies of entries a flush
  changed meanwhile count as dead bytes of the output), registers the
  output, and removes the inputs (`hookCompactBeforeSwap` comes before it).
  Then the old segments are retired: their files are removed once no read
  holds them. A crash before the transaction leaves an unregistered output,
  and one after it unregistered inputs; `Open` deletes both. Index entries
  do not change: they hold values and ids, not locations.

## Concurrency and lock order

```
Engine.metaMu → Database.mu → compactMu → flushMu → storeMu → Collection.mu
                Database.mu (shared) → docLock → uniqueLocks (ascending) → Collection.mu
                Database.mu (shared) → docLocks (ascending, Batch) → uniqueLocks (ascending) → Collection.mu
```

`index.db` transactions are not locks in this order, but two rules hold:
`Collection.mu` is never held across a transaction (the store pointer is
read under `mu` and used after it is released), and no read transaction is
held across a WAL append or while starting an update in the same goroutine.

| Lock / goroutine | Guards / role |
|------------------|---------------|
| `Engine.mu` | databases map only; never held during I/O |
| `Engine.metaMu` | create/drop database, including I/O |
| `Database.mu` | collections map, collection schemas; exclusive for metadata ops (incl. the whole `CreateIndex` build, `UpdateSchema` validation, and the flush inside `RenameCollection`); shared by document writes |
| `docLocks[256]` | one document's read-modify-write; held across the WAL append. A `Batch` takes the stripes of all its ids in ascending order, each once |
| `uniqueLocks[256]` | writers of one `(unique index, encoded value)` (`pkg/db/unique.go`); taken after the document lock, only by writes that set a new value of a unique index, several in ascending stripe order, and held until the write is applied (so across the WAL append). The uniqueness check reads `index.db` in a short read transaction that ends before the append: no read transaction is held across the append |
| `Collection.mu` | the `store` pointer, memtables, index definitions, inflight, `uniquePending`, checkpoint; memory-only sections |
| `storeMu` | opening `index.db` on first use after a rename (`indexStore`) against the operations that close it (`markDropped`, `detach`, `closeReaders`) |
| `flushMu`, `compactMu` | serialize flush / compaction per collection, and against drops and index changes (`CreateIndex` takes both, `DropIndex` takes `flushMu`) |
| `Engine.lwMu`, `metacpMu` | leaf locks: in-flight metadata offsets; `cachydb.metacp` rewrites |
| WAL writer, flusher, compactor, retention, drop removal | background goroutines; all exit on `Close` |

`Close` wakes writers waiting on backpressure, waits for in-flight
operations, stops the background goroutines, runs a final flush, writes
`cachydb.metacp`, closes the segments, the index files, and the WAL, and
releases the lock. It writes no other file. Public calls after `Close`
return `ErrClosed`.

## Crash-point test hooks

`Options.testHook` is unexported and only set from `pkg/db/export_test.go`.
The engine calls it at the points in `pkg/db/hooks.go`:

| Hook | Point |
| --- | --- |
| `hookAfterWALSync` | document write or batch: after the WAL fsync, before the apply |
| `hookAfterSegWrite` | flush: after the segment write, before its fsync |
| `hookAfterSegSync` | flush: after the segment fsync, before the index transaction |
| `hookAfterIndexCommit` | flush: after the index transaction committed, before the overlay is cleared |
| `hookCompactBeforeSwap` | compaction: output durable, before the swap transaction |
| `hookMetaWrite` | metadata operations: after the WAL append, before the meta file change |
| `hookRenameDir` | `rename_coll`: after the directory rename, before `coll.meta` |
| `hookUpgradeCollection` | format upgrade: after each collection's `index.db` is built |
| `hookUpgradeBeforeFormat` | format upgrade: after the format-2 files are removed, before `cachydb.format` is rewritten |
| `hookAfterMatch` | `UpdateMany`/`DeleteMany`: after the snapshot found the ids, before the batch locks them (not a crash point: a test changes documents here to exercise skipping) |

A hook can simulate a crash; the test then reopens the directory
(`pkg/db/crash_test.go`). It is always nil outside tests.
