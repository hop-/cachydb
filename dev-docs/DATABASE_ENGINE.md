# Database Engine Reference (`pkg/db`)

`pkg/db` is a self-contained, embeddable storage engine: `db.Open` returns
an `*Engine` that owns durability. The MCP server and the CLI only call its
methods. This doc covers the public API and the data and query semantics.
Storage internals (formats, WAL, flush, recovery, compaction) are in
[STORAGE.md](STORAGE.md); the overview is in [PROJECT.md](PROJECT.md).

## Opening and closing

```go
e, err := db.Open(ctx, db.Options{Dir: "/path/to/data"})
if err != nil { ... }              // ErrDataDirInUse, ErrIncompatibleDataDir, ...
defer e.Close(ctx)                 // final flush, releases the data-dir lock
```

`Open` takes the exclusive data-directory lock (`<Dir>/cachydb.lock`), checks
the on-disk format, runs recovery, and starts the background flusher,
compactor, and WAL retention. A data directory of format 2 (v0.0.4–v0.0.6)
is upgraded to format 3 in place on the first `Open`, one-way (see
[STORAGE.md](STORAGE.md#format-file-and-old-data-directories)). `Close`
waits for in-flight calls, flushes everything, closes the segments, the
index files, and the WAL, and releases the lock.
It is idempotent. Calls after `Close` return `ErrClosed`.

### Options (`pkg/db/options.go`)

Zero-valued fields take the defaults. For `WALRetentionAge`,
`WALRetentionSize`, `CacheSize`, and `MaxPendingBytes` a **negative** value
means "disabled".

| Field | Default | Meaning |
|-------|---------|---------|
| `Dir` | required | Data directory |
| `Logger` | `slog.Default()` (stderr) | All engine logging; `pkg/db` never writes to stdout |
| `WALSegmentSize` | 64 MiB | WAL segment rotation size |
| `WALRetentionAge` | 24h | Keep processed WAL segments this long; negative = no age limit |
| `WALRetentionSize` | 1 GiB | Keep at most this much WAL; negative = no size limit |
| `DataSegmentSize` | 64 MiB | Data segment roll-over size; also the compaction output cap |
| `FlushInterval` | 1s | Flush and compaction tick |
| `MemtableSize` | 16 MiB | Per-collection memtable size that triggers an early flush |
| `CacheSize` | 64 MiB | LRU cache of decoded documents (charged twice their encoded size); negative = cache disabled |
| `CompactionRatio` | 0.5 | Dead/total bytes of sealed segments that triggers compaction; must be in (0,1] |
| `MaxPendingBytes` | `4 × MemtableSize × max(1, GOMAXPROCS/2)`, max 1 GiB | Write backpressure: document writes wait (honoring `ctx`) while the unflushed bytes of all collections are at or above it; negative = no limit |
| `FlushWorkers` | `min(GOMAXPROCS, 4)` | Collections flushed in parallel by one flush pass; must be >= 0 |
| `MaxBatchDocs` | 10,000 | Most operations in one `Collection.Batch` (`ErrBatchTooLarge` above it); must be >= 0 |
| `MaxBatchBytes` | 32 MiB | Largest WAL payload of one batch: ids and document after-images plus 7 bytes per operation and 4 (`ErrBatchTooLarge` above it); must be >= 0 |

The defaults were kept after the benchmarks in `pkg/db/bench_test.go`; the
reasoning is in the comment above the constants in `options.go`.

A document write (`Insert`, `Update`, `Delete`, `Batch`) can block on
`MaxPendingBytes` when the disk is slower than the writers. It returns
`ctx.Err()` if its context ends first (nothing is written) and `ErrClosed`
if the engine closes. `FormatVersion` and `WALFormatVersion` are the
on-disk format versions this build reads and writes.

## API

### Engine (`engine.go`)

| Method | Notes |
|--------|-------|
| `Open(ctx, Options) (*Engine, error)` | See above |
| `Verify(ctx, Options, VerifyOptions) (*VerifyReport, error)` | Offline integrity check of a directory that is not open (takes the lock, `ErrDataDirInUse` otherwise; no recovery; writes only with `RebuildIndex`). `VerifyOptions`: `Database`, `Collection` (scope; the WAL is always checked), `Deep` (every index entry), `RebuildIndex` (rebuild `index.db` from the segments first), `MaxProblems` (default 1,000). `VerifyReport.OK` is false when `Problems` is not empty; `Notices` are repairs `Open` makes itself. Damage is in the report, not the error. See [STORAGE.md](STORAGE.md#offline-verification-utils-verify) |
| `Close(ctx) error` | Idempotent; if `ctx` ends, remaining flushes are skipped (data stays in the WAL) |
| `CreateDatabase(ctx, name) (*Database, error)` | Returns the existing database without writing a WAL record if it exists |
| `Database(name) (*Database, error)` | `ErrNotFound` if missing |
| `ListDatabases() []string` | Sorted; nil when empty; never lists the system database |
| `SystemDatabase(ctx) (*Database, error)` | Creates or returns the reserved database `SystemDatabaseName` (`_system`), which holds `internal/auth`'s users and tokens. `Database`, `CreateDatabase`, and `DropDatabase` reject its name (`ErrInvalidName`), and `ListDatabases` and `MigrateAll` skip it; recovery, flush, compaction, and `Verify` treat it like any database |
| `DropDatabase(ctx, name) error` | Waits for in-flight writes in the database |
| `MigrateDatabase(ctx, name, target) error` | See [Migrations](#migrations) |
| `MigrateAll(ctx, target) error` | Every database `ListDatabases` returns (not the system database) |

### Database (`database.go`)

| Method | Notes |
|--------|-------|
| `Name() string`, `SchemaVersion() int` | |
| `CreateCollection(ctx, name, *Schema) error` | `nil` schema = schemaless; `ErrExists`, `ErrSchemaValidation` (invalid schema) |
| `Collection(name) (*Collection, error)` | `ErrNotFound` if missing |
| `ListCollections() []string` | Sorted; nil when empty |
| `DropCollection(ctx, name) error` | MCP `delete_collection` |
| `RenameCollection(ctx, old, new) error` | Same database; flushes the collection, then WAL op 10 (`rename_coll`) + directory rename + `coll.meta` rewrite; `ErrNotFound`, `ErrExists`; old handles return `ErrNotFound` afterwards |
| `CopyCollection(ctx, name, newName, target *Database) (int, error)` | `target` nil = same database; creates the target with the schema and indexes, then inserts every document (same ids, ascending) through `Collection.Batch`, each batch at most `MaxBatchDocs` documents and about `MaxBatchBytes` (sizes estimated, a batch over the limit is split in halves); **not atomic** as a whole (on error the target holds the batches written before it; drop it); `ErrExists` |

### Collection (`document.go`, `collection.go`)

| Method | Notes |
|--------|-------|
| `Insert(ctx, *Document) (string, error)` | Empty `doc.ID` gets a UUID v7 (generated ids sort in creation order); `ErrExists`; `ErrSchemaValidation` (schema, or `"_id"` key in `Data`) |
| `Get(ctx, id) (*Document, error)` | Deep copy; `ErrNotFound` |
| `Find(ctx, *Query) ([]*Document, error)` | See [Queries](#queries) |
| `FindWithPlan(ctx, *Query) ([]*Document, QueryPlan, error)` | `Find` plus the plan it ran: driver, index, whether the order was provided, examined and returned counts (for `explain`) |
| `CountDocuments(ctx, *Query) (int, error)` | Filters and `Any` only; fast paths below |
| `Distinct(ctx, field, *Query) ([]any, error)` | Arrays contribute each element; deduplicated like `eq`, sorted by the sort order |
| `Update(ctx, id, set map[string]any) (*Document, error)` | `UpdateWith` with only `Set` |
| `UpdateWith(ctx, id, UpdateSpec) (*Document, bool, error)` | `Set`, then remove `Unset`; `If` condition; `Upsert` creates `{_id}+Set` (returns `upserted`); `ErrNotFound`, `ErrConditionFailed`; `"_id"` in `Set`/`Unset` or a field in both → `ErrSchemaValidation` |
| `Replace(ctx, id, *Document, Condition) (*Document, error)` | Whole-document replace; `doc.ID` empty or `id`; `ErrNotFound` (missing), `ErrConditionFailed` |
| `Delete(ctx, id) error` | `DeleteWith(ctx, id, nil)` |
| `DeleteWith(ctx, id, Condition) error` | `ErrNotFound`, `ErrConditionFailed` |
| `Batch(ctx, []WriteOp) (BatchResult, error)` | Many writes of this collection, all or nothing, as one WAL record (op 11) with one fsync; see [Batches](#batches-batchgo) |
| `UpdateMany(ctx, *Query, UpdateSpec, limit int) (matched, modified int, err error)` | Update every match as one batch; see [Writes by filter](#writes-by-filter) |
| `DeleteMany(ctx, *Query, limit int) (matched, deleted int, err error)` | Delete every match as one batch; see [Writes by filter](#writes-by-filter) |
| `CreateIndex(ctx, name, field, ...IndexOption) error` | `field` may be a nested path; `Unique(true)` makes it unique (see [Unique indexes](#unique-indexes)); builds from existing documents; blocks writes to the whole database while it runs; `ErrExists`; `ErrUniqueViolation` if a unique index would cover shared values |
| `DropIndex(ctx, name) error` | MCP `drop_index`; `ErrNotFound` |
| `Schema() *Schema`, `Indexes() []IndexInfo` | Copies; indexes sorted by name, with `Unique` (MCP `describe_collection`) |
| `UpdateSchema(ctx, *Schema) error` | `nil` removes the schema. Validates every document first (all or nothing; the error lists up to 10 failing ids and the total), then WAL op 9 (`update_schema`) + `coll.meta`; holds `Database.mu` exclusively throughout |
| `Count() int` | O(1) |
| `Name() string` | |

### Conditional writes and `$now`

- `type Condition = []QueryFilter`: the stored document must match every
  filter. It is checked in the write's `build` callback, i.e. under the
  per-document lock right before the WAL append, so check and write are
  atomic against other writers of that document. A missing document with
  a non-empty condition fails with `ErrConditionFailed` too. Error text:
  `condition failed: document <id> no longer matches if`.
- `ResolveNow(v, now)` replaces every exact `{"$now": true}` (at any
  depth) with `now` as an RFC 3339 UTC string with nanoseconds. `Insert`,
  `UpdateWith`, `Replace`, conditions, and query filters resolve it
  themselves, once per call, before schema validation.
- Every single write still logs one `put_doc`/`delete_doc` after-image; a
  `Batch` logs one `batch` record holding every item's after-image (or
  delete).

### Batches (`batch.go`)

`Collection.Batch(ctx, ops []WriteOp) (BatchResult, error)` applies many
inserts, updates, replaces, and deletes of one collection atomically: one
WAL record (op 11, `batch`), one fsync, and either every operation is
applied and durable or none is, also across a crash (the record has one
CRC; a torn record is dropped whole by WAL recovery).

```go
type WriteOp struct {
    Kind     OpKind     // OpInsert, OpUpdate, OpReplace, OpDelete
    ID       string
    Document *Document  // OpInsert, OpReplace
    Update   UpdateSpec // OpUpdate
    If       Condition  // OpUpdate, OpReplace, OpDelete
}
type BatchResult struct{ IDs []string; Upserted []bool } // per operation
```

- Each kind behaves exactly as its single write (`Insert`, `UpdateWith`,
  `Replace`, `DeleteWith`): the same build callbacks, conditions, upsert,
  `$now` (one time for the whole batch), and schema validation. An insert's
  id is `ID`, else `Document.ID`, else a generated UUID v7; a replace's is
  `ID`, else `Document.ID`. An update's stored version must match both `If`
  and `Update.If`. A field the kind does not use must be empty.
- An id may appear only once per batch (every item gets the record's WAL
  offset as its version). The second operation fails with `ErrExists`.
- Any failing operation fails the whole call with a `*BatchItemError`
  (`Index`, `Err`; text `batch operation <i>: ...`) that wraps the
  operation's sentinel: `ErrExists`, `ErrNotFound`, `ErrConditionFailed`,
  `ErrSchemaValidation`, `ErrUniqueViolation`. Nothing is written.
- Unique indexes see the batch's final state: the batch's documents must
  not share a value with each other or with a document outside the batch,
  but one operation may take a value another operation of the batch frees.
- More than `MaxBatchDocs` operations, or a payload over `MaxBatchBytes`,
  fails with `ErrBatchTooLarge`, whose text states the limit.
- An empty batch writes nothing. `ops` is not modified or retained.
- Locking and replay: [STORAGE.md](STORAGE.md#write-path-pkgdbdocumentgo).

### Writes by filter

`UpdateMany(ctx, q, u, limit)` and `DeleteMany(ctx, q, limit)` change every
document matching `q` as one atomic batch (one WAL record and fsync) and
return how many documents matched and how many they changed.

- Only `q.Filters` and `q.Any` apply; `Sort`, `Fields`, `Skip`, or `Limit`
  in `q` is an error (the limit is the call's argument). A nil or empty
  query matches every document.
- `UpdateSpec`: `Set` and `Unset` as in `UpdateWith`; `If` adds filters to
  the query (a document that does not match them is not matched);
  `Upsert` is an error. `$now` resolves to one time for the query and the
  update.
- The ids are found on a snapshot in `_id` order (only the ids are kept).
  Each is then written only if, under its document lock, it still exists
  and matches the query; one that stopped matching or was deleted is
  skipped, not an error, so `modified`/`deleted` can be below `matched`.
  When every match is skipped nothing is written.
- `limit > 0` changes at most the first `limit` matches in `_id` order
  (`matched` then counts at most `limit`); repeat the call to work through a larger set in atomic chunks. With no
  limit (or one above `MaxBatchDocs`), more than `MaxBatchDocs` matches
  fail with `ErrBatchTooLarge`; the text states the match count (also
  returned as `matched`) and nothing is written. A payload over
  `MaxBatchBytes` fails the same way.
- A document the update makes invalid (schema, unique index) fails the
  whole call with that document's error (the `*BatchItemError` wrapper is
  removed: the error names the document).

Every acknowledged write is fsynced to the WAL before the call returns.
Returned documents are deep copies: mutating them never changes stored data.

### Sentinel errors (`errors.go`)

Errors are wrapped with context; test them with `errors.Is`.

| Error | When |
|-------|------|
| `ErrNotFound` | Database, collection, document, or index missing (also after a drop) |
| `ErrExists` | Collection, document id, or index name already exists |
| `ErrInvalidName` | Name fails `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` (`names.go`, `ValidateName`) |
| `ErrSchemaValidation` | Invalid schema, document violates the schema (a `*SchemaError` listing every violation wraps it), a reserved `_id` misuse, or `UpdateSchema` found non-matching documents |
| `ErrConditionFailed` | An `if` condition did not match the stored document (or it is missing); nothing was written |
| `ErrUniqueViolation` | A write's value for a unique index belongs to another document, or `CreateIndex` with `Unique(true)` found documents sharing a value; nothing was written |
| `ErrBatchTooLarge` | A `Batch` has more operations than `MaxBatchDocs` or a larger payload than `MaxBatchBytes`, or `UpdateMany`/`DeleteMany` without a limit match more than `MaxBatchDocs` documents; the text states the limit (and the match count); nothing was written |
| `ErrClosed` | Engine closed |
| `ErrDataDirInUse` | Another process (or engine) holds `cachydb.lock` |
| `ErrIncompatibleDataDir` | Old-format or newer-format data directory |
| `ErrWALCorrupt` | WAL damaged before its tail, or WAL files missing |
| `ErrWALFailed` | A WAL fsync failed; writes fail until restart, reads continue |
| `ErrSchemaTooNew` | A database's schema version is newer than `CurrentSchemaVersion` |

## Documents (`types.go`)

```go
type Document struct {
    ID   string         `json:"_id"`
    Data map[string]any `json:"data"`
}
```

- `MarshalJSON`/`UnmarshalJSON` use a flat object: `_id` next to the data
  fields. A string `_id` becomes `ID`.
- Documents are stored as JSON. Each write round-trips the document through
  JSON, so **integers come back as `float64`**, Go slices as `[]any`, and
  structs as `map[string]any`, in the library as well as over MCP.
- `_id` is never a data field: `Insert` rejects `"_id"` in `Data` (set
  `Document.ID`), and `Update` cannot change it. Ids are non-empty strings
  up to 31,742 bytes (a key of the index file holds at most 32,768 bytes;
  before format 3 the limit was 65,535).
- `Clone()` is a deep copy.
- `GetPath(path)` resolves a field name the way queries and indexes do
  (see [Field paths](#field-paths)); `GetValue(name)` is top-level only
  (schemas use it).

## Schemas (`schema.go`, `types.go`)

```go
type Schema struct { Fields map[string]Field `json:"fields"` }
type Field  struct { Type FieldType `json:"type"`; Required bool `json:"required"` }
```

| `FieldType` | Accepted values (after the JSON round-trip) |
|-------------|---------------------------------------------|
| `string` | string |
| `number` | any number |
| `boolean` | bool |
| `object` | JSON object |
| `array` | JSON array |
| `date` | string (no format check) |

- A schema needs at least one field; field names cannot be empty or `_id`;
  types must be one of the above.
- Validation runs on every insert, replace, and upsert and on the result of
  every update. A required field must be present; a present field must
  have the declared type. Fields not in the schema are allowed.
- `ValidateDocument` returns a `*SchemaError{Violations}` listing every
  missing required field, then every wrong type, each in field order:
  `schema validation failed: required field(s) missing: a, b; wrong type:
  c (expected string, got number)`. It wraps `ErrSchemaValidation`.
- `UpdateSchema` changes the schema (see above); `c.schema` is replaced
  only under `Database.mu` held exclusively, which writes hold shared.
- Schema fields are top-level: a field named `a.b` in a schema is the
  top-level key `"a.b"`, never a nested path.

## Queries

```go
type QueryFilter struct { Field, Operator string; Value any }
type Query struct {
    Filters []QueryFilter   // AND
    Any     [][]QueryFilter // OR of AND groups; empty = no constraint
    Fields  []string        // projection (plus _id); empty = all
    Sort    []SortKey       // then _id asc; empty = _id asc
    Limit, Skip int
}
type SortKey struct { Field, Order string } // "asc" (default) | "desc"
```

Semantics (`compare.go`, `query.go`, `plan.go`):

- All `Filters` must match (AND), and when `Any` is set every filter of at
  least one group. `Field` is a field name, `_id`, or a nested path
  (`meta.author`, see [Field paths](#field-paths)).
- Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`
  (case-insensitive substring of a string or of a string array element;
  `""` matches any string or array), `has` (an array element equals the
  value), `has_any`, `has_all` (some / every value of the array value is
  an element), `exists` (value `true`: the field is present, `null`
  included; `false`: it is missing), `starts_with` (a string field begins
  with the value; case-sensitive, byte-wise; array elements do not count).
  `eq`/`ne`/`in` compare arrays as whole values.
- **Compilation.** Each call compiles its filters once (`compileQuery`,
  `compileFilter`): the operator becomes an `opCode`, the value is
  normalized (`filterValue`: scalars compare directly, objects and arrays
  through `reflect.DeepEqual` after `normalize`), `$now` is resolved. An
  unknown operator, `exists` with a non-boolean value, and `starts_with`
  with a non-string value are errors from `Find`, `CountDocuments`,
  `Distinct`, and conditional writes (`if`), before anything is read or
  written. Other ill-typed values match nothing (`in`/`has_any`/`has_all`
  with a non-array, `contains` with a non-string, a range with a value that
  does not order); a query with such an AND filter reads nothing (driver
  `none`).
- Comparison is type-aware: every numeric kind (and `json.Number`) compares
  as `float64`; strings compare lexically with strings; bools and `null`
  support equality only; objects and arrays support equality (deep, with
  numbers normalized). Values of different types never order:
  `gt/gte/lt/lte` and `eq` are false, `ne` is true.
- **A document missing the filtered field matches no operator, including
  `ne`,** except `exists false`. This differs from MongoDB's `$ne` and is
  intentional.
- `in` takes an array; it matches when the field equals any element.
- Order: `Sort` keys, then `_id` ascending (also for `desc` keys: ties keep
  ascending `_id`), using a total order: missing < null < false < true <
  numbers < strings < arrays < objects (arrays and objects by canonical
  JSON). Without `Sort` (or with exactly `_id asc`) the order is `_id`
  ascending; `_id desc` alone is `_id` descending. `Skip` matches are
  skipped, then at most `Limit` are returned (0 or negative = no limit).
  Paging is deterministic. An invalid order or empty sort field is an
  error.
- `Fields` is applied after filtering and sorting; `_id` is always kept.
  A field found through a nested path is returned nested under its path
  (`meta.author` → `{"meta": {"author": …}}`); several paths into one
  object merge.

### Field paths

A field name with dots is a path through nested objects (`Document.GetPath`,
`fieldPath` in `types.go`). It works in filters, `Any` groups, sort keys,
projection (`Fields`), `Distinct`, conditions (`if`), and index
definitions. Resolution: `_id` is the id; a top-level key equal to the
whole name wins (documents with a literal `"a.b"` key keep working);
otherwise the name is split on `.` and followed through objects. A path
that crosses an array or a value that is not an object resolves to
missing (`tags.0` is missing; indexing into arrays is not supported).
Filters, sort keys, and projections split their paths once per call.
Schemas, `UpdateSpec.Set`, and `Unset` stay top-level.

### Snapshot and plans (`plan.go`)

A query runs in one snapshot: it captures the overlay (the memtable and
`flushing`: id → document or tombstone) and the index definitions under
`Collection.mu`, then opens one read transaction on `index.db`
(`openSnap`). An id in the overlay is evaluated from the overlay only;
the drivers skip its entries. The query holds no collection lock across
the transaction and opens no second one (bbolt would deadlock against a
flush that grows the file). When a segment it reads was retired by
compaction, or an index it chose was dropped or re-created meanwhile, the
query starts over in a new snapshot (`errQueryRetry`, up to 8 times; after
4 it ignores indexes).

One driver produces the candidates of the documents in `index.db`
(`choosePlan`):

| Driver (`QueryPlan.Driver`) | Used when | Order produced |
|---|---|---|
| `id lookup` | `eq` or `in` on `_id` (always preferred) | `_id` |
| `index` | `eq`, `has`, `exists false`, `in`, `has_any`, `has_all` on an indexed field | `_id` (several values: merged union, or intersection for `has_all`) |
| `index range` | `gt`/`gte`/`lt`/`lte`, `starts_with` (prefix range), `exists true` (only while the index's `complex` and `arrays` counters are 0) | value, then `_id` |
| `or union` | every `Any` group has a served filter | `_id` (value-ordered groups are collected first) |
| `index sort` | `Sort` is one key on an indexed field, the index's `complex`, `truncated`, and `arrays` counters are 0, and no filter is served, or the best filter is a range or `starts_with` on the sort key's field (it then bounds the walk, together with the other range filters on that field), or the query has a limit and the best filter is large | the requested order, both directions |
| `keydir` (a walk of the key table) | none of the above | `_id`, both directions |
| `none` | an AND filter can match nothing | — |

Among served filters the smallest estimate wins: a cursor count of the
entries capped at 1,000 (`planEstimateCap`) plus the overlay documents
that are candidates. The overlay's documents are candidates by the same
rule as the entries (`planOption.cand`: the entry rule for
`eq`/`has`/`in`/`has_any`/`has_all`, the filter itself for ranges,
`starts_with`, `exists`), so examined counts do not depend on what is
flushed. Every candidate is re-checked against every filter and group.

Execution: when the driver yields the requested order, the query streams,
merging the overlay's candidates sorted the same way, and stops at
`Skip+Limit` matches (`findByID`, `findBySortWalk`). In `_id` order with a
value-ordered driver (a range), the candidate ids are collected from the
index without reading documents, sorted, then read in order with the same
early stop. A sort the driver does not provide reads every candidate into
a bounded heap of `Skip+Limit` (`findByHeap`, `sortHeap`). A descending
index sort walks value groups backwards and each group forwards.

`Collection.FindWithPlan` returns the `QueryPlan` (driver, index name,
whether the order was provided, examined and returned counts); the
internal `find` returns the examined count for tests.

- `CountDocuments`: no filter → `Count()`; a single filter that the key
  table or an index answers exactly (`countExact`: `eq`/`in` on `_id`;
  `eq`, `has`, `in` on values shorter than 1,023 encoded bytes,
  `exists false`, ranges and `starts_with` on an indexed field) and no
  `Any` → counted from the entries without reading documents (overlay ids
  left out, overlay documents matched); otherwise the candidates of the
  plan `Find` would choose (without the sort walk) are read and matched.
- `Distinct`: without filters on `_id`, or on an indexed field whose
  `complex` and `truncated` counters are 0 and no overlay document is
  complex, decoded from the index entries; otherwise the plan's
  candidates are read.

## Indexes (`index.go`)

- Every index is a bucket `ix/<name>` in the collection's `index.db`,
  maintained by the flusher in the same transaction as the key table. An
  entry is `encoded value · 0x00 0x00 · document id → flags`; the value
  encoding (`idxstore` `AppendValue`) is order-preserving, so the byte
  order is the engine's sort order missing < null < false < true <
  numbers < strings, and within one value entries are in `_id` order.
  Numbers fold `-0` into `0`; so `1` and `"1"` are different values.
- Per document version (`indexKeysOf`): a missing field has one entry
  under the `missing` value; an indexable scalar one entry (flags 0); an
  array one entry per distinct indexable element (`FlagElement`). Objects,
  NaN, and non-indexable elements get no entry and count the document in
  the index's `complex` counter. Strings encoded longer than 1,024 bytes
  are cut (`FlagTruncated`, counted in `truncated` by idxstore) and are
  candidates only. Every array value (empty ones included) counts the
  document in `arrays`.
- `eq` uses the entries without `FlagElement`, `has`/`has_any`/`has_all`
  those with it; ranges and `starts_with` use entries without it. An index
  drives a sort only while `complex`, `truncated`, and `arrays` are all 0:
  then every document has exactly one entry, ordered as `Find` sorts.
- The `_id` lookup uses the key table (no separate `_id` index exists).
- An index field may be a nested path; plans match an index to a filter or
  sort key by the exact field string.
- Unflushed changes are not in `index.db`; readers apply the snapshot rule
  above. See [STORAGE.md](STORAGE.md) for the file.

### Unique indexes

`CreateIndex(ctx, name, field, db.Unique(true))` creates a unique index
(`indexDef.Unique`, stored in `coll.meta` and the `create_index` WAL
payload as `"unique": true`; the key is omitted when false, so older files
load as non-unique). Code: `unique.go`.

- **What is constrained:** an indexable scalar: `null`, a boolean, a
  number (not NaN), a string. A missing field, an array, an object, and NaN
  are not constrained. Two values are the same when `eq` says so (`1` and
  `1.0` are one value; long strings compare in full, not by their cut
  index encoding).
- **Writes:** an insert, update, replace, or upsert whose new value for a
  unique index belongs to another live document fails with an error
  wrapping `ErrUniqueViolation` (`... index "u" on field "email": value
  "x@y" already belongs to document "a"`) and writes nothing. A write that
  keeps the value its document already has is not checked, so a document
  may keep its own value. A delete frees the value at once.
- **How:** under the document lock, before the WAL append, the write takes
  a striped lock per `(index, encoded value)` (`uniqueLocks`, 256 stripes,
  ascending order, held until the write is applied) and checks the overlay
  and then `index.db` (a short read transaction that ends before the WAL
  append). The overlay is searched through `uniquePending` (unique index →
  encoded value → ids of unflushed documents), filled by `applyLocked`,
  cleaned when a memtable entry is overwritten, and cleared for the flushed
  entries in flush step 4 after the index transaction commits. Every hit is
  confirmed against the current overlay document. So two writers with
  different ids and the same value never both succeed, also while the
  first is still unflushed.
- **Creation:** `CreateIndex` with `Unique(true)` first collects the values
  of every live document (the key table's, plus the overlay's, which
  cannot change: writes and flushes wait) and fails, before writing
  anything, with `ErrUniqueViolation` listing up to 10 shared values with
  up to 10 ids each (`buildIndexesChecked`, `duplicateValues`).
- **Recovery:** WAL replay never fails on a unique violation (the record is
  committed; the write path prevents it, so it means a damaged or foreign
  WAL): it applies the write and logs a WARN. Rebuilding an index file and
  replaying `create_index` build unique indexes without a check.

## Migrations

Defined in `migration.go`.

```go
const CurrentSchemaVersion = 1

type MigrationFunc func(ctx context.Context, e *Engine, dbName string) error

func RegisterMigration(fromVersion int, fn MigrationFunc)
func ListMigrations() []int
func (e *Engine) MigrateDatabase(ctx context.Context, dbName string, targetVersion int) error
func (e *Engine) MigrateAll(ctx context.Context, targetVersion int) error
```

- New databases start at `CurrentSchemaVersion`. No migrations are
  registered in this build.
- A migration upgrades `fromVersion` → `fromVersion+1`. It runs on the
  opened engine (after recovery) and must use normal engine operations,
  which are WAL-logged. Steps run one version at a time; after each, the
  new version is persisted to `db.meta`. A crash between a step and its
  version bump reruns the step, so migrations should be idempotent.
- Downgrades are rejected. Migrations are serialized per engine.
- `Open` fails with `ErrSchemaTooNew` for a database newer than
  `CurrentSchemaVersion` and logs a warning for an older one.
- CLI: `cachydb utils migrate` (see [CLI.md](CLI.md)).

## Example

See [`examples/basic/main.go`](../examples/basic/main.go) for a complete,
runnable program (open, schema, insert, index, find, update, delete, close,
reopen).
