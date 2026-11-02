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
compactor, and WAL retention. `Close` waits for in-flight calls, flushes
everything, writes index snapshots, closes the WAL, and releases the lock.
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

The defaults were kept after the benchmarks in `pkg/db/bench_test.go`; the
reasoning is in the comment above the constants in `options.go`.

A document write (`Insert`, `Update`, `Delete`) can block on
`MaxPendingBytes` when the disk is slower than the writers. It returns
`ctx.Err()` if its context ends first (nothing is written) and `ErrClosed`
if the engine closes. `FormatVersion` and `WALFormatVersion` are the
on-disk format versions this build reads and writes.

## API

### Engine (`engine.go`)

| Method | Notes |
|--------|-------|
| `Open(ctx, Options) (*Engine, error)` | See above |
| `Close(ctx) error` | Idempotent; if `ctx` ends, remaining flushes are skipped (data stays in the WAL) |
| `CreateDatabase(ctx, name) (*Database, error)` | Returns the existing database without writing a WAL record if it exists |
| `Database(name) (*Database, error)` | `ErrNotFound` if missing |
| `ListDatabases() []string` | Sorted; nil when empty |
| `DropDatabase(ctx, name) error` | Waits for in-flight writes in the database |
| `MigrateDatabase(ctx, name, target) error` | See [Migrations](#migrations) |
| `MigrateAll(ctx, target) error` | |

### Database (`database.go`)

| Method | Notes |
|--------|-------|
| `Name() string`, `SchemaVersion() int` | |
| `CreateCollection(ctx, name, *Schema) error` | `nil` schema = schemaless; `ErrExists`, `ErrSchemaValidation` (invalid schema) |
| `Collection(name) (*Collection, error)` | `ErrNotFound` if missing |
| `ListCollections() []string` | Sorted; nil when empty |
| `DropCollection(ctx, name) error` | MCP `delete_collection` |
| `RenameCollection(ctx, old, new) error` | Same database; flushes the collection, then WAL op 10 (`rename_coll`) + directory rename + `coll.meta` rewrite; `ErrNotFound`, `ErrExists`; old handles return `ErrNotFound` afterwards |
| `CopyCollection(ctx, name, newName, target *Database) (int, error)` | `target` nil = same database; creates the target with the schema and indexes, then inserts every document (same ids) through the write path, 32 at a time; **not atomic** (drop the partial target on error); `ErrExists` |

### Collection (`document.go`, `collection.go`)

| Method | Notes |
|--------|-------|
| `Insert(ctx, *Document) (string, error)` | Empty `doc.ID` gets a UUID; `ErrExists`; `ErrSchemaValidation` (schema, or `"_id"` key in `Data`) |
| `Get(ctx, id) (*Document, error)` | Deep copy; `ErrNotFound` |
| `Find(ctx, *Query) ([]*Document, error)` | See [Queries](#queries) |
| `CountDocuments(ctx, *Query) (int, error)` | Filters and `Any` only; fast paths below |
| `Distinct(ctx, field, *Query) ([]any, error)` | Arrays contribute each element; deduplicated like `eq`, sorted by the sort order |
| `Update(ctx, id, set map[string]any) (*Document, error)` | `UpdateWith` with only `Set` |
| `UpdateWith(ctx, id, UpdateSpec) (*Document, bool, error)` | `Set`, then remove `Unset`; `If` condition; `Upsert` creates `{_id}+Set` (returns `upserted`); `ErrNotFound`, `ErrConditionFailed`; `"_id"` in `Set`/`Unset` or a field in both → `ErrSchemaValidation` |
| `Replace(ctx, id, *Document, Condition) (*Document, error)` | Whole-document replace; `doc.ID` empty or `id`; `ErrNotFound` (missing), `ErrConditionFailed` |
| `Delete(ctx, id) error` | `DeleteWith(ctx, id, nil)` |
| `DeleteWith(ctx, id, Condition) error` | `ErrNotFound`, `ErrConditionFailed` |
| `CreateIndex(ctx, name, field) error` | Builds from existing documents; blocks writes to the whole database while it runs; `ErrExists` |
| `DropIndex(ctx, name) error` | MCP `drop_index`; `ErrNotFound` |
| `Schema() *Schema`, `Indexes() []IndexInfo` | Copies; indexes sorted by name (MCP `describe_collection`) |
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
- Every write still logs one `put_doc`/`delete_doc` after-image; the WAL
  record format is unchanged.

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
  `Document.ID`), and `Update` cannot change it. Ids are strings up to
  65535 bytes.
- `Clone()` is a deep copy.

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

Semantics (`compare.go`, `query.go`, `document.go`):

- All `Filters` must match (AND), and when `Any` is set every filter of at
  least one group. `Field` is a **top-level** field name or `_id`; there
  is no dot-notation for nested fields.
- Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`
  (case-insensitive substring of a string or of a string array element;
  `""` matches any string or array), `has` (an array element equals the
  value), `has_any`, `has_all` (some / every value of the array value is
  an element). Unknown operators match nothing. `eq`/`ne`/`in` compare
  arrays as whole values.
- Comparison is type-aware: every numeric kind (and `json.Number`) compares
  as `float64`; strings compare lexically with strings; bools and `null`
  support equality only; objects and arrays support equality (deep, with
  numbers normalized). Values of different types never order:
  `gt/gte/lt/lte` and `eq` are false, `ne` is true.
- **A document missing the filtered field matches no operator, including
  `ne`.** This differs from MongoDB's `$ne` and is intentional.
- `in` takes an array; it matches when the field equals any element.
- Order: `Sort` keys, then `_id` ascending. Without `Sort` (or with exactly
  `_id asc`), ids are walked in order and the scan stops once `Skip+Limit`
  matches are found (`_id desc` walks them backwards). Any other sort
  reads every match and keeps the best `Skip+Limit` in a bounded heap
  (`sortHeap`), using a total order: missing < null < false < true <
  numbers < strings < arrays < objects (arrays and objects by canonical
  JSON); `desc` reverses it. `Skip` matches are skipped, then at most
  `Limit` are returned (0 or negative = no limit). Paging is
  deterministic. An invalid order or empty sort field is an error.
- `Fields` is applied after filtering and sorting; `_id` is always kept.
- Candidates: the smallest id set among `eq` on `_id`, and `eq`/`has`
  (one key), `has_any` (union), `has_all` (intersection of its indexable
  values) on an indexed field. Without one, every live document is
  scanned (read from the memtable, cache, or disk). Candidates are always
  re-checked against every filter. `contains`, ranges, and `Any` groups
  never use an index.
- `CountDocuments`: no filter → `Count()`; exactly one `eq` or `has` on an
  indexed field (or `eq` on `_id`) and no `Any` → answered from the index
  without reading documents (exact, through the marker keys below);
  otherwise like `Find` without materializing results.
- `Distinct`: without filters on `_id` or an indexed field, decoded from
  the index keys (unless the unindexable marker is non-empty); otherwise
  scanned.

## Indexes (`index.go`)

- Multi-value hash indexes: canonical key → set of document ids. Duplicate
  values are fully supported; deleting one document removes only its id.
- Keys are type-tagged: `n:<float>` (numbers, `-0` folded into `0`),
  `s:<string>`, `b:t`/`b:f`, `z:` (null). So the number `1` and the string
  `"1"` are different keys.
- An array value is indexed under the key of **each** scalar element
  (plan 004). Objects, nested arrays, and NaN are not indexed; `eq`
  filters on such values scan.
- Two marker keys make counts and distinct values exact: `a:` holds the
  ids whose field is an array, `u:` those whose value (or an element) is
  not indexable. `countEq` = |key set| − |key set ∩ `a:`|; `countHas` =
  |key set ∩ `a:`|.
- `eq`, `has`, `has_any`, and `has_all` use an index; per-element keys
  can make `eq` candidates a superset, which the re-check removes. The
  `_id` lookup uses the keydir (no separate `_id` index exists).
- Indexes are kept in memory, persisted as snapshots, and rebuilt when a
  snapshot is missing or invalid (see [STORAGE.md](STORAGE.md)).

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
