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
`WALRetentionSize`, and `CacheSize` a **negative** value means "disabled".

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
| `CacheSize` | 64 MiB | LRU document cache; negative = cache disabled |
| `CompactionRatio` | 0.5 | Dead/total bytes of sealed segments that triggers compaction; must be in (0,1] |

The defaults were kept after the benchmarks in `pkg/db/bench_test.go`; the
reasoning is in the comment above the constants in `options.go`.

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
| `DropCollection(ctx, name) error` | Not exposed as an MCP tool |

### Collection (`document.go`, `collection.go`)

| Method | Notes |
|--------|-------|
| `Insert(ctx, *Document) (string, error)` | Empty `doc.ID` gets a UUID; `ErrExists`; `ErrSchemaValidation` (schema, or `"_id"` key in `Data`) |
| `Get(ctx, id) (*Document, error)` | Deep copy; `ErrNotFound` |
| `Find(ctx, *Query) ([]*Document, error)` | See [Queries](#queries) |
| `Update(ctx, id, set map[string]any) (*Document, error)` | Merges top-level fields; returns the new version; `ErrNotFound`; `"_id"` in `set` → `ErrSchemaValidation` |
| `Delete(ctx, id) error` | `ErrNotFound` |
| `CreateIndex(ctx, name, field) error` | Builds from existing documents; blocks writes to the whole database while it runs; `ErrExists` |
| `DropIndex(ctx, name) error` | Not exposed as an MCP tool |
| `Count() int` | O(1) |
| `Name() string` | |

Every acknowledged write is fsynced to the WAL before the call returns.
Returned documents are deep copies: mutating them never changes stored data.

### Sentinel errors (`errors.go`)

Errors are wrapped with context; test them with `errors.Is`.

| Error | When |
|-------|------|
| `ErrNotFound` | Database, collection, document, or index missing (also after a drop) |
| `ErrExists` | Collection, document id, or index name already exists |
| `ErrInvalidName` | Name fails `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` (`names.go`, `ValidateName`) |
| `ErrSchemaValidation` | Invalid schema, document violates the schema, or a reserved `_id` misuse |
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
- Validation runs on every insert and on the merged result of every update.
  A required field must be present; a present field must have the declared
  type. Fields not in the schema are allowed.
- Schemas are fixed at collection creation. There is no API to change them.

## Queries

```go
type QueryFilter struct { Field, Operator string; Value any }
type Query struct { Filters []QueryFilter; Limit, Skip int }
```

Semantics (`compare.go`, `document.go`):

- All filters must match (AND). `Field` is a **top-level** field name or
  `_id`; there is no dot-notation for nested fields.
- Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`. Unknown operators
  match nothing.
- Comparison is type-aware: every numeric kind (and `json.Number`) compares
  as `float64`; strings compare lexically with strings; bools and `null`
  support equality only; objects and arrays support equality (deep, with
  numbers normalized). Values of different types never order:
  `gt/gte/lt/lte` and `eq` are false, `ne` is true.
- **A document missing the filtered field matches no operator, including
  `ne`.** This differs from MongoDB's `$ne` and is intentional.
- `in` takes an array; it matches when the field equals any element.
- Results are sorted by `_id` ascending. `Skip` matches are skipped, then
  at most `Limit` are returned (0 or negative = no limit). Paging is
  deterministic.
- Candidates: the smallest id set among `eq` filters on `_id` or on an
  indexed field with an indexable value. Without one, every live document
  is scanned (read from the memtable, cache, or disk). The scan stops once
  `Skip+Limit` matches are found.

## Indexes (`index.go`)

- Multi-value hash indexes: canonical key → set of document ids. Duplicate
  values are fully supported; deleting one document removes only its id.
- Keys are type-tagged: `n:<float>` (numbers, `-0` folded into `0`),
  `s:<string>`, `b:t`/`b:f`, `z:` (null). So the number `1` and the string
  `"1"` are different keys.
- Objects, arrays, and NaN are not indexed. A document whose field is
  missing or holds one has no entry, and `eq` filters on such values scan.
- Only `eq` uses an index. The `_id` lookup uses the keydir (no separate
  `_id` index exists).
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
