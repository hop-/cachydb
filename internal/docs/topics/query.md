---
name: query
title: Query language
summary: find_documents filters, operators (contains, has, has_any, has_all), OR groups, sort, projection, paging, count_documents, distinct, $now, and index use
---
# Query language

`find_documents` takes the filters, OR groups, projection, sort, limit,
and skip **inside a `query` object**:

```json
{
  "collection": "notes",
  "query": {
    "filters": [
      {"field": "tags",  "operator": "has",      "value": "wal"},
      {"field": "title", "operator": "contains", "value": "design"}
    ],
    "any": [
      [{"field": "type", "operator": "eq", "value": "decision"}],
      [{"field": "prio", "operator": "gte", "value": 3}]
    ],
    "fields": ["title", "tags"],
    "sort": [{"field": "updated", "order": "desc"}],
    "limit": 10,
    "skip": 0
  }
}
```

An omitted or empty `query` matches every document in the collection;
one call returns at most the server's default limit of them (100 unless
configured), see "Sorting, projection, and paging".

## Filters and OR groups

- Each filter is `{"field", "operator", "value"}`. **All filters in
  `filters` must match** (AND).
- `any` is a list of groups (each a list of filters). When `any` is set,
  a document must also match **every filter of at least one group** (OR of
  ANDs). An empty or omitted `any` adds no constraint.
- `field` is a field name, `_id`, or a **nested path** with dots, see
  below.

## Nested fields

A field name with dots is a path through nested objects:
`{"field": "meta.author", "operator": "eq", "value": "ana"}` matches
`{"meta": {"author": "ana"}}`. Paths work in filters, `any` groups, `if`
conditions, `sort`, `fields`, `distinct`, and index fields.

- A top-level key that equals the whole name wins: a document with a
  literal `"meta.author"` key is matched by that key.
- A path that crosses an array or a value that is not an object is
  missing: `tags.0` does not index into an array, and `meta.author` is
  missing when `meta` is a string or an array.
- Updates (`updates`, `unset`) and schemas use top-level field names only.

## Operators

| Operator | Matches when the field… |
| --- | --- |
| `eq` | equals `value` |
| `ne` | exists and does not equal `value` |
| `gt`, `gte` | is greater than (or equal to) `value` |
| `lt`, `lte` | is less than (or equal to) `value` |
| `in` | equals any element of `value`, which must be an array |
| `contains` | is a string containing `value` (a string), case-insensitively; or an array with such a string element. `""` matches every string or array |
| `has` | is an array with an element equal to `value` |
| `has_any` | is an array with an element equal to some value of the array `value` |
| `has_all` | is an array containing every value of the array `value` |

Any other operator is rejected with an error that lists the valid ones.

`eq`, `ne`, and `in` compare **whole values**, also for arrays:
`{"field": "tags", "operator": "eq", "value": "wal"}` does not match
`"tags": ["wal", "db"]`. Use `has` to match an element.

## Comparison rules

- Comparison is type-aware. Numbers compare numerically (`2` equals
  `2.0`), strings compare lexically with strings.
- Booleans and `null` support only `eq`, `ne`, `in`, and the element
  operators. Objects and arrays compare by deep equality.
- Values of different types never match `eq`, `gt`, `gte`, `lt`, or
  `lte`; `ne` matches them. `"5"` (a string) does not equal `5`.
- A type the operator does not apply to does not match: `contains` on a
  number, `has` on a non-array.
- **A document that does not have the field matches no operator, not even
  `ne`.** (This differs from MongoDB's `$ne`.)

## `$now`

The value `{"$now": true}` stands for the server's current time, an RFC
3339 UTC string with nanoseconds (`"2026-11-02T15:04:05.123456789Z"`).
It works in filter values (and `if` conditions) and in written
documents; every `$now` in one call gets the same time. Example: expired
entries are `{"field": "expires", "operator": "lt", "value": {"$now": true}}`.
Only the exact object `{"$now": true}` is replaced, so that object cannot
be stored literally.

## Sorting, projection, and paging

- `sort` is a list of `{"field", "order"}` keys (`order` is `asc`, the
  default, or `desc`), applied in order; `_id` ascending breaks ties, so
  paging stays deterministic. Without `sort`, results are sorted by `_id`
  ascending (byte order of the id string).
- Sorting by another field uses a total order across types: missing <
  `null` < `false` < `true` < numbers < strings < arrays < objects
  (`desc` reverses it). Sorting by `_id`, or by one indexed field whose
  values are all single values (no arrays, objects, or strings over about
  1,000 bytes), walks the order and stops as soon as enough matches are
  found; any other sort reads **every matching document**.
- `fields` lists the fields (or nested paths) to return; `_id` is always
  returned and unknown names are ignored. A nested path returns its value
  nested under the path: `"fields": ["meta.author"]` gives
  `{"_id": …, "meta": {"author": …}}`. Filters and sort may use fields
  that are not returned. Omit it for whole documents.
- `skip` matching documents are skipped (after sorting), then at most
  `limit` are returned. `limit` 0 or omitted means the server's default
  limit (`DEFAULT_LIMIT`, 100 unless configured); a positive `limit` is
  used as given.
- A result byte cap (`MAX_RESULT_BYTES`, 1 MiB unless configured) always
  applies: documents are added until the next one would make the JSON of
  `documents` larger than the cap. At least one document is returned
  even if it alone is larger, so paging never gets stuck.
- The result has `count` (the number returned), `documents` (each a flat
  object with its `_id`), and `has_more`: `true` if more documents match
  after these. When it is `true`, `next_skip` is set too.
- **To page**, repeat the same call (same filters and `sort`) with
  `"skip": next_skip` until `has_more` is `false`. Pages follow the sort
  order, so concurrent writes can shift what a later page holds. To know
  the total first, use `count_documents`.

```json
{"collection": "notes", "query": {"sort": [{"field": "updated", "order": "desc"}], "skip": 100}}
```

## Counting and distinct values

- `count_documents {collection, query?: {filters, any}}` returns
  `{"count": n}` without returning documents. No filter, or one `eq`,
  `in`, `has`, or range filter on an indexed field (or `eq`/`in` on `_id`),
  is answered from the index without reading documents.
- `distinct {collection, field, query?: {filters, any}}` returns
  `{"values": [...]}`: the distinct values of the field among the matching
  documents. An array contributes **each element**, so
  `{"field": "tags"}` lists every tag. Values are compared like `eq`
  (objects and arrays by content) and sorted in the order above. Without
  filters, an indexed field is answered from the index.

## Indexes

- `create_index` builds an index on one field, which may be a nested path
  (`meta.author`). For an array field, **each element** is indexed.
- An index can be **unique**: a write whose value for the field already
  belongs to another document then fails with "unique index violation"
  and writes nothing. Only a single value is constrained (a string,
  number, boolean, or `null`); a missing field, an array, and an object are
  not. `1` and `1.0` are the same value. Updating or replacing a document
  may keep its own value, and deleting a document frees its value.
  Creating a unique index fails, and changes nothing, if documents already
  share a value; the error lists up to 10 of the values with their ids.
- Filters that use an index: `eq` and `in` (on `_id` or an indexed
  field), `has`, `has_any` (union), `has_all` (intersection), and the
  ranges `gt`, `gte`, `lt`, `lte` on an indexed field, and `any` groups
  when every group has such a filter. With several, the one with the
  fewest matches is used and every filter is re-checked on its documents.
  `eq` matches whole values and `has*` array elements; ranges compare
  whole values only. With `limit`, an index in the requested order stops
  early.
- `contains`, `ne`, and filters on unindexed fields scan every document
  of the collection (unless another filter uses an index).
- Building an index blocks writes to that database until it finishes;
  `drop_index` removes one.
