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

An omitted or empty `query` returns every document in the collection.

## Filters and OR groups

- Each filter is `{"field", "operator", "value"}`. **All filters in
  `filters` must match** (AND).
- `any` is a list of groups (each a list of filters). When `any` is set,
  a document must also match **every filter of at least one group** (OR of
  ANDs). An empty or omitted `any` adds no constraint.
- `field` is a **top-level** field name or `_id`. There is no dot
  notation for nested fields.

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
  (`desc` reverses it). It reads **every matching document**; sorting by
  `_id` stops as soon as enough matches are found.
- `fields` lists the top-level fields to return; `_id` is always
  returned and unknown names are ignored. Filters and sort may use fields
  that are not returned. Omit it for whole documents.
- `skip` matching documents are skipped (after sorting), then at most
  `limit` are returned. `limit` 0 or omitted means no limit.
- The result has `count` (the number returned) and `documents`; each
  document is a flat object with its `_id`.

## Counting and distinct values

- `count_documents {collection, query?: {filters, any}}` returns
  `{"count": n}` without returning documents. No filter, or one `eq` or
  `has` filter on an indexed field (or `eq` on `_id`), is answered without
  reading documents.
- `distinct {collection, field, query?: {filters, any}}` returns
  `{"values": [...]}`: the distinct values of the field among the matching
  documents. An array contributes **each element**, so
  `{"field": "tags"}` lists every tag. Values are compared like `eq`
  (objects and arrays by content) and sorted in the order above. Without
  filters, an indexed field is answered from the index.

## Indexes

- `create_index` builds a hash index on one top-level field. For an array
  field, **each element** is indexed.
- Filters that use an index: `eq` (on `_id` or an indexed field), and
  `has`, `has_any` (union), `has_all` (intersection) on an indexed field.
  With several, the smallest candidate set is used and every filter is
  re-checked on it.
- `contains`, range operators, `ne`, `in`, `any` groups, and unindexed
  fields scan every document of the collection.
- Building an index blocks writes to that database until it finishes;
  `drop_index` removes one.
