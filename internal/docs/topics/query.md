---
name: query
title: Query language
summary: find_documents filters, operators and their exact semantics, sorting, paging, and index use
---
# Query language

`find_documents` takes the filters, limit, and skip **inside a `query`
object**:

```json
{
  "collection": "users",
  "query": {
    "filters": [
      {"field": "age",    "operator": "gte", "value": 18},
      {"field": "status", "operator": "in",  "value": ["active", "trial"]}
    ],
    "limit": 10,
    "skip": 0
  }
}
```

An omitted or empty `query` returns every document in the collection.

## Filters

- Each filter is `{"field", "operator", "value"}`. **All filters must
  match** (AND). There is no OR; run several queries instead.
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

Any other operator is rejected with an error that lists the valid ones.

## Comparison rules

- Comparison is type-aware. Numbers compare numerically (`2` equals
  `2.0`), strings compare lexically with strings.
- Booleans and `null` support only `eq`, `ne`, and `in`. Objects and
  arrays compare by deep equality.
- Values of different types never match `eq`, `gt`, `gte`, `lt`, or
  `lte`; `ne` matches them. `"5"` (a string) does not equal `5`.
- **A document that does not have the field matches no operator, not even
  `ne`.** (This differs from MongoDB's `$ne`.)

## Sorting and paging

- Results are always sorted by `_id`, ascending (byte order of the id
  string). There is no other sort order.
- `skip` matching documents are skipped, then at most `limit` are
  returned. `limit` 0 or omitted means no limit. Paging with `skip` and
  `limit` is deterministic.
- The result has `count` (the number returned) and `documents`; each
  document is a flat object with its `_id`.

## Indexes

- `create_index` builds a hash index on one top-level field.
- Only `eq` filters use indexes (on `_id` or on an indexed field). Other
  operators and unindexed fields scan every document of the collection.
- With several usable `eq` filters, the most selective one is used and the
  rest are checked on its candidates.
- Building an index blocks writes to that database until it finishes.
