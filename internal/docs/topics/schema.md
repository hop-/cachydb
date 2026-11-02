---
name: schema
title: Collection schemas
summary: Field types, required fields, when validation runs, changing a schema (update_schema), describe_collection, _id rules, and how numbers come back
---
# Collection schemas

A collection can have an optional schema, given to `create_collection`
and changed later with `update_schema`. Without a schema a collection
accepts any JSON object. `describe_collection` returns a collection's
schema (or `null`), its indexes, and its document count.

```json
create_collection {
  "name": "users",
  "schema": {
    "fields": {
      "name":  {"type": "string", "required": true},
      "age":   {"type": "number"},
      "tags":  {"type": "array"},
      "since": {"type": "date"}
    }
  }
}
```

## Field types

| Type | Accepts |
| --- | --- |
| `string` | a JSON string |
| `number` | any JSON number (integer or fraction) |
| `boolean` | `true` or `false` |
| `object` | a JSON object |
| `array` | a JSON array |
| `date` | a string (the format is not checked; ISO 8601 is recommended) |

## Rules

- A schema needs at least one field. Field names cannot be empty or
  `_id`. Each field's `type` must be one of the types above.
- `required: true` means the field must be present. A field that is
  present must have the declared type; `null` does not match any type.
- Fields that are not in the schema are allowed, with any value.
- Validation runs on every `insert_document` and `replace_document`, and
  on the **result** of every `update_document` (after `updates` and
  `unset`; unsetting a required field fails) and upsert.
- `{"$now": true}` values are replaced by the server time (a string)
  before validation, so a `date` field accepts them.
- A failed validation returns an error containing "schema validation
  failed" that lists **every** violation: missing required fields, then
  wrong types, for example `schema validation failed: required field(s)
  missing: name, title; wrong type: age (expected number, got string)`.

## Changing a schema

- `update_schema {collection, schema}` replaces the schema; `"schema":
  null` removes it.
- Every existing document is checked against the new schema first. If any
  fails, nothing changes and the error lists up to 10 failing ids with
  their first violation and the total, for example `2 existing
  document(s) do not match the new schema: n1 (required field missing:
  owner), n2 (...)`. Fix those documents (or the schema) and retry.
- Writes to the database wait while it checks; later writes use the new
  schema.
- To restructure a collection otherwise, `copy_collection` it (schema,
  indexes, documents), change the copy, then `delete_collection` the old
  one and `rename_collection` the copy.

## Document ids

- `_id` is always a string. Pass a string `_id` in the inserted document
  to choose it; otherwise cachydb generates a UUID and returns it as `id`.
- A non-string `_id` is rejected. Inserting an `_id` that already exists
  fails with "already exists".
- `_id` cannot be changed by `update_document` (in `updates` or `unset`)
  or `replace_document` (a different `_id` in the new document is an
  error).

## Values come back as JSON

Documents are stored as JSON. Over MCP, numbers come back as JSON numbers
(`28` stays `28`). Go programs that embed the engine get every number as
`float64`, arrays as `[]any`, and objects as `map[string]any`.
