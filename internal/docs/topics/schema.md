---
name: schema
title: Collection schemas
summary: Field types, required fields, when validation runs, _id rules, and how numbers come back
---
# Collection schemas

A collection can have an optional schema, given once to
`create_collection`. It cannot be changed afterwards. Without a schema a
collection accepts any JSON object.

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
- Validation runs on every `insert_document`, and on the **merged result**
  of every `update_document`.
- A failed validation returns an error containing "schema validation
  failed" and names the field, for example `required field 'name' is
  missing` or `field 'age' has invalid type, expected number`.

## Document ids

- `_id` is always a string. Pass a string `_id` in the inserted document
  to choose it; otherwise cachydb generates a UUID and returns it as `id`.
- A non-string `_id` is rejected. Inserting an `_id` that already exists
  fails with "already exists".
- `_id` cannot be changed by `update_document`.

## Values come back as JSON

Documents are stored as JSON. Over MCP, numbers come back as JSON numbers
(`28` stays `28`). Go programs that embed the engine get every number as
`float64`, arrays as `[]any`, and objects as `map[string]any`.
