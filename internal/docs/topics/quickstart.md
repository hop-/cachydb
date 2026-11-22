---
name: quickstart
title: Quickstart
summary: Run the server over stdio or HTTP, then create a database, collection, and document and query them
---
# Quickstart

## 1. Start the server

cachydb is an MCP server. Your MCP client usually starts it for you (see
`cachydb docs mcp`). To run it yourself:

```bash
cachydb                              # stdio transport, data in ~/.cachydb
cachydb -R /path/to/data             # another data directory
cachydb -t http -p 7601              # Streamable HTTP at http://localhost:7601/mcp
```

In stdio mode, stdout carries only MCP messages; logs go to stderr.

## 2. Use the tools

The calls below are MCP `tools/call` requests, shown as tool name and
arguments.

Create a database and make it this session's default (optional: the
server default `main` already exists):

```json
create_database {"name": "shop"}
use_database    {"name": "shop"}
```

Create a collection, optionally with a schema:

```json
create_collection {
  "name": "products",
  "schema": {"fields": {"name": {"type": "string", "required": true},
                        "price": {"type": "number"}}}
}
```

Insert documents. A string `_id` is optional; without one cachydb
generates a UUID and returns it as `id`:

```json
insert_document {"collection": "products", "document": {"_id": "p1", "name": "Pen", "price": 2.5}}
insert_document {"collection": "products", "document": {"name": "Book", "price": 12}}
```

Insert many at once with `insert_documents`: all of them, or none if
one fails. Read one by id with `get_document`:

```json
insert_documents {"collection": "products", "documents": [{"_id": "p2", "name": "Ink", "price": 4}, {"name": "Pad", "price": 6}]}
get_document {"collection": "products", "id": "p2"}
```

Query. Filters go inside `query.filters`; results are sorted by `_id`
unless `sort` is given, and `fields` limits what comes back:

```json
find_documents {
  "collection": "products",
  "query": {"filters": [{"field": "price", "operator": "lt", "value": 10}],
            "sort": [{"field": "price", "order": "desc"}], "fields": ["name"], "limit": 20}
}
find_documents {"collection": "products", "query": {"filters": [{"field": "name", "operator": "contains", "value": "pen"}]}}
count_documents {"collection": "products"}
```

One call returns at most 100 documents (and 1 MiB) unless you pass a
`limit` or the server is configured otherwise. When the result has
`"has_more": true`, repeat the call with `"skip"` set to its `next_skip`:

```json
find_documents {"collection": "products", "query": {"skip": 100}}
```

Update merges top-level fields (`unset` removes some; `{"$now": true}` is
the server time); `if` makes it conditional; `inc`, `push`,
`add_to_set`, and `pull` change a number or an array in place (see
`cachydb docs query`); delete removes a document:

```json
update_document {"collection": "products", "id": "p1", "updates": {"price": 3, "updated": {"$now": true}}, "unset": ["draft"]}
update_document {"collection": "products", "id": "p1", "updates": {"price": 4}, "if": [{"field": "price", "operator": "eq", "value": 3}]}
update_document {"collection": "products", "id": "p9", "updates": {"name": "Ink"}, "upsert": true}
update_document {"collection": "products", "id": "p1", "inc": {"stock": -1}, "add_to_set": {"tags": "sale"}}
delete_document {"collection": "products", "id": "p1"}
```

Change or delete every match in one atomic call. An empty query is
rejected unless you pass `"all": true`:

```json
update_documents {"collection": "products", "query": {"filters": [{"field": "price", "operator": "gt", "value": 5}]}, "updates": {"sale": true}}
delete_documents {"collection": "products", "query": {"filters": [{"field": "sale", "operator": "eq", "value": true}]}}
```

Speed up lookups, ranges, and sorts on a field with an index (`unique`
also rejects a second document with the same value); inspect the
collection and the sizes of what you can read:

```json
create_index {"collection": "products", "index_name": "by_name", "field_name": "name"}
create_index {"collection": "products", "index_name": "by_sku", "field_name": "sku", "unique": true}
describe_collection {"collection": "products"}
server_stats {}
```

## 3. Inspect from the command line

With the HTTP server running, go through it:

```bash
cachydb client call find_documents '{"database":"shop","collection":"products"}'
cachydb client export -d shop -o shop.jsonl
```

With the server stopped (utility commands take the data-directory lock):

```bash
cachydb utils list -c -R /path/to/data   # databases, collections, document counts
cachydb utils find -d shop -c products -R /path/to/data
cachydb utils export -o backup.jsonl -R /path/to/data
cachydb utils import -i seed.jsonl --on-conflict keep -R /path/to/data   # seed: create only what is missing
```

## Next

- `cachydb docs query`: every operator and its exact semantics.
- `cachydb docs tools`: input and output schemas of all tools.
- `cachydb docs errors`: what error messages mean.
