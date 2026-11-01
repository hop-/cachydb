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

Query. Filters go inside `query.filters`; results are sorted by `_id`:

```json
find_documents {
  "collection": "products",
  "query": {"filters": [{"field": "price", "operator": "lt", "value": 10}], "limit": 20}
}
```

Update merges top-level fields; delete removes a document:

```json
update_document {"collection": "products", "id": "p1", "updates": {"price": 3}}
delete_document {"collection": "products", "id": "p1"}
```

Speed up equality lookups with an index:

```json
create_index {"collection": "products", "index_name": "by_name", "field_name": "name"}
```

## 3. Inspect from the command line

Stop the server first (utility commands take the data-directory lock):

```bash
cachydb utils list -c -R /path/to/data   # databases, collections, document counts
```

## Next

- `cachydb docs query`: every operator and its exact semantics.
- `cachydb docs tools`: input and output schemas of all tools.
- `cachydb docs errors`: what error messages mean.
