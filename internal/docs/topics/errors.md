---
name: errors
title: Error messages
summary: Errors agents may see from the tools and the CLI, what each means, and what to do
---
# Error messages

Tool errors come back as an MCP tool result with `isError: true`. The text
starts with the action (for example `find documents:`) followed by the
cause. Invalid arguments are rejected before the tool runs, with a message
starting `validating "arguments":`.

## Tool errors

| Message contains | Meaning | What to do |
| --- | --- | --- |
| `database "x": not found` | The database does not exist | Call `list_databases`, or `create_database` |
| `collection "x" in database "y": not found` | The collection does not exist in that database | Call `list_collections`; check which database the call used (`current_database`) |
| `document "x" in collection "y": not found` | No document has that `_id` | Check the id; `find_documents` lists ids |
| `already exists` | A collection, index, or document id is already taken | Use another name or id; `create_database` on an existing name succeeds |
| `invalid name` | A name does not match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` | Use letters, digits, `_` and `-`; start with a letter or digit; at most 64 characters |
| `schema validation failed` | The document breaks the collection schema, or the schema itself is invalid | Read the field named in the message; see `cachydb docs schema` |
| `field "_id" in data is reserved` / `field "_id" cannot be changed` | `_id` was not a string on insert, or was in `updates` | Pass a string `_id` on insert only |
| `validating "arguments"` | The arguments do not match the tool's input schema (missing field, wrong type, unknown operator or field type) | Fix the arguments; the message names the property; see `cachydb docs tools` |
| `the database engine is shut down` | The server is stopping | Reconnect after it restarts |
| `write-ahead log failure, restart the server` | A disk write or fsync failed; writes stop | Check the disk, then restart the server |
| `request cancelled` | The client cancelled the call or it timed out | Retry if needed |

## Startup and CLI errors

| Message | Meaning | What to do |
| --- | --- | --- |
| `data directory is in use by another cachydb process` | Another server or utils command holds the directory lock | Stop it, use another `-R` directory, or connect to the running server over HTTP |
| `incompatible data directory` | The directory was written by an older, incompatible cachydb | Use a new directory; the old data is not converted |
| `database schema version is newer than supported` | The data was migrated by a newer cachydb | Upgrade cachydb |
| `invalid transport` | `-t`/`TRANSPORT` is not `stdio` or `http` | Use `stdio` or `http` |
| `loading configuration from the environment` | An environment variable has a malformed value | Fix the variable named in the message |
| `unknown docs topic` | `cachydb docs <topic>` was given an unknown name | Run `cachydb docs` to list the topics |
