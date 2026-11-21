---
name: errors
title: Error messages
summary: Errors agents may see from the tools and the CLI, what each means, and what to do
---
# Error messages

Tool errors come back as an MCP tool result with `isError: true`. The text
starts with the action (for example `find documents:`) followed by the
cause; "not found" and schema errors end with a short `(hint: …)`. Invalid
arguments are rejected before the tool runs, with a message
starting `validating "arguments":`.

## Tool errors

| Message contains | Meaning | What to do |
| --- | --- | --- |
| `database "x": not found` | The database does not exist | Call `list_databases`, or `create_database` |
| `collection "x" in database "y": not found` | The collection does not exist in that database | Call `list_collections`; check which database the call used (`current_database`) |
| `document "x" in collection "y": not found` | No document has that `_id` | Check the id; `find_documents` lists ids |
| `already exists` | A collection, index, or document id is already taken | Use another name or id; `create_database` on an existing name succeeds |
| `invalid name` | A name does not match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` | Use letters, digits, `_` and `-`; start with a letter or digit; at most 64 characters |
| `schema validation failed` | The document breaks the collection schema (every violation is listed), the schema itself is invalid, or `update_schema` found documents that do not match (their ids are listed) | Fix the fields named in the message; see `cachydb docs schema` |
| `condition failed: document x no longer matches if; re-read it and retry` | An `if` condition (update, replace, delete) did not match the stored document, or the document does not exist; nothing was written | Read the document again, recompute, and retry with the new values in `if` |
| `in both updates and unset` | `update_document` named a field in both | Use one of them per field |
| `give updates, unset, or both` | `update_document` had nothing to change | Pass `updates` and/or `unset` |
| `field "_id" in data is reserved` / `field "_id" cannot be changed` | `_id` was not a string on insert, or was in `updates` | Pass a string `_id` on insert only |
| `validating "arguments"` | The arguments do not match the tool's input schema (missing field, wrong type, unknown operator or field type) | Fix the arguments; the message names the property; see `cachydb docs tools` |
| `the database engine is shut down` | The server is stopping | Reconnect after it restarts |
| `write-ahead log failure, restart the server` | A disk write or fsync failed; writes stop | Check the disk, then restart the server |
| `request cancelled` | The client cancelled the call or it timed out | Retry if needed |

## Startup and CLI errors

| Message | Meaning | What to do |
| --- | --- | --- |
| `data directory is in use by another cachydb process` | Another server or utils command holds the directory lock | Stop it, use another `-R` directory, or go through the running HTTP server with `cachydb client` (the message suggests the command) |
| `connecting to the cachydb server at …` | `cachydb client` found no server at `--url` / `CACHYDB_URL` | Start one with `cachydb -t http -p 7601`, or fix the URL |
| `tool error: …` | `cachydb client call` ran a tool that failed (exit status 1) | Read the tool's message after the prefix |
| `already exists (use --on-conflict skip or replace)` | `utils import` / `client import` found an existing `_id`; the message says how many lines were applied | Rerun with `--on-conflict skip` or `replace` |
| `exists with a different schema` / `different index set` | An import target collection differs from the file | Use `--on-conflict replace`, or import into another directory |
| `unsupported export format version` | The file was written by a newer cachydb | Upgrade cachydb |
| `WAL corrupt` / `idxstore: index file …` / `segment: corrupt` | A file of the data directory is damaged, so the server does not start or a read fails | Stop the server and run `cachydb utils verify -R <dir>` (see below) |
| `verify found problems` | `cachydb utils verify` found damage; its stdout lists one line per problem | See "When the server does not start" below |
| `incompatible data directory` | The directory was written by a cachydb this version cannot open: one older than v0.0.4 (`was created by an older cachydb format`), or a newer one (`newer than supported version`). Directories of v0.0.4 to v0.0.6 are not an error: they are upgraded automatically, one-way, at the first start | Older: use a new directory, the old data is not converted. Newer: upgrade cachydb |
| `database schema version is newer than supported` | The data was migrated by a newer cachydb | Upgrade cachydb |
| `invalid transport` | `-t`/`TRANSPORT` is not `stdio` or `http` | Use `stdio` or `http` |
| `loading configuration from the environment` | An environment variable has a malformed value | Fix the variable named in the message |
| `unknown docs topic` | `cachydb docs <topic>` was given an unknown name | Run `cachydb docs` to list the topics |

## When the server does not start

1. Stop every cachydb process on the directory and run
   `cachydb utils verify -R <dir>` (add `--deep` to also check every
   index entry, `--format json` for a report an agent can parse). It
   reads the files without changing them and prints one line per problem
   (the file or `database/collection` first), `note:` lines for what the
   next start repairs by itself, and a summary; exit status 1 means a
   problem was found.
2. A problem in a collection's `index.db` (missing, damaged, wrong
   entries, a wrong count, or "older than the data segments") is
   repairable: run `cachydb utils verify -R <dir> --rebuild-index` (or
   with `-d <db> -c <collection>` for one collection). It rebuilds the
   file from the data segments and verifies again. Do this before
   starting the server when verify says that opening "would discard"
   changes.
3. A problem in a `wal-*.log` or `seg-*.data` file cannot be repaired:
   copy the directory aside, then restore the whole data directory from a
   backup (never `index.db` alone), or export what still opens
   (`cachydb utils export`) into a new directory.
