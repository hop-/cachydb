---
name: errors
title: Error messages
summary: Errors agents may see from the tools and the CLI, what each means, and what to do
---
# Error messages

Tool errors come back as an MCP tool result with `isError: true`. The text
starts with a stable code in brackets, then the action (for example
`find documents:`) and the cause; "not found" and schema errors end with a
short `(hint: …)`:
`[not_found] find documents: collection "x" in database "main": not found (hint: …)`.
Branch on the code; the rest of the text may change between versions.

The one exception: arguments that do not match the tool's input schema
are rejected before the tool runs, with a message starting
`validating "arguments":` and no code.

## Error codes

| Code | Meaning | What to do |
| --- | --- | --- |
| `not_found` | The database, collection, document, index, or user does not exist | Check the name; `list_databases`, `list_collections`, `current_database` |
| `already_exists` | The name or `_id` is taken (also an `_id` repeated within one batch call) | Use another name or id |
| `invalid_name` | A name breaks the naming rule | Use letters, digits, `_` and `-`, starting with a letter or digit, at most 64 characters |
| `invalid_argument` | The call is malformed: an unknown operator or a bad `exists` / `starts_with` value, a bad sort key, an update operator on the wrong type, an empty batch query without `all`, a negative `limit`, nothing to change, an empty or too long `_id`, a `set_access` mistake, or removing the last server admin | Fix the arguments the message names |
| `schema_violation` | The document or schema breaks the collection schema | Fix the fields listed; see `cachydb docs schema` |
| `unique_violation` | A unique index already has that value | Use another value, or change the other document first |
| `condition_failed` | An `if` condition did not match; nothing was written | Re-read the document and retry |
| `limit_exceeded` | A batch is larger than the server's limits (`batch too large`: more than 10,000 documents or 32 MiB, or more than 10,000 matches for `update_documents` / `delete_documents` without a `limit`; the message states the count) | Split the batch, or pass a `limit` and repeat |
| `permission_denied` | The token's user lacks the access the call needs, or is no longer valid | Call `whoami`; ask a server admin; see `cachydb docs access` |
| `unavailable` | The server is shutting down, the write-ahead log failed, or a query was interrupted too often by background compaction | Retry later; after a write-ahead log failure restart the server |
| `cancelled` | The client cancelled the call or it timed out | Retry if needed |
| `internal` | Anything else, usually damaged data | Read the message; see "When the server does not start" below |

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
| `give updates, unset, or an operator` | `update_document` or `update_documents` had nothing to change | Pass `updates`, `unset`, or an update operator (`inc`, `push`, `add_to_set`, `pull`) |
| `invalid update` | An update operator met a field of the wrong type (`inc` on a non-number; `push`, `add_to_set`, `pull` on a non-array), or a field was named in more than one of `updates`, `unset`, and the operators; nothing was written | Check the field's current value (`get_document`); name each field once; see `cachydb docs query` |
| `batch too large` | A batch call exceeded a limit (10,000 documents or 32 MiB per call; for `update_documents` / `delete_documents`, more than 10,000 matches without a `limit`, with the match count); nothing was written | Split the batch, or pass a `limit` and repeat the call |
| `unique index violation` | A write gives a unique index's value to a second document, or `create_index` with `unique` found documents sharing a value (listed); nothing was written | Use another value, or change the other document first |
| `field "_id" in data is reserved` / `field "_id" cannot be changed` | `_id` was not a string on insert, or was in `updates` | Pass a string `_id` on insert only |
| `validating "arguments"` | The arguments do not match the tool's input schema (missing field, wrong type, unknown operator or field type) | Fix the arguments; the message names the property; see `cachydb docs tools` |
| `the database engine is shut down` | The server is stopping | Reconnect after it restarts |
| `write-ahead log failure, restart the server` | A disk write or fsync failed; writes stop | Check the disk, then restart the server |
| `request cancelled` | The client cancelled the call or it timed out | Retry if needed |
| `the query was interrupted by compactions or index changes` | Background compaction or index changes made a query start over too often | Retry the call |
| `permission denied: user "u" needs write access to database "d"` / `must be a server admin` | With tokens, the user lacks the level the call needs (`use_database` and other tools say the same for a database the user cannot read, whether it exists or not) | Call `whoami` to see your access; ask a server admin for a grant; see `cachydb docs access` |
| `unauthenticated` | The user of the session's token was removed, or the call carried no token | Ask a server admin for a valid token |
| `last server admin` | `delete_user` or `set_access` would remove or demote the only server admin | Make another user a server admin first |
| `give level with database` / `nothing to change` | `set_access` got a database without a level, or no change at all | Pass `database` and `level` (`none` removes), `server_admin`, or `default_database` |

## Startup and CLI errors

| Message | Meaning | What to do |
| --- | --- | --- |
| `data directory is in use by another cachydb process` | Another server or utils command holds the directory lock | Stop it, use another `-R` directory, or go through the running HTTP server with `cachydb client` (the message suggests the command) |
| `connecting to the cachydb server at …` | `cachydb client` found no server at `--url` / `CACHYDB_URL` | Start one with `cachydb -t http -p 7601`, or fix the URL |
| `the server requires a token: pass --token or set CACHYDB_TOKEN` / `the server rejected the token` | The server answered HTTP 401: no token, or an unknown or revoked one | Pass a valid token with `--token` or `CACHYDB_TOKEN`; MCP clients send `Authorization: Bearer <token>` |
| `refusing to start: … no user exists` | Clients need a token (`AUTH=required`, or a non-loopback host under `AUTH=auto`) and there is no user | Create the first admin with `cachydb utils user add NAME --admin` (server stopped) or set `BOOTSTRAP_ADMIN_TOKEN`; or, on a trusted network only, `AUTH=off`; see `cachydb docs access` |
| `invalid bootstrap token` | `BOOTSTRAP_ADMIN_TOKEN` (or its file) is shorter than 32 or longer than 512 characters, has other characters, or is another user's token | Use a long random value, for example `openssl rand -hex 32` |
| `BOOTSTRAP_ADMIN_TOKEN and BOOTSTRAP_ADMIN_TOKEN_FILE are both set` | Both settings are given | Keep one |
| `invalid authentication mode` / `TLS needs both a certificate and a key` | `AUTH` is not `auto`, `required`, or `off`, or only one of `TLS_CERT` and `TLS_KEY` is set | Fix the setting named in the message |
| `tool error: …` | `cachydb client call` ran a tool that failed (exit status 1) | Read the tool's message after the prefix |
| `already exists (use --on-conflict skip or keep to keep it; replace overwrites it ...)` | `utils import` / `client import` found an existing `_id`; the message says how many lines were applied | Rerun with `--on-conflict skip` or `keep` to keep existing documents; `replace` overwrites them with the file's copies |
| `exists with a different schema` / `different index set` | An import target collection differs from the file | `--on-conflict keep` leaves existing collections and documents as they are (seeding); `replace` updates the schema and indexes but also overwrites existing documents; or import into another directory |
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
