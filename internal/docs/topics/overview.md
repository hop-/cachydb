---
name: overview
title: CachyDB overview
summary: What cachydb is, its data model, durability guarantees, and where data lives
---
# CachyDB overview

CachyDB is a lightweight document database with a built-in MCP (Model
Context Protocol) server. It is a single Go binary with no external
services. AI agents use it through MCP tools; Go programs can embed the
engine (`github.com/hop-/cachydb/pkg/db`) directly.

## Data model

- A server holds **databases**. A database holds **collections**. A
  collection holds **JSON documents**.
- Every document has a string id, `_id`. Insert one with your own string
  `_id`, or let cachydb generate a UUID.
- A collection may have an optional **schema** (field types and required
  fields), checked on every insert and update. See `cachydb docs schema`.
- A collection may have **hash indexes** on top-level fields. Only `eq`
  filters use them. See `cachydb docs query`.
- Names of databases, collections, and indexes must match
  `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`.
- The server has a default database (`main` unless `-d/--db` says
  otherwise) that it creates at startup. Each MCP session can pick another
  one with `use_database`, and every data tool takes an optional
  `database` argument.

## Durability

- A write that returns success is fsynced to the write-ahead log (WAL)
  first. It survives a crash or power loss and is visible to every
  session at once.
- Concurrent writers share fsyncs (group commit), so throughput grows with
  the number of writers. A single write costs about one fsync.
- A background flusher moves changes into compressed data segments;
  recovery replays any WAL records that were not flushed yet.
- There are no multi-document transactions. Each insert, update, or
  delete is atomic on its own.

## Where data lives

- The data directory is `~/.cachydb` by default. Change it with
  `-R/--root` or the `ROOT_DIR` environment variable.
- Only one cachydb process may use a data directory at a time. A second
  server, or a `cachydb utils` command, fails with "data directory is in
  use by another cachydb process" while a server runs on it.
- The files are binary; do not edit them by hand. To start over, stop the
  server and delete the directory.

## Learn more

- `cachydb docs quickstart`: a first session, step by step.
- `cachydb docs mcp`: connecting an MCP client.
- `cachydb docs tools`: every MCP tool with its JSON Schema.
- Over MCP, the same topics are resources: `cachydb://docs/<topic>`.
