---
name: tools
title: MCP tools reference
summary: Every MCP tool every server registers, with its description, annotations, input and output JSON Schema, and an example call (generated from the code); the user tools are in the access topic
---
# MCP tools reference

This topic is generated from the tool definitions the server registers, so
it always matches the running binary. Every tool that takes a `database`
argument falls back to the session's `use_database` choice, then to the
user's default database (when the server requires tokens), then to the
server default. On a server that requires tokens every tool also needs an
access level, and `whoami` plus the server-admin user tools are added;
see `cachydb docs access`. Successful results always include `"success": true`.
