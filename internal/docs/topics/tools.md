---
name: tools
title: MCP tools reference
summary: Every MCP tool with its description, annotations, input and output JSON Schema, and an example call (generated from the code)
---
# MCP tools reference

This topic is generated from the tool definitions the server registers, so
it always matches the running binary. Every tool that takes a `database`
argument falls back to the session's `use_database` choice, then to the
server default. Successful results always include `"success": true`.
