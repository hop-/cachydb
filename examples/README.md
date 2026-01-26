# CachyDB Examples

This directory contains example programs demonstrating different ways to use CachyDB.

## Examples

### 1. basic/ - Direct Library Usage

Demonstrates using CachyDB as a Go library directly, without MCP.

**What it shows:**

- Creating a database and collections
- Defining schemas
- Inserting documents
- Creating indexes
- Querying with filters
- Saving/loading from disk

**Run it:**

```bash
cd examples/basic
go run main.go
```

This is useful if you want to embed CachyDB in your own Go application.

### 2. mcp-client/ - MCP Client Example

Demonstrates connecting to CachyDB as an MCP client.

**What it shows:**

- Connecting to the MCP server
- Listing available tools
- Creating collections via MCP
- Inserting documents via MCP
- Creating indexes via MCP
- Querying documents via MCP

**Run it:**

```bash
# First, build CachyDB in the parent directory
cd ..
go build -o cachydb

# Then run the MCP client example
cd examples/mcp-client
go run main.go
```

This demonstrates how to integrate CachyDB with MCP clients (AI assistants, etc.).

## Which Example Should I Use?

- **Use `basic/`** if you want to use CachyDB as a library in your Go application
- **Use `mcp-client/`** if you want to understand how CachyDB works with MCP clients
- **Use the MCP Inspector** (see [TESTING.md](../TESTING.md)) for interactive testing with a web UI

## Requirements

Both examples require:

- Go 1.25 or later
- The CachyDB dependencies (automatically installed via `go run`)

The MCP client example also requires:

- The `cachydb` binary to be built in the parent directory
