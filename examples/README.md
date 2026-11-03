# CachyDB Examples

Example programs showing the two ways to use CachyDB. Run them from the
repository root (both also work from their own directory).

## 1. basic/ - Direct Library Usage

Embeds CachyDB as a Go library (`pkg/db`), without MCP.

**What it shows:**

- Opening an engine on a data directory with `db.Open`
- Creating databases and collections, with and without a schema
- Inserting documents with generated and caller-chosen IDs
- Schema validation errors (`db.ErrSchemaValidation`)
- Creating an index and querying with filters (results sorted by `_id`)
- Updating and deleting documents
- A conditional update (`UpdateWith` with `If`, i.e. compare-and-set), `$now`
  server timestamps, `Unset`, and the `ErrConditionFailed` a stale writer gets
- Inspecting a collection's schema and indexes (`Schema`, `Indexes`)
- Tag queries on an indexed array field (`has`), with projection (`Fields`)
  and sorting (`Sort`), plus `contains` text search, `CountDocuments`, and
  `Distinct`
- Closing the engine, reopening the same directory, and finding the data
  still there (durability)

**Run it:**

```bash
go run ./examples/basic             # uses a fresh temporary directory, removed on exit
go run ./examples/basic -dir DIR    # uses (and keeps) DIR
```

| Flag | Default | Description |
|------|---------|-------------|
| `-dir` | temporary directory | Data directory to use; it is kept after the run |

`DIR` must not be in use by a running `cachydb` server (the data directory
is locked by one process at a time).

## 2. mcp-client/ - MCP Client Example

Connects to the `cachydb` binary over the stdio MCP transport, using the
official Go SDK client.

**What it shows:**

- Starting the server as a subprocess and connecting as an MCP client
- Listing the available tools
- Database management (`create_database`, `use_database`, `current_database`, ...)
- Creating a collection with a schema
- Inserting, finding (with `query.filters`), updating, and deleting documents
- A conditional `update_document` (`if`, `$now`, `unset`), and the expected
  error when the condition no longer matches
- `describe_collection`
- Creating indexes, including one on an array field, and querying it with
  `has`, `fields`, and `sort`; `contains` text search; `count_documents` and
  `distinct`

It also shows how a client tells an expected tool error from a real failure
(`CallToolResult.IsError`).

The tool schemas the client receives are typed (enums for `operator` and
field `type`); `cachydb docs tools --format json` prints them without a
client.

**Run it:**

```bash
go build -o cachydb                        # build the server first (repository root)
go run ./examples/mcp-client               # fresh temporary data directory
go run ./examples/mcp-client -root DIR     # uses (and keeps) DIR
go run ./examples/mcp-client -bin PATH     # use a cachydb binary elsewhere
```

| Flag | Default | Description |
|------|---------|-------------|
| `-bin` | `./cachydb`, else `../../cachydb` | Path to the `cachydb` binary |
| `-root` | temporary directory | Server data directory, passed to the server as `-R`; kept after the run |

The example always passes a data directory to the server, so it never
touches the default `~/.cachydb` (which a running server may have locked).

## Which Example Should I Use?

- **Use `basic/`** to embed CachyDB in your own Go application
- **Use `mcp-client/`** to see how MCP clients (AI assistants, tools) talk to CachyDB
- **Use the MCP Inspector** (see [TESTING.md](../TESTING.md)) for interactive testing with a web UI

## Tests

Each example has a smoke test (`main_test.go`) that runs the whole program,
and the programs check their own results. `go test ./...` therefore fails
when an example no longer works with the current API or tools:

```bash
go test ./examples/...          # both (mcp-client builds the cachydb binary first)
go test -short ./examples/...   # basic only; skips the binary build
```

## Requirements

- Go 1.25 or later (dependencies are fetched by `go run`)
- Linux or macOS
- For `mcp-client/`: a built `cachydb` binary
