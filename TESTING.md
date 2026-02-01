# Testing CachyDB MCP Server

## Using MCP Inspector (Recommended)

The easiest way to test the server is with the official MCP Inspector:

```bash
npx @modelcontextprotocol/inspector ./cachydb
```

This will:

- Start an interactive web UI at <http://localhost:5173>
- Connect to your server
- Let you see all available tools
- Call tools with arguments
- View responses in real-time

## Using Claude Desktop

1. Add to your Claude Desktop config:

   **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
   **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
   **Linux:** `~/.config/Claude/claude_desktop_config.json`

   ```json
   {
     "mcpServers": {
       "cachydb": {
         "command": "/absolute/path/to/cachydb",
         "args": [],
         "env": {
           "DB_NAME": "main"
         }
       }
     }
   }
   ```

2. Restart Claude Desktop
3. You should see CachyDB tools available in the tool panel
4. Interact naturally: "Create a users collection" or "Show me all collections"

## Using the MCP Client Example

We provide a complete MCP client example that you can run:

```bash
# Build CachyDB
go build -o cachydb

# Run the MCP client example
cd examples/mcp-client
go run main.go
```

This example demonstrates all MCP operations (create collection, insert, find, update, delete, etc.).

See [`examples/mcp-client/main.go`](examples/mcp-client/main.go) for the full source code.

## Manual Testing with jq

For command-line testing with proper JSON parsing:

```bash
# Initialize and list tools
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' | ./cachydb | jq -c
```

## Direct Library Usage

If you want to use CachyDB as a Go library (without MCP), see the [`examples/basic/`](examples/basic/) directory for a complete example.

## Testing Commands

```JSON
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"current_database","arguments":{}}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_databases","arguments":{}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"create_database","arguments":{"name":"analytics"}}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"use_database","arguments":{"name":"analytics"}}}
{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"current_database","arguments":{}}}
{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"create_collection","arguments":{"name":"events"}}}
{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"list_collections","arguments":{}}}
{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"insert_document","arguments":{"collection":"events","document":{"type":"page_view","user":"alice","timestamp":"2026-02-01T10:00:00Z"}}}}
{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"insert_document","arguments":{"collection":"events","document":{"type":"click","user":"bob","timestamp":"2026-02-01T10:05:00Z"}}}}
{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"find_documents","arguments":{"collection":"events","query":{}}}}
```
