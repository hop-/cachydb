// Command mcp-client connects to a cachydb binary over the stdio MCP
// transport and exercises its tools: database management, collections
// with a schema, document insert/find/update/delete, conditional updates
// with $now, describe_collection, indexes, tag queries with projection and
// sort, text search, count_documents, and distinct.
//
// Build the server first, from the repository root:
//
//	go build -o cachydb
//
// then run the client from the repository root or from this directory:
//
//	go run ./examples/mcp-client                 # fresh temporary data dir
//	go run ./examples/mcp-client -root DIR       # uses (and keeps) DIR
//	go run ./examples/mcp-client -bin PATH       # a binary elsewhere
//
// The data directory is passed to the server with -R, so the example never
// touches the default ~/.cachydb (which a running server may have locked).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	bin := flag.String("bin", "", "path to the cachydb binary (default: ./cachydb or ../../cachydb)")
	root := flag.String("root", "", "server data directory (default: a temporary directory removed on exit)")
	flag.Parse()

	if err := run(context.Background(), *bin, *root); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, binPath, root string) (err error) {
	binPath, err = findBinary(binPath)
	if err != nil {
		return err
	}
	if root == "" {
		tmp, err := os.MkdirTemp("", "cachydb-mcp-example-*")
		if err != nil {
			return fmt.Errorf("creating temp dir: %w", err)
		}
		defer func() { _ = os.RemoveAll(tmp) }() // best effort: it's a temp dir
		root = tmp
	}

	client := mcp.NewClient(&mcp.Implementation{
		Name:    "cachydb-example-client",
		Version: "1.0.0",
	}, nil)

	cmd := exec.Command(binPath, "-R", root)
	cmd.Stderr = os.Stderr // the server logs to stderr; stdout carries only MCP
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", binPath, err)
	}
	// Closing the session closes the server's stdin; the server then
	// flushes, exits, and Close reports a non-zero exit status.
	defer func() {
		if cerr := session.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("server exit: %w", cerr))
		}
	}()

	fmt.Printf("Connected to %s (data dir %s)\n\n", binPath, root)

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		return fmt.Errorf("listing tools: %w", err)
	}
	fmt.Printf("Available tools (%d):\n", len(tools.Tools))
	for _, tool := range tools.Tools {
		fmt.Printf("  - %s: %s\n", tool.Name, tool.Description)
	}
	fmt.Println()

	steps := []struct {
		title     string
		tool      string
		args      map[string]any
		wantError bool // the call is expected to return a tool error
	}{
		{"=== Database management ===\nCurrent database", "current_database", nil, false},
		{"Listing databases", "list_databases", nil, false},
		{"Creating database 'test_db'", "create_database", map[string]any{"name": "test_db"}, false},
		{"Switching this session to 'test_db'", "use_database", map[string]any{"name": "test_db"}, false},
		{"Current database", "current_database", nil, false},

		{"=== Collections & documents ===\nCreating collection 'users' (in test_db)", "create_collection", map[string]any{
			"name": "users",
			"schema": map[string]any{
				"fields": map[string]any{
					"name":  map[string]any{"type": "string", "required": true},
					"email": map[string]any{"type": "string", "required": true},
					"age":   map[string]any{"type": "number", "required": false},
				},
			},
		}, false},
		{"Inserting Alice (generated ID)", "insert_document", map[string]any{
			"collection": "users",
			"document":   map[string]any{"name": "Alice Johnson", "email": "alice@example.com", "age": 28},
		}, false},
		{"Inserting Bob (client-chosen ID 'bob')", "insert_document", map[string]any{
			"collection": "users",
			"document":   map[string]any{"_id": "bob", "name": "Bob Smith", "email": "bob@example.com", "age": 35},
		}, false},
		{"Creating index on email", "create_index", map[string]any{
			"collection": "users", "index_name": "email_idx", "field_name": "email",
		}, false},
		{"Finding all documents", "find_documents", map[string]any{
			"collection": "users", "query": map[string]any{},
		}, false},
		{"Finding documents with age >= 30", "find_documents", map[string]any{
			"collection": "users",
			"query": map[string]any{
				"filters": []map[string]any{{"field": "age", "operator": "gte", "value": 30}},
			},
		}, false},
		{"Finding by email (uses the index)", "find_documents", map[string]any{
			"collection": "users",
			"query": map[string]any{
				"filters": []map[string]any{{"field": "email", "operator": "eq", "value": "bob@example.com"}},
			},
		}, false},
		{"Updating Bob", "update_document", map[string]any{
			"collection": "users", "id": "bob", "updates": map[string]any{"age": 36, "city": "Berlin"},
		}, false},
		{"Conditional update of Bob (only if age is still 36; stamps the server time)", "update_document", map[string]any{
			"collection": "users", "id": "bob",
			"updates": map[string]any{"age": 37, "updated": map[string]any{"$now": true}},
			"unset":   []string{"city"},
			"if":      []map[string]any{{"field": "age", "operator": "eq", "value": 36}},
		}, false},
		{"Same condition again (expected to fail: age is now 37)", "update_document", map[string]any{
			"collection": "users", "id": "bob",
			"updates": map[string]any{"age": 99},
			"if":      []map[string]any{{"field": "age", "operator": "eq", "value": 36}},
		}, true},
		{"Describing 'users' (schema, indexes, count)", "describe_collection", map[string]any{
			"collection": "users",
		}, false},
		{"Deleting Bob", "delete_document", map[string]any{
			"collection": "users", "id": "bob",
		}, false},

		{"=== Tags, projection, sort, count, distinct ===\nCreating schemaless collection 'notes'", "create_collection", map[string]any{
			"name": "notes",
		}, false},
		{"Inserting a note", "insert_document", map[string]any{
			"collection": "notes",
			"document":   map[string]any{"_id": "n1", "title": "WAL design", "priority": 2, "tags": []string{"storage", "durability"}},
		}, false},
		{"Inserting another note", "insert_document", map[string]any{
			"collection": "notes",
			"document":   map[string]any{"_id": "n2", "title": "Index snapshots", "priority": 5, "tags": []string{"storage", "index"}},
		}, false},
		{"Indexing tags (each array element is indexed)", "create_index", map[string]any{
			"collection": "notes", "index_name": "tags_idx", "field_name": "tags",
		}, false},
		{"Notes tagged 'storage': titles only, highest priority first", "find_documents", map[string]any{
			"collection": "notes",
			"query": map[string]any{
				"filters": []map[string]any{{"field": "tags", "operator": "has", "value": "storage"}},
				"fields":  []string{"title"},
				"sort":    []map[string]any{{"field": "priority", "order": "desc"}},
			},
		}, false},
		{"Notes whose title contains 'wal' (any case)", "find_documents", map[string]any{
			"collection": "notes",
			"query": map[string]any{
				"filters": []map[string]any{{"field": "title", "operator": "contains", "value": "wal"}},
			},
		}, false},
		{"Counting notes tagged 'storage'", "count_documents", map[string]any{
			"collection": "notes",
			"query": map[string]any{
				"filters": []map[string]any{{"field": "tags", "operator": "has", "value": "storage"}},
			},
		}, false},
		{"Distinct tags", "distinct", map[string]any{
			"collection": "notes", "field": "tags",
		}, false},
		{"Listing collections in the current database", "list_collections", nil, false},
	}

	for _, s := range steps {
		fmt.Println(s.title + "...")
		args := s.args
		if args == nil {
			args = map[string]any{}
		}
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: s.tool, Arguments: args})
		if err != nil {
			return fmt.Errorf("calling %s: %w", s.tool, err)
		}
		if res.IsError != s.wantError {
			if s.wantError {
				return fmt.Errorf("tool %s succeeded but was expected to fail: %s", s.tool, resultText(res))
			}
			return fmt.Errorf("tool %s failed: %s", s.tool, resultText(res))
		}
		if s.wantError {
			fmt.Printf("  failed as expected: %s\n\n", resultText(res))
			continue
		}
		fmt.Printf("  %s\n\n", resultText(res))
	}

	fmt.Println("All operations completed successfully.")
	return nil
}

// findBinary returns bin if set, else the first cachydb binary found in
// the current directory or two levels up (examples/mcp-client).
func findBinary(bin string) (string, error) {
	candidates := []string{bin}
	if bin == "" {
		candidates = []string{"cachydb", filepath.Join("..", "..", "cachydb")}
	}
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return filepath.Abs(c)
		}
	}
	return "", fmt.Errorf("cachydb binary not found (tried %v); build it from the repository root with: go build -o cachydb", candidates)
}

// resultText renders a tool result: its structured content as indented
// JSON if present, else its text content.
func resultText(res *mcp.CallToolResult) string {
	if !res.IsError && res.StructuredContent != nil {
		if data, err := json.MarshalIndent(res.StructuredContent, "  ", "  "); err == nil {
			return string(data)
		}
	}
	var out string
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			out += t.Text
		}
	}
	return out
}
