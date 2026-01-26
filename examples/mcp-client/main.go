package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	ctx := context.Background()

	// Create MCP client
	client := mcp.NewClient(&mcp.Implementation{
		Name:    "cachydb-test-client",
		Version: "1.0.0",
	}, nil)

	// Connect to CachyDB server
	transport := &mcp.CommandTransport{
		Command: exec.Command("../../cachydb"),
	}
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer session.Close()

	fmt.Println("✓ Connected to CachyDB MCP server")
	fmt.Println()

	// List available tools
	toolsResult, err := session.ListTools(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to list tools: %v", err)
	}

	fmt.Printf("Available tools (%d):\n", len(toolsResult.Tools))
	for _, tool := range toolsResult.Tools {
		fmt.Printf("  • %s - %s\n", tool.Name, tool.Description)
	}
	fmt.Println()

	// Create a collection
	fmt.Println("Creating 'users' collection with schema...")
	createResult, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "create_collection",
		Arguments: map[string]any{
			"name": "users",
			"schema": map[string]any{
				"fields": map[string]any{
					"name": map[string]any{
						"type":     "string",
						"required": true,
					},
					"email": map[string]any{
						"type":     "string",
						"required": true,
					},
					"age": map[string]any{
						"type":     "number",
						"required": false,
					},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to create collection: %v", err)
	}
	printResult(createResult)

	// Insert a document
	fmt.Println("Inserting a document...")
	insertResult, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "insert_document",
		Arguments: map[string]any{
			"collection": "users",
			"document": map[string]any{
				"name":  "Alice Johnson",
				"email": "alice@example.com",
				"age":   28,
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to insert document: %v", err)
	}
	printResult(insertResult)

	// Insert another document
	fmt.Println("Inserting another document...")
	insertResult2, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "insert_document",
		Arguments: map[string]any{
			"collection": "users",
			"document": map[string]any{
				"name":  "Bob Smith",
				"email": "bob@example.com",
				"age":   35,
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to insert document: %v", err)
	}
	printResult(insertResult2)

	// Create an index
	fmt.Println("Creating index on email field...")
	indexResult, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "create_index",
		Arguments: map[string]any{
			"collection": "users",
			"index_name": "email_idx",
			"field_name": "email",
		},
	})
	if err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}
	printResult(indexResult)

	// Find all documents
	fmt.Println("Finding all documents...")
	findResult, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "find_documents",
		Arguments: map[string]any{
			"collection": "users",
			"query":      map[string]any{},
		},
	})
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	printResult(findResult)

	// Find with filter
	fmt.Println("Finding documents with age >= 30...")
	filterResult, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "find_documents",
		Arguments: map[string]any{
			"collection": "users",
			"query": map[string]any{
				"filters": []map[string]any{
					{
						"field":    "age",
						"operator": "gte",
						"value":    30,
					},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	printResult(filterResult)

	// List collections
	fmt.Println("Listing all collections...")
	listResult, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "list_collections",
		Arguments: map[string]any{},
	})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	printResult(listResult)

	fmt.Println("\n✓ All operations completed successfully!")
}

func printResult(result *mcp.CallToolResult) {
	if result.IsError {
		fmt.Printf("  ✗ Error: %v\n\n", result.Content)
		return
	}

	// Try to parse structured content
	if result.StructuredContent != nil {
		jsonData, err := json.MarshalIndent(result.StructuredContent, "  ", "  ")
		if err == nil {
			fmt.Printf("  ✓ %s\n\n", string(jsonData))
			return
		}
	}

	// Fall back to text content
	if len(result.Content) > 0 {
		for _, content := range result.Content {
			if textContent, ok := content.(*mcp.TextContent); ok {
				fmt.Printf("  ✓ %s\n\n", textContent.Text)
			}
		}
	}
}
