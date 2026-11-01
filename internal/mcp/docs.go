package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hop-/cachydb/internal/docs"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Agent-facing documentation generated from the tool definitions: the
// "tools" docs topic, the server instructions, and the docs resources.

// DocsURIPrefix is the URI prefix of the docs resources.
const DocsURIPrefix = "cachydb://docs/"

// ToolDoc is the structured form of one tool in the tools topic.
type ToolDoc struct {
	Name         string               `json:"name"`
	Description  string               `json:"description"`
	Annotations  *mcp.ToolAnnotations `json:"annotations"`
	InputSchema  any                  `json:"inputSchema"`
	OutputSchema any                  `json:"outputSchema"`
	Example      map[string]any       `json:"example"`
}

// ToolDocs returns every tool's documentation, in registration order.
func ToolDocs() []ToolDoc {
	specs := toolSpecs()
	out := make([]ToolDoc, len(specs))
	for i, sp := range specs {
		out[i] = ToolDoc{
			Name:         sp.tool.Name,
			Description:  sp.tool.Description,
			Annotations:  sp.tool.Annotations,
			InputSchema:  sp.tool.InputSchema,
			OutputSchema: sp.tool.OutputSchema,
			Example:      sp.example,
		}
	}
	return out
}

// DocsTopics returns every docs topic in display order, with the tools
// topic's generated reference appended to its static introduction.
func DocsTopics() ([]docs.Topic, error) {
	topics, err := docs.All()
	if err != nil {
		return nil, err
	}
	for i := range topics {
		if topics[i].Name == docs.ToolsTopic {
			topics[i].Content += renderTools(ToolDocs())
		}
	}
	return topics, nil
}

// DocsTopic returns one topic as DocsTopics renders it.
func DocsTopic(name string) (docs.Topic, error) {
	t, err := docs.Get(name)
	if err != nil {
		return docs.Topic{}, err
	}
	if name == docs.ToolsTopic {
		t.Content += renderTools(ToolDocs())
	}
	return t, nil
}

// renderTools renders the tool reference as markdown.
func renderTools(tools []ToolDoc) string {
	var b strings.Builder
	b.WriteString("\n## Tools\n\n")
	for _, t := range tools {
		fmt.Fprintf(&b, "- `%s`: %s\n", t.Name, annotationSummary(t.Annotations))
	}
	for _, t := range tools {
		fmt.Fprintf(&b, "\n## %s\n\n%s\n\n", t.Name, t.Description)
		fmt.Fprintf(&b, "Annotations: %s.\n\n", annotationSummary(t.Annotations))
		writeJSONBlock(&b, "Example arguments", t.Example)
		writeJSONBlock(&b, "Input schema", t.InputSchema)
		writeJSONBlock(&b, "Output schema", t.OutputSchema)
	}
	return b.String()
}

func annotationSummary(a *mcp.ToolAnnotations) string {
	if a == nil {
		return "none"
	}
	var parts []string
	if a.ReadOnlyHint {
		parts = append(parts, "read-only")
	} else {
		parts = append(parts, "writes")
	}
	if a.DestructiveHint != nil && *a.DestructiveHint {
		parts = append(parts, "destructive")
	}
	if a.IdempotentHint {
		parts = append(parts, "idempotent")
	}
	return strings.Join(parts, ", ")
}

func writeJSONBlock(b *strings.Builder, title string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		// Schemas and examples are plain data; this cannot happen.
		data = []byte(fmt.Sprintf("%q", err.Error()))
	}
	fmt.Fprintf(b, "%s:\n\n```json\n%s\n```\n\n", title, data)
}

// Instructions returns the server instructions sent in the initialize
// result: a condensed guide that points to the docs resources.
func Instructions() string {
	var b strings.Builder
	b.WriteString(`cachydb is a document database: databases hold collections, collections hold JSON documents with a string _id.

Typical workflow:
1. list_databases; pick one with use_database (this session only) or pass "database" to each call. Without either, the server default is used (current_database tells which).
2. create_collection, optionally with a schema: {"fields": {"name": {"type": "string", "required": true}}}.
3. insert_document with {"collection", "document"}; a string _id is optional (a UUID is generated).
4. find_documents with filters inside "query": {"query": {"filters": [{"field": "age", "operator": "gte", "value": 18}], "limit": 10}}. Operators: eq ne gt gte lt lte in. All filters must match. A document missing the field matches nothing, not even ne.
5. update_document merges top-level fields; delete_document removes by id; create_index speeds up eq filters.

Ids are strings. Results are sorted by _id. Numbers are JSON numbers; Go callers get integers back as floats. Every successful write is durable.

Full documentation (read as resources):
`)
	for _, name := range docs.Names {
		fmt.Fprintf(&b, "- %s%s\n", DocsURIPrefix, name)
	}
	return b.String()
}

// registerDocsResources registers one text/markdown resource per docs
// topic at cachydb://docs/<topic>.
func registerDocsResources(srv *mcp.Server) error {
	topics, err := DocsTopics()
	if err != nil {
		return err
	}
	for _, t := range topics {
		uri := DocsURIPrefix + t.Name
		content := t.Content
		srv.AddResource(&mcp.Resource{
			URI:         uri,
			Name:        t.Title,
			Title:       t.Title,
			Description: t.Summary,
			MIMEType:    "text/markdown",
		}, func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{
				{URI: uri, MIMEType: "text/markdown", Text: content},
			}}, nil
		})
	}
	return nil
}
