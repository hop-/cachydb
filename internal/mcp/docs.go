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

Workflow:
1. list_databases; use_database (this session) or pass "database"; else the server default (current_database).
2. create_collection, optionally with a schema; describe_collection shows schema, indexes, count.
3. insert_document {"collection", "document"}; _id optional (UUID).
4. find_documents {"query": {"filters": [{"field": "age", "operator": "gte", "value": 18}], "any": [[...], [...]], "sort": [{"field": "age", "order": "desc"}], "fields": ["name"], "limit": 10}}. Operators: eq ne gt gte lt lte in, contains (case-insensitive substring), has has_any has_all (array elements; indexed). Filters AND; "any" groups OR. A missing field matches nothing, not even ne. count_documents and distinct take the same filters.
5. update_document: "updates" set fields, "unset" removes them, "upsert" creates; replace_document swaps a whole document. "if": [filters] on update/replace/delete writes only if the stored document still matches (compare-and-set; on "condition failed" re-read and retry).
6. {"$now": true} as a value is the server time (RFC 3339 UTC).
7. update_schema (checks every document), rename_collection, copy_collection, delete_collection, create_index, drop_index.

Results sort by _id unless "sort" is given. Numbers are JSON numbers. Every successful write is durable.

`)
	b.WriteString(`Full documentation (read as resources):
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
