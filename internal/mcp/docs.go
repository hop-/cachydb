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

// ToolDocs returns the documentation of every tool the server always
// registers, in registration order; the tools topic renders it. The user
// tools are in AccessToolDocs.
func ToolDocs() []ToolDoc { return toolDocs(toolSpecs()) }

func toolDocs(specs []toolSpec) []ToolDoc {
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

// DocsTopics returns every docs topic in display order, with the
// generated tool references appended to the static introductions of the
// tools topic (ToolDocs) and the access topic (AccessToolDocs).
func DocsTopics() ([]docs.Topic, error) {
	topics, err := docs.All()
	if err != nil {
		return nil, err
	}
	for i := range topics {
		topics[i].Content += generatedContent(topics[i].Name)
	}
	return topics, nil
}

// generatedContent returns the generated part of topic name, if any.
func generatedContent(name string) string {
	switch name {
	case docs.ToolsTopic:
		return renderTools("Tools", ToolDocs())
	case docs.AccessTopic:
		return renderTools("User tools", AccessToolDocs())
	default:
		return ""
	}
}

// DocsTopic returns one topic as DocsTopics renders it.
func DocsTopic(name string) (docs.Topic, error) {
	t, err := docs.Get(name)
	if err != nil {
		return docs.Topic{}, err
	}
	t.Content += generatedContent(name)
	return t, nil
}

// renderTools renders a tool reference as markdown under heading.
func renderTools(heading string, tools []ToolDoc) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n## %s\n\n", heading)
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
1. list_databases; use_database (this session) or pass "database"; else your default, then the server default.
2. create_collection, optionally with a schema; describe_collection shows schema, indexes, count.
3. insert_document {"collection", "document"} (_id optional); insert_documents takes many.
4. find_documents {"query": {"filters": [{"field": "age", "operator": "gte", "value": 18}], "any": [[...]], "sort": [{"field": "age", "order": "desc"}], "fields": ["name"], "limit": 10}}. No limit: 100 docs, 1 MiB by default; if has_more, repeat with "skip": next_skip. "explain": true adds the plan. get_document {"collection", "id"} reads one. Operators: eq ne gt gte lt lte in, contains (case-insensitive), starts_with, has has_any has_all (array elements), exists. Fields may be paths (a.b). Filters AND; "any" groups OR. A missing field matches only exists.
5. update_document: "updates" set, "unset" removes, inc/push/add_to_set/pull change in place, "upsert" creates; replace_document swaps the document. "if": [filters] writes only if the document still matches (on condition_failed re-read and retry). update_documents and delete_documents take a query; each batch call is atomic.
6. {"$now": true} as a value is the server time.
7. update_schema, rename_collection, copy_collection, delete_collection, create_index (unique optional), drop_index; server_stats shows sizes.

Errors start with a code such as [not_found] or [permission_denied]. With tokens, access is per database (whoami). Results sort by _id unless "sort" is given. Every successful write is durable.

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
