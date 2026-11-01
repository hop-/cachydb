package mcpserver

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/hop-/cachydb/internal/docs"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// schemaAt walks a JSON Schema (as decoded JSON) along path, where each
// element is a key of the current object.
func schemaAt(t *testing.T, schema any, path ...string) map[string]any {
	t.Helper()
	data, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	var cur any
	if err := json.Unmarshal(data, &cur); err != nil {
		t.Fatalf("unmarshal schema: %v", err)
	}
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("schema path %v: %q is not in an object", path, p)
		}
		if cur, ok = m[p]; !ok {
			t.Fatalf("schema path %v: no %q", path, p)
		}
	}
	m, ok := cur.(map[string]any)
	if !ok {
		t.Fatalf("schema path %v: not an object", path)
	}
	return m
}

func enumOf(t *testing.T, m map[string]any) []string {
	t.Helper()
	raw, ok := m["enum"].([]any)
	if !ok {
		t.Fatalf("no enum in %v", m)
	}
	out := make([]string, len(raw))
	for i, v := range raw {
		out[i], _ = v.(string)
	}
	return out
}

func listTools(t *testing.T, cs *mcp.ClientSession) []*mcp.Tool {
	t.Helper()
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	return res.Tools
}

func toolNamed(t *testing.T, tools []*mcp.Tool, name string) *mcp.Tool {
	t.Helper()
	for _, tl := range tools {
		if tl.Name == name {
			return tl
		}
	}
	t.Fatalf("tool %s not listed", name)
	return nil
}

func TestToolSchemaEnums(t *testing.T) {
	tools := listTools(t, connect(t, newTestServer(t)))

	op := schemaAt(t, toolNamed(t, tools, "find_documents").InputSchema,
		"properties", "query", "properties", "filters", "items", "properties", "operator")
	if got := enumOf(t, op); !slices.Equal(got, Operators) {
		t.Errorf("operator enum = %v, want %v", got, Operators)
	}
	ft := schemaAt(t, toolNamed(t, tools, "create_collection").InputSchema,
		"properties", "schema", "properties", "fields", "additionalProperties", "properties", "type")
	if got := enumOf(t, ft); !slices.Equal(got, FieldTypes) {
		t.Errorf("field type enum = %v, want %v", got, FieldTypes)
	}
}

func TestToolAnnotationsAndOutputSchemas(t *testing.T) {
	type hints struct{ readOnly, destructive, idempotent bool }
	want := map[string]hints{
		"list_databases":    {readOnly: true, idempotent: true},
		"list_collections":  {readOnly: true, idempotent: true},
		"find_documents":    {readOnly: true, idempotent: true},
		"current_database":  {readOnly: true, idempotent: true},
		"create_database":   {idempotent: true},
		"use_database":      {idempotent: true},
		"create_collection": {},
		"insert_document":   {},
		"create_index":      {},
		"update_document":   {},
		"delete_document":   {destructive: true, idempotent: true},
		"delete_database":   {destructive: true, idempotent: true},
	}
	tools := listTools(t, connect(t, newTestServer(t)))
	if len(tools) != len(want) {
		t.Fatalf("listed %d tools, want %d", len(tools), len(want))
	}
	for _, tl := range tools {
		t.Run(tl.Name, func(t *testing.T) {
			w, ok := want[tl.Name]
			if !ok {
				t.Fatalf("unexpected tool")
			}
			a := tl.Annotations
			if a == nil {
				t.Fatal("no annotations")
			}
			destructive := a.DestructiveHint != nil && *a.DestructiveHint
			if got := (hints{a.ReadOnlyHint, destructive, a.IdempotentHint}); got != w {
				t.Errorf("annotations = %+v, want %+v", got, w)
			}
			if !w.readOnly && a.DestructiveHint == nil {
				t.Error("destructiveHint not set on a writing tool")
			}
			if a.Title == "" {
				t.Error("no title")
			}
			if tl.OutputSchema == nil {
				t.Error("no output schema")
			}
			if tl.Description == "" || !strings.HasSuffix(tl.Description, ".") {
				t.Errorf("description %q is not a sentence", tl.Description)
			}
			if _, ok := schemaAt(t, tl.OutputSchema, "properties")["success"]; !ok {
				t.Error("output schema has no success property")
			}
		})
	}
}

// TestToolDefinitionsMatchRegistered: ToolDefinitions (the source of the
// tools docs topic) is exactly what the server registers.
func TestToolDefinitionsMatchRegistered(t *testing.T) {
	tools := listTools(t, connect(t, newTestServer(t)))
	var listed []string
	for _, tl := range tools {
		listed = append(listed, tl.Name)
	}
	var defined []string
	for _, tl := range ToolDefinitions() {
		defined = append(defined, tl.Name)
	}
	slices.Sort(listed)
	slices.Sort(defined)
	if !slices.Equal(listed, defined) {
		t.Fatalf("registered %v, ToolDefinitions %v", listed, defined)
	}

	topic, err := DocsTopic(docs.ToolsTopic)
	if err != nil {
		t.Fatalf("DocsTopic(tools): %v", err)
	}
	var documented []string
	for _, line := range strings.Split(topic.Content, "\n") {
		if name, ok := strings.CutPrefix(line, "## "); ok && name != "Tools" {
			documented = append(documented, name)
		}
	}
	slices.Sort(documented)
	if !slices.Equal(documented, defined) {
		t.Fatalf("tools topic documents %v, want %v", documented, defined)
	}
	for _, td := range ToolDocs() {
		if td.Example == nil {
			t.Errorf("tool %s has no example", td.Name)
		}
	}
}

// TestToolExamplesAreValid calls every tool with its documented example
// (in an order where they make sense) and expects success.
func TestToolExamplesAreValid(t *testing.T) {
	cs := connect(t, newTestServer(t))
	examples := map[string]map[string]any{}
	for _, td := range ToolDocs() {
		examples[td.Name] = td.Example
	}
	order := []string{
		"list_databases", "create_database", "use_database", "current_database",
		"create_collection", "list_collections", "insert_document", "find_documents",
		"update_document", "create_index", "delete_document", "delete_database",
	}
	for _, name := range order {
		res := call(t, cs, name, examples[name])
		if res.IsError {
			t.Fatalf("%s(%v) failed: %s", name, examples[name], resultText(res))
		}
	}
}

func TestInstructions(t *testing.T) {
	ins := Instructions()
	if n := len(ins); n == 0 || n > 2000 {
		t.Fatalf("instructions are %d characters, want 1..2000", n)
	}
	refs := docs.References(ins)
	for _, name := range docs.Names {
		if !slices.Contains(refs, name) {
			t.Errorf("instructions do not point to %s%s", DocsURIPrefix, name)
		}
	}
	for _, r := range refs {
		if _, err := docs.Get(r); err != nil {
			t.Errorf("instructions reference unknown topic %q", r)
		}
	}

	tests := []struct {
		name    string
		version string
	}{
		{name: "latest protocol", version: ""},
		{name: "initialize handshake", version: "2025-11-25"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t)
			serverT, clientT := mcp.NewInMemoryTransports()
			if _, err := s.server.Connect(context.Background(), serverT, nil); err != nil {
				t.Fatalf("server Connect: %v", err)
			}
			client := mcp.NewClient(&mcp.Implementation{Name: "t", Version: "1"}, nil)
			cs, err := client.Connect(context.Background(), clientT, &mcp.ClientSessionOptions{ProtocolVersion: tt.version})
			if err != nil {
				t.Fatalf("client Connect: %v", err)
			}
			t.Cleanup(func() { _ = cs.Close() })
			res := cs.InitializeResult()
			if res.Instructions != ins {
				t.Errorf("instructions = %q, want Instructions()", res.Instructions)
			}
			if res.ServerInfo == nil || res.ServerInfo.Version != testVersion {
				t.Errorf("server info = %+v, want version %q", res.ServerInfo, testVersion)
			}
		})
	}
}

func TestDocsResources(t *testing.T) {
	cs := connect(t, newTestServer(t))
	ctx := context.Background()
	res, err := cs.ListResources(ctx, nil)
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	var uris []string
	for _, r := range res.Resources {
		uris = append(uris, r.URI)
		if r.MIMEType != "text/markdown" || r.Name == "" || r.Description == "" {
			t.Errorf("resource %s: %+v", r.URI, r)
		}
	}
	var want []string
	for _, n := range docs.Names {
		want = append(want, DocsURIPrefix+n)
	}
	slices.Sort(uris)
	slices.Sort(want)
	if !slices.Equal(uris, want) {
		t.Fatalf("resources = %v, want %v", uris, want)
	}

	for _, name := range []string{"query", "tools"} {
		got, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: DocsURIPrefix + name})
		if err != nil {
			t.Fatalf("ReadResource(%s): %v", name, err)
		}
		topic, err := DocsTopic(name)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Contents) != 1 || got.Contents[0].Text != topic.Content || got.Contents[0].MIMEType != "text/markdown" {
			t.Errorf("ReadResource(%s) does not return the topic content", name)
		}
	}
}

func TestInvalidArgumentsHelpfulErrors(t *testing.T) {
	cs := connect(t, newTestServer(t))
	callOK(t, cs, "create_collection", map[string]any{"name": "c"})
	tests := []struct {
		name string
		tool string
		args map[string]any
		want []string
	}{
		{
			name: "unknown operator",
			tool: "find_documents",
			args: map[string]any{"collection": "c", "query": map[string]any{
				"filters": []any{map[string]any{"field": "a", "operator": "like", "value": "x"}},
			}},
			want: []string{"operator", "eq ne gt gte lt lte in"},
		},
		{
			name: "filters outside query",
			tool: "find_documents",
			args: map[string]any{"collection": "c", "filter": map[string]any{"a": 1}},
			want: []string{"filter"},
		},
		{
			name: "unknown field type",
			tool: "create_collection",
			args: map[string]any{"name": "s", "schema": map[string]any{"fields": map[string]any{"a": map[string]any{"type": "int"}}}},
			want: []string{"type", "string number boolean object array date"},
		},
		{
			name: "missing collection",
			tool: "insert_document",
			args: map[string]any{"document": map[string]any{"a": 1}},
			want: []string{"collection"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := call(t, cs, tt.tool, tt.args)
			if !res.IsError {
				t.Fatalf("%s succeeded, want an error", tt.tool)
			}
			text := resultText(res)
			for _, w := range tt.want {
				if !strings.Contains(text, w) {
					t.Errorf("error %q does not mention %q", text, w)
				}
			}
		})
	}
}
