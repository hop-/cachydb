package mcpserver

import (
	"fmt"
	"reflect"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Tool definitions: the single source of truth for the MCP tools. The
// server registers exactly these (registerTools), and the generated
// "tools" docs topic is rendered from them (see docs.go), so the docs
// cannot drift from the code.

// Operators lists the find_documents filter operators.
var Operators = []string{"eq", "ne", "gt", "gte", "lt", "lte", "in"}

// FieldTypes lists the collection schema field types.
var FieldTypes = []string{"string", "number", "boolean", "object", "array", "date"}

// Operator is a find_documents filter operator; its schema is an enum of
// Operators.
type Operator string

// FieldType is a collection schema field type; its schema is an enum of
// FieldTypes.
type FieldType string

// Tool inputs.

type CreateDatabaseInput struct {
	Name string `json:"name" jsonschema:"Database name: letters, digits, _ and -, starting with a letter or digit, at most 64 characters"`
}

type ListDatabasesInput struct{}

type DeleteDatabaseInput struct {
	Name string `json:"name" jsonschema:"Name of the database to delete, with all its collections and documents"`
}

type UseDatabaseInput struct {
	Name string `json:"name" jsonschema:"Name of an existing database to use as this session's default"`
}

type CurrentDatabaseInput struct{}

// FieldDef is one field of a collection schema.
type FieldDef struct {
	Type     FieldType `json:"type" jsonschema:"Field type"`
	Required bool      `json:"required,omitempty" jsonschema:"The field must be present in every document"`
}

// SchemaDef is a collection schema.
type SchemaDef struct {
	Fields map[string]FieldDef `json:"fields" jsonschema:"Field name to definition; at least one field; _id is not allowed"`
}

type CreateCollectionInput struct {
	Database string     `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Name     string     `json:"name" jsonschema:"Collection name: letters, digits, _ and -, starting with a letter or digit, at most 64 characters"`
	Schema   *SchemaDef `json:"schema,omitempty" jsonschema:"Optional schema, checked on every insert and update; fixed once created. Omit for a schemaless collection"`
}

type ListCollectionsInput struct {
	Database string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
}

type InsertDocumentInput struct {
	Database   string         `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string         `json:"collection" jsonschema:"Collection name"`
	Document   map[string]any `json:"document" jsonschema:"The document as a JSON object. A string _id sets the document id; without one a UUID is generated"`
}

// Filter is one find_documents filter.
type Filter struct {
	Field    string   `json:"field" jsonschema:"Top-level document field name, or _id (no dot notation)"`
	Operator Operator `json:"operator" jsonschema:"Comparison operator"`
	Value    any      `json:"value" jsonschema:"Value to compare with; for in, an array of values"`
}

// Query is the find_documents query.
type Query struct {
	Filters []Filter `json:"filters,omitempty" jsonschema:"Filters that must all match (AND); omit to match every document"`
	Limit   int      `json:"limit,omitempty" jsonschema:"Maximum number of documents to return; 0 or omitted means no limit"`
	Skip    int      `json:"skip,omitempty" jsonschema:"Number of matching documents to skip, after sorting by _id"`
}

type FindDocumentsInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	Query      *Query `json:"query,omitempty" jsonschema:"Filters, limit, and skip; omit to return every document"`
}

type UpdateDocumentInput struct {
	Database   string         `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string         `json:"collection" jsonschema:"Collection name"`
	ID         string         `json:"id" jsonschema:"Document _id"`
	Updates    map[string]any `json:"updates" jsonschema:"Top-level fields to set (replaced or added); other fields are kept; _id cannot be changed"`
}

type DeleteDocumentInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	ID         string `json:"id" jsonschema:"Document _id"`
}

type CreateIndexInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	IndexName  string `json:"index_name" jsonschema:"Index name: letters, digits, _ and -, starting with a letter or digit, at most 64 characters"`
	FieldName  string `json:"field_name" jsonschema:"Top-level field to index"`
}

// Tool outputs. The keys are the ones the tools always returned.

// MessageOutput is the result of tools that only report success.
type MessageOutput struct {
	Success bool   `json:"success" jsonschema:"Always true on success"`
	Message string `json:"message" jsonschema:"Human-readable summary"`
}

type ListDatabasesOutput struct {
	Success   bool     `json:"success" jsonschema:"Always true on success"`
	Databases []string `json:"databases" jsonschema:"Database names, sorted"`
}

type UseDatabaseOutput struct {
	Success         bool   `json:"success" jsonschema:"Always true on success"`
	Message         string `json:"message" jsonschema:"Human-readable summary"`
	CurrentDatabase string `json:"current_database" jsonschema:"The session's default database from now on"`
}

type CurrentDatabaseOutput struct {
	Success         bool   `json:"success" jsonschema:"Always true on success"`
	CurrentDatabase string `json:"current_database" jsonschema:"The session's use_database choice, or the server default"`
}

type ListCollectionsOutput struct {
	Success     bool     `json:"success" jsonschema:"Always true on success"`
	Collections []string `json:"collections" jsonschema:"Collection names, sorted"`
	Database    string   `json:"database" jsonschema:"The database that was listed"`
}

type InsertDocumentOutput struct {
	Success bool   `json:"success" jsonschema:"Always true on success"`
	ID      string `json:"id" jsonschema:"The document _id (given or generated)"`
	Message string `json:"message" jsonschema:"Human-readable summary"`
}

type FindDocumentsOutput struct {
	Success   bool             `json:"success" jsonschema:"Always true on success"`
	Count     int              `json:"count" jsonschema:"Number of documents returned"`
	Documents []map[string]any `json:"documents" jsonschema:"Matching documents sorted by _id, each a flat object with its _id"`
}

// toolSpec is one tool: its definition, an example call, and how to
// register its handler.
type toolSpec struct {
	tool     *mcp.Tool
	example  map[string]any
	register func(s *Server, srv *mcp.Server, t *mcp.Tool)
}

// schemaOptions renders Operator and FieldType as string enums.
var schemaOptions = &jsonschema.ForOptions{TypeSchemas: map[reflect.Type]*jsonschema.Schema{
	reflect.TypeFor[Operator]():  {Type: "string", Enum: toAny(Operators)},
	reflect.TypeFor[FieldType](): {Type: "string", Enum: toAny(FieldTypes)},
}}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// schemaFor returns the JSON Schema of T, for tool inputs and outputs.
func schemaFor[T any]() *jsonschema.Schema {
	s, err := jsonschema.For[T](schemaOptions)
	if err != nil {
		// The types are fixed above; a failure is a programming error that
		// every test run catches.
		panic(fmt.Sprintf("jsonschema for %v: %v", reflect.TypeFor[T](), err))
	}
	return s
}

func boolPtr(b bool) *bool { return &b }

// Annotation sets (approach-agent-ux §3.2). Every tool is closed-world:
// it only touches this cachydb instance.
func readOnly(title string) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: boolPtr(false)}
}

func writing(title string, destructive, idempotent bool) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{
		Title:           title,
		DestructiveHint: boolPtr(destructive),
		IdempotentHint:  idempotent,
		OpenWorldHint:   boolPtr(false),
	}
}

// newSpec builds a toolSpec for a typed handler.
func newSpec[In, Out any](
	name, description string,
	annotations *mcp.ToolAnnotations,
	example map[string]any,
	handler func(s *Server) mcp.ToolHandlerFor[In, Out],
) toolSpec {
	return toolSpec{
		tool: &mcp.Tool{
			Name:         name,
			Description:  description,
			Annotations:  annotations,
			InputSchema:  schemaFor[In](),
			OutputSchema: schemaFor[Out](),
		},
		example: example,
		register: func(s *Server, srv *mcp.Server, t *mcp.Tool) {
			mcp.AddTool(srv, t, handler(s))
		},
	}
}

// toolSpecs returns every tool, in the order they are listed in docs.
func toolSpecs() []toolSpec {
	return []toolSpec{
		newSpec("list_databases",
			"List every database on this server, sorted by name.",
			readOnly("List databases"), map[string]any{},
			func(s *Server) mcp.ToolHandlerFor[ListDatabasesInput, ListDatabasesOutput] {
				return s.listDatabasesTool
			}),
		newSpec("create_database",
			"Create a database. Creating a database that already exists succeeds and changes nothing.",
			writing("Create database", false, true), map[string]any{"name": "shop"},
			func(s *Server) mcp.ToolHandlerFor[CreateDatabaseInput, MessageOutput] { return s.createDatabaseTool }),
		newSpec("delete_database",
			"Delete a database with all its collections and documents. This cannot be undone.",
			writing("Delete database", true, true), map[string]any{"name": "shop"},
			func(s *Server) mcp.ToolHandlerFor[DeleteDatabaseInput, MessageOutput] { return s.deleteDatabaseTool }),
		newSpec("use_database",
			"Make an existing database the default for this session's later calls that omit the database argument. Other sessions are not affected.",
			writing("Use database", false, true), map[string]any{"name": "shop"},
			func(s *Server) mcp.ToolHandlerFor[UseDatabaseInput, UseDatabaseOutput] { return s.useDatabaseTool }),
		newSpec("current_database",
			"Return the database that calls without a database argument use: this session's use_database choice, or the server default.",
			readOnly("Current database"), map[string]any{},
			func(s *Server) mcp.ToolHandlerFor[CurrentDatabaseInput, CurrentDatabaseOutput] {
				return s.currentDatabaseTool
			}),
		newSpec("create_collection",
			"Create a collection, optionally with a schema (field types and required fields) that every insert and update must satisfy. See resource cachydb://docs/schema.",
			writing("Create collection", false, false),
			map[string]any{"name": "products", "schema": map[string]any{"fields": map[string]any{
				"name": map[string]any{"type": "string", "required": true}, "price": map[string]any{"type": "number"},
			}}},
			func(s *Server) mcp.ToolHandlerFor[CreateCollectionInput, MessageOutput] {
				return s.createCollectionTool
			}),
		newSpec("list_collections",
			"List the collections of a database, sorted by name.",
			readOnly("List collections"), map[string]any{"database": "shop"},
			func(s *Server) mcp.ToolHandlerFor[ListCollectionsInput, ListCollectionsOutput] {
				return s.listCollectionsTool
			}),
		newSpec("insert_document",
			"Insert a JSON document into a collection and return its id. A string _id in the document sets the id; otherwise a UUID is generated. Fails if the id exists or the schema rejects the document.",
			writing("Insert document", false, false),
			map[string]any{"collection": "products", "document": map[string]any{"_id": "p1", "name": "Pen", "price": 2.5}},
			func(s *Server) mcp.ToolHandlerFor[InsertDocumentInput, InsertDocumentOutput] {
				return s.insertDocumentTool
			}),
		newSpec("find_documents",
			"Find the documents of a collection that match every filter in query.filters, sorted by _id, with optional skip and limit. Comparison is type-aware and a document missing the field matches no operator, not even ne. See resource cachydb://docs/query.",
			readOnly("Find documents"),
			map[string]any{"collection": "products", "query": map[string]any{
				"filters": []any{map[string]any{"field": "price", "operator": "lt", "value": 10}}, "limit": 20,
			}},
			func(s *Server) mcp.ToolHandlerFor[FindDocumentsInput, FindDocumentsOutput] {
				return s.findDocumentsTool
			}),
		newSpec("update_document",
			"Merge top-level fields into a document: given fields are replaced or added, the others are kept. The merged document must satisfy the collection schema.",
			writing("Update document", false, false),
			map[string]any{"collection": "products", "id": "p1", "updates": map[string]any{"price": 3}},
			func(s *Server) mcp.ToolHandlerFor[UpdateDocumentInput, MessageOutput] { return s.updateDocumentTool }),
		newSpec("delete_document",
			"Delete a document by _id. This cannot be undone.",
			writing("Delete document", true, true),
			map[string]any{"collection": "products", "id": "p1"},
			func(s *Server) mcp.ToolHandlerFor[DeleteDocumentInput, MessageOutput] { return s.deleteDocumentTool }),
		newSpec("create_index",
			"Create a hash index on a top-level field so eq filters on it avoid a full scan. Writes to the database wait while the index builds.",
			writing("Create index", false, false),
			map[string]any{"collection": "products", "index_name": "by_name", "field_name": "name"},
			func(s *Server) mcp.ToolHandlerFor[CreateIndexInput, MessageOutput] { return s.createIndexTool }),
	}
}

// ToolDefinitions returns the definition of every MCP tool the server
// registers: name, description, annotations, and input and output JSON
// Schemas. It needs no engine. The returned tools are fresh copies.
func ToolDefinitions() []*mcp.Tool {
	specs := toolSpecs()
	out := make([]*mcp.Tool, len(specs))
	for i, sp := range specs {
		out[i] = sp.tool
	}
	return out
}

// registerTools registers every tool of toolSpecs on srv.
func (s *Server) registerTools(srv *mcp.Server) {
	for _, sp := range toolSpecs() {
		sp.register(s, srv, sp.tool)
	}
}
