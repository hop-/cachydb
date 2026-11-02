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
var Operators = []string{"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "has", "has_any", "has_all"}

// SortOrders lists the sort orders of find_documents.
var SortOrders = []string{"asc", "desc"}

// FieldTypes lists the collection schema field types.
var FieldTypes = []string{"string", "number", "boolean", "object", "array", "date"}

// Operator is a find_documents filter operator; its schema is an enum of
// Operators.
type Operator string

// FieldType is a collection schema field type; its schema is an enum of
// FieldTypes.
type FieldType string

// SortOrder is a sort order; its schema is an enum of SortOrders.
type SortOrder string

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
	Document   map[string]any `json:"document" jsonschema:"The document as a JSON object. A string _id sets the document id; without one a UUID is generated. {\"$now\": true} is the server's current time"`
}

// Filter is one find_documents filter.
type Filter struct {
	Field    string   `json:"field" jsonschema:"Top-level document field name, or _id (no dot notation)"`
	Operator Operator `json:"operator" jsonschema:"Comparison operator: eq ne gt gte lt lte in (whole values); contains (case-insensitive substring of a string, or of a string element of an array); has (an array element equals value); has_any / has_all (some / every value of an array value is an element)"`
	Value    any      `json:"value" jsonschema:"Value to compare with; an array for in, has_any and has_all; {\"$now\": true} is the server's current time"`
}

// SortKey is one find_documents sort key.
type SortKey struct {
	Field string    `json:"field" jsonschema:"Top-level field (or _id) to sort by"`
	Order SortOrder `json:"order,omitempty" jsonschema:"asc (default) or desc"`
}

// Query is the find_documents query.
type Query struct {
	Filters []Filter   `json:"filters,omitempty" jsonschema:"Filters that must all match (AND); omit to match every document"`
	Any     [][]Filter `json:"any,omitempty" jsonschema:"OR groups: when set, a document must also match every filter of at least one group"`
	Fields  []string   `json:"fields,omitempty" jsonschema:"Top-level fields to return (projection); _id is always returned; omit for whole documents"`
	Sort    []SortKey  `json:"sort,omitempty" jsonschema:"Sort keys in order, then _id ascending; omit to sort by _id. Sorting by other fields reads every match"`
	Limit   int        `json:"limit,omitempty" jsonschema:"Maximum number of documents to return; 0 or omitted means no limit"`
	Skip    int        `json:"skip,omitempty" jsonschema:"Number of matching documents to skip, after sorting"`
}

// MatchQuery is the query of count_documents and distinct: filters and OR
// groups only.
type MatchQuery struct {
	Filters []Filter   `json:"filters,omitempty" jsonschema:"Filters that must all match (AND); omit to match every document"`
	Any     [][]Filter `json:"any,omitempty" jsonschema:"OR groups: when set, a document must also match every filter of at least one group"`
}

type FindDocumentsInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	Query      *Query `json:"query,omitempty" jsonschema:"Filters, OR groups, projection, sort, limit, and skip; omit to return every document"`
}

type UpdateDocumentInput struct {
	Database   string         `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string         `json:"collection" jsonschema:"Collection name"`
	ID         string         `json:"id" jsonschema:"Document _id"`
	Updates    map[string]any `json:"updates,omitempty" jsonschema:"Top-level fields to set (replaced or added); other fields are kept; _id cannot be changed. {\"$now\": true} is the server's current time"`
	Unset      []string       `json:"unset,omitempty" jsonschema:"Top-level fields to remove (after updates); at least one of updates and unset is required"`
	If         []Filter       `json:"if,omitempty" jsonschema:"Condition: the stored document must match all of these filters, or nothing is written (compare-and-set)"`
	Upsert     bool           `json:"upsert,omitempty" jsonschema:"Create the document as {_id: id} plus updates if it does not exist"`
}

type ReplaceDocumentInput struct {
	Database   string         `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string         `json:"collection" jsonschema:"Collection name"`
	ID         string         `json:"id" jsonschema:"_id of the existing document to replace"`
	Document   map[string]any `json:"document" jsonschema:"The new document; its _id, if given, must equal id. {\"$now\": true} is the server's current time"`
	If         []Filter       `json:"if,omitempty" jsonschema:"Condition: the stored document must match all of these filters, or nothing is written"`
}

type DeleteDocumentInput struct {
	Database   string   `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string   `json:"collection" jsonschema:"Collection name"`
	ID         string   `json:"id" jsonschema:"Document _id"`
	If         []Filter `json:"if,omitempty" jsonschema:"Condition: the stored document must match all of these filters, or nothing is deleted"`
}

type CountDocumentsInput struct {
	Database   string      `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string      `json:"collection" jsonschema:"Collection name"`
	Query      *MatchQuery `json:"query,omitempty" jsonschema:"Filters and OR groups; omit to count every document"`
}

type DistinctInput struct {
	Database   string      `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string      `json:"collection" jsonschema:"Collection name"`
	Field      string      `json:"field" jsonschema:"Top-level field whose distinct values to list; array values contribute each element"`
	Query      *MatchQuery `json:"query,omitempty" jsonschema:"Filters and OR groups selecting the documents; omit for every document"`
}

type DescribeCollectionInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
}

type UpdateSchemaInput struct {
	Database   string     `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string     `json:"collection" jsonschema:"Collection name"`
	Schema     *SchemaDef `json:"schema" jsonschema:"The new schema, or null to remove it; every existing document must satisfy it"`
}

type RenameCollectionInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection to rename"`
	NewName    string `json:"new_name" jsonschema:"New collection name in the same database; must not exist"`
}

type CopyCollectionInput struct {
	Database       string `json:"database,omitempty" jsonschema:"Database of the source collection; defaults to the session's use_database choice, then the server default"`
	Collection     string `json:"collection" jsonschema:"Source collection"`
	NewName        string `json:"new_name" jsonschema:"Name of the new collection; must not exist"`
	TargetDatabase string `json:"target_database,omitempty" jsonschema:"Existing database for the copy; defaults to the source database"`
}

type DeleteCollectionInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection to delete with all its documents and indexes"`
}

type DropIndexInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	IndexName  string `json:"index_name" jsonschema:"Name of the index to drop"`
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
	Documents []map[string]any `json:"documents" jsonschema:"Matching documents in sort order (by default by _id), each a flat object with its _id"`
}

type UpdateDocumentOutput struct {
	Success  bool   `json:"success" jsonschema:"Always true on success"`
	Message  string `json:"message" jsonschema:"Human-readable summary"`
	Upserted bool   `json:"upserted" jsonschema:"True if upsert created the document"`
}

type CountDocumentsOutput struct {
	Success bool `json:"success" jsonschema:"Always true on success"`
	Count   int  `json:"count" jsonschema:"Number of matching documents"`
}

type DistinctOutput struct {
	Success bool  `json:"success" jsonschema:"Always true on success"`
	Values  []any `json:"values" jsonschema:"Distinct values, sorted: null, false, true, numbers, strings, arrays, objects"`
}

type DescribeCollectionOutput struct {
	Success  bool        `json:"success" jsonschema:"Always true on success"`
	Database string      `json:"database" jsonschema:"Database of the collection"`
	Name     string      `json:"name" jsonschema:"Collection name"`
	Schema   *SchemaDef  `json:"schema" jsonschema:"The schema, or null for a schemaless collection"`
	Indexes  []IndexDesc `json:"indexes" jsonschema:"Secondary indexes, sorted by name"`
	Count    int         `json:"count" jsonschema:"Number of documents"`
}

// IndexDesc describes one secondary index.
type IndexDesc struct {
	Name  string `json:"name" jsonschema:"Index name"`
	Field string `json:"field" jsonschema:"Indexed top-level field"`
}

type CopyCollectionOutput struct {
	Success bool   `json:"success" jsonschema:"Always true on success"`
	Message string `json:"message" jsonschema:"Human-readable summary"`
	Copied  int    `json:"copied" jsonschema:"Number of documents copied"`
}

// toolSpec is one tool: its definition, an example call, and how to
// register its handler.
type toolSpec struct {
	tool     *mcp.Tool
	example  map[string]any
	register func(s *Server, srv *mcp.Server, t *mcp.Tool)
}

// schemaOptions renders Operator, FieldType, and SortOrder as string
// enums.
var schemaOptions = &jsonschema.ForOptions{TypeSchemas: map[reflect.Type]*jsonschema.Schema{
	reflect.TypeFor[Operator]():  {Type: "string", Enum: toAny(Operators)},
	reflect.TypeFor[FieldType](): {Type: "string", Enum: toAny(FieldTypes)},
	reflect.TypeFor[SortOrder](): {Type: "string", Enum: toAny(SortOrders)},
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
			"Find the documents of a collection matching every filter in query.filters (and, if given, at least one group of query.any), with optional sort, projection (fields), skip, and limit; sorted by _id by default. Operators include contains (case-insensitive substring) and has / has_any / has_all (array elements). Comparison is type-aware and a document missing the field matches no operator, not even ne. See resource cachydb://docs/query.",
			readOnly("Find documents"),
			map[string]any{"collection": "products", "query": map[string]any{
				"filters": []any{map[string]any{"field": "price", "operator": "lt", "value": 10}},
				"sort":    []any{map[string]any{"field": "price", "order": "desc"}},
				"fields":  []any{"name", "price"}, "limit": 20,
			}},
			func(s *Server) mcp.ToolHandlerFor[FindDocumentsInput, FindDocumentsOutput] {
				return s.findDocumentsTool
			}),
		newSpec("count_documents",
			"Count the documents of a collection matching query.filters and query.any, without returning them. A single eq or has filter on an indexed field is answered from the index.",
			readOnly("Count documents"),
			map[string]any{"collection": "products", "query": map[string]any{
				"filters": []any{map[string]any{"field": "price", "operator": "lt", "value": 10}},
			}},
			func(s *Server) mcp.ToolHandlerFor[CountDocumentsInput, CountDocumentsOutput] {
				return s.countDocumentsTool
			}),
		newSpec("distinct",
			"List the distinct values of a field among the documents matching query (all by default). Array values contribute each element, so this lists tags. Values are sorted.",
			readOnly("Distinct values"),
			map[string]any{"collection": "products", "field": "name"},
			func(s *Server) mcp.ToolHandlerFor[DistinctInput, DistinctOutput] { return s.distinctTool }),
		newSpec("update_document",
			"Merge top-level fields into a document (updates) and remove fields (unset); other fields are kept and the result must satisfy the schema. With if, the write happens only if the stored document matches those filters (compare-and-set; otherwise a condition-failed error). With upsert, a missing document is created.",
			writing("Update document", false, false),
			map[string]any{"collection": "products", "id": "p1", "updates": map[string]any{"price": 3},
				"if": []any{map[string]any{"field": "price", "operator": "eq", "value": 2.5}}},
			func(s *Server) mcp.ToolHandlerFor[UpdateDocumentInput, UpdateDocumentOutput] {
				return s.updateDocumentTool
			}),
		newSpec("replace_document",
			"Replace a whole existing document atomically (fields not in the new document are removed). The result must satisfy the schema; with if, the stored document must match those filters.",
			writing("Replace document", false, true),
			map[string]any{"collection": "products", "id": "p1", "document": map[string]any{"name": "Pen", "price": 3}},
			func(s *Server) mcp.ToolHandlerFor[ReplaceDocumentInput, MessageOutput] {
				return s.replaceDocumentTool
			}),
		newSpec("delete_document",
			"Delete a document by _id; with if, only if it matches those filters. This cannot be undone.",
			writing("Delete document", true, true),
			map[string]any{"collection": "products", "id": "p1"},
			func(s *Server) mcp.ToolHandlerFor[DeleteDocumentInput, MessageOutput] { return s.deleteDocumentTool }),
		newSpec("describe_collection",
			"Describe a collection: its schema (or null), its indexes, and its document count.",
			readOnly("Describe collection"),
			map[string]any{"collection": "products"},
			func(s *Server) mcp.ToolHandlerFor[DescribeCollectionInput, DescribeCollectionOutput] {
				return s.describeCollectionTool
			}),
		newSpec("update_schema",
			"Replace a collection's schema, or remove it with null. Every existing document is checked first; if any fails, nothing changes and the error lists the failing ids. Writes to the database wait meanwhile.",
			writing("Update schema", false, true),
			map[string]any{"collection": "products", "schema": map[string]any{"fields": map[string]any{
				"name": map[string]any{"type": "string", "required": true}, "price": map[string]any{"type": "number"},
			}}},
			func(s *Server) mcp.ToolHandlerFor[UpdateSchemaInput, MessageOutput] { return s.updateSchemaTool }),
		newSpec("rename_collection",
			"Rename a collection within its database, keeping its documents, schema, and indexes. The new name must not exist.",
			writing("Rename collection", false, false),
			map[string]any{"collection": "products_backup", "new_name": "products_old"},
			func(s *Server) mcp.ToolHandlerFor[RenameCollectionInput, MessageOutput] {
				return s.renameCollectionTool
			}),
		newSpec("copy_collection",
			"Copy a collection (schema, indexes, and documents with the same ids) to a new collection, in the same or another database. Not atomic: after an error the copy may be partial; delete it and retry.",
			writing("Copy collection", false, false),
			map[string]any{"collection": "products", "new_name": "products_backup"},
			func(s *Server) mcp.ToolHandlerFor[CopyCollectionInput, CopyCollectionOutput] {
				return s.copyCollectionTool
			}),
		newSpec("delete_collection",
			"Delete a collection with all its documents and indexes. This cannot be undone.",
			writing("Delete collection", true, true),
			map[string]any{"collection": "products_old"},
			func(s *Server) mcp.ToolHandlerFor[DeleteCollectionInput, MessageOutput] {
				return s.deleteCollectionTool
			}),
		newSpec("create_index",
			"Create a hash index on a top-level field so eq filters (and has, has_any, has_all on array fields, whose elements are indexed) avoid a full scan. Writes to the database wait while the index builds.",
			writing("Create index", false, false),
			map[string]any{"collection": "products", "index_name": "by_name", "field_name": "name"},
			func(s *Server) mcp.ToolHandlerFor[CreateIndexInput, MessageOutput] { return s.createIndexTool }),
		newSpec("drop_index",
			"Drop a secondary index; queries on its field scan again.",
			writing("Drop index", true, true),
			map[string]any{"collection": "products", "index_name": "by_name"},
			func(s *Server) mcp.ToolHandlerFor[DropIndexInput, MessageOutput] { return s.dropIndexTool }),
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
