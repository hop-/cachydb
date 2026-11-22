package mcpserver

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Tool definitions: the single source of truth for the MCP tools. The
// server registers exactly these (registerTools), and the generated
// "tools" docs topic is rendered from them (see docs.go), so the docs
// cannot drift from the code.

// Operators lists the find_documents filter operators.
var Operators = []string{"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "has", "has_any", "has_all", "exists", "starts_with"}

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
	Database string     `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Name     string     `json:"name" jsonschema:"Collection name: letters, digits, _ and -, starting with a letter or digit, at most 64 characters"`
	Schema   *SchemaDef `json:"schema,omitempty" jsonschema:"Optional schema, checked on every insert and update; change it later with update_schema. Omit for a schemaless collection"`
}

type ListCollectionsInput struct {
	Database string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
}

type InsertDocumentInput struct {
	Database   string         `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string         `json:"collection" jsonschema:"Collection name"`
	Document   map[string]any `json:"document" jsonschema:"The document as a JSON object. A string _id sets the document id; without one a UUID is generated. {\"$now\": true} is the server's current time"`
}

// Filter is one find_documents filter.
type Filter struct {
	Field    string   `json:"field" jsonschema:"Document field name, a dotted path into nested objects (meta.author), or _id"`
	Operator Operator `json:"operator" jsonschema:"Comparison operator: eq ne gt gte lt lte in (whole values); contains (case-insensitive substring of a string, or of a string element of an array); has (an array element equals value); has_any / has_all (some / every value of an array value is an element); exists (value true: the field is present, even null; false: it is missing); starts_with (case-sensitive string prefix)"`
	Value    any      `json:"value" jsonschema:"Value to compare with; an array for in, has_any and has_all; true or false for exists; a string for starts_with; {\"$now\": true} is the server's current time"`
}

// SortKey is one find_documents sort key.
type SortKey struct {
	Field string    `json:"field" jsonschema:"Field, dotted path (meta.author), or _id to sort by"`
	Order SortOrder `json:"order,omitempty" jsonschema:"asc (default) or desc"`
}

// Query is the find_documents query.
type Query struct {
	Filters []Filter   `json:"filters,omitempty" jsonschema:"Filters that must all match (AND); omit to match every document"`
	Any     [][]Filter `json:"any,omitempty" jsonschema:"OR groups: when set, a document must also match every filter of at least one group"`
	Fields  []string   `json:"fields,omitempty" jsonschema:"Fields or dotted paths to return (projection); _id is always returned; omit for whole documents"`
	Sort    []SortKey  `json:"sort,omitempty" jsonschema:"Sort keys in order, then _id ascending; omit to sort by _id. Sorting by other fields reads every match"`
	Limit   int        `json:"limit,omitempty" jsonschema:"Maximum number of documents to return; 0 or omitted means the server's default limit (100 unless configured). A result byte cap (1 MiB unless configured) also applies; has_more says whether more match"`
	Skip    int        `json:"skip,omitempty" jsonschema:"Number of matching documents to skip, after sorting; to page, set it to the previous result's next_skip"`
}

// MatchQuery is the query of count_documents and distinct: filters and OR
// groups only.
type MatchQuery struct {
	Filters []Filter   `json:"filters,omitempty" jsonschema:"Filters that must all match (AND); omit to match every document"`
	Any     [][]Filter `json:"any,omitempty" jsonschema:"OR groups: when set, a document must also match every filter of at least one group"`
}

type FindDocumentsInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	Query      *Query `json:"query,omitempty" jsonschema:"Filters, OR groups, projection, sort, limit, and skip; omit to match every document (returned a page at a time, see has_more)"`
	Explain    bool   `json:"explain,omitempty" jsonschema:"Also return plan: how the query ran (the index or scan used, whether it gave the sort order, documents examined and returned)"`
}

type UpdateDocumentInput struct {
	Database   string             `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string             `json:"collection" jsonschema:"Collection name"`
	ID         string             `json:"id" jsonschema:"Document _id"`
	Updates    map[string]any     `json:"updates,omitempty" jsonschema:"Top-level fields to set (replaced or added); other fields are kept; _id cannot be changed. {\"$now\": true} is the server's current time"`
	Unset      []string           `json:"unset,omitempty" jsonschema:"Top-level fields to remove (after updates); give at least one of updates, unset, inc, push, add_to_set, and pull, and name a field in only one of them"`
	Inc        map[string]float64 `json:"inc,omitempty" jsonschema:"Top-level numeric fields to add to (field to amount, negative to subtract); a missing field starts at 0; fails if the field holds a non-number"`
	Push       map[string]any     `json:"push,omitempty" jsonschema:"Append the value to the top-level array field (field to value); a missing field becomes a one-element array; fails if the field holds a non-array"`
	AddToSet   map[string]any     `json:"add_to_set,omitempty" jsonschema:"Like push, but only if no element equals the value"`
	Pull       map[string]any     `json:"pull,omitempty" jsonschema:"Remove every element equal to the value from the top-level array field; a missing field is left alone; fails if the field holds a non-array"`
	If         []Filter           `json:"if,omitempty" jsonschema:"Condition: the stored document must match all of these filters, or nothing is written (compare-and-set)"`
	Upsert     bool               `json:"upsert,omitempty" jsonschema:"Create the document as {_id: id} plus updates (then the operators: inc from 0, push and add_to_set start an array) if it does not exist"`
}

type ReplaceDocumentInput struct {
	Database   string         `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string         `json:"collection" jsonschema:"Collection name"`
	ID         string         `json:"id" jsonschema:"_id of the existing document to replace"`
	Document   map[string]any `json:"document" jsonschema:"The new document; its _id, if given, must equal id. {\"$now\": true} is the server's current time"`
	If         []Filter       `json:"if,omitempty" jsonschema:"Condition: the stored document must match all of these filters, or nothing is written"`
}

type DeleteDocumentInput struct {
	Database   string   `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string   `json:"collection" jsonschema:"Collection name"`
	ID         string   `json:"id" jsonschema:"Document _id"`
	If         []Filter `json:"if,omitempty" jsonschema:"Condition: the stored document must match all of these filters, or nothing is deleted"`
}

type GetDocumentInput struct {
	Database   string   `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string   `json:"collection" jsonschema:"Collection name"`
	ID         string   `json:"id" jsonschema:"Document _id"`
	Fields     []string `json:"fields,omitempty" jsonschema:"Fields to return (projection); _id is always returned; omit for the whole document"`
}

type InsertDocumentsInput struct {
	Database   string           `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string           `json:"collection" jsonschema:"Collection name"`
	Documents  []map[string]any `json:"documents" jsonschema:"The documents as JSON objects, inserted in order. A string _id sets a document's id; without one a UUID is generated. {\"$now\": true} is the server's current time (one time for the whole call)"`
}

type UpdateDocumentsInput struct {
	Database   string             `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string             `json:"collection" jsonschema:"Collection name"`
	Query      *MatchQuery        `json:"query,omitempty" jsonschema:"Filters and OR groups selecting the documents to update; an empty or omitted query is rejected unless all is true"`
	Updates    map[string]any     `json:"updates,omitempty" jsonschema:"Top-level fields to set in every matching document (replaced or added); _id cannot be changed. {\"$now\": true} is the server's current time"`
	Unset      []string           `json:"unset,omitempty" jsonschema:"Top-level fields to remove (after updates); give at least one of updates, unset, inc, push, add_to_set, and pull, and name a field in only one of them"`
	Inc        map[string]float64 `json:"inc,omitempty" jsonschema:"Top-level numeric fields to add to (field to amount, negative to subtract); a missing field starts at 0; fails if the field holds a non-number"`
	Push       map[string]any     `json:"push,omitempty" jsonschema:"Append the value to the top-level array field (field to value); a missing field becomes a one-element array; fails if the field holds a non-array"`
	AddToSet   map[string]any     `json:"add_to_set,omitempty" jsonschema:"Like push, but only if no element equals the value"`
	Pull       map[string]any     `json:"pull,omitempty" jsonschema:"Remove every element equal to the value from the top-level array field; a missing field is left alone; fails if the field holds a non-array"`
	Limit      int                `json:"limit,omitempty" jsonschema:"Update at most this many matching documents, the first by _id; 0 or omitted means no limit (then more matches than the batch limit fail)"`
	All        bool               `json:"all,omitempty" jsonschema:"Set to true to allow an empty query, which matches every document"`
}

type DeleteDocumentsInput struct {
	Database   string      `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string      `json:"collection" jsonschema:"Collection name"`
	Query      *MatchQuery `json:"query,omitempty" jsonschema:"Filters and OR groups selecting the documents to delete; an empty or omitted query is rejected unless all is true"`
	Limit      int         `json:"limit,omitempty" jsonschema:"Delete at most this many matching documents, the first by _id; 0 or omitted means no limit (then more matches than the batch limit fail)"`
	All        bool        `json:"all,omitempty" jsonschema:"Set to true to allow an empty query, which matches every document"`
}

type CountDocumentsInput struct {
	Database   string      `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string      `json:"collection" jsonschema:"Collection name"`
	Query      *MatchQuery `json:"query,omitempty" jsonschema:"Filters and OR groups; omit to count every document"`
}

type DistinctInput struct {
	Database   string      `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string      `json:"collection" jsonschema:"Collection name"`
	Field      string      `json:"field" jsonschema:"Field or dotted path whose distinct values to list; array values contribute each element"`
	Query      *MatchQuery `json:"query,omitempty" jsonschema:"Filters and OR groups selecting the documents; omit for every document"`
}

type DescribeCollectionInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
}

type UpdateSchemaInput struct {
	Database   string     `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string     `json:"collection" jsonschema:"Collection name"`
	Schema     *SchemaDef `json:"schema" jsonschema:"The new schema, or null to remove it; every existing document must satisfy it"`
}

type RenameCollectionInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection to rename"`
	NewName    string `json:"new_name" jsonschema:"New collection name in the same database; must not exist"`
}

type CopyCollectionInput struct {
	Database       string `json:"database,omitempty" jsonschema:"Database of the source collection; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection     string `json:"collection" jsonschema:"Source collection"`
	NewName        string `json:"new_name" jsonschema:"Name of the new collection; must not exist"`
	TargetDatabase string `json:"target_database,omitempty" jsonschema:"Existing database for the copy; defaults to the source database"`
}

type DeleteCollectionInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection to delete with all its documents and indexes"`
}

type DropIndexInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	IndexName  string `json:"index_name" jsonschema:"Name of the index to drop"`
}

type CreateIndexInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name; defaults to the session's use_database choice, then the user's default database, then the server default"`
	Collection string `json:"collection" jsonschema:"Collection name"`
	IndexName  string `json:"index_name" jsonschema:"Index name: letters, digits, _ and -, starting with a letter or digit, at most 64 characters"`
	FieldName  string `json:"field_name" jsonschema:"Field or dotted path (meta.author) to index"`
	Unique     bool   `json:"unique,omitempty" jsonschema:"Make the index unique: a write giving a value another document already has fails with a unique violation (missing fields, arrays, and objects are not constrained). Fails if existing documents share a value"`
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
	CurrentDatabase string `json:"current_database" jsonschema:"The session's use_database choice, else the user's default database, else the server default"`
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
	HasMore   bool             `json:"has_more" jsonschema:"True if more documents match after these (the limit, the default limit, or the result byte cap ended the page)"`
	NextSkip  int              `json:"next_skip,omitempty" jsonschema:"Set when has_more is true: repeat the call with query.skip set to this value for the next page"`
	Plan      *QueryPlan       `json:"plan,omitempty" jsonschema:"How the query ran; only with explain: true"`
}

// QueryPlan describes how find_documents ran a query (explain).
type QueryPlan struct {
	Driver        string `json:"driver" jsonschema:"Where the candidates came from, one of: id lookup; index; index range; or union (one index per OR group); index sort; keydir (every document in _id order); none (no document can match)"`
	Index         string `json:"index,omitempty" jsonschema:"The index the driver read (several comma-separated for an OR union); absent for id lookup, keydir, and none"`
	OrderProvided bool   `json:"order_provided" jsonschema:"True if the driver produced the documents in the requested order, so the query stopped early without sorting"`
	Examined      int    `json:"examined" jsonschema:"Candidate documents read and checked against the filters"`
	Returned      int    `json:"returned" jsonschema:"Documents returned (count)"`
}

type GetDocumentOutput struct {
	Success  bool           `json:"success" jsonschema:"Always true on success"`
	Document map[string]any `json:"document" jsonschema:"The document as a flat object with its _id (only the fields asked for, plus _id, when fields is set)"`
}

type InsertDocumentsOutput struct {
	Success  bool     `json:"success" jsonschema:"Always true on success"`
	IDs      []string `json:"ids" jsonschema:"The _id of each document (given or generated), in the order of documents"`
	Inserted int      `json:"inserted" jsonschema:"Number of documents inserted"`
}

type UpdateDocumentsOutput struct {
	Success  bool `json:"success" jsonschema:"Always true on success"`
	Matched  int  `json:"matched" jsonschema:"Number of documents that matched the query (at most limit)"`
	Modified int  `json:"modified" jsonschema:"Number of documents updated; lower than matched when a document stopped matching before the write"`
}

type DeleteDocumentsOutput struct {
	Success bool `json:"success" jsonschema:"Always true on success"`
	Matched int  `json:"matched" jsonschema:"Number of documents that matched the query (at most limit)"`
	Deleted int  `json:"deleted" jsonschema:"Number of documents deleted; lower than matched when a document stopped matching before the write"`
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
	Name   string `json:"name" jsonschema:"Index name"`
	Field  string `json:"field" jsonschema:"Indexed field or dotted path"`
	Unique bool   `json:"unique,omitempty" jsonschema:"True for a unique index (absent otherwise)"`
}

type CopyCollectionOutput struct {
	Success bool   `json:"success" jsonschema:"Always true on success"`
	Message string `json:"message" jsonschema:"Human-readable summary"`
	Copied  int    `json:"copied" jsonschema:"Number of documents copied"`
}

type ServerStatsInput struct {
	Database string `json:"database,omitempty" jsonschema:"Only this database; omit for every database you can read (the session's database choice does not apply)"`
}

type ServerStatsOutput struct {
	Success   bool            `json:"success" jsonschema:"Always true on success"`
	Databases []DatabaseStats `json:"databases" jsonschema:"The database asked for, or every database the caller can read, sorted by name"`
	Server    *ServerTotals   `json:"server,omitempty" jsonschema:"Server-wide figures; only for server admins (everyone when the server requires no tokens)"`
}

// DatabaseStats is one database of server_stats.
type DatabaseStats struct {
	Name        string            `json:"name" jsonschema:"Database name"`
	Documents   int64             `json:"documents" jsonschema:"Documents in all its collections"`
	DataBytes   int64             `json:"data_bytes" jsonschema:"Sum of the collections' data_bytes"`
	IndexBytes  int64             `json:"index_bytes" jsonschema:"Sum of the collections' index_bytes"`
	Collections []CollectionStats `json:"collections" jsonschema:"Its collections, sorted by name"`
}

// CollectionStats is one collection of server_stats.
type CollectionStats struct {
	Name       string   `json:"name" jsonschema:"Collection name"`
	Documents  int      `json:"documents" jsonschema:"Number of documents"`
	DataBytes  int64    `json:"data_bytes" jsonschema:"Bytes of records in its data segments (superseded versions until compaction; recent changes still only in the write-ahead log are not counted)"`
	IndexBytes int64    `json:"index_bytes" jsonschema:"Size of its index file (the id table and every secondary index)"`
	Indexes    []string `json:"indexes" jsonschema:"Secondary index names, sorted"`
}

// ServerTotals is the server part of server_stats.
type ServerTotals struct {
	Version       string     `json:"version" jsonschema:"Server build version"`
	UptimeSeconds int64      `json:"uptime_seconds" jsonschema:"Seconds since the server started"`
	WALBytes      int64      `json:"wal_bytes" jsonschema:"Size of the write-ahead log files"`
	Cache         CacheTotal `json:"cache" jsonschema:"The document cache"`
}

// CacheTotal describes the document cache in server_stats.
type CacheTotal struct {
	Bytes    int64  `json:"bytes" jsonschema:"Bytes the cached documents take"`
	MaxBytes int64  `json:"max_bytes" jsonschema:"Cache size limit (0: disabled)"`
	Entries  int    `json:"entries" jsonschema:"Number of cached documents"`
	Hits     uint64 `json:"hits" jsonschema:"Document reads served from the cache since start"`
	Misses   uint64 `json:"misses" jsonschema:"Document reads that went to disk since start"`
}

// toolSpec is one tool: its definition, the access level it needs, an
// example call, and how to register its handler.
type toolSpec struct {
	tool    *mcp.Tool
	access  toolAccess
	example map[string]any
	// databaseArg: the tool's input has a "database" argument (implements
	// databaseArg); see validate.
	databaseArg bool
	// admin: a user-management tool, listed only to server admins (see
	// adminOnly and filterToolsList).
	admin    bool
	register func(s *Server, srv *mcp.Server, t *mcp.Tool)
}

// schemaOptions renders Operator, FieldType, SortOrder, and AccessLevel
// as string enums.
var schemaOptions = &jsonschema.ForOptions{TypeSchemas: map[reflect.Type]*jsonschema.Schema{
	reflect.TypeFor[Operator]():    {Type: "string", Enum: toAny(Operators)},
	reflect.TypeFor[FieldType]():   {Type: "string", Enum: toAny(FieldTypes)},
	reflect.TypeFor[SortOrder]():   {Type: "string", Enum: toAny(SortOrders)},
	reflect.TypeFor[AccessLevel](): {Type: "string", Enum: toAny(AccessLevels)},
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

// Annotation sets (approach-agent-ux §3.2). Every tool is closed-world:
// it only touches this cachydb instance.
func readOnly(title string) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: new(false)}
}

func writing(title string, destructive, idempotent bool) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{
		Title:           title,
		DestructiveHint: new(destructive),
		IdempotentHint:  idempotent,
		OpenWorldHint:   new(false),
	}
}

// newSpec builds a toolSpec for a typed handler that needs access acc.
// The registered handler is wrapped: Server.authorizeCall resolves the
// principal and the database and checks acc before the handler runs, and
// in ResultTextSummary mode a successful result's text content is the
// output's one-line summary (summaryOf).
func newSpec[In, Out any](
	name, description string,
	annotations *mcp.ToolAnnotations,
	acc toolAccess,
	example map[string]any,
	handler func(s *Server) mcp.ToolHandlerFor[In, Out],
) toolSpec {
	_, hasDatabaseArg := any(new(In)).(databaseArg)
	return toolSpec{
		tool: &mcp.Tool{
			Name:         name,
			Description:  description,
			Annotations:  annotations,
			InputSchema:  schemaFor[In](),
			OutputSchema: schemaFor[Out](),
		},
		access:      acc,
		example:     example,
		databaseArg: hasDatabaseArg,
		register: func(s *Server, srv *mcp.Server, t *mcp.Tool) {
			h := handler(s)
			mcp.AddTool(srv, t, func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
				ctx, err := s.authorizeCall(ctx, req, name, acc, &in)
				if err != nil {
					var zero Out
					return nil, zero, err
				}
				res, out, err := h(ctx, req, in)
				if err == nil && res == nil && s.summaryText {
					// The SDK keeps a Content the handler set and still
					// fills StructuredContent from out (go-sdk v1.8.0
					// toolForErr), so only the text changes.
					res = &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: summaryOf(out)}}}
				}
				return res, out, err
			})
		},
	}
}

// toolSpecs returns every tool the server always registers, in the order
// they are listed in docs. The user tools (accessToolSpecs) come on top
// when the server requires tokens.
func toolSpecs() []toolSpec {
	return []toolSpec{
		newSpec("list_databases",
			"List the databases you can read, sorted by name (every database when the server requires no tokens).",
			readOnly("List databases"), accessNone, map[string]any{},
			func(s *Server) mcp.ToolHandlerFor[ListDatabasesInput, ListDatabasesOutput] {
				return s.listDatabasesTool
			}),
		newSpec("create_database",
			"Create a database. Creating a database that already exists succeeds and changes nothing.",
			writing("Create database", false, true), accessServer, map[string]any{"name": "shop"},
			func(s *Server) mcp.ToolHandlerFor[CreateDatabaseInput, MessageOutput] { return s.createDatabaseTool }),
		newSpec("delete_database",
			"Delete a database with all its collections and documents. This cannot be undone.",
			writing("Delete database", true, true), accessServer, map[string]any{"name": "shop"},
			func(s *Server) mcp.ToolHandlerFor[DeleteDatabaseInput, MessageOutput] { return s.deleteDatabaseTool }),
		newSpec("use_database",
			"Make an existing database the default for this session's later calls that omit the database argument. Other sessions are not affected.",
			writing("Use database", false, true), accessNone, map[string]any{"name": "shop"},
			func(s *Server) mcp.ToolHandlerFor[UseDatabaseInput, UseDatabaseOutput] { return s.useDatabaseTool }),
		newSpec("current_database",
			"Return the database that calls without a database argument use: this session's use_database choice, else the user's default database (when the server requires tokens), else the server default.",
			readOnly("Current database"), accessNone, map[string]any{},
			func(s *Server) mcp.ToolHandlerFor[CurrentDatabaseInput, CurrentDatabaseOutput] {
				return s.currentDatabaseTool
			}),
		newSpec("create_collection",
			"Create a collection, optionally with a schema (field types and required fields) that every insert and update must satisfy. See resource cachydb://docs/schema.",
			writing("Create collection", false, false), accessAdmin,
			map[string]any{"name": "products", "schema": map[string]any{"fields": map[string]any{
				"name": map[string]any{"type": "string", "required": true}, "price": map[string]any{"type": "number"},
			}}},
			func(s *Server) mcp.ToolHandlerFor[CreateCollectionInput, MessageOutput] {
				return s.createCollectionTool
			}),
		newSpec("list_collections",
			"List the collections of a database, sorted by name.",
			readOnly("List collections"), accessRead, map[string]any{"database": "shop"},
			func(s *Server) mcp.ToolHandlerFor[ListCollectionsInput, ListCollectionsOutput] {
				return s.listCollectionsTool
			}),
		newSpec("insert_document",
			"Insert a JSON document into a collection and return its id. A string _id in the document sets the id; otherwise a UUID is generated. Fails if the id exists or the schema rejects the document.",
			writing("Insert document", false, false), accessWrite,
			map[string]any{"collection": "products", "document": map[string]any{"_id": "p1", "name": "Pen", "price": 2.5}},
			func(s *Server) mcp.ToolHandlerFor[InsertDocumentInput, InsertDocumentOutput] {
				return s.insertDocumentTool
			}),
		newSpec("insert_documents",
			"Insert many JSON documents into a collection in one atomic call: all are inserted, or none if any fails (an existing or repeated id, or a schema error; the error names the item as documents[i]). A string _id and {\"$now\": true} work as in insert_document, and the ids come back in order. At most 10,000 documents and 32 MiB per call; over HTTP a request is limited to 4 MiB, so send larger loads as several calls.",
			writing("Insert documents", false, false), accessWrite,
			map[string]any{"collection": "products", "documents": []any{
				map[string]any{"_id": "p2", "name": "Ink", "price": 4},
				map[string]any{"name": "Pad", "price": 6},
			}},
			func(s *Server) mcp.ToolHandlerFor[InsertDocumentsInput, InsertDocumentsOutput] {
				return s.insertDocumentsTool
			}),
		newSpec("get_document",
			"Return one document by _id as a flat object with its _id, optionally only the listed fields. Fails with not found if no document has that id.",
			readOnly("Get document"), accessRead,
			map[string]any{"collection": "products", "id": "p1", "fields": []any{"name", "price"}},
			func(s *Server) mcp.ToolHandlerFor[GetDocumentInput, GetDocumentOutput] { return s.getDocumentTool }),
		newSpec("find_documents",
			"Find the documents of a collection matching every filter in query.filters (and, if given, at least one group of query.any), with optional sort, projection (fields), skip, and limit; sorted by _id by default. Returns at most the default limit (100) and 1 MiB unless configured; when has_more is true, call again with query.skip = next_skip. Operators include contains (case-insensitive substring), starts_with, has / has_any / has_all (array elements), and exists. Comparison is type-aware and a document missing the field matches no operator except exists (not even ne). explain: true adds the query plan. See resource cachydb://docs/query.",
			readOnly("Find documents"), accessRead,
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
			readOnly("Count documents"), accessRead,
			map[string]any{"collection": "products", "query": map[string]any{
				"filters": []any{map[string]any{"field": "price", "operator": "lt", "value": 10}},
			}},
			func(s *Server) mcp.ToolHandlerFor[CountDocumentsInput, CountDocumentsOutput] {
				return s.countDocumentsTool
			}),
		newSpec("distinct",
			"List the distinct values of a field among the documents matching query (all by default). Array values contribute each element, so this lists tags. Values are sorted.",
			readOnly("Distinct values"), accessRead,
			map[string]any{"collection": "products", "field": "name"},
			func(s *Server) mcp.ToolHandlerFor[DistinctInput, DistinctOutput] { return s.distinctTool }),
		newSpec("update_document",
			"Merge top-level fields into a document (updates), remove fields (unset), and change fields in place: inc adds to a number, push / add_to_set append to an array, pull removes equal elements. Other fields are kept and the result must satisfy the schema. With if, the write happens only if the stored document matches those filters (compare-and-set; otherwise a condition-failed error). With upsert, a missing document is created.",
			writing("Update document", false, false), accessWrite,
			map[string]any{"collection": "products", "id": "p1", "updates": map[string]any{"price": 3},
				"if": []any{map[string]any{"field": "price", "operator": "eq", "value": 2.5}}},
			func(s *Server) mcp.ToolHandlerFor[UpdateDocumentInput, UpdateDocumentOutput] {
				return s.updateDocumentTool
			}),
		newSpec("update_documents",
			"Set fields (updates), remove fields (unset), and apply inc, push, add_to_set, and pull in every document matching query.filters and query.any, atomically: all matching documents change or none do. An empty query is rejected unless all is true, and more than 10,000 matches without a limit fail with the match count and change nothing. With limit only the first matches by _id change; repeating the call makes progress only if the update makes documents stop matching the query.",
			writing("Update documents", false, false), accessWrite,
			map[string]any{"collection": "products", "query": map[string]any{
				"filters": []any{map[string]any{"field": "price", "operator": "gt", "value": 5}},
			}, "updates": map[string]any{"sale": true}},
			func(s *Server) mcp.ToolHandlerFor[UpdateDocumentsInput, UpdateDocumentsOutput] {
				return s.updateDocumentsTool
			}),
		newSpec("replace_document",
			"Replace a whole existing document atomically (fields not in the new document are removed). The result must satisfy the schema; with if, the stored document must match those filters.",
			writing("Replace document", false, true), accessWrite,
			map[string]any{"collection": "products", "id": "p1", "document": map[string]any{"name": "Pen", "price": 3}},
			func(s *Server) mcp.ToolHandlerFor[ReplaceDocumentInput, MessageOutput] {
				return s.replaceDocumentTool
			}),
		newSpec("delete_document",
			"Delete a document by _id; with if, only if it matches those filters. This cannot be undone.",
			writing("Delete document", true, true), accessWrite,
			map[string]any{"collection": "products", "id": "p1"},
			func(s *Server) mcp.ToolHandlerFor[DeleteDocumentInput, MessageOutput] { return s.deleteDocumentTool }),
		newSpec("delete_documents",
			"Delete every document matching query.filters and query.any, atomically: all of them or none. An empty query is rejected unless all is true, and more than 10,000 matches without a limit fail with the match count and delete nothing (pass a limit and repeat the call). This cannot be undone.",
			writing("Delete documents", true, true), accessWrite,
			map[string]any{"collection": "products", "query": map[string]any{
				"filters": []any{map[string]any{"field": "price", "operator": "gt", "value": 5}},
			}, "limit": 100},
			func(s *Server) mcp.ToolHandlerFor[DeleteDocumentsInput, DeleteDocumentsOutput] {
				return s.deleteDocumentsTool
			}),
		newSpec("describe_collection",
			"Describe a collection: its schema (or null), its indexes, and its document count.",
			readOnly("Describe collection"), accessRead,
			map[string]any{"collection": "products"},
			func(s *Server) mcp.ToolHandlerFor[DescribeCollectionInput, DescribeCollectionOutput] {
				return s.describeCollectionTool
			}),
		newSpec("update_schema",
			"Replace a collection's schema, or remove it with null. Every existing document is checked first; if any fails, nothing changes and the error lists the failing ids. Writes to the database wait meanwhile.",
			writing("Update schema", false, true), accessAdmin,
			map[string]any{"collection": "products", "schema": map[string]any{"fields": map[string]any{
				"name": map[string]any{"type": "string", "required": true}, "price": map[string]any{"type": "number"},
			}}},
			func(s *Server) mcp.ToolHandlerFor[UpdateSchemaInput, MessageOutput] { return s.updateSchemaTool }),
		newSpec("rename_collection",
			"Rename a collection within its database, keeping its documents, schema, and indexes. The new name must not exist.",
			writing("Rename collection", false, false), accessAdmin,
			map[string]any{"collection": "products_backup", "new_name": "products_old"},
			func(s *Server) mcp.ToolHandlerFor[RenameCollectionInput, MessageOutput] {
				return s.renameCollectionTool
			}),
		newSpec("copy_collection",
			"Copy a collection (schema, indexes, and documents with the same ids) to a new collection, in the same or another database. Not atomic: after an error the copy may be partial; delete it and retry.",
			// read on the source; admin on the target (CopyCollectionInput.extraAccess).
			writing("Copy collection", false, false), accessRead,
			map[string]any{"collection": "products", "new_name": "products_backup"},
			func(s *Server) mcp.ToolHandlerFor[CopyCollectionInput, CopyCollectionOutput] {
				return s.copyCollectionTool
			}),
		newSpec("delete_collection",
			"Delete a collection with all its documents and indexes. This cannot be undone.",
			writing("Delete collection", true, true), accessAdmin,
			map[string]any{"collection": "products_old"},
			func(s *Server) mcp.ToolHandlerFor[DeleteCollectionInput, MessageOutput] {
				return s.deleteCollectionTool
			}),
		newSpec("create_index",
			"Create an index on a field or dotted path so filters on it (eq, in, ranges, starts_with, exists, and has / has_any / has_all on array elements) and sorts by it avoid a full scan. With unique: true, two documents cannot share a value (a write that would fails with a unique violation). Writes to the database wait while the index builds.",
			writing("Create index", false, false), accessAdmin,
			map[string]any{"collection": "products", "index_name": "by_name", "field_name": "name"},
			func(s *Server) mcp.ToolHandlerFor[CreateIndexInput, MessageOutput] { return s.createIndexTool }),
		newSpec("drop_index",
			"Drop a secondary index; queries on its field scan again.",
			writing("Drop index", true, true), accessAdmin,
			map[string]any{"collection": "products", "index_name": "by_name"},
			func(s *Server) mcp.ToolHandlerFor[DropIndexInput, MessageOutput] { return s.dropIndexTool }),
		newSpec("server_stats",
			"Report, for one database or every database you can read, each collection's document count, data bytes, index bytes, and index names, with per-database totals. Server admins (everyone when the server requires no tokens) also get the write-ahead log size, the document cache's size, hits, and misses, the version, and the uptime.",
			readOnly("Server statistics"), accessRead,
			map[string]any{"database": "shop"},
			func(s *Server) mcp.ToolHandlerFor[ServerStatsInput, ServerStatsOutput] { return s.serverStatsTool }),
	}
}

// ToolDefinitions returns the definition of every MCP tool the server
// always registers: name, description, annotations, and input and output
// JSON Schemas. It needs no engine. The returned tools are fresh copies.
// The user tools, registered only when the server requires tokens, are
// documented by AccessToolDocs.
func ToolDefinitions() []*mcp.Tool {
	specs := toolSpecs()
	out := make([]*mcp.Tool, len(specs))
	for i, sp := range specs {
		out[i] = sp.tool
	}
	return out
}

// registerTools registers every tool of toolSpecs on srv and, when the
// server requires tokens, the user tools of accessToolSpecs, remembering
// which of them are admin tools. It registers nothing and fails if any
// tool lacks a valid access level (see toolSpec.validate).
func (s *Server) registerTools(srv *mcp.Server) error {
	specs := toolSpecs()
	access := accessToolSpecs()
	for _, sp := range slices.Concat(specs, access) {
		if err := sp.validate(); err != nil {
			return err
		}
	}
	if s.tokensRequired {
		specs = append(specs, access...)
	}
	s.adminTools = make(map[string]bool)
	for _, sp := range specs {
		if sp.admin {
			s.adminTools[sp.tool.Name] = true
		}
		sp.register(s, srv, sp.tool)
	}
	return nil
}
