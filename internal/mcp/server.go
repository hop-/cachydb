package mcpserver

import (
	"context"
	"fmt"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server represents the MCP server state
type Server struct {
	dbManager     *db.DatabaseManager
	storage       *db.StorageManager
	server        *mcp.Server
	defaultDBName string
}

// NewServer creates a new MCP server
func NewServer(defaultDBName, rootDir string) (*Server, error) {
	storage, err := db.NewStorageManager(rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	// Load all existing databases (this will also replay WAL)
	dbManager, err := storage.LoadAllDatabases()
	if err != nil {
		return nil, fmt.Errorf("failed to load databases: %w", err)
	}

	// Start background storage syncer
	storage.StartBackgroundSync(dbManager)

	// Ensure default database exists
	if dbManager.GetDatabase(defaultDBName) == nil {
		defaultDB := dbManager.CreateDatabase(defaultDBName)
		if err := storage.LogCreateDatabase(defaultDB.Name); err != nil {
			return nil, fmt.Errorf("failed to log create database: %w", err)
		}
	}

	s := &Server{
		dbManager:     dbManager,
		storage:       storage,
		defaultDBName: defaultDBName,
	}

	// Create MCP server with implementation info
	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "cachydb",
		Version: "1.0.0",
	}, nil)

	// Register all tools
	s.registerTools(mcpServer)

	s.server = mcpServer
	return s, nil
}

// Start starts the MCP server using stdio transport
func (s *Server) Start(ctx context.Context) error {
	return s.server.Run(ctx, &mcp.StdioTransport{})
}

// registerTools registers all MCP tools
func (s *Server) registerTools(server *mcp.Server) {
	// Database management tools
	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_database",
		Description: "Create a new database",
	}, s.createDatabaseTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_databases",
		Description: "List all databases",
	}, s.listDatabasesTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "delete_database",
		Description: "Delete a database",
	}, s.deleteDatabaseTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "use_database",
		Description: "Switch default database for subsequent operations",
	}, s.useDatabaseTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "current_database",
		Description: "Get the current default database name",
	}, s.currentDatabaseTool)

	// Collection management tools
	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_collection",
		Description: "Create a new collection with optional schema",
	}, s.createCollectionTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_collections",
		Description: "List all collections in a database",
	}, s.listCollectionsTool)

	// Document management tools
	mcp.AddTool(server, &mcp.Tool{
		Name:        "insert_document",
		Description: "Insert a document into a collection",
	}, s.insertDocumentTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "find_documents",
		Description: "Find documents in a collection",
	}, s.findDocumentsTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "update_document",
		Description: "Update a document by ID",
	}, s.updateDocumentTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "delete_document",
		Description: "Delete a document by ID",
	}, s.deleteDocumentTool)

	// Index management tools
	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_index",
		Description: "Create an index on a collection field",
	}, s.createIndexTool)
}

// Tool input/output types

// Database management inputs
type CreateDatabaseInput struct {
	Name string `json:"name" jsonschema:"Name of the database"`
}

type ListDatabasesInput struct{}

type DeleteDatabaseInput struct {
	Name string `json:"name" jsonschema:"Name of the database to delete"`
}

type UseDatabaseInput struct {
	Name string `json:"name" jsonschema:"Name of the database to use as default"`
}

type CurrentDatabaseInput struct{}

// Collection management inputs
type CreateCollectionInput struct {
	Database string                 `json:"database,omitempty" jsonschema:"Database name (optional, defaults to configured database)"`
	Name     string                 `json:"name" jsonschema:"Name of the collection"`
	Schema   map[string]interface{} `json:"schema,omitempty" jsonschema:"Optional schema definition with fields"`
}

type InsertDocumentInput struct {
	Database   string                 `json:"database,omitempty" jsonschema:"Database name (optional, defaults to configured database)"`
	Collection string                 `json:"collection" jsonschema:"Name of the collection"`
	Document   map[string]interface{} `json:"document" jsonschema:"Document data to insert"`
}

type FindDocumentsInput struct {
	Database   string                 `json:"database,omitempty" jsonschema:"Database name (optional, defaults to configured database)"`
	Collection string                 `json:"collection" jsonschema:"Name of the collection"`
	Query      map[string]interface{} `json:"query,omitempty" jsonschema:"Query filters, limit, and skip"`
}

type UpdateDocumentInput struct {
	Database   string                 `json:"database,omitempty" jsonschema:"Database name (optional, defaults to configured database)"`
	Collection string                 `json:"collection" jsonschema:"Name of the collection"`
	ID         string                 `json:"id" jsonschema:"Document ID"`
	Updates    map[string]interface{} `json:"updates" jsonschema:"Fields to update"`
}

type DeleteDocumentInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name (optional, defaults to configured database)"`
	Collection string `json:"collection" jsonschema:"Name of the collection"`
	ID         string `json:"id" jsonschema:"Document ID"`
}

type CreateIndexInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database name (optional, defaults to configured database)"`
	Collection string `json:"collection" jsonschema:"Name of the collection"`
	IndexName  string `json:"index_name" jsonschema:"Name for the index"`
	FieldName  string `json:"field_name" jsonschema:"Field to index"`
}

type ListCollectionsInput struct {
	Database string `json:"database,omitempty" jsonschema:"Database name (optional, defaults to configured database)"`
}

// Helper methods

// getDatabase retrieves the database by name, using default if not specified
func (s *Server) getDatabase(dbName string) (*db.Database, error) {
	if dbName == "" {
		dbName = s.defaultDBName
	}

	database := s.dbManager.GetDatabase(dbName)
	if database == nil {
		return nil, fmt.Errorf("database '%s' not found", dbName)
	}

	return database, nil
}

// Tool handlers

// Database management handlers
func (s *Server) createDatabaseTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input CreateDatabaseInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	s.dbManager.CreateDatabase(input.Name)

	// Log to WAL (sync) - storage save happens async in background
	if err := s.storage.LogCreateDatabase(input.Name); err != nil {
		return nil, nil, fmt.Errorf("failed to log create database: %w", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Database '%s' created successfully", input.Name),
	}, nil
}

func (s *Server) listDatabasesTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input ListDatabasesInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	databases := s.dbManager.ListDatabases()

	return nil, map[string]interface{}{
		"success":   true,
		"databases": databases,
	}, nil
}

func (s *Server) deleteDatabaseTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input DeleteDatabaseInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	if !s.dbManager.DeleteDatabase(input.Name) {
		return nil, nil, fmt.Errorf("database '%s' not found", input.Name)
	}

	// Log to WAL (sync)
	if err := s.storage.LogDeleteDatabase(input.Name); err != nil {
		return nil, nil, fmt.Errorf("failed to log delete database: %w", err)
	}

	// Delete database files immediately (this is a destructive operation)
	if err := s.storage.DeleteDatabase(input.Name); err != nil {
		return nil, nil, fmt.Errorf("failed to delete database files: %w", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Database '%s' deleted successfully", input.Name),
	}, nil
}

func (s *Server) useDatabaseTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input UseDatabaseInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	// Check if database exists
	database := s.dbManager.GetDatabase(input.Name)
	if database == nil {
		return nil, nil, fmt.Errorf("database '%s' not found", input.Name)
	}

	// Update default database
	s.defaultDBName = input.Name

	return nil, map[string]interface{}{
		"success":          true,
		"message":          fmt.Sprintf("Now using database '%s' as default", input.Name),
		"current_database": input.Name,
	}, nil
}

func (s *Server) currentDatabaseTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input CurrentDatabaseInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	return nil, map[string]interface{}{
		"success":          true,
		"current_database": s.defaultDBName,
	}, nil
}

// Collection management handlers
func (s *Server) createCollectionTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input CreateCollectionInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.getDatabase(input.Database)
	if err != nil {
		return nil, nil, err
	}

	var schema *db.Schema
	if input.Schema != nil {
		schema = &db.Schema{
			Fields: make(map[string]db.Field),
		}
		if fields, ok := input.Schema["fields"].(map[string]interface{}); ok {
			for fieldName, fieldData := range fields {
				if fieldMap, ok := fieldData.(map[string]interface{}); ok {
					field := db.Field{}
					if t, ok := fieldMap["type"].(string); ok {
						field.Type = db.FieldType(t)
					}
					if r, ok := fieldMap["required"].(bool); ok {
						field.Required = r
					}
					schema.Fields[fieldName] = field
				}
			}
		}
	}

	if err := database.CreateCollection(input.Name, schema); err != nil {
		return nil, nil, err
	}

	// Log to WAL (sync) - storage save happens async in background
	if err := s.storage.LogCreateCollection(database.Name, input.Name, schema); err != nil {
		return nil, nil, fmt.Errorf("failed to log create collection: %w", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Collection '%s' created in database '%s'", input.Name, database.Name),
	}, nil
}

func (s *Server) listCollectionsTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input ListCollectionsInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.getDatabase(input.Database)
	if err != nil {
		return nil, nil, err
	}

	collections := database.ListCollections()

	return nil, map[string]interface{}{
		"success":     true,
		"collections": collections,
		"database":    database.Name,
	}, nil
}

// Document management handlers
func (s *Server) insertDocumentTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input InsertDocumentInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.getDatabase(input.Database)
	if err != nil {
		return nil, nil, err
	}

	coll, err := database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	doc := &db.Document{
		Data: input.Document,
	}
	if id, ok := input.Document["_id"].(string); ok {
		doc.ID = id
		delete(input.Document, "_id")
	}

	if err := coll.Insert(doc); err != nil {
		return nil, nil, err
	}

	// Log to WAL (sync) - storage save happens async in background
	if err := s.storage.LogInsert(database.Name, input.Collection, doc); err != nil {
		return nil, nil, fmt.Errorf("failed to log insert: %w", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"id":      doc.ID,
		"message": fmt.Sprintf("Document inserted with ID: %s", doc.ID),
	}, nil
}

func (s *Server) findDocumentsTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input FindDocumentsInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.getDatabase(input.Database)
	if err != nil {
		return nil, nil, err
	}

	coll, err := database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	query := &db.Query{}
	if input.Query != nil {
		if filters, ok := input.Query["filters"].([]interface{}); ok {
			for _, f := range filters {
				if filterMap, ok := f.(map[string]interface{}); ok {
					filter := db.QueryFilter{}
					if field, ok := filterMap["field"].(string); ok {
						filter.Field = field
					}
					if op, ok := filterMap["operator"].(string); ok {
						filter.Operator = op
					}
					if val, ok := filterMap["value"]; ok {
						filter.Value = val
					}
					query.Filters = append(query.Filters, filter)
				}
			}
		}
		if limit, ok := input.Query["limit"].(float64); ok {
			query.Limit = int(limit)
		}
		if skip, ok := input.Query["skip"].(float64); ok {
			query.Skip = int(skip)
		}
	}

	docs, err := coll.Find(query)
	if err != nil {
		return nil, nil, err
	}

	// Convert documents to JSON for output
	docsJSON := make([]interface{}, len(docs))
	for i, doc := range docs {
		docMap := make(map[string]interface{})
		docMap["_id"] = doc.ID
		for k, v := range doc.Data {
			docMap[k] = v
		}
		docsJSON[i] = docMap
	}

	return nil, map[string]interface{}{
		"success":   true,
		"count":     len(docs),
		"documents": docsJSON,
	}, nil
}

func (s *Server) updateDocumentTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input UpdateDocumentInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.getDatabase(input.Database)
	if err != nil {
		return nil, nil, err
	}

	coll, err := database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	if err := coll.Update(input.ID, input.Updates); err != nil {
		return nil, nil, err
	}

	// Get updated document for WAL
	updatedDoc, err := coll.FindByID(input.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get updated document: %w", err)
	}

	// Log to WAL (sync) - storage save happens async in background
	if err := s.storage.LogUpdate(database.Name, input.Collection, updatedDoc); err != nil {
		return nil, nil, fmt.Errorf("failed to log update: %w", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Document %s updated", input.ID),
	}, nil
}

func (s *Server) deleteDocumentTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input DeleteDocumentInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.getDatabase(input.Database)
	if err != nil {
		return nil, nil, err
	}

	coll, err := database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	if err := coll.Delete(input.ID); err != nil {
		return nil, nil, err
	}

	// Log to WAL (sync) - storage save happens async in background
	if err := s.storage.LogDelete(database.Name, input.Collection, input.ID); err != nil {
		return nil, nil, fmt.Errorf("failed to log delete: %w", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Document %s deleted", input.ID),
	}, nil
}

func (s *Server) createIndexTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input CreateIndexInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.getDatabase(input.Database)
	if err != nil {
		return nil, nil, err
	}

	coll, err := database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	if err := coll.CreateIndex(input.IndexName, input.FieldName); err != nil {
		return nil, nil, err
	}

	// Log to WAL (sync) - storage save happens async in background
	if err := s.storage.LogCreateIndex(database.Name, input.Collection, input.IndexName, input.FieldName); err != nil {
		return nil, nil, fmt.Errorf("failed to log create index: %w", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Index '%s' created on field '%s'", input.IndexName, input.FieldName),
	}, nil
}
