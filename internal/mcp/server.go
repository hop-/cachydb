package mcpserver

import (
	"context"
	"fmt"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server represents the MCP server state
type Server struct {
	database *db.Database
	storage  *db.StorageManager
	server   *mcp.Server
}

// NewServer creates a new MCP server
func NewServer(dbName, rootDir string) (*Server, error) {
	storage := db.NewStorageManager(rootDir)

	var database *db.Database
	if storage.DatabaseExists(dbName) {
		loadedDB, err := storage.LoadDatabase(dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to load database: %w", err)
		}
		database = loadedDB
	} else {
		database = db.NewDatabase(dbName)
		if err := storage.SaveDatabase(database); err != nil {
			return nil, fmt.Errorf("failed to create database: %w", err)
		}
	}

	s := &Server{
		database: database,
		storage:  storage,
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
	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_collection",
		Description: "Create a new collection with optional schema",
	}, s.createCollectionTool)

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

	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_index",
		Description: "Create an index on a collection field",
	}, s.createIndexTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_collections",
		Description: "List all collections in the database",
	}, s.listCollectionsTool)
}

// Tool input/output types
type CreateCollectionInput struct {
	Name   string                 `json:"name" jsonschema:"Name of the collection"`
	Schema map[string]interface{} `json:"schema,omitempty" jsonschema:"Optional schema definition with fields"`
}

type InsertDocumentInput struct {
	Collection string                 `json:"collection" jsonschema:"Name of the collection"`
	Document   map[string]interface{} `json:"document" jsonschema:"Document data to insert"`
}

type FindDocumentsInput struct {
	Collection string                 `json:"collection" jsonschema:"Name of the collection"`
	Query      map[string]interface{} `json:"query,omitempty" jsonschema:"Query filters, limit, and skip"`
}

type UpdateDocumentInput struct {
	Collection string                 `json:"collection" jsonschema:"Name of the collection"`
	ID         string                 `json:"id" jsonschema:"Document ID"`
	Updates    map[string]interface{} `json:"updates" jsonschema:"Fields to update"`
}

type DeleteDocumentInput struct {
	Collection string `json:"collection" jsonschema:"Name of the collection"`
	ID         string `json:"id" jsonschema:"Document ID"`
}

type CreateIndexInput struct {
	Collection string `json:"collection" jsonschema:"Name of the collection"`
	IndexName  string `json:"index_name" jsonschema:"Name for the index"`
	FieldName  string `json:"field_name" jsonschema:"Field to index"`
}

type ListCollectionsInput struct{}

// Tool handlers
func (s *Server) createCollectionTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input CreateCollectionInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
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

	if err := s.database.CreateCollection(input.Name, schema); err != nil {
		return nil, nil, err
	}

	if err := s.storage.SaveDatabase(s.database); err != nil {
		return nil, nil, err
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Collection '%s' created", input.Name),
	}, nil
}

func (s *Server) insertDocumentTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input InsertDocumentInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	coll, err := s.database.GetCollection(input.Collection)
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

	if err := s.storage.SaveCollection(s.database.Name, coll); err != nil {
		return nil, nil, err
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
	coll, err := s.database.GetCollection(input.Collection)
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
	coll, err := s.database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	if err := coll.Update(input.ID, input.Updates); err != nil {
		return nil, nil, err
	}

	if err := s.storage.SaveCollection(s.database.Name, coll); err != nil {
		return nil, nil, err
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
	coll, err := s.database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	if err := coll.Delete(input.ID); err != nil {
		return nil, nil, err
	}

	if err := s.storage.SaveCollection(s.database.Name, coll); err != nil {
		return nil, nil, err
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
	coll, err := s.database.GetCollection(input.Collection)
	if err != nil {
		return nil, nil, err
	}

	if err := coll.CreateIndex(input.IndexName, input.FieldName); err != nil {
		return nil, nil, err
	}

	if err := s.storage.SaveCollection(s.database.Name, coll); err != nil {
		return nil, nil, err
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Index '%s' created on field '%s'", input.IndexName, input.FieldName),
	}, nil
}

func (s *Server) listCollectionsTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input ListCollectionsInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	collections := s.database.ListCollections()
	return nil, map[string]interface{}{
		"success":     true,
		"collections": collections,
	}, nil
}
