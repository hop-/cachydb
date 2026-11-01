package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// httpSessionTimeout is how long an idle Streamable HTTP session is kept
// before the SDK closes it.
const httpSessionTimeout = 30 * time.Minute

// httpShutdownTimeout bounds the graceful HTTP shutdown when Start's
// context is cancelled.
const httpShutdownTimeout = 5 * time.Second

// Server is the CachyDB MCP server. Its tool handlers only call methods of
// the *db.Engine it was given; the engine is owned by the caller, which
// must close it after Close returns.
type Server struct {
	engine        *db.Engine
	server        *mcp.Server
	defaultDBName string
	transport     string
	httpAddr      string
	stderr        io.Writer     // startup/shutdown messages; never stdout
	stdio         mcp.Transport // the stdio transport; tests replace it

	// sessions maps *mcp.ServerSession to *sessionState: the database the
	// session selected with use_database.
	sessions sync.Map

	mu      sync.Mutex // guards closed and the waiters Add/Wait ordering
	closed  bool
	waiters sync.WaitGroup // one per tracked session, see trackSession
}

// sessionState is the per-session state of one MCP client.
type sessionState struct {
	mu       sync.Mutex
	database string // "" = no use_database call yet
}

func (st *sessionState) get() string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.database
}

func (st *sessionState) set(name string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.database = name
}

// NewServer creates an MCP server on the opened engine. It creates the
// default database if it does not exist. transport is "http" or anything
// else for stdio; httpAddr is the listen address for HTTP.
func NewServer(ctx context.Context, engine *db.Engine, defaultDBName, transport, httpAddr string) (*Server, error) {
	if engine == nil {
		return nil, errors.New("creating MCP server: engine is nil")
	}
	if _, err := engine.CreateDatabase(ctx, defaultDBName); err != nil {
		return nil, fmt.Errorf("creating default database %q: %w", defaultDBName, err)
	}

	s := &Server{
		engine:        engine,
		defaultDBName: defaultDBName,
		transport:     transport,
		httpAddr:      httpAddr,
		stderr:        os.Stderr,
		stdio:         &mcp.StdioTransport{},
	}

	s.server = mcp.NewServer(&mcp.Implementation{
		Name:    "cachydb",
		Version: "1.0.0",
	}, &mcp.ServerOptions{
		InitializedHandler: func(_ context.Context, req *mcp.InitializedRequest) {
			s.trackSession(req.Session)
		},
	})
	// Clients on protocol 2026-07-28 and later skip the initialize
	// handshake (server/discover), so InitializedHandler never runs for
	// them: also track the session on every request it sends.
	s.server.AddReceivingMiddleware(s.trackSessionMiddleware)
	s.registerTools(s.server)
	return s, nil
}

// trackSessionMiddleware registers the session of every incoming request
// (see trackSession) before handling it.
func (s *Server) trackSessionMiddleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		if ss, ok := req.GetSession().(*mcp.ServerSession); ok && ss != nil {
			s.trackSession(ss)
		}
		return next(ctx, method, req)
	}
}

// Start runs the configured transport until ctx is cancelled or, for
// stdio, the client disconnects. Cancellation is a clean stop and returns
// nil. Call Close after Start returns.
func (s *Server) Start(ctx context.Context) error {
	switch s.transport {
	case "http":
		return s.startHTTP(ctx)
	default:
		return s.startStdio(ctx)
	}
}

// Close closes every tracked MCP session and waits for their watcher
// goroutines to exit, or for ctx to be done. It does not close the
// engine. Close is idempotent.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	var errs []error
	if err := s.closeSessions(); err != nil {
		errs = append(errs, err)
	}

	done := make(chan struct{})
	go func() {
		s.waiters.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, fmt.Errorf("waiting for MCP sessions to close: %w", ctx.Err()))
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing MCP server: %w", errors.Join(errs...))
	}
	return nil
}

// closeSessions closes every tracked session, which makes its watcher
// goroutine (see trackSession) return.
func (s *Server) closeSessions() error {
	var errs []error
	s.sessions.Range(func(k, _ any) bool {
		if err := k.(*mcp.ServerSession).Close(); err != nil {
			errs = append(errs, err)
		}
		return true
	})
	return errors.Join(errs...)
}

// trackSession registers ss and returns its state. The first registration
// starts a goroutine that removes the session once it ends; Close ends
// every tracked session, so that goroutine always exits.
func (s *Server) trackSession(ss *mcp.ServerSession) *sessionState {
	if v, ok := s.sessions.Load(ss); ok {
		return v.(*sessionState)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		// Not stored, so nothing leaks; the session is being shut down.
		return &sessionState{}
	}
	v, loaded := s.sessions.LoadOrStore(ss, &sessionState{})
	if !loaded {
		s.waiters.Add(1)
		go func() {
			defer s.waiters.Done()
			// The wait error only says how the session ended; there is
			// nothing to do with it beyond forgetting the session.
			_ = ss.Wait()
			s.sessions.Delete(ss)
		}()
	}
	return v.(*sessionState)
}

// sessionDatabase returns the database ss selected, or "".
func (s *Server) sessionDatabase(ss *mcp.ServerSession) string {
	if ss == nil {
		return ""
	}
	if v, ok := s.sessions.Load(ss); ok {
		return v.(*sessionState).get()
	}
	return ""
}

// errServerClosing matches the SDK's internal "server is closing" JSON-RPC
// error (code -32004); the SDK's WireError.Is compares codes.
var errServerClosing = &jsonrpc.Error{Code: -32004}

// isEndOfInput reports whether err only says that the client closed its
// end of the stdio connection, which is a normal way for a session to end.
func isEndOfInput(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, mcp.ErrConnectionClosed) ||
		errors.Is(err, errServerClosing)
}

// startStdio serves one session over stdin/stdout. The session ends when
// ctx is cancelled or the client closes stdin; both are a clean shutdown
// and return nil, so the engine is closed and the process exits 0.
//
// When stdin closes while requests are still in flight, the SDK (v1.8.0)
// cancels them and their responses are not written: a client must keep
// stdin open until it has read the responses it needs.
func (s *Server) startStdio(ctx context.Context) error {
	err := s.server.Run(ctx, s.stdio)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		return nil
	case isEndOfInput(err):
		return nil
	default:
		return fmt.Errorf("stdio transport: %w", err)
	}
}

// httpHandler returns the Streamable HTTP handler serving s.server.
func (s *Server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return s.server
	}, &mcp.StreamableHTTPOptions{SessionTimeout: httpSessionTimeout}))
	return mux
}

// startHTTP serves the Streamable HTTP transport (MCP spec 2025-03-26+) at
// /mcp until ctx is cancelled.
func (s *Server) startHTTP(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.httpAddr, err)
	}
	httpServer := &http.Server{
		Handler:           s.httpHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	stop := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
		case <-stop:
			shutdownDone <- nil
			return
		}
		// End the MCP sessions first so their long-lived streams do not
		// hold the graceful shutdown until its timeout.
		if err := s.closeSessions(); err != nil {
			fmt.Fprintf(s.stderr, "CachyDB MCP HTTP server: closing sessions: %v\n", err)
		}
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), httpShutdownTimeout)
		defer cancel()
		err := httpServer.Shutdown(shutdownCtx)
		if err != nil {
			// Graceful shutdown timed out: drop the remaining connections.
			err = errors.Join(err, httpServer.Close())
		}
		shutdownDone <- err
	}()

	addr := ln.Addr().String()
	fmt.Fprintf(s.stderr, "CachyDB MCP server listening on http://%s/mcp (Streamable HTTP transport)\n", addr)

	serveErr := httpServer.Serve(ln)
	close(stop)
	shutdownErr := <-shutdownDone
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server error: %w", serveErr)
	}
	if shutdownErr != nil {
		// The server is stopped either way (forced close); report it on
		// stderr without failing a requested shutdown.
		fmt.Fprintf(s.stderr, "CachyDB MCP HTTP server forced shutdown: %v\n", shutdownErr)
		return nil
	}
	fmt.Fprintln(s.stderr, "CachyDB MCP HTTP server stopped")
	return nil
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
		Description: "Switch default database for subsequent operations in this session",
	}, s.useDatabaseTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "current_database",
		Description: "Get the current default database name for this session",
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

// toolError turns an engine error into the error a tool returns; the SDK
// reports it to the client as a tool error (IsError) carrying the message.
func toolError(action string, err error) error {
	switch {
	case errors.Is(err, db.ErrNotFound),
		errors.Is(err, db.ErrExists),
		errors.Is(err, db.ErrInvalidName),
		errors.Is(err, db.ErrSchemaValidation):
		// Client errors: the engine message names the object and reason.
		return fmt.Errorf("%s: %w", action, err)
	case errors.Is(err, db.ErrClosed):
		return fmt.Errorf("%s: the database engine is shut down: %w", action, err)
	case errors.Is(err, db.ErrWALFailed), errors.Is(err, db.ErrWALCorrupt):
		return fmt.Errorf("%s: write-ahead log failure, restart the server: %w", action, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%s: request cancelled: %w", action, err)
	default:
		return fmt.Errorf("%s: %w", action, err)
	}
}

// databaseName resolves the database for a request: the explicit input,
// then the session's use_database choice, then the server default.
func (s *Server) databaseName(ss *mcp.ServerSession, input string) string {
	if input != "" {
		return input
	}
	if name := s.sessionDatabase(ss); name != "" {
		return name
	}
	return s.defaultDBName
}

// getCollection resolves the database (see databaseName) and the
// collection in it.
func (s *Server) getCollection(ss *mcp.ServerSession, dbName, collName, action string) (*db.Collection, error) {
	database, err := s.engine.Database(s.databaseName(ss, dbName))
	if err != nil {
		return nil, toolError(action, err)
	}
	coll, err := database.Collection(collName)
	if err != nil {
		return nil, toolError(action, err)
	}
	return coll, nil
}

// nonNil returns names, or an empty slice so JSON gets [] instead of null.
func nonNil(names []string) []string {
	if names == nil {
		return []string{}
	}
	return names
}

// Tool handlers

// Database management handlers
func (s *Server) createDatabaseTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input CreateDatabaseInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	if _, err := s.engine.CreateDatabase(ctx, input.Name); err != nil {
		return nil, nil, toolError("create database", err)
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
	return nil, map[string]interface{}{
		"success":   true,
		"databases": nonNil(s.engine.ListDatabases()),
	}, nil
}

func (s *Server) deleteDatabaseTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input DeleteDatabaseInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	if err := s.engine.DropDatabase(ctx, input.Name); err != nil {
		return nil, nil, toolError("delete database", err)
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
	if _, err := s.engine.Database(input.Name); err != nil {
		return nil, nil, toolError("use database", err)
	}
	if req.Session == nil {
		return nil, nil, errors.New("use database: no MCP session")
	}
	s.trackSession(req.Session).set(input.Name)

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
		"current_database": s.databaseName(req.Session, ""),
	}, nil
}

// parseSchema converts the create_collection schema argument
// ({"fields": {name: {"type": ..., "required": ...}}}) to a *db.Schema.
func parseSchema(raw map[string]interface{}) *db.Schema {
	if raw == nil {
		return nil
	}
	schema := &db.Schema{
		Fields: make(map[string]db.Field),
	}
	if fields, ok := raw["fields"].(map[string]interface{}); ok {
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
	return schema
}

// Collection management handlers
func (s *Server) createCollectionTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input CreateCollectionInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.engine.Database(s.databaseName(req.Session, input.Database))
	if err != nil {
		return nil, nil, toolError("create collection", err)
	}

	if err := database.CreateCollection(ctx, input.Name, parseSchema(input.Schema)); err != nil {
		return nil, nil, toolError("create collection", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Collection '%s' created in database '%s'", input.Name, database.Name()),
	}, nil
}

func (s *Server) listCollectionsTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input ListCollectionsInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	database, err := s.engine.Database(s.databaseName(req.Session, input.Database))
	if err != nil {
		return nil, nil, toolError("list collections", err)
	}

	return nil, map[string]interface{}{
		"success":     true,
		"collections": nonNil(database.ListCollections()),
		"database":    database.Name(),
	}, nil
}

// Document management handlers
func (s *Server) insertDocumentTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input InsertDocumentInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	coll, err := s.getCollection(req.Session, input.Database, input.Collection, "insert document")
	if err != nil {
		return nil, nil, err
	}

	// A string "_id" in the document is the client-chosen document ID; the
	// engine takes it as Document.ID, not as a data field.
	doc := &db.Document{Data: make(map[string]any, len(input.Document))}
	for k, v := range input.Document {
		if k == "_id" {
			if id, ok := v.(string); ok {
				doc.ID = id
				continue
			}
		}
		doc.Data[k] = v
	}

	id, err := coll.Insert(ctx, doc)
	if err != nil {
		return nil, nil, toolError("insert document", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"id":      id,
		"message": fmt.Sprintf("Document inserted with ID: %s", id),
	}, nil
}

// parseQuery converts the find_documents query argument
// ({"filters": [{field, operator, value}], "limit": n, "skip": n}).
func parseQuery(raw map[string]interface{}) *db.Query {
	query := &db.Query{}
	if raw == nil {
		return query
	}
	if filters, ok := raw["filters"].([]interface{}); ok {
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
	if limit, ok := raw["limit"].(float64); ok {
		query.Limit = int(limit)
	}
	if skip, ok := raw["skip"].(float64); ok {
		query.Skip = int(skip)
	}
	return query
}

func (s *Server) findDocumentsTool(
	ctx context.Context,
	req *mcp.CallToolRequest,
	input FindDocumentsInput,
) (*mcp.CallToolResult, map[string]interface{}, error) {
	coll, err := s.getCollection(req.Session, input.Database, input.Collection, "find documents")
	if err != nil {
		return nil, nil, err
	}

	docs, err := coll.Find(ctx, parseQuery(input.Query))
	if err != nil {
		return nil, nil, toolError("find documents", err)
	}

	// Convert documents to JSON for output
	docsJSON := make([]interface{}, len(docs))
	for i, doc := range docs {
		docMap := make(map[string]interface{}, len(doc.Data)+1)
		for k, v := range doc.Data {
			docMap[k] = v
		}
		docMap["_id"] = doc.ID
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
	coll, err := s.getCollection(req.Session, input.Database, input.Collection, "update document")
	if err != nil {
		return nil, nil, err
	}

	if _, err := coll.Update(ctx, input.ID, input.Updates); err != nil {
		return nil, nil, toolError("update document", err)
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
	coll, err := s.getCollection(req.Session, input.Database, input.Collection, "delete document")
	if err != nil {
		return nil, nil, err
	}

	if err := coll.Delete(ctx, input.ID); err != nil {
		return nil, nil, toolError("delete document", err)
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
	coll, err := s.getCollection(req.Session, input.Database, input.Collection, "create index")
	if err != nil {
		return nil, nil, err
	}

	if err := coll.CreateIndex(ctx, input.IndexName, input.FieldName); err != nil {
		return nil, nil, toolError("create index", err)
	}

	return nil, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Index '%s' created on field '%s'", input.IndexName, input.FieldName),
	}, nil
}
