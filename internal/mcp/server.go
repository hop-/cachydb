package mcpserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/hop-/cachydb/pkg/db"
	sdkauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// httpSessionTimeout is how long an idle Streamable HTTP session is kept
// before the SDK closes it.
const httpSessionTimeout = 30 * time.Minute

// httpShutdownTimeout bounds the graceful HTTP shutdown when Start's
// context is cancelled.
const httpShutdownTimeout = 5 * time.Second

// Result bounds of find_documents (approach-agent-interface §2): the
// defaults of Config.DefaultLimit and Config.MaxResultBytes.
const (
	DefaultFindLimit      = 100
	DefaultMaxResultBytes = 1 << 20 // 1 MiB
)

// Server is the CachyDB MCP server. Its tool handlers only call methods of
// the *db.Engine it was given; the engine is owned by the caller, which
// must close it after Close returns.
type Server struct {
	engine        *db.Engine
	server        *mcp.Server
	defaultDBName string
	transport     string
	httpAddr      string // host:port the HTTP transport listens on
	tlsCert       string // PEM files; both set = HTTPS
	tlsKey        string
	version       string    // build version, reported by /healthz
	started       time.Time // when New created the server (server_stats uptime)
	// defaultLimit is the find_documents limit when the call gives none;
	// maxResultBytes caps the JSON size of its documents.
	defaultLimit   int
	maxResultBytes int64
	// tokensRequired means every /mcp request must carry the token of a
	// user in users; /healthz never needs one.
	tokensRequired bool
	users          *auth.Store   // set when tokensRequired, may be set otherwise
	logger         *slog.Logger  // failed authentications, user-management changes
	stderr         io.Writer     // startup/shutdown messages; never stdout
	stdio          mcp.Transport // the stdio transport; tests replace it
	// adminTools names the registered user-management tools, which
	// tools/list shows only to server admins (filterToolsList).
	adminTools map[string]bool

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

// Config is the configuration of a Server.
type Config struct {
	// DefaultDatabase is created if missing and used when a tool call
	// names no database and the session chose none.
	DefaultDatabase string
	// Transport is "http", or anything else for stdio.
	Transport string
	// Listen is where the HTTP transport listens, and whether it serves
	// TLS.
	Listen ListenConfig
	// Version is the build version reported in the server's
	// implementation info and by /healthz.
	Version string
	// TokensRequired makes every HTTP request to /mcp present the token
	// of a user in Users (the start-up decision of auth.Decide). /healthz
	// never needs a token, and stdio is never affected.
	TokensRequired bool
	// Users is the auth store; it must be set when TokensRequired is. It
	// may be set without TokensRequired: delete_database then still
	// removes the deleted database's grants, and every call keeps full
	// access.
	Users *auth.Store
	// Logger receives failed authentications (INFO); nil means
	// slog.Default().
	Logger *slog.Logger
	// DefaultLimit is the number of documents find_documents returns at
	// most when the call gives no limit (or 0); 0 means DefaultFindLimit.
	DefaultLimit int
	// MaxResultBytes caps the JSON size of the documents find_documents
	// returns, whatever the limit (at least one document is returned); 0
	// means DefaultMaxResultBytes.
	MaxResultBytes int64
}

// NewServer creates a server without TLS or tokens: New with the given
// default database, transport, HTTP listen address ("host:port"; empty
// means DefaultHost on a free port), and version.
func NewServer(ctx context.Context, engine *db.Engine, defaultDBName, transport, httpAddr, version string) (*Server, error) {
	var listen ListenConfig
	if httpAddr != "" {
		host, port, err := net.SplitHostPort(httpAddr)
		if err != nil {
			return nil, fmt.Errorf("creating MCP server: listen address: %w", err)
		}
		if listen.Port, err = strconv.Atoi(port); err != nil {
			return nil, fmt.Errorf("creating MCP server: listen address %q: invalid port", httpAddr)
		}
		listen.Host = host
	}
	return New(ctx, engine, Config{
		DefaultDatabase: defaultDBName,
		Transport:       transport,
		Listen:          listen,
		Version:         version,
	})
}

// New creates an MCP server on the opened engine. It creates the default
// database if it does not exist.
func New(ctx context.Context, engine *db.Engine, cfg Config) (*Server, error) {
	if engine == nil {
		return nil, errors.New("creating MCP server: engine is nil")
	}
	if err := cfg.Listen.Validate(); err != nil {
		return nil, fmt.Errorf("creating MCP server: %w", err)
	}
	if cfg.TokensRequired && cfg.Users == nil {
		return nil, errors.New("creating MCP server: tokens are required but there is no auth store")
	}
	if cfg.DefaultLimit < 0 || cfg.MaxResultBytes < 0 {
		return nil, fmt.Errorf("creating MCP server: the default limit (%d) and the result byte cap (%d) must not be negative", cfg.DefaultLimit, cfg.MaxResultBytes)
	}
	if cfg.DefaultLimit == 0 {
		cfg.DefaultLimit = DefaultFindLimit
	}
	if cfg.MaxResultBytes == 0 {
		cfg.MaxResultBytes = DefaultMaxResultBytes
	}
	if _, err := engine.CreateDatabase(ctx, cfg.DefaultDatabase); err != nil {
		return nil, fmt.Errorf("creating default database %q: %w", cfg.DefaultDatabase, err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		engine:         engine,
		defaultDBName:  cfg.DefaultDatabase,
		transport:      cfg.Transport,
		httpAddr:       cfg.Listen.Addr(),
		tlsCert:        cfg.Listen.TLSCert,
		tlsKey:         cfg.Listen.TLSKey,
		version:        cfg.Version,
		started:        time.Now(),
		defaultLimit:   cfg.DefaultLimit,
		maxResultBytes: cfg.MaxResultBytes,
		tokensRequired: cfg.TokensRequired,
		users:          cfg.Users,
		logger:         logger,
		stderr:         os.Stderr,
		stdio:          &mcp.StdioTransport{},
	}

	s.server = mcp.NewServer(&mcp.Implementation{
		Name:    "cachydb",
		Title:   "CachyDB document database",
		Version: cfg.Version,
	}, &mcp.ServerOptions{
		Instructions: Instructions(),
		InitializedHandler: func(_ context.Context, req *mcp.InitializedRequest) {
			s.trackSession(req.Session)
		},
	})
	// Clients on protocol 2026-07-28 and later skip the initialize
	// handshake (server/discover), so InitializedHandler never runs for
	// them: also track the session on every request it sends.
	s.server.AddReceivingMiddleware(s.trackSessionMiddleware)
	if s.tokensRequired {
		s.server.AddReceivingMiddleware(s.filterToolsList)
	}
	if err := s.registerTools(s.server); err != nil {
		return nil, fmt.Errorf("creating MCP server: %w", err)
	}
	if err := registerDocsResources(s.server); err != nil {
		return nil, fmt.Errorf("creating MCP server: %w", err)
	}
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
		s.waiters.Go(func() {
			// The wait error only says how the session ended; there is
			// nothing to do with it beyond forgetting the session.
			_ = ss.Wait()
			s.sessions.Delete(ss)
		})
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

// httpHandler returns the HTTP handler: the Streamable HTTP transport
// serving s.server at /mcp, and the health endpoint at /healthz. When
// tokens are required, /mcp answers 401 to a request without a valid
// bearer token; /healthz stays open.
func (s *Server) httpHandler() http.Handler {
	var mcpHandler http.Handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return s.server
	}, &mcp.StreamableHTTPOptions{SessionTimeout: httpSessionTimeout})
	if s.tokensRequired {
		mcpHandler = s.logMissingToken(sdkauth.RequireBearerToken(s.verifyToken, &sdkauth.RequireBearerTokenOptions{
			// Tokens do not expire.
			AllowMissingExpiration: true,
		})(mcpHandler))
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpHandler)
	mux.HandleFunc("/healthz", s.healthz)
	return mux
}

// verifyToken is the bearer-token verifier of /mcp: it accepts the token
// of any user and returns TokenInfo{UserID: user name}, or an error
// wrapping the SDK's ErrInvalidToken (HTTP 401) for an unknown or revoked
// token. The SDK binds each session to the user that created it and
// answers 403 when another user's token reuses it; the tool handlers
// authorize every call by the user's current grants.
func (s *Server) verifyToken(_ context.Context, token string, r *http.Request) (*sdkauth.TokenInfo, error) {
	p, err := s.users.Authenticate(token)
	if err != nil {
		s.logAuthFailure(r, "unknown or revoked token")
		return nil, fmt.Errorf("%w: unknown or revoked token", sdkauth.ErrInvalidToken)
	}
	return &sdkauth.TokenInfo{UserID: p.Name}, nil
}

// logMissingToken logs the requests next will refuse for having no bearer
// token at all; the SDK answers those without calling verifyToken.
func (s *Server) logMissingToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The SDK's test: exactly "Bearer <token>", scheme case-insensitive.
		fields := strings.Fields(r.Header.Get("Authorization"))
		if len(fields) != 2 || !strings.EqualFold(fields[0], "bearer") {
			s.logAuthFailure(r, "no bearer token")
		}
		next.ServeHTTP(w, r)
	})
}

// logAuthFailure logs a refused /mcp request at INFO with the client's
// address and the reason; never the token.
func (s *Server) logAuthFailure(r *http.Request, reason string) {
	remote := ""
	if r != nil {
		remote = r.RemoteAddr
	}
	s.logger.Info("authentication failed", "remote", remote, "reason", reason)
}

// HTTPHandler returns the HTTP handler the http transport serves: the
// Streamable HTTP transport at /mcp and the health endpoint at /healthz.
// It lets callers (and tests) serve the server on their own listener.
func (s *Server) HTTPHandler() http.Handler { return s.httpHandler() }

// healthResponse is the body of GET /healthz.
type healthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version,omitempty"`
}

// healthz answers GET /healthz: 200 {"status":"ok","version":...} while
// the engine is open, 503 {"status":"unavailable"} once it is closing or
// closed, and 405 for other methods. It is outside MCP session handling
// and is what "cachydb healthcheck" (the image HEALTHCHECK) probes.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	code, body := http.StatusOK, healthResponse{Status: "ok", Version: s.version}
	if s.engine.Closed() {
		code, body = http.StatusServiceUnavailable, healthResponse{Status: "unavailable"}
	}
	data, err := json.Marshal(body)
	if err != nil {
		// Two plain strings; this cannot happen.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	// A write error means the client went away; nothing to report.
	_, _ = w.Write(append(data, '\n'))
}

// startHTTP serves the Streamable HTTP transport (MCP spec 2025-03-26+) at
// /mcp, and /healthz, until ctx is cancelled; over TLS when a certificate
// and key are configured.
func (s *Server) startHTTP(ctx context.Context) error {
	httpServer := &http.Server{
		Handler:           s.httpHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	scheme := "http"
	if s.tlsCert != "" || s.tlsKey != "" {
		// Loaded before listening, so a bad file fails the start-up
		// instead of the first connection.
		cert, err := tls.LoadX509KeyPair(s.tlsCert, s.tlsKey)
		if err != nil {
			return fmt.Errorf("loading the TLS certificate and key: %w", err)
		}
		httpServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		scheme = "https"
	}
	ln, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.httpAddr, err)
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

	access := "no token needed"
	if s.tokensRequired {
		access = "tokens required"
	}
	fmt.Fprintf(s.stderr, "CachyDB MCP server listening on %s://%s/mcp (Streamable HTTP transport, %s)\n", scheme, ln.Addr(), access)

	var serveErr error
	if httpServer.TLSConfig != nil {
		// The certificate is in TLSConfig, so no file names.
		serveErr = httpServer.ServeTLS(ln, "", "")
	} else {
		serveErr = httpServer.Serve(ln)
	}
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

// Helper methods

// toolError turns an engine error into the error a tool returns; the SDK
// reports it to the client as a tool error (IsError) carrying the message.
// Client errors end with a short hint on what to do next. The text starts
// with the error's stable code in brackets (errorCode, errors.go).
func toolError(action string, err error) error {
	return withCode(err, describeError(action, err))
}

// describeError is the text of a tool error without its code: the action,
// the cause, and a hint.
func describeError(action string, err error) error {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return fmt.Errorf("%s: %w (hint: list_databases and list_collections show what exists; current_database shows the database used when none is given)", action, err)
	case errors.Is(err, db.ErrSchemaValidation):
		return fmt.Errorf("%s: %w (hint: see resource cachydb://docs/schema)", action, err)
	case errors.Is(err, db.ErrInvalidUpdate):
		return fmt.Errorf("%s: %w (hint: get_document shows the field's current value; see the update operators in resource cachydb://docs/query)", action, err)
	case errors.Is(err, db.ErrConditionFailed):
		return fmt.Errorf("%s: %w; re-read it and retry", action, err)
	case errors.Is(err, auth.ErrPermissionDenied):
		// The auth message names the user, the database, and the level.
		return fmt.Errorf("%s: %w (hint: ask a server admin for access)", action, err)
	case errors.Is(err, auth.ErrUnauthenticated):
		return fmt.Errorf("%s: %w (hint: ask a server admin for a valid token)", action, err)
	case errors.Is(err, db.ErrExists), errors.Is(err, db.ErrInvalidName):
		// The engine message names the object and the rule.
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

// databaseName resolves the database for a request of principal p: the
// explicit input, then the session's use_database choice, then the user's
// default database, then the server default. Tool handlers get their
// "database" argument already resolved (authorizeCall).
func (s *Server) databaseName(ss *mcp.ServerSession, p auth.Principal, input string) string {
	if input != "" {
		return input
	}
	if name := s.sessionDatabase(ss); name != "" {
		return name
	}
	if p.DefaultDatabase != "" {
		return p.DefaultDatabase
	}
	return s.defaultDBName
}

// getCollection returns the collection collName of the database dbName,
// an already resolved "database" argument.
func (s *Server) getCollection(dbName, collName, action string) (*db.Collection, error) {
	database, err := s.engine.Database(dbName)
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

func (s *Server) createDatabaseTool(ctx context.Context, _ *mcp.CallToolRequest, in CreateDatabaseInput) (*mcp.CallToolResult, MessageOutput, error) {
	if _, err := s.engine.CreateDatabase(ctx, in.Name); err != nil {
		return nil, MessageOutput{}, toolError("create database", err)
	}
	return nil, MessageOutput{Success: true, Message: fmt.Sprintf("Database '%s' created successfully", in.Name)}, nil
}

// listDatabasesTool lists the databases the principal can read.
func (s *Server) listDatabasesTool(ctx context.Context, _ *mcp.CallToolRequest, _ ListDatabasesInput) (*mcp.CallToolResult, ListDatabasesOutput, error) {
	p, ok := principalFrom(ctx)
	if !ok {
		return nil, ListDatabasesOutput{}, toolError("list databases", errNoPrincipal)
	}
	names := s.engine.ListDatabases()
	if !p.Implicit {
		readable := names[:0]
		for _, name := range names {
			// p is the user's current state (authorizeCall re-read it).
			if p.Can(name, auth.LevelRead) {
				readable = append(readable, name)
			}
		}
		names = readable
	}
	return nil, ListDatabasesOutput{Success: true, Databases: nonNil(names)}, nil
}

// deleteDatabaseTool drops the database and then removes every user's
// grant on it, so a database created later under that name starts with
// no grants.
func (s *Server) deleteDatabaseTool(ctx context.Context, _ *mcp.CallToolRequest, in DeleteDatabaseInput) (*mcp.CallToolResult, MessageOutput, error) {
	if err := s.engine.DropDatabase(ctx, in.Name); err != nil {
		return nil, MessageOutput{}, toolError("delete database", err)
	}
	if s.users != nil {
		// The database is gone: finish the cleanup even if the request is
		// cancelled now.
		if err := s.users.ForgetDatabase(context.WithoutCancel(ctx), in.Name); err != nil {
			return nil, MessageOutput{}, toolError("delete database",
				fmt.Errorf("database %q deleted, but its grants were not all removed (retry delete_database after recreating it): %w", in.Name, err))
		}
	}
	return nil, MessageOutput{Success: true, Message: fmt.Sprintf("Database '%s' deleted successfully", in.Name)}, nil
}

// useDatabaseTool sets the session's database; the principal must be able
// to read it. Access is checked before existence, so the answer does not
// reveal which unreadable databases exist.
func (s *Server) useDatabaseTool(ctx context.Context, req *mcp.CallToolRequest, in UseDatabaseInput) (*mcp.CallToolResult, UseDatabaseOutput, error) {
	p, ok := principalFrom(ctx)
	if !ok {
		return nil, UseDatabaseOutput{}, toolError("use database", errNoPrincipal)
	}
	if err := s.authorize(p, in.Name, auth.LevelRead); err != nil {
		return nil, UseDatabaseOutput{}, toolError("use database", err)
	}
	if _, err := s.engine.Database(in.Name); err != nil {
		return nil, UseDatabaseOutput{}, toolError("use database", err)
	}
	if req.Session == nil {
		return nil, UseDatabaseOutput{}, toolError("use database", errors.New("no MCP session"))
	}
	s.trackSession(req.Session).set(in.Name)
	return nil, UseDatabaseOutput{
		Success:         true,
		Message:         fmt.Sprintf("Now using database '%s' as default", in.Name),
		CurrentDatabase: in.Name,
	}, nil
}

func (s *Server) currentDatabaseTool(ctx context.Context, req *mcp.CallToolRequest, _ CurrentDatabaseInput) (*mcp.CallToolResult, CurrentDatabaseOutput, error) {
	p, ok := principalFrom(ctx)
	if !ok {
		return nil, CurrentDatabaseOutput{}, toolError("current database", errNoPrincipal)
	}
	return nil, CurrentDatabaseOutput{Success: true, CurrentDatabase: s.databaseName(req.Session, p, "")}, nil
}

// toDBSchema converts the create_collection schema argument to a
// *db.Schema (nil for a schemaless collection).
func toDBSchema(in *SchemaDef) *db.Schema {
	if in == nil {
		return nil
	}
	schema := &db.Schema{Fields: make(map[string]db.Field, len(in.Fields))}
	for name, f := range in.Fields {
		schema.Fields[name] = db.Field{Type: db.FieldType(f.Type), Required: f.Required}
	}
	return schema
}

func (s *Server) createCollectionTool(ctx context.Context, req *mcp.CallToolRequest, in CreateCollectionInput) (*mcp.CallToolResult, MessageOutput, error) {
	database, err := s.engine.Database(in.Database)
	if err != nil {
		return nil, MessageOutput{}, toolError("create collection", err)
	}
	if err := database.CreateCollection(ctx, in.Name, toDBSchema(in.Schema)); err != nil {
		return nil, MessageOutput{}, toolError("create collection", err)
	}
	return nil, MessageOutput{
		Success: true,
		Message: fmt.Sprintf("Collection '%s' created in database '%s'", in.Name, database.Name()),
	}, nil
}

func (s *Server) listCollectionsTool(_ context.Context, req *mcp.CallToolRequest, in ListCollectionsInput) (*mcp.CallToolResult, ListCollectionsOutput, error) {
	database, err := s.engine.Database(in.Database)
	if err != nil {
		return nil, ListCollectionsOutput{}, toolError("list collections", err)
	}
	return nil, ListCollectionsOutput{
		Success:     true,
		Collections: nonNil(database.ListCollections()),
		Database:    database.Name(),
	}, nil
}

func (s *Server) insertDocumentTool(ctx context.Context, req *mcp.CallToolRequest, in InsertDocumentInput) (*mcp.CallToolResult, InsertDocumentOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "insert document")
	if err != nil {
		return nil, InsertDocumentOutput{}, err
	}

	id, err := coll.Insert(ctx, toDBDocument(in.Document))
	if err != nil {
		return nil, InsertDocumentOutput{}, toolError("insert document", err)
	}
	return nil, InsertDocumentOutput{Success: true, ID: id, Message: fmt.Sprintf("Document inserted with ID: %s", id)}, nil
}

// toDBDocument converts an inserted document argument. A string "_id" is
// the client-chosen document ID; the engine takes it as Document.ID, not
// as a data field. Any other "_id" stays in the data, where the engine
// rejects it.
func toDBDocument(in map[string]any) *db.Document {
	doc := &db.Document{Data: make(map[string]any, len(in))}
	for k, v := range in {
		if k == "_id" {
			if id, ok := v.(string); ok {
				doc.ID = id
				continue
			}
		}
		doc.Data[k] = v
	}
	return doc
}

// insertDocumentsTool inserts every document in one atomic batch. A
// failing document is named by its position in the documents argument.
func (s *Server) insertDocumentsTool(ctx context.Context, req *mcp.CallToolRequest, in InsertDocumentsInput) (*mcp.CallToolResult, InsertDocumentsOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "insert documents")
	if err != nil {
		return nil, InsertDocumentsOutput{}, err
	}
	ops := make([]db.WriteOp, len(in.Documents))
	for i, d := range in.Documents {
		ops[i] = db.WriteOp{Kind: db.OpInsert, Document: toDBDocument(d)}
	}
	res, err := coll.Batch(ctx, ops)
	if err != nil {
		if bie, ok := errors.AsType[*db.BatchItemError](err); ok {
			err = fmt.Errorf("documents[%d]: %w", bie.Index, bie.Err)
		}
		return nil, InsertDocumentsOutput{}, toolError("insert documents", err)
	}
	return nil, InsertDocumentsOutput{Success: true, IDs: nonNil(res.IDs), Inserted: len(res.IDs)}, nil
}

// getDocumentTool returns one document by id. Without fields it reads
// the document directly; with fields it runs an _id lookup with that
// projection, so fields mean what they mean in find_documents.
func (s *Server) getDocumentTool(ctx context.Context, req *mcp.CallToolRequest, in GetDocumentInput) (*mcp.CallToolResult, GetDocumentOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "get document")
	if err != nil {
		return nil, GetDocumentOutput{}, err
	}
	var doc *db.Document
	if len(in.Fields) == 0 {
		doc, err = coll.Get(ctx, in.ID)
	} else {
		var docs []*db.Document
		docs, err = coll.Find(ctx, &db.Query{
			Filters: []db.QueryFilter{{Field: "_id", Operator: "eq", Value: in.ID}},
			Fields:  in.Fields,
		})
		switch {
		case err != nil:
		case len(docs) == 0:
			err = fmt.Errorf("document %q in collection %q: %w", in.ID, in.Collection, db.ErrNotFound)
		default:
			doc = docs[0]
		}
	}
	if err != nil {
		return nil, GetDocumentOutput{}, toolError("get document", err)
	}
	return nil, GetDocumentOutput{Success: true, Document: docMap(doc)}, nil
}

// emptyMatch reports whether q matches every document: no filters, and
// no OR groups or an empty group (which every document matches).
func emptyMatch(q *MatchQuery) bool {
	if q == nil {
		return true
	}
	if len(q.Filters) > 0 {
		return false
	}
	return len(q.Any) == 0 || slices.ContainsFunc(q.Any, func(g []Filter) bool { return len(g) == 0 })
}

// checkBatchQuery rejects an empty query without all, and a negative
// limit; action names the tool in the error.
func checkBatchQuery(action string, q *MatchQuery, all bool, limit int) error {
	if limit < 0 {
		return toolError(action, invalidArgf("limit must not be negative, got %d", limit))
	}
	if !all && emptyMatch(q) {
		return toolError(action, invalidArgf("the query is empty and would match every document; give query.filters or query.any, or set \"all\": true"))
	}
	return nil
}

// updateSpec builds the engine update of update_document and
// update_documents, or returns nil if it changes nothing: no updates
// object (an empty one counts, as before), no unset, and no operator.
func updateSpec(updates map[string]any, unset []string, inc map[string]float64, push, addToSet, pull map[string]any) *db.UpdateSpec {
	if updates == nil && len(unset) == 0 && len(inc) == 0 && len(push) == 0 && len(addToSet) == 0 && len(pull) == 0 {
		return nil
	}
	u := &db.UpdateSpec{Set: updates, Unset: unset, Push: push, AddToSet: addToSet, Pull: pull}
	if len(inc) > 0 {
		u.Inc = make(map[string]any, len(inc))
		for f, n := range inc {
			u.Inc[f] = n
		}
	}
	return u
}

func (s *Server) updateDocumentsTool(ctx context.Context, req *mcp.CallToolRequest, in UpdateDocumentsInput) (*mcp.CallToolResult, UpdateDocumentsOutput, error) {
	if err := checkBatchQuery("update documents", in.Query, in.All, in.Limit); err != nil {
		return nil, UpdateDocumentsOutput{}, err
	}
	u := updateSpec(in.Updates, in.Unset, in.Inc, in.Push, in.AddToSet, in.Pull)
	if u == nil {
		return nil, UpdateDocumentsOutput{}, toolError("update documents", invalidArgf("give updates, unset, or an operator (inc, push, add_to_set, pull)"))
	}
	coll, err := s.getCollection(in.Database, in.Collection, "update documents")
	if err != nil {
		return nil, UpdateDocumentsOutput{}, err
	}
	matched, modified, err := coll.UpdateMany(ctx, toDBMatchQuery(in.Query), *u, in.Limit)
	if err != nil {
		return nil, UpdateDocumentsOutput{}, toolError("update documents", err)
	}
	return nil, UpdateDocumentsOutput{Success: true, Matched: matched, Modified: modified}, nil
}

func (s *Server) deleteDocumentsTool(ctx context.Context, req *mcp.CallToolRequest, in DeleteDocumentsInput) (*mcp.CallToolResult, DeleteDocumentsOutput, error) {
	if err := checkBatchQuery("delete documents", in.Query, in.All, in.Limit); err != nil {
		return nil, DeleteDocumentsOutput{}, err
	}
	coll, err := s.getCollection(in.Database, in.Collection, "delete documents")
	if err != nil {
		return nil, DeleteDocumentsOutput{}, err
	}
	matched, deleted, err := coll.DeleteMany(ctx, toDBMatchQuery(in.Query), in.Limit)
	if err != nil {
		return nil, DeleteDocumentsOutput{}, toolError("delete documents", err)
	}
	return nil, DeleteDocumentsOutput{Success: true, Matched: matched, Deleted: deleted}, nil
}

// toDBFilters converts filters to engine filters.
func toDBFilters(in []Filter) []db.QueryFilter {
	if len(in) == 0 {
		return nil
	}
	out := make([]db.QueryFilter, len(in))
	for i, f := range in {
		out[i] = db.QueryFilter{Field: f.Field, Operator: string(f.Operator), Value: f.Value}
	}
	return out
}

// toDBGroups converts OR groups to engine filter groups.
func toDBGroups(in [][]Filter) [][]db.QueryFilter {
	if len(in) == 0 {
		return nil
	}
	out := make([][]db.QueryFilter, len(in))
	for i, g := range in {
		out[i] = toDBFilters(g)
		if out[i] == nil {
			out[i] = []db.QueryFilter{}
		}
	}
	return out
}

// toDBQuery converts the find_documents query argument to a *db.Query.
func toDBQuery(in *Query) *db.Query {
	q := &db.Query{}
	if in == nil {
		return q
	}
	q.Filters = toDBFilters(in.Filters)
	q.Any = toDBGroups(in.Any)
	q.Fields = in.Fields
	for _, k := range in.Sort {
		q.Sort = append(q.Sort, db.SortKey{Field: k.Field, Order: string(k.Order)})
	}
	q.Limit, q.Skip = in.Limit, in.Skip
	return q
}

// toDBMatchQuery converts a count_documents or distinct query.
func toDBMatchQuery(in *MatchQuery) *db.Query {
	if in == nil {
		return &db.Query{}
	}
	return &db.Query{Filters: toDBFilters(in.Filters), Any: toDBGroups(in.Any)}
}

// docMap returns doc as a flat object with its _id.
func docMap(doc *db.Document) map[string]any {
	m := make(map[string]any, len(doc.Data)+1)
	maps.Copy(m, doc.Data)
	m["_id"] = doc.ID
	return m
}

func (s *Server) findDocumentsTool(ctx context.Context, req *mcp.CallToolRequest, in FindDocumentsInput) (*mcp.CallToolResult, FindDocumentsOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "find documents")
	if err != nil {
		return nil, FindDocumentsOutput{}, err
	}
	q := toDBQuery(in.Query)
	limit := q.Limit
	if limit <= 0 {
		limit = s.defaultLimit
	}
	// Ask for one document more than the limit, to know whether more
	// match (has_more) without counting them.
	q.Limit = limit
	if limit < math.MaxInt {
		q.Limit = limit + 1
	}
	var (
		out     = make([]map[string]any, 0)
		size    = int64(len("[]")) // the JSON size of out
		hasMore bool
		encErr  error
	)
	plan, err := coll.IterateWithPlan(ctx, q, func(doc *db.Document) bool {
		if len(out) == limit {
			hasMore = true
			return false
		}
		m := docMap(doc)
		data, err := json.Marshal(m)
		if err != nil {
			encErr = err
			return false
		}
		n := int64(len(data))
		if len(out) > 0 {
			n++ // the comma before it
		}
		if len(out) > 0 && size+n > s.maxResultBytes {
			hasMore = true
			return false
		}
		out = append(out, m)
		size += n
		return true
	})
	if err == nil {
		err = encErr
	}
	if err != nil {
		return nil, FindDocumentsOutput{}, toolError("find documents", err)
	}
	res := FindDocumentsOutput{Success: true, Count: len(out), Documents: out, HasMore: hasMore}
	if hasMore {
		res.NextSkip = max(q.Skip, 0) + len(out)
	}
	if in.Explain {
		// plan.Returned counts the look-ahead document too; report what
		// this page returned.
		res.Plan = &QueryPlan{Driver: plan.Driver, Index: plan.Index, OrderProvided: plan.OrderProvided,
			Examined: plan.Examined, Returned: len(out)}
	}
	return nil, res, nil
}

func (s *Server) countDocumentsTool(ctx context.Context, req *mcp.CallToolRequest, in CountDocumentsInput) (*mcp.CallToolResult, CountDocumentsOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "count documents")
	if err != nil {
		return nil, CountDocumentsOutput{}, err
	}
	n, err := coll.CountDocuments(ctx, toDBMatchQuery(in.Query))
	if err != nil {
		return nil, CountDocumentsOutput{}, toolError("count documents", err)
	}
	return nil, CountDocumentsOutput{Success: true, Count: n}, nil
}

func (s *Server) distinctTool(ctx context.Context, req *mcp.CallToolRequest, in DistinctInput) (*mcp.CallToolResult, DistinctOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "distinct")
	if err != nil {
		return nil, DistinctOutput{}, err
	}
	vals, err := coll.Distinct(ctx, in.Field, toDBMatchQuery(in.Query))
	if err != nil {
		return nil, DistinctOutput{}, toolError("distinct", err)
	}
	if vals == nil {
		vals = []any{}
	}
	return nil, DistinctOutput{Success: true, Values: vals}, nil
}

func (s *Server) updateDocumentTool(ctx context.Context, req *mcp.CallToolRequest, in UpdateDocumentInput) (*mcp.CallToolResult, UpdateDocumentOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "update document")
	if err != nil {
		return nil, UpdateDocumentOutput{}, err
	}
	u := updateSpec(in.Updates, in.Unset, in.Inc, in.Push, in.AddToSet, in.Pull)
	if u == nil {
		return nil, UpdateDocumentOutput{}, toolError("update document", invalidArgf("give updates, unset, or an operator (inc, push, add_to_set, pull)"))
	}
	u.If, u.Upsert = toDBFilters(in.If), in.Upsert
	_, upserted, err := coll.UpdateWith(ctx, in.ID, *u)
	if err != nil {
		return nil, UpdateDocumentOutput{}, toolError("update document", err)
	}
	msg := fmt.Sprintf("Document %s updated", in.ID)
	if upserted {
		msg = fmt.Sprintf("Document %s created (upsert)", in.ID)
	}
	return nil, UpdateDocumentOutput{Success: true, Message: msg, Upserted: upserted}, nil
}

func (s *Server) replaceDocumentTool(ctx context.Context, req *mcp.CallToolRequest, in ReplaceDocumentInput) (*mcp.CallToolResult, MessageOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "replace document")
	if err != nil {
		return nil, MessageOutput{}, err
	}
	doc := &db.Document{ID: in.ID, Data: make(map[string]any, len(in.Document))}
	for k, v := range in.Document {
		if k == "_id" {
			if id, ok := v.(string); !ok || id != in.ID {
				return nil, MessageOutput{}, toolError("replace document",
					fmt.Errorf("%w: document _id %v differs from id %q", db.ErrSchemaValidation, v, in.ID))
			}
			continue
		}
		doc.Data[k] = v
	}
	if _, err := coll.Replace(ctx, in.ID, doc, toDBFilters(in.If)); err != nil {
		return nil, MessageOutput{}, toolError("replace document", err)
	}
	return nil, MessageOutput{Success: true, Message: fmt.Sprintf("Document %s replaced", in.ID)}, nil
}

func (s *Server) deleteDocumentTool(ctx context.Context, req *mcp.CallToolRequest, in DeleteDocumentInput) (*mcp.CallToolResult, MessageOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "delete document")
	if err != nil {
		return nil, MessageOutput{}, err
	}
	if err := coll.DeleteWith(ctx, in.ID, toDBFilters(in.If)); err != nil {
		return nil, MessageOutput{}, toolError("delete document", err)
	}
	return nil, MessageOutput{Success: true, Message: fmt.Sprintf("Document %s deleted", in.ID)}, nil
}

// fromDBSchema converts a *db.Schema to the tool schema type (nil for a
// schemaless collection).
func fromDBSchema(in *db.Schema) *SchemaDef {
	if in == nil {
		return nil
	}
	out := &SchemaDef{Fields: make(map[string]FieldDef, len(in.Fields))}
	for name, f := range in.Fields {
		out.Fields[name] = FieldDef{Type: FieldType(f.Type), Required: f.Required}
	}
	return out
}

func (s *Server) describeCollectionTool(_ context.Context, req *mcp.CallToolRequest, in DescribeCollectionInput) (*mcp.CallToolResult, DescribeCollectionOutput, error) {
	dbName := in.Database
	coll, err := s.getCollection(dbName, in.Collection, "describe collection")
	if err != nil {
		return nil, DescribeCollectionOutput{}, err
	}
	indexes := make([]IndexDesc, 0)
	for _, ix := range coll.Indexes() {
		indexes = append(indexes, IndexDesc{Name: ix.Name, Field: ix.Field, Unique: ix.Unique})
	}
	return nil, DescribeCollectionOutput{
		Success:  true,
		Database: dbName,
		Name:     coll.Name(),
		Schema:   fromDBSchema(coll.Schema()),
		Indexes:  indexes,
		Count:    coll.Count(),
	}, nil
}

func (s *Server) updateSchemaTool(ctx context.Context, req *mcp.CallToolRequest, in UpdateSchemaInput) (*mcp.CallToolResult, MessageOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "update schema")
	if err != nil {
		return nil, MessageOutput{}, err
	}
	if err := coll.UpdateSchema(ctx, toDBSchema(in.Schema)); err != nil {
		return nil, MessageOutput{}, toolError("update schema", err)
	}
	msg := fmt.Sprintf("Schema of collection '%s' updated", in.Collection)
	if in.Schema == nil {
		msg = fmt.Sprintf("Schema of collection '%s' removed", in.Collection)
	}
	return nil, MessageOutput{Success: true, Message: msg}, nil
}

func (s *Server) renameCollectionTool(ctx context.Context, req *mcp.CallToolRequest, in RenameCollectionInput) (*mcp.CallToolResult, MessageOutput, error) {
	database, err := s.engine.Database(in.Database)
	if err != nil {
		return nil, MessageOutput{}, toolError("rename collection", err)
	}
	if err := database.RenameCollection(ctx, in.Collection, in.NewName); err != nil {
		return nil, MessageOutput{}, toolError("rename collection", err)
	}
	return nil, MessageOutput{
		Success: true,
		Message: fmt.Sprintf("Collection '%s' renamed to '%s' in database '%s'", in.Collection, in.NewName, database.Name()),
	}, nil
}

func (s *Server) copyCollectionTool(ctx context.Context, req *mcp.CallToolRequest, in CopyCollectionInput) (*mcp.CallToolResult, CopyCollectionOutput, error) {
	database, err := s.engine.Database(in.Database)
	if err != nil {
		return nil, CopyCollectionOutput{}, toolError("copy collection", err)
	}
	target := database
	if in.TargetDatabase != "" {
		if target, err = s.engine.Database(in.TargetDatabase); err != nil {
			return nil, CopyCollectionOutput{}, toolError("copy collection", err)
		}
	}
	n, err := database.CopyCollection(ctx, in.Collection, in.NewName, target)
	if err != nil {
		return nil, CopyCollectionOutput{}, toolError("copy collection", err)
	}
	return nil, CopyCollectionOutput{
		Success: true,
		Message: fmt.Sprintf("Copied %d documents from '%s/%s' to '%s/%s'", n, database.Name(), in.Collection, target.Name(), in.NewName),
		Copied:  n,
	}, nil
}

func (s *Server) deleteCollectionTool(ctx context.Context, req *mcp.CallToolRequest, in DeleteCollectionInput) (*mcp.CallToolResult, MessageOutput, error) {
	database, err := s.engine.Database(in.Database)
	if err != nil {
		return nil, MessageOutput{}, toolError("delete collection", err)
	}
	if err := database.DropCollection(ctx, in.Collection); err != nil {
		return nil, MessageOutput{}, toolError("delete collection", err)
	}
	return nil, MessageOutput{
		Success: true,
		Message: fmt.Sprintf("Collection '%s' deleted from database '%s'", in.Collection, database.Name()),
	}, nil
}

func (s *Server) createIndexTool(ctx context.Context, req *mcp.CallToolRequest, in CreateIndexInput) (*mcp.CallToolResult, MessageOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "create index")
	if err != nil {
		return nil, MessageOutput{}, err
	}
	if err := coll.CreateIndex(ctx, in.IndexName, in.FieldName, db.Unique(in.Unique)); err != nil {
		return nil, MessageOutput{}, toolError("create index", err)
	}
	kind := "Index"
	if in.Unique {
		kind = "Unique index"
	}
	return nil, MessageOutput{
		Success: true,
		Message: fmt.Sprintf("%s '%s' created on field '%s'", kind, in.IndexName, in.FieldName),
	}, nil
}

func (s *Server) dropIndexTool(ctx context.Context, req *mcp.CallToolRequest, in DropIndexInput) (*mcp.CallToolResult, MessageOutput, error) {
	coll, err := s.getCollection(in.Database, in.Collection, "drop index")
	if err != nil {
		return nil, MessageOutput{}, err
	}
	if err := coll.DropIndex(ctx, in.IndexName); err != nil {
		return nil, MessageOutput{}, toolError("drop index", err)
	}
	return nil, MessageOutput{Success: true, Message: fmt.Sprintf("Index '%s' dropped", in.IndexName)}, nil
}

// serverStatsTool reports the figures of the database asked for (its read
// access was checked by authorizeCall), or of every database the
// principal can read, and the server totals for server admins.
func (s *Server) serverStatsTool(ctx context.Context, _ *mcp.CallToolRequest, in ServerStatsInput) (*mcp.CallToolResult, ServerStatsOutput, error) {
	const action = "server stats"
	p, ok := principalFrom(ctx)
	if !ok {
		return nil, ServerStatsOutput{}, toolError(action, errNoPrincipal)
	}
	names := []string{in.Database}
	if in.Database == "" {
		names = s.engine.ListDatabases()
		if !p.Implicit {
			readable := names[:0]
			for _, name := range names {
				if p.Can(name, auth.LevelRead) {
					readable = append(readable, name)
				}
			}
			names = readable
		}
	}
	out := ServerStatsOutput{Success: true, Databases: []DatabaseStats{}}
	for _, name := range names {
		d, err := s.engine.Database(name)
		var ds db.DatabaseStats
		if err == nil {
			ds, err = d.Stats(ctx)
		}
		if err != nil {
			if in.Database == "" && errors.Is(err, db.ErrNotFound) {
				continue // dropped meanwhile
			}
			return nil, ServerStatsOutput{}, toolError(action, err)
		}
		out.Databases = append(out.Databases, NewDatabaseStats(ds))
	}
	if p.Implicit || p.ServerAdmin {
		srv, err := s.engine.ServerStats()
		if err != nil {
			return nil, ServerStatsOutput{}, toolError(action, err)
		}
		out.Server = &ServerTotals{
			Version:       s.version,
			UptimeSeconds: int64(time.Since(s.started).Seconds()),
			WALBytes:      srv.WALBytes,
			Cache:         NewCacheTotal(srv.Cache),
		}
	}
	return nil, out, nil
}

// NewDatabaseStats converts the engine's figures of one database to the
// server_stats form (also used by cachydb utils stats).
func NewDatabaseStats(ds db.DatabaseStats) DatabaseStats {
	out := DatabaseStats{Name: ds.Name, Documents: ds.Documents, DataBytes: ds.DataBytes, IndexBytes: ds.IndexBytes,
		Collections: make([]CollectionStats, len(ds.Collections))}
	for i, c := range ds.Collections {
		out.Collections[i] = CollectionStats{Name: c.Name, Documents: c.Documents, DataBytes: c.DataBytes,
			IndexBytes: c.IndexBytes, Indexes: nonNil(c.Indexes)}
	}
	return out
}

// NewCacheTotal converts the engine's cache figures to the server_stats
// form.
func NewCacheTotal(c db.CacheStats) CacheTotal {
	return CacheTotal{Bytes: c.Bytes, MaxBytes: c.MaxBytes, Entries: c.Entries, Hits: c.Hits, Misses: c.Misses}
}
