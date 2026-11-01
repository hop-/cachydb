package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const testDefaultDB = "default"

// testVersion is the build version the test servers report.
const testVersion = "v9.9.9-test"

// newTestServer opens an engine in a temp dir and builds a Server on it.
// Cleanup closes the server, then the engine.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	engine, err := db.Open(ctx, db.Options{
		Dir:    t.TempDir(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	s, err := NewServer(ctx, engine, testDefaultDB, "stdio", "", testVersion)
	if err != nil {
		_ = engine.Close(ctx)
		t.Fatalf("NewServer: %v", err)
	}
	s.stderr = io.Discard
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Close(closeCtx); err != nil {
			t.Errorf("Server.Close: %v", err)
		}
		if err := engine.Close(closeCtx); err != nil {
			t.Errorf("Engine.Close: %v", err)
		}
	})
	return s
}

// connect opens a client session to s over in-memory transports.
func connect(t *testing.T, s *Server) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := s.server.Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server Connect: %v", err)
	}
	return connectClient(t, clientT)
}

func connectClient(t *testing.T, transport mcp.Transport) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	cs, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// call invokes a tool and returns the raw result (protocol errors fail the
// test; tool errors are left to the caller).
func call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s): %v", tool, err)
	}
	return res
}

// callOK invokes a tool, requires success, and returns its structured output.
func callOK(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) map[string]any {
	t.Helper()
	res := call(t, cs, tool, args)
	if res.IsError {
		t.Fatalf("CallTool(%s, %v): tool error: %s", tool, args, resultText(res))
	}
	out, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("CallTool(%s): structured content is %T, want map", tool, res.StructuredContent)
	}
	if out["success"] != true {
		t.Fatalf("CallTool(%s): success = %v", tool, out["success"])
	}
	return out
}

func resultText(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func TestListToolsUnchanged(t *testing.T) {
	s := newTestServer(t)
	cs := connect(t, s)

	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var got []string
	for _, tool := range res.Tools {
		got = append(got, tool.Name)
	}
	slices.Sort(got)
	want := []string{
		"create_collection", "create_database", "create_index", "current_database",
		"delete_database", "delete_document", "find_documents", "insert_document",
		"list_collections", "list_databases", "update_document", "use_database",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
}

func TestPerSessionDatabase(t *testing.T) {
	s := newTestServer(t)
	a, b, c := connect(t, s), connect(t, s), connect(t, s)

	for _, name := range []string{"db1", "db2"} {
		callOK(t, a, "create_database", map[string]any{"name": name})
	}
	callOK(t, a, "use_database", map[string]any{"name": "db1"})
	callOK(t, b, "use_database", map[string]any{"name": "db2"})

	tests := []struct {
		name    string
		session *mcp.ClientSession
		want    string
	}{
		{"session A sees its own choice", a, "db1"},
		{"session B sees its own choice", b, "db2"},
		{"session C falls back to server default", c, testDefaultDB},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := callOK(t, tt.session, "current_database", nil)
			if out["current_database"] != tt.want {
				t.Fatalf("current_database = %v, want %q", out["current_database"], tt.want)
			}
		})
	}

	// Resolution order: input.database, then session choice, then default.
	callOK(t, a, "create_collection", map[string]any{"name": "from_session"})
	callOK(t, a, "create_collection", map[string]any{"name": "explicit", "database": "db2"})
	callOK(t, c, "create_collection", map[string]any{"name": "from_default"})

	collTests := []struct {
		name     string
		session  *mcp.ClientSession
		args     map[string]any
		wantDB   string
		wantColl []any
	}{
		{"session choice", a, nil, "db1", []any{"from_session"}},
		{"explicit database wins over session", a, map[string]any{"database": "db2"}, "db2", []any{"explicit"}},
		{"session B choice", b, nil, "db2", []any{"explicit"}},
		{"server default", c, nil, testDefaultDB, []any{"from_default"}},
	}
	for _, tt := range collTests {
		t.Run(tt.name, func(t *testing.T) {
			out := callOK(t, tt.session, "list_collections", tt.args)
			if out["database"] != tt.wantDB {
				t.Errorf("database = %v, want %q", out["database"], tt.wantDB)
			}
			got, _ := out["collections"].([]any)
			if !slices.Equal(got, tt.wantColl) {
				t.Errorf("collections = %v, want %v", got, tt.wantColl)
			}
		})
	}
}

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func sessionCount(s *Server) int {
	n := 0
	s.sessions.Range(func(_, _ any) bool { n++; return true })
	return n
}

func TestSessionRemovedWhenClientDisconnects(t *testing.T) {
	s := newTestServer(t)
	a, b := connect(t, s), connect(t, s)
	callOK(t, a, "current_database", nil)
	callOK(t, b, "current_database", nil)
	waitFor(t, "two tracked sessions", func() bool { return sessionCount(s) == 2 })

	if err := a.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	waitFor(t, "session removal", func() bool { return sessionCount(s) == 1 })

	out := callOK(t, b, "current_database", nil)
	if out["current_database"] != testDefaultDB {
		t.Fatalf("current_database = %v, want %q", out["current_database"], testDefaultDB)
	}
}

func TestCloseEndsSessions(t *testing.T) {
	s := newTestServer(t)
	cs := connect(t, s)
	callOK(t, cs, "use_database", map[string]any{"name": testDefaultDB})
	waitFor(t, "tracked session", func() bool { return sessionCount(s) == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := sessionCount(s); n != 0 {
		t.Fatalf("sessions after Close = %d, want 0", n)
	}
	done := make(chan struct{})
	go func() {
		_ = cs.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("client session still open after Server.Close")
	}
	// Idempotent.
	if err := s.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestEndToEndToolFlow(t *testing.T) {
	s := newTestServer(t)
	cs := connect(t, s)

	callOK(t, cs, "create_database", map[string]any{"name": "shop"})
	out := callOK(t, cs, "list_databases", nil)
	if dbs, _ := out["databases"].([]any); !slices.Equal(dbs, []any{testDefaultDB, "shop"}) {
		t.Fatalf("databases = %v", out["databases"])
	}
	callOK(t, cs, "use_database", map[string]any{"name": "shop"})
	callOK(t, cs, "create_collection", map[string]any{
		"name": "users",
		"schema": map[string]any{"fields": map[string]any{
			"name": map[string]any{"type": "string", "required": true},
			"age":  map[string]any{"type": "number"},
		}},
	})
	callOK(t, cs, "create_index", map[string]any{"collection": "users", "index_name": "by_age", "field_name": "age"})

	out = callOK(t, cs, "insert_document", map[string]any{
		"collection": "users",
		"document":   map[string]any{"_id": "alice", "name": "Alice", "age": 30},
	})
	if out["id"] != "alice" {
		t.Fatalf("insert id = %v, want alice", out["id"])
	}
	out = callOK(t, cs, "insert_document", map[string]any{
		"collection": "users",
		"document":   map[string]any{"name": "Bob", "age": 25},
	})
	bobID, _ := out["id"].(string)
	if bobID == "" {
		t.Fatalf("generated id missing: %v", out)
	}

	find := func(t *testing.T, filters []any) []any {
		t.Helper()
		out := callOK(t, cs, "find_documents", map[string]any{
			"collection": "users",
			"query":      map[string]any{"filters": filters},
		})
		docs, _ := out["documents"].([]any)
		if got := out["count"]; got != float64(len(docs)) {
			t.Fatalf("count = %v, documents = %d", got, len(docs))
		}
		return docs
	}

	findTests := []struct {
		name    string
		filters []any
		wantIDs []string
	}{
		{"indexed eq", []any{map[string]any{"field": "age", "operator": "eq", "value": 30}}, []string{"alice"}},
		{"range", []any{map[string]any{"field": "age", "operator": "lt", "value": 28}}, []string{bobID}},
		{"by _id", []any{map[string]any{"field": "_id", "operator": "eq", "value": "alice"}}, []string{"alice"}},
		{"no match", []any{map[string]any{"field": "name", "operator": "eq", "value": "Zed"}}, nil},
	}
	for _, tt := range findTests {
		t.Run("find "+tt.name, func(t *testing.T) {
			docs := find(t, tt.filters)
			var ids []string
			for _, d := range docs {
				ids = append(ids, d.(map[string]any)["_id"].(string))
			}
			if !slices.Equal(ids, tt.wantIDs) {
				t.Fatalf("ids = %v, want %v", ids, tt.wantIDs)
			}
		})
	}

	// limit/skip are honored.
	out = callOK(t, cs, "find_documents", map[string]any{
		"collection": "users",
		"query":      map[string]any{"limit": 1, "skip": 1},
	})
	if out["count"] != float64(1) {
		t.Fatalf("paged count = %v, want 1", out["count"])
	}

	callOK(t, cs, "update_document", map[string]any{
		"collection": "users", "id": "alice", "updates": map[string]any{"age": 31, "city": "Yerevan"},
	})
	docs := find(t, []any{map[string]any{"field": "age", "operator": "eq", "value": 31}})
	if len(docs) != 1 {
		t.Fatalf("after update: %d docs, want 1", len(docs))
	}
	alice := docs[0].(map[string]any)
	if alice["_id"] != "alice" || alice["name"] != "Alice" || alice["city"] != "Yerevan" {
		t.Fatalf("updated doc = %v", alice)
	}

	callOK(t, cs, "delete_document", map[string]any{"collection": "users", "id": "alice"})
	if docs := find(t, nil); len(docs) != 1 || docs[0].(map[string]any)["_id"] != bobID {
		t.Fatalf("after delete: %v", docs)
	}

	callOK(t, cs, "delete_database", map[string]any{"name": "shop"})
	out = callOK(t, cs, "list_databases", nil)
	if dbs, _ := out["databases"].([]any); !slices.Equal(dbs, []any{testDefaultDB}) {
		t.Fatalf("databases after delete = %v", out["databases"])
	}
}

func TestToolErrors(t *testing.T) {
	s := newTestServer(t)
	cs := connect(t, s)
	callOK(t, cs, "create_collection", map[string]any{
		"name":   "items",
		"schema": map[string]any{"fields": map[string]any{"name": map[string]any{"type": "string", "required": true}}},
	})
	callOK(t, cs, "insert_document", map[string]any{"collection": "items", "document": map[string]any{"_id": "x", "name": "X"}})
	callOK(t, cs, "create_index", map[string]any{"collection": "items", "index_name": "dup", "field_name": "name"})

	tests := []struct {
		name     string
		tool     string
		args     map[string]any
		wantText string
	}{
		{"use missing database", "use_database", map[string]any{"name": "nope"}, "not found (hint: list_databases"},
		{"delete missing database", "delete_database", map[string]any{"name": "nope"}, "not found"},
		{"invalid database name", "create_database", map[string]any{"name": "bad/name"}, "invalid name"},
		{"explicit missing database", "list_collections", map[string]any{"database": "nope"}, "not found"},
		{"duplicate collection", "create_collection", map[string]any{"name": "items"}, "already exists"},
		{"missing collection", "insert_document", map[string]any{"collection": "nope", "document": map[string]any{"a": 1}}, "not found"},
		{"duplicate id", "insert_document", map[string]any{"collection": "items", "document": map[string]any{"_id": "x", "name": "Y"}}, "already exists"},
		{"schema violation", "insert_document", map[string]any{"collection": "items", "document": map[string]any{"name": 5}}, "schema validation"},
		{"schema violation hint", "insert_document", map[string]any{"collection": "items", "document": map[string]any{"name": 5}}, "cachydb://docs/schema"},
		{"update missing document", "update_document", map[string]any{"collection": "items", "id": "nope", "updates": map[string]any{"name": "Z"}}, "not found"},
		{"update _id", "update_document", map[string]any{"collection": "items", "id": "x", "updates": map[string]any{"_id": "y"}}, "schema validation"},
		{"delete missing document", "delete_document", map[string]any{"collection": "items", "id": "nope"}, "not found"},
		{"duplicate index", "create_index", map[string]any{"collection": "items", "index_name": "dup", "field_name": "name"}, "already exists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := call(t, cs, tt.tool, tt.args)
			if !res.IsError {
				t.Fatalf("IsError = false, want tool error; content %q", resultText(res))
			}
			if text := resultText(res); !strings.Contains(text, tt.wantText) {
				t.Fatalf("error %q does not contain %q", text, tt.wantText)
			}
		})
	}
}

func TestHTTPPerSessionDatabase(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s.httpHandler())
	t.Cleanup(ts.Close)

	newHTTPClient := func() *mcp.ClientSession {
		return connectClient(t, &mcp.StreamableClientTransport{Endpoint: ts.URL + "/mcp", MaxRetries: -1})
	}
	a, b := newHTTPClient(), newHTTPClient()
	callOK(t, a, "create_database", map[string]any{"name": "http1"})
	callOK(t, a, "use_database", map[string]any{"name": "http1"})

	for _, tt := range []struct {
		name    string
		session *mcp.ClientSession
		want    string
	}{
		{"session that switched", a, "http1"},
		{"other session unaffected", b, testDefaultDB},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out := callOK(t, tt.session, "current_database", nil)
			if out["current_database"] != tt.want {
				t.Fatalf("current_database = %v, want %q", out["current_database"], tt.want)
			}
		})
	}
}

func TestStartHTTPStopsOnCancel(t *testing.T) {
	s := newTestServer(t)
	s.transport = "http"
	s.httpAddr = "127.0.0.1:0"
	var logBuf bytes.Buffer
	s.stderr = &logBuf

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx) }()

	// Give the listener a moment, then stop.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
	if !strings.Contains(logBuf.String(), "listening on http://127.0.0.1:") {
		t.Fatalf("startup log %q missing listen address", logBuf.String())
	}
}

func TestNewServerCreatesDefaultDatabase(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.engine.Database(testDefaultDB); err != nil {
		t.Fatalf("default database: %v", err)
	}
	if _, err := NewServer(context.Background(), nil, testDefaultDB, "stdio", "", testVersion); err == nil {
		t.Fatal("NewServer(nil engine) = nil error, want error")
	}
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s.httpHandler())
	t.Cleanup(ts.Close)

	do := func(t *testing.T, method string) (int, http.Header, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), method, ts.URL+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("%s /healthz: %v", method, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("reading body: %v", err)
		}
		return resp.StatusCode, resp.Header, string(body)
	}

	t.Run("open engine", func(t *testing.T) {
		code, h, body := do(t, http.MethodGet)
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %q)", code, body)
		}
		if ct := h.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}
		var got map[string]string
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatalf("body %q: %v", body, err)
		}
		if want := map[string]string{"status": "ok", "version": testVersion}; !maps.Equal(got, want) {
			t.Fatalf("body = %v, want %v", got, want)
		}
	})

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodHead} {
		t.Run("method "+method, func(t *testing.T) {
			code, h, _ := do(t, method)
			if code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405", code)
			}
			if allow := h.Get("Allow"); allow != http.MethodGet {
				t.Errorf("Allow = %q, want GET", allow)
			}
		})
	}

	t.Run("closed engine", func(t *testing.T) {
		if err := s.engine.Close(context.Background()); err != nil {
			t.Fatalf("Engine.Close: %v", err)
		}
		code, _, body := do(t, http.MethodGet)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 (body %q)", code, body)
		}
		if strings.TrimSpace(body) != `{"status":"unavailable"}` {
			t.Fatalf("body = %q", body)
		}
	})
}
