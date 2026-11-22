package mcpserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/hop-/cachydb/pkg/db"
	sdkauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The shared database of the access tests and its fixtures.
const (
	accessDB    = "shared"
	accessColl  = "items"
	accessIndex = "by_x"
	accessOther = "other"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// authEnv is a server that requires tokens, served over a loopback
// httptest server, with its engine, auth store, and log.
type authEnv struct {
	s      *Server
	engine *db.Engine
	users  *auth.Store
	ts     *httptest.Server
	log    *syncBuffer
	tokens map[string]string // user name -> token
}

// newAuthEnv builds a Server with Config{Transport: "http",
// TokensRequired: true} and a server admin "root". Cleanup closes the
// server, then the engine.
func newAuthEnv(t *testing.T) *authEnv {
	t.Helper()
	ctx := context.Background()
	engine, err := db.Open(ctx, db.Options{Dir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	users, err := auth.Open(ctx, engine)
	if err != nil {
		_ = engine.Close(ctx)
		t.Fatalf("auth.Open: %v", err)
	}
	env := &authEnv{engine: engine, users: users, log: &syncBuffer{}, tokens: map[string]string{}}
	s, err := New(ctx, engine, Config{
		DefaultDatabase: testDefaultDB,
		Transport:       "http",
		Version:         testVersion,
		TokensRequired:  true,
		Users:           users,
		Logger:          slog.New(slog.NewTextHandler(env.log, nil)),
	})
	if err != nil {
		_ = engine.Close(ctx)
		t.Fatalf("New: %v", err)
	}
	s.stderr = io.Discard
	env.s = s
	env.ts = httptest.NewServer(s.httpHandler())
	t.Cleanup(func() {
		env.ts.Close()
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Close(closeCtx); err != nil {
			t.Errorf("Server.Close: %v", err)
		}
		if err := engine.Close(closeCtx); err != nil {
			t.Errorf("Engine.Close: %v", err)
		}
	})
	env.addUser(t, "root", auth.UserOptions{ServerAdmin: true})
	return env
}

func (env *authEnv) addUser(t *testing.T, name string, opts auth.UserOptions) string {
	t.Helper()
	tok, err := env.users.CreateUser(context.Background(), name, opts)
	if err != nil {
		t.Fatalf("CreateUser(%s): %v", name, err)
	}
	env.tokens[name] = tok
	return tok
}

// bearer adds a fixed Authorization header to every request.
type bearer struct {
	token string
	base  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}

// session opens an MCP session over HTTP with token.
func (env *authEnv) session(t *testing.T, token string) *mcp.ClientSession {
	t.Helper()
	return connectClient(t, &mcp.StreamableClientTransport{
		Endpoint:   env.ts.URL + "/mcp",
		MaxRetries: -1,
		HTTPClient: &http.Client{Transport: bearer{token: token, base: http.DefaultTransport}},
	})
}

// as opens a session as the user name.
func (env *authEnv) as(t *testing.T, name string) *mcp.ClientSession {
	t.Helper()
	return env.session(t, env.tokens[name])
}

// setAccess makes the stored user name have exactly grants and the given
// server-admin flag.
func (env *authEnv) setAccess(t *testing.T, name string, serverAdmin bool, grants map[string]auth.Level) {
	t.Helper()
	ctx := context.Background()
	u, err := env.users.User(name)
	if err != nil {
		t.Fatal(err)
	}
	for database := range u.Grants {
		if _, keep := grants[database]; !keep {
			if err := env.users.SetAccess(ctx, name, auth.AccessChange{Database: database, Level: auth.LevelNone}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for database, l := range grants {
		if err := env.users.Grant(ctx, name, database, l); err != nil {
			t.Fatal(err)
		}
	}
	if err := env.users.SetServerAdmin(ctx, name, serverAdmin); err != nil {
		t.Fatal(err)
	}
}

// resetFixtures recreates the shared and other databases: collection
// items with document d1 {x: 1} and index by_x, and drops newdb. Grants
// are left alone (the engine is used directly, not delete_database).
func (env *authEnv) resetFixtures(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{accessDB, accessOther, "newdb"} {
		if err := env.engine.DropDatabase(ctx, name); err != nil && !errors.Is(err, db.ErrNotFound) {
			t.Fatal(err)
		}
	}
	for _, name := range []string{accessDB, accessOther} {
		d, err := env.engine.CreateDatabase(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.CreateCollection(ctx, accessColl, nil); err != nil {
			t.Fatal(err)
		}
		c, err := d.Collection(accessColl)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Insert(ctx, &db.Document{ID: "d1", Data: map[string]any{"x": 1}}); err != nil {
			t.Fatal(err)
		}
		if err := c.CreateIndex(ctx, accessIndex, "x"); err != nil {
			t.Fatal(err)
		}
	}
}

// fingerprint describes the engine and user state, to show that a
// denied call changed nothing.
func (env *authEnv) fingerprint(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, name := range env.engine.ListDatabases() {
		d, err := env.engine.Database(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, cn := range d.ListCollections() {
			c, err := d.Collection(cn)
			if err != nil {
				t.Fatal(err)
			}
			docs, err := c.Find(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "%s/%s:", name, cn)
			for _, doc := range docs {
				fmt.Fprintf(&b, " %s=%v", doc.ID, doc.Data)
			}
			fmt.Fprintf(&b, " idx=%v\n", c.Indexes())
		}
	}
	for _, u := range env.users.Users() {
		fmt.Fprintf(&b, "user %s admin=%v default=%q grants=%v tokens=%v\n", u.Name, u.ServerAdmin, u.DefaultDatabase, u.Grants, u.Tokens)
	}
	return b.String()
}

// wantDenied requires res to be the permission_denied tool error naming
// what.
func wantDenied(t *testing.T, res *mcp.CallToolResult, what ...string) {
	t.Helper()
	text := resultText(res)
	if !res.IsError {
		t.Fatalf("call succeeded, want permission denied: %s", text)
	}
	if !strings.Contains(text, auth.ErrPermissionDenied.Error()) {
		t.Fatalf("error %q does not say %q", text, auth.ErrPermissionDenied)
	}
	for _, w := range what {
		if !strings.Contains(text, w) {
			t.Errorf("error %q does not name %q", text, w)
		}
	}
}

// registeredAccess returns every tool spec by name.
func registeredAccess() map[string]toolSpec {
	out := map[string]toolSpec{}
	for _, sp := range slices.Concat(toolSpecs(), accessToolSpecs()) {
		out[sp.tool.Name] = sp
	}
	return out
}

// TestAccessTableEveryTool proves the access level of every registered
// tool (approach-access-control §4) with two users and one shared
// database: alice holds the level the call needs, bob one level below.
// bob's call is refused with permission_denied naming the database and
// the level and changes nothing; alice's call succeeds. The table must
// list every registered tool, so a tool added without a row fails here.
func TestAccessTableEveryTool(t *testing.T) {
	env := newAuthEnv(t)
	env.addUser(t, "alice", auth.UserOptions{})
	env.addUser(t, "bob", auth.UserOptions{})
	alice, bob := env.as(t, "alice"), env.as(t, "bob")
	ctx := context.Background()

	ensureCarol := func(t *testing.T) {
		t.Helper()
		if _, err := env.users.User("carol"); err != nil {
			env.addUser(t, "carol", auth.UserOptions{})
		}
	}
	dropCarol := func(t *testing.T) {
		t.Helper()
		if err := env.users.DeleteUser(ctx, "carol"); err != nil && !errors.Is(err, db.ErrNotFound) {
			t.Fatal(err)
		}
	}
	in := func(extra map[string]any) map[string]any {
		args := map[string]any{"database": accessDB, "collection": accessColl}
		maps.Copy(args, extra)
		return args
	}

	type row struct {
		tool string
		// registered is the level newSpec registers the tool with; need is
		// the level this call needs on accessDB (they differ for
		// use_database, which checks read on its target itself, and
		// copy_collection, whose empty target_database needs admin on the
		// source).
		registered, need auth.Level
		args             map[string]any
		setup            func(t *testing.T)
	}
	rows := []row{
		{tool: "list_databases", registered: auth.LevelNone, need: auth.LevelNone, args: map[string]any{}},
		{tool: "current_database", registered: auth.LevelNone, need: auth.LevelNone, args: map[string]any{}},
		{tool: "whoami", registered: auth.LevelNone, need: auth.LevelNone, args: map[string]any{}},
		{tool: "use_database", registered: auth.LevelNone, need: auth.LevelRead, args: map[string]any{"name": accessDB}},

		{tool: "list_collections", registered: auth.LevelRead, need: auth.LevelRead, args: map[string]any{"database": accessDB}},
		{tool: "describe_collection", registered: auth.LevelRead, need: auth.LevelRead, args: in(nil)},
		{tool: "find_documents", registered: auth.LevelRead, need: auth.LevelRead, args: in(nil)},
		{tool: "count_documents", registered: auth.LevelRead, need: auth.LevelRead, args: in(nil)},
		{tool: "distinct", registered: auth.LevelRead, need: auth.LevelRead, args: in(map[string]any{"field": "x"})},
		{tool: "get_document", registered: auth.LevelRead, need: auth.LevelRead, args: in(map[string]any{"id": "d1"})},
		{tool: "server_stats", registered: auth.LevelRead, need: auth.LevelRead, args: map[string]any{"database": accessDB}},

		{tool: "insert_document", registered: auth.LevelWrite, need: auth.LevelWrite, args: in(map[string]any{"document": map[string]any{"_id": "d2", "x": 2}})},
		{tool: "update_document", registered: auth.LevelWrite, need: auth.LevelWrite, args: in(map[string]any{"id": "d1", "updates": map[string]any{"x": 5}})},
		{tool: "replace_document", registered: auth.LevelWrite, need: auth.LevelWrite, args: in(map[string]any{"id": "d1", "document": map[string]any{"x": 6}})},
		{tool: "delete_document", registered: auth.LevelWrite, need: auth.LevelWrite, args: in(map[string]any{"id": "d1"})},
		{tool: "insert_documents", registered: auth.LevelWrite, need: auth.LevelWrite, args: in(map[string]any{"documents": []any{map[string]any{"_id": "d2", "x": 2}}})},
		{tool: "update_documents", registered: auth.LevelWrite, need: auth.LevelWrite, args: in(map[string]any{"query": map[string]any{"filters": []any{map[string]any{"field": "x", "operator": "eq", "value": 1}}}, "updates": map[string]any{"x": 5}})},
		{tool: "delete_documents", registered: auth.LevelWrite, need: auth.LevelWrite, args: in(map[string]any{"query": map[string]any{"filters": []any{map[string]any{"field": "x", "operator": "eq", "value": 1}}}})},

		{tool: "create_collection", registered: auth.LevelAdmin, need: auth.LevelAdmin, args: map[string]any{"database": accessDB, "name": "fresh"}},
		{tool: "update_schema", registered: auth.LevelAdmin, need: auth.LevelAdmin, args: in(map[string]any{"schema": map[string]any{"fields": map[string]any{"x": map[string]any{"type": "number"}}}})},
		{tool: "rename_collection", registered: auth.LevelAdmin, need: auth.LevelAdmin, args: in(map[string]any{"new_name": "renamed"})},
		{tool: "copy_collection", registered: auth.LevelRead, need: auth.LevelAdmin, args: in(map[string]any{"new_name": "copied"})},
		{tool: "delete_collection", registered: auth.LevelAdmin, need: auth.LevelAdmin, args: in(nil)},
		{tool: "create_index", registered: auth.LevelAdmin, need: auth.LevelAdmin, args: in(map[string]any{"index_name": "by_y", "field_name": "y"})},
		{tool: "drop_index", registered: auth.LevelAdmin, need: auth.LevelAdmin, args: in(map[string]any{"index_name": accessIndex})},

		{tool: "create_database", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{"name": "newdb"}},
		{tool: "delete_database", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{"name": accessDB}},
		{tool: "list_users", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{}},
		{tool: "create_user", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{"name": "carol"}, setup: dropCarol},
		{tool: "set_access", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{"user": "carol", "database": accessDB, "level": "read"}, setup: ensureCarol},
		{tool: "create_token", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{"user": "carol", "label": "laptop"}, setup: func(t *testing.T) {
			ensureCarol(t)
			if err := env.users.RevokeToken(ctx, "carol", "laptop"); err != nil && !errors.Is(err, db.ErrNotFound) {
				t.Fatal(err)
			}
		}},
		{tool: "revoke_token", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{"user": "carol", "label": "spare"}, setup: func(t *testing.T) {
			ensureCarol(t)
			if _, _, err := env.users.CreateToken(ctx, "carol", "spare"); err != nil && !errors.Is(err, db.ErrExists) {
				t.Fatal(err)
			}
		}},
		{tool: "delete_user", registered: auth.LevelServer, need: auth.LevelServer, args: map[string]any{"name": "carol"}, setup: ensureCarol},
	}

	// The table covers exactly the registered tools: what the server lists
	// to a server admin, and every spec.
	specs := registeredAccess()
	var listed []string
	for _, tool := range listTools(t, env.as(t, "root")) {
		listed = append(listed, tool.Name)
	}
	var tabled []string
	for _, r := range rows {
		tabled = append(tabled, r.tool)
	}
	slices.Sort(listed)
	slices.Sort(tabled)
	if len(listed) != 33 || !slices.Equal(listed, tabled) || !slices.Equal(slices.Sorted(maps.Keys(specs)), tabled) {
		t.Fatalf("access table rows %v\nregistered tools %v\nspecs %v\n(add a row for every new tool)", tabled, listed, slices.Sorted(maps.Keys(specs)))
	}

	for _, r := range rows {
		t.Run(r.tool, func(t *testing.T) {
			sp := specs[r.tool]
			if !sp.access.set || sp.access.level != r.registered {
				t.Fatalf("%s registered with %+v, want level %s", r.tool, sp.access, r.registered)
			}
			env.resetFixtures(t)
			if r.setup != nil {
				r.setup(t)
			}
			// alice holds exactly the level needed, bob one below.
			switch r.need {
			case auth.LevelNone:
				env.setAccess(t, "alice", false, nil)
				env.setAccess(t, "bob", false, nil)
			case auth.LevelServer:
				env.setAccess(t, "alice", true, nil)
				env.setAccess(t, "bob", false, map[string]auth.Level{auth.AllDatabases: auth.LevelAdmin, accessDB: auth.LevelAdmin})
			default:
				env.setAccess(t, "alice", false, map[string]auth.Level{accessDB: r.need})
				var below map[string]auth.Level
				if r.need > auth.LevelRead {
					below = map[string]auth.Level{accessDB: r.need - 1}
				}
				env.setAccess(t, "bob", false, below)
			}

			if r.need != auth.LevelNone {
				before := env.fingerprint(t)
				res := call(t, bob, r.tool, r.args)
				if r.need == auth.LevelServer {
					wantDenied(t, res, `"bob"`, "server admin")
				} else {
					wantDenied(t, res, `"bob"`, fmt.Sprintf("needs %s access to database %q", r.need, accessDB))
				}
				if after := env.fingerprint(t); after != before {
					t.Fatalf("denied call changed state:\nbefore %s\nafter  %s", before, after)
				}
			} else {
				callOK(t, bob, r.tool, r.args)
			}
			callOK(t, alice, r.tool, r.args)
		})
	}
}

// TestAccessCopyCollection: copy_collection needs read on the source and
// admin on the target, for an explicit target_database and for an empty
// one (the source database is then the target).
func TestAccessCopyCollection(t *testing.T) {
	env := newAuthEnv(t)
	env.addUser(t, "alice", auth.UserOptions{})
	env.addUser(t, "bob", auth.UserOptions{})
	alice, bob := env.as(t, "alice"), env.as(t, "bob")
	explicit := map[string]any{"database": accessDB, "collection": accessColl, "new_name": "copied", "target_database": accessOther}
	empty := map[string]any{"database": accessDB, "collection": accessColl, "new_name": "copied"}
	L := func(src, dst auth.Level) map[string]auth.Level {
		m := map[string]auth.Level{}
		if src != auth.LevelNone {
			m[accessDB] = src
		}
		if dst != auth.LevelNone {
			m[accessOther] = dst
		}
		return m
	}

	for _, tt := range []struct {
		name   string
		args   map[string]any
		alice  map[string]auth.Level // allowed
		bob    map[string]auth.Level // denied
		denied []string
	}{
		{"explicit target, admin missing on target", explicit, L(auth.LevelRead, auth.LevelAdmin), L(auth.LevelAdmin, auth.LevelWrite),
			[]string{`admin access to database "other"`}},
		{"explicit target, read missing on source", explicit, L(auth.LevelRead, auth.LevelAdmin), L(auth.LevelNone, auth.LevelAdmin),
			[]string{`read access to database "shared"`}},
		{"empty target needs admin on source", empty, L(auth.LevelAdmin, auth.LevelNone), L(auth.LevelWrite, auth.LevelAdmin),
			[]string{`admin access to database "shared"`}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env.resetFixtures(t)
			env.setAccess(t, "alice", false, tt.alice)
			env.setAccess(t, "bob", false, tt.bob)
			before := env.fingerprint(t)
			wantDenied(t, call(t, bob, "copy_collection", tt.args), tt.denied...)
			if after := env.fingerprint(t); after != before {
				t.Fatalf("denied copy changed state:\n%s\n%s", before, after)
			}
			out := callOK(t, alice, "copy_collection", tt.args)
			if out["copied"] != float64(1) {
				t.Fatalf("copied = %v, want 1", out["copied"])
			}
		})
	}
}

// TestAccessDeniedWrapsPermissionDenied calls authorizeCall directly: a
// denied call's error wraps auth.ErrPermissionDenied and names the
// database and the level; an unknown user wraps ErrUnauthenticated; a
// tool without an access level is denied for everyone, even a server
// admin.
func TestAccessDeniedWrapsPermissionDenied(t *testing.T) {
	env := newAuthEnv(t)
	env.addUser(t, "bob", auth.UserOptions{})
	env.setAccess(t, "bob", false, map[string]auth.Level{accessDB: auth.LevelRead})
	reqAs := func(user string) *mcp.CallToolRequest {
		return &mcp.CallToolRequest{Extra: &mcp.RequestExtra{TokenInfo: &sdkauth.TokenInfo{UserID: user}}}
	}
	ctx := context.Background()

	_, err := env.s.authorizeCall(ctx, reqAs("bob"), "insert_document", accessWrite, &InsertDocumentInput{Database: accessDB})
	if !errors.Is(err, auth.ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
	for _, w := range []string{"insert document", `"shared"`, "write"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error %q does not name %q", err, w)
		}
	}

	in := &FindDocumentsInput{Database: accessDB}
	got, err := env.s.authorizeCall(ctx, reqAs("bob"), "find_documents", accessRead, in)
	if err != nil {
		t.Fatalf("read with a read grant: %v", err)
	}
	if p, ok := principalFrom(got); !ok || p.Name != "bob" {
		t.Fatalf("principal in ctx = %+v, %v", p, ok)
	}

	if _, err := env.s.authorizeCall(ctx, reqAs("ghost"), "find_documents", accessRead, &FindDocumentsInput{}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("unknown user: %v, want ErrUnauthenticated", err)
	}
	if _, err := env.s.authorizeCall(ctx, reqAs("root"), "find_documents", toolAccess{}, &FindDocumentsInput{}); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Fatalf("tool without access level: %v, want ErrPermissionDenied", err)
	}
	// An HTTP call without a token on a server that needs one.
	if _, err := env.s.authorizeCall(ctx, &mcp.CallToolRequest{}, "list_databases", accessNone, &ListDatabasesInput{}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("no token: %v, want ErrUnauthenticated", err)
	}
	// The default database is resolved and written back.
	in = &FindDocumentsInput{}
	if _, err := env.s.authorizeCall(ctx, reqAs("root"), "find_documents", accessRead, in); err != nil || in.Database != testDefaultDB {
		t.Fatalf("resolved database = %q, %v; want %q", in.Database, err, testDefaultDB)
	}
}

// TestAccessToolsValidate: a tool registered without an access level, with
// an unknown level, or with a level that does not fit its input fails
// validation (so the server does not start); every real spec passes.
func TestAccessToolsValidate(t *testing.T) {
	noop := func(*Server) mcp.ToolHandlerFor[FindDocumentsInput, MessageOutput] { return nil }
	noDB := func(*Server) mcp.ToolHandlerFor[ListDatabasesInput, MessageOutput] { return nil }
	for _, tt := range []struct {
		name string
		sp   toolSpec
		want string
	}{
		{"no access level", newSpec("t", "d", readOnly("t"), toolAccess{}, nil, noop), "no access level"},
		{"zero toolAccess with a level but unset", newSpec("t", "d", readOnly("t"), toolAccess{level: auth.LevelRead}, nil, noop), "no access level"},
		{"unknown level", newSpec("t", "d", readOnly("t"), toolAccess{level: auth.LevelServer + 1, set: true}, nil, noop), "unknown access level"},
		{"negative level", newSpec("t", "d", readOnly("t"), toolAccess{level: -1, set: true}, nil, noDB), "unknown access level"},
		{"database level without database argument", newSpec("t", "d", readOnly("t"), accessRead, nil, noDB), "no database argument"},
		{"database argument with level none", newSpec("t", "d", readOnly("t"), accessNone, nil, noop), "does not check it"},
		{"database argument with level server", newSpec("t", "d", readOnly("t"), accessServer, nil, noop), "does not check it"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sp.validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate = %v, want an error containing %q", err, tt.want)
			}
		})
	}
	for name, sp := range registeredAccess() {
		if err := sp.validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A registered tool's spec has the databaseArg flag its input implies.
	if sp := registeredAccess()["find_documents"]; !sp.databaseArg {
		t.Error("find_documents has no database argument flag")
	}
	if sp := registeredAccess()["create_database"]; sp.databaseArg {
		t.Error("create_database has a database argument flag")
	}
}

// TestAccessListDatabasesFiltered: with tokens, list_databases returns
// only the databases the principal can read; a grant on * shows every
// database; a server admin sees all of them. _system is never listed.
func TestAccessListDatabasesFiltered(t *testing.T) {
	env := newAuthEnv(t)
	env.resetFixtures(t)
	env.addUser(t, "alice", auth.UserOptions{})
	env.addUser(t, "star", auth.UserOptions{})
	env.addUser(t, "none", auth.UserOptions{})
	env.setAccess(t, "alice", false, map[string]auth.Level{accessDB: auth.LevelRead, "ghost": auth.LevelAdmin})
	env.setAccess(t, "star", false, map[string]auth.Level{auth.AllDatabases: auth.LevelRead})
	all := []any{testDefaultDB, accessOther, accessDB}

	for _, tt := range []struct {
		user string
		want []any
	}{
		{"alice", []any{accessDB}},
		{"star", all},
		{"root", all},
		{"none", []any{}},
	} {
		t.Run(tt.user, func(t *testing.T) {
			out := callOK(t, env.as(t, tt.user), "list_databases", nil)
			got, _ := out["databases"].([]any)
			if got == nil {
				got = []any{}
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("databases = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAccessUseDatabaseSameDenial: use_database refuses an unreadable
// database and a nonexistent one with the same permission error, so the
// answer does not reveal which databases exist; with read access a
// nonexistent database is "not found".
func TestAccessUseDatabaseSameDenial(t *testing.T) {
	env := newAuthEnv(t)
	env.resetFixtures(t)
	env.addUser(t, "bob", auth.UserOptions{})
	env.setAccess(t, "bob", false, map[string]auth.Level{"missing2": auth.LevelRead})
	bob := env.as(t, "bob")

	existing := resultText(call(t, bob, "use_database", map[string]any{"name": accessDB}))
	missing := resultText(call(t, bob, "use_database", map[string]any{"name": "missing"}))
	norm := func(s string) string { return strings.ReplaceAll(s, `"missing"`, `"shared"`) }
	if !strings.Contains(existing, "permission denied") || norm(missing) != existing {
		t.Fatalf("unreadable: %q\nnonexistent: %q\nwant the same permission error", existing, missing)
	}
	res := call(t, bob, "use_database", map[string]any{"name": "missing2"})
	if !res.IsError || !strings.Contains(resultText(res), "not found") {
		t.Fatalf("readable but missing: %s", resultText(res))
	}
}

// TestAccessDefaultDatabaseOrder: the database a call uses is the
// argument, then the session's use_database choice, then the user's
// default database, then the server default.
func TestAccessDefaultDatabaseOrder(t *testing.T) {
	env := newAuthEnv(t)
	env.resetFixtures(t)
	env.addUser(t, "withdef", auth.UserOptions{DefaultDatabase: accessOther})
	env.addUser(t, "nodef", auth.UserOptions{})
	all := map[string]auth.Level{auth.AllDatabases: auth.LevelRead}
	env.setAccess(t, "withdef", false, all)
	env.setAccess(t, "nodef", false, all)
	current := func(cs *mcp.ClientSession) any { return callOK(t, cs, "current_database", nil)["current_database"] }
	listed := func(cs *mcp.ClientSession, args map[string]any) any {
		return callOK(t, cs, "list_collections", args)["database"]
	}

	nodef := env.as(t, "nodef")
	if got := current(nodef); got != testDefaultDB {
		t.Fatalf("no default: current = %v, want server default", got)
	}
	withdef := env.as(t, "withdef")
	if got := current(withdef); got != accessOther {
		t.Fatalf("user default: current = %v, want %s", got, accessOther)
	}
	if got := listed(withdef, nil); got != accessOther {
		t.Fatalf("user default: list_collections on %v", got)
	}
	callOK(t, withdef, "use_database", map[string]any{"name": accessDB})
	if got := current(withdef); got != accessDB {
		t.Fatalf("session choice: current = %v, want %s", got, accessDB)
	}
	if got := listed(withdef, nil); got != accessDB {
		t.Fatalf("session choice: list_collections on %v", got)
	}
	if got := listed(withdef, map[string]any{"database": testDefaultDB}); got != testDefaultDB {
		t.Fatalf("argument: list_collections on %v", got)
	}
	// Another session of the same user still uses the user default.
	if got := current(env.as(t, "withdef")); got != accessOther {
		t.Fatalf("second session: current = %v", got)
	}
}

// grantsOf returns the stored grants of user.
func grantsOf(t *testing.T, users *auth.Store, user string) map[string]auth.Level {
	t.Helper()
	u, err := users.User(user)
	if err != nil {
		t.Fatal(err)
	}
	return u.Grants
}

// TestAccessDeleteDatabaseForgetsGrants: delete_database removes every
// user's grant on the deleted database (not grants on *), so a database
// recreated under the same name inherits none.
func TestAccessDeleteDatabaseForgetsGrants(t *testing.T) {
	env := newAuthEnv(t)
	env.resetFixtures(t)
	env.addUser(t, "alice", auth.UserOptions{})
	env.setAccess(t, "alice", false, map[string]auth.Level{accessDB: auth.LevelAdmin, accessOther: auth.LevelRead, auth.AllDatabases: auth.LevelRead})
	root := env.as(t, "root")

	callOK(t, root, "delete_database", map[string]any{"name": accessDB})
	want := map[string]auth.Level{accessOther: auth.LevelRead, auth.AllDatabases: auth.LevelRead}
	if got := grantsOf(t, env.users, "alice"); !maps.Equal(got, want) {
		t.Fatalf("grants after delete = %v, want %v", got, want)
	}
	callOK(t, root, "create_database", map[string]any{"name": accessDB})
	callOK(t, root, "create_collection", map[string]any{"database": accessDB, "name": "c"})
	alice := env.as(t, "alice")
	callOK(t, alice, "list_collections", map[string]any{"database": accessDB}) // read through *
	wantDenied(t, call(t, alice, "create_collection", map[string]any{"database": accessDB, "name": "d"}), "admin", `"shared"`)
}

// newAuthOffServer is newTestServer with an auth store: authentication
// is off (stdio), but _system exists and holds alice.
func newAuthOffServer(t *testing.T, transport string) (*Server, *auth.Store) {
	t.Helper()
	ctx := context.Background()
	engine, err := db.Open(ctx, db.Options{Dir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	users, err := auth.Open(ctx, engine)
	if err != nil {
		_ = engine.Close(ctx)
		t.Fatal(err)
	}
	s, err := New(ctx, engine, Config{DefaultDatabase: testDefaultDB, Transport: transport, Version: testVersion, Users: users,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		_ = engine.Close(ctx)
		t.Fatal(err)
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
	return s, users
}

// TestAuthOffDeleteDatabaseForgetsGrants: with authentication off but
// _system present, delete_database still forgets the grants.
func TestAuthOffDeleteDatabaseForgetsGrants(t *testing.T) {
	s, users := newAuthOffServer(t, "stdio")
	ctx := context.Background()
	if _, err := users.CreateUser(ctx, "alice", auth.UserOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := users.Grant(ctx, "alice", "gone", auth.LevelWrite); err != nil {
		t.Fatal(err)
	}
	if err := users.Grant(ctx, "alice", auth.AllDatabases, auth.LevelRead); err != nil {
		t.Fatal(err)
	}
	cs := connect(t, s)
	callOK(t, cs, "create_database", map[string]any{"name": "gone"})
	callOK(t, cs, "delete_database", map[string]any{"name": "gone"})
	if got, want := grantsOf(t, users, "alice"), map[string]auth.Level{auth.AllDatabases: auth.LevelRead}; !maps.Equal(got, want) {
		t.Fatalf("grants = %v, want %v", got, want)
	}
}

// TestAuthOffToolsListExactly26: with authentication off (stdio, and
// loopback HTTP with an auth store present) tools/list is exactly the 26
// tools every server registers: no whoami, no admin tools, and the list
// is not marked private.
func TestAuthOffToolsListExactly26(t *testing.T) {
	want := make([]string, 0, 26)
	for _, tool := range ToolDefinitions() {
		want = append(want, tool.Name)
	}
	slices.Sort(want)
	if len(want) != 26 {
		t.Fatalf("ToolDefinitions has %d tools, want 26", len(want))
	}
	check := func(t *testing.T, cs *mcp.ClientSession) {
		t.Helper()
		res, err := cs.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, tool := range res.Tools {
			got = append(got, tool.Name)
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("tools/list = %v\nwant %v", got, want)
		}
		if res.CacheScope == "private" {
			t.Fatal("auth-off tools/list is marked private")
		}
	}
	t.Run("stdio without store", func(t *testing.T) { check(t, connect(t, newTestServer(t))) })
	t.Run("stdio with store", func(t *testing.T) {
		s, _ := newAuthOffServer(t, "stdio")
		check(t, connect(t, s))
	})
	t.Run("http with store", func(t *testing.T) {
		s, _ := newAuthOffServer(t, "http")
		ts := httptest.NewServer(s.httpHandler())
		t.Cleanup(ts.Close)
		cs := connectClient(t, &mcp.StreamableClientTransport{Endpoint: ts.URL + "/mcp", MaxRetries: -1})
		check(t, cs)
		// No token needed, and full access.
		callOK(t, cs, "create_database", map[string]any{"name": "x"})
	})
}

// TestAuthToolsListPerPrincipal: with tokens, a server admin lists 33
// tools and any other user 27 (the admin tools hidden), both marked
// private; the hidden tools are still refused when called.
func TestAuthToolsListPerPrincipal(t *testing.T) {
	env := newAuthEnv(t)
	env.addUser(t, "alice", auth.UserOptions{})
	env.setAccess(t, "alice", false, map[string]auth.Level{auth.AllDatabases: auth.LevelAdmin})
	adminTools := []string{"create_token", "create_user", "delete_user", "list_users", "revoke_token", "set_access"}

	for _, tt := range []struct {
		user   string
		n      int
		admins bool
	}{{"root", 33, true}, {"alice", 27, false}} {
		t.Run(tt.user, func(t *testing.T) {
			res, err := env.as(t, tt.user).ListTools(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if res.CacheScope != "private" {
				t.Errorf("cacheScope = %q, want private", res.CacheScope)
			}
			names := map[string]bool{}
			for _, tool := range res.Tools {
				names[tool.Name] = true
			}
			if len(names) != tt.n {
				t.Fatalf("%d tools, want %d", len(names), tt.n)
			}
			if !names["whoami"] {
				t.Error("whoami missing")
			}
			for _, a := range adminTools {
				if names[a] != tt.admins {
					t.Errorf("admin tool %s listed = %v", a, names[a])
				}
			}
		})
	}
	// Promotion applies at once: the same session now sees 33.
	alice := env.as(t, "alice")
	if n := len(listTools(t, alice)); n != 27 {
		t.Fatalf("before promotion: %d tools", n)
	}
	env.setAccess(t, "alice", true, nil)
	if n := len(listTools(t, alice)); n != 33 {
		t.Fatalf("after promotion: %d tools, want 33", n)
	}
}

// TestAuthWhoami reports the principal; the implicit principal of a
// tokens-required server reached without a token (stdio) says so.
func TestAuthWhoami(t *testing.T) {
	env := newAuthEnv(t)
	env.addUser(t, "alice", auth.UserOptions{DefaultDatabase: accessDB})
	env.setAccess(t, "alice", false, map[string]auth.Level{accessDB: auth.LevelWrite, auth.AllDatabases: auth.LevelRead})
	out := callOK(t, env.as(t, "alice"), "whoami", nil)
	if out["user"] != "alice" || out["server_admin"] != false || out["default_database"] != accessDB {
		t.Fatalf("whoami = %v", out)
	}
	grants, _ := out["grants"].(map[string]any)
	if want := map[string]any{accessDB: "write", "*": "read"}; !maps.Equal(grants, want) {
		t.Fatalf("grants = %v, want %v", grants, want)
	}
	if out := callOK(t, env.as(t, "root"), "whoami", nil); out["user"] != "root" || out["server_admin"] != true {
		t.Fatalf("root whoami = %v", out)
	}

	// The same server over in-memory transports: no token info, not
	// HTTP, so the implicit full-access principal.
	env.s.transport = "stdio"
	cs := connect(t, env.s)
	out = callOK(t, cs, "whoami", nil)
	if u, _ := out["user"].(string); !strings.Contains(u, "no token") || out["server_admin"] != true {
		t.Fatalf("implicit whoami = %v", out)
	}
	if n := len(listTools(t, cs)); n != 33 {
		t.Fatalf("implicit principal lists %d tools, want 33", n)
	}
}

// TestAuthUserTools exercises the user-management tools and their error
// paths as a server admin, and checks that the changes are logged at
// INFO without a token.
func TestAuthUserTools(t *testing.T) {
	env := newAuthEnv(t)
	root := env.as(t, "root")

	out := callOK(t, root, "create_user", map[string]any{"name": "alice", "default_database": accessDB})
	tok, _ := out["token"].(string)
	if !strings.HasPrefix(tok, auth.TokenPrefix) || out["label"] != auth.DefaultTokenLabel || out["user"] != "alice" {
		t.Fatalf("create_user = %v", out)
	}
	env.tokens["alice"] = tok
	if res := call(t, root, "create_user", map[string]any{"name": "alice"}); !res.IsError || !strings.Contains(resultText(res), "exists") {
		t.Fatalf("duplicate create_user: %s", resultText(res))
	}
	if res := call(t, root, "create_user", map[string]any{"name": "_bad"}); !res.IsError {
		t.Fatal("create_user with an invalid name succeeded")
	}

	// The new token works.
	if out := callOK(t, env.as(t, "alice"), "whoami", nil); out["user"] != "alice" {
		t.Fatalf("whoami with the new token = %v", out)
	}

	out = callOK(t, root, "set_access", map[string]any{"user": "alice", "database": accessDB, "level": "write", "default_database": ""})
	u, _ := out["user"].(map[string]any)
	if g, _ := u["grants"].(map[string]any); g[accessDB] != "write" || u["default_database"] != nil {
		t.Fatalf("set_access user = %v", u)
	}
	out = callOK(t, root, "set_access", map[string]any{"user": "alice", "database": accessDB, "level": "none", "server_admin": true})
	u, _ = out["user"].(map[string]any)
	if g, _ := u["grants"].(map[string]any); len(g) != 0 || u["server_admin"] != true {
		t.Fatalf("set_access none+admin user = %v", u)
	}
	for _, tt := range []struct {
		args map[string]any
		want string
	}{
		{map[string]any{"user": "alice", "database": accessDB}, "give level with database"},
		{map[string]any{"user": "alice", "level": "read"}, "give database with level"},
		{map[string]any{"user": "alice"}, "nothing to change"},
		{map[string]any{"user": "ghost", "server_admin": true}, "list_users shows"},
		{map[string]any{"user": "alice", "database": "_system", "level": "read"}, "invalid"},
	} {
		res := call(t, root, "set_access", tt.args)
		if !res.IsError || !strings.Contains(resultText(res), tt.want) {
			t.Errorf("set_access %v = %s, want error with %q", tt.args, resultText(res), tt.want)
		}
	}
	if res := call(t, root, "set_access", map[string]any{"user": "alice", "database": accessDB, "level": "owner"}); !res.IsError {
		t.Error("set_access with an unknown level succeeded")
	}

	out = callOK(t, root, "create_token", map[string]any{"user": "alice", "label": "laptop"})
	laptop, _ := out["token"].(string)
	if out["label"] != "laptop" || !strings.HasPrefix(laptop, auth.TokenPrefix) {
		t.Fatalf("create_token = %v", out)
	}
	if out := callOK(t, root, "create_token", map[string]any{"user": "alice"}); out["label"] != "default-2" {
		t.Fatalf("create_token without label = %v", out)
	}
	if res := call(t, root, "create_token", map[string]any{"user": "alice", "label": "laptop"}); !res.IsError {
		t.Error("duplicate token label accepted")
	}
	if res := call(t, root, "create_token", map[string]any{"user": "ghost"}); !res.IsError || !strings.Contains(resultText(res), "list_users") {
		t.Errorf("create_token for unknown user: %s", resultText(res))
	}

	out = callOK(t, root, "list_users", nil)
	listing := fmt.Sprint(out)
	for _, secret := range []string{tok, laptop, env.tokens["root"]} {
		if strings.Contains(listing, secret) {
			t.Fatal("list_users shows a token")
		}
	}
	users, _ := out["users"].([]any)
	if len(users) != 2 {
		t.Fatalf("list_users = %v", users)
	}
	a, _ := users[0].(map[string]any)
	if a["name"] != "alice" || len(a["tokens"].([]any)) != 3 {
		t.Fatalf("alice in list_users = %v", a)
	}

	callOK(t, root, "revoke_token", map[string]any{"user": "alice", "label": "laptop"})
	if res := call(t, root, "revoke_token", map[string]any{"user": "alice", "label": "laptop"}); !res.IsError || !strings.Contains(resultText(res), "not found") {
		t.Errorf("revoke twice: %s", resultText(res))
	}
	if code := postInitialize(t, env, laptop, ""); code != http.StatusUnauthorized {
		t.Fatalf("revoked token: HTTP %d, want 401", code)
	}

	// The last server admin is protected; with alice an admin too, root
	// can be demoted, and then alice cannot.
	callOK(t, root, "set_access", map[string]any{"user": "root", "server_admin": false})
	aliceS := env.as(t, "alice")
	res := call(t, aliceS, "delete_user", map[string]any{"name": "alice"})
	if !res.IsError || !strings.Contains(resultText(res), "only server admin") {
		t.Fatalf("delete last admin: %s", resultText(res))
	}
	res = call(t, aliceS, "set_access", map[string]any{"user": "alice", "server_admin": false})
	if !res.IsError || !strings.Contains(resultText(res), "only server admin") {
		t.Fatalf("demote last admin: %s", resultText(res))
	}
	callOK(t, aliceS, "set_access", map[string]any{"user": "root", "server_admin": true})
	callOK(t, root, "delete_user", map[string]any{"name": "alice"})
	if res := call(t, root, "delete_user", map[string]any{"name": "alice"}); !res.IsError {
		t.Error("delete_user twice succeeded")
	}
	// A deleted user's session fails at once: its tokens are gone, so
	// the HTTP layer refuses the request (401).
	if res, err := aliceS.CallTool(context.Background(), &mcp.CallToolParams{Name: "whoami"}); err == nil {
		t.Fatalf("deleted user's session still answers: %s", resultText(res))
	} else if !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatalf("deleted user's session: %v, want Unauthorized", err)
	}

	logs := env.log.String()
	for _, action := range []string{"create_user", "set_access", "create_token", "revoke_token", "delete_user"} {
		if !strings.Contains(logs, "action="+action) {
			t.Errorf("log has no %s line:\n%s", action, logs)
		}
	}
	if !strings.Contains(logs, "actor=root") {
		t.Errorf("log does not name the actor:\n%s", logs)
	}
	for _, secret := range []string{tok, laptop, env.tokens["root"]} {
		if strings.Contains(logs, secret) {
			t.Fatal("log contains a token")
		}
	}
}

// postInitialize sends a raw MCP initialize request to /mcp with token
// (none if empty) and session id (none if empty) and returns the HTTP
// status.
func postInitialize(t *testing.T, env *authEnv, token, session string) int {
	t.Helper()
	code, _ := postMCP(t, env, token, session, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"raw","version":"0"}}}`)
	return code
}

func postMCP(t *testing.T, env *authEnv, token, session, body string) (int, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, env.ts.URL+"/mcp", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if session != "" {
		req.Header.Set("Mcp-Session-Id", session)
		req.Header.Set("Mcp-Protocol-Version", "2025-06-18")
	}
	resp, err := env.ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header
}

// TestAuthHTTP401: with tokens required, /mcp answers 401 to a request
// without a token, with a malformed header, with a wrong token, and with
// a revoked token; /healthz stays open; every refusal is logged at INFO
// without the token.
func TestAuthHTTP401(t *testing.T) {
	env := newAuthEnv(t)
	tok := env.addUser(t, "alice", auth.UserOptions{})
	spare, _, err := env.users.CreateToken(context.Background(), "alice", "spare")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.RevokeToken(context.Background(), "alice", "spare"); err != nil {
		t.Fatal(err)
	}
	wrong := auth.TokenPrefix + strings.Repeat("A", 43)

	if code := postInitialize(t, env, tok, ""); code != http.StatusOK {
		t.Fatalf("valid token: HTTP %d, want 200", code)
	}
	for name, token := range map[string]string{"none": "", "wrong": wrong, "revoked": spare} {
		if code := postInitialize(t, env, token, ""); code != http.StatusUnauthorized {
			t.Errorf("%s token: HTTP %d, want 401", name, code)
		}
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, env.ts.URL+"/mcp", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Basic "+tok)
	resp, err := env.ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Basic scheme: HTTP %d, want 401", resp.StatusCode)
	}

	resp, err = env.ts.Client().Get(env.ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz without a token: HTTP %d, want 200", resp.StatusCode)
	}

	logs := env.log.String()
	if n := strings.Count(logs, "authentication failed"); n < 4 {
		t.Errorf("%d authentication-failure lines, want at least 4:\n%s", n, logs)
	}
	for _, reason := range []string{"no bearer token", "unknown or revoked token"} {
		if !strings.Contains(logs, reason) {
			t.Errorf("log has no %q line", reason)
		}
	}
	for _, secret := range []string{tok, wrong, spare} {
		if strings.Contains(logs, secret) {
			t.Fatal("log contains a token")
		}
	}
}

// TestAuthSessionBoundToUser: a session created with alice's token
// answers 403 to a request carrying bob's token, and 401 to one without.
func TestAuthSessionBoundToUser(t *testing.T) {
	env := newAuthEnv(t)
	alice := env.addUser(t, "alice", auth.UserOptions{})
	bob := env.addUser(t, "bob", auth.UserOptions{})

	code, h := postMCP(t, env, alice, "", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"raw","version":"0"}}}`)
	session := h.Get("Mcp-Session-Id")
	if code != http.StatusOK || session == "" {
		t.Fatalf("initialize: HTTP %d, session %q", code, session)
	}
	list := `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
	if code, _ := postMCP(t, env, bob, session, list); code != http.StatusForbidden {
		t.Fatalf("other user's token on the session: HTTP %d, want 403", code)
	}
	if code, _ := postMCP(t, env, "", session, list); code != http.StatusUnauthorized {
		t.Fatalf("no token on the session: HTTP %d, want 401", code)
	}
	if code, _ := postMCP(t, env, alice, session, list); code != http.StatusOK {
		t.Fatalf("owner's token on the session: HTTP %d, want 200", code)
	}
}

// TestAuthRevokedTokenEndsAccessAtOnce: a grant change or token revocation
// applies to an open session's next call.
func TestAuthRevokedTokenEndsAccessAtOnce(t *testing.T) {
	env := newAuthEnv(t)
	env.resetFixtures(t)
	env.addUser(t, "alice", auth.UserOptions{})
	env.setAccess(t, "alice", false, map[string]auth.Level{accessDB: auth.LevelRead})
	alice := env.as(t, "alice")
	args := map[string]any{"database": accessDB, "collection": accessColl}
	callOK(t, alice, "find_documents", args)
	env.setAccess(t, "alice", false, nil)
	wantDenied(t, call(t, alice, "find_documents", args), "read", `"shared"`)
}
