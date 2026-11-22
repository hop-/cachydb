package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hop-/cachydb/internal/auth"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
)

// bootstrapValue is a valid bootstrap token for the tests (64 hex
// characters, like "openssl rand -hex 32").
const bootstrapValue = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// buildAuth builds an app on dir with the given transport, host, and auth
// mode (port 0, never started) and stops it at cleanup.
func buildAuth(t *testing.T, dir, transport, host, mode, bootstrap string, logs *syncBuffer) (*App, error) {
	t.Helper()
	a, err := NewBuilder().
		WithDBName("main").
		WithRootDir(dir).
		WithTransport(transport).
		WithListen(mcpserver.ListenConfig{Host: host}).
		WithAuth(mode).
		WithBootstrapAdminToken(bootstrap).
		WithLogger(testLogger(logs)).
		Build(context.Background())
	if err == nil {
		t.Cleanup(func() {
			if err := a.Stop(context.Background()); err != nil {
				t.Errorf("Stop: %v", err)
			}
		})
	}
	return a, err
}

// addUser creates a user in dir's auth store while no server runs.
func addUser(t *testing.T, dir, name string, admin bool) string {
	t.Helper()
	ctx := context.Background()
	e, err := db.Open(ctx, db.Options{Dir: dir, Logger: testLogger(&syncBuffer{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	s, err := auth.Open(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := s.CreateUser(ctx, name, auth.UserOptions{ServerAdmin: admin})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// hasSystem reports whether dir holds the system database.
func hasSystem(t *testing.T, dir string) bool {
	t.Helper()
	ctx := context.Background()
	e, err := db.Open(ctx, db.Options{Dir: dir, Logger: testLogger(&syncBuffer{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	return e.HasSystemDatabase()
}

// TestAuthStartupMatrix covers approach-access-control §3 through Build,
// without listening: a server that needs tokens and has no user refuses
// to start with a message naming the way out; stdio and loopback HTTP
// start without users; a non-loopback host starts with AUTH=off and logs
// a WARN. No listener is opened (Build does not listen).
func TestAuthStartupMatrix(t *testing.T) {
	for _, tt := range []struct {
		name, transport, host, mode string
		refuse, warn                bool
	}{
		{"stdio auto", "stdio", "", "", false, false},
		{"stdio required", "stdio", "0.0.0.0", "required", false, false},
		{"http loopback default host", "http", "", "auto", false, false},
		{"http loopback ::1", "http", "[::1]", "auto", false, false},
		{"http loopback off", "http", "127.0.0.1", "off", false, false},
		{"http loopback required", "http", "127.0.0.1", "required", true, false},
		{"http any host auto", "http", "0.0.0.0", "auto", true, false},
		{"http any host empty mode", "http", "0.0.0.0", "", true, false},
		{"http any host off", "http", "0.0.0.0", "off", false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			logs := &syncBuffer{}
			_, err := buildAuth(t, dir, tt.transport, tt.host, tt.mode, "", logs)
			if tt.refuse {
				if !errors.Is(err, ErrNoUsers) {
					t.Fatalf("Build = %v, want ErrNoUsers", err)
				}
				for _, part := range []string{"refusing to start", "utils user add", "BOOTSTRAP_ADMIN_TOKEN", "AUTH=off"} {
					if !strings.Contains(err.Error(), part) {
						t.Errorf("refusal %q does not mention %q", err, part)
					}
				}
				// The engine was closed: the directory can be opened again.
				_ = hasSystem(t, dir)
				return
			}
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			hasWarn := strings.Contains(logs.String(), "level=WARN") && strings.Contains(logs.String(), "authentication is off")
			if hasWarn != tt.warn {
				t.Fatalf("WARN logged = %v, want %v:\n%s", hasWarn, tt.warn, logs.String())
			}
		})
	}
}

// TestAuthStartupWithUsers: with a user, a server that needs tokens
// starts.
func TestAuthStartupWithUsers(t *testing.T) {
	dir := t.TempDir()
	addUser(t, dir, "root", true)
	if _, err := buildAuth(t, dir, "http", "127.0.0.1", "required", "", &syncBuffer{}); err != nil {
		t.Fatalf("Build with a user: %v", err)
	}
}

// TestAuthBuilderRejectsBadSettings: an invalid mode, one TLS file, or an
// invalid bootstrap token fail before the engine opens (the directory is
// not even created).
func TestAuthBuilderRejectsBadSettings(t *testing.T) {
	for _, tt := range []struct {
		name string
		b    *Builder
		want string
	}{
		{"invalid auth mode", NewBuilder().WithAuth("maybe"), "AUTH"},
		{"tls cert only", NewBuilder().WithListen(mcpserver.ListenConfig{TLSCert: "c.pem"}), "TLS"},
		{"tls key only", NewBuilder().WithListen(mcpserver.ListenConfig{TLSKey: "k.pem"}), "TLS"},
		{"short bootstrap token", NewBuilder().WithBootstrapAdminToken("short"), "BOOTSTRAP_ADMIN_TOKEN"},
		{"bootstrap token with a space", NewBuilder().WithBootstrapAdminToken(strings.Repeat("a", 20) + " " + strings.Repeat("b", 20)), "BOOTSTRAP_ADMIN_TOKEN"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			_, err := tt.b.WithRootDir(dir).WithTransport("http").WithLogger(testLogger(&syncBuffer{})).Build(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Build = %v, want an error naming %s", err, tt.want)
			}
			if _, serr := os.Stat(dir); !errors.Is(serr, os.ErrNotExist) {
				t.Fatalf("Build opened the engine before rejecting the settings (%v)", serr)
			}
		})
	}
	// The error never contains the rejected token.
	bad := "secret-" + strings.Repeat("x", 10)
	_, err := NewBuilder().WithBootstrapAdminToken(bad).WithRootDir(t.TempDir()).Build(context.Background())
	if err == nil || strings.Contains(err.Error(), bad) {
		t.Fatalf("Build = %v", err)
	}
}

// TestAuthBootstrapPreventsRefusal: a bootstrap token creates the server
// admin "admin" holding it, so a server that needs tokens starts on an
// empty directory; the token then authenticates admin. The change is
// logged, never the token. A second start with the same token changes
// nothing.
func TestAuthBootstrapPreventsRefusal(t *testing.T) {
	dir := t.TempDir()
	logs := &syncBuffer{}
	a, err := buildAuth(t, dir, "http", "0.0.0.0", "auto", bootstrapValue, logs)
	if err != nil {
		t.Fatalf("Build with a bootstrap token: %v", err)
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), bootstrapValue) {
		t.Fatal("the log contains the bootstrap token")
	}
	for _, action := range []string{"action=create_user", "action=create_token"} {
		if !strings.Contains(logs.String(), action) {
			t.Errorf("log has no %s:\n%s", action, logs.String())
		}
	}

	ctx := context.Background()
	e, err := db.Open(ctx, db.Options{Dir: dir, Logger: testLogger(&syncBuffer{})})
	if err != nil {
		t.Fatal(err)
	}
	s, err := auth.Open(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Authenticate(bootstrapValue)
	if err != nil || p.Name != auth.BootstrapAdmin || !p.ServerAdmin {
		t.Fatalf("Authenticate(bootstrap) = %+v, %v", p, err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}

	logs2 := &syncBuffer{}
	if _, err := buildAuth(t, dir, "stdio", "", "", bootstrapValue, logs2); err != nil {
		t.Fatalf("second Build: %v", err)
	}
	if strings.Contains(logs2.String(), "user management") {
		t.Fatalf("an unchanged bootstrap token logged a change:\n%s", logs2.String())
	}
}

// TestAuthStoreOpenedWhenSystemExists: with authentication off, Build
// does not create the system database on a fresh directory, but opens the
// store when it exists, so delete_database still forgets grants.
func TestAuthStoreOpenedWhenSystemExists(t *testing.T) {
	fresh := t.TempDir()
	a, err := buildAuth(t, fresh, "http", "127.0.0.1", "auto", "", &syncBuffer{})
	if err != nil {
		t.Fatal(err)
	}
	if a.engine.HasSystemDatabase() {
		t.Fatal("an auth-off start created the system database")
	}

	withUsers := t.TempDir()
	addUser(t, withUsers, "root", true)
	a, err = buildAuth(t, withUsers, "stdio", "", "off", "", &syncBuffer{})
	if err != nil {
		t.Fatalf("Build with _system and auth off: %v", err)
	}
	if !a.engine.HasSystemDatabase() {
		t.Fatal("system database not found")
	}
}

func TestAuthBootstrapToken(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte("  "+bootstrapValue+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name, value, file, want, wantErr string
	}{
		{"neither", "", "", "", ""},
		{"value trimmed", " " + bootstrapValue + "\n", "", bootstrapValue, ""},
		{"file trimmed", "", file, bootstrapValue, ""},
		{"both", bootstrapValue, file, "", "both set"},
		{"empty file", "", empty, "", "is empty"},
		{"missing file", "", filepath.Join(dir, "nope"), "", "BOOTSTRAP_ADMIN_TOKEN_FILE"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BootstrapToken(tt.value, tt.file)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("BootstrapToken = %q, %v; want error %q", got, err, tt.wantErr)
				}
				if strings.Contains(err.Error(), bootstrapValue) {
					t.Fatal("error contains the token")
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("BootstrapToken = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

// TestBuilderRejectsBadResultSettings: a negative find_documents bound or
// an unknown result text mode fail before the engine opens; zero keeps
// the server default.
func TestBuilderRejectsBadResultSettings(t *testing.T) {
	for _, tt := range []struct {
		name string
		b    *Builder
		want string
	}{
		{"negative default limit", NewBuilder().WithResultLimits(-1, 0), "DEFAULT_LIMIT"},
		{"negative byte cap", NewBuilder().WithResultLimits(0, -1), "MAX_RESULT_BYTES"},
		{"unknown result text", NewBuilder().WithResultText("brief"), "RESULT_TEXT"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			_, err := tt.b.WithRootDir(dir).WithLogger(testLogger(&syncBuffer{})).Build(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Build = %v, want an error naming %s", err, tt.want)
			}
			if _, serr := os.Stat(dir); !errors.Is(serr, os.ErrNotExist) {
				t.Fatalf("Build opened the engine before rejecting the settings (%v)", serr)
			}
		})
	}
	for _, b := range []*Builder{NewBuilder().WithResultLimits(0, 0).WithResultText(""), NewBuilder().WithResultLimits(5, 4096).WithResultText("summary")} {
		a, err := b.WithRootDir(t.TempDir()).WithDBName("main").WithLogger(testLogger(&syncBuffer{})).Build(context.Background())
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		if err := a.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}
}
