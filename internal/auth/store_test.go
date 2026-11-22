package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hop-/cachydb/pkg/db"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// openTestEngine opens an engine on dir and closes it at cleanup (a second
// Close is a no-op, so tests may close it earlier).
func openTestEngine(t *testing.T, dir string) *db.Engine {
	t.Helper()
	e, err := db.Open(context.Background(), db.Options{Dir: dir, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e
}

// newTestStore opens an engine in a temp dir and the store on it.
func newTestStore(t *testing.T) (*Store, *db.Engine) {
	t.Helper()
	e := openTestEngine(t, t.TempDir())
	s, err := Open(context.Background(), e)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, e
}

func mustCreateUser(t *testing.T, s *Store, name string, opts UserOptions) string {
	t.Helper()
	token, err := s.CreateUser(context.Background(), name, opts)
	if err != nil {
		t.Fatalf("CreateUser(%s): %v", name, err)
	}
	return token
}

func mustGrant(t *testing.T, s *Store, name, database string, l Level) {
	t.Helper()
	if err := s.Grant(context.Background(), name, database, l); err != nil {
		t.Fatalf("Grant(%s, %s, %s): %v", name, database, l, err)
	}
}

// systemDocs returns every document of the system collection name.
func systemDocs(t *testing.T, e *db.Engine, name string) []*db.Document {
	t.Helper()
	d, err := e.SystemDatabase(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c, err := d.Collection(name)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := c.Find(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestAuthStoreRoundTrip: users, tokens, and grants persist in the system
// database and survive a reopen; tokens have the documented form and only
// their SHA-256 digests are stored.
func TestAuthStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	s, err := Open(ctx, e)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.HasUsers() || len(s.Users()) != 0 {
		t.Fatal("a new store has users")
	}
	if got := e.ListDatabases(); slices.Contains(got, db.SystemDatabaseName) {
		t.Fatalf("ListDatabases = %v lists the system database", got)
	}

	rootTok := mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	aliceTok := mustCreateUser(t, s, "alice", UserOptions{DefaultDatabase: "shop"})
	mustGrant(t, s, "alice", "shop", LevelWrite)
	mustGrant(t, s, "alice", AllDatabases, LevelRead)
	laptop, label, err := s.CreateToken(ctx, "alice", "laptop")
	if err != nil || label != "laptop" {
		t.Fatalf("CreateToken = %q, %v", label, err)
	}

	for _, tok := range []string{rootTok, aliceTok, laptop} {
		body, ok := strings.CutPrefix(tok, TokenPrefix)
		if !ok {
			t.Fatalf("token %q... lacks the %s prefix", tok[:4], TokenPrefix)
		}
		raw, err := base64.RawURLEncoding.DecodeString(body)
		if err != nil || len(raw) != 32 || strings.ContainsAny(body, "=+/") {
			t.Fatalf("token body is not 32 bytes of unpadded base64url (len %d, err %v)", len(raw), err)
		}
	}
	if rootTok == aliceTok || aliceTok == laptop {
		t.Fatal("two tokens are equal")
	}

	// Only digests are stored: no stored document contains a token, and
	// every token document's _id is the SHA-256 hex of one token.
	want := map[string]bool{sha256Hex(rootTok): true, sha256Hex(aliceTok): true, sha256Hex(laptop): true}
	tokenDocs := systemDocs(t, e, tokensCollection)
	if len(tokenDocs) != 3 {
		t.Fatalf("%d token documents, want 3", len(tokenDocs))
	}
	for _, d := range tokenDocs {
		if !want[d.ID] {
			t.Errorf("token document _id %q is not a token digest", d.ID)
		}
	}
	for _, d := range append(systemDocs(t, e, usersCollection), tokenDocs...) {
		data, _ := json.Marshal(d)
		for _, tok := range []string{rootTok, aliceTok, laptop} {
			if strings.Contains(string(data), tok) || strings.Contains(string(data), strings.TrimPrefix(tok, TokenPrefix)) {
				t.Fatalf("stored document %s contains a token", d.ID)
			}
		}
	}

	check := func(t *testing.T, s *Store) {
		t.Helper()
		p, err := s.Authenticate(aliceTok)
		if err != nil {
			t.Fatalf("Authenticate(alice): %v", err)
		}
		if p.Name != "alice" || p.ServerAdmin || p.DefaultDatabase != "shop" || p.Implicit ||
			p.Grants["shop"] != LevelWrite || p.Grants[AllDatabases] != LevelRead {
			t.Fatalf("alice = %+v", p)
		}
		if p, err := s.Authenticate(laptop); err != nil || p.Name != "alice" {
			t.Fatalf("Authenticate(laptop) = %+v, %v", p, err)
		}
		if p, err := s.Authenticate(rootTok); err != nil || !p.ServerAdmin || p.Name != "root" {
			t.Fatalf("Authenticate(root) = %+v, %v", p, err)
		}
		users := s.Users()
		if len(users) != 2 || users[0].Name != "alice" || users[1].Name != "root" {
			t.Fatalf("Users = %+v", users)
		}
		var labels []string
		for _, tk := range users[0].Tokens {
			labels = append(labels, tk.Label)
			if tk.Created.IsZero() {
				t.Errorf("token %s has no created time", tk.Label)
			}
		}
		if !slices.Equal(labels, []string{DefaultTokenLabel, "laptop"}) {
			t.Fatalf("alice token labels = %v", labels)
		}
		if users[0].Created.IsZero() {
			t.Error("alice has no created time")
		}
		if !s.HasUsers() {
			t.Error("HasUsers = false")
		}
	}
	check(t, s)

	// Mutating a returned principal or user does not change the store.
	p, _ := s.Authenticate(aliceTok)
	p.Grants["shop"] = LevelAdmin
	u, _ := s.User("alice")
	u.Grants["other"] = LevelAdmin
	check(t, s)

	if err := e.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	e2 := openTestEngine(t, dir)
	s2, err := Open(ctx, e2)
	if err != nil {
		t.Fatalf("Open after reopen: %v", err)
	}
	check(t, s2)
}

func TestAuthAuthenticateRejects(t *testing.T) {
	s, _ := newTestStore(t)
	tok := mustCreateUser(t, s, "alice", UserOptions{})
	for name, bad := range map[string]string{
		"empty":           "",
		"unknown":         TokenPrefix + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"prefix only":     TokenPrefix,
		"digest of token": sha256Hex(tok),
		"token with tail": tok + "x",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.Authenticate(bad)
			if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("Authenticate err = %v, want ErrUnauthenticated", err)
			}
			if bad != "" && strings.Contains(err.Error(), bad) {
				t.Fatalf("error %q contains the token", err)
			}
		})
	}
}

func TestAuthAuthorize(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	aliceTok := mustCreateUser(t, s, "alice", UserOptions{})
	mustGrant(t, s, "alice", "shop", LevelWrite)
	mustGrant(t, s, "alice", AllDatabases, LevelRead)
	alice, err := s.Authenticate(aliceTok)
	if err != nil {
		t.Fatal(err)
	}
	root, err := s.Principal("root")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		p       Principal
		db      string
		need    Level
		wantErr error
		wantMsg string
	}{
		{"none", alice, "x", LevelNone, nil, ""},
		{"read", alice, "shop", LevelRead, nil, ""},
		{"write", alice, "shop", LevelWrite, nil, ""},
		{"admin one above write", alice, "shop", LevelAdmin, ErrPermissionDenied, `user "alice" needs admin access to database "shop"`},
		{"wildcard read", alice, "other", LevelRead, nil, ""},
		{"wildcard denies write", alice, "other", LevelWrite, ErrPermissionDenied, `needs write access to database "other"`},
		{"server level", alice, "", LevelServer, ErrPermissionDenied, `user "alice" must be a server admin`},
		{"server admin", root, "any", LevelServer, nil, ""},
		{"implicit", FullAccess(), "any", LevelServer, nil, ""},
		{"unknown user", Principal{Name: "ghost"}, "shop", LevelRead, ErrUnauthenticated, `"ghost" does not exist`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Authorize(tt.p, tt.db, tt.need)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Authorize = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Authorize = %v, want %v containing %q", err, tt.wantErr, tt.wantMsg)
			}
		})
	}

	// The user's current grants decide, not the ones the principal was
	// authenticated with.
	if err := s.Revoke(ctx, "alice", "shop"); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(alice, "shop", LevelWrite); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("Authorize after revoke = %v, want ErrPermissionDenied", err)
	}
	if err := s.DeleteUser(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(alice, "x", LevelNone); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Authorize of a removed user = %v, want ErrUnauthenticated", err)
	}
	if _, err := s.Authenticate(aliceTok); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Authenticate of a removed user's token = %v, want ErrUnauthenticated", err)
	}
}

func TestAuthStoreErrors(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	mustCreateUser(t, s, "alice", UserOptions{})
	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{"duplicate user", func() error { _, err := s.CreateUser(ctx, "alice", UserOptions{}); return err }, db.ErrExists},
		{"invalid user name", func() error { _, err := s.CreateUser(ctx, "_x", UserOptions{}); return err }, db.ErrInvalidName},
		{"invalid default database", func() error {
			_, err := s.CreateUser(ctx, "bob", UserOptions{DefaultDatabase: "bad/db"})
			return err
		}, db.ErrInvalidName},
		{"delete missing user", func() error { return s.DeleteUser(ctx, "ghost") }, db.ErrNotFound},
		{"grant missing user", func() error { return s.Grant(ctx, "ghost", "shop", LevelRead) }, db.ErrNotFound},
		{"grant invalid database", func() error { return s.Grant(ctx, "alice", "bad/db", LevelRead) }, db.ErrInvalidName},
		{"grant system database", func() error { return s.Grant(ctx, "alice", db.SystemDatabaseName, LevelRead) }, db.ErrInvalidName},
		{"grant level none", func() error { return s.Grant(ctx, "alice", "shop", LevelNone) }, ErrInvalidLevel},
		{"grant level server", func() error { return s.Grant(ctx, "alice", "shop", LevelServer) }, ErrInvalidLevel},
		{"revoke missing grant", func() error { return s.Revoke(ctx, "alice", "shop") }, db.ErrNotFound},
		{"revoke missing user", func() error { return s.Revoke(ctx, "ghost", "shop") }, db.ErrNotFound},
		{"token for missing user", func() error { _, _, err := s.CreateToken(ctx, "ghost", ""); return err }, db.ErrNotFound},
		{"invalid token label", func() error { _, _, err := s.CreateToken(ctx, "alice", "bad label"); return err }, db.ErrInvalidName},
		{"duplicate token label", func() error { _, _, err := s.CreateToken(ctx, "alice", DefaultTokenLabel); return err }, db.ErrExists},
		{"revoke missing token", func() error { return s.RevokeToken(ctx, "alice", "nope") }, db.ErrNotFound},
		{"revoke token of missing user", func() error { return s.RevokeToken(ctx, "ghost", DefaultTokenLabel) }, db.ErrNotFound},
		{"user missing", func() error { _, err := s.User("ghost"); return err }, db.ErrNotFound},
		{"principal missing", func() error { _, err := s.Principal("ghost"); return err }, ErrUnauthenticated},
		{"set access invalid level", func() error {
			return s.SetAccess(ctx, "alice", AccessChange{Database: "shop", Level: LevelServer})
		}, ErrInvalidLevel},
		{"set access invalid database", func() error {
			return s.SetAccess(ctx, "alice", AccessChange{Database: "bad/db", Level: LevelRead})
		}, db.ErrInvalidName},
		{"set access invalid default", func() error { return s.SetDefaultDatabase(ctx, "alice", "bad/db") }, db.ErrInvalidName},
		{"set access missing user", func() error { return s.SetServerAdmin(ctx, "ghost", true) }, db.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
	// None of the failures changed anything.
	users := s.Users()
	if len(users) != 2 || len(users[0].Grants) != 0 || len(users[0].Tokens) != 1 {
		t.Fatalf("users after failed changes = %+v", users)
	}
}

func TestAuthTokens(t *testing.T) {
	ctx := context.Background()
	s, e := newTestStore(t)
	first := mustCreateUser(t, s, "alice", UserOptions{})
	var tokens []string
	var labels []string
	for range 3 {
		tok, label, err := s.CreateToken(ctx, "alice", "")
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, tok)
		labels = append(labels, label)
	}
	if want := []string{"default-2", "default-3", "default-4"}; !slices.Equal(labels, want) {
		t.Fatalf("labels = %v, want %v", labels, want)
	}
	if err := s.RevokeToken(ctx, "alice", "default-3"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(tokens[1]); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked token authenticates: %v", err)
	}
	for _, tok := range []string{first, tokens[0], tokens[2]} {
		if _, err := s.Authenticate(tok); err != nil {
			t.Fatalf("a kept token fails: %v", err)
		}
	}
	// The freed label is picked again.
	if _, label, err := s.CreateToken(ctx, "alice", ""); err != nil || label != "default-3" {
		t.Fatalf("CreateToken = %q, %v; want default-3", label, err)
	}

	// Removing the user removes all its tokens.
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	if err := s.DeleteUser(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	for _, d := range systemDocs(t, e, tokensCollection) {
		if d.Data[fieldUser] == "alice" {
			t.Fatalf("token %s of the removed user is still stored", d.ID)
		}
	}
	for _, tok := range append(tokens, first) {
		if _, err := s.Authenticate(tok); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("a token of the removed user authenticates")
		}
	}
	// The name can be used again, with no old grants or tokens.
	again := mustCreateUser(t, s, "alice", UserOptions{})
	if p, err := s.Authenticate(again); err != nil || len(p.Grants) != 0 {
		t.Fatalf("recreated alice = %+v, %v", p, err)
	}
}

func TestAuthForgetDatabase(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustCreateUser(t, s, "alice", UserOptions{})
	mustCreateUser(t, s, "bob", UserOptions{})
	mustCreateUser(t, s, "carol", UserOptions{})
	mustGrant(t, s, "alice", "shop", LevelAdmin)
	mustGrant(t, s, "alice", "logs", LevelRead)
	mustGrant(t, s, "bob", "shop", LevelRead)
	mustGrant(t, s, "bob", AllDatabases, LevelWrite)

	if err := s.ForgetDatabase(ctx, "shop"); err != nil {
		t.Fatalf("ForgetDatabase: %v", err)
	}
	want := map[string]map[string]Level{
		"alice": {"logs": LevelRead},
		"bob":   {AllDatabases: LevelWrite},
		"carol": {},
	}
	for name, grants := range want {
		u, err := s.User(name)
		if err != nil {
			t.Fatal(err)
		}
		if len(u.Grants) != len(grants) {
			t.Errorf("%s grants = %v, want %v", name, u.Grants, grants)
		}
		for d, l := range grants {
			if u.Grants[d] != l {
				t.Errorf("%s grants = %v, want %v", name, u.Grants, grants)
			}
		}
	}
	// The wildcard is not a database: forgetting it removes nothing.
	if err := s.ForgetDatabase(ctx, AllDatabases); err != nil {
		t.Fatal(err)
	}
	if u, _ := s.User("bob"); u.Grants[AllDatabases] != LevelWrite {
		t.Fatalf("ForgetDatabase(*) removed bob's wildcard grant: %v", u.Grants)
	}
	// Nothing to forget is fine.
	if err := s.ForgetDatabase(ctx, "never"); err != nil {
		t.Fatal(err)
	}
}

func TestAuthSetAccess(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	mustCreateUser(t, s, "alice", UserOptions{})

	yes, def := true, "shop"
	if err := s.SetAccess(ctx, "alice", AccessChange{Database: "shop", Level: LevelWrite, ServerAdmin: &yes, DefaultDatabase: &def}); err != nil {
		t.Fatal(err)
	}
	u, _ := s.User("alice")
	if !u.ServerAdmin || u.DefaultDatabase != "shop" || u.Grants["shop"] != LevelWrite {
		t.Fatalf("after SetAccess: %+v", u)
	}
	// A change with an invalid part changes nothing.
	no := false
	if err := s.SetAccess(ctx, "alice", AccessChange{Database: "shop", Level: Level(42), ServerAdmin: &no}); !errors.Is(err, ErrInvalidLevel) {
		t.Fatalf("err = %v", err)
	}
	if u, _ := s.User("alice"); !u.ServerAdmin {
		t.Fatal("a refused SetAccess changed the server-admin flag")
	}
	// Level none removes the grant; removing a missing one is no error.
	for range 2 {
		if err := s.SetAccess(ctx, "alice", AccessChange{Database: "shop", Level: LevelNone}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetDefaultDatabase(ctx, "alice", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetServerAdmin(ctx, "alice", false); err != nil {
		t.Fatal(err)
	}
	if u, _ := s.User("alice"); u.ServerAdmin || u.DefaultDatabase != "" || len(u.Grants) != 0 {
		t.Fatalf("after clearing: %+v", u)
	}
	if !(AccessChange{}).IsZero() || (AccessChange{Database: "x"}).IsZero() || (AccessChange{ServerAdmin: &no}).IsZero() || (AccessChange{DefaultDatabase: &def}).IsZero() {
		t.Error("IsZero is wrong")
	}
}

func TestAuthLastServerAdmin(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	mustCreateUser(t, s, "alice", UserOptions{})
	no := false
	for name, run := range map[string]func() error{
		"delete":         func() error { return s.DeleteUser(ctx, "root") },
		"demote":         func() error { return s.SetServerAdmin(ctx, "root", false) },
		"set access off": func() error { return s.SetAccess(ctx, "root", AccessChange{ServerAdmin: &no}) },
	} {
		if err := run(); !errors.Is(err, ErrLastServerAdmin) {
			t.Errorf("%s the only server admin: err = %v, want ErrLastServerAdmin", name, err)
		}
	}
	if u, err := s.User("root"); err != nil || !u.ServerAdmin {
		t.Fatalf("root after refused changes = %+v, %v", u, err)
	}
	// A user who is not a server admin can always be removed, and with a
	// second server admin the first may go.
	if err := s.DeleteUser(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	mustCreateUser(t, s, "admin2", UserOptions{ServerAdmin: true})
	if err := s.SetServerAdmin(ctx, "root", false); err != nil {
		t.Fatalf("demote with another admin: %v", err)
	}
	if err := s.DeleteUser(ctx, "admin2"); !errors.Is(err, ErrLastServerAdmin) {
		t.Fatalf("delete the now only admin: %v", err)
	}
	if err := s.DeleteUser(ctx, "root"); err != nil {
		t.Fatalf("delete a demoted admin: %v", err)
	}
}

// TestAuthConcurrentGrants: concurrent grants on one user through one
// store all persist (the change lock plus compare-and-set).
func TestAuthConcurrentGrants(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	tok := mustCreateUser(t, s, "alice", UserOptions{})
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	stop := make(chan struct{})
	var readers sync.WaitGroup
	// Authenticate and Authorize run concurrently with the changes.
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				p, err := s.Authenticate(tok)
				if err != nil {
					errs <- err
					return
				}
				_ = s.Authorize(p, "db0", LevelRead)
				_ = s.Users()
			}
		}()
	}
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Grant(ctx, "alice", fmt.Sprintf("db%d", i), LevelRead); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(stop)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if u, _ := s.User("alice"); len(u.Grants) != n {
		t.Fatalf("alice has %d grants, want %d: %v", len(u.Grants), n, u.Grants)
	}
}

// TestAuthConcurrentStoresCompareAndSet: two stores on one engine do not
// share a lock, so only the compare-and-set on the user's rev keeps their
// concurrent grants on one user from overwriting each other.
func TestAuthConcurrentStoresCompareAndSet(t *testing.T) {
	ctx := context.Background()
	s1, e := newTestStore(t)
	s2, err := Open(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	mustCreateUser(t, s1, "alice", UserOptions{})
	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for i := range n {
		for j, s := range []*Store{s1, s2} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Grant(ctx, "alice", fmt.Sprintf("s%d-db%d", j, i), LevelWrite); err != nil {
					errs <- err
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	s3, err := Open(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if u, _ := s3.User("alice"); len(u.Grants) != 2*n {
		t.Fatalf("alice has %d grants, want %d (a concurrent change was lost): %v", len(u.Grants), 2*n, u.Grants)
	}
}

// TestAuthLoadTolerates: documents written by an older version or left by
// a crash load safely: a user without rev can be changed, an unknown
// grant level grants nothing, and a token whose user is gone
// authenticates nobody.
func TestAuthLoadTolerates(t *testing.T) {
	ctx := context.Background()
	s, e := newTestStore(t)
	sys, err := e.SystemDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	users, _ := sys.Collection(usersCollection)
	tokens, _ := sys.Collection(tokensCollection)
	if _, err := users.Insert(ctx, &db.Document{ID: "old", Data: map[string]any{
		fieldServerAdmin: false,
		fieldGrants:      map[string]any{"shop": "write", "bad": "superuser", "num": 3},
		fieldCreated:     "not a time",
	}}); err != nil {
		t.Fatal(err)
	}
	orphan := TokenPrefix + "orphan-token-0123456789"
	if _, err := tokens.Insert(ctx, &db.Document{ID: sha256Hex(orphan), Data: map[string]any{fieldUser: "gone", fieldLabel: "x"}}); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	_ = s
	u, err := s2.User("old")
	if err != nil {
		t.Fatal(err)
	}
	if len(u.Grants) != 1 || u.Grants["shop"] != LevelWrite || !u.Created.IsZero() {
		t.Fatalf("old user = %+v", u)
	}
	if _, err := s2.Authenticate(orphan); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("orphan token: %v", err)
	}
	if err := s2.Grant(ctx, "old", "logs", LevelRead); err != nil {
		t.Fatalf("Grant on a user without rev: %v", err)
	}
	doc, err := users.Get(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	if rev, ok := toInt64(doc.Data[fieldRev]); !ok || rev != 1 {
		t.Fatalf("rev after the first change = %v", doc.Data[fieldRev])
	}
	if doc.Data[fieldCreated] != "not a time" {
		t.Fatalf("created not kept: %v", doc.Data[fieldCreated])
	}
	for v, want := range map[any]int64{int(3): 3, int64(4): 4, float64(5): 5, json.Number("6"): 6} {
		if got, ok := toInt64(v); !ok || got != want {
			t.Errorf("toInt64(%#v) = %d, %v", v, got, ok)
		}
	}
	if _, ok := toInt64("7"); ok {
		t.Error("toInt64 accepts a string")
	}
	if _, ok := toInt64(json.Number("x")); ok {
		t.Error("toInt64 accepts a bad json.Number")
	}
}
