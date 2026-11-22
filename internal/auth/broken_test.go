package auth

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/hop-/cachydb/pkg/db"
)

// Test seam for the store's "broken" state: the in-memory copy can only
// go stale when the reload after a change fails. deadCollection returns a
// collection of a closed engine, whose every call fails; swapping it into
// a Store under the change lock (all refreshes and writes run under it)
// makes the next reload fail without touching the real data.

func deadCollection(t *testing.T) *db.Collection {
	t.Helper()
	ctx := context.Background()
	e, err := db.Open(ctx, db.Options{Dir: t.TempDir(), Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	d, err := e.CreateDatabase(ctx, "dead")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	c, err := d.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Find(ctx, nil); err == nil {
		t.Fatal("a collection of a closed engine still answers")
	}
	return c
}

// swapUsers replaces s.users under the change lock and returns the old
// collection.
func swapUsers(s *Store, c *db.Collection) *db.Collection {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.users
	s.users = c
	return old
}

func swapTokens(s *Store, c *db.Collection) *db.Collection {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.tokens
	s.tokens = c
	return old
}

// TestAuthBrokenStoreFailsClosed: RevokeToken deletes the token, then the
// reload fails. The stale copy still holds the revoked token, so the
// store must refuse every authentication until a reload succeeds; then it
// recovers by itself, with the token revoked.
func TestAuthBrokenStoreFailsClosed(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	aliceTok := mustCreateUser(t, s, "alice", UserOptions{})
	laptop, _, err := s.CreateToken(ctx, "alice", "laptop")
	if err != nil {
		t.Fatal(err)
	}
	alice, err := s.Authenticate(aliceTok)
	if err != nil {
		t.Fatal(err)
	}

	users := swapUsers(s, deadCollection(t)) // RevokeToken writes only tokens
	err = s.RevokeToken(ctx, "alice", "laptop")
	if err == nil {
		t.Fatal("RevokeToken with a failing reload returned nil")
	}
	if !s.broken.Load() {
		t.Fatal("store not marked broken after a failed reload")
	}

	for name, tok := range map[string]string{"revoked token": laptop, "valid token": aliceTok} {
		if _, err := s.Authenticate(tok); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("Authenticate(%s) on a broken store = %v, want ErrUnauthenticated", name, err)
		}
	}
	if _, err := s.Principal("alice"); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("Principal on a broken store = %v, want ErrUnauthenticated", err)
	}
	if err := s.Authorize(alice, "x", LevelNone); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("Authorize on a broken store = %v, want ErrUnauthenticated", err)
	}
	if err := s.Authorize(FullAccess(), "x", LevelServer); err != nil {
		t.Errorf("the implicit principal needs no store: %v", err)
	}

	// The fault clears: the next call reloads and recovers.
	swapUsers(s, users)
	if _, err := s.Authenticate(laptop); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked token after recovery: %v", err)
	}
	if s.broken.Load() {
		t.Fatal("store still broken after a successful reload")
	}
	if p, err := s.Authenticate(aliceTok); err != nil || p.Name != "alice" {
		t.Fatalf("valid token after recovery = %+v, %v", p, err)
	}
}

// TestAuthBrokenStoreConcurrent: authentications racing with a store
// that goes broken and recovers never return a stale success for the
// revoked token after the revoke returned, and are race-free.
func TestAuthBrokenStoreConcurrent(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	aliceTok := mustCreateUser(t, s, "alice", UserOptions{})
	dead := deadCollection(t)

	for round := range 5 {
		victim, label, err := s.CreateToken(ctx, "alice", "")
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					_, _ = s.Authenticate(aliceTok)
					_, _ = s.Principal("alice")
				}
			}()
		}
		users := swapUsers(s, dead)
		if err := s.RevokeToken(ctx, "alice", label); err == nil {
			t.Fatalf("round %d: RevokeToken with a failing reload returned nil", round)
		}
		if _, err := s.Authenticate(victim); err == nil {
			t.Fatalf("round %d: revoked token authenticated on a broken store", round)
		}
		swapUsers(s, users)
		close(stop)
		wg.Wait()
		if _, err := s.Authenticate(victim); err == nil {
			t.Fatalf("round %d: revoked token authenticated after recovery", round)
		}
		if _, err := s.Authenticate(aliceTok); err != nil {
			t.Fatalf("round %d: valid token after recovery: %v", round, err)
		}
	}
}

// TestAuthBrokenStoreKeepsLastServerAdmin: while the store is broken its
// copy may be stale, so a change must not decide the "last server admin"
// rule from it. Here demoting root succeeds in storage but the reload
// fails, so the copy still lists root as an admin; demoting admin2 next
// must not pass the guard on that stale copy and leave no server admin.
func TestAuthBrokenStoreKeepsLastServerAdmin(t *testing.T) {
	ctx := context.Background()
	s, e := newTestStore(t)
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	mustCreateUser(t, s, "admin2", UserOptions{ServerAdmin: true})

	tokens := swapTokens(s, deadCollection(t)) // SetAccess writes only users
	if err := s.SetServerAdmin(ctx, "root", false); err == nil {
		t.Fatal("SetServerAdmin with a failing reload returned nil")
	}
	_ = s.SetServerAdmin(ctx, "admin2", false) // must fail, one way or another
	swapTokens(s, tokens)

	s2, err := Open(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	admins := 0
	for _, u := range s2.Users() {
		if u.ServerAdmin {
			admins++
		}
	}
	if admins == 0 {
		t.Fatal("no server admin left: the last-server-admin guard passed on the stale copy of a broken store")
	}
}
