package auth

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db"
)

const (
	bootTokenA = "0123456789abcdef0123456789abcdef" // 32 characters
	bootTokenB = "fedcba9876543210fedcba9876543210ffff"
)

func TestAuthValidateBootstrapToken(t *testing.T) {
	tests := []struct {
		name  string
		token string
		ok    bool
	}{
		{"32 hex", bootTokenA, true},
		{"base64 with padding", strings.Repeat("Ab+/", 8) + "==", true},
		{"base64url and dots", strings.Repeat("a-_.~", 8), true},
		{"512 characters", strings.Repeat("a", 512), true},
		{"generated token", TokenPrefix + strings.Repeat("Z", 43), true},
		{"31 characters", bootTokenA[:31], false},
		{"513 characters", strings.Repeat("a", 513), false},
		{"space", strings.Repeat("a", 20) + " " + strings.Repeat("b", 20), false},
		{"padding in the middle", strings.Repeat("a", 20) + "=" + strings.Repeat("b", 20), false},
		{"quote", strings.Repeat("a", 32) + `"`, false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBootstrapToken(tt.token)
			if tt.ok {
				if err != nil {
					t.Fatalf("ValidateBootstrapToken: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidBootstrapToken) {
				t.Fatalf("err = %v, want ErrInvalidBootstrapToken", err)
			}
			if len(tt.token) >= 8 && strings.Contains(err.Error(), tt.token) {
				t.Fatalf("error %q contains the token", err)
			}
		})
	}
}

// TestAuthBootstrapEnsureAdmin walks EnsureAdmin through creation,
// idempotence, rotation, promotion, recreation, and refusal.
func TestAuthBootstrapEnsureAdmin(t *testing.T) {
	ctx := context.Background()
	s, e := newTestStore(t)

	res, err := s.EnsureAdmin(ctx, BootstrapAdmin, bootTokenA)
	if err != nil {
		t.Fatalf("EnsureAdmin: %v", err)
	}
	if want := (BootstrapResult{CreatedUser: true, AddedToken: true}); res != want {
		t.Fatalf("first EnsureAdmin = %+v, want %+v", res, want)
	}
	p, err := s.Authenticate(bootTokenA)
	if err != nil || p.Name != BootstrapAdmin || !p.ServerAdmin {
		t.Fatalf("Authenticate(bootstrap) = %+v, %v", p, err)
	}
	u, _ := s.User(BootstrapAdmin)
	if len(u.Tokens) != 1 || u.Tokens[0].Label != BootstrapLabel {
		t.Fatalf("admin tokens = %+v, want one labeled %s", u.Tokens, BootstrapLabel)
	}

	// Idempotent.
	res, err = s.EnsureAdmin(ctx, BootstrapAdmin, bootTokenA)
	if err != nil || res.Changed() {
		t.Fatalf("second EnsureAdmin = %+v, %v; want no change", res, err)
	}

	// Another token of the admin is kept across a rotation.
	other, _, err := s.CreateToken(ctx, BootstrapAdmin, "laptop")
	if err != nil {
		t.Fatal(err)
	}
	res, err = s.EnsureAdmin(ctx, BootstrapAdmin, bootTokenB)
	if err != nil {
		t.Fatal(err)
	}
	if want := (BootstrapResult{AddedToken: true, ReplacedToken: true}); res != want {
		t.Fatalf("rotation = %+v, want %+v", res, want)
	}
	if _, err := s.Authenticate(bootTokenA); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old bootstrap token after rotation: %v", err)
	}
	for _, tok := range []string{bootTokenB, other} {
		if _, err := s.Authenticate(tok); err != nil {
			t.Fatalf("token after rotation: %v", err)
		}
	}

	// An existing admin user without the flag is promoted.
	mustCreateUser(t, s, "root", UserOptions{ServerAdmin: true})
	if err := s.SetServerAdmin(ctx, BootstrapAdmin, false); err != nil {
		t.Fatal(err)
	}
	res, err = s.EnsureAdmin(ctx, BootstrapAdmin, bootTokenB)
	if err != nil {
		t.Fatal(err)
	}
	if want := (BootstrapResult{MadeServerAdmin: true}); res != want {
		t.Fatalf("promotion = %+v, want %+v", res, want)
	}
	if u, _ := s.User(BootstrapAdmin); !u.ServerAdmin {
		t.Fatal("admin not promoted")
	}

	// A deleted admin is recreated.
	if err := s.DeleteUser(ctx, BootstrapAdmin); err != nil {
		t.Fatal(err)
	}
	res, err = s.EnsureAdmin(ctx, BootstrapAdmin, bootTokenB)
	if err != nil || !res.CreatedUser || !res.AddedToken {
		t.Fatalf("recreate = %+v, %v", res, err)
	}

	// A value that is another user's token is refused, without the token
	// in the message.
	rootTok, _, err := s.CreateToken(ctx, "root", "long")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.EnsureAdmin(ctx, BootstrapAdmin, rootTok)
	if !errors.Is(err, ErrInvalidBootstrapToken) || !strings.Contains(err.Error(), `"root"`) || strings.Contains(err.Error(), rootTok) {
		t.Fatalf("other user's token: err = %v", err)
	}
	if p, err := s.Authenticate(rootTok); err != nil || p.Name != "root" {
		t.Fatalf("root's token after the refusal = %+v, %v", p, err)
	}

	// Invalid input.
	if _, err := s.EnsureAdmin(ctx, BootstrapAdmin, "short"); !errors.Is(err, ErrInvalidBootstrapToken) {
		t.Fatalf("short token: %v", err)
	}
	if _, err := s.EnsureAdmin(ctx, "bad name", bootTokenA); !errors.Is(err, db.ErrInvalidName) {
		t.Fatalf("bad name: %v", err)
	}
	_ = e
}

// TestAuthBootstrapOrphanToken: a leftover token document of a removed
// user (a crash between deleting the tokens and the user) does not block
// the bootstrap token with the same value.
func TestAuthBootstrapOrphanToken(t *testing.T) {
	ctx := context.Background()
	s, e := newTestStore(t)
	sys, err := e.SystemDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tokens, _ := sys.Collection(tokensCollection)
	if _, err := tokens.Insert(ctx, &db.Document{ID: sha256Hex(bootTokenA), Data: map[string]any{fieldUser: "gone", fieldLabel: "x"}}); err != nil {
		t.Fatal(err)
	}
	// Also a leftover of the admin itself, stored before the user exists.
	res, err := s.EnsureAdmin(ctx, BootstrapAdmin, bootTokenA)
	if err != nil || !res.CreatedUser || !res.AddedToken {
		t.Fatalf("EnsureAdmin over an orphan = %+v, %v", res, err)
	}
	if p, err := s.Authenticate(bootTokenA); err != nil || p.Name != BootstrapAdmin {
		t.Fatalf("Authenticate = %+v, %v", p, err)
	}
	doc, err := tokens.Get(ctx, sha256Hex(bootTokenA))
	if err != nil || doc.Data[fieldUser] != BootstrapAdmin || doc.Data[fieldLabel] != BootstrapLabel {
		t.Fatalf("token document = %+v, %v", doc, err)
	}

	// A token document that names the admin while the admin user is gone
	// is replaced too.
	if err := s.users.DeleteWith(ctx, BootstrapAdmin, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	res, err = s.EnsureAdmin(ctx, BootstrapAdmin, bootTokenA)
	if err != nil || !res.CreatedUser || !res.AddedToken {
		t.Fatalf("EnsureAdmin over the admin's leftover = %+v, %v", res, err)
	}
}
