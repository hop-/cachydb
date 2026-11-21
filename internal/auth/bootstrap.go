package auth

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/hop-/cachydb/pkg/db"
)

// BootstrapAdmin is the user the bootstrap token (BOOTSTRAP_ADMIN_TOKEN)
// belongs to.
const BootstrapAdmin = "admin"

// BootstrapLabel is the label of the bootstrap token.
const BootstrapLabel = "bootstrap"

// Bootstrap token length bounds, in characters.
const (
	minBootstrapToken = 32
	maxBootstrapToken = 512
)

// ErrInvalidBootstrapToken reports a bootstrap token that is too short, too
// long, or not a bearer token.
var ErrInvalidBootstrapToken = errors.New("invalid bootstrap token")

// bootstrapTokenPattern is RFC 6750's b64token, the form of a bearer token
// in an Authorization header: base64, base64url, hex, and generated tokens
// all match it.
var bootstrapTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)

// ValidateBootstrapToken checks an operator-chosen token: 32 to 512
// characters of A-Z a-z 0-9 - . _ ~ + /, optionally followed by "=". The
// error never contains the token.
func ValidateBootstrapToken(token string) error {
	switch {
	case len(token) < minBootstrapToken:
		return fmt.Errorf("%w: it has %d characters, want at least %d (for example the output of \"openssl rand -hex 32\")",
			ErrInvalidBootstrapToken, len(token), minBootstrapToken)
	case len(token) > maxBootstrapToken:
		return fmt.Errorf("%w: it has %d characters, want at most %d", ErrInvalidBootstrapToken, len(token), maxBootstrapToken)
	case !bootstrapTokenPattern.MatchString(token):
		return fmt.Errorf("%w: use only letters, digits, and - . _ ~ + / (optionally ending in =); no spaces", ErrInvalidBootstrapToken)
	}
	return nil
}

// BootstrapResult says what EnsureAdmin changed; all false means nothing.
type BootstrapResult struct {
	// CreatedUser: the user did not exist and was created.
	CreatedUser bool
	// MadeServerAdmin: the user existed without the server-admin flag.
	MadeServerAdmin bool
	// AddedToken: the token was stored (labeled BootstrapLabel).
	AddedToken bool
	// ReplacedToken: an earlier BootstrapLabel token of the user, a
	// different one, was revoked first.
	ReplacedToken bool
}

// Changed reports whether EnsureAdmin changed anything.
func (r BootstrapResult) Changed() bool {
	return r.CreatedUser || r.MadeServerAdmin || r.AddedToken || r.ReplacedToken
}

// EnsureAdmin makes sure the user name exists, is a server admin, and
// holds token, labeled BootstrapLabel; it is idempotent. If the user holds
// a different BootstrapLabel token (the setting changed since the last
// start), that token is revoked, so only the current value works; other
// tokens of the user are kept. A token that belongs to another user is an
// error. The token must pass ValidateBootstrapToken; only its digest is
// stored, and no error contains it.
func (s *Store) EnsureAdmin(ctx context.Context, name, token string) (BootstrapResult, error) {
	var res BootstrapResult
	if err := db.ValidateName("user", name); err != nil {
		return res, err
	}
	if err := ValidateBootstrapToken(token); err != nil {
		return res, err
	}
	dig := digest(token)
	err := s.change(ctx, func() error {
		held, err := s.claimDigest(ctx, name, dig)
		if err != nil {
			return err
		}
		snap := s.snap.Load()
		u, exists := snap.users[name]
		switch {
		case !exists:
			created := storedUser{
				Principal: Principal{Name: name, ServerAdmin: true},
				rev:       1,
				created:   map[string]any{"$now": true},
			}
			if _, err := s.users.Insert(ctx, created.document()); err != nil {
				return fmt.Errorf("creating user %q: %w", name, err)
			}
			res.CreatedUser = true
		case !u.principal.ServerAdmin:
			err := s.changeUserLocked(ctx, name, func(su *storedUser) error {
				su.ServerAdmin = true
				return nil
			})
			if err != nil {
				return err
			}
			res.MadeServerAdmin = true
		}
		if held {
			return nil
		}
		// Revoke the earlier bootstrap token first: labels are unique per
		// user, and a crash in between leaves the user without one, which
		// the next start repairs.
		for oldDig, t := range snap.tokens {
			if t.user == name && t.label == BootstrapLabel {
				if err := s.tokens.DeleteWith(ctx, oldDig, nil); err != nil && !errors.Is(err, db.ErrNotFound) {
					return fmt.Errorf("revoking the earlier bootstrap token of user %q: %w", name, err)
				}
				res.ReplacedToken = true
			}
		}
		if err := s.insertToken(ctx, name, BootstrapLabel, dig); err != nil {
			return err
		}
		res.AddedToken = true
		return nil
	})
	return res, err
}

// claimDigest reports whether the stored token with digest dig already
// belongs to the user name. A token of another existing user is an error;
// a leftover token of a removed user is deleted so the digest can be
// stored again.
func (s *Store) claimDigest(ctx context.Context, name, dig string) (held bool, err error) {
	doc, err := s.tokens.Get(ctx, dig)
	switch {
	case errors.Is(err, db.ErrNotFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("looking up the bootstrap token: %w", err)
	}
	owner, _ := doc.Data[fieldUser].(string)
	if owner == name {
		if _, ok := s.snap.Load().users[name]; ok {
			return true, nil
		}
	} else if _, ok := s.snap.Load().users[owner]; ok {
		return false, fmt.Errorf("%w: it is already a token of user %q; choose another value", ErrInvalidBootstrapToken, owner)
	}
	if err := s.tokens.DeleteWith(ctx, dig, nil); err != nil && !errors.Is(err, db.ErrNotFound) {
		return false, fmt.Errorf("removing a leftover token: %w", err)
	}
	return false, nil
}
