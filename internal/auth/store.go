package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

// Collections and fields of the system database.
const (
	usersCollection  = "users"
	tokensCollection = "tokens"
	tokenUserIndex   = "user"

	fieldServerAdmin     = "server_admin"
	fieldDefaultDatabase = "default_database"
	fieldGrants          = "grants"
	fieldCreated         = "created"
	// fieldRev counts the changes of a user document; every change is a
	// compare-and-set on it, so concurrent edits of one user never lose
	// each other's grants.
	fieldRev = "rev"

	fieldUser  = "user"
	fieldLabel = "label"
)

// TokenPrefix starts every token.
const TokenPrefix = "cdb_"

// tokenBytes is the number of random bytes in a token.
const tokenBytes = 32

// DefaultTokenLabel is the label of a token created without one, and of
// the first token CreateUser makes.
const DefaultTokenLabel = "default"

// maxChangeAttempts bounds the compare-and-set retries of one user change.
const maxChangeAttempts = 100

// deleteChunk is the most tokens one DeleteMany call removes.
const deleteChunk = 1000

// User is a stored user as Users and User return it. Tokens lists the
// user's tokens by label, never the secret or its digest.
type User struct {
	Name            string
	ServerAdmin     bool
	DefaultDatabase string
	Grants          map[string]Level
	Created         time.Time
	Tokens          []Token
}

// Token describes one token of a user.
type Token struct {
	Label   string
	Created time.Time
}

// UserOptions are the settings of a new user.
type UserOptions struct {
	ServerAdmin     bool
	DefaultDatabase string // empty: none
}

// Store keeps users, token digests, and grants in the system database. All
// changes must go through one Store per engine: it serializes them,
// writes each user change as a compare-and-set, and refreshes its
// in-memory copy afterwards. Reads (Authenticate, Authorize, Users) use
// that copy and are safe for concurrent use.
type Store struct {
	users  *db.Collection
	tokens *db.Collection

	mu   sync.Mutex // serializes changes and refreshes
	snap atomic.Pointer[snapshot]
	// broken is set when the refresh after a change failed, so snap may
	// be stale (a revoked token or deleted user still in it). Until a
	// refresh succeeds, Authenticate and Principal fail closed.
	broken atomic.Bool
}

// snapshot is an immutable copy of the stored users and tokens.
type snapshot struct {
	users  map[string]*userRec
	tokens map[string]tokenRec // by digest
}

type userRec struct {
	principal Principal // Grants is shared: never modified, cloned on the way out
	created   time.Time
	tokens    []Token // sorted by label
}

type tokenRec struct {
	user  string
	label string
}

// Open opens the store on e: it creates the system database and its
// collections and index if they are missing, and loads the users and
// tokens.
func Open(ctx context.Context, e *db.Engine) (*Store, error) {
	d, err := e.SystemDatabase(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening the auth store: %w", err)
	}
	for _, name := range []string{usersCollection, tokensCollection} {
		if err := d.CreateCollection(ctx, name, nil); err != nil && !errors.Is(err, db.ErrExists) {
			return nil, fmt.Errorf("opening the auth store: %w", err)
		}
	}
	s := &Store{}
	if s.users, err = d.Collection(usersCollection); err != nil {
		return nil, fmt.Errorf("opening the auth store: %w", err)
	}
	if s.tokens, err = d.Collection(tokensCollection); err != nil {
		return nil, fmt.Errorf("opening the auth store: %w", err)
	}
	if err := s.tokens.CreateIndex(ctx, tokenUserIndex, fieldUser); err != nil && !errors.Is(err, db.ErrExists) {
		return nil, fmt.Errorf("opening the auth store: %w", err)
	}
	if err := s.refresh(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// refresh reloads the in-memory copy from the collections.
func (s *Store) refresh(ctx context.Context) error {
	userDocs, err := s.users.Find(ctx, nil)
	if err != nil {
		return fmt.Errorf("loading users: %w", err)
	}
	tokenDocs, err := s.tokens.Find(ctx, nil)
	if err != nil {
		return fmt.Errorf("loading tokens: %w", err)
	}
	snap := &snapshot{
		users:  make(map[string]*userRec, len(userDocs)),
		tokens: make(map[string]tokenRec, len(tokenDocs)),
	}
	for _, doc := range userDocs {
		u := decodeUser(doc)
		snap.users[u.Name] = &userRec{principal: u.Principal, created: u.createdAt}
	}
	for _, doc := range tokenDocs {
		user, _ := doc.Data[fieldUser].(string)
		label, _ := doc.Data[fieldLabel].(string)
		u, ok := snap.users[user]
		if !ok {
			// A token whose user is gone (removing a user deletes its
			// tokens first, so only a crash in between leaves one):
			// it authenticates nobody.
			continue
		}
		snap.tokens[doc.ID] = tokenRec{user: user, label: label}
		u.tokens = append(u.tokens, Token{Label: label, Created: parseTime(doc.Data[fieldCreated])})
	}
	for _, u := range snap.users {
		slices.SortFunc(u.tokens, func(a, b Token) int { return strings.Compare(a.Label, b.Label) })
	}
	s.snap.Store(snap)
	s.broken.Store(false)
	return nil
}

// change runs fn under the change lock and refreshes the in-memory copy
// afterwards, also when fn fails (it may have written part of a change).
// fn may decide from s.snap (the last-server-admin rule, token lookups):
// if the store is broken, change reloads it first and, if that fails,
// returns the error without running fn, so no change is decided on a
// stale copy. The refresh after fn ignores ctx's cancellation and is
// retried once. If it still fails, the store is marked broken:
// Authenticate and Principal then fail closed, retrying the refresh,
// until one succeeds.
func (s *Store) change(ctx context.Context, fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reloadIfBrokenLocked(ctx); err != nil {
		return fmt.Errorf("%w; nothing was changed", err)
	}
	err := fn()
	rctx := context.WithoutCancel(ctx)
	rerr := s.refresh(rctx)
	if rerr != nil {
		rerr = s.refresh(rctx)
	}
	if rerr != nil {
		s.broken.Store(true)
		return errors.Join(err, fmt.Errorf("reloading the users after a change (authentication fails until a reload succeeds): %w", rerr))
	}
	return err
}

// current returns the in-memory copy. If the store is broken it first
// retries the refresh, and returns an error wrapping ErrUnauthenticated if
// that fails too.
func (s *Store) current() (*snapshot, error) {
	if !s.broken.Load() {
		return s.snap.Load(), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reloadIfBrokenLocked(context.Background()); err != nil {
		return nil, fmt.Errorf("%w: the user store could not be reloaded", ErrUnauthenticated)
	}
	return s.snap.Load(), nil
}

// reloadIfBrokenLocked refreshes the in-memory copy if the store is
// broken. The caller holds s.mu.
func (s *Store) reloadIfBrokenLocked(ctx context.Context) error {
	if !s.broken.Load() {
		return nil
	}
	if err := s.refresh(ctx); err != nil {
		return fmt.Errorf("the user store could not be reloaded after an earlier failure: %w", err)
	}
	return nil
}

// HasUsers reports whether at least one user exists.
func (s *Store) HasUsers() bool { return len(s.snap.Load().users) > 0 }

// Users returns every user, sorted by name.
func (s *Store) Users() []User {
	snap := s.snap.Load()
	out := make([]User, 0, len(snap.users))
	for _, name := range slices.Sorted(maps.Keys(snap.users)) {
		out = append(out, snap.users[name].user())
	}
	return out
}

// User returns the user name; db.ErrNotFound if there is none.
func (s *Store) User(name string) (User, error) {
	u, ok := s.snap.Load().users[name]
	if !ok {
		return User{}, errNoUser(name)
	}
	return u.user(), nil
}

func (u *userRec) user() User {
	return User{
		Name:            u.principal.Name,
		ServerAdmin:     u.principal.ServerAdmin,
		DefaultDatabase: u.principal.DefaultDatabase,
		Grants:          maps.Clone(u.principal.Grants),
		Created:         u.created,
		Tokens:          slices.Clone(u.tokens),
	}
}

// Principal returns the current principal of the user name, or an error
// wrapping ErrUnauthenticated if the user does not exist.
func (s *Store) Principal(name string) (Principal, error) {
	snap, err := s.current()
	if err != nil {
		return Principal{}, err
	}
	u, ok := snap.users[name]
	if !ok {
		return Principal{}, fmt.Errorf("%w: user %q does not exist", ErrUnauthenticated, name)
	}
	return u.principal.clone(), nil
}

// Authenticate returns the principal of the user holding token, or an
// error wrapping ErrUnauthenticated for an unknown or revoked token. The
// token is looked up by its SHA-256 digest and never appears in the error.
// Generated tokens start with TokenPrefix; a bootstrap token (EnsureAdmin)
// may have any form ValidateBootstrapToken accepts.
func (s *Store) Authenticate(token string) (Principal, error) {
	if token != "" {
		snap, err := s.current()
		if err != nil {
			return Principal{}, err
		}
		if t, ok := snap.tokens[digest(token)]; ok {
			if u, ok := snap.users[t.user]; ok {
				return u.principal.clone(), nil
			}
		}
	}
	return Principal{}, fmt.Errorf("%w: unknown or revoked token", ErrUnauthenticated)
}

// Authorize returns nil if p may act at level need on database, and an
// error wrapping ErrPermissionDenied, naming the database and the level,
// if not. The implicit full-access principal is always allowed. For any
// other principal the user's current grants decide (a change made after
// the principal was authenticated applies at once); a user that no longer
// exists fails with ErrUnauthenticated. For LevelServer database is not
// used.
func (s *Store) Authorize(p Principal, database string, need Level) error {
	if p.Implicit {
		return nil
	}
	cur, err := s.Principal(p.Name)
	if err != nil {
		return err
	}
	if cur.Can(database, need) {
		return nil
	}
	return denied(cur, database, need)
}

// CreateUser adds the user name and returns its first token (label
// "default"), which is shown only this once. The name follows the
// database-name rule; db.ErrExists if the user exists.
func (s *Store) CreateUser(ctx context.Context, name string, opts UserOptions) (string, error) {
	if err := db.ValidateName("user", name); err != nil {
		return "", err
	}
	if opts.DefaultDatabase != "" {
		if err := db.ValidateName("database", opts.DefaultDatabase); err != nil {
			return "", err
		}
	}
	token, dig, err := newToken()
	if err != nil {
		return "", err
	}
	err = s.change(ctx, func() error {
		u := storedUser{
			Principal: Principal{Name: name, ServerAdmin: opts.ServerAdmin, DefaultDatabase: opts.DefaultDatabase},
			rev:       1,
			created:   map[string]any{"$now": true},
		}
		if _, err := s.users.Insert(ctx, u.document()); err != nil {
			if errors.Is(err, db.ErrExists) {
				return fmt.Errorf("user %q: %w", name, db.ErrExists)
			}
			return fmt.Errorf("creating user %q: %w", name, err)
		}
		if err := s.insertToken(ctx, name, DefaultTokenLabel, dig); err != nil {
			// Undo the user so a retry can create it; a crash here
			// leaves a user without tokens, which authenticates nobody.
			if derr := s.users.DeleteWith(ctx, name, nil); derr != nil {
				return errors.Join(err, fmt.Errorf("removing user %q again: %w", name, derr))
			}
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return token, nil
}

// DeleteUser removes the user name and its tokens; db.ErrNotFound if
// there is no such user, ErrLastServerAdmin if it is the only server
// admin.
func (s *Store) DeleteUser(ctx context.Context, name string) error {
	return s.change(ctx, func() error {
		snap := s.snap.Load()
		u, ok := snap.users[name]
		if !ok {
			return errNoUser(name)
		}
		if u.principal.ServerAdmin && snap.lastServerAdmin(name) {
			return errLastServerAdmin(name, "remove")
		}
		// Tokens first: a crash in between leaves a user without tokens,
		// never a token of a removed user.
		q := &db.Query{Filters: []db.QueryFilter{{Field: fieldUser, Operator: "eq", Value: name}}}
		for {
			matched, _, err := s.tokens.DeleteMany(ctx, q, deleteChunk)
			if err != nil {
				return fmt.Errorf("removing the tokens of user %q: %w", name, err)
			}
			if matched == 0 {
				break
			}
		}
		if err := s.users.DeleteWith(ctx, name, nil); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return errNoUser(name)
			}
			return fmt.Errorf("removing user %q: %w", name, err)
		}
		return nil
	})
}

// Grant gives the user name level (read, write, or admin) on database, a
// database name or AllDatabases, replacing an earlier grant on it.
func (s *Store) Grant(ctx context.Context, name, database string, level Level) error {
	if err := validGrantDatabase(database); err != nil {
		return err
	}
	if !level.grantable() {
		return fmt.Errorf("%w: %s (want read, write, or admin)", ErrInvalidLevel, level)
	}
	return s.changeUser(ctx, name, func(u *storedUser) error {
		grants := maps.Clone(u.Grants)
		if grants == nil {
			grants = make(map[string]Level)
		}
		grants[database] = level
		u.Grants = grants
		return nil
	})
}

// Revoke removes the grant of the user name on database; db.ErrNotFound
// if the user has none there.
func (s *Store) Revoke(ctx context.Context, name, database string) error {
	return s.changeUser(ctx, name, func(u *storedUser) error {
		if _, ok := u.Grants[database]; !ok {
			return fmt.Errorf("grant of user %q on database %q: %w", name, database, db.ErrNotFound)
		}
		grants := maps.Clone(u.Grants)
		delete(grants, database)
		u.Grants = grants
		return nil
	})
}

// SetServerAdmin sets or clears the server-admin flag of the user name.
// Clearing it on the only server admin fails with ErrLastServerAdmin.
func (s *Store) SetServerAdmin(ctx context.Context, name string, admin bool) error {
	return s.SetAccess(ctx, name, AccessChange{ServerAdmin: &admin})
}

// AccessChange is one change of a user's access for SetAccess. Nil fields
// are left alone.
type AccessChange struct {
	// Database and Level set the grant on Database (a database name or
	// AllDatabases) to Level; LevelNone removes it (nothing to remove is
	// not an error). Level is used only when Database is set.
	Database string
	Level    Level
	// ServerAdmin sets or clears the server-admin flag.
	ServerAdmin *bool
	// DefaultDatabase sets the default database; "" clears it.
	DefaultDatabase *string
}

// IsZero reports whether c changes nothing.
func (c AccessChange) IsZero() bool {
	return c.Database == "" && c.ServerAdmin == nil && c.DefaultDatabase == nil
}

// SetAccess applies every part of c to the user name in one write, so the
// parts take effect together or not at all. Clearing the server-admin flag
// of the only server admin fails with ErrLastServerAdmin.
func (s *Store) SetAccess(ctx context.Context, name string, c AccessChange) error {
	if c.Database != "" {
		if err := validGrantDatabase(c.Database); err != nil {
			return err
		}
		if c.Level != LevelNone && !c.Level.grantable() {
			return fmt.Errorf("%w: %s (want none, read, write, or admin)", ErrInvalidLevel, c.Level)
		}
	}
	if c.DefaultDatabase != nil && *c.DefaultDatabase != "" {
		if err := db.ValidateName("database", *c.DefaultDatabase); err != nil {
			return err
		}
	}
	return s.changeUser(ctx, name, func(u *storedUser) error {
		if c.ServerAdmin != nil && !*c.ServerAdmin && u.ServerAdmin && s.snap.Load().lastServerAdmin(name) {
			return errLastServerAdmin(name, "demote")
		}
		if c.Database != "" {
			grants := maps.Clone(u.Grants)
			if grants == nil {
				grants = make(map[string]Level)
			}
			if c.Level == LevelNone {
				delete(grants, c.Database)
			} else {
				grants[c.Database] = c.Level
			}
			u.Grants = grants
		}
		if c.ServerAdmin != nil {
			u.ServerAdmin = *c.ServerAdmin
		}
		if c.DefaultDatabase != nil {
			u.DefaultDatabase = *c.DefaultDatabase
		}
		return nil
	})
}

// SetDefaultDatabase sets the default database of the user name; an empty
// database clears it.
func (s *Store) SetDefaultDatabase(ctx context.Context, name, database string) error {
	return s.SetAccess(ctx, name, AccessChange{DefaultDatabase: &database})
}

// ForgetDatabase removes every user's grant on database (not grants on
// AllDatabases), so a database created later under the same name does not
// inherit them. Call it after deleting a database.
func (s *Store) ForgetDatabase(ctx context.Context, database string) error {
	if database == AllDatabases {
		return nil
	}
	// The names come from the copy, so reload a broken store first: a
	// stale copy could miss a user holding a grant on database.
	s.mu.Lock()
	err := s.reloadIfBrokenLocked(ctx)
	snap := s.snap.Load()
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("removing grants on database %q: %w", database, err)
	}
	var names []string
	for name, u := range snap.users {
		if _, ok := u.principal.Grants[database]; ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	var errs []error
	for _, name := range names {
		err := s.changeUser(ctx, name, func(u *storedUser) error {
			if _, ok := u.Grants[database]; !ok {
				return errUnchanged
			}
			grants := maps.Clone(u.Grants)
			delete(grants, database)
			u.Grants = grants
			return nil
		})
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("removing grants on database %q: %w", database, err)
	}
	return nil
}

// CreateToken adds a token labeled label to the user name and returns it
// with its label; the token is shown only this once. An empty label picks
// the first free one of "default", "default-2", "default-3", ...; labels
// follow the database-name rule and are unique per user (db.ErrExists).
func (s *Store) CreateToken(ctx context.Context, name, label string) (token, usedLabel string, err error) {
	if label != "" {
		if err := db.ValidateName("token label", label); err != nil {
			return "", "", err
		}
	}
	token, dig, err := newToken()
	if err != nil {
		return "", "", err
	}
	err = s.change(ctx, func() error {
		u, ok := s.snap.Load().users[name]
		if !ok {
			return errNoUser(name)
		}
		taken := func(l string) bool {
			return slices.ContainsFunc(u.tokens, func(t Token) bool { return t.Label == l })
		}
		if label == "" {
			label = DefaultTokenLabel
			for i := 2; taken(label); i++ {
				label = DefaultTokenLabel + "-" + strconv.Itoa(i)
			}
		} else if taken(label) {
			return fmt.Errorf("token %q of user %q: %w", label, name, db.ErrExists)
		}
		return s.insertToken(ctx, name, label, dig)
	})
	if err != nil {
		return "", "", err
	}
	return token, label, nil
}

// RevokeToken deletes the token labeled label of the user name;
// db.ErrNotFound if the user or the token does not exist.
func (s *Store) RevokeToken(ctx context.Context, name, label string) error {
	return s.change(ctx, func() error {
		snap := s.snap.Load()
		if _, ok := snap.users[name]; !ok {
			return errNoUser(name)
		}
		for dig, t := range snap.tokens {
			if t.user == name && t.label == label {
				if err := s.tokens.DeleteWith(ctx, dig, nil); err != nil {
					return fmt.Errorf("revoking token %q of user %q: %w", label, name, err)
				}
				return nil
			}
		}
		return fmt.Errorf("token %q of user %q: %w", label, name, db.ErrNotFound)
	})
}

func (s *Store) insertToken(ctx context.Context, user, label, dig string) error {
	doc := &db.Document{ID: dig, Data: map[string]any{
		fieldUser:    user,
		fieldLabel:   label,
		fieldCreated: map[string]any{"$now": true},
	}}
	if _, err := s.tokens.Insert(ctx, doc); err != nil {
		return fmt.Errorf("creating token %q of user %q: %w", label, user, err)
	}
	return nil
}

// errUnchanged, returned by a changeUser callback, ends the change
// without writing.
var errUnchanged = errors.New("unchanged")

// changeUser applies fn to the stored user name and writes the result
// only if the stored document has not changed since it was read (a
// compare-and-set on its rev field); otherwise it reads it again and
// retries, so a concurrent change of the same user is never lost.
func (s *Store) changeUser(ctx context.Context, name string, fn func(*storedUser) error) error {
	return s.change(ctx, func() error { return s.changeUserLocked(ctx, name, fn) })
}

// changeUserLocked is changeUser for a caller that holds the change lock
// (inside change).
func (s *Store) changeUserLocked(ctx context.Context, name string, fn func(*storedUser) error) error {
	for range maxChangeAttempts {
		doc, err := s.users.Get(ctx, name)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return errNoUser(name)
			}
			return fmt.Errorf("reading user %q: %w", name, err)
		}
		u := decodeUser(doc)
		cond := db.Condition{{Field: fieldRev, Operator: "exists", Value: false}}
		if u.hasRev {
			cond = db.Condition{{Field: fieldRev, Operator: "eq", Value: u.rev}}
		}
		if err := fn(&u); err != nil {
			if errors.Is(err, errUnchanged) {
				return nil
			}
			return err
		}
		u.rev++
		_, err = s.users.Replace(ctx, name, u.document(), cond)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, db.ErrConditionFailed):
			continue // changed meanwhile (or removed): read again
		default:
			return fmt.Errorf("changing user %q: %w", name, err)
		}
	}
	return fmt.Errorf("changing user %q: it kept changing concurrently (%d attempts)", name, maxChangeAttempts)
}

// storedUser is a user document decoded for a change.
type storedUser struct {
	Principal
	rev       int64
	hasRev    bool
	created   any // stored value, written back unchanged
	createdAt time.Time
}

func (u *storedUser) document() *db.Document {
	grants := make(map[string]any, len(u.Grants))
	for database, l := range u.Grants {
		grants[database] = l.String()
	}
	data := map[string]any{
		fieldServerAdmin: u.ServerAdmin,
		fieldGrants:      grants,
		fieldRev:         u.rev,
	}
	if u.DefaultDatabase != "" {
		data[fieldDefaultDatabase] = u.DefaultDatabase
	}
	if u.created != nil {
		data[fieldCreated] = u.created
	}
	return &db.Document{ID: u.Name, Data: data}
}

// decodeUser decodes a user document. Unknown grant levels are dropped
// (they grant nothing).
func decodeUser(doc *db.Document) storedUser {
	u := storedUser{created: doc.Data[fieldCreated]}
	u.Name = doc.ID
	u.ServerAdmin, _ = doc.Data[fieldServerAdmin].(bool)
	u.DefaultDatabase, _ = doc.Data[fieldDefaultDatabase].(string)
	if g, ok := doc.Data[fieldGrants].(map[string]any); ok && len(g) > 0 {
		u.Grants = make(map[string]Level, len(g))
		for database, v := range g {
			if s, ok := v.(string); ok {
				if l, err := ParseLevel(s); err == nil {
					u.Grants[database] = l
				}
			}
		}
	}
	u.rev, u.hasRev = toInt64(doc.Data[fieldRev])
	u.createdAt = parseTime(u.created)
	return u
}

// toInt64 converts a stored JSON number.
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}

// parseTime parses a stored RFC 3339 time; zero if it is not one.
func parseTime(v any) time.Time {
	s, _ := v.(string)
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func errNoUser(name string) error { return fmt.Errorf("user %q: %w", name, db.ErrNotFound) }

func errLastServerAdmin(name, action string) error {
	return fmt.Errorf("%w: cannot %s user %q, the only server admin; make another user a server admin first", ErrLastServerAdmin, action, name)
}

// lastServerAdmin reports whether no user other than name is a server
// admin.
func (snap *snapshot) lastServerAdmin(name string) bool {
	for other, u := range snap.users {
		if other != name && u.principal.ServerAdmin {
			return false
		}
	}
	return true
}

func validGrantDatabase(database string) error {
	if database == AllDatabases {
		return nil
	}
	return db.ValidateName("database", database)
}

// newToken returns a new token, TokenPrefix followed by 32 bytes from
// crypto/rand in unpadded base64url, and its digest.
func newToken() (token, dig string, err error) {
	b := make([]byte, tokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("generating a token: %w", err)
	}
	token = TokenPrefix + base64.RawURLEncoding.EncodeToString(b)
	return token, digest(token), nil
}

// digest returns the hex SHA-256 digest of token, the only form stored.
func digest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
