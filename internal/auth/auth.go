// Package auth holds cachydb's users, tokens, and per-database access
// levels. Users and token digests live in the engine's reserved system
// database (db.Engine.SystemDatabase), which no tool, listing, or export
// reaches. Store is the only writer of that database and keeps an
// in-memory copy for Authenticate and Authorize.
//
// Levels are ordered read < write < admin per database; a grant on
// AllDatabases ("*") covers every database, including ones created later.
// A server admin may do everything, including creating and deleting
// databases and managing users (LevelServer).
package auth

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
)

var (
	// ErrPermissionDenied reports a principal without the access level a
	// call needs. The error text names the database and the level.
	ErrPermissionDenied = errors.New("permission denied")
	// ErrUnauthenticated reports an unknown or revoked token, or a
	// principal whose user no longer exists.
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrInvalidLevel reports an access level that is not read, write, or
	// admin where a grant level is required.
	ErrInvalidLevel = errors.New("invalid access level")
	// ErrLastServerAdmin reports a change that would remove or demote the
	// only server admin, which would leave nobody able to manage users.
	ErrLastServerAdmin = errors.New("last server admin")
)

// AllDatabases is the database name of a grant that covers every
// database, including future ones.
const AllDatabases = "*"

// Level is an access level. The zero value, LevelNone, needs no access:
// Authorize allows it for every principal.
type Level int

const (
	// LevelNone needs no access (any authenticated principal).
	LevelNone Level = iota
	// LevelRead allows reading a database: find, count, describe, list.
	LevelRead
	// LevelWrite is LevelRead plus document writes.
	LevelWrite
	// LevelAdmin is LevelWrite plus collection, schema, and index changes.
	LevelAdmin
	// LevelServer needs a server admin: creating and deleting databases
	// and managing users. It is never a grant.
	LevelServer
)

var levelNames = [...]string{
	LevelNone:   "none",
	LevelRead:   "read",
	LevelWrite:  "write",
	LevelAdmin:  "admin",
	LevelServer: "server",
}

// String returns the level's name: none, read, write, admin, or server.
func (l Level) String() string {
	if l >= 0 && int(l) < len(levelNames) {
		return levelNames[l]
	}
	return fmt.Sprintf("Level(%d)", int(l))
}

// grantable reports whether l can be granted on a database.
func (l Level) grantable() bool { return l >= LevelRead && l <= LevelAdmin }

// ParseLevel parses a grant level: "read", "write", or "admin". Anything
// else fails with ErrInvalidLevel.
func ParseLevel(s string) (Level, error) {
	for l := LevelRead; l <= LevelAdmin; l++ {
		if levelNames[l] == s {
			return l, nil
		}
	}
	return LevelNone, fmt.Errorf("%w: %q (want read, write, or admin)", ErrInvalidLevel, s)
}

// Principal is the user a request acts as. Grants maps a database name (or
// AllDatabases) to the level granted on it; it is the caller's copy.
type Principal struct {
	Name            string
	ServerAdmin     bool
	DefaultDatabase string
	Grants          map[string]Level
	// Implicit marks the full-access principal used when authentication is
	// off (FullAccess); it belongs to no stored user.
	Implicit bool
}

// FullAccess returns the implicit principal for a server running without
// authentication (stdio, loopback HTTP, or AUTH=off): a server admin that
// belongs to no stored user.
func FullAccess() Principal {
	return Principal{ServerAdmin: true, Implicit: true}
}

// Can reports whether p has at least need on database. LevelNone is always
// allowed and a server admin may do everything; LevelServer needs a server
// admin; otherwise the higher of the grant on database and the grant on
// AllDatabases decides.
func (p Principal) Can(database string, need Level) bool {
	switch {
	case need <= LevelNone, p.ServerAdmin:
		return true
	case need >= LevelServer:
		return false
	}
	return max(p.Grants[database], p.Grants[AllDatabases]) >= need
}

// clone returns a copy of p that shares no map with it.
func (p Principal) clone() Principal {
	p.Grants = maps.Clone(p.Grants)
	return p
}

// denied returns the ErrPermissionDenied error for p lacking need on
// database.
func denied(p Principal, database string, need Level) error {
	if need >= LevelServer {
		return fmt.Errorf("%w: user %q must be a server admin", ErrPermissionDenied, p.Name)
	}
	return fmt.Errorf("%w: user %q needs %s access to database %q", ErrPermissionDenied, p.Name, need, database)
}

// LogChange logs one user-management change at INFO: who made it (actor),
// what it was (action), and the user it changed (target), plus attrs
// (key-value pairs such as the database and level). Callers must never
// pass a token.
func LogChange(logger *slog.Logger, actor, action, target string, attrs ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("user management", append([]any{"actor", actor, "action", action, "target", target}, attrs...)...)
}
