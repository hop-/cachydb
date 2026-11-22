package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Per-tool access control (approach-access-control §4). Every tool has an
// access level, given to newSpec; the wrapper newSpec puts around each
// handler resolves the principal and the target database and calls
// authorizeCall before the handler runs. That wrapper is the single place
// where tools are authorized: handlers only re-check where the level of
// their tool cannot say it all (use_database, list_databases).

// toolAccess is the access level a tool needs. The zero value means "not
// set": toolSpec.validate rejects it, so the server does not start, and
// authorizeCall denies every call of such a tool.
type toolAccess struct {
	level auth.Level
	set   bool
}

// The access levels of tools.
var (
	// accessNone: any principal. The tool takes no database argument and
	// checks any database it touches itself.
	accessNone = toolAccess{level: auth.LevelNone, set: true}
	// accessRead, accessWrite, accessAdmin: that level on the database the
	// "database" argument resolves to (see Server.databaseName).
	accessRead  = toolAccess{level: auth.LevelRead, set: true}
	accessWrite = toolAccess{level: auth.LevelWrite, set: true}
	accessAdmin = toolAccess{level: auth.LevelAdmin, set: true}
	// accessServer: a server admin.
	accessServer = toolAccess{level: auth.LevelServer, set: true}
)

// databaseArg is implemented (on the pointer) by the input of every tool
// that acts on a database chosen by its "database" argument. authorizeCall
// replaces an empty argument with the resolved database, so the handler
// acts on exactly the database that was authorized.
type databaseArg interface {
	databaseArg() *string
}

// extraAccess is implemented (on the pointer) by the input of a tool that
// touches a further database besides its "database" argument. authorizeCall
// calls it after resolving that argument and checks every returned
// requirement too.
type extraAccess interface {
	extraAccess() []accessCheck
}

// accessCheck is one requirement of a call: level on database.
type accessCheck struct {
	database string
	level    auth.Level
}

func (in *CreateCollectionInput) databaseArg() *string   { return &in.Database }
func (in *ListCollectionsInput) databaseArg() *string    { return &in.Database }
func (in *InsertDocumentInput) databaseArg() *string     { return &in.Database }
func (in *FindDocumentsInput) databaseArg() *string      { return &in.Database }
func (in *UpdateDocumentInput) databaseArg() *string     { return &in.Database }
func (in *ReplaceDocumentInput) databaseArg() *string    { return &in.Database }
func (in *DeleteDocumentInput) databaseArg() *string     { return &in.Database }
func (in *CountDocumentsInput) databaseArg() *string     { return &in.Database }
func (in *DistinctInput) databaseArg() *string           { return &in.Database }
func (in *DescribeCollectionInput) databaseArg() *string { return &in.Database }
func (in *UpdateSchemaInput) databaseArg() *string       { return &in.Database }
func (in *RenameCollectionInput) databaseArg() *string   { return &in.Database }
func (in *CopyCollectionInput) databaseArg() *string     { return &in.Database }
func (in *DeleteCollectionInput) databaseArg() *string   { return &in.Database }
func (in *DropIndexInput) databaseArg() *string          { return &in.Database }
func (in *CreateIndexInput) databaseArg() *string        { return &in.Database }
func (in *GetDocumentInput) databaseArg() *string        { return &in.Database }
func (in *InsertDocumentsInput) databaseArg() *string    { return &in.Database }
func (in *UpdateDocumentsInput) databaseArg() *string    { return &in.Database }
func (in *DeleteDocumentsInput) databaseArg() *string    { return &in.Database }
func (in *ServerStatsInput) databaseArg() *string        { return &in.Database }

// allDatabasesArg is implemented (on the pointer) by the input of a tool
// whose empty "database" argument means every database the principal can
// read, not the resolved default: authorizeCall then checks no database,
// and the handler filters by the principal itself (server_stats).
type allDatabasesArg interface {
	databaseArg
	allDatabases()
}

func (in *ServerStatsInput) allDatabases() {}

// extraAccess of copy_collection: the copy is created in the target
// database (the source database when target_database is empty), which
// needs admin; the source itself needs the tool's level, read.
func (in *CopyCollectionInput) extraAccess() []accessCheck {
	target := in.TargetDatabase
	if target == "" {
		target = in.Database
	}
	return []accessCheck{{database: target, level: auth.LevelAdmin}}
}

// validate reports a tool registered without an access level, or with a
// level that does not fit its input: a database level (read, write,
// admin) needs a "database" argument to check, and a tool with such an
// argument must have a database level, or the argument would go
// unchecked.
func (sp toolSpec) validate() error {
	name := sp.tool.Name
	dbLevel := sp.access.level >= auth.LevelRead && sp.access.level <= auth.LevelAdmin
	switch {
	case !sp.access.set:
		return fmt.Errorf("tool %s has no access level", name)
	case sp.access.level < auth.LevelNone || sp.access.level > auth.LevelServer:
		return fmt.Errorf("tool %s has an unknown access level %s", name, sp.access.level)
	case dbLevel && !sp.databaseArg:
		return fmt.Errorf("tool %s needs %s access to a database but its input has no database argument", name, sp.access.level)
	case !dbLevel && sp.databaseArg:
		return fmt.Errorf("tool %s has a database argument but access level %s, which does not check it", name, sp.access.level)
	}
	return nil
}

// principalKey is the context key of the principal authorizeCall resolved.
type principalKey struct{}

// principalFrom returns the principal of the tool call ctx belongs to; ok
// is false outside a call that went through authorizeCall.
func principalFrom(ctx context.Context) (p auth.Principal, ok bool) {
	p, ok = ctx.Value(principalKey{}).(auth.Principal)
	return p, ok
}

// errNoPrincipal reports a handler that ran without authorizeCall; it
// cannot happen for a registered tool.
var errNoPrincipal = errors.New("the call has no principal")

// principal returns the principal of req: the current state of the user
// its bearer token belongs to, or the implicit full-access principal when
// tokens are not required (stdio, loopback HTTP, AUTH=off). An HTTP call
// that should carry a token but carries none is refused.
func (s *Server) principal(req *mcp.CallToolRequest) (auth.Principal, error) {
	if req == nil {
		return s.principalOf(nil)
	}
	return s.principalOf(req.Extra)
}

// principalOf is principal for the extra data of any request.
func (s *Server) principalOf(extra *mcp.RequestExtra) (auth.Principal, error) {
	if extra != nil && extra.TokenInfo != nil && s.users != nil {
		// Re-read by name: a user removed after its token was checked
		// fails here (auth.ErrUnauthenticated).
		return s.users.Principal(extra.TokenInfo.UserID)
	}
	if s.tokensRequired && s.transport == "http" {
		return auth.Principal{}, fmt.Errorf("%w: the request has no bearer token", auth.ErrUnauthenticated)
	}
	return auth.FullAccess(), nil
}

// authorize returns nil if p may act at level on database: always for the
// implicit full-access principal, otherwise as auth.Store.Authorize
// decides from the user's current grants.
func (s *Server) authorize(p auth.Principal, database string, level auth.Level) error {
	if p.Implicit {
		return nil
	}
	if s.users == nil {
		// A stored user's principal comes only from s.users.
		return fmt.Errorf("%w: no auth store", auth.ErrUnauthenticated)
	}
	return s.users.Authorize(p, database, level)
}

// authorizeCall runs before every tool handler: it resolves the principal
// of req and, if in has a "database" argument, the database it names (see
// databaseName) and writes it back into in; then it checks every
// requirement of the call (the tool's level on that database, plus
// extraAccess). It returns ctx carrying the principal, or the tool error
// of a denied call.
func (s *Server) authorizeCall(ctx context.Context, req *mcp.CallToolRequest, name string, acc toolAccess, in any) (context.Context, error) {
	action := strings.ReplaceAll(name, "_", " ")
	p, err := s.principal(req)
	if err != nil {
		return ctx, toolError(action, err)
	}
	if !acc.set {
		return ctx, toolError(action, fmt.Errorf("%w: tool %s has no access level", auth.ErrPermissionDenied, name))
	}
	var ss *mcp.ServerSession
	if req != nil {
		ss = req.Session
	}
	checks := []accessCheck{{level: acc.level}}
	if a, ok := in.(databaseArg); ok {
		arg := a.databaseArg()
		if _, all := in.(allDatabasesArg); all && *arg == "" {
			checks = checks[:0] // the handler shows only readable databases
		} else {
			*arg = s.databaseName(ss, p, *arg)
			checks[0].database = *arg
		}
	}
	if e, ok := in.(extraAccess); ok {
		checks = append(checks, e.extraAccess()...)
	}
	for _, c := range checks {
		if err := s.authorize(p, c.database, c.level); err != nil {
			return ctx, toolError(action, err)
		}
	}
	return context.WithValue(ctx, principalKey{}, p), nil
}
