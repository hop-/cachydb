package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// User tools (approach-access-control §5): whoami for every principal and
// six user-management tools for server admins. They are registered only
// when the server requires tokens (Config.TokensRequired), so a server
// with authentication off lists exactly the tools it always did; and
// filterToolsList hides the admin tools from tools/list for principals
// that are not server admins, so ordinary agents do not pay for their
// schemas. The access level in each spec (checked by authorizeCall) is
// the real guard.

// AccessLevel is a grant level argument of set_access; its schema is an
// enum of AccessLevels.
type AccessLevel string

// AccessLevels lists the levels set_access takes; none removes the grant.
var AccessLevels = []string{"none", "read", "write", "admin"}

// User tool inputs.

type WhoamiInput struct{}

type ListUsersInput struct{}

type CreateUserInput struct {
	Name            string `json:"name" jsonschema:"User name: letters, digits, _ and -, starting with a letter or digit, at most 64 characters"`
	ServerAdmin     bool   `json:"server_admin,omitempty" jsonschema:"Make the user a server admin: every database, creating and deleting databases, and managing users"`
	DefaultDatabase string `json:"default_database,omitempty" jsonschema:"Database the user's calls use when they name none and the session chose none"`
}

type DeleteUserInput struct {
	Name string `json:"name" jsonschema:"Name of the user to delete, with all its tokens; the only server admin cannot be deleted"`
}

type SetAccessInput struct {
	User            string      `json:"user" jsonschema:"Name of the user to change"`
	Database        string      `json:"database,omitempty" jsonschema:"Database whose access level to set, or * for every database including future ones; give it with level"`
	Level           AccessLevel `json:"level,omitempty" jsonschema:"Access level on database: read; write (read plus document writes); admin (write plus collections, schemas, indexes); none removes the grant"`
	ServerAdmin     *bool       `json:"server_admin,omitempty" jsonschema:"Set (true) or clear (false) the server-admin flag; the only server admin cannot be cleared"`
	DefaultDatabase *string     `json:"default_database,omitempty" jsonschema:"Set the user's default database; an empty string clears it"`
}

type CreateTokenInput struct {
	User  string `json:"user" jsonschema:"Name of the user the token belongs to"`
	Label string `json:"label,omitempty" jsonschema:"Label to tell the user's tokens apart (letters, digits, _ and -); default: default, default-2, ..."`
}

type RevokeTokenInput struct {
	User  string `json:"user" jsonschema:"Name of the user the token belongs to"`
	Label string `json:"label" jsonschema:"Label of the token to revoke (list_users shows them)"`
}

// User tool outputs.

// UserInfo describes a user; never a token or its digest.
type UserInfo struct {
	Name            string            `json:"name" jsonschema:"User name"`
	ServerAdmin     bool              `json:"server_admin" jsonschema:"The user is a server admin"`
	DefaultDatabase string            `json:"default_database,omitempty" jsonschema:"The user's default database, if set"`
	Grants          map[string]string `json:"grants" jsonschema:"Database (or * for every database) to access level: read, write, or admin"`
	Created         string            `json:"created,omitempty" jsonschema:"When the user was created (RFC 3339 UTC)"`
	Tokens          []TokenInfo       `json:"tokens" jsonschema:"The user's tokens by label, sorted; never the secret"`
}

// TokenInfo describes one token of a user.
type TokenInfo struct {
	Label   string `json:"label" jsonschema:"Token label"`
	Created string `json:"created,omitempty" jsonschema:"When the token was created (RFC 3339 UTC)"`
}

type WhoamiOutput struct {
	Success         bool              `json:"success" jsonschema:"Always true on success"`
	User            string            `json:"user" jsonschema:"The user this session acts as"`
	ServerAdmin     bool              `json:"server_admin" jsonschema:"The user is a server admin: every database, creating and deleting databases, and managing users"`
	DefaultDatabase string            `json:"default_database,omitempty" jsonschema:"The user's default database, if set"`
	Grants          map[string]string `json:"grants" jsonschema:"Database (or * for every database) to access level: read, write, or admin"`
}

type ListUsersOutput struct {
	Success bool       `json:"success" jsonschema:"Always true on success"`
	Users   []UserInfo `json:"users" jsonschema:"Every user, sorted by name"`
}

type CreateUserOutput struct {
	Success bool   `json:"success" jsonschema:"Always true on success"`
	Message string `json:"message" jsonschema:"Human-readable summary"`
	User    string `json:"user" jsonschema:"The new user's name"`
	Label   string `json:"label" jsonschema:"Label of the first token"`
	Token   string `json:"token" jsonschema:"The user's first token; shown only this once, so hand it over now"`
}

type SetAccessOutput struct {
	Success bool     `json:"success" jsonschema:"Always true on success"`
	Message string   `json:"message" jsonschema:"Human-readable summary"`
	User    UserInfo `json:"user" jsonschema:"The user after the change"`
}

type CreateTokenOutput struct {
	Success bool   `json:"success" jsonschema:"Always true on success"`
	Message string `json:"message" jsonschema:"Human-readable summary"`
	User    string `json:"user" jsonschema:"The user the token belongs to"`
	Label   string `json:"label" jsonschema:"The token's label"`
	Token   string `json:"token" jsonschema:"The new token; shown only this once, so hand it over now"`
}

// adminOnly marks sp as a user-management tool: tools/list shows it only
// to server admins (see filterToolsList).
func (sp toolSpec) adminOnly() toolSpec {
	sp.admin = true
	return sp
}

// accessToolSpecs returns the user tools, registered only when the server
// requires tokens, in the order they are listed in docs.
func accessToolSpecs() []toolSpec {
	const adminNote = " Server admins only."
	return []toolSpec{
		newSpec("whoami",
			"Show the user this session acts as: name, server-admin flag, default database, and access level per database.",
			readOnly("Who am I"), accessNone, map[string]any{},
			func(s *Server) mcp.ToolHandlerFor[WhoamiInput, WhoamiOutput] { return s.whoamiTool }),
		newSpec("list_users",
			"List every user with its server-admin flag, default database, grants, and token labels (never the tokens)."+adminNote,
			readOnly("List users"), accessServer, map[string]any{},
			func(s *Server) mcp.ToolHandlerFor[ListUsersInput, ListUsersOutput] { return s.listUsersTool }).adminOnly(),
		newSpec("create_user",
			"Create a user and return its first token, which is shown only this once. Give it access with set_access."+adminNote,
			writing("Create user", false, false), accessServer, map[string]any{"name": "alice", "default_database": "shop"},
			func(s *Server) mcp.ToolHandlerFor[CreateUserInput, CreateUserOutput] { return s.createUserTool }).adminOnly(),
		newSpec("set_access",
			"Change a user's access in one step: set its level on a database (database and level; none removes it), set or clear the server-admin flag, or set or clear the default database."+adminNote,
			writing("Set access", true, true), accessServer, map[string]any{"user": "alice", "database": "shop", "level": "write"},
			func(s *Server) mcp.ToolHandlerFor[SetAccessInput, SetAccessOutput] { return s.setAccessTool }).adminOnly(),
		newSpec("create_token",
			"Create another token for a user and return it; it is shown only this once."+adminNote,
			writing("Create token", false, false), accessServer, map[string]any{"user": "alice", "label": "laptop"},
			func(s *Server) mcp.ToolHandlerFor[CreateTokenInput, CreateTokenOutput] { return s.createTokenTool }).adminOnly(),
		newSpec("revoke_token",
			"Revoke a user's token by label; it stops working at once."+adminNote,
			writing("Revoke token", true, true), accessServer, map[string]any{"user": "alice", "label": "laptop"},
			func(s *Server) mcp.ToolHandlerFor[RevokeTokenInput, MessageOutput] { return s.revokeTokenTool }).adminOnly(),
		newSpec("delete_user",
			"Delete a user and all its tokens. The only server admin cannot be deleted."+adminNote,
			writing("Delete user", true, true), accessServer, map[string]any{"name": "alice"},
			func(s *Server) mcp.ToolHandlerFor[DeleteUserInput, MessageOutput] { return s.deleteUserTool }).adminOnly(),
	}
}

// AccessToolDocs returns the documentation of the user tools (whoami and
// the server-admin tools), which the server registers only when it
// requires tokens; the access docs topic renders it.
func AccessToolDocs() []ToolDoc { return toolDocs(accessToolSpecs()) }

// filterToolsList is a receiving middleware of a server that requires
// tokens: it marks every tools/list result private to the requesting user
// (the list differs per user) and removes the admin tools from it unless
// the principal is a server admin. Hiding a tool only saves context; the
// tool's access level still guards every call.
func (s *Server) filterToolsList(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		res, err := next(ctx, method, req)
		if err != nil || method != "tools/list" {
			return res, err
		}
		list, ok := res.(*mcp.ListToolsResult)
		if !ok || list == nil {
			return res, err
		}
		list.CacheScope = "private"
		if p, perr := s.principalOf(req.GetExtra()); perr == nil && p.ServerAdmin {
			return list, nil
		}
		// The SDK builds a fresh slice per call, so filtering it in place
		// changes no shared state.
		list.Tools = slices.DeleteFunc(list.Tools, func(t *mcp.Tool) bool { return s.adminTools[t.Name] })
		return list, nil
	}
}

// actor returns the name the logs give the principal of ctx.
func actor(ctx context.Context) string {
	p, ok := principalFrom(ctx)
	switch {
	case !ok:
		return "unknown"
	case p.Implicit:
		return "local (no token)"
	default:
		return p.Name
	}
}

// userToolError is toolError with hints for the user tools.
func userToolError(action string, err error) error {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return fmt.Errorf("%s: %w (hint: list_users shows the users and their token labels)", action, err)
	case errors.Is(err, auth.ErrLastServerAdmin), errors.Is(err, auth.ErrInvalidLevel):
		// The auth message says what to do.
		return fmt.Errorf("%s: %w", action, err)
	default:
		return toolError(action, err)
	}
}

// levelNames converts grants to their level names.
func levelNames(grants map[string]auth.Level) map[string]string {
	out := make(map[string]string, len(grants))
	for database, l := range grants {
		out[database] = l.String()
	}
	return out
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// NewUserInfo describes u as list_users does: never a token or its digest.
func NewUserInfo(u auth.User) UserInfo {
	tokens := make([]TokenInfo, len(u.Tokens))
	for i, t := range u.Tokens {
		tokens[i] = TokenInfo{Label: t.Label, Created: formatTime(t.Created)}
	}
	return UserInfo{
		Name:            u.Name,
		ServerAdmin:     u.ServerAdmin,
		DefaultDatabase: u.DefaultDatabase,
		Grants:          levelNames(u.Grants),
		Created:         formatTime(u.Created),
		Tokens:          tokens,
	}
}

func (s *Server) whoamiTool(ctx context.Context, _ *mcp.CallToolRequest, _ WhoamiInput) (*mcp.CallToolResult, WhoamiOutput, error) {
	p, ok := principalFrom(ctx)
	if !ok {
		return nil, WhoamiOutput{}, toolError("whoami", errNoPrincipal)
	}
	name := p.Name
	if p.Implicit {
		name = "(none: this connection needs no token and has full access)"
	}
	return nil, WhoamiOutput{
		Success:         true,
		User:            name,
		ServerAdmin:     p.ServerAdmin,
		DefaultDatabase: p.DefaultDatabase,
		Grants:          levelNames(p.Grants),
	}, nil
}

func (s *Server) listUsersTool(_ context.Context, _ *mcp.CallToolRequest, _ ListUsersInput) (*mcp.CallToolResult, ListUsersOutput, error) {
	users := s.users.Users()
	out := make([]UserInfo, len(users))
	for i, u := range users {
		out[i] = NewUserInfo(u)
	}
	return nil, ListUsersOutput{Success: true, Users: out}, nil
}

func (s *Server) createUserTool(ctx context.Context, _ *mcp.CallToolRequest, in CreateUserInput) (*mcp.CallToolResult, CreateUserOutput, error) {
	token, err := s.users.CreateUser(ctx, in.Name, auth.UserOptions{ServerAdmin: in.ServerAdmin, DefaultDatabase: in.DefaultDatabase})
	if err != nil {
		return nil, CreateUserOutput{}, userToolError("create user", err)
	}
	auth.LogChange(s.logger, actor(ctx), "create_user", in.Name, "server_admin", in.ServerAdmin, "default_database", in.DefaultDatabase)
	return nil, CreateUserOutput{
		Success: true,
		Message: fmt.Sprintf("User '%s' created. Its token is shown only this once: hand it over now. Give it access with set_access.", in.Name),
		User:    in.Name,
		Label:   auth.DefaultTokenLabel,
		Token:   token,
	}, nil
}

func (s *Server) deleteUserTool(ctx context.Context, _ *mcp.CallToolRequest, in DeleteUserInput) (*mcp.CallToolResult, MessageOutput, error) {
	if err := s.users.DeleteUser(ctx, in.Name); err != nil {
		return nil, MessageOutput{}, userToolError("delete user", err)
	}
	auth.LogChange(s.logger, actor(ctx), "delete_user", in.Name)
	return nil, MessageOutput{Success: true, Message: fmt.Sprintf("User '%s' and its tokens deleted", in.Name)}, nil
}

// accessChange converts set_access's arguments.
func (in SetAccessInput) accessChange() (auth.AccessChange, error) {
	c := auth.AccessChange{Database: in.Database, ServerAdmin: in.ServerAdmin, DefaultDatabase: in.DefaultDatabase}
	switch {
	case in.Database != "" && in.Level == "":
		return c, errors.New("give level with database (none removes the grant)")
	case in.Database == "" && in.Level != "":
		return c, errors.New("give database with level")
	case in.Level == "none":
		c.Level = auth.LevelNone
	case in.Level != "":
		l, err := auth.ParseLevel(string(in.Level))
		if err != nil {
			return c, err
		}
		c.Level = l
	}
	if c.IsZero() {
		return c, errors.New("nothing to change: give database and level, server_admin, or default_database")
	}
	return c, nil
}

func (s *Server) setAccessTool(ctx context.Context, _ *mcp.CallToolRequest, in SetAccessInput) (*mcp.CallToolResult, SetAccessOutput, error) {
	c, err := in.accessChange()
	if err != nil {
		return nil, SetAccessOutput{}, userToolError("set access", err)
	}
	if err := s.users.SetAccess(ctx, in.User, c); err != nil {
		return nil, SetAccessOutput{}, userToolError("set access", err)
	}
	var attrs []any
	if c.Database != "" {
		attrs = append(attrs, "database", c.Database, "level", c.Level.String())
	}
	if c.ServerAdmin != nil {
		attrs = append(attrs, "server_admin", *c.ServerAdmin)
	}
	if c.DefaultDatabase != nil {
		attrs = append(attrs, "default_database", *c.DefaultDatabase)
	}
	auth.LogChange(s.logger, actor(ctx), "set_access", in.User, attrs...)
	u, err := s.users.User(in.User)
	if err != nil {
		// Removed right after the change by another call.
		return nil, SetAccessOutput{}, userToolError("set access", err)
	}
	return nil, SetAccessOutput{
		Success: true,
		Message: fmt.Sprintf("Access of user '%s' updated", in.User),
		User:    NewUserInfo(u),
	}, nil
}

func (s *Server) createTokenTool(ctx context.Context, _ *mcp.CallToolRequest, in CreateTokenInput) (*mcp.CallToolResult, CreateTokenOutput, error) {
	token, label, err := s.users.CreateToken(ctx, in.User, in.Label)
	if err != nil {
		return nil, CreateTokenOutput{}, userToolError("create token", err)
	}
	auth.LogChange(s.logger, actor(ctx), "create_token", in.User, "label", label)
	return nil, CreateTokenOutput{
		Success: true,
		Message: fmt.Sprintf("Token '%s' of user '%s' created. It is shown only this once: hand it over now.", label, in.User),
		User:    in.User,
		Label:   label,
		Token:   token,
	}, nil
}

func (s *Server) revokeTokenTool(ctx context.Context, _ *mcp.CallToolRequest, in RevokeTokenInput) (*mcp.CallToolResult, MessageOutput, error) {
	if err := s.users.RevokeToken(ctx, in.User, in.Label); err != nil {
		return nil, MessageOutput{}, userToolError("revoke token", err)
	}
	auth.LogChange(s.logger, actor(ctx), "revoke_token", in.User, "label", in.Label)
	return nil, MessageOutput{Success: true, Message: fmt.Sprintf("Token '%s' of user '%s' revoked", in.Label, in.User)}, nil
}
