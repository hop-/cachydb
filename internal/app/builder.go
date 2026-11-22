package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/hop-/cachydb/internal/auth"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
)

type Builder struct {
	dbName    string
	rootDir   string
	transport string
	listen    mcpserver.ListenConfig
	authMode  string
	// bootstrapToken is the BOOTSTRAP_ADMIN_TOKEN value; a secret, never
	// logged.
	bootstrapToken string
	version        string
	logger         *slog.Logger

	walRetentionSet  bool
	walRetentionAge  time.Duration
	walRetentionSize int64
	cacheSizeSet     bool
	cacheSize        int64

	defaultLimit   int   // find_documents default limit; 0 = the server default
	maxResultBytes int64 // find_documents result byte cap; 0 = the server default
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) WithDBName(name string) *Builder {
	b.dbName = name
	return b
}

func (b *Builder) WithRootDir(dir string) *Builder {
	b.rootDir = dir
	return b
}

func (b *Builder) WithTransport(transport string) *Builder {
	b.transport = transport
	return b
}

// WithPort sets the HTTP port, keeping the rest of the listener settings.
func (b *Builder) WithPort(port int) *Builder {
	b.listen.Port = port
	return b
}

// WithListen sets the HTTP listener: host (empty means 127.0.0.1), port,
// and TLS files.
func (b *Builder) WithListen(l mcpserver.ListenConfig) *Builder {
	b.listen = l
	return b
}

// WithAuth sets the authentication mode: "auto" (the default, also for
// ""), "required", or "off"; see auth.Decide. Build rejects any other
// value.
func (b *Builder) WithAuth(mode string) *Builder {
	b.authMode = mode
	return b
}

// WithBootstrapAdminToken sets the bootstrap token: at start-up Build
// makes sure the server admin auth.BootstrapAdmin exists and holds it
// (auth.Store.EnsureAdmin). Empty means none. Build rejects a token that
// fails auth.ValidateBootstrapToken. See BootstrapToken for reading it
// from the environment.
func (b *Builder) WithBootstrapAdminToken(token string) *Builder {
	b.bootstrapToken = token
	return b
}

// BootstrapToken returns the bootstrap token given by value
// (BOOTSTRAP_ADMIN_TOKEN) or by the contents of the file named by file
// (BOOTSTRAP_ADMIN_TOKEN_FILE), with surrounding white space removed;
// "" if neither is set. Setting both is an error. No error contains the
// token.
func BootstrapToken(value, file string) (string, error) {
	switch {
	case value != "" && file != "":
		return "", errors.New("BOOTSTRAP_ADMIN_TOKEN and BOOTSTRAP_ADMIN_TOKEN_FILE are both set: use one")
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("BOOTSTRAP_ADMIN_TOKEN_FILE: %w", err)
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return "", fmt.Errorf("BOOTSTRAP_ADMIN_TOKEN_FILE: %s is empty", file)
		}
		return token, nil
	}
	return strings.TrimSpace(value), nil
}

// WithVersion sets the build version the MCP server reports.
func (b *Builder) WithVersion(v string) *Builder {
	b.version = v
	return b
}

// WithLogger sets the engine logger. The default writes text to stderr;
// stdout is reserved for the MCP stdio transport.
func (b *Builder) WithLogger(logger *slog.Logger) *Builder {
	b.logger = logger
	return b
}

// WithWALRetention sets how long (age) and how much (size, in bytes)
// processed WAL is kept. A zero age or size means no limit for that
// dimension. Without this call the engine defaults (24h, 1GiB) apply.
func (b *Builder) WithWALRetention(age time.Duration, size int64) *Builder {
	b.walRetentionSet = true
	b.walRetentionAge = age
	b.walRetentionSize = size
	return b
}

// WithCacheSize sets the document cache size in bytes; 0 disables the
// cache. Without this call the engine default (64MiB) applies.
func (b *Builder) WithCacheSize(n int64) *Builder {
	b.cacheSizeSet = true
	b.cacheSize = n
	return b
}

// WithResultLimits sets the find_documents bounds: the number of
// documents returned when a call gives no limit, and the cap on the JSON
// size of the documents returned. Zero keeps the server default
// (mcpserver.DefaultFindLimit, mcpserver.DefaultMaxResultBytes); Build
// rejects a negative value.
func (b *Builder) WithResultLimits(defaultLimit int, maxResultBytes int64) *Builder {
	b.defaultLimit = defaultLimit
	b.maxResultBytes = maxResultBytes
	return b
}

// EngineOptions returns the db.Options the builder opens the engine with.
func (b *Builder) EngineOptions() db.Options {
	logger := b.logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	opts := db.Options{
		Dir:    b.rootDir,
		Logger: logger,
	}
	// db.Options treats zero as "use the default" and a negative value as
	// "disabled"; the builder's zero means disabled.
	if b.walRetentionSet {
		opts.WALRetentionAge = disabledIfZero(b.walRetentionAge)
		opts.WALRetentionSize = disabledIfZero(b.walRetentionSize)
	}
	if b.cacheSizeSet {
		opts.CacheSize = disabledIfZero(b.cacheSize)
	}
	return opts
}

func disabledIfZero[T int64 | time.Duration](v T) T {
	if v == 0 {
		return -1
	}
	return v
}

// Build opens the engine (taking the data-directory lock and running
// recovery) and creates the MCP server on it. The returned App owns the
// engine; call Stop to close it.
//
// It fails before opening the engine for an invalid authentication mode,
// a TLS certificate without a key (or the reverse), or an invalid
// bootstrap token, and after opening it when HTTP clients need tokens
// (auth.Decide) but no user exists; see ErrNoUsers.
func (b *Builder) Build(ctx context.Context) (_ *App, err error) {
	mode, err := auth.ParseMode(b.authMode)
	if err != nil {
		return nil, fmt.Errorf("AUTH (--auth): %w", err)
	}
	if err := b.listen.Validate(); err != nil {
		return nil, err
	}
	if b.defaultLimit < 0 {
		return nil, fmt.Errorf("DEFAULT_LIMIT (--default-limit): %d must be greater than 0", b.defaultLimit)
	}
	if b.maxResultBytes < 0 {
		return nil, fmt.Errorf("MAX_RESULT_BYTES (--max-result-bytes): %d must be greater than 0", b.maxResultBytes)
	}
	if b.bootstrapToken != "" {
		if err := auth.ValidateBootstrapToken(b.bootstrapToken); err != nil {
			return nil, fmt.Errorf("BOOTSTRAP_ADMIN_TOKEN: %w", err)
		}
	}
	host := b.listen.HostOrDefault()
	decision := auth.Decide(mode, b.transport == "http", host)

	opts := b.EngineOptions()
	engine, err := db.Open(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("opening database engine: %w", err)
	}
	defer func() {
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ShutdownTimeout)
			defer cancel()
			if cerr := engine.Close(closeCtx); cerr != nil {
				err = errors.Join(err, fmt.Errorf("closing engine: %w", cerr))
			}
		}
	}()

	if decision.OpenNetwork {
		opts.Logger.Warn("authentication is off on a listener other machines can reach: every client that reaches it has full access to every database",
			"host", host, "auth", string(mode))
	}
	users, err := b.openUsers(ctx, engine, decision, mode, host, opts.Logger)
	if err != nil {
		return nil, err
	}

	mcpServer, err := mcpserver.New(ctx, engine, mcpserver.Config{
		DefaultDatabase: b.dbName,
		Transport:       b.transport,
		Listen:          b.listen,
		Version:         b.version,
		TokensRequired:  decision.TokensRequired,
		Users:           users,
		Logger:          opts.Logger,
		DefaultLimit:    b.defaultLimit,
		MaxResultBytes:  b.maxResultBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MCP server: %w", err)
	}

	return &App{engine: engine, mcpServer: mcpServer}, nil
}

// ErrNoUsers reports a server that needs tokens but has no user to hold
// one; Build refuses to start it.
var ErrNoUsers = errors.New("no user exists")

// openUsers opens the auth store when the server needs it, applies the
// bootstrap token, and, when HTTP clients need tokens, checks that a user
// exists to present one. It returns nil (no store) when none is needed.
//
// The store is opened when tokens are required, when a bootstrap token is
// set, and whenever the system database already exists, also with
// authentication off: delete_database must then remove the deleted
// database's grants, or a database recreated under that name would
// inherit them once authentication is on. Without any of these, nothing
// creates the system database. Failing to open an existing one fails the
// start-up, with authentication off too, rather than leave stale grants
// behind.
func (b *Builder) openUsers(ctx context.Context, engine *db.Engine, decision auth.Decision, mode auth.Mode, host string, logger *slog.Logger) (*auth.Store, error) {
	if !decision.TokensRequired && b.bootstrapToken == "" && !engine.HasSystemDatabase() {
		return nil, nil
	}
	users, err := auth.Open(ctx, engine)
	if err != nil {
		return nil, err
	}
	if b.bootstrapToken != "" {
		if err := ensureBootstrapAdmin(ctx, users, b.bootstrapToken, logger); err != nil {
			return nil, err
		}
	}
	if decision.TokensRequired && !users.HasUsers() {
		return nil, noUsersError(mode, host)
	}
	return users, nil
}

// ensureBootstrapAdmin applies the bootstrap token and logs what changed,
// never the token.
func ensureBootstrapAdmin(ctx context.Context, users *auth.Store, token string, logger *slog.Logger) error {
	res, err := users.EnsureAdmin(ctx, auth.BootstrapAdmin, token)
	if err != nil {
		return fmt.Errorf("BOOTSTRAP_ADMIN_TOKEN: %w", err)
	}
	const actor = "BOOTSTRAP_ADMIN_TOKEN"
	if res.CreatedUser {
		auth.LogChange(logger, actor, "create_user", auth.BootstrapAdmin, "server_admin", true)
	}
	if res.MadeServerAdmin {
		auth.LogChange(logger, actor, "set_access", auth.BootstrapAdmin, "server_admin", true)
	}
	if res.ReplacedToken {
		auth.LogChange(logger, actor, "revoke_token", auth.BootstrapAdmin, "label", auth.BootstrapLabel, "reason", "the bootstrap token changed")
	}
	if res.AddedToken {
		auth.LogChange(logger, actor, "create_token", auth.BootstrapAdmin, "label", auth.BootstrapLabel)
	}
	return nil
}

// noUsersError explains why the server needs tokens and the ways out.
func noUsersError(mode auth.Mode, host string) error {
	why := fmt.Sprintf("HTTP on host %s is reachable from other machines (AUTH=%s)", host, mode)
	if mode == auth.ModeRequired {
		why = "AUTH=required"
	}
	return fmt.Errorf("refusing to start: %s, so clients need a token, but %w. "+
		"Create the first admin with \"cachydb utils user add NAME --admin\" while the server is stopped, "+
		"or set BOOTSTRAP_ADMIN_TOKEN (for containers); "+
		"or, on a trusted network only, turn authentication off with AUTH=off (--auth off)", why, ErrNoUsers)
}
