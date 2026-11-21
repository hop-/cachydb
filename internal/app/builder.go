package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
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
	version   string
	logger    *slog.Logger

	walRetentionSet  bool
	walRetentionAge  time.Duration
	walRetentionSize int64
	cacheSizeSet     bool
	cacheSize        int64
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
// It fails before opening the engine for an invalid authentication mode
// or a TLS certificate without a key (or the reverse), and after opening
// it when HTTP clients need tokens (auth.Decide) but no user exists; see
// ErrNoUsers.
func (b *Builder) Build(ctx context.Context) (_ *App, err error) {
	mode, err := auth.ParseMode(b.authMode)
	if err != nil {
		return nil, fmt.Errorf("AUTH (--auth): %w", err)
	}
	if err := b.listen.Validate(); err != nil {
		return nil, err
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
	var users *auth.Store
	if decision.TokensRequired {
		if users, err = openUsers(ctx, engine, mode, host); err != nil {
			return nil, err
		}
	}

	mcpServer, err := mcpserver.New(ctx, engine, mcpserver.Config{
		DefaultDatabase: b.dbName,
		Transport:       b.transport,
		Listen:          b.listen,
		Version:         b.version,
		TokensRequired:  decision.TokensRequired,
		Users:           users,
		Logger:          opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MCP server: %w", err)
	}

	return &App{engine: engine, mcpServer: mcpServer}, nil
}

// ErrNoUsers reports a server that needs tokens but has no user to hold
// one; Build refuses to start it.
var ErrNoUsers = errors.New("no user exists")

// openUsers opens the auth store of a server whose HTTP clients need
// tokens and checks that a user exists to present one. Anything that
// creates users at start-up runs before the check.
func openUsers(ctx context.Context, engine *db.Engine, mode auth.Mode, host string) (*auth.Store, error) {
	users, err := auth.Open(ctx, engine)
	if err != nil {
		return nil, err
	}
	if !users.HasUsers() {
		return nil, noUsersError(mode, host)
	}
	return users, nil
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
