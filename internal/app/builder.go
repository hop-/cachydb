package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
)

type Builder struct {
	dbName    string
	rootDir   string
	transport string
	port      int
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

func (b *Builder) WithPort(port int) *Builder {
	b.port = port
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
func (b *Builder) Build(ctx context.Context) (_ *App, err error) {
	engine, err := db.Open(ctx, b.EngineOptions())
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

	httpAddr := fmt.Sprintf(":%d", b.port)
	mcpServer, err := mcpserver.NewServer(ctx, engine, b.dbName, b.transport, httpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create MCP server: %w", err)
	}

	return &App{engine: engine, mcpServer: mcpServer}, nil
}
