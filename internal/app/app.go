package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
)

// ShutdownTimeout bounds App.Stop: closing the MCP sessions and the final
// engine flush must finish within it.
const ShutdownTimeout = 10 * time.Second

// App owns the opened engine and the MCP server running on it.
type App struct {
	engine    *db.Engine
	mcpServer *mcpserver.Server
}

// Start runs the MCP transport until ctx is cancelled, the stdio client
// disconnects, or the transport fails. Call Stop after Start returns, on
// every path.
func (a *App) Start(ctx context.Context) error {
	return a.mcpServer.Start(ctx)
}

// Stop closes the MCP server and then the engine (final flush, WAL close,
// data-directory lock release), bounded by ShutdownTimeout. Pass a context
// that is not already cancelled (for example context.WithoutCancel of the
// signal context) so the final flush can run. Stop is idempotent.
func (a *App) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ShutdownTimeout)
	defer cancel()

	var errs []error
	if err := a.mcpServer.Close(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := a.engine.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("closing engine: %w", err))
	}
	return errors.Join(errs...)
}
