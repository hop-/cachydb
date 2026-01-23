package app

import (
	"context"
	"fmt"

	"github.com/hop-/cachydb/internal/config"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
)

type App struct {
	mcpServer *mcpserver.Server
}

func (a *App) Start(ctx context.Context) error {
	err := a.init()
	if err != nil {
		return err
	}

	return a.mcpServer.Start(ctx)
}

func (a *App) Stop() error {
	// TODO: implement graceful shutdown
	return nil
}

func (a *App) init() error {
	cfg := config.GetConfig()

	mcpServer, err := mcpserver.NewServer(cfg.DBName, cfg.RootDir)
	if err != nil {
		return fmt.Errorf("failed to create MCP server: %w", err)
	}

	a.mcpServer = mcpServer
	return nil
}
