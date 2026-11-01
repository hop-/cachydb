package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/hop-/cachydb/internal/config"
	"github.com/spf13/cobra"
)

var appCmd = &cobra.Command{
	Use:   "app",
	Short: "Run the MCP server (same as running cachydb with no command)",
	Long: `Open the data directory and run the MCP server until the client closes
stdin (stdio), or SIGINT/SIGTERM arrives. On shutdown every change is
flushed to the data segments. See "cachydb docs mcp" for client setup.`,
	Example: `  cachydb app -R /path/to/data             # stdio
  cachydb app -t http -p 8080              # Streamable HTTP at http://localhost:8080/mcp
  TRANSPORT=http PORT=8080 cachydb app     # the same settings from the environment`,
	Args: cobra.NoArgs,
	RunE: runApp,
}

// autorun: This function is called automatically to initialize the command
func init() {
	// Flags for app command
	setAllFlagsToCmd(appCmd)

	rootCmd.AddCommand(appCmd)
}

// setAllFlagsToCmd adds the server flags to cmd. Their defaults come from
// the configuration (environment variable, then built-in default), so the
// precedence is flag > env > default.
func setAllFlagsToCmd(cmd *cobra.Command) {
	c := config.GetConfig()
	cmd.Flags().IntVarP(
		&generalServerPort,
		"port", "p",
		c.Port,
		"HTTP port; the server listens on all interfaces at /mcp (env PORT)",
	)
	cmd.Flags().StringVarP(
		&generalRootDir,
		"root", "R",
		c.RootDir,
		"data directory; one server per directory (env ROOT_DIR)",
	)
	cmd.Flags().StringVarP(
		&generalTransport,
		"transport", "t",
		c.Transport,
		"MCP transport: stdio or http (env TRANSPORT)",
	)
	cmd.Flags().StringVarP(
		&generalDBName,
		"db", "d",
		c.DBName,
		"default database, created if missing (env DB_NAME)",
	)
	setEngineFlags(cmd.Flags())
}

// validateTransport rejects a transport other than stdio or http, so a
// typo in -t or TRANSPORT does not silently fall back to stdio.
func validateTransport(t string) error {
	switch t {
	case "stdio", "http":
		return nil
	default:
		return fmt.Errorf("invalid transport %q: use stdio or http", t)
	}
}

// runApp opens the engine, runs the MCP transport until it returns (stdin
// closed, signal, or error), and then always stops the app, which closes
// the engine with a final flush.
func runApp(cmd *cobra.Command, _ []string) error {
	cmd.SilenceUsage = true
	if err := validateTransport(generalTransport); err != nil {
		return err
	}
	ctx := cmd.Context()

	application, err := newBuilder().
		WithDBName(dbName()).
		WithTransport(generalTransport).
		WithPort(generalServerPort).
		WithVersion(getVersion()).
		Build(ctx)
	if err != nil {
		return err
	}

	startErr := application.Start(ctx)
	// The signal context may already be cancelled; the shutdown still
	// needs its own (bounded) context to flush.
	stopErr := application.Stop(context.WithoutCancel(ctx))
	return errors.Join(startErr, stopErr)
}
