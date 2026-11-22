package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/hop-/cachydb/internal/app"
	"github.com/hop-/cachydb/internal/config"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/spf13/cobra"
)

var appCmd = &cobra.Command{
	Use:   "app",
	Short: "Run the MCP server (same as running cachydb with no command)",
	Long: `Open the data directory and run the MCP server until the client closes
stdin (stdio), or SIGINT/SIGTERM arrives. On shutdown every change is
flushed to the data segments. See "cachydb docs mcp" for client setup.

HTTP listens on 127.0.0.1 (this machine only) unless --host says
otherwise. Clients need a token when --auth is required, or when it is
auto (the default) and the host is not loopback (127.0.0.1, ::1,
localhost); the server then refuses to start while no user exists. stdio
never needs a token. With --tls-cert and --tls-key it serves HTTPS.

Create users with "cachydb utils user add" while the server is stopped,
or set BOOTSTRAP_ADMIN_TOKEN (or BOOTSTRAP_ADMIN_TOKEN_FILE, a file
holding it): every start then makes sure the server admin "admin" exists
and holds that token. See "cachydb docs access".`,
	Example: `  cachydb app -R /path/to/data             # stdio
  cachydb app -t http -p 8080              # Streamable HTTP at http://localhost:8080/mcp
  TRANSPORT=http PORT=8080 cachydb app     # the same settings from the environment
  cachydb app -t http --host 0.0.0.0 --tls-cert cert.pem --tls-key key.pem   # HTTPS on every interface, tokens required`,
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
		"HTTP port, path /mcp (env PORT)",
	)
	cmd.Flags().StringVar(
		&generalHost,
		"host",
		c.Host,
		"HTTP listen host: 127.0.0.1 = this machine only, 0.0.0.0 = every interface (needs a user or --auth off) (env HOST)",
	)
	cmd.Flags().StringVar(
		&generalAuth,
		"auth",
		c.Auth,
		"auto: HTTP clients need a token unless the host is loopback; required: always for HTTP; off: never (env AUTH)",
	)
	cmd.Flags().StringVar(
		&generalTLSCert,
		"tls-cert",
		c.TLSCert,
		"TLS certificate file (PEM); with --tls-key, serve HTTPS (env TLS_CERT)",
	)
	cmd.Flags().StringVar(
		&generalTLSKey,
		"tls-key",
		c.TLSKey,
		"TLS private key file (PEM); with --tls-cert, serve HTTPS (env TLS_KEY)",
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
	cmd.Flags().IntVar(
		&generalDefaultLimit,
		"default-limit",
		c.DefaultLimit,
		"documents find_documents returns at most when a call gives no limit (env DEFAULT_LIMIT)",
	)
	generalMaxResultBytes = c.MaxResultBytes
	cmd.Flags().Var(
		&generalMaxResultBytes,
		"max-result-bytes",
		"cap on the JSON size of the documents one find_documents call returns, e.g. 1MiB; at least one document is returned (env MAX_RESULT_BYTES)",
	)
	setEngineFlags(cmd.Flags())
}

// validateResultLimits rejects a default limit or result byte cap that is
// not positive.
func validateResultLimits(limit int, maxBytes config.ByteSize) error {
	if limit <= 0 {
		return fmt.Errorf("invalid --default-limit %d: must be greater than 0", limit)
	}
	if maxBytes <= 0 {
		return fmt.Errorf("invalid --max-result-bytes %s: must be greater than 0", maxBytes)
	}
	return nil
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
	if err := validateResultLimits(generalDefaultLimit, generalMaxResultBytes); err != nil {
		return err
	}
	ctx := cmd.Context()
	c := config.GetConfig()
	bootstrap, err := app.BootstrapToken(c.BootstrapAdminToken, c.BootstrapAdminTokenFile)
	if err != nil {
		return err
	}

	application, err := newBuilder().
		WithDBName(dbName()).
		WithTransport(generalTransport).
		WithListen(mcpserver.ListenConfig{
			Host:    generalHost,
			Port:    generalServerPort,
			TLSCert: generalTLSCert,
			TLSKey:  generalTLSKey,
		}).
		WithAuth(generalAuth).
		WithBootstrapAdminToken(bootstrap).
		WithResultLimits(generalDefaultLimit, int64(generalMaxResultBytes)).
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
