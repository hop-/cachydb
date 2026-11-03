package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cachydb",
		Short: "A lightweight document database with a built-in MCP server",
		Long: `CachyDB is a lightweight document database with a built-in MCP (Model
Context Protocol) server. Databases hold collections of JSON documents,
with optional schemas and hash indexes. Every acknowledged write is
fsynced to a write-ahead log first, so it survives a crash.

Running:
  With no command, cachydb runs the MCP server over stdio (an MCP client
  starts it as a child process). "-t http -p 7601" serves Streamable HTTP
  at http://localhost:7601/mcp instead.

Data:
  Data lives in -R/--root (default ~/.cachydb). Only one cachydb process
  may use a data directory at a time; utils commands fail while a server
  runs on it.

For AI agents:
  Run "cachydb docs" to list the built-in topics (overview, quickstart,
  mcp, tools, query, schema, config, errors), for example
  "cachydb docs quickstart" or "cachydb docs tools --format json". Over
  MCP, read the resources cachydb://docs/<topic>.

Settings: flag > environment variable > default; see "cachydb docs config".`,
		Example: `  cachydb                                  # MCP server over stdio, data in ~/.cachydb
  cachydb -R /srv/cachydb -d app           # custom data directory and default database
  cachydb -t http -p 7601                  # Streamable HTTP at http://localhost:7601/mcp
  cachydb docs                             # built-in documentation topics`,
		Args: cobra.NoArgs,
		RunE: runApp,
		// Execute prints the error once (cobra.CheckErr); commands silence
		// the usage text themselves once their arguments are valid.
		SilenceErrors: true,
	}
)

// autorun: This function is called automatically to initialize the root command
func init() {
	// Flags for root command
	setAllFlagsToCmd(rootCmd)
}

// Execute runs the CLI. SIGINT and SIGTERM cancel the command context, so
// a running server stops its transport and closes the engine. Errors are
// printed to stderr and exit with status 1.
func Execute() {
	if code := execute(context.Background(), os.Stderr); code != 0 {
		os.Exit(code)
	}
}

// execute runs the root command with the arguments set on it (os.Args by
// default) and returns the process exit status, printing an error to
// stderr instead of exiting, so tests can check it.
func execute(ctx context.Context, stderr io.Writer) int {
	if configErr != nil {
		fmt.Fprintln(stderr, "Error:", configErr)
		return 1
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	err := rootCmd.ExecuteContext(ctx)
	stop()

	if err != nil {
		fmt.Fprintln(stderr, "Error:", err)
		return 1
	}
	return 0
}
