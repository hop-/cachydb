package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cachydb",
		Short: "A lightweight document-based database with MCP support",
		Long:  `CachyDB is a lightweight document-based database similar to MongoDB, with Model Context Protocol (MCP) support for AI integration.`,
		RunE:  runApp,
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
	if configErr != nil {
		fmt.Fprintln(os.Stderr, "Error:", configErr)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := rootCmd.ExecuteContext(ctx)
	stop()

	cobra.CheckErr(err)
}
