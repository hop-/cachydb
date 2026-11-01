package cmd

import (
	"context"
	"errors"

	"github.com/hop-/cachydb/internal/config"
	"github.com/spf13/cobra"
)

var appCmd = &cobra.Command{
	Use:   "app",
	Short: "Run the application (same as default)",
	RunE:  runApp,
}

// autorun: This function is called automatically to initialize the command
func init() {
	// Flags for app command
	setAllFlagsToCmd(appCmd)

	rootCmd.AddCommand(appCmd)
}

func setAllFlagsToCmd(cmd *cobra.Command) {
	cmd.Flags().IntVarP(
		&generalServerPort,
		"port", "p",
		config.GetConfig().Port,
		"port on which connection listener will be started",
	)
	cmd.Flags().StringVarP(
		&generalRootDir,
		"root", "R",
		config.GetConfig().RootDir,
		"root directory for application data and configurations",
	)
	cmd.Flags().StringVarP(
		&generalTransport,
		"transport", "t",
		"",
		"transport type: stdio or http",
	)
	setEngineFlags(cmd.Flags())
}

// runApp opens the engine, runs the MCP transport until it returns (stdin
// closed, signal, or error), and then always stops the app, which closes
// the engine with a final flush.
func runApp(cmd *cobra.Command, _ []string) error {
	cmd.SilenceUsage = true
	ctx := cmd.Context()

	application, err := newBuilder().
		WithDBName(config.GetConfig().DBName).
		WithTransport(generalTransport).
		WithPort(generalServerPort).
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
