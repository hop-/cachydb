package cmd

import (
	"fmt"
	"io"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var (
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show the application version",
		Long:  `Display the current version of the application.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return showApplicationVersion(cmd.OutOrStdout())
		},
	}
)

func init() {
	// No flags for version command

	rootCmd.AddCommand(versionCmd)
}

func showApplicationVersion(w io.Writer) error {
	_, err := fmt.Fprintf(w, "cachydb version %s\n", getVersion())
	return err
}

func getVersion() string {
	if Version != "" {
		return Version
	}

	info, ok := debug.ReadBuildInfo()
	if ok && info.Main.Version != "(devel)" && info.Main.Version != "" {
		return info.Main.Version
	}

	return defaultVersion
}
