package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

var (
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show the application version",
		Long: `Print the cachydb version. With --json, also print the Go version, the
on-disk storage and WAL format versions, and the newest MCP protocol
version this build supports, so tools can check compatibility.`,
		Example: `  cachydb version
  cachydb version --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			if versionJSON {
				return showVersionJSON(cmd.OutOrStdout())
			}
			return showApplicationVersion(cmd.OutOrStdout())
		},
	}

	versionJSON bool
)

func init() {
	versionCmd.Flags().BoolVar(&versionJSON, "json", false, "print version details as JSON")
	rootCmd.AddCommand(versionCmd)
}

func showApplicationVersion(w io.Writer) error {
	_, err := fmt.Fprintf(w, "cachydb version %s\n", getVersion())
	return err
}

// versionInfo is the output of version --json.
type versionInfo struct {
	Version       string `json:"version"`
	Go            string `json:"go"`
	StorageFormat int    `json:"storageFormat"`
	WALFormat     int    `json:"walFormat"`
	MCPProtocol   string `json:"mcpProtocol"`
}

func currentVersionInfo() versionInfo {
	return versionInfo{
		Version:       getVersion(),
		Go:            runtime.Version(),
		StorageFormat: db.FormatVersion,
		WALFormat:     db.WALFormatVersion,
		// The newest protocol version the SDK supports; clients that ask
		// for it get it (over Streamable HTTP the SDK negotiates the newest
		// session-based version instead).
		MCPProtocol: mcp.SupportedProtocolVersions()[0],
	}
}

func showVersionJSON(w io.Writer) error {
	data, err := json.MarshalIndent(currentVersionInfo(), "", "  ")
	if err != nil {
		return fmt.Errorf("encoding version: %w", err)
	}
	_, err = fmt.Fprintf(w, "%s\n", data)
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
