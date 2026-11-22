package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

// statsCmd prints the server_stats figures of a stopped data directory.
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show document counts and sizes of databases and collections",
	Long: `Show, per database and collection, the document count, the data bytes
(records in the data segments), the index bytes (the index.db file), and
the index names, with per-database totals, followed by the server figures:
the version, the write-ahead log size, and the document cache. It is the
offline twin of the server_stats MCP tool; --format json prints the same
keys as that tool (without uptime, and the cache of a freshly opened
engine is empty).

It opens the engine (taking the data-directory lock), so it fails while a
cachydb server is running on the same data directory; then use
"cachydb client call server_stats".`,
	Example: `  cachydb utils stats
  cachydb utils stats -d kb
  cachydb utils stats --format json -R /path/to/data`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if statsFormat != "text" && statsFormat != "json" {
			return fmt.Errorf("invalid --format %q: use text or json", statsFormat)
		}
		cmd.SilenceUsage = true
		return printStats(cmd.Context(), cmd.OutOrStdout(), engineOptions(), statsDatabase, statsFormat)
	},
}

var (
	statsDatabase string
	statsFormat   string
)

func init() {
	statsCmd.Flags().StringVarP(&statsDatabase, "database", "d", "", "only this database")
	statsCmd.Flags().StringVarP(&statsFormat, "format", "f", "text", "output format: text or json")
	utilsCmd.AddCommand(statsCmd)
}

// statsReport is the JSON form of utils stats: server_stats's output
// without success, and the server part without uptime.
type statsReport struct {
	Databases []mcpserver.DatabaseStats `json:"databases"`
	Server    statsServer               `json:"server"`
}

type statsServer struct {
	Version  string               `json:"version"`
	WALBytes int64                `json:"wal_bytes"`
	Cache    mcpserver.CacheTotal `json:"cache"`
}

// printStats opens the engine with opts and prints the figures of the
// database named (every database when empty) as text or JSON.
func printStats(ctx context.Context, w io.Writer, opts db.Options, database, format string) error {
	var rep statsReport
	err := withEngine(ctx, opts, func(e *db.Engine) error {
		names := []string{database}
		if database == "" {
			names = e.ListDatabases()
		}
		rep.Databases = []mcpserver.DatabaseStats{}
		for _, name := range names {
			d, err := e.Database(name)
			if err != nil {
				return err
			}
			ds, err := d.Stats(ctx)
			if err != nil {
				return err
			}
			rep.Databases = append(rep.Databases, mcpserver.NewDatabaseStats(ds))
		}
		srv, err := e.ServerStats()
		if err != nil {
			return err
		}
		rep.Server = statsServer{Version: getVersion(), WALBytes: srv.WALBytes, Cache: mcpserver.NewCacheTotal(srv.Cache)}
		return nil
	})
	if err != nil {
		return fmt.Errorf("stats: %w", clientHint(err, "cachydb client call server_stats"))
	}
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	return writeStatsText(w, rep)
}

// writeStatsText prints one line per database and per collection, then
// the server line.
func writeStatsText(w io.Writer, rep statsReport) error {
	out := &output{w: w}
	if len(rep.Databases) == 0 {
		out.printf("No databases found\n")
	}
	for _, d := range rep.Databases {
		out.printf("%s: %d collection(s), %d documents, data %d bytes, index %d bytes\n",
			d.Name, len(d.Collections), d.Documents, d.DataBytes, d.IndexBytes)
		for _, c := range d.Collections {
			indexes := "none"
			if len(c.Indexes) > 0 {
				indexes = strings.Join(c.Indexes, ", ")
			}
			out.printf("  └─ %s: %d documents, data %d bytes, index %d bytes, indexes: %s\n",
				c.Name, c.Documents, c.DataBytes, c.IndexBytes, indexes)
		}
	}
	s := rep.Server
	out.printf("\nserver: version %s, WAL %d bytes, cache %d of %d bytes (%d documents), %d hits, %d misses\n",
		s.Version, s.WALBytes, s.Cache.Bytes, s.Cache.MaxBytes, s.Cache.Entries, s.Cache.Hits, s.Cache.Misses)
	return out.err
}
