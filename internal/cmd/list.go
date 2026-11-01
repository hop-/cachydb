package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List databases and their schema versions",
	Long: `List all available databases in the data directory along with their schema versions.
This command helps you see what databases exist and their current state.

It opens the engine (taking the data-directory lock), so it fails while a
cachydb server is running on the same data directory.`,
	RunE: runList,
}

var (
	showCollections bool
)

func init() {
	utilsCmd.AddCommand(listCmd)

	listCmd.Flags().BoolVarP(&showCollections, "collections", "c", false, "Show collections for each database")
}

func runList(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	return listDatabases(cmd.Context(), cmd.OutOrStdout(), engineOptions(), showCollections)
}

// listDatabases opens the engine with opts and prints every database with
// its schema version and, if collections is set, its collections with
// their document counts.
func listDatabases(ctx context.Context, w io.Writer, opts db.Options, collections bool) error {
	err := withEngine(ctx, opts, func(e *db.Engine) error {
		out := &output{w: w}
		databases := e.ListDatabases()
		if len(databases) == 0 {
			out.printf("No databases found\n")
			return out.err
		}

		out.printf("Found %d database(s):\n\n", len(databases))
		for _, dbName := range databases {
			database, err := e.Database(dbName)
			if err != nil {
				if errors.Is(err, db.ErrNotFound) {
					continue
				}
				return err
			}
			out.printf("  %s (schema version: %d)\n", dbName, database.SchemaVersion())

			if !collections {
				continue
			}
			names := database.ListCollections()
			if len(names) == 0 {
				out.printf("    └─ (no collections)\n")
				continue
			}
			for _, collName := range names {
				coll, err := database.Collection(collName)
				if err != nil {
					if errors.Is(err, db.ErrNotFound) {
						continue
					}
					return err
				}
				out.printf("    └─ %s (%d documents)\n", collName, coll.Count())
			}
		}
		return out.err
	})
	if err != nil {
		return fmt.Errorf("listing databases: %w", err)
	}
	return nil
}
