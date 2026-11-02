package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hop-/cachydb/internal/exportfmt"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export databases, a database, or a collection as JSON Lines",
	Long: `Export data as JSON Lines (one JSON object per line): a header, then
each database, its collections (schema and indexes), and their documents
sorted by _id. "cachydb utils import" reads it back; see "cachydb docs
config" for the format.

It opens the engine (taking the data-directory lock), so it fails while a
cachydb server is running on the same data directory; use
"cachydb client export" against the running server instead.`,
	Example: `  cachydb utils export -o backup.jsonl          # everything
  cachydb utils export -d kb                    # one database, to stdout
  cachydb utils export -d kb -c notes -o notes.jsonl`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		scope := exportfmt.Scope{Database: exportDatabase, Collection: exportCollection}
		if err := scope.Validate(); err != nil {
			return err
		}
		cmd.SilenceUsage = true
		return withOutputFile(exportOutput, cmd.OutOrStdout(), func(w io.Writer) error {
			return exportOffline(cmd.Context(), w, engineOptions(), scope)
		})
	},
}

var (
	exportDatabase   string
	exportCollection string
	exportOutput     string
)

func init() {
	exportCmd.Flags().StringVarP(&exportDatabase, "database", "d", "", "export only this database")
	exportCmd.Flags().StringVarP(&exportCollection, "collection", "c", "", "export only this collection (requires -d)")
	exportCmd.Flags().StringVarP(&exportOutput, "output", "o", "", "write to this file instead of stdout")
	utilsCmd.AddCommand(exportCmd)
}

// exportOffline opens the engine with opts and exports scope to w.
func exportOffline(ctx context.Context, w io.Writer, opts db.Options, scope exportfmt.Scope) error {
	err := withEngine(ctx, opts, func(e *db.Engine) error {
		return exportfmt.Export(ctx, w, exportfmt.Engine{E: e}, scope, getVersion(), time.Now())
	})
	if err != nil {
		return fmt.Errorf("exporting: %w", clientHint(err, "cachydb client export"))
	}
	return nil
}

// withOutputFile calls fn with the file path (created or truncated), or
// with stdout when path is empty. A file is synced and closed afterwards.
func withOutputFile(path string, stdout io.Writer, fn func(io.Writer) error) (err error) {
	if path == "" {
		return fn(stdout)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if serr := f.Sync(); serr != nil && err == nil {
			err = serr
		}
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return fn(f)
}

// withInputFile calls fn with the file path opened, or with stdin when
// path is "-".
func withInputFile(path string, stdin io.Reader, fn func(io.Reader) error) error {
	if path == "-" {
		return fn(stdin)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() // read-only: nothing to lose on close
	return fn(f)
}

// clientHint adds, to a data-directory-in-use error, how to do the same
// through the running server.
func clientHint(err error, suggestion string) error {
	if errors.Is(err, db.ErrDataDirInUse) {
		return fmt.Errorf("%w (hint: a cachydb server is running on this data directory; run %s against it, see \"cachydb client --help\")", err, suggestion)
	}
	return err
}
