package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/hop-/cachydb/internal/exportfmt"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import a JSON Lines export",
	Long: `Import a file written by "cachydb utils export" or "cachydb client
export". Missing databases and collections are created with their schema
and indexes; documents keep their _id.

--on-conflict decides what happens to a document whose _id exists:
  fail     stop there (the default) and report how many lines were applied
  skip     keep the existing document
  replace  replace it; a collection whose schema differs gets the file's
           schema (checked against its documents) and missing indexes
With fail or skip, an existing collection with a different schema or
index set is an error.

It opens the engine (taking the data-directory lock), so it fails while a
cachydb server is running on the same data directory; use
"cachydb client import" against the running server instead.`,
	Example: `  cachydb utils import -i backup.jsonl
  cachydb utils import -i backup.jsonl --on-conflict skip -R /new/data
  cat backup.jsonl | cachydb utils import -i -`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		mode, err := exportfmt.ParseOnConflict(importOnConflict)
		if err != nil {
			return err
		}
		if importInput == "" {
			return errors.New("-i/--input is required (- reads stdin)")
		}
		cmd.SilenceUsage = true
		return withInputFile(importInput, cmd.InOrStdin(), func(r io.Reader) error {
			return importOffline(cmd.Context(), r, cmd.ErrOrStderr(), engineOptions(), mode)
		})
	},
}

var (
	importInput      string
	importOnConflict string
)

func init() {
	importCmd.Flags().StringVarP(&importInput, "input", "i", "", "file to import (- for stdin)")
	importCmd.Flags().StringVar(&importOnConflict, "on-conflict", string(exportfmt.Fail), "existing _id: fail, skip, or replace")
	utilsCmd.AddCommand(importCmd)
}

// importOffline opens the engine with opts and imports r, reporting a
// summary on diag.
func importOffline(ctx context.Context, r io.Reader, diag io.Writer, opts db.Options, mode exportfmt.OnConflict) error {
	var st exportfmt.Stats
	err := withEngine(ctx, opts, func(e *db.Engine) error {
		var err error
		st, err = exportfmt.Import(ctx, r, exportfmt.Engine{E: e}, mode)
		return err
	})
	if err != nil {
		return fmt.Errorf("importing: %w", clientHint(err, "cachydb client import"))
	}
	printImportStats(diag, st)
	return nil
}

func printImportStats(w io.Writer, st exportfmt.Stats) {
	// A diagnostics line; a failed write to stderr has nowhere to go.
	_, _ = fmt.Fprintf(w, "Imported %d lines: %d databases, %d collections, %d documents inserted, %d replaced, %d skipped\n",
		st.Lines, st.Databases, st.Collections, st.Inserted, st.Replaced, st.Skipped)
}
