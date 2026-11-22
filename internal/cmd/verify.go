package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Check a data directory for damage, offline; optionally rebuild index files",
	Long: `Check a data directory end to end without opening it for use: every
WAL segment (headers, checksums, offset continuity, document records), every
data segment record (checksum, decompression, the live documents), each
collection's index file index.db (bbolt's page check and its own invariants:
the key table, the document count, segment accounting, index entries), that
every key-table entry points at its document's record, and that index.db is
not older than the data segments. --deep also checks every index entry
against the documents.

It prints one line per problem and a one-line summary, and exits 1 when it
found a problem, 0 otherwise. Lines starting with "note:" are things the
next start repairs by itself (a torn WAL tail, a leftover segment of an
interrupted flush); they do not fail the check. --format json prints the
whole report as one JSON object.

--rebuild-index deletes the index files (every one, or with -d and -c one
collection's) and builds them again from the data segments, then verifies.
Use it when verify reports a damaged or stale index.db. Damaged WAL or data
segments cannot be repaired; restore the whole data directory from a backup
(see "cachydb docs errors").

Verify reads the files directly and takes the data-directory lock, so stop
a cachydb server running on the directory first; there is no online
equivalent. Without --rebuild-index it writes nothing.`,
	Example: `  cachydb utils verify -R /path/to/data
  cachydb utils verify --deep --format json
  cachydb utils verify --rebuild-index -d kb -c notes`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if verifyCollection != "" && verifyDatabase == "" {
			return errors.New("-c/--collection requires -d/--database")
		}
		if verifyFormat != "text" && verifyFormat != "json" {
			return fmt.Errorf("invalid --format %q: use text or json", verifyFormat)
		}
		cmd.SilenceUsage = true
		vo := db.VerifyOptions{
			Database:     verifyDatabase,
			Collection:   verifyCollection,
			Deep:         verifyDeep,
			RebuildIndex: verifyRebuild,
		}
		return verifyOffline(cmd.Context(), cmd.OutOrStdout(), engineOptions(), vo, verifyFormat)
	},
}

var (
	verifyDatabase   string
	verifyCollection string
	verifyDeep       bool
	verifyRebuild    bool
	verifyFormat     string
)

func init() {
	verifyCmd.Flags().StringVarP(&verifyDatabase, "database", "d", "", "check (or rebuild) only this database's collections")
	verifyCmd.Flags().StringVarP(&verifyCollection, "collection", "c", "", "check (or rebuild) only this collection (requires -d)")
	verifyCmd.Flags().BoolVar(&verifyDeep, "deep", false, "also check every index entry against the documents")
	verifyCmd.Flags().BoolVar(&verifyRebuild, "rebuild-index", false, "delete and rebuild index.db from the data segments, then verify")
	verifyCmd.Flags().StringVarP(&verifyFormat, "format", "f", "text", "output format: text or json")
	utilsCmd.AddCommand(verifyCmd)
}

// errVerifyProblems is returned when verify found problems, so the
// command exits with status 1 after printing them.
var errVerifyProblems = errors.New("verify found problems")

// verifyOffline runs db.Verify and prints the report as text or JSON.
func verifyOffline(ctx context.Context, w io.Writer, opts db.Options, vo db.VerifyOptions, format string) error {
	r, err := db.Verify(ctx, opts, vo)
	if err != nil {
		if errors.Is(err, db.ErrDataDirInUse) {
			err = fmt.Errorf("%w (hint: a cachydb server is running on this data directory; stop it first: verify reads the files directly and has no \"cachydb client\" equivalent)", err)
		}
		return err
	}
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			return err
		}
	} else if err := writeVerifyText(w, r); err != nil {
		return err
	}
	if !r.OK {
		return fmt.Errorf("%w: %d", errVerifyProblems, len(r.Problems)+r.Omitted)
	}
	return nil
}

// writeVerifyText prints the problems, then the notices, then a one-line
// summary.
func writeVerifyText(w io.Writer, r *db.VerifyReport) error {
	o := &output{w: w}
	for _, name := range r.Rebuilt {
		o.printf("rebuilt: %s/index.db\n", name)
	}
	for _, p := range r.Problems {
		o.printf("%s\n", p)
	}
	if r.Omitted > 0 {
		o.printf("... %d more problem(s) not shown\n", r.Omitted)
	}
	for _, n := range r.Notices {
		o.printf("note: %s\n", n)
	}
	status := "ok"
	if !r.OK {
		status = fmt.Sprintf("%d problem(s)", len(r.Problems)+r.Omitted)
	}
	deep := ""
	if r.Deep {
		deep = fmt.Sprintf(", %d index(es) checked entry by entry", r.Indexes)
	}
	o.printf("verify: %s: %d database(s), %d collection(s), %d document(s), %d data segment(s) with %d record(s), %d WAL segment(s) with %d record(s)%s\n",
		status, r.Databases, r.Collections, r.Documents, r.DataSegments, r.DataRecords, r.WALSegments, r.WALRecords, deep)
	return o.err
}
