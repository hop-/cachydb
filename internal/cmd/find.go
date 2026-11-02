package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/hop-/cachydb/internal/exportfmt"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

var findCmd = &cobra.Command{
	Use:   "find",
	Short: "Print the documents of a collection that match a query",
	Long: `Print the documents of a collection matching --query, a JSON object of
the same shape as the find_documents "query" argument: filters, any,
fields, sort, limit, skip (see "cachydb docs query"). The default output
is JSON Lines, one document per line; --format json prints one array.

It opens the engine (taking the data-directory lock), so it fails while a
cachydb server is running on the same data directory; use
"cachydb client call find_documents ..." against the running server
instead.`,
	Example: `  cachydb utils find -d kb -c notes
  cachydb utils find -d kb -c notes --query '{"filters":[{"field":"tags","operator":"has","value":"wal"}],"fields":["title"]}'
  cachydb utils find -d kb -c notes --query '{"sort":[{"field":"updated","order":"desc"}],"limit":5}' --format json`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if findDatabase == "" || findCollection == "" {
			return errors.New("-d/--database and -c/--collection are required")
		}
		if findFormat != "json" && findFormat != "jsonl" {
			return fmt.Errorf("invalid --format %q: use json or jsonl", findFormat)
		}
		q := &db.Query{}
		if findQuery != "" {
			dec := json.NewDecoder(strings.NewReader(findQuery))
			dec.DisallowUnknownFields()
			if err := dec.Decode(q); err != nil {
				return fmt.Errorf("invalid --query: %w", err)
			}
		}
		cmd.SilenceUsage = true
		return findOffline(cmd.Context(), cmd.OutOrStdout(), engineOptions(), findDatabase, findCollection, q, findFormat)
	},
}

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Print one document by _id",
	Long: `Print the document with the given _id as JSON; exit status 1 if it does
not exist.

It opens the engine (taking the data-directory lock), so it fails while a
cachydb server is running on the same data directory; use
"cachydb client call find_documents ..." against the running server
instead.`,
	Example: `  cachydb utils get -d kb -c notes --id wal-design
  cachydb utils get -d kb -c notes --id wal-design -R /path/to/data`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if findDatabase == "" || findCollection == "" || getID == "" {
			return errors.New("-d/--database, -c/--collection, and --id are required")
		}
		cmd.SilenceUsage = true
		return getOffline(cmd.Context(), cmd.OutOrStdout(), engineOptions(), findDatabase, findCollection, getID)
	},
}

var (
	findDatabase   string
	findCollection string
	findQuery      string
	findFormat     string
	getID          string
)

func init() {
	for _, c := range []*cobra.Command{findCmd, getCmd} {
		c.Flags().StringVarP(&findDatabase, "database", "d", "", "database name")
		c.Flags().StringVarP(&findCollection, "collection", "c", "", "collection name")
		utilsCmd.AddCommand(c)
	}
	findCmd.Flags().StringVarP(&findQuery, "query", "q", "", "query as JSON: filters, any, fields, sort, limit, skip")
	findCmd.Flags().StringVarP(&findFormat, "format", "f", "jsonl", "output format: jsonl or json")
	getCmd.Flags().StringVar(&getID, "id", "", "document _id")
}

// findHint is the client command suggested when the data directory is in
// use.
func findHint(database, collection string) string {
	return fmt.Sprintf(`cachydb client call find_documents '{"database":%q,"collection":%q}'`, database, collection)
}

// findOffline prints the documents matching q.
func findOffline(ctx context.Context, w io.Writer, opts db.Options, database, collection string, q *db.Query, format string) error {
	err := withEngine(ctx, opts, func(e *db.Engine) error {
		c, err := openCollection(e, database, collection)
		if err != nil {
			return err
		}
		docs, err := c.Find(ctx, q)
		if err != nil {
			return err
		}
		out := make([]map[string]any, len(docs))
		for i, d := range docs {
			out[i] = exportfmt.DocMap(d)
		}
		return writeDocs(w, out, format)
	})
	if err != nil {
		return fmt.Errorf("finding documents: %w", clientHint(err, findHint(database, collection)))
	}
	return nil
}

// getOffline prints one document.
func getOffline(ctx context.Context, w io.Writer, opts db.Options, database, collection, id string) error {
	err := withEngine(ctx, opts, func(e *db.Engine) error {
		c, err := openCollection(e, database, collection)
		if err != nil {
			return err
		}
		doc, err := c.Get(ctx, id)
		if err != nil {
			return err
		}
		return writeDocs(w, []map[string]any{exportfmt.DocMap(doc)}, "jsonl")
	})
	if err != nil {
		return fmt.Errorf("getting document: %w", clientHint(err, findHint(database, collection)))
	}
	return nil
}

func openCollection(e *db.Engine, database, collection string) (*db.Collection, error) {
	d, err := e.Database(database)
	if err != nil {
		return nil, err
	}
	return d.Collection(collection)
}

// writeDocs prints docs as JSON Lines or as one JSON array.
func writeDocs(w io.Writer, docs []map[string]any, format string) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(docs)
	}
	enc := json.NewEncoder(w)
	for _, d := range docs {
		if err := enc.Encode(d); err != nil {
			return err
		}
	}
	return nil
}
