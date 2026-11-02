package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/hop-/cachydb/internal/exportfmt"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

// defaultClientURL is where "cachydb client" looks for a server without
// --url or CACHYDB_URL.
const defaultClientURL = "http://127.0.0.1:7601/mcp"

// clientPageSize is how many documents client export reads per
// find_documents call.
const clientPageSize = 500

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Call a running cachydb HTTP server's MCP tools from the shell",
	Long: `Talk to a running cachydb server over Streamable HTTP, as an MCP client.
While a server holds the data directory, "cachydb utils" commands cannot
open it; these commands go through the server instead. They also let
shell scripts and agents without MCP support call every tool.

The server address is --url, else the environment variable CACHYDB_URL,
else ` + defaultClientURL + `. Start the server with
"cachydb -t http -p 7601".`,
	Example: `  cachydb client tools
  cachydb client call find_documents '{"database":"kb","collection":"notes","query":{"limit":5}}'
  echo '{"collection":"notes"}' | cachydb client call count_documents -
  cachydb client export -d kb -o kb.jsonl
  cachydb client import -i kb.jsonl --on-conflict skip --url http://db-host:7601/mcp`,
}

var clientToolsCmd = &cobra.Command{
	Use:   "tools",
	Short: "List the server's tools",
	Long: `List the tools of the server: name and description, or with --format json
the full definitions (input and output schemas, annotations).`,
	Example: `  cachydb client tools
  cachydb client tools --format json`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if clientToolsFormat != "text" && clientToolsFormat != "json" {
			return fmt.Errorf("invalid --format %q: use text or json", clientToolsFormat)
		}
		cmd.SilenceUsage = true
		return withClient(cmd.Context(), clientURL(), func(cs *mcp.ClientSession) error {
			return listClientTools(cmd.Context(), cmd.OutOrStdout(), cs, clientToolsFormat)
		})
	},
}

var clientCallCmd = &cobra.Command{
	Use:   "call <tool> [json-arguments | -]",
	Short: "Call any tool with JSON arguments",
	Long: `Call a tool with a JSON object as its arguments (omitted: {}; "-" reads
them from stdin) and print the tool's structured result as JSON on
stdout. A tool error is printed on stderr and exits with status 1.`,
	Example: `  cachydb client call list_databases
  cachydb client call describe_collection '{"database":"kb","collection":"notes"}'
  cat args.json | cachydb client call update_document -`,
	Args: cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		raw := "{}"
		if len(args) == 2 {
			raw = args[1]
		}
		if raw == "-" {
			data, err := io.ReadAll(cmd.InOrStdin())
			if err != nil {
				return fmt.Errorf("reading arguments from stdin: %w", err)
			}
			raw = string(data)
		}
		var toolArgs map[string]any
		if err := json.Unmarshal([]byte(raw), &toolArgs); err != nil || toolArgs == nil {
			return fmt.Errorf("arguments must be a JSON object: %q", raw)
		}
		cmd.SilenceUsage = true
		return withClient(cmd.Context(), clientURL(), func(cs *mcp.ClientSession) error {
			return callClientTool(cmd.Context(), cmd.OutOrStdout(), cs, args[0], toolArgs)
		})
	},
}

var clientExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export through the server, in the utils export format",
	Long: `Export databases, a database, or a collection through the running
server, in the same JSON Lines format as "cachydb utils export". Documents
are read in pages of ` + fmt.Sprint(clientPageSize) + `, so the export is not a point-in-time
snapshot: writes during the export may or may not be included.`,
	Example: `  cachydb client export -o all.jsonl
  cachydb client export -d kb -c notes`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		scope := exportfmt.Scope{Database: exportDatabase, Collection: exportCollection}
		if err := scope.Validate(); err != nil {
			return err
		}
		cmd.SilenceUsage = true
		return withOutputFile(exportOutput, cmd.OutOrStdout(), func(w io.Writer) error {
			return withClient(cmd.Context(), clientURL(), func(cs *mcp.ClientSession) error {
				return exportfmt.Export(cmd.Context(), w, clientStore{cs: cs}, scope, getVersion(), time.Now())
			})
		})
	},
}

var clientImportCmd = &cobra.Command{
	Use:   "import",
	Short: "Import an export through the server",
	Long: `Import a JSON Lines export through the running server, with the same
rules and --on-conflict modes as "cachydb utils import".`,
	Example: `  cachydb client import -i kb.jsonl
  cachydb client import -i kb.jsonl --on-conflict replace`,
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
			return withClient(cmd.Context(), clientURL(), func(cs *mcp.ClientSession) error {
				st, err := exportfmt.Import(cmd.Context(), r, clientStore{cs: cs}, mode)
				if err != nil {
					return err
				}
				printImportStats(cmd.ErrOrStderr(), st)
				return nil
			})
		})
	},
}

var (
	clientURLFlag     string
	clientToolsFormat string
)

func init() {
	clientCmd.PersistentFlags().StringVar(&clientURLFlag, "url", "", "server MCP endpoint (env CACHYDB_URL; default "+defaultClientURL+")")
	clientToolsCmd.Flags().StringVarP(&clientToolsFormat, "format", "f", "text", "output format: text or json")
	clientExportCmd.Flags().StringVarP(&exportDatabase, "database", "d", "", "export only this database")
	clientExportCmd.Flags().StringVarP(&exportCollection, "collection", "c", "", "export only this collection (requires -d)")
	clientExportCmd.Flags().StringVarP(&exportOutput, "output", "o", "", "write to this file instead of stdout")
	clientImportCmd.Flags().StringVarP(&importInput, "input", "i", "", "file to import (- for stdin)")
	clientImportCmd.Flags().StringVar(&importOnConflict, "on-conflict", string(exportfmt.Fail), "existing _id: fail, skip, or replace")
	clientCmd.AddCommand(clientToolsCmd, clientCallCmd, clientExportCmd, clientImportCmd)
	rootCmd.AddCommand(clientCmd)
}

// clientURL returns the server endpoint: --url, CACHYDB_URL, or the
// default.
func clientURL() string {
	if clientURLFlag != "" {
		return clientURLFlag
	}
	if u := os.Getenv("CACHYDB_URL"); u != "" {
		return u
	}
	return defaultClientURL
}

// withClient connects to the server at url, calls fn, and closes the
// session.
func withClient(ctx context.Context, url string, fn func(*mcp.ClientSession) error) (err error) {
	client := mcp.NewClient(&mcp.Implementation{Name: "cachydb-client", Version: getVersion()}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	if err != nil {
		return fmt.Errorf("connecting to the cachydb server at %s (start one with \"cachydb -t http\", or set --url / CACHYDB_URL): %w", url, err)
	}
	defer func() {
		if cerr := cs.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("closing the MCP session: %w", cerr)
		}
	}()
	return fn(cs)
}

// listClientTools prints the server's tools.
func listClientTools(ctx context.Context, w io.Writer, cs *mcp.ClientSession, format string) error {
	var tools []*mcp.Tool
	for tool, err := range cs.Tools(ctx, nil) {
		if err != nil {
			return fmt.Errorf("listing tools: %w", err)
		}
		tools = append(tools, tool)
	}
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(tools)
	}
	out := &output{w: w}
	for _, t := range tools {
		out.printf("%s\n    %s\n", t.Name, t.Description)
	}
	return out.err
}

// errToolFailed reports a tool error result.
var errToolFailed = errors.New("tool error")

// callClientTool calls tool and prints its structured result.
func callClientTool(ctx context.Context, w io.Writer, cs *mcp.ClientSession, tool string, args map[string]any) error {
	res, err := callTool(ctx, cs, tool, args)
	if err != nil {
		return err
	}
	var v any = res.StructuredContent
	if v == nil {
		v = map[string]any{"content": resultText(res)}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// callTool calls tool; a tool error is returned as an error wrapping
// errToolFailed with the tool's message.
func callTool(ctx context.Context, cs *mcp.ClientSession, tool string, args map[string]any) (*mcp.CallToolResult, error) {
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return nil, fmt.Errorf("calling %s: %w", tool, err)
	}
	if res.IsError {
		return nil, fmt.Errorf("%w: %s: %s", errToolFailed, tool, resultText(res))
	}
	return res, nil
}

// resultText joins the text content of a tool result.
func resultText(res *mcp.CallToolResult) string {
	var parts []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, tc.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// callInto calls tool and decodes its structured result into out.
func callInto(ctx context.Context, cs *mcp.ClientSession, tool string, args map[string]any, out any) error {
	res, err := callTool(ctx, cs, tool, args)
	if err != nil {
		return err
	}
	data, err := json.Marshal(res.StructuredContent)
	if err != nil {
		return fmt.Errorf("%s: encoding result: %w", tool, err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s: decoding result: %w", tool, err)
	}
	return nil
}

// clientStore is an exportfmt Source and Target over the MCP tools of a
// running server.
type clientStore struct {
	cs *mcp.ClientSession
}

var (
	_ exportfmt.Source = clientStore{}
	_ exportfmt.Target = clientStore{}
)

func (c clientStore) Databases(ctx context.Context) ([]string, error) {
	var out mcpserver.ListDatabasesOutput
	err := callInto(ctx, c.cs, "list_databases", map[string]any{}, &out)
	return out.Databases, err
}

// SchemaVersion returns the current schema version: no tool reports it,
// and a server keeps its databases migrated.
func (c clientStore) SchemaVersion(context.Context, string) (int, error) {
	return db.CurrentSchemaVersion, nil
}

func (c clientStore) Collections(ctx context.Context, database string) ([]string, error) {
	var out mcpserver.ListCollectionsOutput
	err := callInto(ctx, c.cs, "list_collections", map[string]any{"database": database}, &out)
	return out.Collections, err
}

func (c clientStore) Describe(ctx context.Context, database, collection string) (*db.Schema, []db.IndexInfo, error) {
	var out mcpserver.DescribeCollectionOutput
	if err := callInto(ctx, c.cs, "describe_collection", map[string]any{"database": database, "collection": collection}, &out); err != nil {
		return nil, nil, err
	}
	var schema *db.Schema
	if out.Schema != nil {
		schema = &db.Schema{Fields: make(map[string]db.Field, len(out.Schema.Fields))}
		for name, f := range out.Schema.Fields {
			schema.Fields[name] = db.Field{Type: db.FieldType(f.Type), Required: f.Required}
		}
	}
	indexes := make([]db.IndexInfo, len(out.Indexes))
	for i, ix := range out.Indexes {
		indexes[i] = db.IndexInfo{Name: ix.Name, Field: ix.Field}
	}
	return schema, indexes, nil
}

func (c clientStore) Documents(ctx context.Context, database, collection string, fn func(map[string]any) error) error {
	for skip := 0; ; skip += clientPageSize {
		var out mcpserver.FindDocumentsOutput
		err := callInto(ctx, c.cs, "find_documents", map[string]any{
			"database": database, "collection": collection,
			"query": map[string]any{"skip": skip, "limit": clientPageSize},
		}, &out)
		if err != nil {
			return err
		}
		for _, d := range out.Documents {
			if err := fn(d); err != nil {
				return err
			}
		}
		if len(out.Documents) < clientPageSize {
			return nil
		}
	}
}

func (c clientStore) EnsureDatabase(ctx context.Context, name string) error {
	_, err := callTool(ctx, c.cs, "create_database", map[string]any{"name": name})
	return err
}

func (c clientStore) Lookup(ctx context.Context, database, collection string) (*db.Schema, []db.IndexInfo, bool, error) {
	colls, err := c.Collections(ctx, database)
	if err != nil {
		return nil, nil, false, err
	}
	found := false
	for _, name := range colls {
		found = found || name == collection
	}
	if !found {
		return nil, nil, false, nil
	}
	schema, indexes, err := c.Describe(ctx, database, collection)
	return schema, indexes, err == nil, err
}

// schemaArg returns s as a tool argument (nil for none).
func schemaArg(s *db.Schema) any {
	if s == nil {
		return nil
	}
	return s
}

func (c clientStore) CreateCollection(ctx context.Context, database, collection string, schema *db.Schema) error {
	args := map[string]any{"database": database, "name": collection}
	if schema != nil {
		args["schema"] = schema
	}
	_, err := callTool(ctx, c.cs, "create_collection", jsonArgs(args))
	return err
}

func (c clientStore) UpdateSchema(ctx context.Context, database, collection string, schema *db.Schema) error {
	_, err := callTool(ctx, c.cs, "update_schema", jsonArgs(map[string]any{"database": database, "collection": collection, "schema": schemaArg(schema)}))
	return err
}

func (c clientStore) CreateIndex(ctx context.Context, database, collection, name, field string) error {
	_, err := callTool(ctx, c.cs, "create_index", map[string]any{"database": database, "collection": collection, "index_name": name, "field_name": field})
	return err
}

func (c clientStore) DropIndex(ctx context.Context, database, collection, name string) error {
	_, err := callTool(ctx, c.cs, "drop_index", map[string]any{"database": database, "collection": collection, "index_name": name})
	return err
}

func (c clientStore) InsertDocument(ctx context.Context, database, collection string, doc map[string]any) error {
	_, err := callTool(ctx, c.cs, "insert_document", map[string]any{"database": database, "collection": collection, "document": doc})
	if err != nil && strings.Contains(err.Error(), db.ErrExists.Error()) {
		return errors.Join(exportfmt.ErrDocumentExists, err)
	}
	return err
}

func (c clientStore) ReplaceDocument(ctx context.Context, database, collection string, doc map[string]any) error {
	id, _ := doc["_id"].(string)
	_, err := callTool(ctx, c.cs, "replace_document", map[string]any{"database": database, "collection": collection, "id": id, "document": doc})
	return err
}

// jsonArgs round-trips args through JSON so typed values (schemas) become
// plain JSON objects.
func jsonArgs(args map[string]any) map[string]any {
	data, err := json.Marshal(args)
	if err != nil {
		return args
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return args
	}
	return out
}
