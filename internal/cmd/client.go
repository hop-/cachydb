package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
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
"cachydb -t http -p 7601".

A server that requires tokens needs --token, or the environment variable
CACHYDB_TOKEN (preferred: a flag shows in the process list); it is sent
as "Authorization: Bearer <token>", only to the scheme and host of the
server address. The client does not follow redirects, so point --url at
the final endpoint. Use https:// for a server on another machine, so the
token is not sent in clear text. See "cachydb docs access".`,
	Example: `  cachydb client tools
  cachydb client call find_documents '{"database":"kb","collection":"notes","query":{"limit":5}}'
  echo '{"collection":"notes"}' | cachydb client call count_documents -
  cachydb client export -d kb -o kb.jsonl
  cachydb client import -i kb.jsonl --on-conflict skip --url http://db-host:7601/mcp
  CACHYDB_TOKEN="$TOKEN" cachydb client call whoami --url https://db-host:7601/mcp`,
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
				t, err := importTarget(cmd.Context(), cs)
				if err != nil {
					return err
				}
				st, err := exportfmt.Import(cmd.Context(), r, t, mode)
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
	clientTokenFlag   string
	clientToolsFormat string
)

func init() {
	clientCmd.PersistentFlags().StringVar(&clientURLFlag, "url", "", "server MCP endpoint (env CACHYDB_URL; default "+defaultClientURL+")")
	clientCmd.PersistentFlags().StringVar(&clientTokenFlag, "token", "", "token for a server that requires one, sent as a bearer token (env CACHYDB_TOKEN, preferred)")
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

// clientToken returns the token: --token, else CACHYDB_TOKEN, else "".
func clientToken() string {
	if clientTokenFlag != "" {
		return clientTokenFlag
	}
	return os.Getenv("CACHYDB_TOKEN")
}

// bearerTransport sends token (if any) as a bearer token on requests to
// the endpoint's scheme and host only, and remembers whether the server
// answered 401 Unauthorized.
type bearerTransport struct {
	token        string
	scheme, host string // the endpoint's; the token is sent nowhere else
	base         http.RoundTripper
	unauthorized atomic.Bool
}

// newBearerTransport returns a bearerTransport that sends token to
// endpoint only. An endpoint that does not parse gets no token; connecting
// to it fails anyway.
func newBearerTransport(endpoint, token string) *bearerTransport {
	t := &bearerTransport{token: token, base: http.DefaultTransport}
	if u, err := url.Parse(endpoint); err == nil {
		t.scheme, t.host = u.Scheme, u.Host
	}
	return t
}

func (t *bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.token != "" && t.host != "" && r.URL.Scheme == t.scheme && strings.EqualFold(r.URL.Host, t.host) {
		// A RoundTripper must not modify the caller's request.
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+t.token)
	}
	resp, err := t.base.RoundTrip(r)
	if err == nil && resp.StatusCode == http.StatusUnauthorized {
		t.unauthorized.Store(true)
	}
	return resp, err
}

// authHint explains a 401 answer, naming the flag; "" if there was none.
func (t *bearerTransport) authHint() string {
	switch {
	case !t.unauthorized.Load():
		return ""
	case t.token == "":
		return "the server requires a token: pass --token or set CACHYDB_TOKEN"
	default:
		return "the server rejected the token (unknown or revoked): check --token / CACHYDB_TOKEN"
	}
}

// withClient connects to the server at url, sending the token of
// clientToken, calls fn, and closes the session. An error after a 401
// answer carries a hint that names --token.
func withClient(ctx context.Context, url string, fn func(*mcp.ClientSession) error) (err error) {
	rt := newBearerTransport(url, clientToken())
	defer func() {
		if hint := rt.authHint(); err != nil && hint != "" {
			err = fmt.Errorf("%s: %w", hint, err)
		}
	}()
	client := mcp.NewClient(&mcp.Implementation{Name: "cachydb-client", Version: getVersion()}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             url,
		HTTPClient:           &http.Client{Transport: rt, CheckRedirect: refuseRedirect},
		DisableStandaloneSSE: true,
		MaxRetries:           -1,
	}, nil)
	if err != nil {
		if rt.authHint() != "" {
			return fmt.Errorf("connecting to the cachydb server at %s: %w", url, err)
		}
		return fmt.Errorf("connecting to the cachydb server at %s (start one with \"cachydb -t http\", or set --url / CACHYDB_URL): %w", url, err)
	}
	defer func() {
		if cerr := cs.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("closing the MCP session: %w", cerr)
		}
	}()
	return fn(cs)
}

// refuseRedirect is the client's redirect policy: the MCP endpoint never
// needs a redirect, and following one could send the request, token
// included, to another host or over plain HTTP.
func refuseRedirect(req *http.Request, _ []*http.Request) error {
	return fmt.Errorf("the server redirected to %s; cachydb client does not follow redirects: set --url / CACHYDB_URL to the final endpoint", req.URL.Redacted())
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
	_ exportfmt.Source      = clientStore{}
	_ exportfmt.BatchTarget = clientStore{}
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

// Documents reads every document of the collection in _id order, one
// find_documents page at a time. It asks for clientPageSize documents per
// page; the server's result byte cap may return fewer, so it follows
// has_more and next_skip. A server older than those keys returns full
// pages, and the listing ends at the first short one.
func (c clientStore) Documents(ctx context.Context, database, collection string, fn func(map[string]any) error) error {
	for skip := 0; ; {
		var out struct {
			Documents []map[string]any `json:"documents"`
			HasMore   *bool            `json:"has_more"`
			NextSkip  int              `json:"next_skip"`
		}
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
		switch {
		case out.HasMore == nil: // a server without paging keys
			if len(out.Documents) < clientPageSize {
				return nil
			}
			skip += clientPageSize
		case !*out.HasMore:
			return nil
		case out.NextSkip <= skip:
			return fmt.Errorf("find_documents on %s.%s: has_more without a next_skip after %d", database, collection, skip)
		default:
			skip = out.NextSkip
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

// importTarget returns the import target for the server of cs: the
// clientStore, which writes documents in batches with insert_documents,
// or, for a server without that tool (older than plan 013), the
// clientStore as a plain Target that inserts one document per call.
func importTarget(ctx context.Context, cs *mcp.ClientSession) (exportfmt.Target, error) {
	for tool, err := range cs.Tools(ctx, nil) {
		if err != nil {
			return nil, fmt.Errorf("listing tools: %w", err)
		}
		if tool.Name == "insert_documents" {
			return clientStore{cs: cs}, nil
		}
	}
	return struct{ exportfmt.Target }{clientStore{cs: cs}}, nil
}

// clientBatchBytes bounds the JSON size of the documents of one
// insert_documents call from client import, below the 4 MiB request body
// limit of the server's HTTP transport (the SDK's
// DefaultMaxRequestBodyBytes) with room for the rest of the request.
const clientBatchBytes = 3 << 20

// itemIndexRE finds the failing item that insert_documents names in its
// error ("documents[3]: ...").
var itemIndexRE = regexp.MustCompile(`documents\[(\d+)\]`)

// InsertDocuments implements exportfmt.BatchTarget with insert_documents,
// one call per run of documents. Documents whose JSON exceeds
// clientBatchBytes fail with db.ErrBatchTooLarge before any call, so
// Import splits them (and sends a single such document through
// InsertDocument). In Skip and Replace mode one find_documents lookup
// first tells which _ids exist; the missing documents are inserted in
// one atomic call, and in Replace mode the existing ones are then
// replaced one replace_document call each (there is no batch replace
// tool), so a failing replace leaves the inserts and earlier replaces
// applied.
func (c clientStore) InsertDocuments(ctx context.Context, database, collection string, docs []map[string]any, mode exportfmt.OnConflict) (exportfmt.Stats, error) {
	data, err := json.Marshal(docs)
	if err != nil {
		return exportfmt.Stats{}, fmt.Errorf("encoding documents: %w", err)
	}
	if len(data) > clientBatchBytes {
		return exportfmt.Stats{}, fmt.Errorf("%w: %d bytes of documents, the client sends at most %d per call",
			db.ErrBatchTooLarge, len(data), clientBatchBytes)
	}
	var exists map[string]bool
	if mode != exportfmt.Fail {
		if exists, err = c.existingIDs(ctx, database, collection, docs); err != nil {
			return exportfmt.Stats{}, err
		}
	}
	var (
		st       exportfmt.Stats
		inserts  []map[string]any
		pos      []int // insert → position in docs
		replaces []int // positions in docs
	)
	for i, d := range docs {
		id, _ := d["_id"].(string)
		switch {
		case !exists[id]:
			inserts = append(inserts, d)
			pos = append(pos, i)
		case mode == exportfmt.Skip:
			st.Skipped++
		default:
			replaces = append(replaces, i)
		}
	}
	if len(inserts) > 0 {
		var out mcpserver.InsertDocumentsOutput
		err := callInto(ctx, c.cs, "insert_documents", map[string]any{
			"database": database, "collection": collection, "documents": inserts,
		}, &out)
		if err != nil {
			if strings.Contains(err.Error(), db.ErrExists.Error()) {
				err = errors.Join(exportfmt.ErrDocumentExists, err)
			}
			if m := itemIndexRE.FindStringSubmatch(err.Error()); m != nil {
				if i, convErr := strconv.Atoi(m[1]); convErr == nil && i < len(pos) {
					return exportfmt.Stats{}, &db.BatchItemError{Index: pos[i], Err: err}
				}
			}
			return exportfmt.Stats{}, err
		}
		st.Inserted = len(inserts)
	}
	for _, i := range replaces {
		if err := c.ReplaceDocument(ctx, database, collection, docs[i]); err != nil {
			return exportfmt.Stats{}, &db.BatchItemError{Index: i, Err: err}
		}
		st.Replaced++
	}
	return st, nil
}

// existingIDs returns which _ids of docs exist in the collection, with
// one find_documents call (more if the result byte cap splits it).
func (c clientStore) existingIDs(ctx context.Context, database, collection string, docs []map[string]any) (map[string]bool, error) {
	ids := make([]any, len(docs))
	for i, d := range docs {
		ids[i] = d["_id"]
	}
	exists := make(map[string]bool)
	for skip := 0; ; {
		var out mcpserver.FindDocumentsOutput
		err := callInto(ctx, c.cs, "find_documents", map[string]any{
			"database": database, "collection": collection,
			"query": map[string]any{
				"filters": []any{map[string]any{"field": "_id", "operator": "in", "value": ids}},
				"fields":  []any{"_id"}, "limit": len(ids), "skip": skip,
			},
		}, &out)
		if err != nil {
			return nil, err
		}
		for _, d := range out.Documents {
			if id, ok := d["_id"].(string); ok {
				exists[id] = true
			}
		}
		if !out.HasMore {
			return exists, nil
		}
		if out.NextSkip <= skip {
			return nil, fmt.Errorf("find_documents on %s.%s: has_more without a next_skip after %d", database, collection, skip)
		}
		skip = out.NextSkip
	}
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
