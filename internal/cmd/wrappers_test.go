package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/exportfmt"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

// saveGlobals restores the package-level flag variables and the root
// command's arguments and writers when t ends: running a command through
// rootCmd sets them.
func saveGlobals(t *testing.T) {
	t.Helper()
	root, port, transport, dbn := generalRootDir, generalServerPort, generalTransport, generalDBName
	age, size, cache := generalWALRetentionAge, generalWALRetentionSize, generalCacheSize
	collections, all, format, vjson := showCollections, docsAll, docsFormat, versionJSON
	hURL, hPort, hTransport := healthcheckURL, healthcheckPort, healthcheckTransport
	mDB, mAll, mTarget, mShow, mList := migrateDatabase, migrateAll, targetVersion, showVersion, listMigrations
	eDB, eColl, eOut, iIn, iMode := exportDatabase, exportCollection, exportOutput, importInput, importOnConflict
	fDB, fColl, fQuery, fFormat, gID := findDatabase, findCollection, findQuery, findFormat, getID
	cURL, cFormat := clientURLFlag, clientToolsFormat
	cfgErr := configErr
	t.Cleanup(func() {
		exportDatabase, exportCollection, exportOutput, importInput, importOnConflict = eDB, eColl, eOut, iIn, iMode
		findDatabase, findCollection, findQuery, findFormat, getID = fDB, fColl, fQuery, fFormat, gID
		clientURLFlag, clientToolsFormat = cURL, cFormat
		generalRootDir, generalServerPort, generalTransport, generalDBName = root, port, transport, dbn
		generalWALRetentionAge, generalWALRetentionSize, generalCacheSize = age, size, cache
		showCollections, docsAll, docsFormat, versionJSON = collections, all, format, vjson
		healthcheckURL, healthcheckPort, healthcheckTransport = hURL, hPort, hTransport
		migrateDatabase, migrateAll, targetVersion, showVersion, listMigrations = mDB, mAll, mTarget, mShow, mList
		configErr = cfgErr
		resetContexts(rootCmd)
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
	})
}

// resetContexts clears the context cobra stored on c and its
// subcommands. ExecuteContext sets the root's context, but a subcommand
// keeps the context of the first run that reached it, which in a test
// binary is an earlier (cancelled) run.
func resetContexts(c *cobra.Command) {
	c.SetContext(nil) //nolint:staticcheck // nil makes cobra take the root's context again
	for _, sub := range c.Commands() {
		resetContexts(sub)
	}
}

// runCLI runs the CLI in-process with args and returns the exit status,
// stdout, and stderr. Flags keep their values between runs (they are
// package variables), so callers pass every flag they depend on.
func runCLI(t *testing.T, ctx context.Context, args ...string) (int, string, string) {
	t.Helper()
	saveGlobals(t)
	resetContexts(rootCmd)
	var stdout, stderr bytes.Buffer
	rootCmd.SetArgs(args)
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&stderr)
	code := execute(ctx, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestExecuteExitStatus(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantOut    string
		wantErrOut string
	}{
		{name: "version", args: []string{"version", "--json=false"}, wantOut: "cachydb version " + getVersion() + "\n"},
		{name: "unknown command", args: []string{"no-such-command"}, wantCode: 1, wantErrOut: `Error: unknown command "no-such-command"`},
		{name: "bad flag", args: []string{"version", "--no-such-flag"}, wantCode: 1, wantErrOut: "Error: unknown flag: --no-such-flag"},
		{name: "invalid transport", args: []string{"app", "-t", "carrier-pigeon", "-R", t.TempDir()}, wantCode: 1, wantErrOut: `Error: invalid transport "carrier-pigeon"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errOut := runCLI(t, context.Background(), tt.args...)
			if code != tt.wantCode {
				t.Errorf("exit status %d, want %d (stderr %q)", code, tt.wantCode, errOut)
			}
			if tt.wantOut != "" && out != tt.wantOut {
				t.Errorf("stdout = %q, want %q", out, tt.wantOut)
			}
			if !strings.Contains(errOut, tt.wantErrOut) {
				t.Errorf("stderr = %q, want it to contain %q", errOut, tt.wantErrOut)
			}
		})
	}

	t.Run("configuration error", func(t *testing.T) {
		saveGlobals(t)
		configErr = errors.New("bad PORT")
		var stderr bytes.Buffer
		if code := execute(context.Background(), &stderr); code != 1 || stderr.String() != "Error: bad PORT\n" {
			t.Errorf("execute = %d, stderr %q", code, stderr.String())
		}
	})
}

func TestCommandWrappers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	seed(t, dir)
	okURL, _ := healthServer(t, http.StatusOK)

	tests := []struct {
		name     string
		args     []string
		wantCode int
		check    func(t *testing.T, out, errOut string)
	}{
		{
			name: "utils list",
			args: []string{"utils", "list", "-c", "-R", dir},
			check: func(t *testing.T, out, _ string) {
				if !strings.Contains(out, "Found 2 database(s)") || !strings.Contains(out, "users (2 documents)") {
					t.Errorf("stdout = %q", out)
				}
			},
		},
		{
			name: "utils migrate --show-version",
			args: []string{"utils", "migrate", "-R", dir, "-d", "main", "--show-version", "--all=false", "--list=false"},
			check: func(t *testing.T, out, _ string) {
				if !strings.Contains(out, strconv.Itoa(db.CurrentSchemaVersion)) {
					t.Errorf("stdout = %q", out)
				}
			},
		},
		{
			name: "utils migrate --list",
			args: []string{"utils", "migrate", "--list", "-R", dir},
			check: func(t *testing.T, out, _ string) {
				if out == "" {
					t.Error("no output")
				}
			},
		},
		{
			name:     "utils migrate without a database",
			args:     []string{"utils", "migrate", "-R", dir, "-d", "", "--all=false", "--list=false", "--show-version=false"},
			wantCode: 1,
			check: func(t *testing.T, _, errOut string) {
				if !strings.HasPrefix(errOut, "Error: ") {
					t.Errorf("stderr = %q", errOut)
				}
			},
		},
		{
			name: "docs index",
			args: []string{"docs", "--all=false", "--format", "md"},
			check: func(t *testing.T, out, _ string) {
				if !strings.Contains(out, "quickstart") {
					t.Errorf("stdout = %q", out)
				}
			},
		},
		{
			name: "docs topic as JSON",
			args: []string{"docs", "query", "--all=false", "--format", "json"},
			check: func(t *testing.T, out, _ string) {
				var v map[string]any
				if err := json.Unmarshal([]byte(out), &v); err != nil || v["name"] != "query" {
					t.Errorf("stdout = %q (%v)", out, err)
				}
			},
		},
		{name: "docs unknown topic", args: []string{"docs", "nope", "--all=false", "--format", "md"}, wantCode: 1},
		{name: "healthcheck ok", args: []string{"healthcheck", "--url", okURL}},
		{
			name: "healthcheck stdio",
			args: []string{"healthcheck", "--url", "", "-t", "stdio"},
			check: func(t *testing.T, _, errOut string) {
				if !strings.Contains(errOut, "nothing to probe") {
					t.Errorf("stderr = %q", errOut)
				}
			},
		},
		{name: "healthcheck refused", args: []string{"healthcheck", "--url", "", "-t", "http", "-p", strconv.Itoa(closedPort(t))}, wantCode: 1},
		{
			name: "version --json",
			args: []string{"version", "--json"},
			check: func(t *testing.T, out, _ string) {
				var v versionInfo
				if err := json.Unmarshal([]byte(out), &v); err != nil || v.Version != getVersion() {
					t.Errorf("stdout = %q (%v)", out, err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errOut := runCLI(t, ctx, tt.args...)
			if code != tt.wantCode {
				t.Fatalf("exit status %d, want %d; stdout %q, stderr %q", code, tt.wantCode, out, errOut)
			}
			if tt.check != nil {
				tt.check(t, out, errOut)
			}
		})
	}
}

// TestRunAppHTTP runs "cachydb app" over HTTP in-process, waits until the
// health probe succeeds, cancels the command context (as SIGTERM does),
// and checks for a clean exit that flushed the data directory.
func TestRunAppHTTP(t *testing.T) {
	dir := t.TempDir()
	port := closedPort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		code        int
		out, errOut string
	}
	done := make(chan result, 1)
	saveGlobals(t)
	resetContexts(rootCmd)
	var stdout, stderr bytes.Buffer
	rootCmd.SetArgs([]string{"app", "-t", "http", "-p", strconv.Itoa(port), "-R", dir, "-d", "appdb"})
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&stderr)
	go func() {
		code := execute(ctx, &stderr)
		done <- result{code, stdout.String(), stderr.String()}
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	deadline := time.Now().Add(10 * time.Second)
	for {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), time.Second)
		err := healthcheck(probeCtx, &bytes.Buffer{}, url, "", 0)
		probeCancel()
		if err == nil {
			break
		}
		select {
		case r := <-done:
			t.Fatalf("app exited early with %d: %s", r.code, r.errOut)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("server not healthy within 10s: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A tool call through the client proves the default database exists.
	err := withClient(context.Background(), fmt.Sprintf("http://127.0.0.1:%d/mcp", port), func(cs *mcp.ClientSession) error {
		_, err := callTool(context.Background(), cs, "create_collection", map[string]any{"name": "c"})
		return err
	})
	if err != nil {
		t.Fatalf("tool call: %v", err)
	}

	cancel()
	select {
	case r := <-done:
		if r.code != 0 {
			t.Fatalf("app exit status %d; stderr %s", r.code, r.errOut)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("app did not stop within 15s of cancellation")
	}

	// The lock is released and the collection is there.
	var out bytes.Buffer
	if err := listDatabases(context.Background(), &out, db.Options{Dir: dir}, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "appdb") || !strings.Contains(out.String(), "c (0 documents)") {
		t.Errorf("after shutdown:\n%s", out.String())
	}
}

// TestClientImportReplace imports through a running server into an
// existing collection with a different schema and indexes, using the
// client's UpdateSchema, DropIndex, and ReplaceDocument, then checks that
// a schema existing documents violate is rejected.
func TestClientImportReplace(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	seedKB(t, srcDir)
	var data bytes.Buffer
	if err := exportOffline(ctx, &data, db.Options{Dir: srcDir}, exportfmt.Scope{}); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	e := openEngine(t, dst)
	d, err := e.CreateDatabase(ctx, "kb")
	if err != nil {
		t.Fatal(err)
	}
	const target = "notes" // seedKB: a schema and the index by_tags on tags
	if err := d.CreateCollection(ctx, target, nil); err != nil {
		t.Fatal(err)
	}
	c, _ := d.Collection(target)
	// by_tags on another field is replaced (DropIndex), by_extra is kept.
	for _, ix := range [][2]string{{"by_extra", "extra"}, {"by_tags", "labels"}} {
		if err := c.CreateIndex(ctx, ix[0], ix[1]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Insert(ctx, &db.Document{ID: "n1", Data: map[string]any{"title": "old"}}); err != nil {
		t.Fatal(err)
	}
	closeEngine(t, e)

	url, stop := liveServer(t, dst)
	err = clientDo(t, url, func(cs *mcp.ClientSession) error {
		_, err := exportfmt.Import(ctx, strings.NewReader(data.String()), clientStore{cs: cs}, exportfmt.Fail)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "different schema") {
		t.Fatalf("import with fail = %v, want a schema conflict", err)
	}
	var st exportfmt.Stats
	err = clientDo(t, url, func(cs *mcp.ClientSession) error {
		var err error
		st, err = exportfmt.Import(ctx, strings.NewReader(data.String()), clientStore{cs: cs}, exportfmt.Replace)
		return err
	})
	if err != nil {
		t.Fatalf("import with replace: %v", err)
	}
	// A second replace import replaces every document.
	err = clientDo(t, url, func(cs *mcp.ClientSession) error {
		var err error
		st, err = exportfmt.Import(ctx, strings.NewReader(data.String()), clientStore{cs: cs}, exportfmt.Replace)
		return err
	})
	if err != nil || st.Replaced == 0 || st.Inserted != 0 {
		t.Fatalf("second import with replace: %+v, %v", st, err)
	}
	// A schema the stored documents violate: require a field none has.
	var violating strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(data.String()), "\n") {
		var l map[string]any
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatal(err)
		}
		if l["type"] == exportfmt.TypeCollection && l["name"] == target {
			l["schema"] = map[string]any{"fields": map[string]any{"no_such_field": map[string]any{"type": "string", "required": true}}}
		}
		if l["type"] == exportfmt.TypeDocument {
			continue
		}
		b, err := json.Marshal(l)
		if err != nil {
			t.Fatal(err)
		}
		violating.Write(append(b, '\n'))
	}
	err = clientDo(t, url, func(cs *mcp.ClientSession) error {
		_, err := exportfmt.Import(ctx, strings.NewReader(violating.String()), clientStore{cs: cs}, exportfmt.Replace)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "updating the schema") {
		t.Fatalf("import of a violated schema = %v, want an update_schema error", err)
	}
	stop()

	var again bytes.Buffer
	if err := exportOffline(ctx, &again, db.Options{Dir: dst}, exportfmt.Scope{Database: "kb", Collection: target}); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	if err := exportOffline(ctx, &want, db.Options{Dir: srcDir}, exportfmt.Scope{Database: "kb", Collection: target}); err != nil {
		t.Fatal(err)
	}
	// Documents and schema match the source; the extra index is kept.
	gotLines := strings.Split(strings.TrimSpace(again.String()), "\n")[1:]
	wantLines := strings.Split(strings.TrimSpace(want.String()), "\n")[1:]
	if len(gotLines) != len(wantLines) {
		t.Fatalf("export has %d lines, want %d:\n%s", len(gotLines), len(wantLines), again.String())
	}
	for i := range gotLines {
		if strings.Contains(wantLines[i], `"type":"collection"`) {
			if !strings.Contains(gotLines[i], `{"name":"by_extra","field":"extra"}`) ||
				!strings.Contains(gotLines[i], `{"name":"by_tags","field":"tags"}`) ||
				!strings.Contains(gotLines[i], `"schema":{"fields":{"title"`) {
				t.Errorf("collection line = %s", gotLines[i])
			}
			continue
		}
		if gotLines[i] != wantLines[i] {
			t.Errorf("line %d = %s, want %s", i, gotLines[i], wantLines[i])
		}
	}
}

// TestDataCommandsCLI runs the utils export/import/find/get and client
// tools/call/export/import commands through the CLI, including their flag
// validation and file handling.
func TestDataCommandsCLI(t *testing.T) {
	ctx := context.Background()
	kb := t.TempDir()
	seedKB(t, kb)
	url, _ := liveServer(t, t.TempDir())
	files := t.TempDir()
	dump := filepath.Join(files, "kb.jsonl")
	viaClient := filepath.Join(files, "client.jsonl")
	missing := filepath.Join(files, "no", "such", "file")

	exportFlags := func(extra ...string) []string {
		return append([]string{"-d", "", "-c", "", "-o", ""}, extra...)
	}
	findFlags := func(extra ...string) []string {
		return append([]string{"-R", kb, "-d", "kb", "-c", "notes", "-q", "", "-f", "jsonl"}, extra...)
	}
	tests := []struct {
		name     string
		args     []string
		env      string // CACHYDB_URL
		wantCode int
		wantOut  string // substring of stdout
		wantErr  string // substring of stderr
	}{
		{name: "utils export to a file", args: append([]string{"utils", "export", "-R", kb}, exportFlags("-o", dump)...)},
		{name: "utils export to stdout", args: append([]string{"utils", "export", "-R", kb}, exportFlags("-d", "kb")...), wantOut: `"name":"notes"`},
		{name: "utils export collection without database", args: append([]string{"utils", "export", "-R", kb}, exportFlags("-c", "notes")...), wantCode: 1, wantErr: "-c requires -d"},
		{name: "utils export to a bad path", args: append([]string{"utils", "export", "-R", kb}, exportFlags("-o", missing)...), wantCode: 1},
		{name: "utils import", args: []string{"utils", "import", "-R", t.TempDir(), "-i", dump, "--on-conflict", "fail"}, wantErr: "inserted"},
		{name: "utils import without input", args: []string{"utils", "import", "-R", t.TempDir(), "-i", "", "--on-conflict", "fail"}, wantCode: 1, wantErr: "-i/--input is required"},
		{name: "utils import bad mode", args: []string{"utils", "import", "-R", t.TempDir(), "-i", dump, "--on-conflict", "merge"}, wantCode: 1, wantErr: "invalid --on-conflict"},
		{name: "utils import missing file", args: []string{"utils", "import", "-R", t.TempDir(), "-i", missing, "--on-conflict", "fail"}, wantCode: 1},
		{name: "utils find", args: append([]string{"utils", "find"}, findFlags("-q", `{"limit":1}`, "-f", "json")...), wantOut: `"_id"`},
		{name: "utils find bad query", args: append([]string{"utils", "find"}, findFlags("-q", `{"nope":1}`)...), wantCode: 1, wantErr: "invalid --query"},
		{name: "utils find bad format", args: append([]string{"utils", "find"}, findFlags("-f", "xml")...), wantCode: 1, wantErr: "invalid --format"},
		{name: "utils find without collection", args: append([]string{"utils", "find"}, findFlags("-c", "")...), wantCode: 1, wantErr: "are required"},
		{name: "utils find missing collection", args: append([]string{"utils", "find"}, findFlags("-c", "nope")...), wantCode: 1, wantErr: "not found"},
		{name: "utils get", args: []string{"utils", "get", "-R", kb, "-d", "kb", "-c", "notes", "--id", "n1"}, wantOut: "WAL design"},
		{name: "utils get missing", args: []string{"utils", "get", "-R", kb, "-d", "kb", "-c", "notes", "--id", "nope"}, wantCode: 1},
		{name: "utils get without id", args: []string{"utils", "get", "-R", kb, "-d", "kb", "-c", "notes", "--id", ""}, wantCode: 1, wantErr: "are required"},
		{name: "client tools", args: []string{"client", "tools", "--url", url, "--format", "text"}, wantOut: "insert_document"},
		{name: "client tools URL from the environment", args: []string{"client", "tools", "--url", "", "--format", "json"}, env: url, wantOut: `"inputSchema"`},
		{name: "client tools bad format", args: []string{"client", "tools", "--url", url, "--format", "yaml"}, wantCode: 1, wantErr: "invalid --format"},
		{name: "client call", args: []string{"client", "call", "--url", url, "list_databases"}, wantOut: "databases"},
		{name: "client call bad arguments", args: []string{"client", "call", "--url", url, "list_databases", "[1]"}, wantCode: 1, wantErr: "must be a JSON object"},
		{name: "client import", args: []string{"client", "import", "--url", url, "-i", dump, "--on-conflict", "skip"}, wantErr: "inserted"},
		{name: "client import without input", args: []string{"client", "import", "--url", url, "-i", "", "--on-conflict", "skip"}, wantCode: 1, wantErr: "-i/--input is required"},
		{name: "client import bad mode", args: []string{"client", "import", "--url", url, "-i", dump, "--on-conflict", "merge"}, wantCode: 1, wantErr: "invalid --on-conflict"},
		{name: "client export to a file", args: append([]string{"client", "export", "--url", url}, exportFlags("-d", "kb", "-o", viaClient)...)},
		{name: "client export collection without database", args: append([]string{"client", "export", "--url", url}, exportFlags("-c", "notes")...), wantCode: 1, wantErr: "-c requires -d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CACHYDB_URL", tt.env)
			code, out, errOut := runCLI(t, ctx, tt.args...)
			if code != tt.wantCode {
				t.Fatalf("exit status %d, want %d; stdout %q, stderr %q", code, tt.wantCode, out, errOut)
			}
			if !strings.Contains(out, tt.wantOut) {
				t.Errorf("stdout = %q, want it to contain %q", out, tt.wantOut)
			}
			if !strings.Contains(errOut, tt.wantErr) {
				t.Errorf("stderr = %q, want it to contain %q", errOut, tt.wantErr)
			}
		})
	}

	// The client export of what the client imported equals the dump
	// (apart from the header).
	a, err := os.ReadFile(dump)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(viaClient)
	if err != nil {
		t.Fatal(err)
	}
	strip := func(s []byte) string { return strings.SplitN(string(s), "\n", 2)[1] }
	if strip(a) != strip(b) {
		t.Errorf("client export differs from the utils export:\n%s\nvs\n%s", a, b)
	}
}
