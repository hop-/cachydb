package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/config"
	"github.com/hop-/cachydb/internal/exportfmt"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

// Tests of the plan-013 stage-5 CLI additions: the find_documents bounds
// and result text settings, utils stats, client export and import over
// the bounded find_documents and insert_documents, and --on-conflict keep.

// saveResultSettings restores the result-setting flag variables, which
// saveGlobals does not cover.
func saveResultSettings(t *testing.T) {
	t.Helper()
	limit, maxBytes, text := generalDefaultLimit, generalMaxResultBytes, generalResultText
	sDB, sFormat := statsDatabase, statsFormat
	t.Cleanup(func() {
		generalDefaultLimit, generalMaxResultBytes, generalResultText = limit, maxBytes, text
		statsDatabase, statsFormat = sDB, sFormat
	})
}

// TestResultSettingsPrecedence (AC-21, AC-27): --default-limit,
// --max-result-bytes, and --result-text win over DEFAULT_LIMIT,
// MAX_RESULT_BYTES, and RESULT_TEXT, which win over the defaults.
func TestResultSettingsPrecedence(t *testing.T) {
	saveResultSettings(t)
	keys := []string{"DEFAULT_LIMIT", "MAX_RESULT_BYTES", "RESULT_TEXT"}
	for _, tt := range []struct {
		name     string
		env      map[string]string
		args     []string
		limit    int
		maxBytes config.ByteSize
		text     string
	}{
		{name: "defaults", limit: 100, maxBytes: config.MiB, text: "full"},
		{name: "env", env: map[string]string{"DEFAULT_LIMIT": "40", "MAX_RESULT_BYTES": "2MiB", "RESULT_TEXT": "summary"}, limit: 40, maxBytes: 2 * config.MiB, text: "summary"},
		{name: "flags", args: []string{"--default-limit", "7", "--max-result-bytes", "64KiB", "--result-text", "summary"}, limit: 7, maxBytes: 64 * config.KiB, text: "summary"},
		{name: "flags over env", env: map[string]string{"DEFAULT_LIMIT": "40", "MAX_RESULT_BYTES": "2MiB", "RESULT_TEXT": "summary"},
			args: []string{"--default-limit", "9", "--max-result-bytes", "1000", "--result-text", "full"}, limit: 9, maxBytes: 1000, text: "full"},
		{name: "mixed", env: map[string]string{"DEFAULT_LIMIT": "40", "RESULT_TEXT": "summary"}, args: []string{"--max-result-bytes", "5KiB"}, limit: 40, maxBytes: 5 * config.KiB, text: "summary"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Cleanup(func() {
				if err := config.Init(); err != nil {
					t.Errorf("restoring config: %v", err)
				}
			})
			for _, k := range keys {
				t.Setenv(k, tt.env[k])
				if _, ok := tt.env[k]; !ok {
					if err := os.Unsetenv(k); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := config.Init(); err != nil {
				t.Fatalf("config.Init: %v", err)
			}
			cmd := &cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { return nil }}
			setAllFlagsToCmd(cmd)
			cmd.SetArgs(tt.args)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if generalDefaultLimit != tt.limit || generalMaxResultBytes != tt.maxBytes || generalResultText != tt.text {
				t.Errorf("limit %d, max bytes %v, result text %q; want %d, %v, %q",
					generalDefaultLimit, generalMaxResultBytes, generalResultText, tt.limit, tt.maxBytes, tt.text)
			}
		})
	}
}

// TestResultSettingsRejected: a limit or byte cap that is not positive and
// an unknown result text mode stop the server before it starts.
func TestResultSettingsRejected(t *testing.T) {
	saveResultSettings(t)
	if err := validateResultLimits(1, 1); err != nil {
		t.Errorf("validateResultLimits(1, 1): %v", err)
	}
	for _, tt := range []struct {
		limit    int
		maxBytes config.ByteSize
	}{{0, 1}, {-1, 1}, {1, 0}, {1, -5}} {
		if err := validateResultLimits(tt.limit, tt.maxBytes); err == nil {
			t.Errorf("validateResultLimits(%d, %v) succeeded", tt.limit, tt.maxBytes)
		}
	}
	ctx := context.Background()
	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"--default-limit", "0"}, "--default-limit"},
		{[]string{"--default-limit", "-3"}, "--default-limit"},
		{[]string{"--max-result-bytes", "0"}, "--max-result-bytes"},
		{[]string{"--result-text", "brief"}, "--result-text"},
	} {
		dir := filepath.Join(t.TempDir(), "data")
		args := append([]string{"-R", dir, "-t", "stdio", "--default-limit", "100", "--max-result-bytes", "1MiB", "--result-text", "full"}, tt.args...)
		code, _, errOut := runCLI(t, ctx, args...)
		if code != 1 || !strings.Contains(errOut, tt.want) {
			t.Errorf("cachydb %v: exit %d, stderr %q; want 1 naming %s", tt.args, code, errOut, tt.want)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("cachydb %v opened the data directory (%v)", tt.args, err)
		}
	}
}

// TestUtilsStats (AC-26): utils stats prints the figures of every
// database or one, as text or JSON with the server_stats keys.
func TestUtilsStats(t *testing.T) {
	saveResultSettings(t)
	ctx := context.Background()
	dir := t.TempDir()
	seedKB(t, dir)
	e := openEngine(t, dir)
	if _, err := e.CreateDatabase(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	closeEngine(t, e)

	code, out, errOut := runCLI(t, ctx, "utils", "stats", "-R", dir, "-d", "", "-f", "text")
	if code != 0 {
		t.Fatalf("utils stats: exit %d, %s", code, errOut)
	}
	for _, want := range []string{
		"empty: 0 collection(s), 0 documents, data 0 bytes, index 0 bytes\n",
		"kb: 1 collection(s), 3 documents, data ",
		"  └─ notes: 3 documents, data ",
		"indexes: by_tags\n",
		"\nserver: version ",
		" hits, ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("text output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "_system") {
		t.Errorf("text output lists the system database:\n%s", out)
	}

	code, out, errOut = runCLI(t, ctx, "utils", "stats", "-R", dir, "-d", "kb", "-f", "json")
	if code != 0 {
		t.Fatalf("utils stats json: exit %d, %s", code, errOut)
	}
	var rep struct {
		Databases []mcpserver.DatabaseStats `json:"databases"`
		Server    map[string]any            `json:"server"`
	}
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("json output: %v\n%s", err, out)
	}
	if len(rep.Databases) != 1 || rep.Databases[0].Name != "kb" || rep.Databases[0].Documents != 3 ||
		len(rep.Databases[0].Collections) != 1 || !reflect.DeepEqual(rep.Databases[0].Collections[0].Indexes, []string{"by_tags"}) ||
		rep.Databases[0].Collections[0].IndexBytes <= 0 {
		t.Errorf("json databases %+v", rep.Databases)
	}
	for _, k := range []string{"version", "wal_bytes", "cache"} {
		if _, ok := rep.Server[k]; !ok {
			t.Errorf("json server lacks %q: %v", k, rep.Server)
		}
	}
	if _, ok := rep.Server["uptime_seconds"]; ok {
		t.Error("json server has uptime_seconds offline")
	}

	if code, _, errOut := runCLI(t, ctx, "utils", "stats", "-R", dir, "-d", "nope", "-f", "text"); code != 1 || !strings.Contains(errOut, "not found") {
		t.Errorf("unknown database: exit %d, %q", code, errOut)
	}
	if code, _, errOut := runCLI(t, ctx, "utils", "stats", "-R", dir, "-d", "", "-f", "xml"); code != 1 || !strings.Contains(errOut, "invalid --format") {
		t.Errorf("bad format: exit %d, %q", code, errOut)
	}
	code, out, _ = runCLI(t, ctx, "utils", "stats", "-R", t.TempDir(), "-d", "", "-f", "text")
	if code != 0 || !strings.HasPrefix(out, "No databases found\n") {
		t.Errorf("empty directory: exit %d, %q", code, out)
	}
}

// seedLarge creates database big with collection docs: a schema, a unique
// index, and n documents of about 3 KB, more than one find_documents page
// under the default byte cap and more than one 3 MiB insert_documents call.
func seedLarge(t *testing.T, dir string, n int) {
	t.Helper()
	ctx := context.Background()
	e := openEngine(t, dir)
	defer closeEngine(t, e)
	d, err := e.CreateDatabase(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	s := &db.Schema{Fields: map[string]db.Field{"i": {Type: db.TypeNumber, Required: true}}}
	if err := d.CreateCollection(ctx, "docs", s); err != nil {
		t.Fatal(err)
	}
	c, _ := d.Collection("docs")
	if err := c.CreateIndex(ctx, "by_code", "code", db.Unique(true)); err != nil {
		t.Fatal(err)
	}
	ops := make([]db.WriteOp, 0, 500)
	flush := func() {
		if _, err := c.Batch(ctx, ops); err != nil {
			t.Fatal(err)
		}
		ops = ops[:0]
	}
	for i := range n {
		ops = append(ops, db.WriteOp{Kind: db.OpInsert, Document: &db.Document{ID: fmt.Sprintf("d%05d", i), Data: map[string]any{
			"i": i, "code": fmt.Sprintf("c%05d", i), "pad": strings.Repeat(string(rune('a'+i%26)), 3500+i%700),
		}}})
		if len(ops) == cap(ops) {
			flush()
		}
	}
	if len(ops) > 0 {
		flush()
	}
	// One document over the result byte cap and near the client's 3 MiB
	// batch bound: exported alone in its page, imported on its own.
	if _, err := c.Insert(ctx, &db.Document{ID: fmt.Sprintf("d%05d", n/2) + "-huge", Data: map[string]any{
		"i": -1, "code": "huge", "pad": strings.Repeat("H", 3<<20+1000)}}); err != nil {
		t.Fatal(err)
	}
}

// stripHeader drops an export's header line (version, time).
func stripHeader(s string) string { return strings.SplitN(s, "\n", 2)[1] }

// TestClientExportImportLarge (AC-21, AC-22, F-038, F-016): a collection
// larger than the result byte cap exports through the client exactly like
// utils export, and client import (insert_documents, runs over 3 MiB
// split) reproduces it, unique index included, in every conflict mode.
func TestClientExportImportLarge(t *testing.T) {
	ctx := context.Background()
	const n = 1300 // about 5 MB, plus a 3 MiB document
	src := t.TempDir()
	seedLarge(t, src, n)
	url, stop := liveServer(t, src) // creates the database "default" too
	var viaClient bytes.Buffer
	if err := clientDo(t, url, func(cs *mcp.ClientSession) error {
		return exportfmt.Export(ctx, &viaClient, clientStore{cs: cs}, exportfmt.Scope{}, "v", time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	stop()
	var offline bytes.Buffer
	if err := exportOffline(ctx, &offline, db.Options{Dir: src}, exportfmt.Scope{}); err != nil {
		t.Fatal(err)
	}
	if offline.Len() < 7<<20 {
		t.Fatalf("export is %d bytes, want more than 7 MiB", offline.Len())
	}
	if a, b := stripHeader(viaClient.String()), stripHeader(offline.String()); a != b {
		al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
		for i := range min(len(al), len(bl)) {
			if al[i] != bl[i] {
				t.Errorf("first difference at line %d:\nclient %.300s\nutils  %.300s", i, al[i], bl[i])
				break
			}
		}
		t.Fatalf("client export differs from utils export (%d vs %d bytes)", len(a), len(b))
	}
	if !strings.Contains(offline.String(), `"unique":true`) {
		t.Fatal("the export does not record the unique index")
	}

	dst := t.TempDir()
	url2, stop2 := liveServer(t, dst)
	imp := func(mode exportfmt.OnConflict) (exportfmt.Stats, error) {
		var st exportfmt.Stats
		err := clientDo(t, url2, func(cs *mcp.ClientSession) error {
			target, err := importTarget(ctx, cs)
			if err != nil {
				return err
			}
			if _, ok := target.(exportfmt.BatchTarget); !ok {
				t.Fatal("the import target of a current server is not a batch target")
			}
			st, err = exportfmt.Import(ctx, strings.NewReader(offline.String()), target, mode)
			return err
		})
		return st, err
	}
	if st, err := imp(exportfmt.Fail); err != nil || st.Inserted != n+1 {
		t.Fatalf("import fail: %+v, %v", st, err)
	}
	if _, err := imp(exportfmt.Fail); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("second import fail: %v", err)
	}
	if st, err := imp(exportfmt.Skip); err != nil || st.Skipped != n+1 || st.Inserted != 0 {
		t.Errorf("import skip: %+v, %v", st, err)
	}
	if st, err := imp(exportfmt.Keep); err != nil || st.Skipped != n+1 || st.Inserted != 0 {
		t.Errorf("import keep: %+v, %v", st, err)
	}
	if st, err := imp(exportfmt.Replace); err != nil || st.Replaced != n+1 || st.Inserted != 0 {
		t.Errorf("import replace: %+v, %v", st, err)
	}
	stop2()
	var again bytes.Buffer
	if err := exportOffline(ctx, &again, db.Options{Dir: dst}, exportfmt.Scope{Database: "big"}); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	if err := exportOffline(ctx, &want, db.Options{Dir: src}, exportfmt.Scope{Database: "big"}); err != nil {
		t.Fatal(err)
	}
	if stripHeader(again.String()) != stripHeader(want.String()) {
		t.Error("client import did not reproduce the data (with its unique index)")
	}
}

// keepFixture writes an export of kb (seedKB plus collection extra) to a
// file, and a target directory where kb/notes exists with a newer schema,
// an extra index, a changed n1, and no n3, and kb/extra is missing.
func keepFixture(t *testing.T) (dump, dst string) {
	t.Helper()
	ctx := context.Background()
	src := t.TempDir()
	seedKB(t, src)
	e := openEngine(t, src)
	d, _ := e.Database("kb")
	if err := d.CreateCollection(ctx, "extra", &db.Schema{Fields: map[string]db.Field{"v": {Type: db.TypeNumber}}}); err != nil {
		t.Fatal(err)
	}
	x, _ := d.Collection("extra")
	if err := x.CreateIndex(ctx, "by_v", "v", db.Unique(true)); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Insert(ctx, &db.Document{ID: "x1", Data: map[string]any{"v": 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CreateDatabase(ctx, "second"); err != nil {
		t.Fatal(err)
	}
	closeEngine(t, e)
	var buf bytes.Buffer
	if err := exportOffline(ctx, &buf, db.Options{Dir: src}, exportfmt.Scope{}); err != nil {
		t.Fatal(err)
	}
	dump = filepath.Join(t.TempDir(), "seed.jsonl")
	if err := os.WriteFile(dump, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	dst = t.TempDir()
	seedKB(t, dst)
	e = openEngine(t, dst)
	d, _ = e.Database("kb")
	c, _ := d.Collection("notes")
	newer := &db.Schema{Fields: map[string]db.Field{"title": {Type: db.TypeString, Required: true}, "prio": {Type: db.TypeNumber}}}
	if err := c.UpdateSchema(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateIndex(ctx, "by_prio", "prio"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Update(ctx, "n1", map[string]any{"title": "WAL design v2", "local": true}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "n3"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ctx, &db.Document{ID: "n9", Data: map[string]any{"title": "local only"}}); err != nil {
		t.Fatal(err)
	}
	closeEngine(t, e)
	return dump, dst
}

// checkKeepResult checks dst after a keep import of keepFixture's dump.
func checkKeepResult(t *testing.T, dst string) {
	t.Helper()
	ctx := context.Background()
	e := openEngine(t, dst)
	defer closeEngine(t, e)
	d, err := e.Database("kb")
	if err != nil {
		t.Fatal(err)
	}
	c, _ := d.Collection("notes")
	newer := &db.Schema{Fields: map[string]db.Field{"title": {Type: db.TypeString, Required: true}, "prio": {Type: db.TypeNumber}}}
	if !reflect.DeepEqual(c.Schema(), newer) {
		t.Errorf("notes schema = %+v, want the newer one kept", c.Schema())
	}
	if got := c.Indexes(); len(got) != 2 || got[0].Name != "by_prio" || got[1].Name != "by_tags" {
		t.Errorf("notes indexes = %+v, want by_prio and by_tags", got)
	}
	n1, err := c.Get(ctx, "n1")
	if err != nil || n1.Data["title"] != "WAL design v2" || n1.Data["local"] != true {
		t.Errorf("n1 = %+v, %v; want the changed version kept", n1, err)
	}
	n3, err := c.Get(ctx, "n3")
	if err != nil || n3.Data["title"] != "More WAL" {
		t.Errorf("n3 = %+v, %v; want it inserted from the file", n3, err)
	}
	if _, err := c.Get(ctx, "n9"); err != nil {
		t.Errorf("n9 (only in the target): %v", err)
	}
	if c.Count() != 4 {
		t.Errorf("notes has %d documents, want 4", c.Count())
	}
	x, err := d.Collection("extra")
	if err != nil {
		t.Fatalf("missing collection extra was not created: %v", err)
	}
	if x.Schema() == nil || x.Count() != 1 {
		t.Errorf("extra: schema %+v, %d documents", x.Schema(), x.Count())
	}
	if ix := x.Indexes(); len(ix) != 1 || ix[0].Name != "by_v" || !ix[0].Unique {
		t.Errorf("extra indexes = %+v, want the unique by_v", ix)
	}
	if _, err := e.Database("second"); err != nil {
		t.Errorf("missing database second was not created: %v", err)
	}
}

// TestImportKeepCLI (AC-28): utils import and client import with
// --on-conflict keep create what is missing (database, collection with
// schema and indexes, documents) and leave everything that exists as it
// is, exiting 0; skip still fails on the different schema.
func TestImportKeepCLI(t *testing.T) {
	ctx := context.Background()
	t.Run("utils import", func(t *testing.T) {
		dump, dst := keepFixture(t)
		code, _, errOut := runCLI(t, ctx, "utils", "import", "-R", dst, "-i", dump, "--on-conflict", "skip")
		if code != 1 || !strings.Contains(errOut, "keep") {
			t.Errorf("skip over a different schema: exit %d, %q; want 1 with a hint naming keep", code, errOut)
		}
		code, _, errOut = runCLI(t, ctx, "utils", "import", "-R", dst, "-i", dump, "--on-conflict", "keep")
		if code != 0 {
			t.Fatalf("keep: exit %d, %s", code, errOut)
		}
		if !strings.Contains(errOut, "inserted") {
			t.Errorf("keep summary: %q", errOut)
		}
		checkKeepResult(t, dst)
		// A second keep changes nothing.
		code, _, errOut = runCLI(t, ctx, "utils", "import", "-R", dst, "-i", dump, "--on-conflict", "keep")
		if code != 0 {
			t.Fatalf("second keep: exit %d, %s", code, errOut)
		}
		checkKeepResult(t, dst)
	})
	t.Run("client import", func(t *testing.T) {
		dump, dst := keepFixture(t)
		url, stop := liveServer(t, dst)
		code, _, errOut := runCLI(t, ctx, "client", "import", "--url", url, "-i", dump, "--on-conflict", "keep")
		if code != 0 {
			t.Fatalf("keep: exit %d, %s", code, errOut)
		}
		code, _, errOut = runCLI(t, ctx, "client", "import", "--url", url, "-i", dump, "--on-conflict", "keep")
		if code != 0 {
			t.Fatalf("second keep: exit %d, %s", code, errOut)
		}
		stop()
		checkKeepResult(t, dst)
	})
}
