package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hop-/cachydb/internal/exportfmt"
	"github.com/hop-/cachydb/pkg/db"
)

// seedKB creates database kb with collection notes (schema, index, three
// documents) in dir.
func seedKB(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	e := openEngine(t, dir)
	defer closeEngine(t, e)
	d, err := e.CreateDatabase(ctx, "kb")
	if err != nil {
		t.Fatal(err)
	}
	s := &db.Schema{Fields: map[string]db.Field{"title": {Type: db.TypeString, Required: true}}}
	if err := d.CreateCollection(ctx, "notes", s); err != nil {
		t.Fatal(err)
	}
	c, _ := d.Collection("notes")
	if err := c.CreateIndex(ctx, "by_tags", "tags"); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []*db.Document{
		{ID: "n1", Data: map[string]any{"title": "WAL design", "tags": []any{"wal"}, "prio": 2}},
		{ID: "n2", Data: map[string]any{"title": "Compaction", "tags": []any{"storage"}, "prio": 1}},
		{ID: "n3", Data: map[string]any{"title": "More WAL", "tags": []any{"wal", "storage"}, "prio": 3}},
	} {
		if _, err := c.Insert(ctx, doc); err != nil {
			t.Fatal(err)
		}
	}
}

// createdRE matches the header's created field, which differs per export.
var createdRE = regexp.MustCompile(`"created":"[^"]*"`)

func TestExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	seedKB(t, src)

	file := filepath.Join(t.TempDir(), "kb.jsonl")
	if err := withOutputFile(file, io.Discard, func(w io.Writer) error {
		return exportOffline(ctx, w, db.Options{Dir: src}, exportfmt.Scope{})
	}); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	var diag bytes.Buffer
	if err := withInputFile(file, nil, func(r io.Reader) error {
		return importOffline(ctx, r, &diag, db.Options{Dir: dst}, exportfmt.Fail)
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(diag.String(), "3 documents inserted") {
		t.Errorf("import summary = %q", diag.String())
	}
	var second bytes.Buffer
	if err := exportOffline(ctx, &second, db.Options{Dir: dst}, exportfmt.Scope{}); err != nil {
		t.Fatal(err)
	}
	if a, b := createdRE.ReplaceAllString(string(first), ""), createdRE.ReplaceAllString(second.String(), ""); a != b {
		t.Errorf("round trip differs:\n%s\nvs\n%s", a, b)
	}

	// On-conflict modes against the now populated target.
	err = importOffline(ctx, bytes.NewReader(first), io.Discard, db.Options{Dir: dst}, exportfmt.Fail)
	if err == nil || !strings.Contains(err.Error(), "lines applied") {
		t.Errorf("second import with fail: %v", err)
	}
	diag.Reset()
	if err := importOffline(ctx, bytes.NewReader(first), &diag, db.Options{Dir: dst}, exportfmt.Skip); err != nil || !strings.Contains(diag.String(), "3 skipped") {
		t.Errorf("skip: %v, %q", err, diag.String())
	}
	diag.Reset()
	if err := importOffline(ctx, bytes.NewReader(first), &diag, db.Options{Dir: dst}, exportfmt.Replace); err != nil || !strings.Contains(diag.String(), "3 replaced") {
		t.Errorf("replace: %v, %q", err, diag.String())
	}
}

func TestFindAndGet(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	seedKB(t, dir)
	opts := db.Options{Dir: dir}

	var out bytes.Buffer
	q := &db.Query{Filters: []db.QueryFilter{{Field: "tags", Operator: "has", Value: "wal"}}, Sort: []db.SortKey{{Field: "prio", Order: "desc"}}, Fields: []string{"title"}}
	if err := findOffline(ctx, &out, opts, "kb", "notes", q, "jsonl"); err != nil {
		t.Fatal(err)
	}
	want := `{"_id":"n3","title":"More WAL"}` + "\n" + `{"_id":"n1","title":"WAL design"}` + "\n"
	if out.String() != want {
		t.Errorf("find jsonl =\n%s\nwant\n%s", out.String(), want)
	}
	out.Reset()
	if err := findOffline(ctx, &out, opts, "kb", "notes", &db.Query{Limit: 1}, "json"); err != nil {
		t.Fatal(err)
	}
	var arr []map[string]any
	if err := json.Unmarshal(out.Bytes(), &arr); err != nil || len(arr) != 1 || arr[0]["_id"] != "n1" {
		t.Errorf("find json = %s (%v)", out.String(), err)
	}

	out.Reset()
	if err := getOffline(ctx, &out, opts, "kb", "notes", "n2"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), `{"_id":"n2"`) {
		t.Errorf("get = %s", out.String())
	}
	if err := getOffline(ctx, io.Discard, opts, "kb", "notes", "nope"); !errors.Is(err, db.ErrNotFound) {
		t.Errorf("get missing: %v", err)
	}
	if err := findOffline(ctx, io.Discard, opts, "kb", "nope", nil, "jsonl"); !errors.Is(err, db.ErrNotFound) {
		t.Errorf("find in a missing collection: %v", err)
	}
}

func TestFindCommandQueryFlag(t *testing.T) {
	dir := t.TempDir()
	seedKB(t, dir)
	for _, tt := range []struct {
		name    string
		args    []string
		want    string
		wantErr string
	}{
		{"query", []string{"utils", "find", "-R", dir, "-d", "kb", "-c", "notes", "--query", `{"filters":[{"field":"title","operator":"contains","value":"compaction"}],"fields":["prio"]}`},
			`{"_id":"n2","prio":1}` + "\n", ""},
		{"unknown query key", []string{"utils", "find", "-R", dir, "-d", "kb", "-c", "notes", "--query", `{"filter":[]}`}, "", "invalid --query"},
		{"missing collection flag", []string{"utils", "find", "-R", dir, "-d", "kb"}, "", "required"},
		{"export -c without -d", []string{"utils", "export", "-R", dir, "-c", "notes"}, "", "requires -d"},
		{"import without -i", []string{"utils", "import", "-R", dir}, "", "required"},
		{"import bad mode", []string{"utils", "import", "-R", dir, "-i", "x", "--on-conflict", "merge"}, "", "invalid --on-conflict"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			rootCmd.SetOut(&out)
			rootCmd.SetErr(io.Discard)
			rootCmd.SetArgs(tt.args)
			t.Cleanup(func() {
				rootCmd.SetOut(nil)
				rootCmd.SetErr(nil)
				rootCmd.SetArgs(nil)
				findQuery, findDatabase, findCollection, exportCollection, exportDatabase, importInput, importOnConflict = "", "", "", "", "", "", "fail"
			})
			err := rootCmd.ExecuteContext(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if out.String() != tt.want {
				t.Errorf("output = %q, want %q", out.String(), tt.want)
			}
		})
	}
}

// TestDataCommandsSuggestClientWhileServerRuns: the data commands fail on
// a locked directory and point to cachydb client.
func TestDataCommandsSuggestClientWhileServerRuns(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openEngine(t, dir)
	defer closeEngine(t, e)
	opts := db.Options{Dir: dir}
	for name, run := range map[string]func() error{
		"find":   func() error { return findOffline(ctx, io.Discard, opts, "kb", "notes", nil, "jsonl") },
		"get":    func() error { return getOffline(ctx, io.Discard, opts, "kb", "notes", "x") },
		"export": func() error { return exportOffline(ctx, io.Discard, opts, exportfmt.Scope{}) },
		"import": func() error { return importOffline(ctx, strings.NewReader(""), io.Discard, opts, exportfmt.Fail) },
	} {
		t.Run(name, func(t *testing.T) {
			err := run()
			if !errors.Is(err, db.ErrDataDirInUse) || !strings.Contains(err.Error(), "cachydb client") {
				t.Fatalf("err = %v, want ErrDataDirInUse with a cachydb client hint", err)
			}
			if name == "find" && !strings.Contains(err.Error(), `cachydb client call find_documents '{"database":"kb","collection":"notes"}'`) {
				t.Errorf("find hint = %v", err)
			}
		})
	}
}
