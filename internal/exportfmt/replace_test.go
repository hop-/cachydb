package exportfmt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

// failingWriter fails every write.
type failingWriter struct{}

var errWrite = errors.New("write failed")

func (failingWriter) Write([]byte) (int, error) { return 0, errWrite }

// TestImportReplaceUpdatesSchemaAndIndexes imports into an existing
// collection whose schema and indexes differ: with --on-conflict replace
// the schema is updated, an index of the same name on another field is
// replaced, missing indexes are added (extra ones are kept), and existing
// documents are replaced.
func TestImportReplaceUpdatesSchemaAndIndexes(t *testing.T) {
	ctx := context.Background()
	src := openEngine(t, t.TempDir())
	seedEngine(t, src)
	data := export(t, src, Scope{Database: "kb", Collection: "notes"})

	dst := openEngine(t, t.TempDir())
	d, err := dst.CreateDatabase(ctx, "kb")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(ctx, "notes", nil); err != nil {
		t.Fatal(err)
	}
	notes, _ := mustColl(t, dst, "kb", "notes")
	for _, ix := range [][2]string{{"by_type", "kind"}, {"by_extra", "extra"}} {
		if err := notes.CreateIndex(ctx, ix[0], ix[1]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := notes.Insert(ctx, &db.Document{ID: "a", Data: map[string]any{"title": "old", "kind": "x"}}); err != nil {
		t.Fatal(err)
	}

	st, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Replace)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if st.Replaced != 1 || st.Inserted != 1 || st.Collections != 1 {
		t.Errorf("stats = %+v, want 1 replaced, 1 inserted", st)
	}
	srcNotes, _ := mustColl(t, src, "kb", "notes")
	if !sameSchema(notes.Schema(), srcNotes.Schema()) {
		t.Errorf("schema = %+v, want %+v", notes.Schema(), srcNotes.Schema())
	}
	var got []string
	for _, ix := range notes.Indexes() {
		got = append(got, ix.Name+"="+ix.Field)
	}
	slices.Sort(got)
	if want := []string{"by_extra=extra", "by_tags=tags", "by_type=type"}; !slices.Equal(got, want) {
		t.Errorf("indexes = %v, want %v", got, want)
	}
	// Every imported document is as exported (the extra index does not
	// show up in documents, so compare documents through a fresh export
	// of src's documents).
	for _, id := range []string{"a", "b"} {
		want, err := srcNotes.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		have, err := notes.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if !sameJSON(t, DocMap(have), DocMap(want)) {
			t.Errorf("document %s = %v, want %v", id, have.Data, want.Data)
		}
	}
}

// TestImportReplaceSchemaViolatedByExistingDocs: the imported schema is
// applied with UpdateSchema, which fails when an existing document does
// not satisfy it; the import stops with that error and the collection
// keeps its schema.
func TestImportReplaceSchemaViolatedByExistingDocs(t *testing.T) {
	ctx := context.Background()
	src := openEngine(t, t.TempDir())
	seedEngine(t, src)
	data := export(t, src, Scope{Database: "kb", Collection: "notes"})

	dst := openEngine(t, t.TempDir())
	d, err := dst.CreateDatabase(ctx, "kb")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(ctx, "notes", nil); err != nil {
		t.Fatal(err)
	}
	notes, _ := mustColl(t, dst, "kb", "notes")
	if _, err := notes.Insert(ctx, &db.Document{ID: "z", Data: map[string]any{"note": "no title"}}); err != nil {
		t.Fatal(err)
	}

	st, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Replace)
	if !errors.Is(err, db.ErrSchemaValidation) || !strings.Contains(err.Error(), "updating the schema") {
		t.Fatalf("Import = %v, want a schema validation error while updating the schema", err)
	}
	if st.Lines != 2 || st.Collections != 0 {
		t.Errorf("stats = %+v, want only the header and database lines applied", st)
	}
	if s := notes.Schema(); s != nil {
		t.Errorf("schema changed to %+v after the failed update", s)
	}
}

// TestEngineAdapterMissing checks that the engine adapter reports missing
// databases and collections as db.ErrNotFound, and Lookup as "not there".
func TestEngineAdapterMissing(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, t.TempDir())
	if _, err := e.CreateDatabase(ctx, "kb"); err != nil {
		t.Fatal(err)
	}
	a := Engine{E: e}
	tests := []struct {
		name string
		call func(database string) error
	}{
		{"SchemaVersion", func(d string) error { _, err := a.SchemaVersion(ctx, d); return err }},
		{"Collections", func(d string) error { _, err := a.Collections(ctx, d); return err }},
		{"Describe", func(d string) error { _, _, err := a.Describe(ctx, d, "c"); return err }},
		{"Documents", func(d string) error { return a.Documents(ctx, d, "c", func(map[string]any) error { return nil }) }},
		{"CreateCollection", func(d string) error { return a.CreateCollection(ctx, d, "c", nil) }},
		{"UpdateSchema", func(d string) error { return a.UpdateSchema(ctx, d, "c", nil) }},
		{"CreateIndex", func(d string) error { return a.CreateIndex(ctx, d, "c", "i", "f") }},
		{"DropIndex", func(d string) error { return a.DropIndex(ctx, d, "c", "i") }},
		{"InsertDocument", func(d string) error { return a.InsertDocument(ctx, d, "c", map[string]any{"_id": "x"}) }},
		{"ReplaceDocument", func(d string) error { return a.ReplaceDocument(ctx, d, "c", map[string]any{"_id": "x"}) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call("nope"); !errors.Is(err, db.ErrNotFound) {
				t.Errorf("missing database: %v, want ErrNotFound", err)
			}
			if tt.name == "SchemaVersion" || tt.name == "Collections" || tt.name == "CreateCollection" {
				return // database-level calls
			}
			if err := tt.call("kb"); !errors.Is(err, db.ErrNotFound) {
				t.Errorf("missing collection: %v, want ErrNotFound", err)
			}
		})
	}
	if _, _, ok, err := a.Lookup(ctx, "kb", "c"); ok || err != nil {
		t.Errorf("Lookup of a missing collection = %v, %v", ok, err)
	}
	if _, _, ok, err := a.Lookup(ctx, "nope", "c"); ok || err != nil {
		t.Errorf("Lookup in a missing database = %v, %v", ok, err)
	}
}

// TestExportErrors checks Export's error paths: an invalid scope, a
// missing database or collection, and a failing writer.
func TestExportErrors(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, t.TempDir())
	seedEngine(t, e)
	now := time.Now()
	tests := []struct {
		name   string
		w      func() *bytes.Buffer
		scope  Scope
		wantIs error
	}{
		{name: "collection without database", scope: Scope{Collection: "notes"}},
		{name: "missing database", scope: Scope{Database: "nope"}, wantIs: db.ErrNotFound},
		{name: "missing collection", scope: Scope{Database: "kb", Collection: "nope"}, wantIs: db.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			err := Export(ctx, &b, Engine{E: e}, tt.scope, "v", now)
			if err == nil || (tt.wantIs != nil && !errors.Is(err, tt.wantIs)) {
				t.Fatalf("Export = %v, want an error (%v)", err, tt.wantIs)
			}
		})
	}
	t.Run("failing writer", func(t *testing.T) {
		if err := Export(ctx, failingWriter{}, Engine{E: e}, Scope{}, "v", now); !errors.Is(err, errWrite) {
			t.Fatalf("Export = %v, want the write error", err)
		}
	})
}

// TestImportLineErrors covers malformed lines of each type and a failing
// target call.
func TestImportLineErrors(t *testing.T) {
	ctx := context.Background()
	header := `{"type":"cachydb-export","version":1}`
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"bad header", `{"type":"cachydb-export","version":"x"}`, "invalid header"},
		{"second header", header + "\n" + header, "a second header"},
		{"bad database line", header + "\n" + `{"type":"database","name":5}`, "invalid database line"},
		{"bad database name", header + "\n" + `{"type":"database","name":"a/b"}`, `database "a/b"`},
		{"bad collection line", header + "\n" + `{"type":"collection","name":[]}`, "invalid collection line"},
		{"collection in a missing database", header + "\n" + `{"type":"collection","database":"nope","name":"c"}`, "collection nope/c"},
		{"bad index", header + "\n" + `{"type":"database","name":"d"}` + "\n" + `{"type":"collection","database":"d","name":"c","indexes":[{"name":"","field":"f"}]}`, `index ""`},
		{"bad document line", header + "\n" + `{"type":"document","doc":5}`, "invalid document line"},
		{"document without id", header + "\n" + `{"type":"document","doc":{"a":1}}`, "without a string _id"},
		{"document in a missing database", header + "\n" + `{"type":"document","database":"nope","collection":"c","doc":{"_id":"a"}}`, `document "a" in nope/c`},
		{"cancelled", header, "context canceled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := openEngine(t, t.TempDir())
			ctx := ctx
			if tt.name == "cancelled" {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			_, err := Import(ctx, strings.NewReader(tt.in), Engine{E: e}, Fail)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Import = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

// sameJSON reports whether a and b encode to the same JSON.
func sameJSON(t *testing.T, a, b any) bool {
	t.Helper()
	ja, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	jb, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(ja, jb)
}
