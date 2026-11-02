package exportfmt

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

func openEngine(t *testing.T, dir string) *db.Engine {
	t.Helper()
	e, err := db.Open(context.Background(), db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	return e
}

// seedEngine fills e with two databases, schemas, indexes, and documents
// of every JSON type.
func seedEngine(t *testing.T, e *db.Engine) {
	t.Helper()
	ctx := context.Background()
	kb, err := e.CreateDatabase(ctx, "kb")
	if err != nil {
		t.Fatal(err)
	}
	s := &db.Schema{Fields: map[string]db.Field{"title": {Type: db.TypeString, Required: true}}}
	if err := kb.CreateCollection(ctx, "notes", s); err != nil {
		t.Fatal(err)
	}
	notes, _ := kb.Collection("notes")
	for _, ix := range [][2]string{{"by_type", "type"}, {"by_tags", "tags"}} {
		if err := notes.CreateIndex(ctx, ix[0], ix[1]); err != nil {
			t.Fatal(err)
		}
	}
	docs := []*db.Document{
		{ID: "b", Data: map[string]any{"title": "B", "type": "note", "tags": []any{"x", 1.5, true, nil}, "meta": map[string]any{"z": 1, "a": "y"}}},
		{ID: "a", Data: map[string]any{"title": "A — ünïcode", "n": 12345678901.0}},
	}
	for _, d := range docs {
		if _, err := notes.Insert(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	if err := kb.CreateCollection(ctx, "empty", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CreateDatabase(ctx, "aa"); err != nil {
		t.Fatal(err)
	}
}

func export(t *testing.T, e *db.Engine, scope Scope) string {
	t.Helper()
	var b bytes.Buffer
	if err := Export(context.Background(), &b, Engine{E: e}, scope, "v-test", time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Export: %v", err)
	}
	return b.String()
}

func TestExportFormat(t *testing.T) {
	e := openEngine(t, t.TempDir())
	seedEngine(t, e)
	got := export(t, e, Scope{Database: "kb", Collection: "notes"})
	want := `{"type":"cachydb-export","version":1,"cachydb":"v-test","created":"2026-11-02T00:00:00Z"}
{"type":"database","name":"kb","schema_version":1}
{"type":"collection","database":"kb","name":"notes","schema":{"fields":{"title":{"type":"string","required":true}}},"indexes":[{"name":"by_tags","field":"tags"},{"name":"by_type","field":"type"}]}
{"type":"document","database":"kb","collection":"notes","doc":{"_id":"a","n":12345678901,"title":"A — ünïcode"}}
{"type":"document","database":"kb","collection":"notes","doc":{"_id":"b","meta":{"a":"y","z":1},"tags":["x",1.5,true,null],"title":"B","type":"note"}}
`
	if got != want {
		t.Errorf("export =\n%s\nwant\n%s", got, want)
	}
	all := export(t, e, Scope{})
	lines := strings.Split(strings.TrimSpace(all), "\n")
	if len(lines) != 7 || !strings.Contains(lines[1], `"name":"aa"`) || !strings.Contains(lines[3], `"name":"empty","schema":null,"indexes":[]`) {
		t.Errorf("full export order wrong:\n%s", all)
	}
	if err := Export(context.Background(), &bytes.Buffer{}, Engine{E: e}, Scope{Collection: "notes"}, "v", time.Now()); err == nil {
		t.Error("collection scope without a database accepted")
	}
}

// TestRoundTrip: export, import into an empty directory, export again:
// identical apart from the header's created.
func TestRoundTrip(t *testing.T) {
	src := openEngine(t, t.TempDir())
	seedEngine(t, src)
	first := export(t, src, Scope{})

	dst := openEngine(t, t.TempDir())
	st, err := Import(context.Background(), strings.NewReader(first), Engine{E: dst}, Fail)
	if err != nil {
		t.Fatal(err)
	}
	if st.Lines != 7 || st.Inserted != 2 || st.Collections != 2 || st.Databases != 2 {
		t.Errorf("stats = %+v", st)
	}
	if second := export(t, dst, Scope{}); second != first {
		t.Errorf("round trip differs:\n%s\nvs\n%s", second, first)
	}
}

func TestImportConflicts(t *testing.T) {
	ctx := context.Background()
	src := openEngine(t, t.TempDir())
	seedEngine(t, src)
	data := export(t, src, Scope{Database: "kb", Collection: "notes"})

	t.Run("fail", func(t *testing.T) {
		dst := openEngine(t, t.TempDir())
		if _, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Fail); err != nil {
			t.Fatal(err)
		}
		st, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Fail)
		if err == nil || !strings.Contains(err.Error(), `line 4 (3 lines applied)`) || !strings.Contains(err.Error(), `document "a"`) {
			t.Fatalf("second import = %v", err)
		}
		if st.Lines != 3 {
			t.Errorf("lines applied = %d", st.Lines)
		}
	})
	t.Run("skip and replace", func(t *testing.T) {
		dst := openEngine(t, t.TempDir())
		if _, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Fail); err != nil {
			t.Fatal(err)
		}
		notes, _ := mustColl(t, dst, "kb", "notes")
		if _, err := notes.Update(ctx, "a", map[string]any{"title": "changed"}); err != nil {
			t.Fatal(err)
		}
		st, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Skip)
		if err != nil || st.Skipped != 2 {
			t.Fatalf("skip: %+v, %v", st, err)
		}
		if d, _ := notes.Get(ctx, "a"); d.Data["title"] != "changed" {
			t.Error("skip replaced a document")
		}
		st, err = Import(ctx, strings.NewReader(data), Engine{E: dst}, Replace)
		if err != nil || st.Replaced != 2 {
			t.Fatalf("replace: %+v, %v", st, err)
		}
		if d, _ := notes.Get(ctx, "a"); d.Data["title"] != "A — ünïcode" {
			t.Error("replace did not replace")
		}
	})
	t.Run("different schema and indexes", func(t *testing.T) {
		dst := openEngine(t, t.TempDir())
		d, _ := dst.CreateDatabase(ctx, "kb")
		if err := d.CreateCollection(ctx, "notes", nil); err != nil {
			t.Fatal(err)
		}
		for _, mode := range []OnConflict{Fail, Skip} {
			if _, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, mode); err == nil || !strings.Contains(err.Error(), "different schema") {
				t.Errorf("%s: %v", mode, err)
			}
		}
		if _, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Replace); err != nil {
			t.Fatal(err)
		}
		if again := export(t, dst, Scope{Database: "kb", Collection: "notes"}); again != data {
			t.Errorf("after replace import:\n%s", again)
		}
	})
	t.Run("bad input", func(t *testing.T) {
		dst := openEngine(t, t.TempDir())
		for name, in := range map[string]string{
			"empty":       "",
			"no header":   `{"type":"database","name":"x","schema_version":1}`,
			"new version": `{"type":"cachydb-export","version":2}`,
			"bad json":    "{",
			"unknown":     `{"type":"cachydb-export","version":1}` + "\n" + `{"type":"what"}`,
		} {
			if _, err := Import(ctx, strings.NewReader(in), Engine{E: dst}, Fail); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}
		if _, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, OnConflict("x")); err == nil {
			t.Error("bad mode accepted")
		}
	})
}

func mustColl(t *testing.T, e *db.Engine, database, collection string) (*db.Collection, error) {
	t.Helper()
	d, err := e.Database(database)
	if err != nil {
		t.Fatal(err)
	}
	c, err := d.Collection(collection)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Fatal(err)
	}
	return c, err
}
