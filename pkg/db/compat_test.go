package db_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/exportfmt"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/hop-/cachydb/test/compat"
)

// TestCompatFixtures opens the data directories written by earlier
// releases (testdata/compat/<version>/data, see the README there) and
// checks that this build reads exactly the content the release wrote
// (expected.jsonl), that the indexes answer like a scan (the index
// snapshots of v0.0.4 are format v1 and are rebuilt), and that the
// upgraded directory takes writes and reopens.
func TestCompatFixtures(t *testing.T) {
	versions, err := filepath.Glob(filepath.Join("testdata", "compat", "v*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) < 2 {
		t.Fatalf("found %d fixtures, want v0.0.4 and later", len(versions))
	}
	for _, fixture := range versions {
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			ctx := context.Background()
			want, err := os.ReadFile(filepath.Join(fixture, "expected.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			// Opening writes (recovery, snapshots, the lock), so work on
			// a copy.
			dir := filepath.Join(t.TempDir(), "data")
			if err := compat.CopyDir(filepath.Join(fixture, "data"), dir); err != nil {
				t.Fatal(err)
			}
			opts := db.Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			e, err := db.Open(ctx, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = e.Close(ctx) // the test already failed
				}
			}()
			if err := compat.CompareExport(want, exportAll(t, e)); err != nil {
				t.Fatalf("content of the %s fixture: %v", filepath.Base(fixture), err)
			}
			checkIndexesLikeScan(t, e, want)

			// Writes on the upgraded directory survive a reopen.
			notes := collection(t, e, "kb", "notes")
			if _, err := notes.Insert(ctx, &db.Document{ID: "after-upgrade", Data: map[string]any{"title": "new", "tags": []any{"wal", "new"}}}); err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if _, err := notes.Update(ctx, "n02", map[string]any{"prio": 4}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if err := e.Close(ctx); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if e, err = db.Open(ctx, opts); err != nil {
				t.Fatalf("reopen: %v", err)
			}
			notes = collection(t, e, "kb", "notes")
			doc, err := notes.Get(ctx, "after-upgrade")
			if err != nil || doc.Data["title"] != "new" {
				t.Fatalf("Get after reopen: %v, %v", doc, err)
			}
			if doc, err := notes.Get(ctx, "n02"); err != nil || doc.Data["prio"] != 4.0 {
				t.Fatalf("updated n02 after reopen: %v, %v", doc, err)
			}
			got, err := notes.Find(ctx, &db.Query{Filters: []db.QueryFilter{{Field: "tags", Operator: "has", Value: "new"}}})
			if err != nil || len(got) != 1 || got[0].ID != "after-upgrade" {
				t.Fatalf("index lookup after reopen: %v, %v", got, err)
			}
			checkIndexesLikeScan(t, e, nil)
			closed = true
			if err := e.Close(ctx); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func exportAll(t *testing.T, e *db.Engine) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := exportfmt.Export(context.Background(), &b, exportfmt.Engine{E: e}, exportfmt.Scope{}, "test", time.Now()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	return b.Bytes()
}

func collection(t *testing.T, e *db.Engine, database, name string) *db.Collection {
	t.Helper()
	d, err := e.Database(database)
	if err != nil {
		t.Fatal(err)
	}
	c, err := d.Collection(name)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// checkIndexesLikeScan checks, for every index of every collection, that
// index-backed queries, counts and distinct values equal what a scan of
// the documents gives. With expected set, the stored values come from it
// (so the fixture's content, not the engine's, decides).
func checkIndexesLikeScan(t *testing.T, e *db.Engine, expected []byte) {
	t.Helper()
	ctx := context.Background()
	for _, dbName := range e.ListDatabases() {
		d, err := e.Database(dbName)
		if err != nil {
			t.Fatal(err)
		}
		for _, collName := range d.ListCollections() {
			c := collection(t, e, dbName, collName)
			all, err := c.Find(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if expected != nil {
				all = expectedDocs(t, expected, dbName, collName)
			}
			for _, ix := range c.Indexes() {
				// Every value the field holds (array elements included).
				var vals []any
				for _, doc := range all {
					v, ok := doc.Data[ix.Field]
					if !ok {
						continue
					}
					if arr, isArr := v.([]any); isArr {
						vals = append(vals, arr...)
					} else {
						vals = append(vals, v)
					}
				}
				for _, v := range vals {
					for _, op := range []string{"eq", "has"} {
						f := db.QueryFilter{Field: ix.Field, Operator: op, Value: v}
						var want []string
						for _, doc := range all {
							if matches(doc.Data[ix.Field], op, v) {
								want = append(want, doc.ID)
							}
						}
						got, err := c.Find(ctx, &db.Query{Filters: []db.QueryFilter{f}})
						if err != nil {
							t.Fatal(err)
						}
						var gotIDs []string
						for _, doc := range got {
							gotIDs = append(gotIDs, doc.ID)
						}
						if !slices.Equal(gotIDs, want) {
							t.Errorf("%s/%s index %s: %s %v = %v, want %v", dbName, collName, ix.Name, op, v, gotIDs, want)
						}
						if n, err := c.CountDocuments(ctx, &db.Query{Filters: []db.QueryFilter{f}}); err != nil || n != len(want) {
							t.Errorf("%s/%s index %s: count %s %v = %d (%v), want %d", dbName, collName, ix.Name, op, v, n, err, len(want))
						}
					}
				}
			}
		}
	}
}

// matches is eq (whole value) or has (array element) for decoded JSON.
func matches(field any, op string, v any) bool {
	if op == "eq" {
		return reflect.DeepEqual(field, v)
	}
	arr, ok := field.([]any)
	if !ok {
		return false
	}
	for _, el := range arr {
		if reflect.DeepEqual(el, v) {
			return true
		}
	}
	return false
}

// expectedDocs returns the documents of one collection in expected.jsonl,
// sorted by id.
func expectedDocs(t *testing.T, expected []byte, database, collection string) []*db.Document {
	t.Helper()
	var out []*db.Document
	for _, line := range bytes.Split(bytes.TrimSpace(expected), []byte("\n")) {
		var l exportfmt.DocumentLine
		if err := json.Unmarshal(line, &l); err != nil {
			t.Fatal(err)
		}
		if l.Type != exportfmt.TypeDocument || l.Database != database || l.Collection != collection {
			continue
		}
		id, _ := l.Doc["_id"].(string)
		delete(l.Doc, "_id")
		out = append(out, &db.Document{ID: id, Data: l.Doc})
	}
	slices.SortFunc(out, func(a, b *db.Document) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	return out
}
