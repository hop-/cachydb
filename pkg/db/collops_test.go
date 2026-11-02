package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// Tests of the plan-004 collection operations: describe accessors,
// UpdateSchema (WAL op 9), RenameCollection (WAL op 10), and
// CopyCollection.

func TestDescribeAccessors(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Fields: map[string]Field{"name": {Type: TypeString, Required: true}}}
	e, c := newTestColl(t, nil, s)
	for _, ix := range [][2]string{{"by_z", "z"}, {"by_a", "a"}} {
		if err := c.CreateIndex(ctx, ix[0], ix[1]); err != nil {
			t.Fatal(err)
		}
	}
	got := c.Schema()
	if !reflect.DeepEqual(got, s) {
		t.Errorf("Schema() = %v, want %v", got, s)
	}
	got.Fields["name"] = Field{Type: TypeNumber}
	if c.Schema().Fields["name"].Type != TypeString {
		t.Error("Schema() shares memory with the collection")
	}
	if want := []IndexInfo{{"by_a", "a"}, {"by_z", "z"}}; !reflect.DeepEqual(c.Indexes(), want) {
		t.Errorf("Indexes() = %v, want %v", c.Indexes(), want)
	}
	d := mustDB(t, e, "app")
	if err := d.CreateCollection(ctx, "free", nil); err != nil {
		t.Fatal(err)
	}
	free := mustColl(t, d, "free")
	if free.Schema() != nil || len(free.Indexes()) != 0 {
		t.Errorf("schemaless collection: %v, %v", free.Schema(), free.Indexes())
	}
}

func TestUpdateSchema(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	for i := range 12 {
		mustInsert(t, c, fmt.Sprintf("d%02d", i), map[string]any{"name": "x", "n": i})
	}
	mustInsert(t, c, "ok", map[string]any{"name": "y", "n": 1, "title": "t"})

	strict := &Schema{Fields: map[string]Field{"title": {Type: TypeString, Required: true}, "n": {Type: TypeNumber}}}
	next := e.wal.NextOffset()
	err := c.UpdateSchema(ctx, strict)
	if !errors.Is(err, ErrSchemaValidation) {
		t.Fatalf("UpdateSchema with failing documents: %v", err)
	}
	for _, want := range []string{"12 existing document(s)", "d00 (required field missing: title)", "d09", "and 2 more"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "d10") || strings.Contains(err.Error(), "ok (") {
		t.Errorf("error lists more than 10 or a valid document: %v", err)
	}
	if e.wal.NextOffset() != next || c.Schema() != nil {
		t.Fatal("a rejected UpdateSchema changed something")
	}
	if err := c.UpdateSchema(ctx, &Schema{}); !errors.Is(err, ErrSchemaValidation) {
		t.Errorf("invalid schema: %v", err)
	}

	loose := &Schema{Fields: map[string]Field{"name": {Type: TypeString, Required: true}, "n": {Type: TypeNumber}}}
	if err := c.UpdateSchema(ctx, loose); err != nil {
		t.Fatal(err)
	}
	if r := walRecord(t, e, next); r.Op.String() != "update_schema" {
		t.Errorf("WAL record = %s, want update_schema", r.Op)
	}
	if _, err := c.Insert(ctx, &Document{ID: "bad", Data: map[string]any{"n": 1}}); !errors.Is(err, ErrSchemaValidation) {
		t.Errorf("insert after UpdateSchema not validated by the new schema: %v", err)
	}
	if !reflect.DeepEqual(readCollMetaFile(t, filepath.Join(c.dir, collMetaFile)).Schema, loose) {
		t.Error("coll.meta does not hold the new schema")
	}

	// Survives a clean reopen and a crash (replay of op 9).
	e = reopen(t, e)
	c = mustColl(t, mustDB(t, e, "app"), "c")
	if !reflect.DeepEqual(c.Schema(), loose) {
		t.Errorf("schema after reopen = %v", c.Schema())
	}
	if err := c.UpdateSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}
	e.abandon()
	e = openEngine(t, crashOpts(opts.Dir))
	c = mustColl(t, mustDB(t, e, "app"), "c")
	if c.Schema() != nil {
		t.Errorf("schema after removal and a crash = %v, want nil", c.Schema())
	}
	mustInsert(t, c, "free", map[string]any{"n": "any"})
}

// TestUpdateSchemaCrash: a crash after the update_schema record and before
// the coll.meta write is completed by replay.
func TestUpdateSchemaCrash(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	crashAt(t, &opts, hookMetaWrite)
	e := openEngine(t, crashOpts(opts.Dir))
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, mustColl(t, d, "c"), "a", map[string]any{"name": "x"})
	e = reopen(t, e)
	_ = e.Close(ctx)

	e = openEngine(t, opts) // armed
	c := mustColl(t, mustDB(t, e, "app"), "c")
	s := &Schema{Fields: map[string]Field{"name": {Type: TypeString, Required: true}}}
	if err := c.UpdateSchema(ctx, s); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("UpdateSchema = %v, want a simulated crash", err)
	}
	e.abandon()
	if m := readCollMetaFile(t, filepath.Join(c.dir, collMetaFile)); m.Schema != nil {
		t.Fatalf("coll.meta changed before the crash point: %v", m.Schema)
	}

	e = openEngine(t, crashOpts(opts.Dir))
	c = mustColl(t, mustDB(t, e, "app"), "c")
	if !reflect.DeepEqual(c.Schema(), s) {
		t.Errorf("schema after recovery = %v, want %v", c.Schema(), s)
	}
	if m := readCollMetaFile(t, filepath.Join(c.dir, collMetaFile)); !reflect.DeepEqual(m.Schema, s) {
		t.Errorf("coll.meta after recovery = %v", m.Schema)
	}
}

// renameBase builds database app with collection "old" (schema, an index,
// documents in data segments and in the memtable) and, if history, an
// earlier collection "new" that was dropped. It returns the model after
// renaming old to new.
func renameBase(t *testing.T, e *Engine, history bool) model {
	t.Helper()
	ctx := context.Background()
	d := mustCreateDB(t, e, "app")
	if history {
		if err := d.CreateCollection(ctx, "new", nil); err != nil {
			t.Fatal(err)
		}
		mustInsert(t, mustColl(t, d, "new"), "ghost", map[string]any{"g": "boo"})
		if err := d.DropCollection(ctx, "new"); err != nil {
			t.Fatal(err)
		}
	}
	s := &Schema{Fields: map[string]Field{"g": {Type: TypeString}}}
	if err := d.CreateCollection(ctx, "old", s); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "old")
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(ctx, "other", nil); err != nil {
		t.Fatal(err)
	}
	mc := &modelColl{schema: s, indexes: map[string]string{"by_g": "g"}, docs: map[string]map[string]any{}}
	for i := range 6 {
		id := fmt.Sprintf("d%d", i)
		mustInsert(t, c, id, map[string]any{"g": fmt.Sprint(i % 2)})
		mc.docs[id] = map[string]any{"g": fmt.Sprint(i % 2)}
	}
	mustFlush(t, c)
	if err := c.Delete(ctx, "d0"); err != nil {
		t.Fatal(err)
	}
	delete(mc.docs, "d0")
	mustInsert(t, c, "late", map[string]any{"g": "1"}) // memtable only
	mc.docs["late"] = map[string]any{"g": "1"}
	mustInsert(t, mustColl(t, d, "other"), "o", map[string]any{"x": 1.0})
	return model{"app": {"new": mc, "other": {indexes: map[string]string{}, docs: map[string]map[string]any{"o": {"x": 1.0}}}}}
}

func TestRenameCollection(t *testing.T) {
	ctx := context.Background()
	for _, history := range []bool{false, true} {
		t.Run(fmt.Sprintf("history=%v", history), func(t *testing.T) {
			opts := crashOpts(t.TempDir())
			e := openEngine(t, opts)
			want := renameBase(t, e, history)
			d := mustDB(t, e, "app")
			oldColl := mustColl(t, d, "old")

			if err := d.RenameCollection(ctx, "old", "other"); !errors.Is(err, ErrExists) {
				t.Errorf("rename onto an existing collection: %v", err)
			}
			if err := d.RenameCollection(ctx, "nope", "x"); !errors.Is(err, ErrNotFound) {
				t.Errorf("rename of a missing collection: %v", err)
			}
			if err := d.RenameCollection(ctx, "old", "bad/name"); !errors.Is(err, ErrInvalidName) {
				t.Errorf("rename to an invalid name: %v", err)
			}
			next := e.wal.NextOffset()
			if err := d.RenameCollection(ctx, "old", "new"); err != nil {
				t.Fatal(err)
			}
			if r := walRecord(t, e, next); r.Op.String() != "rename_coll" || r.Coll != "old" {
				t.Errorf("WAL record = %s %s, want rename_coll old", r.Op, r.Coll)
			}
			if _, err := oldColl.Get(ctx, "d1"); !errors.Is(err, ErrNotFound) {
				t.Errorf("old handle after rename: %v", err)
			}
			if _, err := d.Collection("old"); !errors.Is(err, ErrNotFound) {
				t.Errorf("Collection(old) after rename: %v", err)
			}
			if got := d.ListCollections(); !slices.Equal(got, []string{"new", "other"}) {
				t.Errorf("collections = %v", got)
			}
			if fileExistsT(t, filepath.Join(opts.Dir, "app", "old")) {
				t.Error("old directory still exists")
			}
			if m := readCollMetaFile(t, filepath.Join(opts.Dir, "app", "new", collMetaFile)); m.Name != "new" {
				t.Errorf("coll.meta name = %q", m.Name)
			}
			checkModel(t, e, want)

			// The renamed collection keeps working: writes, flush, index.
			c := mustColl(t, d, "new")
			mustInsert(t, c, "after", map[string]any{"g": "0"})
			want["app"]["new"].docs["after"] = map[string]any{"g": "0"}
			if _, err := c.Update(ctx, "d1", map[string]any{"g": "2"}); err != nil {
				t.Fatal(err)
			}
			want["app"]["new"].docs["d1"] = map[string]any{"g": "2"}
			mustFlush(t, c)
			if docs, examined, err := c.find(ctx, &Query{Filters: []QueryFilter{{"g", "eq", "0"}}}); err != nil || examined != 3 || len(docs) != 3 {
				t.Errorf("indexed find on the renamed collection = %v after %d, %v", docIDs(docs), examined, err)
			}
			mustInsert(t, c, "unflushed", map[string]any{"g": "3"})
			want["app"]["new"].docs["unflushed"] = map[string]any{"g": "3"}
			checkModel(t, e, want)

			// Survives a crash (full replay, with and without the
			// metadata checkpoint) and a clean reopen.
			e.abandon()
			e = openEngine(t, crashOpts(opts.Dir))
			checkModel(t, e, want)
			e.abandon()
			mustRemove(t, filepath.Join(opts.Dir, metaCheckpointFileName))
			e = openEngine(t, crashOpts(opts.Dir))
			checkModel(t, e, want)
			e = reopen(t, e)
			checkModel(t, e, want)
			if entries, _ := os.ReadDir(filepath.Join(opts.Dir, "app")); len(entries) != 3 {
				t.Errorf("app directory holds %d entries, want new, other, db.meta", len(entries))
			}
		})
	}
}

// TestRenameCrash: a crash after the rename_coll record (before the
// directory rename) and one between the directory rename and the
// coll.meta write are both completed by replay.
func TestRenameCrash(t *testing.T) {
	ctx := context.Background()
	for _, p := range []hookPoint{hookMetaWrite, hookRenameDir} {
		for _, history := range []bool{false, true} {
			for _, metacp := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/history=%v/metacp=%v", p, history, metacp), func(t *testing.T) {
					opts := crashOpts(t.TempDir())
					e := openEngine(t, opts)
					want := renameBase(t, e, history)
					e.abandon()

					armed := crashOpts(opts.Dir)
					crashAt(t, &armed, p)
					e = openEngine(t, armed)
					if err := mustDB(t, e, "app").RenameCollection(ctx, "old", "new"); !errors.Is(err, errSimulatedCrash) {
						t.Fatalf("RenameCollection = %v, want a simulated crash", err)
					}
					e.abandon()
					if !metacp {
						mustRemove(t, filepath.Join(opts.Dir, metaCheckpointFileName))
					}
					oldDir := filepath.Join(opts.Dir, "app", "old")
					newDir := filepath.Join(opts.Dir, "app", "new")
					if moved := !fileExistsT(t, oldDir) && fileExistsT(t, newDir); moved != (p == hookRenameDir) {
						t.Fatalf("directory renamed = %v at %s", moved, p)
					}

					e = openEngine(t, crashOpts(opts.Dir))
					checkModel(t, e, want)
					if m := readCollMetaFile(t, filepath.Join(newDir, collMetaFile)); m.Name != "new" {
						t.Errorf("coll.meta name after recovery = %q", m.Name)
					}
					c := mustColl(t, mustDB(t, e, "app"), "new")
					mustInsert(t, c, "post", map[string]any{"g": "1"})
					want["app"]["new"].docs["post"] = map[string]any{"g": "1"}
					e = reopen(t, e)
					checkModel(t, e, want)
				})
			}
		}
	}
}

func TestCopyCollection(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, testOptions(t.TempDir()))
	d := mustCreateDB(t, e, "app")
	s := &Schema{Fields: map[string]Field{"g": {Type: TypeString, Required: true}}}
	if err := d.CreateCollection(ctx, "src", s); err != nil {
		t.Fatal(err)
	}
	src := mustColl(t, d, "src")
	if err := src.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		mustInsert(t, src, fmt.Sprintf("d%03d", i), map[string]any{"g": fmt.Sprint(i % 3), "tags": []any{"x", i}})
	}
	mustFlush(t, src)
	if err := src.Delete(ctx, "d000"); err != nil {
		t.Fatal(err)
	}

	other := mustCreateDB(t, e, "other")
	for _, tt := range []struct {
		name   string
		target *Database
		dbName string
	}{{"same database", nil, "app"}, {"other database", other, "other"}} {
		t.Run(tt.name, func(t *testing.T) {
			n, err := d.CopyCollection(ctx, "src", "dst", tt.target)
			if err != nil || n != 99 {
				t.Fatalf("CopyCollection = %d, %v; want 99", n, err)
			}
			dst := mustColl(t, mustDB(t, e, tt.dbName), "dst")
			if !reflect.DeepEqual(dst.Schema(), s) || !reflect.DeepEqual(dst.Indexes(), src.Indexes()) {
				t.Errorf("copy has schema %v and indexes %v", dst.Schema(), dst.Indexes())
			}
			want, _ := src.Find(ctx, nil)
			got, _ := dst.Find(ctx, nil)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("copied documents differ: %d vs %d", len(got), len(want))
			}
			if docs, examined, err := dst.find(ctx, &Query{Filters: []QueryFilter{{"g", "eq", "1"}}}); err != nil || len(docs) != 33 || examined != 33 {
				t.Errorf("index of the copy: %d docs after %d, %v", len(docs), examined, err)
			}
			if _, err := d.CopyCollection(ctx, "src", "dst", tt.target); !errors.Is(err, ErrExists) {
				t.Errorf("copy onto an existing collection: %v", err)
			}
		})
	}
	if _, err := d.CopyCollection(ctx, "nope", "x", nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("copy of a missing collection: %v", err)
	}
}

// TestConcurrentRenameAndWrites: writes racing a rename either land
// before it (and are in the renamed collection) or fail with ErrNotFound.
func TestConcurrentRenameAndWrites(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	e := openEngine(t, opts)
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "old", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "old")
	if err := c.CreateIndex(ctx, "by_w", "w"); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	done := make(chan []string, writers)
	for w := range writers {
		go func() {
			var ok []string
			for i := 0; ; i++ {
				id := fmt.Sprintf("w%d-%04d", w, i)
				if _, err := c.Insert(ctx, &Document{ID: id, Data: map[string]any{"w": w}}); err != nil {
					if !errors.Is(err, ErrNotFound) {
						t.Errorf("insert %s: %v", id, err)
					}
					done <- ok
					return
				}
				ok = append(ok, id)
				_, _ = c.Get(ctx, id) // reads race the rename too; they may fail with ErrNotFound
			}
		}()
	}
	for c.Count() < 50 {
		runtime.Gosched() // let the writers get going
	}
	if err := d.RenameCollection(ctx, "old", "new"); err != nil {
		t.Fatal(err)
	}
	want := model{"app": {"new": {indexes: map[string]string{"by_w": "w"}, docs: map[string]map[string]any{}}}}
	for range writers {
		for _, id := range <-done {
			var w float64
			if _, err := fmt.Sscanf(id, "w%f-", &w); err != nil {
				t.Fatal(err)
			}
			want["app"]["new"].docs[id] = map[string]any{"w": w}
		}
	}
	checkModel(t, e, want)
	e.abandon()
	e = openEngine(t, crashOpts(opts.Dir))
	checkModel(t, e, want)
}
