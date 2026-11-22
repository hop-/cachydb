package db

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// Repairing a damaged or missing index.db (AC-11, approach-index-and-query
// §8 step 2): open rebuilds it from the data segments, logs a WARN,
// recreates every index of coll.meta, and loses nothing.

// buildIndexedBase creates app/c with indexes by_v (v) and by_author
// (meta.author), holding documents in two flushes, a deleted one, and an
// unflushed one, and returns the engine and the expected state.
func buildIndexedBase(t *testing.T, opts Options) (*Engine, *Collection, model) {
	t.Helper()
	ctx := context.Background()
	e := openEngine(t, opts)
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "c")
	if err := c.CreateIndex(ctx, "by_v", "v"); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateIndex(ctx, "by_author", "meta.author"); err != nil {
		t.Fatal(err)
	}
	docs := map[string]map[string]any{}
	put := func(id string, data map[string]any) {
		mustInsert(t, c, id, data)
		docs[id] = data
	}
	put("a", map[string]any{"v": "x", "meta": map[string]any{"author": "ann"}})
	put("b", map[string]any{"v": 2.0, "tags": []any{"t1", "t2"}})
	put("c", map[string]any{"v": []any{1.0, "y"}})
	put("d", map[string]any{"v": "gone"})
	mustFlush(t, c)
	if err := c.Delete(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	delete(docs, "d")
	if _, err := c.Update(ctx, "a", map[string]any{"v": "x2"}); err != nil {
		t.Fatal(err)
	}
	docs["a"] = map[string]any{"v": "x2", "meta": map[string]any{"author": "ann"}}
	put("e", map[string]any{"v": strings.Repeat("long", 400), "meta": map[string]any{"author": "bob"}})
	mustFlush(t, c)
	put("unflushed", map[string]any{"v": "x2", "meta": map[string]any{"author": "ann"}})
	return e, c, model{"app": {"c": {
		indexes: map[string]string{"by_v": "v", "by_author": "meta.author"},
		docs:    docs,
	}}}
}

// TestIndexFileRebuiltAtOpen damages index.db in every way the open check
// can see and checks the repair.
func TestIndexFileRebuiltAtOpen(t *testing.T) {
	page := os.Getpagesize()
	damages := []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{"missing", func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}},
		{"cut", func(t *testing.T, path string) {
			// bbolt files have unused pages at the end, so cut to the
			// largest page multiple the open check rejects: the file then
			// lacks pages its newest transaction uses.
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for size := len(data) - page; size > 0; size -= page {
				probe := t.TempDir()
				if err := os.WriteFile(idxstore.Path(probe), data[:size], 0o644); err != nil {
					t.Fatal(err)
				}
				st, err := idxstore.Open(probe, idxstore.Options{})
				if err == nil {
					if err := st.Close(); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if !idxstore.NeedsRebuild(err) {
					t.Fatalf("open of a cut file: %v", err)
				}
				if err := os.Truncate(path, int64(size)); err != nil {
					t.Fatal(err)
				}
				return
			}
			t.Fatal("no cut of the index file is rejected")
		}},
		{"empty", func(t *testing.T, path string) {
			if err := os.Truncate(path, 0); err != nil {
				t.Fatal(err)
			}
		}},
		{"garbage", func(t *testing.T, path string) {
			if err := os.WriteFile(path, bytes.Repeat([]byte("garbage!"), int(fileSize(t, path))/8), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"both meta pages overwritten", func(t *testing.T, path string) {
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(bytes.Repeat([]byte{0xA5}, 2*page), 0); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"incomplete build", func(t *testing.T, path string) {
			dir := filepath.Dir(path)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			st, err := idxstore.Create(dir, idxstore.Options{Building: true}) // a rebuild that never finished
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, clean := range []bool{true, false} {
		for _, tt := range damages {
			name := tt.name + "/after a crash"
			if clean {
				name = tt.name + "/after a clean close"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				opts := crashOpts(t.TempDir())
				e, c, want := buildIndexedBase(t, opts)
				dir := c.dir
				if clean {
					if err := e.Close(ctx); err != nil {
						t.Fatal(err)
					}
				} else {
					e.abandon()
				}
				tt.damage(t, idxstore.Path(dir))
				segsBefore := listSegs(t, dir)

				var log syncBuffer
				e = openEngine(t, loggedOptions(opts.Dir, &log))
				if got := e.recov.indexFilesRebuilt.Load(); got != 1 {
					t.Errorf("index files rebuilt = %d, want 1", got)
				}
				if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "rebuilding it from the data segments") {
					t.Errorf("no WARN about the rebuild:\n%s", log.String())
				}
				if strings.Contains(log.String(), "upgrading") {
					t.Errorf("a repair logged as an upgrade:\n%s", log.String())
				}
				checkModel(t, e, want) // documents, Count, and every index entry (checkIndexEntries)
				c = mustColl(t, mustDB(t, e, "app"), "c")
				if got := listSegs(t, dir); !slices.Equal(got, segsBefore) {
					t.Errorf("data segments %v -> %v: a rebuild must keep them", segsBefore, got)
				}
				if got := c.Indexes(); len(got) != 2 {
					t.Errorf("Indexes = %+v, want by_author and by_v", got)
				}
				docs, plan, err := c.FindWithPlan(ctx, &Query{Filters: []QueryFilter{{"meta.author", "eq", "ann"}}})
				if err != nil || plan.Index != "by_author" || !slices.Equal(docIDs(docs), []string{"a", "unflushed"}) {
					t.Errorf("Find on the rebuilt index = %v, plan %+v, %v", docIDs(docs), plan, err)
				}
				// Writes go on, and the repaired file reopens without a
				// rebuild.
				mustInsert(t, c, "new", map[string]any{"v": "x2"})
				want["app"]["c"].docs["new"] = map[string]any{"v": "x2"}
				mustFlush(t, c)
				e = reopen(t, e)
				if got := e.recov.indexFilesRebuilt.Load(); got != 0 {
					t.Errorf("index files rebuilt at the next open = %d, want 0", got)
				}
				checkModel(t, e, want)
			})
		}
	}
}

// TestIndexFileStaleRebuilt: an index file that refers to data segments
// that are gone (for example an old copy restored over a compacted
// collection) does not match the segments; open rebuilds it with a WARN
// instead of trusting it.
func TestIndexFileStaleRebuilt(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	opts.CompactionRatio = 0.01
	e, c, want := buildIndexedBase(t, opts)
	mustSeal(t, c)
	mustFlush(t, c) // the unflushed document, in a new segment
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	stale, err := os.ReadFile(idxstore.Path(c.dir))
	if err != nil {
		t.Fatal(err)
	}
	e, c = openColl(t, opts)
	mustInsert(t, c, "later", map[string]any{"v": 9.0})
	want["app"]["c"].docs["later"] = map[string]any{"v": 9.0}
	mustFlushSeal(t, c)
	if !mustCompact(t, c) {
		t.Fatal("compaction did nothing")
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(idxstore.Path(c.dir), stale, 0o644); err != nil {
		t.Fatal(err)
	}

	var log syncBuffer
	e = openEngine(t, loggedOptions(opts.Dir, &log))
	if got := e.recov.indexFilesRebuilt.Load(); got != 1 {
		t.Errorf("index files rebuilt = %d, want 1", got)
	}
	if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "does not match the data segments") {
		t.Errorf("no WARN about the mismatch:\n%s", log.String())
	}
	checkModel(t, e, want)
	e = reopen(t, e)
	checkModel(t, e, want)
}

// TestIndexFileLongIDFailsOpen: a document id longer than the index file
// can hold, met while rebuilding a format-3 collection, fails Open with an
// error naming the collection and the limit, and leaves the directory as
// it was.
func TestIndexFileLongIDFailsOpen(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	e, c, _ := buildIndexedBase(t, opts)
	dir := c.dir
	if _, err := c.Insert(ctx, &Document{ID: strings.Repeat("k", idxstore.MaxIDLen+1)}); err == nil {
		t.Error("an id longer than the index file can hold was accepted")
	}
	if _, err := c.Insert(ctx, &Document{ID: strings.Repeat("k", idxstore.MaxIDLen), Data: map[string]any{"v": 1.0}}); err != nil {
		t.Errorf("an id of exactly %d bytes: %v", idxstore.MaxIDLen, err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	appendLongIDRecord(t, dir)
	if err := os.Remove(idxstore.Path(dir)); err != nil {
		t.Fatal(err)
	}
	before := dirListing(t, opts.Dir)
	_, err := Open(ctx, opts)
	if err == nil {
		t.Fatal("Open succeeded")
	}
	for _, want := range []string{`"c"`, "31742"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
	if after := dirListing(t, opts.Dir); !slices.Equal(after, before) {
		t.Errorf("the failed open changed the directory:\n%v\n->\n%v", before, after)
	}
	if !fileExistsT(t, segment.DataPath(dir, 99)) {
		t.Error("the segment with the long id was removed")
	}
}

// TestSyncIndexesAtOpen: open drops index buckets that coll.meta does not
// define (an interrupted drop_index) and builds definitions whose bucket is
// missing or not ready (an interrupted create_index), with a WARN for the
// latter; the index file is not rebuilt as a whole.
func TestSyncIndexesAtOpen(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	e, c, want := buildIndexedBase(t, opts)
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := idxstore.Open(c.dir, idxstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	err = st.Update(func(u *idxstore.WTx) error {
		if err := u.DropIndex("by_v"); err != nil { // missing bucket
			return err
		}
		if err := u.DropIndex("by_author"); err != nil {
			return err
		}
		if err := u.CreateIndex("by_author"); err != nil { // empty, not ready
			return err
		}
		if err := u.CreateIndex("orphan"); err != nil { // no definition
			return err
		}
		w, err := u.Index("orphan")
		if err != nil {
			return err
		}
		return w.Put([]byte{idxstore.TagNull}, "a", 0)
	})
	if cerr := st.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatal(err)
	}

	var log syncBuffer
	e = openEngine(t, loggedOptions(opts.Dir, &log))
	if got := e.recov.indexFilesRebuilt.Load(); got != 0 {
		t.Errorf("index files rebuilt = %d, want 0 (only the indexes are synced)", got)
	}
	if !strings.Contains(log.String(), "building indexes that are missing or incomplete") {
		t.Errorf("no WARN about the indexes being built:\n%s", log.String())
	}
	checkModel(t, e, want) // exactly by_v and by_author, with exact entries
	e = reopen(t, e)
	checkModel(t, e, want)
}

// TestIndexKeysOf: the entries one document contributes to an index
// (approach-index-and-query §3): one entry for a scalar (flag 0), one per
// distinct indexable element of an array (FlagElement), one "missing"
// entry for an absent field, none for objects, NaN, and nested arrays
// (complex), and FlagTruncated for values cut at 1,024 bytes.
func TestIndexKeysOf(t *testing.T) {
	enc := func(v any) string {
		e, _, ok := idxstore.EncodeValue(v)
		if !ok {
			t.Fatalf("EncodeValue(%#v) not indexable", v)
		}
		return string(e)
	}
	long := strings.Repeat("x", 2000)
	longEnc, truncated, _ := idxstore.EncodeValue(long)
	if !truncated {
		t.Fatal("long string not truncated")
	}
	var many []any
	for i := range 40 { // above the linear dedup limit: map-based dedup
		many = append(many, float64(i%20))
	}
	type entry struct {
		enc   string
		flags idxstore.Flags
	}
	el := idxstore.FlagElement
	tests := []struct {
		name         string
		data         map[string]any
		field        string
		want         []entry
		complex, arr bool
	}{
		{"scalar string", map[string]any{"f": "a"}, "f", []entry{{enc("a"), 0}}, false, false},
		{"number types fold", map[string]any{"f": 1}, "f", []entry{{enc(1.0), 0}}, false, false},
		{"null", map[string]any{"f": nil}, "f", []entry{{enc(nil), 0}}, false, false},
		{"bool", map[string]any{"f": false}, "f", []entry{{enc(false), 0}}, false, false},
		{"missing", map[string]any{"g": 1.0}, "f", []entry{{string([]byte{idxstore.TagMissing}), 0}}, false, false},
		{"array elements, deduplicated", map[string]any{"f": []any{"a", 1.0, "a", true, 1}}, "f",
			[]entry{{enc("a"), el}, {enc(1.0), el}, {enc(true), el}}, false, true},
		{"empty array", map[string]any{"f": []any{}}, "f", nil, false, true},
		{"array with an object", map[string]any{"f": []any{"a", map[string]any{"k": 1.0}}}, "f", []entry{{enc("a"), el}}, true, true},
		{"array with a nested array", map[string]any{"f": []any{[]any{1.0}}}, "f", nil, true, true},
		{"object", map[string]any{"f": map[string]any{"k": 1.0}}, "f", nil, true, false},
		{"NaN", map[string]any{"f": math.NaN()}, "f", nil, true, false},
		{"truncated string", map[string]any{"f": long}, "f", []entry{{string(longEnc), idxstore.FlagTruncated}}, false, false},
		{"truncated elements share an entry", map[string]any{"f": []any{long + "1", long + "2"}}, "f",
			[]entry{{string(longEnc), el | idxstore.FlagTruncated}}, false, true},
		{"long array", map[string]any{"f": many}, "f", func() []entry {
			var out []entry
			for i := range 20 {
				out = append(out, entry{enc(float64(i)), el})
			}
			return out
		}(), false, true},
		{"nested path", map[string]any{"m": map[string]any{"a": "x"}}, "m.a", []entry{{enc("x"), 0}}, false, false},
		{"literal dotted key wins", map[string]any{"m.a": "lit", "m": map[string]any{"a": "x"}}, "m.a", []entry{{enc("lit"), 0}}, false, false},
		{"path through an array is missing", map[string]any{"m": []any{map[string]any{"a": "x"}}}, "m.a",
			[]entry{{string([]byte{idxstore.TagMissing}), 0}}, false, false},
		{"_id", map[string]any{}, "_id", []entry{{enc("doc"), 0}}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := indexKeysOf(&Document{ID: "doc", Data: tt.data}, tt.field)
			var entries []entry
			for _, e := range got.entries {
				entries = append(entries, entry{string(e.enc), e.flags})
			}
			if !slices.Equal(entries, tt.want) || got.complex != tt.complex || got.array != tt.arr {
				t.Errorf("indexKeysOf = %x, complex %v, array %v; want %x, complex %v, array %v",
					entries, got.complex, got.array, tt.want, tt.complex, tt.arr)
			}
		})
	}
	if got := indexKeysOf(nil, "f"); len(got.entries) != 0 || got.complex || got.array {
		t.Errorf("indexKeysOf(nil) = %+v, want nothing", got)
	}
}
