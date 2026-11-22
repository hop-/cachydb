package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// openColl opens opts.Dir and returns collection app/c.
func openColl(t *testing.T, opts Options) (*Engine, *Collection) {
	t.Helper()
	e := openEngine(t, opts)
	return e, mustColl(t, mustDB(t, e, "app"), "c")
}

// newCrashColl opens a fresh engine (crashOpts, modified by mod) with
// database app and collection c (schema s).
func newCrashColl(t *testing.T, mod func(*Options), s *Schema) (Options, *Engine, *Collection) {
	t.Helper()
	opts := crashOpts(t.TempDir())
	if mod != nil {
		mod(&opts)
	}
	e := openEngine(t, opts)
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(context.Background(), "c", s); err != nil {
		t.Fatal(err)
	}
	return opts, e, mustColl(t, d, "c")
}

// segRecordCount returns the number of records in c's data segments.
func segRecordCount(t *testing.T, dir string) int {
	t.Helper()
	return len(segRecords(t, dir))
}

// TestRecoveryRegressionF2SchemalessCreateColl: a schemaless create_coll
// record replays, both when its meta write never happened and over an
// existing collection.
func TestRecoveryRegressionF2SchemalessCreateColl(t *testing.T) {
	ctx := context.Background()
	t.Run("meta write lost", func(t *testing.T) {
		opts := crashOpts(t.TempDir())
		e := openEngine(t, opts)
		mustCreateDB(t, e, "app")
		e.abandon()

		armed := opts
		crashAt(t, &armed, hookMetaWrite)
		e = openEngine(t, armed)
		if err := mustDB(t, e, "app").CreateCollection(ctx, "c", nil); !errors.Is(err, errSimulatedCrash) {
			t.Fatalf("CreateCollection = %v, want a simulated crash", err)
		}
		e.abandon()

		e, c := openColl(t, opts)
		if c.schema != nil {
			t.Errorf("schema = %+v, want nil", c.schema)
		}
		m := readCollMetaFile(t, filepath.Join(c.dir, collMetaFile))
		if m.Schema != nil || m.CreatedOffset == 0 || m.CreatedOffset != m.WALOffset {
			t.Errorf("replayed coll.meta = %+v", m)
		}
		mustInsert(t, c, "a", map[string]any{"anything": []any{1, "x"}})
		_, c = reopenColl(t, e)
		mustGet(t, c, "a")
	})
	t.Run("replay over existing", func(t *testing.T) {
		opts, e, c := newCrashColl(t, nil, nil)
		mustInsert(t, c, "a", map[string]any{"v": 1})
		e.abandon()
		e, c = openColl(t, opts)
		if c.schema != nil {
			t.Errorf("schema = %+v, want nil", c.schema)
		}
		mustGet(t, c, "a")
		if got := e.recov.replayApplied.Load(); got != 1 {
			t.Errorf("replay applied %d records, want 1 (the insert; create_db/create_coll exist)", got)
		}
	})
}

// TestRecoveryRegressionF3UpdateReplay: updates replay onto flushed and
// unflushed versions, including one that crashed after its WAL sync.
func TestRecoveryRegressionF3UpdateReplay(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	mustInsert(t, c, "flushed", map[string]any{"v": 1, "keep": "yes"})
	mustFlush(t, c)
	mustInsert(t, c, "mem", map[string]any{"v": 1})
	for i := 2; i <= 3; i++ {
		for _, id := range []string{"flushed", "mem"} {
			if _, err := c.Update(ctx, id, map[string]any{"v": i}); err != nil {
				t.Fatal(err)
			}
		}
	}
	e.abandon()

	armed := opts
	crashAt(t, &armed, hookAfterWALSync)
	e, c = openColl(t, armed)
	if _, err := c.Update(ctx, "flushed", map[string]any{"v": 4}); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("Update = %v, want a simulated crash", err)
	}
	e.abandon()

	e, c = openColl(t, opts)
	want := map[string]map[string]any{
		"flushed": {"v": 4.0, "keep": "yes"},
		"mem":     {"v": 3.0},
	}
	for id, data := range want {
		if got := mustGet(t, c, id).Data; fmt.Sprint(got) != fmt.Sprint(data) {
			t.Errorf("Get(%s) = %v, want %v", id, got, data)
		}
	}
	_, c = reopenColl(t, e)
	if got := mustGet(t, c, "flushed").Data["v"]; got != 4.0 {
		t.Errorf("after clean reopen v = %v, want 4", got)
	}
}

// walFiles returns the WAL segment paths in dir, oldest first.
func walFiles(t *testing.T, dir string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

// appendBytes appends b to the file at path.
func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestTornTailsRegressionF4: a torn WAL tail and a torn data tail (alone
// and together) are truncated at open and every acknowledged change is
// recovered.
func TestTornTailsRegressionF4(t *testing.T) {
	tornWAL := []byte{40, 0, 0, 0, 1, 2, 3, 4, 9, 9, 9} // a record header promising 40 bytes, then a few
	for _, tt := range []struct {
		name      string
		wal, data bool
	}{
		{"wal", true, false},
		{"data", false, true},
		{"both", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			opts, e, c := newCrashColl(t, nil, nil)
			if err := c.CreateIndex(ctx, "by_v", "v"); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "a", map[string]any{"v": 1})
			mustInsert(t, c, "b", map[string]any{"v": 2})
			mustFlush(t, c)
			if _, err := c.Update(ctx, "a", map[string]any{"v": 3}); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(ctx, "b"); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "c", map[string]any{"v": 4})
			mustFlush(t, c)
			e.abandon()

			if tt.wal {
				files := walFiles(t, opts.Dir)
				appendBytes(t, files[len(files)-1], tornWAL)
			}
			if tt.data {
				// Cut into the last flush's checkpoint marker.
				chopTail(t, newestDataSegment(t, c.dir), 3)
			}

			e = openEngine(t, opts)
			checkModel(t, e, model{"app": {"c": {
				indexes: map[string]string{"by_v": "v"},
				docs:    map[string]map[string]any{"a": {"v": 3}, "c": {"v": 4}},
			}}})
			c = mustColl(t, mustDB(t, e, "app"), "c")
			mustInsert(t, c, "d", map[string]any{"v": 5})
			_, c = reopenColl(t, e)
			if c.Count() != 3 {
				t.Errorf("Count after writes past the torn tails = %d, want 3", c.Count())
			}
		})
	}
}

// TestRecoveryRegressionF5OffsetsContinue: after crashes without a
// checkpoint, WAL offsets continue and no WAL file is added per start.
func TestRecoveryRegressionF5OffsetsContinue(t *testing.T) {
	opts, e, c := newCrashColl(t, nil, nil)
	var prevNext uint64
	for i := range 4 {
		next := e.wal.NextOffset()
		if i > 0 && next != prevNext {
			t.Fatalf("start %d: NextOffset = %d, previous session ended at %d", i, next, prevNext)
		}
		mustInsert(t, c, fmt.Sprintf("d%d", i), map[string]any{"v": i})
		if got := memOffset(t, c, fmt.Sprintf("d%d", i)); got < next {
			t.Errorf("start %d: new record got offset %d, below NextOffset %d at open", i, got, next)
		}
		prevNext = e.wal.NextOffset()
		e.abandon()
		if n := len(walFiles(t, opts.Dir)); n != 1 {
			t.Fatalf("start %d: %d WAL files, want 1", i, n)
		}
		e, c = openColl(t, opts)
	}
	if c.Count() != 4 {
		t.Errorf("Count = %d, want 4", c.Count())
	}
	for i := range 4 {
		mustGet(t, c, fmt.Sprintf("d%d", i))
	}
}

// closedIndexMeta reads the covered position, the checkpoint, and the
// registered segments of the index file of the collection directory dir,
// which no engine has open.
func closedIndexMeta(t *testing.T, dir string) (idxstore.Position, uint64, []idxstore.SegmentStats) {
	t.Helper()
	st, err := idxstore.Open(dir, idxstore.Options{})
	if err != nil {
		t.Fatalf("opening the index file of %s: %v", dir, err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	var (
		covered idxstore.Position
		cp      uint64
		segs    []idxstore.SegmentStats
	)
	if err := st.View(func(tx *idxstore.Tx) (err error) {
		if covered, err = tx.Covered(); err != nil {
			return err
		}
		if cp, err = tx.Checkpoint(); err != nil {
			return err
		}
		segs, err = tx.Segments()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return covered, cp, segs
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// TestCrashRegressionF7SegSyncBeforeKeydir: a crash between the segment
// fsync and the index commit (AC-6, approach-index-and-query §4). The
// segment then holds records the index file does not know. Open truncates
// the active segment to the index file's covered position, or deletes a
// segment file the index file does not register; the checkpoint did not
// advance, so replay restores the changes from the WAL into the memtable,
// and the next flush writes them exactly once.
func TestCrashRegressionF7SegSyncBeforeKeydir(t *testing.T) {
	for _, tc := range []struct {
		name     string
		newSeg   bool // every flush seals: the crashed flush writes an unregistered segment
		tornTail bool // the crashed flush's last record is torn as well
	}{
		{name: "active segment truncated to covered"},
		{name: "torn tail truncated to covered", tornTail: true},
		{name: "unregistered segment deleted", newSeg: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			opts, e, c := newCrashColl(t, func(o *Options) {
				if tc.newSeg {
					o.DataSegmentSize = 1
				}
			}, nil)
			if err := c.CreateIndex(ctx, "by_v", "v"); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "a", map[string]any{"v": 1})
			mustInsert(t, c, "b", map[string]any{"v": 2})
			mustFlush(t, c)
			cp0 := checkpointOf(c)
			e.abandon()
			before := segRecordCount(t, c.dir)
			covered0, storedCP0, segs0 := closedIndexMeta(t, c.dir)
			if storedCP0 != cp0 {
				t.Fatalf("index file checkpoint = %d, collection checkpoint = %d", storedCP0, cp0)
			}

			armed := opts
			crashAt(t, &armed, hookAfterSegSync)
			e, c = openColl(t, armed)
			if _, err := c.Update(ctx, "a", map[string]any{"v": 10}); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(ctx, "b"); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "c", map[string]any{"v": 3})
			if err := c.flush(); !errors.Is(err, errSimulatedCrash) {
				t.Fatalf("flush = %v, want a simulated crash", err)
			}
			e.abandon()

			// The crashed flush's records are durable in a segment; the index
			// file is unchanged.
			if got := segRecordCount(t, c.dir); got != before+3 {
				t.Fatalf("segments hold %d records after the crashed flush, want %d", got, before+3)
			}
			covered, storedCP, segs := closedIndexMeta(t, c.dir)
			if covered != covered0 || storedCP != cp0 || !slices.Equal(segs, segs0) {
				t.Fatalf("index file changed by the crashed flush: covered %+v -> %+v, checkpoint %d -> %d, segments %+v -> %+v",
					covered0, covered, cp0, storedCP, segs0, segs)
			}
			newest := newestDataSegment(t, c.dir)
			registered := false
			for _, st := range segs {
				registered = registered || segment.DataPath(c.dir, st.Seg) == newest
			}
			if registered == tc.newSeg {
				t.Fatalf("newest segment %s registered = %v, want %v", newest, registered, !tc.newSeg)
			}
			if !tc.newSeg {
				if size := fileSize(t, newest); uint64(size) <= covered.End || segment.DataPath(c.dir, covered.Seg) != newest {
					t.Fatalf("active segment %s is %d bytes, covered %+v: want records beyond covered", newest, size, covered)
				}
			}
			if tc.tornTail {
				chopTail(t, newest, 3)
			}

			e, c = openColl(t, opts)
			want := model{"app": {"c": {indexes: map[string]string{"by_v": "v"}, docs: map[string]map[string]any{"a": {"v": 10}, "c": {"v": 3}}}}}
			checkModel(t, e, want)
			if tc.newSeg {
				if fileExistsT(t, newest) {
					t.Errorf("unregistered segment %s kept by open", newest)
				}
			} else if size := fileSize(t, newest); uint64(size) != covered.End {
				t.Errorf("active segment %s is %d bytes after open, want truncated to covered %d", newest, size, covered.End)
			}
			if got := segRecordCount(t, c.dir); got != before {
				t.Errorf("segments hold %d records after open, want %d (the crashed flush's records removed)", got, before)
			}
			c.mu.RLock()
			memLen, cp := len(c.memtable), c.checkpoint
			c.mu.RUnlock()
			if memLen != 3 {
				t.Errorf("memtable holds %d entries after replay, want 3 (a, the tombstone of b, c)", memLen)
			}
			if cp != cp0 {
				t.Errorf("checkpoint = %d, want %d (the crashed flush never committed)", cp, cp0)
			}
			if got := e.recov.replayApplied.Load(); got != 3 {
				t.Errorf("replay applied %d records, want 3", got)
			}

			mustFlush(t, c)
			if got := segRecordCount(t, c.dir); got != before+3 {
				t.Errorf("flush after replay: segments hold %d records, want %d", got, before+3)
			}
			if got := storedCheckpoint(t, c); got <= cp0 {
				t.Errorf("index file checkpoint after the flush = %d, want above %d", got, cp0)
			}
			checkModel(t, e, want)
			e = reopen(t, e)
			checkModel(t, e, want)
		})
	}
}

// TestRecoveryRegressionF8FailedFlushThenCrash: a failed flush keeps the
// old checkpoint, so a crash right after it loses nothing.
func TestRecoveryRegressionF8FailedFlushThenCrash(t *testing.T) {
	for _, mode := range []string{"append", "sync"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h := &segHooks{}
			opts, e, c := newCrashColl(t, func(o *Options) {
				o.testSegWriter = func(w segmentWriter) segmentWriter { return &hookedSegWriter{segmentWriter: w, h: h} }
			}, nil)
			mustInsert(t, c, "a", map[string]any{"v": 1})
			mustFlush(t, c)
			cp0 := checkpointOf(c)
			mustInsert(t, c, "b", map[string]any{"v": 2})
			if err := c.Delete(ctx, "a"); err != nil {
				t.Fatal(err)
			}
			h.mu.Lock()
			if mode == "append" {
				h.failAppend = 1
			} else {
				h.failSync = 1
			}
			h.mu.Unlock()
			if err := c.flush(); !errors.Is(err, errInjected) {
				t.Fatalf("flush = %v, want the injected error", err)
			}
			e.abandon()

			opts.testSegWriter = nil
			e, c = openColl(t, opts)
			if cp := checkpointOf(c); cp != cp0 {
				t.Errorf("checkpoint after failed flush and crash = %d, want %d", cp, cp0)
			}
			checkModel(t, e, model{"app": {"c": {indexes: map[string]string{}, docs: map[string]map[string]any{"b": {"v": 2}}}}})
		})
	}
}

// TestRecoveryRegressionF9DeleteFlushCrash: a deleted document stays
// deleted after a crash, whether its tombstone was flushed or not.
func TestRecoveryRegressionF9DeleteFlushCrash(t *testing.T) {
	for _, flushDelete := range []bool{true, false} {
		t.Run(fmt.Sprintf("flush-delete=%v", flushDelete), func(t *testing.T) {
			opts, e, c := newCrashColl(t, nil, nil)
			if err := c.CreateIndex(context.Background(), "by_v", "v"); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "x", map[string]any{"v": 1})
			mustInsert(t, c, "keep", map[string]any{"v": 1})
			mustFlush(t, c)
			if err := c.Delete(context.Background(), "x"); err != nil {
				t.Fatal(err)
			}
			if flushDelete {
				mustFlush(t, c)
			}
			e.abandon()
			e = openEngine(t, opts)
			want := model{"app": {"c": {indexes: map[string]string{"by_v": "v"}, docs: map[string]map[string]any{"keep": {"v": 1}}}}}
			checkModel(t, e, want)
			e = reopen(t, e)
			checkModel(t, e, want)
		})
	}
}

// TestConcurrentWritesDuringFlushCrashRegressionF10: concurrent inserts,
// updates, and deletes while the flusher runs (with segment rolls and
// index snapshots) lose nothing acknowledged across a crash.
func TestConcurrentWritesDuringFlushCrashRegressionF10(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, func(o *Options) {
		o.FlushInterval = time.Millisecond
		o.MemtableSize = 2 << 10
		o.DataSegmentSize = 8 << 10
	}, nil)
	if err := c.CreateIndex(ctx, "by_w", "w"); err != nil {
		t.Fatal(err)
	}
	const (
		writers = 8
		ops     = 150
		idsPer  = 12
	)
	results := make([]map[string]map[string]any, writers)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for g := range writers {
		wg.Go(func() {
			docs := map[string]map[string]any{}
			for i := range ops {
				id := fmt.Sprintf("g%d-%d", g, i%idsPer)
				data := map[string]any{"w": float64(i % 5), "g": float64(g), "i": float64(i)}
				var err error
				switch _, exists := docs[id]; {
				case !exists:
					_, err = c.Insert(ctx, &Document{ID: id, Data: data})
				case i%3 == 0:
					err = c.Delete(ctx, id)
					data = nil
				default:
					_, err = c.Update(ctx, id, data)
				}
				if err != nil {
					errs <- fmt.Errorf("writer %d op %d on %s: %w", g, i, id, err)
					return
				}
				if data == nil {
					delete(docs, id)
				} else {
					docs[id] = data
				}
			}
			results[g] = docs
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	e.abandon()

	want := model{"app": {"c": {indexes: map[string]string{"by_w": "w"}, docs: map[string]map[string]any{}}}}
	for _, docs := range results {
		maps.Copy(want["app"]["c"].docs, docs)
	}
	opts.FlushInterval = time.Hour
	e = openEngine(t, opts)
	checkModel(t, e, want)
	e = reopen(t, e)
	checkModel(t, e, want)
}

// TestRecoveryDroppedAndRecreated: records of dropped databases and
// collections are skipped, also when a new one with the same name exists.
func TestRecoveryDroppedAndRecreated(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	e := openEngine(t, opts)
	d := mustCreateDB(t, e, "app")

	// Collection dropped and recreated with another schema.
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "c")
	mustInsert(t, c, "old-flushed", map[string]any{"v": 1})
	mustFlush(t, c)
	mustInsert(t, c, "old-mem", map[string]any{"v": 2})
	if err := c.CreateIndex(ctx, "by_v", "v"); err != nil {
		t.Fatal(err)
	}
	if err := d.DropCollection(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(ctx, "c", crashSchema); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, mustColl(t, d, "c"), "new", map[string]any{"name": "n"})

	// Collection dropped for good.
	if err := d.CreateCollection(ctx, "gone", nil); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, mustColl(t, d, "gone"), "g", map[string]any{"v": 1})
	if err := d.DropCollection(ctx, "gone"); err != nil {
		t.Fatal(err)
	}

	// Database dropped and recreated without its collection.
	old := mustCreateDB(t, e, "db2")
	if err := old.CreateCollection(ctx, "x", nil); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, mustColl(t, old, "x"), "x1", map[string]any{"v": 1})
	if err := e.DropDatabase(ctx, "db2"); err != nil {
		t.Fatal(err)
	}
	if err := mustCreateDB(t, e, "db2").CreateCollection(ctx, "y", nil); err != nil {
		t.Fatal(err)
	}

	// Database dropped for good.
	if err := mustCreateDB(t, e, "tmp").CreateCollection(ctx, "t", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.DropDatabase(ctx, "tmp"); err != nil {
		t.Fatal(err)
	}
	e.abandon()

	want := model{
		"app": {"c": {schema: crashSchema, indexes: map[string]string{}, docs: map[string]map[string]any{"new": {"name": "n"}}}},
		"db2": {"y": {indexes: map[string]string{}, docs: map[string]map[string]any{}}},
	}
	e = openEngine(t, opts)
	checkModel(t, e, want)
	e = reopen(t, e)
	checkModel(t, e, want)
}

// TestRecoveryMetadataReplay: every metadata operation whose meta file
// change was lost in a crash is applied by replay.
func TestRecoveryMetadataReplay(t *testing.T) {
	ctx := context.Background()
	base := func(t *testing.T, e *Engine) {
		d := mustCreateDB(t, e, "app")
		if err := d.CreateCollection(ctx, "c", nil); err != nil {
			t.Fatal(err)
		}
		c := mustColl(t, d, "c")
		if err := c.CreateIndex(ctx, "by_v", "v"); err != nil {
			t.Fatal(err)
		}
		mustInsert(t, c, "a", map[string]any{"v": 1})
	}
	baseModel := func() model {
		return model{"app": {"c": {indexes: map[string]string{"by_v": "v"}, docs: map[string]map[string]any{"a": {"v": 1}}}}}
	}
	tests := []struct {
		name  string
		run   func(e *Engine) error
		apply func(m model)
	}{
		{"create database", func(e *Engine) error { _, err := e.CreateDatabase(ctx, "other"); return err },
			func(m model) { m["other"] = map[string]*modelColl{} }},
		{"drop database", func(e *Engine) error { return e.DropDatabase(ctx, "app") },
			func(m model) { delete(m, "app") }},
		{"create collection", func(e *Engine) error { return mustDB(t, e, "app").CreateCollection(ctx, "n", crashSchema) },
			func(m model) {
				m["app"]["n"] = &modelColl{schema: crashSchema, indexes: map[string]string{}, docs: map[string]map[string]any{}}
			}},
		{"drop collection", func(e *Engine) error { return mustDB(t, e, "app").DropCollection(ctx, "c") },
			func(m model) { delete(m["app"], "c") }},
		{"create index", func(e *Engine) error {
			return mustColl(t, mustDB(t, e, "app"), "c").CreateIndex(ctx, "by_w", "w")
		}, func(m model) { m["app"]["c"].indexes["by_w"] = "w" }},
		{"drop index", func(e *Engine) error { return mustColl(t, mustDB(t, e, "app"), "c").DropIndex(ctx, "by_v") },
			func(m model) { delete(m["app"]["c"].indexes, "by_v") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := crashOpts(t.TempDir())
			e := openEngine(t, opts)
			base(t, e)
			e.abandon()

			armed := opts
			crashAt(t, &armed, hookMetaWrite)
			e = openEngine(t, armed)
			if err := tt.run(e); !errors.Is(err, errSimulatedCrash) {
				t.Fatalf("got %v, want a simulated crash", err)
			}
			e.abandon()

			want := baseModel()
			tt.apply(want)
			e = openEngine(t, opts)
			checkModel(t, e, want)
			e = reopen(t, e)
			checkModel(t, e, want)
		})
	}
}

// TestRecoveryReplayToMemtable: replayed changes land in the memtable;
// Open writes no data segment records, and the next flush persists them.
func TestRecoveryReplayToMemtable(t *testing.T) {
	opts, e, c := newCrashColl(t, nil, nil)
	mustInsert(t, c, "seed", map[string]any{"v": 0})
	mustFlush(t, c)
	for i := range 5 {
		mustInsert(t, c, fmt.Sprintf("d%d", i), map[string]any{"v": i})
	}
	e.abandon()
	before := segRecordCount(t, c.dir)

	e, c = openColl(t, opts)
	if got := segRecordCount(t, c.dir); got != before {
		t.Errorf("Open wrote %d segment records, want 0", got-before)
	}
	c.mu.RLock()
	memLen := len(c.memtable)
	c.mu.RUnlock()
	if memLen != 5 {
		t.Errorf("memtable holds %d entries after replay, want 5", memLen)
	}
	if got := e.recov.replayApplied.Load(); got != 5 {
		t.Errorf("replay applied %d records, want 5", got)
	}
	mustFlush(t, c)
	if got := segRecordCount(t, c.dir); got != before+5 {
		t.Errorf("flush after replay wrote %d records, want 5 (the documents; no checkpoint marker)", got-before)
	}
	if got, want := storedCheckpoint(t, c), checkpointOf(c); got != want {
		t.Errorf("index file checkpoint after the flush = %d, want %d", got, want)
	}
}

// TestRecoveryWALMissing: a WAL that ends before offsets the data
// references (WAL files deleted) fails Open with ErrWALCorrupt.
func TestRecoveryWALMissing(t *testing.T) {
	opts, e, c := newCrashColl(t, nil, nil)
	mustInsert(t, c, "a", map[string]any{"v": 1})
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, p := range walFiles(t, opts.Dir) {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	e2, err := Open(context.Background(), opts)
	if err == nil {
		_ = e2.Close(context.Background())
		t.Fatal("Open succeeded without the WAL")
	}
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("Open = %v, want ErrWALCorrupt", err)
	}
	// The failed Open released the lock.
	if _, err := Open(context.Background(), opts); errors.Is(err, ErrDataDirInUse) {
		t.Fatalf("lock still held after failed Open: %v", err)
	}
}

// TestRecoveryManyCollectionsParallel: collections load in parallel and
// each recovers its own documents and indexes.
func TestRecoveryManyCollectionsParallel(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	e := openEngine(t, opts)
	want := model{}
	for di := range 3 {
		dn := fmt.Sprintf("db%d", di)
		d := mustCreateDB(t, e, dn)
		want[dn] = map[string]*modelColl{}
		for ci := range 6 {
			cn := fmt.Sprintf("c%d", ci)
			if err := d.CreateCollection(ctx, cn, nil); err != nil {
				t.Fatal(err)
			}
			c := mustColl(t, d, cn)
			if err := c.CreateIndex(ctx, "by_v", "v"); err != nil {
				t.Fatal(err)
			}
			mc := &modelColl{indexes: map[string]string{"by_v": "v"}, docs: map[string]map[string]any{}}
			for i := range 10 {
				id := fmt.Sprintf("%s-%s-%d", dn, cn, i)
				mustInsert(t, c, id, map[string]any{"v": i % 3})
				mc.docs[id] = map[string]any{"v": i % 3}
				if i == 4 {
					mustFlush(t, c)
				}
			}
			want[dn][cn] = mc
		}
	}
	e.abandon()
	e = openEngine(t, opts)
	checkModel(t, e, want)
}
