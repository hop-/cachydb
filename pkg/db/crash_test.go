package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// crashOpts returns options for crash and recovery tests: the flusher's
// timer is effectively off, so flushes happen only when a test asks.
func crashOpts(dir string) Options {
	o := testOptions(dir)
	o.FlushInterval = time.Hour
	return o
}

// model is the expected (or observed) logical state of an engine:
// database -> collection -> contents.
type model map[string]map[string]*modelColl

type modelColl struct {
	schema  *Schema
	indexes map[string]string         // name -> field
	docs    map[string]map[string]any // id -> data
}

func (m model) clone() model {
	out := make(model, len(m))
	for dn, colls := range m {
		out[dn] = make(map[string]*modelColl, len(colls))
		for cn, mc := range colls {
			docs := make(map[string]map[string]any, len(mc.docs))
			for id, data := range mc.docs {
				docs[id] = maps.Clone(data)
			}
			out[dn][cn] = &modelColl{schema: cloneSchema(mc.schema), indexes: maps.Clone(mc.indexes), docs: docs}
		}
	}
	return out
}

// dump renders m deterministically, for comparisons with readable diffs.
func (m model) dump() string {
	var b strings.Builder
	for _, dn := range slices.Sorted(maps.Keys(m)) {
		fmt.Fprintf(&b, "db %s\n", dn)
		for _, cn := range slices.Sorted(maps.Keys(m[dn])) {
			mc := m[dn][cn]
			schema, _ := json.Marshal(mc.schema)
			fmt.Fprintf(&b, "  coll %s schema=%s\n", cn, schema)
			for _, name := range slices.Sorted(maps.Keys(mc.indexes)) {
				fmt.Fprintf(&b, "    index %s on %s\n", name, mc.indexes[name])
			}
			for _, id := range slices.Sorted(maps.Keys(mc.docs)) {
				data, _ := json.Marshal(mc.docs[id])
				fmt.Fprintf(&b, "    doc %s %s\n", id, data)
			}
		}
	}
	return b.String()
}

// captureModel reads the whole logical state of e. It also checks that
// every collection's Count and every secondary index agree with its
// documents (an index must equal one rebuilt from scratch).
func captureModel(t *testing.T, e *Engine) model {
	t.Helper()
	ctx := context.Background()
	m := model{}
	for _, dn := range e.ListDatabases() {
		d := mustDB(t, e, dn)
		m[dn] = map[string]*modelColl{}
		for _, cn := range d.ListCollections() {
			c := mustColl(t, d, cn)
			docs, err := c.Find(ctx, nil)
			if err != nil {
				t.Fatalf("Find(%s/%s): %v", dn, cn, err)
			}
			mc := &modelColl{schema: c.schema, indexes: map[string]string{}, docs: map[string]map[string]any{}}
			for _, doc := range docs {
				mc.docs[doc.ID] = doc.Data
			}
			if c.Count() != len(docs) {
				t.Errorf("%s/%s: Count = %d, Find returned %d documents", dn, cn, c.Count(), len(docs))
			}
			d.mu.RLock()
			c.mu.RLock()
			for name, ix := range c.indexes {
				mc.indexes[name] = ix.field
				want := newIndex(name, ix.field)
				for _, doc := range docs {
					want.add(doc.ID, doc)
				}
				if got, w := ix.snapshotKeys(), want.snapshotKeys(); !reflect.DeepEqual(got, w) {
					t.Errorf("%s/%s: index %s = %v, rebuilt from the documents = %v", dn, cn, name, got, w)
				}
			}
			c.mu.RUnlock()
			d.mu.RUnlock()
			m[dn][cn] = mc
		}
	}
	return m
}

// checkModel compares e's state with want.
func checkModel(t *testing.T, e *Engine, want model) {
	t.Helper()
	if got, w := captureModel(t, e).dump(), want.dump(); got != w {
		t.Errorf("state mismatch\n--- got\n%s--- want\n%s--- differing lines\n%s", got, w, lineDiff(got, w))
	}
}

// lineDiff lists the lines only in got ("+") or only in want ("-").
func lineDiff(got, want string) string {
	count := func(s string) map[string]int {
		m := map[string]int{}
		for _, l := range strings.Split(strings.TrimSuffix(s, "\n"), "\n") {
			m[l]++
		}
		return m
	}
	g, w := count(got), count(want)
	var b strings.Builder
	for _, l := range slices.Sorted(maps.Keys(g)) {
		for range g[l] - w[l] {
			fmt.Fprintf(&b, "+ %s\n", l)
		}
	}
	for _, l := range slices.Sorted(maps.Keys(w)) {
		for range w[l] - g[l] {
			fmt.Fprintf(&b, "- %s\n", l)
		}
	}
	return b.String()
}

// chopTail removes n bytes from the end of path (a torn write).
func chopTail(t *testing.T, path string, n int64) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, fi.Size()-n); err != nil {
		t.Fatal(err)
	}
}

// newestDataSegment returns the path of the newest data segment in dir.
func newestDataSegment(t *testing.T, dir string) string {
	t.Helper()
	segs, err := segment.ListSegments(dir)
	if err != nil || len(segs) == 0 {
		t.Fatalf("ListSegments(%s) = %v, %v", dir, segs, err)
	}
	return segment.DataPath(dir, segs[len(segs)-1])
}

// buildCrashBase creates database "app" with collection "c" (schemaless,
// index by_g on g) holding flushed and unflushed documents, abandons the
// engine, and returns the expected state.
func buildCrashBase(t *testing.T, opts Options) model {
	t.Helper()
	ctx := context.Background()
	e := openEngine(t, opts)
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "c")
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "u", map[string]any{"g": "x", "v": 1})
	mustInsert(t, c, "d", map[string]any{"g": "x", "v": 2})
	mustInsert(t, c, "k", map[string]any{"g": "y", "v": 3})
	mustFlush(t, c)
	if _, err := c.Update(ctx, "k", map[string]any{"v": 4}); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "m", map[string]any{"g": "y", "v": 5})
	e.abandon()
	return model{"app": {"c": {
		indexes: map[string]string{"by_g": "g"},
		docs: map[string]map[string]any{
			"u": {"g": "x", "v": 1},
			"d": {"g": "x", "v": 2},
			"k": {"g": "y", "v": 4},
			"m": {"g": "y", "v": 5},
		},
	}}}
}

var crashSchema = &Schema{Fields: map[string]Field{"name": {Type: TypeString, Required: true}}}

// crashOps are the operations of the crash-point matrix. meta marks the
// metadata operations (they reach hookMetaWrite; document writes reach
// hookAfterWALSync).
var crashOps = []struct {
	name  string
	meta  bool
	run   func(ctx context.Context, d *Database, c *Collection) error
	apply func(m model)
}{
	{
		name: "insert",
		run: func(ctx context.Context, _ *Database, c *Collection) error {
			_, err := c.Insert(ctx, &Document{ID: "new", Data: map[string]any{"g": "z", "v": 9}})
			return err
		},
		apply: func(m model) { m["app"]["c"].docs["new"] = map[string]any{"g": "z", "v": 9} },
	},
	{
		name: "update",
		run: func(ctx context.Context, _ *Database, c *Collection) error {
			_, err := c.Update(ctx, "u", map[string]any{"g": "y", "v": 10})
			return err
		},
		apply: func(m model) { m["app"]["c"].docs["u"] = map[string]any{"g": "y", "v": 10} },
	},
	{
		name:  "delete",
		run:   func(ctx context.Context, _ *Database, c *Collection) error { return c.Delete(ctx, "d") },
		apply: func(m model) { delete(m["app"]["c"].docs, "d") },
	},
	{
		name: "create_coll_schema",
		meta: true,
		run: func(ctx context.Context, d *Database, _ *Collection) error {
			return d.CreateCollection(ctx, "s", crashSchema)
		},
		apply: func(m model) {
			m["app"]["s"] = &modelColl{schema: cloneSchema(crashSchema), indexes: map[string]string{}, docs: map[string]map[string]any{}}
		},
	},
	{
		name: "create_coll_schemaless",
		meta: true,
		run: func(ctx context.Context, d *Database, _ *Collection) error {
			return d.CreateCollection(ctx, "n", nil)
		},
		apply: func(m model) {
			m["app"]["n"] = &modelColl{indexes: map[string]string{}, docs: map[string]map[string]any{}}
		},
	},
	{
		name:  "create_index",
		meta:  true,
		run:   func(ctx context.Context, _ *Database, c *Collection) error { return c.CreateIndex(ctx, "by_v", "v") },
		apply: func(m model) { m["app"]["c"].indexes["by_v"] = "v" },
	},
}

// TestCrashMatrix simulates a crash at every hook point for every
// operation and checks that reopening yields exactly the committed state:
// every acknowledged change, plus the change of the operation that
// crashed once its WAL record was durable (the commit point). Metadata
// hooks fire inside the operation; flush hooks fire in a flush right
// after it. The "torn" variants also lose the tail of the flush's
// unsynced segment write.
func TestCrashMatrix(t *testing.T) {
	points := []hookPoint{hookMetaWrite, hookAfterWALSync, hookAfterSegWrite, hookAfterSegSync}
	for _, p := range points {
		for _, op := range crashOps {
			for _, torn := range []bool{false, true} {
				if torn && p != hookAfterSegWrite {
					continue
				}
				name := string(p) + "/" + op.name
				if torn {
					name += "/torn"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					opts := crashOpts(t.TempDir())
					want := buildCrashBase(t, opts)

					armed := opts
					crashAt(t, &armed, p)
					e := openEngine(t, armed)
					d := mustDB(t, e, "app")
					c := mustColl(t, d, "c")
					opErr := op.run(ctx, d, c)
					if opErr != nil && !errors.Is(opErr, errSimulatedCrash) {
						t.Fatalf("%s: %v", op.name, opErr)
					}
					flushErr := e.flushAll(ctx, false)
					if flushErr != nil && !errors.Is(flushErr, errSimulatedCrash) {
						t.Fatalf("flush: %v", flushErr)
					}
					crashed := errors.Is(opErr, errSimulatedCrash) || errors.Is(flushErr, errSimulatedCrash)
					wantCrash := p == hookAfterSegWrite || p == hookAfterSegSync || (p == hookMetaWrite) == op.meta
					if crashed != wantCrash {
						t.Logf("expected state before the operation:\n%s", want.dump())
						t.Fatalf("crashed = %v, want %v at hook %s (op error %v, flush error %v)", crashed, wantCrash, p, opErr, flushErr)
					}
					e.abandon()
					if torn {
						chopTail(t, newestDataSegment(t, c.dir), 3)
					}
					op.apply(want)

					e = openEngine(t, opts)
					checkModel(t, e, want)

					// The recovered engine keeps working and its state
					// survives a clean restart.
					mustInsert(t, mustColl(t, mustDB(t, e, "app"), "c"), "after", map[string]any{"g": "x"})
					want["app"]["c"].docs["after"] = map[string]any{"g": "x"}
					e = reopen(t, e)
					checkModel(t, e, want)
				})
			}
		}
	}
}
