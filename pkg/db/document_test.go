package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// newTestColl opens an engine in a temp dir with database "app" and
// collection "c" (schema s) and returns the collection.
func newTestColl(t *testing.T, opts func(*Options), s *Schema) (*Engine, *Collection) {
	t.Helper()
	o := testOptions(t.TempDir())
	if opts != nil {
		opts(&o)
	}
	e := openEngine(t, o)
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(context.Background(), "c", s); err != nil {
		t.Fatal(err)
	}
	return e, mustColl(t, d, "c")
}

func mustInsert(t *testing.T, c *Collection, id string, data map[string]any) string {
	t.Helper()
	got, err := c.Insert(context.Background(), &Document{ID: id, Data: data})
	if err != nil {
		t.Fatalf("Insert(%q): %v", id, err)
	}
	return got
}

func mustGet(t *testing.T, c *Collection, id string) *Document {
	t.Helper()
	doc, err := c.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	return doc
}

func docIDs(docs []*Document) []string {
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	return ids
}

// seedSegment writes docs as put records into a new data segment seg of c
// and points the keydir at them, as a flush would have (task 10).
func seedSegment(t *testing.T, c *Collection, seg uint32, walOffset uint64, docs ...*Document) {
	t.Helper()
	w, err := segment.Create(c.dir, seg)
	if err != nil {
		t.Fatal(err)
	}
	recs := make([]segment.Record, len(docs))
	for i, d := range docs {
		data, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = segment.Record{Type: segment.TypePut, WALOffset: walOffset, ID: d.ID, Doc: data}
	}
	locs, err := w.Append(recs)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, d := range docs {
		c.keydir[d.ID] = keyEntry{loc: locs[i], walOffset: walOffset}
		c.count.Add(1)
	}
}

func TestInsert(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)

	id, err := c.Insert(ctx, &Document{Data: map[string]any{"a": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Errorf("generated id %q is not a UUID: %v", id, err)
	}
	if got := mustGet(t, c, id); got.ID != id || got.Data["a"] != 1.0 {
		t.Errorf("Get = %+v", got)
	}

	if got := mustInsert(t, c, "x", map[string]any{"b": "v"}); got != "x" {
		t.Errorf("Insert returned %q, want x", got)
	}
	next := e.wal.NextOffset()
	tests := []struct {
		name string
		doc  *Document
		want error
	}{
		{"duplicate", &Document{ID: "x", Data: map[string]any{"b": "w"}}, ErrExists},
		{"reserved _id in data", &Document{ID: "y", Data: map[string]any{"_id": "z"}}, ErrSchemaValidation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := c.Insert(ctx, tt.doc); !errors.Is(err, tt.want) {
				t.Fatalf("Insert = %v, want %v", err, tt.want)
			}
		})
	}
	if _, err := c.Insert(ctx, nil); err == nil {
		t.Error("Insert(nil) succeeded")
	}
	if e.wal.NextOffset() != next {
		t.Error("a rejected insert wrote a WAL record")
	}
	if got := mustGet(t, c, "x"); got.Data["b"] != "v" {
		t.Errorf("duplicate insert changed the document: %+v", got)
	}
	if c.Count() != 2 {
		t.Errorf("Count = %d, want 2", c.Count())
	}

	// The WAL record is the full after-image including _id (§3.1).
	r := walRecord(t, e, next-1)
	if r.Op != wal.OpPutDoc || r.DB != "app" || r.Coll != "c" || r.ID != "x" {
		t.Fatalf("WAL record = %+v", r)
	}
	var payload map[string]any
	if err := json.Unmarshal(r.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"_id": "x", "b": "v"}; !reflect.DeepEqual(payload, want) {
		t.Errorf("payload = %v, want %v", payload, want)
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "u1", map[string]any{"a": 1, "b": "x"})

	got, err := c.Update(ctx, "u1", map[string]any{"b": "y", "c": true})
	if err != nil {
		t.Fatal(err)
	}
	want := &Document{ID: "u1", Data: map[string]any{"a": 1.0, "b": "y", "c": true}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Update returned %+v, want %+v", got, want)
	}
	if g := mustGet(t, c, "u1"); !reflect.DeepEqual(g, want) {
		t.Errorf("Get after Update = %+v, want %+v", g, want)
	}

	next := e.wal.NextOffset()
	tests := []struct {
		name string
		id   string
		set  map[string]any
		want error
	}{
		{"missing document", "nope", map[string]any{"a": 2}, ErrNotFound},
		{"_id change", "u1", map[string]any{"_id": "other"}, ErrSchemaValidation},
		{"_id same value", "u1", map[string]any{"_id": "u1"}, ErrSchemaValidation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := c.Update(ctx, tt.id, tt.set); !errors.Is(err, tt.want) {
				t.Fatalf("Update = %v, want %v", err, tt.want)
			}
		})
	}
	if e.wal.NextOffset() != next {
		t.Error("a rejected update wrote a WAL record")
	}
	if c.Count() != 1 {
		t.Errorf("Count = %d, want 1", c.Count())
	}
}

func TestSchemaValidation(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Fields: map[string]Field{
		"name": {Type: TypeString, Required: true},
		"age":  {Type: TypeNumber},
	}}
	e, c := newTestColl(t, nil, s)
	mustInsert(t, c, "ok", map[string]any{"name": "ann", "age": 3})
	next := e.wal.NextOffset()

	tests := []struct {
		name string
		run  func() error
	}{
		{"insert missing required", func() error {
			_, err := c.Insert(ctx, &Document{ID: "bad", Data: map[string]any{"age": 1}})
			return err
		}},
		{"insert wrong type", func() error {
			_, err := c.Insert(ctx, &Document{ID: "bad", Data: map[string]any{"name": 5}})
			return err
		}},
		{"update wrong type", func() error {
			_, err := c.Update(ctx, "ok", map[string]any{"age": "old"})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, ErrSchemaValidation) {
				t.Fatalf("got %v, want ErrSchemaValidation", err)
			}
		})
	}
	if e.wal.NextOffset() != next {
		t.Error("an invalid document wrote a WAL record")
	}
	if got := mustGet(t, c, "ok"); got.Data["age"] != 3.0 {
		t.Errorf("rejected update changed the document: %+v", got)
	}
	if _, err := c.Get(ctx, "bad"); !errors.Is(err, ErrNotFound) {
		t.Errorf("rejected insert visible: %v", err)
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "d1", map[string]any{"a": 1})
	mustInsert(t, c, "d2", map[string]any{"a": 2})

	if err := c.Delete(ctx, "d1"); err != nil {
		t.Fatal(err)
	}
	if r := walRecord(t, e, e.wal.NextOffset()-1); r.Op != wal.OpDeleteDoc || r.ID != "d1" || len(r.Payload) != 0 {
		t.Errorf("WAL record = %+v", r)
	}
	if _, err := c.Get(ctx, "d1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get deleted = %v, want ErrNotFound", err)
	}
	if err := c.Delete(ctx, "d1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second Delete = %v, want ErrNotFound", err)
	}
	if err := c.Delete(ctx, "never"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete absent = %v, want ErrNotFound", err)
	}
	if c.Count() != 1 {
		t.Errorf("Count = %d, want 1", c.Count())
	}
	// Re-inserting a deleted id works.
	mustInsert(t, c, "d1", map[string]any{"a": 3})
	if c.Count() != 2 {
		t.Errorf("Count = %d, want 2", c.Count())
	}
}

func TestCount(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	seedSegment(t, c, 1, 1, &Document{ID: "s1", Data: map[string]any{}}, &Document{ID: "s2", Data: map[string]any{}})

	steps := []struct {
		name string
		run  func() error
		want int
	}{
		{"seeded", func() error { return nil }, 2},
		{"insert", func() error { _, err := c.Insert(ctx, &Document{ID: "m1"}); return err }, 3},
		{"update memtable doc", func() error { _, err := c.Update(ctx, "m1", map[string]any{"x": 1}); return err }, 3},
		{"update segment doc", func() error { _, err := c.Update(ctx, "s1", map[string]any{"x": 1}); return err }, 3},
		{"delete segment doc", func() error { return c.Delete(ctx, "s2") }, 2},
		{"delete memtable doc", func() error { return c.Delete(ctx, "m1") }, 1},
		{"failed insert", func() error { _, _ = c.Insert(ctx, &Document{ID: "s1"}); return nil }, 1},
		{"failed delete", func() error { _ = c.Delete(ctx, "s2"); return nil }, 1},
	}
	for _, st := range steps {
		if err := st.run(); err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		if got := c.Count(); got != st.want {
			t.Errorf("after %s: Count = %d, want %d", st.name, got, st.want)
		}
	}
	all, err := c.Find(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != c.Count() {
		t.Errorf("Find returned %d documents, Count = %d", len(all), c.Count())
	}
}

// TestWALAppendFailureAppliesNothing closes the WAL under the engine so
// the commit point fails.
func TestWALAppendFailureAppliesNothing(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "a", map[string]any{"v": 1})
	if err := e.wal.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		run  func() error
	}{
		{"insert", func() error { _, err := c.Insert(ctx, &Document{ID: "b"}); return err }},
		{"update", func() error { _, err := c.Update(ctx, "a", map[string]any{"v": 2}); return err }},
		{"delete", func() error { return c.Delete(ctx, "a") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, ErrClosed) {
				t.Fatalf("got %v, want ErrClosed", err)
			}
		})
	}
	if c.Count() != 1 {
		t.Errorf("Count = %d, want 1", c.Count())
	}
	if got := mustGet(t, c, "a"); got.Data["v"] != 1.0 {
		t.Errorf("document changed: %+v", got)
	}
	if _, err := c.Get(ctx, "b"); !errors.Is(err, ErrNotFound) {
		t.Errorf("failed insert visible: %v", err)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.inflight) != 0 {
		t.Errorf("inflight = %v after failed appends, want empty", c.inflight)
	}
	if len(c.memtable) != 1 {
		t.Errorf("memtable has %d entries, want 1", len(c.memtable))
	}
}

func TestWriteCancelledContext(t *testing.T) {
	e, c := newTestColl(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	next := e.wal.NextOffset()
	if _, err := c.Insert(ctx, &Document{ID: "a"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Insert = %v, want context.Canceled", err)
	}
	if e.wal.NextOffset() != next {
		t.Error("cancelled insert wrote a WAL record")
	}
	if c.Count() != 0 {
		t.Errorf("Count = %d, want 0", c.Count())
	}
}

// TestHookAfterWALSync checks that the hook runs after the WAL record is
// durable and before the change is visible, and that a simulated crash
// there leaves memory untouched.
func TestHookAfterWALSync(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		run     func(c *Collection) error
		visible func(t *testing.T, c *Collection) bool
	}{
		{
			name: "insert",
			run: func(c *Collection) error {
				_, err := c.Insert(ctx, &Document{ID: "n", Data: map[string]any{"v": 1}})
				return err
			},
			visible: func(t *testing.T, c *Collection) bool {
				_, err := c.Get(ctx, "n")
				return err == nil
			},
		},
		{
			name: "update",
			run:  func(c *Collection) error { _, err := c.Update(ctx, "base", map[string]any{"v": 2}); return err },
			visible: func(t *testing.T, c *Collection) bool {
				return mustGet(t, c, "base").Data["v"] == 2.0
			},
		},
		{
			name: "delete",
			run:  func(c *Collection) error { return c.Delete(ctx, "base") },
			visible: func(t *testing.T, c *Collection) bool {
				_, err := c.Get(ctx, "base")
				return errors.Is(err, ErrNotFound)
			},
		},
	}
	for _, tt := range tests {
		for _, crash := range []bool{false, true} {
			name := tt.name
			if crash {
				name += "/crash"
			}
			t.Run(name, func(t *testing.T) {
				var (
					armed  bool
					called int
					c      *Collection
					e      *Engine
					before uint64
				)
				e, c = newTestColl(t, func(o *Options) {
					o.testHook = func(p hookPoint) error {
						if p != hookAfterWALSync || !armed { // p first: the flusher calls the hook from another goroutine
							return nil
						}
						called++
						if got := e.wal.NextOffset(); got != before+1 {
							t.Errorf("at hook NextOffset = %d, want %d (record durable)", got, before+1)
						}
						if tt.visible(t, c) {
							t.Error("change visible before apply")
						}
						c.mu.RLock()
						if len(c.inflight) != 1 || c.inflight[0] > before {
							t.Errorf("at hook inflight = %v, want one bound <= %d", c.inflight, before)
						}
						c.mu.RUnlock()
						if crash {
							return errSimulatedCrash
						}
						return nil
					}
				}, nil)
				mustInsert(t, c, "base", map[string]any{"v": 1})
				countBefore := c.Count()
				before = e.wal.NextOffset()

				armed = true
				err := tt.run(c)
				armed = false
				if called != 1 {
					t.Fatalf("hook called %d times, want 1", called)
				}
				if crash {
					if !errors.Is(err, errSimulatedCrash) {
						t.Fatalf("got %v, want errSimulatedCrash", err)
					}
					if tt.visible(t, c) {
						t.Error("change visible after a crash before apply")
					}
					if c.Count() != countBefore {
						t.Errorf("Count changed after crash: %d -> %d", countBefore, c.Count())
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !tt.visible(t, c) {
					t.Error("change not visible after success")
				}
				c.mu.RLock()
				defer c.mu.RUnlock()
				if len(c.inflight) != 0 {
					t.Errorf("inflight = %v after apply, want empty", c.inflight)
				}
			})
		}
	}
}

// TestConcurrentUpdatesSameDocument checks that the doc lock serializes
// read-modify-write: no increment and no merged field is lost.
func TestConcurrentUpdatesSameDocument(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "ctr", map[string]any{"n": 0})
	mustInsert(t, c, "fields", nil)

	const workers, perWorker = 16, 25
	var wg sync.WaitGroup
	errs := make(chan error, 2*workers)
	for w := range workers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range perWorker {
				_, err := c.write(ctx, "ctr", func(cur *Document) (*Document, error) {
					if cur == nil {
						return nil, ErrNotFound
					}
					n, _ := cur.Data["n"].(float64)
					return &Document{ID: "ctr", Data: map[string]any{"n": n + 1}}, nil
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := c.Update(ctx, "fields", map[string]any{fmt.Sprintf("f%02d", w): w}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := mustGet(t, c, "ctr").Data["n"]; got != float64(workers*perWorker) {
		t.Errorf("n = %v, want %d (lost updates)", got, workers*perWorker)
	}
	if got := len(mustGet(t, c, "fields").Data); got != workers {
		t.Errorf("fields has %d keys, want %d (lost updates)", got, workers)
	}
}

func TestConcurrentWritesDifferentDocuments(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	const workers, perWorker = 8, 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				id := fmt.Sprintf("w%d-%02d", w, i)
				if _, err := c.Insert(ctx, &Document{ID: id, Data: map[string]any{"g": w % 2}}); err != nil {
					errs <- err
					return
				}
				if i%2 == 1 {
					if err := c.Delete(ctx, id); err != nil {
						errs <- err
						return
					}
				}
				if _, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "g", Operator: "eq", Value: 1}}, Limit: 5}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if want := workers * perWorker / 2; c.Count() != want {
		t.Errorf("Count = %d, want %d", c.Count(), want)
	}
	got, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "g", Operator: "eq", Value: 0}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := workers / 2 * perWorker / 2; len(got) != want {
		t.Errorf("indexed Find returned %d, want %d", len(got), want)
	}
}

func TestFind(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	for _, idx := range [][2]string{{"by_city", "city"}, {"by_kind", "kind"}, {"by_tag", "tag"}} {
		if err := c.CreateIndex(ctx, idx[0], idx[1]); err != nil {
			t.Fatal(err)
		}
	}
	// Inserted out of order; Find must sort by _id.
	docs := []struct {
		id   string
		data map[string]any
	}{
		{"k", map[string]any{"city": "A", "kind": "x", "n": 9, "tag": "dup"}},
		{"c", map[string]any{"city": "A", "kind": "y", "n": 10, "tag": "dup"}},
		{"h", map[string]any{"city": "A", "kind": "y", "n": 3, "tag": "dup"}},
		{"a", map[string]any{"city": "A", "kind": "x", "n": 1, "tag": "one"}},
		{"f", map[string]any{"city": "A", "kind": "y", "n": "9"}},
		{"b", map[string]any{"city": "B", "kind": "y", "obj": map[string]any{"z": 1}}},
		{"j", map[string]any{"city": "B", "kind": "y", "n": 5}},
	}
	for _, d := range docs {
		mustInsert(t, c, d.id, d.data)
	}
	eq := func(field string, v any) QueryFilter { return QueryFilter{Field: field, Operator: "eq", Value: v} }

	tests := []struct {
		name string
		q    *Query
		want []string
	}{
		{"nil query, sorted by _id", nil, []string{"a", "b", "c", "f", "h", "j", "k"}},
		{"indexed eq", &Query{Filters: []QueryFilter{eq("city", "B")}}, []string{"b", "j"}},
		{"two indexed eq", &Query{Filters: []QueryFilter{eq("city", "A"), eq("kind", "x")}}, []string{"a", "k"}},
		{"F12 duplicate indexed values", &Query{Filters: []QueryFilter{eq("tag", "dup")}}, []string{"c", "h", "k"}},
		{"F11 numeric gt", &Query{Filters: []QueryFilter{{Field: "n", Operator: "gt", Value: 5}}}, []string{"c", "k"}},
		{"non-indexed lte", &Query{Filters: []QueryFilter{{Field: "n", Operator: "lte", Value: 5}}}, []string{"a", "h", "j"}},
		{"indexed eq plus scan filter", &Query{Filters: []QueryFilter{eq("city", "A"), {Field: "n", Operator: "gte", Value: 9}}}, []string{"c", "k"}},
		{"eq number vs string types", &Query{Filters: []QueryFilter{eq("n", "9")}}, []string{"f"}},
		{"eq object falls back to scan", &Query{Filters: []QueryFilter{eq("obj", map[string]any{"z": 1})}}, []string{"b"}},
		{"indexed eq no match", &Query{Filters: []QueryFilter{eq("city", "Z")}}, []string{}},
		{"_id eq", &Query{Filters: []QueryFilter{eq("_id", "h")}}, []string{"h"}},
		{"_id eq non-string", &Query{Filters: []QueryFilter{eq("_id", 1)}}, []string{}},
		{"in", &Query{Filters: []QueryFilter{{Field: "kind", Operator: "in", Value: []any{"x"}}}}, []string{"a", "k"}},
		{"skip and limit", &Query{Skip: 2, Limit: 3}, []string{"c", "f", "h"}},
		{"skip past end", &Query{Skip: 10}, []string{}},
		{"limit with index", &Query{Filters: []QueryFilter{eq("tag", "dup")}, Limit: 2}, []string{"c", "h"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 3 { // deterministic across calls
				got, err := c.Find(ctx, tt.q)
				if err != nil {
					t.Fatal(err)
				}
				if ids := docIDs(got); !slices.Equal(ids, tt.want) {
					t.Fatalf("Find = %v, want %v", ids, tt.want)
				}
			}
		})
	}
}

func TestFindCandidatesAndEarlyExit(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	for _, idx := range [][2]string{{"by_city", "city"}, {"by_kind", "kind"}} {
		if err := c.CreateIndex(ctx, idx[0], idx[1]); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 20 {
		kind := "common"
		if i == 3 || i == 17 {
			kind = "rare"
		}
		mustInsert(t, c, fmt.Sprintf("d%02d", i), map[string]any{"city": "A", "kind": kind, "i": i})
	}
	eq := func(field string, v any) QueryFilter { return QueryFilter{Field: field, Operator: "eq", Value: v} }

	t.Run("smallest indexed eq set", func(t *testing.T) {
		for _, filters := range [][]QueryFilter{
			{eq("city", "A"), eq("kind", "rare")},
			{eq("kind", "rare"), eq("city", "A")},
			{eq("kind", "rare"), eq("unindexed", 1)},
		} {
			ids, err := c.candidates(filters)
			if err != nil {
				t.Fatal(err)
			}
			if want := []string{"d03", "d17"}; !slices.Equal(ids, want) {
				t.Errorf("candidates(%v) = %v, want %v", filters, ids, want)
			}
		}
	})
	t.Run("no indexed eq scans all", func(t *testing.T) {
		ids, err := c.candidates([]QueryFilter{eq("unindexed", 1), {Field: "city", Operator: "ne", Value: "B"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != 20 || !slices.IsSorted(ids) {
			t.Errorf("candidates = %v, want all 20 ids sorted", ids)
		}
	})
	t.Run("early exit at skip+limit", func(t *testing.T) {
		docs, examined, err := c.find(ctx, &Query{Skip: 5, Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"d05", "d06", "d07"}; !slices.Equal(docIDs(docs), want) {
			t.Errorf("Find = %v, want %v", docIDs(docs), want)
		}
		if examined != 8 {
			t.Errorf("examined %d candidates, want 8 (skip+limit)", examined)
		}
	})
	t.Run("indexed set examined only", func(t *testing.T) {
		docs, examined, err := c.find(ctx, &Query{Filters: []QueryFilter{eq("kind", "rare")}})
		if err != nil {
			t.Fatal(err)
		}
		if len(docs) != 2 || examined != 2 {
			t.Errorf("got %d docs after examining %d, want 2 and 2", len(docs), examined)
		}
	})
}

func TestReturnedDocumentsAreDeepClones(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	nested := func() map[string]any {
		return map[string]any{"m": map[string]any{"k": "v"}, "s": []any{1.0, 2.0}}
	}
	input := &Document{ID: "mem", Data: nested()}
	if _, err := c.Insert(ctx, input); err != nil {
		t.Fatal(err)
	}
	input.Data["m"].(map[string]any)["k"] = "changed-input"
	input.Data["s"].([]any)[0] = 99.0
	seedSegment(t, c, 1, 1, &Document{ID: "seg", Data: nested()})

	mutate := func(d *Document) {
		d.Data["m"].(map[string]any)["k"] = "changed"
		d.Data["s"].([]any)[0] = 42.0
		d.Data["new"] = true
	}
	for _, id := range []string{"mem", "seg"} {
		t.Run(id, func(t *testing.T) {
			mutate(mustGet(t, c, id))
			found, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "_id", Operator: "eq", Value: id}}})
			if err != nil || len(found) != 1 {
				t.Fatalf("Find = %v, %v", found, err)
			}
			mutate(found[0])
			if got := mustGet(t, c, id); !reflect.DeepEqual(got.Data, nested()) {
				t.Errorf("stored document changed through a returned copy: %v", got.Data)
			}
		})
	}
	t.Run("update result", func(t *testing.T) {
		got, err := c.Update(ctx, "mem", map[string]any{"x": 1})
		if err != nil {
			t.Fatal(err)
		}
		mutate(got)
		want := nested()
		want["x"] = 1.0
		if g := mustGet(t, c, "mem"); !reflect.DeepEqual(g.Data, want) {
			t.Errorf("stored document changed through Update's result: %v", g.Data)
		}
	})
}

func TestReadFromSegmentAndCache(t *testing.T) {
	ctx := context.Background()
	for _, cacheOn := range []bool{true, false} {
		t.Run(fmt.Sprintf("cache=%v", cacheOn), func(t *testing.T) {
			e, c := newTestColl(t, func(o *Options) {
				if !cacheOn {
					o.CacheSize = -1
				}
			}, nil)
			seedSegment(t, c, 1, 1,
				&Document{ID: "s1", Data: map[string]any{"v": 1}},
				&Document{ID: "s2", Data: map[string]any{"v": 2}},
				&Document{ID: "s3", Data: map[string]any{"v": 3}},
			)
			mustInsert(t, c, "m1", map[string]any{"v": 4})

			if got := mustGet(t, c, "s2"); got.Data["v"] != 2.0 {
				t.Fatalf("Get(s2) = %+v", got)
			}
			_, hit := e.cache.Get(c.cacheKey("s2"), 1)
			if hit != cacheOn {
				t.Errorf("cache hit after read = %v, want %v", hit, cacheOn)
			}
			if got := mustGet(t, c, "s2"); got.Data["v"] != 2.0 { // served by the cache when on
				t.Fatalf("second Get(s2) = %+v", got)
			}

			if _, err := c.Update(ctx, "s2", map[string]any{"v": 20}); err != nil {
				t.Fatal(err)
			}
			if _, hit := e.cache.Get(c.cacheKey("s2"), 1); hit {
				t.Error("cache entry not invalidated by update")
			}
			if got := mustGet(t, c, "s2"); got.Data["v"] != 20.0 {
				t.Errorf("Get after update = %+v", got)
			}
			if err := c.Delete(ctx, "s3"); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Get(ctx, "s3"); !errors.Is(err, ErrNotFound) {
				t.Errorf("tombstone does not hide segment doc: %v", err)
			}
			if _, err := c.Insert(ctx, &Document{ID: "s1"}); !errors.Is(err, ErrExists) {
				t.Errorf("Insert over segment doc = %v, want ErrExists", err)
			}

			all, err := c.Find(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if want := []string{"m1", "s1", "s2"}; !slices.Equal(docIDs(all), want) {
				t.Errorf("Find = %v, want %v", docIDs(all), want)
			}
			gt, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "v", Operator: "gt", Value: 1}}})
			if err != nil {
				t.Fatal(err)
			}
			if want := []string{"m1", "s2"}; !slices.Equal(docIDs(gt), want) {
				t.Errorf("Find v>1 = %v, want %v", docIDs(gt), want)
			}
		})
	}
}

func TestReadFromSegmentMismatch(t *testing.T) {
	_, c := newTestColl(t, nil, nil)
	seedSegment(t, c, 1, 7, &Document{ID: "s1", Data: map[string]any{"v": 1}})
	c.mu.Lock()
	ke := c.keydir["s1"]
	ke.walOffset = 8 // keydir and record disagree
	c.keydir["s1"] = ke
	c.mu.Unlock()
	if _, err := c.Get(context.Background(), "s1"); !errors.Is(err, segment.ErrCorrupt) {
		t.Fatalf("Get = %v, want segment.ErrCorrupt", err)
	}
}

func TestCreateIndexOnPopulatedCollection(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	seedSegment(t, c, 1, 1,
		&Document{ID: "s1", Data: map[string]any{"g": "a"}},
		&Document{ID: "s2", Data: map[string]any{"g": "b"}},
		&Document{ID: "s3", Data: map[string]any{"g": "a"}},
	)
	mustInsert(t, c, "m1", map[string]any{"g": "a"})
	mustInsert(t, c, "m2", map[string]any{"g": []any{"a"}}) // indexed per element (plan 004)
	mustInsert(t, c, "m3", map[string]any{"other": 1})      // field missing
	if err := c.Delete(ctx, "s3"); err != nil {
		t.Fatal(err)
	}

	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	lookup := func(v any) []string {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.indexes["by_g"].lookup(v)
	}
	if got, want := lookup("a"), []string{"m1", "m2", "s1"}; !slices.Equal(got, want) {
		t.Errorf("index a = %v, want %v", got, want)
	}
	if got, want := lookup("b"), []string{"s2"}; !slices.Equal(got, want) {
		t.Errorf("index b = %v, want %v", got, want)
	}

	// Maintained by later writes: update moves the key, delete removes it.
	if _, err := c.Update(ctx, "s1", map[string]any{"g": "b"}); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "m4", map[string]any{"g": "a"})
	if err := c.Delete(ctx, "s2"); err != nil {
		t.Fatal(err)
	}
	if got, want := lookup("a"), []string{"m1", "m2", "m4"}; !slices.Equal(got, want) {
		t.Errorf("index a after writes = %v, want %v", got, want)
	}
	if got, want := lookup("b"), []string{"s1"}; !slices.Equal(got, want) {
		t.Errorf("index b after writes = %v, want %v", got, want)
	}
	// m2 is a candidate through its element key, but eq keeps
	// whole-value semantics: the re-check drops it.
	found, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "g", Operator: "eq", Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"m1", "m4"}; !slices.Equal(docIDs(found), want) {
		t.Errorf("Find = %v, want %v", docIDs(found), want)
	}

	if err := c.DropIndex(ctx, "by_g"); err != nil {
		t.Fatal(err)
	}
	c.mu.RLock()
	_, ok := c.indexes["by_g"]
	c.mu.RUnlock()
	if ok {
		t.Fatal("index still present after DropIndex")
	}
	ids, err := c.candidates([]QueryFilter{{Field: "g", Operator: "eq", Value: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != c.Count() {
		t.Errorf("candidates after drop = %v, want a full scan of %d", ids, c.Count())
	}
	found, err = c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "g", Operator: "eq", Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"m1", "m4"}; !slices.Equal(docIDs(found), want) {
		t.Errorf("Find after drop = %v, want %v", docIDs(found), want)
	}
}

func TestDocumentOpsAfterDropAndClose(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	seedSegment(t, c, 1, 1, &Document{ID: "s1", Data: map[string]any{"v": 1}})
	mustGet(t, c, "s1") // opens the segment reader and fills the cache

	if err := c.db.DropCollection(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if _, hit := e.cache.Get(c.cacheKey("s1"), 1); hit {
		t.Error("cache still holds documents of a dropped collection")
	}
	ops := []struct {
		name string
		run  func() error
	}{
		{"insert", func() error { _, err := c.Insert(ctx, &Document{ID: "x"}); return err }},
		{"get", func() error { _, err := c.Get(ctx, "s1"); return err }},
		{"update", func() error { _, err := c.Update(ctx, "s1", nil); return err }},
		{"delete", func() error { return c.Delete(ctx, "s1") }},
		{"find", func() error { _, err := c.Find(ctx, nil); return err }},
	}
	for _, op := range ops {
		t.Run("dropped/"+op.name, func(t *testing.T) {
			if err := op.run(); !errors.Is(err, ErrNotFound) {
				t.Errorf("got %v, want ErrNotFound", err)
			}
		})
	}

	e2, c2 := newTestColl(t, nil, nil)
	mustInsert(t, c2, "a", nil)
	if err := e2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	ops2 := []struct {
		name string
		run  func() error
	}{
		{"insert", func() error { _, err := c2.Insert(ctx, &Document{ID: "x"}); return err }},
		{"get", func() error { _, err := c2.Get(ctx, "a"); return err }},
		{"update", func() error { _, err := c2.Update(ctx, "a", nil); return err }},
		{"delete", func() error { return c2.Delete(ctx, "a") }},
		{"find", func() error { _, err := c2.Find(ctx, nil); return err }},
	}
	for _, op := range ops2 {
		t.Run("closed/"+op.name, func(t *testing.T) {
			if err := op.run(); !errors.Is(err, ErrClosed) {
				t.Errorf("got %v, want ErrClosed", err)
			}
		})
	}
}

// TestCachedDocumentIsNeverShared: the cache holds one decoded document;
// every Get and Find returns a deep clone, so mutating a result (also its
// nested values) never changes what later reads see.
func TestCachedDocumentIsNeverShared(t *testing.T) {
	ctx := context.Background()
	e, c, _ := newFlushColl(t, nil)
	mustInsert(t, c, "a", map[string]any{"n": 1, "tags": []any{"x"}, "obj": map[string]any{"k": "v"}})
	mustFlush(t, c) // now read from the data segment, through the cache

	first := mustGet(t, c, "a")
	ke := func() keyEntry {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.keydir["a"]
	}()
	cached, ok := e.cache.Get(c.cacheKey("a"), ke.walOffset)
	if !ok {
		t.Fatal("document not cached after a Get")
	}
	if cached.doc == first {
		t.Fatal("Get returned the cached document itself")
	}

	mutate := func(d *Document) {
		d.Data["n"] = 99
		d.Data["tags"].([]any)[0] = "changed"
		d.Data["obj"].(map[string]any)["k"] = "changed"
		d.Data["extra"] = true
	}
	mutate(first)
	found, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "n", Operator: "eq", Value: 1}}})
	if err != nil || len(found) != 1 {
		t.Fatalf("Find = %v, %v; want the unchanged document", found, err)
	}
	mutate(found[0])

	want := map[string]any{"n": float64(1), "tags": []any{"x"}, "obj": map[string]any{"k": "v"}}
	for i := range 2 {
		if got := mustGet(t, c, "a"); !reflect.DeepEqual(got.Data, want) {
			t.Fatalf("Get %d after mutating earlier results = %v, want %v", i, got.Data, want)
		}
	}
	if !reflect.DeepEqual(cached.doc.Data, want) {
		t.Fatalf("cached document changed: %v", cached.doc.Data)
	}
}
