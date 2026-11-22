package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Tests of UpdateMany, DeleteMany, CopyCollection through batches, and the
// flush of the WAL payload (plan 013 §3.1, §3.2, AC-13).

// newManyColl returns collection app/c holding n documents d000.. with
// g = i%3 and n = i, opened with mod applied to the options.
func newManyColl(t *testing.T, mod func(*Options), n int) (*Engine, *Collection) {
	t.Helper()
	e, c := newTestColl(t, mod, nil)
	for i := range n {
		mustInsert(t, c, fmt.Sprintf("d%03d", i), map[string]any{"g": float64(i % 3), "n": float64(i)})
	}
	return e, c
}

// idsWhere returns the sorted ids of the documents of c matching f.
func idsWhere(t *testing.T, c *Collection, f ...QueryFilter) []string {
	t.Helper()
	docs, err := c.Find(context.Background(), &Query{Filters: f})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	slices.Sort(ids)
	return ids
}

// walOps returns the ops of the WAL records from offset from on.
func walOps(t *testing.T, e *Engine, from uint64) []wal.Op {
	t.Helper()
	var ops []wal.Op
	if err := e.wal.ReadFrom(from, func(r wal.Record) error {
		ops = append(ops, r.Op)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ops
}

func TestUpdateMany(t *testing.T) {
	ctx := context.Background()
	e, c := newManyColl(t, nil, 30)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, c) // some matches flushed, some in the memtable
	mustInsert(t, c, "m1", map[string]any{"g": 1.0, "n": -1.0})

	var matched, modified int
	err := expectOneRecord(t, e, wal.OpBatch, func() error {
		var err error
		matched, modified, err = c.UpdateMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 1)}},
			UpdateSpec{Set: map[string]any{"x": "y", "at": map[string]any{"$now": true}}, Unset: []string{"n"}}, 0)
		return err
	})
	if err != nil || matched != 11 || modified != 11 {
		t.Fatalf("UpdateMany = %d, %d, %v; want 11, 11", matched, modified, err)
	}
	docs, err := c.Find(ctx, &Query{Filters: []QueryFilter{eqf("x", "y")}})
	if err != nil || len(docs) != 11 {
		t.Fatalf("%d updated documents (%v), want 11", len(docs), err)
	}
	at := docs[0].Data["at"]
	for _, d := range docs {
		if _, ok := d.Data["n"]; ok || d.Data["at"] != at || d.Data["g"] != 1.0 {
			t.Errorf("updated document %s = %v", d.ID, d.Data)
		}
	}
	if s, _ := at.(string); s == "" {
		t.Errorf("$now gave %v", at)
	}
	if got := idsWhere(t, c, eqf("g", 0)); len(got) != 10 {
		t.Errorf("%d documents with g=0 left, want 10", len(got))
	}
	for _, d := range mustFind(t, c) {
		if d.Data["g"] != 1.0 && d.Data["x"] != nil {
			t.Errorf("non-matching document %s changed: %v", d.ID, d.Data)
		}
	}

	// A limit takes the first matches in _id order.
	matched, modified, err = c.UpdateMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 0)}}, UpdateSpec{Set: map[string]any{"l": true}}, 3)
	if err != nil || matched != 3 || modified != 3 {
		t.Fatalf("UpdateMany with limit 3 = %d, %d, %v", matched, modified, err)
	}
	if got := idsWhere(t, c, eqf("l", true)); !reflect.DeepEqual(got, []string{"d000", "d003", "d006"}) {
		t.Errorf("limit 3 changed %v, want the first three ids", got)
	}
	// u.If is ANDed into the query.
	matched, modified, err = c.UpdateMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 0)}}, UpdateSpec{Set: map[string]any{"z": 1.0}, If: Condition{eqf("l", true)}}, 0)
	if err != nil || matched != 3 || modified != 3 {
		t.Errorf("UpdateMany with If = %d, %d, %v; want 3, 3", matched, modified, err)
	}
	// OR groups, and a nil query matching everything.
	matched, _, err = c.UpdateMany(ctx, &Query{Any: [][]QueryFilter{{eqf("n", 3)}, {eqf("n", 5)}}}, UpdateSpec{Set: map[string]any{"a": 1.0}}, 0)
	if err != nil || matched != 2 {
		t.Errorf("UpdateMany with any = %d, %v; want 2", matched, err)
	}
	matched, modified, err = c.UpdateMany(ctx, nil, UpdateSpec{Set: map[string]any{"all": true}}, 0)
	if err != nil || matched != 31 || modified != 31 {
		t.Errorf("UpdateMany(nil query) = %d, %d, %v; want 31", matched, modified, err)
	}
	// No match writes nothing.
	err = expectOneRecord(t, e, 0, func() error {
		var err error
		matched, modified, err = c.UpdateMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 9)}}, UpdateSpec{Set: map[string]any{"a": 1}}, 0)
		return err
	})
	if err != nil || matched != 0 || modified != 0 {
		t.Errorf("UpdateMany without matches = %d, %d, %v", matched, modified, err)
	}

	// The changes are durable and indexed, after a flush and a reopen.
	want := docsOf(t, c)
	mustFlush(t, c)
	e, c = reopenColl(t, e)
	if got := docsOf(t, c); !reflect.DeepEqual(got, want) {
		t.Errorf("after a reopen the documents differ")
	}
	c.db.mu.RLock()
	checkIndexEntries(t, c, mustFind(t, c))
	c.db.mu.RUnlock()
	_ = e
}

func TestUpdateManyRejects(t *testing.T) {
	ctx := context.Background()
	e, c := newManyColl(t, nil, 3)
	set := UpdateSpec{Set: map[string]any{"a": 1}}
	tests := []struct {
		name    string
		q       *Query
		u       UpdateSpec
		wantErr error
	}{
		{"sort", &Query{Sort: []SortKey{{Field: "g"}}}, set, nil},
		{"fields", &Query{Fields: []string{"g"}}, set, nil},
		{"skip", &Query{Skip: 1}, set, nil},
		{"query limit", &Query{Limit: 1}, set, nil},
		{"upsert", nil, UpdateSpec{Set: map[string]any{"a": 1}, Upsert: true}, nil},
		{"set _id", nil, UpdateSpec{Set: map[string]any{"_id": "x"}}, ErrSchemaValidation},
		{"unset _id", nil, UpdateSpec{Unset: []string{"_id"}}, ErrSchemaValidation},
		{"set and unset", nil, UpdateSpec{Set: map[string]any{"a": 1}, Unset: []string{"a"}}, ErrSchemaValidation},
		{"bad operator", &Query{Filters: []QueryFilter{{Field: "g", Operator: "bogus", Value: 1}}}, set, nil},
		{"bad if", nil, UpdateSpec{Set: map[string]any{"a": 1}, If: Condition{{Field: "g", Operator: "bogus", Value: 1}}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := expectOneRecord(t, e, 0, func() error {
				_, _, err := c.UpdateMany(ctx, tt.q, tt.u, 0)
				return err
			})
			if err == nil {
				t.Fatal("UpdateMany accepted it")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error %v does not wrap %v", err, tt.wantErr)
			}
		})
	}
	for _, q := range []*Query{{Sort: []SortKey{{Field: "g"}}}, {Fields: []string{"g"}}, {Skip: 1}, {Limit: 1}, {Filters: []QueryFilter{{Field: "g", Operator: "bogus", Value: 1}}}} {
		if _, _, err := c.DeleteMany(ctx, q, 0); err == nil {
			t.Errorf("DeleteMany accepted query %+v", q)
		}
	}
	if c.Count() != 3 {
		t.Errorf("Count = %d after rejected calls", c.Count())
	}
}

func TestDeleteMany(t *testing.T) {
	ctx := context.Background()
	e, c := newManyColl(t, nil, 30)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, c)
	var matched, deleted int
	err := expectOneRecord(t, e, wal.OpBatch, func() error {
		var err error
		matched, deleted, err = c.DeleteMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 2)}}, 0)
		return err
	})
	if err != nil || matched != 10 || deleted != 10 {
		t.Fatalf("DeleteMany = %d, %d, %v; want 10, 10", matched, deleted, err)
	}
	if got := idsWhere(t, c, eqf("g", 2)); len(got) != 0 || c.Count() != 20 {
		t.Errorf("after DeleteMany: g=2 holds %v, Count %d", got, c.Count())
	}
	matched, deleted, err = c.DeleteMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 1)}}, 4)
	if err != nil || matched != 4 || deleted != 4 {
		t.Fatalf("DeleteMany with limit 4 = %d, %d, %v", matched, deleted, err)
	}
	if got := idsWhere(t, c, eqf("g", 1)); len(got) != 6 || got[0] != "d013" {
		t.Errorf("limit 4 left %v, want d013.. (the first four ids deleted)", got)
	}
	err = expectOneRecord(t, e, 0, func() error {
		var err error
		matched, deleted, err = c.DeleteMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 2)}}, 0)
		return err
	})
	if err != nil || matched != 0 || deleted != 0 {
		t.Errorf("DeleteMany without matches = %d, %d, %v", matched, deleted, err)
	}
	want := docsOf(t, c)
	e, c = reopenColl(t, e)
	if got := docsOf(t, c); !reflect.DeepEqual(got, want) {
		t.Errorf("after a reopen: %d documents, want %d", len(got), len(want))
	}
	c.db.mu.RLock()
	checkIndexEntries(t, c, mustFind(t, c))
	c.db.mu.RUnlock()
	_ = e
}

// TestWriteManyLimits: more matches than MaxBatchDocs without a limit fail
// with the match count and write nothing; a limit of at most MaxBatchDocs
// works through the set in chunks.
func TestWriteManyLimits(t *testing.T) {
	ctx := context.Background()
	e, c := newManyColl(t, func(o *Options) { o.MaxBatchDocs = 5 }, 12)
	for _, run := range []struct {
		name string
		fn   func(limit int) (int, int, error)
	}{
		{"UpdateMany", func(limit int) (int, int, error) {
			return c.UpdateMany(ctx, nil, UpdateSpec{Set: map[string]any{"x": 1}}, limit)
		}},
		{"DeleteMany", func(limit int) (int, int, error) { return c.DeleteMany(ctx, nil, limit) }},
	} {
		t.Run(run.name, func(t *testing.T) {
			// A limit above MaxBatchDocs counts as no limit (documented on
			// UpdateMany).
			for _, limit := range []int{0, -1, 50} {
				var matched, changed int
				err := expectOneRecord(t, e, 0, func() error {
					var err error
					matched, changed, err = run.fn(limit)
					return err
				})
				if !errors.Is(err, ErrBatchTooLarge) || matched != 12 || changed != 0 ||
					!strings.Contains(err.Error(), "12 documents match, more than 5 (MaxBatchDocs)") {
					t.Errorf("limit %d: %d, %d, %v; want 12 matches and ErrBatchTooLarge", limit, matched, changed, err)
				}
			}
			if c.Count() != 12 {
				t.Errorf("Count = %d after rejected calls", c.Count())
			}
		})
	}
	// Exactly MaxBatchDocs matches are fine.
	if m, n, err := c.UpdateMany(ctx, &Query{Filters: []QueryFilter{{Field: "n", Operator: "lt", Value: 5}}}, UpdateSpec{Set: map[string]any{"five": true}}, 0); err != nil || m != 5 || n != 5 {
		t.Errorf("5 matches with MaxBatchDocs 5 = %d, %d, %v", m, n, err)
	}
	// A limit of MaxBatchDocs works through the collection in chunks.
	total, rounds := 0, 0
	for {
		_, n, err := c.DeleteMany(ctx, nil, 5)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		total, rounds = total+n, rounds+1
	}
	if total != 12 || rounds != 3 || c.Count() != 0 {
		t.Errorf("chunked deletes removed %d in %d rounds, Count %d", total, rounds, c.Count())
	}

	// The payload limit applies to the batch of UpdateMany too.
	e2, c2 := newManyColl(t, func(o *Options) { o.MaxBatchBytes = 200 }, 10)
	err := expectOneRecord(t, e2, 0, func() error {
		_, _, err := c2.UpdateMany(ctx, nil, UpdateSpec{Set: map[string]any{"pad": strings.Repeat("x", 50)}}, 0)
		return err
	})
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Errorf("UpdateMany over MaxBatchBytes: %v", err)
	}
}

// TestWriteManySkipsChangedDocuments: a document that stops matching (or
// is deleted) between the snapshot and the write is skipped, not an error.
func TestWriteManySkipsChangedDocuments(t *testing.T) {
	ctx := context.Background()
	var (
		armed func()
		crash bool
	)
	e, c := newManyColl(t, func(o *Options) {
		o.testHook = func(p hookPoint) error {
			if p != hookAfterMatch { // flusher hooks run on other goroutines
				return nil
			}
			if crash {
				return errSimulatedCrash
			}
			if armed != nil {
				f := armed
				armed = nil
				f()
			}
			return nil
		}
	}, 12)
	armed = func() {
		if _, err := c.Update(ctx, "d001", map[string]any{"g": 2.0}); err != nil {
			t.Error(err)
		}
		if err := c.Delete(ctx, "d004"); err != nil {
			t.Error(err)
		}
		if _, err := c.Update(ctx, "d007", map[string]any{"other": true}); err != nil { // still matches
			t.Error(err)
		}
	}
	matched, modified, err := c.UpdateMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 1)}}, UpdateSpec{Set: map[string]any{"hit": true}}, 0)
	if err != nil || matched != 4 || modified != 2 {
		t.Fatalf("UpdateMany with changed documents = %d, %d, %v; want 4, 2", matched, modified, err)
	}
	if got := idsWhere(t, c, eqf("hit", true)); !reflect.DeepEqual(got, []string{"d007", "d010"}) {
		t.Errorf("updated %v, want [d007 d010]", got)
	}
	if d := mustGet(t, c, "d007"); d.Data["other"] != true {
		t.Errorf("d007 lost the concurrent change: %v", d.Data)
	}

	armed = func() {
		if _, err := c.Update(ctx, "d000", map[string]any{"g": 7.0}); err != nil {
			t.Error(err)
		}
	}
	matched, deleted, err := c.DeleteMany(ctx, &Query{Filters: []QueryFilter{eqf("g", 0)}}, 0)
	if err != nil || matched != 4 || deleted != 3 {
		t.Fatalf("DeleteMany with a changed document = %d, %d, %v; want 4, 3", matched, deleted, err)
	}
	if _, err := c.Get(ctx, "d000"); err != nil {
		t.Errorf("the changed document was deleted: %v", err)
	}

	// Every write skipped: nothing is written.
	armed = func() {
		if err := c.Delete(ctx, "d002"); err != nil {
			t.Error(err)
		}
	}
	next := e.wal.NextOffset()
	matched, deleted, err = c.DeleteMany(ctx, &Query{Filters: []QueryFilter{eqf("n", 2)}}, 0)
	if err != nil || matched != 1 || deleted != 0 {
		t.Fatalf("DeleteMany with every write skipped = %d, %d, %v", matched, deleted, err)
	}
	if ops := walOps(t, e, next); !reflect.DeepEqual(ops, []wal.Op{wal.OpDeleteDoc}) {
		t.Errorf("WAL records written: %v, want only the hook's delete", ops)
	}

	// A crash at the hook stops the call before anything is written.
	crash = true
	next = e.wal.NextOffset()
	if _, _, err := c.UpdateMany(ctx, nil, UpdateSpec{Set: map[string]any{"y": 1}}, 0); !errors.Is(err, errSimulatedCrash) {
		t.Errorf("UpdateMany with a crash after the match = %v", err)
	}
	if _, _, err := c.DeleteMany(ctx, nil, 0); !errors.Is(err, errSimulatedCrash) {
		t.Errorf("DeleteMany with a crash after the match = %v", err)
	}
	if e.wal.NextOffset() != next {
		t.Error("a call that crashed after the match wrote to the WAL")
	}
}

// TestWriteManyInvalidResult: a document the update would make invalid
// fails the whole call with that document's error (not a *BatchItemError)
// and nothing is written.
func TestWriteManyInvalidResult(t *testing.T) {
	ctx := context.Background()
	e, c := newManyColl(t, nil, 6)
	if err := c.CreateIndex(ctx, "u", "k", Unique(true)); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "k1", map[string]any{"k": "taken", "g": 5.0})
	for _, tt := range []struct {
		name string
		q    *Query
		set  map[string]any
	}{
		{"two documents get one value", &Query{Filters: []QueryFilter{eqf("g", 1)}}, map[string]any{"k": "same"}},
		{"a value another document holds", &Query{Filters: []QueryFilter{eqf("n", 1)}}, map[string]any{"k": "taken"}},
	} {
		err := expectOneRecord(t, e, 0, func() error {
			_, _, err := c.UpdateMany(ctx, tt.q, UpdateSpec{Set: tt.set}, 0)
			return err
		})
		if !errors.Is(err, ErrUniqueViolation) {
			t.Errorf("%s: %v, want ErrUniqueViolation", tt.name, err)
		}
		if _, ok := errors.AsType[*BatchItemError](err); ok {
			t.Errorf("%s: the error is a *BatchItemError: %v", tt.name, err)
		}
	}
	if err := c.UpdateSchema(ctx, &Schema{Fields: map[string]Field{"g": {Type: TypeNumber}}}); err != nil {
		t.Fatal(err)
	}
	err := expectOneRecord(t, e, 0, func() error {
		_, _, err := c.UpdateMany(ctx, nil, UpdateSpec{Set: map[string]any{"g": "text"}}, 0)
		return err
	})
	if !errors.Is(err, ErrSchemaValidation) {
		t.Errorf("UpdateMany against the schema: %v", err)
	}
	if _, _, err := c.UpdateMany(ctx, nil, UpdateSpec{Set: map[string]any{"g": 1.0}}, 0); err != nil {
		t.Errorf("a valid UpdateMany after the failures: %v", err)
	}
}

func TestWriteManyClosedAndCancelled(t *testing.T) {
	ctx := context.Background()
	e, c := newManyColl(t, nil, 3)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := c.UpdateMany(cctx, nil, UpdateSpec{Set: map[string]any{"a": 1}}, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("UpdateMany with a cancelled context: %v", err)
	}
	if _, _, err := c.DeleteMany(cctx, nil, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("DeleteMany with a cancelled context: %v", err)
	}
	if c.Count() != 3 {
		t.Errorf("Count = %d", c.Count())
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.UpdateMany(ctx, nil, UpdateSpec{Set: map[string]any{"a": 1}}, 0); !errors.Is(err, ErrClosed) {
		t.Errorf("UpdateMany after Close: %v", err)
	}
	if _, _, err := c.DeleteMany(ctx, nil, 0); !errors.Is(err, ErrClosed) {
		t.Errorf("DeleteMany after Close: %v", err)
	}
}

// TestFlushWritesWALPayload: the memtable keeps the WAL payload, charges
// memEntrySize for it, and the flush writes exactly those bytes.
func TestFlushWritesWALPayload(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, func(o *Options) { o.FlushInterval = 1 << 62 }, nil)
	mustInsert(t, c, "a", map[string]any{"big": 12345678901234567890.0, "s": "<&>", "nested": map[string]any{"z": 1, "a": []any{1, "x"}}})
	if _, err := c.Batch(ctx, []WriteOp{{Kind: OpInsert, Document: &Document{ID: "b", Data: map[string]any{"u": "é"}}}, {Kind: OpUpdate, ID: "a", Update: UpdateSpec{Set: map[string]any{"v": 2}}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "c", map[string]any{"x": 1})
	payloads := map[string][]byte{}
	c.mu.RLock()
	for id, me := range c.memtable {
		if me.size != memEntrySize(id, me.payload) {
			t.Errorf("entry %q costs %d, want memEntrySize %d", id, me.size, memEntrySize(id, me.payload))
		}
		if me.doc != nil {
			payloads[id] = bytes.Clone(me.payload)
		} else if me.payload != nil {
			t.Errorf("tombstone %q keeps payload %q", id, me.payload)
		}
	}
	c.mu.RUnlock()
	if len(payloads) != 2 {
		t.Fatalf("memtable puts: %v", payloads)
	}
	// The kept payload is the WAL record's bytes.
	var walA []byte
	if err := e.wal.ReadFrom(1, func(r wal.Record) error {
		if r.Op == wal.OpBatch {
			items, err := wal.DecodeBatch(r.Payload)
			if err != nil {
				return err
			}
			for _, it := range items {
				if it.ID == "a" {
					walA = bytes.Clone(it.Doc)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(walA, payloads["a"]) {
		t.Errorf("memtable payload of a = %s, the WAL batch item holds %s", payloads["a"], walA)
	}
	mustFlush(t, c)
	if got := e.pending.load(); got != 0 {
		t.Errorf("pending bytes after the flush = %d", got)
	}
	flushed := map[string][]byte{}
	for _, r := range segRecords(t, c.dir) {
		if r.Type == segment.TypePut {
			flushed[r.ID] = r.Doc
		}
	}
	for id, p := range payloads {
		if !bytes.Equal(flushed[id], p) {
			t.Errorf("segment record of %q = %s, want the WAL payload %s", id, flushed[id], p)
		}
	}
	// Reads from the segment decode to the same document.
	e, c = reopenColl(t, e)
	for id, p := range payloads {
		want, err := decodeDocument(id, p)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustGet(t, c, id); !reflect.DeepEqual(got, want) {
			t.Errorf("%s after reopen = %v, want %v", id, got, want)
		}
	}

	// A put entry without a payload is refused instead of written empty.
	if _, _, err := flushRecords(map[string]memEntry{"x": {doc: &Document{ID: "x"}, walOffset: 7}}); err == nil || !strings.Contains(err.Error(), "no encoded payload") {
		t.Errorf("flushRecords of a put without payload: %v", err)
	}
	_ = e
}

// TestReplayedEntriesKeepPayload: entries rebuilt by WAL replay (single and
// batch records) keep the payload and flush it.
func TestReplayedEntriesKeepPayload(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	mustInsert(t, c, "s", map[string]any{"v": 1})
	if _, err := c.Batch(ctx, []WriteOp{{Kind: OpInsert, Document: &Document{ID: "b", Data: map[string]any{"v": 2}}}}); err != nil {
		t.Fatal(err)
	}
	e.abandon()
	e, c = openColl(t, opts)
	c.mu.RLock()
	for _, id := range []string{"s", "b"} {
		me := c.memtable[id]
		if len(me.payload) == 0 || me.size != memEntrySize(id, me.payload) {
			t.Errorf("replayed entry %q: payload %q, size %d", id, me.payload, me.size)
		}
	}
	c.mu.RUnlock()
	mustFlush(t, c)
	e, c = reopenColl(t, e)
	if mustGet(t, c, "b").Data["v"] != 2.0 || mustGet(t, c, "s").Data["v"] != 1.0 {
		t.Error("replayed entries did not flush their documents")
	}
	_ = e
}

// TestCopyCollectionBatches: the copy goes through batches chunked by
// MaxBatchDocs and the estimated payload size; a chunk whose real payload
// is over MaxBatchBytes is split in halves, and a single document over it
// is inserted alone.
func TestCopyCollectionBatches(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, func(o *Options) { o.MaxBatchDocs = 7; o.MaxBatchBytes = 600 }, nil)
	if err := c.CreateIndex(ctx, "by_i", "i"); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		// '<' is escaped as <: the estimate is 6 times too low.
		mustInsert(t, c, fmt.Sprintf("d%03d", i), map[string]any{"i": float64(i), "pad": strings.Repeat("<", 10*(i%5)), "t": []any{true, nil, 1.5}, "o": map[string]any{"k": "v"}})
	}
	mustInsert(t, c, "huge", map[string]any{"pad": strings.Repeat("x", 2000)})
	mustFlush(t, c)
	if err := c.Delete(ctx, "d039"); err != nil {
		t.Fatal(err)
	}
	d := mustDB(t, e, "app")
	next := e.wal.NextOffset()
	n, err := d.CopyCollection(ctx, "c", "dst", nil)
	if err != nil || n != 40 {
		t.Fatalf("CopyCollection = %d, %v; want 40", n, err)
	}
	dst := mustColl(t, d, "dst")
	if want, got := mustFind(t, c), mustFind(t, dst); !reflect.DeepEqual(want, got) {
		t.Errorf("the copy differs: %d vs %d documents", len(got), len(want))
	}
	batches, puts := 0, 0
	for _, op := range walOps(t, e, next) {
		switch op {
		case wal.OpBatch:
			batches++
		case wal.OpPutDoc:
			puts++
		}
	}
	if puts != 1 {
		t.Errorf("%d single inserts, want 1 (the document over MaxBatchBytes)", puts)
	}
	if batches < 39/7+1 {
		t.Errorf("%d batch records for 39 documents with MaxBatchDocs 7", batches)
	}
	dst.db.mu.RLock()
	checkIndexEntries(t, dst, mustFind(t, dst))
	dst.db.mu.RUnlock()

	// Large limits: one batch for the whole collection.
	e2, c2 := newManyColl(t, nil, 100)
	next = e2.wal.NextOffset()
	if n, err := mustDB(t, e2, "app").CopyCollection(ctx, "c", "dst", nil); err != nil || n != 100 {
		t.Fatalf("CopyCollection = %d, %v", n, err)
	}
	if ops := walOps(t, e2, next); slices.Index(ops, wal.OpBatch) < 0 || len(ops) != 2 { // create_coll + one batch
		t.Errorf("WAL records of a copy of 100 small documents: %v", ops)
	}
	if got := mustColl(t, mustDB(t, e2, "app"), "dst").Count(); got != c2.Count() {
		t.Errorf("copy holds %d documents, want %d", got, c2.Count())
	}
}

func TestApproxJSONSize(t *testing.T) {
	v := map[string]any{"s": "abc", "n": 1.5, "b": true, "z": nil, "a": []any{"x", 2.0}, "o": map[string]any{}}
	// {"s":"abc"...}: an estimate within a small factor of the encoding.
	got := approxJSONSize(v)
	if got < 30 || got > 120 {
		t.Errorf("approxJSONSize = %d for %v", got, v)
	}
	for _, tt := range []struct {
		v    any
		want int
	}{{nil, 4}, {true, 5}, {"ab", 4}, {3.0, 16}, {[]any{}, 2}, {map[string]any{}, 2}, {[]any{"a"}, 2 + 3 + 1}, {map[string]any{"k": "v"}, 2 + 1 + 4 + 3}} {
		if got := approxJSONSize(tt.v); got != tt.want {
			t.Errorf("approxJSONSize(%#v) = %d, want %d", tt.v, got, tt.want)
		}
	}
}
