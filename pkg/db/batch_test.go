package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Tests of Collection.Batch (plan 013 §3.1, AC-12).

// docsOf returns every document of c as id -> data.
func docsOf(t *testing.T, c *Collection) map[string]map[string]any {
	t.Helper()
	docs, err := c.Find(context.Background(), nil)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	out := make(map[string]map[string]any, len(docs))
	for _, d := range docs {
		out[d.ID] = d.Data
	}
	return out
}

// batchOffset returns the WAL offset of the newest record, which must be a
// batch record.
func lastBatchRecord(t *testing.T, e *Engine) wal.Record {
	t.Helper()
	r := walRecord(t, e, e.wal.NextOffset()-1)
	if r.Op != wal.OpBatch {
		t.Fatalf("newest WAL record is %s, want batch", r.Op)
	}
	return r
}

func TestBatchMixedOperations(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "upd", map[string]any{"g": "a", "v": 1.0})
	mustInsert(t, c, "rep", map[string]any{"g": "a", "v": 2.0})
	mustInsert(t, c, "del", map[string]any{"g": "b", "v": 3.0})
	mustInsert(t, c, "cond", map[string]any{"g": "b", "v": 4.0})
	mustFlush(t, c) // half of the batch replaces flushed versions
	mustInsert(t, c, "mem", map[string]any{"g": "c"})

	ops := []WriteOp{
		{Kind: OpInsert, Document: &Document{ID: "new", Data: map[string]any{"g": "c", "at": map[string]any{"$now": true}}}},
		{Kind: OpInsert, Document: &Document{Data: map[string]any{"g": "gen"}}}, // generated id
		{Kind: OpInsert, ID: "byid", Document: &Document{Data: map[string]any{"g": "c"}}},
		{Kind: OpUpdate, ID: "upd", Update: UpdateSpec{Set: map[string]any{"v": 10.0, "at": map[string]any{"$now": true}}, Unset: []string{"g"}}},
		{Kind: OpUpdate, ID: "ups", Update: UpdateSpec{Set: map[string]any{"g": "u"}, Upsert: true}},
		{Kind: OpReplace, ID: "rep", Document: &Document{Data: map[string]any{"r": true}}},
		{Kind: OpReplace, Document: &Document{ID: "mem", Data: map[string]any{"g": "m2"}}}, // id from the document
		{Kind: OpDelete, ID: "del"},
		{Kind: OpDelete, ID: "cond", If: Condition{eqf("v", 4.0)}},
	}
	var res BatchResult
	err := expectOneRecord(t, e, wal.OpBatch, func() error {
		var err error
		res, err = c.Batch(ctx, ops)
		return err
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if len(res.IDs) != len(ops) || len(res.Upserted) != len(ops) {
		t.Fatalf("result has %d ids and %d upserted flags, want %d", len(res.IDs), len(res.Upserted), len(ops))
	}
	gen := res.IDs[1]
	if len(gen) != 36 {
		t.Errorf("generated id %q is not a UUID", gen)
	}
	wantIDs := []string{"new", gen, "byid", "upd", "ups", "rep", "mem", "del", "cond"}
	if !reflect.DeepEqual(res.IDs, wantIDs) {
		t.Errorf("IDs = %v, want %v", res.IDs, wantIDs)
	}
	if want := []bool{false, false, false, false, true, false, false, false, false}; !reflect.DeepEqual(res.Upserted, want) {
		t.Errorf("Upserted = %v, want %v", res.Upserted, want)
	}

	got := docsOf(t, c)
	at := got["new"]["at"]
	if s, ok := at.(string); !ok || s == "" || got["upd"]["at"] != at {
		t.Errorf("$now resolved to %v and %v, want one time", at, got["upd"]["at"])
	}
	want := map[string]map[string]any{
		"new":  {"g": "c", "at": at},
		gen:    {"g": "gen"},
		"byid": {"g": "c"},
		"upd":  {"v": 10.0, "at": at},
		"ups":  {"g": "u"},
		"rep":  {"r": true},
		"mem":  {"g": "m2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("after the batch:\n got %v\nwant %v", got, want)
	}
	if c.Count() != len(want) {
		t.Errorf("Count = %d, want %d", c.Count(), len(want))
	}
	// Every item carries the batch record's offset.
	rec := lastBatchRecord(t, e)
	for _, id := range []string{"new", gen, "upd", "rep", "mem", "del"} {
		if off := memOffset(t, c, id); off != rec.Offset {
			t.Errorf("memtable entry of %q has offset %d, want the batch's %d", id, off, rec.Offset)
		}
	}
	items, err := wal.DecodeBatch(rec.Payload)
	if err != nil || len(items) != len(ops) {
		t.Fatalf("batch record holds %d items (%v), want %d", len(items), err, len(ops))
	}
	if items[7].Op != wal.OpDeleteDoc || items[7].ID != "del" || items[0].Op != wal.OpPutDoc {
		t.Errorf("batch items: %+v", items)
	}
	// The index sees the batch, also after a flush and a reopen.
	check := func(c *Collection, why string) {
		t.Helper()
		if got := docsOf(t, c); !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n got %v\nwant %v", why, got, want)
		}
		if ids := indexLookup(t, c, "by_g", "c"); !reflect.DeepEqual(ids, []string{"byid", "new"}) {
			t.Errorf("%s: by_g = c holds %v, want [byid new]", why, ids)
		}
		c.db.mu.RLock()
		checkIndexEntries(t, c, mustFind(t, c))
		c.db.mu.RUnlock()
	}
	check(c, "before the flush")
	mustFlush(t, c)
	check(c, "after the flush")
	e, c = reopenColl(t, e)
	check(c, "after a reopen")
	_ = e
}

// mustFind returns every document of c.
func mustFind(t *testing.T, c *Collection) []*Document {
	t.Helper()
	docs, err := c.Find(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

func TestBatchEmpty(t *testing.T) {
	e, c := newTestColl(t, nil, nil)
	var res BatchResult
	err := expectOneRecord(t, e, 0, func() error {
		var err error
		res, err = c.Batch(context.Background(), nil)
		return err
	})
	if err != nil || len(res.IDs) != 0 {
		t.Errorf("empty Batch = %+v, %v", res, err)
	}
}

// TestBatchItemErrors: every kind of failing operation fails the whole
// batch with a *BatchItemError naming the operation and wrapping its
// sentinel, and nothing is written.
func TestBatchItemErrors(t *testing.T) {
	ctx := context.Background()
	schema := &Schema{Fields: map[string]Field{"name": {Type: TypeString, Required: true}}}
	ok := WriteOp{Kind: OpInsert, Document: &Document{ID: "fresh", Data: map[string]any{"name": "f", "k": "free"}}}
	tests := []struct {
		name    string
		bad     WriteOp
		wantErr error // nil: no sentinel, a usage error
	}{
		{"insert of an existing id", WriteOp{Kind: OpInsert, Document: &Document{ID: "a", Data: map[string]any{"name": "x"}}}, ErrExists},
		{"update of a missing document", WriteOp{Kind: OpUpdate, ID: "nope", Update: UpdateSpec{Set: map[string]any{"name": "x"}}}, ErrNotFound},
		{"replace of a missing document", WriteOp{Kind: OpReplace, ID: "nope", Document: &Document{Data: map[string]any{"name": "x"}}}, ErrNotFound},
		{"delete of a missing document", WriteOp{Kind: OpDelete, ID: "nope"}, ErrNotFound},
		{"update condition fails", WriteOp{Kind: OpUpdate, ID: "a", Update: UpdateSpec{Set: map[string]any{"v": 1}}, If: Condition{eqf("name", "other")}}, ErrConditionFailed},
		{"update's own condition fails", WriteOp{Kind: OpUpdate, ID: "a", Update: UpdateSpec{Set: map[string]any{"v": 1}, If: Condition{eqf("name", "other")}}}, ErrConditionFailed},
		{"replace condition fails", WriteOp{Kind: OpReplace, ID: "a", Document: &Document{Data: map[string]any{"name": "x"}}, If: Condition{eqf("name", "other")}}, ErrConditionFailed},
		{"delete condition fails", WriteOp{Kind: OpDelete, ID: "a", If: Condition{eqf("name", "other")}}, ErrConditionFailed},
		{"insert violates the schema", WriteOp{Kind: OpInsert, Document: &Document{ID: "s", Data: map[string]any{"name": 5}}}, ErrSchemaValidation},
		{"update violates the schema", WriteOp{Kind: OpUpdate, ID: "a", Update: UpdateSpec{Unset: []string{"name"}}}, ErrSchemaValidation},
		{"replace violates the schema", WriteOp{Kind: OpReplace, ID: "a", Document: &Document{Data: map[string]any{}}}, ErrSchemaValidation},
		{"insert with _id in the data", WriteOp{Kind: OpInsert, Document: &Document{ID: "s", Data: map[string]any{"_id": "s", "name": "x"}}}, ErrSchemaValidation},
		{"update sets _id", WriteOp{Kind: OpUpdate, ID: "a", Update: UpdateSpec{Set: map[string]any{"_id": "z"}}}, ErrSchemaValidation},
		{"insert with differing ids", WriteOp{Kind: OpInsert, ID: "x", Document: &Document{ID: "y", Data: map[string]any{"name": "x"}}}, ErrSchemaValidation},
		{"insert takes a unique value", WriteOp{Kind: OpInsert, Document: &Document{ID: "u", Data: map[string]any{"name": "x", "k": "ka"}}}, ErrUniqueViolation},
		{"update takes a unique value", WriteOp{Kind: OpUpdate, ID: "b", Update: UpdateSpec{Set: map[string]any{"k": "ka"}}}, ErrUniqueViolation},
		{"takes the value of another operation", WriteOp{Kind: OpInsert, Document: &Document{ID: "u", Data: map[string]any{"name": "x", "k": "free"}}}, ErrUniqueViolation},
		{"id of another operation", WriteOp{Kind: OpDelete, ID: "fresh"}, ErrExists},
		{"upsert with an empty id", WriteOp{Kind: OpUpdate, Update: UpdateSpec{Set: map[string]any{"name": "x"}, Upsert: true}}, nil},
		{"insert of a nil document", WriteOp{Kind: OpInsert}, nil},
		{"insert with a condition", WriteOp{Kind: OpInsert, Document: &Document{ID: "s", Data: map[string]any{"name": "x"}}, If: Condition{eqf("name", "x")}}, nil},
		{"insert with an update", WriteOp{Kind: OpInsert, Document: &Document{ID: "s", Data: map[string]any{"name": "x"}}, Update: UpdateSpec{Unset: []string{"v"}}}, nil},
		{"update with a document", WriteOp{Kind: OpUpdate, ID: "a", Document: &Document{}, Update: UpdateSpec{Set: map[string]any{"v": 1}}}, nil},
		{"replace with an update", WriteOp{Kind: OpReplace, ID: "a", Document: &Document{Data: map[string]any{"name": "x"}}, Update: UpdateSpec{Upsert: true}}, nil},
		{"replace of a nil document", WriteOp{Kind: OpReplace, ID: "a"}, nil},
		{"delete with a document", WriteOp{Kind: OpDelete, ID: "a", Document: &Document{}}, nil},
		{"delete with an update", WriteOp{Kind: OpDelete, ID: "a", Update: UpdateSpec{Set: map[string]any{"v": 1}}}, nil},
		{"bad condition operator", WriteOp{Kind: OpDelete, ID: "a", If: Condition{{Field: "v", Operator: "bogus", Value: 1}}}, nil},
		{"unknown kind", WriteOp{Kind: OpKind(9), ID: "a"}, nil},
		{"zero kind", WriteOp{ID: "a"}, nil},
		{"id too long", WriteOp{Kind: OpInsert, Document: &Document{ID: strings.Repeat("x", maxIDLen+1), Data: map[string]any{"name": "x"}}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, c := newTestColl(t, nil, schema)
			if err := c.CreateIndex(ctx, "by_k", "k", Unique(true)); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "a", map[string]any{"name": "a", "k": "ka"})
			mustInsert(t, c, "b", map[string]any{"name": "b", "k": "kb"})
			before := docsOf(t, c)
			last := WriteOp{Kind: OpInsert, Document: &Document{ID: "z", Data: map[string]any{"name": "z"}}}
			ops := []WriteOp{ok, tt.bad, last}
			err := expectOneRecord(t, e, 0, func() error {
				_, err := c.Batch(ctx, ops)
				return err
			})
			var bie *BatchItemError
			if !errors.As(err, &bie) {
				t.Fatalf("Batch error = %v, want a *BatchItemError", err)
			}
			if bie.Index != 1 {
				t.Errorf("BatchItemError.Index = %d, want 1 (%v)", bie.Index, err)
			}
			if !strings.HasPrefix(err.Error(), "batch operation 1: ") {
				t.Errorf("error text %q does not name operation 1", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error %v does not wrap %v", err, tt.wantErr)
			}
			if bie.Unwrap() != bie.Err {
				t.Error("Unwrap does not return Err")
			}
			if got := docsOf(t, c); !reflect.DeepEqual(got, before) {
				t.Errorf("a failed batch changed the collection:\n got %v\nwant %v", got, before)
			}
			// Nothing is held: the same ids can be written right away.
			if _, err := c.Batch(ctx, []WriteOp{ok, last}); err != nil {
				t.Errorf("Batch after the failure: %v", err)
			}
		})
	}
}

// TestBatchUniqueFinalState: unique indexes see the batch's final state, so
// one operation may take a value another one frees.
func TestBatchUniqueFinalState(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_k", "k", Unique(true)); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "a", map[string]any{"k": 1.0})
	mustInsert(t, c, "b", map[string]any{"k": 2.0})
	mustInsert(t, c, "c", map[string]any{"k": 3.0})
	// Swap a and b's values, give c's to a new document while deleting c.
	_, err := c.Batch(ctx, []WriteOp{
		{Kind: OpUpdate, ID: "a", Update: UpdateSpec{Set: map[string]any{"k": 2.0}}},
		{Kind: OpUpdate, ID: "b", Update: UpdateSpec{Set: map[string]any{"k": 1.0}}},
		{Kind: OpDelete, ID: "c"},
		{Kind: OpInsert, Document: &Document{ID: "d", Data: map[string]any{"k": 3.0}}},
		{Kind: OpUpdate, ID: "d2", Update: UpdateSpec{Set: map[string]any{"k": []any{"p", "p"}}, Upsert: true}}, // arrays are not constrained
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	for v, id := range map[float64]string{1: "b", 2: "a", 3: "d"} {
		if ids := indexLookup(t, c, "by_k", v); !reflect.DeepEqual(ids, []string{id}) {
			t.Errorf("by_k = %v holds %v, want [%s]", v, ids, id)
		}
	}
	// The unique values are enforced against later writes and survive a
	// reopen.
	if _, err := c.Insert(ctx, &Document{ID: "x", Data: map[string]any{"k": 3.0}}); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("insert of a taken value after the batch: %v", err)
	}
	e, c = reopenColl(t, e)
	if _, err := c.Insert(ctx, &Document{ID: "x", Data: map[string]any{"k": 1.0}}); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("insert of a taken value after a reopen: %v", err)
	}
	// Two operations taking the same new value: the later one fails.
	_, err = c.Batch(ctx, []WriteOp{
		{Kind: OpInsert, Document: &Document{ID: "p", Data: map[string]any{"k": 9.0}}},
		{Kind: OpInsert, Document: &Document{ID: "q", Data: map[string]any{"k": 9.0}}},
	})
	var bie *BatchItemError
	if !errors.As(err, &bie) || bie.Index != 1 || !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("two operations with one value = %v, want operation 1 failing with ErrUniqueViolation", err)
	}
	_ = e
}

func TestBatchLimits(t *testing.T) {
	ctx := context.Background()
	inserts := func(n int, pad int) []WriteOp {
		ops := make([]WriteOp, n)
		for i := range ops {
			ops[i] = WriteOp{Kind: OpInsert, Document: &Document{ID: fmt.Sprintf("d%02d", i), Data: map[string]any{"p": strings.Repeat("x", pad)}}}
		}
		return ops
	}
	t.Run("docs", func(t *testing.T) {
		e, c := newTestColl(t, func(o *Options) { o.MaxBatchDocs = 3 }, nil)
		err := expectOneRecord(t, e, 0, func() error {
			_, err := c.Batch(ctx, inserts(4, 1))
			return err
		})
		if !errors.Is(err, ErrBatchTooLarge) || !strings.Contains(err.Error(), "the limit is 3 (MaxBatchDocs)") {
			t.Errorf("4 operations with MaxBatchDocs 3: %v", err)
		}
		if _, ok := errors.AsType[*BatchItemError](err); ok {
			t.Errorf("a limit error names an operation: %v", err)
		}
		if c.Count() != 0 {
			t.Errorf("Count = %d after a rejected batch", c.Count())
		}
		if _, err := c.Batch(ctx, inserts(3, 1)); err != nil {
			t.Errorf("3 operations with MaxBatchDocs 3: %v", err)
		}
	})
	t.Run("bytes", func(t *testing.T) {
		// Each item: 7 bytes + id (3) + document; the payload adds 4.
		docLen := len(`{"_id":"d00","p":""}`) + 100
		item := 7 + 3 + docLen
		limit := int64(4 + 2*item)
		e, c := newTestColl(t, func(o *Options) { o.MaxBatchBytes = limit }, nil)
		if _, err := c.Batch(ctx, inserts(2, 100)); err != nil {
			t.Fatalf("a batch of exactly MaxBatchBytes: %v", err)
		}
		if got := int64(len(lastBatchRecord(t, e).Payload)); got != limit {
			t.Errorf("payload is %d bytes, want %d", got, limit)
		}
		ops := inserts(3, 100)
		for i := range ops {
			ops[i].Document.ID = fmt.Sprintf("e%02d", i)
		}
		err := expectOneRecord(t, e, 0, func() error {
			_, err := c.Batch(ctx, ops)
			return err
		})
		if !errors.Is(err, ErrBatchTooLarge) || !strings.Contains(err.Error(), fmt.Sprintf("exceeds %d bytes (MaxBatchBytes) at operation 2", limit)) {
			t.Errorf("3 items over MaxBatchBytes: %v", err)
		}
		if c.Count() != 2 {
			t.Errorf("Count = %d, want 2", c.Count())
		}
	})
	t.Run("defaults", func(t *testing.T) {
		_, c := newTestColl(t, nil, nil)
		if o := c.db.e.opts; o.MaxBatchDocs != 10_000 || o.MaxBatchBytes != 32<<20 {
			t.Errorf("defaults MaxBatchDocs %d, MaxBatchBytes %d", o.MaxBatchDocs, o.MaxBatchBytes)
		}
		if _, err := c.Batch(ctx, inserts(10_001, 0)); !errors.Is(err, ErrBatchTooLarge) {
			t.Errorf("10,001 operations: %v", err)
		}
	})
	t.Run("negative options", func(t *testing.T) {
		for _, mod := range []func(*Options){func(o *Options) { o.MaxBatchDocs = -1 }, func(o *Options) { o.MaxBatchBytes = -1 }} {
			o := testOptions(t.TempDir())
			mod(&o)
			if e, err := Open(ctx, o); err == nil {
				_ = e.Close(ctx)
				t.Errorf("Open accepted MaxBatchDocs %d, MaxBatchBytes %d", o.MaxBatchDocs, o.MaxBatchBytes)
			}
		}
	})
}

func TestBatchContextAndDrop(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	op := []WriteOp{{Kind: OpInsert, Document: &Document{ID: "a", Data: map[string]any{}}}}
	err := expectOneRecord(t, e, 0, func() error {
		_, err := c.Batch(cctx, op)
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Batch with a cancelled context: %v", err)
	}
	if err := mustDB(t, e, "app").DropCollection(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Batch(ctx, op); !errors.Is(err, ErrNotFound) {
		t.Errorf("Batch on a dropped collection: %v", err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Batch(ctx, op); !errors.Is(err, ErrClosed) {
		t.Errorf("Batch after Close: %v", err)
	}
}

func TestOpKindString(t *testing.T) {
	for k, want := range map[OpKind]string{OpInsert: "insert", OpUpdate: "update", OpReplace: "replace", OpDelete: "delete", 0: "OpKind(0)", 7: "OpKind(7)"} {
		if got := k.String(); got != want {
			t.Errorf("OpKind(%d).String() = %q, want %q", uint8(k), got, want)
		}
	}
}

// TestBatchCrashAfterWALSync: a crash right after the batch record is
// durable, before any item is applied, recovers every item (with the
// indexes), also across a flush and a second crash.
func TestBatchCrashAfterWALSync(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateIndex(ctx, "by_k", "k", Unique(true)); err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]any{}
	for i := range 20 {
		id := fmt.Sprintf("d%02d", i)
		data := map[string]any{"g": fmt.Sprint(i % 3), "k": float64(i)}
		mustInsert(t, c, id, data)
		want[id] = data
	}
	mustFlush(t, c)
	e.abandon()

	armed := opts
	crashAt(t, &armed, hookAfterWALSync)
	e, c = openColl(t, armed)
	var ops []WriteOp
	for i := range 20 {
		id := fmt.Sprintf("d%02d", i)
		switch i % 4 {
		case 0:
			ops = append(ops, WriteOp{Kind: OpDelete, ID: id})
			delete(want, id)
		case 1:
			ops = append(ops, WriteOp{Kind: OpUpdate, ID: id, Update: UpdateSpec{Set: map[string]any{"g": "x", "k": float64(100 + i)}}})
			want[id] = map[string]any{"g": "x", "k": float64(100 + i)}
		case 2:
			ops = append(ops, WriteOp{Kind: OpReplace, ID: id, Document: &Document{Data: map[string]any{"g": "r"}}})
			want[id] = map[string]any{"g": "r"}
		}
	}
	for i := range 5 {
		id := fmt.Sprintf("n%d", i)
		ops = append(ops, WriteOp{Kind: OpInsert, Document: &Document{ID: id, Data: map[string]any{"g": "n", "k": float64(4 * i)}}}) // k of a deleted document
		want[id] = map[string]any{"g": "n", "k": float64(4 * i)}
	}
	if _, err := c.Batch(ctx, ops); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("Batch = %v, want the simulated crash", err)
	}
	if got := docsOf(t, c); len(got) != 20 {
		t.Errorf("before the restart the batch is applied: %d documents", len(got))
	}
	e.abandon()

	wantModel := model{"app": {"c": {indexes: map[string]string{"by_g": "g", "by_k": "k"}, docs: want}}}
	e = openEngine(t, opts)
	checkModel(t, e, wantModel)
	c = mustColl(t, mustDB(t, e, "app"), "c")
	if _, err := c.Insert(ctx, &Document{ID: "dup", Data: map[string]any{"k": 105.0}}); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("a recovered unique value is not enforced: %v", err)
	}
	mustFlush(t, c)
	e.abandon()
	e = openEngine(t, opts)
	checkModel(t, e, wantModel)
}

// TestTornBatchRecord: a batch record whose tail was not written is
// removed whole at open: none of its items appears, and what came before
// it is intact.
func TestTornBatchRecord(t *testing.T) {
	ctx := context.Background()
	for _, cut := range []int64{1, 20, 200} {
		t.Run(fmt.Sprintf("cut %d bytes", cut), func(t *testing.T) {
			opts, e, c := newCrashColl(t, nil, nil)
			if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "keep", map[string]any{"g": "a"})
			mustInsert(t, c, "upd", map[string]any{"g": "a"})
			mustFlush(t, c)
			mustInsert(t, c, "mem", map[string]any{"g": "b"})
			var ops []WriteOp
			for i := range 10 {
				ops = append(ops, WriteOp{Kind: OpInsert, Document: &Document{ID: fmt.Sprintf("b%d", i), Data: map[string]any{"g": "c", "pad": strings.Repeat("p", 20)}}})
			}
			ops = append(ops, WriteOp{Kind: OpUpdate, ID: "upd", Update: UpdateSpec{Set: map[string]any{"g": "z"}}}, WriteOp{Kind: OpDelete, ID: "mem"})
			if _, err := c.Batch(ctx, ops); err != nil {
				t.Fatal(err)
			}
			rec := lastBatchRecord(t, e)
			if int64(len(rec.Payload)) <= cut {
				t.Fatalf("payload of %d bytes is not longer than the cut", len(rec.Payload))
			}
			e.abandon()
			files := walFiles(t, opts.Dir)
			chopTail(t, files[len(files)-1], cut)

			e = openEngine(t, opts)
			checkModel(t, e, model{"app": {"c": {
				indexes: map[string]string{"by_g": "g"},
				docs:    map[string]map[string]any{"keep": {"g": "a"}, "upd": {"g": "a"}, "mem": {"g": "b"}},
			}}})
			// The log continues after the removed record.
			c = mustColl(t, mustDB(t, e, "app"), "c")
			mustInsert(t, c, "b0", map[string]any{"g": "after"})
			e = reopen(t, e)
			if got := mustGet(t, mustColl(t, mustDB(t, e, "app"), "c"), "b0").Data["g"]; got != "after" {
				t.Errorf("b0 after the torn batch = %v", got)
			}
		})
	}
}

// TestBatchReplayIdempotent: replaying a batch record again, or after its
// items were flushed (some of them superseded since), changes nothing.
func TestBatchReplayIdempotent(t *testing.T) {
	ctx := context.Background()
	_, e, c := newCrashColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "a", map[string]any{"g": "1"})
	mustInsert(t, c, "b", map[string]any{"g": "1"})
	mustInsert(t, c, "gone", map[string]any{"g": "1"})
	if _, err := c.Batch(ctx, []WriteOp{
		{Kind: OpUpdate, ID: "a", Update: UpdateSpec{Set: map[string]any{"g": "2"}}},
		{Kind: OpReplace, ID: "b", Document: &Document{Data: map[string]any{"g": "2"}}},
		{Kind: OpInsert, Document: &Document{ID: "c", Data: map[string]any{"g": "2"}}},
		{Kind: OpDelete, ID: "gone"},
	}); err != nil {
		t.Fatal(err)
	}
	rec := lastBatchRecord(t, e)
	replay := func(why string) {
		t.Helper()
		want := docsOf(t, c)
		applied, err := c.replayBatch(rec)
		if err != nil {
			t.Fatalf("%s: replayBatch: %v", why, err)
		}
		if applied {
			t.Errorf("%s: replayBatch applied an item again", why)
		}
		if got := docsOf(t, c); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: a second replay changed the state:\n got %v\nwant %v", why, got, want)
		}
	}
	replay("items in the memtable")
	mustFlush(t, c)
	replay("items flushed")
	if _, err := c.Update(ctx, "b", map[string]any{"g": "4"}); err != nil {
		t.Fatal(err)
	}
	replay("an item superseded in the memtable")

	// A batch record with a damaged payload is a corrupt WAL.
	bad := rec
	bad.Payload = append([]byte(nil), rec.Payload[:len(rec.Payload)-1]...)
	if _, err := c.replayBatch(bad); !errors.Is(err, ErrWALCorrupt) {
		t.Errorf("replayBatch of a damaged payload: %v", err)
	}
	// An item whose document does not decode fails the replay with its
	// index and id.
	payload, err := wal.AppendBatch(nil, []wal.BatchItem{{Op: wal.OpPutDoc, ID: "y", Doc: []byte(`not json`)}})
	if err != nil {
		t.Fatal(err)
	}
	bad.Payload, bad.Offset = payload, e.wal.NextOffset()+100
	if _, err := c.replayBatch(bad); err == nil || !strings.Contains(err.Error(), `batch item 0 (document "y")`) {
		t.Errorf("replayBatch of an undecodable item: %v", err)
	}
}

// TestBatchRecoveryAfterPartialFlush: recovery replays a batch record that
// is above the checkpoint although its items were flushed, some of them
// superseded (and flushed) since and one superseded in the memtable only;
// the state after one or two recoveries is the state before the crash.
func TestBatchRecoveryAfterPartialFlush(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "a", map[string]any{"g": "1"})
	mustInsert(t, c, "b", map[string]any{"g": "1"})
	mustInsert(t, c, "gone", map[string]any{"g": "1"})
	mustFlush(t, c)
	// A write still in flight holds the checkpoint below the batch.
	c.registerInflight()
	if _, err := c.Batch(ctx, []WriteOp{
		{Kind: OpUpdate, ID: "a", Update: UpdateSpec{Set: map[string]any{"g": "2"}}},
		{Kind: OpReplace, ID: "b", Document: &Document{Data: map[string]any{"g": "2"}}},
		{Kind: OpInsert, Document: &Document{ID: "c", Data: map[string]any{"g": "2"}}},
		{Kind: OpInsert, Document: &Document{ID: "keep", Data: map[string]any{"g": "2"}}},
		{Kind: OpDelete, ID: "gone"},
	}); err != nil {
		t.Fatal(err)
	}
	rec := lastBatchRecord(t, e)
	mustFlush(t, c)
	if cp := storedCheckpoint(t, c); cp >= rec.Offset {
		t.Fatalf("checkpoint %d is not below the batch at %d", cp, rec.Offset)
	}
	if _, err := c.Update(ctx, "a", map[string]any{"g": "3"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "gone", map[string]any{"g": "back"})
	mustFlush(t, c)
	if _, err := c.Update(ctx, "b", map[string]any{"g": "4"}); err != nil {
		t.Fatal(err)
	}
	want := model{"app": {"c": {indexes: map[string]string{"by_g": "g"}, docs: docsOf(t, c)}}}
	if len(want["app"]["c"].docs) != 4 {
		t.Fatalf("unexpected state before the crash: %v", want["app"]["c"].docs)
	}
	e.abandon()
	for range 2 {
		e = openEngine(t, opts)
		checkModel(t, e, want)
		e.abandon()
	}
	e = openEngine(t, opts)
	checkModel(t, e, want)
}

// TestBatchReplaySkipsDroppedCollection: a batch record of a collection
// dropped later is not replayed into anything.
func TestBatchReplaySkipsDroppedCollection(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	if _, err := c.Batch(ctx, []WriteOp{{Kind: OpInsert, Document: &Document{ID: "a", Data: map[string]any{}}}}); err != nil {
		t.Fatal(err)
	}
	d := mustDB(t, e, "app")
	if err := d.DropCollection(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	e.abandon()
	e = openEngine(t, opts)
	checkModel(t, e, model{"app": {"c": {indexes: map[string]string{}, docs: map[string]map[string]any{}}}})
}

// TestConcurrentBatchesAndWrites: batches over overlapping ids, in
// different orders, and single writes on the same ids do not deadlock, and
// every batch is applied whole.
func TestConcurrentBatchesAndWrites(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, func(o *Options) { o.MemtableSize = 4 << 10 }, nil)
	if err := c.CreateIndex(ctx, "by_k", "k", Unique(true)); err != nil {
		t.Fatal(err)
	}
	const ids = 40
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := range 6 {
		wg.Go(func() {
			for round := range 30 {
				ops := make([]WriteOp, 0, 10)
				for i := range 10 {
					id := fmt.Sprintf("d%02d", (w*7+round*3+i*(w+1))%ids)
					if slicesContainID(ops, id) {
						continue
					}
					// Every document of one batch gets the same round tag.
					ops = append(ops, WriteOp{Kind: OpUpdate, ID: id, Update: UpdateSpec{Set: map[string]any{"tag": fmt.Sprintf("w%d-r%d", w, round), "n": float64(len(ops))}, Upsert: true}})
				}
				res, err := c.Batch(ctx, ops)
				if err != nil {
					errs <- err
					return
				}
				if len(res.IDs) != len(ops) {
					errs <- fmt.Errorf("result has %d ids for %d operations", len(res.IDs), len(ops))
					return
				}
			}
		})
	}
	for w := range 3 {
		wg.Go(func() {
			for i := range 100 {
				id := fmt.Sprintf("d%02d", (i*13+w)%ids)
				_, _, err := c.UpdateWith(ctx, id, UpdateSpec{Set: map[string]any{"single": float64(i)}, Upsert: true})
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			if err := c.flush(); err != nil {
				errs <- err
				return
			}
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if c.Count() != len(docsOf(t, c)) {
		t.Errorf("Count %d differs from the documents", c.Count())
	}
	c.db.mu.RLock()
	checkIndexEntries(t, c, mustFind(t, c))
	c.db.mu.RUnlock()
}

func slicesContainID(ops []WriteOp, id string) bool {
	for _, op := range ops {
		if op.ID == id {
			return true
		}
	}
	return false
}
