package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests of the plan-013 stage-5 engine additions (approach-agent-interface):
// Iterate and its retries, the update operators, and the statistics.

// noPanic runs fn and fails the test, instead of crashing the test binary,
// if it panics.
func noPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", what, r)
		}
	}()
	fn()
}

// TestFindUnboundedByDefault: the engine's Find keeps its library
// semantics: no limit means every match, well past the MCP default of 100.
func TestFindUnboundedByDefault(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	ops := make([]WriteOp, 250)
	for i := range ops {
		ops[i] = WriteOp{Kind: OpInsert, Document: &Document{ID: fmt.Sprintf("d%03d", i), Data: map[string]any{"n": float64(i)}}}
	}
	if _, err := c.Batch(ctx, ops); err != nil {
		t.Fatal(err)
	}
	for _, q := range []*Query{nil, {}, {Limit: 0}, {Limit: -1}, {Sort: []SortKey{{Field: "n", Order: "desc"}}}} {
		docs, err := c.Find(ctx, q)
		if err != nil || len(docs) != 250 {
			t.Errorf("Find(%+v) = %d documents, %v; want 250", q, len(docs), err)
		}
		n := 0
		if err := c.Iterate(ctx, q, func(*Document) bool { n++; return true }); err != nil || n != 250 {
			t.Errorf("Iterate(%+v) = %d documents, %v; want 250", q, n, err)
		}
	}
}

// TestIterateStopsAndCopies: Iterate stops when fn returns false, gives fn
// copies, and IterateWithPlan counts the results fn received.
func TestIterateStopsAndCopies(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	for i := range 20 {
		mustInsert(t, c, fmt.Sprintf("d%02d", i), map[string]any{"n": float64(i), "s": fmt.Sprintf("s%02d", 19-i)})
	}
	if err := c.CreateIndex(ctx, "by_n", "n"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []Query{
		{},                              // _id order
		{Sort: []SortKey{{Field: "n"}}}, // index sort walk
		{Sort: []SortKey{{Field: "s"}}}, // heap
		{Sort: []SortKey{{Field: "_id", Order: "desc"}}},
		{Filters: []QueryFilter{{"n", "gte", 5}}},
	} {
		var got []*Document
		plan, err := c.IterateWithPlan(ctx, &q, func(d *Document) bool {
			got = append(got, d)
			d.Data["n"] = "changed" // a copy: the stored document must not change
			return len(got) < 3
		})
		if err != nil || len(got) != 3 || plan.Returned != 3 {
			t.Errorf("IterateWithPlan(%+v): %d results, plan %+v, %v; want 3", q, len(got), plan, err)
		}
		all, err := c.Find(ctx, &Query{Sort: q.Sort, Filters: q.Filters, Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(docIDs(got), docIDs(all)) {
			t.Errorf("Iterate(%+v) = %v, Find = %v", q, docIDs(got), docIDs(all))
		}
		for _, d := range all {
			if _, ok := d.Data["n"].(float64); !ok {
				t.Fatalf("stored document %s changed through Iterate's copy: %v", d.ID, d.Data)
			}
		}
	}
}

// TestHugeSkipDoesNotPanic: a skip near math.MaxInt with a limit (as
// find_documents always sends one) returns nothing on every driver,
// without overflowing skip+limit. Code review c1 (pkg/db/query.go
// findByHeap: keep = skip + limit).
func TestHugeSkipDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	for i := range 10 {
		mustInsert(t, c, fmt.Sprintf("d%02d", i), map[string]any{"n": float64(i), "s": fmt.Sprintf("s%d", i)})
	}
	if err := c.CreateIndex(ctx, "by_n", "n"); err != nil {
		t.Fatal(err)
	}
	for _, skip := range []int{math.MaxInt, math.MaxInt - 50, math.MaxInt - 101} {
		for _, q := range []Query{
			{Skip: skip, Limit: 101, Sort: []SortKey{{Field: "s"}}},              // heap (not indexed)
			{Skip: skip, Limit: 101, Sort: []SortKey{{Field: "n"}}},              // index sort walk
			{Skip: skip, Limit: 101},                                             // _id order
			{Skip: skip, Limit: 101, Filters: []QueryFilter{{"n", "gte", 2}}},    // index range
			{Skip: skip, Limit: 1, Sort: []SortKey{{Field: "s", Order: "desc"}}}, // heap
		} {
			name := fmt.Sprintf("skip %d, %+v", skip, q)
			noPanic(t, "Find "+name, func() {
				docs, err := c.Find(ctx, &q)
				if err != nil || len(docs) != 0 {
					t.Errorf("Find(%s) = %v, %v; want no documents", name, docIDs(docs), err)
				}
			})
			noPanic(t, "Iterate "+name, func() {
				n := 0
				if err := c.Iterate(ctx, &q, func(*Document) bool { n++; return true }); err != nil || n != 0 {
					t.Errorf("Iterate(%s) = %d documents, %v; want none", name, n, err)
				}
			})
		}
	}
}

// TestIterateResumesAfterCompactionRetry pages with Iterate (skip, limit,
// and a stop in fn) on every driver while flushes and compactions retire
// the segments under open snapshots. Iterate continues by position after
// a retry, so its results must still be exact: no document twice, none
// missing (code review c1, the streaming resume paths).
func TestIterateResumesAfterCompactionRetry(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, func(o *Options) {
		o.DataSegmentSize = 2 << 10
		o.CompactionRatio = 0.2
		o.CacheSize = -1 // reads go to the segments
	}, nil)
	if err := c.CreateIndex(ctx, "i_n", "n"); err != nil {
		t.Fatal(err)
	}
	for i := range 300 {
		mustInsert(t, c, fmt.Sprintf("d%04d", i), map[string]any{"n": float64(i % 50), "s": fmt.Sprintf("s%03d", i%100), "pad": strings.Repeat("x", 100)})
	}
	mustFlush(t, c)
	queries := []Query{
		{Skip: 17, Limit: 120},
		{Skip: 40},
		{Sort: []SortKey{{Field: "n"}}, Skip: 33, Limit: 150},
		{Sort: []SortKey{{Field: "n", Order: "desc"}}, Skip: 5},
		{Sort: []SortKey{{Field: "s"}}, Skip: 21, Limit: 200},
		{Sort: []SortKey{{Field: "_id", Order: "desc"}}, Skip: 9, Limit: 250},
		{Filters: []QueryFilter{{"n", "gte", 10}}, Skip: 11, Limit: 100},
	}
	want := make([][]string, len(queries))
	for i := range queries {
		docs, err := c.Find(ctx, &queries[i])
		if err != nil {
			t.Fatal(err)
		}
		want[i] = docIDs(docs)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	var bgErr atomic.Value
	wg.Go(func() { // rewrites only pad: the query results stay the same
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if _, err := c.Update(ctx, fmt.Sprintf("d%04d", i%300), map[string]any{"pad": strings.Repeat("y", i%120)}); err != nil {
				bgErr.Store(err)
				return
			}
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if err := c.flush(); err != nil {
				bgErr.Store(err)
				return
			}
			if _, err := c.compact(ctx); err != nil {
				bgErr.Store(err)
				return
			}
		}
	})
	deadline := time.Now().Add(700 * time.Millisecond)
	completed, interrupted := 0, 0
	for n := 0; time.Now().Before(deadline) && !t.Failed(); n++ {
		i := n % len(queries)
		stopAt := len(want[i])
		if n%3 == 1 && stopAt > 1 {
			stopAt = 1 + n%stopAt // fn stops early
		}
		var got []string
		err := c.Iterate(ctx, &queries[i], func(d *Document) bool {
			got = append(got, d.ID)
			return len(got) < stopAt
		})
		if errors.Is(err, ErrQueryInterrupted) {
			// More retries than allowed under this compaction rate (Find
			// gets the same here): the documented "retry it" outcome.
			interrupted++
			continue
		}
		if err != nil {
			t.Errorf("query %d: %v", i, err)
			break
		}
		completed++
		if !slices.Equal(got, want[i][:min(stopAt, len(want[i]))]) {
			t.Errorf("query %+v (stop at %d) = %v\nwant %v", queries[i], stopAt, got, want[i][:min(stopAt, len(want[i]))])
		}
	}
	close(done)
	wg.Wait()
	if err, _ := bgErr.Load().(error); err != nil {
		t.Fatal(err)
	}
	if completed == 0 {
		t.Errorf("no query completed (%d interrupted)", interrupted)
	}
	t.Logf("%d queries completed and checked, %d interrupted", completed, interrupted)
}

// TestIterateIndexChangeRetry: an index dropped and re-created under the
// same name on another field while queries run must not make Iterate
// return a document twice, skip one, or return them out of order (code
// review c1: the index-change retry resumes by position). The race window
// is small, so this is a stress probe, not a deterministic reproduction.
func TestIterateIndexChangeRetry(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, func(o *Options) { o.CacheSize = -1 }, nil)
	for i := range 200 {
		// n ascends with the id, m descends: walking the wrong index gives
		// the reverse order.
		mustInsert(t, c, fmt.Sprintf("d%04d", i), map[string]any{"n": float64(i), "m": float64(1000 - i)})
	}
	if err := c.CreateIndex(ctx, "ix", "n"); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, c)
	queries := []Query{
		{Sort: []SortKey{{Field: "n"}}, Skip: 7, Limit: 150},
		{Filters: []QueryFilter{{"n", "gte", 50}}, Limit: 120},
		{Filters: []QueryFilter{{"n", "lt", 150}}, Sort: []SortKey{{Field: "n", Order: "desc"}}, Skip: 3},
	}
	want := make([][]string, len(queries))
	for i := range queries {
		docs, err := c.Find(ctx, &queries[i])
		if err != nil {
			t.Fatal(err)
		}
		want[i] = docIDs(docs)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	var bgErr atomic.Value
	wg.Go(func() {
		fields := []string{"m", "n"}
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := c.DropIndex(ctx, "ix"); err != nil {
				bgErr.Store(err)
				return
			}
			if err := c.CreateIndex(ctx, "ix", fields[i%2]); err != nil {
				bgErr.Store(err)
				return
			}
		}
	})
	deadline := time.Now().Add(700 * time.Millisecond)
	for n := 0; time.Now().Before(deadline) && !t.Failed(); n++ {
		i := n % len(queries)
		var got []string
		err := c.Iterate(ctx, &queries[i], func(d *Document) bool {
			got = append(got, d.ID)
			return true
		})
		if err != nil && !errors.Is(err, ErrQueryInterrupted) {
			t.Errorf("query %d: %v", i, err)
			break
		}
		if err == nil && !slices.Equal(got, want[i]) {
			t.Errorf("query %+v = %v\nwant %v", queries[i], got, want[i])
		}
	}
	close(done)
	wg.Wait()
	if err, _ := bgErr.Load().(error); err != nil {
		t.Fatal(err)
	}
}

// TestUpdateOperatorsSemantics covers inc, push, add_to_set, and pull
// (approach-agent-interface §4) through UpdateWith: their effects, the
// missing-field rules, element equality, and that they apply after Set
// and Unset.
func TestUpdateOperatorsSemantics(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "d", map[string]any{"n": 1, "f": 1.5, "tags": []any{"a", "b", "a"}, "nums": []any{1.0, 2, 3}, "keep": "k"})
	update := func(u UpdateSpec) map[string]any {
		t.Helper()
		if _, _, err := c.UpdateWith(ctx, "d", u); err != nil {
			t.Fatalf("UpdateWith(%+v): %v", u, err)
		}
		return mustGet(t, c, "d").Data
	}

	got := update(UpdateSpec{Inc: map[string]any{"n": 2, "f": -0.5, "fresh": 3}})
	if got["n"] != 3.0 || got["f"] != 1.0 || got["fresh"] != 3.0 || got["keep"] != "k" {
		t.Errorf("inc: %v", got)
	}
	got = update(UpdateSpec{Push: map[string]any{"tags": "c", "list": map[string]any{"x": 1}}})
	if !reflect.DeepEqual(got["tags"], []any{"a", "b", "a", "c"}) || !reflect.DeepEqual(got["list"], []any{map[string]any{"x": 1.0}}) {
		t.Errorf("push: tags %v, list %v", got["tags"], got["list"])
	}
	// add_to_set: numbers compare as numbers (2 == 2.0); a missing field
	// starts an array.
	got = update(UpdateSpec{AddToSet: map[string]any{"nums": 2.0, "tags": "z", "set": "one"}})
	if !reflect.DeepEqual(got["nums"], []any{1.0, 2.0, 3.0}) || !reflect.DeepEqual(got["tags"], []any{"a", "b", "a", "c", "z"}) || !reflect.DeepEqual(got["set"], []any{"one"}) {
		t.Errorf("add_to_set: nums %v, tags %v, set %v", got["nums"], got["tags"], got["set"])
	}
	// pull removes every equal element; a missing field stays missing.
	got = update(UpdateSpec{Pull: map[string]any{"tags": "a", "nums": 1, "absent": "x"}})
	if !reflect.DeepEqual(got["tags"], []any{"b", "c", "z"}) || !reflect.DeepEqual(got["nums"], []any{2.0, 3.0}) {
		t.Errorf("pull: tags %v, nums %v", got["tags"], got["nums"])
	}
	if _, ok := got["absent"]; ok {
		t.Errorf("pull on a missing field created it: %v", got)
	}
	// After Set and Unset: Set replaces the array, then push appends to the
	// new value; Unset removes a field, then inc starts it from 0.
	got = update(UpdateSpec{Set: map[string]any{"arr": []any{"s"}}, Unset: []string{"n"}, Push: map[string]any{"tags": "after"}})
	if !reflect.DeepEqual(got["tags"], []any{"b", "c", "z", "after"}) || got["n"] != nil {
		t.Errorf("set/unset then push: %v", got)
	}
	got = update(UpdateSpec{Inc: map[string]any{"n": 5}})
	if got["n"] != 5.0 {
		t.Errorf("inc of a removed field = %v, want 5 (from 0)", got["n"])
	}
	// $now in a push value resolves to the server time.
	got = update(UpdateSpec{Push: map[string]any{"times": map[string]any{"$now": true}}})
	if ts, ok := got["times"].([]any); !ok || len(ts) != 1 {
		t.Errorf("push $now: %v", got["times"])
	} else if _, err := time.Parse(time.RFC3339Nano, ts[0].(string)); err != nil {
		t.Errorf("push $now gave %v: %v", ts[0], err)
	}
}

// TestUpdateOperatorErrors: an operator on a field of the wrong type, a
// field named twice, an operator on _id, and a bad inc value fail with
// ErrInvalidUpdate (a field in Set and Unset keeps ErrSchemaValidation)
// and write nothing, in UpdateWith, UpdateMany, and Batch.
func TestUpdateOperatorErrors(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	orig := map[string]any{"n": 1.0, "s": "str", "arr": []any{1.0}, "null": nil, "obj": map[string]any{"a": 1.0}}
	mustInsert(t, c, "d", orig)
	tests := []struct {
		name string
		u    UpdateSpec
		want error
	}{
		{"inc on a string", UpdateSpec{Inc: map[string]any{"s": 1}}, ErrInvalidUpdate},
		{"inc on an array", UpdateSpec{Inc: map[string]any{"arr": 1}}, ErrInvalidUpdate},
		{"inc on null", UpdateSpec{Inc: map[string]any{"null": 1}}, ErrInvalidUpdate},
		{"inc with a string value", UpdateSpec{Inc: map[string]any{"n": "1"}}, ErrInvalidUpdate},
		{"inc with NaN", UpdateSpec{Inc: map[string]any{"n": math.NaN()}}, ErrInvalidUpdate},
		{"inc overflow", UpdateSpec{Inc: map[string]any{"n": math.MaxFloat64}}, nil}, // 1 + max is finite
		{"push on a number", UpdateSpec{Push: map[string]any{"n": 1}}, ErrInvalidUpdate},
		{"push on an object", UpdateSpec{Push: map[string]any{"obj": 1}}, ErrInvalidUpdate},
		{"add_to_set on a string", UpdateSpec{AddToSet: map[string]any{"s": 1}}, ErrInvalidUpdate},
		{"pull on a string", UpdateSpec{Pull: map[string]any{"s": "s"}}, ErrInvalidUpdate},
		{"pull on null", UpdateSpec{Pull: map[string]any{"null": 1}}, ErrInvalidUpdate},
		{"field in updates and inc", UpdateSpec{Set: map[string]any{"n": 2}, Inc: map[string]any{"n": 1}}, ErrInvalidUpdate},
		{"field in unset and push", UpdateSpec{Unset: []string{"arr"}, Push: map[string]any{"arr": 1}}, ErrInvalidUpdate},
		{"field in two operators", UpdateSpec{Push: map[string]any{"arr": 1}, Pull: map[string]any{"arr": 1}}, ErrInvalidUpdate},
		{"operator on _id", UpdateSpec{Inc: map[string]any{"_id": 1}}, ErrInvalidUpdate},
		{"field in updates and unset", UpdateSpec{Set: map[string]any{"s": "x"}, Unset: []string{"s"}}, ErrSchemaValidation},
		{"one good, one bad operator", UpdateSpec{Inc: map[string]any{"n": 1}, Push: map[string]any{"s": 1}}, ErrInvalidUpdate},
	}
	for _, tt := range tests {
		if tt.want == nil {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := c.UpdateWith(ctx, "d", tt.u); !errors.Is(err, tt.want) {
				t.Errorf("UpdateWith: %v, want %v", err, tt.want)
			}
			if _, _, err := c.UpdateMany(ctx, &Query{Filters: []QueryFilter{{"_id", "eq", "d"}}}, tt.u, 0); !errors.Is(err, tt.want) {
				t.Errorf("UpdateMany: %v, want %v", err, tt.want)
			}
			_, err := c.Batch(ctx, []WriteOp{{Kind: OpUpdate, ID: "d", Update: tt.u}})
			var bie *BatchItemError
			if !errors.Is(err, tt.want) || !errors.As(err, &bie) || bie.Index != 0 {
				t.Errorf("Batch: %v, want a BatchItemError for item 0 wrapping %v", err, tt.want)
			}
			if got := mustGet(t, c, "d").Data; !reflect.DeepEqual(got, orig) {
				t.Errorf("a failed update wrote: %v", got)
			}
		})
	}
	// An inc whose sum overflows to infinity fails too.
	mustInsert(t, c, "big", map[string]any{"n": math.MaxFloat64})
	if _, _, err := c.UpdateWith(ctx, "big", UpdateSpec{Inc: map[string]any{"n": math.MaxFloat64}}); !errors.Is(err, ErrInvalidUpdate) {
		t.Errorf("inc overflowing to +Inf: %v, want ErrInvalidUpdate", err)
	}
}

// TestUpdateOperatorsWithIfUpsertSchema: the operators combine with If
// and Upsert, and the schema is validated on the result.
func TestUpdateOperatorsWithIfUpsertSchema(t *testing.T) {
	ctx := context.Background()
	schema := &Schema{Fields: map[string]Field{"n": {Type: TypeNumber}, "tags": {Type: TypeArray}}}
	_, c := newTestColl(t, nil, schema)
	mustInsert(t, c, "d", map[string]any{"n": 1, "tags": []any{"a"}, "s": "x"})

	// If: only when the stored version matches.
	if _, _, err := c.UpdateWith(ctx, "d", UpdateSpec{Inc: map[string]any{"n": 1}, If: Condition{{"n", "eq", 5}}}); !errors.Is(err, ErrConditionFailed) {
		t.Errorf("inc with a failing if: %v", err)
	}
	if _, _, err := c.UpdateWith(ctx, "d", UpdateSpec{Inc: map[string]any{"n": 1}, If: Condition{{"n", "eq", 1}}}); err != nil {
		t.Errorf("inc with a matching if: %v", err)
	}
	if got := mustGet(t, c, "d").Data["n"]; got != 2.0 {
		t.Errorf("n = %v, want 2", got)
	}
	// Schema on the result: push onto a field the schema types as a
	// number fails (the operator itself fails first, as n is a number);
	// inc on a field the schema types as an array fails the schema.
	if _, _, err := c.UpdateWith(ctx, "d", UpdateSpec{Inc: map[string]any{"tags": 1}}); !errors.Is(err, ErrInvalidUpdate) {
		t.Errorf("inc on an array: %v", err)
	}
	mustInsert(t, c, "e", map[string]any{"n": 1})
	if _, _, err := c.UpdateWith(ctx, "e", UpdateSpec{Inc: map[string]any{"tags": 1}}); !errors.Is(err, ErrSchemaValidation) {
		t.Errorf("inc creating a number where the schema wants an array: %v, want ErrSchemaValidation", err)
	}
	if _, _, err := c.UpdateWith(ctx, "e", UpdateSpec{Push: map[string]any{"n2": 1}, Set: map[string]any{"n": "nope"}}); !errors.Is(err, ErrSchemaValidation) {
		t.Errorf("set breaking the schema with a push: %v", err)
	}
	if got := mustGet(t, c, "e").Data; !reflect.DeepEqual(got, map[string]any{"n": 1.0}) {
		t.Errorf("failed updates wrote: %v", got)
	}

	// Upsert: {_id} + Set, then the operators from nothing.
	_, upserted, err := c.UpdateWith(ctx, "new", UpdateSpec{Upsert: true, Set: map[string]any{"s": "v"}, Inc: map[string]any{"n": 5}, Push: map[string]any{"tags": "t"}, AddToSet: map[string]any{"set": 1}, Pull: map[string]any{"gone": 1}})
	if err != nil || !upserted {
		t.Fatalf("upsert with operators: upserted %v, %v", upserted, err)
	}
	if got := mustGet(t, c, "new").Data; !reflect.DeepEqual(got, map[string]any{"s": "v", "n": 5.0, "tags": []any{"t"}, "set": []any{1.0}}) {
		t.Errorf("upserted %v", got)
	}
	// Upsert of an existing document applies the operators to it.
	if _, upserted, err := c.UpdateWith(ctx, "new", UpdateSpec{Upsert: true, Inc: map[string]any{"n": 1}}); err != nil || upserted {
		t.Errorf("upsert of an existing document: upserted %v, %v", upserted, err)
	}
	if got := mustGet(t, c, "new").Data["n"]; got != 6.0 {
		t.Errorf("n = %v, want 6", got)
	}
}

// TestUpdateOperatorsManyAndBatch: UpdateMany and Batch apply the
// operators per document, atomically: one bad document fails the call and
// nothing is written.
func TestUpdateOperatorsManyAndBatch(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	for i := range 5 {
		mustInsert(t, c, fmt.Sprintf("d%d", i), map[string]any{"n": float64(i), "tags": []any{"x"}, "g": "a"})
	}
	matched, modified, err := c.UpdateMany(ctx, &Query{Filters: []QueryFilter{{"g", "eq", "a"}}},
		UpdateSpec{Inc: map[string]any{"n": 10}, Push: map[string]any{"tags": "y"}, Pull: map[string]any{"other": "x"}}, 0)
	if err != nil || matched != 5 || modified != 5 {
		t.Fatalf("UpdateMany: %d, %d, %v", matched, modified, err)
	}
	for i := range 5 {
		d := mustGet(t, c, fmt.Sprintf("d%d", i)).Data
		if d["n"] != float64(i+10) || !reflect.DeepEqual(d["tags"], []any{"x", "y"}) {
			t.Errorf("d%d = %v", i, d)
		}
	}
	// One document holds a string n: the whole UpdateMany fails.
	mustInsert(t, c, "bad", map[string]any{"n": "s", "g": "a"})
	before, _ := c.Find(ctx, nil)
	if _, _, err := c.UpdateMany(ctx, &Query{Filters: []QueryFilter{{"g", "eq", "a"}}}, UpdateSpec{Inc: map[string]any{"n": 1}}, 0); !errors.Is(err, ErrInvalidUpdate) {
		t.Errorf("UpdateMany over a bad document: %v", err)
	}
	after, _ := c.Find(ctx, nil)
	if !reflect.DeepEqual(before, after) {
		t.Error("a failed UpdateMany wrote")
	}
	// Batch: item 1 fails, item 0 is not applied.
	_, err = c.Batch(ctx, []WriteOp{
		{Kind: OpUpdate, ID: "d0", Update: UpdateSpec{Inc: map[string]any{"n": 1}}},
		{Kind: OpUpdate, ID: "bad", Update: UpdateSpec{AddToSet: map[string]any{"n": 1}}},
	})
	var bie *BatchItemError
	if !errors.As(err, &bie) || bie.Index != 1 || !errors.Is(err, ErrInvalidUpdate) {
		t.Errorf("Batch: %v, want item 1 with ErrInvalidUpdate", err)
	}
	if got := mustGet(t, c, "d0").Data["n"]; got != 10.0 {
		t.Errorf("d0.n = %v after a failed batch, want 10", got)
	}
	// Batch upsert with operators.
	res, err := c.Batch(ctx, []WriteOp{{Kind: OpUpdate, ID: "up", Update: UpdateSpec{Upsert: true, Inc: map[string]any{"c": 1}}}})
	if err != nil || len(res.Upserted) != 1 || !res.Upserted[0] {
		t.Errorf("Batch upsert: %+v, %v", res, err)
	}
	if got := mustGet(t, c, "up").Data; !reflect.DeepEqual(got, map[string]any{"c": 1.0}) {
		t.Errorf("batch upsert = %v", got)
	}
}

// TestUpdateOperatorNoOpModified documents what UpdateMany reports when
// an operator changes nothing (add_to_set of an element already there,
// pull of an absent element): the spec says only that modified counts
// the documents updated (lower than matched when a document stopped
// matching), so this records the current behaviour: they count as
// modified, and the stored data is unchanged.
func TestUpdateOperatorNoOpModified(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "a", map[string]any{"tags": []any{"x"}})
	mustInsert(t, c, "b", map[string]any{"tags": []any{"x"}})
	all := &Query{Filters: []QueryFilter{{"tags", "has", "x"}}}
	for _, u := range []UpdateSpec{
		{AddToSet: map[string]any{"tags": "x"}},
		{Pull: map[string]any{"tags": "absent"}},
	} {
		matched, modified, err := c.UpdateMany(ctx, all, u, 0)
		if err != nil || matched != 2 || modified != 2 {
			t.Errorf("UpdateMany(%+v) = matched %d, modified %d, %v; current behaviour is 2, 2", u, matched, modified, err)
		}
		for _, id := range []string{"a", "b"} {
			if got := mustGet(t, c, id).Data["tags"]; !reflect.DeepEqual(got, []any{"x"}) {
				t.Errorf("%s.tags = %v, want unchanged [x]", id, got)
			}
		}
	}
}

// TestStatsAndCacheCounters: Engine.Stats reports per-database and
// per-collection documents, data bytes after a flush, index bytes, and
// index names; the system database is not listed; the cache counts hits
// and misses.
func TestStatsAndCacheCounters(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	d := mustDB(t, e, "app")
	if err := d.CreateCollection(ctx, "empty", nil); err != nil {
		t.Fatal(err)
	}
	for i := range 30 {
		mustInsert(t, c, fmt.Sprintf("d%02d", i), map[string]any{"n": float64(i), "pad": strings.Repeat("p", 200)})
	}
	if err := c.CreateIndex(ctx, "z_idx", "n"); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateIndex(ctx, "a_idx", "pad"); err != nil {
		t.Fatal(err)
	}
	mustCreateDB(t, e, "other")

	st, err := e.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, ds := range st.Databases {
		names = append(names, ds.Name)
	}
	if !slices.Equal(names, []string{"app", "other"}) {
		t.Fatalf("databases %v, want [app other]", names)
	}
	app := st.Databases[0]
	if len(app.Collections) != 2 || app.Collections[0].Name != "c" || app.Collections[1].Name != "empty" {
		t.Fatalf("collections %+v", app.Collections)
	}
	cs := app.Collections[0]
	if cs.Documents != 30 || app.Documents != 30 || !slices.Equal(cs.Indexes, []string{"a_idx", "z_idx"}) || cs.IndexBytes <= 0 {
		t.Errorf("collection stats %+v (database %+v)", cs, app)
	}
	if st.Server.WALBytes <= 0 {
		t.Errorf("WAL bytes = %d", st.Server.WALBytes)
	}
	mustFlush(t, c)
	cs2, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cs2.DataBytes <= 0 { // records are compressed: no bound from the payload
		t.Errorf("data bytes after a flush = %d, want > 0", cs2.DataBytes)
	}
	ds, err := d.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ds.DataBytes != cs2.DataBytes || ds.IndexBytes < cs2.IndexBytes || len(ds.Collections[1].Indexes) != 0 {
		t.Errorf("database totals %+v, collection %+v", ds, cs2)
	}

	// Cache counters: the first read after the flush misses (or hits a
	// cache filled by the flush), the next read of the same id hits.
	before, _ := e.ServerStats()
	mustGet(t, c, "d05")
	mustGet(t, c, "d05")
	after, _ := e.ServerStats()
	if after.Cache.Hits+after.Cache.Misses < before.Cache.Hits+before.Cache.Misses+2 {
		t.Errorf("two reads moved the counters from %+v to %+v", before.Cache, after.Cache)
	}
	if after.Cache.Hits <= before.Cache.Hits {
		t.Errorf("the second read of d05 did not hit: %+v -> %+v", before.Cache, after.Cache)
	}
	if after.Cache.MaxBytes <= 0 || after.Cache.Entries == 0 {
		t.Errorf("cache stats %+v", after.Cache)
	}
}

// TestStatsCacheDisabledCountsMisses: with the cache disabled, reads of
// flushed documents count as misses and MaxBytes is 0.
func TestStatsCacheDisabledCountsMisses(t *testing.T) {
	e, c := newTestColl(t, func(o *Options) { o.CacheSize = -1 }, nil)
	mustInsert(t, c, "a", map[string]any{"x": 1})
	mustFlush(t, c)
	before, _ := e.ServerStats()
	mustGet(t, c, "a")
	after, _ := e.ServerStats()
	if after.Cache.MaxBytes != 0 || after.Cache.Hits != 0 || after.Cache.Misses <= before.Cache.Misses {
		t.Errorf("disabled cache: %+v -> %+v", before.Cache, after.Cache)
	}
}
