package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// Query plans (AC-7, approach-index-and-query §5): each index-served
// filter, OR groups, a single-key sort on an indexed field, and an
// unfiltered limit drive the query from an index or the key table, and a
// driver that provides the requested order stops at skip+limit.

// seedPlanColls creates two collections with the same 1,000 documents:
// "c" with indexes on n, s, and t, and "plain" without indexes, whose
// answers (a key-table walk) are the reference. n is missing on every 7th
// document, t is an array.
func seedPlanColls(t *testing.T, flushed bool) (indexed, plain *Collection) {
	t.Helper()
	ctx := context.Background()
	_, _, c := newCrashColl(t, nil, nil)
	d := c.db
	if err := d.CreateCollection(ctx, "plain", nil); err != nil {
		t.Fatal(err)
	}
	p := mustColl(t, d, "plain")
	for _, f := range []string{"n", "s", "t"} {
		if err := c.CreateIndex(ctx, "i_"+f, f); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 1000 {
		data := map[string]any{"n": float64(i % 100), "s": fmt.Sprintf("s%03d", i%250), "t": []any{fmt.Sprint("t", i%10), "all"}}
		if i%7 == 0 {
			delete(data, "n")
		}
		id := fmt.Sprintf("d%04d", i)
		mustInsert(t, c, id, data)
		mustInsert(t, p, id, data)
	}
	if flushed {
		mustFlush(t, c)
		mustFlush(t, p)
	}
	return c, p
}

func TestPlanDrivers(t *testing.T) {
	ctx := context.Background()
	f := func(field, op string, v any) QueryFilter { return QueryFilter{Field: field, Operator: op, Value: v} }
	tests := []struct {
		name    string
		q       Query
		driver  string
		index   string
		ordered bool
		// maxExamined bounds the documents read; with an order-providing
		// driver and a limit it is skip+limit (early stop).
		maxExamined int
	}{
		{"eq", Query{Filters: []QueryFilter{f("n", "eq", 5)}, Limit: 3}, driverIndex, "i_n", true, 3},
		{"eq without limit reads only its matches", Query{Filters: []QueryFilter{f("n", "eq", 5)}}, driverIndex, "i_n", true, 9},
		{"in", Query{Filters: []QueryFilter{f("n", "in", []any{5, 6})}, Limit: 3}, driverIndex, "i_n", true, 3},
		{"gt", Query{Filters: []QueryFilter{f("n", "gt", 97)}, Limit: 3}, driverRange, "i_n", false, 3},
		{"gt without limit", Query{Filters: []QueryFilter{f("n", "gt", 97)}}, driverRange, "i_n", false, 18},
		{"gte", Query{Filters: []QueryFilter{f("n", "gte", 98)}, Limit: 2, Skip: 1}, driverRange, "i_n", false, 3},
		{"lt", Query{Filters: []QueryFilter{f("n", "lt", 2)}, Limit: 4}, driverRange, "i_n", false, 4},
		{"lte", Query{Filters: []QueryFilter{f("n", "lte", 1)}, Limit: 2}, driverRange, "i_n", false, 2},
		{"starts_with", Query{Filters: []QueryFilter{f("s", "starts_with", "s24")}, Limit: 5}, driverRange, "i_s", false, 5},
		{"exists false", Query{Filters: []QueryFilter{f("n", "exists", false)}, Limit: 4}, driverIndex, "i_n", true, 4},
		{"exists true", Query{Filters: []QueryFilter{f("s", "exists", true)}, Limit: 4}, driverRange, "i_s", false, 4},
		{"has", Query{Filters: []QueryFilter{f("t", "has", "t3")}, Limit: 4}, driverIndex, "i_t", true, 4},
		{"has_any", Query{Filters: []QueryFilter{f("t", "has_any", []any{"t3", "t4"})}, Limit: 4}, driverIndex, "i_t", true, 4},
		{"has_all", Query{Filters: []QueryFilter{f("t", "has_all", []any{"t3", "all"})}, Limit: 4}, driverIndex, "i_t", true, 4},
		{"or groups", Query{Any: [][]QueryFilter{{f("n", "eq", 3)}, {f("s", "eq", "s004")}}, Limit: 4}, driverUnion, "i_n,i_s", true, 4},
		{"sort asc", Query{Sort: []SortKey{{Field: "s"}}, Limit: 3}, driverIndexSort, "i_s", true, 3},
		{"sort desc", Query{Sort: []SortKey{{Field: "s", Order: "desc"}}, Limit: 3}, driverIndexSort, "i_s", true, 3},
		{"sort desc with missing values", Query{Sort: []SortKey{{Field: "n", Order: "desc"}}, Limit: 3, Skip: 2}, driverIndexSort, "i_n", true, 5},
		{"sort asc puts missing first", Query{Sort: []SortKey{{Field: "n"}}, Limit: 3}, driverIndexSort, "i_n", true, 3},
		{"unfiltered limit", Query{Limit: 3, Skip: 1}, driverKeydir, "", true, 4},
		{"_id desc", Query{Limit: 3, Sort: []SortKey{{Field: "_id", Order: "desc"}}}, driverKeydir, "", true, 3},
		{"_id in", Query{Filters: []QueryFilter{f("_id", "in", []any{"d0001", "d0002", "nope"})}}, driverIDs, "", true, 2},
		{"_id eq", Query{Filters: []QueryFilter{f("_id", "eq", "d0500")}}, driverIDs, "", true, 1},
		{"a filter that matches nothing", Query{Filters: []QueryFilter{f("n", "gt", true)}}, driverNone, "", true, 0},
	}
	for _, flushed := range []bool{false, true} {
		t.Run(fmt.Sprintf("flushed=%v", flushed), func(t *testing.T) {
			c, plain := seedPlanColls(t, flushed)
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					docs, plan, err := c.FindWithPlan(ctx, &tt.q)
					if err != nil {
						t.Fatal(err)
					}
					if plan.Driver != tt.driver || plan.Index != tt.index || plan.OrderProvided != tt.ordered {
						t.Errorf("plan = %+v, want driver %q, index %q, order provided %v", plan, tt.driver, tt.index, tt.ordered)
					}
					if plan.Examined > tt.maxExamined {
						t.Errorf("examined %d documents, want at most %d", plan.Examined, tt.maxExamined)
					}
					if plan.Returned != len(docs) {
						t.Errorf("plan.Returned = %d, %d documents returned", plan.Returned, len(docs))
					}
					want, refPlan, err := plain.FindWithPlan(ctx, &tt.q)
					if err != nil {
						t.Fatal(err)
					}
					if tt.driver != driverIDs && tt.driver != driverNone && tt.q.Filters == nil && tt.q.Any == nil && refPlan.Driver != driverKeydir {
						t.Errorf("the reference collection has no index but used %+v", refPlan)
					}
					if !reflect.DeepEqual(docs, want) {
						t.Errorf("Find = %v, the unindexed collection gives %v", docIDs(docs), docIDs(want))
					}
					if len(docs) == 0 && tt.driver != driverNone {
						t.Error("the query returned nothing; it should test a non-empty result")
					}
				})
			}

			// Counts with one index-served filter read no documents.
			for _, q := range []*Query{
				{Filters: []QueryFilter{f("n", "eq", 5)}},
				{Filters: []QueryFilter{f("n", "in", []any{5, 6})}},
				{Filters: []QueryFilter{f("n", "gte", 90)}},
				{Filters: []QueryFilter{f("s", "starts_with", "s1")}},
				{Filters: []QueryFilter{f("n", "exists", false)}},
				{Filters: []QueryFilter{f("t", "has", "t1")}},
			} {
				n, examined, err := c.countDocuments(ctx, q)
				if err != nil {
					t.Fatal(err)
				}
				wantN, err := plain.CountDocuments(ctx, q)
				if err != nil || n != wantN || examined != 0 {
					t.Errorf("count %s = %d after reading %d, want %d after 0 (%v)", js(q), n, examined, wantN, err)
				}
			}
		})
	}
}

// TestPlanRangeWithMatchingSortStopsEarly: a range filter on an indexed
// field produces its candidates in value order, then _id order, which is
// the order of a single sort key on the same field. Approach §5: "When the
// driver's order is the requested order, stream and stop at skip + limit."
// So such a query reads skip+limit documents, whatever the size of the
// range, in both directions.
func TestPlanRangeWithMatchingSortStopsEarly(t *testing.T) {
	ctx := context.Background()
	c, plain := seedPlanColls(t, true)
	for _, q := range []Query{
		{Filters: []QueryFilter{{"n", "gte", 50}}, Sort: []SortKey{{Field: "n"}}, Limit: 3},
		{Filters: []QueryFilter{{"n", "lt", 50}}, Sort: []SortKey{{Field: "n", Order: "desc"}}, Limit: 3, Skip: 1},
		{Filters: []QueryFilter{{"s", "starts_with", "s1"}}, Sort: []SortKey{{Field: "s"}}, Limit: 2},
	} {
		docs, plan, err := c.FindWithPlan(ctx, &q)
		if err != nil {
			t.Fatal(err)
		}
		want, err := plain.Find(ctx, &q)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(docs, want) {
			t.Errorf("Find %s = %v, want %v", js(q), docIDs(docs), docIDs(want))
		}
		if !plan.OrderProvided || plan.Examined > q.Skip+q.Limit || plan.Index != "i_"+q.Sort[0].Field {
			t.Errorf("Find %s: plan %+v; want the %s index to provide the order and at most %d documents read",
				js(q), plan, q.Sort[0].Field, q.Skip+q.Limit)
		}
	}
}

// TestPlanOverlayMerge: documents changed in the memtable after a flush
// are merged with the index stream in the requested order and never
// returned twice, and their stale index entries are skipped.
func TestPlanOverlayMerge(t *testing.T) {
	ctx := context.Background()
	c, plain := seedPlanColls(t, true)
	for i := 0; i < 1000; i += 37 {
		id := fmt.Sprintf("d%04d", i)
		data := map[string]any{"n": float64(1000 - i), "s": "s000", "t": []any{"t3"}}
		for _, coll := range []*Collection{c, plain} {
			if _, err := coll.Replace(ctx, id, &Document{Data: data}, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, id := range []string{"d0005", "d0500"} {
		for _, coll := range []*Collection{c, plain} {
			if err := coll.Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, q := range []Query{
		{Sort: []SortKey{{Field: "n", Order: "desc"}}, Limit: 10},
		{Sort: []SortKey{{Field: "s"}}, Limit: 12, Skip: 3},
		{Filters: []QueryFilter{{"s", "eq", "s000"}}},
		{Filters: []QueryFilter{{"n", "gte", 99}}, Sort: []SortKey{{Field: "n"}}},
		{Filters: []QueryFilter{{"t", "has", "t3"}}, Limit: 50},
		{Any: [][]QueryFilter{{{"n", "eq", 963}}, {{"s", "eq", "s005"}}}},
		{Limit: 10, Skip: 3},
	} {
		got, plan, err := c.FindWithPlan(ctx, &q)
		if err != nil {
			t.Fatal(err)
		}
		want, err := plain.Find(ctx, &q)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Find %s (plan %+v) = %v, want %v", js(q), plan, docIDs(got), docIDs(want))
		}
		seen := map[string]bool{}
		for _, d := range got {
			if seen[d.ID] {
				t.Errorf("Find %s returned %s twice", js(q), d.ID)
			}
			seen[d.ID] = true
		}
	}
}

// TestQueryCompileErrors: an unknown operator, a non-boolean exists, and a
// non-string starts_with are errors from Find, CountDocuments, Distinct,
// and every conditional write, and a write with such a condition writes
// nothing.
func TestQueryCompileErrors(t *testing.T) {
	ctx := context.Background()
	_, e, c := newCrashColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_a", "a"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "x", map[string]any{"a": 1.0})
	for _, bad := range []QueryFilter{
		{"a", "like", 1},
		{"a", "", 1},
		{"a", "EQ", 1},
		{"a", "exists", "yes"},
		{"a", "exists", nil},
		{"a", "starts_with", 1},
		{"a", "starts_with", []any{"a"}},
	} {
		name := fmt.Sprintf("%s %v", bad.Operator, bad.Value)
		calls := []struct {
			what string
			run  func() error
		}{
			{"Find", func() error { _, err := c.Find(ctx, &Query{Filters: []QueryFilter{bad}}); return err }},
			{"Find in an OR group", func() error {
				_, err := c.Find(ctx, &Query{Any: [][]QueryFilter{{{"a", "eq", 1}}, {bad}}})
				return err
			}},
			{"FindWithPlan", func() error { _, _, err := c.FindWithPlan(ctx, &Query{Filters: []QueryFilter{bad}}); return err }},
			{"CountDocuments", func() error { _, err := c.CountDocuments(ctx, &Query{Filters: []QueryFilter{bad}}); return err }},
			{"Distinct", func() error { _, err := c.Distinct(ctx, "a", &Query{Filters: []QueryFilter{bad}}); return err }},
		}
		for _, call := range calls {
			if err := call.run(); err == nil {
				t.Errorf("%s with %s: no error", call.what, name)
			}
		}
		writes := []struct {
			what string
			run  func() error
		}{
			{"UpdateWith", func() error {
				_, _, err := c.UpdateWith(ctx, "x", UpdateSpec{Set: map[string]any{"b": 1}, If: Condition{bad}})
				return err
			}},
			{"upsert", func() error {
				_, _, err := c.UpdateWith(ctx, "new", UpdateSpec{Set: map[string]any{"b": 1}, If: Condition{bad}, Upsert: true})
				return err
			}},
			{"Replace", func() error {
				_, err := c.Replace(ctx, "x", &Document{Data: map[string]any{"a": 2}}, Condition{bad})
				return err
			}},
			{"DeleteWith", func() error { return c.DeleteWith(ctx, "x", Condition{bad}) }},
		}
		for _, w := range writes {
			off := e.wal.NextOffset()
			err := w.run()
			if err == nil || errors.Is(err, ErrConditionFailed) {
				t.Errorf("%s with %s = %v, want a query error (not a failed condition)", w.what, name, err)
			}
			if got := e.wal.NextOffset(); got != off {
				t.Errorf("%s with %s appended to the WAL", w.what, name)
			}
		}
	}
	if d := mustGet(t, c, "x"); !reflect.DeepEqual(d.Data, map[string]any{"a": 1.0}) {
		t.Errorf("document x = %v after the rejected writes", d.Data)
	}
	if _, err := c.Get(ctx, "new"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(new) = %v, want ErrNotFound", err)
	}
	if c.Count() != 1 {
		t.Errorf("Count = %d, want 1", c.Count())
	}
	// The valid forms are accepted.
	for _, ok := range []QueryFilter{{"a", "exists", true}, {"a", "exists", false}, {"a", "starts_with", ""}} {
		if _, err := c.Find(ctx, &Query{Filters: []QueryFilter{ok}}); err != nil {
			t.Errorf("Find with %v: %v", ok, err)
		}
	}
}

// TestExistsAndStartsWith pins the semantics of the two operators added in
// this plan, with and without an index: exists true matches any present
// value (null, arrays, and objects included); starts_with is a
// case-sensitive byte prefix of a string value, never of array elements.
func TestExistsAndStartsWith(t *testing.T) {
	ctx := context.Background()
	_, _, c := newCrashColl(t, nil, nil)
	for id, v := range map[string]any{
		"null": nil, "str": "abc", "upper": "Abc", "short": "a", "empty": "", "num": 1.0,
		"arr": []any{"abc"}, "obj": map[string]any{"x": "abc"}, "zero": "a\x00b",
	} {
		mustInsert(t, c, id, map[string]any{"v": v})
	}
	mustInsert(t, c, "missing", map[string]any{"w": 1.0})
	tests := []struct {
		f    QueryFilter
		want []string
	}{
		{QueryFilter{"v", "exists", true}, []string{"arr", "empty", "null", "num", "obj", "short", "str", "upper", "zero"}},
		{QueryFilter{"v", "exists", false}, []string{"missing"}},
		{QueryFilter{"v", "starts_with", "a"}, []string{"short", "str", "zero"}},
		{QueryFilter{"v", "starts_with", "ab"}, []string{"str"}},
		{QueryFilter{"v", "starts_with", "a\x00"}, []string{"zero"}},
		{QueryFilter{"v", "starts_with", ""}, []string{"empty", "short", "str", "upper", "zero"}},
		{QueryFilter{"v", "starts_with", "abcd"}, []string{}},
	}
	check := func(t *testing.T) {
		for _, tt := range tests {
			docs, err := c.Find(ctx, &Query{Filters: []QueryFilter{tt.f}})
			if err != nil {
				t.Fatal(err)
			}
			if got := docIDs(docs); !slices.Equal(got, tt.want) {
				t.Errorf("%s %q: %v, want %v", tt.f.Operator, fmt.Sprint(tt.f.Value), got, tt.want)
			}
			if n, err := c.CountDocuments(ctx, &Query{Filters: []QueryFilter{tt.f}}); err != nil || n != len(tt.want) {
				t.Errorf("count %s %v = %d, %v; want %d", tt.f.Operator, tt.f.Value, n, err, len(tt.want))
			}
		}
	}
	t.Run("no index", check)
	if err := c.CreateIndex(ctx, "by_v", "v"); err != nil {
		t.Fatal(err)
	}
	t.Run("unflushed index", check)
	mustFlush(t, c)
	t.Run("flushed index", check)
}

// TestPlanConcurrentQueries runs queries of every plan while writes,
// flushes, and compactions run, which retire segments under open
// snapshots; results must stay exact.
func TestPlanConcurrentQueries(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, func(o *Options) { o.DataSegmentSize = 2 << 10; o.CompactionRatio = 0.2 }, nil)
	for _, f := range []string{"n", "s"} {
		if err := c.CreateIndex(ctx, "i_"+f, f); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 300 {
		mustInsert(t, c, fmt.Sprintf("d%04d", i), map[string]any{"n": float64(i % 50), "s": fmt.Sprintf("s%03d", i%100), "pad": strings.Repeat("x", 100)})
	}
	done := make(chan struct{})
	errs := make(chan error, 2)
	go func() { // the writer keeps n and s, so results stay checkable
		for i := 0; ; i++ {
			select {
			case <-done:
				errs <- nil
				return
			default:
			}
			if _, err := c.Update(ctx, fmt.Sprintf("d%04d", i%300), map[string]any{"pad": strings.Repeat("y", i%120)}); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-done:
				errs <- nil
				return
			default:
			}
			if err := c.flush(); err != nil {
				errs <- err
				return
			}
			if _, err := c.compact(ctx); err != nil {
				errs <- err
				return
			}
		}
	}()
	queries := []Query{
		{Sort: []SortKey{{Field: "s"}}, Limit: 5},
		{Sort: []SortKey{{Field: "n", Order: "desc"}}, Limit: 7},
		{Filters: []QueryFilter{{"n", "gte", 45}}, Limit: 5},
		{Filters: []QueryFilter{{"s", "starts_with", "s09"}}},
		{Filters: []QueryFilter{{"n", "eq", 3}}},
		{Any: [][]QueryFilter{{{"n", "eq", 1}}, {{"s", "eq", "s002"}}}},
		{Limit: 10, Skip: 20},
	}
	want := make([][]string, len(queries))
	for i := range queries {
		docs, err := c.Find(ctx, &queries[i])
		if err != nil {
			t.Fatal(err)
		}
		want[i] = docIDs(docs)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for n := 0; time.Now().Before(deadline) && !t.Failed(); n++ {
		i := n % len(queries)
		docs, err := c.Find(ctx, &queries[i])
		if err != nil {
			t.Errorf("query %d: %v", i, err)
			break
		}
		if got := docIDs(docs); !slices.Equal(got, want[i]) {
			t.Errorf("query %d = %v, want %v", i, got, want[i])
		}
		if cnt, err := c.CountDocuments(ctx, &Query{Filters: []QueryFilter{{"n", "lt", 10}}}); err != nil || cnt != 60 {
			t.Errorf("count = %d, %v; want 60", cnt, err)
		}
		if vals, err := c.Distinct(ctx, "n", &Query{Filters: []QueryFilter{{"n", "lt", 3}}}); err != nil || len(vals) != 3 {
			t.Errorf("distinct = %v, %v; want 0, 1, 2", vals, err)
		}
	}
	close(done)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
