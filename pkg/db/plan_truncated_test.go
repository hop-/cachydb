package db

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// Truncated index values and the sort fallback (AC-7; approach §3 and §5,
// task 05 Amendment 1). A string whose encoding is longer than
// idxstore.MaxValueLen is cut, and its entry is only a candidate: a range
// or prefix bound is served from the index only when it is shorter than
// any cut value (shortValue), and the index never drives a sort while it
// holds a cut, complex, or array value (sortOption). Every answer must
// equal that of an unindexed copy.

// indexStats returns the stored counters of c's index name.
func indexStats(t *testing.T, c *Collection, name string) idxstore.IndexStats {
	t.Helper()
	st, err := c.indexStore()
	if err != nil {
		t.Fatal(err)
	}
	var stats idxstore.IndexStats
	if err := st.View(func(tx *idxstore.Tx) error {
		var ok bool
		var err error
		stats, ok, err = tx.IndexStats(name)
		if err == nil && !ok {
			err = fmt.Errorf("index %s has no stats", name)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return stats
}

// seedPairColls creates collection "c" with an index i_<field> for each
// field and collection "plain" without indexes, both holding docs (id ->
// data), flushed or not.
func seedPairColls(t *testing.T, fields []string, docs map[string]map[string]any, flushed bool) (indexed, plain *Collection) {
	t.Helper()
	ctx := context.Background()
	_, _, c := newCrashColl(t, nil, nil)
	if err := c.db.CreateCollection(ctx, "plain", nil); err != nil {
		t.Fatal(err)
	}
	p := mustColl(t, c.db, "plain")
	for _, f := range fields {
		if err := c.CreateIndex(ctx, "i_"+f, f); err != nil {
			t.Fatal(err)
		}
	}
	for id, data := range docs {
		mustInsert(t, c, id, data)
		mustInsert(t, p, id, data)
	}
	if flushed {
		mustFlush(t, c)
		mustFlush(t, p)
	}
	return c, p
}

// samePlainAnswers checks that Find and CountDocuments of q give the same
// answers on c as on plain, and returns c's plan.
func samePlainAnswers(t *testing.T, c, plain *Collection, q Query) QueryPlan {
	t.Helper()
	ctx := context.Background()
	docs, plan, err := c.FindWithPlan(ctx, &q)
	if err != nil {
		t.Fatalf("Find %s: %v", shortJS(q), err)
	}
	want, err := plain.Find(ctx, &q)
	if err != nil {
		t.Fatalf("reference Find %s: %v", shortJS(q), err)
	}
	if !reflect.DeepEqual(docs, want) {
		t.Errorf("Find %s (plan %+v) = %v, the unindexed collection gives %v", shortJS(q), plan, docIDs(docs), docIDs(want))
	}
	if q.Sort == nil && q.Limit == 0 && q.Skip == 0 {
		n, err := c.CountDocuments(ctx, &q)
		if err != nil {
			t.Fatalf("CountDocuments %s: %v", shortJS(q), err)
		}
		if n != len(want) {
			t.Errorf("CountDocuments %s = %d, the unindexed collection has %d matches", shortJS(q), n, len(want))
		}
	}
	return plan
}

// shortJS renders q with runs of 20 or more equal bytes abbreviated as
// c{n}, for readable failures.
func shortJS(q Query) string {
	s := js(q)
	var b strings.Builder
	for i := 0; i < len(s); {
		j := i
		for j < len(s) && s[j] == s[i] {
			j++
		}
		if j-i >= 20 {
			fmt.Fprintf(&b, "%c{%d}", s[i], j-i)
		} else {
			b.WriteString(s[i:j])
		}
		i = j
	}
	return b.String()
}

func TestPlanTruncatedValues(t *testing.T) {
	rep := strings.Repeat
	// prefix is the first MaxValueLen bytes every long value shares: a
	// 1,024-byte string encodes to 1,025 bytes (tag included), so the
	// entries of all long values below are cut inside it.
	prefix := rep("m", idxstore.MaxValueLen)
	long := func(c1, c2 byte, n int) string { return prefix + string(c1) + rep(string(c2), n) }
	values := []any{
		// Long values (2,000+ bytes) that differ only after the shared prefix.
		long('k', 'a', 1000), long('k', 'b', 1000), long('c', 'z', 1200), long('x', 'a', 990),
		long('a', 'a', 1000), long('m', 'q', 1100), long('m', 'b', 1050), long('f', 'f', 1000),
		long('f', 'f', 1000), // a duplicate
		rep("m", 2500), rep("l", 1500), rep("n", 1500),
		// Around the cut: 1,021 to 1,025 bytes, and 1,022- and 1,023-byte
		// prefixes with a different last byte.
		rep("m", 1021), rep("m", 1022), rep("m", 1023), rep("m", 1024), rep("m", 1025),
		rep("m", 1022) + "a", rep("m", 1022) + "z", rep("m", 1023) + "a", rep("m", 1021) + "n",
		// An escaped 0x00 is never split, so these are cut before it, at
		// MaxValueLen-1 bytes: the shortest cut entries there are.
		rep("m", 1022) + "\x00" + rep("q", 1000), rep("m", 1022) + "\x00", rep("m", 1021) + "\x00\x00" + rep("b", 999),
		// Short values, a number, a null.
		"", "a", "l", "m", "mm", "ml", "mn", "z", 5.0, nil,
	}
	docs := map[string]map[string]any{}
	for i, v := range values {
		// Ids run against the value order, so that an _id-order answer
		// cannot pass for a sorted one.
		docs[fmt.Sprintf("d%03d", len(values)-i)] = map[string]any{"s": v, "i": float64(i)}
	}
	docs["nos"] = map[string]any{"i": -1.0} // s missing

	f := func(op string, v any) []QueryFilter { return []QueryFilter{{Field: "s", Operator: op, Value: v}} }
	var queries []Query
	for _, v := range []any{long('k', 'a', 1000), long('f', 'f', 1000), long('m', 'q', 1100), rep("m", 2500),
		rep("m", 1023), rep("m", 1024), long('k', 'a', 999), prefix} {
		queries = append(queries, Query{Filters: f("eq", v)})
	}
	queries = append(queries,
		Query{Filters: f("in", []any{long('k', 'b', 1000), long('f', 'f', 1000), "m", rep("m", 1025)})},
		Query{Filters: f("in", []any{long('k', 'b', 1001), rep("l", 1500)})},
	)
	bounds := []string{
		"", "l", "m", "ml", "mm", "mn", "n",
		rep("m", 1021), rep("m", 1022), rep("m", 1023), rep("m", 1024), rep("m", 1025),
		rep("m", 1021) + "a", rep("m", 1021) + "n", rep("m", 1022) + "b", rep("m", 1023) + "a",
		rep("m", 1022) + "\x00", rep("m", 1021) + "\x00", rep("m", 1021) + "\x00\x00",
		long('f', 'f', 1000), long('f', 'f', 999), long('k', 'a', 2000), rep("m", 3000), rep("l", 1023),
	}
	for _, b := range bounds {
		for _, op := range []string{"gt", "gte", "lt", "lte"} {
			queries = append(queries, Query{Filters: f(op, b)})
		}
	}
	for _, p := range []string{"", "m", "mm", "l", rep("m", 1021), rep("m", 1022), rep("m", 1023), rep("m", 1024),
		rep("m", 1500), rep("m", 1022) + "\x00", rep("m", 1021) + "\x00", prefix + "k", prefix + "ka", long('f', 'f', 500), long('f', 'f', 1001), rep("l", 1023)} {
		queries = append(queries, Query{Filters: f("starts_with", p)})
	}
	// Sorts on s, alone and with a same-field range or prefix filter.
	sortFilters := [][]QueryFilter{
		nil, f("gte", "m"), f("gt", rep("m", 1021)), f("gte", rep("m", 1023)),
		f("lt", prefix+"m"), f("lte", "z"), f("starts_with", "mm"), f("starts_with", rep("m", 1023)),
	}
	pages := []struct{ skip, limit int }{{0, 0}, {0, 5}, {3, 4}, {10, 6}, {1, 1}}
	var sorts []Query
	for _, sf := range sortFilters {
		for _, order := range []string{"asc", "desc"} {
			for _, pg := range pages {
				sorts = append(sorts, Query{Filters: sf, Sort: []SortKey{{Field: "s", Order: order}}, Skip: pg.skip, Limit: pg.limit})
			}
		}
	}

	// "long values unflushed": the short values are flushed (the index has
	// no cut entry and may drive a sort) and the long ones are only in the
	// memtable, merged into the index walk with their whole values.
	isLong := func(data map[string]any) bool { v, ok := data["s"].(string); return ok && len(v) > 1000 }
	for _, mode := range []string{"unflushed", "flushed", "long values unflushed"} {
		t.Run(mode, func(t *testing.T) {
			var c, plain *Collection
			if mode == "long values unflushed" {
				short := map[string]map[string]any{}
				for id, data := range docs {
					if !isLong(data) {
						short[id] = data
					}
				}
				c, plain = seedPairColls(t, []string{"s"}, short, true)
				for id, data := range docs {
					if isLong(data) {
						mustInsert(t, c, id, data)
						mustInsert(t, plain, id, data)
					}
				}
			} else {
				c, plain = seedPairColls(t, []string{"s"}, docs, mode == "flushed")
			}
			stats := indexStats(t, c, "i_s")
			if (mode == "flushed") != (stats.Truncated > 0) {
				t.Fatalf("index i_s stats %+v: want truncated entries exactly when the long values are flushed", stats)
			}
			for _, q := range queries {
				samePlainAnswers(t, c, plain, q)
			}
			for _, q := range sorts {
				plan := samePlainAnswers(t, c, plain, q)
				// The index has cut entries, whose order among values
				// sharing the prefix is not exact: it must not drive the
				// sort. (Otherwise every long value comes from the memtable
				// with its whole value, and the index may drive the sort.)
				if stats.Truncated > 0 && (plan.Driver == driverIndexSort || plan.OrderProvided) {
					t.Errorf("Find %s: plan %+v drives the sort from an index with %d truncated entries",
						shortJS(q), plan, stats.Truncated)
				}
			}
		})
	}
}

// TestPlanRangeSortFallback: a range filter with a same-field sort and a
// limit streams from the index in sort order only when the index gives
// every document one exactly ordered entry. With an array or an object
// among the values (Arrays or Complex above 0) the range still drives the
// query, the documents are sorted in memory, and the answer equals the
// unindexed one.
func TestPlanRangeSortFallback(t *testing.T) {
	docs := map[string]map[string]any{}
	for i := range 200 {
		docs[fmt.Sprintf("d%03d", 199-i)] = map[string]any{"n": float64(i % 50), "o": float64(i % 40)}
	}
	docs["arr"] = map[string]any{"n": []any{3.0, 60.0, 20.0}, "o": 7.0}
	docs["arr2"] = map[string]any{"n": []any{45.0}, "o": 8.0}
	docs["obj"] = map[string]any{"n": 1.0, "o": map[string]any{"x": 30.0}}

	f := func(field, op string, v any) []QueryFilter {
		return []QueryFilter{{Field: field, Operator: op, Value: v}}
	}
	tests := []struct {
		name  string
		field string
		check func(idxstore.IndexStats) bool
		qs    []Query
	}{
		{
			name: "arrays", field: "n",
			check: func(s idxstore.IndexStats) bool { return s.Arrays > 0 },
			qs: []Query{
				{Filters: f("n", "gte", 10), Sort: []SortKey{{Field: "n"}}, Limit: 5},
				{Filters: f("n", "gte", 40), Sort: []SortKey{{Field: "n", Order: "desc"}}, Limit: 4, Skip: 2},
				{Filters: f("n", "lt", 5), Sort: []SortKey{{Field: "n", Order: "desc"}}, Limit: 6},
			},
		},
		{
			name: "complex", field: "o",
			check: func(s idxstore.IndexStats) bool { return s.Complex > 0 },
			qs: []Query{
				{Filters: f("o", "gte", 10), Sort: []SortKey{{Field: "o"}}, Limit: 5},
				{Filters: f("o", "gte", 30), Sort: []SortKey{{Field: "o", Order: "desc"}}, Limit: 4, Skip: 1},
				{Filters: f("o", "lte", 2), Sort: []SortKey{{Field: "o"}}, Limit: 3, Skip: 2},
			},
		},
	}
	for _, flushed := range []bool{false, true} {
		t.Run(fmt.Sprintf("flushed=%v", flushed), func(t *testing.T) {
			c, plain := seedPairColls(t, []string{"n", "o"}, docs, flushed)
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					stats := indexStats(t, c, "i_"+tt.field)
					if flushed && !tt.check(stats) {
						t.Fatalf("index i_%s stats %+v: the %s counter is 0 after the flush", tt.field, stats, tt.name)
					}
					for _, q := range tt.qs {
						plan := samePlainAnswers(t, c, plain, q)
						if !flushed {
							continue // the index holds no entry yet: nothing to fall back from
						}
						if plan.Driver != driverRange || plan.Index != "i_"+tt.field || plan.OrderProvided {
							t.Errorf("Find %s: plan %+v, want driver %q on i_%s without order provided",
								js(q), plan, driverRange, tt.field)
						}
					}
				})
			}
		})
	}
}
