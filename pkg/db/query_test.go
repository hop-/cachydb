package db

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// Tests of the plan-004 query features: element operators, OR groups,
// element index keys, sort, projection, count, and distinct.

func TestElementOperators(t *testing.T) {
	doc := &Document{ID: "d1", Data: map[string]any{
		"title": "The WAL Design",
		"tags":  []any{"db", "WAL-notes", 3.0, true, nil, map[string]any{"k": 1.0}},
		"nums":  []int{1, 2},
		"n":     5,
		"empty": []any{},
	}}
	tests := []struct {
		name string
		f    QueryFilter
		want bool
	}{
		{"contains case-insensitive", QueryFilter{"title", "contains", "wal"}, true},
		{"contains upper", QueryFilter{"title", "contains", "DESIGN"}, true},
		{"contains no match", QueryFilter{"title", "contains", "segment"}, false},
		{"contains empty string", QueryFilter{"title", "contains", ""}, true},
		{"contains empty string on an empty array", QueryFilter{"empty", "contains", ""}, true},
		{"contains empty string on a number array", QueryFilter{"nums", "contains", ""}, true},
		{"contains empty string on a number", QueryFilter{"n", "contains", ""}, false},
		{"contains array string element", QueryFilter{"tags", "contains", "notes"}, true},
		{"contains array non-string elements only", QueryFilter{"nums", "contains", "1"}, false},
		{"contains on number", QueryFilter{"n", "contains", "5"}, false},
		{"contains non-string value", QueryFilter{"title", "contains", 5}, false},
		{"contains missing field", QueryFilter{"nope", "contains", ""}, false},
		{"has string", QueryFilter{"tags", "has", "db"}, true},
		{"has number", QueryFilter{"tags", "has", 3}, true},
		{"has bool", QueryFilter{"tags", "has", true}, true},
		{"has null", QueryFilter{"tags", "has", nil}, true},
		{"has object", QueryFilter{"tags", "has", map[string]any{"k": 1}}, true},
		{"has typed slice", QueryFilter{"nums", "has", 2.0}, true},
		{"has type-aware", QueryFilter{"tags", "has", "3"}, false},
		{"has absent", QueryFilter{"tags", "has", "x"}, false},
		{"has on a non-array", QueryFilter{"title", "has", "The WAL Design"}, false},
		{"has missing field", QueryFilter{"nope", "has", "db"}, false},
		{"has_any one", QueryFilter{"tags", "has_any", []any{"x", "db"}}, true},
		{"has_any none", QueryFilter{"tags", "has_any", []any{"x", "y"}}, false},
		{"has_any empty", QueryFilter{"tags", "has_any", []any{}}, false},
		{"has_any typed", QueryFilter{"tags", "has_any", []string{"db"}}, true},
		{"has_any non-array value", QueryFilter{"tags", "has_any", "db"}, false},
		{"has_any on a non-array", QueryFilter{"n", "has_any", []any{5}}, false},
		{"has_all all", QueryFilter{"tags", "has_all", []any{"db", 3}}, true},
		{"has_all some", QueryFilter{"tags", "has_all", []any{"db", "x"}}, false},
		{"has_all empty", QueryFilter{"tags", "has_all", []any{}}, true},
		{"has_all non-array value", QueryFilter{"tags", "has_all", "db"}, false},
		{"has_all missing field", QueryFilter{"nope", "has_all", []any{}}, false},
		{"eq keeps whole-value semantics", QueryFilter{"tags", "eq", "db"}, false},
		{"in keeps whole-value semantics", QueryFilter{"tags", "in", []any{"db"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesFilter(doc, tt.f); got != tt.want {
				t.Fatalf("matchesFilter(%+v) = %v, want %v", tt.f, got, tt.want)
			}
		})
	}
}

// seedKB inserts a small knowledge base into c.
func seedKB(t *testing.T, c *Collection) {
	t.Helper()
	mustInsert(t, c, "k1", map[string]any{"type": "note", "title": "WAL internals", "tags": []any{"wal", "storage"}, "prio": 2})
	mustInsert(t, c, "k2", map[string]any{"type": "howto", "title": "Compaction tuning", "tags": []any{"storage", "perf"}, "prio": 1})
	mustInsert(t, c, "k3", map[string]any{"type": "note", "title": "MCP tools", "tags": []any{"mcp"}, "prio": 3})
	mustInsert(t, c, "k4", map[string]any{"type": "decision", "title": "Use zstd", "tags": "storage", "prio": "high"})
	mustInsert(t, c, "k5", map[string]any{"type": "howto", "title": "no tags"})
}

func TestFindElementOperatorsAndAny(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	seedKB(t, c)

	tests := []struct {
		name string
		q    Query
		want []string
	}{
		{"contains", Query{Filters: []QueryFilter{{"title", "contains", "wal"}}}, []string{"k1"}},
		{"has", Query{Filters: []QueryFilter{{"tags", "has", "storage"}}}, []string{"k1", "k2"}},
		{"has_any", Query{Filters: []QueryFilter{{"tags", "has_any", []any{"mcp", "perf"}}}}, []string{"k2", "k3"}},
		{"has_all", Query{Filters: []QueryFilter{{"tags", "has_all", []any{"storage", "wal"}}}}, []string{"k1"}},
		{"any groups", Query{Any: [][]QueryFilter{
			{{"type", "eq", "decision"}},
			{{"type", "eq", "note"}, {"prio", "gte", 3}},
		}}, []string{"k3", "k4"}},
		{"filters and any", Query{
			Filters: []QueryFilter{{"tags", "has", "storage"}},
			Any:     [][]QueryFilter{{{"type", "eq", "howto"}}, {{"title", "contains", "zstd"}}},
		}, []string{"k2"}},
		{"any with an empty group matches all", Query{Any: [][]QueryFilter{{}}}, []string{"k1", "k2", "k3", "k4", "k5"}},
		{"any no group matches", Query{Any: [][]QueryFilter{{{"type", "eq", "x"}}}}, []string{}},
		{"any with skip and limit", Query{Any: [][]QueryFilter{{{"type", "eq", "note"}}, {{"type", "eq", "howto"}}}, Skip: 1, Limit: 2}, []string{"k2", "k3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Find(ctx, &tt.q)
			if err != nil {
				t.Fatal(err)
			}
			if ids := docIDs(got); !reflect.DeepEqual(ids, tt.want) {
				t.Errorf("Find = %v, want %v", ids, tt.want)
			}
		})
	}
}

func TestIndexElementKeys(t *testing.T) {
	ix := newIndex("by_tags", "tags")
	ix.add("a", idxDoc("a", map[string]any{"tags": []any{"x", "y", "x", 1.0, map[string]any{"o": 1.0}, []any{"n"}}}))
	ix.add("b", idxDoc("b", map[string]any{"tags": []any{"y", "z"}}))
	ix.add("c", idxDoc("c", map[string]any{"tags": "x"}))
	ix.add("d", idxDoc("d", map[string]any{"tags": []any{}}))

	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range ix.keys {
			if !yield(k) {
				return
			}
		}
	})
	if want := []string{"n:1", "s:x", "s:y", "s:z"}; !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v (nested objects and arrays skipped)", keys, want)
	}
	if got := ix.lookup("x"); !slices.Equal(got, []string{"a", "c"}) {
		t.Errorf("lookup(x) = %v", got)
	}
	if got, ok := ix.lookupAny([]any{"x", "z"}); !ok || !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("lookupAny(x, z) = %v, %v", got, ok)
	}
	if _, ok := ix.lookupAny([]any{"x", map[string]any{}}); ok {
		t.Error("lookupAny with an unindexable value used the index")
	}
	if got, ok := ix.lookupAll([]any{"y", "x", map[string]any{}}); !ok || !slices.Equal(got, []string{"a"}) {
		t.Errorf("lookupAll(y, x, {}) = %v, %v", got, ok)
	}
	if _, ok := ix.lookupAll([]any{map[string]any{}}); ok {
		t.Error("lookupAll without an indexable value used the index")
	}

	ix.remove("a", idxDoc("a", map[string]any{"tags": []any{"x", "y", "x", 1.0, map[string]any{"o": 1.0}, []any{"n"}}}))
	if _, ok := ix.keys["n:1"]; ok || !slices.Equal(ix.lookup("x"), []string{"c"}) {
		t.Errorf("remove left element keys behind: %v", ix.keys)
	}
}

func TestFindElementOperatorsUseIndex(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_tags", "tags"); err != nil {
		t.Fatal(err)
	}
	seedKB(t, c)
	for i := range 20 {
		mustInsert(t, c, "filler"+string(rune('a'+i)), map[string]any{"tags": []any{"other"}})
	}

	tests := []struct {
		name     string
		f        QueryFilter
		want     []string
		examined int
	}{
		{"has", QueryFilter{"tags", "has", "storage"}, []string{"k1", "k2"}, 3}, // k4's scalar "storage" is a candidate
		{"has_any union", QueryFilter{"tags", "has_any", []any{"mcp", "wal"}}, []string{"k1", "k3"}, 2},
		{"has_all intersection", QueryFilter{"tags", "has_all", []any{"storage", "perf"}}, []string{"k2"}, 1},
		{"has_all with unindexable values", QueryFilter{"tags", "has_all", []any{"perf", map[string]any{}}}, []string{}, 1},
		{"eq rechecked", QueryFilter{"tags", "eq", "storage"}, []string{"k4"}, 3},
		{"has absent", QueryFilter{"tags", "has", "nothing"}, []string{}, 0},
		{"contains scans", QueryFilter{"tags", "contains", "stor"}, []string{"k1", "k2", "k4"}, 25},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs, examined, err := c.find(ctx, &Query{Filters: []QueryFilter{tt.f}})
			if err != nil {
				t.Fatal(err)
			}
			if ids := docIDs(docs); !reflect.DeepEqual(ids, tt.want) {
				t.Errorf("Find = %v, want %v", ids, tt.want)
			}
			if examined != tt.examined {
				t.Errorf("examined %d candidates, want %d", examined, tt.examined)
			}
		})
	}
}

// TestRecoveryOldIndexSnapshotRebuilds: a version-1 index snapshot
// (whole-value keys, before plan 004) is rejected at open and the index
// rebuilt with element keys.
func TestRecoveryOldIndexSnapshotRebuilds(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "s", map[string]any{"g": "a"})
	mustInsert(t, c, "arr", map[string]any{"g": []any{"a", "b"}})
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// What plan 003 wrote: arrays not indexed, sidecar version 1.
	snap, err := segment.ReadIndexSnapshot(c.dir, "by_g")
	if err != nil {
		t.Fatal(err)
	}
	snap.Keys = map[string][]string{"s:a": {"s"}}
	if err := segment.WriteIndexSnapshot(c.dir, snap); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.dir, "idx-by_g.snap")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(data[4:], 1)
	end := len(data) - 4
	binary.LittleEndian.PutUint32(data[end:], crc32.Checksum(data[:end], crc32.MakeTable(crc32.Castagnoli)))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	e = openEngine(t, opts)
	if got := e.recov.indexesRebuilt.Load(); got != 1 {
		t.Errorf("indexes rebuilt = %d, want 1", got)
	}
	if got := e.recov.snapshotsLoaded.Load(); got != 0 {
		t.Errorf("snapshots loaded = %d, want 0", got)
	}
	c = mustColl(t, mustDB(t, e, "app"), "c")
	docs, examined, err := c.find(ctx, &Query{Filters: []QueryFilter{{"g", "has", "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	if ids := docIDs(docs); !slices.Equal(ids, []string{"arr"}) || examined != 1 {
		t.Errorf("has b = %v after examining %d, want [arr] from the index", ids, examined)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Close wrote a current snapshot, which loads.
	e = openEngine(t, opts)
	if got := e.recov.snapshotsLoaded.Load(); got != 1 {
		t.Errorf("snapshots loaded after a clean close = %d, want 1", got)
	}
}

func TestFindSort(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	values := map[string]any{
		"m1": nil, "n1": 2.0, "n2": 10.0, "n3": 2.0, "s1": "b", "s2": "a",
		"t1": true, "f1": false, "a1": []any{1.0}, "o1": map[string]any{"k": 1.0},
	}
	group := func(id string) int {
		if slices.Contains([]string{"n1", "s1", "t1", "x1"}, id) {
			return 1
		}
		return 0
	}
	for id, v := range values {
		mustInsert(t, c, id, map[string]any{"v": v, "g": group(id)})
	}
	mustInsert(t, c, "x1", map[string]any{"g": 1}) // v missing

	tests := []struct {
		name string
		sort []SortKey
		want []string
	}{
		{"total order asc", []SortKey{{Field: "v"}}, []string{"x1", "m1", "f1", "t1", "n1", "n3", "n2", "s2", "s1", "a1", "o1"}},
		{"total order desc", []SortKey{{Field: "v", Order: "desc"}}, []string{"o1", "a1", "s1", "s2", "n2", "n1", "n3", "t1", "f1", "m1", "x1"}},
		{"_id asc", []SortKey{{Field: "_id", Order: "asc"}}, []string{"a1", "f1", "m1", "n1", "n2", "n3", "o1", "s1", "s2", "t1", "x1"}},
		{"_id desc", []SortKey{{Field: "_id", Order: "desc"}}, []string{"x1", "t1", "s2", "s1", "o1", "n3", "n2", "n1", "m1", "f1", "a1"}},
		{"multiple keys", []SortKey{{Field: "g", Order: "desc"}, {Field: "v", Order: "asc"}}, []string{"x1", "t1", "n1", "s1", "m1", "f1", "n3", "n2", "s2", "a1", "o1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Find(ctx, &Query{Sort: tt.sort})
			if err != nil {
				t.Fatal(err)
			}
			if ids := docIDs(got); !slices.Equal(ids, tt.want) {
				t.Fatalf("Find = %v, want %v", ids, tt.want)
			}
			// Deterministic paging: pages of 3 concatenate to the full order.
			var paged []string
			for skip := 0; skip < len(tt.want); skip += 3 {
				page, err := c.Find(ctx, &Query{Sort: tt.sort, Skip: skip, Limit: 3})
				if err != nil {
					t.Fatal(err)
				}
				paged = append(paged, docIDs(page)...)
			}
			if !slices.Equal(paged, tt.want) {
				t.Errorf("paged = %v, want %v", paged, tt.want)
			}
		})
	}

	t.Run("filters and sort", func(t *testing.T) {
		got, err := c.Find(ctx, &Query{Filters: []QueryFilter{{"v", "gte", 2}}, Sort: []SortKey{{Field: "v", Order: "desc"}}, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		if ids := docIDs(got); !slices.Equal(ids, []string{"n2", "n1"}) {
			t.Errorf("Find = %v, want [n2 n1]", ids)
		}
	})
	t.Run("invalid order", func(t *testing.T) {
		if _, err := c.Find(ctx, &Query{Sort: []SortKey{{Field: "v", Order: "up"}}}); err == nil {
			t.Error("order up accepted")
		}
		if _, err := c.Find(ctx, &Query{Sort: []SortKey{{Order: "asc"}}}); err == nil {
			t.Error("empty sort field accepted")
		}
	})
}

func TestFindProjection(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	seedKB(t, c)
	got, err := c.Find(ctx, &Query{
		Filters: []QueryFilter{{"type", "eq", "note"}}, // filter on a field that is not returned
		Sort:    []SortKey{{Field: "prio", Order: "desc"}},
		Fields:  []string{"title", "tags", "nope", "_id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []*Document{
		{ID: "k3", Data: map[string]any{"title": "MCP tools", "tags": []any{"mcp"}}},
		{ID: "k1", Data: map[string]any{"title": "WAL internals", "tags": []any{"wal", "storage"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Find = %v, want %v", got, want)
	}
	got[0].Data["tags"].([]any)[0] = "mutated"
	if again := mustGet(t, c, "k3"); again.Data["tags"].([]any)[0] != "mcp" {
		t.Error("projected value shares memory with the stored document")
	}
	all, err := c.Find(ctx, &Query{Fields: []string{}, Limit: 1})
	if err != nil || len(all) != 1 || len(all[0].Data) != 4 {
		t.Errorf("empty field list = %v, %v; want whole documents", all, err)
	}
}
