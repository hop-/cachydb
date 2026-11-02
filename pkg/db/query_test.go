package db

import (
	"context"
	"reflect"
	"testing"
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
