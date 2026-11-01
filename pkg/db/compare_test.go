package db

import (
	"encoding/json"
	"math"
	"testing"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		name   string
		a, b   any
		want   int
		wantOK bool
	}{
		{"9 < 10 numerically", 9, 10, -1, true},
		{"10 > 9 numerically", 10.0, 9.0, 1, true},
		{"int vs float equal", 3, 3.0, 0, true},
		{"int64 vs float32", int64(2), float32(2.5), -1, true},
		{"uint8 vs int", uint8(200), -1, 1, true},
		{"json.Number vs int", json.Number("10"), 9, 1, true},
		{"json.Number vs float", json.Number("1.5"), 1.5, 0, true},
		{"json.Number vs json.Number", json.Number("9"), json.Number("10"), -1, true},
		{"negative zero equals zero", math.Copysign(0, -1), 0, 0, true},
		{"string lexical less", "apple", "banana", -1, true},
		{"string lexical digits", "9", "10", 1, true},
		{"string equal", "x", "x", 0, true},
		{"number vs string", 1, "1", 0, false},
		{"string vs number", "1", 1, 0, false},
		{"bool equality only", true, true, 0, false},
		{"bool vs bool differ", true, false, 0, false},
		{"nil vs nil", nil, nil, 0, false},
		{"nil vs number", nil, 0, 0, false},
		{"object", map[string]any{"a": 1}, map[string]any{"a": 1}, 0, false},
		{"array", []any{1}, []any{1}, 0, false},
		{"NaN", math.NaN(), 1, 0, false},
		{"invalid json.Number", json.Number("x"), 1, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := compare(tt.a, tt.b)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("compare(%#v, %#v) = (%d, %v), want (%d, %v)", tt.a, tt.b, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"int vs float", 10, 10.0, true},
		{"int vs json.Number", 10, json.Number("10"), true},
		{"float32 vs float64", float32(0.5), 0.5, true},
		{"different numbers", 9, 10, false},
		{"strings equal", "a", "a", true},
		{"strings differ", "a", "b", false},
		{"number vs string", 1, "1", false},
		{"bool equal", true, true, true},
		{"bool differ", true, false, false},
		{"bool vs string", true, "true", false},
		{"nil vs nil", nil, nil, true},
		{"nil vs zero", nil, 0, false},
		{"nil vs empty string", nil, "", false},
		{
			"objects with mixed numeric types",
			map[string]any{"a": 1, "b": []any{int64(2), "x"}},
			map[string]any{"a": 1.0, "b": []any{json.Number("2"), "x"}},
			true,
		},
		{"objects differ", map[string]any{"a": 1}, map[string]any{"a": 2}, false},
		{"objects extra key", map[string]any{"a": 1}, map[string]any{"a": 1, "b": 2}, false},
		{"typed map vs any map", map[string]int{"a": 1}, map[string]any{"a": 1.0}, true},
		{"arrays equal", []any{1, "two", true}, []any{1.0, "two", true}, true},
		{"typed slice vs any slice", []int{1, 2}, []any{1.0, 2.0}, true},
		{"arrays order matters", []any{1, 2}, []any{2, 1}, false},
		{"array vs object", []any{}, map[string]any{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := valuesEqual(tt.a, tt.b); got != tt.want {
				t.Fatalf("valuesEqual(%#v, %#v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestMatchesFilter(t *testing.T) {
	doc := &Document{ID: "d1", Data: map[string]any{
		"age":    9,
		"score":  json.Number("10.5"),
		"name":   "bob",
		"active": true,
		"note":   nil,
		"tags":   []any{"a", "b"},
		"addr":   map[string]any{"zip": 12345.0},
	}}
	tests := []struct {
		name string
		f    QueryFilter
		want bool
	}{
		{"gt numeric not lexical", QueryFilter{"age", "gt", 10}, false},
		{"lt numeric not lexical", QueryFilter{"age", "lt", 10}, true},
		{"gte equal int vs float", QueryFilter{"age", "gte", 9.0}, true},
		{"lte json.Number vs float", QueryFilter{"score", "lte", 10.5}, true},
		{"gt json.Number vs int", QueryFilter{"score", "gt", 10}, true},
		{"eq int vs float", QueryFilter{"age", "eq", 9.0}, true},
		{"eq json.Number vs float", QueryFilter{"score", "eq", 10.5}, true},
		{"eq string", QueryFilter{"name", "eq", "bob"}, true},
		{"gt string", QueryFilter{"name", "gt", "alice"}, true},
		{"lt string", QueryFilter{"name", "lt", "alice"}, false},
		{"mismatched eq", QueryFilter{"age", "eq", "9"}, false},
		{"mismatched ne", QueryFilter{"age", "ne", "9"}, true},
		{"mismatched gt", QueryFilter{"age", "gt", "1"}, false},
		{"mismatched gte", QueryFilter{"age", "gte", "1"}, false},
		{"mismatched lt", QueryFilter{"age", "lt", "99"}, false},
		{"mismatched lte", QueryFilter{"age", "lte", "99"}, false},
		{"bool eq", QueryFilter{"active", "eq", true}, true},
		{"bool ne", QueryFilter{"active", "ne", false}, true},
		{"bool gt not comparable", QueryFilter{"active", "gt", false}, false},
		{"bool gte equal not comparable", QueryFilter{"active", "gte", true}, false},
		{"nil eq nil", QueryFilter{"note", "eq", nil}, true},
		{"nil ne nil", QueryFilter{"note", "ne", nil}, false},
		{"nil ne string", QueryFilter{"note", "ne", "x"}, true},
		{"array eq", QueryFilter{"tags", "eq", []any{"a", "b"}}, true},
		{"array eq typed slice", QueryFilter{"tags", "eq", []string{"a", "b"}}, true},
		{"array ne reordered", QueryFilter{"tags", "ne", []any{"b", "a"}}, true},
		{"object eq int vs float", QueryFilter{"addr", "eq", map[string]any{"zip": 12345}}, true},
		{"object gt not comparable", QueryFilter{"addr", "gt", map[string]any{}}, false},
		{"in number", QueryFilter{"age", "in", []any{1, 9.0, "x"}}, true},
		{"in typed slice", QueryFilter{"age", "in", []int{8, 9}}, true},
		{"in string vs number", QueryFilter{"age", "in", []any{"9"}}, false},
		{"in string", QueryFilter{"name", "in", []string{"alice", "bob"}}, true},
		{"in nil element", QueryFilter{"note", "in", []any{nil}}, true},
		{"in object", QueryFilter{"addr", "in", []any{map[string]any{"zip": json.Number("12345")}}}, true},
		{"in not a slice", QueryFilter{"age", "in", 9}, false},
		{"in nil value", QueryFilter{"age", "in", nil}, false},
		{"in empty", QueryFilter{"age", "in", []any{}}, false},
		{"_id eq", QueryFilter{"_id", "eq", "d1"}, true},
		{"missing field eq", QueryFilter{"missing", "eq", nil}, false},
		{"missing field ne", QueryFilter{"missing", "ne", 1}, false},
		{"unknown operator", QueryFilter{"age", "like", 9}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesFilter(doc, tt.f); got != tt.want {
				t.Fatalf("matchesFilter(%+v) = %v, want %v", tt.f, got, tt.want)
			}
		})
	}
}
