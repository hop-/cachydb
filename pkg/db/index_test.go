package db

import (
	"encoding/json"
	"maps"
	"math"
	"slices"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

func TestIndexKey(t *testing.T) {
	tests := []struct {
		name   string
		v      any
		want   string
		wantOK bool
	}{
		{"int", 1, "n:1", true},
		{"float equal to int", 1.0, "n:1", true},
		{"json.Number", json.Number("1"), "n:1", true},
		{"uint64", uint64(42), "n:42", true},
		{"fraction", 2.5, "n:2.5", true},
		{"negative", -3, "n:-3", true},
		{"large", 1e21, "n:1e+21", true},
		{"negative zero", math.Copysign(0, -1), "n:0", true},
		{"string", "1", "s:1", true},
		{"empty string", "", "s:", true},
		{"string with colon", "n:1", "s:n:1", true},
		{"true", true, "b:t", true},
		{"false", false, "b:f", true},
		{"nil", nil, "z:", true},
		{"object", map[string]any{"a": 1}, "", false},
		{"array", []any{1}, "", false},
		{"typed slice", []string{"a"}, "", false},
		{"NaN", math.NaN(), "", false},
		{"invalid json.Number", json.Number("x"), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := indexKey(tt.v)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("indexKey(%#v) = (%q, %v), want (%q, %v)", tt.v, got, ok, tt.want, tt.wantOK)
			}
		})
	}

	t.Run("number and string differ", func(t *testing.T) {
		n, _ := indexKey(1)
		s, _ := indexKey("1")
		if n == s {
			t.Fatalf("indexKey(1) == indexKey(\"1\") == %q", n)
		}
	})
}

func idxDoc(id string, data map[string]any) *Document {
	return &Document{ID: id, Data: data}
}

func TestIndexMultiValue(t *testing.T) {
	ix := newIndex("by_age", "age")
	docs := map[string]*Document{
		"c": idxDoc("c", map[string]any{"age": 30}),
		"a": idxDoc("a", map[string]any{"age": 30.0}),
		"b": idxDoc("b", map[string]any{"age": json.Number("30")}),
		"d": idxDoc("d", map[string]any{"age": "30"}),
		"e": idxDoc("e", map[string]any{"age": 31}),
		"f": idxDoc("f", map[string]any{"name": "no age"}),
		"g": idxDoc("g", map[string]any{"age": map[string]any{"years": 30}}),
		"h": idxDoc("h", map[string]any{"age": []any{30}}),
		"i": idxDoc("i", map[string]any{"age": nil}),
	}
	for _, id := range slices.Sorted(maps.Keys(docs)) {
		ix.add(id, docs[id])
	}

	tests := []struct {
		name string
		v    any
		want []string
	}{
		{"same value all returned sorted, array elements included", 30, []string{"a", "b", "c", "h"}},
		{"float lookup", 30.0, []string{"a", "b", "c", "h"}},
		{"string kept apart", "30", []string{"d"}},
		{"other number", 31, []string{"e"}},
		{"null", nil, []string{"i"}},
		{"absent value", 99, nil},
		{"object not indexable", map[string]any{"years": 30}, nil},
		{"array not indexable", []any{30}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ix.lookup(tt.v); !slices.Equal(got, tt.want) || (tt.want == nil && got != nil) {
				t.Fatalf("lookup(%#v) = %#v, want %#v", tt.v, got, tt.want)
			}
		})
	}

	t.Run("missing fields have no entry, non-indexable ones only a marker", func(t *testing.T) {
		total := 0
		for _, ids := range ix.keys {
			total += len(ids)
		}
		if total != 9 {
			t.Fatalf("indexed %d ids, want 9 (a, b, c, d, e, h, i; h and g under markers)", total)
		}
		if !slices.Equal(sortedIDs(ix.keys[unindexedMarker]), []string{"g"}) || !slices.Equal(sortedIDs(ix.keys[arrayMarker]), []string{"h"}) {
			t.Fatalf("markers = %v / %v, want [g] / [h]", ix.keys[unindexedMarker], ix.keys[arrayMarker])
		}
	})

	t.Run("removing one keeps the others", func(t *testing.T) {
		ix.remove("b", docs["b"])
		if got := ix.lookup(30); !slices.Equal(got, []string{"a", "c", "h"}) {
			t.Fatalf("after remove b: lookup(30) = %v, want [a c h]", got)
		}
		ix.remove("a", docs["a"])
		ix.remove("c", docs["c"])
		ix.remove("h", docs["h"])
		if got := ix.lookup(30); got != nil {
			t.Fatalf("after removing all: lookup(30) = %v, want nil", got)
		}
		if _, ok := ix.keys["n:30"]; ok {
			t.Fatal("empty key set not deleted")
		}
		if got := ix.lookup("30"); !slices.Equal(got, []string{"d"}) {
			t.Fatalf("lookup(\"30\") = %v, want [d]", got)
		}
	})

	t.Run("removing absent or non-indexable is a no-op", func(t *testing.T) {
		ix.remove("zz", idxDoc("zz", map[string]any{"age": 31}))
		ix.remove("f", docs["f"])
		ix.remove("g", docs["g"])
		ix.remove("x", nil)
		if got := ix.lookup(31); !slices.Equal(got, []string{"e"}) {
			t.Fatalf("lookup(31) = %v, want [e]", got)
		}
	})

	t.Run("lookup result is a copy", func(t *testing.T) {
		got := ix.lookup(31)
		got[0] = "mutated"
		if again := ix.lookup(31); !slices.Equal(again, []string{"e"}) {
			t.Fatalf("lookup(31) = %v after mutating a prior result", again)
		}
	})
}

func TestIndexIDField(t *testing.T) {
	ix := newIndex("by_id", "_id")
	ix.add("x1", idxDoc("x1", map[string]any{}))
	if got := ix.lookup("x1"); !slices.Equal(got, []string{"x1"}) {
		t.Fatalf("lookup(\"x1\") = %v, want [x1]", got)
	}
}

func TestIndexSnapshotRoundTripInMemory(t *testing.T) {
	ix := newIndex("by_tag", "tag")
	ix.add("2", idxDoc("2", map[string]any{"tag": "x"}))
	ix.add("1", idxDoc("1", map[string]any{"tag": "x"}))
	ix.add("3", idxDoc("3", map[string]any{"tag": 7}))
	ix.add("4", idxDoc("4", map[string]any{"tag": false}))

	keys := ix.snapshotKeys()
	want := map[string][]string{
		"s:x": {"1", "2"},
		"n:7": {"3"},
		"b:f": {"4"},
	}
	if !maps.EqualFunc(keys, want, slices.Equal) {
		t.Fatalf("snapshotKeys() = %v, want %v", keys, want)
	}

	// The snapshot shares no memory with the index.
	keys["s:x"][0] = "mutated"
	if got := ix.lookup("x"); !slices.Equal(got, []string{"1", "2"}) {
		t.Fatalf("lookup(\"x\") = %v after mutating the snapshot", got)
	}

	restored := indexFromSnapshot(&segment.IndexSnapshot{
		Name: "by_tag", Field: "tag",
		Keys: map[string][]string{"s:x": {"1", "2"}, "n:7": {"3"}, "b:f": {"4"}, "z:": {}},
	})
	if restored.name != "by_tag" || restored.field != "tag" {
		t.Fatalf("restored name/field = %q/%q", restored.name, restored.field)
	}
	if got := restored.snapshotKeys(); !maps.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("restored snapshotKeys() = %v, want %v", got, want)
	}

	// The restored index keeps working.
	restored.remove("1", idxDoc("1", map[string]any{"tag": "x"}))
	restored.add("5", idxDoc("5", map[string]any{"tag": 7.0}))
	if got := restored.lookup("x"); !slices.Equal(got, []string{"2"}) {
		t.Fatalf("restored lookup(\"x\") = %v, want [2]", got)
	}
	if got := restored.lookup(7); !slices.Equal(got, []string{"3", "5"}) {
		t.Fatalf("restored lookup(7) = %v, want [3 5]", got)
	}
}

func TestIndexSnapshotRoundTripOnDisk(t *testing.T) {
	dir := t.TempDir()
	ix := newIndex("by_city", "city")
	for i, city := range []any{"paris", "rome", "paris", 1, true, nil} {
		id := string(rune('a' + i))
		ix.add(id, idxDoc(id, map[string]any{"city": city}))
	}
	if err := segment.WriteIndexSnapshot(dir, &segment.IndexSnapshot{
		Name: ix.name, Field: ix.field, CoversSeg: 1, CoversPos: 2, CoversWAL: 3,
		Keys: ix.snapshotKeys(),
	}); err != nil {
		t.Fatalf("WriteIndexSnapshot: %v", err)
	}
	snap, err := segment.ReadIndexSnapshot(dir, ix.name)
	if err != nil {
		t.Fatalf("ReadIndexSnapshot: %v", err)
	}
	restored := indexFromSnapshot(snap)
	if !maps.EqualFunc(restored.snapshotKeys(), ix.snapshotKeys(), slices.Equal) {
		t.Fatalf("restored keys = %v, want %v", restored.snapshotKeys(), ix.snapshotKeys())
	}
	if got := restored.lookup("paris"); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("lookup(\"paris\") = %v, want [a c]", got)
	}
}
