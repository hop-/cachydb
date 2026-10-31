package db

import (
	"reflect"
	"testing"
)

func TestDocumentCloneIsDeep(t *testing.T) {
	newDoc := func() *Document {
		return &Document{ID: "d1", Data: map[string]any{
			"name":   "bob",
			"age":    30.0,
			"nested": map[string]any{"inner": map[string]any{"k": "v"}, "list": []any{1.0, 2.0}},
			"tags":   []any{"a", map[string]any{"x": 1.0}},
			"strs":   []string{"p", "q"},
			"ints":   map[string]int{"n": 1},
			"arr":    [2]map[string]any{{"a": 1.0}, {"b": 2.0}},
			"nilmap": map[string]any(nil),
			"null":   nil,
		}}
	}

	tests := []struct {
		name   string
		mutate func(c *Document)
	}{
		{"top-level key", func(c *Document) { c.Data["name"] = "alice" }},
		{"add top-level key", func(c *Document) { c.Data["new"] = 1 }},
		{"id", func(c *Document) { c.ID = "other" }},
		{"nested map value", func(c *Document) {
			c.Data["nested"].(map[string]any)["inner"].(map[string]any)["k"] = "changed"
		}},
		{"nested map add key", func(c *Document) { c.Data["nested"].(map[string]any)["added"] = true }},
		{"slice in nested map", func(c *Document) { c.Data["nested"].(map[string]any)["list"].([]any)[0] = 99.0 }},
		{"slice element", func(c *Document) { c.Data["tags"].([]any)[0] = "z" }},
		{"map inside slice", func(c *Document) { c.Data["tags"].([]any)[1].(map[string]any)["x"] = 2.0 }},
		{"typed slice", func(c *Document) { c.Data["strs"].([]string)[0] = "changed" }},
		{"typed map", func(c *Document) { c.Data["ints"].(map[string]int)["n"] = 2 }},
		{"map inside array", func(c *Document) { c.Data["arr"].([2]map[string]any)[0]["a"] = 9.0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := newDoc()
			c := orig.Clone()
			if !reflect.DeepEqual(c, orig) {
				t.Fatalf("clone differs from original:\n got %#v\nwant %#v", c, orig)
			}
			tt.mutate(c)
			if !reflect.DeepEqual(orig, newDoc()) {
				t.Fatalf("mutating the clone changed the original: %#v", orig.Data)
			}
		})
	}

	t.Run("nil values preserved", func(t *testing.T) {
		c := newDoc().Clone()
		if m, ok := c.Data["nilmap"].(map[string]any); !ok || m != nil {
			t.Fatalf("nilmap = %#v, want typed nil map", c.Data["nilmap"])
		}
		if v, ok := c.Data["null"]; !ok || v != nil {
			t.Fatalf("null = %#v (present %v), want nil", v, ok)
		}
	})

	t.Run("nil document", func(t *testing.T) {
		var d *Document
		if d.Clone() != nil {
			t.Fatal("Clone of nil document is not nil")
		}
	})

	t.Run("nil data", func(t *testing.T) {
		c := (&Document{ID: "x"}).Clone()
		if c.ID != "x" || c.Data == nil || len(c.Data) != 0 {
			t.Fatalf("Clone of nil-data document = %#v", c)
		}
	})
}
