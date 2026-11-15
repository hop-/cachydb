package db

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
)

// Nested field paths (AC-9, approach-index-and-query §6): "meta.author"
// addresses a field of an object in filters, sort keys, projection,
// distinct, conditions, and index definitions. A top-level key equal to the
// whole name wins; a path that crosses an array or a non-object is missing.

// pathDocs are the documents of the path tests. c has a literal top-level
// "meta.author" key that hides its nested meta.author; d reaches meta
// through an array, e through a string; f has no meta; h holds null.
var pathDocs = map[string]map[string]any{
	"a": {"meta": map[string]any{"author": "zed", "n": 3.0, "tags": []any{"x", "y"}}},
	"b": {"meta": map[string]any{"author": "amy", "n": 1.0, "deep": map[string]any{"c": true}}},
	"c": {"meta.author": "literal", "meta": map[string]any{"author": "hidden", "n": 2.0}},
	"d": {"meta": []any{map[string]any{"author": "arr"}}},
	"e": {"meta": "str"},
	"f": {"x": 1.0},
	"g": {"meta": map[string]any{"author": "bob", "n": 2.0}},
	"h": {"meta": map[string]any{"author": nil}},
}

func TestNestedPaths(t *testing.T) {
	ctx := context.Background()
	_, _, c := newCrashColl(t, nil, nil)
	for id, data := range pathDocs {
		mustInsert(t, c, id, data)
	}
	f := func(field, op string, v any) QueryFilter { return QueryFilter{Field: field, Operator: op, Value: v} }
	filters := []struct {
		name string
		q    Query
		want []string
	}{
		{"eq", Query{Filters: []QueryFilter{f("meta.author", "eq", "amy")}}, []string{"b"}},
		{"literal key wins", Query{Filters: []QueryFilter{f("meta.author", "eq", "literal")}}, []string{"c"}},
		{"hidden nested value", Query{Filters: []QueryFilter{f("meta.author", "eq", "hidden")}}, []string{}},
		{"through an array is missing", Query{Filters: []QueryFilter{f("meta.author", "eq", "arr")}}, []string{}},
		{"eq null", Query{Filters: []QueryFilter{f("meta.author", "eq", nil)}}, []string{"h"}},
		{"ne skips missing", Query{Filters: []QueryFilter{f("meta.author", "ne", "amy")}}, []string{"a", "c", "g", "h"}},
		{"exists true", Query{Filters: []QueryFilter{f("meta.author", "exists", true)}}, []string{"a", "b", "c", "g", "h"}},
		{"exists false", Query{Filters: []QueryFilter{f("meta.author", "exists", false)}}, []string{"d", "e", "f"}},
		{"in", Query{Filters: []QueryFilter{f("meta.author", "in", []any{"amy", "bob", "arr"})}}, []string{"b", "g"}},
		{"gt", Query{Filters: []QueryFilter{f("meta.author", "gt", "b")}}, []string{"a", "c", "g"}},
		{"starts_with", Query{Filters: []QueryFilter{f("meta.author", "starts_with", "b")}}, []string{"g"}},
		{"contains", Query{Filters: []QueryFilter{f("meta.author", "contains", "AM")}}, []string{"b"}},
		{"number range", Query{Filters: []QueryFilter{f("meta.n", "gte", 2)}}, []string{"a", "c", "g"}},
		{"has on a nested array", Query{Filters: []QueryFilter{f("meta.tags", "has", "x")}}, []string{"a"}},
		{"three levels", Query{Filters: []QueryFilter{f("meta.deep.c", "eq", true)}}, []string{"b"}},
		{"path through a string", Query{Filters: []QueryFilter{f("meta.author.x", "exists", true)}}, []string{}},
		{"or groups", Query{Any: [][]QueryFilter{{f("meta.author", "eq", "amy")}, {f("meta.n", "eq", 3)}}}, []string{"a", "b"}},
	}
	sorts := []struct {
		name string
		sort []SortKey
		want []string
	}{
		{"asc", []SortKey{{Field: "meta.author"}}, []string{"d", "e", "f", "h", "b", "g", "c", "a"}},
		{"desc", []SortKey{{Field: "meta.author", Order: "desc"}}, []string{"a", "c", "g", "b", "h", "d", "e", "f"}},
		{"two keys", []SortKey{{Field: "meta.n", Order: "desc"}, {Field: "meta.author"}}, []string{"a", "g", "c", "b", "d", "e", "f", "h"}},
	}

	check := func(t *testing.T) {
		for _, tt := range filters {
			docs, err := c.Find(ctx, &tt.q)
			if err != nil {
				t.Fatalf("%s: %v", tt.name, err)
			}
			if got := docIDs(docs); !slices.Equal(got, tt.want) {
				t.Errorf("filter %s: Find = %v, want %v", tt.name, got, tt.want)
			}
			n, err := c.CountDocuments(ctx, &tt.q)
			if err != nil || n != len(tt.want) {
				t.Errorf("filter %s: CountDocuments = %d, %v; want %d", tt.name, n, err, len(tt.want))
			}
		}
		for _, tt := range sorts {
			docs, err := c.Find(ctx, &Query{Sort: tt.sort})
			if err != nil {
				t.Fatal(err)
			}
			if got := docIDs(docs); !slices.Equal(got, tt.want) {
				t.Errorf("sort %s: Find = %v, want %v", tt.name, got, tt.want)
			}
			page, err := c.Find(ctx, &Query{Sort: tt.sort, Skip: 2, Limit: 3})
			if err != nil {
				t.Fatal(err)
			}
			if got := docIDs(page); !slices.Equal(got, tt.want[2:5]) {
				t.Errorf("sort %s, skip 2 limit 3: Find = %v, want %v", tt.name, got, tt.want[2:5])
			}
		}

		// Projection nests a path's value under the path and merges paths
		// into one object; a literal key is returned as it is.
		docs, err := c.Find(ctx, &Query{Fields: []string{"meta.author", "meta.n", "meta.deep.c"}})
		if err != nil {
			t.Fatal(err)
		}
		wantProj := map[string]map[string]any{
			"a": {"meta": map[string]any{"author": "zed", "n": 3.0}},
			"b": {"meta": map[string]any{"author": "amy", "n": 1.0, "deep": map[string]any{"c": true}}},
			"c": {"meta.author": "literal", "meta": map[string]any{"n": 2.0}},
			"d": {}, "e": {}, "f": {},
			"g": {"meta": map[string]any{"author": "bob", "n": 2.0}},
			"h": {"meta": map[string]any{"author": nil}},
		}
		for _, d := range docs {
			if !reflect.DeepEqual(d.Data, wantProj[d.ID]) {
				t.Errorf("projection of %s = %#v, want %#v", d.ID, d.Data, wantProj[d.ID])
			}
		}
		if len(docs) != len(pathDocs) {
			t.Errorf("projection returned %d documents", len(docs))
		}
		// A projected nested value is a copy.
		if len(docs) > 1 {
			docs[1].Data["meta"].(map[string]any)["deep"].(map[string]any)["c"] = false
			if got := mustGet(t, c, "b"); got.Data["meta"].(map[string]any)["deep"].(map[string]any)["c"] != true {
				t.Error("the projected value shares memory with the stored document")
			}
		}

		vals, err := c.Distinct(ctx, "meta.author", nil)
		if want := []any{nil, "amy", "bob", "literal", "zed"}; err != nil || !reflect.DeepEqual(vals, want) {
			t.Errorf("Distinct(meta.author) = %#v, %v; want %#v", vals, err, want)
		}
		vals, err = c.Distinct(ctx, "meta.tags", &Query{Filters: []QueryFilter{f("meta.n", "gt", 2)}})
		if want := []any{"x", "y"}; err != nil || !reflect.DeepEqual(vals, want) {
			t.Errorf("Distinct(meta.tags | meta.n > 2) = %#v, %v; want %#v", vals, err, want)
		}

		// Conditions on paths.
		if _, _, err := c.UpdateWith(ctx, "a", UpdateSpec{Set: map[string]any{"k": 1.0}, If: Condition{f("meta.author", "eq", "nope")}}); !errors.Is(err, ErrConditionFailed) {
			t.Errorf("UpdateWith with a failing path condition = %v, want ErrConditionFailed", err)
		}
		if _, _, err := c.UpdateWith(ctx, "a", UpdateSpec{Set: map[string]any{"k": 1.0}, If: Condition{f("meta.author", "eq", "zed")}}); err != nil {
			t.Errorf("UpdateWith with a matching path condition: %v", err)
		}
		if _, err := c.Replace(ctx, "c", &Document{Data: pathDocs["c"]}, Condition{f("meta.author", "eq", "hidden")}); !errors.Is(err, ErrConditionFailed) {
			t.Errorf("Replace with a condition on a hidden path = %v, want ErrConditionFailed (the literal key wins)", err)
		}
		if _, err := c.Replace(ctx, "c", &Document{Data: pathDocs["c"]}, Condition{f("meta.author", "eq", "literal")}); err != nil {
			t.Errorf("Replace with a condition on the literal key: %v", err)
		}
		if err := c.DeleteWith(ctx, "d", Condition{f("meta.author", "exists", true)}); !errors.Is(err, ErrConditionFailed) {
			t.Errorf("DeleteWith on a path through an array = %v, want ErrConditionFailed", err)
		}
		if _, _, err := c.UpdateWith(ctx, "a", UpdateSpec{Unset: []string{"k"}}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("no index", check)
	for _, ix := range [][2]string{{"by_author", "meta.author"}, {"by_n", "meta.n"}, {"by_tags", "meta.tags"}, {"by_deep", "meta.deep.c"}} {
		if err := c.CreateIndex(ctx, ix[0], ix[1]); err != nil {
			t.Fatal(err)
		}
	}
	if ix := c.Indexes(); len(ix) != 4 {
		t.Fatalf("Indexes = %+v", ix)
	}
	t.Run("unflushed index", check)
	checkIndexes(t, c)
	mustFlush(t, c)
	t.Run("flushed index", check)
	checkIndexes(t, c)

	// The nested-path index drives the query.
	for _, q := range []Query{
		{Filters: []QueryFilter{f("meta.author", "eq", "amy")}},
		{Filters: []QueryFilter{f("meta.author", "starts_with", "b")}},
		{Sort: []SortKey{{Field: "meta.n"}}, Limit: 2},
	} {
		_, plan, err := c.FindWithPlan(ctx, &q)
		if err != nil || (plan.Index != "by_author" && plan.Index != "by_n") {
			t.Errorf("FindWithPlan(%+v) plan = %+v, %v; want a nested-path index", q, plan, err)
		}
	}

	// A unique index on a path: the literal key counts as the value.
	if err := c.CreateIndex(ctx, "u_author", "meta.author", Unique(true)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ctx, &Document{ID: "i", Data: map[string]any{"meta": map[string]any{"author": "amy"}}}); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("Insert of a duplicate nested value = %v, want ErrUniqueViolation", err)
	}
	if _, err := c.Insert(ctx, &Document{ID: "i", Data: map[string]any{"meta.author": "amy"}}); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("Insert of a duplicate through a literal key = %v, want ErrUniqueViolation", err)
	}
	mustInsert(t, c, "i", map[string]any{"meta": map[string]any{"author": "hidden"}}) // c's hidden value is not its value
	checkIndexes(t, c)
}

// pathResolveCases are documents and paths for the agreement test below.
var pathResolveCases = []struct {
	doc  string
	path string
}{
	{`{"a":1}`, "a"},
	{`{"a":1}`, "_id"},
	{`{"a":{"b":1}}`, "a.b"},
	{`{"a":{"b":{"c":null}}}`, "a.b.c"},
	{`{"a":{"b":{"c":null}}}`, "a.b.c.d"},
	{`{"a.b":1,"a":{"b":2}}`, "a.b"},
	{`{"a.b":{"c":1},"a":{"b":{"c":2}}}`, "a.b.c"},
	{`{"a":[{"b":1}]}`, "a.b"},
	{`{"a":"s"}`, "a.b"},
	{`{"a":{"":1}}`, "a."},
	{`{"":{"a":1}}`, ".a"},
	{`{"a":{"b":1}}`, "a..b"},
	{`{"a":{"b":1}}`, ""},
	{`{"":1}`, ""},
	{`{"a":{"b":1}}`, "x.b"},
	{`{"a":null}`, "a.b"},
}

// TestPathResolutionAgrees: Document.GetPath (used by index entries,
// unique values, and distinct) and the compiled fieldPath (used by
// filters, sort, and projection) resolve every path the same way.
func TestPathResolutionAgrees(t *testing.T) {
	for _, tc := range pathResolveCases {
		checkPathAgrees(t, tc.doc, tc.path)
	}
	// The rules themselves, on one document.
	doc := &Document{ID: "x", Data: map[string]any{"a.b": 1.0, "a": map[string]any{"b": 2.0, "c": []any{map[string]any{"d": 3.0}}}}}
	for _, tt := range []struct {
		path string
		want any
		ok   bool
	}{
		{"a.b", 1.0, true},
		{"a.c.d", nil, false},
		{"_id", "x", true},
		{"a.x", nil, false},
	} {
		if v, ok := doc.GetPath(tt.path); ok != tt.ok || !reflect.DeepEqual(v, tt.want) {
			t.Errorf("GetPath(%q) = %#v, %v; want %#v, %v", tt.path, v, ok, tt.want, tt.ok)
		}
	}
}

func checkPathAgrees(t *testing.T, docJSON, path string) {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal([]byte(docJSON), &data); err != nil {
		return
	}
	delete(data, "_id")
	doc := &Document{ID: "id", Data: data}
	v1, ok1 := doc.GetPath(path)
	p := compilePath(path)
	v2, ok2 := p.get(doc)
	if ok1 != ok2 || !reflect.DeepEqual(v1, v2) {
		t.Fatalf("path %q in %s: GetPath = %#v, %v; compiled path = %#v, %v", path, docJSON, v1, ok1, v2, ok2)
	}
	// An index on the path and a filter on it agree as well: the document
	// matches "exists true" exactly when the path resolves.
	cf, err := compileFilter(QueryFilter{Field: path, Operator: "exists", Value: true})
	if err != nil {
		if path == "" {
			return // an empty field name is not a valid filter
		}
		t.Fatalf("compileFilter(%q): %v", path, err)
	}
	if got := cf.match(doc); got != ok1 {
		t.Fatalf("path %q in %s: exists true matches %v, GetPath found %v", path, docJSON, got, ok1)
	}
}
