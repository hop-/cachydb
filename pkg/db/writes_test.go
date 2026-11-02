package db

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Tests of the plan-004 write features: $now values, SchemaError,
// conditional writes, unset, replace, and upsert.

func TestResolveNow(t *testing.T) {
	now := time.Date(2026, 11, 2, 15, 4, 5, 123456789, time.FixedZone("x", 3600))
	ts := "2026-11-02T14:04:05.123456789Z"
	nested := map[string]any{"a": map[string]any{"$now": true}}
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"marker", map[string]any{"$now": true}, ts},
		{"nested object", nested, map[string]any{"a": ts}},
		{"array", []any{1.0, map[string]any{"$now": true}}, []any{1.0, ts}},
		{"deep", map[string]any{"x": []any{map[string]any{"y": map[string]any{"$now": true}}}}, map[string]any{"x": []any{map[string]any{"y": ts}}}},
		{"false is data", map[string]any{"$now": false}, map[string]any{"$now": false}},
		{"extra keys are data", map[string]any{"$now": true, "b": 1.0}, map[string]any{"$now": true, "b": 1.0}},
		{"string is data", map[string]any{"$now": "true"}, map[string]any{"$now": "true"}},
		{"scalar", "x", "x"},
		{"nil", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolveNow(tt.in, now); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ResolveNow(%v) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
	ResolveNow(nested, now)
	if _, ok := nested["a"].(map[string]any); !ok {
		t.Errorf("ResolveNow modified its input: %v", nested)
	}
}

func TestNowInWrites(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Fields: map[string]Field{"at": {Type: TypeDate, Required: true}}}
	_, c := newTestColl(t, nil, s)
	now := map[string]any{"$now": true}

	before := time.Now().UTC()
	mustInsert(t, c, "a", map[string]any{"at": now, "nested": map[string]any{"x": now, "list": []any{now}}})
	doc := mustGet(t, c, "a")
	at, ok := doc.Data["at"].(string)
	if !ok {
		t.Fatalf("at = %#v, want a string", doc.Data["at"])
	}
	parsed, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		t.Fatalf("at %q is not RFC 3339: %v", at, err)
	}
	if parsed.Before(before.Add(-time.Second)) || parsed.After(time.Now().Add(time.Second)) || !strings.HasSuffix(at, "Z") {
		t.Errorf("at = %q, not the current UTC time", at)
	}
	nested := doc.Data["nested"].(map[string]any)
	if nested["x"] != at || nested["list"].([]any)[0] != at {
		t.Errorf("one request got several timestamps: %v", doc.Data)
	}

	if _, err := c.Update(ctx, "a", map[string]any{"seen": now}); err != nil {
		t.Fatal(err)
	}
	doc = mustGet(t, c, "a")
	if seen, ok := doc.Data["seen"].(string); !ok || seen < at {
		t.Errorf("seen = %#v, want a timestamp not before %q", doc.Data["seen"], at)
	}

	// $now as a filter value: every stored timestamp is in the past.
	got, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "at", Operator: "lt", Value: now}}})
	if err != nil || len(got) != 1 {
		t.Fatalf("Find(at < $now) = %v, %v; want 1 document", docIDs(got), err)
	}
	got, err = c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "at", Operator: "gt", Value: now}}})
	if err != nil || len(got) != 0 {
		t.Fatalf("Find(at > $now) = %v, %v; want none", docIDs(got), err)
	}
}

func TestSchemaErrorListsEveryViolation(t *testing.T) {
	s := &Schema{Fields: map[string]Field{
		"b": {Type: TypeString, Required: true},
		"a": {Type: TypeString, Required: true},
		"d": {Type: TypeBoolean},
		"c": {Type: TypeString},
	}}
	err := s.ValidateDocument(&Document{ID: "x", Data: map[string]any{"c": 1.0, "d": "yes"}})
	if !errors.Is(err, ErrSchemaValidation) {
		t.Fatalf("errors.Is(%v, ErrSchemaValidation) = false", err)
	}
	var se *SchemaError
	if !errors.As(err, &se) {
		t.Fatalf("errors.As(%v, *SchemaError) = false", err)
	}
	wantV := []string{
		"required field missing: a",
		"required field missing: b",
		"wrong type: c (expected string, got number)",
		"wrong type: d (expected boolean, got string)",
	}
	if !reflect.DeepEqual(se.Violations, wantV) {
		t.Errorf("Violations = %q, want %q", se.Violations, wantV)
	}
	want := "schema validation failed: required field(s) missing: a, b; wrong type: c (expected string, got number), d (expected boolean, got string)"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
	if err := s.ValidateDocument(&Document{ID: "x", Data: map[string]any{"a": "1", "b": "2"}}); err != nil {
		t.Errorf("valid document: %v", err)
	}

	// Through the write path: wrapped with the document, still matchable.
	_, c := newTestColl(t, nil, s)
	_, err = c.Insert(context.Background(), &Document{ID: "x", Data: map[string]any{"c": 1.0}})
	if !errors.As(err, &se) || !errors.Is(err, ErrSchemaValidation) {
		t.Fatalf("Insert error %v is not a SchemaError", err)
	}
	if len(se.Violations) != 3 || !strings.Contains(err.Error(), `document "x"`) {
		t.Errorf("Insert error = %v (%q)", err, se.Violations)
	}
}
