package db

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/wal"
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

// eqf returns an eq filter.
func eqf(field string, v any) QueryFilter { return QueryFilter{Field: field, Operator: "eq", Value: v} }

// expectOneRecord runs fn and checks that it appended exactly one WAL
// record with op (or none when op is 0).
func expectOneRecord(t *testing.T, e *Engine, op wal.Op, fn func() error) error {
	t.Helper()
	next := e.wal.NextOffset()
	err := fn()
	switch got := e.wal.NextOffset() - next; {
	case op == 0 && got != 0:
		t.Errorf("wrote %d WAL records, want none", got)
	case op != 0 && got != 1:
		t.Errorf("wrote %d WAL records, want 1", got)
	case op != 0:
		if r := walRecord(t, e, next); r.Op != op {
			t.Errorf("WAL record op = %s, want %s", r.Op, op)
		}
	}
	return err
}

func TestConditionalWrites(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "d", map[string]any{"version": 1, "status": "open"})

	tests := []struct {
		name    string
		op      wal.Op // 0: nothing written
		run     func() error
		wantErr error
	}{
		{"update if mismatch", 0, func() error {
			_, _, err := c.UpdateWith(ctx, "d", UpdateSpec{Set: map[string]any{"version": 2}, If: Condition{eqf("version", 0)}})
			return err
		}, ErrConditionFailed},
		{"update if match", wal.OpPutDoc, func() error {
			_, _, err := c.UpdateWith(ctx, "d", UpdateSpec{Set: map[string]any{"version": 2}, If: Condition{eqf("version", 1), eqf("status", "open")}})
			return err
		}, nil},
		{"update if partly matching", 0, func() error {
			_, _, err := c.UpdateWith(ctx, "d", UpdateSpec{Set: map[string]any{"version": 3}, If: Condition{eqf("version", 2), eqf("status", "closed")}})
			return err
		}, ErrConditionFailed},
		{"update if on a missing field", 0, func() error {
			_, _, err := c.UpdateWith(ctx, "d", UpdateSpec{Set: map[string]any{"x": 1}, If: Condition{{Field: "owner", Operator: "ne", Value: "me"}}})
			return err
		}, ErrConditionFailed},
		{"update missing document with if", 0, func() error {
			_, _, err := c.UpdateWith(ctx, "nope", UpdateSpec{Set: map[string]any{"x": 1}, If: Condition{eqf("version", 1)}})
			return err
		}, ErrConditionFailed},
		{"update missing document", 0, func() error {
			_, _, err := c.UpdateWith(ctx, "nope", UpdateSpec{Set: map[string]any{"x": 1}})
			return err
		}, ErrNotFound},
		{"replace if mismatch", 0, func() error {
			_, err := c.Replace(ctx, "d", &Document{Data: map[string]any{"a": 1}}, Condition{eqf("version", 1)})
			return err
		}, ErrConditionFailed},
		{"replace missing document with if", 0, func() error {
			_, err := c.Replace(ctx, "nope", &Document{Data: map[string]any{"a": 1}}, Condition{eqf("version", 1)})
			return err
		}, ErrConditionFailed},
		{"delete if mismatch", 0, func() error {
			return c.DeleteWith(ctx, "d", Condition{eqf("status", "closed")})
		}, ErrConditionFailed},
		{"delete missing document with if", 0, func() error {
			return c.DeleteWith(ctx, "nope", Condition{eqf("status", "open")})
		}, ErrConditionFailed},
		{"delete missing document", 0, func() error {
			return c.DeleteWith(ctx, "nope", nil)
		}, ErrNotFound},
		{"replace if match", wal.OpPutDoc, func() error {
			_, err := c.Replace(ctx, "d", &Document{Data: map[string]any{"version": 3, "status": "open"}}, Condition{eqf("version", 2)})
			return err
		}, nil},
		{"delete if match", wal.OpDeleteDoc, func() error {
			return c.DeleteWith(ctx, "d", Condition{eqf("version", 3)})
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := expectOneRecord(t, e, tt.op, tt.run)
			if tt.wantErr == nil && err != nil {
				t.Fatalf("got %v, want success", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
	if _, err := c.Get(ctx, "d"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after conditional delete: %v", err)
	}

	// The MCP-facing message names the document.
	mustInsert(t, c, "m", map[string]any{"v": 1})
	err := c.DeleteWith(ctx, "m", Condition{eqf("v", 2)})
	if want := "condition failed: document m no longer matches if"; err == nil || err.Error() != want {
		t.Errorf("error = %v, want %q", err, want)
	}

	// $now in a condition: an expiry in the past matches lt $now.
	mustInsert(t, c, "exp", map[string]any{"expires": "2000-01-01T00:00:00Z"})
	if err := c.DeleteWith(ctx, "exp", Condition{{Field: "expires", Operator: "lt", Value: map[string]any{"$now": true}}}); err != nil {
		t.Errorf("DeleteWith(expires < $now): %v", err)
	}
}

func TestUnset(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Fields: map[string]Field{"name": {Type: TypeString, Required: true}}}
	e, c := newTestColl(t, nil, s)
	if err := c.CreateIndex(ctx, "by_tag", "tag"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "a", map[string]any{"name": "x", "tag": "t", "draft": true, "n": 1})

	doc, upserted, err := c.UpdateWith(ctx, "a", UpdateSpec{Set: map[string]any{"n": 2}, Unset: []string{"draft", "tag", "absent"}})
	if err != nil || upserted {
		t.Fatalf("UpdateWith: %v, upserted=%v", err, upserted)
	}
	want := map[string]any{"name": "x", "n": 2.0}
	if !reflect.DeepEqual(doc.Data, want) || !reflect.DeepEqual(mustGet(t, c, "a").Data, want) {
		t.Errorf("after unset: %v, want %v", doc.Data, want)
	}
	if got, err := c.Find(ctx, &Query{Filters: []QueryFilter{eqf("tag", "t")}}); err != nil || len(got) != 0 {
		t.Errorf("index still finds the unset field: %v, %v", docIDs(got), err)
	}

	for _, tt := range []struct {
		name string
		u    UpdateSpec
	}{
		{"required field", UpdateSpec{Unset: []string{"name"}}},
		{"_id", UpdateSpec{Unset: []string{"_id"}}},
		{"in both", UpdateSpec{Set: map[string]any{"n": 3}, Unset: []string{"n"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := expectOneRecord(t, e, 0, func() error {
				_, _, err := c.UpdateWith(ctx, "a", tt.u)
				return err
			})
			if !errors.Is(err, ErrSchemaValidation) {
				t.Fatalf("got %v, want ErrSchemaValidation", err)
			}
		})
	}
	if !reflect.DeepEqual(mustGet(t, c, "a").Data, want) {
		t.Errorf("a rejected unset changed the document")
	}
}

func TestReplace(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Fields: map[string]Field{"name": {Type: TypeString, Required: true}, "at": {Type: TypeDate}}}
	e, c := newTestColl(t, nil, s)
	if err := c.CreateIndex(ctx, "by_name", "name"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "a", map[string]any{"name": "old", "extra": 1})

	var doc *Document
	err := expectOneRecord(t, e, wal.OpPutDoc, func() (err error) {
		doc, err = c.Replace(ctx, "a", &Document{ID: "a", Data: map[string]any{"name": "new", "at": map[string]any{"$now": true}}}, nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Data["extra"]; ok || doc.Data["name"] != "new" {
		t.Errorf("Replace returned %v", doc.Data)
	}
	if _, ok := doc.Data["at"].(string); !ok {
		t.Errorf("$now not resolved: %v", doc.Data["at"])
	}
	if got := mustGet(t, c, "a"); !reflect.DeepEqual(got.Data, doc.Data) {
		t.Errorf("stored %v, returned %v", got.Data, doc.Data)
	}
	for v, n := range map[string]int{"old": 0, "new": 1} {
		if got, err := c.Find(ctx, &Query{Filters: []QueryFilter{eqf("name", v)}}); err != nil || len(got) != n {
			t.Errorf("Find(name=%s) = %v, %v; want %d", v, docIDs(got), err, n)
		}
	}

	for _, tt := range []struct {
		name    string
		id      string
		doc     *Document
		wantErr error
	}{
		{"missing document", "nope", &Document{Data: map[string]any{"name": "x"}}, ErrNotFound},
		{"different _id", "a", &Document{ID: "b", Data: map[string]any{"name": "x"}}, ErrSchemaValidation},
		{"_id in data", "a", &Document{Data: map[string]any{"_id": "a", "name": "x"}}, ErrSchemaValidation},
		{"schema", "a", &Document{Data: map[string]any{"other": 1}}, ErrSchemaValidation},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := expectOneRecord(t, e, 0, func() error {
				_, err := c.Replace(ctx, tt.id, tt.doc, nil)
				return err
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestUpsert(t *testing.T) {
	ctx := context.Background()
	s := &Schema{Fields: map[string]Field{"n": {Type: TypeNumber, Required: true}}}
	e, c := newTestColl(t, nil, s)

	doc, upserted, err := c.UpdateWith(ctx, "u", UpdateSpec{Set: map[string]any{"n": 1, "at": map[string]any{"$now": true}}, Unset: []string{"gone"}, Upsert: true})
	if err != nil || !upserted {
		t.Fatalf("upsert of a missing document: %v, upserted=%v", err, upserted)
	}
	if doc.ID != "u" || doc.Data["n"] != 1.0 || c.Count() != 1 {
		t.Errorf("upserted %v (count %d)", doc, c.Count())
	}
	if _, ok := doc.Data["at"].(string); !ok {
		t.Errorf("$now not resolved on upsert: %v", doc.Data)
	}

	doc, upserted, err = c.UpdateWith(ctx, "u", UpdateSpec{Set: map[string]any{"n": 2}, Upsert: true})
	if err != nil || upserted || doc.Data["n"] != 2.0 || doc.Data["at"] == nil {
		t.Errorf("upsert of an existing document: %v, %v, upserted=%v", doc, err, upserted)
	}

	for _, tt := range []struct {
		name    string
		id      string
		u       UpdateSpec
		wantErr error
	}{
		{"with if", "v", UpdateSpec{Set: map[string]any{"n": 1}, If: Condition{eqf("n", 1)}, Upsert: true}, ErrConditionFailed},
		{"schema", "v", UpdateSpec{Set: map[string]any{"other": 1}, Upsert: true}, ErrSchemaValidation},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := expectOneRecord(t, e, 0, func() error {
				_, _, err := c.UpdateWith(ctx, tt.id, tt.u)
				return err
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
	if _, _, err := c.UpdateWith(ctx, "", UpdateSpec{Set: map[string]any{"n": 1}, Upsert: true}); err == nil {
		t.Error("upsert with an empty id succeeded")
	}
	if c.Count() != 1 {
		t.Errorf("count = %d, want 1", c.Count())
	}
}

// TestConcurrentCompareAndSet runs agents that increment a version field
// with a conditional update: every success is a distinct increment (no
// lost update), the rest get ErrConditionFailed.
func TestConcurrentCompareAndSet(t *testing.T) {
	ctx := context.Background()
	_, c := newTestColl(t, nil, nil)
	mustInsert(t, c, "doc", map[string]any{"version": 0})

	const agents, attempts = 16, 20
	var (
		wg        sync.WaitGroup
		successes atomic.Int64
		conflicts atomic.Int64
	)
	errs := make(chan error, agents)
	for range agents {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range attempts {
				cur, err := c.Get(ctx, "doc")
				if err != nil {
					errs <- err
					return
				}
				v := cur.Data["version"].(float64)
				_, _, err = c.UpdateWith(ctx, "doc", UpdateSpec{
					Set: map[string]any{"version": v + 1},
					If:  Condition{eqf("version", v)},
				})
				switch {
				case err == nil:
					successes.Add(1)
				case errors.Is(err, ErrConditionFailed):
					conflicts.Add(1)
				default:
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	got := mustGet(t, c, "doc").Data["version"].(float64)
	if got != float64(successes.Load()) {
		t.Errorf("version = %v after %d successful compare-and-sets (lost update)", got, successes.Load())
	}
	if successes.Load()+conflicts.Load() != agents*attempts || successes.Load() == 0 {
		t.Errorf("successes %d + conflicts %d != %d", successes.Load(), conflicts.Load(), agents*attempts)
	}
}
