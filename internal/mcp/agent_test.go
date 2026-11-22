package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Tests of the plan-013 stage-5 agent interface (approach-agent-interface):
// bounded results and paging, get_document and the batch tools, the update
// operators, exists/starts_with, explain, unique indexes, error codes,
// server_stats, and the result text setting.

// newConfigServer opens an engine with dbOpts (Dir and Logger filled in)
// and builds a Server with cfg (DefaultDatabase, Transport, and Version
// filled in when empty). Cleanup closes the server, then the engine.
func newConfigServer(t *testing.T, dbOpts db.Options, cfg Config) (*Server, *db.Engine) {
	t.Helper()
	ctx := context.Background()
	dbOpts.Dir = t.TempDir()
	dbOpts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	engine, err := db.Open(ctx, dbOpts)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	if cfg.DefaultDatabase == "" {
		cfg.DefaultDatabase = testDefaultDB
	}
	if cfg.Transport == "" {
		cfg.Transport = "stdio"
	}
	if cfg.Version == "" {
		cfg.Version = testVersion
	}
	s, err := New(ctx, engine, cfg)
	if err != nil {
		_ = engine.Close(ctx)
		t.Fatalf("New: %v", err)
	}
	s.stderr = io.Discard
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Close(closeCtx); err != nil {
			t.Errorf("Server.Close: %v", err)
		}
		if err := engine.Close(closeCtx); err != nil && !errors.Is(err, db.ErrClosed) {
			t.Errorf("Engine.Close: %v", err)
		}
	})
	return s, engine
}

// engineColl creates collection name in the default database of engine
// and returns it.
func engineColl(t *testing.T, engine *db.Engine, name string, schema *db.Schema) *db.Collection {
	t.Helper()
	d, err := engine.Database(testDefaultDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(context.Background(), name, schema); err != nil {
		t.Fatal(err)
	}
	c, err := d.Collection(name)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// wantCode requires res to be a tool error with code, whose text contains
// every string of want.
func wantCode(t *testing.T, res *mcp.CallToolResult, code string, want ...string) {
	t.Helper()
	text := resultText(res)
	if !res.IsError {
		t.Fatalf("call succeeded (%s), want a %s error", text, code)
	}
	if got, _ := ErrorCodeOf(text); got != code {
		t.Fatalf("error %q has code %q, want %q", text, got, code)
	}
	for _, w := range want {
		if !strings.Contains(text, w) {
			t.Errorf("error %q does not contain %q", text, w)
		}
	}
}

// findPage is the decoded result of one find_documents call.
type findPage struct {
	Count     int              `json:"count"`
	Documents []map[string]any `json:"documents"`
	HasMore   bool             `json:"has_more"`
	NextSkip  *int             `json:"next_skip"`
	Plan      *QueryPlan       `json:"plan"`
}

func decodeFind(t *testing.T, out map[string]any) findPage {
	t.Helper()
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var p findPage
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	if p.Count != len(p.Documents) {
		t.Fatalf("count %d, %d documents", p.Count, len(p.Documents))
	}
	return p
}

// docsJSONSize is the size of the JSON array of docs, as the byte cap
// measures it.
func docsJSONSize(t *testing.T, docs []map[string]any) int {
	t.Helper()
	data, err := json.Marshal(docs)
	if err != nil {
		t.Fatal(err)
	}
	return len(data)
}

// pageAll pages find_documents with query (skip set per page) until
// has_more is false and returns every _id in order. It checks every
// page's has_more/next_skip contract, the page size against limit (the
// requested one, else the default), and the byte cap (a page over it must
// hold one document).
func pageAll(t *testing.T, cs *mcp.ClientSession, coll string, query map[string]any, maxLimit, byteCap int) []string {
	t.Helper()
	var ids []string
	skip := 0
	for page := 0; ; page++ {
		if page > 10000 {
			t.Fatal("paging does not end")
		}
		q := maps.Clone(query)
		if skip > 0 {
			q["skip"] = skip
		}
		p := decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": coll, "query": q}))
		if len(p.Documents) > maxLimit {
			t.Fatalf("page %d: %d documents, limit %d", page, len(p.Documents), maxLimit)
		}
		if n := docsJSONSize(t, p.Documents); n > byteCap && len(p.Documents) != 1 {
			t.Fatalf("page %d: %d documents of %d bytes, cap %d", page, len(p.Documents), n, byteCap)
		}
		for _, d := range p.Documents {
			ids = append(ids, d["_id"].(string))
		}
		if !p.HasMore {
			if p.NextSkip != nil {
				t.Fatalf("page %d: next_skip %d without has_more", page, *p.NextSkip)
			}
			return ids
		}
		if p.NextSkip == nil || *p.NextSkip != skip+len(p.Documents) || len(p.Documents) == 0 {
			t.Fatalf("page %d at skip %d: %d documents, has_more, next_skip %v", page, skip, len(p.Documents), p.NextSkip)
		}
		skip = *p.NextSkip
	}
}

// seedPaging inserts n documents of varied size into coll: _id dNNNN, rank
// (indexed, descending with the id, with ties), u (not indexed, a
// permutation), and a pad of 0 to ~3 KB.
func seedPaging(t *testing.T, engine *db.Engine, coll string, n int) (*db.Collection, []string) {
	t.Helper()
	ctx := context.Background()
	c := engineColl(t, engine, coll, nil)
	ops := make([]db.WriteOp, n)
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("d%04d", i)
		ops[i] = db.WriteOp{Kind: db.OpInsert, Document: &db.Document{ID: ids[i], Data: map[string]any{
			"rank": float64((n - i) / 3),
			"u":    float64((i * 7919) % n),
			"pad":  strings.Repeat("p", (i*389)%3000),
		}}}
	}
	if _, err := c.Batch(ctx, ops); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateIndex(ctx, "by_rank", "rank"); err != nil {
		t.Fatal(err)
	}
	return c, ids
}

// wantEachOnce requires got to hold exactly the ids of want, each once,
// in want's order.
func wantEachOnce(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		seen := map[string]int{}
		for _, id := range got {
			seen[id]++
		}
		var dup []string
		for id, n := range seen {
			if n > 1 {
				dup = append(dup, id)
			}
		}
		t.Fatalf("%s: %d ids (%d distinct, duplicates %v), want %d in order", what, len(got), len(seen), dup, len(want))
	}
}

// TestFindPaging (AC-21): pages of the default limit and of an explicit
// limit, by _id and by an indexed and an unindexed sort key, until
// has_more is false, return every match exactly once, in order.
func TestFindPaging(t *testing.T) {
	s, engine := newConfigServer(t, db.Options{}, Config{})
	c, _ := seedPaging(t, engine, "p", 437)
	cs := connect(t, s)
	ctx := context.Background()

	for _, tt := range []struct {
		name  string
		query map[string]any
		dbq   *db.Query
	}{
		{"by _id", map[string]any{}, &db.Query{}},
		{"by indexed rank desc", map[string]any{"sort": []any{map[string]any{"field": "rank", "order": "desc"}}}, &db.Query{Sort: []db.SortKey{{Field: "rank", Order: "desc"}}}},
		{"by unindexed u", map[string]any{"sort": []any{map[string]any{"field": "u"}}}, &db.Query{Sort: []db.SortKey{{Field: "u"}}}},
		{"filtered by rank, _id desc", map[string]any{
			"filters": []any{map[string]any{"field": "rank", "operator": "gte", "value": 20}},
			"sort":    []any{map[string]any{"field": "_id", "order": "desc"}}},
			&db.Query{Filters: []db.QueryFilter{{Field: "rank", Operator: "gte", Value: 20}}, Sort: []db.SortKey{{Field: "_id", Order: "desc"}}}},
	} {
		docs, err := c.Find(ctx, tt.dbq)
		if err != nil {
			t.Fatal(err)
		}
		want := make([]string, len(docs))
		for i, d := range docs {
			want[i] = d.ID
		}
		t.Run(tt.name+", default limit", func(t *testing.T) {
			wantEachOnce(t, tt.name, pageAll(t, cs, "p", tt.query, DefaultFindLimit, DefaultMaxResultBytes), want)
		})
		t.Run(tt.name+", limit 29", func(t *testing.T) {
			q := maps.Clone(tt.query)
			q["limit"] = 29
			wantEachOnce(t, tt.name, pageAll(t, cs, "p", q, 29, DefaultMaxResultBytes), want)
		})
	}

	t.Run("default and explicit limits", func(t *testing.T) {
		p := decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p"}))
		if p.Count != DefaultFindLimit || !p.HasMore || p.NextSkip == nil || *p.NextSkip != DefaultFindLimit {
			t.Errorf("no query: count %d, has_more %v, next_skip %v; want 100, true, 100", p.Count, p.HasMore, p.NextSkip)
		}
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "query": map[string]any{"limit": 0}}))
		if p.Count != DefaultFindLimit {
			t.Errorf("limit 0: count %d, want the default 100", p.Count)
		}
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "query": map[string]any{"limit": 7, "fields": []any{"rank"}}}))
		if p.Count != 7 || !p.HasMore || *p.NextSkip != 7 {
			t.Errorf("limit 7: count %d, has_more %v, next_skip %v", p.Count, p.HasMore, p.NextSkip)
		}
		// Above the default, honoured (projection keeps it under the cap).
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "query": map[string]any{"limit": 250, "fields": []any{"rank"}}}))
		if p.Count != 250 || !p.HasMore {
			t.Errorf("limit 250: count %d, has_more %v", p.Count, p.HasMore)
		}
		// A limit equal to the match count: no more.
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "query": map[string]any{"limit": 437, "fields": []any{"rank"}}}))
		if p.Count != 437 || p.HasMore || p.NextSkip != nil {
			t.Errorf("limit = matches: count %d, has_more %v, next_skip %v", p.Count, p.HasMore, p.NextSkip)
		}
		// The last page ends exactly at the end.
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "query": map[string]any{"skip": 430, "limit": 7}}))
		if p.Count != 7 || p.HasMore {
			t.Errorf("last page: count %d, has_more %v", p.Count, p.HasMore)
		}
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "query": map[string]any{"skip": 1000}}))
		if p.Count != 0 || p.HasMore || p.NextSkip != nil {
			t.Errorf("skip past the end: %+v", p)
		}
	})

	t.Run("explain only when asked", func(t *testing.T) {
		out := callOK(t, cs, "find_documents", map[string]any{"collection": "p", "query": map[string]any{"limit": 5}})
		if _, ok := out["plan"]; ok {
			t.Errorf("plan without explain: %v", out["plan"])
		}
		p := decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "explain": true,
			"query": map[string]any{"limit": 5, "sort": []any{map[string]any{"field": "rank"}}}}))
		if p.Plan == nil || p.Plan.Index != "by_rank" || !p.Plan.OrderProvided || p.Plan.Returned != 5 || p.Plan.Examined < 5 {
			t.Errorf("explain with an indexed sort: %+v", p.Plan)
		}
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "explain": true,
			"query": map[string]any{"limit": 5, "sort": []any{map[string]any{"field": "u"}}}}))
		if p.Plan == nil || p.Plan.OrderProvided || p.Plan.Index != "" || p.Plan.Examined != 437 || p.Plan.Returned != 5 {
			t.Errorf("explain with an unindexed sort: %+v", p.Plan)
		}
		p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p", "explain": true,
			"query": map[string]any{"filters": []any{map[string]any{"field": "_id", "operator": "eq", "value": "d0003"}}}}))
		if p.Plan == nil || p.Plan.Driver == "" || p.Plan.Returned != 1 || p.Count != 1 {
			t.Errorf("explain of an id lookup: %+v", p.Plan)
		}
	})
}

// TestFindByteCap (AC-21): the byte cap ends a page before the limit,
// pages still return every match once, and a document larger than the
// cap comes back alone.
func TestFindByteCap(t *testing.T) {
	const byteCap = 8 << 10
	s, engine := newConfigServer(t, db.Options{}, Config{MaxResultBytes: byteCap, DefaultLimit: 1000})
	c, ids := seedPaging(t, engine, "p", 200)
	cs := connect(t, s)
	for _, query := range []map[string]any{
		{},
		{"sort": []any{map[string]any{"field": "rank"}}},
		{"sort": []any{map[string]any{"field": "u", "order": "desc"}}},
	} {
		docs, err := c.Find(context.Background(), toDBQuery(&Query{Sort: sortOf(query)}))
		if err != nil {
			t.Fatal(err)
		}
		want := make([]string, len(docs))
		for i, d := range docs {
			want[i] = d.ID
		}
		got := pageAll(t, cs, "p", query, 1000, byteCap)
		wantEachOnce(t, fmt.Sprintf("%v", query), got, want)
		if len(got) != len(ids) {
			t.Fatalf("paged %d documents, want %d", len(got), len(ids))
		}
	}
	first := decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "p"}))
	if !first.HasMore || first.Count >= 200 || first.Count < 2 {
		t.Errorf("first page under the cap: count %d, has_more %v", first.Count, first.HasMore)
	}

	// One document over the cap, alone and in the middle of others.
	big := engineColl(t, engine, "big", nil)
	ctx := context.Background()
	for _, d := range []*db.Document{
		{ID: "a", Data: map[string]any{"x": 1}},
		{ID: "b", Data: map[string]any{"pad": strings.Repeat("B", 3*byteCap)}},
		{ID: "c", Data: map[string]any{"x": 3}},
	} {
		if _, err := big.Insert(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	p := decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "big"}))
	if p.Count != 1 || p.Documents[0]["_id"] != "a" || !p.HasMore || *p.NextSkip != 1 {
		t.Errorf("page before the big document: %d documents, has_more %v", p.Count, p.HasMore)
	}
	p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "big", "query": map[string]any{"skip": 1}}))
	if p.Count != 1 || p.Documents[0]["_id"] != "b" || !p.HasMore || *p.NextSkip != 2 {
		t.Errorf("the big document: %d documents, has_more %v", p.Count, p.HasMore)
	}
	p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "big", "query": map[string]any{"skip": 1, "limit": 1}}))
	if p.Count != 1 || p.Documents[0]["_id"] != "b" || !p.HasMore {
		t.Errorf("the big document with limit 1: %d documents, has_more %v", p.Count, p.HasMore)
	}
	p = decodeFind(t, callOK(t, cs, "find_documents", map[string]any{"collection": "big",
		"query": map[string]any{"filters": []any{map[string]any{"field": "_id", "operator": "eq", "value": "b"}}}}))
	if p.Count != 1 || p.HasMore {
		t.Errorf("only the big document matches: %d documents, has_more %v", p.Count, p.HasMore)
	}
}

// sortOf converts the "sort" of a find_documents query argument.
func sortOf(query map[string]any) []SortKey {
	var out []SortKey
	keys, _ := query["sort"].([]any)
	for _, k := range keys {
		m := k.(map[string]any)
		order, _ := m["order"].(string)
		out = append(out, SortKey{Field: m["field"].(string), Order: SortOrder(order)})
	}
	return out
}

// TestFindHugeSkip: a huge skip must not crash the server, whatever the
// sort, and must return an empty last page (code review c1). The handler
// is called directly so that a panic fails the test instead of the test
// binary (the SDK does not recover handler panics: in the server it would
// end the process).
func TestFindHugeSkip(t *testing.T) {
	s, engine := newConfigServer(t, db.Options{}, Config{})
	seedPaging(t, engine, "p", 30)
	for _, sort := range [][]SortKey{nil, {{Field: "rank"}}, {{Field: "u"}}, {{Field: "_id", Order: "desc"}}} {
		for _, skip := range []int{math.MaxInt, math.MaxInt - 100} {
			for _, limit := range []int{0, 5, math.MaxInt} {
				name := fmt.Sprintf("sort %v, skip %d, limit %d", sort, skip, limit)
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("%s: find_documents panicked: %v", name, r)
						}
					}()
					_, out, err := s.findDocumentsTool(context.Background(), nil, FindDocumentsInput{
						Database: testDefaultDB, Collection: "p", Query: &Query{Sort: sort, Skip: skip, Limit: limit}})
					if err != nil || out.Count != 0 || out.HasMore || out.NextSkip != 0 {
						t.Errorf("%s: %+v, %v; want an empty last page", name, out, err)
					}
				}()
			}
		}
	}
}

// TestGetDocument (AC-22).
func TestGetDocument(t *testing.T) {
	cs := connect(t, newTestServer(t))
	callOK(t, cs, "create_collection", map[string]any{"name": "g"})
	callOK(t, cs, "insert_document", map[string]any{"collection": "g", "document": map[string]any{"_id": "x", "a": 1, "b": map[string]any{"c": "d", "e": 2}, "t": []any{"q"}}})
	out := callOK(t, cs, "get_document", map[string]any{"collection": "g", "id": "x"})
	want := map[string]any{"_id": "x", "a": 1.0, "b": map[string]any{"c": "d", "e": 2.0}, "t": []any{"q"}}
	if !reflect.DeepEqual(out["document"], want) {
		t.Errorf("get_document = %v, want %v", out["document"], want)
	}
	out = callOK(t, cs, "get_document", map[string]any{"collection": "g", "id": "x", "fields": []any{"a", "b.c"}})
	if want := map[string]any{"_id": "x", "a": 1.0, "b": map[string]any{"c": "d"}}; !reflect.DeepEqual(out["document"], want) {
		t.Errorf("get_document with fields = %v, want %v", out["document"], want)
	}
	wantCode(t, call(t, cs, "get_document", map[string]any{"collection": "g", "id": "nope"}), CodeNotFound, `"nope"`, "get document")
	wantCode(t, call(t, cs, "get_document", map[string]any{"collection": "g", "id": "nope", "fields": []any{"a"}}), CodeNotFound, `"nope"`)
	wantCode(t, call(t, cs, "get_document", map[string]any{"collection": "missing", "id": "x"}), CodeNotFound)
}

// TestInsertDocuments (AC-22): all or none, the failing item named, ids in
// order, string _id, and one $now for the whole call.
func TestInsertDocuments(t *testing.T) {
	cs := connect(t, newTestServer(t))
	callOK(t, cs, "create_collection", map[string]any{"name": "i", "schema": map[string]any{"fields": map[string]any{"n": map[string]any{"type": "number", "required": true}}}})
	ids := func(out map[string]any) []string {
		var got []string
		for _, v := range out["ids"].([]any) {
			got = append(got, v.(string))
		}
		return got
	}
	count := func() float64 {
		return callOK(t, cs, "count_documents", map[string]any{"collection": "i"})["count"].(float64)
	}
	out := callOK(t, cs, "insert_documents", map[string]any{"collection": "i", "documents": []any{
		map[string]any{"_id": "a", "n": 1, "at": map[string]any{"$now": true}},
		map[string]any{"n": 2, "at": map[string]any{"$now": true}},
		map[string]any{"_id": "c", "n": 3},
	}})
	got := ids(out)
	if out["inserted"] != 3.0 || len(got) != 3 || got[0] != "a" || got[2] != "c" || got[1] == "" {
		t.Fatalf("insert_documents = %v", out)
	}
	a := callOK(t, cs, "get_document", map[string]any{"collection": "i", "id": "a"})["document"].(map[string]any)
	b := callOK(t, cs, "get_document", map[string]any{"collection": "i", "id": got[1]})["document"].(map[string]any)
	if at, ok := a["at"].(string); !ok || at != b["at"] {
		t.Errorf("$now: %v and %v, want one time", a["at"], b["at"])
	} else if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
		t.Errorf("$now = %q: %v", at, err)
	}

	for _, tt := range []struct {
		name string
		docs []any
		code string
		item string
	}{
		{"duplicate of a stored id", []any{map[string]any{"_id": "new1", "n": 1}, map[string]any{"_id": "a", "n": 1}}, CodeAlreadyExists, "documents[1]"},
		{"id twice in the call", []any{map[string]any{"_id": "z", "n": 1}, map[string]any{"_id": "y", "n": 1}, map[string]any{"_id": "z", "n": 1}}, CodeAlreadyExists, "documents[2]"},
		{"schema failure", []any{map[string]any{"_id": "s1", "n": 1}, map[string]any{"_id": "s2", "n": "two"}}, CodeSchemaViolation, "documents[1]"},
		{"missing required field", []any{map[string]any{"_id": "s3"}}, CodeSchemaViolation, "documents[0]"},
		{"non-string _id", []any{map[string]any{"_id": 5, "n": 1}}, CodeSchemaViolation, "documents[0]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := count()
			wantCode(t, call(t, cs, "insert_documents", map[string]any{"collection": "i", "documents": tt.docs}), tt.code, tt.item, "insert documents")
			if after := count(); after != before {
				t.Errorf("a failed insert_documents wrote: %v -> %v documents", before, after)
			}
		})
	}
	out = callOK(t, cs, "insert_documents", map[string]any{"collection": "i", "documents": []any{}})
	if out["inserted"] != 0.0 || len(out["ids"].([]any)) != 0 {
		t.Errorf("empty documents: %v", out)
	}
}

// TestBatchUpdateDelete (AC-22): update_documents and delete_documents
// match, limit, refuse an empty query without all, and fail with the
// match count and no change above the batch limit.
func TestBatchUpdateDelete(t *testing.T) {
	s, _ := newConfigServer(t, db.Options{MaxBatchDocs: 5}, Config{})
	cs := connect(t, s)
	callOK(t, cs, "create_collection", map[string]any{"name": "b"})
	var docs []any
	for i := range 8 {
		docs = append(docs, map[string]any{"_id": fmt.Sprintf("d%d", i), "g": []string{"x", "y"}[i%2], "n": i})
	}
	// MaxBatchDocs also bounds insert_documents.
	wantCode(t, call(t, cs, "insert_documents", map[string]any{"collection": "b", "documents": docs}), CodeLimitExceeded)
	callOK(t, cs, "insert_documents", map[string]any{"collection": "b", "documents": docs[:4]})
	callOK(t, cs, "insert_documents", map[string]any{"collection": "b", "documents": docs[4:]})
	snapshot := func() []any {
		return callOK(t, cs, "find_documents", map[string]any{"collection": "b"})["documents"].([]any)
	}
	gx := map[string]any{"filters": []any{map[string]any{"field": "g", "operator": "eq", "value": "x"}}}

	out := callOK(t, cs, "update_documents", map[string]any{"collection": "b", "query": gx, "updates": map[string]any{"tag": "t"}, "inc": map[string]any{"n": 100}})
	if out["matched"] != 4.0 || out["modified"] != 4.0 {
		t.Errorf("update_documents = %v", out)
	}
	out = callOK(t, cs, "count_documents", map[string]any{"collection": "b", "query": map[string]any{"filters": []any{map[string]any{"field": "n", "operator": "gte", "value": 100}}}})
	if out["count"] != 4.0 {
		t.Errorf("updated documents = %v", out["count"])
	}
	out = callOK(t, cs, "update_documents", map[string]any{"collection": "b", "query": gx, "unset": []any{"tag"}, "limit": 2})
	if out["matched"] != 2.0 || out["modified"] != 2.0 {
		t.Errorf("update_documents with limit 2 = %v", out)
	}

	for _, tool := range []string{"update_documents", "delete_documents"} {
		t.Run(tool, func(t *testing.T) {
			args := func(extra map[string]any) map[string]any {
				a := map[string]any{"collection": "b"}
				if tool == "update_documents" {
					a["updates"] = map[string]any{"z": 1}
				}
				maps.Copy(a, extra)
				return a
			}
			before := snapshot()
			for _, q := range []map[string]any{nil, {"query": map[string]any{}}, {"query": map[string]any{"any": []any{[]any{}}}}} {
				wantCode(t, call(t, cs, tool, args(q)), CodeInvalidArgument, "empty", `"all": true`)
			}
			wantCode(t, call(t, cs, tool, args(map[string]any{"query": gx, "limit": -1})), CodeInvalidArgument, "limit")
			// 8 match, more than MaxBatchDocs 5: the count is named and
			// nothing changes.
			wantCode(t, call(t, cs, tool, args(map[string]any{"all": true})), CodeLimitExceeded, "8 documents match", "limit")
			wantCode(t, call(t, cs, tool, args(map[string]any{"all": true, "limit": 6})), CodeLimitExceeded, "8 documents match")
			if after := snapshot(); !reflect.DeepEqual(before, after) {
				t.Fatalf("a refused %s changed the data", tool)
			}
		})
	}
	if out := callOK(t, cs, "update_documents", map[string]any{"collection": "b", "all": true, "limit": 5, "updates": map[string]any{"z": 1}}); out["matched"] != 5.0 || out["modified"] != 5.0 {
		t.Errorf("update_documents all with limit 5 = %v", out)
	}
	wantCode(t, call(t, cs, "update_documents", map[string]any{"collection": "b", "all": true, "limit": 2}), CodeInvalidArgument, "give updates")
	if out := callOK(t, cs, "delete_documents", map[string]any{"collection": "b", "query": gx, "limit": 3}); out["matched"] != 3.0 || out["deleted"] != 3.0 {
		t.Errorf("delete_documents with limit 3 = %v", out)
	}
	if out := callOK(t, cs, "delete_documents", map[string]any{"collection": "b", "all": true}); out["matched"] != 5.0 || out["deleted"] != 5.0 {
		t.Errorf("delete_documents all = %v", out)
	}
	if out := callOK(t, cs, "delete_documents", map[string]any{"collection": "b", "all": true}); out["matched"] != 0.0 {
		t.Errorf("delete_documents on an empty collection = %v", out)
	}
	wantCode(t, call(t, cs, "delete_documents", map[string]any{"collection": "nope", "all": true}), CodeNotFound)
}

// TestUpdateOperatorsMCP (AC-23): the operators through update_document
// and update_documents, with if and upsert, their errors and conflicts.
func TestUpdateOperatorsMCP(t *testing.T) {
	cs := connect(t, newTestServer(t))
	callOK(t, cs, "create_collection", map[string]any{"name": "o", "schema": map[string]any{"fields": map[string]any{"n": map[string]any{"type": "number"}}}})
	callOK(t, cs, "insert_document", map[string]any{"collection": "o", "document": map[string]any{"_id": "d", "n": 1, "tags": []any{"a"}, "s": "str"}})
	get := func(id string) map[string]any {
		return callOK(t, cs, "get_document", map[string]any{"collection": "o", "id": id})["document"].(map[string]any)
	}
	upd := func(extra map[string]any) *mcp.CallToolResult {
		args := map[string]any{"collection": "o", "id": "d"}
		maps.Copy(args, extra)
		return call(t, cs, "update_document", args)
	}
	if res := upd(map[string]any{"inc": map[string]any{"n": 2.5, "c": 1}, "push": map[string]any{"tags": "b"}, "add_to_set": map[string]any{"set": "s"}, "pull": map[string]any{"missing": 1}}); res.IsError {
		t.Fatal(resultText(res))
	}
	if d := get("d"); d["n"] != 3.5 || d["c"] != 1.0 || !reflect.DeepEqual(d["tags"], []any{"a", "b"}) || !reflect.DeepEqual(d["set"], []any{"s"}) {
		t.Errorf("after operators: %v", d)
	}
	if res := upd(map[string]any{"add_to_set": map[string]any{"tags": "a"}, "pull": map[string]any{"set": "s"}}); res.IsError {
		t.Fatal(resultText(res))
	}
	if d := get("d"); !reflect.DeepEqual(d["tags"], []any{"a", "b"}) || !reflect.DeepEqual(d["set"], []any{}) {
		t.Errorf("add_to_set existing / pull: %v", d)
	}
	before := get("d")
	for _, tt := range []struct {
		name string
		args map[string]any
		code string
		want string
	}{
		{"inc on a string", map[string]any{"inc": map[string]any{"s": 1}}, CodeInvalidArgument, "inc"},
		{"push on a number", map[string]any{"push": map[string]any{"n": 1}}, CodeInvalidArgument, "push"},
		{"add_to_set on a string", map[string]any{"add_to_set": map[string]any{"s": 1}}, CodeInvalidArgument, "add_to_set"},
		{"pull on a string", map[string]any{"pull": map[string]any{"s": 1}}, CodeInvalidArgument, "pull"},
		{"updates and inc", map[string]any{"updates": map[string]any{"n": 1}, "inc": map[string]any{"n": 1}}, CodeInvalidArgument, `"n"`},
		{"unset and pull", map[string]any{"unset": []any{"tags"}, "pull": map[string]any{"tags": "a"}}, CodeInvalidArgument, `"tags"`},
		{"push and add_to_set", map[string]any{"push": map[string]any{"tags": 1}, "add_to_set": map[string]any{"tags": 2}}, CodeInvalidArgument, `"tags"`},
		{"updates and unset", map[string]any{"updates": map[string]any{"s": 1}, "unset": []any{"s"}}, CodeSchemaViolation, `"s"`},
		{"inc on _id", map[string]any{"inc": map[string]any{"_id": 1}}, CodeInvalidArgument, "_id"},
		{"failing if", map[string]any{"inc": map[string]any{"n": 1}, "if": []any{map[string]any{"field": "n", "operator": "eq", "value": 0}}}, CodeConditionFailed, ""},
		{"schema on the result", map[string]any{"push": map[string]any{"x": 1}, "updates": map[string]any{"n": "text"}}, CodeSchemaViolation, ""},
		{"nothing to change", map[string]any{}, CodeInvalidArgument, "give updates, unset, or an operator"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wantCode(t, upd(tt.args), tt.code, tt.want)
			if after := get("d"); !reflect.DeepEqual(after, before) {
				t.Errorf("a failed update wrote: %v -> %v", before, after)
			}
		})
	}
	// SDK schema: inc takes numbers only.
	if res := upd(map[string]any{"inc": map[string]any{"n": "1"}}); !res.IsError {
		t.Error("inc with a string value succeeded")
	}
	// if that matches, and upsert from nothing.
	if res := upd(map[string]any{"inc": map[string]any{"n": 1}, "if": []any{map[string]any{"field": "n", "operator": "eq", "value": 3.5}}}); res.IsError {
		t.Error(resultText(res))
	}
	out := callOK(t, cs, "update_document", map[string]any{"collection": "o", "id": "up", "upsert": true, "updates": map[string]any{"k": "v"}, "inc": map[string]any{"n": 2}, "push": map[string]any{"l": "x"}})
	if out["upserted"] != true {
		t.Errorf("upsert: %v", out)
	}
	if d := get("up"); !reflect.DeepEqual(d, map[string]any{"_id": "up", "k": "v", "n": 2.0, "l": []any{"x"}}) {
		t.Errorf("upserted %v", d)
	}
	// update_documents with operators; one bad document fails them all.
	callOK(t, cs, "insert_document", map[string]any{"collection": "o", "document": map[string]any{"_id": "e", "n": 10, "tags": "not an array"}})
	wantCode(t, call(t, cs, "update_documents", map[string]any{"collection": "o", "all": true, "push": map[string]any{"tags": "z"}}), CodeInvalidArgument, "push")
	if d := get("d"); !reflect.DeepEqual(d["tags"], []any{"a", "b"}) {
		t.Errorf("a failed update_documents wrote: %v", d)
	}
	out = callOK(t, cs, "update_documents", map[string]any{"collection": "o", "all": true, "inc": map[string]any{"n": 1}})
	if out["matched"] != 3.0 || out["modified"] != 3.0 {
		t.Errorf("update_documents inc = %v", out)
	}
	if d := get("e"); d["n"] != 11.0 {
		t.Errorf("e.n = %v, want 11", d["n"])
	}
	wantCode(t, call(t, cs, "update_documents", map[string]any{"collection": "o", "all": true, "updates": map[string]any{"n": 1}, "inc": map[string]any{"n": 1}}), CodeInvalidArgument, `"n"`)
	wantCode(t, call(t, cs, "update_documents", map[string]any{"collection": "o", "all": true, "updates": map[string]any{"n": "x"}}), CodeSchemaViolation)
}

// TestExistsStartsWithMCP (AC-24): both operators in find_documents,
// count_documents, distinct, and if, with and without an index.
func TestExistsStartsWithMCP(t *testing.T) {
	cs := connect(t, newTestServer(t))
	callOK(t, cs, "create_collection", map[string]any{"name": "q"})
	for _, d := range []map[string]any{
		{"_id": "a", "due": "2026-01", "name": "alpha"},
		{"_id": "b", "due": nil, "name": "Alpine"},
		{"_id": "c", "name": "beta"},
		{"_id": "d", "due": "2027-05", "name": "alps", "meta": map[string]any{"by": "ann"}},
	} {
		callOK(t, cs, "insert_document", map[string]any{"collection": "q", "document": d})
	}
	f := func(field, op string, v any) map[string]any {
		return map[string]any{"filters": []any{map[string]any{"field": field, "operator": op, "value": v}}}
	}
	check := func(t *testing.T) {
		for _, tt := range []struct {
			query map[string]any
			want  []string
		}{
			{f("due", "exists", true), []string{"a", "b", "d"}},
			{f("due", "exists", false), []string{"c"}},
			{f("meta.by", "exists", true), []string{"d"}},
			{f("name", "starts_with", "al"), []string{"a", "d"}},
			{f("name", "starts_with", "Al"), []string{"b"}},
			{f("due", "starts_with", "2026"), []string{"a"}},
			{f("name", "starts_with", ""), []string{"a", "b", "c", "d"}},
		} {
			out := callOK(t, cs, "find_documents", map[string]any{"collection": "q", "query": tt.query})
			var got []string
			for _, d := range out["documents"].([]any) {
				got = append(got, d.(map[string]any)["_id"].(string))
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("find %v = %v, want %v", tt.query, got, tt.want)
			}
			if n := callOK(t, cs, "count_documents", map[string]any{"collection": "q", "query": tt.query})["count"]; n != float64(len(tt.want)) {
				t.Errorf("count %v = %v, want %d", tt.query, n, len(tt.want))
			}
		}
		vals := callOK(t, cs, "distinct", map[string]any{"collection": "q", "field": "name", "query": f("due", "exists", false)})["values"]
		if !reflect.DeepEqual(vals, []any{"beta"}) {
			t.Errorf("distinct with exists false = %v", vals)
		}
		vals = callOK(t, cs, "distinct", map[string]any{"collection": "q", "field": "due", "query": f("name", "starts_with", "alp")})["values"]
		if !reflect.DeepEqual(vals, []any{"2026-01", "2027-05"}) {
			t.Errorf("distinct with starts_with = %v", vals)
		}
	}
	t.Run("scan", check)
	callOK(t, cs, "create_index", map[string]any{"collection": "q", "index_name": "by_due", "field_name": "due"})
	callOK(t, cs, "create_index", map[string]any{"collection": "q", "index_name": "by_name", "field_name": "name"})
	t.Run("indexed", check)

	// In if: compare-and-set on presence and prefix.
	wantCode(t, call(t, cs, "update_document", map[string]any{"collection": "q", "id": "c", "updates": map[string]any{"x": 1},
		"if": []any{map[string]any{"field": "due", "operator": "exists", "value": true}}}), CodeConditionFailed)
	callOK(t, cs, "update_document", map[string]any{"collection": "q", "id": "c", "updates": map[string]any{"x": 1},
		"if": []any{map[string]any{"field": "due", "operator": "exists", "value": false}, map[string]any{"field": "name", "operator": "starts_with", "value": "be"}}})
	wantCode(t, call(t, cs, "delete_document", map[string]any{"collection": "q", "id": "a",
		"if": []any{map[string]any{"field": "name", "operator": "starts_with", "value": "be"}}}), CodeConditionFailed)
	// Bad values are argument errors.
	wantCode(t, call(t, cs, "find_documents", map[string]any{"collection": "q", "query": f("due", "exists", "yes")}), CodeInvalidArgument, "exists")
	wantCode(t, call(t, cs, "count_documents", map[string]any{"collection": "q", "query": f("name", "starts_with", 5)}), CodeInvalidArgument, "starts_with")
}

// TestUniqueIndexMCP (AC-24, F-015, F-016): create_index unique,
// describe_collection reports it, a duplicate fails with
// unique_violation, and copy_collection keeps it unique.
func TestUniqueIndexMCP(t *testing.T) {
	cs := connect(t, newTestServer(t))
	callOK(t, cs, "create_collection", map[string]any{"name": "u"})
	callOK(t, cs, "insert_document", map[string]any{"collection": "u", "document": map[string]any{"_id": "a", "email": "a@x", "k": 1}})
	callOK(t, cs, "insert_document", map[string]any{"collection": "u", "document": map[string]any{"_id": "b", "email": "b@x", "k": 1}})
	out := callOK(t, cs, "create_index", map[string]any{"collection": "u", "index_name": "by_email", "field_name": "email", "unique": true})
	if !strings.Contains(out["message"].(string), "Unique index") {
		t.Errorf("message %v", out["message"])
	}
	callOK(t, cs, "create_index", map[string]any{"collection": "u", "index_name": "by_k", "field_name": "k"})
	wantCode(t, call(t, cs, "create_index", map[string]any{"collection": "u", "index_name": "k_unique", "field_name": "k", "unique": true}), CodeUniqueViolation)
	wantIndexes := []any{map[string]any{"name": "by_email", "field": "email", "unique": true}, map[string]any{"name": "by_k", "field": "k"}}
	if got := callOK(t, cs, "describe_collection", map[string]any{"collection": "u"})["indexes"]; !reflect.DeepEqual(got, wantIndexes) {
		t.Errorf("describe_collection indexes = %v, want %v", got, wantIndexes)
	}
	wantCode(t, call(t, cs, "insert_document", map[string]any{"collection": "u", "document": map[string]any{"email": "a@x"}}), CodeUniqueViolation, "insert document")
	wantCode(t, call(t, cs, "update_document", map[string]any{"collection": "u", "id": "b", "updates": map[string]any{"email": "a@x"}}), CodeUniqueViolation)
	wantCode(t, call(t, cs, "insert_documents", map[string]any{"collection": "u", "documents": []any{map[string]any{"email": "n@x"}, map[string]any{"email": "n@x"}}}), CodeUniqueViolation, "documents[1]")

	out = callOK(t, cs, "copy_collection", map[string]any{"collection": "u", "new_name": "u2"})
	if out["copied"] != 2.0 {
		t.Errorf("copy = %v", out)
	}
	if got := callOK(t, cs, "describe_collection", map[string]any{"collection": "u2"})["indexes"]; !reflect.DeepEqual(got, wantIndexes) {
		t.Errorf("copied indexes = %v, want %v", got, wantIndexes)
	}
	wantCode(t, call(t, cs, "insert_document", map[string]any{"collection": "u2", "document": map[string]any{"email": "b@x"}}), CodeUniqueViolation)
}

// TestErrorCodes (AC-25): every code of ErrorCodes is produced by a real
// failure (the codes no tool call can reach here are checked on
// toolError), errors.Is still finds the engine's sentinel, and the errors
// topic documents every code.
func TestErrorCodes(t *testing.T) {
	s, engine := newConfigServer(t, db.Options{MaxBatchDocs: 2}, Config{})
	cs := connect(t, s)
	callOK(t, cs, "create_collection", map[string]any{"name": "e", "schema": map[string]any{"fields": map[string]any{"n": map[string]any{"type": "number"}}}})
	callOK(t, cs, "insert_document", map[string]any{"collection": "e", "document": map[string]any{"_id": "a", "n": 1, "u": "x"}})
	callOK(t, cs, "create_index", map[string]any{"collection": "e", "index_name": "u_unique", "field_name": "u", "unique": true})
	in := func(extra map[string]any) map[string]any {
		args := map[string]any{"collection": "e"}
		maps.Copy(args, extra)
		return args
	}
	seen := map[string]bool{}
	for _, tt := range []struct {
		code, tool string
		args       map[string]any
	}{
		{CodeNotFound, "find_documents", map[string]any{"collection": "nope"}},
		{CodeNotFound, "get_document", in(map[string]any{"id": "zz"})},
		{CodeAlreadyExists, "create_collection", map[string]any{"name": "e"}},
		{CodeAlreadyExists, "insert_document", in(map[string]any{"document": map[string]any{"_id": "a", "n": 2}})},
		{CodeInvalidName, "create_collection", map[string]any{"name": "bad name!"}},
		{CodeInvalidName, "create_index", in(map[string]any{"index_name": "-x", "field_name": "n"})},
		{CodeInvalidArgument, "find_documents", in(map[string]any{"query": map[string]any{"sort": []any{map[string]any{"field": ""}}}})},
		{CodeInvalidArgument, "delete_documents", in(nil)},
		{CodeInvalidArgument, "update_document", in(map[string]any{"id": "a", "inc": map[string]any{"u": 1}})},
		{CodeSchemaViolation, "insert_document", in(map[string]any{"document": map[string]any{"n": "x"}})},
		{CodeSchemaViolation, "replace_document", in(map[string]any{"id": "a", "document": map[string]any{"_id": "b"}})},
		{CodeUniqueViolation, "insert_document", in(map[string]any{"document": map[string]any{"u": "x"}})},
		{CodeConditionFailed, "delete_document", in(map[string]any{"id": "a", "if": []any{map[string]any{"field": "n", "operator": "eq", "value": 9}}})},
		{CodeLimitExceeded, "insert_documents", in(map[string]any{"documents": []any{map[string]any{}, map[string]any{}, map[string]any{}}})},
		{CodeLimitExceeded, "update_documents", in(map[string]any{"all": true, "updates": map[string]any{"z": 1}})}, // only 1 doc: see below
	} {
		if tt.tool == "update_documents" {
			callOK(t, cs, "insert_documents", in(map[string]any{"documents": []any{map[string]any{"n": 5}, map[string]any{"n": 6}}}))
		}
		res := call(t, cs, tt.tool, tt.args)
		wantCode(t, res, tt.code)
		seen[tt.code] = true
	}

	// unavailable: the engine is shut down under the server.
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantCode(t, call(t, cs, "count_documents", in(nil)), CodeUnavailable, "shut down")
	seen[CodeUnavailable] = true

	// permission_denied comes from an access check (also in
	// TestAccessTableEveryTool).
	env := newAuthEnv(t)
	env.addUser(t, "bob", auth.UserOptions{})
	env.resetFixtures(t)
	wantCode(t, call(t, env.as(t, "bob"), "get_document", map[string]any{"database": accessDB, "collection": accessColl, "id": "d1"}), CodePermissionDenied, `"bob"`)
	seen[CodePermissionDenied] = true

	// cancelled and internal: no tool call here reaches them; check the
	// mapping on toolError.
	for _, tt := range []struct {
		err  error
		code string
	}{
		{context.Canceled, CodeCancelled},
		{fmt.Errorf("reading: %w", context.DeadlineExceeded), CodeCancelled},
		{errors.New("something unexpected"), CodeInternal},
		{errNoPrincipal, CodeInternal},
		{db.ErrQueryInterrupted, CodeUnavailable},
		{db.ErrWALFailed, CodeUnavailable},
		{&db.BatchItemError{Index: 3, Err: fmt.Errorf("x: %w", db.ErrUniqueViolation)}, CodeUniqueViolation},
		{fmt.Errorf("%w: 12 documents match", db.ErrBatchTooLarge), CodeLimitExceeded},
		{auth.ErrUnauthenticated, CodePermissionDenied},
		{auth.ErrInvalidLevel, CodeInvalidArgument},
		{db.ErrInvalidArgument, CodeInvalidArgument},
		{db.ErrInvalidUpdate, CodeInvalidArgument},
	} {
		err := toolError("do thing", tt.err)
		if code, ok := ErrorCodeOf(err.Error()); !ok || code != tt.code {
			t.Errorf("toolError(%v) = %q, want code %q", tt.err, err, tt.code)
		}
		if !strings.HasPrefix(err.Error(), "["+tt.code+"] do thing: ") {
			t.Errorf("toolError(%v) = %q, want the code, then the action", tt.err, err)
		}
		if !errors.Is(err, tt.err) {
			t.Errorf("errors.Is(toolError(%v), cause) = false", tt.err)
		}
		seen[tt.code] = true
	}
	if !errors.Is(toolError("x", fmt.Errorf("doc: %w", db.ErrNotFound)), db.ErrNotFound) {
		t.Error("errors.Is lost ErrNotFound")
	}
	if err := toolError("x", invalidArgf("bad %d", 1)); !errors.Is(err, db.ErrInvalidArgument) || err.Error() != "[invalid_argument] x: bad 1" {
		t.Errorf("invalidArgf: %v", err)
	}
	for _, code := range ErrorCodes {
		if !seen[code] {
			t.Errorf("code %s was never produced", code)
		}
	}

	// ErrorCodeOf.
	for text, want := range map[string]bool{
		"[not_found] x":  true,
		"[nope] x":       false,
		"not_found x":    false,
		"[not_found]x":   false,
		"[internal] ":    true,
		"":               false,
		"[invalid_name]": false,
	} {
		if _, ok := ErrorCodeOf(text); ok != want {
			t.Errorf("ErrorCodeOf(%q) ok = %v, want %v", text, ok, want)
		}
	}

	// The errors topic lists every code.
	errorsTopic := readResource(t, connect(t, newTestServer(t)), "cachydb://docs/errors")
	for _, code := range ErrorCodes {
		if !strings.Contains(errorsTopic, code) {
			t.Errorf("errors topic does not document %s", code)
		}
	}
}

// readResource reads a docs resource as text.
func readResource(t *testing.T, cs *mcp.ClientSession, uri string) string {
	t.Helper()
	res, err := cs.ReadResource(context.Background(), &mcp.ReadResourceParams{URI: uri})
	if err != nil {
		t.Fatalf("ReadResource(%s): %v", uri, err)
	}
	var b strings.Builder
	for _, c := range res.Contents {
		b.WriteString(c.Text)
	}
	return b.String()
}

// TestServerStatsAuthOff (AC-26): without tokens everyone gets every
// database and the server totals.
func TestServerStatsAuthOff(t *testing.T) {
	cs := connect(t, newTestServer(t))
	callOK(t, cs, "create_database", map[string]any{"name": "shop"})
	callOK(t, cs, "create_collection", map[string]any{"database": "shop", "name": "orders"})
	callOK(t, cs, "insert_documents", map[string]any{"database": "shop", "collection": "orders", "documents": []any{map[string]any{"a": 1}, map[string]any{"a": 2}}})
	callOK(t, cs, "create_index", map[string]any{"database": "shop", "collection": "orders", "index_name": "by_a", "field_name": "a"})
	callOK(t, cs, "find_documents", map[string]any{"database": "shop", "collection": "orders"})

	out := callOK(t, cs, "server_stats", map[string]any{})
	var st ServerStatsOutput
	data, _ := json.Marshal(out)
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range st.Databases {
		names = append(names, d.Name)
	}
	if !slices.Equal(names, []string{testDefaultDB, "shop"}) {
		t.Fatalf("databases %v", names)
	}
	shop := st.Databases[1]
	if shop.Documents != 2 || len(shop.Collections) != 1 || shop.Collections[0].Name != "orders" || shop.Collections[0].Documents != 2 ||
		!slices.Equal(shop.Collections[0].Indexes, []string{"by_a"}) || shop.Collections[0].IndexBytes <= 0 || shop.IndexBytes != shop.Collections[0].IndexBytes {
		t.Errorf("shop stats %+v", shop)
	}
	if st.Server == nil || st.Server.Version != testVersion || st.Server.WALBytes <= 0 || st.Server.Cache.MaxBytes <= 0 || st.Server.UptimeSeconds < 0 {
		t.Errorf("server totals %+v", st.Server)
	}
	// One database only.
	out = callOK(t, cs, "server_stats", map[string]any{"database": "shop"})
	if dbs := out["databases"].([]any); len(dbs) != 1 || dbs[0].(map[string]any)["name"] != "shop" || out["server"] == nil {
		t.Errorf("server_stats shop = %v", out)
	}
	wantCode(t, call(t, cs, "server_stats", map[string]any{"database": "nope"}), CodeNotFound)
	// The session's database choice does not narrow it.
	callOK(t, cs, "use_database", map[string]any{"name": "shop"})
	if dbs := callOK(t, cs, "server_stats", map[string]any{})["databases"].([]any); len(dbs) != 2 {
		t.Errorf("after use_database: %d databases, want 2", len(dbs))
	}
}

// TestServerStatsPerPrincipal (AC-26): a user sees only the databases the
// user can read and no server block; an unreadable database named
// explicitly is refused; a server admin sees everything.
func TestServerStatsPerPrincipal(t *testing.T) {
	env := newAuthEnv(t)
	env.addUser(t, "bob", auth.UserOptions{})
	env.resetFixtures(t)
	env.setAccess(t, "bob", false, map[string]auth.Level{accessDB: auth.LevelRead})
	names := func(out map[string]any) []string {
		var got []string
		for _, d := range out["databases"].([]any) {
			got = append(got, d.(map[string]any)["name"].(string))
		}
		return got
	}
	bob := env.as(t, "bob")
	out := callOK(t, bob, "server_stats", map[string]any{})
	if got := names(out); !slices.Equal(got, []string{accessDB}) {
		t.Errorf("bob sees %v, want [%s]", got, accessDB)
	}
	if _, ok := out["server"]; ok {
		t.Errorf("bob got the server totals: %v", out["server"])
	}
	out = callOK(t, bob, "server_stats", map[string]any{"database": accessDB})
	if got := names(out); !slices.Equal(got, []string{accessDB}) || out["server"] != nil {
		t.Errorf("bob, database %s: %v", accessDB, out)
	}
	wantCode(t, call(t, bob, "server_stats", map[string]any{"database": accessOther}), CodePermissionDenied, `"bob"`, accessOther)
	wantCode(t, call(t, bob, "server_stats", map[string]any{"database": "does-not-exist"}), CodePermissionDenied)

	// No grants at all: an empty list, not an error.
	env.setAccess(t, "bob", false, nil)
	if got := names(callOK(t, bob, "server_stats", map[string]any{})); len(got) != 0 {
		t.Errorf("bob without grants sees %v", got)
	}
	// A grant on every database.
	env.setAccess(t, "bob", false, map[string]auth.Level{auth.AllDatabases: auth.LevelRead})
	if got := names(callOK(t, bob, "server_stats", map[string]any{})); !slices.Contains(got, accessOther) || !slices.Contains(got, accessDB) {
		t.Errorf("bob with read on every database sees %v", got)
	}

	root := callOK(t, env.as(t, "root"), "server_stats", map[string]any{})
	if got := names(root); !slices.Contains(got, accessDB) || !slices.Contains(got, accessOther) || !slices.Contains(got, testDefaultDB) {
		t.Errorf("root sees %v", got)
	}
	if root["server"] == nil {
		t.Error("root did not get the server totals")
	}
	if slices.Contains(names(root), "_system") {
		t.Error("server_stats lists the system database")
	}
}

// TestResultText (AC-27): full is the default and unchanged (the text is
// the structured content's JSON); summary gives one line of text with
// complete structured content; errors are the same in both.
func TestResultText(t *testing.T) {
	type session struct {
		name string
		cs   *mcp.ClientSession
	}
	var sessions []session
	for _, mode := range []string{"", ResultTextFull, ResultTextSummary} {
		s, _ := newConfigServer(t, db.Options{}, Config{ResultText: mode})
		sessions = append(sessions, session{mode, connect(t, s)})
	}
	// The same operations on each server; ids are given so results agree.
	calls := []struct {
		tool        string
		args        map[string]any
		wantSummary string // "" = only check it is one line
	}{
		{"create_collection", map[string]any{"name": "r"}, "Collection 'r' created in database 'default'"},
		{"insert_documents", map[string]any{"collection": "r", "documents": []any{map[string]any{"_id": "a", "n": 1}, map[string]any{"_id": "b", "n": 2}, map[string]any{"_id": "c", "n": 3}}}, "3 documents inserted"},
		{"find_documents", map[string]any{"collection": "r", "query": map[string]any{"limit": 2}}, "2 documents, has_more: true, next_skip: 2"},
		{"find_documents", map[string]any{"collection": "r", "query": map[string]any{"skip": 2}}, "1 document, has_more: false"},
		{"count_documents", map[string]any{"collection": "r"}, "3 documents"},
		{"get_document", map[string]any{"collection": "r", "id": "a"}, "1 document, _id: a"},
		{"update_documents", map[string]any{"collection": "r", "all": true, "inc": map[string]any{"n": 1}}, "3 matched, 3 modified"},
		{"delete_documents", map[string]any{"collection": "r", "query": map[string]any{"filters": []any{map[string]any{"field": "n", "operator": "gt", "value": 3}}}}, "1 matched, 1 deleted"},
		{"distinct", map[string]any{"collection": "r", "field": "n"}, "2 distinct values"},
		{"describe_collection", map[string]any{"collection": "r"}, "collection r in database default: 2 documents, 0 indexes, schema: none"},
		{"list_databases", map[string]any{}, "1 database"},
		{"list_collections", map[string]any{}, "1 collection in database default"},
		{"current_database", map[string]any{}, "current database: default"},
		{"update_document", map[string]any{"collection": "r", "id": "a", "updates": map[string]any{"x": 1}}, "Document a updated"},
		{"server_stats", map[string]any{"database": "default"}, ""},
	}
	for _, c := range calls {
		var results []*mcp.CallToolResult
		for _, s := range sessions {
			res := call(t, s.cs, c.tool, c.args)
			if res.IsError {
				t.Fatalf("%s on the %q server: %s", c.tool, s.name, resultText(res))
			}
			results = append(results, res)
		}
		def, full, sum := results[0], results[1], results[2]
		// full: the text decodes to the structured content.
		var fromText any
		if err := json.Unmarshal([]byte(resultText(full)), &fromText); err != nil {
			t.Errorf("%s full: text is not JSON: %v", c.tool, err)
		}
		if !reflect.DeepEqual(fromText, full.StructuredContent) {
			t.Errorf("%s full: text %s differs from the structured content %v", c.tool, resultText(full), full.StructuredContent)
		}
		if len(full.Content) != 1 || len(def.Content) != 1 {
			t.Errorf("%s: %d and %d content blocks, want 1", c.tool, len(full.Content), len(def.Content))
		}
		// "" is full, byte for byte (server_stats has an uptime that may tick).
		if c.tool != "server_stats" && resultText(def) != resultText(full) {
			t.Errorf("%s: default text %q, full text %q", c.tool, resultText(def), resultText(full))
		}
		// summary: one line; the structured content is the same as in full.
		text := resultText(sum)
		if strings.Contains(text, "\n") || text == "" || len(sum.Content) != 1 {
			t.Errorf("%s summary: %q is not one line", c.tool, text)
		}
		if c.wantSummary != "" && text != c.wantSummary {
			t.Errorf("%s summary = %q, want %q", c.tool, text, c.wantSummary)
		}
		if c.tool != "server_stats" && !reflect.DeepEqual(sum.StructuredContent, full.StructuredContent) {
			t.Errorf("%s summary: structured content %v, full has %v", c.tool, sum.StructuredContent, full.StructuredContent)
		}
	}
	// find_documents with explain in summary mode names the plan.
	res := call(t, sessions[2].cs, "find_documents", map[string]any{"collection": "r", "explain": true})
	if text := resultText(res); !strings.HasPrefix(text, "2 documents, has_more: false, plan: ") || !strings.Contains(text, "examined: ") {
		t.Errorf("summary with explain = %q", text)
	}
	// Errors are the same text in both modes.
	for _, args := range []map[string]any{{"collection": "nope"}, {"collection": "r", "query": map[string]any{"sort": []any{map[string]any{"field": ""}}}}} {
		a, b := call(t, sessions[1].cs, "find_documents", args), call(t, sessions[2].cs, "find_documents", args)
		if !a.IsError || !b.IsError || resultText(a) != resultText(b) {
			t.Errorf("error in full mode %q, in summary mode %q", resultText(a), resultText(b))
		}
	}
}

// TestNewConfigValidation: New rejects a negative limit or byte cap and an
// unknown result text mode.
func TestNewConfigValidation(t *testing.T) {
	ctx := context.Background()
	engine, err := db.Open(ctx, db.Options{Dir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close(ctx) })
	for _, cfg := range []Config{
		{DefaultDatabase: "d", DefaultLimit: -1},
		{DefaultDatabase: "d", MaxResultBytes: -1},
		{DefaultDatabase: "d", ResultText: "verbose"},
		{DefaultDatabase: "d", ResultText: "FULL"},
	} {
		if _, err := New(ctx, engine, cfg); err == nil {
			t.Errorf("New(%+v) succeeded", cfg)
		}
	}
	for _, mode := range []string{"full", "summary"} {
		if err := ValidateResultText(mode); err != nil {
			t.Errorf("ValidateResultText(%q): %v", mode, err)
		}
	}
	if err := ValidateResultText(""); err == nil {
		t.Error(`ValidateResultText("") succeeded`)
	}
}

// TestSummaryLines covers the summary of every output type that the
// other tests do not produce.
func TestSummaryLines(t *testing.T) {
	for _, tt := range []struct {
		out  any
		want string
	}{
		{MessageOutput{Message: "m"}, "m"},
		{UseDatabaseOutput{Message: "u"}, "u"},
		{InsertDocumentOutput{Message: "i"}, "i"},
		{CopyCollectionOutput{Message: "c"}, "c"},
		{SetAccessOutput{Message: "s"}, "s"},
		{ListUsersOutput{Users: make([]UserInfo, 2)}, "2 users"},
		{DistinctOutput{Values: []any{1}}, "1 distinct value"},
		{DescribeCollectionOutput{Name: "c", Database: "d", Count: 1, Indexes: make([]IndexDesc, 1), Schema: &SchemaDef{Fields: map[string]FieldDef{"a": {}, "b": {}}}}, "collection c in database d: 1 document, 1 index, schema: 2 fields"},
		{ServerStatsOutput{Databases: []DatabaseStats{{Documents: 3, Collections: make([]CollectionStats, 2)}, {Documents: 4}}}, "2 databases, 2 collections, 7 documents"},
		{WhoamiOutput{User: "bob", Grants: map[string]string{"x": "read"}}, "user bob, server_admin: false, 1 grant"},
		{CreateUserOutput{Message: "User created.", Label: "default", Token: "tok"}, "User created. Token (label default): tok"},
		{CreateTokenOutput{Message: "Token created.", Token: "tok"}, "Token created. Token: tok"},
		{FindDocumentsOutput{Count: 0, Plan: &QueryPlan{Driver: "index", Index: "i", Examined: 4}}, "0 documents, has_more: false, plan: index i, examined: 4"},
		{struct{}{}, "ok"},
	} {
		if got := summaryOf(tt.out); got != tt.want {
			t.Errorf("summaryOf(%T) = %q, want %q", tt.out, got, tt.want)
		}
	}
}
