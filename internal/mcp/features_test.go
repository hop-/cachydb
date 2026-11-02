package mcpserver

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

// TestNewToolsEndToEnd exercises every tool and parameter plan 004 added,
// over in-memory transports.
func TestNewToolsEndToEnd(t *testing.T) {
	cs := connect(t, newTestServer(t))
	coll := map[string]any{"collection": "kb"}
	with := func(extra map[string]any) map[string]any {
		out := map[string]any{"collection": "kb"}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	callOK(t, cs, "create_collection", map[string]any{"name": "kb"})
	for _, doc := range []map[string]any{
		{"_id": "k1", "type": "note", "title": "WAL internals", "tags": []any{"wal", "storage"}, "prio": 2, "version": 1},
		{"_id": "k2", "type": "howto", "title": "Compaction", "tags": []any{"storage"}, "prio": 1, "version": 1},
		{"_id": "k3", "type": "note", "title": "MCP tools", "tags": []any{"mcp"}, "prio": 3, "version": 1},
	} {
		callOK(t, cs, "insert_document", with(map[string]any{"document": doc}))
	}
	callOK(t, cs, "create_index", with(map[string]any{"index_name": "by_tags", "field_name": "tags"}))

	ids := func(out map[string]any) []string {
		var got []string
		for _, d := range out["documents"].([]any) {
			got = append(got, d.(map[string]any)["_id"].(string))
		}
		return got
	}
	find := func(query map[string]any) map[string]any {
		return callOK(t, cs, "find_documents", with(map[string]any{"query": query}))
	}

	t.Run("find with new operators, any, sort, fields", func(t *testing.T) {
		out := find(map[string]any{"filters": []any{map[string]any{"field": "title", "operator": "contains", "value": "wal"}}})
		if got := ids(out); !slices.Equal(got, []string{"k1"}) {
			t.Errorf("contains = %v", got)
		}
		out = find(map[string]any{"filters": []any{map[string]any{"field": "tags", "operator": "has_any", "value": []any{"mcp", "storage"}}},
			"sort": []any{map[string]any{"field": "prio", "order": "desc"}}, "fields": []any{"title"}})
		if got := ids(out); !slices.Equal(got, []string{"k3", "k1", "k2"}) {
			t.Errorf("has_any sorted = %v", got)
		}
		if doc := out["documents"].([]any)[0].(map[string]any); len(doc) != 2 || doc["title"] != "MCP tools" {
			t.Errorf("projection returned %v", doc)
		}
		out = find(map[string]any{"any": []any{
			[]any{map[string]any{"field": "type", "operator": "eq", "value": "howto"}},
			[]any{map[string]any{"field": "tags", "operator": "has_all", "value": []any{"wal", "storage"}}},
		}})
		if got := ids(out); !slices.Equal(got, []string{"k1", "k2"}) {
			t.Errorf("any = %v", got)
		}
	})

	t.Run("count and distinct", func(t *testing.T) {
		out := callOK(t, cs, "count_documents", with(map[string]any{"query": map[string]any{
			"filters": []any{map[string]any{"field": "tags", "operator": "has", "value": "storage"}}}}))
		if out["count"] != 2.0 {
			t.Errorf("count = %v", out["count"])
		}
		if out := callOK(t, cs, "count_documents", coll); out["count"] != 3.0 {
			t.Errorf("count all = %v", out["count"])
		}
		out = callOK(t, cs, "distinct", with(map[string]any{"field": "tags"}))
		if got := out["values"]; !reflect.DeepEqual(got, []any{"mcp", "storage", "wal"}) {
			t.Errorf("distinct tags = %v", got)
		}
		out = callOK(t, cs, "distinct", with(map[string]any{"field": "prio", "query": map[string]any{
			"filters": []any{map[string]any{"field": "type", "operator": "eq", "value": "note"}}}}))
		if got := out["values"]; !reflect.DeepEqual(got, []any{2.0, 3.0}) {
			t.Errorf("distinct prio of notes = %v", got)
		}
	})

	t.Run("conditional update, unset, upsert, $now", func(t *testing.T) {
		cond := []any{map[string]any{"field": "version", "operator": "eq", "value": 1}}
		out := callOK(t, cs, "update_document", with(map[string]any{"id": "k1", "if": cond,
			"updates": map[string]any{"version": 2, "seen": map[string]any{"$now": true}}, "unset": []any{"prio"}}))
		if out["upserted"] != false {
			t.Errorf("upserted = %v", out["upserted"])
		}
		res := call(t, cs, "update_document", with(map[string]any{"id": "k1", "if": cond, "updates": map[string]any{"version": 3}}))
		if text := resultText(res); !res.IsError || !strings.Contains(text, "condition failed: document k1 no longer matches if; re-read it and retry") {
			t.Errorf("stale compare-and-set: %v %s", res.IsError, text)
		}
		doc := find(map[string]any{"filters": []any{map[string]any{"field": "_id", "operator": "eq", "value": "k1"}}})["documents"].([]any)[0].(map[string]any)
		if _, ok := doc["prio"]; ok || doc["version"] != 2.0 {
			t.Errorf("after update: %v", doc)
		}
		if seen, ok := doc["seen"].(string); !ok || !strings.HasSuffix(seen, "Z") {
			t.Errorf("$now stored as %v", doc["seen"])
		}
		out = callOK(t, cs, "update_document", with(map[string]any{"id": "k9", "upsert": true, "updates": map[string]any{"type": "note"}}))
		if out["upserted"] != true {
			t.Errorf("upsert = %v", out)
		}
		if res := call(t, cs, "update_document", with(map[string]any{"id": "k1"})); !res.IsError {
			t.Error("update without updates or unset succeeded")
		}
	})

	t.Run("replace and conditional delete", func(t *testing.T) {
		callOK(t, cs, "replace_document", with(map[string]any{"id": "k9", "document": map[string]any{"_id": "k9", "type": "decision"}}))
		if res := call(t, cs, "replace_document", with(map[string]any{"id": "k9", "document": map[string]any{"_id": "other"}})); !res.IsError {
			t.Error("replace with a different _id succeeded")
		}
		if res := call(t, cs, "delete_document", with(map[string]any{"id": "k9",
			"if": []any{map[string]any{"field": "type", "operator": "eq", "value": "note"}}})); !res.IsError {
			t.Error("conditional delete of a non-matching document succeeded")
		}
		callOK(t, cs, "delete_document", with(map[string]any{"id": "k9",
			"if": []any{map[string]any{"field": "type", "operator": "eq", "value": "decision"}}}))
	})

	t.Run("describe, update_schema, copy, rename, delete, drop_index", func(t *testing.T) {
		out := callOK(t, cs, "describe_collection", coll)
		if out["name"] != "kb" || out["database"] != "default" || out["schema"] != nil || out["count"] != 3.0 ||
			!reflect.DeepEqual(out["indexes"], []any{map[string]any{"name": "by_tags", "field": "tags"}}) {
			t.Errorf("describe = %v", out)
		}
		schema := map[string]any{"fields": map[string]any{"title": map[string]any{"type": "string", "required": true}}}
		callOK(t, cs, "update_schema", with(map[string]any{"schema": schema}))
		strict := map[string]any{"fields": map[string]any{"owner": map[string]any{"type": "string", "required": true}}}
		res := call(t, cs, "update_schema", with(map[string]any{"schema": strict}))
		if text := resultText(res); !res.IsError || !strings.Contains(text, "k1 (required field missing: owner)") {
			t.Errorf("rejected schema: %s", text)
		}
		out = callOK(t, cs, "describe_collection", coll)
		if !reflect.DeepEqual(out["schema"], map[string]any{"fields": map[string]any{"title": map[string]any{"type": "string", "required": true}}}) {
			t.Errorf("schema = %v", out["schema"])
		}
		callOK(t, cs, "create_database", map[string]any{"name": "backup"})
		out = callOK(t, cs, "copy_collection", with(map[string]any{"new_name": "kb_copy", "target_database": "backup"}))
		if out["copied"] != 3.0 {
			t.Errorf("copied = %v", out["copied"])
		}
		callOK(t, cs, "rename_collection", map[string]any{"database": "backup", "collection": "kb_copy", "new_name": "kb_old"})
		out = callOK(t, cs, "describe_collection", map[string]any{"database": "backup", "collection": "kb_old"})
		if out["count"] != 3.0 || out["schema"] == nil || len(out["indexes"].([]any)) != 1 {
			t.Errorf("renamed copy = %v", out)
		}
		callOK(t, cs, "drop_index", map[string]any{"database": "backup", "collection": "kb_old", "index_name": "by_tags"})
		if res := call(t, cs, "drop_index", map[string]any{"database": "backup", "collection": "kb_old", "index_name": "by_tags"}); !res.IsError {
			t.Error("dropping a missing index succeeded")
		}
		callOK(t, cs, "delete_collection", map[string]any{"database": "backup", "collection": "kb_old"})
		if res := call(t, cs, "delete_collection", map[string]any{"database": "backup", "collection": "kb_old"}); !res.IsError ||
			!strings.Contains(resultText(res), "not found") {
			t.Errorf("deleting a missing collection: %s", resultText(res))
		}
		out = callOK(t, cs, "list_collections", map[string]any{"database": "backup"})
		if len(out["collections"].([]any)) != 0 {
			t.Errorf("collections after delete = %v", out["collections"])
		}
		callOK(t, cs, "update_schema", with(map[string]any{"schema": nil}))
		if out := callOK(t, cs, "describe_collection", coll); out["schema"] != nil {
			t.Errorf("schema after null = %v", out["schema"])
		}
	})
}
