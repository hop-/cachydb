package db

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// fuzzJSONSeeds are JSON values of every type, used as seeds by the value
// fuzz targets below.
var fuzzJSONSeeds = []string{
	`null`, `true`, `false`, `0`, `-0`, `1`, `1.5`, `-2e10`, `1e308`, `123456789012345678901234567890`,
	`""`, `"a"`, `"1"`, `"ü"`, `"\u0000"`,
	`[]`, `[1,"1",true,null]`, `[[1],{"a":1}]`, `[1,1,2]`,
	`{}`, `{"a":1,"b":[2,3]}`, `{"a":{"b":{"c":null}}}`,
}

// decodeJSONValue decodes data as one JSON value the way the engine does
// (numbers as float64). ok is false for invalid JSON.
func decodeJSONValue(data []byte) (any, bool) {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, false
	}
	return v, true
}

// decodeJSONNumber decodes data with json.Number numbers, a second
// representation the query code accepts.
func decodeJSONNumber(data []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	return v, true
}

// FuzzMetaRead writes random bytes as a db.meta, a coll.meta, the metadata
// checkpoint, and cachydb.format, and checks that the readers never panic
// and that an accepted checkpoint is below the end of the WAL.
func FuzzMetaRead(f *testing.F) {
	f.Add([]byte(`{"name":"main","schema_version":1,"wal_offset":3}`), uint64(10))
	f.Add([]byte(`{"name":"users","schema":{"fields":{"name":{"type":"string","required":true}}},"indexes":[{"name":"by_name","field":"name"}],"wal_offset":5,"created_offset":2}`), uint64(10))
	f.Add([]byte(`{"offset":4}`), uint64(5))
	f.Add([]byte(`{"offset":5}`), uint64(5))
	f.Add([]byte(`{"offset":-1}`), uint64(5))
	f.Add([]byte(`{"format":"cachydb","version":2}`), uint64(1))
	f.Add([]byte(`{"format":"cachydb","version":3}`), uint64(1))
	f.Add([]byte(`{"name":`), uint64(1))
	f.Add([]byte(`null`), uint64(0))
	f.Add([]byte{}, uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, data []byte, next uint64) {
		dir := t.TempDir()
		path := filepath.Join(dir, "x.meta")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		var dm dbMeta
		_ = readMeta(path, &dm) // must not panic; any error is fine
		var cm collMeta
		if err := readMeta(path, &cm); err == nil && cm.Schema != nil {
			// A decoded schema must be usable by validation.
			if cm.Schema.Validate() == nil {
				_ = cm.Schema.ValidateDocument(&Document{ID: "x", Data: map[string]any{"a": 1.0}}) // any result; it must not panic
			}
		}

		if err := os.WriteFile(filepath.Join(dir, metaCheckpointFileName), data, 0o644); err != nil {
			t.Fatal(err)
		}
		off, ok, err := readMetaCheckpoint(dir, next)
		if ok && (err != nil || off >= next) {
			t.Fatalf("readMetaCheckpoint accepted offset %d with next %d (err %v)", off, next, err)
		}
		if !ok && off != 0 {
			t.Fatalf("readMetaCheckpoint returned offset %d without ok", off)
		}

		if err := validateFormatFile(dir, data); err == nil {
			var ff formatFile
			if json.Unmarshal(data, &ff) != nil || ff.Format != formatName || ff.Version != FormatVersion {
				t.Fatalf("validateFormatFile accepted %q", data)
			}
		}
	})
}

// FuzzDecodeDocument checks that decodeDocument never panics, and that a
// decoded document re-encodes to JSON that decodes to the same document.
func FuzzDecodeDocument(f *testing.F) {
	f.Add("a", []byte(`{"_id":"a","name":"Ann","n":1,"tags":["x",2],"o":{"k":null}}`))
	f.Add("a", []byte(`{"_id":"b"}`))
	f.Add("a", []byte(`{"_id":1}`))
	f.Add("", []byte(`{"_id":1}`))
	f.Add("", []byte(`null`))
	f.Add("a", []byte(`[1,2]`))
	f.Add("a", []byte(`{"_id":"a","x":1e999}`))
	f.Add("a", []byte(`{"_id":"a","_id":"a","é":"\xff"}`))

	f.Fuzz(func(t *testing.T, id string, data []byte) {
		doc, err := decodeDocument(id, data)
		if err != nil {
			return
		}
		if doc.ID != id || doc.Data == nil {
			t.Fatalf("decodeDocument(%q) = id %q, data nil %v", id, doc.ID, doc.Data == nil)
		}
		enc, err := json.Marshal(doc)
		if err != nil {
			t.Fatalf("re-encoding a decoded document: %v", err)
		}
		again, err := decodeDocument(id, enc)
		if err != nil {
			t.Fatalf("decoding the re-encoded document %s: %v", enc, err)
		}
		if !reflect.DeepEqual(doc, again) {
			t.Fatalf("round trip changed the document:\n%#v\n%#v", doc, again)
		}
		if !reflect.DeepEqual(doc, doc.Clone()) {
			t.Fatalf("Clone differs from the document")
		}
	})
}

// FuzzIndexKey checks that the index key of every indexable JSON value
// decodes back to an equal value with the same key, that a value's key
// does not depend on its numeric representation, and that equal values
// (valuesEqual) get equal keys.
func FuzzIndexKey(f *testing.F) {
	for _, a := range fuzzJSONSeeds {
		f.Add([]byte(a), []byte(`1`))
	}
	f.Add([]byte(`0`), []byte(`-0`))
	f.Add([]byte(`1`), []byte(`1.0`))
	f.Add([]byte(`100`), []byte(`1e2`))
	f.Add([]byte(`"1"`), []byte(`1`))

	f.Fuzz(func(t *testing.T, ra, rb []byte) {
		a, ok := decodeJSONValue(ra)
		if !ok {
			return
		}
		key, indexable := indexKey(a)
		if indexable {
			if isMarkerKey(key) {
				t.Fatalf("value %#v has the marker key %q", a, key)
			}
			v, ok := decodeIndexKey(key)
			if !ok {
				t.Fatalf("decodeIndexKey(%q) failed for value %#v", key, a)
			}
			if !valuesEqual(a, v) {
				t.Fatalf("key %q of %#v decodes to unequal %#v", key, a, v)
			}
			if k2, _ := indexKey(v); k2 != key {
				t.Fatalf("decoded value %#v has key %q, want %q", v, k2, key)
			}
		}
		if an, ok := decodeJSONNumber(ra); ok {
			if !valuesEqual(a, an) {
				t.Fatalf("float64 and json.Number decodings of %s are not equal", ra)
			}
			if k2, ok2 := indexKey(an); ok2 != indexable || k2 != key {
				t.Fatalf("json.Number decoding of %s has key %q (%v), want %q (%v)", ra, k2, ok2, key, indexable)
			}
		}
		// Every key of the value (array elements included) decodes.
		for _, k := range valueKeys(a) {
			if isMarkerKey(k) {
				continue
			}
			if _, ok := decodeIndexKey(k); !ok {
				t.Fatalf("valueKeys(%#v) produced undecodable key %q", a, k)
			}
		}

		b, ok := decodeJSONValue(rb)
		if !ok {
			return
		}
		kb, bIndexable := indexKey(b)
		if valuesEqual(a, b) && (indexable != bIndexable || key != kb) {
			t.Fatalf("equal values %#v and %#v have keys %q and %q", a, b, key, kb)
		}
		if indexable && bIndexable && key == kb && !valuesEqual(a, b) {
			t.Fatalf("unequal values %#v and %#v share key %q", a, b, key)
		}
	})
}

// FuzzCompare checks the comparison and sort order laws on random JSON
// values: compare is antisymmetric and agrees with valuesEqual where it
// is defined, and the sort order is antisymmetric and transitive.
func FuzzCompare(f *testing.F) {
	for i, a := range fuzzJSONSeeds {
		b := fuzzJSONSeeds[(i+1)%len(fuzzJSONSeeds)]
		c := fuzzJSONSeeds[(i+7)%len(fuzzJSONSeeds)]
		f.Add([]byte(a), []byte(b), []byte(c))
	}
	f.Add([]byte(`1`), []byte(`1`), []byte(`1`))
	f.Add([]byte(`"a"`), []byte(`"b"`), []byte(`"c"`))
	f.Add([]byte(`[1]`), []byte(`[1.0]`), []byte(`[2]`))

	f.Fuzz(func(t *testing.T, ra, rb, rc []byte) {
		a, okA := decodeJSONValue(ra)
		b, okB := decodeJSONValue(rb)
		c, okC := decodeJSONValue(rc)
		if !okA || !okB || !okC {
			return
		}
		vals := []any{a, b, c}
		for _, x := range vals {
			if !valuesEqual(x, x) {
				t.Fatalf("valuesEqual(%#v, itself) is false", x)
			}
			if r, ok := compare(x, x); ok && r != 0 {
				t.Fatalf("compare(%#v, itself) = %d", x, r)
			}
		}
		for _, x := range vals {
			for _, y := range vals {
				cxy, okxy := compare(x, y)
				cyx, okyx := compare(y, x)
				if okxy != okyx || (okxy && cxy != -cyx) {
					t.Fatalf("compare not antisymmetric: (%#v, %#v) = %d,%v; reversed %d,%v", x, y, cxy, okxy, cyx, okyx)
				}
				if okxy && (cxy == 0) != valuesEqual(x, y) {
					t.Fatalf("compare(%#v, %#v) = %d but valuesEqual = %v", x, y, cxy, valuesEqual(x, y))
				}
				if valuesEqual(x, y) != valuesEqual(y, x) {
					t.Fatalf("valuesEqual not symmetric for %#v, %#v", x, y)
				}
				sxy := compareSortValues(newSortValue(x, true), newSortValue(y, true))
				syx := compareSortValues(newSortValue(y, true), newSortValue(x, true))
				if sxy != -syx {
					t.Fatalf("sort order not antisymmetric: (%#v, %#v) = %d, reversed %d", x, y, sxy, syx)
				}
				if okxy && cxy != sxy {
					t.Fatalf("sort order (%d) disagrees with compare (%d) for %#v, %#v", sxy, cxy, x, y)
				}
			}
		}
		missing := newSortValue(nil, false)
		for _, x := range vals {
			if compareSortValues(missing, newSortValue(x, true)) >= 0 {
				t.Fatalf("a missing field does not sort before %#v", x)
			}
		}
		// Transitivity over every ordering of the three values.
		sv := func(x any) sortValue { return newSortValue(x, true) }
		for _, p := range [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
			x, y, z := vals[p[0]], vals[p[1]], vals[p[2]]
			if compareSortValues(sv(x), sv(y)) <= 0 && compareSortValues(sv(y), sv(z)) <= 0 &&
				compareSortValues(sv(x), sv(z)) > 0 {
				t.Fatalf("sort order not transitive: %#v <= %#v <= %#v but %#v > %#v", x, y, z, x, z)
			}
		}
	})
}

// fuzzOperators are the filter operators, plus an unknown one.
var fuzzOperators = []string{"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "has", "has_any", "has_all", "bogus"}

// FuzzMatchFilter matches a random filter against a random document and
// checks that matching never panics and that the operators agree with
// each other: ne is !eq, in [v] is eq v, has_any [v] and has_all [v] are
// has v, gt/lt mirror each other, and gte is gt or eq.
func FuzzMatchFilter(f *testing.F) {
	f.Add([]byte(`{"_id":"d","a":1,"s":"Hello","t":["x",2,null],"o":{"k":1}}`), "a", uint8(0), []byte(`1`))
	f.Add([]byte(`{"_id":"d","s":"Hello"}`), "s", uint8(7), []byte(`"ell"`))
	f.Add([]byte(`{"_id":"d","t":["x",2,null]}`), "t", uint8(8), []byte(`2`))
	f.Add([]byte(`{"_id":"d","t":["x",2,null]}`), "t", uint8(10), []byte(`["x",null]`))
	f.Add([]byte(`{"_id":"d","n":5}`), "n", uint8(6), []byte(`[1,5]`))
	f.Add([]byte(`{"_id":"d","n":5}`), "_id", uint8(2), []byte(`"c"`))
	f.Add([]byte(`{"_id":"d"}`), "missing", uint8(1), []byte(`null`))

	f.Fuzz(func(t *testing.T, rawDoc []byte, field string, op uint8, rawValue []byte) {
		doc := &Document{}
		if err := json.Unmarshal(rawDoc, doc); err != nil || doc.Data == nil {
			return
		}
		value, ok := decodeJSONValue(rawValue)
		if !ok {
			return
		}
		m := func(op string, v any) bool {
			return matchesFilter(doc, QueryFilter{Field: field, Operator: op, Value: v})
		}
		operator := fuzzOperators[int(op)%len(fuzzOperators)]
		got := m(operator, value)

		_, exists := doc.GetValue(field)
		if !exists {
			if got {
				t.Fatalf("%s on a missing field matched", operator)
			}
			return
		}
		if operator == "bogus" && got {
			t.Fatalf("an unknown operator matched")
		}
		eq := m("eq", value)
		if ne := m("ne", value); ne == eq {
			t.Fatalf("ne (%v) is not !eq (%v) for field %q value %#v in %s", ne, eq, field, value, rawDoc)
		}
		if in := m("in", []any{value}); in != eq {
			t.Fatalf("in [v] (%v) differs from eq v (%v)", in, eq)
		}
		has := m("has", value)
		if hasAny := m("has_any", []any{value}); hasAny != has {
			t.Fatalf("has_any [v] (%v) differs from has v (%v)", hasAny, has)
		}
		if hasAll := m("has_all", []any{value}); hasAll != has {
			t.Fatalf("has_all [v] (%v) differs from has v (%v)", hasAll, has)
		}
		gt, gte, lt, lte := m("gt", value), m("gte", value), m("lt", value), m("lte", value)
		if gt && lt {
			t.Fatalf("both gt and lt match")
		}
		if gt && !gte || lt && !lte {
			t.Fatalf("gt/lt match but gte/lte do not")
		}
		if (gte || lte) && !gt && !lt && !eq {
			t.Fatalf("gte/lte match without gt, lt, or eq")
		}
		// The filter matches the same whether the document is matched
		// directly or through a query of one filter or one OR group.
		qf := []QueryFilter{{Field: field, Operator: operator, Value: value}}
		if matchesQuery(doc, qf, nil) != got || matchesQuery(doc, nil, [][]QueryFilter{qf}) != got {
			t.Fatalf("matchesQuery disagrees with matchesFilter for %s", operator)
		}
	})
}
