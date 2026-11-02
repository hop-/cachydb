package db

import (
	"maps"
	"math"
	"slices"
	"strconv"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// indexKey returns the canonical, type-tagged index key for v:
//
//	numbers  "n:" + strconv.FormatFloat(f, 'g', -1, 64)
//	strings  "s:" + s
//	bools    "b:t" / "b:f"
//	null     "z:"
//
// Objects, arrays, NaN, and any other value are not indexable (ok is
// false). The type tag keeps the number 1 and the string "1" apart, and
// every value that valuesEqual treats as equal maps to the same key.
func indexKey(v any) (string, bool) {
	if v == nil {
		return "z:", true
	}
	if f, ok := toFloat(v); ok {
		if math.IsNaN(f) {
			return "", false
		}
		if f == 0 {
			f = 0 // fold -0 into 0: they compare equal
		}
		return "n:" + strconv.FormatFloat(f, 'g', -1, 64), true
	}
	switch t := v.(type) {
	case string:
		return "s:" + t, true
	case bool:
		if t {
			return "b:t", true
		}
		return "b:f", true
	}
	return "", false
}

// index is a multi-value secondary index on one field: each canonical key
// maps to the set of ids of the documents holding that value. A field
// whose value is an array is indexed under the key of each indexable
// element (plan 004 §2.3; objects and arrays nested in it are skipped), so
// one document may appear under several keys. Two marker keys, which no
// value maps to, make counts and distinct values exact without reading
// documents: arrayMarker holds the documents whose field is an array, and
// unindexedMarker those whose value (or an element of it) is not
// indexable. Documents whose field is missing have no entry. An index is
// not safe for concurrent use; the owning collection serializes access.
type index struct {
	name, field string
	keys        map[string]map[string]struct{} // key -> set of ids
}

// newIndex returns an empty index called name on field.
func newIndex(name, field string) *index {
	return &index{
		name:  name,
		field: field,
		keys:  make(map[string]map[string]struct{}),
	}
}

// add records doc under id. When replacing a document, the caller removes
// the previous version first.
func (ix *index) add(id string, doc *Document) {
	for _, key := range ix.docKeys(doc) {
		ids, ok := ix.keys[key]
		if !ok {
			ids = make(map[string]struct{})
			ix.keys[key] = ids
		}
		ids[id] = struct{}{}
	}
}

// remove deletes id from the entries for doc's value, leaving other
// documents with the same value in place. doc must be the version that was
// added.
func (ix *index) remove(id string, doc *Document) {
	for _, key := range ix.docKeys(doc) {
		ids, ok := ix.keys[key]
		if !ok {
			continue
		}
		delete(ids, id)
		if len(ids) == 0 {
			delete(ix.keys, key)
		}
	}
}

// lookup returns the sorted ids of the documents whose field equals v, or
// nil if there are none. It also returns nil for a value that is not
// indexable; callers that must tell the two apart check indexKey first and
// fall back to a scan.
func (ix *index) lookup(v any) []string {
	key, ok := indexKey(v)
	if !ok {
		return nil
	}
	return sortedIDs(ix.keys[key])
}

// snapshotKeys returns the index contents as canonical key -> sorted ids,
// the shape stored in segment.IndexSnapshot.Keys. The result shares no
// memory with the index.
func (ix *index) snapshotKeys() map[string][]string {
	out := make(map[string][]string, len(ix.keys))
	for key, ids := range ix.keys {
		out[key] = sortedIDs(ids)
	}
	return out
}

// indexFromSnapshot rebuilds an index from a persisted snapshot. s must
// not be nil. Empty id lists are skipped.
func indexFromSnapshot(s *segment.IndexSnapshot) *index {
	ix := newIndex(s.Name, s.Field)
	for key, ids := range s.Keys {
		if len(ids) == 0 {
			continue
		}
		set := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		ix.keys[key] = set
	}
	return ix
}

// docKeys returns the index keys of doc's indexed field: the key of a
// scalar value, or the keys of the indexable elements of an array (each
// once). It returns nil when the field is missing or not indexable.
func (ix *index) docKeys(doc *Document) []string {
	if doc == nil {
		return nil
	}
	v, ok := doc.GetValue(ix.field)
	if !ok {
		return nil
	}
	return valueKeys(v)
}

// Marker keys (see index). Value keys always have a type tag other than
// "a" and "u".
const (
	arrayMarker     = "a:"
	unindexedMarker = "u:"
)

// valueKeys returns the index keys of a field value (see docKeys),
// marker keys included.
func valueKeys(v any) []string {
	if key, ok := indexKey(v); ok {
		return []string{key}
	}
	elems, ok := listValues(v)
	if !ok {
		return []string{unindexedMarker}
	}
	keys := []string{arrayMarker}
	unindexed := false
	for _, e := range elems {
		key, ok := indexKey(e)
		switch {
		case !ok:
			unindexed = true
		case !slices.Contains(keys, key):
			keys = append(keys, key)
		}
	}
	if unindexed {
		keys = append(keys, unindexedMarker)
	}
	return keys
}

// isMarkerKey reports whether key is a marker key, not a value key.
func isMarkerKey(key string) bool { return key == arrayMarker || key == unindexedMarker }

// countEq returns the number of documents whose field equals the
// indexable value v as a whole (array values excluded).
func (ix *index) countEq(v any) int {
	set, _ := ix.lookupSet(v)
	return len(set) - intersectionSize(set, ix.keys[arrayMarker])
}

// countHas returns the number of documents whose field is an array with
// an element equal to the indexable value v.
func (ix *index) countHas(v any) int {
	set, _ := ix.lookupSet(v)
	return intersectionSize(set, ix.keys[arrayMarker])
}

// intersectionSize returns |a ∩ b|.
func intersectionSize(a, b map[string]struct{}) int {
	if len(b) < len(a) {
		a, b = b, a
	}
	n := 0
	for id := range a {
		if _, ok := b[id]; ok {
			n++
		}
	}
	return n
}

// distinctValues returns the distinct values held in the field, array
// elements included, decoded from the keys; false if some document holds
// a value that is not indexable (the keys then miss it).
func (ix *index) distinctValues() ([]any, bool) {
	if len(ix.keys[unindexedMarker]) > 0 {
		return nil, false
	}
	out := make([]any, 0, len(ix.keys))
	for key := range ix.keys {
		if isMarkerKey(key) {
			continue
		}
		v, ok := decodeIndexKey(key)
		if !ok {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

// decodeIndexKey returns the value of a value key (see indexKey).
func decodeIndexKey(key string) (any, bool) {
	if len(key) < 2 || key[1] != ':' {
		return nil, false
	}
	body := key[2:]
	switch key[0] {
	case 'z':
		return nil, body == ""
	case 'b':
		switch body {
		case "t":
			return true, true
		case "f":
			return false, true
		}
		return nil, false
	case 'n':
		f, err := strconv.ParseFloat(body, 64)
		return f, err == nil
	case 's':
		return body, true
	}
	return nil, false
}

// lookupSet returns the set of ids under the key of v (shared with the
// index; not to be modified) and whether v is indexable.
func (ix *index) lookupSet(v any) (map[string]struct{}, bool) {
	key, ok := indexKey(v)
	if !ok {
		return nil, false
	}
	return ix.keys[key], true
}

// lookupAny returns the sorted ids of the documents holding any of vals
// (as the value or an array element), and false if some value is not
// indexable (such a value could match unindexed elements).
func (ix *index) lookupAny(vals []any) ([]string, bool) {
	union := make(map[string]struct{})
	for _, v := range vals {
		set, ok := ix.lookupSet(v)
		if !ok {
			return nil, false
		}
		for id := range set {
			union[id] = struct{}{}
		}
	}
	return sortedIDs(union), true
}

// lookupAll returns the sorted ids of the documents holding every
// indexable value of vals, a superset of the documents holding all of
// vals, and false if no value is indexable.
func (ix *index) lookupAll(vals []any) ([]string, bool) {
	var sets []map[string]struct{}
	for _, v := range vals {
		if set, ok := ix.lookupSet(v); ok {
			sets = append(sets, set)
		}
	}
	if len(sets) == 0 {
		return nil, false
	}
	slices.SortFunc(sets, func(a, b map[string]struct{}) int { return len(a) - len(b) })
	out := make(map[string]struct{}, len(sets[0]))
outer:
	for id := range sets[0] {
		for _, set := range sets[1:] {
			if _, ok := set[id]; !ok {
				continue outer
			}
		}
		out[id] = struct{}{}
	}
	return sortedIDs(out), true
}

// sortedIDs returns the members of set in ascending order, or nil if the
// set is empty.
func sortedIDs(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(set))
}
