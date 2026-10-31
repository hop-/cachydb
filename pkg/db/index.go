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
// maps to the set of ids of the documents holding that value. Documents
// whose field is missing or not indexable have no entry. An index is not
// safe for concurrent use; the owning collection serializes access.
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
	key, ok := ix.docKey(doc)
	if !ok {
		return
	}
	ids, ok := ix.keys[key]
	if !ok {
		ids = make(map[string]struct{})
		ix.keys[key] = ids
	}
	ids[id] = struct{}{}
}

// remove deletes id from the entry for doc's value, leaving other
// documents with the same value in place. doc must be the version that was
// added.
func (ix *index) remove(id string, doc *Document) {
	key, ok := ix.docKey(doc)
	if !ok {
		return
	}
	ids, ok := ix.keys[key]
	if !ok {
		return
	}
	delete(ids, id)
	if len(ids) == 0 {
		delete(ix.keys, key)
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

// docKey returns the index key of doc's indexed field.
func (ix *index) docKey(doc *Document) (string, bool) {
	if doc == nil {
		return "", false
	}
	v, ok := doc.GetValue(ix.field)
	if !ok {
		return "", false
	}
	return indexKey(v)
}

// sortedIDs returns the members of set in ascending order, or nil if the
// set is empty.
func sortedIDs(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(set))
}
