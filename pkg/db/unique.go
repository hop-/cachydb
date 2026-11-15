package db

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// Unique indexes (approach-index-and-query §3, "Unique indexes").
//
// A unique index rejects a document write whose value for the index
// already belongs to another live document (ErrUniqueViolation). Only an
// indexable scalar is constrained: null, a boolean, a number (not NaN), or
// a string. A missing field, an array, an object, and NaN are not. Two
// values are the same exactly when eq treats them as equal (distinctKey:
// 1 and 1.0 are the same number; long strings compare in full, not by
// their truncated index encoding). A write that keeps the value its
// document already has is not checked, so updating or replacing a document
// may keep its own value.
//
// The check runs on the write path under the document lock, before the WAL
// append, against the overlay (memtable and flushing) and the index file.
// Two writers with different ids and the same value are serialized by a
// striped lock per (index, encoded value), taken after the document lock
// and held until the write is applied (so across the WAL append):
//
//	Database.mu (R) → docLock → uniqueLocks (ascending stripe) → Collection.mu
//
// The overlay is searched through uniquePending, a per-collection map of
// the unique values of the unflushed documents: applyLocked adds a
// document's values, an overwrite in the memtable removes the values the
// document no longer holds in any overlay layer, and the flush removes the
// values of the flushed entries once its index transaction has committed.
// An entry of the map is only a hint: the check always confirms it against
// the overlay document of that id, so a stale entry costs nothing but its
// memory.
//
// WAL replay never fails on a unique violation (the record is committed):
// it applies the write and logs a warning. Creating a unique index over
// documents that already share a value fails and lists up to 10 of the
// values with their ids.

// numUniqueLocks is the number of striped (index, value) locks per
// collection.
const numUniqueLocks = 256

// maxUniqueDuplicates is how many duplicate values a failed unique index
// creation lists, and how many ids it lists per value.
const maxUniqueDuplicates = 10

// IndexOption configures CreateIndex.
type IndexOption func(*indexDef)

// Unique makes the index unique when on is true: a document write whose
// value for the index already belongs to another document then fails with
// ErrUniqueViolation and writes nothing. Missing fields, arrays, objects,
// and NaN are not constrained.
func Unique(on bool) IndexOption {
	return func(d *indexDef) { d.Unique = on }
}

// uniqueValue is the constrained value a document holds for one unique
// index.
type uniqueValue struct {
	def  indexDef
	v    any    // the value, for error messages
	enc  string // idxstore encoding, possibly truncated: the index entries' value and the lock and pending key
	dkey string // identity: equal exactly when eq treats the values as equal (distinctKey)
}

// uniqueValueOf returns the value doc holds for the unique index def, and
// false when the value is not constrained (missing, not a scalar) or doc
// is nil.
func uniqueValueOf(doc *Document, def indexDef) (uniqueValue, bool) {
	if doc == nil {
		return uniqueValue{}, false
	}
	v, ok := doc.GetPath(def.Field)
	if !ok {
		return uniqueValue{}, false
	}
	enc, truncated, ok := idxstore.AppendValue(nil, v)
	if !ok {
		return uniqueValue{}, false
	}
	uv := uniqueValue{def: def, v: v, enc: string(enc), dkey: "v" + string(enc)}
	if truncated {
		uv.dkey = "j" + canonicalJSON(v) // distinctKey
	}
	return uv, true
}

// holds reports whether doc (nil = none) holds the value uv for its index.
func holds(doc *Document, uv uniqueValue) bool {
	w, ok := uniqueValueOf(doc, uv.def)
	return ok && w.dkey == uv.dkey
}

// changedUniqueValues returns the values next holds for the collection's
// unique indexes that cur (the version it replaces, nil if none) does not
// hold already: the values the write has to check. Callers hold
// Database.mu (shared is enough).
func (c *Collection) changedUniqueValues(cur, next *Document) []uniqueValue {
	var out []uniqueValue
	for _, def := range c.indexes {
		if !def.Unique {
			continue
		}
		uv, ok := uniqueValueOf(next, def)
		if !ok || holds(cur, uv) {
			continue
		}
		out = append(out, uv)
	}
	return out
}

// lockUnique locks the stripes of vals in ascending order (each once) and
// returns the function that unlocks them.
func (c *Collection) lockUnique(vals []uniqueValue) func() {
	stripes := make([]int, 0, len(vals))
	for _, uv := range vals {
		stripes = append(stripes, uniqueStripe(uv.def.Name, uv.enc))
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	locks := make([]*sync.Mutex, len(stripes))
	for i, s := range stripes {
		locks[i] = &c.uniqueLocks[s]
		locks[i].Lock()
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}
}

// uniqueStripe returns the stripe of (index name, encoded value): FNV-1a
// of the name, a zero byte, and the value.
func uniqueStripe(name, enc string) int {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(name); i++ {
		h ^= uint32(name[i])
		h *= prime32
	}
	h *= prime32 // the zero separator
	for i := 0; i < len(enc); i++ {
		h ^= uint32(enc[i])
		h *= prime32
	}
	return int(h % numUniqueLocks)
}

// checkUnique returns an error wrapping ErrUniqueViolation if a live
// document other than id holds one of vals. Callers hold the stripes of
// vals (lockUnique) and the document lock of id, or run in recovery. It
// holds no read transaction when it returns.
func (c *Collection) checkUnique(id string, vals []uniqueValue) error {
	for attempt := 0; ; attempt++ {
		err := c.checkUniqueOnce(id, vals)
		if errors.Is(err, errSegmentRetired) && attempt < maxReadRetries {
			continue // compaction moved a document it was reading
		}
		return err
	}
}

// checkUniqueOnce is one attempt of checkUnique: the overlay first (under
// mu), then the index file (the snapshot rule of document.go), then the
// overlay again for the ids the index file named, because the overlay
// decides for its ids.
func (c *Collection) checkUniqueOnce(id string, vals []uniqueValue) error {
	c.mu.RLock()
	st, err := c.storeLocked()
	if err == nil {
		for _, uv := range vals {
			for _, other := range c.uniquePending[uv.def.Name][uv.enc] {
				if other == id {
					continue
				}
				if doc, _ := c.overlayDocLocked(other); holds(doc, uv) {
					c.mu.RUnlock()
					return uniqueErr(uv, other)
				}
			}
		}
	}
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	if st == nil {
		if st, err = c.openStoreOnUse(); err != nil {
			return err
		}
	}

	type candidate struct {
		val int // index into vals
		id  string
	}
	var cands []candidate
	err = st.View(func(tx *idxstore.Tx) error {
		for i, uv := range vals {
			cur, err := tx.IndexCursor(uv.def.Name)
			if err != nil {
				return err
			}
			enc := []byte(uv.enc)
			for e, ok := cur.Seek(idxstore.ValuePrefix(enc)); ok && bytes.Equal(e.Value, enc); e, ok = cur.Next() {
				other := string(e.ID)
				if e.Flags&idxstore.FlagElement != 0 || other == id {
					continue
				}
				if e.Flags&idxstore.FlagTruncated != 0 {
					// The encoding is a prefix only: compare the values.
					ke, ok, err := tx.Key(other)
					if err != nil {
						return err
					}
					if !ok {
						continue
					}
					doc, err := c.readStored(other, ke)
					if err != nil {
						return err
					}
					if !holds(doc, uv) {
						continue
					}
				}
				cands = append(cands, candidate{val: i, id: other})
			}
			if err := cur.Err(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return c.storeErr(err)
	}
	if len(cands) == 0 {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, cd := range cands {
		uv := vals[cd.val]
		if doc, in := c.overlayDocLocked(cd.id); in && !holds(doc, uv) {
			continue // a newer, unflushed version decides
		}
		return uniqueErr(uv, cd.id)
	}
	return nil
}

// overlayDocLocked returns the unflushed version of id (nil for a
// tombstone) and whether there is one; the memtable wins over flushing.
// Callers hold mu.
func (c *Collection) overlayDocLocked(id string) (*Document, bool) {
	if me, ok := c.memtable[id]; ok {
		return me.doc, true
	}
	if me, ok := c.flushing[id]; ok {
		return me.doc, true
	}
	return nil, false
}

func uniqueErr(uv uniqueValue, other string) error {
	return fmt.Errorf("%w: index %q on field %q: value %s already belongs to document %q",
		ErrUniqueViolation, uv.def.Name, uv.def.Field, shortJSON(uv.v), other)
}

// shortJSON returns the canonical JSON of v, cut to 64 bytes.
func shortJSON(v any) string {
	s := canonicalJSON(v)
	if n := 64; len(s) > n {
		for n > 0 && !utf8.RuneStart(s[n]) {
			n-- // do not cut a character
		}
		return s[:n] + "…"
	}
	return s
}

// addUniquePendingLocked records the unique values of doc (the new
// unflushed version of id) in uniquePending. Callers hold mu exclusively.
func (c *Collection) addUniquePendingLocked(id string, doc *Document) {
	if doc == nil {
		return
	}
	for _, def := range c.indexes {
		if def.Unique {
			c.addUniquePendingDefLocked(def, id, doc)
		}
	}
}

// addUniquePendingDefLocked records the value doc holds for the unique
// index def, if any. Callers hold mu exclusively.
func (c *Collection) addUniquePendingDefLocked(def indexDef, id string, doc *Document) {
	uv, ok := uniqueValueOf(doc, def)
	if !ok {
		return
	}
	if c.uniquePending == nil {
		c.uniquePending = make(map[string]map[string][]string)
	}
	byVal := c.uniquePending[def.Name]
	if byVal == nil {
		byVal = make(map[string][]string)
		c.uniquePending[def.Name] = byVal
	}
	if ids := byVal[uv.enc]; !slices.Contains(ids, id) {
		byVal[uv.enc] = append(ids, id)
	}
}

// fillUniquePendingLocked records the values of every overlay document for
// the unique index def, after it was created. Callers hold mu exclusively.
func (c *Collection) fillUniquePendingLocked(def indexDef) {
	if !def.Unique {
		return
	}
	for _, layer := range []map[string]memEntry{c.flushing, c.memtable} {
		for id, me := range layer {
			c.addUniquePendingDefLocked(def, id, me.doc)
		}
	}
}

// removeUniquePendingLocked removes id from the pending value enc of index
// name. Callers hold mu exclusively.
func (c *Collection) removeUniquePendingLocked(name, enc, id string) {
	byVal := c.uniquePending[name]
	ids := byVal[enc]
	i := slices.Index(ids, id)
	if i < 0 {
		return
	}
	ids = slices.Delete(ids, i, i+1)
	switch {
	case len(ids) > 0:
		byVal[enc] = ids
	case len(byVal) > 1:
		delete(byVal, enc)
	default:
		delete(c.uniquePending, name)
	}
}

// replaceUniquePendingLocked is called by applyLocked when the memtable
// entry old of id is overwritten by next: the values old held that neither
// next nor the flushing version of id holds are removed. Callers hold mu
// exclusively.
func (c *Collection) replaceUniquePendingLocked(id string, old, next *Document) {
	if old == nil || len(c.uniquePending) == 0 {
		return
	}
	var flushed *Document
	if me, ok := c.flushing[id]; ok {
		flushed = me.doc
	}
	for _, def := range c.indexes {
		if !def.Unique {
			continue
		}
		uv, ok := uniqueValueOf(old, def)
		if !ok || holds(next, uv) || holds(flushed, uv) {
			continue
		}
		c.removeUniquePendingLocked(def.Name, uv.enc, id)
	}
}

// pendingDrop is a value of a flushed entry to remove from uniquePending.
type pendingDrop struct {
	uv uniqueValue
	id string
}

// flushedUniqueValues returns the unique values of the flushing entries,
// for flushDropUniquePendingLocked. defs is the flush's index set.
func flushedUniqueValues(flushing map[string]memEntry, defs []indexDef) []pendingDrop {
	var drops []pendingDrop
	for _, def := range defs {
		if !def.Unique {
			continue
		}
		for id, me := range flushing {
			if uv, ok := uniqueValueOf(me.doc, def); ok {
				drops = append(drops, pendingDrop{uv: uv, id: id})
			}
		}
	}
	return drops
}

// flushDropUniquePendingLocked removes the values of flushed entries from
// uniquePending once they are in the index file, except where the newer
// memtable version of the id still holds the value. Callers hold mu
// exclusively (flush step 4).
func (c *Collection) flushDropUniquePendingLocked(drops []pendingDrop) {
	for _, d := range drops {
		if me, ok := c.memtable[d.id]; ok && holds(me.doc, d.uv) {
			continue
		}
		c.removeUniquePendingLocked(d.uv.def.Name, d.uv.enc, d.id)
	}
}

// uniqueOwner is a document holding a value of an index being created as
// unique.
type uniqueOwner struct {
	dkey string
	v    any
	id   string
}

// duplicateValues returns nil if no two owners share a value, and
// otherwise an error wrapping ErrUniqueViolation that lists up to
// maxUniqueDuplicates shared values with up to as many ids each. It sorts
// owners.
func duplicateValues(owners []uniqueOwner) error {
	slices.SortFunc(owners, func(a, b uniqueOwner) int {
		if c := strings.Compare(a.dkey, b.dkey); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})
	var (
		listed []string
		total  int
	)
	for i := 0; i < len(owners); {
		j := i + 1
		for j < len(owners) && owners[j].dkey == owners[i].dkey {
			j++
		}
		if j-i > 1 {
			total++
			if len(listed) < maxUniqueDuplicates {
				n := min(j-i, maxUniqueDuplicates)
				ids := make([]string, n)
				for k := range n {
					ids[k] = fmt.Sprintf("%q", owners[i+k].id)
				}
				more := ""
				if j-i > n {
					more = fmt.Sprintf(" and %d more", j-i-n)
				}
				listed = append(listed, fmt.Sprintf("%s (documents %s%s)", shortJSON(owners[i].v), strings.Join(ids, ", "), more))
			}
		}
		i = j
	}
	if total == 0 {
		return nil
	}
	more := ""
	if total > len(listed) {
		more = fmt.Sprintf("; and %d more value(s)", total-len(listed))
	}
	return fmt.Errorf("%w: %d value(s) belong to more than one document: %s%s",
		ErrUniqueViolation, total, strings.Join(listed, "; "), more)
}
