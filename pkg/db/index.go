package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// Secondary indexes (approach-index-and-query §3). Every index lives in
// the collection's index file (index.db) as one bucket of entries
//
//	encoded value · 0x00 0x00 · document id  ->  flags
//
// maintained by the flusher in the same transaction as the key table, so
// an index always describes exactly the documents the key table lists.
// Changes that are not flushed yet (the memtable and flushing) are not in
// it: readers apply the snapshot rule (document.go) and evaluate those ids
// from the overlay alone.
//
// The entries of one document version on one index (indexKeysOf):
//
//   - field missing: one entry under the "missing" value, flags 0;
//   - an indexable scalar (null, bool, number, string): one entry, flags
//     0, plus FlagTruncated for a string cut at idxstore.MaxValueLen;
//   - an array: one entry per distinct indexable element, FlagElement
//     (plus FlagTruncated where cut);
//   - an object, NaN, or any other value that is not indexable, and an
//     array element that is not (an object, a nested array, NaN): no
//     entry. The document is then counted in the index's complex counter
//     (at most once), so readers know that the entries miss a value.
//
// An array value (empty ones included) also counts the document in the
// index's arrays counter: an index may drive a sort only while no document
// has an array, a complex value, or a truncated entry, because only then
// does every document have exactly one entry, ordered as Find sorts.
//
// The truncated counter is kept by idxstore. eq uses the entries without
// FlagElement, has those with it; every candidate is re-checked against
// every filter, so an index only has to return a superset.

// indexEntry is one entry of a document version in one index.
type indexEntry struct {
	enc   []byte // encoded value (idxstore.AppendValue, AppendMissing); never modified
	flags idxstore.Flags
}

// docIndexKeys is what one document version contributes to one index: its
// entries, whether it holds a value that has no entry (complex), and
// whether the value is an array.
type docIndexKeys struct {
	entries []indexEntry
	complex bool
	array   bool
}

// indexBase holds, per index name, the keys of one document version. A
// memtable entry keeps the base of the version in the index file that it
// replaces (see memEntry).
type indexBase map[string]docIndexKeys

// missingValue is the encoded "field is missing" value. Shared; never
// modified.
var missingValue = idxstore.AppendMissing(nil)

// dedupLinearMax is the array length up to which indexKeysOf finds
// duplicate elements by a linear search instead of a map.
const dedupLinearMax = 16

// indexKeysOf returns the entries doc contributes to an index on field
// (see the top of this file). A nil doc (no version) contributes nothing.
func indexKeysOf(doc *Document, field string) docIndexKeys {
	if doc == nil {
		return docIndexKeys{}
	}
	v, ok := doc.GetValue(field)
	if !ok {
		return docIndexKeys{entries: []indexEntry{{enc: missingValue}}}
	}
	if enc, truncated, ok := idxstore.AppendValue(nil, v); ok {
		return docIndexKeys{entries: []indexEntry{{enc: enc, flags: truncatedFlag(truncated)}}}
	}
	elems, ok := listValues(v)
	if !ok {
		return docIndexKeys{complex: true}
	}
	var (
		out  = docIndexKeys{array: true}
		seen map[string]int // enc -> position in out.entries, for long arrays
	)
	if len(elems) > dedupLinearMax {
		seen = make(map[string]int, len(elems))
	}
	for _, el := range elems {
		enc, truncated, ok := idxstore.AppendValue(nil, el)
		if !ok {
			out.complex = true
			continue
		}
		flags := idxstore.FlagElement | truncatedFlag(truncated)
		// Two elements with the same encoding share one entry (equal
		// values, or long strings cut to the same bytes; a cut one keeps
		// the entry a candidate only).
		i := -1
		if seen != nil {
			if j, dup := seen[string(enc)]; dup {
				i = j
			}
		} else {
			i = slices.IndexFunc(out.entries, func(e indexEntry) bool { return bytes.Equal(e.enc, enc) })
		}
		if i >= 0 {
			out.entries[i].flags |= flags
			continue
		}
		if seen != nil {
			seen[string(enc)] = len(out.entries)
		}
		out.entries = append(out.entries, indexEntry{enc: enc, flags: flags})
	}
	return out
}

func truncatedFlag(truncated bool) idxstore.Flags {
	if truncated {
		return idxstore.FlagTruncated
	}
	return 0
}

// indexKeysAll returns the keys of doc on every index of the collection,
// or nil when doc is nil or there is no index. Callers hold Database.mu
// (the index set changes only under it exclusively) or run in recovery.
func (c *Collection) indexKeysAll(doc *Document) indexBase {
	if doc == nil || len(c.indexes) == 0 {
		return nil
	}
	b := make(indexBase, len(c.indexes))
	for name, def := range c.indexes {
		b[name] = indexKeysOf(doc, def.Field)
	}
	return b
}

// forgetBaseLocked removes the keys of index name from the base of every
// memtable and flushing entry, after the index was created or dropped: the
// flush then reads the version in the index file to find the old keys
// (applyFlush). Callers hold mu exclusively and flushMu (or run in
// recovery), so no flush is reading the bases.
func (c *Collection) forgetBaseLocked(name string) {
	for _, layer := range []map[string]memEntry{c.memtable, c.flushing} {
		for _, me := range layer {
			delete(me.base, name) // a no-op on a nil base
		}
	}
}

// indexOp is one change of an index entry in a flush or build.
type indexOp struct {
	enc   []byte
	id    string
	flags idxstore.Flags
	del   bool
}

// compareIndexOps orders ops by index key (value, then id): the order of
// the entries in the bucket, in which bbolt takes them fastest.
func compareIndexOps(a, b indexOp) int {
	if c := bytes.Compare(a.enc, b.enc); c != 0 {
		return c
	}
	return strings.Compare(a.id, b.id)
}

// appendIndexDiff appends to ops the changes that turn the entries old of
// document id into the entries next: deletes of entries that go, puts of
// entries that are new or change flags. Unchanged entries are left alone.
func appendIndexDiff(ops []indexOp, id string, old, next []indexEntry) []indexOp {
	for _, o := range old {
		if !slices.ContainsFunc(next, func(n indexEntry) bool { return bytes.Equal(n.enc, o.enc) }) {
			ops = append(ops, indexOp{enc: o.enc, id: id, del: true})
		}
	}
	for _, n := range next {
		i := slices.IndexFunc(old, func(o indexEntry) bool { return bytes.Equal(o.enc, n.enc) })
		if i < 0 || old[i].flags != n.flags {
			ops = append(ops, indexOp{enc: n.enc, id: id, flags: n.flags})
		}
	}
	return ops
}

// applyIndexOps sorts ops by key and applies them to w.
func applyIndexOps(w *idxstore.IndexWriter, ops []indexOp) error {
	slices.SortFunc(ops, compareIndexOps)
	for _, op := range ops {
		var err error
		if op.del {
			err = w.Delete(op.enc, op.id)
		} else {
			err = w.Put(op.enc, op.id, op.flags)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// indexBuildBatch is how many index entries a build writes per index
// transaction.
const indexBuildBatch = 50_000

// buildIndexes builds the indexes defs in the index file st from every
// document its key table lists (approach-index-and-query §10, and the
// rebuild of §8/§9): an existing bucket of the same name is dropped, the
// new one is created not ready, the entries are sorted by key and written
// in batches, and the last batch stores the complex and arrays counters and marks the
// indexes ready. An interrupted build leaves an index that is not ready,
// which the next Open builds again (syncIndexes).
//
// The caller keeps the key table and the documents still: it holds
// flushMu and compactMu, or runs at Open before the flusher starts.
func (c *Collection) buildIndexes(ctx context.Context, st *idxstore.Store, defs []indexDef) error {
	if len(defs) == 0 {
		return nil
	}
	ops := make([][]indexOp, len(defs))
	complexN := make([]int64, len(defs))
	arraysN := make([]int64, len(defs))
	err := st.View(func(tx *idxstore.Tx) error {
		n := 0
		cur := tx.KeyCursor()
		for k, ke, ok := cur.First(); ok; k, ke, ok = cur.Next() {
			if n++; n%findCtxCheckEvery == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			id := string(k)
			doc, err := c.readStored(id, ke)
			if err != nil {
				return err
			}
			for i, def := range defs {
				keys := indexKeysOf(doc, def.Field)
				if keys.complex {
					complexN[i]++
				}
				if keys.array {
					arraysN[i]++
				}
				for _, en := range keys.entries {
					ops[i] = append(ops[i], indexOp{enc: en.enc, id: id, flags: en.flags})
				}
			}
		}
		return cur.Err()
	})
	if err != nil {
		return err
	}

	err = st.Update(func(u *idxstore.WTx) error {
		for _, def := range defs {
			if err := u.DropIndex(def.Name); err != nil && !errors.Is(err, idxstore.ErrNoIndex) {
				return err
			}
			if err := u.CreateIndex(def.Name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := range defs {
		slices.SortFunc(ops[i], compareIndexOps)
	}
	// Batches: the entries of every index in turn, then one last
	// transaction with the counters and the ready marks.
	for i, def := range defs {
		for rest := ops[i]; len(rest) > 0; {
			if err := ctx.Err(); err != nil {
				return err
			}
			batch := rest[:min(len(rest), indexBuildBatch)]
			rest = rest[len(batch):]
			err := st.Update(func(u *idxstore.WTx) error {
				if err := u.SetFillPercent(0.9); err != nil { // ascending keys: pack the pages
					return err
				}
				w, err := u.Index(def.Name)
				if err != nil {
					return err
				}
				for _, op := range batch {
					if err := w.Put(op.enc, op.id, op.flags); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		ops[i] = nil // release it
	}
	return st.Update(func(u *idxstore.WTx) error {
		for i, def := range defs {
			w, err := u.Index(def.Name)
			if err != nil {
				return err
			}
			if err := w.AddComplex(complexN[i]); err != nil {
				return err
			}
			if err := w.AddArrays(arraysN[i]); err != nil {
				return err
			}
			if err := w.SetReady(); err != nil {
				return err
			}
		}
		return nil
	})
}

// syncIndexes brings the index buckets of st in line with the collection's
// index definitions (coll.meta) at Open: a bucket without a definition is
// dropped (a create_index that did not reach coll.meta, or a drop_index
// that did not reach the index file), and a definition without a ready
// bucket is built (an interrupted build), with a warning. Callers have not
// published c yet.
func (c *Collection) syncIndexes(ctx context.Context, st *idxstore.Store) error {
	var stats []idxstore.IndexStats
	if err := st.View(func(tx *idxstore.Tx) (err error) {
		stats, err = tx.Indexes()
		return err
	}); err != nil {
		return err
	}
	ready := make(map[string]bool, len(stats))
	var stale []string
	for _, s := range stats {
		if _, ok := c.indexes[s.Name]; !ok {
			stale = append(stale, s.Name)
			continue
		}
		ready[s.Name] = s.Ready
	}
	logger := c.db.e.logger
	if len(stale) > 0 {
		logger.Info("cachydb: dropping index entries without an index definition (an interrupted create_index or drop_index)",
			"database", c.db.name, "collection", c.name, "indexes", stale)
		err := st.Update(func(u *idxstore.WTx) error {
			for _, name := range stale {
				if err := u.DropIndex(name); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	var build []indexDef
	for _, def := range sortedIndexDefs(c.indexes) {
		if !ready[def.Name] {
			build = append(build, def)
		}
	}
	if len(build) == 0 {
		return nil
	}
	names := make([]string, len(build))
	for i, def := range build {
		names[i] = def.Name
	}
	logger.Warn("cachydb: building indexes that are missing or incomplete in the index file",
		"database", c.db.name, "collection", c.name, "indexes", names)
	if err := c.buildIndexes(ctx, st, build); err != nil {
		return fmt.Errorf("building indexes %v: %w", names, err)
	}
	return nil
}

// indexOnLocked returns the definition of an index on field (the one with
// the smallest name if there are several), and false if there is none.
// Callers hold mu or Database.mu.
func (c *Collection) indexOnLocked(field string) (indexDef, bool) {
	var (
		best  indexDef
		found bool
	)
	for _, def := range c.indexes {
		if def.Field == field && (!found || def.Name < best.Name) {
			best, found = def, true
		}
	}
	return best, found
}

// indexesUnchanged reports whether every index of used still exists with
// the same field. A reader that chose its indexes before its read
// transaction calls it afterwards: an index dropped and re-created on
// another field in between would have answered for the wrong field.
func (c *Collection) indexesUnchanged(used []indexDef) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, u := range used {
		if def, ok := c.indexes[u.Name]; !ok || def.Field != u.Field {
			return false
		}
	}
	return true
}

// valueIDs appends to dst, in ascending order, the ids of the entries of
// value enc whose FlagElement equals element, leaving out the ids in skip
// (the overlay, which decides for its ids).
func valueIDs(cur *idxstore.IndexCursor, enc []byte, element bool, skip map[string]*Document, dst []string) []string {
	for e, ok := cur.Seek(idxstore.ValuePrefix(enc)); ok && bytes.Equal(e.Value, enc); e, ok = cur.Next() {
		if (e.Flags&idxstore.FlagElement != 0) != element {
			continue
		}
		if _, in := skip[string(e.ID)]; in {
			continue
		}
		dst = append(dst, string(e.ID))
	}
	return dst
}

// mergeSorted returns the union of the ascending, duplicate-free lists a
// and b, ascending.
func mergeSorted(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	out := make([]string, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch c := strings.Compare(a[i], b[j]); {
		case c < 0:
			out = append(out, a[i])
			i++
		case c > 0:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// distinctKey returns the key under which Distinct deduplicates v: the
// encoded value for an indexable value that was not cut, otherwise its
// canonical JSON. Values that eq treats as equal get the same key.
func distinctKey(v any) string {
	if enc, truncated, ok := idxstore.AppendValue(nil, v); ok && !truncated {
		return "v" + string(enc)
	}
	return "j" + canonicalJSON(v)
}
