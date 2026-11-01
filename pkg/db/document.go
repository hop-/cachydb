package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/google/uuid"

	"github.com/hop-/cachydb/pkg/db/internal/cache"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Document write and read paths (§5.2, §5.3).
//
// Write: Database.mu (shared, held to the end) → docLock(id) → read the
// current version → build and validate the after-image → register
// inflight → WAL append (the commit point) → hookAfterWALSync → apply to
// the memtable, indexes, cache, and count under Collection.mu.
//
// Read: memtable → flushing → cache → keydir + data segment. Documents
// stored in the memtable are immutable; callers of the read path get deep
// clones (or freshly decoded copies) and never share memory with them.

// maxIDLen is the longest document id the WAL and segment formats can
// store (u16 length prefix).
const maxIDLen = math.MaxUint16

// maxReadRetries bounds how often a read restarts after the segment it was
// about to read was retired (compaction moved the document meanwhile).
const maxReadRetries = 8

// findCtxCheckEvery is how many candidates Find examines between context
// checks.
const findCtxCheckEvery = 256

// memEntryOverhead approximates the fixed memory cost of a memtable entry
// beyond its id and encoded document, for the MemtableSize trigger.
const memEntryOverhead = 64

// errSegmentRetired reports that a data segment was retired between the
// keydir lookup and the read; the caller looks the document up again.
var errSegmentRetired = errors.New("data segment retired")

// docLock returns the stripe lock serializing writes to id (FNV-1a).
func (c *Collection) docLock(id string) *sync.Mutex {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= prime32
	}
	return &c.docLocks[h%numDocLocks]
}

func (c *Collection) cacheKey(id string) cache.Key {
	return cache.Key{DB: c.db.name, Coll: c.name, ID: id}
}

// Insert adds doc and returns its id. An empty doc.ID gets a generated
// UUID. It fails with ErrExists if a document with the id exists and with
// ErrSchemaValidation if the document does not satisfy the collection's
// schema or carries "_id" in its data. doc is not modified or retained.
func (c *Collection) Insert(ctx context.Context, doc *Document) (string, error) {
	if doc == nil {
		return "", errors.New("inserting document: document is nil")
	}
	if _, ok := doc.Data["_id"]; ok {
		return "", fmt.Errorf("%w: inserting document: field \"_id\" in data is reserved; set Document.ID", ErrSchemaValidation)
	}
	id := doc.ID
	if id == "" {
		id = uuid.NewString()
	}
	if len(id) > maxIDLen {
		return "", fmt.Errorf("inserting document: id is %d bytes, max %d", len(id), maxIDLen)
	}
	_, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		if cur != nil {
			return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrExists)
		}
		return &Document{ID: id, Data: doc.Data}, nil
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

// Get returns a copy of the document id (ErrNotFound if absent).
func (c *Collection) Get(ctx context.Context, id string) (*Document, error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return nil, err
	}
	defer e.exit()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	doc, owned, err := c.current(id)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrNotFound)
	}
	if !owned {
		doc = doc.Clone()
	}
	return doc, nil
}

// Update merges set into the document id (top-level fields are replaced
// or added) and returns a copy of the new version. It fails with
// ErrNotFound if the document is absent and with ErrSchemaValidation if
// set contains "_id" or the result does not satisfy the schema.
func (c *Collection) Update(ctx context.Context, id string, set map[string]any) (*Document, error) {
	if _, ok := set["_id"]; ok {
		return nil, fmt.Errorf("%w: updating document %q: field \"_id\" cannot be changed", ErrSchemaValidation, id)
	}
	next, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		if cur == nil {
			return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrNotFound)
		}
		// A new top-level map: cur is shared (memtable) and must not
		// change. Nested values are only read; write re-encodes the
		// after-image into fresh values.
		data := make(map[string]any, len(cur.Data)+len(set))
		for k, v := range cur.Data {
			data[k] = v
		}
		for k, v := range set {
			data[k] = v
		}
		return &Document{ID: id, Data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	return next.Clone(), nil
}

// Delete removes the document id (ErrNotFound if absent).
func (c *Collection) Delete(ctx context.Context, id string) error {
	_, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		if cur == nil {
			return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrNotFound)
		}
		return nil, nil
	})
	return err
}

// Count returns the number of live documents in O(1).
func (c *Collection) Count() int { return int(c.count.Load()) }

// write runs one document write (§5.2). build receives the current version
// of id (nil if absent; it must not be modified) and returns the
// after-image, or nil to delete. The returned document is the stored
// after-image (nil for a delete); it is shared with the memtable, so
// callers must clone it before handing it out.
//
// Once the WAL append has started, write waits for it even if ctx is
// cancelled: the outcome is then either "durable and visible" or an error
// with nothing written, never a durable record that is not in memory.
func (c *Collection) write(ctx context.Context, id string, build func(cur *Document) (*Document, error)) (*Document, error) {
	d, e := c.db, c.db.e
	if err := e.enter(); err != nil {
		return nil, err
	}
	defer e.exit()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Lock order (§11): Database.mu (R) → docLock → Collection.mu. The
	// shared database lock blocks drops and index changes until the write
	// is applied; dropped flags change only under it exclusively.
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.dropped || c.dropped {
		return nil, c.errDropped()
	}
	lk := c.docLock(id)
	lk.Lock()
	defer lk.Unlock()

	cur, _, err := c.current(id)
	if err != nil {
		return nil, err
	}
	next, err := build(cur)
	if err != nil {
		return nil, err
	}

	rec := wal.Record{Op: wal.OpDeleteDoc, DB: d.name, Coll: c.name, ID: id}
	if next != nil {
		// The WAL payload is the full after-image including _id (§3.1).
		// The stored document is decoded from it, so it holds exactly
		// what replay and segment reads will produce, and shares no
		// memory with the caller's values.
		payload, err := json.Marshal(&Document{ID: id, Data: next.Data})
		if err != nil {
			return nil, fmt.Errorf("encoding document %q: %w", id, err)
		}
		if next, err = decodeDocument(id, payload); err != nil {
			return nil, err
		}
		if err := c.schema.ValidateDocument(next); err != nil {
			return nil, fmt.Errorf("%w: document %q in collection %q: %w", ErrSchemaValidation, id, c.name, err)
		}
		rec.Op, rec.Payload = wal.OpPutDoc, payload
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	size := int64(len(id)+len(rec.Payload)) + memEntryOverhead
	lb := c.registerInflight()
	off, err := e.appendWAL(context.WithoutCancel(ctx), rec) // commit point
	if err != nil {
		c.unregisterInflight(lb)
		return nil, fmt.Errorf("writing document %q: %w", id, err)
	}
	if err := e.hook(hookAfterWALSync); err != nil {
		return nil, err // simulated crash: no cleanup
	}
	c.apply(id, cur, next, off, lb, size)
	return next, nil
}

// registerInflight records that a document write is about to be appended
// to the WAL and returns the registered value. The record's offset is not
// known before Append returns, so the registered value is
// wal.NextOffset() read now: every record appended later gets an offset
// at least that large, so the minimum over inflight is a conservative
// bound for the low-water mark (§7.1) until the write is applied.
func (c *Collection) registerInflight() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	lb := c.db.e.wal.NextOffset()
	i, _ := slices.BinarySearch(c.inflight, lb)
	c.inflight = slices.Insert(c.inflight, i, lb)
	return lb
}

func (c *Collection) unregisterInflight(lb uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeInflightLocked(lb)
}

func (c *Collection) removeInflightLocked(lb uint64) {
	if i, ok := slices.BinarySearch(c.inflight, lb); ok {
		c.inflight = slices.Delete(c.inflight, i, i+1)
	}
}

// apply makes a durable write visible (§5.2 step 5). prev is the version
// the write replaced (nil if none); next is the after-image (nil for a
// delete); size is the entry's approximate size for the MemtableSize
// flush trigger. Memory only.
func (c *Collection) apply(id string, prev, next *Document, off, lb uint64, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeInflightLocked(lb)
	c.applyLocked(id, prev != nil, prev, next, off, size)
}

// applyLocked is apply without the inflight bookkeeping, shared with WAL
// replay. existed reports whether a live version of id existed before the
// write; prev is that version when the caller has it (needed only to
// update the indexes, so replay passes nil when the collection has none).
// Callers hold mu exclusively.
func (c *Collection) applyLocked(id string, existed bool, prev, next *Document, off uint64, size int64) {
	me, ok := c.memtable[id]
	if ok && me.walOffset >= off {
		return // never happens under the doc lock; replay relies on the same rule
	}
	if ok {
		c.memBytes -= me.size
	}
	// memMin stays a lower bound of the memtable's offsets: an overwritten
	// entry may leave it lower than the true minimum, which only makes the
	// low-water mark more conservative.
	if len(c.memtable) == 0 || off < c.memMin {
		c.memMin = off
	}
	c.memtable[id] = memEntry{doc: next, walOffset: off, size: size}
	c.memBytes += size
	if c.memBytes > c.db.e.opts.MemtableSize {
		c.db.e.kickFlush()
	}
	for _, ix := range c.indexes {
		if prev != nil {
			ix.remove(id, prev)
		}
		if next != nil {
			ix.add(id, next)
		}
	}
	c.db.e.cache.Invalidate(c.cacheKey(id))
	switch {
	case !existed && next != nil:
		c.count.Add(1)
	case existed && next == nil:
		c.count.Add(-1)
	}
}

// current returns the live version of id through the read path (§5.3):
// memtable → flushing → cache → keydir + data segment. doc is nil when the
// document does not exist. When owned is false, doc is shared with the
// memtable and must be neither modified nor handed out; otherwise it is a
// private copy.
func (c *Collection) current(id string) (doc *Document, owned bool, err error) {
	for attempt := 0; ; attempt++ {
		c.mu.RLock()
		if c.dropped {
			c.mu.RUnlock()
			return nil, false, c.errDropped()
		}
		if me, ok := c.memtable[id]; ok {
			c.mu.RUnlock()
			return me.doc, false, nil
		}
		if me, ok := c.flushing[id]; ok {
			c.mu.RUnlock()
			return me.doc, false, nil
		}
		ke, ok := c.keydir[id]
		c.mu.RUnlock()
		if !ok {
			return nil, false, nil
		}

		doc, err := c.readStored(id, ke)
		if errors.Is(err, errSegmentRetired) && attempt < maxReadRetries {
			continue // compaction moved it; look it up again
		}
		if err != nil {
			return nil, false, err
		}
		return doc, true, nil
	}
}

// readStored reads the version of id described by the keydir entry ke,
// from the cache or else from its data segment (and caches it).
func (c *Collection) readStored(id string, ke keyEntry) (*Document, error) {
	lru, key := c.db.e.cache, c.cacheKey(id)
	if data, ok := lru.Get(key, ke.walOffset); ok {
		return decodeDocument(id, data)
	}

	r, err := c.acquireReader(ke.loc.Seg)
	if err != nil {
		return nil, err
	}
	rec, err := r.Read(ke.loc)
	r.Release()
	if err != nil {
		return nil, fmt.Errorf("reading document %q of collection %q: %w", id, c.name, err)
	}
	if rec.Type != segment.TypePut || rec.ID != id || rec.WALOffset != ke.walOffset {
		return nil, fmt.Errorf("reading document %q of collection %q: %w: record at segment %d byte %d is %s %q@%d, want put@%d",
			id, c.name, segment.ErrCorrupt, ke.loc.Seg, ke.loc.Pos, rec.Type, rec.ID, rec.WALOffset, ke.walOffset)
	}
	doc, err := decodeDocument(id, rec.Doc)
	if err != nil {
		return nil, err
	}
	lru.Put(key, ke.walOffset, rec.Doc) // rec.Doc is a fresh buffer, never modified
	return doc, nil
}

// acquireReader returns the reader of data segment seg with a reference
// taken (the caller must Release it), opening the segment on first use.
// It returns errSegmentRetired if the segment was retired.
func (c *Collection) acquireReader(seg uint32) (*segment.Reader, error) {
	c.mu.RLock()
	r, ok := c.readers[seg]
	c.mu.RUnlock()
	if !ok {
		nr, err := segment.OpenReader(c.dir, seg)
		if err != nil {
			return nil, fmt.Errorf("collection %q: %w", c.name, err)
		}
		c.mu.Lock()
		switch {
		case c.dropped:
			c.mu.Unlock()
			_ = nr.Close() // read-only file, nothing to lose
			return nil, c.errDropped()
		case c.readersClosed:
			c.mu.Unlock()
			_ = nr.Close() // read-only file, nothing to lose
			return nil, ErrClosed
		}
		if r, ok = c.readers[seg]; !ok {
			c.readers[seg] = nr
			r = nr
		}
		c.mu.Unlock()
		if r != nr {
			_ = nr.Close() // lost the race to another reader; read-only file
		}
	}
	if !r.Acquire() {
		return nil, errSegmentRetired
	}
	return r, nil
}

// decodeDocument decodes a stored document (WAL payload or segment record)
// and checks that it belongs to id.
func decodeDocument(id string, data []byte) (*Document, error) {
	doc := &Document{}
	if err := json.Unmarshal(data, doc); err != nil {
		return nil, fmt.Errorf("decoding document %q: %w", id, err)
	}
	if doc.ID != id {
		return nil, fmt.Errorf("decoding document %q: stored id is %q", id, doc.ID)
	}
	if doc.Data == nil {
		doc.Data = make(map[string]any)
	}
	return doc, nil
}

// Find returns copies of the documents matching every filter of q, in
// ascending _id order, after skipping q.Skip matches and returning at most
// q.Limit (0 or negative = no limit). A nil q matches everything.
//
// Candidates come from the smallest set among the eq filters on indexed
// fields (or on _id); without one, every live document is scanned. Each
// candidate is checked against all filters, and the scan stops as soon as
// skip+limit matches are found, so paging is deterministic.
func (c *Collection) Find(ctx context.Context, q *Query) ([]*Document, error) {
	docs, _, err := c.find(ctx, q)
	return docs, err
}

// find implements Find and also reports how many candidates it examined.
func (c *Collection) find(ctx context.Context, q *Query) ([]*Document, int, error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return nil, 0, err
	}
	defer e.exit()
	if q == nil {
		q = &Query{}
	}
	skip := max(q.Skip, 0)
	limit := max(q.Limit, 0)

	ids, err := c.candidates(q.Filters)
	if err != nil {
		return nil, 0, err
	}
	out := make([]*Document, 0)
	matched, examined := 0, 0
	for _, id := range ids {
		if examined%findCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return nil, examined, err
			}
		}
		examined++
		doc, owned, err := c.current(id)
		if err != nil {
			return nil, examined, err
		}
		if doc == nil || !matchesAll(doc, q.Filters) {
			continue // deleted since the candidates were taken, or no match
		}
		matched++
		if matched <= skip {
			continue
		}
		if !owned {
			doc = doc.Clone()
		}
		out = append(out, doc)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, examined, nil
}

// matchesAll reports whether doc satisfies every filter.
func matchesAll(doc *Document, filters []QueryFilter) bool {
	for _, f := range filters {
		if !matchesFilter(doc, f) {
			return false
		}
	}
	return true
}

// candidates returns, sorted, the ids Find must examine for filters: the
// smallest candidate set among eq filters on _id or on an indexed field
// with an indexable value, otherwise every live id.
func (c *Collection) candidates(filters []QueryFilter) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dropped {
		return nil, c.errDropped()
	}
	var best []string
	found := false
	for _, f := range filters {
		if f.Operator != "eq" {
			continue
		}
		var ids []string
		switch {
		case f.Field == "_id":
			// The keydir/memtable are the primary index. A non-string
			// value equals no id.
			if s, ok := f.Value.(string); ok {
				ids = []string{s}
			}
		default:
			if _, ok := indexKey(f.Value); !ok {
				continue // objects/arrays/NaN: not indexed, scan
			}
			ix := c.indexOnLocked(f.Field)
			if ix == nil {
				continue
			}
			ids = ix.lookup(f.Value)
		}
		if !found || len(ids) < len(best) {
			best, found = ids, true
		}
	}
	if found {
		return best, nil
	}
	return c.liveIDsLocked(), nil
}

// indexOnLocked returns an index on field (the one with the smallest name
// if there are several), or nil. Callers hold mu.
func (c *Collection) indexOnLocked(field string) *index {
	var best *index
	for _, ix := range c.indexes {
		if ix.field == field && (best == nil || ix.name < best.name) {
			best = ix
		}
	}
	return best
}

// liveIDsLocked returns every live id, sorted: keydir ∪ flushing ∪
// memtable, newer layers overriding older ones (tombstones remove).
// Callers hold mu.
func (c *Collection) liveIDsLocked() []string {
	set := make(map[string]struct{}, len(c.keydir)+len(c.memtable))
	for id := range c.keydir {
		set[id] = struct{}{}
	}
	for _, layer := range []map[string]memEntry{c.flushing, c.memtable} {
		for id, me := range layer {
			if me.doc == nil {
				delete(set, id)
			} else {
				set[id] = struct{}{}
			}
		}
	}
	return sortedIDs(set)
}

// buildIndex builds the index name on field from every live document.
// Callers hold Database.mu exclusively, so no document write runs.
func (c *Collection) buildIndex(ctx context.Context, name, field string) (*index, error) {
	c.mu.RLock()
	ids := c.liveIDsLocked()
	c.mu.RUnlock()

	ix := newIndex(name, field)
	for i, id := range ids {
		if i%findCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		doc, _, err := c.current(id)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			ix.add(id, doc)
		}
	}
	return ix, nil
}
