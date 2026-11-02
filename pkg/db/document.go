package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

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
// stored in the memtable and the cache are immutable; callers of the read
// path get deep clones and never share memory with them.

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
	data := resolveNowData(doc.Data, nowString(time.Now()))
	_, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		if cur != nil {
			return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrExists)
		}
		return &Document{ID: id, Data: data}, nil
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
// set contains "_id" or the result does not satisfy the schema. It is
// UpdateWith with only Set.
func (c *Collection) Update(ctx context.Context, id string, set map[string]any) (*Document, error) {
	doc, _, err := c.UpdateWith(ctx, id, UpdateSpec{Set: set})
	return doc, err
}

// UpdateWith applies u to the document id and returns a copy of the new
// version (plan 004 §1.2–§1.4): the fields of u.Set are replaced or
// added, then the fields of u.Unset are removed. When u.If is not empty
// the stored version must match all of its filters, checked under the
// document's write lock right before the WAL append; otherwise it fails
// with ErrConditionFailed and writes nothing. A missing document fails
// with ErrConditionFailed if u.If is not empty, is created as {_id: id} +
// u.Set if u.Upsert (upserted is then true), and fails with ErrNotFound
// otherwise. $now values in u.Set and u.If are resolved once. It fails
// with ErrSchemaValidation if u.Set or u.Unset names "_id", a field is in
// both, or the result does not satisfy the schema.
func (c *Collection) UpdateWith(ctx context.Context, id string, u UpdateSpec) (doc *Document, upserted bool, err error) {
	if _, ok := u.Set["_id"]; ok {
		return nil, false, fmt.Errorf("%w: updating document %q: field \"_id\" cannot be changed", ErrSchemaValidation, id)
	}
	for _, f := range u.Unset {
		if f == "_id" {
			return nil, false, fmt.Errorf("%w: updating document %q: field \"_id\" cannot be unset", ErrSchemaValidation, id)
		}
		if _, ok := u.Set[f]; ok {
			return nil, false, fmt.Errorf("%w: updating document %q: field %q in both updates and unset", ErrSchemaValidation, id, f)
		}
	}
	if u.Upsert {
		if err := checkDocID(id); err != nil {
			return nil, false, fmt.Errorf("upserting document: %w", err)
		}
	}
	ts := nowString(time.Now())
	set := resolveNowData(u.Set, ts)
	cond := resolveNowFilters(u.If, ts)
	next, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		upserted = false
		if err := c.checkCondition(id, cur, cond); err != nil {
			return nil, err
		}
		if cur == nil {
			if !u.Upsert {
				return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrNotFound)
			}
			upserted = true
			return &Document{ID: id, Data: set}, nil
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
		for _, k := range u.Unset {
			delete(data, k)
		}
		return &Document{ID: id, Data: data}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return next.Clone(), upserted, nil
}

// Replace atomically replaces the whole document id with doc and returns
// a copy of the new version (plan 004 §1.3). doc.ID must be empty or id,
// and doc.Data must not contain "_id" (ErrSchemaValidation). The document
// must exist: ErrNotFound if it does not (ErrConditionFailed if cond is
// not empty). When cond is not empty the stored version must match all of
// its filters (ErrConditionFailed otherwise). $now values are resolved;
// the result must satisfy the schema. doc is not modified or retained.
func (c *Collection) Replace(ctx context.Context, id string, doc *Document, cond Condition) (*Document, error) {
	if doc == nil {
		return nil, errors.New("replacing document: document is nil")
	}
	if doc.ID != "" && doc.ID != id {
		return nil, fmt.Errorf("%w: replacing document %q: the document has a different _id %q", ErrSchemaValidation, id, doc.ID)
	}
	if _, ok := doc.Data["_id"]; ok {
		return nil, fmt.Errorf("%w: replacing document %q: field \"_id\" in data is reserved; set Document.ID", ErrSchemaValidation, id)
	}
	ts := nowString(time.Now())
	data := resolveNowData(doc.Data, ts)
	cond = resolveNowFilters(cond, ts)
	next, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		if err := c.checkCondition(id, cur, cond); err != nil {
			return nil, err
		}
		if cur == nil {
			return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrNotFound)
		}
		return &Document{ID: id, Data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	return next.Clone(), nil
}

// Delete removes the document id (ErrNotFound if absent). It is
// DeleteWith without a condition.
func (c *Collection) Delete(ctx context.Context, id string) error {
	return c.DeleteWith(ctx, id, nil)
}

// DeleteWith removes the document id if it matches every filter of cond
// (plan 004 §1.2). It fails with ErrConditionFailed if cond is not empty
// and the document does not match it or is absent, and with ErrNotFound
// if cond is empty and the document is absent.
func (c *Collection) DeleteWith(ctx context.Context, id string, cond Condition) error {
	cond = resolveNowFilters(cond, nowString(time.Now()))
	_, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		if err := c.checkCondition(id, cur, cond); err != nil {
			return nil, err
		}
		if cur == nil {
			return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, ErrNotFound)
		}
		return nil, nil
	})
	return err
}

// checkCondition returns ErrConditionFailed if cond is not empty and cur
// (the stored version of id, nil if absent) does not match all of it.
func (c *Collection) checkCondition(id string, cur *Document, cond Condition) error {
	if len(cond) == 0 || (cur != nil && matchesAll(cur, cond)) {
		return nil
	}
	return fmt.Errorf("%w: document %s no longer matches if", ErrConditionFailed, id)
}

// checkDocID checks that id can be stored.
func checkDocID(id string) error {
	if id == "" {
		return errors.New("document id must not be empty")
	}
	if len(id) > maxIDLen {
		return fmt.Errorf("id is %d bytes, max %d", len(id), maxIDLen)
	}
	return nil
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
	// Backpressure: wait, holding no lock, while too much is unflushed.
	if err := e.waitPending(ctx); err != nil {
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
			// A *SchemaError, which wraps ErrSchemaValidation.
			return nil, fmt.Errorf("document %q in collection %q: %w", id, c.name, err)
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
	delta := size
	if ok {
		c.memBytes -= me.size
		delta -= me.size
	}
	c.db.e.pending.add(delta)
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
// memtable or the cache and must be neither modified nor handed out
// without a Clone; otherwise it is a private copy. (Every path currently
// returns shared documents, so callers clone only what they hand out.)
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
		return doc, false, nil
	}
}

// cachedDoc is a document cache value: an immutable decoded document and
// the number of bytes it is charged.
type cachedDoc struct {
	doc  *Document
	size int64
}

// cachedDocSize charges a decoded document twice its encoded length: the
// decoded maps and values take roughly that much more memory than the
// JSON they came from.
func cachedDocSize(encodedLen int) int64 { return 2 * int64(encodedLen) }

func newDocCache(maxBytes int64) *cache.LRU[cachedDoc] {
	return cache.New(maxBytes, func(v cachedDoc) int64 { return v.size })
}

// readStored reads the version of id described by the keydir entry ke,
// from the cache or else from its data segment (and caches it). The
// returned document is shared with the cache: it must be neither modified
// nor handed out without a Clone.
func (c *Collection) readStored(id string, ke keyEntry) (*Document, error) {
	lru, key := c.db.e.cache, c.cacheKey(id)
	if v, ok := lru.Get(key, ke.walOffset); ok {
		return v.doc, nil
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
	lru.Put(key, ke.walOffset, cachedDoc{doc: doc, size: cachedDocSize(len(rec.Doc))})
	return doc, nil
}

// acquireReader returns the reader of data segment seg with a reference
// taken (the caller must Release it), opening the segment on first use.
// It returns errSegmentRetired if compaction retired the segment, also
// when that happens while the segment is being opened.
func (c *Collection) acquireReader(seg uint32) (*segment.Reader, error) {
	c.mu.RLock()
	r, ok := c.readers[seg]
	_, gone := c.retired[seg]
	c.mu.RUnlock()
	if gone {
		return nil, errSegmentRetired
	}
	if !ok {
		nr, err := segment.OpenReader(c.dir, seg)
		if err != nil {
			c.mu.RLock()
			_, gone = c.retired[seg]
			c.mu.RUnlock()
			if gone {
				return nil, errSegmentRetired // removed by compaction meanwhile
			}
			return nil, fmt.Errorf("collection %q: %w", c.name, err)
		}
		c.mu.Lock()
		_, gone = c.retired[seg]
		switch {
		case c.dropped:
			c.mu.Unlock()
			_ = nr.Close() // read-only file, nothing to lose
			return nil, c.errDropped()
		case c.readersClosed:
			c.mu.Unlock()
			_ = nr.Close() // read-only file, nothing to lose
			return nil, ErrClosed
		case gone:
			c.mu.Unlock()
			_ = nr.Close() // read-only file of a retired segment, nothing to lose
			return nil, errSegmentRetired
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
	ts := nowString(time.Now())
	filters := resolveNowFilters(q.Filters, ts)
	anyOf := resolveNowGroups(q.Any, ts)

	ids, err := c.candidates(filters)
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
		if doc == nil || !matchesQuery(doc, filters, anyOf) {
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

// candidates returns, sorted, the ids Find must examine for filters: the
// smallest candidate set among eq filters on _id and the index-backed
// filters on an indexed field (eq and has with an indexable value; has_any,
// the union of its values' sets; has_all, their intersection), otherwise
// every live id. Candidates are a superset of the matches: Find re-checks
// every filter.
func (c *Collection) candidates(filters []QueryFilter) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dropped {
		return nil, c.errDropped()
	}
	if ids, ok := c.indexedCandidatesLocked(filters); ok {
		return ids, nil
	}
	return c.liveIDsLocked(), nil
}

// indexedCandidatesLocked returns the smallest index-backed candidate set
// for filters (see candidates), or false if no filter can use an index.
// Callers hold mu.
func (c *Collection) indexedCandidatesLocked(filters []QueryFilter) ([]string, bool) {
	var best []string
	found := false
	for _, f := range filters {
		ids, ok := c.filterCandidatesLocked(f)
		if ok && (!found || len(ids) < len(best)) {
			best, found = ids, true
		}
	}
	return best, found
}

// filterCandidatesLocked returns the sorted ids that may match f through
// the primary key or a secondary index, or false if f cannot use one.
// Callers hold mu.
func (c *Collection) filterCandidatesLocked(f QueryFilter) ([]string, bool) {
	if f.Field == "_id" {
		if f.Operator != "eq" {
			return nil, false
		}
		// The keydir/memtable are the primary index. A non-string value
		// equals no id.
		if s, ok := f.Value.(string); ok {
			return []string{s}, true
		}
		return nil, true
	}
	switch f.Operator {
	case "eq", "has", "has_any", "has_all":
	default:
		return nil, false
	}
	ix := c.indexOnLocked(f.Field)
	if ix == nil {
		return nil, false
	}
	switch f.Operator {
	case "has_any":
		vals, ok := listValues(f.Value)
		if !ok {
			return nil, true // matches nothing
		}
		return ix.lookupAny(vals)
	case "has_all":
		vals, ok := listValues(f.Value)
		if !ok {
			return nil, true // matches nothing
		}
		return ix.lookupAll(vals)
	default: // eq, has
		if _, ok := indexKey(f.Value); !ok {
			return nil, false // objects/arrays/NaN: not indexed, scan
		}
		return ix.lookup(f.Value), true
	}
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
