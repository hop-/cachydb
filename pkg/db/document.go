package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
	"uuid"

	"github.com/hop-/cachydb/pkg/db/internal/cache"
	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Document write and read paths (§5.2, §5.3).
//
// Write: Database.mu (shared, held to the end) → docLock(id) → read the
// current version → build and validate the after-image → register
// inflight → WAL append (the commit point) → hookAfterWALSync → apply to
// the memtable (with the replaced version's index keys, memEntry.base),
// cache, and count under Collection.mu. The secondary indexes in the index
// file change with the flush.
//
// Read: memtable → flushing → key table (a read transaction on the index
// file) → cache or data segment. Documents stored in the memtable and the
// cache are immutable; callers of the read path get deep clones and never
// share memory with them.
//
// Snapshot rule (approach-index-and-query §5): a reader looks at the
// overlay (memtable and flushing) under Collection.mu first and only then
// starts its read transaction on the index file. The flusher clears
// flushing only after its index transaction committed, so a change is
// always in the overlay or in the transaction's snapshot, never in
// neither. An id found in the overlay is answered from the overlay alone.

// maxIDLen is the longest document id the engine stores: what an index
// key in the index file can carry (idxstore.MaxIDLen). The WAL and segment
// formats could hold up to 65,535 bytes.
const maxIDLen = idxstore.MaxIDLen

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
// key-table lookup and the read; the caller looks the document up again.
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
// UUID v7, so generated ids sort in creation order. It fails with
// ErrExists if a document with the id exists and with ErrSchemaValidation
// if the document does not satisfy the collection's schema or carries
// "_id" in its data. doc is not modified or retained.
func (c *Collection) Insert(ctx context.Context, doc *Document) (string, error) {
	if doc == nil {
		return "", errors.New("inserting document: document is nil")
	}
	if _, ok := doc.Data["_id"]; ok {
		return "", fmt.Errorf("%w: inserting document: field \"_id\" in data is reserved; set Document.ID", ErrSchemaValidation)
	}
	id := doc.ID
	if id == "" {
		id = uuid.NewV7().String()
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
	cond, err := compileFilters(resolveNowFilters(u.If, ts))
	if err != nil {
		return nil, false, fmt.Errorf("updating document %q: if: %w", id, err)
	}
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
		maps.Copy(data, cur.Data)
		maps.Copy(data, set)
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
	ccond, err := compileFilters(resolveNowFilters(cond, ts))
	if err != nil {
		return nil, fmt.Errorf("replacing document %q: if: %w", id, err)
	}
	next, err := c.write(ctx, id, func(cur *Document) (*Document, error) {
		if err := c.checkCondition(id, cur, ccond); err != nil {
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
	ccond, err := compileFilters(resolveNowFilters(cond, nowString(time.Now())))
	if err != nil {
		return fmt.Errorf("deleting document %q: if: %w", id, err)
	}
	_, err = c.write(ctx, id, func(cur *Document) (*Document, error) {
		if err := c.checkCondition(id, cur, ccond); err != nil {
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
func (c *Collection) checkCondition(id string, cur *Document, cond []compiledFilter) error {
	if len(cond) == 0 || (cur != nil && matchAll(cur, cond)) {
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

	cur, curOff, err := c.currentVersion(id)
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
	// The index keys of the version this write replaces, for the flush
	// (memEntry.base); computed outside mu. The index set cannot change:
	// Database.mu is held shared.
	c.apply(id, cur != nil, c.indexKeysAll(cur), curOff, next, off, lb, size)
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

// apply makes a durable write visible (§5.2 step 5). existed reports
// whether the write replaced a live version; base and baseOff are that
// version's index keys and WAL offset (nil and 0 if there was none); next
// is the after-image (nil for a delete); size is the entry's approximate
// size for the MemtableSize flush trigger. Memory only.
func (c *Collection) apply(id string, existed bool, base indexBase, baseOff uint64, next *Document, off, lb uint64, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeInflightLocked(lb)
	c.applyLocked(id, existed, base, baseOff, next, off, size)
}

// applyLocked is apply without the inflight bookkeeping, shared with WAL
// replay. existed reports whether a live version of id existed before the
// write (decided from the overlay first, then the key table). base and
// baseOff describe the version the write replaced (see memEntry); they
// are used only when id has no memtable entry yet, since an existing entry
// keeps the base it captured first. Callers hold mu exclusively.
func (c *Collection) applyLocked(id string, existed bool, base indexBase, baseOff uint64, next *Document, off uint64, size int64) {
	me, ok := c.memtable[id]
	if ok && me.walOffset >= off {
		return // never happens under the doc lock; replay relies on the same rule
	}
	delta := size
	if ok {
		c.memBytes -= me.size
		delta -= me.size
		base, baseOff = me.base, me.baseOff
	}
	c.db.e.pending.add(delta)
	// memMin stays a lower bound of the memtable's offsets: an overwritten
	// entry may leave it lower than the true minimum, which only makes the
	// low-water mark more conservative.
	if len(c.memtable) == 0 || off < c.memMin {
		c.memMin = off
	}
	c.memtable[id] = memEntry{doc: next, walOffset: off, size: size, base: base, baseOff: baseOff}
	c.memBytes += size
	if c.memBytes > c.db.e.opts.MemtableSize {
		c.db.e.kickFlush()
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
// memtable → flushing → key table → cache or data segment. doc is nil
// when the document does not exist. When owned is false, doc is shared
// with the memtable or the cache and must be neither modified nor handed
// out without a Clone; otherwise it is a private copy. (Every path
// currently returns shared documents, so callers clone only what they
// hand out.)
func (c *Collection) current(id string) (doc *Document, owned bool, err error) {
	doc, _, err = c.currentVersion(id)
	return doc, false, err
}

// currentVersion is current that also returns the WAL offset of the
// version it found (0 when the document does not exist). The document is
// shared, as current's with owned false.
func (c *Collection) currentVersion(id string) (*Document, uint64, error) {
	for attempt := 0; ; attempt++ {
		// The overlay first, under mu; the read transaction only after it
		// (the snapshot rule above).
		c.mu.RLock()
		if c.dropped {
			c.mu.RUnlock()
			return nil, 0, c.errDropped()
		}
		me, ok := c.memtable[id]
		if !ok {
			me, ok = c.flushing[id]
		}
		c.mu.RUnlock()
		if ok {
			if me.doc == nil {
				return nil, 0, nil // deleted
			}
			return me.doc, me.walOffset, nil
		}

		ke, ok, err := c.storedEntry(id)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return nil, 0, nil
		}
		doc, err := c.readStored(id, ke)
		if errors.Is(err, errSegmentRetired) && attempt < maxReadRetries {
			continue // compaction moved it; look it up again
		}
		if err != nil {
			return nil, 0, err
		}
		return doc, ke.WALOffset, nil
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

// readStored reads the version of id described by the key-table entry ke,
// from the cache or else from its data segment (and caches it). The
// returned document is shared with the cache: it must be neither modified
// nor handed out without a Clone.
func (c *Collection) readStored(id string, ke idxstore.Entry) (*Document, error) {
	lru, key := c.db.e.cache, c.cacheKey(id)
	if v, ok := lru.Get(key, ke.WALOffset); ok {
		return v.doc, nil
	}

	r, err := c.acquireReader(ke.Seg)
	if err != nil {
		return nil, err
	}
	rec, err := r.Read(entryLocation(ke))
	r.Release()
	if err != nil {
		return nil, fmt.Errorf("reading document %q of collection %q: %w", id, c.name, err)
	}
	if rec.Type != segment.TypePut || rec.ID != id || rec.WALOffset != ke.WALOffset {
		return nil, fmt.Errorf("reading document %q of collection %q: %w: record at segment %d byte %d is %s %q@%d, want put@%d",
			id, c.name, segment.ErrCorrupt, ke.Seg, ke.Pos, rec.Type, rec.ID, rec.WALOffset, ke.WALOffset)
	}
	doc, err := decodeDocument(id, rec.Doc)
	if err != nil {
		return nil, err
	}
	lru.Put(key, ke.WALOffset, cachedDoc{doc: doc, size: cachedDocSize(len(rec.Doc))})
	return doc, nil
}

// entryLocation returns the data-segment location a key-table entry
// points at.
func entryLocation(ke idxstore.Entry) segment.Location {
	return segment.Location{Seg: ke.Seg, Pos: ke.Pos, Size: ke.Size}
}

// locationEntry returns the key-table entry for the record at loc written
// by the change with WAL offset off.
func locationEntry(loc segment.Location, off uint64) idxstore.Entry {
	return idxstore.Entry{Seg: loc.Seg, Pos: loc.Pos, Size: loc.Size, WALOffset: off}
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

// captureOverlay returns the overlay M of the snapshot rule (see the top
// of this file): every id with a change that is not in the index file yet,
// with its document (nil = deleted; the memtable wins over flushing), and
// the index file to read after it. The documents are shared with the
// memtable and immutable. A read transaction started on the returned store
// after this call sees every change that is not in M.
func (c *Collection) captureOverlay() (map[string]*Document, *idxstore.Store, error) {
	c.mu.RLock()
	st, err := c.storeLocked()
	var overlay map[string]*Document
	if err == nil {
		overlay = c.overlayLocked()
	}
	c.mu.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	if st == nil {
		// A renamed collection opening its file on first use: still after
		// the overlay was captured.
		if st, err = c.openStoreOnUse(); err != nil {
			return nil, nil, err
		}
	}
	return overlay, st, nil
}

// overlayLocked returns the overlay: every id of the memtable and
// flushing with its document (nil = deleted; the memtable wins), or nil
// when both are empty. Callers hold mu.
func (c *Collection) overlayLocked() map[string]*Document {
	n := len(c.memtable) + len(c.flushing)
	if n == 0 {
		return nil
	}
	overlay := make(map[string]*Document, n)
	for id, me := range c.flushing {
		overlay[id] = me.doc
	}
	for id, me := range c.memtable {
		overlay[id] = me.doc
	}
	return overlay
}

// liveIDs returns every live id in ascending order: an ordered walk of
// the key table merged with the overlay, whose entries override the key
// table's (a deleted id is left out, a new one added in its place).
// Callers hold no collection lock.
func (c *Collection) liveIDs() ([]string, error) {
	overlay, st, err := c.captureOverlay()
	if err != nil {
		return nil, err
	}
	added := make([]string, 0, len(overlay)) // the overlay's live ids, ascending
	for id, doc := range overlay {
		if doc != nil {
			added = append(added, id)
		}
	}
	slices.Sort(added)

	var ids []string
	err = st.View(func(tx *idxstore.Tx) error {
		n, err := tx.Count()
		if err != nil {
			return err
		}
		ids = make([]string, 0, int(n)+len(added))
		next := 0 // added[next:] is not merged yet
		cur := tx.KeyCursor()
		for k, _, ok := cur.First(); ok; k, _, ok = cur.Next() {
			for next < len(added) && added[next] < string(k) {
				ids = append(ids, added[next])
				next++
			}
			if _, changed := overlay[string(k)]; changed {
				continue // the overlay decides: merged from added, or deleted
			}
			ids = append(ids, string(k))
		}
		ids = append(ids, added[next:]...)
		return cur.Err()
	})
	if err != nil {
		return nil, c.storeErr(err)
	}
	return ids, nil
}
