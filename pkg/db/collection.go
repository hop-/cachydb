package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// numDocLocks is the number of striped per-document locks per collection.
const numDocLocks = 256

// memEntry is a change that is durable in the WAL but not yet flushed to a
// data segment (§5.1). A nil doc is a tombstone.
//
// base and baseOff describe the version of the document in the index file
// that the flush of this entry replaces (approach-index-and-query §4, "old
// index keys without a disk read"): baseOff is its WAL offset (0 when there
// is no live version), base its keys per index name. They are captured on
// the write path, which has that version in hand, and a later write to the
// same id keeps them. An index missing from base (created or dropped since
// the capture, see forgetBaseLocked), or a baseOff that no longer matches
// the key table when the flush runs, makes the flush read the version from
// its data segment instead.
type memEntry struct {
	doc       *Document
	walOffset uint64
	size      int64 // approximate encoded size, for the MemtableSize flush trigger
	base      indexBase
	baseOff   uint64
}

// Collection is a set of documents with an optional schema and secondary
// indexes. It stays valid until it (or its database) is dropped.
type Collection struct {
	db     *Database
	name   string
	dir    string
	schema *Schema // replaced only while Database.mu is held exclusively (UpdateSchema)
	// pendingRename is set, during recovery only, on a collection whose
	// directory a rename already moved to this name while its coll.meta
	// still has the old one (a crash between the two, plan 004 §3.4). The
	// collection is registered under the old name until the rename
	// record is replayed.
	pendingRename string

	// mu guards the in-memory structure below; it is held only for
	// memory-only critical sections, never across I/O (§11). Fields also
	// written by metadata operations (indexes, metaOffset, dropped) are
	// only changed while Database.mu is held exclusively, so holding that
	// lock is enough to read them.
	mu sync.RWMutex
	// store is the collection's open index file (index.db): the key table
	// (id -> location of the live flushed version), the live count, the
	// per-segment byte accounting, and the checkpoint. Only the flusher
	// and compaction change it, each in one transaction. It is nil while
	// the file is not open: before load, once the collection is dropped,
	// detached, or closed, and on a renamed collection until its first
	// use (indexStore opens it then). The pointer is read under mu and
	// used after mu is released: mu is never held across a transaction.
	store    *idxstore.Store
	memtable map[string]memEntry // durable-in-WAL, not yet flushed
	flushing map[string]memEntry // immutable memtable being flushed; nil when idle
	// indexes holds the definitions of the secondary indexes, whose
	// entries are in the index file (index.go). It changes only while
	// Database.mu is held exclusively, together with flushMu and mu.
	indexes map[string]indexDef
	// inflight holds, in ascending order, a lower bound of the WAL offset
	// of every document write that is between "registered before the WAL
	// append" and "applied to the memtable". Together with the memtable it
	// keeps the low-water mark (§7.1) from passing a durable record that
	// is not yet visible in memory. See registerInflight.
	inflight      []uint64
	readers       map[uint32]*segment.Reader // open data segments, opened on first read
	readersClosed bool                       // engine shutdown: no new readers, and the index file stays closed
	// retired holds the data segments compaction replaced (§9). Their
	// files are removed (once no read holds them), and no reader may be
	// opened for them again.
	retired map[uint32]struct{}
	// keptTombs holds, per compaction output of this process, the bytes of
	// the tombstones the compaction had to copy into it. The index file
	// counts them as dead, like every tombstone; compaction planning does
	// not (compactionAccounting). It is not persisted: after a restart
	// they count as dead again, until the next compaction of the segment.
	keptTombs  map[uint32]int64
	metaOffset uint64 // coll.meta wal_offset
	created    uint64 // coll.meta created_offset (immutable; 0 = unknown)
	dropped    bool

	// Flush bookkeeping (§5.4, §7.1), guarded by mu.
	memBytes      int64  // sum of memtable entry sizes
	flushingBytes int64  // sum of flushing entry sizes (counted in Engine.pending until the flush completes)
	memMin        uint64 // <= smallest walOffset in memtable (meaningful when non-empty)
	flushingMin   uint64 // <= smallest walOffset in flushing (meaningful when non-nil)
	// checkpoint is the checkpoint stored in the index file, as of its
	// last commit: every change of this collection with a WAL offset at or
	// below it is in the index file and the data segments.
	checkpoint uint64

	// storeMu serializes opening the index file on first use (indexStore)
	// with the operations that close it. Lock order: storeMu → mu.
	storeMu sync.Mutex

	// flushMu serializes flushes of this collection and guards the active
	// segment fields below. Drops hold it while renaming the collection
	// directory, so the flusher never touches a dropped collection's files
	// by path. Lock order: Database.mu → flushMu → mu.
	flushMu      sync.Mutex
	active       segmentWriter // open active data segment; nil when not open
	activeSeg    uint32        // id of the active segment; 0 = no segment yet
	activeSealed bool          // activeSeg is sealed; the next flush creates segment nextSeg
	// nextSeg is the id of the next data segment to create (max
	// registered + 1): the active segment and compaction outputs both
	// take ids from it, so ids are never reused and always grow.
	nextSeg uint32

	// compactMu serializes compactions of this collection with each other
	// and with drops: a compaction holds it for its whole run, including
	// its file I/O outside flushMu, so drops take it before flushMu to
	// keep compaction away from a directory being renamed. Lock order:
	// Database.mu → compactMu → flushMu → mu.
	compactMu sync.Mutex

	// docLocks serializes read-modify-write of one document; stripe chosen
	// by FNV-1a of the id (see docLock).
	docLocks [numDocLocks]sync.Mutex

	// count is the number of live documents: the count stored in the
	// index file plus the net effect of the changes not flushed yet. A
	// flush moves changes from the second part to the first, so it does
	// not touch the counter.
	count atomic.Int64
}

func newCollection(d *Database, name string, s *Schema, created, metaOffset uint64) *Collection {
	return &Collection{
		db:         d,
		name:       name,
		dir:        collDir(d.e.root, d.name, name),
		schema:     s,
		memtable:   make(map[string]memEntry),
		indexes:    make(map[string]indexDef),
		readers:    make(map[uint32]*segment.Reader),
		retired:    make(map[uint32]struct{}),
		keptTombs:  make(map[uint32]int64),
		nextSeg:    1,
		metaOffset: metaOffset,
		created:    created,
	}
}

// Name returns the collection name.
func (c *Collection) Name() string { return c.name }

// IndexInfo describes a secondary index.
type IndexInfo struct {
	Name  string `json:"name"`
	Field string `json:"field"`
}

// Schema returns a copy of the collection's schema, or nil for a
// schemaless collection.
func (c *Collection) Schema() *Schema {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	return cloneSchema(c.schema)
}

// Indexes returns the collection's secondary indexes sorted by name.
func (c *Collection) Indexes() []IndexInfo {
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	defs := sortedIndexDefs(c.indexDefs())
	out := make([]IndexInfo, len(defs))
	for i, d := range defs {
		out[i] = IndexInfo(d)
	}
	return out
}

// maxSchemaFailures is how many failing documents an UpdateSchema error
// lists.
const maxSchemaFailures = 10

// UpdateSchema replaces the collection's schema with s (nil removes it;
// plan 004 §3.3). Every existing document is validated against s first;
// if any fails, it returns an error wrapping ErrSchemaValidation that
// lists up to 10 failing ids with their first violation and the total,
// and nothing changes. It holds the database's metadata lock while it
// validates and commits, so writes to the database wait (no write can
// slip in between). Later writes are validated against s.
func (c *Collection) UpdateSchema(ctx context.Context, s *Schema) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("%w: updating schema of collection %q: %w", ErrSchemaValidation, c.name, err)
	}
	s = cloneSchema(s)
	d, e := c.db, c.db.e
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()

	d.mu.Lock()
	defer d.mu.Unlock()
	if c.dropped {
		return c.errDropped()
	}
	if err := c.validateAll(ctx, s); err != nil {
		return err
	}

	payload, err := json.Marshal(updateSchemaPayload{Schema: s})
	if err != nil {
		return fmt.Errorf("updating schema of collection %q: encoding payload: %w", c.name, err)
	}
	off, done, err := e.appendMetaWAL(ctx, wal.Record{Op: wal.OpUpdateSchema, DB: d.name, Coll: c.name, Payload: payload})
	if err != nil {
		return fmt.Errorf("updating schema of collection %q: %w", c.name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	if err := c.writeMetaWith(s, c.indexDefs(), off); err != nil {
		return fmt.Errorf("updating schema of collection %q: %w", c.name, err)
	}
	done()

	c.mu.Lock()
	c.schema = s
	c.metaOffset = off
	c.mu.Unlock()
	return nil
}

// validateAll checks every live document against s. Callers hold
// Database.mu exclusively, so no document write runs.
func (c *Collection) validateAll(ctx context.Context, s *Schema) error {
	if s == nil {
		return nil
	}
	ids, err := c.liveIDs()
	if err != nil {
		return err
	}
	var (
		failed []string
		total  int
	)
	for i, id := range ids {
		if i%findCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		doc, _, err := c.current(id)
		if err != nil {
			return err
		}
		if doc == nil {
			continue
		}
		err = s.ValidateDocument(doc)
		var se *SchemaError
		if !errors.As(err, &se) {
			continue
		}
		total++
		if len(failed) < maxSchemaFailures {
			failed = append(failed, fmt.Sprintf("%s (%s)", id, se.Violations[0]))
		}
	}
	if total == 0 {
		return nil
	}
	more := ""
	if total > len(failed) {
		more = fmt.Sprintf(", and %d more", total-len(failed))
	}
	return fmt.Errorf("%w: updating schema of collection %q: %d existing document(s) do not match the new schema: %s%s",
		ErrSchemaValidation, c.name, total, strings.Join(failed, ", "), more)
}

// markDropped flags the collection as dropped so background work and
// late callers stop using it, closes its index file, active segment, and
// segment readers, and evicts its documents from the cache. It runs before
// the collection directory is renamed away. Callers hold Database.mu
// exclusively and the collection's flushMu.
func (c *Collection) markDropped() {
	c.storeMu.Lock()
	c.mu.Lock()
	c.dropped = true
	c.releasePendingLocked()
	st := c.store
	c.store = nil
	readers := c.readers
	c.readers = make(map[uint32]*segment.Reader)
	c.mu.Unlock()
	c.storeMu.Unlock()
	if st != nil {
		// Best effort, as below. Close waits for the read transactions
		// still open on the file.
		_ = st.Close()
	}
	if c.active != nil {
		// Best effort: the directory is about to be removed, so nothing
		// written to it matters any more.
		_ = c.active.Close()
		c.active = nil
	}
	for _, r := range readers {
		// Best effort: the files are read-only and the directory is about
		// to be removed; a reader still in use closes on its last Release.
		_ = r.Close()
	}
	c.db.e.cache.InvalidateCollection(c.db.name, c.name)
}

// closeFiles closes the active segment writer, every segment reader, and
// the index file without flushing (cleanup after a failed Open).
func (c *Collection) closeFiles() error {
	c.flushMu.Lock()
	var errs []error
	if c.active != nil {
		if err := c.active.Close(); err != nil {
			errs = append(errs, err)
		}
		c.active = nil
	}
	c.flushMu.Unlock()
	if err := c.closeReaders(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// closeReaders closes every open segment reader and the index file (engine
// shutdown). Neither is opened again.
func (c *Collection) closeReaders() error {
	c.storeMu.Lock()
	c.mu.Lock()
	st := c.store
	c.store = nil
	readers := c.readers
	c.readers = make(map[uint32]*segment.Reader)
	c.readersClosed = true
	c.mu.Unlock()
	c.storeMu.Unlock()
	var errs []error
	if st != nil {
		if err := st.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, r := range readers {
		if err := r.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// indexStore returns the collection's open index file. A renamed
// collection opens its file here on first use (its handle was closed
// before the directory was renamed). It fails with ErrNotFound for a
// dropped collection and with ErrClosed after engine shutdown.
func (c *Collection) indexStore() (*idxstore.Store, error) {
	c.mu.RLock()
	st, err := c.storeLocked()
	c.mu.RUnlock()
	if st != nil || err != nil {
		return st, err
	}
	return c.openStoreOnUse()
}

// storeLocked returns the open index file, or nil when it has to be opened
// first (openStoreOnUse). It fails for a dropped collection and after
// engine shutdown. Callers hold mu.
func (c *Collection) storeLocked() (*idxstore.Store, error) {
	switch {
	case c.dropped:
		return nil, c.errDropped()
	case c.readersClosed:
		return nil, ErrClosed
	}
	return c.store, nil
}

// openStoreOnUse opens the index file of a collection that has none open
// (see indexStore). Callers hold neither mu nor storeMu.
func (c *Collection) openStoreOnUse() (*idxstore.Store, error) {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	c.mu.RLock()
	st, err := c.storeLocked()
	c.mu.RUnlock()
	if st != nil || err != nil {
		return st, err
	}
	// The drop, rename, and shutdown paths take storeMu before they flag
	// the collection, so none of them runs while the file is being opened.
	st, err = idxstore.Open(c.dir, idxstore.Options{})
	if err != nil {
		return nil, fmt.Errorf("collection %q in database %q: %w", c.name, c.db.name, err)
	}
	c.mu.Lock()
	c.store = st
	c.mu.Unlock()
	return st, nil
}

// storeErr maps the error of an index-file operation. A handle that a
// drop, a rename, or Close closed after the caller took it reports
// idxstore.ErrClosed; the caller gets the error it would have got had it
// started a moment later.
func (c *Collection) storeErr(err error) error {
	if !errors.Is(err, idxstore.ErrClosed) {
		return err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dropped {
		return c.errDropped()
	}
	return ErrClosed
}

// storedEntry returns the key-table entry of id: where its live flushed
// version is. ok is false when the index file has no such id. It runs in
// a read transaction of its own; callers hold no collection lock.
func (c *Collection) storedEntry(id string) (ke idxstore.Entry, ok bool, err error) {
	st, err := c.indexStore()
	if err != nil {
		return idxstore.Entry{}, false, err
	}
	if ke, ok, err = st.Key(id); err != nil {
		return idxstore.Entry{}, false, c.storeErr(err)
	}
	return ke, ok, nil
}

// indexDefs returns the definitions of the collection's indexes. Callers
// hold Database.mu (the index set changes only under it exclusively).
func (c *Collection) indexDefs() map[string]indexDef {
	return maps.Clone(c.indexes)
}

func (c *Collection) errDropped() error {
	return fmt.Errorf("collection %q in database %q: %w", c.name, c.db.name, ErrNotFound)
}

// CreateIndex creates the secondary index name on field and builds it
// from the documents already in the collection. It fails with ErrExists if
// an index with that name exists. It holds the database's metadata lock
// for the whole build, so writes to the database wait while it runs
// (known limitation: index creation is rare).
//
// The entries are built into the index file, in batches, from the
// documents the key table lists, before the commit point; the collection's
// flushes and compactions wait meanwhile, so the new index matches the
// key table when it is published. Unflushed changes are found by the next
// flush, which reads the replaced versions' keys from the data segments
// (forgetBaseLocked).
func (c *Collection) CreateIndex(ctx context.Context, name, field string) error {
	if err := ValidateName("index", name); err != nil {
		return err
	}
	if field == "" {
		return fmt.Errorf("creating index %q: field must not be empty", name)
	}
	d, e := c.db, c.db.e
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()

	d.mu.Lock()
	defer d.mu.Unlock()
	if c.dropped {
		return c.errDropped()
	}
	if _, ok := c.indexes[name]; ok {
		return fmt.Errorf("index %q on collection %q: %w", name, c.name, ErrExists)
	}

	// Lock order: Database.mu → compactMu → flushMu → mu. Document writes
	// are blocked by Database.mu, flushes and compactions by these two, so
	// the key table and the documents stay as they are until the index is
	// published below.
	c.compactMu.Lock()
	defer c.compactMu.Unlock()
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	st, err := c.indexStore()
	if err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}

	// Build before the commit point: an error aborts the operation, and
	// entries without a definition are dropped (here, or by the next Open).
	def := indexDef{Name: name, Field: field}
	if err := c.buildIndexes(ctx, st, []indexDef{def}); err != nil {
		c.dropIndexEntries(st, name)
		return fmt.Errorf("creating index %q: %w", name, c.storeErr(err))
	}

	payload, err := json.Marshal(def)
	if err != nil {
		c.dropIndexEntries(st, name)
		return fmt.Errorf("creating index %q: encoding payload: %w", name, err)
	}
	off, done, err := e.appendMetaWAL(ctx, wal.Record{Op: wal.OpCreateIndex, DB: d.name, Coll: c.name, Payload: payload})
	if err != nil {
		c.dropIndexEntries(st, name)
		return fmt.Errorf("creating index %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	defs := c.indexDefs()
	defs[name] = def
	if err := c.writeMeta(defs, off); err != nil {
		// The record stays pinned in the WAL (done is not called), so a
		// restart replays it and builds the index again.
		c.dropIndexEntries(st, name)
		return fmt.Errorf("creating index %q: %w", name, err)
	}
	done()

	c.mu.Lock()
	c.indexes[name] = def
	c.forgetBaseLocked(name)
	c.metaOffset = off
	c.mu.Unlock()
	return nil
}

// dropIndexEntries removes the entries of index name from the index file,
// best effort: entries without a definition are also dropped by the next
// Open (syncIndexes). Callers hold flushMu.
func (c *Collection) dropIndexEntries(st *idxstore.Store, name string) {
	err := st.Update(func(u *idxstore.WTx) error { return u.DropIndex(name) })
	if err != nil && !errors.Is(err, idxstore.ErrNoIndex) {
		c.db.e.logger.Warn("cachydb: removing the entries of an index from the index file; the next open removes them",
			"database", c.db.name, "collection", c.name, "index", name, "err", c.storeErr(err))
	}
}

// DropIndex removes the secondary index name (ErrNotFound if absent).
func (c *Collection) DropIndex(ctx context.Context, name string) error {
	if err := ValidateName("index", name); err != nil {
		return err
	}
	d, e := c.db, c.db.e
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()

	d.mu.Lock()
	defer d.mu.Unlock()
	if c.dropped {
		return c.errDropped()
	}
	if _, ok := c.indexes[name]; !ok {
		return fmt.Errorf("index %q on collection %q: %w", name, c.name, ErrNotFound)
	}

	// No flush runs while the index set and the bucket change (lock
	// order: Database.mu → flushMu → mu): a flush uses one index set from
	// start to end.
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	payload, err := json.Marshal(dropIndexPayload{Name: name})
	if err != nil {
		return fmt.Errorf("dropping index %q: encoding payload: %w", name, err)
	}
	off, done, err := e.appendMetaWAL(ctx, wal.Record{Op: wal.OpDropIndex, DB: d.name, Coll: c.name, Payload: payload})
	if err != nil {
		return fmt.Errorf("dropping index %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	defs := c.indexDefs()
	delete(defs, name)
	if err := c.writeMeta(defs, off); err != nil {
		return fmt.Errorf("dropping index %q: %w", name, err)
	}
	done()

	c.mu.Lock()
	delete(c.indexes, name)
	c.forgetBaseLocked(name)
	c.metaOffset = off
	c.mu.Unlock()

	// The definition is gone, so nothing reads or maintains the entries
	// any more; if removing them fails, the next Open does it.
	if st, err := c.indexStore(); err == nil {
		c.dropIndexEntries(st, name)
	}
	return nil
}

// writeMeta atomically rewrites coll.meta with the given index definitions
// and WAL offset. Callers hold Database.mu exclusively.
func (c *Collection) writeMeta(defs map[string]indexDef, off uint64) error {
	return c.writeMetaWith(c.schema, defs, off)
}

// writeMetaWith is writeMeta with schema s.
func (c *Collection) writeMetaWith(s *Schema, defs map[string]indexDef, off uint64) error {
	return writeCollMeta(c.dir, collMeta{
		Name:          c.name,
		Schema:        s,
		Indexes:       sortedIndexDefs(defs),
		WALOffset:     off,
		CreatedOffset: c.created,
	})
}

// detach makes the collection unusable under its current name before its
// directory is renamed (plan 004 §3.4): it flags it dropped, closes its
// index file, active segment, and segment readers, and evicts its
// documents from the cache. Unlike markDropped it keeps the in-memory
// state, which moveState hands to the renamed collection. Callers hold
// Database.mu exclusively and flushMu, or run during recovery.
func (c *Collection) detach() error {
	c.storeMu.Lock()
	c.mu.Lock()
	c.dropped = true
	st := c.store
	c.store = nil
	readers := c.readers
	c.readers = make(map[uint32]*segment.Reader)
	c.mu.Unlock()
	c.storeMu.Unlock()
	var errs []error
	if st != nil {
		// Close waits for the read transactions still open on the file.
		if err := st.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.active != nil {
		if err := c.active.Close(); err != nil {
			errs = append(errs, err)
		}
		c.active = nil
	}
	for _, r := range readers {
		// A reader still in use closes on its last Release.
		if err := r.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	c.db.e.cache.InvalidateCollection(c.db.name, c.name)
	return errors.Join(errs...)
}

// moveState returns a new collection called name, in directory dir, that
// takes over c's in-memory state (memtable, index definitions, segment bookkeeping,
// checkpoint, count); c keeps empty structures. The index file, which
// detach closed, is opened again by the new collection's first use
// (indexStore), and the active segment by its next flush. Callers hold
// Database.mu exclusively and c's flushMu (or run during recovery), and
// have detached c.
func (c *Collection) moveState(name, dir string, metaOffset uint64) *Collection {
	nc := newCollection(c.db, name, c.schema, c.created, metaOffset)
	nc.dir = dir
	c.mu.Lock()
	defer c.mu.Unlock()
	nc.memtable, c.memtable = c.memtable, make(map[string]memEntry)
	nc.flushing, c.flushing = c.flushing, nil
	nc.indexes, c.indexes = c.indexes, make(map[string]indexDef)
	nc.inflight, c.inflight = c.inflight, nil
	nc.retired, c.retired = c.retired, make(map[uint32]struct{})
	nc.keptTombs, c.keptTombs = c.keptTombs, make(map[uint32]int64)
	nc.memBytes, nc.flushingBytes = c.memBytes, c.flushingBytes
	c.memBytes, c.flushingBytes = 0, 0
	nc.memMin, nc.flushingMin, nc.checkpoint = c.memMin, c.flushingMin, c.checkpoint
	nc.activeSeg, nc.activeSealed = c.activeSeg, c.activeSealed
	nc.nextSeg = c.nextSeg
	nc.count.Store(c.count.Load())
	c.count.Store(0)
	return nc
}
