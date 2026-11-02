package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// numDocLocks is the number of striped per-document locks per collection.
const numDocLocks = 256

// keyEntry is the keydir's record of where the live version of a document
// is stored in the data segments (§4.3).
type keyEntry struct {
	loc       segment.Location
	walOffset uint64
}

// memEntry is a change that is durable in the WAL but not yet flushed to a
// data segment (§5.1). A nil doc is a tombstone.
type memEntry struct {
	doc       *Document
	walOffset uint64
	size      int64 // approximate encoded size, for the MemtableSize flush trigger
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
	mu       sync.RWMutex
	keydir   map[string]keyEntry // id -> live on-disk version
	memtable map[string]memEntry // durable-in-WAL, not yet flushed
	flushing map[string]memEntry // immutable memtable being flushed; nil when idle
	indexes  map[string]*index   // name -> secondary index; contents change only under mu
	// inflight holds, in ascending order, a lower bound of the WAL offset
	// of every document write that is between "registered before the WAL
	// append" and "applied to the memtable". Together with the memtable it
	// keeps the low-water mark (§7.1) from passing a durable record that
	// is not yet visible in memory. See registerInflight.
	inflight      []uint64
	readers       map[uint32]*segment.Reader // open data segments, opened on first read
	readersClosed bool                       // engine shutdown: no new readers
	// retired holds the data segments compaction replaced (§9). Their
	// files are removed (once no read holds them), and no reader may be
	// opened for them again.
	retired    map[uint32]struct{}
	metaOffset uint64 // coll.meta wal_offset
	created    uint64 // coll.meta created_offset (immutable; 0 = unknown)
	// replayTombs holds, during recovery only, the WAL offsets of the
	// tombstones in the data segments above the checkpoint whose id is not
	// in the keydir, so replay does not re-apply deletes that are already
	// durable. nil outside recovery.
	replayTombs map[string]uint64
	dropped     bool

	// Flush bookkeeping (§5.4, §7.1), guarded by mu.
	memBytes      int64  // sum of memtable entry sizes
	flushingBytes int64  // sum of flushing entry sizes (counted in Engine.pending until the flush completes)
	memMin        uint64 // <= smallest walOffset in memtable (meaningful when non-empty)
	flushingMin   uint64 // <= smallest walOffset in flushing (meaningful when non-nil)
	checkpoint    uint64 // value of the last checkpoint marker durable in the data segments
	// deadBytes counts, per data segment, the bytes of records the keydir
	// no longer references: superseded and deleted versions, tombstones,
	// and checkpoint markers. Compaction (§9) uses it.
	deadBytes map[uint32]int64
	// segBytes holds the record bytes of every live data segment (the
	// active one included); its keys are the collection's live segments.
	segBytes map[uint32]int64

	// flushMu serializes flushes of this collection and guards the active
	// segment fields below. Drops hold it while renaming the collection
	// directory, so the flusher never touches a dropped collection's files
	// by path. Lock order: Database.mu → flushMu → mu.
	flushMu      sync.Mutex
	active       segmentWriter       // open active data segment; nil when not open
	activeSeg    uint32              // id of the active segment; 0 = no segment yet
	activeSealed bool                // activeSeg is sealed; the next flush creates segment nextSeg
	activeHint   []segment.HintEntry // records of the active segment, for its hint file
	// nextSeg is the id of the next data segment to create (max existing
	// + 1): the active segment and compaction outputs both take ids from
	// it, so ids are never reused and always grow.
	nextSeg uint32
	// markerSeg is the segment holding the newest checkpoint marker (0 if
	// none). Compaction never rewrites it, so the marker survives (§9).
	markerSeg uint32

	// compactMu serializes compactions of this collection with each other
	// and with drops: a compaction holds it for its whole run, including
	// its file I/O outside flushMu, so drops take it before flushMu to
	// keep compaction away from a directory being renamed. Lock order:
	// Database.mu → compactMu → flushMu → mu.
	compactMu sync.Mutex

	// docLocks serializes read-modify-write of one document; stripe chosen
	// by FNV-1a of the id (see docLock).
	docLocks [numDocLocks]sync.Mutex

	count atomic.Int64 // live documents, kept in sync with keydir and memtable
}

func newCollection(d *Database, name string, s *Schema, created, metaOffset uint64) *Collection {
	return &Collection{
		db:         d,
		name:       name,
		dir:        collDir(d.e.root, d.name, name),
		schema:     s,
		keydir:     make(map[string]keyEntry),
		memtable:   make(map[string]memEntry),
		indexes:    make(map[string]*index),
		readers:    make(map[uint32]*segment.Reader),
		retired:    make(map[uint32]struct{}),
		deadBytes:  make(map[uint32]int64),
		segBytes:   make(map[uint32]int64),
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
	c.mu.RLock()
	ids := c.liveIDsLocked()
	c.mu.RUnlock()
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
// late callers stop using it, closes its active segment and segment
// readers, and evicts its documents from the cache. Callers hold
// Database.mu exclusively and the collection's flushMu.
func (c *Collection) markDropped() {
	c.mu.Lock()
	c.dropped = true
	c.releasePendingLocked()
	readers := c.readers
	c.readers = make(map[uint32]*segment.Reader)
	c.mu.Unlock()
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

// closeFiles closes the active segment writer and every segment reader
// without flushing (cleanup after a failed Open).
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

// closeReaders closes every open segment reader (engine shutdown).
func (c *Collection) closeReaders() error {
	c.mu.Lock()
	readers := c.readers
	c.readers = make(map[uint32]*segment.Reader)
	c.readersClosed = true
	c.mu.Unlock()
	var errs []error
	for _, r := range readers {
		if err := r.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// indexDefs returns the definitions of the collection's indexes. Callers
// hold Database.mu (the index set changes only under it exclusively).
func (c *Collection) indexDefs() map[string]indexDef {
	defs := make(map[string]indexDef, len(c.indexes))
	for name, ix := range c.indexes {
		defs[name] = indexDef{Name: name, Field: ix.field}
	}
	return defs
}

func (c *Collection) errDropped() error {
	return fmt.Errorf("collection %q in database %q: %w", c.name, c.db.name, ErrNotFound)
}

// CreateIndex creates the secondary index name on field and builds it
// from the documents already in the collection. It fails with ErrExists if
// an index with that name exists. It holds the database's metadata lock
// for the whole build, so writes to the database wait while it runs
// (known limitation: index creation is rare).
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

	// Build before the commit point: a read error aborts the operation
	// with nothing written. Document writes are blocked by Database.mu, so
	// the index stays exact until it is published below.
	ix, err := c.buildIndex(ctx, name, field)
	if err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}

	def := indexDef{Name: name, Field: field}
	payload, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("creating index %q: encoding payload: %w", name, err)
	}
	off, done, err := e.appendMetaWAL(ctx, wal.Record{Op: wal.OpCreateIndex, DB: d.name, Coll: c.name, Payload: payload})
	if err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	defs := c.indexDefs()
	defs[name] = def
	if err := c.writeMeta(defs, off); err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}
	done()

	c.mu.Lock()
	c.indexes[name] = ix
	c.metaOffset = off
	c.mu.Unlock()
	return nil
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
	c.metaOffset = off
	c.mu.Unlock()

	// The snapshot is only an accelerator; a stale one is ignored on open
	// because its index is no longer in coll.meta.
	if err := segment.RemoveIndexSnapshot(c.dir, name); err != nil {
		e.logger.Warn("cachydb: removing snapshot of dropped index", "collection", c.name, "index", name, "err", err)
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
// active segment and segment readers, and evicts its documents from the
// cache. Unlike markDropped it keeps the in-memory state, which
// moveState hands to the renamed collection. Callers hold Database.mu
// exclusively and flushMu, or run during recovery.
func (c *Collection) detach() error {
	c.mu.Lock()
	c.dropped = true
	readers := c.readers
	c.readers = make(map[uint32]*segment.Reader)
	c.mu.Unlock()
	var errs []error
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
// takes over c's in-memory state (keydir, memtable, indexes, segment
// bookkeeping, count); c keeps empty structures. The active segment is
// reopened lazily by the next flush. Callers hold Database.mu exclusively
// and c's flushMu (or run during recovery), and have detached c.
func (c *Collection) moveState(name, dir string, metaOffset uint64) *Collection {
	nc := newCollection(c.db, name, c.schema, c.created, metaOffset)
	nc.dir = dir
	c.mu.Lock()
	defer c.mu.Unlock()
	nc.keydir, c.keydir = c.keydir, make(map[string]keyEntry)
	nc.memtable, c.memtable = c.memtable, make(map[string]memEntry)
	nc.flushing, c.flushing = c.flushing, nil
	nc.indexes, c.indexes = c.indexes, make(map[string]*index)
	nc.inflight, c.inflight = c.inflight, nil
	nc.retired, c.retired = c.retired, make(map[uint32]struct{})
	nc.replayTombs, c.replayTombs = c.replayTombs, nil
	nc.memBytes, nc.flushingBytes = c.memBytes, c.flushingBytes
	c.memBytes, c.flushingBytes = 0, 0
	nc.memMin, nc.flushingMin, nc.checkpoint = c.memMin, c.flushingMin, c.checkpoint
	nc.deadBytes, c.deadBytes = c.deadBytes, make(map[uint32]int64)
	nc.segBytes, c.segBytes = c.segBytes, make(map[uint32]int64)
	nc.activeSeg, nc.activeSealed, nc.activeHint = c.activeSeg, c.activeSealed, c.activeHint
	nc.nextSeg, nc.markerSeg = c.nextSeg, c.markerSeg
	nc.count.Store(c.count.Load())
	c.count.Store(0)
	return nc
}
