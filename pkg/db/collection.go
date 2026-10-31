package db

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
}

// Collection is a set of documents with an optional schema and secondary
// indexes. It stays valid until it (or its database) is dropped.
type Collection struct {
	db     *Database
	name   string
	dir    string
	schema *Schema // immutable after creation

	// mu guards the in-memory structure below; it is held only for
	// memory-only critical sections, never across I/O (§11). Fields also
	// written by metadata operations (indexes, metaOffset, dropped) are
	// only changed while Database.mu is held exclusively, so holding that
	// lock is enough to read them.
	mu         sync.RWMutex
	keydir     map[string]keyEntry // id -> live on-disk version
	memtable   map[string]memEntry // durable-in-WAL, not yet flushed
	flushing   map[string]memEntry // immutable memtable being flushed; nil when idle
	indexes    map[string]indexDef // name -> definition; index data structures arrive with the query/index step
	inflight   []uint64            // WAL offsets durable but not yet applied (min-heap; document write path)
	metaOffset uint64              // coll.meta wal_offset
	dropped    bool

	// docLocks serializes read-modify-write of one document; stripe chosen
	// by FNV-1a of the id (document write path).
	docLocks [numDocLocks]sync.Mutex

	count atomic.Int64 // live documents, kept in sync with keydir and memtable
}

func newCollection(d *Database, name string, s *Schema, metaOffset uint64) *Collection {
	return &Collection{
		db:         d,
		name:       name,
		dir:        collDir(d.e.root, d.name, name),
		schema:     s,
		keydir:     make(map[string]keyEntry),
		memtable:   make(map[string]memEntry),
		indexes:    make(map[string]indexDef),
		metaOffset: metaOffset,
	}
}

// Name returns the collection name.
func (c *Collection) Name() string { return c.name }

// markDropped flags the collection as dropped so background work and
// late callers stop using it. Callers hold Database.mu exclusively.
func (c *Collection) markDropped() {
	c.mu.Lock()
	c.dropped = true
	c.mu.Unlock()
}

func (c *Collection) errDropped() error {
	return fmt.Errorf("collection %q in database %q: %w", c.name, c.db.name, ErrNotFound)
}

// Insert adds doc and returns its id (generated when doc.ID is empty).
func (c *Collection) Insert(ctx context.Context, doc *Document) (string, error) {
	return "", fmt.Errorf("Collection.Insert: %w", errNotImplemented)
}

// Get returns a copy of the document id.
func (c *Collection) Get(ctx context.Context, id string) (*Document, error) {
	return nil, fmt.Errorf("Collection.Get: %w", errNotImplemented)
}

// Find returns copies of the documents matching q, sorted by id.
func (c *Collection) Find(ctx context.Context, q *Query) ([]*Document, error) {
	return nil, fmt.Errorf("Collection.Find: %w", errNotImplemented)
}

// Update merges set into the document id and returns the new version.
func (c *Collection) Update(ctx context.Context, id string, set map[string]any) (*Document, error) {
	return nil, fmt.Errorf("Collection.Update: %w", errNotImplemented)
}

// Delete removes the document id.
func (c *Collection) Delete(ctx context.Context, id string) error {
	return fmt.Errorf("Collection.Delete: %w", errNotImplemented)
}

// Count returns the number of live documents in O(1).
func (c *Collection) Count() int { return int(c.count.Load()) }

// CreateIndex defines the secondary index name on field. It fails with
// ErrExists if an index with that name exists. It holds the database's
// metadata lock, so writes to the database wait while it runs.
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

	def := indexDef{Name: name, Field: field}
	payload, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("creating index %q: encoding payload: %w", name, err)
	}
	off, err := e.appendWAL(ctx, wal.Record{Op: wal.OpCreateIndex, DB: d.name, Coll: c.name, Payload: payload})
	if err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	defs := maps.Clone(c.indexes)
	defs[name] = def
	if err := c.writeMeta(defs, off); err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}

	c.mu.Lock()
	c.indexes = defs
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
	off, err := e.appendWAL(ctx, wal.Record{Op: wal.OpDropIndex, DB: d.name, Coll: c.name, Payload: payload})
	if err != nil {
		return fmt.Errorf("dropping index %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	defs := maps.Clone(c.indexes)
	delete(defs, name)
	if err := c.writeMeta(defs, off); err != nil {
		return fmt.Errorf("dropping index %q: %w", name, err)
	}

	c.mu.Lock()
	c.indexes = defs
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
	return writeCollMeta(c.dir, collMeta{
		Name:      c.name,
		Schema:    c.schema,
		Indexes:   sortedIndexDefs(defs),
		WALOffset: off,
	})
}
