package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Database is a named group of collections inside an Engine. It stays
// valid until it is dropped; methods on a dropped Database return
// ErrNotFound.
type Database struct {
	e    *Engine
	name string
	dir  string

	// mu guards the fields below. Metadata operations (create/drop
	// collection, create/drop index, schema-version changes, drop of the
	// database) hold it exclusively for their whole duration, including
	// WAL and file I/O; document writes hold it shared (task 09).
	mu            sync.RWMutex
	collections   map[string]*Collection
	schemaVersion int
	walOffset     uint64 // db.meta wal_offset
	dropped       bool
}

func newDatabase(e *Engine, name string, schemaVersion int, walOffset uint64) *Database {
	return &Database{
		e:             e,
		name:          name,
		dir:           dbDir(e.root, name),
		collections:   make(map[string]*Collection),
		schemaVersion: schemaVersion,
		walOffset:     walOffset,
	}
}

// Name returns the database name.
func (d *Database) Name() string { return d.name }

// SchemaVersion returns the database's schema version (see migrations).
func (d *Database) SchemaVersion() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.schemaVersion
}

// errDropped reports a database dropped while the caller held a reference.
func (d *Database) errDropped() error {
	return fmt.Errorf("database %q: %w", d.name, ErrNotFound)
}

// loadCollection loads one collection directory. It returns (nil, nil) for
// a directory without coll.meta (a create that did not reach its meta
// write).
func (d *Database) loadCollection(name string) (*Collection, error) {
	dir := collDir(d.e.root, d.name, name)
	var m collMeta
	if err := readMeta(filepath.Join(dir, collMetaFile), &m); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.e.logger.Warn("cachydb: skipping collection directory without coll.meta", "dir", dir)
			return nil, nil
		}
		return nil, err
	}
	if m.Name != name {
		return nil, fmt.Errorf("%s: name %q does not match directory %q", filepath.Join(dir, collMetaFile), m.Name, name)
	}
	c := newCollection(d, name, m.Schema, m.WALOffset)
	for _, def := range m.Indexes {
		c.indexes[def.Name] = newIndex(def.Name, def.Field)
	}
	if err := c.load(); err != nil {
		if cerr := c.closeFiles(); cerr != nil {
			d.e.logger.Warn("cachydb: closing segments after failed load", "dir", dir, "err", cerr)
		}
		return nil, err
	}
	return c, nil
}

// CreateCollection creates the collection name with an optional schema
// (nil = schemaless). It fails with ErrExists if the collection exists and
// with ErrSchemaValidation if the schema is invalid.
func (d *Database) CreateCollection(ctx context.Context, name string, s *Schema) error {
	if err := ValidateName("collection", name); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return fmt.Errorf("%w: collection %q: %w", ErrSchemaValidation, name, err)
	}
	s = cloneSchema(s)
	e := d.e
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.dropped {
		return d.errDropped()
	}
	if _, ok := d.collections[name]; ok {
		return fmt.Errorf("collection %q in database %q: %w", name, d.name, ErrExists)
	}

	payload, err := json.Marshal(createCollPayload{Schema: s})
	if err != nil {
		return fmt.Errorf("creating collection %q: encoding payload: %w", name, err)
	}
	off, done, err := e.appendMetaWAL(ctx, wal.Record{Op: wal.OpCreateColl, DB: d.name, Coll: name, Payload: payload})
	if err != nil {
		return fmt.Errorf("creating collection %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	dir := collDir(e.root, d.name, name)
	if err := makeDir(dir); err != nil {
		return fmt.Errorf("creating collection %q: %w", name, err)
	}
	if err := writeCollMeta(dir, collMeta{Name: name, Schema: s, WALOffset: off}); err != nil {
		return fmt.Errorf("creating collection %q: %w", name, err)
	}
	done()

	d.collections[name] = newCollection(d, name, s, off)
	return nil
}

// Collection returns the collection name.
func (d *Database) Collection(name string) (*Collection, error) {
	if err := ValidateName("collection", name); err != nil {
		return nil, err
	}
	if d.e.isClosed() {
		return nil, ErrClosed
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.dropped {
		return nil, d.errDropped()
	}
	c, ok := d.collections[name]
	if !ok {
		return nil, fmt.Errorf("collection %q in database %q: %w", name, d.name, ErrNotFound)
	}
	return c, nil
}

// ListCollections returns the collection names in sorted order (nil for a
// dropped database).
func (d *Database) ListCollections() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.dropped {
		return nil
	}
	return slices.Sorted(maps.Keys(d.collections))
}

// DropCollection drops the collection name. It waits for in-flight writes
// in the database. The directory is renamed to <db>/.deleted-<offset> and
// removed in the background.
func (d *Database) DropCollection(ctx context.Context, name string) error {
	if err := ValidateName("collection", name); err != nil {
		return err
	}
	e := d.e
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.dropped {
		return d.errDropped()
	}
	c, ok := d.collections[name]
	if !ok {
		return fmt.Errorf("dropping collection %q in database %q: %w", name, d.name, ErrNotFound)
	}

	// Wait for a running flush and keep the flusher away from the
	// directory while it is renamed (lock order: Database.mu → flushMu).
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	off, done, err := e.appendMetaWAL(ctx, wal.Record{Op: wal.OpDropColl, DB: d.name, Coll: name})
	if err != nil {
		return fmt.Errorf("dropping collection %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	moved, err := renameAway(c.dir, off)
	if err != nil {
		return fmt.Errorf("dropping collection %q: %w", name, err)
	}
	done()

	c.markDropped()
	delete(d.collections, name)
	e.removeInBackground(moved)
	return nil
}

// setSchemaVersion persists v as the database's schema version.
func (d *Database) setSchemaVersion(v int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.dropped {
		return d.errDropped()
	}
	if err := writeDBMeta(d.dir, dbMeta{Name: d.name, SchemaVersion: v, WALOffset: d.walOffset}); err != nil {
		return fmt.Errorf("setting schema version of database %q: %w", d.name, err)
	}
	d.schemaVersion = v
	return nil
}
