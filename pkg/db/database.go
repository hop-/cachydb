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

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Database is a named group of collections inside an Engine. It stays
// valid until it is dropped; methods on a dropped Database return
// ErrNotFound.
type Database struct {
	e    *Engine
	name string
	dir  string

	// mu guards the fields below. Metadata operations (create/drop/rename
	// collection, create/drop index, schema updates, schema-version
	// changes, drop of the database) hold it exclusively for their whole duration, including
	// WAL and file I/O; document writes hold it shared (task 09).
	mu            sync.RWMutex
	collections   map[string]*Collection
	schemaVersion int
	walOffset     uint64 // db.meta wal_offset
	dropped       bool

	// renamePending maps, during recovery only, a collection directory
	// name to the collection a rename moved there before rewriting its
	// coll.meta (registered in collections under its old name until the
	// rename record is replayed). nil when there is none.
	renamePending map[string]*Collection
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

// loadCollection reads the coll.meta of one collection directory; its data
// segments are loaded later by Collection.load. It returns (nil, nil) for
// a directory without coll.meta (a create that did not reach its meta
// write; WAL replay recreates it if its record is still in the log).
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
	c := newCollection(d, name, m.Schema, m.CreatedOffset, m.WALOffset)
	if m.Name != name {
		if !validName(m.Name) {
			return nil, fmt.Errorf("%s: name %q does not match directory %q", filepath.Join(dir, collMetaFile), m.Name, name)
		}
		// A rename moved the directory but did not rewrite coll.meta
		// (plan 004 §3.4): keep the old name until replay of the rename
		// record finishes it.
		d.e.logger.Warn("cachydb: collection directory renamed but coll.meta not updated; WAL replay completes the rename",
			"dir", dir, "meta_name", m.Name)
		c = newCollection(d, m.Name, m.Schema, m.CreatedOffset, m.WALOffset)
		c.dir, c.pendingRename = dir, name
		if d.renamePending == nil {
			d.renamePending = make(map[string]*Collection)
		}
		d.renamePending[name] = c
	}
	for _, def := range m.Indexes {
		c.indexes[def.Name] = newIndex(def.Name, def.Field)
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
	if err := writeCollMeta(dir, collMeta{Name: name, Schema: s, WALOffset: off, CreatedOffset: off}); err != nil {
		return fmt.Errorf("creating collection %q: %w", name, err)
	}
	done()

	d.collections[name] = newCollection(d, name, s, off, off)
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

	// Wait for a running compaction and flush and keep both away from
	// the directory while it is renamed (lock order: Database.mu →
	// compactMu → flushMu).
	c.compactMu.Lock()
	defer c.compactMu.Unlock()
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

// RenameCollection renames the collection oldName to newName within the
// database (plan 004 §3.4). It fails with ErrNotFound if oldName does not
// exist and with ErrExists if newName does. Pending changes of the
// collection are flushed first; then the rename_coll WAL record commits
// the rename, the directory is renamed, and coll.meta rewritten. Writes to
// the database wait meanwhile. Handles to the old collection return
// ErrNotFound afterwards; get the renamed one with Collection(newName).
func (d *Database) RenameCollection(ctx context.Context, oldName, newName string) error {
	if err := ValidateName("collection", oldName); err != nil {
		return err
	}
	if err := ValidateName("collection", newName); err != nil {
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
	c, ok := d.collections[oldName]
	if !ok {
		return fmt.Errorf("renaming collection %q in database %q: %w", oldName, d.name, ErrNotFound)
	}
	if _, ok := d.collections[newName]; ok {
		return fmt.Errorf("renaming collection %q: collection %q in database %q: %w", oldName, newName, d.name, ErrExists)
	}
	newDir := collDir(e.root, d.name, newName)
	if _, err := os.Lstat(newDir); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("renaming collection %q: %s %w (a leftover directory; remove it)", oldName, newDir, ErrExists)
	}

	// Wait for a running compaction and keep it away (lock order:
	// Database.mu → compactMu → flushMu), then flush: no write runs (the
	// writes hold Database.mu shared), so afterwards every change of the
	// collection is in its data segments and no WAL record under the old
	// name is needed by replay.
	c.compactMu.Lock()
	defer c.compactMu.Unlock()
	if err := c.flush(); err != nil {
		return fmt.Errorf("renaming collection %q: %w", oldName, err)
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.mu.RLock()
	pending := len(c.memtable) > 0 || c.flushing != nil
	c.mu.RUnlock()
	if pending {
		return fmt.Errorf("renaming collection %q: pending changes could not be flushed", oldName)
	}

	payload, err := json.Marshal(renameCollPayload{NewName: newName})
	if err != nil {
		return fmt.Errorf("renaming collection %q: encoding payload: %w", oldName, err)
	}
	off, done, err := e.appendMetaWAL(ctx, wal.Record{Op: wal.OpRenameColl, DB: d.name, Coll: oldName, Payload: payload})
	if err != nil {
		return fmt.Errorf("renaming collection %q: %w", oldName, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	// Committed. If a file step fails now, the collection stays
	// unavailable until a restart replays the record.
	if err := c.detach(); err != nil {
		e.logger.Warn("cachydb: closing segments of a renamed collection", "database", d.name, "collection", oldName, "err", err)
	}
	delete(d.collections, oldName)
	if err := renameDir(c.dir, newDir); err != nil {
		return fmt.Errorf("renaming collection %q (restart to complete it): %w", oldName, err)
	}
	if err := e.hook(hookRenameDir); err != nil {
		return err
	}
	nc := c.moveState(newName, newDir, off)
	if err := nc.writeMeta(nc.indexDefs(), off); err != nil {
		return fmt.Errorf("renaming collection %q (restart to complete it): %w", oldName, err)
	}
	done()
	d.collections[newName] = nc
	return nil
}

// renameDir renames the directory from to to and fsyncs their parent.
func renameDir(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return fmt.Errorf("renaming %s: %w", from, err)
	}
	if err := fsutil.SyncDir(filepath.Dir(to)); err != nil {
		return fmt.Errorf("renaming %s: %w", from, err)
	}
	return nil
}

// copyWorkers is how many documents CopyCollection inserts at once, so
// the WAL group commit batches them.
const copyWorkers = 32

// CopyCollection creates the collection newName in target (nil = this
// database) with the schema and index definitions of the collection name
// and inserts every document of it, with the same ids, through the normal
// write path (plan 004 §3.5). It returns how many documents it copied. It
// fails with ErrExists if the target exists.
//
// It is not atomic: on an error (or a crash) the target may hold only
// part of the documents; drop it and retry. The source is read as of the
// start of the copy: documents written to it meanwhile may or may not be
// included.
func (d *Database) CopyCollection(ctx context.Context, name, newName string, target *Database) (int, error) {
	if target == nil {
		target = d
	}
	src, err := d.Collection(name)
	if err != nil {
		return 0, err
	}
	if err := target.CreateCollection(ctx, newName, src.Schema()); err != nil {
		return 0, fmt.Errorf("copying collection %q: %w", name, err)
	}
	dst, err := target.Collection(newName)
	if err != nil {
		return 0, fmt.Errorf("copying collection %q: %w", name, err)
	}
	for _, ix := range src.Indexes() {
		if err := dst.CreateIndex(ctx, ix.Name, ix.Field); err != nil {
			return 0, fmt.Errorf("copying collection %q: %w", name, err)
		}
	}

	src.mu.RLock()
	ids := src.liveIDsLocked()
	src.mu.RUnlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		copied   int
	)
	work := make(chan string)
	for range min(copyWorkers, max(len(ids), 1)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range work {
				doc, err := src.Get(ctx, id)
				if errors.Is(err, ErrNotFound) {
					continue // deleted since the copy started
				}
				if err == nil {
					_, err = dst.Insert(ctx, doc)
				}
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("copying document %q: %w", id, err)
					cancel()
				} else if err == nil {
					copied++
				}
				mu.Unlock()
			}
		}()
	}
	var aborted error
feed:
	for _, id := range ids {
		select {
		case work <- id:
		case <-ctx.Done():
			aborted = ctx.Err()
			break feed
		}
	}
	close(work)
	wg.Wait()
	if firstErr == nil {
		firstErr = aborted
	}
	if firstErr != nil {
		return copied, fmt.Errorf("copying collection %q to %q (partial copy of %d documents; drop the target and retry): %w", name, newName, copied, firstErr)
	}
	return copied, nil
}
