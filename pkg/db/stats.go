package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// Stats is a snapshot of an engine's sizes (Engine.Stats).
type Stats struct {
	Databases []DatabaseStats // sorted by name; the system database is not listed
	Server    ServerStats
}

// ServerStats holds the engine-wide figures of Stats.
type ServerStats struct {
	WALBytes int64      // size of the write-ahead log's segment files
	Cache    CacheStats // the document cache (Options.CacheSize)
}

// CacheStats describes the document cache: its size and how often a read
// found its document there.
type CacheStats struct {
	Bytes    int64  // bytes charged by the cached documents
	MaxBytes int64  // Options.CacheSize (0: the cache is disabled)
	Entries  int    // number of cached documents
	Hits     uint64 // reads served from the cache since Open
	Misses   uint64 // reads that went to a data segment since Open
}

// DatabaseStats holds the figures of one database: its collections and
// their totals.
type DatabaseStats struct {
	Name        string
	Collections []CollectionStats // sorted by name
	Documents   int64             // sum over the collections
	DataBytes   int64
	IndexBytes  int64
}

// CollectionStats holds the figures of one collection. DataBytes is the
// size of the records in its data segments (live and superseded versions,
// as the index file accounts them; changes not yet flushed from the WAL
// are not included); IndexBytes is the size of its index.db file, which
// holds the key table and every secondary index.
type CollectionStats struct {
	Name       string
	Documents  int
	DataBytes  int64
	IndexBytes int64
	Indexes    []string // secondary index names, sorted
}

// Stats returns the figures of every database and the engine-wide ones.
// Databases and collections dropped while it runs are left out.
func (e *Engine) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	for _, name := range e.ListDatabases() {
		d, err := e.Database(name)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return Stats{}, err
		}
		ds, err := d.Stats(ctx)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return Stats{}, err
		}
		st.Databases = append(st.Databases, ds)
	}
	srv, err := e.ServerStats()
	if err != nil {
		return Stats{}, err
	}
	st.Server = srv
	return st, nil
}

// ServerStats returns the engine-wide figures of Stats: the WAL size and
// the document cache.
func (e *Engine) ServerStats() (ServerStats, error) {
	if err := e.enter(); err != nil {
		return ServerStats{}, err
	}
	defer e.exit()
	walBytes, err := e.wal.Size()
	if err != nil {
		return ServerStats{}, fmt.Errorf("server stats: %w", err)
	}
	cs := e.cache.Stats()
	return ServerStats{
		WALBytes: walBytes,
		Cache: CacheStats{
			Bytes: cs.Bytes, MaxBytes: cs.MaxBytes, Entries: cs.Entries,
			Hits: cs.Hits, Misses: cs.Misses,
		},
	}, nil
}

// Stats returns the figures of every collection of the database and their
// totals. Collections dropped while it runs are left out; a dropped
// database fails with ErrNotFound.
func (d *Database) Stats(ctx context.Context) (DatabaseStats, error) {
	d.mu.RLock()
	dropped := d.dropped
	d.mu.RUnlock()
	if dropped {
		return DatabaseStats{}, d.errDropped()
	}
	ds := DatabaseStats{Name: d.name, Collections: []CollectionStats{}}
	for _, name := range d.ListCollections() {
		if err := ctx.Err(); err != nil {
			return DatabaseStats{}, err
		}
		c, err := d.Collection(name)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return DatabaseStats{}, err
		}
		cs, err := c.Stats(ctx)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return DatabaseStats{}, err
		}
		ds.Collections = append(ds.Collections, cs)
		ds.Documents += int64(cs.Documents)
		ds.DataBytes += cs.DataBytes
		ds.IndexBytes += cs.IndexBytes
	}
	return ds, nil
}

// Stats returns the collection's document count, data and index sizes,
// and index names. A dropped collection fails with ErrNotFound.
func (c *Collection) Stats(ctx context.Context) (CollectionStats, error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return CollectionStats{}, err
	}
	defer e.exit()
	if err := ctx.Err(); err != nil {
		return CollectionStats{}, err
	}
	st, err := c.indexStore()
	if err != nil {
		return CollectionStats{}, err
	}
	var segs []idxstore.SegmentStats
	if err := st.View(func(tx *idxstore.Tx) (err error) {
		segs, err = tx.Segments()
		return err
	}); err != nil {
		return CollectionStats{}, c.storeErr(err)
	}
	fi, err := os.Stat(st.Path())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Dropped or renamed since the store was taken.
			c.mu.RLock()
			dropped := c.dropped
			c.mu.RUnlock()
			if dropped {
				return CollectionStats{}, c.errDropped()
			}
		}
		return CollectionStats{}, fmt.Errorf("collection %q in database %q: %w", c.name, c.db.name, err)
	}
	cs := CollectionStats{Name: c.name, Documents: c.Count(), IndexBytes: fi.Size(), Indexes: []string{}}
	for _, s := range segs {
		cs.DataBytes += s.RecordBytes
	}
	for _, ix := range c.Indexes() {
		cs.Indexes = append(cs.Indexes, ix.Name)
	}
	return cs, nil
}
