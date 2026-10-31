package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/hop-/cachydb/pkg/db/internal/cache"
	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

const lockFileName = "cachydb.lock"

// Engine is an open cachydb data directory. It owns durability: every
// change is committed to the write-ahead log before it becomes visible.
// All methods are safe for concurrent use.
//
// Lock order (§11): Engine.metaMu → Database.mu → docLock → Collection.mu.
// Engine.mu guards only the databases map; it is never held during I/O or
// while acquiring another lock, so it may be taken briefly at any point.
type Engine struct {
	opts   Options
	root   string
	logger *slog.Logger
	lock   *fsutil.Lock
	wal    *wal.Log
	cache  *cache.LRU // encoded documents read from data segments

	// metaMu serializes database-level metadata operations (create/drop
	// database) including their I/O.
	metaMu sync.Mutex
	// migrateMu serializes migrations.
	migrateMu sync.Mutex

	mu        sync.RWMutex
	databases map[string]*Database

	// Close gate: public operations enter() before touching state and
	// exit() when done; Close sets closed and waits for them.
	gateMu sync.Mutex
	closed bool
	ops    sync.WaitGroup

	// Background goroutines (drop removal; flusher, compaction in later
	// tasks) run under bgCtx and are joined by Close. bgCtx is the
	// engine's lifetime context, not a request context: it is cancelled
	// only by Close.
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bg       sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// Open opens (creating if needed) the data directory opts.Dir: it takes
// the exclusive directory lock (ErrDataDirInUse if another process — or
// another Engine in this process — holds it), checks the on-disk format
// (ErrIncompatibleDataDir), removes leftovers of interrupted drops, opens
// the WAL, and loads database and collection metadata. A database whose
// schema version is newer than CurrentSchemaVersion fails with
// ErrSchemaTooNew; an older one is logged.
func Open(ctx context.Context, opts Options) (_ *Engine, err error) {
	opts, err = opts.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Clean(opts.Dir)
	if err := os.MkdirAll(root, dirPerm); err != nil {
		return nil, fmt.Errorf("creating data directory %s: %w", root, err)
	}

	lock, err := fsutil.AcquireLock(filepath.Join(root, lockFileName))
	if err != nil {
		if errors.Is(err, fsutil.ErrLocked) {
			return nil, fmt.Errorf("opening %s: %w", root, ErrDataDirInUse)
		}
		return nil, fmt.Errorf("opening %s: %w", root, err)
	}
	defer func() {
		if err != nil {
			if rerr := lock.Release(); rerr != nil {
				opts.Logger.Warn("cachydb: releasing data-directory lock after failed open", "dir", root, "err", rerr)
			}
		}
	}()

	if err := checkDataDir(root); err != nil {
		return nil, err
	}
	if err := removeDeletedLeftovers(root); err != nil {
		return nil, fmt.Errorf("opening %s: %w", root, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	wlog, err := wal.Open(wal.Options{
		Dir:           root,
		SegmentSize:   opts.WALSegmentSize,
		RetentionAge:  opts.WALRetentionAge,
		RetentionSize: opts.WALRetentionSize,
		Logger:        opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", root, mapWALErr(err))
	}
	defer func() {
		if err != nil {
			if cerr := wlog.Close(); cerr != nil {
				opts.Logger.Warn("cachydb: closing WAL after failed open", "dir", root, "err", cerr)
			}
		}
	}()

	e := &Engine{
		opts:      opts,
		root:      root,
		logger:    opts.Logger,
		lock:      lock,
		wal:       wlog,
		cache:     cache.New(opts.CacheSize),
		databases: make(map[string]*Database),
	}
	if err := e.loadMetadata(ctx); err != nil {
		return nil, fmt.Errorf("opening %s: %w", root, err)
	}
	e.bgCtx, e.bgCancel = context.WithCancel(context.Background())
	return e, nil
}

// loadMetadata reads every <db>/db.meta and <db>/<coll>/coll.meta. The meta
// files are the metadata state; WAL replay on top of them is added by the
// recovery step of the engine.
func (e *Engine) loadMetadata(ctx context.Context) error {
	entries, err := os.ReadDir(e.root)
	if err != nil {
		return fmt.Errorf("reading data directory: %w", err)
	}
	for _, ent := range entries {
		if !ent.IsDir() || !validName(ent.Name()) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		d, err := e.loadDatabase(ent.Name())
		if err != nil {
			return err
		}
		if d != nil {
			e.databases[d.name] = d
		}
	}
	return nil
}

// loadDatabase loads one database directory. It returns (nil, nil) for a
// directory without db.meta (a create that did not reach its meta write).
func (e *Engine) loadDatabase(name string) (*Database, error) {
	dir := dbDir(e.root, name)
	var m dbMeta
	if err := readMeta(filepath.Join(dir, dbMetaFile), &m); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			e.logger.Warn("cachydb: skipping database directory without db.meta", "dir", dir)
			return nil, nil
		}
		return nil, err
	}
	if m.Name != name {
		return nil, fmt.Errorf("%s: name %q does not match directory %q", filepath.Join(dir, dbMetaFile), m.Name, name)
	}
	switch {
	case m.SchemaVersion > CurrentSchemaVersion:
		return nil, fmt.Errorf("%w: database %q has schema version %d, this build supports up to %d; upgrade cachydb",
			ErrSchemaTooNew, name, m.SchemaVersion, CurrentSchemaVersion)
	case m.SchemaVersion < 1:
		return nil, fmt.Errorf("%s: invalid schema version %d", filepath.Join(dir, dbMetaFile), m.SchemaVersion)
	case m.SchemaVersion < CurrentSchemaVersion:
		e.logger.Warn("cachydb: database schema version is older than current; run migrations",
			"database", name, "version", m.SchemaVersion, "current", CurrentSchemaVersion)
	}

	d := newDatabase(e, name, m.SchemaVersion, m.WALOffset)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}
	for _, ent := range entries {
		if !ent.IsDir() || !validName(ent.Name()) {
			continue
		}
		c, err := d.loadCollection(ent.Name())
		if err != nil {
			return nil, err
		}
		if c != nil {
			d.collections[c.name] = c
		}
	}
	return d, nil
}

// Close flushes nothing yet (flushing arrives with the flusher), waits for
// in-flight operations and background goroutines, closes the open data
// segments and the WAL, and releases the data-directory lock. It is idempotent: later calls return
// the first call's result. If ctx ends while waiting, Close still closes
// the WAL and releases the lock and returns an error including ctx.Err().
func (e *Engine) Close(ctx context.Context) error {
	e.closeOnce.Do(func() { e.closeErr = e.close(ctx) })
	return e.closeErr
}

func (e *Engine) close(ctx context.Context) error {
	var errs []error

	e.gateMu.Lock()
	e.closed = true
	e.gateMu.Unlock()
	if err := waitCtx(ctx, &e.ops); err != nil {
		errs = append(errs, fmt.Errorf("waiting for in-flight operations: %w", err))
	}

	e.bgCancel()
	if err := waitCtx(ctx, &e.bg); err != nil {
		errs = append(errs, fmt.Errorf("waiting for background work: %w", err))
	}

	e.mu.RLock()
	for _, d := range e.databases {
		d.mu.RLock()
		for _, c := range d.collections {
			if err := c.closeReaders(); err != nil {
				errs = append(errs, fmt.Errorf("closing segments of %s/%s: %w", d.name, c.name, err))
			}
		}
		d.mu.RUnlock()
	}
	e.mu.RUnlock()

	if err := e.wal.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing WAL: %w", mapWALErr(err)))
	}
	if err := e.lock.Release(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("closing %s: %w", e.root, err)
	}
	return nil
}

// waitCtx waits for wg or for ctx to end, whichever comes first. The
// helper goroutine exits once wg completes.
func waitCtx(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enter registers an in-flight public operation; it fails with ErrClosed
// once Close has started. Every successful enter must be paired with exit.
func (e *Engine) enter() error {
	e.gateMu.Lock()
	defer e.gateMu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.ops.Add(1)
	return nil
}

func (e *Engine) exit() { e.ops.Done() }

func (e *Engine) isClosed() bool {
	e.gateMu.Lock()
	defer e.gateMu.Unlock()
	return e.closed
}

// spawn runs fn in a background goroutine joined by Close. fn must return
// promptly once ctx is cancelled. It must be called from inside an
// enter/exit pair (so Close cannot have started waiting on bg yet).
func (e *Engine) spawn(fn func(ctx context.Context)) {
	e.bg.Add(1)
	go func() {
		defer e.bg.Done()
		fn(e.bgCtx)
	}()
}

// removeInBackground deletes a directory renamed away by a drop.
func (e *Engine) removeInBackground(path string) {
	e.spawn(func(context.Context) {
		if err := os.RemoveAll(path); err != nil {
			e.logger.Warn("cachydb: removing dropped directory; it will be retried on next open", "path", path, "err", err)
		}
	})
}

// appendWAL appends r and maps WAL errors to the engine's sentinels.
func (e *Engine) appendWAL(ctx context.Context, r wal.Record) (uint64, error) {
	off, err := e.wal.Append(ctx, r)
	if err != nil {
		return 0, fmt.Errorf("WAL append (%s): %w", r.Op, mapWALErr(err))
	}
	return off, nil
}

// mapWALErr wraps WAL errors so callers can match the engine's sentinels
// (and still the wal ones).
func mapWALErr(err error) error {
	switch {
	case errors.Is(err, wal.ErrCorrupt):
		return fmt.Errorf("%w: %w", ErrWALCorrupt, err)
	case errors.Is(err, wal.ErrFailed):
		return fmt.Errorf("%w: %w", ErrWALFailed, err)
	case errors.Is(err, wal.ErrClosed):
		return fmt.Errorf("%w: %w", ErrClosed, err)
	default:
		return err
	}
}

// CreateDatabase creates the database name, or returns the existing one
// (without writing a WAL record) if it already exists.
func (e *Engine) CreateDatabase(ctx context.Context, name string) (*Database, error) {
	if err := ValidateName("database", name); err != nil {
		return nil, err
	}
	if err := e.enter(); err != nil {
		return nil, err
	}
	defer e.exit()

	if d := e.lookupDatabase(name); d != nil {
		return d, nil
	}
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	if d := e.lookupDatabase(name); d != nil {
		return d, nil
	}

	off, err := e.appendWAL(ctx, wal.Record{Op: wal.OpCreateDB, DB: name})
	if err != nil {
		return nil, fmt.Errorf("creating database %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return nil, err
	}
	dir := dbDir(e.root, name)
	if err := makeDir(dir); err != nil {
		return nil, fmt.Errorf("creating database %q: %w", name, err)
	}
	if err := writeDBMeta(dir, dbMeta{Name: name, SchemaVersion: CurrentSchemaVersion, WALOffset: off}); err != nil {
		return nil, fmt.Errorf("creating database %q: %w", name, err)
	}

	d := newDatabase(e, name, CurrentSchemaVersion, off)
	e.mu.Lock()
	e.databases[name] = d
	e.mu.Unlock()
	return d, nil
}

// Database returns the open database name.
func (e *Engine) Database(name string) (*Database, error) {
	if err := ValidateName("database", name); err != nil {
		return nil, err
	}
	if e.isClosed() {
		return nil, ErrClosed
	}
	d := e.lookupDatabase(name)
	if d == nil {
		return nil, fmt.Errorf("database %q: %w", name, ErrNotFound)
	}
	return d, nil
}

func (e *Engine) lookupDatabase(name string) *Database {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.databases[name]
}

// ListDatabases returns the database names in sorted order.
func (e *Engine) ListDatabases() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return slices.Sorted(maps.Keys(e.databases))
}

// DropDatabase drops the database name and all its collections. It waits
// for in-flight writes in the database. The directory is renamed to
// <root>/.deleted-<offset> and removed in the background.
func (e *Engine) DropDatabase(ctx context.Context, name string) error {
	if err := ValidateName("database", name); err != nil {
		return err
	}
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()

	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	d := e.lookupDatabase(name)
	if d == nil {
		return fmt.Errorf("dropping database %q: %w", name, ErrNotFound)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	off, err := e.appendWAL(ctx, wal.Record{Op: wal.OpDropDB, DB: name})
	if err != nil {
		return fmt.Errorf("dropping database %q: %w", name, err)
	}
	if err := e.hook(hookMetaWrite); err != nil {
		return err
	}
	moved, err := renameAway(d.dir, off)
	if err != nil {
		return fmt.Errorf("dropping database %q: %w", name, err)
	}

	d.dropped = true
	for _, c := range d.collections {
		c.markDropped()
	}
	e.mu.Lock()
	delete(e.databases, name)
	e.mu.Unlock()
	e.removeInBackground(moved)
	return nil
}
