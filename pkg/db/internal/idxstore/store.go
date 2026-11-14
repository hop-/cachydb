// Package idxstore owns a collection's index file, index.db: one bbolt
// database holding the key table (document id -> location in the data
// segments), every secondary index, and the metadata that ties them to the
// WAL and the data segments (approach-index-and-query §2, §3). It is the
// only package that imports bbolt, and no bbolt type appears in its API.
//
// Layout:
//
//	bucket "meta"       fixed names, see below
//	bucket "keydir"     document id -> Entry (24 bytes)
//	bucket "ix/<name>"  index key (key.go) -> Flags (1 byte)
//
// The meta bucket holds "format" (the layout version), "complete" (absent
// while a rebuild is in progress), "checkpoint" (WAL offset), "count"
// (live documents, kept by PutKey and DeleteKey), "covered" (the data
// position the index reflects), one "seg:<id>" per live data segment
// (record bytes, dead bytes), and one "ix:<name>" per index (its complex
// and truncated counters and whether it is ready). All integers in the
// file are big-endian.
//
// Everything changes through Store.Update, one bbolt transaction: either
// all of it is durable or none is. That is what makes "the index reflects
// the WAL up to checkpoint and the segments up to covered" atomic.
//
// Write order: bbolt keeps the keys a transaction adds in memory, in the
// leaf they belong to, and splits leaves only at commit. Adding many keys
// to one leaf in random order therefore costs quadratic time: 50,000
// random index keys put into an empty index in one transaction took 3.7 s,
// against 0.04 s in ascending key order, and 0.06 s in random order once
// the index held 50,000 entries spread over many leaves. Apply a large
// transaction's keys in ascending order, bucket by bucket, whenever the
// bucket may be empty or small: the first flushes of a collection, an
// index build, a rebuild.
//
// Concurrency: a Store is safe for concurrent use. There is one writer at
// a time (Update calls queue) and any number of readers, each seeing the
// snapshot of the moment it began. A Tx, a WTx, and the cursors and index
// writers made from them belong to one goroutine. Never start an Update
// from a goroutine that holds an open Tx, and never open a Tx (Begin,
// View, Store.Key) inside the function of an Update, which has the WTx to
// read from: the writer may have to remap the file, which waits for every
// reader. For the same reason a long-lived Tx can delay a commit, and it
// keeps the pages it sees from being reused, so close read transactions
// promptly.
package idxstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

// FileName is the name of the index file inside a collection directory.
const FileName = "index.db"

const (
	fileMode = 0o644

	// formatVersion is the layout version stored under "format".
	formatVersion = 1

	// minFileSize is the size of the smallest file bbolt writes: four
	// pages of the smallest page size. Anything shorter is damaged.
	minFileSize = 4 * 4096

	// DefaultTimeout is how long Open and Create wait for the file lock
	// when Options.Timeout is zero. The engine's data-directory lock
	// already excludes other processes, so the wait only matters when a
	// handle of the same process is still closing.
	DefaultTimeout = time.Second

	// DefaultFillPercent is the fill percent used when Options leaves it
	// zero: bbolt's default, which splits a full page in half. It suits
	// keys arriving in random order; raise it for keys that arrive in
	// ascending order (see WTx.SetFillPercent).
	DefaultFillPercent = bolt.DefaultFillPercent

	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// Errors. The first three say that the file cannot be used as it is and
// has to be rebuilt from the data segments; NeedsRebuild tests for them.
var (
	// ErrMissing is returned by Open when the directory has no index file.
	ErrMissing = errors.New("idxstore: index file is missing")
	// ErrCorrupt is returned by Open when the file is not a usable index
	// file: too short, cut off, not a bbolt database, or without the
	// expected layout.
	ErrCorrupt = errors.New("idxstore: index file is corrupt")
	// ErrIncomplete is returned by Open for a file created with
	// Options.Building whose build never reached WTx.MarkComplete.
	ErrIncomplete = errors.New("idxstore: index file was not completely built")
	// ErrLocked is returned by Open and Create when the file lock was not
	// obtained within Options.Timeout: another handle has the file open.
	ErrLocked = errors.New("idxstore: index file is locked")
	// ErrNoIndex is returned when a named index does not exist.
	ErrNoIndex = errors.New("idxstore: no such index")
	// ErrIndexExists is returned by WTx.CreateIndex for an existing name.
	ErrIndexExists = errors.New("idxstore: index already exists")
	// ErrBadID is returned for a document id that is empty or longer than
	// MaxIDLen.
	ErrBadID = errors.New("idxstore: invalid document id")
)

// NeedsRebuild reports whether err, returned by Open, means the index
// file is missing, damaged, or half built, so the caller should remove it
// and build a new one from the data segments.
func NeedsRebuild(err error) bool {
	return errors.Is(err, ErrMissing) || errors.Is(err, ErrCorrupt) || errors.Is(err, ErrIncomplete)
}

// Options configures Open and Create. The zero value is ready to use.
type Options struct {
	// Timeout bounds the wait for the file lock; zero means
	// DefaultTimeout.
	Timeout time.Duration
	// KeyFillPercent and IndexFillPercent are the bbolt fill percents of
	// the key table and of the index buckets: how full a page is left
	// when it splits, from 0.1 to 1.0. Zero means DefaultFillPercent.
	// They are not stored in the file; a transaction can override them
	// with WTx.SetFillPercent.
	KeyFillPercent   float64
	IndexFillPercent float64
	// Building makes Create mark the new file incomplete: Open rejects it
	// with ErrIncomplete until an update transaction calls
	// WTx.MarkComplete. Use it when the file is filled over several
	// transactions (a rebuild), so that a crash in between cannot leave a
	// file that looks whole. Open ignores it.
	Building bool
}

// resolved returns o with its defaults applied, or an error if a fill
// percent is out of range.
func (o Options) resolved() (Options, error) {
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	for _, p := range []*float64{&o.KeyFillPercent, &o.IndexFillPercent} {
		if *p == 0 {
			*p = DefaultFillPercent
		}
		if err := checkFillPercent(*p); err != nil {
			return o, err
		}
	}
	return o, nil
}

func checkFillPercent(p float64) error {
	if !(p >= minFillPercent && p <= maxFillPercent) { // also rejects NaN
		return fmt.Errorf("idxstore: fill percent %v out of range [%v, %v]", p, minFillPercent, maxFillPercent)
	}
	return nil
}

// Store is an open index file.
type Store struct {
	db        *bolt.DB
	path      string
	keyFill   float64
	indexFill float64
}

// Path returns the path of the index file of the collection directory dir.
func Path(dir string) string { return filepath.Join(dir, FileName) }

// Open opens the index file of dir. It fails with an error for which
// NeedsRebuild is true when the file is missing (ErrMissing), damaged
// (ErrCorrupt), or half built (ErrIncomplete), and with ErrLocked when
// another handle holds it; any other error is an I/O failure.
//
// Open's checks are cheap and do not depend on the file's size: the file
// is long enough for the pages its newest transaction uses, bbolt accepts
// its meta pages, and the meta bucket and the buckets it names are
// present and well formed. Damage inside the tree is not found here; see
// Check.
func Open(dir string, opts Options) (*Store, error) {
	opts, err := opts.resolved()
	if err != nil {
		return nil, err
	}
	path := Path(dir)
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrMissing, path)
	}
	if err != nil {
		return nil, fmt.Errorf("idxstore: opening %s: %w", path, err)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrCorrupt, path)
	}
	// bbolt would initialise an empty file as a new database and would
	// fault reading a page beyond the end of a cut-off one, so check the
	// length before it touches anything but the meta pages.
	if fi.Size() < minFileSize {
		return nil, fmt.Errorf("%w: %s is %d bytes", ErrCorrupt, path, fi.Size())
	}
	if err := checkLength(path, fi.Size(), opts.Timeout); err != nil {
		return nil, err
	}

	s, err := openFile(path, opts)
	if err != nil {
		return nil, err
	}
	if err := s.checkLayout(); err != nil {
		_ = s.db.Close() // the layout error is the one to report
		return nil, err
	}
	return s, nil
}

// checkLength opens the file read-only, which makes bbolt read nothing but
// the two meta pages, and compares the size the newest transaction needs
// with the file's size.
func checkLength(path string, size int64, timeout time.Duration) error {
	db, err := bolt.Open(path, fileMode, &bolt.Options{ReadOnly: true, Timeout: timeout})
	if err != nil {
		return openError(path, err)
	}
	var need int64
	err = db.View(func(tx *bolt.Tx) error {
		need = tx.Size()
		return nil
	})
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("idxstore: opening %s: %w", path, err)
	}
	if need > size {
		return fmt.Errorf("%w: %s is %d bytes, its newest transaction needs %d", ErrCorrupt, path, size, need)
	}
	return nil
}

// openFile opens (and, when it does not exist, creates) the bbolt file.
func openFile(path string, opts Options) (*Store, error) {
	db, err := bolt.Open(path, fileMode, &bolt.Options{
		Timeout: opts.Timeout,
		// The map free list stays fast when the free list is large and
		// fragmented. It is synced with every commit (NoFreelistSync
		// false), so opening never has to scan the file to rebuild it.
		FreelistType:   bolt.FreelistMapType,
		NoFreelistSync: false,
	})
	if err != nil {
		return nil, openError(path, err)
	}
	return &Store{db: db, path: path, keyFill: opts.KeyFillPercent, indexFill: opts.IndexFillPercent}, nil
}

// openError classifies an error of bolt.Open. Only what bbolt itself calls
// an invalid file counts as corrupt; a lock timeout or an I/O error must
// never make the caller throw the file away.
func openError(path string, err error) error {
	switch {
	case errors.Is(err, berrors.ErrTimeout):
		return fmt.Errorf("%w: %s", ErrLocked, path)
	case errors.Is(err, berrors.ErrInvalid), errors.Is(err, berrors.ErrVersionMismatch), errors.Is(err, berrors.ErrChecksum):
		return fmt.Errorf("%w: %s: %v", ErrCorrupt, path, err)
	}
	return fmt.Errorf("idxstore: opening %s: %w", path, err)
}

// Create creates the index file of dir, empty: no keys, no indexes, count
// and checkpoint 0, nothing covered. It fails with an error wrapping
// fs.ErrExist if the file exists (see Remove). The file and its directory
// entry are durable when Create returns.
func Create(dir string, opts Options) (*Store, error) {
	opts, err := opts.resolved()
	if err != nil {
		return nil, err
	}
	path := Path(dir)
	if _, err := os.Lstat(path); err == nil {
		return nil, fmt.Errorf("idxstore: creating %s: %w", path, fs.ErrExist)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("idxstore: creating %s: %w", path, err)
	}
	s, err := openFile(path, opts)
	if err != nil {
		_ = os.Remove(path) // bbolt may have created it before failing; the open error is the one to report
		return nil, err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucket(metaBucket)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucket(keysBucket); err != nil {
			return err
		}
		if err := meta.Put(keyFormat, binary.BigEndian.AppendUint32(nil, formatVersion)); err != nil {
			return err
		}
		for _, k := range [][]byte{keyCheckpoint, keyCount} {
			if err := meta.Put(k, make([]byte, 8)); err != nil {
				return err
			}
		}
		if opts.Building {
			return nil
		}
		return meta.Put(keyComplete, completeValue)
	})
	if err == nil {
		err = fsutil.SyncDir(dir)
	}
	if err != nil {
		// Leave nothing behind: a file without its layout would only be
		// reported as corrupt by the next Open.
		_ = s.db.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("idxstore: creating %s: %w", path, err)
	}
	return s, nil
}

// Remove deletes the index file of dir, if there is one, and makes the
// removal durable. The file must not be open.
func Remove(dir string) error {
	path := Path(dir)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("idxstore: removing %s: %w", path, err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return fmt.Errorf("idxstore: removing %s: %w", path, err)
	}
	return nil
}

// Path returns the path of the store's file.
func (s *Store) Path() string { return s.path }

// Close closes the file. It waits for the running update and for every
// open read transaction, so close those first. The Store must not be used
// afterwards.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("idxstore: closing %s: %w", s.path, err)
	}
	return nil
}

// Begin starts a read transaction: a consistent snapshot of the whole
// file as of now, unaffected by later updates. The caller must Close it.
func (s *Store) Begin() (*Tx, error) {
	btx, err := s.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("idxstore: %s: starting a read transaction: %w", s.path, err)
	}
	r, err := newReader(btx, s.path)
	if err != nil {
		_ = btx.Rollback() // read-only; the layout error is the one to report
		return nil, err
	}
	return &Tx{reader: r}, nil
}

// View runs fn in a read transaction and closes it. It returns fn's error
// as it is.
func (s *Store) View(fn func(*Tx) error) error {
	t, err := s.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = t.Close() }() // a read transaction has nothing to lose
	return fn(t)
}

// Key returns the key-table entry of id in a read transaction of its own.
func (s *Store) Key(id string) (e Entry, ok bool, err error) {
	t, err := s.Begin()
	if err != nil {
		return Entry{}, false, err
	}
	defer func() { _ = t.Close() }() // a read transaction has nothing to lose
	return t.Key(id)
}

// Update runs fn in the store's update transaction. When fn returns nil
// the transaction commits: every change fn made is on disk and fsynced
// when Update returns nil. When fn returns an error, or the commit fails,
// nothing changes and Update returns that error (fn's as it is).
//
// Updates run one at a time. fn must not keep u, or anything obtained
// from it, after it returns.
func (s *Store) Update(fn func(u *WTx) error) error {
	var fnErr error
	err := s.db.Update(func(btx *bolt.Tx) error {
		r, err := newReader(btx, s.path)
		if err != nil {
			return err
		}
		r.keys.FillPercent = s.keyFill
		fnErr = fn(&WTx{reader: r, indexFill: s.indexFill})
		return fnErr
	})
	if fnErr != nil {
		return fnErr // fn's own error, as it is; bbolt rolled back
	}
	if err != nil {
		return fmt.Errorf("idxstore: %s: update: %w", s.path, err)
	}
	return nil
}
