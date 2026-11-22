package db

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Offline verification (approach-toolchain-and-storage §3.3, the
// "cachydb utils verify" command). Verify reads a data directory the way
// Open would, without running recovery: it takes the data-directory lock,
// starts no background work, and writes nothing unless asked to rebuild
// index files. It checks:
//
//  1. the WAL: every segment's header, every record's checksum, offset
//     continuity across all segments, and that every document record
//     decodes as replay would decode it (put_doc, and every item of a
//     batch record);
//  2. per collection: every data segment record (checksum, decompression,
//     the live documents' JSON), then idxstore's full consistency check of
//     index.db (Store.Check: bbolt's page check and the key-table, count,
//     segment-accounting, and index-entry invariants);
//  3. that every key-table entry points at a put record of its id with its
//     WAL offset, and that no record in the data index.db covers is newer
//     than what the key table holds (an index.db older than the segments);
//  4. that data index.db does not cover (unregistered segment files, the
//     tail of the covered segment, which Open deletes and truncates) is
//     either already reflected in the key table or still replayable from
//     the WAL; otherwise opening would lose it (follow-up F-010);
//  5. with VerifyOptions.Deep: that every index holds exactly the entries
//     its documents produce, and the complex and arrays counters.
//
// Findings Open repairs by itself without losing data (a torn WAL tail,
// a leftover segment of an interrupted flush, an index that is not built)
// are notices; everything else is a problem.

// VerifyOptions selects what Verify checks.
type VerifyOptions struct {
	// Database limits the collection checks (and RebuildIndex) to one
	// database; Collection, which needs Database, to one collection. The
	// WAL is always checked.
	Database   string
	Collection string
	// Deep also checks every index entry against the documents.
	Deep bool
	// RebuildIndex deletes the index file (index.db) of every collection
	// in scope and builds it again from the data segments, as Open does
	// for a damaged one, before verifying.
	RebuildIndex bool
	// MaxProblems bounds the problems (and, separately, the notices) the
	// report lists; the rest are counted in VerifyReport.Omitted. Zero or
	// less means 1,000.
	MaxProblems int
}

// defaultVerifyMaxProblems is VerifyOptions.MaxProblems when unset.
const defaultVerifyMaxProblems = 1000

// verifyCtxCheckEvery is how many records or entries the verifier reads
// between context checks.
const verifyCtxCheckEvery = 1024

// VerifyFinding is one problem or notice of a VerifyReport. File is the
// file it concerns, relative to the data directory, when there is one;
// Database and Collection name the collection it concerns, when there is
// one.
type VerifyFinding struct {
	File       string `json:"file,omitempty"`
	Database   string `json:"database,omitempty"`
	Collection string `json:"collection,omitempty"`
	Message    string `json:"message"`
}

// String returns the finding as one line: where, then what.
func (f VerifyFinding) String() string {
	switch {
	case f.File != "":
		return f.File + ": " + f.Message
	case f.Collection != "":
		return f.Database + "/" + f.Collection + ": " + f.Message
	case f.Database != "":
		return f.Database + ": " + f.Message
	}
	return f.Message
}

// VerifyReport is the result of Verify. OK is true when Problems is empty
// and nothing was omitted.
type VerifyReport struct {
	OK           bool            `json:"ok"`
	Dir          string          `json:"dir"`
	Format       int             `json:"format"` // data-directory format version found
	Deep         bool            `json:"deep"`
	Databases    int             `json:"databases"`
	Collections  int             `json:"collections"`
	Documents    int64           `json:"documents"` // live documents the index files count
	DataSegments int             `json:"data_segments"`
	DataRecords  int64           `json:"data_records"`
	Indexes      int             `json:"indexes_checked"` // indexes checked entry by entry (Deep)
	WALSegments  int             `json:"wal_segments"`
	WALRecords   int64           `json:"wal_records"`
	WALFirst     uint64          `json:"wal_first"` // offset of the oldest WAL record kept
	WALNext      uint64          `json:"wal_next"`  // offset the next WAL record would get
	Rebuilt      []string        `json:"rebuilt,omitempty"`
	Problems     []VerifyFinding `json:"problems"`
	Notices      []VerifyFinding `json:"notices"`
	Omitted      int             `json:"omitted_problems,omitempty"`
}

// Verify checks the data directory opts.Dir offline (see the top of this
// file) and returns what it found. It needs the data-directory lock and
// fails with ErrDataDirInUse while an Engine (in any process) has the
// directory open. It fails with ErrIncompatibleDataDir for a directory
// Open would refuse, and with ErrNotFound for a VerifyOptions.Database or
// Collection that does not exist. Damage is reported in the result, not
// as an error; the error reports a failure to run the check.
//
// Of opts only Dir, Logger, and DataSegmentSize are used. Without
// RebuildIndex nothing in the directory is written (the lock file is
// created if missing). RebuildIndex needs a directory of FormatVersion: a
// format-2 directory is upgraded by Open, which builds the index files.
func Verify(ctx context.Context, opts Options, vo VerifyOptions) (_ *VerifyReport, err error) {
	opts, err = opts.withDefaults()
	if err != nil {
		return nil, err
	}
	if vo.Collection != "" && vo.Database == "" {
		return nil, errors.New("verify: a collection needs its database")
	}
	if vo.MaxProblems <= 0 {
		vo.MaxProblems = defaultVerifyMaxProblems
	}
	root := filepath.Clean(opts.Dir)
	switch fi, err := os.Stat(root); {
	case errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("verify: data directory %s does not exist", root)
	case err != nil:
		return nil, fmt.Errorf("verify: %w", err)
	case !fi.IsDir():
		return nil, fmt.Errorf("verify: %s is not a directory", root)
	}

	lock, err := fsutil.AcquireLock(filepath.Join(root, lockFileName))
	if err != nil {
		if errors.Is(err, fsutil.ErrLocked) {
			return nil, fmt.Errorf("verifying %s: %w", root, ErrDataDirInUse)
		}
		return nil, fmt.Errorf("verifying %s: %w", root, err)
	}
	if !lock.Supported() {
		opts.Logger.Warn("cachydb: data-dir locking unsupported on "+runtime.GOOS+
			"; do not open this directory from two processes at once", "dir", root)
	}
	defer func() {
		if rerr := lock.Release(); rerr != nil && err == nil {
			err = fmt.Errorf("verifying %s: releasing the data-directory lock: %w", root, rerr)
		}
	}()

	v := &verifier{
		root: root,
		vo:   vo,
		r:    &VerifyReport{Dir: root, Deep: vo.Deep, Problems: []VerifyFinding{}, Notices: []VerifyFinding{}},
		// The engine holds the metadata and lends its collections to the
		// rebuild routine; it never opens the WAL or starts background
		// work.
		e: &Engine{
			opts:      opts,
			root:      root,
			logger:    opts.Logger,
			cache:     newDocCache(0),
			databases: make(map[string]*Database),
		},
	}
	if err := v.run(ctx); err != nil {
		return nil, fmt.Errorf("verifying %s: %w", root, err)
	}
	v.r.OK = len(v.r.Problems) == 0 && v.r.Omitted == 0
	return v.r, nil
}

// verifier holds the state of one Verify run.
type verifier struct {
	root     string
	vo       VerifyOptions
	r        *VerifyReport
	e        *Engine
	walFirst uint64
	walNext  uint64
	// walDamaged is set when the WAL has damage Open fails on: its end
	// is then not known.
	walDamaged bool
	// refs is, per file, the largest WAL offset it references: the data
	// must not reference an offset the WAL has not reached.
	refs map[string]uint64
}

func (v *verifier) problem(f VerifyFinding) {
	if len(v.r.Problems) >= v.vo.MaxProblems {
		v.r.Omitted++
		return
	}
	v.r.Problems = append(v.r.Problems, f)
}

func (v *verifier) notice(f VerifyFinding) {
	if len(v.r.Notices) < v.vo.MaxProblems {
		v.r.Notices = append(v.r.Notices, f)
	}
}

// remaining returns how many more problems the report lists, at least 1.
func (v *verifier) remaining() int { return max(v.vo.MaxProblems-len(v.r.Problems), 1) }

// rel returns path relative to the data directory, for findings.
func (v *verifier) rel(path string) string {
	if r, err := filepath.Rel(v.root, path); err == nil {
		return r
	}
	return path
}

// reference records that file references WAL offset off.
func (v *verifier) reference(file string, off uint64) {
	if v.refs == nil {
		v.refs = make(map[string]uint64)
	}
	v.refs[file] = max(v.refs[file], off)
}

func (v *verifier) run(ctx context.Context) error {
	format, err := v.checkFormat()
	if err != nil {
		return err
	}
	v.r.Format = format
	if err := v.loadMetadata(ctx); err != nil {
		return err
	}
	colls, err := v.scope()
	if err != nil {
		return err
	}
	if v.vo.RebuildIndex {
		if format != FormatVersion {
			return fmt.Errorf("the data directory has format version %d; open it once (cachydb utils list) to upgrade it, which builds the index files", format)
		}
		for _, c := range colls {
			if err := v.rebuild(ctx, c); err != nil {
				return err
			}
		}
	}
	if err := v.checkWAL(); err != nil {
		return err
	}
	for _, c := range colls {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := v.checkCollection(ctx, c); err != nil {
			return fmt.Errorf("collection %q in database %q: %w", c.name, c.db.name, err)
		}
	}
	if v.walDamaged {
		return nil // the end of the WAL is not known; Open fails on the damage anyway
	}
	for _, file := range slices.Sorted(maps.Keys(v.refs)) {
		if off := v.refs[file]; off >= v.walNext {
			v.problem(VerifyFinding{File: file, Message: fmt.Sprintf(
				"references WAL offset %d, but the WAL ends before offset %d: WAL files are missing, Open fails", off, v.walNext)})
		}
	}
	return nil
}

// checkFormat reads cachydb.format as Open does (checkDataDir), without
// writing it, and returns the format version.
func (v *verifier) checkFormat() (int, error) {
	path := filepath.Join(v.root, formatFileName)
	data, err := os.ReadFile(path)
	if err == nil {
		err := validateFormatFile(v.root, data)
		switch {
		case err == nil:
			return FormatVersion, nil
		case errors.Is(err, errFormatUpgrade):
			v.notice(VerifyFinding{File: formatFileName, Message: fmt.Sprintf(
				"format version %d: the next Open upgrades the directory to version %d (one-way) and builds the index files; index.db is not checked",
				upgradableFormatVersion, FormatVersion)})
			return upgradableFormatVersion, nil
		}
		return 0, err
	}
	if !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("reading %s: %w", path, err)
	}
	old, err := hasOldLayout(v.root)
	if err != nil {
		return 0, err
	}
	if old {
		return 0, fmt.Errorf("%w: data directory %s was created by an older cachydb format; move or delete it",
			ErrIncompatibleDataDir, v.root)
	}
	entries, err := os.ReadDir(v.root)
	if err != nil {
		return 0, fmt.Errorf("reading data directory: %w", err)
	}
	if slices.ContainsFunc(entries, func(e os.DirEntry) bool { return e.Name() != lockFileName }) {
		v.notice(VerifyFinding{File: formatFileName, Message: "missing; Open writes it"})
	}
	return FormatVersion, nil
}

// loadMetadata reads every db.meta and coll.meta like Engine.loadMetadata,
// but a database whose metadata cannot be read is a problem, not the end
// of the check.
func (v *verifier) loadMetadata(ctx context.Context) error {
	entries, err := os.ReadDir(v.root)
	if err != nil {
		return fmt.Errorf("reading data directory: %w", err)
	}
	for _, ent := range entries {
		if !ent.IsDir() || !validDatabaseDir(ent.Name()) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		d, err := v.e.loadDatabase(ent.Name())
		switch {
		case err != nil:
			v.problem(VerifyFinding{File: ent.Name(), Database: ent.Name(), Message: fmt.Sprintf("Open fails: %v", err)})
		case d == nil:
			v.notice(VerifyFinding{File: ent.Name(), Message: "directory without db.meta (an interrupted create_database); Open skips it"})
		default:
			v.e.databases[d.name] = d
		}
	}
	return nil
}

// scope returns the collections to check, by database and collection
// name, and counts the databases and collections in scope.
func (v *verifier) scope() ([]*Collection, error) {
	if name := v.vo.Database; name != "" {
		d, ok := v.e.databases[name]
		if !ok {
			return nil, fmt.Errorf("database %q: %w", name, ErrNotFound)
		}
		if c := v.vo.Collection; c != "" {
			if _, ok := d.collections[c]; !ok {
				return nil, fmt.Errorf("collection %q in database %q: %w", c, name, ErrNotFound)
			}
		}
	}
	var colls []*Collection
	dbs := make(map[string]bool)
	for _, c := range v.e.collectionsInOrder() {
		if v.vo.Database != "" && c.db.name != v.vo.Database || v.vo.Collection != "" && c.name != v.vo.Collection {
			continue
		}
		colls = append(colls, c)
	}
	for name, d := range v.e.databases {
		if v.vo.Database == "" || name == v.vo.Database {
			dbs[name] = true
			v.reference(filepath.Join(name, dbMetaFile), d.walOffset)
		}
	}
	for _, c := range colls {
		v.reference(v.rel(filepath.Join(c.dir, collMetaFile)), max(c.metaOffset, c.created))
	}
	v.r.Databases, v.r.Collections = len(dbs), len(colls)
	return colls, nil
}

// rebuild deletes the collection's index file and builds it again from
// its data segments (rebuildIndex, the routine Open uses for a damaged
// one).
func (v *verifier) rebuild(ctx context.Context, c *Collection) error {
	st, err := c.rebuildIndex(ctx)
	if err == nil {
		err = st.Close()
	}
	if cerr := c.closeFiles(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("rebuilding the index file of collection %q in database %q: %w", c.name, c.db.name, err)
	}
	v.e.logger.Info("cachydb: rebuilt the index file from the data segments",
		"database", c.db.name, "collection", c.name, "file", idxstore.Path(c.dir))
	v.r.Rebuilt = append(v.r.Rebuilt, c.db.name+"/"+c.name)
	return nil
}

// checkWAL checks every WAL segment and every document record's payload,
// and the metadata checkpoint against the end of the WAL.
func (v *verifier) checkWAL() error {
	res, err := wal.Verify(v.root, v.checkWALRecord)
	if err != nil {
		return err
	}
	v.r.WALSegments, v.r.WALRecords = res.Segments, res.Records
	v.r.WALFirst, v.r.WALNext = res.First, res.Next
	v.walFirst, v.walNext = res.First, res.Next
	v.walDamaged = len(res.Problems) > 0
	for _, f := range res.Problems {
		v.problem(VerifyFinding{File: v.rel(f.Path), Message: fmt.Sprintf("at byte %d: %s; Open fails", f.Pos, f.Reason)})
	}
	for _, f := range res.Notices {
		v.notice(VerifyFinding{File: v.rel(f.Path), Message: fmt.Sprintf("at byte %d: %s", f.Pos, f.Reason)})
	}
	if _, _, err := readMetaCheckpoint(v.root, res.Next); err != nil && !v.walDamaged {
		v.notice(VerifyFinding{File: metaCheckpointFileName, Message: fmt.Sprintf("%v; Open ignores it and replays from the meta files' offsets", err)})
	}
	return nil
}

// checkWALRecord checks that a document record meets the rules replay
// applies (the id rule of checkReplayID, the payload decoded the way
// replay decodes it), that a batch record names each id once, and that
// a metadata record's payload is JSON.
func (v *verifier) checkWALRecord(path string, pos int64, r wal.Record) error {
	report := func(err error, consequence string) {
		v.problem(VerifyFinding{File: v.rel(path), Database: r.DB, Collection: r.Coll,
			Message: fmt.Sprintf("at byte %d: %s record at WAL offset %d for %s/%s: %v; %s",
				pos, r.Op, r.Offset, r.DB, r.Coll, err, consequence)})
	}
	bad := func(err error) { report(err, "replay fails") }
	switch r.Op {
	case wal.OpPutDoc, wal.OpDeleteDoc:
		if err := checkReplayID(r.ID); err != nil {
			bad(err)
			return nil
		}
		if r.Op != wal.OpPutDoc {
			return nil
		}
		if _, err := decodeDocument(r.ID, r.Payload); err != nil {
			bad(err)
		}
	case wal.OpBatch:
		items, err := wal.DecodeBatch(r.Payload)
		if err != nil {
			bad(err)
			return nil
		}
		for i, it := range items {
			if err := checkReplayID(it.ID); err != nil {
				bad(fmt.Errorf("item %d: %w", i, err))
				return nil
			}
			if it.Op != wal.OpPutDoc {
				continue
			}
			if _, err := decodeDocument(it.ID, it.Doc); err != nil {
				bad(fmt.Errorf("item %d: %w", i, err))
				return nil
			}
		}
		if i, j := repeatedBatchID(items); i >= 0 {
			report(fmt.Errorf("item %d repeats the id %q of item %d, which the batch format forbids", i, items[i].ID, j),
				"replay applies only the first item of the id")
		}
	case wal.OpCreateColl, wal.OpCreateIndex, wal.OpDropIndex, wal.OpUpdateSchema, wal.OpRenameColl:
		if !json.Valid(r.Payload) {
			bad(errors.New("the payload is not valid JSON"))
		}
	}
	return nil
}

// staleRec is a record of the data segments that the key table does not
// reflect.
type staleRec struct {
	seg uint32
	off uint64
}

// deepIndex is the state of the entry-by-entry check of one index.
type deepIndex struct {
	def     indexDef
	stats   idxstore.IndexStats
	cur     *idxstore.IndexCursor
	present int64 // expected entries found
	complex int64
	arrays  int64
	broken  bool // the cursor met a damaged entry (Store.Check reports it)
}

// collCheck is the state of the check of one collection.
type collCheck struct {
	v       *verifier
	c       *Collection
	tx      *idxstore.Tx // nil without a usable index file
	is      indexState
	keys    int64
	readers map[uint32]*segment.Reader
	// damaged holds the segments whose scan a damaged record stopped,
	// already reported.
	damaged map[uint32]bool

	newest      uint32 // the newest segment on disk
	registered  map[uint32]bool
	missingSegs map[uint32]bool
	// incomplete is set when damage stopped the scan of data index.db
	// covers, so not every live document was read.
	incomplete bool

	matched  int64           // key-table entries whose record the scan met
	reported map[string]bool // ids whose key-table entry is already reported
	bad      map[string]bool // ids whose key-table entry points at the wrong record
	// suspects: covered records newer than the key table's version of
	// their id; absent: covered puts of ids the key table does not have;
	// lost and lostAbsent: the same for data index.db does not cover that
	// the WAL no longer replays; tombs: per id the key table does not have,
	// its newest tombstone.
	suspects   map[string]staleRec
	absent     map[string]staleRec
	lost       map[string]staleRec
	lostAbsent map[string]staleRec
	tombs      map[string]uint64

	deep []*deepIndex
}

func (k *collCheck) at(file string) VerifyFinding {
	return VerifyFinding{File: file, Database: k.c.db.name, Collection: k.c.name}
}

func (k *collCheck) indexFile() string { return k.v.rel(idxstore.Path(k.c.dir)) }

func (k *collCheck) segFile(seg uint32) string { return k.v.rel(segment.DataPath(k.c.dir, seg)) }

func (k *collCheck) problem(file, format string, args ...any) {
	f := k.at(file)
	f.Message = fmt.Sprintf(format, args...)
	k.v.problem(f)
}

func (k *collCheck) notice(file, format string, args ...any) {
	f := k.at(file)
	f.Message = fmt.Sprintf(format, args...)
	k.v.notice(f)
}

// checkCollection checks one collection's data segments and index file.
func (v *verifier) checkCollection(ctx context.Context, c *Collection) (err error) {
	k := &collCheck{
		v: v, c: c,
		readers:     make(map[uint32]*segment.Reader),
		damaged:     make(map[uint32]bool),
		registered:  make(map[uint32]bool),
		missingSegs: make(map[uint32]bool),
		reported:    make(map[string]bool),
		bad:         make(map[string]bool),
		suspects:    make(map[string]staleRec),
		absent:      make(map[string]staleRec),
		lost:        make(map[string]staleRec),
		lostAbsent:  make(map[string]staleRec),
		tombs:       make(map[string]uint64),
	}
	defer func() {
		for _, r := range k.readers {
			if cerr := r.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	}()
	segs, err := segment.ListSegments(c.dir)
	if err != nil {
		return err
	}
	v.r.DataSegments += len(segs)
	if len(segs) > 0 {
		k.newest = segs[len(segs)-1]
	}

	st, err := k.openIndex()
	if err != nil {
		return err
	}
	if st != nil {
		defer func() {
			if cerr := st.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
		if err := k.prepare(st, segs); err != nil {
			return err
		}
		tx, err := st.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Close() }() // a read transaction has nothing to lose
		k.tx = tx
		if err := k.prepareTx(); err != nil {
			return err
		}
	}

	for _, seg := range segs {
		if err := k.scanSegment(ctx, seg); err != nil {
			return err
		}
	}
	if k.tx == nil {
		return nil
	}
	if err := k.findBadEntries(ctx); err != nil {
		return err
	}
	k.reportStale()
	for _, di := range k.deep {
		if err := k.finishIndex(ctx, di); err != nil {
			return err
		}
	}
	return nil
}

// openIndex opens the collection's index file and runs its full check.
// It returns nil (and no error) when there is no usable index file: a
// format-2 directory, a missing or damaged file (a problem), or a file
// whose pages are damaged.
func (k *collCheck) openIndex() (*idxstore.Store, error) {
	if k.v.r.Format != FormatVersion {
		return nil, nil
	}
	st, err := idxstore.Open(k.c.dir, idxstore.Options{})
	switch {
	case idxstore.NeedsRebuild(err):
		k.problem(k.indexFile(), "%s; Open rebuilds it from the data segments, or run verify with --rebuild-index", withoutPath(err, idxstore.Path(k.c.dir)))
		return nil, nil
	case err != nil:
		return nil, err
	}
	problems, err := st.Check(k.v.remaining())
	if err != nil {
		_ = st.Close() // read-only use; the check error is the one to report
		return nil, err
	}
	pageDamage := false
	for _, p := range problems {
		pageDamage = pageDamage || strings.HasPrefix(p, idxstore.PageProblemPrefix)
		k.problem(k.indexFile(), "%s; run verify with --rebuild-index", p)
	}
	if pageDamage {
		if err := st.Close(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return st, nil
}

// prepare reads the index file's state and compares the segments it
// registers and covers with the files on disk, as Open's alignSegments
// would.
func (k *collCheck) prepare(st *idxstore.Store, onDisk []uint32) error {
	is, err := readIndexState(st)
	if err != nil {
		return err
	}
	k.is = is
	k.v.r.Documents += is.count
	k.v.reference(k.indexFile(), max(is.maxWAL, is.checkpoint))
	for _, s := range is.segs {
		k.registered[s.Seg] = true
		if !slices.Contains(onDisk, s.Seg) {
			k.missingSegs[s.Seg] = true
			k.problem(k.segFile(s.Seg), "registered in index.db, but the file is missing: its documents are lost (Open rebuilds index.db from the remaining segments)")
		}
	}
	if seg := is.covered.Seg; k.registered[seg] && !k.missingSegs[seg] {
		fi, err := os.Stat(segment.DataPath(k.c.dir, seg))
		if err != nil {
			return err
		}
		switch size := uint64(fi.Size()); {
		case size < is.covered.End:
			k.problem(k.segFile(seg), "is %d bytes, but index.db covers %d: data is missing (Open rebuilds index.db from the segments)", size, is.covered.End)
		case size > is.covered.End:
			k.notice(k.segFile(seg), "%d bytes past the end index.db covers (an interrupted flush or a torn write); Open truncates them, and WAL replay restores the changes the WAL still holds",
				size-is.covered.End)
		}
	}
	for _, seg := range onDisk {
		if k.registered[seg] {
			continue
		}
		rolled, err := k.rolledSegment(seg)
		if err != nil {
			return err
		}
		if !rolled {
			k.notice(k.segFile(seg), "not registered in index.db (an interrupted flush or compaction); Open deletes it, and WAL replay restores the changes the WAL still holds")
		}
	}
	return nil
}

// rolledSegment reports whether the unregistered segment seg is the next
// segment a flush that filled the covered segment created (sealActive):
// newer than the covered segment and header only. That is the normal
// state after a clean close, not an interruption, and it holds no data,
// so it gets no finding (Open deletes it and the next flush creates it
// again).
func (k *collCheck) rolledSegment(seg uint32) (bool, error) {
	if seg <= k.is.covered.Seg {
		return false, nil
	}
	fi, err := os.Stat(segment.DataPath(k.c.dir, seg))
	if err != nil {
		return false, err
	}
	return fi.Size() == segment.HeaderSize, nil
}

// prepareTx counts the key table and sets up the index checks in the read
// transaction the scan uses.
func (k *collCheck) prepareTx() error {
	cur := k.tx.KeyCursor()
	for _, _, ok := cur.First(); ok; _, _, ok = cur.Next() {
		k.keys++
	}
	if cur.Err() != nil {
		k.incomplete = true // Store.Check reports the damaged entry
	}
	stats, err := k.tx.Indexes()
	if err != nil {
		return err
	}
	byName := make(map[string]idxstore.IndexStats, len(stats))
	for _, s := range stats {
		byName[s.Name] = s
		if _, ok := k.c.indexes[s.Name]; !ok {
			k.notice(k.indexFile(), "holds entries of index %q, which coll.meta does not define (an interrupted create_index or drop_index); Open drops them", s.Name)
		}
	}
	for _, def := range sortedIndexDefs(k.c.indexes) {
		s, ok := byName[def.Name]
		switch {
		case !ok:
			k.notice(k.indexFile(), "has no entries for index %q (an interrupted create_index); Open builds it", def.Name)
			continue
		case !s.Ready:
			k.notice(k.indexFile(), "index %q is not completely built (an interrupted build); Open builds it again", def.Name)
			continue
		case !k.v.vo.Deep:
			continue
		}
		cur, err := k.tx.IndexCursor(def.Name)
		if err != nil {
			return err
		}
		k.deep = append(k.deep, &deepIndex{def: def, stats: s, cur: cur})
		k.v.r.Indexes++
	}
	return nil
}

// covered reports whether the record at loc is in the data the index file
// covers: a registered segment, and in the covered segment, before the
// covered end.
func (k *collCheck) covered(loc segment.Location) bool {
	if !k.registered[loc.Seg] {
		return false
	}
	return loc.Seg != k.is.covered.Seg || loc.Pos+uint64(loc.Size) <= k.is.covered.End
}

// replayable reports whether Open's WAL replay is sure to restore the
// change at WAL offset off: it is newer than the collection's checkpoint
// and still in the WAL.
func (k *collCheck) replayable(off uint64) bool {
	return off > k.is.checkpoint && off >= k.v.walFirst && off < k.v.walNext
}

// scanSegment reads every record of one data segment, checksums and
// documents included, and checks it against the key table.
func (k *collCheck) scanSegment(ctx context.Context, seg uint32) error {
	n := 0
	validEnd, err := segment.Scan(k.c.dir, seg, func(r segment.Record, loc segment.Location) error {
		if n++; n%verifyCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		k.v.r.DataRecords++
		return k.record(r, loc)
	})
	if !errors.Is(err, segment.ErrCorrupt) {
		return err
	}
	switch {
	case k.tx == nil && seg == k.newest:
		// Without a usable index.db, Open rebuilds it (or upgrades a
		// format-2 directory), which opens the newest segment as the
		// active one: that truncates a torn final write and refuses any
		// other damage.
		keep, size, cerr := segment.CheckActive(k.c.dir, seg)
		switch {
		case cerr == nil:
			k.notice(k.segFile(seg), "%s: a torn final write; Open truncates it (%d bytes) when it rebuilds index.db",
				withoutPath(err, segment.DataPath(k.c.dir, seg)), size-keep)
		case errors.Is(cerr, segment.ErrCorrupt):
			k.problem(k.segFile(seg), "%s", withoutPath(err, segment.DataPath(k.c.dir, seg)))
		default:
			return cerr
		}
	case k.tx == nil:
		k.problem(k.segFile(seg), "%s", withoutPath(err, segment.DataPath(k.c.dir, seg)))
	case k.registered[seg] && (seg != k.is.covered.Seg || uint64(validEnd) < k.is.covered.End):
		k.incomplete = true
		k.damaged[seg] = true
		k.problem(k.segFile(seg), "%s; the records after it cannot be scanned", withoutPath(err, segment.DataPath(k.c.dir, seg)))
	default:
		k.notice(k.segFile(seg), "%s; this is past the data index.db covers, which Open removes", withoutPath(err, segment.DataPath(k.c.dir, seg)))
	}
	return nil
}

// record checks one data record against the key table.
func (k *collCheck) record(r segment.Record, loc segment.Location) error {
	if k.tx == nil || r.Type == segment.TypeCheckpoint {
		return nil
	}
	e, ok, err := k.tx.Key(r.ID)
	if err != nil {
		return nil // a damaged entry; Store.Check reports it
	}
	cov := k.covered(loc)
	switch {
	case r.Type == segment.TypePut && ok && e.Seg == loc.Seg && e.Pos == loc.Pos:
		k.matched++
		switch {
		case e.Size != loc.Size || e.WALOffset != r.WALOffset:
			k.reported[r.ID] = true
			k.problem(k.indexFile(), "the key-table entry of %q has size %d and WAL offset %d, but the record it points at (%s byte %d) has size %d and offset %d",
				r.ID, e.Size, e.WALOffset, k.segFile(loc.Seg), loc.Pos, loc.Size, r.WALOffset)
		case !cov:
			k.reported[r.ID] = true
			k.problem(k.indexFile(), "the key-table entry of %q points at %s byte %d, past the data index.db covers (Open truncates or deletes it)",
				r.ID, k.segFile(loc.Seg), loc.Pos)
		default:
			k.live(r, loc)
		}
	case ok && e.WALOffset >= r.WALOffset:
		// An older version, or a copy of the live one: dead data.
	case ok:
		// The segments hold a newer change of the id than the key table.
		switch {
		case cov:
			keepNewest(k.suspects, r.ID, staleRec{seg: loc.Seg, off: r.WALOffset})
		case !k.replayable(r.WALOffset):
			keepNewest(k.lost, r.ID, staleRec{seg: loc.Seg, off: r.WALOffset})
		}
	case r.Type == segment.TypeTombstone:
		k.tombs[r.ID] = max(k.tombs[r.ID], r.WALOffset)
	case cov:
		keepNewest(k.absent, r.ID, staleRec{seg: loc.Seg, off: r.WALOffset})
	case !k.replayable(r.WALOffset):
		keepNewest(k.lostAbsent, r.ID, staleRec{seg: loc.Seg, off: r.WALOffset})
	}
	return nil
}

func keepNewest(m map[string]staleRec, id string, s staleRec) {
	if old, ok := m[id]; !ok || s.off > old.off {
		m[id] = s
	}
}

// live checks the live version of a document: that it decodes, and with
// Deep, that every index holds its entries.
func (k *collCheck) live(r segment.Record, loc segment.Location) {
	doc, err := decodeDocument(r.ID, r.Doc)
	if err != nil {
		k.problem(k.segFile(loc.Seg), "at byte %d: %v", loc.Pos, err)
		return
	}
	for _, di := range k.deep {
		if di.broken {
			continue
		}
		keys := indexKeysOf(doc, di.def.Field)
		if keys.complex {
			di.complex++
		}
		if keys.array {
			di.arrays++
		}
		for _, en := range keys.entries {
			key := idxstore.IndexKey(en.enc, r.ID)
			got, found := di.cur.Seek(key)
			if di.cur.Err() != nil {
				di.broken = true // Store.Check reports the damaged entry
				break
			}
			if !found || !bytes.Equal(got.Key, key) {
				k.problem(k.indexFile(), "index %q has no entry for document %q with value %s", di.def.Name, r.ID, describeIndexValue(en.enc))
				continue
			}
			di.present++
			if got.Flags != en.flags {
				k.problem(k.indexFile(), "index %q: the entry for document %q with value %s has flags %d, want %d",
					di.def.Name, r.ID, describeIndexValue(en.enc), got.Flags, en.flags)
			}
		}
	}
}

// withoutPath returns the message of err without the file path it names,
// which the finding names already (relative to the data directory).
func withoutPath(err error, path string) string {
	msg := err.Error()
	for _, p := range []string{": " + path, path + " "} {
		if strings.Contains(msg, p) {
			return strings.Replace(msg, p, "", 1)
		}
	}
	return msg
}

// describeIndexValue prints an encoded index value for a finding.
func describeIndexValue(enc []byte) string {
	v, missing, err := idxstore.DecodeValue(enc)
	switch {
	case err != nil:
		return fmt.Sprintf("%x (undecodable)", enc)
	case missing:
		return "(field missing)"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// readRecord reads the record a key-table entry points at.
func (k *collCheck) readRecord(e idxstore.Entry) (segment.Record, error) {
	r, ok := k.readers[e.Seg]
	if !ok {
		var err error
		if r, err = segment.OpenReader(k.c.dir, e.Seg); err != nil {
			return segment.Record{}, err
		}
		k.readers[e.Seg] = r
	}
	return r.Read(entryLocation(e))
}

// findBadEntries finds the key-table entries the scan did not meet at
// their record and reports those that do not point at a put of their id
// with their WAL offset. It runs only when there are such entries, and
// stops once it has accounted for all of them.
func (k *collCheck) findBadEntries(ctx context.Context) error {
	want := k.keys - k.matched
	if want <= 0 {
		return nil
	}
	var found int64
	cur := k.tx.KeyCursor()
	n := 0
	for id, e, ok := cur.First(); ok && found < want; id, e, ok = cur.Next() {
		if n++; n%verifyCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		sid := string(id)
		if k.reported[sid] {
			continue // the scan met its record and reported it
		}
		if !k.registered[e.Seg] || k.missingSegs[e.Seg] {
			found++ // reported: an unregistered segment by Store.Check, a missing file above
			continue
		}
		rec, err := k.readRecord(e)
		loc := entryLocation(e)
		switch {
		case err != nil && k.damaged[e.Seg] && errors.Is(err, segment.ErrCorrupt):
			// A damaged record of a segment the scan reported.
		case err != nil:
			k.problem(k.indexFile(), "the key-table entry of %q points at %s byte %d, where there is no valid record: %s",
				sid, k.segFile(e.Seg), e.Pos, withoutPath(err, segment.DataPath(k.c.dir, e.Seg)))
		case rec.Type != segment.TypePut || rec.ID != sid || rec.WALOffset != e.WALOffset:
			k.problem(k.indexFile(), "the key-table entry of %q points at %s byte %d, which holds the %s of %q at WAL offset %d, not this document at offset %d",
				sid, k.segFile(e.Seg), e.Pos, rec.Type, rec.ID, rec.WALOffset, e.WALOffset)
		case !k.covered(loc):
			k.problem(k.indexFile(), "the key-table entry of %q points at %s byte %d, past the data index.db covers (Open truncates or deletes it)",
				sid, k.segFile(e.Seg), e.Pos)
		default:
			continue // sound; the scan stopped before it (damage reported above)
		}
		k.bad[sid] = true
		found++
	}
	return nil
}

// reportStale reports, per kind, the changes in the data segments that
// the key table does not reflect: an index file older than the segments,
// and the data opening would discard (follow-up F-010).
func (k *collCheck) reportStale() {
	var stale []staleItem
	for id, s := range k.suspects {
		if !k.bad[id] {
			stale = append(stale, staleItem{id, s})
		}
	}
	for id, s := range k.absent {
		if k.tombs[id] < s.off {
			stale = append(stale, staleItem{id, s})
		}
	}
	if len(stale) > 0 {
		first := oldestStale(stale)
		k.problem(k.indexFile(), "is older than the data segments: %d document change(s) in data it covers are newer than its key table (first: %q at WAL offset %d in %s), so reads miss them; run verify with --rebuild-index",
			len(stale), first.id, first.s.off, k.segFile(first.s.seg))
	}

	var lost []staleItem
	for id, s := range k.lost {
		if !k.bad[id] {
			lost = append(lost, staleItem{id, s})
		}
	}
	for id, s := range k.lostAbsent {
		if k.tombs[id] < s.off {
			lost = append(lost, staleItem{id, s})
		}
	}
	if len(lost) > 0 {
		first := oldestStale(lost)
		k.problem(k.indexFile(), "is older than the data segments: opening would discard %d document change(s) that it does not cover and that the WAL no longer replays (checkpoint %d, the WAL starts at offset %d; first: %q at WAL offset %d in %s); run verify with --rebuild-index before opening the directory",
			len(lost), k.is.checkpoint, k.v.walFirst, first.id, first.s.off, k.segFile(first.s.seg))
	}
}

type staleItem struct {
	id string
	s  staleRec
}

// oldestStale returns the item with the lowest WAL offset (then id).
func oldestStale(items []staleItem) staleItem {
	return slices.MinFunc(items, func(a, b staleItem) int {
		return cmp.Or(cmp.Compare(a.s.off, b.s.off), cmp.Compare(a.id, b.id))
	})
}

// finishIndex ends the deep check of one index: its counters, and the
// entries no document produces.
func (k *collCheck) finishIndex(ctx context.Context, di *deepIndex) error {
	if di.broken {
		return nil
	}
	name := di.def.Name
	if !k.incomplete {
		if di.stats.Complex != di.complex {
			k.problem(k.indexFile(), "index %q: the complex counter is %d, the documents have %d complex values", name, di.stats.Complex, di.complex)
		}
		if di.stats.Arrays != di.arrays {
			k.problem(k.indexFile(), "index %q: the arrays counter is %d, %d documents have an array value", name, di.stats.Arrays, di.arrays)
		}
	}
	extra := int64(di.cur.Count(nil, nil, 0)) - di.present
	if extra <= 0 {
		return nil
	}
	cur, err := k.tx.IndexCursor(name)
	if err != nil {
		return err
	}
	var (
		found  int64
		n      int
		lastID string
		last   docIndexKeys
	)
	for ent, ok := cur.First(); ok && found < extra; ent, ok = cur.Next() {
		if n++; n%verifyCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		id := string(ent.ID)
		if id != lastID {
			e, ok, err := k.tx.Key(id)
			if err != nil || !ok || k.bad[id] || k.reported[id] {
				found++ // reported: an entry without a document by Store.Check, a bad key-table entry above
				continue
			}
			rec, err := k.readRecord(e)
			if err != nil {
				found++ // reported by findBadEntries or the scan
				continue
			}
			doc, err := decodeDocument(id, rec.Doc)
			if err != nil {
				found++ // reported by the scan
				continue
			}
			lastID, last = id, indexKeysOf(doc, di.def.Field)
		}
		if !slices.ContainsFunc(last.entries, func(en indexEntry) bool { return bytes.Equal(en.enc, ent.Value) }) {
			k.problem(k.indexFile(), "index %q has an entry for document %q with value %s, which the document does not hold",
				name, id, describeIndexValue(ent.Value))
			found++
		}
	}
	return nil
}
