package db

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Recovery at Open (§7.2, §7.3; approach-index-and-query §8):
//
//  1. Read every db.meta and coll.meta (loadMetadata).
//  2. Per collection, in parallel bounded by GOMAXPROCS (loadCollections):
//     open the index file (index.db) and bring the data segments in line
//     with it: delete segment files it does not register and truncate the
//     active segment to the position it covers (what a flush or compaction
//     interrupted between its segment fsync and its index commit leaves
//     behind). An index file that is missing, damaged, or does not match
//     the segments is rebuilt from them with a header-only scan
//     (rebuildIndex), with a warning, secondary indexes included. (A
//     directory of format version 2 has no index files: Open builds them
//     all before this step, see upgrade.go.) Index
//     entries without a definition in coll.meta are dropped, and
//     definitions without complete entries are built (syncIndexes).
//  3. Check that the WAL reaches past every offset the data references.
//  4. Replay the WAL sequentially from the oldest offset any metadata file
//     or collection may still be missing, applying each record
//     idempotently (replayRecord). Replayed document changes land in the
//     memtables and are flushed by the normal flusher.
//
// Open does not scan the data segments unless it rebuilds an index file
// or builds an index.
// WAL retention and the flusher start only after replay (see Open).

// recoveryStats counts what recovery did. They are logged and read by
// tests.
type recoveryStats struct {
	collectionsUpgraded atomic.Int64 // collections whose index file the format upgrade 2 -> 3 built (upgrade.go)
	indexFilesRebuilt   atomic.Int64 // index files (index.db) of format 3 rebuilt because they were missing, damaged, or did not match the data segments
	segmentsScanned     atomic.Int64 // data segments scanned by the upgrade and those rebuilds
	replayApplied       atomic.Int64 // WAL records whose effect replay applied
	replaySkipped       atomic.Int64 // WAL records replay found already applied or obsolete
}

// replayCtxCheckEvery is how many WAL records replay applies between
// context checks.
const replayCtxCheckEvery = 1024

// rebuildBatch is how many key-table entries a rebuild writes per index
// transaction.
const rebuildBatch = 50_000

// recover loads the data directory into e and replays the WAL. With
// upgrade set (a directory of format version 2, see checkDataDir) it
// upgrades the directory to format version 3 after reading the metadata
// and before loading the collections (upgradeFormat). On error the caller
// closes the collections' segment files.
func (e *Engine) recover(ctx context.Context, upgrade bool) error {
	start := time.Now()
	if err := e.loadMetadata(ctx); err != nil {
		return err
	}
	if upgrade {
		if err := e.upgradeFormat(ctx); err != nil {
			return err
		}
	}
	maxSeen, err := e.loadCollections(ctx)
	if err != nil {
		return err
	}
	for _, d := range e.databases {
		maxSeen = max(maxSeen, d.walOffset)
		for _, c := range d.collections {
			maxSeen = max(maxSeen, c.metaOffset, c.created, c.checkpoint)
		}
	}
	// Every offset the data references was durable in the WAL before it
	// was written, so the WAL must continue past it. If it does not, WAL
	// files were lost and new records would reuse offsets that the data
	// already holds, breaking "last writer wins by offset".
	if next := e.wal.NextOffset(); maxSeen >= next {
		return fmt.Errorf("%w: the data references WAL offset %d but the log ends before offset %d; WAL files are missing",
			ErrWALCorrupt, maxSeen, next)
	}

	switch cp, ok, err := readMetaCheckpoint(e.root, e.wal.NextOffset()); {
	case err != nil:
		e.logger.Warn("cachydb: ignoring the metadata checkpoint; replay starts at the meta files' offsets", "err", err)
	case ok:
		e.metacp, e.metacpLoaded = cp, true
	}

	from, err := e.replay(ctx)
	if err != nil {
		return err
	}
	st := &e.recov
	attrs := []any{
		"dir", e.root,
		"replay_from", from,
		"replayed", st.replayApplied.Load(),
		"replay_skipped", st.replaySkipped.Load(),
		"collections_upgraded", st.collectionsUpgraded.Load(),
		"index_files_rebuilt", st.indexFilesRebuilt.Load(),
		"segments_scanned", st.segmentsScanned.Load(),
		"duration", time.Since(start),
	}
	if st.replayApplied.Load() > 0 {
		e.logger.Info("cachydb: recovered changes from the WAL", attrs...)
	} else {
		e.logger.Debug("cachydb: opened data directory", attrs...)
	}
	return nil
}

// loadCollections opens the index file of every collection, brings its
// data segments and secondary indexes in line with it, in
// parallel, bounded by GOMAXPROCS. It returns the largest WAL offset the
// index files reflect.
func (e *Engine) loadCollections(ctx context.Context) (uint64, error) {
	var colls []*Collection
	for _, d := range e.databases {
		for _, c := range d.collections {
			colls = append(colls, c)
		}
	}
	if len(colls) == 0 {
		return 0, nil
	}

	var (
		mu      sync.Mutex
		maxSeen uint64
		errs    []error
		failed  atomic.Bool
		wg      sync.WaitGroup
	)
	jobs := make(chan *Collection)
	for range min(runtime.GOMAXPROCS(0), len(colls)) {
		wg.Go(func() {
			for c := range jobs {
				if failed.Load() {
					continue // drain: Open fails anyway
				}
				off, err := c.load(ctx)
				mu.Lock()
				maxSeen = max(maxSeen, off)
				if err != nil {
					errs = append(errs, err)
					failed.Store(true)
				}
				mu.Unlock()
			}
		})
	}
	for _, c := range colls {
		jobs <- c
	}
	close(jobs)
	wg.Wait()
	return maxSeen, errors.Join(errs...)
}

// indexState is what load reads from a collection's index file.
type indexState struct {
	checkpoint uint64
	maxWAL     uint64
	count      int64
	covered    idxstore.Position
	segs       []idxstore.SegmentStats // ascending
}

func readIndexState(st *idxstore.Store) (indexState, error) {
	var is indexState
	err := st.View(func(tx *idxstore.Tx) (err error) {
		if is.checkpoint, err = tx.Checkpoint(); err != nil {
			return err
		}
		if is.maxWAL, err = tx.MaxWALOffset(); err != nil {
			return err
		}
		if is.count, err = tx.Count(); err != nil {
			return err
		}
		if is.covered, err = tx.Covered(); err != nil {
			return err
		}
		is.segs, err = tx.Segments()
		return err
	})
	return is, err
}

// load opens the collection at Open (approach-index-and-query §8 step 2):
// it opens the index file, rebuilding it from the data segments when it
// is missing, damaged, or does not match them (with a warning); removes
// what an interrupted flush or compaction left in the directory
// (alignSegments); takes the checkpoint, the count, and the segment
// bookkeeping from the index file; and brings the index buckets in line
// with the index definitions (syncIndexes). It returns the largest WAL
// offset the index file reflects. Callers have not published c yet.
func (c *Collection) load(ctx context.Context) (uint64, error) {
	st, is, err := c.openIndex(ctx)
	if err != nil {
		return 0, fmt.Errorf("loading collection %q: %w", c.name, err)
	}
	c.mu.Lock()
	c.store = st
	c.checkpoint = is.checkpoint
	c.mu.Unlock()
	c.count.Store(is.count)
	if err := c.syncIndexes(ctx, st); err != nil {
		return 0, fmt.Errorf("loading indexes of collection %q: %w", c.name, err)
	}
	return max(is.maxWAL, is.checkpoint), nil
}

// openIndex returns the collection's open index file, in line with the
// data segments, and its state.
func (c *Collection) openIndex(ctx context.Context) (*idxstore.Store, indexState, error) {
	logger := c.db.e.logger
	st, err := idxstore.Open(c.dir, idxstore.Options{})
	switch {
	case err == nil:
		is, mismatch, err := c.alignSegments(st)
		if err == nil && mismatch == "" {
			return st, is, nil
		}
		if cerr := st.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			return nil, indexState{}, err
		}
		logger.Warn("cachydb: the index file does not match the data segments; rebuilding it from them",
			"database", c.db.name, "collection", c.name, "file", idxstore.Path(c.dir), "reason", mismatch)
	case idxstore.NeedsRebuild(err):
		logger.Warn("cachydb: the index file is missing or damaged; rebuilding it from the data segments",
			"database", c.db.name, "collection", c.name, "file", idxstore.Path(c.dir), "err", err)
	default:
		return nil, indexState{}, err
	}

	if st, err = c.rebuildIndex(ctx); err != nil {
		return nil, indexState{}, err
	}
	c.db.e.recov.indexFilesRebuilt.Add(1)
	is, mismatch, err := c.alignSegments(st)
	if err == nil && mismatch != "" {
		err = fmt.Errorf("the rebuilt index file does not match the data segments: %s", mismatch)
	}
	if err != nil {
		_ = st.Close() // the alignment error is the one to report
		return nil, indexState{}, err
	}
	return st, is, nil
}

// alignSegments brings the data segments in line with the index file st
// (approach-index-and-query §4, crash points) and sets the collection's
// segment bookkeeping (activeSeg, activeSealed, nextSeg):
//
//   - a segment file the index file does not register is deleted: the
//     output of a compaction, or the first flush into a new segment, that
//     did not reach its index commit, or the inputs of a compaction that
//     did;
//   - the segment the index file covers is truncated to the covered
//     position: the records beyond it are a flush that did not reach its
//     index commit (WAL replay restores them), or a torn write.
//
// When the index file refers to data that is not there (a registered
// segment without its file, or a covered position beyond the end of its
// segment), nothing is changed and mismatch says what is wrong; the
// caller rebuilds the index file. Callers have not published c yet.
func (c *Collection) alignSegments(st *idxstore.Store) (is indexState, mismatch string, err error) {
	if is, err = readIndexState(st); err != nil {
		return is, "", err
	}
	onDisk, err := segment.ListSegments(c.dir)
	if err != nil {
		return is, "", err
	}
	registered := make(map[uint32]bool, len(is.segs))
	for _, s := range is.segs {
		registered[s.Seg] = true
		if !slices.Contains(onDisk, s.Seg) {
			return is, fmt.Sprintf("data segment %d is registered but its file is missing", s.Seg), nil
		}
	}
	var coveredSize int64 // file size of the covered segment, if it is registered
	if registered[is.covered.Seg] {
		fi, err := os.Stat(segment.DataPath(c.dir, is.covered.Seg))
		if err != nil {
			return is, "", err
		}
		if coveredSize = fi.Size(); uint64(coveredSize) < is.covered.End {
			return is, fmt.Sprintf("data segment %d is %d bytes, the index file covers %d", is.covered.Seg, coveredSize, is.covered.End), nil
		}
	}

	logger := c.db.e.logger
	removed := false
	for _, seg := range onDisk {
		if registered[seg] {
			continue
		}
		path := segment.DataPath(c.dir, seg)
		logger.Info("cachydb: removing a data segment the index file does not register (an interrupted flush or compaction)", "file", path)
		if err := os.Remove(path); err != nil {
			return is, "", fmt.Errorf("removing unregistered data segment: %w", err)
		}
		removed = true
	}
	if removed {
		if err := fsutil.SyncDir(c.dir); err != nil {
			return is, "", err
		}
	}
	if uint64(coveredSize) > is.covered.End {
		path := segment.DataPath(c.dir, is.covered.Seg)
		logger.Warn("cachydb: truncating the torn tail of a data segment to the position the index file covers (an interrupted flush or a torn write); WAL replay restores the changes",
			"file", path, "bytes", uint64(coveredSize)-is.covered.End)
		if err := truncateFile(path, int64(is.covered.End)); err != nil {
			return is, "", fmt.Errorf("truncating %s: %w", path, err)
		}
		coveredSize = int64(is.covered.End)
	}

	// The newest registered segment is the active one. Flushes go on
	// appending to it when it is the segment the last flush wrote to and it
	// is not full; otherwise (a full segment, or a compaction output) the
	// next flush starts a new one.
	c.active, c.activeSeg, c.activeSealed, c.nextSeg = nil, 0, false, 1
	if n := len(is.segs); n > 0 {
		newest := is.segs[n-1].Seg
		c.activeSeg, c.nextSeg = newest, newest+1
		c.activeSealed = newest != is.covered.Seg || coveredSize >= c.db.e.opts.DataSegmentSize
	}
	return is, "", nil
}

// rebuildEntry is the newest record of one id found by a rebuild.
type rebuildEntry struct {
	ke   idxstore.Entry
	live bool // a put; false for a tombstone
}

// rebuildIndex builds a new index file for the collection from its data
// segments and returns it open (approach-index-and-query §8 step 2 and §9
// step 1). It scans every segment with the header-only scan; per id the
// record with the highest WAL offset wins and a tombstone removes the id.
// It writes the key table in ascending id order, in batches (the count is
// kept by the index file itself), then builds every secondary index of
// coll.meta from the live documents (buildIndexes), and last the
// per-segment byte accounting, the covered position (the end of the newest
// segment), the highest WAL offset seen, and the checkpoint: the newest
// checkpoint marker an older version left in the segments, 0 if there is
// none. WAL replay then starts at the collection's replay floor and
// applies what is newer.
//
// The file is created incomplete and marked complete by its last
// transaction, so a crash in between leaves a file the next Open rebuilds
// again. Nothing is written or removed before every segment was read: an
// id that is too long for the index file fails the rebuild, and with it
// Open, with the directory left as it was.
func (c *Collection) rebuildIndex(ctx context.Context) (_ *idxstore.Store, err error) {
	e := c.db.e
	segs, err := segment.ListSegments(c.dir)
	if err != nil {
		return nil, err
	}
	var (
		newest     = make(map[string]rebuildEntry) // per id, the record with the highest WAL offset
		total      = make(map[uint32]int64, len(segs))
		maxOff     uint64
		checkpoint uint64
		newestEnd  int64    // end of the last valid record of the newest segment
		torn       []uint32 // segments whose scan stopped at a damaged record
	)
	visit := func(r segment.Record, loc segment.Location) error {
		total[loc.Seg] += int64(loc.Size)
		maxOff = max(maxOff, r.WALOffset)
		if r.Type == segment.TypeCheckpoint {
			checkpoint = max(checkpoint, r.WALOffset)
			return nil
		}
		if len(r.ID) > idxstore.MaxIDLen {
			return fmt.Errorf("collection %q in database %q: a document id of %d bytes in data segment %d is longer than the limit of %d bytes; this version cannot index it",
				c.name, c.db.name, len(r.ID), loc.Seg, idxstore.MaxIDLen)
		}
		if prev, ok := newest[r.ID]; ok && prev.ke.WALOffset >= r.WALOffset {
			return nil
		}
		newest[r.ID] = rebuildEntry{ke: locationEntry(loc, r.WALOffset), live: r.Type == segment.TypePut}
		return nil
	}
	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		total[seg] = 0
		newestEnd, err = segment.ScanHeaders(c.dir, seg, visit)
		switch {
		case errors.Is(err, segment.ErrCorrupt):
			torn = append(torn, seg) // a torn tail, or damage: decided below
		case err != nil:
			return nil, err
		}
		e.recov.segmentsScanned.Add(1)
	}

	// From here on the directory changes. A segment whose scan stopped
	// early is opened as an active segment would be: that truncates a torn
	// tail (to where the scan stopped) and fails on any other damage.
	for _, seg := range torn {
		w, err := c.openActiveSegment(seg)
		if err != nil {
			return nil, err
		}
		if seg == segs[len(segs)-1] {
			newestEnd = w.Size() // also right after a torn header was rewritten
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
	}
	var covered idxstore.Position
	if len(segs) > 0 {
		covered = idxstore.Position{Seg: segs[len(segs)-1], End: uint64(newestEnd)}
	}
	if err := idxstore.Remove(c.dir); err != nil {
		return nil, err
	}
	st, err := idxstore.Create(c.dir, idxstore.Options{Building: true})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = st.Close() // the build error is the one to report; the file stays incomplete
		}
	}()

	type liveKey struct {
		id string
		ke idxstore.Entry
	}
	live := make([]liveKey, 0, len(newest))
	liveBytes := make(map[uint32]int64, len(segs))
	for id, en := range newest {
		if en.live {
			live = append(live, liveKey{id: id, ke: en.ke})
			liveBytes[en.ke.Seg] += int64(en.ke.Size)
		}
	}
	newest = nil // release it before the writes
	slices.SortFunc(live, func(a, b liveKey) int { return cmp.Compare(a.id, b.id) })
	for len(live) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := live[:min(len(live), rebuildBatch)]
		live = live[len(batch):]
		err := st.Update(func(u *idxstore.WTx) error {
			for _, k := range batch {
				if err := u.PutKey(k.id, k.ke); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if err := c.buildIndexes(ctx, st, sortedIndexDefs(c.indexes)); err != nil {
		return nil, fmt.Errorf("building indexes: %w", err)
	}
	err = st.Update(func(u *idxstore.WTx) error {
		for _, seg := range segs {
			if err := u.SetSegment(idxstore.SegmentStats{Seg: seg, RecordBytes: total[seg], DeadBytes: total[seg] - liveBytes[seg]}); err != nil {
				return err
			}
		}
		if err := u.SetCovered(covered); err != nil {
			return err
		}
		if err := u.RaiseMaxWALOffset(maxOff); err != nil {
			return err
		}
		if err := u.SetCheckpoint(checkpoint); err != nil {
			return err
		}
		return u.MarkComplete()
	})
	if err != nil {
		return nil, err
	}
	return st, nil
}

// replayStart returns the first WAL offset replay must read (§7.2 step 6).
// With a usable metadata checkpoint N (cachydb.metacp, see metacp.go) it
// is the minimum of N+1 and every collection's replay floor + 1: every
// metadata record at or below N is reflected in the meta files. Without
// one it is one past the minimum over every metadata file's wal_offset and
// every collection's replay floor. A metadata operation whose meta write did not
// happen has an offset above the wal_offset of the file it would have
// changed (its database, or the collection), so it is covered; with no
// database at all, a create_db may be missing, so replay reads the whole
// log. ReadFrom starts at the oldest kept record if the offset is below
// it: records retention deleted were processed.
func (e *Engine) replayStart() uint64 {
	e.metacpMu.Lock()
	metacp, haveMetacp := e.metacp, e.metacpLoaded
	e.metacpMu.Unlock()
	if haveMetacp {
		from := metacp + 1
		for _, d := range e.databases {
			for _, c := range d.collections {
				from = min(from, c.replayFloor()+1)
			}
		}
		return from
	}

	if len(e.databases) == 0 {
		return 1
	}
	from := uint64(math.MaxUint64)
	for _, d := range e.databases {
		from = min(from, d.walOffset+1)
		for _, c := range d.collections {
			from = min(from, c.metaOffset+1, c.replayFloor()+1)
		}
	}
	return from
}

// replayFloor returns the offset at or below which no WAL record changes
// the collection's documents any more: everything up to the checkpoint is
// durable in its data segments, and records before the collection (or its
// database) was created belong to an earlier, dropped collection of the
// same name.
func (c *Collection) replayFloor() uint64 {
	return max(c.checkpoint, c.created, c.db.walOffset)
}

// replay streams the WAL from replayStart and applies every record
// idempotently. It returns the start offset.
func (e *Engine) replay(ctx context.Context) (uint64, error) {
	from := e.replayStart()
	n := 0
	err := e.wal.ReadFrom(from, func(r wal.Record) error {
		if n++; n%replayCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		applied, err := e.replayRecord(ctx, r)
		if err != nil {
			return fmt.Errorf("replaying WAL record %d (%s %s/%s): %w", r.Offset, r.Op, r.DB, r.Coll, err)
		}
		if applied {
			e.recov.replayApplied.Add(1)
		} else {
			e.recov.replaySkipped.Add(1)
		}
		return nil
	})
	if err != nil {
		return from, fmt.Errorf("WAL replay from offset %d: %w", from, mapWALErr(err))
	}
	// A rename whose record is gone (it cannot be: the record stays in
	// the WAL until coll.meta is rewritten) is finished at the offset
	// coll.meta has, so the directory and its meta agree again.
	for _, d := range e.databases {
		for name, c := range d.renamePending {
			e.logger.Warn("cachydb: finishing a collection rename without its WAL record",
				"database", d.name, "collection", c.name, "new_name", name)
			if err := d.finishRename(c, name, c.metaOffset); err != nil {
				return from, err
			}
		}
		d.renamePending = nil
	}
	return from, nil
}

// replayRecord applies one WAL record if its effect is missing (§7.3). It
// never fails because the target already exists or is gone: records for a
// database or collection that no longer exists, or that belong to an
// earlier incarnation of the name, are skipped. It reports whether it
// changed anything.
func (e *Engine) replayRecord(ctx context.Context, r wal.Record) (bool, error) {
	d := e.databases[r.DB]
	switch r.Op {
	case wal.OpCreateDB:
		if d != nil {
			return false, nil
		}
		return true, e.replayCreateDB(r)
	case wal.OpDropDB:
		if d == nil || d.walOffset >= r.Offset {
			return false, nil
		}
		return true, e.replayDropDB(d, r.Offset)
	}

	if d == nil {
		return false, nil
	}
	if _, ok := d.renamePending[r.Coll]; ok {
		// The name's directory holds a collection renamed to it whose
		// rename record comes later: records about the name before that
		// belong to earlier collections of the name, which were gone
		// before the rename (it requires a free name).
		return false, nil
	}
	c := d.collections[r.Coll]
	switch r.Op {
	case wal.OpCreateColl:
		if c != nil || r.Offset <= d.walOffset {
			return false, nil
		}
		return true, d.replayCreateColl(r)
	case wal.OpDropColl:
		if c == nil || c.metaOffset >= r.Offset {
			return false, nil
		}
		return true, d.replayDropColl(c, r.Offset)
	case wal.OpCreateIndex, wal.OpDropIndex:
		if c == nil || c.metaOffset >= r.Offset {
			return false, nil
		}
		return true, c.replayIndexOp(ctx, r)
	case wal.OpUpdateSchema:
		if c == nil || c.metaOffset >= r.Offset {
			return false, nil
		}
		return true, c.replayUpdateSchema(r)
	case wal.OpRenameColl:
		return d.replayRename(c, r)
	case wal.OpPutDoc, wal.OpDeleteDoc:
		if c == nil || r.Offset <= c.replayFloor() {
			return false, nil
		}
		return c.replayDoc(r)
	case wal.OpBatch:
		if c == nil || r.Offset <= c.replayFloor() {
			return false, nil
		}
		return c.replayBatch(r)
	default:
		return false, fmt.Errorf("%w: unknown operation %d", ErrWALCorrupt, r.Op)
	}
}

func (e *Engine) replayCreateDB(r wal.Record) error {
	if err := ValidateName("database", r.DB); err != nil {
		return err
	}
	dir := dbDir(e.root, r.DB)
	if err := makeDir(dir); err != nil {
		return err
	}
	if err := writeDBMeta(dir, dbMeta{Name: r.DB, SchemaVersion: CurrentSchemaVersion, WALOffset: r.Offset}); err != nil {
		return err
	}
	e.databases[r.DB] = newDatabase(e, r.DB, CurrentSchemaVersion, r.Offset)
	return nil
}

func (e *Engine) replayDropDB(d *Database, off uint64) error {
	for _, c := range d.collections {
		c.closeForReplayDrop()
	}
	if err := e.removeAway(d.dir, off); err != nil {
		return err
	}
	d.dropped = true
	delete(e.databases, d.name)
	e.cache.InvalidateDatabase(d.name)
	return nil
}

func (d *Database) replayCreateColl(r wal.Record) error {
	if err := ValidateName("collection", r.Coll); err != nil {
		return err
	}
	var p createCollPayload
	if err := json.Unmarshal(r.Payload, &p); err != nil {
		return fmt.Errorf("%w: decoding create_coll payload: %w", ErrWALCorrupt, err)
	}
	dir := collDir(d.e.root, d.name, r.Coll)
	if err := makeDir(dir); err != nil {
		return err
	}
	// A directory without coll.meta is what an interrupted create left: an
	// index file in it holds nothing.
	if err := idxstore.Remove(dir); err != nil {
		return err
	}
	st, err := idxstore.Create(dir, idxstore.Options{})
	if err != nil {
		return err
	}
	if err := writeCollMeta(dir, collMeta{Name: r.Coll, Schema: p.Schema, WALOffset: r.Offset, CreatedOffset: r.Offset}); err != nil {
		_ = st.Close() // the meta error is the one to report
		return err
	}
	c := newCollection(d, r.Coll, p.Schema, r.Offset, r.Offset)
	c.store = st
	d.collections[r.Coll] = c
	return nil
}

func (d *Database) replayDropColl(c *Collection, off uint64) error {
	c.closeForReplayDrop()
	if err := d.e.removeAway(c.dir, off); err != nil {
		return err
	}
	delete(d.collections, c.name)
	d.e.cache.InvalidateCollection(d.name, c.name)
	return nil
}

// closeForReplayDrop closes the files of a collection that replay drops
// and marks it dropped.
func (c *Collection) closeForReplayDrop() {
	if err := c.closeFiles(); err != nil {
		c.db.e.logger.Warn("cachydb: closing segments of a collection dropped by WAL replay",
			"database", c.db.name, "collection", c.name, "err", err)
	}
	c.mu.Lock()
	c.dropped = true
	c.releasePendingLocked()
	c.mu.Unlock()
}

// removeAway renames dir to its .deleted-<off> name and removes it. A
// failed removal is logged; Open removes the leftover next time.
func (e *Engine) removeAway(dir string, off uint64) error {
	moved, err := renameAway(dir, off)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(moved); err != nil {
		e.logger.Warn("cachydb: removing dropped directory; it will be retried on next open", "path", moved, "err", err)
	}
	return nil
}

// replayIndexOp applies a create_index or drop_index record whose effect
// coll.meta does not reflect yet. A created index is built in the index
// file from the documents its key table lists, unless the collection
// already has it on the same field; the replayed changes in the memtable
// reach it with their flush, which reads the replaced versions' keys from
// the data segments (forgetBaseLocked). A unique index is built without a
// uniqueness check (the record is committed) and its pending values are
// filled from the overlay. A dropped index loses its entries.
func (c *Collection) replayIndexOp(ctx context.Context, r wal.Record) error {
	defs := c.indexDefs()
	switch r.Op {
	case wal.OpCreateIndex:
		var def indexDef
		if err := json.Unmarshal(r.Payload, &def); err != nil {
			return fmt.Errorf("%w: decoding create_index payload: %w", ErrWALCorrupt, err)
		}
		if err := ValidateName("index", def.Name); err != nil {
			return err
		}
		if cur, ok := c.indexes[def.Name]; !ok || cur.Field != def.Field {
			st, err := c.indexStore()
			if err != nil {
				return err
			}
			if err := c.buildIndexes(ctx, st, []indexDef{def}); err != nil {
				return fmt.Errorf("building index %q: %w", def.Name, err)
			}
		}
		defs[def.Name] = def
		if err := c.writeMeta(defs, r.Offset); err != nil {
			return err
		}
		c.mu.Lock()
		c.indexes[def.Name] = def
		c.forgetBaseLocked(def.Name)
		delete(c.uniquePending, def.Name)
		c.fillUniquePendingLocked(def)
		c.metaOffset = r.Offset
		c.mu.Unlock()
	case wal.OpDropIndex:
		var p dropIndexPayload
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return fmt.Errorf("%w: decoding drop_index payload: %w", ErrWALCorrupt, err)
		}
		delete(defs, p.Name)
		if err := c.writeMeta(defs, r.Offset); err != nil {
			return err
		}
		c.mu.Lock()
		delete(c.indexes, p.Name)
		c.forgetBaseLocked(p.Name)
		delete(c.uniquePending, p.Name)
		c.metaOffset = r.Offset
		c.mu.Unlock()
		st, err := c.indexStore()
		if err != nil {
			return err
		}
		c.dropIndexEntries(st, p.Name)
	}
	return nil
}

// replayUpdateSchema applies an update_schema record whose effect
// coll.meta does not reflect yet.
func (c *Collection) replayUpdateSchema(r wal.Record) error {
	var p updateSchemaPayload
	if err := json.Unmarshal(r.Payload, &p); err != nil {
		return fmt.Errorf("%w: decoding update_schema payload: %w", ErrWALCorrupt, err)
	}
	if err := c.writeMetaWith(p.Schema, c.indexDefs(), r.Offset); err != nil {
		return err
	}
	c.mu.Lock()
	c.schema = p.Schema
	c.metaOffset = r.Offset
	c.mu.Unlock()
	return nil
}

// replayRename applies a rename_coll record (plan 004 §3.4) whose effect
// is missing; c is the collection currently under the record's old name
// (nil if none). The cases, in order:
//
//   - c's directory was already renamed but its coll.meta not rewritten
//     (a crash between the two): rewrite coll.meta and finish the rename;
//   - the new name already holds the renamed collection (or a later one):
//     the rename is applied, and a c created before the record is a copy
//     replay re-created from older records; it is removed, as the rename
//     removed the old name;
//   - c predates the record and the new name is free: rename the
//     directory, rewrite coll.meta, and re-key the collection;
//   - otherwise the record is obsolete.
func (d *Database) replayRename(c *Collection, r wal.Record) (bool, error) {
	var p renameCollPayload
	if err := json.Unmarshal(r.Payload, &p); err != nil {
		return false, fmt.Errorf("%w: decoding rename_coll payload: %w", ErrWALCorrupt, err)
	}
	if err := ValidateName("collection", p.NewName); err != nil {
		return false, err
	}
	switch t := d.collections[p.NewName]; {
	case c != nil && c.pendingRename == p.NewName:
		delete(d.renamePending, p.NewName)
		return true, d.finishRename(c, p.NewName, r.Offset)
	case t != nil && (t.metaOffset >= r.Offset || t.created > r.Offset):
		if c == nil || c.created >= r.Offset {
			return false, nil
		}
		return true, d.replayDropColl(c, r.Offset)
	case c != nil && c.created < r.Offset && c.metaOffset < r.Offset && t == nil:
		if _, busy := d.renamePending[p.NewName]; busy {
			// The new name's directory holds a later collection renamed
			// to it: this collection went away under the new name
			// before that, through records replay skips.
			return true, d.replayDropColl(c, r.Offset)
		}
		if err := c.detach(); err != nil {
			d.e.logger.Warn("cachydb: closing segments of a collection renamed by WAL replay",
				"database", d.name, "collection", c.name, "err", err)
		}
		newDir := collDir(d.e.root, d.name, p.NewName)
		if err := renameDir(c.dir, newDir); err != nil {
			return false, err
		}
		c.dir = newDir
		return true, d.finishRename(c, p.NewName, r.Offset)
	default:
		return false, nil
	}
}

// finishRename rewrites the coll.meta of c, whose directory is already
// the new name's, and registers it under the new name (recovery only).
func (d *Database) finishRename(c *Collection, newName string, off uint64) error {
	if err := c.detach(); err != nil {
		d.e.logger.Warn("cachydb: closing segments of a collection renamed by WAL replay",
			"database", d.name, "collection", c.name, "err", err)
	}
	delete(d.collections, c.name)
	nc := c.moveState(newName, c.dir, off)
	if err := nc.writeMeta(nc.indexDefs(), off); err != nil {
		return err
	}
	d.collections[newName] = nc
	return nil
}

// replayDoc applies a put_doc or delete_doc record to the memtable iff its
// offset is above the current version's (the memtable's, else the key
// table's), updating the count like a write and capturing the replaced
// version's index keys, read from its data segment (§7.3). Callers
// have checked the offset against the collection's replay floor.
//
// A delete of an id that is in neither is skipped: the delete is already
// in the index file, or the document never reached it. No record of the
// durable tombstones is needed for that: replay is sequential, so a put
// of the same id replayed before this record is in the memtable, and one
// replayed after it has a higher offset.
func (c *Collection) replayDoc(r wal.Record) (bool, error) {
	return c.replayItem(r.Op, r.ID, r.Payload, r.Offset, func(other string) bool { return other == r.ID })
}

// replayItem is replayDoc for one document change: op (put_doc or
// delete_doc) of the document id with the after-image doc, written at WAL
// offset off, by a single record or as an item of a batch record. The
// unique check that only logs leaves out the documents for which skip
// returns true (id itself, or every id of the batch).
func (c *Collection) replayItem(op wal.Op, id string, doc []byte, off uint64, skip func(string) bool) (bool, error) {
	if len(id) > maxIDLen {
		return false, fmt.Errorf("collection %q in database %q: the document id is %d bytes, longer than the limit of %d bytes; this version cannot store it",
			c.name, c.db.name, len(id), maxIDLen)
	}
	var next *Document
	if op == wal.OpPutDoc {
		var err error
		if next, err = decodeDocument(id, doc); err != nil {
			return false, err
		}
	}

	// The memtable first, then the key table (nothing is being flushed
	// during replay).
	c.mu.RLock()
	me, inMem := c.memtable[id]
	c.mu.RUnlock()
	var (
		curOff  uint64
		existed bool
		base    indexBase // the replaced version's keys (memEntry.base)
		baseOff uint64
		cur     *Document // the replaced version, when unique indexes need it
	)
	if inMem {
		curOff, existed, cur = me.walOffset, me.doc != nil, me.doc // applyLocked keeps the entry's base
	} else {
		ke, ok, err := c.storedEntry(id)
		if err != nil {
			return false, err
		}
		if !ok && next == nil {
			return false, nil
		}
		if ok {
			curOff, existed, baseOff = ke.WALOffset, true, ke.WALOffset
			if off > curOff && len(c.indexes) > 0 {
				prev, err := c.readStored(id, ke)
				if err != nil {
					return false, err
				}
				base = c.indexKeysAll(prev)
				cur = prev
			}
		}
	}
	if off <= curOff {
		return false, nil
	}
	// The record is committed: a unique violation (which the write path
	// prevents, so it means a damaged or foreign WAL) is applied anyway and
	// only logged.
	if vals := c.changedUniqueValues(cur, next); len(vals) > 0 {
		if err := c.checkUniqueExcept(skip, vals); err != nil {
			c.db.e.logger.Warn("cachydb: WAL replay applied a write that violates a unique index",
				"database", c.db.name, "collection", c.name, "id", id, "offset", off, "err", err)
		}
	}

	size := int64(len(id)+len(doc)) + memEntryOverhead
	c.mu.Lock()
	c.applyLocked(id, existed, base, baseOff, next, off, size)
	c.mu.Unlock()
	return true, nil
}
