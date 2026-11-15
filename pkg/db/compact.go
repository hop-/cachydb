package db

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// Compaction (§9; approach-index-and-query §7) reclaims the space of
// records the key table no longer references: superseded and deleted
// versions, tombstones, and the checkpoint markers of older versions.
//
// One background goroutine checks every collection each FlushInterval. A
// collection is compacted when its sealed segments hold at least 2
// segments and deadBytes/totalBytes >= CompactionRatio, or, failing that,
// when a run of at least smallRunMin adjacent sealed segments are each
// smaller than DataSegmentSize/4 (small segments are merged even when
// fully live; plan 002 C3). The byte accounting comes from the index file.
// The input run is a contiguous run of registered segments, never the
// active segment. The output is one new segment, id nextSeg (max
// registered + 1), fsynced (file and directory) before the swap.
//
// The live records of an input segment are found with a header-only scan
// (segment.ScanHeaders) and the key table: a put is live when its id's
// entry points at exactly that record. The swap is one index transaction:
// it repoints each copied id whose entry still has the old location and
// WAL offset (entries a concurrent flush changed meanwhile are left alone
// and their copies count as dead bytes of the output), registers the
// output, and removes the inputs. Only then are the old segments retired:
// their files are removed once the last in-flight read releases them. A
// crash before the transaction leaves an output file the index file does
// not register, and a crash after it leaves unregistered input files; Open
// deletes both (Collection.load).
//
// Ordering invariant: a superseded version is always in a segment whose id
// is at most that of the record superseding it. Flushes append to the
// active segment, which always has the highest id: before reserving the
// output id, compaction seals the active segment, so every later flush
// goes to a segment newer than the output. Given the invariant, a
// tombstone may be dropped exactly when every segment on disk with an id
// up to the run's last one is in the run — i.e. the run starts at the
// oldest existing segment — because then every older version of its
// document is in the run too and is not copied (the key table no longer
// references it). Otherwise tombstones are copied: an older version in a
// segment outside the run would come back if the index file were rebuilt
// from the segments. The check lists the files on disk, so a retired
// segment whose file is still held by a read (or failed to be removed)
// keeps tombstones until it is gone.

// compactBatchBytes is roughly how many document bytes compaction buffers
// before appending them to the output segment.
const compactBatchBytes = 1 << 20

// smallRunMin is the shortest run of small sealed segments that is merged
// even without dead bytes.
const smallRunMin = 4

// compactPlan describes one compaction of a collection.
type compactPlan struct {
	run       []uint32 // input segments, ascending
	out       uint32   // output segment id
	dropTombs bool     // the run starts at the oldest existing segment
}

// compactMove is a live put copied to the output segment.
type compactMove struct {
	id        string
	walOffset uint64
	old, new  segment.Location
}

// compactRecord is a record of an input segment that compaction copies.
type compactRecord struct {
	typ       segment.RecordType
	id        string
	walOffset uint64
	loc       segment.Location
}

// startCompactor starts the background compactor. Called once by Open,
// before the engine is returned, so Close cannot be waiting on bg yet.
func (e *Engine) startCompactor() {
	e.bg.Go(func() {
		e.compactLoop(e.bgCtx)
	})
}

// compactLoop runs a compaction pass every FlushInterval. Compactions run
// one at a time, on this goroutine only. It exits when ctx is cancelled
// (Close), or after a simulated crash (tests).
func (e *Engine) compactLoop(ctx context.Context) {
	t := time.NewTicker(e.opts.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := e.compactAll(ctx); errors.Is(err, errSimulatedCrash) {
			return
		}
	}
}

// compactAll compacts every collection that needs it. Failures are logged
// and joined; a failed compaction changes nothing and is retried by the
// next pass. It stops when ctx ends, and at once after a simulated crash.
func (e *Engine) compactAll(ctx context.Context) error {
	var errs []error
	for _, c := range e.allCollections() {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if _, err := c.compact(ctx); err != nil {
			if errors.Is(err, errSimulatedCrash) {
				return err
			}
			if ctx.Err() != nil {
				errs = append(errs, err) // shutting down: not worth an error log
				break
			}
			e.logger.Error("cachydb: compaction failed; it is retried later",
				"database", c.db.name, "collection", c.name, "err", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// compact compacts the collection once if it needs it (see the package
// comment above) and reports whether it did. An error before the swap
// leaves the collection unchanged and removes the partial output.
func (c *Collection) compact(ctx context.Context) (bool, error) {
	c.compactMu.Lock()
	defer c.compactMu.Unlock()
	plan, ok, err := c.planCompaction()
	if err != nil {
		return false, fmt.Errorf("compacting collection %q: %w", c.name, err)
	}
	if !ok {
		return false, nil
	}
	if err := c.runCompaction(ctx, plan); err != nil {
		return false, fmt.Errorf("compacting segments %v of collection %q: %w", plan.run, c.name, err)
	}
	return true, nil
}

// planCompaction picks the input run from the index file's segment
// accounting, seals the active segment, and reserves the output id.
// Callers hold compactMu.
func (c *Collection) planCompaction() (compactPlan, bool, error) {
	e := c.db.e
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.RLock()
	dropped := c.dropped
	c.mu.RUnlock()
	if dropped {
		return compactPlan{}, false, nil
	}
	// flushMu keeps the flusher out, so the accounting read here still
	// holds when the active segment is sealed below.
	segs, total, dead, err := c.compactionAccounting()
	if err != nil {
		return compactPlan{}, false, err
	}
	skip := map[uint32]bool{c.activeSeg: true}
	run := pickCompactionRun(segs, skip, total, dead, e.opts.CompactionRatio, e.opts.DataSegmentSize)
	if run == nil {
		run = pickSmallRun(segs, skip, total, dead, e.opts.DataSegmentSize)
	}
	if run == nil {
		return compactPlan{}, false, nil
	}

	onDisk, err := segment.ListSegments(c.dir)
	if err != nil {
		return compactPlan{}, false, err
	}
	_, registered := total[c.activeSeg]
	c.sealForCompaction(registered)
	out := c.nextSeg
	c.nextSeg++
	return compactPlan{run: run, out: out, dropTombs: coversOlderVersions(run, onDisk)}, true, nil
}

// compactionAccounting returns what compaction planning works with: the
// collection's registered data segments in ascending order, and per
// segment its record bytes and the dead bytes a compaction could reclaim.
// The numbers come from the index file. The index file counts every
// tombstone as dead; the tombstones a compaction of this process had to
// copy (keptTombs) are taken out again, because compacting their segment
// once more would only copy them once more.
// Callers hold no collection lock other than flushMu or compactMu.
func (c *Collection) compactionAccounting() (segs []uint32, total, dead map[uint32]int64, err error) {
	st, err := c.indexStore()
	if err != nil {
		return nil, nil, nil, err
	}
	var stats []idxstore.SegmentStats
	err = st.View(func(tx *idxstore.Tx) (err error) {
		stats, err = tx.Segments()
		return err
	})
	if err != nil {
		return nil, nil, nil, c.storeErr(err)
	}
	segs = make([]uint32, len(stats))
	total = make(map[uint32]int64, len(stats))
	dead = make(map[uint32]int64, len(stats))
	c.mu.RLock()
	defer c.mu.RUnlock()
	for i, s := range stats {
		segs[i], total[s.Seg] = s.Seg, s.RecordBytes
		dead[s.Seg] = max(s.DeadBytes-c.keptTombs[s.Seg], 0)
	}
	return segs, total, dead, nil
}

// pickCompactionRun returns the input run for a compaction, or nil if none
// is needed. segs lists the live segments in ascending order; segments in
// skip (the active one) are not sealed candidates and split runs. A compaction is needed when there are
// at least 2 candidates and their dead bytes reach ratio of their total
// bytes. The run is the contiguous run of candidates with the most dead
// bytes — the oldest one on ties — whose live bytes fit in maxLive (one
// output segment), or a single segment if none fits.
func pickCompactionRun(segs []uint32, skip map[uint32]bool, total, dead map[uint32]int64, ratio float64, maxLive int64) []uint32 {
	var n int
	var sumTotal, sumDead int64
	for _, s := range segs {
		if !skip[s] {
			n++
			sumTotal += total[s]
			sumDead += dead[s]
		}
	}
	if n < 2 || sumTotal <= 0 || float64(sumDead) < ratio*float64(sumTotal) {
		return nil
	}
	live := func(s uint32) int64 { return max(total[s]-dead[s], 0) }

	var best []uint32
	var bestDead int64
	for i := range segs {
		var cumLive, cumDead int64
		for j := i; j < len(segs) && !skip[segs[j]]; j++ {
			if j > i && cumLive+live(segs[j]) > maxLive {
				break
			}
			cumLive += live(segs[j])
			cumDead += dead[segs[j]]
			if cumDead > bestDead {
				best, bestDead = segs[i:j+1], cumDead
			}
		}
	}
	return slices.Clone(best)
}

// pickSmallRun returns the oldest run of at least smallRunMin adjacent
// candidate segments (segs ascending; skip splits runs, as in
// pickCompactionRun) that are each smaller than segSize/4, extended while
// the merged live bytes fit in segSize; nil if there is none.
func pickSmallRun(segs []uint32, skip map[uint32]bool, total, dead map[uint32]int64, segSize int64) []uint32 {
	small := func(s uint32) bool { return !skip[s] && total[s] < segSize/4 }
	live := func(s uint32) int64 { return max(total[s]-dead[s], 0) }
	for i := range segs {
		if !small(segs[i]) {
			continue
		}
		var cumLive int64
		j := i
		for ; j < len(segs) && small(segs[j]); j++ {
			if j > i && cumLive+live(segs[j]) > segSize {
				break
			}
			cumLive += live(segs[j])
		}
		if j-i >= smallRunMin {
			return slices.Clone(segs[i:j])
		}
	}
	return nil
}

// coversOlderVersions reports whether tombstones may be dropped when
// compacting run: every segment on disk with an id up to the run's last
// one is in the run (see the ordering invariant above).
func coversOlderVersions(run, onDisk []uint32) bool {
	last := run[len(run)-1]
	for _, s := range onDisk {
		if s <= last && !slices.Contains(run, s) {
			return false
		}
	}
	return true
}

// sealForCompaction seals the active segment, if it is not sealed yet, so
// that every later flush goes to a segment newer than the compaction
// output (the ordering invariant). The next flush creates the new active
// segment. An active segment that no flush has written to (registered is
// false: the index file does not know it) is removed instead of being kept
// as an empty sealed segment. Failures are logged: the data is already
// durable, and a leftover file that is not registered is removed on open.
// Callers hold flushMu.
func (c *Collection) sealForCompaction(registered bool) {
	if c.activeSeg == 0 || c.activeSealed {
		return
	}
	e := c.db.e
	seg := c.activeSeg
	path := segment.DataPath(c.dir, seg)
	if w := c.active; w != nil {
		c.active = nil
		if err := w.Close(); err != nil {
			e.logger.Warn("cachydb: closing data segment sealed for compaction", "file", path, "err", err)
		}
	}
	c.activeSealed = true
	if !registered {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			e.logger.Warn("cachydb: removing empty data segment", "file", path, "err", err)
		}
	}
}

// runCompaction copies the live records of plan.run into segment plan.out,
// swaps the key table over in one index transaction, and retires the old
// segments. Callers hold compactMu.
func (c *Collection) runCompaction(ctx context.Context, plan compactPlan) error {
	e := c.db.e
	st, err := c.indexStore()
	if err != nil {
		return err
	}
	w, err := segment.Create(c.dir, plan.out)
	if err != nil {
		return err
	}
	open := true
	fail := func(err error) error {
		if open {
			_ = w.Close() // already failing; the output is removed below
		}
		c.removeSegmentFiles(plan.out, nil)
		return err
	}

	var (
		moves     []compactMove
		records   int
		outBytes  int64
		tombBytes int64 // copied tombstones: dead bytes of the output from the start
		batch     []segment.Record
		batchSrc  []compactRecord
		pending   int
	)
	appendBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		locs, err := w.Append(batch)
		if err != nil {
			return err
		}
		for i, loc := range locs {
			src := batchSrc[i]
			outBytes += int64(loc.Size)
			records++
			if src.typ == segment.TypePut {
				moves = append(moves, compactMove{id: src.id, walOffset: src.walOffset, old: src.loc, new: loc})
			} else {
				tombBytes += int64(loc.Size)
			}
		}
		batch, batchSrc, pending = batch[:0], batchSrc[:0], 0
		return nil
	}

	for _, seg := range plan.run {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		keep, err := c.liveEntries(st, seg, plan.dropTombs)
		if err != nil {
			return fail(err)
		}
		if len(keep) == 0 {
			continue
		}
		r, err := segment.OpenReader(c.dir, seg)
		if err != nil {
			return fail(err)
		}
		for _, en := range keep {
			rec := segment.Record{Type: en.typ, WALOffset: en.walOffset, ID: en.id}
			if en.typ == segment.TypePut {
				if rec, err = r.Read(en.loc); err != nil {
					break
				}
				if rec.Type != segment.TypePut || rec.ID != en.id || rec.WALOffset != en.walOffset {
					err = fmt.Errorf("%w: record at segment %d byte %d is %s %q@%d, the scan said put %q@%d",
						segment.ErrCorrupt, seg, en.loc.Pos, rec.Type, rec.ID, rec.WALOffset, en.id, en.walOffset)
					break
				}
			}
			batch = append(batch, rec)
			batchSrc = append(batchSrc, en)
			if pending += len(rec.Doc) + len(rec.ID); pending >= compactBatchBytes {
				if err = appendBatch(); err != nil {
					break
				}
				if err = ctx.Err(); err != nil {
					break
				}
			}
		}
		if cerr := r.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			return fail(err)
		}
	}
	if err := appendBatch(); err != nil {
		return fail(err)
	}

	// Make the output durable — data and directory entry — before
	// anything references it.
	open = false
	if err := w.Close(); err != nil {
		return fail(err)
	}
	if err := fsutil.SyncDir(c.dir); err != nil {
		return fail(err)
	}
	if err := e.hook(hookCompactBeforeSwap); err != nil {
		return err // simulated crash: no cleanup
	}

	// Swap: one index transaction, in ascending id order (see applyFlush).
	// No collection lock is held; a flush that commits before it changes
	// the entries the compare-and-swap then leaves alone.
	slices.SortFunc(moves, func(a, b compactMove) int { return cmp.Compare(a.id, b.id) })
	var lost int64
	committing := false
	err = st.Update(func(u *idxstore.WTx) error {
		lost, committing = 0, false
		for _, m := range moves {
			cur, ok, err := u.Key(m.id)
			if err != nil {
				return err
			}
			if !ok || cur != locationEntry(m.old, m.walOffset) {
				lost += int64(m.new.Size) // changed since it was copied
				continue
			}
			if err := u.PutKey(m.id, locationEntry(m.new, m.walOffset)); err != nil {
				return err
			}
		}
		if err := u.SetSegment(idxstore.SegmentStats{Seg: plan.out, RecordBytes: outBytes, DeadBytes: lost + tombBytes}); err != nil {
			return err
		}
		for _, s := range plan.run {
			if err := u.DeleteSegment(s); err != nil {
				return err
			}
		}
		committing = true
		return nil
	})
	if err != nil {
		err = c.storeErr(err)
		if committing && c.maybeRegistered(st, plan.out) {
			// The commit failed late: bbolt may already have written the
			// transaction, so the key table may point into the output (see
			// afterFailedIndexCommit). Keep every file, inputs and output;
			// the next Open removes whichever side the index file does not
			// register.
			return fmt.Errorf("%w (the output segment %d and the input segments are kept until the next open)", err, plan.out)
		}
		return fail(err)
	}

	// Retire the inputs (memory only), then remove their files.
	c.mu.Lock()
	if tombBytes > 0 {
		c.keptTombs[plan.out] = tombBytes
	}
	readers := make(map[uint32]*segment.Reader)
	for _, s := range plan.run {
		delete(c.keptTombs, s)
		c.retired[s] = struct{}{}
		if r, ok := c.readers[s]; ok {
			readers[s] = r
			delete(c.readers, s)
		}
	}
	c.mu.Unlock()

	for _, s := range plan.run {
		c.removeSegmentFiles(s, readers[s])
	}

	e.logger.Debug("cachydb: compacted data segments",
		"database", c.db.name, "collection", c.name, "segments", plan.run, "output", plan.out,
		"output_bytes", outBytes, "records", records, "tombstones_dropped", plan.dropTombs)
	return nil
}

// maybeRegistered reports whether the index file may register data segment
// seg: true when it does, or when that cannot be read.
func (c *Collection) maybeRegistered(st *idxstore.Store, seg uint32) bool {
	registered := true
	err := st.View(func(tx *idxstore.Tx) (err error) {
		_, registered, err = tx.Segment(seg)
		return err
	})
	return err != nil || registered
}

// liveEntries returns the records of segment seg that compaction copies:
// puts whose key-table entry points at exactly this record, and tombstones
// unless dropTombs. Checkpoint markers (in segments written by older
// versions) are never copied. The records come from a header-only scan,
// which verifies every checksum but decompresses nothing.
func (c *Collection) liveEntries(st *idxstore.Store, seg uint32, dropTombs bool) ([]compactRecord, error) {
	var recs []compactRecord
	_, err := segment.ScanHeaders(c.dir, seg, func(r segment.Record, loc segment.Location) error {
		if r.Type == segment.TypePut || (r.Type == segment.TypeTombstone && !dropTombs) {
			recs = append(recs, compactRecord{typ: r.Type, id: r.ID, walOffset: r.WALOffset, loc: loc})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	keep := recs[:0]
	err = st.View(func(tx *idxstore.Tx) error {
		for _, en := range recs {
			if en.typ == segment.TypePut {
				ke, ok, err := tx.Key(en.id)
				if err != nil {
					return err
				}
				if !ok || ke != locationEntry(en.loc, en.walOffset) {
					continue
				}
			}
			keep = append(keep, en)
		}
		return nil
	})
	if err != nil {
		return nil, c.storeErr(err)
	}
	return keep, nil
}

// removeSegmentFiles deletes the data file of segment seg: at once, or
// through r (an open reader of it) once the last read releases it. A hint
// file an older version wrote for the segment goes first. Errors are
// logged; a leftover data file is not registered in the index file, and
// Open removes it.
func (c *Collection) removeSegmentFiles(seg uint32, r *segment.Reader) {
	logger := c.db.e.logger
	logErr := func(err error) {
		logger.Warn("cachydb: removing compacted data segment", "database", c.db.name, "collection", c.name, "err", err)
	}
	remove := func(path string) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			logErr(err)
		}
	}
	remove(segment.HintPath(c.dir, seg))
	if r != nil {
		r.RetireWith(logErr)
		return
	}
	remove(segment.DataPath(c.dir, seg))
}
