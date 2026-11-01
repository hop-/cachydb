package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// Compaction (§9) reclaims the space of records the keydir no longer
// references: superseded and deleted versions, tombstones, and checkpoint
// markers.
//
// One background goroutine checks every collection each FlushInterval. A
// collection is compacted when its sealed segments hold at least 2
// segments and deadBytes/totalBytes >= CompactionRatio. The input run is a
// contiguous run of sealed segments, never the active segment nor the
// segment holding the newest checkpoint marker (markers are not copied, so
// that one must survive). The output is one new segment, id nextSeg (max
// existing + 1), written with its hint file and fsynced (file and
// directory) before the swap. The swap repoints, under Collection.mu, only
// the keydir entries that still reference the copied (seg, pos) and WAL
// offset (compare-and-swap); entries a concurrent flush changed meanwhile
// are left alone and their copies count as dead bytes of the output. The
// old segments are then retired: their hint files are removed at once,
// their data files once the last in-flight read releases them. Index
// snapshots are rewritten afterwards.
//
// Ordering invariant: a superseded version is always in a segment whose id
// is at most that of the record superseding it. Flushes append to the
// active segment, which always has the highest id: before reserving the
// output id, compaction seals the active segment, so every later flush
// goes to a segment newer than the output. Given the invariant, a
// tombstone may be dropped exactly when every segment on disk with an id
// up to the run's last one is in the run — i.e. the run starts at the
// oldest existing segment — because then every older version of its
// document is in the run too and is not copied (the keydir no longer
// references it). Otherwise tombstones are copied: an older version in a
// segment outside the run would come back on the next Open. The check
// lists the files on disk, so a retired segment whose file is still held
// by a read (or failed to be removed) keeps tombstones until it is gone.

// compactBatchBytes is roughly how many document bytes compaction buffers
// before appending them to the output segment.
const compactBatchBytes = 1 << 20

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

// startCompactor starts the background compactor. Called once by Open,
// before the engine is returned, so Close cannot be waiting on bg yet.
func (e *Engine) startCompactor() {
	e.bg.Add(1)
	go func() {
		defer e.bg.Done()
		e.compactLoop(e.bgCtx)
	}()
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

// planCompaction picks the input run, seals the active segment, and
// reserves the output id. Callers hold compactMu.
func (c *Collection) planCompaction() (compactPlan, bool, error) {
	e := c.db.e
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.RLock()
	if c.dropped {
		c.mu.RUnlock()
		return compactPlan{}, false, nil
	}
	segs := slices.Sorted(maps.Keys(c.segBytes))
	skip := map[uint32]bool{c.activeSeg: true, c.markerSeg: true}
	run := pickCompactionRun(segs, skip, c.segBytes, c.deadBytes, e.opts.CompactionRatio, e.opts.DataSegmentSize)
	c.mu.RUnlock()
	if run == nil {
		return compactPlan{}, false, nil
	}

	onDisk, err := segment.ListSegments(c.dir)
	if err != nil {
		return compactPlan{}, false, err
	}
	c.sealForCompaction()
	out := c.nextSeg
	c.nextSeg++
	return compactPlan{run: run, out: out, dropTombs: coversOlderVersions(run, onDisk)}, true, nil
}

// pickCompactionRun returns the input run for a compaction, or nil if none
// is needed. segs lists the live segments in ascending order; segments in
// skip (the active one, the one with the newest checkpoint marker) are not
// sealed candidates and split runs. A compaction is needed when there are
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
// segment. An active segment without records is removed instead of being
// kept as an empty sealed segment. Failures are logged: the data is
// already durable, a missing hint is rebuilt on open, and a leftover empty
// file is harmless. Callers hold flushMu.
func (c *Collection) sealForCompaction() {
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
	if len(c.activeHint) == 0 {
		c.mu.Lock()
		delete(c.segBytes, seg)
		c.mu.Unlock()
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			e.logger.Warn("cachydb: removing empty data segment", "file", path, "err", err)
		}
		return
	}
	if err := segment.WriteHint(c.dir, seg, c.activeHint); err != nil {
		e.logger.Warn("cachydb: writing hint file; the segment will be scanned on open", "file", path, "err", err)
	}
	c.activeHint = nil
}

// runCompaction copies the live records of plan.run into segment plan.out,
// swaps the keydir, retires the old segments, and rewrites the index
// snapshots. Callers hold compactMu.
func (c *Collection) runCompaction(ctx context.Context, plan compactPlan) error {
	e := c.db.e
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
		moves    []compactMove
		hint     []segment.HintEntry
		outBytes int64
		batch    []segment.Record
		batchSrc []segment.HintEntry
		pending  int
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
			hint = append(hint, segment.HintEntry{ID: src.ID, Type: src.Type, WALOffset: src.WALOffset, Loc: loc})
			if src.Type == segment.TypePut {
				moves = append(moves, compactMove{id: src.ID, walOffset: src.WALOffset, old: src.Loc, new: loc})
			}
		}
		batch, batchSrc, pending = batch[:0], batchSrc[:0], 0
		return nil
	}

	for _, seg := range plan.run {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		keep, err := c.liveEntries(seg, plan.dropTombs)
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
			rec := segment.Record{Type: en.Type, WALOffset: en.WALOffset, ID: en.ID}
			if en.Type == segment.TypePut {
				if rec, err = r.Read(en.Loc); err != nil {
					break
				}
				if rec.Type != segment.TypePut || rec.ID != en.ID || rec.WALOffset != en.WALOffset {
					err = fmt.Errorf("%w: record at segment %d byte %d is %s %q@%d, hint says put %q@%d",
						segment.ErrCorrupt, seg, en.Loc.Pos, rec.Type, rec.ID, rec.WALOffset, en.ID, en.WALOffset)
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

	// Make the output durable — data, hint, directory entries — before
	// anything references it.
	open = false
	if err := w.Close(); err != nil {
		return fail(err)
	}
	if err := segment.WriteHint(c.dir, plan.out, hint); err != nil {
		return fail(err)
	}
	if err := fsutil.SyncDir(c.dir); err != nil {
		return fail(err)
	}
	if err := e.hook(hookCompactBeforeSwap); err != nil {
		return err // simulated crash: no cleanup
	}

	// Swap (memory only).
	c.mu.Lock()
	var lost int64
	for _, m := range moves {
		if ke, ok := c.keydir[m.id]; ok && ke.walOffset == m.walOffset && ke.loc == m.old {
			c.keydir[m.id] = keyEntry{loc: m.new, walOffset: m.walOffset}
		} else {
			lost += int64(m.new.Size) // changed since it was copied
		}
	}
	c.segBytes[plan.out] = outBytes
	if lost > 0 {
		c.deadBytes[plan.out] = lost
	}
	readers := make(map[uint32]*segment.Reader)
	for _, s := range plan.run {
		delete(c.segBytes, s)
		delete(c.deadBytes, s)
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

	c.flushMu.Lock()
	c.writeSnapshotsAfterCompaction()
	c.flushMu.Unlock()

	e.logger.Debug("cachydb: compacted data segments",
		"database", c.db.name, "collection", c.name, "segments", plan.run, "output", plan.out,
		"output_bytes", outBytes, "records", len(hint), "tombstones_dropped", plan.dropTombs)
	return nil
}

// liveEntries returns the records of segment seg that compaction copies:
// puts the keydir still references at exactly this location, and
// tombstones unless dropTombs. Checkpoint markers are never copied. The
// records come from the segment's hint file, or a scan if it has none.
func (c *Collection) liveEntries(seg uint32, dropTombs bool) ([]segment.HintEntry, error) {
	entries, err := segment.ReadHint(c.dir, seg)
	if err == nil {
		err = checkHintFits(c.dir, seg, entries)
	}
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			c.db.e.logger.Warn("cachydb: ignoring invalid hint file; scanning the segment",
				"file", segment.DataPath(c.dir, seg), "err", err)
		}
		if entries, err = scanHint(c.dir, seg); err != nil {
			return nil, err
		}
	}
	keep := entries[:0]
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, en := range entries {
		switch en.Type {
		case segment.TypePut:
			if ke, ok := c.keydir[en.ID]; ok && ke.loc == en.Loc {
				keep = append(keep, en)
			}
		case segment.TypeTombstone:
			if !dropTombs {
				keep = append(keep, en)
			}
		}
	}
	return keep, nil
}

// removeSegmentFiles deletes the hint file of segment seg and then its data
// file: at once, or through r (an open reader of it) once the last read
// releases it. Errors are logged; a leftover data file only holds records
// that also exist elsewhere, and is loaded as a normal segment on open.
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
	// Hint first: a data file left without its hint is scanned on open,
	// while a hint left without its data file is ignored.
	remove(segment.HintPath(c.dir, seg))
	if r != nil {
		r.RetireWith(logErr)
		return
	}
	remove(segment.DataPath(c.dir, seg))
}

// writeSnapshotsAfterCompaction rewrites the index snapshots so they cover
// the newest segment: snapshots covering a retired segment are useless,
// and output records count as "after" an older covered position. Callers
// hold flushMu, so no flush changes the segments meanwhile.
func (c *Collection) writeSnapshotsAfterCompaction() {
	c.mu.RLock()
	newest := slices.Max(slices.Collect(maps.Keys(c.segBytes)))
	c.mu.RUnlock()
	var size int64
	if c.active != nil && c.activeSeg == newest {
		size = c.active.Size()
	} else {
		fi, err := os.Stat(segment.DataPath(c.dir, newest))
		if err != nil {
			c.db.e.logger.Warn("cachydb: writing index snapshots after compaction; the indexes will be rebuilt on open",
				"collection", c.name, "err", err)
			return
		}
		size = fi.Size()
	}
	c.writeIndexSnapshots(newest, uint64(size))
}
