package db

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Background flush (§5.4), checkpoint markers, and the low-water mark
// (§7.1).
//
// A flush swaps a collection's memtable into the immutable flushing
// table, appends the changed documents and tombstones (sorted by WAL
// offset) plus one checkpoint marker to the active data segment, fsyncs
// once, and then repoints the keydir. Collection.mu is held only for the
// two memory-only swaps, never during segment I/O. A failed flush keeps
// flushing (reads still see it) and the old checkpoint, and the next
// flush retries with flushing ∪ memtable.

// segmentWriter is the part of *segment.Writer the flusher uses. Tests
// wrap it (Options.testSegWriter) to inject slow or failing I/O.
type segmentWriter interface {
	Append(recs []segment.Record) ([]segment.Location, error)
	Sync() error
	Size() int64
	Seg() uint32
	Close() error
}

// lowWaterMark returns the smallest WAL offset that is not yet processed,
// i.e. not yet durable in data segments or metadata files (§7.1):
//
//	lowWater = min(pending..., next)
//
// next is wal.NextOffset(), one past the last durable record; every record
// at or above it is unprocessed by definition. pending holds, per source
// that still has unprocessed records, a lower bound of its smallest
// offset: a collection's memtable, flushing table and inflight writes, and
// in-flight metadata operations. Sources without pending records are
// simply left out. Lower bounds are safe: a smaller result only delays WAL
// retention.
func lowWaterMark(next uint64, pending ...uint64) uint64 {
	lw := next
	for _, p := range pending {
		lw = min(lw, p)
	}
	return lw
}

// checkpointBelow returns the checkpoint for low-water mark lw: the
// largest offset such that every record at or below it is processed. WAL
// offsets start at 1, so lw >= 1 in practice; 0 maps to 0 ("nothing").
func checkpointBelow(lw uint64) uint64 {
	if lw == 0 {
		return 0
	}
	return lw - 1
}

// flushCheckpointLocked returns the checkpoint marker value for a flush
// whose flushing table was just swapped in (§7.1): one below the smallest
// offset of this collection's records that the flush does not cover. The
// memtable is empty at this point, so those are the inflight writes, and
// everything else is below wal.NextOffset(). Callers hold mu exclusively.
//
// Why this never covers a record the flush does not write: a document
// write registers its inflight lower bound under mu before its WAL append,
// and moves from inflight to the memtable under mu. A record of this
// collection with an offset below NextOffset is durable, so it was
// registered before this critical section began; it is therefore either
// inflight (bounding cp), in the flushing table being written, or already
// in a data segment. A record not yet registered gets an offset of at
// least NextOffset, which only grows.
func (c *Collection) flushCheckpointLocked() uint64 {
	next := c.db.e.wal.NextOffset()
	if len(c.inflight) > 0 {
		return checkpointBelow(lowWaterMark(next, c.inflight[0]))
	}
	return checkpointBelow(lowWaterMark(next))
}

// pendingMinLocked returns a lower bound of the smallest WAL offset of the
// collection's unprocessed records (memtable, flushing, inflight), or
// false if it has none. A dropped collection has none: its records are
// processed by the drop. Callers hold mu.
func (c *Collection) pendingMinLocked() (uint64, bool) {
	if c.dropped {
		return 0, false
	}
	var pending []uint64
	if len(c.memtable) > 0 {
		pending = append(pending, c.memMin)
	}
	if c.flushing != nil {
		pending = append(pending, c.flushingMin)
	}
	if len(c.inflight) > 0 {
		pending = append(pending, c.inflight[0])
	}
	if len(pending) == 0 {
		return 0, false
	}
	return slices.Min(pending), true
}

// lowWater returns the engine's low-water mark (§7.1), which drives WAL
// retention: WAL segments entirely below it may be deleted. It is safe to
// call from any goroutine that holds no engine lock.
func (e *Engine) lowWater() uint64 {
	// Read the next offset FIRST. Every record below it is durable and was
	// registered (inflight or metadata) before its append, so the scan
	// below sees it unless it is already processed. Reading it after the
	// scan could count a record that was registered and committed in
	// between as processed.
	next := e.wal.NextOffset()
	var pending []uint64
	e.lwMu.Lock()
	if len(e.metaInflight) > 0 {
		pending = append(pending, e.metaInflight[0])
	}
	e.lwMu.Unlock()
	for _, c := range e.allCollections() {
		c.mu.RLock()
		m, ok := c.pendingMinLocked()
		c.mu.RUnlock()
		if ok {
			pending = append(pending, m)
		}
	}
	return lowWaterMark(next, pending...)
}

// appendMetaWAL appends a metadata record (the commit point of a metadata
// operation) and keeps it unprocessed for the low-water mark until the
// caller calls done, once the record's effect is durable in the meta files
// or directories. A caller whose post-commit file step fails does not call
// done: the record then stays pinned in the WAL until restart, so replay
// can still apply it.
func (e *Engine) appendMetaWAL(ctx context.Context, r wal.Record) (off uint64, done func(), err error) {
	e.lwMu.Lock()
	lb := e.wal.NextOffset() // lower bound of r's offset, as in registerInflight
	i, _ := slices.BinarySearch(e.metaInflight, lb)
	e.metaInflight = slices.Insert(e.metaInflight, i, lb)
	e.lwMu.Unlock()

	release := func() {
		e.lwMu.Lock()
		defer e.lwMu.Unlock()
		if i, ok := slices.BinarySearch(e.metaInflight, lb); ok {
			e.metaInflight = slices.Delete(e.metaInflight, i, i+1)
		}
	}
	off, err = e.appendWAL(ctx, r)
	if err != nil {
		release()
		return 0, nil, err
	}
	return off, release, nil
}

// allCollections returns every collection of every live database. It
// takes Engine.mu and each Database.mu only while copying.
func (e *Engine) allCollections() []*Collection {
	e.mu.RLock()
	dbs := slices.Collect(maps.Values(e.databases))
	e.mu.RUnlock()
	var out []*Collection
	for _, d := range dbs {
		d.mu.RLock()
		if !d.dropped {
			for _, c := range d.collections {
				out = append(out, c)
			}
		}
		d.mu.RUnlock()
	}
	return out
}

// kickFlush asks the flusher to flush collections whose memtable exceeds
// MemtableSize. It never blocks.
func (e *Engine) kickFlush() {
	select {
	case e.flushKick <- struct{}{}:
	default: // a kick is already pending
	}
}

// startFlusher starts the background flusher. Called once by Open, before
// the engine is returned, so Close cannot be waiting on bg yet.
func (e *Engine) startFlusher() {
	e.bg.Add(1)
	go func() {
		defer e.bg.Done()
		e.flushLoop(e.bgCtx)
	}()
}

// flushLoop flushes every collection each FlushInterval and the
// collections over MemtableSize when kicked. It exits when ctx is
// cancelled (Close), or after a simulated crash (tests), as a dead process
// would.
func (e *Engine) flushLoop(ctx context.Context) {
	t := time.NewTicker(e.opts.FlushInterval)
	defer t.Stop()
	for {
		onlyFull := false
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-e.flushKick:
			onlyFull = true
		}
		if err := e.flushAll(ctx, onlyFull); errors.Is(err, errSimulatedCrash) {
			return
		}
	}
}

// flushAll flushes every collection (only those whose memtable exceeds
// MemtableSize if onlyFull). Failures are logged and joined; the failed
// collections keep their data in memory and are retried by the next pass.
// It stops between collections when ctx ends, and at once after a
// simulated crash.
func (e *Engine) flushAll(ctx context.Context, onlyFull bool) error {
	var errs []error
	for _, c := range e.allCollections() {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if onlyFull && !c.memtableFull() {
			continue
		}
		if err := c.flush(); err != nil {
			if errors.Is(err, errSimulatedCrash) {
				return err
			}
			e.logger.Error("cachydb: flush failed; the changes stay in memory and the WAL and are retried",
				"database", c.db.name, "collection", c.name, "err", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Collection) memtableFull() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.memBytes > c.db.e.opts.MemtableSize
}

// flushEntry is one memtable entry being flushed.
type flushEntry struct {
	id string
	me memEntry
}

// flush writes the collection's pending changes to its active data segment
// (§5.4) and, if the segment reached DataSegmentSize, seals it and rolls
// to a new one. It returns nil when there is nothing to flush or the
// collection was dropped.
func (c *Collection) flush() error {
	e := c.db.e
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	// 1. Swap (memory only).
	c.mu.Lock()
	if c.dropped || (len(c.memtable) == 0 && c.flushing == nil) {
		c.mu.Unlock()
		return nil
	}
	switch {
	case c.flushing == nil:
		c.flushing, c.flushingMin = c.memtable, c.memMin
	case len(c.memtable) > 0:
		// Retry after a failed flush: flushing ∪ memtable, newer wins.
		merged := maps.Clone(c.flushing)
		for id, me := range c.memtable {
			if old, ok := merged[id]; !ok || me.walOffset > old.walOffset {
				merged[id] = me
			}
		}
		c.flushing, c.flushingMin = merged, min(c.flushingMin, c.memMin)
	}
	c.memtable, c.memMin, c.memBytes = make(map[string]memEntry), 0, 0
	cp := c.flushCheckpointLocked()
	flushing := c.flushing // immutable while set; only the flusher replaces it
	c.mu.Unlock()

	// 2. Write the records and the checkpoint marker, one fsync.
	entries, recs, err := flushRecords(flushing, cp)
	if err != nil {
		return fmt.Errorf("flushing collection %q: %w", c.name, err)
	}
	if err := c.ensureActive(); err != nil {
		return fmt.Errorf("flushing collection %q: %w", c.name, err)
	}
	w := c.active
	pre := w.Size()
	locs, err := w.Append(recs)
	if err != nil {
		c.abandonActive(pre)
		return fmt.Errorf("flushing collection %q: %w", c.name, err)
	}
	if err := e.hook(hookAfterSegWrite); err != nil {
		return err // simulated crash: no cleanup
	}
	if err := w.Sync(); err != nil {
		c.abandonActive(pre)
		return fmt.Errorf("flushing collection %q: %w", c.name, err)
	}
	if err := e.hook(hookAfterSegSync); err != nil {
		return err // simulated crash: no cleanup
	}

	// 3. Repoint the keydir (memory only).
	c.mu.Lock()
	for i, en := range entries {
		loc := locs[i]
		old, had := c.keydir[en.id]
		if had && old.walOffset >= en.me.walOffset {
			c.deadBytes[loc.Seg] += int64(loc.Size) // already superseded on disk
			continue
		}
		if had {
			c.deadBytes[old.loc.Seg] += int64(old.loc.Size)
		}
		if en.me.doc == nil {
			delete(c.keydir, en.id)
			c.deadBytes[loc.Seg] += int64(loc.Size) // tombstones are never referenced
		} else {
			c.keydir[en.id] = keyEntry{loc: loc, walOffset: en.me.walOffset}
		}
	}
	marker := locs[len(locs)-1]
	c.deadBytes[marker.Seg] += int64(marker.Size)
	c.flushing, c.flushingMin = nil, 0
	c.checkpoint = cp
	c.mu.Unlock()

	for i := range recs {
		c.activeHint = append(c.activeHint, segment.HintEntry{
			ID: recs[i].ID, Type: recs[i].Type, WALOffset: recs[i].WALOffset, Loc: locs[i],
		})
	}

	// 4. Roll the segment once it is full.
	if w.Size() >= e.opts.DataSegmentSize {
		c.sealActive()
	}
	return nil
}

// flushRecords returns the flushing entries sorted by WAL offset and the
// segment records to append for them: one put or tombstone per entry, in
// the same order, then the checkpoint marker with value cp.
func flushRecords(flushing map[string]memEntry, cp uint64) ([]flushEntry, []segment.Record, error) {
	entries := make([]flushEntry, 0, len(flushing))
	for id, me := range flushing {
		entries = append(entries, flushEntry{id: id, me: me})
	}
	slices.SortFunc(entries, func(a, b flushEntry) int { return cmp.Compare(a.me.walOffset, b.me.walOffset) })

	recs := make([]segment.Record, 0, len(entries)+1)
	for _, en := range entries {
		if en.me.doc == nil {
			recs = append(recs, segment.Record{Type: segment.TypeTombstone, WALOffset: en.me.walOffset, ID: en.id})
			continue
		}
		data, err := json.Marshal(en.me.doc)
		if err != nil {
			return nil, nil, fmt.Errorf("encoding document %q: %w", en.id, err)
		}
		recs = append(recs, segment.Record{Type: segment.TypePut, WALOffset: en.me.walOffset, ID: en.id, Doc: data})
	}
	recs = append(recs, segment.Record{Type: segment.TypeCheckpoint, WALOffset: cp})
	return entries, recs, nil
}

// ensureActive opens the active data segment if it is not open: segment 1
// for a collection without segments, the next segment after a sealed one,
// or the current one again after a failed flush. Callers hold flushMu.
func (c *Collection) ensureActive() error {
	if c.active != nil {
		return nil
	}
	var (
		w   *segment.Writer
		err error
	)
	switch {
	case c.activeSeg == 0:
		w, err = c.createSegment(1)
	case c.activeSealed:
		w, err = c.createSegment(c.activeSeg + 1)
	default:
		w, err = c.openActiveSegment(c.activeSeg)
	}
	if err != nil {
		return err
	}
	if w.Seg() != c.activeSeg {
		c.activeHint = nil
	}
	c.activeSeg, c.activeSealed = w.Seg(), false
	c.active = c.db.e.wrapSegmentWriter(w)
	return nil
}

// createSegment creates data segment seg. If the file already exists (an
// earlier attempt failed after creating it), it is opened for append and
// its records are scanned into activeHint.
func (c *Collection) createSegment(seg uint32) (*segment.Writer, error) {
	w, err := segment.Create(c.dir, seg)
	if !errors.Is(err, fs.ErrExist) {
		return w, err
	}
	if w, err = c.openActiveSegment(seg); err != nil {
		return nil, err
	}
	hint, err := scanHint(c.dir, seg)
	if err != nil {
		_ = w.Close() // already failing; the scan error is reported
		return nil, err
	}
	c.activeHint = hint
	return w, nil
}

// openActiveSegment opens existing segment seg for append, truncating a
// torn tail, which it logs.
func (c *Collection) openActiveSegment(seg uint32) (*segment.Writer, error) {
	w, err := segment.OpenActive(c.dir, seg)
	if err != nil {
		return nil, err
	}
	if n := w.TruncatedBytes(); n > 0 {
		c.db.e.logger.Warn("cachydb: truncated torn tail of data segment",
			"file", segment.DataPath(c.dir, seg), "bytes", n)
	}
	return w, nil
}

// scanHint returns the hint entries of every record in segment seg.
func scanHint(dir string, seg uint32) ([]segment.HintEntry, error) {
	var hint []segment.HintEntry
	_, err := segment.Scan(dir, seg, func(r segment.Record, loc segment.Location) error {
		hint = append(hint, segment.HintEntry{ID: r.ID, Type: r.Type, WALOffset: r.WALOffset, Loc: loc})
		return nil
	})
	return hint, err
}

func (e *Engine) wrapSegmentWriter(w *segment.Writer) segmentWriter {
	if e.opts.testSegWriter != nil {
		return e.opts.testSegWriter(w)
	}
	return w
}

// abandonActive handles a failed append or fsync of the active segment:
// it closes the writer (whose error is sticky) and truncates the file back
// to pre, the size before the failed flush, so no record or checkpoint
// marker of the failed attempt can surface later. The next flush reopens
// the segment. Callers hold flushMu.
func (c *Collection) abandonActive(pre int64) {
	w := c.active
	c.active = nil
	_ = w.Close() // the writer already failed; that error is reported by the caller
	path := segment.DataPath(c.dir, c.activeSeg)
	if err := truncateFile(path, pre); err != nil {
		c.db.e.logger.Warn("cachydb: truncating data segment after failed flush", "file", path, "err", err)
	}
}

// truncateFile truncates path to size and fsyncs it.
func truncateFile(path string, size int64) (err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

// sealActive seals the full active segment (§4.1): it closes it (fsync),
// writes its hint file and the collection's index snapshots, and opens the
// next segment. The segment's data is already durable, so failures here
// are logged, not returned: a missing hint or snapshot is rebuilt on open,
// and a failed segment creation is retried by the next flush. Callers hold
// flushMu.
func (c *Collection) sealActive() {
	e := c.db.e
	w, seg := c.active, c.activeSeg
	end := w.Size()
	c.active, c.activeSealed = nil, true
	if err := w.Close(); err != nil {
		e.logger.Warn("cachydb: closing sealed data segment", "file", segment.DataPath(c.dir, seg), "err", err)
	}
	if err := segment.WriteHint(c.dir, seg, c.activeHint); err != nil {
		e.logger.Warn("cachydb: writing hint file; the segment will be scanned on open",
			"file", segment.DataPath(c.dir, seg), "err", err)
	}
	c.activeHint = nil
	c.writeIndexSnapshots(seg, uint64(end))
	if err := c.ensureActive(); err != nil {
		e.logger.Warn("cachydb: creating next data segment; retried by the next flush",
			"database", c.db.name, "collection", c.name, "err", err)
	}
}

// writeIndexSnapshots writes a snapshot of every index of the collection
// (§8), covering the data segments up to (seg, pos) and WAL offset
// checkpoint. The indexes may also reflect newer changes still in the
// memtable; those are in the WAL above the checkpoint, and replaying them
// over the snapshot is idempotent. Failures are logged: a missing snapshot
// means a rebuild on open. Callers hold flushMu.
func (c *Collection) writeIndexSnapshots(seg uint32, pos uint64) {
	c.mu.RLock()
	snaps := make([]*segment.IndexSnapshot, 0, len(c.indexes))
	for _, ix := range c.indexes {
		snaps = append(snaps, &segment.IndexSnapshot{
			Name:      ix.name,
			Field:     ix.field,
			CoversSeg: seg,
			CoversPos: pos,
			CoversWAL: c.checkpoint,
			Keys:      ix.snapshotKeys(),
		})
	}
	c.mu.RUnlock()
	for _, s := range snaps {
		if err := segment.WriteIndexSnapshot(c.dir, s); err != nil {
			c.db.e.logger.Warn("cachydb: writing index snapshot; the index will be rebuilt on open",
				"collection", c.name, "index", s.Name, "err", err)
		}
	}
}

// shutdown runs after the final flush on Close: if everything was flushed
// it writes the index snapshots (§8), then closes the active segment.
func (c *Collection) shutdown() error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.mu.RLock()
	clean := !c.dropped && len(c.memtable) == 0 && c.flushing == nil
	c.mu.RUnlock()
	if c.active == nil {
		return nil
	}
	if clean {
		c.writeIndexSnapshots(c.activeSeg, uint64(c.active.Size()))
	}
	err := c.active.Close()
	c.active = nil
	if err != nil {
		return fmt.Errorf("closing active segment of %s/%s: %w", c.db.name, c.name, err)
	}
	return nil
}
