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
	"sync"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Background flush (§5.4; approach-index-and-query §4), the checkpoint,
// and the low-water mark (§7.1).
//
// A flush swaps a collection's memtable into the immutable flushing
// table, appends the changed documents and tombstones (sorted by WAL
// offset) to the active data segment, fsyncs once, and then applies them
// to the index file in one transaction: the key-table changes, the
// secondary index entries and counters, the live count, the per-segment
// byte accounting, the position the index now covers, and the checkpoint. Collection.mu is held only for the two
// memory-only swaps, never during segment or index I/O. A failed flush
// keeps flushing (reads still see it) and the old checkpoint, and the
// next flush retries with flushing ∪ memtable.
//
// A crash between the segment fsync and the index commit leaves records
// in the segment that the index file does not know. The next Open
// truncates the segment back to the covered position (Collection.load),
// and WAL replay restores the changes: the checkpoint did not advance.

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

// flushCheckpointLocked returns the checkpoint for a flush
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
		if i, ok := slices.BinarySearch(e.metaInflight, lb); ok {
			e.metaInflight = slices.Delete(e.metaInflight, i, i+1)
		}
		e.lwMu.Unlock()
		e.writeMetaCheckpoint()
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
	e.bg.Go(func() {
		e.flushLoop(e.bgCtx)
	})
}

// flushLoop flushes every collection each FlushInterval and, when kicked,
// the collections over MemtableSize, or every collection while writers
// wait on MaxPendingBytes. It exits when ctx is cancelled (Close), or after
// a simulated crash (tests), as a dead process would.
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
			onlyFull = !e.overLimit()
		}
		if err := e.flushAll(ctx, onlyFull); errors.Is(err, errSimulatedCrash) {
			return
		}
	}
}

// flushAll flushes every collection (only those whose memtable exceeds
// MemtableSize if onlyFull), up to FlushWorkers at a time. Failures are
// logged and joined; the failed collections keep their data in memory and
// are retried by the next pass. It starts no new flush once ctx ends or a
// flush hit a simulated crash, and returns after every started flush.
func (e *Engine) flushAll(ctx context.Context, onlyFull bool) error {
	var todo []*Collection
	for _, c := range e.allCollections() {
		if onlyFull && !c.memtableFull() {
			continue
		}
		todo = append(todo, c)
	}

	var (
		mu      sync.Mutex
		errs    []error
		crashed bool
		wg      sync.WaitGroup
	)
	stop := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return crashed
	}
	sem := make(chan struct{}, max(1, e.opts.FlushWorkers))
	for _, c := range todo {
		if err := ctx.Err(); err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
			break
		}
		sem <- struct{}{}
		if stop() {
			<-sem
			break
		}
		wg.Go(func() {
			defer func() { <-sem }()
			err := c.flush()
			if err == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if errors.Is(err, errSimulatedCrash) {
				crashed = true
				errs = append(errs, err)
				return
			}
			e.logger.Error("cachydb: flush failed; the changes stay in memory and the WAL and are retried",
				"database", c.db.name, "collection", c.name, "err", err)
			errs = append(errs, err)
		})
	}
	wg.Wait()
	if crashed {
		return errSimulatedCrash
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
		c.flushing, c.flushingMin, c.flushingBytes = c.memtable, c.memMin, c.memBytes
	case len(c.memtable) > 0:
		// Retry after a failed flush: flushing ∪ memtable, newer wins.
		merged := maps.Clone(c.flushing)
		var replaced int64
		for id, me := range c.memtable {
			if old, ok := merged[id]; !ok || me.walOffset > old.walOffset {
				if ok {
					replaced += old.size
				}
				merged[id] = me
			} else {
				replaced += me.size // superseded by the flushing entry
			}
		}
		c.flushing, c.flushingMin = merged, min(c.flushingMin, c.memMin)
		c.flushingBytes += c.memBytes - replaced
		if replaced != 0 {
			e.pending.release(replaced)
		}
	}
	c.memtable, c.memMin, c.memBytes = make(map[string]memEntry), 0, 0
	cp := c.flushCheckpointLocked()
	flushing := c.flushing // immutable while set; only the flusher replaces it
	// The index set cannot change before this flush ends: CreateIndex and
	// DropIndex hold flushMu.
	defs := sortedIndexDefs(c.indexes)
	c.mu.Unlock()

	// 2. Write the records, one fsync.
	entries, recs, err := flushRecords(flushing)
	if err != nil {
		return fmt.Errorf("flushing collection %q: %w", c.name, err)
	}
	st, err := c.indexStore()
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

	// 3. One index transaction (no collection lock is held): the key
	// table, index entries, count, segment accounting, covered position,
	// and checkpoint change together or not at all.
	covered := idxstore.Position{Seg: w.Seg(), End: uint64(w.Size())}
	committing := false
	err = st.Update(func(u *idxstore.WTx) error {
		committing = false
		if err := c.applyFlush(u, defs, entries, locs, cp, covered); err != nil {
			return err
		}
		committing = true
		return nil
	})
	if err != nil {
		c.afterFailedIndexCommit(st, covered, pre, committing)
		return fmt.Errorf("flushing collection %q: %w", c.name, c.storeErr(err))
	}
	if err := e.hook(hookAfterIndexCommit); err != nil {
		return err // simulated crash: no cleanup
	}

	// 4. The changes are in the index file: drop them from the overlay
	// (memory only). Readers that still see flushing started before this
	// point; readers that do not start their read transaction after the
	// commit above.
	c.mu.Lock()
	flushed := c.flushingBytes
	c.flushing, c.flushingMin, c.flushingBytes = nil, 0, 0
	c.checkpoint = max(c.checkpoint, cp)
	c.mu.Unlock()
	e.pending.release(flushed) // wakes writers waiting on MaxPendingBytes

	// 5. Roll the segment once it is full.
	if w.Size() >= e.opts.DataSegmentSize {
		c.sealActive()
	}
	return nil
}

// flushRecords returns the flushing entries sorted by WAL offset and the
// segment records to append for them: one put or tombstone per entry, in
// the same order. (Checkpoint marker records are no longer written; the
// checkpoint is in the index file.)
func flushRecords(flushing map[string]memEntry) ([]flushEntry, []segment.Record, error) {
	entries := make([]flushEntry, 0, len(flushing))
	for id, me := range flushing {
		entries = append(entries, flushEntry{id: id, me: me})
	}
	slices.SortFunc(entries, func(a, b flushEntry) int { return cmp.Compare(a.me.walOffset, b.me.walOffset) })

	recs := make([]segment.Record, 0, len(entries))
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
	return entries, recs, nil
}

// applyFlush is the index transaction of a flush (approach-index-and-query
// §4 step 3). entries were appended to the active segment at locs (same
// order); defs is the collection's index set. For each entry it reads the
// key-table entry of the id: if that one is not older (WAL replay over data
// that is already flushed), the new record is dead on arrival; otherwise
// the entry replaces it, or removes the id for a tombstone, and the old
// record becomes dead. The index entries of the old version (oldIndexKeys)
// are replaced by those of the new one, and each index's complex and
// arrays counters follow. Then it adds the record and dead bytes to the segment accounting
// (registering the active segment on its first flush) and stores the
// covered position and the checkpoint. The live count and the truncated
// counters follow the key table and the entries by themselves (idxstore).
//
// The ids are applied in ascending order, not WAL-offset order: an id
// occurs once per flush, so the result is the same, and bbolt takes keys
// in order far faster (see the idxstore package comment). For the same
// reason each index's changes are sorted by key before they are applied.
func (c *Collection) applyFlush(u *idxstore.WTx, defs []indexDef, entries []flushEntry, locs []segment.Location, cp uint64, covered idxstore.Position) error {
	order := make([]int, len(entries))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return cmp.Compare(entries[a].id, entries[b].id) })

	recBytes := make(map[uint32]int64, 1)
	deadBytes := make(map[uint32]int64, 2)
	ixOps := make([][]indexOp, len(defs))
	complexDelta := make([]int64, len(defs))
	arraysDelta := make([]int64, len(defs))
	var maxOff uint64
	for _, i := range order {
		en, loc := entries[i], locs[i]
		size := int64(loc.Size)
		recBytes[loc.Seg] += size
		maxOff = max(maxOff, en.me.walOffset)
		old, had, err := u.Key(en.id)
		if err != nil {
			return err
		}
		if had && old.WALOffset >= en.me.walOffset {
			deadBytes[loc.Seg] += size // already superseded on disk
			continue
		}
		if len(defs) > 0 {
			oldKeys, err := c.oldIndexKeys(en, defs, old, had)
			if err != nil {
				return err
			}
			for j, def := range defs {
				var next docIndexKeys
				if en.me.doc != nil {
					next = indexKeysOf(en.me.doc, def.Field)
				}
				ixOps[j] = appendIndexDiff(ixOps[j], en.id, oldKeys[def.Name].entries, next.entries)
				complexDelta[j] += boolInt(next.complex) - boolInt(oldKeys[def.Name].complex)
				arraysDelta[j] += boolInt(next.array) - boolInt(oldKeys[def.Name].array)
			}
		}
		if had {
			deadBytes[old.Seg] += int64(old.Size)
		}
		if en.me.doc == nil {
			deadBytes[loc.Seg] += size // tombstones are never referenced
			if err := u.DeleteKey(en.id); err != nil {
				return err
			}
			continue
		}
		if err := u.PutKey(en.id, locationEntry(loc, en.me.walOffset)); err != nil {
			return err
		}
	}
	for j, def := range defs {
		if len(ixOps[j]) == 0 && complexDelta[j] == 0 && arraysDelta[j] == 0 {
			continue
		}
		w, err := u.Index(def.Name)
		if err != nil {
			return err
		}
		if err := applyIndexOps(w, ixOps[j]); err != nil {
			return err
		}
		if err := w.AddComplex(complexDelta[j]); err != nil {
			return err
		}
		if err := w.AddArrays(arraysDelta[j]); err != nil {
			return err
		}
	}
	segs := slices.Collect(maps.Keys(recBytes))
	for seg := range deadBytes {
		if _, ok := recBytes[seg]; !ok {
			segs = append(segs, seg)
		}
	}
	slices.Sort(segs)
	for _, seg := range segs {
		if err := u.AddSegment(seg, recBytes[seg], deadBytes[seg]); err != nil {
			return err
		}
	}
	if err := u.SetCovered(covered); err != nil {
		return err
	}
	if err := u.RaiseMaxWALOffset(maxOff); err != nil {
		return err
	}
	cur, err := u.Checkpoint()
	if err != nil || cp <= cur {
		return err // checkpoints only grow
	}
	return u.SetCheckpoint(cp)
}

// oldIndexKeys returns the index keys, per index of defs, of the version of
// en's document that the key table holds (old, if had): the keys captured
// on the write path (memEntry.base) when they describe that version and
// cover every index, otherwise the keys of the version read from its data
// segment. The second case follows an index created or dropped since the
// capture, and a flush retried after a commit that failed late but reached
// the file.
func (c *Collection) oldIndexKeys(en flushEntry, defs []indexDef, old idxstore.Entry, had bool) (indexBase, error) {
	if !had {
		return nil, nil // no version in the index file, so no entries
	}
	if en.me.baseOff == old.WALOffset {
		complete := true
		for _, def := range defs {
			if _, ok := en.me.base[def.Name]; !ok {
				complete = false
				break
			}
		}
		if complete {
			return en.me.base, nil
		}
	}
	doc, err := c.readStored(en.id, old)
	if err != nil {
		return nil, fmt.Errorf("reading the replaced version of %q for its index keys: %w", en.id, err)
	}
	keys := make(indexBase, len(defs))
	for _, def := range defs {
		keys[def.Name] = indexKeysOf(doc, def.Field)
	}
	return keys, nil
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// afterFailedIndexCommit cleans up the active segment after the index
// transaction of a flush failed; the flush wrote the records between pre
// and covered.End. Callers hold flushMu.
//
// When the transaction never reached its commit (committing is false), or
// the index file still shows an older covered position, nothing in the
// index file references those records: the segment is truncated back, as
// after a failed append. A commit that failed late is different: bbolt may
// already have written the transaction, so that the file shows it although
// it was not made durable. Then the key table may point at the records and
// they must stay; the retried flush appends them again, finds the entries
// in place (same WAL offsets), and counts its own copies as dead.
func (c *Collection) afterFailedIndexCommit(st *idxstore.Store, covered idxstore.Position, pre int64, committing bool) {
	if committing {
		var now idxstore.Position
		err := st.View(func(tx *idxstore.Tx) (err error) {
			now, err = tx.Covered()
			return err
		})
		if err != nil || now == covered {
			if w := c.active; w != nil {
				c.active = nil
				_ = w.Close() // the flush already failed; the next one reopens the segment
			}
			c.db.e.logger.Warn("cachydb: the index transaction of a flush failed at commit; keeping its records in the data segment",
				"file", segment.DataPath(c.dir, c.activeSeg), "err", err)
			return
		}
	}
	c.abandonActive(pre)
}

// ensureActive opens the active data segment if it is not open: a new
// segment (id nextSeg, so it is newer than every existing segment,
// compaction outputs included) for a collection without segments or after
// a sealed one, or the current one again after a failed flush or a
// restart. A new segment is registered in the index file by the first
// flush that writes to it. Callers hold flushMu.
func (c *Collection) ensureActive() error {
	if c.active != nil {
		return nil
	}
	var (
		w   *segment.Writer
		err error
	)
	if c.activeSeg == 0 || c.activeSealed {
		w, err = c.createSegment(c.nextSeg)
	} else {
		w, err = c.openActiveSegment(c.activeSeg)
	}
	if err != nil {
		return err
	}
	seg := w.Seg()
	c.activeSeg, c.activeSealed = seg, false
	c.nextSeg = max(c.nextSeg, seg+1)
	c.active = c.db.e.wrapSegmentWriter(w)
	return nil
}

// createSegment creates data segment seg, a new id. If the file already
// exists, an earlier attempt failed after creating it: segment ids are
// never reused and the index file has never referenced this one, so the
// leftover is removed and the segment created again.
func (c *Collection) createSegment(seg uint32) (*segment.Writer, error) {
	w, err := segment.Create(c.dir, seg)
	if !errors.Is(err, fs.ErrExist) {
		return w, err
	}
	path := segment.DataPath(c.dir, seg)
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("removing leftover data segment %s: %w", path, err)
	}
	return segment.Create(c.dir, seg)
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

func (e *Engine) wrapSegmentWriter(w *segment.Writer) segmentWriter {
	if e.opts.testSegWriter != nil {
		return e.opts.testSegWriter(w)
	}
	return w
}

// abandonActive handles a failed append or fsync of the active segment (or
// an index transaction that changed nothing): it closes the writer (whose
// error is sticky) and truncates the file back to pre, the size before the
// failed flush, so no record of the failed attempt can surface later. The
// next flush reopens the segment. Callers hold flushMu.
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

// sealActive seals the full active segment (§4.1): it closes it (fsync)
// and opens the next segment. The segment's data is already durable and
// indexed, so failures here are logged, not returned: a failed segment
// creation is retried by the next flush. Callers hold flushMu.
func (c *Collection) sealActive() {
	e := c.db.e
	w, seg := c.active, c.activeSeg
	c.active, c.activeSealed = nil, true
	if err := w.Close(); err != nil {
		e.logger.Warn("cachydb: closing sealed data segment", "file", segment.DataPath(c.dir, seg), "err", err)
	}
	if err := c.ensureActive(); err != nil {
		e.logger.Warn("cachydb: creating next data segment; retried by the next flush",
			"database", c.db.name, "collection", c.name, "err", err)
	}
}

// shutdown runs after the final flush on Close: it closes the active
// segment. The index file is closed with the segment readers
// (closeReaders).
func (c *Collection) shutdown() error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	if c.active == nil {
		return nil
	}
	err := c.active.Close()
	c.active = nil
	if err != nil {
		return fmt.Errorf("closing active segment of %s/%s: %w", c.db.name, c.name, err)
	}
	return nil
}
