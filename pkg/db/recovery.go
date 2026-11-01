package db

import (
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

	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Recovery at Open (§7.2, §7.3, §8):
//
//  1. Read every db.meta and coll.meta (loadMetadata).
//  2. Per collection, in parallel bounded by GOMAXPROCS (loadCollections):
//     load the keydir from hint files or segment scans with "last writer
//     wins by WAL offset", truncate a torn tail of the active segment,
//     take the checkpoint from the newest checkpoint marker, and load the
//     secondary indexes from their snapshots plus catch-up (or rebuild
//     them when a snapshot is missing or invalid).
//  3. Check that the WAL reaches past every offset the data references.
//  4. Replay the WAL sequentially from the oldest offset any metadata file
//     or collection may still be missing, applying each record
//     idempotently (replayRecord). Replayed document changes land in the
//     memtables and are flushed by the normal flusher.
//
// WAL retention and the flusher start only after replay (see Open).

// recoveryStats counts what recovery did. They are logged and read by
// tests.
type recoveryStats struct {
	hintsLoaded     atomic.Int64 // sealed segments loaded from their hint file
	segmentsScanned atomic.Int64 // segments scanned (active, or sealed without a valid hint)
	snapshotsLoaded atomic.Int64 // indexes loaded from a snapshot plus catch-up
	indexesRebuilt  atomic.Int64 // indexes rebuilt from the documents
	replayApplied   atomic.Int64 // WAL records whose effect replay applied
	replaySkipped   atomic.Int64 // WAL records replay found already applied or obsolete
}

// replayCtxCheckEvery is how many WAL records replay applies between
// context checks.
const replayCtxCheckEvery = 1024

// recover loads the data directory into e and replays the WAL. On error
// the caller closes the collections' segment files.
func (e *Engine) recover(ctx context.Context) error {
	start := time.Now()
	if err := e.loadMetadata(ctx); err != nil {
		return err
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
		"hints_loaded", st.hintsLoaded.Load(),
		"segments_scanned", st.segmentsScanned.Load(),
		"index_snapshots_loaded", st.snapshotsLoaded.Load(),
		"indexes_rebuilt", st.indexesRebuilt.Load(),
		"duration", time.Since(start),
	}
	if st.replayApplied.Load() > 0 {
		e.logger.Info("cachydb: recovered changes from the WAL", attrs...)
	} else {
		e.logger.Debug("cachydb: opened data directory", attrs...)
	}
	return nil
}

// loadCollections loads the data segments and indexes of every collection
// in parallel, bounded by GOMAXPROCS. It returns the largest WAL offset
// found in the data segments.
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
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}
	for _, c := range colls {
		jobs <- c
	}
	close(jobs)
	wg.Wait()
	return maxSeen, errors.Join(errs...)
}

// load reads the collection's data segments at Open (§7.2 step 4, §8):
// sealed segments from their hint files (scanning and rewriting the hint
// when it is missing or invalid), the newest unsealed segment by scanning
// it after truncating a torn tail. The keydir keeps, per id, the record
// with the highest WAL offset (a tombstone removes the id), so load order
// does not matter (§4.3). The checkpoint is the newest checkpoint marker.
// Then the secondary indexes are loaded (loadIndexes). It returns the
// largest WAL offset in the segments. Callers have not published c yet.
func (c *Collection) load(ctx context.Context) (uint64, error) {
	segs, err := segment.ListSegments(c.dir)
	if err != nil {
		return 0, fmt.Errorf("loading collection %q: %w", c.name, err)
	}
	snaps := c.readSnapshots(segs)

	var maxOff uint64
	newest := make(map[string]uint64) // highest WAL offset seen per id, puts and tombstones
	total := make(map[uint32]int64)   // record bytes per segment
	for _, seg := range segs {
		total[seg] = 0
	}
	if len(segs) > 0 {
		c.nextSeg = segs[len(segs)-1] + 1
	}
	visit := func(en segment.HintEntry) {
		total[en.Loc.Seg] += int64(en.Loc.Size)
		maxOff = max(maxOff, en.WALOffset)
		for _, s := range snaps {
			s.observe(en)
		}
		if en.Type == segment.TypeCheckpoint {
			if en.WALOffset >= c.checkpoint {
				c.checkpoint, c.markerSeg = en.WALOffset, en.Loc.Seg
			}
			return
		}
		if prev, ok := newest[en.ID]; ok && prev >= en.WALOffset {
			return
		}
		newest[en.ID] = en.WALOffset
		if en.Type == segment.TypePut {
			c.keydir[en.ID] = keyEntry{loc: en.Loc, walOffset: en.WALOffset}
		} else {
			delete(c.keydir, en.ID)
		}
	}
	for i, seg := range segs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		entries, err := c.segmentEntries(seg, i == len(segs)-1)
		if err != nil {
			return 0, fmt.Errorf("loading collection %q: %w", c.name, err)
		}
		for _, en := range entries {
			visit(en)
		}
	}

	// Durable tombstones above the checkpoint (a lost marker, or writes
	// that were inflight during a flush) would otherwise look absent to
	// replay; their ids are not in the keydir.
	for id, off := range newest {
		if _, ok := c.keydir[id]; !ok && off > c.checkpoint {
			if c.replayTombs == nil {
				c.replayTombs = make(map[string]uint64)
			}
			c.replayTombs[id] = off
		}
	}

	live := make(map[uint32]int64, len(total))
	for _, ke := range c.keydir {
		live[ke.loc.Seg] += int64(ke.loc.Size)
	}
	for seg, n := range total {
		if dead := n - live[seg]; dead > 0 {
			c.deadBytes[seg] = dead
		}
	}
	c.segBytes = total
	c.count.Store(int64(len(c.keydir)))
	if err := c.loadIndexes(ctx, snaps); err != nil {
		return 0, fmt.Errorf("loading indexes of collection %q: %w", c.name, err)
	}
	return maxOff, nil
}

// segmentEntries returns the records of segment seg as hint entries. A
// segment with a valid hint file is sealed and loaded from the hint; if it
// is the newest one, the next flush starts a new segment. Otherwise the
// newest segment becomes the active one (opened for append after
// truncating a torn tail) and is scanned; an older segment without a valid
// hint is scanned and its hint rewritten.
func (c *Collection) segmentEntries(seg uint32, newest bool) ([]segment.HintEntry, error) {
	e := c.db.e
	hint, err := segment.ReadHint(c.dir, seg)
	if err == nil {
		err = checkHintFits(c.dir, seg, hint)
	}
	switch {
	case err == nil:
		e.recov.hintsLoaded.Add(1)
		if newest {
			c.activeSeg, c.activeSealed = seg, true
		}
		return hint, nil
	case !errors.Is(err, os.ErrNotExist):
		e.logger.Warn("cachydb: ignoring invalid hint file; scanning the segment", "file", segment.DataPath(c.dir, seg), "err", err)
	}

	if newest {
		w, err := c.openActiveSegment(seg)
		if err != nil {
			return nil, err
		}
		c.active, c.activeSeg = e.wrapSegmentWriter(w), seg
	}
	entries, err := scanHint(c.dir, seg)
	if err != nil {
		return nil, err
	}
	e.recov.segmentsScanned.Add(1)
	if newest {
		c.activeHint = entries
		return entries, nil
	}
	if err := segment.WriteHint(c.dir, seg, entries); err != nil {
		e.logger.Warn("cachydb: rewriting hint file", "file", segment.DataPath(c.dir, seg), "err", err)
	}
	return entries, nil
}

// checkHintFits checks that every record of hint lies inside the data file
// of segment seg, so a hint never points past a shortened segment.
func checkHintFits(dir string, seg uint32, hint []segment.HintEntry) error {
	fi, err := os.Stat(segment.DataPath(dir, seg))
	if err != nil {
		return err
	}
	for _, en := range hint {
		if end := en.Loc.Pos + uint64(en.Loc.Size); end > uint64(fi.Size()) {
			return fmt.Errorf("%w: hint entry ends at byte %d, segment %d is %d bytes", segment.ErrCorrupt, end, seg, fi.Size())
		}
	}
	return nil
}

// snapState tracks one index snapshot during load: the ids of every data
// record after the position the snapshot covers (catch-up, §8) and
// whether that position is a record boundary.
type snapState struct {
	snap     *segment.IndexSnapshot
	dirty    map[string]struct{}
	straddle bool // a record starts before and ends after the covered position
}

func (s *snapState) observe(en segment.HintEntry) {
	switch {
	case en.Loc.Seg < s.snap.CoversSeg:
		return
	case en.Loc.Seg == s.snap.CoversSeg && en.Loc.Pos < s.snap.CoversPos:
		if en.Loc.Pos+uint64(en.Loc.Size) > s.snap.CoversPos {
			s.straddle = true
		}
		return
	}
	if en.ID != "" {
		s.dirty[en.ID] = struct{}{}
	}
}

// readSnapshots reads the snapshot of every index of the collection whose
// file exists, matches the index definition in coll.meta, and covers an
// existing segment. Others are rebuilt by loadIndexes.
func (c *Collection) readSnapshots(segs []uint32) map[string]*snapState {
	out := make(map[string]*snapState)
	for name, ix := range c.indexes {
		snap, err := segment.ReadIndexSnapshot(c.dir, name)
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case err != nil:
			c.db.e.logger.Warn("cachydb: ignoring invalid index snapshot; rebuilding the index",
				"collection", c.name, "index", name, "err", err)
			continue
		case snap.Name != name || snap.Field != ix.field:
			c.db.e.logger.Warn("cachydb: ignoring index snapshot of another definition; rebuilding the index",
				"collection", c.name, "index", name, "snapshot_field", snap.Field, "field", ix.field)
			continue
		case !slices.Contains(segs, snap.CoversSeg):
			c.db.e.logger.Warn("cachydb: ignoring index snapshot covering a missing segment; rebuilding the index",
				"collection", c.name, "index", name, "segment", snap.CoversSeg)
			continue
		}
		out[name] = &snapState{snap: snap, dirty: make(map[string]struct{})}
	}
	return out
}

// snapshotUsable reports whether the snapshot's covered position is a record
// boundary inside its segment.
func (c *Collection) snapshotUsable(s *snapState) bool {
	if s.straddle {
		return false
	}
	var size int64
	if s.snap.CoversSeg == c.activeSeg && c.active != nil {
		size = c.active.Size()
	} else {
		fi, err := os.Stat(segment.DataPath(c.dir, s.snap.CoversSeg))
		if err != nil {
			return false
		}
		size = fi.Size()
	}
	return s.snap.CoversPos <= uint64(size)
}

// loadIndexes fills the collection's (empty) secondary indexes after the
// keydir is loaded (§8). An index with a usable snapshot starts from it;
// catch-up then re-indexes every id with a data record after the covered
// position: the id is removed from the index and its current version
// added. Other indexes are rebuilt from every document in the keydir. WAL
// replay then updates the indexes like any write.
//
// A snapshot may also reflect memtable changes newer than its covered
// position. Those are in the WAL above the collection's checkpoint (or
// were flushed after the covered position, making the id part of
// catch-up), and replaying them over the snapshot is idempotent.
func (c *Collection) loadIndexes(ctx context.Context, snaps map[string]*snapState) error {
	if len(c.indexes) == 0 {
		return nil
	}
	st := &c.db.e.recov
	type catchUp struct {
		ix    *index
		dirty map[string]struct{}
	}
	var (
		rebuild []*index
		catch   []catchUp
		dirty   = make(map[string]struct{})
	)
	for name, ix := range c.indexes {
		s, ok := snaps[name]
		if !ok || !c.snapshotUsable(s) {
			if ok {
				c.db.e.logger.Warn("cachydb: ignoring index snapshot with an invalid covered position; rebuilding the index",
					"collection", c.name, "index", name)
			}
			rebuild = append(rebuild, ix)
			st.indexesRebuilt.Add(1)
			continue
		}
		loaded := indexFromSnapshot(s.snap)
		removeIDs(loaded, s.dirty)
		c.indexes[name] = loaded
		catch = append(catch, catchUp{ix: loaded, dirty: s.dirty})
		for id := range s.dirty {
			dirty[id] = struct{}{}
		}
		st.snapshotsLoaded.Add(1)
	}

	// Read each needed document once: all of them when an index is
	// rebuilt, otherwise only the caught-up ids.
	ids := dirty
	if len(rebuild) > 0 {
		ids = make(map[string]struct{}, len(c.keydir))
		for id := range c.keydir {
			ids[id] = struct{}{}
		}
	}
	n := 0
	for id := range ids {
		if n++; n%findCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		ke, ok := c.keydir[id]
		if !ok {
			continue // deleted after the covered position
		}
		doc, err := c.readStored(id, ke)
		if err != nil {
			return err
		}
		for _, ix := range rebuild {
			ix.add(id, doc)
		}
		for _, cu := range catch {
			if _, ok := cu.dirty[id]; ok {
				cu.ix.add(id, doc)
			}
		}
	}
	return nil
}

// removeIDs deletes every id in ids from every key of ix.
func removeIDs(ix *index, ids map[string]struct{}) {
	if len(ids) == 0 {
		return
	}
	for key, set := range ix.keys {
		if len(set) <= len(ids) {
			for id := range set {
				if _, ok := ids[id]; ok {
					delete(set, id)
				}
			}
		} else {
			for id := range ids {
				delete(set, id)
			}
		}
		if len(set) == 0 {
			delete(ix.keys, key)
		}
	}
}

// replayStart returns the first WAL offset replay must read (§7.2 step 6):
// one past the minimum over every metadata file's wal_offset and every
// collection's replay floor. A metadata operation whose meta write did not
// happen has an offset above the wal_offset of the file it would have
// changed (its database, or the collection), so it is covered; with no
// database at all, a create_db may be missing, so replay reads the whole
// log. ReadFrom starts at the oldest kept record if the offset is below
// it: records retention deleted were processed.
func (e *Engine) replayStart() uint64 {
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
	for _, d := range e.databases {
		for _, c := range d.collections {
			c.replayTombs = nil
		}
	}
	if err != nil {
		return from, fmt.Errorf("WAL replay from offset %d: %w", from, mapWALErr(err))
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
	case wal.OpPutDoc, wal.OpDeleteDoc:
		if c == nil || r.Offset <= c.replayFloor() {
			return false, nil
		}
		return c.replayDoc(r)
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
	if err := writeCollMeta(dir, collMeta{Name: r.Coll, Schema: p.Schema, WALOffset: r.Offset, CreatedOffset: r.Offset}); err != nil {
		return err
	}
	d.collections[r.Coll] = newCollection(d, r.Coll, p.Schema, r.Offset, r.Offset)
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
// coll.meta does not reflect yet. A created index is built from the
// collection's current state (data segments plus the changes replayed so
// far); later replayed writes keep it up to date.
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
		ix, ok := c.indexes[def.Name]
		if !ok || ix.field != def.Field {
			var err error
			if ix, err = c.buildIndex(ctx, def.Name, def.Field); err != nil {
				return err
			}
		}
		defs[def.Name] = def
		if err := c.writeMeta(defs, r.Offset); err != nil {
			return err
		}
		c.mu.Lock()
		c.indexes[def.Name] = ix
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
		c.metaOffset = r.Offset
		c.mu.Unlock()
		if err := segment.RemoveIndexSnapshot(c.dir, p.Name); err != nil {
			c.db.e.logger.Warn("cachydb: removing snapshot of dropped index", "collection", c.name, "index", p.Name, "err", err)
		}
	}
	return nil
}

// replayDoc applies a put_doc or delete_doc record to the memtable iff its
// offset is above the current version's (memtable, keydir, or a durable
// tombstone above the checkpoint), updating
// the indexes and count like a write (§7.3). Callers have checked the
// offset against the collection's replay floor.
func (c *Collection) replayDoc(r wal.Record) (bool, error) {
	var next *Document
	if r.Op == wal.OpPutDoc {
		var err error
		if next, err = decodeDocument(r.ID, r.Payload); err != nil {
			return false, err
		}
	}

	c.mu.RLock()
	var (
		curOff   uint64
		existed  bool
		prev     *Document
		ke       keyEntry
		inKeydir bool
	)
	if me, ok := c.memtable[r.ID]; ok {
		curOff, existed, prev = me.walOffset, me.doc != nil, me.doc
	} else if ke, inKeydir = c.keydir[r.ID]; inKeydir {
		curOff, existed = ke.walOffset, true
	} else {
		curOff = c.replayTombs[r.ID] // 0 if none
	}
	needPrev := inKeydir && len(c.indexes) > 0
	c.mu.RUnlock()
	if r.Offset <= curOff {
		return false, nil
	}
	if needPrev {
		var err error
		if prev, err = c.readStored(r.ID, ke); err != nil {
			return false, err
		}
	}

	size := int64(len(r.ID)+len(r.Payload)) + memEntryOverhead
	c.mu.Lock()
	c.applyLocked(r.ID, existed, prev, next, r.Offset, size)
	c.mu.Unlock()
	return true, nil
}
