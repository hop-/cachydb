package db

import (
	"fmt"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// load reads the collection's data segments at Open. This is the minimal
// load path: it opens the newest segment as the active one (truncating a
// torn tail), scans every segment to build the keydir by "last writer wins
// by WAL offset" (§4.3), takes the checkpoint from the newest checkpoint
// marker, computes dead bytes per segment, and rebuilds the secondary
// indexes from the documents. Hint files, index snapshots, and WAL replay
// are added by the recovery step (§7.2).
func (c *Collection) load() error {
	segs, err := segment.ListSegments(c.dir)
	if err != nil {
		return fmt.Errorf("loading collection %q: %w", c.name, err)
	}
	if len(segs) == 0 {
		return nil
	}
	last := segs[len(segs)-1]
	w, err := c.openActiveSegment(last)
	if err != nil {
		return fmt.Errorf("loading collection %q: %w", c.name, err)
	}
	c.active, c.activeSeg = c.db.e.wrapSegmentWriter(w), last

	newest := make(map[string]uint64) // highest WAL offset seen per id, puts and tombstones
	total := make(map[uint32]int64)   // record bytes per segment
	for _, seg := range segs {
		_, err := segment.Scan(c.dir, seg, func(r segment.Record, loc segment.Location) error {
			total[seg] += int64(loc.Size)
			if seg == last {
				c.activeHint = append(c.activeHint, segment.HintEntry{ID: r.ID, Type: r.Type, WALOffset: r.WALOffset, Loc: loc})
			}
			if r.Type == segment.TypeCheckpoint {
				c.checkpoint = max(c.checkpoint, r.WALOffset)
				return nil
			}
			if prev, ok := newest[r.ID]; ok && prev >= r.WALOffset {
				return nil
			}
			newest[r.ID] = r.WALOffset
			if r.Type == segment.TypePut {
				c.keydir[r.ID] = keyEntry{loc: loc, walOffset: r.WALOffset}
			} else {
				delete(c.keydir, r.ID)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("loading collection %q: %w", c.name, err)
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
	c.count.Store(int64(len(c.keydir)))
	return c.rebuildIndexes()
}

// rebuildIndexes fills the (empty) secondary indexes from every document
// in the keydir. Used at load, before the collection is published.
func (c *Collection) rebuildIndexes() error {
	if len(c.indexes) == 0 {
		return nil
	}
	for id := range c.keydir {
		doc, _, err := c.current(id)
		if err != nil {
			return fmt.Errorf("rebuilding indexes of collection %q: %w", c.name, err)
		}
		if doc == nil {
			continue
		}
		for _, ix := range c.indexes {
			ix.add(id, doc)
		}
	}
	return nil
}
