package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

// Metadata checkpoint (plan 002, C4).
//
// The root file cachydb.metacp holds {"offset": N}: every metadata WAL
// record (create/drop database, create/drop/rename collection,
// create/drop index, update schema) at or below N is reflected in the meta files and directories.
// Replay then starts at min(N+1, every collection's replay floor + 1)
// instead of one past the oldest meta file's wal_offset, so Open does not
// re-read the whole kept WAL since the oldest database was created.
//
// N is the metadata-only low-water mark minus one: a metadata operation
// stays in Engine.metaInflight from before its WAL append until its file
// change is durable (appendMetaWAL), so every metadata record below that
// mark is reflected. The file is rewritten (atomically) after each
// metadata operation completes, after recovery, and on Close. A smaller
// value than the truth is always safe; a missing, unreadable, or
// implausible file (N at or past the end of the WAL) falls back to the
// meta files' wal_offset values.

const metaCheckpointFileName = "cachydb.metacp"

type metaCheckpointFile struct {
	Offset uint64 `json:"offset"`
}

// metaLowWater returns one past the largest offset N such that every
// metadata record at or below N is reflected in the meta files.
func (e *Engine) metaLowWater() uint64 {
	next := e.wal.NextOffset() // read first, as in lowWater
	e.lwMu.Lock()
	defer e.lwMu.Unlock()
	if len(e.metaInflight) > 0 {
		return lowWaterMark(next, e.metaInflight[0])
	}
	return lowWaterMark(next)
}

// writeMetaCheckpoint persists the current metadata checkpoint if it
// advanced. Failures are logged: an older (smaller) checkpoint stays
// valid, it only makes the next replay start earlier.
func (e *Engine) writeMetaCheckpoint() {
	cp := checkpointBelow(e.metaLowWater())
	e.metacpMu.Lock()
	defer e.metacpMu.Unlock()
	if cp <= e.metacp {
		return
	}
	data, err := json.Marshal(metaCheckpointFile{Offset: cp})
	if err == nil {
		err = fsutil.WriteFileAtomic(filepath.Join(e.root, metaCheckpointFileName), data, metaPerm)
	}
	if err != nil {
		e.logger.Warn("cachydb: writing the metadata checkpoint; the next open replays more of the WAL",
			"file", metaCheckpointFileName, "err", err)
		return
	}
	e.metacp = cp
}

// readMetaCheckpoint returns the persisted metadata checkpoint and whether
// it is usable. next is wal.NextOffset(); a checkpoint at or past it would
// claim records the log does not have, so it is ignored.
func readMetaCheckpoint(root string, next uint64) (uint64, bool, error) {
	data, err := os.ReadFile(filepath.Join(root, metaCheckpointFileName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading %s: %w", metaCheckpointFileName, err)
	}
	var f metaCheckpointFile
	if err := json.Unmarshal(data, &f); err != nil {
		return 0, false, fmt.Errorf("decoding %s: %w", metaCheckpointFileName, err)
	}
	if f.Offset >= next {
		return 0, false, fmt.Errorf("%s offset %d is not below the end of the WAL (%d)", metaCheckpointFileName, f.Offset, next)
	}
	return f.Offset, true, nil
}
