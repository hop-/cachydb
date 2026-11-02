package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

// Hint file layout (§4.2), inside the sidecar framing (sidecar.go) with
// magic "CDBH":
//
//	u32 segment id      must match the file name
//	u32 entry count
//	entries, each:
//	  u16 id_len · id   (0 for a checkpoint marker)
//	  u8  type
//	  u64 wal_offset
//	  u64 pos
//	  u32 size
//
// Entry Loc.Seg is not stored: it is the hint's segment id.
const (
	hintMagic         = "CDBH"
	hintSuffix        = ".hint"
	hintEntryMinSize  = 2 + 1 + 8 + 8 + 4
	minRecordDiskSize = recordHeaderSize + recordFixedBody
)

// HintEntry is one record of a data segment as summarized in its hint
// file: enough to rebuild the keydir (and find the last checkpoint marker)
// without reading the segment.
type HintEntry struct {
	ID        string // empty for TypeCheckpoint
	Type      RecordType
	WALOffset uint64
	Loc       Location
}

// HintPath returns the path of the hint file of segment seg in dir,
// <dir>/seg-%06d.hint (compaction removes it with a retired segment).
func HintPath(dir string, seg uint32) string { return hintPath(dir, seg) }

// hintPath returns <dir>/seg-%06d.hint.
func hintPath(dir string, seg uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d%s", dataPrefix, seg, hintSuffix))
}

// validateHintEntry checks that e describes a record of segment seg that
// a hint file can hold.
func validateHintEntry(e *HintEntry, seg uint32) error {
	switch e.Type {
	case TypePut, TypeTombstone:
		if e.ID == "" {
			return fmt.Errorf("%s entry without id", e.Type)
		}
		if len(e.ID) > maxIDLen {
			return fmt.Errorf("entry id is %d bytes, max %d", len(e.ID), maxIDLen)
		}
	case TypeCheckpoint:
		if e.ID != "" {
			return errors.New("checkpoint entry with an id")
		}
	default:
		return fmt.Errorf("invalid entry type %d", uint8(e.Type))
	}
	if e.Loc.Seg != seg {
		return fmt.Errorf("entry location in segment %d, hint is for segment %d", e.Loc.Seg, seg)
	}
	if e.Loc.Pos < headerSize {
		return fmt.Errorf("entry position %d inside the segment header", e.Loc.Pos)
	}
	if e.Loc.Size < minRecordDiskSize {
		return fmt.Errorf("entry size %d below the minimum record size %d", e.Loc.Size, minRecordDiskSize)
	}
	return nil
}

// WriteHint atomically writes the hint file <dir>/seg-%06d.hint for
// segment seg, replacing any existing one. Every entry's Loc.Seg must be
// seg. Entries are stored in the given order.
func WriteHint(dir string, seg uint32, entries []HintEntry) error {
	path := hintPath(dir, seg)
	if uint64(len(entries)) > maxSidecarCount {
		return fmt.Errorf("segment: hint %s: %d entries, max %d", path, len(entries), uint64(maxSidecarCount))
	}
	size := 8
	for i := range entries {
		e := &entries[i]
		if err := validateHintEntry(e, seg); err != nil {
			return fmt.Errorf("segment: hint %s: entry %d: %w", path, i, err)
		}
		size += hintEntryMinSize + len(e.ID)
	}

	buf := newSidecar(hintMagic, hintVersion, size)
	buf = binary.LittleEndian.AppendUint32(buf, seg)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))
	for i := range entries {
		e := &entries[i]
		buf = appendString(buf, e.ID)
		buf = append(buf, byte(e.Type))
		buf = binary.LittleEndian.AppendUint64(buf, e.WALOffset)
		buf = binary.LittleEndian.AppendUint64(buf, e.Loc.Pos)
		buf = binary.LittleEndian.AppendUint32(buf, e.Loc.Size)
	}
	buf = finishSidecar(buf)
	if err := fsutil.WriteFileAtomic(path, buf, sidecarPerm); err != nil {
		return fmt.Errorf("segment: writing hint: %w", err)
	}
	return nil
}

// ReadHint reads and verifies the hint file of segment seg in dir. A
// missing file returns an error satisfying errors.Is(err, os.ErrNotExist);
// a damaged one returns an error wrapping ErrCorrupt.
func ReadHint(dir string, seg uint32) ([]HintEntry, error) {
	path := hintPath(dir, seg)
	body, err := readSidecar(path, hintMagic, hintVersion)
	if err != nil {
		return nil, err
	}
	d := sidecarDecoder{b: body}
	fileSeg := d.u32()
	n := d.count(hintEntryMinSize)
	if d.err == nil && fileSeg != seg {
		return nil, sidecarCorrupt(path, fmt.Sprintf("hint is for segment %d", fileSeg))
	}
	entries := make([]HintEntry, 0, n)
	for i := 0; i < n && d.err == nil; i++ {
		e := HintEntry{ID: d.str()}
		e.Type = RecordType(d.u8())
		e.WALOffset = d.u64()
		e.Loc = Location{Seg: seg, Pos: d.u64(), Size: d.u32()}
		if d.err != nil {
			break
		}
		if err := validateHintEntry(&e, seg); err != nil {
			return nil, sidecarCorrupt(path, fmt.Sprintf("entry %d: %v", i, err))
		}
		entries = append(entries, e)
	}
	if err := d.done(); err != nil {
		return nil, sidecarCorrupt(path, err.Error())
	}
	return entries, nil
}
