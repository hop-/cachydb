package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

// Index snapshot layout (§8), inside the sidecar framing (sidecar.go) with
// magic "CDBI":
//
//	u16 len · name      index name, must match the file name
//	u16 len · field     indexed field
//	u32 covers_seg      data position (seg, pos) the snapshot covers
//	u64 covers_pos
//	u64 covers_wal      WAL offset the snapshot covers
//	u32 key count
//	keys, sorted ascending, each:
//	  u16 len · key     canonical key
//	  u32 id count
//	  ids, each u16 len · id
const (
	snapshotMagic      = "CDBI"
	snapshotPrefix     = "idx-"
	snapshotSuffix     = ".snap"
	snapshotKeyMinSize = 2 + 4
	snapshotIDMinSize  = 2
)

// IndexSnapshot is the persisted state of one secondary index, tagged with
// the data position and WAL offset it reflects.
type IndexSnapshot struct {
	Name, Field string
	CoversSeg   uint32
	CoversPos   uint64
	CoversWAL   uint64
	Keys        map[string][]string // canonical key -> ids
}

// snapshotPath returns <dir>/idx-<name>.snap after checking that name is a
// plain file-name component. Names are validated by the engine (§2); this
// is a defensive check against path traversal.
func snapshotPath(dir, name string) (string, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("segment: invalid index name %q", name)
	}
	if len(name) > maxSidecarString {
		return "", fmt.Errorf("segment: index name is %d bytes, max %d", len(name), maxSidecarString)
	}
	return filepath.Join(dir, snapshotPrefix+name+snapshotSuffix), nil
}

func checkSidecarString(what, s string) error {
	if len(s) > maxSidecarString {
		return fmt.Errorf("%s is %d bytes, max %d", what, len(s), maxSidecarString)
	}
	return nil
}

// WriteIndexSnapshot atomically writes <dir>/idx-<s.Name>.snap, replacing
// any existing snapshot of that index. Keys are stored sorted; each key's
// ids are stored in the given order.
func WriteIndexSnapshot(dir string, s *IndexSnapshot) error {
	if s == nil {
		return errors.New("segment: nil index snapshot")
	}
	path, err := snapshotPath(dir, s.Name)
	if err != nil {
		return err
	}
	if err := checkSidecarString("field", s.Field); err != nil {
		return fmt.Errorf("segment: snapshot %s: %w", path, err)
	}
	if uint64(len(s.Keys)) > maxSidecarCount {
		return fmt.Errorf("segment: snapshot %s: %d keys, max %d", path, len(s.Keys), uint64(maxSidecarCount))
	}
	keys := make([]string, 0, len(s.Keys))
	size := 2 + len(s.Name) + 2 + len(s.Field) + 4 + 8 + 8 + 4
	for k, ids := range s.Keys {
		if err := checkSidecarString("key", k); err != nil {
			return fmt.Errorf("segment: snapshot %s: %w", path, err)
		}
		if uint64(len(ids)) > maxSidecarCount {
			return fmt.Errorf("segment: snapshot %s: key has %d ids, max %d", path, len(ids), uint64(maxSidecarCount))
		}
		size += snapshotKeyMinSize + len(k)
		for _, id := range ids {
			if err := checkSidecarString("id", id); err != nil {
				return fmt.Errorf("segment: snapshot %s: %w", path, err)
			}
			size += snapshotIDMinSize + len(id)
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)

	buf := newSidecar(snapshotMagic, size)
	buf = appendString(buf, s.Name)
	buf = appendString(buf, s.Field)
	buf = binary.LittleEndian.AppendUint32(buf, s.CoversSeg)
	buf = binary.LittleEndian.AppendUint64(buf, s.CoversPos)
	buf = binary.LittleEndian.AppendUint64(buf, s.CoversWAL)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(keys)))
	for _, k := range keys {
		ids := s.Keys[k]
		buf = appendString(buf, k)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ids)))
		for _, id := range ids {
			buf = appendString(buf, id)
		}
	}
	buf = finishSidecar(buf)
	if err := fsutil.WriteFileAtomic(path, buf, sidecarPerm); err != nil {
		return fmt.Errorf("segment: writing index snapshot: %w", err)
	}
	return nil
}

// ReadIndexSnapshot reads and verifies the snapshot of index name in dir.
// A missing file returns an error satisfying errors.Is(err,
// os.ErrNotExist); a damaged one returns an error wrapping ErrCorrupt.
// Keys is never nil.
func ReadIndexSnapshot(dir, name string) (*IndexSnapshot, error) {
	path, err := snapshotPath(dir, name)
	if err != nil {
		return nil, err
	}
	body, err := readSidecar(path, snapshotMagic)
	if err != nil {
		return nil, err
	}
	d := sidecarDecoder{b: body}
	s := &IndexSnapshot{Name: d.str(), Field: d.str()}
	s.CoversSeg = d.u32()
	s.CoversPos = d.u64()
	s.CoversWAL = d.u64()
	if d.err == nil && s.Name != name {
		return nil, sidecarCorrupt(path, fmt.Sprintf("snapshot is for index %q", s.Name))
	}
	n := d.count(snapshotKeyMinSize)
	s.Keys = make(map[string][]string, n)
	for i := 0; i < n && d.err == nil; i++ {
		k := d.str()
		m := d.count(snapshotIDMinSize)
		ids := make([]string, m)
		for j := range ids {
			ids[j] = d.str()
		}
		if d.err != nil {
			break
		}
		if _, dup := s.Keys[k]; dup {
			return nil, sidecarCorrupt(path, fmt.Sprintf("duplicate key %q", k))
		}
		s.Keys[k] = ids
	}
	if err := d.done(); err != nil {
		return nil, sidecarCorrupt(path, err.Error())
	}
	return s, nil
}

// RemoveIndexSnapshot deletes the snapshot of index name in dir and
// fsyncs dir. Removing a snapshot that does not exist is not an error.
func RemoveIndexSnapshot(dir, name string) error {
	path, err := snapshotPath(dir, name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("segment: removing index snapshot: %w", err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return fmt.Errorf("segment: removing index snapshot %s: %w", path, err)
	}
	return nil
}
