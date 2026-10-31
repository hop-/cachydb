package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// reseal recomputes the trailing checksum of a sidecar file image, so tests
// can craft structurally invalid files that pass the crc check.
func reseal(data []byte) []byte {
	out := slices.Clone(data)
	end := len(out) - sidecarCRCSize
	binary.LittleEndian.PutUint32(out[end:], crc32.Checksum(out[:end], crcTable))
	return out
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return data
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// assertOnlyFiles fails if dir holds anything but the named files (e.g. a
// leftover temp file from an atomic write).
func assertOnlyFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name())
	}
	slices.Sort(got)
	slices.Sort(names)
	if !slices.Equal(got, names) {
		t.Fatalf("dir holds %v, want %v", got, names)
	}
}

func hintEntries(seg uint32, n int) []HintEntry {
	out := make([]HintEntry, 0, n)
	pos := uint64(headerSize)
	for i := range n {
		e := HintEntry{ID: fmt.Sprintf("doc-%07d", i), Type: TypePut, WALOffset: uint64(1000 + i)}
		switch i % 10 {
		case 3:
			e.Type = TypeTombstone
		case 9:
			e = HintEntry{Type: TypeCheckpoint, WALOffset: uint64(1000 + i)}
		}
		size := uint32(minRecordDiskSize + len(e.ID) + i%50)
		e.Loc = Location{Seg: seg, Pos: pos, Size: size}
		pos += uint64(size)
		out = append(out, e)
	}
	return out
}

func TestHintRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		seg     uint32
		entries []HintEntry
	}{
		{"nil entries", 1, nil},
		{"empty entries", 2, []HintEntry{}},
		{"single put", 3, []HintEntry{{ID: "a", Type: TypePut, WALOffset: 7, Loc: Location{Seg: 3, Pos: headerSize, Size: 40}}}},
		{"put tombstone checkpoint", 42, []HintEntry{
			{ID: "a", Type: TypePut, WALOffset: 7, Loc: Location{Seg: 42, Pos: headerSize, Size: 40}},
			{ID: "a", Type: TypeTombstone, WALOffset: 8, Loc: Location{Seg: 42, Pos: headerSize + 40, Size: 25}},
			{Type: TypeCheckpoint, WALOffset: 8, Loc: Location{Seg: 42, Pos: headerSize + 65, Size: minRecordDiskSize}},
		}},
		{"max id length and offsets", 1 << 31, []HintEntry{
			{ID: strings.Repeat("x", maxIDLen), Type: TypePut, WALOffset: 1<<64 - 1, Loc: Location{Seg: 1 << 31, Pos: 1<<63 + 5, Size: 1<<32 - 1}},
		}},
		{"large", 7, hintEntries(7, 100_000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteHint(dir, tt.seg, tt.entries); err != nil {
				t.Fatalf("WriteHint: %v", err)
			}
			assertOnlyFiles(t, dir, fmt.Sprintf("seg-%06d.hint", tt.seg))
			got, err := ReadHint(dir, tt.seg)
			if err != nil {
				t.Fatalf("ReadHint: %v", err)
			}
			if len(got) != len(tt.entries) {
				t.Fatalf("read %d entries, want %d", len(got), len(tt.entries))
			}
			for i := range got {
				if got[i] != tt.entries[i] {
					t.Fatalf("entry %d = %+v, want %+v", i, got[i], tt.entries[i])
				}
			}
		})
	}
}

func TestHintFileName(t *testing.T) {
	tests := []struct {
		seg  uint32
		want string
	}{
		{1, "seg-000001.hint"},
		{999999, "seg-999999.hint"},
		{1234567, "seg-1234567.hint"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteHint(dir, tt.seg, nil); err != nil {
				t.Fatalf("WriteHint: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, tt.want)); err != nil {
				t.Fatalf("hint file: %v", err)
			}
			if got := hintPath(dir, tt.seg); got != filepath.Join(dir, tt.want) {
				t.Errorf("hintPath = %s, want %s", got, filepath.Join(dir, tt.want))
			}
		})
	}
}

func TestWriteHintReplaces(t *testing.T) {
	dir := t.TempDir()
	if err := WriteHint(dir, 1, hintEntries(1, 10)); err != nil {
		t.Fatalf("WriteHint: %v", err)
	}
	want := hintEntries(1, 3)
	if err := WriteHint(dir, 1, want); err != nil {
		t.Fatalf("WriteHint (replace): %v", err)
	}
	got, err := ReadHint(dir, 1)
	if err != nil {
		t.Fatalf("ReadHint: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ReadHint = %+v, want %+v", got, want)
	}
	assertOnlyFiles(t, dir, "seg-000001.hint")
}

func TestWriteHintRejectsInvalidEntries(t *testing.T) {
	ok := Location{Seg: 1, Pos: headerSize, Size: 40}
	tests := []struct {
		name  string
		entry HintEntry
	}{
		{"location in another segment", HintEntry{ID: "a", Type: TypePut, Loc: Location{Seg: 2, Pos: headerSize, Size: 40}}},
		{"put without id", HintEntry{Type: TypePut, Loc: ok}},
		{"tombstone without id", HintEntry{Type: TypeTombstone, Loc: ok}},
		{"checkpoint with id", HintEntry{ID: "a", Type: TypeCheckpoint, Loc: ok}},
		{"unknown type", HintEntry{ID: "a", Type: 9, Loc: ok}},
		{"zero type", HintEntry{ID: "a", Loc: ok}},
		{"id too long", HintEntry{ID: strings.Repeat("x", maxIDLen+1), Type: TypePut, Loc: ok}},
		{"position inside header", HintEntry{ID: "a", Type: TypePut, Loc: Location{Seg: 1, Pos: 0, Size: 40}}},
		{"size below minimum record", HintEntry{ID: "a", Type: TypePut, Loc: Location{Seg: 1, Pos: headerSize, Size: minRecordDiskSize - 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteHint(dir, 1, []HintEntry{tt.entry}); err == nil {
				t.Fatal("WriteHint succeeded, want error")
			}
			assertOnlyFiles(t, dir)
		})
	}
}

func TestReadHintMissing(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadHint(dir, 1)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadHint error = %v, want os.ErrNotExist", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing hint reported as corrupt: %v", err)
	}
}

// corruptions returns damaged variants of a valid sidecar file image:
// every single-byte flip, every truncation, and trailing garbage.
func corruptions(valid []byte) map[string][]byte {
	out := make(map[string][]byte)
	for i := range valid {
		b := slices.Clone(valid)
		b[i] ^= 0x40
		out[fmt.Sprintf("flip byte %d", i)] = b
	}
	for n := 0; n < len(valid); n++ {
		out[fmt.Sprintf("truncate to %d", n)] = slices.Clone(valid[:n])
	}
	out["trailing garbage"] = append(slices.Clone(valid), 1, 2, 3, 4)
	return out
}

func TestReadHintCorrupt(t *testing.T) {
	src := t.TempDir()
	entries := hintEntries(5, 12)
	if err := WriteHint(src, 5, entries); err != nil {
		t.Fatalf("WriteHint: %v", err)
	}
	valid := readFile(t, hintPath(src, 5))

	for name, data := range corruptions(valid) {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, hintPath(dir, 5), data)
			got, err := ReadHint(dir, 5)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadHint = %d entries, err %v; want ErrCorrupt", len(got), err)
			}
		})
	}
}

func TestReadHintStructurallyInvalid(t *testing.T) {
	src := t.TempDir()
	if err := WriteHint(src, 5, hintEntries(5, 2)); err != nil {
		t.Fatalf("WriteHint: %v", err)
	}
	valid := readFile(t, hintPath(src, 5))
	countAt := sidecarHeaderSize + 4
	firstEntry := countAt + 4
	typeAt := firstEntry + 2 + len("doc-0000000")

	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"wrong segment id", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[sidecarHeaderSize:], 6)
			return b
		}},
		{"count too large", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[countAt:], 1<<32-1)
			return b
		}},
		{"count too small leaves trailing bytes", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[countAt:], 1)
			return b
		}},
		{"id length past end", func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[firstEntry:], 1<<16-1)
			return b
		}},
		{"unknown record type", func(b []byte) []byte {
			b[typeAt] = 9
			return b
		}},
		{"wrong version", func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[sidecarMagicLen:], sidecarVersion+1)
			return b
		}},
		{"index snapshot magic", func(b []byte) []byte {
			copy(b, snapshotMagic)
			return b
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, hintPath(dir, 5), reseal(tt.mutate(slices.Clone(valid))))
			if _, err := ReadHint(dir, 5); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadHint error = %v, want ErrCorrupt", err)
			}
		})
	}

	t.Run("hint of another segment under this name", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, hintPath(dir, 6), valid)
		if _, err := ReadHint(dir, 6); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("ReadHint error = %v, want ErrCorrupt", err)
		}
	})
}

func equalSnapshots(a, b *IndexSnapshot) bool {
	if a.Name != b.Name || a.Field != b.Field || a.CoversSeg != b.CoversSeg ||
		a.CoversPos != b.CoversPos || a.CoversWAL != b.CoversWAL || len(a.Keys) != len(b.Keys) {
		return false
	}
	for k, ids := range a.Keys {
		other, ok := b.Keys[k]
		if !ok || !slices.Equal(ids, other) {
			return false
		}
	}
	return true
}

func largeSnapshot() *IndexSnapshot {
	s := &IndexSnapshot{Name: "big", Field: "user.city", CoversSeg: 12, CoversPos: 1 << 26, CoversWAL: 1 << 40, Keys: map[string][]string{}}
	hot := make([]string, 200_000)
	for i := range hot {
		hot[i] = fmt.Sprintf("id-%07d", i)
	}
	s.Keys[`s:"hot"`] = hot
	for i := range 10_000 {
		s.Keys[fmt.Sprintf("n:%d", i)] = []string{fmt.Sprintf("a-%d", i), fmt.Sprintf("b-%d", i)}
	}
	return s
}

func TestIndexSnapshotRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		snap *IndexSnapshot
	}{
		{"nil keys", &IndexSnapshot{Name: "by_age", Field: "age"}},
		{"empty keys", &IndexSnapshot{Name: "by_age", Field: "age", CoversSeg: 1, CoversPos: headerSize, Keys: map[string][]string{}}},
		{"key with empty id list", &IndexSnapshot{Name: "e", Field: "f", Keys: map[string][]string{"k": {}}}},
		{"empty key and empty field", &IndexSnapshot{Name: "e", Keys: map[string][]string{"": {"x"}}}},
		{"several keys", &IndexSnapshot{
			Name: "by-city", Field: "address.city", CoversSeg: 3, CoversPos: 4096, CoversWAL: 77,
			Keys: map[string][]string{`s:"Paris"`: {"d3", "d1", "d2"}, `s:"Rome"`: {"d4"}, "n:1": {"d5"}, "null": {"d6", "d7"}},
		}},
		{"max offsets", &IndexSnapshot{Name: "m", Field: "x", CoversSeg: 1<<32 - 1, CoversPos: 1<<64 - 1, CoversWAL: 1<<64 - 1,
			Keys: map[string][]string{strings.Repeat("k", maxSidecarString): {strings.Repeat("i", maxSidecarString)}}}},
		{"large id lists", largeSnapshot()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteIndexSnapshot(dir, tt.snap); err != nil {
				t.Fatalf("WriteIndexSnapshot: %v", err)
			}
			assertOnlyFiles(t, dir, "idx-"+tt.snap.Name+".snap")
			got, err := ReadIndexSnapshot(dir, tt.snap.Name)
			if err != nil {
				t.Fatalf("ReadIndexSnapshot: %v", err)
			}
			if got.Keys == nil {
				t.Fatal("ReadIndexSnapshot returned nil Keys")
			}
			if !equalSnapshots(got, tt.snap) {
				t.Fatalf("round trip mismatch: got name=%q field=%q covers=(%d,%d,%d) %d keys",
					got.Name, got.Field, got.CoversSeg, got.CoversPos, got.CoversWAL, len(got.Keys))
			}
		})
	}
}

func TestIndexSnapshotDeterministic(t *testing.T) {
	keys := map[string][]string{}
	for i := range 500 {
		keys[fmt.Sprintf("k%d", i)] = []string{fmt.Sprintf("id%d", i)}
	}
	s := &IndexSnapshot{Name: "d", Field: "f", Keys: keys}
	a, b := t.TempDir(), t.TempDir()
	for _, dir := range []string{a, b} {
		if err := WriteIndexSnapshot(dir, s); err != nil {
			t.Fatalf("WriteIndexSnapshot: %v", err)
		}
	}
	if !bytes.Equal(readFile(t, filepath.Join(a, "idx-d.snap")), readFile(t, filepath.Join(b, "idx-d.snap"))) {
		t.Fatal("two writes of the same snapshot differ")
	}
}

func TestIndexSnapshotReplaceAndRemove(t *testing.T) {
	dir := t.TempDir()
	first := &IndexSnapshot{Name: "ix", Field: "a", Keys: map[string][]string{"1": {"x"}}}
	second := &IndexSnapshot{Name: "ix", Field: "a", CoversWAL: 9, Keys: map[string][]string{"2": {"y", "z"}}}
	if err := WriteIndexSnapshot(dir, first); err != nil {
		t.Fatalf("WriteIndexSnapshot: %v", err)
	}
	if err := WriteIndexSnapshot(dir, second); err != nil {
		t.Fatalf("WriteIndexSnapshot (replace): %v", err)
	}
	assertOnlyFiles(t, dir, "idx-ix.snap")
	got, err := ReadIndexSnapshot(dir, "ix")
	if err != nil {
		t.Fatalf("ReadIndexSnapshot: %v", err)
	}
	if !equalSnapshots(got, second) {
		t.Fatalf("ReadIndexSnapshot = %+v, want %+v", got, second)
	}

	if err := RemoveIndexSnapshot(dir, "ix"); err != nil {
		t.Fatalf("RemoveIndexSnapshot: %v", err)
	}
	assertOnlyFiles(t, dir)
	if _, err := ReadIndexSnapshot(dir, "ix"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadIndexSnapshot after remove: %v, want os.ErrNotExist", err)
	}
	if err := RemoveIndexSnapshot(dir, "ix"); err != nil {
		t.Fatalf("RemoveIndexSnapshot of missing snapshot: %v", err)
	}
}

func TestReadIndexSnapshotMissing(t *testing.T) {
	_, err := ReadIndexSnapshot(t.TempDir(), "nope")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadIndexSnapshot error = %v, want os.ErrNotExist", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing snapshot reported as corrupt: %v", err)
	}
}

func TestReadIndexSnapshotCorrupt(t *testing.T) {
	src := t.TempDir()
	s := &IndexSnapshot{Name: "c", Field: "f", CoversSeg: 2, CoversPos: 99, CoversWAL: 5,
		Keys: map[string][]string{"a": {"1", "2"}, "b": {"3"}}}
	if err := WriteIndexSnapshot(src, s); err != nil {
		t.Fatalf("WriteIndexSnapshot: %v", err)
	}
	valid := readFile(t, filepath.Join(src, "idx-c.snap"))

	for name, data := range corruptions(valid) {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, filepath.Join(dir, "idx-c.snap"), data)
			if _, err := ReadIndexSnapshot(dir, "c"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadIndexSnapshot error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestReadIndexSnapshotStructurallyInvalid(t *testing.T) {
	src := t.TempDir()
	s := &IndexSnapshot{Name: "c", Field: "f", Keys: map[string][]string{"a": {"1"}, "b": {"2"}}}
	if err := WriteIndexSnapshot(src, s); err != nil {
		t.Fatalf("WriteIndexSnapshot: %v", err)
	}
	valid := readFile(t, filepath.Join(src, "idx-c.snap"))
	keyCountAt := sidecarHeaderSize + 2 + 1 + 2 + 1 + 4 + 8 + 8
	firstKey := keyCountAt + 4
	secondKey := firstKey + 2 + 1 + 4 + 2 + 1

	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"key count too large", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[keyCountAt:], 1<<32-1)
			return b
		}},
		{"key count too small leaves trailing bytes", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[keyCountAt:], 1)
			return b
		}},
		{"id count too large", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[firstKey+2+1:], 1<<20)
			return b
		}},
		{"duplicate key", func(b []byte) []byte {
			b[secondKey+2] = 'a'
			return b
		}},
		{"hint magic", func(b []byte) []byte {
			copy(b, hintMagic)
			return b
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, filepath.Join(dir, "idx-c.snap"), reseal(tt.mutate(slices.Clone(valid))))
			if _, err := ReadIndexSnapshot(dir, "c"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadIndexSnapshot error = %v, want ErrCorrupt", err)
			}
		})
	}

	t.Run("snapshot of another index under this name", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "idx-other.snap"), valid)
		if _, err := ReadIndexSnapshot(dir, "other"); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("ReadIndexSnapshot error = %v, want ErrCorrupt", err)
		}
	})
}

func TestIndexSnapshotInvalidArguments(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../x", "a/b", `a\b`, "a\x00b", strings.Repeat("n", maxSidecarString+1)} {
		t.Run(fmt.Sprintf("name %.20q", name), func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteIndexSnapshot(dir, &IndexSnapshot{Name: name}); err == nil {
				t.Error("WriteIndexSnapshot succeeded, want error")
			}
			if _, err := ReadIndexSnapshot(dir, name); err == nil || errors.Is(err, os.ErrNotExist) {
				t.Errorf("ReadIndexSnapshot error = %v, want invalid-name error", err)
			}
			if err := RemoveIndexSnapshot(dir, name); err == nil {
				t.Error("RemoveIndexSnapshot succeeded, want error")
			}
			assertOnlyFiles(t, dir)
		})
	}

	long := strings.Repeat("v", maxSidecarString+1)
	for name, s := range map[string]*IndexSnapshot{
		"nil snapshot":   nil,
		"field too long": {Name: "a", Field: long},
		"key too long":   {Name: "a", Keys: map[string][]string{long: {"x"}}},
		"id too long":    {Name: "a", Keys: map[string][]string{"k": {long}}},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteIndexSnapshot(dir, s); err == nil {
				t.Error("WriteIndexSnapshot succeeded, want error")
			}
			assertOnlyFiles(t, dir)
		})
	}
}
