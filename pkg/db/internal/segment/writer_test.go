package segment

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type scanned struct {
	rec Record
	loc Location
}

func scanAll(t *testing.T, dir string, seg uint32) ([]scanned, int64) {
	t.Helper()
	var out []scanned
	end, err := Scan(dir, seg, func(r Record, l Location) error {
		out = append(out, scanned{r, l})
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return out, end
}

func sameRecord(a, b Record) bool {
	return a.Type == b.Type && a.WALOffset == b.WALOffset && a.ID == b.ID && bytes.Equal(a.Doc, b.Doc)
}

func mixedBatch(base uint64) []Record {
	return []Record{
		{Type: TypePut, WALOffset: base, ID: fmt.Sprintf("doc-%d", base), Doc: []byte(`{"small":true}`)},
		{Type: TypePut, WALOffset: base + 1, ID: fmt.Sprintf("doc-%d", base+1), Doc: largeDoc("big", 2048)},
		{Type: TypeTombstone, WALOffset: base + 2, ID: fmt.Sprintf("doc-%d", base)},
		{Type: TypeCheckpoint, WALOffset: base + 2},
	}
}

func mustCreate(t *testing.T, dir string, seg uint32) *Writer {
	t.Helper()
	w, err := Create(dir, seg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return w
}

func mustAppend(t *testing.T, w *Writer, recs []Record) []Location {
	t.Helper()
	locs, err := w.Append(recs)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return locs
}

func mustClose(t *testing.T, w *Writer) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWriterAppendScanRead(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 1)
	if w.Seg() != 1 || w.Size() != headerSize {
		t.Fatalf("new writer: seg %d size %d", w.Seg(), w.Size())
	}
	recs := mixedBatch(10)
	locs := mustAppend(t, w, recs)
	if len(locs) != len(recs) {
		t.Fatalf("got %d locations for %d records", len(locs), len(recs))
	}
	pos := uint64(headerSize)
	for i, l := range locs {
		if l.Seg != 1 || l.Pos != pos {
			t.Errorf("loc[%d] = %+v, want seg 1 pos %d", i, l, pos)
		}
		pos += uint64(l.Size)
	}
	if w.Size() != int64(pos) {
		t.Errorf("Size = %d, want %d", w.Size(), pos)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	mustClose(t, w)

	// Codec bytes on disk: small doc stored raw, large doc zstd-compressed.
	data, err := os.ReadFile(DataPath(dir, 1))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []byte{codecNone, codecZstd} {
		l := locs[i]
		if got := codecOf(t, data[l.Pos:l.Pos+uint64(l.Size)]); got != want {
			t.Errorf("record %d codec = %d, want %d", i, got, want)
		}
	}

	got, end := scanAll(t, dir, 1)
	if end != int64(pos) {
		t.Errorf("Scan validEnd = %d, want %d", end, pos)
	}
	if len(got) != len(recs) {
		t.Fatalf("Scan saw %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if !sameRecord(got[i].rec, recs[i]) || got[i].loc != locs[i] {
			t.Errorf("scan[%d] = %+v @ %+v, want %+v @ %+v", i, got[i].rec, got[i].loc, recs[i], locs[i])
		}
	}

	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()
	for i, l := range locs {
		rec, err := r.Read(l)
		if err != nil {
			t.Fatalf("Read(%+v): %v", l, err)
		}
		if !sameRecord(rec, recs[i]) {
			t.Errorf("Read[%d] = %+v, want %+v", i, rec, recs[i])
		}
	}
	if _, err := r.Read(Location{Seg: 2, Pos: locs[0].Pos, Size: locs[0].Size}); err == nil {
		t.Errorf("Read with a foreign segment id succeeded")
	}
}

func TestAppendInvalidBatchWritesNothing(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 1)
	defer mustClose(t, w)
	if _, err := w.Append([]Record{{Type: TypePut, ID: "a"}, {Type: 99}}); err == nil {
		t.Fatal("Append of an invalid batch succeeded")
	}
	if w.Size() != headerSize {
		t.Errorf("Size = %d after a rejected batch, want %d", w.Size(), headerSize)
	}
	if locs, err := w.Append(nil); err != nil || locs != nil {
		t.Errorf("Append(nil) = %v, %v", locs, err)
	}
}

// Regression for F1: reopening the active segment and appending must never
// overwrite existing bytes.
func TestAppendAfterReopenPreservesData(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 3)
	first := mixedBatch(1)
	mustAppend(t, w, first)
	mustClose(t, w)
	before, err := os.ReadFile(DataPath(dir, 3))
	if err != nil {
		t.Fatal(err)
	}

	for round := range 2 { // reopen twice to cover repeated restarts
		w, err := OpenActive(dir, 3)
		if err != nil {
			t.Fatalf("OpenActive (round %d): %v", round, err)
		}
		if w.TruncatedBytes() != 0 {
			t.Errorf("TruncatedBytes = %d on a clean segment", w.TruncatedBytes())
		}
		mustAppend(t, w, mixedBatch(uint64(100*(round+1))))
		mustClose(t, w)
	}

	after, err := os.ReadFile(DataPath(dir, 3))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after[:headerSize], fileHeader()) {
		t.Errorf("header changed: %x", after[:headerSize])
	}
	if !bytes.HasPrefix(after, before) {
		t.Fatal("existing bytes were overwritten by an append after reopen")
	}
	want := slices.Concat(first, mixedBatch(100), mixedBatch(200))
	got, end := scanAll(t, dir, 3)
	if end != int64(len(after)) {
		t.Errorf("Scan validEnd = %d, file size %d", end, len(after))
	}
	if len(got) != len(want) {
		t.Fatalf("Scan saw %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !sameRecord(got[i].rec, want[i]) {
			t.Errorf("scan[%d] = %+v, want %+v", i, got[i].rec, want[i])
		}
	}
}

func TestOpenActiveTruncatesTornTail(t *testing.T) {
	tests := []struct {
		name string
		// damage receives the file contents and the start of the last
		// record and returns the damaged contents.
		damage func(data []byte, last int) []byte
		// keepLast: the final record is intact and must survive.
		keepLast bool
	}{
		{"partial final record", func(d []byte, last int) []byte { return d[:len(d)-5] }, false},
		{"partial record header", func(d []byte, last int) []byte { return d[:last+3] }, false},
		{"crc mismatch at EOF", func(d []byte, last int) []byte { d[len(d)-1] ^= 0xff; return d }, false},
		{"zeroed final record", func(d []byte, last int) []byte { clear(d[last:]); return d }, false},
		{"zero-filled tail after intact record", func(d []byte, last int) []byte { return append(d, make([]byte, 4096)...) }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			w := mustCreate(t, dir, 1)
			good := mixedBatch(1)
			goodLocs := mustAppend(t, w, good)
			last := Record{Type: TypePut, WALOffset: 50, ID: "torn", Doc: largeDoc("torn", 1024)}
			lastLoc := mustAppend(t, w, []Record{last})[0]
			mustClose(t, w)

			path := DataPath(dir, 1)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data = tt.damage(data, int(lastLoc.Pos))
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}

			goodEnd := int64(goodLocs[len(goodLocs)-1].Pos) + int64(goodLocs[len(goodLocs)-1].Size)
			if tt.keepLast {
				good = append(good, last)
				goodEnd = int64(lastLoc.Pos) + int64(lastLoc.Size)
			}
			w, err = OpenActive(dir, 1)
			if err != nil {
				t.Fatalf("OpenActive: %v", err)
			}
			if w.Size() != goodEnd {
				t.Errorf("Size = %d, want %d", w.Size(), goodEnd)
			}
			if w.TruncatedBytes() != int64(len(data))-goodEnd {
				t.Errorf("TruncatedBytes = %d, want %d", w.TruncatedBytes(), int64(len(data))-goodEnd)
			}
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if fi.Size() != goodEnd {
				t.Errorf("file size = %d after truncation, want %d", fi.Size(), goodEnd)
			}
			next := Record{Type: TypePut, WALOffset: 60, ID: "next", Doc: []byte(`{"n":1}`)}
			mustAppend(t, w, []Record{next})
			mustClose(t, w)

			got, _ := scanAll(t, dir, 1)
			want := append(good, next)
			if len(got) != len(want) {
				t.Fatalf("Scan saw %d records, want %d", len(got), len(want))
			}
			for i := range want {
				if !sameRecord(got[i].rec, want[i]) {
					t.Errorf("scan[%d] = %+v, want %+v", i, got[i].rec, want[i])
				}
			}
		})
	}
}

func TestOpenActiveMidFileCorruption(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 1)
	locs := mustAppend(t, w, mixedBatch(1))
	mustClose(t, w)
	path := DataPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[locs[0].Pos+recordHeaderSize+2] ^= 0xff // first record's body
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenActive(dir, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenActive error = %v, want ErrCorrupt", err)
	}
	// Scan reports the damage without touching the file.
	end, err := Scan(dir, 1, func(Record, Location) error { return nil })
	if !errors.Is(err, ErrCorrupt) || end != headerSize {
		t.Errorf("Scan = %d, %v; want %d, ErrCorrupt", end, err, headerSize)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() != int64(len(data)) {
		t.Errorf("file modified by failed OpenActive/Scan: %v, %v", fi, err)
	}
}

func TestOpenActiveHeader(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		wantErr bool
	}{
		{"empty file", nil, false},
		{"short header", []byte("CDB"), false},
		{"zero header", make([]byte, headerSize), false},
		{"bad magic", append([]byte("XXXX"), make([]byte, 12)...), true},
		{"bad version", func() []byte { h := fileHeader(); h[4] = 9; return h }(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(DataPath(dir, 1), tt.content, 0o644); err != nil {
				t.Fatal(err)
			}
			w, err := OpenActive(dir, 1)
			if tt.wantErr {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("OpenActive error = %v, want ErrCorrupt", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("OpenActive: %v", err)
			}
			if w.Size() != headerSize {
				t.Errorf("Size = %d, want %d", w.Size(), headerSize)
			}
			mustAppend(t, w, mixedBatch(1))
			mustClose(t, w)
			if got, _ := scanAll(t, dir, 1); len(got) != 4 {
				t.Errorf("Scan saw %d records, want 4", len(got))
			}
		})
	}
}

func TestReadDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 1)
	locs := mustAppend(t, w, mixedBatch(1))
	mustClose(t, w)
	path := DataPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	big := locs[1]
	data[big.Pos+uint64(big.Size)-3] ^= 0x55 // inside the compressed body
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	tests := []struct {
		name string
		loc  Location
	}{
		{"crc mismatch", big},
		{"size mismatch", Location{Seg: 1, Pos: locs[0].Pos, Size: locs[0].Size + 1}},
		{"past end of file", Location{Seg: 1, Pos: uint64(len(data)) - 4, Size: 64}},
		{"inside header", Location{Seg: 1, Pos: 0, Size: locs[0].Size}},
		{"too small", Location{Seg: 1, Pos: locs[0].Pos, Size: 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := r.Read(tt.loc); !errors.Is(err, ErrCorrupt) {
				t.Errorf("Read(%+v) error = %v, want ErrCorrupt", tt.loc, err)
			}
		})
	}
	if rec, err := r.Read(locs[0]); err != nil || rec.ID != "doc-1" {
		t.Errorf("Read of an intact record = %+v, %v", rec, err)
	}
}

func TestScanStopsOnCallbackError(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 1)
	locs := mustAppend(t, w, mixedBatch(1))
	mustClose(t, w)
	stop := errors.New("stop")
	n := 0
	end, err := Scan(dir, 1, func(Record, Location) error {
		n++
		if n == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || n != 2 {
		t.Fatalf("Scan = %v after %d calls, want stop after 2", err, n)
	}
	if want := int64(locs[0].Pos) + int64(locs[0].Size); end != want {
		t.Errorf("validEnd = %d, want %d", end, want)
	}
}

func TestCreateExistingFails(t *testing.T) {
	dir := t.TempDir()
	mustClose(t, mustCreate(t, dir, 1))
	if _, err := Create(dir, 1); !errors.Is(err, os.ErrExist) {
		t.Errorf("Create of an existing segment = %v, want ErrExist", err)
	}
}

func TestWriterClosed(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 1)
	mustClose(t, w)
	if _, err := w.Append(mixedBatch(1)); err == nil {
		t.Error("Append after Close succeeded")
	}
	if err := w.Sync(); err == nil {
		t.Error("Sync after Close succeeded")
	}
	if err := w.Close(); err == nil {
		t.Error("second Close succeeded")
	}
}

func TestDataPathAndListSegments(t *testing.T) {
	dir := t.TempDir()
	if got, want := DataPath(dir, 7), filepath.Join(dir, "seg-000007.data"); got != want {
		t.Errorf("DataPath = %q, want %q", got, want)
	}
	if got, want := DataPath(dir, 1234567), filepath.Join(dir, "seg-1234567.data"); got != want {
		t.Errorf("DataPath = %q, want %q", got, want)
	}
	files := []string{
		"seg-000010.data", "seg-000002.data", "seg-1234567.data",
		"seg-01.data", "seg-0000002.data", "seg-00000x.data", "seg-000003.hint",
		"seg-000004.data.tmp-1", "seg-99999999999.data", "other",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "seg-000005.data"), 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := ListSegments(dir)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if want := []uint32{2, 10, 1234567}; !slices.Equal(got, want) {
		t.Errorf("ListSegments = %v, want %v", got, want)
	}
	if _, err := ListSegments(filepath.Join(dir, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ListSegments(missing) = %v, want ErrNotExist", err)
	}
}
