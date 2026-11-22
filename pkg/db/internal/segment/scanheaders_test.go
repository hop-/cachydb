package segment

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"testing"
)

// scanBoth runs Scan and ScanHeaders over segment seg and returns what
// each saw and returned.
func scanBoth(t *testing.T, dir string, seg uint32) (full, hdrs []scanned, fullEnd, hdrsEnd int64, fullErr, hdrsErr error) {
	t.Helper()
	fullEnd, fullErr = Scan(dir, seg, func(r Record, l Location) error {
		full = append(full, scanned{r, l})
		return nil
	})
	hdrsEnd, hdrsErr = ScanHeaders(dir, seg, func(r Record, l Location) error {
		hdrs = append(hdrs, scanned{r, l})
		return nil
	})
	return full, hdrs, fullEnd, hdrsEnd, fullErr, hdrsErr
}

// checkHeadersMatch checks that ScanHeaders saw the records Scan saw, at
// the same locations, without documents.
func checkHeadersMatch(t *testing.T, full, hdrs []scanned) {
	t.Helper()
	if len(hdrs) != len(full) {
		t.Fatalf("ScanHeaders saw %d records, Scan %d", len(hdrs), len(full))
	}
	for i := range full {
		f, h := full[i], hdrs[i]
		if h.rec.Type != f.rec.Type || h.rec.ID != f.rec.ID || h.rec.WALOffset != f.rec.WALOffset || h.loc != f.loc {
			t.Errorf("record %d: ScanHeaders %+v at %+v, Scan %+v at %+v", i, h.rec, h.loc, f.rec, f.loc)
		}
		if h.rec.Doc != nil {
			t.Errorf("record %d: ScanHeaders returned a document", i)
		}
	}
}

func TestScanHeadersAgreesWithScan(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 7)
	var want []Record
	for base := uint64(1); base < 40; base += 4 {
		batch := mixedBatch(base)
		mustAppend(t, w, batch)
		want = append(want, batch...)
	}
	mustClose(t, w)

	full, hdrs, fullEnd, hdrsEnd, fullErr, hdrsErr := scanBoth(t, dir, 7)
	if fullErr != nil || hdrsErr != nil {
		t.Fatalf("Scan = %v, ScanHeaders = %v", fullErr, hdrsErr)
	}
	if fullEnd != hdrsEnd {
		t.Errorf("valid end: Scan %d, ScanHeaders %d", fullEnd, hdrsEnd)
	}
	if fi, err := os.Stat(DataPath(dir, 7)); err != nil || fi.Size() != hdrsEnd {
		t.Errorf("ScanHeaders valid end %d, file size %v (%v)", hdrsEnd, fi.Size(), err)
	}
	if len(full) != len(want) {
		t.Fatalf("Scan saw %d records, want %d", len(full), len(want))
	}
	checkHeadersMatch(t, full, hdrs)
	for _, h := range hdrs {
		if h.loc.Seg != 7 {
			t.Errorf("location %+v does not name segment 7", h.loc)
		}
	}

	// An empty segment (header only) has no records.
	w = mustCreate(t, dir, 8)
	mustClose(t, w)
	n := 0
	end, err := ScanHeaders(dir, 8, func(Record, Location) error { n++; return nil })
	if err != nil || n != 0 || end != headerSize {
		t.Errorf("ScanHeaders of an empty segment = %d records, end %d, %v", n, end, err)
	}
	if _, err := ScanHeaders(dir, 99, func(Record, Location) error { return nil }); err == nil {
		t.Error("ScanHeaders of a missing segment succeeded")
	}
	stop := errors.New("stop")
	n = 0
	end, err = ScanHeaders(dir, 7, func(Record, Location) error {
		n++
		if n == 3 {
			return stop
		}
		return nil
	})
	if err != stop || end != int64(hdrs[1].loc.Pos)+int64(hdrs[1].loc.Size) {
		t.Errorf("ScanHeaders stopped by its callback = %d, %v; want the end of record 2 and the callback's error", end, err)
	}
}

// TestScanHeadersDamage: ScanHeaders stops at a torn tail or a checksum
// failure exactly where Scan does, with ErrCorrupt and the same valid end.
func TestScanHeadersDamage(t *testing.T) {
	tests := []struct {
		name   string
		damage func(d []byte, locs []Location) []byte
		// wantRecords is the number of intact records before the damage.
		wantRecords int
	}{
		{"partial final record", func(d []byte, _ []Location) []byte { return d[:len(d)-5] }, 4},
		{"partial record header", func(d []byte, l []Location) []byte { return d[:l[4].Pos+3] }, 4},
		{"crc mismatch at the end", func(d []byte, _ []Location) []byte { d[len(d)-1] ^= 0xff; return d }, 4},
		{"zeroed final record", func(d []byte, l []Location) []byte { clear(d[l[4].Pos:]); return d }, 4},
		{"crc mismatch in the middle", func(d []byte, l []Location) []byte { d[l[1].Pos+recordHeaderSize+3] ^= 0x55; return d }, 1},
		{"length out of range", func(d []byte, l []Location) []byte {
			binary.LittleEndian.PutUint32(d[l[2].Pos:], 1)
			return d
		}, 2},
		{"bad file header", func(d []byte, _ []Location) []byte { d[0] = 'X'; return d }, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			w := mustCreate(t, dir, 1)
			locs := mustAppend(t, w, mixedBatch(1))
			locs = append(locs, mustAppend(t, w, []Record{{Type: TypePut, WALOffset: 50, ID: "last", Doc: largeDoc("last", 1024)}})...)
			mustClose(t, w)
			path := DataPath(dir, 1)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data = tt.damage(data, locs)
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}

			full, hdrs, fullEnd, hdrsEnd, fullErr, hdrsErr := scanBoth(t, dir, 1)
			if !errors.Is(hdrsErr, ErrCorrupt) || !errors.Is(fullErr, ErrCorrupt) {
				t.Fatalf("ScanHeaders = %v, Scan = %v; want ErrCorrupt from both", hdrsErr, fullErr)
			}
			if hdrsEnd != fullEnd {
				t.Errorf("valid end: ScanHeaders %d, Scan %d", hdrsEnd, fullEnd)
			}
			if len(hdrs) != tt.wantRecords {
				t.Errorf("ScanHeaders saw %d records before the damage, want %d", len(hdrs), tt.wantRecords)
			}
			checkHeadersMatch(t, full, hdrs)
			if fi, err := os.Stat(path); err != nil || fi.Size() != int64(len(data)) {
				t.Error("ScanHeaders changed the file")
			}
		})
	}
}

// TestScanHeadersDoesNotDecompress: a put whose checksum is right but whose
// compressed document is garbage passes ScanHeaders (it never runs the
// decoder) and fails Scan.
func TestScanHeadersDoesNotDecompress(t *testing.T) {
	dir := t.TempDir()
	w := mustCreate(t, dir, 1)
	recs := mixedBatch(1) // record 1 holds a 2 KiB document: compressed
	locs := mustAppend(t, w, recs)
	mustClose(t, w)
	path := DataPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	big := locs[1]
	body := data[big.Pos+recordHeaderSize : big.Pos+uint64(big.Size)]
	if codec := body[11+len(recs[1].ID)+4]; codec != codecZstd {
		t.Fatalf("record 1 has codec %d, want zstd", codec)
	}
	for i := len(body) - 8; i < len(body)-1; i++ {
		body[i] ^= 0xA5 // inside the compressed payload
	}
	binary.LittleEndian.PutUint32(data[big.Pos+4:], crc32.Checksum(body, crcTable))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	var hdrs []scanned
	end, err := ScanHeaders(dir, 1, func(r Record, l Location) error {
		hdrs = append(hdrs, scanned{r, l})
		return nil
	})
	if err != nil || len(hdrs) != len(recs) || end != int64(len(data)) {
		t.Fatalf("ScanHeaders = %d records, end %d, %v; want all %d records", len(hdrs), end, err, len(recs))
	}
	if hdrs[1].rec.ID != recs[1].ID || hdrs[1].rec.Type != TypePut || hdrs[1].loc != big {
		t.Errorf("ScanHeaders record 1 = %+v at %+v", hdrs[1].rec, hdrs[1].loc)
	}
	if _, err := Scan(dir, 1, func(Record, Location) error { return nil }); err == nil {
		t.Error("Scan accepted a document that does not decompress")
	}
}
