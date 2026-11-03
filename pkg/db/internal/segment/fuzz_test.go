package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// fuzzRecords are valid records used to build fuzz seeds: small (codec 0)
// and large (zstd) puts, a tombstone and a checkpoint marker.
func fuzzRecords() []Record {
	return []Record{
		{Type: TypePut, WALOffset: 7, ID: "a", Doc: []byte(`{"_id":"a","n":1}`)},
		{Type: TypePut, WALOffset: 8, ID: "b", Doc: largeDoc("b", 300)},
		{Type: TypePut, WALOffset: 9, ID: "e"},
		{Type: TypeTombstone, WALOffset: 10, ID: "a"},
		{Type: TypeCheckpoint, WALOffset: 10},
	}
}

// FuzzSegmentRecordDecode checks that decodeBody never panics, rejects
// garbage with errBadBody, and that every record it accepts is valid and
// survives an encode/decode round trip unchanged.
func FuzzSegmentRecordDecode(f *testing.F) {
	for _, r := range fuzzRecords() {
		enc, err := appendRecord(nil, &r)
		if err != nil {
			f.Fatal(err)
		}
		body := enc[recordHeaderSize:]
		f.Add(body)
		f.Add(body[:len(body)-1])
		f.Add(append(bytes.Clone(body), 0))
		flipped := bytes.Clone(body)
		flipped[len(flipped)-1] ^= 0x01
		f.Add(flipped)
	}
	f.Add([]byte{})
	f.Add(make([]byte, recordFixedBody))

	f.Fuzz(func(t *testing.T, body []byte) {
		rec, err := decodeBody(body)
		if err != nil {
			if !errors.Is(err, errBadBody) {
				t.Fatalf("decodeBody error %v does not wrap errBadBody", err)
			}
			return
		}
		if err := validateRecord(&rec); err != nil {
			t.Fatalf("decoded record does not validate: %v", err)
		}
		enc, err := appendRecord(nil, &rec)
		if err != nil {
			t.Fatalf("appendRecord: %v", err)
		}
		again, err := decodeBody(enc[recordHeaderSize:])
		if err != nil {
			t.Fatalf("decoding the re-encoded record: %v", err)
		}
		if !sameRecord(rec, again) {
			t.Fatalf("round trip changed the record: %+v -> %+v", rec, again)
		}
	})
}

// sealSidecar builds a sidecar file image around body with a valid
// checksum.
func sealSidecar(magic string, version uint16, body []byte) []byte {
	buf := newSidecar(magic, version, len(body))
	buf = append(buf, body...)
	return finishSidecar(buf)
}

// FuzzSidecarRead writes random bytes as a hint file (kind even) or an
// index snapshot (kind odd) and checks that ReadHint and ReadIndexSnapshot
// never panic, fail only with ErrCorrupt, and that whatever they accept
// re-encodes to identical data. With seal set, the bytes are the body of a
// sidecar with a valid header and checksum, so the fuzzer reaches the
// structural checks behind the checksum.
func FuzzSidecarRead(f *testing.F) {
	const seg = 6
	seedDir := f.TempDir()
	if err := WriteHint(seedDir, seg, hintEntries(seg, 12)); err != nil {
		f.Fatal(err)
	}
	snap := &IndexSnapshot{Name: "ix", Field: "tags", CoversSeg: 3, CoversPos: 1234, CoversWAL: 99,
		Keys: map[string][]string{`s:"a"`: {"1", "2"}, `n:3`: {"3"}, `b:true`: {}}}
	if err := WriteIndexSnapshot(seedDir, snap); err != nil {
		f.Fatal(err)
	}
	for kind, path := range []string{hintPath(seedDir, seg), filepath.Join(seedDir, "idx-ix.snap")} {
		data, err := os.ReadFile(path)
		if err != nil {
			f.Fatal(err)
		}
		body := data[sidecarHeaderSize : len(data)-sidecarCRCSize]
		f.Add(uint8(kind), false, data)
		f.Add(uint8(kind), false, data[:len(data)-1])
		f.Add(uint8(kind), true, body)
		f.Add(uint8(kind), true, body[:len(body)-3])
		f.Add(uint8(kind), true, append(bytes.Clone(body), 0))
		huge := bytes.Clone(body)
		// The count after the hint's segment / the snapshot's fixed part.
		at := 4
		if kind == 1 {
			at = 2 + 2 + 2 + 4 + 4 + 8 + 8
		}
		binary.LittleEndian.PutUint32(huge[at:], 0xFFFFFFFF)
		f.Add(uint8(kind), true, huge)
	}
	f.Add(uint8(0), false, []byte{})
	f.Add(uint8(1), true, []byte{})

	f.Fuzz(func(t *testing.T, kind uint8, seal bool, data []byte) {
		dir := t.TempDir()
		if kind%2 == 0 {
			if seal {
				data = sealSidecar(hintMagic, hintVersion, data)
			}
			if err := os.WriteFile(hintPath(dir, seg), data, 0o644); err != nil {
				t.Fatal(err)
			}
			entries, err := ReadHint(dir, seg)
			if err != nil {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("ReadHint error %v does not wrap ErrCorrupt", err)
				}
				return
			}
			out := t.TempDir()
			if err := WriteHint(out, seg, entries); err != nil {
				t.Fatalf("WriteHint of accepted entries: %v", err)
			}
			if got := readFile(t, hintPath(out, seg)); !bytes.Equal(got, data) {
				t.Fatalf("hint re-encodes differently:\n got %x\nwant %x", got, data)
			}
			return
		}

		const name = "ix"
		if seal {
			data = sealSidecar(snapshotMagic, snapshotVersion, data)
		}
		if err := os.WriteFile(filepath.Join(dir, "idx-"+name+".snap"), data, 0o644); err != nil {
			t.Fatal(err)
		}
		s, err := ReadIndexSnapshot(dir, name)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadIndexSnapshot error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		out := t.TempDir()
		if err := WriteIndexSnapshot(out, s); err != nil {
			t.Fatalf("WriteIndexSnapshot of an accepted snapshot: %v", err)
		}
		again, err := ReadIndexSnapshot(out, name)
		if err != nil {
			t.Fatalf("reading the re-encoded snapshot: %v", err)
		}
		if !equalSnapshots(s, again) {
			t.Fatalf("snapshot re-encodes differently: %+v -> %+v", s, again)
		}
	})
}

// FuzzSegmentScan writes random bytes as a data segment and checks that
// Scan and Reader.Read never panic, fail only with ErrCorrupt, and that
// every record Scan yields is the CRC-checked record at its location.
func FuzzSegmentScan(f *testing.F) {
	const seg = 2
	seedDir := f.TempDir()
	w, err := Create(seedDir, seg)
	if err != nil {
		f.Fatal(err)
	}
	if _, err := w.Append(fuzzRecords()); err != nil {
		f.Fatal(err)
	}
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	valid, err := os.ReadFile(DataPath(seedDir, seg))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(valid[:headerSize])
	f.Add(valid[:headerSize-1])
	f.Add(valid[:len(valid)-2])
	f.Add(append(bytes.Clone(valid), make([]byte, 64)...))
	for _, at := range []int{4, headerSize + 1, headerSize + 8, len(valid) / 2} {
		b := bytes.Clone(valid)
		b[at] ^= 0x04
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		if err := os.WriteFile(DataPath(dir, seg), data, 0o644); err != nil {
			t.Fatal(err)
		}
		var got []scanned
		end, err := Scan(dir, seg, func(r Record, l Location) error {
			got = append(got, scanned{r, l})
			return nil
		})
		if err != nil && !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Scan error %v does not wrap ErrCorrupt", err)
		}
		if err == nil && end != int64(len(data)) {
			t.Fatalf("Scan returned nil at byte %d of %d", end, len(data))
		}
		pos := uint64(headerSize)
		for i, s := range got {
			if s.loc.Seg != seg || s.loc.Pos != pos {
				t.Fatalf("record %d at %+v, want position %d", i, s.loc, pos)
			}
			raw := data[s.loc.Pos : s.loc.Pos+uint64(s.loc.Size)]
			if crc32.Checksum(raw[recordHeaderSize:], crcTable) != binary.LittleEndian.Uint32(raw[4:]) {
				t.Fatalf("record %d at %+v has a bad checksum", i, s.loc)
			}
			pos += uint64(s.loc.Size)
		}
		if len(got) > 0 && int64(pos) != end {
			t.Fatalf("records end at %d, Scan reported %d", pos, end)
		}

		r, err := OpenReader(dir, seg)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("OpenReader error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
		for i, s := range got {
			rec, err := r.Read(s.loc)
			if err != nil {
				t.Fatalf("Read of scanned record %d: %v", i, err)
			}
			if !sameRecord(rec, s.rec) {
				t.Fatalf("Read and Scan disagree on record %d", i)
			}
		}
		// A location from garbage must fail cleanly, never panic.
		if len(data) >= 12 {
			loc := Location{Seg: seg, Pos: binary.LittleEndian.Uint64(data[:8]), Size: binary.LittleEndian.Uint32(data[8:12])}
			if loc.Size <= 1<<20 { // keep the read buffer small
				if _, err := r.Read(loc); err != nil && !errors.Is(err, ErrCorrupt) {
					t.Fatalf("Read(%+v) error %v does not wrap ErrCorrupt", loc, err)
				}
			}
		}
	})
}
