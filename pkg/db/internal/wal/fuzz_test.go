package wal

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fuzzRecords are valid records used to build fuzz seeds.
func fuzzRecords() []Record {
	return []Record{
		{Offset: 1, Time: time.Unix(0, 1_700_000_000_000_000_000), Op: OpCreateDB, DB: "main"},
		{Offset: 2, Time: time.Unix(0, 1_700_000_000_000_000_001), Op: OpCreateColl, DB: "main", Coll: "users", Payload: []byte(`{"schema":null}`)},
		{Offset: 3, Time: time.Unix(0, 1_700_000_000_000_000_002), Op: OpPutDoc, DB: "main", Coll: "users", ID: "u1", Payload: []byte(`{"_id":"u1","name":"Ann","tags":["a","b"]}`)},
		{Offset: 4, Time: time.Unix(0, 1_700_000_000_000_000_003), Op: OpDeleteDoc, DB: "main", Coll: "users", ID: "u1"},
		{Offset: 5, Time: time.Unix(0, -1), Op: OpRenameColl, DB: "main", Coll: "users", Payload: []byte(`{"new_name":"people"}`)},
	}
}

// encodeSegment returns a segment file: the header followed by recs.
func encodeSegment(recs []Record) []byte {
	buf := segmentHeader()
	for i := range recs {
		buf = appendRecord(buf, &recs[i])
	}
	return buf
}

// FuzzWALRecordDecode checks that decodeBody never panics, and that every
// body it accepts re-encodes to exactly the same bytes.
func FuzzWALRecordDecode(f *testing.F) {
	for _, r := range fuzzRecords() {
		enc := appendRecord(nil, &r)
		body := enc[recordHeaderSize:]
		f.Add(body)
		f.Add(body[:len(body)-1])           // truncated
		f.Add(append(bytes.Clone(body), 0)) // trailing byte
		flipped := bytes.Clone(body)        // bit-flipped op
		flipped[16] ^= 0x80
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
		enc := appendRecord(nil, &rec)
		if got := enc[recordHeaderSize:]; !bytes.Equal(got, body) {
			t.Fatalf("re-encoded body differs:\n got %x\nwant %x", got, body)
		}
		if n := int64(len(enc)); n != recordSize(&rec) {
			t.Fatalf("encoded size %d, recordSize %d", n, recordSize(&rec))
		}
	})
}

// FuzzWALSegmentRead writes random bytes as the only WAL segment and checks
// that readSegment and Open never panic, fail only with ErrCorrupt, and
// never return a record that is not a valid, CRC-checked record of the
// file: every record they yield re-encodes to the file bytes at its
// position, and after Open the file is exactly the records it accepted.
func FuzzWALSegmentRead(f *testing.F) {
	recs := fuzzRecords()
	valid := encodeSegment(recs)
	f.Add(valid)
	f.Add(segmentHeader())
	f.Add(valid[:segmentHeaderSize-3])            // torn header
	f.Add(valid[:len(valid)-5])                   // torn tail
	f.Add(append(bytes.Clone(valid), 1, 2, 3, 4)) // garbage tail
	f.Add(append(bytes.Clone(valid), make([]byte, 100)...))
	for _, at := range []int{0, 5, segmentHeaderSize + 2, segmentHeaderSize + 9, len(valid) / 2, len(valid) - 1} {
		b := bytes.Clone(valid)
		b[at] ^= 0x10
		f.Add(b)
	}
	gap := encodeSegment([]Record{recs[0], recs[2]}) // offsets 1, 3
	f.Add(gap)
	f.Add(make([]byte, 40))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, segmentFileName(1))
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}

		// readSegment reads without repairing: every record it yields
		// must match the file bytes at its position.
		expected := uint64(1)
		pos := int64(segmentHeaderSize)
		err := readSegment(path, 1, math.MaxUint64, &expected, func(r Record) error {
			enc := appendRecord(nil, &r)
			end := pos + int64(len(enc))
			if end > int64(len(data)) || !bytes.Equal(data[pos:end], enc) {
				t.Fatalf("readSegment yielded record %d that is not in the file at byte %d", r.Offset, pos)
			}
			pos = end
			return nil
		})
		if err != nil && !errors.Is(err, ErrCorrupt) {
			t.Fatalf("readSegment: error %v does not wrap ErrCorrupt", err)
		}
		if err == nil && pos != int64(len(data)) {
			t.Fatalf("readSegment returned nil after %d of %d bytes", pos, len(data))
		}

		l, err := open(Options{Dir: dir, Logger: logger}, syncFile, time.Now)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open: error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		var got []Record
		rerr := l.ReadFrom(1, func(r Record) error {
			got = append(got, r)
			return nil
		})
		if err := l.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if rerr != nil {
			t.Fatalf("ReadFrom after a successful Open: %v", rerr)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// The reserved header bytes are not checked, so compare the header
		// by validity and the rest byte for byte.
		if len(after) < segmentHeaderSize || checkSegmentHeader(after) != nil {
			t.Fatalf("after Open the segment header is invalid: %x", after[:min(len(after), segmentHeaderSize)])
		}
		if want := encodeSegment(got); !bytes.Equal(after[segmentHeaderSize:], want[segmentHeaderSize:]) {
			t.Fatalf("after Open the segment is not exactly its %d accepted records", len(got))
		}
		// Open only ever truncates: the accepted records are a prefix of
		// the original file (a torn header is rewritten, so skip it).
		if len(data) >= segmentHeaderSize && len(after) > segmentHeaderSize &&
			!bytes.Equal(after[segmentHeaderSize:], data[segmentHeaderSize:len(after)]) {
			t.Fatalf("Open changed bytes instead of truncating")
		}
		for i, r := range got {
			if r.Offset != uint64(i+1) {
				t.Fatalf("record %d has offset %d", i, r.Offset)
			}
		}
	})
}
