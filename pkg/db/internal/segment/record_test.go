package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"runtime"
	"strings"
	"testing"
)

// largeDoc returns a compressible JSON document of at least n bytes.
func largeDoc(id string, n int) []byte {
	var b strings.Builder
	b.WriteString(`{"_id":"` + id + `","items":[`)
	for i := 0; b.Len() < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"name":"item","qty":1,"tags":["a","b","c"]}`)
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

// codecOf returns the codec byte of the single encoded record in enc.
func codecOf(t *testing.T, enc []byte) byte {
	t.Helper()
	idLen := int(binary.LittleEndian.Uint16(enc[recordHeaderSize+9:]))
	return enc[recordHeaderSize+11+idLen+4]
}

func TestRecordRoundTrip(t *testing.T) {
	small := []byte(`{"_id":"a","n":1}`)
	big := largeDoc("b", 4096)
	tests := []struct {
		name      string
		rec       Record
		wantCodec byte
	}{
		{"put small doc uses codec 0", Record{Type: TypePut, WALOffset: 7, ID: "a", Doc: small}, codecNone},
		{"put 63-byte doc uses codec 0", Record{Type: TypePut, WALOffset: 8, ID: "c", Doc: bytes.Repeat([]byte("x"), minCompressSize-1)}, codecNone},
		{"put 64-byte doc uses zstd", Record{Type: TypePut, WALOffset: 9, ID: "d", Doc: bytes.Repeat([]byte("y"), minCompressSize)}, codecZstd},
		{"put large doc uses zstd", Record{Type: TypePut, WALOffset: 1 << 40, ID: "b", Doc: big}, codecZstd},
		{"put empty doc", Record{Type: TypePut, WALOffset: 3, ID: "e"}, codecNone},
		{"tombstone", Record{Type: TypeTombstone, WALOffset: 11, ID: "a"}, codecNone},
		{"checkpoint marker", Record{Type: TypeCheckpoint, WALOffset: 12}, codecNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateRecord(&tt.rec); err != nil {
				t.Fatalf("validateRecord: %v", err)
			}
			enc, err := appendRecord(nil, &tt.rec)
			if err != nil {
				t.Fatalf("appendRecord: %v", err)
			}
			if got := codecOf(t, enc); got != tt.wantCodec {
				t.Errorf("codec = %d, want %d", got, tt.wantCodec)
			}
			length := binary.LittleEndian.Uint32(enc)
			if int(length) != len(enc)-recordHeaderSize {
				t.Errorf("length field = %d, want %d", length, len(enc)-recordHeaderSize)
			}
			if crc := crc32.Checksum(enc[recordHeaderSize:], crcTable); crc != binary.LittleEndian.Uint32(enc[4:]) {
				t.Errorf("crc mismatch")
			}
			got, err := decodeBody(enc[recordHeaderSize:])
			if err != nil {
				t.Fatalf("decodeBody: %v", err)
			}
			if got.Type != tt.rec.Type || got.WALOffset != tt.rec.WALOffset || got.ID != tt.rec.ID || !bytes.Equal(got.Doc, tt.rec.Doc) {
				t.Errorf("round trip = %+v, want %+v", got, tt.rec)
			}
		})
	}
}

func TestLargeDocIsCompressed(t *testing.T) {
	doc := largeDoc("x", 16<<10)
	enc, err := appendRecord(nil, &Record{Type: TypePut, WALOffset: 1, ID: "x", Doc: doc})
	if err != nil {
		t.Fatalf("appendRecord: %v", err)
	}
	if len(enc) >= len(doc)/2 {
		t.Errorf("encoded record is %d bytes for a %d-byte compressible doc", len(enc), len(doc))
	}
}

func TestValidateRecordRejects(t *testing.T) {
	tests := []struct {
		name string
		rec  Record
	}{
		{"unknown type", Record{Type: 9, ID: "a"}},
		{"zero type", Record{ID: "a"}},
		{"put without id", Record{Type: TypePut, Doc: []byte("{}")}},
		{"tombstone without id", Record{Type: TypeTombstone}},
		{"tombstone with doc", Record{Type: TypeTombstone, ID: "a", Doc: []byte("{}")}},
		{"checkpoint with id", Record{Type: TypeCheckpoint, ID: "a"}},
		{"checkpoint with doc", Record{Type: TypeCheckpoint, Doc: []byte("{}")}},
		{"id too long", Record{Type: TypePut, ID: strings.Repeat("i", maxIDLen+1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateRecord(&tt.rec); err == nil {
				t.Errorf("validateRecord(%+v) = nil, want error", tt.rec)
			}
		})
	}
}

func TestDecodeBodyMalformed(t *testing.T) {
	good, err := appendRecord(nil, &Record{Type: TypePut, WALOffset: 1, ID: "a", Doc: []byte(`{"_id":"a"}`)})
	if err != nil {
		t.Fatal(err)
	}
	body := func() []byte { return append([]byte(nil), good[recordHeaderSize:]...) }
	tests := []struct {
		name string
		mut  func([]byte) []byte
	}{
		{"too short", func(b []byte) []byte { return b[:recordFixedBody-1] }},
		{"unknown type", func(b []byte) []byte { b[0] = 42; return b }},
		{"id longer than body", func(b []byte) []byte { binary.LittleEndian.PutUint16(b[9:], 1000); return b }},
		{"raw length mismatch", func(b []byte) []byte { binary.LittleEndian.PutUint32(b[12:], 3); return b }},
		{"unknown codec", func(b []byte) []byte { b[16] = 7; return b }},
		{"bad zstd body", func(b []byte) []byte { b[16] = codecZstd; return b }},
		{"truncated body", func(b []byte) []byte { return b[:len(b)-1] }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeBody(tt.mut(body())); !errors.Is(err, errBadBody) {
				t.Errorf("decodeBody error = %v, want errBadBody", err)
			}
		})
	}
}

// TestDecodeBodyZstdRawLength checks that the raw length of a zstd record
// (which sizes the output buffer) is checked against the payload before
// anything is allocated, and that the decoder never writes past it. A
// CRC-valid record with a huge raw length used to allocate that many
// bytes (found by FuzzSegmentRecordDecode).
func TestDecodeBodyZstdRawLength(t *testing.T) {
	doc := largeDoc("z", 4096)
	good, err := appendRecord(nil, &Record{Type: TypePut, WALOffset: 1, ID: "z", Doc: doc})
	if err != nil {
		t.Fatal(err)
	}
	rawLenAt := 1 + 8 + 2 + len("z")
	withRawLen := func(n uint32) []byte {
		b := append([]byte(nil), good[recordHeaderSize:]...)
		binary.LittleEndian.PutUint32(b[rawLenAt:], n)
		return b
	}
	tests := []struct {
		name   string
		rawLen uint32
	}{
		{"huge", 0xFFFFFFF0},
		{"beyond the payload's expansion", uint32(len(good)) * maxZstdExpansion},
		{"one short", uint32(len(doc)) - 1},
		{"one long", uint32(len(doc)) + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := withRawLen(tt.rawLen)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := decodeBody(body)
			runtime.ReadMemStats(&after)
			if !errors.Is(err, errBadBody) {
				t.Fatalf("decodeBody error = %v, want errBadBody", err)
			}
			if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
				t.Errorf("decodeBody allocated %d bytes for a %d-byte record", n, len(body))
			}
		})
	}

	// The tightest real compression (128 KiB RLE blocks) still decodes.
	big := bytes.Repeat([]byte("a"), 4<<20)
	enc, err := appendRecord(nil, &Record{Type: TypePut, WALOffset: 2, ID: "big", Doc: big})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := decodeBody(enc[recordHeaderSize:])
	if err != nil || !bytes.Equal(rec.Doc, big) {
		t.Fatalf("decoding a %d-byte document compressed to %d bytes: %v", len(big), len(enc), err)
	}
}
