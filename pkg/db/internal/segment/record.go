package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// RecordType identifies the kind of a data record (§4.1). The numeric
// values are part of the on-disk format and must never change.
type RecordType uint8

// Data record types.
const (
	TypePut        RecordType = 1 // a document after-image
	TypeTombstone  RecordType = 2 // a deleted document
	TypeCheckpoint RecordType = 3 // checkpoint marker; WALOffset is the checkpoint value
)

// String returns the format name of the type (e.g. "put").
func (t RecordType) String() string {
	switch t {
	case TypePut:
		return "put"
	case TypeTombstone:
		return "tombstone"
	case TypeCheckpoint:
		return "checkpoint"
	default:
		return fmt.Sprintf("type(%d)", uint8(t))
	}
}

func (t RecordType) valid() bool { return t >= TypePut && t <= TypeCheckpoint }

// Record is one data-segment entry.
type Record struct {
	Type      RecordType
	WALOffset uint64 // WAL offset of the change; for TypeCheckpoint: the checkpoint value
	ID        string // document id; empty for TypeCheckpoint
	Doc       []byte // uncompressed JSON (TypePut only); compression is internal (zstd)
}

// Location addresses one record inside a data segment.
type Location struct {
	Seg  uint32
	Pos  uint64 // byte offset of the record start
	Size uint32 // full on-disk record size
}

// Per-record codecs (§4.1).
const (
	codecNone byte = 0
	codecZstd byte = 1
)

// maxZstdExpansion bounds the decompressed size of one compressed byte: a
// zstd block holds at most 128 KiB and needs at least a 3-byte header.
const maxZstdExpansion = 128 << 10

// minCompressSize is the document size below which documents are stored
// uncompressed (codec 0): zstd framing overhead outweighs any gain.
const minCompressSize = 64

// On-disk record layout (§4.1, little-endian):
//
//	u32 length        bytes after this field and crc
//	u32 crc32c        Castagnoli, over the bytes after this field
//	u8  type
//	u64 wal_offset
//	u16 id_len · id   (0 for a checkpoint marker)
//	u32 raw_len       uncompressed document length (0 for tombstone/marker)
//	u8  codec         1 = zstd, 0 = none
//	... body          (compressed) JSON document, put only
const (
	recordHeaderSize = 4 + 4             // length + crc
	recordFixedBody  = 1 + 8 + 2 + 4 + 1 // fixed part of the body
	maxIDLen         = math.MaxUint16
	// maxBodyLen keeps the full record size representable in Location.Size.
	maxBodyLen = math.MaxUint32 - recordHeaderSize
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// The shared zstd encoder and decoder. EncodeAll and DecodeAll are safe for
// concurrent use; both are created on first use.
var (
	zstdEncoder = sync.OnceValues(func() (*zstd.Encoder, error) {
		return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	})
	// DecodeAll is capped at the capacity of the destination, which
	// decodeBody sizes to the record's raw length, so a damaged frame
	// header cannot make the decoder allocate more.
	zstdDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
		return zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecodeAllCapLimit(true))
	})
)

// validateRecord reports whether r can be encoded.
func validateRecord(r *Record) error {
	switch r.Type {
	case TypePut, TypeTombstone:
		if r.ID == "" {
			return fmt.Errorf("segment: %s record without id", r.Type)
		}
		if len(r.ID) > maxIDLen {
			return fmt.Errorf("segment: record id is %d bytes, max %d", len(r.ID), maxIDLen)
		}
		if r.Type == TypeTombstone && len(r.Doc) != 0 {
			return errors.New("segment: tombstone record with a document")
		}
		if r.Type == TypePut && len(r.Doc) > math.MaxUint32 {
			return fmt.Errorf("segment: document is %d bytes, max %d", len(r.Doc), int64(math.MaxUint32))
		}
	case TypeCheckpoint:
		if r.ID != "" || len(r.Doc) != 0 {
			return errors.New("segment: checkpoint record with an id or document")
		}
	default:
		return fmt.Errorf("segment: invalid record type %d", uint8(r.Type))
	}
	return nil
}

// appendRecord encodes r (which must pass validateRecord) onto buf and
// returns the extended buffer. On error buf is returned unchanged in
// length.
func appendRecord(buf []byte, r *Record) ([]byte, error) {
	le := binary.LittleEndian
	start := len(buf)
	buf = le.AppendUint32(buf, 0) // length placeholder
	buf = le.AppendUint32(buf, 0) // crc placeholder
	buf = append(buf, byte(r.Type))
	buf = le.AppendUint64(buf, r.WALOffset)
	buf = le.AppendUint16(buf, uint16(len(r.ID)))
	buf = append(buf, r.ID...)
	if r.Type != TypePut {
		buf = le.AppendUint32(buf, 0)
		buf = append(buf, codecNone)
	} else {
		buf = le.AppendUint32(buf, uint32(len(r.Doc)))
		if len(r.Doc) < minCompressSize {
			buf = append(buf, codecNone)
			buf = append(buf, r.Doc...)
		} else {
			enc, err := zstdEncoder()
			if err != nil {
				return buf[:start], fmt.Errorf("segment: creating zstd encoder: %w", err)
			}
			buf = append(buf, codecZstd)
			buf = enc.EncodeAll(r.Doc, buf)
		}
	}
	body := len(buf) - start - recordHeaderSize
	if body > maxBodyLen {
		return buf[:start], fmt.Errorf("segment: record is %d bytes, max %d", body, int64(maxBodyLen))
	}
	le.PutUint32(buf[start:], uint32(body))
	le.PutUint32(buf[start+4:], crc32.Checksum(buf[start+recordHeaderSize:], crcTable))
	return buf, nil
}

var errBadBody = errors.New("malformed record body")

// decodeBody decodes the crc-covered part of a record, decompressing the
// document. The returned record does not alias body.
func decodeBody(body []byte) (Record, error) {
	le := binary.LittleEndian
	if len(body) < recordFixedBody {
		return Record{}, errBadBody
	}
	var r Record
	r.Type = RecordType(body[0])
	if !r.Type.valid() {
		return Record{}, fmt.Errorf("%w: unknown type %d", errBadBody, body[0])
	}
	r.WALOffset = le.Uint64(body[1:])
	idLen := int(le.Uint16(body[9:]))
	rest := body[11:]
	if len(rest) < idLen+4+1 {
		return Record{}, errBadBody
	}
	r.ID = string(rest[:idLen])
	rest = rest[idLen:]
	rawLen := le.Uint32(rest)
	codec := rest[4]
	payload := rest[5:]

	if r.Type != TypePut {
		if rawLen != 0 || codec != codecNone || len(payload) != 0 {
			return Record{}, fmt.Errorf("%w: %s record with a body", errBadBody, r.Type)
		}
		if (r.Type == TypeCheckpoint) != (r.ID == "") {
			return Record{}, fmt.Errorf("%w: %s record with bad id", errBadBody, r.Type)
		}
		return r, nil
	}
	if r.ID == "" {
		return Record{}, fmt.Errorf("%w: put record without id", errBadBody)
	}
	switch codec {
	case codecNone:
		if uint64(len(payload)) != uint64(rawLen) {
			return Record{}, fmt.Errorf("%w: body length %d, raw length %d", errBadBody, len(payload), rawLen)
		}
		r.Doc = append([]byte(nil), payload...)
	case codecZstd:
		// rawLen sizes the output buffer, so check it against the most
		// the payload can expand to before allocating: a zstd block
		// decodes to at most 128 KiB and takes at least 3 bytes.
		if uint64(rawLen) > uint64(len(payload))*maxZstdExpansion {
			return Record{}, fmt.Errorf("%w: raw length %d impossible for %d compressed bytes", errBadBody, rawLen, len(payload))
		}
		dec, err := zstdDecoder()
		if err != nil {
			return Record{}, fmt.Errorf("segment: creating zstd decoder: %w", err)
		}
		doc, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
		if err != nil {
			return Record{}, fmt.Errorf("%w: zstd: %v", errBadBody, err)
		}
		if uint64(len(doc)) != uint64(rawLen) {
			return Record{}, fmt.Errorf("%w: decompressed %d bytes, raw length %d", errBadBody, len(doc), rawLen)
		}
		r.Doc = doc
	default:
		return Record{}, fmt.Errorf("%w: unknown codec %d", errBadBody, codec)
	}
	return r, nil
}
