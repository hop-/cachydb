package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

// Op identifies the kind of change a Record describes.
type Op uint8

// Record operations (§3.1). The numeric values are part of the on-disk
// format and must never change.
const (
	OpPutDoc      Op = 1 // payload: full document after-image (JSON, includes _id)
	OpDeleteDoc   Op = 2 // payload: none
	OpCreateDB    Op = 3 // payload: none
	OpDropDB      Op = 4 // payload: none
	OpCreateColl  Op = 5 // payload: {"schema": <Schema or null>}
	OpDropColl    Op = 6 // payload: none
	OpCreateIndex Op = 7 // payload: {"name":..., "field":...}
	OpDropIndex   Op = 8 // payload: {"name":...}
)

// String returns the format name of the op (e.g. "put_doc").
func (o Op) String() string {
	switch o {
	case OpPutDoc:
		return "put_doc"
	case OpDeleteDoc:
		return "delete_doc"
	case OpCreateDB:
		return "create_db"
	case OpDropDB:
		return "drop_db"
	case OpCreateColl:
		return "create_coll"
	case OpDropColl:
		return "drop_coll"
	case OpCreateIndex:
		return "create_index"
	case OpDropIndex:
		return "drop_index"
	default:
		return fmt.Sprintf("op(%d)", uint8(o))
	}
}

func (o Op) valid() bool { return o >= OpPutDoc && o <= OpDropIndex }

// Record is one WAL entry. Offset and Time are assigned by Log.Append; any
// values the caller sets are overwritten.
type Record struct {
	Offset       uint64    // assigned by Append; starts at 1
	Time         time.Time // assigned by Append
	Op           Op
	DB, Coll, ID string
	Payload      []byte // JSON per §3.1 table; may be empty
}

// On-disk record layout (§3.1, little-endian):
//
//	u32 length        bytes after this field and crc
//	u32 crc32c        Castagnoli, over the bytes after this field
//	u64 offset
//	i64 unix_nanos
//	u8  op
//	u16 db_len   · db bytes
//	u16 coll_len · coll bytes
//	u16 id_len   · id bytes
//	u32 payload_len · payload
const (
	recordHeaderSize = 4 + 4                     // length + crc
	recordFixedBody  = 8 + 8 + 1 + 2 + 2 + 2 + 4 // fixed part of the body
	maxNameLen       = math.MaxUint16
	maxBodyLen       = math.MaxUint32
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// bodySize is the number of bytes covered by the length field and the crc.
func bodySize(r *Record) int64 {
	return recordFixedBody + int64(len(r.DB)) + int64(len(r.Coll)) + int64(len(r.ID)) + int64(len(r.Payload))
}

// recordSize is the full on-disk size of r.
func recordSize(r *Record) int64 { return recordHeaderSize + bodySize(r) }

// validateRecord reports whether r can be encoded.
func validateRecord(r *Record) error {
	if !r.Op.valid() {
		return fmt.Errorf("wal: invalid record op %d", uint8(r.Op))
	}
	for _, f := range []struct{ name, val string }{{"db", r.DB}, {"coll", r.Coll}, {"id", r.ID}} {
		if len(f.val) > maxNameLen {
			return fmt.Errorf("wal: record %s is %d bytes, max %d", f.name, len(f.val), maxNameLen)
		}
	}
	if n := bodySize(r); n > maxBodyLen {
		return fmt.Errorf("wal: record is %d bytes, max %d", n, int64(maxBodyLen))
	}
	return nil
}

// appendRecord encodes r (which must pass validateRecord) onto buf.
func appendRecord(buf []byte, r *Record) []byte {
	le := binary.LittleEndian
	start := len(buf)
	buf = le.AppendUint32(buf, uint32(bodySize(r)))
	buf = le.AppendUint32(buf, 0) // crc placeholder
	buf = le.AppendUint64(buf, r.Offset)
	buf = le.AppendUint64(buf, uint64(r.Time.UnixNano()))
	buf = append(buf, byte(r.Op))
	buf = appendString16(buf, r.DB)
	buf = appendString16(buf, r.Coll)
	buf = appendString16(buf, r.ID)
	buf = le.AppendUint32(buf, uint32(len(r.Payload)))
	buf = append(buf, r.Payload...)
	le.PutUint32(buf[start+4:], crc32.Checksum(buf[start+recordHeaderSize:], crcTable))
	return buf
}

func appendString16(buf []byte, s string) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

var errBadBody = errors.New("malformed record body")

// decodeBody decodes the crc-covered part of a record. The returned record
// does not alias body.
func decodeBody(body []byte) (Record, error) {
	le := binary.LittleEndian
	if len(body) < recordFixedBody {
		return Record{}, errBadBody
	}
	var r Record
	r.Offset = le.Uint64(body[0:])
	r.Time = time.Unix(0, int64(le.Uint64(body[8:])))
	r.Op = Op(body[16])
	rest := body[17:]
	var ok bool
	if r.DB, rest, ok = takeString16(rest); !ok {
		return Record{}, errBadBody
	}
	if r.Coll, rest, ok = takeString16(rest); !ok {
		return Record{}, errBadBody
	}
	if r.ID, rest, ok = takeString16(rest); !ok {
		return Record{}, errBadBody
	}
	if len(rest) < 4 {
		return Record{}, errBadBody
	}
	n := le.Uint32(rest)
	rest = rest[4:]
	if uint64(len(rest)) != uint64(n) {
		return Record{}, errBadBody
	}
	if n > 0 {
		r.Payload = append([]byte(nil), rest...)
	}
	if !r.Op.valid() {
		return Record{}, fmt.Errorf("%w: unknown op %d", errBadBody, uint8(r.Op))
	}
	return r, nil
}

func takeString16(b []byte) (string, []byte, bool) {
	if len(b) < 2 {
		return "", nil, false
	}
	n := int(binary.LittleEndian.Uint16(b))
	b = b[2:]
	if len(b) < n {
		return "", nil, false
	}
	return string(b[:n]), b[n:], true
}
