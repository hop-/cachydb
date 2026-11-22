package idxstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	bolt "go.etcd.io/bbolt"
)

// Index keys.
//
// An index entry's key is
//
//	encoded value · 0x00 0x00 · document id
//
// and the encoding of a value is order-preserving: comparing two encoded
// values byte by byte gives the engine's sort order
// (missing < null < false < true < numbers < strings; numbers by value,
// strings by bytes). Within one value the entries are therefore in id
// order.
//
//	field missing   TagMissing
//	null            TagNull
//	false / true    TagFalse / TagTrue
//	number          TagNumber + 8 bytes: the IEEE-754 bits, big-endian, with
//	                the sign bit flipped for a number that is not negative
//	                and every bit inverted for a negative one; -0 is stored
//	                as 0 and NaN has no encoding
//	string          TagString + the bytes, 0x00 written as 0x00 0xFF
//
// A reader always knows where a value ends: every kind but a string has a
// fixed length, and inside a string a 0x00 is always followed by 0xFF, so
// the 0x00 0x00 terminator cannot be mistaken. The escape also keeps "a"
// before "a\x00" before "a\x01".
//
// The tags are part of the on-disk format and must never change. The gaps
// between them leave room for new value kinds.
const (
	TagMissing byte = 0x05
	TagNull    byte = 0x10
	TagFalse   byte = 0x20
	TagTrue    byte = 0x21
	TagNumber  byte = 0x30
	TagString  byte = 0x40
)

const (
	// MaxValueLen is the longest encoded value, tag included. A string
	// whose encoding would be longer is cut to at most this many bytes
	// (never in the middle of an escaped 0x00) and its entry carries
	// FlagTruncated.
	MaxValueLen = 1024

	// MaxIDLen is the longest document id the store can hold: bbolt keys
	// are limited to 32,768 bytes and an index key is a value of up to
	// MaxValueLen bytes, the terminator, and the id.
	MaxIDLen = bolt.MaxKeySize - MaxValueLen - terminatorLen

	terminatorLen = 2
	numberLen     = 1 + 8 // tag + bits
	escapeByte    = 0xFF  // follows a 0x00 that is part of a string
)

// Flags is the value byte of an index entry.
type Flags uint8

const (
	// FlagElement marks an entry for one element of an array field, as
	// opposed to the field's whole value.
	FlagElement Flags = 1 << 0
	// FlagTruncated marks an entry whose encoded value was cut at
	// MaxValueLen. Such an entry is a candidate only: its position among
	// values sharing the same first bytes is not exact, and the document
	// must be read to know its value.
	FlagTruncated Flags = 1 << 1

	flagsMask = FlagElement | FlagTruncated
)

// ErrBadKey is returned (wrapped) for bytes that are not a well-formed
// encoded value or index key.
var ErrBadKey = errors.New("idxstore: malformed index key")

func badKey(reason string) error { return fmt.Errorf("%w: %s", ErrBadKey, reason) }

// AppendMissing appends the encoding of "the field is missing" to dst.
func AppendMissing(dst []byte) []byte { return append(dst, TagMissing) }

// AppendValue appends the encoding of the scalar v to dst. ok is false,
// and dst is returned unchanged, when v is not indexable: an object, an
// array, NaN, or any type other than nil, bool, string, a Go numeric type,
// or a json.Number that parses. Every numeric type is encoded as its
// float64 value, so values the engine compares as equal get the same
// encoding. truncated reports that a string was cut at MaxValueLen.
//
// The encoding of a string is a prefix of the encoding of every string
// that starts with it, so it also serves as the seek prefix of a
// "starts with" search.
func AppendValue(dst []byte, v any) (out []byte, truncated, ok bool) {
	switch t := v.(type) {
	case nil:
		return append(dst, TagNull), false, true
	case bool:
		if t {
			return append(dst, TagTrue), false, true
		}
		return append(dst, TagFalse), false, true
	case string:
		out, truncated = appendString(dst, t)
		return out, truncated, true
	}
	f, ok := toFloat(v)
	if !ok || math.IsNaN(f) {
		return dst, false, false
	}
	return appendNumber(dst, f), false, true
}

// EncodeValue returns the encoding of the scalar v in a new slice; see
// AppendValue.
func EncodeValue(v any) (enc []byte, truncated, ok bool) {
	return AppendValue(nil, v)
}

// toFloat reports v as a float64 if it is a Go numeric value or a valid
// json.Number. It accepts the same types as the engine's numeric
// comparison.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

// appendNumber appends the encoding of f, which must not be NaN.
func appendNumber(dst []byte, f float64) []byte {
	if f == 0 {
		f = 0 // fold -0 into 0: they compare equal
	}
	bits := math.Float64bits(f)
	if bits>>63 == 0 {
		bits ^= 1 << 63 // not negative: above every negative number
	} else {
		bits = ^bits // negative: a larger magnitude sorts first
	}
	return binary.BigEndian.AppendUint64(append(dst, TagNumber), bits)
}

// appendString appends the encoding of s, cut so that the encoded value is
// at most MaxValueLen bytes. An escaped 0x00 is never split, so a cut value
// is MaxValueLen or MaxValueLen-1 bytes long.
func appendString(dst []byte, s string) (out []byte, truncated bool) {
	start := len(dst)
	dst = append(dst, TagString)
	for i := 0; i < len(s); i++ {
		c := s[i]
		need := 1
		if c == 0 {
			need = 2
		}
		if len(dst)-start+need > MaxValueLen {
			return dst, true
		}
		dst = append(dst, c)
		if c == 0 {
			dst = append(dst, escapeByte)
		}
	}
	return dst, false
}

// valueEnd returns the length of the encoded value at the start of b. A
// string ends at the first 0x00 0x00 or, when there is none, at the end of
// b; every other kind has a fixed length. It rejects an unknown tag, a
// short number, and a 0x00 in a string that is followed by neither 0xFF
// nor 0x00.
func valueEnd(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, badKey("empty value")
	}
	switch b[0] {
	case TagMissing, TagNull, TagFalse, TagTrue:
		return 1, nil
	case TagNumber:
		if len(b) < numberLen {
			return 0, badKey("short number")
		}
		return numberLen, nil
	case TagString:
		i := 1
		for i < len(b) {
			if b[i] != 0 {
				i++
				continue
			}
			if i+1 == len(b) {
				return 0, badKey("string ends inside an escape")
			}
			switch b[i+1] {
			case escapeByte:
				i += 2
			case 0:
				return i, nil
			default:
				return 0, badKey("bad escape in string")
			}
		}
		return i, nil
	}
	return 0, badKey(fmt.Sprintf("unknown tag 0x%02x", b[0]))
}

// checkValue reports whether enc is exactly one well-formed encoded value
// of at most MaxValueLen bytes.
func checkValue(enc []byte) error {
	n, err := valueEnd(enc)
	if err != nil {
		return err
	}
	if n != len(enc) {
		return badKey("bytes after the value")
	}
	if n > MaxValueLen {
		return badKey(fmt.Sprintf("value is %d bytes, max %d", n, MaxValueLen))
	}
	return nil
}

// DecodeValue returns the value enc encodes: nil for null, a bool, a
// float64, or a string. missing is true (and v nil) for the "field is
// missing" value. A truncated string decodes to the bytes that were kept.
// The result does not alias enc.
func DecodeValue(enc []byte) (v any, missing bool, err error) {
	if err := checkValue(enc); err != nil {
		return nil, false, err
	}
	switch enc[0] {
	case TagMissing:
		return nil, true, nil
	case TagNull:
		return nil, false, nil
	case TagFalse:
		return false, false, nil
	case TagTrue:
		return true, false, nil
	case TagNumber:
		bits := binary.BigEndian.Uint64(enc[1:])
		if bits>>63 != 0 {
			bits ^= 1 << 63
		} else {
			bits = ^bits
		}
		f := math.Float64frombits(bits)
		if math.IsNaN(f) || (f == 0 && math.Signbit(f)) {
			return nil, false, badKey("number is NaN or -0")
		}
		return f, false, nil
	default: // TagString; checkValue accepted the tag
		body := enc[1:]
		s := make([]byte, 0, len(body))
		for i := 0; i < len(body); i++ {
			s = append(s, body[i])
			if body[i] == 0 {
				i++ // skip the escape byte checkValue verified
			}
		}
		return string(s), false, nil
	}
}

// AppendIndexKey appends the index key of (enc, id) to dst: the encoded
// value, the terminator, and the document id.
func AppendIndexKey(dst, enc []byte, id string) []byte {
	dst = append(dst, enc...)
	dst = append(dst, 0, 0)
	return append(dst, id...)
}

// IndexKey returns the index key of (enc, id) in a new slice.
func IndexKey(enc []byte, id string) []byte {
	return AppendIndexKey(make([]byte, 0, len(enc)+terminatorLen+len(id)), enc, id)
}

// ValuePrefix returns the prefix shared by the index keys of every entry
// whose encoded value is exactly enc: enc and the terminator. Seeking to it
// positions a cursor on the value's first entry (its smallest id).
func ValuePrefix(enc []byte) []byte {
	return AppendIndexKey(make([]byte, 0, len(enc)+terminatorLen), enc, "")
}

// SplitIndexKey splits an index key into its encoded value and document
// id. Both alias key.
func SplitIndexKey(key []byte) (enc, id []byte, err error) {
	n, err := valueEnd(key)
	if err != nil {
		return nil, nil, err
	}
	if n > MaxValueLen {
		return nil, nil, badKey(fmt.Sprintf("value is %d bytes, max %d", n, MaxValueLen))
	}
	rest := key[n:]
	if len(rest) < terminatorLen || rest[0] != 0 || rest[1] != 0 {
		return nil, nil, badKey("no terminator after the value")
	}
	id = rest[terminatorLen:]
	if len(id) == 0 {
		return nil, nil, badKey("no document id")
	}
	return key[:n], id, nil
}

// PrefixEnd returns the smallest key that is greater than every key
// starting with prefix, in a new slice: the exclusive upper bound of the
// prefix's range. It returns nil when there is no such key (prefix is
// empty or all 0xFF), which means "to the end".
func PrefixEnd(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xFF {
			end := append([]byte(nil), prefix[:i+1]...)
			end[i]++
			return end
		}
	}
	return nil
}
