package idxstore

import (
	"bytes"
	"cmp"
	"errors"
	"math"
	"strings"
	"testing"
)

// fuzzValue builds an indexable value from fuzz input: kind selects
// missing, null, false, true, a number, or a string. ok is false for NaN,
// which has no encoding.
func fuzzValue(kind uint8, num float64, str string) (v any, ok bool) {
	switch kind % 6 {
	case 0:
		return missingValue{}, true
	case 1:
		return nil, true
	case 2:
		return false, true
	case 3:
		return true, true
	case 4:
		return num, !math.IsNaN(num)
	}
	return str, true
}

// refRank and refCompare are the engine's sort order, written out naively:
// missing < null < false < true < numbers < strings.
func refRank(v any) int {
	switch t := v.(type) {
	case missingValue:
		return 0
	case nil:
		return 1
	case bool:
		if t {
			return 3
		}
		return 2
	case float64:
		return 4
	}
	return 5
}

func refCompare(a, b any) int {
	if c := cmp.Compare(refRank(a), refRank(b)); c != 0 {
		return c
	}
	switch x := a.(type) {
	case float64:
		return cmp.Compare(x, b.(float64)) // -0 == 0
	case string:
		return strings.Compare(x, b.(string))
	}
	return 0
}

// FuzzIndexKey checks the index key encoding on random pairs of values and
// ids: the encoding round-trips (up to -0 and truncation), byte order of
// encoded values equals the engine's sort order (monotone for truncated
// strings), index keys order by value and then by id, and SplitIndexKey
// takes a key apart again.
func FuzzIndexKey(f *testing.F) {
	f.Add(uint8(4), 1.5, "", "a", uint8(4), -1.5, "", "b")
	f.Add(uint8(4), 0.0, "", "a", uint8(4), math.Copysign(0, -1), "", "a")
	f.Add(uint8(5), 0.0, "a\x00b", "x", uint8(5), 0.0, "a\x00", "\x00")
	f.Add(uint8(5), 0.0, "a", "\xff", uint8(5), 0.0, "a\x00", "\x00")
	f.Add(uint8(0), 0.0, "", "id", uint8(1), 0.0, "", "id")
	f.Add(uint8(2), 0.0, "", "id", uint8(3), 0.0, "", "id")
	f.Add(uint8(5), 0.0, strings.Repeat("x", 1100), "a", uint8(5), 0.0, strings.Repeat("x", 1100)+"y", "b")
	f.Add(uint8(5), 0.0, strings.Repeat("\x00", 600), "a", uint8(5), 0.0, strings.Repeat("\x00", 511)+"\x01", "b")
	f.Add(uint8(4), math.Inf(-1), "", "a", uint8(4), -math.MaxFloat64, "", "a")
	f.Fuzz(func(t *testing.T, kindA uint8, numA float64, strA, idA string, kindB uint8, numB float64, strB, idB string) {
		a, okA := fuzzValue(kindA, numA, strA)
		b, okB := fuzzValue(kindB, numB, strB)
		if !okA || !okB {
			if _, _, ok := EncodeValue(math.NaN()); ok {
				t.Fatal("NaN has an encoding")
			}
			return
		}
		if idA == "" || idB == "" || len(idA) > MaxIDLen || len(idB) > MaxIDLen {
			return
		}
		encode := func(v any) ([]byte, bool) {
			if _, ok := v.(missingValue); ok {
				return AppendMissing(nil), false
			}
			enc, truncated, ok := EncodeValue(v)
			if !ok {
				t.Fatalf("EncodeValue(%#v) not indexable", v)
			}
			if len(enc) > MaxValueLen {
				t.Fatalf("EncodeValue(%#v) is %d bytes, max %d", v, len(enc), MaxValueLen)
			}
			return enc, truncated
		}
		ea, truncA := encode(a)
		eb, truncB := encode(b)

		// Round trip.
		for _, x := range []struct {
			v         any
			enc       []byte
			truncated bool
		}{{a, ea, truncA}, {b, eb, truncB}} {
			got, missing, err := DecodeValue(x.enc)
			if err != nil {
				t.Fatalf("DecodeValue(%x) of %#v: %v", x.enc, x.v, err)
			}
			if _, isMissing := x.v.(missingValue); isMissing != missing {
				t.Fatalf("DecodeValue(%x) missing = %v for %#v", x.enc, missing, x.v)
			}
			switch want := x.v.(type) {
			case missingValue:
			case string:
				s := got.(string)
				if x.truncated {
					if !strings.HasPrefix(want, s) || len(s) >= len(want) {
						t.Fatalf("truncated %q decodes to %q, want a shorter prefix", want, s)
					}
				} else if s != want {
					t.Fatalf("%q decodes to %q", want, s)
				}
			case float64:
				if got != want || math.Signbit(got.(float64)) != (want < 0) {
					t.Fatalf("%v decodes to %#v", want, got)
				}
			default:
				if got != x.v {
					t.Fatalf("%#v decodes to %#v", x.v, got)
				}
			}
		}

		// Value order.
		want := refCompare(a, b)
		gotVal := bytes.Compare(ea, eb)
		if truncA || truncB {
			if want < 0 && gotVal > 0 || want > 0 && gotVal < 0 {
				t.Fatalf("truncated values out of order: %q vs %q: compare %d, want %d", a, b, gotVal, want)
			}
		} else if gotVal != want {
			t.Fatalf("encodings of %#v (%x) and %#v (%x) compare %d, want %d", a, ea, b, eb, gotVal, want)
		}

		// Key order: by value, then by id.
		ka, kb := IndexKey(ea, idA), IndexKey(eb, idB)
		wantKey := gotVal
		if wantKey == 0 {
			wantKey = strings.Compare(idA, idB)
		}
		if got := bytes.Compare(ka, kb); got != wantKey {
			t.Fatalf("keys (%#v, %q) and (%#v, %q) compare %d, want %d", a, idA, b, idB, got, wantKey)
		}
		for _, k := range []struct {
			key, enc []byte
			id       string
		}{{ka, ea, idA}, {kb, eb, idB}} {
			enc, id, err := SplitIndexKey(k.key)
			if err != nil || !bytes.Equal(enc, k.enc) || string(id) != k.id {
				t.Fatalf("SplitIndexKey(%x) = %x, %q, %v; want %x, %q", k.key, enc, id, err, k.enc, k.id)
			}
			if !bytes.HasPrefix(k.key, ValuePrefix(k.enc)) {
				t.Fatalf("key %x does not start with its value prefix", k.key)
			}
		}
		if (bytes.HasPrefix(kb, ValuePrefix(ea))) != (gotVal == 0) {
			t.Fatalf("ValuePrefix(%x) prefix of key %x: values compare %d", ea, kb, gotVal)
		}
	})
}

// FuzzIndexKeyDecode feeds random bytes to the key readers: they never
// panic, reject what is malformed with ErrBadKey, and what they accept
// re-encodes to the same bytes.
func FuzzIndexKeyDecode(f *testing.F) {
	for _, seed := range [][]byte{
		IndexKey([]byte{TagString, 'a', 0, 0xff}, "id"),
		IndexKey([]byte{TagMissing}, "x"),
		IndexKey([]byte{TagNumber, 0xbf, 0xf0, 0, 0, 0, 0, 0, 0}, "\x00"),
		{TagString, 0, 1}, {TagNumber, 1}, {0x99, 0, 0, 'a'}, {},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key []byte) {
		enc, id, err := SplitIndexKey(key)
		if err == nil {
			if len(id) == 0 || !bytes.Equal(IndexKey(enc, string(id)), key) {
				t.Fatalf("SplitIndexKey(%x) = %x, %q: does not rebuild the key", key, enc, id)
			}
		}
		v, missing, err := DecodeValue(key)
		if err != nil {
			if !errors.Is(err, ErrBadKey) {
				t.Fatalf("DecodeValue(%x) = %v, want ErrBadKey", key, err)
			}
			return
		}
		var again []byte
		if missing {
			again = AppendMissing(nil)
		} else {
			var truncated, ok bool
			again, truncated, ok = EncodeValue(v)
			if !ok || truncated {
				t.Fatalf("decoded %#v from %x does not encode back (ok %v, truncated %v)", v, key, ok, truncated)
			}
		}
		if !bytes.Equal(again, key) {
			t.Fatalf("DecodeValue(%x) = %#v, which encodes to %x", key, v, again)
		}
	})
}
