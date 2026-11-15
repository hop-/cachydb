package idxstore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
)

// orderedValues lists indexable values in the engine's sort order:
// missing < null < false < true < numbers (by value) < strings (by bytes).
// Neighbours are strictly ascending. missingValue stands for "the field is
// missing".
var orderedValues = []any{
	missingValue{}, nil, false, true,
	math.Inf(-1), -math.MaxFloat64, -1e10, -2, -1, -0.5, -math.SmallestNonzeroFloat64,
	0.0, math.SmallestNonzeroFloat64, 0.5, 1, 2, 1e10, math.MaxFloat64, math.Inf(1),
	"", "\x00", "\x00\x00", "\x00\x01", "\x00\xff", "\x01", "a", "a\x00", "a\x00\x00", "a\x00b",
	"a\x01", "ab", "a\xff", "b", "\xfe", "\xff", "\xff\x00", "\xff\xff",
}

type missingValue struct{}

// encodeTest encodes v (missingValue as the missing tag) and fails the test
// for a value that is not indexable.
func encodeTest(t testing.TB, v any) []byte {
	t.Helper()
	if _, ok := v.(missingValue); ok {
		return AppendMissing(nil)
	}
	enc, _, ok := EncodeValue(v)
	if !ok {
		t.Fatalf("EncodeValue(%#v) not indexable", v)
	}
	return enc
}

func TestEncodeValueExactBytes(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want string // hex
	}{
		{"null", nil, "10"},
		{"false", false, "20"},
		{"true", true, "21"},
		{"zero", 0.0, "308000000000000000"},
		{"negative zero is zero", math.Copysign(0, -1), "308000000000000000"},
		{"one", 1.0, "30bff0000000000000"},
		{"minus one", -1.0, "30400fffffffffffff"},
		{"empty string", "", "40"},
		{"string", "ab", "406162"},
		{"string with 0x00 escaped", "a\x00b", "406100ff62"},
		{"string with 0xff kept", "\xff", "40ff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, truncated, ok := EncodeValue(tt.v)
			if !ok || truncated || hex.EncodeToString(enc) != tt.want {
				t.Errorf("EncodeValue(%#v) = %x, truncated %v, ok %v; want %s", tt.v, enc, truncated, ok, tt.want)
			}
		})
	}
	if got := hex.EncodeToString(AppendMissing(nil)); got != "05" {
		t.Errorf("AppendMissing = %s, want 05", got)
	}
	if got := hex.EncodeToString(IndexKey([]byte{TagString, 'a'}, "id")); got != "406100006964" {
		t.Errorf("IndexKey = %s, want value, 00 00, id", got)
	}
	if got := hex.EncodeToString(ValuePrefix([]byte{TagString, 'a'})); got != "40610000" {
		t.Errorf("ValuePrefix = %s, want value and the terminator", got)
	}
	// AppendValue appends to what dst holds.
	if out, _, ok := AppendValue([]byte{0xAA}, true); !ok || !bytes.Equal(out, []byte{0xAA, TagTrue}) {
		t.Errorf("AppendValue(dst, true) = %x", out)
	}
}

func TestEncodeValueNumericTypes(t *testing.T) {
	want, _, _ := EncodeValue(float64(7))
	for _, v := range []any{
		int(7), int8(7), int16(7), int32(7), int64(7), uint(7), uint8(7), uint16(7), uint32(7), uint64(7),
		float32(7), json.Number("7"), json.Number("7.0"), json.Number("7e0"),
	} {
		got, truncated, ok := EncodeValue(v)
		if !ok || truncated || !bytes.Equal(got, want) {
			t.Errorf("EncodeValue(%T %v) = %x, %v, %v; want %x like float64(7)", v, v, got, truncated, ok, want)
		}
	}
}

func TestEncodeValueNotIndexable(t *testing.T) {
	for _, v := range []any{
		map[string]any{}, map[string]any{"a": 1.0}, []any{}, []any{1.0}, math.NaN(), float32(math.NaN()),
		json.Number("x"), []byte("a"), struct{}{}, complex(1, 2),
	} {
		out, truncated, ok := AppendValue([]byte{9}, v)
		if ok || truncated || !bytes.Equal(out, []byte{9}) {
			t.Errorf("AppendValue(%T %v) = %x, %v, %v; want dst unchanged and not ok", v, v, out, truncated, ok)
		}
	}
}

// TestKeyOrderMatchesSortOrder checks every pair of orderedValues: the
// encoded values compare as the values do, and index keys order by value,
// then by document id (also for ids holding 0x00 and 0xFF).
func TestKeyOrderMatchesSortOrder(t *testing.T) {
	ids := []string{"\x00", "\x00\x00", "\x00\xff", "a", "a\x00", "ab", "b", "\xff", "\xff\xff"} // ascending
	encs := make([][]byte, len(orderedValues))
	for i, v := range orderedValues {
		encs[i] = encodeTest(t, v)
	}
	for i := range orderedValues {
		for j := range orderedValues {
			want := cmpInt(i, j)
			if got := bytes.Compare(encs[i], encs[j]); got != want {
				t.Errorf("encodings of %#v (%x) and %#v (%x) compare %d, want %d", orderedValues[i], encs[i], orderedValues[j], encs[j], got, want)
			}
			for a, ida := range ids {
				for b, idb := range ids {
					w := want
					if w == 0 {
						w = cmpInt(a, b)
					}
					ka, kb := IndexKey(encs[i], ida), IndexKey(encs[j], idb)
					if got := bytes.Compare(ka, kb); got != w {
						t.Fatalf("keys (%#v, %q) and (%#v, %q) compare %d, want %d: %x %x", orderedValues[i], ida, orderedValues[j], idb, got, w, ka, kb)
					}
				}
			}
		}
	}
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// TestValuePrefixAndPrefixEnd checks the seek bounds of one value: the
// prefix starts exactly the keys of that value, and PrefixEnd is above all
// of them and at or below every key of a larger value.
func TestValuePrefixAndPrefixEnd(t *testing.T) {
	ids := []string{"\x00", "a", "\xff\xff\xff"}
	for i, v := range orderedValues {
		enc := encodeTest(t, v)
		prefix := ValuePrefix(enc)
		end := PrefixEnd(prefix)
		for j, w := range orderedValues {
			encW := encodeTest(t, w)
			for _, id := range ids {
				k := IndexKey(encW, id)
				if bytes.HasPrefix(k, prefix) != (i == j) {
					t.Errorf("ValuePrefix(%#v) is a prefix of the key of (%#v, %q): %v", v, w, id, i != j)
				}
				if c := bytes.Compare(k, end); (c < 0) != (j <= i) {
					t.Errorf("key of (%#v, %q) vs PrefixEnd of %#v: compare %d", w, id, v, c)
				}
				if c := bytes.Compare(k, prefix); (c >= 0) != (j >= i) {
					t.Errorf("key of (%#v, %q) vs ValuePrefix of %#v: compare %d", w, id, v, c)
				}
			}
		}
	}
	tests := []struct {
		in, want []byte
	}{
		{nil, nil},
		{[]byte{0xff, 0xff}, nil},
		{[]byte{1, 0xff}, []byte{2}},
		{[]byte{0x40, 0x61, 0, 0}, []byte{0x40, 0x61, 0, 1}},
	}
	for _, tt := range tests {
		if got := PrefixEnd(tt.in); !bytes.Equal(got, tt.want) {
			t.Errorf("PrefixEnd(%x) = %x, want %x", tt.in, got, tt.want)
		}
	}
	in := []byte{1, 2}
	_ = PrefixEnd(in)
	if !bytes.Equal(in, []byte{1, 2}) {
		t.Error("PrefixEnd changed its argument")
	}
}

// TestStartsWithPrefix checks that the encoding of a string is a prefix of
// the encoding of exactly the strings that start with it.
func TestStartsWithPrefix(t *testing.T) {
	strs := []string{"", "\x00", "\x00\x00", "a", "a\x00", "a\x00b", "ab", "abc", "b", "\xff"}
	for _, p := range strs {
		ep := encodeTest(t, p)
		for _, s := range strs {
			es := encodeTest(t, s)
			if bytes.HasPrefix(es, ep) != strings.HasPrefix(s, p) {
				t.Errorf("encoding of %q has the prefix of %q: %v, strings.HasPrefix %v", s, p, bytes.HasPrefix(es, ep), strings.HasPrefix(s, p))
			}
		}
	}
}

func TestDecodeValueRoundTrip(t *testing.T) {
	for _, v := range orderedValues {
		enc := encodeTest(t, v)
		got, missing, err := DecodeValue(enc)
		if err != nil {
			t.Fatalf("DecodeValue(%x) of %#v: %v", enc, v, err)
		}
		if _, isMissing := v.(missingValue); isMissing {
			if !missing || got != nil {
				t.Errorf("DecodeValue(missing) = %#v, %v", got, missing)
			}
			continue
		}
		want := v
		if n, ok := v.(int); ok {
			want = float64(n)
		}
		if missing || got != want {
			t.Errorf("DecodeValue(EncodeValue(%#v)) = %#v, missing %v", v, got, missing)
		}
		// Splitting an index key gives back the value and the id.
		k := IndexKey(enc, "id\x00\xff")
		e, id, err := SplitIndexKey(k)
		if err != nil || !bytes.Equal(e, enc) || string(id) != "id\x00\xff" {
			t.Errorf("SplitIndexKey(%x) = %x, %q, %v", k, e, id, err)
		}
	}
	got, _, err := DecodeValue(encodeTest(t, math.Copysign(0, -1)))
	if err != nil || got != 0.0 || math.Signbit(got.(float64)) {
		t.Errorf("-0 decodes to %#v, %v; want +0", got, err)
	}
	// The result does not alias the input.
	enc := encodeTest(t, "abc")
	s, _, _ := DecodeValue(enc)
	enc[1] = 'z'
	if s != "abc" {
		t.Errorf("decoded string changed with its input: %q", s)
	}
}

func TestEncodeValueTruncation(t *testing.T) {
	tests := []struct {
		name          string
		s             string
		wantLen       int
		wantTruncated bool
	}{
		{"fits exactly", strings.Repeat("x", MaxValueLen-1), MaxValueLen, false},
		{"one byte over", strings.Repeat("x", MaxValueLen), MaxValueLen, true},
		{"far over", strings.Repeat("x", 5000), MaxValueLen, true},
		{"escape fits exactly", strings.Repeat("x", MaxValueLen-3) + "\x00", MaxValueLen, false},
		{"escape not split at the boundary", strings.Repeat("x", MaxValueLen-2) + "\x00", MaxValueLen - 1, true},
		{"only zeros", strings.Repeat("\x00", 3000), MaxValueLen - 1, true},
		{"0xff bytes are not escaped", strings.Repeat("\xff", MaxValueLen-1), MaxValueLen, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, truncated, ok := EncodeValue(tt.s)
			if !ok || truncated != tt.wantTruncated || len(enc) != tt.wantLen {
				t.Fatalf("EncodeValue = %d bytes, truncated %v, ok %v; want %d bytes, truncated %v", len(enc), truncated, ok, tt.wantLen, tt.wantTruncated)
			}
			if err := checkValue(enc); err != nil {
				t.Fatalf("the encoding is not well formed: %v", err)
			}
			// A cut value is a prefix of the full encoding, so it decodes to a
			// prefix of the string and sorts at or below it.
			dec, _, err := DecodeValue(enc)
			if err != nil || !strings.HasPrefix(tt.s, dec.(string)) || (len(dec.(string)) < len(tt.s)) != tt.wantTruncated {
				t.Errorf("decoded %d bytes (%v), want a prefix of the %d-byte string", len(dec.(string)), err, len(tt.s))
			}
			e, id, err := SplitIndexKey(IndexKey(enc, "\x00id"))
			if err != nil || !bytes.Equal(e, enc) || string(id) != "\x00id" {
				t.Errorf("SplitIndexKey of a %d-byte value: %v", len(enc), err)
			}
		})
	}
	// Strings that share their first MaxValueLen-1 bytes truncate to the
	// same value: a truncated entry is a candidate only.
	long := strings.Repeat("x", 3000)
	a, _, _ := EncodeValue(long)
	b, _, _ := EncodeValue(long + "y")
	if !bytes.Equal(a, b) {
		t.Error("strings with the same first 1,023 bytes truncate differently")
	}
}

func TestMalformedKeys(t *testing.T) {
	long := append([]byte{TagString}, bytes.Repeat([]byte("x"), MaxValueLen)...) // 1,025 bytes
	for _, bad := range [][]byte{
		nil, {}, {0x99}, {0x30, 1, 2},
		{TagString, 0x00},                              // ends inside an escape
		{TagString, 0x00, 0x01},                        // bad escape
		{TagString, 'a'},                               // no terminator
		{TagString, 'a', 0, 0},                         // no id
		{TagNull, 0, 0},                                // no id
		{TagNull, 0x01, 0, 'a'},                        // no terminator after a fixed-size value
		{TagNumber, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 'a'}, // terminator missing after a number
		append(append(long, 0, 0), 'i'),                // value too long
	} {
		if _, _, err := SplitIndexKey(bad); !errors.Is(err, ErrBadKey) {
			t.Errorf("SplitIndexKey(%x) = %v, want ErrBadKey", bad, err)
		}
	}
	for _, bad := range [][]byte{
		nil, {0x99}, {TagNull, 0x00}, {TagNumber, 1}, {TagString, 0x00}, {TagString, 'a', 0, 0},
		{TagNumber, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // -0
		{TagNumber, 0xff, 0xf8, 0, 0, 0, 0, 0, 1},                   // NaN
		{TagNumber, 0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, // NaN (negative)
		long,
	} {
		if _, _, err := DecodeValue(bad); !errors.Is(err, ErrBadKey) {
			t.Errorf("DecodeValue(%x) = %v, want ErrBadKey", bad, err)
		}
	}
}
