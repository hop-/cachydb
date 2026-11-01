package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ByteSize is a size in bytes that parses and prints human-readable
// values such as "64MiB", "1GiB", "512KB", or "1048576". Binary units
// (KiB, MiB, GiB, TiB) are powers of 1024, decimal units (KB, MB, GB, TB)
// powers of 1000; a bare number or the "B" suffix means bytes. Units are
// case-insensitive and a fractional number ("1.5GiB") is allowed as long
// as the result is a whole, non-negative number of bytes.
//
// ByteSize implements envconfig.Decoder (env vars) and pflag.Value
// (command-line flags).
type ByteSize int64

// Binary size units.
const (
	KiB ByteSize = 1 << (10 * (iota + 1))
	MiB
	GiB
	TiB
)

// byteUnits maps lower-cased suffixes to their multiplier. Longer suffixes
// are matched first (see ParseByteSize).
var byteUnits = []struct {
	suffix string
	mult   int64
}{
	{"kib", int64(KiB)},
	{"mib", int64(MiB)},
	{"gib", int64(GiB)},
	{"tib", int64(TiB)},
	{"kb", 1e3},
	{"mb", 1e6},
	{"gb", 1e9},
	{"tb", 1e12},
	{"b", 1},
}

// ParseByteSize parses a human-readable size such as "64MiB".
func ParseByteSize(s string) (ByteSize, error) {
	in := strings.TrimSpace(s)
	if in == "" {
		return 0, fmt.Errorf("invalid size %q: empty", s)
	}
	lower := strings.ToLower(in)

	num, mult := lower, int64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(lower, u.suffix) {
			num, mult = strings.TrimSpace(strings.TrimSuffix(lower, u.suffix)), u.mult
			break
		}
	}
	if num == "" {
		return 0, fmt.Errorf("invalid size %q: missing number", s)
	}

	if n, err := strconv.ParseInt(num, 10, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("invalid size %q: must not be negative", s)
		}
		if n > math.MaxInt64/mult {
			return 0, fmt.Errorf("invalid size %q: too large", s)
		}
		return ByteSize(n * mult), nil
	}

	f, err := strconv.ParseFloat(num, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("invalid size %q: expected a number with an optional unit (B, KiB, MiB, GiB, TiB, KB, MB, GB, TB)", s)
	}
	if f < 0 {
		return 0, fmt.Errorf("invalid size %q: must not be negative", s)
	}
	total := f * float64(mult)
	if total >= math.MaxInt64 {
		return 0, fmt.Errorf("invalid size %q: too large", s)
	}
	if total != math.Trunc(total) {
		return 0, fmt.Errorf("invalid size %q: not a whole number of bytes", s)
	}
	return ByteSize(total), nil
}

// String formats b with the largest binary unit that divides it exactly,
// e.g. "64MiB", "1GiB", or "1000B". ParseByteSize(b.String()) == b.
func (b ByteSize) String() string {
	if b == 0 {
		return "0B"
	}
	for _, u := range []struct {
		name string
		mult ByteSize
	}{{"TiB", TiB}, {"GiB", GiB}, {"MiB", MiB}, {"KiB", KiB}} {
		if b%u.mult == 0 {
			return strconv.FormatInt(int64(b/u.mult), 10) + u.name
		}
	}
	return strconv.FormatInt(int64(b), 10) + "B"
}

// Decode implements envconfig.Decoder.
func (b *ByteSize) Decode(value string) error {
	return b.Set(value)
}

// Set implements pflag.Value.
func (b *ByteSize) Set(value string) error {
	v, err := ParseByteSize(value)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// Type implements pflag.Value.
func (b *ByteSize) Type() string {
	return "size"
}
