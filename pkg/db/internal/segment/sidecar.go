package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
)

// Sidecar files (hint files §4.2, index snapshots §8) share one framing,
// little-endian like the data segments:
//
//	[4]byte magic       "CDBH" (hint) or "CDBI" (index snapshot)
//	u16     version     sidecarVersion
//	...     body        format specific
//	u32     crc32c      Castagnoli, over every byte before it
//
// Strings are encoded as u16 length + bytes and counts as u32. Files are
// replaced atomically with fsutil.WriteFileAtomic, so a reader sees either
// the old or the new file; anything else (truncation, flipped bytes, a
// foreign file) fails the checksum or the structural checks and is reported
// as ErrCorrupt.
const (
	sidecarVersion    = 1
	sidecarMagicLen   = 4
	sidecarHeaderSize = sidecarMagicLen + 2 // magic + version
	sidecarCRCSize    = 4
	sidecarPerm       = 0o644
	maxSidecarString  = math.MaxUint16
	maxSidecarCount   = math.MaxUint32
)

// sidecarCorrupt builds an ErrCorrupt error naming the sidecar file.
func sidecarCorrupt(path, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrCorrupt, path, reason)
}

// newSidecar starts a sidecar buffer with the magic and version.
func newSidecar(magic string, sizeHint int) []byte {
	buf := make([]byte, 0, sidecarHeaderSize+sizeHint+sidecarCRCSize)
	buf = append(buf, magic...)
	return binary.LittleEndian.AppendUint16(buf, sidecarVersion)
}

// finishSidecar appends the checksum over everything written so far.
func finishSidecar(buf []byte) []byte {
	return binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, crcTable))
}

// appendString encodes s as u16 length + bytes; s must already be known to
// fit (len(s) <= maxSidecarString).
func appendString(buf []byte, s string) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

// readSidecar reads path and verifies its magic, version and checksum. It
// returns the body between the header and the checksum. A missing file is
// reported with an error wrapping os.ErrNotExist.
func readSidecar(path, magic string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("segment: reading %s: %w", path, err)
	}
	if len(data) < sidecarHeaderSize+sidecarCRCSize {
		return nil, sidecarCorrupt(path, fmt.Sprintf("file is %d bytes, too short", len(data)))
	}
	if !bytes.Equal(data[:sidecarMagicLen], []byte(magic)) {
		return nil, sidecarCorrupt(path, "bad magic")
	}
	if v := binary.LittleEndian.Uint16(data[sidecarMagicLen:]); v != sidecarVersion {
		return nil, sidecarCorrupt(path, fmt.Sprintf("unsupported version %d", v))
	}
	end := len(data) - sidecarCRCSize
	if got, want := crc32.Checksum(data[:end], crcTable), binary.LittleEndian.Uint32(data[end:]); got != want {
		return nil, sidecarCorrupt(path, "checksum mismatch")
	}
	return data[sidecarHeaderSize:end], nil
}

var errSidecarShort = errors.New("unexpected end of body")

// sidecarDecoder reads fixed-width values and strings from a sidecar body.
// The first failure is sticky in err; later reads return zero values.
type sidecarDecoder struct {
	b   []byte
	err error
}

func (d *sidecarDecoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if len(d.b) < n {
		d.err = errSidecarShort
		d.b = nil
		return nil
	}
	p := d.b[:n]
	d.b = d.b[n:]
	return p
}

func (d *sidecarDecoder) u8() uint8 {
	if p := d.take(1); p != nil {
		return p[0]
	}
	return 0
}

func (d *sidecarDecoder) u16() uint16 {
	if p := d.take(2); p != nil {
		return binary.LittleEndian.Uint16(p)
	}
	return 0
}

func (d *sidecarDecoder) u32() uint32 {
	if p := d.take(4); p != nil {
		return binary.LittleEndian.Uint32(p)
	}
	return 0
}

func (d *sidecarDecoder) u64() uint64 {
	if p := d.take(8); p != nil {
		return binary.LittleEndian.Uint64(p)
	}
	return 0
}

func (d *sidecarDecoder) str() string {
	n := int(d.u16())
	if p := d.take(n); p != nil {
		return string(p)
	}
	return ""
}

// count reads a u32 element count and rejects counts that cannot fit in
// the remaining body given each element's minimum encoded size, so a
// damaged count never triggers a huge allocation.
func (d *sidecarDecoder) count(minElemSize int) int {
	n := d.u32()
	if d.err != nil {
		return 0
	}
	if uint64(n)*uint64(minElemSize) > uint64(len(d.b)) {
		d.err = fmt.Errorf("count %d exceeds remaining %d bytes", n, len(d.b))
		d.b = nil
		return 0
	}
	return int(n)
}

// done reports any decoding failure, including trailing bytes.
func (d *sidecarDecoder) done() error {
	if d.err != nil {
		return d.err
	}
	if len(d.b) != 0 {
		return fmt.Errorf("%d trailing bytes", len(d.b))
	}
	return nil
}
