package wal

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

// allOpsRecords returns one representative record per Op.
func allOpsRecords() []Record {
	return []Record{
		{Op: OpPutDoc, DB: "shop", Coll: "users", ID: "u-1", Payload: []byte(`{"_id":"u-1","name":"Ann","age":31}`)},
		{Op: OpDeleteDoc, DB: "shop", Coll: "users", ID: "u-1"},
		{Op: OpCreateDB, DB: "shop"},
		{Op: OpDropDB, DB: "wal"},
		{Op: OpCreateColl, DB: "shop", Coll: "orders", Payload: []byte(`{"schema":null}`)},
		{Op: OpDropColl, DB: "shop", Coll: "orders"},
		{Op: OpCreateIndex, DB: "shop", Coll: "users", Payload: []byte(`{"name":"by_age","field":"age"}`)},
		{Op: OpDropIndex, DB: "shop", Coll: "users", Payload: []byte(`{"name":"by_age"}`)},
	}
}

func assertRecordEqual(t *testing.T, got, want Record) {
	t.Helper()
	if got.Offset != want.Offset || !got.Time.Equal(want.Time) || got.Op != want.Op ||
		got.DB != want.DB || got.Coll != want.Coll || got.ID != want.ID || !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("record mismatch:\n got  %+v (payload %q)\n want %+v (payload %q)", got, got.Payload, want, want.Payload)
	}
}

func TestRecordCodecRoundTrip(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 30, 45, 123456789, time.UTC)
	for i, r := range allOpsRecords() {
		t.Run(r.Op.String(), func(t *testing.T) {
			r.Offset = uint64(i) + 1
			r.Time = base.Add(time.Duration(i) * time.Second)
			buf := appendRecord([]byte("prefix"), &r)
			enc := buf[len("prefix"):]
			if int64(len(enc)) != recordSize(&r) {
				t.Fatalf("encoded %d bytes, recordSize says %d", len(enc), recordSize(&r))
			}
			got, err := decodeBody(enc[recordHeaderSize:])
			if err != nil {
				t.Fatalf("decodeBody: %v", err)
			}
			assertRecordEqual(t, got, r)
			// The decoded payload must not alias the encode buffer.
			if len(got.Payload) > 0 {
				enc[len(enc)-1] ^= 0xff
				if !bytes.Equal(got.Payload, r.Payload) {
					t.Error("decoded payload aliases the input buffer")
				}
			}
		})
	}
}

func TestDecodeBodyRejectsMalformed(t *testing.T) {
	r := Record{Offset: 1, Time: time.Unix(0, 1), Op: OpPutDoc, DB: "d", Coll: "c", ID: "i", Payload: []byte("{}")}
	body := appendRecord(nil, &r)[recordHeaderSize:]
	badOp := append([]byte(nil), body...)
	badOp[16] = 99
	tests := []struct {
		name string
		body []byte
	}{
		{name: "too short", body: body[:recordFixedBody-1]},
		{name: "truncated payload", body: body[:len(body)-1]},
		{name: "trailing bytes", body: append(append([]byte(nil), body...), 0)},
		{name: "unknown op", body: badOp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeBody(tt.body); !errors.Is(err, errBadBody) {
				t.Errorf("decodeBody error = %v, want errBadBody", err)
			}
		})
	}
}

func TestValidateRecord(t *testing.T) {
	long := strings.Repeat("x", maxNameLen+1)
	tests := []struct {
		name    string
		rec     Record
		wantErr bool
	}{
		{name: "valid", rec: Record{Op: OpCreateDB, DB: "d"}},
		{name: "max name length", rec: Record{Op: OpPutDoc, DB: strings.Repeat("x", maxNameLen)}},
		{name: "op zero", rec: Record{Op: 0, DB: "d"}, wantErr: true},
		{name: "op out of range", rec: Record{Op: OpDropIndex + 1, DB: "d"}, wantErr: true},
		{name: "db too long", rec: Record{Op: OpCreateDB, DB: long}, wantErr: true},
		{name: "coll too long", rec: Record{Op: OpCreateColl, DB: "d", Coll: long}, wantErr: true},
		{name: "id too long", rec: Record{Op: OpPutDoc, DB: "d", Coll: "c", ID: long}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRecord(&tt.rec)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRecord error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSegmentName(t *testing.T) {
	tests := []struct {
		name   string
		file   string
		want   uint64
		wantOK bool
	}{
		{name: "first", file: "wal-00000000000000000001.log", want: 1, wantOK: true},
		{name: "large", file: "wal-00000000000004213377.log", want: 4213377, wantOK: true},
		{name: "old naming", file: "wal-1700000000-1.log"},
		{name: "too few digits", file: "wal-0001.log"},
		{name: "non digit", file: "wal-0000000000000000000x.log"},
		{name: "wrong suffix", file: "wal-00000000000000000001.tmp"},
		{name: "overflow", file: "wal-99999999999999999999.log"},
		{name: "format file", file: "cachydb.format"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseSegmentName(tt.file)
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("parseSegmentName(%q) = %d, %v; want %d, %v", tt.file, got, ok, tt.want, tt.wantOK)
			}
		})
	}
	if got := segmentFileName(4213377); got != "wal-00000000000004213377.log" {
		t.Errorf("segmentFileName = %q", got)
	}
}
