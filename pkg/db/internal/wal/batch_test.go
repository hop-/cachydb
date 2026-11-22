package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

// testBatchItems returns a batch of puts and deletes, an empty document
// and the longest id included.
func testBatchItems() []BatchItem {
	return []BatchItem{
		{Op: OpPutDoc, ID: "a", Doc: []byte(`{"_id":"a","v":1}`)},
		{Op: OpDeleteDoc, ID: "b"},
		{Op: OpPutDoc, ID: "c", Doc: []byte(`{}`)},
		{Op: OpPutDoc, ID: strings.Repeat("x", maxNameLen), Doc: []byte(`{"n":null}`)},
		{Op: OpDeleteDoc, ID: "é"},
	}
}

func TestBatchPayloadRoundTrip(t *testing.T) {
	for _, items := range [][]BatchItem{nil, testBatchItems()[:1], testBatchItems()} {
		payload, err := AppendBatch([]byte("prefix"), items)
		if err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		if !bytes.HasPrefix(payload, []byte("prefix")) {
			t.Fatal("AppendBatch did not append to buf")
		}
		payload = payload[len("prefix"):]
		if got := BatchPayloadSize(items); got != int64(len(payload)) {
			t.Errorf("BatchPayloadSize = %d, encoded %d bytes", got, len(payload))
		}
		if n := binary.LittleEndian.Uint32(payload); n != uint32(len(items)) {
			t.Errorf("count field = %d, want %d", n, len(items))
		}
		got, err := DecodeBatch(payload)
		if err != nil {
			t.Fatalf("DecodeBatch: %v", err)
		}
		if len(got) != len(items) {
			t.Fatalf("decoded %d items, want %d", len(got), len(items))
		}
		for i := range items {
			w, g := items[i], got[i]
			if g.Op != w.Op || g.ID != w.ID || !bytes.Equal(g.Doc, w.Doc) {
				t.Errorf("item %d = {%s %q %q}, want {%s %q %q}", i, g.Op, g.ID, g.Doc, w.Op, w.ID, w.Doc)
			}
			if w.Op == OpDeleteDoc && g.Doc != nil {
				t.Errorf("item %d: a delete decodes with document bytes %q", i, g.Doc)
			}
		}
		// Re-encoding the decoded items gives the same bytes.
		again, err := AppendBatch(nil, got)
		if err != nil || !bytes.Equal(again, payload) {
			t.Errorf("re-encoded payload differs (%v)", err)
		}
	}
}

func TestBatchItemEncodedSize(t *testing.T) {
	it := BatchItem{Op: OpPutDoc, ID: "abc", Doc: []byte(`{"x":1}`)}
	if got, want := it.EncodedSize(), int64(1+2+3+4+7); got != want {
		t.Errorf("EncodedSize = %d, want %d", got, want)
	}
	if got := BatchPayloadSize(nil); got != 4 {
		t.Errorf("BatchPayloadSize(nil) = %d, want 4", got)
	}
}

func TestAppendBatchRejectsInvalidItems(t *testing.T) {
	tests := []struct {
		name string
		item BatchItem
		want string
	}{
		{"empty id", BatchItem{Op: OpPutDoc, Doc: []byte(`{}`)}, "empty id"},
		{"id too long", BatchItem{Op: OpDeleteDoc, ID: strings.Repeat("x", maxNameLen+1)}, "id is"},
		{"delete with a document", BatchItem{Op: OpDeleteDoc, ID: "a", Doc: []byte(`{}`)}, "delete carries"},
		{"other op", BatchItem{Op: OpCreateDB, ID: "a"}, "is not put_doc or delete_doc"},
		{"nested batch", BatchItem{Op: OpBatch, ID: "a"}, "is not put_doc or delete_doc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := []byte("keep")
			items := []BatchItem{{Op: OpPutDoc, ID: "ok", Doc: []byte(`{}`)}, tt.item}
			got, err := AppendBatch(buf, items)
			if err == nil {
				t.Fatal("AppendBatch accepted the item")
			}
			if !strings.Contains(err.Error(), "batch item 1") || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not name item 1 and %q", err, tt.want)
			}
			if string(got) != "keep" {
				t.Errorf("buf changed to %q on error", got)
			}
		})
	}
}

// rawItem encodes one batch item without checking it.
func rawItem(op Op, id string, doc []byte) []byte {
	b := []byte{byte(op)}
	b = binary.LittleEndian.AppendUint16(b, uint16(len(id)))
	b = append(b, id...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(doc)))
	return append(b, doc...)
}

// rawBatch encodes count and then the item bytes.
func rawBatch(count uint32, items ...[]byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, count)
	for _, it := range items {
		b = append(b, it...)
	}
	return b
}

func TestDecodeBatchRejectsMalformed(t *testing.T) {
	good := rawItem(OpPutDoc, "a", []byte(`{}`))
	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{"empty", nil, "too short for the item count"},
		{"short count", []byte{1, 0, 0}, "too short for the item count"},
		{"count larger than the payload can hold", rawBatch(1 << 30), "do not fit"},
		{"count above the items", rawBatch(2, good, []byte{1, 0xff, 0xff, 0, 0, 0}), "item 1: truncated"},
		{"truncated id", rawBatch(1, []byte{1, 10, 0, 'a', 'b', 'c', 'd', 'e'}), "item 0: truncated"},
		{"truncated document length", rawBatch(1, []byte{1, 4, 0, 'a', 'b', 'c', 'd', 0}), "item 0: truncated"},
		{"document length past the end", rawBatch(1, []byte{1, 1, 0, 'a', 100, 0, 0, 0, '{', '}'}), "document of 100 bytes is truncated"},
		{"trailing bytes", append(rawBatch(1, good), 0), "after the last item"},
		{"empty id", rawBatch(1, rawItem(OpPutDoc, "", []byte(`{}`))), "empty id"},
		{"delete with a document", rawBatch(1, rawItem(OpDeleteDoc, "a", []byte(`{}`))), "delete carries"},
		{"unknown op", rawBatch(1, rawItem(Op(99), "a", nil)), "is not put_doc or delete_doc"},
		{"nested batch", rawBatch(1, rawItem(OpBatch, "a", []byte{0, 0, 0, 0})), "is not put_doc or delete_doc"},
		{"second item bad", rawBatch(2, good, rawItem(OpCreateColl, "b", nil)), "item 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items, err := DecodeBatch(tt.payload)
			if err == nil {
				t.Fatalf("DecodeBatch accepted %x as %d items", tt.payload, len(items))
			}
			if !errors.Is(err, errBadBatch) {
				t.Errorf("error %v does not wrap errBadBatch", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}

// TestDecodeBatchDocumentsAliasPayload: documents alias the payload (the
// engine keeps them as memtable payloads) but cannot grow into the next
// item; ids are copies.
func TestDecodeBatchDocumentsAliasPayload(t *testing.T) {
	payload, err := AppendBatch(nil, testBatchItems()[:3])
	if err != nil {
		t.Fatal(err)
	}
	items, err := DecodeBatch(payload)
	if err != nil {
		t.Fatal(err)
	}
	if cap(items[0].Doc) != len(items[0].Doc) {
		t.Errorf("document capacity %d > length %d: an append would overwrite the next item", cap(items[0].Doc), len(items[0].Doc))
	}
	id := items[0].ID
	for i := range payload {
		payload[i] = 'z'
	}
	if id != "a" || items[0].ID != "a" {
		t.Errorf("id changed with the payload: %q", items[0].ID)
	}
	if !bytes.Equal(items[0].Doc, bytes.Repeat([]byte("z"), len(items[0].Doc))) {
		t.Errorf("document %q does not alias the payload", items[0].Doc)
	}
}

// TestBatchRecordInLog: a batch record goes through the log like any other
// record, and its payload decodes after a reopen.
func TestBatchRecordInLog(t *testing.T) {
	env := newEnv(t, 0)
	l := env.mustOpen(t, nil)
	payload, err := AppendBatch(nil, testBatchItems())
	if err != nil {
		t.Fatal(err)
	}
	off := mustAppend(t, l, Record{Op: OpBatch, DB: "d", Coll: "c", Payload: payload})
	if _, err := l.Append(t.Context(), Record{Op: OpBatch, DB: "d", Coll: "c", ID: "x", Payload: payload}); err == nil {
		t.Error("Append accepted a batch record with an id")
	}
	mustClose(t, l)
	l = env.mustOpen(t, nil)
	recs := readAll(t, l, off)
	if len(recs) != 1 || recs[0].Op != OpBatch || recs[0].Op.String() != "batch" {
		t.Fatalf("read %+v, want one batch record", recs)
	}
	items, err := DecodeBatch(recs[0].Payload)
	if err != nil || len(items) != len(testBatchItems()) {
		t.Fatalf("DecodeBatch after reopen: %d items, %v", len(items), err)
	}
}

func TestOpString(t *testing.T) {
	if got := OpBatch.String(); got != "batch" {
		t.Errorf("OpBatch.String() = %q", got)
	}
	if OpBatch != 11 {
		t.Errorf("OpBatch = %d; WAL op numbers are part of the format", OpBatch)
	}
	if got := (OpBatch + 1).String(); got != "op(12)" {
		t.Errorf("(OpBatch+1).String() = %q", got)
	}
	if !OpBatch.valid() || (OpBatch + 1).valid() {
		t.Error("valid() does not end at OpBatch")
	}
}
