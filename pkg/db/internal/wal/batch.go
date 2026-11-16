package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Batch payload (OpBatch, plan 013 §3.1), little-endian:
//
//	u32 count
//	count × { u8 op · u16 id_len · id · u32 doc_len · doc }
//
// op is OpPutDoc (doc: the document after-image, JSON with _id) or
// OpDeleteDoc (doc_len 0). Ids are not empty. The record's own id is empty;
// every item shares the record's offset.
const (
	batchCountSize    = 4
	batchItemOverhead = 1 + 2 + 4 // op + id_len + doc_len
	maxBatchItems     = math.MaxUint32
	maxBatchDocLen    = math.MaxUint32
)

// errBadBatch reports a batch payload that does not follow the layout.
var errBadBatch = errors.New("malformed batch payload")

// BatchItem is one document change of a batch record.
type BatchItem struct {
	Op  Op     // OpPutDoc or OpDeleteDoc
	ID  string // the document id; not empty, at most 65,535 bytes
	Doc []byte // OpPutDoc: the after-image (JSON, includes _id); OpDeleteDoc: empty
}

// BatchPayloadSize returns the encoded size of items as a batch payload.
func BatchPayloadSize(items []BatchItem) int64 {
	n := int64(batchCountSize)
	for i := range items {
		n += items[i].EncodedSize()
	}
	return n
}

// EncodedSize returns the number of bytes it takes in a batch payload.
func (it *BatchItem) EncodedSize() int64 {
	return batchItemOverhead + int64(len(it.ID)) + int64(len(it.Doc))
}

// checkBatchItem reports whether it can be encoded.
func checkBatchItem(it *BatchItem) error {
	switch it.Op {
	case OpPutDoc:
		if int64(len(it.Doc)) > maxBatchDocLen {
			return fmt.Errorf("document is %d bytes, max %d", len(it.Doc), int64(maxBatchDocLen))
		}
	case OpDeleteDoc:
		if len(it.Doc) != 0 {
			return fmt.Errorf("delete carries %d document bytes", len(it.Doc))
		}
	default:
		return fmt.Errorf("op %s is not put_doc or delete_doc", it.Op)
	}
	if it.ID == "" {
		return errors.New("empty id")
	}
	if len(it.ID) > maxNameLen {
		return fmt.Errorf("id is %d bytes, max %d", len(it.ID), maxNameLen)
	}
	return nil
}

// AppendBatch encodes items as a batch payload onto buf. It fails if an
// item cannot be encoded (see BatchItem) or there are more than 2³²−1
// items; buf is then returned unchanged.
func AppendBatch(buf []byte, items []BatchItem) ([]byte, error) {
	if int64(len(items)) > maxBatchItems {
		return buf, fmt.Errorf("wal: batch has %d items, max %d", len(items), int64(maxBatchItems))
	}
	for i := range items {
		if err := checkBatchItem(&items[i]); err != nil {
			return buf, fmt.Errorf("wal: batch item %d: %w", i, err)
		}
	}
	le := binary.LittleEndian
	buf = le.AppendUint32(buf, uint32(len(items)))
	for i := range items {
		it := &items[i]
		buf = append(buf, byte(it.Op))
		buf = appendString16(buf, it.ID)
		buf = le.AppendUint32(buf, uint32(len(it.Doc)))
		buf = append(buf, it.Doc...)
	}
	return buf, nil
}

// DecodeBatch decodes a batch payload. Every item must be valid (see
// BatchItem) and the payload must end with the last item. The returned
// documents alias payload; ids do not. Errors wrap a "malformed batch
// payload" error.
func DecodeBatch(payload []byte) ([]BatchItem, error) {
	le := binary.LittleEndian
	if len(payload) < batchCountSize {
		return nil, fmt.Errorf("%w: %d bytes, too short for the item count", errBadBatch, len(payload))
	}
	count := uint64(le.Uint32(payload))
	rest := payload[batchCountSize:]
	// Every item takes at least batchItemOverhead+1 bytes: never allocate
	// for more items than the payload can hold.
	if count > uint64(len(rest))/(batchItemOverhead+1) {
		return nil, fmt.Errorf("%w: %d items do not fit in %d bytes", errBadBatch, count, len(rest))
	}
	items := make([]BatchItem, count)
	for i := range items {
		if len(rest) < 1 {
			return nil, fmt.Errorf("%w: item %d: truncated", errBadBatch, i)
		}
		it := BatchItem{Op: Op(rest[0])}
		var ok bool
		if it.ID, rest, ok = takeString16(rest[1:]); !ok || len(rest) < 4 {
			return nil, fmt.Errorf("%w: item %d: truncated", errBadBatch, i)
		}
		n := uint64(le.Uint32(rest))
		rest = rest[4:]
		if uint64(len(rest)) < n {
			return nil, fmt.Errorf("%w: item %d: document of %d bytes is truncated", errBadBatch, i, n)
		}
		if n > 0 {
			it.Doc = rest[:n:n]
		}
		rest = rest[n:]
		if err := checkBatchItem(&it); err != nil {
			return nil, fmt.Errorf("%w: item %d: %w", errBadBatch, i, err)
		}
		items[i] = it
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d bytes after the last item", errBadBatch, len(rest))
	}
	return items, nil
}
