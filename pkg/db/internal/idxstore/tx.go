package idxstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

// Bucket and meta key names. They are part of the file format.
var (
	metaBucket = []byte("meta")
	keysBucket = []byte("keydir")

	keyFormat     = []byte("format")     // u32 layout version
	keyComplete   = []byte("complete")   // present once the file is whole
	keyCheckpoint = []byte("checkpoint") // u64 WAL offset
	keyCount      = []byte("count")      // u64 live documents
	keyCovered    = []byte("covered")    // u32 segment · u64 end position
	segKeyPrefix  = []byte("seg:")       // + u32 segment id -> i64 record bytes · i64 dead bytes
	ixKeyPrefix   = []byte("ix:")        // + index name -> i64 complex · i64 truncated · u8 ready

	ixBucketPrefix = []byte("ix/") // + index name: the index's bucket

	completeValue = []byte{1}

	// flagValues are the four possible index entry values. bbolt keeps a
	// reference to a value until the commit, so they must never change.
	flagValues = [flagsMask + 1][]byte{{0}, {1}, {2}, {3}}
)

const (
	entrySize      = 4 + 8 + 4 + 8
	coveredSize    = 4 + 8
	segStatsSize   = 8 + 8
	indexStatsSize = 8 + 8 + 1
)

// Entry is a key-table entry: where the live version of a document is in
// the data segments, and the WAL offset of the change that wrote it.
type Entry struct {
	Seg       uint32 // data segment id
	Pos       uint64 // byte offset of the record in the segment
	Size      uint32 // full on-disk size of the record
	WALOffset uint64
}

func encodeEntry(e Entry) []byte {
	be := binary.BigEndian
	b := make([]byte, 0, entrySize)
	b = be.AppendUint32(b, e.Seg)
	b = be.AppendUint64(b, e.Pos)
	b = be.AppendUint32(b, e.Size)
	return be.AppendUint64(b, e.WALOffset)
}

func decodeEntry(v []byte) (Entry, bool) {
	if len(v) != entrySize {
		return Entry{}, false
	}
	be := binary.BigEndian
	return Entry{Seg: be.Uint32(v), Pos: be.Uint64(v[4:]), Size: be.Uint32(v[12:]), WALOffset: be.Uint64(v[16:])}, true
}

// Position is a place in a collection's data segments: everything up to
// byte End of segment Seg. The zero Position means "nothing".
type Position struct {
	Seg uint32
	End uint64
}

// SegmentStats is the byte accounting of one live data segment.
// RecordBytes is the total size of its records; DeadBytes is the part the
// key table does not reference (superseded and deleted versions,
// tombstones, checkpoint markers). The difference is the size of the
// documents the key table points at in that segment; Check verifies it.
type SegmentStats struct {
	Seg         uint32
	RecordBytes int64
	DeadBytes   int64
}

// IndexStats describes one index. Complex is the number of documents
// whose value has no entry because it is not indexable; the caller
// maintains it with IndexWriter.AddComplex. Truncated is the number of
// entries carrying FlagTruncated; the store maintains it. Ready is false
// from CreateIndex until IndexWriter.SetReady: an index that is not ready
// is still being built, or its build was interrupted.
type IndexStats struct {
	Name      string
	Complex   int64
	Truncated int64
	Ready     bool
}

func encodeIndexStats(st IndexStats) []byte {
	be := binary.BigEndian
	b := make([]byte, 0, indexStatsSize)
	b = be.AppendUint64(b, uint64(st.Complex))
	b = be.AppendUint64(b, uint64(st.Truncated))
	if st.Ready {
		return append(b, 1)
	}
	return append(b, 0)
}

func decodeIndexStats(name string, v []byte) (IndexStats, bool) {
	if len(v) != indexStatsSize || v[16] > 1 {
		return IndexStats{}, false
	}
	be := binary.BigEndian
	return IndexStats{Name: name, Complex: int64(be.Uint64(v)), Truncated: int64(be.Uint64(v[8:])), Ready: v[16] == 1}, true
}

func segKey(seg uint32) []byte {
	return binary.BigEndian.AppendUint32(append(make([]byte, 0, len(segKeyPrefix)+4), segKeyPrefix...), seg)
}

func ixKey(name string) []byte {
	return append(append(make([]byte, 0, len(ixKeyPrefix)+len(name)), ixKeyPrefix...), name...)
}

func ixBucket(name string) []byte {
	return append(append(make([]byte, 0, len(ixBucketPrefix)+len(name)), ixBucketPrefix...), name...)
}

// reader is the read side shared by Tx and WTx. Its exported methods are
// the read API of both.
type reader struct {
	tx   *bolt.Tx
	meta *bolt.Bucket
	keys *bolt.Bucket
	path string
}

func newReader(btx *bolt.Tx, path string) (reader, error) {
	r := reader{tx: btx, meta: btx.Bucket(metaBucket), keys: btx.Bucket(keysBucket), path: path}
	if r.meta == nil || r.keys == nil {
		return reader{}, r.corrupt("the meta or keydir bucket is missing")
	}
	return r, nil
}

func (r *reader) corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s: %s", ErrCorrupt, r.path, fmt.Sprintf(format, args...))
}

// Key returns the key-table entry of the document id; ok is false when
// the key table has no such id.
func (r *reader) Key(id string) (e Entry, ok bool, err error) {
	v := r.keys.Get([]byte(id))
	if v == nil {
		return Entry{}, false, nil
	}
	e, ok = decodeEntry(v)
	if !ok {
		return Entry{}, false, r.corrupt("key table entry of %q is %d bytes", id, len(v))
	}
	return e, true, nil
}

// Checkpoint returns the WAL offset up to which the file reflects this
// collection's changes (0 initially).
func (r *reader) Checkpoint() (uint64, error) { return r.metaUint64(keyCheckpoint) }

// Count returns the number of documents in the key table. It is kept by
// PutKey and DeleteKey, so reading it costs nothing.
func (r *reader) Count() (int64, error) {
	n, err := r.metaUint64(keyCount)
	return int64(n), err
}

func (r *reader) metaUint64(key []byte) (uint64, error) {
	v := r.meta.Get(key)
	if len(v) != 8 {
		return 0, r.corrupt("meta %q is %d bytes", key, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// Covered returns the data position the file reflects: every record
// before it is accounted for in the key table and the indexes, and none
// after it. It is the zero Position until SetCovered is first called.
func (r *reader) Covered() (Position, error) {
	v := r.meta.Get(keyCovered)
	if v == nil {
		return Position{}, nil
	}
	if len(v) != coveredSize {
		return Position{}, r.corrupt("meta %q is %d bytes", keyCovered, len(v))
	}
	return Position{Seg: binary.BigEndian.Uint32(v), End: binary.BigEndian.Uint64(v[4:])}, nil
}

// Segments returns the registered data segments in ascending id order.
func (r *reader) Segments() ([]SegmentStats, error) {
	var out []SegmentStats
	c := r.meta.Cursor()
	for k, v := c.Seek(segKeyPrefix); bytes.HasPrefix(k, segKeyPrefix); k, v = c.Next() {
		st, ok := decodeSegStats(k, v)
		if !ok {
			return nil, r.corrupt("bad meta entry %q", k)
		}
		out = append(out, st)
	}
	return out, nil
}

// Segment returns the accounting of data segment seg; ok is false when it
// is not registered.
func (r *reader) Segment(seg uint32) (st SegmentStats, ok bool, err error) {
	k := segKey(seg)
	v := r.meta.Get(k)
	if v == nil {
		return SegmentStats{}, false, nil
	}
	st, ok = decodeSegStats(k, v)
	if !ok {
		return SegmentStats{}, false, r.corrupt("bad meta entry %q", k)
	}
	return st, true, nil
}

func decodeSegStats(k, v []byte) (SegmentStats, bool) {
	if len(k) != len(segKeyPrefix)+4 || len(v) != segStatsSize {
		return SegmentStats{}, false
	}
	be := binary.BigEndian
	return SegmentStats{
		Seg:         be.Uint32(k[len(segKeyPrefix):]),
		RecordBytes: int64(be.Uint64(v)),
		DeadBytes:   int64(be.Uint64(v[8:])),
	}, true
}

// Indexes returns every index in ascending name order.
func (r *reader) Indexes() ([]IndexStats, error) {
	var out []IndexStats
	c := r.meta.Cursor()
	for k, v := c.Seek(ixKeyPrefix); bytes.HasPrefix(k, ixKeyPrefix); k, v = c.Next() {
		st, ok := decodeIndexStats(string(k[len(ixKeyPrefix):]), v)
		if !ok {
			return nil, r.corrupt("bad meta entry %q", k)
		}
		out = append(out, st)
	}
	return out, nil
}

// IndexStats returns the counters and state of the index name; ok is
// false when there is no such index.
func (r *reader) IndexStats(name string) (st IndexStats, ok bool, err error) {
	v := r.meta.Get(ixKey(name))
	if v == nil {
		return IndexStats{}, false, nil
	}
	st, ok = decodeIndexStats(name, v)
	if !ok {
		return IndexStats{}, false, r.corrupt("bad meta entry of index %q", name)
	}
	return st, true, nil
}

// KeyCursor returns a cursor over the key table, in id order.
func (r *reader) KeyCursor() *KeyCursor {
	return &KeyCursor{c: r.keys.Cursor(), r: r}
}

// IndexCursor returns a cursor over the entries of the index name, in key
// order: by value, then by document id. It fails with ErrNoIndex when
// there is no such index.
func (r *reader) IndexCursor(name string) (*IndexCursor, error) {
	b := r.tx.Bucket(ixBucket(name))
	if b == nil {
		return nil, fmt.Errorf("%w: %q", ErrNoIndex, name)
	}
	return &IndexCursor{c: b.Cursor(), r: r, name: name}, nil
}

// Tx is a read transaction: a snapshot of the file as of Store.Begin.
// Everything read through it, and through its cursors, is consistent with
// that moment. Byte slices it hands out point into the file and are valid
// only until Close.
//
// Its read methods (Key, Count, Checkpoint, Covered, Segments, Segment,
// Indexes, IndexStats, KeyCursor, IndexCursor) are also available on WTx.
type Tx struct {
	reader
}

// Close ends the transaction. Calling it again is a no-op.
func (t *Tx) Close() error {
	if err := t.tx.Rollback(); err != nil && !errors.Is(err, berrors.ErrTxClosed) {
		return fmt.Errorf("idxstore: %s: closing a read transaction: %w", t.path, err)
	}
	return nil
}

// WTx is the update transaction passed to the function of Store.Update.
// It reads like a Tx, seeing its own changes, and everything it writes
// becomes durable together, or not at all. A cursor made from a WTx must
// be repositioned (First, Last, Seek, SeekBefore) after the transaction
// writes to the bucket it walks.
type WTx struct {
	reader
	indexFill float64
	indexes   map[string]*IndexWriter
}

// SetFillPercent overrides, for this transaction, the fill percent of the
// key table and of every index (0.1 to 1.0; see Options). A high value
// packs pages densely and suits a load whose keys arrive in ascending
// order.
func (u *WTx) SetFillPercent(p float64) error {
	if err := checkFillPercent(p); err != nil {
		return err
	}
	u.keys.FillPercent = p
	u.indexFill = p
	for _, w := range u.indexes {
		if w.b != nil {
			w.b.FillPercent = p
		}
	}
	return nil
}

func checkID(id string) error {
	if len(id) == 0 || len(id) > MaxIDLen {
		return fmt.Errorf("%w: %d bytes, must be 1 to %d", ErrBadID, len(id), MaxIDLen)
	}
	return nil
}

// PutKey sets the key-table entry of the document id and counts the
// document if it is new.
func (u *WTx) PutKey(id string, e Entry) error {
	if err := checkID(id); err != nil {
		return err
	}
	k := []byte(id)
	isNew := u.keys.Get(k) == nil
	if err := u.keys.Put(k, encodeEntry(e)); err != nil {
		return u.writeErr(err)
	}
	if isNew {
		return u.addCount(1)
	}
	return nil
}

// DeleteKey removes the document id from the key table and from the
// count. Deleting an id that is not there does nothing.
func (u *WTx) DeleteKey(id string) error {
	k := []byte(id)
	if u.keys.Get(k) == nil {
		return nil
	}
	if err := u.keys.Delete(k); err != nil {
		return u.writeErr(err)
	}
	return u.addCount(-1)
}

func (u *WTx) addCount(delta int64) error {
	n, err := u.Count()
	if err != nil {
		return err
	}
	if n+delta < 0 {
		return u.corrupt("document count %d below the number of keys", n)
	}
	return u.putMeta(keyCount, binary.BigEndian.AppendUint64(nil, uint64(n+delta)))
}

func (u *WTx) putMeta(key, value []byte) error {
	if err := u.meta.Put(key, value); err != nil {
		return u.writeErr(err)
	}
	return nil
}

func (u *WTx) writeErr(err error) error {
	return fmt.Errorf("idxstore: %s: %w", u.path, err)
}

// SetCheckpoint stores the WAL offset up to which the file reflects this
// collection's changes.
func (u *WTx) SetCheckpoint(cp uint64) error {
	return u.putMeta(keyCheckpoint, binary.BigEndian.AppendUint64(nil, cp))
}

// SetCovered stores the data position the file reflects (see Covered).
func (u *WTx) SetCovered(p Position) error {
	b := binary.BigEndian.AppendUint32(make([]byte, 0, coveredSize), p.Seg)
	return u.putMeta(keyCovered, binary.BigEndian.AppendUint64(b, p.End))
}

// SetSegment registers data segment st.Seg, or replaces its accounting.
func (u *WTx) SetSegment(st SegmentStats) error {
	b := binary.BigEndian.AppendUint64(make([]byte, 0, segStatsSize), uint64(st.RecordBytes))
	return u.putMeta(segKey(st.Seg), binary.BigEndian.AppendUint64(b, uint64(st.DeadBytes)))
}

// AddSegment adds recordBytes and deadBytes (either may be negative or
// zero) to the accounting of data segment seg, registering it first if it
// is not registered.
func (u *WTx) AddSegment(seg uint32, recordBytes, deadBytes int64) error {
	st, _, err := u.Segment(seg)
	if err != nil {
		return err
	}
	return u.SetSegment(SegmentStats{Seg: seg, RecordBytes: st.RecordBytes + recordBytes, DeadBytes: st.DeadBytes + deadBytes})
}

// DeleteSegment removes data segment seg from the registered segments.
// Removing one that is not registered does nothing.
func (u *WTx) DeleteSegment(seg uint32) error {
	if err := u.meta.Delete(segKey(seg)); err != nil {
		return u.writeErr(err)
	}
	return nil
}

// MarkComplete marks a file created with Options.Building as whole, so
// Open accepts it from this transaction's commit on. On a file that is
// already complete it does nothing.
func (u *WTx) MarkComplete() error {
	return u.putMeta(keyComplete, completeValue)
}

// CreateIndex adds an empty index called name, with zero counters and not
// ready (see IndexStats). It fails with ErrIndexExists when there is one.
func (u *WTx) CreateIndex(name string) error {
	if name == "" {
		return errors.New("idxstore: index name is empty")
	}
	if _, err := u.tx.CreateBucket(ixBucket(name)); err != nil {
		if errors.Is(err, berrors.ErrBucketExists) {
			return fmt.Errorf("%w: %q", ErrIndexExists, name)
		}
		return u.writeErr(err)
	}
	return u.putMeta(ixKey(name), encodeIndexStats(IndexStats{}))
}

// DropIndex removes the index name with all its entries and counters. It
// fails with ErrNoIndex when there is none. An IndexWriter of the dropped
// index must not be used again.
func (u *WTx) DropIndex(name string) error {
	if err := u.tx.DeleteBucket(ixBucket(name)); err != nil {
		if errors.Is(err, berrors.ErrBucketNotFound) {
			return fmt.Errorf("%w: %q", ErrNoIndex, name)
		}
		return u.writeErr(err)
	}
	if w := u.indexes[name]; w != nil {
		w.b = nil
		delete(u.indexes, name)
	}
	if err := u.meta.Delete(ixKey(name)); err != nil {
		return u.writeErr(err)
	}
	return nil
}

// Index returns the writer of the index name, the same one for every call
// in this transaction. It fails with ErrNoIndex when there is no such
// index.
func (u *WTx) Index(name string) (*IndexWriter, error) {
	if w := u.indexes[name]; w != nil {
		return w, nil
	}
	b := u.tx.Bucket(ixBucket(name))
	if b == nil {
		return nil, fmt.Errorf("%w: %q", ErrNoIndex, name)
	}
	b.FillPercent = u.indexFill
	w := &IndexWriter{u: u, name: name, b: b, statsKey: ixKey(name)}
	if u.indexes == nil {
		u.indexes = make(map[string]*IndexWriter)
	}
	u.indexes[name] = w
	return w, nil
}

// IndexWriter changes one index inside an update transaction.
type IndexWriter struct {
	u        *WTx
	name     string
	b        *bolt.Bucket // nil once the index is dropped
	statsKey []byte
	key      []byte // scratch for the entry's key; bbolt copies keys
}

// Put adds the entry (enc, id) with flags, or replaces its flags. enc is
// an encoded value (AppendValue, AppendMissing) and id the document id.
// The index's truncated counter follows FlagTruncated.
func (w *IndexWriter) Put(enc []byte, id string, flags Flags) error {
	if flags&^flagsMask != 0 {
		return fmt.Errorf("idxstore: index %q: invalid entry flags %#x", w.name, uint8(flags))
	}
	old, err := w.seek(enc, id)
	if err != nil {
		return err
	}
	if flags&FlagTruncated != 0 && enc[0] != TagString {
		return fmt.Errorf("idxstore: index %q: only a string can be truncated", w.name)
	}
	was := old != nil && Flags(old[0])&FlagTruncated != 0
	if err := w.b.Put(w.key, flagValues[flags]); err != nil {
		return w.u.writeErr(err)
	}
	switch now := flags&FlagTruncated != 0; {
	case now && !was:
		return w.add(0, 1)
	case was && !now:
		return w.add(0, -1)
	}
	return nil
}

// Delete removes the entry (enc, id). Deleting an entry that is not there
// does nothing.
func (w *IndexWriter) Delete(enc []byte, id string) error {
	old, err := w.seek(enc, id)
	if err != nil || old == nil {
		return err
	}
	truncated := Flags(old[0])&FlagTruncated != 0
	if err := w.b.Delete(w.key); err != nil {
		return w.u.writeErr(err)
	}
	if truncated {
		return w.add(0, -1)
	}
	return nil
}

// seek validates (enc, id), leaves the entry's key in w.key, and returns
// the entry's current value, nil if there is no such entry.
func (w *IndexWriter) seek(enc []byte, id string) ([]byte, error) {
	if w.b == nil {
		return nil, fmt.Errorf("%w: %q was dropped in this transaction", ErrNoIndex, w.name)
	}
	if err := checkValue(enc); err != nil {
		return nil, fmt.Errorf("idxstore: index %q: %w", w.name, err)
	}
	if err := checkID(id); err != nil {
		return nil, err
	}
	w.key = AppendIndexKey(w.key[:0], enc, id)
	old := w.b.Get(w.key)
	if old != nil && len(old) != 1 {
		return nil, w.u.corrupt("index %q: entry value is %d bytes", w.name, len(old))
	}
	return old, nil
}

// AddComplex adds delta (which may be negative) to the index's count of
// documents whose value is not indexable and so has no entry.
func (w *IndexWriter) AddComplex(delta int64) error { return w.add(delta, 0) }

// SetReady marks the index as completely built (see IndexStats).
func (w *IndexWriter) SetReady() error {
	st, err := w.stats()
	if err != nil {
		return err
	}
	st.Ready = true
	return w.u.putMeta(w.statsKey, encodeIndexStats(st))
}

func (w *IndexWriter) stats() (IndexStats, error) {
	if w.b == nil {
		return IndexStats{}, fmt.Errorf("%w: %q was dropped in this transaction", ErrNoIndex, w.name)
	}
	st, ok := decodeIndexStats(w.name, w.u.meta.Get(w.statsKey))
	if !ok {
		return IndexStats{}, w.u.corrupt("bad or missing meta entry of index %q", w.name)
	}
	return st, nil
}

func (w *IndexWriter) add(complexDelta, truncatedDelta int64) error {
	if complexDelta == 0 && truncatedDelta == 0 {
		return nil
	}
	st, err := w.stats()
	if err != nil {
		return err
	}
	st.Complex += complexDelta
	st.Truncated += truncatedDelta
	return w.u.putMeta(w.statsKey, encodeIndexStats(st))
}

// seekBefore moves c to the last key that is less than key.
func seekBefore(c *bolt.Cursor, key []byte) (k, v []byte) {
	if k, _ := c.Seek(key); k == nil {
		return c.Last() // every key is less than key
	}
	return c.Prev()
}

// KeyCursor walks the key table in id order. Every positioning method
// returns the entry it lands on; ok is false when there is none (the
// table is empty, or the cursor ran off either end) or when a damaged
// entry stopped the walk, which Err reports. After ok is false,
// reposition with First, Last, Seek, or SeekBefore before moving on. The
// id slices point into the file: copy them to keep them past the
// transaction.
type KeyCursor struct {
	c   *bolt.Cursor
	r   *reader
	err error
}

// First moves to the smallest id.
func (c *KeyCursor) First() (id []byte, e Entry, ok bool) { return c.item(c.c.First()) }

// Last moves to the largest id.
func (c *KeyCursor) Last() (id []byte, e Entry, ok bool) { return c.item(c.c.Last()) }

// Next moves to the next larger id.
func (c *KeyCursor) Next() (id []byte, e Entry, ok bool) { return c.item(c.c.Next()) }

// Prev moves to the next smaller id.
func (c *KeyCursor) Prev() (id []byte, e Entry, ok bool) { return c.item(c.c.Prev()) }

// Seek moves to the smallest id that is greater than or equal to id.
func (c *KeyCursor) Seek(id []byte) (found []byte, e Entry, ok bool) { return c.item(c.c.Seek(id)) }

// SeekBefore moves to the largest id that is less than id.
func (c *KeyCursor) SeekBefore(id []byte) (found []byte, e Entry, ok bool) {
	return c.item(seekBefore(c.c, id))
}

// Err returns the damage that stopped the cursor, if any.
func (c *KeyCursor) Err() error { return c.err }

func (c *KeyCursor) item(k, v []byte) ([]byte, Entry, bool) {
	if k == nil || c.err != nil {
		return nil, Entry{}, false
	}
	e, ok := decodeEntry(v)
	if !ok {
		c.err = c.r.corrupt("key table entry of %q is %d bytes", k, len(v))
		return nil, Entry{}, false
	}
	return k, e, true
}

// IndexEntry is one entry of an index. Its slices point into the file:
// copy them to keep them past the transaction.
type IndexEntry struct {
	Key   []byte // the whole index key: Value, the terminator, ID
	Value []byte // the encoded value (see DecodeValue)
	ID    []byte // the document id
	Flags Flags
}

// IndexCursor walks one index in key order: by value in the engine's sort
// order, then by document id. Seek and SeekBefore take index keys or
// prefixes of them, built with IndexKey, ValuePrefix, PrefixEnd, or an
// encoded value alone:
//
//   - every entry of the value enc: Seek(ValuePrefix(enc)), then Next
//     while Value equals enc;
//   - values greater than enc: Seek(PrefixEnd(ValuePrefix(enc)));
//   - values less than enc, descending: SeekBefore(ValuePrefix(enc)), then
//     Prev;
//   - strings starting with p: Seek(enc of p), then Next while Value has
//     that prefix;
//   - one kind of value only: stop when Value[0] is no longer its tag.
//
// The positioning methods behave as those of KeyCursor.
type IndexCursor struct {
	c    *bolt.Cursor
	r    *reader
	name string
	err  error
}

// First moves to the first entry.
func (c *IndexCursor) First() (IndexEntry, bool) { return c.item(c.c.First()) }

// Last moves to the last entry.
func (c *IndexCursor) Last() (IndexEntry, bool) { return c.item(c.c.Last()) }

// Next moves to the next entry.
func (c *IndexCursor) Next() (IndexEntry, bool) { return c.item(c.c.Next()) }

// Prev moves to the previous entry.
func (c *IndexCursor) Prev() (IndexEntry, bool) { return c.item(c.c.Prev()) }

// Seek moves to the first entry whose key is greater than or equal to
// key.
func (c *IndexCursor) Seek(key []byte) (IndexEntry, bool) { return c.item(c.c.Seek(key)) }

// SeekBefore moves to the last entry whose key is less than key.
func (c *IndexCursor) SeekBefore(key []byte) (IndexEntry, bool) {
	return c.item(seekBefore(c.c, key))
}

// Count returns the number of entries whose key is in [from, to), without
// decoding them. A nil from starts at the first entry and a nil to runs
// to the last. A positive limit stops the count there, which makes Count
// a cheap size estimate. It moves the cursor.
func (c *IndexCursor) Count(from, to []byte, limit int) int {
	n := 0
	for k, _ := c.c.Seek(from); k != nil; k, _ = c.c.Next() {
		if to != nil && bytes.Compare(k, to) >= 0 {
			break
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return n
}

// Err returns the damage that stopped the cursor, if any.
func (c *IndexCursor) Err() error { return c.err }

func (c *IndexCursor) item(k, v []byte) (IndexEntry, bool) {
	if k == nil || c.err != nil {
		return IndexEntry{}, false
	}
	enc, id, err := SplitIndexKey(k)
	if err != nil || len(v) != 1 || Flags(v[0])&^flagsMask != 0 {
		c.err = c.r.corrupt("index %q: bad entry %q", c.name, k)
		return IndexEntry{}, false
	}
	return IndexEntry{Key: k, Value: enc, ID: id, Flags: Flags(v[0])}, true
}
