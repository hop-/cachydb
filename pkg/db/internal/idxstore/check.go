package idxstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// DefaultCheckLimit is the number of problems Check reports when its
// limit is not positive.
const DefaultCheckLimit = 100

// checkLayout verifies what Open promises about an opened file (see
// Open): the layout version, the complete mark, and that every meta value
// decodes and matches the buckets. Its cost depends on the number of
// segments and indexes, not on the number of documents.
func (s *Store) checkLayout() error {
	btx, err := s.db.Begin(false)
	if err != nil {
		return fmt.Errorf("idxstore: %s: starting a read transaction: %w", s.path, err)
	}
	defer func() { _ = btx.Rollback() }() // a read transaction has nothing to lose
	r, err := newReader(btx, s.path)
	if err != nil {
		return err
	}
	return r.checkLayout()
}

func (r *reader) checkLayout() error {
	if v := r.meta.Get(keyFormat); len(v) != 4 || binary.BigEndian.Uint32(v) != formatVersion {
		return r.corrupt("meta %q is %x, want version %d", keyFormat, v, formatVersion)
	}
	if r.meta.Get(keyComplete) == nil {
		return fmt.Errorf("%w: %s", ErrIncomplete, r.path)
	}
	if _, err := r.Checkpoint(); err != nil {
		return err
	}
	if _, err := r.MaxWALOffset(); err != nil {
		return err
	}
	if _, err := r.Count(); err != nil {
		return err
	}
	if _, err := r.Covered(); err != nil {
		return err
	}
	if _, err := r.Segments(); err != nil {
		return err
	}
	indexes, err := r.Indexes()
	if err != nil {
		return err
	}
	// Index buckets and index meta entries must match one to one. Both
	// are sorted by name, so walk them together.
	i := 0
	c := r.tx.Cursor()
	for k, _ := c.Seek(ixBucketPrefix); bytes.HasPrefix(k, ixBucketPrefix); k, _ = c.Next() {
		name := string(k[len(ixBucketPrefix):])
		if i == len(indexes) || indexes[i].Name > name {
			return r.corrupt("index %q has a bucket but no meta entry", name)
		}
		if indexes[i].Name < name {
			break
		}
		i++
	}
	if i < len(indexes) {
		return r.corrupt("index %q has a meta entry but no bucket", indexes[i].Name)
	}
	return nil
}

// Check verifies the whole file and returns a description of every
// problem it finds, up to limit of them (DefaultCheckLimit when limit is
// not positive). No problems means the file is sound; err reports a
// failure to run the check, not a finding.
//
// It runs bbolt's page-level consistency check, then verifies the layout
// and the invariants this package maintains: every key-table entry is
// well formed and points at a registered segment; count equals the number
// of entries; for every segment, record bytes less dead bytes equals the
// bytes the key table references; every index entry is well formed and
// belongs to a document in the key table; every truncated counter equals
// the number of truncated entries; every complex and arrays counter is
// between 0 and count.
//
// Check reads the whole file and holds the update lock while it does, so
// its cost grows with the file and updates wait for it. It is meant for
// verification tools and tests, not for every open.
func (s *Store) Check(limit int) (problems []string, err error) {
	if limit <= 0 {
		limit = DefaultCheckLimit
	}
	// bbolt's check is only safe next to other writers when it runs in
	// the update transaction, so take that and roll it back.
	btx, err := s.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("idxstore: %s: starting the check: %w", s.path, err)
	}
	defer func() { _ = btx.Rollback() }() // nothing was written

	p := &problemList{limit: limit}
	for cerr := range btx.Check() { // drain it: the checker blocks on every send
		p.add("bbolt: %v", cerr)
	}
	if len(p.list) > 0 {
		return p.list, nil // the tree is damaged; walking it could fault
	}
	r, err := newReader(btx, s.path)
	if err == nil {
		err = r.checkLayout()
	}
	if err != nil {
		p.add("%v", err)
		return p.list, nil
	}
	if err := r.verify(p); err != nil {
		return nil, err
	}
	return p.list, nil
}

// problemList collects up to limit problem descriptions.
type problemList struct {
	list  []string
	limit int
}

func (p *problemList) add(format string, args ...any) {
	if !p.full() {
		p.list = append(p.list, fmt.Sprintf(format, args...))
	}
}

func (p *problemList) full() bool { return len(p.list) >= p.limit }

// short returns b for printing, cut to a readable length.
func short(b []byte) []byte {
	const maxPrinted = 64
	if len(b) > maxPrinted {
		return b[:maxPrinted]
	}
	return b
}

// verify checks the invariants between the key table, the indexes, and
// the meta values of a file whose layout checkLayout accepted.
func (r *reader) verify(p *problemList) error {
	segs, err := r.Segments()
	if err != nil {
		return err
	}
	live := make(map[uint32]int64, len(segs)) // segment -> bytes the key table references
	for _, st := range segs {
		live[st.Seg] = 0
	}
	var keys int64
	c := r.keys.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		keys++
		e, ok := decodeEntry(v)
		if _, registered := live[e.Seg]; !ok {
			p.add("key table: entry of %q is %d bytes", short(k), len(v))
		} else if len(k) > MaxIDLen {
			p.add("key table: id %q... is %d bytes, max %d", short(k), len(k), MaxIDLen)
		} else if !registered {
			p.add("key table: %q is in segment %d, which is not registered", short(k), e.Seg)
		} else {
			live[e.Seg] += int64(e.Size)
		}
		if p.full() {
			return nil
		}
	}
	count, err := r.Count()
	if err != nil {
		return err
	}
	if count != keys {
		p.add("count is %d, the key table has %d entries", count, keys)
	}
	for _, st := range segs {
		switch {
		case st.RecordBytes < 0 || st.DeadBytes < 0 || st.DeadBytes > st.RecordBytes:
			p.add("segment %d: %d record bytes, %d dead bytes", st.Seg, st.RecordBytes, st.DeadBytes)
		case st.RecordBytes-st.DeadBytes != live[st.Seg]:
			p.add("segment %d: %d record bytes less %d dead bytes, but the key table references %d bytes",
				st.Seg, st.RecordBytes, st.DeadBytes, live[st.Seg])
		}
	}

	indexes, err := r.Indexes()
	if err != nil {
		return err
	}
	for _, ix := range indexes {
		if p.full() {
			return nil
		}
		b := r.tx.Bucket(ixBucket(ix.Name))
		if b == nil {
			return r.corrupt("index %q has a meta entry but no bucket", ix.Name)
		}
		r.verifyIndex(p, ix, b, keys)
	}
	return nil
}

func (r *reader) verifyIndex(p *problemList, ix IndexStats, b *bolt.Bucket, keys int64) {
	var truncated int64
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		_, id, err := SplitIndexKey(k)
		switch {
		case err != nil || len(v) != 1 || Flags(v[0])&^flagsMask != 0:
			p.add("index %q: bad entry %q", ix.Name, short(k))
		case r.keys.Get(id) == nil:
			p.add("index %q: entry %q is for a document that is not in the key table", ix.Name, short(k))
		case Flags(v[0])&FlagTruncated != 0:
			truncated++
		}
		if p.full() {
			return
		}
	}
	if ix.Truncated != truncated {
		p.add("index %q: truncated counter is %d, the index has %d truncated entries", ix.Name, ix.Truncated, truncated)
	}
	if ix.Complex < 0 || ix.Complex > keys {
		p.add("index %q: complex counter is %d with %d documents", ix.Name, ix.Complex, keys)
	}
	if ix.Arrays < 0 || ix.Arrays > keys {
		p.add("index %q: arrays counter is %d with %d documents", ix.Name, ix.Arrays, keys)
	}
}
