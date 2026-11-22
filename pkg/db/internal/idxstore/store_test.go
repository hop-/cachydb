package idxstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func mustCreate(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	s, err := Create(dir, opts)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() }) // a second Close of an already closed store reports nothing useful
	return s
}

func mustOpen(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustUpdate(t *testing.T, s *Store, fn func(u *WTx) error) {
	t.Helper()
	if err := s.Update(fn); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func mustView(t *testing.T, s *Store, fn func(tx *Tx) error) {
	t.Helper()
	if err := s.View(fn); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func checkClean(t *testing.T, s *Store) {
	t.Helper()
	problems, err := s.Check(0)
	if err != nil || len(problems) != 0 {
		t.Fatalf("Check = %q, %v; want no problems", problems, err)
	}
}

func enc(t testing.TB, v any) []byte {
	t.Helper()
	return encodeTest(t, v)
}

func TestCreateAndOpen(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrMissing) || !NeedsRebuild(err) {
		t.Fatalf("Open of a missing file = %v, want ErrMissing (needs rebuild)", err)
	}
	s := mustCreate(t, dir, Options{})
	if s.Path() != Path(dir) || !strings.HasSuffix(s.Path(), FileName) {
		t.Errorf("Path = %s, want %s", s.Path(), Path(dir))
	}
	if _, err := Create(dir, Options{}); !errors.Is(err, fs.ErrExist) {
		t.Errorf("second Create = %v, want fs.ErrExist", err)
	}
	if _, err := Open(dir, Options{Timeout: 50 * time.Millisecond}); !errors.Is(err, ErrLocked) || NeedsRebuild(err) {
		t.Errorf("Open while open = %v, want ErrLocked (no rebuild)", err)
	}
	// A new file is empty: no keys, no indexes, nothing covered.
	mustView(t, s, func(tx *Tx) error {
		n, err := tx.Count()
		cp, err2 := tx.Checkpoint()
		mw, err3 := tx.MaxWALOffset()
		cov, err4 := tx.Covered()
		segs, err5 := tx.Segments()
		ixs, err6 := tx.Indexes()
		if err := errors.Join(err, err2, err3, err4, err5, err6); err != nil {
			return err
		}
		if n != 0 || cp != 0 || mw != 0 || cov != (Position{}) || len(segs) != 0 || len(ixs) != 0 {
			t.Errorf("new file: count %d, checkpoint %d, maxwal %d, covered %+v, segments %v, indexes %v", n, cp, mw, cov, segs, ixs)
		}
		return nil
	})
	checkClean(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = mustOpen(t, dir, Options{KeyFillPercent: 0.9, IndexFillPercent: 0.7})
	checkClean(t, s)
}

func TestOptionsFillPercent(t *testing.T) {
	for _, p := range []float64{0.05, 1.5, -1, math.NaN()} {
		if _, err := Create(t.TempDir(), Options{KeyFillPercent: p}); err == nil {
			t.Errorf("Create with key fill percent %v accepted", p)
		}
		dir := t.TempDir()
		s := mustCreate(t, dir, Options{})
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir, Options{IndexFillPercent: p}); err == nil {
			t.Errorf("Open with index fill percent %v accepted", p)
		}
	}
	if fi, err := os.ReadDir(t.TempDir()); err != nil || len(fi) != 0 {
		t.Errorf("unexpected files: %v %v", fi, err)
	}
}

// seedStore fills a store with 100 documents doc000..doc099 in segment 1
// and an index f: odd ids hold "b", even ids "a", doc050 the number 1,
// doc000 also a truncated long string, doc001 also the missing value.
func seedStore(t *testing.T, s *Store) (encA, encB, enc1, encLong []byte) {
	t.Helper()
	encA, encB, enc1 = enc(t, "a"), enc(t, "b"), enc(t, 1.0)
	encLong, truncated, _ := EncodeValue(strings.Repeat("z", 3000))
	if !truncated {
		t.Fatal("long string not truncated")
	}
	mustUpdate(t, s, func(u *WTx) error {
		if err := u.CreateIndex("f"); err != nil {
			return err
		}
		w, err := u.Index("f")
		if err != nil {
			return err
		}
		for i := range 100 {
			id := fmt.Sprintf("doc%03d", i)
			if err := u.PutKey(id, Entry{Seg: 1, Pos: uint64(16 + i*10), Size: 10, WALOffset: uint64(i + 1)}); err != nil {
				return err
			}
			e := encA
			if i%2 == 1 {
				e = encB
			}
			if i == 50 {
				e = enc1
			}
			if err := w.Put(e, id, 0); err != nil {
				return err
			}
		}
		if err := w.Put(encLong, "doc000", FlagTruncated|FlagElement); err != nil {
			return err
		}
		if err := w.Put(encLong, "doc000", FlagTruncated); err != nil { // replaces the flags: counted once
			return err
		}
		if err := w.Put(AppendMissing(nil), "doc001", 0); err != nil {
			return err
		}
		if err := w.AddComplex(3); err != nil {
			return err
		}
		if err := w.AddArrays(2); err != nil {
			return err
		}
		if err := w.SetReady(); err != nil {
			return err
		}
		if err := u.AddSegment(1, 1000, 0); err != nil {
			return err
		}
		if err := u.AddSegment(1, 16, 16); err != nil {
			return err
		}
		if err := u.SetCheckpoint(100); err != nil {
			return err
		}
		if err := u.RaiseMaxWALOffset(120); err != nil {
			return err
		}
		if err := u.RaiseMaxWALOffset(110); err != nil { // lower: ignored
			return err
		}
		if n, err := u.Count(); err != nil || n != 100 {
			return fmt.Errorf("count inside the transaction = %d, %v; want 100", n, err)
		}
		return u.SetCovered(Position{Seg: 1, End: 1032})
	})
	return encA, encB, enc1, encLong
}

// checkSeeded checks the state seedStore leaves.
func checkSeeded(t *testing.T, s *Store) {
	t.Helper()
	mustView(t, s, func(tx *Tx) error {
		if n, _ := tx.Count(); n != 100 {
			t.Errorf("Count = %d, want 100", n)
		}
		if cp, _ := tx.Checkpoint(); cp != 100 {
			t.Errorf("Checkpoint = %d, want 100", cp)
		}
		if mw, _ := tx.MaxWALOffset(); mw != 120 {
			t.Errorf("MaxWALOffset = %d, want 120 (raised, never lowered)", mw)
		}
		if c, _ := tx.Covered(); c != (Position{Seg: 1, End: 1032}) {
			t.Errorf("Covered = %+v", c)
		}
		if segs, _ := tx.Segments(); !slices.Equal(segs, []SegmentStats{{Seg: 1, RecordBytes: 1016, DeadBytes: 16}}) {
			t.Errorf("Segments = %+v", segs)
		}
		if st, ok, _ := tx.Segment(1); !ok || st.RecordBytes != 1016 {
			t.Errorf("Segment(1) = %+v, %v", st, ok)
		}
		if _, ok, _ := tx.Segment(2); ok {
			t.Error("Segment(2) registered")
		}
		want := IndexStats{Name: "f", Complex: 3, Truncated: 1, Arrays: 2, Ready: true}
		if ixs, _ := tx.Indexes(); !slices.Equal(ixs, []IndexStats{want}) {
			t.Errorf("Indexes = %+v, want [%+v]", ixs, want)
		}
		if st, ok, _ := tx.IndexStats("f"); !ok || st != want {
			t.Errorf("IndexStats(f) = %+v, %v", st, ok)
		}
		if _, ok, _ := tx.IndexStats("nope"); ok {
			t.Error("IndexStats of a missing index found")
		}
		if e, ok, _ := tx.Key("doc007"); !ok || e != (Entry{Seg: 1, Pos: 86, Size: 10, WALOffset: 8}) {
			t.Errorf("Key(doc007) = %+v, %v", e, ok)
		}
		for _, id := range []string{"zzz", "", "doc1000"} {
			if _, ok, err := tx.Key(id); ok || err != nil {
				t.Errorf("Key(%q) = %v, %v; want absent", id, ok, err)
			}
		}
		return nil
	})
}

func TestStoreUpdateAndRead(t *testing.T) {
	dir := t.TempDir()
	s := mustCreate(t, dir, Options{})
	seedStore(t, s)
	checkSeeded(t, s)
	checkClean(t, s)
	if e, ok, err := s.Key("doc099"); err != nil || !ok || e.WALOffset != 100 {
		t.Errorf("Store.Key(doc099) = %+v, %v, %v", e, ok, err)
	}
	// Everything is durable: it survives a reopen.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = mustOpen(t, dir, Options{})
	checkSeeded(t, s)
	checkClean(t, s)
}

func TestStoreUpdateRollsBack(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	seedStore(t, s)
	boom := errors.New("boom")
	err := s.Update(func(u *WTx) error {
		for _, step := range []error{
			u.PutKey("zzz", Entry{Seg: 1}),
			u.DeleteKey("doc007"),
			u.SetCheckpoint(999),
			u.RaiseMaxWALOffset(999),
			u.SetCovered(Position{Seg: 9, End: 9}),
			u.AddSegment(2, 5, 0),
			u.DeleteSegment(1),
			u.DropIndex("f"),
			u.CreateIndex("g"),
		} {
			if step != nil {
				return step
			}
		}
		return boom
	})
	if err != boom {
		t.Fatalf("Update = %v, want fn's own error as it is", err)
	}
	checkSeeded(t, s)
	checkClean(t, s)
}

func TestStoreWrites(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	encA, encB, enc1, encLong := seedStore(t, s)
	mustUpdate(t, s, func(u *WTx) error {
		if err := u.CreateIndex("f"); !errors.Is(err, ErrIndexExists) {
			return fmt.Errorf("CreateIndex of an existing index = %v, want ErrIndexExists", err)
		}
		if err := u.CreateIndex(""); err == nil {
			return errors.New("CreateIndex with an empty name accepted")
		}
		if _, err := u.Index("nope"); !errors.Is(err, ErrNoIndex) {
			return fmt.Errorf("Index(nope) = %v, want ErrNoIndex", err)
		}
		w, err := u.Index("f")
		if err != nil {
			return err
		}
		if w2, _ := u.Index("f"); w2 != w {
			return errors.New("Index returned a second writer for the same index")
		}
		// Invalid entries.
		if err := w.Put(enc1, "doc001", FlagTruncated); err == nil {
			return errors.New("a truncated number accepted")
		}
		if err := w.Put(encA, "doc001", 4); err == nil {
			return errors.New("unknown flag bits accepted")
		}
		if err := w.Put([]byte{TagString, 0x00}, "doc001", 0); !errors.Is(err, ErrBadKey) {
			return fmt.Errorf("a malformed value = %v, want ErrBadKey", err)
		}
		if err := w.Put(append(slices.Clone(encA), 0, 0), "doc001", 0); !errors.Is(err, ErrBadKey) {
			return fmt.Errorf("a value with trailing bytes = %v, want ErrBadKey", err)
		}
		if err := w.Put(encA, "", 0); !errors.Is(err, ErrBadID) {
			return fmt.Errorf("an empty id = %v, want ErrBadID", err)
		}
		if err := w.Delete(encA, strings.Repeat("i", MaxIDLen+1)); !errors.Is(err, ErrBadID) {
			return fmt.Errorf("Delete with a long id = %v, want ErrBadID", err)
		}
		if err := u.PutKey(strings.Repeat("i", MaxIDLen+1), Entry{}); !errors.Is(err, ErrBadID) {
			return fmt.Errorf("PutKey with a long id = %v, want ErrBadID", err)
		}
		if err := u.PutKey("", Entry{}); !errors.Is(err, ErrBadID) {
			return fmt.Errorf("PutKey with an empty id = %v, want ErrBadID", err)
		}
		// The longest id fits: as a key and in an index entry with the
		// longest value.
		maxID := strings.Repeat("m", MaxIDLen)
		if err := u.PutKey(maxID, Entry{Seg: 1, Size: 1}); err != nil {
			return fmt.Errorf("PutKey with a %d-byte id: %w", MaxIDLen, err)
		}
		if err := w.Put(encLong, maxID, FlagTruncated); err != nil {
			return fmt.Errorf("index entry with a %d-byte id: %w", MaxIDLen, err)
		}
		if err := w.Delete(encLong, maxID); err != nil {
			return err
		}
		if err := u.DeleteKey(maxID); err != nil {
			return err
		}
		// Counters follow the flags: replacing a truncated entry by an
		// untruncated one and deleting truncated entries.
		if err := w.Put(encLong, "doc002", FlagTruncated); err != nil {
			return err
		}
		if err := w.Put(encLong, "doc002", 0); err == nil {
			// a cut-length value without the flag is allowed: it is a string
			if err := w.Delete(encLong, "doc002"); err != nil {
				return err
			}
		} else {
			return err
		}
		if err := w.Delete(encLong, "doc000"); err != nil {
			return err
		}
		if err := w.Delete(encLong, "doc000"); err != nil { // absent: no-op
			return err
		}
		if err := w.Delete(encB, "doc099"); err != nil {
			return err
		}
		if err := u.DeleteKey("doc099"); err != nil {
			return err
		}
		if err := u.DeleteKey("doc099"); err != nil { // absent: no-op
			return err
		}
		if err := u.PutKey("doc000", Entry{Seg: 1, Pos: 16, Size: 10, WALOffset: 500}); err != nil { // overwrite: count unchanged
			return err
		}
		if err := u.AddSegment(1, 0, 10); err != nil {
			return err
		}
		if err := w.AddComplex(-3); err != nil {
			return err
		}
		if err := w.AddArrays(-2); err != nil {
			return err
		}
		if err := w.AddComplex(0); err != nil { // nothing to do
			return err
		}
		return u.DeleteSegment(7) // not registered: no-op
	})
	mustView(t, s, func(tx *Tx) error {
		if n, _ := tx.Count(); n != 99 {
			t.Errorf("Count = %d, want 99", n)
		}
		st, _, _ := tx.IndexStats("f")
		if st != (IndexStats{Name: "f", Ready: true}) {
			t.Errorf("IndexStats = %+v, want zero counters, ready", st)
		}
		if e, _, _ := tx.Key("doc000"); e.WALOffset != 500 {
			t.Errorf("Key(doc000) = %+v, want the overwrite", e)
		}
		return nil
	})
	checkClean(t, s)

	// DropIndex removes the bucket and the counters; a writer of the
	// dropped index fails.
	mustUpdate(t, s, func(u *WTx) error {
		w, err := u.Index("f")
		if err != nil {
			return err
		}
		if err := u.DropIndex("f"); err != nil {
			return err
		}
		if err := w.Put(encA, "x", 0); !errors.Is(err, ErrNoIndex) {
			return fmt.Errorf("Put after DropIndex = %v, want ErrNoIndex", err)
		}
		if err := w.AddComplex(1); !errors.Is(err, ErrNoIndex) {
			return fmt.Errorf("AddComplex after DropIndex = %v, want ErrNoIndex", err)
		}
		if err := w.SetReady(); !errors.Is(err, ErrNoIndex) {
			return fmt.Errorf("SetReady after DropIndex = %v, want ErrNoIndex", err)
		}
		if err := u.DropIndex("f"); !errors.Is(err, ErrNoIndex) {
			return fmt.Errorf("second DropIndex = %v, want ErrNoIndex", err)
		}
		if _, err := u.IndexCursor("f"); !errors.Is(err, ErrNoIndex) {
			return fmt.Errorf("IndexCursor of a dropped index = %v, want ErrNoIndex", err)
		}
		return u.CreateIndex("g")
	})
	mustView(t, s, func(tx *Tx) error {
		if ixs, _ := tx.Indexes(); !slices.Equal(ixs, []IndexStats{{Name: "g"}}) {
			t.Errorf("Indexes after drop and create = %+v, want [g, not ready]", ixs)
		}
		return nil
	})
	checkClean(t, s)
}

func TestSetFillPercent(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	mustUpdate(t, s, func(u *WTx) error {
		if err := u.CreateIndex("n"); err != nil {
			return err
		}
		w, err := u.Index("n") // writer made before the override: follows it too
		if err != nil {
			return err
		}
		for _, bad := range []float64{0, 0.09, 1.01, math.NaN()} {
			if err := u.SetFillPercent(bad); err == nil {
				return fmt.Errorf("fill percent %v accepted", bad)
			}
		}
		if err := u.SetFillPercent(1); err != nil {
			return err
		}
		for i := range 2000 {
			id := fmt.Sprintf("%06d", i)
			if err := u.PutKey(id, Entry{Seg: 1, Size: 1}); err != nil {
				return err
			}
			if err := w.Put(enc(t, float64(i)), id, 0); err != nil {
				return err
			}
		}
		return u.SetSegment(SegmentStats{Seg: 1, RecordBytes: 2000})
	})
	checkClean(t, s)
}

func TestKeyCursor(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	mustView(t, s, func(tx *Tx) error {
		kc := tx.KeyCursor()
		if _, _, ok := kc.First(); ok {
			t.Error("First on an empty key table found an entry")
		}
		if _, _, ok := kc.Last(); ok {
			t.Error("Last on an empty key table found an entry")
		}
		return nil
	})
	seedStore(t, s)
	mustView(t, s, func(tx *Tx) error {
		kc := tx.KeyCursor()
		var ids []string
		for id, e, ok := kc.First(); ok; id, e, ok = kc.Next() {
			ids = append(ids, string(id))
			if want := fmt.Sprintf("doc%03d", e.WALOffset-1); string(id) != want {
				t.Errorf("entry of %s has WAL offset %d", id, e.WALOffset)
			}
		}
		if len(ids) != 100 || !slices.IsSorted(ids) || kc.Err() != nil {
			t.Errorf("forward walk: %d ids, sorted %v, err %v", len(ids), slices.IsSorted(ids), kc.Err())
		}
		var back []string
		for id, _, ok := kc.Last(); ok; id, _, ok = kc.Prev() {
			back = append(back, string(id))
		}
		slices.Reverse(back)
		if !slices.Equal(back, ids) {
			t.Error("backward walk differs from the forward walk")
		}
		tests := []struct {
			name, seek string
			before     bool
			want       string // "" means none
		}{
			{"seek between", "doc0505", false, "doc051"},
			{"seek exact", "doc050", false, "doc050"},
			{"seek past the end", "zzzz", false, ""},
			{"seek before the start", "a", false, "doc000"},
			{"seek before between", "doc0505", true, "doc050"},
			{"seek before exact", "doc050", true, "doc049"},
			{"seek before past the end", "zzzz", true, "doc099"},
			{"seek before the first", "doc000", true, ""},
		}
		for _, tt := range tests {
			var id []byte
			var ok bool
			if tt.before {
				id, _, ok = kc.SeekBefore([]byte(tt.seek))
			} else {
				id, _, ok = kc.Seek([]byte(tt.seek))
			}
			if got := string(id); ok != (tt.want != "") || got != tt.want {
				t.Errorf("%s: %q, %v; want %q", tt.name, got, ok, tt.want)
			}
		}
		return nil
	})
}

func TestIndexCursor(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	encA, encB, _, encLong := seedStore(t, s)
	mustView(t, s, func(tx *Tx) error {
		if _, err := tx.IndexCursor("nope"); !errors.Is(err, ErrNoIndex) {
			t.Errorf("IndexCursor(nope) = %v, want ErrNoIndex", err)
		}
		ic, err := tx.IndexCursor("f")
		if err != nil {
			return err
		}
		// eq "a": the value's entries in id order.
		var eqA []string
		for e, ok := ic.Seek(ValuePrefix(encA)); ok && bytes.Equal(e.Value, encA); e, ok = ic.Next() {
			eqA = append(eqA, string(e.ID))
		}
		if len(eqA) != 49 || eqA[0] != "doc000" || !slices.IsSorted(eqA) {
			t.Errorf("eq a: %d ids from %v", len(eqA), eqA[:min(3, len(eqA))])
		}
		if c := ic.Count(ValuePrefix(encA), PrefixEnd(ValuePrefix(encA)), 0); c != 49 {
			t.Errorf("Count of a = %d, want 49", c)
		}
		if c := ic.Count(ValuePrefix(encA), PrefixEnd(ValuePrefix(encA)), 10); c != 10 {
			t.Errorf("capped Count = %d, want 10", c)
		}
		if c := ic.Count(nil, nil, 0); c != 102 {
			t.Errorf("Count of everything = %d, want 102", c)
		}
		if c := ic.Count(ValuePrefix(encB), nil, 0); c != 51 {
			t.Errorf("Count from b to the end = %d, want 51 (50 b's and the long string)", c)
		}
		// gt "a": the b's and the long string.
		n := 0
		for e, ok := ic.Seek(PrefixEnd(ValuePrefix(encA))); ok; e, ok = ic.Next() {
			if e.Value[0] != TagString {
				t.Errorf("gt a reached a non-string %x", e.Value)
			}
			n++
		}
		if n != 51 {
			t.Errorf("gt a: %d entries, want 51", n)
		}
		// starts_with "z": the truncated long string only.
		n = 0
		pz := enc(t, "z")
		for e, ok := ic.Seek(pz); ok && bytes.HasPrefix(e.Value, pz); e, ok = ic.Next() {
			n++
		}
		if n != 1 {
			t.Errorf("starts with z: %d entries, want 1", n)
		}
		// First is the missing entry, then the number; Last the long string.
		if e, ok := ic.First(); !ok || e.Value[0] != TagMissing || string(e.ID) != "doc001" || e.Flags != 0 {
			t.Errorf("First = %+v, %v", e, ok)
		}
		if e, ok := ic.Next(); !ok || e.Value[0] != TagNumber || string(e.ID) != "doc050" {
			t.Errorf("second entry = %+v, %v", e, ok)
		}
		if e, ok := ic.Last(); !ok || e.Flags != FlagTruncated || string(e.ID) != "doc000" || !bytes.Equal(e.Value, encLong) ||
			!bytes.Equal(e.Key, IndexKey(encLong, "doc000")) {
			t.Errorf("Last = %+v, %v", e, ok)
		}
		if _, ok := ic.Next(); ok {
			t.Error("Next past the last entry found one")
		}
		// lt "b", descending: the a's (ids descending), the number, missing.
		var desc []string
		for e, ok := ic.SeekBefore(ValuePrefix(encB)); ok; e, ok = ic.Prev() {
			desc = append(desc, string(e.ID))
		}
		if len(desc) != 51 || desc[0] != "doc098" || desc[48] != "doc000" || desc[49] != "doc050" || desc[50] != "doc001" {
			t.Errorf("lt b descending: %d ids %v", len(desc), desc)
		}
		if e, ok := ic.SeekBefore([]byte{0xff}); !ok || string(e.ID) != "doc000" {
			t.Errorf("SeekBefore past every key = %+v, %v; want the last entry", e, ok)
		}
		if _, ok := ic.SeekBefore(nil); ok {
			t.Error("SeekBefore the first key found an entry")
		}
		if ic.Err() != nil {
			t.Errorf("Err = %v", ic.Err())
		}
		return nil
	})
}

// TestReadTransactionIsSnapshot: a read transaction keeps the state of the
// moment it began while an update commits.
func TestReadTransactionIsSnapshot(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	encA, _, _, _ := seedStore(t, s)
	// A commit that has to grow the memory map waits for every open read
	// transaction (see the package comment). Grow the file first, so the
	// small update below commits into free pages.
	mustUpdate(t, s, func(u *WTx) error {
		for i := range 5000 {
			if err := u.PutKey(fmt.Sprintf("grow%05d", i), Entry{Seg: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	mustUpdate(t, s, func(u *WTx) error {
		for i := range 5000 {
			if err := u.DeleteKey(fmt.Sprintf("grow%05d", i)); err != nil {
				return err
			}
		}
		return nil
	})
	tx, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.Update(func(u *WTx) error {
			w, err := u.Index("f")
			if err != nil {
				return err
			}
			if err := w.Delete(encA, "doc000"); err != nil {
				return err
			}
			if err := u.DeleteKey("doc000"); err != nil {
				return err
			}
			return u.SetCheckpoint(200)
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		_ = tx.Close() // let the update finish; the failure is reported below
		t.Fatalf("update blocked for 10s by an open read transaction (result once the reader closed: %v). "+
			"Likely cause: the commit had to remap (grow) the file, which waits for every open read transaction, "+
			"because the free pages made above no longer suffice (a bbolt freelist or mmap growth change?); "+
			"this is not necessarily a snapshot bug", <-done)
	}
	if n, _ := tx.Count(); n != 100 {
		t.Errorf("snapshot Count = %d, want 100", n)
	}
	if cp, _ := tx.Checkpoint(); cp != 100 {
		t.Errorf("snapshot Checkpoint = %d, want 100", cp)
	}
	if _, ok, _ := tx.Key("doc000"); !ok {
		t.Error("snapshot lost doc000")
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	mustView(t, s, func(tx *Tx) error {
		if n, _ := tx.Count(); n != 99 {
			t.Errorf("Count after the update = %d, want 99", n)
		}
		return nil
	})
}

func TestClosedStore(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(); !errors.Is(err, ErrClosed) {
		t.Errorf("Begin = %v, want ErrClosed", err)
	}
	if err := s.View(func(*Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("View = %v, want ErrClosed", err)
	}
	if _, _, err := s.Key("a"); !errors.Is(err, ErrClosed) {
		t.Errorf("Key = %v, want ErrClosed", err)
	}
	if err := s.Update(func(*WTx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("Update = %v, want ErrClosed", err)
	}
}

func TestRemove(t *testing.T) {
	dir := t.TempDir()
	if err := Remove(dir); err != nil {
		t.Errorf("Remove without a file = %v", err)
	}
	s := mustCreate(t, dir, Options{})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrMissing) {
		t.Errorf("Open after Remove = %v, want ErrMissing", err)
	}
	mustCreate(t, dir, Options{}) // the name is free again
}

func TestBuildingFile(t *testing.T) {
	dir := t.TempDir()
	s := mustCreate(t, dir, Options{Building: true})
	mustUpdate(t, s, func(u *WTx) error { return u.PutKey("a", Entry{Seg: 1, Size: 3}) })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrIncomplete) || !NeedsRebuild(err) {
		t.Fatalf("Open of an incomplete file = %v, want ErrIncomplete (needs rebuild)", err)
	}
	if err := Remove(dir); err != nil {
		t.Fatal(err)
	}
	s = mustCreate(t, dir, Options{Building: true})
	mustUpdate(t, s, func(u *WTx) error {
		if err := u.PutKey("a", Entry{Seg: 1, Size: 3}); err != nil {
			return err
		}
		if err := u.SetSegment(SegmentStats{Seg: 1, RecordBytes: 3}); err != nil {
			return err
		}
		return u.MarkComplete()
	})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = mustOpen(t, dir, Options{Building: true}) // Open ignores Building
	checkClean(t, s)
	mustUpdate(t, s, func(u *WTx) error { return u.MarkComplete() }) // already complete: no-op
}

// TestCheckFindsDrift: Check reports every broken invariant and stops at
// its limit.
func TestCheckFindsDrift(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	encA, _, _, _ := seedStore(t, s)
	mustUpdate(t, s, func(u *WTx) error {
		w, err := u.Index("f")
		if err != nil {
			return err
		}
		return errors.Join(
			w.Put(encA, "ghost", 0),         // entry of a document not in the key table
			u.AddSegment(1, 5, 0),           // record bytes no key references
			u.PutKey("lost", Entry{Seg: 9}), // key in an unregistered segment
			w.AddComplex(-10),               // negative complex counter
			w.AddArrays(500),                // arrays counter above the document count
			u.SetSegment(SegmentStats{Seg: 3, RecordBytes: 1, DeadBytes: 2}), // more dead than record bytes
		)
	})
	problems, err := s.Check(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ghost", "segment 1:", "lost", "complex counter", "arrays counter", "segment 3:"} {
		if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, want) }) {
			t.Errorf("Check problems %q do not mention %q", problems, want)
		}
	}
	if len(problems) != 6 {
		t.Errorf("Check found %d problems, want 6: %q", len(problems), problems)
	}
	if p, _ := s.Check(2); len(p) != 2 {
		t.Errorf("Check(2) returned %d problems, want 2", len(p))
	}

	// A truncated counter that disagrees with the entries, and a count that
	// disagrees with the key table, written behind the store's back.
	s2 := mustCreate(t, t.TempDir(), Options{})
	seedStore(t, s2)
	if err := s2.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		if err := meta.Put(keyCount, binary.BigEndian.AppendUint64(nil, 7)); err != nil {
			return err
		}
		return meta.Put(ixKey("f"), encodeIndexStats(IndexStats{Truncated: 5, Ready: true}))
	}); err != nil {
		t.Fatal(err)
	}
	problems, err = s2.Check(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"count is 7", "truncated counter is 5"} {
		if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, want) }) {
			t.Errorf("Check problems %q do not mention %q", problems, want)
		}
	}
}

// TestDamagedEntries: a malformed key-table entry or index entry is
// reported as ErrCorrupt by the readers and stops a cursor.
func TestDamagedEntries(t *testing.T) {
	s := mustCreate(t, t.TempDir(), Options{})
	seedStore(t, s)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(keysBucket).Put([]byte("doc0005"), []byte{1, 2, 3}); err != nil {
			return err
		}
		return tx.Bucket(ixBucket("f")).Put([]byte{0x99, 0, 0, 'x'}, []byte{0}) // sorts after every entry
	}); err != nil {
		t.Fatal(err)
	}
	mustView(t, s, func(tx *Tx) error {
		if _, _, err := tx.Key("doc0005"); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Key of a malformed entry = %v, want ErrCorrupt", err)
		}
		kc := tx.KeyCursor()
		n := 0
		for _, _, ok := kc.First(); ok; _, _, ok = kc.Next() {
			n++
		}
		if !errors.Is(kc.Err(), ErrCorrupt) || n != 1 { // doc000 < doc0005 < doc001
			t.Errorf("key walk stopped after %d entries with %v, want 1 and ErrCorrupt", n, kc.Err())
		}
		if _, _, ok := kc.First(); ok {
			t.Error("a cursor that hit damage moves again")
		}
		ic, err := tx.IndexCursor("f")
		if err != nil {
			return err
		}
		if _, ok := ic.Last(); ok || !errors.Is(ic.Err(), ErrCorrupt) {
			t.Errorf("index cursor on a malformed entry: ok %v, err %v; want ErrCorrupt", ok, ic.Err())
		}
		return nil
	})
	problems, err := s.Check(0)
	if err != nil || len(problems) < 2 {
		t.Errorf("Check = %q, %v; want the malformed key and index entries", problems, err)
	}
}

// TestOpenDamagedFile: Open rejects every kind of damaged file with
// ErrCorrupt (so the engine rebuilds it) and never faults.
func TestOpenDamagedFile(t *testing.T) {
	src := t.TempDir()
	s := mustCreate(t, src, Options{})
	seedStore(t, s)
	// A second transaction, so both meta pages hold a valid state.
	mustUpdate(t, s, func(u *WTx) error { return u.SetCheckpoint(101) })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(Path(src))
	if err != nil {
		t.Fatal(err)
	}
	page := os.Getpagesize()
	var need int64 // the file size the newest transaction needs
	bdb, err := bolt.Open(Path(src), 0o644, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := bdb.View(func(tx *bolt.Tx) error { need = tx.Size(); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Close(); err != nil {
		t.Fatal(err)
	}
	if need <= int64(4*page) {
		t.Fatalf("the newest transaction needs only %d bytes; the cut below would not damage it", need)
	}
	if len(good) < 4*page {
		t.Fatalf("index file is %d bytes, expected at least 4 pages", len(good))
	}

	writeDir := func(t *testing.T, data []byte) string {
		dir := t.TempDir()
		if err := os.WriteFile(Path(dir), data, 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	overwrite := func(off, n int) []byte {
		b := slices.Clone(good)
		copy(b[off:off+n], bytes.Repeat([]byte("garbage!"), n/8+1))
		return b
	}
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"one byte", good[:1]},
		{"cut inside the meta pages", good[:page+100]},
		{"cut below four pages", good[:4*page-1]},
		{"cut in half", good[:len(good)/2]},
		{"cut below what the newest transaction needs", good[:need-1]},
		{"garbage", bytes.Repeat([]byte("garbage!"), len(good)/8)},
		{"both meta pages overwritten", overwrite(0, 2*page)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Open(writeDir(t, tt.data), Options{})
			if !errors.Is(err, ErrCorrupt) || !NeedsRebuild(err) {
				t.Errorf("Open = %v, want ErrCorrupt", err)
			}
		})
	}
	t.Run("a directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(Path(dir), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Open = %v, want ErrCorrupt", err)
		}
	})
	t.Run("one meta page overwritten", func(t *testing.T) {
		// bbolt falls back to the other meta page, which holds a committed
		// transaction, so Open accepts the file (this is not "damaged" for
		// the open check). Check, the full verification, finds the damage.
		s := mustOpen(t, writeDir(t, overwrite(0, page)), Options{})
		mustView(t, s, func(tx *Tx) error {
			cp, err := tx.Checkpoint()
			if err != nil || (cp != 100 && cp != 101) {
				t.Errorf("Checkpoint = %d, %v; want one of the two committed states", cp, err)
			}
			return nil
		})
		if problems, err := s.Check(0); err != nil || len(problems) == 0 {
			t.Errorf("Check = %q, %v; want the overwritten meta page reported", problems, err)
		}
	})
}

// TestOpenBadLayout: a well-formed bbolt file without the expected layout
// is ErrCorrupt.
func TestOpenBadLayout(t *testing.T) {
	tests := []struct {
		name   string
		damage func(tx *bolt.Tx) error
	}{
		{"no meta bucket", func(tx *bolt.Tx) error { return tx.DeleteBucket(metaBucket) }},
		{"no key table", func(tx *bolt.Tx) error { return tx.DeleteBucket(keysBucket) }},
		{"old layout version", func(tx *bolt.Tx) error {
			return tx.Bucket(metaBucket).Put(keyFormat, binary.BigEndian.AppendUint32(nil, 1))
		}},
		{"short checkpoint", func(tx *bolt.Tx) error { return tx.Bucket(metaBucket).Put(keyCheckpoint, []byte{1}) }},
		{"short count", func(tx *bolt.Tx) error { return tx.Bucket(metaBucket).Put(keyCount, []byte{1}) }},
		{"short maxwal", func(tx *bolt.Tx) error { return tx.Bucket(metaBucket).Put(keyMaxWAL, []byte{1}) }},
		{"short covered", func(tx *bolt.Tx) error { return tx.Bucket(metaBucket).Put(keyCovered, []byte{1}) }},
		{"bad segment entry", func(tx *bolt.Tx) error { return tx.Bucket(metaBucket).Put(segKey(4), []byte{1}) }},
		{"bad index entry", func(tx *bolt.Tx) error { return tx.Bucket(metaBucket).Put(ixKey("f"), []byte{1}) }},
		{"index bucket without meta entry", func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket(ixBucket("orphan"))
			return err
		}},
		{"index meta entry without bucket", func(tx *bolt.Tx) error { return tx.DeleteBucket(ixBucket("f")) }},
		{"meta entry of a later index without bucket", func(tx *bolt.Tx) error {
			return tx.Bucket(metaBucket).Put(ixKey("zz"), encodeIndexStats(IndexStats{}))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Create(dir, Options{})
			if err != nil {
				t.Fatal(err)
			}
			mustUpdate(t, s, func(u *WTx) error { return u.CreateIndex("f") })
			if err := s.db.Update(tt.damage); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) || !NeedsRebuild(err) {
				t.Errorf("Open = %v, want ErrCorrupt", err)
			}
		})
	}
}
