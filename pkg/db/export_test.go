package db

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// Key-table accessors: the key table moved from the Collection.keydir map
// into the index file (index.db); tests read it through these.

// keyEntryOf returns the key-table entry of id, and whether there is one.
func keyEntryOf(t testing.TB, c *Collection, id string) (idxstore.Entry, bool) {
	t.Helper()
	ke, ok, err := c.storedEntry(id)
	if err != nil {
		t.Fatalf("key-table lookup of %q: %v", id, err)
	}
	return ke, ok
}

// keyTable returns the whole key table: id -> entry.
func keyTable(t testing.TB, c *Collection) map[string]idxstore.Entry {
	t.Helper()
	st, err := c.indexStore()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]idxstore.Entry)
	err = st.View(func(tx *idxstore.Tx) error {
		cur := tx.KeyCursor()
		for k, ke, ok := cur.First(); ok; k, ke, ok = cur.Next() {
			out[string(k)] = ke
		}
		return cur.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// putKeyEntries writes entries into the key table in one index
// transaction, as a flush would, and adds addBytes record bytes to segment
// seg's accounting (registering it).
func putKeyEntries(t testing.TB, c *Collection, seg uint32, addBytes int64, entries map[string]idxstore.Entry) {
	t.Helper()
	st, err := c.indexStore()
	if err != nil {
		t.Fatal(err)
	}
	err = st.Update(func(u *idxstore.WTx) error {
		for id, ke := range entries {
			if err := u.PutKey(id, ke); err != nil {
				return err
			}
		}
		return u.AddSegment(seg, addBytes, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// segAccounting returns the registered data segments' record bytes and
// dead bytes as compaction planning sees them (compactionAccounting).
func segAccounting(t testing.TB, c *Collection) (total, dead map[uint32]int64) {
	t.Helper()
	_, total, dead, err := c.compactionAccounting()
	if err != nil {
		t.Fatal(err)
	}
	return total, dead
}

// Index accessors: the secondary indexes moved from the in-memory index
// type into the index file; tests read them through these.

// entryString renders one index entry for comparisons.
func entryString(enc []byte, id string, flags idxstore.Flags) string {
	return fmt.Sprintf("%x/%s/%d", enc, id, flags)
}

// docEntryStrings renders the entries doc contributes to an index on field
// and reports whether it is complex.
func docEntryStrings(doc *Document, field string) ([]string, bool) {
	keys := indexKeysOf(doc, field)
	out := make([]string, len(keys.entries))
	for i, en := range keys.entries {
		out[i] = entryString(en.enc, doc.ID, en.flags)
	}
	return out, keys.complex
}

// checkIndexEntries checks the secondary indexes of c against docs, its
// live documents as Find returns them (same or stronger than comparing an
// in-memory index with one rebuilt from the documents):
//
//   - the index file holds exactly one ready bucket per index definition;
//   - each bucket holds exactly the entries of the versions the key table
//     points at (none missing, none extra, same flags), and its complex,
//     truncated, and arrays counters equal the number of complex
//     documents, truncated entries, and array values among them;
//   - the entries of the ids outside the overlay plus the entries of the
//     overlay's documents equal the entries of docs: what a reader sees;
//   - idxstore's own consistency check finds nothing.
//
// It returns the index definitions (name -> field). Callers hold
// Database.mu shared and no collection lock; flushes and compactions of c
// wait while it runs.
func checkIndexEntries(t testing.TB, c *Collection, docs []*Document) map[string]string {
	t.Helper()
	c.compactMu.Lock()
	defer c.compactMu.Unlock()
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.RLock()
	defs := maps.Clone(c.indexes)
	overlay := make(map[string]*Document, len(c.memtable)+len(c.flushing))
	for id, me := range c.flushing {
		overlay[id] = me.doc
	}
	for id, me := range c.memtable {
		overlay[id] = me.doc
	}
	c.mu.RUnlock()
	fields := make(map[string]string, len(defs))
	for name, def := range defs {
		fields[name] = def.Field
	}

	st, err := c.indexStore()
	if err != nil {
		t.Fatalf("%s: index file: %v", c.name, err)
	}
	type bucket struct {
		stats   idxstore.IndexStats
		entries []string
	}
	got := map[string]*bucket{}
	stored := map[string]*Document{} // the versions the key table points at
	err = st.View(func(tx *idxstore.Tx) error {
		stats, err := tx.Indexes()
		if err != nil {
			return err
		}
		for _, s := range stats {
			b := &bucket{stats: s}
			cur, err := tx.IndexCursor(s.Name)
			if err != nil {
				return err
			}
			for e, ok := cur.First(); ok; e, ok = cur.Next() {
				b.entries = append(b.entries, entryString(e.Value, string(e.ID), e.Flags))
			}
			if err := cur.Err(); err != nil {
				return err
			}
			got[s.Name] = b
		}
		kc := tx.KeyCursor()
		for k, ke, ok := kc.First(); ok; k, ke, ok = kc.Next() {
			doc, err := c.readStored(string(k), ke)
			if err != nil {
				return err
			}
			stored[string(k)] = doc
		}
		return kc.Err()
	})
	if err != nil {
		t.Fatalf("%s: reading the index file: %v", c.name, err)
	}

	if names, want := slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(defs)); !slices.Equal(names, want) {
		t.Errorf("%s: index buckets %v, index definitions %v", c.name, names, want)
	}
	for name, field := range fields {
		b := got[name]
		if b == nil {
			continue
		}
		if !b.stats.Ready {
			t.Errorf("%s: index %s is not ready", c.name, name)
		}
		var want []string
		var complexN, truncatedN, arraysN int64
		for _, doc := range stored {
			es, cx := docEntryStrings(doc, field)
			want = append(want, es...)
			if cx {
				complexN++
			}
			if indexKeysOf(doc, field).array {
				arraysN++
			}
			for _, en := range indexKeysOf(doc, field).entries {
				if en.flags&idxstore.FlagTruncated != 0 {
					truncatedN++
				}
			}
		}
		slices.Sort(want)
		gotEntries := slices.Sorted(slices.Values(b.entries))
		if !slices.Equal(gotEntries, want) {
			t.Errorf("%s: index %s holds\n  %v\nthe key table's documents give\n  %v", c.name, name, gotEntries, want)
		}
		if b.stats.Complex != complexN || b.stats.Truncated != truncatedN || b.stats.Arrays != arraysN {
			t.Errorf("%s: index %s counters complex=%d truncated=%d arrays=%d, want %d, %d and %d",
				c.name, name, b.stats.Complex, b.stats.Truncated, b.stats.Arrays, complexN, truncatedN, arraysN)
		}

		// What a reader sees: the entries outside the overlay, plus the
		// overlay's documents, against the live documents.
		var seen, wantSeen []string
		for _, e := range b.entries {
			id := e[strings.Index(e, "/")+1 : strings.LastIndex(e, "/")]
			if _, in := overlay[id]; !in {
				seen = append(seen, e)
			}
		}
		for _, doc := range overlay {
			if doc != nil {
				es, _ := docEntryStrings(doc, field)
				seen = append(seen, es...)
			}
		}
		for _, doc := range docs {
			es, _ := docEntryStrings(doc, field)
			wantSeen = append(wantSeen, es...)
		}
		slices.Sort(seen)
		slices.Sort(wantSeen)
		if !slices.Equal(seen, wantSeen) {
			t.Errorf("%s: index %s with the overlay gives\n  %v\nthe live documents give\n  %v", c.name, name, seen, wantSeen)
		}
	}
	problems, err := st.Check(10)
	if err != nil || len(problems) > 0 {
		t.Errorf("%s: index file check: %v, %v", c.name, problems, err)
	}
	return fields
}

// indexLookup returns, sorted, the ids of the documents whose entries in
// index name hold the value v (as the whole value or an element), as a
// reader sees them: the index file's entries outside the overlay, and the
// overlay's documents.
func indexLookup(t testing.TB, c *Collection, name string, v any) []string {
	t.Helper()
	c.mu.RLock()
	def, ok := c.indexes[name]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("no index %s", name)
	}
	enc, _, ok := idxstore.AppendValue(nil, v)
	if !ok {
		return nil
	}
	overlay, st, err := c.captureOverlay()
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	err = st.View(func(tx *idxstore.Tx) error {
		cur, err := tx.IndexCursor(name)
		if err != nil {
			return err
		}
		ids = valueIDs(cur, enc, false, overlay, ids)
		ids = valueIDs(cur, enc, true, overlay, ids)
		return cur.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	for id, doc := range overlay {
		if doc == nil {
			continue
		}
		for _, en := range indexKeysOf(doc, def.Field).entries {
			if string(en.enc) == string(enc) {
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// Crash-test helpers (interfaces.md #crash-hooks).

// crashAt arms opts so that an engine opened with them simulates a crash
// the first time it reaches hook point p: that hook call returns
// errSimulatedCrash, and so does every later hook call at any point, as a
// dead process does nothing more. The operation (or flush) that hit the
// hook returns at once without cleanup. Tests then call abandon and
// reopen the directory with opts.testHook cleared.
//
// If t fails, the hook calls the engine made (point, time since arming, and
// whether the call crashed) are logged, so an intermittent failure shows
// where the simulated crash really happened.
func crashAt(t *testing.T, opts *Options, p hookPoint) {
	t.Helper()
	var (
		dead  atomic.Bool
		mu    sync.Mutex
		calls []string
	)
	start := time.Now()
	opts.testHook = func(at hookPoint) error {
		if at == p {
			dead.Store(true)
		}
		crashed := dead.Load()
		mu.Lock()
		calls = append(calls, fmt.Sprintf("%s +%v crashed=%v", at, time.Since(start).Round(time.Microsecond), crashed))
		mu.Unlock()
		if crashed {
			return errSimulatedCrash
		}
		return nil
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		t.Logf("crash hook armed at %s; %d hook calls:\n  %s", p, len(calls), strings.Join(calls, "\n  "))
	})
}

// abandon stops e the way a dead process would leave it: no final flush
// and no index snapshots. It waits for in-flight operations, stops the
// background goroutines, closes the segment files and the WAL (whose
// records are already durable), and releases the directory lock so the
// same directory can be reopened. A later Close is a no-op.
func (e *Engine) abandon() {
	e.closeOnce.Do(func() {
		e.gateMu.Lock()
		e.closed = true
		e.gateMu.Unlock()
		e.pending.broadcast()
		e.ops.Wait()
		e.bgCancel()
		e.bg.Wait()

		e.mu.RLock()
		for _, d := range e.databases {
			d.mu.RLock()
			for _, c := range d.collections {
				_ = c.closeFiles() // read-only or already durable: nothing to lose
			}
			d.mu.RUnlock()
		}
		e.mu.RUnlock()
		_ = e.wal.Close() // a failed WAL (tests) has nothing left to write
		_ = e.lock.Release()
	})
}
