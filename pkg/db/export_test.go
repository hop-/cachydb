package db

import (
	"fmt"
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
