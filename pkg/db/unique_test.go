package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Unique indexes (AC-8, approach-index-and-query §3 "Unique indexes").

// checkIndexes runs checkIndexEntries against c's live documents.
func checkIndexes(t *testing.T, c *Collection) {
	t.Helper()
	docs, err := c.Find(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	checkIndexEntries(t, c, docs)
}

// rejectsUnique checks that err is a unique violation and that the failed
// write left nothing behind: no WAL record, no change to Count or to the
// document with id.
func rejectsUnique(t *testing.T, e *Engine, c *Collection, what string, write func() error, id string, before *Document) {
	t.Helper()
	off, count := e.wal.NextOffset(), c.Count()
	err := write()
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("%s = %v, want ErrUniqueViolation", what, err)
	}
	if got := e.wal.NextOffset(); got != off {
		t.Errorf("%s: the rejected write appended to the WAL (%d -> %d)", what, off, got)
	}
	if got := c.Count(); got != count {
		t.Errorf("%s: Count %d -> %d", what, count, got)
	}
	got, gerr := c.Get(context.Background(), id)
	switch {
	case before == nil && !errors.Is(gerr, ErrNotFound):
		t.Errorf("%s: document %q exists after a rejected write: %+v, %v", what, id, got, gerr)
	case before != nil && (gerr != nil || fmt.Sprint(got.Data) != fmt.Sprint(before.Data)):
		t.Errorf("%s: document %q = %+v, %v; want unchanged %+v", what, id, got, gerr, before.Data)
	}
}

func TestUniqueIndexRejectsDuplicates(t *testing.T) {
	ctx := context.Background()
	for _, flushed := range []bool{false, true} {
		t.Run(fmt.Sprintf("flushed=%v", flushed), func(t *testing.T) {
			_, e, c := newCrashColl(t, nil, nil)
			if err := c.CreateIndex(ctx, "by_email", "email", Unique(true)); err != nil {
				t.Fatal(err)
			}
			if ix := c.Indexes(); len(ix) != 1 || !ix[0].Unique || ix[0].Field != "email" {
				t.Fatalf("Indexes = %+v, want one unique index on email", ix)
			}
			long := strings.Repeat("q", 2000) // past the 1,024-byte truncation of index values
			for id, data := range map[string]map[string]any{
				"a":     {"email": "x@y", "n": 1.0},
				"num":   {"email": 1.0},
				"null":  {"email": nil},
				"long1": {"email": long + "1"},
				// Not constrained: missing, objects, arrays, and their elements.
				"m1": {"n": 1.0}, "m2": {"n": 1.0},
				"o1": {"email": map[string]any{"k": 1.0}}, "o2": {"email": map[string]any{"k": 1.0}},
				"r1": {"email": []any{"x@y"}}, "r2": {"email": []any{"x@y"}},
			} {
				mustInsert(t, c, id, data)
			}
			// A long string that differs only after the truncation point is a
			// different value.
			mustInsert(t, c, "long2", map[string]any{"email": long + "2"})
			if flushed {
				mustFlush(t, c)
			}
			m1 := mustGet(t, c, "m1")

			rejectsUnique(t, e, c, "Insert of a duplicate", func() error {
				_, err := c.Insert(ctx, &Document{ID: "b", Data: map[string]any{"email": "x@y"}})
				return err
			}, "b", nil)
			rejectsUnique(t, e, c, "Insert of an equal number (1 and 1.0)", func() error {
				_, err := c.Insert(ctx, &Document{ID: "b", Data: map[string]any{"email": 1}})
				return err
			}, "b", nil)
			rejectsUnique(t, e, c, "Insert of a second null", func() error {
				_, err := c.Insert(ctx, &Document{ID: "b", Data: map[string]any{"email": nil}})
				return err
			}, "b", nil)
			rejectsUnique(t, e, c, "Insert of a long duplicate", func() error {
				_, err := c.Insert(ctx, &Document{ID: "b", Data: map[string]any{"email": long + "2"}})
				return err
			}, "b", nil)
			rejectsUnique(t, e, c, "Update to a duplicate", func() error {
				_, err := c.Update(ctx, "m1", map[string]any{"email": "x@y"})
				return err
			}, "m1", m1)
			rejectsUnique(t, e, c, "Replace with a duplicate", func() error {
				_, err := c.Replace(ctx, "m1", &Document{Data: map[string]any{"email": "x@y"}}, nil)
				return err
			}, "m1", m1)
			rejectsUnique(t, e, c, "upsert of a duplicate", func() error {
				_, _, err := c.UpdateWith(ctx, "new", UpdateSpec{Set: map[string]any{"email": "x@y"}, Upsert: true})
				return err
			}, "new", nil)
			err := func() error {
				_, err := c.Insert(ctx, &Document{ID: "b", Data: map[string]any{"email": "x@y"}})
				return err
			}()
			if err == nil || !strings.Contains(err.Error(), `"a"`) || !strings.Contains(err.Error(), "by_email") {
				t.Errorf("violation error %q does not name the index and the owner", err)
			}

			// Keeping one's own value is allowed.
			if _, err := c.Update(ctx, "a", map[string]any{"n": 2.0}); err != nil {
				t.Errorf("Update that keeps the document's own value: %v", err)
			}
			if _, err := c.Replace(ctx, "a", &Document{Data: map[string]any{"email": "x@y"}}, nil); err != nil {
				t.Errorf("Replace that keeps the document's own value: %v", err)
			}
			if _, _, err := c.UpdateWith(ctx, "a", UpdateSpec{Set: map[string]any{"email": "x@y", "k": 1.0}, If: Condition{{"email", "eq", "x@y"}}}); err != nil {
				t.Errorf("conditional update that keeps the value: %v", err)
			}
			// Moving a value away, or deleting its document, frees it.
			if _, err := c.Update(ctx, "a", map[string]any{"email": "z@y"}); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "b", map[string]any{"email": "x@y"})
			if err := c.Delete(ctx, "b"); err != nil {
				t.Fatal(err)
			}
			if flushed {
				mustFlush(t, c)
			}
			mustInsert(t, c, "b2", map[string]any{"email": "x@y"})
			rejectsUnique(t, e, c, "Insert of the moved-to value", func() error {
				_, err := c.Insert(ctx, &Document{ID: "c", Data: map[string]any{"email": "z@y"}})
				return err
			}, "c", nil)
			checkIndexes(t, c)

			// The constraint survives a reopen.
			_, c2 := reopenColl(t, e)
			if ix := c2.Indexes(); len(ix) != 1 || !ix[0].Unique {
				t.Errorf("Indexes after reopen = %+v, want the unique index", ix)
			}
			if _, err := c2.Insert(ctx, &Document{ID: "c", Data: map[string]any{"email": "x@y"}}); !errors.Is(err, ErrUniqueViolation) {
				t.Errorf("Insert of a duplicate after reopen = %v, want ErrUniqueViolation", err)
			}
		})
	}
}

// TestUniqueConcurrentWriters: 16 goroutines insert different ids with the
// same value; exactly one succeeds, also while flushes run and when the
// value already belongs to an unflushed or flushed document (then none
// does). After a crash and replay the index holds one entry.
func TestUniqueConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	const writers = 16
	for _, tc := range []struct {
		name     string
		owner    string // "" none, "unflushed", "flushed": a document holding the value before the race
		flushing bool   // flushes run during the race
	}{
		{name: "race"},
		{name: "race with flushes", flushing: true},
		{name: "owner unflushed", owner: "unflushed"},
		{name: "owner flushed", owner: "flushed"},
		{name: "owner unflushed with flushes", owner: "unflushed", flushing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := range 8 {
				opts, e, c := newCrashColl(t, nil, nil)
				if err := c.CreateIndex(ctx, "u", "v", Unique(true)); err != nil {
					t.Fatal(err)
				}
				want := 1
				if tc.owner != "" {
					mustInsert(t, c, "owner", map[string]any{"v": "same"})
					if tc.owner == "flushed" {
						mustFlush(t, c)
					}
					want = 0
				}
				var (
					ok    atomic.Int32
					wg    sync.WaitGroup
					start = make(chan struct{})
				)
				for g := range writers {
					wg.Go(func() {
						<-start
						_, err := c.Insert(ctx, &Document{ID: fmt.Sprintf("w%02d", g), Data: map[string]any{"v": "same"}})
						switch {
						case err == nil:
							ok.Add(1)
						case !errors.Is(err, ErrUniqueViolation):
							t.Errorf("writer %d: %v", g, err)
						}
						if tc.flushing && g%4 == 0 {
							if err := c.flush(); err != nil {
								t.Errorf("flush: %v", err)
							}
						}
					})
				}
				close(start)
				wg.Wait()
				if got := int(ok.Load()); got != want {
					t.Fatalf("round %d: %d of %d writers succeeded, want %d", round, got, writers, want)
				}
				if ids := indexLookup(t, c, "u", "same"); len(ids) != 1 {
					t.Fatalf("round %d: the value belongs to %v", round, ids)
				}

				// Crash and replay: still one document with the value.
				e.abandon()
				e = openEngine(t, opts)
				c = mustColl(t, mustDB(t, e, "app"), "c")
				if n, err := c.CountDocuments(ctx, &Query{Filters: []QueryFilter{{"v", "eq", "same"}}}); err != nil || n != 1 {
					t.Fatalf("round %d: after replay %d documents hold the value (%v), want 1", round, n, err)
				}
				mustFlush(t, c)
				if ids := indexLookup(t, c, "u", "same"); len(ids) != 1 {
					t.Fatalf("round %d: after replay and flush the index holds %v", round, ids)
				}
				checkIndexes(t, c)
				if _, err := c.Insert(ctx, &Document{ID: "late", Data: map[string]any{"v": "same"}}); !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("round %d: Insert after replay = %v, want ErrUniqueViolation", round, err)
				}
				if err := e.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestUniqueCreateOverDuplicates: creating a unique index over existing
// duplicates (flushed or only in the memtable) fails with
// ErrUniqueViolation, lists at most 10 values, and changes nothing.
func TestUniqueCreateOverDuplicates(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	if err := c.CreateIndex(ctx, "by_w", "w"); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		// v: 13 values held by 3 or 4 documents each (0..12), plus d39 alone.
		mustInsert(t, c, fmt.Sprintf("d%02d", i), map[string]any{"v": float64(i % 13), "w": float64(i), "x": float64(i)})
		if i == 20 {
			mustFlush(t, c)
		}
	}
	walBefore := e.wal.NextOffset()
	err := c.CreateIndex(ctx, "u", "v", Unique(true))
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("CreateIndex over duplicates = %v, want ErrUniqueViolation", err)
	}
	msg := err.Error()
	if n := strings.Count(msg, "(documents "); n != 10 {
		t.Errorf("the error lists %d values, want 10 (of 13): %s", n, msg)
	}
	if !strings.Contains(msg, "13 value(s)") || !strings.Contains(msg, "and 3 more value(s)") {
		t.Errorf("the error does not give the total and the rest: %s", msg)
	}
	if !strings.Contains(msg, `"d00", "d13", "d26", "d39"`) {
		t.Errorf("the error does not list the documents of value 0: %s", msg)
	}
	if got := e.wal.NextOffset(); got != walBefore {
		t.Errorf("the failed CreateIndex appended to the WAL (%d -> %d)", walBefore, got)
	}
	if ix := c.Indexes(); len(ix) != 1 || ix[0].Name != "by_w" {
		t.Errorf("Indexes = %+v, want only by_w", ix)
	}
	checkIndexes(t, c) // no bucket for u

	// A duplicate only in the memtable is found too.
	if err := c.CreateIndex(ctx, "ux", "x", Unique(true)); err != nil {
		t.Fatalf("CreateIndex over unique values: %v", err)
	}
	if err := c.DropIndex(ctx, "ux"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Update(ctx, "d00", map[string]any{"x": 39.0}); err != nil { // the unflushed d39 holds 39
		t.Fatal(err)
	}
	if err := c.CreateIndex(ctx, "ux", "x", Unique(true)); !errors.Is(err, ErrUniqueViolation) || !strings.Contains(err.Error(), `"d00", "d39"`) {
		t.Errorf("CreateIndex over a duplicate in the memtable = %v, want ErrUniqueViolation naming d00 and d39", err)
	}
	if ix := c.Indexes(); len(ix) != 1 {
		t.Errorf("Indexes = %+v, want only by_w", ix)
	}
	checkIndexes(t, c)

	// Nothing comes back after a crash and replay.
	e.abandon()
	e = openEngine(t, opts)
	c = mustColl(t, mustDB(t, e, "app"), "c")
	if ix := c.Indexes(); len(ix) != 1 || ix[0].Name != "by_w" {
		t.Errorf("Indexes after replay = %+v, want only by_w", ix)
	}
	checkIndexes(t, c)
}

// TestUniqueReplayViolationWarns: a committed WAL record that violates a
// unique index (it bypassed the check) is applied by replay with a WARN;
// open does not fail, later duplicates are still rejected, and the
// documents involved can still be updated.
func TestUniqueReplayViolationWarns(t *testing.T) {
	ctx := context.Background()
	for _, flushOwner := range []bool{false, true} {
		t.Run(fmt.Sprintf("owner flushed=%v", flushOwner), func(t *testing.T) {
			opts, e, c := newCrashColl(t, nil, nil)
			if err := c.CreateIndex(ctx, "u", "v", Unique(true)); err != nil {
				t.Fatal(err)
			}
			mustInsert(t, c, "a", map[string]any{"v": 1.0})
			if flushOwner {
				mustFlush(t, c)
			}
			if _, err := e.appendWAL(ctx, wal.Record{Op: wal.OpPutDoc, DB: "app", Coll: "c", ID: "b", Payload: []byte(`{"_id":"b","v":1}`)}); err != nil {
				t.Fatal(err)
			}
			e.abandon()

			var log syncBuffer
			e = openEngine(t, loggedOptions(opts.Dir, &log))
			c = mustColl(t, mustDB(t, e, "app"), "c")
			if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "violates a unique index") {
				t.Errorf("no WARN for the replayed violation:\n%s", log.String())
			}
			mustGet(t, c, "a")
			mustGet(t, c, "b")
			if _, err := c.Insert(ctx, &Document{ID: "c", Data: map[string]any{"v": 1.0}}); !errors.Is(err, ErrUniqueViolation) {
				t.Errorf("Insert of a third duplicate = %v, want ErrUniqueViolation", err)
			}
			mustFlush(t, c)
			if _, err := c.Update(ctx, "b", map[string]any{"w": 1.0}); err != nil {
				t.Errorf("Update of a replayed duplicate that keeps its value: %v", err)
			}
			if ids := indexLookup(t, c, "u", 1.0); len(ids) != 2 {
				t.Errorf("index entries of the value = %v, want a and b", ids)
			}
			checkIndexes(t, c)
		})
	}
}

// TestUniqueIndexDefinitionPersists: Unique is stored in coll.meta and the
// WAL payload of create_index (replayed after a crash), and a non-unique
// definition keeps its old encoding.
func TestUniqueIndexDefinitionPersists(t *testing.T) {
	ctx := context.Background()
	opts, e, c := newCrashColl(t, nil, nil)
	armed := opts
	if err := c.CreateIndex(ctx, "plain", "p"); err != nil {
		t.Fatal(err)
	}
	e.abandon()
	crashAt(t, &armed, hookMetaWrite)
	e, c = openColl(t, armed)
	if err := c.CreateIndex(ctx, "u", "v", Unique(true)); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("CreateIndex = %v, want the simulated crash", err)
	}
	e.abandon()
	_, c = openColl(t, opts)
	got := map[string]bool{}
	for _, ix := range c.Indexes() {
		got[ix.Name] = ix.Unique
	}
	if len(got) != 2 || got["plain"] || !got["u"] {
		t.Errorf("Indexes after replay = %v, want plain (not unique) and u (unique)", c.Indexes())
	}
	mustInsert(t, c, "a", map[string]any{"v": "x"})
	if _, err := c.Insert(ctx, &Document{ID: "b", Data: map[string]any{"v": "x"}}); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("Insert of a duplicate after the replayed create_index = %v", err)
	}
	m := readCollMetaFile(t, filepath.Join(c.dir, collMetaFile))
	for _, d := range m.Indexes {
		if d.Name == "u" && !d.Unique || d.Name == "plain" && d.Unique {
			t.Errorf("coll.meta index %+v", d)
		}
	}
	checkIndexes(t, c)
}
