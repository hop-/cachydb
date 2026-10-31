package segment

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v", path, err)
	}
	return false
}

// writeSegment creates segment seg holding n put records and returns their
// records and locations.
func writeSegment(t *testing.T, dir string, seg uint32, n int) ([]Record, []Location) {
	t.Helper()
	w := mustCreate(t, dir, seg)
	recs := make([]Record, n)
	for i := range recs {
		id := fmt.Sprintf("doc-%d", i)
		doc := []byte(fmt.Sprintf(`{"_id":%q}`, id))
		if i%2 == 1 {
			doc = largeDoc(id, 512+i)
		}
		recs[i] = Record{Type: TypePut, WALOffset: uint64(i + 1), ID: id, Doc: doc}
	}
	locs := mustAppend(t, w, recs)
	mustClose(t, w)
	return recs, locs
}

func mustOpenReader(t *testing.T, dir string, seg uint32) *Reader {
	t.Helper()
	r, err := OpenReader(dir, seg)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	return r
}

func TestReaderRetireLifecycle(t *testing.T) {
	tests := []struct {
		name string
		refs int
	}{
		{"no outstanding reference", 0},
		{"one outstanding reference", 1},
		{"two outstanding references", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			recs, locs := writeSegment(t, dir, 1, 4)
			path := DataPath(dir, 1)
			r := mustOpenReader(t, dir, 1)
			for range tt.refs {
				if !r.Acquire() {
					t.Fatal("Acquire on a live reader failed")
				}
			}
			r.Retire()
			r.Retire() // idempotent
			if r.Acquire() {
				t.Fatal("Acquire after Retire succeeded")
			}
			for i := range tt.refs {
				if !fileExists(t, path) {
					t.Fatalf("file removed while %d reference(s) held", tt.refs-i)
				}
				// A retired reader still serves holders of a reference.
				rec, err := r.Read(locs[1])
				if err != nil || !sameRecord(rec, recs[1]) {
					t.Fatalf("Read on a retired, referenced reader = %+v, %v", rec, err)
				}
				r.Release()
			}
			if fileExists(t, path) {
				t.Fatal("file still exists after the last Release")
			}
			r.Release() // unbalanced Release is ignored
		})
	}
}

func TestReaderClose(t *testing.T) {
	dir := t.TempDir()
	_, locs := writeSegment(t, dir, 1, 2)
	path := DataPath(dir, 1)

	r := mustOpenReader(t, dir, 1)
	if !r.Acquire() {
		t.Fatal("Acquire failed")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if r.Acquire() {
		t.Fatal("Acquire after Close succeeded")
	}
	if _, err := r.Read(locs[0]); err != nil {
		t.Fatalf("Read with a held reference after Close: %v", err)
	}
	r.Release()
	if _, err := r.Read(locs[0]); err == nil {
		t.Fatal("Read after the file was closed succeeded")
	}
	if !fileExists(t, path) {
		t.Fatal("Close removed the file")
	}
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	r.Retire() // Retire after Close still removes the file
	if fileExists(t, path) {
		t.Error("Retire after Close left the file")
	}
}

func TestOpenReaderBadHeader(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(DataPath(dir, 1), []byte("not a segment file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(dir, 1); !errors.Is(err, ErrCorrupt) {
		t.Errorf("OpenReader error = %v, want ErrCorrupt", err)
	}
	if _, err := OpenReader(dir, 2); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("OpenReader(missing) error = %v, want ErrNotExist", err)
	}
}

// Concurrent Read calls racing with Retire: run with -race. The file must
// exist while any reference is held and be removed after the last Release.
func TestReaderConcurrentReadAndRetire(t *testing.T) {
	dir := t.TempDir()
	recs, locs := writeSegment(t, dir, 1, 32)
	path := DataPath(dir, 1)
	r := mustOpenReader(t, dir, 1)

	const workers = 16
	var (
		wg      sync.WaitGroup
		reads   atomic.Int64
		started = make(chan struct{}, workers)
		errs    = make(chan error, workers)
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			signaled := false
			for i := 0; ; i++ {
				if !r.Acquire() {
					if !signaled {
						started <- struct{}{}
					}
					return
				}
				j := (w + i) % len(locs)
				rec, err := r.Read(locs[j])
				if err == nil && !sameRecord(rec, recs[j]) {
					err = fmt.Errorf("record %d mismatch: %+v", j, rec)
				}
				if err == nil {
					if _, serr := os.Stat(path); serr != nil {
						err = fmt.Errorf("file gone while a reference is held: %w", serr)
					}
				}
				r.Release()
				if err != nil {
					errs <- err
					return
				}
				reads.Add(1)
				if !signaled {
					signaled = true
					started <- struct{}{}
				}
			}
		}()
	}
	for range workers {
		<-started
	}
	r.Retire()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if reads.Load() == 0 {
		t.Error("no reads completed")
	}
	if fileExists(t, path) {
		t.Error("file still exists after all references were released")
	}
}
