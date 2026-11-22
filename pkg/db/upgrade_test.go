package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/test/compat"
)

// The format upgrade 2 -> 3 (AC-5, approach-index-and-query §9) on the
// release fixtures. TestCompatFixtures checks the upgraded content against
// each release's export; these tests check the upgrade itself: logging,
// files, crash safety, and that it runs once.

var compatVersions = []string{"v0.0.4", "v0.0.5", "v0.0.6"}

// copyFixture copies the data directory of the release fixture v into a
// temporary directory (the fixtures are read-only inputs).
func copyFixture(t *testing.T, v string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	if err := compat.CopyDir(filepath.Join("testdata", "compat", v, "data"), dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

// loggedOptions returns options for dir whose logger writes text lines at
// every level into buf.
func loggedOptions(dir string, buf *syncBuffer) Options {
	o := testOptions(dir)
	o.Logger = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return o
}

// dataDump is the content of every collection of e, as Find returns it.
func dataDump(t *testing.T, e *Engine) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, dn := range e.ListDatabases() {
		d := mustDB(t, e, dn)
		for _, cn := range d.ListCollections() {
			c := mustColl(t, d, cn)
			docs, err := c.Find(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(docs)
			if err != nil {
				t.Fatal(err)
			}
			ix, err := json.Marshal(c.Indexes())
			if err != nil {
				t.Fatal(err)
			}
			out[dn+"/"+cn] = string(b) + " indexes " + string(ix)
		}
	}
	return out
}

func formatVersionOf(t *testing.T, dir string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, formatFileName))
	if err != nil {
		t.Fatal(err)
	}
	var ff formatFile
	if err := json.Unmarshal(data, &ff); err != nil {
		t.Fatalf("format file %q: %v", data, err)
	}
	return ff.Version
}

// format2Files lists the hint files, index snapshots, and their temporary
// files under dir.
func format2Files(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := de.Name()
		if strings.HasSuffix(name, ".hint") || strings.HasSuffix(name, ".snap") || strings.Contains(name, ".hint.tmp-") || strings.Contains(name, ".snap.tmp-") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// indexFiles lists the index.db files under dir.
func indexFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
		if err == nil && de.Name() == idxstore.FileName {
			out = append(out, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// dirListing renders every file under dir with its size.
func dirListing(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() || de.Name() == lockFileName {
			return err
		}
		fi, err := de.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		out = append(out, fmt.Sprintf("%s %d %s", rel, fi.Size(), fi.ModTime()))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// checkUpgradeLog checks the log of an upgrading open: exactly one INFO
// line per collection with the upgrade wording, and nothing that sounds
// like damage (AC-5: the upgrade never says "corrupt").
func checkUpgradeLog(t *testing.T, log string, collections int) {
	t.Helper()
	if n := strings.Count(log, "upgrading data directory format 2 -> 3; building index"); n != collections {
		t.Errorf("%d upgrade lines in the log, want one per collection (%d):\n%s", n, collections, log)
	}
	for line := range strings.SplitSeq(log, "\n") {
		if strings.Contains(line, "upgrading data directory") && !strings.Contains(line, "level=INFO") {
			t.Errorf("upgrade line not at INFO: %s", line)
		}
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") {
			t.Errorf("the upgrade logged a warning or error: %s", line)
		}
		lower := strings.ToLower(line)
		for _, word := range []string{"corrupt", "invalid", "damaged"} {
			if strings.Contains(lower, word) {
				t.Errorf("the upgrade log says %q: %s", word, line)
			}
		}
	}
}

func collectionCount(t *testing.T, e *Engine) int {
	t.Helper()
	n := 0
	for _, dn := range e.ListDatabases() {
		n += len(mustDB(t, e, dn).ListCollections())
	}
	return n
}

// TestUpgradeFixtures: each format-2 fixture is upgraded once, with one
// INFO line per collection and nothing that sounds like damage; afterwards
// the format file says 3, every collection has index.db, no hint file or
// index snapshot is left, and a second open does no upgrade work and reads
// the same data.
func TestUpgradeFixtures(t *testing.T) {
	ctx := context.Background()
	for _, v := range compatVersions {
		t.Run(v, func(t *testing.T) {
			dir := copyFixture(t, v)
			if got := formatVersionOf(t, dir); got != 2 {
				t.Fatalf("fixture %s has format %d, want 2", v, got)
			}
			if len(format2Files(t, dir)) == 0 {
				t.Fatalf("fixture %s has no hint files or snapshots to remove", v)
			}
			// A leftover temporary hint file of an interrupted write.
			tmp := filepath.Join(dir, "kb", "notes", "seg-000001.hint.tmp-123")
			writeFile(t, tmp, "x")

			var log syncBuffer
			e := openEngine(t, loggedOptions(dir, &log))
			want := dataDump(t, e)
			n := collectionCount(t, e)
			if n < 3 {
				t.Fatalf("fixture %s has %d collections, expected several", v, n)
			}
			if got := e.recov.collectionsUpgraded.Load(); got != int64(n) {
				t.Errorf("collections upgraded = %d, want %d", got, n)
			}
			if got := e.recov.indexFilesRebuilt.Load(); got != 0 {
				t.Errorf("index files rebuilt = %d, want 0 (an upgrade is not a repair)", got)
			}
			if err := e.Close(ctx); err != nil {
				t.Fatal(err)
			}
			checkUpgradeLog(t, log.String(), n)
			if got := formatVersionOf(t, dir); got != 3 {
				t.Errorf("format after the upgrade = %d, want 3", got)
			}
			if left := format2Files(t, dir); len(left) != 0 {
				t.Errorf("format-2 files left after the upgrade: %v", left)
			}
			if got := len(indexFiles(t, dir)); got != n {
				t.Errorf("%d index.db files, want one per collection (%d)", got, n)
			}

			// Second open: no upgrade, no rebuild, no segment scan.
			var log2 syncBuffer
			e = openEngine(t, loggedOptions(dir, &log2))
			if a, b, c := e.recov.collectionsUpgraded.Load(), e.recov.indexFilesRebuilt.Load(), e.recov.segmentsScanned.Load(); a != 0 || b != 0 || c != 0 {
				t.Errorf("second open: upgraded %d, rebuilt %d, scanned %d segments; want no work", a, b, c)
			}
			if strings.Contains(log2.String(), "upgrading") || strings.Contains(log2.String(), "rebuilding") {
				t.Errorf("second open log mentions an upgrade or rebuild:\n%s", log2.String())
			}
			if got := dataDump(t, e); !mapsEqual(got, want) {
				t.Errorf("data after the second open differs:\n got %v\nwant %v", got, want)
			}
			checkModel(t, e, captureModel(t, e)) // index entries agree with the documents
		})
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestUpgradeCrash: a crash after any collection's index file was built,
// or after the format-2 files were removed but before the format file was
// rewritten, leaves a format-2 directory that the next open upgrades from
// the start, with the same data.
func TestUpgradeCrash(t *testing.T) {
	ctx := context.Background()
	for _, v := range compatVersions {
		t.Run(v, func(t *testing.T) {
			// The reference: a clean upgrade.
			ref := openEngine(t, testOptions(copyFixture(t, v)))
			want := dataDump(t, ref)
			n := collectionCount(t, ref)
			if err := ref.Close(ctx); err != nil {
				t.Fatal(err)
			}
			// Crash at the k-th upgrade hook: k <= n is hookUpgradeCollection
			// after collection k, k == n+1 is hookUpgradeBeforeFormat.
			for k := 1; k <= n+1; k++ {
				dir := copyFixture(t, v)
				o := testOptions(dir)
				calls := 0
				var points []hookPoint
				o.testHook = func(p hookPoint) error {
					if p != hookUpgradeCollection && p != hookUpgradeBeforeFormat {
						return nil
					}
					calls++
					points = append(points, p)
					if calls == k {
						return errSimulatedCrash
					}
					return nil
				}
				if _, err := Open(ctx, o); !errors.Is(err, errSimulatedCrash) {
					t.Fatalf("k=%d: Open = %v, want the simulated crash", k, err)
				}
				wantPoint := hookUpgradeCollection
				if k == n+1 {
					wantPoint = hookUpgradeBeforeFormat
				}
				if points[len(points)-1] != wantPoint {
					t.Fatalf("k=%d: crashed at %s, want %s", k, points[len(points)-1], wantPoint)
				}
				if got := formatVersionOf(t, dir); got != 2 {
					t.Fatalf("k=%d: format after the crash = %d, want 2", k, got)
				}
				if left := format2Files(t, dir); (len(left) > 0) != (k <= n) {
					t.Errorf("k=%d: format-2 files after the crash: %v", k, left)
				}
				// A partly written index file of the interrupted upgrade must
				// not matter either.
				for _, p := range indexFiles(t, dir) {
					if err := os.Truncate(p, 100); err != nil {
						t.Fatal(err)
					}
					break
				}

				var log syncBuffer
				e, err := Open(ctx, loggedOptions(dir, &log))
				if err != nil {
					t.Fatalf("k=%d: open after the crash: %v", k, err)
				}
				if got := dataDump(t, e); !mapsEqual(got, want) {
					t.Errorf("k=%d: data differs from a clean upgrade:\n got %v\nwant %v", k, got, want)
				}
				if got := e.recov.collectionsUpgraded.Load(); got != int64(n) {
					t.Errorf("k=%d: collections upgraded after the crash = %d, want all %d (the upgrade restarts)", k, got, n)
				}
				checkModel(t, e, captureModel(t, e))
				if err := e.Close(ctx); err != nil {
					t.Fatal(err)
				}
				checkUpgradeLog(t, log.String(), n)
				if got := formatVersionOf(t, dir); got != 3 {
					t.Errorf("k=%d: format = %d, want 3", k, got)
				}
				if left := format2Files(t, dir); len(left) != 0 {
					t.Errorf("k=%d: format-2 files left: %v", k, left)
				}
			}
		})
	}
}

// TestUpgradeRejectsOtherVersions: versions below 2 and above 3 still fail
// with ErrIncompatibleDataDir and change nothing.
func TestUpgradeRejectsOtherVersions(t *testing.T) {
	for _, version := range []string{"0", "1", "4", "99"} {
		t.Run("version "+version, func(t *testing.T) {
			dir := copyFixture(t, "v0.0.6")
			content := `{"format":"cachydb","version":` + version + `}`
			writeFile(t, filepath.Join(dir, formatFileName), content)
			before := dirListing(t, dir)
			_, err := Open(context.Background(), testOptions(dir))
			if !errors.Is(err, ErrIncompatibleDataDir) {
				t.Fatalf("Open = %v, want ErrIncompatibleDataDir", err)
			}
			if after := dirListing(t, dir); !slices.Equal(after, before) {
				t.Errorf("a rejected open changed the directory:\n%v\n->\n%v", before, after)
			}
			if data, _ := os.ReadFile(filepath.Join(dir, formatFileName)); string(data) != content {
				t.Errorf("format file = %q, want %q unchanged", data, content)
			}
		})
	}
}

// appendLongIDRecord adds data segment 99 holding a put whose id is one
// byte longer than the index file can hold.
func appendLongIDRecord(t *testing.T, collDir string) {
	t.Helper()
	w, err := segment.Create(collDir, 99)
	if err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("k", idxstore.MaxIDLen+1)
	if _, err := w.Append([]segment.Record{{Type: segment.TypePut, WALOffset: 1, ID: long, Doc: []byte(`{"_id":"` + long + `"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestUpgradeLongIDFails: an id too long for the index file stops the
// upgrade with an error naming the collection and the limit; the directory
// stays a format-2 directory the old release opens (no index.db, the hint
// files and snapshots kept, the format file not rewritten).
func TestUpgradeLongIDFails(t *testing.T) {
	dir := copyFixture(t, "v0.0.6")
	appendLongIDRecord(t, filepath.Join(dir, "shop", "items"))
	hints := format2Files(t, dir)
	_, err := Open(context.Background(), testOptions(dir))
	if err == nil {
		t.Fatal("Open succeeded with an id longer than the index file can hold")
	}
	for _, want := range []string{`"items"`, "31742"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
	if got := formatVersionOf(t, dir); got != 2 {
		t.Errorf("format = %d after the failed upgrade, want 2", got)
	}
	if left := indexFiles(t, dir); len(left) != 0 {
		t.Errorf("index files left by the failed upgrade: %v", left)
	}
	if got := format2Files(t, dir); !slices.Equal(got, hints) {
		t.Errorf("format-2 files after the failed upgrade = %v, want %v", got, hints)
	}
}
