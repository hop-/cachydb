package db

import (
	"context"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// AC-4: each collection directory holds index.db, no hint or index
// snapshot file is written, and bbolt is imported only from idxstore.

// TestFreshDirectoryFileKinds walks a new format-3 data directory after
// flushes, sealed segments, a compaction, and Close, and allows only the
// file kinds format 3 writes: no hint, snapshot, or temporary file.
func TestFreshDirectoryFileKinds(t *testing.T) {
	ctx := context.Background()
	e, c, _ := newFlushColl(t, func(o *Options) {
		o.DataSegmentSize = 4 << 10
		o.CompactionRatio = 0.3
	})
	if err := c.CreateIndex(ctx, "by_k", "k"); err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		mustInsert(t, c, fmt.Sprintf("doc-%03d", i), map[string]any{"k": float64(i % 7), "pad": strings.Repeat("p", 80)})
		if i%25 == 24 {
			mustFlush(t, c)
		}
	}
	for i := range 200 {
		if i%3 != 0 {
			if err := c.Delete(ctx, fmt.Sprintf("doc-%03d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	mustFlushSeal(t, c)
	mustInsert(t, c, "tail", map[string]any{"k": 1.0})
	mustFlushSeal(t, c)
	if !mustCompact(t, c) {
		t.Fatal("no compaction ran; the test needs one")
	}
	mustInsert(t, c, "after", map[string]any{"k": 2.0}) // left to Close
	root, collDir := e.root, c.dir
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}

	collRel, err := filepath.Rel(root, collDir)
	if err != nil {
		t.Fatal(err)
	}
	dbRel := filepath.Dir(collRel)
	segName := regexp.MustCompile(`^seg-\d+\.data$`)
	walName := regexp.MustCompile(`^wal-\d+\.log$`)
	allowed := func(rel string) bool {
		dir, name := filepath.Dir(rel), filepath.Base(rel)
		switch dir {
		case ".":
			return name == formatFileName || name == lockFileName || name == metaCheckpointFileName || walName.MatchString(name)
		case dbRel:
			return name == dbMetaFile
		case collRel:
			return name == collMetaFile || name == idxstore.FileName || segName.MatchString(name)
		}
		return false
	}
	var files []string
	var sawIndex, sawSeg bool
	err = filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files = append(files, rel)
		if !allowed(rel) {
			t.Errorf("unexpected file %s in a fresh format-3 data directory", rel)
		}
		sawIndex = sawIndex || rel == filepath.Join(collRel, idxstore.FileName)
		sawSeg = sawSeg || (filepath.Dir(rel) == collRel && segName.MatchString(filepath.Base(rel)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawIndex || !sawSeg {
		t.Errorf("the collection directory lacks index.db or a data segment: %v", files)
	}
	if t.Failed() {
		t.Logf("files: %v", files)
	}
}

// TestBboltImportedOnlyByIdxstore reads the imports of every Go file of
// the module (tests included) and fails if one outside
// pkg/db/internal/idxstore imports go.etcd.io/bbolt.
func TestBboltImportedOnlyByIdxstore(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	allowedDir := filepath.Join(root, "pkg", "db", "internal", "idxstore")
	fset := token.NewFileSet()
	parsed, importers := 0, 0
	err = filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			if name := de.Name(); p != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		parsed++
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if path != "go.etcd.io/bbolt" && !strings.HasPrefix(path, "go.etcd.io/bbolt/") {
				continue
			}
			importers++
			if filepath.Dir(p) != allowedDir {
				rel, _ := filepath.Rel(root, p)
				t.Errorf("%s imports %s; only pkg/db/internal/idxstore may", rel, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if parsed < 50 || importers == 0 {
		t.Fatalf("parsed %d Go files under %s and found %d bbolt imports: the walk did not cover the module", parsed, root, importers)
	}
}
