package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

// Metadata file names and the prefix of directories awaiting removal (§1, §6).
const (
	dbMetaFile    = "db.meta"
	collMetaFile  = "coll.meta"
	deletedPrefix = ".deleted-"
	metaPerm      = 0o644
	dirPerm       = 0o755
)

// dbMeta is the JSON content of <root>/<db>/db.meta.
type dbMeta struct {
	Name          string `json:"name"`
	SchemaVersion int    `json:"schema_version"`
	WALOffset     uint64 `json:"wal_offset"` // offset of the record that last wrote this file's state
}

// indexDef is an index definition as stored in coll.meta and in
// create_index WAL payloads.
type indexDef struct {
	Name  string `json:"name"`
	Field string `json:"field"`
}

// collMeta is the JSON content of <root>/<db>/<coll>/coll.meta.
// WALOffset is the offset of the record that last wrote the file (create
// or an index change); CreatedOffset is the offset of the create_coll
// record of this incarnation of the collection, which WAL replay uses to
// skip records of an earlier, dropped collection with the same name
// (§7.3). 0 means unknown.
type collMeta struct {
	Name          string     `json:"name"`
	Schema        *Schema    `json:"schema"`
	Indexes       []indexDef `json:"indexes"` // sorted by name; never null
	WALOffset     uint64     `json:"wal_offset"`
	CreatedOffset uint64     `json:"created_offset"`
}

// WAL payloads for metadata records (§3.1).
type createCollPayload struct {
	Schema *Schema `json:"schema"`
}

type dropIndexPayload struct {
	Name string `json:"name"`
}

func dbDir(root, db string) string         { return filepath.Join(root, db) }
func collDir(root, db, coll string) string { return filepath.Join(root, db, coll) }

// deletedDir is where a dropped database (parent = root) or collection
// (parent = its database directory) is renamed to before removal.
func deletedDir(parent string, off uint64) string {
	return filepath.Join(parent, deletedPrefix+strconv.FormatUint(off, 10))
}

func writeMeta(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding %s: %w", path, err)
	}
	if err := fsutil.WriteFileAtomic(path, data, metaPerm); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

func writeDBMeta(dir string, m dbMeta) error {
	return writeMeta(filepath.Join(dir, dbMetaFile), m)
}

func writeCollMeta(dir string, m collMeta) error {
	if m.Indexes == nil {
		m.Indexes = []indexDef{}
	}
	return writeMeta(filepath.Join(dir, collMetaFile), m)
}

// readMeta decodes the meta file at path into v. A missing file returns an
// error satisfying errors.Is(err, os.ErrNotExist).
func readMeta(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("decoding %s: %w", path, err)
	}
	return nil
}

// makeDir creates dir (if missing) and fsyncs its parent so the new entry
// is durable.
func makeDir(dir string) error {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}
	if err := fsutil.SyncDir(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}
	return nil
}

// renameAway atomically renames dir to its .deleted-<off> name in the same
// parent and fsyncs the parent. It returns the new path.
func renameAway(dir string, off uint64) (string, error) {
	parent := filepath.Dir(dir)
	dst := deletedDir(parent, off)
	if err := os.Rename(dir, dst); err != nil {
		return "", fmt.Errorf("renaming %s for removal: %w", dir, err)
	}
	if err := fsutil.SyncDir(parent); err != nil {
		return "", fmt.Errorf("renaming %s for removal: %w", dir, err)
	}
	return dst, nil
}

// removeDeletedLeftovers removes every .deleted-* directory left in root
// (dropped databases) and in each database directory (dropped
// collections) by a drop whose background removal did not finish.
func removeDeletedLeftovers(root string) error {
	var errs []error
	removeIn := func(dir string) []os.DirEntry {
		entries, err := os.ReadDir(dir)
		if err != nil {
			errs = append(errs, fmt.Errorf("reading %s: %w", dir, err))
			return nil
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), deletedPrefix) {
				p := filepath.Join(dir, e.Name())
				if err := os.RemoveAll(p); err != nil {
					errs = append(errs, fmt.Errorf("removing leftover %s: %w", p, err))
				}
			}
		}
		return entries
	}
	for _, e := range removeIn(root) {
		if e.IsDir() && validName(e.Name()) {
			removeIn(filepath.Join(root, e.Name()))
		}
	}
	return errors.Join(errs...)
}

// cloneSchema returns a copy of s that shares no maps with it.
func cloneSchema(s *Schema) *Schema {
	if s == nil {
		return nil
	}
	return &Schema{Fields: maps.Clone(s.Fields)}
}

// sortedIndexDefs returns the definitions in defs ordered by name.
func sortedIndexDefs(defs map[string]indexDef) []indexDef {
	out := make([]indexDef, 0, len(defs))
	for _, name := range slices.Sorted(maps.Keys(defs)) {
		out = append(out, defs[name])
	}
	return out
}
