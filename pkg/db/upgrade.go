package db

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// The format upgrade (approach-index-and-query §9). A data directory of
// format version 2 (v0.0.4 to v0.0.6) has no index.db: its key tables were
// loaded from the data segments and hint files, its secondary indexes from
// index snapshots (idx-*.snap). Open upgrades such a directory in place,
// after taking the lock and reading the metadata, before it loads the
// collections and replays the WAL:
//
//  1. Every index.db in the directory is deleted: in a format-2 directory
//     it can only be the partial result of an interrupted upgrade.
//  2. Per collection, one after the other: log one INFO line and build
//     index.db from the data segments (rebuildIndex: the newest WAL offset
//     per id wins, tombstones remove, the checkpoint is the newest
//     checkpoint marker, 0 if none).
//  3. After every collection succeeded, delete the hint files and index
//     snapshots of every collection, then write cachydb.format with
//     version 3 (atomically). Until then the directory still says version
//     2, so a crash at any point restarts the upgrade from step 1.
//
// The data segments and the WAL keep their format; nothing is converted
// back. Collections are upgraded one at a time to bound the memory a
// rebuild needs (one id map per collection).

// upgradeFormat upgrades the data directory from format version 2 to 3.
// The caller has loaded the metadata (loadMetadata) and not yet loaded the
// collections; the index files it builds are closed again, and
// loadCollections opens them as in any format-3 directory.
//
// The files of format 2 are removed only after every collection's index
// file was built, so an upgrade that fails (for example on a document id
// that is too long for the index file) leaves a directory that the release
// which wrote it still opens; the index files it built are removed again
// (best effort; the next upgrade removes them anyway).
func (e *Engine) upgradeFormat(ctx context.Context) (err error) {
	colls := e.collectionsInOrder()
	wrap := func(err error) error {
		return fmt.Errorf("upgrading data directory format %d -> %d: %w", upgradableFormatVersion, FormatVersion, err)
	}
	for _, c := range colls {
		if err := idxstore.Remove(c.dir); err != nil {
			return wrap(fmt.Errorf("removing the index file of an interrupted upgrade: %w", err))
		}
	}
	defer func() {
		if err == nil || errors.Is(err, errSimulatedCrash) {
			return
		}
		for _, c := range colls {
			if rerr := idxstore.Remove(c.dir); rerr != nil {
				e.logger.Warn("cachydb: removing an index file after a failed format upgrade", "dir", c.dir, "err", rerr)
			}
		}
	}()
	for _, c := range colls {
		if err := c.upgrade(ctx); err != nil {
			return wrap(fmt.Errorf("collection %q in database %q: %w", c.name, c.db.name, err))
		}
		e.recov.collectionsUpgraded.Add(1)
		if err := e.hook(hookUpgradeCollection); err != nil {
			return err
		}
	}
	for _, c := range colls {
		if err := removeFormat2Files(c.dir); err != nil {
			return wrap(err)
		}
	}
	if err := e.hook(hookUpgradeBeforeFormat); err != nil {
		return err
	}
	if err := writeFormatFile(e.root); err != nil {
		return wrap(err)
	}
	return nil
}

// collectionsInOrder returns every loaded collection, by database name and
// then collection name.
func (e *Engine) collectionsInOrder() []*Collection {
	var colls []*Collection
	for _, d := range e.databases {
		for _, c := range d.collections {
			colls = append(colls, c)
		}
	}
	slices.SortFunc(colls, func(a, b *Collection) int {
		return cmp.Or(cmp.Compare(a.db.name, b.db.name), cmp.Compare(a.name, b.name))
	})
	return colls
}

// upgrade builds the collection's index file from its data segments
// (upgradeFormat step 2) and closes it. Callers have not published c yet.
func (c *Collection) upgrade(ctx context.Context) error {
	c.db.e.logger.Info("cachydb: upgrading data directory format 2 -> 3; building index",
		"database", c.db.name, "collection", c.name, "dir", c.dir)
	st, err := c.rebuildIndex(ctx)
	if err != nil {
		return err
	}
	return st.Close()
}

// removeFormat2Files deletes the hint files (seg-*.hint) and index
// snapshots (idx-*.snap) of format version 2 from a collection directory,
// with the temporary files an interrupted write of one left.
func removeFormat2Files(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	removed := false
	for _, ent := range entries {
		if ent.IsDir() || !isFormat2File(ent.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, ent.Name())); err != nil {
			return fmt.Errorf("removing a file of data directory format %d: %w", upgradableFormatVersion, err)
		}
		removed = true
	}
	if !removed {
		return nil
	}
	return fsutil.SyncDir(dir)
}

// isFormat2File matches seg-*.hint and idx-*.snap, and their temporary
// files (<name>.tmp-<random>, fsutil.WriteFileAtomic).
func isFormat2File(name string) bool {
	if i := strings.Index(name, ".tmp-"); i >= 0 {
		name = name[:i]
	}
	return strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".hint") ||
		strings.HasPrefix(name, "idx-") && strings.HasSuffix(name, ".snap")
}
