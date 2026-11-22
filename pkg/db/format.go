package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// FormatVersion is the on-disk data-directory format this build reads and
// writes. Version 1 is the legacy layout (no cachydb.format file). Version
// 2 (v0.0.4 to v0.0.6: hint files, index snapshots) is upgraded to version
// 3 (one index.db per collection) by Open (upgrade.go).
const FormatVersion = 3

// upgradableFormatVersion is the one older data-directory format this
// build upgrades in place to FormatVersion.
const upgradableFormatVersion = 2

// WALFormatVersion is the write-ahead log segment format this build reads
// and writes.
const WALFormatVersion = wal.FormatVersion

const (
	formatFileName = "cachydb.format"
	formatName     = "cachydb"
)

// formatFile is the JSON content of <root>/cachydb.format.
type formatFile struct {
	Format  string `json:"format"`
	Version int    `json:"version"`
}

// errFormatUpgrade is validateFormatFile's result for a directory of
// upgradableFormatVersion: readable, after the upgrade.
var errFormatUpgrade = errors.New("data directory format needs an upgrade")

// checkDataDir creates root if needed, detects the old layout
// (ErrIncompatibleDataDir with the "move or delete it" message), and
// writes/validates cachydb.format. upgrade is true for a directory of
// format version 2, which Open upgrades (upgradeFormat) before it loads
// the collections; the format file still says 2 until that finished.
func checkDataDir(root string) (upgrade bool, err error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return false, fmt.Errorf("creating data directory %s: %w", root, err)
	}

	path := filepath.Join(root, formatFileName)
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		err := validateFormatFile(root, data)
		if errors.Is(err, errFormatUpgrade) {
			return true, nil
		}
		return false, err
	case !errors.Is(err, os.ErrNotExist):
		return false, fmt.Errorf("reading %s: %w", path, err)
	}

	old, err := hasOldLayout(root)
	if err != nil {
		return false, err
	}
	if old {
		return false, fmt.Errorf("%w: data directory %s was created by an older cachydb format; move or delete it",
			ErrIncompatibleDataDir, root)
	}
	return false, writeFormatFile(root)
}

// writeFormatFile writes cachydb.format with FormatVersion, atomically.
func writeFormatFile(root string) error {
	data, err := json.Marshal(formatFile{Format: formatName, Version: FormatVersion})
	if err != nil {
		return fmt.Errorf("encoding %s: %w", formatFileName, err)
	}
	if err := fsutil.WriteFileAtomic(filepath.Join(root, formatFileName), data, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", formatFileName, err)
	}
	return nil
}

// validateFormatFile checks the content of cachydb.format: nil for
// FormatVersion, errFormatUpgrade for upgradableFormatVersion, else
// ErrIncompatibleDataDir.
func validateFormatFile(root string, data []byte) error {
	var ff formatFile
	if err := json.Unmarshal(data, &ff); err != nil {
		return fmt.Errorf("%w: data directory %s has an unreadable %s: %v",
			ErrIncompatibleDataDir, root, formatFileName, err)
	}
	if ff.Format != formatName {
		return fmt.Errorf("%w: data directory %s: %s has format %q, want %q",
			ErrIncompatibleDataDir, root, formatFileName, ff.Format, formatName)
	}
	switch {
	case ff.Version == FormatVersion:
		return nil
	case ff.Version == upgradableFormatVersion:
		return errFormatUpgrade
	case ff.Version > FormatVersion:
		return fmt.Errorf("%w: data directory %s has format version %d, newer than supported version %d; upgrade cachydb",
			ErrIncompatibleDataDir, root, ff.Version, FormatVersion)
	default:
		return fmt.Errorf("%w: data directory %s was created by an older cachydb format (version %d) that this cachydb cannot upgrade (it upgrades version %d); move or delete it",
			ErrIncompatibleDataDir, root, ff.Version, upgradableFormatVersion)
	}
}

// hasOldLayout reports whether root holds files of the legacy (version 1)
// layout: wal.checkpoint, old-style wal-<unix>-<offset>.log segments, or a
// <db>/db.meta.json file.
func hasOldLayout(root string) (bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, fmt.Errorf("reading data directory %s: %w", root, err)
	}
	for _, e := range entries {
		name := e.Name()
		if name == "wal.checkpoint" || isOldWALName(name) {
			return true, nil
		}
		if !e.IsDir() {
			continue
		}
		_, err := os.Stat(filepath.Join(root, name, "db.meta.json"))
		switch {
		case err == nil:
			return true, nil
		case !errors.Is(err, os.ErrNotExist):
			return false, fmt.Errorf("checking %s for legacy metadata: %w", filepath.Join(root, name), err)
		}
	}
	return false, nil
}

// isOldWALName matches the legacy wal-<unix>-<offset>.log naming. Current
// WAL segments are wal-<20-digit offset>.log, with a single dash.
func isOldWALName(name string) bool {
	mid, ok := strings.CutPrefix(name, "wal-")
	if !ok {
		return false
	}
	mid, ok = strings.CutSuffix(mid, ".log")
	return ok && strings.Contains(mid, "-")
}
