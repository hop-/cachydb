//go:build unix

package fsutil

import (
	"fmt"
	"os"
)

// SyncDir fsyncs a directory so that entries created, renamed, or removed
// in it are durable (no-op error-free on platforms without dir fsync).
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("syncing dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close() // already failing; the sync error is reported
		return fmt.Errorf("syncing dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("syncing dir %s: closing: %w", dir, err)
	}
	return nil
}
