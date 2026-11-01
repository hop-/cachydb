//go:build unix

package fsutil

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// SyncDir fsyncs a directory so that entries created, renamed, or removed
// in it are durable. A file system that does not support fsync on a
// directory (EINVAL or ENOTSUP, for example some network and FUSE file
// systems) is treated as success: there is nothing more the engine can do.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("syncing dir %s: %w", dir, err)
	}
	if err := ignoreUnsupportedSync(d.Sync()); err != nil {
		_ = d.Close() // already failing; the sync error is reported
		return fmt.Errorf("syncing dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("syncing dir %s: closing: %w", dir, err)
	}
	return nil
}

// ignoreUnsupportedSync returns nil for the errors a file system gives when
// it cannot fsync a directory, and err otherwise.
func ignoreUnsupportedSync(err error) error {
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		return nil
	}
	return err
}
