// Package fsutil provides the durable file-system primitives used by the
// cachydb storage engine: atomic file replacement, directory fsync, and an
// exclusive data-directory lock.
package fsutil

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// WriteFileAtomic writes data to path.tmp-<rand>, fsyncs it, renames it
// over path, then fsyncs the parent directory. On any error the temporary
// file is removed and path is left untouched.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) (err error) {
	suffix, err := randomSuffix()
	if err != nil {
		return fmt.Errorf("atomic write %s: %w", path, err)
	}
	tmp := path + ".tmp-" + suffix

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return fmt.Errorf("atomic write %s: creating temp file: %w", path, err)
	}
	defer func() {
		if err != nil {
			// Best-effort cleanup; the original error is what matters.
			_ = os.Remove(tmp)
		}
	}()

	if _, err = f.Write(data); err != nil {
		_ = f.Close() // already failing; the write error is reported
		return fmt.Errorf("atomic write %s: writing temp file: %w", path, err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close() // already failing; the sync error is reported
		return fmt.Errorf("atomic write %s: syncing temp file: %w", path, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("atomic write %s: closing temp file: %w", path, err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("atomic write %s: renaming temp file: %w", path, err)
	}
	if err = SyncDir(filepath.Dir(path)); err != nil {
		// The rename already happened; the temp name no longer exists, so
		// the deferred Remove is a harmless no-op.
		return fmt.Errorf("atomic write %s: %w", path, err)
	}
	return nil
}

func randomSuffix() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating temp suffix: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// ErrLocked is returned (wrapped) by AcquireLock when another process holds the lock.
var ErrLocked = errors.New("fsutil: lock held by another process")
