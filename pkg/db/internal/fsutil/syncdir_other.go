//go:build !unix

package fsutil

// SyncDir fsyncs a directory (no-op error-free on platforms without dir fsync).
// On non-unix platforms directory fsync is not available, so it returns nil.
func SyncDir(dir string) error {
	return nil
}
