//go:build !unix

package fsutil

// AcquireLock takes an exclusive, non-blocking flock on path (creating it).
//
// On non-unix platforms (Windows) data-directory locking is unsupported:
// this returns a no-op Lock and does not protect against a second process
// opening the same directory.
func AcquireLock(path string) (*Lock, error) {
	return &Lock{path: path}, nil
}

// Release is a no-op on non-unix platforms.
func (l *Lock) Release() error {
	return nil
}
