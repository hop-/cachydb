package fsutil

import (
	"os"
	"sync"
)

// Lock is an exclusive lock on a file, obtained with AcquireLock and freed
// with Release.
type Lock struct {
	mu   sync.Mutex
	path string
	f    *os.File // nil for the no-op lock and after Release

	// unsupported marks the no-op lock of a platform without flock.
	unsupported bool
}

// Supported reports whether l really excludes other processes. It is
// false on platforms without flock (see lock_other.go), where the caller
// should warn that two processes could open the same directory.
func (l *Lock) Supported() bool {
	return !l.unsupported
}
