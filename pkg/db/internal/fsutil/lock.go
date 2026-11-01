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
}
