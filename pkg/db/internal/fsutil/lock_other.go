//go:build !(linux || darwin || freebsd || openbsd || netbsd || dragonfly)

package fsutil

// AcquireLock is the no-op lock of platforms without syscall.Flock
// (Windows, Solaris, illumos, AIX, and others). It creates nothing and
// does not protect against a second process opening the same directory;
// the returned Lock reports Supported() == false so the caller can warn.
func AcquireLock(path string) (*Lock, error) {
	return &Lock{path: path, unsupported: true}, nil
}

// Release is a no-op for the unsupported lock.
func (l *Lock) Release() error {
	return nil
}
