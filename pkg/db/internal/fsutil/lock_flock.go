//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package fsutil

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// AcquireLock takes an exclusive, non-blocking flock on path (creating it).
// If another holder (another process, or another open file description in
// this process) has the lock, it returns an error wrapping ErrLocked.
func AcquireLock(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("acquiring lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close() // already failing; the flock error is reported
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("acquiring lock %s: %w", path, ErrLocked)
		}
		return nil, fmt.Errorf("acquiring lock %s: %w", path, err)
	}
	return &Lock{path: path, f: f}, nil
}

// Release unlocks and closes the lock file. It is safe to call more than
// once; calls after the first return nil.
func (l *Lock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	f := l.f
	l.f = nil
	unlockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close() // closing also drops the flock
	if unlockErr != nil {
		return fmt.Errorf("releasing lock %s: %w", l.path, unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("releasing lock %s: closing: %w", l.path, closeErr)
	}
	return nil
}
