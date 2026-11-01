//go:build !(linux || darwin || freebsd || openbsd || netbsd || dragonfly)

package fsutil

import (
	"path/filepath"
	"testing"
)

func TestAcquireLockUnsupported(t *testing.T) {
	l, err := AcquireLock(filepath.Join(t.TempDir(), "cachydb.lock"))
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	if l.Supported() {
		t.Fatal("Supported() = true for the no-op lock")
	}
	if err := l.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}
