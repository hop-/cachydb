//go:build unix

package fsutil

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAcquireLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cachydb.lock")

	first, err := AcquireLock(path)
	if err != nil {
		t.Fatalf("first AcquireLock: %v", err)
	}

	// A second holder (separate open file description, same process) must
	// be refused while the first holds the lock.
	second, err := AcquireLock(path)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second AcquireLock error = %v, want ErrLocked", err)
	}
	if second != nil {
		t.Fatalf("second AcquireLock returned a lock alongside error")
	}

	if err := first.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Errorf("second Release = %v, want nil (idempotent)", err)
	}

	third, err := AcquireLock(path)
	if err != nil {
		t.Fatalf("AcquireLock after Release: %v", err)
	}
	if err := third.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestAcquireLockErrors(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "missing parent dir", path: filepath.Join(t.TempDir(), "missing", "cachydb.lock")},
		{name: "path is a directory", path: t.TempDir()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := AcquireLock(tt.path)
			if err == nil {
				_ = l.Release()
				t.Fatal("AcquireLock succeeded, want error")
			}
			if errors.Is(err, ErrLocked) {
				t.Errorf("error = %v, must not be ErrLocked", err)
			}
		})
	}
}
