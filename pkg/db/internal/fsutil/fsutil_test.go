package fsutil

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWriteFileAtomic(t *testing.T) {
	tests := []struct {
		name     string
		existing []byte // nil = file does not exist beforehand
		data     []byte
	}{
		{name: "new file", data: []byte(`{"a":1}`)},
		{name: "overwrite existing", existing: []byte("old contents that are longer"), data: []byte("new")},
		{name: "empty data", existing: []byte("old"), data: []byte{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "file.json")
			if tt.existing != nil {
				if err := os.WriteFile(path, tt.existing, 0o644); err != nil {
					t.Fatalf("seeding file: %v", err)
				}
			}

			if err := WriteFileAtomic(path, tt.data, 0o644); err != nil {
				t.Fatalf("WriteFileAtomic: %v", err)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading back: %v", err)
			}
			if !bytes.Equal(got, tt.data) {
				t.Errorf("contents = %q, want %q", got, tt.data)
			}
			assertNoTempFiles(t, dir)
		})
	}
}

func TestWriteFileAtomicPerm(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits are not meaningful on windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "private")
	if err := WriteFileAtomic(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("perm = %o, want 600", got)
	}
}

func TestWriteFileAtomicErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, dir string) string // returns target path
	}{
		{
			name: "missing parent dir",
			setup: func(t *testing.T, dir string) string {
				return filepath.Join(dir, "no-such-dir", "file")
			},
		},
		{
			name: "target is a non-empty directory",
			setup: func(t *testing.T, dir string) string {
				target := filepath.Join(dir, "target")
				if err := os.MkdirAll(filepath.Join(target, "child"), 0o755); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				return target
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := tt.setup(t, dir)
			if err := WriteFileAtomic(path, []byte("data"), 0o644); err == nil {
				t.Fatal("WriteFileAtomic succeeded, want error")
			}
			assertNoTempFiles(t, dir)
		})
	}
}

func TestSyncDir(t *testing.T) {
	if err := SyncDir(t.TempDir()); err != nil {
		t.Errorf("SyncDir(existing) = %v, want nil", err)
	}
	if runtime.GOOS != "windows" {
		if err := SyncDir(filepath.Join(t.TempDir(), "missing")); err == nil {
			t.Error("SyncDir(missing) = nil, want error")
		}
	}
}

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.Contains(d.Name(), ".tmp-") {
			t.Errorf("leftover temp file: %s", p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
}
