package db

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckDataDirNew(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) string // returns root
	}{
		{
			name:  "empty dir",
			setup: func(t *testing.T) string { return t.TempDir() },
		},
		{
			name:  "missing dir is created",
			setup: func(t *testing.T) string { return filepath.Join(t.TempDir(), "a", "b") },
		},
		{
			name: "only lock file and new-style WAL",
			setup: func(t *testing.T) string {
				root := t.TempDir()
				writeFile(t, filepath.Join(root, "cachydb.lock"), "")
				writeFile(t, filepath.Join(root, "wal-00000000000000000001.log"), "")
				return root
			},
		},
		{
			name: "database dir with new-style meta",
			setup: func(t *testing.T) string {
				root := t.TempDir()
				writeFile(t, filepath.Join(root, "shop", "db.meta"), "{}")
				return root
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := tt.setup(t)
			if err := checkDataDir(root); err != nil {
				t.Fatalf("checkDataDir: %v", err)
			}

			data, err := os.ReadFile(filepath.Join(root, "cachydb.format"))
			if err != nil {
				t.Fatalf("reading format file: %v", err)
			}
			var ff struct {
				Format  string `json:"format"`
				Version int    `json:"version"`
			}
			if err := json.Unmarshal(data, &ff); err != nil {
				t.Fatalf("decoding format file %q: %v", data, err)
			}
			if ff.Format != "cachydb" || ff.Version != 2 {
				t.Errorf("format file = %+v, want {cachydb 2}", ff)
			}

			// Reopening a directory with a valid format file succeeds.
			if err := checkDataDir(root); err != nil {
				t.Errorf("second checkDataDir: %v", err)
			}
		})
	}
}

func TestCheckDataDirOldLayout(t *testing.T) {
	tests := []struct {
		name string
		file string // relative path created inside root
	}{
		{"wal checkpoint", "wal.checkpoint"},
		{"old-style WAL segment", "wal-1730000000-000042.log"},
		{"legacy db meta", filepath.Join("shop", "db.meta.json")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, tt.file), "")

			err := checkDataDir(root)
			if !errors.Is(err, ErrIncompatibleDataDir) {
				t.Fatalf("checkDataDir = %v, want ErrIncompatibleDataDir", err)
			}
			wantMsg := "data directory " + root + " was created by an older cachydb format; move or delete it"
			if !strings.Contains(err.Error(), wantMsg) {
				t.Errorf("error = %q, want it to contain %q", err, wantMsg)
			}
			if _, statErr := os.Stat(filepath.Join(root, "cachydb.format")); !errors.Is(statErr, os.ErrNotExist) {
				t.Errorf("cachydb.format was written into an old-layout dir (stat err %v)", statErr)
			}
		})
	}
}

func TestCheckDataDirFormatFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{"current version", `{"format":"cachydb","version":2}`, false},
		{"higher version", `{"format":"cachydb","version":3}`, true},
		{"lower version", `{"format":"cachydb","version":1}`, true},
		{"wrong format name", `{"format":"other","version":2}`, true},
		{"garbage", `not json`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, "cachydb.format"), tt.content)

			err := checkDataDir(root)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("checkDataDir = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrIncompatibleDataDir) {
				t.Errorf("checkDataDir = %v, want ErrIncompatibleDataDir", err)
			}
		})
	}
}

func TestCheckDataDirFormatFileWinsOverLegacyFiles(t *testing.T) {
	// A valid cachydb.format means the directory is already in the new
	// format; legacy-looking names are not re-checked.
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "cachydb.format"), `{"format":"cachydb","version":2}`)
	writeFile(t, filepath.Join(root, "wal.checkpoint"), "")
	if err := checkDataDir(root); err != nil {
		t.Errorf("checkDataDir = %v, want nil", err)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
