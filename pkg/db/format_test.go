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
			upgrade, err := checkDataDir(root)
			if err != nil {
				t.Fatalf("checkDataDir: %v", err)
			}
			if upgrade {
				t.Error("checkDataDir asks to upgrade a new directory")
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
			if ff.Format != "cachydb" || ff.Version != 3 {
				t.Errorf("format file = %+v, want {cachydb 3}", ff)
			}

			// Reopening a directory with a valid format file succeeds.
			if upgrade, err := checkDataDir(root); err != nil || upgrade {
				t.Errorf("second checkDataDir = %v, %v; want no upgrade, nil", upgrade, err)
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

			_, err := checkDataDir(root)
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
		name        string
		content     string
		wantErr     bool
		wantUpgrade bool
	}{
		{"current version", `{"format":"cachydb","version":3}`, false, false},
		{"version 2 is upgraded", `{"format":"cachydb","version":2}`, false, true},
		{"higher version", `{"format":"cachydb","version":4}`, true, false},
		{"lower version", `{"format":"cachydb","version":1}`, true, false},
		{"version 0", `{"format":"cachydb","version":0}`, true, false},
		{"wrong format name", `{"format":"other","version":3}`, true, false},
		{"garbage", `not json`, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "cachydb.format")
			writeFile(t, path, tt.content)

			upgrade, err := checkDataDir(root)
			if got, rerr := os.ReadFile(path); rerr != nil || string(got) != tt.content {
				t.Errorf("checkDataDir changed the format file to %q (%v)", got, rerr)
			}
			if !tt.wantErr {
				if err != nil {
					t.Errorf("checkDataDir = %v, want nil", err)
				}
				if upgrade != tt.wantUpgrade {
					t.Errorf("upgrade = %v, want %v", upgrade, tt.wantUpgrade)
				}
				return
			}
			if !errors.Is(err, ErrIncompatibleDataDir) {
				t.Errorf("checkDataDir = %v, want ErrIncompatibleDataDir", err)
			}
			if upgrade {
				t.Error("upgrade = true together with an error")
			}
		})
	}
}

func TestCheckDataDirFormatFileWinsOverLegacyFiles(t *testing.T) {
	// A valid cachydb.format means the directory is already in the new
	// format; legacy-looking names are not re-checked.
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "cachydb.format"), `{"format":"cachydb","version":3}`)
	writeFile(t, filepath.Join(root, "wal.checkpoint"), "")
	if upgrade, err := checkDataDir(root); err != nil || upgrade {
		t.Errorf("checkDataDir = %v, %v; want no upgrade, nil", upgrade, err)
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
