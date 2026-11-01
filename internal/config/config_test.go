package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInitEngineSettings(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		wantAge  time.Duration
		wantWAL  ByteSize
		wantCach ByteSize
		wantErr  bool
	}{
		{
			name:     "defaults",
			wantAge:  24 * time.Hour,
			wantWAL:  GiB,
			wantCach: 64 * MiB,
		},
		{
			name:     "from env",
			env:      map[string]string{"WAL_RETENTION_AGE": "90m", "WAL_RETENTION_SIZE": "256MiB", "CACHE_SIZE": "1GiB"},
			wantAge:  90 * time.Minute,
			wantWAL:  256 * MiB,
			wantCach: GiB,
		},
		{
			name:     "zero disables",
			env:      map[string]string{"WAL_RETENTION_AGE": "0s", "WAL_RETENTION_SIZE": "0", "CACHE_SIZE": "0"},
			wantAge:  0,
			wantWAL:  0,
			wantCach: 0,
		},
		{name: "invalid age", env: map[string]string{"WAL_RETENTION_AGE": "soon"}, wantErr: true},
		{name: "invalid size", env: map[string]string{"WAL_RETENTION_SIZE": "big"}, wantErr: true},
		{name: "invalid cache size", env: map[string]string{"CACHE_SIZE": "-1MiB"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"WAL_RETENTION_AGE", "WAL_RETENTION_SIZE", "CACHE_SIZE"} {
				t.Setenv(k, tt.env[k])
			}
			// t.Setenv with "" still defines the variable; envconfig then
			// uses the empty value, so unset the ones the case omits.
			for _, k := range []string{"WAL_RETENTION_AGE", "WAL_RETENTION_SIZE", "CACHE_SIZE"} {
				if _, ok := tt.env[k]; !ok {
					unsetenv(t, k)
				}
			}

			err := Init()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Init() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Init(): %v", err)
			}
			c := GetConfig()
			if c.WALRetentionAge != tt.wantAge {
				t.Errorf("WALRetentionAge = %v, want %v", c.WALRetentionAge, tt.wantAge)
			}
			if c.WALRetentionSize != tt.wantWAL {
				t.Errorf("WALRetentionSize = %v, want %v", c.WALRetentionSize, tt.wantWAL)
			}
			if c.CacheSize != tt.wantCach {
				t.Errorf("CacheSize = %v, want %v", c.CacheSize, tt.wantCach)
			}
		})
	}
}

// unsetenv removes key for the rest of the test; t.Setenv must have been
// called for key first so the original value is restored on cleanup.
func unsetenv(t *testing.T, key string) {
	t.Helper()
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unsetenv %s: %v", key, err)
	}
}

func TestInitServerSettings(t *testing.T) {
	keys := []string{"PORT", "ROOT_DIR", "DB_NAME", "TRANSPORT"}
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	defaultRoot := filepath.Join(home, ".cachydb")
	if windowsRootDirName != "" {
		defaultRoot = filepath.Join(home, windowsRootDirName)
	}

	tests := []struct {
		name      string
		env       map[string]string
		wantPort  int
		wantRoot  string
		wantDB    string
		wantTrans string
		wantErr   bool
	}{
		{name: "defaults", wantPort: 7601, wantRoot: defaultRoot, wantDB: "main", wantTrans: "stdio"},
		{
			name:     "from env",
			env:      map[string]string{"PORT": "8123", "ROOT_DIR": "/srv/cachydb", "DB_NAME": "app", "TRANSPORT": "http"},
			wantPort: 8123, wantRoot: "/srv/cachydb", wantDB: "app", wantTrans: "http",
		},
		{
			name:     "empty values mean unset",
			env:      map[string]string{"ROOT_DIR": "", "DB_NAME": "", "TRANSPORT": ""},
			wantPort: 7601, wantRoot: defaultRoot, wantDB: "main", wantTrans: "stdio",
		},
		{name: "invalid port", env: map[string]string{"PORT": "http"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range keys {
				t.Setenv(k, tt.env[k])
				if _, ok := tt.env[k]; !ok {
					unsetenv(t, k)
				}
			}
			t.Cleanup(func() {
				// Reload the real environment for later tests.
				if err := Init(); err != nil {
					t.Logf("reloading config: %v", err)
				}
			})

			err := Init()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Init() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Init(): %v", err)
			}
			c := GetConfig()
			if c.Port != tt.wantPort {
				t.Errorf("Port = %d, want %d", c.Port, tt.wantPort)
			}
			if c.RootDir != tt.wantRoot {
				t.Errorf("RootDir = %q, want %q", c.RootDir, tt.wantRoot)
			}
			if c.DBName != tt.wantDB {
				t.Errorf("DBName = %q, want %q", c.DBName, tt.wantDB)
			}
			if c.Transport != tt.wantTrans {
				t.Errorf("Transport = %q, want %q", c.Transport, tt.wantTrans)
			}
		})
	}
}
