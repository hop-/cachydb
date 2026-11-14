package config

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
		{
			name:     "empty values mean unset",
			env:      map[string]string{"WAL_RETENTION_AGE": "", "WAL_RETENTION_SIZE": "", "CACHE_SIZE": ""},
			wantAge:  24 * time.Hour,
			wantWAL:  GiB,
			wantCach: 64 * MiB,
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
			// t.Setenv with "" defines the variable as empty. Unset the ones
			// the case omits, so "defaults" runs without the variables and
			// only "empty values mean unset" runs with empty ones.
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
		{
			name:     "empty port means unset",
			env:      map[string]string{"PORT": "", "DB_NAME": "app"},
			wantPort: 7601, wantRoot: defaultRoot, wantDB: "app", wantTrans: "stdio",
		},
		{name: "invalid port", env: map[string]string{"PORT": "http"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Registered before the t.Setenv calls, so it runs after they
			// have restored the environment.
			t.Cleanup(func() {
				// Reload the real environment for later tests.
				if err := Init(); err != nil {
					t.Logf("reloading config: %v", err)
				}
			})
			for _, k := range keys {
				t.Setenv(k, tt.env[k])
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

// envKeys are the environment variables Init reads.
var envKeys = []string{"PORT", "ROOT_DIR", "DB_NAME", "TRANSPORT", "WAL_RETENTION_AGE", "WAL_RETENTION_SIZE", "CACHE_SIZE"}

// setEnv gives the test exactly env out of envKeys (the others are unset)
// and reloads the configuration from the real environment afterwards.
func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	// Registered before the t.Setenv calls, so it runs after they have
	// restored the environment.
	t.Cleanup(func() {
		if err := Init(); err != nil {
			t.Logf("reloading config: %v", err)
		}
	})
	for _, k := range envKeys {
		t.Setenv(k, env[k])
		if _, ok := env[k]; !ok {
			unsetenv(t, k)
		}
	}
}

// TestInitMalformedValues checks that a malformed value is an error that
// names its variable (and only the malformed ones), that it keeps its
// default, and that every other setting is still applied.
func TestInitMalformedValues(t *testing.T) {
	type settings struct {
		port  int
		age   time.Duration
		wal   ByteSize
		cache ByteSize
	}
	tests := []struct {
		name      string
		env       map[string]string
		wantNames []string
		wantIs    error
		want      settings
	}{
		{
			name:      "port",
			env:       map[string]string{"PORT": "http", "WAL_RETENTION_AGE": "90m", "WAL_RETENTION_SIZE": "256MiB", "CACHE_SIZE": "1GiB"},
			wantNames: []string{"PORT"},
			wantIs:    strconv.ErrSyntax,
			want:      settings{port: 7601, age: 90 * time.Minute, wal: 256 * MiB, cache: GiB},
		},
		{
			name:      "port is not a whole number",
			env:       map[string]string{"PORT": "76.5"},
			wantNames: []string{"PORT"},
			want:      settings{port: 7601, age: 24 * time.Hour, wal: GiB, cache: 64 * MiB},
		},
		{
			name:      "port out of range for an int",
			env:       map[string]string{"PORT": "99999999999999999999"},
			wantNames: []string{"PORT"},
			wantIs:    strconv.ErrRange,
			want:      settings{port: 7601, age: 24 * time.Hour, wal: GiB, cache: 64 * MiB},
		},
		{
			name:      "age",
			env:       map[string]string{"PORT": "8123", "WAL_RETENTION_AGE": "soon", "CACHE_SIZE": "0"},
			wantNames: []string{"WAL_RETENTION_AGE"},
			want:      settings{port: 8123, age: 24 * time.Hour, wal: GiB, cache: 0},
		},
		{
			name:      "age without a unit",
			env:       map[string]string{"WAL_RETENTION_AGE": "90"},
			wantNames: []string{"WAL_RETENTION_AGE"},
			want:      settings{port: 7601, age: 24 * time.Hour, wal: GiB, cache: 64 * MiB},
		},
		{
			name:      "wal size",
			env:       map[string]string{"PORT": "8123", "WAL_RETENTION_SIZE": "big", "CACHE_SIZE": "1GiB"},
			wantNames: []string{"WAL_RETENTION_SIZE"},
			want:      settings{port: 8123, age: 24 * time.Hour, wal: GiB, cache: GiB},
		},
		{
			name:      "cache size",
			env:       map[string]string{"WAL_RETENTION_AGE": "90m", "CACHE_SIZE": "-1MiB"},
			wantNames: []string{"CACHE_SIZE"},
			want:      settings{port: 7601, age: 90 * time.Minute, wal: GiB, cache: 64 * MiB},
		},
		{
			name:      "two variables",
			env:       map[string]string{"PORT": "http", "WAL_RETENTION_AGE": "90m", "CACHE_SIZE": "lots"},
			wantNames: []string{"PORT", "CACHE_SIZE"},
			want:      settings{port: 7601, age: 90 * time.Minute, wal: GiB, cache: 64 * MiB},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"ROOT_DIR": "/srv/cachydb", "DB_NAME": "app", "TRANSPORT": "http"}
			maps.Copy(env, tt.env)
			setEnv(t, env)

			err := Init()
			if err == nil {
				t.Fatal("Init() succeeded, want error")
			}
			msg := err.Error()
			if !strings.HasPrefix(msg, "loading configuration from the environment: ") {
				t.Errorf("error %q lacks the documented prefix", msg)
			}
			for _, k := range envKeys {
				named := strings.Contains(msg, k+": ")
				if want := slices.Contains(tt.wantNames, k); named != want {
					t.Errorf("error %q names %s = %v, want %v", msg, k, named, want)
				}
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Errorf("error %v does not wrap %v", err, tt.wantIs)
			}

			c := GetConfig()
			if got := (settings{c.Port, c.WALRetentionAge, c.WALRetentionSize, c.CacheSize}); got != tt.want {
				t.Errorf("settings after the error = %+v, want %+v", got, tt.want)
			}
			if c.RootDir != "/srv/cachydb" || c.DBName != "app" || c.Transport != "http" {
				t.Errorf("RootDir, DBName, Transport = %q, %q, %q; want /srv/cachydb, app, http", c.RootDir, c.DBName, c.Transport)
			}
		})
	}
}

// TestInitStartsFromDefaults checks that a second Init does not keep a
// value of the first one whose variable is gone.
func TestInitStartsFromDefaults(t *testing.T) {
	setEnv(t, map[string]string{"PORT": "8123", "DB_NAME": "app", "CACHE_SIZE": "1GiB"})
	if err := Init(); err != nil {
		t.Fatalf("Init(): %v", err)
	}
	if c := GetConfig(); c.Port != 8123 || c.DBName != "app" || c.CacheSize != GiB {
		t.Fatalf("Port, DBName, CacheSize = %d, %q, %v; want 8123, app, 1GiB", c.Port, c.DBName, c.CacheSize)
	}

	for _, k := range []string{"PORT", "DB_NAME", "CACHE_SIZE"} {
		unsetenv(t, k)
	}
	if err := Init(); err != nil {
		t.Fatalf("second Init(): %v", err)
	}
	if c := GetConfig(); c.Port != 7601 || c.DBName != "main" || c.CacheSize != 64*MiB {
		t.Errorf("Port, DBName, CacheSize = %d, %q, %v; want the defaults 7601, main, 64MiB", c.Port, c.DBName, c.CacheSize)
	}
}

// TestInitReadsOnlyDocumentedVariables checks that a variable named after
// a Config field without a documented variable is not read. (ROOTDIRNAME
// used to change the default data directory by accident.)
func TestInitReadsOnlyDocumentedVariables(t *testing.T) {
	setEnv(t, nil)
	t.Setenv("ROOTDIRNAME", "elsewhere")

	if err := Init(); err != nil {
		t.Fatalf("Init(): %v", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	name := ".cachydb"
	if windowsRootDirName != "" {
		name = windowsRootDirName
	}
	c := GetConfig()
	if want := filepath.Join(home, name); c.RootDir != want {
		t.Errorf("RootDir = %q, want %q", c.RootDir, want)
	}
	if c.RootDirName != name {
		t.Errorf("RootDirName = %q, want %q", c.RootDirName, name)
	}
}
