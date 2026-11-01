package config

import (
	"os"
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
