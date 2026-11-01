package db

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{"simple", "users", true},
		{"single char", "a", true},
		{"single digit", "7", true},
		{"mixed case digits", "Users2024", true},
		{"underscore and dash", "my_db-1", true},
		{"named wal", "wal", true},
		{"64 chars", strings.Repeat("a", 64), true},
		{"empty", "", false},
		{"65 chars", strings.Repeat("a", 65), false},
		{"parent traversal", "../x", false},
		{"slash", "a/b", false},
		{"backslash", `a\b`, false},
		{"leading dot", ".x", false},
		{"dot inside", "LOCK.x", false},
		{"dot dot", "..", false},
		{"leading underscore", "_x", false},
		{"leading dash", "-x", false},
		{"space", "a b", false},
		{"nul byte", "a\x00b", false},
		{"non-ascii", "café", false},
		{"absolute path", "/etc", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateName("database", tt.input)
			if tt.valid {
				if err != nil {
					t.Errorf("ValidateName(%q) = %v, want nil", tt.input, err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidName) {
				t.Errorf("ValidateName(%q) = %v, want error wrapping ErrInvalidName", tt.input, err)
			}
		})
	}
}

func TestValidateNameMessageIncludesKind(t *testing.T) {
	for _, kind := range []string{"database", "collection", "index"} {
		t.Run(kind, func(t *testing.T) {
			err := ValidateName(kind, "../x")
			if err == nil || !strings.Contains(err.Error(), kind) {
				t.Errorf("ValidateName(%q, \"../x\") = %v, want message mentioning %q", kind, err, kind)
			}
		})
	}
}
