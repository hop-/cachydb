// Package compat holds helpers for the compatibility fixture tests: the
// data directories written by older releases under
// pkg/db/testdata/compat and the generator in test/compat/gen.
package compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
)

// CompareExport compares two exports in the JSON Lines export format
// line by line as JSON values, ignoring the header's "created" and
// "cachydb" fields (when and by which version the export was written). It
// returns an error describing the first difference, or nil.
func CompareExport(want, got []byte) error {
	wl, err := parseLines(want)
	if err != nil {
		return fmt.Errorf("expected export: %w", err)
	}
	gl, err := parseLines(got)
	if err != nil {
		return fmt.Errorf("actual export: %w", err)
	}
	for i := 0; i < len(wl) || i < len(gl); i++ {
		switch {
		case i >= len(gl):
			return fmt.Errorf("line %d missing: want %s", i+1, compact(wl[i]))
		case i >= len(wl):
			return fmt.Errorf("line %d unexpected: %s", i+1, compact(gl[i]))
		case !reflect.DeepEqual(wl[i], gl[i]):
			return fmt.Errorf("line %d differs:\n got %s\nwant %s", i+1, compact(gl[i]), compact(wl[i]))
		}
	}
	return nil
}

func parseLines(data []byte) ([]map[string]any, error) {
	var out []map[string]any
	for i, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var v map[string]any
		if err := json.Unmarshal(line, &v); err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		if i == 0 {
			delete(v, "created")
			delete(v, "cachydb")
		}
		out = append(out, v)
	}
	return out, nil
}

func compact(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(data)
}

// CopyDir copies the regular files and directories under src to dst,
// which must not exist.
func CopyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("copying %s: not a regular file", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}
