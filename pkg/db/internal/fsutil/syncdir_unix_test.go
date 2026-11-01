//go:build unix

package fsutil

import (
	"errors"
	"io/fs"
	"syscall"
	"testing"
)

func TestIgnoreUnsupportedSync(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantNil bool
	}{
		{name: "nil", err: nil, wantNil: true},
		{name: "EINVAL", err: &fs.PathError{Op: "sync", Path: "/d", Err: syscall.EINVAL}, wantNil: true},
		{name: "ENOTSUP", err: &fs.PathError{Op: "sync", Path: "/d", Err: syscall.ENOTSUP}, wantNil: true},
		{name: "EIO", err: &fs.PathError{Op: "sync", Path: "/d", Err: syscall.EIO}},
		{name: "other", err: errors.New("boom")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ignoreUnsupportedSync(tt.err)
			if (got == nil) != tt.wantNil {
				t.Fatalf("ignoreUnsupportedSync(%v) = %v, wantNil %v", tt.err, got, tt.wantNil)
			}
			if got != nil && !errors.Is(got, tt.err) {
				t.Fatalf("ignoreUnsupportedSync(%v) = %v, want the input error", tt.err, got)
			}
		})
	}
}
