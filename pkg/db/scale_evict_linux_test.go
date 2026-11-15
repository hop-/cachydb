//go:build linux && (amd64 || arm64)

package db

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// evictDir drops the clean page-cache pages of every file under root with
// posix_fadvise(POSIX_FADV_DONTNEED), which needs no root privileges. Pages
// that a process still maps stay cached, so close the engine first.
func evictDir(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		const posixFadvDontneed = 4
		if _, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, posixFadvDontneed, 0, 0); errno != 0 {
			return &os.PathError{Op: "fadvise", Path: path, Err: errno}
		}
		return nil
	})
}
