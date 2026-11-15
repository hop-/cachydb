//go:build !(linux && (amd64 || arm64))

package db

import "errors"

// evictDir is only implemented on Linux (amd64, arm64); see
// scale_evict_linux_test.go.
func evictDir(string) error {
	return errors.New("page-cache eviction is not implemented on this platform")
}
