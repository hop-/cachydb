package db

import (
	"errors"
	"fmt"
)

// hookPoint names a point in the engine where a test may inject a
// simulated crash via Options.testHook.
type hookPoint string

// Crash-hook points. Each is called by the engine at the named point of a
// write, flush, compaction, or metadata operation.
const (
	hookAfterWALSync      hookPoint = "after-wal-sync-before-apply"       // document writes (task 09)
	hookAfterSegWrite     hookPoint = "after-segment-write-before-fsync"  // flush (task 10)
	hookAfterSegSync      hookPoint = "after-segment-fsync-before-keydir" // flush (task 10)
	hookCompactBeforeSwap hookPoint = "compaction-before-swap"            // compaction (task 12)
	hookMetaWrite         hookPoint = "during-meta-write"                 // metadata ops: after the WAL append, before the meta file change
)

// errSimulatedCrash is returned (possibly wrapped) by hook when the test
// hook asks for a crash.
var errSimulatedCrash = errors.New("simulated crash")

// hook returns an error satisfying errors.Is(err, errSimulatedCrash) when
// opts.testHook says so; callers must then return immediately WITHOUT
// cleanup, as if the process died at p. It is a no-op (nil) when testHook
// is nil, which is always the case outside tests.
func (e *Engine) hook(p hookPoint) error {
	if e.opts.testHook == nil {
		return nil
	}
	err := e.opts.testHook(p)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errSimulatedCrash):
		return err
	default:
		return fmt.Errorf("%w at %s: %w", errSimulatedCrash, p, err)
	}
}
