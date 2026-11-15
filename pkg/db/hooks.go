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
	hookAfterWALSync        hookPoint = "after-wal-sync-before-apply"       // document writes (task 09)
	hookAfterSegWrite       hookPoint = "after-segment-write-before-fsync"  // flush (task 10)
	hookAfterSegSync        hookPoint = "after-segment-fsync-before-keydir" // flush (task 10): before the index transaction
	hookAfterIndexCommit    hookPoint = "after-index-commit-before-apply"   // flush: after the index transaction committed, before the overlay is cleared
	hookCompactBeforeSwap   hookPoint = "compaction-before-swap"            // compaction (task 12)
	hookMetaWrite           hookPoint = "during-meta-write"                 // metadata ops: after the WAL append, before the meta file change
	hookRenameDir           hookPoint = "rename-after-dir-before-meta"      // rename_collection: after the directory rename, before coll.meta (plan 004)
	hookUpgradeCollection   hookPoint = "upgrade-after-collection"          // format upgrade 2 -> 3 (Open): after each collection's index file was built, before the next collection (or the removal of the format-2 files)
	hookUpgradeBeforeFormat hookPoint = "upgrade-before-format-write"       // format upgrade 2 -> 3 (Open): after the format-2 files were removed, before cachydb.format is rewritten
)

// Index-commit sites: the index transactions Options.testIndexCommit can
// fail without a crash.
const (
	indexCommitFlush   = "flush"   // the index transaction of a flush
	indexCommitCompact = "compact" // the swap transaction of a compaction
)

// indexCommitHook returns the error opts.testIndexCommit injects at site:
// with committed false it is called inside the index transaction just
// before the commit, with committed true just after the transaction
// committed. It is a no-op (nil) when testIndexCommit is nil, which is
// always the case outside tests.
func (e *Engine) indexCommitHook(site string, committed bool) error {
	if e.opts.testIndexCommit == nil {
		return nil
	}
	return e.opts.testIndexCommit(site, committed)
}

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
