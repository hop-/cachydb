package db

import (
	"sync/atomic"
)

// Crash-test helpers (interfaces.md #crash-hooks).

// crashAt arms opts so that an engine opened with them simulates a crash
// the first time it reaches hook point p: that hook call returns
// errSimulatedCrash, and so does every later hook call at any point, as a
// dead process does nothing more. The operation (or flush) that hit the
// hook returns at once without cleanup. Tests then call abandon and
// reopen the directory with opts.testHook cleared.
func crashAt(opts *Options, p hookPoint) {
	var dead atomic.Bool
	opts.testHook = func(at hookPoint) error {
		if at == p {
			dead.Store(true)
		}
		if dead.Load() {
			return errSimulatedCrash
		}
		return nil
	}
}

// abandon stops e the way a dead process would leave it: no final flush
// and no index snapshots. It waits for in-flight operations, stops the
// background goroutines, closes the segment files and the WAL (whose
// records are already durable), and releases the directory lock so the
// same directory can be reopened. A later Close is a no-op.
func (e *Engine) abandon() {
	e.closeOnce.Do(func() {
		e.gateMu.Lock()
		e.closed = true
		e.gateMu.Unlock()
		e.ops.Wait()
		e.bgCancel()
		e.bg.Wait()

		e.mu.RLock()
		for _, d := range e.databases {
			d.mu.RLock()
			for _, c := range d.collections {
				_ = c.closeFiles() // read-only or already durable: nothing to lose
			}
			d.mu.RUnlock()
		}
		e.mu.RUnlock()
		_ = e.wal.Close() // a failed WAL (tests) has nothing left to write
		_ = e.lock.Release()
	})
}
