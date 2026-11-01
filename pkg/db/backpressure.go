package db

import (
	"context"
	"sync"
	"sync/atomic"
)

// Write backpressure (plan 002, C2).
//
// The engine counts the bytes of every unflushed document change: the
// memtables and the flushing tables of all collections. A document write
// waits before its WAL append while that total is at or above
// Options.MaxPendingBytes, so memory stays bounded when the disk is slower
// than the writers. Each completed flush (and each drop) releases bytes and
// wakes the waiters. The wait happens before any lock is taken, so the
// lock order is unchanged.

// pendingBytes tracks the unflushed bytes and the writers waiting on them.
type pendingBytes struct {
	n atomic.Int64

	mu   sync.Mutex
	wake chan struct{} // closed and replaced by release
}

func newPendingBytes() *pendingBytes {
	return &pendingBytes{wake: make(chan struct{})}
}

// add changes the count by delta (positive for new memtable bytes).
func (p *pendingBytes) add(delta int64) { p.n.Add(delta) }

// load returns the current count.
func (p *pendingBytes) load() int64 { return p.n.Load() }

// release subtracts n and wakes every waiter.
func (p *pendingBytes) release(n int64) {
	p.n.Add(-n)
	p.broadcast()
}

// broadcast wakes every waiter without changing the count.
func (p *pendingBytes) broadcast() {
	p.mu.Lock()
	close(p.wake)
	p.wake = make(chan struct{})
	p.mu.Unlock()
}

func (p *pendingBytes) waitChan() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wake
}

// overLimit reports whether writes must wait.
func (e *Engine) overLimit() bool {
	limit := e.opts.MaxPendingBytes
	return limit > 0 && e.pending.load() >= limit
}

// waitPending blocks while the unflushed bytes are at or above
// MaxPendingBytes. It asks the flusher to flush, and returns ctx.Err() if
// ctx ends first, or ErrClosed if the engine is closing. Callers hold no
// engine lock.
func (e *Engine) waitPending(ctx context.Context) error {
	for e.overLimit() {
		// Take the wake channel before re-checking, so a release between
		// the check and the select is not missed.
		ch := e.pending.waitChan()
		if !e.overLimit() {
			return nil
		}
		if e.isClosed() {
			return ErrClosed
		}
		e.kickFlush()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// releasePendingLocked returns the collection's unflushed bytes to the
// engine (the collection is being dropped) and wakes waiting writers.
// Callers hold c.mu exclusively.
func (c *Collection) releasePendingLocked() {
	if n := c.memBytes + c.flushingBytes; n != 0 {
		c.db.e.pending.release(n)
	}
	c.memBytes, c.flushingBytes = 0, 0
}
