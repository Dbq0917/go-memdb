// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var (
	// ErrWriteLockTimeout is returned by a write transaction when it cannot
	// acquire the single-writer lock before the configured MaxLockWait
	// deadline, or because the context passed to TxnCtx was canceled.
	ErrWriteLockTimeout = errors.New("memdb: timed out waiting for the writer lock")

	// ErrTxnTooOld is returned when a write transaction attempts its first
	// mutation after the configured MaxTxnAge has elapsed. Such transactions
	// are aborted automatically and must be retried.
	ErrTxnTooOld = errors.New("memdb: write transaction is older than MaxTxnAge")
)

// writerLock serializes write transactions. It is implemented as a buffered
// channel (capacity 1) rather than a sync.Mutex so that acquisition can be
// abandoned via context (see MaxLockWait) without busy-looping on TryLock.
//
// The lock is acquired lazily, on the first mutation of a write transaction,
// and released when that transaction commits or aborts. A transaction that
// only reads never holds the lock and therefore never blocks other writers.
type writerLock struct {
	sem chan struct{}

	// Metrics, updated with atomic operations.
	contended atomic.Int64 // acquires that found the lock already held
	timeouts  atomic.Int64 // acquires that gave up (timeout / context cancel)
	waitNs    atomic.Int64 // cumulative time spent blocked, nanoseconds
}

func newWriterLock() *writerLock {
	return &writerLock{sem: make(chan struct{}, 1)}
}

// tryAcquire takes the lock without blocking. It reports whether the lock was
// acquired.
func (l *writerLock) tryAcquire() bool {
	select {
	case l.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// acquire blocks until the lock is held or ctx is canceled. A nil ctx waits
// indefinitely, matching the legacy sync.Mutex behavior.
func (l *writerLock) acquire(ctx context.Context) error {
	// Fast path: the common uncontended case adds no timer/select overhead.
	if l.tryAcquire() {
		return nil
	}

	start := time.Now()
	l.contended.Add(1)

	if ctx == nil {
		l.sem <- struct{}{}
	} else {
		select {
		case l.sem <- struct{}{}:
		case <-ctx.Done():
			l.timeouts.Add(1)
			l.waitNs.Add(time.Since(start).Nanoseconds())
			return ErrWriteLockTimeout
		}
	}

	l.waitNs.Add(time.Since(start).Nanoseconds())
	return nil
}

// release hands the lock back. It must only be called by the holder.
func (l *writerLock) release() {
	<-l.sem
}

// WriterLockStats is a point-in-time snapshot of writer-lock contention
// metrics. The zero value is valid.
type WriterLockStats struct {
	// Held reports whether a transaction currently holds the writer lock.
	Held bool

	// ContendedWaits counts acquire attempts that found the lock held and
	// had to block before succeeding.
	ContendedWaits int64

	// Timeouts counts acquire attempts that abandoned the wait because of
	// MaxLockWait or a canceled TxnCtx context.
	Timeouts int64

	// TotalWaitTime is the cumulative time transactions spent blocked on
	// the writer lock.
	TotalWaitTime time.Duration

	// CurrentHoldTime is how long the current holder has held the lock. It is
	// zero when Held is false.
	CurrentHoldTime time.Duration
}

// stats reads the contention counters and, when held, the current holder's
// lock hold time.
func (l *writerLock) stats(holdTime time.Duration) WriterLockStats {
	return WriterLockStats{
		Held:            holdTime > 0,
		ContendedWaits:  l.contended.Load(),
		Timeouts:        l.timeouts.Load(),
		TotalWaitTime:   time.Duration(l.waitNs.Load()),
		CurrentHoldTime: holdTime,
	}
}
