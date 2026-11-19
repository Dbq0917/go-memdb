// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// writerLatency measures the wall time of N short write transactions while a
// background workload runs. Each write is an independent insert+commit so it
// must acquire and release the writer lock.
func writerLatency(t *testing.T, db *MemDB, n int) (max, total time.Duration) {
	t.Helper()
	for i := 0; i < n; i++ {
		obj := testObj()
		obj.ID = fmt.Sprintf("latency-%d", i)

		start := time.Now()
		txn := db.Txn(true)
		if err := txn.Insert("main", obj); err != nil {
			t.Fatalf("err: %v", err)
		}
		txn.Commit()
		elapsed := time.Since(start)

		if elapsed > max {
			max = elapsed
		}
		total += elapsed
	}
	return max, total
}

// TestLongReadTxn_DoesNotBlockWriters is the regression test for the reported
// stall: a long-lived transaction (the reporting service's snapshot) used to
// hold the writer lock for its whole life, so writers blocked until commit.
// With lazy locking the long txn only reads, so it never takes the lock and
// writer latency stays flat regardless of how long it stays open.
func TestLongReadTxn_DoesNotBlockWriters(t *testing.T) {
	db := testDB(t)

	// Seed some rows so the long transaction has something to read.
	seed := db.Txn(true)
	for i := 0; i < 25; i++ {
		obj := testObj()
		obj.ID = fmt.Sprintf("seed-%d", i)
		if err := seed.Insert("main", obj); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	seed.Commit()

	// Baseline: no long transaction open.
	const writes = 100
	baseMax, baseTotal := writerLatency(t, db, writes)

	// Open a long-lived write transaction and only read from it. This mirrors
	// a read-mostly service that keeps a snapshot-style transaction around.
	longTxn := db.Txn(true)
	defer longTxn.Abort()

	iter, err := longTxn.Get("main", "id_prefix", "seed-")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	seen := 0
	for raw := iter.Next(); raw != nil; raw = iter.Next() {
		seen++
	}
	if seen != 25 {
		t.Fatalf("expected 25 seeded rows, got %d", seen)
	}
	if longTxn.Mutating() {
		t.Fatalf("read-only usage must not take the writer lock")
	}
	if len(db.WriteTxns()) != 1 {
		t.Fatalf("long txn should still be introspectable")
	}

	longMax, longTotal := writerLatency(t, db, writes)

	t.Logf("writer latency  baseline: max=%s avg=%s", baseMax, baseTotal/writes)
	t.Logf("writer latency long-txn: max=%s avg=%s", longMax, longTotal/writes)

	// The long read txn adds no lock contention, so the worst case must stay
	// in the same order of magnitude as the baseline rather than blocking for
	// the transaction's lifetime. A generous multiplier absorbs scheduler
	// noise on shared CI runners.
	limit := 25 * time.Millisecond
	if longMax > limit {
		t.Fatalf("writer stalled for %s while a long read txn was open (limit %s)", longMax, limit)
	}
	if baseMax > limit {
		t.Fatalf("baseline unexpectedly slow: %s", baseMax)
	}
}

// TestMutatingLongTxn_BlocksWriters documents the other half of the contract:
// once the long transaction actually mutates, it legitimately holds the
// single-writer lock and other writers wait until it finishes. This is the
// contention that MaxLockWait makes observable and bounded.
func TestMutatingLongTxn_BlocksWriters(t *testing.T) {
	db := testDB(t)

	longTxn := db.Txn(true)
	if err := longTxn.Insert("main", testObj()); err != nil {
		t.Fatalf("err: %v", err)
	}

	stats := db.WriterLockStats()
	if !stats.Held || stats.CurrentHoldTime <= 0 {
		t.Fatalf("writer lock should be held, got %+v", stats)
	}

	done := make(chan struct{})
	go func() {
		other := db.Txn(true)
		obj := testObj()
		obj.ID = "concurrent-writer"
		_ = other.Insert("main", obj)
		other.Commit()
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("a mutating txn must serialize other writers")
	case <-time.After(20 * time.Millisecond):
	}

	longTxn.Commit()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("writer should proceed after commit")
	}

	stats = db.WriterLockStats()
	if stats.Held {
		t.Fatalf("lock should be free after commit")
	}
	if stats.ContendedWaits < 1 {
		t.Fatalf("expected at least one contended wait, got %+v", stats)
	}
}

func TestMaxLockWait_BoundsWriteStall(t *testing.T) {
	db, err := NewMemDBWithOptions(testValidSchema(), &Options{MaxLockWait: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	holder := db.Txn(true)
	if err := holder.Insert("main", testObj()); err != nil {
		t.Fatalf("err: %v", err)
	}
	defer holder.Abort()

	start := time.Now()
	blocked := db.Txn(true)
	err = blocked.Insert("main", testObj())
	waited := time.Since(start)

	if !errors.Is(err, ErrWriteLockTimeout) {
		t.Fatalf("expected ErrWriteLockTimeout, got %v", err)
	}
	if waited < 25*time.Millisecond || waited > time.Second {
		t.Fatalf("wait %s not bounded by MaxLockWait", waited)
	}
	if blocked.Mutating() {
		t.Fatalf("timed-out txn must not hold the lock")
	}

	stats := db.WriterLockStats()
	if stats.Timeouts != 1 {
		t.Fatalf("expected 1 timeout, got %+v", stats)
	}

	// The holder must still be able to commit, and a later writer succeeds.
	holder.Commit()
	next := db.Txn(true)
	if err := next.Insert("main", testObj()); err != nil {
		t.Fatalf("lock must remain usable after a timeout: %v", err)
	}
	next.Commit()
}

func TestTxnCtx_CanceledWhileWaiting(t *testing.T) {
	db := testDB(t)

	holder := db.Txn(true)
	if err := holder.Insert("main", testObj()); err != nil {
		t.Fatalf("err: %v", err)
	}
	defer holder.Abort()

	ctx, cancel := context.WithCancel(context.Background())
	blocked := db.TxnCtx(ctx, true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- blocked.Insert("main", testObj())
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrWriteLockTimeout) {
			t.Fatalf("expected ErrWriteLockTimeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("cancel did not unblock the waiting writer")
	}
}

func TestMaxTxnAge_RejectsStaleTxn(t *testing.T) {
	db, err := NewMemDBWithOptions(testValidSchema(), &Options{MaxTxnAge: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	stale := db.Txn(true)

	// Reading while stale is fine: no lock is involved.
	if _, err := stale.First("main", "id", "nope"); err != nil {
		t.Fatalf("reads should remain allowed: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	err = stale.Insert("main", testObj())
	if !errors.Is(err, ErrTxnTooOld) {
		t.Fatalf("expected ErrTxnTooOld, got %v", err)
	}

	// The stale txn aborted itself and must not hold or leak the lock.
	if len(db.WriteTxns()) != 0 {
		t.Fatalf("stale txn should be unregistered")
	}
	fresh := db.Txn(true)
	if err := fresh.Insert("main", testObj()); err != nil {
		t.Fatalf("fresh txn should acquire the lock: %v", err)
	}
	fresh.Commit()
}

func TestNonMutatingWriteTxn_DeferStillRuns(t *testing.T) {
	db := testDB(t)

	ran := false
	txn := db.Txn(true)
	txn.Defer(func() { ran = true })

	// Reading does not take the lock.
	if _, err := txn.First("main", "id", "missing"); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	if !ran {
		t.Fatalf("deferred functions must run even without a mutation")
	}
	if len(db.WriteTxns()) != 0 {
		t.Fatalf("committed txn should be unregistered")
	}
	if db.WriterLockStats().Held {
		t.Fatalf("non-mutating txn must not leave the lock held")
	}
}

func TestTxnCtx_ReadOnlyUnaffectedByContext(t *testing.T) {
	db := testDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Already canceled.

	// Read transactions never acquire the writer lock, so a canceled context
	// must not interfere with reads.
	txn := db.TxnCtx(ctx, false)
	if _, err := txn.First("main", "id", "anything"); err != nil {
		t.Fatalf("read txn must ignore the context: %v", err)
	}
	txn.Abort()

	// A write txn that only reads is likewise unaffected.
	writeTxn := db.TxnCtx(ctx, true)
	if _, err := writeTxn.First("main", "id", "anything"); err != nil {
		t.Fatalf("non-mutating write txn must ignore the context: %v", err)
	}
	writeTxn.Abort()
}

func TestConcurrentWriters_SerializedAndIsolated(t *testing.T) {
	db := testDB(t)

	const n = 20
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(2)

	writer := func(prefix string) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			obj := testObj()
			obj.ID = fmt.Sprintf("%s-%d", prefix, i)
			txn := db.Txn(true)
			if err := txn.Insert("main", obj); err != nil {
				errCh <- err
				txn.Abort()
				return
			}
			txn.Commit()
		}
	}
	go writer("serial")
	go writer("parallel")

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("concurrent writer failed: %v", err)
	default:
	}

	readTxn := db.Txn(false)
	defer readTxn.Abort()
	for i := 0; i < n; i++ {
		for _, prefix := range []string{"serial", "parallel"} {
			raw, err := readTxn.First("main", "id", fmt.Sprintf("%s-%d", prefix, i))
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if raw == nil {
				t.Fatalf("missing %s-%d", prefix, i)
			}
		}
	}
}
