// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWriterLock_AcquireRelease(t *testing.T) {
	lock := newWriterLock()

	if !lock.tryAcquire() {
		t.Fatalf("first acquire should succeed")
	}
	if lock.tryAcquire() {
		t.Fatalf("second acquire while held should fail")
	}

	acquired := make(chan struct{})
	go func() {
		if err := lock.acquire(nil); err != nil {
			t.Errorf("unexpected err: %v", err)
			return
		}
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatalf("acquire should block while the lock is held")
	case <-time.After(20 * time.Millisecond):
	}

	lock.release()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatalf("acquire should succeed after release")
	}
}

func TestWriterLock_ContextCancel(t *testing.T) {
	lock := newWriterLock()
	if err := lock.acquire(nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := lock.acquire(ctx)
	if !errors.Is(err, ErrWriteLockTimeout) {
		t.Fatalf("expected ErrWriteLockTimeout, got %v", err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatalf("acquire should have waited for the timeout")
	}

	stats := lock.stats(0)
	if stats.Timeouts != 1 {
		t.Fatalf("expected 1 timeout, got %d", stats.Timeouts)
	}
	if stats.ContendedWaits != 1 {
		t.Fatalf("expected 1 contended wait, got %d", stats.ContendedWaits)
	}

	// The lock must still be owned by the original holder.
	if lock.tryAcquire() {
		t.Fatalf("timed-out acquire must not leak the lock")
	}
	lock.release()
	if !lock.tryAcquire() {
		t.Fatalf("lock should be available after release")
	}
}

func TestWriterLock_Stats(t *testing.T) {
	lock := newWriterLock()

	held := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = lock.acquire(nil)
		close(held)
		<-release
		lock.release()
	}()
	<-held

	waiterDone := make(chan struct{})
	go func() {
		_ = lock.acquire(nil)
		lock.release()
		close(waiterDone)
	}()

	time.Sleep(20 * time.Millisecond)
	release <- struct{}{}
	<-waiterDone

	stats := lock.stats(0)
	if stats.ContendedWaits != 1 {
		t.Fatalf("expected 1 contended wait, got %d", stats.ContendedWaits)
	}
	if stats.Timeouts != 0 {
		t.Fatalf("expected 0 timeouts, got %d", stats.Timeouts)
	}
	if stats.TotalWaitTime <= 0 {
		t.Fatalf("expected non-zero cumulative wait time")
	}
}

func TestTxnRegistry_Lifecycle(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(true)
	infos := db.WriteTxns()
	if len(infos) != 1 {
		t.Fatalf("expected 1 tracked write txn, got %d", len(infos))
	}
	if infos[0].ID != txn.ID() || infos[0].Mutating {
		t.Fatalf("bad initial info: %+v", infos[0])
	}
	if db.OldestWriteTxnAge() <= 0 {
		t.Fatalf("oldest age should be positive")
	}

	if err := txn.Insert("main", testObj()); err != nil {
		t.Fatalf("err: %v", err)
	}
	infos = db.WriteTxns()
	if len(infos) != 1 || !infos[0].Mutating || infos[0].LockHoldTime <= 0 {
		t.Fatalf("expected a mutating txn with hold time, got %+v", infos)
	}

	txn.Commit()
	if len(db.WriteTxns()) != 0 {
		t.Fatalf("committed txn should be unregistered")
	}
	if db.OldestWriteTxnAge() != 0 {
		t.Fatalf("oldest age should reset to zero")
	}

	// A read txn is never registered.
	readTxn := db.Txn(false)
	defer readTxn.Abort()
	if len(db.WriteTxns()) != 0 {
		t.Fatalf("read txn must not be registered")
	}
}

func TestTxnRegistry_AbortWithoutMutation(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(true)
	if len(db.WriteTxns()) != 1 {
		t.Fatalf("txn should be tracked on open")
	}

	txn.Abort()
	if len(db.WriteTxns()) != 0 {
		t.Fatalf("aborted txn should be unregistered")
	}
	stats := db.WriterLockStats()
	if stats.Held {
		t.Fatalf("a non-mutating txn must never take the lock")
	}
}
