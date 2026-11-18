// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"fmt"
	"testing"
	"time"
)

func TestMemDB_ActiveTxns(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if infos := db.ActiveTxns(); len(infos) != 0 {
		t.Fatalf("expected no active transactions, got %d", len(infos))
	}

	readTxn := db.Txn(false)
	writeTxn := db.Txn(true)

	infos := db.ActiveTxns()
	if len(infos) != 2 {
		t.Fatalf("expected 2 active transactions, got %d", len(infos))
	}

	byID := make(map[uint64]TxnInfo, len(infos))
	for _, info := range infos {
		byID[info.ID] = info
		if info.Start.IsZero() {
			t.Fatalf("transaction %d has no start time", info.ID)
		}
		if age := info.Age(); age < 0 {
			t.Fatalf("transaction %d has negative age %v", info.ID, age)
		}
	}

	readInfo, ok := byID[readTxn.ID()]
	if !ok {
		t.Fatalf("read transaction %d not reported", readTxn.ID())
	}
	if readInfo.Write {
		t.Fatalf("read transaction reported as write")
	}

	writeInfo, ok := byID[writeTxn.ID()]
	if !ok {
		t.Fatalf("write transaction %d not reported", writeTxn.ID())
	}
	if !writeInfo.Write {
		t.Fatalf("write transaction reported as read")
	}

	// Committing the write transaction and aborting the read transaction
	// must remove them from the registry.
	writeTxn.Commit()
	readTxn.Abort()

	if infos := db.ActiveTxns(); len(infos) != 0 {
		t.Fatalf("expected no active transactions, got %d", len(infos))
	}

	// Commit on a read transaction must also stop tracking it.
	readTxn = db.Txn(false)
	readTxn.Commit()
	if infos := db.ActiveTxns(); len(infos) != 0 {
		t.Fatalf("expected no active transactions, got %d", len(infos))
	}
}

func TestMemDB_LongRunningTxns(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(false)
	defer txn.Abort()

	// A zero threshold matches every open transaction.
	if infos := db.LongRunningTxns(0); len(infos) != 1 {
		t.Fatalf("expected 1 long-running transaction, got %d", len(infos))
	}

	// A huge threshold matches nothing.
	if infos := db.LongRunningTxns(time.Hour); len(infos) != 0 {
		t.Fatalf("expected no long-running transactions, got %d", len(infos))
	}
}

func TestTxn_WriteTxnTimeout(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	db.SetWriteTxnTimeout(50 * time.Millisecond)

	txn := db.Txn(true)
	obj := testObj()
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Let the transaction exceed the timeout. The next mutating operation
	// must fail and the transaction must be aborted.
	time.Sleep(100 * time.Millisecond)

	obj2 := testObj()
	obj2.ID = "other-obj"
	if err := txn.Insert("main", obj2); err != ErrTxnTimeout {
		t.Fatalf("expected ErrTxnTimeout, got %v", err)
	}

	// The writer lock must have been released, so a new writer makes
	// progress immediately instead of being stuck behind the stale txn.
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		txn2 := db.Txn(true)
		txn2.Commit()
	}()
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("writer lock not released after transaction timeout")
	}

	// The timed-out transaction must not be tracked anymore.
	if infos := db.ActiveTxns(); len(infos) != 0 {
		t.Fatalf("expected no active transactions, got %d", len(infos))
	}

	// None of the timed-out transaction's changes may be visible.
	readTxn := db.Txn(false)
	defer readTxn.Abort()
	out, err := readTxn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != nil {
		t.Fatalf("changes from timed-out transaction must not be visible")
	}
}

func TestTxn_WriteTxnTimeoutCommit(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	db.SetWriteTxnTimeout(50 * time.Millisecond)

	txn := db.Txn(true)
	obj := testObj()
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Committing after the timeout aborts instead of committing.
	time.Sleep(100 * time.Millisecond)
	txn.Commit()

	readTxn := db.Txn(false)
	defer readTxn.Abort()
	out, err := readTxn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != nil {
		t.Fatalf("commit after timeout must not publish changes")
	}
}

func TestTxn_ReadTxnNoTimeout(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	db.SetWriteTxnTimeout(50 * time.Millisecond)

	obj := testObj()
	txn := db.Txn(true)
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// A read transaction outliving the write timeout keeps working: it
	// holds no locks and blocks nobody, so it is allowed to run to
	// completion (e.g. a long report).
	readTxn := db.Txn(false)
	defer readTxn.Abort()
	time.Sleep(100 * time.Millisecond)

	out, err := readTxn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil {
		t.Fatalf("read transaction should still see its snapshot")
	}
}

func TestTxn_LongReadDoesNotBlockWrites(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	obj := testObj()
	txn := db.Txn(true)
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Open a long-lived read transaction, like a report scanning the DB.
	report := db.Txn(false)
	defer report.Abort()

	// Writers must make progress while the report transaction is open.
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		for i := 0; i < 100; i++ {
			wtxn := db.Txn(true)
			wobj := testObj()
			wobj.ID = fmt.Sprintf("obj-%d", i)
			if err := wtxn.Insert("main", wobj); err != nil {
				t.Errorf("err: %v", err)
				wtxn.Abort()
				return
			}
			wtxn.Commit()
		}
	}()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("writers blocked by long-running read transaction")
	}

	// The report transaction still sees its original consistent snapshot:
	// none of the concurrently committed writes are visible to it.
	it, err := report.Get("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	count := 0
	for obj := it.Next(); obj != nil; obj = it.Next() {
		count++
	}
	if count != 1 {
		t.Fatalf("read transaction observed %d objects, expected its snapshot to contain 1", count)
	}
}
