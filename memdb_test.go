// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"testing"
	"time"
)

func TestMemDB_SingleWriter_LazyLock(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Opening write transactions takes no lock until the first mutation, so
	// any number of them can be open concurrently with readers without
	// blocking each other.
	tx1 := db.Txn(true)
	tx2 := db.Txn(false) // Should not block!
	tx3 := db.Txn(false) // Should not block!
	tx4 := db.Txn(false) // Should not block!
	tx5 := db.Txn(true)  // Open writer that has not mutated: should not block.

	if tx1.Mutating() || tx5.Mutating() {
		t.Fatalf("a write transaction must not hold the lock before its first mutation")
	}

	openCh := make(chan struct{})
	go func() {
		defer close(openCh)
		db.Txn(true)
	}()

	select {
	case <-openCh:
	case <-time.After(25 * time.Millisecond):
		t.Fatalf("opening a write transaction must not block while nobody holds the lock")
	}

	// Now tx1 performs its first mutation and takes the writer lock.
	obj := testObj()
	if err := tx1.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !tx1.Mutating() {
		t.Fatalf("tx1 should hold the writer lock after Insert")
	}

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		other := db.Txn(true)
		// The first mutation is where lock contention shows up.
		if err := other.Insert("main", testObj()); err != nil {
			t.Errorf("err: %v", err)
			return
		}
		other.Commit()
	}()

	select {
	case <-doneCh:
		t.Fatalf("should not allow another writer")
	case <-time.After(10 * time.Millisecond):
	}

	// Readers and non-mutating writers must keep making progress.
	readTxn := db.Txn(false)
	if _, err := readTxn.First("main", "id", obj.ID); err != nil {
		t.Fatalf("reader must not block: %v", err)
	}
	readTxn.Abort()

	if !tx5.Mutating() {
		if _, err := tx5.First("main", "id", obj.ID); err != nil {
			t.Fatalf("non-mutating writer must still be able to read: %v", err)
		}
	}

	tx1.Abort()
	tx5.Abort()
	tx2.Abort()
	tx3.Abort()
	tx4.Abort()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("should allow another writer")
	}
}

func TestMemDB_Snapshot(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add an object
	obj := testObj()
	txn := db.Txn(true)
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Clone the db
	db2 := db.Snapshot()

	// Remove the object
	txn = db.Txn(true)
	if err := txn.Delete("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Object should exist in second snapshot but not first
	txn = db.Txn(false)
	out, err := txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != nil {
		t.Fatalf("should not exist %#v", out)
	}

	txn = db2.Txn(true)
	out, err = txn.First("main", "id", obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil {
		t.Fatalf("should exist")
	}
}
