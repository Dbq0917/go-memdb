// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemDB_SingleWriter_MultiReader(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	tx1 := db.Txn(true)
	tx2 := db.Txn(false) // Should not block!
	tx3 := db.Txn(false) // Should not block!
	tx4 := db.Txn(false) // Should not block!

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		db.Txn(true)
	}()

	select {
	case <-doneCh:
		t.Fatalf("should not allow another writer")
	case <-time.After(10 * time.Millisecond):
	}

	tx1.Abort()
	tx2.Abort()
	tx3.Abort()
	tx4.Abort()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("should allow another writer")
	}
}

func TestMemDB_ConcurrentInsert_UniqueConstraint(t *testing.T) {
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"main": {
				Name: "main",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"foo": {
						Name:    "foo",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "Foo"},
					},
				},
			},
		},
	}
	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	const workers = 2
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			txn := db.Txn(true)
			obj := &TestObject{
				ID:  fmt.Sprintf("object-%d", i),
				Foo: "shared",
			}
			if err := txn.Insert("main", obj); err != nil {
				errs <- err
				txn.Abort()
				return
			}
			errs <- txn.Commit()
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)

	var committed, rejected int
	for err := range errs {
		if err == nil {
			committed++
			continue
		}
		if !errors.Is(err, ErrUniqueConstraint) {
			t.Fatalf("unexpected commit error: %v", err)
		}
		rejected++
	}
	if committed != 1 || rejected != 1 {
		t.Fatalf("expected one commit and one rejection, got %d and %d", committed, rejected)
	}

	txn := db.Txn(false)
	defer txn.Abort()
	iter, err := txn.Get("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	var rows int
	for iter.Next() != nil {
		rows++
	}
	if rows != 1 {
		t.Fatalf("expected one row after concurrent insert, got %d", rows)
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
