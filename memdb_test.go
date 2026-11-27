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

func barIndexSchema() *IndexSchema {
	return &IndexSchema{
		Name:    "bar",
		Indexer: &IntFieldIndex{Field: "Bar"},
	}
}

func testIdxObj(id string, bar int) *TestObject {
	return &TestObject{ID: id, Foo: "foo-" + id, Qux: []string{"q"}, Bar: bar}
}

func TestMemDB_AddIndex(t *testing.T) {
	db := testDB(t)

	// Insert some objects before the index exists
	objs := []*TestObject{
		testIdxObj("1", 1),
		testIdxObj("2", 2),
		testIdxObj("3", 2),
	}
	txn := db.Txn(true)
	for _, obj := range objs {
		if err := txn.Insert("main", obj); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	txn.Commit()

	// The index must not be queryable before it is added
	txn = db.Txn(false)
	if _, err := txn.Get("main", "bar", 2); err == nil {
		t.Fatalf("should fail")
	}
	txn.Abort()

	// Add the index online
	if err := db.AddIndex("main", barIndexSchema()); err != nil {
		t.Fatalf("err: %v", err)
	}

	// The backfilled index should serve all pre-existing records
	txn = db.Txn(false)
	iter, err := txn.Get("main", "bar", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	var got []interface{}
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		got = append(got, obj)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d", len(got))
	}
	out, err := txn.First("main", "bar", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil || out.(*TestObject).ID != "1" {
		t.Fatalf("bad: %#v", out)
	}
	txn.Abort()

	// New writes maintain the index
	txn = db.Txn(true)
	if err := txn.Insert("main", testIdxObj("4", 2)); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Updates move the object between index keys
	if err := txn.Insert("main", testIdxObj("1", 3)); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Deletes remove the object from the index
	if err := txn.Delete("main", testIdxObj("2", 2)); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	txn = db.Txn(false)
	iter, err = txn.Get("main", "bar", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got = got[:0]
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		got = append(got, obj)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d", len(got))
	}
	for _, obj := range got {
		if id := obj.(*TestObject).ID; id != "3" && id != "4" {
			t.Fatalf("bad: %v", id)
		}
	}
	out, err = txn.First("main", "bar", 3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil || out.(*TestObject).ID != "1" {
		t.Fatalf("bad: %#v", out)
	}
	txn.Abort()
}

func TestMemDB_AddIndex_MultiAndUnique(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(true)
	if err := txn.Insert("main", &TestObject{ID: "1", Foo: "a", Qux: []string{"a", "b"}}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("main", &TestObject{ID: "2", Foo: "b", Qux: []string{"b"}}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Multi-indexer
	err := db.AddIndex("main", &IndexSchema{
		Name:    "qux2",
		Indexer: &StringSliceFieldIndex{Field: "Qux"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Unique index
	err = db.AddIndex("main", &IndexSchema{
		Name:    "id2",
		Unique:  true,
		Indexer: &StringFieldIndex{Field: "ID"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn = db.Txn(false)
	iter, err := txn.Get("main", "qux2", "b")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	n := 0
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("expected 2 results, got %d", n)
	}
	out, err := txn.First("main", "id2", "1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil || out.(*TestObject).ID != "1" {
		t.Fatalf("bad: %#v", out)
	}
	txn.Abort()
}

func TestMemDB_AddIndex_SchemaNotMutated(t *testing.T) {
	schema := testValidSchema()
	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Take a snapshot before the index is added
	snap := db.Snapshot()

	if err := db.AddIndex("main", barIndexSchema()); err != nil {
		t.Fatalf("err: %v", err)
	}

	// The schema handed to NewMemDB must not be modified in-place
	if _, ok := schema.Tables["main"].Indexes["bar"]; ok {
		t.Fatalf("original schema was modified")
	}

	// The live schema knows about the new index
	if _, ok := db.DBSchema().Tables["main"].Indexes["bar"]; !ok {
		t.Fatalf("new schema missing index")
	}

	// The old snapshot keeps a consistent view of the old schema and must
	// fail cleanly instead of panicking or returning empty results
	txn := snap.Txn(false)
	defer txn.Abort()
	if _, err := txn.Get("main", "bar", 1); err == nil {
		t.Fatalf("should fail")
	}
}

func TestMemDB_AddIndex_Errors(t *testing.T) {
	db := testDB(t)

	if err := db.AddIndex("main", nil); err == nil {
		t.Fatalf("should fail")
	}
	if err := db.AddIndex("nope", barIndexSchema()); err == nil {
		t.Fatalf("should fail")
	}
	if err := db.AddIndex("main", &IndexSchema{Name: "broken"}); err == nil {
		t.Fatalf("should fail")
	}
	// Adding an existing index (including "id") must fail
	if err := db.AddIndex("main", &IndexSchema{
		Name:    "foo",
		Indexer: &StringFieldIndex{Field: "Foo"},
	}); err == nil {
		t.Fatalf("should fail")
	}
	if err := db.AddIndex("main", &IndexSchema{
		Name:    "id",
		Unique:  true,
		Indexer: &StringFieldIndex{Field: "ID"},
	}); err == nil {
		t.Fatalf("should fail")
	}

	// The failed attempts must not have leaked anything into the schema
	if len(db.DBSchema().Tables["main"].Indexes) != 3 {
		t.Fatalf("schema changed by failed AddIndex")
	}
}

// errIndexer always fails to build a key, used to exercise backfill
// failure and rollback.
type errIndexer struct{}

func (e *errIndexer) FromObject(obj interface{}) (bool, []byte, error) {
	return false, nil, fmt.Errorf("boom")
}

func (e *errIndexer) FromArgs(args ...interface{}) ([]byte, error) {
	return nil, fmt.Errorf("boom")
}

func TestMemDB_AddIndex_BackfillFailureRollsBack(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(true)
	if err := txn.Insert("main", testObj()); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	err := db.AddIndex("main", &IndexSchema{
		Name:    "broken",
		Indexer: &errIndexer{},
	})
	if err == nil {
		t.Fatalf("should fail")
	}

	// The index must be gone from the schema again
	if _, ok := db.DBSchema().Tables["main"].Indexes["broken"]; ok {
		t.Fatalf("index leaked into schema")
	}

	// Queries against it fail as unknown, not as not-ready
	txn = db.Txn(false)
	_, err = txn.Get("main", "broken")
	if err == nil || errors.Is(err, ErrIndexNotReady) {
		t.Fatalf("bad: %v", err)
	}
	txn.Abort()

	// The table keeps working for writes and reads
	txn = db.Txn(true)
	if err := txn.Insert("main", &TestObject{ID: "2", Foo: "two", Qux: []string{"q"}}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()
	txn = db.Txn(false)
	out, err := txn.First("main", "foo", "two")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil {
		t.Fatalf("should exist")
	}
	txn.Abort()
}

func TestMemDB_AddIndex_PendingNotReady(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(true)
	if err := txn.Insert("main", testIdxObj("1", 1)); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Register the index without backfilling it yet
	if err := db.registerIndex("main", barIndexSchema()); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Queries against the pending index must fail loudly, not return empty
	txn = db.Txn(false)
	if _, err := txn.Get("main", "bar", 1); !errors.Is(err, ErrIndexNotReady) {
		t.Fatalf("bad: %v", err)
	}
	if _, err := txn.First("main", "bar", 1); !errors.Is(err, ErrIndexNotReady) {
		t.Fatalf("bad: %v", err)
	}
	if _, err := txn.GetReverse("main", "bar", 1); !errors.Is(err, ErrIndexNotReady) {
		t.Fatalf("bad: %v", err)
	}
	if _, err := txn.LowerBound("main", "bar", 1); !errors.Is(err, ErrIndexNotReady) {
		t.Fatalf("bad: %v", err)
	}
	txn.Abort()

	// Writes while the index is pending are maintained in the index
	txn = db.Txn(true)
	if err := txn.Insert("main", testIdxObj("2", 2)); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Finish the backfill and mark the index ready
	if err := db.backfillIndex("main", "bar"); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := db.markIndexReady("main", "bar"); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Both the pre-existing and the pending-era records are indexed
	txn = db.Txn(false)
	defer txn.Abort()
	iter, err := txn.Get("main", "bar")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	n := 0
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("expected 2 results, got %d", n)
	}
}

func TestMemDB_AddIndex_ConcurrentReaders(t *testing.T) {
	db := testDB(t)

	const num = 1000
	txn := db.Txn(true)
	for i := 0; i < num; i++ {
		if err := txn.Insert("main", testIdxObj(fmt.Sprintf("%d", i), 7)); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	txn.Commit()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := db.AddIndex("main", barIndexSchema()); err != nil {
			t.Errorf("err: %v", err)
		}
	}()

	// Readers must never observe a partially built index: a query either
	// fails (unknown or not-ready index) or returns the complete result.
	for {
		select {
		case <-done:
			return
		default:
		}
		txn := db.Txn(false)
		iter, err := txn.Get("main", "bar")
		if err != nil {
			txn.Abort()
			continue
		}
		n := 0
		for obj := iter.Next(); obj != nil; obj = iter.Next() {
			n++
		}
		txn.Abort()
		if n != num {
			t.Fatalf("partial results: expected %d, got %d", num, n)
		}
	}
}

func TestMemDB_AddIndex_ConcurrentWriters(t *testing.T) {
	db := testDB(t)

	const num = 1000
	txn := db.Txn(true)
	for i := 0; i < num; i++ {
		if err := txn.Insert("main", testIdxObj(fmt.Sprintf("%d", i), i)); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	txn.Commit()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := db.AddIndex("main", barIndexSchema()); err != nil {
			t.Errorf("err: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		for i := num; i < 2*num; i++ {
			txn := db.Txn(true)
			if err := txn.Insert("main", testIdxObj(fmt.Sprintf("%d", i), i)); err != nil {
				t.Errorf("err: %v", err)
				txn.Abort()
				return
			}
			txn.Commit()
		}
	}()
	wg.Wait()

	// Every record, including the ones written while the index was being
	// added, must be present in the new index.
	txn = db.Txn(false)
	defer txn.Abort()
	iter, err := txn.Get("main", "bar")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	n := 0
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		n++
	}
	if n != 2*num {
		t.Fatalf("expected %d results, got %d", 2*num, n)
	}
}
