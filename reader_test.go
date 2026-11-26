// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestReaderRegistry_LifecycleAndOldest(t *testing.T) {
	db := testDB(t)

	if _, ok := db.OldestReader(); ok {
		t.Fatal("empty database should not report a reader")
	}
	if age := db.OldestReaderAge(); age != 0 {
		t.Fatalf("empty database age = %v, want 0", age)
	}
	if readers := db.ActiveReaders(); len(readers) != 0 {
		t.Fatalf("empty database readers = %v, want none", readers)
	}

	first := db.Txn(false)
	firstID := first.ID()
	if firstID == 0 {
		t.Fatal("read transaction should have a non-zero ID")
	}

	time.Sleep(2 * time.Millisecond)
	second := db.Txn(false)
	if second.ID() == firstID {
		t.Fatalf("reader IDs must be unique, both are %d", firstID)
	}

	info, ok := db.OldestReader()
	if !ok {
		t.Fatal("expected an oldest reader")
	}
	if info.ID != firstID {
		t.Fatalf("oldest reader ID = %d, want %d", info.ID, firstID)
	}
	if info.Age < 2*time.Millisecond {
		t.Fatalf("oldest reader age = %v, want at least 2ms", info.Age)
	}
	if info.OpenFor() != info.Age {
		t.Fatalf("OpenFor = %v, Age = %v", info.OpenFor(), info.Age)
	}
	if db.ReaderCount() != 2 {
		t.Fatalf("reader count = %d, want 2", db.ReaderCount())
	}

	readers := db.ActiveReaders()
	if len(readers) != 2 || readers[0].ID != firstID || readers[1].ID != second.ID() {
		t.Fatalf("active readers not ordered oldest first: %+v", readers)
	}

	second.Commit()
	if db.ReaderCount() != 1 {
		t.Fatalf("reader count after commit = %d, want 1", db.ReaderCount())
	}
	first.Abort()
	if db.ReaderCount() != 0 {
		t.Fatalf("reader count after abort = %d, want 0", db.ReaderCount())
	}
	if first.ID() != firstID {
		t.Fatalf("reader ID changed after close: %d -> %d", firstID, first.ID())
	}

	// A write transaction is not a reader and does not retain the reader ID
	// space.
	writeTxn := db.Txn(true)
	if writeTxn.ID() != 0 {
		t.Fatalf("write transaction ID = %d, want 0", writeTxn.ID())
	}
	if db.ReaderCount() != 0 {
		t.Fatalf("write transaction was counted as a reader")
	}
	writeTxn.Abort()
}

func TestReadTxnCloseReleasesSnapshot(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(false)
	if txn.rootTxn == nil {
		t.Fatal("open reader should hold a snapshot root")
	}

	txn.Abort()
	if txn.rootTxn != nil {
		t.Fatal("aborting a reader should release its snapshot root")
	}

	// Repeated close is safe.
	txn.Abort()
	txn.Commit()

	txn = db.Txn(false)
	txn.Commit()
	if txn.rootTxn != nil {
		t.Fatal("committing a reader should release its snapshot root")
	}
}

func TestReaderRegistry_ConcurrentReaders(t *testing.T) {
	db := testDB(t)

	const readers = 256
	txns := make([]*Txn, readers)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range txns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			txns[i] = db.Txn(false)
		}(i)
	}
	close(start)
	wg.Wait()

	if count := db.ReaderCount(); count != readers {
		t.Fatalf("reader count = %d, want %d", count, readers)
	}
	if len(db.ActiveReaders()) != readers {
		t.Fatalf("active readers = %d, want %d", len(db.ActiveReaders()), readers)
	}

	start = make(chan struct{})
	for i := range txns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			txns[i].Abort()
		}(i)
	}
	close(start)
	wg.Wait()

	if count := db.ReaderCount(); count != 0 {
		t.Fatalf("reader count after close = %d, want 0", count)
	}
}

func TestTxnSnapshotIsRegisteredReader(t *testing.T) {
	db := testDB(t)

	writer := db.Txn(true)
	if err := writer.Insert("main", testObj()); err != nil {
		t.Fatalf("err: %v", err)
	}

	snapshot := writer.Snapshot()
	if snapshot == nil {
		t.Fatal("expected a snapshot transaction")
	}
	if db.ReaderCount() != 1 {
		t.Fatalf("reader count = %d, want 1", db.ReaderCount())
	}
	info, ok := db.OldestReader()
	if !ok || info.ID != snapshot.ID() {
		t.Fatalf("snapshot reader not reported: %+v, %v", info, ok)
	}

	// Closing the source transaction must not affect the independent
	// snapshot reader.
	writer.Abort()
	if db.ReaderCount() != 1 {
		t.Fatalf("source abort closed the snapshot reader")
	}

	snapshot.Abort()
	if db.ReaderCount() != 0 {
		t.Fatalf("snapshot abort left a registered reader")
	}
}

func TestReaderRegistry_CleansAbandonedTxn(t *testing.T) {
	db := testDB(t)

	func() {
		for i := 0; i < 32; i++ {
			_ = db.Txn(false)
		}
	}()

	if count := db.ReaderCount(); count != 32 {
		t.Fatalf("reader count = %d, want 32 before GC", count)
	}

	deadline := time.Now().Add(5 * time.Second)
	for db.ReaderCount() != 0 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if count := db.ReaderCount(); count != 0 {
		t.Fatalf("abandoned readers were not cleaned up: %d remain", count)
	}
}

func TestReaderRegistry_KeepsLiveReader(t *testing.T) {
	db := testDB(t)

	live := db.Txn(false)
	defer live.Abort()

	func() {
		for i := 0; i < 16; i++ {
			_ = db.Txn(false)
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for db.ReaderCount() != 1 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if count := db.ReaderCount(); count != 1 {
		t.Fatalf("reader count = %d, want only the live reader", count)
	}

	info, ok := db.OldestReader()
	if !ok || info.ID != live.ID() {
		t.Fatalf("live reader was reaped: %+v, %v", info, ok)
	}
}
