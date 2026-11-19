// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"fmt"
	"testing"
)

// BenchmarkWriteTxn measures the cost of short independent write
// transactions, the hot path that must not regress from lazy locking.
func BenchmarkWriteTxn(b *testing.B) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}
	obj := testObj()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.ID = fmt.Sprintf("bench-%d", i)
		txn := db.Txn(true)
		if err := txn.Insert("main", obj); err != nil {
			b.Fatalf("err: %v", err)
		}
		txn.Commit()
	}
}

// BenchmarkWriteTxn_WithLongReadTxn runs the same write workload while a
// long-lived transaction stays open and only reads. With lazy locking the
// long transaction never takes the writer lock, so throughput should match
// BenchmarkWriteTxn.
func BenchmarkWriteTxn_WithLongReadTxn(b *testing.B) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	longTxn := db.Txn(true)
	b.Cleanup(longTxn.Abort)

	obj := testObj()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.ID = fmt.Sprintf("bench-long-%d", i)
		txn := db.Txn(true)
		if err := txn.Insert("main", obj); err != nil {
			b.Fatalf("err: %v", err)
		}

		// Keep the long transaction visibly reading so it cannot be optimized
		// away; it must never block the commit above.
		if _, err := longTxn.First("main", "id", obj.ID); err != nil {
			b.Fatalf("long txn read failed: %v", err)
		}

		txn.Commit()
	}
}

// BenchmarkReadTxn confirms the read hot path is unchanged (no lock or
// registry work).
func BenchmarkReadTxn(b *testing.B) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	seed := db.Txn(true)
	seedObj := testObj()
	seedObj.ID = "bench-read"
	if err := seed.Insert("main", seedObj); err != nil {
		b.Fatalf("err: %v", err)
	}
	seed.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.Txn(false)
		raw, err := txn.First("main", "id", "bench-read")
		if err != nil || raw == nil {
			b.Fatalf("bad lookup: %v %v", raw, err)
		}
		txn.Abort()
	}
}
