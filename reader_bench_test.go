// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import "testing"

func BenchmarkReadTxnOpenClose(b *testing.B) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn := db.Txn(false)
			txn.Abort()
		}
	})
}
