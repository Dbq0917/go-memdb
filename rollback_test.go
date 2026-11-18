// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"strconv"
	"testing"
	"time"
)

// indexContents materializes every row reachable through the given index.
// It is used by the rollback tests to prove that aborted mutations leave no
// stale entries in any index, including non-unique and multi-value indexes.
func indexContents(t *testing.T, db *MemDB, table, index string) []interface{} {
	t.Helper()
	txn := db.Txn(false)
	defer txn.Abort()
	iter, err := txn.Get(table, index)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	var out []interface{}
	for raw := iter.Next(); raw != nil; raw = iter.Next() {
		out = append(out, raw)
	}
	return out
}

// TestTxn_Abort_IndexRollback_Insert verifies that rows inserted in an
// aborted transaction cannot be looked up through any index afterwards. This
// is the reconciliation scenario: rolled-back keys must not be discoverable
// through secondary indexes.
func TestTxn_Abort_IndexRollback_Insert(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(true)
	assertNilError(t, txn.Insert("main", &TestObject{
		ID: "a", Foo: "x", Qux: []string{"q1", "q2"},
	}))
	assertNilError(t, txn.Insert("main", &TestObject{
		ID: "b", Foo: "y", Qux: []string{"q2"},
	}))
	txn.Abort()

	for _, index := range []string{"id", "foo", "qux"} {
		if got := indexContents(t, db, "main", index); len(got) != 0 {
			t.Fatalf("index %q retained rolled-back entries: %#v", index, got)
		}
	}
}

// TestTxn_Abort_IndexRollback_MixedMutation verifies that a mix of inserts,
// updates and deletes in one aborted transaction is reversed atomically in
// the primary index, every secondary index and every multi-value index.
func TestTxn_Abort_IndexRollback_MixedMutation(t *testing.T) {
	db := testDB(t)

	old := &TestObject{ID: "keep", Foo: "old", Qux: []string{"oq1"}}
	gone := &TestObject{ID: "gone", Foo: "gone-foo", Qux: []string{"gq"}}
	seed := db.Txn(true)
	assertNilError(t, seed.Insert("main", old))
	assertNilError(t, seed.Insert("main", gone))
	seed.Commit()

	txn := db.Txn(true)
	assertNilError(t, txn.Insert("main", &TestObject{
		ID: "new", Foo: "new-foo", Qux: []string{"nq"},
	}))
	assertNilError(t, txn.Insert("main", &TestObject{
		ID: "keep", Foo: "changed", Qux: []string{"cq"},
	}))
	assertNilError(t, txn.Delete("main", gone))
	txn.Abort()

	read := db.Txn(false)
	defer read.Abort()

	// Primary index: exactly the two committed rows, with the original
	// object pointers restored for both the updated and deleted rows.
	if got := indexContents(t, db, "main", "id"); len(got) != 2 {
		t.Fatalf("id index has %d entries after abort, want 2: %#v", len(got), got)
	}
	raw, err := read.First("main", "id", "keep")
	assertNilError(t, err)
	if raw != old {
		t.Fatalf("updated row not restored: got %#v want %#v", raw, old)
	}
	raw, err = read.First("main", "id", "gone")
	assertNilError(t, err)
	if raw != gone {
		t.Fatalf("deleted row not restored: got %#v want %#v", raw, gone)
	}
	raw, err = read.First("main", "id", "new")
	assertNilError(t, err)
	if raw != nil {
		t.Fatalf("inserted row leaked through id index: %#v", raw)
	}

	// Secondary and multi-value indexes: the committed entries are back.
	for _, tc := range []struct {
		index, arg string
		want       interface{}
	}{
		{"foo", "old", old},
		{"foo", "gone-foo", gone},
		{"qux", "oq1", old},
		{"qux", "gq", gone},
	} {
		raw, err := read.First("main", tc.index, tc.arg)
		assertNilError(t, err)
		if raw != tc.want {
			t.Fatalf("index %q/%q not restored: got %#v want %#v",
				tc.index, tc.arg, raw, tc.want)
		}
	}

	// No entry introduced by the aborted transaction may remain.
	for _, tc := range []struct{ index, arg string }{
		{"foo", "changed"},
		{"foo", "new-foo"},
		{"qux", "cq"},
		{"qux", "nq"},
	} {
		raw, err := read.First("main", tc.index, tc.arg)
		assertNilError(t, err)
		if raw != nil {
			t.Fatalf("index %q/%q retained a rolled-back entry: %#v",
				tc.index, tc.arg, raw)
		}
	}
}

// TestTxn_Abort_IndexRollback_DeletePrefix verifies that the efficient
// subtree delete path is reversed in every index on abort.
func TestTxn_Abort_IndexRollback_DeletePrefix(t *testing.T) {
	db := testDB(t)

	seed := db.Txn(true)
	assertNilError(t, seed.Insert("main", &TestObject{
		ID: "p1", Foo: "prefix-1", Qux: []string{"q1"},
	}))
	assertNilError(t, seed.Insert("main", &TestObject{
		ID: "p2", Foo: "prefix-2", Qux: []string{"q2"},
	}))
	assertNilError(t, seed.Insert("main", &TestObject{
		ID: "o1", Foo: "other", Qux: []string{"q3"},
	}))
	seed.Commit()

	txn := db.Txn(true)
	found, err := txn.DeletePrefix("main", "foo_prefix", "prefix")
	assertNilError(t, err)
	if !found {
		t.Fatalf("DeletePrefix should have matched entries")
	}
	txn.Abort()

	if got := indexContents(t, db, "main", "id"); len(got) != 3 {
		t.Fatalf("id index has %d entries after abort, want 3: %#v", len(got), got)
	}
	if got := indexContents(t, db, "main", "foo"); len(got) != 3 {
		t.Fatalf("foo index has %d entries after abort, want 3: %#v", len(got), got)
	}
	if got := indexContents(t, db, "main", "qux"); len(got) != 3 {
		t.Fatalf("qux index has %d entries after abort, want 3: %#v", len(got), got)
	}
}

// TestTxn_Abort_ReleasesWriter verifies that abort releases the single-writer
// lock and leaves the database usable for subsequent write transactions.
func TestTxn_Abort_ReleasesWriter(t *testing.T) {
	db := testDB(t)

	txn := db.Txn(true)
	assertNilError(t, txn.Insert("main", &TestObject{
		ID: "x", Foo: "f", Qux: []string{"qx"},
	}))
	txn.Abort()

	txn2 := db.Txn(true)
	assertNilError(t, txn2.Insert("main", &TestObject{
		ID: "y", Foo: "g", Qux: []string{"qy"},
	}))
	txn2.Commit()

	read := db.Txn(false)
	defer read.Abort()
	raw, err := read.First("main", "id", "x")
	assertNilError(t, err)
	if raw != nil {
		t.Fatalf("aborted row leaked: %#v", raw)
	}
	raw, err = read.First("main", "id", "y")
	assertNilError(t, err)
	if raw == nil {
		t.Fatalf("row committed after the abort is missing")
	}
}

// TestTxn_Abort_DoesNotNotifyWatchers verifies that an aborted transaction
// does not close watch channels: nothing about the committed state changed,
// so pending queries must keep watching. Only a later real commit fires them.
func TestTxn_Abort_DoesNotNotifyWatchers(t *testing.T) {
	db := testDB(t)

	original := &TestObject{ID: "k", Foo: "v", Qux: []string{"q"}}
	seed := db.Txn(true)
	assertNilError(t, seed.Insert("main", original))
	seed.Commit()

	read := db.Txn(false)
	defer read.Abort()
	watchCh, _, err := read.FirstWatch("main", "id", "k")
	assertNilError(t, err)

	aborted := db.Txn(true)
	assertNilError(t, aborted.Insert("main", &TestObject{
		ID: "k", Foo: "changed", Qux: []string{"q2"},
	}))
	aborted.Abort()

	select {
	case <-watchCh:
		t.Fatalf("watch fired despite the transaction being aborted")
	case <-time.After(50 * time.Millisecond):
	}

	committed := db.Txn(true)
	assertNilError(t, committed.Insert("main", &TestObject{
		ID: "k", Foo: "committed", Qux: []string{"q3"},
	}))
	committed.Commit()

	select {
	case <-watchCh:
	case <-time.After(time.Second):
		t.Fatalf("watch did not fire after a real commit")
	}
}

// TestTxn_Abort_ConcurrentReaderIsolation verifies that a long-running read
// transaction and a fresh read transaction both observe only committed data
// while a write transaction is aborted around them.
func TestTxn_Abort_ConcurrentReaderIsolation(t *testing.T) {
	db := testDB(t)

	old := &TestObject{ID: "keep", Foo: "old", Qux: []string{"oq"}}
	gone := &TestObject{ID: "gone", Foo: "gone-foo", Qux: []string{"gq"}}
	seed := db.Txn(true)
	assertNilError(t, seed.Insert("main", old))
	assertNilError(t, seed.Insert("main", gone))
	seed.Commit()

	longReader := db.Txn(false)
	defer longReader.Abort()

	writer := db.Txn(true)
	assertNilError(t, writer.Insert("main", &TestObject{
		ID: "new", Foo: "new-foo", Qux: []string{"nq"},
	}))
	assertNilError(t, writer.Delete("main", gone))
	writer.Abort()

	freshReader := db.Txn(false)
	defer freshReader.Abort()

	for _, txn := range []*Txn{longReader, freshReader} {
		raw, err := txn.First("main", "id", "gone")
		assertNilError(t, err)
		if raw != gone {
			t.Fatalf("deleted row should remain visible after abort, got %#v", raw)
		}
		raw, err = txn.First("main", "id", "new")
		assertNilError(t, err)
		if raw != nil {
			t.Fatalf("inserted row should be invisible after abort, got %#v", raw)
		}
	}
}

const benchmarkRows = 100

func newBenchmarkDB(b *testing.B) *MemDB {
	b.Helper()
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}
	return db
}

func makeBenchmarkRows(iteration int) []*TestObject {
	rows := make([]*TestObject, benchmarkRows)
	for i := range rows {
		rows[i] = &TestObject{
			ID:  "row-" + strconv.Itoa(iteration) + "-" + strconv.Itoa(i),
			Foo: "foo-" + strconv.Itoa(i),
			Qux: []string{"qux-" + strconv.Itoa(i%4)},
		}
	}
	return rows
}

// BenchmarkTxn_Abort shows that rolling back discards the pending
// copy-on-write index mutations without applying inverse writes, so abort
// cost is independent of the commit path and adds nothing to it.
func BenchmarkTxn_Abort(b *testing.B) {
	db := newBenchmarkDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.Txn(true)
		for _, row := range makeBenchmarkRows(i) {
			if err := txn.Insert("main", row); err != nil {
				b.Fatalf("err: %v", err)
			}
		}
		txn.Abort()
	}
}

// BenchmarkTxn_Commit is the commit-path baseline. It must not change when
// the rollback behaviour is modified, because abort and commit share no work.
func BenchmarkTxn_Commit(b *testing.B) {
	db := newBenchmarkDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.Txn(true)
		for _, row := range makeBenchmarkRows(i) {
			if err := txn.Insert("main", row); err != nil {
				b.Fatalf("err: %v", err)
			}
		}
		txn.Commit()
	}
}
