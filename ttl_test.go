// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"reflect"
	"testing"
	"time"
)

// ttlObject is a TestObject-like row carrying an explicit expiry.
type ttlObject struct {
	ID     string
	Expiry time.Time
}

func (o *ttlObject) ExpiresAt() time.Time {
	return o.Expiry
}

// permanentObject deliberately does not implement Expirer to exercise the
// never-expiring path.
type permanentObject struct {
	ID string
}

type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time {
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.t = c.t.Add(d)
}

func ttlSchema() *DBSchema {
	return &DBSchema{
		Tables: map[string]*TableSchema{
			"doc": {
				Name: "doc",
				TTL:  &TTLConfig{},
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
				},
			},
		},
	}
}

func newTTLDB(t *testing.T) (*MemDB, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Unix(1000, 0)}
	db, err := NewMemDB(ttlSchema(), WithClock(clock.now))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db, clock
}

func idsFromIter(t *testing.T, iter ResultIterator) []string {
	t.Helper()
	var got []string
	for {
		raw := iter.Next()
		if raw == nil {
			return got
		}
		switch obj := raw.(type) {
		case *ttlObject:
			got = append(got, obj.ID)
		case *permanentObject:
			got = append(got, obj.ID)
		default:
			t.Fatalf("unexpected object type %T", raw)
		}
	}
}

func TestTTL_ReservedIndexInstalled(t *testing.T) {
	db, _ := newTTLDB(t)

	// The user-facing schema stays untouched.
	if _, ok := ttlSchema().Tables["doc"].Indexes[ttlIndexName]; ok {
		t.Fatalf("user schema must not contain the reserved TTL index")
	}
	// The runtime schema gets the internal expiry index.
	if _, ok := db.schema.Tables["doc"].Indexes[ttlIndexName]; !ok {
		t.Fatalf("reserved TTL index was not installed")
	}
}

func TestTTL_ReservedIndexNameRejected(t *testing.T) {
	schema := ttlSchema()
	schema.Tables["doc"].Indexes[ttlIndexName] = &IndexSchema{
		Name:    ttlIndexName,
		Unique:  true,
		Indexer: &StringFieldIndex{Field: "ID"},
	}
	if _, err := NewMemDB(schema); err == nil {
		t.Fatalf("expected validation error for reserved index name")
	}
}

func TestTTL_ReadFiltersExpiredOnEveryIterator(t *testing.T) {
	db, clock := newTTLDB(t)

	base := clock.t
	rows := []*ttlObject{
		{ID: "expired-1", Expiry: base.Add(-time.Second)},
		{ID: "alive-1", Expiry: base.Add(time.Minute)},
		{ID: "expired-2", Expiry: base.Add(-time.Hour)},
		{ID: "alive-2", Expiry: base.Add(time.Hour)},
	}

	txn := db.Txn(true)
	for _, row := range rows {
		if err := txn.Insert("doc", row); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()

	want := []string{"alive-1", "alive-2"}

	iter, err := read.Get("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := idsFromIter(t, iter); !reflect.DeepEqual(got, want) {
		t.Fatalf("Get mismatch: got %v want %v", got, want)
	}

	reverse, err := read.GetReverse("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := idsFromIter(t, reverse); !reflect.DeepEqual(got, []string{"alive-2", "alive-1"}) {
		t.Fatalf("GetReverse mismatch: got %v", got)
	}

	lower, err := read.LowerBound("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := idsFromIter(t, lower); !reflect.DeepEqual(got, want) {
		t.Fatalf("LowerBound mismatch: got %v want %v", got, want)
	}

	first, err := read.First("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if first.(*ttlObject).ID != "alive-1" {
		t.Fatalf("First returned expired or wrong row: %#v", first)
	}

	last, err := read.Last("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if last.(*ttlObject).ID != "alive-2" {
		t.Fatalf("Last returned expired or wrong row: %#v", last)
	}
}

func TestTTL_PointLookupHidesExpired(t *testing.T) {
	db, clock := newTTLDB(t)
	expired := &ttlObject{ID: "gone", Expiry: clock.t.Add(-time.Second)}

	txn := db.Txn(true)
	if err := txn.Insert("doc", expired); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()

	raw, err := read.First("doc", "id", "gone")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("expired object must not be returned, got %#v", raw)
	}

	// Move the clock back and the same physical record is visible again, which
	// proves read filtering is lazy and does not mutate stored data.
	clock.t = clock.t.Add(-time.Minute)
	fresh := db.Txn(false)
	defer fresh.Abort()
	raw, err = fresh.First("doc", "id", "gone")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil {
		t.Fatalf("object should be visible before its expiry")
	}
}

func TestTTL_NonExpiringObjectsStayVisible(t *testing.T) {
	db, clock := newTTLDB(t)

	txn := db.Txn(true)
	if err := txn.Insert("doc", &permanentObject{ID: "forever"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("doc", &ttlObject{ID: "zero", Expiry: time.Time{}}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	clock.advance(24 * time.Hour)
	read := db.Txn(false)
	defer read.Abort()

	iter, err := read.Get("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := idsFromIter(t, iter); !reflect.DeepEqual(got, []string{"forever", "zero"}) {
		t.Fatalf("permanent rows must survive: got %v", got)
	}
}

func TestTTL_PurgeRemovesOnlyExpiredAndAdvancesWatermark(t *testing.T) {
	db, clock := newTTLDB(t)
	base := clock.t

	txn := db.Txn(true)
	if err := txn.Insert("doc", &ttlObject{ID: "old-1", Expiry: base.Add(-10 * time.Second)}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("doc", &ttlObject{ID: "old-2", Expiry: base.Add(-5 * time.Second)}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("doc", &ttlObject{ID: "new", Expiry: base.Add(10 * time.Second)}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	purge := db.Txn(true)
	removed, err := purge.PurgeExpired("doc", 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	purge.Commit()

	if removed != 2 {
		t.Fatalf("expected 2 removals, got %d", removed)
	}
	if mark := db.loadWatermarks()["doc"]; mark != base.UnixNano() {
		t.Fatalf("watermark mismatch: got %d want %d", mark, base.UnixNano())
	}

	read := db.Txn(false)
	defer read.Abort()
	iter, err := read.Get("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := idsFromIter(t, iter); !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatalf("only the live row should remain, got %v", got)
	}

	// Once the watermark catches up, wrapTTL must hand back the raw iterator
	// instead of the filtering wrapper.
	raw, _, err := read.getIndexIterator("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	wrapped := read.wrapTTL("doc", &radixIterator{iter: raw})
	if _, ok := wrapped.(*expiryIterator); ok {
		t.Fatalf("filtering iterator should be skipped once the watermark is current")
	}
}

func TestTTL_LimitedPurgeDoesNotAdvanceWatermark(t *testing.T) {
	db, clock := newTTLDB(t)
	base := clock.t

	txn := db.Txn(true)
	for i, id := range []string{"a", "b", "c"} {
		if err := txn.Insert("doc", &ttlObject{
			ID:     id,
			Expiry: base.Add(-time.Duration(i+1) * time.Second),
		}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	txn.Commit()

	first := db.Txn(true)
	removed, err := first.PurgeExpired("doc", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	first.Commit()
	if removed != 2 {
		t.Fatalf("limit should remove 2 rows, got %d", removed)
	}
	if mark := db.loadWatermarks()["doc"]; mark != 0 {
		t.Fatalf("limited purge must not advance the watermark, got %d", mark)
	}

	second := db.Txn(true)
	removed, err = second.PurgeExpired("doc", 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	second.Commit()
	if removed != 1 {
		t.Fatalf("second purge should remove the last row, got %d", removed)
	}
	if mark := db.loadWatermarks()["doc"]; mark != base.UnixNano() {
		t.Fatalf("watermark should advance after a complete purge, got %d", mark)
	}
}

func TestTTL_PurgeOnNonTTLTableErrors(t *testing.T) {
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"plain": {
				Name: "plain",
				Indexes: map[string]*IndexSchema{
					"id": {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
				},
			},
		},
	}
	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)
	defer txn.Abort()
	if _, err := txn.PurgeExpired("plain", 0); err == nil {
		t.Fatalf("purge on a table without TTL must fail")
	}

	read := db.Txn(false)
	defer read.Abort()
	if _, err := read.PurgeExpired("plain", 0); err == nil {
		t.Fatalf("purge on a read-only transaction must fail")
	}
}

func TestTTL_UpdateExpiryReindexes(t *testing.T) {
	db, clock := newTTLDB(t)
	base := clock.t

	txn := db.Txn(true)
	if err := txn.Insert("doc", &ttlObject{ID: "row", Expiry: base.Add(-time.Second)}); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Extend the lifetime: the old expiry index entry must be replaced.
	if err := txn.Insert("doc", &ttlObject{ID: "row", Expiry: base.Add(time.Minute)}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()
	raw, err := read.First("doc", "id", "row")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil {
		t.Fatalf("refreshed object should be visible")
	}

	purge := db.Txn(true)
	removed, err := purge.PurgeExpired("doc", 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	purge.Commit()
	if removed != 0 {
		t.Fatalf("refreshed object must not be purged, removed %d", removed)
	}
}

func TestTTL_ConcurrentReadsAndPurge(t *testing.T) {
	db, clock := newTTLDB(t)
	base := clock.t

	writer := db.Txn(true)
	for i := 0; i < 200; i++ {
		expiry := base.Add(time.Duration(i-100) * time.Second)
		if err := writer.Insert("doc", &ttlObject{
			ID:     "row-" + itoa(i),
			Expiry: expiry,
		}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	writer.Commit()

	stop := make(chan struct{})
	done := make(chan struct{})

	// Readers continuously iterate and must never observe an expired row.
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			read := db.Txn(false)
			iter, err := read.Get("doc", "id")
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			for raw := iter.Next(); raw != nil; raw = iter.Next() {
				row := raw.(*ttlObject)
				if !row.Expiry.After(clock.t) {
					t.Errorf("reader observed expired object %s", row.ID)
					return
				}
			}
			read.Abort()
		}
	}()

	// Background cleanup drains the table in small batches.
	for removed := 1; removed > 0; {
		purge := db.Txn(true)
		var err error
		removed, err = purge.PurgeExpired("doc", 25)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		purge.Commit()
	}
	close(stop)
	<-done

	final := db.Txn(false)
	defer final.Abort()
	iter, _ := final.Get("doc", "id")
	if got := idsFromIter(t, iter); len(got) != 99 {
		t.Fatalf("expected 99 strictly-future rows, got %d", len(got))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func TestTTL_DeleteAllMaintainsExpiryIndex(t *testing.T) {
	db, clock := newTTLDB(t)
	base := clock.t

	txn := db.Txn(true)
	for i := 0; i < 5; i++ {
		if err := txn.Insert("doc", &ttlObject{
			ID:     itoa(i),
			Expiry: base.Add(time.Minute),
		}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	txn.Commit()

	del := db.Txn(true)
	removed, err := del.DeleteAll("doc", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	del.Commit()
	if removed != 5 {
		t.Fatalf("expected 5 deletions, got %d", removed)
	}

	// After deleting every row, a purge must find nothing left in the expiry
	// index. Any stale entry would surface as a dangling object here.
	purge := db.Txn(true)
	left, err := purge.PurgeExpired("doc", 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	purge.Commit()
	if left != 0 {
		t.Fatalf("expiry index should be empty after DeleteAll, found %d", left)
	}
}
