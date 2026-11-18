// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"encoding/binary"
	"fmt"
	"time"
)

// ttlIndexName is the reserved name of the internal expiry index installed
// on every table whose schema has a non-nil TTL config. The leading and
// trailing double underscores keep it out of the way of user defined index
// names.
const ttlIndexName = "__memdb_ttl__"

// Expirer is implemented by objects that carry an expiry time. When a table
// is configured with a TTL, read paths transparently hide any object whose
// ExpiresAt value is not the zero time and is at or before the transaction's
// read timestamp.
//
// Returning the zero time.Time means the object never expires.
type Expirer interface {
	ExpiresAt() time.Time
}

// TTLConfig enables time to live support for a table. Presence of a non-nil
// TTLConfig is all that is required: objects decide their own expiry by
// implementing Expirer. Objects without the Expirer interface, or whose
// ExpiresAt returns the zero time, are treated as never expiring.
type TTLConfig struct{}

// expiryOf extracts the expiry as a unix-nano timestamp. The boolean result is
// false when the object does not implement Expirer. A zero timestamp means the
// object never expires.
func expiryOf(obj interface{}) (int64, bool) {
	expirer, ok := obj.(Expirer)
	if !ok {
		return 0, false
	}
	at := expirer.ExpiresAt()
	if at.IsZero() {
		return 0, true
	}
	return at.UnixNano(), true
}

// encodeExpiryPrefix returns the 8 byte big-endian encoding of an expiry
// timestamp. Big-endian keeps the lexicographic order of the index keys equal
// to chronological order, which is what makes ordered purge scans possible.
func encodeExpiryPrefix(expiry int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(expiry))
	return key
}

// ttlIndexer maintains an internal non-unique index keyed by expiry timestamp.
// The insert path appends the primary key to non-unique index values, so the
// full key is expiry (8 bytes) + primary key, which stays unique.
type ttlIndexer struct{}

func (t *ttlIndexer) FromObject(obj interface{}) (bool, []byte, error) {
	expiry, ok := expiryOf(obj)
	if !ok || expiry == 0 {
		// AllowMissing is set on the index schema, so never-expiring objects
		// are simply absent from the expiry index.
		return false, nil, nil
	}
	return true, encodeExpiryPrefix(expiry), nil
}

func (t *ttlIndexer) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ttl index expects a single argument")
	}
	expiry, ok := args[0].(int64)
	if !ok {
		return nil, fmt.Errorf("ttl index argument must be int64 unix-nano, got %T", args[0])
	}
	return encodeExpiryPrefix(expiry), nil
}

// expiryIterator wraps a ResultIterator and skips expired objects. The read
// timestamp is captured once when the iterator is created rather than on every
// call to Next, so the per-record cost is a single int64 comparison.
type expiryIterator struct {
	iter ResultIterator
	now  int64
}

func (e *expiryIterator) WatchCh() <-chan struct{} {
	return e.iter.WatchCh()
}

func (e *expiryIterator) Next() interface{} {
	for {
		value := e.iter.Next()
		if value == nil {
			return nil
		}
		expiry, ok := expiryOf(value)
		if ok && expiry != 0 && expiry <= e.now {
			continue
		}
		return value
	}
}

// nowNanos returns the transaction's read timestamp. It is evaluated lazily at
// most once per transaction so that every iterator within a transaction shares
// a consistent notion of time, matching the snapshot semantics of the radix
// tree. Zero is used as the sentinel because unix-nano timestamps are positive
// in practice.
func (txn *Txn) nowNanos() int64 {
	if txn.now == 0 {
		txn.now = txn.db.nowFn().UnixNano()
	}
	return txn.now
}

// watermark returns the purge high-water mark for a table: every expiry at or
// below the mark has already been physically removed. The map is published
// immutably, so caching a reference on the transaction is safe.
func (txn *Txn) watermark(table string) int64 {
	if txn.watermarkCache == nil {
		txn.watermarkCache = txn.db.loadWatermarks()
	}
	return txn.watermarkCache[table]
}

// wrapTTL hides expired records from an iterator on a TTL table. When the
// purge high-water mark has caught up with the read timestamp there are no
// expired records left in the tree, so the raw iterator is returned without
// any per-record filtering at all. Only while physical cleanup is lagging do
// expired records pay a single int64 comparison each.
func (txn *Txn) wrapTTL(table string, iter ResultIterator) ResultIterator {
	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok || tableSchema.TTL == nil {
		return iter
	}
	now := txn.nowNanos()
	if now <= txn.watermark(table) {
		return iter
	}
	return &expiryIterator{iter: iter, now: now}
}

// isExpired reports whether an object is expired at the transaction's read
// timestamp. It is used on point lookups where there is no iterator to wrap.
func (txn *Txn) isExpired(obj interface{}) bool {
	expiry, ok := expiryOf(obj)
	return ok && expiry != 0 && expiry <= txn.nowNanos()
}

// PurgeExpired physically removes objects whose expiry is at or before the
// transaction's read timestamp. It is meant to be driven by a periodic
// background job, decoupled from the read path, which already hides expired
// objects lazily.
//
// limit bounds the number of objects removed in a single call; a limit <= 0
// removes every currently expired object. Deletions are batched into one
// transaction and the internal expiry index is ordered by timestamp, so the
// work scans only the expired prefix instead of the whole table. The returned
// count is the number of objects removed.
//
// On commit the table's purge high-water mark advances to the read timestamp
// only when the scan was not cut short by limit. Advancing the mark while
// expired records may still remain would let later reads skip their
// per-record expiry checks, so a limited run simply leaves the mark where it
// was and lets the next invocation continue.
func (txn *Txn) PurgeExpired(table string, limit int) (int, error) {
	if !txn.write {
		return 0, fmt.Errorf("cannot purge in read-only transaction")
	}

	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok {
		return 0, fmt.Errorf("invalid table '%s'", table)
	}
	if tableSchema.TTL == nil {
		return 0, fmt.Errorf("table '%s' does not have TTL enabled", table)
	}

	now := txn.nowNanos()
	if now <= txn.watermark(table) {
		// A previous purge already covered everything expiring before now.
		return 0, nil
	}

	// Walk the expiry index from the oldest entry. Keys are ordered by expiry
	// prefix, so the scan stops as soon as it reaches a still-live timestamp.
	indexTxn := txn.readableIndex(table, ttlIndexName)
	rootIter := indexTxn.Root().Iterator()
	rootIter.SeekLowerBound(encodeExpiryPrefix(0))

	var expired []interface{}
	for {
		_, value, found := rootIter.Next()
		if !found {
			break
		}
		expiry, _ := expiryOf(value)
		if expiry == 0 || expiry > now {
			break
		}
		expired = append(expired, value)
		if limit > 0 && len(expired) >= limit {
			break
		}
	}

	// Materialize before mutating because deleting invalidates the snapshot an
	// open iterator would otherwise rely on.
	for _, obj := range expired {
		if err := txn.Delete(table, obj); err != nil {
			return len(expired), err
		}
	}

	complete := limit <= 0 || len(expired) < limit
	if complete {
		if txn.purgeMarks == nil {
			txn.purgeMarks = make(map[string]int64)
		}
		if now > txn.purgeMarks[table] {
			txn.purgeMarks[table] = now
		}
	}
	return len(expired), nil
}
