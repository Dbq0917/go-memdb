// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/hashicorp/go-immutable-radix"
)

// Option customizes a MemDB at construction time.
type Option func(*MemDB)

// WithClock overrides the wall clock used for expiry decisions. It is primarily
// useful for deterministic tests; production callers use the real time clock.
func WithClock(now func() time.Time) Option {
	return func(db *MemDB) {
		db.nowFn = now
	}
}

// MemDB is an in-memory database providing Atomicity, Consistency, and
// Isolation from ACID. MemDB doesn't provide Durability since it is an
// in-memory database.
//
// MemDB provides a table abstraction to store objects (rows) with multiple
// indexes based on inserted values. The database makes use of immutable radix
// trees to provide transactions and MVCC.
//
// Objects inserted into MemDB are not copied. It is **extremely important**
// that objects are not modified in-place after they are inserted since they
// are stored directly in MemDB. It remains unsafe to modify inserted objects
// even after they've been deleted from MemDB since there may still be older
// snapshots of the DB being read from other goroutines.
type MemDB struct {
	schema  *DBSchema
	root    unsafe.Pointer // *iradix.Tree underneath
	primary bool

	// nowFn returns the current wall time. It defaults to time.Now and can be
	// replaced through WithClock.
	nowFn func() time.Time

	// watermarks maps a table name to the purge high-water mark. The map is
	// treated as immutable once published and replaced atomically.
	watermarks unsafe.Pointer // map[string]int64

	// There can only be a single writer at once
	writer sync.Mutex
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(schema *DBSchema, opts ...Option) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Install the reserved expiry index on every TTL table. The schema is
	// copied so user supplied maps are never mutated.
	schema = withTTLIndexes(schema)

	// Create the MemDB
	db := &MemDB{
		schema:  schema,
		root:    unsafe.Pointer(iradix.New()),
		primary: true,
		nowFn:   time.Now,
	}
	for _, opt := range opts {
		opt(db)
	}
	if db.nowFn == nil {
		db.nowFn = time.Now
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
}

// withTTLIndexes returns a deep-copied schema in which every TTL table carries
// the reserved internal expiry index.
func withTTLIndexes(schema *DBSchema) *DBSchema {
	clone := &DBSchema{Tables: make(map[string]*TableSchema, len(schema.Tables))}
	for name, table := range schema.Tables {
		tableCopy := &TableSchema{
			Name:    table.Name,
			TTL:     table.TTL,
			Indexes: make(map[string]*IndexSchema, len(table.Indexes)+1),
		}
		for indexName, indexSchema := range table.Indexes {
			tableCopy.Indexes[indexName] = indexSchema
		}
		if table.TTL != nil {
			tableCopy.Indexes[ttlIndexName] = &IndexSchema{
				Name:         ttlIndexName,
				AllowMissing: true,
				Unique:       false,
				Indexer:      &ttlIndexer{},
			}
		}
		clone.Tables[name] = tableCopy
	}
	return clone
}

// loadWatermarks atomically loads the current purge high-water mark map.
func (db *MemDB) loadWatermarks() map[string]int64 {
	ptr := atomic.LoadPointer(&db.watermarks)
	if ptr == nil {
		return map[string]int64{}
	}
	return *(*map[string]int64)(ptr)
}

// advanceWatermarks publishes merged purge high-water marks. It is invoked once
// per committing purge transaction, after the new tree root is published.
func (db *MemDB) advanceWatermarks(updates map[string]int64) {
	if len(updates) == 0 {
		return
	}
	merged := db.loadWatermarks()
	next := make(map[string]int64, len(merged)+len(updates))
	for table, mark := range merged {
		next[table] = mark
	}
	for table, mark := range updates {
		if mark > next[table] {
			next[table] = mark
		}
	}
	atomic.StorePointer(&db.watermarks, unsafe.Pointer(&next))
}

// DBSchema returns schema in use for introspection.
//
// The method is intended for *read-only* debugging use cases,
// returned schema should *never be modified in-place*.
func (db *MemDB) DBSchema() *DBSchema {
	return db.schema
}

// getRoot is used to do an atomic load of the root pointer
func (db *MemDB) getRoot() *iradix.Tree {
	root := (*iradix.Tree)(atomic.LoadPointer(&db.root))
	return root
}

// Txn is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
	}
	return txn
}

// Snapshot is used to capture a point-in-time snapshot  of the database that
// will not be affected by any write operations to the existing DB.
//
// If MemDB is storing reference-based values (pointers, maps, slices, etc.),
// the Snapshot will not deep copy those values. Therefore, it is still unsafe
// to modify any inserted values in either DB.
func (db *MemDB) Snapshot() *MemDB {
	clone := &MemDB{
		schema:     db.schema,
		root:       unsafe.Pointer(db.getRoot()),
		primary:    false,
		nowFn:      db.nowFn,
		watermarks: db.watermarks,
	}
	return clone
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initialize() error {
	root := db.getRoot()
	for tName, tableSchema := range db.schema.Tables {
		for iName := range tableSchema.Indexes {
			index := iradix.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.root = unsafe.Pointer(root)
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
