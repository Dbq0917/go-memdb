// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"context"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/hashicorp/go-immutable-radix"
)

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

	// There can only be a single writer at once. The lock is acquired lazily
	// by write transactions on their first mutation, so read-only activity
	// (including long-lived snapshots) never blocks writers.
	writer *writerLock

	// registry tracks open write transactions so long-running ones can be
	// identified.
	registry *txnRegistry

	// MaxLockWait bounds how long a write transaction waits for the writer
	// lock during its first mutation before that mutation returns
	// ErrWriteLockTimeout. Zero means wait forever, which preserves the
	// historical behavior.
	MaxLockWait time.Duration

	// MaxTxnAge bounds the age of a write transaction at the moment it
	// performs its first mutation. An older transaction fails with
	// ErrTxnTooOld instead of taking the writer lock. Zero disables the
	// check.
	MaxTxnAge time.Duration

	// holdStart tracks when the current holder acquired the writer lock, so
	// hold time can be reported without taking the registry mutex. Zero
	// means the lock is free.
	holdStart unsafe.Pointer // *time.Time
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(schema *DBSchema) (*MemDB, error) {
	return NewMemDBWithOptions(schema, nil)
}

// Options configures optional MemDB behavior. A nil Options or zero-value
// fields preserve the historical defaults (unbounded lock waits and no
// transaction age limit).
type Options struct {
	// MaxLockWait bounds how long a write transaction waits to acquire the
	// writer lock. Zero waits indefinitely.
	MaxLockWait time.Duration

	// MaxTxnAge bounds how long after a write transaction was created it is
	// still allowed to perform its first mutation. Zero disables the limit.
	MaxTxnAge time.Duration
}

// NewMemDBWithOptions creates a new MemDB with the given schema and options.
func NewMemDBWithOptions(schema *DBSchema, opts *Options) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema:   schema,
		root:     unsafe.Pointer(iradix.New()),
		primary:  true,
		writer:   newWriterLock(),
		registry: newTxnRegistry(),
	}
	if opts != nil {
		db.MaxLockWait = opts.MaxLockWait
		db.MaxTxnAge = opts.MaxTxnAge
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
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
//
// For write transactions the single-writer lock is acquired lazily on the
// first mutation rather than here, so opening a write transaction and reading
// from it does not block other writers.
func (db *MemDB) Txn(write bool) *Txn {
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
		ctx:     context.Background(),
	}
	if write {
		txn.started = time.Now()
		txn.id = db.registry.register(txn, txn.started)
	}
	return txn
}

// TxnCtx is like Txn but binds a context to a write transaction. The context
// applies to the single-writer lock acquisition on the first mutation: if the
// context is canceled, or the DB's MaxLockWait elapses before the lock is
// acquired, the mutation returns ErrWriteLockTimeout and the transaction is
// aborted. The context has no effect on read transactions or on mutations
// once the lock has been acquired.
func (db *MemDB) TxnCtx(ctx context.Context, write bool) *Txn {
	if ctx == nil {
		ctx = context.Background()
	}
	txn := db.Txn(write)
	txn.ctx = ctx
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
		schema:      db.schema,
		root:        unsafe.Pointer(db.getRoot()),
		primary:     false,
		writer:      newWriterLock(),
		registry:    newTxnRegistry(),
		MaxLockWait: db.MaxLockWait,
		MaxTxnAge:   db.MaxTxnAge,
	}
	return clone
}

// WriteTxns returns information about every currently open write transaction,
// ordered oldest first. It is intended for monitoring and for identifying
// long-running transactions that may be holding (or waiting on) the writer
// lock.
func (db *MemDB) WriteTxns() []WriteTxnInfo {
	return db.registry.snapshot(time.Now())
}

// OldestWriteTxnAge returns the age of the oldest open write transaction, or
// zero if no write transaction is open.
func (db *MemDB) OldestWriteTxnAge() time.Duration {
	return db.registry.oldestAge(time.Now())
}

// WriterLockStats returns point-in-time contention metrics for the
// single-writer lock, including current hold time and cumulative waits.
func (db *MemDB) WriterLockStats() WriterLockStats {
	var hold time.Duration
	if p := (*time.Time)(atomic.LoadPointer(&db.holdStart)); p != nil {
		hold = time.Since(*p)
	}
	return db.writer.stats(hold)
}

// setHoldStart updates the current lock-hold timestamp atomically.
func (db *MemDB) setHoldStart(t time.Time) {
	atomic.StorePointer(&db.holdStart, unsafe.Pointer(&t))
}

// clearHoldStart marks the writer lock as free.
func (db *MemDB) clearHoldStart() {
	atomic.StorePointer(&db.holdStart, nil)
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
