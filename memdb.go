// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"sort"
	"sync"
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

	// There can only be a single writer at once
	writer sync.Mutex

	// txns tracks all currently open transactions so that long-running
	// transactions can be identified and inspected. Read transactions hold
	// no locks and never block writers, so they are tracked purely for
	// observability. Write transactions hold the writer lock, so they can
	// additionally be bounded by a timeout.
	txnsMu          sync.Mutex
	txns            map[uint64]*Txn
	lastTxn         uint64
	writeTxnTimeout atomic.Int64 // nanoseconds, 0 means no timeout
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(schema *DBSchema) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema:  schema,
		root:    unsafe.Pointer(iradix.New()),
		primary: true,
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
func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
		start:   time.Now(),
	}
	db.registerTxn(txn)
	return txn
}

// TxnInfo describes a transaction that is currently open against the
// database. It is a point-in-time snapshot returned by ActiveTxns and
// LongRunningTxns.
type TxnInfo struct {
	// ID uniquely identifies the transaction for the lifetime of the
	// process. IDs are monotonically increasing.
	ID uint64

	// Write is true for write transactions. Write transactions hold the
	// single writer lock and therefore block other writers.
	Write bool

	// Start is the time at which the transaction was created.
	Start time.Time
}

// Age returns how long the transaction has been open.
func (info TxnInfo) Age() time.Duration {
	return time.Since(info.Start)
}

// ActiveTxns returns information about every transaction that is currently
// open against the database, oldest first. This can be used to identify
// long-running transactions, for example to drive metrics or logging.
func (db *MemDB) ActiveTxns() []TxnInfo {
	db.txnsMu.Lock()
	defer db.txnsMu.Unlock()

	infos := make([]TxnInfo, 0, len(db.txns))
	for id, txn := range db.txns {
		infos = append(infos, TxnInfo{
			ID:    id,
			Write: txn.write,
			Start: txn.start,
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Start.Before(infos[j].Start)
	})
	return infos
}

// LongRunningTxns returns information about every open transaction that has
// been alive for at least minAge, oldest first. Since read transactions in
// MemDB hold no locks and never block writers, a long-running read
// transaction (such as a report or backup) is generally harmless and only
// worth surfacing for observability. A long-running write transaction holds
// the single writer lock and blocks all other writers; those can be bounded
// with SetWriteTxnTimeout.
func (db *MemDB) LongRunningTxns(minAge time.Duration) []TxnInfo {
	var out []TxnInfo
	for _, info := range db.ActiveTxns() {
		if info.Age() >= minAge {
			out = append(out, info)
		}
	}
	return out
}

// SetWriteTxnTimeout bounds how long a write transaction may stay open.
// Write transactions hold the single writer lock, so an unbounded write
// transaction stalls all other writers. Once a write transaction exceeds
// the timeout, the next mutating operation or Commit fails with
// ErrTxnTimeout and the transaction is aborted, releasing the writer lock.
//
// The timeout does not apply to read transactions: they hold no locks, run
// against an immutable snapshot, and never block writers, so long-running
// readers (reports, backups) are allowed to run to completion.
//
// A zero or negative timeout disables the check, which is the default.
func (db *MemDB) SetWriteTxnTimeout(timeout time.Duration) {
	db.writeTxnTimeout.Store(int64(timeout))
}

// WriteTxnTimeout returns the currently configured write transaction
// timeout. A zero value means no timeout is enforced.
func (db *MemDB) WriteTxnTimeout() time.Duration {
	return time.Duration(db.writeTxnTimeout.Load())
}

// registerTxn records an open transaction so it can be inspected via
// ActiveTxns. The transaction is removed when it is committed or aborted.
func (db *MemDB) registerTxn(txn *Txn) {
	db.txnsMu.Lock()
	defer db.txnsMu.Unlock()

	if db.txns == nil {
		db.txns = make(map[uint64]*Txn)
	}
	db.lastTxn++
	txn.id = db.lastTxn
	db.txns[txn.id] = txn
}

// unregisterTxn removes a transaction from the open transaction registry.
func (db *MemDB) unregisterTxn(txn *Txn) {
	db.txnsMu.Lock()
	defer db.txnsMu.Unlock()

	delete(db.txns, txn.id)
}

// Snapshot is used to capture a point-in-time snapshot  of the database that
// will not be affected by any write operations to the existing DB.
//
// If MemDB is storing reference-based values (pointers, maps, slices, etc.),
// the Snapshot will not deep copy those values. Therefore, it is still unsafe
// to modify any inserted values in either DB.
func (db *MemDB) Snapshot() *MemDB {
	clone := &MemDB{
		schema:  db.schema,
		root:    unsafe.Pointer(db.getRoot()),
		primary: false,
	}
	clone.writeTxnTimeout.Store(db.writeTxnTimeout.Load())
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
