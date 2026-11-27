// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"fmt"
	"sync"
	"sync/atomic"
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
	state   unsafe.Pointer // *dbState underneath
	primary bool

	// There can only be a single writer at once
	writer sync.Mutex

	// schemaMu serializes online schema changes (AddIndex) against each
	// other. Regular write transactions only need the writer lock.
	schemaMu sync.Mutex
}

// dbState bundles the schema and the root of the index trees so that both
// are always swapped atomically: readers observe either the old pair or the
// new pair, never a mix of the two. A dbState must never be modified once
// stored; updates replace it wholesale.
type dbState struct {
	schema *DBSchema
	root   *iradix.Tree
}

// ErrIndexNotReady is returned when querying an index that has been
// registered by AddIndex but whose backfill from existing records has not
// completed yet. It is returned instead of serving partial or empty results
// from the incomplete index.
var ErrIndexNotReady = fmt.Errorf("index not ready")

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(schema *DBSchema) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		primary: true,
	}
	db.state = unsafe.Pointer(&dbState{
		schema: schema,
		root:   iradix.New(),
	})
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
	return db.getState().schema
}

// getState is used to do an atomic load of the state pointer
func (db *MemDB) getState() *dbState {
	return (*dbState)(atomic.LoadPointer(&db.state))
}

// setState is used to do an atomic store of the state pointer. Callers must
// hold the writer lock so that concurrent writers cannot lose each other's
// updates.
func (db *MemDB) setState(state *dbState) {
	atomic.StorePointer(&db.state, unsafe.Pointer(state))
}

// Txn is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	state := db.getState()
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: state.root.Txn(),
		schema:  state.schema,
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
		primary: false,
	}
	// The state is immutable once stored, so the snapshot can safely share
	// the same bundle; later commits on the primary DB replace the primary's
	// state pointer and never touch this one.
	clone.state = unsafe.Pointer(db.getState())
	return clone
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initialize() error {
	state := db.getState()
	root := state.root
	for tName, tableSchema := range state.schema.Tables {
		for iName := range tableSchema.Indexes {
			index := iradix.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.state = unsafe.Pointer(&dbState{schema: state.schema, root: root})
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}

// AddIndex adds a new index to an existing table without requiring the
// database to be rebuilt. The new index is backfilled from the records
// already stored in the table and is fully usable once AddIndex returns.
//
// The add is performed online:
//
//   - Read transactions are never blocked. While the backfill is in
//     progress, queries against any other index behave as before, and
//     queries against the new index fail with ErrIndexNotReady rather than
//     returning partial or empty results.
//   - Write transactions started after the index is registered maintain the
//     new index as usual, so no writes are lost to the backfill. Writers are
//     only blocked while the backfill transaction itself runs.
//
// The schema is updated copy-on-write: the DBSchema passed to NewMemDB (or
// previously returned by DBSchema) is never modified in-place, and DBSchema
// returns the new schema once the index is registered.
func (db *MemDB) AddIndex(table string, index *IndexSchema) error {
	if index == nil {
		return fmt.Errorf("index schema is nil")
	}

	// Serialize against other concurrent schema changes.
	db.schemaMu.Lock()
	defer db.schemaMu.Unlock()

	if err := db.registerIndex(table, index); err != nil {
		return err
	}
	if err := db.backfillIndex(table, index.Name); err != nil {
		// Roll back the registration so the index doesn't linger in a
		// permanently not-ready state.
		db.removeIndex(table, index.Name)
		return err
	}
	return db.markIndexReady(table, index.Name)
}

// registerIndex installs the index in the schema (marked as pending) and
// attaches an empty index tree to the root so that subsequent write
// transactions maintain the index while it is being backfilled. The new
// state is swapped in atomically as a whole.
func (db *MemDB) registerIndex(table string, index *IndexSchema) error {
	if err := index.Validate(); err != nil {
		return fmt.Errorf("table %q: %s", table, err)
	}

	db.writer.Lock()
	defer db.writer.Unlock()

	state := db.getState()
	tableSchema, ok := state.schema.Tables[table]
	if !ok {
		return fmt.Errorf("invalid table '%s'", table)
	}
	if _, ok := tableSchema.Indexes[index.Name]; ok {
		return fmt.Errorf("index '%s' already exists on table '%s'", index.Name, table)
	}

	// Copy the table schema, adding the new index and marking it pending.
	// The original schema is left untouched.
	newTable := *tableSchema
	newTable.Indexes = make(map[string]*IndexSchema, len(tableSchema.Indexes)+1)
	for name, idx := range tableSchema.Indexes {
		newTable.Indexes[name] = idx
	}
	newTable.Indexes[index.Name] = index
	newTable.pending = make(map[string]struct{}, len(tableSchema.pending)+1)
	for name := range tableSchema.pending {
		newTable.pending[name] = struct{}{}
	}
	newTable.pending[index.Name] = struct{}{}

	// Attach an empty tree for the new index. The tree is only ever
	// published as part of a whole new root, so readers never observe a
	// partially built state: until the backfill completes, the pending
	// marker makes queries against this index fail with ErrIndexNotReady.
	newRoot, _, _ := state.root.Insert(indexPath(table, index.Name), iradix.New())

	db.setState(&dbState{
		schema: copySchemaWithTable(state.schema, table, &newTable),
		root:   newRoot,
	})
	return nil
}

// backfillIndex populates a previously registered index from the records
// already stored in the table. The backfill runs as a single write
// transaction, so it never blocks readers and commits atomically.
func (db *MemDB) backfillIndex(table, index string) error {
	txn := db.Txn(true)
	defer txn.Abort()

	tableSchema := txn.schema.Tables[table]
	indexSchema := tableSchema.Indexes[index]
	idIndexer := tableSchema.Indexes[id].Indexer.(SingleIndexer)

	indexTxn := txn.writableIndex(table, index)
	idTxn := txn.readableIndex(table, id)
	iter := idTxn.Root().Iterator()
	for {
		_, obj, ok := iter.Next()
		if !ok {
			break
		}

		// Get the primary ID of the object, which is needed to compute
		// unique keys for non-unique indexes.
		ok, idVal, err := idIndexer.FromObject(obj)
		if err != nil {
			return fmt.Errorf("failed to build primary index: %v", err)
		}
		if !ok {
			return fmt.Errorf("object missing primary index")
		}

		ok, vals, err := indexKeys(indexSchema, idVal, obj)
		if err != nil {
			return fmt.Errorf("failed to build index '%s': %v", index, err)
		}
		if !ok {
			if indexSchema.AllowMissing {
				continue
			}
			return fmt.Errorf("missing value for index '%s'", index)
		}
		for _, val := range vals {
			indexTxn.Insert(val, obj)
		}
	}

	txn.Commit()
	return nil
}

// markIndexReady clears the pending marker of a backfilled index so that
// queries against it are served. The root is carried over unchanged.
func (db *MemDB) markIndexReady(table, index string) error {
	db.writer.Lock()
	defer db.writer.Unlock()

	state := db.getState()
	tableSchema, ok := state.schema.Tables[table]
	if !ok {
		return fmt.Errorf("invalid table '%s'", table)
	}
	if _, ok := tableSchema.pending[index]; !ok {
		return fmt.Errorf("index '%s' on table '%s' is not pending", index, table)
	}

	newTable := *tableSchema
	newTable.Indexes = tableSchema.Indexes
	newTable.pending = nil
	if len(tableSchema.pending) > 1 {
		newTable.pending = make(map[string]struct{}, len(tableSchema.pending)-1)
		for name := range tableSchema.pending {
			if name != index {
				newTable.pending[name] = struct{}{}
			}
		}
	}

	db.setState(&dbState{
		schema: copySchemaWithTable(state.schema, table, &newTable),
		root:   state.root,
	})
	return nil
}

// removeIndex drops an index from the schema and removes its tree from the
// root. It is used to roll back a failed AddIndex.
func (db *MemDB) removeIndex(table, index string) {
	db.writer.Lock()
	defer db.writer.Unlock()

	state := db.getState()
	tableSchema, ok := state.schema.Tables[table]
	if !ok {
		return
	}
	if _, ok := tableSchema.Indexes[index]; !ok {
		return
	}

	newTable := *tableSchema
	newTable.Indexes = make(map[string]*IndexSchema, len(tableSchema.Indexes)-1)
	for name, idx := range tableSchema.Indexes {
		if name != index {
			newTable.Indexes[name] = idx
		}
	}
	newTable.pending = nil
	for name := range tableSchema.pending {
		if name != index {
			if newTable.pending == nil {
				newTable.pending = make(map[string]struct{}, len(tableSchema.pending)-1)
			}
			newTable.pending[name] = struct{}{}
		}
	}

	newRoot, _, _ := state.root.Delete(indexPath(table, index))

	db.setState(&dbState{
		schema: copySchemaWithTable(state.schema, table, &newTable),
		root:   newRoot,
	})
}

// copySchemaWithTable returns a copy of schema with the given table replaced
// by newTable. The original schema is left untouched.
func copySchemaWithTable(schema *DBSchema, table string, newTable *TableSchema) *DBSchema {
	newSchema := &DBSchema{Tables: make(map[string]*TableSchema, len(schema.Tables))}
	for name, tableSchema := range schema.Tables {
		newSchema.Tables[name] = tableSchema
	}
	newSchema.Tables[table] = newTable
	return newSchema
}

// indexKeys computes the index keys for obj according to the index schema.
// For non-unique indexes the primary key is appended to make each key
// unique, matching the behavior of Insert.
func indexKeys(indexSchema *IndexSchema, idVal []byte, obj interface{}) (bool, [][]byte, error) {
	var (
		ok   bool
		vals [][]byte
		err  error
	)
	switch indexer := indexSchema.Indexer.(type) {
	case SingleIndexer:
		var val []byte
		ok, val, err = indexer.FromObject(obj)
		vals = [][]byte{val}
	case MultiIndexer:
		ok, vals, err = indexer.FromObject(obj)
	}
	if err != nil {
		return false, nil, err
	}
	if ok && !indexSchema.Unique {
		for i := range vals {
			vals[i] = append(vals[i], idVal...)
		}
	}
	return ok, vals, nil
}
