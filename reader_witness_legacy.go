// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

//go:build !go1.24

package memdb

import "runtime"

// On older Go versions weak pointers are unavailable. The finalizer only
// removes the registry entry after the transaction is unreachable; it never
// aborts a live reader.
type readerWitness struct{}

func newReaderWitness(registry *readerRegistry, id uint64, txn *Txn) readerWitness {
	runtime.SetFinalizer(txn, func(*Txn) {
		registry.unregisterID(id)
	})
	return readerWitness{}
}

func (readerWitness) alive() bool {
	return true
}

func clearReaderWitness(txn *Txn) {
	runtime.SetFinalizer(txn, nil)
}
