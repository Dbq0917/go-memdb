// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

//go:build go1.24

package memdb

import "weak"

// readerWitness uses a weak pointer so the registry can see that a dropped
// read transaction is gone without keeping it (or its MVCC root) alive.
type readerWitness struct {
	txn weak.Pointer[Txn]
}

func newReaderWitness(_ *readerRegistry, _ uint64, txn *Txn) readerWitness {
	return readerWitness{txn: weak.Make(txn)}
}

func (w readerWitness) alive() bool {
	return w.txn.Value() != nil
}

func clearReaderWitness(_ *Txn) {}
