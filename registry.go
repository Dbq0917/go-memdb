// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"sort"
	"sync"
	"time"
)

// WriteTxnInfo describes a live write transaction. It is a point-in-time
// snapshot used for identifying long-running transactions that can stall
// writers.
type WriteTxnInfo struct {
	// ID uniquely identifies the transaction for its lifetime.
	ID uint64

	// Started is when the transaction was created.
	Started time.Time

	// Age is how long the transaction has been open.
	Age time.Duration

	// Mutating reports whether the transaction has performed its first
	// mutation and therefore holds the writer lock.
	Mutating bool

	// LockHoldTime is how long the transaction has held the writer lock.
	// It is zero when Mutating is false.
	LockHoldTime time.Duration
}

// txnEntry tracks a single open write transaction.
type txnEntry struct {
	id       uint64
	started  time.Time
	lockedAt time.Time // zero until the writer lock is acquired
}

// txnRegistry keeps track of open write transactions so long-running ones can
// be identified and constrained. All access is guarded by mu.
type txnRegistry struct {
	mu     sync.Mutex
	nextID uint64
	txns   map[*Txn]*txnEntry
}

func newTxnRegistry() *txnRegistry {
	return &txnRegistry{txns: make(map[*Txn]*txnEntry)}
}

// register records a newly opened write transaction and returns its ID.
func (r *txnRegistry) register(txn *Txn, started time.Time) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	entry := &txnEntry{id: r.nextID, started: started}
	r.txns[txn] = entry
	return entry.id
}

// markLocked records that the transaction now holds the writer lock.
func (r *txnRegistry) markLocked(txn *Txn, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.txns[txn]; ok {
		entry.lockedAt = at
	}
}

// unregister removes a transaction that has committed or aborted.
func (r *txnRegistry) unregister(txn *Txn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.txns, txn)
}

// snapshot returns introspection info for every open write transaction. The
// oldest transaction is first.
func (r *txnRegistry) snapshot(now time.Time) []WriteTxnInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	infos := make([]WriteTxnInfo, 0, len(r.txns))
	for _, entry := range r.txns {
		info := WriteTxnInfo{
			ID:      entry.id,
			Started: entry.started,
			Age:     now.Sub(entry.started),
		}
		if !entry.lockedAt.IsZero() {
			info.Mutating = true
			info.LockHoldTime = now.Sub(entry.lockedAt)
		}
		infos = append(infos, info)
	}

	// Oldest transaction first; callers care most about the long-running ones.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Started.Before(infos[j].Started)
	})
	return infos
}

// oldestAge returns the age of the oldest open write transaction, or zero if
// there are none.
func (r *txnRegistry) oldestAge(now time.Time) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	var oldest time.Time
	for _, entry := range r.txns {
		started := entry.started
		if started.IsZero() {
			continue
		}
		if oldest.IsZero() || started.Before(oldest) {
			oldest = started
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return now.Sub(oldest)
}
