// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// readerShardCount controls how reader registrations are distributed. Reader
// open and close operations only contend within one shard, so opening a read
// transaction does not serialize all readers on a single global lock.
const readerShardCount = 64

// ReaderInfo describes a read transaction that is currently holding a
// snapshot of the database open. It is a point-in-time view returned by
// MemDB.OldestReader and MemDB.ActiveReaders.
type ReaderInfo struct {
	// ID is the transaction's identifier. It matches Txn.ID.
	ID uint64

	// Started is when the read transaction was created.
	Started time.Time

	// Age is how long the read transaction has been open at the time the
	// information was collected.
	Age time.Duration
}

// OpenFor returns the age of the reader when the information was collected.
func (info ReaderInfo) OpenFor() time.Duration {
	return info.Age
}

// readerRegistry tracks the metadata of open read transactions. It
// deliberately does not retain *Txn values or radix roots: doing so would
// make the registry itself keep every old database version alive.
type readerRegistry struct {
	nextID atomic.Uint64
	shards [readerShardCount]readerShard
}

type readerShard struct {
	mu      sync.Mutex
	readers map[uint64]readerEntry
}

type readerEntry struct {
	started time.Time
	witness readerWitness
}

func (r *readerRegistry) register(txn *Txn, started time.Time) uint64 {
	id := r.nextID.Add(1)
	shard := &r.shards[id%readerShardCount]
	witness := newReaderWitness(r, id, txn)

	shard.mu.Lock()
	if shard.readers == nil {
		shard.readers = make(map[uint64]readerEntry)
	}
	shard.readers[id] = readerEntry{started: started, witness: witness}
	shard.mu.Unlock()

	return id
}

func (r *readerRegistry) unregisterID(id uint64) {
	shard := &r.shards[id%readerShardCount]
	shard.mu.Lock()
	delete(shard.readers, id)
	if len(shard.readers) == 0 {
		// Release the map after a burst of readers drains.
		shard.readers = nil
	}
	shard.mu.Unlock()
}

func (r *readerRegistry) oldest(now time.Time) (ReaderInfo, bool) {
	var oldest ReaderInfo
	var found bool

	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.Lock()
		for id, entry := range shard.readers {
			if !entry.witness.alive() {
				delete(shard.readers, id)
				continue
			}
			started := entry.started
			if !found || started.Before(oldest.Started) ||
				(started.Equal(oldest.Started) && id < oldest.ID) {
				oldest = ReaderInfo{
					ID:      id,
					Started: started,
					Age:     nonNegativeDuration(now.Sub(started)),
				}
				found = true
			}
		}
		shard.mu.Unlock()
	}

	return oldest, found
}

func (r *readerRegistry) snapshot(now time.Time) []ReaderInfo {
	var infos []ReaderInfo

	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.Lock()
		for id, entry := range shard.readers {
			if !entry.witness.alive() {
				delete(shard.readers, id)
				continue
			}
			started := entry.started
			infos = append(infos, ReaderInfo{
				ID:      id,
				Started: started,
				Age:     nonNegativeDuration(now.Sub(started)),
			})
		}
		shard.mu.Unlock()
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Started.Equal(infos[j].Started) {
			return infos[i].ID < infos[j].ID
		}
		return infos[i].Started.Before(infos[j].Started)
	})
	return infos
}

func (r *readerRegistry) count() int {
	var count int
	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.Lock()
		for id, entry := range shard.readers {
			if !entry.witness.alive() {
				delete(shard.readers, id)
			}
		}
		count += len(shard.readers)
		shard.mu.Unlock()
	}
	return count
}

func nonNegativeDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

// startReader registers a read transaction and arranges for it to be
// unregistered if the caller drops it without calling Abort or Commit.
func (txn *Txn) startReader() {
	txn.started = time.Now()
	txn.id = txn.db.readers.register(txn, txn.started)
	txn.readerOpen = true
}

func (txn *Txn) closeReader() {
	clearReaderWitness(txn)
	txn.releaseReader()
	txn.rootTxn = nil
	txn.modified = nil
}

func (txn *Txn) releaseReader() {
	if !txn.readerOpen {
		return
	}

	txn.db.readers.unregisterID(txn.id)
	txn.readerOpen = false
}
