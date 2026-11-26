## Unreleased

### Improvements

* Track open read transactions with striped, low-contention registration.
  `MemDB.OldestReader`, `MemDB.ActiveReaders`, `MemDB.OldestReaderAge`, and
  `Txn.ID` expose which reader is holding a snapshot and for how long.
  Closing a reader releases its snapshot root immediately. Readers dropped
  without `Abort` or `Commit` are retired via weak references on Go 1.24+
  (with a finalizer fallback on older toolchains).

### Changes

### Fixed

### Security
