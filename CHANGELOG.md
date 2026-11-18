## Unreleased

### Improvements

* Add `Txn.GetMulti` to query multiple indexes in a single call and return
  the de-duplicated union of their results as a streaming iterator, without
  materializing the full result set in memory.

### Changes

### Fixed

### Security
