## Unreleased

### Improvements

* Add `MemDB.AddIndex` to add an index to an existing table online. The new
  index is backfilled from existing records without blocking readers; queries
  against it fail with `ErrIndexNotReady` until the backfill completes.

### Changes

### Fixed

### Security
