## Unreleased

### Improvements

### Changes

### Fixed

* Guarantee that aborting a write transaction rolls back every staged index
  mutation (insert, update, delete and prefix delete) across primary,
  secondary and multi-value indexes. The rollback discards the pending
  copy-on-write index transactions instead of applying inverse writes, so the
  commit path retains its single-pass index updates and no watch
  notifications are emitted for aborted transactions. Regression tests now
  assert index-wide recovery after abort.

### Security
