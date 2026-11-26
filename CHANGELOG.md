## Unreleased

### Improvements

### Changes

### Fixed

* `Txn.Insert` no longer leaves indexes partly applied when it fails. Index
  keys are now computed and validated for every index before anything is
  written, so a failed insert or update leaves all of the table's indexes
  untouched instead of differing from index to index.

### Security
