## Unreleased

### Improvements

### Changes

### Fixed

* Re-check unique index constraints atomically at commit. A write transaction
  that would overwrite a still-visible row in a unique index now aborts with
  `ErrUniqueConstraint` instead of publishing a partial or duplicate write.

### Security
