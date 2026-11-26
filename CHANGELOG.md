## Unreleased

### Improvements

* Add optional table relations (`TableSchema.Relations`) so deleting a row can
  either cascade to referencing rows or block the delete with
  `ErrReferenceExists`. Tables without relations keep their existing behavior.

### Changes

### Fixed

### Security
