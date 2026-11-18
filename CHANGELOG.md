## Unreleased

### Improvements

* Add `MemDB.ActiveTxns` and `MemDB.LongRunningTxns` to identify long-running
  transactions, and `Txn.ID` to correlate them with logs and metrics.
* Add `MemDB.SetWriteTxnTimeout` to bound how long a write transaction may
  hold the writer lock. Timed-out write transactions fail with
  `ErrTxnTimeout` and are aborted, releasing the lock for other writers.
  Read transactions are never timed out since they hold no locks and do not
  block writers.

### Changes

### Fixed

### Security
