# Performance and Best Practices

**Files:** `include/rocksdb/utilities/transaction_db.h`, `include/rocksdb/utilities/optimistic_transaction_db.h`, `include/rocksdb/utilities/transaction.h`

## Write Policy Selection

| Workload | Recommended Policy | Reason |
|----------|-------------------|--------|
| General purpose | WriteCommitted | Simplest, most mature, supports all features |
| Low-latency 2PC commits | WritePrepared | Common-case commit writes WAL marker only; prepare does the heavy lifting. If a commit-time batch is present, commit may also write data. |
| Very large transactions (GBs) | WriteUnprepared | Flushes data incrementally; no memory bottleneck |
| Needs user-defined timestamps | WriteCommitted | Only policy that supports UDT |

**WritePrepared benchmarks** (from original design, via MyRocks):
- Insert throughput: +68%
- Update-with-index throughput: +61%, latency: -28%
- Read-write throughput: +6%
- Read-only throughput: -1.2% (due to IsInSnapshot overhead)

## Optimistic vs. Pessimistic

| Factor | Threshold | Recommendation |
|--------|-----------|----------------|
| Conflict rate < 10% | Low conflict | Optimistic likely faster (no lock overhead) |
| Conflict rate > 50% | High conflict | Pessimistic likely faster (no wasted work) |
| 10-50% | Medium conflict | Benchmark both |

For optimistic transactions, prefer `kValidateParallel` (default) over `kValidateSerial` to reduce write group contention.

## Lock Manager Tuning

### Point Lock Manager

| Option | Default | Tuning |
|--------|---------|--------|
| `num_stripes` | 16 | Increase to 64+ for high concurrency; decrease to 4-8 if memory-constrained |
| `max_num_locks` | -1 (unlimited) | Set positive to prevent lock table memory exhaustion |
| `transaction_lock_timeout` | 1000ms | Increase for long-running transactions; decrease for fast-fail behavior |
| `default_lock_timeout` | 1000ms | Timeout for non-transactional writes through TransactionDB |

### Range Lock Manager

- `SetMaxLockMemory()`: control total memory for range locks
- `SetEscalationBarrierFunc()`: prevent lock escalation across application boundaries
- Range locks are useful when application performs range scans and needs phantom read prevention

## Deadlock Handling

| Approach | When to use |
|----------|-------------|
| Lock timeout only | Simple workloads with rare contention |
| Deadlock detection | Complex lock patterns, multi-key transactions |
| Both enabled | Maximum safety: detection catches cycles quickly, timeout is safety net |

Tuning `deadlock_timeout_us`:
- Set to 0 for immediate detection when deadlocks are frequent
- Set to a value slightly longer than typical transaction duration when deadlocks are rare

## Common Pitfalls

**1. Lost update (use GetForUpdate):**

Incorrect: `Get()` followed by `Put()` based on the read value. Another transaction may modify the key between the read and write.

Correct: `GetForUpdate()` acquires a lock during the read, preventing concurrent modifications.

**2. Snapshot not set:**

Without calling `SetSnapshot()`, each `Get()` reads the latest committed value. This can lead to inconsistent reads within a transaction if other transactions commit between reads.

**3. Deadlock from inconsistent lock ordering:**

When multiple transactions lock keys in different orders (e.g., T1 locks A then B, T2 locks B then A), deadlock occurs. Solutions: consistent lock ordering, enable `deadlock_detect`, or use lock timeouts.

**4. Memory exhaustion with large WriteCommitted transactions:**

WriteCommitted buffers all writes in memory until commit. For very large transactions, either:
- Switch to WriteUnprepared (flushes data incrementally)
- Use the commit bypass memtable optimization (WriteCommitted with `commit_bypass_memtable`)
- Set `max_write_batch_size` to limit transaction size

**5. Non-transactional writes bypassing locks:**

Direct calls to `DB::Put()` through a `TransactionDB` internally create a transaction via `BeginInternalTransaction()` and use `CommitBatch()`, which sorts keys before locking to avoid deadlocks with concurrent `Write()` calls. These internal transactions do not participate in snapshot isolation. For full isolation, use `TransactionDB::Write()` with `TransactionDBWriteOptimizations` or create a transaction.

**6. DeleteRange incompatibility:**

`DeleteRange()` support depends on the DB type and write policy:

| DB Type | Direct API | Via `TransactionDB::Write()` |
|---------|-----------|------------------------------|
| `OptimisticTransactionDB` | Not supported | Not supported (rejected even in `WriteBatch`) |
| `TransactionDB` (WriteCommitted) | Not supported | Supported with `skip_concurrency_control = true` |
| `TransactionDB` (WritePrepared/WriteUnprepared) | Not supported | Supported with `skip_concurrency_control = true` AND `skip_duplicate_key_check = true` |

## db_bench Transaction Benchmarks

Transaction throughput can be measured using `db_bench`:

```
# Pessimistic transactions
./db_bench --benchmarks=randomtransaction --transaction_db

# Optimistic transactions
./db_bench --benchmarks=randomtransaction --optimistic_transaction_db
```

Key `db_bench` options for transactions:
- `--transaction_db`: use `TransactionDB`
- `--optimistic_transaction_db`: use `OptimisticTransactionDB`
- `--transaction_lock_timeout`: lock timeout in ms
- `--transaction_sets`: number of key groups per transaction
- `--transaction_set_snapshot`: set snapshot at transaction start

## WritePrepared/WriteUnprepared Tuning

| Option | Default | Notes |
|--------|---------|-------|
| `wp_commit_cache_bits` (private) | 23 (8M entries) | Larger = more memory, longer before eviction |
| `wp_snapshot_cache_bits` (private) | 7 (128 entries) | Increase if many concurrent snapshots |
| `two_write_queues` (`DBOptions`) | false | Recommended true for WritePrepared/WriteUnprepared |
| `default_write_batch_flush_threshold` | 0 (disabled) | WriteUnprepared: set to limit in-memory batch size |

Note: `wp_commit_cache_bits` and `wp_snapshot_cache_bits` are private members of `TransactionDBOptions` (not user-configurable in production; used primarily for testing).

## WAL Compatibility

Important: The WAL format differs between WriteCommitted and WritePrepared/WriteUnprepared. Switching write policies requires flushing the DB first to empty the WAL. The SST file format is compatible across all policies.
