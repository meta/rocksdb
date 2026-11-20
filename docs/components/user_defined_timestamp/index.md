# RocksDB User-Defined Timestamps (UDT)

## Overview

User-Defined Timestamps (UDT) allows applications to embed custom timestamps directly into keys, enabling multi-version concurrency control (MVCC), time-travel queries, and timestamp-based garbage collection. The timestamp is appended as a suffix to the user key and participates in key ordering: for the same user key, larger (newer) timestamps come first. RocksDB provides built-in comparators for uint64_t timestamps and supports custom timestamp formats via the `Comparator` interface.

**Key source files:** `include/rocksdb/comparator.h`, `include/rocksdb/advanced_options.h`, `include/rocksdb/options.h`, `db/dbformat.h`, `util/udt_util.h`, `util/comparator.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Key Encoding and Comparator | [01_key_encoding_and_comparator.md](01_key_encoding_and_comparator.md) | Internal key format with timestamps, the `Comparator` timestamp API, built-in `ComparatorWithU64TsImpl`, and key extraction helpers. |
| 2. Write Path | [02_write_path.md](02_write_path.md) | Timestamp-aware `Put`/`Delete`/`DeleteRange`/`Merge` APIs, `WriteBatch` encoding, and WAL `UserDefinedTimestampSizeRecord`. |
| 3. Read Path | [03_read_path.md](03_read_path.md) | Point lookups with `ReadOptions::timestamp`, iterator range scans with `iter_start_ts`, key shape conventions, and collapsed history validation. |
| 4. Compaction and Garbage Collection | [04_compaction_and_gc.md](04_compaction_and_gc.md) | `full_history_ts_low` cutoff, `CompactionIterator` GC logic, `IncreaseFullHistoryTsLow` API, and timestamp-aware version retention. |
| 5. Flush and Persistence | [05_flush_and_persistence.md](05_flush_and_persistence.md) | `persist_user_defined_timestamps` option, timestamp stripping during flush, `FileMetaData` tracking, and the "UDT in memtable only" mode. |
| 6. Recovery and WAL Replay | [06_recovery_and_wal_replay.md](06_recovery_and_wal_replay.md) | `TimestampRecoveryHandler`, `TimestampSizeConsistencyMode`, best-effort timestamp reconciliation during crash recovery. |
| 7. Migration and Compatibility | [07_migration_and_compatibility.md](07_migration_and_compatibility.md) | Enabling/disabling UDT on existing data, `ValidateUserDefinedTimestampsOptions`, supported transitions, and `OpenAndTrimHistory`. |
| 8. Transaction Integration | [08_transaction_integration.md](08_transaction_integration.md) | WriteCommitted transaction support, timestamped snapshots, `enable_udt_validation`, and MyRocks compatibility. |
| 9. Tools and Testing | [09_tools_and_testing.md](09_tools_and_testing.md) | `db_bench` timestamp support, stress test coverage, `SstFileWriter` timestamp APIs, and external file ingestion. |
| 10. Best Practices | [10_best_practices.md](10_best_practices.md) | Recommended configurations, common pitfalls, key API conventions, and performance considerations. |

## Key Characteristics

- **Timestamp is part of user key**: appended as a fixed-size suffix, size determined by `Comparator::timestamp_size()`
- **Descending timestamp order**: for the same user key (without timestamp), larger timestamps come first
- **Per-column-family**: each CF can independently enable/disable UDT with its own comparator
- **Built-in uint64_t support**: `BytewiseComparatorWithU64Ts()` and `ReverseBytewiseComparatorWithU64Ts()`
- **Timestamp-based GC**: `full_history_ts_low` controls which old versions can be garbage collected
- **Optional persistence**: `persist_user_defined_timestamps=false` strips timestamps during flush (memtable-only mode)
- **WAL recovery**: `UserDefinedTimestampSizeRecord` enables correct key parsing during crash recovery
- **Transaction support**: WriteCommitted transactions support UDT with timestamped snapshots

## Key Invariants

- All keys in a UDT-enabled column family must have exactly `comparator->timestamp_size()` timestamp bytes
- Application-level requirement: for the same user key, timestamps and sequence numbers should increase together (if seq1 < seq2 then ts1 <= ts2). This is not enforced by the engine but is required for correct version ordering
- Changing timestamp size from N to M (both non-zero, N != M) is not supported
- `full_history_ts_low` can only increase, never decrease
- WAL timestamp size records are logged before any WriteBatch that needs them
- `persist_user_defined_timestamps` cannot be toggled while UDT is enabled (ts_sz > 0)
