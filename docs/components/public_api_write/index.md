# RocksDB Public Write API

## Overview

RocksDB provides a rich set of write APIs covering single-key convenience methods, atomic batched writes via `WriteBatch`, deferred read-modify-write via merge operators, range deletions, and bulk loading via external SST file ingestion. Most mutating APIs flow through `WriteBatch` encoding and the `DB::Write()` path, with options for durability, flow control, and per-key integrity protection. The `SstFileWriter` and `DB::IngestExternalFile()` path is a separate write path that bypasses WAL and memtable insertion entirely.

**Key source files:** `include/rocksdb/db.h`, `include/rocksdb/write_batch.h`, `include/rocksdb/write_batch_base.h`, `include/rocksdb/merge_operator.h`, `include/rocksdb/sst_file_writer.h`, `db/write_batch.cc`, `db/merge_helper.h`, `db/range_tombstone_fragmenter.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Write API Overview | [01_overview.md](01_overview.md) | Write operation types, single-key convenience APIs, `WriteOptions`, write path overview, column family and timestamp support. |
| 2. WriteBatch Format and Encoding | [02_write_batch.md](02_write_batch.md) | Binary `rep_` format, record encoding, Handler iteration pattern, content flags, sequence number assignment, save points, and per-key protection. |
| 3. SingleDelete Semantics | [03_single_delete.md](03_single_delete.md) | Optimized single-Put deletion, correctness requirements, compaction behavior with peek-ahead strategy, and known performance pitfalls. |
| 4. WriteBatchWithIndex | [04_write_batch_with_index.md](04_write_batch_with_index.md) | Skip-list-indexed write batch for read-your-own-writes, overwrite mode, sub-batch tracking, `BaseDeltaIterator`, and transaction integration. |
| 5. Merge Operator | [05_merge_operator.md](05_merge_operator.md) | `MergeOperator` and `AssociativeMergeOperator` interfaces, `FullMergeV3`, partial merge, `ShouldMerge`, `MergeContext`, and `OpFailureScope`. |
| 6. Merge Implementation | [06_merge_implementation.md](06_merge_implementation.md) | `MergeHelper::MergeUntil()` during compaction, merge resolution during point lookups, compaction filter interaction, snapshot constraints, and `max_successive_merges`. |
| 7. DeleteRange API | [07_delete_range.md](07_delete_range.md) | Half-open range deletion API, internal `kTypeRangeDeletion` representation, memtable and SST storage, and design rationale for the dedicated meta block. |
| 8. DeleteRange Implementation | [08_delete_range_implementation.md](08_delete_range_implementation.md) | `FragmentedRangeTombstoneList` for non-overlapping fragments, `TruncatedRangeDelIterator`, `ReadRangeDelAggregator` and `CompactionRangeDelAggregator`, and per-stripe snapshot handling. |
| 9. SstFileWriter API | [09_sst_file_writer.md](09_sst_file_writer.md) | Creating SST files externally for bulk ingestion, strict ascending key ordering, sequence number 0 assignment, compression selection, and page cache invalidation. |
| 10. External File Ingestion | [10_external_file_ingestion.md](10_external_file_ingestion.md) | `DB::IngestExternalFile()` workflow (prepare, run, apply), level placement algorithm, sequence number assignment rules, and atomic replace range. |
| 11. Low-Priority Write Support | [11_low_priority_write.md](11_low_priority_write.md) | `WriteOptions::low_pri` throttling mechanism, `WriteController` rate limiting, compaction pressure detection, and 2PC interaction. |

## Key Characteristics

- **Atomic batching**: `WriteBatch` groups multiple operations across column families into a single atomic WAL + memtable write
- **Seven operation types**: Put, TimedPut, Delete, SingleDelete, DeleteRange, Merge, PutEntity -- each with default and non-default CF encoding variants
- **Deferred merge**: Merge operands accumulate and resolve lazily during reads or compaction, keeping the write path fast
- **Range deletion**: O(1) write cost regardless of range size; tombstones stored in a separate meta block per SST file
- **Tombstone fragmentation**: Overlapping range tombstones are fragmented into non-overlapping pieces enabling O(log n) coverage checks
- **External ingestion**: `SstFileWriter` + `IngestExternalFile()` bypasses memtable and WAL for bulk loading
- **Per-key protection**: Optional 8-byte checksums per WriteBatch entry detect in-memory corruption
- **Low-priority throttling**: Background writes can be rate-limited independently of foreground writes when compaction falls behind
- **WriteBatchWithIndex**: Skip-list index over WriteBatch enables read-your-own-writes for transaction support

## Key Invariants and Constraints

**Invariants** (violation causes data corruption):
- SingleDelete is correct only if exactly one Put exists since the previous SingleDelete for that key
- After fragmentation, no two range tombstone fragments overlap

**Constraints** (enforced at runtime, returns error status):
- `sync=true` and `disableWAL=true` cannot be used together
- Point keys in `SstFileWriter` must be added in strictly ascending order
- DB-generated files ingested with `allow_db_generated_files=true` must not overlap with existing DB data
