# RocksDB Compaction

## Overview

Compaction is the background process that maintains the LSM-tree by merging SST files across levels. It reclaims space from obsolete key versions and deletions, reduces read amplification by consolidating data, and enforces level size targets. RocksDB supports three compaction styles -- leveled, universal, and FIFO -- each with distinct tradeoffs between write amplification, space amplification, and read amplification.

**Key source files:** `db/compaction/compaction.h`, `db/compaction/compaction_job.h`, `db/compaction/compaction_job.cc`, `db/compaction/compaction_picker.h`, `db/compaction/compaction_picker_level.cc`, `db/compaction/compaction_picker_universal.cc`, `db/compaction/compaction_picker_fifo.cc`, `db/compaction/compaction_iterator.h`, `db/compaction/compaction_iterator.cc`, `include/rocksdb/advanced_options.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Overview | [01_overview.md](01_overview.md) | Compaction pipeline (pick, execute, install), triggers, score calculation, the `Compaction` object, and component interactions. |
| 2. Leveled Compaction | [02_leveled_compaction.md](02_leveled_compaction.md) | Level structure, the four-step picking algorithm, L0 handling, clean cut expansion, output file splitting, and key options. |
| 3. File Selection and Priority | [03_file_selection_and_priority.md](03_file_selection_and_priority.md) | `CompactionPri` options (`kByCompensatedSize`, `kMinOverlappingRatio`, `kRoundRobin`, etc.), file ordering, TTL boosting, and the picking process. |
| 4. Dynamic Level Size Calculation | [04_dynamic_levels.md](04_dynamic_levels.md) | `level_compaction_dynamic_level_bytes` algorithm, base level computation, unnecessary level draining, and impact on compression settings. |
| 5. Trivial Move Optimization | [05_trivial_move.md](05_trivial_move.md) | Conditions for trivial move, L0 trivial move, non-L0 multi-file extension, execution fast path, and limitations. |
| 6. Universal Compaction | [06_universal_compaction.md](06_universal_compaction.md) | Sorted run abstraction, five prioritized strategies (periodic, size-amp, size-ratio, count, delete-triggered), incremental mode, and configuration. |
| 7. FIFO Compaction | [07_fifo_compaction.md](07_fifo_compaction.md) | TTL and size-based file deletion, intra-L0 compaction (cost-based and ratio-based), temperature change compaction, and BlobDB interaction. |
| 8. CompactionJob Execution | [08_compaction_job.md](08_compaction_job.md) | Three-phase lifecycle (Prepare, Run, Install), `ProcessKeyValueCompaction`, output file splitting criteria, stats aggregation, and resumption. |
| 9. Subcompaction Parallelism | [09_subcompaction.md](09_subcompaction.md) | Key-range partitioning via anchor points, `SubcompactionState`, thread execution model, and round-robin resource management. |
| 10. Remote Compaction | [10_remote_compaction.md](10_remote_compaction.md) | Four-phase protocol (schedule, compact, wait, install), serialization format, worker execution, fallback to local, and output verification. |
| 11. CompactionIterator | [11_compaction_iterator.md](11_compaction_iterator.md) | Input iterator stack, core state machine, snapshot visibility rules, key type processing (Put, Delete, SingleDelete, Merge), sequence number zeroing, and blob processing. |
| 12. CompactionFilter | [12_compaction_filter.md](12_compaction_filter.md) | `CompactionFilter` and `CompactionFilterFactory` APIs, decision types, invocation flow, scope limitations, and interactions with snapshots, merges, and BlobDB. |
| 13. SST Partitioner | [13_sst_partitioner.md](13_sst_partitioner.md) | `SstPartitioner` interface, fixed-prefix implementation, integration with output file splitting and trivial move. |
| 14. Manual Compaction | [14_manual_compaction.md](14_manual_compaction.md) | `CompactRange` and `CompactFiles` APIs, bottommost level options, execution flow per compaction style, cancellation, and thread scheduling. |
| 15. Disk Space Management | [15_disk_space_management.md](15_disk_space_management.md) | `SstFileManager`, compaction space reservation, rate-limited file deletion via `DeleteScheduler`, chunked deletion, and error recovery. |

## Key Characteristics

- **Three compaction styles**: Leveled (size-based per-level targets), universal (size-tiered sorted runs), FIFO (time/size-based deletion)
- **Three-phase pipeline**: Pick (under mutex), execute (mutex released, parallelizable via subcompactions), install (under mutex)
- **Dynamic level targets**: Base level and level sizes computed from actual data size, working backward from the largest level
- **Trivial move**: Files moved between levels by metadata update only when no merge, compression change, or grandparent overlap issue
- **Subcompaction parallelism**: Key-range partitioning via anchor points enables parallel execution within a single compaction job
- **Remote compaction**: Offloads CPU-intensive merge work to separate processes via a user-implemented `CompactionService`
- **Compaction filter**: User-defined key inspection, modification, or deletion during compaction (and optionally flush)
- **SST partitioner**: User-defined output file splitting at application-level boundaries (e.g., key prefix)
- **Disk space management**: `SstFileManager` enforces space limits, reserves space for compactions, and rate-limits file deletion
- **Write flow integration**: `WriteController` throttles or stalls writes when compaction falls behind

## Key Invariants

- Compaction inputs must form a clean cut: no user key can be partially included across input files at the same level
- L1+ levels maintain sorted, non-overlapping SST files; compaction output preserves this property
- At most one L0-to-base compaction runs concurrently in leveled compaction (intra-L0 compaction may run in parallel)
- No two subcompactions within a job may have overlapping key ranges
- Deletion markers are only dropped when the key does not exist beyond the output level (or at the bottommost level)
