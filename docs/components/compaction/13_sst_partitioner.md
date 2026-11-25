# SST Partitioner

**Files:** `include/rocksdb/sst_partitioner.h`, `db/compaction/sst_partitioner.cc`, `db/compaction/compaction_outputs.cc`, `db/compaction/compaction.cc`

## Overview

SstPartitioner is a pluggable interface that controls how compaction splits output SST files at application-defined boundaries. Without a partitioner, compaction splits output files based on size targets and grandparent overlap. With a partitioner, compaction additionally splits at user-defined key boundaries, which can reduce write amplification when files are later promoted or compacted further.

The primary use case is splitting SST files at key prefix boundaries so that files align with application-level partitions (e.g., tenant IDs, date ranges, geographic regions).

## Configuration

The partitioner factory is set via `sst_partitioner_factory` in `ColumnFamilyOptions` (see `include/rocksdb/options.h`). Use `NewSstPartitionerFixedPrefixFactory()` from `include/rocksdb/sst_partitioner.h` to create the built-in fixed-prefix partitioner.

Note: This feature is marked as experimental in the options documentation.

## SstPartitioner Interface

Defined in `include/rocksdb/sst_partitioner.h`, the interface has two methods:

| Method | Description |
|--------|-------------|
| `ShouldPartition(request)` | Called for each key during compaction. Returns `kRequired` to force a file split between the previous key and the current key, or `kNotRequired` to continue the current file. |
| `CanDoTrivialMove(smallest, largest)` | Called during trivial move evaluation. Returns `true` if all keys in `[smallest, largest]` belong to the same partition, allowing trivial move. Returns `false` if a partition boundary exists within the range. |

### PartitionerRequest

The `PartitionerRequest` struct passed to `ShouldPartition()` contains:

| Field | Type | Description |
|-------|------|-------------|
| `prev_user_key` | `const Slice*` | The last key written to the current output file |
| `current_user_key` | `const Slice*` | The next key to be written |
| `current_output_file_size` | `uint64_t` | Current output file size in bytes |

### SstPartitioner::Context

The factory receives context about the compaction when creating a partitioner:

| Field | Type | Description |
|-------|------|-------------|
| `is_full_compaction` | `bool` | Whether all files are being compacted |
| `is_manual_compaction` | `bool` | Whether triggered by manual compaction |
| `output_level` | `int` | Target output level |
| `smallest_user_key` | `Slice` | Smallest key in the compaction |
| `largest_user_key` | `Slice` | Largest key in the compaction |

## SstPartitionerFactory

`SstPartitionerFactory` (in `include/rocksdb/sst_partitioner.h`) extends `Customizable` and creates `SstPartitioner` instances. Custom factories must implement:

| Method | Description |
|--------|-------------|
| `CreatePartitioner(context)` | Creates a partitioner for the given compaction context |
| `Name()` | Returns a unique name identifying this factory |

## Built-in Implementation: SstPartitionerFixedPrefix

RocksDB provides `SstPartitionerFixedPrefix` and its factory `SstPartitionerFixedPrefixFactory` for the common case of splitting at fixed-length key prefix boundaries.

**Behavior**: Compares the first `len` bytes of the previous and current user keys. If they differ, returns `kRequired` to force a file split. For keys shorter than `len`, the entire key is used as the prefix.

**CanDoTrivialMove**: Uses the same prefix comparison on the smallest and largest keys. Returns `true` (allow trivial move) only if both keys share the same prefix.

Create via the factory function `NewSstPartitionerFixedPrefixFactory(prefix_len)` in `include/rocksdb/sst_partitioner.h`.

## Integration with Compaction

### Partitioner Creation

During compaction, the `CompactionOutputs` constructor creates a partitioner via `Compaction::CreateSstPartitioner()` in `db/compaction/compaction.cc`. The partitioner is only created for non-L0 output levels -- the `CompactionOutputs` constructor sets `partitioner_` to `nullptr` when `output_level() == 0`, because L0 output files are never partitioned (L0 files can have overlapping key ranges). Additionally, `ShouldStopBefore()` has a secondary guard that returns false for L0 output regardless of the partitioner.

### Output File Splitting

The partitioner is checked in `CompactionOutputs::ShouldStopBefore()` in `db/compaction/compaction_outputs.cc`. The partitioner check occurs after TTL-based cutting but before size-based and grandparent-based checks. When the partitioner returns `kRequired`, the current output file is finalized and a new file is started.

The `last_key_for_partitioner_` field tracks the previous key for the partitioner request. It is updated for every key including range deletion sentinel keys emitted by `CompactionMergingIterator`.

### Interaction with Trivial Move

During trivial move evaluation, the partitioner's `CanDoTrivialMove()` is called to verify that the file being moved does not span a partition boundary. If it does, trivial move is blocked and a full compaction is performed to split the file.

### Interaction with Manual Compaction

When `CompactRange()` is called with both `begin` and `end` specified and a partitioner is configured, the overlap check logic in `CompactRangeInternal()` consults the partitioner. If the partitioner indicates a partition boundary exists within `[begin, end]`, the overlap check uses file-level granularity instead of key-level granularity. This ensures that files spanning partition boundaries are included in the compaction even if the keys within those files do not overlap the specified range at the key level.

## Design Considerations

**Write amplification reduction**: By aligning SST file boundaries with application-defined partitions, trivial moves become more likely in future compactions. A file that maps entirely to one partition can be moved to the next level without re-reading and re-writing its contents.

**File count impact**: Aggressive partitioning (short prefixes with high cardinality) creates more, smaller files. This increases metadata overhead and can impact read performance through more files to search. Balance partition granularity against the number of resulting files.

**No L0 partitioning**: Partitioning is disabled for L0 output because L0 allows overlapping files. Partitioning L0 would not provide the trivial-move benefit since L0 files must be merged on compaction regardless.
