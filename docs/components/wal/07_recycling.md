# Recycling

**Files:** `include/rocksdb/options.h`, `db/log_format.h`, `db/db_impl/db_impl_open.cc`, `db/log_reader.cc`

## Overview

WAL recycling reuses deleted WAL files instead of creating new ones, avoiding filesystem allocation overhead such as `open()` syscalls, metadata updates, and disk space allocation via `fallocate()`.

## Configuration

`DBOptions::recycle_log_file_num` (see `include/rocksdb/options.h`):
- Default: 0 (recycling disabled)
- When > 0: Specifies the maximum number of WAL files to keep for recycling

## How Recycling Works

Step 1: When a WAL becomes obsolete (its backed MemTable is flushed), instead of deleting it, the file is added to a recycle list.

Step 2: When `DBImpl::CreateWAL()` needs a new WAL and a recycled file is available, it calls `FileSystem::ReuseWritableFile(new_log_fname, old_log_fname)` to reuse the file. This renames the old file to the new name and opens it for writing, avoiding new file allocation.

Step 3: The writer uses **recyclable record types** (types 5-8 and 11, 131) instead of legacy types (1-4 and 10, 130). Recyclable records include a 4-byte log number in the header, enabling the reader to distinguish new records from stale data remaining in the recycled file.

Step 4: The reader skips any records whose log number does not match the expected WAL number (returned as `kOldRecord`). This effectively treats stale data as EOF.

## Recyclable Record Format

When recycling is enabled, the header grows from 7 bytes to 11 bytes:

| Field | Size | Description |
|-------|------|-------------|
| CRC | 4 bytes | CRC32C over type + log number + payload |
| Size | 2 bytes | Payload length |
| Type | 1 byte | Recyclable type variant |
| Log Number | 4 bytes | Lower 32 bits of current WAL number |

The CRC computation extends to cover the log number, providing integrity protection for the log number field itself.

## Tradeoffs

| Benefit | Cost |
|---------|------|
| Avoids file allocation latency | 4 extra bytes per physical record |
| Reduces filesystem metadata churn | Reader complexity (must filter stale records) |
| Reduces `fallocate()` blocking | Limited to `recycle_log_file_num` recycled files |

## Sanitization Rules

Several conditions cause WAL recycling to be disabled during `DBOptions` sanitization at `DB::Open()` (see `SanitizeOptions()` in `db/db_impl/db_impl_open.cc`):

1. **WAL archival enabled**: If `WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0`, recycling is disabled (`recycle_log_file_num = 0`). Archival and recycling are incompatible because archived WALs need to be retained for their data, not reused.

2. **Incompatible recovery modes**: Recycling is disabled for:
   - `kTolerateCorruptedTailRecords`: Inconsistent because WAL recycling produces stale records at the tail that resemble corruption
   - `kAbsoluteConsistency`: Temporarily disabled due to a bug that can introduce holes in recovered data (see GitHub PR #7252)

   Only `kPointInTimeRecovery` and `kSkipAnyCorruptedRecords` are compatible with recycling. Note that `kPointInTimeRecovery` was previously incompatible but was re-enabled (see PR #12403).

## Compatibility with disableWAL

`recycle_log_file_num > 0` is generally incompatible with `WriteOptions::disableWAL = true`. The exception is internal 2PC split writes using two write queues with `disable_memtable = true`. Attempting this combination otherwise returns `Status::InvalidArgument()`.

## Reader Behavior with Recycled Logs

When the reader encounters a recyclable record type:
1. If this is not the first record (`first_record_read_` is true) and no recyclable record has been seen yet (`recycled_` is false), a recyclable record appearing mid-file is treated as `kBadRecord`. A recycled WAL must start with a recyclable record type.
2. The `recycled_` flag is set to true (a one-way latch, never reset to false). For each recyclable record, the 32-bit log number is extracted and compared to the expected log number.
3. Records with a mismatched log number are skipped (`kOldRecord`) and their data is consumed from the buffer.
4. In most recovery modes, encountering `kOldRecord` stops replay (treated as logical EOF). In `kSkipAnyCorruptedRecords` mode, replay continues past old records.

Note: The enforcement is one-directional. The code rejects recyclable records appearing after non-recyclable records (enforcing that recycled WALs start with recyclable types). However, once `recycled_` is true, non-recyclable record types are not explicitly rejected.
