# Output Verification

**Files:** `include/rocksdb/advanced_options.h`, `include/rocksdb/options.h`, `db/output_validator.h`, `db/compaction/compaction_job.cc`, `db/flush_job.cc`

## Overview

RocksDB provides several mechanisms to verify the integrity of SST files produced by flush and compaction. These post-write checks detect corruption that occurs during the write path (e.g., memory corruption, software bugs, storage layer issues).

## VerifyOutputFlags

`VerifyOutputFlags` is a bitmask enum in `include/rocksdb/advanced_options.h` providing fine-grained control over compaction output verification:

### Verification Types (What to Check)

| Flag | Value | Description |
|------|-------|-------------|
| `kVerifyNone` | 0x0 | No verification |
| `kVerifyBlockChecksum` | 1 << 0 | Verify block checksums by re-reading blocks |
| `kVerifyIteration` | 1 << 1 | Reopen the file, iterate all keys, and compare a rolling hash against what was computed during the write |
| `kVerifyFileChecksum` | 1 << 2 | Verify the full-file checksum |
| `kVerifyAll` | 0xFFFFFFFF | All verification types and scopes |

### Scope Flags (When to Apply)

| Flag | Value | Description |
|------|-------|-------------|
| `kEnableForLocalCompaction` | 1 << 10 | Enable for local compaction |
| `kEnableForRemoteCompaction` | 1 << 11 | Enable for remote compaction |

Both a verification type flag and a scope flag must be set for verification to occur. For example, `kVerifyIteration | kEnableForLocalCompaction` enables iteration verification for local compactions only.

**Configuration:** Set `verify_output_flags` in `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`). Default: `kVerifyNone`. Dynamically changeable via `SetOptions()` (as a `uint32_t`).

## paranoid_file_checks (Legacy)

`paranoid_file_checks` in `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) is an older mechanism that enables full key/value hash verification after writing every SST file (both flush and compaction). Default: false.

When enabled, the newly created SST file is reopened, all keys are read, and a rolling hash is compared against the hash computed during the write. This applies to both flush and compaction output. For compaction, this is equivalent to enabling `kVerifyIteration` for both local and remote compaction. Note that `verify_output_flags` currently applies to compaction output only; flush verification still uses `paranoid_file_checks`.

**Performance Impact:** Roughly doubles I/O for each flush and compaction operation (write + verify read).

## OutputValidator

`OutputValidator` in `db/output_validator.h` is the class that implements the rolling hash verification. It takes each key/value pair via `Add()` and maintains a running 64-bit hash (`paranoid_hash_`). The write path and verify path each create an `OutputValidator` instance; `CompareValidator()` checks that their hashes match.

The validator also checks key ordering: each key must be greater than the previous key according to the internal key comparator. This detects key-ordering violations caused by bugs or corruption.

## Record Count Verification

### Flush Record Count

`flush_verify_memtable_count` in `DBOptions` (see `include/rocksdb/options.h`) validates that the number of entries read from the MemTable during flush matches the number written to the output SST file. Default: true. This option is deprecated and may be removed once the feature is proven stable.

### Compaction Record Count

`compaction_verify_record_count` in `DBOptions` (see `include/rocksdb/options.h`) verifies record counts during compaction. It performs two checks: (1) input record count -- compares the number of entries read from compaction input files against input file metadata; (2) output record count -- compares the number of entries written by the compaction iterator against the sum of entries in the output SST files. Default: true. This option is also deprecated.

**Limitations:**
- Not verified when a compaction filter returns `kRemoveAndSkipUntil`
- Range deletions are not included in the count

## force_consistency_checks

`force_consistency_checks` in `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) enables LSM consistency checks in release mode. Default: true.

In debug builds, RocksDB always runs consistency checks on the LSM tree every time it changes (flush, compaction, `AddFile`). This option extends those checks to release builds. The CPU overhead is negligible for normal mixed operations but can slow down saturated writing.

## Verification Workflow

### After Flush

Step 1 -- Flush writes MemTable entries to a new SST file, maintaining an `OutputValidator` hash

Step 2 -- If `paranoid_file_checks` is true, the SST is reopened and all keys are re-read with a second `OutputValidator`

Step 3 -- The two hashes are compared via `CompareValidator()`

Step 4 -- If `flush_verify_memtable_count` is true, the entry count is also verified

### After Compaction

Step 1 -- Compaction reads input files and writes output files, maintaining an `OutputValidator` per output

Step 2 -- If `compaction_verify_record_count` is true, the input entry count is verified

Step 3 -- `CompactionJob::VerifyOutputFiles()` performs post-compaction verification based on `verify_output_flags`

Step 4 -- Verification may include block checksum re-read, full iteration with hash comparison, and file checksum verification

## Additional Output Verification Options

### detect_filter_construct_corruption

`detect_filter_construct_corruption` in `BlockBasedTableOptions` (see `include/rocksdb/table.h`). Default: false.

When enabled, after building a Bloom or Ribbon filter block, the filter is verified by querying all keys that were added to it. This detects hardware-induced corruption during filter construction (e.g., CPU/memory errors that produce a corrupt filter). In production, this has detected hardware errors that later manifested as machine-check exceptions.

### verify_compression

`verify_compression` in `BlockBasedTableOptions` (see `include/rocksdb/table.h`). Default: false.

When enabled, after compressing each SST block, the compressed data is immediately decompressed and compared with the original. This detects bugs in compression libraries or hardware corruption during compression. The overhead is substantial (decompressing every block during writes) but provides strong guarantees about compression correctness.
