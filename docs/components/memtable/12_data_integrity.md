# Data Integrity

**Files:** `db/memtable.cc`, `db/memtable.h`, `db/kv_checksum.h`

## Overview

MemTable provides optional per-key checksums and paranoid memory validation to detect in-memory data corruption. These features add CPU overhead but can catch corruption from hardware errors, wild writes, or use-after-free bugs.

## Per-Key Protection Bytes

When `memtable_protection_bytes_per_key` is set to a non-zero value (1, 2, 4, or 8 bytes, see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), each memtable entry stores a checksum appended after the value bytes.

### Checksum Computation

The checksum is computed by `UpdateEntryChecksum()` using the `ProtectionInfo64` framework in `db/kv_checksum.h`. It protects:

- User key (K)
- Value (V)
- Operation type (O)
- Sequence number (S)

When `kv_prot_info` is provided by the caller (from the write path's end-to-end protection chain), the checksum is derived from the existing protection info. Otherwise, it is computed fresh from the key, value, type, and sequence number.

### Checksum Verification

`MemTable::VerifyEntryChecksum()` re-derives the checksum from the entry's decoded fields and compares it against the stored checksum. It is called in two contexts:

1. **During insertion**: After encoding the entry in `Add()`, if `kv_prot_info` is provided, the encoded entry is immediately verified via `VerifyEncodedEntry()`.
2. **During lookup**: In `SaveValue()`, if `protection_bytes_per_key > 0`, each entry found during a point lookup is verified before its value is returned.

A checksum mismatch produces a `Status::Corruption` with details about the corrupted entry (key, sequence number) when `allow_data_in_errors` is true.

## Paranoid Memory Checks

When `paranoid_memory_checks` is enabled (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), additional validation is performed during memtable traversal:

### Get Path

`GetFromTable()` calls `table_->GetAndValidate()` instead of `table_->Get()`. The `GetAndValidate()` method (implemented in the skip list) validates key ordering and per-key checksums during the seek operation.

### Iterator Validation

`MemTableIterator` uses validation-aware navigation:

- `Seek()` calls `iter_->SeekAndValidate()` which checks node ordering and key checksums
- `Next()` calls `iter_->NextAndValidate()` which verifies that each node's key is properly ordered relative to its predecessor
- `Prev()` calls `iter_->PrevAndValidate()` for the same in reverse

If a corruption is detected, the iterator becomes invalid and returns `Status::Corruption`.

## Per-Key Checksum on Seek

The `memtable_veirfy_per_key_checksum_on_seek` option (note: the typo "veirfy" is in the source code, see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) enables key-level checksum verification during memtable seeks without the full overhead of `paranoid_memory_checks`.

Important: this option only has effect when `memtable_protection_bytes_per_key > 0`. If protection bytes are set to 0, no checksums are stored, and the seek-check option enables the validation-aware traversal paths but has nothing to verify.

When enabled, a `key_validation_callback_` is constructed during `MemTable` initialization that calls `ValidateKey()` -> `VerifyEntryChecksum()` for each key visited during seek operations. This callback is passed to `SeekAndValidate()` and `GetAndValidate()`.

## Entry Verification on Encode

`MemTable::VerifyEncodedEntry()` decodes an entry from its encoded form and verifies it against the caller-provided `ProtectionInfoKVOS64`. This catches encoding errors immediately at write time. It is called from `Add()` when `kv_prot_info` is provided.

## Configuration Summary

| Option | Default | Description |
|--------|---------|-------------|
| `memtable_protection_bytes_per_key` | 0 (disabled) | Bytes of checksum per entry (0, 1, 2, 4, or 8) |
| `paranoid_memory_checks` | false | Full validation during traversal |
| `memtable_veirfy_per_key_checksum_on_seek` | false | Checksum verification during seeks only |
| `allow_data_in_errors` | false | Include key/value data in error messages |
