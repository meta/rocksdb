# MemTable and Block Protection

**Files:** `include/rocksdb/advanced_options.h`, `db/memtable.h`, `db/kv_checksum.h`, `table/block_based/block.h`, `table/block_based/data_block_hash_index.h`

## MemTable Per-Key Checksums

### Configuration

`memtable_protection_bytes_per_key` in `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) controls per-entry checksum size in the MemTable. Supported values: 0 (disabled, default), 1, 2, 4, or 8 bytes.

When enabled, each MemTable entry stores a `ProtectionInfoKVOS` checksum (protecting key, value, op type, and sequence number) alongside the entry data. The checksum is verified when entries are read from the MemTable.

### Interaction with WriteBatch Protection

When both `WriteOptions::protection_bytes_per_key` and `memtable_protection_bytes_per_key` are enabled, the MemTable checksum is derived from the WriteBatch checksum via field stripping and protection (strip CF ID, add sequence number). This avoids recomputing the hash from scratch, reducing CPU overhead on the writer thread.

### Dynamically Changeable

This option can be changed at runtime via `SetOptions()`. New MemTables use the updated setting; existing MemTables retain their original configuration.

## Paranoid Memory Checks

`paranoid_memory_checks` in `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) enables key ordering validation during MemTable reads and scans on skiplist-based memtables. Default: false.

When enabled, the skiplist iterator uses `GetAndValidate()`, `NextAndValidate()`, and `PrevAndValidate()` methods that verify keys are in the expected order. This detects memory corruption that could cause keys to appear out of order in the skiplist.

## Per-Key Checksum on Seek

`memtable_veirfy_per_key_checksum_on_seek` in `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) validates per-key checksums during MemTable seek operations. Default: false.

This option depends on `memtable_protection_bytes_per_key > 0`. If `memtable_protection_bytes_per_key` is zero, no validation is performed regardless of this setting.

## In-Memory Block Protection

### Configuration

`block_protection_bytes_per_key` in `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) enables per-key checksum protection for parsed in-memory SST blocks. Supported values: 0 (disabled, default), 1, 2, 4, or 8 bytes.

### How It Works

When a block is parsed into a `Block` object (after decompression and block checksum verification), per-key checksums are computed and stored alongside the block's parsed data via `InitializeDataBlockProtection()`, `InitializeIndexBlockProtection()`, or `InitializeMetaIndexBlockProtection()`. Each key read from the block is verified against its stored checksum. If the parsed block is subsequently inserted into the block cache, the protection travels with it.

This protects against in-memory data corruption (e.g., bit flips in DRAM, use-after-free bugs) that occurs after the block has passed its on-disk checksum verification. Protection applies to parsed data, index, and meta-index blocks. It does not apply to raw compressed blocks, filter block partitions, or compression dictionaries that are not represented as `Block` objects.

### Performance Impact

Block protection has a non-trivial negative impact on read performance due to checksum computation on block parse and verification on each key read. Different values of the option have similar performance impact but different memory cost and corruption detection probability:

| Bytes per Key | Detection Probability | Memory Overhead |
|---------------|-----------------------|-----------------|
| 1 | 255/256 (~99.6%) | 1 byte per key |
| 2 | 65535/65536 (~99.998%) | 2 bytes per key |
| 4 | ~1 - 2^-32 | 4 bytes per key |
| 8 | ~1 - 2^-64 | 8 bytes per key |

### Dynamically Changeable

This option can be changed at runtime via `SetOptions()`. Newly loaded blocks use the updated setting; blocks already in cache retain their original protection level.

## Protection Coverage Summary

| Layer | Option | Protects Against | Default |
|-------|--------|-----------------|---------|
| WriteBatch | `WriteOptions::protection_bytes_per_key` | Corruption during batch construction and application | Disabled |
| MemTable entries | `memtable_protection_bytes_per_key` | DRAM bit flips, memory corruption in MemTable | Disabled |
| MemTable ordering | `paranoid_memory_checks` | Skiplist structural corruption | Disabled |
| MemTable seek | `memtable_veirfy_per_key_checksum_on_seek` | Entry corruption during seek | Disabled |
| Parsed blocks | `block_protection_bytes_per_key` | In-memory corruption of cached SST blocks | Disabled |
| On-disk blocks | `verify_checksums` (ReadOptions) | Storage corruption, disk bit flips | Enabled |
