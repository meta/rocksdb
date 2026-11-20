# Per-Key Write Path Protection

**Files:** `db/kv_checksum.h`, `include/rocksdb/write_batch.h`, `include/rocksdb/options.h`

## Overview

RocksDB provides optional per-key checksums that protect key-value entries as they flow through the write path. This detects in-memory corruption (e.g., bit flips, buffer overflows, use-after-free) that would not be caught by on-disk checksums. The protection is implemented via the `ProtectionInfo` class hierarchy in `db/kv_checksum.h`.

## ProtectionInfo Class Hierarchy

Each class protects a different combination of fields, corresponding to the entry's lifecycle stage:

| Class | Protected Fields | Lifecycle Stage |
|-------|-----------------|-----------------|
| `ProtectionInfo<T>` | Base XOR hash | Foundation |
| `ProtectionInfoKVO<T>` | Key, Value, OpType | WriteBatch entry before CF assignment |
| `ProtectionInfoKVOC<T>` | Key, Value, OpType, CF ID | WriteBatch entry with CF |
| `ProtectionInfoKVOS<T>` | Key, Value, OpType, SeqNo | MemTable entry with sequence number |
| `ProtectionInfoKV<T>` | Key, Value | Simple KV pair |

The standard alias is `ProtectionInfo64` (and variants), using 64-bit protection values. The underlying `ProtectionInfo` encoding infrastructure supports widths of 1, 2, 4, or 8 bytes, but not all options support all widths:

| Option | Supported Values |
|--------|-----------------|
| `WriteOptions::protection_bytes_per_key` | 0 (disabled) or 8 only |
| `memtable_protection_bytes_per_key` | 0, 1, 2, 4, or 8 |
| `block_protection_bytes_per_key` | 0, 1, 2, 4, or 8 |

Narrower widths reduce memory overhead at the cost of lower corruption detection probability (e.g., 1 byte gives 255/256 detection rate).

## Hashing Design

Each field is hashed with an independent seed to prevent field-swap corruption from going undetected:

| Field | Seed Constant | Purpose |
|-------|---------------|---------|
| Key | `kSeedK = 0` | Key data |
| Value | `kSeedV = 0xD28AAD72F49BD50B` | Value data |
| OpType | `kSeedO = 0xA5155AE5E937AA16` | ValueType (Put, Delete, Merge, etc.) |
| SeqNo | `kSeedS = 0x77A00858DDD37F21` | Sequence number |
| CF ID | `kSeedC = 0x4A2AB5CBD26F542C` | Column family identifier |

Seeds are spaced by a large odd increment to avoid correlated hash collisions. The protection value is the XOR of all field hashes.

## Field Transitions

As entries move through the write path, fields are added or removed using dedicated methods:

| Method | Operation | Example Use |
|--------|-----------|-------------|
| `ProtectC(cf_id)` | Add CF ID protection | WriteBatch assigns CF to entry |
| `StripC(cf_id)` | Remove CF ID protection | Before inserting into MemTable |
| `ProtectS(seqno)` | Add sequence number protection | Assigning sequence during write |
| `StripS(seqno)` | Remove sequence number protection | Before output to SST |
| `UpdateK(old_key, new_key)` | Change key protection | Key transformation |
| `UpdateV(old_val, new_val)` | Change value protection | Value transformation |
| `UpdateO(old_op, new_op)` | Change op type protection | Merge operand type change |
| `UpdateS(old_seq, new_seq)` | Change sequence protection | Sequence number update |
| `UpdateC(old_cf, new_cf)` | Change CF ID protection | CF reassignment |

These incremental updates avoid recomputing the full checksum when only one field changes.

## Encoding and Verification

Checksums are encoded to a configurable number of bytes (1, 2, 4, or 8) via `ProtectionInfo::Encode()`, which uses `EncodeFixed16`/`EncodeFixed32`/`EncodeFixed64` for the appropriate width. Verification via `ProtectionInfo::Verify()` decodes and compares.

On verification failure, `ProtectionInfo::GetStatus()` returns `Status::Corruption("ProtectionInfo mismatch")`.

## WriteBatch Configuration

`WriteOptions::protection_bytes_per_key` (see `include/rocksdb/options.h`) controls per-key checksum protection in the WriteBatch. Currently supported values are 0 (disabled, default) and 8.

The `WriteBatch` constructor also accepts a `protection_bytes_per_key` parameter (see `include/rocksdb/write_batch.h`). When enabled, checksums are computed when entries are added to the batch and verified when entries are iterated or applied to the MemTable.

## Write Path Flow

Step 1 -- Application adds entries to WriteBatch with `protection_bytes_per_key > 0`

Step 2 -- `ProtectionInfoKVO` is computed for each entry (key + value + op type)

Step 3 -- When the batch is applied, CF ID is added: `ProtectionInfoKVO` becomes `ProtectionInfoKVOC`

Step 4 -- During MemTable insertion, CF is stripped and sequence number is added: `ProtectionInfoKVOC` becomes `ProtectionInfoKVOS`

Step 5 -- `ProtectionInfoKVOS` checksums are stored alongside entries in the MemTable (if `memtable_protection_bytes_per_key > 0`)

**Note:** It is recommended to enable WriteBatch per-key checksums together with MemTable protection, so that checksum computation happens outside writer threads (the MemTable checksum can be derived from the WriteBatch checksum without recomputation).

## Non-Persistence

The `ProtectionInfo` classes are non-persistent and endianness-dependent. They protect data only while it is in memory. On-disk integrity relies on block checksums, file checksums, and other mechanisms.
