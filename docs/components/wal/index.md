# RocksDB Write-Ahead Log (WAL)

## Overview

The Write-Ahead Log (WAL) is RocksDB's durability mechanism. Each write is appended to the WAL before being inserted into the MemTable, ensuring committed data survives process crashes and power failures. During recovery, RocksDB replays WAL records to reconstruct in-memory state. The WAL uses a block-structured format inherited from LevelDB, with extensions for recycling, compression, user-defined timestamps, and WAL chain verification.

**Key source files:** `db/log_format.h`, `db/log_writer.h`, `db/log_writer.cc`, `db/log_reader.h`, `db/log_reader.cc`, `db/wal_manager.h`, `db/wal_manager.cc`, `db/wal_edit.h`, `include/rocksdb/wal_filter.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Record Format and Block Structure | [01_record_format.md](01_record_format.md) | Physical record layout (legacy and recyclable), 32KB block structure, record fragmentation across blocks, and record type enum. |
| 2. Writer | [02_writer.md](02_writer.md) | `log::Writer` implementation: `AddRecord()` fragmentation loop, `EmitPhysicalRecord()` header encoding and CRC, compression integration, and metadata records. |
| 3. Reader | [03_reader.md](03_reader.md) | `log::Reader` and `FragmentBufferedReader`: `ReadRecord()` reassembly, `ReadPhysicalRecord()` parsing and checksum verification, decompression, and corruption reporting. |
| 4. Recovery Modes | [04_recovery_modes.md](04_recovery_modes.md) | `WALRecoveryMode` enum, corruption handling per mode, interaction with recycled logs, and the `RecoverLogFiles()` flow during `DB::Open()`. |
| 5. WAL Lifecycle | [05_lifecycle.md](05_lifecycle.md) | Creation via `CreateWAL()`, rotation on MemTable switch, archival to `archive/` directory, and purging via TTL or size limits. |
| 6. Sync and Durability | [06_sync_and_durability.md](06_sync_and_durability.md) | `WriteOptions::sync`, `manual_wal_flush`, `FlushWAL()`, `SyncWAL()`, `LockWAL()`, group commit batching, and durability tradeoffs. |
| 7. Recycling | [07_recycling.md](07_recycling.md) | `recycle_log_file_num`, recyclable record types with log number field, `ReuseWritableFile()`, sanitization rules, and compatibility constraints. |
| 8. Compression | [08_compression.md](08_compression.md) | `wal_compression` option, `kSetCompressionType` record, streaming compress/decompress via `StreamingCompress`/`StreamingUncompress`, and format version. |
| 9. Tracking and Verification | [09_tracking_and_verification.md](09_tracking_and_verification.md) | `track_and_verify_wals_in_manifest`, `track_and_verify_wals`, `PredecessorWALInfo` chain verification, `WalSet` in MANIFEST, and `WalAddition`/`WalDeletion`. |
| 10. WAL Filter and Replication | [10_filter_and_replication.md](10_filter_and_replication.md) | `WalFilter` interface for recovery filtering, `GetUpdatesSince()` for replication, `TransactionLogIterator`, and WAL retention for replication. |
| 11. 2PC Transaction Support | [11_2pc_transactions.md](11_2pc_transactions.md) | Prepare/commit/rollback markers in WriteBatch, WAL retention for prepared transactions, `MinLogNumberToKeep()`, and recovery of uncommitted transactions. |
| 12. Configuration Reference | [12_configuration.md](12_configuration.md) | Complete reference of all WAL-related options in `DBOptions`, `WriteOptions`, defaults, dynamic changeability, and sanitization rules. |

## Key Characteristics

- **Block-structured format**: Fixed 32KB blocks (`kBlockSize`), records fragmented across block boundaries
- **Two header formats**: Legacy 7-byte header; recyclable 11-byte header with log number for file reuse
- **CRC32C checksums**: Cover record type + payload (legacy) or type + log number + payload (recyclable)
- **Streaming compression**: Optional ZSTD compression via `StreamingCompress`/`StreamingUncompress`
- **Four recovery modes**: From tolerating tail corruption to absolute consistency to disaster salvage
- **WAL recycling**: Reuses deleted WAL files to avoid filesystem allocation overhead
- **Chain verification**: Optional `PredecessorWALInfo` records detect missing WALs (WAL holes)
- **MANIFEST tracking**: Optional `WalAddition`/`WalDeletion` records in MANIFEST track synced WAL sizes
- **Forward compatibility**: Unknown record types with bit 7 set (`kRecordTypeSafeIgnoreMask`) are safely ignored

## Key Invariants

- Writer never leaves fewer than `header_size` bytes in a block; remaining space is zero-padded
- CRC checksum covers everything after the CRC field itself (type byte, optional log number, and payload)
- When `recycle_log_files` is enabled, reader must skip records whose log number does not match the current WAL
- A WAL can only be archived/deleted after its log number is less than `MinLogNumberToKeep()`, which considers both live MemTables and prepared 2PC transactions
