# RocksDB Data Integrity & Checksum Verification

## Overview

RocksDB protects data integrity through multiple independent layers of checksums and verification, spanning on-disk formats (SST block trailers, WAL records, MANIFEST), in-flight handoff (writer-to-filesystem CRC32c), in-memory protection (per-key checksums in WriteBatch, MemTable, and parsed blocks), and post-write validation (output file iteration, record counting, unique ID cross-checks). These mechanisms are largely independent and can be enabled or tuned separately.

**Key source files:** `include/rocksdb/table.h` (ChecksumType), `table/format.h` (block trailer, context checksums), `util/crc32c.h` / `util/crc32c.cc`, `db/log_format.h` (WAL record format), `include/rocksdb/file_checksum.h`, `db/kv_checksum.h` (ProtectionInfo), `db/output_validator.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Checksum Types and Algorithms | [01_checksum_types.md](01_checksum_types.md) | `ChecksumType` enum, CRC32c hardware acceleration, xxHash/XXH3, CRC masking, and algorithm selection. |
| 2. Block Checksums | [02_block_checksums.md](02_block_checksums.md) | SST block trailer format, write/read checksum flow, context checksums (format version >= 6), and verification options. |
| 3. WAL and MANIFEST Checksums | [03_wal_manifest_checksums.md](03_wal_manifest_checksums.md) | WAL per-record CRC32c, record format, recyclable records, MANIFEST integrity, and WAL recovery modes. |
| 4. File Checksums and Handoff | [04_file_checksums_handoff.md](04_file_checksums_handoff.md) | Full-file checksums, `FileChecksumGenFactory`, handoff checksums via `DataVerificationInfo`, and ingestion verification. |
| 5. Per-Key Write Path Protection | [05_write_path_protection.md](05_write_path_protection.md) | `ProtectionInfo` class hierarchy, WriteBatch per-key checksums, field hashing with independent seeds. |
| 6. MemTable and Block Protection | [06_memtable_block_protection.md](06_memtable_block_protection.md) | In-memory per-key checksums for MemTable entries and parsed SST blocks, paranoid memory checks. |
| 7. Output Verification | [07_output_verification.md](07_output_verification.md) | `VerifyOutputFlags`, `paranoid_file_checks`, `OutputValidator` rolling hash, record count verification. |
| 8. SST Unique ID Verification | [08_unique_id_verification.md](08_unique_id_verification.md) | Unique ID derivation from table properties, MANIFEST cross-check, and misplacement detection. |
| 9. WAL Tracking and Verification | [09_wal_tracking.md](09_wal_tracking.md) | MANIFEST-based WAL tracking, predecessor WAL verification, and WAL hole detection. |
| 10. Manual Verification and Recovery | [10_verification_recovery.md](10_verification_recovery.md) | `DB::VerifyChecksum()`, `DB::VerifyFileChecksums()`, `RepairDB()`, best-efforts recovery, and error handling. |

## Key Characteristics

- **Multiple checksum algorithms**: CRC32c (hardware-accelerated), xxHash, xxHash64, XXH3 (default, fastest on modern CPUs)
- **Layered protection**: Independent checksums at block, record, file, and in-memory levels
- **Context-aware checksums**: Format version >= 6 adds per-file random salt and per-block offset mixing to detect block misplacement
- **Per-key checksums**: Optional protection through WriteBatch, MemTable, and parsed block layers (1/2/4/8 bytes configurable)
- **Handoff checksums**: CRC32c passed to filesystem layer via `DataVerificationInfo` for end-to-end write path integrity
- **Post-write validation**: `OutputValidator` rolling hash, block checksum re-read, file checksum re-computation
- **WAL always uses CRC32c**: Regardless of SST checksum type, ensuring fast hardware-accelerated recovery
- **Configurable paranoia levels**: From minimal (verify_checksums on read) to maximum (all paranoid options enabled)
- **Close-time MANIFEST validation**: Optional re-read and self-healing of MANIFEST corruption on DB close

## Key Invariants

- Checksum type is stored in the SST footer and block trailers; it is immutable after file creation
- Block checksums cover compressed data plus the compression type byte, verified before decompression
- WAL record checksums cover the type byte and payload but not the length bytes
- Corruption detected during reads returns `Status::Corruption()` rather than crashing
- File checksums stored in MANIFEST only; not stored inside the SST file itself
