# SST Unique ID Verification

**Files:** `table/unique_id_impl.h`, `table/unique_id.cc`, `include/rocksdb/unique_id.h`, `include/rocksdb/options.h`, `db/version_edit.h`

## Overview

RocksDB assigns a unique 128-bit ID to each SST file and stores it in the MANIFEST. On file open, the ID is recomputed from table properties and compared against the MANIFEST value. This detects file misplacement, accidental copying between databases, and certain forms of file-level corruption.

## Unique ID Derivation

The internal unique ID is derived from three table properties in `GetSstInternalUniqueId()` (see `table/unique_id_impl.h`):

| Property | Source | Role |
|----------|--------|------|
| `db_id` | Hashed | Identifies the database instance |
| `db_session_id` | Decoded from base-36 | Identifies the DB session that created the file |
| `orig_file_number` | Passed as `file_number` parameter, XORed into the ID | Identifies the specific file within the session |

The `db_session_id` is a 20-character base-36 string (`EncodeSessionId()` in `table/unique_id_impl.h`) encoding ~103 bits of entropy. This is enough to expect no collisions across a billion servers each opening databases a million times.

## Internal vs External IDs

RocksDB maintains two forms of unique IDs:

| Form | Type | Purpose |
|------|------|---------|
| Internal (`UniqueId64x2`) | 128-bit structured | MANIFEST storage, verification |
| External | 128-bit hashed | Public API (`GetUniqueIdFromTableProperties`) |

`InternalUniqueIdToExternal()` transforms internal IDs to external form by applying an additional hashing layer. This ensures that prefixes of the external ID have full entropy, making them suitable for use as identifiers in external systems.

## MANIFEST Storage

When an SST file is created, its internal unique ID is recorded in `FileMetaData::unique_id` (see `db/version_edit.h`) and persisted via `VersionEdit::AddFile`. The null value `kNullUniqueId64x2` indicates no unique ID is available (e.g., files created before RocksDB 7.3).

## Verification on File Open

### Configuration

`verify_sst_unique_id_in_manifest` in `DBOptions` (see `include/rocksdb/options.h`) controls unique ID verification. Default: true.

### Verification Flow

Step 1 -- When an SST file is opened (e.g., for read, compaction, or during `DB::Open()`), the table reader extracts `db_id`, `db_session_id`, and `orig_file_number` from table properties

Step 2 -- `GetSstInternalUniqueId()` recomputes the internal unique ID

Step 3 -- The computed ID is compared against the `unique_id` stored in `FileMetaData` (from MANIFEST)

Step 4 -- On mismatch, `Status::Corruption()` is returned

### Detection Capabilities

Unique ID verification detects:
- SST files copied from another database (different `db_id`)
- SST files from a different session (different `db_session_id`)
- File number reuse after corruption (different `orig_file_number`)
- File replacement or swap (any property mismatch)

### When Verification Happens

Verification occurs each time an SST file is opened, which depends on `max_open_files`:
- With `max_open_files = -1`: All files are opened at `DB::Open()` time, so all files are verified immediately
- With a limited `max_open_files`: Files are opened on demand, so verification happens lazily as files are accessed

Note: An early version of this feature opened all SST files at `DB::Open()` time for verification, but this is no longer guaranteed.

### Limitations

- Only applies to block-based table format SST files
- Files created before RocksDB 7.3 do not have unique IDs in MANIFEST and are skipped
- Setting to false should only be needed if unexpected problems arise

## Relationship to Context Checksums

Unique ID verification and context checksums (format version >= 6) are independent mechanisms:

| Mechanism | Scope | What It Detects |
|-----------|-------|-----------------|
| Unique ID verification | Whole file | File misplacement, wrong file identity |
| Context checksums | Per block | Block-level misplacement within or between files |

The `base_context_checksum` in the footer is a random value unrelated to the SST unique ID. Both mechanisms can be enabled simultaneously for defense in depth.
