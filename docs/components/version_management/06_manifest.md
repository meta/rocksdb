# MANIFEST File

**Files:** `db/version_set.h`, `db/version_set.cc`, `db/log_format.h`, `db/log_writer.h`, `db/log_reader.h`

## Overview

The MANIFEST file is the durable metadata log of the database. It records every VersionEdit so that the database state can be reconstructed on recovery. All metadata changes must be written to MANIFEST before they take effect in memory.

## Format

The MANIFEST reuses the same log format as WAL files (see `db/log_format.h`):

```
Log file := block*
Block := record* trailer?       (block size = 32KB)
Record := checksum (4B) | length (2B) | type (1B) | data
Type := kFullType | kFirstType | kMiddleType | kLastType
```

Each logical record contains one serialized VersionEdit. Large edits are split across multiple physical records using the First/Middle/Last fragmentation scheme.

Note: The log format also defines recyclable record variants used by WAL files, but MANIFEST writers are always created with recycle_log_files=false, so MANIFEST files only use the standard record types.

## The CURRENT File

The CURRENT file is a small text file containing the filename of the active MANIFEST (e.g., MANIFEST-000004\n). On recovery, RocksDB reads CURRENT to find which MANIFEST to replay.

SetCurrentFile() (in `file/filename.cc`) atomically updates CURRENT by writing to a temporary file and renaming it. This ensures CURRENT always points to a valid MANIFEST even if the process crashes mid-update.

## MANIFEST Lifecycle

**Creation**: A new MANIFEST is created when:
1. new_descriptor_log=true is passed to LogAndApply()
2. No MANIFEST is currently open (descriptor_log_ is null)
3. The current MANIFEST exceeds tuned_max_manifest_file_size_ (this triggers a new MANIFEST for the next write, not the current one)

When creating a new MANIFEST, WriteCurrentStateToManifest() writes a full snapshot of the current database state as the first records. The snapshot ordering is: DB ID record (if write_dbid_to_manifest is true), then WAL additions and a WAL deletion watermark, then per-CF state (comparator name, UDT persistence metadata, file records).

**Appending**: During normal operation, VersionEdits are appended to the current MANIFEST via log::Writer::AddRecord(), then synced.

**Rolling**: After writing a new MANIFEST and updating CURRENT, the old MANIFEST filename is added to obsolete_manifests_ for later deletion by PurgeObsoleteFiles().

## Size Auto-Tuning

MANIFEST size is managed by three parameters in MutableDBOptions:

| Parameter | Field in VersionSet | Description |
|-----------|-------------------|-------------|
| max_manifest_file_size | min_max_manifest_file_size_ | Minimum cap on MANIFEST size |
| max_manifest_space_amp_pct | max_manifest_space_amp_pct_ | Allowed space amplification percentage |
| manifest_preallocation_size | manifest_preallocation_size_ | Preallocation block size for the file |

TuneMaxManifestFileSize() computes the actual threshold:

```
tuned_max_manifest_file_size_ = max(min_max_manifest_file_size_,
    last_compacted_manifest_file_size_ * (100 + max_manifest_space_amp_pct_) / 100)
```

This means after rolling to a new MANIFEST, the threshold adapts based on the compacted size of that MANIFEST. A database with many files naturally has a larger threshold.

Note: Setting max_manifest_space_amp_pct to 0 approximates the old rewrite-on-every-manifest-write behavior once the compacted MANIFEST exceeds the minimum cap. Negative values are clamped to zero. Changes take effect on the next manifest write, not immediately.

## Close-Time MANIFEST Validation

When verify_manifest_content_on_close is enabled (see DBOptions), VersionSet::Close() performs a full reread of the MANIFEST file to verify its contents. If verification fails, it rewrites the MANIFEST via LogAndApply() and verifies again. This can perform up to two full rereads and one rewrite. This means Close() can do real I/O, rotate MANIFEST, or return corruption and I/O errors even when no user writes are in flight.

## WAL Tracking in MANIFEST

When track_and_verify_wals_in_manifest is enabled (see DBOptions in `include/rocksdb/options.h`), the MANIFEST tracks WAL lifecycle via kWalAddition2 and kWalDeletion2 tags. VersionSet maintains a wals_ (WalSet) with tracked WALs. When rolling to a new MANIFEST, the current WAL state is included in the snapshot, and a WAL deletion watermark record is written to preserve obsolete-WAL state and prevent "resurrecting" deleted WALs.

## Error Handling

If a MANIFEST write fails:
- descriptor_log_ is reset, forcing the next LogAndApply() to create a new MANIFEST
- New files from the failed commit are quarantined from deletion
- If rolling to a new MANIFEST failed after writing but before updating CURRENT, the old MANIFEST is quarantined to prevent premature deletion
- On non-local filesystems, the new MANIFEST is kept (not deleted) because the rename to CURRENT might have succeeded server-side despite a client error
