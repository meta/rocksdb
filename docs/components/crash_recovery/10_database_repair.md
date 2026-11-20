# Database Repair

**Files:** `db/repair.cc`, `include/rocksdb/db.h`

## Overview

`RepairDB()` (see `include/rocksdb/db.h`) performs best-effort recovery to salvage as much data as possible from a corrupted database. Unlike normal recovery or best-efforts recovery, the repairer does not guarantee bringing the database to a time-consistent state. Some data may be lost.

## Repair Process

The `Repairer::Run()` method in `db/repair.cc` executes repair in the following order:

### Step 1: Find Files

The repairer scans all database paths (including `wal_dir` if different from `dbname`) and classifies files by type:
- MANIFEST files (`kDescriptorFile`) are collected for archival
- WAL files (`kWalFile`) are collected for log-to-table conversion
- SST files (`kTableFile`) are collected for metadata extraction
- All other files are ignored

### Step 2: Archive Old MANIFESTs and Create Fresh MANIFEST

All existing MANIFEST files are moved to the `lost/` subdirectory. A temporary `DBImpl` is created to call `NewDB()` for fresh MANIFEST creation, then `VersionSet::Recover()` loads it into memory.

### Step 3: Extract Metadata (Pre-existing SSTs)

All pre-existing SST files are scanned to compute smallest/largest keys, largest sequence number, and oldest blob file reference. This step also creates column families discovered in SST files that are not yet in the VersionSet. Files that cannot be scanned (corrupted SSTs) are logged as warnings and moved to the `lost/` subdirectory.

### Step 4: Convert WALs to Tables

Each WAL file is replayed using `log::Reader` with checksumming enabled. Corrupted records are logged and skipped -- the repairer intentionally prioritizes data consistency over completeness.

For each WAL:
1. Create per-CF memtables
2. Replay valid WriteBatch records into memtables (invalid records are skipped with a warning)
3. For each CF with a non-empty memtable, flush to a new SST file via `BuildTable()`
4. Move the processed WAL file to the `lost/` subdirectory

**Important:** User-defined timestamp size differences are handled via `HandleWriteBatchTimestampSizeDifference()` with `kVerifyConsistency` mode.

### Step 5: Extract Metadata (New SSTs from WALs)

SST files newly created from WAL replay are scanned for metadata, the same as Step 3.

### Step 6: Add Tables to MANIFEST

`AddTables()` writes all recovered table metadata into the MANIFEST via incremental `LogAndApply()` calls. All table files are placed at L0 (no attempt to reconstruct the original level assignment). The final state has: log number set to zero, next file number set to 1 + largest file number found, and last sequence number set to the largest sequence number across all tables.

## API Variants

`RepairDB()` has three overloads in `include/rocksdb/db.h`:

| Overload | Column Families |
|----------|-----------------|
| `RepairDB(dbname, db_options, column_families)` | Known CFs only; warns on unknown CFs |
| `RepairDB(dbname, db_options, column_families, unknown_cf_opts)` | Known CFs + unknown CFs created automatically with provided options |
| `RepairDB(dbname, options)` | Single options for all CFs |

The overload accepting `unknown_cf_opts` automatically creates column families found in existing SST files but not specified by the caller, using the provided options for those CFs.

## Limitations and Caveats

- **Not time-consistent:** Repair recovers individual files but does not guarantee that the resulting database represents any single point in time
- **All files at L0:** The repaired database has all SST files at L0, which triggers heavy compaction on next open
- **Data loss possible:** Corrupted WAL records and unreadable SST files are logged and archived to the `lost/` subdirectory
- **Unflushed column families are lost:** Column families that were created recently and have no SST files will be dropped during repair. This can damage even a healthy DB if column families haven't been flushed yet
- **Incompatible with `track_and_verify_wals`:** The stricter WAL verification imposed by this option may fail after repair
- **Locking:** The repairer acquires the database lock, preventing concurrent access

## RepairDB vs Best-Efforts Recovery

| Feature | `RepairDB()` | `best_efforts_recovery` |
|---------|-------------|------------------------|
| Invocation | Standalone function, DB must be closed | During `DB::Open()` |
| WAL handling | Converts WALs to SST files | Skips WALs entirely |
| Corrupted SSTs | Skips unreadable files | Recovers to point-in-time before missing files |
| MANIFEST | Creates fresh MANIFEST from scratch | Replays existing MANIFEST, skipping missing files |
| Level assignment | All files placed at L0 | Preserves original level structure |
| Time consistency | Not guaranteed | Guaranteed (valid point-in-time) |
| Requires valid MANIFEST | No (scans files directly) | Yes (at least one non-empty MANIFEST) |
