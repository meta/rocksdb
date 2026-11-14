# Recovery

**Files:** `db/version_edit_handler.h`, `db/version_edit_handler.cc`, `db/version_set.h`, `db/version_set.cc`

## Overview

Recovery reconstructs the in-memory database state by replaying VersionEdit records from the MANIFEST file. Different handler classes support different recovery modes: strict recovery, best-effort recovery, and secondary instance tailing.

## Normal Recovery Flow

Step 1: Read the CURRENT file to get the MANIFEST filename.

Step 2: Open the MANIFEST file and create a VersionEditHandler.

Step 3: For each VersionEdit record in the MANIFEST:
- Decode the edit
- Route to the correct CF's VersionBuilder via the column_family_ field
- Handle CF add/drop operations
- Apply file additions/deletions via VersionBuilder::Apply()

Step 4: Handler finalization via CheckIterationResult():
- Load table handlers for all newly added files
- Force final version creation via MaybeCreateVersionBeforeApplyEdit(..., true), which calls SaveTo() and AppendVersion() for each CF

Step 5: Post-replay epoch number repair (separate from handler finalization):
- VersionSet::Recover() or TryRecoverFromOneManifest() calls RecoverEpochNumbers() to infer missing epoch numbers for files from older MANIFEST versions
- This happens after handler iteration succeeds, not during it

## Handler Classes

| Class | Purpose |
|-------|---------|
| VersionEditHandler | Normal recovery: replays all MANIFEST edits, fails on missing files |
| VersionEditHandlerPointInTime | Best-effort recovery: rolls back to last consistent state if files are missing |
| ManifestTailer | Tails MANIFEST for secondary/follower instances, processing new edits as they appear |
| DumpManifestHandler | Debug tool: prints each VersionEdit while replaying (used by ldb dump_manifest) |
| ListColumnFamiliesHandler | Scans MANIFEST to enumerate column family names without full recovery |
| FileChecksumRetriever | Extracts file checksums from MANIFEST without full recovery |

## VersionEditHandler (Normal Recovery)

The standard handler requires all referenced files to exist. If a file is missing, recovery fails with a Corruption status, unless no_error_if_files_missing is set or paranoid_checks is false (which relaxes table loading failures).

Key behaviors:
- Column families listed in column_families parameter are opened; others are tracked in do_not_open_column_families_ and must be dropped by later edits (in non-read-only mode)
- ExtractInfoFromVersionEdit() updates global state (sequence numbers, file numbers, etc.)
- MaybeHandleFileBoundariesForNewFiles() adjusts file boundaries when user_defined_timestamps_persisted is false: the smallest boundary is padded with the minimum timestamp, while the largest boundary is padded with the maximum timestamp when it is a range-tombstone sentinel. It can also force user_defined_timestamps_persisted=false for legacy files when UDT persistence is being enabled.

## Best-Effort Recovery (VersionEditHandlerPointInTime)

Used when Options::best_efforts_recovery is true. This handler maintains save points in each VersionBuilder and rolls back to the last known-good state when files are missing:

Step 1: Before applying each edit, MaybeCreateVersionBeforeApplyEdit() checks validity via ValidVersionAvailable(), then calls CreateOrReplaceSavePoint() to snapshot the pre-edit builder state.

Step 2: The edit is applied via Apply(). Missing-file detection happens during VersionBuilder::Apply() through ApplyFileAddition() and ApplyBlobFileAddition(), which call VerifyFile() and VerifyBlobFile().

Step 3: After applying, validity is re-checked. If the pre-edit state was valid but the post-edit state is not (the edit made it invalid), a Version is created from the save point (the pre-edit state).

Step 4: At the end, CheckIterationResult() verifies a valid Version exists. If allow_incomplete_valid_version is true, a Version missing only a suffix of L0 files is acceptable (subject to the constraints documented in 05_version_builder.md).

### Atomic Group Handling

VersionEditHandlerPointInTime uses atomic_update_versions_ to ensure all-or-nothing atomic group recovery:
- OnAtomicGroupReplayBegin() sets in_atomic_group_ to true
- Version updates within the group are buffered in atomic_update_versions_
- atomic_update_versions_missing_ counts CFs not yet updated
- When all CFs in the group have been processed, AtomicUpdateVersionsApply() installs them all at once
- If recovery fails mid-group, the buffered updates are discarded

## ManifestTailer

Used by secondary and follower instances via ReactiveVersionSet. The tailer has two modes:

| Mode | Description |
|------|-------------|
| kRecovery | Initial full MANIFEST replay |
| kCatchUp | Incremental tailing of new edits |

Key differences from the base handler:
- MustOpenAllColumnFamilies() returns false in both modes
- In catch-up mode, OnColumnFamilyAdd() ignores new CFs that do not already exist in the local ColumnFamilySet; only CFs already present are tracked
- PrepareToReadNewManifest() resets state for switching to a new MANIFEST file
- GetUpdatedColumnFamilies() returns CFs modified since the last read
- GetAndClearIntermediateFiles() returns files that were added then deleted during tailing (for cleanup by the follower to prevent file leaks)

## TryRecover (Multi-MANIFEST Recovery)

VersionSet::TryRecover() implements a more aggressive recovery strategy:

Step 1: Try the latest MANIFEST first.

Step 2: If that fails, try previous MANIFEST files in reverse chronological order.

Step 3: Use VersionEditHandlerPointInTime for each attempt to find the most recent consistent point.

This is useful when the latest MANIFEST is corrupted but an older one is still valid.

## Epoch Number Recovery

MANIFEST files from older RocksDB versions may contain files with missing epoch numbers (kUnknownEpochNumber = 0). VersionStorageInfo::RecoverEpochNumbers() handles this:

Step 1: Check if any file has a missing epoch number via HasMissingEpochNumber().

Step 2: If missing, reset all files' epoch numbers based on L0 ordering (sequence-number-based sorting is used temporarily by VersionBuilder before epoch inference).

Step 3: Reserve epoch 1 for ingest-behind files (kReservedEpochNumberForFileIngestedBehind).

Step 4: Advance ColumnFamilyData::next_epoch_number_ past the maximum assigned epoch.

The EpochNumberRequirement enum controls whether missing epoch numbers are an error (kMustPresent) or acceptable (kMightMissing).
