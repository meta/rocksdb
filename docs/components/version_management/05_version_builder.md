# VersionBuilder

**Files:** `db/version_builder.h`, `db/version_builder.cc`

## Overview

VersionBuilder efficiently accumulates a sequence of VersionEdits and produces a new VersionStorageInfo. It avoids creating intermediate Version objects for each edit, which is important during recovery when replaying thousands of MANIFEST records.

## Apply/SaveTo Workflow

Step 1: Construct a VersionBuilder from an existing Version's VersionStorageInfo as the base.

Step 2: Call Apply(edit) for each VersionEdit. This records file additions and deletions without modifying the base. Validates add/delete consistency (e.g., deleted file must exist, added file must not already exist at a different level, no duplicate additions).

Step 3: Call SaveTo(new_vstorage) to materialize the final state. Merges the base storage info with accumulated additions/deletions. Files from the base that weren't deleted are carried forward; new files are inserted at their designated levels. When force_consistency_checks is enabled (or in debug builds unconditionally), SaveTo() runs CheckConsistency() to validate file ordering and overlap constraints (non-overlapping for L1+, sorted key ranges).

Step 4: Call LoadTableHandlers() to open table readers for newly added SST files. This can be parallelized across threads via max_threads. Called after SaveTo().

## BaseReferencedVersionBuilder

BaseReferencedVersionBuilder is a convenience wrapper that refs the current Version in its constructor and unrefs in the destructor. Both operations require the DB mutex. This ensures the base Version stays alive while the builder accumulates edits.

## Save Points

VersionBuilder supports one save point via CreateOrReplaceSavePoint(). This is used by VersionEditHandlerPointInTime during best-effort recovery:

Step 1: Before applying each edit, CreateOrReplaceSavePoint() snapshots the current builder state (moves the current mutable Rep into savepoint_ and clones it). This captures the pre-edit state regardless of whether all files are present.

Step 2: The edit is then applied via Apply(). If the applied edit causes a previously valid state to become invalid (e.g., references a missing file), the save point provides a rollback target representing the last valid pre-edit state.

Step 3: SaveSavePointTo() materializes the save point state into a new VersionStorageInfo, and LoadSavePointTableHandlers() opens its table readers.

Only one save point exists at a time. Calling CreateOrReplaceSavePoint() again replaces the previous one. Whether the save point represents a usable Version is determined separately by ValidVersionAvailable().

## Incomplete Valid Versions

When allow_incomplete_valid_version is true, a Version missing only a suffix of L0 SST files (and their associated blob files) may be considered valid, subject to additional constraints:

- The version must not have been modified by an atomic group edit (edited_in_atomic_group_ must be false)
- Missing SST files must be restricted to L0 only; any missing non-L0 SST invalidates the version
- Missing L0 SSTs must form a suffix in the expected L0 ordering
- Missing blob files are only tolerated when they are older than the minimum needed blob file or linked only to the missing L0 suffix

This supports best-effort recovery where the most recently flushed data may be missing but older data remains consistent.

ValidVersionAvailable() checks whether the builder can produce a valid Version by first checking for a complete version, then falling back to the incomplete-suffix check if the above constraints are met. HasMissingFiles() reports whether any files are missing.

## Intermediate File Tracking

When track_found_and_missing_files is enabled, the builder tracks files that are added and then deleted within the same sequence of edits. These "intermediate files" can be cleaned up. GetAndClearIntermediateFiles() returns a mutable reference to the intermediate file paths vector. Despite the name, the function does not clear the vector; the caller is responsible for clearing it after use.

## Consistency Validation

Validation is split across Apply() and SaveTo():

During Apply(), the builder validates:
- A deleted file must exist in the base or have been previously added
- An added file must not already exist at a different level
- No duplicate file additions

During SaveTo(), when force_consistency_checks is enabled (default: true) or in debug builds, CheckConsistency() validates:
- File ordering constraints for each level (non-overlapping key ranges for L1+)
- Correct key range boundaries across files
- Sequence number ordering within levels
