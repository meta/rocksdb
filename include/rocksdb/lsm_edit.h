//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;

// EXPERIMENTAL
//
// One external SST file to install, together with the level it will occupy.
// See DB::ApplyLsmEdit().
struct LsmEditFile {
  // Path to the external SST file. The file is linked, moved or copied into
  // the DB directory according to LsmEditOptions.
  std::string path;

  // The level this file will occupy after the edit. RocksDB does not search
  // for a level: the file is installed here or the edit fails, except when
  // LsmEditOptions::allow_replacement_file_relocation permits moving eligible
  // sequence-zero range-replacement files upward. Files declared at the same
  // level > 0 must not overlap each other unless merge_incoming_files is set,
  // and must not overlap any surviving file already at that level, unless
  // merge_existing_files
  // replaces that file with a merged output. Files declared at level 0 may
  // overlap; among those, later entries in LsmEdit::add_files are treated as
  // newer, matching the order L0 files are consulted on a read. When files at
  // different levels overlap, every entry affecting the same key in the
  // higher level must have a greater sequence number. In a preserved-sequence
  // edit, sequence number zero is accepted in overlapping point files and
  // the declared level order then determines which value is newer, but a
  // range tombstone must still carry a sequence number high enough to delete
  // the point keys it covers.
  // Equal-key/sequence merge operands must be consolidated into one incoming
  // merged level; they cannot be retained in separate runs.
  // See LsmEdit::add_files for range replacement's global sequence assignment.
  int level = 0;

  // Whole-file checksum of `path` and the name of the function that produced
  // it. Both must be non-empty or both empty, and an edit must supply them for
  // either every file or no files. See LsmEditOptions::verify_file_checksum
  // for how they are used.
  std::string checksum;
  std::string checksum_func_name;

  // Reset this input's entry sequence numbers to zero while merging. Useful
  // for a snapshot built in a different DB whose local sequences must not
  // outrank concurrent updates. Requires merge_incoming_files and point-only
  // snapshot inputs (no merge operands or range tombstones). Only the latest
  // point version in each reset input is retained; later inputs break ties.
  bool zero_sequence_numbers = false;
};

// One live SST file to remove, identified by its current level and file
// number. See LsmEdit::deletion_mode.
struct LsmEditFileDeletion {
  int level = 0;
  uint64_t file_number = 0;
};

enum class LsmEditDeletionMode {
  // Delete every existing file wholly contained in LsmEdit::range. Retain and
  // optionally probe files that cross a range boundary.
  kFilesInRange,

  // Delete exactly LsmEdit::delete_files. The range is a safety boundary, not
  // a statement that all existing contents inside it are being replaced.
  kExplicitFiles,
};

// EXPERIMENTAL
//
// Options for DB::ApplyLsmEdit(), applying to every LsmEdit in the call.
struct LsmEditOptions {
  // Hard-link the files into the DB instead of copying them, falling back to a
  // copy when the filesystem cannot link across the two locations. The source
  // paths are removed on success. Cannot be combined with `link_files`.
  bool move_files = false;

  // Hard-link the files into the DB instead of copying them, leaving the
  // source paths in place. Appropriate when the files belong to a live DB that
  // the caller will drop separately. Cannot be combined with `move_files`.
  bool link_files = false;

  // Recompute each file's whole-file checksum and compare it against the
  // LsmEditFile::checksum supplied by the caller. Only meaningful when the DB
  // has a file_checksum_gen_factory and the caller supplied checksums.
  bool verify_file_checksum = true;

  // Flush any memtable holding keys inside an edit's range, so the edit is not
  // shadowed by unflushed writes. When false, such an edit fails rather than
  // stalling writes for a flush.
  bool allow_blocking_flush = true;

  // Populate the block cache with blocks read while installing the files.
  // Grafted data is usually cold at install time, so the default avoids
  // evicting live working-set blocks.
  bool fill_cache = false;

  // In kFilesInRange mode, how to treat an existing file whose key range
  // crosses an edit's range boundary while lying partly outside it
  // ("straddling"). Such a file cannot simply be dropped, because the part
  // outside the range must survive.
  //
  // When true, RocksDB probes the straddling file for keys inside the range
  // when the replacement files' sequence numbers must be preserved, or when
  // exact declared levels leave no room for a covering tombstone.
  // The file is retained untouched if it holds none, so an edit whose range is
  // logically empty succeeds even though the range is not aligned to file
  // boundaries. Each probe costs roughly one seek.
  //
  // When false, such a straddling file fails the edit. A replacement whose
  // files all support global sequence assignment instead uses a covering
  // tombstone when placement permits it. See LsmEdit::add_files.
  bool probe_straddling_files = true;

  // Temperature hint for reading the files being installed.
  Temperature file_temperature = Temperature::kUnknown;

  // Max threads used to open the installed files while committing.
  int file_opening_threads = 16;

  // Allow an eligible sequence-zero range replacement to move files toward
  // L0 when their declared levels leave no room for a covering tombstone.
  // Relative level order and L0 input order are preserved, with L0 serving as
  // overflow. Other edits always use their exact declared levels.
  bool allow_replacement_file_relocation = false;

  // Compact incoming files assigned to each level into one output SST using
  // that level's table/compression options. Existing destination files are
  // never merged or implicitly removed unless merge_existing_files is set.
  // Higher sequences win; for equal sequences, later incoming files take
  // precedence. Deletes, merge operands
  // and range tombstones retain their normal RocksDB semantics. The output is
  // built during preparation and installed atomically with the rest of the
  // edit. This rewrites incoming data and is opt-in.
  // If equal-key/sequence merge operands cannot be consolidated with their
  // available base or by partial merging, the edit returns NotSupported.
  // Merged outputs preserve entry sequences rather than using external SST
  // V2 global-sequence assignment. Range replacement therefore probes retained
  // straddlers instead of generating a covering replacement tombstone.
  bool merge_incoming_files = false;

  // Include live destination SSTs whose boundaries overlap the merged output
  // at its declared level. Replace those SSTs atomically with the merged file,
  // retaining their keys outside the incoming range and their entry sequences.
  // Effective live-file sequences include MANIFEST-assigned global overrides.
  // Requires merge_incoming_files and an add-only kExplicitFiles edit. A
  // concurrent replacement/compaction of a selected file causes TryAgain.
  // Selection includes files between incoming runs and expands transitively
  // until the output has no same-level boundary overlap. Thus it can rewrite
  // much more data than the incoming files contain. Higher sequences win;
  // equal-sequence incoming entries take precedence over existing entries.
  // Selected files must also use self-contained blobs; references to separate
  // blob files return NotSupported.
  bool merge_existing_files = false;

  // Reject active snapshots because importing preserved sequences can change
  // their view. False permits add-only kExplicitFiles edits, or bounded
  // kFilesInRange edits with merge_incoming_files and probe_straddling_files
  // enabled. The latter must remove no destination files and every retained
  // straddler must hold no point entries or range tombstones in the range;
  // otherwise the edit fails. This is an import into a verified empty range,
  // not replacement of existing data. The caller must ensure that old
  // snapshots cannot read the newly imported keyspace, or accept that those
  // snapshots may observe imported data. Deleting destination data requires
  // the default true setting.
  bool snapshot_consistency = true;
};

// EXPERIMENTAL
//
// A range-scoped edit to one column family. See DB::ApplyLsmEdit().
struct LsmEdit {
  ColumnFamilyHandle* column_family = nullptr;

  // The key range this edit may affect, half-open: `start` inclusive, `limit`
  // exclusive. Every added or explicitly deleted file must lie inside it.
  // Leaving both endpoints unset means the whole column family; setting only
  // one is not supported.
  // With merge_existing_files, this bounds the original incoming files, not
  // the merged output: selected destination files can extend outside it, and
  // their out-of-range keys are retained.
  RangeOpt range;

  // Whether RocksDB derives deletions from `range` or applies the explicit
  // `delete_files` list. In kFilesInRange mode, `delete_files` must be empty
  // and the range's logical contents are replaced by `add_files`. In
  // kExplicitFiles mode, unlisted files inside the range remain present.
  LsmEditDeletionMode deletion_mode = LsmEditDeletionMode::kFilesInRange;

  // Existing files to remove in kExplicitFiles mode. Each file must still be
  // live at the declared level when the edit commits and must be wholly
  // contained in `range`. File numbers must not be repeated.
  std::vector<LsmEditFileDeletion> delete_files;

  // Files to add inside `range`, each at its declared level. This may be empty
  // when kExplicitFiles mode removes at least one file.
  //
  // In kFilesInRange mode, if every file has only sequence-zero entries and
  // supports the external SST V2 global sequence property, RocksDB assigns
  // fresh global sequences, ordering deeper levels before shallower levels
  // and preserving input order within L0. Straddling files are retained and
  // an internal range-tombstone SST hides their old data. It is placed below
  // every replacement run and above every retained overlapping file; L0 can
  // hold all runs when relocation is allowed. Its sequence is newer than
  // existing data and older than every replacement file. The SST
  // contents are not rewritten. This requires range-deletion support and room
  // for the tombstone, or LsmEditOptions::allow_replacement_file_relocation.
  //
  // Other edits preserve every input file's sequence numbers, including mixed
  // edits and DB-generated SSTs that contain only sequence-zero entries.
  // RocksDB validates sequence-number ordering between overlapping added files
  // and surviving live files. Disjoint sequence-number ranges use a
  // metadata-only fast path, but ambiguous pairs require scanning their
  // contents. A file can be scanned once per ambiguous overlapping peer,
  // producing quadratic work in the worst case.
  std::vector<LsmEditFile> add_files;
};

}  // namespace ROCKSDB_NAMESPACE
