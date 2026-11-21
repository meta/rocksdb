//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_meta.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_edit_handler.h"
#include "db/version_set.h"
#include "db/version_util.h"
#include "port/port.h"
#include "table/table_reader.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/hash_containers.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

Status ApplyBlobRelocationCountDelta(size_t current, int64_t signed_delta,
                                     size_t* result) {
  assert(result != nullptr);
  // A count is the number of routes to one relocation file. It is far below the
  // size_t and int64_t limits, so the addition below cannot overflow.
  if (signed_delta >= 0) {
    *result = current + static_cast<size_t>(signed_delta);
    return Status::OK();
  }
  const uint64_t magnitude = static_cast<uint64_t>(-signed_delta);
  if (magnitude > current) {
    return Status::Corruption("Invalid blob relocation route count");
  }
  *result = current - static_cast<size_t>(magnitude);
  return Status::OK();
}

}  // namespace

// Scalar/flag state that a VersionEdit can mutate while being applied. Factored
// into a small struct so the single-edit undo (VersionBuilder::Rep::EditUndo)
// can snapshot and restore all of it with a single copy/swap, and so the field
// list is defined once rather than duplicated between the live state and its
// undo record. VersionBuilder::Rep inherits this (as does EditUndo) so the
// scalar state can be snapshotted/restored via up_cast<MutableScalars> with a
// single copy/swap; Rep is a file-local implementation class, so exposing the
// base is inconsequential.
struct MutableScalars {
  // Whether there are invalid new files or invalid deletions on levels larger
  // than num_levels_.
  bool has_invalid_levels_ = false;

  // The fields below are only meaningful when track_found_and_missing_files_ is
  // enabled.

  // The highest file number among all missing blob files, or
  // kInvalidBlobFileNumber if none are missing. Useful to check whether a
  // complete Version is available.
  uint64_t missing_blob_files_high_ = kInvalidBlobFileNumber;
  // Cached result of the last validity check: true if all the files making up
  // the Version can be found. Or, when allow_incomplete_valid_version_ is true
  // and the version was never edited in an atomic group, true if only a suffix
  // of L0 SST files and their associated blob files are missing.
  bool valid_version_available_ = false;
  // True if the version was ever edited in an atomic group.
  bool edited_in_atomic_group_ = false;
  // True if the Version was updated since the last validity check, so the
  // cached `valid_version_available_` must be recomputed on the next check
  // rather than reused.
  bool version_updated_since_last_check_ = false;

  // The per-CF blob indirection format fence: 0 when it is not set, else
  // kBlobIndirectionManifestFormatVersion (2). Records for indirect-origin blob
  // files, relocation files and routes need it. Once set, it is never cleared,
  // and every Version this builder saves inherits it.
  uint32_t blob_indirection_format_version_ = 0;
};

class VersionBuilder::Rep : public MutableScalars {
  class NewestFirstBySeqNo {
   public:
    bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const {
      assert(lhs);
      assert(rhs);

      if (lhs->fd.largest_seqno != rhs->fd.largest_seqno) {
        return lhs->fd.largest_seqno > rhs->fd.largest_seqno;
      }

      if (lhs->fd.smallest_seqno != rhs->fd.smallest_seqno) {
        return lhs->fd.smallest_seqno > rhs->fd.smallest_seqno;
      }

      // Break ties by file number
      return lhs->fd.GetNumber() > rhs->fd.GetNumber();
    }
  };

  class NewestFirstByEpochNumber {
   private:
    inline static const NewestFirstBySeqNo seqno_cmp;

   public:
    bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const {
      assert(lhs);
      assert(rhs);

      if (lhs->epoch_number != rhs->epoch_number) {
        return lhs->epoch_number > rhs->epoch_number;
      } else {
        return seqno_cmp(lhs, rhs);
      }
    }
  };
  class BySmallestKey {
   public:
    explicit BySmallestKey(const InternalKeyComparator* cmp) : cmp_(cmp) {}

    bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const {
      assert(lhs);
      assert(rhs);
      assert(cmp_);

      const int r = cmp_->Compare(lhs->smallest, rhs->smallest);
      if (r != 0) {
        return (r < 0);
      }

      // Break ties by file number
      return (lhs->fd.GetNumber() < rhs->fd.GetNumber());
    }

   private:
    const InternalKeyComparator* cmp_;
  };

  struct LevelState {
    std::unordered_set<uint64_t> deleted_files;
    // Map from file number to file meta data.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
  };

  // A class that represents the accumulated changes (like additional garbage or
  // newly linked/unlinked SST files) for a given blob file after applying a
  // series of VersionEdits.
  class BlobFileMetaDataDelta {
   public:
    bool IsEmpty() const {
      return !additional_garbage_count_ && !additional_garbage_bytes_ &&
             newly_linked_ssts_.empty() && newly_unlinked_ssts_.empty();
    }

    uint64_t GetAdditionalGarbageCount() const {
      return additional_garbage_count_;
    }

    uint64_t GetAdditionalGarbageBytes() const {
      return additional_garbage_bytes_;
    }

    const std::unordered_set<uint64_t>& GetNewlyLinkedSsts() const {
      return newly_linked_ssts_;
    }

    const std::unordered_set<uint64_t>& GetNewlyUnlinkedSsts() const {
      return newly_unlinked_ssts_;
    }

    void AddGarbage(uint64_t count, uint64_t bytes) {
      additional_garbage_count_ += count;
      additional_garbage_bytes_ += bytes;
    }

    void LinkSst(uint64_t sst_file_number) {
      assert(newly_linked_ssts_.find(sst_file_number) ==
             newly_linked_ssts_.end());

      // Reconcile with newly unlinked SSTs on the fly. (Note: an SST can be
      // linked to and unlinked from the same blob file in the case of a trivial
      // move.)
      auto it = newly_unlinked_ssts_.find(sst_file_number);

      if (it != newly_unlinked_ssts_.end()) {
        newly_unlinked_ssts_.erase(it);
      } else {
        newly_linked_ssts_.emplace(sst_file_number);
      }
    }

    void UnlinkSst(uint64_t sst_file_number) {
      assert(newly_unlinked_ssts_.find(sst_file_number) ==
             newly_unlinked_ssts_.end());

      // Reconcile with newly linked SSTs on the fly. (Note: an SST can be
      // linked to and unlinked from the same blob file in the case of a trivial
      // move.)
      auto it = newly_linked_ssts_.find(sst_file_number);

      if (it != newly_linked_ssts_.end()) {
        newly_linked_ssts_.erase(it);
      } else {
        newly_unlinked_ssts_.emplace(sst_file_number);
      }
    }

   private:
    uint64_t additional_garbage_count_ = 0;
    uint64_t additional_garbage_bytes_ = 0;
    std::unordered_set<uint64_t> newly_linked_ssts_;
    std::unordered_set<uint64_t> newly_unlinked_ssts_;
  };

  // A class that represents the state of a blob file after applying a series of
  // VersionEdits. In addition to the resulting state, it also contains the
  // delta (see BlobFileMetaDataDelta above). The resulting state can be used to
  // identify obsolete blob files, while the delta makes it possible to
  // efficiently detect trivial moves.
  class MutableBlobFileMetaData {
   public:
    // To be used for brand new blob files
    explicit MutableBlobFileMetaData(
        std::shared_ptr<SharedBlobFileMetaData>&& shared_meta)
        : shared_meta_(std::move(shared_meta)) {}

    // To be used for pre-existing blob files
    explicit MutableBlobFileMetaData(
        const std::shared_ptr<BlobFileMetaData>& meta)
        : shared_meta_(meta->GetSharedMeta()),
          linked_ssts_(meta->GetLinkedSsts()),
          garbage_blob_count_(meta->GetGarbageBlobCount()),
          garbage_blob_bytes_(meta->GetGarbageBlobBytes()) {}

    const std::shared_ptr<SharedBlobFileMetaData>& GetSharedMeta() const {
      return shared_meta_;
    }

    uint64_t GetBlobFileNumber() const {
      assert(shared_meta_);
      return shared_meta_->GetBlobFileNumber();
    }

    bool HasDelta() const { return !delta_.IsEmpty(); }

    const std::unordered_set<uint64_t>& GetLinkedSsts() const {
      return linked_ssts_;
    }

    uint64_t GetGarbageBlobCount() const { return garbage_blob_count_; }

    uint64_t GetGarbageBlobBytes() const { return garbage_blob_bytes_; }

    bool AddGarbage(uint64_t count, uint64_t bytes) {
      assert(shared_meta_);

      if (garbage_blob_count_ + count > shared_meta_->GetTotalBlobCount() ||
          garbage_blob_bytes_ + bytes > shared_meta_->GetTotalBlobBytes()) {
        return false;
      }

      delta_.AddGarbage(count, bytes);

      garbage_blob_count_ += count;
      garbage_blob_bytes_ += bytes;

      return true;
    }

    void LinkSst(uint64_t sst_file_number) {
      delta_.LinkSst(sst_file_number);

      assert(linked_ssts_.find(sst_file_number) == linked_ssts_.end());
      linked_ssts_.emplace(sst_file_number);
    }

    void UnlinkSst(uint64_t sst_file_number) {
      delta_.UnlinkSst(sst_file_number);

      assert(linked_ssts_.find(sst_file_number) != linked_ssts_.end());
      linked_ssts_.erase(sst_file_number);
    }

   private:
    std::shared_ptr<SharedBlobFileMetaData> shared_meta_;
    // Accumulated changes
    BlobFileMetaDataDelta delta_;
    // Resulting state after applying the changes
    BlobFileMetaData::LinkedSsts linked_ssts_;
    uint64_t garbage_blob_count_ = 0;
    uint64_t garbage_blob_bytes_ = 0;
  };

  const FileOptions& file_options_;
  const ImmutableCFOptions* const ioptions_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  VersionSet* version_set_;
  int num_levels_;
  LevelState* levels_;
  // Store sizes of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::unordered_map<int, size_t> invalid_level_sizes_;
  // Current levels of table files affected by additions/deletions.
  std::unordered_map<uint64_t, int> table_file_levels_;
  // Current compact cursors that should be changed after the last compaction
  std::unordered_map<int, InternalKey> updated_compact_cursors_;
  const NewestFirstByEpochNumber level_zero_cmp_by_epochno_;
  const NewestFirstBySeqNo level_zero_cmp_by_seqno_;
  const BySmallestKey level_nonzero_cmp_;

  // Mutable metadata objects for all blob files affected by the series of
  // version edits.
  std::map<uint64_t, MutableBlobFileMetaData> mutable_blob_file_metas_;

  // Dropped blob files (see VersionStorageInfo::GetDroppedBlobFiles())
  // recorded by the series of version edits.
  std::unordered_set<uint64_t> dropped_blob_files_;
  // The smallest oldest_blob_file_number among the SSTs that the last
  // SaveTableFilesTo() call saved. SaveBlobFilesAndCheckTo() drops every blob
  // file with a smaller number, because no saved SST references it. If no saved
  // SST references a blob file, the value is kInvalidBlobFileNumber and
  // SaveBlobFilesAndCheckTo() drops all blob files. The two steps are separate
  // public calls, and ProcessManifestWrites() releases the DB mutex between
  // them. SaveTableFilesTo() is const, so it keeps the value in a mutable
  // member.
  mutable uint64_t saved_min_oldest_blob_file_num_ = kInvalidBlobFileNumber;
#ifndef NDEBUG
  // Debug check of the call order. SaveTableFilesTo() sets it, and the next
  // SaveBlobFilesAndCheckTo() checks and clears it. So
  // saved_min_oldest_blob_file_num_ is never used before a save, and never used
  // twice for one save.
  mutable bool table_files_saved_ = false;
#endif  // NDEBUG

  // The relocation file and route changes this builder made on top of the base
  // Version. The base Version is immutable, so only the changes are kept here.
  // A deletion of a base entry is a null pointer in the relocation file map and
  // std::nullopt in the route map.
  //
  // How a relocation change reaches the new Version:
  //   1. VersionBuilder::Apply() calls ApplyBlobRelocationState(), which
  //      checks one VersionEdit in local staged maps. Only if every check
  //      passes does it copy the staged changes into the change maps below
  //      (the *_changes_ fields; they hold the changes of all VersionEdits this
  //      builder applied).
  //   2. SaveBlobFilesAndCheckTo() starts from the base Version's shared
  //      relocation state. It syncs the blob file entries of the file table,
  //      then replays the change maps through
  //      VersionStorageInfo::AddBlobRelocationFile(),
  //      DeleteBlobRelocationRoute(), DeleteBlobRelocationFile() and
  //      AddBlobRelocationRoute(), in the order explained in
  //      SaveBlobFilesAndCheckTo().
  //   3. BlobRelocationState buffers these table changes in its pending map
  //      (the first change copies the state; the base stays unchanged).
  //   4. VersionStorageInfo::SealBlobRelocationState() calls
  //      BlobRelocationState::Seal(), which turns the buffer into one
  //      BlobFileTable::Apply() call. BlobFileTable::Apply() rebuilds only the
  //      changed shards.
  //
  // Relocation files, keyed by file number. A null pointer marks a deletion.
  // (A std::optional here would make the undo map below hold an optional of an
  // optional of a shared_ptr, which gcc 13 wrongly reports as maybe
  // uninitialized.)
  std::map<uint64_t, std::shared_ptr<BlobRelocationFileMetaData>>
      blob_relocation_file_changes_;
  // Routes, keyed by the route's old file number.
  std::map<uint64_t, std::optional<BlobRelocationRoute>>
      blob_relocation_route_changes_;
  // Per relocation file, the change in the number of routes to it.
  std::map<uint64_t, int64_t> blob_relocation_target_count_deltas_;

  std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr_;

  ColumnFamilyData* cfd_;
  VersionEditHandler* version_edit_handler_;
  bool track_found_and_missing_files_;
  // True once the consistency check of base_vstorage_ has passed. The base
  // Version never changes during the builder's life, so Apply() checks it only
  // until the first check passes. A failed check leaves this false, so the next
  // Apply() reports the problem again.
  bool base_checked_ = false;
  // If false, only a complete Version with all files consisting it found is
  // considered valid. If true, besides complete Version, if the Version is
  // never edited in an atomic group, an incomplete Version with only a suffix
  // of L0 files missing is also considered valid.
  bool allow_incomplete_valid_version_;
  // See RecordMissingBlobFilesAsDropped().
  bool record_missing_blob_files_as_dropped_ = false;

  // These are only tracked if `track_found_and_missing_files_` is enabled.

  // The SST files that are found (blob files not included yet).
  std::unordered_set<uint64_t> found_files_;
  // Missing SST files for L0
  std::unordered_set<uint64_t> l0_missing_files_;
  // Missing SST files for non L0 levels
  std::unordered_set<uint64_t> non_l0_missing_files_;
  // Intermediate SST files (blob files not included yet)
  std::vector<std::string> intermediate_files_;
  // Missing blob files, useful to check if only the missing L0 files'
  // associated blob files are missing.
  std::unordered_set<uint64_t> missing_blob_files_;
  // Relocation files that recovery could not find or verify. Best-efforts
  // recovery can accept a Version that misses only a suffix of L0 files and the
  // blob files that only those L0 files use. A missing relocation file never
  // qualifies: unlike a normal blob file, it has no linked-SST metadata to
  // prove that only the missing L0 files use it.
  UnorderedSet<uint64_t> missing_blob_relocation_files_;
  // Counts of oldest_blob_file_number over the SSTs that SaveSSTFilesTo()
  // would save (see ForEachSavedSst()), for MinOldestBlobFileOfSavedSsts().
  // Built by one scan the first time a fenced best-efforts check needs it, so
  // a column family without the fence never pays for it. After that,
  // ApplyFileAddition() and ApplyFileDeletion() keep it up to date. Undo and
  // redo (ToggleUndo()) drop it, and the next check builds it again.
  mutable std::map<uint64_t, uint64_t> saved_oldest_counts_;
  mutable bool saved_oldest_counts_built_ = false;
  // Indirect-origin blob files that a blob file retirement record removed from
  // the Version. Their metadata stays in mutable_blob_file_metas_, but
  // AddBlobFileIfNeeded() does not save them. A route may use a retired file's
  // number as its old file number.
  UnorderedSet<uint64_t> retired_blob_files_;

  // Note: additional scalar/flag state mutated while applying edits
  // (has_invalid_levels_, missing_blob_files_high_, valid_version_available_,
  // edited_in_atomic_group_, version_updated_since_last_check_) lives in the
  // MutableScalars base so the single-edit undo can snapshot/restore it in one
  // copy/swap.

  // End of fields that are only tracked when `track_found_and_missing_files_`
  // is enabled.

  // Records how to undo (and redo) the most recent `Apply()` so that
  // VersionEditHandlerPointInTime can build a `Version` reflecting the state
  // *before* an edit (on a valid->invalid negative edge) without copying the
  // whole `Rep` on every edit. Only populated when
  // `track_found_and_missing_files_` is true.
  //
  // Each recorded map/set entry stores the "other side" value (the value not
  // currently live). Applying `ToggleUndo()` swaps every recorded slot with the
  // live state, so it is self-inverse: one call rolls back to the pre-edit
  // state, a second call redoes to the post-edit state. Objects momentarily
  // removed from the live state (e.g. blob metadata) are held inside these
  // records via value/shared_ptr, so nothing referenced by the pre-edit state
  // is destroyed during the tentative window. `FileMetaData` removed from
  // `added_files` by a deletion is instead kept alive via `deferred_unref` and
  // only released by `CommitLastApply()`.
  struct EditUndo : MutableScalars {
    // True while the live state has been rolled back to the pre-edit state
    // (i.e. `ToggleUndo()` has run an odd number of times).
    bool rolled_back = false;

    // Each map below records, for the keys the edit touches, the value on the
    // "other side" (the value not currently live): before the edit while in the
    // forward/post-edit state, and after the edit once rolled back. `nullopt`
    // means the key is absent on that side. `ToggleUndo()` swaps each recorded
    // entry with the corresponding live container, so it is self-inverse.

    // levels_[level].added_files entries, keyed by (level, file number). Holds
    // the FileMetaData* on the non-live side (the map only owns the pointer,
    // not the object; see `deferred_unref` for lifetime of deleted files).
    std::map<std::pair<int, uint64_t>, std::optional<FileMetaData*>>
        added_files;
    // levels_[level].deleted_files membership, keyed by (level, file number);
    // value is whether the file number is present on the non-live side.
    std::map<std::pair<int, uint64_t>, bool> deleted_files;
    // table_file_levels_ entries, keyed by file number (level, or absent).
    UnorderedMap<uint64_t, std::optional<int>> table_file_levels;
    // invalid_level_sizes_ entries, keyed by level (count, or absent).
    UnorderedMap<int, std::optional<size_t>> invalid_level_sizes;
    // updated_compact_cursors_ entries, keyed by level (cursor key, or absent).
    UnorderedMap<int, std::optional<InternalKey>> compact_cursors;
    // mutable_blob_file_metas_ entries, keyed by blob file number. Holds the
    // MutableBlobFileMetaData by value on the non-live side; because it owns a
    // shared_ptr to the SharedBlobFileMetaData, a rolled-back blob addition is
    // kept alive here rather than destroyed (which would spuriously mark the
    // blob obsolete).
    UnorderedMap<uint64_t, std::optional<MutableBlobFileMetaData>> blob_metas;
    // The three maps below hold blob_relocation_file_changes_,
    // blob_relocation_route_changes_ and blob_relocation_target_count_deltas_
    // entries. The outer optional is empty when the edit map had no entry for
    // the key on the non-live side. A deletion entry is a null pointer in the
    // first map and an empty inner optional in the second.
    std::map<uint64_t,
             std::optional<std::shared_ptr<BlobRelocationFileMetaData>>>
        blob_relocation_files;
    std::map<uint64_t, std::optional<std::optional<BlobRelocationRoute>>>
        blob_relocation_routes;
    std::map<uint64_t, std::optional<int64_t>> blob_relocation_target_counts;
    // Membership on the non-live side for each tracking set, keyed by file
    // number (blob file number for `missing_blob_files`).
    UnorderedMap<uint64_t, bool> found_files;
    UnorderedMap<uint64_t, bool> l0_missing_files;
    UnorderedMap<uint64_t, bool> non_l0_missing_files;
    UnorderedMap<uint64_t, bool> missing_blob_files;
    UnorderedMap<uint64_t, bool> dropped_blob_files;
    UnorderedMap<uint64_t, bool> missing_blob_relocation_files;
    UnorderedMap<uint64_t, bool> retired_blob_files;

    // `intermediate_files_` is append-only within an `Apply()`. Restore by
    // truncating to `intermediate_files_size` (rollback) / re-appending
    // `intermediate_tail` (redo).
    size_t intermediate_files_size = 0;
    std::vector<std::string> intermediate_tail;

    // (Scalar/flag state on the non-live side is held in the MutableScalars
    // base subobject.)

    // `FileMetaData` removed from `added_files` by a deletion in this edit. The
    // ref is held here (not dropped) until `CommitLastApply()`, so a rollback
    // can reinstall the file.
    std::vector<FileMetaData*> deferred_unref;
  };
  std::unique_ptr<EditUndo> undo_;

 public:
  Rep(const FileOptions& file_options, const ImmutableCFOptions* ioptions,
      TableCache* table_cache, VersionStorageInfo* base_vstorage,
      VersionSet* version_set,
      std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr,
      ColumnFamilyData* cfd, VersionEditHandler* version_edit_handler,
      bool track_found_and_missing_files, bool allow_incomplete_valid_version)
      : file_options_(file_options),
        ioptions_(ioptions),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        version_set_(version_set),
        num_levels_(base_vstorage->num_levels()),
        level_zero_cmp_by_epochno_(),
        level_zero_cmp_by_seqno_(),
        level_nonzero_cmp_(base_vstorage_->InternalComparator()),
        file_metadata_cache_res_mgr_(file_metadata_cache_res_mgr),
        cfd_(cfd),
        version_edit_handler_(version_edit_handler),
        track_found_and_missing_files_(track_found_and_missing_files),
        allow_incomplete_valid_version_(allow_incomplete_valid_version) {
    assert(ioptions_);

    blob_indirection_format_version_ =
        base_vstorage_->GetBlobIndirectionFormatVersion();

    levels_ = new LevelState[num_levels_];
    if (track_found_and_missing_files_) {
      assert(cfd_);
      assert(version_edit_handler_);
      // `track_found_and_missing_files_` mode used by VersionEditHandlerPIT
      // assumes the initial base version is valid. For best efforts recovery,
      // base will be empty. For manifest tailing usage like secondary instance,
      // they do not allow incomplete version, so the base version in subsequent
      // catch up attempts should be valid too.
      valid_version_available_ = true;
      edited_in_atomic_group_ = false;
      version_updated_since_last_check_ = false;
    }
  }

  // Rep is only ever held via std::unique_ptr and is not copied. (The previous
  // save-point mechanism deep-copied Rep on every edit, which was quadratic;
  // it has been replaced by single-edit undo, see EditUndo above.)
  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;
  Rep(Rep&&) = delete;
  Rep& operator=(Rep&&) = delete;

  ~Rep() {
    // Release any FileMetaData whose deletion was deferred by an uncommitted
    // Apply(), and ensure the live state is in the forward (post-edit) position
    // so the loop below unrefs exactly the files still owned by added_files.
    CommitLastApply();
    for (int level = 0; level < num_levels_; level++) {
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        UnrefFile(pair.second);
      }
    }

    delete[] levels_;
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->fd.pinned_reader.Get() != nullptr) {
        assert(table_cache_ != nullptr);
        // NOTE: have to release in raw cache interface to avoid using a
        // TypedHandle for PinnedTableReader's internal handle
        f->fd.pinned_reader.Release(table_cache_->get_cache().get());
      }

      // Evict from table cache to prevent leaked entries for files whose
      // last reference is held by the VersionBuilder (e.g., files from a
      // failed LogAndApply that were never installed in any Version).
      // Release() above only releases the pinned handle's ref but does not
      // remove the entry from the cache hash table.
      if (table_cache_ != nullptr) {
        TableCache::Evict(table_cache_->get_cache().get(), f->fd.GetNumber());
      }

      if (file_metadata_cache_res_mgr_) {
        Status s = file_metadata_cache_res_mgr_->UpdateCacheReservation(
            f->ApproximateMemoryUsage(), false /* increase */);
        s.PermitUncheckedError();
      }
      delete f;
    }
  }

  //======== Single-edit undo support (track_found_and_missing_files_) =======//
  // See the EditUndo declaration above for the overall model.

  // Records the pre-edit value of `map[key]` into `saved` (first-touch only, so
  // repeated touches of the same key within one edit keep the true pre-edit
  // value). The stored value is the "other side" for ToggleMap().
  template <typename Map, typename Saved, typename K>
  static void RecordMapEntry(const Map& map, Saved& saved, const K& key) {
    if (saved.find(key) != saved.end()) {
      return;
    }
    auto it = map.find(key);
    saved.emplace(key,
                  it != map.end()
                      ? std::optional<typename Map::mapped_type>(it->second)
                      : std::nullopt);
  }

  // Swaps every recorded entry between `map` and `saved` (self-inverse).
  template <typename Map, typename Saved>
  static void ToggleMapEntries(Map& map, Saved& saved) {
    for (auto& elem : saved) {
      const auto& key = elem.first;
      auto& other = elem.second;
      // The branches swap without a local std::optional of the mapped type.
      // emplace() and swap() also avoid requiring a default-constructible
      // mapped_type (e.g. MutableBlobFileMetaData has no default constructor).
      auto it = map.find(key);
      if (it != map.end()) {
        if (other.has_value()) {
          using std::swap;
          swap(it->second, *other);
        } else {
          other.emplace(std::move(it->second));
          map.erase(it);
        }
      } else if (other.has_value()) {
        map.emplace(key, std::move(*other));
        other.reset();
      }
    }
  }

  template <typename Set, typename Saved, typename K>
  static void RecordSetMembership(const Set& set, Saved& saved, const K& key) {
    if (saved.find(key) != saved.end()) {
      return;
    }
    saved.emplace(key, set.find(key) != set.end());
  }

  template <typename Set, typename Saved>
  static void ToggleSetMembership(Set& set, Saved& saved) {
    for (auto& elem : saved) {
      const auto& key = elem.first;
      bool& other_present = elem.second;
      bool live_present = set.find(key) != set.end();
      if (other_present) {
        set.insert(key);
      } else {
        set.erase(key);
      }
      other_present = live_present;
    }
  }

  void RecordAddedFileEntry(int level, uint64_t fn) {
    assert(undo_);
    assert(level < num_levels_);
    auto key = std::make_pair(level, fn);
    if (undo_->added_files.find(key) != undo_->added_files.end()) {
      return;
    }
    const auto& map = levels_[level].added_files;
    auto it = map.find(fn);
    undo_->added_files.emplace(
        key, it != map.end() ? std::optional<FileMetaData*>(it->second)
                             : std::nullopt);
  }

  void RecordDeletedFileEntry(int level, uint64_t fn) {
    assert(undo_);
    assert(level < num_levels_);
    auto key = std::make_pair(level, fn);
    if (undo_->deleted_files.find(key) != undo_->deleted_files.end()) {
      return;
    }
    const auto& set = levels_[level].deleted_files;
    undo_->deleted_files.emplace(key, set.find(fn) != set.end());
  }

  // Snapshots the pre-edit value of every piece of state that `edit` will
  // mutate, so the edit can later be rolled back / redone. Must be called
  // before the edit is applied and only in tracking mode.
  void CaptureUndo(const VersionEdit& edit) {
    assert(track_found_and_missing_files_);
    assert(!undo_);
    undo_ = std::make_unique<EditUndo>();
    undo_->intermediate_files_size = intermediate_files_.size();
    // Snapshot the pre-edit scalars (the MutableScalars base subobject).
    up_cast<MutableScalars>(*undo_) = up_cast<MutableScalars>(*this);

    auto touch_table_file = [&](int level, uint64_t fn) {
      RecordMapEntry(table_file_levels_, undo_->table_file_levels, fn);
      RecordSetMembership(found_files_, undo_->found_files, fn);
      RecordSetMembership(l0_missing_files_, undo_->l0_missing_files, fn);
      RecordSetMembership(non_l0_missing_files_, undo_->non_l0_missing_files,
                          fn);
      if (level >= 0 && level < num_levels_) {
        RecordAddedFileEntry(level, fn);
        RecordDeletedFileEntry(level, fn);
      } else if (level >= num_levels_) {
        RecordMapEntry(invalid_level_sizes_, undo_->invalid_level_sizes, level);
      }
    };
    auto touch_blob_file = [&](uint64_t blob_file_number) {
      if (blob_file_number == kInvalidBlobFileNumber) {
        return;
      }
      RecordMapEntry(mutable_blob_file_metas_, undo_->blob_metas,
                     blob_file_number);
      RecordSetMembership(missing_blob_files_, undo_->missing_blob_files,
                          blob_file_number);
    };

    for (const auto& ba : edit.GetBlobFileAdditions()) {
      touch_blob_file(ba.GetBlobFileNumber());
    }
    for (const auto& bg : edit.GetBlobFileGarbages()) {
      touch_blob_file(bg.GetBlobFileNumber());
    }
    for (const uint64_t blob_file_number : edit.GetDroppedBlobFiles()) {
      RecordSetMembership(dropped_blob_files_, undo_->dropped_blob_files,
                          blob_file_number);
    }
    for (uint64_t file_number : edit.GetBlobFileRetirements()) {
      touch_blob_file(file_number);
      RecordSetMembership(retired_blob_files_, undo_->retired_blob_files,
                          file_number);
    }
    for (const BlobRelocationFileAddition& addition :
         edit.GetBlobRelocationFileAdditions()) {
      RecordMapEntry(blob_relocation_file_changes_,
                     undo_->blob_relocation_files, addition.GetFileNumber());
      RecordSetMembership(missing_blob_files_, undo_->missing_blob_files,
                          addition.GetFileNumber());
      RecordSetMembership(missing_blob_relocation_files_,
                          undo_->missing_blob_relocation_files,
                          addition.GetFileNumber());
    }
    for (uint64_t file_number : edit.GetBlobRelocationFileDeletions()) {
      RecordMapEntry(blob_relocation_file_changes_,
                     undo_->blob_relocation_files, file_number);
      RecordSetMembership(missing_blob_relocation_files_,
                          undo_->missing_blob_relocation_files, file_number);
    }
    for (uint64_t file_number : edit.GetBlobPhysicalFileLosses()) {
      RecordMapEntry(blob_relocation_file_changes_,
                     undo_->blob_relocation_files, file_number);
      RecordSetMembership(missing_blob_relocation_files_,
                          undo_->missing_blob_relocation_files, file_number);
    }
    const auto touch_route = [&](uint64_t old_file_number,
                                 const BlobRelocationRoute* replacement) {
      RecordMapEntry(blob_relocation_route_changes_,
                     undo_->blob_relocation_routes, old_file_number);
      const auto touch_count = [&](uint64_t target) {
        RecordMapEntry(blob_relocation_target_count_deltas_,
                       undo_->blob_relocation_target_counts, target);
      };
      const uint64_t current = GetBlobRelocationRouteTarget(old_file_number);
      if (current != kInvalidBlobFileNumber) {
        touch_count(current);
      }
      if (replacement != nullptr) {
        touch_count(replacement->GetTargetFileNumber());
      }
    };
    for (const BlobRelocationRoute& route : edit.GetBlobRelocationRoutes()) {
      touch_route(route.GetOldFileNumber(), &route);
    }
    for (uint64_t old_file_number : edit.GetBlobRelocationRouteDeletions()) {
      // A null replacement distinguishes deletion from route replacement.
      touch_route(old_file_number, nullptr);
    }
    for (const auto& deleted_file : edit.GetDeletedFiles()) {
      const int level = deleted_file.first;
      const uint64_t fn = deleted_file.second;
      touch_table_file(level, fn);
      // Only reachable blob metadata is touched (mirrors ApplyFileDeletion).
      if (level >= 0 && level < num_levels_ &&
          GetCurrentLevelForTableFile(fn) == level) {
        touch_blob_file(GetOldestBlobFileNumberForTableFile(level, fn));
      }
    }
    for (const auto& new_file : edit.GetNewFiles()) {
      const int level = new_file.first;
      const FileMetaData& meta = new_file.second;
      touch_table_file(level, meta.fd.GetNumber());
      touch_blob_file(meta.oldest_blob_file_number);
    }
    for (const auto& cursor : edit.GetCompactCursors()) {
      const int level = cursor.first;
      if (level >= 0 && level < num_levels_) {
        RecordMapEntry(updated_compact_cursors_, undo_->compact_cursors, level);
      }
    }
  }

  // Swaps the live state with the recorded "other side", moving between the
  // post-edit and pre-edit states. Self-inverse.
  void ToggleUndo() {
    assert(undo_);
    for (auto& elem : undo_->added_files) {
      const int level = elem.first.first;
      const uint64_t fn = elem.first.second;
      auto& other = elem.second;
      auto& map = levels_[level].added_files;
      auto it = map.find(fn);
      std::optional<FileMetaData*> live =
          it != map.end() ? std::optional<FileMetaData*>(it->second)
                          : std::nullopt;
      if (other.has_value()) {
        map[fn] = *other;
      } else if (it != map.end()) {
        // Note: erasing the map entry does not delete the FileMetaData; its
        // lifetime is managed via `deferred_unref` / the built Version's ref.
        map.erase(it);
      }
      other = live;
    }
    for (auto& elem : undo_->deleted_files) {
      const int level = elem.first.first;
      const uint64_t fn = elem.first.second;
      bool& other_present = elem.second;
      auto& set = levels_[level].deleted_files;
      bool live_present = set.find(fn) != set.end();
      if (other_present) {
        set.insert(fn);
      } else {
        set.erase(fn);
      }
      other_present = live_present;
    }
    ToggleMapEntries(table_file_levels_, undo_->table_file_levels);
    ToggleMapEntries(invalid_level_sizes_, undo_->invalid_level_sizes);
    ToggleMapEntries(updated_compact_cursors_, undo_->compact_cursors);
    ToggleMapEntries(mutable_blob_file_metas_, undo_->blob_metas);
    ToggleMapEntries(blob_relocation_file_changes_,
                     undo_->blob_relocation_files);
    ToggleMapEntries(blob_relocation_route_changes_,
                     undo_->blob_relocation_routes);
    ToggleMapEntries(blob_relocation_target_count_deltas_,
                     undo_->blob_relocation_target_counts);
    ToggleSetMembership(found_files_, undo_->found_files);
    ToggleSetMembership(l0_missing_files_, undo_->l0_missing_files);
    ToggleSetMembership(non_l0_missing_files_, undo_->non_l0_missing_files);
    ToggleSetMembership(missing_blob_files_, undo_->missing_blob_files);
    ToggleSetMembership(dropped_blob_files_, undo_->dropped_blob_files);
    ToggleSetMembership(missing_blob_relocation_files_,
                        undo_->missing_blob_relocation_files);
    ToggleSetMembership(retired_blob_files_, undo_->retired_blob_files);

    if (!undo_->rolled_back) {
      // Forward -> pre-edit: peel off the entries appended by this edit.
      for (size_t i = undo_->intermediate_files_size;
           i < intermediate_files_.size(); ++i) {
        undo_->intermediate_tail.push_back(std::move(intermediate_files_[i]));
      }
      intermediate_files_.resize(undo_->intermediate_files_size);
    } else {
      // Pre-edit -> forward: re-append.
      for (auto& s : undo_->intermediate_tail) {
        intermediate_files_.push_back(std::move(s));
      }
      undo_->intermediate_tail.clear();
    }

    // Swap the scalar/flag state (the MutableScalars base subobject).
    std::swap(up_cast<MutableScalars>(*this), up_cast<MutableScalars>(*undo_));
    // The SST sets changed back or forward; rebuild the counts on next use.
    saved_oldest_counts_built_ = false;

    undo_->rolled_back = !undo_->rolled_back;
  }

  // Reverts the most recent Apply() to the pre-edit state.
  void RollbackLastApply() {
    assert(undo_);
    assert(!undo_->rolled_back);
    ToggleUndo();
  }

  // Re-applies the most recent Apply() after a RollbackLastApply().
  void RedoLastApply() {
    assert(undo_);
    assert(undo_->rolled_back);
    ToggleUndo();
  }

  // Finalizes the most recent Apply(): frees FileMetaData whose deletion was
  // deferred and drops the undo record. Safe to call with no pending edit.
  void CommitLastApply() {
    if (!undo_) {
      return;
    }
    if (undo_->rolled_back) {
      // Always finalize in the forward (post-edit) state.
      ToggleUndo();
    }
    for (FileMetaData* f : undo_->deferred_unref) {
      UnrefFile(f);
    }
    undo_.reset();
  }

  // Mapping used for checking the consistency of links between SST files and
  // blob files. It is built using the forward links (table file -> blob file),
  // and is subsequently compared with the inverse mapping stored in the
  // BlobFileMetaData objects.
  using ExpectedLinkedSsts =
      std::unordered_map<uint64_t, BlobFileMetaData::LinkedSsts>;

  static void UpdateExpectedLinkedSsts(
      uint64_t table_file_number, uint64_t blob_file_number,
      ExpectedLinkedSsts* expected_linked_ssts) {
    assert(expected_linked_ssts);

    if (blob_file_number == kInvalidBlobFileNumber) {
      return;
    }

    (*expected_linked_ssts)[blob_file_number].emplace(table_file_number);
  }

  template <typename Checker>
  Status CheckConsistencyDetailsForLevel(
      const VersionStorageInfo* vstorage, int level, Checker checker,
      const std::string& sync_point,
      ExpectedLinkedSsts* expected_linked_ssts) const {
#ifdef NDEBUG
    (void)sync_point;
#endif

    assert(vstorage);
    assert(level >= 0 && level < num_levels_);
    assert(expected_linked_ssts);

    const auto& level_files = vstorage->LevelFiles(level);

    if (level_files.empty()) {
      return Status::OK();
    }

    assert(level_files[0]);
    UpdateExpectedLinkedSsts(level_files[0]->fd.GetNumber(),
                             level_files[0]->oldest_blob_file_number,
                             expected_linked_ssts);

    for (size_t i = 1; i < level_files.size(); ++i) {
      assert(level_files[i]);
      UpdateExpectedLinkedSsts(level_files[i]->fd.GetNumber(),
                               level_files[i]->oldest_blob_file_number,
                               expected_linked_ssts);

      auto lhs = level_files[i - 1];
      auto rhs = level_files[i];

#ifndef NDEBUG
      auto pair = std::make_pair(&lhs, &rhs);
      TEST_SYNC_POINT_CALLBACK(sync_point, &pair);
#endif

      const Status s = checker(lhs, rhs);
      if (!s.ok()) {
        return s;
      }
    }

    return Status::OK();
  }

  // Make sure table files are sorted correctly and that the links between
  // table files and blob files are consistent.
  Status CheckConsistencyDetails(const VersionStorageInfo* vstorage) const {
    assert(vstorage);

    ExpectedLinkedSsts expected_linked_ssts;

    if (num_levels_ > 0) {
      const InternalKeyComparator* const icmp = vstorage->InternalComparator();
      EpochNumberRequirement epoch_number_requirement =
          vstorage->GetEpochNumberRequirement();
      assert(icmp);
      // Check L0
      {
        auto l0_checker = [this, epoch_number_requirement, icmp](
                              const FileMetaData* lhs,
                              const FileMetaData* rhs) {
          assert(lhs);
          assert(rhs);

          if (epoch_number_requirement ==
              EpochNumberRequirement::kMightMissing) {
            if (!level_zero_cmp_by_seqno_(lhs, rhs)) {
              std::ostringstream oss;
              oss << "L0 files are not sorted properly: files #"
                  << lhs->fd.GetNumber() << " with seqnos (largest, smallest) "
                  << lhs->fd.largest_seqno << " , " << lhs->fd.smallest_seqno
                  << ", #" << rhs->fd.GetNumber()
                  << " with seqnos (largest, smallest) "
                  << rhs->fd.largest_seqno << " , " << rhs->fd.smallest_seqno;
              return Status::Corruption("VersionBuilder", oss.str());
            }
          } else if (epoch_number_requirement ==
                     EpochNumberRequirement::kMustPresent) {
            if (lhs->epoch_number == rhs->epoch_number) {
              bool range_overlapped =
                  icmp->Compare(lhs->smallest, rhs->largest) <= 0 &&
                  icmp->Compare(lhs->largest, rhs->smallest) >= 0;

              if (range_overlapped) {
                std::ostringstream oss;
                oss << "L0 files of same epoch number but overlapping range #"
                    << lhs->fd.GetNumber()
                    << " , smallest key: " << lhs->smallest.DebugString(true)
                    << " , largest key: " << lhs->largest.DebugString(true)
                    << " , epoch number: " << lhs->epoch_number << " vs. file #"
                    << rhs->fd.GetNumber()
                    << " , smallest key: " << rhs->smallest.DebugString(true)
                    << " , largest key: " << rhs->largest.DebugString(true)
                    << " , epoch number: " << rhs->epoch_number;
                return Status::Corruption("VersionBuilder", oss.str());
              }
            }

            if (!level_zero_cmp_by_epochno_(lhs, rhs)) {
              std::ostringstream oss;
              oss << "L0 files are not sorted properly: files #"
                  << lhs->fd.GetNumber() << " with epoch number "
                  << lhs->epoch_number << ", #" << rhs->fd.GetNumber()
                  << " with epoch number " << rhs->epoch_number;
              return Status::Corruption("VersionBuilder", oss.str());
            }
          }

          return Status::OK();
        };

        const Status s = CheckConsistencyDetailsForLevel(
            vstorage, /* level */ 0, l0_checker,
            "VersionBuilder::CheckConsistency0", &expected_linked_ssts);
        if (!s.ok()) {
          return s;
        }
      }

      // Check L1 and up

      for (int level = 1; level < num_levels_; ++level) {
        auto checker = [this, level, icmp](const FileMetaData* lhs,
                                           const FileMetaData* rhs) {
          assert(lhs);
          assert(rhs);

          if (!level_nonzero_cmp_(lhs, rhs)) {
            std::ostringstream oss;
            oss << 'L' << level << " files are not sorted properly: files #"
                << lhs->fd.GetNumber() << ", #" << rhs->fd.GetNumber();

            return Status::Corruption("VersionBuilder", oss.str());
          }

          // Make sure there is no overlap in level
          if (icmp->Compare(lhs->largest, rhs->smallest) >= 0) {
            std::ostringstream oss;
            oss << 'L' << level << " has overlapping ranges: file #"
                << lhs->fd.GetNumber()
                << " largest key: " << lhs->largest.DebugString(true)
                << " vs. file #" << rhs->fd.GetNumber()
                << " smallest key: " << rhs->smallest.DebugString(true);

            return Status::Corruption("VersionBuilder", oss.str());
          }

          return Status::OK();
        };

        const Status s = CheckConsistencyDetailsForLevel(
            vstorage, level, checker, "VersionBuilder::CheckConsistency1",
            &expected_linked_ssts);
        if (!s.ok()) {
          return s;
        }
      }
    }

    // Make sure that all blob files in the version have non-garbage data and
    // the links between them and the table files are consistent.
    const auto& blob_files = vstorage->GetBlobFiles();
    for (const auto& blob_file_meta : blob_files) {
      assert(blob_file_meta);

      const uint64_t blob_file_number = blob_file_meta->GetBlobFileNumber();

      if (!blob_file_meta->IsIndirectOrigin() &&
          blob_file_meta->GetGarbageBlobCount() >=
              blob_file_meta->GetTotalBlobCount()) {
        std::ostringstream oss;
        oss << "Blob file #" << blob_file_number
            << " consists entirely of garbage";

        return Status::Corruption("VersionBuilder", oss.str());
      }

      if (blob_file_meta->GetLinkedSsts() !=
          expected_linked_ssts[blob_file_number]) {
        std::ostringstream oss;
        oss << "Links are inconsistent between table files and blob file #"
            << blob_file_number;

        return Status::Corruption("VersionBuilder", oss.str());
      }
    }

    Status ret_s;
    TEST_SYNC_POINT_CALLBACK("VersionBuilder::CheckConsistencyBeforeReturn",
                             &ret_s);
    return ret_s;
  }

  Status CheckConsistency(const VersionStorageInfo* vstorage) const {
    assert(vstorage);

    // Always run consistency checks in debug build
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      return Status::OK();
    }
#endif
    Status s = CheckConsistencyDetails(vstorage);
    if (s.IsCorruption() && s.getState()) {
      // Make it clear the error is due to force_consistency_checks = 1 or
      // debug build
#ifdef NDEBUG
      auto prefix = "force_consistency_checks";
#else
      auto prefix = "force_consistency_checks(DEBUG)";
#endif
      s = Status::Corruption(prefix, s.getState());
    } else {
      // was only expecting corruption with message, or OK
      assert(s.ok());
    }
    return s;
  }

  bool CheckConsistencyForNumLevels() const {
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }

    for (const auto& pair : invalid_level_sizes_) {
      const size_t level_size = pair.second;
      if (level_size != 0) {
        return false;
      }
    }

    return true;
  }

  bool IsBlobFileInVersion(uint64_t blob_file_number) const {
    auto mutable_it = mutable_blob_file_metas_.find(blob_file_number);
    if (mutable_it != mutable_blob_file_metas_.end()) {
      return true;
    }

    assert(base_vstorage_);
    const auto meta = base_vstorage_->GetBlobFileMetaData(blob_file_number);

    return !!meta;
  }

  MutableBlobFileMetaData* GetOrCreateMutableBlobFileMetaData(
      uint64_t blob_file_number) {
    auto mutable_it = mutable_blob_file_metas_.find(blob_file_number);
    if (mutable_it != mutable_blob_file_metas_.end()) {
      return &mutable_it->second;
    }

    assert(base_vstorage_);
    const auto meta = base_vstorage_->GetBlobFileMetaData(blob_file_number);

    if (meta) {
      mutable_it = mutable_blob_file_metas_
                       .emplace(blob_file_number, MutableBlobFileMetaData(meta))
                       .first;
      return &mutable_it->second;
    }

    return nullptr;
  }

  Status ApplyBlobFileAddition(const BlobFileAddition& blob_file_addition) {
    const uint64_t blob_file_number = blob_file_addition.GetBlobFileNumber();

    if (blob_file_addition.IsIndirectOrigin() &&
        blob_indirection_format_version_ !=
            kBlobIndirectionManifestFormatVersion) {
      return Status::Corruption(
          "Indirect-origin blob file requires the format fence");
    }

    if (HasBlobRelocationFileNumber(blob_file_number)) {
      return Status::Corruption(
          "Normal blob file conflicts with a relocation file");
    }
    if (GetBlobRelocationRouteTarget(blob_file_number) !=
        kInvalidBlobFileNumber) {
      return Status::Corruption("Normal blob file conflicts with a route");
    }

    if (IsBlobFileInVersion(blob_file_number)) {
      std::ostringstream oss;
      oss << "Blob file #" << blob_file_number << " already added";

      return Status::Corruption("VersionBuilder", oss.str());
    }

    auto deleter = [vs = version_set_, ioptions = ioptions_,
                    bc = cfd_ ? cfd_->blob_file_cache()
                              : nullptr](SharedBlobFileMetaData* shared_meta) {
      if (vs) {
        assert(ioptions);
        assert(!ioptions->cf_paths.empty());
        assert(shared_meta);

        vs->AddObsoleteBlobFile(shared_meta->GetBlobFileNumber(),
                                ioptions->cf_paths.front().path);
      }
      if (bc) {
        bc->Evict(shared_meta->GetBlobFileNumber());
      }

      delete shared_meta;
    };

    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, blob_file_addition.GetTotalBlobCount(),
        blob_file_addition.GetTotalBlobBytes(),
        blob_file_addition.GetChecksumMethod(),
        blob_file_addition.GetChecksumValue(),
        blob_file_addition.IsIndirectOrigin(), std::move(deleter));

    mutable_blob_file_metas_.emplace(
        blob_file_number, MutableBlobFileMetaData(std::move(shared_meta)));

    Status s;
    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      s = version_edit_handler_->VerifyBlobFile(cfd_, blob_file_number,
                                                blob_file_addition);
      if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
        missing_blob_files_high_ =
            std::max(missing_blob_files_high_, blob_file_number);
        missing_blob_files_.insert(blob_file_number);
        s = Status::OK();
      } else if (!s.ok()) {
        return s;
      }
    }

    return s;
  }

  Status ApplyBlobFileGarbage(const BlobFileGarbage& blob_file_garbage) {
    const uint64_t blob_file_number = blob_file_garbage.GetBlobFileNumber();

    MutableBlobFileMetaData* const mutable_meta =
        GetOrCreateMutableBlobFileMetaData(blob_file_number);

    if (!mutable_meta) {
      // The file is not in the Version. If it has a route, an indirect blob GC
      // install edit already moved its live values and wrote the blob file
      // retirement record of the file. A compaction that started before that
      // edit can still report garbage for the file. A garbage count has no use
      // for a removed file, so ignore it.
      if (GetBlobRelocationRouteTarget(blob_file_number) !=
          kInvalidBlobFileNumber) {
        return Status::OK();
      }
      std::ostringstream oss;
      oss << "Blob file #" << blob_file_number << " not found";

      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (!mutable_meta->AddGarbage(blob_file_garbage.GetGarbageBlobCount(),
                                  blob_file_garbage.GetGarbageBlobBytes())) {
      std::ostringstream oss;
      oss << "Garbage overflow for blob file #" << blob_file_number;
      return Status::Corruption("VersionBuilder", oss.str());
    }

    return Status::OK();
  }

  std::shared_ptr<BlobRelocationFileMetaData> GetBlobRelocationFile(
      uint64_t file_number) const {
    const auto edit = blob_relocation_file_changes_.find(file_number);
    if (edit != blob_relocation_file_changes_.end()) {
      return edit->second;
    }
    return base_vstorage_->GetBlobRelocationFileMetaData(file_number);
  }

  bool HasBlobRelocationFileNumber(uint64_t file_number) const {
    return blob_relocation_file_changes_.find(file_number) !=
               blob_relocation_file_changes_.end() ||
           base_vstorage_->GetBlobRelocationFileMetaData(file_number) !=
               nullptr;
  }

  bool IsBlobFileRetiredInBuilder(uint64_t file_number) const {
    return retired_blob_files_.count(file_number) != 0;
  }

  // True if an earlier edit applied to this builder deleted relocation file
  // `file_number`. It is not about blob file retirement.
  bool IsBlobRelocationFileDeletedInBuilder(uint64_t file_number) const {
    const auto edit = blob_relocation_file_changes_.find(file_number);
    if (edit != blob_relocation_file_changes_.end()) {
      return edit->second == nullptr;
    }
    // The base Version keeps no record of deleted files. This builder sees the
    // deletions applied to it: the whole MANIFEST at replay, or one LogAndApply
    // batch of version edits at runtime. At runtime, LogAndApply also checks
    // the earlier batches of the current MANIFEST
    // (VersionSet::RejectReaddedBlobRelocationState()).
    return false;
  }

  // Returns the relocation file that the route of old file `old_file_number`
  // points to, or kInvalidBlobFileNumber when `old_file_number` has no route.
  uint64_t GetBlobRelocationRouteTarget(uint64_t old_file_number) const {
    const auto edit = blob_relocation_route_changes_.find(old_file_number);
    if (edit != blob_relocation_route_changes_.end()) {
      return edit->second.has_value() ? edit->second->GetTargetFileNumber()
                                      : kInvalidBlobFileNumber;
    }
    return base_vstorage_->GetBlobRelocationRouteTarget(old_file_number);
  }

  // True if an earlier edit applied to this builder deleted the route of old
  // file `old_file_number` (a route deletion record).
  bool IsBlobRelocationRouteDeletedInBuilder(uint64_t old_file_number) const {
    const auto edit = blob_relocation_route_changes_.find(old_file_number);
    if (edit != blob_relocation_route_changes_.end()) {
      return !edit->second.has_value();
    }
    // The base Version keeps no record of deleted routes. This builder sees the
    // deletions applied to it: the whole MANIFEST at replay, or one LogAndApply
    // batch of version edits at runtime. At runtime, LogAndApply also checks
    // the earlier batches of the current MANIFEST
    // (VersionSet::RejectReaddedBlobRelocationState()).
    return false;
  }

  Status GetBlobRelocationTargetCount(uint64_t physical_file_number,
                                      int64_t additional_delta,
                                      size_t* count) const {
    assert(count != nullptr);
    const size_t base_count =
        base_vstorage_->GetBlobRelocationRouteTargetCount(physical_file_number);
    const auto delta =
        blob_relocation_target_count_deltas_.find(physical_file_number);
    const int64_t existing_delta =
        delta == blob_relocation_target_count_deltas_.end() ? 0 : delta->second;
    size_t current = 0;
    Status status =
        ApplyBlobRelocationCountDelta(base_count, existing_delta, &current);
    if (!status.ok()) {
      return status;
    }
    return ApplyBlobRelocationCountDelta(current, additional_delta, count);
  }

  std::shared_ptr<BlobRelocationFileMetaData>
  CreateVersionOwnedBlobRelocationFile(
      BlobRelocationFileAddition addition) const {
    auto deleter = [vs = version_set_, ioptions = ioptions_,
                    bc = cfd_ ? cfd_->blob_file_cache()
                              : nullptr](BlobRelocationFileMetaData* metadata) {
      if (vs != nullptr) {
        assert(ioptions != nullptr);
        assert(!ioptions->cf_paths.empty());
        assert(metadata != nullptr);
        vs->AddObsoleteBlobFile(metadata->GetFileNumber(),
                                ioptions->cf_paths.front().path);
      }
      // This runs under the DB mutex when the last Version that owns the file
      // is destroyed. It runs without the DB mutex when a failed LogAndApply
      // batch or ManifestTailer frees relocation state (see "Freeing
      // relocation state" at VersionSet::FreeBlobRelocationState()). Because
      // it can run under the DB mutex, it must not take the stripe mutex of
      // the file, which a relocation file open can hold while it reads its
      // location blocks. So it only drops a remembered failed open, and
      // PurgeObsoleteFiles() evicts the cached reader later (see
      // BlobFileCache::Evict()). While file deletions are disabled, no purge
      // runs, so the reader of an obsolete relocation file stays cached until
      // they are enabled again.
      if (bc != nullptr) {
        bc->ForgetFailedRelocationOpen(metadata->GetFileNumber());
      }
      delete metadata;
    };
    return BlobRelocationFileMetaData::Create(std::move(addition),
                                              std::move(deleter));
  }

  Status ApplyBlobFileRetirement(uint64_t file_number) {
    if (retired_blob_files_.count(file_number) != 0) {
      // A retirement record holds only the file number. A second record for a
      // file that an earlier edit of this builder already retired (for example,
      // in a replay) changes nothing, so accept it.
      return Status::OK();
    }
    MutableBlobFileMetaData* const meta =
        GetOrCreateMutableBlobFileMetaData(file_number);
    if (meta == nullptr || !meta->GetSharedMeta()->IsIndirectOrigin()) {
      return Status::Corruption(
          "Blob file retirement requires a live indirect-origin blob file");
    }
    retired_blob_files_.insert(file_number);
    if (track_found_and_missing_files_ &&
        missing_blob_files_.erase(file_number) != 0) {
      // Recovery applies the file's addition first. The file can already be
      // deleted from disk, so the addition put it in missing_blob_files_. The
      // retirement shows that the file left on purpose, so remove it from the
      // missing set and recompute missing_blob_files_high_. Otherwise the
      // Version looks incomplete. CaptureUndo() saved the set membership and
      // missing_blob_files_high_ for rollback.
      missing_blob_files_high_ = kInvalidBlobFileNumber;
      for (uint64_t missing_file_number : missing_blob_files_) {
        missing_blob_files_high_ =
            std::max(missing_blob_files_high_, missing_file_number);
      }
    }
    return Status::OK();
  }

  // Checks and applies the format fence and the relocation records of one
  // edit. Steps:
  //   1. Check the per-CF fence (records need it; the compaction style must
  //      allow it).
  //   2. Stage relocation file additions, deletions and accepted losses in
  //      local maps. A lost file stays in the Version, marked lost.
  //   3. Stage route sets and deletions, and the change in the route count of
  //      each target.
  //   4. Check the final state: no route to a deleted or missing file, no route
  //      from a live file, no chains, and at most 2^32 - 1 routes per target
  //      (the file table keeps a 32-bit route count).
  //   5. Only then commit the staged changes to the builder's change maps, so a
  //      bad edit changes nothing.
  Status ApplyBlobRelocationState(const VersionEdit& edit) {
    const bool has_records = !edit.GetBlobRelocationFileAdditions().empty() ||
                             !edit.GetBlobRelocationFileDeletions().empty() ||
                             !edit.GetBlobPhysicalFileLosses().empty() ||
                             !edit.GetBlobRelocationRoutes().empty() ||
                             !edit.GetBlobRelocationRouteDeletions().empty() ||
                             !edit.GetBlobFileRetirements().empty();
    if (!has_records && !edit.HasBlobIndirectionFormatVersion()) {
      return Status::OK();
    }
    // Step 1: check the per-CF fence.
    uint32_t final_format_version = blob_indirection_format_version_;
    if (edit.HasBlobIndirectionFormatVersion()) {
      const uint32_t requested = edit.GetBlobIndirectionFormatVersion();
      if (requested != kBlobIndirectionManifestFormatVersion ||
          (final_format_version != 0 && final_format_version != requested)) {
        return Status::Corruption("Invalid blob indirection format fence");
      }
      final_format_version = requested;
    }
    if (has_records &&
        final_format_version != kBlobIndirectionManifestFormatVersion) {
      return Status::Corruption(
          "Blob relocation records require the format fence");
    }
    if (ioptions_ != nullptr) {
      const Status s = CheckBlobIndirectionCompactionStyle(
          ioptions_->compaction_style, final_format_version != 0);
      if (!s.ok()) {
        return s;
      }
    }
    if (!has_records) {
      blob_indirection_format_version_ = final_format_version;
      return Status::OK();
    }

    // Step 2: stage relocation file additions, deletions and accepted losses.
    using StagedFile =
        std::optional<std::shared_ptr<BlobRelocationFileMetaData>>;
    using StagedRoute = std::optional<BlobRelocationRoute>;
    std::map<uint64_t, StagedFile> staged_files;
    std::map<uint64_t, StagedRoute> staged_routes;
    std::map<uint64_t, int64_t> staged_target_count_deltas;
    std::set<uint64_t> seen_file_additions;
    std::set<uint64_t> newly_created_files;
    const std::set<uint64_t> file_losses(
        edit.GetBlobPhysicalFileLosses().begin(),
        edit.GetBlobPhysicalFileLosses().end());
    if (file_losses.size() != edit.GetBlobPhysicalFileLosses().size() ||
        file_losses.count(kInvalidBlobFileNumber) != 0) {
      return Status::Corruption("Invalid or duplicate blob relocation loss");
    }
    std::set<uint64_t> seen_route_additions;
    std::set<uint64_t> new_table_file_numbers;
    std::set<uint64_t> new_blob_file_numbers;
    for (const auto& new_file : edit.GetNewFiles()) {
      new_table_file_numbers.insert(new_file.second.fd.GetNumber());
    }
    for (const BlobFileAddition& blob_addition : edit.GetBlobFileAdditions()) {
      new_blob_file_numbers.insert(blob_addition.GetBlobFileNumber());
    }

    const auto get_final_file = [&](uint64_t file_number) {
      const auto staged = staged_files.find(file_number);
      if (staged != staged_files.end()) {
        return staged->second.has_value() ? *staged->second : nullptr;
      }
      return GetBlobRelocationFile(file_number);
    };
    const auto get_final_route_target = [&](uint64_t old_file_number) {
      const auto staged = staged_routes.find(old_file_number);
      if (staged != staged_routes.end()) {
        return staged->second.has_value()
                   ? staged->second->GetTargetFileNumber()
                   : kInvalidBlobFileNumber;
      }
      return GetBlobRelocationRouteTarget(old_file_number);
    };

    for (const BlobRelocationFileAddition& addition :
         edit.GetBlobRelocationFileAdditions()) {
      if (!seen_file_additions.insert(addition.GetFileNumber()).second ||
          staged_files.find(addition.GetFileNumber()) != staged_files.end()) {
        return Status::Corruption("Duplicate blob relocation file edit");
      }
      Status status = addition.Validate();
      if (!status.ok()) {
        return status;
      }
      if (IsBlobFileInVersion(addition.GetFileNumber())) {
        return Status::Corruption(
            "Blob relocation file conflicts with a normal blob file");
      }
      if (GetCurrentLevelForTableFile(addition.GetFileNumber()) !=
          VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
        return Status::Corruption(
            "Blob relocation file conflicts with a table file");
      }
      if (new_table_file_numbers.count(addition.GetFileNumber()) != 0) {
        return Status::Corruption(
            "Blob relocation file conflicts with a table file addition");
      }
      if (new_blob_file_numbers.count(addition.GetFileNumber()) != 0) {
        return Status::Corruption(
            "Blob relocation file conflicts with a normal blob addition");
      }
      if (IsBlobRelocationFileDeletedInBuilder(addition.GetFileNumber())) {
        return Status::Corruption("Blob relocation file number was deleted");
      }
      const std::shared_ptr<BlobRelocationFileMetaData> current =
          GetBlobRelocationFile(addition.GetFileNumber());
      if (current != nullptr) {
        // A repeated record (MANIFEST replay or catch-up) must equal the file
        // the builder has. The record holds only small fields (numbers and the
        // file checksum strings), so the compare is cheap enough to run here,
        // under the DB mutex.
        if (current->GetAddition() != addition) {
          return Status::Corruption(
              "Conflicting blob relocation file addition");
        }
        continue;
      }
      bool marked_lost = file_losses.count(addition.GetFileNumber()) != 0;
#ifndef NDEBUG
      // Tests mark a relocation file lost while a DB opens; the open then
      // writes the loss into its new MANIFEST.
      std::pair<uint64_t, bool*> test_loss{addition.GetFileNumber(),
                                           &marked_lost};
      TEST_SYNC_POINT_CALLBACK("VersionBuilder::MarkBlobFileLostForTest",
                               &test_loss);
#endif  // NDEBUG
      if (track_found_and_missing_files_ && !marked_lost) {
        assert(version_edit_handler_ != nullptr);
        status =
            version_edit_handler_->VerifyBlobRelocationFile(cfd_, addition);
        const bool file_unusable = status.IsPathNotFound() ||
                                   status.IsNotFound() || status.IsCorruption();
        if (file_unusable && record_missing_blob_files_as_dropped_) {
          // best_efforts_blob_db_repair: a missing or corrupt relocation file
          // is marked lost, and the open writes the loss record into its new
          // MANIFEST. Base RocksDB has no relocation files, so it has no
          // dropped-file rule for them ("Lost relocation files" in
          // db/blob/blob_relocation_manifest.h says why the file is not
          // dropped). A missing relocation file does not cause a fallback in
          // time. If a fallback crosses an indirect blob GC edit,
          // CheckRepairFallbackAgainstGc() fails the open.
          marked_lost = true;
        } else if (file_unusable) {
          missing_blob_relocation_files_.insert(addition.GetFileNumber());
        } else if (!status.ok()) {
          return status;
        }
      }
      std::shared_ptr<BlobRelocationFileMetaData> added =
          BlobRelocationFileMetaData::Create(addition);
      newly_created_files.insert(addition.GetFileNumber());
      if (marked_lost) {
        added = BlobRelocationFileMetaData::CreateMarkedLost(std::move(added));
      }
      staged_files.emplace(addition.GetFileNumber(), std::move(added));
    }

    for (uint64_t file_number : file_losses) {
      const std::shared_ptr<BlobRelocationFileMetaData> current =
          get_final_file(file_number);
      // A loss record names only a relocation file. A normal or origin blob
      // file that is missing uses the base dropped-file record instead
      // (kDroppedBlobFile, see best_efforts_blob_db_repair).
      if (current == nullptr ||
          std::find(edit.GetBlobRelocationFileDeletions().begin(),
                    edit.GetBlobRelocationFileDeletions().end(), file_number) !=
              edit.GetBlobRelocationFileDeletions().end()) {
        return Status::Corruption("Blob relocation loss names no live target");
      }
      if (!current->IsMarkedLost()) {
        staged_files[file_number] =
            BlobRelocationFileMetaData::CreateMarkedLost(current);
      }
      if (track_found_and_missing_files_) {
        missing_blob_relocation_files_.erase(file_number);
      }
    }

    for (uint64_t file_number : edit.GetBlobRelocationFileDeletions()) {
      if (file_number == kInvalidBlobFileNumber ||
          seen_file_additions.count(file_number) != 0 ||
          staged_files.find(file_number) != staged_files.end()) {
        return Status::Corruption("Invalid blob relocation file deletion");
      }
      if (IsBlobRelocationFileDeletedInBuilder(file_number)) {
        // A repeated deletion record (MANIFEST replay or catch-up) changes
        // nothing, so accept it.
        if (track_found_and_missing_files_) {
          missing_blob_relocation_files_.erase(file_number);
        }
        continue;
      }
      // As for an SST deletion, the file must be live in this column family.
      // A deletion of another column family's file is a bad record.
      if (GetBlobRelocationFile(file_number) == nullptr) {
        return Status::Corruption(
            "Blob relocation file deletion names no live file");
      }
      staged_files.emplace(file_number, std::nullopt);
      if (track_found_and_missing_files_) {
        missing_blob_relocation_files_.erase(file_number);
      }
    }

    // Step 3: stage route sets and deletions.
    const auto stage_route = [&](uint64_t old_file_number,
                                 StagedRoute replacement) -> Status {
      if (staged_routes.find(old_file_number) != staged_routes.end()) {
        return Status::Corruption("Duplicate blob relocation route edit");
      }
      const uint64_t current = GetBlobRelocationRouteTarget(old_file_number);
      if (current != kInvalidBlobFileNumber) {
        staged_target_count_deltas[current] -= 1;
      }
      if (replacement.has_value()) {
        staged_target_count_deltas[replacement->GetTargetFileNumber()] += 1;
      }
      staged_routes.emplace(old_file_number, std::move(replacement));
      return Status::OK();
    };

    for (const BlobRelocationRoute& route : edit.GetBlobRelocationRoutes()) {
      const uint64_t old_file_number = route.GetOldFileNumber();
      if (!seen_route_additions.insert(old_file_number).second) {
        return Status::Corruption("Duplicate blob relocation route edit");
      }
      Status status = route.Validate();
      if (!status.ok()) {
        return status;
      }
      if (IsBlobRelocationRouteDeletedInBuilder(old_file_number)) {
        return Status::Corruption("Blob relocation route was deleted");
      }
      if (GetBlobRelocationRouteTarget(old_file_number) ==
          route.GetTargetFileNumber()) {
        // The route already exists with this target (a repeated record), so
        // there is nothing to stage.
        continue;
      }
      status = stage_route(old_file_number, route);
      if (!status.ok()) {
        return status;
      }
    }

    for (uint64_t old_file_number : edit.GetBlobRelocationRouteDeletions()) {
      if (old_file_number == kInvalidBlobFileNumber ||
          seen_route_additions.count(old_file_number) != 0 ||
          staged_routes.find(old_file_number) != staged_routes.end()) {
        return Status::Corruption("Invalid blob relocation route deletion");
      }
      if (IsBlobRelocationRouteDeletedInBuilder(old_file_number)) {
        // A repeated deletion record (MANIFEST replay or catch-up) changes
        // nothing, so accept it.
        continue;
      }
      Status status = stage_route(old_file_number, std::nullopt);
      if (!status.ok()) {
        return status;
      }
    }

    const auto get_final_target_count = [&](uint64_t physical_file_number,
                                            size_t* count) {
      const auto delta = staged_target_count_deltas.find(physical_file_number);
      return GetBlobRelocationTargetCount(
          physical_file_number,
          delta == staged_target_count_deltas.end() ? 0 : delta->second, count);
    };

    // Step 4: check the final state after this edit: no route points to a
    // deleted relocation file, and no added relocation file number is the old
    // file number of a route.
    for (const auto& file : staged_files) {
      if (file.second.has_value()) {
        if (get_final_route_target(file.first) != kInvalidBlobFileNumber) {
          return Status::Corruption(
              "Blob relocation file conflicts with a route");
        }
        continue;
      }
      size_t target_count = 0;
      Status status = get_final_target_count(file.first, &target_count);
      if (!status.ok()) {
        return status;
      }
      if (target_count != 0) {
        return Status::Corruption(
            "Deleted blob relocation file is still a route target");
      }
    }

    for (const auto& route_edit : staged_routes) {
      if (!route_edit.second.has_value()) {
        continue;
      }
      const BlobRelocationRoute& route = *route_edit.second;
      // The target must be a live relocation file and must not have a route of
      // its own: one hop, no chains.
      if (get_final_file(route.GetTargetFileNumber()) == nullptr) {
        return Status::Corruption("Invalid blob relocation route target");
      }
      if (get_final_route_target(route.GetTargetFileNumber()) !=
          kInvalidBlobFileNumber) {
        return Status::Corruption("Blob relocation route would form a chain");
      }
      size_t target_count = 0;
      Status count_status =
          get_final_target_count(route.GetTargetFileNumber(), &target_count);
      if (!count_status.ok()) {
        return count_status;
      }
      if (target_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("Too many routes to one relocation file");
      }
      // The old file number must not be a live file: not a live relocation
      // file, not a blob file in the Version, and not a blob file this edit
      // adds. A blob file removed by blob file retirement in this or an earlier
      // edit is not live.
      const uint64_t old_file_number = route.GetOldFileNumber();
      if (get_final_file(old_file_number) != nullptr ||
          (IsBlobFileInVersion(old_file_number) &&
           retired_blob_files_.count(old_file_number) == 0) ||
          new_blob_file_numbers.count(old_file_number) != 0) {
        return Status::Corruption(
            "Blob relocation route old file is a live file");
      }
      size_t incoming_count = 0;
      Status status = get_final_target_count(old_file_number, &incoming_count);
      if (!status.ok()) {
        return status;
      }
      if (incoming_count != 0) {
        return Status::Corruption("Blob relocation route would form a chain");
      }
    }

    // Step 5: every record passed the checks against the final state. Commit
    // them only now, so a bad record cannot leave part of the edit's file and
    // route changes in the builder.
    blob_indirection_format_version_ = final_format_version;
    for (auto& file : staged_files) {
      if (!file.second.has_value()) {
        blob_relocation_file_changes_[file.first] = nullptr;
        continue;
      }
      const std::shared_ptr<BlobRelocationFileMetaData>& staged = *file.second;
      if (newly_created_files.count(file.first) == 0) {
        blob_relocation_file_changes_[file.first] = staged;
      } else {
        std::shared_ptr<BlobRelocationFileMetaData> owned =
            CreateVersionOwnedBlobRelocationFile(staged->GetAddition());
        if (staged->IsMarkedLost()) {
          owned =
              BlobRelocationFileMetaData::CreateMarkedLost(std::move(owned));
        }
        blob_relocation_file_changes_[file.first] = std::move(owned);
      }
    }
    for (auto& route : staged_routes) {
      blob_relocation_route_changes_[route.first] = std::move(route.second);
    }
    for (const auto& delta : staged_target_count_deltas) {
      blob_relocation_target_count_deltas_[delta.first] += delta.second;
      if (blob_relocation_target_count_deltas_[delta.first] == 0) {
        blob_relocation_target_count_deltas_.erase(delta.first);
      }
    }
    return Status::OK();
  }

  int GetCurrentLevelForTableFile(uint64_t file_number) const {
    auto it = table_file_levels_.find(file_number);
    if (it != table_file_levels_.end()) {
      return it->second;
    }

    assert(base_vstorage_);
    return base_vstorage_->GetFileLocation(file_number).GetLevel();
  }

  uint64_t GetOldestBlobFileNumberForTableFile(int level,
                                               uint64_t file_number) const {
    assert(level < num_levels_);

    const auto& added_files = levels_[level].added_files;

    auto it = added_files.find(file_number);
    if (it != added_files.end()) {
      const FileMetaData* const meta = it->second;
      assert(meta);

      return meta->oldest_blob_file_number;
    }

    assert(base_vstorage_);
    const FileMetaData* const meta =
        base_vstorage_->GetFileMetaDataByNumber(file_number);
    assert(meta);

    return meta->oldest_blob_file_number;
  }

  Status ApplyFileDeletion(int level, uint64_t file_number) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (level != current_level) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot delete table file #" << file_number << " from level "
          << level << " since it is ";
      if (current_level ==
          VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
        oss << "not in the LSM tree";
      } else {
        oss << "on level " << current_level;
      }

      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      assert(invalid_level_sizes_[level] > 0);
      --invalid_level_sizes_[level];

      table_file_levels_[file_number] =
          VersionStorageInfo::FileLocation::Invalid().GetLevel();

      return Status::OK();
    }

    const uint64_t blob_file_number =
        GetOldestBlobFileNumberForTableFile(level, file_number);

    if (saved_oldest_counts_built_ &&
        blob_file_number != kInvalidBlobFileNumber &&
        CountsInSavedOldest(level, file_number)) {
      const auto count_it = saved_oldest_counts_.find(blob_file_number);
      assert(count_it != saved_oldest_counts_.end());
      if (count_it != saved_oldest_counts_.end() && --count_it->second == 0) {
        saved_oldest_counts_.erase(count_it);
      }
    }

    if (blob_file_number != kInvalidBlobFileNumber) {
      MutableBlobFileMetaData* const mutable_meta =
          GetOrCreateMutableBlobFileMetaData(blob_file_number);
      if (mutable_meta) {
        mutable_meta->UnlinkSst(file_number);
      }
    }

    auto& level_state = levels_[level];

    auto& add_files = level_state.added_files;
    auto add_it = add_files.find(file_number);
    if (add_it != add_files.end()) {
      if (undo_) {
        // Defer freeing so a rollback of this edit can reinstall the file.
        undo_->deferred_unref.push_back(add_it->second);
      } else {
        UnrefFile(add_it->second);
      }
      add_files.erase(add_it);
    }

    auto& del_files = level_state.deleted_files;
    assert(del_files.find(file_number) == del_files.end());
    del_files.emplace(file_number);

    table_file_levels_[file_number] =
        VersionStorageInfo::FileLocation::Invalid().GetLevel();

    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      if (l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
        l0_missing_files_.erase(file_number);
      } else if (non_l0_missing_files_.find(file_number) !=
                 non_l0_missing_files_.end()) {
        non_l0_missing_files_.erase(file_number);
      } else {
        auto fiter = found_files_.find(file_number);
        // Only mark new files added during this catchup attempt for deletion.
        // These files were never installed in VersionStorageInfo.
        // Already referenced files that are deleted by a VersionEdit will
        // be added to the VersionStorageInfo's obsolete files when the old
        // version is dereferenced.
        if (fiter != found_files_.end()) {
          assert(!ioptions_->cf_paths.empty());
          intermediate_files_.emplace_back(
              MakeTableFileName(ioptions_->cf_paths[0].path, file_number));
          found_files_.erase(fiter);
        }
      }
    }

    return Status::OK();
  }

  Status ApplyFileAddition(int level, const FileMetaData& meta) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const uint64_t file_number = meta.fd.GetNumber();

    if (HasBlobRelocationFileNumber(file_number)) {
      return Status::Corruption(
          "Table file conflicts with a blob relocation file");
    }

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (current_level !=
        VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot add table file #" << file_number << " to level " << level
          << " since it is already in the LSM tree on level " << current_level;
      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      ++invalid_level_sizes_[level];
      table_file_levels_[file_number] = level;

      return Status::OK();
    }

    auto& level_state = levels_[level];

    auto& del_files = level_state.deleted_files;
    auto del_it = del_files.find(file_number);
    if (del_it != del_files.end()) {
      del_files.erase(del_it);
    }

    FileMetaData* const f = new FileMetaData(meta);
    f->refs = 1;
    // A Version does not keep the in-memory blob reference summary of an SST
    // alive. The census delta takes the summary from the edit (see
    // VersionSet::ProcessManifestWrites()). Later readers read the reference
    // metadata from the SST file.
    f->blob_reference_summary.reset();
    if (blob_indirection_format_version_ ==
            kBlobIndirectionManifestFormatVersion &&
        f->blob_reference_descriptor.empty()) {
      // An empty descriptor means the SST has no exact reference metadata.
      // ValidateBlobReferenceMetadata() allows this only for a coarse SST
      // (frontier set: the SST records the oldest indirect-origin file number
      // it may reference; see "Coarse SST" in
      // db/blob/blob_reference_metadata.h), a level move of an SST already in
      // the Version, or an SST with the legacy marker. Mark every such SST
      // without a frontier as legacy, so an edit that copies its metadata (for
      // example a trivial move) carries the marker.
      f->blob_reference_legacy_metadata =
          f->oldest_coarse_indirect_origin_file_number ==
          kInvalidBlobFileNumber;
    }

    if (file_metadata_cache_res_mgr_) {
      Status s = file_metadata_cache_res_mgr_->UpdateCacheReservation(
          f->ApproximateMemoryUsage(), true /* increase */);
      if (!s.ok()) {
        delete f;
        s = Status::MemoryLimit(
            "Can't allocate " +
            kCacheEntryRoleToCamelString[lossless_cast<std::uint32_t>(
                CacheEntryRole::kFileMetadata)] +
            " due to exceeding the memory limit "
            "based on "
            "cache capacity");
        return s;
      }
    }

    auto& add_files = level_state.added_files;
    assert(add_files.find(file_number) == add_files.end());
    add_files.emplace(file_number, f);

    const uint64_t blob_file_number = f->oldest_blob_file_number;

    if (blob_file_number != kInvalidBlobFileNumber) {
      MutableBlobFileMetaData* const mutable_meta =
          GetOrCreateMutableBlobFileMetaData(blob_file_number);
      if (mutable_meta) {
        mutable_meta->LinkSst(file_number);
      }
    }

    table_file_levels_[file_number] = level;

    Status s;
    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      assert(!ioptions_->cf_paths.empty());
      const std::string fpath =
          MakeTableFileName(ioptions_->cf_paths[0].path, file_number);
      s = version_edit_handler_->VerifyFile(cfd_, fpath, level, meta);
      if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
        if (0 == level) {
          l0_missing_files_.insert(file_number);
        } else {
          non_l0_missing_files_.insert(file_number);
        }
        if (s.IsCorruption()) {
          found_files_.insert(file_number);
        }
        s = Status::OK();
      } else if (!s.ok()) {
        saved_oldest_counts_built_ = false;
        return s;
      } else {
        found_files_.insert(file_number);
      }
    }

    if (saved_oldest_counts_built_ &&
        blob_file_number != kInvalidBlobFileNumber &&
        CountsInSavedOldest(level, file_number)) {
      ++saved_oldest_counts_[blob_file_number];
    }

    return s;
  }

  Status ApplyCompactCursors(int level,
                             const InternalKey& smallest_uncompacted_key) {
    if (level < 0) {
      std::ostringstream oss;
      oss << "Cannot add compact cursor (" << level << ","
          << smallest_uncompacted_key.Encode().ToString()
          << " due to invalid level (level = " << level << ")";
      return Status::Corruption("VersionBuilder", oss.str());
    }
    if (level < num_levels_) {
      // Omit levels (>= num_levels_) when re-open with shrinking num_levels_
      updated_compact_cursors_[level] = smallest_uncompacted_key;
    }
    return Status::OK();
  }

  Status ValidateBlobReferenceMetadata(const VersionEdit& edit) const {
    uint32_t final_format_version = blob_indirection_format_version_;
    if (edit.HasBlobIndirectionFormatVersion()) {
      final_format_version = edit.GetBlobIndirectionFormatVersion();
    }

    for (const auto& new_file : edit.GetNewFiles()) {
      const FileMetaData& meta = new_file.second;
      if (meta.oldest_coarse_indirect_origin_file_number !=
          kInvalidBlobFileNumber) {
        if (final_format_version != kBlobIndirectionManifestFormatVersion ||
            meta.oldest_coarse_indirect_origin_file_number > kFileNumberMask ||
            meta.blob_reference_legacy_metadata ||
            !meta.blob_reference_descriptor.empty()) {
          return Status::Corruption("Invalid coarse blob reference metadata");
        }
        continue;
      }
      if (!meta.blob_reference_descriptor.empty()) {
        if (meta.blob_reference_legacy_metadata) {
          return Status::Corruption("Conflicting blob reference metadata");
        }
        if (final_format_version != kBlobIndirectionManifestFormatVersion) {
          return Status::Corruption(
              "Exact blob reference metadata requires the format fence");
        }
        BlobReferenceMetadataDescriptor descriptor;
        Status status = BlobReferenceMetadataDescriptor::DecodeFrom(
            meta.blob_reference_descriptor, &descriptor);
        if (!status.ok()) {
          return status;
        }
        continue;
      }
      if (meta.blob_reference_legacy_metadata) {
        if (final_format_version != kBlobIndirectionManifestFormatVersion) {
          return Status::Corruption(
              "Legacy blob reference metadata requires the format fence");
        }
        continue;
      }
      if (final_format_version != kBlobIndirectionManifestFormatVersion) {
        continue;
      }

      // The SST has no descriptor, no legacy marker and no coarse frontier. In
      // a fenced CF this is allowed only for a level move: the SST is already
      // in this Version and the same edit deletes it from its current level.
      // Such an SST can be older than the fence. Every SST built after the
      // fence has a descriptor, a coarse frontier, or the legacy marker
      // (blob_reference_legacy_metadata), also when it keeps no blob reference.
      const uint64_t file_number = meta.fd.GetNumber();
      const int current_level = GetCurrentLevelForTableFile(file_number);
      if (current_level ==
              VersionStorageInfo::FileLocation::Invalid().GetLevel() ||
          edit.GetDeletedFiles().find({current_level, file_number}) ==
              edit.GetDeletedFiles().end()) {
        return Status::Corruption(
            "An SST added after the format fence has no exact blob reference "
            "metadata");
      }
    }
    return Status::OK();
  }

  // Apply all of the edits in *edit to the current state.
  Status Apply(const VersionEdit* edit) {
    if (edit == nullptr) {
      return Status::InvalidArgument("VersionBuilder edit is null");
    }
    bool version_updated = false;
    if (!base_checked_) {
      TEST_SYNC_POINT("VersionBuilder::Apply:CheckBaseConsistency");
      const Status s = CheckConsistency(base_vstorage_);
      if (!s.ok()) {
        return s;
      }
      base_checked_ = true;
    }
    {
      const Status s = ValidateBlobReferenceMetadata(*edit);
      if (!s.ok()) {
        return s;
      }
    }

    // In tracking mode, snapshot how to undo this edit before mutating, so
    // VersionEditHandlerPointInTime can build a Version from the pre-edit state
    // (on a valid->invalid negative edge) via RollbackLastApply() instead of
    // copying the whole Rep on every edit.
    if (track_found_and_missing_files_) {
      CaptureUndo(*edit);
    }

    // Note: we process the blob file related changes first because the
    // table file addition/deletion logic depends on the blob files
    // already being there.

    // Apply blob file retirement records before the relocation records. For
    // example, an indirect blob GC install edit writes the blob file
    // retirement record of origin file 10 and adds a route from file 10 in the
    // same edit. The route check requires that file 10 is no longer live.
    {
      const std::vector<uint64_t>& retirements = edit->GetBlobFileRetirements();
      if (std::set<uint64_t>(retirements.begin(), retirements.end()).size() !=
          retirements.size()) {
        return Status::Corruption("Duplicate blob file retirement");
      }
    }
    for (uint64_t file_number : edit->GetBlobFileRetirements()) {
      const Status s = ApplyBlobFileRetirement(file_number);
      if (!s.ok()) {
        return s;
      }
      version_updated = true;
    }

    {
      const Status s = ApplyBlobRelocationState(*edit);
      if (!s.ok()) {
        return s;
      }
      if (edit->HasBlobIndirectionFormatVersion() ||
          !edit->GetBlobRelocationFileAdditions().empty() ||
          !edit->GetBlobRelocationFileDeletions().empty() ||
          !edit->GetBlobPhysicalFileLosses().empty() ||
          !edit->GetBlobRelocationRoutes().empty() ||
          !edit->GetBlobRelocationRouteDeletions().empty() ||
          !edit->GetBlobFileRetirements().empty()) {
        version_updated = true;
      }
    }

    // Add new blob files
    for (const auto& blob_file_addition : edit->GetBlobFileAdditions()) {
      const Status s = ApplyBlobFileAddition(blob_file_addition);
      if (!s.ok()) {
        return s;
      }
      version_updated = true;
    }

    // Increase the amount of garbage for blob files affected by GC
    for (const auto& blob_file_garbage : edit->GetBlobFileGarbages()) {
      const Status s = ApplyBlobFileGarbage(blob_file_garbage);
      if (!s.ok()) {
        return s;
      }
      version_updated = true;
    }

    for (const uint64_t blob_file_number : edit->GetDroppedBlobFiles()) {
      dropped_blob_files_.insert(blob_file_number);
    }

    // Delete table files
    for (const auto& deleted_file : edit->GetDeletedFiles()) {
      const int level = deleted_file.first;
      const uint64_t file_number = deleted_file.second;

      const Status s = ApplyFileDeletion(level, file_number);
      if (!s.ok()) {
        return s;
      }
      version_updated = true;
    }

    // Add new table files
    for (const auto& new_file : edit->GetNewFiles()) {
      const int level = new_file.first;
      const FileMetaData& meta = new_file.second;

      const Status s = ApplyFileAddition(level, meta);
      if (!s.ok()) {
        return s;
      }
      version_updated = true;
    }

    // Populate compact cursors for round-robin compaction, leave
    // the cursor to be empty to indicate it is invalid
    for (const auto& cursor : edit->GetCompactCursors()) {
      const int level = cursor.first;
      const InternalKey smallest_uncompacted_key = cursor.second;
      const Status s = ApplyCompactCursors(level, smallest_uncompacted_key);
      if (!s.ok()) {
        return s;
      }
    }

    if (track_found_and_missing_files_ && version_updated) {
      version_updated_since_last_check_ = true;
      if (!edited_in_atomic_group_ && edit->IsInAtomicGroup()) {
        edited_in_atomic_group_ = true;
      }
    }
    return Status::OK();
  }

  // Helper function template for merging the blob file metadata from the base
  // version with the mutable metadata representing the state after applying the
  // edits. The function objects process_base and process_mutable are
  // respectively called to handle a base version object when there is no
  // matching mutable object, and a mutable object when there is no matching
  // base version object. process_both is called to perform the merge when a
  // given blob file appears both in the base version and the mutable list. The
  // helper stops processing objects if a function object returns false. Blob
  // files with a file number below first_blob_file are not processed.
  template <typename ProcessBase, typename ProcessMutable, typename ProcessBoth>
  void MergeBlobFileMetas(uint64_t first_blob_file, ProcessBase process_base,
                          ProcessMutable process_mutable,
                          ProcessBoth process_both) const {
    assert(base_vstorage_);

    auto base_it = base_vstorage_->GetBlobFileMetaDataLB(first_blob_file);
    const auto base_it_end = base_vstorage_->GetBlobFiles().end();

    auto mutable_it = mutable_blob_file_metas_.lower_bound(first_blob_file);
    const auto mutable_it_end = mutable_blob_file_metas_.end();

    while (base_it != base_it_end && mutable_it != mutable_it_end) {
      const auto& base_meta = *base_it;
      assert(base_meta);

      const uint64_t base_blob_file_number = base_meta->GetBlobFileNumber();
      const uint64_t mutable_blob_file_number = mutable_it->first;

      if (base_blob_file_number < mutable_blob_file_number) {
        if (!process_base(base_meta)) {
          return;
        }

        ++base_it;
      } else if (mutable_blob_file_number < base_blob_file_number) {
        const auto& mutable_meta = mutable_it->second;

        if (!process_mutable(mutable_meta)) {
          return;
        }

        ++mutable_it;
      } else {
        assert(base_blob_file_number == mutable_blob_file_number);

        const auto& mutable_meta = mutable_it->second;

        if (!process_both(base_meta, mutable_meta)) {
          return;
        }

        ++base_it;
        ++mutable_it;
      }
    }

    while (base_it != base_it_end) {
      const auto& base_meta = *base_it;

      if (!process_base(base_meta)) {
        return;
      }

      ++base_it;
    }

    while (mutable_it != mutable_it_end) {
      const auto& mutable_meta = mutable_it->second;

      if (!process_mutable(mutable_meta)) {
        return;
      }

      ++mutable_it;
    }
  }

  // Helper function template for finding the first blob file that has linked
  // SSTs.
  template <typename Meta>
  static bool CheckLinkedSsts(const Meta& meta,
                              uint64_t* min_oldest_blob_file_num) {
    assert(min_oldest_blob_file_num);

    if (!meta.GetLinkedSsts().empty()) {
      assert(*min_oldest_blob_file_num == kInvalidBlobFileNumber);

      *min_oldest_blob_file_num = meta.GetBlobFileNumber();

      return false;
    }

    return true;
  }

  // Find the oldest blob file that has linked SSTs.
  uint64_t GetMinOldestBlobFileNumber() const {
    uint64_t min_oldest_blob_file_num = kInvalidBlobFileNumber;

    auto process_base =
        [&min_oldest_blob_file_num](
            const std::shared_ptr<BlobFileMetaData>& base_meta) {
          assert(base_meta);

          return CheckLinkedSsts(*base_meta, &min_oldest_blob_file_num);
        };

    auto process_mutable = [&min_oldest_blob_file_num](
                               const MutableBlobFileMetaData& mutable_meta) {
      return CheckLinkedSsts(mutable_meta, &min_oldest_blob_file_num);
    };

    auto process_both = [&min_oldest_blob_file_num](
                            const std::shared_ptr<BlobFileMetaData>& base_meta,
                            const MutableBlobFileMetaData& mutable_meta) {
#ifndef NDEBUG
      assert(base_meta);
      assert(base_meta->GetSharedMeta() == mutable_meta.GetSharedMeta());
#else
      (void)base_meta;
#endif

      // Look at mutable_meta since it supersedes *base_meta
      return CheckLinkedSsts(mutable_meta, &min_oldest_blob_file_num);
    };

    MergeBlobFileMetas(kInvalidBlobFileNumber, process_base, process_mutable,
                       process_both);

    return min_oldest_blob_file_num;
  }

  static std::shared_ptr<BlobFileMetaData> CreateBlobFileMetaData(
      const MutableBlobFileMetaData& mutable_meta) {
    return BlobFileMetaData::Create(
        mutable_meta.GetSharedMeta(), mutable_meta.GetLinkedSsts(),
        mutable_meta.GetGarbageBlobCount(), mutable_meta.GetGarbageBlobBytes());
  }

  bool OnlyLinkedToMissingL0Files(
      const std::unordered_set<uint64_t>& linked_ssts) const {
    return std::all_of(
        linked_ssts.begin(), linked_ssts.end(), [&](const uint64_t& element) {
          return l0_missing_files_.find(element) != l0_missing_files_.end();
        });
  }

  // Add the blob file specified by meta to *vstorage if it is determined to
  // contain valid data (blobs). With record_missing_blob_files_as_dropped_, a
  // missing blob file that SST files might reference is appended to
  // *dropped_blob_files instead.
  template <typename Meta>
  void AddBlobFileIfNeeded(VersionStorageInfo* vstorage, Meta&& meta,
                           uint64_t blob_file_number,
                           uint64_t oldest_linked_blob_file,
                           std::vector<uint64_t>* dropped_blob_files) const {
    assert(vstorage);
    assert(meta);
    assert(dropped_blob_files);

    const auto& linked_ssts = meta->GetLinkedSsts();
    // Legacy rules: a normal blob file below the oldest linked blob file
    // (`oldest_linked_blob_file`) has no reader left, and a normal blob file
    // that no SST links and whose values all count as garbage is dropped too.
    // Neither rule applies to an indirect origin. After it is relocated, an
    // SST can still use its file number as a generation number without linking
    // it through oldest_blob_file_number. An indirect origin leaves the Version
    // only through a blob file retirement record, so its life never depends on
    // its garbage count. This is a change from base RocksDB behavior.
    // SaveRuleKeepsBlobFile() holds these rules, so that best-efforts recovery
    // (MissesNeededBlobFile()) uses the same decision.
    if (!SaveRuleKeepsBlobFile(
            blob_file_number, meta->IsIndirectOrigin(), linked_ssts.empty(),
            meta->GetGarbageBlobCount() >= meta->GetTotalBlobCount(),
            oldest_linked_blob_file)) {
      return;
    }
    if (track_found_and_missing_files_) {
      if (missing_blob_files_.find(blob_file_number) !=
          missing_blob_files_.end()) {
        // A normal blob file without linked SST files or live blobs was deleted
        // as obsolete, so no SST file references it. When missing blob files
        // are recorded as dropped, a missing indirect origin is always
        // recorded, even when all its values count as garbage. This rule is
        // defensive. Until a blob file retirement record removes an indirect
        // origin, only unflattened references read values from it. So when all
        // its values count as garbage, no SST reads from it, unless the garbage
        // count is wrong. When the garbage count is right, no read sees it as
        // dropped.
        if (record_missing_blob_files_as_dropped_) {
          dropped_blob_files->push_back(blob_file_number);
        }
        return;
      }
      // Leave the empty case for the below blob garbage collection logic.
      if (!linked_ssts.empty() && OnlyLinkedToMissingL0Files(linked_ssts)) {
        // Retained SST files can still reference this blob file even though
        // none of them is linked to it.
        if (record_missing_blob_files_as_dropped_) {
          dropped_blob_files->push_back(blob_file_number);
        }
        return;
      }
    }

    vstorage->AddBlobFile(std::forward<Meta>(meta));
  }

  // Merge the blob file metadata from the base version with the changes (edits)
  // applied, and save the result into *vstorage. Missing blob files recorded
  // as dropped are appended to *dropped_blob_files (see AddBlobFileIfNeeded()).
  void SaveBlobFilesTo(VersionStorageInfo* vstorage,
                       uint64_t min_oldest_blob_file_num,
                       std::vector<uint64_t>* dropped_blob_files) const {
    assert(vstorage);
    assert(!track_found_and_missing_files_ || valid_version_available_);

    assert(base_vstorage_);
    vstorage->ReserveBlob(base_vstorage_->GetBlobFiles().size() +
                          mutable_blob_file_metas_.size());

    // The oldest linked blob file is the smallest oldest_blob_file_number of
    // the saved SSTs. It comes from the SSTs, not from the blob files that list
    // linked SSTs. In a fenced CF an SST can have, as its oldest blob file, an
    // indirect origin that blob file retirement removed. That SST links no blob
    // file in this Version, but it can still reference newer normal blob files.
    const uint64_t oldest_linked_blob_file = min_oldest_blob_file_num;

    // In a legacy CF (format version 0), an invalid oldest linked blob file
    // means that no SST references a blob file, so no blob file is saved. A
    // fenced CF cannot take this shortcut: its indirect origins leave only
    // through blob file retirement records, whatever the oldest linked blob
    // file is.
    if (blob_indirection_format_version_ == 0 &&
        oldest_linked_blob_file == kInvalidBlobFileNumber) {
      return;
    }

    auto process_base =
        [this, vstorage, oldest_linked_blob_file, dropped_blob_files](
            const std::shared_ptr<BlobFileMetaData>& base_meta) {
          assert(base_meta);

          AddBlobFileIfNeeded(vstorage, base_meta,
                              base_meta->GetBlobFileNumber(),
                              oldest_linked_blob_file, dropped_blob_files);

          return true;
        };

    auto process_mutable =
        [this, vstorage, oldest_linked_blob_file,
         dropped_blob_files](const MutableBlobFileMetaData& mutable_meta) {
          AddBlobFileIfNeeded(vstorage, CreateBlobFileMetaData(mutable_meta),
                              mutable_meta.GetBlobFileNumber(),
                              oldest_linked_blob_file, dropped_blob_files);

          return true;
        };

    auto process_both = [this, vstorage, oldest_linked_blob_file,
                         dropped_blob_files](
                            const std::shared_ptr<BlobFileMetaData>& base_meta,
                            const MutableBlobFileMetaData& mutable_meta) {
      assert(base_meta);
      assert(base_meta->GetSharedMeta() == mutable_meta.GetSharedMeta());

      if (!mutable_meta.HasDelta()) {
        assert(base_meta->GetGarbageBlobCount() ==
               mutable_meta.GetGarbageBlobCount());
        assert(base_meta->GetGarbageBlobBytes() ==
               mutable_meta.GetGarbageBlobBytes());
        assert(base_meta->GetLinkedSsts() == mutable_meta.GetLinkedSsts());

        AddBlobFileIfNeeded(vstorage, base_meta, base_meta->GetBlobFileNumber(),
                            oldest_linked_blob_file, dropped_blob_files);

        return true;
      }

      AddBlobFileIfNeeded(vstorage, CreateBlobFileMetaData(mutable_meta),
                          mutable_meta.GetBlobFileNumber(),
                          oldest_linked_blob_file, dropped_blob_files);

      return true;
    };

    MergeBlobFileMetas(blob_indirection_format_version_ == 0
                           ? oldest_linked_blob_file
                           : kInvalidBlobFileNumber,
                       process_base, process_mutable, process_both);
  }

  // Save into *vstorage the dropped blob files of the base Version and of the
  // edits, plus `dropped`, the missing blob files SaveBlobFilesTo() recorded.
  // `min_oldest_blob_file_num` is the oldest blob file referenced by the SST
  // files in *vstorage; a blob file older than it, or in *vstorage, is not
  // kept. This rule also fits a dropped indirect origin. Indirect blob GC has
  // not retired it, so it has no route, and only unflattened references can use
  // it. Such a reference always sets oldest_blob_file_number (see
  // UpdateFileMetaDataBoundaries()), so the origin stays in the list while an
  // SST can use it.
  void SaveDroppedBlobFilesTo(VersionStorageInfo* vstorage,
                              uint64_t min_oldest_blob_file_num,
                              std::vector<uint64_t>&& dropped) const {
    assert(vstorage);
    assert(base_vstorage_);

    if (min_oldest_blob_file_num == kInvalidBlobFileNumber) {
      return;
    }

    const std::vector<uint64_t>& base_dropped =
        base_vstorage_->GetDroppedBlobFiles();
    dropped.insert(dropped.end(), base_dropped.begin(), base_dropped.end());
    dropped.insert(dropped.end(), dropped_blob_files_.begin(),
                   dropped_blob_files_.end());
    if (dropped.empty()) {
      return;
    }

    dropped.erase(std::remove_if(dropped.begin(), dropped.end(),
                                 [&](uint64_t blob_file_number) {
                                   return blob_file_number <
                                              min_oldest_blob_file_num ||
                                          vstorage->GetBlobFileMetaData(
                                              blob_file_number) != nullptr;
                                 }),
                  dropped.end());
    std::sort(dropped.begin(), dropped.end());
    dropped.erase(std::unique(dropped.begin(), dropped.end()), dropped.end());
    vstorage->SetDroppedBlobFiles(std::move(dropped));
  }

  void MaybeAddFile(VersionStorageInfo* vstorage, int level, FileMetaData* f,
                    uint64_t* min_oldest_blob_file_num) const {
    assert(min_oldest_blob_file_num);
    const uint64_t file_number = f->fd.GetNumber();
    if (track_found_and_missing_files_ && level == 0 &&
        l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
      return;
    }

    const auto& level_state = levels_[level];

    const auto& del_files = level_state.deleted_files;
    const auto del_it = del_files.find(file_number);

    if (del_it != del_files.end()) {
      // f is to-be-deleted table file
      vstorage->RemoveCurrentStats(f);
    } else {
      const auto& add_files = level_state.added_files;
      const auto add_it = add_files.find(file_number);

      // Note: if the file appears both in the base version and in the added
      // list, the added FileMetaData supersedes the one in the base version.
      if (add_it != add_files.end() && add_it->second != f) {
        vstorage->RemoveCurrentStats(f);
      } else {
        vstorage->AddFile(level, f);
        const uint64_t blob_file_number = f->oldest_blob_file_number;
        if (blob_file_number != kInvalidBlobFileNumber &&
            (*min_oldest_blob_file_num == kInvalidBlobFileNumber ||
             blob_file_number < *min_oldest_blob_file_num)) {
          *min_oldest_blob_file_num = blob_file_number;
        }
      }
    }
  }

  // Whether the blob file links of this point (each SST's
  // oldest_blob_file_number) describe every blob file that an SST references.
  // That holds in a column family without the blob indirection format fence,
  // the base case. With the fence it does not hold:
  // - an indirect reference can name a blob file that is not the oldest blob
  //   file of its SST, and indirect blob GC deletes such a file without
  //   rewriting the SSTs that reference it, so the SSTs of an earlier point
  //   can still reference a deleted file;
  // - an SST's oldest blob file can be a retired origin that is no longer in
  //   the Version, so GetMinOldestBlobFileNumber() can be higher than the
  //   smallest file the SSTs reference.
  // So with the fence, best-efforts recovery does not use those links. It
  // treats a point as incomplete when a blob file that the save rule would
  // keep in that point's Version is missing (see MissesNeededBlobFile()).
  // Recovery then falls back to the newest point that misses no needed blob
  // file (the empty column family if the file is in the starting snapshot of
  // the MANIFEST). This can fall back further than base would: the base
  // relaxation for a missing blob file linked only to a missing L0 suffix
  // applies only in part (a normal blob file below the oldest blob file of the
  // saved SSTs is not needed, and missing L0 SSTs are left out of that
  // minimum), so in the worst case recovery reaches an empty DB, which is
  // consistent. To keep more data, open with best_efforts_blob_db_repair,
  // which records the missing files as dropped.
  bool BlobFileLinksDescribeAllReferences() const {
    return blob_indirection_format_version_ == 0;
  }

  // The keep decision of the save rule (SaveBlobFilesTo() and
  // AddBlobFileIfNeeded()) for a blob file that exists on disk. A blob file
  // that a blob file retirement record removed is not saved. An indirect origin
  // is saved otherwise: it leaves the Version only through its retirement. A
  // normal blob file is not saved when it is below `oldest_linked_blob_file`
  // (the smallest oldest_blob_file_number of the saved SSTs;
  // kInvalidBlobFileNumber means that no SST has one), or when no SST links it
  // and all its values count as garbage.
  bool SaveRuleKeepsBlobFile(uint64_t blob_file_number, bool is_indirect_origin,
                             bool no_linked_ssts, bool all_garbage,
                             uint64_t oldest_linked_blob_file) const {
    if (retired_blob_files_.count(blob_file_number) != 0) {
      return false;
    }
    if (is_indirect_origin) {
      return true;
    }
    if (oldest_linked_blob_file == kInvalidBlobFileNumber ||
        blob_file_number < oldest_linked_blob_file) {
      return false;
    }
    return !(no_linked_ssts && all_garbage);
  }

  // Calls `fn` for each SST that SaveSSTFilesTo() would save at this point:
  // the base SSTs that no edit deleted or replaced, and the added SSTs, but
  // not a missing L0 SST (see MaybeAddFile()).
  template <typename Fn>
  void ForEachSavedSst(Fn&& fn) const {
    for (int level = 0; level < num_levels_; ++level) {
      const LevelState& level_state = levels_[level];
      const auto saved = [&](const FileMetaData* f) {
        const uint64_t file_number = f->fd.GetNumber();
        return !(track_found_and_missing_files_ && level == 0 &&
                 l0_missing_files_.count(file_number) != 0) &&
               level_state.deleted_files.count(file_number) == 0;
      };
      for (const FileMetaData* f : base_vstorage_->LevelFiles(level)) {
        const auto add_it = level_state.added_files.find(f->fd.GetNumber());
        if (saved(f) &&
            (add_it == level_state.added_files.end() || add_it->second == f)) {
          fn(f);
        }
      }
      for (const auto& added : level_state.added_files) {
        if (saved(added.second)) {
          fn(added.second);
        }
      }
    }
  }

  // Whether ApplyFileAddition() or ApplyFileDeletion() counts this SST in
  // saved_oldest_counts_: SaveSSTFilesTo() does not save a missing L0 SST.
  bool CountsInSavedOldest(int level, uint64_t file_number) const {
    return !(track_found_and_missing_files_ && level == 0 &&
             l0_missing_files_.count(file_number) != 0);
  }

  // The smallest oldest_blob_file_number among the SSTs that SaveSSTFilesTo()
  // would save at this point, or kInvalidBlobFileNumber.
  uint64_t MinOldestBlobFileOfSavedSsts() const {
    if (!saved_oldest_counts_built_) {
      saved_oldest_counts_.clear();
      ForEachSavedSst([this](const FileMetaData* f) {
        if (f->oldest_blob_file_number != kInvalidBlobFileNumber) {
          ++saved_oldest_counts_[f->oldest_blob_file_number];
        }
      });
      saved_oldest_counts_built_ = true;
    }
    const uint64_t min_oldest = saved_oldest_counts_.empty()
                                    ? kInvalidBlobFileNumber
                                    : saved_oldest_counts_.begin()->first;
#ifndef NDEBUG
    uint64_t scanned = kInvalidBlobFileNumber;
    ForEachSavedSst([&scanned](const FileMetaData* f) {
      const uint64_t oldest = f->oldest_blob_file_number;
      if (oldest != kInvalidBlobFileNumber &&
          (scanned == kInvalidBlobFileNumber || oldest < scanned)) {
        scanned = oldest;
      }
    });
    assert(min_oldest == scanned);
#endif
    return min_oldest;
  }

  // Whether a missing blob file is one that the save rule would keep in this
  // point's Version (SaveRuleKeepsBlobFile()). Such a point is incomplete. A
  // file that a dropped-file record of a best_efforts_blob_db_repair open
  // names is accepted as missing. The cheap checks come first: a missing
  // indirect origin that is not retired is always needed, and a missing
  // normal blob file that no SST links and that is all garbage is never
  // needed. Only a remaining normal blob file needs the oldest linked blob
  // file, and MinOldestBlobFileOfSavedSsts() keeps it incrementally.
  bool MissesNeededBlobFile() const {
    uint64_t oldest_linked_blob_file = kInvalidBlobFileNumber;
    bool have_oldest_linked = false;
    for (const uint64_t blob_file_number : missing_blob_files_) {
      if (dropped_blob_files_.count(blob_file_number) != 0) {
        continue;
      }
      bool is_indirect_origin = false;
      bool no_linked_ssts = false;
      bool all_garbage = false;
      const auto mutable_it = mutable_blob_file_metas_.find(blob_file_number);
      if (mutable_it != mutable_blob_file_metas_.end()) {
        const MutableBlobFileMetaData& meta = mutable_it->second;
        is_indirect_origin = meta.GetSharedMeta()->IsIndirectOrigin();
        no_linked_ssts = meta.GetLinkedSsts().empty();
        all_garbage = meta.GetGarbageBlobCount() >=
                      meta.GetSharedMeta()->GetTotalBlobCount();
      } else {
        const std::shared_ptr<BlobFileMetaData> base_meta =
            base_vstorage_->GetBlobFileMetaData(blob_file_number);
        if (base_meta == nullptr) {
          // A missing file always has metadata, because its addition put it in
          // missing_blob_files_. Treat an unknown one as needed (fail safe).
          assert(false);
          return true;
        }
        is_indirect_origin = base_meta->IsIndirectOrigin();
        no_linked_ssts = base_meta->GetLinkedSsts().empty();
        all_garbage =
            base_meta->GetGarbageBlobCount() >= base_meta->GetTotalBlobCount();
      }
      if (!is_indirect_origin && no_linked_ssts && all_garbage) {
        continue;
      }
      if (!have_oldest_linked) {
        oldest_linked_blob_file = MinOldestBlobFileOfSavedSsts();
        have_oldest_linked = true;
      }
      if (SaveRuleKeepsBlobFile(blob_file_number, is_indirect_origin,
                                no_linked_ssts, all_garbage,
                                oldest_linked_blob_file)) {
        return true;
      }
    }
    return false;
  }

  bool ContainsCompleteVersion() const {
    assert(track_found_and_missing_files_);
    return l0_missing_files_.empty() && non_l0_missing_files_.empty() &&
           missing_blob_relocation_files_.empty() &&
           (record_missing_blob_files_as_dropped_ ||
            missing_blob_files_high_ == kInvalidBlobFileNumber ||
            (BlobFileLinksDescribeAllReferences()
                 ? missing_blob_files_high_ < GetMinOldestBlobFileNumber()
                 : !MissesNeededBlobFile()));
  }

  bool RecordsMissingBlobFilesAsDropped() const {
    return record_missing_blob_files_as_dropped_;
  }

  void RecordMissingBlobFilesAsDropped() {
    assert(track_found_and_missing_files_);
    assert(allow_incomplete_valid_version_);
    assert(mutable_blob_file_metas_.empty());
    assert(!undo_);
    record_missing_blob_files_as_dropped_ = true;
  }

  std::shared_ptr<void> ReleaseBlobRelocationChanges() {
    if (blob_relocation_file_changes_.empty() &&
        blob_relocation_route_changes_.empty()) {
      return nullptr;
    }
    struct Edits {
      decltype(blob_relocation_file_changes_) files;
      decltype(blob_relocation_route_changes_) routes;
      decltype(blob_relocation_target_count_deltas_) target_counts;
    };
    auto edits = std::make_shared<Edits>();
    edits->files.swap(blob_relocation_file_changes_);
    edits->routes.swap(blob_relocation_route_changes_);
    edits->target_counts.swap(blob_relocation_target_count_deltas_);
    return edits;
  }

  bool HasMissingFiles() const {
    assert(track_found_and_missing_files_);
    return !l0_missing_files_.empty() || !non_l0_missing_files_.empty() ||
           missing_blob_files_high_ != kInvalidBlobFileNumber ||
           !missing_blob_relocation_files_.empty();
  }

  bool HasMissingBlobRelocationFiles() const {
    assert(track_found_and_missing_files_);
    return !missing_blob_relocation_files_.empty();
  }

  std::vector<std::string> GetAndClearIntermediateFiles() {
    assert(track_found_and_missing_files_);
    std::vector<std::string> result;
    result.swap(intermediate_files_);
    return result;
  }

  void ClearFoundFiles() {
    assert(track_found_and_missing_files_);
    found_files_.clear();
  }

  template <typename Cmp>
  void SaveSSTFilesTo(VersionStorageInfo* vstorage, int level, Cmp cmp,
                      uint64_t* min_oldest_blob_file_num) const {
    // Merge the set of added files with the set of pre-existing files.
    // Drop any deleted files.  Store the result in *vstorage.
    const auto& base_files = base_vstorage_->LevelFiles(level);
    const auto& unordered_added_files = levels_[level].added_files;
    vstorage->Reserve(level, base_files.size() + unordered_added_files.size());

    MergeUnorderdAddedFilesWithBase(
        base_files, unordered_added_files, cmp, [&](FileMetaData* file) {
          MaybeAddFile(vstorage, level, file, min_oldest_blob_file_num);
        });
  }

  template <typename Cmp, typename AddFileFunc>
  void MergeUnorderdAddedFilesWithBase(
      const std::vector<FileMetaData*>& base_files,
      const std::unordered_map<uint64_t, FileMetaData*>& unordered_added_files,
      Cmp cmp, AddFileFunc add_file_func) const {
    // Sort added files for the level.
    std::vector<FileMetaData*> added_files;
    added_files.reserve(unordered_added_files.size());
    for (const auto& pair : unordered_added_files) {
      added_files.push_back(pair.second);
    }
    std::sort(added_files.begin(), added_files.end(), cmp);

    auto base_iter = base_files.begin();
    auto base_end = base_files.end();
    auto added_iter = added_files.begin();
    auto added_end = added_files.end();
    while (added_iter != added_end || base_iter != base_end) {
      if (base_iter == base_end ||
          (added_iter != added_end && cmp(*added_iter, *base_iter))) {
        add_file_func(*added_iter++);
      } else {
        add_file_func(*base_iter++);
      }
    }
  }

  bool PromoteEpochNumberRequirementIfNeeded(
      VersionStorageInfo* vstorage) const {
    if (vstorage->HasMissingEpochNumber()) {
      return false;
    }

    for (int level = 0; level < num_levels_; ++level) {
      for (const auto& pair : levels_[level].added_files) {
        const FileMetaData* f = pair.second;
        if (f->epoch_number == kUnknownEpochNumber) {
          return false;
        }
      }
    }

    vstorage->SetEpochNumberRequirement(EpochNumberRequirement::kMustPresent);
    return true;
  }

  // Returns the oldest blob file number referenced by the retained SSTs, or
  // kInvalidBlobFileNumber when they contain no blob references.
  uint64_t SaveSSTFilesTo(VersionStorageInfo* vstorage) const {
    assert(vstorage);
    uint64_t min_oldest_blob_file_num = kInvalidBlobFileNumber;

    if (!num_levels_) {
      return min_oldest_blob_file_num;
    }

    EpochNumberRequirement epoch_number_requirement =
        vstorage->GetEpochNumberRequirement();

    if (epoch_number_requirement == EpochNumberRequirement::kMightMissing) {
      bool promoted = PromoteEpochNumberRequirementIfNeeded(vstorage);
      if (promoted) {
        epoch_number_requirement = vstorage->GetEpochNumberRequirement();
      }
    }

    if (epoch_number_requirement == EpochNumberRequirement::kMightMissing) {
      SaveSSTFilesTo(vstorage, /* level */ 0, level_zero_cmp_by_seqno_,
                     &min_oldest_blob_file_num);
    } else {
      SaveSSTFilesTo(vstorage, /* level */ 0, level_zero_cmp_by_epochno_,
                     &min_oldest_blob_file_num);
    }

    for (int level = 1; level < num_levels_; ++level) {
      SaveSSTFilesTo(vstorage, level, level_nonzero_cmp_,
                     &min_oldest_blob_file_num);
    }

    return min_oldest_blob_file_num;
  }

  void SaveCompactCursorsTo(VersionStorageInfo* vstorage) const {
    for (auto iter = updated_compact_cursors_.begin();
         iter != updated_compact_cursors_.end(); iter++) {
      vstorage->AddCursorForOneLevel(iter->first, iter->second);
    }
  }

  bool ValidVersionAvailable() {
    assert(track_found_and_missing_files_);
    if (version_updated_since_last_check_) {
      valid_version_available_ = ContainsCompleteVersion();
      if (!valid_version_available_ && !edited_in_atomic_group_ &&
          allow_incomplete_valid_version_) {
        valid_version_available_ = OnlyMissingL0Suffix();
      }
      version_updated_since_last_check_ = false;
    }
    return valid_version_available_;
  }

  bool OnlyMissingL0Suffix() const {
    if (!non_l0_missing_files_.empty() ||
        !missing_blob_relocation_files_.empty()) {
      return false;
    }
    assert(!(l0_missing_files_.empty() && missing_blob_files_.empty()));

    if (!l0_missing_files_.empty() && !MissingL0FilesAreL0Suffix()) {
      return false;
    }
    if (!record_missing_blob_files_as_dropped_ &&
        !missing_blob_files_.empty() &&
        (BlobFileLinksDescribeAllReferences()
             ? !RemainingSstFilesNotMissingBlobFiles()
             : MissesNeededBlobFile())) {
      return false;
    }
    return true;
  }

  // Check missing L0 files are a suffix of expected sorted L0 files.
  bool MissingL0FilesAreL0Suffix() const {
    assert(non_l0_missing_files_.empty());
    assert(!l0_missing_files_.empty());
    std::vector<FileMetaData*> expected_sorted_l0_files;
    const auto& base_files = base_vstorage_->LevelFiles(0);
    const auto& unordered_added_files = levels_[0].added_files;
    expected_sorted_l0_files.reserve(base_files.size() +
                                     unordered_added_files.size());
    EpochNumberRequirement epoch_number_requirement =
        base_vstorage_->GetEpochNumberRequirement();

    if (epoch_number_requirement == EpochNumberRequirement::kMightMissing) {
      MergeUnorderdAddedFilesWithBase(
          base_files, unordered_added_files, level_zero_cmp_by_seqno_,
          [&](FileMetaData* file) {
            expected_sorted_l0_files.push_back(file);
          });
    } else {
      MergeUnorderdAddedFilesWithBase(
          base_files, unordered_added_files, level_zero_cmp_by_epochno_,
          [&](FileMetaData* file) {
            expected_sorted_l0_files.push_back(file);
          });
    }
    assert(expected_sorted_l0_files.size() >= l0_missing_files_.size());
    std::unordered_set<uint64_t> unaddressed_missing_files = l0_missing_files_;
    for (auto iter = expected_sorted_l0_files.begin();
         iter != expected_sorted_l0_files.end(); iter++) {
      uint64_t file_number = (*iter)->fd.GetNumber();
      if (l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
        assert(unaddressed_missing_files.find(file_number) !=
               unaddressed_missing_files.end());
        unaddressed_missing_files.erase(file_number);
      } else if (!unaddressed_missing_files.empty()) {
        return false;
      } else {
        break;
      }
    }
    return true;
  }

  // Check for each of the missing blob file missing, it either is older than
  // the minimum oldest blob file required by this Version or only linked to
  // the missing L0 files.
  bool RemainingSstFilesNotMissingBlobFiles() const {
    assert(non_l0_missing_files_.empty());
    assert(!missing_blob_files_.empty());
    bool no_l0_files_missing = l0_missing_files_.empty();
    uint64_t min_oldest_blob_file_num = GetMinOldestBlobFileNumber();
    for (const auto& missing_blob_file : missing_blob_files_) {
      if (missing_blob_file < min_oldest_blob_file_num) {
        continue;
      }
      auto iter = mutable_blob_file_metas_.find(missing_blob_file);
      assert(iter != mutable_blob_file_metas_.end());
      const std::unordered_set<uint64_t>& linked_ssts =
          iter->second.GetLinkedSsts();
      // TODO(yuzhangyu): In theory, if no L0 SST files ara missing, and only
      // blob files exclusively linked to a L0 suffix are missing, we can
      // recover to a valid point in time too. We don't recover that type of
      // incomplete Version yet.
      if (!linked_ssts.empty() && no_l0_files_missing) {
        return false;
      }
      if (!OnlyLinkedToMissingL0Files(linked_ssts)) {
        return false;
      }
    }
    return true;
  }

  Status SaveTableFilesTo(VersionStorageInfo* vstorage) const {
    assert(!track_found_and_missing_files_ || valid_version_available_);
    Status s;

#ifndef NDEBUG
    // The same check is done within Apply() so we skip it in release mode.
    s = CheckConsistency(base_vstorage_);
    if (!s.ok()) {
      return s;
    }
#endif  // NDEBUG

    s = CheckConsistency(vstorage);
    if (!s.ok()) {
      return s;
    }

    saved_min_oldest_blob_file_num_ = SaveSSTFilesTo(vstorage);
#ifndef NDEBUG
    table_files_saved_ = true;
#endif  // NDEBUG
    SaveCompactCursorsTo(vstorage);

    return Status::OK();
  }

  Status SaveBlobFilesAndCheckTo(VersionStorageInfo* vstorage) {
    assert(!track_found_and_missing_files_ || valid_version_available_);
    if (vstorage == nullptr) {
      return Status::InvalidArgument("VersionStorageInfo is null");
    }

#ifndef NDEBUG
    assert(table_files_saved_);
    table_files_saved_ = false;
#endif  // NDEBUG
    std::vector<uint64_t> dropped_blob_files;
    SaveBlobFilesTo(vstorage, saved_min_oldest_blob_file_num_,
                    &dropped_blob_files);
    SaveDroppedBlobFilesTo(vstorage, saved_min_oldest_blob_file_num_,
                           std::move(dropped_blob_files));
    if (blob_indirection_format_version_ != 0) {
      // SaveBlobFilesTo() can drop a normal blob file just because the oldest
      // linked blob file moved, even when no edit names the file. So compare
      // the base and the new blob file lists here, outside the DB mutex, and
      // record every base file that is gone. The census delta removes the same
      // files from its candidate heap, so the heap and the Version agree on the
      // live blob files.
      const VersionStorageInfo::BlobFiles& next_files =
          vstorage->GetBlobFiles();
      size_t next = 0;
      for (const std::shared_ptr<BlobFileMetaData>& file :
           base_vstorage_->GetBlobFiles()) {
        const uint64_t file_number = file->GetBlobFileNumber();
        while (next < next_files.size() &&
               next_files[next]->GetBlobFileNumber() < file_number) {
          ++next;
        }
        if (next == next_files.size() ||
            next_files[next]->GetBlobFileNumber() != file_number) {
          vstorage->RecordRemovedBlobFile(file_number);
        }
      }
    }
    vstorage->SetBlobIndirectionFormatVersion(blob_indirection_format_version_);

    Status base_state_status = base_vstorage_->ValidateBlobRelocationState();
    if (!base_state_status.ok()) {
      return base_state_status;
    }
    // Start from the base Version's relocation state, which is immutable and
    // shared. A Version keeps sharing all of it only when it changes no
    // relocation file, no route and (with the fence set) no blob file. The
    // first change copies the state, and the file table copies only the shards
    // it changes (BlobFileTable is copy-on-write). At runtime (LogAndApply,
    // ManifestTailer) this runs without the DB mutex.
    vstorage->ShareBlobRelocationState(*base_vstorage_);
    if (blob_indirection_format_version_ != 0) {
      // The file table also lists the live blob files. The first fenced Version
      // adds all of them. Otherwise, merge the sorted blob file vectors of the
      // base and new Version, and update only the files that were added,
      // removed or given new metadata.
      const bool first_fenced = !base_vstorage_->HasBlobIndirectionFormat();
      std::vector<uint64_t> changed_blob_files;
      if (!first_fenced) {
        const VersionStorageInfo::BlobFiles& before =
            base_vstorage_->GetBlobFiles();
        const VersionStorageInfo::BlobFiles& after = vstorage->GetBlobFiles();
        size_t b = 0;
        size_t a = 0;
        while (b < before.size() || a < after.size()) {
          if (a == after.size() ||
              (b < before.size() && before[b]->GetBlobFileNumber() <
                                        after[a]->GetBlobFileNumber())) {
            changed_blob_files.push_back(before[b++]->GetBlobFileNumber());
          } else if (b == before.size() || after[a]->GetBlobFileNumber() <
                                               before[b]->GetBlobFileNumber()) {
            changed_blob_files.push_back(after[a++]->GetBlobFileNumber());
          } else {
            if (before[b] != after[a]) {
              changed_blob_files.push_back(after[a]->GetBlobFileNumber());
            }
            ++a;
            ++b;
          }
        }
      }
      vstorage->SyncBlobFileTable(changed_blob_files, first_fenced,
                                  file_metadata_cache_res_mgr_);
    }
    // Apply the changes in this order:
    // 1. Add relocation files, so a new route's target is in the table.
    // 2. Delete routes while their targets are still in the table, so the route
    //    count of each target is updated.
    // 3. Delete relocation files. DeleteBlobRelocationFile() finds the file
    //    through its table entry, so it must run before step 4 replaces that
    //    entry with a route of the same number.
    // 4. Set routes. A deleted relocation file's number can become a route's
    //    old file number in the same edit (indirect blob GC routes a
    //    collected relocation file to its output).
    using FileEdits = decltype(blob_relocation_file_changes_);
    using RouteEdits = decltype(blob_relocation_route_changes_);
    for (const FileEdits::value_type& file_edit :
         blob_relocation_file_changes_) {
      if (file_edit.second != nullptr) {
        vstorage->AddBlobRelocationFile(file_edit.second);
      }
    }
    for (const RouteEdits::value_type& route_edit :
         blob_relocation_route_changes_) {
      if (!route_edit.second.has_value()) {
        vstorage->DeleteBlobRelocationRoute(route_edit.first);
      }
    }
    for (const FileEdits::value_type& file_edit :
         blob_relocation_file_changes_) {
      if (file_edit.second == nullptr) {
        vstorage->DeleteBlobRelocationFile(file_edit.first);
      }
    }
    for (const RouteEdits::value_type& route_edit :
         blob_relocation_route_changes_) {
      if (route_edit.second.has_value()) {
        vstorage->AddBlobRelocationRoute(*route_edit.second);
      }
    }
    // Apply the buffered file table changes above in one step.
    vstorage->SealBlobRelocationState();

    // Apply() already checked, in all builds, every changed file and route
    // against the final state. The base state was validated above, so the
    // unchanged entries are valid too. Debug builds still run the full check as
    // a test. Release builds mark the new state as validated, so
    // ValidateBlobRelocationState() checks only the format fence and the
    // byte-total overflow flag, and does not scan every entry of each new
    // Version (for example, on each flush).
    Status status;
#ifndef NDEBUG
    status = vstorage->ValidateBlobRelocationState();
#else
    vstorage->MarkBlobRelocationChangesValidated();
    status = vstorage->ValidateBlobRelocationState();
#endif
    if (!status.ok()) {
      return status;
    }
    if (!blob_relocation_file_changes_.empty() ||
        !blob_relocation_route_changes_.empty() ||
        blob_indirection_format_version_ !=
            base_vstorage_->GetBlobIndirectionFormatVersion()) {
      vstorage->ComputeBlobRelocationStateFingerprint();
    }
    return CheckConsistency(vstorage);
  }

  // Save the current state in *vstorage.
  Status SaveTo(VersionStorageInfo* vstorage) {
    Status s = SaveTableFilesTo(vstorage);
    if (!s.ok()) {
      return s;
    }

    return SaveBlobFilesAndCheckTo(vstorage);
  }

  Status LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                           bool prefetch_index_and_filter_in_cache,
                           bool is_initial_load,
                           const MutableCFOptions& mutable_cf_options,
                           size_t max_file_size_for_l0_meta_pin,
                           const ReadOptions& read_options) {
    assert(table_cache_ != nullptr);
    assert(!track_found_and_missing_files_ || valid_version_available_);

    size_t table_cache_capacity =
        table_cache_->get_cache().get()->GetCapacity();
    bool always_load = (table_cache_capacity == TableCache::kInfiniteCapacity);
    size_t max_load = std::numeric_limits<size_t>::max();

    if (!always_load) {
      // If it is initial loading and not set to always loading all the
      // files, we only load up to kInitialLoadLimit files, to limit the
      // time reopening the DB.
      const size_t kInitialLoadLimit = 16;
      size_t load_limit;
      // If the table cache is not 1/4 full, we pin the table handle to
      // file metadata to avoid the cache read costs when reading the file.
      // The downside of pinning those files is that LRU won't be followed
      // for those files. This doesn't matter much because if number of files
      // of the DB excceeds table cache capacity, eventually no table reader
      // will be pinned and LRU will be followed.
      if (is_initial_load) {
        load_limit = std::min(kInitialLoadLimit, table_cache_capacity / 4);
      } else {
        load_limit = table_cache_capacity / 4;
      }

      size_t table_cache_usage = table_cache_->get_cache().get()->GetUsage();
      if (table_cache_usage >= load_limit) {
        // TODO (yanqin) find a suitable status code.
        return Status::OK();
      } else {
        max_load = load_limit - table_cache_usage;
      }
    }

    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level].added_files) {
        auto* file_meta = file_meta_pair.second;
        uint64_t file_number = file_meta->fd.GetNumber();
        if (track_found_and_missing_files_ && level == 0 &&
            l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
          continue;
        }
        // If the file has been opened before, just skip it.
        if (file_meta->fd.pinned_reader.Get() == nullptr) {
          files_meta.emplace_back(file_meta, level);
        }
        if (files_meta.size() >= max_load) {
          break;
        }
      }
      if (files_meta.size() >= max_load) {
        break;
      }
    }

    return LoadTableHandlersHelper(
        files_meta, table_cache_, file_options_,
        *base_vstorage_->InternalComparator(), internal_stats, max_threads,
        prefetch_index_and_filter_in_cache, mutable_cf_options,
        max_file_size_for_l0_meta_pin, read_options);
  }
};

VersionBuilder::VersionBuilder(
    const FileOptions& file_options, const ImmutableCFOptions* ioptions,
    TableCache* table_cache, VersionStorageInfo* base_vstorage,
    VersionSet* version_set,
    std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr,
    ColumnFamilyData* cfd, VersionEditHandler* version_edit_handler,
    bool track_found_and_missing_files, bool allow_incomplete_valid_version)
    : rep_(new Rep(file_options, ioptions, table_cache, base_vstorage,
                   version_set, file_metadata_cache_res_mgr, cfd,
                   version_edit_handler, track_found_and_missing_files,
                   allow_incomplete_valid_version)) {}

VersionBuilder::~VersionBuilder() = default;

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

Status VersionBuilder::Apply(const VersionEdit* edit) {
  return rep_->Apply(edit);
}

Status VersionBuilder::SaveTo(VersionStorageInfo* vstorage) {
  return rep_->SaveTo(vstorage);
}

Status VersionBuilder::SaveTableFilesTo(VersionStorageInfo* vstorage) const {
  return rep_->SaveTableFilesTo(vstorage);
}

Status VersionBuilder::SaveBlobFilesAndCheckTo(VersionStorageInfo* vstorage) {
  return rep_->SaveBlobFilesAndCheckTo(vstorage);
}

Status VersionBuilder::LoadTableHandlers(
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache, bool is_initial_load,
    const MutableCFOptions& mutable_cf_options,
    size_t max_file_size_for_l0_meta_pin, const ReadOptions& read_options) {
  return rep_->LoadTableHandlers(internal_stats, max_threads,
                                 prefetch_index_and_filter_in_cache,
                                 is_initial_load, mutable_cf_options,
                                 max_file_size_for_l0_meta_pin, read_options);
}

void VersionBuilder::RollbackLastApply() { rep_->RollbackLastApply(); }

void VersionBuilder::RedoLastApply() { rep_->RedoLastApply(); }

void VersionBuilder::CommitLastApply() { rep_->CommitLastApply(); }

bool VersionBuilder::ValidVersionAvailable() {
  return rep_->ValidVersionAvailable();
}

bool VersionBuilder::HasMissingFiles() const { return rep_->HasMissingFiles(); }

void VersionBuilder::RecordMissingBlobFilesAsDropped() {
  rep_->RecordMissingBlobFilesAsDropped();
}

bool VersionBuilder::RecordsMissingBlobFilesAsDropped() const {
  return rep_->RecordsMissingBlobFilesAsDropped();
}

std::shared_ptr<void> VersionBuilder::ReleaseBlobRelocationChanges() {
  return rep_->ReleaseBlobRelocationChanges();
}

bool VersionBuilder::HasMissingBlobRelocationFiles() const {
  return rep_->HasMissingBlobRelocationFiles();
}

bool VersionBuilder::IsBlobFileRemovedByGc(uint64_t file_number) const {
  return rep_->IsBlobFileRetiredInBuilder(file_number) ||
         rep_->IsBlobRelocationFileDeletedInBuilder(file_number);
}

std::vector<std::string> VersionBuilder::GetAndClearIntermediateFiles() {
  return rep_->GetAndClearIntermediateFiles();
}

void VersionBuilder::ClearFoundFiles() { return rep_->ClearFoundFiles(); }

BaseReferencedVersionBuilder::BaseReferencedVersionBuilder(
    ColumnFamilyData* cfd, Version* base_version,
    VersionEditHandler* version_edit_handler,
    bool track_found_and_missing_files, bool allow_incomplete_valid_version)
    : version_builder_(new VersionBuilder(
          base_version->version_set()->file_options(), &cfd->ioptions(),
          cfd->table_cache(), base_version->storage_info(),
          base_version->version_set(),
          cfd->GetFileMetadataCacheReservationManager(), cfd,
          version_edit_handler, track_found_and_missing_files,
          allow_incomplete_valid_version)),
      version_(base_version) {
  version_->Ref();
}

BaseReferencedVersionBuilder::~BaseReferencedVersionBuilder() {
  version_->Unref();
}

}  // namespace ROCKSDB_NAMESPACE
