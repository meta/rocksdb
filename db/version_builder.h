//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once

#include <memory>

#include "db/version_edit.h"
#include "rocksdb/file_system.h"
#include "rocksdb/metadata.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {

struct ImmutableCFOptions;
class TableCache;
class VersionStorageInfo;
class VersionEdit;
struct FileMetaData;
class InternalStats;
class Version;
class VersionSet;
class VersionEditHandler;
class ColumnFamilyData;
class CacheReservationManager;

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionBuilder {
 public:
  VersionBuilder(const FileOptions& file_options,
                 const ImmutableCFOptions* ioptions, TableCache* table_cache,
                 VersionStorageInfo* base_vstorage, VersionSet* version_set,
                 std::shared_ptr<CacheReservationManager>
                     file_metadata_cache_res_mgr = nullptr,
                 ColumnFamilyData* cfd = nullptr,
                 VersionEditHandler* version_edit_handler = nullptr,
                 bool track_found_and_missing_files = false,
                 bool allow_incomplete_valid_version = false);
  ~VersionBuilder();

  bool CheckConsistencyForNumLevels();

  Status Apply(const VersionEdit* edit);

  // Save the current Version to the provided `vstorage`.
  Status SaveTo(VersionStorageInfo* vstorage) const;

  // Load table handlers for newly added files in the builder. This does not
  // load any files in the base storage.
  Status LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                           bool prefetch_index_and_filter_in_cache,
                           bool is_initial_load,
                           const MutableCFOptions& mutable_cf_options,
                           size_t max_file_size_for_l0_meta_pin,
                           const ReadOptions& read_options);

  //============APIs only used by VersionEditHandlerPointInTime ============//

  // The builder can find all the files to build a `Version`. Or if
  // `allow_incomplete_valid_version_` is true and the version history is never
  // edited in an atomic group, and only a suffix of L0 SST files and their
  // associated blob files are missing.
  // From the users' perspective, missing a suffix of L0 files means missing the
  // user's most recently written data. So the remaining available files still
  // presents a valid point in time view, although for some previous time.
  // This validity check result will be cached and reused if the Version is not
  // updated between two validity checks.
  bool ValidVersionAvailable();

  bool HasMissingFiles() const;

  // When applying a sequence of VersionEdits, intermediate files are the ones
  // that are added and then deleted. Returns the tracked files and clears them
  // so that subsequent VersionEdits start with a clean state.
  std::vector<std::string> GetAndClearIntermediateFiles();

  // Clearing all the found files in this Version.
  void ClearFoundFiles();

  // Single-edit undo for building a Version reflecting the state *before* the
  // most recent `Apply()`. On a valid->invalid negative edge,
  // `RollbackLastApply()` reverts the builder to the pre-edit state (so
  // `SaveTo`/`LoadTableHandlers` build the pre-edit Version), then
  // `RedoLastApply()` restores the post-edit state to continue replay.
  // `CommitLastApply()` finalizes the most recent `Apply()` (freeing files
  // whose deletion was deferred) and must be called once per `Apply()`; it is a
  // no-op if there is nothing pending. This replaces the previous per-edit
  // whole-Rep save-point copy, which was quadratic over a MANIFEST.
  void RollbackLastApply();
  void RedoLastApply();
  void CommitLastApply();

  //======= End of APIs only used by VersionEditPointInTime==========//

 private:
  class Rep;
  std::unique_ptr<Rep> rep_;
};

// A VersionBuilder wrapper that keeps the supplied base Version referenced.
// Both construction and destruction require the DB mutex.
class BaseReferencedVersionBuilder {
 public:
  BaseReferencedVersionBuilder(
      ColumnFamilyData* cfd, Version* base_version,
      VersionEditHandler* version_edit_handler = nullptr,
      bool track_found_and_missing_files = false,
      bool allow_incomplete_valid_version = false);
  ~BaseReferencedVersionBuilder();
  VersionBuilder* version_builder() const { return version_builder_.get(); }

 private:
  std::unique_ptr<VersionBuilder> version_builder_;
  Version* version_;
};
}  // namespace ROCKSDB_NAMESPACE
