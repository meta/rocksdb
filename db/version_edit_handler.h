//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <utility>

#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

struct FileMetaData;

class VersionEditHandlerBase {
 public:
  explicit VersionEditHandlerBase(const ReadOptions& read_options)
      : read_options_(read_options),
        max_manifest_read_size_(std::numeric_limits<uint64_t>::max()) {}

  virtual ~VersionEditHandlerBase() = default;

  void Iterate(log::Reader& reader, Status* log_read_status);

  const Status& status() const { return status_; }

  AtomicGroupReadBuffer& GetReadBuffer() { return read_buffer_; }

  uint64_t GetLastValidRecordEnd() const { return last_valid_record_end_; }

 protected:
  explicit VersionEditHandlerBase(const ReadOptions& read_options,
                                  uint64_t max_read_size)
      : read_options_(read_options), max_manifest_read_size_(max_read_size) {}
  virtual Status Initialize() { return Status::OK(); }

  virtual Status ApplyVersionEdit(VersionEdit& edit,
                                  ColumnFamilyData** cfd) = 0;

  virtual Status OnAtomicGroupReplayBegin() { return Status::OK(); }
  virtual Status OnAtomicGroupReplayEnd() { return Status::OK(); }

  virtual void CheckIterationResult(const log::Reader& /*reader*/,
                                    Status* /*s*/) {}

  void ResetReadState() {
    read_buffer_.Clear();
    last_valid_record_end_ = 0;
  }

  Status status_;

  const ReadOptions& read_options_;

  // File offset at the end of the last fully applied logical unit.
  // For non-atomic edits, this advances after each edit is decoded.
  // For atomic groups, this only advances after the entire group is
  // decoded, so an incomplete atomic group at the tail is excluded.
  uint64_t last_valid_record_end_ = 0;

 private:
  AtomicGroupReadBuffer read_buffer_;
  const uint64_t max_manifest_read_size_;
};

class ListColumnFamiliesHandler : public VersionEditHandlerBase {
 public:
  explicit ListColumnFamiliesHandler(const ReadOptions& read_options)
      : VersionEditHandlerBase(read_options) {}

  ~ListColumnFamiliesHandler() override = default;

  const std::map<uint32_t, std::string> GetColumnFamilyNames() const {
    return column_family_names_;
  }

 protected:
  Status ApplyVersionEdit(VersionEdit& edit,
                          ColumnFamilyData** /*unused*/) override;

 private:
  // default column family is always implicitly there
  std::map<uint32_t, std::string> column_family_names_{
      {0, kDefaultColumnFamilyName}};
};

class FileChecksumRetriever : public VersionEditHandlerBase {
 public:
  FileChecksumRetriever(const ReadOptions& read_options, uint64_t max_read_size)
      : VersionEditHandlerBase(read_options, max_read_size) {}

  ~FileChecksumRetriever() override = default;

  Status FetchFileChecksumList(FileChecksumList& file_checksum_list);

 protected:
  Status ApplyVersionEdit(VersionEdit& edit,
                          ColumnFamilyData** /*unused*/) override;

 private:
  // Map from CF to file # to string pair, where first portion of the value
  // is checksum, and second portion of the value is checksum function name.
  //
  // [column family id A]
  //      |
  //      |-- [file #1] -> [checksum #1, checksum function name #1]
  //      |-- [file #2] -> [checksum #2, checksum function name #2]
  //      |
  //     ...
  //      |
  //      |-- [file #N] -> [checksum #N, checksum function name #N]
  // [column family id B]
  //      |
  //      |-- [file #1] -> [checksum #1, checksum function name #1]
  //      |
  //     ...
  //      |
  //      |-- [file #M] -> [checksum #M, checksum function name #M]
  //      |
  //     ...
  std::unordered_map<
      uint32_t,
      std::unordered_map<uint64_t, std::pair<std::string, std::string>>>
      cf_file_checksums_;
};

using VersionBuilderUPtr = std::unique_ptr<BaseReferencedVersionBuilder>;

// A class used for scanning MANIFEST file.
// VersionEditHandler reads a MANIFEST file, parses the version edits, and
// builds the version set's in-memory state, e.g. the version storage info for
// the versions of column families. It replays all the version edits in one
// MANIFEST file to build the end version.
//
// To use this class and its subclasses,
// 1. Create an object of VersionEditHandler or its subclasses.
//    VersionEditHandler handler(read_only, column_families, version_set,
//                               track_found_and_missing_files,
//                               no_error_if_files_missing);
// 2. Status s = handler.Iterate(reader, &db_id);
// 3. Check s and handle possible errors.
//
// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandler is shared by multiple threads.
class VersionEditHandler : public VersionEditHandlerBase {
 public:
  explicit VersionEditHandler(
      bool read_only,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      VersionSet* version_set, bool track_found_and_missing_files,
      bool no_error_if_files_missing,
      const std::shared_ptr<IOTracer>& io_tracer,
      const ReadOptions& read_options, bool allow_incomplete_valid_version,
      EpochNumberRequirement epoch_number_requirement =
          EpochNumberRequirement::kMustPresent,
      bool skip_load_table_files = false)
      : VersionEditHandler(
            read_only, column_families, version_set,
            track_found_and_missing_files, no_error_if_files_missing, io_tracer,
            read_options, skip_load_table_files, allow_incomplete_valid_version,
            epoch_number_requirement) {}

  ~VersionEditHandler() override = default;

  const VersionEditParams& GetVersionEditParams() const {
    return version_edit_params_;
  }

  void GetDbId(std::string* db_id) const {
    if (db_id && version_edit_params_.HasDbId()) {
      *db_id = version_edit_params_.GetDbId();
    }
  }

  virtual Status VerifyFile(ColumnFamilyData* /*cfd*/,
                            const std::string& /*fpath*/, int /*level*/,
                            const FileMetaData& /*fmeta*/) {
    return Status::OK();
  }

  virtual Status VerifyBlobFile(ColumnFamilyData* /*cfd*/,
                                uint64_t /*blob_file_num*/,
                                const BlobFileAddition& /*blob_addition*/) {
    return Status::OK();
  }

 protected:
  explicit VersionEditHandler(
      bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
      VersionSet* version_set, bool track_found_and_missing_files,
      bool no_error_if_files_missing,
      const std::shared_ptr<IOTracer>& io_tracer,
      const ReadOptions& read_options, bool skip_load_table_files,
      bool allow_incomplete_valid_version,
      EpochNumberRequirement epoch_number_requirement =
          EpochNumberRequirement::kMustPresent);

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override;

  virtual Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnColumnFamilyDrop(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnNonCfOperation(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnWalAddition(VersionEdit& edit);

  Status OnWalDeletion(VersionEdit& edit);

  Status Initialize() override;

  void CheckColumnFamilyId(const VersionEdit& edit, bool* do_not_open_cf,
                           bool* cf_in_builders) const;

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  ColumnFamilyData* CreateCfAndInit(const ColumnFamilyOptions& cf_options,
                                    const VersionEdit& edit);

  virtual ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit);

  virtual Status MaybeCreateVersionBeforeApplyEdit(const VersionEdit& edit,
                                                   ColumnFamilyData* cfd,
                                                   bool force_create_version);

  virtual Status LoadTables(ColumnFamilyData* cfd,
                            bool prefetch_index_and_filter_in_cache,
                            bool is_initial_load);

  virtual bool MustOpenAllColumnFamilies() const {
    return !version_set_->unchanging();
  }

  const bool read_only_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  VersionSet* version_set_;
  std::unordered_map<uint32_t, VersionBuilderUPtr> builders_;
  std::unordered_map<std::string, ColumnFamilyOptions> name_to_options_;
  const bool track_found_and_missing_files_;
  // Keeps track of column families in manifest that were not found in
  // column families parameters. Namely, the user asks to not open these column
  // families. In non read only mode, if those column families are not dropped
  // by subsequent manifest records, Recover() will return failure status.
  std::unordered_map<uint32_t, std::string> do_not_open_column_families_;
  VersionEditParams version_edit_params_;
  bool no_error_if_files_missing_;
  std::shared_ptr<IOTracer> io_tracer_;
  bool skip_load_table_files_;
  bool initialized_;
  std::unique_ptr<std::unordered_map<uint32_t, std::string>> cf_to_cmp_names_;
  // If false, only a complete Version for which all files consisting it can be
  // found is considered a valid Version. If true, besides complete Version, an
  // incomplete Version with only a suffix of L0 files missing is also
  // considered valid if the Version is never edited in an atomic group.
  const bool allow_incomplete_valid_version_;
  // If true, every column family's VersionBuilder records missing blob files
  // as dropped instead of treating them as missing (see
  // VersionBuilder::RecordMissingBlobFilesAsDropped()).
  bool record_missing_blob_files_as_dropped_ = false;
  EpochNumberRequirement epoch_number_requirement_;
  std::unordered_set<uint32_t> cfds_to_mark_no_udt_;

 private:
  Status ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                    const VersionEdit& edit);

  // When `FileMetaData.user_defined_timestamps_persisted` is false and
  // user-defined timestamp size is non-zero. User-defined timestamps are
  // stripped from file boundaries: `smallest`, `largest` in
  // `VersionEdit.DecodeFrom` before they were written to Manifest.
  // This is the mirroring change to handle file boundaries on the Manifest read
  // path for this scenario: to pad a minimum timestamp to the user key in
  // `smallest` and `largest` so their format are consistent with the running
  // user comparator.
  Status MaybeHandleFileBoundariesForNewFiles(VersionEdit& edit,
                                              const ColumnFamilyData* cfd);
};

// A class similar to its base class, i.e. VersionEditHandler.
// Unlike VersionEditHandler that only aims to build the end version, this class
// supports building the most recent point in time version. A point in time
// version is a version for which no files are missing, or if
// `allow_incomplete_valid_version` is true, only a suffix of L0 files (and
// their associated blob files) are missing.
//
// Building a point in time version when end version is not available can
// be useful for best efforts recovery (options.best_efforts_recovery), which
// uses this class and sets `allow_incomplete_valid_version` to true.
// It's also useful for secondary instances/follower instances for which end
// version could be transiently unavailable. These two cases use subclass
// `ManifestTailer` and sets `allow_incomplete_valid_version` to false.
//
// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandlerPointInTime is shared by multiple threads.
class VersionEditHandlerPointInTime : public VersionEditHandler {
 public:
  // `record_missing_blob_files_as_dropped` requires
  // `allow_incomplete_valid_version`.
  VersionEditHandlerPointInTime(
      bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
      VersionSet* version_set, const std::shared_ptr<IOTracer>& io_tracer,
      const ReadOptions& read_options, bool allow_incomplete_valid_version,
      bool record_missing_blob_files_as_dropped, bool trust_manifest_recovery,
      bool defer_sst_file_opening,
      EpochNumberRequirement epoch_number_requirement);
  ~VersionEditHandlerPointInTime() override;

  bool HasMissingFiles() const;

  // Returns the column family's log number as of the Version most recently
  // installed for it by this handler, i.e. the log number that the MANIFEST
  // records reflected by that Version had put in effect. Data in WALs older
  // than the returned number is readable from the files that Version
  // references, without those WALs.
  //
  // Returns 0 when no Version has been installed for `cf_id`.
  //
  // REQUIRES: db mutex
  uint64_t GetInstalledVersionLogNumber(uint32_t cf_id) const;

  Status VerifyFile(ColumnFamilyData* cfd, const std::string& fpath, int level,
                    const FileMetaData& fmeta) override;
  Status VerifyBlobFile(ColumnFamilyData* cfd, uint64_t blob_file_num,
                        const BlobFileAddition& blob_addition) override;

 protected:
  Status OnAtomicGroupReplayBegin() override;
  Status OnAtomicGroupReplayEnd() override;
  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit) override;
  // `MaybeCreateVersionBeforeApplyEdit(..., false)` creates a version upon a
  // negative edge trigger (transition from valid to invalid).
  //
  // `MaybeCreateVersionBeforeApplyEdit(..., true)` creates a version on a
  // positive level trigger (state is valid).
  Status MaybeCreateVersionBeforeApplyEdit(const VersionEdit& edit,
                                           ColumnFamilyData* cfd,
                                           bool force_create_version) override;

  Status LoadTables(ColumnFamilyData* cfd,
                    bool prefetch_index_and_filter_in_cache,
                    bool is_initial_load) override;

  // Returns whether the builder of column family `cf_id` holds a state of the
  // MANIFEST being read, so that a Version may be created from it.
  virtual bool BuilderHoldsPointInTime(uint32_t /*cf_id*/) const {
    return true;
  }

  // A Version built from the MANIFEST records read up to some point in time,
  // together with the column family's log number that those records had put in
  // effect. Keeping the two together is what lets an installed Version report
  // the log number it covers.
  struct PointInTimeVersion {
    Version* version = nullptr;
    uint64_t log_number = 0;
  };

  std::unordered_map<uint32_t, PointInTimeVersion> versions_;

  // `atomic_update_versions_` is for ensuring all-or-nothing AtomicGroup
  // recoveries.  When `atomic_update_versions_` is nonempty, it serves as a
  // barrier to updating `versions_` until all its values are populated.
  std::unordered_map<uint32_t, PointInTimeVersion> atomic_update_versions_;
  // `atomic_update_versions_missing_` counts the null `version`s in
  // `atomic_update_versions_`.
  size_t atomic_update_versions_missing_;

  bool in_atomic_group_ = false;

  // When true (set only via the OpenAndCompact remote-compaction path),
  // recovery trusts the MANIFEST when reconstructing the LSM version: it does
  // not stat/open SST or blob files to classify them found/missing (VerifyFile
  // / VerifyBlobFile return OK) and does not open candidate-version table
  // handlers (LoadTableHandlers is skipped). This prevents both a fatal open of
  // a transient/obsolete file (added then later deleted in the MANIFEST,
  // already physically removed by a live primary) and a rollback to an earlier,
  // wrong LSM shape. Safe because the compaction's input files are
  // ref-protected from deletion by the primary, so any missing file is
  // necessarily a non-input file the compaction never reads; inputs are opened
  // (and unique-id verified) on demand by the compaction itself.
  //
  // NOTE: This does not yet suppress the bounded file-property sampling in
  // Version::PrepareAppend -> UpdateAccumulatedStats, which still reads some
  // non-input files' properties (tolerating any that are missing). That path is
  // needed today to populate input FileMetaData::num_entries for compaction
  // input-record-count verification. A strict "open only compaction inputs"
  // recovery is a planned follow-up that also skips that sampling and sources
  // input num_entries from the worker's own input-table-properties read.
  const bool trust_manifest_recovery_ = false;

  // When true, initial recovery trusts the MANIFEST's SST metadata and leaves
  // opening the installed Version's SSTs to the DB's background file-opening
  // job. ManifestTailer clears this after initial recovery so catch-up keeps
  // validating and opening newly discovered SSTs synchronously.
  bool defer_sst_file_opening_ = false;

 private:
  bool AtomicUpdateVersionsCompleted();
  bool AtomicUpdateVersionsContains(uint32_t cfid);
  void AtomicUpdateVersionsDropCf(uint32_t cfid);

  // This function is called for Version updates for column families in an
  // incomplete atomic update. It buffers them in `atomic_update_versions_`.
  void AtomicUpdateVersionsPut(PointInTimeVersion pit_version);

  // This function is called upon completion of an atomic update. It applies the
  // updates buffered in `atomic_update_versions_` to `versions_`.
  void AtomicUpdateVersionsApply();

  // The log number of the Version last installed for each column family. See
  // GetInstalledVersionLogNumber().
  std::unordered_map<uint32_t, uint64_t> installed_version_log_numbers_;
};

// A class similar to `VersionEditHandlerPointInTime` that parse MANIFEST and
// builds point in time version.
// `ManifestTailer` supports reading one MANIFEST file in multiple tailing
// attempts and supports switching to a different MANIFEST after
// `PrepareToReadNewManifest` is called. This class is used by secondary and
// follower instance.
class ManifestTailer : public VersionEditHandlerPointInTime {
 public:
  explicit ManifestTailer(std::vector<ColumnFamilyDescriptor> column_families,
                          VersionSet* version_set,
                          const std::shared_ptr<IOTracer>& io_tracer,
                          const ReadOptions& read_options,
                          bool trust_manifest_recovery,
                          bool defer_sst_file_opening,
                          EpochNumberRequirement epoch_number_requirement =
                              EpochNumberRequirement::kMustPresent)
      : VersionEditHandlerPointInTime(
            /*read_only=*/true, std::move(column_families), version_set,
            io_tracer, read_options,
            /*allow_incomplete_valid_version=*/false,
            /*record_missing_blob_files_as_dropped=*/false,
            trust_manifest_recovery, defer_sst_file_opening,
            epoch_number_requirement),
        mode_(Mode::kRecovery) {}

  Status VerifyFile(ColumnFamilyData* cfd, const std::string& fpath, int level,
                    const FileMetaData& fmeta) override;

  void PrepareToReadNewManifest() {
    initialized_ = false;
    ResetReadState();
    cfs_awaiting_from_previous_manifest_ = cfs_awaiting_file_list_;
  }

  std::unordered_set<ColumnFamilyData*>& GetUpdatedColumnFamilies() {
    return cfds_changed_;
  }

  std::vector<std::string> GetAndClearIntermediateFiles();

 protected:
  Status Initialize() override;

  bool MustOpenAllColumnFamilies() const override { return false; }

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override;

  Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd) override;

  bool BuilderHoldsPointInTime(uint32_t cf_id) const override {
    return cfs_awaiting_file_list_.count(cf_id) == 0;
  }

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

  void AwaitFileList(uint32_t cf_id);

  enum Mode : uint8_t {
    kRecovery = 0,
    kCatchUp = 1,
  };

  Mode mode_;
  std::unordered_set<ColumnFamilyData*> cfds_changed_;
  // Column families whose builder was reset to their empty initial Version to
  // replay a new MANIFEST, and that have not yet applied the record in it that
  // lists their files.
  std::unordered_set<uint32_t> cfs_awaiting_file_list_;
  // Column families that were awaiting their file list when the tailer
  // switched to the current MANIFEST, and that it has not added again.
  std::unordered_set<uint32_t> cfs_awaiting_from_previous_manifest_;
};

class DumpManifestHandler : public VersionEditHandler {
 public:
  DumpManifestHandler(std::vector<ColumnFamilyDescriptor> column_families,
                      VersionSet* version_set,
                      const std::shared_ptr<IOTracer>& io_tracer,
                      const ReadOptions& read_options, bool verbose, bool hex,
                      bool json)
      : VersionEditHandler(
            /*read_only=*/true, std::move(column_families), version_set,
            /*track_found_and_missing_files=*/false,
            /*no_error_if_files_missing=*/false, io_tracer, read_options,
            /*skip_load_table_files=*/true,
            /*allow_incomplete_valid_version=*/false,
            /*epoch_number_requirement=*/EpochNumberRequirement::kMustPresent),
        verbose_(verbose),
        hex_(hex),
        json_(json),
        count_(0) {
    cf_to_cmp_names_.reset(new std::unordered_map<uint32_t, std::string>());
  }

  ~DumpManifestHandler() override = default;

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd) override {
    // Write out each individual edit
    if (json_) {
      // Print out DebugStrings. Can include non-terminating null characters.
      std::string edit_dump_str = edit.DebugJSON(count_, hex_);
      fwrite(edit_dump_str.data(), sizeof(char), edit_dump_str.size(), stdout);
      fwrite("\n", sizeof(char), 1, stdout);
    } else if (verbose_) {
      // Print out DebugStrings. Can include non-terminating null characters.
      std::string edit_dump_str = edit.DebugString(hex_);
      fwrite(edit_dump_str.data(), sizeof(char), edit_dump_str.size(), stdout);
    }
    ++count_;
    return VersionEditHandler::ApplyVersionEdit(edit, cfd);
  }

  void CheckIterationResult(const log::Reader& reader, Status* s) override;

 private:
  const bool verbose_;
  const bool hex_;
  const bool json_;
  int count_;
};

}  // namespace ROCKSDB_NAMESPACE
