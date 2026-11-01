//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Repairer does best effort recovery to recover as much data as possible after
// a disaster without compromising consistency. It does not guarantee bringing
// the database to a time consistent state.
//
// Repair process is broken into 4 phases:
// (a) Find files
// (b) Convert logs to tables
// (c) Extract metadata
// (d) Write Descriptor
//
// (a) Find files
//
// The repairer goes through all the files in the directory, and classifies them
// based on their file name. Any file that cannot be identified by name will be
// ignored.
//
// (b) Convert logs to table
//
// Legacy repair replays every active log file, skipping sections whose checksum
// does not match. Activated blob-indirection repair instead certifies a
// complete global WAL prefix in its owned catalog. It retains an unconverted
// suffix when transaction context cannot be reconstructed, rather than
// partially converting that suffix or certifying incomplete coverage.
//
// (c) Extract metadata
//
// We scan every table to compute
// (1) smallest/largest for the table
// (2) largest sequence number in the table
// (3) oldest blob file referred to by the table (if applicable)
//
// If we are unable to scan the file, then we ignore the table.
//
// (d) Write Descriptor
//
// We generate descriptor contents:
//  - legacy repair sets log number to zero; activated blob-indirection repair
//    publishes each CF's certified/proven log floor to avoid double replay
//  - next-file-number is set to 1 + largest file number we found
//  - last-sequence-number is set to largest sequence# found across
//    all tables (see 2c)
//  - compaction pointers are cleared
//  - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
//   (d) We can provide options for time consistent recovery and unsafe recovery
//       (ignore checksum failure when applicable)
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.

#include <cinttypes>
#include <map>
#include <queue>
#include <set>
#include <tuple>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_sequential_reader.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_file.h"
#include "db/blob/blob_relocation_manifest.h"
#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_edit_handler.h"
#include "db/wide/wide_column_serialization.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/adaptive/adaptive_table_factory.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/embedded_blob_sst.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint64_t kMaxRepairBlobPayloadSize = uint64_t{256} * 1024 * 1024;

class RepairLossLogReporter : public log::Reader::Reporter {
 public:
  explicit RepairLossLogReporter(Status* status) : status_(status) {}

  void Corruption(size_t, const Status& status, uint64_t) override {
    if (status_->ok()) {
      *status_ = status;
    }
  }

 private:
  Status* const status_;
};

// Repair normally reconstructs metadata from physical files. Durable loss is
// different: a restored copy of a deliberately lost file must not resurrect
// its keys. Retain the last fully decoded MANIFEST units that describe loss,
// without opening SSTs or materializing their individual blob references.
class RepairRelocationLossHandler : public VersionEditHandlerBase {
 public:
  explicit RepairRelocationLossHandler(const ReadOptions& read_options)
      : VersionEditHandlerBase(read_options) {}

  std::map<uint64_t, BlobRelocationFileAddition> files;
  std::set<uint64_t> missing_files;
  std::map<uint32_t, std::map<BlobRelocationRouteSource, uint64_t>> routes;
  std::map<uint32_t, uint64_t> cf_floors;
  std::map<uint32_t, std::map<uint64_t, FileMetaData>> cf_tables;
  std::map<uint32_t, std::string> cf_names{{0, kDefaultColumnFamilyName}};
  std::set<uint32_t> feature_cfs;
  SequenceNumber last_sequence = 0;

  void DiscardIncompleteAtomicGroup() {
    if (in_atomic_group_) {
      files.swap(saved_files_);
      missing_files.swap(saved_missing_files_);
      routes.swap(saved_routes_);
      cf_floors.swap(saved_cf_floors_);
      cf_tables.swap(saved_cf_tables_);
      cf_names.swap(saved_cf_names_);
      feature_cfs.swap(saved_feature_cfs_);
      last_sequence = saved_last_sequence_;
    }
  }

 protected:
  Status OnAtomicGroupReplayBegin() override {
    saved_files_ = files;
    saved_missing_files_ = missing_files;
    saved_routes_ = routes;
    saved_cf_floors_ = cf_floors;
    saved_cf_tables_ = cf_tables;
    saved_cf_names_ = cf_names;
    saved_feature_cfs_ = feature_cfs;
    saved_last_sequence_ = last_sequence;
    in_atomic_group_ = true;
    return Status::OK();
  }

  Status OnAtomicGroupReplayEnd() override {
    in_atomic_group_ = false;
    saved_files_.clear();
    saved_missing_files_.clear();
    saved_routes_.clear();
    saved_cf_floors_.clear();
    saved_cf_tables_.clear();
    saved_cf_names_.clear();
    saved_feature_cfs_.clear();
    return Status::OK();
  }

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData**) override {
    const uint32_t cf_id = edit.GetColumnFamily();
    if (edit.IsColumnFamilyDrop()) {
      for (std::map<uint64_t, BlobRelocationFileAddition>::iterator file =
               files.begin();
           file != files.end();) {
        if (file->second.GetColumnFamilyId() == cf_id) {
          missing_files.erase(file->first);
          file = files.erase(file);
        } else {
          ++file;
        }
      }
      routes.erase(cf_id);
      cf_floors.erase(cf_id);
      cf_tables.erase(cf_id);
      cf_names.erase(cf_id);
      feature_cfs.erase(cf_id);
      return Status::OK();
    }
    // Validate the whole edit before mutating the retained valid prefix. A
    // malformed standalone unit must not leave its earlier additions/losses
    // installed; incomplete atomic groups have a separate whole-group undo.
    std::map<uint64_t, const BlobRelocationFileAddition*> additions;
    for (const BlobRelocationFileAddition& addition :
         edit.GetBlobRelocationFileAdditions()) {
      if (addition.GetColumnFamilyId() != cf_id) {
        return Status::Corruption(
            "Repair loss metadata crosses column families");
      }
      std::shared_ptr<const BlobRelocationDirectory> directory;
      Status status = addition.GetValidatedDirectory(&directory);
      if (!status.ok()) {
        return status;
      }
      const auto current = files.find(addition.GetFileNumber());
      if (current != files.end() &&
          current->second.GetColumnFamilyId() != cf_id) {
        return Status::Corruption("Repair loss file has conflicting ownership");
      }
      if (!additions.emplace(addition.GetFileNumber(), &addition).second) {
        return Status::Corruption("Duplicate repair relocation file addition");
      }
    }
    const auto get_file =
        [&](uint64_t number) -> const BlobRelocationFileAddition* {
      const auto added = additions.find(number);
      if (added != additions.end()) {
        return added->second;
      }
      const auto current = files.find(number);
      return current == files.end() ? nullptr : &current->second;
    };
    for (uint64_t number : edit.GetBlobRelocationFileLosses()) {
      const BlobRelocationFileAddition* const file = get_file(number);
      if (file == nullptr || file->GetColumnFamilyId() != cf_id ||
          std::find(edit.GetBlobRelocationFileDeletions().begin(),
                    edit.GetBlobRelocationFileDeletions().end(),
                    number) != edit.GetBlobRelocationFileDeletions().end()) {
        return Status::Corruption("Repair loss record names no live file");
      }
    }
    for (uint64_t number : edit.GetBlobRelocationFileDeletions()) {
      const BlobRelocationFileAddition* const file = get_file(number);
      if (additions.count(number) != 0 ||
          (file != nullptr && file->GetColumnFamilyId() != cf_id)) {
        return Status::Corruption(
            "Repair loss deletion crosses owners/additions");
      }
    }
    for (const BlobRelocationRoute& route : edit.GetBlobRelocationRoutes()) {
      const BlobRelocationFileAddition* const file =
          get_file(route.GetPhysicalFileNumber());
      if (file == nullptr || file->GetColumnFamilyId() != cf_id) {
        return Status::Corruption(
            "Repair route names no CF-owned relocation file");
      }
    }
    for (const BlobRelocationFileAddition& addition :
         edit.GetBlobRelocationFileAdditions()) {
      files.insert_or_assign(addition.GetFileNumber(), addition);
    }
    for (uint64_t number : edit.GetBlobRelocationFileLosses()) {
      missing_files.insert(number);
    }
    for (const BlobRelocationRoute& route : edit.GetBlobRelocationRoutes()) {
      routes[cf_id].insert_or_assign(
          BlobRelocationRouteSource{route.GetGenerationFileNumber(),
                                    route.GetOriginFileNumber()},
          route.GetPhysicalFileNumber());
    }
    for (const BlobRelocationRouteSource& source :
         edit.GetBlobRelocationRouteDeletions()) {
      routes[cf_id].erase(source);
    }
    for (uint64_t number : edit.GetBlobRelocationFileDeletions()) {
      files.erase(number);
      missing_files.erase(number);
    }
    if (edit.HasLogNumber()) {
      cf_floors[cf_id] = edit.GetLogNumber();
    }
    if (edit.HasBlobIndirectionFormatVersion() ||
        !edit.GetBlobRelocationFileAdditions().empty()) {
      feature_cfs.insert(cf_id);
    }
    if (edit.IsColumnFamilyAdd()) {
      cf_names[cf_id] = edit.GetColumnFamilyName();
    }
    for (const auto& deleted : edit.GetDeletedFiles()) {
      cf_tables[cf_id].erase(deleted.second);
    }
    for (const auto& added : edit.GetNewFiles()) {
      cf_tables[cf_id].insert_or_assign(added.second.fd.GetNumber(),
                                        added.second);
    }
    if (edit.HasLastSequence()) {
      last_sequence = std::max(last_sequence, edit.GetLastSequence());
    }
    return Status::OK();
  }

 private:
  bool in_atomic_group_ = false;
  std::map<uint64_t, BlobRelocationFileAddition> saved_files_;
  std::set<uint64_t> saved_missing_files_;
  std::map<uint32_t, std::map<BlobRelocationRouteSource, uint64_t>>
      saved_routes_;
  std::map<uint32_t, uint64_t> saved_cf_floors_;
  std::map<uint32_t, std::map<uint64_t, FileMetaData>> saved_cf_tables_;
  std::map<uint32_t, std::string> saved_cf_names_;
  std::set<uint32_t> saved_feature_cfs_;
  SequenceNumber saved_last_sequence_ = 0;
};

// Decode an entire logical batch before standalone insertion can partially
// mutate any CF. Recovery-context markers are recognized, never sent to a
// null-DB transaction handler. Unknown CFs retain explicit repair skip policy.
class RepairBatchValidator : public WriteBatch::Handler {
 public:
  explicit RepairBatchValidator(ColumnFamilySet* families)
      : families_(families) {}
  bool requires_context = false;
  size_t ignored_cf_operations = 0;
  Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
    return Check(cf, key);
  }
  Status TimedPutCF(uint32_t cf, const Slice& key, const Slice&,
                    uint64_t) override {
    return Check(cf, key);
  }
  Status DeleteCF(uint32_t cf, const Slice& key) override {
    return Check(cf, key);
  }
  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    return Check(cf, key);
  }
  Status DeleteRangeCF(uint32_t cf, const Slice& begin,
                       const Slice& end) override {
    Status status = Check(cf, begin);
    return status.ok() ? Check(cf, end) : status;
  }
  Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
    return Check(cf, key);
  }
  Status PutEntityCF(uint32_t cf, const Slice& key,
                     const Slice& entity) override {
    Status status = Check(cf, key);
    if (!status.ok()) {
      return status;
    }
    return WideColumnSerialization::ForEachBlobIndex(
        entity, [](const BlobIndex&) { return Status::OK(); });
  }
  Status PutBlobIndexCF(uint32_t cf, const Slice& key,
                        const Slice& value) override {
    Status status = Check(cf, key);
    BlobIndex index;
    return status.ok() ? index.DecodeFrom(value) : status;
  }
  Status MarkBeginPrepare(bool) override { return Context(); }
  Status MarkEndPrepare(const Slice&) override { return Context(); }
  Status MarkNoop(bool) override { return Context(); }
  Status MarkRollback(const Slice&) override { return Context(); }
  Status MarkCommit(const Slice&) override { return Context(); }
  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
    return Context();
  }

 private:
  Status Context() {
    requires_context = true;
    return Status::OK();
  }
  Status Check(uint32_t cf, const Slice& key) {
    const ColumnFamilyData* cfd = families_->GetColumnFamily(cf);
    if (cfd == nullptr) {
      ++ignored_cf_operations;
      return Status::OK();
    }
    return key.size() >= cfd->user_comparator()->timestamp_size()
               ? Status::OK()
               : Status::Corruption("Repair WAL key lacks timestamp");
  }
  ColumnFamilySet* const families_;
};

class Repairer {
 public:
  Repairer(const std::string& dbname, const DBOptions& db_options,
           const std::vector<ColumnFamilyDescriptor>& column_families,
           const ColumnFamilyOptions& default_cf_opts,
           const ColumnFamilyOptions& unknown_cf_opts, bool create_unknown_cfs)
      : dbname_(dbname),
        db_session_id_(DBImpl::GenerateDbSessionId(db_options.env)),
        env_(db_options.env),
        file_options_(),
        db_options_(SanitizeOptions(dbname_, db_options)),
        immutable_db_options_(ImmutableDBOptions(db_options_)),
        icmp_(default_cf_opts.comparator),
        default_cf_opts_(SanitizeCfOptions(immutable_db_options_,
                                           /*read_only*/ false,
                                           default_cf_opts)),
        default_iopts_(
            ImmutableOptions(immutable_db_options_, default_cf_opts_)),
        default_mopts_(MutableCFOptions(default_cf_opts_)),
        unknown_cf_opts_(SanitizeCfOptions(immutable_db_options_,
                                           /*read_only*/ false,
                                           unknown_cf_opts)),
        create_unknown_cfs_(create_unknown_cfs),
        raw_table_cache_(
            // TableCache can be small since we expect each table to be opened
            // once.
            NewLRUCache(10, db_options_.table_cache_numshardbits)),
        table_cache_(new TableCache(default_iopts_, &file_options_,
                                    raw_table_cache_.get(),
                                    /*block_cache_tracer=*/nullptr,
                                    /*io_tracer=*/nullptr, db_session_id_)),
        wb_(db_options_.db_write_buffer_size),
        wc_(db_options_.delayed_write_rate),
        vset_(dbname_, &immutable_db_options_, MutableDBOptions{db_options_},
              file_options_, raw_table_cache_.get(), &wb_, &wc_,
              /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
              /*db_id=*/"", db_session_id_, db_options.daily_offpeak_time_utc,
              /*error_handler=*/nullptr, /*read_only=*/false),
        next_file_number_(1),
        db_lock_(nullptr),
        closed_(false) {
    for (const auto& cfd : column_families) {
      cf_name_to_opts_[cfd.name] = SanitizeCfOptions(
          immutable_db_options_, /*read_only=*/false, cfd.options);
    }
  }

  // No copying allowed
  Repairer(const Repairer&) = delete;
  Repairer& operator=(const Repairer&) = delete;
  Repairer(Repairer&&) = delete;
  Repairer& operator=(Repairer&&) = delete;

  const ColumnFamilyOptions* GetColumnFamilyOptions(
      const std::string& cf_name) {
    if (cf_name_to_opts_.find(cf_name) == cf_name_to_opts_.end()) {
      if (create_unknown_cfs_) {
        return &unknown_cf_opts_;
      }
      return nullptr;
    }
    return &cf_name_to_opts_[cf_name];
  }

  // Adds a column family to the VersionSet with cf_options_ and updates
  // manifest.
  Status AddColumnFamily(const std::string& cf_name, uint32_t cf_id) {
    // TODO: plumb Env::IOActivity, Env::IOPriority;
    const ReadOptions read_options;
    const WriteOptions write_options;

    const auto* cf_opts = GetColumnFamilyOptions(cf_name);
    if (cf_opts == nullptr) {
      return Status::Corruption("Encountered unknown column family with name=" +
                                cf_name + ", id=" + std::to_string(cf_id));
    }
    Options opts(db_options_, *cf_opts);

    VersionEdit edit;
    edit.SetComparatorName(opts.comparator->Name());
    edit.SetPersistUserDefinedTimestamps(opts.persist_user_defined_timestamps);
    edit.SetLogNumber(0);
    edit.SetColumnFamily(cf_id);
    ColumnFamilyData* cfd;
    cfd = nullptr;
    edit.AddColumnFamily(cf_name);

    mutex_.Lock();
    std::unique_ptr<FSDirectory> db_dir;
    Status status = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(),
                                                        &db_dir, nullptr);
    if (status.ok()) {
      status = vset_.LogAndApply(cfd, read_options, write_options, &edit,
                                 &mutex_, db_dir.get(),
                                 false /* new_descriptor_log */, cf_opts);
    }
    mutex_.Unlock();
    return status;
  }

  Status Close() {
    Status s = Status::OK();
    if (!closed_) {
      if (repair_catalog_log_ != nullptr) {
        s = repair_catalog_log_->Close(WriteOptions());
        repair_catalog_log_.reset();
      }
      if (db_lock_ != nullptr) {
        Status unlocked = env_->UnlockFile(db_lock_);
        if (s.ok()) {
          s = unlocked;
        } else {
          unlocked.PermitUncheckedError();
        }
        db_lock_ = nullptr;
      }
      closed_ = true;
    }
    return s;
  }

  ~Repairer() { Close().PermitUncheckedError(); }

  Status Run() {
    Status status = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!status.ok()) {
      return status;
    }
    bool recognized = false;
    Status catalog_status =
        ReadRepairCatalog(env_, dbname_, &repair_catalog_, &recognized);
    if (!catalog_status.ok() && recognized) {
      return catalog_status;
    }
    if (recognized) {
      bool committed = false;
      status = ResolveRepairCatalog(env_, dbname_, repair_catalog_, false, true,
                                    &committed);
      if (!status.ok() || committed) {
        return status;
      }
      repair_catalog_ = {};
    }
    status = FindFiles();
    const size_t original_blob_count = blob_file_paths_.size();
    if (status.ok()) {
      // Configuration is not activation proof. Probe secondary CF blob paths
      // before choosing repair semantics, including when policy is now off.
      status = FindFeatureColumnFamilyBlobFiles(true);
    }
    if (status.ok()) {
      status = DetectBlobIndirection();
    }
    if (status.ok() && !feature_repair_) {
      // A detection-only probe is not feature authority, but discovered file
      // numbers still reserve physical identities before legacy WAL output.
      blob_file_paths_.resize(original_blob_count);
      return RunLegacyRepair();
    }
    if (status.ok() && !catalog_status.ok()) {
      return catalog_status;
    }
    if (status.ok()) {
      // Once detection is latched these paths are required, not optional. Bind
      // all donors and allocation high-water marks before the catalog starts.
      status = FindFeatureColumnFamilyBlobFiles(false);
    }
    if (status.ok()) {
      status = ReadDurableBlobLoss();
    }
    if (status.ok()) {
      status = StartRepairCatalog();
    }
    if (status.ok()) {
      status = AllocateRepairFileNumber(&repair_manifest_file_number_);
      if (status.ok()) {
        // Use a dedicated namespace: a later-discovered secondary-path donor
        // may have this same number and use N.dbtmp while restoring its blob.
        // Neither this prefix nor extension is discoverable as a MANIFEST.
        repair_manifest_path_ = dbname_ + "/REPAIR-" +
                                std::to_string(repair_manifest_file_number_) +
                                "-" + db_session_id_ + ".dbtmp";
        status = RegisterRepairOutput(repair_manifest_path_,
                                      repair_manifest_file_number_, 0, 0);
        if (status.ok()) {
          status = RegisterRepairOutput(
              DescriptorFileName(dbname_, repair_manifest_file_number_),
              repair_manifest_file_number_, 0, 0);
        }
        if (status.ok()) {
          status = vset_.RecoverForRepair(
              repair_manifest_path_, repair_manifest_file_number_,
              {{kDefaultColumnFamilyName, default_cf_opts_}});
        }
      }
    }
    if (status.ok()) {
      // Need to scan existing SST files first so the column families are
      // created before we process WAL files
      status = ExtractMetaData();

      // ExtractMetaData() uses table_fds_ to know which SST files' metadata to
      // extract -- we need to clear it here since metadata for existing SST
      // files has been extracted already
      if (status.ok()) {
        status = FindFeatureColumnFamilyBlobFiles(false);
      }
      if (status.ok()) {
        table_fds_.clear();
        status = PrepareOriginalReplayCoverage();
        if (status.ok()) {
          status = ConvertLogFilesToTables();
        }
        if (status.ok()) {
          status = ExtractMetaData();
        }
      }
    }
    if (status.ok()) {
      status = RepairBlobState();
      if (status.ok()) {
        status = AddTables();
      }
      if (status.ok()) {
        status = PublishManifest();
      }
    }
    if (status.ok()) {
      uint64_t bytes = 0;
      for (size_t i = 0; i < tables_.size(); i++) {
        bytes += tables_[i].meta.fd.GetFileSize();
      }
      ROCKS_LOG_WARN(db_options_.info_log,
                     "**** Repaired rocksdb %s; "
                     "recovered %" ROCKSDB_PRIszt " files; %" PRIu64
                     " bytes. "
                     "Some data may have been lost. "
                     "****",
                     dbname_.c_str(), tables_.size(), bytes);
    }
    return status;
  }

 private:
  using BlobIdentity = std::pair<uint64_t, uint64_t>;
  using RepairReferenceSource =
      std::tuple<BlobReferenceEncodingClass, uint64_t, uint64_t>;
  static RepairReferenceSource ReferenceSource(const BlobReference& reference) {
    return {reference.encoding_class, reference.generation_file_number,
            reference.origin_file_number};
  }

  struct TableInfo {
    FileMetaData meta;
    uint32_t column_family_id;
    std::string column_family_name;
    std::set<RepairReferenceSource> reference_sources;
  };

  struct BlobFilePath {
    uint64_t file_number = 0;
    std::string path;
  };

  struct NormalBlobFileInfo {
    BlobFileAddition addition;
    uint32_t column_family_id = 0;
    uint64_t file_size = 0;
    CompressionType compression = kNoCompression;
    std::string path;
    bool is_canonical = false;
  };

  struct RelocationBlobFileInfo {
    uint32_t column_family_id = 0;
    uint64_t file_size = 0;
    std::string path;
    bool is_canonical = false;
    BlobRelocationFileAddition addition;
    BlobRelocationDirectory directory;
  };

  using NormalBlobFileCandidates = std::multimap<uint64_t, NormalBlobFileInfo>;
  using RelocationBlobFileCandidates =
      std::multimap<uint64_t, RelocationBlobFileInfo>;

  struct BlobReaderCache {
    uint64_t file_number = kInvalidBlobFileNumber;
    std::string path;
    std::unique_ptr<BlobFileReader> reader;
    std::string normal_record_path;
    std::unique_ptr<BlobLogSequentialReader> normal_records;
    uint64_t checked_offset = 0;
    uint64_t checked_value_size = 0;
    uint64_t checked_record_bytes = 0;
    std::string checked_user_key;
  };

  struct RepairInputGroup {
    TableInfo* table;
    RepairReferenceSource source;
  };

  struct RepairReferenceCursor {
    Repairer* repairer;
    RepairInputGroup input;
    ReadOptions read_options;
    TableReader* table_reader = nullptr;
    std::unique_ptr<InternalIterator> data_iterator;
    std::unique_ptr<BlobReferenceIterator> reference_iterator;
    std::unique_ptr<InternalIterator> legacy_lookup;
    std::shared_ptr<CacheReservationManager> reservation_manager;
    std::vector<BlobReference> pending;
    size_t position = 0;
    bool valid = false;

    ~RepairReferenceCursor() {
      if (reference_iterator == nullptr && reservation_manager != nullptr) {
        reservation_manager->UpdateCacheReservation(0).PermitUncheckedError();
      }
    }

    Status Open() {
      read_options.fill_cache = false;
      read_options.verify_checksums = true;
      read_options.total_order_seek = true;
      ColumnFamilyData* const cfd =
          repairer->vset_.GetColumnFamilySet()->GetColumnFamily(
              input.table->column_family_id);
      if (cfd == nullptr) {
        return Status::Corruption("Missing repair cursor column family");
      }
      data_iterator.reset(repairer->table_cache_->NewIterator(
          read_options, repairer->file_options_, cfd->internal_comparator(),
          input.table->meta, nullptr, cfd->GetLatestMutableCFOptions(),
          &table_reader, nullptr, TableReaderCaller::kRepair, nullptr, true, -1,
          0, nullptr, nullptr, false, nullptr, nullptr, false, nullptr,
          RepairTableOpenOptions()));
      Status status = data_iterator->status();
      if (!status.ok() || table_reader == nullptr) {
        return status.ok() ? Status::Corruption("Missing repair cursor reader")
                           : status;
      }
      const BlockBasedTableOptions* const options =
          GetBlockBasedTableWriterOptions(
              cfd->GetLatestMutableCFOptions().table_factory.get());
      if (options != nullptr && options->block_cache != nullptr) {
        reservation_manager = std::make_shared<CacheReservationManagerImpl<
            CacheEntryRole::kBlobReferenceMetadataBuildingBuffer>>(
            options->block_cache);
      }
      if (!input.table->meta.blob_reference_descriptor.empty()) {
        status = table_reader->NewBlobReferenceIterator(
            read_options, input.table->meta.blob_reference_descriptor,
            reservation_manager, &reference_iterator);
        if (status.ok()) {
          status = reference_iterator->SeekSource(std::get<0>(input.source),
                                                  std::get<1>(input.source),
                                                  std::get<2>(input.source));
          valid = status.ok() && reference_iterator->Valid();
        }
        return status;
      }
      data_iterator->SeekToFirst();
      return LoadPhysicalReferences();
    }

    const BlobReference& reference() const {
      assert(valid);
      return reference_iterator != nullptr ? reference_iterator->reference()
                                           : pending[position];
    }

    Status Next() {
      assert(valid);
      if (reference_iterator != nullptr) {
        Status status = reference_iterator->Next();
        valid = status.ok() && reference_iterator->Valid();
        return status;
      }
      ++position;
      if (position != pending.size()) {
        return Status::OK();
      }
      data_iterator->Next();
      return LoadPhysicalReferences();
    }

    Status LoadPhysicalReferences() {
      valid = false;
      pending.clear();
      position = 0;
      // Ordinary flush/L0 output writes a normal identity root in internal-key
      // order. Filtering that stream for one source preserves physical offset
      // order. Exact/relocated SSTs use their sorted metadata cursor instead.
      for (; data_iterator->Valid(); data_iterator->Next()) {
        ParsedInternalKey key;
        Status status = ParseInternalKey(data_iterator->key(), &key, false);
        if (!status.ok()) {
          return status;
        }
        const auto collect = [&](const BlobIndex& index) -> Status {
          BlobReference reference;
          bool included = false;
          Status decoded =
              BlobReference::FromBlobIndex(index, &reference, &included);
          if (!decoded.ok() || !included ||
              ReferenceSource(reference) != input.source) {
            return decoded;
          }
          const uint64_t header_bytes =
              BlobLogRecord::CalculateAdjustmentForRecordHeader(
                  key.user_key.size());
          if (reference.stored_value_size >
              std::numeric_limits<uint64_t>::max() - header_bytes) {
            return Status::Corruption("Repair physical record size overflows");
          }
          reference.record_bytes = reference.stored_value_size + header_bytes;
          if (pending.size() == pending.capacity()) {
            const size_t capacity =
                pending.capacity() == 0 ? 1 : pending.capacity() * 2;
            if (capacity < pending.capacity() ||
                capacity >
                    (std::numeric_limits<size_t>::max() - sizeof(*this)) /
                        sizeof(BlobReference)) {
              return Status::MemoryLimit(
                  "Repair physical reference buffer overflows");
            }
            if (reservation_manager != nullptr) {
              Status reserved = reservation_manager->UpdateCacheReservation(
                  sizeof(*this) + capacity * sizeof(BlobReference));
              if (!reserved.ok()) {
                return reserved;
              }
            }
            pending.reserve(capacity);
          }
          pending.push_back(reference);
          return Status::OK();
        };
        if (key.type == kTypeBlobIndex) {
          BlobIndex index;
          status = index.DecodeFrom(data_iterator->value());
          if (status.ok()) {
            status = collect(index);
          }
        } else if (key.type == kTypeWideColumnEntity) {
          status = WideColumnSerialization::ForEachBlobIndex(
              data_iterator->value(), collect);
        }
        if (!status.ok()) {
          return status;
        }
        if (!pending.empty()) {
          valid = true;
          return Status::OK();
        }
      }
      return data_iterator->status();
    }

    Status VerifyLegacyUserKey(const BlobReference& expected,
                               const std::string& blob_user_key) {
      ColumnFamilyData* const cfd =
          repairer->vset_.GetColumnFamilySet()->GetColumnFamily(
              input.table->column_family_id);
      const InternalKey target(blob_user_key, kMaxSequenceNumber,
                               kValueTypeForSeek);
      if (reference_iterator == nullptr) {
        ParsedInternalKey current_key;
        Status status =
            ParseInternalKey(data_iterator->key(), &current_key, false);
        if (!status.ok()) {
          return status;
        }
        if (current_key.user_key != blob_user_key) {
          return Status::Corruption(
              "Legacy blob key differs from referencing SST key");
        }
      }
      if (legacy_lookup == nullptr) {
        legacy_lookup.reset(table_reader->NewIterator(
            read_options, nullptr, nullptr, true, TableReaderCaller::kRepair));
      }
      legacy_lookup->Seek(target.Encode());
      for (; legacy_lookup->Valid(); legacy_lookup->Next()) {
        ParsedInternalKey key;
        Status status = ParseInternalKey(legacy_lookup->key(), &key, false);
        if (!status.ok()) {
          return status;
        }
        if (!cfd->user_comparator()->Equal(key.user_key, blob_user_key)) {
          break;
        }
        bool found = false;
        const auto check = [&](const BlobIndex& index) -> Status {
          BlobReference actual;
          bool included = false;
          Status decoded =
              BlobReference::FromBlobIndex(index, &actual, &included);
          found |=
              decoded.ok() && included && !actual.HasStableIdentity() &&
              actual.generation_file_number ==
                  expected.generation_file_number &&
              actual.original_value_offset == expected.original_value_offset &&
              actual.stored_value_size == expected.stored_value_size &&
              actual.compression == expected.compression;
          return decoded;
        };
        if (key.type == kTypeBlobIndex) {
          BlobIndex index;
          status = index.DecodeFrom(legacy_lookup->value());
          if (status.ok()) {
            status = check(index);
          }
        } else if (key.type == kTypeWideColumnEntity) {
          status = WideColumnSerialization::ForEachBlobIndex(
              legacy_lookup->value(), check);
        }
        if (!status.ok()) {
          return status;
        }
        if (found) {
          std::string copied_key = key.user_key.ToString();
          TEST_SYNC_POINT_CALLBACK(
              "Repairer::ScanTable:LegacyBlobReferenceUserKey", &copied_key);
          return copied_key == blob_user_key
                     ? Status::OK()
                     : Status::Corruption(
                           "Legacy blob key does not match SST contents");
        }
      }
      const Status status = legacy_lookup->status();
      return status.ok()
                 ? Status::Corruption(
                       "Legacy blob reference is absent from SST user key")
                 : status;
    }
  };

  using RepairReferenceVisitor = std::function<Status(
      const BlobReference&, const std::vector<RepairReferenceCursor*>&)>;

  Status VisitRepairReferences(const std::vector<RepairInputGroup>& inputs,
                               const RepairReferenceVisitor& visitor) {
    std::vector<std::unique_ptr<RepairReferenceCursor>> cursors;
    const auto later = [](const RepairReferenceCursor* lhs,
                          const RepairReferenceCursor* rhs) {
      return lhs->reference().original_value_offset >
             rhs->reference().original_value_offset;
    };
    std::priority_queue<RepairReferenceCursor*,
                        std::vector<RepairReferenceCursor*>, decltype(later)>
        heap(later);
    for (const RepairInputGroup& input : inputs) {
      std::unique_ptr<RepairReferenceCursor> cursor =
          std::make_unique<RepairReferenceCursor>();
      cursor->repairer = this;
      cursor->input = input;
      Status status = cursor->Open();
      if (!status.ok()) {
        return status;
      }
      if (!cursor->valid) {
        return Status::Corruption(
            "Repair SST is missing a required reference source");
      }
      heap.push(cursor.get());
      cursors.push_back(std::move(cursor));
    }
    std::vector<RepairReferenceCursor*> owners;
    while (!heap.empty()) {
      const BlobReference reference = heap.top()->reference();
      owners.clear();
      while (!heap.empty() && heap.top()->reference().original_value_offset ==
                                  reference.original_value_offset) {
        RepairReferenceCursor* const cursor = heap.top();
        heap.pop();
        const BlobReference& other = cursor->reference();
        if (reference.HasStableIdentity()
                ? !SameStableBlobMetadata(reference, other)
                : !SameBlobReference(reference, other)) {
          return Status::Corruption(
              "Conflicting immutable metadata for one blob identity");
        }
        owners.push_back(cursor);
      }
      Status status = visitor(reference, owners);
      if (!status.ok()) {
        return status;
      }
      for (RepairReferenceCursor* cursor : owners) {
        const uint64_t previous = cursor->reference().original_value_offset;
        do {
          status = cursor->Next();
          if (!status.ok()) {
            return status;
          }
          if (cursor->valid &&
              cursor->reference().original_value_offset == previous &&
              (reference.HasStableIdentity()
                   ? !SameStableBlobMetadata(reference, cursor->reference())
                   : !SameBlobReference(reference, cursor->reference()))) {
            return Status::Corruption("Conflicting duplicate repair reference");
          }
        } while (cursor->valid &&
                 cursor->reference().original_value_offset == previous);
        if (cursor->valid) {
          if (cursor->reference().original_value_offset < previous) {
            return Status::Corruption(
                "Repair physical references are not ordered");
          }
          heap.push(cursor);
        }
      }
    }
    return Status::OK();
  }

  enum class NormalBlobValidationMode {
    kPublishedFile,
    kPayloadDonor,
  };

  static TableCacheOpenOptions RepairTableOpenOptions() {
    TableCacheOpenOptions options;
    options.open_ephemeral_table_reader = true;
    options.avoid_shared_metadata_cache = true;
    options.isolate_repair_caches = true;
    options.skip_filters = true;
    return options;
  }

  struct ColumnFamilyBlobRepairPlan {
    std::map<uint64_t, BlobFileAddition> normal_files;
    std::map<uint64_t, BlobRelocationFileAddition> relocation_files;
    std::map<BlobRelocationRouteSource, uint64_t> routes;
    std::set<uint64_t> missing_files;
  };

  std::string const dbname_;
  std::string db_session_id_;
  Env* const env_;
  const FileOptions file_options_;
  const DBOptions db_options_;
  const ImmutableDBOptions immutable_db_options_;
  const InternalKeyComparator icmp_;
  const ColumnFamilyOptions default_cf_opts_;
  const ImmutableOptions default_iopts_;  // table_cache_ holds reference
  const MutableCFOptions default_mopts_;
  const ColumnFamilyOptions unknown_cf_opts_;
  const bool create_unknown_cfs_;
  std::shared_ptr<Cache> raw_table_cache_;
  std::unique_ptr<TableCache> table_cache_;
  WriteBufferManager wb_;
  WriteController wc_;
  VersionSet vset_;
  std::unordered_map<std::string, ColumnFamilyOptions> cf_name_to_opts_;
  InstrumentedMutex mutex_;

  std::vector<std::string> manifests_;
  std::vector<std::string> archive_after_publication_;
  std::vector<FileDescriptor> table_fds_;
  std::vector<uint64_t> logs_;
  std::vector<TableInfo> tables_;
  std::vector<BlobFilePath> blob_file_paths_;
  std::set<uint32_t> feature_format_column_families_;
  std::unordered_map<uint32_t, ColumnFamilyBlobRepairPlan> blob_repair_plans_;
  std::map<uint64_t, BlobRelocationFileAddition> durable_loss_files_;
  std::map<uint32_t, std::map<BlobRelocationRouteSource, uint64_t>>
      durable_loss_routes_;
  uint64_t next_file_number_;
  uint64_t repair_manifest_file_number_ = 0;
  std::string repair_manifest_path_;
  bool feature_repair_ = false;
  RepairCatalog repair_catalog_;
  std::unique_ptr<log::Writer> repair_catalog_log_;
  std::string original_current_;
  bool original_current_absent_ = false;
  RepairCatalogRecord original_manifest_authority_;
  uint64_t certified_wal_floor_ = 0;
  SequenceNumber certified_max_sequence_ = 0;
  bool deferred_wal_ = false;
  bool original_manifest_complete_ = false;
  std::map<uint32_t, uint64_t> original_cf_floors_;
  std::map<uint32_t, std::string> original_cf_names_;
  std::map<uint32_t, std::map<uint64_t, FileMetaData>> original_cf_tables_;
  std::map<uint32_t, uint64_t> proven_original_cf_floors_;
  // Lock over the persistent DB state. Non-nullptr iff successfully
  // acquired.
  FileLock* db_lock_;
  bool closed_;

  Status RunLegacyRepair() {
    // Positive persisted evidence, not the configured policy flag or a probe
    // error, selects stricter feature recovery. Preserve the base salvage
    // contract, including early publication and immediate archival attempts.
    for (const std::string& manifest : manifests_) {
      ArchiveFile(dbname_ + "/" + manifest);
    }
    DBImpl db(db_options_, dbname_);
    Status status = db.NewDB(nullptr);
    if (status.ok()) {
      status =
          vset_.Recover({{kDefaultColumnFamilyName, default_cf_opts_}}, false);
    }
    if (status.ok()) {
      ExtractMetaData().PermitUncheckedError();
      table_fds_.clear();
      ConvertLogFilesToTables().PermitUncheckedError();
      ExtractMetaData().PermitUncheckedError();
      status = AddTables();
    }
    return status;
  }

  Status DetectBlobIndirection() {
    // Detection may conservatively use an orphan; durable loss authority may
    // only use CURRENT's named complete units. Never union alternate images.
    for (const std::string& manifest : manifests_) {
      std::unique_ptr<SequentialFileReader> file;
      Status status = SequentialFileReader::Create(
          env_->GetFileSystem(), dbname_ + "/" + manifest, file_options_, &file,
          nullptr, nullptr);
      if (!status.ok()) {
        continue;
      }
      RepairLossLogReporter reporter(&status);
      log::Reader reader(nullptr, std::move(file), &reporter, true, 0);
      Slice record;
      std::string scratch;
      while (reader.ReadRecord(&record, &scratch)) {
        VersionEdit edit;
        Status decoded = edit.DecodeFrom(record);
        if (!decoded.ok()) {
          continue;
        }
        feature_repair_ |= edit.HasBlobIndirectionFormatVersion() ||
                           edit.HasDbBlobIndirectionFormatVersion() ||
                           !edit.GetBlobRelocationFileAdditions().empty() ||
                           !edit.GetBlobRelocationFileLosses().empty();
        if (edit.HasNextFile()) {
          status = TrackDiscoveredFileNumber(edit.GetNextFile());
          if (!status.ok()) {
            return status;
          }
        }
        for (const BlobRelocationRoute& route :
             edit.GetBlobRelocationRoutes()) {
          status = TrackDiscoveredFileNumber(route.GetGenerationFileNumber());
          if (status.ok()) {
            status = TrackDiscoveredFileNumber(route.GetOriginFileNumber());
          }
          if (!status.ok()) {
            return status;
          }
        }
      }
      // An optional alternate-image read is only detection, not authority.
      status.PermitUncheckedError();
    }
    for (const FileDescriptor& fd : table_fds_) {
      FileMetaData meta;
      meta.fd = fd;
      uint64_t size = 0;
      Status size_status = env_->GetFileSize(
          TableFileName(db_options_.db_paths, fd.GetNumber(), fd.GetPathId()),
          &size);
      if (!size_status.ok()) {
        continue;
      }
      meta.fd = FileDescriptor(fd.GetNumber(), fd.GetPathId(), size);
      std::shared_ptr<const TableProperties> properties;
      ReadOptions read_options;
      read_options.fill_cache = false;
      Status status = table_cache_->GetTableProperties(
          file_options_, read_options, icmp_, meta, &properties, default_mopts_,
          false, RepairTableOpenOptions());
      TEST_SYNC_POINT_CALLBACK(
          "Repairer::DetectBlobIndirection:TableProperties", &status);
      if (!status.ok()) {
        continue;
      }
      ExtractBlobReferenceDescriptorFromTableProperties(*properties, &meta);
      status = ExtractBlobReferenceClassificationFromTableProperties(
          *properties, &meta);
      if (!status.ok()) {
        if (feature_repair_) {
          return status;
        }
        continue;
      }
      // A decoded classification property, even a legacy one, is persisted
      // feature evidence. Runtime options and failed reads are not.
      const bool feature = !meta.blob_reference_descriptor.empty() ||
                           meta.oldest_coarse_identity_blob_file_number !=
                               kInvalidBlobFileNumber ||
                           meta.blob_reference_legacy_metadata;
      feature_repair_ |= feature;
      if (feature) {
        const ColumnFamilyOptions* options =
            GetColumnFamilyOptions(properties->column_family_name);
        if (options != nullptr) {
          status = CheckBlobIndirectionCompactionStyle(
              options->compaction_style, true);
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
    for (const BlobFilePath& file : blob_file_paths_) {
      std::unique_ptr<RandomAccessFileReader> reader;
      Status status = RandomAccessFileReader::Create(
          env_->GetFileSystem(), file.path, file_options_, &reader, nullptr);
      if (!status.ok()) {
        continue;
      }
      char scratch[BlobLogHeader::kSize];
      Slice prefix;
      status = reader->Read(0, sizeof(scratch), IOOptions(), &prefix, scratch,
                            nullptr);
      if (!status.ok()) {
        continue;
      }
      if (prefix.starts_with(Slice(kBlobRelocationFilePrefix,
                                   kBlobRelocationFilePrefixSize))) {
        feature_repair_ = true;
      } else {
        BlobLogHeader header;
        status = header.DecodeFrom(prefix);
        if (status.ok() && header.has_repair_metadata &&
            header.is_identity_root) {
          feature_repair_ = true;
        }
      }
    }
    return Status::OK();
  }

  Status AppendRepairCatalog(RepairCatalogRecord record) {
    if (repair_catalog_.last_ordinal == std::numeric_limits<uint64_t>::max()) {
      return Status::Corruption("Repair catalog ordinal overflow");
    }
    record.ordinal = repair_catalog_.last_ordinal + 1;
    std::string encoded;
    record.EncodeTo(&encoded);
    if (encoded.size() > log::kBlockSize) {
      return Status::NotSupported(
          "Repair catalog unit exceeds log block bound");
    }
    Status status = repair_catalog_log_->AddRecord(WriteOptions(), encoded);
    if (status.ok()) {
      bool previously_failed = false;
      status = repair_catalog_log_->file()->Sync(
          IOOptions(), immutable_db_options_.use_fsync, &previously_failed);
    }
    if (status.ok()) {
      repair_catalog_.last_ordinal = record.ordinal;
      if (record.type == RepairCatalogRecordType::kCoverage) {
        repair_catalog_.coverage_floor = record.number;
        repair_catalog_.coverage_max_sequence = record.size;
      }
    }
    return status;
  }

  Status StartRepairCatalog() {
    const std::string path = RepairInProgressFileName(dbname_);
    Status status = env_->FileExists(path);
    if (!status.IsNotFound() && !status.IsPathNotFound()) {
      return status.ok()
                 ? Status::Corruption("Repair catalog ownership conflict", path)
                 : status;
    }
    // Publish the catalog name only after its authority prefix is durable. A
    // crash before the rename leaves an ordinary temp file that open purges.
    // next_file_number_ is above every discovered number, including temps.
    const std::string temporary_path = TempFileName(dbname_, next_file_number_);
    std::unique_ptr<FSWritableFile> file;
    status = NewWritableFile(env_->GetFileSystem().get(), temporary_path, &file,
                             file_options_);
    if (!status.ok()) {
      return status;
    }
    repair_catalog_log_ =
        std::make_unique<log::Writer>(std::make_unique<WritableFileWriter>(
                                          std::move(file), path, file_options_),
                                      0, false);
    repair_catalog_.header.path = original_current_;
    repair_catalog_.header.identity = db_session_id_;
    repair_catalog_.header.column_family = original_current_absent_ ? 1 : 0;
    repair_catalog_.header.number = next_file_number_;
    repair_catalog_.header.source_wal =
        static_cast<uint64_t>(db_options_.two_write_queues) |
        (static_cast<uint64_t>(db_options_.unordered_write) << 1) |
        (static_cast<uint64_t>(create_unknown_cfs_) << 2);
    status = AppendRepairCatalog(repair_catalog_.header);
    if (!status.ok()) {
      return status;
    }
    if (!original_manifest_authority_.path.empty()) {
      status = AppendRepairCatalog(original_manifest_authority_);
      if (!status.ok()) {
        return status;
      }
      repair_catalog_.inputs.emplace(original_manifest_authority_.path,
                                     original_manifest_authority_);
    }
    TEST_SYNC_POINT_CALLBACK("Repairer::StartRepairCatalog:BeforeRename",
                             &status);
    if (!status.ok()) {
      return status;
    }
    status = env_->RenameFile(temporary_path, path);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<FSDirectory> directory;
    status = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(),
                                                 &directory, nullptr);
    if (status.ok()) {
      status = directory->FsyncWithDirOptions(IOOptions(), nullptr,
                                              DirFsyncOptions(path));
    }
    if (!status.ok()) {
      return status;
    }
    // Bind original input namespaces before outputs exist. Data-integrity
    // scans remain in the normal repair flow; catalog allocation is not input
    // deletion authority. Archive records get complete seals before publish.
    std::vector<std::string> inputs;
    for (const FileDescriptor& fd : table_fds_) {
      inputs.push_back(
          TableFileName(db_options_.db_paths, fd.GetNumber(), fd.GetPathId()));
    }
    for (const BlobFilePath& blob : blob_file_paths_) {
      inputs.push_back(blob.path);
    }
    for (const std::string& manifest : manifests_) {
      inputs.push_back(dbname_ + "/" + manifest);
    }
    for (uint64_t wal : logs_) {
      inputs.push_back(LogFileName(immutable_db_options_.GetWalDir(), wal));
    }
    std::sort(inputs.begin(), inputs.end());
    inputs.erase(std::unique(inputs.begin(), inputs.end()), inputs.end());
    for (const std::string& input : inputs) {
      if (repair_catalog_.inputs.count(input) != 0) {
        continue;
      }
      RepairCatalogRecord record;
      record.type = RepairCatalogRecordType::kInput;
      record.path = input;
      const size_t slash = input.find_last_of("/\\");
      FileType type;
      if (ParseFileName(
              input.substr(slash == std::string::npos ? 0 : slash + 1),
              &record.number, &type) &&
          type == kWalFile) {
        record.source_wal = record.number;
      }
      status = env_->GetFileSize(input, &record.size);
      if (!status.ok()) {
        return status;
      }
      status = AppendRepairCatalog(record);
      if (!status.ok()) {
        return status;
      }
      repair_catalog_.inputs.emplace(input, std::move(record));
    }
    return Status::OK();
  }

  Status RegisterRepairOutput(const std::string& path, uint64_t number,
                              uint32_t cf, uint64_t wal) {
    if (!feature_repair_) {
      return Status::OK();
    }
    Status status = env_->FileExists(path);
    if (!status.IsNotFound() && !status.IsPathNotFound()) {
      return status.ok()
                 ? Status::Corruption("Repair output already exists", path)
                 : status;
    }
    RepairCatalogRecord record;
    record.type = RepairCatalogRecordType::kIntent;
    record.path = path;
    record.number = number;
    record.column_family = cf;
    record.source_wal = wal;
    status = AppendRepairCatalog(record);
    if (status.ok()) {
      repair_catalog_.outputs.emplace(path, std::move(record));
    }
    return status;
  }

  Status SealRepairOutput(const std::string& path) {
    if (!feature_repair_) {
      return Status::OK();
    }
    const std::map<std::string, RepairCatalogRecord>::iterator intent =
        repair_catalog_.outputs.find(path);
    if (intent == repair_catalog_.outputs.end()) {
      return Status::Corruption(
          "Repair output has no durable allocation intent", path);
    }
    RepairCatalogRecord record = intent->second;
    record.type = RepairCatalogRecordType::kSeal;
    Status status = env_->GetFileSize(path, &record.size);
    if (status.ok()) {
      status = GetRepairFileChecksum(env_, path, record.size, &record.checksum);
    }
    if (status.ok()) {
      status = AppendRepairCatalog(record);
    }
    if (status.ok()) {
      intent->second = std::move(record);
    }
    return status;
  }

  Status RecordRepairArchive(const std::string& path) {
    if (repair_catalog_.archives.count(path) != 0) {
      return Status::OK();
    }
    RepairCatalogRecord record;
    record.type = RepairCatalogRecordType::kArchive;
    record.path = path;
    // A prior repair (or a later recreated DB) can have the same file number.
    // Attempt-specific archive names preserve those earlier bytes and make
    // retry cleanup independently idempotent.
    record.identity = repair_catalog_.header.identity;
    Status status = env_->GetFileSize(path, &record.size);
    if (status.ok()) {
      status = GetRepairFileChecksum(env_, path, record.size, &record.checksum);
    }
    if (status.ok()) {
      status = AppendRepairCatalog(record);
    }
    if (status.ok()) {
      repair_catalog_.archives.emplace(path, std::move(record));
    }
    return status;
  }

  Status PrepareOriginalReplayCoverage() {
    std::map<std::pair<uint32_t, uint64_t>, const TableInfo*> selected;
    for (const TableInfo& table : tables_) {
      if (!selected
               .emplace(std::make_pair(table.column_family_id,
                                       table.meta.fd.GetNumber()),
                        &table)
               .second) {
        return Status::Corruption("Duplicate original repair SST identity");
      }
    }
    for (const auto& name : original_cf_names_) {
      if (vset_.GetColumnFamilySet()->GetColumnFamily(name.first) == nullptr &&
          GetColumnFamilyOptions(name.second) != nullptr) {
        Status status = AddColumnFamily(name.second, name.first);
        if (!status.ok()) {
          return status;
        }
      }
    }
    for (ColumnFamilyData* cfd : *vset_.GetColumnFamilySet()) {
      bool complete = original_manifest_complete_;
      const std::map<uint64_t, FileMetaData>& expected =
          original_cf_tables_[cfd->GetID()];
      for (const auto& file : expected) {
        const std::map<std::pair<uint32_t, uint64_t>,
                       const TableInfo*>::const_iterator actual =
            selected.find({cfd->GetID(), file.first});
        if (actual == selected.end() ||
            actual->second->meta.fd.GetFileSize() !=
                file.second.fd.GetFileSize() ||
            (file.second.unique_id != kNullUniqueId64x2 &&
             actual->second->meta.unique_id != file.second.unique_id)) {
          complete = false;
          break;
        }
      }
      const uint64_t floor = complete ? original_cf_floors_[cfd->GetID()] : 0;
      proven_original_cf_floors_[cfd->GetID()] = floor;
      cfd->SetLogNumber(floor);
      RepairCatalogRecord policy;
      policy.type = RepairCatalogRecordType::kPolicy;
      policy.column_family = cfd->GetID();
      policy.path = cfd->GetName();
      policy.identity = cfd->user_comparator()->Name();
      policy.number = cfd->user_comparator()->timestamp_size();
      policy.source_wal = floor;
      Status policy_status = AppendRepairCatalog(policy);
      if (!policy_status.ok()) {
        return policy_status;
      }
      repair_catalog_.policies.emplace(cfd->GetID(), std::move(policy));
      if (floor != 0) {
        Status status = TrackDiscoveredFileNumber(floor);
        if (!status.ok()) {
          return status;
        }
      }
      if (!complete && !logs_.empty() &&
          durable_loss_routes_.count(cfd->GetID()) != 0) {
        return Status::Corruption(
            "Feature repair cannot certify WAL coverage of durable blob loss");
      }
    }
    return Status::OK();
  }

  Status ReadDurableBlobLoss() {
    std::string current;
    Status status = ReadFileToString(env_, CurrentFileName(dbname_), &current);
    if (status.IsNotFound() || status.IsPathNotFound()) {
      original_current_absent_ = true;
      return Status::OK();  // Explicit salvage without trustworthy history.
    }
    if (!status.ok()) {
      return status;
    }
    original_current_ = current;
    uint64_t manifest_number = 0;
    FileType type;
    if (current.empty() || current.back() != '\n') {
      return Status::OK();
    }
    current.pop_back();
    if (!ParseFileName(current, &manifest_number, &type) ||
        type != kDescriptorFile || manifest_number == 0 ||
        DescriptorFileName("", manifest_number) != "/" + current) {
      return Status::OK();
    }
    const std::string path = DescriptorFileName(dbname_, manifest_number);
    original_manifest_authority_.type = RepairCatalogRecordType::kInput;
    original_manifest_authority_.path = path;
    original_manifest_authority_.number = manifest_number;
    original_manifest_authority_.identity = "original-manifest";
    status = env_->GetFileSize(path, &original_manifest_authority_.size);
    if (status.IsNotFound() || status.IsPathNotFound()) {
      original_manifest_authority_.identity = "original-manifest-absent";
      return Status::OK();
    }
    if (!status.ok()) {
      return status;
    }
    status =
        GetRepairFileChecksum(env_, path, original_manifest_authority_.size,
                              &original_manifest_authority_.checksum);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<FSSequentialFile> file;
    status = immutable_db_options_.fs->NewSequentialFile(
        path, immutable_db_options_.fs->OptimizeForManifestRead(file_options_),
        &file, nullptr);
    if (status.IsNotFound() || status.IsPathNotFound()) {
      return Status::OK();
    }
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<SequentialFileReader> file_reader =
        std::make_unique<SequentialFileReader>(std::move(file), path,
                                               /*io_tracer=*/nullptr);
    Status log_status;
    RepairLossLogReporter reporter(&log_status);
    log::Reader reader(nullptr, std::move(file_reader), &reporter,
                       /*checksum=*/true, /*log_number=*/0);
    const ReadOptions read_options;
    RepairRelocationLossHandler handler(read_options);
    handler.Iterate(reader, &log_status);
    handler.DiscardIncompleteAtomicGroup();
    if (!handler.status().ok() && !handler.status().IsCorruption()) {
      return handler.status();
    }
    if (!log_status.ok() && !log_status.IsCorruption()) {
      return log_status;
    }
    original_manifest_complete_ = handler.status().ok() && log_status.ok();
    original_cf_floors_ = handler.cf_floors;
    original_cf_tables_ = handler.cf_tables;
    original_cf_names_ = handler.cf_names;
    feature_format_column_families_.insert(handler.feature_cfs.begin(),
                                           handler.feature_cfs.end());
    for (uint32_t cf : handler.feature_cfs) {
      const std::map<uint32_t, std::string>::const_iterator name =
          original_cf_names_.find(cf);
      if (name != original_cf_names_.end()) {
        const ColumnFamilyOptions* options =
            GetColumnFamilyOptions(name->second);
        if (options != nullptr) {
          status = CheckBlobIndirectionCompactionStyle(
              options->compaction_style, true);
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
    certified_max_sequence_ = handler.last_sequence;
    // A corrupt tail cannot make a previously committed deletion reversible.
    // The shared handler excludes incomplete atomic groups from this prefix.
    for (uint64_t number : handler.missing_files) {
      const auto file_info = handler.files.find(number);
      if (file_info != handler.files.end()) {
        durable_loss_files_.emplace(number, file_info->second);
      }
    }
    for (const auto& cf : handler.routes) {
      for (const auto& route : cf.second) {
        if (durable_loss_files_.count(route.second) != 0) {
          durable_loss_routes_[cf.first].emplace(route);
        }
      }
    }
    return Status::OK();
  }

  uint64_t GetDurableBlobLoss(uint32_t cf_id,
                              const BlobReference& reference) const {
    if (!reference.HasStableIdentity()) {
      return kInvalidBlobFileNumber;
    }
    uint64_t physical = reference.generation_file_number;
    const auto routes = durable_loss_routes_.find(cf_id);
    if (routes != durable_loss_routes_.end()) {
      const auto route = routes->second.find(
          {reference.generation_file_number, reference.origin_file_number});
      if (route != routes->second.end()) {
        physical = route->second;
      }
    }
    const auto file = durable_loss_files_.find(physical);
    return file != durable_loss_files_.end() &&
                   file->second.GetColumnFamilyId() == cf_id
               ? physical
               : kInvalidBlobFileNumber;
  }

  Status PublishManifest() {
    Status status = vset_.SyncAndCloseRepairManifest();
    if (!status.ok()) {
      return status;
    }
    const std::string final_path =
        DescriptorFileName(dbname_, repair_manifest_file_number_);
    TEST_SYNC_POINT("Repairer::PublishManifest:BeforeRename");
    status = env_->RenameFile(repair_manifest_path_, final_path);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<FSDirectory> directory;
    status = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(),
                                                 &directory, /*dbg=*/nullptr);
    if (!status.ok()) {
      return status;
    }
    status = directory->FsyncWithDirOptions(IOOptions(), /*dbg=*/nullptr,
                                            DirFsyncOptions(final_path));
    TEST_SYNC_POINT_CALLBACK("Repairer::PublishManifest:AfterDirectorySync",
                             &status);
    if (!status.ok()) {
      return status;
    }
    const WriteOptions write_options(Env::IOActivity::kDBOpen);
    status = SealRepairOutput(final_path);
    if (!status.ok()) {
      return status;
    }
    // Sealed allocation is not automatically candidate membership. Invalid
    // salvage/output files are removed only after authority settles.
    for (auto& output : repair_catalog_.outputs) {
      if (output.second.type != RepairCatalogRecordType::kSeal ||
          output.first == final_path) {
        continue;
      }
      bool used = false;
      for (const TableInfo& table : tables_) {
        used |= table.meta.fd.GetNumber() == output.second.number &&
                output.first == TableFileName(db_options_.db_paths,
                                              table.meta.fd.GetNumber(),
                                              table.meta.fd.GetPathId());
      }
      for (const auto& plan : blob_repair_plans_) {
        used |= plan.second.normal_files.count(output.second.number) != 0 ||
                plan.second.relocation_files.count(output.second.number) != 0;
      }
      if (!used) {
        output.second.identity = "unused";
        status = AppendRepairCatalog(output.second);
        if (!status.ok()) {
          return status;
        }
      }
    }
    for (const std::string& manifest : manifests_) {
      status = RecordRepairArchive(dbname_ + "/" + manifest);
      if (!status.ok()) {
        return status;
      }
    }
    for (const std::string& path : archive_after_publication_) {
      if (repair_catalog_.outputs.count(path) == 0) {
        status = RecordRepairArchive(path);
        if (!status.ok()) {
          return status;
        }
      }
    }
    repair_catalog_.candidate = repair_catalog_.outputs.at(final_path);
    repair_catalog_.candidate.type = RepairCatalogRecordType::kCandidate;
    repair_catalog_.candidate.source_wal = certified_wal_floor_;
    status = AppendRepairCatalog(repair_catalog_.candidate);
    if (!status.ok()) {
      return status;
    }
    status = repair_catalog_log_->Close(WriteOptions());
    repair_catalog_log_.reset();
    if (!status.ok()) {
      return status;
    }
    status = SetCurrentFile(write_options, env_->GetFileSystem().get(), dbname_,
                            repair_manifest_file_number_,
                            immutable_db_options_.metadata_write_temperature,
                            directory.get());
    if (status.ok()) {
      bool committed = false;
      status = ResolveRepairCatalog(env_, dbname_, repair_catalog_, false,
                                    false, &committed);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Repair image committed; cleanup incomplete: %s",
                       status.ToString().c_str());
      }
    }
    return status;
  }

  Status TrackDiscoveredFileNumber(uint64_t file_number) {
    if (file_number == std::numeric_limits<uint64_t>::max()) {
      return Status::Corruption("Invalid maximum repair file number");
    }
    next_file_number_ = std::max(next_file_number_, file_number + 1);
    return Status::OK();
  }

  Status AllocateRepairFileNumber(uint64_t* file_number) {
    if (file_number == nullptr) {
      return Status::InvalidArgument("Null repair file number output");
    }
    if (next_file_number_ == 0 ||
        next_file_number_ >= std::numeric_limits<uint64_t>::max() - 1) {
      return Status::Corruption("Repair file number space is exhausted");
    }
    *file_number = next_file_number_++;
    return Status::OK();
  }

  Status FindFiles() {
    std::vector<std::string> filenames;
    bool found_file = false;
    std::vector<std::string> to_search_paths;

    for (size_t path_id = 0; path_id < db_options_.db_paths.size(); path_id++) {
      to_search_paths.push_back(db_options_.db_paths[path_id].path);
    }

    // search wal_dir if user uses a customize wal_dir
    bool same = immutable_db_options_.IsWalDirSameAsDBPath(dbname_);
    if (!same) {
      to_search_paths.push_back(immutable_db_options_.wal_dir);
    }

    for (size_t path_id = 0; path_id < to_search_paths.size(); path_id++) {
      ROCKS_LOG_INFO(db_options_.info_log, "Searching path %s\n",
                     to_search_paths[path_id].c_str());
      Status status = env_->GetChildren(to_search_paths[path_id], &filenames);
      if (!status.ok()) {
        return status;
      }
      if (!filenames.empty()) {
        found_file = true;
      }

      uint64_t number;
      FileType type;
      for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
          status = TrackDiscoveredFileNumber(number);
          if (!status.ok()) {
            return status;
          }
          if (type == kDescriptorFile) {
            manifests_.push_back(filenames[i]);
          } else {
            if (type == kWalFile) {
              logs_.push_back(number);
            } else if (type == kTableFile) {
              table_fds_.emplace_back(number, static_cast<uint32_t>(path_id),
                                      0);
            } else if (type == kBlobFile) {
              blob_file_paths_.push_back(BlobFilePath{
                  number, BlobFileName(to_search_paths[path_id], number)});
            } else {
              // Ignore other files
            }
          }
        }
      }
    }
    if (!found_file) {
      return Status::Corruption(dbname_, "repair found no files");
    }
    return Status::OK();
  }

  Status FindFeatureColumnFamilyBlobFiles(bool detection_only) {
    std::set<std::string> known_paths;
    for (const BlobFilePath& file : blob_file_paths_) {
      known_paths.insert(file.path);
    }
    std::set<std::string> feature_directories;
    for (const DbPath& path : default_cf_opts_.cf_paths) {
      feature_directories.insert(path.path);
    }
    if (create_unknown_cfs_) {
      for (const DbPath& path : unknown_cf_opts_.cf_paths) {
        feature_directories.insert(path.path);
      }
    }
    for (const auto& configured_cf : cf_name_to_opts_) {
      for (const DbPath& path : configured_cf.second.cf_paths) {
        feature_directories.insert(path.path);
      }
    }
    for (ColumnFamilyData* cfd : *vset_.GetColumnFamilySet()) {
      if (feature_format_column_families_.count(cfd->GetID()) == 0 &&
          !cfd->GetLatestMutableCFOptions().enable_blob_indirection) {
        continue;
      }
      if (cfd->ioptions().cf_paths.empty()) {
        return Status::Corruption(
            "Blob-indirection column family has no canonical path");
      }
      for (const DbPath& path : cfd->ioptions().cf_paths) {
        feature_directories.insert(path.path);
      }
    }

    for (const std::string& directory : feature_directories) {
      std::vector<std::string> filenames;
      Status status = env_->GetChildren(directory, &filenames);
      if (!status.ok()) {
        if (detection_only) {
          continue;
        }
        return status;
      }
      for (const std::string& filename : filenames) {
        uint64_t number = 0;
        FileType type = kTempFile;
        if (!ParseFileName(filename, &number, &type) || type != kBlobFile) {
          continue;
        }
        status = TrackDiscoveredFileNumber(number);
        if (!status.ok()) {
          return status;
        }
        TEST_SYNC_POINT_CALLBACK(
            "Repairer::FindFeatureColumnFamilyBlobFiles:BlobFileNumber",
            &number);
        const std::string path = BlobFileName(directory, number);
        if (known_paths.insert(path).second) {
          if (repair_catalog_log_ != nullptr) {
            RepairCatalogRecord input;
            input.type = RepairCatalogRecordType::kInput;
            input.path = path;
            input.number = number;
            status = env_->GetFileSize(path, &input.size);
            if (status.ok()) {
              status = AppendRepairCatalog(input);
            }
            if (!status.ok()) {
              return status;
            }
            repair_catalog_.inputs.emplace(path, std::move(input));
          }
          blob_file_paths_.push_back(BlobFilePath{number, path});
        }
      }
    }
    return Status::OK();
  }

  Status ConvertLogFilesToTables() {
    const auto& wal_dir = immutable_db_options_.GetWalDir();
    if (feature_repair_) {
      std::sort(logs_.begin(), logs_.end());
      logs_.erase(std::unique(logs_.begin(), logs_.end()), logs_.end());
      // These policies can depend on sequence/transaction recovery context.
      // Keep the complete suffix for existing recovery, never a null-DB replay.
      if (db_options_.two_write_queues || db_options_.unordered_write) {
        deferred_wal_ = !logs_.empty();
        return Status::OK();
      }
    }
    for (size_t i = 0; i < logs_.size(); i++) {
      // we should use LogFileName(wal_dir, logs_[i]) here. user might uses
      // wal_dir option.
      std::string logname = LogFileName(wal_dir, logs_[i]);
      Status status = ConvertLogToTable(wal_dir, logs_[i]);
      if (feature_repair_) {
        if (!status.ok()) {
          return status;
        }
        if (deferred_wal_) {
          break;
        }
        if (logs_[i] == std::numeric_limits<uint64_t>::max()) {
          return Status::Corruption("Repair WAL floor overflow");
        }
        certified_wal_floor_ = logs_[i] + 1;
        RepairCatalogRecord coverage;
        coverage.type = RepairCatalogRecordType::kCoverage;
        coverage.number = certified_wal_floor_;
        coverage.size = certified_max_sequence_;
        status = AppendRepairCatalog(coverage);
        if (!status.ok()) {
          return status;
        }
        archive_after_publication_.push_back(logname);
        continue;
      }
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Log #%" PRIu64 ": ignoring conversion error: %s",
                       logs_[i], status.ToString().c_str());
      }
      ArchiveFile(logname);
    }
    return Status::OK();
  }

  Status ConvertLogToTable(const std::string& wal_dir, uint64_t log) {
    struct LogReporter : public log::Reader::Reporter {
      Env* env;
      std::shared_ptr<Logger> info_log;
      uint64_t lognum;
      Status read_error;
      ~LogReporter() override { read_error.PermitUncheckedError(); }
      void Corruption(size_t bytes, const Status& s,
                      uint64_t log_number = kMaxSequenceNumber) override {
        // We print error messages for corruption, but continue repairing.
        ROCKS_LOG_ERROR(info_log, "Log #%" PRIu64 ": dropping %d bytes; %s",
                        log_number == kMaxSequenceNumber ? lognum : log_number,
                        static_cast<int>(bytes), s.ToString().c_str());
        if (!s.IsCorruption()) {
          read_error = s;
        }
      }
    };

    // Open the log file
    std::string logname = LogFileName(wal_dir, log);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<SequentialFileReader> lfile_reader;
    Status status = SequentialFileReader::Create(
        fs, logname, fs->OptimizeForLogRead(file_options_), &lfile_reader,
        nullptr /* dbg */, nullptr /* rate limiter */);
    if (!status.ok()) {
      return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = db_options_.info_log;
    reporter.lognum = log;
    // We intentionally make log::Reader do checksumming so that
    // corruptions cause entire commits to be skipped instead of
    // propagating bad information (like overly large sequence
    // numbers).
    log::Reader reader(db_options_.info_log, std::move(lfile_reader), &reporter,
                       true /*enable checksum*/, log);

    // Initialize per-column family memtables
    for (auto* cfd : *vset_.GetColumnFamilySet()) {
      cfd->CreateNewMemtable(kMaxSequenceNumber);
    }
    std::unique_ptr<ColumnFamilyMemTablesImpl> cf_mems =
        std::make_unique<ColumnFamilyMemTablesImpl>(vset_.GetColumnFamilySet());

    // Read all the records and add to a memtable
    const UnorderedMap<uint32_t, size_t>& running_ts_sz =
        vset_.GetRunningColumnFamiliesTimestampSize();
    std::string scratch;
    Slice record;
    WriteBatch batch;

    int counter = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
        continue;
      }
      Status record_status = WriteBatchInternal::SetContents(&batch, record);
      if (feature_repair_ && record_status.ok()) {
        RepairBatchValidator validator(vset_.GetColumnFamilySet());
        record_status = batch.Iterate(&validator);
        if (record_status.ok() && validator.requires_context) {
          deferred_wal_ = true;
          // No output from this WAL exists yet. Its ordinary records and all
          // later WALs remain intact for the existing transaction recovery.
          return Status::OK();
        }
        if (!record_status.ok()) {
          if (!record_status.IsCorruption()) {
            return record_status;
          }
          reporter.Corruption(record.size(), record_status);
          continue;  // Complete decode precedes any memtable mutation.
        }
        if (validator.ignored_cf_operations != 0) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Repair discarded %" ROCKSDB_PRIszt
                         " WAL operations for excluded CFs",
                         validator.ignored_cf_operations);
        }
        const SequenceNumber sequence = WriteBatchInternal::Sequence(&batch);
        const uint64_t count = WriteBatchInternal::Count(&batch);
        if (sequence > kMaxSequenceNumber ||
            (count != 0 && count - 1 > kMaxSequenceNumber - sequence)) {
          return Status::Corruption("Repair batch sequence range overflow");
        }
        certified_max_sequence_ = std::max(
            certified_max_sequence_, sequence + (count != 0 ? count - 1 : 0));
      }
      if (record_status.ok()) {
        const UnorderedMap<uint32_t, size_t>& record_ts_sz =
            reader.GetRecordedTimestampSize();
        // Use same value for `seq_per_batch` and `batch_per_txn` as
        // WriteBatchInternal::InsertInto does below.
        record_status = HandleWriteBatchTimestampSizeDifference(
            &batch, running_ts_sz, record_ts_sz,
            TimestampSizeConsistencyMode::kVerifyConsistency,
            /* seq_per_batch */ false, /* batch_per_txn */ true);
        if (record_status.ok()) {
          record_status = WriteBatchInternal::InsertInto(
              &batch, cf_mems.get(), nullptr, nullptr,
              /*ignore_missing_column_families=*/feature_repair_,
              /*log_number=*/feature_repair_ ? log : 0);
        }
      }
      if (record_status.ok()) {
        counter += WriteBatchInternal::Count(&batch);
      } else {
        if (feature_repair_) {
          return record_status;
        }
        ROCKS_LOG_WARN(db_options_.info_log, "Log #%" PRIu64 ": ignoring %s",
                       log, record_status.ToString().c_str());
      }
    }
    if (feature_repair_ &&
        (reader.hasReadError() || !reporter.read_error.ok())) {
      return reporter.read_error.ok()
                 ? Status::IOError("Repair WAL read failed")
                 : reporter.read_error;
    }

    // Dump a table for each column family with entries in this log file.
    for (auto* cfd : *vset_.GetColumnFamilySet()) {
      // Do not record a version edit for this conversion to a Table
      // since ExtractMetaData() will also generate edits.
      MemTable* mem = cfd->mem();
      if (mem->IsEmpty()) {
        continue;
      }

      FileMetaData meta;
      uint64_t file_number = 0;
      status = AllocateRepairFileNumber(&file_number);
      if (!status.ok()) {
        break;
      }
      TEST_SYNC_POINT_CALLBACK("Repairer::ConvertLogToTable:TableFileNumber",
                               &file_number);
      meta.fd = FileDescriptor(file_number, 0, 0);
      if (feature_repair_) {
        status = RegisterRepairOutput(
            TableFileName(db_options_.db_paths, file_number, 0), file_number,
            cfd->GetID(), log);
        if (!status.ok()) {
          break;
        }
      }
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions ro;
      ro.total_order_seek = true;
      Arena arena;
      ScopedArenaPtr<InternalIterator> iter(
          mem->NewIterator(ro, /*seqno_to_time_mapping=*/nullptr, &arena,
                           /*prefix_extractor=*/nullptr, /*for_flush=*/true));
      int64_t _current_time = 0;
      immutable_db_options_.clock->GetCurrentTime(&_current_time)
          .PermitUncheckedError();  // ignore error
      const uint64_t current_time = static_cast<uint64_t>(_current_time);
      meta.file_creation_time = current_time;
      SnapshotChecker* snapshot_checker = DisableGCSnapshotChecker::Instance();

      auto write_hint = cfd->current()->storage_info()->CalculateSSTWriteHint(
          /*level=*/0, db_options_.calculate_sst_write_lifetime_hint_set);

      std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
          range_del_iters;
      auto range_del_iter = mem->NewRangeTombstoneIterator(
          ro, kMaxSequenceNumber, false /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }

      IOStatus io_s;
      CompressionOptions default_compression;
      // TODO: plumb Env::IOActivity, Env::IOPriority
      const ReadOptions read_options;
      const WriteOptions write_option(Env::IO_HIGH);
      TableBuilderOptions tboptions(
          cfd->ioptions(), cfd->GetLatestMutableCFOptions(), read_options,
          write_option, cfd->internal_comparator(),
          cfd->internal_tbl_prop_coll_factories(), kNoCompression,
          default_compression, cfd->GetID(), cfd->GetName(), -1 /* level */,
          current_time /* newest_key_time */, false /* is_bottommost */,
          TableFileCreationReason::kRecovery, 0 /* oldest_key_time */,
          0 /* file_creation_time */, "DB Repairer" /* db_id */, db_session_id_,
          0 /*target_file_size*/, meta.fd.GetNumber(),
          kMaxSequenceNumber /*last_level_inclusive_max_seqno_threshold*/,
          dbname_ /*db_name*/);
      // Legacy salvage output stays legacy. It has no format fence, so exact
      // metadata would fail publication and become false feature evidence.
      tboptions.collect_blob_reference_metadata =
          feature_repair_ &&
          (cfd->GetLatestMutableCFOptions().enable_blob_indirection ||
           feature_format_column_families_.find(cfd->GetID()) !=
               feature_format_column_families_.end());

      SeqnoToTimeMapping empty_seqno_to_time_mapping;
      status = BuildTable(
          dbname_, /* versions */ nullptr, immutable_db_options_, tboptions,
          file_options_, table_cache_.get(), iter.get(),
          std::move(range_del_iters), &meta, nullptr /* blob_file_additions */,
          {}, kMaxSequenceNumber, kMaxSequenceNumber, kMaxSequenceNumber,
          snapshot_checker, false /* paranoid_file_checks*/,
          nullptr /* internal_stats */, &io_s, nullptr /*IOTracer*/,
          BlobFileCreationReason::kRecovery,
          nullptr /* seqno_to_time_mapping */, nullptr /* event_logger */,
          0 /* job_id */, nullptr /* table_properties */, write_hint);
      if (!io_s.ok() && status.ok()) {
        status = io_s;
      }
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Log #%" PRIu64 ": %d ops saved to Table #%" PRIu64 " %s",
                     log, counter, meta.fd.GetNumber(),
                     status.ToString().c_str());
      if (status.ok()) {
        if (meta.fd.GetFileSize() > 0) {
          status = SealRepairOutput(
              TableFileName(db_options_.db_paths, file_number, 0));
          if (!status.ok()) {
            break;
          }
          table_fds_.push_back(meta.fd);
        }
      } else {
        break;
      }
    }
    return status;
  }

  Status ExtractMetaData() {
    for (size_t i = 0; i < table_fds_.size(); i++) {
      TableInfo t;
      t.meta.fd = table_fds_[i];
      bool repair_metadata_error = false;
      Status status = ScanTable(&t, &repair_metadata_error);
      if (!status.ok()) {
        // Legacy salvage keeps base behavior: set one bad table aside and
        // continue. Its caller cannot stop after inputs were archived.
        if (repair_metadata_error && feature_repair_) {
          return status;
        }
        std::string fname = TableFileName(
            db_options_.db_paths, t.meta.fd.GetNumber(), t.meta.fd.GetPathId());
        if (feature_repair_ && (!status.IsCorruption() ||
                                repair_catalog_.outputs.count(fname) != 0)) {
          // A certified WAL output cannot be dropped after its source WAL's
          // coverage was advanced. Ambiguous original-file I/O is not absence.
          return status;
        }
        char file_num_buf[kFormatFileNumberBufSize];
        FormatFileNumber(t.meta.fd.GetNumber(), t.meta.fd.GetPathId(),
                         file_num_buf, sizeof(file_num_buf));
        ROCKS_LOG_WARN(db_options_.info_log, "Table #%s: ignoring %s",
                       file_num_buf, status.ToString().c_str());
        // Keep the pre-repair image discoverable until CURRENT commits the
        // complete replacement, even for inputs repair elects not to salvage.
        if (feature_repair_) {
          archive_after_publication_.push_back(fname);
        } else {
          ArchiveFile(fname);
        }
      } else {
        // A legacy classification is feature evidence for its CF, so the
        // repaired MANIFEST must fence that CF.
        if (!t.meta.blob_reference_descriptor.empty() ||
            t.meta.blob_reference_legacy_metadata) {
          feature_format_column_families_.insert(t.column_family_id);
        }
        tables_.push_back(t);
      }
    }
    return Status::OK();
  }

  Status CollectLegacyBlobReferences(const ParsedInternalKey& parsed,
                                     const Slice& value,
                                     bool has_exact_blob_references,
                                     bool* repair_metadata_error,
                                     TableInfo* table) {
    assert(repair_metadata_error != nullptr);
    assert(table != nullptr);
    if (parsed.type != kTypeBlobIndex && parsed.type != kTypeWideColumnEntity) {
      return Status::OK();
    }

    const auto collect_reference = [&](const BlobIndex& blob_index) -> Status {
      BlobReference reference;
      bool included = false;
      Status status =
          BlobReference::FromBlobIndex(blob_index, &reference, &included);
      if (!status.ok() || !included) {
        return status;
      }
      table->reference_sources.emplace(reference.encoding_class,
                                       reference.generation_file_number,
                                       reference.origin_file_number);
      if (reference.HasStableIdentity()) {
        if (has_exact_blob_references) {
          return Status::OK();
        }
        const uint64_t frontier =
            table->meta.oldest_coarse_identity_blob_file_number;
        if (frontier == kInvalidBlobFileNumber ||
            reference.encoding_class !=
                BlobReferenceEncodingClass::kFeatureIdentity ||
            reference.origin_file_number < frontier) {
          *repair_metadata_error = true;
          return Status::Corruption(
              "Feature BlobIndex is missing exact/coarse SST metadata");
        }
      }
      return Status::OK();
    };

    if (parsed.type == kTypeBlobIndex) {
      BlobIndex blob_index;
      Status status = blob_index.DecodeFrom(value);
      return status.ok() ? collect_reference(blob_index) : status;
    }
    return WideColumnSerialization::ForEachBlobIndex(value, collect_reference);
  }

  Status ScanTable(TableInfo* t, bool* repair_metadata_error) {
    assert(t != nullptr);
    assert(repair_metadata_error != nullptr);
    *repair_metadata_error = false;
    std::string fname = TableFileName(
        db_options_.db_paths, t->meta.fd.GetNumber(), t->meta.fd.GetPathId());
    int counter = 0;
    bool has_exact_blob_references = false;
    uint64_t file_size;
    Status status = env_->GetFileSize(fname, &file_size);
    ColumnFamilyData* cfd = nullptr;
    TableReader* scan_table_reader = nullptr;
    std::unique_ptr<InternalIterator> data_iterator;
    t->meta.fd = FileDescriptor(t->meta.fd.GetNumber(), t->meta.fd.GetPathId(),
                                file_size);
    std::shared_ptr<const TableProperties> props;
    if (status.ok()) {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions read_options;
      read_options.fill_cache = false;
      read_options.verify_checksums = true;
      status = table_cache_->GetTableProperties(
          file_options_, read_options, icmp_, t->meta, &props, default_mopts_,
          /*no_io=*/false, RepairTableOpenOptions());
    }
    if (status.ok()) {
      auto s =
          GetSstInternalUniqueId(props->db_id, props->db_session_id,
                                 props->orig_file_number, &t->meta.unique_id);
      if (!s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Table #%" PRIu64
                       ": unable to get unique id, default to Unknown.",
                       t->meta.fd.GetNumber());
      }
      ExtractBlobReferenceDescriptorFromTableProperties(*props, &t->meta);
      status = ExtractBlobReferenceClassificationFromTableProperties(*props,
                                                                     &t->meta);
      if (!status.ok()) {
        *repair_metadata_error = true;
      }
      has_exact_blob_references = !t->meta.blob_reference_descriptor.empty();
      uint64_t raw_column_family_id = props->column_family_id;
      TEST_SYNC_POINT_CALLBACK("Repairer::ScanTable:ColumnFamilyId",
                               &raw_column_family_id);
      if ((has_exact_blob_references ||
           props->user_collected_properties.count(
               kBlobReferenceClassificationPropertyName) != 0) &&
          (raw_column_family_id > std::numeric_limits<uint32_t>::max() ||
           raw_column_family_id == TablePropertiesCollectorFactory::Context::
                                       kUnknownColumnFamily)) {
        *repair_metadata_error = true;
        status = Status::Corruption(
            "SST blob repair metadata has an invalid column family");
      }
      if (status.ok()) {
        t->column_family_id = static_cast<uint32_t>(raw_column_family_id);
        if (t->column_family_id ==
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) {
          ROCKS_LOG_WARN(
              db_options_.info_log,
              "Table #%" PRIu64
              ": column family unknown (probably due to legacy format); "
              "adding to default column family id 0.",
              t->meta.fd.GetNumber());
          t->column_family_id = 0;
        }

        if (vset_.GetColumnFamilySet()->GetColumnFamily(t->column_family_id) ==
            nullptr) {
          status =
              AddColumnFamily(props->column_family_name, t->column_family_id);
          if (!status.ok() &&
              GetColumnFamilyOptions(props->column_family_name) != nullptr) {
            // An unknown CF retains normal salvage behavior. Failure to
            // commit a known CF is fatal: the staged writer may be unusable.
            *repair_metadata_error = true;
          }
        }
      }
      t->meta.oldest_ancestor_time = props->oldest_ancestor_time;
      t->meta.user_defined_timestamps_persisted =
          static_cast<bool>(props->user_defined_timestamps_persisted);
    }
    if (status.ok() && !feature_repair_ &&
        (has_exact_blob_references || t->meta.blob_reference_legacy_metadata ||
         t->meta.oldest_coarse_identity_blob_file_number !=
             kInvalidBlobFileNumber)) {
      // Detection missed this feature table. Legacy output has no format
      // fence, so archive it like any other table repair cannot use.
      status = Status::Corruption("Feature SST found during legacy repair");
    }
    if (status.ok()) {
      if (t->meta.oldest_coarse_identity_blob_file_number !=
          kInvalidBlobFileNumber) {
        feature_format_column_families_.insert(t->column_family_id);
      }
      if (has_exact_blob_references) {
        BlobReferenceMetadataDescriptor descriptor;
        status = BlobReferenceMetadataDescriptor::DecodeFrom(
            t->meta.blob_reference_descriptor, &descriptor);
        if (!status.ok()) {
          *repair_metadata_error = true;
        } else {
          TEST_SYNC_POINT_CALLBACK(
              "Repairer::ScanTable:BlobReferenceMetadataDescriptor",
              &descriptor);
        }
        if (status.ok() &&
            (!descriptor.HasRepairMetadata() ||
             descriptor.column_family_id != t->column_family_id ||
             descriptor.sst_file_number != props->orig_file_number)) {
          *repair_metadata_error = true;
          status = Status::Corruption(
              "SST has incomplete or mismatched blob repair metadata");
        }
      } else {
        // With the MANIFEST unavailable, a descriptorless readable SST is the
        // durable pre-activation form. Its BlobIndexes are inventoried below.
        // After every SST has been scanned, activated column families mark
        // these files explicitly so the repaired format fence can accept
        // them. A database that was never activated must not gain the fence.
      }
    }
    if (status.ok()) {
      uint64_t tail_size = FileMetaData::CalculateTailSize(file_size, *props);
      t->meta.tail_size = tail_size;
    }
    if (status.ok()) {
      cfd = vset_.GetColumnFamilySet()->GetColumnFamily(t->column_family_id);
      if (cfd->GetName() != props->column_family_name) {
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "Table #%" PRIu64
            ": inconsistent column family name '%s'; expected '%s' for column "
            "family id %" PRIu32 ".",
            t->meta.fd.GetNumber(), props->column_family_name.c_str(),
            cfd->GetName().c_str(), t->column_family_id);
        status = Status::Corruption(dbname_, "inconsistent column family name");
      }
    }
    if (status.ok()) {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions ropts;
      ropts.total_order_seek = true;
      ropts.fill_cache = false;
      ropts.verify_checksums = true;
      data_iterator.reset(table_cache_->NewIterator(
          ropts, file_options_, cfd->internal_comparator(), t->meta,
          nullptr /* range_del_agg */, cfd->GetLatestMutableCFOptions(),
          &scan_table_reader, /*file_read_hist=*/nullptr,
          TableReaderCaller::kRepair, /*arena=*/nullptr, /*skip_filters=*/false,
          /*level=*/-1, /*max_file_size_for_l0_meta_pin=*/0,
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key=*/nullptr,
          /*allow_unprepared_value=*/false,
          /*range_del_read_seqno=*/nullptr, /*range_del_iter=*/nullptr,
          /*maybe_pin_table_handle=*/false, /*file_open_metadata=*/nullptr,
          RepairTableOpenOptions()));
      status = data_iterator->status();
      if (status.ok() && scan_table_reader == nullptr) {
        status = Status::Corruption("Missing repair table reader");
      }
      if (status.ok() && has_exact_blob_references) {
        status = scan_table_reader->VerifyBlobReferenceMetadata(
            ropts, t->meta.blob_reference_descriptor);
        if (status.ok()) {
          status = cfd->table_cache()->ReadBlobReferenceSstSummary(
              *scan_table_reader, t->meta, cfd->GetLatestMutableCFOptions(),
              &t->meta.blob_reference_summary);
        }
        if (!status.ok()) {
          *repair_metadata_error = true;
        }
      }
      ParsedInternalKey parsed;
      for (data_iterator->SeekToFirst(); status.ok() && data_iterator->Valid();
           data_iterator->Next()) {
        Slice key = data_iterator->key();
        Status pik_status =
            ParseInternalKey(key, &parsed, db_options_.allow_data_in_errors);
        if (!pik_status.ok()) {
          ROCKS_LOG_ERROR(db_options_.info_log,
                          "Table #%" PRIu64 ": unparsable key - %s",
                          t->meta.fd.GetNumber(), pik_status.getState());
          continue;
        }

        counter++;

        status = CollectLegacyBlobReferences(parsed, data_iterator->value(),
                                             has_exact_blob_references,
                                             repair_metadata_error, t);
        if (!status.ok()) {
          break;
        }

        status = t->meta.UpdateBoundaries(key, data_iterator->value(),
                                          parsed.sequence, parsed.type);
        if (!status.ok()) {
          break;
        }
      }
      if (status.ok() && !data_iterator->status().ok()) {
        status = data_iterator->status();
      }
      ROCKS_LOG_INFO(db_options_.info_log, "Table #%" PRIu64 ": %d entries %s",
                     t->meta.fd.GetNumber(), counter,
                     status.ToString().c_str());
    }
    if (status.ok()) {
      // XXX/FIXME: This is just basic, naive handling of range tombstones,
      // like call to UpdateBoundariesForRange in builder.cc where we assume
      // an SST file is a full sorted run. This probably needs the extra logic
      // from compaction_job.cc around call to UpdateBoundariesForRange (to
      // handle range tombstones extendingg beyond range of other entries).
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions ropts;
      ropts.fill_cache = false;
      ropts.verify_checksums = true;
      std::unique_ptr<FragmentedRangeTombstoneIterator> r_iter;
      assert(scan_table_reader != nullptr);
      r_iter.reset(static_cast<FragmentedRangeTombstoneIterator*>(
          scan_table_reader->NewRangeTombstoneIterator(ropts)));

      if (r_iter) {
        status = r_iter->status();
        r_iter->SeekToFirst();

        while (status.ok() && r_iter->Valid()) {
          auto tombstone = r_iter->Tombstone();
          auto kv = tombstone.Serialize();
          t->meta.UpdateBoundariesForRange(
              kv.first, tombstone.SerializeEndKey(), tombstone.seq_,
              cfd->internal_comparator());
          r_iter->Next();
        }
      }
    }
    return status;
  }

  Status GetRelocationTableOptions(
      const ColumnFamilyData& cfd,
      BlockBasedTableOptions* table_options) const {
    assert(table_options != nullptr);
    const TableFactory* const table_factory =
        cfd.GetLatestMutableCFOptions().table_factory.get();
    if (table_factory == nullptr) {
      return Status::NotSupported(
          "Blob relocation repair requires a block-based table factory");
    }
    const BlockBasedTableOptions* options =
        table_factory->GetOptions<BlockBasedTableOptions>();
    const AdaptiveTableFactory* const adaptive =
        table_factory->CheckedCast<AdaptiveTableFactory>();
    if (adaptive != nullptr) {
      options = adaptive->GetBlockBasedTableWriterOptions();
    }
    if (options == nullptr) {
      return Status::NotSupported(
          "Blob relocation repair requires a block-based table factory");
    }
    *table_options = *options;
    return Status::OK();
  }

  Status ScanNormalBlobFile(const BlobFilePath& file,
                            const std::set<uint64_t>& required_origins,
                            NormalBlobFileInfo* info,
                            bool* belongs_to_active_column_family) {
    *belongs_to_active_column_family = false;
    uint64_t file_size = 0;
    Status status = env_->GetFileSize(file.path, &file_size);
    if (!status.ok()) {
      return status;
    }
    if (file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
      return Status::Corruption("Blob file is too small");
    }
    std::unique_ptr<RandomAccessFileReader> file_reader;
    status = RandomAccessFileReader::Create(
        env_->GetFileSystem(), file.path, file_options_, &file_reader, nullptr);
    if (!status.ok()) {
      return status;
    }
    BlobLogSequentialReader reader(std::move(file_reader),
                                   immutable_db_options_.clock,
                                   immutable_db_options_.stats);
    BlobLogHeader header;
    status = reader.ReadHeader(&header);
    if (!status.ok()) {
      return status;
    }
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(header.column_family_id);
    if (header.has_ttl || header.expiration_range != ExpirationRange{} ||
        cfd == nullptr || cfd->ioptions().cf_paths.empty() ||
        (header.has_repair_metadata &&
         header.repair_file_number != file.file_number)) {
      return Status::Corruption("Invalid normal blob repair header");
    }
    if (feature_format_column_families_.count(header.column_family_id) == 0 ||
        required_origins.count(file.file_number) == 0) {
      return Status::OK();
    }
    *belongs_to_active_column_family = true;
    uint64_t blob_count = 0;
    uint64_t blob_bytes = 0;
    while (reader.GetNextByte() < file_size - BlobLogFooter::kSize) {
      BlobLogRecord record;
      uint64_t offset = 0;
      TEST_SYNC_POINT("Repairer::ScanNormalBlobFile:ReadRecord");
      status = reader.ReadRecordHeader(&record, &offset,
                                       file_size - BlobLogFooter::kSize);
      if (status.ok()) {
        // Inventory only framing. Selected requirements are validated by the
        // ordered record cursor, without retaining a map of every offset/key.
        status = reader.ReadRecordPayload(&record,
                                          BlobLogSequentialReader::kReadHeader);
      }
      if (!status.ok()) {
        return status;
      }
      if (record.record_size() >
          std::numeric_limits<uint64_t>::max() - blob_bytes) {
        return Status::Corruption("Blob record byte total overflow");
      }
      blob_bytes += record.record_size();
      ++blob_count;
    }
    if (reader.GetNextByte() != file_size - BlobLogFooter::kSize) {
      return Status::Corruption("Blob records do not align with footer");
    }
    BlobLogFooter footer;
    status = reader.ReadFooter(&footer);
    if (!status.ok()) {
      return status;
    }
    const ExpirationRange expected =
        header.has_repair_metadata
            ? ExpirationRange{blob_bytes, header.GetRepairMetadataFooterValue()}
            : ExpirationRange{};
    if (reader.GetNextByte() != file_size || footer.blob_count != blob_count ||
        footer.expiration_range != expected) {
      return Status::Corruption("Blob footer does not match file contents");
    }
    info->addition = BlobFileAddition(file.file_number, blob_count, blob_bytes,
                                      "", "", header.is_identity_root);
    info->column_family_id = header.column_family_id;
    info->file_size = file_size;
    info->compression = header.compression;
    info->path = file.path;
    info->is_canonical =
        file.path ==
        BlobFileName(cfd->ioptions().cf_paths.front().path, file.file_number);
    return Status::OK();
  }

  Status CopyNormalBlobFileToCanonical(uint64_t file_number,
                                       const NormalBlobFileInfo& file) {
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(file.column_family_id);
    if (cfd == nullptr || cfd->ioptions().cf_paths.empty()) {
      return Status::Corruption("Missing canonical blob file path");
    }
    const std::string& output_directory = cfd->ioptions().cf_paths.front().path;
    const std::string output_path = BlobFileName(output_directory, file_number);
    const std::string temporary_path =
        TempFileName(output_directory, file_number);
    if (file.path == output_path) {
      return Status::InvalidArgument("Blob file is already canonical");
    }

    // Repair holds the DB lock, but rename can still replace an existing
    // destination. Never overwrite pre-repair bytes before CURRENT commits.
    // Restoring an absent canonical file is safe; replacing an existing one
    // would require a separate crash-safe backup/restore protocol.
    Status status = env_->FileExists(output_path);
    if (status.ok()) {
      return Status::Corruption(
          "Repair refuses to replace an existing canonical blob file",
          output_path);
    }
    if (!status.IsNotFound()) {
      return status;
    }
    status = RegisterRepairOutput(temporary_path, file_number, cfd->GetID(), 0);
    if (status.ok()) {
      status = RegisterRepairOutput(output_path, file_number, cfd->GetID(), 0);
    }
    if (!status.ok()) {
      return status;
    }
    status = CopyFile(env_->GetFileSystem(), file.path, Temperature::kUnknown,
                      temporary_path, Temperature::kUnknown, file.file_size,
                      db_options_.use_fsync,
                      /*io_tracer=*/nullptr);
    if (status.ok()) {
      status = env_->RenameFile(temporary_path, output_path);
    }
    if (status.ok()) {
      std::unique_ptr<FSDirectory> directory;
      status = env_->GetFileSystem()->NewDirectory(
          output_directory, IOOptions(), &directory, /*dbg=*/nullptr);
      if (status.ok()) {
        status = directory->FsyncWithDirOptions(
            IOOptions(), /*dbg=*/nullptr,
            DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      }
    }
    if (!status.ok()) {
      env_->DeleteFile(temporary_path).PermitUncheckedError();
    }
    if (status.ok()) {
      status = SealRepairOutput(output_path);
    }
    return status;
  }

  Status DiscoverBlobFiles(const std::set<uint64_t>& required_normal_records,
                           NormalBlobFileCandidates* normal_files,
                           RelocationBlobFileCandidates* relocation_files) {
    assert(normal_files != nullptr);
    assert(relocation_files != nullptr);
    for (const BlobFilePath& file : blob_file_paths_) {
      if (durable_loss_files_.count(file.file_number) != 0) {
        continue;  // A restored physical donor cannot undo committed loss.
      }
      uint64_t file_size = 0;
      Status status = env_->GetFileSize(file.path, &file_size);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Blob file #%" PRIu64 ": ignoring %s", file.file_number,
                       status.ToString().c_str());
        continue;
      }
      bool is_relocation = false;
      if (file_size >= kBlobRelocationFilePrefixSize) {
        std::unique_ptr<RandomAccessFileReader> prefix_reader;
        status = RandomAccessFileReader::Create(
            env_->GetFileSystem(), file.path, file_options_, &prefix_reader,
            /*dbg=*/nullptr);
        if (status.ok()) {
          char scratch[kBlobRelocationFilePrefixSize];
          Slice prefix;
          status = prefix_reader->Read(0, kBlobRelocationFilePrefixSize,
                                       IOOptions(), &prefix, scratch,
                                       /*dbg=*/nullptr);
          is_relocation = status.ok() &&
                          prefix.size() == kBlobRelocationFilePrefixSize &&
                          prefix == Slice(kBlobRelocationFilePrefix,
                                          kBlobRelocationFilePrefixSize);
        }
      }

      if (!is_relocation) {
        NormalBlobFileInfo info;
        bool belongs_to_active_column_family = false;
        status = ScanNormalBlobFile(file, required_normal_records, &info,
                                    &belongs_to_active_column_family);
        if (status.ok() && belongs_to_active_column_family) {
          normal_files->emplace(file.file_number, std::move(info));
        }
      } else {
        std::unique_ptr<BlobFileReader> reader;
        status = Status::Corruption(
            "Relocation file does not match a recovered column family");
        Status external_status;
        external_status.PermitUncheckedOk();
        for (ColumnFamilyData* cfd : *vset_.GetColumnFamilySet()) {
          if (feature_format_column_families_.count(cfd->GetID()) == 0) {
            continue;
          }
          BlockBasedTableOptions table_options;
          Status candidate_status =
              GetRelocationTableOptions(*cfd, &table_options);
          if (!candidate_status.ok()) {
            candidate_status.PermitUncheckedError();
            continue;
          }
          const MutableCFOptions& mutable_options =
              cfd->GetLatestMutableCFOptions();
          std::unique_ptr<BlobFileReader> candidate;
          candidate_status = BlobFileReader::CreateRelocationFileForRepair(
              cfd->ioptions(), ReadOptions(), file_options_,
              /*blob_file_read_hist=*/nullptr, file.file_number, file_size,
              file.path, db_session_id_, table_options,
              mutable_options.compression_manager.get(),
              mutable_options.block_protection_bytes_per_key,
              /*io_tracer=*/nullptr, &candidate);
          if (candidate_status.ok() &&
              candidate->GetRelocationColumnFamilyId() == cfd->GetID()) {
            reader = std::move(candidate);
            status.PermitUncheckedError();
            status = Status::OK();
            break;
          }
          if (!candidate_status.ok() && !candidate_status.IsCorruption() &&
              !candidate_status.IsNotSupported() && external_status.ok()) {
            external_status = candidate_status;
          }
          candidate_status.PermitUncheckedError();
        }
        if (!status.ok() && !external_status.ok()) {
          status = std::move(external_status);
        } else {
          external_status.PermitUncheckedError();
        }
        const BlobRelocationDirectory* directory =
            status.ok() ? reader->GetRelocationDirectory() : nullptr;
        if (status.ok() && directory == nullptr) {
          status = Status::Corruption("Missing blob relocation directory");
        }
        if (status.ok()) {
          RelocationBlobFileInfo info;
          info.column_family_id = reader->GetRelocationColumnFamilyId();
          info.file_size = file_size;
          info.path = file.path;
          ColumnFamilyData* const owner =
              vset_.GetColumnFamilySet()->GetColumnFamily(
                  info.column_family_id);
          info.is_canonical =
              owner != nullptr && !owner->ioptions().cf_paths.empty() &&
              file.path == BlobFileName(owner->ioptions().cf_paths.front().path,
                                        file.file_number);
          if (directory->HasRepairMetadata()) {
            std::string encoded_directory;
            directory->EncodeTo(&encoded_directory);
            info.addition = BlobRelocationFileAddition(
                file.file_number, info.column_family_id, file_size,
                /*checksum_method=*/"", /*checksum_value=*/"",
                std::move(encoded_directory), reader->GetRelocationTailSize());
          }
          info.directory = *directory;
          relocation_files->emplace(file.file_number, std::move(info));
        }
      }
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Blob file #%" PRIu64 ": ignoring invalid candidate: %s",
                       file.file_number, status.ToString().c_str());
        if (!status.IsCorruption() && !status.IsNotSupported()) {
          return status;
        }
      }
    }
    return Status::OK();
  }

  static bool SameBlobReference(const BlobReference& lhs,
                                const BlobReference& rhs) {
    return lhs.encoding_class == rhs.encoding_class &&
           lhs.generation_file_number == rhs.generation_file_number &&
           lhs.origin_file_number == rhs.origin_file_number &&
           lhs.original_value_offset == rhs.original_value_offset &&
           lhs.stored_value_size == rhs.stored_value_size &&
           lhs.record_bytes == rhs.record_bytes &&
           lhs.compression == rhs.compression &&
           lhs.stable_checksum_version == rhs.stable_checksum_version &&
           lhs.stable_checksum == rhs.stable_checksum &&
           lhs.stored_checksum == rhs.stored_checksum;
  }

  static bool SameStableBlobMetadata(const BlobReference& lhs,
                                     const BlobReference& rhs) {
    return lhs.HasStableIdentity() && rhs.HasStableIdentity() &&
           lhs.origin_file_number == rhs.origin_file_number &&
           lhs.original_value_offset == rhs.original_value_offset &&
           lhs.stored_value_size == rhs.stored_value_size &&
           lhs.compression == rhs.compression &&
           lhs.stable_checksum_version == rhs.stable_checksum_version &&
           lhs.stable_checksum == rhs.stable_checksum &&
           lhs.stored_checksum == rhs.stored_checksum;
  }

  Status ReadRequiredNormalRecord(const BlobReference& reference,
                                  const NormalBlobFileInfo& file,
                                  BlobReaderCache* cache) {
    if (cache->normal_records == nullptr ||
        cache->normal_record_path != file.path ||
        reference.original_value_offset < cache->checked_offset) {
      cache->normal_records.reset();
      cache->normal_record_path.clear();
      cache->checked_offset = 0;
      std::unique_ptr<RandomAccessFileReader> reader;
      Status status = RandomAccessFileReader::Create(
          env_->GetFileSystem(), file.path, file_options_, &reader, nullptr);
      if (!status.ok()) {
        return status;
      }
      cache->normal_records = std::make_unique<BlobLogSequentialReader>(
          std::move(reader), immutable_db_options_.clock,
          immutable_db_options_.stats);
      BlobLogHeader header;
      status = cache->normal_records->ReadHeader(&header);
      if (!status.ok()) {
        cache->normal_records.reset();
        return status;
      }
      if (header.column_family_id != file.column_family_id ||
          header.compression != file.compression) {
        cache->normal_records.reset();
        return Status::Corruption("Normal blob donor changed during repair");
      }
      cache->normal_record_path = file.path;
    }
    if (cache->checked_offset != 0 &&
        reference.original_value_offset == cache->checked_offset) {
      return Status::OK();
    }
    while (cache->normal_records->GetNextByte() <
           file.file_size - BlobLogFooter::kSize) {
      BlobLogRecord record;
      uint64_t offset = 0;
      Status status = cache->normal_records->ReadRecordHeader(
          &record, &offset, file.file_size - BlobLogFooter::kSize);
      if (!status.ok()) {
        return status;
      }
      if (offset > reference.original_value_offset) {
        return Status::Corruption(
            "Blob reference does not name a record boundary");
      }
      if (offset != reference.original_value_offset) {
        status = cache->normal_records->ReadRecordPayload(
            &record, BlobLogSequentialReader::kReadHeader);
        if (!status.ok()) {
          return status;
        }
        continue;
      }
      cache->checked_user_key.clear();
      status = reference.HasStableIdentity()
                   ? cache->normal_records->ReadAndVerifyRecordPayload(record)
                   : cache->normal_records->ReadAndVerifyRecordPayloadAndGetKey(
                         record, &cache->checked_user_key);
      if (!status.ok()) {
        cache->normal_records.reset();
        return status;
      }
      cache->checked_offset = offset;
      cache->checked_value_size = record.value_size;
      cache->checked_record_bytes = record.record_size();
      TEST_SYNC_POINT("Repairer::ScanNormalBlobFile:RetainedRecord");
      return Status::OK();
    }
    return Status::Corruption("Blob file does not contain a required record");
  }

  static Status MakeRepairBlobIndex(const BlobReference& reference,
                                    BlobIndex* index) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, reference.generation_file_number,
        reference.origin_file_number, reference.original_value_offset,
        reference.stored_value_size, reference.compression,
        reference.stable_checksum_version, reference.stable_checksum,
        reference.stored_checksum);
    return index->DecodeFrom(encoded);
  }

  Status ValidateNormalBlobReference(
      uint32_t column_family_id, const BlobReference& reference,
      const NormalBlobFileInfo& file,
      const std::string* expected_legacy_user_key,
      NormalBlobValidationMode validation_mode,
      std::string* validated_stored_payload, BlobReaderCache* reader_cache) {
    assert(reader_cache != nullptr);
    if (file.column_family_id != column_family_id ||
        file.compression != reference.compression) {
      return Status::Corruption("Blob reference owner or compression mismatch");
    }
    Status record_status =
        ReadRequiredNormalRecord(reference, file, reader_cache);
    if (!record_status.ok()) {
      return record_status;
    }
    if (reader_cache->checked_value_size != reference.stored_value_size ||
        (reference.encoding_class != BlobReferenceEncodingClass::kRelocated &&
         reference.record_bytes != 0 &&
         reader_cache->checked_record_bytes != reference.record_bytes)) {
      return Status::Corruption("Blob reference does not name an exact record");
    }
    if (validation_mode == NormalBlobValidationMode::kPublishedFile &&
        reference.HasStableIdentity() != file.addition.IsIdentityRoot()) {
      return Status::Corruption(
          "Blob reference encoding does not match file classification");
    }
    if (expected_legacy_user_key != nullptr &&
        reader_cache->checked_user_key != *expected_legacy_user_key) {
      return Status::Corruption(
          "Legacy blob reference does not match its SST user key");
    }
    if (!reference.HasStableIdentity()) {
      return Status::OK();
    }
    const uint64_t file_number = file.addition.GetBlobFileNumber();
    if (reader_cache->file_number != file_number ||
        reader_cache->path != file.path) {
      reader_cache->reader.reset();
      reader_cache->file_number = kInvalidBlobFileNumber;
      reader_cache->path.clear();
      ColumnFamilyData* const cfd =
          vset_.GetColumnFamilySet()->GetColumnFamily(column_family_id);
      if (cfd == nullptr) {
        return Status::Corruption("Missing blob reference column family");
      }
      Status status = BlobFileReader::CreateFromPathForRepair(
          cfd->ioptions(), ReadOptions(), file_options_, column_family_id,
          /*blob_file_read_hist=*/nullptr, file_number, file.path,
          /*io_tracer=*/nullptr, &reader_cache->reader);
      if (!status.ok()) {
        return status;
      }
      reader_cache->file_number = file_number;
      reader_cache->path = file.path;
    }
    uint64_t bytes_read = 0;
    if (validated_stored_payload == nullptr &&
        reference.stable_checksum_version ==
            BlobIndex::kStableChecksumVersion2) {
      BlobIndex index;
      Status status = MakeRepairBlobIndex(reference, &index);
      return status.ok() ? reader_cache->reader->StreamStoredBlobForRelocation(
                               ReadOptions(), index,
                               [](const Slice&) { return Status::OK(); })
                         : status;
    }
    return reader_cache->reader->GetStoredBlobForRelocation(
        ReadOptions(), reference.original_value_offset,
        reference.stored_value_size, reference.compression,
        reference.stable_checksum_version, reference.stable_checksum,
        reference.stored_checksum, kMaxRepairBlobPayloadSize,
        kMaxRepairBlobPayloadSize, validated_stored_payload, &bytes_read);
  }

  Status GetRelocationBlobReader(uint64_t file_number,
                                 const RelocationBlobFileInfo& file,
                                 BlobReaderCache* reader_cache,
                                 BlobFileReader** reader) {
    assert(reader_cache != nullptr);
    assert(reader != nullptr);
    *reader = nullptr;
    if (reader_cache->file_number == file_number &&
        reader_cache->path == file.path) {
      assert(reader_cache->reader != nullptr);
      *reader = reader_cache->reader.get();
      return Status::OK();
    }

    reader_cache->reader.reset();
    reader_cache->file_number = kInvalidBlobFileNumber;
    reader_cache->path.clear();
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(file.column_family_id);
    if (cfd == nullptr) {
      return Status::Corruption("Missing blob relocation column family");
    }
    BlockBasedTableOptions table_options;
    Status status = GetRelocationTableOptions(*cfd, &table_options);
    if (!status.ok()) {
      return status;
    }
    const MutableCFOptions& mutable_options = cfd->GetLatestMutableCFOptions();
    status = BlobFileReader::CreateRelocationFileForRepair(
        cfd->ioptions(), ReadOptions(), file_options_,
        /*blob_file_read_hist=*/nullptr, file_number, file.file_size, file.path,
        db_session_id_, table_options,
        mutable_options.compression_manager.get(),
        mutable_options.block_protection_bytes_per_key,
        /*io_tracer=*/nullptr, &reader_cache->reader);
    if (!status.ok()) {
      return status;
    }
    const BlobRelocationDirectory* const reopened_directory =
        reader_cache->reader->GetRelocationDirectory();
    if (reader_cache->reader->GetRelocationColumnFamilyId() !=
            file.column_family_id ||
        reopened_directory == nullptr ||
        reopened_directory->format_version() !=
            file.directory.format_version() ||
        reopened_directory->directory_digest() !=
            file.directory.directory_digest() ||
        reopened_directory->total_entries() != file.directory.total_entries() ||
        reopened_directory->total_record_bytes() !=
            file.directory.total_record_bytes()) {
      reader_cache->reader.reset();
      return Status::Corruption("Blob relocation file changed during repair");
    }
    reader_cache->file_number = file_number;
    reader_cache->path = file.path;
    *reader = reader_cache->reader.get();
    return Status::OK();
  }

  static Status ValidateRelocationBlobReference(
      const BlobReference& reference, BlobFileReader* reader,
      std::string* validated_stored_payload) {
    assert(reader != nullptr);
    if (validated_stored_payload == nullptr &&
        reference.stable_checksum_version ==
            BlobIndex::kStableChecksumVersion2) {
      BlobIndex index;
      Status status = MakeRepairBlobIndex(reference, &index);
      return status.ok() ? reader->StreamStoredBlobForRelocation(
                               ReadOptions(), index,
                               [](const Slice&) { return Status::OK(); })
                         : status;
    }
    Status read_status;
    StoredRelocationFileReadRequest request{
        reference.origin_file_number,      reference.original_value_offset,
        reference.stored_value_size,       reference.compression,
        reference.stable_checksum_version, reference.stable_checksum,
        validated_stored_payload,          &read_status,
        reference.stored_checksum};
    uint64_t bytes_read = 0;
    reader->MultiGetStoredBlobsFromRelocationFile(
        ReadOptions(), 1, &request, kMaxRepairBlobPayloadSize,
        kMaxRepairBlobPayloadSize, &bytes_read);
    return read_status;
  }

  Status ReadRepairBlobPayload(
      uint32_t column_family_id, const BlobReference& reference,
      const NormalBlobFileCandidates& normal_files,
      const RelocationBlobFileCandidates& relocation_files,
      BlobReaderCache* normal_reader_cache,
      BlobReaderCache* relocation_reader_cache, std::string* stored_payload) {
    assert(normal_reader_cache != nullptr);
    assert(relocation_reader_cache != nullptr);
    assert(stored_payload != nullptr);
    stored_payload->clear();
    Status external_status;
    external_status.PermitUncheckedOk();

    const auto normal_range =
        normal_files.equal_range(reference.origin_file_number);
    for (auto normal = normal_range.first; normal != normal_range.second;
         ++normal) {
      Status status = ValidateNormalBlobReference(
          column_family_id, reference, normal->second,
          /*expected_legacy_user_key=*/nullptr,
          NormalBlobValidationMode::kPayloadDonor,
          /*validated_stored_payload=*/stored_payload, normal_reader_cache);
      if (status.ok()) {
        external_status.PermitUncheckedError();
        return status;
      }
      if (!status.IsCorruption() && !status.IsNotSupported() &&
          external_status.ok()) {
        external_status = status;
      }
      status.PermitUncheckedError();
    }

    for (const auto& candidate : relocation_files) {
      if (candidate.second.column_family_id != column_family_id ||
          candidate.second.directory.Find(reference.origin_file_number) ==
              nullptr) {
        continue;
      }
      BlobFileReader* reader = nullptr;
      Status read_status = GetRelocationBlobReader(
          candidate.first, candidate.second, relocation_reader_cache, &reader);
      if (read_status.ok()) {
        read_status =
            ValidateRelocationBlobReference(reference, reader, stored_payload);
      }
      if (read_status.ok()) {
        external_status.PermitUncheckedError();
        return read_status;
      }
      if (!read_status.IsCorruption() && !read_status.IsNotSupported() &&
          external_status.ok()) {
        external_status = read_status;
      }
      read_status.PermitUncheckedError();
      stored_payload->clear();
    }
    if (!external_status.ok()) {
      return external_status;
    }
    return Status::Corruption(
        "Repair cannot find a valid copy of a required blob value");
  }

  Status SelectRepairBlobReader(
      uint32_t cf_id, const BlobReference& reference,
      const NormalBlobFileCandidates& normal_files,
      const RelocationBlobFileCandidates& relocation_files,
      BlobReaderCache* normal_cache, BlobReaderCache* relocation_cache,
      BlobFileReader** reader) {
    const auto range = normal_files.equal_range(reference.origin_file_number);
    for (auto file = range.first; file != range.second; ++file) {
      Status status = ValidateNormalBlobReference(
          cf_id, reference, file->second, nullptr,
          NormalBlobValidationMode::kPayloadDonor, nullptr, normal_cache);
      if (status.ok()) {
        *reader = normal_cache->reader.get();
        return status;
      }
      if (!status.IsCorruption() && !status.IsNotSupported()) {
        return status;
      }
    }
    for (const auto& file : relocation_files) {
      if (file.second.column_family_id != cf_id ||
          file.second.directory.Find(reference.origin_file_number) == nullptr) {
        continue;
      }
      Status status = GetRelocationBlobReader(file.first, file.second,
                                              relocation_cache, reader);
      if (status.ok()) {
        status = ValidateRelocationBlobReference(reference, *reader, nullptr);
      }
      if (status.ok()) {
        return status;
      }
      if (!status.IsCorruption() && !status.IsNotSupported()) {
        return status;
      }
    }
    return Status::Corruption(
        "Repair cannot find a valid copy of required blob bytes");
  }

  Status BuildRepairRelocationFile(
      uint32_t column_family_id,
      const std::vector<BlobRelocationRouteSource>& sources,
      const std::vector<RepairInputGroup>& references,
      const NormalBlobFileCandidates& normal_files,
      const RelocationBlobFileCandidates& relocation_files,
      BlobReaderCache* normal_reader_cache,
      BlobReaderCache* relocation_reader_cache,
      BlobRelocationFileAddition* addition) {
    assert(normal_reader_cache != nullptr);
    assert(relocation_reader_cache != nullptr);
    assert(addition != nullptr);
    assert(!sources.empty());
    const uint64_t origin_file_number = sources.front().origin_file_number;
    for (const BlobRelocationRouteSource& source : sources) {
      if (source.origin_file_number != origin_file_number) {
        return Status::Corruption(
            "Repair relocation sources have inconsistent origins");
      }
    }
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(column_family_id);
    if (cfd == nullptr) {
      return Status::Corruption("Missing blob repair column family");
    }
    BlockBasedTableOptions source_table_options;
    Status status = GetRelocationTableOptions(*cfd, &source_table_options);
    if (!status.ok()) {
      return status;
    }

    uint64_t file_number = 0;
    status = AllocateRepairFileNumber(&file_number);
    if (!status.ok()) {
      return status;
    }
    const std::string output_directory = cfd->ioptions().cf_paths.front().path;
    const std::string output_path = BlobFileName(output_directory, file_number);
    const std::string temporary_path =
        TempFileName(output_directory, file_number);
    FileOptions file_options = file_options_;
    file_options.open_contract = FileOpenContract::kNoReopenForWrite |
                                 FileOpenContract::kNoReadersWhileOpenForWrite;
    std::unique_ptr<FSWritableFile> file;
    status =
        RegisterRepairOutput(temporary_path, file_number, column_family_id, 0);
    if (status.ok()) {
      status =
          RegisterRepairOutput(output_path, file_number, column_family_id, 0);
    }
    if (status.ok()) {
      status = NewWritableFile(env_->GetFileSystem().get(), temporary_path,
                               &file, file_options);
    }
    // The table builder retains this comparator by reference through Finish.
    InternalKeyComparator comparator(BytewiseComparator());
    std::unique_ptr<WritableFileWriter> file_writer;
    std::unique_ptr<TableBuilder> table_builder;
    BlobRelocationFileMetadata metadata;
    uint64_t file_size = 0;
    if (status.ok()) {
      if (file == nullptr) {
        status =
            Status::Corruption("File system returned a null repair blob file");
      } else {
        FileTypeSet checksum_types =
            immutable_db_options_.checksum_handoff_file_types;
        file_writer = std::make_unique<WritableFileWriter>(
            std::move(file), temporary_path, file_options,
            immutable_db_options_.clock, /*io_tracer=*/nullptr,
            immutable_db_options_.stats,
            Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS,
            immutable_db_options_.listeners,
            immutable_db_options_.file_checksum_gen_factory.get(),
            checksum_types.Contains(FileType::kBlobFile), false);
        MutableCFOptions relocation_options = cfd->GetLatestMutableCFOptions();
        relocation_options.enable_blob_indirection = false;
        relocation_options.prefix_extractor.reset();
        relocation_options.table_factory =
            std::make_shared<BlockBasedTableFactory>(
                MakeBlobRelocationTableOptions(source_table_options));
        InternalTblPropCollFactories collectors;
        CompressionOptions no_compression;
        EmbeddedBlobSstBuilderOptions embedded_options;
        embedded_options.min_blob_size = 0;
        const ReadOptions builder_read_options;
        const WriteOptions builder_write_options;
        const std::string repair_db_id = "DB Repairer";
        TableBuilderOptions builder_options(
            cfd->ioptions(), relocation_options, builder_read_options,
            builder_write_options, comparator, &collectors, kNoCompression,
            no_compression, column_family_id, cfd->GetName(), /*level=*/-1,
            kUnknownNewestKeyTime, /*is_bottommost=*/false,
            TableFileCreationReason::kRecovery, /*oldest_key_time=*/0,
            /*file_creation_time=*/0, repair_db_id, db_session_id_,
            /*target_file_size=*/0, file_number, kMaxSequenceNumber, dbname_,
            /*is_remote_compaction=*/false, &embedded_options);
        table_builder.reset(
            NewTableBuilder(builder_options, file_writer.get()));
        status =
            table_builder == nullptr
                ? Status::Corruption("Failed to create repair blob builder")
                : table_builder->StartBlobRelocationFile();
      }
    }
    for (const BlobRelocationRouteSource& source : sources) {
      if (!status.ok()) {
        break;
      }
      status = table_builder->AddBlobRelocationRouteSource(
          origin_file_number, source.generation_file_number);
    }
    if (status.ok()) {
      status = table_builder->AddBlobRelocationRouteSource(origin_file_number,
                                                           file_number);
    }
    if (status.ok()) {
      status = VisitRepairReferences(
          references,
          [&](const BlobReference& reference,
              const std::vector<RepairReferenceCursor*>&) -> Status {
            if (reference.stable_checksum_version ==
                BlobIndex::kStableChecksumVersion2) {
              BlobFileReader* reader = nullptr;
              Status selected = SelectRepairBlobReader(
                  column_family_id, reference, normal_files, relocation_files,
                  normal_reader_cache, relocation_reader_cache, &reader);
              if (!selected.ok()) {
                return selected;
              }
              BlobIndex index;
              selected = MakeRepairBlobIndex(reference, &index);
              if (!selected.ok()) {
                return selected;
              }
              return table_builder->AddBlobRelocation(
                  reference.origin_file_number, reference.original_value_offset,
                  reference.stored_value_size, reference.compression,
                  reference.stored_checksum,
                  [&](const BlobChunkConsumer& consumer) {
                    return reader->StreamStoredBlobForRelocation(
                        ReadOptions(), index, consumer);
                  });
            }
            std::string stored_payload;
            Status selected = ReadRepairBlobPayload(
                column_family_id, reference, normal_files, relocation_files,
                normal_reader_cache, relocation_reader_cache, &stored_payload);
            return selected.ok() ? table_builder->AddBlobRelocation(
                                       reference.origin_file_number,
                                       reference.original_value_offset,
                                       stored_payload, reference.compression)
                                 : selected;
          });
    }
    if (table_builder != nullptr) {
      if (status.ok()) {
        status = table_builder->Finish();
        file_size = table_builder->FileSize();
        if (status.ok()) {
          status = table_builder->GetBlobRelocationFileMetadata(&metadata);
        }
      } else {
        table_builder->Abandon();
      }
      const IOStatus builder_status = table_builder->io_status();
      if (!builder_status.ok() && status.ok()) {
        status = builder_status;
      }
      table_builder.reset();
    }

    std::string checksum_method;
    std::string checksum_value;
    if (file_writer != nullptr) {
      IOOptions io_options;
      if (status.ok()) {
        status =
            WritableFileWriter::PrepareIOOptions(WriteOptions(), io_options);
      }
      if (status.ok()) {
        status = immutable_db_options_.use_fsync
                     ? file_writer->Fsync(io_options)
                     : file_writer->Sync(io_options);
      }
      const IOStatus close_status = file_writer->Close(io_options);
      if (!close_status.ok() && status.ok()) {
        status = close_status;
      }
      if (status.ok()) {
        checksum_value = file_writer->GetFileChecksum();
        checksum_method = file_writer->GetFileChecksumFuncName();
        if (checksum_value == kUnknownFileChecksum) {
          checksum_value.clear();
        }
        if (checksum_method == kUnknownFileChecksumFuncName) {
          checksum_method.clear();
        }
        if (checksum_method.empty() != checksum_value.empty()) {
          status = Status::Corruption(
              "Incomplete repair blob file checksum metadata");
        }
      }
      file_writer.reset();
    }
    if (status.ok()) {
      status = env_->RenameFile(temporary_path, output_path);
    }
    if (status.ok()) {
      std::unique_ptr<FSDirectory> directory;
      status = env_->GetFileSystem()->NewDirectory(
          output_directory, IOOptions(), &directory, /*dbg=*/nullptr);
      if (status.ok()) {
        status = directory->FsyncWithDirOptions(
            IOOptions(), /*dbg=*/nullptr,
            DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      }
    }
    if (!status.ok()) {
      env_->DeleteFile(temporary_path).PermitUncheckedError();
      return status;
    }
    *addition = BlobRelocationFileAddition(
        file_number, column_family_id, file_size, std::move(checksum_method),
        std::move(checksum_value), std::move(metadata.encoded_directory),
        metadata.tail_size);
    status = addition->Validate();
    return status.ok() ? SealRepairOutput(output_path) : status;
  }

  Status RepairBlobState() {
    if (feature_format_column_families_.empty()) {
      return Status::OK();
    }
    for (uint32_t cf : feature_format_column_families_) {
      if (vset_.GetColumnFamilySet()->GetColumnFamily(cf) != nullptr) {
        blob_repair_plans_.try_emplace(cf);
      }
    }
    // Only lightweight (SST, source) handles survive the table scan. Each
    // origin is processed through block-bounded sorted cursors; references
    // shared by SSTs/generations are coalesced, not copied into global maps.
    using GroupKey = std::tuple<uint32_t, bool, uint64_t>;
    std::map<GroupKey, std::vector<RepairInputGroup>> groups;
    std::set<uint64_t> required_origins;
    for (TableInfo& table : tables_) {
      if (feature_format_column_families_.count(table.column_family_id) == 0) {
        continue;
      }
      table.meta.oldest_blob_file_number = kInvalidBlobFileNumber;
      if (table.meta.blob_reference_descriptor.empty()) {
        table.meta.blob_reference_legacy_metadata =
            table.meta.oldest_coarse_identity_blob_file_number ==
            kInvalidBlobFileNumber;
      }
      ColumnFamilyBlobRepairPlan& plan =
          blob_repair_plans_[table.column_family_id];
      for (const RepairReferenceSource& source : table.reference_sources) {
        BlobReference reference;
        reference.encoding_class = std::get<0>(source);
        reference.generation_file_number = std::get<1>(source);
        reference.origin_file_number = std::get<2>(source);
        const uint64_t lost =
            GetDurableBlobLoss(table.column_family_id, reference);
        if (lost != kInvalidBlobFileNumber) {
          plan.relocation_files.emplace(lost, durable_loss_files_.at(lost));
          plan.missing_files.insert(lost);
          if (reference.generation_file_number != lost) {
            plan.routes.emplace(
                BlobRelocationRouteSource{reference.generation_file_number,
                                          reference.origin_file_number},
                lost);
          }
          continue;
        }
        groups[{table.column_family_id, reference.HasStableIdentity(),
                reference.origin_file_number}]
            .push_back({&table, source});
        required_origins.insert(reference.origin_file_number);
      }
    }
    NormalBlobFileCandidates normal_files;
    RelocationBlobFileCandidates relocation_files;
    Status status =
        DiscoverBlobFiles(required_origins, &normal_files, &relocation_files);
    if (!status.ok()) {
      return status;
    }
    BlobReaderCache normal_cache;
    BlobReaderCache relocation_cache;
    for (const auto& group : groups) {
      const uint32_t cf_id = std::get<0>(group.first);
      const bool stable = std::get<1>(group.first);
      const uint64_t origin_number = std::get<2>(group.first);
      ColumnFamilyBlobRepairPlan& plan = blob_repair_plans_[cf_id];
      std::set<BlobRelocationRouteSource> unique_sources;
      bool coarse_identity = false;
      for (const RepairInputGroup& input : group.second) {
        unique_sources.insert({std::get<1>(input.source), origin_number});
        coarse_identity |=
            input.table->meta.oldest_coarse_identity_blob_file_number !=
            kInvalidBlobFileNumber;
      }
      const std::vector<BlobRelocationRouteSource> sources(
          unique_sources.begin(), unique_sources.end());
      uint64_t selected = kInvalidBlobFileNumber;
      const bool can_keep_normal =
          !stable || (sources.size() == 1 &&
                      sources.front().generation_file_number == origin_number);
      if (can_keep_normal) {
        const auto range = normal_files.equal_range(origin_number);
        const NormalBlobFileInfo* chosen = nullptr;
        for (auto file = range.first; file != range.second; ++file) {
          if (file->second.column_family_id != cf_id ||
              (stable && !coarse_identity && !file->second.is_canonical)) {
            continue;
          }
          status = VisitRepairReferences(
              group.second,
              [&](const BlobReference& reference,
                  const std::vector<RepairReferenceCursor*>& owners) -> Status {
                Status checked = ValidateNormalBlobReference(
                    cf_id, reference, file->second, nullptr,
                    NormalBlobValidationMode::kPublishedFile, nullptr,
                    &normal_cache);
                if (checked.ok() && !stable) {
                  for (RepairReferenceCursor* cursor : owners) {
                    checked = cursor->VerifyLegacyUserKey(
                        reference, normal_cache.checked_user_key);
                    if (!checked.ok()) {
                      break;
                    }
                  }
                }
                return checked;
              });
          if (status.ok()) {
            if (chosen == nullptr || file->second.is_canonical ||
                (!chosen->is_canonical && file->second.path < chosen->path)) {
              chosen = &file->second;
            }
            if (chosen->is_canonical) {
              break;
            }
          } else if (!status.IsCorruption() && !status.IsNotSupported()) {
            return status;
          }
        }
        if (chosen != nullptr) {
          if (!chosen->is_canonical) {
            status = CopyNormalBlobFileToCanonical(origin_number, *chosen);
            if (!status.ok()) {
              return status;
            }
          }
          selected = origin_number;
          plan.normal_files.emplace(selected, chosen->addition);
          for (const RepairInputGroup& input : group.second) {
            uint64_t& oldest = input.table->meta.oldest_blob_file_number;
            oldest = oldest == kInvalidBlobFileNumber
                         ? selected
                         : std::min(oldest, selected);
          }
        }
      }
      if (selected == kInvalidBlobFileNumber && (!stable || coarse_identity)) {
        return Status::Corruption(
            coarse_identity
                ? "Coarse SST identity root is unavailable"
                : "Repair cannot find a valid required legacy blob file");
      }
      if (selected == kInvalidBlobFileNumber) {
        for (const auto& file : relocation_files) {
          if (file.second.column_family_id != cf_id ||
              !file.second.is_canonical ||
              !file.second.directory.HasRepairMetadata()) {
            continue;
          }
          const BlobRelocationOrigin* const origin =
              file.second.directory.Find(origin_number);
          if (origin == nullptr) {
            continue;
          }
          bool serves_all = true;
          for (const BlobRelocationRouteSource& source : sources) {
            serves_all &=
                std::find(origin->served_generation_file_numbers.begin(),
                          origin->served_generation_file_numbers.end(),
                          source.generation_file_number) !=
                origin->served_generation_file_numbers.end();
          }
          if (!serves_all) {
            continue;
          }
          BlobFileReader* reader = nullptr;
          status = GetRelocationBlobReader(file.first, file.second,
                                           &relocation_cache, &reader);
          if (status.ok()) {
            status = VisitRepairReferences(
                group.second, [&](const BlobReference& reference,
                                  const std::vector<RepairReferenceCursor*>&) {
                  return ValidateRelocationBlobReference(reference, reader,
                                                         nullptr);
                });
          }
          if (status.ok()) {
            selected = file.first;
            plan.relocation_files.emplace(selected, file.second.addition);
            break;
          }
          if (!status.IsCorruption() && !status.IsNotSupported()) {
            return status;
          }
        }
      }
      if (selected == kInvalidBlobFileNumber) {
        BlobRelocationFileAddition rebuilt;
        status = BuildRepairRelocationFile(
            cf_id, sources, group.second, normal_files, relocation_files,
            &normal_cache, &relocation_cache, &rebuilt);
        if (!status.ok()) {
          return status;
        }
        selected = rebuilt.GetFileNumber();
        plan.relocation_files.emplace(selected, std::move(rebuilt));
      }
      if (stable) {
        for (const BlobRelocationRouteSource& source : sources) {
          if (selected != source.generation_file_number) {
            plan.routes.emplace(source, selected);
          }
        }
      }
    }
    return Status::OK();
  }

  Status AddTables() {
    if (next_file_number_ == 0 ||
        next_file_number_ >= std::numeric_limits<uint64_t>::max() - 1) {
      return Status::Corruption("Repair next file number is exhausted");
    }
    // TODO: plumb Env::IOActivity, Env::IOPriority;
    const ReadOptions read_options;
    const WriteOptions write_options;

    std::unordered_map<uint32_t, std::vector<const TableInfo*>> cf_id_to_tables;
    SequenceNumber max_sequence = feature_repair_ ? certified_max_sequence_ : 0;
    if (feature_repair_) {
      for (ColumnFamilyData* cfd : *vset_.GetColumnFamilySet()) {
        cf_id_to_tables.try_emplace(cfd->GetID());
      }
      if (certified_wal_floor_ != 0) {
        Status status = TrackDiscoveredFileNumber(certified_wal_floor_);
        if (!status.ok()) {
          return status;
        }
      }
    }
    for (size_t i = 0; i < tables_.size(); i++) {
      cf_id_to_tables[tables_[i].column_family_id].push_back(&tables_[i]);
      if (max_sequence < tables_[i].meta.fd.largest_seqno) {
        max_sequence = tables_[i].meta.fd.largest_seqno;
      }
    }
    vset_.SetLastAllocatedSequence(max_sequence);
    vset_.SetLastPublishedSequence(max_sequence);
    vset_.SetLastSequence(max_sequence);

    for (const auto& cf_id_and_tables : cf_id_to_tables) {
      auto* cfd =
          vset_.GetColumnFamilySet()->GetColumnFamily(cf_id_and_tables.first);

      // Recover files' epoch number using dummy VersionStorageInfo
      VersionBuilder dummy_version_builder(
          cfd->current()->version_set()->file_options(), &cfd->ioptions(),
          cfd->table_cache(), cfd->current()->storage_info(),
          cfd->current()->version_set(),
          cfd->GetFileMetadataCacheReservationManager());
      VersionStorageInfo dummy_vstorage(
          &cfd->internal_comparator(), cfd->user_comparator(),
          cfd->NumberLevels(), cfd->ioptions().compaction_style,
          nullptr /* src_vstorage */, cfd->ioptions().force_consistency_checks,
          EpochNumberRequirement::kMightMissing, cfd->ioptions().clock,
          /*bottommost_file_compaction_delay=*/0,
          cfd->current()->version_set()->offpeak_time_option(),
          PeriodicCompactionPhaseParams{});
      Status s;
      VersionEdit dummy_edit;
      const auto repair_plan = blob_repair_plans_.find(cfd->GetID());
      if (repair_plan != blob_repair_plans_.end()) {
        dummy_edit.SetBlobIndirectionFormatVersion(
            kBlobIndirectionManifestFormatVersion);
      }
      for (const auto* table : cf_id_and_tables.second) {
        // TODO(opt): separate out into multiple levels
        const auto& meta = table->meta;
        dummy_edit.AddFile(0, meta);
      }
      s = dummy_version_builder.Apply(&dummy_edit);
      if (s.ok()) {
        s = dummy_version_builder.SaveTo(&dummy_vstorage);
      }
      if (s.ok()) {
        dummy_vstorage.RecoverEpochNumbers(cfd);
      }
      if (s.ok()) {
        // Record changes from this repair in VersionEdit, including files with
        // recovered epoch numbers
        VersionEdit edit;
        edit.SetComparatorName(cfd->user_comparator()->Name());
        edit.SetPersistUserDefinedTimestamps(
            cfd->ioptions().persist_user_defined_timestamps);
        edit.SetLogNumber(
            feature_repair_ ? std::max(certified_wal_floor_,
                                       proven_original_cf_floors_[cfd->GetID()])
                            : 0);
        edit.SetNextFile(next_file_number_);
        edit.SetColumnFamily(cfd->GetID());
        for (int level = 0; level < dummy_vstorage.num_levels(); ++level) {
          for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
            edit.AddFile(level, *file_meta);
          }
        }
        if (repair_plan != blob_repair_plans_.end()) {
          edit.SetBlobIndirectionFormatVersion(
              kBlobIndirectionManifestFormatVersion);
          for (const auto& addition : repair_plan->second.normal_files) {
            edit.AddBlobFile(addition.second);
          }
          for (const auto& addition : repair_plan->second.relocation_files) {
            edit.AddBlobRelocationFile(addition.second);
          }
          for (uint64_t file : repair_plan->second.missing_files) {
            edit.MarkBlobRelocationFileMissing(file);
          }
          for (const auto& route : repair_plan->second.routes) {
            edit.SetBlobRelocationRoute(BlobRelocationRoute(
                route.first.generation_file_number,
                route.first.origin_file_number, route.second));
          }
        }

        // Release resources occupied by the dummy VersionStorageInfo
        for (int level = 0; level < dummy_vstorage.num_levels(); ++level) {
          for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
            if (file_meta->Unref()) {
              delete file_meta;
            }
          }
        }

        // Persist record of changes
        assert(next_file_number_ > 0);
        vset_.MarkFileNumberUsed(next_file_number_ - 1);
        mutex_.Lock();
        std::unique_ptr<FSDirectory> db_dir;
        s = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(), &db_dir,
                                                nullptr);
        if (s.ok()) {
          s = vset_.LogAndApply(cfd, read_options, write_options, &edit,
                                &mutex_, db_dir.get(),
                                false /* new_descriptor_log */);
        }
        mutex_.Unlock();
      }
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  }

  void ArchiveFile(const std::string& fname) {
    // Move into another directory.  E.g., for
    //    dir/foo
    // rename to
    //    dir/lost/foo
    const char* slash = strrchr(fname.c_str(), '/');
    std::string new_dir;
    if (slash != nullptr) {
      new_dir.assign(fname.data(), slash - fname.data());
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir).PermitUncheckedError();  // Ignore error
    std::string new_file = new_dir;
    new_file.append("/");
    new_file.append((slash == nullptr) ? fname.c_str() : slash + 1);
    Status s = env_->RenameFile(fname, new_file);
    ROCKS_LOG_INFO(db_options_.info_log, "Archiving %s: %s\n", fname.c_str(),
                   s.ToString().c_str());
  }
};

Status GetDefaultCFOptions(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    ColumnFamilyOptions* res) {
  assert(res != nullptr);
  auto iter = std::find_if(column_families.begin(), column_families.end(),
                           [](const ColumnFamilyDescriptor& cfd) {
                             return cfd.name == kDefaultColumnFamilyName;
                           });
  if (iter == column_families.end()) {
    return Status::InvalidArgument(
        "column_families", "Must contain entry for default column family");
  }
  *res = iter->options;
  return Status::OK();
}
}  // anonymous namespace

Status RepairDB(const std::string& dbname, const DBOptions& db_options,
                const std::vector<ColumnFamilyDescriptor>& column_families) {
  ColumnFamilyOptions default_cf_opts;
  Status status = GetDefaultCFOptions(column_families, &default_cf_opts);
  if (!status.ok()) {
    return status;
  }

  Repairer repairer(dbname, db_options, column_families, default_cf_opts,
                    ColumnFamilyOptions() /* unknown_cf_opts */,
                    false /* create_unknown_cfs */);
  status = repairer.Run();
  if (status.ok()) {
    status = repairer.Close();
  }
  return status;
}

Status RepairDB(const std::string& dbname, const DBOptions& db_options,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                const ColumnFamilyOptions& unknown_cf_opts) {
  ColumnFamilyOptions default_cf_opts;
  Status status = GetDefaultCFOptions(column_families, &default_cf_opts);
  if (!status.ok()) {
    return status;
  }

  Repairer repairer(dbname, db_options, column_families, default_cf_opts,
                    unknown_cf_opts, true /* create_unknown_cfs */);
  status = repairer.Run();
  if (status.ok()) {
    status = repairer.Close();
  }
  return status;
}

Status RepairDB(const std::string& dbname, const Options& options) {
  Options opts(options);
  DBOptions db_options(opts);
  ColumnFamilyOptions cf_options(opts);

  Repairer repairer(dbname, db_options, {}, cf_options /* default_cf_opts */,
                    cf_options /* unknown_cf_opts */,
                    true /* create_unknown_cfs */);
  Status status = repairer.Run();
  if (status.ok()) {
    status = repairer.Close();
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
