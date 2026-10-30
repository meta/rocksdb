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
// Every log file that is active is replayed. All sections of the file where the
// checksum does not match is skipped over. We intentionally give preference to
// data consistency.
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
//  - log number is set to zero
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

#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/unique_id_impl.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

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
      cf_name_to_opts_[cfd.name] = cfd.options;
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
      if (db_lock_ != nullptr) {
        s = env_->UnlockFile(db_lock_);
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
    status = FindFiles();
    DBImpl* db_impl = nullptr;
    if (status.ok()) {
      // Discard older manifests and start a fresh one
      for (size_t i = 0; i < manifests_.size(); i++) {
        ArchiveFile(dbname_ + "/" + manifests_[i]);
      }
      // Just create a DBImpl temporarily so we can reuse NewDB()
      db_impl = new DBImpl(db_options_, dbname_);
      status = db_impl->NewDB(/*new_filenames=*/nullptr);
    }
    delete db_impl;

    if (status.ok()) {
      // Recover using the fresh manifest created by NewDB()
      status =
          vset_.Recover({{kDefaultColumnFamilyName, default_cf_opts_}}, false);
    }
    if (status.ok()) {
      // Need to scan existing SST files first so the column families are
      // created before we process WAL files
      ExtractMetaData();

      // ExtractMetaData() uses table_fds_ to know which SST files' metadata to
      // extract -- we need to clear it here since metadata for existing SST
      // files has been extracted already
      table_fds_.clear();
      ConvertLogFilesToTables();
      ExtractMetaData();
      status = AddTables();
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
  struct TableInfo {
    FileMetaData meta;
    uint32_t column_family_id;
    std::string column_family_name;
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
  std::vector<FileDescriptor> table_fds_;
  std::vector<uint64_t> logs_;
  std::vector<TableInfo> tables_;
  uint64_t next_file_number_;
  // Lock over the persistent DB state. Non-nullptr iff successfully
  // acquired.
  FileLock* db_lock_;
  bool closed_;

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
          if (type == kDescriptorFile) {
            manifests_.push_back(filenames[i]);
          } else {
            if (number + 1 > next_file_number_) {
              next_file_number_ = number + 1;
            }
            if (type == kWalFile) {
              logs_.push_back(number);
            } else if (type == kTableFile) {
              table_fds_.emplace_back(number, static_cast<uint32_t>(path_id),
                                      0);
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

  void ConvertLogFilesToTables() {
    const auto& wal_dir = immutable_db_options_.GetWalDir();
    for (size_t i = 0; i < logs_.size(); i++) {
      // we should use LogFileName(wal_dir, logs_[i]) here. user might uses
      // wal_dir option.
      std::string logname = LogFileName(wal_dir, logs_[i]);
      Status status = ConvertLogToTable(wal_dir, logs_[i]);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Log #%" PRIu64 ": ignoring conversion error: %s",
                       logs_[i], status.ToString().c_str());
      }
      ArchiveFile(logname);
    }
  }

  Status ConvertLogToTable(const std::string& wal_dir, uint64_t log) {
    struct LogReporter : public log::Reader::Reporter {
      Env* env;
      std::shared_ptr<Logger> info_log;
      uint64_t lognum;
      void Corruption(size_t bytes, const Status& s,
                      uint64_t log_number = kMaxSequenceNumber) override {
        // We print error messages for corruption, but continue repairing.
        ROCKS_LOG_ERROR(info_log, "Log #%" PRIu64 ": dropping %d bytes; %s",
                        log_number == kMaxSequenceNumber ? lognum : log_number,
                        static_cast<int>(bytes), s.ToString().c_str());
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
    auto cf_mems = new ColumnFamilyMemTablesImpl(vset_.GetColumnFamilySet());

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
          record_status =
              WriteBatchInternal::InsertInto(&batch, cf_mems, nullptr, nullptr);
        }
      }
      if (record_status.ok()) {
        counter += WriteBatchInternal::Count(&batch);
      } else {
        ROCKS_LOG_WARN(db_options_.info_log, "Log #%" PRIu64 ": ignoring %s",
                       log, record_status.ToString().c_str());
      }
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
      meta.fd = FileDescriptor(next_file_number_++, 0, 0);
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
          table_fds_.push_back(meta.fd);
        }
      } else {
        break;
      }
    }
    delete cf_mems;
    return status;
  }

  void ExtractMetaData() {
    for (size_t i = 0; i < table_fds_.size(); i++) {
      TableInfo t;
      t.meta.fd = table_fds_[i];
      Status status = ScanTable(&t);
      if (!status.ok()) {
        std::string fname = TableFileName(
            db_options_.db_paths, t.meta.fd.GetNumber(), t.meta.fd.GetPathId());
        char file_num_buf[kFormatFileNumberBufSize];
        FormatFileNumber(t.meta.fd.GetNumber(), t.meta.fd.GetPathId(),
                         file_num_buf, sizeof(file_num_buf));
        ROCKS_LOG_WARN(db_options_.info_log, "Table #%s: ignoring %s",
                       file_num_buf, status.ToString().c_str());
        ArchiveFile(fname);
      } else {
        tables_.push_back(t);
      }
    }
  }

  Status ScanTable(TableInfo* t) {
    std::string fname = TableFileName(
        db_options_.db_paths, t->meta.fd.GetNumber(), t->meta.fd.GetPathId());
    int counter = 0;
    uint64_t file_size;
    Status status = env_->GetFileSize(fname, &file_size);
    t->meta.fd = FileDescriptor(t->meta.fd.GetNumber(), t->meta.fd.GetPathId(),
                                file_size);
    std::shared_ptr<const TableProperties> props;
    if (status.ok()) {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      const ReadOptions read_options;
      status = table_cache_->GetTableProperties(
          file_options_, read_options, icmp_, t->meta, &props, default_mopts_);
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
      t->column_family_id = static_cast<uint32_t>(props->column_family_id);
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
      }
      t->meta.oldest_ancestor_time = props->oldest_ancestor_time;
      t->meta.user_defined_timestamps_persisted =
          static_cast<bool>(props->user_defined_timestamps_persisted);
    }
    if (status.ok()) {
      uint64_t tail_size = FileMetaData::CalculateTailSize(file_size, *props);
      t->meta.tail_size = tail_size;
    }
    ColumnFamilyData* cfd = nullptr;
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
      InternalIterator* iter = table_cache_->NewIterator(
          ropts, file_options_, cfd->internal_comparator(), t->meta,
          nullptr /* range_del_agg */, cfd->GetLatestMutableCFOptions(),
          /*table_reader_ptr=*/nullptr, /*file_read_hist=*/nullptr,
          TableReaderCaller::kRepair, /*arena=*/nullptr, /*skip_filters=*/false,
          /*level=*/-1, /*max_file_size_for_l0_meta_pin=*/0,
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key=*/nullptr,
          /*allow_unprepared_value=*/false);
      ParsedInternalKey parsed;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        Status pik_status =
            ParseInternalKey(key, &parsed, db_options_.allow_data_in_errors);
        if (!pik_status.ok()) {
          ROCKS_LOG_ERROR(db_options_.info_log,
                          "Table #%" PRIu64 ": unparsable key - %s",
                          t->meta.fd.GetNumber(), pik_status.getState());
          continue;
        }

        counter++;

        status = t->meta.UpdateBoundaries(key, iter->value(), parsed.sequence,
                                          parsed.type);
        if (!status.ok()) {
          break;
        }
      }
      if (status.ok() && !iter->status().ok()) {
        status = iter->status();
      }
      delete iter;

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
      std::unique_ptr<FragmentedRangeTombstoneIterator> r_iter;
      status = table_cache_->GetRangeTombstoneIterator(
          ropts, cfd->internal_comparator(), t->meta,
          cfd->GetLatestMutableCFOptions(), &r_iter);

      if (r_iter) {
        r_iter->SeekToFirst();

        while (r_iter->Valid()) {
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

  Status AddTables() {
    // TODO: plumb Env::IOActivity, Env::IOPriority;
    const ReadOptions read_options;
    const WriteOptions write_options;

    std::unordered_map<uint32_t, std::vector<const TableInfo*>> cf_id_to_tables;
    SequenceNumber max_sequence = 0;
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
      for (const auto* table : cf_id_and_tables.second) {
        // TODO(opt): separate out into multiple levels
        const auto& meta = table->meta;
        dummy_edit.AddFile(
            0, meta.fd.GetNumber(), meta.fd.GetPathId(), meta.fd.GetFileSize(),
            meta.smallest, meta.largest, meta.fd.smallest_seqno,
            meta.fd.largest_seqno, meta.marked_for_compaction, meta.temperature,
            meta.oldest_blob_file_number, meta.oldest_ancestor_time,
            meta.file_creation_time, meta.epoch_number, meta.file_checksum,
            meta.file_checksum_func_name, meta.unique_id,
            meta.compensated_range_deletion_size, meta.tail_size,
            meta.user_defined_timestamps_persisted, meta.min_timestamp,
            meta.max_timestamp);
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
        edit.SetLogNumber(0);
        edit.SetNextFile(next_file_number_);
        edit.SetColumnFamily(cfd->GetID());
        for (int level = 0; level < dummy_vstorage.num_levels(); ++level) {
          for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
            edit.AddFile(level, *file_meta);
          }
        }

        // Release resources occupied by the dummy VersionStorageInfo
        for (int level = 0; level < dummy_vstorage.num_levels(); ++level) {
          for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
            file_meta->refs--;
            if (file_meta->refs <= 0) {
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
