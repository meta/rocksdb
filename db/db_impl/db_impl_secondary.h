//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "logging/logging.h"
#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

// A wrapper class to hold log reader, log reporter, log status.
class LogReaderContainer {
 public:
  LogReaderContainer()
      : reader_(nullptr), reporter_(nullptr), status_(nullptr) {}
  LogReaderContainer(Env* env, std::shared_ptr<Logger> info_log,
                     std::string fname,
                     std::unique_ptr<SequentialFileReader>&& file_reader,
                     uint64_t log_number) {
    LogReporter* reporter = new LogReporter();
    status_ = new Status();
    reporter->env = env;
    reporter->info_log = info_log.get();
    reporter->fname = std::move(fname);
    reporter->status = status_;
    reporter_ = reporter;
    // We intentially make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    reader_ = new log::FragmentBufferedReader(info_log, std::move(file_reader),
                                              reporter, true /*checksum*/,
                                              log_number);
  }
  // No copying allowed
  LogReaderContainer(const LogReaderContainer&) = delete;
  LogReaderContainer& operator=(const LogReaderContainer&) = delete;
  LogReaderContainer(LogReaderContainer&&) = delete;
  LogReaderContainer& operator=(LogReaderContainer&&) = delete;
  log::FragmentBufferedReader* reader_;
  log::Reader::Reporter* reporter_;
  Status* status_;
  ~LogReaderContainer() {
    delete reader_;
    delete reporter_;
    delete status_;
  }

 private:
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    std::string fname;
    Status* status;  // nullptr if immutable_db_options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s,
                    uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      ROCKS_LOG_WARN(info_log, "%s%s: dropping %d bytes; %s",
                     (this->status == nullptr ? "(ignoring error) " : ""),
                     fname.c_str(), static_cast<int>(bytes),
                     s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) {
        *this->status = s;
      }
    }
  };
};

// The secondary instance shares access to the storage as the primary.
// The secondary is able to read and replay changes described in both the
// MANIFEST and the WAL files without coordination with the primary.
// The secondary instance can be opened using `DB::OpenAsSecondary`. After
// that, it can call `DBImplSecondary::TryCatchUpWithPrimary` to make best
// effort attempts to catch up with the primary.
// TODO: Share common structure with CompactedDBImpl and DBImplReadOnly
class DBImplSecondary : public DBImpl {
 public:
  DBImplSecondary(const DBOptions& options, const std::string& dbname,
                  std::string secondary_path);
  ~DBImplSecondary() override;

  // Recover by replaying MANIFEST only. Also initialize manifest_reader_
  // to facilitate future operations. WAL recovery, if needed, is done
  // separately after opening (see DB::OpenAsSecondary).
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only, bool error_if_wal_file_exists,
                 bool error_if_data_exists_in_wals, bool is_retry = false,
                 uint64_t* = nullptr, RecoveryContext* recovery_ctx = nullptr,
                 bool* can_retry = nullptr) override;

  // Can return IOError due to files being deleted by the primary. To avoid
  // IOError in this case, application can coordinate between primary and
  // secondaries so that primary will not delete files that are currently being
  // used by the secondaries. The application can also provide a custom FS/Env
  // implementation so that files will remain present until all primary and
  // secondaries indicate that they can be deleted. As a partial hacky
  // workaround, the secondaries can be opened with `max_open_files=-1` so that
  // it eagerly keeps all talbe files open and is able to access the contents of
  // deleted files via prior open fd.
  using DBImpl::GetImpl;
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(Status, GetImpl, const ReadOptions& options,
                                  const Slice& key,
                                  GetImplOptions& get_impl_options);

  using DBImpl::MultiGetWithMetadata;
  void MultiGetWithMetadata(const ReadOptions& options, const size_t num_keys,
                            ColumnFamilyHandle* const* column_families,
                            const Slice* keys, PinnableSlice* values,
                            Status* statuses,
                            MultiGetOutputMetadata* output_metadata,
                            const bool sorted_input = false) override;

  using DBImpl::MultiGetEntityWithMetadata;
  void MultiGetEntityWithMetadata(const ReadOptions& options, size_t num_keys,
                                  ColumnFamilyHandle* const* column_families,
                                  const Slice* keys,
                                  PinnableWideColumns* results,
                                  Status* statuses,
                                  MultiGetOutputMetadata* output_metadata,
                                  bool sorted_input = false) override;
  void MultiGetEntityWithMetadata(
      const ReadOptions& options, size_t num_keys, const Slice* keys,
      PinnableAttributeGroups* results,
      MultiGetOutputMetadata* output_metadata) override;

  using DBImpl::NewIterator;
  // Operations on the created iterators can return IOError due to files being
  // deleted by the primary. To avoid IOError in this case, application can
  // coordinate between primary and secondaries so that primary will not delete
  // files that are currently being used by the secondaries. The application can
  // also provide a custom FS/Env implementation so that files will remain
  // present until all primary and secondaries indicate that they can be
  // deleted. As a partial hacky workaround, the secondaries can be opened with
  // `max_open_files=-1` so that it eagerly keeps all talbe files open and is
  // able to access the contents of deleted files via prior open fd.
  Iterator* NewIterator(const ReadOptions& _read_options,
                        ColumnFamilyHandle* column_family) override;

  ArenaWrappedDBIter* NewIteratorImpl(const ReadOptions& read_options,
                                      ColumnFamilyHandleImpl* cfh,
                                      SuperVersion* sv, SequenceNumber snapshot,
                                      ReadCallback* read_callback,
                                      bool expose_blob_index = false,
                                      bool allow_refresh = true);

  Status NewIterators(const ReadOptions& _read_options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override;
  Status NewIterators(const std::vector<ReadOptions>& read_options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override;

  const Snapshot* GetSnapshot() override;

  using DBImpl::Put;
  Status Put(const WriteOptions& /*options*/,
             ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
             const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::PutEntity;
  Status PutEntity(const WriteOptions& /* options */,
                   ColumnFamilyHandle* /* column_family */,
                   const Slice& /* key */,
                   const WideColumns& /* columns */) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }
  Status PutEntity(const WriteOptions& /* options */, const Slice& /* key */,
                   const AttributeGroups& /* attribute_groups */) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::Merge;
  Status Merge(const WriteOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::Delete;
  Status Delete(const WriteOptions& /*options*/,
                ColumnFamilyHandle* /*column_family*/,
                const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SingleDelete;
  Status SingleDelete(const WriteOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status Write(const WriteOptions& /*options*/,
               WriteBatch* /*updates*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::CompactRange;
  Status CompactRange(const CompactRangeOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice* /*begin*/, const Slice* /*end*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& /*compact_options*/,
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*input_file_names*/,
      const int /*output_level*/, const int /*output_path_id*/ = -1,
      std::vector<std::string>* const /*output_file_names*/ = nullptr,
      CompactionJobInfo* /*compaction_job_info*/ = nullptr) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status DisableFileDeletions() override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status EnableFileDeletions() override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  Status GetLiveFiles(std::vector<std::string>& ret,
                      uint64_t* manifest_file_size,
                      bool /*flush_memtable*/) override {
    return DBImpl::GetLiveFiles(ret, manifest_file_size,
                                false /* flush_memtable */);
  }

  using DBImpl::Flush;
  Status Flush(const FlushOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SetDBOptions;
  Status SetDBOptions(const std::unordered_map<std::string, std::string>&
                      /*options_map*/) override {
    // Currently not supported because changing certain options may cause
    // flush/compaction.
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SetOptions;
  Status SetOptions(
      const std::unordered_map<ColumnFamilyHandle*,
                               std::unordered_map<std::string, std::string>>&
      /*column_families_opts_map*/) override {
    // Currently not supported because changing certain options may cause
    // flush/compaction and/or write to MANIFEST.
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DBImpl::SyncWAL;
  Status SyncWAL() override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DB::IngestExternalFile;
  Status IngestExternalFile(
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*external_files*/,
      const IngestExternalFileOptions& /*ingestion_options*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DB::PrepareFileIngestion;
  Status PrepareFileIngestion(
      const std::vector<IngestExternalFileArg>& /*args*/,
      std::unique_ptr<FileIngestionHandle>* /*handle*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  using DB::CommitFileIngestionHandles;
  Status CommitFileIngestionHandles(
      std::vector<std::unique_ptr<FileIngestionHandle>> /*handles*/) override {
    return Status::NotSupported("Not supported operation in secondary mode.");
  }

  // Try to catch up with the primary by reading as much as possible from the
  // log files until there is nothing more to read or encounters an error. If
  // the amount of information in the log files to process is huge, this
  // method can take long time due to all the I/O and CPU costs.
  Status TryCatchUpWithPrimary() override;

  // Try to find log reader using log_number from log_readers_ map, initialize
  // if it doesn't exist
  Status MaybeInitLogReader(uint64_t log_number,
                            log::FragmentBufferedReader** log_reader);

#ifndef NDEBUG
  Status TEST_CompactWithoutInstallation(const OpenAndCompactOptions& options,
                                         ColumnFamilyHandle* cfh,
                                         const CompactionServiceInput& input,
                                         CompactionServiceResult* result) {
    return CompactWithoutInstallation(options, cfh, input, result);
  }
#endif  // NDEBUG

 protected:
  Status CloseImpl() override;

  Status FlushForGetLiveFiles(bool /*force_atomic_flush*/) override {
    // No-op for read-only DB
    return Status::OK();
  }

  bool OwnTablesAndLogs() const override {
    // Currently, the secondary instance does not own the database files. It
    // simply opens the files of the primary instance and tracks their file
    // descriptors until they become obsolete. In the future, the secondary may
    // create links to database files. OwnTablesAndLogs will return true then.
    return false;
  }

  // Seals `cfd`'s active memtable: adds it to the immutable memtable list and
  // puts a fresh, empty active memtable in its place. This is the counterpart
  // of DBImpl::SwitchMemtable(), which cannot be used here because there is no
  // writer queue and no WAL of this instance's own.
  //
  // `next_log_number` is recorded on the sealed memtable. A subsequent
  // MemTableList::RemoveOldMemTables(log_number) call considers the sealed
  // memtable collectible once `next_log_number <= log_number`, so pass a log
  // number above every WAL whose entries the memtable holds.
  // `new_mem_earliest_seq` becomes the replacement memtable's earliest
  // sequence number.
  //
  // REQUIRES: mutex_ held
  void SealActiveMemtable(ColumnFamilyData* cfd, uint64_t next_log_number,
                          SequenceNumber new_mem_earliest_seq,
                          JobContext* job_context);

  std::unique_ptr<log::FragmentBufferedReader> manifest_reader_;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter_;
  std::unique_ptr<Status> manifest_reader_status_;

  // Publishes a view of every initialized, non-dropped column family while
  // mutex_ protects their SuperVersions. The previous view is retained for
  // cleanup outside mutex_ and outside reader threads. No-op once the view has
  // been torn down for close.
  // REQUIRES: mutex_ held
  void PublishSecondaryReadView();

  // Reclaims retired views that have no readers. This must be called without
  // mutex_ because releasing a view's final SuperVersion can lock it.
  void CleanupRetiredSecondaryReadViews();

 private:
  friend class DB;

  struct SecondaryReadView {
    using SuperVersions = std::unordered_map<uint32_t, SuperVersion*>;

    SecondaryReadView(DBImplSecondary* db_arg, SequenceNumber sequence_arg)
        : db(db_arg), sequence(sequence_arg) {}
    ~SecondaryReadView();
    SecondaryReadView(const SecondaryReadView&) = delete;
    SecondaryReadView& operator=(const SecondaryReadView&) = delete;
    SecondaryReadView(SecondaryReadView&&) = delete;
    SecondaryReadView& operator=(SecondaryReadView&&) = delete;

    DBImplSecondary* db;
    SequenceNumber sequence;
    SuperVersions super_versions;
  };

  void RetireSecondaryReadView(
      std::shared_ptr<const SecondaryReadView> read_view);
  // REQUIRES: retired_secondary_read_views_mutex_ held
  bool HasReclaimableSecondaryReadView() const;
  // REQUIRES: retired_secondary_read_views_mutex_ held
  bool AllSecondaryReadViewsReclaimed() const;
  bool ReclaimRetiredSecondaryReadViews();
  void ReclaimRetiredSecondaryReadViewsLoop();
  void DrainRetiredSecondaryReadViews();
  void StopSecondaryReadViewReclaimer();
  void ReleaseSecondaryReadView(
      std::shared_ptr<const SecondaryReadView>* read_view);
  void ResetSecondaryReadView();

  Status NewIteratorsImpl(
      const std::vector<ReadOptions>& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators);

  // No copying allowed
  DBImplSecondary(const DBImplSecondary&) = delete;
  void operator=(const DBImplSecondary&) = delete;
  DBImplSecondary(DBImplSecondary&&) = delete;
  void operator=(DBImplSecondary&&) = delete;

  using DBImpl::Recover;

  Status FindAndRecoverLogFiles(
      std::unordered_set<ColumnFamilyData*>* cfds_changed,
      JobContext* job_context);
  Status FindNewLogNumbers(std::vector<uint64_t>* logs);
  // Replays WALs after manifest recovery, refreshes log_readers_ as needed, and
  // drops recovered transactions the primary resolved. Cleanup runs even when
  // `log_numbers` is empty.
  // REQUIRES: log_numbers are sorted in ascending order
  Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                         SequenceNumber* next_sequence,
                         std::unordered_set<ColumnFamilyData*>* cfds_changed,
                         JobContext* job_context);

  // Records `log_number` as the newest WAL whose entries `cf_id`'s active
  // memtable may hold, never lowering what is already recorded. Returns what
  // was recorded before this call, or nullopt if nothing was.
  //
  // REQUIRES: mutex_ held
  std::optional<uint64_t> RecordCurrentLog(uint32_t cf_id, uint64_t log_number);

  // Seals the column family's active memtable if everything it holds is
  // already readable through the installed Version, so that a following
  // RemoveOldMemTables(installed_log_number) call drops it. Returns whether the
  // memtable was sealed; in the common case it is not, and this is a no-op.
  //
  // `installed_log_number` is the column family's log number as of the Version
  // most recently installed for it, from
  // ReactiveVersionSet::GetInstalledVersionLogNumber(). It gates the seal, and
  // the sealed memtable's next log number is at most that value.
  //
  // REQUIRES: mutex_ held
  // REQUIRES: installed_log_number <= cfd->GetLogNumber()
  // REQUIRES: cf_id_to_current_log_[cfd->GetID()] is at least as new as the
  // newest WAL whose entries the active memtable holds, i.e. any WAL replay for
  // this round has run. Too low a value drops a memtable the installed Version
  // does not cover; too high only retains it for longer. RecoverLogFiles()
  // records both before and after each insert to keep that true; see there.
  bool MaybeSealFullyFlushedActiveMemtable(ColumnFamilyData* cfd,
                                           uint64_t installed_log_number,
                                           JobContext* job_context);

  // Warns while `installed_log_number` lags behind cfd->GetLogNumber() and the
  // column family is holding immutable memtables that RemoveOldMemTables()
  // cannot collect as a result. Nothing on a secondary flushes them, so their
  // number grows by one per WAL switch until the primary's flushed files become
  // readable or the instance is reopened.
  //
  // Warns only when what it would report has changed, since the condition can
  // persist across arbitrarily many catch-up calls.
  //
  // REQUIRES: mutex_ held
  void MaybeWarnAboutRetainedMemtables(ColumnFamilyData* cfd,
                                       uint64_t installed_log_number);

  // Drops recovered transactions the primary resolved and flushed.
  //
  // REQUIRES: mutex_ held
  // REQUIRES: every WAL reader for the round opened successfully
  // REQUIRES: called before replaying records from the round
  void DeleteResolvedRecoveredTransactions();

  // Run compaction without installation, the output files will be placed in the
  // secondary DB path. The LSM tree won't be changed, the secondary DB is still
  // in read-only mode.
  Status CompactWithoutInstallation(const OpenAndCompactOptions& options,
                                    ColumnFamilyHandle* cfh,
                                    const CompactionServiceInput& input,
                                    CompactionServiceResult* result);

 private:
  // Holds results of compaction progress files and output files from a single
  // directory scan
  struct CompactionProgressFilesScan {
    // The latest (newest) progress file filename
    std::optional<std::string> latest_progress_filename;
    uint64_t latest_progress_timestamp = 0;

    // Older progress file filenames (to be deleted)
    autovector<std::string> old_progress_filenames;

    // Temporary progress file filenames (to be deleted)
    autovector<std::string> temp_progress_filenames;

    // All output file numbers - for cleanup optimization
    std::vector<uint64_t> table_file_numbers;
    std::vector<uint64_t> blob_file_numbers;

    bool HasLatestProgressFile() const {
      return latest_progress_filename.has_value();
    }

    void Clear() {
      latest_progress_filename.reset();
      latest_progress_timestamp = 0;
      old_progress_filenames.clear();
      temp_progress_filenames.clear();
      table_file_numbers.clear();
      blob_file_numbers.clear();
    }
  };

  Status InitializeCompactionWorkspace(
      bool allow_resumption, bool resumption_requested,
      std::unique_ptr<FSDirectory>* output_dir,
      std::unique_ptr<log::Writer>* compaction_progress_writer);

  Status PrepareCompactionProgressState();

  Status ScanCompactionProgressFiles(CompactionProgressFilesScan* scan_result);

  Status DeleteCompactionProgressFiles(
      const std::vector<std::string>& filenames);

  Status CleanupOldAndTemporaryCompactionProgressFiles(
      bool preserve_latest, const CompactionProgressFilesScan& scan_result);

  Status LoadCompactionProgressAndCleanupExtraOutputFiles(
      const std::string& compaction_progress_file_path,
      const CompactionProgressFilesScan& scan_result);

  Status ParseCompactionProgressFile(
      const std::string& compaction_progress_file_path,
      CompactionProgress* compaction_progress);

  Status HandleInvalidOrNoCompactionProgress(
      const std::optional<std::string>& compaction_progress_file_path,
      const CompactionProgressFilesScan& scan_result);

  Status CleanupPhysicalCompactionOutputFiles(
      bool preserve_tracked_files,
      const CompactionProgressFilesScan& scan_result);

  Status FinalizeCompactionProgressWriter(
      std::unique_ptr<log::Writer>* compaction_progress_writer);

  Status CreateCompactionProgressWriter(
      const std::string& file_path,
      std::unique_ptr<log::Writer>* compaction_progress_writer);

  Status PersistInitialCompactionProgress(
      log::Writer* compaction_progress_writer,
      CompactionProgress* compaction_progress);

  Status RenameCompactionProgressFile(const std::string& temp_file_path,
                                      std::string* final_file_path);

  Status HandleCompactionProgressWriterCreationFailure(
      const std::string& temp_file_path, const std::string& final_file_path,
      std::unique_ptr<log::Writer>* compaction_progress_writer);

  uint64_t CalculateResumedCompactionBytes(
      const CompactionProgress& compaction_progress) const;

  // Internal helper for opening a secondary instance. Recover() replays
  // MANIFEST only. When recover_wal is true, WAL files are also replayed
  // (needed by DB::OpenAsSecondary). When false, WAL replay is skipped
  // (used by DB::OpenAndCompact which only needs LSM state).
  //
  // When trust_manifest_recovery is true (only DB::OpenAndCompact), MANIFEST
  // recovery trusts the manifest and does not stat/open SST or blob files, so
  // only the compaction's (deletion-protected) input files are opened later, on
  // demand. Must be false for normal secondary opens.
  static Status OpenAsSecondaryImpl(
      const DBOptions& db_options, const std::string& dbname,
      const std::string& secondary_path,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr,
      bool recover_wal, bool trust_manifest_recovery);

  // Cache log readers for each log number, used for continue WAL replay
  // after recovery
  std::map<uint64_t, std::unique_ptr<LogReaderContainer>> log_readers_;

  // The newest WAL whose entries each column family's active memtable may
  // hold, keyed by column family id. An upper bound, not necessarily a WAL the
  // memtable took entries from. Column family ids are never reused, so an entry
  // left behind by a dropped column family can never be mistaken for a new
  // column family's.
  std::unordered_map<uint32_t, uint64_t> cf_id_to_current_log_;

  // The installed Version's log number and retained memtable count last
  // reported by MaybeWarnAboutRetainedMemtables() for each column family.
  std::unordered_map<uint32_t, std::pair<uint64_t, int>>
      cf_id_to_retention_warning_;

  // Mutated only under mutex_, so that load-then-store stays atomic between
  // publication and teardown. Readers load it without mutex_, so every access
  // must still go through AtomicSharedPtrLoad/Store.
  std::shared_ptr<const SecondaryReadView> secondary_read_view_;
  // Set by ResetSecondaryReadView() so a later catch-up cannot publish a view
  // that would outlive the drain close waits on.
  // Guarded by mutex_, which every mutation of secondary_read_view_ holds.
  bool secondary_read_view_closed_ = false;
  std::mutex retired_secondary_read_views_mutex_;
  std::condition_variable retired_secondary_read_views_cv_;
  std::vector<std::shared_ptr<const SecondaryReadView>>
      retired_secondary_read_views_;
  size_t secondary_read_view_reclaims_in_flight_ = 0;
  bool secondary_read_view_reclaimer_stopping_ = false;
  std::unique_ptr<port::Thread> secondary_read_view_reclaimer_;

  const std::string secondary_path_;

  CompactionProgress compaction_progress_;
};

}  // namespace ROCKSDB_NAMESPACE
