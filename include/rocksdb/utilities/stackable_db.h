// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <map>
#include <memory>
#include <string>

#include "rocksdb/db.h"

#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#endif

namespace ROCKSDB_NAMESPACE {

// This class contains APIs to stack rocksdb wrappers.Eg. Stack TTL over base d
class StackableDB : public DB {
 public:
  // StackableDB take sole ownership of the underlying db.
  explicit StackableDB(DB* db) : db_(db) {}

  // StackableDB take shared ownership of the underlying db.
  explicit StackableDB(std::shared_ptr<DB> db)
      : db_(db.get()), shared_db_ptr_(db) {}

  // StackableDB take sole ownership of the underlying db.
  explicit StackableDB(std::unique_ptr<DB>&& db)
      : db_(db.get()), shared_db_ptr_(std::move(db)) {}

  // No copying allowed
  StackableDB(const StackableDB&) = delete;
  StackableDB& operator=(const StackableDB&) = delete;
  StackableDB(StackableDB&&) = delete;
  StackableDB& operator=(StackableDB&&) = delete;

  ~StackableDB() override {
    if (shared_db_ptr_ == nullptr) {
      delete db_;
    } else {
      assert(shared_db_ptr_.get() == db_);
    }
    db_ = nullptr;
  }

  Status Close() override { return db_->Close(); }

  virtual DB* GetBaseDB() { return db_; }

  DB* GetRootDB() override { return db_->GetRootDB(); }

  // Wrappers must explicitly opt in so coroutine reads do not bypass their
  // synchronous Get() and MultiGet() overrides.
  CoroDB* GetCoroDB() override { return nullptr; }

  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const std::string& column_family_name,
                            ColumnFamilyHandle** handle) override {
    return db_->CreateColumnFamily(options, column_family_name, handle);
  }

  Status CreateColumnFamilies(
      const ColumnFamilyOptions& options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles) override {
    return db_->CreateColumnFamilies(options, column_family_names, handles);
  }

  Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles) override {
    return db_->CreateColumnFamilies(column_families, handles);
  }

  Status DropColumnFamily(ColumnFamilyHandle* column_family) override {
    return db_->DropColumnFamily(column_family);
  }

  Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& column_families) override {
    return db_->DropColumnFamilies(column_families);
  }

  Status DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) override {
    return db_->DestroyColumnFamilyHandle(column_family);
  }

  using DB::Put;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& val) override {
    return db_->Put(options, column_family, key, val);
  }
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& ts, const Slice& val) override {
    return db_->Put(options, column_family, key, ts, val);
  }

  using DB::PutEntity;
  Status PutEntity(const WriteOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns) override {
    return db_->PutEntity(options, column_family, key, columns);
  }
  Status PutEntity(const WriteOptions& options, const Slice& key,
                   const AttributeGroups& attribute_groups) override {
    return db_->PutEntity(options, key, attribute_groups);
  }

  using DB::Get;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value,
             std::string* timestamp) override {
    return db_->Get(options, column_family, key, value, timestamp);
  }

  using DB::GetAsync;
  using DB::GetWithMetadata;
  Status GetWithMetadata(const ReadOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableSlice* value,
                         OutputMetadata* output_metadata) override {
    return db_->GetWithMetadata(options, column_family, key, value,
                                output_metadata);
  }

  using DB::GetEntity;

  Status GetEntity(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableWideColumns* columns) override {
    return db_->GetEntity(options, column_family, key, columns);
  }

  Status GetEntity(const ReadOptions& options, const Slice& key,
                   PinnableAttributeGroups* result) override {
    return db_->GetEntity(options, key, result);
  }

  using DB::GetEntityWithMetadata;
  Status GetEntityWithMetadata(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableWideColumns* columns,
                               OutputMetadata* output_metadata) override {
    return db_->GetEntityWithMetadata(options, column_family, key, columns,
                                      output_metadata);
  }

  using DB::GetEntityLazy;
  Status GetEntityLazy(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       LazyWideColumns* result) override {
    return db_->GetEntityLazy(options, column_family, key, result);
  }

  using DB::GetMergeOperands;
  Status GetMergeOperands(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          PinnableSlice* slice,
                          GetMergeOperandsOptions* get_merge_operands_options,
                          int* number_of_operands) override {
    return db_->GetMergeOperands(options, column_family, key, slice,
                                 get_merge_operands_options,
                                 number_of_operands);
  }

  using DB::MultiGet;
  void MultiGet(const ReadOptions& options, const size_t num_keys,
                ColumnFamilyHandle** column_families, const Slice* keys,
                PinnableSlice* values, std::string* timestamps,
                Status* statuses, const bool sorted_input = false) override {
    return db_->MultiGet(options, num_keys, column_families, keys, values,
                         timestamps, statuses, sorted_input);
  }

  using DB::MultiGetAsync;
  using DB::MultiGetWithMetadata;
  void MultiGetWithMetadata(const ReadOptions& options, const size_t num_keys,
                            ColumnFamilyHandle* const* column_families,
                            const Slice* keys, PinnableSlice* values,
                            Status* statuses,
                            MultiGetOutputMetadata* output_metadata,
                            const bool sorted_input = false) override {
    db_->MultiGetWithMetadata(options, num_keys, column_families, keys, values,
                              statuses, output_metadata, sorted_input);
  }

  using DB::MultiGetEntity;

  void MultiGetEntity(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, size_t num_keys,
                      const Slice* keys, PinnableWideColumns* results,
                      Status* statuses, bool sorted_input) override {
    db_->MultiGetEntity(options, column_family, num_keys, keys, results,
                        statuses, sorted_input);
  }

  void MultiGetEntity(const ReadOptions& options, size_t num_keys,
                      ColumnFamilyHandle** column_families, const Slice* keys,
                      PinnableWideColumns* results, Status* statuses,
                      bool sorted_input) override {
    db_->MultiGetEntity(options, num_keys, column_families, keys, results,
                        statuses, sorted_input);
  }

  void MultiGetEntity(const ReadOptions& options, size_t num_keys,
                      const Slice* keys,
                      PinnableAttributeGroups* results) override {
    db_->MultiGetEntity(options, num_keys, keys, results);
  }

  using DB::MultiGetEntityWithMetadata;
  void MultiGetEntityWithMetadata(const ReadOptions& options, size_t num_keys,
                                  ColumnFamilyHandle* const* column_families,
                                  const Slice* keys,
                                  PinnableWideColumns* results,
                                  Status* statuses,
                                  MultiGetOutputMetadata* output_metadata,
                                  bool sorted_input = false) override {
    db_->MultiGetEntityWithMetadata(options, num_keys, column_families, keys,
                                    results, statuses, output_metadata,
                                    sorted_input);
  }
  void MultiGetEntityWithMetadata(
      const ReadOptions& options, size_t num_keys, const Slice* keys,
      PinnableAttributeGroups* results,
      MultiGetOutputMetadata* output_metadata) override {
    db_->MultiGetEntityWithMetadata(options, num_keys, keys, results,
                                    output_metadata);
  }

  using DB::MultiGetEntityLazy;
  void MultiGetEntityLazy(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, size_t num_keys,
                          const Slice* keys, LazyWideColumnsBatch* result,
                          Status* statuses, bool sorted_input) override {
    db_->MultiGetEntityLazy(options, column_family, num_keys, keys, result,
                            statuses, sorted_input);
  }

  using DB::IngestExternalFile;
  Status IngestExternalFile(ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& external_files,
                            const IngestExternalFileOptions& options) override {
    return db_->IngestExternalFile(column_family, external_files, options);
  }

  using DB::IngestExternalFiles;
  Status IngestExternalFiles(
      const std::vector<IngestExternalFileArg>& args) override {
    return db_->IngestExternalFiles(args);
  }

  using DB::PrepareFileIngestion;
  Status PrepareFileIngestion(
      const std::vector<IngestExternalFileArg>& args,
      std::unique_ptr<FileIngestionHandle>* handle) override {
    return db_->PrepareFileIngestion(args, handle);
  }

  using DB::CommitFileIngestionHandles;
  Status CommitFileIngestionHandles(
      std::vector<std::unique_ptr<FileIngestionHandle>> handles) override {
    return db_->CommitFileIngestionHandles(std::move(handles));
  }

  using DB::CreateColumnFamilyWithImport;
  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& options, const std::string& column_family_name,
      const ImportColumnFamilyOptions& import_options,
      const ExportImportFilesMetaData& metadata,
      ColumnFamilyHandle** handle) override {
    return db_->CreateColumnFamilyWithImport(options, column_family_name,
                                             import_options, metadata, handle);
  }

  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& options, const std::string& column_family_name,
      const ImportColumnFamilyOptions& import_options,
      const std::vector<const ExportImportFilesMetaData*>& metadatas,
      ColumnFamilyHandle** handle) override {
    return db_->CreateColumnFamilyWithImport(options, column_family_name,
                                             import_options, metadatas, handle);
  }

  using DB::ClipColumnFamily;
  Status ClipColumnFamily(ColumnFamilyHandle* column_family,
                          const Slice& begin_key,
                          const Slice& end_key) override {
    return db_->ClipColumnFamily(column_family, begin_key, end_key);
  }

  using DB::VerifyFileChecksums;
  Status VerifyFileChecksums(const ReadOptions& read_opts) override {
    return db_->VerifyFileChecksums(read_opts);
  }

  Status VerifyChecksum() override { return db_->VerifyChecksum(); }

  Status VerifyChecksum(const ReadOptions& options) override {
    return db_->VerifyChecksum(options);
  }

  using DB::KeyMayExist;
  bool KeyMayExist(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   std::string* value, bool* value_found = nullptr) override {
    return db_->KeyMayExist(options, column_family, key, value, value_found);
  }

  using DB::Delete;
  Status Delete(const WriteOptions& wopts, ColumnFamilyHandle* column_family,
                const Slice& key) override {
    return db_->Delete(wopts, column_family, key);
  }
  Status Delete(const WriteOptions& wopts, ColumnFamilyHandle* column_family,
                const Slice& key, const Slice& ts) override {
    return db_->Delete(wopts, column_family, key, ts);
  }

  using DB::SingleDelete;
  Status SingleDelete(const WriteOptions& wopts,
                      ColumnFamilyHandle* column_family,
                      const Slice& key) override {
    return db_->SingleDelete(wopts, column_family, key);
  }
  Status SingleDelete(const WriteOptions& wopts,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts) override {
    return db_->SingleDelete(wopts, column_family, key, ts);
  }

  using DB::DeleteRange;
  Status DeleteRange(const WriteOptions& wopts,
                     ColumnFamilyHandle* column_family, const Slice& start_key,
                     const Slice& end_key) override {
    return db_->DeleteRange(wopts, column_family, start_key, end_key);
  }

  using DB::Merge;
  Status Merge(const WriteOptions& options, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& value) override {
    return db_->Merge(options, column_family, key, value);
  }
  Status Merge(const WriteOptions& options, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& ts, const Slice& value) override {
    return db_->Merge(options, column_family, key, ts, value);
  }

  Status Write(const WriteOptions& opts, WriteBatch* updates) override {
    return db_->Write(opts, updates);
  }

  using DB::NewIterator;
  Iterator* NewIterator(const ReadOptions& opts,
                        ColumnFamilyHandle* column_family) override {
    return db_->NewIterator(opts, column_family);
  }

  Status NewIterators(const ReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override {
    return db_->NewIterators(options, column_families, iterators);
  }

  Status NewIterators(const std::vector<ReadOptions>& read_options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override {
    return db_->NewIterators(read_options, column_families, iterators);
  }

  using DB::NewCoalescingIterator;
  std::unique_ptr<Iterator> NewCoalescingIterator(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override {
    return db_->NewCoalescingIterator(options, column_families);
  }

  using DB::NewAttributeGroupIterator;
  std::unique_ptr<AttributeGroupIterator> NewAttributeGroupIterator(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override {
    return db_->NewAttributeGroupIterator(options, column_families);
  }

  using DB::NewMultiScan;
  std::unique_ptr<MultiScan> NewMultiScan(
      const ReadOptions& opts, ColumnFamilyHandle* column_family,
      const MultiScanArgs& scan_opts) override {
    return db_->NewMultiScan(opts, column_family, scan_opts);
  }

  const Snapshot* GetSnapshot() override { return db_->GetSnapshot(); }

  void ReleaseSnapshot(const Snapshot* snapshot) override {
    return db_->ReleaseSnapshot(snapshot);
  }

  using DB::GetMapProperty;
  using DB::GetProperty;
  bool GetProperty(ColumnFamilyHandle* column_family, const Slice& property,
                   std::string* value) override {
    return db_->GetProperty(column_family, property, value);
  }
  bool GetMapProperty(ColumnFamilyHandle* column_family, const Slice& property,
                      std::map<std::string, std::string>* value) override {
    return db_->GetMapProperty(column_family, property, value);
  }

  using DB::GetIntProperty;
  bool GetIntProperty(ColumnFamilyHandle* column_family, const Slice& property,
                      uint64_t* value) override {
    return db_->GetIntProperty(column_family, property, value);
  }

  using DB::GetAggregatedIntProperty;
  bool GetAggregatedIntProperty(const Slice& property,
                                uint64_t* value) override {
    return db_->GetAggregatedIntProperty(property, value);
  }

  using DB::GetApproximateSizes;
  Status GetApproximateSizes(const SizeApproximationOptions& options,
                             ColumnFamilyHandle* column_family, const Range* r,
                             int n, uint64_t* sizes) override {
    return db_->GetApproximateSizes(options, column_family, r, n, sizes);
  }

  using DB::GetApproximateMemTableStats;
  void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                   const Range& range, uint64_t* const count,
                                   uint64_t* const size) override {
    return db_->GetApproximateMemTableStats(column_family, range, count, size);
  }

  using DB::CompactRange;
  Status CompactRange(const CompactRangeOptions& options,
                      ColumnFamilyHandle* column_family, const Slice* begin,
                      const Slice* end) override {
    return db_->CompactRange(options, column_family, begin, end);
  }

  using DB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override {
    return db_->CompactFiles(compact_options, column_family, input_file_names,
                             output_level, output_path_id, output_file_names,
                             compaction_job_info);
  }

  Status PauseBackgroundWork() override { return db_->PauseBackgroundWork(); }
  Status ContinueBackgroundWork() override {
    return db_->ContinueBackgroundWork();
  }

  Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override {
    return db_->EnableAutoCompaction(column_family_handles);
  }

  void EnableManualCompaction() override {
    return db_->EnableManualCompaction();
  }
  void DisableManualCompaction() override {
    return db_->DisableManualCompaction();
  }
  void AbortAllCompactions() override { return db_->AbortAllCompactions(); }
  void ResumeAllCompactions() override { return db_->ResumeAllCompactions(); }
  void AbortCompactions(ColumnFamilyHandle* column_family) override {
    return db_->AbortCompactions(column_family);
  }
  void ResumeCompactions(ColumnFamilyHandle* column_family) override {
    return db_->ResumeCompactions(column_family);
  }

  Status WaitForCompact(
      const WaitForCompactOptions& wait_for_compact_options) override {
    return db_->WaitForCompact(wait_for_compact_options);
  }

  using DB::NumberLevels;
  int NumberLevels(ColumnFamilyHandle* column_family) override {
    return db_->NumberLevels(column_family);
  }

  using DB::Level0StopWriteTrigger;
  int Level0StopWriteTrigger(ColumnFamilyHandle* column_family) override {
    return db_->Level0StopWriteTrigger(column_family);
  }

  const std::string& GetName() const override { return db_->GetName(); }

  Env* GetEnv() const override { return db_->GetEnv(); }

  FileSystem* GetFileSystem() const override { return db_->GetFileSystem(); }

  using DB::GetOptions;
  Options GetOptions(ColumnFamilyHandle* column_family) const override {
    return db_->GetOptions(column_family);
  }

  using DB::GetDBOptions;
  DBOptions GetDBOptions() const override { return db_->GetDBOptions(); }

  using DB::Flush;
  Status Flush(const FlushOptions& fopts,
               ColumnFamilyHandle* column_family) override {
    return db_->Flush(fopts, column_family);
  }
  Status Flush(
      const FlushOptions& fopts,
      const std::vector<ColumnFamilyHandle*>& column_families) override {
    return db_->Flush(fopts, column_families);
  }

  Status SyncWAL() override { return db_->SyncWAL(); }

  using DB::FlushWAL;
  Status FlushWAL(bool sync) override { return db_->FlushWAL(sync); }
  Status FlushWAL(const FlushWALOptions& options) override {
    return db_->FlushWAL(options);
  }

  Status LockWAL() override { return db_->LockWAL(); }

  Status UnlockWAL() override { return db_->UnlockWAL(); }

  Status DisableFileDeletions() override { return db_->DisableFileDeletions(); }

  Status EnableFileDeletions() override { return db_->EnableFileDeletions(); }

  void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) override {
    db_->GetLiveFilesMetaData(metadata);
  }

  Status GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) override {
    return db_->GetLiveFilesChecksumInfo(checksum_list);
  }

  Status GetLiveFilesStorageInfo(
      const LiveFilesStorageInfoOptions& opts,
      std::vector<LiveFileStorageInfo>* files) override {
    return db_->GetLiveFilesStorageInfo(opts, files);
  }

  Status GetPreparedFileInfoForExternalSstIngestion(
      const std::string& file_path,
      std::shared_ptr<const PreparedFileInfo>* file_info) override {
    return db_->GetPreparedFileInfoForExternalSstIngestion(file_path,
                                                           file_info);
  }

  void GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                               ColumnFamilyMetaData* cf_meta) override {
    db_->GetColumnFamilyMetaData(column_family, cf_meta);
  }

  void GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                               const GetColumnFamilyMetaDataOptions& options,
                               ColumnFamilyMetaData* metadata) override {
    db_->GetColumnFamilyMetaData(column_family, options, metadata);
  }

  using DB::StartBlockCacheTrace;
  Status StartBlockCacheTrace(
      const TraceOptions& trace_options,
      std::unique_ptr<TraceWriter>&& trace_writer) override {
    return db_->StartBlockCacheTrace(trace_options, std::move(trace_writer));
  }

  Status StartBlockCacheTrace(
      const BlockCacheTraceOptions& options,
      std::unique_ptr<BlockCacheTraceWriter>&& trace_writer) override {
    return db_->StartBlockCacheTrace(options, std::move(trace_writer));
  }

  using DB::EndBlockCacheTrace;
  Status EndBlockCacheTrace() override { return db_->EndBlockCacheTrace(); }

  using DB::StartIOTrace;
  Status StartIOTrace(const TraceOptions& options,
                      std::unique_ptr<TraceWriter>&& trace_writer) override {
    return db_->StartIOTrace(options, std::move(trace_writer));
  }

  using DB::EndIOTrace;
  Status EndIOTrace() override { return db_->EndIOTrace(); }

  using DB::StartTrace;
  Status StartTrace(const TraceOptions& options,
                    std::unique_ptr<TraceWriter>&& trace_writer) override {
    return db_->StartTrace(options, std::move(trace_writer));
  }

  using DB::EndTrace;
  Status EndTrace() override { return db_->EndTrace(); }

  using DB::NewDefaultReplayer;
  Status NewDefaultReplayer(const std::vector<ColumnFamilyHandle*>& handles,
                            std::unique_ptr<TraceReader>&& reader,
                            std::unique_ptr<Replayer>* replayer) override {
    return db_->NewDefaultReplayer(handles, std::move(reader), replayer);
  }

  Status GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs,
                      bool flush_memtable = true) override {
    return db_->GetLiveFiles(vec, mfs, flush_memtable);
  }

  SequenceNumber GetLatestSequenceNumber() const override {
    return db_->GetLatestSequenceNumber();
  }

  Status IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                  std::string ts_low) override {
    return db_->IncreaseFullHistoryTsLow(column_family, ts_low);
  }

  Status GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                             std::string* ts_low) override {
    return db_->GetFullHistoryTsLow(column_family, ts_low);
  }

  Status GetNewestUserDefinedTimestamp(ColumnFamilyHandle* column_family,
                                       std::string* newest_timestamp) override {
    return db_->GetNewestUserDefinedTimestamp(column_family, newest_timestamp);
  }

  Status GetSortedWalFiles(VectorWalPtr& files) override {
    return db_->GetSortedWalFiles(files);
  }

  Status GetCurrentWalFile(
      std::unique_ptr<WalFile>* current_wal_file) override {
    return db_->GetCurrentWalFile(current_wal_file);
  }

  Status GetCreationTimeOfOldestFile(uint64_t* creation_time) override {
    return db_->GetCreationTimeOfOldestFile(creation_time);
  }

  Status GetDbIdentity(std::string& identity) const override {
    return db_->GetDbIdentity(identity);
  }

  Status GetDbSessionId(std::string& session_id) const override {
    return db_->GetDbSessionId(session_id);
  }

  using DB::SetOptions;
  Status SetOptions(
      const std::unordered_map<ColumnFamilyHandle*,
                               std::unordered_map<std::string, std::string>>&
          column_families_opts_map) override {
    return db_->SetOptions(column_families_opts_map);
  }

  Status SetDBOptions(const std::unordered_map<std::string, std::string>&
                          new_options) override {
    return db_->SetDBOptions(new_options);
  }

  using DB::ResetStats;
  Status ResetStats() override { return db_->ResetStats(); }

  using DB::GetPropertiesOfAllTables;
  Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                  TablePropertiesCollection* props) override {
    return db_->GetPropertiesOfAllTables(column_family, props);
  }

  using DB::GetPropertiesOfTablesInRange;
  Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) override {
    return db_->GetPropertiesOfTablesInRange(column_family, range, n, props);
  }

  using DB::GetPropertiesOfTablesByLevel;
  Status GetPropertiesOfTablesByLevel(
      ColumnFamilyHandle* column_family,
      std::vector<std::unique_ptr<TablePropertiesCollection>>* props_by_level)
      override {
    return db_->GetPropertiesOfTablesByLevel(column_family, props_by_level);
  }

  Status GetUpdatesSince(
      SequenceNumber seq_number, std::unique_ptr<WalIterator>* iter,
      const WalIterator::ReadOptions& read_options) override {
    return db_->GetUpdatesSince(seq_number, iter, read_options);
  }

  Status SuggestCompactRange(ColumnFamilyHandle* column_family,
                             const Slice* begin, const Slice* end) override {
    return db_->SuggestCompactRange(column_family, begin, end);
  }

  Status PromoteL0(ColumnFamilyHandle* column_family,
                   int target_level) override {
    return db_->PromoteL0(column_family, target_level);
  }

  ColumnFamilyHandle* DefaultColumnFamily() const override {
    return db_->DefaultColumnFamily();
  }

  Status TryCatchUpWithPrimary() override {
    return db_->TryCatchUpWithPrimary();
  }

  Status Resume() override { return db_->Resume(); }

 protected:
  DB* db_;
  std::shared_ptr<DB> shared_db_ptr_;
};

}  // namespace ROCKSDB_NAMESPACE
