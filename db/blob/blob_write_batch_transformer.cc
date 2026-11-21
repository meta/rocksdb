//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_write_batch_transformer.h"

#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/column_family.h"
#include "db/version_set.h"
#include "db/wide/wide_column_serialization.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "port/likely.h"

namespace ROCKSDB_NAMESPACE {

BlobWriteBatchTransformer::BlobWriteBatchTransformer(
    const BlobPartitionManagerProvider& partition_mgr_provider,
    WriteBatch* output_batch,
    const BlobDirectWriteSettingsProvider& settings_provider,
    const WriteOptions& write_options)
    : partition_mgr_provider_(partition_mgr_provider),
      output_batch_(output_batch),
      settings_provider_(settings_provider),
      write_options_(write_options) {
  assert(partition_mgr_provider_);
  assert(output_batch_);
  assert(settings_provider_);
}

Status BlobWriteBatchTransformer::TransformBatch(
    const WriteOptions& write_options, WriteBatch* input_batch,
    WriteBatch* output_batch,
    const BlobPartitionManagerProvider& partition_mgr_provider,
    const BlobDirectWriteSettingsProvider& settings_provider, bool* transformed,
    std::vector<BlobFilePartitionManager*>* used_managers,
    std::vector<RollbackInfo>* rollback_infos) {
  assert(input_batch);
  assert(output_batch);
  assert(transformed);

  output_batch->Clear();
  *transformed = false;

  BlobWriteBatchTransformer transformer(partition_mgr_provider, output_batch,
                                        settings_provider, write_options);

  Status s = input_batch->Iterate(&transformer);
  *transformed = transformer.HasTransformed();

  if (used_managers) {
    used_managers->assign(transformer.used_managers_.begin(),
                          transformer.used_managers_.end());
  }

  if (rollback_infos) {
    *rollback_infos = std::move(transformer.rollback_infos_);
  }

  return s;
}

Status BlobWriteBatchTransformer::MaybePreprocessWideColumns(
    const WriteOptions& write_options, uint32_t column_family_id,
    const Slice& key, const WideColumns& columns,
    BlobFilePartitionManager* partition_mgr,
    const BlobDirectWriteSettings& settings, bool serialize_inline_entity,
    std::string* entity, bool* transformed,
    std::vector<RollbackInfo>* rollback_infos) {
  assert(entity != nullptr);
  assert(transformed != nullptr);

  *transformed = false;

  if (partition_mgr == nullptr || !settings.enable_blob_direct_write) {
    if (!serialize_inline_entity) {
      return Status::OK();
    }
    entity->clear();
    return WideColumnSerialization::Serialize(columns, *entity);
  }

  std::vector<std::pair<size_t, BlobIndex>> new_blob_columns;
  new_blob_columns.reserve(columns.size());
  std::string blob_index_buf;
  bool has_entity_partition = false;
  uint32_t entity_partition = 0;

  for (size_t column_idx = 0; column_idx < columns.size(); ++column_idx) {
    const Slice& column_value = columns[column_idx].value();
    if (column_value.size() < settings.min_blob_size) {
      continue;
    }
    if (!has_entity_partition) {
      entity_partition = partition_mgr->SelectWideColumnPartition(
          column_family_id, key, columns);
      has_entity_partition = true;
    }

    uint64_t blob_file_number = 0;
    uint64_t blob_offset = 0;
    uint64_t blob_size = 0;
    Status s = partition_mgr->WriteBlob(
        write_options, column_family_id, settings.compression_type, key,
        column_value, &blob_file_number, &blob_offset, &blob_size, settings,
        &entity_partition);
    if (!s.ok()) {
      return s;
    }

    if (rollback_infos != nullptr) {
      const uint64_t record_bytes =
          BlobLogRecord::kHeaderSize + key.size() + blob_size;
      rollback_infos->push_back(
          {partition_mgr, blob_file_number, /*count=*/1, record_bytes});
    }

    BlobIndex blob_index;
    BlobIndex::EncodeBlob(&blob_index_buf, blob_file_number, blob_offset,
                          blob_size, settings.compression_type);
    s = blob_index.DecodeFrom(blob_index_buf);
    if (!s.ok()) {
      return s;
    }

    new_blob_columns.emplace_back(column_idx, blob_index);
  }

  if (new_blob_columns.empty()) {
    if (!serialize_inline_entity) {
      return Status::OK();
    }
    entity->clear();
    return WideColumnSerialization::Serialize(columns, *entity);
  }

  entity->clear();
  Status s =
      WideColumnSerialization::SerializeV2(columns, new_blob_columns, *entity);
  if (s.ok()) {
    *transformed = true;
  }
  return s;
}

Status BlobWriteBatchTransformer::PutCF(uint32_t column_family_id,
                                        const Slice& key, const Slice& value) {
  // Use cached settings/manager for the same CF to avoid per-entry lookup.
  if (column_family_id != cached_cf_id_) {
    cached_settings_ = settings_provider_(column_family_id);
    cached_partition_mgr_ = partition_mgr_provider_(column_family_id);
    cached_cf_id_ = column_family_id;
  }
  const auto& settings = cached_settings_;

  if (!cached_partition_mgr_ || !settings.enable_blob_direct_write ||
      value.size() < settings.min_blob_size) {
    return WriteBatchInternal::Put(output_batch_, column_family_id, key, value);
  }

  uint64_t blob_file_number = 0;
  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  Status s = cached_partition_mgr_->WriteBlob(
      write_options_, column_family_id, settings.compression_type, key, value,
      &blob_file_number, &blob_offset, &blob_size, settings);
  if (!s.ok()) {
    return s;
  }

  used_managers_.insert(cached_partition_mgr_);

  // Track the exact file so stale transformed attempts can rollback
  // per-file rather than smearing bytes across all partitions at seal time.
  const uint64_t record_bytes =
      BlobLogRecord::kHeaderSize + key.size() + blob_size;
  rollback_infos_.push_back(
      {cached_partition_mgr_, blob_file_number, /*count=*/1, record_bytes});

  BlobIndex::EncodeBlob(&blob_index_buf_, blob_file_number, blob_offset,
                        blob_size, settings.compression_type);

  has_transformed_ = true;
  return WriteBatchInternal::PutBlobIndex(output_batch_, column_family_id, key,
                                          blob_index_buf_);
}

Status BlobWriteBatchTransformer::TimedPutCF(uint32_t column_family_id,
                                             const Slice& key,
                                             const Slice& value,
                                             uint64_t write_time) {
  // TimedPut: pass through without blob separation for now.
  return WriteBatchInternal::TimedPut(output_batch_, column_family_id, key,
                                      value, write_time);
}

Status BlobWriteBatchTransformer::PutEntityCF(uint32_t column_family_id,
                                              const Slice& key,
                                              const Slice& entity) {
  if (column_family_id != cached_cf_id_) {
    cached_settings_ = settings_provider_(column_family_id);
    cached_partition_mgr_ = partition_mgr_provider_(column_family_id);
    cached_cf_id_ = column_family_id;
  }
  const auto& settings = cached_settings_;

  if (!cached_partition_mgr_ || !settings.enable_blob_direct_write) {
    return WriteBatchInternal::PutEntitySerialized(
        output_batch_, column_family_id, key, entity);
  }

  std::vector<WideColumn> columns;
  std::vector<std::pair<size_t, BlobIndex>> existing_blob_columns;
  Status status = WideColumnSerialization::Deserialize(entity, columns,
                                                       &existing_blob_columns);
  if (status.ok()) {
    // Reject pre-serialized entities that already contain blob references.
    // Passing them through would preserve references to blob files outside this
    // batch's direct-write lifetime tracking, which can later turn into stale
    // reads after blob GC.
    if (UNLIKELY(!existing_blob_columns.empty())) {
      return Status::NotSupported(
          "Blob direct write does not support pre-serialized wide entities "
          "with blob references");
    }

    std::string rewritten_entity;
    bool transformed = false;
    status = MaybePreprocessWideColumns(
        write_options_, column_family_id, key, columns, cached_partition_mgr_,
        settings, /*serialize_inline_entity=*/false, &rewritten_entity,
        &transformed, &rollback_infos_);
    if (status.ok()) {
      if (!transformed) {
        return WriteBatchInternal::PutEntitySerialized(
            output_batch_, column_family_id, key, entity);
      }

      has_transformed_ = true;
      used_managers_.insert(cached_partition_mgr_);
      return WriteBatchInternal::PutEntitySerialized(
          output_batch_, column_family_id, key, rewritten_entity);
    }
  }
  return status;
}

Status BlobWriteBatchTransformer::DeleteCF(uint32_t column_family_id,
                                           const Slice& key) {
  return WriteBatchInternal::Delete(output_batch_, column_family_id, key);
}

Status BlobWriteBatchTransformer::SingleDeleteCF(uint32_t column_family_id,
                                                 const Slice& key) {
  return WriteBatchInternal::SingleDelete(output_batch_, column_family_id, key);
}

Status BlobWriteBatchTransformer::DeleteRangeCF(uint32_t column_family_id,
                                                const Slice& begin_key,
                                                const Slice& end_key) {
  return WriteBatchInternal::DeleteRange(output_batch_, column_family_id,
                                         begin_key, end_key);
}

Status BlobWriteBatchTransformer::MergeCF(uint32_t column_family_id,
                                          const Slice& key,
                                          const Slice& value) {
  return WriteBatchInternal::Merge(output_batch_, column_family_id, key, value);
}

Status BlobWriteBatchTransformer::PutBlobIndexCF(uint32_t column_family_id,
                                                 const Slice& key,
                                                 const Slice& value) {
  // Already a blob index -- pass through unchanged.
  return WriteBatchInternal::PutBlobIndex(output_batch_, column_family_id, key,
                                          value);
}

void BlobWriteBatchTransformer::LogData(const Slice& blob) {
  output_batch_->PutLogData(blob).PermitUncheckedError();
}

Status BlobWriteBatchTransformer::MarkBeginPrepare(bool unprepared) {
  return WriteBatchInternal::InsertBeginPrepare(
      output_batch_, !unprepared /* write_after_commit */, unprepared);
}

Status BlobWriteBatchTransformer::MarkEndPrepare(const Slice& xid) {
  return WriteBatchInternal::InsertEndPrepare(output_batch_, xid);
}

Status BlobWriteBatchTransformer::MarkCommit(const Slice& xid) {
  return WriteBatchInternal::MarkCommit(output_batch_, xid);
}

Status BlobWriteBatchTransformer::MarkCommitWithTimestamp(const Slice& xid,
                                                          const Slice& ts) {
  return WriteBatchInternal::MarkCommitWithTimestamp(output_batch_, xid, ts);
}

Status BlobWriteBatchTransformer::MarkRollback(const Slice& xid) {
  return WriteBatchInternal::MarkRollback(output_batch_, xid);
}

Status BlobWriteBatchTransformer::MarkNoop(bool /*empty_batch*/) {
  return WriteBatchInternal::InsertNoop(output_batch_);
}

// First pass of BlobDirectWriteRecoveryResolver::Resolve(). Finds out, without
// copying anything, whether the batch has a value in an unregistered blob
// file. Stops at the first such value.
class BlobDirectWriteRecoveryResolver::Scanner : public WriteBatch::Handler {
 public:
  Scanner(BlobDirectWriteRecoveryResolver* resolver, uint64_t wal_number)
      : resolver_(resolver), wal_number_(wal_number) {}

  bool needs_resolve() const { return needs_resolve_; }

  bool Continue() override { return !needs_resolve_; }

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice& /*key*/,
                        const Slice& value) override {
    ColumnFamilyData* cfd =
        resolver_->GetReplayedColumnFamily(column_family_id, wal_number_);
    if (cfd == nullptr) {
      return Status::OK();
    }
    BlobIndex blob_index;
    Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      return s;
    }
    return resolver_->CheckBlobIndex(cfd, blob_index, &needs_resolve_);
  }

  Status PutEntityCF(uint32_t column_family_id, const Slice& /*key*/,
                     const Slice& entity) override {
    ColumnFamilyData* cfd =
        resolver_->GetReplayedColumnFamily(column_family_id, wal_number_);
    if (cfd == nullptr) {
      return Status::OK();
    }
    return WideColumnSerialization::ForEachBlobFileNumber(
        entity, [&](const BlobIndex& blob_index) -> Status {
          bool needs_resolve = false;
          Status s = resolver_->CheckBlobIndex(cfd, blob_index, &needs_resolve);
          needs_resolve_ = needs_resolve_ || needs_resolve;
          return s;
        });
  }

  // The remaining entry types never point to a blob file.
  Status PutCF(uint32_t, const Slice&, const Slice&) override {
    return Status::OK();
  }
  Status TimedPutCF(uint32_t, const Slice&, const Slice&, uint64_t) override {
    return Status::OK();
  }
  Status DeleteCF(uint32_t, const Slice&) override { return Status::OK(); }
  Status SingleDeleteCF(uint32_t, const Slice&) override {
    return Status::OK();
  }
  Status DeleteRangeCF(uint32_t, const Slice&, const Slice&) override {
    return Status::OK();
  }
  Status MergeCF(uint32_t, const Slice&, const Slice&) override {
    return Status::OK();
  }
  void LogData(const Slice&) override {}
  Status MarkBeginPrepare(bool) override { return Status::OK(); }
  Status MarkEndPrepare(const Slice&) override { return Status::OK(); }
  Status MarkCommit(const Slice&) override { return Status::OK(); }
  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
    return Status::OK();
  }
  Status MarkRollback(const Slice&) override { return Status::OK(); }
  Status MarkNoop(bool) override { return Status::OK(); }

 private:
  BlobDirectWriteRecoveryResolver* const resolver_;
  const uint64_t wal_number_;
  bool needs_resolve_ = false;
};

// Second pass of BlobDirectWriteRecoveryResolver::Resolve(). Copies every
// entry of the batch into `output` and makes values in unregistered blob files
// raw again.
class BlobDirectWriteRecoveryResolver::Rewriter : public WriteBatch::Handler {
 public:
  Rewriter(BlobDirectWriteRecoveryResolver* resolver, uint64_t wal_number,
           WriteBatch* output)
      : resolver_(resolver), wal_number_(wal_number), output_(output) {}

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice& key,
                        const Slice& value) override {
    ColumnFamilyData* cfd =
        resolver_->GetReplayedColumnFamily(column_family_id, wal_number_);
    if (cfd != nullptr) {
      BlobIndex blob_index;
      Status s = blob_index.DecodeFrom(value);
      bool needs_resolve = false;
      if (s.ok()) {
        s = resolver_->CheckBlobIndex(cfd, blob_index, &needs_resolve);
      }
      if (s.ok() && needs_resolve) {
        s = ReadValue(cfd, key, blob_index, &value_buf_);
        if (s.ok()) {
          s = WriteBatchInternal::Put(output_, column_family_id, key,
                                      value_buf_);
        }
        return s;
      }
      if (!s.ok()) {
        return s;
      }
    }
    return WriteBatchInternal::PutBlobIndex(output_, column_family_id, key,
                                            value);
  }

  Status PutEntityCF(uint32_t column_family_id, const Slice& key,
                     const Slice& entity) override {
    ColumnFamilyData* cfd =
        resolver_->GetReplayedColumnFamily(column_family_id, wal_number_);
    if (cfd == nullptr) {
      return WriteBatchInternal::PutEntitySerialized(output_, column_family_id,
                                                     key, entity);
    }
    WideColumns columns;
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    Status s =
        WideColumnSerialization::Deserialize(entity, columns, &blob_columns);
    if (!s.ok()) {
      return s;
    }
    // Blob columns that still point to MANIFEST blob files stay blob columns.
    std::vector<std::pair<size_t, BlobIndex>> kept_blob_columns;
    // Raw values of the resolved columns, and the column index of each.
    std::vector<std::string> resolved_values;
    std::vector<size_t> resolved_column_indexes;
    for (const auto& [column_index, blob_index] : blob_columns) {
      bool needs_resolve = false;
      s = resolver_->CheckBlobIndex(cfd, blob_index, &needs_resolve);
      if (!s.ok()) {
        return s;
      }
      if (!needs_resolve) {
        kept_blob_columns.emplace_back(column_index, blob_index);
        continue;
      }
      resolved_values.emplace_back();
      s = ReadValue(cfd, key, blob_index, &resolved_values.back());
      if (!s.ok()) {
        return s;
      }
      resolved_column_indexes.push_back(column_index);
    }
    if (resolved_values.empty()) {
      return WriteBatchInternal::PutEntitySerialized(output_, column_family_id,
                                                     key, entity);
    }
    // `resolved_values` no longer grows, so the columns can point into it.
    for (size_t i = 0; i < resolved_values.size(); ++i) {
      columns[resolved_column_indexes[i]].value() = resolved_values[i];
    }
    std::string rewritten_entity;
    if (kept_blob_columns.empty()) {
      s = WideColumnSerialization::Serialize(columns, rewritten_entity);
    } else {
      s = WideColumnSerialization::SerializeV2(columns, kept_blob_columns,
                                               rewritten_entity);
    }
    if (!s.ok()) {
      return s;
    }
    return WriteBatchInternal::PutEntitySerialized(output_, column_family_id,
                                                   key, rewritten_entity);
  }

  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    return WriteBatchInternal::Put(output_, column_family_id, key, value);
  }
  Status TimedPutCF(uint32_t column_family_id, const Slice& key,
                    const Slice& value, uint64_t write_time) override {
    return WriteBatchInternal::TimedPut(output_, column_family_id, key, value,
                                        write_time);
  }
  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    return WriteBatchInternal::Delete(output_, column_family_id, key);
  }
  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override {
    return WriteBatchInternal::SingleDelete(output_, column_family_id, key);
  }
  Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                       const Slice& end_key) override {
    return WriteBatchInternal::DeleteRange(output_, column_family_id, begin_key,
                                           end_key);
  }
  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
    return WriteBatchInternal::Merge(output_, column_family_id, key, value);
  }
  void LogData(const Slice& blob) override {
    output_->PutLogData(blob).PermitUncheckedError();
  }
  Status MarkBeginPrepare(bool unprepared) override {
    return WriteBatchInternal::InsertBeginPrepare(
        output_, !unprepared /* write_after_commit */, unprepared);
  }
  Status MarkEndPrepare(const Slice& xid) override {
    return WriteBatchInternal::InsertEndPrepare(output_, xid);
  }
  Status MarkCommit(const Slice& xid) override {
    return WriteBatchInternal::MarkCommit(output_, xid);
  }
  Status MarkCommitWithTimestamp(const Slice& xid, const Slice& ts) override {
    return WriteBatchInternal::MarkCommitWithTimestamp(output_, xid, ts);
  }
  Status MarkRollback(const Slice& xid) override {
    return WriteBatchInternal::MarkRollback(output_, xid);
  }
  Status MarkNoop(bool /*empty_batch*/) override {
    return WriteBatchInternal::InsertNoop(output_);
  }

  // Number of values this batch made raw again.
  uint64_t resolved_value_count() const { return resolved_value_count_; }

  // Unregistered blob files this batch read from.
  const UnorderedSet<uint64_t>& read_files() const { return read_files_; }

 private:
  Status ReadValue(ColumnFamilyData* cfd, const Slice& key,
                   const BlobIndex& blob_index, std::string* value) {
    Status s = resolver_->ReadValue(cfd, wal_number_, key, blob_index, value);
    if (s.ok()) {
      ++resolved_value_count_;
      read_files_.insert(blob_index.file_number());
    }
    return s;
  }

  BlobDirectWriteRecoveryResolver* const resolver_;
  const uint64_t wal_number_;
  WriteBatch* const output_;
  // Reused buffer for one resolved Put value.
  std::string value_buf_;
  uint64_t resolved_value_count_ = 0;
  UnorderedSet<uint64_t> read_files_;
};

BlobDirectWriteRecoveryResolver::BlobDirectWriteRecoveryResolver(
    const ColumnFamilySet* column_families, const FileOptions& file_options,
    UnorderedSet<uint64_t> unregistered_blob_files)
    : column_families_(column_families),
      file_options_(file_options),
      unregistered_blob_files_(std::move(unregistered_blob_files)) {
  assert(column_families_ != nullptr);
}

BlobDirectWriteRecoveryResolver::~BlobDirectWriteRecoveryResolver() = default;

ColumnFamilyData* BlobDirectWriteRecoveryResolver::GetReplayedColumnFamily(
    uint32_t column_family_id, uint64_t wal_number) const {
  // Mirrors the skip rules of MemTableInserter::SeekToColumnFamily() during
  // recovery.
  ColumnFamilyData* cfd = column_families_->GetColumnFamily(column_family_id);
  if (cfd == nullptr || cfd->IsDropped() || wal_number < cfd->GetLogNumber()) {
    return nullptr;
  }
  return cfd;
}

Status BlobDirectWriteRecoveryResolver::CheckBlobIndex(
    ColumnFamilyData* cfd, const BlobIndex& blob_index, bool* needs_resolve) {
  assert(cfd != nullptr);
  assert(needs_resolve != nullptr);
  *needs_resolve = false;
  // Blob direct write never writes inlined or TTL blob indexes. Other writers
  // (the stackable BlobDB) do, and their files are not managed here.
  if (blob_index.IsInlined() || blob_index.HasTTL()) {
    return Status::OK();
  }
  const uint64_t file_number = blob_index.file_number();
  if (cfd->current()->storage_info()->GetBlobFileMetaData(file_number)) {
    return Status::OK();
  }
  if (unregistered_blob_files_.count(file_number) > 0) {
    *needs_resolve = true;
    return Status::OK();
  }
  if (!cfd->GetLatestMutableCFOptions().enable_blob_files) {
    // Without integrated blob files, a BlobIndex to a file that is not on disk
    // is left unchanged. It can come from another writer (the stackable
    // BlobDB). It can also come from blob direct write of an earlier session
    // whose file is lost; that case is not detected.
    return Status::OK();
  }
  // A read-only open of a DB that another process still writes can meet a
  // blob file that was created after recovery listed the directory. Check the
  // disk once for each such file number. A read-write open holds the DB lock,
  // so no file appears after the listing, and this check only costs one
  // lookup on the error path.
  if (missing_blob_files_.count(file_number) == 0) {
    const std::string path =
        BlobFileName(cfd->ioptions().cf_paths.front().path, file_number);
    IOStatus io_s = cfd->ioptions().fs->FileExists(path, IOOptions(),
                                                   /*dbg=*/nullptr);
    if (io_s.ok()) {
      unregistered_blob_files_.insert(file_number);
      *needs_resolve = true;
      return Status::OK();
    }
    if (!io_s.IsNotFound()) {
      return io_s;
    }
    missing_blob_files_.insert(file_number);
  }
  return Status::Corruption("WAL record refers to blob file " +
                            std::to_string(file_number) +
                            ", which is neither in the MANIFEST nor on disk");
}

Status BlobDirectWriteRecoveryResolver::GetReader(ColumnFamilyData* cfd,
                                                  uint64_t wal_number,
                                                  uint64_t file_number,
                                                  BlobFileReader** reader) {
  assert(cfd != nullptr);
  assert(reader != nullptr);
  auto it = readers_.find(file_number);
  if (it != readers_.end()) {
    *reader = it->second.get();
    return Status::OK();
  }
  if (readers_.size() >= kMaxOpenReaders) {
    // Close an arbitrary reader (the first one in the map). Replay reads the
    // files of one WAL mostly in order, so a closed reader is rarely needed
    // again.
    readers_.erase(readers_.begin());
  }
  std::unique_ptr<BlobFileReader> new_reader;
  // An unregistered blob file may not be sealed yet, so it can have no footer.
  // Skip the footer check.
  ReadOptions read_options(Env::IOActivity::kDBOpen);
  Status s = BlobFileReader::Create(
      cfd->ioptions(), read_options, file_options_, cfd->GetID(),
      /*blob_file_read_hist=*/nullptr, file_number, /*io_tracer=*/nullptr,
      /*skip_footer_validation=*/true, &new_reader);
  if (s.IsNotFound() || s.IsPathNotFound()) {
    return Status::Corruption("Blob file " + std::to_string(file_number) +
                              " referenced by WAL " +
                              std::to_string(wal_number) + " is missing");
  }
  if (!s.ok()) {
    return s;
  }
  *reader = new_reader.get();
  readers_.emplace(file_number, std::move(new_reader));
  return Status::OK();
}

Status BlobDirectWriteRecoveryResolver::ReadValue(ColumnFamilyData* cfd,
                                                  uint64_t wal_number,
                                                  const Slice& user_key,
                                                  const BlobIndex& blob_index,
                                                  std::string* value) {
  assert(cfd != nullptr);
  assert(value != nullptr);
  const uint64_t file_number = blob_index.file_number();
  BlobFileReader* reader = nullptr;
  Status s = GetReader(cfd, wal_number, file_number, &reader);
  if (!s.ok()) {
    return s;
  }
  // A reader keeps the file size from when it was opened. In a read-only open
  // of a DB that another process still writes, the file can grow after that.
  // If the value ends past that size, open the file again once. In a
  // read-write open the file does not grow, so this happens only for a torn
  // record. The second reader then reports the same Corruption.
  if (blob_index.offset() + blob_index.size() > reader->GetFileSize()) {
    readers_.erase(file_number);
    s = GetReader(cfd, wal_number, file_number, &reader);
    if (!s.ok()) {
      return s;
    }
  }

  ReadOptions read_options(Env::IOActivity::kDBOpen);
  read_options.verify_checksums = true;
  std::unique_ptr<BlobContents> contents;
  uint64_t bytes_read = 0;
  s = reader->GetBlob(read_options, user_key, blob_index.offset(),
                      blob_index.size(), blob_index.compression(),
                      /*prefetch_buffer=*/nullptr,
                      /*allocator=*/nullptr, &contents, &bytes_read);
  if (!s.ok()) {
    return s;
  }
  value->assign(contents->data().data(), contents->data().size());
  return Status::OK();
}

Status BlobDirectWriteRecoveryResolver::Resolve(
    uint64_t wal_number, const WriteBatch& batch,
    std::unique_ptr<WriteBatch>* resolved_batch) {
  assert(resolved_batch != nullptr);
  resolved_batch->reset();

  Scanner scanner(this, wal_number);
  Status s = batch.Iterate(&scanner);
  if (!s.ok() || !scanner.needs_resolve()) {
    return s;
  }
  // Recovery rebuilds a prepared 2PC transaction from the markers of its
  // prepare section, so a rewrite must keep those markers exactly. The
  // Rewriter does not: for example, it writes a kTypeBeginPersistedPrepareXID
  // marker as kTypeBeginPrepareXID, which changes how recovery rebuilds the
  // transaction. The write path does not put BlobIndex entries into a prepare
  // section (see MaybeTransformBatchForBlobDirectWrite() and
  // WritePreparedTxnDB::VerifyCFOptions()), so this is only a safety check.
  // Fail the open instead of rewriting such a record.
  if (batch.HasBeginPrepare()) {
    return Status::NotSupported(
        "WAL record with a 2PC prepare section refers to a blob file that is "
        "not in the MANIFEST");
  }

  auto output = std::make_unique<WriteBatch>();
  Rewriter rewriter(this, wal_number, output.get());
  s = batch.Iterate(&rewriter);
  if (!s.ok()) {
    return s;
  }
  WriteBatchInternal::SetSequence(output.get(),
                                  WriteBatchInternal::Sequence(&batch));
  *resolved_batch = std::move(output);
  // Count only batches that Resolve() rewrote without error.
  resolved_value_count_ += rewriter.resolved_value_count();
  for (uint64_t file_number : rewriter.read_files()) {
    uint64_t& max_wal_number = referenced_files_[file_number];
    max_wal_number = std::max(max_wal_number, wal_number);
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
