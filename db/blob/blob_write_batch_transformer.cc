//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_write_batch_transformer.h"

#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blog/blog_format.h"
#include "db/wide/wide_column_serialization.h"
#include "db/write_batch_internal.h"
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
    if (column_value.empty() || column_value.size() < settings.min_blob_size) {
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
          partition_mgr->is_blog_format()
              ? ComputeBlogRecordSize(blob_size, /*compact_eligible=*/true)
              : BlobLogRecord::kHeaderSize + key.size() + blob_size;
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
      value.empty() || value.size() < settings.min_blob_size) {
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
      cached_partition_mgr_->is_blog_format()
          ? ComputeBlogRecordSize(blob_size, /*compact_eligible=*/true)
          : BlobLogRecord::kHeaderSize + key.size() + blob_size;
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

}  // namespace ROCKSDB_NAMESPACE
