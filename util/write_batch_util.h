//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <unordered_set>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {
// ColumnFamilyCollector is a write batch handler which does nothing
// except recording unique column family IDs
class ColumnFamilyCollector : public WriteBatch::Handler {
  std::unordered_set<uint32_t> column_family_ids_;

  Status AddColumnFamilyId(uint32_t column_family_id) {
    column_family_ids_.insert(column_family_id);
    return Status::OK();
  }

 public:
  explicit ColumnFamilyCollector() {}

  ~ColumnFamilyCollector() override = default;

  Status PutCF(uint32_t column_family_id, const Slice&, const Slice&) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status PutEntityCF(uint32_t column_family_id, const Slice&,
                     const Slice&) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status TimedPutCF(uint32_t column_family_id, const Slice&, const Slice&,
                    uint64_t) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status DeleteCF(uint32_t column_family_id, const Slice&) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status SingleDeleteCF(uint32_t column_family_id, const Slice&) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status DeleteRangeCF(uint32_t column_family_id, const Slice&,
                       const Slice&) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status MergeCF(uint32_t column_family_id, const Slice&,
                 const Slice&) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice&,
                        const Slice&) override {
    return AddColumnFamilyId(column_family_id);
  }

  Status MarkBeginPrepare(bool) override { return Status::OK(); }

  Status MarkEndPrepare(const Slice&) override { return Status::OK(); }

  Status MarkRollback(const Slice&) override { return Status::OK(); }

  Status MarkCommit(const Slice&) override { return Status::OK(); }

  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
    return Status::OK();
  }

  Status MarkNoop(bool) override { return Status::OK(); }

  const std::unordered_set<uint32_t>& column_families() const {
    return column_family_ids_;
  }
};

Status CollectColumnFamilyIdsFromWriteBatch(
    const WriteBatch& batch, std::vector<uint32_t>* column_family_ids);

}  // namespace ROCKSDB_NAMESPACE
