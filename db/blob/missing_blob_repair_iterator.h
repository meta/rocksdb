//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/version_set.h"
#include "db/wide/wide_column_serialization.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

// Replaces references absent from an immutable input Version with deletions
// or entities containing the surviving columns. Input values must already be
// prepared, as they are for compaction input. Neither input is owned.
class MissingBlobRepairIterator : public InternalIterator {
 public:
  MissingBlobRepairIterator(InternalIterator* input,
                            const VersionStorageInfo* input_storage,
                            size_t timestamp_size)
      : input_(input),
        input_storage_(input_storage),
        timestamp_size_(timestamp_size) {
    assert(input_);
    assert(input_storage_);
    Update();
  }

  bool Valid() const override { return input_->Valid() && status_.ok(); }

  void SeekToFirst() override {
    input_->SeekToFirst();
    Update();
  }

  void SeekToLast() override {
    input_->SeekToLast();
    Update();
  }

  void Seek(const Slice& target) override {
    // A rewritten type can sort after its original type at the same sequence.
    const std::string target_copy = target.ToString();
    ParsedInternalKey parsed;
    status_ = ParseInternalKey(target_copy, &parsed, false);
    if (!status_.ok()) {
      return;
    }
    InternalKey seek_key(parsed.user_key, parsed.sequence, kValueTypeForSeek);
    input_->Seek(seek_key.Encode());
    Update();
    while (Valid() && input_storage_->InternalComparator()->Compare(
                          key(), target_copy) < 0) {
      Next();
    }
  }

  void SeekForPrev(const Slice& target) override {
    const std::string target_copy = target.ToString();
    input_->SeekForPrev(target_copy);
    Update();
    while (Valid() && input_storage_->InternalComparator()->Compare(
                          key(), target_copy) > 0) {
      Prev();
    }
  }

  void Next() override {
    assert(Valid());
    input_->Next();
    Update();
  }

  void Prev() override {
    assert(Valid());
    input_->Prev();
    Update();
  }

  Slice key() const override {
    assert(Valid());
    return repaired_key_.empty() ? input_->key() : Slice(repaired_key_);
  }

  Slice value() const override {
    assert(Valid());
    return value_rewritten_ ? Slice(repaired_value_) : input_->value();
  }

  uint64_t write_unix_time() const override {
    return input_->write_unix_time();
  }

  Status status() const override { return status_; }

  bool PrepareValue() override {
    assert(Valid());
    return true;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* manager) override {
    input_->SetPinnedItersMgr(manager);
  }

  bool IsKeyPinned() const override {
    return repaired_key_.empty() && input_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    return !value_rewritten_ && input_->IsValuePinned();
  }

  bool IsSafeToDropWhilePinned() const override {
    return !value_rewritten_ && input_->IsSafeToDropWhilePinned();
  }

  bool IsDeleteRangeSentinelKey() const override {
    return input_->IsDeleteRangeSentinelKey();
  }

  bool MayBeOutOfLowerBound() override {
    return input_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    return input_->UpperBoundCheckResult();
  }

  void SetRangeDelReadSeqno(SequenceNumber sequence) override {
    input_->SetRangeDelReadSeqno(sequence);
  }

  void GetReadaheadState(ReadaheadFileInfo* state) override {
    input_->GetReadaheadState(state);
  }

  void SetReadaheadState(ReadaheadFileInfo* state) override {
    input_->SetReadaheadState(state);
  }

  Status GetProperty(std::string name, std::string* value) override {
    return input_->GetProperty(std::move(name), value);
  }

  bool IsRepairDeletion() const { return repair_deletion_; }

 private:
  // Only external, non-TTL indexes use the input Version's blob file set.
  // Absence is checked in metadata; inline, same-file and legacy TTL indexes
  // retain their existing resolution rules.
  bool IsMissing(const BlobIndex& index) const {
    if (index.IsInlined() || index.HasTTL() || index.IsSameFile()) {
      return false;
    }
    const VersionStorageInfo::BlobFiles::const_iterator it =
        input_storage_->GetBlobFileMetaDataLB(index.file_number());
    return it == input_storage_->GetBlobFiles().end() ||
           (*it)->GetBlobFileNumber() != index.file_number();
  }

  // Preserve the sequence and user-key timestamp so the lost value continues
  // to hide older versions, including versions retained for snapshots.
  void SetDeletion(const ParsedInternalKey& parsed) {
    repaired_key_ = input_->key().ToString();
    const ValueType type =
        timestamp_size_ == 0 ? kTypeDeletion : kTypeDeletionWithTimestamp;
    UpdateInternalKey(&repaired_key_, parsed.sequence, type);
    value_rewritten_ = true;
    repair_deletion_ = true;
  }

  // Inspect column indexes without resolving blob payloads. Removing columns
  // changes the positions of surviving blob indexes, so rebuild those positions
  // before serialization. Only loss of every column becomes a deletion.
  Status RepairEntity(const ParsedInternalKey& parsed) {
    columns_.clear();
    blob_columns_.clear();
    Status s = WideColumnSerialization::Deserialize(input_->value(), columns_,
                                                    &blob_columns_);
    if (!s.ok()) {
      return s;
    }

    bool has_missing = false;
    for (const std::pair<size_t, BlobIndex>& column : blob_columns_) {
      has_missing |= IsMissing(column.second);
    }
    if (!has_missing) {
      return Status::OK();
    }

    surviving_columns_.clear();
    surviving_blobs_.clear();
    size_t blob_position = 0;
    for (size_t i = 0; i < columns_.size(); ++i) {
      const BlobIndex* index = nullptr;
      if (blob_position < blob_columns_.size() &&
          blob_columns_[blob_position].first == i) {
        index = &blob_columns_[blob_position++].second;
      }
      if (index && IsMissing(*index)) {
        continue;
      }
      if (index) {
        surviving_blobs_.emplace_back(surviving_columns_.size(), *index);
      }
      surviving_columns_.push_back(columns_[i]);
    }
    if (surviving_columns_.empty()) {
      SetDeletion(parsed);
      return Status::OK();
    }

    s = surviving_blobs_.empty()
            ? WideColumnSerialization::Serialize(surviving_columns_,
                                                 repaired_value_)
            : WideColumnSerialization::SerializeV2(
                  surviving_columns_, surviving_blobs_, repaired_value_);
    if (s.ok()) {
      value_rewritten_ = true;
    }
    return s;
  }

  // Reset rewritten buffers on every movement. Preserve input errors and range
  // deletion sentinels before decoding point values; malformed indexes fail.
  void Update() {
    repaired_key_.clear();
    repaired_value_.clear();
    value_rewritten_ = false;
    repair_deletion_ = false;
    status_ = input_->status();
    if (!status_.ok() || !input_->Valid() ||
        input_->IsDeleteRangeSentinelKey()) {
      return;
    }

    ParsedInternalKey parsed;
    status_ = ParseInternalKey(input_->key(), &parsed, false);
    if (!status_.ok() || (parsed.type != kTypeBlobIndex &&
                          parsed.type != kTypeWideColumnEntity)) {
      return;
    }
    BlobIndex index;
    if (parsed.user_key.size() < timestamp_size_) {
      status_ =
          Status::Corruption("Blob reference key has an invalid timestamp");
    } else if (parsed.type == kTypeWideColumnEntity) {
      status_ = RepairEntity(parsed);
    } else {
      status_ = index.DecodeFrom(input_->value());
    }
    if (!status_.ok()) {
      return;
    }
    if (parsed.type == kTypeBlobIndex && IsMissing(index)) {
      SetDeletion(parsed);
    }
  }

  InternalIterator* input_;
  const VersionStorageInfo* input_storage_;
  size_t timestamp_size_;
  std::string repaired_key_;
  std::string repaired_value_;
  WideColumns columns_;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_;
  WideColumns surviving_columns_;
  std::vector<std::pair<size_t, BlobIndex>> surviving_blobs_;
  Status status_;
  bool value_rewritten_ = false;
  bool repair_deletion_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
