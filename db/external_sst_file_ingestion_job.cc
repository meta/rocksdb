//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/external_sst_file_ingestion_job.h"

#include <algorithm>
#include <cinttypes>
#include <map>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/builder.h"
#include "db/compaction/clipping_iterator.h"
#include "db/compaction/compaction_picker.h"
#include "db/db_impl/db_impl.h"
#include "db/job_context.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/wide/wide_column_serialization.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "monitoring/statistics_impl.h"
#include "options/options_helper.h"
#include "table/merging_iterator.h"
#include "table/prepared_file_info.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
uint64_t ResolveExternalFileTime(const std::string& file_path,
                                 const char* time_name, uint64_t carried_time,
                                 uint64_t now, Logger* info_log) {
  if (carried_time == 0) {
    return now;
  }
  if (now != 0 && carried_time > now) {
    ROCKS_LOG_WARN(info_log,
                   "External file %s has %s %" PRIu64
                   " later than current time %" PRIu64 "; using current time",
                   file_path.c_str(), time_name, carried_time, now);
    return now;
  }
  return carried_time;
}
}  // namespace

void ResolveExternalFileTimes(const std::string& file_path,
                              uint64_t carried_file_creation_time,
                              uint64_t carried_oldest_ancestor_time,
                              uint64_t now, Logger* info_log,
                              uint64_t* file_creation_time,
                              uint64_t* oldest_ancestor_time) {
  *file_creation_time =
      ResolveExternalFileTime(file_path, "file_creation_time",
                              carried_file_creation_time, now, info_log);
  if (carried_oldest_ancestor_time == kUnknownOldestAncestorTime) {
    *oldest_ancestor_time = *file_creation_time;
  } else {
    *oldest_ancestor_time =
        ResolveExternalFileTime(file_path, "oldest ancestor time",
                                carried_oldest_ancestor_time, now, info_log);
  }
}

Status CheckExternalFileNotTooOld(const std::string& file_path,
                                  uint64_t file_creation_time,
                                  const TableProperties& table_properties,
                                  uint64_t now,
                                  CompactionStyle compaction_style,
                                  const MutableCFOptions& mutable_cf_options,
                                  Logger* info_log) {
  const char* limit_name;
  uint64_t max_age;
  const char* judged_time_name = "file_creation_time";
  uint64_t judged_time = file_creation_time;
  switch (compaction_style) {
    case kCompactionStyleLevel:
    case kCompactionStyleUniversal:
      limit_name = "periodic_compaction_seconds";
      max_age = mutable_cf_options.periodic_compaction_seconds;
      break;
    case kCompactionStyleFIFO:
      limit_name = "ttl";
      max_age = mutable_cf_options.ttl;
      // Estimate when the newest data was written, as FIFO TTL compaction
      // does (FIFOCompactionPicker::PickTTLCompaction()), so that a file it
      // would treat as expired is rejected
      if (table_properties.newest_key_time != kUnknownNewestKeyTime) {
        judged_time_name = "newest_key_time";
        judged_time = table_properties.newest_key_time;
      } else if (table_properties.oldest_ancestor_time !=
                 kUnknownOldestAncestorTime) {
        judged_time_name = "oldest_ancestor_time";
        judged_time = table_properties.oldest_ancestor_time;
      }
      break;
    default:
      return Status::OK();
  }
  if (judged_time == 0 || judged_time >= now) {
    return Status::OK();
  }
  const uint64_t age = now - judged_time;
  if (max_age == 0 || age <= max_age) {
    return Status::OK();
  }
  std::string msg =
      "External file " + file_path + " is too old: " + judged_time_name +
      " is " + std::to_string(age) + " seconds ago, exceeding " + limit_name +
      "=" + std::to_string(max_age) + " (reject_too_old_file)";
  ROCKS_LOG_WARN(info_log, "%s", msg.c_str());
  return Status::InvalidArgument(msg);
}

namespace {

class LsmEditSequenceOrderValidator {
 public:
  LsmEditSequenceOrderValidator(const InternalKeyComparator& icmp,
                                const SliceTransform* prefix_extractor,
                                bool fill_cache, bool allow_data_in_errors,
                                bool* source_priority_required)
      : ucmp_(icmp.user_comparator()),
        prefix_extractor_(prefix_extractor),
        allow_data_in_errors_(allow_data_in_errors),
        source_priority_required_(source_priority_required) {
    assert(source_priority_required_ != nullptr);
    read_options_.fill_cache = fill_cache;
    read_options_.total_order_seek = true;
  }

  Status Validate(const IngestedFileInfo& newer_file, TableReader* newer_reader,
                  const IngestedFileInfo& older_file, TableReader* older_reader,
                  bool allow_equal_sequence_numbers) const {
    Status status =
        ValidatePointEntries(newer_file, newer_reader, older_file, older_reader,
                             allow_equal_sequence_numbers);
    if (!status.ok()) {
      return status;
    }
    status = ValidateTombstonesAgainstPoints(
        newer_file, newer_reader, older_file, older_reader,
        /*tombstones_are_newer=*/true, allow_equal_sequence_numbers);
    if (!status.ok()) {
      return status;
    }
    status = ValidateTombstonesAgainstPoints(
        older_file, older_reader, newer_file, newer_reader,
        /*tombstones_are_newer=*/false, allow_equal_sequence_numbers);
    if (!status.ok()) {
      return status;
    }
    return ValidateRangeTombstones(newer_file, newer_reader, older_file,
                                   older_reader, allow_equal_sequence_numbers);
  }

 private:
  struct PointGroup {
    std::string user_key;
    SequenceNumber smallest_seqno = kMaxSequenceNumber;
    SequenceNumber largest_seqno = 0;
    bool smallest_is_merge = false;
    bool largest_is_merge = false;
    bool smallest_is_single_delete = false;
    bool largest_is_single_delete = false;
  };

  bool IsOrdered(SequenceNumber newer_seqno, SequenceNumber older_seqno,
                 bool allow_equal_sequence_numbers,
                 bool allow_zero_sequence_numbers = true) const {
    return newer_seqno > older_seqno ||
           (newer_seqno == older_seqno &&
            (allow_equal_sequence_numbers ||
             (allow_zero_sequence_numbers && newer_seqno == 0)));
  }

  Status InvalidOrder(const IngestedFileInfo& newer_file,
                      SequenceNumber newer_seqno,
                      const IngestedFileInfo& older_file,
                      SequenceNumber older_seqno) const {
    return Status::InvalidArgument(
        "LSM edit has invalid sequence number ordering: file " +
        newer_file.external_file_path + " at level " +
        std::to_string(newer_file.declared_level) + " has sequence " +
        std::to_string(newer_seqno) + ", while overlapping file " +
        older_file.external_file_path + " at level " +
        std::to_string(older_file.declared_level) + " has sequence " +
        std::to_string(older_seqno));
  }

  std::unique_ptr<InternalIterator> NewPointIterator(
      TableReader* reader) const {
    return std::unique_ptr<InternalIterator>(reader->NewIterator(
        read_options_, prefix_extractor_, /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));
  }

  Status ReadPointGroup(InternalIterator* iter, PointGroup* group,
                        bool* valid) const {
    assert(iter != nullptr);
    assert(group != nullptr);
    assert(valid != nullptr);
    if (!iter->Valid()) {
      *valid = false;
      return iter->status();
    }

    ParsedInternalKey parsed;
    Status status =
        ParseInternalKey(iter->key(), &parsed, allow_data_in_errors_);
    if (!status.ok()) {
      return status;
    }
    group->user_key.assign(parsed.user_key.data(), parsed.user_key.size());
    group->smallest_seqno = parsed.sequence;
    group->largest_seqno = parsed.sequence;
    group->smallest_is_merge = parsed.type == kTypeMerge;
    group->largest_is_merge = parsed.type == kTypeMerge;
    group->smallest_is_single_delete = parsed.type == kTypeSingleDeletion;
    group->largest_is_single_delete = parsed.type == kTypeSingleDeletion;
    iter->Next();
    while (iter->Valid()) {
      status = ParseInternalKey(iter->key(), &parsed, allow_data_in_errors_);
      if (!status.ok()) {
        return status;
      }
      if (ucmp_->Compare(group->user_key, parsed.user_key) != 0) {
        break;
      }
      if (parsed.sequence < group->smallest_seqno) {
        group->smallest_seqno = parsed.sequence;
        group->smallest_is_merge = parsed.type == kTypeMerge;
        group->smallest_is_single_delete = parsed.type == kTypeSingleDeletion;
      } else if (parsed.sequence == group->smallest_seqno) {
        group->smallest_is_merge |= parsed.type == kTypeMerge;
        group->smallest_is_single_delete |= parsed.type == kTypeSingleDeletion;
      }
      if (parsed.sequence > group->largest_seqno) {
        group->largest_seqno = parsed.sequence;
        group->largest_is_merge = parsed.type == kTypeMerge;
        group->largest_is_single_delete = parsed.type == kTypeSingleDeletion;
      } else if (parsed.sequence == group->largest_seqno) {
        group->largest_is_merge |= parsed.type == kTypeMerge;
        group->largest_is_single_delete |= parsed.type == kTypeSingleDeletion;
      }
      iter->Next();
    }
    *valid = true;
    return iter->status();
  }

  Status ValidatePointEntries(const IngestedFileInfo& newer_file,
                              TableReader* newer_reader,
                              const IngestedFileInfo& older_file,
                              TableReader* older_reader,
                              bool allow_equal_sequence_numbers) const {
    std::unique_ptr<InternalIterator> newer_iter =
        NewPointIterator(newer_reader);
    std::unique_ptr<InternalIterator> older_iter =
        NewPointIterator(older_reader);
    const Slice first =
        ucmp_->Compare(newer_file.smallest_internal_key.user_key(),
                       older_file.smallest_internal_key.user_key()) >= 0
            ? newer_file.smallest_internal_key.user_key()
            : older_file.smallest_internal_key.user_key();
    const Slice last =
        ucmp_->Compare(newer_file.largest_internal_key.user_key(),
                       older_file.largest_internal_key.user_key()) <= 0
            ? newer_file.largest_internal_key.user_key()
            : older_file.largest_internal_key.user_key();
    const InternalKey seek(first, kMaxSequenceNumber, kValueTypeForSeek);
    newer_iter->Seek(seek.Encode());
    older_iter->Seek(seek.Encode());

    PointGroup newer_group;
    PointGroup older_group;
    bool has_newer = false;
    bool has_older = false;
    Status status = ReadPointGroup(newer_iter.get(), &newer_group, &has_newer);
    if (!status.ok()) {
      return status;
    }
    status = ReadPointGroup(older_iter.get(), &older_group, &has_older);
    if (!status.ok()) {
      return status;
    }

    while (has_newer && has_older) {
      if (ucmp_->Compare(newer_group.user_key, last) > 0 ||
          ucmp_->Compare(older_group.user_key, last) > 0) {
        break;
      }
      const int cmp =
          ucmp_->Compare(newer_group.user_key, older_group.user_key);
      if (cmp == 0) {
        if (!IsOrdered(newer_group.smallest_seqno, older_group.largest_seqno,
                       allow_equal_sequence_numbers)) {
          return InvalidOrder(newer_file, newer_group.smallest_seqno,
                              older_file, older_group.largest_seqno);
        }
        if (newer_group.smallest_seqno == older_group.largest_seqno &&
            (newer_group.smallest_is_single_delete ||
             older_group.largest_is_single_delete)) {
          return Status::NotSupported(
              "LSM edit does not support equal-sequence SingleDelete ties");
        }
        if (newer_group.smallest_seqno == older_group.largest_seqno &&
            newer_group.smallest_is_merge && older_group.largest_is_merge) {
          return Status::NotSupported(
              "LSM edit equal-sequence merge operands must be consolidated "
              "into one incoming merged level");
        }
        if (newer_group.smallest_seqno == older_group.largest_seqno) {
          *source_priority_required_ = true;
        }
        status = ReadPointGroup(newer_iter.get(), &newer_group, &has_newer);
        if (status.ok()) {
          status = ReadPointGroup(older_iter.get(), &older_group, &has_older);
        }
      } else if (cmp < 0) {
        status = ReadPointGroup(newer_iter.get(), &newer_group, &has_newer);
      } else {
        status = ReadPointGroup(older_iter.get(), &older_group, &has_older);
      }
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }

  Status ValidateTombstonesAgainstPoints(
      const IngestedFileInfo& tombstone_file, TableReader* tombstone_reader,
      const IngestedFileInfo& point_file, TableReader* point_reader,
      bool tombstones_are_newer, bool allow_equal_sequence_numbers) const {
    std::unique_ptr<FragmentedRangeTombstoneIterator> tombstone_iter(
        tombstone_reader->NewRangeTombstoneIterator(read_options_));
    if (tombstone_iter == nullptr) {
      return Status::OK();
    }
    std::unique_ptr<InternalIterator> point_iter =
        NewPointIterator(point_reader);

    for (tombstone_iter->SeekToFirst(); tombstone_iter->Valid();
         tombstone_iter->Next()) {
      InternalKey seek_key(tombstone_iter->start_key(), kMaxSequenceNumber,
                           kValueTypeForSeek);
      point_iter->Seek(seek_key.Encode());
      PointGroup point_group;
      bool has_point = false;
      Status status =
          ReadPointGroup(point_iter.get(), &point_group, &has_point);
      if (!status.ok()) {
        return status;
      }
      while (has_point && ucmp_->Compare(point_group.user_key,
                                         tombstone_iter->end_key()) < 0) {
        const SequenceNumber newer_seqno = tombstones_are_newer
                                               ? tombstone_iter->seq()
                                               : point_group.smallest_seqno;
        const SequenceNumber older_seqno = tombstones_are_newer
                                               ? point_group.largest_seqno
                                               : tombstone_iter->seq();
        if (!IsOrdered(
                newer_seqno, older_seqno,
                tombstones_are_newer ? false : allow_equal_sequence_numbers,
                /*allow_zero_sequence_numbers=*/!tombstones_are_newer)) {
          return tombstones_are_newer
                     ? InvalidOrder(tombstone_file, newer_seqno, point_file,
                                    older_seqno)
                     : InvalidOrder(point_file, newer_seqno, tombstone_file,
                                    older_seqno);
        }
        status = ReadPointGroup(point_iter.get(), &point_group, &has_point);
        if (!status.ok()) {
          return status;
        }
      }
    }
    return tombstone_iter->status();
  }

  Status ValidateRangeTombstones(const IngestedFileInfo& newer_file,
                                 TableReader* newer_reader,
                                 const IngestedFileInfo& older_file,
                                 TableReader* older_reader,
                                 bool allow_equal_sequence_numbers) const {
    std::unique_ptr<FragmentedRangeTombstoneIterator> newer_iter(
        newer_reader->NewRangeTombstoneIterator(read_options_));
    std::unique_ptr<FragmentedRangeTombstoneIterator> older_iter(
        older_reader->NewRangeTombstoneIterator(read_options_));
    if (newer_iter == nullptr || older_iter == nullptr) {
      return Status::OK();
    }

    for (newer_iter->SeekToFirst(); newer_iter->Valid(); newer_iter->Next()) {
      older_iter->Seek(newer_iter->start_key());
      while (older_iter->Valid() && ucmp_->Compare(older_iter->start_key(),
                                                   newer_iter->end_key()) < 0) {
        if (ucmp_->Compare(newer_iter->start_key(), older_iter->end_key()) <
                0 &&
            !IsOrdered(newer_iter->seq(), older_iter->seq(),
                       allow_equal_sequence_numbers)) {
          return InvalidOrder(newer_file, newer_iter->seq(), older_file,
                              older_iter->seq());
        }
        older_iter->Next();
      }
    }
    return Status::OK();
  }

  const Comparator* const ucmp_;
  const SliceTransform* const prefix_extractor_;
  const bool allow_data_in_errors_;
  bool* const source_priority_required_;
  ReadOptions read_options_;
};

bool HasPointEntries(const IngestedFileInfo& file) {
  return file.num_entries > 0;
}

bool CanSkipSequenceOrderScan(const IngestedFileInfo& newer_file,
                              const IngestedFileInfo& older_file,
                              bool allow_equal_sequence_numbers) {
  if (newer_file.smallest_seqno == kMaxSequenceNumber ||
      older_file.largest_seqno == kMaxSequenceNumber) {
    return false;
  }
  if (newer_file.smallest_seqno > older_file.largest_seqno) {
    return true;
  }
  if (newer_file.smallest_seqno != older_file.largest_seqno) {
    return false;
  }
  if ((!allow_equal_sequence_numbers && newer_file.smallest_seqno != 0) ||
      !newer_file.entry_stats_initialized ||
      !older_file.entry_stats_initialized) {
    return false;
  }
  if (newer_file.table_properties.num_merge_operands > 0 &&
      older_file.table_properties.num_merge_operands > 0) {
    return false;
  }
  if (newer_file.table_properties.num_deletions > 0 ||
      older_file.table_properties.num_deletions > 0) {
    return false;
  }
  return !(newer_file.num_range_deletions > 0 && HasPointEntries(older_file));
}

IngestedFileInfo LiveFileInfo(const FileMetaData& file, int level) {
  IngestedFileInfo info;
  info.external_file_path = MakeTableFileName(file.fd.GetNumber());
  info.smallest_internal_key = file.smallest;
  info.largest_internal_key = file.largest;
  info.fd = file.fd;
  info.file_size = file.fd.GetFileSize();
  // Live entry statistics are lazily populated and not immutable metadata.
  info.num_entries = 0;
  info.num_range_deletions = 0;
  info.entry_stats_initialized = false;
  info.oldest_ancestor_time = file.oldest_ancestor_time;
  info.smallest_seqno = file.fd.smallest_seqno;
  info.largest_seqno = file.fd.largest_seqno;
  info.assigned_seqno = file.fd.largest_seqno;
  info.declared_level = level;
  info.existing_file = true;
  return info;
}

Status RejectExternalBlobReference(const BlobIndex& blob_index,
                                   const std::string& external_file) {
  if (blob_index.IsInlined() || blob_index.IsSameFile()) {
    return Status::OK();
  }
  return Status::NotSupported("LSM edit cannot install SST " + external_file +
                              " because it references external blob file " +
                              std::to_string(blob_index.file_number()));
}

Status ValidateNoExternalBlobReferences(const ParsedInternalKey& parsed,
                                        const Slice& value,
                                        const std::string& external_file) {
  if (parsed.type == kTypeBlobIndex) {
    BlobIndex blob_index;
    Status status = blob_index.DecodeFrom(value);
    if (!status.ok()) {
      return status;
    }
    return RejectExternalBlobReference(blob_index, external_file);
  }
  if (parsed.type == kTypeWideColumnEntity) {
    return WideColumnSerialization::ForEachBlobFileNumber(
        value, [&](const BlobIndex& blob_index) {
          return RejectExternalBlobReference(blob_index, external_file);
        });
  }
  return Status::OK();
}

class LsmEditMergeIterator : public InternalIterator {
 public:
  LsmEditMergeIterator(const InternalKeyComparator& icmp,
                       const ReadOptions& read_options,
                       const SliceTransform* prefix_extractor,
                       const std::vector<IngestedFileInfo*>& files,
                       const std::vector<std::unique_ptr<TableReader>>& readers,
                       size_t compaction_readahead_size,
                       const Slice* lower_bound = nullptr,
                       const Slice* upper_bound = nullptr)
      : ucmp_(icmp.user_comparator()), read_options_(read_options) {
    inputs_.reserve(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
      inputs_.emplace_back();
      auto& input = inputs_.back();
      input.reader = readers[i].get();
      input.reset_sequence = files[i]->zero_sequence_numbers;
      input.iter.reset(input.reader->NewIterator(
          read_options_, prefix_extractor, nullptr, false,
          TableReaderCaller::kCompaction, compaction_readahead_size));
      if (lower_bound || upper_bound) {
        input.unclipped_iter = std::move(input.iter);
        input.iter = std::make_unique<ClippingIterator>(
            input.unclipped_iter.get(), lower_bound, upper_bound, &icmp);
      }
    }
  }

  bool Valid() const override { return status_.ok() && !heap_.empty(); }
  Status status() const override { return status_; }
  Slice key() const override { return inputs_[heap_.front()].key; }
  Slice value() const override { return inputs_[heap_.front()].value; }

  void SeekToFirst() override {
    status_ = Status::OK();
    heap_.clear();
    for (size_t i = 0; i < inputs_.size(); ++i) {
      inputs_[i].iter->SeekToFirst();
      Refresh(i);
      if (!status_.ok()) {
        heap_.clear();
        return;
      }
      if (inputs_[i].iter->Valid() && status_.ok()) {
        heap_.push_back(i);
      }
    }
    std::make_heap(heap_.begin(), heap_.end(),
                   [this](size_t a, size_t b) { return Later(a, b); });
  }

  void Next() override {
    assert(Valid());
    const auto& winner = inputs_[heap_.front()];
    previous_user_key_.assign(winner.parsed.user_key.data(),
                              winner.parsed.user_key.size());
    const SequenceNumber seq = winner.parsed.sequence;
    const bool merge_operand = winner.parsed.type == kTypeMerge;
    const bool single_delete = winner.parsed.type == kTypeSingleDeletion;
    Advance();
    // An equal-sequence point overwrite hides older puts/deletes. Merge
    // operands instead need their older operands and base value.
    while (Valid() &&
           ucmp_->Compare(previous_user_key_,
                          inputs_[heap_.front()].parsed.user_key) == 0 &&
           seq == inputs_[heap_.front()].parsed.sequence) {
      if (single_delete ||
          inputs_[heap_.front()].parsed.type == kTypeSingleDeletion) {
        status_ = Status::NotSupported(
            "LSM edit does not support equal-sequence SingleDelete ties");
        heap_.clear();
        return;
      }
      if (merge_operand) {
        break;
      }
      Advance();
    }
  }

  void Seek(const Slice&) override { UnsupportedSeek(); }
  void SeekForPrev(const Slice&) override { UnsupportedSeek(); }
  void SeekToLast() override { UnsupportedSeek(); }
  void Prev() override { UnsupportedSeek(); }

 private:
  struct Input {
    TableReader* reader = nullptr;
    std::unique_ptr<InternalIterator> unclipped_iter;
    std::unique_ptr<InternalIterator> iter;
    bool reset_sequence = false;
    ParsedInternalKey parsed;
    std::string key;
    Slice value;
    std::string resolved_entity;
    std::vector<PinnableSlice> blobs;
  };

  bool Later(size_t a, size_t b) const {
    const auto& first = inputs_[a].parsed;
    const auto& second = inputs_[b].parsed;
    const int cmp = ucmp_->Compare(first.user_key, second.user_key);
    if (cmp != 0) {
      return cmp > 0;
    }
    if (first.sequence != second.sequence) {
      return first.sequence < second.sequence;
    }
    return a < b;
  }

  Status ResolveBlob(Input& input, const BlobIndex& index,
                     PinnableSlice* value) {
    const auto* reader = input.reader->GetSameFileBlobReader();
    if (!index.IsSameFile() || reader == nullptr) {
      return Status::NotSupported(
          "LSM edit merge cannot resolve this blob reference");
    }
    return reader->GetSameFileBlob(read_options_, index, 0, kWholeBlobLength,
                                   BlobVerifyPolicy::kVerifyIfPresent, value);
  }

  Status ResolveValue(Input& input) {
    input.value = input.iter->value();
    input.blobs.clear();
    input.resolved_entity.clear();
    if (input.parsed.type == kTypeBlobIndex) {
      BlobIndex index;
      Status s = index.DecodeFrom(input.value);
      if (!s.ok() || index.IsInlined()) {
        return s;
      }
      input.blobs.resize(1);
      s = ResolveBlob(input, index, &input.blobs.front());
      if (s.ok()) {
        input.value = input.blobs.front();
        input.parsed.type = kTypeValue;
      }
      return s;
    }
    if (input.parsed.type != kTypeWideColumnEntity) {
      return Status::OK();
    }
    bool has_blobs = false;
    Status s = WideColumnSerialization::HasBlobColumns(input.value, has_blobs);
    if (!s.ok() || !has_blobs) {
      return s;
    }
    WideColumns columns;
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    s = WideColumnSerialization::Deserialize(input.value, columns,
                                             &blob_columns);
    if (!s.ok()) {
      return s;
    }
    input.blobs.resize(blob_columns.size());
    for (size_t i = 0; i < blob_columns.size(); ++i) {
      const auto& [column, index] = blob_columns[i];
      s = ResolveBlob(input, index, &input.blobs[i]);
      if (!s.ok()) {
        return s;
      }
      columns[column] = WideColumn(columns[column].name(), input.blobs[i]);
    }
    s = WideColumnSerialization::Serialize(columns, input.resolved_entity);
    input.value = input.resolved_entity;
    return s;
  }

  void Refresh(size_t index) {
    auto& input = inputs_[index];
    if (!input.iter->Valid()) {
      if (status_.ok()) {
        status_ = input.iter->status();
      }
      return;
    }
    status_ = ParseInternalKey(input.iter->key(), &input.parsed, false);
    if (!status_.ok()) {
      return;
    }
    if (input.reset_sequence) {
      if (input.parsed.type == kTypeMerge) {
        status_ = Status::InvalidArgument(
            "LSM edit zero_sequence_numbers does not support merge operands");
        return;
      }
      input.parsed.sequence = 0;
      if (input.parsed.type == kTypeSingleDeletion) {
        input.parsed.type = kTypeDeletion;
      }
    }
    status_ = ResolveValue(input);
    if (!status_.ok()) {
      return;
    }
    input.key.clear();
    AppendInternalKey(&input.key, input.parsed);
    input.parsed.user_key = ExtractUserKey(input.key);
  }

  void Advance() {
    const auto compare = [this](size_t a, size_t b) { return Later(a, b); };
    std::pop_heap(heap_.begin(), heap_.end(), compare);
    const size_t index = heap_.back();
    heap_.pop_back();
    inputs_[index].iter->Next();
    Refresh(index);
    if (inputs_[index].iter->Valid() && status_.ok()) {
      heap_.push_back(index);
      std::push_heap(heap_.begin(), heap_.end(), compare);
    }
  }

  void UnsupportedSeek() {
    status_ = Status::NotSupported("LSM edit merge uses a forward iterator");
    heap_.clear();
  }

  const Comparator* ucmp_;
  const ReadOptions& read_options_;
  std::vector<Input> inputs_;
  std::vector<size_t> heap_;
  std::string previous_user_key_;
  Status status_;
};

}  // namespace

bool ExternalSstFileIngestionJob::SupportsAtomicReplaceRangeTombstone() const {
  if (lsm_edit_spec_.has_value()) {
    return assign_lsm_edit_global_seqnos_ &&
           cfd_->is_delete_range_supported() && ucmp_->timestamp_size() == 0;
  }
  return cfd_->is_delete_range_supported() &&
         cfd_->ioptions().compaction_style == kCompactionStyleUniversal &&
         ingestion_options_.allow_global_seqno &&
         !ingestion_options_.allow_db_generated_files &&
         !ingestion_options_.snapshot_consistency &&
         ucmp_->timestamp_size() == 0;
}

bool ExternalSstFileIngestionJob::CanUseAtomicReplaceRangeTombstone(
    const std::optional<RangeOpt>& atomic_replace_range) const {
  return atomic_replace_range.has_value() &&
         atomic_replace_range->start.has_value() &&
         atomic_replace_range->limit.has_value() &&
         SupportsAtomicReplaceRangeTombstone() &&
         !ingestion_options_.fail_if_not_bottommost_level;
}

bool ExternalSstFileIngestionJob::HasPartialOverlap(
    const VersionStorageInfo* vstorage) const {
  assert(vstorage != nullptr);
  assert(atomic_replace_range_.has_value());
  assert(!atomic_replace_range_->unset());
  for (int level = 0; level < cfd_->NumberLevels(); ++level) {
    for (const auto* file : vstorage->LevelFiles(level)) {
      if (file_range_checker_.Overlaps(*atomic_replace_range_, file->smallest,
                                       file->largest) &&
          !file_range_checker_.Contains(*atomic_replace_range_, file->smallest,
                                        file->largest)) {
        return true;
      }
    }
  }
  return false;
}

size_t ExternalSstFileIngestionJob::NumFilesToPrepare(
    size_t num_external_files,
    const std::optional<RangeOpt>& atomic_replace_range) const {
  // Eligibility is known only after reading the input files in Prepare().
  const bool reserve_lsm_edit_tombstone =
      lsm_edit_spec_.has_value() &&
      lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kFilesInRange &&
      atomic_replace_range.has_value() && atomic_replace_range->start &&
      atomic_replace_range->limit && cfd_->is_delete_range_supported();
  // An empty bounded slot can straddle at most two files at each level > 0,
  // and each selected file produces at most two split outputs.
  size_t level_split_outputs = 0;
  if (lsm_edit_spec_) {
    // At a nonzero level each cut straddles at most one file. A selected file
    // with K interior cuts has K+1 <= 2K output partitions.
    for (const auto& split : lsm_edit_spec_->level_splits) {
      level_split_outputs += 2 * split.cut_points.size();
    }
  }
  return level_split_outputs +
         num_external_files *
             (lsm_edit_spec_ && lsm_edit_spec_->merge_incoming_files ? 2 : 1) +
         static_cast<size_t>(
             reserve_lsm_edit_tombstone ||
             CanUseAtomicReplaceRangeTombstone(atomic_replace_range)) +
         (lsm_edit_spec_ && lsm_edit_spec_->split_existing_files
              ? 4 * static_cast<size_t>(cfd_->NumberLevels())
              : 0);
}

Status ExternalSstFileIngestionJob::PrepareAtomicReplaceRangeTombstone(
    const Slice& start, const Slice& limit, uint64_t file_number,
    SuperVersion* super_version) {
  assert(cfd_->is_delete_range_supported());
  assert(assign_lsm_edit_global_seqnos_ ||
         cfd_->ioptions().compaction_style == kCompactionStyleUniversal);
  assert(assign_lsm_edit_global_seqnos_ ||
         ingestion_options_.allow_global_seqno);
  assert(assign_lsm_edit_global_seqnos_ ||
         !ingestion_options_.allow_db_generated_files);
  assert(!ingestion_options_.fail_if_not_bottommost_level);
  assert(!ingestion_options_.snapshot_consistency);
  assert(ucmp_->timestamp_size() == 0);

  const uint32_t path_id = 0;
  const std::string file_path =
      TableFileName(cfd_->ioptions().cf_paths, file_number, path_id);
  ExternalSstFileInfo external_file_info;
  Status status;
  {
    Options options(
        BuildDBOptions(db_options_, mutable_db_options_),
        BuildColumnFamilyOptions(cfd_->initial_cf_options(),
                                 super_version->mutable_cf_options));
    SstFileWriter writer(env_options_, options, ucmp_);
    status = writer.Open(
        file_path, super_version->mutable_cf_options.default_write_temperature);
    if (status.ok()) {
      status = writer.DeleteRange(start, limit);
    }
    if (status.ok()) {
      status = writer.Finish(&external_file_info);
    }
  }
  if (!status.ok()) {
    fs_->DeleteFile(file_path, IOOptions(), nullptr).PermitUncheckedError();
    return status;
  }

  IngestedFileInfo tombstone;
  tombstone.file_temperature =
      super_version->mutable_cf_options.default_write_temperature;
  tombstone.prefetch_lmax_index_and_filter_blocks =
      ingestion_options_.prefetch_lmax_index_and_filter_blocks;
  status = GetIngestedFileInfo(file_path, file_number,
                               external_file_info.prepared_file_info.get(),
                               &tombstone, super_version);
  if (!status.ok()) {
    fs_->DeleteFile(file_path, IOOptions(), nullptr).PermitUncheckedError();
    return status;
  }

  tombstone.internal_file_path = file_path;
  // Written by this job directly into the DB directory
  int64_t now = 0;
  if (clock_->GetCurrentTime(&now).ok()) {
    tombstone.db_ownership_start_time = lossless_cast<uint64_t>(now);
  }
  tombstone.copy_file = true;
  tombstone.generated_for_ingestion = true;
  tombstone.file_checksum = external_file_info.file_checksum;
  tombstone.file_checksum_func_name =
      external_file_info.file_checksum_func_name;
  atomic_replace_range_tombstone_.emplace(std::move(tombstone));
  return lsm_edit_spec_
             ? RegisterGeneratedFile(&*atomic_replace_range_tombstone_)
             : Status::OK();
}

void ExternalSstFileIngestionJob::ActivateAtomicReplaceRangeTombstone() {
  assert(atomic_replace_range_tombstone_.has_value());
  // Prepare rejects an empty external file list and verifies every input file
  // is contained in the replacement range. The covering tombstone must
  // therefore overlap at least one input file.
  assert(!files_to_ingest_.empty());
  if (assign_lsm_edit_global_seqnos_) {
    assert(atomic_replace_range_tombstone_->declared_level >= 0);
  }
  file_batches_to_ingest_.clear();
  files_to_ingest_.emplace_back(std::move(*atomic_replace_range_tombstone_));
  std::rotate(files_to_ingest_.begin(), files_to_ingest_.end() - 1,
              files_to_ingest_.end());
  atomic_replace_range_tombstone_.reset();
  atomic_replace_range_tombstone_active_ = true;
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);
  assert(files_overlap_);
  DivideInputFilesIntoBatches();
}

Status ExternalSstFileIngestionJob::PickLsmEditTombstoneLevel(
    const autovector<std::pair<int, const FileMetaData*>>& straddling_files) {
  assert(assign_lsm_edit_global_seqnos_);
  assert(atomic_replace_range_tombstone_.has_value());
  int deepest_level = 0;
  for (const IngestedFileInfo& file : files_to_ingest_) {
    deepest_level = std::max(deepest_level, file.declared_level);
  }
  if (deepest_level == 0) {
    atomic_replace_range_tombstone_->declared_level = 0;
    return Status::OK();
  }
  // Reads stop searching deeper files after observing a covering tombstone.
  // It must therefore follow the replacement data in the level search order.
  int deepest_tombstone_level = cfd_->NumberLevels() - 1;
  for (const auto& straddler : straddling_files) {
    deepest_tombstone_level =
        std::min(deepest_tombstone_level, std::max(0, straddler.first - 1));
  }
  for (int level = deepest_level + 1; level <= deepest_tombstone_level;
       ++level) {
    if (DeclaredFileFitsInLevel(&*atomic_replace_range_tombstone_, level)) {
      atomic_replace_range_tombstone_->declared_level = level;
      return Status::OK();
    }
  }
  if (!lsm_edit_spec_->allow_replacement_file_relocation) {
    return Status::InvalidArgument(
        "LSM edit range replacement requires room for a tombstone below the "
        "declared files; allow_replacement_file_relocation can move them up");
  }

  int tombstone_level = deepest_tombstone_level;
  while (tombstone_level > 0 &&
         !DeclaredFileFitsInLevel(&*atomic_replace_range_tombstone_,
                                  tombstone_level)) {
    --tombstone_level;
  }
  atomic_replace_range_tombstone_->declared_level = tombstone_level;
  RelocateLsmEditReplacementFiles(tombstone_level);
  return Status::OK();
}

void ExternalSstFileIngestionJob::RelocateLsmEditReplacementFiles(
    int tombstone_level) {
  std::stable_sort(files_to_ingest_.begin(), files_to_ingest_.end(),
                   [](const auto& a, const auto& b) {
                     return a.declared_level > b.declared_level;
                   });
  int next_level = tombstone_level - 1;
  for (size_t begin = 0; begin < files_to_ingest_.size();) {
    const int source_level = files_to_ingest_[begin].declared_level;
    size_t end = begin + 1;
    while (end < files_to_ingest_.size() &&
           files_to_ingest_[end].declared_level == source_level) {
      ++end;
    }
    int target_level = std::max(0, std::min(source_level, next_level));
    while (target_level > 0 &&
           !std::all_of(files_to_ingest_.begin() + begin,
                        files_to_ingest_.begin() + end,
                        [this, target_level](const IngestedFileInfo& file) {
                          return DeclaredFileFitsInLevel(&file, target_level);
                        })) {
      --target_level;
    }
    for (size_t i = begin; i < end; ++i) {
      files_to_ingest_[i].declared_level = target_level;
    }
    next_level = target_level - 1;
    begin = end;
  }
}

Status ExternalSstFileIngestionJob::RecordDeclaredLevels(
    const std::vector<std::string>& external_files_paths) {
  assert(lsm_edit_spec_.has_value());
  const std::vector<int>& levels = lsm_edit_spec_->levels;
  const size_t num_files = files_to_ingest_.size();
  if (levels.size() != num_files ||
      (!lsm_edit_spec_->zero_sequence_numbers.empty() &&
       lsm_edit_spec_->zero_sequence_numbers.size() != num_files)) {
    return Status::InvalidArgument(
        "LSM edit declares " + std::to_string(levels.size()) + " levels for " +
        std::to_string(num_files) + " files");
  }
  if (ucmp_->timestamp_size() > 0) {
    return Status::NotSupported(
        "LSM edit is not supported on a column family with user-defined "
        "timestamps");
  }

  const int num_levels = cfd_->NumberLevels();
  Status split_status = ValidateLevelSplits();
  if (!split_status.ok()) {
    return split_status;
  }
  const bool fifo_compaction =
      cfd_->ioptions().compaction_style == kCompactionStyleFIFO;
  // Files sharing a level above L0 must be disjoint, which is the invariant
  // that level maintains. Group by level so the check is per level rather than
  // over all input files, which may legitimately overlap across levels.
  std::map<int, autovector<const IngestedFileInfo*>> files_per_level;
  for (size_t i = 0; i < num_files; i++) {
    const int level = levels[i];
    if (level < 0 || level >= num_levels) {
      return Status::InvalidArgument(
          "LSM edit declares level " + std::to_string(level) + " for file " +
          external_files_paths[i] + ", outside [0, " +
          std::to_string(num_levels) + ")");
    }
    if (fifo_compaction && level != 0) {
      return Status::InvalidArgument(
          "LSM edit declares nonzero level " + std::to_string(level) +
          " for a column family using FIFO compaction");
    }
    files_to_ingest_[i].declared_level = level;
    if (!lsm_edit_spec_->zero_sequence_numbers.empty()) {
      files_to_ingest_[i].zero_sequence_numbers =
          lsm_edit_spec_->zero_sequence_numbers[i];
    }
    if (level > 0 && !lsm_edit_spec_->merge_incoming_files) {
      files_per_level[level].push_back(&files_to_ingest_[i]);
    }
  }

  for (auto& [level, files] : files_per_level) {
    std::sort(files.begin(), files.end(), file_range_checker_);
    for (size_t i = 0; i + 1 < files.size(); i++) {
      if (file_range_checker_.Overlaps(*files[i], *files[i + 1],
                                       /* known_sorted= */ true)) {
        return Status::InvalidArgument(
            "LSM edit declares level " + std::to_string(level) +
            " for files whose key ranges overlap each other");
      }
    }
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::AssignLsmEditGlobalSequenceNumbers(
    SequenceNumber last_seqno) {
  assert(assign_lsm_edit_global_seqnos_);
  if (files_to_ingest_.size() > kMaxSequenceNumber - last_seqno) {
    return Status::InvalidArgument(
        "LSM edit has insufficient sequence numbers for range replacement");
  }
  std::vector<IngestedFileInfo*> files;
  files.reserve(files_to_ingest_.size());
  for (IngestedFileInfo& file : files_to_ingest_) {
    files.push_back(&file);
  }
  std::stable_sort(
      files.begin(), files.end(), [](const auto* a, const auto* b) {
        if (a->generated_for_ingestion != b->generated_for_ingestion) {
          return a->generated_for_ingestion;
        }
        return a->declared_level > b->declared_level;
      });
  for (IngestedFileInfo* file : files) {
    file->assigned_seqno = ++last_seqno;
    file->smallest_seqno = last_seqno;
    file->largest_seqno = last_seqno;
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::ValidateDeclaredLevelSequenceOrder(
    SuperVersion* super_version) {
  assert(lsm_edit_spec_.has_value());
  const size_t num_files = files_to_ingest_.size();
  std::vector<std::unique_ptr<TableReader>> readers(num_files);
  LsmEditSequenceOrderValidator validator(
      cfd_->internal_comparator(),
      super_version->mutable_cf_options.prefix_extractor.get(),
      ingestion_options_.fill_cache, db_options_.allow_data_in_errors,
      &requires_source_priority_for_equal_sequences_);

  std::vector<std::pair<IngestedFileInfo*, size_t>> files_by_range;
  files_by_range.reserve(num_files);
  for (size_t i = 0; i < num_files; ++i) {
    files_by_range.emplace_back(&files_to_ingest_[i], i);
  }
  std::sort(files_by_range.begin(), files_by_range.end(),
            [this](const auto& first, const auto& second) {
              return file_range_checker_(first.first, second.first);
            });

  // Sorting makes candidate selection O(n log n + P), where P is the number
  // of overlapping pairs. Disjoint sequence-number ranges avoid table scans,
  // and each TableReader is opened at most once. A large P with ambiguous
  // sequence ranges still warrants a tagged multiway scan in the future.
  for (size_t i = 0; i < num_files; ++i) {
    for (size_t j = i + 1; j < num_files; ++j) {
      IngestedFileInfo* first = files_by_range[i].first;
      IngestedFileInfo* second = files_by_range[j].first;
      const size_t first_index = files_by_range[i].second;
      const size_t second_index = files_by_range[j].second;
      if (!file_range_checker_.Overlaps(*first, *second,
                                        /* known_sorted= */ true)) {
        break;
      }

      IngestedFileInfo* newer = first;
      IngestedFileInfo* older = second;
      size_t newer_index = first_index;
      size_t older_index = second_index;
      const bool same_level = first->declared_level == second->declared_level;
      if (same_level && first->declared_level > 0) {
        return Status::InvalidArgument(
            "LSM edit added or split files overlap at the same level");
      }
      assert(!same_level || first->declared_level == 0);
      if ((same_level && first_index < second_index) ||
          (!same_level && first->declared_level > second->declared_level)) {
        std::swap(newer, older);
        std::swap(newer_index, older_index);
      }
      const bool allow_equal_sequence_numbers = same_level;
      if (CanSkipSequenceOrderScan(*newer, *older,
                                   allow_equal_sequence_numbers)) {
        if (newer->smallest_seqno == older->largest_seqno &&
            HasPointEntries(*newer) && HasPointEntries(*older)) {
          requires_source_priority_for_equal_sequences_ = true;
        }
        continue;
      }

      for (const size_t index : {newer_index, older_index}) {
        if (readers[index] != nullptr) {
          continue;
        }
        IngestedFileInfo* file = &files_to_ingest_[index];
        Status status =
            ResetTableReader(file->external_file_path, file->fd.GetNumber(),
                             file->user_defined_timestamps_persisted,
                             super_version, file, &readers[index]);
        if (!status.ok()) {
          return status;
        }
      }
      Status status = validator.Validate(*newer, readers[newer_index].get(),
                                         *older, readers[older_index].get(),
                                         allow_equal_sequence_numbers);
      if (!status.ok()) {
        return status;
      }
    }
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::DeleteLiveFile(int level,
                                                   const FileMetaData* file) {
  assert(file != nullptr);
  if (file->being_compacted) {
    return Status::InvalidArgument(
        "Atomic replace range overlaps with pending compaction");
  }
  edit_.DeleteFile(level, file->fd.GetNumber());
  deleted_file_compaction_inputs_.emplace_back(level, file);
  return Status::OK();
}

bool ExternalSstFileIngestionJob::NeedsSurvivingSequenceValidation() const {
  const bool pure_level_split = !lsm_edit_spec_->level_splits.empty() &&
                                lsm_edit_spec_->levels.empty() &&
                                lsm_edit_spec_->delete_files.empty();
  // A pure split preserves data and levels. Range replacement removes covered
  // files and probes or tombstones retained data before installation.
  return !pure_level_split &&
         lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kExplicitFiles;
}

std::vector<ExternalSstFileIngestionJob::DeclaredLevelSurvivor>
ExternalSstFileIngestionJob::GetDeclaredLevelSurvivors(
    const VersionStorageInfo* vstorage,
    const VersionEdit::DeletedFiles& deleted) const {
  std::vector<DeclaredLevelSurvivor> survivors;
  for (size_t i = 0; i < files_to_ingest_.size(); ++i) {
    const IngestedFileInfo& added = files_to_ingest_[i];
    for (int level = 0; level < cfd_->NumberLevels(); ++level) {
      if (level > 0 && level == added.declared_level) {
        continue;
      }
      const auto& files = vstorage->LevelFiles(level);
      auto live = files.begin();
      if (level > 0) {
        live = std::lower_bound(
            files.begin(), files.end(), added.smallest_internal_key,
            [this](const FileMetaData* f, const InternalKey& key) {
              return sstableKeyCompare(ucmp_, f->largest, key) < 0;
            });
      }
      for (; live != files.end(); ++live) {
        if (level > 0 && sstableKeyCompare(ucmp_, (*live)->smallest,
                                           added.largest_internal_key) > 0) {
          break;
        }
        if (deleted.count({level, (*live)->fd.GetNumber()}) == 0 &&
            file_range_checker_.Overlaps(added, (*live)->smallest,
                                         (*live)->largest)) {
          survivors.push_back({i, level, *live});
        }
      }
    }
  }
  return survivors;
}

Status ExternalSstFileIngestionJob::RevalidateDeclaredLevelSurvivors(
    const VersionStorageInfo* vstorage) const {
  if (!NeedsSurvivingSequenceValidation()) {
    return Status::OK();
  }
  std::vector<std::pair<int, uint64_t>> current;
  for (const auto& survivor :
       GetDeclaredLevelSurvivors(vstorage, edit_.GetDeletedFiles())) {
    current.emplace_back(survivor.level, survivor.file->fd.GetNumber());
  }
  std::sort(current.begin(), current.end());
  current.erase(std::unique(current.begin(), current.end()), current.end());
  if (current != prepared_sequence_survivors_) {
    return Status::TryAgain(
        "LSM edit overlapping live files changed after sequence validation; "
        "retry the edit");
  }
  return Status::OK();
}

Status
ExternalSstFileIngestionJob::ValidateDeclaredLevelSurvivingFileSequenceOrder(
    SuperVersion* super_version) {
  assert(lsm_edit_spec_.has_value());
  TEST_SYNC_POINT_CALLBACK(
      "ExternalSstFileIngestionJob::SurvivingFileSequenceOrder:Prepare",
      nullptr);
  VersionEdit::DeletedFiles deleted;
  for (const auto& deletion : lsm_edit_spec_->delete_files) {
    deleted.emplace(deletion.level, deletion.file_number);
  }
  for (const auto& deletion : rewritten_existing_deletions_) {
    deleted.emplace(deletion.level, deletion.file_number);
  }
  const auto* vstorage = super_version->current->storage_info();
  assert(lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kExplicitFiles);
  std::vector<std::unique_ptr<TableReader>> added_readers(
      files_to_ingest_.size());
  LsmEditSequenceOrderValidator validator(
      cfd_->internal_comparator(),
      super_version->mutable_cf_options.prefix_extractor.get(),
      ingestion_options_.fill_cache, db_options_.allow_data_in_errors,
      &requires_source_priority_for_equal_sequences_);
  ReadOptions read_options;
  read_options.fill_cache = ingestion_options_.fill_cache;
  for (const auto& candidate : GetDeclaredLevelSurvivors(vstorage, deleted)) {
    const size_t added_index = candidate.added_index;
    const int level = candidate.level;
    const FileMetaData* live_file = candidate.file;
    prepared_sequence_survivors_.emplace_back(level, live_file->fd.GetNumber());
    IngestedFileInfo& added_file = files_to_ingest_[added_index];
    assert(added_file.declared_level >= 0);
    IngestedFileInfo survivor = LiveFileInfo(*live_file, level);
    const bool added_file_is_newer = added_file.declared_level <= level;
    const IngestedFileInfo& newer = added_file_is_newer ? added_file : survivor;
    const IngestedFileInfo& older = added_file_is_newer ? survivor : added_file;
    const bool allow_equal_sequence_numbers =
        level == 0 && added_file.declared_level == 0;
    if (CanSkipSequenceOrderScan(newer, older, allow_equal_sequence_numbers)) {
      if (newer.smallest_seqno == older.largest_seqno &&
          HasPointEntries(newer) && HasPointEntries(older)) {
        requires_source_priority_for_equal_sequences_ = true;
      }
      continue;
    }

    if (added_readers[added_index] == nullptr) {
      Status status = ResetTableReader(
          added_file.external_file_path, added_file.fd.GetNumber(),
          added_file.user_defined_timestamps_persisted, super_version,
          &added_file, &added_readers[added_index]);
      if (!status.ok()) {
        return status;
      }
    }

    TableCache::TypedHandle* live_handle = nullptr;
    TableReader* live_reader = nullptr;
    FileOptions file_options{env_options_};
    file_options.temperature = live_file->temperature;
    Status status = cfd_->table_cache()->FindTable(
        read_options, file_options, cfd_->internal_comparator(), *live_file,
        &live_handle, super_version->mutable_cf_options, &live_reader,
        /*no_io=*/false, cfd_->internal_stats()->GetFileReadHist(level),
        /*skip_filters=*/false, level,
        /*prefetch_index_and_filter_in_cache=*/false,
        /*max_file_size_for_l0_meta_pin=*/0, live_file->temperature);
    if (status.ok()) {
      const std::shared_ptr<const TableProperties> properties =
          live_reader->GetTableProperties();
      if (properties) {
        survivor.num_entries = properties->num_entries;
        survivor.num_range_deletions = properties->num_range_deletions;
        survivor.table_properties.num_deletions = properties->num_deletions;
        survivor.table_properties.num_merge_operands =
            properties->num_merge_operands;
        survivor.entry_stats_initialized = true;
      }
      bool skip_scan =
          CanSkipSequenceOrderScan(newer, older, allow_equal_sequence_numbers);
      TEST_SYNC_POINT_CALLBACK(
          "ExternalSstFileIngestionJob::SurvivingFileSequenceOrder:"
          "SkipScan",
          &skip_scan);
      if (!skip_scan) {
        status = added_file_is_newer
                     ? validator.Validate(
                           added_file, added_readers[added_index].get(),
                           survivor, live_reader, allow_equal_sequence_numbers)
                     : validator.Validate(survivor, live_reader, added_file,
                                          added_readers[added_index].get(),
                                          allow_equal_sequence_numbers);
      } else if (newer.smallest_seqno == older.largest_seqno &&
                 HasPointEntries(newer) && HasPointEntries(older)) {
        requires_source_priority_for_equal_sequences_ = true;
      }
    }
    if (live_handle != nullptr) {
      cfd_->table_cache()->get_cache().Release(live_handle);
    }
    if (!status.ok()) {
      return status;
    }
  }
  std::sort(prepared_sequence_survivors_.begin(),
            prepared_sequence_survivors_.end());
  prepared_sequence_survivors_.erase(
      std::unique(prepared_sequence_survivors_.begin(),
                  prepared_sequence_survivors_.end()),
      prepared_sequence_survivors_.end());
  return Status::OK();
}

Status ExternalSstFileIngestionJob::AddExplicitFileDeletions(
    const VersionStorageInfo* vstorage) {
  assert(lsm_edit_spec_.has_value());
  assert(lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kExplicitFiles);
  for (const LsmEditFileDeletion& deletion : lsm_edit_spec_->delete_files) {
    if (deletion.level < 0 || deletion.level >= cfd_->NumberLevels()) {
      return Status::InvalidArgument(
          "LSM edit deletes file " + std::to_string(deletion.file_number) +
          " from level " + std::to_string(deletion.level) +
          ", outside the column family's levels");
    }

    const FileMetaData* file_to_delete = nullptr;
    for (const FileMetaData* file : vstorage->LevelFiles(deletion.level)) {
      if (file->fd.GetNumber() == deletion.file_number) {
        file_to_delete = file;
        break;
      }
    }
    if (file_to_delete == nullptr) {
      return Status::InvalidArgument(
          "LSM edit deletes file " + std::to_string(deletion.file_number) +
          " that is not live at level " + std::to_string(deletion.level));
    }
    if (!atomic_replace_range_->unset() &&
        !file_range_checker_.Contains(*atomic_replace_range_,
                                      file_to_delete->smallest,
                                      file_to_delete->largest)) {
      return Status::InvalidArgument(
          "LSM edit range does not contain explicitly deleted file " +
          std::to_string(deletion.file_number));
    }
    Status status = DeleteLiveFile(deletion.level, file_to_delete);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::Prepare(
    const std::vector<std::string>& external_files_paths,
    const std::vector<std::string>& files_checksums,
    const std::vector<std::string>& files_checksum_func_names,
    const std::vector<const PreparedFileInfo*>& file_infos,
    const std::optional<RangeOpt>& atomic_replace_range,
    const Temperature& file_temperature, uint64_t next_file_number,
    SuperVersion* sv) {
  Status status;

  // Read the information of files we are ingesting.
  for (size_t i = 0; i < external_files_paths.size(); i++) {
    const std::string& file_path = external_files_paths[i];
    IngestedFileInfo file_to_ingest;
    // For temperature, first assume it matches provided hint
    file_to_ingest.file_temperature = file_temperature;
    file_to_ingest.prefetch_lmax_index_and_filter_blocks =
        ingestion_options_.prefetch_lmax_index_and_filter_blocks;
    const PreparedFileInfo* prepared_file_info =
        file_infos.empty() ? nullptr : file_infos[i];
    status = GetIngestedFileInfo(file_path, next_file_number++,
                                 prepared_file_info, &file_to_ingest, sv);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to get ingested file info: %s: %s",
                     file_path.c_str(), status.ToString().c_str());
      return status;
    }

    // Files generated in another DB or CF may have a different column family
    // ID, so we let it pass here.
    if (file_to_ingest.cf_id !=
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily &&
        file_to_ingest.cf_id != cfd_->GetID() &&
        !ingestion_options_.allow_db_generated_files) {
      return Status::InvalidArgument(
          "External file column family id don't match");
    }

    if (file_to_ingest.num_entries == 0 &&
        file_to_ingest.num_range_deletions == 0) {
      return Status::InvalidArgument("File contain no entries");
    }

    if (!file_to_ingest.smallest_internal_key.Valid() ||
        !file_to_ingest.largest_internal_key.Valid()) {
      return Status::Corruption("Generated table have corrupted keys");
    }

    if (ingestion_options_.reject_too_old_file) {
      int64_t now = 0;
      if (!clock_->GetCurrentTime(&now).ok()) {
        now = 0;
      }
      status = CheckExternalFileNotTooOld(
          file_path, file_to_ingest.table_properties.file_creation_time,
          file_to_ingest.table_properties, lossless_cast<uint64_t>(now),
          cfd_->ioptions().compaction_style, sv->mutable_cf_options,
          db_options_.info_log.get());
      if (!status.ok()) {
        return status;
      }
    }

    files_to_ingest_.emplace_back(std::move(file_to_ingest));
  }

  auto num_files = files_to_ingest_.size();
  const bool explicit_deletion_only =
      lsm_edit_spec_.has_value() &&
      lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kExplicitFiles &&
      !lsm_edit_spec_->delete_files.empty();
  const bool explicit_split_only =
      lsm_edit_spec_ && !lsm_edit_spec_->level_splits.empty() &&
      lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kExplicitFiles;
  if (num_files == 0 && !explicit_deletion_only && !explicit_split_only) {
    return Status::InvalidArgument("The list of files is empty");
  }
  // Detect whether the input files overlap one another; this drives how they
  // are divided into batches below.
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);

  if (lsm_edit_spec_.has_value()) {
    status = RecordDeclaredLevels(external_files_paths);
    if (!status.ok()) {
      return status;
    }
    assign_lsm_edit_global_seqnos_ =
        !lsm_edit_spec_->merge_incoming_files &&
        lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kFilesInRange &&
        cfd_->is_delete_range_supported() && !files_to_ingest_.empty() &&
        std::all_of(files_to_ingest_.begin(), files_to_ingest_.end(),
                    [](const IngestedFileInfo& file) {
                      return file.version == 2 && file.original_seqno == 0 &&
                             file.smallest_seqno == 0 &&
                             file.largest_seqno == 0;
                    });
    if (!assign_lsm_edit_global_seqnos_ &&
        !lsm_edit_spec_->merge_incoming_files) {
      status = ValidateDeclaredLevelSequenceOrder(sv);
      if (!status.ok()) {
        return status;
      }
    }
  }

  if (atomic_replace_range.has_value()) {
    atomic_replace_range_.emplace();

    if (atomic_replace_range->start && atomic_replace_range->limit) {
      if (ucmp_->CompareWithoutTimestamp(*atomic_replace_range->start,
                                         *atomic_replace_range->limit) >= 0) {
        return Status::InvalidArgument(
            "Atomic replace range limit must be greater than start");
      }
      // User keys to internal keys (with timestamps)
      const size_t ts_sz = ucmp_->timestamp_size();
      std::string start_with_ts;
      std::string limit_with_ts;
      auto [start, limit] = MaybeAddTimestampsToRange(
          atomic_replace_range->start, atomic_replace_range->limit, ts_sz,
          &start_with_ts, &limit_with_ts);
      assert(start.has_value());
      assert(limit.has_value());
      atomic_replace_range_->smallest_internal_key.Set(
          *start, kMaxSequenceNumber, kValueTypeForSeek);
      atomic_replace_range_->largest_internal_key =
          RangeTombstone(*start, *limit, kMaxSequenceNumber).SerializeEndKey();
      // Check files to ingest against replace range
      for (size_t i = 0; i < num_files; i++) {
        if (!file_range_checker_.Contains(*atomic_replace_range_,
                                          files_to_ingest_[i])) {
          return Status::InvalidArgument(
              lsm_edit_spec_.has_value()
                  ? "LSM edit range does not contain all files"
                  : "Atomic replace range does not contain all files");
        }
      }
    } else {
      // Currently if either bound is not present, both must be
      assert(atomic_replace_range->start.has_value() == false);
      assert(atomic_replace_range->limit.has_value() == false);
      assert(atomic_replace_range_->smallest_internal_key.unset());
      assert(atomic_replace_range_->largest_internal_key.unset());
    }
  }

  if (files_overlap_) {
    if (ingestion_options_.ingest_behind) {
      return Status::NotSupported(
          "Files with overlapping ranges cannot be ingested with ingestion "
          "behind mode.");
    }

    // Overlapping files need at least two different sequence numbers. If
    // settings disables global seqno, ingestion will fail anyway, so fail
    // fast in prepare.
    if (!ingestion_options_.allow_global_seqno &&
        !ingestion_options_.allow_db_generated_files) {
      return Status::InvalidArgument(
          "Global seqno is required, but disabled (because external files key "
          "range overlaps).");
    }

    if (ucmp_->timestamp_size() > 0) {
      return Status::NotSupported(
          "Files with overlapping ranges cannot be ingested to column "
          "family with user-defined timestamp enabled.");
    }
  }

  // Copy/Move external files into DB
  std::unordered_set<size_t> ingestion_path_ids;
  for (IngestedFileInfo& f : files_to_ingest_) {
    f.copy_file = false;
    const std::string path_outside_db = f.external_file_path;
    const std::string path_inside_db = TableFileName(
        cfd_->ioptions().cf_paths, f.fd.GetNumber(), f.fd.GetPathId());
    if (ingestion_options_.move_files || ingestion_options_.link_files) {
      status =
          fs_->LinkFile(path_outside_db, path_inside_db, IOOptions(), nullptr);
      if (status.ok()) {
        // It is unsafe to assume application had sync the file and file
        // directory before ingest the file. For integrity of RocksDB we need
        // to sync the file.
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncIngestedFile");
        Status s = fs_->SyncFile(path_inside_db, env_options_, IOOptions(),
                                 db_options_.use_fsync, nullptr);
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncIngestedFile");
        TEST_SYNC_POINT_CALLBACK(
            "ExternalSstFileIngestionJob::CheckSyncReturnCode", &s);
        // Some file systems (especially remote/distributed) don't support
        // explicitly syncing the file and don't require it. Ignore the
        // NotSupported error in that case.
        if (!s.IsNotSupported()) {
          status = s;
          if (!status.ok()) {
            ROCKS_LOG_WARN(db_options_.info_log,
                           "Failed to sync ingested file %s: %s",
                           path_inside_db.c_str(), status.ToString().c_str());
          }
        }
      } else if (status.IsNotSupported() &&
                 ingestion_options_.failed_move_fall_back_to_copy) {
        // Original file is on a different FS, use copy instead of hard linking.
        f.copy_file = true;
        ROCKS_LOG_INFO(db_options_.info_log,
                       "Tried to link file %s but it's not supported : %s",
                       path_outside_db.c_str(), status.ToString().c_str());
      } else {
        ROCKS_LOG_WARN(db_options_.info_log, "Failed to link file %s to %s: %s",
                       path_outside_db.c_str(), path_inside_db.c_str(),
                       status.ToString().c_str());
      }
    } else {
      f.copy_file = true;
    }

    if (f.copy_file) {
      TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Prepare:CopyFile",
                               nullptr);
      // Always determining the destination temperature from the ingested-to
      // level would be difficult because in general we only find out the level
      // ingested to later, during Run().
      // However, we can guarantee "last level" temperature for when the user
      // requires ingestion to the last level.
      Temperature dst_temp =
          (ingestion_options_.ingest_behind ||
           ingestion_options_.fail_if_not_bottommost_level)
              ? sv->mutable_cf_options.last_level_temperature
              : sv->mutable_cf_options.default_write_temperature;
      // Note: CopyFile also syncs the new file.
      status = CopyFile(fs_.get(), path_outside_db, f.file_temperature,
                        path_inside_db, dst_temp, 0, db_options_.use_fsync,
                        io_tracer_);
      // The destination of the copy will be ingested
      f.file_temperature = dst_temp;

      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log, "Failed to copy file %s to %s: %s",
                       path_outside_db.c_str(), path_inside_db.c_str(),
                       status.ToString().c_str());
      }
    } else {
      // Note: we currently assume that linking files does not cross
      // temperatures, so no need to change f.file_temperature
    }
    TEST_SYNC_POINT("ExternalSstFileIngestionJob::Prepare:FileAdded");
    if (!status.ok()) {
      break;
    }
    f.internal_file_path = path_inside_db;
    int64_t now = 0;
    if (clock_->GetCurrentTime(&now).ok()) {
      f.db_ownership_start_time = lossless_cast<uint64_t>(now);
    }
    // Initialize the checksum information of ingested files.
    f.file_checksum = kUnknownFileChecksum;
    f.file_checksum_func_name = kUnknownFileChecksumFuncName;
    ingestion_path_ids.insert(f.fd.GetPathId());
  }

  if (status.ok() && CanUseAtomicReplaceRangeTombstone(atomic_replace_range) &&
      (assign_lsm_edit_global_seqnos_ ||
       HasPartialOverlap(sv->current->storage_info()))) {
    assert(atomic_replace_range->start.has_value());
    assert(atomic_replace_range->limit.has_value());
    status = PrepareAtomicReplaceRangeTombstone(*atomic_replace_range->start,
                                                *atomic_replace_range->limit,
                                                next_file_number, sv);
    if (status.ok()) {
      assert(atomic_replace_range_tombstone_.has_value());
      ingestion_path_ids.insert(
          atomic_replace_range_tombstone_->fd.GetPathId());
    }
  }

  TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncDir");
  if (status.ok()) {
    for (auto path_id : ingestion_path_ids) {
      status = GetDataDir(path_id)->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to sync directory %" ROCKSDB_PRIszt
                       " while ingest file: %s",
                       path_id, status.ToString().c_str());
        break;
      }
    }
  }
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncDir");

  // Generate and check the SST file checksum.
  if (status.ok() && db_options_.file_checksum_gen_factory != nullptr) {
    bool need_generate_file_checksum;
    if (ingestion_options_.verify_file_checksum == false &&
        files_checksums.size() == files_to_ingest_.size() &&
        files_checksum_func_names.size() == files_to_ingest_.size()) {
      // Only when verify_file_checksum == false and the checksum for ingested
      // files are provided, DB will use the provided checksum and does not
      // generate the checksum for ingested files.
      need_generate_file_checksum = false;
    } else {
      need_generate_file_checksum = true;
    }
    std::vector<std::string> generated_checksums;
    std::vector<std::string> generated_checksum_func_names;
    // Step 1: generate the checksum for ingested sst file.
    if (need_generate_file_checksum) {
      for (size_t i = 0; i < files_to_ingest_.size(); i++) {
        std::string generated_checksum;
        std::string generated_checksum_func_name;
        std::string requested_checksum_func_name =
            i < files_checksum_func_names.size() ? files_checksum_func_names[i]
                                                 : "";
        // TODO: rate limit file reads for checksum calculation during file
        // ingestion.
        // TODO: plumb Env::IOActivity
        ReadOptions ro;
        // Pass user-provided checksums through FileOptions when available.
        // The caller may not have provided checksums at all (empty vectors),
        // so we guard with a bounds check.
        FileOptions fopts;
        if (i < files_checksums.size()) {
          fopts.file_checksum = files_checksums[i];
        }
        if (i < files_checksum_func_names.size()) {
          fopts.file_checksum_func_name = files_checksum_func_names[i];
        } else {
          fopts.file_checksum_func_name = kNoFileChecksumFuncName;
        }
        IOStatus io_s = GenerateOneFileChecksum(
            fs_.get(), files_to_ingest_[i].internal_file_path,
            db_options_.file_checksum_gen_factory.get(),
            requested_checksum_func_name, &generated_checksum,
            &generated_checksum_func_name,
            ingestion_options_.verify_checksums_readahead_size,
            db_options_.allow_mmap_reads, io_tracer_,
            db_options_.rate_limiter.get(), ro, db_options_.stats,
            db_options_.clock, fopts);
        if (!io_s.ok()) {
          status = io_s;
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Sst file checksum generation of file: %s failed: %s",
                         files_to_ingest_[i].internal_file_path.c_str(),
                         status.ToString().c_str());
          break;
        }
        files_to_ingest_[i].file_checksum = generated_checksum;
        files_to_ingest_[i].file_checksum_func_name =
            generated_checksum_func_name;
        generated_checksums.push_back(generated_checksum);
        generated_checksum_func_names.push_back(generated_checksum_func_name);
      }
    }

    // Step 2: based on the verify_file_checksum and ingested checksum
    // information, do the verification.
    if (status.ok()) {
      if (files_checksums.size() == files_to_ingest_.size() &&
          files_checksum_func_names.size() == files_to_ingest_.size()) {
        // Verify the checksum and checksum function name.
        if (ingestion_options_.verify_file_checksum) {
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            if (files_checksum_func_names[i] !=
                generated_checksum_func_names[i]) {
              status = Status::InvalidArgument(
                  "DB file checksum gen factory " +
                  std::string(db_options_.file_checksum_gen_factory->Name()) +
                  " generated checksum function name " +
                  generated_checksum_func_names[i] + " for file " +
                  external_files_paths[i] +
                  " which does not match requested/provided " +
                  files_checksum_func_names[i]);
              break;
            }
            if (files_checksums[i] != generated_checksums[i]) {
              status = Status::Corruption(
                  "Checksum verification mismatch for ingestion file " +
                  external_files_paths[i] + " using function " +
                  generated_checksum_func_names[i] + ". Expected: " +
                  Slice(files_checksums[i]).ToString(/*hex=*/true) +
                  " Computed: " +
                  Slice(generated_checksums[i]).ToString(/*hex=*/true));
              break;
            }
          }
        } else {
          // If verify_file_checksum is not enabled, we only verify the factory
          // recognizes the checksum function name. If it does not match, fail
          // the ingestion. If matches, we trust the ingested checksum
          // information and store in the Manifest.
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            FileChecksumGenContext gen_context;
            gen_context.file_name = files_to_ingest_[i].internal_file_path;
            gen_context.requested_checksum_func_name =
                files_checksum_func_names[i];
            auto file_checksum_gen =
                db_options_.file_checksum_gen_factory
                    ->CreateFileChecksumGenerator(gen_context);

            if (file_checksum_gen == nullptr ||
                files_checksum_func_names[i] != file_checksum_gen->Name()) {
              status = Status::InvalidArgument(
                  "Checksum function name " + files_checksum_func_names[i] +
                  " for file " + external_files_paths[i] +
                  " not recognized by DB checksum gen factory" +
                  db_options_.file_checksum_gen_factory->Name() +
                  (file_checksum_gen ? (" Returned function " +
                                        std::string(file_checksum_gen->Name()))
                                     : ""));
              break;
            }
            files_to_ingest_[i].file_checksum = files_checksums[i];
            files_to_ingest_[i].file_checksum_func_name =
                files_checksum_func_names[i];
          }
        }
      } else if (files_checksums.size() != files_checksum_func_names.size() ||
                 files_checksums.size() != 0) {
        // The checksum or checksum function name vector are not both empty
        // and they are incomplete.
        status = Status::InvalidArgument(
            "The checksum information of ingested sst files are nonempty and "
            "the size of checksums or the size of the checksum function "
            "names does not match with the number of ingested sst files");
      }
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log, "Ingestion failed: %s",
                       status.ToString().c_str());
      }
    }
  }

  if (status.ok() && lsm_edit_spec_ && lsm_edit_spec_->merge_incoming_files) {
    status = MergeIncomingFiles(sv, next_file_number);
    if (status.ok()) {
      status = ValidateDeclaredLevelSequenceOrder(sv);
    }
    next_file_number += num_files;
  }
  if (status.ok() && lsm_edit_spec_ && lsm_edit_spec_->split_existing_files) {
    status = SplitExistingFiles(sv, next_file_number);
    if (status.ok()) {
      status = ValidateDeclaredLevelSequenceOrder(sv);
    }
  }
  if (status.ok() && lsm_edit_spec_ && !lsm_edit_spec_->level_splits.empty()) {
    status = SplitDeclaredLevels(sv, next_file_number);
    if (status.ok() && NeedsSurvivingSequenceValidation()) {
      status = ValidateDeclaredLevelSequenceOrder(sv);
    }
  }
  if (status.ok() && lsm_edit_spec_ && NeedsSurvivingSequenceValidation()) {
    status = ValidateDeclaredLevelSurvivingFileSequenceOrder(sv);
  }

  if (status.ok()) {
    DivideInputFilesIntoBatches();
  }

  return status;
}

Status ExternalSstFileIngestionJob::BuildMergedFile(
    const std::vector<IngestedFileInfo*>& inputs, int level,
    uint64_t file_number, SuperVersion* sv, IngestedFileInfo* output,
    const RangeOpt* output_bounds) {
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  ro.total_order_seek = true;
  ro.io_activity = Env::IOActivity::kCompaction;
  if (output_bounds) {
    ro.iterate_lower_bound =
        output_bounds->start ? &*output_bounds->start : nullptr;
    ro.iterate_upper_bound =
        output_bounds->limit ? &*output_bounds->limit : nullptr;
  }
  InternalKey lower_key;
  InternalKey upper_key;
  InternalKey range_upper_key;
  if (output_bounds && output_bounds->start) {
    lower_key.Set(*output_bounds->start, kMaxSequenceNumber, kValueTypeForSeek);
  }
  if (output_bounds && output_bounds->limit) {
    upper_key.Set(*output_bounds->limit, kMaxSequenceNumber, kValueTypeForSeek);
    range_upper_key.Set(*output_bounds->limit, kMaxSequenceNumber,
                        kTypeRangeDeletion);
  }
  const Slice lower = lower_key.unset() ? Slice() : lower_key.Encode();
  const Slice upper = upper_key.unset() ? Slice() : upper_key.Encode();
  std::vector<std::unique_ptr<TableReader>> readers;
  std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>> range_iters;
  SequenceNumber job_snapshot = versions_->LastSequence();
  int64_t current_time = 0;
  Status time_status = clock_->GetCurrentTime(&current_time);
  if (!time_status.ok()) {
    return time_status;
  }
  const uint64_t file_creation_time =
      lossless_cast<uint64_t>(std::max(int64_t{0}, current_time));
  uint64_t newest_key_time = 0;
  uint64_t oldest_key_time = file_creation_time;
  uint64_t oldest_ancestor_time = file_creation_time;
  for (auto* input : inputs) {
    if (input->zero_sequence_numbers && input->num_range_deletions > 0) {
      return Status::InvalidArgument(
          "LSM edit zero_sequence_numbers does not support range tombstones");
    }
    readers.emplace_back();
    Status s = ResetTableReader(
        input->internal_file_path, input->fd.GetNumber(),
        input->user_defined_timestamps_persisted, sv, input, &readers.back());
    if (!s.ok()) {
      return s;
    }
    auto* reader = readers.back().get();
    const std::shared_ptr<const TableProperties> properties =
        reader->GetTableProperties();
    const TableProperties& props =
        properties != nullptr ? *properties : input->table_properties;
    newest_key_time = std::max(newest_key_time, props.newest_key_time != 0
                                                    ? props.newest_key_time
                                                    : file_creation_time);
    oldest_key_time = std::min(oldest_key_time, props.oldest_key_time != 0
                                                    ? props.oldest_key_time
                                                    : file_creation_time);
    const uint64_t input_ancestor_time = input->oldest_ancestor_time != 0
                                             ? input->oldest_ancestor_time
                                             : props.oldest_ancestor_time;
    oldest_ancestor_time = std::min(
        oldest_ancestor_time,
        input_ancestor_time != 0 ? input_ancestor_time : file_creation_time);
    reader->SetupForCompaction();
    std::unique_ptr<FragmentedRangeTombstoneIterator> ranges(
        reader->NewRangeTombstoneIterator(ro));
    if (ranges) {
      if (output_bounds || input->existing_file) {
        assert(merge_snapshot_context_);
        CompactionRangeDelAggregator clipped_ranges(
            &cfd_->internal_comparator(),
            merge_snapshot_context_->snapshot_seqs);
        const InternalKey* lower_bound =
            lower_key.unset() ? nullptr : &lower_key;
        const InternalKey* upper_bound =
            range_upper_key.unset() ? nullptr : &range_upper_key;
        const auto& icmp = cfd_->internal_comparator();
        if (input->existing_file) {
          if (!lower_bound ||
              icmp.Compare(input->smallest_internal_key, *lower_bound) > 0) {
            lower_bound = &input->smallest_internal_key;
          }
          if (!upper_bound ||
              icmp.Compare(input->largest_internal_key, *upper_bound) < 0) {
            upper_bound = &input->largest_internal_key;
          }
        }
        clipped_ranges.AddTombstones(std::move(ranges), lower_bound,
                                     upper_bound);
        ranges = clipped_ranges.NewIterator();
      }
      range_iters.emplace_back(std::move(ranges));
    }
    if (!input->zero_sequence_numbers) {
      job_snapshot = std::max(job_snapshot, input->largest_seqno);
      max_merged_input_seqno_ =
          std::max(max_merged_input_seqno_, input->largest_seqno);
    }
  }
  LsmEditMergeIterator iter(cfd_->internal_comparator(), ro,
                            sv->mutable_cf_options.prefix_extractor.get(),
                            inputs, readers,
                            mutable_db_options_.compaction_readahead_size,
                            lower_key.unset() ? nullptr : &lower,
                            upper_key.unset() ? nullptr : &upper);
  ImmutableOptions ioptions = cfd_->ioptions();
  ioptions.compaction_filter = nullptr;
  ioptions.compaction_filter_factory.reset();
  MutableCFOptions moptions = sv->mutable_cf_options;
  moptions.enable_blob_files = false;
  const auto* storage = sv->current->storage_info();
  // Declared levels may be above the current dynamic base level.
  const int base_level =
      std::min(std::max(1, level), std::max(1, storage->base_level()));
  const auto compression =
      GetCompressionType(storage, moptions, level, base_level);
  const auto compression_options =
      GetCompressionOptions(moptions, storage, level);
  WriteOptions wo;
  wo.io_activity = Env::IOActivity::kCompaction;
  TableBuilderOptions tboptions(
      ioptions, moptions, ro, wo, cfd_->internal_comparator(),
      cfd_->internal_tbl_prop_coll_factories(), compression,
      compression_options, cfd_->GetID(), cfd_->GetName(), level,
      newest_key_time, /*is_bottommost=*/false,
      TableFileCreationReason::kCompaction, oldest_key_time, versions_->DbId(),
      versions_->DbSessionId(), /*target_file_size=*/0, file_number);
  tboptions.require_strict_key_order = true;
  FileMetaData meta;
  meta.fd = FileDescriptor(file_number, 0, 0);
  meta.oldest_ancestor_time = oldest_ancestor_time;
  meta.file_creation_time = file_creation_time;
  FileOptions file_options{env_options_};
  file_options.temperature = level == cfd_->NumberLevels() - 1
                                 ? moptions.last_level_temperature
                                 : moptions.default_write_temperature;
  IOStatus io_status;
  const JobContext* snapshot_context = merge_snapshot_context_.get();
  std::vector<SequenceNumber> snapshots;
  if (snapshot_context) {
    snapshots = snapshot_context->snapshot_seqs;
    job_snapshot = snapshot_context->GetJobSnapshotSequence();
  }
  Status s = BuildTable(
      versions_->DbName(), versions_, db_options_, tboptions, file_options,
      cfd_->table_cache(), &iter, std::move(range_iters), &meta,
      /*blob_file_additions=*/nullptr, std::move(snapshots),
      snapshot_context ? snapshot_context->GetEarliestSnapshotSequence()
                       : kMaxSequenceNumber,
      snapshot_context ? snapshot_context->earliest_write_conflict_snapshot
                       : kMaxSequenceNumber,
      job_snapshot,
      snapshot_context ? snapshot_context->snapshot_checker : nullptr,
      moptions.paranoid_file_checks, cfd_->internal_stats(), &io_status,
      io_tracer_, BlobFileCreationReason::kCompaction,
      /*seqno_to_time_mapping=*/nullptr, event_logger_);
  if (!io_status.ok() && s.ok()) {
    s = io_status;
  }
  TableCache::Evict(cfd_->table_cache()->get_cache().get(), file_number);
  if (!s.ok() || meta.fd.GetFileSize() == 0) {
    return s;
  }
  const std::string path = TableFileName(ioptions.cf_paths, file_number, 0);
  output->internal_file_path = path;
  output->generated_for_ingestion = true;
  output->file_size = meta.fd.GetFileSize();
  s = RegisterGeneratedFile(output);
  if (!s.ok()) {
    return s;
  }
  output->file_temperature = file_options.temperature;
  output->prefetch_lmax_index_and_filter_blocks =
      ingestion_options_.prefetch_lmax_index_and_filter_blocks;
  TEST_SYNC_POINT_CALLBACK(
      "ExternalSstFileIngestionJob::BuildMergedFile:BeforeOutputFileInfo", &s);
  if (s.ok()) {
    s = GetIngestedFileInfo(path, file_number, nullptr, output, sv);
  }
  output->declared_level = level;
  output->file_checksum = meta.file_checksum;
  output->file_checksum_func_name = meta.file_checksum_func_name;
  RecordTick(db_options_.stats, COMPACT_WRITE_BYTES, meta.fd.GetFileSize());
  return s;
}

FSDirectory* ExternalSstFileIngestionJob::GetDataDir(size_t path_id) const {
  FSDirectory* directory = cfd_->GetDataDir(path_id);
  return directory != nullptr ? directory : directories_->GetDataDir(path_id);
}

Status ExternalSstFileIngestionJob::MergeIncomingFiles(
    SuperVersion* sv, uint64_t next_file_number) {
  merged_input_files_ = std::move(files_to_ingest_);
  std::map<int, std::vector<IngestedFileInfo*>> by_level;
  for (auto& input : merged_input_files_) {
    by_level[input.declared_level].push_back(&input);
  }
  for (auto& [level, inputs] : by_level) {
    if (lsm_edit_spec_->merge_existing_files) {
      KeyRangeInfo range;
      for (const auto* input : inputs) {
        file_range_checker_.MaybeUpdateRange(
            input->smallest_internal_key, input->largest_internal_key, &range);
      }
      const size_t begin = merged_existing_files_.size();
      SelectExistingMergeInputs(level, range, sv);
      TEST_SYNC_POINT("ExternalSstFileIngestionJob::MergeExisting:Selected");
      std::vector<IngestedFileInfo*> combined;
      for (size_t i = begin; i < merged_existing_files_.size(); ++i) {
        combined.push_back(&merged_existing_files_[i]);
      }
      combined.insert(combined.end(), inputs.begin(), inputs.end());
      inputs = std::move(combined);
    }
    files_to_ingest_.emplace_back();
    Status s = BuildMergedFile(inputs, level, next_file_number++, sv,
                               &files_to_ingest_.back());
    if (!s.ok()) {
      return s;
    }
    if (files_to_ingest_.back().internal_file_path.empty()) {
      files_to_ingest_.pop_back();
    }
    for (const auto* input : inputs) {
      RecordTick(db_options_.stats, COMPACT_READ_BYTES, input->file_size);
    }
  }
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);
  return GetDataDir(0)->FsyncWithDirOptions(
      IOOptions(), nullptr,
      DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
}

Status ExternalSstFileIngestionJob::CheckSplitRangeEmpty(IngestedFileInfo* file,
                                                         SuperVersion* sv) {
  assert(atomic_replace_range_ && !atomic_replace_range_->unset());
  const Slice start = atomic_replace_range_->smallest_internal_key.user_key();
  const Slice limit = atomic_replace_range_->largest_internal_key.user_key();
  std::unique_ptr<TableReader> reader;
  Status status = ResetTableReader(
      file->internal_file_path, file->fd.GetNumber(),
      file->user_defined_timestamps_persisted, sv, file, &reader);
  if (!status.ok()) {
    return status;
  }
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  ro.total_order_seek = true;
  std::unique_ptr<InternalIterator> points(reader->NewIterator(
      ro, sv->mutable_cf_options.prefix_extractor.get(), nullptr, false,
      TableReaderCaller::kExternalSSTIngestion));
  const InternalKey seek(start, kMaxSequenceNumber, kValueTypeForSeek);
  points->Seek(seek.Encode());
  status = points->status();
  if (!status.ok()) {
    return status;
  }
  if (points->Valid()) {
    ParsedInternalKey parsed;
    status = ParseInternalKey(points->key(), &parsed,
                              db_options_.allow_data_in_errors);
    if (!status.ok()) {
      return status;
    }
    if (ucmp_->Compare(parsed.user_key, limit) < 0) {
      return Status::InvalidArgument("LSM edit split range is not empty: " +
                                     file->internal_file_path);
    }
  }
  std::unique_ptr<FragmentedRangeTombstoneIterator> ranges(
      reader->NewRangeTombstoneIterator(ro));
  if (ranges) {
    for (ranges->SeekToFirst(); ranges->Valid(); ranges->Next()) {
      if (ucmp_->Compare(ranges->start_key(), limit) < 0 &&
          ucmp_->Compare(ranges->end_key(), start) > 0) {
        return Status::InvalidArgument(
            "LSM edit split range is covered by a range tombstone: " +
            file->internal_file_path);
      }
    }
    return ranges->status();
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::ValidateLevelSplits() const {
  if (!lsm_edit_spec_) {
    return Status::OK();
  }
  std::unordered_set<int> levels;
  for (const auto& split : lsm_edit_spec_->level_splits) {
    if (split.level <= 0 || split.level >= cfd_->NumberLevels() ||
        cfd_->ioptions().compaction_style == kCompactionStyleFIFO) {
      return Status::InvalidArgument(
          "LSM edit level split requires a valid nonzero level in a non-FIFO "
          "CF");
    }
    if (!levels.insert(split.level).second) {
      return Status::InvalidArgument("LSM edit repeats a split level");
    }
    if (split.cut_points.empty()) {
      return Status::InvalidArgument("LSM edit level split has no cut points");
    }
    for (size_t i = 1; i < split.cut_points.size(); ++i) {
      if (ucmp_->Compare(split.cut_points[i - 1], split.cut_points[i]) >= 0) {
        return Status::InvalidArgument(
            "LSM edit split cuts must be strictly increasing in comparator "
            "order");
      }
    }
  }
  return Status::OK();
}

std::vector<Slice> ExternalSstFileIngestionJob::FileSplitPoints(
    const FileMetaData& file, const LsmEditLevelSplit& split) const {
  std::vector<Slice> cuts;
  auto cut =
      std::upper_bound(split.cut_points.begin(), split.cut_points.end(),
                       file.smallest.user_key(),
                       [this](const Slice& key, const std::string& boundary) {
                         return ucmp_->Compare(key, boundary) < 0;
                       });
  for (; cut != split.cut_points.end(); ++cut) {
    const InternalKey boundary(*cut, kMaxSequenceNumber, kValueTypeForSeek);
    if (sstableKeyCompare(ucmp_, file.largest, boundary) < 0) {
      break;
    }
    cuts.emplace_back(*cut);
  }
  return cuts;
}

Status ExternalSstFileIngestionJob::SplitDeclaredLevels(
    SuperVersion* sv, uint64_t next_file_number) {
  struct SplitInput {
    IngestedFileInfo file;
    std::vector<Slice> cuts;
  };
  std::vector<SplitInput> selected;
  for (const auto& split : lsm_edit_spec_->level_splits) {
    for (const auto* file :
         sv->current->storage_info()->LevelFiles(split.level)) {
      if (std::any_of(lsm_edit_spec_->delete_files.begin(),
                      lsm_edit_spec_->delete_files.end(),
                      [&](const auto& deletion) {
                        return deletion.level == split.level &&
                               deletion.file_number == file->fd.GetNumber();
                      })) {
        continue;
      }
      auto cuts = FileSplitPoints(*file, split);
      if (cuts.empty()) {
        continue;
      }
      auto info = LiveFileInfo(*file, split.level);
      info.internal_file_path =
          TableFileName(cfd_->ioptions().cf_paths, file->fd.GetNumber(),
                        file->fd.GetPathId());
      info.external_file_path = info.internal_file_path;
      info.file_temperature = file->temperature;
      selected.push_back({std::move(info), std::move(cuts)});
    }
  }
  if (selected.empty()) {
    return Status::OK();
  }
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::SplitLevels:Selected");
  for (auto& input : selected) {
    rewritten_existing_deletions_.push_back(
        {input.file.declared_level, input.file.fd.GetNumber()});
    OptSlice start;
    for (size_t i = 0; i <= input.cuts.size(); ++i) {
      RangeOpt bounds;
      bounds.start = start;
      if (i < input.cuts.size()) {
        bounds.limit = input.cuts[i];
      }
      files_to_ingest_.emplace_back();
      Status status = BuildMergedFile({&input.file}, input.file.declared_level,
                                      next_file_number++, sv,
                                      &files_to_ingest_.back(), &bounds);
      if (!status.ok()) {
        return status;
      }
      if (files_to_ingest_.back().internal_file_path.empty()) {
        files_to_ingest_.pop_back();
      }
      start = bounds.limit;
    }
    RecordTick(db_options_.stats, COMPACT_READ_BYTES, input.file.file_size);
  }
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);
  return GetDataDir(0)->FsyncWithDirOptions(
      IOOptions(), nullptr,
      DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
}

Status ExternalSstFileIngestionJob::ValidateLevelSplitSurvivors(
    const VersionStorageInfo* storage) const {
  if (!lsm_edit_spec_) {
    return Status::OK();
  }
  for (const auto& split : lsm_edit_spec_->level_splits) {
    if (cfd_->RangeOverlapWithCompaction(split.cut_points.front(),
                                         split.cut_points.back(), split.level,
                                         /*range_limit_exclusive=*/false)) {
      return Status::TryAgain(
          "LSM edit level split overlaps a pending compaction");
    }
    for (const auto* file : storage->LevelFiles(split.level)) {
      if (edit_.GetDeletedFiles().count({split.level, file->fd.GetNumber()}) !=
          0) {
        continue;
      }
      if (!FileSplitPoints(*file, split).empty()) {
        return Status::TryAgain(
            "LSM edit level split acquired a new straddling file");
      }
    }
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::SplitExistingFiles(
    SuperVersion* sv, uint64_t next_file_number) {
  assert(lsm_edit_spec_->split_existing_files);
  assert(atomic_replace_range_ && !atomic_replace_range_->unset());
  std::map<int, std::vector<const IngestedFileInfo*>> incoming;
  for (const auto& file : files_to_ingest_) {
    if (file.declared_level > 0) {
      incoming[file.declared_level].push_back(&file);
    }
  }
  std::vector<IngestedFileInfo> selected;
  std::unordered_set<uint64_t> selected_numbers;
  for (const auto& [level, files] : incoming) {
    const auto& live_files = sv->current->storage_info()->LevelFiles(level);
    for (const auto* incoming_file : files) {
      auto live = std::lower_bound(
          live_files.begin(), live_files.end(),
          incoming_file->smallest_internal_key,
          [this](const FileMetaData* file, const InternalKey& key) {
            return sstableKeyCompare(ucmp_, file->largest, key) < 0;
          });
      for (; live != live_files.end() &&
             sstableKeyCompare(ucmp_, (*live)->smallest,
                               incoming_file->largest_internal_key) <= 0;
           ++live) {
        if (!selected_numbers.insert((*live)->fd.GetNumber()).second) {
          continue;
        }
        auto info = LiveFileInfo(**live, level);
        info.internal_file_path =
            TableFileName(cfd_->ioptions().cf_paths, (*live)->fd.GetNumber(),
                          (*live)->fd.GetPathId());
        info.external_file_path = info.internal_file_path;
        info.file_temperature = (*live)->temperature;
        Status status = CheckSplitRangeEmpty(&info, sv);
        if (!status.ok()) {
          return status;
        }
        selected.push_back(std::move(info));
      }
    }
  }
  if (selected.empty()) {
    return Status::OK();
  }
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::SplitExisting:Selected");
  RangeOpt left;
  left.limit = atomic_replace_range_->smallest_internal_key.user_key();
  RangeOpt right;
  right.start = atomic_replace_range_->largest_internal_key.user_key();
  for (auto& input : selected) {
    rewritten_existing_deletions_.push_back(
        {input.declared_level, input.fd.GetNumber()});
    for (const auto* bounds : {&left, &right}) {
      files_to_ingest_.emplace_back();
      Status status =
          BuildMergedFile({&input}, input.declared_level, next_file_number++,
                          sv, &files_to_ingest_.back(), bounds);
      if (!status.ok()) {
        return status;
      }
      if (files_to_ingest_.back().internal_file_path.empty()) {
        files_to_ingest_.pop_back();
      }
    }
    RecordTick(db_options_.stats, COMPACT_READ_BYTES, input.file_size);
  }
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);
  return GetDataDir(0)->FsyncWithDirOptions(
      IOOptions(), nullptr,
      DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
}

void ExternalSstFileIngestionJob::SelectExistingMergeInputs(
    int level, const KeyRangeInfo& incoming_range, SuperVersion* sv) {
  KeyRangeInfo range = incoming_range;
  std::unordered_set<uint64_t> selected;
  std::vector<const FileMetaData*> files;
  const auto& live = sv->current->storage_info()->LevelFiles(level);
  bool changed;
  do {
    changed = false;
    for (const auto* file : live) {
      if (selected.count(file->fd.GetNumber()) != 0 ||
          !file_range_checker_.Overlaps(range, file->smallest, file->largest)) {
        continue;
      }
      selected.insert(file->fd.GetNumber());
      files.push_back(file);
      file_range_checker_.MaybeUpdateRange(file->smallest, file->largest,
                                           &range);
      changed = true;
    }
  } while (changed);
  if (level == 0) {
    files.clear();
    for (auto it = live.rbegin(); it != live.rend(); ++it) {
      if (selected.count((*it)->fd.GetNumber()) != 0) {
        files.push_back(*it);
      }
    }
  }
  for (const auto* file : files) {
    auto info = LiveFileInfo(*file, level);
    info.internal_file_path = TableFileName(
        cfd_->ioptions().cf_paths, file->fd.GetNumber(), file->fd.GetPathId());
    info.external_file_path = info.internal_file_path;
    info.file_size = file->fd.GetFileSize();
    info.file_temperature = file->temperature;
    merged_existing_files_.push_back(std::move(info));
    rewritten_existing_deletions_.push_back({level, file->fd.GetNumber()});
  }
}

Status ExternalSstFileIngestionJob::AddRewrittenExistingFileDeletions(
    const VersionStorageInfo* storage) {
  std::vector<std::pair<int, const FileMetaData*>> files;
  for (const auto& selected : rewritten_existing_deletions_) {
    const auto& live = storage->LevelFiles(selected.level);
    auto found = std::find_if(live.begin(), live.end(), [&](const auto* file) {
      return file->fd.GetNumber() == selected.file_number;
    });
    if (found == live.end() || (*found)->being_compacted) {
      return Status::TryAgain("LSM edit destination rewrite inputs changed");
    }
    files.emplace_back(selected.level, *found);
  }
  for (const auto& [level, file] : files) {
    Status status = DeleteLiveFile(level, file);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

void ExternalSstFileIngestionJob::DivideInputFilesIntoBatches() {
  if (!files_overlap_) {
    // No overlap, treat as one batch without the need of tracking overall batch
    // range.
    file_batches_to_ingest_.emplace_back(/* _track_batch_range= */ false);
    for (auto& file : files_to_ingest_) {
      file_batches_to_ingest_.back().AddFile(&file, file_range_checker_);
    }
    return;
  }

  file_batches_to_ingest_.emplace_back(/* _track_batch_range= */ true);
  for (auto& file : files_to_ingest_) {
    if (!file_batches_to_ingest_.back().unset() &&
        file_range_checker_.Overlaps(file_batches_to_ingest_.back(), file,
                                     /* known_sorted= */ false)) {
      file_batches_to_ingest_.emplace_back(/* _track_batch_range= */ true);
    }
    file_batches_to_ingest_.back().AddFile(&file, file_range_checker_);
  }
}

bool ExternalSstFileIngestionJob::ComputeFilesOverlap(
    const autovector<IngestedFileInfo>& files) const {
  const size_t num_files = files.size();
  if (num_files <= 1) {
    return false;
  }
  // Verify whether the files have overlapping ranges by sorting copies of the
  // file ranges and checking adjacent pairs.
  autovector<const IngestedFileInfo*> sorted_files;
  for (size_t i = 0; i < num_files; i++) {
    sorted_files.push_back(&files[i]);
  }
  std::sort(sorted_files.begin(), sorted_files.end(), file_range_checker_);
  for (size_t i = 0; i + 1 < num_files; i++) {
    if (file_range_checker_.Overlaps(*sorted_files[i], *sorted_files[i + 1],
                                     /* known_sorted= */ true)) {
      return true;
    }
  }
  return false;
}

Status ExternalSstFileIngestionJob::MergeForSameColumnFamily(
    ExternalSstFileIngestionJob* other) {
  assert(other != nullptr);
  assert(other != this);
  assert(cfd_ == other->cfd_);
  if (atomic_replace_range_.has_value() ||
      other->atomic_replace_range_.has_value()) {
    return Status::NotSupported(
        "cannot merge file ingestion handles for the same column family when "
        "atomic_replace_range is used");
  }
  if (!(ingestion_options_ == other->ingestion_options_)) {
    return Status::InvalidArgument(
        "file ingestion handles for the same column family must be prepared "
        "with the same IngestExternalFileOptions");
  }
  // Append the other job's prepared files after this job's so that, for any
  // overlapping keys, the other job's data wins via a higher assigned sequence
  // number -- the same semantics as passing all the files to a single ingestion
  // call in this order. Recompute overlap and rebuild the batches over the
  // union.
  for (IngestedFileInfo& file : other->files_to_ingest_) {
    files_to_ingest_.push_back(std::move(file));
  }
  other->files_to_ingest_.clear();
  other->file_batches_to_ingest_.clear();
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);
  file_batches_to_ingest_.clear();
  DivideInputFilesIntoBatches();
  return Status::OK();
}

Status ExternalSstFileIngestionJob::NeedsFlush(bool* flush_needed,
                                               SuperVersion* super_version) {
  if (lsm_edit_spec_ && !lsm_edit_spec_->level_splits.empty() &&
      lsm_edit_spec_->levels.empty() && lsm_edit_spec_->delete_files.empty()) {
    // Repartitioning existing data introduces no entries that a memtable
    // could shadow; the normal compaction snapshot context protects its
    // history.
    *flush_needed = false;
    return Status::OK();
  }
  Status status;
  if (atomic_replace_range_.has_value() && atomic_replace_range_->unset()) {
    // For replacing whole CF, we can simply check whether memtable is empty
    *flush_needed = !super_version->mem->IsEmpty();
  } else {
    autovector<UserKeyRange> ranges;
    if (atomic_replace_range_.has_value()) {
      assert(!atomic_replace_range_->smallest_internal_key.unset());
      assert(!atomic_replace_range_->largest_internal_key.unset());
      // NOTE: we already checked in Prepare() that the atomic_replace_range
      // covers all the files_to_ingest.
      ranges.emplace_back(
          atomic_replace_range_->smallest_internal_key.user_key(),
          atomic_replace_range_->largest_internal_key.user_key());
    } else {
      ranges.reserve(files_to_ingest_.size());
      for (const IngestedFileInfo& file_to_ingest : files_to_ingest_) {
        ranges.emplace_back(file_to_ingest.start_ukey,
                            file_to_ingest.limit_ukey);
      }
    }
    status = cfd_->RangesOverlapWithMemtables(
        ranges, super_version, db_options_.allow_data_in_errors, flush_needed,
        atomic_replace_range_.has_value() && !atomic_replace_range_->unset());
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to check ranges overlap with memtables: %s",
                     status.ToString().c_str());
    }
  }
  if (status.ok() && *flush_needed) {
    if (!ingestion_options_.allow_blocking_flush) {
      status = Status::InvalidArgument("External file requires flush");
    }
    if (ucmp_->timestamp_size() > 0) {
      status = Status::InvalidArgument(
          "Column family enables user-defined timestamps, please make "
          "sure the key range (without timestamp) of external file does not "
          "overlap with key range in the memtables.");
    }
  }
  return status;
}

// REQUIRES: we have become the only writer by entering both write_thread_ and
// nonmem_write_thread_
Status ExternalSstFileIngestionJob::Run() {
  const bool has_registered_output =
      std::any_of(files_to_ingest_.begin(), files_to_ingest_.end(),
                  [](const IngestedFileInfo& file) {
                    return file.registered_with_sst_file_manager;
                  }) ||
      (atomic_replace_range_tombstone_ &&
       atomic_replace_range_tombstone_->registered_with_sst_file_manager);
  if (has_registered_output &&
      db_options_.sst_file_manager->IsMaxAllowedSpaceReached()) {
    return Status::SpaceLimit(
        "Max allowed space was reached by generated ingestion SST");
  }
  max_assigned_seqno_ = std::max(max_assigned_seqno_, max_merged_input_seqno_);
  SuperVersion* super_version = cfd_->GetSuperVersion();
  // If column family is flushed after Prepare and before Run, we should have a
  // specific state of Memtables. The mutable Memtable should be empty, and the
  // immutable Memtable list should be empty.
  if (flushed_before_run_ && (super_version->imm->NumNotFlushed() != 0 ||
                              !super_version->mem->IsEmpty())) {
    return Status::TryAgain(
        "Inconsistent memtable state detected when flushed before run.");
  }
  Status status;
#ifndef NDEBUG
  // We should never run the job with a memtable that is overlapping
  // with the files we are ingesting
  bool need_flush = false;
  status = NeedsFlush(&need_flush, super_version);
  if (!status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to check if flush is needed: %s",
                   status.ToString().c_str());
    return status;
  }
  if (need_flush) {
    return Status::TryAgain("need_flush");
  }
  assert(status.ok() && need_flush == false);
#endif

  bool force_global_seqno = false;

  const bool pure_level_split =
      lsm_edit_spec_ && !lsm_edit_spec_->level_splits.empty() &&
      lsm_edit_spec_->levels.empty() && lsm_edit_spec_->delete_files.empty();
  if (lsm_edit_spec_.has_value() && lsm_edit_spec_->snapshot_consistency &&
      !pure_level_split &&
      db_snapshots_->AnySnapshotWhileNewBlocked(
          merge_snapshot_context_ && merge_snapshot_context_->job_snapshot
              ? merge_snapshot_context_->job_snapshot->snapshot()
              : nullptr,
          SnapshotCheck::kUser)) {
    return Status::InvalidArgument(
        "LSM edit is not supported while snapshots are active");
  }

  if (ingestion_options_.snapshot_consistency) {
    // We need to assign a global sequence number to all the files even
    // if the don't overlap with any ranges since we have snapshots
    force_global_seqno =
        db_snapshots_->AnySnapshotWhileNewBlocked(nullptr, SnapshotCheck::kAll);
  }
  // It is safe to use this instead of LastAllocatedSequence since we are
  // the only active writer, and hence they are equal
  SequenceNumber last_seqno = versions_->LastSequence();
  edit_.SetColumnFamily(cfd_->GetID());

  if (atomic_replace_range_.has_value()) {
    // A grouped MANIFEST write can advance current before publishing its SV.
    files_to_replace_version_ = cfd_->current();
    files_to_replace_version_->Ref();
    auto* vstorage = files_to_replace_version_->storage_info();
    const bool explicit_deletions =
        lsm_edit_spec_.has_value() &&
        lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kExplicitFiles;
    if (atomic_replace_range_->unset()) {
      if (cfd_->compaction_picker()->IsCompactionInProgress() &&
          !pure_level_split) {
        if (lsm_edit_spec_ && !lsm_edit_spec_->level_splits.empty()) {
          return Status::TryAgain(
              "LSM edit level split overlaps a pending compaction");
        }
        return Status::InvalidArgument(
            "Atomic replace range (full) overlaps with pending compaction");
      }
      if (explicit_deletions) {
        status = AddExplicitFileDeletions(vstorage);
        if (!status.ok()) {
          return status;
        }
      } else {
        for (int lvl = 0; lvl < cfd_->NumberLevels(); lvl++) {
          for (auto file : vstorage->LevelFiles(lvl)) {
            status = AddFileToReplace(lvl, file);
            if (!status.ok()) {
              return status;
            }
          }
        }
      }
    } else {
      assert(!atomic_replace_range_->smallest_internal_key.unset());
      assert(!atomic_replace_range_->largest_internal_key.unset());
      bool has_partial_overlap = false;
      autovector<std::pair<int, const FileMetaData*>> straddling_files;
      for (int lvl = 0; lvl < cfd_->NumberLevels(); lvl++) {
        if (cfd_->RangeOverlapWithCompaction(
                atomic_replace_range_->smallest_internal_key.user_key(),
                atomic_replace_range_->largest_internal_key.user_key(), lvl,
                /*range_limit_exclusive=*/true)) {
          if (lsm_edit_spec_ && lsm_edit_spec_->split_existing_files) {
            return Status::TryAgain(
                "LSM edit split overlaps a pending compaction");
          }
          return Status::InvalidArgument(
              "Atomic replace range overlaps with pending compaction");
        }
        if (!explicit_deletions) {
          for (auto file : vstorage->LevelFiles(lvl)) {
            if (file_range_checker_.Overlaps(*atomic_replace_range_,
                                             file->smallest, file->largest)) {
              if (file_range_checker_.Contains(*atomic_replace_range_,
                                               file->smallest, file->largest)) {
                status = AddFileToReplace(lvl, file);
                if (!status.ok()) {
                  return status;
                }
              } else {
                has_partial_overlap = true;
                straddling_files.emplace_back(lvl, file);
              }
            }
          }
        }
      }
      if (explicit_deletions) {
        status = AddExplicitFileDeletions(vstorage);
        if (!status.ok()) {
          return status;
        }
      }
      if (has_partial_overlap && lsm_edit_spec_.has_value() &&
          !assign_lsm_edit_global_seqnos_) {
        // A straddling file keeps the part of itself that lies outside the
        // edit, so it cannot be deleted. Retaining it is only correct if it
        // holds nothing inside the edit's range.
        if (!lsm_edit_spec_->probe_straddling_files) {
          return Status::InvalidArgument(
              "LSM edit range partially overlaps an existing file and "
              "probe_straddling_files is disabled");
        }
        status = ProbeStraddlingFiles(super_version, straddling_files);
        if (!status.ok()) {
          return status;
        }
        has_partial_overlap = false;
      }
      if (has_partial_overlap) {
        if (ingestion_options_.fail_if_not_bottommost_level &&
            SupportsAtomicReplaceRangeTombstone()) {
          return Status::TryAgain(
              "Atomic replace range partially overlaps with an existing file, "
              "so replacement files cannot all be ingested to Lmax");
        }
        if (!atomic_replace_range_tombstone_.has_value()) {
          if (SupportsAtomicReplaceRangeTombstone()) {
            return Status::TryAgain(
                "Atomic replace range acquired a partial overlap after "
                "preparation; retry ingestion");
          }
          return Status::InvalidArgument(
              "Atomic replace range partially overlaps with existing file");
        }
        if (assign_lsm_edit_global_seqnos_) {
          status = PickLsmEditTombstoneLevel(straddling_files);
          if (!status.ok()) {
            if (!lsm_edit_spec_->probe_straddling_files) {
              return status;
            }
            const Status probe_status =
                ProbeStraddlingFiles(super_version, straddling_files);
            if (!probe_status.ok()) {
              return probe_status.IsInvalidArgument() ? status : probe_status;
            }
            has_partial_overlap = false;
          }
        }
        if (has_partial_overlap) {
          ActivateAtomicReplaceRangeTombstone();
        }
      }
    }
  }

  if (lsm_edit_spec_.has_value()) {
    if (!lsm_edit_spec_->snapshot_consistency &&
        lsm_edit_spec_->deletion_mode == LsmEditDeletionMode::kFilesInRange &&
        !edit_.GetDeletedFiles().empty()) {
      return Status::InvalidArgument(
          "LSM edit snapshot_consistency=false cannot delete destination "
          "files: range is not empty");
    }
    status = AddRewrittenExistingFileDeletions(cfd_->current()->storage_info());
    if (!status.ok()) {
      return status;
    }
    status = ValidateLevelSplitSurvivors(cfd_->current()->storage_info());
    if (!status.ok()) {
      return status;
    }
    if (assign_lsm_edit_global_seqnos_) {
      status = AssignLsmEditGlobalSequenceNumbers(last_seqno);
      if (!status.ok()) {
        return status;
      }
    }
    status = RevalidateDeclaredLevelSurvivors(cfd_->current()->storage_info());
    if (!status.ok()) {
      return status;
    }
    if (requires_source_priority_for_equal_sequences_ &&
        !assign_lsm_edit_global_seqnos_) {
      edit_.RequireSourcePriorityForEqualSequences();
    }
  }

  // Find levels to ingest into
  std::optional<int> prev_batch_uppermost_level;
  // Batches at the front contain older updates and are placed deeper in the
  // LSM tree than later overlapping batches.
  for (auto& batch : file_batches_to_ingest_) {
    int batch_uppermost_level = 0;
    status = AssignLevelsForOneBatch(batch, force_global_seqno, &last_seqno,
                                     &batch_uppermost_level,
                                     prev_batch_uppermost_level);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to assign levels for one batch: %s",
                     status.ToString().c_str());
      return status;
    }

    prev_batch_uppermost_level = batch_uppermost_level;
  }

  if (lsm_edit_spec_.has_value() && max_assigned_seqno_ > 0 &&
      (!edit_.HasLastSequence() ||
       max_assigned_seqno_ > edit_.GetLastSequence())) {
    edit_.SetLastSequence(max_assigned_seqno_);
  }
  CreateEquivalentFileIngestingCompactions();
  return status;
}

Status ExternalSstFileIngestionJob::AssignLevelsForOneBatch(
    FileBatchInfo& batch, bool force_global_seqno, SequenceNumber* last_seqno,
    int* batch_uppermost_level, std::optional<int> prev_batch_uppermost_level) {
  assert(batch_uppermost_level);
  *batch_uppermost_level = std::numeric_limits<int>::max();
  for (IngestedFileInfo* file : batch.files) {
    Status status;
    assert(file);
    SequenceNumber assigned_seqno = 0;
    if (ingestion_options_.ingest_behind) {
      status = CheckLevelForIngestedBehindFile(file);
    } else {
      status = AssignLevelAndSeqnoForIngestedFile(
          force_global_seqno, cfd_->ioptions().compaction_style, *last_seqno,
          file, &assigned_seqno, prev_batch_uppermost_level);
    }

    // Modify the smallest/largest internal key to include the sequence number
    // that we just learned. Only overwrite sequence number zero. There could
    // be a nonzero sequence number already to indicate a range tombstone's
    // exclusive endpoint.
    ParsedInternalKey smallest_parsed;
    ParsedInternalKey largest_parsed;
    if (status.ok()) {
      status = ParseInternalKey(*(file->smallest_internal_key.rep()),
                                &smallest_parsed, false /* log_err_key */);
    }
    if (status.ok()) {
      status = ParseInternalKey(*(file->largest_internal_key.rep()),
                                &largest_parsed, false /* log_err_key */);
    }
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log, "Failed to parse internal key: %s",
                     status.ToString().c_str());
      return status;
    }

    // If any ingested file overlaps with the DB, it will fail here.
    if (ingestion_options_.allow_db_generated_files && assigned_seqno != 0 &&
        !assign_lsm_edit_global_seqnos_) {
      return Status::InvalidArgument(
          "An ingested file overlaps with existing data in the DB and has been "
          "assigned a non-zero sequence number, which is not allowed when "
          "'allow_db_generated_files' is enabled.");
    }

    if (smallest_parsed.sequence == 0 && assigned_seqno != 0) {
      UpdateInternalKey(file->smallest_internal_key.rep(), assigned_seqno,
                        smallest_parsed.type);
    }
    if (largest_parsed.sequence == 0 && assigned_seqno != 0) {
      UpdateInternalKey(file->largest_internal_key.rep(), assigned_seqno,
                        largest_parsed.type);
    }

    status = AssignGlobalSeqnoForIngestedFile(file, assigned_seqno);
    if (!status.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Failed to assign global sequence number for ingested file: %s",
          status.ToString().c_str());
      return status;
    }
    TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Run",
                             &assigned_seqno);
    assert(assign_lsm_edit_global_seqnos_ || assigned_seqno == 0 ||
           assigned_seqno == *last_seqno + 1);
    if (assigned_seqno > *last_seqno) {
      *last_seqno = assigned_seqno;
    }
    max_assigned_seqno_ = std::max(max_assigned_seqno_, assigned_seqno);

    int64_t temp_current_time = 0;
    uint64_t current_time = 0;
    if (clock_->GetCurrentTime(&temp_current_time).ok()) {
      current_time = static_cast<uint64_t>(temp_current_time);
    }
    uint64_t file_creation_time = kUnknownFileCreationTime;
    uint64_t oldest_ancestor_time = kUnknownOldestAncestorTime;
    ResolveExternalFileTimes(
        file->external_file_path, file->table_properties.file_creation_time,
        file->table_properties.oldest_ancestor_time, current_time,
        db_options_.info_log.get(), &file_creation_time, &oldest_ancestor_time);
    uint64_t tail_size = FileMetaData::CalculateTailSize(
        file->fd.GetFileSize(), file->table_properties);

    bool marked_for_compaction =
        file->table_properties.num_range_deletions == 1 &&
        (file->table_properties.num_entries ==
         file->table_properties.num_range_deletions);
    SequenceNumber smallest_seqno = file->assigned_seqno;
    SequenceNumber largest_seqno = file->assigned_seqno;
    if (ingestion_options_.allow_db_generated_files &&
        !assign_lsm_edit_global_seqnos_) {
      assert(file->assigned_seqno == 0);
      assert(file->smallest_seqno != kMaxSequenceNumber);
      assert(file->largest_seqno != kMaxSequenceNumber);
      smallest_seqno = file->smallest_seqno;
      largest_seqno = file->largest_seqno;
      max_assigned_seqno_ = std::max(max_assigned_seqno_, file->largest_seqno);
    }
    FileMetaData f_metadata(
        file->fd.GetNumber(), file->fd.GetPathId(), file->fd.GetFileSize(),
        file->smallest_internal_key, file->largest_internal_key, smallest_seqno,
        largest_seqno, false, file->file_temperature, kInvalidBlobFileNumber,
        oldest_ancestor_time, file_creation_time,
        ingestion_options_.ingest_behind
            ? kReservedEpochNumberForFileIngestedBehind
            : cfd_->NewEpochNumber(),  // orders files ingested to L0
        file->file_checksum, file->file_checksum_func_name, file->unique_id, 0,
        tail_size, file->user_defined_timestamps_persisted, "", "");
    f_metadata.temperature = file->file_temperature;
    f_metadata.marked_for_compaction = marked_for_compaction;
    f_metadata.db_ownership_start_time = file->db_ownership_start_time;
    f_metadata.data_file_provenance =
        DataFileProvenanceForOrigin(DataFileProvenance::Origin::kIngestion);
    f_metadata.skip_index_and_filter_blocks_prefetch =
        !file->prefetch_lmax_index_and_filter_blocks &&
        file->picked_level == cfd_->NumberLevels() - 1;
    // Extract min/max timestamps from table properties for UDT support.
    // This ensures ingested files have proper timestamp ranges in FileMetaData,
    // similar to files created by flush and compaction.
    ExtractTimestampFromTableProperties(file->table_properties, &f_metadata);
    // Retrieve file open metadata for fast SST open
    if (mutable_db_options_.fast_sst_open) {
      std::unique_ptr<FSRandomAccessFile> readable_file;
      FileOptions fopts{env_options_};
      fopts.file_checksum = f_metadata.file_checksum;
      fopts.file_checksum_func_name = f_metadata.file_checksum_func_name;
      IOStatus io_s = fs_->NewRandomAccessFile(file->internal_file_path, fopts,
                                               &readable_file, nullptr);
      if (io_s.ok()) {
        io_s =
            readable_file->GetFileOpenMetadata(&f_metadata.file_open_metadata);
        if (io_s.ok() && !f_metadata.file_open_metadata.empty() &&
            f_metadata.file_open_metadata.size() <=
                FSRandomAccessFile::kMaxFileOpenMetadataSize) {
          RecordTick(db_options_.stats, FILE_OPEN_METADATA_RETRIEVED);
        } else {
          if (io_s.ok() && f_metadata.file_open_metadata.size() >
                               FSRandomAccessFile::kMaxFileOpenMetadataSize) {
            ROCKS_LOG_WARN(db_options_.info_log,
                           "File open metadata for %s too large (%zu bytes), "
                           "ignoring",
                           file->internal_file_path.c_str(),
                           f_metadata.file_open_metadata.size());
          }
          f_metadata.file_open_metadata.clear();
        }
      }
    }
    edit_.AddFile(file->picked_level, f_metadata);

    *batch_uppermost_level =
        std::min(*batch_uppermost_level, file->picked_level);
  }

  return Status::OK();
}

void ExternalSstFileIngestionJob::CreateEquivalentFileIngestingCompactions() {
  // A map from output level to input levels of compactions equivalent to this
  // ingestion job.
  // TODO: simplify below logic to creating compaction per ingested file
  // instead of per output level, once we figure out how to treat ingested files
  // with adjacent range deletion tombstones to same output level in the same
  // job as non-overlapping compactions.
  std::map<int, std::map<int, CompactionInputFiles>>
      output_level_to_file_ingesting_compaction_inputs;

  for (const auto& pair : edit_.GetNewFiles()) {
    int output_level = pair.first;
    const FileMetaData& f_metadata = pair.second;

    CompactionInputFiles& input =
        output_level_to_file_ingesting_compaction_inputs[output_level][0];
    if (input.files.empty()) {
      // Treat the source level of ingested files to be level 0
      input.level = 0;
    }

    compaction_input_metdatas_.push_back(new FileMetaData(f_metadata));
    input.files.push_back(compaction_input_metdatas_.back());
  }

  for (const auto& [level, file] : deleted_file_compaction_inputs_) {
    CompactionInputFiles& input =
        output_level_to_file_ingesting_compaction_inputs[level][level];
    if (input.files.empty()) {
      input.level = level;
    }
    input.files.push_back(const_cast<FileMetaData*>(file));
  }

  if (lsm_edit_spec_) {
    for (const auto& split : lsm_edit_spec_->level_splits) {
      // Even unchanged, aligned files could be joined across a cut while the
      // MANIFEST write releases mutex_. Reserve the whole cut span until
      // install.
      auto* boundary = new FileMetaData();
      boundary->fd = FileDescriptor(0, 0, 0);
      boundary->smallest.Set(split.cut_points.front(), kMaxSequenceNumber,
                             kValueTypeForSeek);
      boundary->largest.Set(split.cut_points.back(), 0, kValueTypeForSeek);
      compaction_input_metdatas_.push_back(boundary);
      auto& input =
          output_level_to_file_ingesting_compaction_inputs[split.level][0];
      input.level = 0;
      input.files.push_back(boundary);
    }
  }

  for (const auto& pair : output_level_to_file_ingesting_compaction_inputs) {
    int output_level = pair.first;
    std::vector<CompactionInputFiles> inputs;
    inputs.reserve(pair.second.size());
    for (const auto& input_pair : pair.second) {
      inputs.push_back(input_pair.second);
    }

    const auto& mutable_cf_options = cfd_->GetLatestMutableCFOptions();
    file_ingesting_compactions_.push_back(new Compaction(
        cfd_->current()->storage_info(), cfd_->ioptions(), mutable_cf_options,
        mutable_db_options_, std::move(inputs), output_level,
        /* output file size limit not applicable */
        MaxFileSizeForLevel(mutable_cf_options, output_level,
                            cfd_->ioptions().compaction_style),
        LLONG_MAX /* max compaction bytes, not applicable */,
        0 /* output path ID, not applicable */, mutable_cf_options.compression,
        mutable_cf_options.compression_opts, Temperature::kUnknown,
        0 /* max_subcompaction, not applicable */,
        {} /* grandparents, not applicable */,
        std::nullopt /* earliest_snapshot */, nullptr /* snapshot_checker */,
        CompactionReason::kExternalSstIngestion, "" /* trim_ts */,
        -1 /* score, not applicable */,
        files_overlap_ ||
            (lsm_edit_spec_ && !lsm_edit_spec_->level_splits.empty())
        /* l0_files_might_overlap */));
  }
}

Status ExternalSstFileIngestionJob::AddFileToReplace(int level,
                                                     FileMetaData* file) {
  // Range checks against registered compactions do not cover every input
  // file, e.g. inputs of a compaction outputting to a different level or
  // files marked by DeleteFilesInRanges().
  return DeleteLiveFile(level, file);
}

void ExternalSstFileIngestionJob::RegisterRange() {
  const bool has_live_l0_inputs =
      std::any_of(deleted_file_compaction_inputs_.begin(),
                  deleted_file_compaction_inputs_.end(),
                  [](const auto& input) { return input.first == 0; });
  for (const auto& c : file_ingesting_compactions_) {
    cfd_->compaction_picker()->RegisterCompaction(
        c, c->output_level() == 0 && has_live_l0_inputs);
  }
  if (!deleted_file_compaction_inputs_.empty()) {
    if (lsm_edit_spec_) {
      cfd_->current()->storage_info()->ComputeCompactionScore(
          cfd_->ioptions(), cfd_->GetLatestMutableCFOptions(),
          cfd_->GetFullHistoryTsLow());
    } else {
      cfd_->current()
          ->storage_info()
          ->RemoveFilesBeingCompactedFromCompactionCandidates();
    }
  }
}

void ExternalSstFileIngestionJob::UnregisterRange(
    bool restore_compaction_candidates) {
  for (const auto& c : file_ingesting_compactions_) {
    c->MarkFilesBeingCompacted(false);
    cfd_->compaction_picker()->UnregisterCompaction(c);
    delete c;
  }
  file_ingesting_compactions_.clear();

  if (!deleted_file_compaction_inputs_.empty()) {
    if (lsm_edit_spec_ || restore_compaction_candidates) {
      cfd_->current()->storage_info()->ComputeCompactionScore(
          cfd_->ioptions(), cfd_->GetLatestMutableCFOptions(),
          cfd_->GetFullHistoryTsLow());
    }
    deleted_file_compaction_inputs_.clear();
  }

  for (const auto& f : compaction_input_metdatas_) {
    delete f;
  }
  compaction_input_metdatas_.clear();

  if (files_to_replace_version_ != nullptr) {
    files_to_replace_version_->Unref();
    files_to_replace_version_ = nullptr;
  }
}

void ExternalSstFileIngestionJob::UpdateStats() {
  // Update internal stats for new ingested files
  uint64_t total_keys = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_time = clock_->NowMicros() - job_start_time_;

  EventLoggerStream stream = event_logger_->Log();
  stream << "event" << "ingest_finished";
  stream << "files_ingested";
  stream.StartArray();

  for (IngestedFileInfo& f : files_to_ingest_) {
    InternalStats::CompactionStats stats(
        CompactionReason::kExternalSstIngestion, 1);
    stats.micros = total_time;
    // If actual copy occurred for this file, then we need to count the file
    // size as the actual bytes written. If the file was linked, then we ignore
    // the bytes written for file metadata.
    // TODO (yanqin) maybe account for file metadata bytes for exact accuracy?
    if (f.copy_file) {
      stats.bytes_written = f.fd.GetFileSize();
    } else {
      stats.bytes_moved = f.fd.GetFileSize();
    }
    stats.num_output_files = 1;
    cfd_->internal_stats()->AddCompactionStats(f.picked_level,
                                               Env::Priority::USER, stats);
    cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_INGESTED_ADD_FILE,
                                       f.fd.GetFileSize());
    total_keys += f.num_entries;
    if (f.picked_level == 0) {
      total_l0_files += 1;
    }
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[AddFile] External SST file %s was ingested in L%d with path %s "
        "(global_seqno=%" PRIu64 ")\n",
        f.external_file_path.c_str(), f.picked_level,
        f.internal_file_path.c_str(), f.assigned_seqno);
    stream << "file" << f.internal_file_path << "level" << f.picked_level;
  }
  stream.EndArray();

  stream << "lsm_state";
  stream.StartArray();
  auto vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_KEYS_TOTAL,
                                     total_keys);
  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_FILES_TOTAL,
                                     files_to_ingest_.size());
  cfd_->internal_stats()->AddCFStats(
      InternalStats::INGESTED_LEVEL0_NUM_FILES_TOTAL, total_l0_files);
}

void ExternalSstFileIngestionJob::Cleanup(const Status& status) {
  if (merge_snapshot_context_) {
    merge_snapshot_context_->Clean();
    merge_snapshot_context_.reset();
  }
  IOOptions io_opts;
  for (const auto& input : merged_input_files_) {
    Status removal =
        fs_->DeleteFile(input.internal_file_path, io_opts, nullptr);
    if (!removal.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log, "Failed to remove merged input copy %s: %s",
          input.internal_file_path.c_str(), removal.ToString().c_str());
    }
    if (status.ok() && ingestion_options_.move_files) {
      removal = fs_->DeleteFile(input.external_file_path, io_opts, nullptr);
      if (!removal.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to remove original merged input %s: %s",
                       input.external_file_path.c_str(),
                       removal.ToString().c_str());
      }
    }
  }
  merged_input_files_.clear();
  if (!status.ok()) {
    // We failed to add the files to the database
    // remove all the files we copied
    DeleteInternalFiles();
    files_overlap_ = false;
  } else {
    if (atomic_replace_range_tombstone_.has_value()) {
      Status s = DeleteInternalFile(&*atomic_replace_range_tombstone_);
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Failed to remove unused atomic replace range tombstone %s: %s",
            atomic_replace_range_tombstone_->internal_file_path.c_str(),
            s.ToString().c_str());
      }
      atomic_replace_range_tombstone_.reset();
    }
  }
  if (status.ok() && ingestion_options_.move_files) {
    // The files were moved and added successfully, remove original file links
    for (IngestedFileInfo& f : files_to_ingest_) {
      if (f.generated_for_ingestion) {
        continue;
      }
      Status s = fs_->DeleteFile(f.external_file_path, io_opts, nullptr);
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "%s was added to DB successfully but failed to remove original "
            "file link : %s",
            f.external_file_path.c_str(), s.ToString().c_str());
      }
    }
  }
}

void ExternalSstFileIngestionJob::DeleteInternalFiles() {
  for (IngestedFileInfo& f : files_to_ingest_) {
    if (f.internal_file_path.empty()) {
      continue;
    }
    Status s = DeleteInternalFile(&f);
    if (!s.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "AddFile() clean up for file %s failed : %s",
                     f.internal_file_path.c_str(), s.ToString().c_str());
    }
  }
  if (atomic_replace_range_tombstone_.has_value() &&
      !atomic_replace_range_tombstone_->internal_file_path.empty()) {
    Status s = DeleteInternalFile(&*atomic_replace_range_tombstone_);
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "AddFile() clean up for generated tombstone file %s failed : %s",
          atomic_replace_range_tombstone_->internal_file_path.c_str(),
          s.ToString().c_str());
    }
    atomic_replace_range_tombstone_.reset();
  }
}

Status ExternalSstFileIngestionJob::RegisterGeneratedFile(
    IngestedFileInfo* file) {
  auto* manager = static_cast_with_check<SstFileManagerImpl>(
      db_options_.sst_file_manager.get());
  if (manager == nullptr) {
    return Status::OK();
  }
  Status status = manager->OnAddFile(file->internal_file_path, file->file_size);
  if (!status.ok()) {
    return status;
  }
  file->registered_with_sst_file_manager = true;
  if (manager->IsMaxAllowedSpaceReached()) {
    return Status::SpaceLimit(
        "Max allowed space was reached by generated ingestion SST");
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::DeleteInternalFile(IngestedFileInfo* file) {
  Status status =
      fs_->DeleteFile(file->internal_file_path, IOOptions(), nullptr);
  if ((status.ok() || status.IsNotFound()) &&
      file->registered_with_sst_file_manager) {
    auto* manager = static_cast_with_check<SstFileManagerImpl>(
        db_options_.sst_file_manager.get());
    assert(manager != nullptr);
    Status untrack_status = manager->OnDeleteFile(file->internal_file_path);
    file->registered_with_sst_file_manager = false;
    if (!untrack_status.ok()) {
      return untrack_status;
    }
  }
  return status;
}

Status ExternalSstFileIngestionJob::ResetTableReader(
    const std::string& external_file, uint64_t new_file_number,
    bool user_defined_timestamps_persisted, SuperVersion* sv,
    IngestedFileInfo* file_to_ingest,
    std::unique_ptr<TableReader>* table_reader) {
  std::unique_ptr<FSRandomAccessFile> sst_file;
  FileOptions fo{env_options_};
  fo.temperature = file_to_ingest->file_temperature;
  fo.file_checksum_func_name = kNoFileChecksumFuncName;
  Status status =
      fs_->NewRandomAccessFile(external_file, fo, &sst_file, nullptr);
  if (!status.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "Failed to create random access file for external file %s: %s",
        external_file.c_str(), status.ToString().c_str());
    return status;
  }
  Temperature updated_temp = sst_file->GetTemperature();
  if (updated_temp != Temperature::kUnknown &&
      updated_temp != file_to_ingest->file_temperature) {
    // The hint was missing or wrong. Track temperature reported by storage.
    file_to_ingest->file_temperature = updated_temp;
  }
  std::unique_ptr<RandomAccessFileReader> sst_file_reader(
      new RandomAccessFileReader(std::move(sst_file), external_file,
                                 nullptr /*Env*/, io_tracer_));
  table_reader->reset();
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  status = sv->mutable_cf_options.table_factory->NewTableReader(
      ro,
      TableReaderOptions(
          cfd_->ioptions(), sv->mutable_cf_options.prefix_extractor,
          sv->mutable_cf_options.compression_manager.get(), env_options_,
          cfd_->internal_comparator(),
          sv->mutable_cf_options.block_protection_bytes_per_key,
          /*skip_filters*/ false, /*immortal*/ false,
          /*force_direct_prefetch*/ false, /*level*/ -1,
          /*block_cache_tracer*/ nullptr,
          /*max_file_size_for_l0_meta_pin*/ 0, versions_->DbSessionId(),
          /*cur_file_num*/ new_file_number,
          /* unique_id */ {},
          /* largest_seqno */ file_to_ingest->assigned_seqno,
          /* tail_size */ 0, user_defined_timestamps_persisted),
      std::move(sst_file_reader), file_to_ingest->file_size, table_reader,
      // No need to prefetch index/filter if caching is not needed.
      /*prefetch_index_and_filter_in_cache=*/ingestion_options_.fill_cache);
  return status;
}

Status ExternalSstFileIngestionJob::SanityCheckTableProperties(
    const std::string& external_file, const TableProperties& props,
    IngestedFileInfo* file_to_ingest) {
  const auto& uprops = props.user_collected_properties;

  // Get table version
  auto version_iter = uprops.find(ExternalSstFilePropertyNames::kVersion);
  if (version_iter == uprops.end()) {
    assert(!SstFileWriter::CreatedBySstFileWriter(props));
    if (!ingestion_options_.allow_db_generated_files) {
      return Status::Corruption("External file version not found");
    } else {
      // 0 is special version for when a file from live DB does not have the
      // version table property
      file_to_ingest->version = 0;
    }
  } else {
    assert(SstFileWriter::CreatedBySstFileWriter(props));
    file_to_ingest->version = DecodeFixed32(version_iter->second.c_str());
  }

  auto seqno_iter = uprops.find(ExternalSstFilePropertyNames::kGlobalSeqno);
  if (file_to_ingest->version == 2) {
    // version 2 imply that we have global sequence number
    if (seqno_iter == uprops.end()) {
      return Status::Corruption(
          "External file global sequence number not found");
    }

    // Set the global sequence number
    file_to_ingest->original_seqno = DecodeFixed64(seqno_iter->second.c_str());
  } else if (file_to_ingest->version == 1) {
    // SST file V1 should not have global seqno field
    assert(seqno_iter == uprops.end());
    file_to_ingest->original_seqno = 0;
    if (ingestion_options_.allow_blocking_flush ||
        ingestion_options_.allow_global_seqno) {
      return Status::InvalidArgument(
          "External SST file V1 does not support global seqno");
    }
  } else if (file_to_ingest->version == 0) {
    // allow_db_generated_files is true
    assert(seqno_iter == uprops.end());
    file_to_ingest->original_seqno = 0;
  } else {
    return Status::InvalidArgument("External file version " +
                                   std::to_string(file_to_ingest->version) +
                                   " is not supported");
  }

  file_to_ingest->cf_id = static_cast<uint32_t>(props.column_family_id);
  file_to_ingest->table_properties = props;

  // Get number of entries in table
  file_to_ingest->num_entries = props.num_entries;
  file_to_ingest->num_range_deletions = props.num_range_deletions;
  file_to_ingest->entry_stats_initialized = true;

  // Validate table properties related to comparator name and user defined
  // timestamps persisted flag.
  file_to_ingest->user_defined_timestamps_persisted =
      static_cast<bool>(props.user_defined_timestamps_persisted);
  bool mark_sst_file_has_no_udt = false;
  Status s = ValidateUserDefinedTimestampsOptions(
      cfd_->user_comparator(), props.comparator_name,
      cfd_->ioptions().persist_user_defined_timestamps,
      file_to_ingest->user_defined_timestamps_persisted,
      &mark_sst_file_has_no_udt);
  if (s.ok() && mark_sst_file_has_no_udt) {
    // A column family that enables user-defined timestamps in Memtable only
    // feature can also ingest external files created by a setting that disables
    // user-defined timestamps. In that case, we need to re-mark the
    // user_defined_timestamps_persisted flag for the file. The open-and-scan
    // caller is then responsible for reopening its `TableReader` with the
    // updated flag.
    file_to_ingest->user_defined_timestamps_persisted = false;
  } else if (!s.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "ValidateUserDefinedTimestampsOptions failed for external file %s: %s",
        external_file.c_str(), s.ToString().c_str());
    return s;
  }

  return s;
}

Status ExternalSstFileIngestionJob::GetIngestedFileInfoFromFileInfo(
    const std::string& external_file,
    const PreparedFileInfo& prepared_file_info,
    IngestedFileInfo* file_to_ingest) {
  // Boundaries, size, and table properties were obtained without opening the
  // file (e.g. produced by SstFileWriter::Finish). We reuse them directly.
  file_to_ingest->file_size = prepared_file_info.file_size;
  Status status = SanityCheckTableProperties(
      external_file, prepared_file_info.table_properties, file_to_ingest);
  if (!status.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "Failed to sanity check table properties for external file %s: %s",
        external_file.c_str(), status.ToString().c_str());
    return status;
  }

  const size_t ts_sz = ucmp_->timestamp_size();
  if (ts_sz > 0 && !file_to_ingest->user_defined_timestamps_persisted) {
    auto pad_timestamp = [ts_sz](std::string* result, const Slice& key) {
      assert(result->empty());
      if (ExtractValueType(key) == kTypeRangeDeletion) {
        PadInternalKeyWithMaxTimestamp(result, key, ts_sz);
      } else {
        PadInternalKeyWithMinTimestamp(result, key, ts_sz);
      }
    };
    pad_timestamp(file_to_ingest->smallest_internal_key.rep(),
                  prepared_file_info.smallest.Encode());
    pad_timestamp(file_to_ingest->largest_internal_key.rep(),
                  prepared_file_info.largest.Encode());
  } else {
    file_to_ingest->smallest_internal_key = prepared_file_info.smallest;
    file_to_ingest->largest_internal_key = prepared_file_info.largest;
  }

  if (ingestion_options_.allow_db_generated_files) {
    // Sequence numbers are preserved (not reassigned), so the bounds must be
    // known before we skip the GetSeqnoBoundaryForFile scan.
    if (!file_to_ingest->table_properties.HasKeyLargestSeqno()) {
      return Status::Corruption("Unknown largest seqno for db generated file.");
    }
    file_to_ingest->largest_seqno =
        file_to_ingest->table_properties.key_largest_seqno;
    if (file_to_ingest->largest_seqno == 0) {
      file_to_ingest->smallest_seqno = 0;
    } else {
      if (!file_to_ingest->table_properties.HasKeySmallestSeqno()) {
        return Status::Corruption(
            "Unknown smallest seqno for db generated file.");
      }
      file_to_ingest->smallest_seqno =
          file_to_ingest->table_properties.key_smallest_seqno;
    }
  } else {
    // Normal ingestion reassigns a global sequence number later, so the file's
    // keys must currently be at seqno 0 (mirror of the open-and-scan check).
    SequenceNumber largest_seqno =
        file_to_ingest->table_properties.key_largest_seqno;
    // UINT64_MAX means unknown and the file is generated before table property
    // `key_largest_seqno` is introduced.
    if (largest_seqno != UINT64_MAX && largest_seqno > 0) {
      return Status::Corruption(
          "External file has non zero largest sequence number " +
          std::to_string(largest_seqno));
    }
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::GetIngestedFileInfoFromFile(
    const std::string& external_file, uint64_t new_file_number,
    IngestedFileInfo* file_to_ingest, SuperVersion* sv,
    std::unique_ptr<TableReader>* out_table_reader) {
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::GetIngestedFileInfo:ReadPath");

  // Get external file size
  Status status = fs_->GetFileSize(external_file, IOOptions(),
                                   &file_to_ingest->file_size, nullptr);
  if (!status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get file size for external file %s: %s",
                   external_file.c_str(), status.ToString().c_str());
    return status;
  }

  // Create TableReader for external file.
  std::unique_ptr<TableReader> table_reader;
  // Initially create the `TableReader` with flag
  // `user_defined_timestamps_persisted` to be true since that's the most
  // common case
  status = ResetTableReader(external_file, new_file_number,
                            /*user_defined_timestamps_persisted=*/true, sv,
                            file_to_ingest, &table_reader);
  if (!status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to reset table reader for external file %s: %s",
                   external_file.c_str(), status.ToString().c_str());
    return status;
  }

  status = SanityCheckTableProperties(
      external_file, *table_reader->GetTableProperties(), file_to_ingest);
  if (!status.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "Failed to sanity check table properties for external file %s: %s",
        external_file.c_str(), status.ToString().c_str());
    return status;
  }

  // The `TableReader` above was opened with `user_defined_timestamps_persisted`
  // assumed true. If the sanity check determined the file has no persisted
  // timestamps (UDT-in-Memtable-only feature), reopen it with the corrected
  // flag so keys are parsed properly by the scan below.
  if (ucmp_->timestamp_size() > 0 &&
      !file_to_ingest->user_defined_timestamps_persisted) {
    status = ResetTableReader(external_file, new_file_number,
                              file_to_ingest->user_defined_timestamps_persisted,
                              sv, file_to_ingest, &table_reader);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to reset table reader for external file %s: %s",
                     external_file.c_str(), status.ToString().c_str());
      return status;
    }
  }

  const bool allow_data_in_errors = db_options_.allow_data_in_errors;
  ParsedInternalKey key;
  if (ingestion_options_.allow_db_generated_files) {
    // We are ingesting a DB generated SST file for which we don't reassign
    // sequence numbers. We need its smallest sequence number and largest
    // sequence number for FileMetaData.
    Status seqno_status = GetSeqnoBoundaryForFile(
        table_reader.get(), sv, file_to_ingest, allow_data_in_errors);

    if (!seqno_status.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Failed to get sequence number boundary for external file %s: %s",
          external_file.c_str(), seqno_status.ToString().c_str());
      return seqno_status;
    }
    assert(file_to_ingest->smallest_seqno <= file_to_ingest->largest_seqno);
    assert(file_to_ingest->largest_seqno < kMaxSequenceNumber);
  } else {
    SequenceNumber largest_seqno =
        table_reader.get()->GetTableProperties()->key_largest_seqno;
    // UINT64_MAX means unknown and the file is generated before table
    // property `key_largest_seqno` is introduced.
    if (largest_seqno != UINT64_MAX && largest_seqno > 0) {
      return Status::Corruption(
          "External file has non zero largest sequence number " +
          std::to_string(largest_seqno));
    }
  }

  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ro, sv->mutable_cf_options.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));

  // Get first (smallest) and last (largest) key from file.
  iter->SeekToFirst();
  if (iter->Valid()) {
    Status pik_status =
        ParseInternalKey(iter->key(), &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    if (key.sequence != 0 && !ingestion_options_.allow_db_generated_files) {
      return Status::Corruption("External file has non zero sequence number");
    }
    file_to_ingest->smallest_internal_key.SetFrom(key);

    Slice largest;
    if (strcmp(sv->mutable_cf_options.table_factory->Name(), "PlainTable") ==
        0) {
      // PlainTable iterator does not support SeekToLast().
      largest = iter->key();
      for (; iter->Valid(); iter->Next()) {
        if (cfd_->internal_comparator().Compare(iter->key(), largest) > 0) {
          largest = iter->key();
        }
      }
      if (!iter->status().ok()) {
        return iter->status();
      }
    } else {
      iter->SeekToLast();
      if (!iter->Valid()) {
        if (iter->status().ok()) {
          // The file contains at least 1 key since iter is valid after
          // SeekToFirst().
          return Status::Corruption("Can not find largest key in sst file");
        } else {
          return iter->status();
        }
      }
      largest = iter->key();
    }

    pik_status = ParseInternalKey(largest, &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    if (key.sequence != 0 && !ingestion_options_.allow_db_generated_files) {
      return Status::Corruption("External file has non zero sequence number");
    }
    file_to_ingest->largest_internal_key.SetFrom(key);
  } else if (!iter->status().ok()) {
    return iter->status();
  }

  if (lsm_edit_spec_.has_value()) {
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Status pik_status =
          ParseInternalKey(iter->key(), &key, allow_data_in_errors);
      if (!pik_status.ok()) {
        return Status::Corruption("Corrupted key in external file. ",
                                  pik_status.getState());
      }
      Status blob_status =
          ValidateNoExternalBlobReferences(key, iter->value(), external_file);
      if (!blob_status.ok()) {
        return blob_status;
      }
    }
    if (!iter->status().ok()) {
      return iter->status();
    }
  }

  std::unique_ptr<InternalIterator> range_del_iter(
      table_reader->NewRangeTombstoneIterator(ro));
  // We may need to adjust these key bounds, depending on whether any range
  // deletion tombstones extend past them.
  if (range_del_iter != nullptr) {
    for (range_del_iter->SeekToFirst(); range_del_iter->Valid();
         range_del_iter->Next()) {
      Status pik_status =
          ParseInternalKey(range_del_iter->key(), &key, allow_data_in_errors);
      if (!pik_status.ok()) {
        return Status::Corruption("Corrupted key in external file. ",
                                  pik_status.getState());
      }
      if (key.sequence != 0 && !ingestion_options_.allow_db_generated_files) {
        return Status::Corruption(
            "External file has a range deletion with non zero sequence "
            "number.");
      }
#ifndef NDEBUG
      // To keep aligned with the fast path that expects range deletion keys to
      // always use max ts.
      const size_t ts_sz = ucmp_->timestamp_size();
      if (ts_sz > 0) {
        const std::string max_ts(ts_sz, '\xff');
        assert(key.user_key.size() >= ts_sz);
        assert(ucmp_->CompareTimestamp(
                   ExtractTimestampFromUserKey(key.user_key, ts_sz), max_ts) ==
               0);
        assert(range_del_iter->value().size() >= ts_sz);
        assert(ucmp_->CompareTimestamp(
                   ExtractTimestampFromUserKey(range_del_iter->value(), ts_sz),
                   max_ts) == 0);
      }
#endif
      RangeTombstone tombstone(key, range_del_iter->value());
      file_range_checker_.MaybeUpdateRange(tombstone.SerializeKey(),
                                           tombstone.SerializeEndKey(),
                                           file_to_ingest);
    }
  }

  *out_table_reader = std::move(table_reader);
  return Status::OK();
}

Status ExternalSstFileIngestionJob::GetIngestedFileInfo(
    const std::string& external_file, uint64_t new_file_number,
    const PreparedFileInfo* prepared_file_info,
    IngestedFileInfo* file_to_ingest, SuperVersion* sv) {
  file_to_ingest->external_file_path = external_file;

  std::unique_ptr<TableReader> table_reader;
  Status status =
      prepared_file_info != nullptr
          ? GetIngestedFileInfoFromFileInfo(external_file, *prepared_file_info,
                                            file_to_ingest)
          : GetIngestedFileInfoFromFile(external_file, new_file_number,
                                        file_to_ingest, sv, &table_reader);
  if (!status.ok()) {
    return status;
  }

  assert(file_to_ingest->file_size > 0);
  assert(!file_to_ingest->unset());
  assert(file_to_ingest->table_properties.num_entries > 0 ||
         file_to_ingest->table_properties.num_range_deletions > 0);
  if (ingestion_options_.allow_db_generated_files) {
    // These files keep their original sequence numbers (derived, not
    // reassigned), so the bounds must be valid here.
    assert(file_to_ingest->smallest_seqno <= file_to_ingest->largest_seqno);
    assert(file_to_ingest->largest_seqno < kMaxSequenceNumber);
  }

  // Verify the file checksum if requested. The open-and-scan path already has a
  // `TableReader` open; the fast-path opens one here only when verification is
  // requested -- otherwise it performs no file I/O.
  if (ingestion_options_.verify_checksums_before_ingest) {
    if (table_reader == nullptr) {
      status =
          ResetTableReader(external_file, new_file_number,
                           file_to_ingest->user_defined_timestamps_persisted,
                           sv, file_to_ingest, &table_reader);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to reset table reader for external file %s: %s",
                       external_file.c_str(), status.ToString().c_str());
        return status;
      }
    }
    // If customized readahead size is needed, we can pass a user option all the
    // way to here. Right now we just rely on the default readahead to keep
    // things simple.
    // TODO: plumb Env::IOActivity, Env::IOPriority
    ReadOptions ro;
    ro.readahead_size = ingestion_options_.verify_checksums_readahead_size;
    ro.fill_cache = ingestion_options_.fill_cache;
    status = table_reader->VerifyChecksum(
        ro, TableReaderCaller::kExternalSSTIngestion);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to verify checksum for external file %s: %s",
                     external_file.c_str(), status.ToString().c_str());
      return status;
    }
  }

  // Assign FD with number.
  file_to_ingest->fd =
      FileDescriptor(new_file_number, 0, file_to_ingest->file_size);

  const size_t ts_sz = ucmp_->timestamp_size();
  Slice smallest = file_to_ingest->smallest_internal_key.user_key();
  Slice largest = file_to_ingest->largest_internal_key.user_key();
  if (ts_sz > 0) {
    AppendUserKeyWithMaxTimestamp(&file_to_ingest->start_ukey, smallest, ts_sz);
    AppendUserKeyWithMinTimestamp(&file_to_ingest->limit_ukey, largest, ts_sz);
  } else {
    file_to_ingest->start_ukey.assign(smallest.data(), smallest.size());
    file_to_ingest->limit_ukey.assign(largest.data(), largest.size());
  }

  auto s =
      GetSstInternalUniqueId(file_to_ingest->table_properties.db_id,
                             file_to_ingest->table_properties.db_session_id,
                             file_to_ingest->table_properties.orig_file_number,
                             &(file_to_ingest->unique_id));
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get SST unique id for file %s",
                   file_to_ingest->internal_file_path.c_str());
    file_to_ingest->unique_id = kNullUniqueId64x2;
  }

  return status;
}

Status ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile(
    bool force_global_seqno, CompactionStyle compaction_style,
    SequenceNumber last_seqno, IngestedFileInfo* file_to_ingest,
    SequenceNumber* assigned_seqno,
    std::optional<int> prev_batch_uppermost_level) {
  *assigned_seqno = 0;
  if (file_to_ingest->declared_level >= 0) {
    *assigned_seqno = file_to_ingest->assigned_seqno;
    // DB::ApplyLsmEdit(): the caller stated where this file goes, so validate
    // that instead of searching for a level.
    return UseDeclaredLevelForIngestedFile(file_to_ingest);
  }
  Status status;
  const size_t ts_sz = ucmp_->timestamp_size();
  assert(!prev_batch_uppermost_level.has_value() ||
         prev_batch_uppermost_level.value() < cfd_->NumberLevels());
  bool must_assign_to_l0 = (prev_batch_uppermost_level.has_value() &&
                            prev_batch_uppermost_level.value() == 0) ||
                           compaction_style == kCompactionStyleFIFO;

  if (force_global_seqno || (!ingestion_options_.allow_db_generated_files &&
                             (files_overlap_ || must_assign_to_l0))) {
    *assigned_seqno = last_seqno + 1;
    if (must_assign_to_l0) {
      assert(ts_sz == 0);
      file_to_ingest->picked_level = 0;
      if (ingestion_options_.fail_if_not_bottommost_level &&
          cfd_->NumberLevels() > 1) {
        status = Status::TryAgain(
            "Files cannot be ingested to Lmax. Please make sure key range of "
            "Lmax does not overlap with files to ingest.");
      }
      return status;
    }
  }

  bool overlap_with_db = false;
  Arena arena;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  ro.total_order_seek = true;
  int target_level = 0;
  auto* vstorage = cfd_->current()->storage_info();
  assert(!must_assign_to_l0 || ingestion_options_.allow_db_generated_files);
  int assigned_level_exclusive_end = cfd_->NumberLevels();
  if (must_assign_to_l0) {
    assigned_level_exclusive_end = 0;
  } else if (prev_batch_uppermost_level.has_value()) {
    assigned_level_exclusive_end = prev_batch_uppermost_level.value();
  }

  // When ingesting db generated files, we require that ingested files do not
  // overlap with any file in the DB. So we need to check all levels.
  int overlap_checking_exclusive_end =
      ingestion_options_.allow_db_generated_files
          ? cfd_->NumberLevels()
          : assigned_level_exclusive_end;
  const bool is_generated_range_tombstone =
      atomic_replace_range_tombstone_active_ &&
      file_to_ingest->generated_for_ingestion;
  for (int lvl = 0; lvl < overlap_checking_exclusive_end; lvl++) {
    if (lvl > 0 && lvl < vstorage->base_level()) {
      continue;
    }
    if (lvl < assigned_level_exclusive_end &&
        atomic_replace_range_.has_value() &&
        !atomic_replace_range_tombstone_active_) {
      target_level = lvl;
      continue;
    }
    if (cfd_->RangeOverlapWithCompaction(file_to_ingest->start_ukey,
                                         file_to_ingest->limit_ukey, lvl,
                                         /*range_limit_exclusive=*/
                                         is_generated_range_tombstone)) {
      // We must use L0 or any level higher than `lvl` to be able to overwrite
      // the compaction output keys that we overlap with in this level, We also
      // need to assign this file a seqno to overwrite the compaction output
      // keys in level `lvl`
      overlap_with_db = true;
      break;
    } else if (vstorage->NumLevelFiles(lvl) > 0) {
      bool overlap_with_level = false;
      status = cfd_->current()->OverlapWithLevelIterator(
          ro, env_options_, file_to_ingest->start_ukey,
          file_to_ingest->limit_ukey, lvl, &overlap_with_level);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to check overlap with level iterator: %s",
                       status.ToString().c_str());
        return status;
      }
      if (overlap_with_level) {
        // We must use L0 or any level higher than `lvl` to be able to overwrite
        // the keys that we overlap with in this level, We also need to assign
        // this file a seqno to overwrite the existing keys in level `lvl`
        overlap_with_db = true;
        break;
      }
    }

    // We don't overlap with any keys in this level, but we still need to check
    // if our file can fit in it
    if (lvl < assigned_level_exclusive_end &&
        IngestedFileFitInLevel(file_to_ingest, lvl)) {
      target_level = lvl;
    }
  }

  if (ingestion_options_.fail_if_not_bottommost_level &&
      target_level < cfd_->NumberLevels() - 1) {
    status = Status::TryAgain(
        "Files cannot be ingested to Lmax. Please make sure key range of Lmax "
        "and ongoing compaction's output to Lmax does not overlap with files "
        "to ingest. Input files overlapping with each other can cause some "
        "file to be assigned to non Lmax level.");
    return status;
  }

  TEST_SYNC_POINT_CALLBACK(
      "ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile",
      &overlap_with_db);
  file_to_ingest->picked_level = target_level;
  if (overlap_with_db) {
    if (ts_sz > 0) {
      status = Status::InvalidArgument(
          "Column family enables user-defined timestamps, please make sure the "
          "key range (without timestamp) of external file does not overlap "
          "with key range (without timestamp) in the db");
      return status;
    }
    if (*assigned_seqno == 0) {
      *assigned_seqno = last_seqno + 1;
    }
  }

  return status;
}

Status ExternalSstFileIngestionJob::CheckLevelForIngestedBehindFile(
    IngestedFileInfo* file_to_ingest) {
  assert(!atomic_replace_range_.has_value());

  auto* vstorage = cfd_->current()->storage_info();
  // First, check if new files fit in the last level
  int last_lvl = cfd_->NumberLevels() - 1;
  if (!IngestedFileFitInLevel(file_to_ingest, last_lvl)) {
    return Status::InvalidArgument(
        "Can't ingest_behind file as it doesn't fit "
        "at the last level!");
  }

  // Second, check if despite cf_allow_ingest_behind=true we still have 0
  // seqnums at some upper level
  for (int lvl = 0; lvl < cfd_->NumberLevels() - 1; lvl++) {
    for (auto file : vstorage->LevelFiles(lvl)) {
      if (file->fd.smallest_seqno == 0) {
        return Status::InvalidArgument(
            "Can't ingest_behind file as despite cf_allow_ingest_behind=true "
            "there are files with 0 seqno in database at upper levels!");
      }
    }
  }

  file_to_ingest->picked_level = last_lvl;
  return Status::OK();
}

Status ExternalSstFileIngestionJob::ProbeStraddlingFiles(
    SuperVersion* super_version,
    const autovector<std::pair<int, const FileMetaData*>>& straddling_files) {
  assert(lsm_edit_spec_.has_value());
  assert(atomic_replace_range_.has_value());
  assert(!atomic_replace_range_->unset());
  assert(ucmp_->timestamp_size() == 0);

  const Slice start = atomic_replace_range_->smallest_internal_key.user_key();
  const Slice limit = atomic_replace_range_->largest_internal_key.user_key();
  const InternalKeyComparator& icmp = cfd_->internal_comparator();
  InternalKey seek_key;
  seek_key.Set(start, kMaxSequenceNumber, kValueTypeForSeek);

  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  ro.total_order_seek = true;

  for (const auto& [level, file] : straddling_files) {
    Arena arena;
    ReadRangeDelAggregator range_del_agg(&icmp,
                                         kMaxSequenceNumber /* upper_bound */);
    ScopedArenaPtr<InternalIterator> iter(cfd_->table_cache()->NewIterator(
        ro, env_options_, icmp, *file, &range_del_agg,
        super_version->mutable_cf_options, /*table_reader_ptr=*/nullptr,
        cfd_->internal_stats()->GetFileReadHist(level),
        TableReaderCaller::kExternalSSTIngestion, &arena,
        /*skip_filters=*/false, level, /*max_file_size_for_l0_meta_pin=*/0,
        /*smallest_compaction_key=*/nullptr,
        /*largest_compaction_key=*/nullptr, /*allow_unprepared_value=*/false,
        nullptr, nullptr));
    iter->Seek(seek_key.Encode());
    Status s = iter->status();
    if (!s.ok()) {
      return s;
    }
    bool in_range = false;
    if (iter->Valid()) {
      ParsedInternalKey parsed;
      s = ParseInternalKey(iter->key(), &parsed,
                           db_options_.allow_data_in_errors);
      if (!s.ok()) {
        return s;
      }
      in_range = ucmp_->Compare(parsed.user_key, limit) < 0;
    }
    // A range tombstone reaching into the edit's range would shadow the
    // grafted data, which carries the sequence numbers it was written with.
    if (!in_range) {
      in_range = range_del_agg.IsRangeOverlapped(start, limit,
                                                 /*end_exclusive=*/true);
    }
    if (in_range) {
      return Status::InvalidArgument("LSM edit range is not empty: " +
                                     MakeTableFileName(file->fd.GetNumber()) +
                                     " at level " + std::to_string(level) +
                                     " holds keys inside it");
    }
  }
  return Status::OK();
}

bool ExternalSstFileIngestionJob::DeclaredFileFitsInLevel(
    const IngestedFileInfo* file_to_ingest, int level) const {
  if (level == 0) {
    // Level 0 sorted runs are allowed to overlap.
    return true;
  }
  const VersionEdit::DeletedFiles& deleted = edit_.GetDeletedFiles();
  const InternalKey& start = file_to_ingest->smallest_internal_key;
  const InternalKey& limit = file_to_ingest->largest_internal_key;
  const std::vector<FileMetaData*>& files =
      cfd_->current()->storage_info()->LevelFiles(level);
  // OverlapInLevel() cannot exclude files removed by this VersionEdit. Find
  // the first possible overlap, then scan only intersecting surviving files.
  auto existing = std::lower_bound(
      files.begin(), files.end(), start,
      [this](const FileMetaData* file, const InternalKey& key) {
        return sstableKeyCompare(ucmp_, file->largest, key) < 0;
      });
  for (; existing != files.end() &&
         sstableKeyCompare(ucmp_, (*existing)->smallest, limit) <= 0;
       ++existing) {
    if (deleted.count({level, (*existing)->fd.GetNumber()}) > 0) {
      continue;
    }
    return false;
  }
  return true;
}

Status ExternalSstFileIngestionJob::UseDeclaredLevelForIngestedFile(
    IngestedFileInfo* file_to_ingest) {
  assert(lsm_edit_spec_.has_value());
  const int level = file_to_ingest->declared_level;
  assert(level >= 0 && level < cfd_->NumberLevels());

  // Run() has already rejected the edit if its range overlaps a pending
  // compaction, and every file being installed lies inside that range, so the
  // only thing left to establish is that the level has room.
  if (!DeclaredFileFitsInLevel(file_to_ingest, level)) {
    return Status::InvalidArgument(
        "LSM edit declares level " + std::to_string(level) +
        " for a file whose key range overlaps a file that stays at that level");
  }
  file_to_ingest->picked_level = level;
  return Status::OK();
}

Status ExternalSstFileIngestionJob::AssignGlobalSeqnoForIngestedFile(
    IngestedFileInfo* file_to_ingest, SequenceNumber seqno) {
  if (ingestion_options_.allow_db_generated_files &&
      !assign_lsm_edit_global_seqnos_) {
    assert(seqno == 0);
    assert(file_to_ingest->original_seqno == 0);
  }
  if (file_to_ingest->original_seqno != seqno) {
    if (!ingestion_options_.allow_global_seqno &&
        !assign_lsm_edit_global_seqnos_) {
      return Status::InvalidArgument("Global seqno is required, but disabled");
    }
    if (file_to_ingest->original_seqno != 0) {
      return Status::InvalidArgument(
          "External file global seqno does not match assigned seqno");
    }
  }

  file_to_ingest->assigned_seqno = seqno;
  return Status::OK();
}

bool ExternalSstFileIngestionJob::IngestedFileFitInLevel(
    const IngestedFileInfo* file_to_ingest, int level) {
  if (level == 0) {
    // Files can always fit in L0
    return true;
  }

  auto* vstorage = cfd_->current()->storage_info();
  Slice file_smallest_user_key(file_to_ingest->start_ukey);
  Slice file_largest_user_key(file_to_ingest->limit_ukey);

  if (vstorage->OverlapInLevel(level, &file_smallest_user_key,
                               &file_largest_user_key)) {
    // File overlap with another files in this level, we cannot
    // add it to this level
    return false;
  }

  // File did not overlap with level files, nor compaction output
  return true;
}

Status ExternalSstFileIngestionJob::GetSeqnoBoundaryForFile(
    TableReader* table_reader, SuperVersion* sv,
    IngestedFileInfo* file_to_ingest, bool allow_data_in_errors) {
  const auto tp = table_reader->GetTableProperties();
  const bool has_largest_seqno = tp->HasKeyLargestSeqno();
  SequenceNumber largest_seqno = tp->key_largest_seqno;
  if (has_largest_seqno) {
    file_to_ingest->largest_seqno = largest_seqno;
    if (largest_seqno == 0) {
      file_to_ingest->smallest_seqno = 0;
      return Status::OK();
    }
    if (tp->HasKeySmallestSeqno()) {
      file_to_ingest->smallest_seqno = tp->key_smallest_seqno;
      return Status::OK();
    }
  }

  // For older SST files they may not be recorded in table properties, so
  // we scan the file to find out.
  TEST_SYNC_POINT(
      "ExternalSstFileIngestionJob::GetSeqnoBoundaryForFile:FileScan");
  SequenceNumber smallest_seqno = kMaxSequenceNumber;
  SequenceNumber largest_seqno_from_iter = 0;
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ro, sv->mutable_cf_options.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));
  ParsedInternalKey key;
  iter->SeekToFirst();
  while (iter->Valid()) {
    Status pik_status =
        ParseInternalKey(iter->key(), &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    smallest_seqno = std::min(smallest_seqno, key.sequence);
    largest_seqno_from_iter = std::max(largest_seqno_from_iter, key.sequence);
    iter->Next();
  }
  if (!iter->status().ok()) {
    return iter->status();
  }

  if (table_reader->GetTableProperties()->num_range_deletions > 0) {
    std::unique_ptr<InternalIterator> range_del_iter(
        table_reader->NewRangeTombstoneIterator(ro));
    if (range_del_iter != nullptr) {
      for (range_del_iter->SeekToFirst(); range_del_iter->Valid();
           range_del_iter->Next()) {
        Status pik_status =
            ParseInternalKey(range_del_iter->key(), &key, allow_data_in_errors);
        if (!pik_status.ok()) {
          return Status::Corruption("Corrupted key in external file. ",
                                    pik_status.getState());
        }
        smallest_seqno = std::min(smallest_seqno, key.sequence);
        largest_seqno_from_iter =
            std::max(largest_seqno_from_iter, key.sequence);
      }
      if (!range_del_iter->status().ok()) {
        return range_del_iter->status();
      }
    }
  }

  file_to_ingest->smallest_seqno = smallest_seqno;
  if (!has_largest_seqno) {
    file_to_ingest->largest_seqno = largest_seqno_from_iter;
  } else {
    assert(largest_seqno == largest_seqno_from_iter);
    file_to_ingest->largest_seqno = largest_seqno;
  }

  if (file_to_ingest->largest_seqno == kMaxSequenceNumber) {
    return Status::InvalidArgument(
        "Unknown smallest seqno for db generated file.");
  }
  if (file_to_ingest->smallest_seqno == kMaxSequenceNumber) {
    return Status::InvalidArgument(
        "Unknown largest seqno for db generated file.");
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
