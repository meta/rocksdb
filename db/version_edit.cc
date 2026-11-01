//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/blob/blob_index.h"
#include "db/version_set.h"
#include "db/wide/wide_column_serialization.h"
#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {}  // anonymous namespace

// After copying, we rely on the caller to ensure no double releases. Fragile,
// but keeps copying cheap.
PinnedTableReader& PinnedTableReader::operator=(
    const PinnedTableReader& other) {
  TableReader* r = other.reader_.load(std::memory_order_acquire);
  // Only read handle_ when reader_ is non-null. Pin() writes handle_ before
  // reader_ (with release), so a non-null reader_ guarantees handle_ is stable.
  // If reader_ is null, Pin() may be in progress -- avoid reading handle_.
  handle_ = (r != nullptr) ? other.handle_ : nullptr;
  reader_.store(r, std::memory_order_release);
  return *this;
}

Cache::Handle* PinnedTableReader::GetCacheHandle() const {
  (void)reader_.load(std::memory_order_acquire);
  return handle_;
}

void PinnedTableReader::Pin(Cache::Handle* handle, TableReader* reader) {
  assert(handle != nullptr);
  assert(reader != nullptr);
  handle_ = handle;
  reader_.store(reader, std::memory_order_release);
}

void PinnedTableReader::Release(Cache* cache) {
  (void)reader_.load(std::memory_order_acquire);
  if (handle_ != nullptr) {
    cache->Release(handle_);
    handle_ = nullptr;
    reader_.store(nullptr, std::memory_order_relaxed);
  }
}

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id) {
  assert(number <= kFileNumberMask);
  return number | (path_id * (kFileNumberMask + 1));
}

Status FileMetaData::UpdateBoundaries(const Slice& key, const Slice& value,
                                      SequenceNumber seqno,
                                      ValueType value_type) {
  // Helper: update oldest_blob_file_number from a single BlobIndex.
  auto update_oldest_blob = [&](const BlobIndex& blob_index) -> Status {
    if (!blob_index.IsInlined() && !blob_index.HasTTL()) {
      // INTEGRITY TRIPWIRE -- do not weaken. This is generic file metadata on
      // the table-build output path (flush / compaction / recovery, via
      // BuildTable and CompactionOutputs), which must only ever see external
      // blob-file references. file_number 0 is kInvalidBlobFileNumber, which is
      // deliberately also the same-file ("embedded") sentinel
      // kCurrentFileBlobIndexFileNumber (see blob_constants.h). Reaching here
      // with 0 means one of:
      //   * a same-file/embedded reference leaked into the output stream
      //     unresolved -- e.g. EmbeddedBlobResolvingIterator failed to resolve
      //     and fell back to the raw value on a blob-region read error
      //     (T277566778); or
      //   * an uninitialized/corrupt BlobIndex (default file number never set).
      // Rejecting it flags the bug closer to its root cause. Do NOT relax this
      // to skip same-file refs (e.g. via BlobIndex::IsSameFile()): that would
      // mask real corruption AND could persist unresolvable same-file
      // references into ordinary (non-embedded) SSTs, turning a transient error
      // into permanent data loss.
      if (blob_index.file_number() == kInvalidBlobFileNumber) {
        return Status::Corruption("Invalid blob file number");
      }

      if (oldest_blob_file_number == kInvalidBlobFileNumber ||
          oldest_blob_file_number > blob_index.file_number()) {
        oldest_blob_file_number = blob_index.file_number();
      }
    }
    return Status::OK();
  };

  if (value_type == kTypeBlobIndex) {
    BlobIndex blob_index;
    if (Status s = blob_index.DecodeFrom(value); !s.ok()) {
      return s;
    }

    if (Status s = update_oldest_blob(blob_index); !s.ok()) {
      return s;
    }
  } else if (value_type == kTypeWideColumnEntity) {
    if (Status s = WideColumnSerialization::ForEachBlobFileNumber(
            value, update_oldest_blob);
        !s.ok()) {
      return s;
    }
  }

  if (smallest.size() == 0) {
    smallest.DecodeFrom(key);
  }
  largest.DecodeFrom(key);
  fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
  fd.largest_seqno = std::max(fd.largest_seqno, seqno);

  return Status::OK();
}

void VersionEdit::Clear() { *this = VersionEdit(); }

bool VersionEdit::ShouldEmitPerColumnFamilyRecoveryEdit(
    uint64_t current_log_number) const {
  return (HasLogNumber() && GetLogNumber() > current_log_number) ||
         NumEntries() > 0 || HasComparatorName() || HasPrevLogNumber() ||
         HasNextFile() || HasMaxColumnFamily() || HasMinLogNumberToKeep() ||
         HasLastSequence() || !GetCompactCursors().empty() || HasDbId() ||
         IsColumnFamilyManipulation() || IsInAtomicGroup() ||
         HasFullHistoryTsLow() || HasPersistUserDefinedTimestamps() ||
         HasSubcompactionProgress() || HasCommittedOptionsFileNumber();
}

bool VersionEdit::EncodeTo(std::string* dst,
                           std::optional<size_t> ts_sz) const {
  assert(!IsNoManifestWriteDummy());
  if (has_db_id_) {
    PutVarint32(dst, kDbId);
    PutLengthPrefixedSlice(dst, db_id_);
  }
  if (has_comparator_) {
    assert(has_persist_user_defined_timestamps_);
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32Varint64(dst, kLogNumber, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32Varint64(dst, kPrevLogNumber, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32Varint64(dst, kNextFileNumber, next_file_number_);
  }
  if (has_max_column_family_) {
    PutVarint32(dst, kMaxColumnFamily, max_column_family_);
  }
  if (has_min_log_number_to_keep_) {
    PutVarint32Varint64(dst, kMinLogNumberToKeep, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    PutVarint32Varint64(dst, kLastSequence, last_sequence_);
  }
  for (size_t i = 0; i < compact_cursors_.size(); i++) {
    if (compact_cursors_[i].second.Valid()) {
      PutVarint32(dst, kCompactCursor);
      PutVarint32(dst, compact_cursors_[i].first);  // level
      PutLengthPrefixedSlice(dst, compact_cursors_[i].second.Encode());
    }
  }
  for (const auto& deleted : deleted_files_) {
    PutVarint32Varint32Varint64(dst, kDeletedFile, deleted.first /* level */,
                                deleted.second /* file number */);
  }

  bool min_log_num_written = false;

  assert(new_files_.empty() || ts_sz.has_value());
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    if (!f.smallest.Valid() || !f.largest.Valid() ||
        f.epoch_number == kUnknownEpochNumber) {
      return false;
    }
    EncodeToNewFile4(f, new_files_[i].first, ts_sz.value(),
                     has_min_log_number_to_keep_, min_log_number_to_keep_,
                     min_log_num_written, dst);
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    PutVarint32(dst, kBlobFileAddition);
    blob_file_addition.EncodeTo(dst);
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    PutVarint32(dst, kBlobFileGarbage);
    blob_file_garbage.EncodeTo(dst);
  }

  for (const auto& wal_addition : wal_additions_) {
    PutVarint32(dst, kWalAddition2);
    std::string encoded;
    wal_addition.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  if (!wal_deletion_.IsEmpty()) {
    PutVarint32(dst, kWalDeletion2);
    std::string encoded;
    wal_deletion_.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  // 0 is default and does not need to be explicitly written
  if (column_family_ != 0) {
    PutVarint32(dst, kColumnFamily, column_family_);
  }

  if (is_column_family_add_) {
    PutVarint32(dst, kColumnFamilyAdd);
    PutLengthPrefixedSlice(dst, Slice(column_family_name_));
  }

  if (is_column_family_drop_) {
    PutVarint32(dst, kColumnFamilyDrop);
  }

  if (is_in_atomic_group_) {
    PutVarint32(dst, kInAtomicGroup);
    PutVarint32(dst, remaining_entries_);
  }

  if (HasFullHistoryTsLow()) {
    PutVarint32(dst, kFullHistoryTsLow);
    PutLengthPrefixedSlice(dst, full_history_ts_low_);
  }

  if (HasPersistUserDefinedTimestamps()) {
    // persist_user_defined_timestamps flag should be logged in the same
    // VersionEdit as the user comparator name.
    assert(has_comparator_);
    PutVarint32(dst, kPersistUserDefinedTimestamps);
    char p = static_cast<char>(persist_user_defined_timestamps_);
    PutLengthPrefixedSlice(dst, Slice(&p, 1));
  }

  if (HasSubcompactionProgress()) {
    PutVarint32(dst, kSubcompactionProgress);
    std::string progress_data;
    subcompaction_progress_.EncodeTo(&progress_data);
    PutLengthPrefixedSlice(dst, progress_data);
  }

  if (has_last_compacted_manifest_file_size_) {
    PutVarint32(dst, kLastCompactedManifestFileSize);
    std::string varint_size;
    PutVarint64(&varint_size, last_compacted_manifest_file_size_);
    PutLengthPrefixedSlice(dst, Slice(varint_size));
  }

  if (has_committed_options_file_number_) {
    PutVarint32(dst, kCommittedOptionsFileNumber);
    std::string encoded;
    PutVarint64(&encoded, committed_options_file_number_);
    PutLengthPrefixedSlice(dst, encoded);
  }

  return true;
}

void VersionEdit::EncodeToNewFile4(const FileMetaData& f, int level,
                                   size_t ts_sz,
                                   bool has_min_log_number_to_keep,
                                   uint64_t min_log_number_to_keep,
                                   bool& min_log_num_written,
                                   std::string* dst) {
  PutVarint32(dst, kNewFile4);
  PutVarint32Varint64(dst, level, f.fd.GetNumber());
  PutVarint64(dst, f.fd.GetFileSize());
  EncodeFileBoundaries(dst, f, ts_sz);
  PutVarint64Varint64(dst, f.fd.smallest_seqno, f.fd.largest_seqno);
  // Customized fields' format:
  // +-----------------------------+
  // | 1st field's tag (varint32)  |
  // +-----------------------------+
  // | 1st field's size (varint32) |
  // +-----------------------------+
  // |    bytes for 1st field      |
  // |  (based on size decoded)    |
  // +-----------------------------+
  // |                             |
  // |          ......             |
  // |                             |
  // +-----------------------------+
  // | last field's size (varint32)|
  // +-----------------------------+
  // |    bytes for last field     |
  // |  (based on size decoded)    |
  // +-----------------------------+
  // | terminating tag (varint32)  |
  // +-----------------------------+
  //
  // Customized encoding for fields:
  //   tag kPathId: 1 byte as path_id
  //   tag kNeedCompaction:
  //        now only can take one char value 1 indicating need-compaction
  //
  PutVarint32(dst, NewFileCustomTag::kOldestAncestorTime);
  std::string varint_oldest_ancestor_time;
  PutVarint64(&varint_oldest_ancestor_time, f.oldest_ancestor_time);
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintOldestAncestorTime",
                           &varint_oldest_ancestor_time);
  PutLengthPrefixedSlice(dst, Slice(varint_oldest_ancestor_time));

  PutVarint32(dst, NewFileCustomTag::kFileCreationTime);
  std::string varint_file_creation_time;
  PutVarint64(&varint_file_creation_time, f.file_creation_time);
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintFileCreationTime",
                           &varint_file_creation_time);
  PutLengthPrefixedSlice(dst, Slice(varint_file_creation_time));

  PutVarint32(dst, NewFileCustomTag::kEpochNumber);
  std::string varint_epoch_number;
  PutVarint64(&varint_epoch_number, f.epoch_number);
  PutLengthPrefixedSlice(dst, Slice(varint_epoch_number));

  if (f.file_checksum_func_name != kUnknownFileChecksumFuncName) {
    PutVarint32(dst, NewFileCustomTag::kFileChecksum);
    PutLengthPrefixedSlice(dst, Slice(f.file_checksum));

    PutVarint32(dst, NewFileCustomTag::kFileChecksumFuncName);
    PutLengthPrefixedSlice(dst, Slice(f.file_checksum_func_name));
  }

  if (f.fd.GetPathId() != 0) {
    PutVarint32(dst, NewFileCustomTag::kPathId);
    char p = static_cast<char>(f.fd.GetPathId());
    PutLengthPrefixedSlice(dst, Slice(&p, 1));
  }
  if (f.temperature != Temperature::kUnknown) {
    PutVarint32(dst, NewFileCustomTag::kTemperature);
    char p = static_cast<char>(f.temperature);
    PutLengthPrefixedSlice(dst, Slice(&p, 1));
  }
  if (f.marked_for_compaction) {
    PutVarint32(dst, NewFileCustomTag::kNeedCompaction);
    char p = static_cast<char>(1);
    PutLengthPrefixedSlice(dst, Slice(&p, 1));
  }
  if (has_min_log_number_to_keep && !min_log_num_written) {
    PutVarint32(dst, NewFileCustomTag::kMinLogNumberToKeepHack);
    std::string varint_log_number;
    PutFixed64(&varint_log_number, min_log_number_to_keep);
    PutLengthPrefixedSlice(dst, Slice(varint_log_number));
    min_log_num_written = true;
  }
  if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
    PutVarint32(dst, NewFileCustomTag::kOldestBlobFileNumber);
    std::string oldest_blob_file_number;
    PutVarint64(&oldest_blob_file_number, f.oldest_blob_file_number);
    PutLengthPrefixedSlice(dst, Slice(oldest_blob_file_number));
  }
  UniqueId64x2 unique_id = f.unique_id;
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:UniqueId", &unique_id);
  if (unique_id != kNullUniqueId64x2) {
    PutVarint32(dst, NewFileCustomTag::kUniqueId);
    std::string unique_id_str = EncodeUniqueIdBytes(&unique_id);
    PutLengthPrefixedSlice(dst, Slice(unique_id_str));
  }
  if (f.compensated_range_deletion_size) {
    PutVarint32(dst, NewFileCustomTag::kCompensatedRangeDeletionSize);
    std::string compensated_range_deletion_size;
    PutVarint64(&compensated_range_deletion_size,
                f.compensated_range_deletion_size);
    PutLengthPrefixedSlice(dst, Slice(compensated_range_deletion_size));
  }
  if (f.tail_size) {
    PutVarint32(dst, NewFileCustomTag::kTailSize);
    std::string varint_tail_size;
    PutVarint64(&varint_tail_size, f.tail_size);
    PutLengthPrefixedSlice(dst, Slice(varint_tail_size));
  }
  if (!f.user_defined_timestamps_persisted) {
    // The default value for the flag is true, it's only explicitly persisted
    // when it's false. We are putting 0 as the value here to signal false
    // (i.e. UDTS not persisted).
    PutVarint32(dst, NewFileCustomTag::kUserDefinedTimestampsPersisted);
    char p = static_cast<char>(0);
    PutLengthPrefixedSlice(dst, Slice(&p, 1));
  }
  // Encode min/max timestamp if they are non-empty
  if (!f.min_timestamp.empty()) {
    PutVarint32(dst, NewFileCustomTag::kMinTimestamp);
    PutLengthPrefixedSlice(dst, Slice(f.min_timestamp));
  }
  if (!f.max_timestamp.empty()) {
    PutVarint32(dst, NewFileCustomTag::kMaxTimestamp);
    PutLengthPrefixedSlice(dst, Slice(f.max_timestamp));
  }
  if (!f.file_open_metadata.empty()) {
    PutVarint32(dst, NewFileCustomTag::kFileOpenMetadata);
    PutLengthPrefixedSlice(dst, Slice(f.file_open_metadata));
  }
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:NewFile4:CustomizeFields",
                           dst);

  PutVarint32(dst, NewFileCustomTag::kTerminate);
}
static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return dst->Valid();
  } else {
    return false;
  }
}

bool VersionEdit::GetLevel(Slice* input, int* level, int& max_level) {
  uint32_t v = 0;
  if (GetVarint32(input, &v)) {
    *level = v;
    if (max_level < *level) {
      max_level = *level;
    }
    return true;
  } else {
    return false;
  }
}

const char* VersionEdit::DecodeNewFile4From(Slice* input, int& max_level,
                                            uint64_t& min_log_number_to_keep,
                                            bool& has_min_log_number_to_keep,
                                            NewFiles& new_files,
                                            FileMetaData& f) {
  int level = 0;
  uint64_t number = 0;
  uint32_t path_id = 0;
  uint64_t file_size = 0;
  SequenceNumber smallest_seqno = 0;
  SequenceNumber largest_seqno = kMaxSequenceNumber;
  if (GetLevel(input, &level, max_level) && GetVarint64(input, &number) &&
      GetVarint64(input, &file_size) && GetInternalKey(input, &f.smallest) &&
      GetInternalKey(input, &f.largest) &&
      GetVarint64(input, &smallest_seqno) &&
      GetVarint64(input, &largest_seqno)) {
    // See comments in VersionEdit::EncodeTo() for format of customized fields
    while (true) {
      uint32_t custom_tag = 0;
      Slice field;
      if (!GetVarint32(input, &custom_tag)) {
        return "new-file4 custom field";
      }
      if (custom_tag == kTerminate) {
        break;
      }
      if (!GetLengthPrefixedSlice(input, &field)) {
        return "new-file4 custom field length prefixed slice error";
      }
      switch (custom_tag) {
        case kPathId:
          if (field.size() != 1) {
            return "path_id field wrong size";
          }
          path_id = field[0];
          if (path_id > 3) {
            return "path_id wrong vaue";
          }
          break;
        case kOldestAncestorTime:
          if (!GetVarint64(&field, &f.oldest_ancestor_time)) {
            return "invalid oldest ancestor time";
          }
          break;
        case kFileCreationTime:
          if (!GetVarint64(&field, &f.file_creation_time)) {
            return "invalid file creation time";
          }
          break;
        case kEpochNumber:
          if (!GetVarint64(&field, &f.epoch_number)) {
            return "invalid epoch number";
          }
          break;
        case kFileChecksum:
          f.file_checksum = field.ToString();
          break;
        case kFileChecksumFuncName:
          f.file_checksum_func_name = field.ToString();
          break;
        case kNeedCompaction:
          if (field.size() != 1) {
            return "need_compaction field wrong size";
          }
          f.marked_for_compaction = (field[0] == 1);
          break;
        case kMinLogNumberToKeepHack:
          // This is a hack to encode kMinLogNumberToKeep in a
          // forward-compatible fashion.
          if (!GetFixed64(&field, &min_log_number_to_keep)) {
            return "deleted log number malformatted";
          }
          has_min_log_number_to_keep = true;
          break;
        case kOldestBlobFileNumber:
          if (!GetVarint64(&field, &f.oldest_blob_file_number)) {
            return "invalid oldest blob file number";
          }
          break;
        case kTemperature:
          if (field.size() != 1) {
            return "temperature field wrong size";
          } else {
            Temperature casted_field = static_cast<Temperature>(field[0]);
            if (casted_field < Temperature::kLastTemperature) {
              f.temperature = casted_field;
            }
          }
          break;
        case kUniqueId:
          if (!DecodeUniqueIdBytes(field.ToString(), &f.unique_id).ok()) {
            f.unique_id = kNullUniqueId64x2;
            return "invalid unique id";
          }
          break;
        case kCompensatedRangeDeletionSize:
          if (!GetVarint64(&field, &f.compensated_range_deletion_size)) {
            return "Invalid compensated range deletion size";
          }
          break;
        case kTailSize:
          if (!GetVarint64(&field, &f.tail_size)) {
            return "invalid tail start offset";
          }
          break;
        case kUserDefinedTimestampsPersisted:
          if (field.size() != 1) {
            return "user-defined timestamps persisted field wrong size";
          }
          f.user_defined_timestamps_persisted = (field[0] == 1);
          break;
        case kMinTimestamp:
          f.min_timestamp = field.ToString();
          break;
        case kMaxTimestamp:
          f.max_timestamp = field.ToString();
          break;
        case kFileOpenMetadata:
          f.file_open_metadata = field.ToString();
          break;
        default:
          if ((custom_tag & kCustomTagNonSafeIgnoreMask) != 0) {
            // Should not proceed if cannot understand it
            return "new-file4 custom field not supported";
          }
          break;
      }
    }
  } else {
    return "new-file4 entry";
  }
  f.fd =
      FileDescriptor(number, path_id, file_size, smallest_seqno, largest_seqno);
  new_files.emplace_back(level, f);
  return nullptr;
}

void VersionEdit::EncodeFileBoundaries(std::string* dst,
                                       const FileMetaData& meta, size_t ts_sz) {
  if (ts_sz == 0 || meta.user_defined_timestamps_persisted) {
    PutLengthPrefixedSlice(dst, meta.smallest.Encode());
    PutLengthPrefixedSlice(dst, meta.largest.Encode());
    return;
  }
  std::string smallest_buf;
  std::string largest_buf;
  StripTimestampFromInternalKey(&smallest_buf, meta.smallest.Encode(), ts_sz);
  StripTimestampFromInternalKey(&largest_buf, meta.largest.Encode(), ts_sz);
  PutLengthPrefixedSlice(dst, smallest_buf);
  PutLengthPrefixedSlice(dst, largest_buf);
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
#ifndef NDEBUG
  bool ignore_ignorable_tags = false;
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:IgnoreIgnorableTags",
                           &ignore_ignorable_tags);
#endif
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag = 0;

  // Temporary storage for parsing
  int level = 0;
  FileMetaData f;
  Slice str;
  InternalKey key;
  while (msg == nullptr && GetVarint32(&input, &tag)) {
#ifndef NDEBUG
    if (ignore_ignorable_tags && tag > kTagSafeIgnoreMask) {
      tag = kTagSafeIgnoreMask;
    }
#endif
    switch (tag) {
      case kDbId:
        if (GetLengthPrefixedSlice(&input, &str)) {
          db_id_ = str.ToString();
          has_db_id_ = true;
        } else {
          msg = "db id";
        }
        break;
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kMaxColumnFamily:
        if (GetVarint32(&input, &max_column_family_)) {
          has_max_column_family_ = true;
        } else {
          msg = "max column family";
        }
        break;

      case kMinLogNumberToKeep:
        if (GetVarint64(&input, &min_log_number_to_keep_)) {
          has_min_log_number_to_keep_ = true;
        } else {
          msg = "min log number to kee";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactCursor:
        if (GetLevel(&input, &level, max_level_) &&
            GetInternalKey(&input, &key)) {
          // Here we re-use the output format of compact pointer in LevelDB
          // to persist compact_cursors_
          compact_cursors_.push_back(std::make_pair(level, key));
        } else {
          if (!msg) {
            msg = "compaction cursor";
          }
        }
        break;

      case kDeletedFile: {
        uint64_t number = 0;
        if (GetLevel(&input, &level, max_level_) &&
            GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          if (!msg) {
            msg = "deleted file";
          }
        }
        break;
      }

      case kNewFile: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        if (GetLevel(&input, &level, max_level_) &&
            GetVarint64(&input, &number) && GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          f.fd = FileDescriptor(number, 0, file_size);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file entry";
          }
        }
        break;
      }
      case kNewFile2: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, max_level_) &&
            GetVarint64(&input, &number) && GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, 0, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file2 entry";
          }
        }
        break;
      }

      case kNewFile3: {
        uint64_t number = 0;
        uint32_t path_id = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, max_level_) &&
            GetVarint64(&input, &number) && GetVarint32(&input, &path_id) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, path_id, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file3 entry";
          }
        }
        break;
      }

      case kNewFile4: {
        FileMetaData ignored_file;
        msg = DecodeNewFile4From(&input, max_level_, min_log_number_to_keep_,
                                 has_min_log_number_to_keep_, new_files_,
                                 ignored_file);
        break;
      }

      case kBlobFileAddition:
      case kBlobFileAddition_DEPRECATED: {
        BlobFileAddition blob_file_addition;
        const Status s = blob_file_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFile(std::move(blob_file_addition));
        break;
      }

      case kBlobFileGarbage:
      case kBlobFileGarbage_DEPRECATED: {
        BlobFileGarbage blob_file_garbage;
        const Status s = blob_file_garbage.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFileGarbage(std::move(blob_file_garbage));
        break;
      }

      case kWalAddition: {
        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalAddition2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalAddition not prefixed by length";
          break;
        }

        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalDeletion: {
        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kWalDeletion2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalDeletion not prefixed by length";
          break;
        }

        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kColumnFamily:
        if (!GetVarint32(&input, &column_family_)) {
          if (!msg) {
            msg = "set column family id";
          }
        }
        break;

      case kColumnFamilyAdd:
        if (GetLengthPrefixedSlice(&input, &str)) {
          is_column_family_add_ = true;
          column_family_name_ = str.ToString();
        } else {
          if (!msg) {
            msg = "column family add";
          }
        }
        break;

      case kColumnFamilyDrop:
        is_column_family_drop_ = true;
        break;

      case kInAtomicGroup:
        is_in_atomic_group_ = true;
        if (!GetVarint32(&input, &remaining_entries_)) {
          if (!msg) {
            msg = "remaining entries";
          }
        }
        break;

      case kFullHistoryTsLow:
        if (!GetLengthPrefixedSlice(&input, &str)) {
          msg = "full_history_ts_low";
        } else if (str.empty()) {
          msg = "full_history_ts_low: empty";
        } else {
          full_history_ts_low_.assign(str.data(), str.size());
        }
        break;

      case kPersistUserDefinedTimestamps:
        if (!GetLengthPrefixedSlice(&input, &str)) {
          msg = "persist_user_defined_timestamps";
        } else if (str.size() != 1) {
          msg = "persist_user_defined_timestamps field wrong size";
        } else {
          persist_user_defined_timestamps_ = (str[0] == 1);
          has_persist_user_defined_timestamps_ = true;
        }
        break;

      case kSubcompactionProgress: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "SubcompactionProgress not prefixed by length";
          break;
        }

        SubcompactionProgress progress;
        Status s = progress.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        SetSubcompactionProgress(progress);
        break;
      }

      case kLastCompactedManifestFileSize: {
        Slice encoded;
        if (GetLengthPrefixedSlice(&input, &encoded) &&
            GetVarint64(&encoded, &last_compacted_manifest_file_size_)) {
          has_last_compacted_manifest_file_size_ = true;
        } else {
          msg = "last compacted manifest file size";
        }
        break;
      }

      case kCommittedOptionsFileNumber: {
        Slice encoded;
        if (GetLengthPrefixedSlice(&input, &encoded) &&
            GetVarint64(&encoded, &committed_options_file_number_) &&
            encoded.empty()) {
          has_committed_options_file_number_ = true;
        } else {
          msg = "committed options file number";
        }
        break;
      }

      default:
        if (tag & kTagSafeIgnoreMask) {
          // Tag from future which can be safely ignored.
          // The next field must be the length of the entry.
          uint32_t field_len;
          if (!GetVarint32(&input, &field_len) ||
              static_cast<size_t>(field_len) > input.size()) {
            if (!msg) {
              msg = "safely ignoreable tag length error";
            }
          } else {
            input.remove_prefix(static_cast<size_t>(field_len));
          }
        } else {
          msg = "unknown tag";
        }
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString(bool hex_key) const {
  std::string r;
  r.append("VersionEdit {");
  if (has_db_id_) {
    r.append("\n  DB ID: ");
    r.append(db_id_);
  }
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_persist_user_defined_timestamps_) {
    r.append("\n  PersistUserDefinedTimestamps: ");
    r.append(persist_user_defined_timestamps_ ? "true" : "false");
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFileNumber: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_max_column_family_) {
    r.append("\n  MaxColumnFamily: ");
    AppendNumberTo(&r, max_column_family_);
  }
  if (has_min_log_number_to_keep_) {
    r.append("\n  MinLogNumberToKeep: ");
    AppendNumberTo(&r, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (const auto& level_and_compact_cursor : compact_cursors_) {
    r.append("\n  CompactCursor: ");
    AppendNumberTo(&r, level_and_compact_cursor.first);
    r.append(" ");
    r.append(level_and_compact_cursor.second.DebugString(hex_key));
  }
  for (const auto& deleted_file : deleted_files_) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, deleted_file.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_file.second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetNumber());
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetFileSize());
    r.append(" ");
    r.append(f.smallest.DebugString(hex_key));
    r.append(" .. ");
    r.append(f.largest.DebugString(hex_key));
    if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
      r.append(" blob_file:");
      AppendNumberTo(&r, f.oldest_blob_file_number);
    }
    // Misspelled label kept for compatibility with tools parsing this output
    r.append(" oldest_ancester_time:");
    AppendNumberTo(&r, f.oldest_ancestor_time);
    r.append(" file_creation_time:");
    AppendNumberTo(&r, f.file_creation_time);
    r.append(" epoch_number:");
    AppendNumberTo(&r, f.epoch_number);
    r.append(" file_checksum:");
    r.append(Slice(f.file_checksum).ToString(true));
    r.append(" file_checksum_func_name: ");
    r.append(f.file_checksum_func_name);
    if (f.temperature != Temperature::kUnknown) {
      r.append(" temperature: ");
      // Maybe change to human readable format whenthe feature becomes
      // permanent
      r.append(std::to_string(static_cast<int>(f.temperature)));
    }
    if (f.unique_id != kNullUniqueId64x2) {
      r.append(" unique_id(internal): ");
      UniqueId64x2 id = f.unique_id;
      r.append(InternalUniqueIdToHumanString(&id));
      r.append(" public_unique_id: ");
      InternalUniqueIdToExternal(&id);
      r.append(UniqueIdToHumanString(EncodeUniqueIdBytes(&id)));
    }
    r.append(" tail size: ");
    AppendNumberTo(&r, f.tail_size);
    r.append(" User-defined timestamps persisted: ");
    r.append(f.user_defined_timestamps_persisted ? "true" : "false");
    if (!f.file_open_metadata.empty()) {
      r.append(" file_open_metadata_size: ");
      AppendNumberTo(&r, f.file_open_metadata.size());
    }
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    r.append("\n  BlobFileAddition: ");
    r.append(blob_file_addition.DebugString());
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    r.append("\n  BlobFileGarbage: ");
    r.append(blob_file_garbage.DebugString());
  }

  for (const auto& wal_addition : wal_additions_) {
    r.append("\n  WalAddition: ");
    r.append(wal_addition.DebugString());
  }

  if (!wal_deletion_.IsEmpty()) {
    r.append("\n  WalDeletion: ");
    r.append(wal_deletion_.DebugString());
  }

  r.append("\n  ColumnFamily: ");
  AppendNumberTo(&r, column_family_);
  if (is_column_family_add_) {
    r.append("\n  ColumnFamilyAdd: ");
    r.append(column_family_name_);
  }
  if (is_column_family_drop_) {
    r.append("\n  ColumnFamilyDrop");
  }
  if (is_in_atomic_group_) {
    r.append("\n  AtomicGroup: ");
    AppendNumberTo(&r, remaining_entries_);
    r.append(" entries remains");
  }
  if (HasFullHistoryTsLow()) {
    r.append("\n FullHistoryTsLow: ");
    r.append(Slice(full_history_ts_low_).ToString(hex_key));
  }
  if (HasSubcompactionProgress()) {
    r.append("\n SubcompactionProgress: ");
    r.append(subcompaction_progress_.ToString());
  }
  if (has_last_compacted_manifest_file_size_) {
    r.append("\n  LastCompactedManifestFileSize: ");
    AppendNumberTo(&r, last_compacted_manifest_file_size_);
  }
  if (has_committed_options_file_number_) {
    r.append("\n  CommittedOptionsFileNumber: ");
    AppendNumberTo(&r, committed_options_file_number_);
  }
  r.append("\n}\n");
  return r;
}

std::string VersionEdit::DebugJSON(int edit_num, bool hex_key) const {
  JSONWriter jw;
  jw << "EditNumber" << edit_num;

  if (has_db_id_) {
    jw << "DB ID" << db_id_;
  }
  if (has_comparator_) {
    jw << "Comparator" << comparator_;
  }
  if (has_log_number_) {
    jw << "LogNumber" << log_number_;
  }
  if (has_prev_log_number_) {
    jw << "PrevLogNumber" << prev_log_number_;
  }
  if (has_next_file_number_) {
    jw << "NextFileNumber" << next_file_number_;
  }
  if (has_max_column_family_) {
    jw << "MaxColumnFamily" << max_column_family_;
  }
  if (has_min_log_number_to_keep_) {
    jw << "MinLogNumberToKeep" << min_log_number_to_keep_;
  }
  if (has_last_sequence_) {
    jw << "LastSeq" << last_sequence_;
  }

  if (!deleted_files_.empty()) {
    jw << "DeletedFiles";
    jw.StartArray();

    for (const auto& deleted_file : deleted_files_) {
      jw.StartArrayedObject();
      jw << "Level" << deleted_file.first;
      jw << "FileNumber" << deleted_file.second;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!new_files_.empty()) {
    jw << "AddedFiles";
    jw.StartArray();

    for (size_t i = 0; i < new_files_.size(); i++) {
      jw.StartArrayedObject();
      jw << "Level" << new_files_[i].first;
      const FileMetaData& f = new_files_[i].second;
      jw << "FileNumber" << f.fd.GetNumber();
      jw << "FileSize" << f.fd.GetFileSize();
      jw << "SmallestIKey" << f.smallest.DebugString(hex_key);
      jw << "LargestIKey" << f.largest.DebugString(hex_key);
      // Misspelled key kept for compatibility with tools parsing this output
      jw << "OldestAncesterTime" << f.oldest_ancestor_time;
      jw << "FileCreationTime" << f.file_creation_time;
      jw << "EpochNumber" << f.epoch_number;
      jw << "FileChecksum" << Slice(f.file_checksum).ToString(true);
      jw << "FileChecksumFuncName" << f.file_checksum_func_name;
      if (f.temperature != Temperature::kUnknown) {
        jw << "temperature" << std::to_string(static_cast<int>(f.temperature));
      }
      if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
        jw << "OldestBlobFile" << f.oldest_blob_file_number;
      }
      if (f.temperature != Temperature::kUnknown) {
        // Maybe change to human readable format whenthe feature becomes
        // permanent
        jw << "Temperature" << static_cast<int>(f.temperature);
      }
      jw << "TailSize" << f.tail_size;
      jw << "UserDefinedTimestampsPersisted"
         << f.user_defined_timestamps_persisted;
      if (!f.file_open_metadata.empty()) {
        jw << "FileOpenMetadataSize" << f.file_open_metadata.size();
      }
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_additions_.empty()) {
    jw << "BlobFileAdditions";

    jw.StartArray();

    for (const auto& blob_file_addition : blob_file_additions_) {
      jw.StartArrayedObject();
      jw << blob_file_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_garbages_.empty()) {
    jw << "BlobFileGarbages";

    jw.StartArray();

    for (const auto& blob_file_garbage : blob_file_garbages_) {
      jw.StartArrayedObject();
      jw << blob_file_garbage;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_additions_.empty()) {
    jw << "WalAdditions";

    jw.StartArray();

    for (const auto& wal_addition : wal_additions_) {
      jw.StartArrayedObject();
      jw << wal_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_deletion_.IsEmpty()) {
    jw << "WalDeletion";
    jw.StartObject();
    jw << wal_deletion_;
    jw.EndObject();
  }

  jw << "ColumnFamily" << column_family_;

  if (is_column_family_add_) {
    jw << "ColumnFamilyAdd" << column_family_name_;
  }
  if (is_column_family_drop_) {
    jw << "ColumnFamilyDrop" << column_family_name_;
  }
  if (is_in_atomic_group_) {
    jw << "AtomicGroup" << remaining_entries_;
  }

  if (HasFullHistoryTsLow()) {
    jw << "FullHistoryTsLow" << Slice(full_history_ts_low_).ToString(hex_key);
  }

  if (HasSubcompactionProgress()) {
    jw << "SubcompactionProgress" << subcompaction_progress_.ToString();
  }

  if (has_last_compacted_manifest_file_size_) {
    jw << "LastCompactedManifestFileSize" << last_compacted_manifest_file_size_;
  }

  if (has_committed_options_file_number_) {
    jw << "CommittedOptionsFileNumber" << committed_options_file_number_;
  }

  jw.EndObject();

  return jw.Get();
}

void SubcompactionProgressPerLevel::EncodeTo(std::string* dst) const {
  if (num_processed_output_records_ > 0) {
    PutVarint32(
        dst,
        SubcompactionProgressPerLevelCustomTag::kNumProcessedOutputRecords);
    std::string varint_records;
    PutVarint64(&varint_records, num_processed_output_records_);
    PutLengthPrefixedSlice(dst, varint_records);
  }

  if (!output_files_.empty()) {
    PutVarint32(dst, SubcompactionProgressPerLevelCustomTag::kOutputFilesDelta);
    std::string files_data;
    EncodeOutputFiles(&files_data);
    PutLengthPrefixedSlice(dst, files_data);
  }

  PutVarint32(dst, SubcompactionProgressPerLevelCustomTag::
                       kSubcompactionProgressPerLevelTerminate);
}

Status SubcompactionProgressPerLevel::DecodeFrom(Slice* input) {
  Clear();

  while (true) {
    uint32_t tag = 0;
    if (!GetVarint32(input, &tag)) {
      return Status::Corruption("SubcompactionProgressPerLevel", "tag error");
    }

    if (tag == SubcompactionProgressPerLevelCustomTag::
                   kSubcompactionProgressPerLevelTerminate) {
      break;
    }

    Slice field;
    if (!GetLengthPrefixedSlice(input, &field)) {
      return Status::Corruption("SubcompactionProgressPerLevel",
                                "field length prefixed slice error");
    }

    switch (tag) {
      case SubcompactionProgressPerLevelCustomTag::kNumProcessedOutputRecords: {
        if (!GetVarint64(&field, &num_processed_output_records_)) {
          return Status::Corruption("SubcompactionProgressPerLevel",
                                    "invalid num_processed_output_records_");
        }
        break;
      }

      case SubcompactionProgressPerLevelCustomTag::kOutputFilesDelta: {
        Status s = DecodeOutputFiles(&field, output_files_);
        if (!s.ok()) {
          return s;
        }
        break;
      }

      default:
        // Forward compatibility: Handle unknown tags
        if ((tag & SubcompactionProgressPerLevelCustomTag::
                       kSubcompactionProgressPerLevelCustomTagSafeIgnoreMask) !=
            0) {
          break;
        } else {
          return Status::NotSupported("SubcompactionProgress",
                                      "unsupported critical custom field");
        }
    }
  }

  return Status::OK();
}

void SubcompactionProgressPerLevel::EncodeOutputFiles(std::string* dst) const {
  size_t new_files_count =
      output_files_.size() > last_persisted_output_files_count_
          ? output_files_.size() - last_persisted_output_files_count_
          : 0;

  assert(new_files_count > 0);

  PutVarint32(dst, static_cast<uint32_t>(new_files_count));

  for (size_t i = last_persisted_output_files_count_; i < output_files_.size();
       ++i) {
    std::string file_dst;
    bool ignored_min_log_written = false;

    VersionEdit::EncodeToNewFile4(
        output_files_[i], -1 /* level */, 0 /* ts_sz */,
        false /* has_min_log_number_to_keep */, 0 /* min_log_number_to_keep */,
        ignored_min_log_written, &file_dst);

    PutLengthPrefixedSlice(dst, file_dst);
  }
}

Status SubcompactionProgressPerLevel::DecodeOutputFiles(
    Slice* input, autovector<FileMetaData>& output_files) {
  uint32_t new_file_count = 0;
  if (!GetVarint32(input, &new_file_count)) {
    return Status::Corruption("SubcompactionProgressPerLevel",
                              "new output file count");
  }

  assert(output_files.size() == 0);

  output_files.reserve(new_file_count);

  for (uint32_t i = 0; i < new_file_count; ++i) {
    Slice file_input;
    if (!GetLengthPrefixedSlice(input, &file_input)) {
      return Status::Corruption("SubcompactionProgressPerLevel",
                                "output file metadata");
    }

    uint32_t tag = 0;
    if (!GetVarint32(&file_input, &tag) || tag != kNewFile4) {
      return Status::Corruption("SubcompactionProgressPerLevel",
                                "expected kNewFile4 tag");
    }

    int ignored_max_level = -1;
    uint64_t ignored_min_log_number_to_keep = 0;
    bool ignored_has_min_log_number_to_keep = false;
    VersionEdit::NewFiles ignored_new_files;
    FileMetaData file;

    const char* err = VersionEdit::DecodeNewFile4From(
        &file_input, ignored_max_level, ignored_min_log_number_to_keep,
        ignored_has_min_log_number_to_keep, ignored_new_files, file);

    if (err != nullptr) {
      return Status::Corruption("SubcompactionProgressPerLevel", err);
    }

    output_files.push_back(std::move(file));
  }

  return Status::OK();
}

void SubcompactionProgress::EncodeTo(std::string* dst) const {
  if (!next_internal_key_to_compact.empty()) {
    PutVarint32(dst, SubcompactionProgressCustomTag::kNextInternalKeyToCompact);
    PutLengthPrefixedSlice(dst, next_internal_key_to_compact);
  }

  PutVarint32(dst, SubcompactionProgressCustomTag::kNumProcessedInputRecords);
  std::string varint_records;
  PutVarint64(&varint_records, num_processed_input_records);
  PutLengthPrefixedSlice(dst, varint_records);

  if (output_level_progress.GetOutputFiles().size() >
      output_level_progress.GetLastPersistedOutputFilesCount()) {
    PutVarint32(dst, SubcompactionProgressCustomTag::kOutputLevelProgress);
    std::string level_progress_data;
    output_level_progress.EncodeTo(&level_progress_data);
    PutLengthPrefixedSlice(dst, level_progress_data);
  }

  if (proximal_output_level_progress.GetOutputFiles().size() >
      proximal_output_level_progress.GetLastPersistedOutputFilesCount()) {
    PutVarint32(dst,
                SubcompactionProgressCustomTag::kProximalOutputLevelProgress);
    std::string level_progress_data;
    proximal_output_level_progress.EncodeTo(&level_progress_data);
    PutLengthPrefixedSlice(dst, level_progress_data);
  }

  if (has_blob_compaction_progress) {
    PutVarint32(dst, SubcompactionProgressCustomTag::kBlobCompactionProgress);
    std::string blob_progress_data;
    PutVarint64(&blob_progress_data, next_blob_file_number);

    const size_t first_addition = GetLastPersistedBlobFileAdditionsCount();
    assert(first_addition <= blob_file_additions.size());
    PutVarint64(&blob_progress_data,
                blob_file_additions.size() - first_addition);
    for (size_t i = first_addition; i < blob_file_additions.size(); ++i) {
      std::string encoded_addition;
      blob_file_additions[i].EncodeTo(&encoded_addition);
      PutLengthPrefixedSlice(&blob_progress_data, encoded_addition);
    }

    PutVarint64(&blob_progress_data, blob_file_garbages.size());
    for (const auto& garbage : blob_file_garbages) {
      std::string encoded_garbage;
      garbage.EncodeTo(&encoded_garbage);
      PutLengthPrefixedSlice(&blob_progress_data, encoded_garbage);
    }
    PutLengthPrefixedSlice(dst, blob_progress_data);
  }
  PutVarint32(dst,
              SubcompactionProgressCustomTag::kSubcompactionProgressTerminate);
}

Status SubcompactionProgress::DecodeFrom(Slice* input) {
  Clear();

  while (true) {
    uint32_t custom_tag = 0;
    if (!GetVarint32(input, &custom_tag)) {
      return Status::Corruption("SubcompactionProgress",
                                "custom field tag error");
    }

    if (custom_tag ==
        SubcompactionProgressCustomTag::kSubcompactionProgressTerminate) {
      break;
    }

    Slice field;
    if (!GetLengthPrefixedSlice(input, &field)) {
      return Status::Corruption("SubcompactionProgress",
                                "custom field length prefixed slice error");
    }

    switch (custom_tag) {
      case SubcompactionProgressCustomTag::kNextInternalKeyToCompact:
        next_internal_key_to_compact = field.ToString();
        break;

      case SubcompactionProgressCustomTag::kNumProcessedInputRecords:
        if (!GetVarint64(&field, &num_processed_input_records)) {
          return Status::Corruption("SubcompactionProgress",
                                    "invalid num_processed_input_records");
        }
        break;

      case SubcompactionProgressCustomTag::kOutputLevelProgress: {
        Status s = output_level_progress.DecodeFrom(&field);
        if (!s.ok()) {
          return s;
        }
        break;
      }

      case SubcompactionProgressCustomTag::kProximalOutputLevelProgress: {
        Status s = proximal_output_level_progress.DecodeFrom(&field);
        if (!s.ok()) {
          return s;
        }
        break;
      }

      case SubcompactionProgressCustomTag::kBlobCompactionProgress: {
        has_blob_compaction_progress = true;
        if (!GetVarint64(&field, &next_blob_file_number)) {
          return Status::Corruption("SubcompactionProgress",
                                    "invalid next blob file number");
        }

        uint64_t num_additions = 0;
        if (!GetVarint64(&field, &num_additions) ||
            num_additions > field.size()) {
          return Status::Corruption("SubcompactionProgress",
                                    "invalid blob file addition count");
        }
        blob_file_additions.reserve(static_cast<size_t>(num_additions));
        for (uint64_t i = 0; i < num_additions; ++i) {
          Slice encoded_addition;
          if (!GetLengthPrefixedSlice(&field, &encoded_addition)) {
            return Status::Corruption("SubcompactionProgress",
                                      "invalid blob file addition");
          }
          BlobFileAddition addition;
          Status s = addition.DecodeFrom(&encoded_addition);
          if (!s.ok() || !encoded_addition.empty()) {
            return Status::Corruption("SubcompactionProgress",
                                      "invalid blob file addition metadata");
          }
          blob_file_additions.emplace_back(std::move(addition));
        }

        uint64_t num_garbages = 0;
        if (!GetVarint64(&field, &num_garbages) ||
            num_garbages > field.size()) {
          return Status::Corruption("SubcompactionProgress",
                                    "invalid blob file garbage count");
        }
        blob_file_garbages.reserve(static_cast<size_t>(num_garbages));
        for (uint64_t i = 0; i < num_garbages; ++i) {
          Slice encoded_garbage;
          if (!GetLengthPrefixedSlice(&field, &encoded_garbage)) {
            return Status::Corruption("SubcompactionProgress",
                                      "invalid blob file garbage");
          }
          BlobFileGarbage garbage;
          Status s = garbage.DecodeFrom(&encoded_garbage);
          if (!s.ok() || !encoded_garbage.empty()) {
            return Status::Corruption("SubcompactionProgress",
                                      "invalid blob file garbage metadata");
          }
          blob_file_garbages.emplace_back(std::move(garbage));
        }
        if (!field.empty()) {
          return Status::Corruption("SubcompactionProgress",
                                    "trailing blob compaction progress data");
        }
        break;
      }

      default:
        if ((custom_tag & SubcompactionProgressCustomTag::
                              kSubcompactionProgressCustomTagSafeIgnoreMask) !=
            0) {
          break;
        } else {
          return Status::NotSupported("SubcompactionProgress",
                                      "unsupported critical custom field");
        }
    }
  }

  return Status::OK();
}

bool SubcompactionProgressBuilder::ProcessVersionEdit(const VersionEdit& edit) {
  if (!edit.HasSubcompactionProgress()) {
    return false;
  }

  const SubcompactionProgress& progress = edit.GetSubcompactionProgress();

  MergeDeltaProgress(progress);

  has_subcompaction_progress_ = true;

  return true;
}

void SubcompactionProgressBuilder::MergeDeltaProgress(
    const SubcompactionProgress& delta_progress) {
  accumulated_subcompaction_progress_.next_internal_key_to_compact =
      delta_progress.next_internal_key_to_compact;

  accumulated_subcompaction_progress_.num_processed_input_records =
      delta_progress.num_processed_input_records;

  MaybeMergeDeltaProgressPerLevel(
      accumulated_subcompaction_progress_.output_level_progress,
      delta_progress.output_level_progress);

  MaybeMergeDeltaProgressPerLevel(
      accumulated_subcompaction_progress_.proximal_output_level_progress,
      delta_progress.proximal_output_level_progress);

  if (delta_progress.has_blob_compaction_progress) {
    accumulated_subcompaction_progress_.has_blob_compaction_progress = true;
    accumulated_subcompaction_progress_.next_blob_file_number =
        delta_progress.next_blob_file_number;
    accumulated_subcompaction_progress_.blob_file_additions.insert(
        accumulated_subcompaction_progress_.blob_file_additions.end(),
        delta_progress.blob_file_additions.begin(),
        delta_progress.blob_file_additions.end());
    accumulated_subcompaction_progress_.blob_file_garbages =
        delta_progress.blob_file_garbages;
  }
}

void SubcompactionProgressBuilder::MaybeMergeDeltaProgressPerLevel(
    SubcompactionProgressPerLevel& accumulated_level_progress,
    const SubcompactionProgressPerLevel& delta_level_progress) {
  const auto& delta_files = delta_level_progress.GetOutputFiles();
  if (delta_files.empty()) {
    return;
  }
  for (const FileMetaData& file : delta_files) {
    accumulated_level_progress.AddToOutputFiles(file);  // Stored as copy
  }

  accumulated_level_progress.SetNumProcessedOutputRecords(
      delta_level_progress.GetNumProcessedOutputRecords());
}

void SubcompactionProgressBuilder::Clear() {
  accumulated_subcompaction_progress_.Clear();
  has_subcompaction_progress_ = false;
}
}  // namespace ROCKSDB_NAMESPACE
