//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/types_util.h"

#include <cassert>
#include <cstring>

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

static_assert(kInternalKeyFooterSize == kNumInternalBytes,
              "Internal key footer size mismatch");

Status GetInternalKeyForSeek(const Slice& user_key,
                             const Comparator* comparator, std::string* buf) {
  if (!comparator) {
    return Status::InvalidArgument(
        "Constructing an internal key requires user key comparator.");
  }
  size_t ts_sz = comparator->timestamp_size();
  Slice max_ts = comparator->GetMaxTimestamp();
  if (ts_sz != max_ts.size()) {
    return Status::InvalidArgument(
        "The maximum timestamp returned by Comparator::GetMaxTimestamp is "
        "invalid.");
  }
  buf->reserve(user_key.size() + ts_sz + kNumInternalBytes);
  buf->assign(user_key.data(), user_key.size());
  if (ts_sz) {
    buf->append(max_ts.data(), max_ts.size());
  }
  PutFixed64(buf, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
  return Status::OK();
}

Status GetInternalKeyForSeekForPrev(const Slice& user_key,
                                    const Comparator* comparator,
                                    std::string* buf) {
  if (!comparator) {
    return Status::InvalidArgument(
        "Constructing an internal key requires user key comparator.");
  }
  size_t ts_sz = comparator->timestamp_size();
  Slice min_ts = comparator->GetMinTimestamp();
  if (ts_sz != min_ts.size()) {
    return Status::InvalidArgument(
        "The minimum timestamp returned by Comparator::GetMinTimestamp is "
        "invalid.");
  }
  buf->reserve(user_key.size() + ts_sz + kNumInternalBytes);
  buf->assign(user_key.data(), user_key.size());
  if (ts_sz) {
    buf->append(min_ts.data(), min_ts.size());
  }
  PutFixed64(buf, PackSequenceAndType(0, kValueTypeForSeekForPrev));
  return Status::OK();
}

Status ParseEntry(const Slice& internal_key, const Comparator* comparator,
                  ParsedEntryInfo* parsed_entry) {
  if (internal_key.size() < kNumInternalBytes) {
    return Status::InvalidArgument("Internal key size invalid.");
  }
  if (!comparator) {
    return Status::InvalidArgument(
        "Parsing an internal key requires user key comparator.");
  }
  ParsedInternalKey pikey;
  Status status = ParseInternalKey(internal_key, &pikey, /*log_err_key=*/false);
  if (!status.ok()) {
    return status;
  }

  size_t ts_sz = comparator->timestamp_size();
  if (pikey.user_key.size() < ts_sz) {
    return Status::InvalidArgument("User key(with timestamp) size invalid.");
  }
  if (ts_sz == 0) {
    parsed_entry->user_key = pikey.user_key;
  } else {
    parsed_entry->user_key = StripTimestampFromUserKey(pikey.user_key, ts_sz);
    parsed_entry->timestamp =
        ExtractTimestampFromUserKey(pikey.user_key, ts_sz);
  }
  parsed_entry->sequence = pikey.sequence;
  parsed_entry->type = ROCKSDB_NAMESPACE::GetEntryType(pikey.type);
  return Status::OK();
}

Status PackSequenceAndType(uint64_t seq, EntryType type, uint64_t* packed) {
  if (seq > kMaxSequenceNumber) {
    return Status::InvalidArgument("Sequence number exceeds the 56-bit limit.");
  }

  ValueType value_type;
  switch (type) {
    case kEntryPut:
      value_type = kTypeValue;
      break;
    case kEntryDelete:
      value_type = kTypeDeletion;
      break;
    case kEntrySingleDelete:
      value_type = kTypeSingleDeletion;
      break;
    case kEntryMerge:
      value_type = kTypeMerge;
      break;
    case kEntryRangeDeletion:
      value_type = kTypeRangeDeletion;
      break;
    case kEntryBlobIndex:
      value_type = kTypeBlobIndex;
      break;
    case kEntryDeleteWithTimestamp:
      value_type = kTypeDeletionWithTimestamp;
      break;
    case kEntryWideColumnEntity:
      value_type = kTypeWideColumnEntity;
      break;
    case kEntryTimedPut:
      value_type = kTypeValuePreferredSeqno;
      break;
    case kEntryOther:
      return Status::InvalidArgument("Unsupported entry type.");
    default:
      return Status::InvalidArgument("Unknown entry type.");
  }

  *packed = PackSequenceAndType(seq, value_type);
  return Status::OK();
}

void UnPackSequenceAndType(uint64_t packed, uint64_t* seq, EntryType* type) {
  ValueType value_type;
  UnPackSequenceAndType(packed, seq, &value_type);
  *type = GetEntryType(value_type);
}

Status EncodeEntry(const ParsedEntryInfo& entry, std::string* internal_key) {
  uint64_t packed = 0;
  Status status = PackSequenceAndType(entry.sequence, entry.type, &packed);
  if (!status.ok()) {
    return status;
  }
  internal_key->reserve(GetEncodedEntrySize(entry));
  internal_key->assign(entry.user_key.data(), entry.user_key.size());
  if (!entry.timestamp.empty()) {
    internal_key->append(entry.timestamp.data(), entry.timestamp.size());
  }
  PutFixed64(internal_key, packed);
  return Status::OK();
}

Status EncodeEntry(const ParsedEntryInfo& entry, char* internal_key) {
  uint64_t packed = 0;
  Status status = PackSequenceAndType(entry.sequence, entry.type, &packed);
  if (!status.ok()) {
    return status;
  }
#ifndef NDEBUG
  const uintptr_t buffer_begin = reinterpret_cast<uintptr_t>(internal_key);
  const uintptr_t buffer_end = buffer_begin + GetEncodedEntrySize(entry);
  for (const Slice& slice : {entry.user_key, entry.timestamp}) {
    const uintptr_t slice_begin = reinterpret_cast<uintptr_t>(slice.data());
    assert((slice.empty() || slice_begin >= buffer_end ||
            slice_begin + slice.size() <= buffer_begin) &&
           "Input slices must not overlap the written region");
  }
#endif
  char* dst = internal_key;
  if (!entry.user_key.empty()) {
    memcpy(dst, entry.user_key.data(), entry.user_key.size());
  }
  dst += entry.user_key.size();
  if (!entry.timestamp.empty()) {
    memcpy(dst, entry.timestamp.data(), entry.timestamp.size());
  }
  dst += entry.timestamp.size();
  EncodeFixed64(dst, packed);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
