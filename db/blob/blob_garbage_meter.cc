//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_garbage_meter.h"

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

Status BlobGarbageMeter::ProcessInFlow(const Slice& key, const Slice& value) {
  return ProcessFlow(key, value, /*is_inflow=*/true);
}

Status BlobGarbageMeter::ProcessOutFlow(const Slice& key, const Slice& value) {
  return ProcessFlow(key, value, /*is_inflow=*/false);
}

Status BlobGarbageMeter::GetBlobReferenceDetails(const ParsedInternalKey& ikey,
                                                 const BlobIndex& blob_index,
                                                 uint64_t* blob_file_number,
                                                 uint64_t* bytes) {
  assert(blob_file_number);
  assert(*blob_file_number == kInvalidBlobFileNumber);
  assert(bytes);
  assert(*bytes == 0);

  if (blob_index.IsInlined() || blob_index.HasTTL()) {
    return Status::Corruption("Unexpected TTL/inlined blob index");
  }
  if (blob_index.IsSameFile()) {
    return Status::OK();
  }

  *blob_file_number = blob_index.file_number();
  *bytes =
      blob_index.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(ikey.user_key.size());

  return Status::OK();
}

Status BlobGarbageMeter::ParseBlobIndexReference(const ParsedInternalKey& ikey,
                                                 const Slice& value,
                                                 uint64_t* blob_file_number,
                                                 uint64_t* bytes) {
  assert(blob_file_number);
  assert(*blob_file_number == kInvalidBlobFileNumber);
  assert(bytes);
  assert(*bytes == 0);

  if (ikey.type != kTypeBlobIndex) {
    return Status::OK();
  }

  BlobIndex blob_index;

  {
    const Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      return s;
    }
  }

  return GetBlobReferenceDetails(ikey, blob_index, blob_file_number, bytes);
}

void BlobGarbageMeter::AddFlow(uint64_t blob_file_number, uint64_t bytes,
                               bool is_inflow) {
  if (is_inflow) {
    flows_[blob_file_number].AddInFlow(bytes);
    return;
  }

  // Note: in order to measure the amount of additional garbage, we only need to
  // track the outflow for preexisting files, i.e. those that also had inflow.
  // (Newly written files would only have outflow.)
  auto it = flows_.find(blob_file_number);
  if (it != flows_.end()) {
    it->second.AddOutFlow(bytes);
  }
}

Status BlobGarbageMeter::ProcessFlow(const Slice& key, const Slice& value,
                                     bool is_inflow) {
  ParsedInternalKey ikey;

  {
    constexpr bool log_err_key = false;
    const Status s = ParseInternalKey(key, &ikey, log_err_key);
    if (!s.ok()) {
      return s;
    }
  }

  uint64_t blob_file_number = kInvalidBlobFileNumber;
  uint64_t bytes = 0;
  if (Status s =
          ParseBlobIndexReference(ikey, value, &blob_file_number, &bytes);
      !s.ok()) {
    return s;
  }

  if (blob_file_number != kInvalidBlobFileNumber) {
    AddFlow(blob_file_number, bytes, is_inflow);
    return Status::OK();
  }

  return ProcessEntityBlobReferences(ikey, value, is_inflow);
}

Status BlobGarbageMeter::ProcessEntityBlobReferences(
    const ParsedInternalKey& ikey, const Slice& value, bool is_inflow) {
  if (ikey.type != kTypeWideColumnEntity) {
    return Status::OK();
  }

  bool has_blob_columns = false;
  Status status =
      WideColumnSerialization::HasBlobColumns(value, has_blob_columns);
  if (!status.ok() || !has_blob_columns) {
    return status;
  }

  return WideColumnSerialization::ForEachBlobIndex(
      value, [&](const BlobIndex& blob_index) -> Status {
        uint64_t file_number = kInvalidBlobFileNumber;
        uint64_t blob_bytes = 0;
        if (const Status s = GetBlobReferenceDetails(ikey, blob_index,
                                                     &file_number, &blob_bytes);
            !s.ok()) {
          return s;
        }
        if (file_number != kInvalidBlobFileNumber) {
          AddFlow(file_number, blob_bytes, is_inflow);
        }
        return Status::OK();
      });
}

}  // namespace ROCKSDB_NAMESPACE
