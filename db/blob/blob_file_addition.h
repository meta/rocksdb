//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>

#include "db/blob/blob_constants.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

// Manifest-side blob file schema version. Zero means legacy blob log format
// with no blog-specific identity descriptor.
static constexpr uint8_t kLegacyBlobFileSchemaVersion = 0;
static constexpr size_t kBlobFileIdentitySize = 10;

class BlobFileAddition {
 public:
  BlobFileAddition() = default;

  BlobFileAddition(uint64_t blob_file_number, uint64_t total_blob_count,
                   uint64_t total_blob_bytes, std::string checksum_method,
                   std::string checksum_value,
                   uint64_t physical_blob_file_size = 0,
                   uint8_t schema_version = kLegacyBlobFileSchemaVersion,
                   std::string file_identity = {})
      : blob_file_number_(blob_file_number),
        total_blob_count_(total_blob_count),
        total_blob_bytes_(total_blob_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)),
        physical_blob_file_size_(physical_blob_file_size),
        schema_version_(schema_version),
        file_identity_(std::move(file_identity)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
    assert(!physical_blob_file_size_ ||
           physical_blob_file_size_ >= total_blob_bytes_);
    assert((schema_version_ == kLegacyBlobFileSchemaVersion) ==
           file_identity_.empty());
    assert(file_identity_.empty() ||
           file_identity_.size() == kBlobFileIdentitySize);
  }

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetTotalBlobCount() const { return total_blob_count_; }
  uint64_t GetTotalBlobBytes() const { return total_blob_bytes_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }
  uint64_t GetPhysicalBlobFileSize() const { return physical_blob_file_size_; }
  uint8_t GetSchemaVersion() const { return schema_version_; }
  const std::string& GetFileIdentity() const { return file_identity_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  uint64_t blob_file_number_ = kInvalidBlobFileNumber;
  uint64_t total_blob_count_ = 0;
  uint64_t total_blob_bytes_ = 0;
  std::string checksum_method_;
  std::string checksum_value_;
  uint64_t physical_blob_file_size_ = 0;
  uint8_t schema_version_ = kLegacyBlobFileSchemaVersion;
  std::string file_identity_;
};

bool operator==(const BlobFileAddition& lhs, const BlobFileAddition& rhs);
bool operator!=(const BlobFileAddition& lhs, const BlobFileAddition& rhs);

std::ostream& operator<<(std::ostream& os,
                         const BlobFileAddition& blob_file_addition);
JSONWriter& operator<<(JSONWriter& jw,
                       const BlobFileAddition& blob_file_addition);

}  // namespace ROCKSDB_NAMESPACE
