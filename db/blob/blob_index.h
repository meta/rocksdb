//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <sstream>
#include <string>

#include "db/blob/blob_constants.h"
#include "rocksdb/compression_type.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// BlobIndex is a pointer to the blob and metadata of the blob. The index is
// stored in base DB as ValueType::kTypeBlobIndex.
// There are four types of blob index:
//
//    kInlinedTTL:
//      +------+------------+---------------+
//      | type | expiration | value         |
//      +------+------------+---------------+
//      | char | varint64   | variable size |
//      +------+------------+---------------+
//
//    kBlob:
//      +------+-------------+----------+----------+-------------+
//      | type | file number | offset   | size     | compression |
//      +------+-------------+----------+----------+-------------+
//      | char | varint64    | varint64 | varint64 | char        |
//      +------+-------------+----------+----------+-------------+
//
//    kBlobTTL:
//      +------+------------+-------------+----------+----------+-------------+
//      | type | expiration | file number | offset   | size     | compression |
//      +------+------------+-------------+----------+----------+-------------+
//      | char | varint64   | varint64    | varint64 | varint64 | char        |
//      +------+------------+-------------+----------+----------+-------------+
//
//    kIndirectBlob:
//      +------+---------+------------+--------+-----------------+----------+
//      | type | version | generation | origin | original offset | size     |
//      +------+---------+------------+--------+-----------------+----------+
//      | char | varint32| varint64   |varint64| varint64        | varint64 |
//      +------+---------+------------+--------+-----------------+----------+
//      +-------------+------------------+-------------+-----------------+
//      | compression | checksum version | logical CRC | stored CRC (*)  |
//      +-------------+------------------+-------------+-----------------+
//      | char        | varint32         | fixed32     | fixed32         |
//      +-------------+------------------+-------------+-----------------+
//
// The indirect form names an immutable logical value by (origin, original
// offset). Generation is the physical route source carried by the SST and can
// change when compaction flattens a route. The stable checksum does not change
// when the value is relocated. Both checksums are unmasked CRC32C and exclude
// the user key. The logical CRC covers uncompressed value bytes. Checksum v2
// additionally covers exact stored payload bytes followed by the compression
// byte, so GC can authenticate and copy compressed bytes without decompression.
// (*) For kNoCompression the stored CRC is omitted and derived by extending
// the logical CRC with the compression byte. Experimental checksum v1 contains
// only the logical CRC and remains readable, but new feature writes use v2.
//
// There isn't a kInlined (without TTL) type since we can store it as a plain
// value (i.e. ValueType::kTypeValue).
class BlobIndex {
 public:
  static constexpr uint32_t kIndirectBlobFormatVersion = 1;
  static constexpr uint32_t kStableChecksumVersion1 = 1;
  static constexpr uint32_t kStableChecksumVersion2 = 2;

  static bool IsSupportedStableChecksumVersion(uint32_t version) {
    return version == kStableChecksumVersion1 ||
           version == kStableChecksumVersion2;
  }

  static uint32_t ExtendChecksumWithCompression(uint32_t payload_checksum,
                                                CompressionType compression) {
    const char marker = static_cast<char>(compression);
    return crc32c::Extend(payload_checksum, &marker, 1);
  }

  static uint32_t ComputeStoredChecksum(const Slice& payload,
                                        CompressionType compression) {
    return ExtendChecksumWithCompression(
        crc32c::Value(payload.data(), payload.size()), compression);
  }

  enum class Type : unsigned char {
    kInlinedTTL = 0,
    kBlob = 1,
    kBlobTTL = 2,
    kIndirectBlob = 3,
    kUnknown = 4,
  };

  BlobIndex() : type_(Type::kUnknown) {}

  BlobIndex(const BlobIndex&) = default;
  BlobIndex& operator=(const BlobIndex&) = default;
  BlobIndex(BlobIndex&&) = default;
  BlobIndex& operator=(BlobIndex&&) = default;

  ~BlobIndex() = default;

  bool IsInlined() const { return type_ == Type::kInlinedTTL; }

  bool IsIndirect() const { return type_ == Type::kIndirectBlob; }

  // True for a blob record stored in the same physical file as the table entry.
  // Only embedded-blob SST reader/writer paths should interpret file number
  // zero this way; generic blob metadata treats it as invalid.
  bool IsSameFile() const {
    return (type_ == Type::kBlob || type_ == Type::kBlobTTL) &&
           file_number_ == kCurrentFileBlobIndexFileNumber;
  }

  bool HasTTL() const {
    return type_ == Type::kInlinedTTL || type_ == Type::kBlobTTL;
  }

  uint64_t expiration() const {
    assert(HasTTL());
    return expiration_;
  }

  const Slice& value() const {
    assert(IsInlined());
    return value_;
  }

  uint64_t file_number() const {
    assert(!IsInlined());
    return file_number_;
  }

  uint64_t offset() const {
    assert(!IsInlined());
    return offset_;
  }

  uint64_t size() const {
    assert(!IsInlined());
    return size_;
  }

  CompressionType compression() const {
    assert(!IsInlined());
    return compression_;
  }

  uint64_t generation_file_number() const {
    assert(IsIndirect());
    return file_number_;
  }

  uint64_t origin_file_number() const {
    assert(IsIndirect());
    return origin_file_number_;
  }

  uint64_t original_value_offset() const {
    assert(IsIndirect());
    return offset_;
  }

  uint32_t stable_checksum_version() const {
    assert(IsIndirect());
    return stable_checksum_version_;
  }

  uint32_t stable_checksum() const {
    assert(IsIndirect());
    return stable_checksum_;
  }

  uint32_t stored_checksum() const {
    assert(IsIndirect());
    return stored_checksum_;
  }

  Status VerifyStoredChecksum(const Slice& payload) const {
    if (!IsIndirect() || payload.size() != size_) {
      return Status::Corruption("Invalid stored blob checksum input");
    }
    if (stable_checksum_version_ == kStableChecksumVersion2 &&
        ComputeStoredChecksum(payload, compression_) != stored_checksum_) {
      return Status::Corruption("Stored blob checksum mismatch");
    }
    return Status::OK();
  }

  Status SetGenerationFileNumber(uint64_t generation_file_number) {
    if (!IsIndirect() || generation_file_number == 0) {
      return Status::InvalidArgument("Invalid indirect blob generation");
    }
    file_number_ = generation_file_number;
    return Status::OK();
  }

  // Performs semantic validation that is intentionally not part of decoding
  // legacy encodings. Keeping it separate preserves compatibility for callers
  // that only decode existing BlobIndexes, while metadata-producing paths can
  // fail closed before excluding or publishing a malformed reference.
  Status Validate() const {
    const char* kErrorMessage = "Invalid blob index";
    if (type_ >= Type::kUnknown) {
      return Status::Corruption(kErrorMessage, "Unknown blob index type");
    }
    if (IsInlined()) {
      return Status::OK();
    }
    if (file_number_ == 0 && !IsSameFile()) {
      return Status::Corruption(kErrorMessage, "Invalid blob file number");
    }
    if (!IsValidCompression(compression_)) {
      return Status::Corruption(kErrorMessage, "Invalid blob compression type");
    }
    if (IsIndirect() &&
        (origin_file_number_ == 0 ||
         !IsSupportedStableChecksumVersion(stable_checksum_version_))) {
      return Status::Corruption(kErrorMessage,
                                "Invalid indirect blob identity");
    }
    return Status::OK();
  }

  Status DecodeFrom(Slice slice) {
    const char* kErrorMessage = "Error while decoding blob index";
    if (slice.empty()) {
      return Status::Corruption(kErrorMessage, "Empty blob index");
    }

    Reset();
    type_ = static_cast<Type>(static_cast<unsigned char>(slice[0]));
    if (type_ >= Type::kUnknown) {
      return Status::Corruption(
          kErrorMessage, "Unknown blob index type: " +
                             std::to_string(static_cast<unsigned int>(type_)));
    }
    slice = Slice(slice.data() + 1, slice.size() - 1);
    if (HasTTL()) {
      if (!GetVarint64(&slice, &expiration_)) {
        return Status::Corruption(kErrorMessage, "Corrupted expiration");
      }
    }
    if (IsInlined()) {
      value_ = slice;
    } else if (IsIndirect()) {
      uint32_t format_version = 0;
      if (!GetCanonicalVarint32(&slice, &format_version)) {
        return Status::Corruption(kErrorMessage,
                                  "Corrupted indirect format version");
      }
      if (format_version != kIndirectBlobFormatVersion) {
        return Status::Corruption(kErrorMessage,
                                  "Unsupported indirect blob index version: " +
                                      std::to_string(format_version));
      }
      if (!GetCanonicalVarint64(&slice, &file_number_) ||
          !GetCanonicalVarint64(&slice, &origin_file_number_) ||
          !GetCanonicalVarint64(&slice, &offset_) ||
          !GetCanonicalVarint64(&slice, &size_)) {
        return Status::Corruption(kErrorMessage,
                                  "Corrupted indirect blob identity");
      }
      if (file_number_ == 0 || origin_file_number_ == 0) {
        return Status::Corruption(kErrorMessage,
                                  "Invalid indirect blob file number");
      }
      if (slice.empty()) {
        return Status::Corruption(kErrorMessage,
                                  "Missing indirect blob compression");
      }
      compression_ =
          static_cast<CompressionType>(static_cast<unsigned char>(slice[0]));
      slice.remove_prefix(1);
      if (!IsValidCompression(compression_)) {
        return Status::Corruption(kErrorMessage,
                                  "Invalid blob compression type");
      }
      if (!GetCanonicalVarint32(&slice, &stable_checksum_version_)) {
        return Status::Corruption(kErrorMessage,
                                  "Corrupted stable checksum version");
      }
      if (!IsSupportedStableChecksumVersion(stable_checksum_version_)) {
        return Status::Corruption(kErrorMessage,
                                  "Unsupported stable checksum version: " +
                                      std::to_string(stable_checksum_version_));
      }
      if (!GetFixed32(&slice, &stable_checksum_)) {
        return Status::Corruption(kErrorMessage, "Corrupted stable checksum");
      }
      if (stable_checksum_version_ == kStableChecksumVersion2) {
        if (compression_ == kNoCompression) {
          stored_checksum_ =
              ExtendChecksumWithCompression(stable_checksum_, compression_);
        } else if (!GetFixed32(&slice, &stored_checksum_)) {
          return Status::Corruption(kErrorMessage, "Corrupted stored checksum");
        }
      }
      if (!slice.empty()) {
        return Status::Corruption(kErrorMessage,
                                  "Trailing indirect blob bytes");
      }
    } else {
      if (!GetVarint64(&slice, &file_number_) ||
          !GetVarint64(&slice, &offset_) || !GetVarint64(&slice, &size_)) {
        return Status::Corruption(kErrorMessage, "Corrupted blob offset");
      }
      if (slice.size() != 1) {
        return Status::Corruption(kErrorMessage, "Corrupted blob compression");
      }
      compression_ =
          static_cast<CompressionType>(static_cast<unsigned char>(slice[0]));
    }
    return Status::OK();
  }

  std::string DebugString(bool output_hex) const {
    std::ostringstream oss;

    if (IsInlined()) {
      oss << "[inlined blob] value:" << value_.ToString(output_hex);
    } else {
      oss << (IsIndirect() ? "[indirect blob ref] generation:"
                           : "[blob ref] file:");
      if (IsSameFile()) {
        oss << "same";
      } else {
        oss << file_number_;
      }
      if (IsIndirect()) {
        oss << " origin:" << origin_file_number_;
      }
      oss << " offset:" << offset_ << " size:" << size_;
      if (IsIndirect()) {
        oss << " checksum_v" << stable_checksum_version_ << ":"
            << stable_checksum_;
        if (stable_checksum_version_ == kStableChecksumVersion2) {
          oss << " stored_crc:" << stored_checksum_;
        }
      }
      oss << " compression: " << CompressionTypeToString(compression_);
    }

    if (HasTTL()) {
      oss << " exp:" << expiration_;
    }

    return oss.str();
  }

  // Encode this blob index into dst based on its type.
  void EncodeTo(std::string* dst) const {
    if (IsInlined()) {
      EncodeInlinedTTL(dst, expiration_, value_);
    } else if (HasTTL()) {
      EncodeBlobTTL(dst, expiration_, file_number_, offset_, size_,
                    compression_);
    } else if (IsIndirect()) {
      EncodeIndirectBlob(dst, file_number_, origin_file_number_, offset_, size_,
                         compression_, stable_checksum_version_,
                         stable_checksum_, stored_checksum_);
    } else {
      EncodeBlob(dst, file_number_, offset_, size_, compression_);
    }
  }

  static void EncodeInlinedTTL(std::string* dst, uint64_t expiration,
                               const Slice& value) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(1 + kMaxVarint64Length + value.size());
    dst->push_back(static_cast<char>(Type::kInlinedTTL));
    PutVarint64(dst, expiration);
    dst->append(value.data(), value.size());
  }

  static void EncodeBlob(std::string* dst, uint64_t file_number,
                         uint64_t offset, uint64_t size,
                         CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 3 + 2);
    dst->push_back(static_cast<char>(Type::kBlob));
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

  static void EncodeBlobTTL(std::string* dst, uint64_t expiration,
                            uint64_t file_number, uint64_t offset,
                            uint64_t size, CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 4 + 2);
    dst->push_back(static_cast<char>(Type::kBlobTTL));
    PutVarint64(dst, expiration);
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

  static void EncodeIndirectBlob(
      std::string* dst, uint64_t generation_file_number,
      uint64_t origin_file_number, uint64_t original_value_offset,
      uint64_t stored_value_size, CompressionType compression,
      uint32_t stable_checksum_version, uint32_t stable_checksum) {
    assert(stable_checksum_version == kStableChecksumVersion1);
    EncodeIndirectBlob(dst, generation_file_number, origin_file_number,
                       original_value_offset, stored_value_size, compression,
                       stable_checksum_version, stable_checksum, 0);
  }

  static void EncodeIndirectBlob(
      std::string* dst, uint64_t generation_file_number,
      uint64_t origin_file_number, uint64_t original_value_offset,
      uint64_t stored_value_size, CompressionType compression,
      uint32_t stable_checksum_version, uint32_t stable_checksum,
      uint32_t stored_checksum) {
    assert(dst != nullptr);
    assert(generation_file_number != 0);
    assert(origin_file_number != 0);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 6 + sizeof(stable_checksum) + 2);
    dst->push_back(static_cast<char>(Type::kIndirectBlob));
    PutVarint32(dst, kIndirectBlobFormatVersion);
    PutVarint64(dst, generation_file_number);
    PutVarint64(dst, origin_file_number);
    PutVarint64(dst, original_value_offset);
    PutVarint64(dst, stored_value_size);
    dst->push_back(static_cast<char>(compression));
    PutVarint32(dst, stable_checksum_version);
    PutFixed32(dst, stable_checksum);
    if (stable_checksum_version == kStableChecksumVersion2 &&
        compression != kNoCompression) {
      PutFixed32(dst, stored_checksum);
    }
  }

  static void EncodeIdentityBlob(std::string* dst, uint64_t origin_file_number,
                                 uint64_t original_value_offset,
                                 uint64_t stored_value_size,
                                 CompressionType compression,
                                 uint32_t stable_checksum) {
    EncodeIndirectBlob(dst, origin_file_number, origin_file_number,
                       original_value_offset, stored_value_size, compression,
                       kStableChecksumVersion1, stable_checksum);
  }

  static void EncodeIdentityBlob(std::string* dst, uint64_t origin_file_number,
                                 uint64_t original_value_offset,
                                 uint64_t stored_value_size,
                                 CompressionType compression,
                                 uint32_t logical_checksum,
                                 uint32_t stored_checksum) {
    EncodeIndirectBlob(dst, origin_file_number, origin_file_number,
                       original_value_offset, stored_value_size, compression,
                       kStableChecksumVersion2, logical_checksum,
                       stored_checksum);
  }

 private:
  static bool GetCanonicalVarint32(Slice* input, uint32_t* value) {
    if (input == nullptr || value == nullptr) {
      return false;
    }
    const size_t before = input->size();
    return GetVarint32(input, value) &&
           before - input->size() == VarintLength(*value);
  }

  static bool GetCanonicalVarint64(Slice* input, uint64_t* value) {
    if (input == nullptr || value == nullptr) {
      return false;
    }
    const size_t before = input->size();
    return GetVarint64(input, value) &&
           before - input->size() == VarintLength(*value);
  }

  static bool IsValidCompression(CompressionType compression) {
    return compression <= kLastBuiltinCompression ||
           (compression >= kFirstCustomCompression &&
            compression <= kLastCustomCompression);
  }

  void Reset() {
    type_ = Type::kUnknown;
    expiration_ = 0;
    value_.clear();
    file_number_ = 0;
    origin_file_number_ = 0;
    offset_ = 0;
    size_ = 0;
    stable_checksum_version_ = 0;
    stable_checksum_ = 0;
    stored_checksum_ = 0;
    compression_ = kNoCompression;
  }

  Type type_ = Type::kUnknown;
  uint64_t expiration_ = 0;
  Slice value_;
  uint64_t file_number_ = 0;
  uint64_t origin_file_number_ = 0;
  uint64_t offset_ = 0;
  uint64_t size_ = 0;
  uint32_t stable_checksum_version_ = 0;
  uint32_t stable_checksum_ = 0;
  uint32_t stored_checksum_ = 0;
  CompressionType compression_ = kNoCompression;
};

}  // namespace ROCKSDB_NAMESPACE
