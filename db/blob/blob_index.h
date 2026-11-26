//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <sstream>
#include <string>

#include "db/blob/blob_constants.h"
#include "port/likely.h"
#include "rocksdb/compression_type.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// The fields of an indirect BlobIndex that describe its value bytes: the stored
// size, the compression type, the logical CRC (stable_checksum) and the stored
// CRC (stored_checksum). They never change, even when indirect blob GC moves
// the value. The stable blob cache keeps the fingerprint with each cached value
// (see StableBlobContents), so that a value saved to a secondary cache can be
// checked when it comes back. A cache hit does not compare the cached
// fingerprint with the fingerprint of the BlobIndex (see
// BlobSource::GetIndirectBlobFromCache()).
struct StableBlobFingerprint {
  uint64_t stored_value_size = 0;
  CompressionType compression = kNoCompression;
  uint32_t stable_checksum = 0;
  uint32_t stored_checksum = 0;

  bool operator==(const StableBlobFingerprint& other) const {
    return stored_value_size == other.stored_value_size &&
           compression == other.compression &&
           stable_checksum == other.stable_checksum &&
           stored_checksum == other.stored_checksum;
  }
};

// BlobIndex is a pointer to the blob and metadata of the blob. The index is
// stored in base DB as ValueType::kTypeBlobIndex.
// There are five types of blob index. The fields of the two indirect types
// (generation, origin, original offset and the two CRCs) are explained after
// the diagrams.
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
//    kIndirectBlob (generation == origin, not stored):
//      +------+--------+-----------------+----------+
//      | type | origin | original offset | size     |
//      +------+--------+-----------------+----------+
//      | char |varint64| varint64        | varint64 |
//      +------+--------+-----------------+----------+
//    kIndirectFlattenedBlob (generation > origin):
//      +------+------------+--------+-----------------+----------+
//      | type | generation | origin | original offset | size     |
//      +------+------------+--------+-----------------+----------+
//      | char | varint64   |varint64| varint64        | varint64 |
//      +------+------------+--------+-----------------+----------+
//    both, after the size:
//      +-------------+-------------+-----------------+
//      | compression | logical CRC | stored CRC (*)  |
//      +-------------+-------------+-----------------+
//      | char        | fixed32     | fixed32         |
//      +-------------+-------------+-----------------+
//
// Background: indirect blob GC must move live values out of a blob file that
// has a lot of garbage. GC copies them into a relocation file (see "Relocation
// file" below). A legacy kBlob or kBlobTTL index finds the value only by its
// physical file number and offset, so moving a value would mean rewriting
// every SST that points to it. The two indirect types (kIndirectBlob = 3 and
// kIndirectFlattenedBlob = 4) let an SST keep its index when the value moves.
// An index of one of these types is an "indirect BlobIndex". When
// enable_blob_indirection is on, the blob file builder writes kIndirectBlob
// indexes.
//
// Relocation file: a file into which GC copies live values. A reader finds a
// value in it by its stable identity (origin, original offset; see "Stable
// identity" below).
//
// Stable identity: (origin, original offset).
//   - origin: the blob file where the value was first written.
//   - original offset: the value's offset in the origin file.
//   The stable identity names one immutable value and never changes, even
//   when the value moves to another file. A relocation file stores the value
//   under this key.
//
// Generation number (or generation): the file number that a reference uses to
// find its value. Type 4 stores it. Type 3 does not store it, because it equals
// the origin. A new value has generation number == origin. When GC moves the
// value, the reference keeps its generation number, and a route (see Route
// below) sends the read to the relocation file. The generation number is only
// a number: the file with that number may be a live blob file, a live
// relocation file, or an old file that has a route.
//   - Live file: a blob file or relocation file that GC has not removed.
//   - Old file: a file number whose file GC has removed. The number stays
//     only as the key of a route, because SSTs may still store it as their
//     generation number.
//   - Route: a mapping from an old file number to the relocation file that
//     now holds its values. Routes never chain, so a read follows at most one
//     route. The read then finds the value in that file by its stable
//     identity.
//   - An index with generation number == origin is an "unflattened"
//     reference.
//   - Compaction can later rewrite the generation number to the relocation
//     file that holds the value ("flatten"), so the reference does not need
//     the route any more. The type becomes kIndirectFlattenedBlob; the stable
//     identity, size, compression and checksums do not change. Only flatten
//     makes the generation differ from the origin.
//
// Format version: the type byte also gives the format version. Types 3 and 4
// are version 1 (kIndirectBlobFormatVersion), and a later format takes new type
// values.
//
// Why two types: type 3 has no generation field, because its generation equals
// its origin. This is true for every reference until compaction flattens it, so
// most references do not store the same file number twice. Type 4 stores the
// generation, which must be larger than the origin (a relocation file is newer
// than every origin it holds). The decoder rejects type 4 with generation <=
// origin, so each reference has one encoding.
//
// Why generation comes before origin:
//   - A read uses the fields in this order. First it uses the generation to
//     find the file that holds the value. Then it finds the value in that file
//     by (origin, original offset).
//   - The per-SST reference metadata sorts references by (encoding class,
//     generation, origin, original offset) (see blob_reference_metadata.h).
//     The index uses the same order.
//   - Origin sits in the middle, so both pairs stay adjacent: (generation,
//     origin) follows the encoding class in the group key of the reference
//     metadata, and (origin, original offset) is the stable identity.
//   - It matches the legacy kBlob layout, where the file the SST points to
//     comes first.
//
// Checksums: both are raw CRC32C values (crc32c::Value, not crc32c::Mask,
// unlike the blob log record CRCs), because the stored CRC is derived with
// crc32c::Extend. They exclude the user key and do not change when the value is
// relocated.
//   - logical CRC (stable_checksum()): covers the uncompressed value bytes.
//   - stored CRC (stored_checksum()): covers the exact stored payload bytes,
//     followed by the compression byte. GC uses it to check and copy compressed
//     bytes without decompressing them.
//   (*) For kNoCompression the stored CRC is not written. It equals the
//   logical CRC extended with the compression byte, so the decoder computes
//   it.
//
// There isn't a kInlined (without TTL) type since we can store it as a plain
// value (i.e. ValueType::kTypeValue).
class BlobIndex {
 public:
  // The only indirect format, given by the type byte (kIndirectBlob and
  // kIndirectFlattenedBlob). Any later change to its fields, including the
  // checksums, uses new type values and a new value here.
  static constexpr uint32_t kIndirectBlobFormatVersion = 1;

  // The largest valid file number, 2^62 - 1. It equals kFileNumberMask in
  // db/version_edit.h, which this header does not include (blob_source.cc
  // checks that the two are equal). DecodeFrom() rejects an indirect index
  // with a larger file number, so the stable cache key (see
  // GetStableBlobCacheKey() in db/blob/blob_source.cc) can use the top two
  // bits of a file number for other data.
  static constexpr uint64_t kMaxFileNumber = 0x3FFFFFFFFFFFFFFFULL;

  // True for a built-in or custom compression type that a blob may be stored
  // with.
  static bool IsValidCompression(CompressionType compression) {
    return compression <= kLastBuiltinCompression ||
           (compression >= kFirstCustomCompression &&
            compression <= kLastCustomCompression);
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
    kIndirectFlattenedBlob = 4,
    kUnknown = 5,
  };

  BlobIndex() : type_(Type::kUnknown) {}

  BlobIndex(const BlobIndex&) = default;
  BlobIndex& operator=(const BlobIndex&) = default;
  BlobIndex(BlobIndex&&) = default;
  BlobIndex& operator=(BlobIndex&&) = default;

  ~BlobIndex() = default;

  bool IsInlined() const { return type_ == Type::kInlinedTTL; }

  bool IsIndirect() const {
    return type_ == Type::kIndirectBlob ||
           type_ == Type::kIndirectFlattenedBlob;
  }

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

  // For an indirect index, file_number() is the generation number and
  // offset() is the original offset in the origin file. The two can name
  // different files. Use generation_file_number(), origin_file_number() and
  // original_value_offset() for indirect indexes.
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

  uint32_t stable_checksum() const {
    assert(IsIndirect());
    return stable_checksum_;
  }

  // For kNoCompression the encoding has no stored CRC: it is the logical CRC
  // extended with the compression byte (ExtendChecksumWithCompression()). It
  // is computed here, when a caller asks, so that a decode does not pay for
  // it. Most decodes (for example in compaction) never read it.
  uint32_t stored_checksum() const {
    assert(IsIndirect());
    return compression_ == kNoCompression
               ? ExtendChecksumWithCompression(stable_checksum_, compression_)
               : stored_checksum_;
  }

  StableBlobFingerprint fingerprint() const {
    assert(IsIndirect());
    return StableBlobFingerprint{size_, compression_, stable_checksum_,
                                 stored_checksum()};
  }

  // Checks the decoded fields. The type must be known, and then an inlined
  // index always passes. Other types need a nonzero file number (except a
  // same-file index) and a valid compression type, and an indirect index also
  // needs a nonzero origin. DecodeFrom() does not run all of these checks for
  // the legacy types, so existing callers that only decode keep their behavior.
  // The reference collector (BlobReferenceMetadataCollector::Add(), through
  // ConvertBlobIndex() in blob_reference_metadata.cc),
  // BlobReference::FromBlobIndex() and
  // WideColumnSerialization::ForEachBlobIndex() call Validate(). So a malformed
  // index fails the metadata build. It is not skipped and not written into the
  // metadata.
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
    if (IsIndirect() && origin_file_number_ == 0) {
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
      return DecodeIndirectFrom(slice, kErrorMessage);
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
        oss << " logical_crc:" << stable_checksum_
            << " stored_crc:" << stored_checksum();
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
      EncodeIndirectBlob(dst, file_number_,
                         {.origin_file_number = origin_file_number_,
                          .original_value_offset = offset_,
                          .stored_value_size = size_,
                          .compression = compression_,
                          .stable_checksum = stable_checksum_,
                          .stored_checksum = stored_checksum()});
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

  // The fields of an indirect BlobIndex except the generation. Callers set
  // them by name, so values of the same type (the file number, offset and size;
  // the two CRCs) cannot be swapped by mistake. The generation is a separate
  // argument of EncodeIndirectBlob(), so the common unflattened path
  // (EncodeUnflattenedIndirectBlob()) cannot leave it unset.
  struct IndirectBlobFields {
    uint64_t origin_file_number = 0;
    uint64_t original_value_offset = 0;
    uint64_t stored_value_size = 0;
    CompressionType compression = kNoCompression;
    uint32_t stable_checksum = 0;
    uint32_t stored_checksum = 0;
  };

  static void EncodeIndirectBlob(std::string* dst,
                                 uint64_t generation_file_number,
                                 const IndirectBlobFields& fields) {
    const uint64_t origin_file_number = fields.origin_file_number;
    const uint64_t original_value_offset = fields.original_value_offset;
    const uint64_t stored_value_size = fields.stored_value_size;
    const CompressionType compression = fields.compression;
    const uint32_t stable_checksum = fields.stable_checksum;
    const uint32_t stored_checksum = fields.stored_checksum;
    assert(dst != nullptr);
    assert(origin_file_number != 0);
    assert(generation_file_number >= origin_file_number);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 5 + 2 * sizeof(stable_checksum) + 1);
    if (generation_file_number == origin_file_number) {
      dst->push_back(static_cast<char>(Type::kIndirectBlob));
    } else {
      dst->push_back(static_cast<char>(Type::kIndirectFlattenedBlob));
      PutVarint64(dst, generation_file_number);
    }
    PutVarint64(dst, origin_file_number);
    PutVarint64(dst, original_value_offset);
    PutVarint64(dst, stored_value_size);
    dst->push_back(static_cast<char>(compression));
    PutFixed32(dst, stable_checksum);
    if (compression != kNoCompression) {
      PutFixed32(dst, stored_checksum);
    }
  }

  // Encodes an unflattened indirect BlobIndex: its generation is its origin.
  static void EncodeUnflattenedIndirectBlob(std::string* dst,
                                            const IndirectBlobFields& fields) {
    EncodeIndirectBlob(dst, fields.origin_file_number, fields);
  }

 private:
  // Decodes the identity of an indirect index (kIndirectBlob or
  // kIndirectFlattenedBlob) from `slice`, which starts after the type byte.
  // Kept out of line so that DecodeFrom() stays small for the base kinds
  // (kInlinedTTL, kBlob, kBlobTTL).
  ROCKSDB_NOINLINE Status DecodeIndirectFrom(Slice slice,
                                             const char* kErrorMessage) {
    const bool flattened = type_ == Type::kIndirectFlattenedBlob;
    if ((flattened && !GetCanonicalVarint64(&slice, &file_number_)) ||
        !GetCanonicalVarint64(&slice, &origin_file_number_) ||
        !GetCanonicalVarint64(&slice, &offset_) ||
        !GetCanonicalVarint64(&slice, &size_)) {
      return Status::Corruption(kErrorMessage,
                                "Corrupted indirect blob identity");
    }
    if (!flattened) {
      file_number_ = origin_file_number_;
    }
    if (file_number_ == 0 || origin_file_number_ == 0) {
      return Status::Corruption(kErrorMessage,
                                "Invalid indirect blob file number");
    }
    if (flattened && file_number_ <= origin_file_number_) {
      return Status::Corruption(kErrorMessage,
                                "Flattened blob generation is not above its "
                                "origin");
    }
    // The generation is at least the origin, so this also bounds the origin.
    if (file_number_ > kMaxFileNumber) {
      return Status::Corruption(kErrorMessage,
                                "Indirect blob file number is too large");
    }
    if (slice.empty()) {
      return Status::Corruption(kErrorMessage,
                                "Missing indirect blob compression");
    }
    compression_ =
        static_cast<CompressionType>(static_cast<unsigned char>(slice[0]));
    slice.remove_prefix(1);
    if (!IsValidCompression(compression_)) {
      return Status::Corruption(kErrorMessage, "Invalid blob compression type");
    }
    if (!GetFixed32(&slice, &stable_checksum_)) {
      return Status::Corruption(kErrorMessage, "Corrupted stable checksum");
    }
    // For kNoCompression, stored_checksum() derives the stored CRC.
    if (compression_ != kNoCompression &&
        !GetFixed32(&slice, &stored_checksum_)) {
      return Status::Corruption(kErrorMessage, "Corrupted stored checksum");
    }
    if (!slice.empty()) {
      return Status::Corruption(kErrorMessage, "Trailing indirect blob bytes");
    }
    return Status::OK();
  }

  void Reset() {
    type_ = Type::kUnknown;
    expiration_ = 0;
    value_.clear();
    file_number_ = 0;
    origin_file_number_ = 0;
    offset_ = 0;
    size_ = 0;
    stable_checksum_ = 0;
    stored_checksum_ = 0;
    compression_ = kNoCompression;
  }

  Type type_ = Type::kUnknown;
  // compression_ sits next to type_ (both are one byte), so they share one
  // 8-byte slot and BlobIndex stays at 72 bytes on 64-bit builds.
  CompressionType compression_ = kNoCompression;
  uint64_t expiration_ = 0;
  Slice value_;
  uint64_t file_number_ = 0;
  uint64_t origin_file_number_ = 0;
  uint64_t offset_ = 0;
  uint64_t size_ = 0;
  uint32_t stable_checksum_ = 0;
  // The encoded stored CRC. It stays 0 for kNoCompression; see
  // stored_checksum().
  uint32_t stored_checksum_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
