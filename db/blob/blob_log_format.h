//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Log format information shared by reader and writer.

#pragma once

#include <memory>
#include <utility>

#include "db/blob/blob_constants.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

constexpr uint32_t kMagicNumber = 2395959;  // 0x00248f37
// Version 1: the base blob file format.
constexpr uint32_t kVersion1 = 1;
// Version 2: written for the blob files of blob indirection
// (enable_blob_indirection). Like version 1, but the file has no TTL, and the
// header stores the file's own number (see the header format below). The
// footer is the same as in version 1.
constexpr uint32_t kVersion2 = 2;

using ExpirationRange = std::pair<uint64_t, uint64_t>;

// clang-format off

// Format of blob log file header (30 bytes):
//
//    +--------------+---------+---------+-------+-------------+-------------------+
//    | magic number | version |  cf id  | flags | compression | expiration range  |
//    +--------------+---------+---------+-------+-------------+-------------------+
//    |   Fixed32    | Fixed32 | Fixed32 | char  |    char     | Fixed64   Fixed64 |
//    +--------------+---------+---------+-------+-------------+-------------------+
//
// List of flags:
//   has_ttl: Whether the file contain TTL data.
//
// Version 1: the expiration range in the header is a rough range based on
// blob_db_options.ttl_range_secs.
// Version 2: the file has no TTL, and the flags byte is 0. The first
// expiration-range field holds the file's own number (not 0), and the second
// one holds 0. A reader compares the number with the number in the file name,
// so a blob file that was copied or renamed to another number fails to open.
// Only blob indirection writes version 2, so a tool can tell from the version
// alone, without the MANIFEST, that the file belongs to blob indirection.
//
// DecodeFrom() returns Corruption for an unknown version and, in version 2, for
// a nonzero flags byte, file number 0 or a nonzero second field.

// clang-format on

struct BlobLogHeader {
  static constexpr size_t kSize = 30;

  BlobLogHeader() = default;
  BlobLogHeader(uint32_t _column_family_id, CompressionType _compression,
                bool _has_ttl, const ExpirationRange& _expiration_range)
      : column_family_id(_column_family_id),
        compression(_compression),
        has_ttl(_has_ttl),
        expiration_range(_expiration_range) {}

  uint32_t version = kVersion1;
  uint32_t column_family_id = 0;
  CompressionType compression = kNoCompression;
  bool has_ttl = false;
  // Version 2 only: the file's own number.
  uint64_t file_number = kInvalidBlobFileNumber;
  ExpirationRange expiration_range;

  void EncodeTo(std::string* dst) const;

  Status DecodeFrom(Slice slice);
};

// clang-format off

// Format of blob log file footer (32 bytes):
//
//    +--------------+------------+-------------------+------------+
//    | magic number | blob count | expiration range  | footer CRC |
//    +--------------+------------+-------------------+------------+
//    |   Fixed32    |  Fixed64   | Fixed64 + Fixed64 |   Fixed32  |
//    +--------------+------------+-------------------+------------+
//
// The footer will be presented only when the blob file is properly closed.
//
// Unlike the same field in file header, expiration range in the footer is the
// range of smallest and largest expiration of the data in this file.

// clang-format on

struct BlobLogFooter {
  static constexpr size_t kSize = 32;

  uint64_t blob_count = 0;
  ExpirationRange expiration_range = std::make_pair(0, 0);
  uint32_t crc = 0;

  void EncodeTo(std::string* dst);

  Status DecodeFrom(Slice slice);
};

// clang-format off

// Blob record format (32 bytes header + key + value):
//
//    +------------+--------------+------------+------------+----------+---------+-----------+
//    | key length | value length | expiration | header CRC | blob CRC |   key   |   value   |
//    +------------+--------------+------------+------------+----------+---------+-----------+
//    |   Fixed64  |   Fixed64    |  Fixed64   |  Fixed32   | Fixed32  | key len | value len |
//    +------------+--------------+------------+------------+----------+---------+-----------+
//
// If file has has_ttl = false, expiration field is always 0, and the blob
// doesn't has expiration.
//
// Also note that if compression is used, value is compressed value and value
// length is compressed value length.
//
// Header CRC is the checksum of (key_len + val_len + expiration), while
// blob CRC is the checksum of (key + value).
//
// We could use variable length encoding (Varint64) to save more space, but it
// make reader more complicated.

// clang-format on

struct BlobLogRecord {
  // header include fields up to blob CRC
  static constexpr size_t kHeaderSize = 32;

  // Note that the offset field of BlobIndex actually points to the blob value
  // as opposed to the start of the blob record. The following method can
  // be used to calculate the adjustment needed to read the blob record header.
  static constexpr uint64_t CalculateAdjustmentForRecordHeader(
      uint64_t key_size) {
    return key_size + kHeaderSize;
  }

  uint64_t key_size = 0;
  uint64_t value_size = 0;
  uint64_t expiration = 0;
  uint32_t header_crc = 0;
  uint32_t blob_crc = 0;
  Slice key;
  Slice value;
  std::unique_ptr<char[]> key_buf;
  std::unique_ptr<char[]> value_buf;

  uint64_t record_size() const { return kHeaderSize + key_size + value_size; }

  // The base signature: EncodeHeaderTo(dst, /*value_crc32c=*/nullptr).
  void EncodeHeaderTo(std::string* dst);

  // Encodes the record header into `dst`. The header ends with the record CRC
  // (blob_crc), which covers the key and the value. `value_crc32c` is optional:
  // when it is not null, it must be the CRC32C of `value`, already computed by
  // the caller. The record CRC then combines it with the key CRC and does not
  // read the value again.
  void EncodeHeaderTo(std::string* dst, const uint32_t* value_crc32c);

  Status DecodeHeaderFrom(Slice src);

  Status CheckBlobCRC() const;
};

// Checks whether a blob offset is potentially valid or not.
inline bool IsValidBlobOffset(uint64_t value_offset, uint64_t key_size,
                              uint64_t value_size, uint64_t file_size,
                              bool has_footer = true) {
  constexpr uint64_t kMinPrefix =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize;
  if (value_offset < kMinPrefix) {
    return false;
  }
  if (value_offset - kMinPrefix < key_size) {
    return false;
  }

  const uint64_t footer_size = has_footer ? BlobLogFooter::kSize : 0;
  if (file_size < footer_size) {
    return false;
  }
  const uint64_t max_value_end = file_size - footer_size;
  if (value_size > max_value_end) {
    return false;
  }
  if (value_offset > max_value_end - value_size) {
    return false;
  }

  return true;
}

}  // namespace ROCKSDB_NAMESPACE
