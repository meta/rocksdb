//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include <cinttypes>
#include <cstdint>
#include <string>
#include <utility>

#include "block_fetcher.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "memory/memory_allocator_impl.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "options/options_helper.h"
#include "port/likely.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/persistent_cache_helper.h"
#include "test_util/sync_point.h"
#include "unique_id_impl.h"
#include "util/aligned_buffer.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/hash.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

const char* const kHostnameForDbHostId = "__hostname__";

bool ShouldReportDetailedTime(Env* env, Statistics* stats) {
  return env != nullptr && stats != nullptr &&
         stats->get_stats_level() > kExceptDetailedTimers;
}

Status ReadTableMagicNumber(const IOOptions& io_options,
                            FSRandomAccessFile& file, uint64_t file_size,
                            uint64_t* magic_number) {
  *magic_number = 0;
  if (file_size < kMagicNumberLengthByte) {
    return Status::OK();
  }
  char scratch[kMagicNumberLengthByte];
  char* read_scratch = scratch;
  uint64_t read_offset = file_size - kMagicNumberLengthByte;
  size_t prefix_size = 0;
  size_t read_size = kMagicNumberLengthByte;
  AlignedBuffer aligned_buffer;
  if (file.use_direct_io()) {
    const size_t alignment = file.GetRequiredBufferAlignment();
    const uint64_t aligned_offset = read_offset & ~(uint64_t{alignment} - 1);
    prefix_size = static_cast<size_t>(read_offset - aligned_offset);
    read_offset = aligned_offset;
    read_size = Roundup(prefix_size + kMagicNumberLengthByte, alignment);
    aligned_buffer.Alignment(alignment);
    aligned_buffer.AllocateNewBuffer(read_size);
    read_scratch = aligned_buffer.BufferStart();
  }
  Slice result;
  Status status = file.Read(read_offset, read_size, io_options, &result,
                            read_scratch, nullptr);
  if (!status.ok()) {
    return status;
  }
  if (result.size() < prefix_size + kMagicNumberLengthByte) {
    return Status::Corruption("Truncated table format signature");
  }
  *magic_number = DecodeFixed64(result.data() + prefix_size);
  return Status::OK();
}

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~uint64_t{0});
  assert(size_ != ~uint64_t{0});
  PutVarint64Varint64(dst, offset_, size_);
}

char* BlockHandle::EncodeTo(char* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~uint64_t{0});
  assert(size_ != ~uint64_t{0});
  char* cur = EncodeVarint64(dst, offset_);
  cur = EncodeVarint64(cur, size_);
  return cur;
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    // reset in case failure after partially decoding
    offset_ = 0;
    size_ = 0;
    return Status::Corruption("bad block handle");
  }
}

Status BlockHandle::DecodeSizeFrom(uint64_t _offset, Slice* input) {
  if (GetVarint64(input, &size_)) {
    offset_ = _offset;
    return Status::OK();
  } else {
    // reset in case failure after partially decoding
    offset_ = 0;
    size_ = 0;
    return Status::Corruption("bad block handle");
  }
}

// Return a string that contains the copy of handle.
std::string BlockHandle::ToString(bool hex) const {
  std::string handle_str;
  EncodeTo(&handle_str);
  if (hex) {
    return Slice(handle_str).ToString(true);
  } else {
    return handle_str;
  }
}

const BlockHandle BlockHandle::kNullBlockHandle(0, 0);

void IndexValue::EncodeTo(std::string* dst, bool have_first_key,
                          const BlockHandle* previous_handle,
                          bool use_value_delta_escape) const {
  if (previous_handle) {
    // WART: this is specific to Block-based table
    const uint64_t contiguous_offset = previous_handle->offset() +
                                       previous_handle->size() +
                                       BlockBasedTable::kBlockTrailerSize;
    if (use_value_delta_escape) {
      // format_version >= 8: reserve varint 0 as an escape so a non-contiguous
      // handle can still be a (key-delta-encoded) non-restart entry. Genuine
      // size deltas are remapped to zigzag(delta)+1 (always >= 1) to keep 0
      // free.
      if (handle.offset() == contiguous_offset) {
        int64_t size_delta = static_cast<int64_t>(handle.size()) -
                             static_cast<int64_t>(previous_handle->size());
        // +1 could only overflow for |size_delta| ~ 2^63, which is impossible
        // for block-size deltas (block size is 32-bit).
        assert(i64ToZigzag(size_delta) != ~uint64_t{0});
        PutVarint64(dst, i64ToZigzag(size_delta) + 1);
      } else {
        // Escape: a full BlockHandle (offset + size) follows.
        TEST_SYNC_POINT("IndexValue::EncodeTo:ValueDeltaEscape");
        PutVarint64(dst, 0);
        handle.EncodeTo(dst);
      }
    } else {
      assert(handle.offset() == contiguous_offset);
      PutVarsignedint64(dst, handle.size() - previous_handle->size());
    }
  } else {
    handle.EncodeTo(dst);
  }
  assert(dst->size() != 0);

  if (have_first_key) {
    PutLengthPrefixedSlice(dst, first_internal_key);
  }
}

Status IndexValue::DecodeFrom(Slice* input, bool have_first_key,
                              const BlockHandle* previous_handle,
                              bool use_value_delta_escape) {
  if (previous_handle) {
    // WART: this is specific to Block-based table
    const uint64_t contiguous_offset = previous_handle->offset() +
                                       previous_handle->size() +
                                       BlockBasedTable::kBlockTrailerSize;
    if (use_value_delta_escape) {
      uint64_t u;
      if (!GetVarint64(input, &u)) {
        return Status::Corruption("bad delta-encoded index value");
      }
      if (u == 0) {
        // Escape: a full BlockHandle (offset + size) follows.
        Status s = handle.DecodeFrom(input);
        if (!s.ok()) {
          return s;
        }
      } else {
        int64_t delta = zigzagToI64(u - 1);
        handle =
            BlockHandle(contiguous_offset, previous_handle->size() + delta);
      }
    } else {
      int64_t delta;
      if (!GetVarsignedint64(input, &delta)) {
        return Status::Corruption("bad delta-encoded index value");
      }
      handle = BlockHandle(contiguous_offset, previous_handle->size() + delta);
    }
  } else {
    Status s = handle.DecodeFrom(input);
    if (!s.ok()) {
      return s;
    }
  }

  if (!have_first_key) {
    first_internal_key = Slice();
  } else if (!GetLengthPrefixedSlice(input, &first_internal_key)) {
    return Status::Corruption("bad first key in block info");
  }

  return Status::OK();
}

std::string IndexValue::ToString(bool hex, bool have_first_key) const {
  std::string s;
  EncodeTo(&s, have_first_key, nullptr);
  if (hex) {
    return Slice(s).ToString(true);
  } else {
    return s;
  }
}

namespace {
inline bool IsLegacyFooterFormat(uint64_t magic_number) {
  return magic_number == kLegacyPlainTableMagicNumber;
}
// Used when reading format_version=0 footers (plain tables)
inline uint64_t UpconvertLegacyFooterFormat(uint64_t magic_number) {
  if (magic_number == kLegacyPlainTableMagicNumber) {
    return kPlainTableMagicNumber;
  }
  assert(false);
  return magic_number;
}
// Used by plain tables to write format_version=0 footers
inline uint64_t DownconvertToLegacyFooterFormat(uint64_t magic_number) {
  if (magic_number == kPlainTableMagicNumber) {
    return kLegacyPlainTableMagicNumber;
  }
  assert(false);
  return magic_number;
}
inline uint8_t BlockTrailerSizeForMagicNumber(uint64_t magic_number) {
  if (magic_number == kBlockBasedTableMagicNumber) {
    return static_cast<uint8_t>(BlockBasedTable::kBlockTrailerSize);
  } else {
    return 0;
  }
}

// NOTE: format_version 0 is still used by plain tables and format_version 1 by
// cuckoo table. For block-based tables, format_version < 2 is no longer
// supported for reading or writing. Legacy magic numbers on block-based tables
// are used only for good error reporting.
//
// Footer format, in three parts:
// * Part1
//   -> format_version == 0 (inferred from legacy magic number)
//      <empty> (0 bytes)
//   -> format_version >= 1
//      checksum type (char, 1 byte)
// * Part2
//   -> format_version <= 5
//      metaindex handle (varint64 offset, varint64 size)
//      index handle     (varint64 offset, varint64 size)
//      <zero padding> for part2 size = 2 * BlockHandle::kMaxEncodedLength = 40
//        - This padding is unchecked/ignored
//   -> format_version >= 6
//      extended magic number (4 bytes) = 0x3e 0x00 0x7a 0x00
//        - Also surely invalid (size 0) handles if interpreted as older version
//        - (Helps ensure a corrupted format_version doesn't get us far with no
//           footer checksum.)
//      footer_checksum (uint32LE, 4 bytes)
//        - Checksum of above checksum type of whole footer, with this field
//          set to all zeros.
//      base_context_checksum (uint32LE, 4 bytes)
//      metaindex block size (& offset from footer) (uint32LE, 4 bytes)
//        -> format_version < 8
//           - Assumed to be immediately before footer, < 4GB
//        -> format_version >= 8
//           - Top 16 bits are number of bytes gap between start of footer and
//             end of metaindex block (space for future file checksum data)
//      <zero padding> (16 bytes, reserved for future use)
//      incompatible feature flags (uint64LE, 8 bytes)
//        - Brings part2 size also to 40 bytes
//        - Unknown non-zero bits are rejected by readers
// * Part3
//   -> format_version == 0 (inferred from legacy magic number)
//      legacy magic number (8 bytes)
//   -> format_version >= 1 (inferred from NOT legacy magic number)
//      format_version (uint32LE, 4 bytes), also called "footer version"
//      newer magic number (8 bytes)
const std::array<char, 4> kExtendedMagic{{0x3e, 0x00, 0x7a, 0x00}};
constexpr size_t kFooterPart2Size = 2 * BlockHandle::kMaxEncodedLength;
}  // namespace

Status FooterBuilder::Build(uint64_t magic_number, uint32_t format_version,
                            uint64_t footer_offset, ChecksumType checksum_type,
                            const BlockHandle& metaindex_handle,
                            const BlockHandle& index_handle,
                            uint32_t base_context_checksum,
                            uint64_t incompatible_features) {
  assert(magic_number != Footer::kNullTableMagicNumber);
  assert(IsSupportedFormatVersionForWrite(magic_number, format_version) ||
         TEST_AllowUnsupportedFormatVersion());
  if (format_version < 6 && incompatible_features != 0) {
    return Status::InvalidArgument(
        "incompatible footer features require format_version >= 6");
  }
  if ((incompatible_features & ~kKnownFooterFeatures) != 0) {
    return Status::InvalidArgument(
        "unsupported incompatible footer features: " +
        std::to_string(incompatible_features & ~kKnownFooterFeatures));
  }

  char* part2;
  char* part3;
  if (format_version > 0) {
    slice_ = Slice(data_.data(), Footer::kNewVersionsEncodedLength);
    // Generate parts 1 and 3
    char* cur = data_.data();
    // Part 1
    *(cur++) = checksum_type;
    // Part 2
    part2 = cur;
    // Skip over part 2 for now
    cur += kFooterPart2Size;
    // Part 3
    part3 = cur;
    EncodeFixed32(cur, format_version);
    cur += 4;
    EncodeFixed64(cur, magic_number);
    assert(cur + 8 == slice_.data() + slice_.size());
  } else {
    // format_version == 0 is used by plain tables
    slice_ = Slice(data_.data(), Footer::kVersion0EncodedLength);
    // Legacy SST files use kCRC32c checksum but it's not stored in footer.
    assert(checksum_type == kNoChecksum || checksum_type == kCRC32c);
    // Generate part 3 (part 1 empty, skip part 2 for now)
    part2 = data_.data();
    part3 = part2 + kFooterPart2Size;
    char* cur = part3;
    // Use legacy magic numbers to indicate format_version=0, for
    // compatibility. No other cases should use format_version=0.
    EncodeFixed64(cur, DownconvertToLegacyFooterFormat(magic_number));
    assert(cur + 8 == slice_.data() + slice_.size());
  }

  if (format_version >= 6) {
    if (BlockTrailerSizeForMagicNumber(magic_number) != 0) {
      // base context checksum required for table formats with block checksums
      assert(base_context_checksum != 0);
      assert(ChecksumModifierForContext(base_context_checksum, 0) != 0);
    } else {
      // base context checksum not used
      assert(base_context_checksum == 0);
      assert(ChecksumModifierForContext(base_context_checksum, 0) == 0);
    }

    // Start populating Part 2
    char* cur = data_.data() + /* part 1 size */ 1;
    // Set extended magic of part2
    std::copy(kExtendedMagic.begin(), kExtendedMagic.end(), cur);
    cur += kExtendedMagic.size();
    // Fill checksum data with zeros (for later computing checksum)
    char* checksum_data = cur;
    EncodeFixed32(cur, 0);
    cur += 4;
    // Save base context checksum
    EncodeFixed32(cur, base_context_checksum);
    cur += 4;
    // Compute and save the metaindex block size, and (format_version >= 8) the
    // gap between the end of the metaindex block and the start of the footer.
    const uint64_t metaindex_block_end =
        metaindex_handle.offset() + metaindex_handle.size() +
        BlockTrailerSizeForMagicNumber(magic_number);
    if (FormatVersionUsesMetaindexGap(format_version)) {
      // Low 16 bits: metaindex block size. High 16 bits: gap in bytes.
      if (metaindex_handle.size() > 0xFFFF) {
        return Status::NotSupported(
            "Metaindex block size > 64KB with format_version >= 8");
      }
      uint32_t metaindex_size = static_cast<uint32_t>(metaindex_handle.size());
      uint64_t gap = 0;
      if (metaindex_size != 0) {
        assert(metaindex_block_end <= footer_offset);
        gap = footer_offset - metaindex_block_end;
      }
      if (gap > 0xFFFF) {
        return Status::NotSupported(
            "Gap between metaindex block and footer > 64KB");
      }
      EncodeFixed32(cur, (static_cast<uint32_t>(gap) << 16) | metaindex_size);
    } else {
      uint32_t metaindex_size = static_cast<uint32_t>(metaindex_handle.size());
      if (metaindex_size != metaindex_handle.size()) {
        return Status::NotSupported("Metaindex block size > 4GB");
      }
      // Metaindex must be adjacent to footer
      assert(metaindex_size == 0 || metaindex_block_end == footer_offset);
      EncodeFixed32(cur, metaindex_size);
    }
    cur += 4;

    // Zero pad reserved bytes, then write incompatible feature flags. Older
    // readers reject any non-zero flags.
    std::fill_n(cur, 16U, char{0});
    cur += 16;
    EncodeFixed64(cur, incompatible_features);
    cur += sizeof(uint64_t);
    assert(cur == part3);

    // Compute checksum, add context
    uint32_t checksum = ComputeBuiltinChecksum(
        checksum_type, data_.data(), Footer::kNewVersionsEncodedLength);
    checksum +=
        ChecksumModifierForContext(base_context_checksum, footer_offset);
    // Store it
    EncodeFixed32(checksum_data, checksum);
  } else {
    // Base context checksum not used
    assert(!FormatVersionUsesContextChecksum(format_version));
    // Should be left empty
    assert(base_context_checksum == 0);
    assert(ChecksumModifierForContext(base_context_checksum, 0) == 0);

    // Populate all of part 2
    char* cur = part2;
    cur = metaindex_handle.EncodeTo(cur);
    cur = index_handle.EncodeTo(cur);
    // Zero pad remainder
    std::fill(cur, part3, char{0});
  }
  return Status::OK();
}

Status Footer::DecodeFrom(Slice input, uint64_t input_offset,
                          uint64_t enforce_table_magic_number) {
  // Only decode to unused Footer
  assert(table_magic_number_ == kNullTableMagicNumber);
  assert(input != nullptr);
  assert(input.size() >= kMinEncodedLength);

  const char* magic_ptr = input.data() + input.size() - kMagicNumberLengthByte;
  uint64_t magic = DecodeFixed64(magic_ptr);

  // Legacy block-based tables (format_version < 2) are no longer supported.
  // (This constant is only used here and in the corresponding test.)
  if (magic == 0xdb4775248b80fb57ull) {
    return Status::NotSupported(
        "Unsupported legacy magic number for block-based SST format. Load with "
        "RocksDB >= 4.6.0 and < 11.0.0 and run full compaction to upgrade.");
  }

  // Check for legacy formats
  bool legacy = IsLegacyFooterFormat(magic);
  if (legacy) {
    // Legacy plain tables are still supported - upconvert magic
    magic = UpconvertLegacyFooterFormat(magic);
  }
  if (enforce_table_magic_number != 0 && enforce_table_magic_number != magic) {
    return Status::Corruption("Bad table magic number: expected " +
                              std::to_string(enforce_table_magic_number) +
                              ", found " + std::to_string(magic));
  }
  table_magic_number_ = magic;
  block_trailer_size_ = BlockTrailerSizeForMagicNumber(magic);

  // Parse Part3
  const char* part3_ptr = magic_ptr;
  uint32_t computed_checksum = 0;
  uint64_t footer_offset = 0;
  if (legacy) {
    // Legacy format (format_version=0, used by plain tables)
    // The size is already asserted to be at least kMinEncodedLength
    // at the beginning of the function
    input.remove_prefix(input.size() - kVersion0EncodedLength);
    format_version_ = 0 /* legacy */;
    checksum_type_ = kCRC32c;
  } else {
    part3_ptr = magic_ptr - 4;
    format_version_ = DecodeFixed32(part3_ptr);
    if (UNLIKELY(!IsSupportedFormatVersionForRead(magic, format_version_) &&
                 !TEST_AllowUnsupportedFormatVersion())) {
      return Status::Corruption("Corrupt or unsupported format_version " +
                                std::to_string(format_version_) +
                                " for magic " + std::to_string(magic));
    }
    // All known format versions >= 1 occupy exactly this many bytes.
    if (UNLIKELY(input.size() < kNewVersionsEncodedLength)) {
      return Status::Corruption("Input is too short to be an SST file");
    }
    uint64_t adjustment = input.size() - kNewVersionsEncodedLength;
    input.remove_prefix(adjustment);
    footer_offset = input_offset + adjustment;

    // Parse Part1
    char chksum = input.data()[0];
    checksum_type_ = lossless_cast<ChecksumType>(chksum);
    if (UNLIKELY(!IsSupportedChecksumType(checksum_type()))) {
      return Status::Corruption("Corrupt or unsupported checksum type: " +
                                std::to_string(lossless_cast<uint8_t>(chksum)));
    }
    // This is the most convenient place to compute the checksum
    if (checksum_type_ != kNoChecksum && format_version_ >= 6) {
      std::array<char, kNewVersionsEncodedLength> copy_without_checksum;
      std::copy_n(input.data(), kNewVersionsEncodedLength,
                  copy_without_checksum.data());
      EncodeFixed32(&copy_without_checksum[5], 0);  // Clear embedded checksum
      computed_checksum =
          ComputeBuiltinChecksum(checksum_type(), copy_without_checksum.data(),
                                 kNewVersionsEncodedLength);
    }
    // Consume checksum type field
    input.remove_prefix(1);
  }

  // Parse Part2
  if (format_version_ >= 6) {
    Slice ext_magic(input.data(), 4);
    if (UNLIKELY(ext_magic.compare(Slice(kExtendedMagic.data(),
                                         kExtendedMagic.size())) != 0)) {
      return Status::Corruption("Bad extended magic number: 0x" +
                                ext_magic.ToString(/*hex*/ true));
    }
    input.remove_prefix(4);
    uint32_t stored_checksum = 0;
    uint32_t metaindex_size = 0;
    bool success;
    success = GetFixed32(&input, &stored_checksum);
    assert(success);
    success = GetFixed32(&input, &base_context_checksum_);
    assert(success);
    if (UNLIKELY(ChecksumModifierForContext(base_context_checksum_, 0) == 0)) {
      return Status::Corruption("Invalid base context checksum");
    }
    computed_checksum +=
        ChecksumModifierForContext(base_context_checksum_, footer_offset);
    if (UNLIKELY(computed_checksum != stored_checksum)) {
      return Status::Corruption("Footer at " + std::to_string(footer_offset) +
                                " checksum mismatch");
    }
    success = GetFixed32(&input, &metaindex_size);
    assert(success);
    (void)success;
    // For format_version >= 8, the high 16 bits are the gap (in bytes) between
    // the end of the metaindex block and the start of the footer (reserved for
    // future file checksum data), and the low 16 bits are the metaindex block
    // size. For older versions the whole field is the metaindex block size and
    // the metaindex is adjacent to the footer.
    uint64_t metaindex_gap = 0;
    if (FormatVersionUsesMetaindexGap(format_version_)) {
      metaindex_gap = metaindex_size >> 16;
      metaindex_size &= 0xFFFF;
    }
    uint64_t metaindex_end =
        footer_offset - GetBlockTrailerSize() - metaindex_gap;
    metaindex_handle_ =
        BlockHandle(metaindex_end - metaindex_size, metaindex_size);

    // Mark unpopulated
    index_handle_ = BlockHandle::NullBlockHandle();

    // 16 bytes of unchecked reserved padding
    input.remove_prefix(16U);

    success = GetFixed64(&input, &incompatible_features_);
    assert(success);
    const uint64_t unknown_features =
        incompatible_features_ & ~kKnownFooterFeatures;
    if (UNLIKELY(unknown_features != 0)) {
      return Status::NotSupported(
          "File uses unsupported incompatible footer features: " +
          std::to_string(unknown_features));
    }
    // End of part 2
    assert(input.data() == part3_ptr);
  } else {
    // format_version_ < 6
    Status result = metaindex_handle_.DecodeFrom(&input);
    if (result.ok()) {
      result = index_handle_.DecodeFrom(&input);
    }
    if (!result.ok()) {
      return result;
    }
    // Padding in part2 is ignored
  }
  return Status::OK();
}

std::string Footer::ToString() const {
  std::string result;
  result.reserve(1024);

  result.append("metaindex handle: " + metaindex_handle_.ToString() +
                " offset: " + std::to_string(metaindex_handle_.offset()) +
                " size: " + std::to_string(metaindex_handle_.size()) + "\n  ");
  result.append("index handle: " + index_handle_.ToString() +
                " offset: " + std::to_string(index_handle_.offset()) +
                " size: " + std::to_string(index_handle_.size()) + "\n  ");
  result.append("table_magic_number: " + std::to_string(table_magic_number_) +
                "\n  ");
  if (!IsLegacyFooterFormat(table_magic_number_)) {
    result.append("format version: " + std::to_string(format_version_) + "\n");
    if (incompatible_features_ != 0) {
      result.append("  incompatible features: " +
                    std::to_string(incompatible_features_) + "\n");
    }
  }
  return result;
}

bool& TEST_AllowUnsupportedFormatVersion() {
  static bool allow = false;
  return allow;
}

static Status ReadFooterFromFileInternal(
    const IOOptions& opts, RandomAccessFileReader* file, FileSystem& fs,
    FilePrefetchBuffer* prefetch_buffer, uint64_t expected_file_size,
    Footer* footer, uint64_t enforce_table_magic_number) {
  uint64_t file_size_from_file_system = 0;
  Status s = GetFileSizeFromOpenFileOrPath(file->file(), &fs, file->file_name(),
                                           &file_size_from_file_system, nullptr,
                                           FileSizeFallback::kAnyOpenFileError);
  if (!s.ok()) {
    return s;
  }

  if (expected_file_size != file_size_from_file_system) {
    // When file is opened during DB Open, the expected file size is from
    // manifest. Otherwise it is not guaranteed.
    return Status::Corruption("Sst file size mismatch between expected " +
                              std::to_string(expected_file_size) +
                              " and file system " +
                              std::to_string(file_size_from_file_system) +
                              " sstable: " + file->file_name());
  }

  if (expected_file_size < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short (" +
                              std::to_string(expected_file_size) +
                              " bytes) to be an "
                              "sstable: " +
                              file->file_name());
  }

  std::array<char, Footer::kMaxEncodedLength + 1> footer_buf;
  AlignedBuffer direct_io_buffer;
  Slice footer_input;
  uint64_t read_offset = (expected_file_size > Footer::kMaxEncodedLength)
                             ? expected_file_size - Footer::kMaxEncodedLength
                             : 0;
  // TODO: Need to pass appropriate deadline to TryReadFromCache(). Right now,
  // there is no readahead for point lookups, so TryReadFromCache will fail if
  // the required data is not in the prefetch buffer. Once deadline is enabled
  // for iterator, TryReadFromCache might do a readahead. Revisit to see if we
  // need to pass a timeout at that point
  // TODO: rate limit footer reads.
  if (prefetch_buffer == nullptr ||
      !prefetch_buffer->TryReadFromCache(opts, file, read_offset,
                                         Footer::kMaxEncodedLength,
                                         &footer_input, nullptr)) {
    FSReadRequest read_req;
    read_req.offset = read_offset;
    read_req.len = Footer::kMaxEncodedLength;
    if (file->use_direct_io()) {
      read_req.scratch = nullptr;
      AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
      file->Read(opts, &read_req, &direct_io_context);
    } else {
      read_req.scratch = footer_buf.data();
      file->Read(opts, &read_req);
    }
    s = std::move(read_req.status);
    if (!s.ok()) {
      return s;
    }
    footer_input = read_req.result;
  }

  TEST_SYNC_POINT_CALLBACK("ReadFooterFromFileInternal:0", &footer_input);

  // Check that we actually read the whole footer from the file.
  if (footer_input.size() < Footer::kMinEncodedLength) {
    return Status::Corruption(
        "The number of bytes read for Footer input " +
        std::to_string(footer_input.size()) +
        " is smaller than minimum footer encoded length: " +
        std::to_string(Footer::kMinEncodedLength) + " for file " +
        file->file_name() + "\n");
  }

  s = footer->DecodeFrom(footer_input, read_offset, enforce_table_magic_number);
  if (!s.ok()) {
    s = Status::CopyAppendMessage(s, " in ", file->file_name());
    return s;
  }
  return Status::OK();
}

Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FileSystem& fs, FilePrefetchBuffer* prefetch_buffer,
                          uint64_t expected_file_size, Footer* footer,
                          uint64_t enforce_table_magic_number,
                          Statistics* stats) {
  Status s = ReadFooterFromFileInternal(opts, file, fs, prefetch_buffer,
                                        expected_file_size, footer,
                                        enforce_table_magic_number);
  if (s.IsCorruption() &&
      CheckFSFeatureSupport(&fs, FSSupportedOps::kVerifyAndReconstructRead)) {
    IOOptions new_opts = opts;
    new_opts.verify_and_reconstruct_read = true;
    footer->Reset();
    s = ReadFooterFromFileInternal(new_opts, file, fs,
                                   /*prefetch_buffer=*/nullptr,
                                   expected_file_size, footer,
                                   enforce_table_magic_number);
    RecordTick(stats, FILE_READ_CORRUPTION_RETRY_COUNT);
    if (s.ok()) {
      RecordTick(stats, FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT);
    }
  }
  return s;
}

namespace {
// Custom handling for the last byte of a block, to avoid invoking streaming
// API to get an effective block checksum. This function is its own inverse
// because it uses xor.
inline uint32_t ModifyChecksumForLastByte(uint32_t checksum, char last_byte) {
  // This strategy bears some resemblance to extending a CRC checksum by one
  // more byte, except we don't need to re-mix the input checksum as long as
  // we do this step only once (per checksum).
  const uint32_t kRandomPrime = 0x6b9083d9;
  return checksum ^ lossless_cast<uint8_t>(last_byte) * kRandomPrime;
}
}  // namespace

uint32_t ComputeBuiltinChecksum(ChecksumType type, const char* data,
                                size_t data_size) {
  switch (type) {
    case kCRC32c:
      return crc32c::Mask(crc32c::Value(data, data_size));
    case kxxHash:
      return XXH32(data, data_size, /*seed*/ 0);
    case kxxHash64:
      return Lower32of64(XXH64(data, data_size, /*seed*/ 0));
    case kXXH3: {
      if (data_size == 0) {
        // Special case because of special handling for last byte, not
        // present in this case. Can be any value different from other
        // small input size checksums.
        return 0;
      } else {
        // See corresponding code in ComputeBuiltinChecksumWithLastByte
        uint32_t v = Lower32of64(XXH3_64bits(data, data_size - 1));
        return ModifyChecksumForLastByte(v, data[data_size - 1]);
      }
    }
    case kNoChecksum:
    default:
      return 0;
  }
}

uint32_t ComputeBuiltinChecksumWithLastByte(ChecksumType type, const char* data,
                                            size_t data_size, char last_byte) {
  switch (type) {
    case kCRC32c: {
      uint32_t crc = crc32c::Value(data, data_size);
      // Extend to cover last byte (compression type)
      crc = crc32c::Extend(crc, &last_byte, 1);
      return crc32c::Mask(crc);
    }
    case kxxHash: {
      XXH32_state_t* const state = XXH32_createState();
      XXH32_reset(state, 0);
      XXH32_update(state, data, data_size);
      // Extend to cover last byte (compression type)
      XXH32_update(state, &last_byte, 1);
      uint32_t v = XXH32_digest(state);
      XXH32_freeState(state);
      return v;
    }
    case kxxHash64: {
      XXH64_state_t* const state = XXH64_createState();
      XXH64_reset(state, 0);
      XXH64_update(state, data, data_size);
      // Extend to cover last byte (compression type)
      XXH64_update(state, &last_byte, 1);
      uint32_t v = Lower32of64(XXH64_digest(state));
      XXH64_freeState(state);
      return v;
    }
    case kXXH3: {
      // XXH3 is a complicated hash function that is extremely fast on
      // contiguous input, but that makes its streaming support rather
      // complex. It is worth custom handling of the last byte (`type`)
      // in order to avoid allocating a large state object and bringing
      // that code complexity into CPU working set.
      uint32_t v = Lower32of64(XXH3_64bits(data, data_size));
      return ModifyChecksumForLastByte(v, last_byte);
    }
    case kNoChecksum:
    default:
      return 0;
  }
}

Status DecompressBlockData(
    Decompressor::Args& args, Decompressor& decompressor,
    BlockContents* out_contents, const ImmutableOptions& ioptions,
    MemoryAllocator* allocator,
    ReadScopedBlockBufferProviderRef block_buffer_provider) {
  assert(args.compression_type != kNoCompression && "Invalid compression type");

  StopWatchNano timer(ioptions.clock,
                      ShouldReportDetailedTime(ioptions.env, ioptions.stats));

  Status s = decompressor.ExtractUncompressedSize(args);
  if (UNLIKELY(!s.ok())) {
    return s;
  }
  if (block_buffer_provider.has_value()) {
    // Compressed provider path: allocate the final uncompressed block backing
    // from the provider, then decompress directly into it.
    ReadScopedBlockBufferProvider::Lease read_scoped_lease;
    s = AllocateReadScopedBlockBuffer(block_buffer_provider->get(),
                                      args.uncompressed_size, 1,
                                      &read_scoped_lease);
    if (UNLIKELY(!s.ok())) {
      return s;
    }

    s = decompressor.DecompressBlock(args, read_scoped_lease.data);
    if (UNLIKELY(!s.ok())) {
      return s;
    }

    out_contents->data = Slice(read_scoped_lease.data, args.uncompressed_size);
    out_contents->cleanup = std::move(read_scoped_lease.cleanup);
    out_contents->backing_size = read_scoped_lease.size;
  } else {
    CacheAllocationPtr ubuf = AllocateBlock(args.uncompressed_size, allocator);

    s = decompressor.DecompressBlock(args, ubuf.get());
    if (UNLIKELY(!s.ok())) {
      return s;
    }

    *out_contents = BlockContents(std::move(ubuf), args.uncompressed_size);
  }

  if (ShouldReportDetailedTime(ioptions.env, ioptions.stats)) {
    RecordTimeToHistogram(ioptions.stats, DECOMPRESSION_TIMES_NANOS,
                          timer.ElapsedNanos());
  }
  RecordTick(ioptions.stats, BYTES_DECOMPRESSED_FROM,
             args.compressed_data.size());
  RecordTick(ioptions.stats, BYTES_DECOMPRESSED_TO, out_contents->data.size());
  RecordTick(ioptions.stats, NUMBER_BLOCK_DECOMPRESSED);
  PERF_COUNTER_ADD(block_decompress_count, 1);

  TEST_SYNC_POINT_CALLBACK("DecompressBlockData:TamperWithReturnValue",
                           static_cast<void*>(&s));
  TEST_SYNC_POINT_CALLBACK("DecompressBlockData:TamperWithDecompressionOutput",
                           static_cast<void*>(out_contents));

  return s;
}

Status DecompressBlockData(const char* data, size_t size, CompressionType type,
                           Decompressor& decompressor,
                           BlockContents* out_contents,
                           const ImmutableOptions& ioptions,
                           MemoryAllocator* allocator,
                           Decompressor::ManagedWorkingArea* working_area) {
  Decompressor::Args args;
  args.compressed_data = Slice(data, size);
  args.compression_type = type;
  args.working_area = working_area;
  return DecompressBlockData(args, decompressor, out_contents, ioptions,
                             allocator, std::nullopt);
}

Status DecompressSerializedBlock(
    const char* data, size_t size, CompressionType type,
    Decompressor& decompressor, BlockContents* out_contents,
    const ImmutableOptions& ioptions, MemoryAllocator* allocator,
    ReadScopedBlockBufferProviderRef block_buffer_provider) {
  assert(data[size] != kNoCompression);
  assert(data[size] == static_cast<char>(type));
  Decompressor::Args args;
  args.compressed_data = Slice(data, size);
  args.compression_type = type;
  return DecompressBlockData(args, decompressor, out_contents, ioptions,
                             allocator, block_buffer_provider);
}

Status DecompressBlockData(Decompressor::Args& args, Decompressor& decompressor,
                           BlockContents* out_contents,
                           const ImmutableOptions& ioptions,
                           MemoryAllocator* allocator) {
  return DecompressBlockData(args, decompressor, out_contents, ioptions,
                             allocator, std::nullopt);
}

Status DecompressSerializedBlock(
    Decompressor::Args& args, Decompressor& decompressor,
    BlockContents* out_contents, const ImmutableOptions& ioptions,
    MemoryAllocator* allocator,
    ReadScopedBlockBufferProviderRef block_buffer_provider) {
  assert(args.compressed_data.data()[args.compressed_data.size()] !=
         kNoCompression);
  assert(args.compressed_data.data()[args.compressed_data.size()] ==
         static_cast<char>(args.compression_type));
  return DecompressBlockData(args, decompressor, out_contents, ioptions,
                             allocator, block_buffer_provider);
}

// Replace the contents of db_host_id with the actual hostname, if db_host_id
// matches the keyword kHostnameForDbHostId
Status ReifyDbHostIdProperty(Env* env, std::string* db_host_id) {
  assert(db_host_id);
  if (*db_host_id == kHostnameForDbHostId) {
    Status s = env->GetHostNameString(db_host_id);
    if (!s.ok()) {
      db_host_id->clear();
    }
    return s;
  }

  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
