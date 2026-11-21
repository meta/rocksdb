// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_relocation_file.h"

#include <algorithm>
#include <limits>

#include "db/blob/blob_gen2_format.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint32_t kRelocationDirectoryMagic = 0x524C4344U;  // RLCD
// Header: magic (4) | format version (4) | origin count (8) | served file
// count (8) | total entries (8) | total record bytes (8) | directory digest
// (4) | header CRC (4).
constexpr size_t kDirectoryDigestOffset = 40;
constexpr size_t kHeaderChecksumOffset = 44;
constexpr size_t kDirectoryHeaderSize = 48;
// Origin entry: origin, first and last original offset, entry count and
// record bytes (8 each). Then one 8-byte number per served file.
constexpr size_t kOriginEntrySize = 40;
constexpr size_t kServedFileSize = sizeof(uint64_t);
// The digest covers the counts and totals (header bytes 8..39) and the body.
constexpr size_t kDigestedHeaderOffset = 8;

uint32_t ComputeDirectoryDigest(const char* header, const char* body,
                                size_t body_size) {
  const uint32_t header_crc =
      crc32c::Value(header + kDigestedHeaderOffset,
                    kDirectoryDigestOffset - kDigestedHeaderOffset);
  return crc32c::Extend(header_crc, body, body_size);
}

bool AddRelocationBytesWithoutOverflow(uint64_t lhs, uint64_t rhs,
                                       uint64_t* sum) {
  if (sum == nullptr || rhs > std::numeric_limits<uint64_t>::max() - lhs) {
    return false;
  }
  *sum = lhs + rhs;
  return true;
}

}  // namespace

void BlobRelocationDirectory::EncodeTo(std::string* output) const {
  assert(output != nullptr);
  output->clear();
  output->reserve(kDirectoryHeaderSize + origins_.size() * kOriginEntrySize +
                  served_file_numbers_.size() * kServedFileSize);
  PutFixed32(output, kRelocationDirectoryMagic);
  PutFixed32(output, kFormatVersion);
  PutFixed64(output, origins_.size());
  PutFixed64(output, served_file_numbers_.size());
  PutFixed64(output, total_entries_);
  PutFixed64(output, total_record_bytes_);
  // Directory digest and header CRC, filled in below.
  PutFixed32(output, 0);
  PutFixed32(output, 0);
  assert(output->size() == kDirectoryHeaderSize);

  for (const BlobRelocationOrigin& origin : origins_) {
    PutFixed64(output, origin.origin_file_number);
    PutFixed64(output, origin.first_original_offset);
    PutFixed64(output, origin.last_original_offset);
    PutFixed64(output, origin.entry_count);
    PutFixed64(output, origin.record_bytes);
  }
  for (uint64_t file_number : served_file_numbers_) {
    PutFixed64(output, file_number);
  }
  const uint32_t digest = ComputeDirectoryDigest(
      output->data(), output->data() + kDirectoryHeaderSize,
      output->size() - kDirectoryHeaderSize);
  EncodeFixed32(output->data() + kDirectoryDigestOffset, digest);
  EncodeFixed32(output->data() + kHeaderChecksumOffset,
                crc32c::Value(output->data(), kHeaderChecksumOffset));
}

Status BlobRelocationDirectory::DecodeFrom(const Slice& input,
                                           BlobRelocationDirectory* directory) {
  if (directory == nullptr) {
    return Status::InvalidArgument("Null blob relocation directory output");
  }
  if (input.size() < kDirectoryHeaderSize) {
    return Status::Corruption("Invalid blob relocation directory size");
  }

  Slice cursor = input;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint64_t origin_count = 0;
  uint64_t served_count = 0;
  uint32_t directory_digest = 0;
  uint32_t header_checksum = 0;
  BlobRelocationDirectory decoded;
  if (!GetFixed32(&cursor, &magic) || !GetFixed32(&cursor, &version) ||
      !GetFixed64(&cursor, &origin_count) ||
      !GetFixed64(&cursor, &served_count) ||
      !GetFixed64(&cursor, &decoded.total_entries_) ||
      !GetFixed64(&cursor, &decoded.total_record_bytes_) ||
      !GetFixed32(&cursor, &directory_digest) ||
      !GetFixed32(&cursor, &header_checksum)) {
    return Status::Corruption("Truncated blob relocation directory");
  }
  if (magic != kRelocationDirectoryMagic || version != kFormatVersion) {
    return Status::Corruption("Unsupported blob relocation directory");
  }
  // The size must be exactly what the counts say; this bounds both counts
  // by the input size, so no fixed limit is needed.
  if (origin_count == 0 || served_count == 0 ||
      origin_count > cursor.size() / kOriginEntrySize ||
      served_count > cursor.size() / kServedFileSize ||
      cursor.size() !=
          origin_count * kOriginEntrySize + served_count * kServedFileSize) {
    return Status::Corruption("Invalid blob relocation directory counts");
  }
  if (crc32c::Value(input.data(), kHeaderChecksumOffset) != header_checksum) {
    return Status::Corruption(
        "Blob relocation directory header checksum mismatch");
  }
  if (ComputeDirectoryDigest(input.data(), cursor.data(), cursor.size()) !=
      directory_digest) {
    return Status::Corruption("Blob relocation directory checksum mismatch");
  }

  decoded.origins_.reserve(static_cast<size_t>(origin_count));
  uint64_t computed_entries = 0;
  uint64_t computed_record_bytes = 0;
  for (uint64_t i = 0; i < origin_count; ++i) {
    BlobRelocationOrigin origin;
    if (!GetFixed64(&cursor, &origin.origin_file_number) ||
        !GetFixed64(&cursor, &origin.first_original_offset) ||
        !GetFixed64(&cursor, &origin.last_original_offset) ||
        !GetFixed64(&cursor, &origin.entry_count) ||
        !GetFixed64(&cursor, &origin.record_bytes)) {
      return Status::Corruption("Truncated blob relocation origin entry");
    }
    if (origin.origin_file_number == 0 || origin.entry_count == 0 ||
        origin.first_original_offset > origin.last_original_offset ||
        origin.entry_count - 1 >
            origin.last_original_offset - origin.first_original_offset ||
        origin.entry_count > origin.record_bytes / kSimpleGen2BlobTrailerSize ||
        (!decoded.origins_.empty() &&
         decoded.origins_.back().origin_file_number >=
             origin.origin_file_number) ||
        !AddRelocationBytesWithoutOverflow(computed_entries, origin.entry_count,
                                           &computed_entries) ||
        !AddRelocationBytesWithoutOverflow(computed_record_bytes,
                                           origin.record_bytes,
                                           &computed_record_bytes)) {
      return Status::Corruption("Invalid blob relocation origin entry");
    }
    decoded.origins_.push_back(origin);
  }
  decoded.served_file_numbers_.reserve(static_cast<size_t>(served_count));
  for (uint64_t i = 0; i < served_count; ++i) {
    uint64_t file_number = 0;
    if (!GetFixed64(&cursor, &file_number) || file_number == 0 ||
        (!decoded.served_file_numbers_.empty() &&
         decoded.served_file_numbers_.back() >= file_number)) {
      return Status::Corruption("Invalid blob relocation served file");
    }
    decoded.served_file_numbers_.push_back(file_number);
  }
  if (!cursor.empty() || computed_entries != decoded.total_entries_ ||
      computed_record_bytes != decoded.total_record_bytes_) {
    return Status::Corruption("Blob relocation directory totals mismatch");
  }
  decoded.directory_digest_ = directory_digest;
  decoded.encoded_size_ = input.size();
  *directory = std::move(decoded);
  return Status::OK();
}

Status BlobRelocationDirectory::ValidateForFileSize(uint64_t file_size) const {
  if (file_size < kBlobRelocationFilePrefixSize ||
      total_record_bytes_ > file_size - kBlobRelocationFilePrefixSize) {
    return Status::Corruption(
        "Blob relocation record bytes exceed physical file size");
  }
  if (encoded_size_ > file_size) {
    return Status::Corruption(
        "Blob relocation directory exceeds physical file size");
  }
  return Status::OK();
}

const BlobRelocationOrigin* BlobRelocationDirectory::Find(
    uint64_t origin_file_number) const {
  const auto it =
      std::lower_bound(origins_.begin(), origins_.end(), origin_file_number,
                       [](const BlobRelocationOrigin& origin, uint64_t value) {
                         return origin.origin_file_number < value;
                       });
  return it != origins_.end() && it->origin_file_number == origin_file_number
             ? &*it
             : nullptr;
}

Status BlobRelocationDirectoryBuilder::Add(uint64_t origin_file_number,
                                           uint64_t original_value_offset,
                                           uint64_t stored_value_size,
                                           CompressionType compression) {
  if (finished_) {
    return Status::InvalidArgument(
        "Cannot add to finished blob relocation directory");
  }
  if (origin_file_number == 0 || !CompressionTypeSupported(compression) ||
      stored_value_size >
          std::numeric_limits<uint64_t>::max() - kSimpleGen2BlobTrailerSize) {
    return Status::InvalidArgument("Invalid blob relocation record");
  }
  if (has_current_ && origin_file_number < current_.origin_file_number) {
    return Status::InvalidArgument("Blob relocation origins are not sorted");
  }
  if (has_current_ && origin_file_number == current_.origin_file_number &&
      original_value_offset <= current_.last_original_offset) {
    return Status::InvalidArgument("Blob relocation offsets are not unique");
  }
  if (has_current_ && origin_file_number == current_.origin_file_number &&
      original_value_offset < current_value_end_) {
    return Status::InvalidArgument("Blob relocation values overlap");
  }
  if (stored_value_size >
      std::numeric_limits<uint64_t>::max() - original_value_offset) {
    return Status::InvalidArgument("Blob relocation value end overflows");
  }
  const uint64_t value_end = original_value_offset + stored_value_size;
  const uint64_t record_bytes = stored_value_size + kSimpleGen2BlobTrailerSize;
  if (!has_current_ || origin_file_number != current_.origin_file_number) {
    Status status = FinishCurrentOrigin();
    if (!status.ok()) {
      return status;
    }
    BlobRelocationOrigin next;
    next.origin_file_number = origin_file_number;
    next.first_original_offset = original_value_offset;
    next.last_original_offset = original_value_offset;
    next.entry_count = 1;
    next.record_bytes = record_bytes;
    current_ = next;
    current_value_end_ = value_end;
    has_current_ = true;
    return Status::OK();
  }

  BlobRelocationOrigin next = current_;
  if (!AddRelocationBytesWithoutOverflow(current_.entry_count, 1,
                                         &next.entry_count) ||
      !AddRelocationBytesWithoutOverflow(current_.record_bytes, record_bytes,
                                         &next.record_bytes)) {
    return Status::InvalidArgument("Blob relocation directory overflow");
  }
  next.last_original_offset = original_value_offset;
  current_ = next;
  current_value_end_ = value_end;
  return Status::OK();
}

Status BlobRelocationDirectoryBuilder::AddServedFile(uint64_t file_number) {
  if (finished_) {
    return Status::InvalidArgument(
        "Cannot add to finished blob relocation directory");
  }
  if (file_number == 0) {
    return Status::InvalidArgument("Invalid blob relocation served file");
  }
  if (!served_file_numbers_.empty() &&
      served_file_numbers_.back() == file_number) {
    return Status::OK();
  }
  served_file_numbers_.push_back(file_number);
  return Status::OK();
}

Status BlobRelocationDirectoryBuilder::FinishCurrentOrigin() {
  if (!has_current_) {
    return Status::OK();
  }
  uint64_t total_entries = 0;
  uint64_t total_record_bytes = 0;
  if (!AddRelocationBytesWithoutOverflow(
          directory_.total_entries_, current_.entry_count, &total_entries) ||
      !AddRelocationBytesWithoutOverflow(directory_.total_record_bytes_,
                                         current_.record_bytes,
                                         &total_record_bytes)) {
    return Status::InvalidArgument("Blob relocation directory overflow");
  }
  directory_.origins_.push_back(current_);
  directory_.total_entries_ = total_entries;
  directory_.total_record_bytes_ = total_record_bytes;
  has_current_ = false;
  return Status::OK();
}

Status BlobRelocationDirectoryBuilder::Finish(
    BlobRelocationDirectory* directory, std::string* encoded) {
  if (directory == nullptr) {
    return Status::InvalidArgument("Null blob relocation directory output");
  }
  if (finished_) {
    return Status::InvalidArgument(
        "Blob relocation directory already finished");
  }
  Status status = FinishCurrentOrigin();
  if (!status.ok()) {
    return status;
  }
  if (directory_.origins_.empty()) {
    return Status::InvalidArgument("Empty blob relocation directory");
  }
  std::sort(served_file_numbers_.begin(), served_file_numbers_.end());
  served_file_numbers_.erase(
      std::unique(served_file_numbers_.begin(), served_file_numbers_.end()),
      served_file_numbers_.end());
  if (served_file_numbers_.empty()) {
    return Status::InvalidArgument("Blob relocation file serves no file");
  }
  directory_.served_file_numbers_ = served_file_numbers_;
  std::string encoded_directory;
  directory_.EncodeTo(&encoded_directory);
  status = BlobRelocationDirectory::DecodeFrom(encoded_directory, directory);
  if (status.ok()) {
    finished_ = true;
    if (encoded != nullptr) {
      *encoded = std::move(encoded_directory);
    }
  }
  return status;
}

void EncodeBlobRelocationKeyTo(uint64_t origin_file_number,
                               uint64_t original_value_offset, char* out) {
  EncodeFixed64(out, EndianSwapValue(origin_file_number));
  EncodeFixed64(out + sizeof(uint64_t), EndianSwapValue(original_value_offset));
}

std::string EncodeBlobRelocationKey(uint64_t origin_file_number,
                                    uint64_t original_value_offset) {
  std::string key(kBlobRelocationKeySize, '\0');
  EncodeBlobRelocationKeyTo(origin_file_number, original_value_offset,
                            key.data());
  return key;
}

Status DecodeBlobRelocationKey(const Slice& key, uint64_t* origin_file_number,
                               uint64_t* original_value_offset) {
  if (origin_file_number == nullptr || original_value_offset == nullptr) {
    return Status::InvalidArgument("Null blob relocation key output");
  }
  if (key.size() !=
      sizeof(*origin_file_number) + sizeof(*original_value_offset)) {
    return Status::Corruption("Invalid blob relocation key size");
  }
  *origin_file_number = EndianSwapValue(DecodeFixed64(key.data()));
  *original_value_offset =
      EndianSwapValue(DecodeFixed64(key.data() + sizeof(*origin_file_number)));
  if (*origin_file_number == 0) {
    return Status::Corruption("Invalid blob relocation origin");
  }
  return Status::OK();
}

BlockBasedTableOptions MakeBlobRelocationTableOptions(
    const BlockBasedTableOptions& source) {
  BlockBasedTableOptions options(source);
  // format_version 6 adds the per-file context to block and record checksums,
  // so a record copied to another offset fails its check. Relocation files are
  // a new file kind, so this has no compatibility cost.
  options.format_version = std::max<uint32_t>(options.format_version, 6);
  // Relocation files use kCRC32c for block and record checksums, whatever
  // checksum type the table options set, unless they set kNoChecksum. A record
  // trailer can then reuse the stored CRC32C of the value, so streaming a value
  // needs no second checksum pass over its payload. Normal SSTs and blob files
  // keep their configured checksum type.
  if (options.checksum != kNoChecksum) {
    options.checksum = kCRC32c;
  }
  options.index_type = BlockBasedTableOptions::kBinarySearch;
  // A lookup key usually lies between two location entry keys, so a data
  // block hash index (exact keys only) cannot help, and the index key of a
  // data block must be its last key (see "Table options" in the header). The
  // CF's restart interval is kept.
  options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinarySearch;
  options.index_shortening =
      BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
  // 4 KB is an estimate, not a measured best value. A small block makes each
  // lookup read fewer bytes (the records are outside the data blocks), but it
  // makes the index larger.
  options.block_size = 4 * 1024;
  // Alignment padding only wastes space between records and blocks here.
  options.block_align = false;
  options.super_block_alignment_size = 0;
  options.flush_block_policy_factory.reset();
  options.filter_policy.reset();
  options.partition_filters = false;
  options.user_defined_index_factory.reset();
  options.index_mode = BlockBasedTableOptions::IndexMode::kStandardOnly;
  options.use_udi_as_primary_index = false;
  options.fail_if_no_udi_on_open = false;
  // Index caching and pinning follow the CF's table options
  // (cache_index_and_filter_blocks, metadata_cache_options): a relocation
  // file is opened without a level, so it pins like a non-L0 SST.
  return options;
}

void EncodeBlobRelocationGroup(uint64_t first_record_offset,
                               const BlobRelocationGroupValue* values,
                               size_t count, std::string* output) {
  assert(values != nullptr);
  assert(count >= 1 && count <= kBlobRelocationGroupSize);
  assert(output != nullptr);
  output->clear();
  PutVarint32(output, static_cast<uint32_t>(count));
  PutVarint64(output, first_record_offset);
  // The end of the value before (0 before the first value).
  uint64_t previous_end = 0;
  for (size_t i = 0; i < count; ++i) {
    assert(values[i].original_value_offset >= previous_end);
    assert(values[i].stored_value_size >= 1 &&
           values[i].stored_value_size <= kMaxSimpleGen2BlobPayloadSize);
    PutVarint64(output, values[i].original_value_offset - previous_end);
    PutVarint64(output, values[i].stored_value_size);
    previous_end =
        values[i].original_value_offset + values[i].stored_value_size;
  }
}

const BlobRelocationGroup::Value* BlobRelocationGroup::Find(
    uint64_t original_value_offset) const {
  const Value* const end = values.data() + count;
  const Value* const found =
      std::lower_bound(values.data(), end, original_value_offset,
                       [](const Value& value, uint64_t offset) {
                         return value.original_value_offset < offset;
                       });
  return found != end && found->original_value_offset == original_value_offset
             ? found
             : nullptr;
}

Status DecodeBlobRelocationGroup(const Slice& value,
                                 uint64_t last_original_value_offset,
                                 BlobRelocationGroup* group) {
  assert(group != nullptr);
  Slice input = value;
  uint32_t count = 0;
  if (!GetVarint32(&input, &count) || count < 1 ||
      count > kBlobRelocationGroupSize) {
    return Status::Corruption("Invalid blob relocation location entry count");
  }
  BlobRelocationGroup decoded;
  uint64_t record_offset = 0;
  if (!GetVarint64(&input, &record_offset)) {
    return Status::Corruption("Invalid blob relocation location entry");
  }
  decoded.count = count;
  // The end of the value before (0 before the first value), and the original
  // offset of the current value.
  uint64_t previous_end = 0;
  uint64_t offset = 0;
  for (uint32_t i = 0; i < count; ++i) {
    uint64_t gap = 0;
    uint64_t size = 0;
    if (!GetVarint64(&input, &gap) || !GetVarint64(&input, &size)) {
      return Status::Corruption("Invalid blob relocation location entry");
    }
    if (gap > std::numeric_limits<uint64_t>::max() - previous_end) {
      return Status::Corruption(
          "Invalid blob relocation location entry offset gap");
    }
    if (size == 0 || size > kMaxSimpleGen2BlobPayloadSize) {
      return Status::Corruption(
          "Invalid blob relocation location entry stored size");
    }
    offset = previous_end + gap;
    if (size > std::numeric_limits<uint64_t>::max() - offset) {
      return Status::Corruption(
          "Invalid blob relocation location entry offset gap");
    }
    previous_end = offset + size;
    BlobRelocationGroup::Value& entry = decoded.values[i];
    entry.original_value_offset = offset;
    entry.record_offset = record_offset;
    entry.stored_value_size = size;
    const uint64_t record_bytes = size + kSimpleGen2BlobTrailerSize;
    if (record_offset > std::numeric_limits<uint64_t>::max() - record_bytes) {
      return Status::Corruption(
          "Invalid blob relocation location entry record offset");
    }
    record_offset += record_bytes;
  }
  if (!input.empty()) {
    return Status::Corruption("Blob relocation location entry has extra bytes");
  }
  if (offset != last_original_value_offset) {
    return Status::Corruption(
        "Blob relocation location entry key does not match its last value");
  }
  *group = decoded;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
