// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_relocation_file.h"

#include <algorithm>
#include <limits>

#include "db/blob/blob_gen2_format.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint32_t kRelocationDirectoryMagic = 0x524C4344U;  // RLCD
constexpr size_t kDirectoryHeaderSize = 40;
constexpr size_t kLegacyOriginEntrySize = 48;
constexpr size_t kOriginEntrySize = 56;
constexpr size_t kMaxDirectorySize = 64 * 1024 * 1024;

bool AddRelocationBytesWithoutOverflow(uint64_t lhs, uint64_t rhs,
                                       uint64_t* sum) {
  if (sum == nullptr || rhs > std::numeric_limits<uint64_t>::max() - lhs) {
    return false;
  }
  *sum = lhs + rhs;
  return true;
}

void PutBigEndian64(std::string* output, uint64_t value) {
  for (size_t i = 0; i < sizeof(value); ++i) {
    output->push_back(
        static_cast<char>(value >> (8 * (sizeof(value) - 1 - i))));
  }
}

uint64_t DecodeBigEndian64(const char* input) {
  uint64_t value = 0;
  for (size_t i = 0; i < sizeof(value); ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[i]);
  }
  return value;
}

void ExtendDigest(uint64_t original_value_offset,
                  uint64_t relocated_value_offset, uint64_t stored_value_size,
                  CompressionType compression, uint32_t* digest) {
  std::string encoded;
  encoded.reserve(sizeof(uint64_t) * 3 + 1);
  PutFixed64(&encoded, original_value_offset);
  PutFixed64(&encoded, relocated_value_offset);
  PutFixed64(&encoded, stored_value_size);
  encoded.push_back(static_cast<char>(compression));
  *digest = crc32c::Extend(*digest, encoded.data(), encoded.size());
}

}  // namespace

void BlobRelocationDirectory::EncodeTo(std::string* output) const {
  assert(output != nullptr);
  output->clear();
  uint32_t format_version = format_version_;
  TEST_SYNC_POINT_CALLBACK("BlobRelocationDirectory::EncodeTo:FormatVersion",
                           &format_version);
  assert(format_version >= kOldestReadableFormatVersion &&
         format_version <= kFormatVersion);
  size_t route_source_count = 0;
  if (format_version >= 2) {
    for (const BlobRelocationOrigin& origin : origins_) {
      route_source_count += origin.served_generation_file_numbers.size();
    }
  }
  const size_t origin_entry_size =
      format_version >= 2 ? kOriginEntrySize : kLegacyOriginEntrySize;
  output->reserve(kDirectoryHeaderSize + origins_.size() * origin_entry_size +
                  route_source_count * sizeof(uint64_t));
  PutFixed32(output, kRelocationDirectoryMagic);
  PutFixed32(output, format_version);
  PutFixed64(output, origins_.size());
  PutFixed64(output, total_entries_);
  PutFixed64(output, total_record_bytes_);
  PutFixed32(output, 0);
  PutFixed32(output, 0);
  assert(output->size() == kDirectoryHeaderSize);

  for (const BlobRelocationOrigin& origin : origins_) {
    PutFixed64(output, origin.origin_file_number);
    PutFixed64(output, origin.first_original_offset);
    PutFixed64(output, origin.last_original_offset);
    PutFixed64(output, origin.entry_count);
    PutFixed64(output, origin.record_bytes);
    PutFixed32(output, origin.entries_digest);
    PutFixed32(output, 0);
    if (format_version >= 2) {
      PutFixed64(output, origin.served_generation_file_numbers.size());
      for (uint64_t generation : origin.served_generation_file_numbers) {
        PutFixed64(output, generation);
      }
    }
  }
  const uint32_t digest = crc32c::Value(output->data() + kDirectoryHeaderSize,
                                        output->size() - kDirectoryHeaderSize);
  EncodeFixed32(output->data() + 32, digest);
  EncodeFixed32(output->data() + 36,
                crc32c::Value(output->data(), kDirectoryHeaderSize - 4));
}

Status BlobRelocationDirectory::DecodeFrom(const Slice& input,
                                           BlobRelocationDirectory* directory) {
  if (directory == nullptr) {
    return Status::InvalidArgument("Null blob relocation directory output");
  }
  if (input.size() < kDirectoryHeaderSize || input.size() > kMaxDirectorySize) {
    return Status::Corruption("Invalid blob relocation directory size");
  }

  Slice cursor = input;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint64_t origin_count = 0;
  uint32_t directory_digest = 0;
  uint32_t header_checksum = 0;
  BlobRelocationDirectory decoded;
  if (!GetFixed32(&cursor, &magic) || !GetFixed32(&cursor, &version) ||
      !GetFixed64(&cursor, &origin_count) ||
      !GetFixed64(&cursor, &decoded.total_entries_) ||
      !GetFixed64(&cursor, &decoded.total_record_bytes_) ||
      !GetFixed32(&cursor, &directory_digest) ||
      !GetFixed32(&cursor, &header_checksum)) {
    return Status::Corruption("Truncated blob relocation directory");
  }
  if (magic != kRelocationDirectoryMagic ||
      version < kOldestReadableFormatVersion || version > kFormatVersion) {
    return Status::Corruption("Unsupported blob relocation directory");
  }
  const size_t minimum_origin_entry_size =
      version >= 2 ? kOriginEntrySize : kLegacyOriginEntrySize;
  if (origin_count == 0 || origin_count > kMaxOrigins ||
      origin_count > std::numeric_limits<size_t>::max() ||
      origin_count > (kMaxDirectorySize - kDirectoryHeaderSize) /
                         minimum_origin_entry_size ||
      cursor.size() < origin_count * minimum_origin_entry_size) {
    return Status::Corruption("Invalid blob relocation origin count");
  }
  if (crc32c::Value(input.data(), kDirectoryHeaderSize - 4) !=
      header_checksum) {
    return Status::Corruption(
        "Blob relocation directory header checksum mismatch");
  }
  if (crc32c::Value(cursor.data(), cursor.size()) != directory_digest) {
    return Status::Corruption("Blob relocation directory checksum mismatch");
  }

  decoded.origins_.reserve(static_cast<size_t>(origin_count));
  decoded.format_version_ = version;
  uint64_t computed_entries = 0;
  uint64_t computed_record_bytes = 0;
  uint64_t total_route_sources = 0;
  for (uint64_t i = 0; i < origin_count; ++i) {
    BlobRelocationOrigin origin;
    uint32_t reserved = 0;
    if (!GetFixed64(&cursor, &origin.origin_file_number) ||
        !GetFixed64(&cursor, &origin.first_original_offset) ||
        !GetFixed64(&cursor, &origin.last_original_offset) ||
        !GetFixed64(&cursor, &origin.entry_count) ||
        !GetFixed64(&cursor, &origin.record_bytes) ||
        !GetFixed32(&cursor, &origin.entries_digest) ||
        !GetFixed32(&cursor, &reserved)) {
      return Status::Corruption("Truncated blob relocation origin entry");
    }
    uint64_t route_source_count = 0;
    if (version >= 2 &&
        (!GetFixed64(&cursor, &route_source_count) ||
         route_source_count > kMaxRouteSources - total_route_sources ||
         route_source_count > cursor.size() / sizeof(uint64_t))) {
      return Status::Corruption("Invalid blob relocation route-source count");
    }
    origin.served_generation_file_numbers.reserve(
        static_cast<size_t>(route_source_count));
    for (uint64_t source = 0; source < route_source_count; ++source) {
      uint64_t generation = 0;
      if (!GetFixed64(&cursor, &generation) || generation == 0 ||
          (!origin.served_generation_file_numbers.empty() &&
           origin.served_generation_file_numbers.back() >= generation)) {
        return Status::Corruption("Invalid blob relocation route source");
      }
      origin.served_generation_file_numbers.push_back(generation);
    }
    total_route_sources += route_source_count;
    if (reserved != 0 || origin.origin_file_number == 0 ||
        origin.entry_count == 0 ||
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
  if (!cursor.empty() || computed_entries != decoded.total_entries_ ||
      computed_record_bytes != decoded.total_record_bytes_) {
    return Status::Corruption("Blob relocation directory totals mismatch");
  }
  decoded.directory_digest_ = directory_digest;
  *directory = std::move(decoded);
  return Status::OK();
}

bool BlobRelocationDirectory::HasRepairMetadata() const {
  return format_version_ >= 2 &&
         std::all_of(origins_.begin(), origins_.end(),
                     [](const BlobRelocationOrigin& origin) {
                       return !origin.served_generation_file_numbers.empty();
                     });
}

Status BlobRelocationDirectory::ValidateForFileSize(uint64_t file_size) const {
  if (file_size < kBlobRelocationFilePrefixSize ||
      total_record_bytes_ > file_size - kBlobRelocationFilePrefixSize) {
    return Status::Corruption(
        "Blob relocation record bytes exceed physical file size");
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
                                           uint64_t relocated_value_offset,
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
  const uint64_t record_bytes = stored_value_size + kSimpleGen2BlobTrailerSize;
  if (!has_current_ || origin_file_number != current_.origin_file_number) {
    if (has_current_ && directory_.origins_.size() + 1 >=
                            BlobRelocationDirectory::kMaxOrigins) {
      return Status::MemoryLimit("Too many origins in blob relocation file");
    }
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
    ExtendDigest(original_value_offset, relocated_value_offset,
                 stored_value_size, compression, &next.entries_digest);
    current_ = next;
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
  ExtendDigest(original_value_offset, relocated_value_offset, stored_value_size,
               compression, &next.entries_digest);
  current_ = next;
  return Status::OK();
}

Status BlobRelocationDirectoryBuilder::AddServedRouteSource(
    uint64_t origin_file_number, uint64_t generation_file_number) {
  if (finished_) {
    return Status::InvalidArgument(
        "Cannot add to finished blob relocation directory");
  }
  if (origin_file_number == 0 || generation_file_number == 0) {
    return Status::InvalidArgument("Invalid blob relocation route source");
  }
  if (served_route_sources_.size() >=
      BlobRelocationDirectory::kMaxRouteSources) {
    return Status::MemoryLimit("Too many blob relocation route sources");
  }
  served_route_sources_.emplace_back(origin_file_number,
                                     generation_file_number);
  return Status::OK();
}

Status BlobRelocationDirectoryBuilder::FinishCurrentOrigin() {
  if (!has_current_) {
    return Status::OK();
  }
  if (directory_.origins_.size() >= BlobRelocationDirectory::kMaxOrigins) {
    return Status::MemoryLimit("Too many origins in blob relocation file");
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
    BlobRelocationDirectory* directory) {
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
  std::sort(served_route_sources_.begin(), served_route_sources_.end());
  served_route_sources_.erase(
      std::unique(served_route_sources_.begin(), served_route_sources_.end()),
      served_route_sources_.end());
  for (const auto& source : served_route_sources_) {
    const auto origin = std::lower_bound(
        directory_.origins_.begin(), directory_.origins_.end(), source.first,
        [](const BlobRelocationOrigin& value, uint64_t origin_file_number) {
          return value.origin_file_number < origin_file_number;
        });
    if (origin == directory_.origins_.end() ||
        origin->origin_file_number != source.first) {
      return Status::InvalidArgument(
          "Blob relocation route source has no origin partition");
    }
    origin->served_generation_file_numbers.push_back(source.second);
  }
  std::string encoded;
  directory_.EncodeTo(&encoded);
  status = BlobRelocationDirectory::DecodeFrom(encoded, directory);
  if (status.ok()) {
    finished_ = true;
  }
  return status;
}

std::string EncodeBlobRelocationKey(uint64_t origin_file_number,
                                    uint64_t original_value_offset) {
  std::string key;
  key.reserve(sizeof(origin_file_number) + sizeof(original_value_offset));
  PutBigEndian64(&key, origin_file_number);
  PutBigEndian64(&key, original_value_offset);
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
  *origin_file_number = DecodeBigEndian64(key.data());
  *original_value_offset =
      DecodeBigEndian64(key.data() + sizeof(*origin_file_number));
  if (*origin_file_number == 0) {
    return Status::Corruption("Invalid blob relocation origin");
  }
  return Status::OK();
}

BlockBasedTableOptions MakeBlobRelocationTableOptions(
    const BlockBasedTableOptions& source) {
  BlockBasedTableOptions options(source);
  // Dedicated relocation outputs reuse the stable stored-byte CRC in their
  // physical trailer. This permits bounded streaming without a second payload
  // checksum pass. Normal SST/blob writers retain their configured checksums;
  // disabling physical checksums remains supported for relocation files.
  if (options.checksum != kNoChecksum) {
    options.checksum = kCRC32c;
  }
  options.index_type = BlockBasedTableOptions::kBinarySearch;
  options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinarySearch;
  options.flush_block_policy_factory.reset();
  options.filter_policy.reset();
  options.partition_filters = false;
  options.user_defined_index_factory.reset();
  options.use_udi_as_primary_index = false;
  options.fail_if_no_udi_on_open = false;
  return options;
}

}  // namespace ROCKSDB_NAMESPACE
