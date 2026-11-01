//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_contents.h"

#include <cassert>
#include <cstring>
#include <limits>

#include "cache/cache_entry_roles.h"
#include "cache/cache_helpers.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_index.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/malloc.h"
#include "rocksdb/advanced_compression.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint32_t kStableBlobContentsMagic = 0x53424C42U;  // SBLB
constexpr uint32_t kStableBlobContentsFormatVersion = 2;

size_t ApproximateAllocationUsage(const CacheAllocationPtr& allocation,
                                  size_t allocation_size, const void* object,
                                  [[maybe_unused]] size_t object_size) {
  size_t usage = 0;
  if (allocation) {
    MemoryAllocator* const allocator = allocation.get_deleter().allocator;
    if (allocator != nullptr) {
      usage += allocator->UsableSize(allocation.get(), allocation_size);
    } else {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      usage += malloc_usable_size(allocation.get());
#else
      usage += allocation_size;
#endif
    }
  }
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<void*>(object));
#else
  usage += object_size;
#endif
  return usage;
}

}  // namespace

size_t BlobContents::ApproximateMemoryUsage() const {
  return ApproximateAllocationUsage(allocation_, data_.size(), this,
                                    sizeof(*this));
}

StableBlobContents::StableBlobContents(CacheAllocationPtr&& allocation,
                                       size_t serialized_size)
    : StableBlobContents(std::move(allocation), serialized_size,
                         /*verify_value=*/true) {}

StableBlobContents::StableBlobContents(CacheAllocationPtr&& allocation,
                                       size_t serialized_size,
                                       bool verify_value)
    : allocation_(std::move(allocation)),
      serialized_(allocation_.get(), serialized_size) {
  Decode(verify_value);
}

Status StableBlobContents::Create(
    const Slice& value, uint64_t stored_value_size, CompressionType compression,
    uint32_t stable_checksum_version, uint32_t stable_checksum,
    MemoryAllocator* allocator, std::unique_ptr<StableBlobContents>* output) {
  return Create(value, stored_value_size, compression, stable_checksum_version,
                stable_checksum, 0, allocator, output);
}

Status StableBlobContents::Create(
    const Slice& value, uint64_t stored_value_size, CompressionType compression,
    uint32_t stable_checksum_version, uint32_t stable_checksum,
    uint32_t stored_checksum, MemoryAllocator* allocator,
    std::unique_ptr<StableBlobContents>* output) {
  return CreateImpl(value, stored_value_size, compression,
                    stable_checksum_version, stable_checksum, stored_checksum,
                    allocator, /*verify_value=*/true, output);
}

Status StableBlobContents::CreateWithVerifiedValue(
    const Slice& value, uint64_t stored_value_size, CompressionType compression,
    uint32_t stable_checksum_version, uint32_t stable_checksum,
    uint32_t stored_checksum, MemoryAllocator* allocator,
    std::unique_ptr<StableBlobContents>* output) {
  return CreateImpl(value, stored_value_size, compression,
                    stable_checksum_version, stable_checksum, stored_checksum,
                    allocator, /*verify_value=*/false, output);
}

Status StableBlobContents::CreateImpl(
    const Slice& value, uint64_t stored_value_size, CompressionType compression,
    uint32_t stable_checksum_version, uint32_t stable_checksum,
    uint32_t stored_checksum, MemoryAllocator* allocator, bool verify_value,
    std::unique_ptr<StableBlobContents>* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("Null stable blob cache output");
  }
  if (!CompressionTypeSupported(compression) ||
      !BlobIndex::IsSupportedStableChecksumVersion(stable_checksum_version) ||
      (compression == kNoCompression && stored_value_size != value.size()) ||
      value.size() > std::numeric_limits<size_t>::max() - kHeaderSize) {
    return Status::InvalidArgument("Invalid stable blob cache metadata");
  }
  const size_t serialized_size = kHeaderSize + value.size();
  CacheAllocationPtr allocation = AllocateBlock(serialized_size, allocator);
  char* const data = allocation.get();
  EncodeFixed32(data, kStableBlobContentsMagic);
  EncodeFixed32(data + 4, kStableBlobContentsFormatVersion);
  EncodeFixed64(data + 8, stored_value_size);
  EncodeFixed64(data + 16, value.size());
  EncodeFixed32(data + 24, stable_checksum_version);
  EncodeFixed32(data + 28, stable_checksum);
  data[32] = static_cast<char>(compression);
  data[33] = 1;
  data[34] = 0;
  data[35] = 0;
  EncodeFixed32(data + 36, stored_checksum);
  EncodeFixed32(data + 40, crc32c::Value(data, 40));
  if (!value.empty()) {
    memcpy(data + kHeaderSize, value.data(), value.size());
  }

  std::unique_ptr<StableBlobContents> decoded(new StableBlobContents(
      std::move(allocation), serialized_size, verify_value));
  if (!decoded->validation_status().ok()) {
    return decoded->validation_status();
  }
  *output = std::move(decoded);
  return Status::OK();
}

void StableBlobContents::Decode(bool verify_value) {
  if (serialized_.size() < kHeaderSize) {
    validation_status_ =
        Status::Corruption("Truncated stable blob cache entry");
    return;
  }
  const char* const header = serialized_.data();
  const uint32_t magic = DecodeFixed32(header);
  const uint32_t format_version = DecodeFixed32(header + 4);
  stored_value_size_ = DecodeFixed64(header + 8);
  const uint64_t logical_size = DecodeFixed64(header + 16);
  stable_checksum_version_ = DecodeFixed32(header + 24);
  stable_checksum_ = DecodeFixed32(header + 28);
  stored_checksum_ = DecodeFixed32(header + 36);
  compression_ =
      static_cast<CompressionType>(static_cast<unsigned char>(header[32]));
  if (magic != kStableBlobContentsMagic ||
      format_version != kStableBlobContentsFormatVersion || header[33] != 1 ||
      header[34] != 0 || header[35] != 0 ||
      DecodeFixed32(header + 40) != crc32c::Value(header, 40) ||
      logical_size != serialized_.size() - kHeaderSize ||
      !CompressionTypeSupported(compression_) ||
      (compression_ == kNoCompression && stored_value_size_ != logical_size) ||
      !BlobIndex::IsSupportedStableChecksumVersion(stable_checksum_version_) ||
      (stable_checksum_version_ == BlobIndex::kStableChecksumVersion1 &&
       stored_checksum_ != 0) ||
      (stable_checksum_version_ == BlobIndex::kStableChecksumVersion2 &&
       compression_ == kNoCompression &&
       stored_checksum_ != BlobIndex::ExtendChecksumWithCompression(
                               stable_checksum_, compression_))) {
    validation_status_ = Status::Corruption("Invalid stable blob cache entry");
    return;
  }
  data_ = Slice(serialized_.data() + kHeaderSize,
                static_cast<size_t>(logical_size));
  if (verify_value &&
      crc32c::Value(data_.data(), data_.size()) != stable_checksum_) {
    data_.clear();
    validation_status_ =
        Status::Corruption("Stable blob cache checksum mismatch");
    return;
  }
  validation_status_ = Status::OK();
}

size_t StableBlobContents::ApproximateMemoryUsage() const {
  return ApproximateAllocationUsage(allocation_, serialized_.size(), this,
                                    sizeof(*this));
}

Status UncompressBlobContentsIfNeeded(
    const Slice& stored_value, CompressionType compression_type,
    Decompressor* decompressor, MemoryAllocator* allocator, SystemClock* clock,
    Statistics* statistics, std::unique_ptr<BlobContents>* result) {
  assert(result != nullptr);
  if (!CompressionTypeSupported(compression_type)) {
    return Status::Corruption("Unsupported blob compression type");
  }
  if (compression_type == kNoCompression) {
    BlobContentsCreator::Create(result, nullptr, stored_value, kNoCompression,
                                allocator);
    return Status::OK();
  }
  if (decompressor == nullptr) {
    return Status::Corruption("Missing blob decompressor");
  }

  Decompressor::Args args;
  args.compression_type = compression_type;
  args.compressed_data = stored_value;
  Status status = decompressor->ExtractUncompressedSize(args);
  if (!status.ok()) {
    return Status::Corruption(status.ToString());
  }
  if (args.uncompressed_size > kMaxSimpleGen2BlobPayloadSize) {
    return Status::Corruption("Uncompressed blob value is too large");
  }

  CacheAllocationPtr output = AllocateBlock(args.uncompressed_size, allocator);
  {
    PERF_TIMER_GUARD(blob_decompress_time);
    StopWatch stop_watch(clock, statistics, BLOB_DB_DECOMPRESSION_MICROS);
    status = decompressor->DecompressBlock(args, output.get());
  }
  TEST_SYNC_POINT_CALLBACK(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", &status);
  if (!status.ok()) {
    return Status::Corruption(status.ToString());
  }
  result->reset(new BlobContents(std::move(output), args.uncompressed_size));
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
