//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_contents.h"

#include <algorithm>
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

size_t ApproximateAllocationUsage(const CacheAllocationPtr& allocation,
                                  size_t allocation_size,
                                  [[maybe_unused]] const void* object,
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

Status StableBlobContents::CreateWithVerifiedValue(
    const Slice& value, const StableBlobFingerprint& fingerprint,
    MemoryAllocator* allocator, std::unique_ptr<StableBlobContents>* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("Null stable blob cache output");
  }
  if (!CompressionTypeSupported(fingerprint.compression) ||
      (fingerprint.compression == kNoCompression &&
       fingerprint.stored_value_size != value.size())) {
    return Status::InvalidArgument("Invalid stable blob cache metadata");
  }
  std::unique_ptr<StableBlobContents> contents(new StableBlobContents());
  contents->allocation_ = AllocateAndCopyBlock(value, allocator);
  contents->data_ = Slice(contents->allocation_.get(), value.size());
  contents->fingerprint_ = fingerprint;
  contents->valid_ = contents->Validate(/*verify_value=*/false);
  if (!contents->valid_) {
    return contents->validation_status();
  }
  *output = std::move(contents);
  return Status::OK();
}

Status StableBlobContents::CreateWithVerifiedAllocation(
    CacheAllocationPtr&& allocation, size_t value_size,
    const StableBlobFingerprint& fingerprint,
    std::unique_ptr<StableBlobContents>* output) {
  if (output == nullptr || (value_size != 0 && !allocation)) {
    return Status::InvalidArgument("Invalid stable blob cache input");
  }
  if (!CompressionTypeSupported(fingerprint.compression) ||
      (fingerprint.compression == kNoCompression &&
       fingerprint.stored_value_size != value_size)) {
    return Status::InvalidArgument("Invalid stable blob cache metadata");
  }
  std::unique_ptr<StableBlobContents> contents(new StableBlobContents());
  contents->allocation_ = std::move(allocation);
  contents->data_ = Slice(contents->allocation_.get(), value_size);
  contents->fingerprint_ = fingerprint;
  contents->valid_ = contents->Validate(/*verify_value=*/false);
  if (!contents->valid_) {
    return contents->validation_status();
  }
  *output = std::move(contents);
  return Status::OK();
}

std::unique_ptr<StableBlobContents> StableBlobContents::CreateFromSaved(
    const Slice& saved, MemoryAllocator* allocator) {
  std::unique_ptr<StableBlobContents> contents(new StableBlobContents());
  if (saved.size() < kSavedHeaderSize) {
    return contents;
  }
  const char* const header = saved.data();
  contents->fingerprint_.stored_value_size = DecodeFixed64(header);
  contents->fingerprint_.stable_checksum = DecodeFixed32(header + 8);
  contents->fingerprint_.stored_checksum = DecodeFixed32(header + 12);
  contents->fingerprint_.compression =
      static_cast<CompressionType>(static_cast<unsigned char>(header[16]));
  if (DecodeFixed32(header + 17) != crc32c::Value(header, 17)) {
    return contents;
  }
  const Slice value(saved.data() + kSavedHeaderSize,
                    saved.size() - kSavedHeaderSize);
  contents->allocation_ = AllocateAndCopyBlock(value, allocator);
  contents->data_ = Slice(contents->allocation_.get(), value.size());
  contents->valid_ = contents->Validate(/*verify_value=*/true);
  return contents;
}

bool StableBlobContents::Validate(bool verify_value) const {
  const StableBlobFingerprint& f = fingerprint_;
  if (!CompressionTypeSupported(f.compression) ||
      (f.compression == kNoCompression &&
       (f.stored_value_size != data_.size() ||
        f.stored_checksum != BlobIndex::ExtendChecksumWithCompression(
                                 f.stable_checksum, f.compression)))) {
    return false;
  }
  return !verify_value ||
         crc32c::Value(data_.data(), data_.size()) == f.stable_checksum;
}

void StableBlobContents::EncodeSavedHeader(char* header) const {
  EncodeFixed64(header, fingerprint_.stored_value_size);
  EncodeFixed32(header + 8, fingerprint_.stable_checksum);
  EncodeFixed32(header + 12, fingerprint_.stored_checksum);
  header[16] = static_cast<char>(fingerprint_.compression);
  EncodeFixed32(header + 17, crc32c::Value(header, 17));
}

void StableBlobContents::SaveTo(size_t from_offset, size_t length,
                                char* out) const {
  assert(from_offset + length <= SavedSize());
  if (from_offset < kSavedHeaderSize) {
    char header[kSavedHeaderSize];
    EncodeSavedHeader(header);
    const size_t n = std::min(length, kSavedHeaderSize - from_offset);
    memcpy(out, header + from_offset, n);
    out += n;
    length -= n;
    from_offset = kSavedHeaderSize;
  }
  if (length > 0) {
    memcpy(out, data_.data() + (from_offset - kSavedHeaderSize), length);
  }
}

size_t StableBlobContents::ApproximateMemoryUsage() const {
  return ApproximateAllocationUsage(allocation_, data_.size(), this,
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
  TEST_SYNC_POINT_CALLBACK("UncompressBlobContentsIfNeeded:TamperWithResult",
                           &status);
  if (!status.ok()) {
    return Status::Corruption(status.ToString());
  }
  result->reset(new BlobContents(std::move(output), args.uncompressed_size));
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
