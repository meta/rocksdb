//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>

#include "memory/memory_allocator_impl.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Decompressor;
class Statistics;
class SystemClock;

// A class representing a single uncompressed value read from a blob file.
class BlobContents {
 public:
  BlobContents(CacheAllocationPtr&& allocation, size_t size)
      : allocation_(std::move(allocation)), data_(allocation_.get(), size) {}

  BlobContents(const BlobContents&) = delete;
  BlobContents& operator=(const BlobContents&) = delete;

  BlobContents(BlobContents&&) = default;
  BlobContents& operator=(BlobContents&&) = default;

  ~BlobContents() = default;

  const Slice& data() const { return data_; }
  size_t size() const { return data_.size(); }

  size_t ApproximateMemoryUsage() const;

  // For TypedCacheInterface
  const Slice& ContentSlice() const { return data_; }
  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kBlobValue;

 private:
  CacheAllocationPtr allocation_;
  Slice data_;
};

class BlobContentsCreator : public Cache::CreateContext {
 public:
  static void Create(std::unique_ptr<BlobContents>* out, size_t* out_charge,
                     const Slice& contents, CompressionType /*type*/,
                     MemoryAllocator* alloc) {
    auto raw = new BlobContents(AllocateAndCopyBlock(contents, alloc),
                                contents.size());
    out->reset(raw);
    if (out_charge) {
      *out_charge = raw->ApproximateMemoryUsage();
    }
  }
};

// A stable-cache entry owns an uncompressed logical value together with the
// immutable BlobIndex fingerprint that validated it. The serialized header is
// retained in ContentSlice so secondary-cache promotion reconstructs the same
// validation boundary.
class StableBlobContents {
 public:
  static constexpr size_t kHeaderSize = 44;

  StableBlobContents(CacheAllocationPtr&& allocation, size_t serialized_size);
  StableBlobContents(const StableBlobContents&) = delete;
  StableBlobContents& operator=(const StableBlobContents&) = delete;
  StableBlobContents(StableBlobContents&&) = default;
  StableBlobContents& operator=(StableBlobContents&&) = default;
  ~StableBlobContents() = default;

  static Status Create(const Slice& value, uint64_t stored_value_size,
                       CompressionType compression,
                       uint32_t stable_checksum_version,
                       uint32_t stable_checksum, MemoryAllocator* allocator,
                       std::unique_ptr<StableBlobContents>* output);

  static Status Create(const Slice& value, uint64_t stored_value_size,
                       CompressionType compression,
                       uint32_t stable_checksum_version,
                       uint32_t stable_checksum, uint32_t stored_checksum,
                       MemoryAllocator* allocator,
                       std::unique_ptr<StableBlobContents>* output);

  // Primary-cache admission only: the caller has just computed and matched
  // value's logical CRC against this immutable fingerprint (or computed it
  // while producing the value). Skip a second full-value CRC after copying.
  // Secondary-cache promotion never uses this path: its public constructor
  // validates both the serialized header and the entire logical value.
  static Status CreateWithVerifiedValue(
      const Slice& value, uint64_t stored_value_size,
      CompressionType compression, uint32_t stable_checksum_version,
      uint32_t stable_checksum, uint32_t stored_checksum,
      MemoryAllocator* allocator, std::unique_ptr<StableBlobContents>* output);

  const Status& validation_status() const { return validation_status_; }
  const Slice& data() const { return data_; }
  size_t size() const { return data_.size(); }
  uint64_t stored_value_size() const { return stored_value_size_; }
  CompressionType compression() const { return compression_; }
  uint32_t stable_checksum_version() const { return stable_checksum_version_; }
  uint32_t stable_checksum() const { return stable_checksum_; }
  uint32_t stored_checksum() const { return stored_checksum_; }

  size_t ApproximateMemoryUsage() const;

  const Slice& ContentSlice() const { return serialized_; }
  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kBlobValue;

 private:
  StableBlobContents(CacheAllocationPtr&& allocation, size_t serialized_size,
                     bool verify_value);
  static Status CreateImpl(const Slice& value, uint64_t stored_value_size,
                           CompressionType compression,
                           uint32_t stable_checksum_version,
                           uint32_t stable_checksum, uint32_t stored_checksum,
                           MemoryAllocator* allocator, bool verify_value,
                           std::unique_ptr<StableBlobContents>* output);
  void Decode(bool verify_value);

  CacheAllocationPtr allocation_;
  Slice serialized_;
  Slice data_;
  Status validation_status_;
  uint64_t stored_value_size_ = 0;
  uint32_t stable_checksum_version_ = 0;
  uint32_t stable_checksum_ = 0;
  uint32_t stored_checksum_ = 0;
  CompressionType compression_ = kNoCompression;
};

class StableBlobContentsCreator : public Cache::CreateContext {
 public:
  static void Create(std::unique_ptr<StableBlobContents>* output,
                     size_t* output_charge, const Slice& contents,
                     CompressionType /*type*/, MemoryAllocator* allocator) {
    StableBlobContents* value = new StableBlobContents(
        AllocateAndCopyBlock(contents, allocator), contents.size());
    output->reset(value);
    if (output_charge != nullptr) {
      *output_charge = value->ApproximateMemoryUsage();
    }
  }
};

// Decodes one stored blob representation into an owned uncompressed value.
// The caller supplies the same decompressor schema used by the source file.
Status UncompressBlobContentsIfNeeded(
    const Slice& stored_value, CompressionType compression_type,
    Decompressor* decompressor, MemoryAllocator* allocator, SystemClock* clock,
    Statistics* statistics, std::unique_ptr<BlobContents>* result);

}  // namespace ROCKSDB_NAMESPACE
