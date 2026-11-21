//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>

#include "cache/typed_cache.h"
#include "db/blob/blob_index.h"
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

  // Gives up the buffer, so a cache entry can adopt it without a copy. The
  // object is empty afterwards.
  CacheAllocationPtr ReleaseAllocation() {
    data_.clear();
    return std::move(allocation_);
  }

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

// One entry of the stable blob cache. The stable cache key contains the
// value's stable identity (origin, original offset), not the file that holds
// the value, so an entry stays valid when the value moves to a relocation
// file (see BlobSource). An entry holds one uncompressed value plus the
// StableBlobFingerprint (from the BlobIndex) that validated it.
//
// In primary memory the buffer holds only the value, like BlobContents. The
// cache item helper below writes the saved form (header + value) only when
// the entry moves to a secondary cache. Promotion back to primary memory
// checks the header and the whole value again.
class StableBlobContents {
 public:
  // Saved form: stored value size (8) | logical CRC (4) | stored CRC (4) |
  // compression (1) | CRC32C of these 17 bytes (4), then the value. The value
  // size is the rest of the saved slice.
  static constexpr size_t kSavedHeaderSize = 21;

  StableBlobContents(const StableBlobContents&) = delete;
  StableBlobContents& operator=(const StableBlobContents&) = delete;
  ~StableBlobContents() = default;

  // Use only to insert into the primary cache. The caller has already
  // computed the logical CRC of `value` (for example while it produced the
  // value) and matched it against `fingerprint`. This function copies `value`
  // and does not compute the CRC of the whole value again.
  static Status CreateWithVerifiedValue(
      const Slice& value, const StableBlobFingerprint& fingerprint,
      MemoryAllocator* allocator, std::unique_ptr<StableBlobContents>* output);

  // Like CreateWithVerifiedValue, but adopts `allocation`, whose first
  // `value_size` bytes are the value, instead of copying it. Bytes after the
  // value (for example a record trailer) stay unused.
  static Status CreateWithVerifiedAllocation(
      CacheAllocationPtr&& allocation, size_t value_size,
      const StableBlobFingerprint& fingerprint,
      std::unique_ptr<StableBlobContents>* output);

  // Promotion from the secondary cache. It does not trust the secondary
  // cache: it checks the header CRC, the logical CRC of the whole value and
  // that the compression type is supported. It checks the other fingerprint
  // fields only when they follow from the value: for kNoCompression, the
  // stored size equals the value size and the stored CRC follows from the
  // logical CRC. A saved form shorter than kSavedHeaderSize (21 bytes) is
  // also rejected. On a failure the returned entry is invalid
  // (validation_status() is Corruption) and readers erase it.
  static std::unique_ptr<StableBlobContents> CreateFromSaved(
      const Slice& saved, MemoryAllocator* allocator);

  Status validation_status() const {
    return valid_ ? Status::OK()
                  : Status::Corruption("Invalid stable blob cache entry");
  }
  const Slice& data() const { return data_; }
  size_t size() const { return data_.size(); }
  const StableBlobFingerprint& fingerprint() const { return fingerprint_; }

  size_t ApproximateMemoryUsage() const;

  // Saved form for the cache item helper.
  size_t SavedSize() const { return kSavedHeaderSize + data_.size(); }
  void SaveTo(size_t from_offset, size_t length, char* out) const;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kBlobValue;

 private:
  StableBlobContents() = default;

  // Checks the fingerprint fields and, if `verify_value`, the value CRC.
  bool Validate(bool verify_value) const;

  // Writes the kSavedHeaderSize-byte saved header into `header`.
  void EncodeSavedHeader(char* header) const;

  CacheAllocationPtr allocation_;
  Slice data_;
  StableBlobFingerprint fingerprint_;
  bool valid_ = false;
};

// Create context type of the stable cache interface. Promotion goes through
// the cache item helper specialization below, so it holds no state.
class StableBlobContentsCreator : public Cache::CreateContext {};

// Cache item helper functions for the stable cache. The size and save
// callbacks use the saved form (header + value), not the primary buffer.
template <>
class FullTypedCacheHelperFns<StableBlobContents, StableBlobContentsCreator>
    : public BasicTypedCacheHelperFns<StableBlobContents> {
 public:
  using ObjectPtr = Cache::ObjectPtr;
  using CreateContext = Cache::CreateContext;

 protected:
  using BasicTypedCacheHelperFns<StableBlobContents>::DownCastValue;
  using BasicTypedCacheHelperFns<StableBlobContents>::UpCastValue;

  static size_t Size(ObjectPtr v) { return DownCastValue(v)->SavedSize(); }

  static Status SaveTo(ObjectPtr v, size_t from_offset, size_t length,
                       char* out) {
    DownCastValue(v)->SaveTo(from_offset, length, out);
    return Status::OK();
  }

  static Status Create(const Slice& data, CompressionType /*type*/,
                       CacheTier source, CreateContext* /*context*/,
                       MemoryAllocator* allocator, ObjectPtr* out_obj,
                       size_t* out_charge) {
    if (source != CacheTier::kVolatileTier) {
      return Status::InvalidArgument();
    }
    std::unique_ptr<StableBlobContents> value =
        StableBlobContents::CreateFromSaved(data, allocator);
    if (out_charge != nullptr) {
      *out_charge = value->ApproximateMemoryUsage();
    }
    *out_obj = UpCastValue(value.release());
    return Status::OK();
  }
};

// Decodes the stored bytes of one blob value into an owned uncompressed value
// in `*result`. For kNoCompression it copies the stored bytes. The caller
// passes the decompressor of the file that holds the value. Returns
// Corruption for an unsupported compression type, a null decompressor with a
// compressed value, an uncompressed size above kMaxSimpleGen2BlobPayloadSize
// (checked before allocating; this also applies to legacy blobs), or a
// decompression error.
Status UncompressBlobContentsIfNeeded(
    const Slice& stored_value, CompressionType compression_type,
    Decompressor* decompressor, MemoryAllocator* allocator, SystemClock* clock,
    Statistics* statistics, std::unique_ptr<BlobContents>* result);

}  // namespace ROCKSDB_NAMESPACE
