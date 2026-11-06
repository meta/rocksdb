// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once

#include <algorithm>

#include "memory/memory_allocator_impl.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/options.h"
#include "table/block_based/block_type.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/compression_context_cache.h"

#ifdef ZSTD
#include <zstd.h>
#include <zstd_errors.h>
// ZSTD_Compress2(), ZSTD_compressStream2() and frame parameters all belong to
// advanced APIs and require v1.4.0+, which is from April 2019.
// https://github.com/facebook/zstd/blob/eb9f881eb810f2242f1ef36b3f3e7014eecb8fa6/lib/zstd.h#L297C40-L297C45
// To avoid a rat's nest of #ifdefs, we now require v1.4.0+ for ZSTD support.
#if ZSTD_VERSION_NUMBER < 10400
#error "ZSTD support requires version >= 1.4.0 (libzstd-devel)"
#endif  // ZSTD_VERSION_NUMBER
// The above release also includes digested dictionary support, but some
// required functions (ZSTD_createDDict_byReference) are still only exported
// with ZSTD_STATIC_LINKING_ONLY defined.
#if defined(ZSTD_STATIC_LINKING_ONLY)
#define ROCKSDB_ZSTD_DDICT
#endif  // defined(ZSTD_STATIC_LINKING_ONLY)
//  For ZDICT_* functions
#include <zdict.h>
// ZDICT_finalizeDictionary API is exported and stable since v1.4.5
#if ZSTD_VERSION_NUMBER >= 10405
#define ROCKSDB_ZDICT_FINALIZE
#endif  // ZSTD_VERSION_NUMBER >= 10405
#endif  // ZSTD

namespace ROCKSDB_NAMESPACE {
// Internal helper for readers decoding persisted compression metadata.
// Resolves `compatibility_name` to a CompressionManager by first checking
// whether `configured_compression_manager` can read that compatibility name,
// then falling back to constructing a standalone manager from the serialized
// name. Callers that want special handling for an empty compatibility name
// should do so before calling this helper.
Status ResolveCompressionManagerByCompatibilityName(
    Slice compatibility_name,
    CompressionManager* configured_compression_manager,
    std::shared_ptr<CompressionManager>* resolved_manager);

// Need this for the context allocation override
// On windows we need to do this explicitly
#if defined(ZSTD) && defined(ROCKSDB_JEMALLOC) && defined(OS_WIN) && \
    defined(ZSTD_STATIC_LINKING_ONLY)
#define ROCKSDB_ZSTD_CUSTOM_MEM
namespace port {
ZSTD_customMem GetJeZstdAllocationOverrides();
}  // namespace port
#endif  // defined(ZSTD) && defined(ROCKSDB_JEMALLOC) && defined(OS_WIN) &&
        // defined(ZSTD_STATIC_LINKING_ONLY)

#ifdef ZSTD
// Translates a configured compression_opts.level into the effective ZSTD
// compression level. Besides resolving the "use default" sentinel, this
// removes a discontinuity at level 0: ZSTD itself treats a requested level of
// 0 as "use the default level" (historically 3), which would make level 0
// more aggressive than levels 1 and 2. Mapping 0 to -1 keeps the level spectrum
// monotonic, which is friendlier to auto-tuning.
inline int SanitizeZSTDCompressionLevel(int level) {
  if (level == CompressionOptions::kDefaultCompressionLevel) {
    // NB: ZSTD_CLEVEL_DEFAULT is historically == 3
    return ZSTD_CLEVEL_DEFAULT;
  } else if (level == 0) {
    // Avoid library's discontinuity at level 0
    return -1;
  } else {
    return level;
  }
}
#endif  // ZSTD

// Cached data represents a portion that can be re-used
// If, in the future we have more than one native context to
// cache we can arrange this as a tuple
class ZSTDUncompressCachedData {
 public:
#if defined(ZSTD)
  using ZSTDNativeContext = ZSTD_DCtx*;
#else
  using ZSTDNativeContext = void*;
#endif  // ZSTD
  ZSTDUncompressCachedData() {}
  // Init from cache
  ZSTDUncompressCachedData(const ZSTDUncompressCachedData& o) = delete;
  ZSTDUncompressCachedData& operator=(const ZSTDUncompressCachedData&) = delete;
  ZSTDUncompressCachedData(ZSTDUncompressCachedData&& o) noexcept
      : ZSTDUncompressCachedData() {
    *this = std::move(o);
  }
  ZSTDUncompressCachedData& operator=(ZSTDUncompressCachedData&& o) noexcept {
    assert(zstd_ctx_ == nullptr);
    std::swap(zstd_ctx_, o.zstd_ctx_);
    std::swap(cache_idx_, o.cache_idx_);
    return *this;
  }
  ZSTDNativeContext Get() const { return zstd_ctx_; }
  int64_t GetCacheIndex() const { return cache_idx_; }
  void CreateIfNeeded() {
    if (zstd_ctx_ == nullptr) {
#if !defined(ZSTD)
      zstd_ctx_ = nullptr;
#elif defined(ROCKSDB_ZSTD_CUSTOM_MEM)
      zstd_ctx_ =
          ZSTD_createDCtx_advanced(port::GetJeZstdAllocationOverrides());
#else  // ZSTD && !ROCKSDB_ZSTD_CUSTOM_MEM
      zstd_ctx_ = ZSTD_createDCtx();
#endif
      cache_idx_ = -1;
    }
  }
  void InitFromCache(const ZSTDUncompressCachedData& o, int64_t idx) {
    zstd_ctx_ = o.zstd_ctx_;
    cache_idx_ = idx;
  }
  ~ZSTDUncompressCachedData() {
#if defined(ZSTD)
    if (zstd_ctx_ != nullptr && cache_idx_ == -1) {
      ZSTD_freeDCtx(zstd_ctx_);
    }
#endif  // ZSTD
  }

 private:
  ZSTDNativeContext zstd_ctx_ = nullptr;
  int64_t cache_idx_ = -1;  // -1 means this instance owns the context
};
}  // namespace ROCKSDB_NAMESPACE

#if defined(XPRESS)
#include "port/xpress.h"
#endif

namespace ROCKSDB_NAMESPACE {

class FailureDecompressor : public Decompressor {
 public:
  explicit FailureDecompressor(Status&& status) : status_(std::move(status)) {
    assert(!status_.ok());
  }
  ~FailureDecompressor() override { status_.PermitUncheckedError(); }
  // No copying allowed
  FailureDecompressor(const FailureDecompressor&) = delete;
  FailureDecompressor& operator=(const FailureDecompressor&) = delete;
  FailureDecompressor(FailureDecompressor&&) = delete;
  FailureDecompressor& operator=(FailureDecompressor&&) = delete;

  const char* Name() const override { return "FailureDecompressor"; }

  Status ExtractUncompressedSize(Args& /*args*/) override { return status_; }

  Status DecompressBlock(const Args& /*args*/,
                         char* /*uncompressed_output*/) override {
    return status_;
  }

 protected:
  Status status_;
};

// Owns a decompression dictionary, and associated Decompressor, for storing
// in the block cache.
//
// Justification: for a "processed" dictionary to be saved in block cache, we
// also need a reference to the decompressor that processed it, to ensure it
// is recognized properly. At that point, we might as well have the dictionary
// part of the decompressor identity and track an associated decompressor along
// with a decompression dictionary in the block cache, and the decompressor
// hides potential details of processing the dictionary.
struct DecompressorDict {
  // Block containing the data for the compression dictionary in case the
  // constructor that takes a string parameter is used.
  std::string dict_str_;

  // Block containing the data for the compression dictionary in case the
  // constructor that takes a Slice parameter is used and the passed in
  // CacheAllocationPtr is not nullptr.
  CacheAllocationPtr dict_allocation_;

  // A Decompressor referencing and using the dictionary owned by this.
  std::unique_ptr<Decompressor> decompressor_;

  // Approximate owned memory usage
  size_t memory_usage_;

  DecompressorDict(std::string&& dict, Decompressor& from_decompressor)
      : dict_str_(std::move(dict)) {
    Populate(from_decompressor, dict_str_);
  }

  DecompressorDict(Slice slice, CacheAllocationPtr&& allocation,
                   Decompressor& from_decompressor)
      : dict_allocation_(std::move(allocation)) {
    Populate(from_decompressor, slice);
  }

  DecompressorDict(DecompressorDict&& rhs) noexcept
      : dict_str_(std::move(rhs.dict_str_)),
        dict_allocation_(std::move(rhs.dict_allocation_)),
        decompressor_(std::move(rhs.decompressor_)),
        memory_usage_(std::move(rhs.memory_usage_)) {}

  DecompressorDict& operator=(DecompressorDict&& rhs) noexcept {
    if (this == &rhs) {
      return *this;
    }
    dict_str_ = std::move(rhs.dict_str_);
    dict_allocation_ = std::move(rhs.dict_allocation_);
    decompressor_ = std::move(rhs.decompressor_);
    return *this;
  }
  // Disable copy
  DecompressorDict(const DecompressorDict&) = delete;
  DecompressorDict& operator=(const DecompressorDict&) = delete;
  ~DecompressorDict() = default;

  // The object is self-contained if the string constructor is used, or the
  // Slice constructor is invoked with a non-null allocation. Otherwise, it
  // is the caller's responsibility to ensure that the underlying storage
  // outlives this object.
  bool own_bytes() const { return !dict_str_.empty() || dict_allocation_; }

  const Slice& GetRawDict() const { return decompressor_->GetSerializedDict(); }

  // For TypedCacheInterface
  const Slice& ContentSlice() const { return GetRawDict(); }
  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kOtherBlock;
  static constexpr BlockType kBlockType = BlockType::kCompressionDictionary;

  size_t ApproximateMemoryUsage() const { return memory_usage_; }

 private:
  void Populate(Decompressor& from_decompressor, Slice dict);
};

// Holds dictionary and related data, like ZSTD's digested compression
// dictionary.
struct CompressionDict {
#ifdef ZSTD
  ZSTD_CDict* zstd_cdict_ = nullptr;
#endif  // ZSTD
  std::string dict_;

 public:
  CompressionDict() = default;
  CompressionDict(std::string&& dict, CompressionType type, int level) {
    dict_ = std::move(dict);
#ifdef ZSTD
    zstd_cdict_ = nullptr;
    if (!dict_.empty() && type == kZSTD) {
      level = SanitizeZSTDCompressionLevel(level);
      // Should be safe (but slower) if below call fails as we'll use the
      // raw dictionary to compress.
      zstd_cdict_ = ZSTD_createCDict(dict_.data(), dict_.size(), level);
      assert(zstd_cdict_ != nullptr);
    }
#else
    (void)type;
    (void)level;
#endif  // ZSTD
  }

  CompressionDict(CompressionDict&& other) {
#ifdef ZSTD
    zstd_cdict_ = other.zstd_cdict_;
    other.zstd_cdict_ = nullptr;
#endif  // ZSTD
    dict_ = std::move(other.dict_);
  }
  CompressionDict& operator=(CompressionDict&& other) {
    if (this == &other) {
      return *this;
    }
#ifdef ZSTD
    zstd_cdict_ = other.zstd_cdict_;
    other.zstd_cdict_ = nullptr;
#endif  // ZSTD
    dict_ = std::move(other.dict_);
    return *this;
  }

  ~CompressionDict() {
#ifdef ZSTD
    size_t res = 0;
    if (zstd_cdict_ != nullptr) {
      res = ZSTD_freeCDict(zstd_cdict_);
    }
    assert(res == 0);  // Last I checked they can't fail
    (void)res;         // prevent unused var warning
#endif                 // ZSTD
  }

#ifdef ZSTD
  const ZSTD_CDict* GetDigestedZstdCDict() const { return zstd_cdict_; }
#endif  // ZSTD

  Slice GetRawDict() const { return dict_; }
  bool empty() const { return dict_.empty(); }

  static const CompressionDict& GetEmptyDict() {
    static CompressionDict empty_dict{};
    return empty_dict;
  }

  // Disable copy
  CompressionDict(const CompressionDict&) = delete;
  CompressionDict& operator=(const CompressionDict&) = delete;
};

class CompressionContext : public Compressor::WorkingArea {
 private:
#ifdef ZSTD
  ZSTD_CCtx* zstd_ctx_ = nullptr;

  ZSTD_CCtx* CreateZSTDContext() {
#ifdef ROCKSDB_ZSTD_CUSTOM_MEM
    return ZSTD_createCCtx_advanced(port::GetJeZstdAllocationOverrides());
#else   // ROCKSDB_ZSTD_CUSTOM_MEM
    return ZSTD_createCCtx();
#endif  // ROCKSDB_ZSTD_CUSTOM_MEM
  }

 public:
  // callable inside ZSTD_Compress
  ZSTD_CCtx* ZSTDPreallocCtx() const {
    assert(zstd_ctx_ != nullptr);
    return zstd_ctx_;
  }

 private:
#endif  // ZSTD

  void CreateNativeContext(CompressionType type, int level, bool checksum) {
#ifdef ZSTD
    if (type == kZSTD) {
      zstd_ctx_ = CreateZSTDContext();
      level = SanitizeZSTDCompressionLevel(level);
      size_t err =
          ZSTD_CCtx_setParameter(zstd_ctx_, ZSTD_c_compressionLevel, level);
      if (ZSTD_isError(err)) {
        assert(false);
        ZSTD_freeCCtx(zstd_ctx_);
        zstd_ctx_ = CreateZSTDContext();
      }
      if (checksum) {
        err = ZSTD_CCtx_setParameter(zstd_ctx_, ZSTD_c_checksumFlag, 1);
        if (ZSTD_isError(err)) {
          assert(false);
          ZSTD_freeCCtx(zstd_ctx_);
          zstd_ctx_ = CreateZSTDContext();
        }
      }
    }
#else
    (void)type;
    (void)level;
    (void)checksum;
#endif  // ZSTD
  }
  void DestroyNativeContext() {
#ifdef ZSTD
    if (zstd_ctx_ != nullptr) {
      ZSTD_freeCCtx(zstd_ctx_);
    }
#endif  // ZSTD
  }

 public:
  explicit CompressionContext(CompressionType type,
                              const CompressionOptions& options) {
    CreateNativeContext(type, options.level, options.checksum);
  }
  ~CompressionContext() { DestroyNativeContext(); }
  CompressionContext(const CompressionContext&) = delete;
  CompressionContext& operator=(const CompressionContext&) = delete;
  CompressionContext(CompressionContext&&) = delete;
  CompressionContext& operator=(CompressionContext&&) = delete;
};

// This is like a working area, reusable for different dicts, etc.
// TODO: refactor / consolidate
class UncompressionContext : public Decompressor::WorkingArea {
 private:
  CompressionContextCache* ctx_cache_ = nullptr;
  ZSTDUncompressCachedData uncomp_cached_data_;

 public:
  explicit UncompressionContext(CompressionType type) {
    if (type == kZSTD) {
      ctx_cache_ = CompressionContextCache::Instance();
      uncomp_cached_data_ = ctx_cache_->GetCachedZSTDUncompressData();
    }
  }
  ~UncompressionContext() {
    if (uncomp_cached_data_.GetCacheIndex() != -1) {
      assert(ctx_cache_ != nullptr);
      ctx_cache_->ReturnCachedZSTDUncompressData(
          uncomp_cached_data_.GetCacheIndex());
    }
  }
  UncompressionContext(const UncompressionContext&) = delete;
  UncompressionContext& operator=(const UncompressionContext&) = delete;
  UncompressionContext(UncompressionContext&&) = delete;
  UncompressionContext& operator=(UncompressionContext&&) = delete;

  ZSTDUncompressCachedData::ZSTDNativeContext GetZSTDContext() const {
    return uncomp_cached_data_.Get();
  }
};

inline bool Snappy_Supported() {
#ifdef SNAPPY
  return true;
#else
  return false;
#endif
}

inline bool Zlib_Supported() {
#ifdef ZLIB
  return true;
#else
  return false;
#endif
}

inline bool BZip2_Supported() {
#ifdef BZIP2
  return true;
#else
  return false;
#endif
}

inline bool LZ4_Supported() {
#ifdef LZ4
  return true;
#else
  return false;
#endif
}

inline CompressionType GetDefaultCompressionType() {
  return LZ4_Supported()
             ? kLZ4Compression
             : (Snappy_Supported() ? kSnappyCompression : kNoCompression);
}

inline bool XPRESS_Supported() {
#ifdef XPRESS
  return true;
#else
  return false;
#endif
}

inline bool ZSTD_Supported() {
#ifdef ZSTD
  // NB: ZSTD format is finalized since version 0.8.0. See ZSTD_VERSION_NUMBER
  // check above.
  return true;
#else
  return false;
#endif
}

inline bool ZSTD_Streaming_Supported() {
#if defined(ZSTD)
  return true;
#else
  return false;
#endif
}

inline bool StreamingCompressionTypeSupported(
    CompressionType compression_type) {
  switch (compression_type) {
    case kNoCompression:
      return true;
    case kZSTD:
      return ZSTD_Streaming_Supported();
    default:
      return false;
  }
}

inline bool CompressionTypeSupported(CompressionType compression_type) {
  switch (compression_type) {
    case kNoCompression:
      return true;
    case kSnappyCompression:
      return Snappy_Supported();
    case kZlibCompression:
      return Zlib_Supported();
    case kBZip2Compression:
      return BZip2_Supported();
    case kLZ4Compression:
      return LZ4_Supported();
    case kLZ4HCCompression:
      return LZ4_Supported();
    case kXpressCompression:
      return XPRESS_Supported();
    case kZSTD:
      return ZSTD_Supported();
    default:  // Including custom compression types
      return false;
  }
}

inline bool DictCompressionTypeSupported(CompressionType compression_type) {
  switch (compression_type) {
    case kNoCompression:
      return false;
    case kSnappyCompression:
      return false;
    case kZlibCompression:
      return Zlib_Supported();
    case kBZip2Compression:
      return false;
    case kLZ4Compression:
    case kLZ4HCCompression:
#if LZ4_VERSION_NUMBER >= 10400  // r124+
      return LZ4_Supported();
#else
      return false;
#endif
    case kXpressCompression:
      return false;
    case kZSTD:
      // NB: dictionary supported since 0.5.0. See ZSTD_VERSION_NUMBER check
      // above.
      return ZSTD_Supported();
    default:  // Including custom compression types
      return false;
  }
}

// WART: does not match OptionsHelper::compression_type_string_map
std::string CompressionTypeToString(CompressionType compression_type);

// WART: does not match OptionsHelper::compression_type_string_map
CompressionType CompressionTypeFromString(std::string compression_type_str);

std::string CompressionOptionsToString(
    const CompressionOptions& compression_options);

inline bool ZSTD_TrainDictionarySupported() {
#ifdef ZSTD
  // NB: Dictionary trainer is available since v0.6.1 for static linking, but
  // not available for dynamic linking until v1.1.3. See ZSTD_VERSION_NUMBER
  // check above.
  return true;
#else
  return false;
#endif
}

inline bool ZSTD_FinalizeDictionarySupported() {
#ifdef ROCKSDB_ZDICT_FINALIZE
  return true;
#else
  return false;
#endif
}

// Use to check whether compression types are related or unrelated
inline CompressionType CanonicalCompressionType(CompressionType type) {
  switch (type) {
    // Configuring LZ4 or LZ4HC can result in using the other, depending on
    // compression level.
    case kLZ4HCCompression:
      return kLZ4Compression;
    default:
      return type;
  }
}

// The new compression APIs intentionally make it difficult to generate
// compressed data larger than the original. (It is better to store the
// uncompressed version in that case.) For legacy cases that must store
// compressed data even when larger than the uncompressed, this is a convenient
// wrapper to support that, with a compressor from BuiltinCompressionManager and
// a GrowableBuffer.
Status LegacyForceBuiltinCompression(
    Compressor& builtin_compressor,
    Compressor::ManagedWorkingArea* working_area, Slice from,
    GrowableBuffer* to);

// Records the compression type for subsequent WAL records.
class CompressionTypeRecord {
 public:
  explicit CompressionTypeRecord(CompressionType compression_type)
      : compression_type_(compression_type) {}

  CompressionType GetCompressionType() const { return compression_type_; }

  inline void EncodeTo(std::string* dst) const {
    assert(dst != nullptr);
    PutFixed32(dst, compression_type_);
  }

  inline Status DecodeFrom(Slice* src) {
    constexpr char class_name[] = "CompressionTypeRecord";

    uint32_t val;
    if (!GetFixed32(src, &val)) {
      return Status::Corruption(class_name,
                                "Error decoding WAL compression type");
    }
    CompressionType compression_type = static_cast<CompressionType>(val);
    if (!StreamingCompressionTypeSupported(compression_type)) {
      return Status::Corruption(class_name,
                                "WAL compression type not supported");
    }
    compression_type_ = compression_type;
    return Status::OK();
  }

  inline std::string DebugString() const {
    return "compression_type: " + CompressionTypeToString(compression_type_);
  }

 private:
  CompressionType compression_type_;
};

// Sink for StreamingCompress::CompressAndFlushToSink. Chunks arrive in
// compression order; returning non-OK aborts the flush and the error is
// relayed to the CompressAndFlushToSink caller.
class StreamingCompressOutput {
 public:
  virtual ~StreamingCompressOutput() = default;
  virtual Status Append(const char* data, size_t len) = 0;
};

// Base class to implement compression for a stream of buffers.
// Instantiate an implementation of the class using Create() with the
// compression type and use Compress() repeatedly.
// The output buffer needs to be at least max_output_len.
// Call Reset() in between frame boundaries or in case of an error.
// NOTE: This class is not thread safe.
class StreamingCompress {
 public:
  StreamingCompress(CompressionType compression_type,
                    const CompressionOptions& opts,
                    uint32_t compress_format_version, size_t max_output_len)
      : compression_type_(compression_type),
        opts_(opts),
        compress_format_version_(compress_format_version),
        max_output_len_(max_output_len) {}
  virtual ~StreamingCompress() = default;
  // compress should be called repeatedly with the same input till the method
  // returns 0
  // Parameters:
  // input - buffer to compress
  // input_size - size of input buffer
  // output - compressed buffer allocated by caller, should be at least
  // max_output_len
  // output_size - size of the output buffer
  // Returns -1 for errors, the remaining size of the input buffer that needs
  // to be compressed
  virtual int Compress(const char* input, size_t input_size, char* output,
                       size_t* output_pos) = 0;
  // static method to create object of a class inherited from
  // StreamingCompress based on the actual compression type.
  static std::unique_ptr<StreamingCompress> Create(
      CompressionType compression_type, const CompressionOptions& opts,
      uint32_t compress_format_version, size_t max_output_len);
  // Unlike Compress(), which ends a frame per input, compresses all of
  // `input` as a continuation of the current frame and appends the output to
  // `*output`, flushed so that the output since the last Reset() decodes to
  // all input since then. Later input may reference earlier input, so the
  // output is only decodable in order. Not limited by max_output_len. Must
  // not be mixed with Compress() within a frame. On error, the frame is
  // Reset() and earlier output can no longer be continued.
  virtual Status CompressAndFlush(const Slice& input, std::string* output) = 0;
  // Same continuation-and-flush semantics as CompressAndFlush(), but the
  // output is delivered to `*output` in chunks instead of being accumulated.
  // Not limited by max_output_len. Must not be mixed with Compress() within
  // a frame. A compression error Reset()s the frame like CompressAndFlush();
  // a sink error is relayed as-is without touching the frame, but the caller
  // must still abandon the stream: the input was consumed and later output
  // would reference bytes the sink never received.
  virtual Status CompressAndFlushToSink(const Slice& input,
                                        StreamingCompressOutput* output) = 0;
  virtual void Reset() = 0;

 protected:
  const CompressionType compression_type_;
  const CompressionOptions opts_;
  const uint32_t compress_format_version_;
  const size_t max_output_len_;
};

// Base class to uncompress a stream of compressed buffers.
// Instantiate an implementation of the class using Create() with the
// compression type and use Uncompress() repeatedly.
// The output buffer needs to be at least max_output_len.
// Call Reset() in between frame boundaries or in case of an error.
// NOTE: This class is not thread safe.
class StreamingUncompress {
 public:
  StreamingUncompress(CompressionType compression_type,
                      uint32_t compress_format_version, size_t max_output_len)
      : compression_type_(compression_type),
        compress_format_version_(compress_format_version),
        max_output_len_(max_output_len) {}
  virtual ~StreamingUncompress() = default;
  // Uncompress can be called repeatedly to progressively process the same
  // input buffer, or can be called with a new input buffer. When the input
  // buffer is not fully consumed, the return value is > 0 or output_size
  // == max_output_len. When calling uncompress to continue processing the
  // same input buffer, the input argument should be nullptr.
  // Parameters:
  // input - buffer to uncompress
  // input_size - size of input buffer
  // output - uncompressed buffer allocated by caller, should be at least
  // max_output_len
  // output_size - size of the output buffer
  // Returns -1 for errors, remaining input to be processed otherwise.
  virtual int Uncompress(const char* input, size_t input_size, char* output,
                         size_t* output_pos) = 0;
  static std::unique_ptr<StreamingUncompress> Create(
      CompressionType compression_type, uint32_t compress_format_version,
      size_t max_output_len);
  virtual void Reset() = 0;

 protected:
  CompressionType compression_type_;
  uint32_t compress_format_version_;
  size_t max_output_len_;
};

class ZSTDStreamingCompress final : public StreamingCompress {
 public:
  explicit ZSTDStreamingCompress(const CompressionOptions& opts,
                                 uint32_t compress_format_version,
                                 size_t max_output_len)
      : StreamingCompress(kZSTD, opts, compress_format_version,
                          max_output_len) {
#ifdef ZSTD
    cctx_ = ZSTD_createCCtx();
    // Each compressed frame will have a checksum
    ZSTD_CCtx_setParameter(cctx_, ZSTD_c_checksumFlag, 1);
    assert(cctx_ != nullptr);
    input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
  }
  ~ZSTDStreamingCompress() override {
#ifdef ZSTD
    ZSTD_freeCCtx(cctx_);
#endif
  }
  // No copying allowed
  ZSTDStreamingCompress(const ZSTDStreamingCompress&) = delete;
  ZSTDStreamingCompress& operator=(const ZSTDStreamingCompress&) = delete;
  ZSTDStreamingCompress(ZSTDStreamingCompress&&) = delete;
  ZSTDStreamingCompress& operator=(ZSTDStreamingCompress&&) = delete;
  int Compress(const char* input, size_t input_size, char* output,
               size_t* output_pos) override;
  Status CompressAndFlush(const Slice& input, std::string* output) override;
  Status CompressAndFlushToSink(const Slice& input,
                                StreamingCompressOutput* output) override;
  void Reset() override;
#ifdef ZSTD
  ZSTD_CCtx* cctx_;
  ZSTD_inBuffer input_buffer_;
  // Fixed-size output chunk for CompressAndFlushToSink, sized once from
  // ZSTD_CStreamOutSize() and reused across calls.
  std::string chunk_;
#endif
};

class ZSTDStreamingUncompress final : public StreamingUncompress {
 public:
  explicit ZSTDStreamingUncompress(uint32_t compress_format_version,
                                   size_t max_output_len)
      : StreamingUncompress(kZSTD, compress_format_version, max_output_len) {
#ifdef ZSTD
    dctx_ = ZSTD_createDCtx();
    assert(dctx_ != nullptr);
    input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
  }
  ~ZSTDStreamingUncompress() override {
#ifdef ZSTD
    ZSTD_freeDCtx(dctx_);
#endif
  }
  // No copying allowed
  ZSTDStreamingUncompress(const ZSTDStreamingUncompress&) = delete;
  ZSTDStreamingUncompress& operator=(const ZSTDStreamingUncompress&) = delete;
  ZSTDStreamingUncompress(ZSTDStreamingUncompress&&) = delete;
  ZSTDStreamingUncompress& operator=(ZSTDStreamingUncompress&&) = delete;
  int Uncompress(const char* input, size_t input_size, char* output,
                 size_t* output_size) override;
  void Reset() override;

 private:
#ifdef ZSTD
  ZSTD_DCtx* dctx_;
  ZSTD_inBuffer input_buffer_;
#endif
};

}  // namespace ROCKSDB_NAMESPACE
