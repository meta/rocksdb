// Copyright (c) 2022-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/compression.h"

#ifdef BZIP2
#include <bzlib.h>
#endif  // BZIP2

#include <limits>

#ifdef LZ4
#include <lz4.h>
#include <lz4hc.h>
#if LZ4_VERSION_NUMBER < 10700  // < r129
#error "LZ4 support requires version >= 1.7.0 (lz4-devel)"
#endif  // LZ4_VERSION_NUMBER < 10700
#endif  // LZ4

#ifdef SNAPPY
#include <snappy-sinksource.h>
#include <snappy.h>
#endif  // SNAPPY

#ifdef ZLIB
#include <zlib.h>
#endif  // ZLIB

#include "options/options_helper.h"
#include "port/likely.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// WART: does not match OptionsHelper::compression_type_string_map
std::string CompressionTypeToString(CompressionType compression_type) {
  switch (compression_type) {
    case kNoCompression:
      return "NoCompression";
    case kSnappyCompression:
      return "Snappy";
    case kZlibCompression:
      return "Zlib";
    case kBZip2Compression:
      return "BZip2";
    case kLZ4Compression:
      return "LZ4";
    case kLZ4HCCompression:
      return "LZ4HC";
    case kXpressCompression:
      return "Xpress";
    case kZSTD:
      return "ZSTD";
    case kDisableCompressionOption:
      return "DisableOption";
    default: {
      bool is_custom = compression_type >= kFirstCustomCompression &&
                       compression_type <= kLastCustomCompression;
      unsigned char c = lossless_cast<unsigned char>(compression_type);
      return (is_custom ? "Custom" : "Reserved") +
             ToBaseCharsString<16>(2, c, /*uppercase=*/true);
    }
  }
}

// WART: does not match OptionsHelper::compression_type_string_map
CompressionType CompressionTypeFromString(std::string compression_type_str) {
  if (!compression_type_str.empty()) {
    switch (compression_type_str[0]) {
      case 'N':
        if (compression_type_str == "NoCompression") {
          return kNoCompression;
        }
        break;
      case 'S':
        if (compression_type_str == "Snappy") {
          return kSnappyCompression;
        }
        break;
      case 'Z':
        if (compression_type_str == "ZSTD") {
          return kZSTD;
        }
        if (compression_type_str == "Zlib") {
          return kZlibCompression;
        }
        break;
      case 'B':
        if (compression_type_str == "BZip2") {
          return kBZip2Compression;
        }
        break;
      case 'L':
        if (compression_type_str == "LZ4") {
          return kLZ4Compression;
        }
        if (compression_type_str == "LZ4HC") {
          return kLZ4HCCompression;
        }
        break;
      case 'X':
        if (compression_type_str == "Xpress") {
          return kXpressCompression;
        }
        break;
      default:;
    }
  }
  // unrecognized
  return kDisableCompressionOption;
}

std::string CompressionOptionsToString(
    const CompressionOptions& compression_options) {
  std::string result;
  result.reserve(512);
  result.append("window_bits=")
      .append(std::to_string(compression_options.window_bits))
      .append("; ");
  result.append("level=")
      .append(std::to_string(compression_options.level))
      .append("; ");
  result.append("strategy=")
      .append(std::to_string(compression_options.strategy))
      .append("; ");
  result.append("max_dict_bytes=")
      .append(std::to_string(compression_options.max_dict_bytes))
      .append("; ");
  result.append("zstd_max_train_bytes=")
      .append(std::to_string(compression_options.zstd_max_train_bytes))
      .append("; ");
  // NOTE: parallel_threads is skipped because it doesn't really affect the file
  // contents written, arguably doesn't belong in CompressionOptions
  result.append("enabled=")
      .append(std::to_string(compression_options.enabled))
      .append("; ");
  result.append("max_dict_buffer_bytes=")
      .append(std::to_string(compression_options.max_dict_buffer_bytes))
      .append("; ");
  result.append("use_zstd_dict_trainer=")
      .append(std::to_string(compression_options.use_zstd_dict_trainer))
      .append("; ");
  result.append("max_compressed_bytes_per_kb=")
      .append(std::to_string(compression_options.max_compressed_bytes_per_kb))
      .append("; ");
  // Only emitted when enabled: auto_skip is experimental and default-off, so
  // keeping it out of the common case avoids enlarging every SST's persisted
  // compression_options property. Emitting it unconditionally grows every SST
  // by ~40 bytes, which overflows the tight size tolerance in
  // BlockBasedTableTest.CompressionRatioThreshold (table/table_test.cc). (The
  // canonical options serialization always includes it; this is a curated
  // summary.) auto_skip_min_sample_every is only meaningful when auto_skip is
  // on, so it is gated the same way.
  if (compression_options.auto_skip) {
    result.append("auto_skip=")
        .append(std::to_string(compression_options.auto_skip))
        .append("; ");
    result.append("auto_skip_min_sample_every=")
        .append(std::to_string(compression_options.auto_skip_min_sample_every))
        .append("; ");
  }
  result.append("checksum=")
      .append(std::to_string(compression_options.checksum))
      .append("; ");
  return result;
}

std::unique_ptr<StreamingCompress> StreamingCompress::Create(
    CompressionType compression_type, const CompressionOptions& opts,
    uint32_t compress_format_version, size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return std::make_unique<ZSTDStreamingCompress>(
          opts, compress_format_version, max_output_len);
    }
    default:
      return nullptr;
  }
}

std::unique_ptr<StreamingUncompress> StreamingUncompress::Create(
    CompressionType compression_type, uint32_t compress_format_version,
    size_t max_output_len) {
  switch (compression_type) {
    case kZSTD: {
      if (!ZSTD_Streaming_Supported()) {
        return nullptr;
      }
      return std::make_unique<ZSTDStreamingUncompress>(compress_format_version,
                                                       max_output_len);
    }
    default:
      return nullptr;
  }
}

int ZSTDStreamingCompress::Compress(const char* input, size_t input_size,
                                    char* output, size_t* output_pos) {
  assert(input != nullptr && output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to compress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifndef ZSTD
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#else
  if (input_buffer_.src == nullptr || input_buffer_.src != input) {
    // New input
    // Catch errors where the previous input was not fully decompressed.
    assert(input_buffer_.pos == input_buffer_.size);
    input_buffer_ = {input, input_size, /*pos=*/0};
  } else if (input_buffer_.src == input) {
    // Same input, not fully compressed.
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  const size_t remaining =
      ZSTD_compressStream2(cctx_, &output_buffer, &input_buffer_, ZSTD_e_end);
  if (ZSTD_isError(remaining)) {
    // Failure
    Reset();
    return -1;
  }
  // Success
  *output_pos = output_buffer.pos;
  return (int)remaining;
#endif
}

void ZSTDStreamingCompress::Reset() {
#ifdef ZSTD
  ZSTD_CCtx_reset(cctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

int ZSTDStreamingUncompress::Uncompress(const char* input, size_t input_size,
                                        char* output, size_t* output_pos) {
  assert(output != nullptr && output_pos != nullptr);
  *output_pos = 0;
  // Don't need to uncompress an empty input
  if (input_size == 0) {
    return 0;
  }
#ifdef ZSTD
  if (input) {
    // New input
    input_buffer_ = {input, input_size, /*pos=*/0};
  }
  ZSTD_outBuffer output_buffer = {output, max_output_len_, /*pos=*/0};
  size_t ret = ZSTD_decompressStream(dctx_, &output_buffer, &input_buffer_);
  if (ZSTD_isError(ret)) {
    Reset();
    return -1;
  }
  *output_pos = output_buffer.pos;
  return (int)(input_buffer_.size - input_buffer_.pos);
#else
  (void)input;
  (void)input_size;
  (void)output;
  return -1;
#endif
}

void ZSTDStreamingUncompress::Reset() {
#ifdef ZSTD
  ZSTD_DCtx_reset(dctx_, ZSTD_ResetDirective::ZSTD_reset_session_only);
  input_buffer_ = {/*src=*/nullptr, /*size=*/0, /*pos=*/0};
#endif
}

void DecompressorDict::Populate(Decompressor& from_decompressor, Slice dict) {
  if (UNLIKELY(dict.empty())) {
    dict_str_ = {};
    dict_allocation_ = {};
    // Appropriately reject bad files with empty dictionary block.
    // It is longstanding not to write an empty dictionary block:
    // https://github.com/meta/rocksdb/blame/10.2.fb/table/block_based/block_based_table_builder.cc#L1841
    decompressor_ = std::make_unique<FailureDecompressor>(
        Status::Corruption("Decompression dictionary is empty"));
  } else {
    Status s = from_decompressor.MaybeCloneForDict(dict, &decompressor_);
    if (decompressor_ == nullptr) {
      dict_str_ = {};
      dict_allocation_ = {};
      assert(!s.ok());
      decompressor_ = std::make_unique<FailureDecompressor>(std::move(s));
    } else {
      assert(s.ok());
      assert(decompressor_->GetSerializedDict() == dict);
    }
  }

  memory_usage_ = sizeof(struct DecompressorDict);
  memory_usage_ += dict_str_.size();
  if (dict_allocation_) {
    auto allocator = dict_allocation_.get_deleter().allocator;
    if (allocator) {
      memory_usage_ +=
          allocator->UsableSize(dict_allocation_.get(), GetRawDict().size());
    } else {
      memory_usage_ += GetRawDict().size();
    }
  }
  memory_usage_ += decompressor_->ApproximateOwnedMemoryUsage();
}

// ZSTD dictionary training implementations
std::string ZSTD_TrainDictionary(const std::string& samples,
                                 const std::vector<size_t>& sample_lens,
                                 size_t max_dict_bytes) {
#ifdef ZSTD
  assert(samples.empty() == sample_lens.empty());
  if (samples.empty()) {
    return "";
  }
  std::string dict_data(max_dict_bytes, '\0');
  size_t dict_len = ZDICT_trainFromBuffer(
      &dict_data[0], max_dict_bytes, &samples[0], &sample_lens[0],
      static_cast<unsigned>(sample_lens.size()));
  if (ZDICT_isError(dict_len)) {
    return "";
  }
  assert(dict_len <= max_dict_bytes);
  dict_data.resize(dict_len);
  return dict_data;
#else
  assert(false);
  (void)samples;
  (void)sample_lens;
  (void)max_dict_bytes;
  return "";
#endif  // ZSTD
}

std::string ZSTD_TrainDictionary(const std::string& samples,
                                 size_t sample_len_shift,
                                 size_t max_dict_bytes) {
#ifdef ZSTD
  // skips potential partial sample at the end of "samples"
  size_t num_samples = samples.size() >> sample_len_shift;
  std::vector<size_t> sample_lens(num_samples, size_t(1) << sample_len_shift);
  return ZSTD_TrainDictionary(samples, sample_lens, max_dict_bytes);
#else
  assert(false);
  (void)samples;
  (void)sample_len_shift;
  (void)max_dict_bytes;
  return "";
#endif  // ZSTD
}

std::string ZSTD_FinalizeDictionary(const std::string& samples,
                                    const std::vector<size_t>& sample_lens,
                                    size_t max_dict_bytes, int level) {
#ifdef ROCKSDB_ZDICT_FINALIZE
  assert(samples.empty() == sample_lens.empty());
  if (samples.empty()) {
    return "";
  }
  level = SanitizeZSTDCompressionLevel(level);
  std::string dict_data(max_dict_bytes, '\0');
  size_t dict_len = ZDICT_finalizeDictionary(
      dict_data.data(), max_dict_bytes, samples.data(),
      std::min(static_cast<size_t>(samples.size()), max_dict_bytes),
      samples.data(), sample_lens.data(),
      static_cast<unsigned>(sample_lens.size()),
      {level, 0 /* notificationLevel */, 0 /* dictID */});
  if (ZDICT_isError(dict_len)) {
    return "";
  } else {
    assert(dict_len <= max_dict_bytes);
    dict_data.resize(dict_len);
    return dict_data;
  }
#else
  assert(false);
  (void)samples;
  (void)sample_lens;
  (void)max_dict_bytes;
  (void)level;
  return "";
#endif  // ROCKSDB_ZDICT_FINALIZE
}

// ***********************************************************************
// BEGIN built-in implementation of customization interface
// ***********************************************************************
Status Decompressor::ExtractUncompressedSize(Args& args) {
  // Default implementation:
  //
  // Standard format for prepending uncompressed size to the compressed
  // payload. (RocksDB compress_format_version=2 except Snappy)
  //
  // This is historically a varint32, but it is preliminarily generalized
  // to varint64, in case that is supported on the write side for some
  // algorithms.
  if (LIKELY(GetVarint64(&args.compressed_data, &args.uncompressed_size))) {
    if (LIKELY(args.uncompressed_size <= SIZE_MAX)) {
      return Status::OK();
    } else {
      return Status::MemoryLimit("Uncompressed size too large for platform");
    }
  } else {
    return Status::Corruption("Unable to extract uncompressed size");
  }
}

const Slice& Decompressor::GetSerializedDict() const {
  // Default: empty slice => no dictionary
  static Slice kEmptySlice;
  return kEmptySlice;
}

namespace {

class CompressorBase : public Compressor {
 public:
  explicit CompressorBase(const CompressionOptions& opts) : opts_(opts) {}

  uint32_t GetRecommendedParallelThreads() const override {
    return opts_.parallel_threads;
  }

 protected:
  CompressionOptions opts_;
};

class CompressorWithSimpleDictBase : public CompressorBase {
 public:
  explicit CompressorWithSimpleDictBase(const CompressionOptions& opts,
                                        std::string&& dict_data = {})
      : CompressorBase(opts), dict_data_(std::move(dict_data)) {}

  DictConfig GetDictGuidance(CacheEntryRole /*block_type*/) const override {
    if (opts_.max_dict_bytes == 0) {
      return DictDisabled{};
    }
    return DictSampling{opts_.max_dict_bytes};
  }

  // NOTE: empty dict is equivalent to no dict
  Slice GetSerializedDict() const override { return dict_data_; }

  std::unique_ptr<Compressor> Clone() const override {
    return CloneForDict(std::string{dict_data_});
  }

  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole /*block_type*/, DictConfigArgs&& dict_config) const final {
    if (auto* samples = std::get_if<DictSamples>(&dict_config)) {
      assert(samples->Verify());
      if (samples->empty()) {
        return nullptr;
      }
      return CloneForDict(std::move(samples->sample_data));
    } else if (auto* predef = std::get_if<DictPreDefined>(&dict_config)) {
      if (predef->dict_data.empty()) {
        return nullptr;
      }
      return CloneForDict(std::move(predef->dict_data));
    } else {
      assert(std::holds_alternative<DictDisabled>(dict_config));
      return nullptr;
    }
  }

  virtual std::unique_ptr<Compressor> CloneForDict(
      std::string&& dict_data) const = 0;

 protected:
  const std::string dict_data_;
};

// NOTE: the legacy behavior is to pretend to use dictionary compression when
// enabled, including storing a dictionary block, but to ignore it. That is
// matched here.
class BuiltinSnappyCompressorV2 final : public CompressorWithSimpleDictBase {
 public:
  using CompressorWithSimpleDictBase::CompressorWithSimpleDictBase;

  const char* Name() const override { return "BuiltinSnappyCompressorV2"; }

  CompressionType GetPreferredCompressionType() const override {
    return kSnappyCompression;
  }

  // Snappy is more often SLOWER with parallel compression than faster.
  uint32_t GetRecommendedParallelThreads() const override { return 1; }

  std::unique_ptr<Compressor> CloneForDict(
      std::string&& dict_data) const override {
    return std::make_unique<BuiltinSnappyCompressorV2>(opts_,
                                                       std::move(dict_data));
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea*) override {
#ifdef SNAPPY
    struct MySink : public snappy::Sink {
      MySink(char* output, size_t output_size)
          : output_(output), output_size_(output_size) {}

      char* output_;
      size_t output_size_;
      size_t pos_ = 0;

      void Append(const char* data, size_t n) override {
        if (pos_ + n <= output_size_) {
          std::memcpy(output_ + pos_, data, n);
          pos_ += n;
        } else {
          // Virtual abort
          pos_ = output_size_ + 1;
        }
      }

      char* GetAppendBuffer(size_t length, char* scratch) override {
        if (pos_ + length <= output_size_) {
          return output_ + pos_;
        }
        return scratch;
      }
    };
    MySink sink{compressed_output, *compressed_output_size};
    snappy::ByteArraySource source{uncompressed_data.data(),
                                   uncompressed_data.size()};

    size_t outlen = snappy::Compress(&source, &sink);
    if (outlen > 0 && sink.pos_ <= sink.output_size_) {
      // Compression kept/successful
      assert(outlen == sink.pos_);
      *compressed_output_size = outlen;
      *out_compression_type = kSnappyCompression;
      return Status::OK();
    }
    // Compression rejected
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }

  std::shared_ptr<Decompressor> GetOptimizedDecompressor() const override;
};

[[maybe_unused]]
std::pair<char*, size_t> StartCompressBlockV2(Slice uncompressed_data,
                                              char* compressed_output,
                                              size_t compressed_output_size) {
  if (  // Can't compress more than 4GB
      uncompressed_data.size() > std::numeric_limits<uint32_t>::max() ||
      // Need enough output space for encoding uncompressed size
      compressed_output_size <= 5) {
    // Compression bypassed
    return {nullptr, 0};
  }
  // Standard format for prepending uncompressed size to the compressed
  // data in compress_format_version=2
  char* alg_output = EncodeVarint32(
      compressed_output, static_cast<uint32_t>(uncompressed_data.size()));
  size_t alg_max_output_size =
      compressed_output_size - (alg_output - compressed_output);
  return {alg_output, alg_max_output_size};
}

class BuiltinZlibCompressorV2 final : public CompressorWithSimpleDictBase {
 public:
  using CompressorWithSimpleDictBase::CompressorWithSimpleDictBase;

  const char* Name() const override { return "BuiltinZlibCompressorV2"; }

  CompressionType GetPreferredCompressionType() const override {
    return kZlibCompression;
  }

  std::unique_ptr<Compressor> CloneForDict(
      std::string&& dict_data) const override {
    return std::make_unique<BuiltinZlibCompressorV2>(opts_,
                                                     std::move(dict_data));
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea*) override {
#ifdef ZLIB
    auto [alg_output, alg_max_output_size] = StartCompressBlockV2(
        uncompressed_data, compressed_output, *compressed_output_size);
    if (alg_max_output_size == 0) {
      // Compression bypassed
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }

    // The memLevel parameter specifies how much memory should be allocated for
    // the internal compression state.
    // memLevel=1 uses minimum memory but is slow and reduces compression ratio.
    // memLevel=9 uses maximum memory for optimal speed.
    // The default value is 8. See zconf.h for more details.
    static const int memLevel = 8;
    int level = opts_.level;
    if (level == CompressionOptions::kDefaultCompressionLevel) {
      level = Z_DEFAULT_COMPRESSION;
    }

    z_stream stream;
    memset(&stream, 0, sizeof(z_stream));

    // Initialize the zlib stream
    int st = deflateInit2(&stream, level, Z_DEFLATED, opts_.window_bits,
                          memLevel, opts_.strategy);
    if (st != Z_OK) {
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }

    // Set dictionary if available
    if (!dict_data_.empty()) {
      st = deflateSetDictionary(
          &stream, reinterpret_cast<const Bytef*>(dict_data_.data()),
          static_cast<unsigned int>(dict_data_.size()));
      if (st != Z_OK) {
        deflateEnd(&stream);
        *compressed_output_size = 0;
        *out_compression_type = kNoCompression;
        return Status::OK();
      }
    }

    // Set up input
    stream.next_in = (Bytef*)uncompressed_data.data();
    stream.avail_in = static_cast<unsigned int>(uncompressed_data.size());

    // Set up output
    stream.next_out = reinterpret_cast<Bytef*>(alg_output);
    stream.avail_out = static_cast<unsigned int>(alg_max_output_size);

    // Compress
    st = deflate(&stream, Z_FINISH);
    size_t outlen = alg_max_output_size - stream.avail_out;
    deflateEnd(&stream);

    if (st == Z_STREAM_END) {
      // Compression kept/successful
      *compressed_output_size =
          outlen + /*header size*/ (alg_output - compressed_output);
      *out_compression_type = kZlibCompression;
      return Status::OK();
    }
    // Compression failed or rejected
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }
};

class BuiltinBZip2CompressorV2 final : public CompressorWithSimpleDictBase {
 public:
  using CompressorWithSimpleDictBase::CompressorWithSimpleDictBase;

  const char* Name() const override { return "BuiltinBZip2CompressorV2"; }

  CompressionType GetPreferredCompressionType() const override {
    return kBZip2Compression;
  }

  std::unique_ptr<Compressor> CloneForDict(
      std::string&& dict_data) const override {
    return std::make_unique<BuiltinBZip2CompressorV2>(opts_,
                                                      std::move(dict_data));
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea*) override {
#ifdef BZIP2
    auto [alg_output, alg_max_output_size] = StartCompressBlockV2(
        uncompressed_data, compressed_output, *compressed_output_size);
    if (alg_max_output_size == 0) {
      // Compression bypassed
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }

    // BZip2 doesn't actually use the dictionary, but we store it for
    // compatibility similar to BuiltinSnappyCompressorV2

    // Initialize the bzip2 stream
    bz_stream stream;
    memset(&stream, 0, sizeof(bz_stream));

    // Block size 1 is 100K.
    // 0 is for silent.
    // 30 is the default workFactor
    int st = BZ2_bzCompressInit(&stream, 1, 0, 30);
    if (st != BZ_OK) {
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }

    // Set up input
    stream.next_in = const_cast<char*>(uncompressed_data.data());
    stream.avail_in = static_cast<unsigned int>(uncompressed_data.size());

    // Set up output
    stream.next_out = alg_output;
    stream.avail_out = static_cast<unsigned int>(alg_max_output_size);

    // Compress
    st = BZ2_bzCompress(&stream, BZ_FINISH);
    size_t outlen = alg_max_output_size - stream.avail_out;
    BZ2_bzCompressEnd(&stream);

    // Check for success
    if (st == BZ_STREAM_END) {
      // Compression kept/successful
      *compressed_output_size = outlen + (alg_output - compressed_output);
      *out_compression_type = kBZip2Compression;
      return Status::OK();
    }
    // Compression failed or rejected
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }
};

class BuiltinLZ4CompressorV2WithDict : public CompressorWithSimpleDictBase {
 public:
  using CompressorWithSimpleDictBase::CompressorWithSimpleDictBase;

  const char* Name() const override { return "BuiltinLZ4CompressorV2"; }

  CompressionType GetPreferredCompressionType() const override {
    return kLZ4Compression;
  }

  // LZ4 (accelerated, not LZ4HC) is more often SLOWER with parallel
  // compression than faster.
  uint32_t GetRecommendedParallelThreads() const override { return 1; }

  std::unique_ptr<Compressor> CloneForDict(
      std::string&& dict_data) const override {
    return std::make_unique<BuiltinLZ4CompressorV2WithDict>(
        opts_, std::move(dict_data));
  }

  ManagedWorkingArea ObtainWorkingArea() override {
#ifdef LZ4
    return {reinterpret_cast<WorkingArea*>(LZ4_createStream()), this};
#else
    return {};
#endif
  }
  void ReleaseWorkingArea(WorkingArea* wa) override {
    if (wa) {
#ifdef LZ4
      LZ4_freeStream(reinterpret_cast<LZ4_stream_t*>(wa));
#endif
    }
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
#ifdef LZ4
    auto [alg_output, alg_max_output_size] = StartCompressBlockV2(
        uncompressed_data, compressed_output, *compressed_output_size);
    if (alg_max_output_size == 0) {
      // Compression bypassed
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }

    ManagedWorkingArea tmp_wa;
    LZ4_stream_t* stream;
    if (wa != nullptr && wa->owner() == this) {
      stream = reinterpret_cast<LZ4_stream_t*>(wa->get());
#if LZ4_VERSION_NUMBER >= 10900  // >= version 1.9.0
      LZ4_resetStream_fast(stream);
#else
      LZ4_resetStream(stream);
#endif
    } else {
      tmp_wa = ObtainWorkingArea();
      stream = reinterpret_cast<LZ4_stream_t*>(tmp_wa.get());
    }
    if (!dict_data_.empty()) {
      // TODO: more optimization possible here?
      LZ4_loadDict(stream, dict_data_.data(),
                   static_cast<int>(dict_data_.size()));
    }
    int acceleration = LZ4AccelerationFromLevel(opts_.level);
    auto outlen = LZ4_compress_fast_continue(
        stream, uncompressed_data.data(), alg_output,
        static_cast<int>(uncompressed_data.size()),
        static_cast<int>(alg_max_output_size), acceleration);
    if (outlen > 0) {
      // Compression kept/successful
      size_t output_size = static_cast<size_t>(
          outlen + /*header size*/ (alg_output - compressed_output));
      assert(output_size <= *compressed_output_size);
      *compressed_output_size = output_size;
      *out_compression_type = kLZ4Compression;
      return Status::OK();
    }
    // Compression rejected
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    (void)wa;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }

 protected:
  // Translates a non-positive `level` to an LZ4 "acceleration" value (=
  // -level), clamped to the maximum effective acceleration. The clamp avoids
  // signed overflow when negating INT_MIN and reflects that lz4 internally caps
  // acceleration at LZ4_ACCELERATION_MAX (currently 65537; not exposed by
  // lz4.h).
  [[maybe_unused]] static int LZ4AccelerationFromLevel(int level) {
    assert(level <= 0 || level == CompressionOptions::kDefaultCompressionLevel);
    constexpr int kLZ4MaxAcceleration = 65537;  // == LZ4_ACCELERATION_MAX
    if (level <= -kLZ4MaxAcceleration) {
      return kLZ4MaxAcceleration;
    }
    if (level >= 0) {
      return 1;
    }
    return -level;
  }
};

class BuiltinLZ4CompressorV2NoDict final
    : public BuiltinLZ4CompressorV2WithDict {
 public:
  BuiltinLZ4CompressorV2NoDict(const CompressionOptions& opts)
      : BuiltinLZ4CompressorV2WithDict(opts, /*dict_data=*/{}) {}

  std::unique_ptr<Compressor> Clone() const override {
    return std::make_unique<BuiltinLZ4CompressorV2NoDict>(opts_);
  }

  ManagedWorkingArea ObtainWorkingArea() override {
    // Using an LZ4_stream_t between compressions and resetting with
    // LZ4_resetStream_fast is actually slower than using a fresh LZ4_stream_t
    // each time, or not involving a stream at all. Similarly, using an extState
    // does not seem to offer a performance boost, perhaps a small regression.
    return {};
  }

  void ReleaseWorkingArea(WorkingArea* wa) override {
    // Should not be called
    (void)wa;
    assert(wa == nullptr);
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
#ifdef LZ4
    (void)wa;
    auto [alg_output, alg_max_output_size] = StartCompressBlockV2(
        uncompressed_data, compressed_output, *compressed_output_size);
    if (alg_max_output_size == 0) {
      // Compression bypassed
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }
    int acceleration = LZ4AccelerationFromLevel(opts_.level);
    auto outlen =
        LZ4_compress_fast(uncompressed_data.data(), alg_output,
                          static_cast<int>(uncompressed_data.size()),
                          static_cast<int>(alg_max_output_size), acceleration);
    if (outlen > 0) {
      // Compression kept/successful
      size_t output_size = static_cast<size_t>(
          outlen + /*header size*/ (alg_output - compressed_output));
      assert(output_size <= *compressed_output_size);
      *compressed_output_size = output_size;
      *out_compression_type = kLZ4Compression;
      return Status::OK();
    }
    // Compression rejected
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    (void)wa;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }
};

class BuiltinLZ4HCCompressorV2 final : public CompressorWithSimpleDictBase {
 public:
  using CompressorWithSimpleDictBase::CompressorWithSimpleDictBase;

  const char* Name() const override { return "BuiltinLZ4HCCompressorV2"; }

  CompressionType GetPreferredCompressionType() const override {
    return kLZ4HCCompression;
  }

  std::unique_ptr<Compressor> CloneForDict(
      std::string&& dict_data) const override {
    return std::make_unique<BuiltinLZ4HCCompressorV2>(opts_,
                                                      std::move(dict_data));
  }

  ManagedWorkingArea ObtainWorkingArea() override {
#ifdef LZ4
    return {reinterpret_cast<WorkingArea*>(LZ4_createStreamHC()), this};
#else
    return {};
#endif
  }
  void ReleaseWorkingArea(WorkingArea* wa) override {
    if (wa) {
#ifdef LZ4
      LZ4_freeStreamHC(reinterpret_cast<LZ4_streamHC_t*>(wa));
#endif
    }
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
#ifdef LZ4
    auto [alg_output, alg_max_output_size] = StartCompressBlockV2(
        uncompressed_data, compressed_output, *compressed_output_size);
    if (alg_max_output_size == 0) {
      // Compression bypassed
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }

    int level = opts_.level;
    if (level == CompressionOptions::kDefaultCompressionLevel) {
      level = 0;  // lz4hc.h says any value < 1 will be sanitized to default
    } else if (level > LZ4HC_CLEVEL_MAX) {
      // Map large positive levels to the maximum effective level. lz4hc clamps
      // internally too, but make it explicit for clarity/determinism.
      level = LZ4HC_CLEVEL_MAX;
    }

    ManagedWorkingArea tmp_wa;
    LZ4_streamHC_t* stream;
    if (wa != nullptr && wa->owner() == this) {
      stream = reinterpret_cast<LZ4_streamHC_t*>(wa->get());
    } else {
      tmp_wa = ObtainWorkingArea();
      stream = reinterpret_cast<LZ4_streamHC_t*>(tmp_wa.get());
    }
#if LZ4_VERSION_NUMBER >= 10900  // >= version 1.9.0
    LZ4_resetStreamHC_fast(stream, level);
#else
    LZ4_resetStreamHC(stream, level);
#endif
    if (dict_data_.size() > 0) {
      // TODO: more optimization possible here?
      LZ4_loadDictHC(stream, dict_data_.data(),
                     static_cast<int>(dict_data_.size()));
    }

    auto outlen =
        LZ4_compress_HC_continue(stream, uncompressed_data.data(), alg_output,
                                 static_cast<int>(uncompressed_data.size()),
                                 static_cast<int>(alg_max_output_size));
    if (outlen > 0) {
      // Compression kept/successful
      size_t output_size = static_cast<size_t>(
          outlen + /*header size*/ (alg_output - compressed_output));
      assert(output_size <= *compressed_output_size);
      *compressed_output_size = output_size;
      *out_compression_type = kLZ4HCCompression;
      return Status::OK();
    }
    // Compression rejected
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    (void)wa;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }
};

class BuiltinXpressCompressorV2 final : public CompressorWithSimpleDictBase {
 public:
  using CompressorWithSimpleDictBase::CompressorWithSimpleDictBase;

  const char* Name() const override { return "BuiltinXpressCompressorV2"; }

  CompressionType GetPreferredCompressionType() const override {
    return kXpressCompression;
  }

  std::unique_ptr<Compressor> CloneForDict(
      std::string&& dict_data) const override {
    return std::make_unique<BuiltinXpressCompressorV2>(opts_,
                                                       std::move(dict_data));
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea*) override {
#ifdef XPRESS
    // XPRESS doesn't actually use the dictionary, but we store it for
    // compatibility similar to BuiltinSnappyCompressorV2

    // Use the new CompressWithMaxSize function that writes directly to the
    // output buffer
    size_t compressed_size = port::xpress::CompressWithMaxSize(
        uncompressed_data.data(), uncompressed_data.size(), compressed_output,
        *compressed_output_size);

    if (compressed_size > 0) {
      // Compression kept/successful
      *compressed_output_size = compressed_size;
      *out_compression_type = kXpressCompression;
      return Status::OK();
    }

    // Compression rejected or failed
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }
};

class BuiltinZSTDCompressorV2 final : public CompressorBase {
 public:
  explicit BuiltinZSTDCompressorV2(const CompressionOptions& opts,
                                   CompressionDict&& dict = {})
      : CompressorBase(opts), dict_(std::move(dict)) {}

  const char* Name() const override { return "BuiltinZSTDCompressorV2"; }

  CompressionType GetPreferredCompressionType() const override { return kZSTD; }

  uint32_t GetRecommendedParallelThreads() const override {
    // Accelerated ZSTD levels could see a small throughput increase with
    // parallel compression, but the overall CPU overhead is generally not worth
    // it.
    return opts_.level < 0 ? 1
                           : CompressorBase::GetRecommendedParallelThreads();
  }

  std::unique_ptr<Compressor> Clone() const override {
    CompressionDict dict_copy{dict_.GetRawDict().ToString(), kZSTD,
                              opts_.level};
    return std::make_unique<BuiltinZSTDCompressorV2>(opts_,
                                                     std::move(dict_copy));
  }

  DictConfig GetDictGuidance(CacheEntryRole /*block_type*/) const override {
    if (opts_.max_dict_bytes == 0) {
      // Dictionary compression disabled
      return DictDisabled{};
    } else {
      size_t max_sample_bytes = opts_.zstd_max_train_bytes > 0
                                    ? opts_.zstd_max_train_bytes
                                    : opts_.max_dict_bytes;
      return DictSampling{max_sample_bytes};
    }
  }

  // NOTE: empty dict is equivalent to no dict
  Slice GetSerializedDict() const override { return dict_.GetRawDict(); }

  ManagedWorkingArea ObtainWorkingArea() override {
#ifdef ZSTD
    ZSTD_CCtx* ctx =
#ifdef ROCKSDB_ZSTD_CUSTOM_MEM
        ZSTD_createCCtx_advanced(port::GetJeZstdAllocationOverrides());
#else   // ROCKSDB_ZSTD_CUSTOM_MEM
        ZSTD_createCCtx();
#endif  // ROCKSDB_ZSTD_CUSTOM_MEM
    int level = SanitizeZSTDCompressionLevel(opts_.level);
    size_t err = ZSTD_CCtx_setParameter(ctx, ZSTD_c_compressionLevel, level);
    if (ZSTD_isError(err)) {
      assert(false);
      ZSTD_freeCCtx(ctx);
      ctx = ZSTD_createCCtx();
    }
    if (opts_.checksum) {
      err = ZSTD_CCtx_setParameter(ctx, ZSTD_c_checksumFlag, 1);
      if (ZSTD_isError(err)) {
        assert(false);
        ZSTD_freeCCtx(ctx);
        ctx = ZSTD_createCCtx();
      }
    }
    return ManagedWorkingArea(reinterpret_cast<WorkingArea*>(ctx), this);
#else
    return {};
#endif  // ZSTD
  }

  void ReleaseWorkingArea(WorkingArea* wa) override {
    if (wa) {
#ifdef ZSTD
      ZSTD_freeCCtx(reinterpret_cast<ZSTD_CCtx*>(wa));
#endif  // ZSTD
    }
  }

  Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                       size_t* compressed_output_size,
                       CompressionType* out_compression_type,
                       ManagedWorkingArea* wa) override {
#ifdef ZSTD
    auto [alg_output, alg_max_output_size] = StartCompressBlockV2(
        uncompressed_data, compressed_output, *compressed_output_size);
    if (alg_max_output_size == 0) {
      // Compression bypassed
      *compressed_output_size = 0;
      *out_compression_type = kNoCompression;
      return Status::OK();
    }

    ManagedWorkingArea tmp_wa;
    if (wa == nullptr || wa->owner() != this) {
      tmp_wa = ObtainWorkingArea();
      wa = &tmp_wa;
    }
    assert(wa->get() != nullptr);
    ZSTD_CCtx* ctx = reinterpret_cast<ZSTD_CCtx*>(wa->get());

    if (dict_.GetDigestedZstdCDict() != nullptr) {
      ZSTD_CCtx_refCDict(ctx, dict_.GetDigestedZstdCDict());
    } else {
      ZSTD_CCtx_loadDictionary(ctx, dict_.GetRawDict().data(),
                               dict_.GetRawDict().size());
    }

    // Compression level is set in `contex` during ObtainWorkingArea()
    size_t outlen =
        ZSTD_compress2(ctx, alg_output, alg_max_output_size,
                       uncompressed_data.data(), uncompressed_data.size());
    if (!ZSTD_isError(outlen)) {
      // Compression kept/successful
      size_t output_size = static_cast<size_t>(
          outlen + /*header size*/ (alg_output - compressed_output));
      assert(output_size <= *compressed_output_size);
      *compressed_output_size = output_size;
      *out_compression_type = kZSTD;
      return Status::OK();
    }
    if (ZSTD_getErrorCode(outlen) != ZSTD_error_dstSize_tooSmall) {
      return Status::Corruption(std::string("ZSTD_compress2 failed: ") +
                                ZSTD_getErrorName(outlen));
    }
    // Compression rejected
    *compressed_output_size = 1;
#else
    (void)uncompressed_data;
    (void)compressed_output;
    (void)wa;
    // Compression bypassed (not supported)
    *compressed_output_size = 0;
#endif
    *out_compression_type = kNoCompression;
    return Status::OK();
  }

  std::unique_ptr<Compressor> MaybeCloneSpecialized(
      CacheEntryRole /*block_type*/,
      DictConfigArgs&& dict_config) const override {
    // Handle DictDisabled
    // TODO: use holds_alternative
    if (auto* disabled = std::get_if<DictDisabled>(&dict_config)) {
      (void)disabled;
      return nullptr;
    }

    std::string dict_data;

    // Handle DictPreDefined - use the pre-defined dictionary directly
    if (auto* predef = std::get_if<DictPreDefined>(&dict_config)) {
      if (predef->dict_data.empty()) {
        return nullptr;
      }
      dict_data = std::move(predef->dict_data);
    }

    // Handle DictSamples - train dictionary from samples
    if (auto* samples = std::get_if<DictSamples>(&dict_config)) {
      assert(samples->Verify());
      if (samples->empty()) {
        return nullptr;
      }
      // Migrated from BlockBasedTableBuilder::EnterUnbuffered()
      if (opts_.zstd_max_train_bytes > 0) {
        assert(samples->sample_data.size() <= opts_.zstd_max_train_bytes);
        if (opts_.use_zstd_dict_trainer) {
          dict_data = ZSTD_TrainDictionary(
              samples->sample_data, samples->sample_lens, opts_.max_dict_bytes);
        } else {
          dict_data = ZSTD_FinalizeDictionary(
              samples->sample_data, samples->sample_lens, opts_.max_dict_bytes,
              opts_.level);
        }
      } else {
        assert(samples->sample_data.size() <= opts_.max_dict_bytes);
        // ZSTD "raw content dictionary" - "Any buffer is a valid raw content
        // dictionary." Or similar for other compressions.
        dict_data = std::move(samples->sample_data);
      }
    }

    CompressionDict dict{std::move(dict_data), kZSTD, opts_.level};
    return std::make_unique<BuiltinZSTDCompressorV2>(opts_, std::move(dict));
  }

  std::shared_ptr<Decompressor> GetOptimizedDecompressor() const override;

 protected:
  const CompressionDict dict_;
};

// Subroutines for BuiltinDecompressorV2

Status Snappy_DecompressBlock(const Decompressor::Args& args,
                              char* uncompressed_output) {
#ifdef SNAPPY
  if (!snappy::RawUncompress(args.compressed_data.data(),
                             args.compressed_data.size(),
                             uncompressed_output)) {
    return Status::Corruption("Error decompressing snappy data");
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("Snappy not supported in this build");
#endif
}

Status Zlib_DecompressBlock(const Decompressor::Args& args, Slice dict,
                            char* uncompressed_output) {
#ifdef ZLIB
  // NOTE: uses "raw" format
  constexpr int kWindowBits = -14;

  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));

  // For raw inflate, the windowBits should be -8..-15.
  // If windowBits is bigger than zero, it will use either zlib
  // header or gzip header. Adding 32 to it will do automatic detection.
  int st = inflateInit2(&_stream, kWindowBits);
  if (UNLIKELY(st != Z_OK)) {
    return Status::Corruption("Failed to initialize zlib inflate: " +
                              std::to_string(st));
  }

  if (!dict.empty()) {
    // Initialize the compression library's dictionary
    st = inflateSetDictionary(&_stream,
                              reinterpret_cast<const Bytef*>(dict.data()),
                              static_cast<unsigned int>(dict.size()));
    if (UNLIKELY(st != Z_OK)) {
      return Status::Corruption("Failed to initialize zlib dictionary: " +
                                std::to_string(st));
    }
  }

  _stream.next_in = const_cast<Bytef*>(
      reinterpret_cast<const Bytef*>(args.compressed_data.data()));
  _stream.avail_in = static_cast<unsigned int>(args.compressed_data.size());

  _stream.next_out = reinterpret_cast<Bytef*>(uncompressed_output);
  _stream.avail_out = static_cast<unsigned int>(args.uncompressed_size);

  st = inflate(&_stream, Z_SYNC_FLUSH);
  if (UNLIKELY(st != Z_STREAM_END)) {
    inflateEnd(&_stream);
    // NOTE: Z_OK is still corruption because it means we got the size wrong
    return Status::Corruption("Failed zlib inflate: " + std::to_string(st));
  }

  // We should have no bytes left
  if (_stream.avail_out != 0) {
    inflateEnd(&_stream);
    return Status::Corruption("Size mismatch decompressing zlib data");
  }

  inflateEnd(&_stream);
  return Status::OK();
#else
  (void)args;
  (void)dict;
  (void)uncompressed_output;
  return Status::NotSupported("Zlib not supported in this build");
#endif
}

Status BZip2_DecompressBlock(const Decompressor::Args& args,
                             char* uncompressed_output) {
#ifdef BZIP2
  auto uncompressed_size = static_cast<unsigned int>(args.uncompressed_size);
  if (BZ_OK != BZ2_bzBuffToBuffDecompress(
                   uncompressed_output, &uncompressed_size,
                   const_cast<char*>(args.compressed_data.data()),
                   static_cast<unsigned int>(args.compressed_data.size()),
                   0 /*small mem*/, 0 /*verbosity*/)) {
    return Status::Corruption("Error decompressing bzip2 data");
  }
  if (uncompressed_size != args.uncompressed_size) {
    return Status::Corruption("Size mismatch decompressing bzip2 data");
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("BZip2 not supported in this build");
#endif
}

Status LZ4_DecompressBlock(const Decompressor::Args& args, Slice dict,
                           char* uncompressed_output) {
#ifdef LZ4
  int expected_uncompressed_size = static_cast<int>(args.uncompressed_size);
  LZ4_streamDecode_t* stream = LZ4_createStreamDecode();
  if (!dict.empty()) {
    LZ4_setStreamDecode(stream, dict.data(), static_cast<int>(dict.size()));
  }
  int uncompressed_size = LZ4_decompress_safe_continue(
      stream, args.compressed_data.data(), uncompressed_output,
      static_cast<int>(args.compressed_data.size()),
      expected_uncompressed_size);
  LZ4_freeStreamDecode(stream);

  if (uncompressed_size != expected_uncompressed_size) {
    if (uncompressed_size < 0) {
      return Status::Corruption("Error decompressing LZ4 data");
    } else {
      return Status::Corruption("Size mismatch decompressing LZ4 data");
    }
  }
  return Status::OK();
#else
  (void)args;
  (void)dict;
  (void)uncompressed_output;
  return Status::NotSupported("LZ4 not supported in this build");
#endif
}

Status XPRESS_DecompressBlock(const Decompressor::Args& args,
                              char* uncompressed_output) {
#ifdef XPRESS
  int64_t actual_uncompressed_size = port::xpress::DecompressToBuffer(
      args.compressed_data.data(), args.compressed_data.size(),
      uncompressed_output, args.uncompressed_size);
  if (actual_uncompressed_size !=
      static_cast<int64_t>(args.uncompressed_size)) {
    if (actual_uncompressed_size < 0) {
      return Status::Corruption("Error decompressing XPRESS data");
    } else {
      return Status::Corruption("Size mismatch decompressing XPRESS data");
    }
  }
  return Status::OK();
#else
  (void)args;
  (void)uncompressed_output;
  return Status::NotSupported("XPRESS not supported in this build");
#endif
}

template <bool kIsDigestedDict = false>
Status ZSTD_DecompressBlockWithContext(
    const Decompressor::Args& args,
    std::conditional_t<kIsDigestedDict, void*, Slice> dict,
    ZSTDUncompressCachedData::ZSTDNativeContext zstd_context,
    char* uncompressed_output) {
#ifdef ZSTD
  size_t uncompressed_size;
  assert(zstd_context != nullptr);
  if constexpr (kIsDigestedDict) {
#ifdef ROCKSDB_ZSTD_DDICT
    uncompressed_size = ZSTD_decompress_usingDDict(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size(),
        static_cast<ZSTD_DDict*>(dict));
#else
    static_assert(!kIsDigestedDict,
                  "Inconsistent expectation of ZSTD digested dict support");
#endif  // ROCKSDB_ZSTD_DDICT
  } else if (dict.empty()) {
    uncompressed_size = ZSTD_decompressDCtx(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size());
  } else {
    uncompressed_size = ZSTD_decompress_usingDict(
        zstd_context, uncompressed_output, args.uncompressed_size,
        args.compressed_data.data(), args.compressed_data.size(), dict.data(),
        dict.size());
  }
  if (ZSTD_isError(uncompressed_size)) {
    return Status::Corruption(std::string("ZSTD ") +
                              ZSTD_getErrorName(uncompressed_size));
  } else if (uncompressed_size != args.uncompressed_size) {
    return Status::Corruption("ZSTD decompression size mismatch");
  } else {
    return Status::OK();
  }
#else
  (void)args;
  (void)dict;
  (void)zstd_context;
  (void)uncompressed_output;
  return Status::NotSupported("ZSTD not supported in this build");
#endif
}

template <bool kIsDigestedDict = false>
Status ZSTD_DecompressBlock(
    const Decompressor::Args& args,
    std::conditional_t<kIsDigestedDict, void*, Slice> dict,
    const Decompressor* decompressor, char* uncompressed_output) {
  if (args.working_area && args.working_area->owner() == decompressor) {
    auto ctx = static_cast<UncompressionContext*>(args.working_area->get());
    assert(ctx != nullptr);
    if (ctx->GetZSTDContext() != nullptr) {
      return ZSTD_DecompressBlockWithContext<kIsDigestedDict>(
          args, dict, ctx->GetZSTDContext(), uncompressed_output);
    }
  }
  UncompressionContext tmp_ctx{kZSTD};
  return ZSTD_DecompressBlockWithContext<kIsDigestedDict>(
      args, dict, tmp_ctx.GetZSTDContext(), uncompressed_output);
}

class BuiltinDecompressorV2 : public Decompressor {
 public:
  const char* Name() const override { return "BuiltinDecompressorV2"; }

  Status ExtractUncompressedSize(Args& args) override {
    assert(args.compression_type != kNoCompression);
    if (args.compression_type == kSnappyCompression) {
      // 1st exception to encoding of uncompressed size
#ifdef SNAPPY
      size_t uncompressed_length = 0;
      if (!snappy::GetUncompressedLength(args.compressed_data.data(),
                                         args.compressed_data.size(),
                                         &uncompressed_length)) {
        return Status::Corruption("Error reading snappy compressed length");
      }
      args.uncompressed_size = uncompressed_length;
      return Status::OK();
#else
      return Status::NotSupported("Snappy not supported in this build");
#endif
    } else if (args.compression_type == kXpressCompression) {
      // 2nd exception to encoding of uncompressed size
#ifdef XPRESS
      int64_t result = port::xpress::GetDecompressedSize(
          args.compressed_data.data(), args.compressed_data.size());
      if (result < 0) {
        return Status::Corruption("Error reading XPRESS compressed length");
      }
      args.uncompressed_size = static_cast<size_t>(result);
      return Status::OK();
#else
      return Status::NotSupported("XPRESS not supported in this build");
#endif

    } else {
      // Extract encoded uncompressed size
      return Decompressor::ExtractUncompressedSize(args);
    }
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    switch (args.compression_type) {
      case kSnappyCompression:
        return Snappy_DecompressBlock(args, uncompressed_output);
      case kZlibCompression:
        return Zlib_DecompressBlock(args, /*dict=*/Slice{},
                                    uncompressed_output);
      case kBZip2Compression:
        return BZip2_DecompressBlock(args, uncompressed_output);
      case kLZ4Compression:
      case kLZ4HCCompression:
        return LZ4_DecompressBlock(args, /*dict=*/Slice{}, uncompressed_output);
      case kXpressCompression:
        return XPRESS_DecompressBlock(args, uncompressed_output);
      case kZSTD:
        return ZSTD_DecompressBlock(args, /*dict=*/Slice{}, this,
                                    uncompressed_output);
      default:
        return Status::NotSupported(
            "Compression type not supported or not built-in: " +
            CompressionTypeToString(args.compression_type));
    }
  }

  Status MaybeCloneForDict(const Slice&,
                           std::unique_ptr<Decompressor>*) override;

  size_t ApproximateOwnedMemoryUsage() const override {
    return sizeof(BuiltinDecompressorV2);
  }
};

class BuiltinDecompressorV2SnappyOnly final : public BuiltinDecompressorV2 {
 public:
  const char* Name() const override {
    return "BuiltinDecompressorV2SnappyOnly";
  }

  Status ExtractUncompressedSize(Args& args) override {
    assert(args.compression_type == kSnappyCompression);
#ifdef SNAPPY
    size_t uncompressed_length = 0;
    if (!snappy::GetUncompressedLength(args.compressed_data.data(),
                                       args.compressed_data.size(),
                                       &uncompressed_length)) {
      return Status::Corruption("Error reading snappy compressed length");
    }
    args.uncompressed_size = uncompressed_length;
    return Status::OK();
#else
    (void)args;
    return Status::NotSupported("Snappy not supported in this build");
#endif
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    assert(args.compression_type == kSnappyCompression);
    return Snappy_DecompressBlock(args, uncompressed_output);
  }
};

class BuiltinDecompressorV2WithDict final : public BuiltinDecompressorV2 {
 public:
  explicit BuiltinDecompressorV2WithDict(const Slice& dict) : dict_(dict) {}

  const char* Name() const override { return "BuiltinDecompressorV2WithDict"; }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    switch (args.compression_type) {
      case kSnappyCompression:
        // NOTE: quietly ignores the dictionary (for compatibility)
        return Snappy_DecompressBlock(args, uncompressed_output);
      case kZlibCompression:
        return Zlib_DecompressBlock(args, dict_, uncompressed_output);
      case kBZip2Compression:
        // NOTE: quietly ignores the dictionary (for compatibility)
        return BZip2_DecompressBlock(args, uncompressed_output);
      case kLZ4Compression:
      case kLZ4HCCompression:
        return LZ4_DecompressBlock(args, dict_, uncompressed_output);
      case kXpressCompression:
        // NOTE: quietly ignores the dictionary (for compatibility)
        return XPRESS_DecompressBlock(args, uncompressed_output);
      case kZSTD:
        return ZSTD_DecompressBlock(args, dict_, this, uncompressed_output);
      default:
        return Status::NotSupported(
            "Compression type not supported or not built-in: " +
            CompressionTypeToString(args.compression_type));
    }
  }

  const Slice& GetSerializedDict() const override { return dict_; }

  size_t ApproximateOwnedMemoryUsage() const override {
    return sizeof(BuiltinDecompressorV2WithDict);
  }

 protected:
  const Slice dict_;
};

Status BuiltinDecompressorV2::MaybeCloneForDict(
    const Slice& dict, std::unique_ptr<Decompressor>* out) {
  // Check RocksDB-promised precondition
  assert(dict.size() > 0);
  // Because of unfortunate decisions in handling built-in compression types,
  // all the compression types before ZSTD that do not actually support
  // dictionary compression pretend to support it. Specifically, we have to be
  // able to read files with a compression dictionary block using those
  // compression types even though the compression dictionary is ignored by
  // the compression algorithm. And the Decompressor has to return the
  // configured dictionary from GetSerializedDict() even if it is ignored. This
  // unfortunately means that a new schema version (BuiltinV3?) would be needed
  // toactually support dictionary compression in the future for these
  // algorithms (if the libraries add support).
  // TODO: can we make this a better/cleaner experience?
  *out = std::make_unique<BuiltinDecompressorV2WithDict>(dict);
  return Status::OK();
}

class BuiltinDecompressorV2OptimizeZstd : public BuiltinDecompressorV2 {
 public:
  const char* Name() const override {
    return "BuiltinDecompressorV2OptimizeZstd";
  }

  ManagedWorkingArea ObtainWorkingArea(CompressionType preferred) override {
    if (preferred == kZSTD) {
      // TODO: evaluate whether it makes sense to use core local cache here.
      // (Perhaps not, because explicit WorkingArea could be long-running.)
      return ManagedWorkingArea(new UncompressionContext(kZSTD), this);
    } else {
      return {};
    }
  }

  void ReleaseWorkingArea(WorkingArea* wa) override {
    delete static_cast<UncompressionContext*>(wa);
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    if (LIKELY(args.compression_type == kZSTD)) {
      return ZSTD_DecompressBlock(args, /*dict=*/Slice{}, this,
                                  uncompressed_output);
    } else {
      return BuiltinDecompressorV2::DecompressBlock(args, uncompressed_output);
    }
  }

  Status MaybeCloneForDict(const Slice& /*serialized_dict*/,
                           std::unique_ptr<Decompressor>* /*out*/) override;
};

class BuiltinDecompressorV2OptimizeZstdWithDict final
    : public BuiltinDecompressorV2OptimizeZstd {
 public:
  explicit BuiltinDecompressorV2OptimizeZstdWithDict(const Slice& dict)
      :
#ifdef ROCKSDB_ZSTD_DDICT
        dict_(dict),
        ddict_(ZSTD_createDDict_byReference(dict.data(), dict.size())) {
    assert(ddict_ != nullptr);
  }
#else
        dict_(dict) {
  }
#endif  // ROCKSDB_ZSTD_DDICT

  // No copying allowed
  BuiltinDecompressorV2OptimizeZstdWithDict(
      const BuiltinDecompressorV2OptimizeZstdWithDict&) = delete;
  BuiltinDecompressorV2OptimizeZstdWithDict& operator=(
      const BuiltinDecompressorV2OptimizeZstdWithDict&) = delete;
  BuiltinDecompressorV2OptimizeZstdWithDict(
      BuiltinDecompressorV2OptimizeZstdWithDict&&) = delete;
  BuiltinDecompressorV2OptimizeZstdWithDict& operator=(
      BuiltinDecompressorV2OptimizeZstdWithDict&&) = delete;

  const char* Name() const override {
    return "BuiltinDecompressorV2OptimizeZstdWithDict";
  }

  ~BuiltinDecompressorV2OptimizeZstdWithDict() override {
#ifdef ROCKSDB_ZSTD_DDICT
    size_t res = ZSTD_freeDDict(ddict_);
    assert(res == 0);  // Last I checked they can't fail
    (void)res;         // prevent unused var warning
#endif                 // ROCKSDB_ZSTD_DDICT
  }

  const Slice& GetSerializedDict() const override { return dict_; }

  size_t ApproximateOwnedMemoryUsage() const override {
    size_t sz = sizeof(BuiltinDecompressorV2WithDict);
#ifdef ROCKSDB_ZSTD_DDICT
    sz += ZSTD_sizeof_DDict(ddict_);
#endif  // ROCKSDB_ZSTD_DDICT
    return sz;
  }

  Status DecompressBlock(const Args& args, char* uncompressed_output) override {
    if (LIKELY(args.compression_type == kZSTD)) {
#ifdef ROCKSDB_ZSTD_DDICT
      return ZSTD_DecompressBlock</*kIsDigestedDict=*/true>(
          args, ddict_, this, uncompressed_output);
#else
      return ZSTD_DecompressBlock(args, dict_, this, uncompressed_output);
#endif  // ROCKSDB_ZSTD_DDICT
    } else {
      return BuiltinDecompressorV2WithDict(dict_).DecompressBlock(
          args, uncompressed_output);
    }
  }

 protected:
  const Slice dict_;
#ifdef ROCKSDB_ZSTD_DDICT
  ZSTD_DDict* const ddict_;
#endif  // ROCKSDB_ZSTD_DDICT
};

Status BuiltinDecompressorV2OptimizeZstd::MaybeCloneForDict(
    const Slice& serialized_dict, std::unique_ptr<Decompressor>* out) {
  *out = std::make_unique<BuiltinDecompressorV2OptimizeZstdWithDict>(
      serialized_dict);
  return Status::OK();
}
class BuiltinCompressionManagerV2 final : public CompressionManager {
 public:
  BuiltinCompressionManagerV2() = default;
  ~BuiltinCompressionManagerV2() override = default;

  const char* Name() const override { return "BuiltinCompressionManagerV2"; }

  const char* CompatibilityName() const override { return "BuiltinV2"; }

  std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                            CompressionType type) override {
    if (opts.max_compressed_bytes_per_kb <= 0) {
      // No acceptable compression ratio => no compression
      return nullptr;
    }
    if (!SupportsCompressionType(type)) {
      // Unrecognized or support not compiled in. Fall back on default
      type = ColumnFamilyOptions{}.compression;
    }
    switch (type) {
      case kNoCompression:
      default:
        assert(type == kNoCompression);  // Others should be excluded above
        return nullptr;
      case kSnappyCompression:
        return std::make_unique<BuiltinSnappyCompressorV2>(opts);
      case kZlibCompression:
        return std::make_unique<BuiltinZlibCompressorV2>(opts);
      case kBZip2Compression:
        return std::make_unique<BuiltinBZip2CompressorV2>(opts);
      case kLZ4Compression:
      case kLZ4HCCompression: {
        // LZ4 and LZ4HC share the same wire format and decompressor. It is a
        // historical oddity that they are distinct CompressionTypes. To blend
        // them together more naturally, we now allow either configured
        // compression type to access the same spectrum of compression levels
        // supported by LZ4 and LZ4HC compressors. The appropriate compressor
        // for the specified level is chosen regardless of which of the two
        // types is configured, except that the default levels vary:
        //   level <= 0    -> LZ4 fast (acceleration = max(1, -level))
        //   level >= 1    -> LZ4HC (that level, 1..12)
        //   default level ->
        //     LZ4 -> LZ4 fast, acceleration 1 (equivalent to level -1)
        //     LZ4HC -> LZ4HC level 9
        bool use_hc;
        if (opts.level == CompressionOptions::kDefaultCompressionLevel) {
          use_hc = (type == kLZ4HCCompression);
        } else {
          use_hc = (opts.level >= 1);
        }
        if (use_hc) {
          return std::make_unique<BuiltinLZ4HCCompressorV2>(opts);
        } else {
          return std::make_unique<BuiltinLZ4CompressorV2NoDict>(opts);
        }
      }
      case kXpressCompression:
        return std::make_unique<BuiltinXpressCompressorV2>(opts);
      case kZSTD:
        return std::make_unique<BuiltinZSTDCompressorV2>(opts);
    }
  }

  std::shared_ptr<Decompressor> GetDecompressor() override {
    return GetGeneralDecompressor();
  }

  std::shared_ptr<Decompressor> GetDecompressorOptimizeFor(
      CompressionType optimize_for_type) override {
    if (optimize_for_type == kZSTD) {
      return GetZstdDecompressor();
    } else {
      return GetGeneralDecompressor();
    }
  }

  std::shared_ptr<Decompressor> GetDecompressorForTypes(
      const CompressionType* types_begin,
      const CompressionType* types_end) override {
    if (types_begin == types_end) {
      return nullptr;
    } else if (types_begin + 1 == types_end &&
               *types_begin == kSnappyCompression) {
      // Exclusively Snappy
      return GetSnappyDecompressor();
    } else if (std::find(types_begin, types_end, kZSTD) != types_end) {
      // Includes ZSTD
      return GetZstdDecompressor();
    } else {
      // Everything else
      return GetGeneralDecompressor();
    }
  }

  bool SupportsCompressionType(CompressionType type) const override {
    return CompressionTypeSupported(type);
  }

 protected:
  BuiltinDecompressorV2 decompressor_;
  BuiltinDecompressorV2OptimizeZstd zstd_decompressor_;
  BuiltinDecompressorV2SnappyOnly snappy_decompressor_;

 public:
  inline std::shared_ptr<Decompressor> GetGeneralDecompressor() {
    return std::shared_ptr<Decompressor>(shared_from_this(), &decompressor_);
  }

  inline std::shared_ptr<Decompressor> GetZstdDecompressor() {
    return std::shared_ptr<Decompressor>(shared_from_this(),
                                         &zstd_decompressor_);
  }

  inline std::shared_ptr<Decompressor> GetSnappyDecompressor() {
    return std::shared_ptr<Decompressor>(shared_from_this(),
                                         &snappy_decompressor_);
  }
};

const std::shared_ptr<BuiltinCompressionManagerV2>
    kBuiltinCompressionManagerV2 =
        std::make_shared<BuiltinCompressionManagerV2>();

std::shared_ptr<Decompressor>
BuiltinZSTDCompressorV2::GetOptimizedDecompressor() const {
  return kBuiltinCompressionManagerV2->GetZstdDecompressor();
}

std::shared_ptr<Decompressor>
BuiltinSnappyCompressorV2::GetOptimizedDecompressor() const {
  return kBuiltinCompressionManagerV2->GetSnappyDecompressor();
}

}  // namespace

Status CompressionManager::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<CompressionManager>* result) {
  if (value == kNullptrString || value.empty()) {
    result->reset();
    return Status::OK();
  }

  static std::once_flag loaded;
  std::call_once(loaded, [&]() {
    auto& library = *ObjectLibrary::Default();
    // TODO: try to enhance ObjectLibrary to support singletons
    library.AddFactory<CompressionManager>(
        kBuiltinCompressionManagerV2->CompatibilityName(),
        [](const std::string& /*uri*/,
           std::unique_ptr<CompressionManager>* guard,
           std::string* /*errmsg*/) {
          *guard = std::make_unique<BuiltinCompressionManagerV2>();
          return guard->get();
        });
  });

  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, result->get(),
                                              value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (id.empty()) {  // We have no Id but have options.  Not good
    return Status::NotSupported("Cannot reset object ", id);
  } else {
    status = config_options.registry->NewSharedObject(id, result);
  }
  if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
    return Status::OK();
  } else if (status.ok()) {
    status = Customizable::ConfigureNewObject(config_options, result->get(),
                                              opt_map);
  }
  return status;
}

std::string CompressionManager::CompressionTypeToString(
    CompressionType type) const {
  return ROCKSDB_NAMESPACE::CompressionTypeToString(type);
}

std::shared_ptr<CompressionManager>
CompressionManager::FindCompatibleCompressionManager(Slice compatibility_name) {
  if (compatibility_name.compare(CompatibilityName()) == 0) {
    return shared_from_this();
  } else {
    std::shared_ptr<CompressionManager> out;
    Status s =
        CreateFromString(ConfigOptions(), compatibility_name.ToString(), &out);
    if (s.ok()) {
      return out;
    } else {
      return nullptr;
    }
  }
}

const std::shared_ptr<CompressionManager>& GetBuiltinV2CompressionManager() {
  static const std::shared_ptr<CompressionManager> v2_as_base =
      kBuiltinCompressionManagerV2;
  return v2_as_base;
}

// ***********************************************************************
// END built-in implementation of customization interface
// ***********************************************************************

Status LegacyForceBuiltinCompression(
    Compressor& builtin_compressor,
    Compressor::ManagedWorkingArea* working_area, Slice from,
    GrowableBuffer* to) {
  // For legacy cases that store compressed data even when it's larger than the
  // uncompressed data (!!!), we need a reliable upper bound on the compressed
  // size. This is based on consulting various algorithms documentation etc.
  // and adding ~4 bytes for encoded uncompressed size. (Snappy is the worst
  // case for multiplicative overhead at n + n/6, bounded by 19*n/16 to avoid
  // costly division. Bzip2 is the worst case for additive overhead at 600
  // bytes.)
  size_t n = from.size();
  size_t upper_bound = ((19 * n) >> 4) + 604;
  // The upper bound has only been established considering built-in compression
  // types through kZSTD. (Might need updating if this fails.)
  assert(builtin_compressor.GetPreferredCompressionType() <= kZSTD);

  to->ResetForSize(upper_bound);
  CompressionType actual_type = kNoCompression;
  Status s = builtin_compressor.CompressBlock(
      from, to->data(), &to->MutableSize(), &actual_type, working_area);
  TEST_SYNC_POINT_CALLBACK("LegacyForceBuiltinCompression:TamperWithStatus",
                           &s);

  if (!s.ok()) {
    return s;
  }
  if (actual_type == kNoCompression) {
    // abort in debug builds
    assert(actual_type != kNoCompression);
    return Status::Corruption("Compression unexpectedly declined or aborted");
  }
  assert(actual_type == builtin_compressor.GetPreferredCompressionType());
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
