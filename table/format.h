//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "file/file_prefetch_buffer.h"
#include "file/random_access_file_reader.h"
#include "memory/memory_allocator_impl.h"
#include "options/cf_options.h"
#include "port/likely.h"
#include "port/malloc.h"
#include "port/port.h"  // noexcept
#include "rocksdb/cleanable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/cast_util.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

class RandomAccessFile;
struct ReadOptions;

using ReadScopedBlockBufferProviderRef =
    std::optional<std::reference_wrapper<ReadScopedBlockBufferProvider>>;

bool ShouldReportDetailedTime(Env* env, Statistics* stats);

// the length of the magic number in bytes.
constexpr uint32_t kMagicNumberLengthByte = 8;

extern const uint64_t kBlockBasedTableMagicNumber;

extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;

extern const uint64_t kCuckooTableMagicNumber;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
 public:
  // Creates a block handle with special values indicating "uninitialized,"
  // distinct from the "null" block handle.
  BlockHandle();
  BlockHandle(uint64_t offset, uint64_t size);

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t _offset) { offset_ = _offset; }

  // The size of the stored block, this size does not include the block trailer.
  uint64_t size() const { return size_; }
  void set_size(uint64_t _size) { size_ = _size; }

  void EncodeTo(std::string* dst) const;
  char* EncodeTo(char* dst) const;
  Status DecodeFrom(Slice* input);
  Status DecodeSizeFrom(uint64_t offset, Slice* input);

  // Return a string that contains the copy of handle.
  std::string ToString(bool hex = true) const;

  // if the block handle's offset and size are both "0", we will view it
  // as a null block handle that points to no where.
  bool IsNull() const { return offset_ == 0 && size_ == 0; }

  static const BlockHandle& NullBlockHandle() { return kNullBlockHandle; }

  // Maximum encoding length of a BlockHandle
  static constexpr uint32_t kMaxEncodedLength = 2 * kMaxVarint64Length;

  inline bool operator==(const BlockHandle& rhs) const {
    return offset_ == rhs.offset_ && size_ == rhs.size_;
  }
  inline bool operator!=(const BlockHandle& rhs) const {
    return !(*this == rhs);
  }

 private:
  uint64_t offset_;
  uint64_t size_;

  static const BlockHandle kNullBlockHandle;
};

struct EncodedBlockHandle {
  explicit EncodedBlockHandle(const BlockHandle& h) {
    auto end = h.EncodeTo(buffer.data());
    size = end - buffer.data();
  }
  Slice AsSlice() const { return Slice(buffer.data(), size); }
  std::array<char, BlockHandle::kMaxEncodedLength> buffer;
  size_t size;
};

// Value in block-based table file index.
//
// The index entry for block n is: y -> h, [x],
// where: y is some key between the last key of block n (inclusive) and the
// first key of block n+1 (exclusive); h is BlockHandle pointing to block n;
// x, if present, is the first key of block n (unshortened).
// This struct represents the "h, [x]" part.
struct IndexValue {
  BlockHandle handle;
  // Empty means unknown.
  Slice first_internal_key;

  IndexValue() = default;
  IndexValue(BlockHandle _handle, Slice _first_internal_key)
      : handle(_handle), first_internal_key(_first_internal_key) {}

  // have_first_key indicates whether the `first_internal_key` is used.
  // If previous_handle is not null, delta encoding is used;
  // in this case, the two handles normally point to consecutive blocks:
  // handle.offset() ==
  //     previous_handle->offset() + previous_handle->size() + kBlockTrailerSize
  //
  // use_value_delta_escape enables the format_version >= 8 value-delta codec:
  // genuine size deltas are remapped (zigzag(delta)+1, always >= 1) so that the
  // varint 0 is free to act as an escape meaning "a full BlockHandle follows."
  // The escape is emitted (only) for a non-contiguous handle, so such an entry
  // does not have to be forced to a restart. When use_value_delta_escape is
  // false (format_version <= 7), the encoding is byte-identical to the legacy
  // PutVarsignedint64(size delta). Only meaningful when previous_handle !=
  // null.
  void EncodeTo(std::string* dst, bool have_first_key,
                const BlockHandle* previous_handle,
                bool use_value_delta_escape = false) const;
  Status DecodeFrom(Slice* input, bool have_first_key,
                    const BlockHandle* previous_handle,
                    bool use_value_delta_escape = false);

  std::string ToString(bool hex, bool have_first_key) const;
};

// Given a file's base_context_checksum and an offset of a block within that
// file, choose a 32-bit value that is as unique as possible. This value will
// be added to the standard checksum to get a checksum "with context," or can
// be subtracted to "remove" context. Returns zero (no modifier) if feature is
// disabled with base_context_checksum == 0.
inline uint32_t ChecksumModifierForContext(uint32_t base_context_checksum,
                                           uint64_t offset) {
  // To disable on base_context_checksum == 0, we could write
  // `if (base_context_checksum == 0) return 0;` but benchmarking shows
  // measurable performance penalty vs. this: compute the modifier
  // unconditionally and use an "all or nothing" bit mask to enable
  // or disable.
  uint32_t all_or_nothing = uint32_t{0} - (base_context_checksum != 0);

  // Desired properties:
  // (call this function f(b, o) where b = base and o = offset)
  // 1. Fast
  // 2. f(b1, o) == f(b2, o) iff b1 == b2
  //    (Perfectly preserve base entropy)
  // 3. f(b, o1) == f(b, o2) only if o1 == o2 or |o1-o2| >= 4 billion
  //    (Guaranteed uniqueness for nearby offsets)
  // 3. f(b, o + j * 2**32) == f(b, o + k * 2**32) only if j == k
  //    (Upper bits matter, and *aligned* misplacement fails check)
  // 4. f(b1, o) == f(b2, o + x) then preferably not
  //    f(b1, o + y) == f(b2, o + x + y)
  //    (Avoid linearly correlated matches)
  // 5. f(b, o) == 0 depends on both b and o
  //    (No predictable overlap with non-context checksums)
  uint32_t modifier =
      base_context_checksum ^ (Lower32of64(offset) + Upper32of64(offset));

  return modifier & all_or_nothing;
}

// Sentinel for "no format_version known yet," e.g. a Footer that has not been
// populated by DecodeFrom. Deliberately larger than any real version so that
// accidentally treating it as one is caught by the FormatVersionUses*
// predicates below rather than silently reading as "newest of everything."
constexpr uint32_t kInvalidFormatVersion = 0xffffffffU;

// Minimum format version supported for reading SST files in block-based format.
//
// When phasing out old format versions, first increase the write minimum,
// then later (>= 6 mo) increase the read minimum when removing the
// implementation for both read and write.
constexpr uint32_t kMinSupportedBbtFormatVersionForRead = 2;

// Minimum format version supported for writing new SST files in block-based
// format. This should be >= kMinSupportedFormatVersionForRead.
//
// When phasing out old format versions, first increase the write minimum,
// then later (>= 6 mo) increase the read minimum when removing the
// implementation for both read and write.
constexpr uint32_t kMinSupportedBbtFormatVersionForWrite = 2;
static_assert(kMinSupportedBbtFormatVersionForWrite >=
              kMinSupportedBbtFormatVersionForRead);

inline bool IsSupportedFormatVersionForRead(uint64_t magic, uint32_t version) {
  if (magic == kBlockBasedTableMagicNumber) {
    return version >= kMinSupportedBbtFormatVersionForRead &&
           version <= BlockBasedTableOptions::kLatestSupportedFormatVersion;
  } else if (magic == kPlainTableMagicNumber) {
    return version == 0;
  } else if (magic == kCuckooTableMagicNumber) {
    return version == 1;
  } else {
    return false;
  }
}

inline bool IsSupportedFormatVersionForWrite(uint64_t magic, uint32_t version) {
  if (magic == kBlockBasedTableMagicNumber) {
    return version >= kMinSupportedBbtFormatVersionForWrite &&
           version <= BlockBasedTableOptions::kLatestSupportedFormatVersion;
  } else if (magic == kPlainTableMagicNumber) {
    return version == 0;
  } else if (magic == kCuckooTableMagicNumber) {
    return version == 1;
  } else {
    return false;
  }
}

// The FormatVersionUses* predicates below answer "does this (known) format
// version use feature X". Every one of them is a `version >= N` test, so
// kInvalidFormatVersion would satisfy all of them and quietly report "uses the
// newest of everything." Callers must therefore pass a real version -- from
// BlockBasedTableOptions, or from a Footer that has actually been decoded.
inline void AssertKnownFormatVersion(uint32_t version) {
  assert(version != kInvalidFormatVersion);
  (void)version;
}

// Same as having a unique id in footer.
inline bool FormatVersionUsesContextChecksum(uint32_t version) {
  AssertKnownFormatVersion(version);
  return version >= 6;
}

inline bool FormatVersionUsesIndexHandleInFooter(uint32_t version) {
  AssertKnownFormatVersion(version);
  return version < 6;
}

inline bool FormatVersionUsesCompressionManagerName(uint32_t version) {
  AssertKnownFormatVersion(version);
  return version >= 7;
}

// format_version >= 8 decouples the index block's value-delta encoding from the
// key "shared" sentinel by reserving an in-value escape codepoint that means "a
// full BlockHandle follows" (see IndexValue::EncodeTo/DecodeFrom). This lets a
// non-contiguous index entry (whose block handle does NOT start where the
// previous one ended, e.g. because of super_block_alignment padding or
// interleaved embedded blob records) remain a normal key-delta-encoded,
// on-cadence index entry instead of being forced to a restart (shared==0).
inline bool FormatVersionUsesValueDeltaEscape(uint32_t version) {
  AssertKnownFormatVersion(version);
  return version >= 8;
}

// format_version >= 8 repurposes the top 16 bits of the footer's 4-byte
// "metaindex block size" field to record the number of bytes of gap between the
// end of the metaindex block (including its trailer) and the start of the
// footer (space reserved for future file checksum data). The low 16 bits hold
// the metaindex block size. For format_version < 8 the whole field is the
// metaindex block size and the metaindex is assumed adjacent to the footer.
// See Footer format documentation in format.cc. (Note: fv < 6 allowed an
// arbitrary gap due to inefficient metaindex handle encoding, but that's
// obsolete.)
inline bool FormatVersionUsesMetaindexGap(uint32_t version) {
  AssertKnownFormatVersion(version);
  return version >= 8;
}

// format_version >= 8 allows blocks with sorted keys (data and index blocks) to
// store the common user-key prefix of all keys in the block once (in the
// leading bytes, before restarts[0]) instead of re-storing it at every restart
// point. Only used for (reverse-)bytewise comparators without user-defined
// timestamps; signaled per-block by a nonzero stored prefix length
// (restarts[0] > 0), not by a footer bit. Older readers reject format_version 8
// at footer decode (fail-safe).
inline bool FormatVersionUsesCommonKeyPrefix(uint32_t version) {
  AssertKnownFormatVersion(version);
  return version >= 8;
}

// Incompatible features are stored in checked footer bytes for format_version
// >= 6. Older readers reject any non-zero feature bits.
inline constexpr uint64_t kFooterFeatureRequireUserDefinedIndex = uint64_t{1}
                                                                  << 0;
inline constexpr uint64_t kKnownFooterFeatures =
    kFooterFeatureRequireUserDefinedIndex;

// Footer encapsulates the fixed information stored at the tail end of every
// SST file. In general, it should only include things that cannot go
// elsewhere under the metaindex block. For example, checksum_type is
// required for verifying metaindex block checksum (when applicable), but
// index block handle can easily go in metaindex block. See also FooterBuilder
// below.
class Footer {
 public:
  // Create empty. Populate using DecodeFrom.
  Footer() {}

  void Reset() {
    table_magic_number_ = kNullTableMagicNumber;
    format_version_ = kInvalidFormatVersion;
    base_context_checksum_ = 0;
    incompatible_features_ = 0;
    metaindex_handle_ = BlockHandle::NullBlockHandle();
    index_handle_ = BlockHandle::NullBlockHandle();
    checksum_type_ = kInvalidChecksumType;
    block_trailer_size_ = 0;
  }

  // Deserialize a footer (populate fields) from `input` and check for various
  // corruptions. `input_offset` is the offset within the target file of
  // `input` buffer, which is needed for verifying format_version >= 6 footer.
  // If enforce_table_magic_number != 0, will return corruption if table magic
  // number is not equal to enforce_table_magic_number.
  Status DecodeFrom(Slice input, uint64_t input_offset,
                    uint64_t enforce_table_magic_number = 0);

  // Table magic number identifies file as RocksDB SST file and which kind of
  // SST format is use.
  uint64_t table_magic_number() const { return table_magic_number_; }

  // A version (footer and more) within a kind of SST. (It would add more
  // unnecessary complexity to separate footer versions and
  // BBTO::format_version.)
  uint32_t format_version() const { return format_version_; }

  // Test-only: set just the format_version, for unit tests that construct a
  // BlockBasedTable::Rep without a real file to decode a footer from.
  // Production code must populate the whole footer via DecodeFrom.
  void TEST_SetFormatVersion(uint32_t format_version) {
    format_version_ = format_version;
  }

  // See ChecksumModifierForContext()
  uint32_t base_context_checksum() const { return base_context_checksum_; }

  uint64_t incompatible_features() const { return incompatible_features_; }

  bool HasIncompatibleFeature(uint64_t feature) const {
    return (incompatible_features_ & feature) != 0;
  }

  // Block handle for metaindex block.
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }

  // Block handle for (top-level) index block.
  // TODO? remove from this struct and only read on decode for legacy cases
  const BlockHandle& index_handle() const { return index_handle_; }

  // Checksum type used in the file, including footer for format version >= 6.
  ChecksumType checksum_type() const {
    return static_cast<ChecksumType>(checksum_type_);
  }

  // Block trailer size used by file with this footer (e.g. 5 for block-based
  // table and 0 for plain table). This is inferred from magic number so
  // not in the serialized form.
  inline size_t GetBlockTrailerSize() const { return block_trailer_size_; }

  // Convert this object to a human readable form
  std::string ToString() const;

  // Encoded lengths of Footers. Bytes for serialized Footer will always be
  // >= kMinEncodedLength and <= kMaxEncodedLength.
  //
  // Footer version 0 (legacy) will always occupy exactly this many bytes.
  // It consists of two block handles, padding, and a magic number.
  static constexpr uint32_t kVersion0EncodedLength =
      2 * BlockHandle::kMaxEncodedLength + kMagicNumberLengthByte;
  static constexpr uint32_t kMinEncodedLength = kVersion0EncodedLength;

  // Footer of versions 1 and higher will always occupy exactly this many
  // bytes. It originally consisted of the checksum type, two block handles,
  // padding (to maximum handle encoding size), a format version number, and a
  // magic number.
  static constexpr uint32_t kNewVersionsEncodedLength =
      1 + 2 * BlockHandle::kMaxEncodedLength + 4 + kMagicNumberLengthByte;
  static constexpr uint32_t kMaxEncodedLength = kNewVersionsEncodedLength;

  static constexpr uint64_t kNullTableMagicNumber = 0;

 private:
  static constexpr int kInvalidChecksumType =
      (1 << (sizeof(ChecksumType) * 8)) | kNoChecksum;

  uint64_t table_magic_number_ = kNullTableMagicNumber;
  uint32_t format_version_ = kInvalidFormatVersion;
  uint32_t base_context_checksum_ = 0;
  uint64_t incompatible_features_ = 0;
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
  int checksum_type_ = kInvalidChecksumType;
  uint8_t block_trailer_size_ = 0;
};

// Builder for Footer
class FooterBuilder {
 public:
  // Run builder in inputs. This is a single step with lots of parameters for
  // efficiency (based on perf testing).
  // * table_magic_number identifies file as RocksDB SST file and which kind of
  // SST format is use.
  // * format_version is a version for the footer and can also apply to other
  // aspects of the SST file (see BlockBasedTableOptions::format_version).
  // NOTE: To save complexity in the caller, when format_version == 0 and
  // there is a corresponding legacy magic number to the one specified, the
  // legacy magic number will be written for forward compatibility.
  // * footer_offset is the file offset where the footer will be written
  // (for future use).
  // * checksum_type is for formats using block checksums.
  // * index_handle is optional for some SST kinds and (for caller convenience)
  // ignored when format_version >= 6. (Must be added to metaindex in that
  // case.)
  // * unique_id must be specified if format_vesion >= 6 and SST uses block
  // checksums with context. Otherwise, auto-generated if format_vesion >= 6.
  Status Build(uint64_t table_magic_number, uint32_t format_version,
               uint64_t footer_offset, ChecksumType checksum_type,
               const BlockHandle& metaindex_handle,
               const BlockHandle& index_handle = BlockHandle::NullBlockHandle(),
               uint32_t base_context_checksum = 0) {
    return Build(table_magic_number, format_version, footer_offset,
                 checksum_type, metaindex_handle, index_handle,
                 base_context_checksum, /*incompatible_features=*/0);
  }

  Status Build(uint64_t table_magic_number, uint32_t format_version,
               uint64_t footer_offset, ChecksumType checksum_type,
               const BlockHandle& metaindex_handle,
               const BlockHandle& index_handle, uint32_t base_context_checksum,
               uint64_t incompatible_features);

  // After Builder, get a Slice for the serialized Footer, backed by this
  // FooterBuilder.
  const Slice& GetSlice() const {
    assert(slice_.size());
    return slice_;
  }

 private:
  Slice slice_;
  std::array<char, Footer::kMaxEncodedLength> data_;
};

// Set to true to allow unit testing of writing unsupported block-based table
// format versions (to test read side)
bool& TEST_AllowUnsupportedFormatVersion();

// Read the footer from file
// If enforce_table_magic_number != 0, ReadFooterFromFile() will return
// corruption if table_magic number is not equal to enforce_table_magic_number
Status ReadFooterFromFile(const IOOptions& opts, RandomAccessFileReader* file,
                          FileSystem& fs, FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number = 0,
                          Statistics* stats = nullptr);

// Computes a checksum using the given ChecksumType. Sometimes we need to
// include one more input byte logically at the end but not part of the main
// data buffer. If data_size >= 1, then
// ComputeBuiltinChecksum(type, data, size)
// ==
// ComputeBuiltinChecksumWithLastByte(type, data, size - 1, data[size - 1])
uint32_t ComputeBuiltinChecksum(ChecksumType type, const char* data,
                                size_t size);
uint32_t ComputeBuiltinChecksumWithLastByte(ChecksumType type, const char* data,
                                            size_t size, char last_byte);

// Represents the contents of a block read from an SST file. Depending on how
// it's created, it may or may not own the actual block bytes. As an example,
// BlockContents objects representing data read from mmapped files only point
// into the mmapped region. Depending on context, it might be a serialized
// (potentially compressed) block, including a trailer beyond `size`, or an
// uncompressed block.
//
// Please try to use this terminology when dealing with blocks:
// * "Serialized block" - bytes that go into storage. For block-based table
// (usually the case) this includes the block trailer. Here the `size` does
// not include the trailer, but other places in code might include the trailer
// in the size.
// * "Maybe compressed block" - like a serialized block, but without the
// trailer (or no promise of including a trailer). Must be accompanied by a
// CompressionType in some other variable or field.
// * "Uncompressed block" - "payload" bytes that are either stored with no
// compression, used as input to compression function, or result of
// decompression function.
// * "Parsed block" - an in-memory form of a block in block cache, as it is
// used by the table reader. Different C++ types are used depending on the
// block type (see block_cache.h). Only trivially parsable block types
// use BlockContents as the parsed form.
//
struct BlockContents {
  // Points to block payload (without trailer)
  Slice data;
  CacheAllocationPtr allocation;
  // Optional shared lifetime token for block bytes not owned through
  // `allocation`, such as bytes backed by a read-scoped provider.
  SharedCleanablePtr cleanup;
  // Usable byte size of the provider/external backing allocation.
  size_t backing_size = 0;

#ifndef NDEBUG
  // Whether there is a known trailer after what is pointed to by `data`.
  // See BlockBasedTable::GetCompressionType.
  bool has_trailer = false;
#endif  // NDEBUG

  void AssertSingleOwner() const {
    assert(allocation.get() == nullptr || cleanup.get() == nullptr);
  }

  BlockContents() {}

  // Does not take ownership of the underlying data bytes.
  BlockContents(const Slice& _data) : data(_data) {}

  // Takes ownership of the underlying data bytes.
  BlockContents(CacheAllocationPtr&& _data, size_t _size)
      : data(_data.get(), _size), allocation(std::move(_data)) {}

  // Takes ownership of the underlying data bytes.
  BlockContents(std::unique_ptr<char[]>&& _data, size_t _size)
      : data(_data.get(), _size) {
    allocation.reset(_data.release());
  }

  // Returns whether the object has ownership of the underlying data bytes.
  bool own_bytes() const {
    AssertSingleOwner();
    return allocation.get() != nullptr || cleanup.get() != nullptr;
  }

  // The additional memory space taken by the block data.
  size_t usable_size() const {
    AssertSingleOwner();
    // FIXME: doesn't account for possible block trailer
    if (allocation.get() != nullptr) {
      auto allocator = allocation.get_deleter().allocator;
      if (allocator) {
        return allocator->UsableSize(allocation.get(), data.size());
      }
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      return malloc_usable_size(allocation.get());
#else
      return data.size();
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    } else if (cleanup.get() != nullptr) {
      return backing_size;
    } else {
      return 0;  // no extra memory is occupied by the data
    }
  }

  size_t ApproximateMemoryUsage() const {
    AssertSingleOwner();
    return usable_size() + sizeof(*this);
  }

  BlockContents(BlockContents&& other) noexcept { *this = std::move(other); }

  BlockContents& operator=(BlockContents&& other) {
    data = std::move(other.data);
    allocation = std::move(other.allocation);
    cleanup = std::move(other.cleanup);
    backing_size = other.backing_size;
#ifndef NDEBUG
    has_trailer = other.has_trailer;
#endif  // NDEBUG
    AssertSingleOwner();
    return *this;
  }

  ~BlockContents() = default;
};

// The `data` points to serialized block contents read in from file, which
// must be compressed and include a trailer beyond `size`. A new buffer is
// allocated with the given allocator (or default) and the uncompressed
// contents are returned in `out_contents`. Statistics updated.
Status DecompressSerializedBlock(
    const char* data, size_t size, CompressionType type,
    Decompressor& decompressor, BlockContents* out_contents,
    const ImmutableOptions& ioptions, MemoryAllocator* allocator = nullptr,
    ReadScopedBlockBufferProviderRef block_buffer_provider = std::nullopt);

Status DecompressSerializedBlock(
    Decompressor::Args& args, Decompressor& decompressor,
    BlockContents* out_contents, const ImmutableOptions& ioptions,
    MemoryAllocator* allocator = nullptr,
    ReadScopedBlockBufferProviderRef block_buffer_provider = std::nullopt);

// This is a variant of DecompressSerializedBlock that does not expect a
// block trailer beyond `size`. (CompressionType is passed in.)
Status DecompressBlockData(
    const char* data, size_t size, CompressionType type,
    Decompressor& decompressor, BlockContents* out_contents,
    const ImmutableOptions& ioptions, MemoryAllocator* allocator = nullptr,
    Decompressor::ManagedWorkingArea* working_area = nullptr);

Status DecompressBlockData(Decompressor::Args& args, Decompressor& decompressor,
                           BlockContents* out_contents,
                           const ImmutableOptions& ioptions,
                           MemoryAllocator* allocator = nullptr);

// Pointer overloads for read paths where the decompressor may be absent: it is
// null for a file whose blocks are all uncompressed, and during table open for
// the metaindex/properties blocks that are read before the file's decompressor
// is constructed. Those blocks are always written with kNoCompression, so a
// compressed block reaching one of these overloads with no decompressor means
// the file's per-block compression byte is corrupt or tampered; report it as
// corruption instead of dereferencing null.
inline Status DecompressSerializedBlock(
    const char* data, size_t size, CompressionType type,
    UnownedPtr<Decompressor> decompressor, BlockContents* out_contents,
    const ImmutableOptions& ioptions, MemoryAllocator* allocator = nullptr,
    ReadScopedBlockBufferProviderRef block_buffer_provider = std::nullopt) {
  if (UNLIKELY(!decompressor)) {
    return Status::Corruption(
        "Block claims compression but no decompressor is available");
  }
  return DecompressSerializedBlock(data, size, type, *decompressor,
                                   out_contents, ioptions, allocator,
                                   block_buffer_provider);
}

inline Status DecompressSerializedBlock(
    Decompressor::Args& args, UnownedPtr<Decompressor> decompressor,
    BlockContents* out_contents, const ImmutableOptions& ioptions,
    MemoryAllocator* allocator = nullptr,
    ReadScopedBlockBufferProviderRef block_buffer_provider = std::nullopt) {
  if (UNLIKELY(!decompressor)) {
    return Status::Corruption(
        "Block claims compression but no decompressor is available");
  }
  return DecompressSerializedBlock(args, *decompressor, out_contents, ioptions,
                                   allocator, block_buffer_provider);
}

inline Status DecompressBlockData(
    const char* data, size_t size, CompressionType type,
    UnownedPtr<Decompressor> decompressor, BlockContents* out_contents,
    const ImmutableOptions& ioptions, MemoryAllocator* allocator = nullptr,
    Decompressor::ManagedWorkingArea* working_area = nullptr) {
  if (UNLIKELY(!decompressor)) {
    return Status::Corruption(
        "Block claims compression but no decompressor is available");
  }
  return DecompressBlockData(data, size, type, *decompressor, out_contents,
                             ioptions, allocator, working_area);
}

// Replace db_host_id contents with the real hostname if necessary
Status ReifyDbHostIdProperty(Env* env, std::string* db_host_id);

// Implementation details follow.  Clients should ignore,

// TODO(andrewkr): we should prefer one way of representing a null/uninitialized
// BlockHandle. Currently we use zeros for null and use negation-of-zeros for
// uninitialized.
inline BlockHandle::BlockHandle() : BlockHandle(~uint64_t{0}, ~uint64_t{0}) {}

inline BlockHandle::BlockHandle(uint64_t _offset, uint64_t _size)
    : offset_(_offset), size_(_size) {}

}  // namespace ROCKSDB_NAMESPACE
