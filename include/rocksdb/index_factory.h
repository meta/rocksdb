// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  *  EXPERIMENTAL: This interface is part of the RocksDB User     *
//  *  Defined Index (UDI) framework and may change at any time     *
//  *  without notice. It is not yet considered part of the stable   *
//  *  public API.                                                   *
//  *****************************************************************

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "rocksdb/advanced_iterator.h"
#include "rocksdb/comparator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct ReadOptions;

// On-disk meta block prefix for custom indexes. Treated as part of the
// SST format: changing the string would break readability of existing
// SSTs that use the UserDefinedIndex feature.
inline constexpr const char* kIndexFactoryMetaPrefix =
    "rocksdb.user_defined_index.";

// ============================================================================
// IndexFactory: pluggable index for BlockBasedTable SST files.
//
// Lets custom index implementations (e.g., trie, learned index) coexist
// with the standard index (BinarySearch, HashSearch, or TwoLevelIndex
// depending on BlockBasedTableOptions::index_type). Per
// BlockBasedTableOptions::index_mode:
//   - kStandardOnly: only the standard index is built; any factory pointer
//     is ignored.
//   - kStandardDefault / kStandardRequired / kCustomDefault: both indexes are
//     built. Reads default to the standard index in the kStandard* modes and
//     to the custom index in kCustomDefault. ReadOptions::read_index can
//     override per-read. kStandardRequired is the strict-open variant of
//     kStandardDefault.
//   - kCustomOnly: only the custom index is built. A minimal empty stub
//     replaces the standard index block to satisfy the SST footer format.
//
// Design: mirrors FilterPolicy -- the standard index is analogous to the
// default data block format, custom IndexFactory implementations to custom
// FilterPolicy implementations.
//
// NOTE: The IndexFactory API is intentionally asymmetric between build
// and read. Both the standard and custom indexes share the factory
// abstraction for SST construction, but standard-index reads continue to
// use the internal BlockBasedTable::IndexReader path. That internal
// reader contract carries table-local behavior (cache, prefetch, pinning,
// iterator reuse) that is not part of this public SPI. Custom
// IndexFactoryReader implementations are adapted to the internal contract
// via IndexFactoryReaderWrapper.
// ============================================================================

// ---------------------------------------------------------------------------
// IndexFactoryBuilder: builds a custom index during SST construction.
//
// Called by BlockBasedTableBuilder for every key (OnKeyAdded) and every
// data block boundary (AddIndexEntry / PrepareAddEntry+FinishAddEntry).
// The builder accumulates index entries and serializes them into a meta
// block stored in the SST.
//
// Thread safety:
//   - AddIndexEntry is called only on the emit thread in synchronous mode.
//   - OnKeyAdded and PrepareAddEntry are called on the emit thread.
//   - FinishAddEntry: called on the BG writer thread (in commit order)
//     when parallel compression is active; otherwise on the emit thread.
//   - Emit-thread calls may overlap FinishAddEntry. Implementations must
//     synchronize shared state or keep callback state disjoint.
//   - EstimatedSize: may be called on the emit thread concurrently with
//     FinishAddEntry on the BG writer thread when an implementation opts
//     into parallel compression.
//   - Finish is called only after all FinishAddEntry calls complete.
//
// To opt into parallel compression an implementation overrides
// SupportsParallelAddEntry / CreatePreparedAddEntry / PrepareAddEntry /
// FinishAddEntry. The default (false from SupportsParallelAddEntry) keeps
// the synchronous AddIndexEntry path on a single thread.
// ---------------------------------------------------------------------------
class IndexFactoryBuilder {
 public:
  // Simple block handle used by the public interface.
  // Equivalent to the internal BlockHandle but without encoding/decoding.
  struct BlockHandle {
    uint64_t offset;
    uint64_t size;
  };

  // Context passed to AddIndexEntry describing the internal key tags
  // (packed sequence number + value type) for the last key in the
  // current block and the first key in the next block. These enable
  // custom indexes that need sequence-number-aware separator selection
  // (e.g., for correct Seek when the same user key spans multiple
  // blocks with different sequence numbers).
  struct IndexEntryContext {
    uint64_t last_key_tag = 0;
    uint64_t first_key_tag = 0;
    // True when block alignment padding put this data block at a
    // non-sequential offset, so its handle must not be delta encoded
    // against the previous one. Only meaningful for AddIndexEntry; the
    // parallel path learns the offset later and receives the same flag as
    // an explicit FinishAddEntry parameter.
    bool skip_delta_encoding = false;
  };

  // Value type categories for OnKeyAdded. Scoped to keep these names
  // distinct from ROCKSDB_NAMESPACE::ValueType in db/dbformat.h.
  enum class ValueType : uint8_t {
    kValue = 0,
    kDelete = 1,
    kMerge = 2,
    kOther = 3,
    // Sentinel. Use for `vt < kTypeMax` bounds checks and
    // `static_cast<size_t>(kTypeMax)` sizing only; do not serialize.
    kTypeMax,
  };
  // Compatibility names for code that used the old UserDefinedIndexBuilder
  // unscoped constants.
  static constexpr ValueType kValue = ValueType::kValue;
  static constexpr ValueType kDelete = ValueType::kDelete;
  static constexpr ValueType kMerge = ValueType::kMerge;
  static constexpr ValueType kOther = ValueType::kOther;
  static constexpr ValueType kTypeMax = ValueType::kTypeMax;

  virtual ~IndexFactoryBuilder() = default;

  // Called once for each data block boundary. The implementation should
  // record the association between the separator key and the block handle.
  //
  // @param last_key_in_current_block  User key of the last entry in the
  //                                   current data block.
  // @param first_key_in_next_block    User key of the first entry in the
  //                                   next data block. nullptr for the
  //                                   last block in the SST.
  // @param block_handle               Location and size of the data block.
  // @param separator_scratch          Scratch space for computing the
  //                                   separator. The returned Slice may
  //                                   reference this string.
  // @param context                    Packed sequence+type tags for the
  //                                   boundary keys.
  // @return                           The separator key actually stored.
  virtual Slice AddIndexEntry(const Slice& last_key_in_current_block,
                              const Slice* first_key_in_next_block,
                              const BlockHandle& block_handle,
                              std::string* separator_scratch,
                              const IndexEntryContext& context) = 0;

  // Called for every key added to the SST. This provides the custom
  // index with per-key visibility (e.g., for building a filter or
  // maintaining statistics). The default implementation is a no-op.
  //
  // @param key    User key (no internal key trailer).
  // @param type   Value type category.
  // @param value  The user value associated with this key.
  virtual void OnKeyAdded(const Slice& /*key*/, ValueType /*type*/,
                          const Slice& /*value*/) {}

  // Serialize the complete index into one self-contained byte buffer. Custom
  // readers receive only this block, so all reader-required state must be
  // included. The memory backing the returned Slice must remain valid until
  // this builder is destroyed.
  // An empty buffer with OK status omits usable custom index contents. Modes
  // that build the standard index preserve a zero-size custom meta block and
  // fall back to the standard index. kCustomOnly rejects an empty buffer.
  virtual Status Finish(Slice* index_contents) = 0;

  // Returns the estimated size in bytes of the index built so far.
  // Used by BlockBasedTableBuilder for SST file size estimation.
  // Implementations that opt into parallel compression (see
  // SupportsParallelAddEntry) must keep this safe under concurrent
  // FinishAddEntry calls on the BG writer thread.
  //
  // The value must be an upper bound on what Finish() will produce: the table
  // builder adds it to the estimated SST tail size, and compaction asserts
  // that the final tail does not exceed the last estimate, so implementations
  // must not under-report.
  virtual uint64_t EstimatedSize() const = 0;

  // =========================================================================
  // Optional protocol. A default implementation is provided.
  // Custom index implementations only need this to participate in parallel
  // compression.
  // =========================================================================

  // --- Parallel compression protocol ---
  //
  // Splits AddIndexEntry into two phases so the table builder can run
  // data block compression in parallel:
  //   Phase 1 (emit thread): PrepareAddEntry -- record separator keys.
  //   Phase 2 (BG writer thread): FinishAddEntry -- record the block handle.
  //
  // An implementation that returns true from SupportsParallelAddEntry() must
  // return non-null prepared entries from CreatePreparedAddEntry(), fill them
  // in PrepareAddEntry(), and commit the matching index entry in
  // FinishAddEntry(). The default returns false so implementations stay on the
  // synchronous AddIndexEntry path.
  //
  // Any prepared entry can be abandoned without a matching FinishAddEntry(),
  // so implementations must not rely on that pairing to release resources. The
  // table builder discards one entry per builder up front to check this
  // contract, and drops the rest if the SST build fails partway through.
  //
  // The table builder turns parallel compression on for an SST only if
  // every configured builder (built-in plus any custom factory) returns
  // true. If any builder returns false the SST falls back to single-
  // threaded compression -- there is no per-builder mixing within an SST.

  struct PreparedAddEntry {
    virtual ~PreparedAddEntry() = default;
  };

  virtual bool SupportsParallelAddEntry() const { return false; }

  virtual std::unique_ptr<PreparedAddEntry> CreatePreparedAddEntry() {
    return nullptr;
  }

  // Phase 1: called on the emit thread. Records the separator keys.
  // The block handle is not yet known. The key Slices reference table
  // builder buffers that are overwritten before the matching
  // FinishAddEntry() runs, so anything needed later must be copied into
  // the prepared entry.
  virtual void PrepareAddEntry(const Slice& /*last_key_in_current_block*/,
                               const Slice* /*first_key_in_next_block*/,
                               const IndexEntryContext& /*context*/,
                               PreparedAddEntry* /*out*/) {}

  // Phase 2: called on the write thread. Records the block handle.
  // skip_delta_encoding is true when block alignment padding causes
  // non-sequential offsets.
  virtual void FinishAddEntry(const BlockHandle& /*block_handle*/,
                              PreparedAddEntry* /*entry*/,
                              std::string* /*separator_scratch*/,
                              bool /*skip_delta_encoding*/) {}
};

// ---------------------------------------------------------------------------
// IndexFactoryIterator: iterates over index entries in a custom index.
//
// Returned by IndexFactoryReader::NewIterator. Each position in the
// iterator corresponds to a data block in the SST file.
//
// The iterator returns user keys (not internal keys) as separator keys,
// and simple BlockHandle values (offset + size). The BlockBasedTable
// reader adapts these to the internal InternalIteratorBase<IndexValue>
// interface automatically.
// ---------------------------------------------------------------------------
class IndexFactoryIterator {
 public:
  virtual ~IndexFactoryIterator() = default;

  // Hint for upcoming scan ranges. Implementations may use this for
  // prefetching or bounding.
  // @param scan_opts  Array of scan range descriptors.
  // @param num_opts   Number of elements in scan_opts.
  virtual void Prepare(const ScanOptions scan_opts[], size_t num_opts) = 0;

  // Context for Seek, carrying the packed sequence+type tag of the
  // target key. Used by indexes that need sequence-number-aware block
  // selection (e.g., when the same user key spans multiple blocks).
  struct SeekContext {
    uint64_t target_tag = 0;
  };

  // Position at the first entry >= target and populate result.
  virtual Status SeekAndGetResult(const Slice& target, IterateResult* result,
                                  const SeekContext& context) = 0;

  // Advance to the next entry and populate result.
  virtual Status NextAndGetResult(IterateResult* result) = 0;

  // Position at the first entry.
  // Default: seeks with an empty key (works for bytewise comparator).
  virtual Status SeekToFirstAndGetResult(IterateResult* result) {
    return SeekAndGetResult(Slice(), result, SeekContext{});
  }

  // Position at the last entry. Optional -- reverse iteration support.
  // Default: returns NotSupported.
  virtual Status SeekToLastAndGetResult(IterateResult* result) {
    (void)result;
    return Status::NotSupported("SeekToLast not supported by this index");
  }

  // Move to the previous entry. Optional -- reverse iteration support.
  // Default: returns NotSupported.
  virtual Status PrevAndGetResult(IterateResult* result) {
    (void)result;
    return Status::NotSupported("Prev not supported by this index");
  }

  // Returns the block handle for the current position.
  virtual IndexFactoryBuilder::BlockHandle value() = 0;
};

// ---------------------------------------------------------------------------
// IndexFactoryReader: reads a custom index from a serialized SST block.
// ---------------------------------------------------------------------------
class IndexFactoryReader {
 public:
  virtual ~IndexFactoryReader() = default;

  // Create an iterator over the index.
  virtual std::unique_ptr<IndexFactoryIterator> NewIterator(
      const ReadOptions& read_options) = 0;

  // Approximate heap memory used by this reader, excluding the serialized
  // index block. RocksDB charges that block separately, to the block cache or
  // the owning table reader.
  virtual size_t ApproximateMemoryUsage() const = 0;

  // A reader whose ApproximateMemoryUsage() includes the serialized index
  // block's size can return true here. RocksDB then subtracts the block size
  // from that estimate instead of charging the block twice.
  virtual bool MemoryUsageIncludesIndexBlock() const { return false; }
};

// ---------------------------------------------------------------------------
// IndexFactoryOptions: configuration passed to NewBuilder / NewReader.
// ---------------------------------------------------------------------------
struct IndexFactoryOptions {
  // The user comparator for this column family. Defaults to
  // BytewiseComparator() so a default-constructed IndexFactoryOptions
  // matches the behavior of the previous UserDefinedIndexOption.
  const Comparator* comparator = BytewiseComparator();

  // A custom index is rejected when this comparator carries user-defined
  // timestamps. Equal user keys sort newest-timestamp-first, so an index that
  // orders keys bytewise disagrees with the order separators were handed to it
  // in. And with persist_user_defined_timestamps=false, data blocks are written
  // without timestamps while separators still carry them, which no
  // implementation can reconcile because that flag is not exposed here.
};

// ---------------------------------------------------------------------------
// IndexFactory: the top-level factory that creates builders and readers.
//
// Extends Customizable for string-based construction (CreateFromString),
// options serialization, and Name()-based identification.
// ---------------------------------------------------------------------------
class IndexFactory : public Customizable {
 public:
  ~IndexFactory() override = default;

  static const char* Type() { return "UserDefinedIndexFactory"; }

  // Create an IndexFactory from a string identifier (e.g., "trie").
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<IndexFactory>* factory);

  // Create a builder for constructing the index during SST creation.
  virtual Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const {
    (void)options;
    builder.reset(NewBuilder());
    return Status::OK();
  }

  // Create a reader for an existing serialized index block.
  // @param options         Configuration (comparator, etc.)
  // @param index_contents  Raw bytes of the serialized index. This Slice and
  //                        the bytes it references remain valid for the
  //                        lifetime of the reader. The factory may advance
  //                        the Slice; that does not affect other readers of
  //                        the same block.
  virtual Status NewReader(const IndexFactoryOptions& options,
                           Slice& index_contents,
                           std::unique_ptr<IndexFactoryReader>& reader) const {
    (void)options;
    reader = NewReader(index_contents);
    return Status::OK();
  }

  // Compatibility API for code that still subclasses UserDefinedIndexFactory.
  // New subclasses should override the option-taking methods above.
  virtual IndexFactoryBuilder* NewBuilder() const { return nullptr; }

  virtual std::unique_ptr<IndexFactoryReader> NewReader(
      Slice& /*index_block*/) const {
    return nullptr;
  }
};

}  // namespace ROCKSDB_NAMESPACE
