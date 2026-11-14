//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  Trie-based User Defined Index (UDI) for RocksDB's block-based tables.
//
//  This provides a TrieIndexFactory that implements the IndexFactory
//  interface, building a trie index (see trie.h) from the separator keys
//  generated during SST file construction.
//
//  Usage:
//    auto trie_factory = std::make_shared<TrieIndexFactory>();
//    BlockBasedTableOptions table_options;
//    table_options.user_defined_index_factory = trie_factory;
//    table_options.index_mode =
//    BlockBasedTableOptions::IndexMode::kStandardDefault;
//
//  In kStandardDefault mode, reads use the standard index by default.
//  Set ReadOptions::read_index to kPreferCustom to use the trie:
//    ReadOptions ro;
//    ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
//    auto iter = db->NewIterator(ro);
//
//  In kCustomDefault/kCustomOnly mode, all reads use the trie automatically.

#pragma once

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/types.h"
#include "util/atomic.h"
#include "utilities/trie_index/trie.h"

namespace ROCKSDB_NAMESPACE {
class ObjectLibrary;

namespace trie_index {

// Registers the built-in trie UDI in the supplied object library.
int RegisterBuiltinTrieIndexFactory(ObjectLibrary& library,
                                    const std::string& arg);

// ============================================================================
// TrieIndexBuilder: Implements IndexFactoryBuilder using TrieBuilder.
//
// During SST file construction, RocksDB calls:
//   1. OnKeyAdded() for each key-value pair.
//   2. AddIndexEntry() at each data block boundary.
//   3. Finish() to serialize the index.
//
// The separator of each data block goes straight into a TrieBuilder, which
// serializes the index in Finish().
// ============================================================================
class TrieIndexBuilder final : public IndexFactoryBuilder {
 public:
  explicit TrieIndexBuilder(const Comparator* comparator);
  ~TrieIndexBuilder() override = default;

  // Called at each data block boundary. Computes a short separator between
  // last_key_in_current_block and first_key_in_next_block, adds it to the
  // trie with the block's handle, and returns it.
  //
  // A block gets the real tag of its last key when that user key continues
  // into the next block, when its separator equals the previous block's, or
  // when it is the last block; other blocks get the 0 tag.
  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch,
                      const IndexEntryContext& context) override;

  // Called for each key added to the SST. Currently a no-op -- the trie is
  // built entirely from separator keys provided via AddIndexEntry().
  void OnKeyAdded(const Slice& key, ValueType type,
                  const Slice& value) override;

  // Finalize the trie and return the serialized index data.
  Status Finish(Slice* index_contents) override;

  // Returns an estimate of the current serialized index size. Separator
  // statistics are updated only by emit-thread callbacks and block statistics
  // are relaxed atomics, so concurrent FinishAddEntry calls do not require
  // synchronization.
  uint64_t EstimatedSize() const override;

  // --- Parallel compression protocol ---
  //
  // The trie builder participates in the parallel pipeline by splitting
  // AddIndexEntry into Prepare (emit thread, computes the separator) and
  // Finish (write thread, attaches the now-known block handle and adds the
  // block to trie_builder_). Because FinishAddEntry runs serially on the BG
  // worker thread (the table builder commits blocks in order), trie_builder_
  // needs no locking.
  bool SupportsParallelAddEntry() const override { return true; }
  std::unique_ptr<PreparedAddEntry> CreatePreparedAddEntry() override;
  void PrepareAddEntry(const Slice& last_key_in_current_block,
                       const Slice* first_key_in_next_block,
                       const IndexEntryContext& context,
                       PreparedAddEntry* out) override;
  void FinishAddEntry(const BlockHandle& block_handle, PreparedAddEntry* entry,
                      std::string* separator_scratch,
                      bool skip_delta_encoding) override;

 private:
  const Comparator* comparator_;
  // Serialized index.
  std::string data_;
  bool finished_;
  // Set when FinishAddEntry receives an entry with nothing prepared in it, so
  // Finish() fails instead of writing an index that omits that data block.
  bool has_unprepared_entry_ = false;
  // Whether the gaps between blocks differ, so that sizes are stored. One of
  // the block statistics described below; kept here to avoid padding.
  RelaxedAtomic<bool> irregular_block_gaps_{false};

  // Blocks added so far. Owned by the thread that adds blocks (the BG writer
  // during parallel builds).
  TrieBuilder trie_builder_;

  // EstimatedSize() is called on the emit thread. Keep its state disjoint from
  // trie_builder_, which the BG writer mutates during parallel builds.
  // Separator statistics of the entries prepared so far. Its block statistics
  // are unused; the atomics below hold them.
  TrieBuilder::SizeStats estimate_;
  std::string previous_prepared_separator_;
  // Adds a separator to estimate_ and returns its shared prefix length with
  // the previous one, for reusing emit-owned separator bytes. may_have_tag is
  // whether the entry may get a nonzero tag.
  size_t UpdateSizeEstimate(const Slice& separator,
                            const Slice& previous_separator, bool may_have_tag);

  // Statistics of the block handles added so far, with irregular_block_gaps_
  // above. Written by the thread that adds handles (the BG writer during
  // parallel builds) and read by EstimatedSize() on the emit thread. They
  // bound the widths of the offset and size columns. Blocks still being
  // compressed in a parallel build are charged like the largest block so far,
  // so the estimate can fall short until their handles arrive.
  RelaxedAtomic<uint64_t> max_block_step_{0};
  RelaxedAtomic<uint64_t> max_block_size_{0};
  // Owned by the thread that adds handles.
  TrieBlockHandle previous_handle_;
  uint64_t block_gap_ = 0;
  // Records the next block's handle in the statistics above.
  void TrackHandle(const TrieBlockHandle& handle);
  // Whether separator equals the separator of the block added last.
  bool SharesPreviousSeparator(const Slice& separator) const;

  // Staged data for the parallel AddIndexEntry protocol. Populated by
  // PrepareAddEntry on the emit thread, consumed by FinishAddEntry on
  // the BG worker thread. The block handle is unknown when Prepare
  // runs and is filled in by Finish.
  struct PreparedTrieEntry : public IndexFactoryBuilder::PreparedAddEntry {
    std::string separator_key;
    // Real tag of last_key_in_current_block, for last blocks and for blocks
    // sharing a separator with the previous one.
    uint64_t last_key_tag = 0;
    // True if first_key_in_next_block was non-null at Prepare time.
    bool has_next_block = false;
    // True if last_key and first_key_in_next_block share the same user
    // key (computed from inputs alone). Finish may upgrade this to true
    // if FindShortestSeparator failed to shorten and the result matches
    // the previous block's separator, which only the thread adding blocks
    // can check.
    bool same_user_key_initial = false;
    // Set by PrepareAddEntry and cleared by FinishAddEntry. Finishing an
    // entry while this is false fails the build.
    bool valid = false;
  };
};

// ============================================================================
// TrieIndexIterator: Implements IndexFactoryIterator using TrieIterator.
//
// Wraps TrieIterator and adapts it to the UDI iterator interface, handling
// bounds checking against ScanOptions.
// ============================================================================
class TrieIndexIterator final : public IndexFactoryIterator {
 public:
  TrieIndexIterator(const Trie* trie, const Comparator* comparator);

  // Prepare for a batch of scans. Stores scan bounds for later use.
  void Prepare(const ScanOptions scan_opts[], size_t num_opts) override;
  Status SeekAndGetResult(const Slice& target, IterateResult* result,
                          const SeekContext& context) override;
  Status NextAndGetResult(IterateResult* result) override;
  Status SeekToFirstAndGetResult(IterateResult* result) override;
  Status SeekToLastAndGetResult(IterateResult* result) override;
  Status PrevAndGetResult(IterateResult* result) override;
  IndexFactoryBuilder::BlockHandle value() override;

 private:
  // Whether CheckBounds() can return anything but kInbound.
  bool HasBounds() const { return prepared_ && !scan_opts_.empty(); }

  // Checks whether the current block may hold keys in the current scan.
  // reference_key is the key to compare against the limit: for Seek this is
  // the seek target, for Next this is the previous separator key. Separators
  // are upper bounds on block contents, not first-in-block keys, so the
  // current separator cannot be compared against the limit directly -- see
  // the UDI API contract in user_defined_index.h.
  IterBoundCheck CheckBounds(const Slice& reference_key) const;

  // Fills *result from the iterator position after a call that returned s.
  // bounds is the bound check result to report when positioned.
  Status SetResult(const Status& s, IterBoundCheck bounds,
                   IterateResult* result) const;

  const Comparator* comparator_;
  TrieIterator iter_;
  std::vector<ScanOptions> scan_opts_;
  size_t current_scan_idx_ = 0;
  bool prepared_ = false;
};

// ============================================================================
// TrieIndexReader: Implements IndexFactoryReader.
// ============================================================================
class TrieIndexReader : public IndexFactoryReader {
 public:
  explicit TrieIndexReader(const Comparator* comparator);

  // Initialize from serialized index data. The data must remain valid for
  // the lifetime of this reader (it's typically a block cache entry).
  Status InitFromSlice(const Slice& data);

  std::unique_ptr<IndexFactoryIterator> NewIterator(
      const ReadOptions& read_options) override;

  // The index is used in place, so only this object is charged.
  size_t ApproximateMemoryUsage() const override;

 private:
  const Comparator* comparator_;
  Trie trie_;
};

// ============================================================================
// TrieIndexFactory: Implements IndexFactory.
//
// Factory for creating TrieIndexBuilder (during SST file writes) and
// TrieIndexReader (during SST file reads). Registered as a Customizable with
// name "trie_index".
// ============================================================================
class TrieIndexFactory : public IndexFactory {
 public:
  TrieIndexFactory() = default;
  ~TrieIndexFactory() override = default;

  static const char* kClassName() { return "trie_index"; }
  const char* Name() const override { return kClassName(); }
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;

  Status NewBuilder(
      const IndexFactoryOptions& option,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  Status NewReader(const IndexFactoryOptions& option, Slice& index_block,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

  // Rejects a comparator other than BytewiseComparator.
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
