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
//  interface, building a trie index from the separator keys generated during
//  SST file construction.
//
//  Two serialized formats exist. Readers accept both; the factory's
//  format_version option picks the one new SST files are written with:
//  - 1 (default): a LOUDS-encoded Fast Succinct Trie. Data-block offsets and
//    sizes are 32-bit integers. Only supported on little-endian hosts.
//  - 2: a byte trie over buckets of front-coded separators, with 64-bit block
//    handles (see trie_v2.h). Smaller and faster than format 1, especially
//    for keys with long shared prefixes. RocksDB versions that predate it
//    fail to open SST files written with it while this factory is
//    configured.
//
//  Usage:
//    // TrieIndexFactory(2) writes format 2.
//    auto trie_factory = std::make_shared<TrieIndexFactory>();
//    BlockBasedTableOptions table_options;
//    table_options.user_defined_index_factory = trie_factory;
//    table_options.index_mode =
//        BlockBasedTableOptions::IndexMode::kStandardDefault;
//  or in an options string:
//    user_defined_index_factory={id=trie_index;format_version=2}
//
//  In kStandardDefault mode, reads use the standard index by default.
//  Set ReadOptions::read_index to kPreferCustom to use the trie:
//    ReadOptions ro;
//    ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
//    auto iter = db->NewIterator(ro);
//
//  In kCustomDefault/kCustomOnly mode, all reads use the trie automatically.
//
//  Format 1 stores data-block offsets and sizes as 32-bit integers. If
//  either exceeds UINT32_MAX, SST construction fails with NotSupported in
//  every mode that builds the trie, including kStandardDefault. File-size
//  targets do not guarantee this limit: L0 outputs and SstFileWriter
//  outputs are not split at the target size. Bound those outputs explicitly,
//  use format 2, or use kStandardOnly to stop building the trie.

#pragma once

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/types.h"
#include "util/atomic.h"
#include "utilities/trie_index/louds_trie.h"
#include "utilities/trie_index/trie_v2.h"

namespace ROCKSDB_NAMESPACE {
class ObjectLibrary;

namespace trie_index {

// Registers the built-in trie UDI in the supplied object library.
int RegisterBuiltinTrieIndexFactory(ObjectLibrary& library,
                                    const std::string& arg);

// Scan bounds from IndexFactoryIterator::Prepare(), shared by the iterators
// of both formats.
class TrieScanBounds {
 public:
  void Prepare(const ScanOptions scan_opts[], size_t num_opts);

  // Moves past the scans whose limit is at or before a Seek target.
  void SkipScansBefore(const Slice& target, const Comparator* comparator);

  // Whether Check() can return anything but kInbound.
  bool Active() const { return prepared_ && !scan_opts_.empty(); }

  // Checks whether the current block may hold keys in the current scan.
  // reference_key is the key to compare against the limit: for Seek this is
  // the seek target, for Next this is the previous separator key. Separators
  // are upper bounds on block contents, not first-in-block keys, so the
  // current separator cannot be compared against the limit directly -- see
  // the UDI API contract in user_defined_index.h.
  IterBoundCheck Check(const Slice& reference_key,
                       const Comparator* comparator) const;

 private:
  std::vector<ScanOptions> scan_opts_;
  size_t current_scan_idx_ = 0;
  bool prepared_ = false;
};

// ============================================================================
// TrieIndexBuilder: Implements IndexFactoryBuilder.
//
// During SST file construction, RocksDB calls:
//   1. OnKeyAdded() for each key-value pair.
//   2. AddIndexEntry() at each data block boundary.
//   3. Finish() to serialize the index.
//
// The trie builder collects the separator keys from AddIndexEntry() and
// builds the trie in the requested format during Finish().
// ============================================================================
class TrieIndexBuilder final : public IndexFactoryBuilder {
 public:
  // format_version is the serialized format to write, 1 or 2.
  TrieIndexBuilder(const Comparator* comparator, uint32_t format_version);
  ~TrieIndexBuilder() override = default;

  // Called at each data block boundary. We compute a short separator
  // between last_key_in_current_block and first_key_in_next_block, buffer
  // it for deferred trie construction in Finish(), and return it.
  //
  // The sequence numbers from context are used to detect same-user-key
  // block boundaries, which decide whether an entry carries a real tag or
  // the 0 sentinel. Every non-empty trie emits the seqno side-table.
  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch,
                      const IndexEntryContext& context) override;

  // Called for each key added to the SST. Currently a no-op -- the trie is
  // built entirely from separator keys provided via AddIndexEntry().
  void OnKeyAdded(const Slice& key, ValueType type,
                  const Slice& value) override;

  // Finalize the trie and return the serialized index data. In format 1,
  // returns NotSupported if any data-block offset or size exceeds UINT32_MAX;
  // that format encodes these fields as 32-bit integers.
  Status Finish(Slice* index_contents) override;

  // Returns an estimate of the current serialized index size. The estimate is
  // updated only by emit-thread callbacks, so concurrent FinishAddEntry calls
  // do not require synchronization.
  uint64_t EstimatedSize() const override;

  // --- Parallel compression protocol ---
  //
  // The trie builder participates in the parallel pipeline by splitting
  // AddIndexEntry into Prepare (emit thread, computes the separator) and
  // Finish (write thread, attaches the now-known block handle and appends
  // to buffered_entries_). Because FinishAddEntry runs serially on the BG
  // worker thread (the table builder commits blocks in order), the entry
  // buffer mutation needs no locking.
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
  // Builds the format 2 index from buffered_entries_.
  Status FinishV2(Slice* index_contents);

  const Comparator* comparator_;
  const uint32_t format_version_;
  LoudsTrieBuilder trie_builder_;
  // Serialized format 2 index.
  std::string v2_data_;
  bool finished_;
  bool has_unsupported_handle_ = false;
  // Set when FinishAddEntry receives an entry with nothing prepared in it, so
  // Finish() fails instead of writing an index that omits that data block.
  bool has_unprepared_entry_ = false;

  // Buffered separator entries: (separator_key, tag, handle).
  // The separator_key is the user-key-only separator computed by
  // FindShortestSeparator. The tag field stores:
  //   - For same-user-key boundaries: the real tag of last_key
  //   - For the last block: the real tag of last_key
  //   - For intermediate non-boundary entries: 0 (sentinel)
  struct BufferedEntry {
    std::string separator_key;
    uint64_t tag{};
    TrieBlockHandle handle;
  };
  std::vector<BufferedEntry> buffered_entries_;

  // EstimatedSize() is called on the emit thread. Keep its state disjoint from
  // buffered_entries_, which the BG writer mutates during parallel builds.
  // Adjacent separators charge their common prefix only once.
  uint64_t estimated_trie_edges_ = 0;
  uint64_t estimated_num_entries_ = 0;
  std::string previous_prepared_separator_;
  // Format 2 only: bytes of every separator front coded against the previous
  // one, the longest separator, separators equal to the previous one, and
  // entries that may carry a nonzero tag.
  uint64_t estimated_v2_bucket_bytes_ = 0;
  uint64_t estimated_v2_max_key_len_ = 0;
  uint64_t estimated_v2_duplicates_ = 0;
  uint64_t estimated_v2_tagged_ = 0;
  // Returns the shared prefix length for reusing emit-owned separator bytes.
  // may_have_tag is whether the entry may get a nonzero tag.
  size_t UpdateSizeEstimate(const Slice& separator,
                            const Slice& previous_separator, bool may_have_tag);

  // Format 2 only: statistics of the block handles added so far. Written by
  // the thread that adds handles (the BG writer during parallel builds) and
  // read by EstimatedSize() on the emit thread. They bound the widths of the
  // offset and size columns.
  RelaxedAtomic<uint64_t> max_block_step_{0};
  RelaxedAtomic<uint64_t> max_block_size_{0};
  RelaxedAtomic<bool> irregular_block_gaps_{false};
  // Owned by the thread that adds handles.
  TrieBlockHandle previous_handle_;
  uint64_t block_gap_ = 0;
  // Records the next block's handle in the statistics above.
  void TrackHandle(const TrieBlockHandle& handle);
  uint64_t EstimatedV2Size() const;

  // Staged data for the parallel AddIndexEntry protocol. Populated by
  // PrepareAddEntry on the emit thread, consumed by FinishAddEntry on
  // the BG worker thread. The block handle is unknown when Prepare
  // runs and is filled in by Finish.
  struct PreparedTrieEntry : public IndexFactoryBuilder::PreparedAddEntry {
    std::string separator_key;
    // Real tag of last_key_in_current_block. Used as entry.tag for last
    // blocks and for same-user-key runs.
    uint64_t last_key_tag = 0;
    // True if first_key_in_next_block was non-null at Prepare time.
    bool has_next_block = false;
    // True if last_key and first_key_in_next_block share the same user
    // key (computed from inputs alone). Finish may upgrade this to true
    // if FindShortestSeparator failed to shorten and the result matches
    // the previous buffered entry's separator (a check that depends on
    // buffer state and so must run during Finish).
    bool same_user_key_initial = false;
    // Set by PrepareAddEntry and cleared by FinishAddEntry. Finishing an
    // entry while this is false fails the build.
    bool valid = false;
  };
};

// ============================================================================
// TrieIndexIterator: Implements IndexFactoryIterator using
// LoudsTrieIterator.
//
// Wraps LoudsTrieIterator and adapts it to the UDI iterator interface,
// handling bounds checking against ScanOptions.
// ============================================================================
class TrieIndexIterator final : public IndexFactoryIterator {
 public:
  // @param has_seqno_encoding: true if the trie was built with a seqno
  //   side-table (enabling post-seek correction for same-user-key boundaries).
  //   This flag is read from the trie's serialized header.
  TrieIndexIterator(const LoudsTrie* trie, const Comparator* comparator,
                    bool has_seqno_encoding);
  ~TrieIndexIterator() override = default;

  // Prepare for a batch of scans. Stores scan bounds for later use.
  void Prepare(const ScanOptions scan_opts[], size_t num_opts) override;

  // Position at the very first index entry. Descends directly to the
  // leftmost leaf without a full seek traversal.
  Status SeekToFirstAndGetResult(IterateResult* result) override;

  // Position at the very last index entry. Descends directly to the
  // rightmost leaf without a full seek traversal.
  Status SeekToLastAndGetResult(IterateResult* result) override;

  // Move to the previous index entry. When in an overflow run, decrements
  // within the run before moving to the previous trie leaf.
  Status PrevAndGetResult(IterateResult* result) override;

  // Seek to the first index entry >= target. When has_seqno_encoding_ is
  // true, the trie is searched with user_key only, then post-seek correction
  // uses target_tag from context to advance through overflow blocks.
  Status SeekAndGetResult(const Slice& target, IterateResult* result,
                          const SeekContext& context) override;

  // Advance to the next index entry. When in an overflow run, advances
  // within the run before moving to the next trie leaf.
  Status NextAndGetResult(IterateResult* result) override;

  // Return the BlockHandle of the current block. When positioned on an
  // overflow block, returns the overflow block's handle instead of the
  // trie leaf's handle.
  IndexFactoryBuilder::BlockHandle value() override;

 private:
  IterBoundCheck CheckBounds(const Slice& reference_key) const {
    return bounds_.Check(reference_key, comparator_);
  }

  const Comparator* comparator_;
  LoudsTrieIterator iter_;
  // Pointer to the trie for side-table access during overflow iteration.
  const LoudsTrie* trie_;
  // Scratch space for the current separator key (reconstructed from trie).
  std::string current_key_scratch_;
  // Previous separator key, used as reference for Next() bounds checking.
  // Always valid when NextAndGetResult is called, since Next requires a
  // preceding Seek or Next which sets current_key_scratch_ (copied here).
  std::string prev_key_scratch_;

  TrieScanBounds bounds_;

  // True if the trie was built with a seqno side-table. When true:
  //   - SeekAndGetResult seeks with user key only, then does post-seek
  //     correction using seqno comparison
  //   - value() may return overflow block handles
  //   - NextAndGetResult advances within overflow runs before trie leaves
  bool has_seqno_encoding_;

  // Reset overflow state to the default single-block position.
  void ResetOverflowState() {
    overflow_run_index_ = 0;
    overflow_run_size_ = 1;
    overflow_base_idx_ = 0;
  }

  // Set up overflow state for the current trie leaf. When position_at_last
  // is true (reverse iteration), positions at the last block in the run.
  void SetupOverflowForCurrentLeaf(bool position_at_last) {
    if (has_seqno_encoding_ && iter_.Valid()) {
      uint64_t leaf_idx = iter_.LeafIndex();
      uint32_t block_count = trie_->GetLeafBlockCount(leaf_idx);
      overflow_run_size_ = block_count;
      overflow_base_idx_ = trie_->GetOverflowBase(leaf_idx);
      if (position_at_last) {
        overflow_run_index_ = block_count - 1;
      }
    }
  }

  // Copy the current trie key into current_key_scratch_ and set the result.
  void CopyTrieKeyToResult(IterateResult* result) {
    Slice trie_key = iter_.Key();
    current_key_scratch_.assign(trie_key.data(), trie_key.size());
    result->key = Slice(current_key_scratch_);
  }

  // ---- Overflow run state ----
  //
  // When the trie iterator lands on a leaf that has block_count > 1 (a
  // same-user-key run), these track the current position within the run:
  //
  // overflow_run_index_: 0 = primary block (trie leaf), 1+ = overflow block.
  //   Used by value() to select the right handle.
  // overflow_run_size_: total blocks for current leaf (from leaf_block_counts).
  //   1 = no overflow (the common case).
  // overflow_base_idx_: starting index into overflow arrays for this leaf
  //   (from overflow_base_ prefix sum).
  uint32_t overflow_run_index_;
  uint32_t overflow_run_size_;
  uint32_t overflow_base_idx_;
};

// ============================================================================
// TrieV2IndexIterator: Implements IndexFactoryIterator over a format 2 index.
// ============================================================================
class TrieV2IndexIterator final : public IndexFactoryIterator {
 public:
  TrieV2IndexIterator(const TrieV2Index* index, const Comparator* comparator);

  void Prepare(const ScanOptions scan_opts[], size_t num_opts) override;
  Status SeekAndGetResult(const Slice& target, IterateResult* result,
                          const SeekContext& context) override;
  Status NextAndGetResult(IterateResult* result) override;
  Status SeekToFirstAndGetResult(IterateResult* result) override;
  Status SeekToLastAndGetResult(IterateResult* result) override;
  Status PrevAndGetResult(IterateResult* result) override;
  IndexFactoryBuilder::BlockHandle value() override;

 private:
  // Fills *result from the iterator position after a call that returned s.
  // bounds is the bound check result to report when positioned.
  Status SetResult(const Status& s, IterBoundCheck bounds,
                   IterateResult* result) const;

  const Comparator* comparator_;
  TrieV2Iterator iter_;
  TrieScanBounds bounds_;
  // The separator before a Next, used as the reference key for its bound
  // check.
  std::string prev_key_;
};

// ============================================================================
// TrieV2IndexReader: Implements IndexFactoryReader over a format 2 index.
// ============================================================================
class TrieV2IndexReader : public IndexFactoryReader {
 public:
  explicit TrieV2IndexReader(const Comparator* comparator);

  // The data must remain valid for the lifetime of this reader.
  Status InitFromSlice(const Slice& data);

  std::unique_ptr<IndexFactoryIterator> NewIterator(
      const ReadOptions& read_options) override;

  // The index is used in place, so only this object is charged.
  size_t ApproximateMemoryUsage() const override;

 private:
  const Comparator* comparator_;
  TrieV2Index index_;
};

// ============================================================================
// TrieIndexReader: Implements IndexFactoryReader over a format 1 index.
//
// Owns (or references) the deserialized LoudsTrie and creates iterators
// for read operations.
// ============================================================================
class TrieIndexReader : public IndexFactoryReader {
 public:
  explicit TrieIndexReader(const Comparator* comparator);
  ~TrieIndexReader() override = default;

  // Initialize from serialized index data. The data must remain valid for
  // the lifetime of this reader (it's typically a block cache entry).
  Status InitFromSlice(const Slice& data);

  // Create a new iterator for scanning.
  std::unique_ptr<IndexFactoryIterator> NewIterator(
      const ReadOptions& read_options) override;

  // Approximate memory usage of the deserialized trie.
  size_t ApproximateMemoryUsage() const override;

 private:
  const Comparator* comparator_;
  LoudsTrie trie_;
};

// ============================================================================
// TrieIndexFactory: Implements IndexFactory.
//
// Factory for creating TrieIndexBuilder (during SST file writes) and the
// reader matching each index's format (during SST file reads). Registered as
// a Customizable with name "trie_index".
// ============================================================================
class TrieIndexFactory : public IndexFactory {
 public:
  // Writes format version 1.
  TrieIndexFactory();
  // Writes the given format version, 1 or 2. Other values fail
  // ValidateOptions() and NewBuilder() with InvalidArgument. On big-endian
  // hosts, format 1 fails them with NotSupported.
  explicit TrieIndexFactory(uint32_t format_version);
  ~TrieIndexFactory() override = default;

  static const char* kClassName() { return "trie_index"; }
  const char* Name() const override { return kClassName(); }
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;

  Status NewBuilder(
      const IndexFactoryOptions& option,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override;

  // Reads either format version, whatever this factory writes.
  Status NewReader(const IndexFactoryOptions& option, Slice& index_block,
                   std::unique_ptr<IndexFactoryReader>& reader) const override;

  // Rejects an unknown format version and a comparator other than
  // BytewiseComparator.
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

 private:
  Status ValidateFormatVersion() const;

  uint32_t format_version_;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
