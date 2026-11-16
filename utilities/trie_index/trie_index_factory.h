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
//  In kCustomDefault and kCustomOnly mode, reads use the trie unless they set
//  ReadOptions::read_index to kBuiltin.
//
//  A block's separator is a user key at or after every user key in the block
//  and at or before the first user key of the next block. A block's tag is
//  the packed sequence number and type of its last key, or 0 when the block
//  can be found by user key alone (see BlockTag() in trie_index_factory.cc).
//
//  In every index_mode but kStandardOnly, the table builder creates a
//  TrieIndexBuilder for each SST and passes it each data block's boundary
//  keys and handle. TrieIndexBuilder adds the block's separator, handle and
//  tag to a TrieBuilder (format in trie.h), whose output becomes the meta
//  block "rocksdb.user_defined_index.trie_index". TrieIndexReader views that
//  block in place through a Trie, and each TrieIndexIterator wraps a
//  TrieIterator and checks the bounds given in ScanOptions. A Seek finds the
//  first block whose (separator, tag) is at or after the target's (user key,
//  tag), by separator ascending and then tag descending.
//
//  The trie orders separators byte by byte, so it requires
//  BytewiseComparator.

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
//   2. At each data block boundary, AddIndexEntry(), or with parallel
//      compression PrepareAddEntry() and later FinishAddEntry().
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
  // trie with the block's handle, and returns it. For the last block,
  // first_key_in_next_block is null and the separator is
  // last_key_in_current_block itself.
  //
  // A block gets the real tag of its last key when that user key continues
  // into the next block, when its separator equals the previous block's, or
  // when it is the last block; other blocks get the 0 tag.
  //
  // Runs PrepareAddEntry() and then FinishAddEntry(), and returns the
  // separator in *separator_scratch.
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

  // Returns an estimate of the current serialized index size. With parallel
  // compression it runs on the emit thread while FinishAddEntry() runs on the
  // BG writer thread. Separator statistics are written only on the emit
  // thread and block statistics are relaxed atomics, so no lock is needed.
  uint64_t EstimatedSize() const override;

  // --- Parallel compression protocol ---
  //
  // AddIndexEntry() is split in two: PrepareAddEntry() computes the
  // separator on the emit thread, and FinishAddEntry() adds the block, whose
  // handle is known only once the block is written, on the BG writer thread.
  // The table builder calls FinishAddEntry() serially and in block order, so
  // trie_builder_ needs no lock.
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
  // Whether separator equals the separator of the block added last.
  bool SharesPreviousSeparator(const Slice& separator) const;
  // Adds a separator to estimate_ and returns its shared prefix length with
  // previous_separator. may_have_tag is whether the entry may get a nonzero
  // tag.
  size_t UpdateSizeEstimate(const Slice& separator,
                            const Slice& previous_separator, bool may_have_tag);
  // Records the next block's handle in the block statistics.
  void TrackHandle(const TrieBlockHandle& handle);

  // A data block's separator and what decides its tag, from PrepareAddEntry()
  // until FinishAddEntry() receives the block's handle.
  struct PreparedTrieEntry : public IndexFactoryBuilder::PreparedAddEntry {
    std::string separator_key;
    // Real tag of last_key_in_current_block. BlockTag() uses it for the last
    // block, and when this block's last user key continues into the next
    // block or its separator equals the previous block's.
    uint64_t last_key_tag = 0;
    // True if first_key_in_next_block was non-null at Prepare time.
    bool has_next_block = false;
    // True if last_key_in_current_block and first_key_in_next_block have the
    // same user key, computed from the inputs alone. FinishAddEntry() also
    // tags the block when its separator equals the previous block's, which
    // only the thread that adds blocks can check.
    bool same_user_key = false;
    // Set by PrepareAddEntry and cleared by FinishAddEntry. Finishing an
    // entry while this is false fails the build.
    bool valid = false;
  };

  // Members are grouped by the thread that writes them. With parallel
  // compression, PrepareAddEntry() and EstimatedSize() run on the emit thread
  // while FinishAddEntry() runs on the BG writer thread; otherwise everything
  // runs on the emit thread. Finish() runs after the last FinishAddEntry().

  const Comparator* comparator_;

  // Emit thread. Kept apart from trie_builder_ so that EstimatedSize() does
  // not read what the BG writer thread is changing.
  //
  // Separator statistics of the entries prepared so far. Its block statistics
  // are unused; the atomics below hold them.
  TrieBuilder::SizeStats estimate_;
  // Separator of the entry prepared last.
  std::string previous_prepared_separator_;
  // The entry AddIndexEntry() prepares and finishes.
  PreparedTrieEntry serial_entry_;

  // The thread that adds blocks, in FinishAddEntry() (the BG writer thread in
  // parallel builds).
  //
  // Blocks added so far.
  TrieBuilder trie_builder_;
  // The previous block's handle and the gap after it, for TrackHandle().
  TrieBlockHandle previous_handle_;
  uint64_t block_gap_ = 0;
  // Statistics of the block handles added so far, which EstimatedSize() reads
  // on the emit thread. They bound the widths of the offset and size columns.
  // Blocks still being compressed in a parallel build are charged like the
  // largest block so far, so the estimate can fall short until their handles
  // arrive. The table builder takes its last estimate in Finish(), after the
  // last FinishAddEntry(), so that one is an upper bound, as index_factory.h
  // requires.
  RelaxedAtomic<uint64_t> max_block_step_{0};
  RelaxedAtomic<uint64_t> max_block_size_{0};
  // Whether TrieBuilder::Finish() must store block sizes (see
  // SizeStats::irregular_gaps in trie.h).
  RelaxedAtomic<bool> irregular_block_gaps_{false};
  // Set when FinishAddEntry() receives an entry with nothing prepared in it,
  // so Finish() fails instead of writing an index that omits that data block.
  bool has_unprepared_entry_ = false;

  // Finish().
  bool finished_ = false;
  // Serialized index.
  std::string data_;
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
  // current separator cannot be compared against the limit directly.
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
