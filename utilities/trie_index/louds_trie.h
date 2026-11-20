//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  Fast Succinct Trie (FST) implementation based on the LOUDS (Level-Order
//  Unary Degree Sequence) encoding, inspired by the SuRF paper (Zhang et al.,
//  SIGMOD 2018). The trie uses a hybrid encoding:
//
//  - LOUDS-Dense: For the upper levels of the trie (levels close to the root)
//    where the fanout tends to be high. Uses 256-bit bitmaps per node (one bit
//    per possible byte label), achieving excellent cache locality and O(1)
//    child lookup via popcount.
//
//  - LOUDS-Sparse: For the lower levels of the trie where fanout is typically
//    low. Uses compact label arrays and bitvectors, achieving better space
//    efficiency than the dense encoding for sparse regions.
//
//  The boundary between dense and sparse levels (the "cutoff level") is chosen
//  to minimize total space: dense levels use 256 bits per node regardless of
//  fanout, while sparse levels use ~10 bits per edge. When the average fanout
//  drops below ~25 children per node, sparse becomes more efficient.
//
//  Key design decisions:
//  - Immutable: Built once from sorted keys during SST file construction.
//  - Flat-array layout: The entire trie is stored as a sequence of bitvectors,
//    making serialization trivial and enabling zero-copy reads from disk.
//  - Leaf-indexed: Each trie leaf maps to a data block handle via packed
//    uint32_t offset/size arrays, indexed by the leaf's BFS ordinal.
//  - Key reconstruction: The separator key is reconstructed by tracing
//    the path from root to the current leaf, collecting byte labels at
//    each level. Dense levels encode the label in the bit position
//    (pos % 256), sparse levels store it in the label array.
//
//  Leaf ordinal computation (SuRF formulas):
//  - Dense leaf: rank1(d_labels, pos+1) - rank1(d_has_child, rank1(d_labels,
//    pos+1)) + rank1(d_is_prefix_key, node_num+1) - 1
//    where pos = node_num * 256 + label_byte
//  - Sparse leaf: ((label_pos + 1) - rank1(s_has_child, label_pos + 1)) +
//    rank1(s_is_prefix_key, node_num+1) + dense_leaf_count - 1

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/autovector.h"
#include "utilities/trie_index/bitvector.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Forward declarations
// ============================================================================
class LoudsTrie;

// ============================================================================
// BlockHandle: offset and size of a data block in the SST file.
// Matches IndexFactoryBuilder::BlockHandle but defined locally to avoid
// header dependencies in the core trie implementation.
// ============================================================================
struct TrieBlockHandle {
  uint64_t offset = 0;
  uint64_t size = 0;
};

// ============================================================================
// LoudsTrieBuilder: Constructs a LOUDS-encoded trie from sorted keys.
//
// Usage:
//   LoudsTrieBuilder builder;
//   for each data block in sorted order:
//     builder.AddKey(separator_key, block_handle);
//   builder.Finish();
//   Slice serialized = builder.GetSerializedData();
//
// The builder collects all separator keys, then in Finish() it:
//  1. Determines the optimal cutoff level between dense and sparse encoding.
//  2. Constructs LOUDS-Dense bitvectors for levels [0, cutoff).
//  3. Constructs LOUDS-Sparse bitvectors for levels [cutoff, max_depth).
//  4. Serializes everything into a flat buffer.
//
// All keys must be added in sorted order (according to the comparator used
// by the SST file).
// ============================================================================
class LoudsTrieBuilder {
 public:
  LoudsTrieBuilder();

  // Add a separator key and its associated data block handle.
  // Keys must be added in sorted (ascending) order.
  // This is the basic form used when seqno encoding is not active.
  void AddKey(const Slice& key, const TrieBlockHandle& handle);

  // Add a separator key with seqno side-table metadata. Used when
  // has_seqno_encoding_ is true. The seqno is stored in a side-table
  // alongside the trie (NOT encoded into the key). block_count is the
  // number of consecutive data blocks that share this separator key
  // (1 = no overflow, >1 = same-user-key run).
  //
  // IMPORTANT: When block_count > 1, the overflow blocks must be added
  // via AddOverflowBlock() immediately after this call, before calling
  // AddKey() for the next separator.
  void AddKeyWithSeqno(const Slice& key, const TrieBlockHandle& handle,
                       uint64_t seqno, uint32_t block_count);

  // Add an overflow block for the most recently added key (same separator).
  // Called block_count-1 times after AddKeyWithSeqno() with block_count > 1.
  // Each overflow block has its own handle and seqno.
  void AddOverflowBlock(const TrieBlockHandle& handle, uint64_t seqno);

  // Set the seqno encoding flag. Must be called before Finish().
  // When true, the serialized trie will include a seqno side-table after
  // the handle arrays, enabling post-seek correction for same-user-key
  // block boundaries.
  void SetHasSeqnoEncoding(bool has_seqno_encoding) {
    has_seqno_encoding_ = has_seqno_encoding;
  }

  // Finalize the trie construction. After this call, GetSerializedData()
  // returns the serialized trie.
  void Finish();

  // Get the serialized trie data. Valid only after Finish().
  Slice GetSerializedData() const { return Slice(serialized_data_); }

 private:
  // Determine the optimal cutoff level between dense and sparse encoding.
  // Returns the first level where sparse encoding is more space-efficient.
  uint32_t ComputeCutoffLevel() const;

  // Serialize all trie data structures into serialized_data_.
  void SerializeAll();

  // ---- Input data ----
  std::vector<std::string> keys_;
  std::vector<TrieBlockHandle> handles_;

  // ---- Seqno side-table data (populated by AddKeyWithSeqno/AddOverflowBlock)
  // ----
  //
  // Per-key (one entry per call to AddKey/AddKeyWithSeqno):
  //   seqnos_[i]: seqno for the i-th separator (0 = sentinel for non-boundary)
  //   block_counts_[i]: how many consecutive blocks share this separator
  //                     (1 = normal, >1 = same-user-key run with overflows)
  //
  // Per-overflow-block (one entry per call to AddOverflowBlock):
  //   overflow_handles_[j]: handle for the j-th overflow block
  //   overflow_seqnos_[j]: seqno for the j-th overflow block
  std::vector<uint64_t> seqnos_;
  std::vector<uint32_t> block_counts_;
  std::vector<TrieBlockHandle> overflow_handles_;
  std::vector<uint64_t> overflow_seqnos_;

  // ---- Trie structure (built during Finish()) ----

  // Cutoff level: levels [0, cutoff_level_) use dense, rest use sparse.
  uint32_t cutoff_level_;

  // Max key depth (length of longest key).
  uint32_t max_depth_;

  // LOUDS-Dense bitvectors (all levels concatenated into single bitvectors):
  //   d_labels_: 256-bit bitmaps concatenated for all nodes across all dense
  //     levels. Bit (node_num * 256 + label) is set if node has child with
  //     that label.
  //   d_has_child_: One bit per set bit in d_labels_. Set if the child
  //     is an internal node (has further children), clear if it's a leaf.
  //   d_is_prefix_key_: One bit per node across all dense levels. Set if
  //     the path from root to this node forms a valid key (prefix match).
  BitvectorBuilder d_labels_;
  BitvectorBuilder d_has_child_;
  BitvectorBuilder d_is_prefix_key_;

  // LOUDS-Sparse arrays (all sparse levels concatenated):
  //   s_labels_: Byte labels of all edges, in level-order.
  //   s_has_child_: One bit per label. Set if the child is internal.
  //   s_louds_: One bit per label. Set at the first label of each node
  //     (marks node boundaries in the label array).
  //   s_is_prefix_key_: One bit per node in the sparse region. Set if the
  //     path to this node forms a valid key.
  std::vector<uint8_t> s_labels_;
  BitvectorBuilder s_has_child_;
  BitvectorBuilder s_louds_;
  BitvectorBuilder s_is_prefix_key_;

  // Total number of leaves in the dense section.
  uint64_t dense_leaf_count_;

  // Total number of nodes in all dense levels combined.
  uint64_t dense_node_count_;

  // Number of sparse root nodes: internal children at the last dense level
  // that cross into the sparse region. See LoudsTrie::dense_child_count_.
  uint64_t dense_child_count_;

  // Whether separator keys include seqno encoding. Written to the serialized
  // header so the reader can detect it.
  bool has_seqno_encoding_;

  // ---- Serialized output ----
  std::string serialized_data_;
};

// ============================================================================
// LoudsTrie: Immutable LOUDS-encoded trie for reading.
//
// Deserialized from a flat buffer (e.g., read from an SST meta-block).
// Supports Seek (find the first leaf >= target key) and Next (advance to
// the next leaf in sorted order).
//
// The trie does NOT own the underlying data when initialized from external
// memory (e.g., a block cache entry). The caller must ensure the data remains
// valid for the lifetime of this object.
//
// The trie is organized as:
// - Dense levels [0, cutoff_level_): bitvectors d_labels_, d_has_child_,
//   d_is_prefix_key_ with 256-bit bitmaps per node.
// - Sparse levels [cutoff_level_, max_depth_]: arrays s_labels_,
//   s_has_child_, s_louds_, s_is_prefix_key_.
// ============================================================================
class LoudsTrie {
 public:
  LoudsTrie();

  // LoudsTrie contains Bitvector members (which hold raw pointers into
  // owned or external memory) and a raw pointer to sparse labels data.
  // Copying would create dangling pointers or aliased external references.
  //
  // Move safety: when aligned_copy_ is non-empty, bitvectors and raw
  // pointers reference its buffer. The C++ standard guarantees that
  // std::string's noexcept move constructor transfers the heap buffer
  // without reallocation (noexcept precludes allocation, and COW is
  // forbidden since C++11). Trie data always exceeds the SSO threshold
  // (hundreds to thousands of bytes), so aligned_copy_ is always
  // heap-allocated, and move always preserves the buffer address.
  ~LoudsTrie() = default;

  LoudsTrie(const LoudsTrie&) = delete;
  LoudsTrie& operator=(const LoudsTrie&) = delete;
  LoudsTrie(LoudsTrie&&) = default;
  LoudsTrie& operator=(LoudsTrie&&) = default;

  // Initialize from serialized data. Returns Status::OK() on success,
  // or Status::Corruption() if the data is malformed.
  Status InitFromData(const Slice& data);

  // ---- Accessors ----
  uint64_t NumKeys() const { return num_keys_; }
  uint32_t CutoffLevel() const { return cutoff_level_; }
  uint32_t MaxDepth() const { return max_depth_; }

  // Whether this trie was built with a seqno side-table (enabling post-seek
  // correction for same-user-key block boundaries). When true, the serialized
  // data includes per-leaf seqno and block count arrays, plus overflow block
  // metadata for runs of blocks sharing the same separator key.
  bool HasSeqnoEncoding() const { return has_seqno_encoding_; }

  // Get the block handle for the i-th leaf (0-indexed).
  TrieBlockHandle GetHandle(uint64_t leaf_index) const;

  // Whether this trie has path-compression chains. Used by the iterator
  // to select a specialized Seek implementation at construction time,
  // avoiding any per-level overhead when chains are absent.
  bool HasChains() const { return !s_chain_lens_.empty(); }

  // Approximate heap memory used by auxiliary data structures (child position
  // lookup tables). Does not include the serialized data itself (which is
  // typically owned by the block cache).
  size_t ApproximateAuxMemoryUsage() const {
    return (s_child_start_pos_.capacity() + s_child_end_pos_.capacity()) *
           sizeof(uint32_t);
  }

  // Allow the iterator to access internal bitvectors directly for
  // performance-critical rank/select operations during traversal.
  friend class LoudsTrieIterator;

 private:
  // Number of keys (leaves).
  uint64_t num_keys_;

  // Cutoff level between dense and sparse.
  uint32_t cutoff_level_;

  // Maximum key depth.
  uint32_t max_depth_;

  // True if the trie includes a seqno side-table for post-seek correction.
  // Set from the flags field in the serialized header.
  bool has_seqno_encoding_;

  // Dense leaf count (leaves in levels [0, cutoff_level_)).
  uint64_t dense_leaf_count_;

  // Total number of nodes across all dense levels.
  uint64_t dense_node_count_;

  // Number of sparse root nodes: internal children at the last dense level
  // (cutoff_level_ - 1) that cross into the sparse region. Used to offset
  // sparse node numbering so that children of sparse internal labels are
  // numbered after these root sparse nodes. When cutoff_level_ == 0, this
  // is set to 1 (the root itself).
  uint64_t dense_child_count_;

  // LOUDS-Dense bitvectors (all dense levels concatenated).
  Bitvector d_labels_;
  Bitvector d_has_child_;
  Bitvector d_is_prefix_key_;

  // LOUDS-Sparse (all sparse levels concatenated).
  const uint8_t* s_labels_data_;
  uint64_t s_labels_size_;
  Bitvector s_has_child_;
  Bitvector s_louds_;
  Bitvector s_is_prefix_key_;

  // SuRF-style child position lookup tables for Select-free traversal.
  // Instead of computing FindNthOneBit(node_num) during traversal, we
  // precompute child start/end positions indexed by internal label rank. This
  // allows traversal using only Rank1 (O(1)) and array lookup (O(1)).
  //
  // For the k-th internal label (has_child[pos]=1, where k = Rank1(pos+1)-1):
  //   s_child_start_pos_[k] = start position of child node
  //   s_child_end_pos_[k] = end position (exclusive) of child node
  //
  // Memory overhead: 8 bytes per internal node (2 x uint32_t).
  std::vector<uint32_t> s_child_start_pos_;
  std::vector<uint32_t> s_child_end_pos_;

  // Path compression: chain metadata for fanout-1 chains in the sparse region.
  //
  // A "chain" is a sequence of >= 2 consecutive fanout-1 nodes (nodes with
  // exactly one label that is internal) starting from the child of an internal
  // label. Chains are common in tries with long shared prefixes (e.g.,
  // zero-padded numeric keys, URL paths).
  //
  // For the k-th internal label (same indexing as s_child_start_pos_):
  // Storage uses a bitmap (1 bit per internal label) for O(1) chain detection,
  // plus compact arrays indexed by chain ordinal (Rank1 on the bitmap).
  //
  // Lookup during Seek:
  //   1. s_chain_bitmap_.GetBit(child_idx) -- has chain?
  //   2. chain_idx = s_chain_bitmap_.Rank1(child_idx + 1) - 1
  //   3. s_chain_lens_[chain_idx], s_chain_suffix_offsets_[chain_idx], etc.
  //
  // Space overhead: 1 bit per internal label (bitmap) + 10 bytes per chain
  // (offset + len + end_child_idx) + suffix bytes. For key sets with few
  // chains (e.g., random hex), overhead is < 1 byte per internal label.
  Bitvector s_chain_bitmap_;
  std::vector<uint32_t> s_chain_suffix_offsets_;
  std::vector<uint16_t> s_chain_lens_;
  std::vector<uint32_t> s_chain_end_child_idx_;
  const uint8_t* s_chain_suffix_data_;
  uint64_t s_chain_suffix_size_;

  // Block handles: packed uint32_t arrays for data block offsets and sizes.
  // BFS leaf order does not necessarily match key-sorted order (deeper leaves
  // appear later in BFS even if they precede shallower leaves
  // lexicographically), so offsets are NOT monotonically non-decreasing and
  // cannot use Elias-Fano encoding. Instead, we store offsets and sizes as
  // packed uint32_t arrays for O(1) random access.
  //
  // uint32_t limits individual values to ~4 GB, which is sufficient since
  // RocksDB SST files are typically 64 MB to 1 GB and never exceed 4 GB.
  const uint32_t* handle_offsets_;
  const uint32_t* handle_sizes_;

  // ---- Seqno side-table (deserialized when has_seqno_encoding_ is true) ----
  //
  // The side-table enables post-seek correction for same-user-key block
  // boundaries. It stores per-leaf seqno and block count data in BFS leaf
  // order, plus overflow block handles/seqnos for runs where the same
  // separator maps to multiple blocks.
  //
  // leaf_seqnos_[i]: seqno for the i-th leaf (BFS order).
  //   For non-boundary leaves: stores 0 (sentinel meaning "no seqno
  //   correction needed"), matching the standard index's user-key-only
  //   comparison mode.
  //   For same-user-key boundary and last-block leaves: stores the actual
  //   tag ((sequence_number << 8) | value_type).
  //
  // leaf_block_counts_[i]: how many consecutive blocks share this separator.
  //   1 = no overflow (the common case). >1 = same-user-key run.
  //
  // overflow_offsets_/overflow_sizes_/overflow_seqnos_: packed arrays for
  //   the overflow blocks (total count = sum of (block_count-1) for all
  //   leaves).
  //
  // overflow_base_[i]: prefix sum of (block_count-1) for leaves [0, i),
  //   precomputed during InitFromData() for O(1) random access into the
  //   overflow arrays. overflow_base_[i] is the starting index into the
  //   overflow arrays for leaf i's overflow blocks.
  const uint64_t* leaf_seqnos_;
  const uint32_t* leaf_block_counts_;
  const uint32_t* overflow_offsets_;
  const uint32_t* overflow_sizes_;
  const uint64_t* overflow_seqnos_;
  uint32_t num_overflow_blocks_;
  std::vector<uint32_t> overflow_base_;

 public:
  // ---- Seqno side-table accessors (used by TrieIndexIterator) ----

  // Get the seqno for the i-th leaf (BFS order). Returns 0 (sentinel) for
  // non-boundary leaves.
  uint64_t GetLeafSeqno(uint64_t leaf_index) const {
    assert(has_seqno_encoding_ && leaf_seqnos_ != nullptr);
    assert(leaf_index < num_keys_);
    return leaf_seqnos_[leaf_index];
  }

  // Get the block count for the i-th leaf. Returns 1 for normal leaves.
  uint32_t GetLeafBlockCount(uint64_t leaf_index) const {
    assert(has_seqno_encoding_ && leaf_block_counts_ != nullptr);
    assert(leaf_index < num_keys_);
    return leaf_block_counts_[leaf_index];
  }

  // Get the overflow base (starting index into overflow arrays) for leaf i.
  uint32_t GetOverflowBase(uint64_t leaf_index) const {
    assert(has_seqno_encoding_);
    assert(leaf_index < overflow_base_.size());
    return overflow_base_[leaf_index];
  }

  // Get the handle for the j-th overflow block.
  TrieBlockHandle GetOverflowHandle(uint32_t overflow_index) const {
    assert(overflow_index < num_overflow_blocks_);
    return TrieBlockHandle{overflow_offsets_[overflow_index],
                           overflow_sizes_[overflow_index]};
  }

  // Get the seqno for the j-th overflow block.
  uint64_t GetOverflowSeqno(uint32_t overflow_index) const {
    assert(overflow_index < num_overflow_blocks_);
    return overflow_seqnos_[overflow_index];
  }

 private:
  // Aligned copy of the serialized trie data, used when the input data from
  // the block reader is not 8-byte aligned (e.g., mmap at an unaligned file
  // offset). All Bitvector and raw pointer members reference this buffer
  // when non-empty. std::string::data() returns memory from new[]/malloc,
  // which is aligned to at least alignof(max_align_t) >= 8.
  std::string aligned_copy_;
};

// ============================================================================
// LoudsTrieIterator: Iterates over the leaves of a LoudsTrie.
//
// Supports bidirectional iteration: Seek/SeekToFirst/Next (forward) and
// SeekToLast/Prev (reverse).
// Reconstructs the separator key from the trie path at each position.
//
// The iterator maintains a stack of positions through the trie (one per
// level from root to current leaf). This enables:
// - Key reconstruction: collecting the label byte at each level.
// - Backtracking for Next(): pop to parent, advance to next sibling.
// - Leaf ordinal computation: using rank formulas on bitvectors.
//
// Design follows the SuRF reference implementation for correctness.
// ============================================================================
class LoudsTrieIterator {
 public:
  explicit LoudsTrieIterator(const LoudsTrie* trie);

  // Position on the very first leaf (smallest key) by descending from the
  // root to the leftmost leaf. More efficient than Seek(Slice()) because it
  // skips SeekImpl's target-consumption loop and its redundant prefix key
  // check at root (DescendToLeftmostLeaf handles prefix keys at every node).
  // Returns true if positioned on a valid leaf.
  bool SeekToFirst();

  // Position on the very last leaf (largest key) by descending from the
  // root to the rightmost leaf. Returns true if positioned on a valid leaf.
  bool SeekToLast();

  // Move to the previous leaf in sorted order.
  // Returns true if positioned on a valid leaf, false if before the first.
  bool Prev();

  // Seek to the first leaf whose key is >= `target`.
  // Returns true if positioned on a valid leaf.
  //
  // Dispatches to a specialized implementation selected at construction time
  // based on whether the trie has path-compression chains. This eliminates
  // all chain-related code from the instruction cache when chains are absent,
  // following the same pattern as RocksDB's BlockIter::ParseNextKey template.
  bool Seek(const Slice& target) {
    if (has_chains_) {
      return SeekImpl<true>(target);
    }
    return SeekImpl<false>(target);
  }

  // Advance to the next leaf in sorted order.
  // Returns true if positioned on a valid leaf.
  bool Next();

  // Check if the iterator is positioned on a valid leaf.
  bool Valid() const { return valid_; }

  // Get the current separator key. Valid only when Valid() is true.
  // The returned Slice is valid until the next Seek/Next call.
  Slice Key() const { return Slice(key_buf_.get(), key_len_); }

  // Get the current leaf index (for mapping to block handles).
  uint64_t LeafIndex() const { return leaf_index_; }

  // Get the block handle for the current leaf.
  TrieBlockHandle Value() const;

 private:
  // Position within a single trie level. The iterator maintains a stack
  // of these from root to the current position.
  //
  // Packed into 8 bytes by encoding the is_dense flag in bit 63 of the
  // position value. Since bitvector positions and label array indices are
  // well under 2^63, this is safe. This halves the per-level memory from
  // 16 bytes (with alignment padding) to 8 bytes, improving cache
  // utilization for the path_ stack.
  struct LevelPos {
    // Position in the bitvector/label array at this level, with the
    // is_dense flag encoded in the high bit (bit 63).
    // - Dense: bit position in d_labels_ (= node_num * 256 +
    //   label_byte). The label byte is pos % 256.
    // - Sparse: index into s_labels_ array.
    uint64_t pos_and_flag;

    static constexpr uint64_t kDenseFlag = uint64_t(1) << 63;

    uint64_t pos() const { return pos_and_flag & ~kDenseFlag; }
    bool is_dense() const { return (pos_and_flag & kDenseFlag) != 0; }

    static LevelPos MakeDense(uint64_t p) { return {p | kDenseFlag}; }
    static LevelPos MakeSparse(uint64_t p) { return {p}; }
  };

  // --- Dense level helpers ---

  // Seek within a dense node for a target label byte.
  // Sets result to the position of the label if found, or the position of
  // the next label >= target_byte if not found.
  // Returns true if the exact label was found, false if we landed on a
  // label > target_byte (or no label exists).
  bool DenseSeekLabel(uint64_t node_num, uint8_t target_byte,
                      uint64_t* out_pos);

  // Compute the child node number for a dense internal child with the given
  // label_rank. Takes a pre-computed label_rank to avoid redundant
  // Rank1(d_labels_) calls in hot paths where label_rank was already computed
  // for has_child checking.
  uint64_t DenseChildNodeNumFromRank(uint64_t label_rank) const;

  // Compute the leaf ordinal for a dense leaf at `pos`.
  // leaf_idx = rank1(d_labels_, pos+1) - rank1(d_has_child_,
  //   rank1(d_labels_, pos+1)) + rank1(d_is_prefix_key_, node_num+1) - 1
  // Takes a pre-computed label_rank.
  uint64_t DenseLeafIndexFromRank(uint64_t pos, uint64_t label_rank) const;

  // Same as DenseLeafIndexFromRank but also takes a pre-computed
  // d_has_child_.Rank1(label_rank + 1) to avoid redundant rank call.
  uint64_t DenseLeafIndexFromRankAndHasChildRank(uint64_t pos,
                                                 uint64_t label_rank,
                                                 uint64_t has_child_rank) const;

  // Compute the leaf ordinal for a dense prefix key at node `node_num`.
  // A prefix key leaf comes before any child leaves of that node.
  uint64_t DensePrefixKeyLeafIndex(uint64_t node_num) const;

  // --- Sparse level helpers ---

  // Seek within a sparse node starting at `node_start_pos` for label byte.
  // Returns true if the exact label was found, false otherwise. Writes the
  // position to `out_pos`. `node_end_pos` is one past the last label
  // position of this node.
  bool SparseSeekLabel(uint64_t node_start_pos, uint64_t node_end_pos,
                       uint8_t target_byte, uint64_t* out_pos);

  // Compute the child node number for a sparse internal child at `pos`.
  // child_node_num = dense_child_count_ + rank1(s_has_child_, pos+1) - 1
  // (offset by dense_child_count_ because children of sparse internal labels
  // are numbered after the root sparse nodes).
  uint64_t SparseChildNodeNum(uint64_t pos) const;

  // Compute the leaf ordinal for a sparse leaf at `pos`.
  uint64_t SparseLeafIndex(uint64_t pos) const;

  // Same as SparseLeafIndex but takes a pre-computed
  // s_has_child_.Rank1(pos + 1) to avoid redundant rank call.
  uint64_t SparseLeafIndexFromHasChildRank(uint64_t pos,
                                           uint64_t has_child_rank) const;

  // Compute the leaf ordinal for a sparse prefix key at sparse node
  // `sparse_node_num`. The sparse_node_num is 0-indexed among sparse nodes
  // only (not including dense nodes).
  uint64_t SparsePrefixKeyLeafIndex(uint64_t sparse_node_num) const;

  // Get the sparse node number (0-indexed among sparse nodes) from a
  // position in the s_labels_ array.
  uint64_t SparseNodeNum(uint64_t pos) const;

  // Get the start position (in s_labels_) for sparse node `sparse_node_num`.
  uint64_t SparseNodeStartPos(uint64_t sparse_node_num) const;

  // Get the end position (one past last label) for a sparse node starting
  // at `start_pos`.
  uint64_t SparseNodeEndPos(uint64_t start_pos) const;

  // --- Traversal helpers ---

  // Descend from the given node to the leftmost leaf in its subtree,
  // pushing entries onto path_ and building key_buf_. Sets
  // leaf_index_ and valid_. Returns true if a leaf was found.
  bool DescendToLeftmostLeaf(bool in_dense, uint64_t node_num);

  // Descend from the given node to the rightmost leaf in its subtree,
  // pushing entries onto path_ and building key_buf_. Sets
  // leaf_index_ and valid_. Returns true if a leaf was found.
  // Unlike DescendToLeftmostLeaf, this does NOT check prefix keys --
  // prefix keys are the smallest leaf at a node, so in reverse order
  // they are visited last (handled by Retreat when backtracking).
  bool DescendToRightmostLeaf(bool in_dense, uint64_t node_num);

  // Advance to the next valid leaf by backtracking up the trie path
  // and finding the next sibling label, then descending to the leftmost
  // leaf in that subtree. Used by Next() and SeekImpl().
  // Returns true if a next leaf was found.
  bool Advance();

  // Retreat to the previous valid leaf by backtracking up the trie path
  // and finding the previous sibling label, then descending to the
  // rightmost leaf in that subtree. When no previous sibling exists,
  // checks if the current node has a prefix key (which is the smallest
  // leaf at a node, so it comes last in reverse order).
  // Used by Prev().
  // Returns true if a previous leaf was found.
  bool Retreat();

  // Seek implementation, templated on whether path-compression chains exist.
  // When kHasChains=false, the compiler eliminates ALL chain-related code,
  // keeping the i-cache footprint minimal for tries without chains.
  // This follows the same pattern as RocksDB's BlockIter::ParseNextKey.
  template <bool kHasChains>
  bool SeekImpl(const Slice& target);

  // True if the trie has path-compression chains. Set once in the constructor
  // and used by Seek() to dispatch to the correct specialization.
  bool has_chains_;

  const LoudsTrie* trie_;
  bool valid_;
  uint64_t leaf_index_;

  // The reconstructed key at the current position. Each byte corresponds
  // to a level in path_. For dense levels, the byte is pos % 256; for
  // sparse levels, the byte is s_labels_[pos].
  //
  // Key reconstruction appends one byte per trie level in the Seek/Next
  // hot loop, so the append operation must be as cheap as possible -- a
  // single inlined store + increment with no function call overhead. The
  // buffer is heap-allocated once in the constructor to MaxDepth()+1 bytes.
  //
  // All append sites go through AppendKeySlot() which validates bounds in
  // debug builds. In release builds it compiles to the same single store +
  // increment with no overhead (the assert is elided).
  std::unique_ptr<char[]> key_buf_;
  uint32_t key_len_;
  uint32_t key_cap_;

  // Returns a reference to the next key buffer slot, advancing key_len_.
  // Validates in debug builds that the buffer has space. A corrupted trie
  // with depth > max_depth_ would overflow key_buf_ without this check.
  char& AppendKeySlot() {
    assert(key_len_ < key_cap_ &&
           "key_buf_ overflow: trie depth exceeds max_depth_");
    return key_buf_[key_len_++];
  }

  // Stack of positions from root to current leaf. path_[i] holds the
  // position at depth i in the trie. path_.size() equals key_len_ for
  // both child leaves and prefix keys (in which case the last path_
  // entry's label is not appended to
  // key_buf_ since the node itself is the key).
  //
  // For a prefix key, we mark it by setting is_at_prefix_key_ = true and
  // the path_ only goes up to the prefix key node level (no label entry
  // for the prefix key itself since the key terminates at the node, not
  // at a child edge).
  //
  // Uses autovector with 24 inline slots to avoid heap allocation for
  // tries up to 24 levels deep. Most real-world key sets have depth < 24.
  autovector<LevelPos, 24> path_;

  // True if the current leaf is a prefix key (the key terminates at a
  // node that also has children). In this case, path_.size() == depth
  // of the node and key_buf_[0..key_len_) holds the prefix key value.
  bool is_at_prefix_key_;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
