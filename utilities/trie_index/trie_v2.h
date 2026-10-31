//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  Trie index, serialized format version 2.
//
//  Maps the separator of every data block in an SST to the block's handle.
//  Separators are user keys (bytewise ordered) plus an optional tag (packed
//  sequence number and value type), and Seek finds the first block whose
//  separator is at or after a target in internal key order.
//
//  Structure, following the block-tree design used by Lucene and tantivy:
//  - The bytes shared by every separator are stored once.
//  - Distinct separators are grouped into fixed-size buckets. Inside a bucket
//    each separator is front coded against the previous one.
//  - A path-compressed byte trie over the first separator of each bucket (the
//    bucket head) routes a Seek to one bucket. Trie nodes are typed (leaf,
//    sparse or bitmap), hold their compressed path bytes inline, and point to
//    children through narrow backward-relative offsets.
//  - Block offsets are stored as a linear frame-of-reference column, so
//    there is no file size limit. Block sizes are derived from consecutive
//    offsets when the gaps between blocks are uniform.
//  - Blocks that share a separator (one user key spanning several blocks)
//    and nonzero tags are stored in small sparse side tables.
//
//  Serialized layout. Integers are little-endian and read with unaligned
//  loads, so the index is used in place at any alignment and on any host.
//
//    Header (kTrieV2HeaderSize bytes):
//      u32 magic        "TRIE", shared with format version 1
//      u32 version      2
//      u32 flags        0; readers reject any other value
//      u32 bucket_size  separators per bucket, at least 1
//      u64 num_blocks   data blocks indexed
//      u64 num_keys     distinct separators, at most num_blocks
//      u32 prefix_len   length of the prefix shared by every separator
//      u32 max_key_len  length of the longest separator
//      u32 id_width     bytes per bucket id in trie nodes, 1 to 4
//      u32 trie_root    position of the root node within the trie section
//      u64 size_gap     block size = next block offset - offset - size_gap,
//                       or all ones when sizes are stored explicitly
//      u64 trie_offset, bucket_table_offset, bucket_data_offset,
//          offset_table_offset, size_table_offset, run_table_offset,
//          tag_table_offset, end_offset
//                       section boundaries from the start of the index.
//                       Sections are contiguous and in this order;
//                       size_table_offset is 0 when sizes are derived.
//    Shared prefix bytes
//    Trie section
//    Bucket table: column of num_buckets + 1 positions in the bucket data
//    Bucket data
//    Offset table: column of num_blocks + 1 offsets; the last one is where a
//      next block would start
//    Size table (only when sizes are explicit): column of num_blocks sizes
//    Run table: u64 count, then count entries of
//      {u64 key_index, u64 extra_blocks_through}. One entry per separator
//      shared by more than one block, in key order. extra_blocks_through is
//      the total number of extra blocks of this run and all earlier runs.
//    Tag table: u64 count, then count entries of {u64 block_index, u64 tag}
//      for every block with a nonzero tag, in block order.
//    16 zero bytes, so that 8-byte loads near the end stay in bounds.
//
//  Column (linear frame of reference, groups of 64 values):
//    u64 count, u32 width, u32 0, then per group {u64 base, u64 slope}, then
//    count residuals of `width` bits packed little-endian, then 8 zero bytes.
//    Value i is base + slope * (i % 64) + residual(i) in 64-bit unsigned
//    arithmetic, where base and slope belong to group i / 64.
//
//  Trie node:
//    u8 header: bits 0-1 kind (0 leaf, 1 sparse, 2 bitmap), bit 2 terminal,
//      bits 3-4 child pointer width - 1, bits 5-7 zero. A leaf is terminal.
//    Path length: u8, or 255 followed by a varint32.
//    Path bytes: the compressed edge leading into this node's labels.
//    Terminal nodes: bucket id (id_width bytes). The node's path spells that
//      bucket's head.
//    Sparse nodes: u8 child count - 1, then the child labels in ascending
//      order.
//    Bitmap nodes: 256-bit label bitmap as four u64 words.
//    Child pointers, one per label, pointer width bytes each. A child starts
//      `pointer` bytes before its parent; children are always written first.
//
//  Bucket data: for every bucket, its separators after the head, each as
//    u8 (drop << 4 | add) when drop < 15 and add < 15, otherwise 0xFF
//    followed by varint32 drop and varint32 add; then `add` suffix bytes.
//    The separator is the previous one minus its last `drop` bytes, plus the
//    suffix.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "utilities/trie_index/trie_block_handle.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

inline constexpr uint32_t kTrieV2FormatVersion = 2;
inline constexpr size_t kTrieV2HeaderSize = 120;

// Builds a format version 2 trie index from the separators of an SST's data
// blocks, added in block order.
class TrieV2Builder {
 public:
  // bucket_size is the number of distinct separators per bucket. Larger
  // buckets make the index smaller and Seek slower.
  explicit TrieV2Builder(uint32_t bucket_size);

  // Adds the next data block. Separators must be bytewise non-decreasing.
  // Consecutive equal separators describe one user key spanning several
  // blocks. tag is 0 when the block can be found by user key alone,
  // otherwise the packed sequence number and type of the block's last key.
  void AddBlock(const Slice& separator, const TrieBlockHandle& handle,
                uint64_t tag);

  // Serializes the index into *output. Fails if separators were added out of
  // order, or a column value or the trie does not fit the format.
  Status Finish(std::string* output);

 private:
  const uint32_t bucket_size_;
  // Distinct separators, concatenated, and the end of each one.
  std::string key_bytes_;
  std::vector<size_t> key_ends_;
  // Number of blocks sharing each distinct separator, minus one, for the
  // separators where it is nonzero: {key index, extra blocks}.
  std::vector<std::pair<uint64_t, uint64_t>> runs_;
  std::vector<TrieBlockHandle> handles_;
  // {block index, tag} for every block with a nonzero tag.
  std::vector<std::pair<uint64_t, uint64_t>> tags_;
  bool out_of_order_ = false;
};

// Read-only view of a serialized format version 2 index. Init() validates
// everything except the bucket data, which the iterator checks as it decodes.
class TrieV2Index {
 public:
  TrieV2Index() = default;

  // Parses data, which must stay valid and unchanged for the lifetime of
  // this object and its iterators.
  Status Init(const Slice& data);

  uint64_t NumBlocks() const { return num_blocks_; }

 private:
  friend class TrieV2Iterator;

  // One frame-of-reference column.
  struct Column {
    const char* groups = nullptr;
    const char* residuals = nullptr;
    uint64_t count = 0;
    uint64_t mask = 0;
    uint32_t width = 0;

    // Parses a column of expected_count values occupying exactly
    // [begin, end).
    Status Init(const char* begin, const char* end, uint64_t expected_count);
    uint64_t Get(uint64_t i) const;
  };

  Status ValidateSideTables(uint64_t run_table_size, uint64_t tag_table_size);
  uint64_t RunKey(uint64_t r) const;
  uint64_t RunExtraThrough(uint64_t r) const;
  uint64_t TagBlock(uint64_t t) const;
  uint64_t TagValue(uint64_t t) const;
  Status ValidateTrie(uint64_t trie_size);

  const char* data_ = nullptr;
  uint64_t num_blocks_ = 0;
  uint64_t num_keys_ = 0;
  uint64_t num_buckets_ = 0;
  uint64_t size_gap_ = 0;
  const char* prefix_ = nullptr;
  const char* trie_ = nullptr;
  const char* bucket_data_ = nullptr;
  const char* runs_ = nullptr;
  const char* tags_ = nullptr;
  uint64_t num_runs_ = 0;
  uint64_t num_tags_ = 0;
  Column bucket_table_;
  Column offset_table_;
  Column size_table_;
  uint32_t bucket_size_ = 0;
  uint32_t prefix_len_ = 0;
  uint32_t max_key_len_ = 0;
  uint32_t id_width_ = 0;
  uint32_t trie_root_ = 0;
  bool explicit_sizes_ = false;
};

// Iterates the blocks of a TrieV2Index in order. Positioning methods return
// Corruption if they meet malformed bucket data, and leave the iterator
// invalid.
class TrieV2Iterator {
 public:
  explicit TrieV2Iterator(const TrieV2Index* index);

  Status SeekToFirst();
  Status SeekToLast();
  // Positions at the first block whose separator is at or after (key, tag)
  // in internal key order: by key ascending, then by tag descending, where a
  // block without a tag sorts after every tag of its key.
  Status Seek(const Slice& key, uint64_t tag);
  Status Next();
  Status Prev();

  bool Valid() const { return block_ < index_->num_blocks_; }
  // The current block's separator. Valid until the next positioning call.
  Slice key() const { return Slice(key_); }
  TrieBlockHandle value() const;

 private:
  // A trie position from which a bucket head can be spelled out: the
  // terminal node itself, or the smallest or largest head below a node.
  enum class Spell : uint8_t { kNone, kTerminal, kSmallest, kLargest };
  struct Candidate {
    uint32_t node = 0;
    // Number of target bytes (after the shared prefix) that lead to node.
    uint32_t depth = 0;
    // Label leading from the parent into node, or -1 if depth already
    // includes it.
    int label = -1;
    Spell spell = Spell::kNone;
  };
  struct Node;

  void Invalidate();
  // Finds the bucket heads closest below (floor) and at or above (ceiling)
  // target, which excludes the shared prefix.
  void Descend(const char* target, size_t target_len, Candidate* floor,
               Candidate* ceiling) const;
  // Writes the head that candidate leads to into key_ and returns its bucket
  // id. target holds at least candidate.depth bytes.
  uint64_t SpellHead(const Candidate& candidate, const char* target);
  // Positions on the head of bucket, whose key is already in key_.
  void EnterBucket(uint64_t bucket);
  // Decodes the next separator in the bucket into key_. Returns the length
  // shared with the previous separator, excluding the shared prefix, and the
  // suffix through *suffix and *suffix_len.
  Status DecodeNext(size_t* shared, const char** suffix, size_t* suffix_len);
  // Moves from the bucket head to the first separator >= target in this
  // bucket. Sets *found to false if every separator is smaller.
  Status ScanBucket(const char* target, size_t target_len, bool* found);
  // Sets the block range of the current key index and positions on its
  // first block.
  void EnterKey();
  // After positioning on the first separator >= target, moves past the blocks
  // of that separator whose tags are greater than tag, if it equals target.
  Status ApplyTag(const Slice& target, uint64_t tag);
  Status NextKey();
  // Decodes forward in the current bucket until the current key index is
  // key_index.
  Status AdvanceTo(uint64_t key_index);
  // Positions on the last block of the current key.
  void EnterKeyFromEnd();

  const TrieV2Index* index_;
  std::string key_;
  std::string scratch_;
  const char* bucket_pos_ = nullptr;
  const char* bucket_end_ = nullptr;
  uint64_t bucket_ = 0;
  uint64_t key_index_ = 0;
  // Blocks [run_begin_, run_end_) share the current separator.
  uint64_t run_begin_ = 0;
  uint64_t run_end_ = 0;
  uint64_t block_;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
