//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  Serialized trie index.
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
//  - Block offsets are 64-bit values in a column (see "Column" below), so
//    there is no 32-bit limit; Finish() rejects only extreme layouts (see
//    AppendColumn() in trie.cc). The gap after a block is the number of bytes
//    from its end to the start of the next block, normally the block trailer.
//    When every gap is the same, block sizes are not stored: a size is the
//    next offset minus the offset minus size_gap.
//  - Blocks that share a separator (one user key spanning several blocks)
//    and nonzero tags are stored in small sparse side tables.
//
//  Serialized layout. Integers are little-endian and read with unaligned
//  loads, so the index is used in place at any alignment and on any host.
//
//    Header (kTrieHeaderSize bytes):
//      u32 magic              0x54524945, stored as the bytes "EIRT"
//      u32 version            2. Version 1 was an earlier layout that readers
//                             reject.
//      u32 flags              0; readers reject any other value
//      u32 bucket_size        separators per bucket, at least 1
//      u64 num_blocks         data blocks indexed
//      u64 num_separators     distinct separators, at most num_blocks
//      u32 prefix_len         length of the prefix shared by every separator
//      u32 max_separator_len  length of the longest separator
//      u32 id_width           bytes per bucket id in trie nodes, 1 to 4
//      u32 trie_root          position of the root node within the trie
//                             section
//      u64 size_gap           block size = next block offset - offset -
//                             size_gap, or all ones when sizes are stored
//                             explicitly
//      u64 trie_offset, bucket_table_offset, bucket_data_offset,
//          offset_table_offset, size_table_offset, run_table_offset,
//          tag_table_offset, end_offset
//                             section boundaries from the start of the index.
//                             Sections are contiguous and in this order;
//                             size_table_offset is 0 when sizes are derived,
//                             and end_offset is the size of the index.
//    Shared prefix bytes
//    Trie section
//    Bucket table: column of num_buckets + 1 positions in the bucket data
//    Bucket data
//    Offset table: column of num_blocks + 1 non-decreasing offsets. When
//      sizes are derived, the last one is where a next block would start;
//      otherwise it repeats the last block's offset
//    Size table (only when sizes are explicit): column of num_blocks sizes
//    Run table: u64 count, then count entries of
//      {u64 separator_index, u64 cumulative_extra_blocks}. One entry per
//      separator shared by more than one block, in separator order.
//      cumulative_extra_blocks counts the blocks after the first of this run
//      and all earlier runs. For example, if separator 3 spans 2 blocks and
//      separator 7 spans 4, the entries are {3, 1} and {7, 4}, and separator
//      7's blocks start at block 7 + 1 = 8.
//    Tag table: u64 count, then count entries of {u64 block_index, u64 tag}
//      for every block with a nonzero tag, in block order. The tags of
//      consecutive blocks that share a separator do not ascend, which lets
//      Seek binary search them. It ends the index.
//
//  Column (linear frame of reference, groups of 64 values):
//    u64 count, then per group {u64 base, u32 slope, u32 layout}, then the
//    groups' residuals back to back, then 8 zero bytes.
//    Value i is base + slope * (i % 64) + residual(i) in 64-bit unsigned
//    arithmetic, where base, slope and layout belong to group i / 64: each
//    group's values are a straight line plus a small residual per value.
//    layout = (start_word << 6) | width. width is the number of bits per
//    residual in the group, at most 56, so one unaligned 8-byte load and a
//    shift of up to 7 bits read any residual. start_word is where the
//    group's residuals begin, in 8-byte words from the first group's, and
//    equals the total size of the earlier groups' residuals. A group of n
//    values takes ceil(n * width / 64) words, its residuals packed
//    little-endian.
//
//  Trie node:
//    u8 header: bits 0-1 kind (0 leaf, 1 sparse, 2 bitmap), bit 2 terminal,
//      bits 3-4 child pointer width - 1, bits 5-7 zero. A leaf is terminal.
//    Path length: u8, or 255 followed by a varint32.
//    Path bytes: the compressed edge leading into this node's labels.
//    Terminal nodes: bucket id (id_width bytes). The paths and labels from
//      the root to this node spell that bucket's head, without the shared
//      prefix.
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
//    suffix. drop is as small as it can be: the suffix is not empty and does
//    not start with the byte it replaces. Seek relies on that to pass over a
//    separator without comparing its bytes.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

inline constexpr size_t kTrieHeaderSize = 120;

// Offset and size of a data block in the SST file.
struct TrieBlockHandle {
  uint64_t offset = 0;
  uint64_t size = 0;
};

// Builds a trie index from the separators of an SST's data blocks, added in
// block order.
class TrieBuilder {
 public:
  // bucket_size is the number of distinct separators per bucket. Larger
  // buckets make the index smaller and Seek slower.
  explicit TrieBuilder(uint32_t bucket_size);

  // Adds the next data block. Separators must be bytewise non-decreasing,
  // and so must block offsets.
  // Consecutive equal separators describe one user key spanning several
  // blocks. tag is 0 when the block can be found by user key alone,
  // otherwise the packed sequence number and type of the block's last key.
  // The tags of consecutive blocks with equal separators must not ascend.
  void AddBlock(const Slice& separator, const TrieBlockHandle& handle,
                uint64_t tag);

  // Blocks added so far, and the last distinct separator added, which is the
  // most recent block's separator when separators are in order (empty when
  // there are no blocks). The separator stays valid until the next
  // AddBlock().
  uint64_t NumBlocks() const { return handles_.size(); }
  Slice LastSeparator() const;

  // Serializes the index into *output. Fails if separators, tags or offsets
  // were added out of order, or a column value or the trie does not fit the
  // format.
  Status Finish(std::string* output);

  // Bytes a separator takes in its bucket when it drops `drop` bytes from the
  // end of the previous separator and adds `add`.
  static uint64_t EntrySize(uint64_t drop, uint64_t add);

  // Running totals over the blocks added so far, from which SizeBound()
  // bounds the size Finish() produces.
  struct SizeStats {
    uint64_t num_blocks = 0;
    // Bytes of each separator past the prefix it shares with the previous
    // one, and the EntrySize() of every separator.
    uint64_t suffix_bytes = 0;
    uint64_t entry_bytes = 0;
    uint64_t max_separator_len = 0;
    // Separators equal to the previous one, and blocks that may get a
    // nonzero tag.
    uint64_t duplicates = 0;
    uint64_t tagged = 0;
    // Largest distance between consecutive block offsets, largest block
    // size, and whether Finish() must store block sizes: two gaps between
    // blocks differ, two blocks overlap, a block ends past 2^64 - 1, or the
    // offset where a next block would start does not fit 64 bits.
    uint64_t max_block_step = 0;
    uint64_t max_block_size = 0;
    bool irregular_gaps = false;
  };
  // bucket_size is the builder's, and must be positive.
  static uint64_t SizeBound(const SizeStats& stats, uint32_t bucket_size);

 private:
  const uint32_t bucket_size_;
  // Distinct separators, concatenated, and the end of each one.
  std::string separator_bytes_;
  std::vector<size_t> separator_ends_;
  // A distinct separator shared by more than one block.
  struct Run {
    uint64_t separator_index;
    // Number of blocks sharing the separator, minus one.
    uint64_t extra_blocks;
  };
  struct Tag {
    uint64_t block_index;
    uint64_t tag;
  };
  std::vector<Run> runs_;
  std::vector<TrieBlockHandle> handles_;
  // Every block with a nonzero tag.
  std::vector<Tag> tags_;
  // Tag of the block added last, 0 when it has none.
  uint64_t last_tag_ = 0;
  bool out_of_order_ = false;
  bool tags_ascend_ = false;
};

// Read-only view of a serialized trie index. Init() validates the header,
// section and column layouts, side tables and trie. It does not visit every
// bucket position, block offset or separator, or check that the tags of one
// separator descend; the iterator keeps reads of them inside the index and
// returns Corruption for a separator out of bounds or a block size that would
// wrap around. Other wrong bytes give a wrong key or handle, as the index has
// no checksum of its own.
//
// Init() proves that the trie is a tree whose terminal nodes, in pre-order,
// hold bucket ids 0, 1, 2, ..., so the iterator parses trie nodes without
// bounds checks. As the buckets themselves are not validated, the iterator
// checks that a walk reaches the bucket it must (the next or previous one, the
// first or the last).
class Trie {
 public:
  Trie() = default;

  // Parses data, which must stay valid and unchanged for the lifetime of
  // this object and its iterators. On failure the index is left empty.
  Status Init(const Slice& data);

 private:
  friend class TrieIterator;
  friend class TrieTest;

  uint64_t NumBlocks() const { return num_blocks_; }

  // One frame-of-reference column.
  struct Column {
    const char* groups = nullptr;
    const char* residuals = nullptr;
    uint64_t count = 0;

    // Parses a column of expected_count values occupying exactly
    // [begin, end).
    Status Init(const char* begin, const char* end, uint64_t expected_count);
    uint64_t Get(uint64_t i) const;
  };

  Status ValidateSideTables(uint64_t run_table_size, uint64_t tag_table_size);
  // Index of the first entry of a side table, of count entries, whose first
  // field is at least value.
  static uint64_t SideTableLowerBound(const char* table, uint64_t count,
                                      uint64_t value);
  uint64_t RunSeparator(uint64_t r) const;
  uint64_t RunCumulativeExtraBlocks(uint64_t r) const;
  uint64_t TagBlock(uint64_t t) const;
  uint64_t TagValue(uint64_t t) const;
  Status Parse(const Slice& data);
  Status ValidateTrie(uint64_t trie_size);

  uint64_t num_blocks_ = 0;
  uint64_t num_separators_ = 0;
  uint64_t num_buckets_ = 0;
  uint64_t size_gap_ = 0;
  const char* prefix_ = nullptr;
  const char* trie_ = nullptr;
  const char* bucket_data_ = nullptr;
  const char* runs_ = nullptr;
  const char* tags_ = nullptr;
  uint64_t num_runs_ = 0;
  uint64_t num_tags_ = 0;
  uint64_t bucket_data_size_ = 0;
  Column bucket_table_;
  Column offset_table_;
  Column size_table_;
  uint32_t bucket_size_ = 0;
  uint32_t prefix_len_ = 0;
  uint32_t max_separator_len_ = 0;
  uint32_t id_width_ = 0;
  uint32_t trie_root_ = 0;
  bool explicit_sizes_ = false;
};

// Iterates the blocks of a Trie in order. Positioning methods return
// Corruption, and leave the iterator invalid, if a separator or block handle
// they read is malformed or the trie leads to an unexpected bucket.
//
// Position: key_ holds the current separator, with the shared prefix.
// separator_index_ is its index among the distinct separators and bucket_ is
// its bucket. block_ is the current block, in [run_begin_, run_end_), the
// blocks that share the separator.
//
// Seek(target, tag):
// 1. Compare target with the shared prefix. If target is past every
//    separator the iterator becomes invalid, and if it is before every
//    separator Seek moves to the first block.
// 2. Descend() walks the trie with the rest of target and returns the floor
//    (the largest bucket head below it) and the ceiling (the smallest head at
//    or above it).
// 3. If the ceiling equals target, enter its bucket at the head and go to
//    step 5. If there is no floor, every head is after target, and Seek
//    moves to the first block.
// 4. MeasureFloor() finds the floor's bucket and the length of its head, and
//    ScanBucket() decodes the bucket up to the first separator at or after
//    target, which is the only one it writes into key_. If there is none,
//    the answer is the next bucket's head, the ceiling, or the iterator
//    becomes invalid when there is no next bucket.
// 5. EnterSeparator() finds the separator's blocks in the run table. If the
//    separator equals target, ApplyTag() skips its blocks whose tag is
//    greater than tag, and moves to the next separator if it skips them all.
//
// Next() moves to the next block of the separator, or with NextSeparator()
// to the next separator. Prev() moves to the previous block of the
// separator, or decodes forward from the previous separator's bucket head.
// Prev() keeps the last head it found and that bucket's bounds, so it calls
// Descend() only for a bucket other than that one.
class TrieIterator {
 public:
  explicit TrieIterator(const Trie* index);

  Status SeekToFirst();
  Status SeekToLast();
  // Positions at the first block whose separator is at or after
  // (target, tag) in internal key order: by separator ascending, then by tag
  // descending, where a block without a tag sorts after every tag of its
  // separator.
  Status Seek(const Slice& target, uint64_t tag);
  Status Next();
  // Moving to the previous separator costs up to bucket_size - 1 decodes,
  // because front coding only decodes forward from a bucket head, plus a
  // Descend() to find that head when it is in another bucket than the one
  // Prev() last found.
  Status Prev();

  bool Valid() const { return block_ < index_->num_blocks_; }
  // The current block's separator. Valid until the next positioning call; it
  // may be passed to Seek().
  Slice key() const { return Slice(key_); }
  TrieBlockHandle value() const { return handle_; }

 private:
  // A trie position from which a bucket head can be spelled out: the
  // terminal node itself, or the smallest or largest head below a node.
  enum class Spell : uint8_t { kNone, kTerminal, kSmallest, kLargest };
  struct Candidate {
    // Number of target bytes (after the shared prefix) that start the head:
    // the bytes before node's path, or before the label when has_label. For
    // kTerminal they include node's path and are the whole head.
    size_t depth = 0;
    uint32_t node = 0;
    Spell spell = Spell::kNone;
    // Whether the label leading from the parent into node follows the depth
    // bytes, rather than being included in them.
    bool has_label = false;
    uint8_t label = 0;
    // For a floor found where target leaves node's path, the number of path
    // bytes that match target before it does, and 0 for other candidates. A
    // floor's head starts with depth + path_matched bytes of target, then
    // ends or continues with a smaller byte.
    size_t path_matched = 0;
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
  // Returns the bucket id of the head a floor leads to, and through *len its
  // length without the shared prefix, without writing it.
  uint64_t MeasureFloor(const Candidate& floor, size_t* len) const;
  // Positions on the head of bucket, whose key is already in key_.
  void EnterBucket(uint64_t bucket);
  // Spells the head candidate leads to, which must be bucket's, and positions
  // on it. Returns false, leaving the iterator invalid, if the trie leads
  // elsewhere, which means it is corrupt.
  bool EnterBucketAt(const Candidate& candidate, const char* target,
                     uint64_t bucket);
  // A separator in front coding: it starts with the first `shared` bytes of
  // the previous separator, excluding the shared prefix, and continues with
  // the suffix_len bytes at suffix.
  struct Entry {
    size_t shared = 0;
    const char* suffix = nullptr;
    size_t suffix_len = 0;
  };
  // Moves to the next separator in the bucket without writing it into key_.
  // len is the current separator's length, excluding the shared prefix.
  // Returns false if the entry is corrupt.
  bool ReadEntry(size_t len, Entry* entry);
  // Moves to the next separator in the bucket and writes it into key_.
  Status DecodeNext();
  // Moves from the bucket head, which is below target, to the first
  // separator >= target in this bucket. head_len is the head's length and
  // matched the number of target bytes it starts with, both without the
  // shared prefix. Sets *found to false if every separator is smaller, and
  // *equal to whether the separator found equals target. Writes the
  // separator found into key_, and leaves key_ as it was when none is.
  Status ScanBucket(const char* target, size_t target_len, size_t head_len,
                    size_t matched, bool* found, bool* equal);
  // Sets the block range of the current separator index.
  void EnterSeparator();
  // Positions on block and reads its handle.
  Status EnterBlock(uint64_t block);
  // After positioning on a separator equal to the Seek target, moves past its
  // blocks whose tags are greater than tag.
  Status ApplyTag(uint64_t tag);
  // Moves to the first block of the next separator, or makes the iterator
  // invalid after the last one. At the end of a bucket it needs the next
  // bucket's head, and the trie has no map from bucket id to node, so it
  // calls Descend() with the current separator (without the shared prefix)
  // plus a 0 byte. That is the smallest string greater than the separator,
  // so its ceiling is the next bucket's head.
  Status NextSeparator();
  // Decodes forward in the current bucket until the current separator index is
  // separator_index.
  Status AdvanceTo(uint64_t separator_index);
  // Positions on the last block of the current separator.
  Status EnterSeparatorFromEnd();

  static constexpr uint64_t kNoBucket = ~uint64_t{0};
  const Trie* index_;
  std::string key_;
  // Scratch buffer for a Seek target that points into key_, and for the
  // current separator during NextSeparator() and Prev() trie descents.
  std::string scratch_;
  // The head, with the shared prefix, of bucket prev_head_bucket_, which is
  // the bucket Prev() last found by descending the trie, and the bucket's
  // bounds in the bucket data. The trie does not change, so they stay valid
  // for the iterator's life.
  std::string prev_head_;
  uint64_t prev_head_bucket_ = kNoBucket;
  const char* prev_bucket_pos_ = nullptr;
  const char* prev_bucket_end_ = nullptr;
  const char* bucket_pos_ = nullptr;
  const char* bucket_end_ = nullptr;
  uint64_t bucket_ = 0;
  uint64_t separator_index_ = 0;
  // Blocks [run_begin_, run_end_) share the current separator.
  uint64_t run_begin_ = 0;
  uint64_t run_end_ = 0;
  uint64_t block_;
  TrieBlockHandle handle_;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
