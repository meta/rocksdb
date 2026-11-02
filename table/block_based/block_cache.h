//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Code supporting block cache (Cache) access for block-based table, based on
// the convenient APIs in typed_cache.h

#pragma once

#include <type_traits>

#include "cache/typed_cache.h"
#include "port/lang.h"
#include "table/block_based/block.h"
#include "table/block_based/block_type.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

// Metaprogramming wrappers for Block, to give each type a single role when
// used with FullTypedCacheInterface.
// (NOTE: previous attempts to create actual derived classes of Block with
// virtual calls resulted in performance regression)

class Block_kData : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kDataBlock;
  static constexpr BlockType kBlockType = BlockType::kData;
};

class Block_kIndex : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kIndexBlock;
  static constexpr BlockType kBlockType = BlockType::kIndex;
};

class Block_kFilterPartitionIndex : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole =
      CacheEntryRole::kFilterMetaBlock;
  static constexpr BlockType kBlockType = BlockType::kFilterPartitionIndex;
};

class Block_kRangeDeletion : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kOtherBlock;
  static constexpr BlockType kBlockType = BlockType::kRangeDeletion;
};

// Useful for creating the Block even though meta index blocks are not
// yet stored in block cache
class Block_kMetaIndex : public Block {
 public:
  using Block::Block;

  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kOtherBlock;
  static constexpr BlockType kBlockType = BlockType::kMetaIndex;
};

class Block_kUserDefinedIndex : public BlockContents {
 public:
  static constexpr CacheEntryRole kCacheEntryRole = CacheEntryRole::kIndexBlock;
  static constexpr BlockType kBlockType = BlockType::kUserDefinedIndex;

  explicit Block_kUserDefinedIndex(BlockContents&& other)
      : BlockContents(std::move(other)) {}
  const Slice& ContentSlice() const { return data; }
};

struct BlockCreateContext : public Cache::CreateContext {
  BlockCreateContext() {}
  BlockCreateContext(const BlockBasedTableOptions* _table_options,
                     const ImmutableOptions* _ioptions, Statistics* _statistics,
                     Decompressor* _decompressor,
                     uint8_t _protection_bytes_per_key,
                     const Comparator* _raw_ucmp, bool _index_value_is_full,
                     bool _index_has_first_key, bool _index_value_delta_escape,
                     uint32_t _data_block_restart_interval = 0,
                     uint32_t _index_block_restart_interval = 0)
      : table_options(_table_options),
        ioptions(_ioptions),
        statistics(_statistics),
        decompressor(_decompressor),
        raw_ucmp(_raw_ucmp),
        protection_bytes_per_key(_protection_bytes_per_key),
        index_value_is_full(_index_value_is_full),
        index_has_first_key(_index_has_first_key),
        index_value_delta_escape(_index_value_delta_escape),
        data_block_restart_interval(_data_block_restart_interval),
        index_block_restart_interval(_index_block_restart_interval) {}

  const BlockBasedTableOptions* table_options = nullptr;
  const ImmutableOptions* ioptions = nullptr;
  Statistics* statistics = nullptr;
  // TODO: refactor to avoid copying BlockCreateContext for dict in block cache
  Decompressor* decompressor = nullptr;
  const Comparator* raw_ucmp = nullptr;
  uint8_t protection_bytes_per_key = 0;
  bool index_value_is_full;
  bool index_has_first_key;
  // format_version >= 8 index value-delta escape codec (see IndexValue).
  bool index_value_delta_escape = false;
  // Whether index keys are internal keys (vs. user keys). Only used to rule
  // out index blocks whose last user key cannot be shorter than their common
  // prefix (see Block::InitializeIndexBlockCommonPrefixCheck()). true is safe
  // for correctness, but may cause false positives.
  bool index_key_includes_seq = true;
  // Whether to check whether the last user key of data and index blocks is
  // shorter than their common prefix (see
  // Block::last_user_key_shorter_than_common_prefix_). Set by
  // BlockBasedTable::Open() when reading a table with
  // ReverseBytewiseComparator, the only comparator using the prefix-optimized
  // seek that orders a user key after longer user keys that extend it. Blocks
  // of a table being built (e.g. for prepopulate_block_cache) never need it.
  bool check_common_prefix = false;
  // Restart intervals from table properties (0 if not available)
  uint32_t data_block_restart_interval = 0;
  uint32_t index_block_restart_interval = 0;

  // For TypedCacheInterface
  template <typename TBlocklike>
  inline void Create(std::unique_ptr<TBlocklike>* parsed_out,
                     size_t* charge_out, const Slice& data,
                     CompressionType type, MemoryAllocator* alloc) {
    BlockContents uncompressed_block_contents;
    if (type != CompressionType::kNoCompression) {
      assert(decompressor != nullptr);
      Status s =
          DecompressBlockData(data.data(), data.size(), type, *decompressor,
                              &uncompressed_block_contents, *ioptions, alloc);
      if (!s.ok()) {
        parsed_out->reset();
        return;
      }
    } else {
      uncompressed_block_contents =
          BlockContents(AllocateAndCopyBlock(data, alloc), data.size());
    }
    Create(parsed_out, std::move(uncompressed_block_contents));
    *charge_out = parsed_out->get()->ApproximateMemoryUsage();
  }

  void Create(std::unique_ptr<Block_kData>* parsed_out, BlockContents&& block);
  void Create(std::unique_ptr<Block_kIndex>* parsed_out, BlockContents&& block);
  void Create(std::unique_ptr<Block_kFilterPartitionIndex>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<Block_kRangeDeletion>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<Block_kMetaIndex>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<Block_kUserDefinedIndex>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<ParsedFullFilterBlock>* parsed_out,
              BlockContents&& block);
  void Create(std::unique_ptr<DecompressorDict>* parsed_out,
              BlockContents&& block);
};

// Convenient cache interface to use for block_cache, with support for
// SecondaryCache.
template <typename TBlocklike>
using BlockCacheInterface =
    FullTypedCacheInterface<TBlocklike, BlockCreateContext>;

// Shortcut name for cache handles under BlockCacheInterface
template <typename TBlocklike>
using BlockCacheTypedHandle =
    typename BlockCacheInterface<TBlocklike>::TypedHandle;

// Selects the right helper based on BlockType and CacheTier
const Cache::CacheItemHelper* GetCacheItemHelper(
    BlockType block_type,
    CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier);

// For SFINAE check that a type is "blocklike" with a kCacheEntryRole member.
// Can get difficult compiler/linker errors without a good check like this.
template <typename TUse, typename TBlocklike>
using WithBlocklikeCheck = std::enable_if_t<
    TBlocklike::kCacheEntryRole == CacheEntryRole::kMisc || true, TUse>;

template <typename TBlocklike>
using BlocklikeStatus = WithBlocklikeCheck<Status, TBlocklike>;

// Helper for the uncache_aggressiveness option
class UncacheAggressivenessAdvisor {
 public:
  UncacheAggressivenessAdvisor(uint32_t uncache_aggressiveness) {
    assert(uncache_aggressiveness > 0);
    allowance_ = std::min(uncache_aggressiveness, uint32_t{3});
    threshold_ = std::pow(0.99, uncache_aggressiveness - 1);
  }
  void Report(bool erased) { ++(erased ? useful_ : not_useful_); }
  bool ShouldContinue() {
    if (not_useful_ < allowance_) {
      return true;
    } else {
      // See UncacheAggressivenessAdvisor unit test
      return (useful_ + 1.0) / (useful_ + not_useful_ - allowance_ + 1.5) >=
             threshold_;
    }
  }

 private:
  // Baseline minimum number of "not useful" to consider stopping, to allow
  // sufficient evidence for checking the threshold. Actual minimum will be
  // higher as threshold gets well below 1.0.
  int allowance_;
  // After allowance, stop if useful ratio is below this threshold
  double threshold_;
  // Counts
  int useful_ = 0;
  int not_useful_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
