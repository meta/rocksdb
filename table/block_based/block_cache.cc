//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/block_cache.h"

#include "table/block_based/block_based_table_reader.h"

namespace ROCKSDB_NAMESPACE {

void BlockCreateContext::Create(std::unique_ptr<Block_kData>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new Block_kData(std::move(block),
                                    table_options->read_amp_bytes_per_bit,
                                    statistics, data_block_restart_interval));
  parsed_out->get()->InitializeDataBlockProtectionInfo(protection_bytes_per_key,
                                                       raw_ucmp);
  if (check_common_prefix) {
    parsed_out->get()->InitializeDataBlockCommonPrefixCheck();
  }
}
void BlockCreateContext::Create(std::unique_ptr<Block_kIndex>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new Block_kIndex(std::move(block),
                                     /*read_amp_bytes_per_bit*/ 0, statistics,
                                     index_block_restart_interval));
  parsed_out->get()->InitializeIndexBlockProtectionInfo(
      protection_bytes_per_key, raw_ucmp, index_value_is_full,
      index_has_first_key, index_value_delta_escape);
  if (check_common_prefix) {
    parsed_out->get()->InitializeIndexBlockCommonPrefixCheck(
        raw_ucmp, index_key_includes_seq, index_value_is_full,
        index_has_first_key, index_value_delta_escape);
  }
}
void BlockCreateContext::Create(
    std::unique_ptr<Block_kFilterPartitionIndex>* parsed_out,
    BlockContents&& block) {
  parsed_out->reset(new Block_kFilterPartitionIndex(
      std::move(block), /*read_amp_bytes_per_bit*/ 0, statistics,
      index_block_restart_interval));
  parsed_out->get()->InitializeIndexBlockProtectionInfo(
      protection_bytes_per_key, raw_ucmp, index_value_is_full,
      index_has_first_key, index_value_delta_escape);
  if (check_common_prefix) {
    // Same iterator parameters as
    // PartitionedFilterBlockReader::NewFilterPartitionIndexIterator().
    parsed_out->get()->InitializeIndexBlockCommonPrefixCheck(
        raw_ucmp, index_key_includes_seq, index_value_is_full,
        /*index_has_first_key=*/false, index_value_delta_escape);
  }
}
void BlockCreateContext::Create(
    std::unique_ptr<Block_kRangeDeletion>* parsed_out, BlockContents&& block) {
  parsed_out->reset(new Block_kRangeDeletion(
      std::move(block), /*read_amp_bytes_per_bit*/ 0, statistics));
}
void BlockCreateContext::Create(std::unique_ptr<Block_kMetaIndex>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new Block_kMetaIndex(
      std::move(block), /*read_amp_bytes_per_bit*/ 0, statistics));
  parsed_out->get()->InitializeMetaIndexBlockProtectionInfo(
      protection_bytes_per_key);
}

void BlockCreateContext::Create(
    std::unique_ptr<Block_kUserDefinedIndex>* parsed_out,
    BlockContents&& block) {
  parsed_out->reset(new Block_kUserDefinedIndex(std::move(block)));
}

void BlockCreateContext::Create(
    std::unique_ptr<ParsedFullFilterBlock>* parsed_out, BlockContents&& block) {
  parsed_out->reset(new ParsedFullFilterBlock(
      table_options->filter_policy.get(), std::move(block)));
}

void BlockCreateContext::Create(std::unique_ptr<DecompressorDict>* parsed_out,
                                BlockContents&& block) {
  parsed_out->reset(new DecompressorDict(
      block.data, std::move(block.allocation), *decompressor));
}

namespace {
// For getting SecondaryCache-compatible helpers from a BlockType. This is
// useful for accessing block cache in untyped contexts, such as for generic
// cache warming in table builder.
const std::array<const Cache::CacheItemHelper*,
                 static_cast<unsigned>(BlockType::kInvalid) + 1>
    kCacheItemFullHelperForBlockType{{
        BlockCacheInterface<Block_kData>::GetFullHelper(),
        BlockCacheInterface<ParsedFullFilterBlock>::GetFullHelper(),
        BlockCacheInterface<Block_kFilterPartitionIndex>::GetFullHelper(),
        nullptr,  // kProperties
        BlockCacheInterface<DecompressorDict>::GetFullHelper(),
        BlockCacheInterface<Block_kRangeDeletion>::GetFullHelper(),
        nullptr,  // kHashIndexPrefixes
        nullptr,  // kHashIndexMetadata
        nullptr,  // kMetaIndex (not yet stored in block cache)
        BlockCacheInterface<Block_kIndex>::GetFullHelper(),
        BlockCacheInterface<Block_kUserDefinedIndex>::GetFullHelper(),
        nullptr,  // kInvalid
    }};

// For getting basic helpers from a BlockType (no SecondaryCache support)
const std::array<const Cache::CacheItemHelper*,
                 static_cast<unsigned>(BlockType::kInvalid) + 1>
    kCacheItemBasicHelperForBlockType{{
        BlockCacheInterface<Block_kData>::GetBasicHelper(),
        BlockCacheInterface<ParsedFullFilterBlock>::GetBasicHelper(),
        BlockCacheInterface<Block_kFilterPartitionIndex>::GetBasicHelper(),
        nullptr,  // kProperties
        BlockCacheInterface<DecompressorDict>::GetBasicHelper(),
        BlockCacheInterface<Block_kRangeDeletion>::GetBasicHelper(),
        nullptr,  // kHashIndexPrefixes
        nullptr,  // kHashIndexMetadata
        nullptr,  // kMetaIndex (not yet stored in block cache)
        BlockCacheInterface<Block_kIndex>::GetBasicHelper(),
        BlockCacheInterface<Block_kUserDefinedIndex>::GetBasicHelper(),
        nullptr,  // kInvalid
    }};
}  // namespace

const Cache::CacheItemHelper* GetCacheItemHelper(
    BlockType block_type, CacheTier lowest_used_cache_tier) {
  if (lowest_used_cache_tier > CacheTier::kVolatileTier) {
    return kCacheItemFullHelperForBlockType[static_cast<unsigned>(block_type)];
  } else {
    return kCacheItemBasicHelperForBlockType[static_cast<unsigned>(block_type)];
  }
}

}  // namespace ROCKSDB_NAMESPACE
