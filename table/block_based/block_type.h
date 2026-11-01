//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Represents the types of blocks used in the block based table format.
// See https://github.com/meta/rocksdb/wiki/Rocksdb-BlockBasedTable-Format
// for details.
// For code sanity, BlockType should imply a specific TBlocklike for
// BlocklikeTraits.
enum class BlockType : uint8_t {
  kData,
  kFilter,  // for second level partitioned filters and full filters
  kFilterPartitionIndex,  // for top-level index of filter partitions
  kProperties,
  kCompressionDictionary,
  kRangeDeletion,
  kHashIndexPrefixes,
  kHashIndexMetadata,
  kMetaIndex,
  kIndex,
  kUserDefinedIndex,
  // Note: keep kInvalid the last value when adding new enum values.
  kInvalid
};

inline const char* BlockTypeToString(BlockType block_type) {
  switch (block_type) {
    case BlockType::kData:
      return "Data";
    case BlockType::kFilter:
      return "Filter";
    case BlockType::kFilterPartitionIndex:
      return "FilterPartitionIndex";
    case BlockType::kProperties:
      return "Properties";
    case BlockType::kCompressionDictionary:
      return "CompressionDictionary";
    case BlockType::kRangeDeletion:
      return "RangeDeletion";
    case BlockType::kHashIndexPrefixes:
      return "HashIndexPrefixes";
    case BlockType::kHashIndexMetadata:
      return "HashIndexMetadata";
    case BlockType::kMetaIndex:
      return "MetaIndex";
    case BlockType::kIndex:
      return "Index";
    case BlockType::kUserDefinedIndex:
      return "UserDefinedIndex";
    case BlockType::kInvalid:
      return "Invalid";
  }
  return "Unknown";
}

}  // namespace ROCKSDB_NAMESPACE
