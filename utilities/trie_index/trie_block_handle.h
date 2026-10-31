//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// Offset and size of a data block in the SST file. Matches
// IndexFactoryBuilder::BlockHandle but defined locally to avoid header
// dependencies in the core trie implementations.
struct TrieBlockHandle {
  uint64_t offset = 0;
  uint64_t size = 0;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
