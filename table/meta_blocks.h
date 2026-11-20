//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/table_properties_collector.h"
#include "rocksdb/comparator.h"
#include "rocksdb/memory_allocator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/block_type.h"
#include "table/format.h"
#include "util/kv_map.h"

namespace ROCKSDB_NAMESPACE {

class Block;
class BlockBuilder;
class BlockHandle;
class Env;
class Footer;
class Logger;
class RandomAccessFile;
struct TableProperties;

// Meta block names for metaindex
extern const std::string kPropertiesBlockName;
extern const std::string kIndexBlockName;
extern const std::string kCompressionDictBlockName;
extern const std::string kRangeDelBlockName;

class MetaIndexBuilder {
 public:
  MetaIndexBuilder(const MetaIndexBuilder&) = delete;
  MetaIndexBuilder& operator=(const MetaIndexBuilder&) = delete;
  MetaIndexBuilder(MetaIndexBuilder&&) = delete;
  MetaIndexBuilder& operator=(MetaIndexBuilder&&) = delete;
  ~MetaIndexBuilder() = default;

  MetaIndexBuilder();
  void Add(const std::string& key, const BlockHandle& handle);

  // Write all the added key/value pairs to the block and return the contents
  // of the block.
  Slice Finish();

 private:
  // store the sorted key/handle of the metablocks.
  stl_wrappers::KVMap meta_block_handles_;
  std::unique_ptr<BlockBuilder> meta_index_block_;
};

class PropertyBlockBuilder {
 public:
  PropertyBlockBuilder(const PropertyBlockBuilder&) = delete;
  PropertyBlockBuilder& operator=(const PropertyBlockBuilder&) = delete;
  PropertyBlockBuilder(PropertyBlockBuilder&&) = delete;
  PropertyBlockBuilder& operator=(PropertyBlockBuilder&&) = delete;
  ~PropertyBlockBuilder() = default;

  PropertyBlockBuilder();

  void AddTableProperty(const TableProperties& props);
  void Add(const std::string& key, uint64_t value);
  void Add(const std::string& key, const std::string& value);
  void Add(const UserCollectedProperties& user_collected_properties);

  // Write all the added entries to the block and return the block contents
  Slice Finish();

 private:
  std::unique_ptr<BlockBuilder> properties_block_;
  stl_wrappers::KVMap props_;
#ifndef NDEBUG
  const Comparator* comparator_ = BytewiseComparator();
  Slice last_prop_added_to_block_;
#endif /* !NDEBUG */
};

// Were we encounter any error occurs during user-defined statistics collection,
// we'll write the warning message to info log.
void LogPropertiesCollectionError(Logger* info_log, const std::string& method,
                                  const std::string& name);

// Utility functions help table builder to trigger batch events for user
// defined property collectors.
// Return value indicates if there is any error occurred; if error occurred,
// the warning message will be logged.
// NotifyCollectTableCollectorsOnAdd() triggers the `Add` event for all
// property collectors.
bool NotifyCollectTableCollectorsOnAdd(
    const Slice& key, const Slice& value, uint64_t file_size,
    const std::vector<std::unique_ptr<InternalTblPropColl>>& collectors,
    Logger* info_log);

void NotifyCollectTableCollectorsOnBlockAdd(
    const std::vector<std::unique_ptr<InternalTblPropColl>>& collectors,
    uint64_t block_uncomp_bytes, uint64_t block_compressed_bytes_fast,
    uint64_t block_compressed_bytes_slow);

// NotifyCollectTableCollectorsOnFinish() triggers the `Finish` event for all
// property collectors. The collected properties will be added to `builder`.
// It will also populate `user_collected_properties` and `readable_properties`
// with the collected properties.
bool NotifyCollectTableCollectorsOnFinish(
    const std::vector<std::unique_ptr<InternalTblPropColl>>& collectors,
    Logger* info_log, PropertyBlockBuilder* builder,
    UserCollectedProperties& user_collected_properties,
    UserCollectedProperties& readable_properties);

// The optional offset is relative to the start of the properties block.
Status ParsePropertiesBlock(
    const ImmutableOptions& ioptions, Block& block,
    std::unique_ptr<TableProperties>& new_table_properties,
    uint32_t* global_seqno_value_offset);

// Sets global_seqno to the sequence number that should replace per-entry
// sequence numbers while reading this file. When largest_seqno is
// kDisableGlobalSequenceNumber, leaves the stored sequence numbers unchanged.
Status GetGlobalSequenceNumber(const TableProperties& table_properties,
                               SequenceNumber largest_seqno,
                               SequenceNumber* global_seqno);

// Read table properties from a file using known BlockHandle.
// @returns a status to indicate if the operation succeeded. On success,
//          *table_properties will point to a heap-allocated TableProperties
//          object, otherwise value of `table_properties` will not be modified.
Status ReadTablePropertiesHelper(
    const ReadOptions& ro, const BlockHandle& handle,
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ImmutableOptions& ioptions,
    std::unique_ptr<TableProperties>* table_properties,
    MemoryAllocator* memory_allocator = nullptr);

// Read table properties from the properties block of a plain table.
// @returns a status to indicate if the operation succeeded. On success,
//          *table_properties will point to a heap-allocated TableProperties
//          object, otherwise value of `table_properties` will not be modified.
Status ReadTableProperties(RandomAccessFileReader* file, uint64_t file_size,
                           uint64_t table_magic_number,
                           const ImmutableOptions& ioptions,
                           const ReadOptions& read_options,
                           std::unique_ptr<TableProperties>* properties,
                           MemoryAllocator* memory_allocator = nullptr,
                           FilePrefetchBuffer* prefetch_buffer = nullptr);

// Find the meta block from the meta index block. Returns OK and
// block_handle->IsNull() if not found.
Status FindOptionalMetaBlock(InternalIterator* meta_index_iter,
                             const std::string& meta_block_name,
                             BlockHandle* block_handle);

// Find the meta block from the meta index block. Returns Corruption if not
// found.
Status FindMetaBlock(InternalIterator* meta_index_iter,
                     const std::string& meta_block_name,
                     BlockHandle* block_handle);

// Find the meta block
Status FindMetaBlockInFile(RandomAccessFileReader* file, uint64_t file_size,
                           uint64_t table_magic_number,
                           const ImmutableOptions& ioptions,
                           const ReadOptions& read_options,
                           const std::string& meta_block_name,
                           BlockHandle* block_handle,
                           MemoryAllocator* memory_allocator = nullptr,
                           FilePrefetchBuffer* prefetch_buffer = nullptr,
                           Footer* footer_out = nullptr);

// Read meta block contents
Status ReadMetaIndexBlockInFile(RandomAccessFileReader* file,
                                uint64_t file_size, uint64_t table_magic_number,
                                const ImmutableOptions& ioptions,
                                const ReadOptions& read_options,
                                BlockContents* block_contents,
                                MemoryAllocator* memory_allocator = nullptr,
                                FilePrefetchBuffer* prefetch_buffer = nullptr,
                                Footer* footer_out = nullptr);

// Read the specified meta block with name meta_block_name
// from `file` and initialize `contents` with contents of this block.
// Return Status::OK in case of success.
Status ReadMetaBlock(RandomAccessFileReader* file,
                     FilePrefetchBuffer* prefetch_buffer, uint64_t file_size,
                     uint64_t table_magic_number,
                     const ImmutableOptions& ioptions,
                     const ReadOptions& read_options,
                     const std::string& meta_block_name, BlockType block_type,
                     BlockContents* contents,
                     MemoryAllocator* memory_allocator = nullptr);

}  // namespace ROCKSDB_NAMESPACE
