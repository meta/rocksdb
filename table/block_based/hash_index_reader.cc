//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/hash_index_reader.h"

#include "table/block_fetcher.h"
#include "table/meta_blocks.h"

namespace ROCKSDB_NAMESPACE {
Status HashIndexReader::Create(const BlockBasedTable* table,
                               const ReadOptions& ro,
                               FilePrefetchBuffer* prefetch_buffer,
                               InternalIterator* meta_index_iter,
                               bool use_cache, bool prefetch, bool pin,
                               BlockCacheLookupContext* lookup_context,
                               std::unique_ptr<IndexReader>* index_reader) {
  assert(table != nullptr);
  assert(index_reader != nullptr);
  assert(!pin || prefetch);

  const BlockBasedTable::Rep* rep = table->get_rep();
  assert(rep != nullptr);

  CachableEntry<Block> index_block;
  if (prefetch || !use_cache) {
    const Status s =
        ReadIndexBlock(table, prefetch_buffer, ro, use_cache,
                       /*get_context=*/nullptr, lookup_context, &index_block);
    if (!s.ok()) {
      return s;
    }

    if (use_cache && !pin) {
      index_block.Reset();
    }
  }

  // Note, failure to create prefix hash index does not need to be a
  // hard error. We can still fall back to the original binary search index.
  // So, Create will succeed regardless, from this point on.

  index_reader->reset(new HashIndexReader(table, std::move(index_block)));

  // Get prefixes block
  BlockHandle prefixes_handle;
  Status s =
      FindMetaBlock(meta_index_iter, kHashIndexPrefixesBlock, &prefixes_handle);
  if (!s.ok()) {
    // TODO: log error
    return Status::OK();
  }

  // Get index metadata block
  BlockHandle prefixes_meta_handle;
  s = FindMetaBlock(meta_index_iter, kHashIndexPrefixesMetadataBlock,
                    &prefixes_meta_handle);
  if (!s.ok()) {
    // TODO: log error
    return Status::OK();
  }

  RandomAccessFileReader* const file = rep->file.get();
  const Footer& footer = rep->footer;
  const ImmutableOptions& ioptions = rep->ioptions;
  const PersistentCacheOptions& cache_options =
      rep->metadata_persistent_cache_options;
  MemoryAllocator* const memory_allocator =
      GetMemoryAllocator(rep->table_options);

  // Read contents for the blocks
  BlockContents prefixes_contents;
  BlockFetcher prefixes_block_fetcher(
      file, prefetch_buffer, footer, ro, prefixes_handle, &prefixes_contents,
      ioptions, true /*decompress*/, true /*maybe_compressed*/,
      BlockType::kHashIndexPrefixes, rep->decompressor.get(), cache_options,
      memory_allocator);
  s = prefixes_block_fetcher.ReadBlockContents();
  if (!s.ok()) {
    return s;
  }
  BlockContents prefixes_meta_contents;
  BlockFetcher prefixes_meta_block_fetcher(
      file, prefetch_buffer, footer, ro, prefixes_meta_handle,
      &prefixes_meta_contents, ioptions, true /*decompress*/,
      true /*maybe_compressed*/, BlockType::kHashIndexMetadata,
      rep->decompressor.get(), cache_options, memory_allocator);
  s = prefixes_meta_block_fetcher.ReadBlockContents();
  if (!s.ok()) {
    // TODO: log error
    return Status::OK();
  }

  BlockPrefixIndex* prefix_index = nullptr;
  assert(rep->table_prefix_extractor);
  s = BlockPrefixIndex::Create(rep->table_prefix_extractor.get(),
                               prefixes_contents.data,
                               prefixes_meta_contents.data, &prefix_index);
  // TODO: log error
  if (s.ok()) {
    HashIndexReader* const hash_index_reader =
        static_cast<HashIndexReader*>(index_reader->get());
    hash_index_reader->prefix_index_.reset(prefix_index);
  }

  return Status::OK();
}

InternalIteratorBase<IndexValue>* HashIndexReader::NewIterator(
    const ReadOptions& read_options, bool disable_prefix_seek,
    IndexBlockIter* iter, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
  const BlockBasedTable::Rep* rep = table()->get_rep();
  CachableEntry<Block> index_block;
  const Status s = GetOrReadIndexBlock(get_context, lookup_context,
                                       &index_block, read_options);
  if (!s.ok()) {
    if (iter != nullptr) {
      iter->Invalidate(s);
      return iter;
    }

    return NewErrorInternalIterator<IndexValue>(s);
  }

  Statistics* kNullStats = nullptr;
  const bool total_order_seek =
      read_options.total_order_seek || disable_prefix_seek;
  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  auto it = index_block.GetValue()->NewIndexIterator(
      internal_comparator()->user_comparator(),
      rep->get_global_seqno(BlockType::kIndex), iter, kNullStats,
      total_order_seek, index_has_first_key(), index_key_includes_seq(),
      index_value_is_full(), false /* block_contents_pinned */,
      user_defined_timestamps_persisted(), prefix_index_.get(),
      BlockBasedTableOptions::kBinary, index_value_delta_escape());

  assert(it != nullptr);
  index_block.TransferTo(it);

  return it;
}
}  // namespace ROCKSDB_NAMESPACE
