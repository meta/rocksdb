//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <optional>
#include <type_traits>

#include "util/aligned_buffer.h"
#include "util/async_file_reader.h"
#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param uncompression_dict Data for presetting the compression library's
//    dictionary.
template <typename TBlocklike>
DEFINE_SYNC_AND_ASYNC(Status, ReadAndParseBlockFromFile)(
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ReadOptions& options, const BlockHandle& handle,
    std::unique_ptr<TBlocklike>* result, const ImmutableOptions& ioptions,
    BlockCreateContext& create_context, bool maybe_compressed,
    UnownedPtr<Decompressor> decomp,
    const PersistentCacheOptions& cache_options,
    MemoryAllocator* memory_allocator, bool for_compaction, bool async_read,
    ReadScopedBlockBufferProviderRef block_buffer_provider = std::nullopt) {
  assert(result);

  BlockContents contents;
  BlockFetcher block_fetcher(
      file, prefetch_buffer, footer, options, handle, &contents, ioptions,
      /*do_uncompress*/ maybe_compressed, maybe_compressed,
      TBlocklike::kBlockType, decomp, cache_options, memory_allocator, nullptr,
      for_compaction, block_buffer_provider);
  Status s;
  // If prefetch_buffer is not allocated, it will fallback to synchronous
  // reading of block contents.
  if (async_read && prefetch_buffer != nullptr) {
    s = block_fetcher.ReadAsyncBlockContents();
    if (!s.ok()) {
      CO_RETURN s;
    }
  } else {
#if defined(WITH_COROUTINES)
    s = co_await folly::coro::co_nothrow(
        block_fetcher.ReadBlockContentsCoroutine());
#else
    s = block_fetcher.ReadBlockContents();
#endif
  }
  if (s.ok()) {
    create_context.Create(result, std::move(contents));
  }
  CO_RETURN s;
}

// If contents is nullptr, this function looks up the block caches for the
// data block referenced by handle, and read the block from disk if necessary.
// If contents is non-null, it skips the cache lookup and disk read, since
// the caller has already read it. In both cases, if ro.fill_cache is true,
// it inserts the block into the block cache.
template <typename TBlocklike>
DEFINE_SYNC_AND_ASYNC(BlocklikeStatus<TBlocklike>,
                      BlockBasedTable::MaybeReadBlockAndLoadToCache)(
    FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
    const BlockHandle& handle, UnownedPtr<Decompressor> decomp,
    bool for_compaction, CachableEntry<TBlocklike>* out_parsed_block,
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    BlockContents* contents, bool async_read,
    bool use_block_cache_for_lookup) const {
  assert(out_parsed_block != nullptr);
  const bool no_io = (ro.read_tier == kBlockCacheTier);
  BlockCacheInterface<TBlocklike> block_cache{
      rep_->table_options.block_cache.get()};
  // First, try to get the block from the cache
  //
  // If either block cache is enabled, we'll try to read from it.
  Status s;
  CacheKey key_data;
  Slice key;
  bool is_cache_hit = false;
  if (block_cache) {
    // create key for block cache
    key_data = GetCacheKey(rep_->base_cache_key, handle);
    key = key_data.AsSlice();

    if (!contents) {
      if (use_block_cache_for_lookup) {
        s = GetDataBlockFromCache(key, block_cache, out_parsed_block,
                                  get_context, decomp);
        // Value could still be null at this point, so check the cache handle
        // and update the read pattern for prefetching
        if (out_parsed_block->GetValue() ||
            out_parsed_block->GetCacheHandle()) {
          // TODO(haoyu): Differentiate cache hit on uncompressed block cache
          // and compressed block cache.
          is_cache_hit = true;
          if (prefetch_buffer) {
            // Update the block details so that PrefetchBuffer can use the read
            // pattern to determine if reads are sequential or not for
            // prefetching. It should also take in account blocks read from
            // cache.
            prefetch_buffer->UpdateReadPattern(
                handle.offset(), BlockSizeWithTrailer(handle),
                ro.adaptive_readahead /*decrease_readahead_size*/);
          }
        }
      }
    }

    // Can't find the block from the cache. If I/O is allowed, read from the
    // file.
    if (out_parsed_block->GetValue() == nullptr &&
        out_parsed_block->GetCacheHandle() == nullptr && !no_io &&
        ro.fill_cache) {
      Statistics* statistics = rep_->ioptions.stats;
      const bool maybe_compressed =
          BlockTypeMaybeCompressed(TBlocklike::kBlockType) &&
          rep_->decompressor;
      // This flag, if true, tells BlockFetcher to return the uncompressed
      // block when ReadBlockContents() is called.
      const bool do_uncompress = maybe_compressed;
      CompressionType contents_comp_type;
      // Maybe serialized or uncompressed
      BlockContents tmp_contents;
      BlockContents uncomp_contents;
      BlockContents comp_contents;
      if (!contents) {
        Histograms histogram = for_compaction ? READ_BLOCK_COMPACTION_MICROS
                                              : READ_BLOCK_GET_MICROS;
        StopWatch sw(rep_->ioptions.clock, statistics, histogram);
        // Setting do_uncompress to false may cause an extra mempcy in the
        // following cases -
        // 1. Compression is enabled, but block is not actually compressed
        // 2. Compressed block is in the prefetch buffer
        // 3. Direct IO
        //
        // It would also cause a memory allocation to be used rather than
        // stack if the compressed block size is < 5KB
        BlockFetcher block_fetcher(
            rep_->file.get(), prefetch_buffer, rep_->footer, ro, handle,
            &tmp_contents, rep_->ioptions, do_uncompress, maybe_compressed,
            TBlocklike::kBlockType, decomp, rep_->persistent_cache_options,
            GetMemoryAllocator(rep_->table_options),
            /*allocator=*/nullptr);

        // If prefetch_buffer is not allocated, it will fallback to synchronous
        // reading of block contents.
        if (async_read && prefetch_buffer != nullptr) {
          s = block_fetcher.ReadAsyncBlockContents();
          if (!s.ok()) {
            CO_RETURN s;
          }
        } else {
#if defined(WITH_COROUTINES)
          s = co_await folly::coro::co_nothrow(
              block_fetcher.ReadBlockContentsCoroutine());
#else
          s = block_fetcher.ReadBlockContents();
#endif
        }

        contents_comp_type = block_fetcher.compression_type();
        if (get_context) {
          switch (TBlocklike::kBlockType) {
            case BlockType::kIndex:
            case BlockType::kUserDefinedIndex:
              ++get_context->get_context_stats_.num_index_read;
              break;
            case BlockType::kFilter:
            case BlockType::kFilterPartitionIndex:
              ++get_context->get_context_stats_.num_filter_read;
              break;
            default:
              break;
          }
        }
        if (s.ok()) {
          if (do_uncompress && contents_comp_type != kNoCompression) {
            comp_contents = BlockContents(block_fetcher.GetCompressedBlock());
            uncomp_contents = std::move(tmp_contents);
          } else if (contents_comp_type != kNoCompression) {
            // do_uncompress must be false, so output of BlockFetcher is
            // compressed
            comp_contents = std::move(tmp_contents);
          } else {
            uncomp_contents = std::move(tmp_contents);
          }

          // If filling cache is allowed and a cache is configured, try to put
          // the block to the cache. Do this here while block_fetcher is in
          // scope, since comp_contents will be a reference to the compressed
          // block in block_fetcher
          s = PutDataBlockToCache(
              key, block_cache, out_parsed_block, std::move(uncomp_contents),
              std::move(comp_contents), contents_comp_type, decomp,
              GetMemoryAllocator(rep_->table_options), get_context);
        }
      } else {
        contents_comp_type = GetBlockCompressionType(*contents);
        if (contents_comp_type != kNoCompression) {
          comp_contents = std::move(*contents);
        } else {
          uncomp_contents = std::move(*contents);
        }

        if (s.ok()) {
          // If filling cache is allowed and a cache is configured, try to put
          // the block to the cache.
          s = PutDataBlockToCache(
              key, block_cache, out_parsed_block, std::move(uncomp_contents),
              std::move(comp_contents), contents_comp_type, decomp,
              GetMemoryAllocator(rep_->table_options), get_context);
        }
      }
    }
  }

  // TODO: optimize so that lookup_context != nullptr implies the others
  if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled() &&
      lookup_context) {
    SaveLookupContextOrTraceRecord(
        key, is_cache_hit, ro, out_parsed_block->GetValue(), lookup_context);
  }

  assert(s.ok() || out_parsed_block->GetValue() == nullptr);
  CO_RETURN s;
}

template <typename TBlocklike /*, auto*/>
DEFINE_SYNC_AND_ASYNC(BlocklikeStatus<TBlocklike>,
                      BlockBasedTable::RetrieveBlock)(
    FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
    const BlockHandle& handle, UnownedPtr<Decompressor> decomp,
    CachableEntry<TBlocklike>* out_parsed_block, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, bool for_compaction,
    bool use_cache, bool async_read, bool use_block_cache_for_lookup) const {
  assert(out_parsed_block);
  assert(out_parsed_block->IsEmpty());

  if (use_cache) {
    Status s = CO_AWAIT(
        MaybeReadBlockAndLoadToCache, prefetch_buffer, ro, handle, decomp,
        for_compaction, out_parsed_block, get_context, lookup_context,
        /*contents=*/nullptr, async_read, use_block_cache_for_lookup);

    if (!s.ok()) {
      CO_RETURN s;
    }

    if (out_parsed_block->GetValue() != nullptr ||
        out_parsed_block->GetCacheHandle() != nullptr) {
      assert(s.ok());
      CO_RETURN s;
    }
  }

  assert(out_parsed_block->IsEmpty());

  const bool no_io = ro.read_tier == kBlockCacheTier;
  if (no_io) {
    CO_RETURN Status::Incomplete("no blocking io");
  }

  const bool maybe_compressed =
      BlockTypeMaybeCompressed(TBlocklike::kBlockType) && rep_->decompressor;
  std::unique_ptr<TBlocklike> block;
  Status s;

  {
    Histograms histogram =
        for_compaction ? READ_BLOCK_COMPACTION_MICROS : READ_BLOCK_GET_MICROS;
    StopWatch sw(rep_->ioptions.clock, rep_->ioptions.stats, histogram);
    ReadScopedBlockBufferProviderRef block_buffer_provider =
        (!use_cache && TBlocklike::kBlockType == BlockType::kData)
            ? GetReadScopedBlockBufferProvider(ro,
                                               rep_->ioptions.allow_mmap_reads)
            : std::nullopt;
    s = CO_AWAIT(ReadAndParseBlockFromFile, rep_->file.get(), prefetch_buffer,
                 rep_->footer, ro, handle, &block, rep_->ioptions,
                 rep_->create_context, maybe_compressed, decomp,
                 rep_->persistent_cache_options,
                 GetMemoryAllocator(rep_->table_options), for_compaction,
                 async_read, block_buffer_provider);

    if (get_context) {
      switch (TBlocklike::kBlockType) {
        case BlockType::kIndex:
        case BlockType::kUserDefinedIndex:
          ++(get_context->get_context_stats_.num_index_read);
          break;
        case BlockType::kFilter:
        case BlockType::kFilterPartitionIndex:
          ++(get_context->get_context_stats_.num_filter_read);
          break;
        default:
          break;
      }
    }
  }

  if (!s.ok()) {
    CO_RETURN s;
  }

  out_parsed_block->SetOwnedValue(std::move(block));

  assert(s.ok());
  CO_RETURN s;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
template <typename TBlockIter>
DEFINE_SYNC_AND_ASYNC(TBlockIter*, BlockBasedTable::NewDataBlockIterator)(
    const ReadOptions& ro, const BlockHandle& handle, TBlockIter* input_iter,
    BlockType block_type, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    FilePrefetchBuffer* prefetch_buffer, bool for_compaction, bool async_read,
    Status& s, bool use_block_cache_for_lookup) const {
  static_assert(std::is_same_v<TBlockIter, DataBlockIter> ||
                std::is_same_v<TBlockIter, IndexBlockIter>);
  using IterBlocklike =
      std::conditional_t<std::is_same_v<TBlockIter, DataBlockIter>, Block_kData,
                         Block_kIndex>;
  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  TBlockIter* iter = input_iter != nullptr ? input_iter : new TBlockIter;
  if (!s.ok()) {
    iter->Invalidate(s);
    CO_RETURN iter;
  }

  CachableEntry<Block> block;
  {
    CachableEntry<DecompressorDict> dict;
    Decompressor* decomp = rep_->decompressor.get();
    if (rep_->uncompression_dict_reader && block_type == BlockType::kData) {
      // For async scans, don't use the prefetch buffer since an async prefetch
      // might already be under way and this would invalidate it. Also, the
      // uncompression dict is typically at the end of the file and would
      // most likely break the sequentiality of the access pattern.
      // Same is with auto_readahead_size. It iterates over index to lookup for
      // data blocks. And this could break the the sequentiality of the access
      // pattern.
      s = rep_->uncompression_dict_reader->GetOrReadUncompressionDictionary(
          ((ro.async_io || ro.auto_readahead_size) ? nullptr : prefetch_buffer),
          ro, get_context, lookup_context, &dict);
      if (!s.ok()) {
        iter->Invalidate(s);
        CO_RETURN iter;
      }
      assert(dict.GetValue());
      if (dict.GetValue()) {
        decomp = dict.GetValue()->decompressor_.get();
      }
    }

    if (block_type == BlockType::kRangeDeletion) {
      s = CO_AWAIT(RetrieveBlock, prefetch_buffer, ro, handle, decomp,
                   &block.As<Block_kRangeDeletion>(), get_context,
                   lookup_context, for_compaction, /* use_cache */ true,
                   async_read, use_block_cache_for_lookup);
    } else {
      const bool use_cache =
          block_type != BlockType::kData ||
          ShouldUseDataBlockCacheForIterator(rep_->table_options, ro,
                                             rep_->ioptions.allow_mmap_reads);
      s = CO_AWAIT(RetrieveBlock, prefetch_buffer, ro, handle, decomp,
                   &block.As<IterBlocklike>(), get_context, lookup_context,
                   for_compaction, use_cache, async_read,
                   use_block_cache_for_lookup && use_cache);
    }
  }

  if (s.IsTryAgain() && async_read) {
    CO_RETURN iter;
  }

  if (!s.ok()) {
    assert(block.IsEmpty());
    iter->Invalidate(s);
    CO_RETURN iter;
  }

  assert(block.GetValue() != nullptr);
  assert(block_type != BlockType::kData ||
         block.GetValue()->HasSeparatedKV() ==
             rep_->separate_key_value_in_data_block);

  // Block contents are pinned and it is still pinned after the iterator
  // is destroyed as long as cleanup functions are moved to another object,
  // when:
  // 1. block cache handle is set to be released in cleanup function, or
  // 2. it's pointing to immortal source. If own_bytes is true then we are
  //    not reading data from the original source, whether immortal or not.
  //    Otherwise, the block is pinned iff the source is immortal.
  // Relied upon by BlockBasedTableIterator::IsDataBlockFromImmortalSource().
  const bool block_contents_pinned =
      block.IsCached() ||
      (!block.GetValue()->own_bytes() && rep_->immortal_table);
  iter = InitBlockIterator<TBlockIter>(rep_, block.GetValue(), block_type, iter,
                                       block_contents_pinned);

  if (!block.IsCached()) {
    if (!ro.fill_cache) {
      using IterPlaceholderCacheInterface =
          PlaceholderCacheInterface<CacheEntryRole::kMisc>;
      IterPlaceholderCacheInterface block_cache{
          rep_->table_options.block_cache.get()};
      if (block_cache) {
        // insert a dummy record to block cache to track the memory usage
        Cache::Handle* cache_handle = nullptr;
        CacheKey key =
            CacheKey::CreateUniqueForCacheLifetime(block_cache.get());
        s = block_cache.Insert(key.AsSlice(),
                               block.GetValue()->ApproximateMemoryUsage(),
                               &cache_handle);

        if (s.ok()) {
          assert(cache_handle != nullptr);
          iter->RegisterCleanup(&ForceReleaseCachedEntry, block_cache.get(),
                                cache_handle);
        }
      }
    }
  } else {
    iter->SetCacheHandle(block.GetCacheHandle());
  }

  block.TransferTo(iter);

  CO_RETURN iter;
}

// Convert an uncompressed data block (i.e CachableEntry<Block>)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
template <typename TBlockIter>
DEFINE_SYNC_AND_ASYNC(TBlockIter*, BlockBasedTable::NewDataBlockIterator)(
    const ReadOptions& ro, CachableEntry<Block>& block, TBlockIter* input_iter,
    Status s) const {
  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  TBlockIter* iter = input_iter != nullptr ? input_iter : new TBlockIter;
  if (!s.ok()) {
    iter->Invalidate(s);
    CO_RETURN iter;
  }

  assert(block.GetValue() != nullptr);
  assert(block.GetValue()->HasSeparatedKV() ==
         rep_->separate_key_value_in_data_block);
  // Block contents are pinned and it is still pinned after the iterator
  // is destroyed as long as cleanup functions are moved to another object,
  // when:
  // 1. block cache handle is set to be released in cleanup function, or
  // 2. it's pointing to immortal source. If own_bytes is true then we are
  //    not reading data from the original source, whether immortal or not.
  //    Otherwise, the block is pinned iff the source is immortal.
  // Relied upon by BlockBasedTableIterator::IsDataBlockFromImmortalSource().
  const bool block_contents_pinned =
      block.IsCached() ||
      (!block.GetValue()->own_bytes() && rep_->immortal_table);
  iter = InitBlockIterator<TBlockIter>(rep_, block.GetValue(), BlockType::kData,
                                       iter, block_contents_pinned);

  if (!block.IsCached()) {
    if (!ro.fill_cache) {
      using IterPlaceholderCacheInterface =
          PlaceholderCacheInterface<CacheEntryRole::kMisc>;
      IterPlaceholderCacheInterface block_cache{
          rep_->table_options.block_cache.get()};
      if (block_cache) {
        // insert a dummy record to block cache to track the memory usage
        Cache::Handle* cache_handle = nullptr;
        CacheKey key =
            CacheKey::CreateUniqueForCacheLifetime(block_cache.get());
        s = block_cache.Insert(key.AsSlice(),
                               block.GetValue()->ApproximateMemoryUsage(),
                               &cache_handle);

        if (s.ok()) {
          assert(cache_handle != nullptr);
          iter->RegisterCleanup(&ForceReleaseCachedEntry, block_cache.get(),
                                cache_handle);
        }
      }
    }
  } else {
    iter->SetCacheHandle(block.GetCacheHandle());
  }

  block.TransferTo(iter);
  CO_RETURN iter;
}

DEFINE_SYNC_AND_ASYNC(Status, BlockBasedTable::Get)
(const ReadOptions& read_options, const Slice& key, GetContext* get_context,
 const SliceTransform* prefix_extractor, bool skip_filters) {
  // Similar to Bloom filter !may_match
  // If timestamp is beyond the range of the table, skip
  if (!TimestampMayMatch(read_options)) {
    CO_RETURN Status::OK();
  }
  assert(key.size() >= 8);  // key must be internal key
  assert(get_context != nullptr);
  Status s;

  FilterBlockReader* const filter =
      !skip_filters ? rep_->filter.get() : nullptr;

  // First check the full filter
  // If full filter not useful, Then go into each block
  uint64_t tracing_get_id = get_context->get_tracing_get_id();
  BlockCacheLookupContext lookup_context{
      TableReaderCaller::kUserGet, tracing_get_id,
      /*get_from_user_specified_snapshot=*/read_options.snapshot != nullptr};
  if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled()) {
    // Trace the key since it contains both user key and sequence number.
    lookup_context.referenced_key = key.ToString();
    lookup_context.get_from_user_specified_snapshot =
        read_options.snapshot != nullptr;
  }
  TEST_SYNC_POINT("BlockBasedTable::Get:BeforeFilterMatch");
  const bool may_match =
      FullFilterKeyMayMatch(filter, key, prefix_extractor, get_context,
                            &lookup_context, read_options);
  TEST_SYNC_POINT("BlockBasedTable::Get:AfterFilterMatch");
  if (may_match) {
    IndexBlockIter iiter_on_stack;
    // if prefix_extractor found in block differs from options, disable
    // BlockPrefixIndex. Only do this check when index_type is kHashSearch.
    bool need_upper_bound_check = false;
    if (rep_->index_type == BlockBasedTableOptions::kHashSearch) {
      need_upper_bound_check = PrefixExtractorChanged(prefix_extractor);
    }
    auto iiter =
        NewIndexIterator(read_options, need_upper_bound_check, &iiter_on_stack,
                         get_context, &lookup_context);
    std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    size_t ts_sz =
        rep_->internal_comparator.user_comparator()->timestamp_size();
    bool matched = false;  // if such user key matched a key in SST
    bool done = false;
    // Embedded blob resolution is gated on this table actually having embedded
    // blob records, so the common (non-embedded) Get path is unchanged. The
    // scratch (reused across entries) is only constructed for embedded tables
    // and avoids per-entry allocation.
    std::optional<EmbeddedValueGetScratch> embedded_scratch;
    if (rep_->has_embedded_blobs) {
      embedded_scratch.emplace();
    }
    // Defer same-file wide-column blob resolution to GetContext (zero-copy)
    // unless a row-cache replay log is being built: the replay log (and thus
    // the row cache) must hold a fully resolved entity, since a later row-cache
    // hit never opens this table and so cannot resolve same-file references.
    const bool defer_embedded_wide_columns =
        embedded_scratch.has_value() && !get_context->HasReplayLog();
    for (iiter->Seek(key); iiter->Valid() && !done; iiter->Next()) {
      IndexValue v = iiter->value();

      if (!v.first_internal_key.empty() && !skip_filters &&
          UserComparatorWrapper(rep_->internal_comparator.user_comparator())
                  .CompareWithoutTimestamp(
                      ExtractUserKey(key),
                      ExtractUserKey(v.first_internal_key)) < 0) {
        // The requested key falls between highest key in previous block and
        // lowest key in current block.
        break;
      }

      BlockCacheLookupContext lookup_data_block_context{
          TableReaderCaller::kUserGet, tracing_get_id,
          /*get_from_user_specified_snapshot=*/read_options.snapshot !=
              nullptr};
      bool does_referenced_key_exist = false;
      DataBlockIter biter;
      uint64_t referenced_data_size = 0;
      Status tmp_status;
      CO_AWAIT(NewDataBlockIterator, read_options, v.handle, &biter,
               BlockType::kData, get_context, &lookup_data_block_context,
               /*prefetch_buffer=*/nullptr, /*for_compaction=*/false,
               /*async_read=*/false, tmp_status,
               /*use_block_cache_for_lookup=*/true);

      if (read_options.read_tier == kBlockCacheTier &&
          biter.status().IsIncomplete()) {
        // couldn't get block from block_cache
        // Update Saver.state to Found because we are only looking for
        // whether we can guarantee the key is not there when "no_io" is set
        get_context->MarkKeyMayExist();
        s = biter.status();
        break;
      }
      if (!biter.status().ok()) {
        s = biter.status();
        break;
      }

      bool may_exist = biter.SeekForGet(key);
      // If user-specified timestamp is supported, we cannot end the search
      // just because hash index lookup indicates the key+ts does not exist.
      if (!may_exist && ts_sz == 0) {
        // HashSeek cannot find the key this block and the the iter is not
        // the end of the block, i.e. cannot be in the following blocks
        // either. In this case, the seek_key cannot be found, so we break
        // from the top level for-loop.
        done = true;
      } else {
        // Call the *saver function on each entry/block until it returns false
        for (; biter.Valid(); biter.Next()) {
          Slice key_to_save = biter.key();
          Slice value_to_save = biter.value();
          Cleanable* value_pinner = biter.IsValuePinned() ? &biter : nullptr;
          if (embedded_scratch) {
            s = ResolveEmbeddedValueForGet(
                read_options, biter.key(), biter.value(), &*embedded_scratch,
                value_pinner, &key_to_save, &value_to_save, &value_pinner,
                defer_embedded_wide_columns);
            if (!s.ok()) {
              break;
            }
          }

          ParsedInternalKey parsed_key;
          Status pik_status = ParseInternalKey(
              key_to_save, &parsed_key, false /* log_err_key */);  // TODO
          if (!pik_status.ok()) {
            s = pik_status;
            break;
          }

          Status read_status;
          bool ret = get_context->SaveValue(
              parsed_key, value_to_save, &matched, &read_status, value_pinner,
              defer_embedded_wide_columns ? this : nullptr);
          if (!read_status.ok()) {
            s = read_status;
            break;
          }
          if (!ret) {
            if (get_context->State() == GetContext::GetState::kFound) {
              does_referenced_key_exist = true;
              referenced_data_size = biter.key().size() + biter.value().size();
            }
            done = true;
            break;
          }
        }
        if (s.ok()) {
          s = biter.status();
        }
        if (!s.ok()) {
          break;
        }
      }
      // Write the block cache access record.
      if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled()) {
        // Avoid making copy of block_key, cf_name, and referenced_key when
        // constructing the access record.
        Slice referenced_key;
        if (does_referenced_key_exist) {
          referenced_key = biter.key();
        } else {
          referenced_key = key;
        }
        FinishTraceRecord(lookup_data_block_context,
                          lookup_data_block_context.block_key, referenced_key,
                          does_referenced_key_exist, referenced_data_size);
      }

      if (done) {
        // Avoid the extra Next which is expensive in two-level indexes
        break;
      }
    }
    if (matched && filter != nullptr) {
      if (rep_->whole_key_filtering) {
        RecordTick(rep_->ioptions.stats, BLOOM_FILTER_FULL_TRUE_POSITIVE);
      } else {
        RecordTick(rep_->ioptions.stats, BLOOM_FILTER_PREFIX_TRUE_POSITIVE);
      }
      // Includes prefix stats
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_true_positive, 1,
                                rep_->level);
    }

    if (s.ok() && !iiter->status().IsNotFound()) {
      s = iiter->status();
    }
  }

  CO_RETURN s;
}

// This function reads multiple data blocks from disk using Env::MultiRead()
// and optionally inserts them into the block cache. It uses the scratch
// buffer provided by the caller, which is contiguous. If scratch is a nullptr
// it allocates a separate buffer for each block. Typically, if the blocks
// need to be uncompressed and there is no compressed block cache, callers
// can allocate a temporary scratch buffer in order to minimize memory
// allocations.
// If options.fill_cache is true, it inserts the blocks into cache. If its
// false and scratch is non-null and the blocks are uncompressed, it copies
// the buffers to heap. In any case, the CachableEntry<Block> returned will
// own the data bytes.
// If compression is enabled and also there is no compressed block cache,
// the adjacent blocks are read out in one IO (combined read)
// batch - A MultiGetRange with only those keys with unique data blocks not
//         found in cache
// handles - A vector of block handles. Some of them me be NULL handles
// scratch - An optional contiguous buffer to read compressed blocks into
DEFINE_SYNC_AND_ASYNC(void, BlockBasedTable::RetrieveMultipleBlocks)
(const ReadOptions& options, const MultiGetRange* batch,
 const autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE>* handles,
 Status* statuses, CachableEntry<Block_kData>* results, char* scratch,
 UnownedPtr<Decompressor> decomp, bool use_fs_scratch) const {
  RandomAccessFileReader* file = rep_->file.get();
  const Footer& footer = rep_->footer;
  const ImmutableOptions& ioptions = rep_->ioptions;

  if (ioptions.allow_mmap_reads) {
    size_t idx_in_batch = 0;
    for (auto mget_iter = batch->begin(); mget_iter != batch->end();
         ++mget_iter, ++idx_in_batch) {
      const BlockHandle& handle = (*handles)[idx_in_batch];
      if (handle.IsNull()) {
        continue;
      }

      // XXX: use_cache=true means double cache query?
      statuses[idx_in_batch] = RetrieveBlock(
          nullptr, options, handle, decomp,
          &results[idx_in_batch].As<Block_kData>(), mget_iter->get_context,
          /* lookup_context */ nullptr,
          /* for_compaction */ false, /* use_cache */ true,
          /* async_read */ false, /* use_block_cache_for_lookup */ true);
    }
    assert(idx_in_batch == handles->size());
    CO_RETURN;
  }

  if (options.read_tier == kBlockCacheTier) {
    for (size_t i = 0; i < handles->size(); ++i) {
      if (!(*handles)[i].IsNull()) {
        statuses[i] = Status::Incomplete("no blocking io");
      }
    }
    CO_RETURN;
  }

  // In direct IO mode, blocks share the direct io buffer.
  // Otherwise, blocks share the scratch buffer.
  const bool use_shared_buffer = file->use_direct_io() || scratch != nullptr;

  autovector<FSReadRequest, MultiGetContext::MAX_BATCH_SIZE> read_reqs;
  size_t buf_offset = 0;
  size_t idx_in_batch = 0;

  uint64_t prev_offset = 0;
  size_t prev_len = 0;
  autovector<size_t, MultiGetContext::MAX_BATCH_SIZE> req_idx_for_block;
  autovector<size_t, MultiGetContext::MAX_BATCH_SIZE> req_offset_for_block;
  for (auto mget_iter = batch->begin(); mget_iter != batch->end();
       ++mget_iter, ++idx_in_batch) {
    const BlockHandle& handle = (*handles)[idx_in_batch];
    if (handle.IsNull()) {
      continue;
    }

    size_t prev_end = static_cast<size_t>(prev_offset) + prev_len;

    // If current block is adjacent to the previous one, at the same time,
    // compression is enabled and there is no compressed cache, we combine
    // the two block read as one.
    // We don't combine block reads here in direct IO mode. MultiRead()
    // realigns and merges direct IO requests when necessary, but the async_io
    // path below reads each request on its own.
    if ((use_shared_buffer || use_fs_scratch) && !file->use_direct_io() &&
        prev_end == handle.offset()) {
      req_offset_for_block.emplace_back(prev_len);
      prev_len += BlockSizeWithTrailer(handle);
    } else {
      // No compression or current block and previous one is not adjacent:
      // Step 1, create a new request for previous blocks
      if (prev_len != 0) {
        FSReadRequest req;
        req.offset = prev_offset;
        req.len = prev_len;
        if (file->use_direct_io() || use_fs_scratch) {
          req.scratch = nullptr;
        } else if (use_shared_buffer) {
          req.scratch = scratch + buf_offset;
          buf_offset += req.len;
        } else {
          req.scratch = new char[req.len];
        }
        read_reqs.emplace_back(std::move(req));
      }

      // Step 2, remember the previous block info
      prev_offset = handle.offset();
      prev_len = BlockSizeWithTrailer(handle);
      req_offset_for_block.emplace_back(0);
    }
    req_idx_for_block.emplace_back(read_reqs.size());

    PERF_COUNTER_ADD(block_read_count, 1);
    PERF_COUNTER_ADD(block_read_byte, BlockSizeWithTrailer(handle));
    PERF_COUNTER_ADD(data_block_read_byte, BlockSizeWithTrailer(handle));
  }
  // Handle the last block and process the pending last request
  if (prev_len != 0) {
    FSReadRequest req;
    req.offset = prev_offset;
    req.len = prev_len;
    if (file->use_direct_io() || use_fs_scratch) {
      req.scratch = nullptr;
    } else if (use_shared_buffer) {
      req.scratch = scratch + buf_offset;
    } else {
      req.scratch = new char[req.len];
    }
    read_reqs.emplace_back(std::move(req));
  }

  AlignedBuffer direct_io_buffer;
  AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
  {
    IOOptions opts;
    IODebugContext dbg;
    IOStatus s = file->PrepareIOOptions(options, opts, &dbg);
    if (s.ok()) {
#if defined(WITH_COROUTINES)
      // RandomAccessFileReader handles direct IO alignment on both paths. On
      // the async_io path each direct-IO request is aligned and read on its
      // own, so adjacent blocks can read (and charge the rate limiter for) a
      // shared aligned page twice. Each request keeps its aligned buffer in
      // fs_scratch until the blocks below are parsed.
      if (batch->context()->use_coro_read()) {
        s = co_await file->MultiReadCoroutine(
            opts, &read_reqs[0], read_reqs.size(), &direct_io_context, &dbg);
      } else {
        co_await batch->context()->reader().MultiReadAsync(
            file, opts, &read_reqs[0], read_reqs.size(), &dbg);
      }
#else   // WITH_COROUTINES
      s = file->MultiRead(opts, &read_reqs[0], read_reqs.size(),
                          &direct_io_context, &dbg);
#endif  // WITH_COROUTINES
    }
    if (!s.ok()) {
      // Discard all the results in this batch if there is any time out
      // or overall MultiRead error
      for (FSReadRequest& req : read_reqs) {
        req.status = s;
      }
    }
  }

  idx_in_batch = 0;
  size_t valid_batch_idx = 0;
  for (auto mget_iter = batch->begin(); mget_iter != batch->end();
       ++mget_iter, ++idx_in_batch) {
    const BlockHandle& handle = (*handles)[idx_in_batch];

    if (handle.IsNull()) {
      continue;
    }

    assert(valid_batch_idx < req_idx_for_block.size());
    assert(valid_batch_idx < req_offset_for_block.size());
    assert(req_idx_for_block[valid_batch_idx] < read_reqs.size());
    size_t& req_idx = req_idx_for_block[valid_batch_idx];
    size_t& req_offset = req_offset_for_block[valid_batch_idx];
    valid_batch_idx++;
    FSReadRequest& req = read_reqs[req_idx];
    Status s = req.status;
    if (s.ok()) {
      if ((req.result.size() != req.len) ||
          (req_offset + BlockSizeWithTrailer(handle) > req.result.size())) {
        s = Status::Corruption("truncated block read from " +
                               rep_->file->file_name() + " offset " +
                               std::to_string(handle.offset()) + ", expected " +
                               std::to_string(req.len) + " bytes, got " +
                               std::to_string(req.result.size()));
      }
    }

    BlockContents serialized_block;
    if (s.ok()) {
      if (!use_fs_scratch && !use_shared_buffer) {
        // We allocated a buffer for this block. Give ownership of it to
        // BlockContents so it can free the memory
        assert(req.result.data() == req.scratch);
        assert(req.result.size() == BlockSizeWithTrailer(handle));
        assert(req_offset == 0);
        serialized_block =
            BlockContents(std::unique_ptr<char[]>(req.scratch), handle.size());
      } else {
        // We used the scratch buffer or direct io buffer
        // which are shared by the blocks.
        // In case of use_fs_scratch, underlying file system provided buffer is
        // used. serialized_block does not have the ownership.
        serialized_block =
            BlockContents(Slice(req.result.data() + req_offset, handle.size()));
      }
#ifndef NDEBUG
      serialized_block.has_trailer = true;
#endif

      if (options.verify_checksums) {
        PERF_TIMER_GUARD(block_checksum_time);
        const char* data = serialized_block.data.data();
        // Since the scratch might be shared, the offset of the data block in
        // the buffer might not be 0. req.result.data() only point to the
        // begin address of each read request, we need to add the offset
        // in each read request. Checksum is stored in the block trailer,
        // beyond the payload size.
        s = VerifyBlockChecksum(footer, data, handle.size(),
                                rep_->file->file_name(), handle.offset(),
                                BlockType::kData);
        RecordTick(ioptions.stats, BLOCK_CHECKSUM_COMPUTE_COUNT);
        if (!s.ok()) {
          RecordTick(ioptions.stats, BLOCK_CHECKSUM_MISMATCH_COUNT);
        }
        TEST_SYNC_POINT_CALLBACK("RetrieveMultipleBlocks:VerifyChecksum", &s);
        if (!s.ok() &&
            CheckFSFeatureSupport(ioptions.fs.get(),
                                  FSSupportedOps::kVerifyAndReconstructRead)) {
          assert(s.IsCorruption());
          assert(!ioptions.allow_mmap_reads);
          RecordTick(ioptions.stats, FILE_READ_CORRUPTION_RETRY_COUNT);

          // Repeat the read for this particular block using the regular
          // synchronous Read API. We can use the same chunk of memory
          // pointed to by data, since the size is identical and we know
          // its not a memory mapped file
          Slice result;
          IOOptions opts;
          IODebugContext dbg;
          IOStatus io_s = file->PrepareIOOptions(options, opts, &dbg);
          opts.verify_and_reconstruct_read = true;
          io_s = file->Read(handle.offset(), BlockSizeWithTrailer(handle), opts,
                            &result, const_cast<char*>(data), &dbg);
          if (io_s.ok()) {
            assert(result.data() == data);
            assert(result.size() == BlockSizeWithTrailer(handle));
            s = VerifyBlockChecksum(footer, data, handle.size(),
                                    rep_->file->file_name(), handle.offset(),
                                    BlockType::kData);
            if (s.ok()) {
              RecordTick(ioptions.stats,
                         FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT);
            }
          } else {
            s = io_s;
          }
        }
      }
    } else if (!use_shared_buffer) {
      // Free the allocated scratch buffer.
      delete[] req.scratch;
    }

    if (s.ok()) {
      s = CreateAndPinBlockInCache(options, handle, decomp, &serialized_block,
                                   &results[idx_in_batch]);
    }
    statuses[idx_in_batch] = s;
  }

  if (use_fs_scratch) {
    // Free the FileSystem-provided scratch buffers here. The direct-IO
    // coroutine path also hands its aligned buffers back via fs_scratch. Read
    // requests might have been combined into one.
    for (FSReadRequest& req : read_reqs) {
      if (req.fs_scratch != nullptr) {
        req.fs_scratch.reset();
        req.fs_scratch = nullptr;
      }
    }
  }
}

using MultiGetRange = MultiGetContext::Range;
DEFINE_SYNC_AND_ASYNC(void, BlockBasedTable::MultiGet)
(const ReadOptions& read_options, const MultiGetRange* mget_range,
 const SliceTransform* prefix_extractor, bool skip_filters) {
  if (mget_range->empty()) {
    // Caller should ensure non-empty (performance bug)
    assert(false);
    CO_RETURN;  // Nothing to do
  }

  FilterBlockReader* const filter =
      !skip_filters ? rep_->filter.get() : nullptr;
  MultiGetRange sst_file_range(*mget_range, mget_range->begin(),
                               mget_range->end());

  // First check the full filter
  // If full filter not useful, Then go into each block
  uint64_t tracing_mget_id = BlockCacheTraceHelper::kReservedGetId;
  if (sst_file_range.begin()->get_context) {
    tracing_mget_id = sst_file_range.begin()->get_context->get_tracing_get_id();
  }
  // TODO: need more than one lookup_context here to track individual filter
  // and index partition hits and misses.
  BlockCacheLookupContext metadata_lookup_context{
      TableReaderCaller::kUserMultiGet, tracing_mget_id,
      /*_get_from_user_specified_snapshot=*/read_options.snapshot != nullptr};
  FullFilterKeysMayMatch(filter, &sst_file_range, prefix_extractor,
                         &metadata_lookup_context, read_options);

  if (!sst_file_range.empty()) {
    IndexBlockIter iiter_on_stack;
    // if prefix_extractor found in block differs from options, disable
    // BlockPrefixIndex. Only do this check when index_type is kHashSearch.
    bool need_upper_bound_check = false;
    if (rep_->index_type == BlockBasedTableOptions::kHashSearch) {
      need_upper_bound_check = PrefixExtractorChanged(prefix_extractor);
    }
    auto iiter = NewIndexIterator(
        read_options, need_upper_bound_check, &iiter_on_stack,
        sst_file_range.begin()->get_context, &metadata_lookup_context);
    std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    uint64_t prev_offset = std::numeric_limits<uint64_t>::max();
    autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE> block_handles;
    std::array<CachableEntry<Block_kData>, MultiGetContext::MAX_BATCH_SIZE>
        results;
    std::array<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
    // Empty data_lookup_contexts means "unused," when block cache tracing is
    // disabled. (Limited options as element type is not default contructible.)
    std::vector<BlockCacheLookupContext> data_lookup_contexts;
    MultiGetContext::Mask reused_mask = 0;
#ifdef WITH_COROUTINES
    // Coroutine mode allocates scratch on the heap, so allocate the required
    // size when I/O is needed instead of embedding an 8 KiB buffer in the
    // frame.
    constexpr size_t kStackBufSize = 0;
#else
    constexpr size_t kStackBufSize = kMultiGetReadStackBufSize;
#endif
    std::array<char, kStackBufSize> stack_buf;
    std::unique_ptr<char[]> block_buf;
    if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled()) {
      // Awkward because BlockCacheLookupContext is not CopyAssignable
      data_lookup_contexts.reserve(MultiGetContext::MAX_BATCH_SIZE);
      for (size_t i = 0; i < MultiGetContext::MAX_BATCH_SIZE; ++i) {
        data_lookup_contexts.push_back(metadata_lookup_context);
      }
    }
    {
      MultiGetRange data_block_range(sst_file_range, sst_file_range.begin(),
                                     sst_file_range.end());
      CachableEntry<DecompressorDict> dict;
      Status dict_status;
      dict_status.PermitUncheckedError();
      bool dict_inited = false;
      size_t total_len = 0;

      // GetContext for any key will do, as the stats will be aggregated
      // anyway
      GetContext* get_context = sst_file_range.begin()->get_context;

      {
        using BCI = BlockCacheInterface<Block_kData>;
        BCI block_cache{rep_->table_options.block_cache.get()};
        std::array<BCI::TypedAsyncLookupHandle, MultiGetContext::MAX_BATCH_SIZE>
            async_handles;
        BlockCreateContext create_ctx = rep_->create_context;
        std::array<CacheKey, MultiGetContext::MAX_BATCH_SIZE> cache_keys;
        size_t cache_lookup_count = 0;

        for (auto miter = data_block_range.begin();
             miter != data_block_range.end(); ++miter) {
          const Slice& key = miter->ikey;
          iiter->Seek(miter->ikey);

          IndexValue v;
          if (iiter->Valid()) {
            v = iiter->value();
          }
          if (!iiter->Valid() ||
              (!v.first_internal_key.empty() && !skip_filters &&
               UserComparatorWrapper(
                   rep_->internal_comparator.user_comparator())
                       .CompareWithoutTimestamp(
                           ExtractUserKey(key),
                           ExtractUserKey(v.first_internal_key)) < 0)) {
            // The requested key falls between highest key in previous block and
            // lowest key in current block.
            if (!iiter->status().IsNotFound()) {
              *(miter->s) = iiter->status();
            }
            data_block_range.SkipKey(miter);
            sst_file_range.SkipKey(miter);
            continue;
          }

          if (!dict_inited && rep_->uncompression_dict_reader) {
            dict_status = rep_->uncompression_dict_reader
                              ->GetOrReadUncompressionDictionary(
                                  nullptr /* prefetch_buffer */, read_options,
                                  get_context, &metadata_lookup_context, &dict);
            dict_inited = true;
          }

          if (!dict_status.ok()) {
            assert(!dict_status.IsNotFound());
            *(miter->s) = dict_status;
            data_block_range.SkipKey(miter);
            sst_file_range.SkipKey(miter);
            continue;
          } else {
            assert(!dict_inited || dict.GetValue() != nullptr);
          }
          if (dict.GetValue()) {
            create_ctx.decompressor = dict.GetValue()->decompressor_.get();
          }

          if (v.handle.offset() == prev_offset) {
            // This key can reuse the previous block (later on).
            // Mark previous as "reused"
            reused_mask |= MultiGetContext::Mask{1}
                           << (block_handles.size() - 1);
            // Use null handle to indicate this one reuses same block as
            // previous.
            block_handles.emplace_back(BlockHandle::NullBlockHandle());
            continue;
          }
          prev_offset = v.handle.offset();
          block_handles.emplace_back(v.handle);

          if (block_cache) {
            // Lookup the cache for the given data block referenced by an index
            // iterator value (i.e BlockHandle). If it exists in the cache,
            // initialize block to the contents of the data block.

            // An async version of MaybeReadBlockAndLoadToCache /
            // GetDataBlockFromCache
            BCI::TypedAsyncLookupHandle& async_handle =
                async_handles[cache_lookup_count];
            cache_keys[cache_lookup_count] =
                GetCacheKey(rep_->base_cache_key, v.handle);
            async_handle.key = cache_keys[cache_lookup_count].AsSlice();
            // NB: StartAsyncLookupFull populates async_handle.helper
            async_handle.create_context = &create_ctx;
            async_handle.priority = GetCachePriority<Block_kData>();
            async_handle.stats = rep_->ioptions.statistics.get();

            block_cache.StartAsyncLookupFull(
                async_handle, rep_->ioptions.lowest_used_cache_tier);
            ++cache_lookup_count;
            // TODO: stats?
          }
        }

        if (block_cache) {
          block_cache.get()->WaitAll(&async_handles[0], cache_lookup_count);
        }
        size_t lookup_idx = 0;
        for (size_t i = 0; i < block_handles.size(); ++i) {
          // If this block was a success or failure or not needed because
          // the corresponding key is in the same block as a prior key, skip
          if (block_handles[i] == BlockHandle::NullBlockHandle()) {
            continue;
          }
          if (!block_cache) {
            total_len += BlockSizeWithTrailer(block_handles[i]);
          } else {
            BCI::TypedHandle* h = async_handles[lookup_idx].Result();
            if (h) {
              // Cache hit
              results[i].SetCachedValue(block_cache.Value(h), block_cache.get(),
                                        h);
              // Don't need to fetch
              block_handles[i] = BlockHandle::NullBlockHandle();
              UpdateCacheHitMetrics(BlockType::kData, get_context,
                                    block_cache.get()->GetUsage(h));
            } else {
              // Cache miss
              total_len += BlockSizeWithTrailer(block_handles[i]);
              UpdateCacheMissMetrics(BlockType::kData, get_context);
            }
            if (!data_lookup_contexts.empty()) {
              // Populate cache key before it's discarded
              data_lookup_contexts[i].block_key =
                  async_handles[lookup_idx].key.ToString();
            }
            ++lookup_idx;
          }
        }
        assert(lookup_idx == cache_lookup_count);
      }

      if (total_len) {
        char* scratch = nullptr;
        bool use_fs_scratch = false;
        assert(dict_inited || !rep_->uncompression_dict_reader);
        assert(dict_status.ok());

        if (!rep_->file->use_direct_io()) {
          if (CheckFSFeatureSupport(rep_->ioptions.fs.get(),
                                    FSSupportedOps::kFSBuffer)) {
            use_fs_scratch = true;
          }
        }

        // If using direct IO, then scratch is not used, so keep it nullptr.
        // If the blocks need to be uncompressed and we don't need the
        // compressed blocks, then we can use a contiguous block of
        // memory to read in all the blocks as it will be temporary
        // storage
        // 1. If blocks are compressed and compressed block cache is there,
        //    alloc heap bufs
        // 2. If blocks are uncompressed, alloc heap bufs
        // 3. If blocks are compressed and no compressed block cache, use
        //    scratch storage
        if (!use_fs_scratch && !rep_->file->use_direct_io() &&
            rep_->decompressor) {
          if (total_len <= stack_buf.size()) {
            scratch = stack_buf.data();
          } else {
            scratch = new char[total_len];
            block_buf.reset(scratch);
          }
        }
        CO_AWAIT(RetrieveMultipleBlocks, read_options, &data_block_range,
                 &block_handles, &statuses[0], &results[0], scratch,
                 dict.GetValue() ? dict.GetValue()->decompressor_.get()
                                 : rep_->decompressor.get(),
                 use_fs_scratch);
        if (get_context) {
          ++(get_context->get_context_stats_.num_sst_read);
        }
      }
    }

    DataBlockIter first_biter;
    DataBlockIter next_biter;
    size_t idx_in_batch = 0;
    SharedCleanablePtr shared_cleanable;
    // Embedded blob resolution is gated on this table actually having embedded
    // blob records, so the common (non-embedded) MultiGet path is unchanged.
    // The scratch (reused across entries) is only constructed for embedded
    // tables and avoids per-entry allocation.
    std::optional<EmbeddedValueGetScratch> embedded_scratch;
    if (rep_->has_embedded_blobs) {
      embedded_scratch.emplace();
    }
    // MultiGet does not build row-cache replay logs (TableCache::MultiGetFilter
    // returns NotSupported when row_cache is set, falling back to per-key Get),
    // so same-file wide-column blob resolution is always deferred to GetContext
    // for zero-copy resolution here.
    for (auto miter = sst_file_range.begin(); miter != sst_file_range.end();
         ++miter) {
      Status s;
      GetContext* get_context = miter->get_context;
      const Slice& key = miter->ikey;
      bool matched = false;  // if such user key matched a key in SST
      bool done = false;
      bool first_block = true;
      do {
        DataBlockIter* biter = nullptr;
        uint64_t referenced_data_size = 0;
        Block_kData* parsed_block_value = nullptr;
        bool reusing_prev_block;
        bool later_reused;
        bool does_referenced_key_exist = false;
        bool handle_present = false;
        BlockCacheLookupContext* lookup_data_block_context =
            data_lookup_contexts.empty() ? nullptr
                                         : &data_lookup_contexts[idx_in_batch];
        if (first_block) {
          handle_present = !block_handles[idx_in_batch].IsNull();
          parsed_block_value = results[idx_in_batch].GetValue();
          if (handle_present || parsed_block_value) {
            first_biter.Invalidate(Status::OK());
            NewDataBlockIterator<DataBlockIter>(
                read_options, results[idx_in_batch].As<Block>(), &first_biter,
                statuses[idx_in_batch]);
            reusing_prev_block = false;
          } else {
            // If handle is null and result is empty, then the status is never
            // set, which should be the initial value: ok().
            assert(statuses[idx_in_batch].ok());
            reusing_prev_block = true;
          }
          biter = &first_biter;
          later_reused =
              (reused_mask & (MultiGetContext::Mask{1} << idx_in_batch)) != 0;
          idx_in_batch++;
        } else {
          IndexValue v = iiter->value();
          if (!v.first_internal_key.empty() && !skip_filters &&
              UserComparatorWrapper(rep_->internal_comparator.user_comparator())
                      .CompareWithoutTimestamp(
                          ExtractUserKey(key),
                          ExtractUserKey(v.first_internal_key)) < 0) {
            // The requested key falls between highest key in previous block and
            // lowest key in current block.
            break;
          }

          next_biter.Invalidate(Status::OK());
          Status tmp_s;
          NewDataBlockIterator<DataBlockIter>(
              read_options, iiter->value().handle, &next_biter,
              BlockType::kData, get_context, lookup_data_block_context,
              /* prefetch_buffer= */ nullptr, /* for_compaction = */ false,
              /*async_read = */ false, tmp_s,
              /* use_block_cache_for_lookup = */ true);
          biter = &next_biter;
          reusing_prev_block = false;
          later_reused = false;
        }

        if (read_options.read_tier == kBlockCacheTier &&
            biter->status().IsIncomplete()) {
          // couldn't get block from block_cache
          // Update Saver.state to Found because we are only looking for
          // whether we can guarantee the key is not there with kBlockCacheTier
          get_context->MarkKeyMayExist();
          s = biter->status();
          break;
        }
        if (!biter->status().ok()) {
          s = biter->status();
          break;
        }

        // Reusing blocks complicates pinning/Cleanable, because the cache
        // entry referenced by biter can only be released once all returned
        // pinned values are released. This code previously did an extra
        // block_cache Ref for each reuse, but that unnecessarily increases
        // block cache contention. Instead we can use a variant of shared_ptr
        // to release in block cache only once.
        //
        // Although the biter loop below might SaveValue multiple times for
        // merges, just one value_pinner suffices, as MultiGet will merge
        // the operands before returning to the API user.
        Cleanable* value_pinner;
        if (biter->IsValuePinned()) {
          if (reusing_prev_block) {
            // Note that we don't yet know if the MultiGet results will need
            // to pin this block, so we might wrap a block for sharing and
            // still end up with 1 (or 0) pinning ref. Not ideal but OK.
            //
            // Here we avoid adding redundant cleanups if we didn't end up
            // delegating the cleanup from last time around.
            if (!biter->HasCleanups()) {
              assert(shared_cleanable.get());
              if (later_reused) {
                shared_cleanable.RegisterCopyWith(biter);
              } else {
                shared_cleanable.MoveAsCleanupTo(biter);
              }
            }
          } else if (later_reused) {
            assert(biter->HasCleanups());
            // Make the existing cleanups on `biter` sharable:
            shared_cleanable.Allocate();
            // Move existing `biter` cleanup(s) to `shared_cleanable`
            biter->DelegateCleanupsTo(&*shared_cleanable);
            // Reference `shared_cleanable` as new cleanup for `biter`
            shared_cleanable.RegisterCopyWith(biter);
          }
          assert(biter->HasCleanups());
          value_pinner = biter;
        } else {
          value_pinner = nullptr;
        }

        bool may_exist = biter->SeekForGet(key);
        if (!may_exist) {
          // HashSeek cannot find the key this block and the the iter is not
          // the end of the block, i.e. cannot be in the following blocks
          // either. In this case, the seek_key cannot be found, so we break
          // from the top level for-loop.
          break;
        }

        // Call the *saver function on each entry/block until it returns false
        for (; biter->status().ok() && biter->Valid(); biter->Next()) {
          Slice key_to_save = biter->key();
          Slice value_to_save = biter->value();
          Cleanable* effective_pinner = value_pinner;
          const bool defer_embedded_wide_columns =
              embedded_scratch.has_value() && !get_context->HasReplayLog();
          if (embedded_scratch) {
            s = ResolveEmbeddedValueForGet(
                read_options, biter->key(), biter->value(), &*embedded_scratch,
                value_pinner, &key_to_save, &value_to_save, &effective_pinner,
                defer_embedded_wide_columns);
            if (!s.ok()) {
              break;
            }
          }

          ParsedInternalKey parsed_key;
          Status pik_status = ParseInternalKey(
              key_to_save, &parsed_key, false /* log_err_key */);  // TODO
          if (!pik_status.ok()) {
            s = pik_status;
            break;
          }
          Status read_status;
          bool ret = get_context->SaveValue(
              parsed_key, value_to_save, &matched, &read_status,
              effective_pinner, defer_embedded_wide_columns ? this : nullptr);
          if (!read_status.ok()) {
            s = read_status;
            break;
          }
          if (!ret) {
            if (get_context->State() == GetContext::GetState::kFound) {
              does_referenced_key_exist = true;
              referenced_data_size =
                  biter->key().size() + biter->value().size();
            }
            done = true;
            break;
          }
        }
        // Write the block cache access.
        // XXX: There appear to be 'break' statements above that bypass this
        // writing of the block cache trace record
        if (lookup_data_block_context && !reusing_prev_block && first_block) {
          Slice referenced_key;
          if (does_referenced_key_exist) {
            referenced_key = biter->key();
          } else {
            referenced_key = key;
          }

          // block_key is self-assigned here (previously assigned from
          // cache_keys / async_handles, now out of scope)
          SaveLookupContextOrTraceRecord(lookup_data_block_context->block_key,
                                         /*is_cache_hit=*/!handle_present,
                                         read_options, parsed_block_value,
                                         lookup_data_block_context);
          FinishTraceRecord(
              *lookup_data_block_context, lookup_data_block_context->block_key,
              referenced_key, does_referenced_key_exist, referenced_data_size);
        }
        if (s.ok()) {
          s = biter->status();
        }
        if (done || !s.ok()) {
          // Avoid the extra Next which is expensive in two-level indexes
          break;
        }
        if (first_block) {
          iiter->Seek(key);
          if (!iiter->Valid()) {
            break;
          }
        }
        first_block = false;
        iiter->Next();
      } while (iiter->Valid());

      if (matched && filter != nullptr) {
        if (rep_->whole_key_filtering) {
          RecordTick(rep_->ioptions.stats, BLOOM_FILTER_FULL_TRUE_POSITIVE);
        } else {
          RecordTick(rep_->ioptions.stats, BLOOM_FILTER_PREFIX_TRUE_POSITIVE);
        }
        // Includes prefix stats
        PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_true_positive, 1,
                                  rep_->level);
      }
      if (s.ok() && !iiter->status().IsNotFound()) {
        s = iiter->status();
      }
      *(miter->s) = s;
    }
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    // Not sure why we need to do it. Should investigate more.
    for (auto& st : statuses) {
      st.PermitUncheckedError();
    }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
  }
}
}  // namespace ROCKSDB_NAMESPACE
#endif
