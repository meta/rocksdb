//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/block_based_table_reader.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "block_cache.h"
#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "db/blob/blob_contents.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_source.h"
#include "db/compaction/compaction_picker.h"
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "db/wide/wide_column_serialization.h"
#include "file/file_prefetch_buffer.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "memory/memory_allocator_impl.h"
#include "monitoring/perf_context_imp.h"
#include "options/options_helper.h"
#include "parsed_full_filter_block.h"
#include "port/lang.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/trace_record.h"
#include "table/block_based/binary_search_index_reader.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_iterator.h"
#include "table/block_based/block_prefix_index.h"
#include "table/block_based/block_type.h"
#include "table/block_based/embedded_blob_resolving_iterator.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/hash_index_reader.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/block_based/partitioned_index_reader.h"
#include "table/block_based/user_defined_index_wrapper.h"
#include "table/block_fetcher.h"
#include "table/embedded_blob_sst.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/multiget_context.h"
#include "table/persistent_cache_helper.h"
#include "table/persistent_cache_options.h"
#include "table/sst_file_writer_collectors.h"
#include "table/two_level_iterator.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {

CacheAllocationPtr CopyBufferToHeap(MemoryAllocator* allocator, Slice& buf) {
  CacheAllocationPtr heap_buf;
  heap_buf = AllocateBlock(buf.size(), allocator);
  memcpy(heap_buf.get(), buf.data(), buf.size());
  return heap_buf;
}

// Pins all of `contents` into `value`, which takes ownership.
Status PinBlobContents(std::unique_ptr<BlobContents> contents,
                       PinnableSlice* value) {
  const Slice result = contents->data();
  BlobContents* const owned = contents.release();
  value->PinSlice(
      result,
      [](void* arg1, void* /*arg2*/) {
        delete static_cast<BlobContents*>(arg1);
      },
      owned, nullptr);
  return Status::OK();
}

}  // namespace

Status AllocateReadScopedBlockBuffer(
    ReadScopedBlockBufferProvider& block_buffer_provider, size_t requested_size,
    size_t requested_alignment, ReadScopedBlockBufferProvider::Lease* lease) {
  assert(lease != nullptr);

  Status s = block_buffer_provider.Allocate(requested_size, requested_alignment,
                                            lease);
  if (!s.ok()) {
    return s;
  }
  if (lease->data == nullptr || lease->size < requested_size ||
      lease->cleanup.get() == nullptr) {
    return Status::InvalidArgument(
        "ReadScopedBlockBufferProvider returned an invalid lease");
  }
  if (requested_alignment > 1 && (reinterpret_cast<uintptr_t>(lease->data) &
                                  (requested_alignment - 1)) != 0) {
    return Status::InvalidArgument(
        "ReadScopedBlockBufferProvider returned a misaligned lease");
  }
  return Status::OK();
}

Status AllocateReadScopedAlignedBuffer(
    ReadScopedBlockBufferProvider& block_buffer_provider, size_t size,
    size_t alignment, ReadScopedBlockBufferProvider::Lease* lease,
    AlignedBuffer::ExternalAllocation* out) {
  if (out == nullptr) {
    return Status::InvalidArgument(
        "AlignedBuffer allocation output is nullptr");
  }
  Status s = AllocateReadScopedBlockBuffer(block_buffer_provider, size,
                                           alignment, lease);
  if (!s.ok()) {
    return s;
  }

  SharedCleanablePtr owner_cleanup = lease->cleanup;
  out->data = lease->data;
  out->size = lease->size;
  out->owner = FSAllocationPtr(
      lease->data, [owner_cleanup = std::move(owner_cleanup)](void*) mutable {
        owner_cleanup.Reset();
      });
  return Status::OK();
}

AlignedBuffer::Allocator MakeReadScopedAlignedBufferAllocator(
    ReadScopedBlockBufferProviderRef block_buffer_provider,
    ReadScopedBlockBufferProvider::Lease* lease) {
  assert(block_buffer_provider.has_value());
  assert(lease != nullptr);
  return [block_buffer_provider, lease](
             size_t size, size_t alignment,
             AlignedBuffer::ExternalAllocation* out) -> Status {
    assert(block_buffer_provider.has_value());
    return AllocateReadScopedAlignedBuffer(block_buffer_provider->get(), size,
                                           alignment, lease, out);
  };
}

ReadScopedBlockBufferProviderRef GetReadScopedBlockBufferProvider(
    const ReadOptions& ro, bool allow_mmap_reads) {
  if (allow_mmap_reads || ro.read_scoped_block_buffer_provider == nullptr) {
    return std::nullopt;
  }
  return std::ref(*ro.read_scoped_block_buffer_provider);
}

bool ShouldUseDataBlockCacheForIterator(
    const BlockBasedTableOptions& table_options, const ReadOptions& ro,
    bool allow_mmap_reads) {
  if (GetReadScopedBlockBufferProvider(ro, allow_mmap_reads).has_value()) {
    // Provider-backed scan reads use caller-owned read-scope storage as the
    // data-block backing, so they intentionally skip both lookup and insertion
    // in the shared data-block cache.
    return false;
  }
  return table_options.block_cache != nullptr;
}

Status CopyBufferToHeapBlockContents(Slice src, size_t data_size,
                                     MemoryAllocator* allocator,
                                     BlockContents* out_contents) {
  assert(out_contents != nullptr);
  assert(data_size <= src.size());

  *out_contents = BlockContents(CopyBufferToHeap(allocator, src), data_size);
#ifndef NDEBUG
  out_contents->has_trailer = src.size() > data_size;
#endif
  return Status::OK();
}

Status CopyBufferToReadScopedBlockContents(
    Slice src, size_t data_size,
    ReadScopedBlockBufferProvider& block_buffer_provider,
    BlockContents* out_contents) {
  assert(out_contents != nullptr);
  assert(data_size <= src.size());

  ReadScopedBlockBufferProvider::Lease read_scoped_lease;
  Status s = AllocateReadScopedBlockBuffer(block_buffer_provider, src.size(), 1,
                                           &read_scoped_lease);
  if (!s.ok()) {
    return s;
  }

  memcpy(read_scoped_lease.data, src.data(), src.size());
  out_contents->data = Slice(read_scoped_lease.data, data_size);
  out_contents->cleanup = std::move(read_scoped_lease.cleanup);
  out_contents->backing_size = read_scoped_lease.size;
  out_contents->AssertSingleOwner();
#ifndef NDEBUG
  out_contents->has_trailer = src.size() > data_size;
#endif
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE

// Generate the regular and coroutine versions of some methods by
// including block_based_table_reader_sync_and_async.h twice
// Macros in the header will expand differently based on whether
// WITH_COROUTINES or WITHOUT_COROUTINES is defined
// clang-format off
#define WITHOUT_COROUTINES
#include "table/block_based/block_based_table_reader_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "table/block_based/block_based_table_reader_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {

extern const uint64_t kBlockBasedTableMagicNumber;
extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;

BlockBasedTable::~BlockBasedTable() {
  auto ua = rep_->uncache_aggressiveness.LoadRelaxed();
  // NOTE: there is an undiagnosed incompatibility with mmap reads,
  // where attempting to read the index below can result in bus error.
  // In theory the mmap should remain in place until destruction of
  // rep_, so even a page fault should be satisfiable. But also, combining
  // mmap reads with block cache is weird, so it's not a concerning loss.
  if (ua > 0 && rep_->table_options.block_cache &&
      !rep_->ioptions.allow_mmap_reads) {
    if (rep_->filter) {
      rep_->filter->EraseFromCacheBeforeDestruction(ua);
    }
    if (rep_->index_reader) {
      {
        // TODO: Also uncache data blocks known after any gaps in partitioned
        // index. Right now the iterator errors out as soon as there's an
        // index partition not in cache.
        IndexBlockIter iiter_on_stack;
        ReadOptions ropts;
        ropts.read_tier = kBlockCacheTier;  // No I/O
        auto iiter = NewIndexIterator(
            ropts, /*disable_prefix_seek=*/false, &iiter_on_stack,
            /*get_context=*/nullptr, /*lookup_context=*/nullptr);
        std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
        if (iiter != &iiter_on_stack) {
          iiter_unique_ptr.reset(iiter);
        }
        // Un-cache the data blocks the index iterator with tell us about
        // without I/O. (NOTE: It's extremely unlikely that a data block
        // will be in block cache without the index block pointing to it
        // also in block cache.)
        UncacheAggressivenessAdvisor advisor(ua);
        for (iiter->SeekToFirst(); iiter->Valid() && advisor.ShouldContinue();
             iiter->Next()) {
          bool erased = EraseFromCache(iiter->value().handle);
          advisor.Report(erased);
        }
        iiter->status().PermitUncheckedError();
      }

      // Un-cache the index block(s)
      rep_->index_reader->EraseFromCacheBeforeDestruction(ua);
    }
  }
  delete rep_;
}

namespace {
// For hash based index, return false if table_properties->prefix_extractor_name
// and prefix_extractor both exist and match, otherwise true.
inline bool PrefixExtractorChangedHelper(
    const TableProperties* table_properties,
    const SliceTransform* prefix_extractor) {
  // BlockBasedTableOptions::kHashSearch requires prefix_extractor to be set.
  // Turn off hash index in prefix_extractor is not set; if  prefix_extractor
  // is set but prefix_extractor_block is not set, also disable hash index
  if (prefix_extractor == nullptr || table_properties == nullptr ||
      table_properties->prefix_extractor_name.empty()) {
    return true;
  }

  // prefix_extractor and prefix_extractor_block are both non-empty
  if (table_properties->prefix_extractor_name != prefix_extractor->AsString()) {
    return true;
  } else {
    return false;
  }
}

template <typename TBlocklike>
uint32_t GetBlockNumRestarts(const TBlocklike& block) {
  if constexpr (std::is_convertible_v<const TBlocklike&, const Block&>) {
    const Block& b = block;
    return b.NumRestarts();
  } else {
    return 0;
  }
}

}  // namespace

void BlockBasedTable::UpdateCacheHitMetrics(BlockType block_type,
                                            GetContext* get_context,
                                            size_t usage) const {
  Statistics* const statistics = rep_->ioptions.stats;

  PERF_COUNTER_ADD(block_cache_hit_count, 1);
  PERF_COUNTER_ADD(block_cache_read_byte, usage);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 1,
                            static_cast<uint32_t>(rep_->level));

  if (get_context) {
    ++get_context->get_context_stats_.num_cache_hit;
    get_context->get_context_stats_.num_cache_bytes_read += usage;
  } else {
    RecordTick(statistics, BLOCK_CACHE_HIT);
    RecordTick(statistics, BLOCK_CACHE_BYTES_READ, usage);
  }

  switch (block_type) {
    case BlockType::kFilter:
    case BlockType::kFilterPartitionIndex:
      PERF_COUNTER_ADD(block_cache_filter_hit_count, 1);
      PERF_COUNTER_ADD(block_cache_filter_read_byte, usage);

      if (get_context) {
        ++get_context->get_context_stats_.num_cache_filter_hit;
      } else {
        RecordTick(statistics, BLOCK_CACHE_FILTER_HIT);
      }
      break;

    case BlockType::kCompressionDictionary:
      // TODO: introduce perf counter for compression dictionary hit count
      PERF_COUNTER_ADD(block_cache_compression_dict_read_byte, usage);
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_compression_dict_hit;
      } else {
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_HIT);
      }
      break;

    case BlockType::kIndex:
    case BlockType::kUserDefinedIndex:
      PERF_COUNTER_ADD(block_cache_index_hit_count, 1);
      PERF_COUNTER_ADD(block_cache_index_read_byte, usage);

      if (get_context) {
        ++get_context->get_context_stats_.num_cache_index_hit;
      } else {
        RecordTick(statistics, BLOCK_CACHE_INDEX_HIT);
      }
      break;

    case BlockType::kData:
    case BlockType::kProperties:
    case BlockType::kRangeDeletion:
    case BlockType::kHashIndexPrefixes:
    case BlockType::kHashIndexMetadata:
    case BlockType::kMetaIndex:
    case BlockType::kInvalid:
    default:
      // TODO: introduce dedicated tickers/statistics/counters
      // for range tombstones
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_data_hit;
      } else {
        RecordTick(statistics, BLOCK_CACHE_DATA_HIT);
      }
      break;
  }
}

void BlockBasedTable::UpdateCacheMissMetrics(BlockType block_type,
                                             GetContext* get_context) const {
  Statistics* const statistics = rep_->ioptions.stats;

  // TODO: introduce aggregate (not per-level) block cache miss count
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_miss_count, 1,
                            static_cast<uint32_t>(rep_->level));

  if (get_context) {
    ++get_context->get_context_stats_.num_cache_miss;
  } else {
    RecordTick(statistics, BLOCK_CACHE_MISS);
  }

  // TODO: introduce perf counters for misses per block type
  switch (block_type) {
    case BlockType::kFilter:
    case BlockType::kFilterPartitionIndex:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_filter_miss;
      } else {
        RecordTick(statistics, BLOCK_CACHE_FILTER_MISS);
      }
      break;

    case BlockType::kCompressionDictionary:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_compression_dict_miss;
      } else {
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_MISS);
      }
      break;

    case BlockType::kIndex:
    case BlockType::kUserDefinedIndex:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_index_miss;
      } else {
        RecordTick(statistics, BLOCK_CACHE_INDEX_MISS);
      }
      break;

    case BlockType::kData:
    case BlockType::kProperties:
    case BlockType::kRangeDeletion:
    case BlockType::kHashIndexPrefixes:
    case BlockType::kHashIndexMetadata:
    case BlockType::kMetaIndex:
    case BlockType::kInvalid:
    default:
      // TODO: introduce dedicated tickers/statistics/counters
      // for range tombstones
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_data_miss;
      } else {
        RecordTick(statistics, BLOCK_CACHE_DATA_MISS);
      }
      break;
  }
}

void BlockBasedTable::UpdateCacheInsertionMetrics(
    BlockType block_type, GetContext* get_context, size_t usage, bool redundant,
    Statistics* const statistics) {
  // TODO: introduce perf counters for block cache insertions
  if (get_context) {
    ++get_context->get_context_stats_.num_cache_add;
    if (redundant) {
      ++get_context->get_context_stats_.num_cache_add_redundant;
    }
    get_context->get_context_stats_.num_cache_bytes_write += usage;
  } else {
    RecordTick(statistics, BLOCK_CACHE_ADD);
    if (redundant) {
      RecordTick(statistics, BLOCK_CACHE_ADD_REDUNDANT);
    }
    RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, usage);
  }

  switch (block_type) {
    case BlockType::kFilter:
    case BlockType::kFilterPartitionIndex:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_filter_add;
        if (redundant) {
          ++get_context->get_context_stats_.num_cache_filter_add_redundant;
        }
        get_context->get_context_stats_.num_cache_filter_bytes_insert += usage;
      } else {
        RecordTick(statistics, BLOCK_CACHE_FILTER_ADD);
        if (redundant) {
          RecordTick(statistics, BLOCK_CACHE_FILTER_ADD_REDUNDANT);
        }
        RecordTick(statistics, BLOCK_CACHE_FILTER_BYTES_INSERT, usage);
      }
      break;

    case BlockType::kCompressionDictionary:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_compression_dict_add;
        if (redundant) {
          ++get_context->get_context_stats_
                .num_cache_compression_dict_add_redundant;
        }
        get_context->get_context_stats_
            .num_cache_compression_dict_bytes_insert += usage;
      } else {
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_ADD);
        if (redundant) {
          RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT);
        }
        RecordTick(statistics, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,
                   usage);
      }
      break;

    case BlockType::kIndex:
    case BlockType::kUserDefinedIndex:
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_index_add;
        if (redundant) {
          ++get_context->get_context_stats_.num_cache_index_add_redundant;
        }
        get_context->get_context_stats_.num_cache_index_bytes_insert += usage;
      } else {
        RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
        if (redundant) {
          RecordTick(statistics, BLOCK_CACHE_INDEX_ADD_REDUNDANT);
        }
        RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, usage);
      }
      break;

    case BlockType::kData:
    case BlockType::kProperties:
    case BlockType::kRangeDeletion:
    case BlockType::kHashIndexPrefixes:
    case BlockType::kHashIndexMetadata:
    case BlockType::kMetaIndex:
    case BlockType::kInvalid:
    default:
      // TODO: introduce dedicated tickers/statistics/counters
      // for range tombstones
      if (get_context) {
        ++get_context->get_context_stats_.num_cache_data_add;
        if (redundant) {
          ++get_context->get_context_stats_.num_cache_data_add_redundant;
        }
        get_context->get_context_stats_.num_cache_data_bytes_insert += usage;
      } else {
        RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
        if (redundant) {
          RecordTick(statistics, BLOCK_CACHE_DATA_ADD_REDUNDANT);
        }
        RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT, usage);
      }
      break;
  }
}

namespace {
// Return True if table_properties has `user_prop_name` has a `true` value
// or it doesn't contain this property (for backward compatible).
bool IsFeatureSupported(const TableProperties& table_properties,
                        const std::string& user_prop_name, Logger* info_log) {
  auto& props = table_properties.user_collected_properties;
  auto pos = props.find(user_prop_name);
  // Older version doesn't have this value set. Skip this check.
  if (pos != props.end()) {
    if (pos->second == kPropFalse) {
      return false;
    } else if (pos->second != kPropTrue) {
      ROCKS_LOG_WARN(info_log, "Property %s has invalidate value %s",
                     user_prop_name.c_str(), pos->second.c_str());
    }
  }
  return true;
}

Status GetDecompressor(const std::string& compression_name,
                       UnownedPtr<CompressionManager> compression_manager,
                       uint32_t table_format_version,
                       std::shared_ptr<Decompressor>* out_decompressor) {
  if (compression_name.empty()) {
    // Very old file (before RocksDB 4.9.0) that might contain compressed
    // blocks. Get a general decompressor (for all supported format_versions)
    auto mgr_to_use = GetBuiltinV2CompressionManager();
    *out_decompressor = mgr_to_use->GetDecompressor();
    return Status::OK();
  }
  if (FormatVersionUsesCompressionManagerName(table_format_version)) {
    constexpr char kFieldSep = ';';
    size_t separator_pos = compression_name.find_first_of(kFieldSep);
    if (separator_pos == std::string::npos) {
      return Status::Corruption(
          "Missing separator in compression_name property");
    }
    // Built with explicit CompressionManager and schema support for
    // identifying its compatibility name, which is the first field here.
    Slice compatibility_name(compression_name.data(), separator_pos);
    std::shared_ptr<CompressionManager> mgr_to_use;
    if (compression_manager) {
      // First attempt to go through the compression manager configured for
      // writing new files, for efficiency (usually correct) and not forcing
      // use of ObjectLibrary registration (dependency injection).
      mgr_to_use = compression_manager->FindCompatibleCompressionManager(
          compatibility_name);
    }
    if (mgr_to_use == nullptr) {
      ConfigOptions strict;
      strict.ignore_unknown_options = false;
      strict.ignore_unsupported_options = false;
      Status s = CompressionManager::CreateFromString(
          strict, compatibility_name.ToString(), &mgr_to_use);
      // Even though we might be able to recover from "not found" if only
      // built-in compression types are used (would be checked below), it
      // would provide misleading or unreliable success to allow that to
      // succeed.
      if (!s.ok()) {
        return s;
      }
      assert(mgr_to_use || compatibility_name == kNullptrString ||
             compatibility_name.empty());
    }

    // Second field is set of compression types actually used in the file
    size_t start_pos = separator_pos + 1;
    separator_pos = compression_name.find_first_of(kFieldSep, start_pos);
    if (UNLIKELY(separator_pos == std::string::npos)) {
      return Status::Corruption("Missing second field from compression_name");
    }
    if (UNLIKELY((separator_pos - start_pos) & 1)) {
      return Status::Corruption(
          "Second field of compression_name has odd size");
    }
    size_t count = (separator_pos - start_pos) / 2;
    auto ctypes = std::make_unique<CompressionType[]>(count);
    const char* ptr = compression_name.data() + start_pos;
    for (size_t i = 0; i < count; ++i) {
      uint64_t val = 0;
      bool success = ParseBaseChars<16>(&ptr, 2, &val);
      if (UNLIKELY(!success || val == kNoCompression ||
                   val >= kDisableCompressionOption)) {
        return Status::Corruption(
            "Error parsing second field of compression_name");
      }
      ctypes[i] = static_cast<CompressionType>(val);
    }
    if (mgr_to_use) {
      *out_decompressor = mgr_to_use->GetDecompressorForTypes(
          ctypes.get(), ctypes.get() + count);
      assert(*out_decompressor || count == 0);
    } else {
      // Compression/decompression disabled
      *out_decompressor = nullptr;
      assert(count == 0);
    }
    // Can ignore possible additional future fields
  } else {
    // No explicit CompressionManager, e.g. legacy file support where
    // decompressing with built-in CompressionManager works.
    CompressionType saved_comp_type =
        CompressionTypeFromString(compression_name);
    if (saved_comp_type == kDisableCompressionOption) {
      // Unrecognized. For RocksDB versions able to read format_version=7,
      // this is considered an error so that we can continue to evolve the
      // schema of the compression_name property and report good error
      // messages.
      return Status::Corruption("Unrecognized compression_name: " +
                                compression_name);
    } else if (saved_comp_type != kNoCompression) {
      // Use built-in compression manager
      auto mgr_to_use = GetBuiltinV2CompressionManager();
      *out_decompressor =
          mgr_to_use->GetDecompressorOptimizeFor(saved_comp_type);
    } else {
      // No compression -> decompressor not needed
      *out_decompressor = nullptr;
    }
  }
  return Status::OK();
}
}  // namespace

void BlockBasedTable::SetupBaseCacheKey(const TableProperties* properties,
                                        const std::string& cur_db_session_id,
                                        uint64_t cur_file_number,
                                        OffsetableCacheKey* out_base_cache_key,
                                        bool* out_is_stable) {
  // Use a stable cache key if sufficient data is in table properties
  std::string db_session_id;
  uint64_t file_num;
  std::string db_id;
  if (properties && !properties->db_session_id.empty() &&
      properties->orig_file_number > 0) {
    // (Newer SST file case)
    // We must have both properties to get a stable unique id because
    // CreateColumnFamilyWithImport or IngestExternalFiles can change the
    // file numbers on a file.
    db_session_id = properties->db_session_id;
    file_num = properties->orig_file_number;
    // Less critical, populated in earlier release than above
    db_id = properties->db_id;
    if (out_is_stable) {
      *out_is_stable = true;
    }
  } else {
    // (Old SST file case)
    // We use (unique) cache keys based on current identifiers. These are at
    // least stable across table file close and re-open, but not across
    // different DBs nor DB close and re-open.
    db_session_id = cur_db_session_id;
    file_num = cur_file_number;
    // Plumbing through the DB ID to here would be annoying, and of limited
    // value because of the case of VersionSet::Recover opening some table
    // files and later setting the DB ID. So we just rely on uniqueness
    // level provided by session ID.
    db_id = "unknown";
    if (out_is_stable) {
      *out_is_stable = false;
    }
  }

  // Too many tests to update to get these working
  // assert(file_num > 0);
  // assert(!db_session_id.empty());
  // assert(!db_id.empty());

  // Minimum block size is 5 bytes; therefore we can trim off two lower bits
  // from offsets. See GetCacheKey.
  *out_base_cache_key = OffsetableCacheKey(db_id, db_session_id, file_num);
}

CacheKey BlockBasedTable::GetCacheKey(const OffsetableCacheKey& base_cache_key,
                                      const BlockHandle& handle) {
  // Minimum block size is 5 bytes; therefore we can trim off two lower bits
  // from offset. This is the same scheme SimpleGen2Blob records use (see
  // OffsetableCacheKey::WithOffsetForMinSizeRecord), so blocks and embedded
  // blob records in the same file share one collision-free keyspace.
  return base_cache_key.WithOffsetForMinSizeRecord(handle.offset());
}

Status BlockBasedTable::Open(
    const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const EnvOptions& env_options, const BlockBasedTableOptions& table_options,
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    uint8_t block_protection_bytes_per_key,
    std::unique_ptr<TableReader>* table_reader, uint64_t tail_size,
    std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr,
    std::shared_ptr<AtomicCacheReservationManager> readahead_cache_res_mgr,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    UnownedPtr<CompressionManager> compression_manager,
    const bool prefetch_index_and_filter_in_cache, const bool skip_filters,
    const int level, const bool immortal_table,
    const SequenceNumber largest_seqno, const bool force_direct_prefetch,
    TailPrefetchStats* tail_prefetch_stats,
    BlockCacheTracer* const block_cache_tracer,
    size_t max_file_size_for_l0_meta_pin, const std::string& cur_db_session_id,
    uint64_t cur_file_num, UniqueId64x2 expected_unique_id,
    const bool user_defined_timestamps_persisted,
    const bool avoid_shared_metadata_cache, BlobSource* blob_source) {
  table_reader->reset();

  Status s = ValidateIndexMode(table_options.GetEffectiveIndexMode());
  if (!s.ok()) {
    return s;
  }
  Footer footer;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;

  // From read_options, retain deadline, io_timeout, rate_limiter_priority, and
  // verify_checksums. In future, we may retain more options.
  // TODO: audit more ReadOptions and do this in a way that brings attention
  // on new ReadOptions?
  ReadOptions ro;
  ro.deadline = read_options.deadline;
  ro.io_timeout = read_options.io_timeout;
  ro.rate_limiter_priority = read_options.rate_limiter_priority;
  ro.verify_checksums = read_options.verify_checksums;
  ro.io_activity = read_options.io_activity;
  ro.fill_cache = read_options.fill_cache;

  // Prefetch both index and filters, down to all partitions. The L0 fast path
  // is skipped when open-time reads must avoid the shared metadata cache.
  const bool prefetch_all = prefetch_index_and_filter_in_cache ||
                            (level == 0 && !avoid_shared_metadata_cache);
  const bool preload_all = !table_options.cache_index_and_filter_blocks;

  if (!ioptions.allow_mmap_reads && !env_options.use_mmap_reads) {
    s = PrefetchTail(ro, ioptions, file.get(), file_size, force_direct_prefetch,
                     tail_prefetch_stats, prefetch_all, preload_all,
                     &prefetch_buffer, ioptions.stats, tail_size,
                     ioptions.logger, readahead_cache_res_mgr);
    // Return error in prefetch path to users.
    if (!s.ok()) {
      return s;
    }
  } else {
    // Should not prefetch for mmap mode.
    prefetch_buffer.reset(new FilePrefetchBuffer(
        ReadaheadParams(), false /* enable */, true /* track_min_offset */));
  }

  // Read in the following order:
  //    1. Footer
  //    2. [metaindex block]
  //    3. [meta block: properties]
  //    4. [meta block: range deletion tombstone]
  //    5. [meta block: compression dictionary]
  //    6. [meta block: index]
  //    7. [meta block: filter]
  IOOptions opts;
  IODebugContext dbg;
  s = file->PrepareIOOptions(ro, opts, &dbg);
  if (s.ok()) {
    s = ReadFooterFromFile(opts, file.get(), *ioptions.fs,
                           prefetch_buffer.get(), file_size, &footer,
                           kBlockBasedTableMagicNumber, ioptions.stats);
  }
  if (!s.ok()) {
    if (s.IsCorruption()) {
      RecordTick(ioptions.statistics.get(), SST_FOOTER_CORRUPTION_COUNT);
    }
    return s;
  }
  if (!IsSupportedFormatVersionForRead(kBlockBasedTableMagicNumber,
                                       footer.format_version()) &&
      !TEST_AllowUnsupportedFormatVersion()) {
    return Status::Corruption(
        "Unknown Footer version. Maybe this file was created with newer "
        "version of RocksDB?");
  }

  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};
  Rep* rep = new BlockBasedTable::Rep(
      ioptions, env_options, table_options, internal_comparator, skip_filters,
      file_size, level, immortal_table, user_defined_timestamps_persisted);
  rep->file = std::move(file);
  rep->footer = footer;
  rep->blob_source_ = blob_source;
  rep->readahead_cache_res_mgr = std::move(readahead_cache_res_mgr);

  // For fully portable/stable cache keys, we need to read the properties
  // block before setting up cache keys. TODO: consider setting up a bootstrap
  // cache key for PersistentCache to use for metaindex and properties blocks.
  rep->persistent_cache_options = PersistentCacheOptions();

  // Meta-blocks are not dictionary compressed. Explicitly set the dictionary
  // handle to null, otherwise it may be seen as uninitialized during the below
  // meta-block reads.
  rep->compression_dict_handle = BlockHandle::NullBlockHandle();

  rep->create_context.protection_bytes_per_key = block_protection_bytes_per_key;
  // Read metaindex
  std::unique_ptr<BlockBasedTable> new_table(
      new BlockBasedTable(rep, block_cache_tracer));
  std::unique_ptr<Block> metaindex;
  std::unique_ptr<InternalIterator> metaindex_iter;
  s = new_table->ReadMetaIndexBlock(ro, prefetch_buffer.get(), &metaindex,
                                    &metaindex_iter);
  if (!s.ok()) {
    return s;
  }

  // Populates table_properties and some fields that depend on it,
  // such as index_type.
  s = new_table->ReadPropertiesBlock(ro, prefetch_buffer.get(),
                                     metaindex_iter.get(), largest_seqno);
  if (!s.ok()) {
    return s;
  }
  if ((table_options.GetEffectiveIndexMode() ==
           BlockBasedTableOptions::IndexMode::kCustomDefault ||
       table_options.GetEffectiveIndexMode() ==
           BlockBasedTableOptions::IndexMode::kCustomOnly) &&
      table_options.user_defined_index_factory == nullptr) {
    return Status::InvalidArgument(
        "index_mode kCustomDefault/kCustomOnly requires "
        "user_defined_index_factory");
  }
  const bool requires_custom_reader =
      footer.HasIncompatibleFeature(kFooterFeatureRequireUserDefinedIndex) ||
      rep->table_properties->standard_index_is_stub != 0;
  if (requires_custom_reader &&
      (table_options.GetEffectiveIndexMode() ==
           BlockBasedTableOptions::IndexMode::kStandardOnly ||
       table_options.user_defined_index_factory == nullptr)) {
    return Status::InvalidArgument(
        "SST has only a stub standard index; configure the matching "
        "user_defined_index_factory to read it");
  }

  // Read compression metadata and configure decompressor
  s = GetDecompressor(
      rep->table_properties ? rep->table_properties->compression_name
                            : std::string{},
      compression_manager, footer.format_version(), &rep->decompressor);
  if (!s.ok()) {
    return s;
  }

  // Populate BlockCreateContext
  rep->create_context = BlockCreateContext(
      &rep->table_options, &rep->ioptions, rep->ioptions.stats,
      rep->decompressor.get(), block_protection_bytes_per_key,
      rep->internal_comparator.user_comparator(), rep->index_value_is_full,
      rep->index_has_first_key,
      FormatVersionUsesValueDeltaEscape(rep->footer.format_version()),
      rep->data_block_restart_interval, rep->index_block_restart_interval);
  // Blocks written by RocksDB 11.10 through 11.13 with
  // ReverseBytewiseComparator can need full-key seeks (see
  // BlockCreateContext::check_common_prefix).
  if (rep->internal_comparator.user_comparator() ==
      ReverseBytewiseComparator()) {
    rep->create_context.check_common_prefix = true;
    rep->create_context.index_key_includes_seq = rep->index_key_includes_seq;
  }

  // Check expected unique id if provided
  if (expected_unique_id != kNullUniqueId64x2) {
    auto props = rep->table_properties;
    if (!props) {
      return Status::Corruption("Missing table properties on file " +
                                std::to_string(cur_file_num) +
                                " with known unique ID");
    }
    UniqueId64x2 actual_unique_id{};
    s = GetSstInternalUniqueId(props->db_id, props->db_session_id,
                               props->orig_file_number, &actual_unique_id,
                               /*force*/ true);
    assert(s.ok());  // because force=true
    if (expected_unique_id != actual_unique_id) {
      return Status::Corruption(
          "Mismatch in unique ID on table file " +
          std::to_string(cur_file_num) +
          ". Expected: " + InternalUniqueIdToHumanString(&expected_unique_id) +
          " Actual: " + InternalUniqueIdToHumanString(&actual_unique_id));
    }
    TEST_SYNC_POINT_CALLBACK("BlockBasedTable::Open::PassedVerifyUniqueId",
                             &actual_unique_id);
  } else {
    TEST_SYNC_POINT_CALLBACK("BlockBasedTable::Open::SkippedVerifyUniqueId",
                             nullptr);
    if (ioptions.verify_sst_unique_id_in_manifest && ioptions.logger) {
      // A crude but isolated way of reporting unverified files. This should not
      // be an ongoing concern so doesn't deserve a place in Statistics IMHO.
      static std::atomic<uint64_t> unverified_count{0};
      auto prev_count =
          unverified_count.fetch_add(1, std::memory_order_relaxed);
      if (prev_count == 0) {
        ROCKS_LOG_WARN(
            ioptions.logger,
            "At least one SST file opened without unique ID to verify: %" PRIu64
            ".sst",
            cur_file_num);
      } else if (prev_count % 1000 == 0) {
        ROCKS_LOG_WARN(
            ioptions.logger,
            "Another ~1000 SST files opened without unique ID to verify");
      }
    }
  }

  // Set up prefix extracto as needed
  bool force_null_table_prefix_extractor = false;
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTable::Open::ForceNullTablePrefixExtractor",
      &force_null_table_prefix_extractor);
  if (force_null_table_prefix_extractor) {
    assert(!rep->table_prefix_extractor);
  } else if (!PrefixExtractorChangedHelper(rep->table_properties.get(),
                                           prefix_extractor.get())) {
    // Establish fast path for unchanged prefix_extractor
    rep->table_prefix_extractor = prefix_extractor;
  } else {
    // Current prefix_extractor doesn't match table
    //**TODO: If/When the DBOptions has a registry in it, the ConfigOptions
    // will need to use it
    ConfigOptions config_options;
    Status st = SliceTransform::CreateFromString(
        config_options, rep->table_properties->prefix_extractor_name,
        &(rep->table_prefix_extractor));
    if (!st.ok()) {
      //**TODO: Should this be error be returned or swallowed?
      ROCKS_LOG_ERROR(rep->ioptions.logger,
                      "Failed to create prefix extractor[%s]: %s",
                      rep->table_properties->prefix_extractor_name.c_str(),
                      st.ToString().c_str());
    }
  }

  // With properties loaded, we can set up portable/stable cache keys
  SetupBaseCacheKey(rep->table_properties.get(), cur_db_session_id,
                    cur_file_num, &rep->base_cache_key);

  rep->persistent_cache_options =
      PersistentCacheOptions(rep->table_options.persistent_cache,
                             rep->base_cache_key, rep->ioptions.stats);

  s = new_table->ReadRangeDelBlock(ro, prefetch_buffer.get(),
                                   metaindex_iter.get(), internal_comparator,
                                   &lookup_context);
  if (!s.ok()) {
    return s;
  }
  rep->verify_checksum_set_on_open = ro.verify_checksums;
  s = new_table->PrefetchIndexAndFilterBlocks(
      ro, prefetch_buffer.get(), metaindex_iter.get(), new_table.get(),
      prefetch_all, table_options, level, file_size,
      max_file_size_for_l0_meta_pin, avoid_shared_metadata_cache,
      &lookup_context);

  if (s.ok()) {
    // Update tail prefetch stats
    assert(prefetch_buffer.get() != nullptr);
    if (tail_prefetch_stats != nullptr) {
      assert(prefetch_buffer->min_offset_read() < file_size);
      tail_prefetch_stats->RecordEffectiveSize(
          static_cast<size_t>(file_size) - prefetch_buffer->min_offset_read());
    }
  }

  if (s.ok() && table_reader_cache_res_mgr) {
    std::size_t mem_usage = new_table->ApproximateMemoryUsage();
    s = table_reader_cache_res_mgr->MakeCacheReservation(
        mem_usage, &(rep->table_reader_cache_res_handle));
    if (s.IsMemoryLimit()) {
      s = Status::MemoryLimit(
          "Can't allocate " +
          kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
              CacheEntryRole::kBlockBasedTableReader)] +
          " due to memory limit based on "
          "cache capacity for memory allocation");
    }
  }

  if (s.ok()) {
    *table_reader = std::move(new_table);
  }
  return s;
}

Status BlockBasedTable::PrefetchTail(
    const ReadOptions& ro, const ImmutableOptions& ioptions,
    RandomAccessFileReader* file, uint64_t file_size,
    bool force_direct_prefetch, TailPrefetchStats* tail_prefetch_stats,
    const bool prefetch_all, const bool preload_all,
    std::unique_ptr<FilePrefetchBuffer>* prefetch_buffer, Statistics* stats,
    uint64_t tail_size, Logger* const logger,
    const std::shared_ptr<AtomicCacheReservationManager>&
        readahead_cache_res_mgr) {
  assert(tail_size <= file_size);

  size_t tail_prefetch_size = 0;
  if (tail_size != 0) {
    tail_prefetch_size = tail_size;
  } else {
    // Fallback for SST files, for which tail size is not recorded in the
    // manifest. Eventually, this fallback might be removed, so it's
    // better to make sure that such SST files get compacted.
    // See https://github.com/meta/rocksdb/issues/12664
    if (tail_prefetch_stats != nullptr) {
      // Multiple threads may get a 0 (no history) when running in parallel,
      // but it will get cleared after the first of them finishes.
      tail_prefetch_size = tail_prefetch_stats->GetSuggestedPrefetchSize();
    }
    if (tail_prefetch_size == 0) {
      // Before read footer, readahead backwards to prefetch data. Do more
      // readahead if we're going to read index/filter.
      // TODO: This may incorrectly select small readahead in case partitioned
      // index/filter is enabled and top-level partition pinning is enabled.
      // That's because we need to issue readahead before we read the
      // properties, at which point we don't yet know the index type.
      tail_prefetch_size = prefetch_all || preload_all ? 512 * 1024 : 4 * 1024;

      ROCKS_LOG_WARN(
          logger,
          "[%s] Tail prefetch size %zu is calculated based on heuristics.",
          file->file_name().c_str(), tail_prefetch_size);
    } else {
      ROCKS_LOG_WARN(logger,
                     "[%s] Tail prefetch size %zu is calculated based on "
                     "TailPrefetchStats.",
                     file->file_name().c_str(), tail_prefetch_size);
    }
    TEST_SYNC_POINT("BlockBasedTable::PrefetchTail::TaiSizeNotRecorded");
  }
  size_t prefetch_off;
  size_t prefetch_len;
  if (file_size < tail_prefetch_size) {
    prefetch_off = 0;
    prefetch_len = static_cast<size_t>(file_size);
  } else {
    prefetch_off = static_cast<size_t>(file_size - tail_prefetch_size);
    prefetch_len = tail_prefetch_size;
  }

#ifndef NDEBUG
  std::pair<size_t*, size_t*> prefetch_off_len_pair = {&prefetch_off,
                                                       &prefetch_len};
  TEST_SYNC_POINT_CALLBACK("BlockBasedTable::Open::TailPrefetchLen",
                           &prefetch_off_len_pair);
#endif  // NDEBUG

  IOOptions opts;
  IODebugContext dbg;
  Status s = file->PrepareIOOptions(ro, opts, &dbg);
  // Try file system prefetch
  if (s.ok() && !file->use_direct_io() && !force_direct_prefetch) {
    if (!file->Prefetch(opts, prefetch_off, prefetch_len).IsNotSupported()) {
      prefetch_buffer->reset(new FilePrefetchBuffer(
          ReadaheadParams(), false /* enable */, true /* track_min_offset */));
      return Status::OK();
    }
  }

  // Use `FilePrefetchBuffer`
  prefetch_buffer->reset(new FilePrefetchBuffer(
      ReadaheadParams(), true /* enable */, true /* track_min_offset */,
      ioptions.fs.get() /* fs */, nullptr /* clock */, stats,
      /* readahead_cb */ nullptr,
      FilePrefetchBufferUsage::kTableOpenPrefetchTail));
  if (readahead_cache_res_mgr) {
    (*prefetch_buffer)->SetCacheReservationManager(readahead_cache_res_mgr);
  }

  if (s.ok()) {
    s = (*prefetch_buffer)->Prefetch(opts, file, prefetch_off, prefetch_len);
  }
  return s;
}

Status BlockBasedTable::ReadPropertiesBlock(
    const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
    InternalIterator* meta_iter, const SequenceNumber largest_seqno) {
  Status s;
  BlockHandle handle;
  s = FindOptionalMetaBlock(meta_iter, kPropertiesBlockName, &handle);

  if (!s.ok()) {
    return s;
  } else if (handle.IsNull()) {
    return Status::Corruption("Cannot find Properties block from file.");
  }

  s = meta_iter->status();
  std::unique_ptr<TableProperties> table_properties;
  if (s.ok()) {
    s = ReadTablePropertiesHelper(
        ro, handle, rep_->file.get(), prefetch_buffer, rep_->footer,
        rep_->ioptions, &table_properties, nullptr /* memory_allocator */);
  }

  if (!s.ok()) {
    return s;
  }

  assert(table_properties != nullptr);
  rep_->table_properties = std::move(table_properties);

  rep_->data_block_restart_interval = static_cast<uint32_t>(
      rep_->table_properties->data_block_restart_interval);
  rep_->index_block_restart_interval = static_cast<uint32_t>(
      rep_->table_properties->index_block_restart_interval);
  rep_->separate_key_value_in_data_block =
      rep_->table_properties->separate_key_value_in_data_block > 0;

  rep_->index_has_first_key =
      rep_->index_type == BlockBasedTableOptions::kBinarySearchWithFirstKey;

  s = rep_->seqno_to_time_mapping.DecodeFrom(
      rep_->table_properties->seqno_to_time_mapping);
  if (!s.ok()) {
    ROCKS_LOG_WARN(rep_->ioptions.logger,
                   "Problem reading or processing seqno-to-time mapping: %s",
                   s.ToString().c_str());
  }

  // Read the table properties
  rep_->whole_key_filtering &= IsFeatureSupported(
      *(rep_->table_properties),
      BlockBasedTablePropertyNames::kWholeKeyFiltering, rep_->ioptions.logger);
  rep_->prefix_filtering &= IsFeatureSupported(
      *(rep_->table_properties), BlockBasedTablePropertyNames::kPrefixFiltering,
      rep_->ioptions.logger);

  rep_->index_key_includes_seq =
      rep_->table_properties->index_key_is_user_key == 0;
  rep_->index_value_is_full =
      rep_->table_properties->index_value_is_delta_encoded == 0;

  // Read index_type from properties (required for format_version >= 2)
  auto& props = rep_->table_properties->user_collected_properties;
  // The presence of the embedded blob stats property signals that this SST
  // contains same-file ("embedded") blob records. The counter values are
  // diagnostic only; only presence matters for correctness.
  rep_->has_embedded_blobs =
      props.find(kEmbeddedBlobSstStatsPropertyName) != props.end();
  if (rep_->has_embedded_blobs) {
    rep_->embedded_blob_decompressor =
        GetBuiltinV2CompressionManager()->GetDecompressor();
  }
  auto index_type_pos = props.find(BlockBasedTablePropertyNames::kIndexType);
  if (index_type_pos == props.end()) {
    return Status::Corruption("Missing index type property");
  }
  rep_->index_type = static_cast<BlockBasedTableOptions::IndexType>(
      DecodeFixed32(index_type_pos->second.c_str()));
  auto min_ts_pos = props.find("rocksdb.timestamp_min");
  if (min_ts_pos != props.end()) {
    rep_->min_timestamp = Slice(min_ts_pos->second);
  }
  auto max_ts_pos = props.find("rocksdb.timestamp_max");
  if (max_ts_pos != props.end()) {
    rep_->max_timestamp = Slice(max_ts_pos->second);
  }
  rep_->index_has_first_key =
      rep_->index_type == BlockBasedTableOptions::kBinarySearchWithFirstKey;

  s = GetGlobalSequenceNumber(*(rep_->table_properties), largest_seqno,
                              &(rep_->global_seqno));
  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep_->ioptions.logger, "%s", s.ToString().c_str());
  }
  return s;
}

bool BlockBasedTable::HasEmbeddedBlobRecords() const {
  return rep_->has_embedded_blobs;
}

Status BlockBasedTable::ValidateEmbeddedBlobIndex(const BlobIndex& blob_index,
                                                  size_t* payload_size,
                                                  size_t* record_size) const {
  assert(payload_size != nullptr);
  assert(record_size != nullptr);
  if (!blob_index.IsSameFile()) {
    return Status::InvalidArgument("Blob index does not reference this file");
  }
  if (blob_index.HasTTL()) {
    return Status::Corruption(
        "Embedded blob index must not contain TTL metadata");
  }
  if (!rep_->has_embedded_blobs) {
    return Status::Corruption(
        "Same-file blob index found without embedded blob records");
  }

  const uint64_t payload_size_u64 = blob_index.size();
  if (payload_size_u64 > kMaxSimpleGen2BlobPayloadSize ||
      !CompressionTypeSupported(blob_index.compression())) {
    return Status::Corruption(
        "Embedded blob record size or compression is not supported");
  }
  const uint64_t record_size_u64 =
      payload_size_u64 + kSimpleGen2BlobTrailerSize;
  // The per-record context checksum is the correctness guard for same-file
  // references; this cheap bound just prevents a wild read past EOF (the range
  // pre-check no longer exists now that records are interleaved with blocks).
  const uint64_t record_offset = blob_index.offset();
  if (record_offset > rep_->file_size ||
      record_size_u64 > rep_->file_size - record_offset) {
    return Status::Corruption("Embedded blob record extends past file size");
  }

  *payload_size = static_cast<size_t>(payload_size_u64);
  *record_size = static_cast<size_t>(record_size_u64);
  return Status::OK();
}

Status BlockBasedTable::ResolveEmbeddedBlob(const ReadOptions& read_options,
                                            const BlobIndex& blob_index,
                                            std::string* value) const {
  assert(value != nullptr);
  size_t payload_size = 0;
  size_t record_size = 0;
  Status s = ValidateEmbeddedBlobIndex(blob_index, &payload_size, &record_size);
  if (!s.ok()) {
    return s;
  }
  // Direct file read (no blob cache): a block-cache-only read cannot be served.
  if (read_options.read_tier == kBlockCacheTier) {
    return Status::Incomplete(
        "Cannot read embedded blob in block-cache-only mode");
  }
  if (blob_index.compression() == kNoCompression) {
    value->resize(record_size);
    s = ReadAndVerifySimpleGen2BlobRecord(
        read_options, rep_->file.get(), blob_index.offset(), payload_size,
        record_size, rep_->footer.checksum_type(),
        rep_->footer.base_context_checksum(), blob_index.compression(),
        &(*value)[0]);
    if (s.ok()) {
      value->resize(payload_size);
    }
    return s;
  }
  CacheAllocationPtr stored_record = AllocateBlock(record_size, nullptr);
  s = ReadAndVerifySimpleGen2BlobRecord(
      read_options, rep_->file.get(), blob_index.offset(), payload_size,
      record_size, rep_->footer.checksum_type(),
      rep_->footer.base_context_checksum(), blob_index.compression(),
      stored_record.get());
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<BlobContents> contents;
  s = UncompressBlobContentsIfNeeded(
      Slice(stored_record.get(), payload_size), blob_index.compression(),
      rep_->embedded_blob_decompressor.get(), /*allocator=*/nullptr,
      rep_->ioptions.clock, rep_->ioptions.stats, &contents);
  if (!s.ok()) {
    return s;
  }
  value->assign(contents->data().data(), contents->data().size());
  return Status::OK();
}

Status BlockBasedTable::ResolveEmbeddedBlobPinned(
    const ReadOptions& read_options, const BlobIndex& blob_index,
    PinnableSlice* value) const {
  assert(value != nullptr);
  size_t payload_size = 0;
  size_t record_size = 0;
  Status s = ValidateEmbeddedBlobIndex(blob_index, &payload_size, &record_size);
  if (!s.ok()) {
    return s;
  }
  // Direct file read (no blob cache): a block-cache-only read cannot be served.
  if (read_options.read_tier == kBlockCacheTier) {
    return Status::Incomplete(
        "Cannot read embedded blob in block-cache-only mode");
  }
  CacheAllocationPtr stored_record = AllocateBlock(record_size, nullptr);
  s = ReadAndVerifySimpleGen2BlobRecord(
      read_options, rep_->file.get(), blob_index.offset(), payload_size,
      record_size, rep_->footer.checksum_type(),
      rep_->footer.base_context_checksum(), blob_index.compression(),
      stored_record.get());
  if (!s.ok()) {
    return s;
  }
  return DecompressAndPinEmbeddedBlob(std::move(stored_record), payload_size,
                                      blob_index.compression(), value);
}

void BlockBasedTable::DropUserCollectedProperty(const std::string& name) {
  // It replaces rep_->table_properties, which readers share, so it must run
  // before any read (see MarkReadStarted()).
  assert(!rep_->read_started.load(std::memory_order_relaxed));
  if (rep_->table_properties == nullptr ||
      rep_->table_properties->user_collected_properties.count(name) == 0) {
    return;
  }
  std::shared_ptr<TableProperties> properties =
      std::make_shared<TableProperties>(*rep_->table_properties);
  properties->user_collected_properties.erase(name);
  rep_->table_properties = std::move(properties);
}

Status BlockBasedTable::DecompressEmbeddedBlob(
    CacheAllocationPtr stored, size_t payload_size, CompressionType compression,
    MemoryAllocator* allocator, std::unique_ptr<BlobContents>* contents) const {
  if (compression == kNoCompression) {
    contents->reset(new BlobContents(std::move(stored), payload_size));
    return Status::OK();
  }
  return UncompressBlobContentsIfNeeded(
      Slice(stored.get(), payload_size), compression,
      rep_->embedded_blob_decompressor.get(), allocator, rep_->ioptions.clock,
      rep_->ioptions.stats, contents);
}

Status BlockBasedTable::DecompressAndPinEmbeddedBlob(
    CacheAllocationPtr stored, size_t payload_size, CompressionType compression,
    PinnableSlice* value) const {
  std::unique_ptr<BlobContents> contents;
  Status s =
      DecompressEmbeddedBlob(std::move(stored), payload_size, compression,
                             /*allocator=*/nullptr, &contents);
  return s.ok() ? PinBlobContents(std::move(contents), value) : s;
}

Status BlockBasedTable::ResolveEmbeddedBlobCached(
    const ReadOptions& read_options, const BlobIndex& blob_index,
    PinnableSlice* value) const {
  assert(value != nullptr);
  assert(rep_->blob_source_ != nullptr);
  size_t payload_size = 0;
  size_t record_size = 0;
  Status s = ValidateEmbeddedBlobIndex(blob_index, &payload_size, &record_size);
  if (!s.ok()) {
    return s;
  }

  // BlobSource derives the cache key from the SimpleGen2Blob format (the same
  // offset scheme as the SST's data blocks; see GetSimpleGen2BlobCacheKey), so
  // we only pass the SST's base cache key. This keeps embedded blob records
  // collision-free with data blocks even when blob_cache == block_cache.
  return rep_->blob_source_->GetSimpleGen2Blob(
      read_options, rep_->base_cache_key, rep_->file.get(), blob_index.offset(),
      payload_size, rep_->footer.checksum_type(),
      rep_->footer.base_context_checksum(), blob_index.compression(), value,
      /*bytes_read=*/nullptr);
}

Status BlockBasedTable::GetSameFileBlob(const ReadOptions& read_options,
                                        const BlobIndex& blob_index,
                                        uint64_t range_offset,
                                        size_t range_length,
                                        BlobVerifyPolicy verify_policy,
                                        PinnableSlice* value) const {
  MarkReadStarted();
  if (range_length != kWholeBlobLength) {
    if (blob_index.compression() != kNoCompression) {
      // As in base RocksDB, a range of a compressed value is not supported: a
      // byte range cannot be decompressed alone. Callers read the whole value
      // and slice it.
      return Status::Corruption("Cannot range-read a compressed blob");
    }
    // An uncompressed range reads only the range bytes, with no checksum check
    // and no cache fill. Callers send only strict parts of a value (see
    // ReadPathBlobResolver::ResolveColumnRange), and a range is never turned
    // into a whole read. A DB-owned SST reads the range through BlobSource. A
    // reader without a BlobSource, such as a relocation table (opened as a
    // standalone block-based reader), reads the range directly with
    // ReadSimpleGen2BlobRange.
    assert(verify_policy != BlobVerifyPolicy::kVerifyIfPresent);
    if (rep_->blob_source_ != nullptr) {
      return ResolveEmbeddedBlobRangeCached(read_options, blob_index,
                                            range_offset, range_length, value);
    }
    size_t payload_size = 0;
    size_t record_size = 0;
    Status status =
        ValidateEmbeddedBlobIndex(blob_index, &payload_size, &record_size);
    if (!status.ok()) {
      return status;
    }
    if (range_offset > payload_size ||
        range_length > payload_size - range_offset) {
      return Status::InvalidArgument("Embedded blob range is out of bounds");
    }
    if (read_options.read_tier == kBlockCacheTier) {
      return Status::Incomplete(
          "Cannot read embedded blob in block-cache-only mode");
    }
    CacheAllocationPtr buffer =
        AllocateBlock(range_length, /*allocator=*/nullptr);
    status = ReadSimpleGen2BlobRange(
        read_options, rep_->file.get(), blob_index.offset(), payload_size,
        range_offset, range_length, blob_index.compression(), buffer.get());
    if (!status.ok()) {
      return status;
    }
    value->Reset();
    return PinBlobContents(
        std::make_unique<BlobContents>(std::move(buffer), range_length), value);
  }

  // Whole-value read: prefer the blob-cache-backed read (BLOB_DB_* stats) when
  // a BlobSource is wired, otherwise a direct pinned read
  // (SstFileReader/sst_dump/repair). For kVerifyIfPresent, force whole-record
  // checksum verification even when ReadOptions::verify_checksums is off.
  if (verify_policy == BlobVerifyPolicy::kVerifyIfPresent &&
      !read_options.verify_checksums) {
    ReadOptions verify_read_options = read_options;
    verify_read_options.verify_checksums = true;
    return rep_->blob_source_ != nullptr
               ? ResolveEmbeddedBlobCached(verify_read_options, blob_index,
                                           value)
               : ResolveEmbeddedBlobPinned(verify_read_options, blob_index,
                                           value);
  }
  return rep_->blob_source_ != nullptr
             ? ResolveEmbeddedBlobCached(read_options, blob_index, value)
             : ResolveEmbeddedBlobPinned(read_options, blob_index, value);
}

Status BlockBasedTable::ResolveEmbeddedBlobRangeCached(
    const ReadOptions& read_options, const BlobIndex& blob_index,
    uint64_t range_offset, size_t range_length, PinnableSlice* value) const {
  assert(value != nullptr);
  assert(rep_->blob_source_ != nullptr);
  size_t payload_size = 0;
  size_t record_size = 0;
  // A block-cache-only read is handled after the blob-cache probe inside
  // GetSimpleGen2BlobRange (a cached whole payload can still serve a sub-range;
  // only a genuine miss yields Incomplete), matching the separate-file
  // GetBlobRange path.
  Status s = ValidateEmbeddedBlobIndex(blob_index, &payload_size, &record_size);
  if (!s.ok()) {
    return s;
  }

  return rep_->blob_source_->GetSimpleGen2BlobRange(
      read_options, rep_->base_cache_key, rep_->file.get(), blob_index.offset(),
      payload_size, rep_->footer.checksum_type(),
      rep_->footer.base_context_checksum(), blob_index.compression(),
      range_offset, range_length, value, /*bytes_read=*/nullptr);
}

void BlockBasedTable::RetrieveDataBlocks(
    const ReadOptions& read_options,
    const autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE>& handles,
    BlockCacheLookupContext* lookup_context, CachableEntry<Block_kData>* blocks,
    Status* statuses) const {
  assert(!handles.empty());
  CachableEntry<DecompressorDict> dict;
  Status dict_status;
  if (rep_->uncompression_dict_reader) {
    dict_status =
        rep_->uncompression_dict_reader->GetOrReadUncompressionDictionary(
            /*prefetch_buffer=*/nullptr, read_options,
            /*get_context=*/nullptr, lookup_context, &dict);
  }
  TEST_SYNC_POINT_CALLBACK("BlockBasedTable::MultiGetRawEntries:DictStatus",
                           &dict_status);
  if (!dict_status.ok()) {
    std::fill_n(statuses, handles.size(), dict_status);
  } else {
    const UnownedPtr<Decompressor> decomp =
        dict.GetValue() ? dict.GetValue()->decompressor_.get()
                        : rep_->decompressor.get();
    // Look up each block in the block cache. A hit, a cache error, or a
    // miss without I/O (kBlockCacheTier, which fails the block with
    // Incomplete) needs no read: its handle becomes null. The other handles
    // are the missing blocks.
    autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE> missing = handles;
    size_t missing_bytes = 0;
    size_t missing_count = 0;
    std::array<bool, MultiGetContext::MAX_BATCH_SIZE> cache_hit{};
    BlockCacheInterface<Block_kData> block_cache{
        rep_->table_options.block_cache.get()};
    for (size_t b = 0; b < handles.size(); ++b) {
      if (block_cache) {
        const CacheKey cache_key =
            GetCacheKey(rep_->base_cache_key, handles[b]);
        statuses[b] =
            GetDataBlockFromCache(cache_key.AsSlice(), block_cache, &blocks[b],
                                  /*get_context=*/nullptr, decomp);
        if (!statuses[b].ok() || blocks[b].GetValue() != nullptr) {
          cache_hit[b] = statuses[b].ok();
          missing[b] = BlockHandle::NullBlockHandle();
          continue;
        }
      }
      if (read_options.read_tier == kBlockCacheTier) {
        statuses[b] = Status::Incomplete("no blocking io");
        missing[b] = BlockHandle::NullBlockHandle();
        continue;
      }
      missing_bytes += BlockSizeWithTrailer(handles[b]);
      ++missing_count;
    }
    // Read the missing blocks, verify and decompress them, and insert them
    // into the block cache. Two or more missing blocks are read with one
    // batched read (RetrieveMultipleBlocks). One missing block (for example,
    // one Get of a relocated value) takes the normal block read path, a
    // plain positional read, without a second cache lookup (the lookup above
    // already missed).
    std::array<Status, MultiGetContext::MAX_BATCH_SIZE> read_status;
    if (missing_count > 1) {
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTable::MultiGetRawEntries:RetrieveMultipleBlocks",
          &missing);
      char* scratch = nullptr;
      std::unique_ptr<char[]> scratch_owner;
      bool use_fs_scratch = false;
      if (!rep_->file->use_direct_io()) {
        use_fs_scratch = CheckFSFeatureSupport(rep_->ioptions.fs.get(),
                                               FSSupportedOps::kFSBuffer);
      }
      if (!use_fs_scratch && !rep_->file->use_direct_io() &&
          rep_->decompressor) {
        scratch_owner.reset(new char[missing_bytes]);
        scratch = scratch_owner.get();
      }
      RetrieveMultipleBlocks(read_options, /*batch=*/nullptr, &missing,
                             read_status.data(), blocks, scratch, decomp,
                             use_fs_scratch);
    }
    for (size_t b = 0; b < handles.size(); ++b) {
      if (missing[b].IsNull()) {
        continue;
      }
      if (missing_count == 1) {
        blocks[b].Reset();
        statuses[b] = RetrieveBlock(
            /*prefetch_buffer=*/nullptr, read_options, missing[b], decomp,
            &blocks[b], /*get_context=*/nullptr, lookup_context,
            /*for_compaction=*/false, /*use_cache=*/true,
            /*async_read=*/false, /*use_block_cache_for_lookup=*/false);
      } else {
        statuses[b] = read_status[b];
      }
    }
    // The entries of blocks that were not read in the batch stay unused.
    for (Status& status : read_status) {
      status.PermitUncheckedError();
    }
    // Write a block cache trace record for each cache hit and each block of
    // the batched read. A single missing block went through RetrieveBlock(),
    // which writes its own record.
    if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled() &&
        lookup_context) {
      for (size_t b = 0; b < handles.size(); ++b) {
        const bool traced_by_read = missing_count == 1 && !missing[b].IsNull();
        if (traced_by_read || !statuses[b].ok() ||
            blocks[b].GetValue() == nullptr) {
          continue;
        }
        const CacheKey cache_key =
            GetCacheKey(rep_->base_cache_key, handles[b]);
        SaveLookupContextOrTraceRecord(cache_key.AsSlice(), cache_hit[b],
                                       read_options, blocks[b].GetValue(),
                                       lookup_context);
      }
    }
  }
}

void BlockBasedTable::MultiGetRawEntriesImpl(const ReadOptions& read_options,
                                             size_t num_keys, const Slice* keys,
                                             RawEntryCallback on_entry_fn,
                                             const void* on_entry_arg) const {
  MarkReadStarted();
  const auto on_entry = [&](size_t key_index, const Status& status,
                            const Slice& internal_key, const Slice& value) {
    on_entry_fn(on_entry_arg, key_index, status, internal_key, value);
  };
  if (num_keys > MultiGetContext::MAX_BATCH_SIZE) {
    // The arrays below hold one batch. Fail a larger batch instead of
    // writing past them.
    for (size_t i = 0; i < num_keys; ++i) {
      on_entry(i, Status::InvalidArgument("Too many keys in a raw lookup"),
               Slice(), Slice());
    }
    return;
  }
  if (num_keys == 0) {
    return;
  }
  // Raw reads of a blob relocation file. The relocation reader checks the raw
  // values itself.
  BlockCacheLookupContext lookup_context{
      TableReaderCaller::kBlobRelocationFileRead};
  IndexBlockIter iiter_on_stack;
  InternalIteratorBase<IndexValue>* const iiter = NewIndexIterator(
      read_options, /*need_upper_bound_check=*/false, &iiter_on_stack,
      /*get_context=*/nullptr, &lookup_context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_owner;
  if (iiter != &iiter_on_stack) {
    iiter_owner.reset(iiter);
  }

  // 1. Find the data block of each key. Sorted keys in one block are adjacent,
  // so each distinct block is listed once.
  constexpr size_t kNoBlock = std::numeric_limits<size_t>::max();
  std::array<size_t, MultiGetContext::MAX_BATCH_SIZE> key_block;
  std::array<Status, MultiGetContext::MAX_BATCH_SIZE> key_status;
  autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE> handles;
  for (size_t i = 0; i < num_keys; ++i) {
    assert(i == 0 ||
           rep_->internal_comparator.Compare(keys[i - 1], keys[i]) <= 0);
    key_block[i] = kNoBlock;
    iiter->Seek(keys[i]);
    if (!iiter->Valid()) {
      key_status[i] =
          iiter->status().ok() ? Status::NotFound() : iiter->status();
      continue;
    }
    const BlockHandle handle = iiter->value().handle;
    if (handles.empty() || handles.back().offset() != handle.offset()) {
      handles.push_back(handle);
    }
    key_block[i] = handles.size() - 1;
  }

  // 2. Look up each block in the block cache, and read the missing blocks.
  std::array<CachableEntry<Block_kData>, MultiGetContext::MAX_BATCH_SIZE>
      blocks;
  std::array<Status, MultiGetContext::MAX_BATCH_SIZE> block_status;
  if (!handles.empty()) {
    RetrieveDataBlocks(read_options, handles, &lookup_context, blocks.data(),
                       block_status.data());
  }

  // 3. Search each key in its block.
  DataBlockIter biter;
  size_t iter_block = kNoBlock;
  for (size_t i = 0; i < num_keys; ++i) {
    const size_t b = key_block[i];
    if (b == kNoBlock) {
      on_entry(i, key_status[i], Slice(), Slice());
      continue;
    }
    if (!block_status[b].ok()) {
      on_entry(i, block_status[b], Slice(), Slice());
      continue;
    }
    if (iter_block != b) {
      biter.Invalidate(Status::OK());
      NewDataBlockIterator<DataBlockIter>(read_options, blocks[b].As<Block>(),
                                          &biter, block_status[b]);
      iter_block = b;
    }
    // SeekForGet uses the data block hash index when the table has one;
    // without it, it finds the first key at or after the lookup key (blob
    // relocation files look up values inside a location entry this way).
    if (!biter.SeekForGet(keys[i]) || !biter.Valid()) {
      on_entry(i, biter.status().ok() ? Status::NotFound() : biter.status(),
               Slice(), Slice());
      continue;
    }
    on_entry(i, Status::OK(), biter.key(), biter.value());
  }
  // The callbacks get these statuses by reference, and the entries past the
  // keys and blocks of this batch are never set.
  for (size_t i = 0; i < key_status.size(); ++i) {
    key_status[i].PermitUncheckedError();
    block_status[i].PermitUncheckedError();
  }
}

void BlockBasedTable::MultiGetSameFileBlob(
    const ReadOptions& read_options, size_t num_reads,
    SameFileBlobReadRequest* reqs) const {
  MarkReadStarted();
  if (rep_->blob_source_ == nullptr) {
    struct WholePending {
      size_t request_index = 0;
      size_t payload_size = 0;
      size_t record_size = 0;
      CacheAllocationPtr buffer;
    };
    struct RangePending {
      size_t request_index = 0;
      size_t range_length = 0;
      CacheAllocationPtr buffer;
    };

    std::vector<WholePending> whole_pending;
    std::vector<RangePending> range_pending;
    std::vector<SimpleGen2RecordReadRequest> whole_reads;
    std::vector<SimpleGen2RecordReadRequest> verified_whole_reads;
    std::vector<SimpleGen2RangeReadRequest> range_reads;
    whole_pending.reserve(num_reads);
    range_pending.reserve(num_reads);
    whole_reads.reserve(num_reads);
    verified_whole_reads.reserve(num_reads);
    range_reads.reserve(num_reads);

    for (size_t i = 0; i < num_reads; ++i) {
      SameFileBlobReadRequest& req = reqs[i];
      assert(req.blob_index != nullptr);
      assert(req.result != nullptr);
      assert(req.status != nullptr);
      req.result->Reset();
      *req.status = Status::OK();

      MemoryAllocator* const allocator =
          req.standalone != nullptr ? req.standalone->allocator : nullptr;
      SimpleGen2ReadExtras* const gen2_extras =
          req.standalone != nullptr ? &req.standalone->gen2 : nullptr;
      if (req.range_length != kWholeBlobLength &&
          req.blob_index->compression() != kNoCompression) {
        // A range of a compressed value is not supported, as in
        // GetSameFileBlob(). This check comes first, so the result does not
        // depend on the read tier.
        *req.status = Status::Corruption("Cannot range-read a compressed blob");
        continue;
      }
      size_t payload_size = 0;
      size_t record_size = 0;
      *req.status = ValidateEmbeddedBlobIndex(*req.blob_index, &payload_size,
                                              &record_size);
      if (!req.status->ok()) {
        continue;
      }
      if (read_options.read_tier == kBlockCacheTier) {
        *req.status = Status::Incomplete(
            "Cannot read embedded blob in block-cache-only mode");
        continue;
      }
      // An uncompressed range reads only its bytes and checks no checksum. It
      // is never turned into a whole read, as in GetSameFileBlob().
      if (req.range_length != kWholeBlobLength) {
        assert(req.verify_policy != BlobVerifyPolicy::kVerifyIfPresent);
        if (req.range_offset > payload_size ||
            req.range_length > payload_size - req.range_offset) {
          *req.status =
              Status::InvalidArgument("Embedded blob range is out of bounds");
          continue;
        }
        CacheAllocationPtr buffer = AllocateBlock(req.range_length, allocator);
        range_reads.push_back(SimpleGen2RangeReadRequest{
            req.blob_index->offset(), payload_size, req.range_offset,
            req.range_length, req.blob_index->compression(), buffer.get(),
            req.status});
        range_pending.push_back(
            RangePending{i, req.range_length, std::move(buffer)});
        continue;
      }

      CacheAllocationPtr buffer = AllocateBlock(record_size, allocator);
      SimpleGen2RecordReadRequest read{req.blob_index->offset(),
                                       payload_size,
                                       record_size,
                                       req.blob_index->compression(),
                                       buffer.get(),
                                       req.status,
                                       gen2_extras};
      if (req.verify_policy == BlobVerifyPolicy::kVerifyIfPresent &&
          !read_options.verify_checksums) {
        verified_whole_reads.push_back(read);
      } else {
        whole_reads.push_back(read);
      }
      whole_pending.push_back(
          WholePending{i, payload_size, record_size, std::move(buffer)});
    }

    if (!whole_reads.empty()) {
      ReadAndVerifySimpleGen2BlobRecords(
          read_options, rep_->file.get(), rep_->footer.checksum_type(),
          rep_->footer.base_context_checksum(), whole_reads.size(),
          whole_reads.data());
    }
    if (!verified_whole_reads.empty()) {
      ReadOptions verified_options = read_options;
      verified_options.verify_checksums = true;
      ReadAndVerifySimpleGen2BlobRecords(
          verified_options, rep_->file.get(), rep_->footer.checksum_type(),
          rep_->footer.base_context_checksum(), verified_whole_reads.size(),
          verified_whole_reads.data());
    }
    if (!range_reads.empty()) {
      ReadSimpleGen2BlobRanges(read_options, rep_->file.get(),
                               range_reads.size(), range_reads.data());
    }

    for (WholePending& pending : whole_pending) {
      SameFileBlobReadRequest& req = reqs[pending.request_index];
      // The payload CRC is the value CRC only for an uncompressed value.
      if (req.standalone != nullptr &&
          req.blob_index->compression() != kNoCompression) {
        req.standalone->gen2.payload_crc32c.reset();
      }
      if (!req.status->ok()) {
        continue;
      }
      MemoryAllocator* const allocator =
          req.standalone != nullptr ? req.standalone->allocator : nullptr;
      std::unique_ptr<BlobContents> contents;
      *req.status = DecompressEmbeddedBlob(
          std::move(pending.buffer), pending.payload_size,
          req.blob_index->compression(), allocator, &contents);
      if (!req.status->ok()) {
        continue;
      }
      if (req.standalone == nullptr) {
        *req.status = PinBlobContents(std::move(contents), req.result);
      } else {
        req.standalone->contents = std::move(contents);
      }
    }
    for (RangePending& pending : range_pending) {
      SameFileBlobReadRequest& req = reqs[pending.request_index];
      if (!req.status->ok()) {
        continue;
      }
      std::unique_ptr<BlobContents> contents = std::make_unique<BlobContents>(
          std::move(pending.buffer), pending.range_length);
      if (req.standalone != nullptr) {
        req.standalone->contents = std::move(contents);
      } else {
        *req.status = PinBlobContents(std::move(contents), req.result);
      }
    }
    return;
  }

  // Split the requests into a whole-record group and a sub-range group. Each
  // group goes to one BlobSource batched call, which reads its cache misses in
  // MultiRead batches. Force-verify reads use the scalar path because they
  // cannot share the batch's read options. A range of a compressed value is
  // not supported (see GetSameFileBlob()).
  std::vector<SimpleGen2BlobReadRequest> whole_reqs;
  std::vector<SimpleGen2BlobRangeReadRequest> range_reqs;

  for (size_t i = 0; i < num_reads; ++i) {
    SameFileBlobReadRequest& req = reqs[i];
    assert(req.blob_index);
    assert(req.result);
    assert(req.status);

    if (req.range_length != kWholeBlobLength &&
        req.blob_index->compression() != kNoCompression) {
      *req.status = Status::Corruption("Cannot range-read a compressed blob");
      continue;
    }
    if (req.verify_policy == BlobVerifyPolicy::kVerifyIfPresent) {
      *req.status =
          GetSameFileBlob(read_options, *req.blob_index, req.range_offset,
                          req.range_length, req.verify_policy, req.result);
      continue;
    }

    size_t payload_size = 0;
    size_t record_size = 0;
    const Status vs =
        ValidateEmbeddedBlobIndex(*req.blob_index, &payload_size, &record_size);
    if (!vs.ok()) {
      *req.status = vs;
      continue;
    }

    if (req.range_length == kWholeBlobLength) {
      whole_reqs.push_back(SimpleGen2BlobReadRequest{
          req.blob_index->offset(), payload_size, req.blob_index->compression(),
          req.result, req.status});
    } else {
      range_reqs.push_back(SimpleGen2BlobRangeReadRequest{
          req.blob_index->offset(), payload_size, req.range_offset,
          req.range_length, req.blob_index->compression(), req.result,
          req.status});
    }
  }

  if (!whole_reqs.empty()) {
    rep_->blob_source_->MultiGetSimpleGen2Blob(
        read_options, rep_->base_cache_key, rep_->file.get(),
        rep_->footer.checksum_type(), rep_->footer.base_context_checksum(),
        whole_reqs.size(), whole_reqs.data());
  }
  if (!range_reqs.empty()) {
    rep_->blob_source_->MultiGetSimpleGen2BlobRange(
        read_options, rep_->base_cache_key, rep_->file.get(), range_reqs.size(),
        range_reqs.data());
  }
}

void BlockBasedTable::MultiGetStoredSameFileBlob(
    const ReadOptions& read_options, uint64_t coalesce_bytes, size_t num_reads,
    StoredSameFileBlobReadRequest* reqs, uint64_t max_stored_bytes,
    CoalescedReadBuffers* buffers, uint64_t* bytes_read) const {
  if (bytes_read != nullptr) {
    *bytes_read = 0;
  }
  if (num_reads == 0) {
    return;
  }
  if (reqs == nullptr) {
    return;
  }
  if (rep_ == nullptr || buffers == nullptr) {
    // Set every status: the caller's statuses can already be OK, and an OK
    // status with an empty payload would look like a successful read.
    const Status error =
        rep_ == nullptr
            ? Status::Corruption("Blob relocation table is not open")
            : Status::InvalidArgument("Missing blob GC read buffers");
    for (size_t i = 0; i < num_reads; ++i) {
      reqs[i].status = error;
    }
    return;
  }
  const Rep& rep = *rep_;

  struct Pending {
    size_t request_index = 0;
    size_t payload_size = 0;
    uint64_t record_offset = 0;
    size_t record_size = 0;
  };
  std::vector<Pending> pending;
  pending.reserve(num_reads);
  uint64_t total_stored_bytes = 0;
  for (size_t i = 0; i < num_reads; ++i) {
    StoredSameFileBlobReadRequest& req = reqs[i];
    if (req.stored_payload != nullptr) {
      *req.stored_payload = Slice();
    }
    req.status = Status::OK();
    size_t payload_size = 0;
    size_t record_size = 0;
    req.status =
        ValidateEmbeddedBlobIndex(req.blob_index, &payload_size, &record_size);
    if (!req.status.ok()) {
      continue;
    }
    if (payload_size >
        max_stored_bytes - std::min(total_stored_bytes, max_stored_bytes)) {
      req.status =
          Status::MemoryLimit("Stored blob relocation batch exceeds limit");
      continue;
    }
    total_stored_bytes += payload_size;
    pending.push_back(
        Pending{i, payload_size, req.blob_index.offset(), record_size});
  }
  if (pending.empty()) {
    return;
  }
  // Records follow key order, so this sort is usually a no-op.
  std::sort(pending.begin(), pending.end(),
            [](const Pending& lhs, const Pending& rhs) {
              return lhs.record_offset < rhs.record_offset;
            });
  std::vector<uint64_t> offsets(pending.size());
  std::vector<size_t> lengths(pending.size());
  uint64_t total_record_bytes = 0;
  for (size_t i = 0; i < pending.size(); ++i) {
    offsets[i] = pending[i].record_offset;
    lengths[i] = pending[i].record_size;
    total_record_bytes += pending[i].record_size;
  }
  std::vector<Slice> records(pending.size());
  std::vector<Status> statuses(pending.size());
  // The buffer limit is the requested bytes plus max_stored_bytes for the
  // merged gaps (see ReadCoalescedRanges()). Reads past it are deferred
  // (Incomplete).
  ReadCoalescedRanges(
      read_options, rep.file.get(), coalesce_bytes, max_stored_bytes,
      total_record_bytes +
          std::min(max_stored_bytes,
                   std::numeric_limits<uint64_t>::max() - total_record_bytes),
      pending.size(), offsets.data(), lengths.data(), records.data(),
      statuses.data(), buffers,
      /*num_reads=*/nullptr, /*read_bytes=*/nullptr);

  uint64_t successful_bytes = 0;
  for (size_t i = 0; i < pending.size(); ++i) {
    const Pending& entry = pending[i];
    StoredSameFileBlobReadRequest& req = reqs[entry.request_index];
    req.status = statuses[i];
    if (req.status.ok()) {
      req.status = VerifySimpleGen2BlobRecord(
          rep.footer.checksum_type(), rep.footer.base_context_checksum(),
          records[i], entry.payload_size, entry.record_offset,
          req.blob_index.compression(), rep.file->file_name());
    }
    if (!req.status.ok()) {
      continue;
    }
    const Slice stored(records[i].data(), entry.payload_size);
    if (BlobIndex::ComputeStoredChecksum(
            stored, req.blob_index.compression()) != req.stored_checksum) {
      req.status =
          Status::Corruption("Stored checksum mismatch while relocating blob");
      continue;
    }
    if (req.stored_payload != nullptr) {
      *req.stored_payload = stored;
    }
    successful_bytes += entry.record_size;
  }
  if (bytes_read != nullptr) {
    *bytes_read = successful_bytes;
  }
}

Status BlockBasedTable::StreamStoredSameFileBlob(
    const ReadOptions& read_options, const BlobIndex& blob_index,
    uint32_t stored_checksum, const BlobChunkConsumer& consumer) const {
  size_t payload_size = 0;
  size_t record_size = 0;
  Status status =
      ValidateEmbeddedBlobIndex(blob_index, &payload_size, &record_size);
  if (!status.ok()) {
    return status;
  }
  // Validate the trailer before delivery. The stored CRC, checked while
  // streaming, verifies the payload whatever checksum type the file uses.
  std::array<char, kSimpleGen2BlobTrailerSize> trailer{};
  IOOptions options;
  IODebugContext debug_context;
  status = rep_->file->PrepareIOOptions(read_options, options, &debug_context);
  Slice trailer_slice;
  if (status.ok()) {
    status = rep_->file->Read(blob_index.offset() + payload_size,
                              trailer.size(), options, &trailer_slice,
                              trailer.data(), &debug_context);
  }
  if (!status.ok()) {
    return status;
  }
  if (trailer_slice.size() != trailer.size() ||
      lossless_cast<CompressionType>(trailer_slice[0]) !=
          blob_index.compression()) {
    return Status::Corruption("Invalid streaming blob trailer");
  }
  if (rep_->footer.checksum_type() == kCRC32c &&
      DecodeFixed32(trailer_slice.data() + 1) !=
          crc32c::Mask(stored_checksum) +
              ChecksumModifierForContext(rep_->footer.base_context_checksum(),
                                         blob_index.offset())) {
    return Status::Corruption("Streaming blob trailer checksum mismatch");
  }
  return StreamAndVerifyBlobStoredPayload(
      read_options, rep_->file.get(), blob_index.offset(), payload_size,
      blob_index.compression(), stored_checksum, consumer);
}

Status BlockBasedTable::MaybeResolveEmbeddedValue(
    const ReadOptions& read_options, const Slice& internal_key,
    const Slice& value, std::string* resolved_internal_key,
    std::string* resolved_value, bool* resolved, PinnableSlice* pinned_value,
    bool* value_pinned, bool skip_wide_column_entities) const {
  assert(resolved_internal_key != nullptr);
  assert(resolved_value != nullptr);
  assert(resolved != nullptr);

  *resolved = false;
  if (value_pinned != nullptr) {
    *value_pinned = false;
  }
  if (!rep_->has_embedded_blobs) {
    return Status::OK();
  }

#ifndef NDEBUG
  // Test-only hook: inject a resolution failure (simulating a blob-region read
  // fault) so tests can verify that the error is surfaced rather than masked by
  // exposing the raw same-file value downstream. Compiled out in release.
  {
    Status injected_status;
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTable::MaybeResolveEmbeddedValue:InjectError",
        &injected_status);
    if (!injected_status.ok()) {
      return injected_status;
    }
  }
#endif  // NDEBUG

  ParsedInternalKey parsed_key;
  // FIXME: de-dup ParseInternalKey() work from callers
  Status s =
      ParseInternalKey(internal_key, &parsed_key, false /* log_err_key */);
  if (!s.ok()) {
    return s;
  }

  if (parsed_key.type == kTypeBlobIndex) {
    BlobIndex blob_index;
    s = blob_index.DecodeFrom(value);
    if (!s.ok() || !blob_index.IsSameFile()) {
      return s;
    }

    // Whole-value same-file blob. Route through the BlobSource (blob value
    // cache + BLOB_DB_* stats) when available, pinning the cached payload (no
    // copy). When no BlobSource is wired (SstFileReader/sst_dump/repair), fall
    // back to a direct read. When the caller provides no PinnableSlice, use a
    // local one so we still benefit from caching, then copy into
    // `resolved_value`.
    if (rep_->blob_source_ != nullptr) {
      if (pinned_value != nullptr) {
        s = ResolveEmbeddedBlobCached(read_options, blob_index, pinned_value);
        if (s.ok() && value_pinned != nullptr) {
          *value_pinned = true;
        }
      } else {
        PinnableSlice local_value;
        s = ResolveEmbeddedBlobCached(read_options, blob_index, &local_value);
        if (s.ok()) {
          resolved_value->assign(local_value.data(), local_value.size());
        }
      }
    } else if (pinned_value != nullptr) {
      s = ResolveEmbeddedBlobPinned(read_options, blob_index, pinned_value);
      if (s.ok() && value_pinned != nullptr) {
        *value_pinned = true;
      }
    } else {
      s = ResolveEmbeddedBlob(read_options, blob_index, resolved_value);
    }
    if (!s.ok()) {
      return s;
    }

    InternalKey resolved_key(parsed_key.user_key, parsed_key.sequence,
                             kTypeValue);
    const Slice encoded_resolved_key = resolved_key.Encode();
    resolved_internal_key->assign(encoded_resolved_key.data(),
                                  encoded_resolved_key.size());
    *resolved = true;
    return Status::OK();
  }

  if (parsed_key.type != kTypeWideColumnEntity) {
    return Status::OK();
  }

  if (skip_wide_column_entities) {
    // Get()/MultiGet() path: hand the raw entity to GetContext, which resolves
    // its same-file (and separate-file) blob columns zero-copy via an
    // EmbeddedAwareBlobFetcher. Only the iterator path re-serializes below.
    return Status::OK();
  }

  // FIXME: Reading embedded blob wide columns is very heavyweight with memcpy
  // and serialization/deserialization
  bool has_blob_columns = false;
  s = WideColumnSerialization::HasBlobColumns(value, has_blob_columns);
  if (!s.ok() || !has_blob_columns) {
    return s;
  }

  Slice entity(value);
  std::vector<WideColumn> columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  s = WideColumnSerialization::Deserialize(entity, columns, &blob_columns);
  if (!s.ok()) {
    return s;
  }

  std::vector<std::pair<std::string, std::string>> rewritten_columns;
  rewritten_columns.reserve(columns.size());
  for (const WideColumn& column : columns) {
    rewritten_columns.emplace_back(column.name().ToString(),
                                   column.value().ToString());
  }

  std::vector<std::pair<size_t, BlobIndex>> remaining_blob_columns;
  remaining_blob_columns.reserve(blob_columns.size());
  for (const auto& blob_column : blob_columns) {
    const size_t column_index = blob_column.first;
    const BlobIndex& blob_index = blob_column.second;
    if (!blob_index.IsSameFile()) {
      remaining_blob_columns.emplace_back(blob_column);
      continue;
    }
    if (column_index >= rewritten_columns.size()) {
      return Status::Corruption("Wide-column blob index out of range");
    }

    // Per-column same-file blob: cache via BlobSource when available (same
    // SST-like key, keyed by this column's record offset), copying into the
    // rebuilt column value; otherwise read directly.
    if (rep_->blob_source_ != nullptr) {
      PinnableSlice column_value;
      s = ResolveEmbeddedBlobCached(read_options, blob_index, &column_value);
      if (s.ok()) {
        rewritten_columns[column_index].second.assign(column_value.data(),
                                                      column_value.size());
      }
    } else {
      s = ResolveEmbeddedBlob(read_options, blob_index,
                              &rewritten_columns[column_index].second);
    }
    if (!s.ok()) {
      return s;
    }
    *resolved = true;
  }

  if (!*resolved) {
    return Status::OK();
  }

  resolved_internal_key->assign(internal_key.data(), internal_key.size());
  resolved_value->clear();
  if (!remaining_blob_columns.empty()) {
    return WideColumnSerialization::SerializeV2(
        rewritten_columns, remaining_blob_columns, *resolved_value);
  }

  WideColumns resolved_columns;
  resolved_columns.reserve(rewritten_columns.size());
  for (const auto& column : rewritten_columns) {
    resolved_columns.emplace_back(column.first, column.second);
  }
  return WideColumnSerialization::Serialize(resolved_columns, *resolved_value);
}

Status BlockBasedTable::ResolveEmbeddedValueForGet(
    const ReadOptions& read_options, const Slice& key, const Slice& value,
    EmbeddedValueGetScratch* scratch, Cleanable* block_pinner,
    Slice* key_to_save, Slice* value_to_save, Cleanable** value_pinner,
    bool defer_wide_column_entities) const {
  assert(scratch != nullptr);
  *key_to_save = key;
  *value_to_save = value;
  // When nothing is resolved, save the original entry pinned by the data block.
  *value_pinner = block_pinner;
  bool resolved = false;
  bool value_pinned = false;
  // The pinned buffer (if any) from a prior entry has been delegated to that
  // entry's output; reset before reuse so PinSlice can pin again.
  scratch->pinned_value.Reset();
  Status s = MaybeResolveEmbeddedValue(
      read_options, key, value, &scratch->key_buf, &scratch->value_buf,
      &resolved, &scratch->pinned_value, &value_pinned,
      defer_wide_column_entities);
  if (s.ok() && resolved) {
    *key_to_save = Slice(scratch->key_buf);
    if (value_pinned) {
      // Whole-value blob: pin the owned buffer into the output (no copy).
      *value_to_save = Slice(scratch->pinned_value);
      *value_pinner = &scratch->pinned_value;
    } else {
      // Built (wide-column) value lives in reused scratch; it must be copied.
      *value_to_save = Slice(scratch->value_buf);
      *value_pinner = nullptr;
    }
  }
  return s;
}

Status BlockBasedTable::ReadRangeDelBlock(
    const ReadOptions& read_options, FilePrefetchBuffer* prefetch_buffer,
    InternalIterator* meta_iter,
    const InternalKeyComparator& internal_comparator,
    BlockCacheLookupContext* lookup_context) {
  Status s;
  BlockHandle range_del_handle;
  s = FindOptionalMetaBlock(meta_iter, kRangeDelBlockName, &range_del_handle);
  if (!s.ok()) {
    ROCKS_LOG_WARN(
        rep_->ioptions.logger,
        "Error when seeking to range delete tombstones block from file: %s",
        s.ToString().c_str());
  } else if (!range_del_handle.IsNull()) {
    Status tmp_status;
    std::unique_ptr<InternalIterator> iter(NewDataBlockIterator<DataBlockIter>(
        read_options, range_del_handle,
        /*input_iter=*/nullptr, BlockType::kRangeDeletion,
        /*get_context=*/nullptr, lookup_context, prefetch_buffer,
        /*for_compaction= */ false, /*async_read= */ false, tmp_status,
        /*use_block_cache_for_lookup=*/true));
    assert(iter != nullptr);
    s = iter->status();
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          rep_->ioptions.logger,
          "Encountered error while reading data from range del block %s",
          s.ToString().c_str());
      IGNORE_STATUS_IF_ERROR(s);
    } else {
      std::vector<SequenceNumber> snapshots;
      // When user defined timestamps are not persisted, the range tombstone end
      // key read from the data block doesn't include user timestamp.
      // The range tombstone start key should already include user timestamp as
      // it's handled at block parsing level in the same way as the other data
      // blocks.
      rep_->fragmented_range_dels =
          std::make_shared<FragmentedRangeTombstoneList>(
              std::move(iter), internal_comparator, false /*for_compaction=*/,
              snapshots, rep_->user_defined_timestamps_persisted);
    }
  }
  return s;
}

Status BlockBasedTable::PrefetchIndexAndFilterBlocks(
    const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
    InternalIterator* meta_iter, BlockBasedTable* new_table, bool prefetch_all,
    const BlockBasedTableOptions& table_options, const int level,
    size_t file_size, size_t max_file_size_for_l0_meta_pin,
    bool avoid_shared_metadata_cache, BlockCacheLookupContext* lookup_context) {
  // Find filter handle and filter type
  if (rep_->filter_policy) {
    auto name = rep_->filter_policy->CompatibilityName();
    for (const auto& [filter_type, prefix] :
         {std::make_pair(Rep::FilterType::kFullFilter, kFullFilterBlockPrefix),
          std::make_pair(Rep::FilterType::kPartitionedFilter,
                         kPartitionedFilterBlockPrefix),
          std::make_pair(Rep::FilterType::kNoFilter,
                         kObsoleteFilterBlockPrefix)}) {
      std::string filter_block_key = prefix + name;
      if (FindMetaBlock(meta_iter, filter_block_key, &rep_->filter_handle)
              .ok()) {
        rep_->filter_type = filter_type;
        if (filter_type == Rep::FilterType::kNoFilter) {
          ROCKS_LOG_WARN(
              rep_->ioptions.logger,
              "Detected obsolete filter type in %s. Read performance might "
              "suffer until DB is fully re-compacted.",
              rep_->file->file_name().c_str());
        }
        break;
      }
    }
  }
  // Partition filters cannot be enabled without partition indexes
  assert(rep_->filter_type != Rep::FilterType::kPartitionedFilter ||
         rep_->index_type == BlockBasedTableOptions::kTwoLevelIndexSearch);

  // Find compression dictionary handle
  Status s = FindOptionalMetaBlock(meta_iter, kCompressionDictBlockName,
                                   &rep_->compression_dict_handle);
  if (!s.ok()) {
    return s;
  }

  BlockBasedTableOptions::IndexType index_type = rep_->index_type;

  const bool use_cache = table_options.cache_index_and_filter_blocks &&
                         !avoid_shared_metadata_cache;
  [[maybe_unused]] std::pair<bool, bool> shared_metadata_cache_context = {
      avoid_shared_metadata_cache, use_cache};
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTable::PrefetchIndexAndFilterBlocks:SharedMetadataCache",
      &shared_metadata_cache_context);

  const bool maybe_flushed = !avoid_shared_metadata_cache && level == 0 &&
                             file_size <= max_file_size_for_l0_meta_pin;
  std::function<bool(PinningTier, PinningTier)> is_pinned =
      [maybe_flushed, &is_pinned](PinningTier pinning_tier,
                                  PinningTier fallback_pinning_tier) {
        // Fallback to fallback would lead to infinite recursion. Disallow it.
        assert(fallback_pinning_tier != PinningTier::kFallback);

        switch (pinning_tier) {
          case PinningTier::kFallback:
            return is_pinned(fallback_pinning_tier,
                             PinningTier::kNone /* fallback_pinning_tier */);
          case PinningTier::kNone:
            return false;
          case PinningTier::kFlushedAndSimilar:
            return maybe_flushed;
          case PinningTier::kAll:
            return true;
        };

        // In GCC, this is needed to suppress `control reaches end of non-void
        // function [-Werror=return-type]`.
        assert(false);
        return false;
      };
  const bool pin_top_level_index =
      !avoid_shared_metadata_cache &&
      is_pinned(table_options.metadata_cache_options.top_level_index_pinning,
                table_options.pin_top_level_index_and_filter
                    ? PinningTier::kAll
                    : PinningTier::kNone);
  const bool pin_partition =
      !avoid_shared_metadata_cache &&
      is_pinned(table_options.metadata_cache_options.partition_pinning,
                table_options.pin_l0_filter_and_index_blocks_in_cache
                    ? PinningTier::kFlushedAndSimilar
                    : PinningTier::kNone);
  const bool pin_unpartitioned =
      !avoid_shared_metadata_cache &&
      is_pinned(table_options.metadata_cache_options.unpartitioned_pinning,
                table_options.pin_l0_filter_and_index_blocks_in_cache
                    ? PinningTier::kFlushedAndSimilar
                    : PinningTier::kNone);

  // pin the first level of index
  const bool pin_index =
      index_type == BlockBasedTableOptions::kTwoLevelIndexSearch
          ? pin_top_level_index
          : pin_unpartitioned;
  // prefetch the first level of index
  // WART: this might be redundant (unnecessary cache hit) if !pin_index,
  // depending on prepopulate_block_cache option
  const bool prefetch_index = prefetch_all || pin_index;
  const bool requires_custom_reader =
      rep_->footer.HasIncompatibleFeature(
          kFooterFeatureRequireUserDefinedIndex) ||
      rep_->table_properties->standard_index_is_stub != 0;

  std::unique_ptr<IndexReader> index_reader;
  s = new_table->CreateIndexReader(ro, prefetch_buffer, meta_iter, use_cache,
                                   prefetch_index, pin_index, lookup_context,
                                   &index_reader);
  if (!s.ok()) {
    return s;
  }
  // kStandardOnly behaves exactly like "no factory configured" by contract,
  // so we skip UDI block probing entirely in that mode even if a factory
  // pointer happens to be set in the table options. This avoids spurious
  // disk reads, log spam, and statistics noise.
  bool custom_reader_installed = false;
  if (table_options.GetEffectiveIndexMode() !=
          BlockBasedTableOptions::IndexMode::kStandardOnly &&
      table_options.user_defined_index_factory != nullptr) {
    std::string udi_name(table_options.user_defined_index_factory->Name());
    BlockHandle udi_block_handle;

    // kStandardDefault is the only mode left here that treats the custom index
    // as optional. kStandardRequired promises it is present, kCustomDefault and
    // kCustomOnly route reads through it, and a file written in kCustomOnly has
    // no usable standard index to fall back to. kStandardOnly never gets here,
    // which is what lets this compare against kStandardDefault alone.
    assert(table_options.GetEffectiveIndexMode() !=
           BlockBasedTableOptions::IndexMode::kStandardOnly);
    const bool custom_index_required =
        table_options.GetEffectiveIndexMode() !=
            BlockBasedTableOptions::IndexMode::kStandardDefault ||
        requires_custom_reader;

    // Use FindMetaBlock (not FindOptionalMetaBlock) so we get a non-OK status
    // when the block is missing, allowing the index_mode logic below to
    // decide whether to error or warn.
    s = FindMetaBlock(meta_iter, kIndexFactoryMetaPrefix + udi_name,
                      &udi_block_handle);
    if (!s.ok()) {
      RecordTick(rep_->ioptions.statistics.get(),
                 SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT);
      if (custom_index_required) {
        // kStandardRequired preserves the legacy fail_if_no_udi_on_open=true
        // behavior. kCustomDefault and kCustomOnly route all reads through the
        // UDI, so a missing UDI block is also a hard error.
        ROCKS_LOG_ERROR(rep_->ioptions.logger,
                        "Failed to find the UDI block %s in file %s; %s",
                        udi_name.c_str(), rep_->file->file_name().c_str(),
                        s.ToString().c_str());
        // Make the status more informative
        s = Status::Corruption(s.ToString(), rep_->file->file_name());
        return s;
      } else {
        // kStandardDefault: the custom index is optional. Fall back to
        // the standard index for this SST and log a warning so operators
        // can identify SSTs missing the custom block (e.g., still pending
        // compaction after enabling the factory).
        ROCKS_LOG_WARN(rep_->ioptions.logger,
                       "Failed to find the UDI block %s in file %s; %s",
                       udi_name.c_str(), rep_->file->file_name().c_str(),
                       s.ToString().c_str());
        s = Status::OK();
      }
    }

    // A zero-size UDI block means no user-defined index for this SST. Older UDI
    // builds treated this as "effectively no UDI" even with
    // fail_if_no_udi_on_open=true or use_udi_as_primary_index=true, so keep
    // opening these files through the standard index when one is populated.
    // kCustomOnly has only a footer-satisfying standard-index stub, so it must
    // still fail loudly instead of exposing an OK empty scan.
    if (s.ok() && udi_block_handle.size() == 0 && requires_custom_reader) {
      RecordTick(rep_->ioptions.statistics.get(),
                 SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT);
      ROCKS_LOG_ERROR(rep_->ioptions.logger,
                      "UDI block %s in file %s has size 0; cannot serve "
                      "reads in the configured index_mode",
                      udi_name.c_str(), rep_->file->file_name().c_str());
      return Status::Corruption(
          "UDI block " + udi_name + " has size 0 in " +
          rep_->file->file_name() +
          " -- cannot serve reads in the configured index_mode");
    }
    if (udi_block_handle.size() > 0) {
      // Read the block, and allocate on heap or pin in cache. The UDI block is
      // not compressed. RetrieveBlock will verify the checksum.
      if (s.ok()) {
        PERF_TIMER_GUARD(read_index_block_nanos);
        s = RetrieveBlock(prefetch_buffer, ro, udi_block_handle,
                          rep_->decompressor.get(), &rep_->udi_block,
                          /*get_context=*/nullptr, lookup_context,
                          /*for_compaction=*/false, use_cache,
                          /*async_read=*/false,
                          /*use_block_cache_for_lookup=*/true);
      }
      if (s.ok()) {
        assert(!rep_->udi_block.IsEmpty());
        // Release-build guard: a null value here would SIGSEGV on .data.
        // Route it through the shared accounting/fallback path below rather
        // than returning, so kStandardDefault can still fall back.
        if (UNLIKELY(rep_->udi_block.GetValue() == nullptr)) {
          s = Status::Corruption("UDI block read returned null contents for " +
                                 udi_name + " in " + rep_->file->file_name());
        }
      }
      if (s.ok()) {
        std::unique_ptr<IndexFactoryReader> udi_reader;
        // NewReader() may advance its Slice, or keep a pointer to it for the
        // reader's lifetime. Give it this table's copy so a cached block
        // shared with other table readers keeps its full view.
        rep_->udi_index_contents = rep_->udi_block.GetValue()->data;
        const size_t index_block_size = rep_->udi_index_contents.size();
        IndexFactoryOptions udi_option;
        udi_option.comparator = rep_->internal_comparator.user_comparator();
        s = table_options.user_defined_index_factory->NewReader(
            udi_option, rep_->udi_index_contents, udi_reader);
        if (s.ok()) {
          if (udi_reader) {
            // Primary UDI mode is purely config-driven. The
            // udi_is_primary_index table property is informational only
            // (for diagnostics / sst_dump) and does not affect routing.
            // This keeps rollback simple: lowering index_mode sends reads
            // back to the standard index without waiting for compaction.
            // SSTs written in kCustomOnly are the exception -- they have no
            // standard index, so the wrapper keeps routing them to the
            // custom index (see standard_index_is_stub in NewIterator).
            index_reader = std::make_unique<IndexFactoryReaderWrapper>(
                udi_name, std::move(index_reader), std::move(udi_reader),
                table_options.GetEffectiveIndexMode() ==
                        BlockBasedTableOptions::IndexMode::kCustomDefault ||
                    table_options.GetEffectiveIndexMode() ==
                        BlockBasedTableOptions::IndexMode::kCustomOnly,
                requires_custom_reader, index_block_size);
            custom_reader_installed = true;
          } else {
            s = Status::Corruption("Failed to create UDI reader for " +
                                   udi_name + " in file " +
                                   rep_->file->file_name());
          }
        }
      }
    }

    // A custom index block the factory cannot use gets the same treatment as a
    // missing one: in kStandardDefault the file is still fully indexed by the
    // standard index, so keep it readable instead of taking it offline. Only
    // statuses that mean "these bytes are unusable" qualify. An IOError or a
    // block cache limit is transient or environmental, so it propagates instead
    // of permanently downgrading this reader to the standard index.
    //
    // The ERROR log is the actionable signal here. The ticker is shared with
    // the missing-block case above, which is expected and noisy while a factory
    // is being rolled out, so it cannot single this case out on its own.
    if (!s.ok()) {
      // Tick whether or not this reader can fall back, so the counter means
      // "SSTs that could not use their custom index" instead of "SSTs that
      // fell back". A non-OK status here always means the block was located
      // but could not be read or parsed; every missing-block path above either
      // ticked already or returned.
      RecordTick(rep_->ioptions.statistics.get(),
                 SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT);
      if (!custom_index_required &&
          (s.IsCorruption() || s.IsNotSupported() || s.IsInvalidArgument())) {
        ROCKS_LOG_ERROR(rep_->ioptions.logger,
                        "Failed to load the UDI block %s in file %s, falling "
                        "back to the standard index; %s",
                        udi_name.c_str(), rep_->file->file_name().c_str(),
                        s.ToString().c_str());
        rep_->udi_index_contents.clear();
        rep_->udi_block.Reset();
        s = Status::OK();
      } else {
        ROCKS_LOG_ERROR(rep_->ioptions.logger,
                        "Failed to load the UDI block %s in file %s; the "
                        "configured index_mode has no usable fallback; %s",
                        udi_name.c_str(), rep_->file->file_name().c_str(),
                        s.ToString().c_str());
      }
    }
  }

  if (requires_custom_reader && !custom_reader_installed) {
    if (!s.ok()) {
      return s;
    }
    return Status::Corruption(
        "SST requires a matching user_defined_index_factory reader: " +
        rep_->file->file_name());
  }

  rep_->index_reader = std::move(index_reader);

  // The partitions of partitioned index are always stored in cache. They
  // are hence follow the configuration for pin and prefetch regardless of
  // the value of cache_index_and_filter_blocks
  if (s.ok() && !avoid_shared_metadata_cache &&
      (prefetch_all || pin_partition)) {
    s = rep_->index_reader->CacheDependencies(ro, pin_partition,
                                              prefetch_buffer);
  }
  if (!s.ok()) {
    return s;
  }

  // pin the first level of filter
  const bool pin_filter =
      rep_->filter_type == Rep::FilterType::kPartitionedFilter
          ? pin_top_level_index
          : pin_unpartitioned;
  // prefetch the first level of filter
  // WART: this might be redundant (unnecessary cache hit) if !pin_filter,
  // depending on prepopulate_block_cache option
  const bool prefetch_filter = prefetch_all || pin_filter;

  if (rep_->filter_policy) {
    auto filter = new_table->CreateFilterBlockReader(
        ro, prefetch_buffer, use_cache, prefetch_filter, pin_filter,
        lookup_context);

    if (filter) {
      // Refer to the comment above about paritioned indexes always being cached
      if (!avoid_shared_metadata_cache && (prefetch_all || pin_partition)) {
        s = filter->CacheDependencies(ro, pin_partition, prefetch_buffer);
        if (!s.ok()) {
          return s;
        }
      }
      rep_->filter = std::move(filter);
    }
  }

  // NOTE: before the fix to https://github.com/meta/rocksdb/issues/12409, a
  // file could have a (de)compression dictionary block without a configured
  // compression, so we need to ignore the dictionary in that case.
  if (!rep_->compression_dict_handle.IsNull() && rep_->decompressor) {
    std::unique_ptr<UncompressionDictReader> uncompression_dict_reader;
    s = UncompressionDictReader::Create(
        this, ro, prefetch_buffer, use_cache, prefetch_all || pin_unpartitioned,
        pin_unpartitioned, lookup_context, &uncompression_dict_reader);
    if (!s.ok()) {
      return s;
    }

    rep_->uncompression_dict_reader = std::move(uncompression_dict_reader);
  }

  assert(s.ok());
  return s;
}

void BlockBasedTable::SetupForCompaction() {}

std::shared_ptr<const TableProperties> BlockBasedTable::GetTableProperties()
    const {
  return rep_->table_properties;
}

namespace {

// Readahead for walks over every reference data block (verify). It starts at
// 256 KB and grows to 1 MB (estimates).
constexpr size_t kBlobReferenceWalkInitialReadahead = 256 * 1024;
constexpr size_t kBlobReferenceWalkMaxReadahead = 1024 * 1024;

// Reads one metadata block with checksum verification, without filling the
// block cache and without the persistent cache. Data blocks may be
// compressed; the index is not.
// Blob reference metadata blocks have no block type of their own. They use
// BlockType::kUserDefinedIndex, the type that GetBlockTypeForMetaBlockByName()
// gives the reference index block (kBlobReferenceIndexBlockName).
Status ReadBlobReferenceMetadataBlock(const BlockBasedTable::Rep& rep,
                                      const ReadOptions& caller_read_options,
                                      const BlockHandle& handle,
                                      bool maybe_compressed,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      BlockContents* contents) {
  // No persistent cache, also for a reader from the table cache, so census
  // reads and indirect blob GC reads never fill it (as fill_cache=false does
  // for the block cache).
  const PersistentCacheOptions no_persistent_cache;
  ReadOptions read_options(caller_read_options);
  read_options.fill_cache = false;
  read_options.verify_checksums = true;
  IOStatus io_status =
      BlockFetcher(rep.file.get(), prefetch_buffer, rep.footer, read_options,
                   handle, contents, rep.ioptions,
                   maybe_compressed /* do_uncompress */, maybe_compressed,
                   BlockType::kUserDefinedIndex,
                   maybe_compressed ? rep.decompressor.get() : nullptr,
                   no_persistent_cache)
          .ReadBlockContents();
  return io_status;
}

// Checks the descriptor against the table property, then reads and decodes
// the reference index.
Status ReadBlobReferenceIndex(const BlockBasedTable::Rep& rep,
                              const ReadOptions& read_options,
                              const Slice& expected_descriptor,
                              BlobReferenceMetadataDescriptor* descriptor,
                              BlobReferenceIndex* index) {
  if (rep.table_properties == nullptr) {
    return Status::Corruption("Missing table properties");
  }
  const UserCollectedProperties::const_iterator property =
      rep.table_properties->user_collected_properties.find(
          kBlobReferenceDescriptorPropertyName);
  if (property == rep.table_properties->user_collected_properties.end()) {
    return Status::Corruption("Missing required blob reference descriptor");
  }
  if (Slice(property->second) != expected_descriptor) {
    return Status::Corruption("Blob reference descriptor mismatch");
  }
  Status status = BlobReferenceMetadataDescriptor::DecodeFrom(
      expected_descriptor, descriptor);
  if (!status.ok()) {
    return status;
  }
  BlockContents index_contents;
  status = ReadBlobReferenceMetadataBlock(
      rep, read_options, descriptor->index_handle,
      /*maybe_compressed=*/false, /*prefetch_buffer=*/nullptr, &index_contents);
  if (!status.ok()) {
    return status;
  }
  return DecodeBlobReferenceIndex(index_contents.data, *descriptor, index);
}

// Helper of BlockBasedTable::ReadBlobReferences(). It reads the reference data
// blocks `blocks` (block ordinals in increasing order) and calls `visit` for
// each one in order with its uncompressed contents. The contents stay valid
// only during the call.
// - Batches: the reads go through MultiRead in batches of at most
//   kBlobReferenceReadBatchBytes (256 KB) of on-disk bytes (block plus
//   trailer). A block larger than the batch budget returns Corruption. With
//   mmap reads or direct I/O, the blocks are read one at a time instead.
// - Requests: each run of adjacent blocks in a batch is one read request.
// - Checks: each block is checked with its trailer checksum. A block that
//   fails its checksum or decompression is passed to `visit` with an empty
//   Slice and its Corruption status. Other read errors are returned.
Status ReadBlobReferenceDataBlocks(
    const BlockBasedTable::Rep& rep, const ReadOptions& read_options,
    const BlobReferenceIndex& index, const std::vector<size_t>& blocks,
    const std::function<Status(size_t block, const Slice& contents,
                               const Status& block_status)>& visit) {
  RandomAccessFileReader* const file = rep.file.get();
  if (rep.ioptions.allow_mmap_reads || file->use_direct_io()) {
    // With mmap reads or direct I/O, read the same blocks one at a time
    // through the normal block read path (ReadBlobReferenceMetadataBlock())
    // instead of the batched MultiRead path below.
    for (size_t block : blocks) {
      BlockContents contents;
      Status status = ReadBlobReferenceMetadataBlock(
          rep, read_options, index.blocks[block], /*maybe_compressed=*/true,
          /*prefetch_buffer=*/nullptr, &contents);
      if (!status.ok() && !status.IsCorruption()) {
        return status;
      }
      status = visit(block, status.ok() ? contents.data : Slice(), status);
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }
  const auto on_disk_size = [&](size_t block) {
    return static_cast<size_t>(
        BlockBasedTable::BlockSizeWithTrailer(index.blocks[block]));
  };
  size_t batch_budget = kBlobReferenceReadBatchBytes;
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTable::ReadBlobReferenceDataBlocks:BatchBytes", &batch_budget);
  std::unique_ptr<char[]> buffer(new char[batch_budget]);
  size_t next = 0;
  while (next < blocks.size()) {
    // One batch: as many blocks as fit in the buffer, at least one.
    const size_t batch_begin = next;
    size_t batch_bytes = 0;
    while (next < blocks.size() &&
           batch_bytes + on_disk_size(blocks[next]) <= batch_budget) {
      batch_bytes += on_disk_size(blocks[next]);
      ++next;
    }
    if (next == batch_begin) {
      return Status::Corruption("Blob reference data block is too large");
    }
    // One read request per run of adjacent blocks.
    std::vector<FSReadRequest> requests;
    std::vector<size_t> request_of_block;
    std::vector<size_t> buffer_offset_of_block;
    size_t buffer_offset = 0;
    for (size_t i = batch_begin; i < next; ++i) {
      const BlockHandle& handle = index.blocks[blocks[i]];
      if (requests.empty() ||
          requests.back().offset + requests.back().len != handle.offset()) {
        FSReadRequest request;
        request.offset = handle.offset();
        request.len = 0;
        request.scratch = buffer.get() + buffer_offset;
        requests.push_back(std::move(request));
      }
      requests.back().len += on_disk_size(blocks[i]);
      request_of_block.push_back(requests.size() - 1);
      buffer_offset_of_block.push_back(buffer_offset);
      buffer_offset += on_disk_size(blocks[i]);
    }
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTable::ReadBlobReferenceDataBlocks:MultiRead", &requests);
    IOOptions io_options;
    IODebugContext dbg;
    IOStatus io_status = file->PrepareIOOptions(read_options, io_options, &dbg);
    if (io_status.ok()) {
      io_status = file->MultiRead(io_options, requests.data(), requests.size(),
                                  /*aligned_buf=*/nullptr, &dbg);
    }
    // The loop below can return at the first failed request, and a failed call
    // leaves the request statuses unread, so mark them all as checked here (as
    // ReadCoalescedRanges() does). The loop still reads each status it needs.
    for (FSReadRequest& request : requests) {
      request.status.PermitUncheckedError();
    }
    if (!io_status.ok()) {
      return io_status;
    }
    for (size_t i = batch_begin; i < next; ++i) {
      const size_t block = blocks[i];
      const BlockHandle& handle = index.blocks[block];
      const FSReadRequest& request =
          requests[request_of_block[i - batch_begin]];
      if (!request.status.ok()) {
        return request.status;
      }
      const size_t offset_in_request =
          buffer_offset_of_block[i - batch_begin] -
          lossless_cast<size_t>(request.scratch - buffer.get());
      Status status;
      if (request.result.size() != request.len) {
        status = Status::Corruption("Truncated blob reference block read");
      }
      const char* const data = request.result.data() + offset_in_request;
      const size_t size = static_cast<size_t>(handle.size());
      if (status.ok()) {
        status =
            VerifyBlockChecksum(rep.footer, data, size, file->file_name(),
                                handle.offset(), BlockType::kUserDefinedIndex);
      }
      BlockContents contents;
      if (status.ok()) {
        const CompressionType type = lossless_cast<CompressionType>(data[size]);
        if (type == kNoCompression) {
          contents = BlockContents(Slice(data, size));
        } else if (rep.decompressor == nullptr) {
          status = Status::Corruption(
              "Compressed blob reference block without a decompressor");
        } else {
          status = DecompressSerializedBlock(
              data, size, type, *rep.decompressor, &contents, rep.ioptions);
        }
      }
      if (!status.ok() && !status.IsCorruption()) {
        return status;
      }
      status = visit(block, status.ok() ? contents.data : Slice(), status);
      if (!status.ok()) {
        return status;
      }
    }
  }
  return Status::OK();
}

// Decode state of one request of a batch read.
struct BlobReferenceGroupReadState {
  // The first and last data block that hold this group, as positions in
  // index.blocks.
  size_t first_block = 0;
  size_t last_block = 0;
  // The output size when this request started to append, valid once
  // `started` is true.
  size_t output_begin = 0;
  std::optional<BlobReferenceGroupDecoder> decoder;
  bool started = false;
  // True when the request has finished, with success or with an error. The
  // block visitor skips it.
  bool done = false;
};

// A block reader over the data blocks of `index`. `*holder` keeps the last
// block alive until the next read.
BlobReferenceBlockReader DataBlockReader(const BlockBasedTable::Rep& rep,
                                         const ReadOptions& read_options,
                                         const BlobReferenceIndex& index,
                                         FilePrefetchBuffer* prefetch_buffer,
                                         BlockContents* holder) {
  return [&rep, &read_options, &index, prefetch_buffer, holder](
             size_t block, Slice* contents) {
    *holder = BlockContents();
    Status status = ReadBlobReferenceMetadataBlock(
        rep, read_options, index.blocks[block], /*maybe_compressed=*/true,
        prefetch_buffer, holder);
    *contents = holder->data;
    return status;
  };
}

}  // namespace

Status BlockBasedTable::ReadBlobReferenceGroups(
    const ReadOptions& read_options, const Slice& expected_descriptor,
    const std::function<Status(size_t buffer_bytes)>& before_allocation,
    std::vector<BlobReferenceGroup>* groups) const {
  if (groups == nullptr) {
    return Status::InvalidArgument("Null blob reference group output");
  }
  if (rep_ == nullptr) {
    return Status::Corruption("Missing block-based table reader state");
  }
  // The index holds every group total, so no data block is read. The decoder
  // checks that the totals add up to the descriptor.
  BlobReferenceMetadataDescriptor descriptor;
  BlobReferenceIndex index;
  Status status = ReadBlobReferenceIndex(
      *rep_, read_options, expected_descriptor, &descriptor, &index);
  return status.ok()
             ? CopyIndirectBlobReferenceGroups(index, before_allocation, groups)
             : status;
}

namespace {
// Appends to a reserved vector, for requests without a sink.
class VectorBlobReferenceSink : public BlobReferenceSink {
 public:
  explicit VectorBlobReferenceSink(std::vector<BlobReference>* output)
      : output_(output) {}
  size_t size() const override { return output_->size(); }
  size_t Room() const override { return output_->capacity() - output_->size(); }
  Status Append(const BlobReference& reference) override {
    if (output_->size() == output_->capacity()) {
      return Status::Corruption("Blob reference group count mismatch");
    }
    output_->push_back(reference);
    return Status::OK();
  }
  void Truncate(size_t size) override { output_->resize(size); }
  const void* identity() const override { return output_; }

 private:
  std::vector<BlobReference>* output_;
};

// Reads the exact groups `reads` (sorted by key, unique) from the decoded
// `index` into `sinks`.
Status ReadIndexedBlobReferenceGroups(
    const BlockBasedTable::Rep& rep, const ReadOptions& read_options,
    const BlobReferenceIndex& index, std::vector<BlobReferenceGroupRead>& reads,
    const std::vector<BlobReferenceSink*>& sinks) {
  // Merge the sorted requests with the sorted index groups, and collect the
  // blocks that the found groups need. A request whose group is absent fails
  // with NotFound. A request whose group does not fit its output fails with
  // Aborted. A group ends in the block where the next group starts. If the next
  // group starts at the beginning of its block (run_offset 0), the group ends
  // in the block before. The last group ends in the last data block.
  // (start_block and run_offset are fields of BlobReferenceIndexEntry in
  // db/blob/blob_reference_metadata.h.)
  //
  // For example, group G starts in block 3, and the next group H starts in
  // block 4. There are two cases:
  //
  //   (a) H starts inside block 4      (b) H starts at the first byte of
  //       (run_offset > 0):                block 4 (run_offset 0):
  //       G ends in block 4.               G ends in block 3.
  //
  //       | block 3   | block 4   |        | block 3   | block 4   |
  //       |  [G=======|===][H==   |        |  [G======]|[H=========|
  std::vector<BlobReferenceGroupReadState> states(reads.size());
  std::vector<size_t> blocks;
  // Several requests can share one sink: a whole-generation read becomes one
  // request per group, and indirect blob GC reads all generation numbers of one
  // input into one live set. Requests that share one sink share its room.
  // Before any decode, each request is checked against the room left after the
  // earlier requests of that sink. A request that does not fit gets Aborted and
  // adds nothing.
  UnorderedMap<const void*, uint64_t> planned_by_sink;
  size_t group = 0;
  for (size_t i = 0; i < reads.size(); ++i) {
    const BlobReferenceGroup& requested = *reads[i].group;
    while (group < index.groups.size() &&
           index.groups[group].group.group_key() < requested.group_key()) {
      ++group;
    }
    BlobReferenceGroupReadState& state = states[i];
    state.done = true;
    if (group == index.groups.size() ||
        !(index.groups[group].group.group_key() == requested.group_key())) {
      reads[i].status = Status::NotFound("Blob reference group is absent");
      continue;
    }
    const BlobReferenceIndexEntry& entry = index.groups[group];
    reads[i].found_reference_count = entry.group.reference_count;
    reads[i].found_stored_value_bytes = entry.group.stored_value_bytes;
    reads[i].found_record_bytes = entry.group.record_bytes;
    uint64_t& planned = planned_by_sink[sinks[i]->identity()];
    const uint64_t room = sinks[i]->Room();
    if (entry.group.reference_count >
        room - std::min<uint64_t>(planned, room)) {
      reads[i].status =
          Status::Aborted("Blob reference group exceeds the reserved output");
      continue;
    }
    planned += entry.group.reference_count;
    state.decoder.emplace(index, group);
    state.first_block = entry.start_block;
    if (group + 1 < index.groups.size()) {
      const BlobReferenceIndexEntry& next = index.groups[group + 1];
      state.last_block =
          next.run_offset == 0 ? next.start_block - 1 : next.start_block;
    } else {
      state.last_block = index.blocks.size() - 1;
    }
    state.done = false;
    for (size_t block = state.first_block; block <= state.last_block; ++block) {
      if (blocks.empty() || blocks.back() < block) {
        blocks.push_back(block);
      }
    }
  }

  // Fails one request. A request that started to append drops only what it
  // appended (its output goes back to output_begin). A request that fails
  // before it appended anything leaves the output unchanged. Several requests,
  // or several calls, can share one output, so the references that earlier
  // requests or the caller appended always stay. A request that is cut short
  // fails in its last block (see below), before any later request of the same
  // output starts. So a failure never removes a later request's references.
  const auto fail = [&](size_t i, const Status& error) {
    reads[i].status = error;
    if (states[i].started) {
      sinks[i]->Truncate(states[i].output_begin);
    }
    states[i].done = true;
  };
  size_t first_open = 0;
  Status status = ReadBlobReferenceDataBlocks(
      rep, read_options, index, blocks,
      [&](size_t block, const Slice& contents, const Status& block_status) {
        // Requests are sorted by group key, so their first blocks do not
        // decrease. first_open is the first request that has not finished. The
        // loop stops at the first request that starts after this block.
        for (size_t i = first_open;
             i < reads.size() &&
             (states[i].done || states[i].first_block <= block);
             ++i) {
          BlobReferenceGroupReadState& state = states[i];
          if (state.done || state.last_block < block) {
            continue;
          }
          if (!block_status.ok()) {
            fail(i, block_status);
            continue;
          }
          if (block == state.first_block) {
            // Earlier requests or calls may have appended to the same output;
            // this request starts after them.
            state.output_begin = sinks[i]->size();
            state.started = true;
          }
          BlobReferenceSink& output = *sinks[i];
          // BlobReferenceGroupDecoder does all format checks of the group. This
          // loop only checks that the decoder expects this block and that the
          // output has room.
          Status run_status =
              state.decoder->next_block() != block
                  ? Status::Corruption("Blob reference group is cut short")
                  : state.decoder->DecodeRun(
                        contents, [&](const BlobReference& reference) {
                          if (output.Room() == 0) {
                            return Status::Corruption(
                                "Blob reference group count mismatch");
                          }
                          return output.Append(reference);
                        });
          if (run_status.ok() && !state.decoder->done() &&
              block == state.last_block) {
            // The group's last block ended while references are still
            // missing. Fail the request at its last block, so that it is
            // marked done and first_open can move past it. The loop at the end
            // of this function would fail it the same way.
            run_status =
                Status::Corruption("Blob reference group is cut short");
          }
          if (run_status.ok() && state.decoder->done()) {
            state.done = true;
          }
          if (!run_status.ok()) {
            fail(i, run_status);
          }
        }
        while (first_open < reads.size() && states[first_open].done) {
          ++first_open;
        }
        return Status::OK();
      });
  if (!status.ok()) {
    for (size_t i = 0; i < reads.size(); ++i) {
      if (!states[i].done) {
        fail(i, status);
      }
    }
    return status;
  }
  for (size_t i = 0; i < reads.size(); ++i) {
    if (!states[i].done) {
      fail(i, Status::Corruption("Blob reference group is cut short"));
    }
  }
  return Status::OK();
}

}  // namespace

Status BlockBasedTable::ReadBlobReferences(
    const ReadOptions& read_options, const Slice& expected_descriptor,
    const std::function<Status(size_t index_bytes)>& after_index_read,
    std::vector<BlobReferenceGroupRead>* requests) const {
  if (requests == nullptr) {
    return Status::InvalidArgument("Missing blob reference batch state");
  }
  std::vector<BlobReferenceGroupRead>& reads = *requests;
  // When the whole call fails, the caller may skip the request statuses (see
  // TableReader::ReadBlobReferences()), so mark them as checked here.
  const auto fail_call = [&reads](Status call_status) {
    for (BlobReferenceGroupRead& read : reads) {
      read.status.PermitUncheckedError();
    }
    return call_status;
  };
  if (rep_ == nullptr) {
    return fail_call(
        Status::InvalidArgument("Missing blob reference batch state"));
  }
  std::vector<VectorBlobReferenceSink> vector_sinks;
  vector_sinks.reserve(reads.size());
  std::vector<BlobReferenceSink*> sinks(reads.size());
  const bool whole_generation = !reads.empty() && reads[0].whole_generation;
  for (size_t i = 0; i < reads.size(); ++i) {
    // Request i - 1 passed this check, so its group is not null.
    const bool out_of_order =
        i > 0 && reads[i].group != nullptr &&
        (whole_generation ? reads[i - 1].group->generation_file_number >=
                                reads[i].group->generation_file_number
                          : !(reads[i - 1].group->group_key() <
                              reads[i].group->group_key()));
    if (reads[i].group == nullptr ||
        (reads[i].output == nullptr && reads[i].sink == nullptr) ||
        reads[i].whole_generation != whole_generation || out_of_order) {
      return fail_call(Status::InvalidArgument(
          "Blob reference batch requests must be sorted and unique"));
    }
    if (reads[i].sink != nullptr) {
      sinks[i] = reads[i].sink;
    } else {
      vector_sinks.emplace_back(reads[i].output);
      sinks[i] = &vector_sinks.back();
    }
    reads[i].status = Status::OK();
    reads[i].found_reference_count = 0;
    reads[i].found_stored_value_bytes = 0;
    reads[i].found_record_bytes = 0;
  }

  BlobReferenceMetadataDescriptor descriptor;
  BlobReferenceIndex index;
  Status status = ReadBlobReferenceIndex(
      *rep_, read_options, expected_descriptor, &descriptor, &index);
  if (!status.ok()) {
    return fail_call(status);
  }
  if (after_index_read) {
    status = after_index_read(
        sizeof(index) + index.blocks.capacity() * sizeof(BlockHandle) +
        index.groups.capacity() * sizeof(BlobReferenceIndexEntry));
    if (!status.ok()) {
      return fail_call(status);
    }
  }
  if (!whole_generation) {
    status = ReadIndexedBlobReferenceGroups(*rep_, read_options, index, reads,
                                            sinks);
    return status.ok() ? status : fail_call(status);
  }

  // Expand each generation into its exact groups, in index order (sorted by
  // key), then add the results up per generation.
  std::vector<BlobReferenceGroupRead> group_reads;
  std::vector<BlobReferenceSink*> group_sinks;
  std::vector<size_t> owners;
  for (const BlobReferenceIndexEntry& entry : index.groups) {
    const std::vector<BlobReferenceGroupRead>::const_iterator request =
        std::lower_bound(
            reads.begin(), reads.end(), entry.group.generation_file_number,
            [](const BlobReferenceGroupRead& read, uint64_t generation) {
              return read.group->generation_file_number < generation;
            });
    if (request == reads.end() || request->group->generation_file_number !=
                                      entry.group.generation_file_number) {
      continue;
    }
    const size_t owner = lossless_cast<size_t>(request - reads.cbegin());
    BlobReferenceGroupRead group_read;
    group_read.group = &entry.group;
    group_read.sink = sinks[owner];
    group_reads.push_back(std::move(group_read));
    group_sinks.push_back(sinks[owner]);
    owners.push_back(owner);
  }
  std::vector<bool> found(reads.size(), false);
  status = ReadIndexedBlobReferenceGroups(*rep_, read_options, index,
                                          group_reads, group_sinks);
  for (size_t i = 0; i < group_reads.size(); ++i) {
    BlobReferenceGroupRead& read = reads[owners[i]];
    found[owners[i]] = true;
    read.found_reference_count += group_reads[i].found_reference_count;
    read.found_stored_value_bytes += group_reads[i].found_stored_value_bytes;
    read.found_record_bytes += group_reads[i].found_record_bytes;
    // Check the group status first, so that it is checked also when the
    // request already failed.
    if (!group_reads[i].status.ok() && read.status.ok()) {
      read.status = group_reads[i].status;
    }
  }
  for (size_t i = 0; i < reads.size(); ++i) {
    if (!found[i]) {
      reads[i].status = Status::NotFound("Blob reference generation is absent");
    }
  }
  return status.ok() ? status : fail_call(status);
}

Status BlockBasedTable::VerifyBlobReferenceMetadata(
    const ReadOptions& read_options, const Slice& expected_descriptor) const {
  if (rep_ == nullptr) {
    return Status::Corruption("Missing block-based table reader state");
  }
  BlobReferenceMetadataDescriptor descriptor;
  BlobReferenceIndex index;
  Status status = ReadBlobReferenceIndex(
      *rep_, read_options, expected_descriptor, &descriptor, &index);
  if (!status.ok()) {
    return status;
  }
  // Every block is read in file order, with readahead. The walk checks the key
  // order across runs and blocks, and every group total. The index decoder
  // already checked the descriptor totals.
  ReadaheadParams readahead;
  readahead.initial_readahead_size = kBlobReferenceWalkInitialReadahead;
  readahead.max_readahead_size = kBlobReferenceWalkMaxReadahead;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;
  rep_->CreateFilePrefetchBuffer(readahead, &prefetch_buffer,
                                 /*readaheadsize_cb=*/nullptr,
                                 FilePrefetchBufferUsage::kUnknown);
  BlockContents holder;
  return ForEachBlobReference(
      index,
      DataBlockReader(*rep_, read_options, index, prefetch_buffer.get(),
                      &holder),
      [](const BlobReference& /*reference*/) { return Status::OK(); });
}

const SeqnoToTimeMapping& BlockBasedTable::GetSeqnoToTimeMapping() const {
  return rep_->seqno_to_time_mapping;
}

size_t BlockBasedTable::ApproximateMemoryUsage() const {
  size_t usage = 0;
  if (rep_) {
    usage += rep_->ApproximateMemoryUsage();
  } else {
    return usage;
  }
  if (rep_->filter) {
    usage += rep_->filter->ApproximateMemoryUsage();
  }
  if (rep_->index_reader) {
    usage += rep_->index_reader->ApproximateMemoryUsage();
  }
  if (rep_->uncompression_dict_reader) {
    usage += rep_->uncompression_dict_reader->ApproximateMemoryUsage();
  }
  if (rep_->table_properties) {
    usage += rep_->table_properties->ApproximateMemoryUsage();
  }
  // Cached block memory is charged to the block cache. Count the retained raw
  // custom-index block here only when the table reader owns it directly.
  if (rep_->udi_block.GetOwnValue()) {
    assert(rep_->udi_block.GetValue() != nullptr);
    usage += rep_->udi_block.GetValue()->ApproximateMemoryUsage();
  }
  return usage;
}

// Load the meta-index-block from the file. On success, return the loaded
// metaindex
// block and its iterator.
Status BlockBasedTable::ReadMetaIndexBlock(
    const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
    std::unique_ptr<Block>* metaindex_block,
    std::unique_ptr<InternalIterator>* iter) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  std::unique_ptr<Block_kMetaIndex> metaindex;
  Status s = ReadAndParseBlockFromFile(
      rep_->file.get(), prefetch_buffer, rep_->footer, ro,
      rep_->footer.metaindex_handle(), &metaindex, rep_->ioptions,
      rep_->create_context, true /*maybe_compressed*/, rep_->decompressor.get(),
      rep_->persistent_cache_options, GetMemoryAllocator(rep_->table_options),
      false /* for_compaction */, false /* async_read */);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep_->ioptions.logger,
                    "Encountered error while reading data from properties"
                    " block %s",
                    s.ToString().c_str());
    return s;
  }

  *metaindex_block = std::move(metaindex);
  // meta block uses bytewise comparator.
  iter->reset(metaindex_block->get()->NewMetaIterator());
  return Status::OK();
}

template <typename TBlocklike>
Cache::Priority BlockBasedTable::GetCachePriority() const {
  // Here we treat the legacy name "...index_and_filter_blocks..." to mean all
  // metadata blocks that might go into block cache, EXCEPT only those needed
  // for the read path (Get, etc.). TableProperties should not be needed on the
  // read path (prefix extractor setting is an O(1) size special case that we
  // are working not to require from TableProperties), so it is not given
  // high-priority treatment if it should go into BlockCache.
  if constexpr (TBlocklike::kBlockType == BlockType::kData ||
                TBlocklike::kBlockType == BlockType::kProperties) {
    return Cache::Priority::LOW;
  } else if (rep_->table_options
                 .cache_index_and_filter_blocks_with_high_priority) {
    return Cache::Priority::HIGH;
  } else {
    return Cache::Priority::LOW;
  }
}

template <typename TBlocklike>
WithBlocklikeCheck<Status, TBlocklike> BlockBasedTable::GetDataBlockFromCache(
    const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
    CachableEntry<TBlocklike>* out_parsed_block, GetContext* get_context,
    UnownedPtr<Decompressor> decomp) const {
  assert(out_parsed_block);
  assert(out_parsed_block->IsEmpty());

  Status s;
  Statistics* statistics = rep_->ioptions.statistics.get();

  // Lookup uncompressed cache first
  if (block_cache) {
    assert(!cache_key.empty());
    typename BlockCacheInterface<TBlocklike>::TypedHandle* cache_handle;
    if (decomp.get() != rep_->decompressor.get() && decomp) {
      // `decomp` must be a dictionary-aware decompressor, which is only
      // available in the block cache (so that dictionaries can be evicted
      // from memory) and can't live in the table reader.
      // NOTE: inefficient BlockCreateContext copy for dict-aware decompressor
      // (see TODO in block_cache.h)
      BlockCreateContext create_ctx = rep_->create_context;
      create_ctx.decompressor = decomp.get();
      cache_handle = block_cache.LookupFull(
          cache_key, &create_ctx, GetCachePriority<TBlocklike>(), statistics,
          rep_->ioptions.lowest_used_cache_tier);
    } else {
      cache_handle = block_cache.LookupFull(
          cache_key, &rep_->create_context, GetCachePriority<TBlocklike>(),
          statistics, rep_->ioptions.lowest_used_cache_tier);
    }

    // Avoid updating metrics here if the handle is not complete yet. This
    // happens with MultiGet and secondary cache. So update the metrics only
    // if its a miss, or a hit and value is ready
    if (!cache_handle) {
      UpdateCacheMissMetrics(TBlocklike::kBlockType, get_context);
    } else {
      TBlocklike* value = block_cache.Value(cache_handle);
      if (value) {
        UpdateCacheHitMetrics(TBlocklike::kBlockType, get_context,
                              block_cache.get()->GetUsage(cache_handle));
      }
      out_parsed_block->SetCachedValue(value, block_cache.get(), cache_handle);
      return s;
    }
  }

  // If not found, search from the compressed block cache.
  assert(out_parsed_block->IsEmpty());

  return s;
}

template <typename TBlocklike>
WithBlocklikeCheck<Status, TBlocklike> BlockBasedTable::PutDataBlockToCache(
    const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
    CachableEntry<TBlocklike>* out_parsed_block,
    BlockContents&& uncompressed_block_contents,
    BlockContents&& compressed_block_contents, CompressionType block_comp_type,
    UnownedPtr<Decompressor> decomp, MemoryAllocator* memory_allocator,
    GetContext* get_context) const {
  const ImmutableOptions& ioptions = rep_->ioptions;
  assert(out_parsed_block);
  assert(out_parsed_block->IsEmpty());

  Status s;
  Statistics* statistics = ioptions.stats;

  std::unique_ptr<TBlocklike> block_holder;
  if (block_comp_type != kNoCompression &&
      uncompressed_block_contents.data.empty()) {
    assert(compressed_block_contents.data.data());
    // Retrieve the uncompressed contents into a new buffer
    s = DecompressBlockData(
        compressed_block_contents.data.data(),
        compressed_block_contents.data.size(), block_comp_type, *decomp,
        &uncompressed_block_contents, ioptions, memory_allocator);
    if (!s.ok()) {
      return s;
    }
  }
  rep_->create_context.Create(&block_holder,
                              std::move(uncompressed_block_contents));

  // insert into uncompressed block cache
  if (block_cache && block_holder->own_bytes()) {
    size_t charge = block_holder->ApproximateMemoryUsage();
    BlockCacheTypedHandle<TBlocklike>* cache_handle = nullptr;
    s = block_cache.InsertFull(cache_key, block_holder.get(), charge,
                               &cache_handle, GetCachePriority<TBlocklike>(),
                               rep_->ioptions.lowest_used_cache_tier,
                               compressed_block_contents.data, block_comp_type);

    if (s.ok()) {
      assert(cache_handle != nullptr);
      out_parsed_block->SetCachedValue(block_holder.release(),
                                       block_cache.get(), cache_handle);

      UpdateCacheInsertionMetrics(TBlocklike::kBlockType, get_context, charge,
                                  s.IsOkOverwritten(), rep_->ioptions.stats);
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
    }
  } else {
    out_parsed_block->SetOwnedValue(std::move(block_holder));
  }

  return s;
}

std::unique_ptr<FilterBlockReader> BlockBasedTable::CreateFilterBlockReader(
    const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer, bool use_cache,
    bool prefetch, bool pin, BlockCacheLookupContext* lookup_context) {
  auto& rep = rep_;
  auto filter_type = rep->filter_type;
  if (filter_type == Rep::FilterType::kNoFilter) {
    return std::unique_ptr<FilterBlockReader>();
  }

  assert(rep->filter_policy);

  switch (filter_type) {
    case Rep::FilterType::kPartitionedFilter:
      return PartitionedFilterBlockReader::Create(
          this, ro, prefetch_buffer, use_cache, prefetch, pin, lookup_context);

    case Rep::FilterType::kFullFilter:
      return FullFilterBlockReader::Create(this, ro, prefetch_buffer, use_cache,
                                           prefetch, pin, lookup_context);

    case Rep::FilterType::kNoFilter:
    default:
      // filter_type is either kNoFilter (exited the function at the first if),
      // or it must be covered in this switch block
      assert(false);
      return std::unique_ptr<FilterBlockReader>();
  }
}

// disable_prefix_seek should be set to true when prefix_extractor found in SST
// differs from the one in mutable_cf_options and index type is HashBasedIndex
InternalIteratorBase<IndexValue>* BlockBasedTable::NewIndexIterator(
    const ReadOptions& read_options, bool disable_prefix_seek,
    IndexBlockIter* input_iter, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) const {
  assert(rep_ != nullptr);
  assert(rep_->index_reader != nullptr);

  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  return rep_->index_reader->NewIterator(read_options, disable_prefix_seek,
                                         input_iter, get_context,
                                         lookup_context);
}

// TODO?
template <>
DataBlockIter* BlockBasedTable::InitBlockIterator<DataBlockIter>(
    const Rep* rep, Block* block, BlockType block_type,
    DataBlockIter* input_iter, bool block_contents_pinned) {
  return block->NewDataIterator(rep->internal_comparator.user_comparator(),
                                rep->get_global_seqno(block_type), input_iter,
                                rep->ioptions.stats, block_contents_pinned,
                                rep->user_defined_timestamps_persisted);
}

// TODO?
template <>
IndexBlockIter* BlockBasedTable::InitBlockIterator<IndexBlockIter>(
    const Rep* rep, Block* block, BlockType block_type,
    IndexBlockIter* input_iter, bool block_contents_pinned) {
  return block->NewIndexIterator(
      rep->internal_comparator.user_comparator(),
      rep->get_global_seqno(block_type), input_iter, rep->ioptions.stats,
      /* total_order_seek */ true, rep->index_has_first_key,
      rep->index_key_includes_seq, rep->index_value_is_full,
      block_contents_pinned, rep->user_defined_timestamps_persisted,
      nullptr /* prefix_index */, rep->table_options.index_block_search_type,
      FormatVersionUsesValueDeltaEscape(rep->footer.format_version()));
}

// Right now only called for Data blocks.
template <typename TBlocklike>
Status BlockBasedTable::LookupAndPinBlocksInCache(
    const ReadOptions& ro, const BlockHandle& handle,
    CachableEntry<TBlocklike>* out_parsed_block) const {
  BlockCacheInterface<TBlocklike> block_cache{
      rep_->table_options.block_cache.get()};

  TEST_SYNC_POINT("BlockBasedTable::LookupAndPinBlocksInCache:Start");

  if (!block_cache) {
    // Callers can reach here after data-block cache use has been disabled by
    // options such as read-scoped provider-backed scans. Treat the lookup as a
    // cache miss rather than requiring every caller to special-case null cache.
    return Status::OK();
  }

  Status s;
  CachableEntry<DecompressorDict> cached_dict;
  if (rep_->uncompression_dict_reader) {
    s = rep_->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        /* prefetch_buffer= */ nullptr, ro,
        /* get_context= */ nullptr, /* lookup_context= */ nullptr,
        &cached_dict);
    if (!s.ok()) {
      return s;
    }
    if (!cached_dict.GetValue()) {
      return Status::Corruption("Success but no dictionary read");
    }
  }

  // Do the lookup.
  CacheKey key_data = GetCacheKey(rep_->base_cache_key, handle);
  const Slice key = key_data.AsSlice();

  Statistics* statistics = rep_->ioptions.statistics.get();

  typename BlockCacheInterface<TBlocklike>::TypedHandle* cache_handle;
  if (cached_dict.GetValue()) {
    // NOTE: inefficient BlockCreateContext copy for dict-aware decompressor
    // (see TODO in block_cache.h)
    BlockCreateContext create_ctx = rep_->create_context;
    create_ctx.decompressor = cached_dict.GetValue()->decompressor_.get();
    cache_handle = block_cache.LookupFull(
        key, &create_ctx, GetCachePriority<TBlocklike>(), statistics,
        rep_->ioptions.lowest_used_cache_tier);
  } else {
    cache_handle = block_cache.LookupFull(
        key, &rep_->create_context, GetCachePriority<TBlocklike>(), statistics,
        rep_->ioptions.lowest_used_cache_tier);
  }

  if (!cache_handle) {
    UpdateCacheMissMetrics(TBlocklike::kBlockType, /* get_context = */ nullptr);
    return s;
  }

  // Found in Cache.
  TBlocklike* value = block_cache.Value(cache_handle);
  if (value) {
    UpdateCacheHitMetrics(TBlocklike::kBlockType, /* get_context = */ nullptr,
                          block_cache.get()->GetUsage(cache_handle));
  }
  out_parsed_block->SetCachedValue(value, block_cache.get(), cache_handle);

  assert(!out_parsed_block->IsEmpty());

  return s;
}

template <typename TBlocklike>
Status BlockBasedTable::CreateAndPinBlockInCache(
    const ReadOptions& ro, const BlockHandle& handle,
    UnownedPtr<Decompressor> decomp, BlockContents* contents,
    CachableEntry<TBlocklike>* out_parsed_block) const {
  CompressionType compression_type = GetBlockCompressionType(*contents);
  // If we don't own the contents and we don't need to decompress, copy
  // the block to heap in order to have ownership. If decompression is
  // needed, then the decompressor will allocate a buffer.
  if (!contents->own_bytes() && compression_type == kNoCompression) {
    Slice src = Slice(contents->data.data(), BlockSizeWithTrailer(handle));
    *contents = BlockContents(
        CopyBufferToHeap(GetMemoryAllocator(rep_->table_options), src),
        handle.size());
#ifndef NDEBUG
    contents->has_trailer = true;
#endif
  }

  Status s;
  if (ro.fill_cache) {
    s = MaybeReadBlockAndLoadToCache(nullptr, ro, handle, decomp,
                                     /*for_compaction=*/false, out_parsed_block,
                                     nullptr, nullptr, contents,
                                     /*async_read=*/false,
                                     /*use_block_cache_for_lookup=*/true);
  }

  if (!s.ok()) {
    return s;
  }

  // fill_cache could be false, or no block cache is configured. In that
  // case, decompress if necessary and take ownership of the block
  if (out_parsed_block->GetValue() == nullptr && contents != nullptr) {
    BlockContents tmp_contents;
    if (compression_type != kNoCompression) {
      s = DecompressSerializedBlock(contents->data.data(), handle.size(),
                                    compression_type, *decomp, &tmp_contents,
                                    rep_->ioptions,
                                    GetMemoryAllocator(rep_->table_options));
    } else {
      tmp_contents = std::move(*contents);
    }
    if (s.ok()) {
      std::unique_ptr<TBlocklike> block_holder;
      rep_->create_context.Create(&block_holder, std::move(tmp_contents));
      out_parsed_block->SetOwnedValue(std::move(block_holder));
    }
  }
  return s;
}

template <typename TBlocklike>
WithBlocklikeCheck<void, TBlocklike>
BlockBasedTable::SaveLookupContextOrTraceRecord(
    const Slice& block_key, bool is_cache_hit, const ReadOptions& ro,
    const TBlocklike* parsed_block_value,
    BlockCacheLookupContext* lookup_context) const {
  assert(lookup_context);
  size_t usage = 0;
  uint64_t nkeys = 0;
  if (parsed_block_value) {
    // Approximate the number of keys in the block using restarts.
    int interval = rep_->table_options.block_restart_interval;
    nkeys = interval * GetBlockNumRestarts(*parsed_block_value);
    // On average, the last restart should be just over half utilized.
    // Specifically, 1..N should be N/2 + 0.5. For example, 7 -> 4, 8 -> 4.5.
    // Use the get_id to alternate between rounding up vs. down.
    if (nkeys > 0) {
      bool rounding = static_cast<int>(lookup_context->get_id) & 1;
      nkeys -= (interval - rounding) / 2;
    }
    usage = parsed_block_value->ApproximateMemoryUsage();
  }
  TraceType trace_block_type = TraceType::kTraceMax;
  switch (TBlocklike::kBlockType) {
    case BlockType::kData:
      trace_block_type = TraceType::kBlockTraceDataBlock;
      break;
    case BlockType::kFilter:
    case BlockType::kFilterPartitionIndex:
      trace_block_type = TraceType::kBlockTraceFilterBlock;
      break;
    case BlockType::kCompressionDictionary:
      trace_block_type = TraceType::kBlockTraceUncompressionDictBlock;
      break;
    case BlockType::kRangeDeletion:
      trace_block_type = TraceType::kBlockTraceRangeDeletionBlock;
      break;
    case BlockType::kIndex:
    case BlockType::kUserDefinedIndex:
      trace_block_type = TraceType::kBlockTraceIndexBlock;
      break;
    case BlockType::kProperties:
    case BlockType::kHashIndexPrefixes:
    case BlockType::kHashIndexMetadata:
    case BlockType::kMetaIndex:
    case BlockType::kInvalid:
    default:
      // This cannot happen.
      assert(false);
      break;
  }
  const bool no_io = ro.read_tier == kBlockCacheTier;
  bool no_insert = no_io || !ro.fill_cache;
  if (BlockCacheTraceHelper::IsGetOrMultiGetOnDataBlock(
          trace_block_type, lookup_context->caller)) {
    // Make a copy of the block key here since it will be logged later.
    lookup_context->FillLookupContext(is_cache_hit, no_insert, trace_block_type,
                                      /*block_size=*/usage,
                                      block_key.ToString(), nkeys);

    // Defer logging the access to Get() and MultiGet() to trace additional
    // information, e.g., referenced_key
  } else {
    // Avoid making copy of block_key if it doesn't need to be saved in
    // BlockCacheLookupContext
    lookup_context->FillLookupContext(is_cache_hit, no_insert, trace_block_type,
                                      /*block_size=*/usage,
                                      /*block_key=*/{}, nkeys);

    // Fill in default values for irrelevant/unknown fields
    FinishTraceRecord(*lookup_context, block_key,
                      lookup_context->referenced_key,
                      /*does_referenced_key_exist*/ false,
                      /*referenced_data_size*/ 0);
  }
}

// Explicitly instantiate templates for each "blocklike" type we use.
// This makes it possible to keep the template definitions in the .cc include
// path rather than broad headers.
#define INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(T)                                \
  template Status BlockBasedTable::RetrieveBlock<T>(                           \
      FilePrefetchBuffer * prefetch_buffer, const ReadOptions& ro,             \
      const BlockHandle& handle, UnownedPtr<Decompressor> decomp,              \
      CachableEntry<T>* out_parsed_block, GetContext* get_context,             \
      BlockCacheLookupContext* lookup_context, bool for_compaction,            \
      bool use_cache, bool async_read, bool use_block_cache_for_lookup) const; \
  template Status BlockBasedTable::MaybeReadBlockAndLoadToCache<T>(            \
      FilePrefetchBuffer * prefetch_buffer, const ReadOptions& ro,             \
      const BlockHandle& handle, UnownedPtr<Decompressor> decomp,              \
      bool for_compaction, CachableEntry<T>* block_entry,                      \
      GetContext* get_context, BlockCacheLookupContext* lookup_context,        \
      BlockContents* contents, bool async_read,                                \
      bool use_block_cache_for_lookup) const;                                  \
  template Status BlockBasedTable::LookupAndPinBlocksInCache<T>(               \
      const ReadOptions& ro, const BlockHandle& handle,                        \
      CachableEntry<T>* out_parsed_block) const;                               \
  template Status BlockBasedTable::CreateAndPinBlockInCache<T>(                \
      const ReadOptions& ro, const BlockHandle& handle,                        \
      UnownedPtr<Decompressor> decomp, BlockContents* block_contents,          \
      CachableEntry<T>* out_parsed_block) const;

INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(ParsedFullFilterBlock);
INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(DecompressorDict);
INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(Block_kData);
INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(Block_kIndex);
INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(Block_kFilterPartitionIndex);
INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(Block_kRangeDeletion);
INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(Block_kMetaIndex);
INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES(Block_kUserDefinedIndex);

#undef INSTANTIATE_BLOCKLIKE_SYNC_TEMPLATES

#if USE_COROUTINES
#define INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(T)                                \
  template folly::coro::Task<Status>                                           \
  BlockBasedTable::RetrieveBlockCoroutine<T>(                                  \
      FilePrefetchBuffer * prefetch_buffer, const ReadOptions& ro,             \
      const BlockHandle& handle, UnownedPtr<Decompressor> decomp,              \
      CachableEntry<T>* out_parsed_block, GetContext* get_context,             \
      BlockCacheLookupContext* lookup_context, bool for_compaction,            \
      bool use_cache, bool async_read, bool use_block_cache_for_lookup) const; \
  template folly::coro::Task<Status>                                           \
  BlockBasedTable::MaybeReadBlockAndLoadToCacheCoroutine<T>(                   \
      FilePrefetchBuffer * prefetch_buffer, const ReadOptions& ro,             \
      const BlockHandle& handle, UnownedPtr<Decompressor> decomp,              \
      bool for_compaction, CachableEntry<T>* block_entry,                      \
      GetContext* get_context, BlockCacheLookupContext* lookup_context,        \
      BlockContents* contents, bool async_read,                                \
      bool use_block_cache_for_lookup) const;

INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(ParsedFullFilterBlock);
INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(DecompressorDict);
INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(Block_kData);
INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(Block_kIndex);
INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(Block_kFilterPartitionIndex);
INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(Block_kRangeDeletion);
INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(Block_kMetaIndex);
INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES(Block_kUserDefinedIndex);

#undef INSTANTIATE_BLOCKLIKE_CORO_TEMPLATES
#endif

#define INSTANTIATE_BLOCK_ITER_SYNC_TEMPLATE(T)                        \
  template T* BlockBasedTable::NewDataBlockIterator<T>(                \
      const ReadOptions& ro, const BlockHandle& handle, T* input_iter, \
      BlockType block_type, GetContext* get_context,                   \
      BlockCacheLookupContext* lookup_context,                         \
      FilePrefetchBuffer* prefetch_buffer, bool for_compaction,        \
      bool async_read, Status& s, bool use_block_cache_for_lookup) const;

INSTANTIATE_BLOCK_ITER_SYNC_TEMPLATE(DataBlockIter);
INSTANTIATE_BLOCK_ITER_SYNC_TEMPLATE(IndexBlockIter);

#undef INSTANTIATE_BLOCK_ITER_SYNC_TEMPLATE

#if USE_COROUTINES
#define INSTANTIATE_BLOCK_ITER_CORO_TEMPLATE(T)                        \
  template folly::coro::Task<T*>                                       \
  BlockBasedTable::NewDataBlockIteratorCoroutine<T>(                   \
      const ReadOptions& ro, const BlockHandle& handle, T* input_iter, \
      BlockType block_type, GetContext* get_context,                   \
      BlockCacheLookupContext* lookup_context,                         \
      FilePrefetchBuffer* prefetch_buffer, bool for_compaction,        \
      bool async_read, Status& s, bool use_block_cache_for_lookup) const;

INSTANTIATE_BLOCK_ITER_CORO_TEMPLATE(DataBlockIter);
INSTANTIATE_BLOCK_ITER_CORO_TEMPLATE(IndexBlockIter);

#undef INSTANTIATE_BLOCK_ITER_CORO_TEMPLATE
#endif

template DataBlockIter* BlockBasedTable::NewDataBlockIterator<DataBlockIter>(
    const ReadOptions& ro, CachableEntry<Block>& block,
    DataBlockIter* input_iter, Status s) const;

#if USE_COROUTINES
template folly::coro::Task<DataBlockIter*>
BlockBasedTable::NewDataBlockIteratorCoroutine<DataBlockIter>(
    const ReadOptions& ro, CachableEntry<Block>& block,
    DataBlockIter* input_iter, Status s) const;
#endif

void BlockBasedTable::FinishTraceRecord(
    const BlockCacheLookupContext& lookup_context, const Slice& block_key,
    const Slice& referenced_key, bool does_referenced_key_exist,
    uint64_t referenced_data_size) const {
  // Avoid making copy of referenced_key if it doesn't need to be saved in
  // BlockCacheLookupContext
  BlockCacheTraceRecord access_record(
      rep_->ioptions.clock->NowMicros(),
      /*block_key=*/"", lookup_context.block_type, lookup_context.block_size,
      rep_->cf_id_for_tracing(),
      /*cf_name=*/"", rep_->level_for_tracing(), rep_->sst_number_for_tracing(),
      lookup_context.caller, lookup_context.is_cache_hit,
      lookup_context.no_insert, lookup_context.get_id,
      lookup_context.get_from_user_specified_snapshot,
      /*referenced_key=*/"", referenced_data_size,
      lookup_context.num_keys_in_block, does_referenced_key_exist);
  // TODO: Should handle status here?
  block_cache_tracer_
      ->WriteBlockAccess(access_record, block_key, rep_->cf_name_for_tracing(),
                         referenced_key)
      .PermitUncheckedError();
}

BlockBasedTable::PartitionedIndexIteratorState::PartitionedIndexIteratorState(
    const BlockBasedTable* table,
    UnorderedMap<uint64_t, CachableEntry<Block>>* block_map)
    : table_(table), block_map_(block_map) {}

InternalIteratorBase<IndexValue>*
BlockBasedTable::PartitionedIndexIteratorState::NewSecondaryIterator(
    const BlockHandle& handle) {
  // Return a block iterator on the index partition
  auto block = block_map_->find(handle.offset());
  // block_map_ must be exhaustive
  if (block == block_map_->end()) {
    assert(false);
    // Signal problem to caller
    return nullptr;
  }
  const Rep* rep = table_->get_rep();
  assert(rep);

  Statistics* kNullStats = nullptr;
  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  return block->second.GetValue()->NewIndexIterator(
      rep->internal_comparator.user_comparator(),
      rep->get_global_seqno(BlockType::kIndex), nullptr, kNullStats, true,
      rep->index_has_first_key, rep->index_key_includes_seq,
      rep->index_value_is_full, /*block_contents_pinned=*/false,
      rep->user_defined_timestamps_persisted, nullptr /* prefix_index */,
      BlockBasedTableOptions::kBinary,
      FormatVersionUsesValueDeltaEscape(rep->footer.format_version()));
}

// This will be broken if the user specifies an unusual implementation
// of Options.comparator, or if the user specifies an unusual
// definition of prefixes in BlockBasedTableOptions.filter_policy.
// In particular, we require the following three properties:
//
// 1) key.starts_with(prefix(key))
// 2) Compare(prefix(key), key) <= 0.
// 3) If Compare(key1, key2) <= 0, then Compare(prefix(key1), prefix(key2)) <= 0
//
// If read_options.read_tier == kBlockCacheTier, this method will do no I/O and
// will return true if the filter block is not in memory and not found in block
// cache.
//
// REQUIRES: this method shouldn't be called while the DB lock is held.
bool BlockBasedTable::PrefixRangeMayMatch(
    const Slice& internal_key, const ReadOptions& read_options,
    const SliceTransform* options_prefix_extractor,
    const bool need_upper_bound_check, BlockCacheLookupContext* lookup_context,
    bool* filter_checked) const {
  if (!rep_->filter_policy) {
    return true;
  }

  const SliceTransform* prefix_extractor;

  if (rep_->table_prefix_extractor == nullptr) {
    if (need_upper_bound_check) {
      return true;
    }
    prefix_extractor = options_prefix_extractor;
  } else {
    prefix_extractor = rep_->table_prefix_extractor.get();
  }
  auto ts_sz = rep_->internal_comparator.user_comparator()->timestamp_size();
  auto user_key_without_ts =
      ExtractUserKeyAndStripTimestamp(internal_key, ts_sz);
  if (!prefix_extractor->InDomain(user_key_without_ts)) {
    return true;
  }

  bool may_match = true;

  FilterBlockReader* const filter = rep_->filter.get();
  *filter_checked = false;
  if (filter != nullptr) {
    const Slice* const const_ikey_ptr = &internal_key;
    may_match = filter->RangeMayExist(
        read_options.iterate_upper_bound, user_key_without_ts, prefix_extractor,
        rep_->internal_comparator.user_comparator(), const_ikey_ptr,
        filter_checked, need_upper_bound_check, lookup_context, read_options);
  }

  return may_match;
}

bool BlockBasedTable::PrefixExtractorChanged(
    const SliceTransform* prefix_extractor) const {
  if (prefix_extractor == nullptr) {
    return true;
  } else if (prefix_extractor == rep_->table_prefix_extractor.get()) {
    return false;
  } else {
    return PrefixExtractorChangedHelper(rep_->table_properties.get(),
                                        prefix_extractor);
  }
}

Statistics* BlockBasedTable::GetStatistics() const {
  return rep_->ioptions.stats;
}
bool BlockBasedTable::IsLastLevel() const {
  return rep_->level == rep_->ioptions.num_levels - 1;
}

InternalIterator* BlockBasedTable::NewIterator(
    const ReadOptions& read_options, const SliceTransform* prefix_extractor,
    Arena* arena, bool skip_filters, TableReaderCaller caller,
    size_t compaction_readahead_size, bool allow_unprepared_value) {
  return NewIteratorImpl(read_options, prefix_extractor, arena, skip_filters,
                         caller, compaction_readahead_size,
                         allow_unprepared_value, /*seek_target=*/nullptr);
}

InternalIterator* BlockBasedTable::NewIteratorForSeek(
    const ReadOptions& read_options, const SliceTransform* prefix_extractor,
    Arena* arena, bool skip_filters, TableReaderCaller caller,
    size_t compaction_readahead_size, bool allow_unprepared_value,
    const Slice& seek_target) {
  return NewIteratorImpl(read_options, prefix_extractor, arena, skip_filters,
                         caller, compaction_readahead_size,
                         allow_unprepared_value, &seek_target);
}

InternalIterator* BlockBasedTable::NewIteratorImpl(
    const ReadOptions& read_options, const SliceTransform* prefix_extractor,
    Arena* arena, bool skip_filters, TableReaderCaller caller,
    size_t compaction_readahead_size, bool allow_unprepared_value,
    const Slice* seek_target) {
  MarkReadStarted();
  BlockCacheLookupContext lookup_context{caller};
  bool need_upper_bound_check =
      read_options.auto_prefix_mode || PrefixExtractorChanged(prefix_extractor);
  bool check_filter =
      !skip_filters &&
      GetIteratorPrefixMode(read_options, prefix_extractor != nullptr) !=
          IteratorPrefixMode::kTotalOrder;
  // Runs the check that the iterator's first Seek() would run, with the same
  // arguments.
  auto first_seek_prefix_check =
      BlockBasedTableIterator::FirstSeekPrefixCheck::kNotRun;
  if (seek_target != nullptr && check_filter &&
      rep_->fragmented_range_dels == nullptr) {
    bool filter_checked = false;
    if (!PrefixRangeMayMatch(*seek_target, read_options, prefix_extractor,
                             need_upper_bound_check, &lookup_context,
                             &filter_checked)) {
      RecordTick(rep_->ioptions.stats, IsLastLevel()
                                           ? LAST_LEVEL_SEEK_FILTERED
                                           : NON_LAST_LEVEL_SEEK_FILTERED);
      return nullptr;
    }
    first_seek_prefix_check =
        filter_checked
            ? BlockBasedTableIterator::FirstSeekPrefixCheck::kPassedFilter
            : BlockBasedTableIterator::FirstSeekPrefixCheck::
                  kPassedWithoutFilter;
  }
  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter(NewIndexIterator(
      read_options,
      /*disable_prefix_seek=*/need_upper_bound_check &&
          rep_->index_type == BlockBasedTableOptions::kHashSearch,
      /*input_iter=*/nullptr, /*get_context=*/nullptr, &lookup_context));
  // Same-file ("embedded") blob references are resolved by a wrapper iterator
  // so the block-based iterator never exposes an unresolved BlobIndex. The SST
  // dump tool and the blob relocation reader must keep seeing raw values, so
  // they are excluded. The relocation reader checks the key type of each
  // location entry itself.
  const bool resolve_embedded_values =
      caller != TableReaderCaller::kSSTDumpTool &&
      caller != TableReaderCaller::kBlobRelocationFileRead &&
      HasEmbeddedBlobRecords();
  // When wrapping, the inner iterator must materialize each data block (it must
  // not defer to the index's first key), so force allow_unprepared_value off;
  // the wrapper re-introduces value laziness above it.
  const bool inner_allow_unprepared_value =
      allow_unprepared_value && !resolve_embedded_values;

  if (arena == nullptr) {
    auto* iter = new BlockBasedTableIterator(
        this, read_options, rep_->internal_comparator, std::move(index_iter),
        check_filter, need_upper_bound_check, prefix_extractor, caller,
        first_seek_prefix_check, compaction_readahead_size,
        inner_allow_unprepared_value);
    if (!resolve_embedded_values) {
      return iter;
    }
    if (allow_unprepared_value) {
      return new LazyEmbeddedBlobResolvingIterator(this, read_options, iter,
                                                   /*arena_mode=*/false);
    }
    return new EagerEmbeddedBlobResolvingIterator(this, read_options, iter,
                                                  /*arena_mode=*/false);
  } else {
    auto* mem = arena->AllocateAligned(sizeof(BlockBasedTableIterator));
    auto* iter = new (mem) BlockBasedTableIterator(
        this, read_options, rep_->internal_comparator, std::move(index_iter),
        check_filter, need_upper_bound_check, prefix_extractor, caller,
        first_seek_prefix_check, compaction_readahead_size,
        inner_allow_unprepared_value);
    if (!resolve_embedded_values) {
      return iter;
    }
    if (allow_unprepared_value) {
      auto* wrap_mem =
          arena->AllocateAligned(sizeof(LazyEmbeddedBlobResolvingIterator));
      return new (wrap_mem) LazyEmbeddedBlobResolvingIterator(
          this, read_options, iter, /*arena_mode=*/true);
    }
    auto* wrap_mem =
        arena->AllocateAligned(sizeof(EagerEmbeddedBlobResolvingIterator));
    return new (wrap_mem) EagerEmbeddedBlobResolvingIterator(
        this, read_options, iter, /*arena_mode=*/true);
  }
}

FragmentedRangeTombstoneIterator* BlockBasedTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options) {
  if (rep_->fragmented_range_dels == nullptr) {
    return nullptr;
  }
  SequenceNumber snapshot = kMaxSequenceNumber;
  if (read_options.snapshot != nullptr) {
    snapshot = read_options.snapshot->GetSequenceNumber();
  }
  return new FragmentedRangeTombstoneIterator(rep_->fragmented_range_dels,
                                              rep_->internal_comparator,
                                              snapshot, read_options.timestamp);
}

FragmentedRangeTombstoneIterator* BlockBasedTable::NewRangeTombstoneIterator(
    SequenceNumber read_seqno, const Slice* timestamp) {
  if (rep_->fragmented_range_dels == nullptr) {
    return nullptr;
  }
  return new FragmentedRangeTombstoneIterator(rep_->fragmented_range_dels,
                                              rep_->internal_comparator,
                                              read_seqno, timestamp);
}

bool BlockBasedTable::FullFilterKeyMayMatch(
    FilterBlockReader* filter, const Slice& internal_key,
    const SliceTransform* prefix_extractor, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) const {
  if (filter == nullptr) {
    return true;
  }
  Slice user_key = ExtractUserKey(internal_key);
  const Slice* const const_ikey_ptr = &internal_key;
  bool may_match = true;
  size_t ts_sz = rep_->internal_comparator.user_comparator()->timestamp_size();
  Slice user_key_without_ts = StripTimestampFromUserKey(user_key, ts_sz);
  if (rep_->whole_key_filtering) {
    may_match = filter->KeyMayMatch(user_key_without_ts, const_ikey_ptr,
                                    get_context, lookup_context, read_options);
    if (may_match) {
      RecordTick(rep_->ioptions.stats, BLOOM_FILTER_FULL_POSITIVE);
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, 1, rep_->level);
    } else {
      RecordTick(rep_->ioptions.stats, BLOOM_FILTER_USEFUL);
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, rep_->level);
    }
  } else if (!PrefixExtractorChanged(prefix_extractor) &&
             prefix_extractor->InDomain(user_key_without_ts)) {
    // FIXME ^^^: there should be no reason for Get() to depend on current
    // prefix_extractor at all. It should always use table_prefix_extractor.
    may_match = filter->PrefixMayMatch(
        prefix_extractor->Transform(user_key_without_ts), const_ikey_ptr,
        get_context, lookup_context, read_options);
    RecordTick(rep_->ioptions.stats, BLOOM_FILTER_PREFIX_CHECKED);
    if (may_match) {
      // Includes prefix stats
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, 1, rep_->level);
    } else {
      RecordTick(rep_->ioptions.stats, BLOOM_FILTER_PREFIX_USEFUL);
      // Includes prefix stats
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, rep_->level);
    }
  }
  return may_match;
}

void BlockBasedTable::FullFilterKeysMayMatch(
    FilterBlockReader* filter, MultiGetRange* range,
    const SliceTransform* prefix_extractor,
    BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) const {
  if (filter == nullptr) {
    return;
  }
  uint64_t before_keys = range->KeysLeft();
  assert(before_keys > 0);  // Caller should ensure
  if (rep_->whole_key_filtering) {
    filter->KeysMayMatch(range, lookup_context, read_options);
    uint64_t after_keys = range->KeysLeft();
    if (after_keys) {
      RecordTick(rep_->ioptions.stats, BLOOM_FILTER_FULL_POSITIVE, after_keys);
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, after_keys,
                                rep_->level);
    }
    uint64_t filtered_keys = before_keys - after_keys;
    if (filtered_keys) {
      RecordTick(rep_->ioptions.stats, BLOOM_FILTER_USEFUL, filtered_keys);
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, filtered_keys,
                                rep_->level);
    }
  } else if (!PrefixExtractorChanged(prefix_extractor)) {
    // FIXME ^^^: there should be no reason for MultiGet() to depend on current
    // prefix_extractor at all. It should always use table_prefix_extractor.
    filter->PrefixesMayMatch(range, prefix_extractor, lookup_context,
                             read_options);
    RecordTick(rep_->ioptions.stats, BLOOM_FILTER_PREFIX_CHECKED, before_keys);
    uint64_t after_keys = range->KeysLeft();
    if (after_keys) {
      // Includes prefix stats
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, after_keys,
                                rep_->level);
    }
    uint64_t filtered_keys = before_keys - after_keys;
    if (filtered_keys) {
      RecordTick(rep_->ioptions.stats, BLOOM_FILTER_PREFIX_USEFUL,
                 filtered_keys);
      // Includes prefix stats
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, filtered_keys,
                                rep_->level);
    }
  }
}

Status BlockBasedTable::ApproximateKeyAnchors(const ReadOptions& read_options,
                                              std::vector<Anchor>& anchors) {
  // We iterator the whole index block here. More efficient implementation
  // is possible if we push this operation into IndexReader. For example, we
  // can directly sample from restart block entries in the index block and
  // only read keys needed. Here we take a simple solution. Performance is
  // likely not to be a problem. We are compacting the whole file, so all
  // keys will be read out anyway. An extra read to index block might be
  // a small share of the overhead. We can try to optimize if needed.
  //
  // `CacheDependencies()` brings all the blocks into cache using one I/O. That
  // way the full index scan usually finds the index data it is looking for in
  // cache rather than doing an I/O for each "dependency" (partition).
  Status s = rep_->index_reader->CacheDependencies(
      read_options, false /* pin */, nullptr /* prefetch_buffer */);
  if (!s.ok()) {
    return s;
  }

  IndexBlockIter iiter_on_stack;
  auto iiter = NewIndexIterator(
      read_options, /*disable_prefix_seek=*/false, &iiter_on_stack,
      /*get_context=*/nullptr, /*lookup_context=*/nullptr);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr.reset(iiter);
  }

  // If needed the threshold could be more adaptive. For example, it can be
  // based on size, so that a larger will be sampled to more partitions than a
  // smaller file. The size might also need to be passed in by the caller based
  // on total compaction size.
  const uint64_t kMaxNumAnchors = uint64_t{128};
  uint64_t num_blocks = this->GetTableProperties()->num_data_blocks;
  uint64_t num_blocks_per_anchor = num_blocks / kMaxNumAnchors;
  if (num_blocks_per_anchor == 0) {
    num_blocks_per_anchor = 1;
  }

  uint64_t count = 0;
  std::string last_key;
  uint64_t range_size = 0;
  uint64_t prev_offset = 0;
  for (iiter->SeekToFirst(); iiter->Valid(); iiter->Next()) {
    const BlockHandle& bh = iiter->value().handle;
    range_size += bh.offset() + bh.size() - prev_offset;
    prev_offset = bh.offset() + bh.size();
    if (++count % num_blocks_per_anchor == 0) {
      count = 0;
      anchors.emplace_back(iiter->user_key(), range_size);
      range_size = 0;
    } else {
      last_key = iiter->user_key().ToString();
    }
  }
  if (count != 0) {
    anchors.emplace_back(last_key, range_size);
  }
  return Status::OK();
}

bool BlockBasedTable::TimestampMayMatch(const ReadOptions& read_options) const {
  if (read_options.timestamp != nullptr && !rep_->min_timestamp.empty()) {
    RecordTick(rep_->ioptions.stats, TIMESTAMP_FILTER_TABLE_CHECKED);
    auto read_ts = read_options.timestamp;
    auto comparator = rep_->internal_comparator.user_comparator();
    if (comparator->CompareTimestamp(*read_ts, rep_->min_timestamp) < 0) {
      RecordTick(rep_->ioptions.stats, TIMESTAMP_FILTER_TABLE_FILTERED);
      return false;
    }
  }
  return true;
}

Status BlockBasedTable::MultiGetFilter(const ReadOptions& read_options,
                                       const SliceTransform* prefix_extractor,
                                       MultiGetRange* mget_range) {
  if (mget_range->empty()) {
    // Caller should ensure non-empty (performance bug)
    assert(false);
    return Status::OK();  // Nothing to do
  }

  FilterBlockReader* const filter = rep_->filter.get();
  if (!filter) {
    return Status::OK();
  }

  // First check the full filter
  // If full filter not useful, Then go into each block
  uint64_t tracing_mget_id = BlockCacheTraceHelper::kReservedGetId;
  if (mget_range->begin()->get_context) {
    tracing_mget_id = mget_range->begin()->get_context->get_tracing_get_id();
  }
  BlockCacheLookupContext lookup_context{
      TableReaderCaller::kUserMultiGet, tracing_mget_id,
      /*_get_from_user_specified_snapshot=*/read_options.snapshot != nullptr};
  FullFilterKeysMayMatch(filter, mget_range, prefix_extractor, &lookup_context,
                         read_options);

  return Status::OK();
}

Status BlockBasedTable::Prefetch(const ReadOptions& read_options,
                                 const Slice* const begin,
                                 const Slice* const end) {
  auto& comparator = rep_->internal_comparator;
  UserComparatorWrapper user_comparator(comparator.user_comparator());
  // pre-condition
  if (begin && end && comparator.Compare(*begin, *end) > 0) {
    return Status::InvalidArgument(*begin, *end);
  }
  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};
  IndexBlockIter iiter_on_stack;
  auto iiter = NewIndexIterator(read_options, /*disable_prefix_seek=*/false,
                                &iiter_on_stack, /*get_context=*/nullptr,
                                &lookup_context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIteratorBase<IndexValue>>(iiter);
  }

  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }

  // indicates if we are on the last page that need to be pre-fetched
  bool prefetching_boundary_page = false;

  for (begin ? iiter->Seek(*begin) : iiter->SeekToFirst(); iiter->Valid();
       iiter->Next()) {
    BlockHandle block_handle = iiter->value().handle;
    const bool is_user_key = !rep_->index_key_includes_seq;
    if (end &&
        ((!is_user_key && comparator.Compare(iiter->key(), *end) >= 0) ||
         (is_user_key && user_comparator.Compare(iiter->user_key(),
                                                 ExtractUserKey(*end)) >= 0))) {
      if (prefetching_boundary_page) {
        break;
      }

      // The index entry represents the last key in the data block.
      // We should load this page into memory as well, but no more
      prefetching_boundary_page = true;
    }

    // Load the block specified by the block_handle into the block cache
    DataBlockIter biter;
    Status tmp_status;
    NewDataBlockIterator<DataBlockIter>(
        read_options, block_handle, &biter, /*block_type=*/BlockType::kData,
        /*get_context=*/nullptr, &lookup_context,
        /*prefetch_buffer=*/nullptr, /*for_compaction=*/false,
        /*async_read=*/false, tmp_status, /*use_block_cache_for_lookup=*/true);

    if (!biter.status().ok()) {
      // there was an unexpected error while pre-fetching
      return biter.status();
    }
  }

  return Status::OK();
}

Status BlockBasedTable::VerifyChecksum(const ReadOptions& read_options,
                                       TableReaderCaller caller,
                                       bool meta_blocks_only) {
  Status s;
  // Check Meta blocks
  std::unique_ptr<Block> metaindex;
  std::unique_ptr<InternalIterator> metaindex_iter;
  s = ReadMetaIndexBlock(read_options, nullptr /* prefetch buffer */,
                         &metaindex, &metaindex_iter);
  if (s.ok()) {
    s = VerifyChecksumInMetaBlocks(read_options, metaindex_iter.get());
    if (!s.ok()) {
      return s;
    }
  } else {
    return s;
  }
  // The meta index names only the index block of the exact blob reference
  // metadata, not its data blocks. So for an SST that has this metadata (the
  // descriptor property exists), also read and check every reference data
  // block. This read is in proportion to the size of the reference metadata,
  // and SSTs without this metadata do not pay it. It runs also with
  // meta_blocks_only, because the reference blocks are metadata that a scan of
  // the table's entries does not read.
  if (rep_->table_properties != nullptr) {
    const UserCollectedProperties::const_iterator descriptor =
        rep_->table_properties->user_collected_properties.find(
            kBlobReferenceDescriptorPropertyName);
    if (descriptor != rep_->table_properties->user_collected_properties.end()) {
      s = VerifyBlobReferenceMetadata(read_options, descriptor->second);
      if (!s.ok()) {
        return s;
      }
    }
  }
  if (meta_blocks_only) {
    return s;
  }
  // Check Data blocks
  IndexBlockIter iiter_on_stack;
  BlockCacheLookupContext context{caller};
  InternalIteratorBase<IndexValue>* iiter = NewIndexIterator(
      read_options, /*disable_prefix_seek=*/false, &iiter_on_stack,
      /*get_context=*/nullptr, &context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIteratorBase<IndexValue>>(iiter);
  }
  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }
  s = VerifyChecksumInBlocks(read_options, iiter);
  return s;
}

Status BlockBasedTable::VerifyChecksumInBlocks(
    const ReadOptions& read_options,
    InternalIteratorBase<IndexValue>* index_iter) {
  Status s;
  // We are scanning the whole file, so no need to do exponential
  // increasing of the buffer size.
  size_t readahead_size = (read_options.readahead_size != 0)
                              ? read_options.readahead_size
                              : rep_->table_options.max_auto_readahead_size;
  // FilePrefetchBuffer doesn't work in mmap mode and readahead is not
  // needed there.
  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = readahead_size;
  readahead_params.max_readahead_size = readahead_size;
  FilePrefetchBuffer prefetch_buffer(
      readahead_params, !rep_->ioptions.allow_mmap_reads /* enable */);
  if (rep_->readahead_cache_res_mgr) {
    prefetch_buffer.SetCacheReservationManager(rep_->readahead_cache_res_mgr);
  }

  for (index_iter->SeekToFirst(); index_iter->Valid(); index_iter->Next()) {
    s = index_iter->status();
    if (!s.ok()) {
      break;
    }
    BlockHandle handle = index_iter->value().handle;
    BlockContents contents;
    BlockFetcher block_fetcher(
        rep_->file.get(), &prefetch_buffer, rep_->footer, read_options, handle,
        &contents, rep_->ioptions, false /* decompress */,
        false /*maybe_compressed*/, BlockType::kData, nullptr /*decompressor*/,
        rep_->persistent_cache_options);
    s = block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      break;
    }
  }
  if (s.ok()) {
    // In the case of two level indexes, we would have exited the above loop
    // by checking index_iter->Valid(), but Valid() might have returned false
    // due to an IO error. So check the index_iter status
    s = index_iter->status();
  }
  return s;
}

BlockType BlockBasedTable::GetBlockTypeForMetaBlockByName(
    const Slice& meta_block_name) {
  if (meta_block_name.starts_with(kFullFilterBlockPrefix)) {
    return BlockType::kFilter;
  }

  if (meta_block_name.starts_with(kPartitionedFilterBlockPrefix)) {
    return BlockType::kFilterPartitionIndex;
  }

  if (meta_block_name == kPropertiesBlockName) {
    return BlockType::kProperties;
  }

  if (meta_block_name == kCompressionDictBlockName) {
    return BlockType::kCompressionDictionary;
  }

  if (meta_block_name == kRangeDelBlockName) {
    return BlockType::kRangeDeletion;
  }

  if (meta_block_name == kHashIndexPrefixesBlock) {
    return BlockType::kHashIndexPrefixes;
  }

  if (meta_block_name == kHashIndexPrefixesMetadataBlock) {
    return BlockType::kHashIndexMetadata;
  }

  if (meta_block_name == kIndexBlockName) {
    return BlockType::kIndex;
  }

  if (meta_block_name.starts_with(kIndexFactoryMetaPrefix)) {
    return BlockType::kUserDefinedIndex;
  }

  // The blob reference index block has no BlockType of its own.
  // VerifyChecksumInMetaBlocks() reads every block in the meta-index and needs
  // a type for each (an unknown name fails the assert below).
  // kUserDefinedIndex blocks are kept as plain bytes (Block_kUserDefinedIndex
  // in block_cache.h is a BlockContents), so no code parses them as a Block.
  if (meta_block_name == kBlobReferenceIndexBlockName) {
    return BlockType::kUserDefinedIndex;
  }

  if (meta_block_name.starts_with(kObsoleteFilterBlockPrefix)) {
    // Obsolete but possible in old files
    return BlockType::kInvalid;
  }

  assert(false);
  return BlockType::kInvalid;
}

Status BlockBasedTable::VerifyChecksumInMetaBlocks(
    const ReadOptions& read_options, InternalIteratorBase<Slice>* index_iter) {
  Status s;
  for (index_iter->SeekToFirst(); index_iter->Valid(); index_iter->Next()) {
    s = index_iter->status();
    if (!s.ok()) {
      break;
    }
    BlockHandle handle;
    Slice input = index_iter->value();
    s = handle.DecodeFrom(&input);
    if (!s.ok()) {
      break;
    }
    BlockContents contents;
    const Slice meta_block_name = index_iter->key();
    if (meta_block_name == kPropertiesBlockName) {
      // Unfortunate special handling for properties block checksum w/
      // global seqno
      std::unique_ptr<TableProperties> table_properties;
      s = ReadTablePropertiesHelper(read_options, handle, rep_->file.get(),
                                    nullptr /* prefetch_buffer */, rep_->footer,
                                    rep_->ioptions, &table_properties,
                                    nullptr /* memory_allocator */);
    } else if (rep_->verify_checksum_set_on_open &&
               meta_block_name == kIndexBlockName) {
      // WART: For now, to maintain similar I/O behavior as before
      // format_version=6, we skip verifying index block checksum--but only
      // if it was checked on open.
    } else {
      // FIXME? Need to verify checksums of index and filter partitions?
      s = BlockFetcher(rep_->file.get(), nullptr /* prefetch buffer */,
                       rep_->footer, read_options, handle, &contents,
                       rep_->ioptions, false /* decompress */,
                       false /*maybe_compressed*/,
                       GetBlockTypeForMetaBlockByName(meta_block_name),
                       nullptr /*decompressor*/, rep_->persistent_cache_options)
              .ReadBlockContents();
    }
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

bool BlockBasedTable::EraseFromCache(const BlockHandle& handle) const {
  assert(rep_ != nullptr);

  Cache* const cache = rep_->table_options.block_cache.get();
  if (cache == nullptr) {
    return false;
  }

  CacheKey key = GetCacheKey(rep_->base_cache_key, handle);

  Cache::Handle* const cache_handle = cache->Lookup(key.AsSlice());
  if (cache_handle == nullptr) {
    return false;
  }

  return cache->Release(cache_handle, /*erase_if_last_ref=*/true);
}

bool BlockBasedTable::TEST_BlockInCache(const BlockHandle& handle) const {
  assert(rep_ != nullptr);

  Cache* const cache = rep_->table_options.block_cache.get();
  if (cache == nullptr) {
    return false;
  }

  CacheKey key = GetCacheKey(rep_->base_cache_key, handle);

  Cache::Handle* const cache_handle = cache->Lookup(key.AsSlice());
  if (cache_handle == nullptr) {
    return false;
  }

  cache->Release(cache_handle);

  return true;
}

bool BlockBasedTable::TEST_KeyInCache(const ReadOptions& options,
                                      const Slice& key) {
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter(NewIndexIterator(
      options, /*disable_prefix_seek=*/false, /*input_iter=*/nullptr,
      /*get_context=*/nullptr, /*lookup_context=*/nullptr));
  iiter->Seek(key);
  assert(iiter->status().ok());
  assert(iiter->Valid());

  return TEST_BlockInCache(iiter->value().handle);
}

void BlockBasedTable::TEST_GetDataBlockHandle(const ReadOptions& options,
                                              const Slice& key,
                                              BlockHandle& handle) {
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter(NewIndexIterator(
      options, /*disable_prefix_seek=*/false, /*input_iter=*/nullptr,
      /*get_context=*/nullptr, /*lookup_context=*/nullptr));
  iiter->Seek(key);
  assert(iiter->Valid());
  handle = iiter->value().handle;
}

// REQUIRES: The following fields of rep_ should have already been populated:
//  1. file
//  2. index_handle,
//  3. options
//  4. internal_comparator
//  5. index_type
Status BlockBasedTable::CreateIndexReader(
    const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
    InternalIterator* meta_iter, bool use_cache, bool prefetch, bool pin,
    BlockCacheLookupContext* lookup_context,
    std::unique_ptr<IndexReader>* index_reader) {
  if (FormatVersionUsesIndexHandleInFooter(rep_->footer.format_version())) {
    rep_->index_handle = rep_->footer.index_handle();
  } else {
    Status s = FindMetaBlock(meta_iter, kIndexBlockName, &rep_->index_handle);
    if (!s.ok()) {
      return s;
    }
  }

  switch (rep_->index_type) {
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      return PartitionIndexReader::Create(this, ro, prefetch_buffer, use_cache,
                                          prefetch, pin, lookup_context,
                                          index_reader);
    }
    case BlockBasedTableOptions::kBinarySearch:
      FALLTHROUGH_INTENDED;
    case BlockBasedTableOptions::kBinarySearchWithFirstKey: {
      return BinarySearchIndexReader::Create(this, ro, prefetch_buffer,
                                             use_cache, prefetch, pin,
                                             lookup_context, index_reader);
    }
    case BlockBasedTableOptions::kHashSearch: {
      if (!rep_->table_prefix_extractor) {
        ROCKS_LOG_WARN(rep_->ioptions.logger,
                       "Missing prefix extractor for hash index. Fall back to"
                       " binary search index.");
        return BinarySearchIndexReader::Create(this, ro, prefetch_buffer,
                                               use_cache, prefetch, pin,
                                               lookup_context, index_reader);
      } else {
        return HashIndexReader::Create(this, ro, prefetch_buffer, meta_iter,
                                       use_cache, prefetch, pin, lookup_context,
                                       index_reader);
      }
    }
    default: {
      std::string error_message =
          "Unrecognized index type: " + std::to_string(rep_->index_type);
      return Status::InvalidArgument(error_message.c_str());
    }
  }
}

uint64_t BlockBasedTable::ApproximateDataOffsetOf(
    const InternalIteratorBase<IndexValue>& index_iter,
    uint64_t data_size) const {
  assert(index_iter.status().ok());
  if (index_iter.Valid()) {
    BlockHandle handle = index_iter.value().handle;
    return handle.offset();
  } else {
    // The iterator is past the last key in the file.
    return data_size;
  }
}

uint64_t BlockBasedTable::GetApproximateDataSize() {
  return rep_->table_properties->data_size;
}

uint64_t BlockBasedTable::ApproximateOffsetOf(const ReadOptions& read_options,
                                              const Slice& key,
                                              TableReaderCaller caller) {
  uint64_t data_size = GetApproximateDataSize();
  if (UNLIKELY(data_size == 0)) {
    // Hmm. Let's just split in half to avoid skewing one way or another,
    // since we don't know whether we're operating on lower bound or
    // upper bound.
    return rep_->file_size / 2;
  }

  BlockCacheLookupContext context(caller);
  IndexBlockIter iiter_on_stack;
  auto index_iter =
      NewIndexIterator(read_options, /*disable_prefix_seek=*/true,
                       /*input_iter=*/&iiter_on_stack, /*get_context=*/nullptr,
                       /*lookup_context=*/&context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
  if (index_iter != &iiter_on_stack) {
    iiter_unique_ptr.reset(index_iter);
  }

  index_iter->Seek(key);
  uint64_t offset;
  if (index_iter->status().ok()) {
    offset = ApproximateDataOffsetOf(*index_iter, data_size);
  } else {
    // Split in half to avoid skewing one way or another,
    // since we don't know whether we're operating on lower bound or
    // upper bound.
    return rep_->file_size / 2;
  }

  // Pro-rate file metadata (incl filters) size-proportionally across data
  // blocks.
  double size_ratio =
      static_cast<double>(offset) / static_cast<double>(data_size);
  return static_cast<uint64_t>(size_ratio *
                               static_cast<double>(rep_->file_size));
}

uint64_t BlockBasedTable::ApproximateSize(const ReadOptions& read_options,
                                          const Slice& start, const Slice& end,
                                          TableReaderCaller caller) {
  assert(rep_->internal_comparator.Compare(start, end) <= 0);

  uint64_t data_size = GetApproximateDataSize();
  if (UNLIKELY(data_size == 0)) {
    // Hmm. Assume whole file is involved, since we have lower and upper
    // bound. This likely skews the estimate if we consider that this function
    // is typically called with `[start, end]` fully contained in the file's
    // key-range.
    return rep_->file_size;
  }

  BlockCacheLookupContext context(caller);
  IndexBlockIter iiter_on_stack;
  auto index_iter =
      NewIndexIterator(read_options, /*disable_prefix_seek=*/true,
                       /*input_iter=*/&iiter_on_stack, /*get_context=*/nullptr,
                       /*lookup_context=*/&context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
  if (index_iter != &iiter_on_stack) {
    iiter_unique_ptr.reset(index_iter);
  }

  index_iter->Seek(start);
  uint64_t start_offset;
  if (index_iter->status().ok()) {
    start_offset = ApproximateDataOffsetOf(*index_iter, data_size);
  } else {
    // Assume file is involved from the start. This likely skews the estimate
    // but is consistent with the above error handling.
    start_offset = 0;
  }

  index_iter->Seek(end);
  uint64_t end_offset;
  if (index_iter->status().ok()) {
    end_offset = ApproximateDataOffsetOf(*index_iter, data_size);
  } else {
    // Assume file is involved until the end. This likely skews the estimate
    // but is consistent with the above error handling.
    end_offset = data_size;
  }

  assert(end_offset >= start_offset);
  // Pro-rate file metadata (incl filters) size-proportionally across data
  // blocks.
  double size_ratio = static_cast<double>(end_offset - start_offset) /
                      static_cast<double>(data_size);
  return static_cast<uint64_t>(size_ratio *
                               static_cast<double>(rep_->file_size));
}

bool BlockBasedTable::TEST_FilterBlockInCache() const {
  assert(rep_ != nullptr);
  return rep_->filter_type != Rep::FilterType::kNoFilter &&
         TEST_BlockInCache(rep_->filter_handle);
}

bool BlockBasedTable::TEST_IndexBlockInCache() const {
  assert(rep_ != nullptr);

  return TEST_BlockInCache(rep_->index_handle);
}

Status BlockBasedTable::GetKVPairsFromDataBlocks(
    const ReadOptions& read_options, std::vector<KVPairBlock>* kv_pair_blocks) {
  std::unique_ptr<InternalIteratorBase<IndexValue>> blockhandles_iter(
      NewIndexIterator(read_options, /*disable_prefix_seek=*/false,
                       /*input_iter=*/nullptr, /*get_context=*/nullptr,
                       /*lookup_context=*/nullptr));

  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    // Cannot read Index Block
    return s;
  }

  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();

    if (!s.ok()) {
      break;
    }

    std::unique_ptr<InternalIterator> datablock_iter;
    Status tmp_status;
    datablock_iter.reset(NewDataBlockIterator<DataBlockIter>(
        read_options, blockhandles_iter->value().handle,
        /*input_iter=*/nullptr, /*block_type=*/BlockType::kData,
        /*get_context=*/nullptr, /*lookup_context=*/nullptr,
        /*prefetch_buffer=*/nullptr, /*for_compaction=*/false,
        /*async_read=*/false, tmp_status, /*use_block_cache_for_lookup=*/true));
    s = datablock_iter->status();

    if (!s.ok()) {
      // Error reading the block - Skipped
      continue;
    }

    KVPairBlock kv_pair_block;
    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        // Error reading the block - Skipped
        break;
      }
      const Slice& key = datablock_iter->key();
      const Slice& value = datablock_iter->value();
      std::string key_copy = std::string(key.data(), key.size());
      std::string value_copy = std::string(value.data(), value.size());

      kv_pair_block.emplace_back(std::move(key_copy), std::move(value_copy));
    }
    kv_pair_blocks->push_back(std::move(kv_pair_block));
  }
  return Status::OK();
}

Status BlockBasedTable::DumpTable(WritableFile* out_file,
                                  bool show_sequence_number_type) {
  WritableFileStringStreamAdapter out_file_wrapper(out_file);
  std::ostream out_stream(&out_file_wrapper);
  // Output Footer
  out_stream << "Footer Details:\n"
                "--------------------------------------\n";
  out_stream << "  " << rep_->footer.ToString() << "\n";

  // Output Checksum Type Legend
  out_stream << "Block Checksum Type Legend:\n"
                "--------------------------------------\n";
  out_stream << "  0 = kNoChecksum\n";
  out_stream << "  1 = kCRC32c\n";
  out_stream << "  2 = kxxHash\n";
  out_stream << "  3 = kxxHash64\n";
  out_stream << "  4 = kXXH3\n";
  out_stream << "  (This file uses checksum type: "
             << static_cast<int>(rep_->footer.checksum_type()) << ")\n\n";

  // Output MetaIndex
  out_stream << "Metaindex Details:\n"
                "--------------------------------------\n";
  std::unique_ptr<Block> metaindex;
  std::unique_ptr<InternalIterator> metaindex_iter;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions ro;
  Status s = ReadMetaIndexBlock(ro, nullptr /* prefetch_buffer */, &metaindex,
                                &metaindex_iter);
  if (s.ok()) {
    // Print metaindex block checksum
    DumpBlockChecksumInfo(rep_->footer.metaindex_handle(), ro,
                          "Metaindex block", out_stream);

    for (metaindex_iter->SeekToFirst(); metaindex_iter->Valid();
         metaindex_iter->Next()) {
      s = metaindex_iter->status();
      if (!s.ok()) {
        return s;
      }
      // Parse block handle from metaindex value
      BlockHandle block_handle;
      Slice input = metaindex_iter->value();
      Status handle_status = block_handle.DecodeFrom(&input);

      if (!handle_status.ok()) {
        out_stream << "  Skip the block with type "
                   << metaindex_iter->key().ToString()
                   << " due to error: " << handle_status.ToString() << "\n\n";
        continue;
      }

      if (metaindex_iter->key() == kPropertiesBlockName) {
        out_stream << "  Properties block handle: "
                   << metaindex_iter->value().ToString(true) << "\n";
        DumpBlockChecksumInfo(block_handle, ro, "Properties block", out_stream);
      } else if (metaindex_iter->key() == kCompressionDictBlockName) {
        out_stream << "  Compression dictionary block handle: "
                   << metaindex_iter->value().ToString(true) << "\n";
        DumpBlockChecksumInfo(block_handle, ro, "Compression dictionary block",
                              out_stream);
      } else if (strstr(metaindex_iter->key().ToString().c_str(),
                        "filter.rocksdb.") != nullptr) {
        out_stream << "  Filter block handle: "
                   << metaindex_iter->value().ToString(true) << "\n";
        DumpBlockChecksumInfo(block_handle, ro, "Filter block", out_stream);
      } else if (metaindex_iter->key() == kRangeDelBlockName) {
        out_stream << "  Range deletion block handle: "
                   << metaindex_iter->value().ToString(true) << "\n";
        DumpBlockChecksumInfo(block_handle, ro, "Range deletion block",
                              out_stream);
      }
    }
    out_stream << "\n";
  } else {
    return s;
  }

  // Output TableProperties
  const ROCKSDB_NAMESPACE::TableProperties* table_properties;
  table_properties = rep_->table_properties.get();

  if (table_properties != nullptr) {
    out_stream << "Table Properties:\n"
                  "--------------------------------------\n";
    out_stream << "  " << table_properties->ToString("\n  ", ": ") << "\n";
  }

  if (rep_->filter) {
    out_stream << "Filter Details:\n"
                  "--------------------------------------\n";
    out_stream << "  " << rep_->filter->ToString() << "\n";
  }

  // Output Index block
  s = DumpIndexBlock(out_stream);
  if (!s.ok()) {
    return s;
  }

  // Output compression dictionary
  if (rep_->uncompression_dict_reader) {
    CachableEntry<DecompressorDict> uncompression_dict;
    s = rep_->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        nullptr /* prefetch_buffer */, ro, nullptr /* get_context */,
        nullptr /* lookup_context */, &uncompression_dict);
    if (!s.ok()) {
      return s;
    }

    assert(uncompression_dict.GetValue());

    const Slice& raw_dict = uncompression_dict.GetValue()->GetRawDict();
    out_stream << "Compression Dictionary:\n"
                  "--------------------------------------\n";
    out_stream << "  size (bytes): " << raw_dict.size() << "\n\n";
    out_stream << "  HEX    " << raw_dict.ToString(true) << "\n\n";
  }

  // Output range deletions block
  auto* range_del_iter = NewRangeTombstoneIterator(ro);
  if (range_del_iter != nullptr) {
    range_del_iter->SeekToFirst();
    if (range_del_iter->Valid()) {
      out_stream << "Range deletions:\n"
                    "--------------------------------------\n";
      for (; range_del_iter->Valid(); range_del_iter->Next()) {
        DumpKeyValue(range_del_iter->key(), range_del_iter->value(), out_stream,
                     show_sequence_number_type);
      }
      out_stream << "\n";
    }
    delete range_del_iter;
  }
  // Output Data blocks
  s = DumpDataBlocks(out_stream, show_sequence_number_type);

  if (!s.ok()) {
    return s;
  }

  if (!out_stream.good()) {
    return Status::IOError("Failed to write to output file");
  }
  return Status::OK();
}

void BlockBasedTable::DumpBlockChecksumInfo(const BlockHandle& block_handle,
                                            const ReadOptions& read_options,
                                            const char* block_name,
                                            std::ostream& out_stream) const {
  if (rep_->footer.GetBlockTrailerSize() == 0) {
    return;
  }

  size_t block_size = static_cast<size_t>(block_handle.size());
  size_t block_size_with_trailer = block_size + kBlockTrailerSize;
  std::unique_ptr<char[]> raw_block(new char[block_size_with_trailer]);
  Slice raw_block_slice;
  IOOptions opts;
  IODebugContext dbg;
  IOStatus io_s = rep_->file->PrepareIOOptions(read_options, opts, &dbg);
  if (io_s.ok()) {
    io_s = rep_->file->Read(block_handle.offset(), block_size_with_trailer,
                            opts, &raw_block_slice, raw_block.get(), &dbg);
  }
  if (io_s.ok() && raw_block_slice.size() == block_size_with_trailer) {
    const char* data = raw_block_slice.data();
    uint8_t compression_type_byte = static_cast<uint8_t>(data[block_size]);
    uint32_t stored_checksum = DecodeFixed32(data + block_size + 1);
    uint32_t modifier = ChecksumModifierForContext(
        rep_->footer.base_context_checksum(), block_handle.offset());
    uint32_t actual_checksum = stored_checksum - modifier;
    out_stream << "  " << block_name << " checksum type: "
               << static_cast<int>(rep_->footer.checksum_type())
               << "  checksum value: 0x" << std::hex << actual_checksum
               << std::dec << "  offset: " << block_handle.offset()
               << "  size: " << block_size << "  compression type: "
               << static_cast<int>(compression_type_byte) << "\n";
  } else {
    out_stream << "  ERROR: Failed to read " << block_name << " checksum info";
    if (!io_s.ok()) {
      out_stream << " - " << io_s.ToString();
    } else if (raw_block_slice.size() != block_size_with_trailer) {
      out_stream << " - read " << raw_block_slice.size() << " bytes, expected "
                 << block_size_with_trailer;
    }
    out_stream << "\n";
  }
}

Status BlockBasedTable::DumpIndexBlock(std::ostream& out_stream) {
  out_stream << "Index Details:\n"
                "--------------------------------------\n";
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;

  // Print index block checksum information
  DumpBlockChecksumInfo(rep_->index_handle, read_options, "Index block",
                        out_stream);

  std::unique_ptr<InternalIteratorBase<IndexValue>> blockhandles_iter(
      NewIndexIterator(read_options, /*disable_prefix_seek=*/false,
                       /*input_iter=*/nullptr, /*get_context=*/nullptr,
                       /*lookup_context=*/nullptr));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_stream << "Can not read Index Block \n\n";
    return s;
  }

  out_stream << "  Block key hex dump: Data block handle\n";
  out_stream << "  Block key ascii\n\n";
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }
    Slice key = blockhandles_iter->key();
    Slice user_key;
    InternalKey ikey;
    if (!rep_->index_key_includes_seq) {
      user_key = key;
    } else {
      ikey.DecodeFrom(key);
      user_key = ikey.user_key();
    }

    out_stream << "  HEX    " << user_key.ToString(true) << ": "
               << blockhandles_iter->value().ToString(true,
                                                      rep_->index_has_first_key)
               << " offset " << blockhandles_iter->value().handle.offset()
               << " size " << blockhandles_iter->value().handle.size() << "\n";

    std::string str_key = user_key.ToString();
    std::string res_key;
    char cspace = ' ';
    for (size_t i = 0; i < str_key.size(); i++) {
      res_key.append(&str_key[i], 1);
      res_key.append(1, cspace);
    }
    out_stream << "  ASCII  " << res_key << "\n";
    out_stream << "  ------\n";
  }
  out_stream << "\n";
  return Status::OK();
}

Status BlockBasedTable::DumpDataBlocks(std::ostream& out_stream,
                                       bool show_sequence_number_type) {
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  std::unique_ptr<InternalIteratorBase<IndexValue>> blockhandles_iter(
      NewIndexIterator(read_options, /*disable_prefix_seek=*/false,
                       /*input_iter=*/nullptr, /*get_context=*/nullptr,
                       /*lookup_context=*/nullptr));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_stream << "Can not read Index Block \n\n";
    return s;
  }

  uint64_t datablock_size_min = std::numeric_limits<uint64_t>::max();
  uint64_t datablock_size_max = 0;
  uint64_t datablock_size_sum = 0;

  size_t block_id = 1;
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       block_id++, blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }

    BlockHandle bh = blockhandles_iter->value().handle;
    uint64_t datablock_size = bh.size();
    datablock_size_min = std::min(datablock_size_min, datablock_size);
    datablock_size_max = std::max(datablock_size_max, datablock_size);
    datablock_size_sum += datablock_size;

    out_stream << "Data Block # " << block_id << " @ "
               << blockhandles_iter->value().handle.ToString(true) << "\n";

    // Read block checksum information
    DumpBlockChecksumInfo(bh, read_options, "Data block", out_stream);

    out_stream << "--------------------------------------\n";

    std::unique_ptr<InternalIterator> datablock_iter;
    Status tmp_status;
    datablock_iter.reset(NewDataBlockIterator<DataBlockIter>(
        read_options, blockhandles_iter->value().handle,
        /*input_iter=*/nullptr, /*block_type=*/BlockType::kData,
        /*get_context=*/nullptr, /*lookup_context=*/nullptr,
        /*prefetch_buffer=*/nullptr, /*for_compaction=*/false,
        /*async_read=*/false, tmp_status, /*use_block_cache_for_lookup=*/true));
    s = datablock_iter->status();

    if (!s.ok()) {
      out_stream << "Error reading the block - Skipped \n\n";
      continue;
    }

    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        out_stream << "Error reading the block - Skipped \n";
        break;
      }
      DumpKeyValue(datablock_iter->key(), datablock_iter->value(), out_stream,
                   show_sequence_number_type);
    }
    out_stream << "\n";
  }

  uint64_t num_datablocks = block_id - 1;
  if (num_datablocks) {
    double datablock_size_avg =
        static_cast<double>(datablock_size_sum) / num_datablocks;
    out_stream << "Data Block Summary:\n";
    out_stream << "--------------------------------------\n";
    out_stream << "  # data blocks: " << num_datablocks << "\n";
    out_stream << "  min data block size: " << datablock_size_min << "\n";
    out_stream << "  max data block size: " << datablock_size_max << "\n";
    out_stream << "  avg data block size: "
               << std::to_string(datablock_size_avg) << "\n";
  }

  return Status::OK();
}

void BlockBasedTable::DumpKeyValue(const Slice& key, const Slice& value,
                                   std::ostream& out_stream,
                                   bool show_sequence_number_type) {
  ParsedInternalKey result;
  auto s = ParseInternalKey(key, &result, true);
  if (!s.ok()) {
    out_stream << "Error parsing internal key - Skipped \n";
    return;
  }

  if (show_sequence_number_type) {
    out_stream << "  HEX    " << result.user_key.ToString(true)
               << "  seq: " << result.sequence
               << "  type: " << std::to_string(result.type) << " : "
               << value.ToString(true) << "\n";
  } else {
    out_stream << "  HEX    " << result.user_key.ToString(true) << ": "
               << value.ToString(true) << "\n";
  }

  std::string str_key = result.user_key.ToString();
  std::string str_value = value.ToString();
  std::string res_key;
  std::string res_value;
  char cspace = ' ';
  for (size_t i = 0; i < str_key.size(); i++) {
    if (str_key[i] == '\0') {
      res_key.append("\\0", 2);
    } else {
      res_key.append(&str_key[i], 1);
    }
    res_key.append(1, cspace);
  }
  for (size_t i = 0; i < str_value.size(); i++) {
    if (str_value[i] == '\0') {
      res_value.append("\\0", 2);
    } else {
      res_value.append(&str_value[i], 1);
    }
    res_value.append(1, cspace);
  }

  out_stream << "  ASCII  " << res_key << ": " << res_value << "\n";
  out_stream << "  ------\n";
}

void BlockBasedTable::MarkObsolete(uint32_t uncache_aggressiveness) {
  rep_->uncache_aggressiveness.StoreRelaxed(uncache_aggressiveness);
}

// After the InitBlockIterator specializations, which it uses.
Status BlockBasedTable::PinDataBlocks(const ReadOptions& read_options,
                                      PinnedDataBlocks* pinned) const {
  assert(pinned != nullptr);
  // The caller's options say how to do I/O (checksums, rate limiter, I/O
  // activity). The options below must not depend on the caller, because the
  // blocks stay pinned after this call returns:
  // - fill_cache and kReadAllTier: every block is read and charged to the
  //   block cache, even when the caller's read does not fill the cache or
  //   reads only cached blocks.
  // - No deadline or I/O timeout: the reader stays in the blob file cache, so
  //   a pin that fails on one caller's time limit would leave the reader
  //   without kept blocks for as long as it is open.
  // - No read-scoped buffer provider: such a buffer is freed when the
  //   caller's read ends, but a pinned block must live as long as the table.
  ReadOptions pin_options = read_options;
  pin_options.fill_cache = true;
  pin_options.read_tier = kReadAllTier;
  pin_options.deadline = std::chrono::microseconds::zero();
  pin_options.io_timeout = std::chrono::microseconds::zero();
  pin_options.read_scoped_block_buffer_provider = nullptr;
  // The same caller as in MultiGetRawEntries.
  BlockCacheLookupContext lookup_context{
      TableReaderCaller::kBlobRelocationFileRead};
  IndexBlockIter iiter_on_stack;
  InternalIteratorBase<IndexValue>* const iiter = NewIndexIterator(
      pin_options, /*need_upper_bound_check=*/false, &iiter_on_stack,
      /*get_context=*/nullptr, &lookup_context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_owner;
  if (iiter != &iiter_on_stack) {
    iiter_owner.reset(iiter);
  }
  // Read the blocks in batches of up to MultiGetContext::MAX_BATCH_SIZE (32)
  // blocks (see RetrieveDataBlocks()), so a cold open does not do one read per
  // block.
  const bool has_block_cache = rep_->table_options.block_cache != nullptr;
  autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE> handles;
  std::array<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
  const auto retrieve_batch = [&]() {
    const size_t first = pinned->blocks.size();
    pinned->blocks.resize(first + handles.size());
    RetrieveDataBlocks(pin_options, handles, &lookup_context,
                       pinned->blocks.data() + first, statuses.data());
    // The loop below stops at the first failed block, so the statuses after it
    // can stay unread.
    for (const Status& status : statuses) {
      status.PermitUncheckedError();
    }
    for (size_t b = 0; b < handles.size(); ++b) {
      if (!statuses[b].ok()) {
        return statuses[b];
      }
      const CachableEntry<Block_kData>& block = pinned->blocks[first + b];
      if (block.GetValue() == nullptr) {
        return Status::Corruption("Data block to pin has no contents");
      }
      // A block that the block cache did not take is not charged. A block
      // that does not own its bytes points into the memory-mapped file
      // (allow_mmap_reads and no compression). The block cache never takes
      // such a block. Its bytes are in the mapped file, not on the heap, and
      // the mapping lives as long as the table, so the block is kept.
      if (has_block_cache && !block.IsCached() &&
          block.GetValue()->own_bytes()) {
        return Status::Incomplete(
            "Block cache cannot hold a pinned data block");
      }
      pinned->memory_bytes += block.GetValue()->ApproximateMemoryUsage();
    }
    handles.clear();
    return Status::OK();
  };
  for (iiter->SeekToFirst(); iiter->Valid(); iiter->Next()) {
    const Slice index_key = iiter->key();
    pinned->last_user_keys.emplace_back(
        rep_->index_key_includes_seq ? ExtractUserKey(index_key).ToString()
                                     : index_key.ToString());
    handles.push_back(iiter->value().handle);
    if (handles.size() == MultiGetContext::MAX_BATCH_SIZE) {
      const Status status = retrieve_batch();
      if (!status.ok()) {
        return status;
      }
    }
  }
  if (!iiter->status().ok()) {
    return iiter->status();
  }
  return handles.empty() ? Status::OK() : retrieve_batch();
}

void BlockBasedTable::MultiGetRawEntriesPinnedImpl(
    const PinnedDataBlocks& pinned, size_t num_keys, const Slice* keys,
    RawEntryCallback on_entry, const void* on_entry_arg) const {
  MarkReadStarted();
  assert(pinned.last_user_keys.size() == pinned.blocks.size());
  const Comparator* const user_comparator =
      rep_->internal_comparator.user_comparator();
  constexpr size_t kNoBlock = std::numeric_limits<size_t>::max();
  size_t iter_block = kNoBlock;
  DataBlockIter biter;
  for (size_t i = 0; i < num_keys; ++i) {
    assert(i == 0 ||
           rep_->internal_comparator.Compare(keys[i - 1], keys[i]) <= 0);
    const Slice user_key = ExtractUserKey(keys[i]);
    // The first block whose last key is at or after the key holds the first
    // entry at or after it.
    const std::vector<std::string>::const_iterator found = std::lower_bound(
        pinned.last_user_keys.begin(), pinned.last_user_keys.end(), user_key,
        [user_comparator](const std::string& last_key, const Slice& key) {
          return user_comparator->Compare(last_key, key) < 0;
        });
    if (found == pinned.last_user_keys.end()) {
      on_entry(on_entry_arg, i, Status::NotFound(), Slice(), Slice());
      continue;
    }
    const size_t b = static_cast<size_t>(found - pinned.last_user_keys.begin());
    if (b != iter_block) {
      // The block stays pinned by `pinned`, so the iterator must not take its
      // cache handle (NewDataBlockIterator() would move it).
      biter.Invalidate(Status::OK());
      InitBlockIterator<DataBlockIter>(rep_, pinned.blocks[b].GetValue(),
                                       BlockType::kData, &biter,
                                       /*block_contents_pinned=*/true);
      iter_block = b;
    }
    if (!biter.SeekForGet(keys[i]) || !biter.Valid()) {
      on_entry(on_entry_arg, i,
               biter.status().ok() ? Status::NotFound() : biter.status(),
               Slice(), Slice());
      continue;
    }
    on_entry(on_entry_arg, i, Status::OK(), biter.key(), biter.value());
  }
}

}  // namespace ROCKSDB_NAMESPACE
