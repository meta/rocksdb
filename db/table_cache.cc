//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include <algorithm>
#include <utility>

#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "monitoring/file_read_sample.h"
#include "monitoring/perf_context_imp.h"
#include "options/options_helper.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/statistics.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/multiget_context.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/stop_watch.h"

// Generate the regular and coroutine versions of some methods by
// including table_cache_sync_and_async.h twice
// Macros in the header will expand differently based on whether
// WITH_COROUTINES or WITHOUT_COROUTINES is defined
// clang-format off
#define WITHOUT_COROUTINES
#include "db/table_cache_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/table_cache_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {

namespace {

static Slice GetSliceForFileNumber(const uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

void AppendVarint64(IterKey* key, uint64_t v) {
  char buf[10];
  auto ptr = EncodeVarint64(buf, v);
  key->TrimAppend(key->Size(), buf, ptr - buf);
}

}  // anonymous namespace

const int kLoadConcurency = 128;

TableCache::TableCache(const ImmutableOptions& ioptions,
                       const FileOptions* file_options, Cache* const cache,
                       BlockCacheTracer* const block_cache_tracer,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       const std::string& db_session_id, bool fast_sst_open,
                       std::string column_family_name)
    : ioptions_(ioptions),
      file_options_(*file_options),
      cache_(cache),
      immortal_tables_(false),
      should_pin_table_handles_(cache_.get()->GetCapacity() >=
                                kInfiniteCapacity),
      fast_sst_open_(fast_sst_open),
      block_cache_tracer_(block_cache_tracer),
      loader_mutex_(kLoadConcurency),
      io_tracer_(io_tracer),
      db_session_id_(db_session_id),
      column_family_name_(std::move(column_family_name)) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId());
  }
}

TableCache::~TableCache() = default;

Status TableCache::GetTableReader(
    const ReadOptions& ro, const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, bool sequential_mode,
    HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
    const MutableCFOptions& mutable_cf_options, bool skip_filters, int level,
    bool prefetch_index_and_filter_in_cache,
    size_t max_file_size_for_l0_meta_pin, Temperature file_temperature,
    std::string* file_open_metadata, bool avoid_shared_metadata_cache) {
  std::string fname = TableFileName(
      ioptions_.cf_paths, file_meta.fd.GetNumber(), file_meta.fd.GetPathId());
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopts = file_options;
  fopts.temperature = file_temperature;
  fopts.file_checksum = file_meta.file_checksum;
  fopts.file_checksum_func_name = file_meta.file_checksum_func_name;
  // Pass file open metadata for fast SST open. Use a local copy since
  // fopts.file_metadata is a non-owning pointer and file_meta is const.
  // Only pass metadata when fast_sst_open is enabled; otherwise ignore
  // previously persisted metadata (e.g. stale filesystem credentials).
  std::string file_open_metadata_copy;
  if (fast_sst_open_.load(std::memory_order_relaxed) &&
      !file_meta.file_open_metadata.empty()) {
    file_open_metadata_copy = file_meta.file_open_metadata;
    fopts.file_metadata = &file_open_metadata_copy;
    RecordTick(ioptions_.stats, FILE_OPEN_METADATA_PASSED);
  }
  Status s = PrepareIOFromReadOptions(ro, ioptions_.clock, fopts.io_options);
  TEST_SYNC_POINT_CALLBACK("TableCache::GetTableReader:BeforeOpenFile",
                           const_cast<Status*>(&s));
  if (s.ok()) {
    s = ioptions_.fs->NewRandomAccessFile(fname, fopts, &file, nullptr);
  }
  if (s.ok()) {
    RecordTick(ioptions_.stats, NO_FILE_OPENS);
  } else if (s.IsPathNotFound()) {
    fname = Rocks2LevelTableFileName(fname);
    // If this file is also not found, we want to use the error message
    // that contains the table file name which is less confusing.
    Status temp_s =
        PrepareIOFromReadOptions(ro, ioptions_.clock, fopts.io_options);
    if (temp_s.ok()) {
      temp_s = ioptions_.fs->NewRandomAccessFile(fname, fopts, &file, nullptr);
    }
    if (temp_s.ok()) {
      RecordTick(ioptions_.stats, NO_FILE_OPENS);
      s = temp_s;
    }
  }

  if (s.ok()) {
    // Retrieve file open metadata before wrapping the file
    if (file_open_metadata != nullptr) {
      IOStatus io_s = file->GetFileOpenMetadata(file_open_metadata);
      if (io_s.ok() && !file_open_metadata->empty() &&
          file_open_metadata->size() <=
              FSRandomAccessFile::kMaxFileOpenMetadataSize) {
        RecordTick(ioptions_.stats, FILE_OPEN_METADATA_RETRIEVED);
      } else {
        if (io_s.ok() && file_open_metadata->size() >
                             FSRandomAccessFile::kMaxFileOpenMetadataSize) {
          ROCKS_LOG_WARN(ioptions_.logger,
                         "File open metadata for %s too large (%zu bytes), "
                         "ignoring",
                         fname.c_str(), file_open_metadata->size());
        }
        file_open_metadata->clear();
      }
    }
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(Env::AccessPattern::kRandom);
    }
    if (ioptions_.default_temperature != Temperature::kUnknown &&
        file_temperature == Temperature::kUnknown) {
      file_temperature = ioptions_.default_temperature;
    }
    StopWatch sw(ioptions_.clock, ioptions_.stats, TABLE_OPEN_IO_MICROS);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(
            std::move(file), fname, ioptions_.clock, io_tracer_,
            ioptions_.stats, SST_READ_MICROS, file_read_hist,
            ioptions_.rate_limiter.get(), ioptions_.listeners, file_temperature,
            level == ioptions_.num_levels - 1, ioptions_.fs.get()));
    UniqueId64x2 expected_unique_id;
    if (ioptions_.verify_sst_unique_id_in_manifest) {
      expected_unique_id = file_meta.unique_id;
    } else {
      expected_unique_id = kNullUniqueId64x2;  // null ID == no verification
    }
    TableReaderOptions table_reader_options(
        ioptions_, mutable_cf_options.prefix_extractor,
        mutable_cf_options.compression_manager.get(), file_options,
        internal_comparator, mutable_cf_options.block_protection_bytes_per_key,
        skip_filters, immortal_tables_, false /* force_direct_prefetch */,
        level, block_cache_tracer_, max_file_size_for_l0_meta_pin,
        db_session_id_, file_meta.fd.GetNumber(), expected_unique_id,
        file_meta.fd.largest_seqno, file_meta.tail_size,
        file_meta.user_defined_timestamps_persisted,
        avoid_shared_metadata_cache, column_family_name_);
    // Route same-file ("embedded") blob reads through the CFD's BlobSource for
    // caching + stats. nullptr in non-DB contexts (e.g. repair).
    table_reader_options.blob_source = blob_source_;
    s = mutable_cf_options.table_factory->NewTableReader(
        ro, table_reader_options, std::move(file_reader),
        file_meta.fd.GetFileSize(), table_reader,
        prefetch_index_and_filter_in_cache);
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
  }
  return s;
}

Cache::Handle* TableCache::Lookup(Cache* cache, uint64_t file_number) {
  // NOTE: sharing same Cache with BlobFileCache
  Slice key = GetSliceForFileNumber(&file_number);
  return cache->Lookup(key);
}

// TODO: consider making handle RAII.
Status TableCache::FindTable(
    const ReadOptions& ro, const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, TypedHandle** handle,
    const MutableCFOptions& mutable_cf_options, TableReader** out_table_reader,
    const bool no_io, HistogramImpl* file_read_hist, bool skip_filters,
    int level, bool prefetch_index_and_filter_in_cache,
    size_t max_file_size_for_l0_meta_pin, Temperature file_temperature,
    bool pin_table_handle, std::string* file_open_metadata,
    std::unique_ptr<TableReader>* fresh_table_reader_owner,
    const TableCacheOpenOptions& open_options) {
  assert(out_table_reader != nullptr && *out_table_reader == nullptr);
  assert(handle != nullptr && *handle == nullptr);
  // open_ephemeral_table_reader requests a fresh reader;
  // fresh_table_reader_owner is where we return it. The two must agree.
  assert(open_options.open_ephemeral_table_reader ==
         (fresh_table_reader_owner != nullptr));
  PERF_TIMER_GUARD_WITH_CLOCK(find_table_nanos, ioptions_.clock);

  // Bypass path: open a fresh TableReader, skipping the pinned-reader fast path
  // and the shared cache. The caller takes ownership via
  // fresh_table_reader_owner. no_io is not allowed here since opening a new
  // reader always needs I/O.
  if (fresh_table_reader_owner != nullptr) {
    assert(!no_io);
    if (no_io) {
      // Defensive in release builds: the caller violated the contract.
      return Status::Incomplete(
          "fresh TableReader requested but no_io is set; cannot open file");
    }
    std::unique_ptr<TableReader> table_reader;
    const bool effective_skip_filters =
        skip_filters || open_options.skip_filters;
    TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:FreshTableReader",
                             const_cast<TableCacheOpenOptions*>(&open_options));
    Status s = GetTableReader(
        ro, file_options, internal_comparator, file_meta,
        false /* sequential mode */, file_read_hist, &table_reader,
        mutable_cf_options, effective_skip_filters, level,
        prefetch_index_and_filter_in_cache &&
            !open_options.avoid_shared_metadata_cache,
        max_file_size_for_l0_meta_pin, file_temperature, file_open_metadata,
        open_options.avoid_shared_metadata_cache);
    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(ioptions_.stats, NO_FILE_ERRORS);
      IGNORE_STATUS_IF_ERROR(s);
      return s;
    }
    *out_table_reader = table_reader.get();
    *fresh_table_reader_owner = std::move(table_reader);
    *handle = nullptr;
    return s;
  }

  // Fast path: if table reader is already pinned, return it directly without a
  // cache lookup.
  auto pinned_reader = file_meta.fd.pinned_reader.Get();
  if (pinned_reader != nullptr) {
    *handle = nullptr;
    *out_table_reader = pinned_reader;
    return Status::OK();
  }

  uint64_t number = file_meta.fd.GetNumber();
  // NOTE: sharing same Cache with BlobFileCache
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_.Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));

  Status s = Status::OK();
  if (*handle == nullptr) {
    if (no_io) {
      s = Status::Incomplete("Table not found in table_cache, no_io is set");
      return s;
    }
    MutexLock load_lock(&loader_mutex_.Get(key));

    // Check if another thread has already pinned the table reader
    pinned_reader = file_meta.fd.pinned_reader.Get();
    if (pinned_reader != nullptr) {
      *handle = nullptr;
      *out_table_reader = pinned_reader;
      return s;
    }

    // We check the cache again under loading mutex
    *handle = cache_.Lookup(key);
    if (*handle == nullptr) {
      std::unique_ptr<TableReader> table_reader;
      s = GetTableReader(ro, file_options, internal_comparator, file_meta,
                         false /* sequential mode */, file_read_hist,
                         &table_reader, mutable_cf_options, skip_filters, level,
                         prefetch_index_and_filter_in_cache,
                         max_file_size_for_l0_meta_pin, file_temperature,
                         file_open_metadata);
      if (!s.ok()) {
        assert(table_reader == nullptr);
        RecordTick(ioptions_.stats, NO_FILE_ERRORS);
        // We do not cache error results so that if the error is transient,
        // or somebody repairs the file, we recover automatically.
        IGNORE_STATUS_IF_ERROR(s);
      } else {
        s = cache_.Insert(key, table_reader.get(), 1, handle);
        if (s.ok()) {
          // Release ownership of table reader.
          [[maybe_unused]] TableReader* released_reader =
              table_reader.release();
        }
      }
    }

    if (s.ok()) {
      *out_table_reader = cache_.Value(*handle);
      if (pin_table_handle) {
        file_meta.fd.pinned_reader.Pin(*handle, *out_table_reader);
        *handle = nullptr;
      }
    }
  } else {
    *out_table_reader = cache_.Value(*handle);
    if (pin_table_handle) {
      // handle is in cache but not pinned. This should happen fairly rarely,
      // and once the reader is pinned, we will no longer need to go through
      // these mutexes again.
      MutexLock load_lock(&loader_mutex_.Get(key));
      if (file_meta.fd.pinned_reader.Get() != nullptr) {
        // Another thread has pinned the handle; release our lookup ref.
        cache_.Release(*handle);
      } else {
        file_meta.fd.pinned_reader.Pin(*handle, *out_table_reader);
      }
      *handle = nullptr;
    }
  }

  return s;
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const FileOptions& file_options,
    const InternalKeyComparator& icomparator, const FileMetaData& file_meta,
    RangeDelAggregator* range_del_agg,
    const MutableCFOptions& mutable_cf_options, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, TableReaderCaller caller, Arena* arena,
    bool skip_filters, int level, size_t max_file_size_for_l0_meta_pin,
    const InternalKey* smallest_compaction_key,
    const InternalKey* largest_compaction_key, bool allow_unprepared_value,
    const SequenceNumber* read_seqno,
    std::unique_ptr<TruncatedRangeDelIterator>* range_del_iter,
    bool maybe_pin_table_handle, std::string* file_open_metadata,
    const TableCacheOpenOptions& open_options) {
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  Status s;
  TableReader* table_reader = nullptr;
  TypedHandle* handle = nullptr;
  assert(!open_options.open_ephemeral_table_reader ||
         table_reader_ptr == nullptr);
  // Holds ownership of a freshly-opened TableReader when the caller asked us
  // to bypass the shared cache. When non-empty, the iterator we hand back must
  // arrange to free it on destruction.
  std::unique_ptr<TableReader> ephemeral_reader;
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }
  const bool effective_skip_filters = skip_filters || open_options.skip_filters;
  bool for_compaction = caller == TableReaderCaller::kCompaction;
  TEST_SYNC_POINT_CALLBACK("TableCache::NewIterator::BeforeFindTable",
                           const_cast<FileDescriptor*>(&file_meta.fd));
  s = FindTable(
      options, file_options, icomparator, file_meta, &handle,
      mutable_cf_options, &table_reader,
      options.read_tier == kBlockCacheTier /* no_io */, file_read_hist,
      effective_skip_filters, level,
      /*prefetch_index_and_filter_in_cache=*/
      !open_options.avoid_shared_metadata_cache, max_file_size_for_l0_meta_pin,
      file_meta.temperature,
      maybe_pin_table_handle && should_pin_table_handles_, file_open_metadata,
      open_options.open_ephemeral_table_reader ? &ephemeral_reader : nullptr,
      open_options);
  InternalIterator* result = nullptr;
  if (s.ok()) {
    if (HasTableFilter(options) &&
        !(*options.table_filter)(*table_reader->GetTableProperties())) {
      result = NewEmptyInternalIterator<Slice>(arena);
    } else {
      result = table_reader->NewIterator(
          options, mutable_cf_options.prefix_extractor.get(), arena,
          effective_skip_filters, caller,
          file_options.compaction_readahead_size, allow_unprepared_value);
    }
    if (handle != nullptr) {
      cache_.RegisterReleaseAsCleanup(handle, *result);
      handle = nullptr;  // prevent from releasing below
    }
    // Don't hand ephemeral_reader to result's cleanup yet: range-del
    // processing below can set s to non-OK, in which case result is replaced
    // by an error iterator at function end. Transfer ownership only once we
    // know s stays OK; otherwise ephemeral_reader's destructor frees it.

    if (for_compaction) {
      table_reader->SetupForCompaction();
    }
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = table_reader;
    }
  }
  if (s.ok() && !options.ignore_range_deletions) {
    if (range_del_iter != nullptr) {
      auto new_range_del_iter =
          read_seqno ? table_reader->NewRangeTombstoneIterator(
                           *read_seqno, options.timestamp)
                     : table_reader->NewRangeTombstoneIterator(options);
      if (new_range_del_iter == nullptr || new_range_del_iter->empty()) {
        delete new_range_del_iter;
        *range_del_iter = nullptr;
      } else {
        *range_del_iter = std::make_unique<TruncatedRangeDelIterator>(
            std::unique_ptr<FragmentedRangeTombstoneIterator>(
                new_range_del_iter),
            &icomparator, &file_meta.smallest, &file_meta.largest);
      }
    }
    if (range_del_agg != nullptr) {
      if (range_del_agg->AddFile(file_meta.fd.GetNumber())) {
        std::unique_ptr<FragmentedRangeTombstoneIterator> new_range_del_iter(
            static_cast<FragmentedRangeTombstoneIterator*>(
                table_reader->NewRangeTombstoneIterator(options)));
        if (new_range_del_iter != nullptr) {
          s = new_range_del_iter->status();
        }
        if (s.ok()) {
          const InternalKey* smallest = &file_meta.smallest;
          const InternalKey* largest = &file_meta.largest;
          if (smallest_compaction_key != nullptr) {
            smallest = smallest_compaction_key;
          }
          if (largest_compaction_key != nullptr) {
            largest = largest_compaction_key;
          }
          range_del_agg->AddTombstones(std::move(new_range_del_iter), smallest,
                                       largest);
        }
      }
    }
  }

  if (handle != nullptr) {
    cache_.Release(handle);
  }
  // Range-del processing is done and s is final. Hand the ephemeral reader's
  // lifetime to the returned iterator; if s is non-OK, leave it for
  // ephemeral_reader's destructor. RegisterCleanup gets the raw pointer before
  // release(), so if its allocation throws the reader is still owned by
  // ephemeral_reader and freed on unwind.
  if (s.ok() && ephemeral_reader && result != nullptr) {
    TableReader* raw = ephemeral_reader.get();
    result->RegisterCleanup(
        [](void* arg1, void* /*arg2*/) {
          delete static_cast<TableReader*>(arg1);
        },
        raw, nullptr);
    // release() returns raw, which the cleanup now owns; drop the unique_ptr's
    // ownership so the reader isn't freed twice.
    [[maybe_unused]] TableReader* released = ephemeral_reader.release();
    assert(released == raw);
  }
  if (!s.ok()) {
    // Today result is always null here: it is only set when s was OK, and the
    // only later change to s, new_range_del_iter->status(), is always OK for a
    // FragmentedRangeTombstoneIterator. The assert documents that; the cleanup
    // still disposes of any stray result before the error iterator replaces it.
    assert(result == nullptr);
    if (result != nullptr) {
      if (arena != nullptr) {
        result->~InternalIterator();
      } else {
        delete result;
      }
      result = nullptr;
    }
    result = NewErrorInternalIterator<Slice>(s, arena);
  }
  return result;
}

Status TableCache::GetRangeTombstoneIterator(
    const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, const MutableCFOptions& mutable_cf_options,
    std::unique_ptr<FragmentedRangeTombstoneIterator>* out_iter) {
  assert(out_iter);
  Status s;
  TableReader* t = nullptr;
  TypedHandle* handle = nullptr;
  s = FindTable(options, file_options_, internal_comparator, file_meta, &handle,
                mutable_cf_options, &t);
  if (s.ok()) {
    // Note: NewRangeTombstoneIterator could return nullptr
    out_iter->reset(t->NewRangeTombstoneIterator(options));
  }
  if (handle) {
    if (*out_iter) {
      cache_.RegisterReleaseAsCleanup(handle, **out_iter);
    } else {
      cache_.Release(handle);
    }
  }
  return s;
}

uint64_t TableCache::CreateRowCacheKeyPrefix(const ReadOptions& options,
                                             const FileDescriptor& fd,
                                             const Slice& internal_key,
                                             GetContext* get_context,
                                             IterKey& row_cache_key) {
  uint64_t fd_number = fd.GetNumber();
  // We use the user key as cache key instead of the internal key,
  // otherwise the whole cache would be invalidated every time the
  // sequence key increases. However, to support caching snapshot
  // reads, we append a sequence number (incremented by 1 to
  // distinguish from 0) other than internal_key seq no
  // to determine row cache entry visibility.
  // If the snapshot is larger than the largest seqno in the file,
  // all data should be exposed to the snapshot, so we treat it
  // the same as there is no snapshot. The exception is that if
  // a seq-checking callback is registered, some internal keys
  // may still be filtered out.
  uint64_t cache_entry_seq_no = 0;

  // Maybe we can include the whole file ifsnapshot == fd.largest_seqno.
  if (options.snapshot != nullptr &&
      (get_context->has_callback() ||
       static_cast_with_check<const SnapshotImpl>(options.snapshot)
               ->GetSequenceNumber() <= fd.largest_seqno)) {
    // We should consider to use options.snapshot->GetSequenceNumber()
    // instead of GetInternalKeySeqno(k), which will make the code
    // easier to understand.
    const MetadataReadBounds* metadata_read_bounds =
        get_context->metadata_read_bounds();
    cache_entry_seq_no = 1 + (metadata_read_bounds != nullptr
                                  ? metadata_read_bounds->read_snapshot_seq
                                  : GetInternalKeySeqno(internal_key));
  }

  // Compute row cache key.
  row_cache_key.TrimAppend(row_cache_key.Size(), row_cache_id_.data(),
                           row_cache_id_.size());
  AppendVarint64(&row_cache_key, fd_number);
  AppendVarint64(&row_cache_key, cache_entry_seq_no);

  // Provide a sequence number for callback checking on cache hit.
  // As cache_entry_seq_no starts at 1, decrease it's value by 1 to get
  // a sequence number align with get context's logic.
  return cache_entry_seq_no == 0 ? 0 : cache_entry_seq_no - 1;
}

bool TableCache::GetFromRowCache(const Slice& user_key, IterKey& row_cache_key,
                                 size_t prefix_size, GetContext* get_context,
                                 Status* read_status, SequenceNumber seq_no) {
  bool found = false;

  row_cache_key.TrimAppend(prefix_size, user_key.data(), user_key.size());
  RowCacheInterface row_cache{ioptions_.row_cache.get()};
  if (auto row_handle = row_cache.Lookup(row_cache_key.GetUserKey())) {
    // Cleanable routine to release the cache entry
    Cleanable value_pinner;
    // If it comes here value is located on the cache.
    // found_row_cache_entry points to the value on cache,
    // and value_pinner has cleanup procedure for the cached entry.
    // After replayGetContextLog() returns, get_context.pinnable_slice_
    // will point to cache entry buffer (or a copy based on that) and
    // cleanup routine under value_pinner will be delegated to
    // get_context.pinnable_slice_. Cache entry is released when
    // get_context.pinnable_slice_ is reset.
    row_cache.RegisterReleaseAsCleanup(row_handle, value_pinner);
    // If row cache hit, knowing cache key is the same to row_cache_key,
    // can use row_cache_key's seq no to construct InternalKey.
    *read_status = replayGetContextLog(*row_cache.Value(row_handle), user_key,
                                       get_context, &value_pinner, seq_no);
    RecordTick(ioptions_.stats, ROW_CACHE_HIT);
    found = true;
  } else {
    RecordTick(ioptions_.stats, ROW_CACHE_MISS);
  }
  return found;
}

void TableCache::UpdateRangeTombstoneSeqnums(
    const ReadOptions& options, TableReader* t,
    MultiGetContext::Range& table_range) {
  std::unique_ptr<FragmentedRangeTombstoneIterator> latest_range_del_iter;
  bool track_newer_versions = false;
  if (!table_range.empty()) {
    ReadCallback* callback = table_range.begin()->get_context->read_callback();
    if (callback != nullptr && callback->GetMetadataReadBounds() != nullptr) {
      for (auto iter = table_range.begin(); iter != table_range.end(); ++iter) {
        if (iter->get_context->NeedToTrackNewerVersions()) {
          track_newer_versions = true;
          break;
        }
      }
    }
  }
  if (track_newer_versions) {
    SequenceNumber latest_range_del_read_seq = 0;
    for (auto iter = table_range.begin(); iter != table_range.end(); ++iter) {
      if (iter->get_context->NeedToTrackNewerVersions()) {
        latest_range_del_read_seq = std::max(latest_range_del_read_seq,
                                             GetInternalKeySeqno(iter->ikey));
      }
    }
    if (latest_range_del_read_seq > 0) {
      latest_range_del_iter.reset(t->NewRangeTombstoneIterator(
          latest_range_del_read_seq, options.timestamp));
    }
  }

  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter;
  if (!options.ignore_range_deletions) {
    range_del_iter.reset(t->NewRangeTombstoneIterator(options));
  }
  if (range_del_iter != nullptr || latest_range_del_iter != nullptr) {
    for (auto iter = table_range.begin(); iter != table_range.end(); ++iter) {
      if (latest_range_del_iter != nullptr &&
          iter->get_context->NeedToTrackNewerVersions()) {
        const SequenceNumber covering_seq =
            latest_range_del_iter->MaxCoveringTombstoneSeqnum(
                iter->ukey_with_ts, iter->get_context->read_callback());
        if (covering_seq != 0) {
          iter->get_context->RecordNewerVersionIfNeeded(covering_seq,
                                                        kTypeRangeDeletion);
        }
      }
      if (range_del_iter == nullptr) {
        continue;
      }
      SequenceNumber* max_covering_tombstone_seq =
          iter->get_context->max_covering_tombstone_seq();
      SequenceNumber seq =
          range_del_iter->MaxCoveringTombstoneSeqnum(iter->ukey_with_ts);
      if (seq > *max_covering_tombstone_seq) {
        *max_covering_tombstone_seq = seq;
        if (iter->get_context->NeedTimestamp()) {
          iter->get_context->SetTimestampFromRangeTombstone(
              range_del_iter->timestamp());
        }
      }
    }
  }
}

Status TableCache::MultiGetFilter(
    const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, const MutableCFOptions& mutable_cf_options,
    HistogramImpl* file_read_hist, int level,
    MultiGetContext::Range* mget_range, TypedHandle** handle) {
  assert(*handle == nullptr);
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;

  // Check if we need to use the row cache. If yes, then we cannot do the
  // filtering here, since the filtering needs to happen after the row cache
  // lookup.
  KeyContext& first_key = *mget_range->begin();
  bool track_newer_versions = false;
  ReadCallback* callback = first_key.get_context->read_callback();
  if (callback != nullptr && callback->GetMetadataReadBounds() != nullptr) {
    for (auto iter = mget_range->begin(); iter != mget_range->end(); ++iter) {
      if (iter->get_context->NeedToTrackNewerVersions()) {
        track_newer_versions = true;
        break;
      }
    }
  }
  if (ioptions_.row_cache && !first_key.get_context->NeedToReadSequence() &&
      !track_newer_versions) {
    return Status::NotSupported();
  }
  Status s;
  TableReader* t = nullptr;
  MultiGetContext::Range tombstone_range(*mget_range, mget_range->begin(),
                                         mget_range->end());
  s = FindTable(options, file_options_, internal_comparator, file_meta, handle,
                mutable_cf_options, &t,
                options.read_tier == kBlockCacheTier /* no_io */,
                file_read_hist,
                /*skip_filters=*/false, level,
                true /* prefetch_index_and_filter_in_cache */,
                /*max_file_size_for_l0_meta_pin=*/0, file_meta.temperature,
                should_pin_table_handles_);
  if (s.ok()) {
    s = t->MultiGetFilter(options, mutable_cf_options.prefix_extractor.get(),
                          mget_range);
  }
  if (s.ok() && (!options.ignore_range_deletions || track_newer_versions)) {
    // Update the range tombstone sequence numbers for the keys here
    // as TableCache::MultiGet may or may not be called, and even if it
    // is, it may be called with fewer keys in the rangedue to filtering.
    UpdateRangeTombstoneSeqnums(options, t, tombstone_range);
  }
  if (mget_range->empty() && *handle) {
    cache_.Release(*handle);
    *handle = nullptr;
  }

  return s;
}

Status TableCache::GetTableProperties(
    const FileOptions& file_options, const ReadOptions& read_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta,
    std::shared_ptr<const TableProperties>* properties,
    const MutableCFOptions& mutable_cf_options, bool no_io) {
  TypedHandle* table_handle = nullptr;
  TableReader* table = nullptr;
  Status s =
      FindTable(read_options, file_options, internal_comparator, file_meta,
                &table_handle, mutable_cf_options, &table, no_io);
  if (!s.ok()) {
    return s;
  }
  *properties = table->GetTableProperties();
  if (table_handle) {
    cache_.Release(table_handle);
  }
  return s;
}

Status TableCache::ApproximateKeyAnchors(
    const ReadOptions& ro, const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, const MutableCFOptions& mutable_cf_options,

    std::vector<TableReader::Anchor>& anchors) {
  Status s;
  TableReader* t = nullptr;
  TypedHandle* handle = nullptr;
  s = FindTable(ro, file_options_, internal_comparator, file_meta, &handle,
                mutable_cf_options, &t);
  if (s.ok() && t != nullptr) {
    s = t->ApproximateKeyAnchors(ro, anchors);
  }
  if (handle != nullptr) {
    cache_.Release(handle);
  }
  return s;
}

size_t TableCache::GetMemoryUsageByTableReader(
    const FileOptions& file_options, const ReadOptions& read_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, const MutableCFOptions& mutable_cf_options) {
  TypedHandle* table_handle = nullptr;
  TableReader* table = nullptr;
  Status s =
      FindTable(read_options, file_options, internal_comparator, file_meta,
                &table_handle, mutable_cf_options, &table, true /* no_io */);
  if (!s.ok()) {
    return 0;
  }
  auto ret = table->ApproximateMemoryUsage();
  if (table_handle) {
    cache_.Release(table_handle);
  }
  return ret;
}

void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}

uint64_t TableCache::ApproximateOffsetOf(
    const ReadOptions& read_options, const Slice& key,
    const FileMetaData& file_meta, TableReaderCaller caller,
    const InternalKeyComparator& internal_comparator,
    const MutableCFOptions& mutable_cf_options) {
  uint64_t result = 0;
  TableReader* table_reader = nullptr;
  TypedHandle* table_handle = nullptr;
  Status s =
      FindTable(read_options, file_options_, internal_comparator, file_meta,
                &table_handle, mutable_cf_options, &table_reader);

  if (s.ok() && table_reader != nullptr) {
    result = table_reader->ApproximateOffsetOf(read_options, key, caller);
  }
  if (table_handle != nullptr) {
    cache_.Release(table_handle);
  }

  return result;
}

uint64_t TableCache::ApproximateSize(
    const ReadOptions& read_options, const Slice& start, const Slice& end,
    const FileMetaData& file_meta, TableReaderCaller caller,
    const InternalKeyComparator& internal_comparator,
    const MutableCFOptions& mutable_cf_options) {
  uint64_t result = 0;
  TableReader* table_reader = nullptr;
  TypedHandle* table_handle = nullptr;
  Status s =
      FindTable(read_options, file_options_, internal_comparator, file_meta,
                &table_handle, mutable_cf_options, &table_reader);

  if (s.ok() && table_reader != nullptr) {
    result = table_reader->ApproximateSize(read_options, start, end, caller);
  }
  if (table_handle != nullptr) {
    cache_.Release(table_handle);
  }

  return result;
}

void TableCache::ReleaseObsolete(Cache* cache, uint64_t file_number,
                                 Cache::Handle* h,
                                 uint32_t uncache_aggressiveness) {
  CacheInterface typed_cache(cache);
  TypedHandle* table_handle = reinterpret_cast<TypedHandle*>(h);
  if (table_handle == nullptr) {
    table_handle = typed_cache.Lookup(GetSliceForFileNumber(&file_number));
  }
  if (table_handle != nullptr) {
    TableReader* table_reader = typed_cache.Value(table_handle);
    table_reader->MarkObsolete(uncache_aggressiveness);
    // Mark the entry Invisible so that if concurrent readers hold references,
    // the entry will be erased when the last reference is released.
    cache->Erase(GetSliceForFileNumber(&file_number));
    typed_cache.Release(table_handle);
  }
}

}  // namespace ROCKSDB_NAMESPACE
