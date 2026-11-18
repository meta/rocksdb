//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(Status, TableCache::Get)
(const ReadOptions& options, const InternalKeyComparator& internal_comparator,
 const FileMetaData& file_meta, const Slice& k, GetContext* get_context,
 const MutableCFOptions& mutable_cf_options, HistogramImpl* file_read_hist,
 bool skip_filters, int level, size_t max_file_size_for_l0_meta_pin) {
  auto& fd = file_meta.fd;
  std::string* row_cache_entry = nullptr;
  bool done = false;
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;

  // Check row cache if enabled.
  // Reuse row_cache_key sequence number when row cache hits.
  Status s;
  if (ioptions_.row_cache && !get_context->NeedToReadSequence() &&
      !get_context->NeedToTrackNewerVersions()) {
    auto user_key = ExtractUserKey(k);
    uint64_t cache_entry_seq_no =
        CreateRowCacheKeyPrefix(options, fd, k, get_context, row_cache_key);
    done = GetFromRowCache(user_key, row_cache_key, row_cache_key.Size(),
                           get_context, &s, cache_entry_seq_no);
    if (!done) {
      row_cache_entry = &row_cache_entry_buffer;
    }
  }
  TEST_SYNC_POINT_CALLBACK("TableCache::Get::BeforeFindTable",
                           const_cast<FileDescriptor*>(&fd));
  TableReader* t = nullptr;
  TypedHandle* handle = nullptr;
  if (s.ok() && !done) {
    s = FindTable(options, file_options_, internal_comparator, file_meta,
                  &handle, mutable_cf_options, &t,
                  options.read_tier == kBlockCacheTier /* no_io */,
                  file_read_hist, skip_filters, level,
                  true /* prefetch_index_and_filter_in_cache */,
                  max_file_size_for_l0_meta_pin, file_meta.temperature,
                  should_pin_table_handles_);
    SequenceNumber* max_covering_tombstone_seq =
        get_context->max_covering_tombstone_seq();
    const bool track_newer_versions = get_context->NeedToTrackNewerVersions();
    if (s.ok() && ((max_covering_tombstone_seq != nullptr &&
                    !options.ignore_range_deletions) ||
                   track_newer_versions)) {
      // Runs only for an L0 file with range tombstones (see
      // RangeTombstonesMayCoverUserKey()).
      const auto may_cover = [&]() {
        return level != 0 || RangeTombstonesMayCoverUserKey(
                                 internal_comparator.user_comparator(),
                                 file_meta, ExtractUserKey(k));
      };
      if (track_newer_versions) {
        std::unique_ptr<FragmentedRangeTombstoneIterator> latest_range_del_iter(
            t->NewRangeTombstoneIterator(GetInternalKeySeqno(k),
                                         options.timestamp));
        const SequenceNumber covering_seq =
            latest_range_del_iter != nullptr && may_cover()
                ? latest_range_del_iter->MaxCoveringTombstoneSeqnum(
                      ExtractUserKey(k), get_context->read_callback())
                : 0;
        if (covering_seq != 0) {
          get_context->RecordNewerVersionIfNeeded(covering_seq,
                                                  kTypeRangeDeletion);
        }
      }
      if (max_covering_tombstone_seq != nullptr &&
          !options.ignore_range_deletions) {
        std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
            t->NewRangeTombstoneIterator(options));
        if (range_del_iter != nullptr && may_cover()) {
          SequenceNumber seq =
              range_del_iter->MaxCoveringTombstoneSeqnum(ExtractUserKey(k));
          if (seq > *max_covering_tombstone_seq) {
            *max_covering_tombstone_seq = seq;
            if (get_context->NeedTimestamp()) {
              get_context->SetTimestampFromRangeTombstone(
                  range_del_iter->timestamp());
            }
          }
        }
      }
    }
    if (s.ok()) {
      get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
      s = CO_AWAIT(t->Get, options, k, get_context,
                   mutable_cf_options.prefix_extractor.get(), skip_filters);
      get_context->SetReplayLog(nullptr);
    } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
      // Couldn't find table in cache and couldn't open it because of no_io.
      get_context->MarkKeyMayExist();
      done = true;
    }
  }

  // Put the replay log in row cache only if something was found.
  if (!done && s.ok() && row_cache_entry && !row_cache_entry->empty()) {
    RowCacheInterface row_cache{ioptions_.row_cache.get()};
    size_t charge = row_cache_entry->capacity() + sizeof(std::string);
    auto row_ptr = new std::string(std::move(*row_cache_entry));
    Status rcs = row_cache.Insert(row_cache_key.GetUserKey(), row_ptr, charge);
    if (!rcs.ok()) {
      // If row cache is full, it's OK to continue, but we keep ownership of
      // row_ptr.
      delete row_ptr;
    }
  }

  if (handle != nullptr) {
    cache_.Release(handle);
  }
  CO_RETURN s;
}

// Batched version of TableCache::MultiGet.
DEFINE_SYNC_AND_ASYNC(Status, TableCache::MultiGet)
(const ReadOptions& options, const InternalKeyComparator& internal_comparator,
 const FileMetaData& file_meta, const MultiGetContext::Range* mget_range,
 const MutableCFOptions& mutable_cf_options, HistogramImpl* file_read_hist,
 bool skip_filters, bool skip_range_deletions, int level, TypedHandle* handle) {
  auto& fd = file_meta.fd;
  Status s;
  TEST_SYNC_POINT_CALLBACK("TableCache::MultiGet::BeforeFindTable",
                           const_cast<FileDescriptor*>(&fd));
  TableReader* t = fd.pinned_reader.Get();
  MultiGetRange table_range(*mget_range, mget_range->begin(),
                            mget_range->end());
  if (handle != nullptr && t == nullptr) {
    t = cache_.Value(handle);
  }
  autovector<std::string, MultiGetContext::MAX_BATCH_SIZE> row_cache_entries;
  IterKey row_cache_key;
  size_t row_cache_key_prefix_size = 0;
  KeyContext& first_key = *table_range.begin();
  bool track_newer_versions = false;
  ReadCallback* callback = first_key.get_context->read_callback();
  const bool is_metadata_read =
      callback != nullptr && callback->GetMetadataReadBounds() != nullptr;
  if (is_metadata_read) {
    for (auto iter = table_range.begin(); iter != table_range.end(); ++iter) {
      if (iter->get_context->NeedToTrackNewerVersions()) {
        track_newer_versions = true;
        break;
      }
    }
  }
  bool lookup_row_cache = ioptions_.row_cache &&
                          !first_key.get_context->NeedToReadSequence() &&
                          !track_newer_versions;

  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (lookup_row_cache) {
    GetContext* first_context = first_key.get_context;
    const SequenceNumber row_cache_entry_seq_no = CreateRowCacheKeyPrefix(
        options, fd, first_key.ikey, first_context, row_cache_key);
    const SequenceNumber replay_seq_no =
        is_metadata_read ? row_cache_entry_seq_no : kMaxSequenceNumber;
    row_cache_key_prefix_size = row_cache_key.Size();

    for (auto miter = table_range.begin(); miter != table_range.end();
         ++miter) {
      const Slice& user_key = miter->ukey_with_ts;

      GetContext* get_context = miter->get_context;

      Status read_status;
      bool ret =
          GetFromRowCache(user_key, row_cache_key, row_cache_key_prefix_size,
                          get_context, &read_status, replay_seq_no);
      if (!read_status.ok()) {
        CO_RETURN read_status;
      }
      if (ret) {
        table_range.SkipKey(miter);
      } else {
        row_cache_entries.emplace_back();
        get_context->SetReplayLog(&(row_cache_entries.back()));
      }
    }
  }

  // Check that table_range is not empty. Its possible all keys may have been
  // found in the row cache and thus the range may now be empty
  if (s.ok() && !table_range.empty()) {
    if (t == nullptr) {
      assert(handle == nullptr);
      s = FindTable(options, file_options_, internal_comparator, file_meta,
                    &handle, mutable_cf_options, &t,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    file_read_hist, skip_filters, level,
                    true /* prefetch_index_and_filter_in_cache */,
                    0 /*max_file_size_for_l0_meta_pin*/, file_meta.temperature,
                    should_pin_table_handles_);
      TEST_SYNC_POINT_CALLBACK("TableCache::MultiGet:FindTable", &s);
      assert(!s.ok() || t);
    }
    if (s.ok() && !skip_range_deletions &&
        (!options.ignore_range_deletions || track_newer_versions)) {
      UpdateRangeTombstoneSeqnums(
          options, t, internal_comparator.user_comparator(),
          level == 0 ? &file_meta : nullptr, table_range);
    }
    if (s.ok()) {
      CO_AWAIT(t->MultiGet, options, &table_range,
               mutable_cf_options.prefix_extractor.get(), skip_filters);
    } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
      for (auto iter = table_range.begin(); iter != table_range.end(); ++iter) {
        Status* status = iter->s;
        if (status->IsIncomplete()) {
          // Couldn't find Table in cache but treat as kFound if no_io set
          iter->get_context->MarkKeyMayExist();
          s = Status::OK();
        }
      }
    }
  }

  if (lookup_row_cache) {
    size_t row_idx = 0;
    RowCacheInterface row_cache{ioptions_.row_cache.get()};

    for (auto miter = table_range.begin(); miter != table_range.end();
         ++miter) {
      std::string& row_cache_entry = row_cache_entries[row_idx++];
      const Slice& user_key = miter->ukey_with_ts;
      GetContext* get_context = miter->get_context;

      get_context->SetReplayLog(nullptr);
      // Compute row cache key.
      row_cache_key.TrimAppend(row_cache_key_prefix_size, user_key.data(),
                               user_key.size());
      // Put the replay log in row cache only if something was found.
      if (s.ok() && !row_cache_entry.empty()) {
        size_t charge = row_cache_entry.capacity() + sizeof(std::string);
        auto row_ptr = new std::string(std::move(row_cache_entry));
        // If row cache is full, it's OK.
        row_cache.Insert(row_cache_key.GetUserKey(), row_ptr, charge)
            .PermitUncheckedError();
      }
    }
  }

  if (handle != nullptr) {
    cache_.Release(handle);
  }
  CO_RETURN s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
