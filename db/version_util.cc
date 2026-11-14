//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/version_util.h"

#include <atomic>
#include <functional>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

Status LoadTableHandlersHelper(
    const std::vector<std::pair<FileMetaData*, int>>& files_meta,
    TableCache* table_cache, const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache,
    const MutableCFOptions& mutable_cf_options,
    size_t max_file_size_for_l0_meta_pin, const ReadOptions& read_options,
    std::atomic<bool>* stop) {
  assert(table_cache != nullptr);

  if (files_meta.empty()) {
    return Status::OK();
  }

  std::atomic<size_t> next_file_meta_idx(0);
  std::atomic<bool> has_error(false);
  Status ret;
  std::function<void()> load_handlers_func([&]() {
    while (true) {
      size_t file_idx = next_file_meta_idx.fetch_add(1);

      if (has_error.load(std::memory_order_relaxed)) {
        break;
      }

      if (file_idx >= files_meta.size()) {
        break;
      }

      if (stop != nullptr && stop->load()) {
        break;
      }

      auto* cache = table_cache->get_cache().get();
      if (cache->GetCapacity() < TableCache::kInfiniteCapacity &&
          cache->GetUsage() >= cache->GetCapacity()) {
        break;
      }

      auto* file_meta = files_meta[file_idx].first;
      int level = files_meta[file_idx].second;

      TEST_SYNC_POINT_CALLBACK(
          "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable", file_meta);

      TableCache::TypedHandle* handle = nullptr;
      TableReader* table_reader = nullptr;
      const bool prefetch_index_and_filter_for_file =
          prefetch_index_and_filter_in_cache &&
          !file_meta->skip_index_and_filter_blocks_prefetch;
      auto status = table_cache->FindTable(
          read_options, file_options, internal_comparator, *file_meta, &handle,
          mutable_cf_options, &table_reader, false /* no_io */,
          internal_stats->GetFileReadHist(level), false /* skip_filters */,
          level, prefetch_index_and_filter_for_file,
          max_file_size_for_l0_meta_pin, file_meta->temperature,
          true /* pin_table_handle */);

      TEST_SYNC_POINT_CALLBACK(
          "VersionBuilder::Rep::LoadTableHandlers::AfterFindTable", &status);

      if (!status.ok()) {
        bool expected = false;
        if (has_error.compare_exchange_strong(expected, true)) {
          ret = status;
        }
      }
    }
  });

  std::vector<std::jthread> threads;
  for (int i = 1; i < max_threads; i++) {
    try {
      threads.emplace_back(load_handlers_func);
    } catch (const std::system_error&) {
      break;
    }
  }
  load_handlers_func();
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
