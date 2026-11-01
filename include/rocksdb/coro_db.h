//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "folly/coro/Task.h"
#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

template <typename Base>
class CoroStackableDBBase;
class SstFileReader;

// EXPERIMENTAL native coroutine read interface.
//
// Using this interface requires Folly to be available and RocksDB to be built
// with USE_COROUTINES=1.
//
// These are the coroutine counterparts to DB::GetAsync() and
// DB::MultiGetAsync(). See those APIs for the shared read, input/output
// lifetime, and concurrency contract. Task completion takes the place of
// OnComplete for those requirements.
//
// The returned tasks are lazy: no read begins until a task is awaited or
// started.
//
// CoGet() and CoMultiGet() schedule native coroutine reads on the read
// IOExecutor's EventBase. They use the synchronous DB API when native
// coroutine reads or a read executor are unavailable.
//
// STATS:
// CoGet() and CoMultiGet() populate TLS with stats for only that operation.
// Set the desired stats configuration on the thread that starts or awaits each
// task. Starting the task captures that configuration and resets it to
// disabled. SetAsyncReadCPUTimeEnabled() (rocksdb/perf_level.h) is part of
// that configuration.
// They can resume their callers on a different thread, so reading TLS after
// awaiting them is not valid. To consume coroutine stats, override the
// protected CoroStackableDB hook and copy the TLS values immediately after
// awaiting the wrapped operation, before suspending again.
//
// Stats are generally more expensive in coroutine APIs than sync counterpart
// due to request context overhead. For optimal performance when stats are not
// needed, set PerfLevel::kDisable and disable I/O stats with
// get_iostats_context()->disable_iostats = true before each read request.
class CoroDB {
 public:
  virtual ~CoroDB() = default;

  static folly::coro::Task<Status> CoGet(DB* db, const ReadOptions& options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key, PinnableSlice* value,
                                         std::string* timestamp);

  static folly::coro::Task<Status> CoGet(DB* db, const ReadOptions& options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key, std::string* value,
                                         std::string* timestamp);

  static folly::coro::Task<Status> CoGet(DB* db, const ReadOptions& options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key,
                                         PinnableSlice* value) {
    return CoGet(db, options, column_family, key, value,
                 /*timestamp=*/nullptr);
  }

  static folly::coro::Task<Status> CoGet(DB* db, const ReadOptions& options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key, std::string* value) {
    return CoGet(db, options, column_family, key, value,
                 /*timestamp=*/nullptr);
  }

  static folly::coro::Task<Status> CoGet(DB* db, const ReadOptions& options,
                                         const Slice& key, std::string* value);

  static folly::coro::Task<Status> CoGet(DB* db, const ReadOptions& options,
                                         const Slice& key, std::string* value,
                                         std::string* timestamp);

  static folly::coro::Task<Status> CoGet(SstFileReader* reader,
                                         const ReadOptions& options,
                                         const Slice& key,
                                         PinnableSlice* value);

  static folly::coro::Task<Status> CoGet(SstFileReader* reader,
                                         const ReadOptions& options,
                                         const Slice& key, std::string* value);

  static folly::coro::Task<std::vector<Status>> CoMultiGet(
      DB* db, const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<Slice>& keys, std::vector<std::string>* values,
      std::vector<std::string>* timestamps);

  static folly::coro::Task<std::vector<Status>> CoMultiGet(
      DB* db, const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<Slice>& keys, std::vector<std::string>* values) {
    return CoMultiGet(db, options, column_families, keys, values,
                      /*timestamps=*/nullptr);
  }

  static folly::coro::Task<std::vector<Status>> CoMultiGet(
      DB* db, const ReadOptions& options, const std::vector<Slice>& keys,
      std::vector<std::string>* values) {
    return CoMultiGet(db, options, keys, values, /*timestamps=*/nullptr);
  }

  static folly::coro::Task<std::vector<Status>> CoMultiGet(
      DB* db, const ReadOptions& options, const std::vector<Slice>& keys,
      std::vector<std::string>* values, std::vector<std::string>* timestamps);

  static folly::coro::Task<std::vector<Status>> CoMultiGet(
      SstFileReader* reader, const ReadOptions& options,
      const std::vector<Slice>& keys, std::vector<PinnableSlice>* values);

  static folly::coro::Task<std::vector<Status>> CoMultiGet(
      SstFileReader* reader, const ReadOptions& options,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  static folly::coro::Task<void> CoMultiGet(
      DB* db, const ReadOptions& options, size_t num_keys,
      ColumnFamilyHandle** column_families, const Slice* keys,
      PinnableSlice* values, std::string* timestamps, Status* statuses,
      bool sorted_input = false);

  static folly::coro::Task<void> CoMultiGet(
      DB* db, const ReadOptions& options, ColumnFamilyHandle* column_family,
      size_t num_keys, const Slice* keys, PinnableSlice* values,
      std::string* timestamps, Status* statuses, bool sorted_input = false);

  static folly::coro::Task<void> CoMultiGet(DB* db, const ReadOptions& options,
                                            ColumnFamilyHandle* column_family,
                                            size_t num_keys, const Slice* keys,
                                            PinnableSlice* values,
                                            Status* statuses,
                                            bool sorted_input = false) {
    return CoMultiGet(db, options, column_family, num_keys, keys, values,
                      /*timestamps=*/nullptr, statuses, sorted_input);
  }

  static folly::coro::Task<void> CoMultiGet(
      DB* db, const ReadOptions& options, size_t num_keys,
      ColumnFamilyHandle** column_families, const Slice* keys,
      PinnableSlice* values, Status* statuses, bool sorted_input = false) {
    return CoMultiGet(db, options, num_keys, column_families, keys, values,
                      /*timestamps=*/nullptr, statuses, sorted_input);
  }

  static folly::coro::Task<Status> CoGetEntity(
      DB* db, const ReadOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, PinnableWideColumns* columns);

  static folly::coro::Task<Status> CoGetEntity(DB* db,
                                               const ReadOptions& options,
                                               const Slice& key,
                                               PinnableWideColumns* columns);

  static folly::coro::Task<Status> CoGetEntity(DB* db,
                                               const ReadOptions& options,
                                               const Slice& key,
                                               PinnableAttributeGroups* result);

 protected:
  friend class DB;
  template <typename Base>
  friend class CoroStackableDBBase;

  // Reads key and completes after populating value and the optional timestamp.
  // The returned Status describes the operation and its outputs.
  virtual folly::coro::Task<Status> GetCoroutine(
      const ReadOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, PinnableSlice* value, std::string* timestamp) = 0;

  // Reads num_keys keys and completes after populating all per-key statuses,
  // values, and optional timestamps. There is no aggregate Status.
  virtual folly::coro::Task<void> MultiGetCoroutine(
      const ReadOptions& options, size_t num_keys,
      ColumnFamilyHandle** column_families, const Slice* keys,
      PinnableSlice* values, std::string* timestamps, Status* statuses,
      bool sorted_input) = 0;

  virtual folly::coro::Task<Status> GetEntityCoroutine(
      const ReadOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, PinnableWideColumns* columns) = 0;

  virtual folly::coro::Task<Status> GetEntityCoroutine(
      const ReadOptions& options, const Slice& key,
      PinnableAttributeGroups* result) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
