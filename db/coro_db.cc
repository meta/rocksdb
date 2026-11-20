// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/rocksdb_namespace.h"

#if USE_COROUTINES

#include <algorithm>
#include <array>
#include <cassert>
#include <utility>
#include <vector>

#include "folly/Executor.h"
#include "folly/coro/CurrentExecutor.h"
#include "folly/coro/Nothrow.h"
#include "folly/executors/IOExecutor.h"
#include "folly/io/async/EventBase.h"
#include "rocksdb/coro_db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_reader.h"
#include "table/multiget_context.h"
#include "table/sst_file_reader_impl.h"
#include "util/coro_stats_util.h"

namespace ROCKSDB_NAMESPACE {

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        ColumnFamilyHandle* column_family,
                                        const Slice& key, PinnableSlice* value,
                                        std::string* timestamp) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();
  CoroutineStatsConfig stats_config = CaptureAndDisableCoroutineStatsConfig();

  Status status;
  if (coro_db == nullptr || read_event_base == nullptr) {
    CoroutineStatsContextScope stats_scope(
        WithoutAsyncReadCPUTime(std::move(stats_config)), db->GetEnv());
    status = db->Get(options, column_family, key, value, timestamp);
  } else {
    auto task = [](CoroutineStatsConfig task_stats_config, Env* task_env,
                   CoroDB* task_db, const ReadOptions& task_options,
                   ColumnFamilyHandle* task_column_family, Slice task_key,
                   PinnableSlice* task_value,
                   std::string* task_timestamp) -> folly::coro::Task<Status> {
      CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                             task_env);
      co_return co_await folly::coro::co_nothrow(
          task_db->GetCoroutine(task_options, task_column_family, task_key,
                                task_value, task_timestamp));
    }(std::move(stats_config), db->GetEnv(), coro_db, options, column_family,
                                                key, value, timestamp);
    auto* current_executor = co_await folly::coro::co_current_executor;
    if (current_executor == read_event_base) {
      status = co_await folly::coro::co_nothrow(std::move(task));
    } else {
      status = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
          folly::Executor::getKeepAliveToken(read_event_base),
          std::move(task)));
    }
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        ColumnFamilyHandle* column_family,
                                        const Slice& key, std::string* value,
                                        std::string* timestamp) {
  assert(value != nullptr);
  PinnableSlice pinnable_value(value);
  assert(!pinnable_value.IsPinned());
  Status status = co_await folly::coro::co_nothrow(
      CoGet(db, options, column_family, key, &pinnable_value, timestamp));
  if (status.ok() && pinnable_value.IsPinned()) {
    value->assign(pinnable_value.data(), pinnable_value.size());
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        const Slice& key, std::string* value) {
  assert(db != nullptr);
  return CoGet(db, options, db->DefaultColumnFamily(), key, value);
}

folly::coro::Task<Status> CoroDB::CoGet(DB* db, const ReadOptions& options,
                                        const Slice& key, std::string* value,
                                        std::string* timestamp) {
  assert(db != nullptr);
  return CoGet(db, options, db->DefaultColumnFamily(), key, value, timestamp);
}

folly::coro::Task<Status> CoroDB::CoGet(SstFileReader* reader,
                                        const ReadOptions& options,
                                        const Slice& key,
                                        PinnableSlice* value) {
  assert(reader != nullptr);
  SstFileReader::Rep* rep = reader->rep_.get();
  auto* read_executor = rep->read_executor;
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();
  CoroutineStatsConfig stats_config = CaptureAndDisableCoroutineStatsConfig();

  Status status;
  if (read_event_base == nullptr) {
    CoroutineStatsContextScope stats_scope(
        WithoutAsyncReadCPUTime(std::move(stats_config)), rep->options.env);
    status = reader->Get(options, key, value);
  } else {
    auto task = [](CoroutineStatsConfig task_stats_config, Env* task_env,
                   SstFileReader* task_reader, const ReadOptions& task_options,
                   Slice task_key,
                   PinnableSlice* task_value) -> folly::coro::Task<Status> {
      CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                             task_env);
      co_return co_await folly::coro::co_nothrow(
          task_reader->rep_->GetCoroutine(task_options, task_key, task_value));
    }(std::move(stats_config), rep->options.env, reader, options, key, value);
    auto* current_executor = co_await folly::coro::co_current_executor;
    if (current_executor == read_event_base) {
      status = co_await folly::coro::co_nothrow(std::move(task));
    } else {
      status = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
          folly::Executor::getKeepAliveToken(read_event_base),
          std::move(task)));
    }
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGet(SstFileReader* reader,
                                        const ReadOptions& options,
                                        const Slice& key, std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_value(value);
  assert(!pinnable_value.IsPinned());
  Status status = co_await folly::coro::co_nothrow(
      CoGet(reader, options, key, &pinnable_value));
  if (status.ok() && pinnable_value.IsPinned()) {
    value->assign(pinnable_value.data(), pinnable_value.size());
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGetEntity(DB* db,
                                              const ReadOptions& options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              PinnableWideColumns* columns) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();
  CoroutineStatsConfig stats_config = CaptureAndDisableCoroutineStatsConfig();

  Status status;
  if (coro_db == nullptr || read_event_base == nullptr) {
    CoroutineStatsContextScope stats_scope(
        WithoutAsyncReadCPUTime(std::move(stats_config)), db->GetEnv());
    status = db->GetEntity(options, column_family, key, columns);
  } else {
    auto task =
        [](CoroutineStatsConfig task_stats_config, Env* task_env,
           CoroDB* task_db, const ReadOptions& task_options,
           ColumnFamilyHandle* task_column_family, Slice task_key,
           PinnableWideColumns* task_columns) -> folly::coro::Task<Status> {
      CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                             task_env);
      co_return co_await folly::coro::co_nothrow(task_db->GetEntityCoroutine(
          task_options, task_column_family, task_key, task_columns));
    }(std::move(stats_config), db->GetEnv(), coro_db, options, column_family,
                                              key, columns);
    status = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base), std::move(task)));
  }
  co_return status;
}

folly::coro::Task<Status> CoroDB::CoGetEntity(DB* db,
                                              const ReadOptions& options,
                                              const Slice& key,
                                              PinnableWideColumns* columns) {
  assert(db != nullptr);
  return CoGetEntity(db, options, db->DefaultColumnFamily(), key, columns);
}

folly::coro::Task<Status> CoroDB::CoGetEntity(DB* db,
                                              const ReadOptions& options,
                                              const Slice& key,
                                              PinnableAttributeGroups* result) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();
  CoroutineStatsConfig stats_config = CaptureAndDisableCoroutineStatsConfig();

  Status status;
  if (coro_db == nullptr || read_event_base == nullptr) {
    CoroutineStatsContextScope stats_scope(
        WithoutAsyncReadCPUTime(std::move(stats_config)), db->GetEnv());
    status = db->GetEntity(options, key, result);
  } else {
    auto task =
        [](CoroutineStatsConfig task_stats_config, Env* task_env,
           CoroDB* task_db, const ReadOptions& task_options, Slice task_key,
           PinnableAttributeGroups* task_result) -> folly::coro::Task<Status> {
      CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                             task_env);
      co_return co_await folly::coro::co_nothrow(
          task_db->GetEntityCoroutine(task_options, task_key, task_result));
    }(std::move(stats_config), db->GetEnv(), coro_db, options, key, result);
    status = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base), std::move(task)));
  }
  co_return status;
}

folly::coro::Task<std::vector<Status>> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    const std::vector<Slice>& keys, std::vector<std::string>* values,
    std::vector<std::string>* timestamps) {
  const size_t num_keys = keys.size();
  std::vector<Status> statuses(num_keys);
  std::vector<PinnableSlice> pinnable_values(num_keys);

  values->resize(num_keys);
  if (timestamps != nullptr) {
    timestamps->resize(num_keys);
  }
  co_await folly::coro::co_nothrow(CoMultiGet(
      db, options, num_keys,
      const_cast<ColumnFamilyHandle**>(column_families.data()), keys.data(),
      pinnable_values.data(),
      timestamps == nullptr ? nullptr : timestamps->data(), statuses.data(),
      /*sorted_input=*/false));
  for (size_t i = 0; i < num_keys; ++i) {
    if (statuses[i].ok()) {
      (*values)[i].assign(pinnable_values[i].data(), pinnable_values[i].size());
    }
  }
  co_return statuses;
}

folly::coro::Task<std::vector<Status>> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options, const std::vector<Slice>& keys,
    std::vector<std::string>* values, std::vector<std::string>* timestamps) {
  assert(db != nullptr);
  std::vector<ColumnFamilyHandle*> column_families(keys.size(),
                                                   db->DefaultColumnFamily());
  co_return co_await folly::coro::co_nothrow(
      CoMultiGet(db, options, column_families, keys, values, timestamps));
}

folly::coro::Task<std::vector<Status>> CoroDB::CoMultiGet(
    SstFileReader* reader, const ReadOptions& options,
    const std::vector<Slice>& keys, std::vector<PinnableSlice>* values) {
  assert(reader != nullptr);
  SstFileReader::Rep* rep = reader->rep_.get();
  auto* read_executor = rep->read_executor;
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();
  CoroutineStatsConfig stats_config = CaptureAndDisableCoroutineStatsConfig();

  std::vector<Status> statuses;
  if (read_event_base == nullptr) {
    CoroutineStatsContextScope stats_scope(
        WithoutAsyncReadCPUTime(std::move(stats_config)), rep->options.env);
    statuses = reader->MultiGet(options, keys, values);
  } else {
    auto task = [](CoroutineStatsConfig task_stats_config, Env* task_env,
                   SstFileReader* task_reader, const ReadOptions& task_options,
                   const std::vector<Slice>& task_keys,
                   std::vector<PinnableSlice>* task_values)
        -> folly::coro::Task<std::vector<Status>> {
      CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                             task_env);
      co_return co_await folly::coro::co_nothrow(
          task_reader->rep_->MultiGetCoroutine(task_options, task_keys,
                                               task_values));
    }(std::move(stats_config), rep->options.env, reader, options, keys, values);
    statuses = co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base), std::move(task)));
  }
  co_return statuses;
}

folly::coro::Task<std::vector<Status>> CoroDB::CoMultiGet(
    SstFileReader* reader, const ReadOptions& options,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<PinnableSlice> pinnable_values;
  std::vector<Status> statuses = co_await folly::coro::co_nothrow(
      CoMultiGet(reader, options, keys, &pinnable_values));
  values->resize(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    if (statuses[i].ok()) {
      (*values)[i].assign(pinnable_values[i].data(), pinnable_values[i].size());
    }
  }
  co_return statuses;
}

folly::coro::Task<void> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options, size_t num_keys,
    ColumnFamilyHandle** column_families, const Slice* keys,
    PinnableSlice* values, std::string* timestamps, Status* statuses,
    bool sorted_input) {
  assert(db != nullptr);
  CoroDB* coro_db = db->GetCoroDB();
  auto* file_system = db->GetFileSystem();
  auto* read_executor =
      file_system == nullptr ? nullptr : file_system->GetReadExecutor();
  auto* read_event_base =
      read_executor == nullptr ? nullptr : read_executor->getEventBase();
  CoroutineStatsConfig stats_config = CaptureAndDisableCoroutineStatsConfig();

  if (coro_db == nullptr || read_event_base == nullptr) {
    CoroutineStatsContextScope stats_scope(
        WithoutAsyncReadCPUTime(std::move(stats_config)), db->GetEnv());
    db->MultiGet(options, num_keys, column_families, keys, values, timestamps,
                 statuses, sorted_input);
  } else {
    auto task = [](CoroutineStatsConfig task_stats_config, Env* task_env,
                   CoroDB* task_db, const ReadOptions& task_options,
                   size_t task_num_keys,
                   ColumnFamilyHandle** task_column_families,
                   const Slice* task_keys, PinnableSlice* task_values,
                   std::string* task_timestamps, Status* task_statuses,
                   bool task_sorted_input) -> folly::coro::Task<void> {
      CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                             task_env);
      co_await folly::coro::co_nothrow(task_db->MultiGetCoroutine(
          task_options, task_num_keys, task_column_families, task_keys,
          task_values, task_timestamps, task_statuses, task_sorted_input));
    }(std::move(stats_config), db->GetEnv(), coro_db, options, num_keys,
                                           column_families, keys, values,
                                           timestamps, statuses, sorted_input);
    co_await folly::coro::co_nothrow(folly::coro::co_withExecutor(
        folly::Executor::getKeepAliveToken(read_event_base), std::move(task)));
  }
}

folly::coro::Task<void> CoroDB::CoMultiGet(
    DB* db, const ReadOptions& options, ColumnFamilyHandle* column_family,
    size_t num_keys, const Slice* keys, PinnableSlice* values,
    std::string* timestamps, Status* statuses, bool sorted_input) {
  if (num_keys > MultiGetContext::MAX_BATCH_SIZE) {
    std::vector<ColumnFamilyHandle*> column_families(num_keys, column_family);
    co_await folly::coro::co_nothrow(
        CoMultiGet(db, options, num_keys, column_families.data(), keys, values,
                   timestamps, statuses, sorted_input));
  } else {
    std::array<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
        column_families;
    std::fill(column_families.begin(), column_families.begin() + num_keys,
              column_family);
    co_await folly::coro::co_nothrow(
        CoMultiGet(db, options, num_keys, column_families.data(), keys, values,
                   timestamps, statuses, sorted_input));
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_COROUTINES
