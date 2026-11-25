//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <coroutine>
#include <cstddef>
#include <utility>
#include <vector>

#include "folly/coro/Collect.h"
#include "folly/coro/Nothrow.h"
#include "folly/coro/Task.h"
#include "rocksdb/external_table.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL native coroutine lookup interface.
//
// Using this interface requires Folly to be available and RocksDB to be built
// with USE_COROUTINES=1. The returned tasks are lazy and run on the caller's
// executor. The reader and all input and output storage must remain valid until
// the task completes.
template <ExternalTableMode Mode>
class CoroExternalTableReaderBase;

template <>
class CoroExternalTableReaderBase<ExternalTableMode::kOnlyZeroSeqnoAndPuts> {
 public:
  virtual ~CoroExternalTableReaderBase() = default;

  virtual folly::coro::Task<Status> GetCoroutine(
      const ReadOptions& read_options, const Slice& key,
      const SliceTransform* prefix_extractor, PinnableSlice* result) = 0;

  // The default implementation starts one GetCoroutine task per key before
  // awaiting their completion.
  virtual folly::coro::Task<void> MultiGetCoroutine(
      const ReadOptions& read_options, const std::vector<Slice>& keys,
      const SliceTransform* prefix_extractor,
      std::vector<PinnableSlice>* results, std::vector<Status>* statuses) {
    results->resize(keys.size());
    std::vector<folly::coro::Task<Status>> tasks;
    tasks.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      tasks.emplace_back(GetCoroutine(read_options, keys[i], prefix_extractor,
                                      &(*results)[i]));
    }
    std::vector<Status> collected_statuses = co_await folly::coro::co_nothrow(
        folly::coro::collectAllRange(std::move(tasks)));
    statuses->resize(collected_statuses.size());
    for (size_t i = 0; i < collected_statuses.size(); ++i) {
      (*statuses)[i] = std::move(collected_statuses[i]);
    }
  }
};

template <>
class CoroExternalTableReaderBase<ExternalTableMode::kFull> {
 public:
  virtual ~CoroExternalTableReaderBase() = default;

  virtual folly::coro::Task<Status> GetCoroutine(
      const ReadOptions& read_options, const Slice& key,
      const SliceTransform* prefix_extractor,
      ExternalTableGetContext* context) = 0;

  // The default implementation starts one GetCoroutine task per request before
  // awaiting their completion.
  virtual folly::coro::Task<void> MultiGetCoroutine(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      ExternalTableMultiGetContext* context, std::vector<Status>* statuses) {
    const size_t num_requests = context->Size();
    std::vector<ExternalTableMultiGetRequest> requests;
    requests.reserve(num_requests);
    for (size_t i = 0; i < num_requests; ++i) {
      requests.push_back(context->GetRequest(i));
    }
    std::vector<folly::coro::Task<Status>> tasks;
    tasks.reserve(num_requests);
    for (ExternalTableMultiGetRequest& request : requests) {
      tasks.emplace_back(GetCoroutine(read_options, request.lookup_key,
                                      prefix_extractor, request.get_context));
    }
    std::vector<Status> collected_statuses = co_await folly::coro::co_nothrow(
        folly::coro::collectAllRange(std::move(tasks)));
    statuses->resize(collected_statuses.size());
    for (size_t i = 0; i < collected_statuses.size(); ++i) {
      (*statuses)[i] = std::move(collected_statuses[i]);
    }
  }
};

using CoroExternalTableReader =
    CoroExternalTableReaderBase<ExternalTableMode::kOnlyZeroSeqnoAndPuts>;
using CoroFullExternalTableReader =
    CoroExternalTableReaderBase<ExternalTableMode::kFull>;

}  // namespace ROCKSDB_NAMESPACE
