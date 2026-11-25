// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <string>

#include "folly/coro/Task.h"
#include "rocksdb/attribute_groups.h"
#include "rocksdb/coro_db.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/utilities/stackable_db.h"

namespace ROCKSDB_NAMESPACE {

// Using this interface requires Folly to be available and RocksDB to be built
// with USE_COROUTINES=1.
template <typename Base>
class CoroStackableDBBase : public Base, public CoroDB {
 public:
  using Base::Base;
  CoroDB* GetCoroDB() override {
    return this->db_->GetCoroDB() == nullptr ? nullptr : this;
  }

 protected:
  folly::coro::Task<Status> GetCoroutine(const ReadOptions& options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key, PinnableSlice* value,
                                         std::string* timestamp) override {
    CoroDB* coro_db = this->db_->GetCoroDB();
    assert(coro_db != nullptr);
    return coro_db->GetCoroutine(options, column_family, key, value, timestamp);
  }

  folly::coro::Task<void> MultiGetCoroutine(
      const ReadOptions& options, size_t num_keys,
      ColumnFamilyHandle** column_families, const Slice* keys,
      PinnableSlice* values, std::string* timestamps, Status* statuses,
      bool sorted_input) override {
    CoroDB* coro_db = this->db_->GetCoroDB();
    assert(coro_db != nullptr);
    return coro_db->MultiGetCoroutine(options, num_keys, column_families, keys,
                                      values, timestamps, statuses,
                                      sorted_input);
  }

  folly::coro::Task<Status> GetEntityCoroutine(
      const ReadOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, PinnableWideColumns* columns) override {
    CoroDB* coro_db = this->db_->GetCoroDB();
    assert(coro_db != nullptr);
    return coro_db->GetEntityCoroutine(options, column_family, key, columns);
  }

  folly::coro::Task<Status> GetEntityCoroutine(
      const ReadOptions& options, const Slice& key,
      PinnableAttributeGroups* result) override {
    CoroDB* coro_db = this->db_->GetCoroDB();
    assert(coro_db != nullptr);
    return coro_db->GetEntityCoroutine(options, key, result);
  }
};

using CoroStackableDB = CoroStackableDBBase<StackableDB>;

}  // namespace ROCKSDB_NAMESPACE
