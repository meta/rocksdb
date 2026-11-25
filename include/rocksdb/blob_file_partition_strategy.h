//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

// Implementations may keep internal state, but SelectPartition() runs on the
// write hot path and may be called concurrently from multiple writer threads,
// so any internal mutation must be synchronized. Implementations should avoid
// I/O, callbacks into RocksDB APIs, and other blocking or expensive work.
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe.
class BlobFilePartitionStrategy {
 public:
  virtual ~BlobFilePartitionStrategy() = default;

  // Returns a name that identifies this strategy for debugging and logging.
  virtual const char* Name() const = 0;

  // Select a partition for the given blob direct write.
  //
  // `value` is the original uncompressed user value, even when blob
  // compression is enabled for the target blob file. The return value can be
  // any uint32_t; the caller applies modulo num_partitions internally.
  virtual uint32_t SelectPartition(uint32_t num_partitions,
                                   uint32_t column_family_id, const Slice& key,
                                   const Slice& value) = 0;

  // Select a partition for the given wide-column blob direct write. This is
  // called once per PutEntity() before writing any blob-backed columns from the
  // entity, and the chosen partition is reused for all blob-backed columns in
  // that entity. The default behavior delegates to the plain-value overload
  // using the default column value when present, otherwise the first column
  // value, or an empty Slice for an empty entity.
  //
  // Derived classes that override only the Slice-based overload should add
  // `using BlobFilePartitionStrategy::SelectPartition;` so this overload
  // remains visible.
  virtual uint32_t SelectPartition(uint32_t num_partitions,
                                   uint32_t column_family_id, const Slice& key,
                                   const WideColumns& columns) {
    const Slice* value = nullptr;
    for (const auto& column : columns) {
      if (column.name() == kDefaultWideColumnName) {
        value = &column.value();
        break;
      }
    }
    if (value == nullptr && !columns.empty()) {
      value = &columns.front().value();
    }
    return SelectPartition(num_partitions, column_family_id, key,
                           value != nullptr ? *value : Slice());
  }
};

}  // namespace ROCKSDB_NAMESPACE
