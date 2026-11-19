//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Sources observed while attempting a read, not proof of successful completion.
enum class ReadSource : uint8_t {
  kUnknown = 0,
  kCache = 1,
  kRemoteStorage = 2,
  kMixed = 3,
};

}  // namespace ROCKSDB_NAMESPACE
