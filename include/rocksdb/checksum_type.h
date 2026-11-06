//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Types of checksums to use for checking integrity of logical blocks within
// files. All checksums currently use 32 bits of checking power (1 in 4B
// chance of failing to detect random corruption). Traditionally, the actual
// checking power can be far from ideal if the corruption is due to misplaced
// data (e.g. physical blocks out of order in a file, or from another file),
// which is fixed in format_version=6 (see below).
enum ChecksumType : char {
  kNoChecksum = 0x0,
  kCRC32c = 0x1,
  kxxHash = 0x2,
  kxxHash64 = 0x3,
  kXXH3 = 0x4,  // Supported since RocksDB 6.27
};

}  // namespace ROCKSDB_NAMESPACE
