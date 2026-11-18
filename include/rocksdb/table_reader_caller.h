//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// A list of callers for a table reader. It is used to trace the caller that
// accesses on a block. This is only used for block cache tracing and analysis.
// A user may use kUncategorized if the caller is not interesting for analysis
// or the table reader is called in the test environment, e.g., unit test, table
// reader benchmark, etc.
enum TableReaderCaller : char {
  kUserGet = 1,
  kUserMultiGet = 2,
  kUserIterator = 3,
  kUserApproximateSize = 4,
  kUserVerifyChecksum = 5,
  kSSTDumpTool = 6,
  kExternalSSTIngestion = 7,
  kRepair = 8,
  kPrefetch = 9,
  kCompaction = 10,
  // A compaction job may refill the block cache with blocks in the new SST
  // files if paranoid_file_checks is true.
  kCompactionRefill = 11,
  // After building a table, it may load all its blocks into the block cache if
  // paranoid_file_checks is true.
  kFlush = 12,
  // sst_file_reader.
  kSSTFileReader = 13,
  // A list of callers that are either not interesting for analysis or are
  // calling from a test environment, e.g., unit test, benchmark, etc.
  kUncategorized = 14,
  // Reads of a blob relocation file (blob indirection): its location blocks
  // and raw records. The relocation reader checks the raw values itself.
  kBlobRelocationFileRead = 15,
  // All callers should be added before kMaxBlockCacheLookupCaller.
  kMaxBlockCacheLookupCaller
};
}  // namespace ROCKSDB_NAMESPACE
