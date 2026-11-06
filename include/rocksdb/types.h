// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <memory>
#include <unordered_map>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// Define all public custom types here.

using ColumnFamilyId = uint32_t;

// Represents a sequence number in a WAL file.
using SequenceNumber = uint64_t;

struct TableProperties;
using TablePropertiesCollection =
    std::unordered_map<std::string, std::shared_ptr<const TableProperties>>;

const SequenceNumber kMinUnCommittedSeq = 1;  // 0 is always committed

enum class TableFileCreationReason : uint8_t {
  kFlush,
  kCompaction,
  kRecovery,
  kMisc,
  kSstFileWriter,
};

enum class BlobFileCreationReason {
  kFlush,
  kCompaction,
  kRecovery,
};

// The types of files RocksDB uses in a DB directory. (Available for
// advanced options.)
enum FileType {
  kWalFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile,  // Either the current one, or an old one
  kMetaDatabase,
  kIdentityFile,
  kOptionsFile,
  kBlobFile,
  kCompactionProgressFile
};

// User-oriented representation of internal key types.
// Ordering of this enum entries should not change.
enum EntryType {
  kEntryPut,
  kEntryDelete,
  kEntrySingleDelete,
  kEntryMerge,
  kEntryRangeDeletion,
  kEntryBlobIndex,
  kEntryDeleteWithTimestamp,
  kEntryWideColumnEntity,
  kEntryTimedPut,  // That hasn't yet converted to a standard Put entry
  kEntryOther,
};

// Structured user-oriented representation of an internal key. It includes user
// key, sequence number, and type.
// If user-defined timestamp is enabled, `timestamp` contains the user-defined
// timestamp, it's otherwise an empty Slice.
struct ParsedEntryInfo {
  Slice user_key;
  Slice timestamp;
  SequenceNumber sequence;
  EntryType type;
  bool copied_user_key;
};

enum class WriteStallCause {
  // Beginning of CF-scope write stall causes
  //
  // Always keep `kMemtableLimit` as the first stat in this section
  kMemtableLimit,
  kL0FileCountLimit,
  kPendingCompactionBytes,
  kCFScopeWriteStallCauseEnumMax,
  // End of CF-scope write stall causes

  // Beginning of DB-scope write stall causes
  //
  // Always keep `kWriteBufferManagerLimit` as the first stat in this section
  kWriteBufferManagerLimit,
  kDBScopeWriteStallCauseEnumMax,
  // End of DB-scope write stall causes

  // Always add new WriteStallCause before `kNone`
  kNone,
};

enum class WriteStallCondition {
  kDelayed,
  kStopped,
  // Always add new WriteStallCondition before `kNormal`
  kNormal,
};

// Temperature of a file. Used to pass to FileSystem for a different
// placement and/or coding.
// Reserve some numbers in the middle, in case we need to insert new tier
// there.
enum class Temperature : uint8_t {
  kUnknown = 0,
  kHot = 0x04,
  kWarm = 0x08,
  kCool = 0x0A,
  kCold = 0x0C,
  kIce = 0x10,
  // XXX: this is mis-named. It is instead an invalid temperature beyond the
  // rest
  kLastTemperature,
};

}  // namespace ROCKSDB_NAMESPACE
