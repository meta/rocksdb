//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.txt for more detail.

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

enum RecordType : uint8_t {
  // Zero is reserved for preallocated files
  kZeroType = 0,
  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,

  // For recycled log files
  kRecyclableFullType = 5,
  kRecyclableFirstType = 6,
  kRecyclableMiddleType = 7,
  kRecyclableLastType = 8,

  // Compression Type
  kSetCompressionType = 9,

  // Recyclability is not encoded in the type value (in particular, not in the
  // low bit); recyclable and non-recyclable types are not laid out uniformly.
  // Use IsRecyclableRecordType to test for it.
  // User-defined timestamp sizes
  kUserDefinedTimestampSizeType = 10,
  kRecyclableUserDefinedTimestampSizeType = 11,

  // Marks a WAL file as carrying per-record ordering numbers (wal_index / LSN).
  // Kept < 128 and without the safe-ignore bit so that older readers treat it
  // as an unknown record type and report corruption instead of silently
  // misinterpreting the file.
  //
  // The marker carries no payload and no reader acts on its contents. Its
  // value is positional: it is the first record in the file, so an older
  // binary fails on record #1 rather than partway through replay, and the
  // failure names the file rather than an arbitrary offset within it.
  kWALIndexMarkerType = 12,
  kRecyclableWALIndexMarkerType = 13,

  // Data records carrying a leading wal_index. Each physical fragment is
  // self-describing so the marker is not required for correct decoding.
  kWALIndexFullType = 14,
  kWALIndexFirstType = 15,
  kWALIndexMiddleType = 16,
  kWALIndexLastType = 17,
  kRecyclableWALIndexFullType = 18,
  kRecyclableWALIndexFirstType = 19,
  kRecyclableWALIndexMiddleType = 20,
  kRecyclableWALIndexLastType = 21,

  // Declares that a closed range of wal_index values is not part of the live
  // WAL history and must not be treated as a missing record. A producer may
  // use it for indices that never carried data or for records it deliberately
  // discarded. Consumes no wal_index.
  //
  // Unlike the marker these are not positionally constrained: a cover is
  // written after the append it stands in for failed, so it can appear
  // anywhere. They get their own types rather than a marker payload because
  // the reader enforces the marker's "first record" rule, which a cover would
  // violate by construction.
  kWALIndexVoidType = 22,
  kRecyclableWALIndexVoidType = 23,

  // For WAL verification
  kPredecessorWALInfoType = 130,
  kRecyclePredecessorWALInfoType = 131,
};
// Unknown type of value with the 8-th bit set will be ignored
constexpr uint8_t kRecordTypeSafeIgnoreMask = 1 << 7;
constexpr uint8_t kMaxRecordType = kRecyclePredecessorWALInfoType;

// The whole downgrade-safety story rests on the WAL index types staying below
// the safe-ignore bit: an older binary must reject an indexed record, not
// discard it and report success. Renumbering one of them into the ignorable
// range would turn silent data loss into the expected behaviour.
// A skipped void record is worse than a skipped marker: the reader would take
// the covered range for data that is simply missing.
static_assert(kWALIndexMarkerType < kRecordTypeSafeIgnoreMask &&
                  kRecyclableWALIndexMarkerType < kRecordTypeSafeIgnoreMask &&
                  kWALIndexFullType < kRecordTypeSafeIgnoreMask &&
                  kWALIndexFirstType < kRecordTypeSafeIgnoreMask &&
                  kWALIndexMiddleType < kRecordTypeSafeIgnoreMask &&
                  kWALIndexLastType < kRecordTypeSafeIgnoreMask &&
                  kRecyclableWALIndexFullType < kRecordTypeSafeIgnoreMask &&
                  kRecyclableWALIndexFirstType < kRecordTypeSafeIgnoreMask &&
                  kRecyclableWALIndexMiddleType < kRecordTypeSafeIgnoreMask &&
                  kRecyclableWALIndexLastType < kRecordTypeSafeIgnoreMask &&
                  kWALIndexVoidType < kRecordTypeSafeIgnoreMask &&
                  kRecyclableWALIndexVoidType < kRecordTypeSafeIgnoreMask,
              "WAL index record types must fail closed on older readers");

inline constexpr bool IsWALIndexRecordType(uint8_t type) {
  return (type >= kWALIndexFullType && type <= kWALIndexLastType) ||
         (type >= kRecyclableWALIndexFullType &&
          type <= kRecyclableWALIndexLastType);
}

// Metadata records that carry wal_index bookkeeping rather than user data.
// They are never returned as logical records and never consume an index.
inline constexpr bool IsWALIndexMetadataRecordType(uint8_t type) {
  return type == kWALIndexMarkerType || type == kRecyclableWALIndexMarkerType ||
         type == kWALIndexVoidType || type == kRecyclableWALIndexVoidType;
}

inline constexpr bool IsRecyclableRecordType(uint8_t type) {
  return (type >= kRecyclableFullType && type <= kRecyclableLastType) ||
         (type >= kRecyclableWALIndexFullType &&
          type <= kRecyclableWALIndexLastType) ||
         type == kRecyclableUserDefinedTimestampSizeType ||
         type == kRecyclableWALIndexMarkerType ||
         type == kRecyclableWALIndexVoidType ||
         type == kRecyclePredecessorWALInfoType;
}

constexpr unsigned int kBlockSize = 32768;

// Number of bytes of the fixed64 wal_index prefixed to each logical record when
// WAL index is enabled.
constexpr uint32_t kWALIndexSize = 8;

// A void record's payload is the covered range as fixed64 lo followed by
// fixed64 hi, both inclusive.
constexpr uint32_t kWALIndexVoidPayloadSize = 2 * kWALIndexSize;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte)
constexpr int kHeaderSize = 4 + 2 + 1;

// Recyclable header is checksum (4 bytes), length (2 bytes), type (1 byte),
// log number (4 bytes).
constexpr int kRecyclableHeaderSize = 4 + 2 + 1 + 4;

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
