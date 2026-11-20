//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Size in bytes of an internal key's sequence-number and value-type footer.
constexpr size_t kInternalKeyFooterSize = 8;

// Given a user key, creates the internal key used for `Seek` operation for a
// raw table iterator. The internal key is stored in `buf`.
// `comparator` should be the same as the `Options.comparator` used to create
// the column family or the `SstFileWriter`.
Status GetInternalKeyForSeek(const Slice& user_key,
                             const Comparator* comparator, std::string* buf);

// Given a user key, creates the internal key used for `SeekForPrev` operation
// for a raw table iterator. The internal key is stored in `buf`.
// `comparator`: see doc for `GetInternalKeyForSeek`.
Status GetInternalKeyForSeekForPrev(const Slice& user_key,
                                    const Comparator* comparator,
                                    std::string* buf);

// Util method that takes an internal key and parse it to get `ParsedEntryInfo`.
// Such an internal key usually comes from a table iterator.
// `comparator`: see doc for `GetInternalKeyForSeek`.
Status ParseEntry(const Slice& internal_key, const Comparator* comparator,
                  ParsedEntryInfo* parsed_entry);

// Returns the buffer size required to encode entry.
inline size_t GetEncodedEntrySize(const ParsedEntryInfo& entry) {
  return entry.user_key.size() + entry.timestamp.size() +
         kInternalKeyFooterSize;
}

// Encodes entry into internal_key, replacing its contents on success.
// entry.timestamp is appended verbatim to the user key and must match the
// column family's timestamp size. Input slices must not refer to internal_key.
// Returns InvalidArgument for unsupported entry types or a sequence above the
// 56-bit limit; internal_key is unchanged on error.
Status EncodeEntry(const ParsedEntryInfo& entry, std::string* internal_key);

// Encodes entry into a caller-owned buffer without allocating on success.
// internal_key must have at least GetEncodedEntrySize(entry) writable bytes.
// Writes exactly that many bytes, leaving remaining space unchanged.
// Input slices must not overlap the written region.
// Timestamp handling and errors are as above.
Status EncodeEntry(const ParsedEntryInfo& entry, char* internal_key);

// Packs seq and the internal value type corresponding to type into *packed.
// The sequence occupies the upper 56 bits and the value type the lower 8 bits.
// Returns InvalidArgument for unsupported entry types or a sequence above the
// 56-bit limit; *packed is unchanged on error.
Status PackSequenceAndType(uint64_t seq, EntryType type, uint64_t* packed);

// Extracts the sequence number into *seq and the public entry type into *type.
// Internal value types without a public equivalent are returned as kEntryOther.
void UnPackSequenceAndType(uint64_t packed, uint64_t* seq, EntryType* type);

}  // namespace ROCKSDB_NAMESPACE
