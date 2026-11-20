//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>

#include "rocksdb/unique_id.h"

namespace ROCKSDB_NAMESPACE {

// Standard size unique ID, good enough for almost all practical purposes
using UniqueId64x2 = std::array<uint64_t, 2>;

// Value never used as an actual unique ID so can be used for "null"
constexpr UniqueId64x2 kNullUniqueId64x2 = {};

// Extended size unique ID, for extra certainty of uniqueness among SST files
// spanning many hosts over a long time (rarely if ever needed)
using UniqueId64x3 = std::array<uint64_t, 3>;

// Value never used as an actual unique ID so can be used for "null"
constexpr UniqueId64x3 kNullUniqueId64x3 = {};

// Dynamic pointer wrapper for one of the two above
struct UniqueIdPtr {
  uint64_t* ptr = nullptr;
  bool extended = false;

  // NOLINTNEXTLINE(google-explicit-constructor)
  /*implicit*/ UniqueIdPtr(UniqueId64x2* id) {
    ptr = (*id).data();
    extended = false;
  }
  // NOLINTNEXTLINE(google-explicit-constructor)
  /*implicit*/ UniqueIdPtr(UniqueId64x3* id) {
    ptr = (*id).data();
    extended = true;
  }
};

// Helper for GetUniqueIdFromTableProperties. This function can also be used
// for temporary ids for files without sufficient information in table
// properties. The internal unique id is more structured than the public
// unique id, so can be manipulated in more ways but very carefully.
// These must be long term stable to ensure GetUniqueIdFromTableProperties
// is long term stable.
Status GetSstInternalUniqueId(const std::string& db_id,
                              const std::string& db_session_id,
                              uint64_t file_number, UniqueIdPtr out,
                              bool force = false);

// Helper for GetUniqueIdFromTableProperties. External unique ids go through
// this extra hashing layer so that prefixes of the unique id have predictable
// "full" entropy. This hashing layer is 1-to-1 on the first 128 bits and on
// the full 192 bits.
// This transformation must be long term stable to ensure
// GetUniqueIdFromTableProperties is long term stable.
void InternalUniqueIdToExternal(UniqueIdPtr in_out);

// Reverse of InternalUniqueIdToExternal mostly for testing purposes
// (demonstrably 1-to-1 on the first 128 bits and on the full 192 bits).
void ExternalUniqueIdToInternal(UniqueIdPtr in_out);

// Convert numerical format to byte format for public API
std::string EncodeUniqueIdBytes(UniqueIdPtr in);

// Reverse of EncodeUniqueIdBytes.
Status DecodeUniqueIdBytes(const std::string& unique_id, UniqueIdPtr out);

// For presenting internal IDs for debugging purposes. Visually distinct from
// UniqueIdToHumanString for external IDs.
std::string InternalUniqueIdToHumanString(UniqueIdPtr in);

// Reformat a random value down to our "DB session id" format,
// which is intended to be compact and friendly for use in file names.
// `lower` is fully preserved and data is lost from `upper`.
//
// Detail: Encoded into 20 chars in base-36 ([0-9A-Z]), which is ~103 bits of
// entropy, which is enough to expect no collisions across a billion servers
// each opening DBs a million times (~2^50). Benefits vs. RFC-4122 unique id:
// * Save ~ dozen bytes per SST file
// * Shorter shared backup file names (some platforms have low limits)
// * Visually distinct from DB id format (usually RFC-4122)
std::string EncodeSessionId(uint64_t upper, uint64_t lower);

// Reverse of EncodeSessionId. Returns NotSupported on error rather than
// Corruption because non-standard session IDs should be allowed with degraded
// functionality.
Status DecodeSessionId(const std::string& db_session_id, uint64_t* upper,
                       uint64_t* lower);

}  // namespace ROCKSDB_NAMESPACE
