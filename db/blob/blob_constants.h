//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Version shared by the file-local metadata that lets feature-aware RepairDB
// reconstruct blob indirection without a surviving MANIFEST. This version is
// negotiated by CompactionService and is deliberately separate from the
// normal read-format versions.
inline constexpr uint32_t kBlobRepairMetadataFormatVersion = 1;
inline constexpr size_t kBlobGcMaxSelectedFiles = 8;

// WARNING: This value is == kCurrentFileBlobIndexFileNumber.
// Use this name only where file number zero means "no valid blob file" or
// "current file" is not understood/supported.
constexpr uint64_t kInvalidBlobFileNumber = 0;

// WARNING: This value is == kInvalidBlobFileNumber.
// Use this name only for BlobIndex references to the same physical file as what
// is currently being read; generic blob-file metadata must treat zero as
// invalid. (Using a distinct value like 1 was found to be more problematic,
// e.g. because of legacy "stackable" blob implementation.)
//
// This "zero is invalid unless you are the embedded reader/writer" contract is
// enforced by integrity checks that reject file number zero on generic paths;
// see FileMetaData::UpdateBoundaries (write/output path) and Version::GetBlob /
// Version::MultiGetBlob (read path). Same-file references must be resolved (by
// EmbeddedBlobResolvingIterator) before they reach those paths. Do not weaken
// those checks -- they catch leaks/corruption closer to the root cause.
constexpr uint64_t kCurrentFileBlobIndexFileNumber = kInvalidBlobFileNumber;
static_assert(kCurrentFileBlobIndexFileNumber == kInvalidBlobFileNumber);

// Sentinel range length meaning "the whole blob value" for the unified
// blob-read primitives, which take a range descriptor {range_offset,
// range_length}; range_length == kWholeBlobLength selects the whole value and
// any other value selects the strict sub-range
// [range_offset, range_offset + range_length).
//
// This is the internal counterpart of the public kLazyWholeColumn
// (include/rocksdb/lazy_wide_columns.h); both are the maximum size_t. The lazy
// resolver does not forward the public value as a range descriptor -- it
// normalizes a whole-column request to kWholeBlobLength -- so the two are not
// required to be equal, but both rely on the sentinel exceeding any real
// blob/column size so a "whole" request is never mistaken for a strict
// sub-range. Keep them in sync.
inline constexpr size_t kWholeBlobLength = std::numeric_limits<size_t>::max();

// How aggressively a blob read verifies the record's checksum. Derived once at
// the lazy read-path boundary (ReadPathBlobResolver) from
// (ReadOptions::verify_checksums, LazyColumnReadRequest::force_verify) and
// threaded down through the blob-read primitives, replacing the earlier mix of
// a verify_checksums bool and a separate force-verify method. (Not literally a
// constant, but kept here as a small shared blob-read enum rather than its own
// header.)
//
//  - kVerifyIfPresent: verify the whole-record checksum whenever the format
//    provides one, even if ReadOptions::verify_checksums is off. A byte-range
//    request under this policy is escalated to a whole-record read (a strict
//    sub-range cannot cover the checksum). This is the public force_verify.
//  - kVerifyIfNoAmplification: verify when the checksummable unit is already
//    being read -- a whole-value read verifies (the check is "free"), while a
//    strict sub-range read skips verification rather than amplify the read to
//    the whole record. Corresponds to verify_checksums on, force_verify off.
//  - kSkip: never verify. Corresponds to verify_checksums off, force_verify
//    off.
enum class BlobVerifyPolicy : uint8_t {
  kVerifyIfPresent,
  kVerifyIfNoAmplification,
  kSkip,
};

}  // namespace ROCKSDB_NAMESPACE
