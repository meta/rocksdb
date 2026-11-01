//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "db/blob/blob_constants.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobIndex;
class PinnableSlice;
struct ReadOptions;

// One request for SameFileBlobReader::MultiGetSameFileBlob (the batched
// counterpart of GetSameFileBlob). See GetSameFileBlob for the per-field
// semantics (range_length == kWholeBlobLength selects the whole value).
struct SameFileBlobReadRequest {
  const BlobIndex* blob_index = nullptr;
  uint64_t range_offset = 0;
  size_t range_length = kWholeBlobLength;
  BlobVerifyPolicy verify_policy = BlobVerifyPolicy::kVerifyIfNoAmplification;
  PinnableSlice* result = nullptr;
  Status* status = nullptr;
  const BlobIndex* stable_blob_index = nullptr;
};

// One exact stored-byte read used while rebuilding a relocation file. The
// physical SimpleGen2 checksum and the stable stored-byte checksum are both
// verified. Experimental checksum v1 instead verifies the decompressed value.
// The exact compressed payload is returned when
// stored_payload is non-null; a null output requests validation only.
struct StoredSameFileBlobReadRequest {
  const BlobIndex& blob_index;
  uint32_t stable_checksum_version;
  uint32_t stable_checksum;
  std::string* stored_payload;
  Status& status;
  uint32_t stored_checksum = 0;
};

// Narrow interface for resolving a same-file ("embedded") blob reference
// against the physical file that currently holds the table entry. Implemented
// by BlockBasedTable, which owns the reader context (file, footer, blob cache)
// needed to read an embedded blob record.
//
// Kept in its own low-level header, free of any table-layer dependency, so that
// db/blob (EmbeddedAwareBlobFetcher) and table/get_context can depend on just
// this interface without pulling in BlockBasedTable. ReadPathBlobResolver (the
// lazy-resolution read path) can likewise hold a const SameFileBlobReader*.
class SameFileBlobReader {
 public:
  virtual ~SameFileBlobReader() = default;

  // Resolves a same-file ("embedded") blob reference (one for which
  // blob_index.IsSameFile() is true) into `value`, pinning the payload
  // zero-copy (from the blob cache or an owned buffer).
  //
  // range_length == kWholeBlobLength resolves the whole value. Any other length
  // resolves only the sub-range [range_offset, range_offset + range_length),
  // reading just those bytes on a cache miss -- skipping whole-record checksum
  // verification (a strict sub-range cannot cover it) and never populating the
  // blob cache; valid only for an uncompressed record and a verify_policy other
  // than kVerifyIfPresent, with range_offset + range_length <=
  // blob_index.size().
  //
  // verify_policy controls whole-record checksum verification (see
  // BlobVerifyPolicy); a whole read under kVerifyIfPresent verifies even when
  // ReadOptions::verify_checksums is off. read_tier == kBlockCacheTier without
  // a cached record yields Status::Incomplete; a bad record yields
  // Status::Corruption.
  virtual Status GetSameFileBlob(const ReadOptions& read_options,
                                 const BlobIndex& blob_index,
                                 uint64_t range_offset, size_t range_length,
                                 BlobVerifyPolicy verify_policy,
                                 PinnableSlice* value) const = 0;

  // Batched counterpart of GetSameFileBlob: resolves `num_reads` same-file blob
  // references (whole or sub-range, mixed) against this reader, writing each
  // outcome to *reqs[i].status. Implementations may coalesce the underlying
  // disk reads into a single MultiRead. The default implementation simply loops
  // over GetSameFileBlob (correct, but not coalesced); BlockBasedTable
  // overrides it to coalesce reads of embedded records in the same SST.
  virtual void MultiGetSameFileBlob(const ReadOptions& read_options,
                                    size_t num_reads,
                                    SameFileBlobReadRequest* reqs) const {
    for (size_t i = 0; i < num_reads; ++i) {
      SameFileBlobReadRequest& req = reqs[i];
      const Status s =
          GetSameFileBlob(read_options, *req.blob_index, req.range_offset,
                          req.range_length, req.verify_policy, req.result);
      if (req.status) {
        *req.status = s;
      }
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
