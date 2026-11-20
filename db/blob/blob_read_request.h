//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// Non-owning metadata needed when opening a blob file for reading. The
// checksum slices must remain valid while this object is in use. A file without
// finalized checksum metadata, such as an active direct-write blob file, uses
// empty slices for both checksum fields.
struct BlobFileOpenInfo {
  uint64_t file_number = 0;
  Slice file_checksum;
  Slice file_checksum_func_name;
};

// A read Blob request structure for use in BlobSource::MultiGetBlob and
// BlobFileReader::MultiGetBlob.
struct BlobReadRequest {
  // User key to lookup the paired blob
  // TODO: consider simplifying to copied Slice?
  const Slice* user_key = nullptr;

  // File offset in bytes
  uint64_t offset = 0;

  // Length to read in bytes
  size_t len = 0;

  // Blob compression type
  CompressionType compression = kNoCompression;

  // Output parameter set by MultiGetBlob() to point to the data buffer, and
  // the number of valid bytes
  PinnableSlice* result = nullptr;

  // Status of read
  Status* status = nullptr;

  BlobReadRequest(const Slice& _user_key, uint64_t _offset, size_t _len,
                  CompressionType _compression, PinnableSlice* _result,
                  Status* _status)
      : user_key(&_user_key),
        offset(_offset),
        len(_len),
        compression(_compression),
        result(_result),
        status(_status) {}

  BlobReadRequest() = default;
  BlobReadRequest(const BlobReadRequest& other) = default;
  BlobReadRequest& operator=(const BlobReadRequest& other) = default;
  BlobReadRequest(BlobReadRequest&& other) = default;
  BlobReadRequest& operator=(BlobReadRequest&& other) = default;
  ~BlobReadRequest() = default;
};

using BlobFileReadRequests =
    std::tuple<BlobFileOpenInfo, uint64_t /* file_size */,
               autovector<BlobReadRequest>>;

// A byte-range (partial) blob read request for the lazy blob-read multi
// primitives (BlobFileReader::MultiGetBlobRange /
// BlobSource::MultiGetBlobRange). Unlike BlobReadRequest (whole value), it
// reads only the sub-range [range_offset, range_offset + range_length) of an
// *uncompressed* blob value, skipping whole-record checksum verification and
// blob-cache population -- the multi-read counterpart of
// BlobSource::GetBlobRange / BlobFileReader::GetBlobRange.
struct BlobRangeReadRequest {
  // User key that pairs with the blob value (used for offset validation).
  Slice user_key;

  // The blob value's file offset and full (logical == on-disk, uncompressed)
  // size, from the BlobIndex.
  uint64_t offset = 0;
  uint64_t value_size = 0;

  // The requested sub-range within the value. `range_length` is the number of
  // bytes to read; the caller must ensure range_offset + range_length <=
  // value_size (it clamps before building the request).
  uint64_t range_offset = 0;
  size_t range_length = 0;

  // Output parameter set to point to the requested bytes, and the read status.
  PinnableSlice* result = nullptr;
  Status* status = nullptr;

  BlobRangeReadRequest(Slice _user_key, uint64_t _offset, uint64_t _value_size,
                       uint64_t _range_offset, size_t _range_length,
                       PinnableSlice* _result, Status* _status)
      : user_key(_user_key),
        offset(_offset),
        value_size(_value_size),
        range_offset(_range_offset),
        range_length(_range_length),
        result(_result),
        status(_status) {}

  BlobRangeReadRequest() = default;
};

using BlobFileRangeReadRequests =
    std::tuple<BlobFileOpenInfo, uint64_t /* file_size */,
               autovector<BlobRangeReadRequest>>;

}  // namespace ROCKSDB_NAMESPACE
