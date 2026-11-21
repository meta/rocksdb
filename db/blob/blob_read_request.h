//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>
#include <optional>
#include <vector>

#include "db/blob/blob_constants.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class BlobContents;
class BlobIndex;
struct SameFileBlobStandaloneRead;

// Non-owning metadata needed when opening a blob file for reading. The
// checksum slices must remain valid while this object is in use. A file without
// finalized checksum metadata, such as an active direct-write blob file, uses
// empty slices for both checksum fields.
struct BlobFileOpenInfo {
  uint64_t file_number = 0;
  Slice file_checksum;
  Slice file_checksum_func_name;
};

// The physical file that an indirect BlobIndex resolves to: a normal blob
// file, or a relocation file with its expected identity. Non-owning, like
// `open_info`.
struct BlobPhysicalFile {
  BlobFileOpenInfo open_info;
  uint64_t file_size = 0;
  bool is_relocation = false;
  // Relocation files only: the expected table tail size (index, properties,
  // metaindex and footer; see db/blob/blob_relocation_file.h) and the expected
  // directory digest. The code that installs the file records both values.
  // BlobRelocationFileReader::Open checks that the file matches them.
  uint64_t tail_size = 0;
  uint32_t directory_digest = 0;
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

  // Optional: when the record checksum is checked and passes, receives the
  // CRC32C of the stored value bytes, computed in the same pass. For an
  // uncompressed value, this is the logical CRC of the value.
  std::optional<uint32_t>* value_crc32c = nullptr;

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

// One lookup of a value by its stable identity (origin, original offset) in a
// relocation file that the caller has already selected. The BlobIndex gives the
// origin, original offset, stored size and compression. The relocation reader
// does not check the logical CRC. BlobSource checks it after it has built the
// value, when read_options.verify_checksums is set or the value goes into the
// stable blob cache.
struct RelocationBlobReadRequest {
  // Indirect BlobIndex to look up; not owned.
  const BlobIndex* blob_index = nullptr;
  // Requested range; kWholeBlobLength selects the whole value. A range of a
  // compressed value is not supported and gets Corruption.
  uint64_t range_offset = 0;
  size_t range_length = kWholeBlobLength;
  // Per-request status; not owned.
  Status* status = nullptr;
  // Receives the value (or range) as an owned buffer in `output->contents`, and
  // the read outputs (see SameFileBlobStandaloneRead); not owned. The reader
  // sets `output->allocator` and passes `output` down to the table reader as
  // is.
  SameFileBlobStandaloneRead* output = nullptr;
};

// One read of an indirect BlobIndex (kIndirectBlob or kIndirectFlattenedBlob).
// The caller has already resolved all requests in the batch to the same
// physical file. BlobSource looks up the stable blob cache and sends the reads
// to the normal blob file reader or the relocation file reader of that file.
// It checks the logical CRC when read_options.verify_checksums is set or the
// value goes into the stable blob cache.
struct IndirectBlobReadRequest {
  // User key of the record in a normal blob file (used for its record
  // header); not used for relocation files.
  Slice user_key;
  // Indirect BlobIndex to read; not owned.
  const BlobIndex* blob_index = nullptr;
  // Requested range; kWholeBlobLength selects the whole value.
  uint64_t range_offset = 0;
  size_t range_length = kWholeBlobLength;
  // Output value and per-request status; not owned.
  PinnableSlice* result = nullptr;
  Status* status = nullptr;
  // True when the caller already looked up the stable blob cache for this
  // value and missed (see BlobSource::GetIndirectBlobFromCache()), so
  // BlobSource does not look it up again.
  bool cache_checked = false;
};

// The indirect reads of one batch that go to one physical file. It is a named
// type, like BlobFileReadRequests, and not a struct local to
// Version::MultiGetBlobLazy(): gcc 16 merged the vector code of that local
// struct with other vector code in a unity build and then reported a false
// -Wfree-nonheap-object error.
struct BlobFileIndirectReadRequests {
  BlobPhysicalFile file;
  // BlobSource takes a pointer and a count, so the requests must be
  // contiguous. autovector splits its inline and overflow storage, so
  // &requests[0] does not point to a contiguous array.
  std::vector<IndirectBlobReadRequest> requests;
};

}  // namespace ROCKSDB_NAMESPACE
