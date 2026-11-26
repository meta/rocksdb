//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

#include "cache/cache_key.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct ReadOptions;
struct WriteOptions;
class RandomAccessFileReader;
class WritableFileWriter;
class Slice;
enum ChecksumType : char;

// "SimpleGen2Blob" is the second-generation on-disk format for a blob payload
// referenced by a same-file / context-relative blob index. A record is just the
// payload bytes followed by a small block-style trailer:
//
//   <payload bytes> <1-byte compression marker> <4-byte checksum>
//
// The checksum is a builtin block checksum over (payload + compression marker),
// context-modified by the record's absolute file offset (see
// ChecksumModifierForContext / ComputeBuiltinChecksumWithLastByte in
// table/format.h) so that an identical payload at a different offset gets a
// different stored checksum. The payload holds the stored bytes as given by the
// writer: raw value bytes, or value bytes compressed with the type in the
// compression marker. The read functions in this file return the stored bytes.
// The caller decompresses them when the marker is not kNoCompression.
//
// This format is deliberately decoupled from any particular file type: the same
// record shape is read out of SST files (embedded blobs) and is intended to be
// reused by other blob-bearing files. The only file-type-specific inputs are
// the byte source (a RandomAccessFileReader) and the checksum context (checksum
// type + base context checksum), both passed in by the caller.
inline constexpr size_t kSimpleGen2BlobTrailerSize = 5;
// Largest payload size a SimpleGen2Blob record may have. It is UINT32_MAX, the
// 32-bit value size limit of the table format, or less when size_t cannot hold
// the payload plus the trailer. Writers and readers reject larger sizes.
inline constexpr uint64_t kMaxSimpleGen2BlobPayloadSize =
    std::numeric_limits<size_t>::max() - kSimpleGen2BlobTrailerSize <
            std::numeric_limits<uint32_t>::max()
        ? std::numeric_limits<size_t>::max() - kSimpleGen2BlobTrailerSize
        : std::numeric_limits<uint32_t>::max();

// Cache key for the SimpleGen2Blob record at byte `record_offset` within a file
// whose base cache key is `base_cache_key`. Delegates to the shared
// OffsetableCacheKey::WithOffsetForMinSizeRecord scheme -- the exact same
// scheme block-based SST data blocks use via BlockBasedTable::GetCacheKey --
// which is valid because a SimpleGen2Blob record always carries a >= 5-byte
// trailer. As a result the cache key is collision-free with the file's data
// blocks even when the blob cache and block cache are the same cache, with no
// separately maintained algorithm to drift out of sync.
inline CacheKey GetSimpleGen2BlobCacheKey(
    const OffsetableCacheKey& base_cache_key, uint64_t record_offset) {
  return base_cache_key.WithOffsetForMinSizeRecord(record_offset);
}

// Reads a SimpleGen2Blob record of `record_size` bytes (which must equal
// `payload_size + kSimpleGen2BlobTrailerSize`) from `file` at `record_offset`
// into `buf` (capacity >= record_size) and verifies its compression marker and,
// when read_options.verify_checksums is set, its context-modified checksum.
//
// `checksum_type` and `base_context_checksum` are the file's checksum context
// (e.g. from the SST footer). `expected_compression` is the compression type
// the caller expects the record to carry (from the blob index); it must match
// the record's marker.
//
// On success, buf[0, payload_size) holds the verified stored payload. The
// caller decompresses it when the marker is not kNoCompression.
Status ReadAndVerifySimpleGen2BlobRecord(
    const ReadOptions& read_options, RandomAccessFileReader* file,
    uint64_t record_offset, size_t payload_size, size_t record_size,
    ChecksumType checksum_type, uint32_t base_context_checksum,
    CompressionType expected_compression, char* buf);

// Reads a byte sub-range [range_offset, range_offset + range_length) of an
// *uncompressed* SimpleGen2Blob payload directly into `buf` (capacity >=
// range_length), reading only those bytes. Unlike
// ReadAndVerifySimpleGen2BlobRecord it does not read the trailer and does not
// verify the record's checksum (a strict sub-range cannot cover it); callers
// that require verification read the whole record instead. `payload_size` is
// the full payload size (from the blob index); the caller must ensure
// range_offset + range_length <= payload_size. `expected_compression` must be
// kNoCompression (a strict sub-range of a compressed payload cannot be
// decompressed in isolation).
//
// On success, buf[0, range_length) holds the requested payload bytes.
Status ReadSimpleGen2BlobRange(const ReadOptions& read_options,
                               RandomAccessFileReader* file,
                               uint64_t record_offset, size_t payload_size,
                               uint64_t range_offset, size_t range_length,
                               CompressionType expected_compression, char* buf);

// Optional outputs of one batched SimpleGen2 record read. The caller owns one
// per request and points the request's `extras` at it; the reader only writes
// it.
struct SimpleGen2ReadExtras {
  // Set to the CRC32C of the payload when the record checksum was checked with
  // kCRC32c and passed. It is computed in the same pass as the check, so the
  // bytes are not read a second time. For an uncompressed payload this is the
  // CRC32C of the value. Left unset otherwise.
  std::optional<uint32_t> payload_crc32c;
};

// One whole-record request in a batched SimpleGen2Blob read (see
// ReadAndVerifySimpleGen2BlobRecords). Mirrors the scalar
// ReadAndVerifySimpleGen2BlobRecord inputs, with the read bytes going into
// caller-provided `buf` (capacity >= record_size) and the per-record outcome in
// `*status`.
struct SimpleGen2RecordReadRequest {
  uint64_t record_offset = 0;
  size_t payload_size = 0;
  size_t record_size = 0;  // == payload_size + kSimpleGen2BlobTrailerSize
  CompressionType expected_compression = kNoCompression;
  char* buf = nullptr;
  Status* status = nullptr;
  // Optional outputs; may be null.
  SimpleGen2ReadExtras* extras = nullptr;
};

// One sub-range request in a batched SimpleGen2Blob range read (see
// ReadSimpleGen2BlobRanges). Mirrors the scalar ReadSimpleGen2BlobRange inputs.
struct SimpleGen2RangeReadRequest {
  uint64_t record_offset = 0;
  size_t payload_size = 0;
  uint64_t range_offset = 0;
  size_t range_length = 0;
  CompressionType expected_compression = kNoCompression;
  char* buf = nullptr;
  Status* status = nullptr;
};

// Batch counterpart of ReadAndVerifySimpleGen2BlobRecord: reads `num_records`
// whole records from `file` into reqs[i].buf. The records form one batch. For
// example, LazyWideColumnsBatch::MultiResolve groups its embedded reads by SST,
// and the table reader of each SST makes one batch of whole records and one
// batch of byte ranges (see ReadSimpleGen2BlobRanges). A batch is read with one
// MultiRead call, or with one plain Read when the batch needs a single file
// read. The read is made when preparing the I/O options succeeds.
// FSRandomAccessFile::MultiRead requires requests that do not overlap, so:
// - Requests that overlap, including exact duplicates (same offset and size),
//   share one file read. Each of them gets the status of that read and, on
//   success, a copy of its own bytes. Callers count read statistics per
//   request, so a request that shares a read still counts as a read.
// - Then each request that read OK is checked on its own: first its compression
//   marker, then, when read_options.verify_checksums is set, its
//   context-modified checksum.
// Per-record outcome is written to *reqs[i].status; on success reqs[i].buf[0,
// payload_size) holds the verified payload (the trailer sits unused at the
// tail). Requests may be given in any order. The comment above MultiReadGen2 in
// blob_gen2_format.cc describes the merge, the buffers and the error rules.
void ReadAndVerifySimpleGen2BlobRecords(const ReadOptions& read_options,
                                        RandomAccessFileReader* file,
                                        ChecksumType checksum_type,
                                        uint32_t base_context_checksum,
                                        size_t num_records,
                                        SimpleGen2RecordReadRequest* reqs);

// Batch counterpart of ReadSimpleGen2BlobRange: reads the requested sub-range
// of `num_records` *uncompressed* payloads from `file`, with no trailer read
// and no checksum check. The requests form one batch of byte ranges, as
// described at ReadAndVerifySimpleGen2BlobRecords. A valid empty range gets OK
// without I/O. The batch is read with one MultiRead call, or with one plain
// Read, as in ReadAndVerifySimpleGen2BlobRecords. The read is made only when at
// least one range needs bytes and preparing the I/O options succeeds. The
// merging of overlapping requests works as in
// ReadAndVerifySimpleGen2BlobRecords, and each request gets a copy of its own
// bytes. Per-record outcome is in *reqs[i].status; on success reqs[i].buf[0,
// range_length) holds the requested bytes. Requests may be given in any order.
void ReadSimpleGen2BlobRanges(const ReadOptions& read_options,
                              RandomAccessFileReader* file, size_t num_records,
                              SimpleGen2RangeReadRequest* reqs);

// Writes a SimpleGen2Blob record for `payload` at the current end of `file`,
// which the caller asserts is byte offset `record_offset`. Appends the payload
// bytes followed by the 5-byte trailer (compression marker + context-modified
// builtin checksum), mirroring ReadAndVerifySimpleGen2BlobRecord so the on-disk
// record format lives in one module.
//
// `checksum_type` and `base_context_checksum` are the file's checksum context
// (e.g. from the SST footer). `payload` is already in stored form, and
// `compression` is the type it was compressed with (kNoCompression for raw
// bytes). This function writes it as is and never compresses it.
IOStatus WriteSimpleGen2BlobRecord(WritableFileWriter* file,
                                   const WriteOptions& write_options,
                                   ChecksumType checksum_type,
                                   uint32_t base_context_checksum,
                                   uint64_t record_offset, const Slice& payload,
                                   CompressionType compression);

}  // namespace ROCKSDB_NAMESPACE
