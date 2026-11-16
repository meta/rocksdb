//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_gen2_format.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <string>
#include <vector>

#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/format.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Verifies a SimpleGen2Blob record's builtin checksum: the stored 4-byte
// checksum (at record[payload_size + 1 ..], context-modified by the record's
// file offset) against the checksum computed over the payload plus its 1-byte
// compression marker. Returns OK, or a Corruption carrying debugging
// information -- stored (with the per-offset context modifier removed),
// computed, and checksum type, with CRC values unmasked so they can be compared
// against a reference. Mirrors VerifyBlockChecksum() for the block-based
// format, since a SimpleGen2Blob record's trailer uses the same
// builtin-checksum scheme; shared by the scalar and batched readers so both
// report the same diagnostics.
Status VerifySimpleGen2BlobChecksum(ChecksumType checksum_type,
                                    uint32_t base_context_checksum,
                                    const char* record, size_t payload_size,
                                    uint64_t record_offset,
                                    const std::string& file_name) {
  uint32_t stored = DecodeFixed32(record + payload_size + 1);
  const uint32_t modifier =
      ChecksumModifierForContext(base_context_checksum, record_offset);
  stored -= modifier;
  const uint32_t computed = ComputeBuiltinChecksumWithLastByte(
      checksum_type, record, payload_size, record[payload_size]);
  if (stored == computed) {
    return Status::OK();
  }
  // Unmask CRC values (as VerifyBlockChecksum does) so a reader of the error
  // can compare against a reference checksum.
  uint32_t stored_for_msg = stored;
  uint32_t computed_for_msg = computed;
  if (checksum_type == kCRC32c) {
    stored_for_msg = crc32c::Unmask(stored);
    computed_for_msg = crc32c::Unmask(computed);
  }
  return Status::Corruption("SimpleGen2 blob record checksum mismatch: stored" +
                            std::string(modifier ? "(context removed)" : "") +
                            " = " + std::to_string(stored_for_msg) +
                            ", computed = " + std::to_string(computed_for_msg) +
                            ", type = " + std::to_string(checksum_type) +
                            " in " + file_name + " offset " +
                            std::to_string(record_offset) + " size " +
                            std::to_string(payload_size));
}

}  // namespace

Status ReadAndVerifySimpleGen2BlobRecord(
    const ReadOptions& read_options, RandomAccessFileReader* file,
    uint64_t record_offset, size_t payload_size, size_t record_size,
    ChecksumType checksum_type, uint32_t base_context_checksum,
    CompressionType expected_compression, char* buf) {
  assert(file != nullptr);
  assert(buf != nullptr);
  assert(record_size == payload_size + kSimpleGen2BlobTrailerSize);

  Slice result;
  IOOptions opts;
  IODebugContext dbg;
  Status s = file->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    s = file->Read(record_offset, record_size, opts, &result, buf, &dbg);
  }
  if (!s.ok()) {
    return s;
  }
  if (result.size() != record_size) {
    return Status::Corruption("Incomplete blob record read from " +
                              file->file_name() + " at offset " +
                              std::to_string(record_offset) + ": expected " +
                              std::to_string(record_size) + " bytes, got " +
                              std::to_string(result.size()));
  }
  // With mmap reads the data lands outside `buf`; copy it in so the caller can
  // rely on `buf` owning the bytes (this is the only copy on the mmap path).
  // TODO: fix this extra memcpy in the mmap case
  if (result.data() != buf) {
    memcpy(buf, result.data(), record_size);
  }

  const char* record = buf;
  const CompressionType compression =
      static_cast<CompressionType>(record[payload_size]);
  if (compression != expected_compression) {
    return Status::Corruption(
        "Blob record compression mismatch in " + file->file_name() +
        " at offset " + std::to_string(record_offset) + ": index expects " +
        std::to_string(static_cast<int>(expected_compression)) +
        ", record has " + std::to_string(static_cast<int>(compression)));
  }

  if (read_options.verify_checksums) {
    const Status checksum_status = VerifySimpleGen2BlobChecksum(
        checksum_type, base_context_checksum, record, payload_size,
        record_offset, file->file_name());
    if (!checksum_status.ok()) {
      return checksum_status;
    }
  }

  // The compression marker matched the blob index above, but SimpleGen2Blob
  // payloads are currently written uncompressed only (see
  // WriteSimpleGen2BlobRecord), so a record that actually carries a compression
  // type is not readable here. This guards a corrupt or forward-incompatible
  // record whose (compressed) marker happened to agree with a (compressed)
  // index.
  if (compression != kNoCompression) {
    return Status::Corruption("Blob record compression " +
                              std::to_string(static_cast<int>(compression)) +
                              " is not supported in " + file->file_name() +
                              " at offset " + std::to_string(record_offset));
  }
  return Status::OK();
}

Status ReadSimpleGen2BlobRange(const ReadOptions& read_options,
                               RandomAccessFileReader* file,
                               uint64_t record_offset, size_t payload_size,
                               uint64_t range_offset, size_t range_length,
                               CompressionType expected_compression,
                               char* buf) {
  assert(file != nullptr);
  assert(buf != nullptr);

  // A strict sub-range of a compressed payload cannot be decompressed in
  // isolation; callers resolve such payloads whole and slice instead.
  if (expected_compression != kNoCompression) {
    return Status::Corruption(
        "Cannot range-read a compressed blob (compression " +
        std::to_string(static_cast<int>(expected_compression)) + ") in " +
        file->file_name() + " at offset " + std::to_string(record_offset));
  }

  // The requested sub-range must lie within the payload (the trailer that
  // follows the payload is never part of a range read).
  if (range_offset > payload_size ||
      range_length > payload_size - range_offset) {
    return Status::InvalidArgument(
        "Blob range [" + std::to_string(range_offset) + ", +" +
        std::to_string(range_length) + ") out of bounds for payload size " +
        std::to_string(payload_size) + " in " + file->file_name() +
        " at offset " + std::to_string(record_offset));
  }

  Slice result;
  IOOptions opts;
  IODebugContext dbg;
  Status s = file->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    s = file->Read(record_offset + range_offset, range_length, opts, &result,
                   buf, &dbg);
  }
  if (!s.ok()) {
    return s;
  }
  if (result.size() != range_length) {
    return Status::Corruption("Incomplete blob range read from " +
                              file->file_name() + " at offset " +
                              std::to_string(record_offset + range_offset) +
                              ": expected " + std::to_string(range_length) +
                              " bytes, got " + std::to_string(result.size()));
  }
  // With mmap reads the data lands outside `buf`; copy it in so the caller can
  // rely on `buf` owning the bytes (the only copy on the mmap path).
  if (result.data() != buf) {
    memcpy(buf, result.data(), range_length);
  }

  // No trailer read and no checksum verification: a strict sub-range cannot
  // cover the record's checksum (callers that require it read the whole
  // record).
  return Status::OK();
}

namespace {

// One record's read description for the batched SimpleGen2Blob readers below:
// where/how much to read, where to put it, and where to report the outcome.
struct Gen2ReadSlot {
  uint64_t offset = 0;
  size_t len = 0;
  char* buf = nullptr;
  Status* status = nullptr;
};

// Issues a single MultiRead for the records described by `slots` and copies
// each result into its buf, setting *slots[i].status. Shared by the
// whole-record and range batch readers; per-record format checks (compression
// marker / checksum) are done by the whole-record caller afterwards from the
// (now populated) buf.
void MultiReadGen2(const ReadOptions& read_options,
                   RandomAccessFileReader* file,
                   std::vector<Gen2ReadSlot>& slots) {
  assert(file != nullptr);
  assert(!slots.empty());

  // RandomAccessFileReader::MultiRead requires the requests sorted ascending by
  // file offset -- it asserts this, and in direct-I/O mode it aligns and merges
  // only consecutive requests, so unsorted input reads the wrong bytes. The
  // callers group records by SST but not by offset, so sort here. Each slot
  // carries its own output buf/status pointer, so sorting does not disturb
  // result routing.
  std::sort(slots.begin(), slots.end(),
            [](const Gen2ReadSlot& a, const Gen2ReadSlot& b) {
              return a.offset < b.offset;
            });

  const size_t num = slots.size();
  std::vector<FSReadRequest> read_reqs;
  read_reqs.reserve(num);
  for (size_t i = 0; i < num; ++i) {
    FSReadRequest read_req{};
    read_req.offset = slots[i].offset;
    read_req.len = slots[i].len;
    read_reqs.emplace_back(std::move(read_req));
  }

  AlignedBuffer direct_io_buffer;
  const bool direct_io = file->use_direct_io();
  if (direct_io) {
    for (size_t i = 0; i < num; ++i) {
      read_reqs[i].scratch = nullptr;
    }
  } else {
    for (size_t i = 0; i < num; ++i) {
      read_reqs[i].scratch = slots[i].buf;
    }
  }

  IOOptions opts;
  IODebugContext dbg;
  Status s = file->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    s = file->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                        &direct_io_context, &dbg);
  }
  if (!s.ok()) {
    // Batch read failed; s is written to every slot's status below, so these
    // per-request FS statuses carry nothing extra.
    for (auto& read_req : read_reqs) {
      read_req.status.PermitUncheckedError();
    }
    for (size_t i = 0; i < num; ++i) {
      *slots[i].status = s;
    }
    return;
  }

  for (size_t i = 0; i < num; ++i) {
    FSReadRequest& read_req = read_reqs[i];
    if (read_req.status.ok() && read_req.result.size() != read_req.len) {
      read_req.status = IOStatus::Corruption(
          "Incomplete blob record read from " + file->file_name() +
          " at offset " + std::to_string(read_req.offset) + ": expected " +
          std::to_string(read_req.len) + " bytes, got " +
          std::to_string(read_req.result.size()));
    }
    *slots[i].status = read_req.status;
    if (!slots[i].status->ok()) {
      continue;
    }
    // With mmap/direct reads the data lands outside the caller's buf; copy it
    // in so the caller can rely on buf owning the bytes.
    if (read_req.result.data() != slots[i].buf) {
      memcpy(slots[i].buf, read_req.result.data(), read_req.len);
    }
  }
}

}  // namespace

void ReadAndVerifySimpleGen2BlobRecords(const ReadOptions& read_options,
                                        RandomAccessFileReader* file,
                                        ChecksumType checksum_type,
                                        uint32_t base_context_checksum,
                                        size_t num_records,
                                        SimpleGen2RecordReadRequest* reqs) {
  assert(file != nullptr);
  assert(num_records > 0);

  std::vector<Gen2ReadSlot> slots;
  slots.reserve(num_records);
  for (size_t i = 0; i < num_records; ++i) {
    assert(reqs[i].buf != nullptr);
    assert(reqs[i].status != nullptr);
    assert(reqs[i].record_size ==
           reqs[i].payload_size + kSimpleGen2BlobTrailerSize);
    slots.push_back({reqs[i].record_offset, reqs[i].record_size, reqs[i].buf,
                     reqs[i].status});
  }

  MultiReadGen2(read_options, file, slots);

  for (size_t i = 0; i < num_records; ++i) {
    if (!reqs[i].status->ok()) {
      continue;
    }
    const char* record = reqs[i].buf;
    const size_t payload_size = reqs[i].payload_size;
    const CompressionType compression =
        static_cast<CompressionType>(record[payload_size]);
    if (compression != reqs[i].expected_compression) {
      *reqs[i].status = Status::Corruption(
          "Blob record compression mismatch in " + file->file_name() +
          " at offset " + std::to_string(reqs[i].record_offset) +
          ": index expects " +
          std::to_string(static_cast<int>(reqs[i].expected_compression)) +
          ", record has " + std::to_string(static_cast<int>(compression)));
      continue;
    }
    if (read_options.verify_checksums) {
      *reqs[i].status = VerifySimpleGen2BlobChecksum(
          checksum_type, base_context_checksum, record, payload_size,
          reqs[i].record_offset, file->file_name());
      if (!reqs[i].status->ok()) {
        continue;
      }
    }
    // See ReadAndVerifySimpleGen2BlobRecord: the marker matched the index, but
    // only uncompressed SimpleGen2Blob payloads are currently supported.
    if (compression != kNoCompression) {
      *reqs[i].status = Status::Corruption(
          "Blob record compression " +
          std::to_string(static_cast<int>(compression)) +
          " is not supported in " + file->file_name() + " at offset " +
          std::to_string(reqs[i].record_offset));
      continue;
    }
  }
}

void ReadSimpleGen2BlobRanges(const ReadOptions& read_options,
                              RandomAccessFileReader* file, size_t num_records,
                              SimpleGen2RangeReadRequest* reqs) {
  assert(file != nullptr);
  assert(num_records > 0);

  std::vector<Gen2ReadSlot> slots;
  slots.reserve(num_records);
  for (size_t i = 0; i < num_records; ++i) {
    assert(reqs[i].buf != nullptr);
    assert(reqs[i].status != nullptr);
    // A strict sub-range of a compressed payload cannot be decompressed in
    // isolation; callers resolve such payloads whole and slice instead.
    if (reqs[i].expected_compression != kNoCompression) {
      *reqs[i].status = Status::Corruption(
          "Cannot range-read a compressed blob (compression " +
          std::to_string(static_cast<int>(reqs[i].expected_compression)) +
          ") in " + file->file_name() + " at offset " +
          std::to_string(reqs[i].record_offset));
      continue;
    }
    // The requested sub-range must lie within the payload (the trailer that
    // follows the payload is never part of a range read).
    if (reqs[i].range_offset > reqs[i].payload_size ||
        reqs[i].range_length > reqs[i].payload_size - reqs[i].range_offset) {
      *reqs[i].status = Status::InvalidArgument(
          "Blob range [" + std::to_string(reqs[i].range_offset) + ", +" +
          std::to_string(reqs[i].range_length) +
          ") out of bounds for payload size " +
          std::to_string(reqs[i].payload_size) + " in " + file->file_name() +
          " at offset " + std::to_string(reqs[i].record_offset));
      continue;
    }
    slots.push_back({reqs[i].record_offset + reqs[i].range_offset,
                     reqs[i].range_length, reqs[i].buf, reqs[i].status});
  }

  if (slots.empty()) {
    return;
  }

  // slots only covers requests that passed validation above; a failed
  // validation left *reqs[i].status non-OK and is skipped here.
  MultiReadGen2(read_options, file, slots);
}

IOStatus WriteSimpleGen2BlobRecord(WritableFileWriter* file,
                                   const WriteOptions& write_options,
                                   ChecksumType checksum_type,
                                   uint32_t base_context_checksum,
                                   uint64_t record_offset, const Slice& payload,
                                   CompressionType compression) {
  assert(file != nullptr);
  // Placeholder for future embedded blob compression support; only
  // uncompressed payloads are currently written.
  assert(compression == kNoCompression);

  std::array<char, kSimpleGen2BlobTrailerSize> trailer;
  trailer[0] = lossless_cast<char>(compression);
  uint32_t checksum = ComputeBuiltinChecksumWithLastByte(
      checksum_type, payload.data(), payload.size(), /*last_byte=*/trailer[0]);
  checksum += ChecksumModifierForContext(base_context_checksum, record_offset);
  EncodeFixed32(trailer.data() + 1, checksum);

  IOOptions opts;
  IOStatus io_s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!io_s.ok()) {
    return io_s;
  }
  if (!payload.empty()) {
    io_s = file->Append(payload, opts);
    if (!io_s.ok()) {
      return io_s;
    }
  }
  return file->Append(Slice(trailer.data(), trailer.size()), opts);
}

}  // namespace ROCKSDB_NAMESPACE
