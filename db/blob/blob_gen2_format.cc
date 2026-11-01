//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_gen2_format.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "db/blob/blob_index.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/format.h"
#include "test_util/sync_point.h"
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

// Issues MultiRead batches for the records described by `slots` and copies
// each result into its buf, setting *slots[i].status. Duplicate and overlapping
// ranges are partitioned because the FileSystem::MultiRead contract requires
// non-overlapping requests. Shared by
// the whole-record and range batch readers; per-record format checks are done
// by the whole-record caller afterwards from the populated buffers.
void MultiReadGen2(const ReadOptions& read_options,
                   RandomAccessFileReader* file,
                   std::vector<Gen2ReadSlot>& slots) {
  assert(file != nullptr);
  assert(!slots.empty());

  // Sort by the full physical range so each batch is submitted in the order
  // RandomAccessFileReader requires.
  std::sort(slots.begin(), slots.end(),
            [](const Gen2ReadSlot& a, const Gen2ReadSlot& b) {
              return std::make_pair(a.offset, a.len) <
                     std::make_pair(b.offset, b.len);
            });

  struct PhysicalRead {
    uint64_t offset;
    size_t len;
    size_t slot_index;
  };
  std::vector<PhysicalRead> physical_reads;
  physical_reads.reserve(slots.size());
  for (size_t i = 0; i < slots.size(); ++i) {
    if (slots[i].len == 0) {
      *slots[i].status = Status::OK();
      continue;
    }
    if (slots[i].offset > std::numeric_limits<uint64_t>::max() - slots[i].len) {
      *slots[i].status = Status::Corruption("Blob read range overflow");
      continue;
    }
    physical_reads.push_back(PhysicalRead{slots[i].offset, slots[i].len, i});
  }
  if (physical_reads.empty()) {
    return;
  }

  struct ReadBatch {
    uint64_t end_offset;
    std::vector<size_t> read_indexes;
  };
  std::vector<ReadBatch> batches;
  for (size_t i = 0; i < physical_reads.size(); ++i) {
    const PhysicalRead& read = physical_reads[i];
    size_t batch_index = batches.size();
    for (size_t j = 0; j < batches.size(); ++j) {
      if (batches[j].end_offset <= read.offset) {
        batch_index = j;
        break;
      }
    }
    const uint64_t end_offset = read.offset + read.len;
    if (batch_index == batches.size()) {
      batches.push_back(ReadBatch{end_offset, {i}});
    } else {
      batches[batch_index].end_offset = end_offset;
      batches[batch_index].read_indexes.push_back(i);
    }
  }

  const bool direct_io = file->use_direct_io();
  for (size_t batch_index = 0; batch_index < batches.size(); ++batch_index) {
    const ReadBatch& batch = batches[batch_index];
    IOOptions opts;
    IODebugContext dbg;
    Status s = file->PrepareIOOptions(read_options, opts, &dbg);
    if (!s.ok()) {
      for (size_t unissued = batch_index; unissued < batches.size();
           ++unissued) {
        for (size_t read_index : batches[unissued].read_indexes) {
          const PhysicalRead& read = physical_reads[read_index];
          *slots[read.slot_index].status = s;
        }
      }
      return;
    }

    std::vector<FSReadRequest> read_reqs;
    read_reqs.reserve(batch.read_indexes.size());
    for (size_t read_index : batch.read_indexes) {
      const PhysicalRead& read = physical_reads[read_index];
      FSReadRequest read_req;
      read_req.offset = read.offset;
      read_req.len = read.len;
      read_req.scratch = direct_io ? nullptr : slots[read.slot_index].buf;
      read_reqs.emplace_back(std::move(read_req));
    }

    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    TEST_SYNC_POINT_CALLBACK("MultiReadGen2:Requests", &read_reqs);
    s = file->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                        &direct_io_context, &dbg);
    if (!s.ok()) {
      for (FSReadRequest& read_req : read_reqs) {
        read_req.status.PermitUncheckedError();
      }
      for (size_t read_index : batch.read_indexes) {
        const PhysicalRead& read = physical_reads[read_index];
        *slots[read.slot_index].status = s;
      }
      continue;
    }

    for (size_t i = 0; i < read_reqs.size(); ++i) {
      FSReadRequest& read_req = read_reqs[i];
      if (read_req.status.ok() && read_req.result.size() != read_req.len) {
        read_req.status = IOStatus::Corruption(
            "Incomplete blob record read from " + file->file_name() +
            " at offset " + std::to_string(read_req.offset) + ": expected " +
            std::to_string(read_req.len) + " bytes, got " +
            std::to_string(read_req.result.size()));
      }
      const PhysicalRead& read = physical_reads[batch.read_indexes[i]];
      Gen2ReadSlot& slot = slots[read.slot_index];
      *slot.status = read_req.status;
      if (slot.status->ok() && read_req.result.data() != slot.buf) {
        memcpy(slot.buf, read_req.result.data(), read_req.len);
      }
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

Status StreamAndVerifyBlobStoredPayload(
    const ReadOptions& read_options, RandomAccessFileReader* file,
    uint64_t record_offset, uint64_t payload_size, CompressionType compression,
    uint32_t stored_checksum, const BlobChunkConsumer& consumer) {
  if (file == nullptr || !consumer ||
      payload_size > std::numeric_limits<uint64_t>::max() - record_offset) {
    return Status::InvalidArgument("Invalid stored blob stream");
  }
  std::array<char, kBlobRelocationStreamChunkSize> scratch;
  uint64_t position = 0;
  uint32_t checksum = 0;
  while (position < payload_size) {
    const size_t length = lossless_cast<size_t>(
        std::min<uint64_t>(scratch.size(), payload_size - position));
    IOOptions options;
    IODebugContext debug_context;
    Status status =
        file->PrepareIOOptions(read_options, options, &debug_context);
    if (!status.ok()) {
      return status;
    }
    Slice chunk;
    status = file->Read(record_offset + position, length, options, &chunk,
                        scratch.data(), &debug_context);
    if (!status.ok()) {
      return status;
    }
    if (chunk.size() != length) {
      return Status::Corruption("Incomplete stored blob stream");
    }
    TEST_SYNC_POINT_CALLBACK("StreamAndVerifyBlobStoredPayload:Chunk", &chunk);
    if (chunk.size() != length) {
      return Status::Corruption("Changed stored blob stream length");
    }
    checksum = crc32c::Extend(checksum, chunk.data(), chunk.size());
    status = consumer(chunk);
    if (!status.ok()) {
      return status;
    }
    position += length;
  }
  if (BlobIndex::ExtendChecksumWithCompression(checksum, compression) !=
      stored_checksum) {
    return Status::Corruption("Stored blob stream checksum mismatch");
  }
  return Status::OK();
}

Status WriteSimpleGen2BlobRecordStream(
    WritableFileWriter* file, const WriteOptions& write_options,
    ChecksumType checksum_type, uint32_t base_context_checksum,
    uint64_t record_offset, uint64_t payload_size, CompressionType compression,
    uint32_t stored_checksum, const VerifiedBlobStoredSource& source) {
  if (file == nullptr || !source ||
      (checksum_type != kCRC32c && checksum_type != kNoChecksum) ||
      payload_size > kMaxSimpleGen2BlobPayloadSize ||
      record_offset > std::numeric_limits<uint64_t>::max() - payload_size -
                          kSimpleGen2BlobTrailerSize) {
    return Status::InvalidArgument("Invalid streaming blob output");
  }
  uint64_t delivered = 0;
  Status consumer_status;
  Status status = source([&](const Slice& chunk) {
    if (!consumer_status.ok()) {
      return consumer_status;
    }
    if (chunk.size() > payload_size - delivered) {
      consumer_status = Status::Corruption("Stored blob stream is too long");
      return consumer_status;
    }
    IOOptions options;
    consumer_status =
        WritableFileWriter::PrepareIOOptions(write_options, options);
    if (consumer_status.ok()) {
      consumer_status = file->Append(chunk, options);
    }
    if (consumer_status.ok()) {
      delivered += chunk.size();
    }
    return consumer_status;
  });
  // Do not allow a source that swallowed the destination error to publish.
  if (!consumer_status.ok()) {
    status.PermitUncheckedError();
    return consumer_status;
  }
  if (!status.ok()) {
    return status;
  }
  if (delivered != payload_size) {
    return Status::Corruption("Stored blob stream is too short");
  }
  std::array<char, kSimpleGen2BlobTrailerSize> trailer;
  trailer[0] = lossless_cast<char>(compression);
  const uint32_t checksum =
      checksum_type == kCRC32c ? crc32c::Mask(stored_checksum) : 0;
  EncodeFixed32(trailer.data() + 1,
                checksum + ChecksumModifierForContext(base_context_checksum,
                                                      record_offset));
  IOOptions options;
  status = WritableFileWriter::PrepareIOOptions(write_options, options);
  return status.ok() ? Status(file->Append(
                           Slice(trailer.data(), trailer.size()), options))
                     : status;
}

}  // namespace ROCKSDB_NAMESPACE
