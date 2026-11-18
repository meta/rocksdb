//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_gen2_format.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <vector>

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
//
// With kCRC32c and a non-null `payload_crc32c`, a passing check also sets
// *payload_crc32c to the CRC32C of the payload alone. It is computed in the
// same pass, so a caller that checks the logical CRC of an uncompressed value
// does not read the bytes again.
Status VerifySimpleGen2BlobChecksum(ChecksumType checksum_type,
                                    uint32_t base_context_checksum,
                                    const char* record, size_t payload_size,
                                    uint64_t record_offset,
                                    const std::string& file_name,
                                    std::optional<uint32_t>* payload_crc32c) {
  uint32_t stored = DecodeFixed32(record + payload_size + 1);
  const uint32_t modifier =
      ChecksumModifierForContext(base_context_checksum, record_offset);
  stored -= modifier;
  uint32_t computed = 0;
  if (checksum_type == kCRC32c && payload_crc32c != nullptr) {
    const uint32_t payload_crc = crc32c::Value(record, payload_size);
    computed = crc32c::Mask(
        crc32c::Extend(payload_crc, &record[payload_size], /*size=*/1));
    if (stored == computed) {
      *payload_crc32c = payload_crc;
    }
  } else {
    computed = ComputeBuiltinChecksumWithLastByte(
        checksum_type, record, payload_size, record[payload_size]);
  }
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
        record_offset, file->file_name(), /*payload_crc32c=*/nullptr);
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
  bool* copied = nullptr;
};

// Reads the ranges described by `slots` and copies each result into its buf,
// setting *slots[i].status. Shared by the whole-record and range batch readers;
// the whole-record caller checks each record's format afterwards from the
// filled buffers.
// 1. Sort the slots by (offset, length).
// 2. Read an exact duplicate range once. The duplicate slot gets a copy of the
//    bytes and the status, and *copied is set.
// 3. Put each range into the first batch whose last range ends at or before its
//    offset, or into a new batch. Partly overlapping ranges thus go to
//    different batches, because FileSystem::MultiRead requires non-overlapping
//    requests.
// 4. Issue each batch as one MultiRead, or as one plain Read when the batch has
//    a single range.
void MultiReadGen2(const ReadOptions& read_options,
                   RandomAccessFileReader* file,
                   std::vector<Gen2ReadSlot>& slots) {
  assert(file != nullptr);
  assert(!slots.empty());

  // Sort by (offset, length). Each batch is then in ascending offset order, as
  // RandomAccessFileReader::MultiRead requires, and exact duplicates are next
  // to each other.
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
  // Exact duplicates, as pairs of (duplicate slot, slot whose read it copies).
  std::vector<std::pair<size_t, size_t>> duplicates;
  for (size_t i = 0; i < slots.size(); ++i) {
    if (slots[i].len == 0) {
      *slots[i].status = Status::OK();
      continue;
    }
    if (slots[i].offset > std::numeric_limits<uint64_t>::max() - slots[i].len) {
      *slots[i].status = Status::Corruption("Blob read range overflow");
      continue;
    }
    if (!physical_reads.empty() &&
        physical_reads.back().offset == slots[i].offset &&
        physical_reads.back().len == slots[i].len) {
      duplicates.emplace_back(i, physical_reads.back().slot_index);
      continue;
    }
    physical_reads.push_back(PhysicalRead{slots[i].offset, slots[i].len, i});
  }
  if (physical_reads.empty()) {
    return;
  }
  const auto copy_duplicates = [&]() {
    for (const auto& [duplicate, source] : duplicates) {
      if (slots[duplicate].copied != nullptr) {
        *slots[duplicate].copied = true;
      }
      *slots[duplicate].status = *slots[source].status;
      if (slots[duplicate].status->ok()) {
        memcpy(slots[duplicate].buf, slots[source].buf, slots[duplicate].len);
      }
    }
  };

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
      copy_duplicates();
      return;
    }

    std::vector<FSReadRequest> read_reqs;
    read_reqs.reserve(batch.read_indexes.size());
    for (size_t read_index : batch.read_indexes) {
      const PhysicalRead& read = physical_reads[read_index];
      FSReadRequest read_req{};
      read_req.offset = read.offset;
      read_req.len = read.len;
      read_req.scratch = direct_io ? nullptr : slots[read.slot_index].buf;
      read_reqs.emplace_back(std::move(read_req));
    }

    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    TEST_SYNC_POINT_CALLBACK("MultiReadGen2:Requests", &read_reqs);
    if (read_reqs.size() == 1) {
      // A batch with one range (for example, a Get of one relocated value) uses
      // a plain positional read, without the MultiRead submit and wait.
      TEST_SYNC_POINT("MultiReadGen2:SingleRead");
      file->Read(opts, &read_reqs[0], &direct_io_context, &dbg);
      s = Status::OK();
    } else {
      s = file->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                          &direct_io_context, &dbg);
    }
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
  copy_duplicates();
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
    slots.push_back(
        {reqs[i].record_offset, reqs[i].record_size, reqs[i].buf,
         reqs[i].status,
         reqs[i].extras != nullptr ? &reqs[i].extras->copied : nullptr});
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
          reqs[i].record_offset, file->file_name(),
          reqs[i].extras != nullptr ? &reqs[i].extras->payload_crc32c
                                    : nullptr);
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
    slots.push_back(
        {reqs[i].record_offset + reqs[i].range_offset, reqs[i].range_length,
         reqs[i].buf, reqs[i].status,
         reqs[i].extras != nullptr ? &reqs[i].extras->copied : nullptr});
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

}  // namespace ROCKSDB_NAMESPACE
