//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_gen2_format.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
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
};

// The inline size of the slot and physical read lists in MultiReadGen2. A call
// with more entries reserves each list once on the heap, so this size only
// decides when a call allocates. 32 is an estimate of a common call size: it
// equals MultiGetContext::MAX_BATCH_SIZE, so a call from one MultiGet batch
// with one read per key fits. It is not a limit: one
// LazyWideColumnsBatch::MultiResolve call can pass any number of reads.
constexpr size_t kGen2ReadInlineSize = 32;

// Reads the byte ranges in `slots` from `file`. Each slot gets its bytes in buf
// and its result in *status. The whole-record and the range batch readers below
// share this helper. One call reads one batch: the reads of one file and one
// read kind (see the batch readers in blob_gen2_format.h).
//
// Background: FSRandomAccessFile::MultiRead requires that the requests of one
// call do not overlap. The slots can still overlap. The same range can appear
// twice (for example, a MultiGetEntityLazy call with the same key twice), and
// two range reads of one value can partly overlap. So the helper works in these
// steps:
// 1. Sort the slots by (offset, length). Then overlapping slots are next to
//    each other, and one pass over the slots finds them.
// 2. Merge. A "physical read" is one request to RandomAccessFileReader::
//    MultiRead. Walk the sorted slots and add each slot to the current physical
//    read when the slot starts before the end of that read. Otherwise the slot
//    starts a new physical read. The slots of one physical read are its
//    members. Exact duplicates are members of the same physical read, so they
//    share one file read. Slots with a gap between them stay in separate
//    physical reads, so the bytes in the gap are not read. Slots that only
//    touch also stay separate, so a slot that shares no bytes can still read
//    straight into its own buf.
// 3. Choose where each physical read puts its bytes. When one member covers
//    the whole physical read, the read goes straight into that member's buf.
//    This is the case for a physical read with one member, for exact
//    duplicates, and for nested ranges whose longest range starts at the
//    physical read's offset. Otherwise (members that only partly overlap), the
//    read goes into a scratch buffer of the merged size. With direct I/O, the
//    reader reads into its own aligned buffer instead. It aligns the physical
//    reads to the alignment that the file requires, and may merge them,
//    before it calls the file system.
// 4. Issue all physical reads in one MultiRead call, or in one plain Read when
//    there is only one physical read. The call is made only when at least one
//    slot needs bytes and preparing the I/O options succeeds.
// 5. Route results. Each member gets the status of its physical read. Each
//    member that read OK copies its own slice of the returned bytes into its
//    buf, unless the slice is already there. The covering member's buf is a
//    source of these copies and is never written by them.
//
// For example, slots A and B both read file bytes [0, 100) and slot C reads
// [50, 150). They merge into one physical read of [0, 150):
//
//   offset    0         50        100       150
//   A         [-------------------)
//   B         [-------------------)
//   C                   [-------------------)
//   read      [-----------------------------)
//
// No member covers [0, 150), so the read goes into scratch. A and B copy bytes
// [0, 100) of it, and C copies [50, 150). If C read [20, 30) instead, the
// physical read would be [0, 100). A covers it, so the read would go into A's
// buf, and B and C would copy their slices from A's buf.
//
// Results:
// - If preparing the I/O options or the MultiRead call fails as a whole, every
//   slot that needed a read gets that error.
// - If one physical read fails, all of its members get its error.
// - If a physical read returns fewer bytes than asked, a member whose range
//   lies fully inside the returned bytes gets OK and its bytes. Any other
//   member gets Corruption ("Incomplete blob record read").
// - An empty slot gets OK without I/O. A slot whose end offset does not fit in
//   uint64_t gets Corruption, because the merge step computes end offsets.
// The whole-record caller checks each record buffer on its own afterwards.
//
// Memory: scratch is allocated only without direct I/O, and only for physical
// reads that no member covers. These reads share one allocation per call. Its
// size is at most the total length of their members, because a merged span is
// never longer than the ranges it covers put together. In a valid file, two
// different records never share bytes, so whole records never partly overlap
// and whole-record reads never use scratch. Their bytes then go only into the
// caller's buffers, which can come from the blob cache's memory allocator (for
// example, one that keeps blob bytes out of core dumps). The range callers
// allocate their buffers without that allocator, so scratch for range reads
// does not bypass it. An mmap read can return bytes from the file mapping even
// when the helper gives it a buffer. The members then copy from the returned
// bytes, as in step 5.
void MultiReadGen2(const ReadOptions& read_options,
                   RandomAccessFileReader* file,
                   autovector<Gen2ReadSlot, kGen2ReadInlineSize>& slots) {
  assert(file != nullptr);
  assert(!slots.empty());

  // Step 1.
  std::sort(slots.begin(), slots.end(),
            [](const Gen2ReadSlot& a, const Gen2ReadSlot& b) {
              return std::make_pair(a.offset, a.len) <
                     std::make_pair(b.offset, b.len);
            });

  // Empty slots and slots whose end offset overflows get their status in step
  // 2 and are never members.
  const auto needs_read = [](const Gen2ReadSlot& slot) {
    return slot.len > 0 &&
           slot.offset <= std::numeric_limits<uint64_t>::max() - slot.len;
  };

  // Step 2. A physical read covers the sorted slots [first_slot, end_slot).
  // The slots in that index range that need a read are its members.
  // first_slot is always a member.
  struct PhysicalRead {
    uint64_t offset = 0;
    uint64_t end_offset = 0;
    size_t first_slot = 0;
    size_t end_slot = 0;
    // The member that starts at `offset` and is the longest. It covers the
    // whole physical read when it also ends at `end_offset`.
    size_t longest_first_slot = 0;
  };
  autovector<PhysicalRead, kGen2ReadInlineSize> physical_reads;
  physical_reads.reserve(slots.size());
  for (size_t i = 0; i < slots.size(); ++i) {
    Gen2ReadSlot& slot = slots[i];
    if (slot.len == 0) {
      *slot.status = Status::OK();
      continue;
    }
    if (!needs_read(slot)) {
      *slot.status = Status::Corruption("Blob read range overflow in " +
                                        file->file_name() + " at offset " +
                                        std::to_string(slot.offset));
      continue;
    }
    const uint64_t end_offset = slot.offset + slot.len;
    if (!physical_reads.empty() &&
        slot.offset < physical_reads.back().end_offset) {
      PhysicalRead& read = physical_reads.back();
      read.end_offset = std::max(read.end_offset, end_offset);
      read.end_slot = i + 1;
      if (slot.offset == read.offset) {
        // The sort order makes this slot the longest so far at this offset.
        read.longest_first_slot = i;
      }
    } else {
      physical_reads.push_back(
          PhysicalRead{slot.offset, end_offset, i, i + 1, i});
    }
  }
  if (physical_reads.empty()) {
    return;
  }

  // Step 3. Direct I/O reads into the reader's aligned buffer, so it needs
  // neither caller buffers nor scratch.
  const bool direct_io = file->use_direct_io();
  // A physical read covers only slots that overlap, so its span is at most the
  // sum of its members' lengths. All member buffers exist at the same time, so
  // the span fits in size_t, also where size_t has 32 bits.
  const auto span_of = [](const PhysicalRead& read) {
    const uint64_t span = read.end_offset - read.offset;
    assert(span <= std::numeric_limits<size_t>::max());
    return static_cast<size_t>(span);
  };
  const auto covering_buf = [&](const PhysicalRead& read) -> char* {
    const Gen2ReadSlot& longest = slots[read.longest_first_slot];
    return longest.offset + longest.len == read.end_offset ? longest.buf
                                                           : nullptr;
  };
  size_t scratch_size = 0;
  if (!direct_io) {
    for (const PhysicalRead& read : physical_reads) {
      if (covering_buf(read) == nullptr) {
        scratch_size += span_of(read);
      }
    }
  }
  std::unique_ptr<char[]> scratch;
  if (scratch_size > 0) {
    scratch.reset(new char[scratch_size]);
  }
  // MultiRead takes the requests as one array, so they go into a vector. An
  // autovector keeps one array only up to its inline size.
  std::vector<FSReadRequest> read_reqs;
  read_reqs.reserve(physical_reads.size());
  char* next_scratch = scratch.get();
  for (const PhysicalRead& read : physical_reads) {
    FSReadRequest read_req{};
    read_req.offset = read.offset;
    read_req.len = span_of(read);
    if (direct_io) {
      read_req.scratch = nullptr;
    } else if (char* buf = covering_buf(read)) {
      read_req.scratch = buf;
    } else {
      // The reader changes the first byte of each buffer before the read, so
      // that a file system that reports success without writing the buffer
      // fails the checks. Changing the byte reads it, so it must be set.
      next_scratch[0] = 0;
      read_req.scratch = next_scratch;
      next_scratch += read_req.len;
    }
    read_reqs.emplace_back(std::move(read_req));
  }

  // Step 4.
  AlignedBuffer direct_io_buffer;
  IOOptions opts;
  IODebugContext dbg;
  Status s = file->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    TEST_SYNC_POINT_CALLBACK("MultiReadGen2:Requests", &read_reqs);
    if (read_reqs.size() == 1) {
      // One physical read (for example, a Get of one relocated value) uses a
      // plain positional read, without the MultiRead submit and wait.
      TEST_SYNC_POINT("MultiReadGen2:SingleRead");
      file->Read(opts, &read_reqs[0], &direct_io_context, &dbg);
      s = Status::OK();
    } else {
      s = file->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                          &direct_io_context, &dbg);
    }
  }
  if (!s.ok()) {
    // The call failed as a whole. s goes to every member below, so the
    // per-request statuses carry nothing extra.
    for (FSReadRequest& read_req : read_reqs) {
      read_req.status.PermitUncheckedError();
    }
    for (const PhysicalRead& read : physical_reads) {
      for (size_t i = read.first_slot; i < read.end_slot; ++i) {
        if (needs_read(slots[i])) {
          *slots[i].status = s;
        }
      }
    }
    return;
  }

  // Step 5.
  for (size_t r = 0; r < physical_reads.size(); ++r) {
    const PhysicalRead& read = physical_reads[r];
    const FSReadRequest& read_req = read_reqs[r];
    const bool read_ok = read_req.status.ok();
    const uint64_t returned_end = read.offset + read_req.result.size();
    for (size_t i = read.first_slot; i < read.end_slot; ++i) {
      Gen2ReadSlot& slot = slots[i];
      if (!needs_read(slot)) {
        continue;
      }
      if (!read_ok) {
        *slot.status = read_req.status;
        continue;
      }
      if (slot.offset + slot.len > returned_end) {
        const uint64_t got =
            returned_end > slot.offset ? returned_end - slot.offset : 0;
        *slot.status = Status::Corruption(
            "Incomplete blob record read from " + file->file_name() +
            " at offset " + std::to_string(slot.offset) + ": expected " +
            std::to_string(slot.len) + " bytes, got " + std::to_string(got));
        continue;
      }
      *slot.status = Status::OK();
      const char* data = read_req.result.data() + (slot.offset - read.offset);
      if (data != slot.buf) {
        memcpy(slot.buf, data, slot.len);
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

  autovector<Gen2ReadSlot, kGen2ReadInlineSize> slots;
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

  autovector<Gen2ReadSlot, kGen2ReadInlineSize> slots;
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

  std::array<char, kSimpleGen2BlobTrailerSize> trailer{};
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
