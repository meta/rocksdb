//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <cassert>
#include <string>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_log_format.h"
#include "file/file_prefetch_buffer.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/format.h"
#include "table/multiget_context.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#include "util/compression.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFileReader::Create(
    const ImmutableOptions& immutable_options, const ReadOptions& read_options,
    const FileOptions& file_options, uint32_t column_family_id,
    HistogramImpl* blob_file_read_hist, uint64_t blob_file_number,
    const std::shared_ptr<IOTracer>& io_tracer, bool skip_footer_validation,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  assert(blob_file_reader);
  assert(!*blob_file_reader);

  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFileReader> file_reader;

  {
    const Status s =
        OpenFile(immutable_options, file_options, blob_file_read_hist,
                 blob_file_number, io_tracer, &file_size, &file_reader,
                 /*skip_footer_size_check=*/skip_footer_validation);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file_reader);

  Statistics* const statistics = immutable_options.stats;

  CompressionType compression_type = kNoCompression;

  {
    const Status s =
        ReadHeader(file_reader.get(), read_options, column_family_id,
                   statistics, &compression_type);
    if (!s.ok()) {
      return s;
    }
  }

  if (!skip_footer_validation) {
    const Status s =
        ReadFooter(file_reader.get(), read_options, file_size, statistics);
    if (!s.ok()) {
      return s;
    }
  }

  std::shared_ptr<Decompressor> decompressor;
  if (compression_type != kNoCompression) {
    // The blob format has always used compression format 2
    decompressor = GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
        compression_type);
  }

  blob_file_reader->reset(
      new BlobFileReader(std::move(file_reader), file_size, compression_type,
                         std::move(decompressor), immutable_options.clock,
                         statistics, !skip_footer_validation));

  return Status::OK();
}

Status BlobFileReader::OpenFile(
    const ImmutableOptions& immutable_options, const FileOptions& file_opts,
    HistogramImpl* blob_file_read_hist, uint64_t blob_file_number,
    const std::shared_ptr<IOTracer>& io_tracer, uint64_t* file_size,
    std::unique_ptr<RandomAccessFileReader>* file_reader,
    bool skip_footer_size_check) {
  assert(file_size);
  assert(file_reader);

  const auto& cf_paths = immutable_options.cf_paths;
  assert(!cf_paths.empty());

  const std::string blob_file_path =
      BlobFileName(cf_paths.front().path, blob_file_number);

  FileSystem* const fs = immutable_options.fs.get();
  assert(fs);

  constexpr IODebugContext* dbg = nullptr;

  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions reader_file_opts = file_opts;

  if (skip_footer_size_check && reader_file_opts.use_direct_reads) {
    reader_file_opts.use_direct_reads = false;
  }

  {
    TEST_SYNC_POINT("BlobFileReader::OpenFile:NewRandomAccessFile");

    const Status s =
        fs->NewRandomAccessFile(blob_file_path, reader_file_opts, &file, dbg);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file);

  {
    Status s = GetFileSizeFromOpenFileOrPath(
        file.get(), fs, blob_file_path, file_size, dbg,
        FileSizeFallback::kNotSupportedOnly,
        []() { TEST_SYNC_POINT("BlobFileReader::OpenFile:GetFileSize"); });
    if (!s.ok()) {
      return s;
    }
  }

  if (!skip_footer_size_check &&
      *file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    return Status::Corruption("Malformed blob file");
  }
  if (skip_footer_size_check && *file_size < BlobLogHeader::kSize) {
    return Status::Corruption("Malformed blob file");
  }

  if (immutable_options.advise_random_on_open) {
    file->Hint(Env::AccessPattern::kRandom);
  }

  file_reader->reset(new RandomAccessFileReader(
      std::move(file), blob_file_path, immutable_options.clock, io_tracer,
      immutable_options.stats, BLOB_DB_BLOB_FILE_READ_MICROS,
      blob_file_read_hist, immutable_options.rate_limiter.get(),
      immutable_options.listeners));

  return Status::OK();
}

Status BlobFileReader::ReadHeader(const RandomAccessFileReader* file_reader,
                                  const ReadOptions& read_options,
                                  uint32_t column_family_id,
                                  Statistics* statistics,
                                  CompressionType* compression_type) {
  assert(file_reader);
  assert(compression_type);

  Slice header_slice;
  Buffer buf;
  AlignedBuffer direct_io_buffer;

  {
    TEST_SYNC_POINT("BlobFileReader::ReadHeader:ReadFromFile");

    constexpr uint64_t read_offset = 0;
    constexpr size_t read_size = BlobLogHeader::kSize;

    const Status s =
        ReadFromFile(file_reader, read_options, read_offset, read_size,
                     statistics, &header_slice, &buf, &direct_io_buffer);
    if (!s.ok()) {
      return s;
    }

    TEST_SYNC_POINT_CALLBACK("BlobFileReader::ReadHeader:TamperWithResult",
                             &header_slice);
  }

  BlobLogHeader header;

  {
    const Status s = header.DecodeFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  constexpr ExpirationRange no_expiration_range;

  if (header.has_ttl || header.expiration_range != no_expiration_range) {
    return Status::Corruption("Unexpected TTL blob file");
  }

  if (header.column_family_id != column_family_id) {
    return Status::Corruption("Column family ID mismatch");
  }

  *compression_type = header.compression;

  return Status::OK();
}

Status BlobFileReader::ReadFooter(const RandomAccessFileReader* file_reader,
                                  const ReadOptions& read_options,
                                  uint64_t file_size, Statistics* statistics) {
  assert(file_size >= BlobLogHeader::kSize + BlobLogFooter::kSize);
  assert(file_reader);

  Slice footer_slice;
  Buffer buf;
  AlignedBuffer direct_io_buffer;

  {
    TEST_SYNC_POINT("BlobFileReader::ReadFooter:ReadFromFile");

    const uint64_t read_offset = file_size - BlobLogFooter::kSize;
    constexpr size_t read_size = BlobLogFooter::kSize;

    const Status s =
        ReadFromFile(file_reader, read_options, read_offset, read_size,
                     statistics, &footer_slice, &buf, &direct_io_buffer);
    if (!s.ok()) {
      return s;
    }

    TEST_SYNC_POINT_CALLBACK("BlobFileReader::ReadFooter:TamperWithResult",
                             &footer_slice);
  }

  BlobLogFooter footer;

  {
    const Status s = footer.DecodeFrom(footer_slice);
    if (!s.ok()) {
      return s;
    }
  }

  constexpr ExpirationRange no_expiration_range;

  if (footer.expiration_range != no_expiration_range) {
    return Status::Corruption("Unexpected TTL blob file");
  }

  return Status::OK();
}

Status BlobFileReader::ReadFromFile(const RandomAccessFileReader* file_reader,
                                    const ReadOptions& read_options,
                                    uint64_t read_offset, size_t read_size,
                                    Statistics* statistics, Slice* slice,
                                    Buffer* buf,
                                    AlignedBuffer* direct_io_buffer) {
  assert(slice);
  assert(buf);
  assert(direct_io_buffer);

  assert(file_reader);

  RecordTick(statistics, BLOB_DB_BLOB_FILE_BYTES_READ, read_size);

  Status s;

  IOOptions io_options;
  IODebugContext dbg;
  s = file_reader->PrepareIOOptions(read_options, io_options, &dbg);
  if (!s.ok()) {
    return s;
  }

  FSReadRequest read_req;
  read_req.offset = read_offset;
  read_req.len = read_size;
  if (file_reader->use_direct_io()) {
    read_req.scratch = nullptr;
    AlignedBufferAllocationContext direct_io_context{direct_io_buffer};
    file_reader->Read(io_options, &read_req, &direct_io_context, &dbg);
  } else {
    buf->reset(new char[read_size]);
    read_req.scratch = buf->get();
    file_reader->Read(io_options, &read_req, nullptr, &dbg);
  }
  s = std::move(read_req.status);

  if (!s.ok()) {
    return s;
  }
  *slice = read_req.result;

  if (slice->size() != read_size) {
    return Status::Corruption("Incomplete blob read from " +
                              file_reader->file_name() + " at offset " +
                              std::to_string(read_offset) + ": expected " +
                              std::to_string(read_size) + " bytes, got " +
                              std::to_string(slice->size()));
  }

  return Status::OK();
}

BlobFileReader::BlobFileReader(
    std::unique_ptr<RandomAccessFileReader>&& file_reader, uint64_t file_size,
    CompressionType compression_type,
    std::shared_ptr<Decompressor> decompressor, SystemClock* clock,
    Statistics* statistics, bool has_footer)
    : file_reader_(std::move(file_reader)),
      file_size_(file_size),
      compression_type_(compression_type),
      decompressor_(std::move(decompressor)),
      clock_(clock),
      statistics_(statistics),
      has_footer_(has_footer) {
  assert(file_reader_);
}

BlobFileReader::~BlobFileReader() = default;

Status BlobFileReader::GetBlob(
    const ReadOptions& read_options, const Slice& user_key, uint64_t offset,
    uint64_t value_size, CompressionType compression_type,
    FilePrefetchBuffer* prefetch_buffer, MemoryAllocator* allocator,
    std::unique_ptr<BlobContents>* result, uint64_t* bytes_read) const {
  assert(result);

  const uint64_t key_size = user_key.size();

  if (!IsValidBlobOffset(offset, key_size, value_size, file_size_,
                         has_footer_)) {
    return Status::Corruption(
        "Invalid blob offset " + std::to_string(offset) + " for value size " +
        std::to_string(value_size) + " in " + file_reader_->file_name() +
        " (file size " + std::to_string(file_size_) + ")");
  }

  if (compression_type != compression_type_) {
    return Status::Corruption("Compression type mismatch when reading blob");
  }

  // Note: if verify_checksum is set, we read the entire blob record to be able
  // to perform the verification; otherwise, we just read the blob itself. Since
  // the offset in BlobIndex actually points to the blob value, we need to make
  // an adjustment in the former case.
  const uint64_t adjustment =
      read_options.verify_checksums
          ? BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)
          : 0;
  assert(offset >= adjustment);

  const uint64_t record_offset = offset - adjustment;
  const uint64_t record_size = value_size + adjustment;

  Slice record_slice;
  Buffer buf;
  AlignedBuffer direct_io_buffer;

  bool prefetched = false;

  if (prefetch_buffer) {
    Status s;
    constexpr bool for_compaction = true;

    IOOptions io_options;
    IODebugContext dbg;
    s = file_reader_->PrepareIOOptions(read_options, io_options, &dbg);
    if (!s.ok()) {
      return s;
    }
    prefetched = prefetch_buffer->TryReadFromCache(
        io_options, file_reader_.get(), record_offset,
        static_cast<size_t>(record_size), &record_slice, &s, for_compaction);
    if (!s.ok()) {
      return s;
    }
  }

  if (!prefetched) {
    TEST_SYNC_POINT("BlobFileReader::GetBlob:ReadFromFile");
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, record_size);
    PERF_TIMER_GUARD(blob_read_time);
    const Status s =
        ReadFromFile(file_reader_.get(), read_options, record_offset,
                     static_cast<size_t>(record_size), statistics_,
                     &record_slice, &buf, &direct_io_buffer);
    if (!s.ok()) {
      return s;
    }
  }

  TEST_SYNC_POINT_CALLBACK("BlobFileReader::GetBlob:TamperWithResult",
                           &record_slice);

  if (read_options.verify_checksums) {
    const Status s = VerifyBlob(record_slice, user_key, value_size);
    if (!s.ok()) {
      return s;
    }
  }

  const Slice value_slice(record_slice.data() + adjustment, value_size);

  {
    const Status s = UncompressBlobIfNeeded(value_slice, compression_type,
                                            decompressor_.get(), allocator,
                                            clock_, statistics_, result);
    if (!s.ok()) {
      return s;
    }
  }

  if (bytes_read) {
    *bytes_read = record_size;
  }

  return Status::OK();
}

Status BlobFileReader::GetBlobRange(const ReadOptions& read_options,
                                    const Slice& user_key, uint64_t offset,
                                    uint64_t value_size, uint64_t range_offset,
                                    size_t range_length,
                                    MemoryAllocator* allocator,
                                    std::unique_ptr<BlobContents>* result,
                                    uint64_t* bytes_read) const {
  assert(result);

  // Partial reads are only meaningful for uncompressed blobs: a strict
  // sub-range of a compressed record cannot be decompressed in isolation.
  // Callers (BlobSource::GetBlobRange) must ensure this; enforce it here too.
  if (compression_type_ != kNoCompression) {
    return Status::Corruption("Cannot range-read a compressed blob");
  }

  const uint64_t key_size = user_key.size();

  // Validate that the full value region is within the file (same check GetBlob
  // performs); this guards the sub-range read below.
  if (!IsValidBlobOffset(offset, key_size, value_size, file_size_,
                         has_footer_)) {
    return Status::Corruption(
        "Invalid blob offset " + std::to_string(offset) + " for value size " +
        std::to_string(value_size) + " in " + file_reader_->file_name() +
        " (file size " + std::to_string(file_size_) + ")");
  }

  // The requested sub-range must lie within the value.
  if (range_offset > value_size || range_length > value_size - range_offset) {
    return Status::InvalidArgument(
        "Blob range [" + std::to_string(range_offset) + ", +" +
        std::to_string(range_length) + ") out of bounds for value size " +
        std::to_string(value_size) + " in " + file_reader_->file_name());
  }

  // Read only the requested bytes, at the value's file position plus the range
  // offset. Unlike GetBlob there is no record-header adjustment (we do not read
  // the key/header) and no whole-record checksum verification (a strict
  // sub-range cannot cover it -- callers that require verification take the
  // full-read path instead).
  const uint64_t read_offset = offset + range_offset;
  const size_t read_size = range_length;

  Slice range_slice;
  Buffer buf;
  AlignedBuffer direct_io_buffer;

  TEST_SYNC_POINT("BlobFileReader::GetBlobRange:ReadFromFile");
  PERF_COUNTER_ADD(blob_read_count, 1);
  PERF_COUNTER_ADD(blob_read_byte, read_size);
  PERF_TIMER_GUARD(blob_read_time);

  {
    const Status s =
        ReadFromFile(file_reader_.get(), read_options, read_offset, read_size,
                     statistics_, &range_slice, &buf, &direct_io_buffer);
    if (!s.ok()) {
      return s;
    }
  }

  TEST_SYNC_POINT_CALLBACK("BlobFileReader::GetBlobRange:TamperWithResult",
                           &range_slice);

  // The blob is uncompressed, so the bytes read are the requested value bytes;
  // copy them into an owned BlobContents (no decompression).
  BlobContentsCreator::Create(result, /*out_charge=*/nullptr, range_slice,
                              kNoCompression, allocator);

  if (bytes_read) {
    *bytes_read = read_size;
  }

  return Status::OK();
}

void BlobFileReader::MultiGetBlob(
    const ReadOptions& read_options, MemoryAllocator* allocator,
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>&
        blob_reqs,
    uint64_t* bytes_read) const {
  const size_t num_blobs = blob_reqs.size();
  assert(num_blobs > 0);
  assert(num_blobs <= MultiGetContext::MAX_BATCH_SIZE);

#ifndef NDEBUG
  for (size_t i = 0; i < num_blobs - 1; ++i) {
    assert(blob_reqs[i].first->offset <= blob_reqs[i + 1].first->offset);
  }
#endif  // !NDEBUG

  std::vector<FSReadRequest> read_reqs;
  autovector<uint64_t> adjustments;
  uint64_t total_len = 0;
  read_reqs.reserve(num_blobs);
  for (size_t i = 0; i < num_blobs; ++i) {
    BlobReadRequest* const req = blob_reqs[i].first;
    assert(req);
    assert(req->user_key);
    assert(req->status);

    const size_t key_size = req->user_key->size();
    const uint64_t offset = req->offset;
    const uint64_t value_size = req->len;

    if (!IsValidBlobOffset(offset, key_size, value_size, file_size_,
                           has_footer_)) {
      *req->status = Status::Corruption(
          "Invalid blob offset " + std::to_string(offset) + " for value size " +
          std::to_string(value_size) + " in " + file_reader_->file_name() +
          " (file size " + std::to_string(file_size_) + ")");
      continue;
    }
    if (req->compression != compression_type_) {
      *req->status =
          Status::Corruption("Compression type mismatch when reading a blob");
      continue;
    }

    const uint64_t adjustment =
        read_options.verify_checksums
            ? BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)
            : 0;
    assert(req->offset >= adjustment);
    adjustments.push_back(adjustment);

    FSReadRequest read_req;
    read_req.offset = req->offset - adjustment;
    read_req.len = req->len + adjustment;
    total_len += read_req.len;
    read_reqs.emplace_back(std::move(read_req));
  }

  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, total_len);

  if (read_reqs.empty()) {
    if (bytes_read) {
      *bytes_read = 0;
    }
    return;
  }

  Buffer buf;
  AlignedBuffer direct_io_buffer;

  Status s;
  bool direct_io = file_reader_->use_direct_io();
  if (direct_io) {
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = nullptr;
    }
  } else {
    buf.reset(new char[total_len]);
    std::ptrdiff_t pos = 0;
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = buf.get() + pos;
      pos += read_reqs[i].len;
    }
  }
  TEST_SYNC_POINT("BlobFileReader::MultiGetBlob:ReadFromFile");
  PERF_COUNTER_ADD(blob_read_count, num_blobs);
  PERF_COUNTER_ADD(blob_read_byte, total_len);
  IOOptions opts;
  IODebugContext dbg;
  s = file_reader_->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    s = file_reader_->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                                &direct_io_context, &dbg);
  }
  if (!s.ok()) {
    for (auto& req : read_reqs) {
      req.status.PermitUncheckedError();
    }
    for (auto& blob_req : blob_reqs) {
      BlobReadRequest* const req = blob_req.first;
      assert(req);
      assert(req->status);

      if (!req->status->IsCorruption()) {
        // Avoid overwriting corruption status.
        *req->status = s;
      }
    }
    return;
  }

  assert(s.ok());

  uint64_t total_bytes = 0;
  for (size_t i = 0, j = 0; i < num_blobs; ++i) {
    BlobReadRequest* const req = blob_reqs[i].first;
    assert(req);
    assert(req->user_key);
    assert(req->status);

    if (!req->status->ok()) {
      continue;
    }

    assert(j < read_reqs.size());
    auto& read_req = read_reqs[j++];
    const auto& record_slice = read_req.result;
    if (read_req.status.ok() && record_slice.size() != read_req.len) {
      read_req.status = IOStatus::Corruption(
          "Incomplete blob read from " + file_reader_->file_name() +
          " at offset " + std::to_string(read_req.offset) + ": expected " +
          std::to_string(read_req.len) + " bytes, got " +
          std::to_string(record_slice.size()));
    }

    *req->status = read_req.status;
    if (!req->status->ok()) {
      continue;
    }

    // Verify checksums if enabled
    if (read_options.verify_checksums) {
      *req->status = VerifyBlob(record_slice, *req->user_key, req->len);
      if (!req->status->ok()) {
        continue;
      }
    }

    // Uncompress blob if needed
    Slice value_slice(record_slice.data() + adjustments[j - 1], req->len);
    *req->status = UncompressBlobIfNeeded(
        value_slice, compression_type_, decompressor_.get(), allocator, clock_,
        statistics_, &blob_reqs[i].second);
    if (req->status->ok()) {
      total_bytes += record_slice.size();
    }
  }

  if (bytes_read) {
    *bytes_read = total_bytes;
  }
}

void BlobFileReader::MultiGetBlobRange(
    const ReadOptions& read_options,
    autovector<std::pair<BlobRangeReadRequest*, std::unique_ptr<BlobContents>>>&
        blob_reqs,
    uint64_t* bytes_read) const {
  const size_t num_blobs = blob_reqs.size();
  assert(num_blobs > 0);
  assert(num_blobs <= MultiGetContext::MAX_BATCH_SIZE);

  // Range reads are only issued for uncompressed blob references (a strict
  // sub-range of a compressed record cannot be decompressed in isolation).
  // Version::MultiGetBlobLazy rejects a compressed blob index before it reaches
  // here, so every request's blob index is uncompressed by contract; a
  // compressed blob file therefore means the blob index disagrees with the
  // file. Report that as the compression-type mismatch it is -- exactly like
  // the whole-value MultiGetBlob path -- rather than as a usage error. (Unlike
  // the single-read path, BlobSource has no per-request compression to check
  // for a range read, so this reader-level check is the range multi path's
  // compression backstop.)
  if (compression_type_ != kNoCompression) {
    for (auto& blob_req : blob_reqs) {
      assert(blob_req.first);
      assert(blob_req.first->status);
      *blob_req.first->status =
          Status::Corruption("Compression type mismatch when reading a blob");
    }
    return;
  }

#ifndef NDEBUG
  for (size_t i = 0; i < num_blobs - 1; ++i) {
    assert(blob_reqs[i].first->offset + blob_reqs[i].first->range_offset <=
           blob_reqs[i + 1].first->offset +
               blob_reqs[i + 1].first->range_offset);
  }
#endif  // !NDEBUG

  std::vector<FSReadRequest> read_reqs;
  uint64_t total_len = 0;
  read_reqs.reserve(num_blobs);
  for (size_t i = 0; i < num_blobs; ++i) {
    BlobRangeReadRequest* const req = blob_reqs[i].first;
    assert(req);
    assert(req->status);

    const size_t key_size = req->user_key.size();
    // Validate that the full value region is within the file (same check
    // GetBlob performs); this guards the sub-range read below.
    if (!IsValidBlobOffset(req->offset, key_size, req->value_size, file_size_,
                           has_footer_)) {
      *req->status = Status::Corruption(
          "Invalid blob offset " + std::to_string(req->offset) +
          " for value size " + std::to_string(req->value_size) + " in " +
          file_reader_->file_name() + " (file size " +
          std::to_string(file_size_) + ")");
      continue;
    }
    // The requested sub-range must lie within the value.
    if (req->range_offset > req->value_size ||
        req->range_length > req->value_size - req->range_offset) {
      *req->status = Status::InvalidArgument(
          "Blob range [" + std::to_string(req->range_offset) + ", +" +
          std::to_string(req->range_length) +
          ") out of bounds for value size " + std::to_string(req->value_size) +
          " in " + file_reader_->file_name());
      continue;
    }

    // Read only the requested bytes, at the value's file position plus the
    // range offset -- no record-header adjustment, no whole-record checksum.
    FSReadRequest read_req;
    read_req.offset = req->offset + req->range_offset;
    read_req.len = req->range_length;
    total_len += read_req.len;
    read_reqs.emplace_back(std::move(read_req));
  }

  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, total_len);

  if (read_reqs.empty()) {
    if (bytes_read) {
      *bytes_read = 0;
    }
    return;
  }

  Buffer buf;
  AlignedBuffer direct_io_buffer;

  Status s;
  bool direct_io = file_reader_->use_direct_io();
  if (direct_io) {
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = nullptr;
    }
  } else {
    buf.reset(new char[total_len]);
    std::ptrdiff_t pos = 0;
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = buf.get() + pos;
      pos += read_reqs[i].len;
    }
  }
  TEST_SYNC_POINT("BlobFileReader::MultiGetBlobRange:ReadFromFile");
  PERF_COUNTER_ADD(blob_read_count, num_blobs);
  PERF_COUNTER_ADD(blob_read_byte, total_len);
  IOOptions opts;
  IODebugContext dbg;
  s = file_reader_->PrepareIOOptions(read_options, opts, &dbg);
  if (s.ok()) {
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    s = file_reader_->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                                &direct_io_context, &dbg);
  }
  if (!s.ok()) {
    // Batch read failed; s is written to every request's status below, so these
    // per-request FS statuses carry nothing extra.
    for (auto& req : read_reqs) {
      req.status.PermitUncheckedError();
    }
    for (auto& blob_req : blob_reqs) {
      BlobRangeReadRequest* const req = blob_req.first;
      assert(req);
      assert(req->status);
      if (!req->status->IsCorruption() && !req->status->IsInvalidArgument()) {
        // Avoid overwriting per-request validation errors.
        *req->status = s;
      }
    }
    return;
  }

  assert(s.ok());

  TEST_SYNC_POINT_CALLBACK("BlobFileReader::MultiGetBlobRange:TamperWithResult",
                           &read_reqs);

  uint64_t total_bytes = 0;
  for (size_t i = 0, j = 0; i < num_blobs; ++i) {
    BlobRangeReadRequest* const req = blob_reqs[i].first;
    assert(req);
    assert(req->status);

    if (!req->status->ok()) {
      continue;
    }

    assert(j < read_reqs.size());
    auto& read_req = read_reqs[j++];
    const Slice& range_slice = read_req.result;
    if (read_req.status.ok() && range_slice.size() != read_req.len) {
      read_req.status = IOStatus::Corruption(
          "Incomplete blob range read from " + file_reader_->file_name() +
          " at offset " + std::to_string(read_req.offset) + ": expected " +
          std::to_string(read_req.len) + " bytes, got " +
          std::to_string(range_slice.size()));
    }

    *req->status = read_req.status;
    if (!req->status->ok()) {
      continue;
    }

    // The blob is uncompressed, so the bytes read are exactly the requested
    // value bytes; copy them into an owned BlobContents (no decompression).
    BlobContentsCreator::Create(&blob_reqs[i].second, /*out_charge=*/nullptr,
                                range_slice, kNoCompression,
                                /*allocator=*/nullptr);
    total_bytes += range_slice.size();
  }

  if (bytes_read) {
    *bytes_read = total_bytes;
  }
}

Status BlobFileReader::VerifyBlob(const Slice& record_slice,
                                  const Slice& user_key, uint64_t value_size) {
  PERF_TIMER_GUARD(blob_checksum_time);

  BlobLogRecord record;

  const Slice header_slice(record_slice.data(), BlobLogRecord::kHeaderSize);

  {
    const Status s = record.DecodeHeaderFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  if (record.key_size != user_key.size()) {
    return Status::Corruption("Key size mismatch when reading blob");
  }

  if (record.value_size != value_size) {
    return Status::Corruption("Value size mismatch when reading blob");
  }

  record.key =
      Slice(record_slice.data() + BlobLogRecord::kHeaderSize, record.key_size);
  if (record.key != user_key) {
    return Status::Corruption("Key mismatch when reading blob");
  }

  record.value = Slice(record.key.data() + record.key_size, value_size);

  {
    TEST_SYNC_POINT_CALLBACK("BlobFileReader::VerifyBlob:CheckBlobCRC",
                             &record);

    const Status s = record.CheckBlobCRC();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status BlobFileReader::UncompressBlobIfNeeded(
    const Slice& value_slice, CompressionType compression_type,
    Decompressor* decompressor, MemoryAllocator* allocator, SystemClock* clock,
    Statistics* statistics, std::unique_ptr<BlobContents>* result) {
  assert(result);

  if (compression_type == kNoCompression) {
    BlobContentsCreator::Create(result, nullptr, value_slice, kNoCompression,
                                allocator);
    return Status::OK();
  }

  assert(decompressor);

  Decompressor::Args args;
  args.compression_type = compression_type;
  args.compressed_data = value_slice;

  Status s = decompressor->ExtractUncompressedSize(args);
  if (!s.ok()) {
    return Status::Corruption(s.ToString());
  }

  CacheAllocationPtr output = AllocateBlock(args.uncompressed_size, allocator);

  {
    PERF_TIMER_GUARD(blob_decompress_time);
    StopWatch stop_watch(clock, statistics, BLOB_DB_DECOMPRESSION_MICROS);
    s = decompressor->DecompressBlock(args, output.get());
  }

  TEST_SYNC_POINT_CALLBACK(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", &s);

  if (!s.ok()) {
    return Status::Corruption(s.ToString());
  }

  result->reset(new BlobContents(std::move(output), args.uncompressed_size));

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
