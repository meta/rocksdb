//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <string>
#include <tuple>
#include <vector>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_relocation_file.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "file/file_prefetch_buffer.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/embedded_blob_sst.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/multiget_context.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

struct BlobFileReader::RelocationFileState {
  explicit RelocationFileState(uint32_t expected_column_family_id)
      : column_family_id(expected_column_family_id),
        internal_comparator(BytewiseComparator()) {}

  BlockBasedTable* table() const {
    return static_cast_with_check<BlockBasedTable>(table_reader.get());
  }

  uint32_t column_family_id;
  uint64_t tail_size = 0;
  InternalKeyComparator internal_comparator;
  std::unique_ptr<TableReader> table_reader;
  BlobRelocationDirectory directory;
};

namespace {

Status ValidateRelocationBlobReadRequest(const BlobIndex& blob_index,
                                         uint64_t range_offset,
                                         size_t range_length) {
  if (!blob_index.IsIndirect()) {
    return Status::Corruption("Invalid stable blob relocation lookup");
  }
  Status status = blob_index.Validate();
  if (!status.ok()) {
    return status;
  }
  if (blob_index.size() > kMaxSimpleGen2BlobPayloadSize ||
      !CompressionTypeSupported(blob_index.compression())) {
    return Status::Corruption("Unsupported blob relocation payload");
  }
  if (range_length != kWholeBlobLength &&
      blob_index.compression() == kNoCompression &&
      (range_offset > blob_index.size() ||
       range_length > blob_index.size() - range_offset)) {
    return Status::InvalidArgument("Blob relocation range is out of bounds");
  }
  return Status::OK();
}

}  // namespace

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
                 /*skip_footer_size_check=*/skip_footer_validation,
                 /*is_relocation_file=*/false);
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

Status BlobFileReader::CreateRelocationFile(
    const ImmutableOptions& immutable_options, const ReadOptions& read_options,
    const FileOptions& file_options, HistogramImpl* blob_file_read_hist,
    uint32_t column_family_id, uint64_t blob_file_number,
    uint64_t expected_file_size, uint64_t expected_tail_size,
    uint32_t expected_directory_digest,
    const std::string& current_db_session_id,
    const BlockBasedTableOptions& source_table_options,
    CompressionManager* compression_manager,
    uint8_t block_protection_bytes_per_key,
    const std::shared_ptr<IOTracer>& io_tracer,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  BlockBasedTableFactory factory(
      MakeBlobRelocationTableOptions(source_table_options));
  return CreateRelocationFileWithFactory(
      immutable_options, read_options, file_options, blob_file_read_hist,
      column_family_id, blob_file_number, expected_file_size,
      expected_tail_size, expected_directory_digest, current_db_session_id,
      factory, compression_manager, block_protection_bytes_per_key, io_tracer,
      blob_file_reader);
}

Status BlobFileReader::CreateRelocationFileWithFactory(
    const ImmutableOptions& immutable_options, const ReadOptions& read_options,
    const FileOptions& file_options, HistogramImpl* blob_file_read_hist,
    uint32_t column_family_id, uint64_t blob_file_number,
    uint64_t expected_file_size, uint64_t expected_tail_size,
    uint32_t expected_directory_digest,
    const std::string& current_db_session_id, const TableFactory& factory,
    CompressionManager* compression_manager,
    uint8_t block_protection_bytes_per_key,
    const std::shared_ptr<IOTracer>& io_tracer,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  if (blob_file_reader == nullptr || *blob_file_reader != nullptr) {
    return Status::InvalidArgument("Invalid relocation reader output");
  }
  if (blob_file_number == 0 || expected_file_size == 0 ||
      current_db_session_id.empty() || expected_tail_size == 0 ||
      expected_tail_size > expected_file_size) {
    return Status::InvalidArgument("Invalid blob relocation file metadata");
  }

  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  Status status =
      OpenFile(immutable_options, file_options, blob_file_read_hist,
               blob_file_number, io_tracer, &file_size, &file_reader,
               /*skip_footer_size_check=*/false, /*is_relocation_file=*/true);
  if (!status.ok()) {
    return status;
  }
  if (file_size != expected_file_size) {
    return Status::Corruption("Blob relocation file size mismatch");
  }

  Slice prefix;
  Buffer prefix_buffer;
  AlignedBuffer direct_io_buffer;
  status = ReadFromFile(file_reader.get(), read_options, /*read_offset=*/0,
                        kBlobRelocationFilePrefixSize,
                        immutable_options.statistics.get(), &prefix,
                        &prefix_buffer, &direct_io_buffer);
  if (!status.ok()) {
    return status;
  }
  if (prefix !=
      Slice(kBlobRelocationFilePrefix, kBlobRelocationFilePrefixSize)) {
    return Status::Corruption("Blob relocation file prefix mismatch");
  }

  std::unique_ptr<RelocationFileState> relocation_file_state =
      std::make_unique<RelocationFileState>(column_family_id);
  const std::shared_ptr<const SliceTransform> no_prefix_extractor;
  TableReaderOptions table_reader_options(
      immutable_options, no_prefix_extractor, compression_manager, file_options,
      relocation_file_state->internal_comparator,
      block_protection_bytes_per_key,
      /*skip_filters=*/true);
  table_reader_options.cur_file_num = blob_file_number;
  table_reader_options.cur_db_session_id = current_db_session_id;
  table_reader_options.tail_size = expected_tail_size;
  status = factory.NewTableReader(read_options, table_reader_options,
                                  std::move(file_reader), file_size,
                                  &relocation_file_state->table_reader,
                                  /*prefetch_index_and_filter_in_cache=*/true);
  if (!status.ok()) {
    return status;
  }

  const std::shared_ptr<const TableProperties> properties =
      relocation_file_state->table_reader->GetTableProperties();
  if (properties == nullptr) {
    return Status::Corruption("Blob relocation file has no table properties");
  }
  relocation_file_state->tail_size =
      FileMetaData::CalculateTailSize(file_size, *properties);
  if (relocation_file_state->tail_size != expected_tail_size) {
    return Status::Corruption("Blob relocation metadata tail size mismatch");
  }
  if (properties->column_family_id != column_family_id) {
    return Status::Corruption("Blob relocation column family mismatch");
  }
  if (properties->orig_file_number != blob_file_number ||
      properties->db_session_id.empty()) {
    return Status::Corruption("Blob relocation file identity is missing");
  }
  const auto directory_it = properties->user_collected_properties.find(
      kBlobRelocationDirectoryPropertyName);
  const auto stats_it = properties->user_collected_properties.find(
      kEmbeddedBlobSstStatsPropertyName);
  if (directory_it == properties->user_collected_properties.end() ||
      stats_it == properties->user_collected_properties.end()) {
    return Status::Corruption("Blob relocation file metadata is missing");
  }
  status = BlobRelocationDirectory::DecodeFrom(
      directory_it->second, &relocation_file_state->directory);
  if (!status.ok()) {
    return status;
  }
  status = relocation_file_state->directory.ValidateForFileSize(file_size);
  if (!status.ok()) {
    return status;
  }
  if (relocation_file_state->directory.directory_digest() !=
      expected_directory_digest) {
    return Status::Corruption("Blob relocation directory digest mismatch");
  }
  EmbeddedBlobStats stats;
  status = DecodeEmbeddedBlobStats(stats_it->second, &stats);
  if (!status.ok()) {
    return status;
  }
  const uint64_t entry_count = relocation_file_state->directory.total_entries();
  const bool trailer_bytes_overflow =
      entry_count >
      std::numeric_limits<uint64_t>::max() / kSimpleGen2BlobTrailerSize;
  const uint64_t trailer_bytes =
      trailer_bytes_overflow ? 0 : entry_count * kSimpleGen2BlobTrailerSize;
  if (!stats.HasRecords() || stats.blob_count != entry_count ||
      properties->num_entries != entry_count || trailer_bytes_overflow ||
      stats.payload_bytes >
          std::numeric_limits<uint64_t>::max() - trailer_bytes ||
      stats.payload_bytes + trailer_bytes !=
          relocation_file_state->directory.total_record_bytes()) {
    return Status::Corruption("Blob relocation file entry totals mismatch");
  }

  blob_file_reader->reset(new BlobFileReader(file_size, immutable_options.clock,
                                             immutable_options.stats,
                                             std::move(relocation_file_state)));
  return Status::OK();
}

Status BlobFileReader::OpenFile(
    const ImmutableOptions& immutable_options, const FileOptions& file_opts,
    HistogramImpl* blob_file_read_hist, uint64_t blob_file_number,
    const std::shared_ptr<IOTracer>& io_tracer, uint64_t* file_size,
    std::unique_ptr<RandomAccessFileReader>* file_reader,
    bool skip_footer_size_check, bool is_relocation_file) {
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

  if (skip_footer_size_check && !is_relocation_file &&
      reader_file_opts.use_direct_reads) {
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

  if (is_relocation_file &&
      *file_size < kBlobRelocationFilePrefixSize + Footer::kMinEncodedLength) {
    return Status::Corruption("Malformed blob relocation file");
  }
  if (!is_relocation_file && !skip_footer_size_check &&
      *file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    return Status::Corruption("Malformed blob file");
  }
  if (!is_relocation_file && skip_footer_size_check &&
      *file_size < BlobLogHeader::kSize) {
    return Status::Corruption("Malformed blob file");
  }

  if (immutable_options.advise_random_on_open) {
    file->Hint(FSRandomAccessFile::kRandom);
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

BlobFileReader::BlobFileReader(
    uint64_t file_size, SystemClock* clock, Statistics* statistics,
    std::unique_ptr<RelocationFileState> relocation_file_state)
    : file_size_(file_size),
      compression_type_(kNoCompression),
      clock_(clock),
      statistics_(statistics),
      has_footer_(true),
      relocation_file_state_(std::move(relocation_file_state)) {
  assert(relocation_file_state_ != nullptr);
  assert(relocation_file_state_->table_reader != nullptr);
}

BlobFileReader::~BlobFileReader() = default;

uint32_t BlobFileReader::GetRelocationDirectoryDigest() const {
  return relocation_file_state_ == nullptr
             ? 0
             : relocation_file_state_->directory.directory_digest();
}

uint64_t BlobFileReader::GetRelocationTailSize() const {
  return relocation_file_state_ != nullptr ? relocation_file_state_->tail_size
                                           : 0;
}

uint32_t BlobFileReader::GetRelocationColumnFamilyId() const {
  return relocation_file_state_ == nullptr
             ? 0
             : relocation_file_state_->column_family_id;
}

std::unique_ptr<InternalIterator>
BlobFileReader::NewRelocationFileLookupIterator(
    ReadOptions* lookup_options) const {
  assert(relocation_file_state_ != nullptr);
  assert(lookup_options != nullptr);
  lookup_options->iterate_lower_bound = nullptr;
  lookup_options->iterate_upper_bound = nullptr;
  lookup_options->async_io = false;
  lookup_options->readahead_size = 0;
  // Relocation table values are same-file BlobIndexes. kSSTDumpTool is the
  // existing raw-table mode that prevents the normal embedded-value wrapper
  // from resolving them before FindRelocationFileBlobIndex validates them.
  return std::unique_ptr<InternalIterator>(
      relocation_file_state_->table_reader->NewIterator(
          *lookup_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
          /*skip_filters=*/true, TableReaderCaller::kSSTDumpTool,
          /*compaction_readahead_size=*/0,
          /*allow_unprepared_value=*/false));
}

Status BlobFileReader::FindRelocationFileBlobIndex(
    InternalIterator* iterator, const BlobIndex& stable_blob_index,
    BlobIndex* same_file_blob_index) const {
  assert(iterator != nullptr);
  assert(same_file_blob_index != nullptr);
  if (relocation_file_state_ == nullptr) {
    return Status::Corruption("Invalid stable blob relocation lookup");
  }
  Status status = ValidateRelocationBlobReadRequest(
      stable_blob_index, /*range_offset=*/0, kWholeBlobLength);
  if (!status.ok()) {
    return status;
  }
  const BlobRelocationOrigin* origin = relocation_file_state_->directory.Find(
      stable_blob_index.origin_file_number());
  if (origin == nullptr ||
      stable_blob_index.original_value_offset() <
          origin->first_original_offset ||
      stable_blob_index.original_value_offset() >
          origin->last_original_offset) {
    return Status::Corruption("Blob relocation origin is not present");
  }

  const std::string user_key =
      EncodeBlobRelocationKey(stable_blob_index.origin_file_number(),
                              stable_blob_index.original_value_offset());
  const InternalKey lookup_key(user_key, kMaxSequenceNumber, kValueTypeForSeek);
  iterator->Seek(lookup_key.Encode());
  if (!iterator->Valid()) {
    status = iterator->status();
    return status.ok() ? Status::Corruption("Blob relocation entry is missing")
                       : status;
  }

  ParsedInternalKey parsed_key;
  status = ParseInternalKey(iterator->key(), &parsed_key,
                            /*log_err_key=*/false);
  if (!status.ok()) {
    return status;
  }
  if (parsed_key.user_key != Slice(user_key) || parsed_key.sequence != 0 ||
      parsed_key.type != kTypeBlobIndex) {
    return Status::Corruption("Invalid blob relocation entry key");
  }
  status = same_file_blob_index->DecodeFrom(iterator->value());
  if (!status.ok()) {
    return status;
  }
  if (!same_file_blob_index->IsSameFile() ||
      same_file_blob_index->size() != stable_blob_index.size() ||
      same_file_blob_index->compression() != stable_blob_index.compression()) {
    return Status::Corruption("Blob relocation entry metadata mismatch");
  }
  return Status::OK();
}

Status BlobFileReader::GetBlobFromRelocationFile(
    const ReadOptions& read_options, const BlobIndex& blob_index,
    uint64_t range_offset, size_t range_length, PinnableSlice* result,
    uint64_t* bytes_read) const {
  if (result == nullptr || relocation_file_state_ == nullptr) {
    return Status::Corruption("Blob file is not a relocation file");
  }
  Status status =
      ValidateRelocationBlobReadRequest(blob_index, range_offset, range_length);
  if (!status.ok()) {
    return status;
  }
  ReadOptions lookup_options(read_options);
  std::unique_ptr<InternalIterator> iterator =
      NewRelocationFileLookupIterator(&lookup_options);
  BlobIndex same_file_blob_index;
  status = FindRelocationFileBlobIndex(iterator.get(), blob_index,
                                       &same_file_blob_index);
  if (!status.ok()) {
    return status;
  }
  const BlobVerifyPolicy verify_policy =
      range_length != kWholeBlobLength &&
              blob_index.compression() == kNoCompression
          ? BlobVerifyPolicy::kVerifyIfNoAmplification
          : BlobVerifyPolicy::kVerifyIfPresent;
  status = relocation_file_state_->table()->GetSameFileBlob(
      read_options, same_file_blob_index, range_offset, range_length,
      verify_policy, result);
  if (status.ok() && bytes_read != nullptr) {
    *bytes_read = range_length == kWholeBlobLength ||
                          blob_index.compression() != kNoCompression
                      ? blob_index.size() + kSimpleGen2BlobTrailerSize
                      : range_length;
  }
  return status;
}

void BlobFileReader::MultiGetBlobFromRelocationFile(
    const ReadOptions& read_options, size_t num_reads,
    RelocationBlobReadRequest* requests, uint64_t* bytes_read) const {
  if (num_reads == 0) {
    if (bytes_read != nullptr) {
      *bytes_read = 0;
    }
    return;
  }
  assert(requests != nullptr);
  if (bytes_read != nullptr) {
    *bytes_read = 0;
  }
  if (relocation_file_state_ == nullptr) {
    for (size_t i = 0; i < num_reads; ++i) {
      assert(requests[i].status != nullptr);
      *requests[i].status =
          Status::Corruption("Blob file is not a relocation file");
    }
    return;
  }

  std::vector<size_t> lookup_order;
  lookup_order.reserve(num_reads);
  for (size_t i = 0; i < num_reads; ++i) {
    RelocationBlobReadRequest& request = requests[i];
    assert(request.blob_index != nullptr);
    assert(request.result != nullptr);
    assert(request.status != nullptr);
    request.result->Reset();
    *request.status = ValidateRelocationBlobReadRequest(
        *request.blob_index, request.range_offset, request.range_length);
    if (request.status->ok()) {
      lookup_order.push_back(i);
    }
  }
  if (lookup_order.empty()) {
    return;
  }
  std::sort(lookup_order.begin(), lookup_order.end(),
            [requests](size_t lhs, size_t rhs) {
              const BlobIndex& left = *requests[lhs].blob_index;
              const BlobIndex& right = *requests[rhs].blob_index;
              return std::make_tuple(left.origin_file_number(),
                                     left.original_value_offset()) <
                     std::make_tuple(right.origin_file_number(),
                                     right.original_value_offset());
            });

  ReadOptions lookup_options(read_options);
  std::unique_ptr<InternalIterator> iterator =
      NewRelocationFileLookupIterator(&lookup_options);
  std::vector<BlobIndex> same_file_blob_indexes(num_reads);
  std::vector<SameFileBlobReadRequest> same_file_requests;
  same_file_requests.reserve(num_reads);
  for (size_t index : lookup_order) {
    RelocationBlobReadRequest& request = requests[index];
    *request.status = FindRelocationFileBlobIndex(
        iterator.get(), *request.blob_index, &same_file_blob_indexes[index]);
    if (!request.status->ok()) {
      continue;
    }
    const BlobVerifyPolicy verify_policy =
        request.range_length != kWholeBlobLength &&
                request.blob_index->compression() == kNoCompression
            ? BlobVerifyPolicy::kVerifyIfNoAmplification
            : BlobVerifyPolicy::kVerifyIfPresent;
    same_file_requests.push_back(
        {&same_file_blob_indexes[index], request.range_offset,
         request.range_length, verify_policy, request.result, request.status});
  }
  if (!same_file_requests.empty()) {
    relocation_file_state_->table()->MultiGetSameFileBlob(
        read_options, same_file_requests.size(), same_file_requests.data());
  }

  uint64_t total_bytes_read = 0;
  for (size_t i = 0; i < num_reads; ++i) {
    if (!requests[i].status->ok()) {
      continue;
    }
    const BlobIndex& blob_index = *requests[i].blob_index;
    const uint64_t read_size =
        requests[i].range_length == kWholeBlobLength ||
                blob_index.compression() != kNoCompression
            ? blob_index.size() + kSimpleGen2BlobTrailerSize
            : requests[i].range_length;
    if (read_size > std::numeric_limits<uint64_t>::max() - total_bytes_read) {
      *requests[i].status = Status::Corruption("Blob relocation read overflow");
      continue;
    }
    total_bytes_read += read_size;
  }
  if (bytes_read != nullptr) {
    *bytes_read = total_bytes_read;
  }
}

Status BlobFileReader::GetBlob(
    const ReadOptions& read_options, const Slice& user_key, uint64_t offset,
    uint64_t value_size, CompressionType compression_type,
    FilePrefetchBuffer* prefetch_buffer, MemoryAllocator* allocator,
    std::unique_ptr<BlobContents>* result, uint64_t* bytes_read) const {
  return GetBlob(read_options, user_key, offset, value_size, compression_type,
                 prefetch_buffer, allocator, result, bytes_read, nullptr);
}

Status BlobFileReader::GetBlob(
    const ReadOptions& read_options, const Slice& user_key, uint64_t offset,
    uint64_t value_size, CompressionType compression_type,
    FilePrefetchBuffer* prefetch_buffer, MemoryAllocator* allocator,
    std::unique_ptr<BlobContents>* result, uint64_t* bytes_read,
    const BlobIndex* stable_blob_index) const {
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
  if (read_options.verify_checksums && stable_blob_index != nullptr) {
    const Status status = stable_blob_index->VerifyStoredChecksum(value_slice);
    if (!status.ok()) {
      return status;
    }
  }

  {
    const Status s = UncompressBlobContentsIfNeeded(
        value_slice, compression_type, decompressor_.get(), allocator, clock_,
        statistics_, result);
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

Status BlobFileReader::GetStoredBlobForRelocation(
    const ReadOptions& read_options, uint64_t offset,
    uint64_t stored_value_size, CompressionType compression_type,
    uint32_t stable_checksum_version, uint32_t stable_checksum,
    uint64_t max_stored_value_size, uint64_t max_logical_value_size,
    std::string* stored_payload, uint64_t* bytes_read) const {
  Status status;
  StoredBlobRelocationReadRequest request{
      offset,           stored_value_size,
      compression_type, stable_checksum_version,
      stable_checksum,  stored_payload,
      &status};
  MultiGetStoredBlobsForRelocation(read_options, 1, &request,
                                   max_stored_value_size,
                                   max_logical_value_size, bytes_read);
  return status;
}

void BlobFileReader::MultiGetStoredBlobsForRelocation(
    const ReadOptions& read_options, size_t num_requests,
    StoredBlobRelocationReadRequest* requests, uint64_t max_stored_value_size,
    uint64_t max_logical_value_size, uint64_t* bytes_read) const {
  if (bytes_read != nullptr) {
    *bytes_read = 0;
  }
  if (num_requests == 0) {
    return;
  }
  if (requests == nullptr) {
    return;
  }
  assert(num_requests <= MultiGetContext::MAX_BATCH_SIZE);

  std::vector<FSReadRequest> read_requests;
  std::vector<size_t> request_indexes;
  read_requests.reserve(num_requests);
  request_indexes.reserve(num_requests);
  uint64_t total_read_size = 0;
  for (size_t i = 0; i < num_requests; ++i) {
    StoredBlobRelocationReadRequest& request = requests[i];
    if (request.status == nullptr) {
      continue;
    }
    Status& request_status = *request.status;
    request_status = Status::OK();
    if (request.stored_payload != nullptr) {
      request.stored_payload->clear();
    }
    if (IsRelocationFile()) {
      request_status = Status::InvalidArgument(
          "Normal stored-blob read used on a relocation file");
      continue;
    }
    if (!BlobIndex::IsSupportedStableChecksumVersion(
            request.stable_checksum_version)) {
      request_status =
          Status::NotSupported("Unsupported stable blob checksum version");
      continue;
    }
    if (request.stored_value_size > max_stored_value_size) {
      request_status =
          Status::MemoryLimit("Stored blob exceeds relocation read limit");
      continue;
    }
    if (request.stored_value_size > std::numeric_limits<size_t>::max() ||
        !IsValidBlobOffset(request.offset, /*key_size=*/0,
                           request.stored_value_size, file_size_,
                           has_footer_)) {
      request_status =
          Status::Corruption("Invalid stored blob relocation range");
      continue;
    }
    if (request.compression != compression_type_) {
      request_status =
          Status::Corruption("Compression type mismatch when relocating blob");
      continue;
    }
    if (!request_indexes.empty()) {
      assert(requests[request_indexes.back()].offset <= request.offset);
    }
    if (request.stored_value_size >
        std::numeric_limits<uint64_t>::max() - total_read_size) {
      request_status = Status::Corruption("Stored blob read size overflow");
      continue;
    }
    total_read_size += request.stored_value_size;
    FSReadRequest read_request;
    read_request.offset = request.offset;
    read_request.len = static_cast<size_t>(request.stored_value_size);
    read_requests.emplace_back(std::move(read_request));
    request_indexes.push_back(i);
  }

  if (read_requests.empty()) {
    return;
  }
  if (total_read_size > std::numeric_limits<size_t>::max()) {
    for (size_t index : request_indexes) {
      assert(requests[index].status != nullptr);
      Status& request_status = *requests[index].status;
      request_status =
          Status::MemoryLimit("Stored blob read batch is too large");
    }
    return;
  }

  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, total_read_size);
  Buffer buffer;
  AlignedBuffer direct_io_buffer;
  if (file_reader_->use_direct_io()) {
    for (FSReadRequest& request : read_requests) {
      request.scratch = nullptr;
    }
  } else {
    buffer.reset(new char[static_cast<size_t>(total_read_size)]);
    size_t position = 0;
    for (FSReadRequest& request : read_requests) {
      request.scratch = buffer.get() + position;
      position += request.len;
    }
  }

  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT(
      "BlobFileReader::MultiGetStoredBlobsForRelocation:ReadFromFile");
  PERF_COUNTER_ADD(blob_read_count, read_requests.size());
  PERF_COUNTER_ADD(blob_read_byte, total_read_size);
  PERF_TIMER_GUARD(blob_read_time);
  IOOptions io_options;
  IODebugContext debug_context;
  Status status =
      file_reader_->PrepareIOOptions(read_options, io_options, &debug_context);
  if (status.ok()) {
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    status = file_reader_->MultiRead(io_options, read_requests.data(),
                                     read_requests.size(), &direct_io_context,
                                     &debug_context);
  }
  if (!status.ok()) {
    for (FSReadRequest& request : read_requests) {
      request.status.PermitUncheckedError();
    }
    for (size_t index : request_indexes) {
      assert(requests[index].status != nullptr);
      Status& request_status = *requests[index].status;
      request_status = status;
    }
    return;
  }

  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK(
      "BlobFileReader::MultiGetStoredBlobsForRelocation:TamperWithResult",
      &read_requests);
  uint64_t successful_bytes = 0;
  for (size_t i = 0; i < read_requests.size(); ++i) {
    StoredBlobRelocationReadRequest& request = requests[request_indexes[i]];
    assert(request.status != nullptr);
    Status& request_status = *request.status;
    FSReadRequest& read_request = read_requests[i];
    request_status = read_request.status;
    if (!request_status.ok()) {
      continue;
    }
    if (read_request.result.size() != read_request.len) {
      request_status = Status::Corruption("Incomplete stored blob read from " +
                                          file_reader_->file_name());
      continue;
    }
    if (request.stable_checksum_version == BlobIndex::kStableChecksumVersion2) {
      if (BlobIndex::ComputeStoredChecksum(read_request.result,
                                           request.compression) !=
          request.stored_checksum) {
        request_status = Status::Corruption(
            "Stored checksum mismatch while relocating blob");
        continue;
      }
      if (request.stored_payload != nullptr) {
        request.stored_payload->assign(read_request.result.data(),
                                       read_request.result.size());
      }
      successful_bytes += read_request.result.size();
      continue;
    }
    // Only experimental checksum-v1 files need logical-value materialization.
    uint32_t actual_stable_checksum = 0;
    if (request.compression == kNoCompression) {
      if (read_request.result.size() > max_logical_value_size) {
        request_status =
            Status::MemoryLimit("Logical blob exceeds relocation read limit");
        continue;
      }
      actual_stable_checksum =
          crc32c::Value(read_request.result.data(), read_request.result.size());
    } else {
      if (decompressor_ == nullptr) {
        request_status = Status::Corruption("Missing blob decompressor");
        continue;
      }
      Decompressor::Args args;
      args.compression_type = request.compression;
      args.compressed_data = read_request.result;
      Status size_status = decompressor_->ExtractUncompressedSize(args);
      if (!size_status.ok()) {
        request_status = Status::Corruption(size_status.ToString());
        continue;
      }
      if (args.uncompressed_size > max_logical_value_size) {
        request_status =
            Status::MemoryLimit("Logical blob exceeds relocation read limit");
        continue;
      }
      std::unique_ptr<BlobContents> logical_value;
      request_status = UncompressBlobContentsIfNeeded(
          read_request.result, request.compression, decompressor_.get(),
          /*allocator=*/nullptr, clock_, statistics_, &logical_value);
      if (!request_status.ok()) {
        continue;
      }
      if (logical_value == nullptr) {
        request_status = Status::Corruption(
            "Missing logical value while relocating compressed blob");
        continue;
      }
      actual_stable_checksum =
          crc32c::Value(logical_value->data().data(), logical_value->size());
    }
    if (actual_stable_checksum != request.stable_checksum) {
      request_status =
          Status::Corruption("Stable checksum mismatch while relocating blob");
      continue;
    }
    if (request.stored_payload != nullptr) {
      request.stored_payload->assign(read_request.result.data(),
                                     read_request.result.size());
    }
    successful_bytes += read_request.result.size();
  }
  if (bytes_read != nullptr) {
    *bytes_read = successful_bytes;
  }
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
    TEST_SYNC_POINT_CALLBACK("BlobFileReader::MultiGetBlob:Requests",
                             &read_reqs);
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
    *req->status = UncompressBlobContentsIfNeeded(
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
    TEST_SYNC_POINT_CALLBACK("BlobFileReader::MultiGetBlobRange:Requests",
                             &read_reqs);
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

}  // namespace ROCKSDB_NAMESPACE
