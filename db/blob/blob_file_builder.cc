//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cassert>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_completion_callback.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/blob/blob_source.h"
#include "db/event_helpers.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

BlobFileBuilder::BlobFileBuilder(
    VersionSet* versions, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    const WriteOptions* write_options, std::string db_id,
    std::string db_session_id, int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCompletionCallback* blob_callback,
    BlobFileCreationReason creation_reason, std::string blob_output_path,
    std::vector<std::string>* output_file_paths,
    std::vector<BlobFileAddition>* blob_file_additions)
    : BlobFileBuilder([versions]() { return versions->NewFileNumber(); }, fs,
                      immutable_options, mutable_cf_options, file_options,
                      write_options, std::move(db_id), std::move(db_session_id),
                      job_id, column_family_id, column_family_name, write_hint,
                      io_tracer, blob_callback, creation_reason,
                      std::move(blob_output_path), output_file_paths,
                      blob_file_additions) {}

BlobFileBuilder::BlobFileBuilder(
    std::function<uint64_t()> file_number_generator, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    const WriteOptions* write_options, std::string db_id,
    std::string db_session_id, int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCompletionCallback* blob_callback,
    BlobFileCreationReason creation_reason, std::string blob_output_path,
    std::vector<std::string>* output_file_paths,
    std::vector<BlobFileAddition>* blob_file_additions)
    : file_number_generator_(std::move(file_number_generator)),
      fs_(fs),
      immutable_options_(immutable_options),
      min_blob_size_(mutable_cf_options->min_blob_size),
      blob_file_size_(mutable_cf_options->blob_file_size),
      blob_file_writable_file_max_buffer_size_(
          mutable_cf_options->blob_file_writable_file_max_buffer_size),
      blob_compression_type_(mutable_cf_options->blob_compression_type),
      // TODO with schema change: support custom compression manager and options
      // such as max_compressed_bytes_per_kb
      // NOTE: returns nullptr for kNoCompression
      blob_compressor_(GetBuiltinV2CompressionManager()->GetCompressor(
          mutable_cf_options->blob_compression_opts, blob_compression_type_)),
      blob_compressor_wa_(blob_compressor_
                              ? blob_compressor_->ObtainWorkingArea()
                              : Compressor::ManagedWorkingArea{}),
      prepopulate_blob_cache_(mutable_cf_options->prepopulate_blob_cache),
      file_options_(file_options),
      write_options_(write_options),
      db_id_(std::move(db_id)),
      db_session_id_(std::move(db_session_id)),
      job_id_(job_id),
      column_family_id_(column_family_id),
      column_family_name_(column_family_name),
      write_hint_(write_hint),
      io_tracer_(io_tracer),
      blob_callback_(blob_callback),
      creation_reason_(creation_reason),
      blob_output_path_(std::move(blob_output_path)),
      output_file_paths_(output_file_paths),
      blob_file_additions_(blob_file_additions),
      blob_count_(0),
      blob_bytes_(0) {
  assert(file_number_generator_);
  assert(fs_);
  assert(immutable_options_);
  assert(file_options_);
  assert(write_options_);
  assert(output_file_paths_);
  assert(output_file_paths_->empty());
  assert(blob_file_additions_);
  // Resumable compactions restore finalized additions before creating a new
  // builder. Any newly generated blob files are appended to that state.
}

BlobFileBuilder::~BlobFileBuilder() = default;

std::string BlobFileBuilder::GetBlobFilePath(uint64_t blob_file_number) const {
  return BlobFileName(blob_output_path_, blob_file_number);
}

Status BlobFileBuilder::Add(const Slice& key, const Slice& value,
                            std::string* blob_index) {
  assert(blob_index);
  assert(blob_index->empty());

  if (value.empty() || value.size() < min_blob_size_) {
    return Status::OK();
  }

  {
    const Status s = OpenBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  Slice blob = value;
  GrowableBuffer compressed_blob;

  {
    const Status s = CompressBlobIfNeeded(&blob, &compressed_blob);
    if (!s.ok()) {
      return s;
    }
  }

  uint64_t blob_file_number = 0;
  uint64_t blob_offset = 0;

  {
    const Status s =
        WriteBlobToFile(key, blob, &blob_file_number, &blob_offset);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = CloseBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s =
        PutBlobIntoCacheIfNeeded(value, blob_file_number, blob_offset);
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_options_->info_log,
                     "Failed to pre-populate the blob into blob cache: %s",
                     s.ToString().c_str());
    }
  }

  BlobIndex::EncodeBlob(blob_index, blob_file_number, blob_offset, blob.size(),
                        blob_compression_type_);

  return Status::OK();
}

Status BlobFileBuilder::Finish() {
  if (!IsBlobFileOpen()) {
    return Status::OK();
  }

  return CloseBlobFile();
}

bool BlobFileBuilder::IsBlobFileOpen() const { return !!writer_; }

Status BlobFileBuilder::OpenBlobFileIfNeeded() {
  if (IsBlobFileOpen()) {
    return Status::OK();
  }

  assert(!blob_count_);
  assert(!blob_bytes_);

  assert(file_number_generator_);
  const uint64_t blob_file_number = file_number_generator_();
  if (blob_file_number == kInvalidBlobFileNumber) {
    return Status::Incomplete("Blob file number range exhausted");
  }

  std::string blob_file_path = GetBlobFilePath(blob_file_number);

  if (blob_callback_) {
    blob_callback_->OnBlobFileCreationStarted(
        blob_file_path, column_family_name_, job_id_, creation_reason_);
  }

  std::unique_ptr<FSWritableFile> file;
  FileOptions fo_copy;
  {
    assert(file_options_);
    fo_copy = *file_options_;
    fo_copy.open_contract = FileOpenContract::kNoReopenForWrite |
                            FileOpenContract::kNoReadersWhileOpenForWrite;
    fo_copy.write_hint = write_hint_;
    if (blob_file_writable_file_max_buffer_size_ > 0) {
      fo_copy.writable_file_max_buffer_size =
          blob_file_writable_file_max_buffer_size_;
    }
    Status s = NewWritableFile(fs_, blob_file_path, &file, fo_copy);

    TEST_SYNC_POINT_CALLBACK(
        "BlobFileBuilder::OpenBlobFileIfNeeded:NewWritableFile", &s);

    if (!s.ok()) {
      return s;
    }
  }

  // Note: blob files get added to output_file_paths_ right after the open, so
  // they can be cleaned up upon failure. Contrast this with
  // blob_file_additions_, which only contains successfully written files.
  assert(output_file_paths_);
  output_file_paths_->emplace_back(std::move(blob_file_path));

  assert(file);
  file->SetIOPriority(write_options_->rate_limiter_priority);
  // Subsequent attempts to override the hint via SetWriteLifeTimeHint
  // with the very same value will be ignored by the fs.
  file->SetWriteLifeTimeHint(fo_copy.write_hint);
  FileTypeSet tmp_set = immutable_options_->checksum_handoff_file_types;
  Statistics* const statistics = immutable_options_->stats;
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), output_file_paths_->back(), fo_copy,
      immutable_options_->clock, io_tracer_, statistics,
      Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS, immutable_options_->listeners,
      immutable_options_->file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kBlobFile), false));

  constexpr bool do_flush = false;

  std::unique_ptr<BlobLogWriter> blob_log_writer(new BlobLogWriter(
      std::move(file_writer), immutable_options_->clock, statistics,
      blob_file_number, immutable_options_->use_fsync, do_flush));

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  BlobLogHeader header(column_family_id_, blob_compression_type_, has_ttl,
                       expiration_range);

  {
    Status s = blob_log_writer->WriteHeader(*write_options_, header);

    TEST_SYNC_POINT_CALLBACK(
        "BlobFileBuilder::OpenBlobFileIfNeeded:WriteHeader", &s);

    if (!s.ok()) {
      return s;
    }
  }

  writer_ = std::move(blob_log_writer);

  assert(IsBlobFileOpen());

  return Status::OK();
}

Status BlobFileBuilder::CompressBlobIfNeeded(
    Slice* blob, GrowableBuffer* compressed_blob) const {
  assert(blob);
  assert(compressed_blob);
  assert(compressed_blob->empty());
  assert(immutable_options_);

  if (!blob_compressor_) {
    assert(blob_compression_type_ == kNoCompression);
    return Status::OK();
  }
  assert(blob_compression_type_ != kNoCompression);

  // WART: always stored as compressed even when that increases the size.

  Status s;
  StopWatch stop_watch(immutable_options_->clock, immutable_options_->stats,
                       BLOB_DB_COMPRESSION_MICROS);
  s = LegacyForceBuiltinCompression(*blob_compressor_, &blob_compressor_wa_,
                                    *blob, compressed_blob);
  if (!s.ok()) {
    return s;
  }

  *blob = Slice(*compressed_blob);

  return Status::OK();
}

Status BlobFileBuilder::WriteBlobToFile(const Slice& key, const Slice& blob,
                                        uint64_t* blob_file_number,
                                        uint64_t* blob_offset) {
  assert(IsBlobFileOpen());
  assert(blob_file_number);
  assert(blob_offset);

  uint64_t key_offset = 0;

  Status s =
      writer_->AddRecord(*write_options_, key, blob, &key_offset, blob_offset);

  TEST_SYNC_POINT_CALLBACK("BlobFileBuilder::WriteBlobToFile:AddRecord", &s);

  if (!s.ok()) {
    return s;
  }

  *blob_file_number = writer_->get_log_number();

  ++blob_count_;
  blob_bytes_ += BlobLogRecord::kHeaderSize + key.size() + blob.size();

  return Status::OK();
}

Status BlobFileBuilder::CloseBlobFile() {
  assert(IsBlobFileOpen());

  BlobLogFooter footer;
  footer.blob_count = blob_count_;

  std::string checksum_method;
  std::string checksum_value;

  Status s = writer_->AppendFooter(*write_options_, footer, &checksum_method,
                                   &checksum_value);

  TEST_SYNC_POINT_CALLBACK("BlobFileBuilder::WriteBlobToFile:AppendFooter", &s);

  if (!s.ok()) {
    return s;
  }

  const uint64_t blob_file_number = writer_->get_log_number();

  if (blob_callback_) {
    s = blob_callback_->OnBlobFileCompleted(
        GetBlobFilePath(blob_file_number), column_family_name_, job_id_,
        blob_file_number, creation_reason_, s, checksum_value, checksum_method,
        blob_count_, blob_bytes_, /*set_bg_error_on_space_limit=*/true);
  }

  assert(blob_file_additions_);
  blob_file_additions_->emplace_back(blob_file_number, blob_count_, blob_bytes_,
                                     std::move(checksum_method),
                                     std::move(checksum_value));

  assert(immutable_options_);
  ROCKS_LOG_INFO(immutable_options_->logger,
                 "[%s] [JOB %d] Generated blob file #%" PRIu64 ": %" PRIu64
                 " total blobs, %" PRIu64 " total bytes",
                 column_family_name_.c_str(), job_id_, blob_file_number,
                 blob_count_, blob_bytes_);

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;

  return s;
}

Status BlobFileBuilder::CloseBlobFileIfNeeded() {
  assert(IsBlobFileOpen());

  const WritableFileWriter* const file_writer = writer_->file();
  assert(file_writer);

  if (file_writer->GetFileSize() < blob_file_size_) {
    return Status::OK();
  }

  return CloseBlobFile();
}

void BlobFileBuilder::Abandon(const Status& s) {
  if (!IsBlobFileOpen()) {
    return;
  }
  if (blob_callback_) {
    // BlobFileBuilder::Abandon() is called because of error while writing to
    // Blob files. So we can ignore the below error.
    const uint64_t blob_file_number = writer_->get_log_number();
    blob_callback_
        ->OnBlobFileCompleted(GetBlobFilePath(blob_file_number),
                              column_family_name_, job_id_, blob_file_number,
                              creation_reason_, s, "", "", blob_count_,
                              blob_bytes_, /*set_bg_error_on_space_limit=*/true)
        .PermitUncheckedError();
  }

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;
}

Status BlobFileBuilder::PutBlobIntoCacheIfNeeded(const Slice& blob,
                                                 uint64_t blob_file_number,
                                                 uint64_t blob_offset) const {
  Status s = Status::OK();

  BlobSource::SharedCacheInterface blob_cache{immutable_options_->blob_cache};
  auto statistics = immutable_options_->statistics.get();
  bool warm_cache =
      prepopulate_blob_cache_ == PrepopulateBlobCache::kFlushOnly &&
      creation_reason_ == BlobFileCreationReason::kFlush;

  if (blob_cache && warm_cache) {
    const OffsetableCacheKey base_cache_key(db_id_, db_session_id_,
                                            blob_file_number);
    const CacheKey cache_key = base_cache_key.WithOffset(blob_offset);
    const Slice key = cache_key.AsSlice();

    const Cache::Priority priority = Cache::Priority::BOTTOM;

    s = blob_cache.InsertSaved(key, blob, nullptr /*context*/, priority,
                               immutable_options_->lowest_used_cache_tier);

    if (s.ok()) {
      RecordTick(statistics, BLOB_DB_CACHE_ADD);
      RecordTick(statistics, BLOB_DB_CACHE_BYTES_WRITE, blob.size());
    } else {
      RecordTick(statistics, BLOB_DB_CACHE_ADD_FAILURES);
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
