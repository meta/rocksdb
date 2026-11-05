
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/blob_db/blob_file.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <memory>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "file/filename.h"
#include "file/readahead_raf.h"
#include "logging/logging.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace ROCKSDB_NAMESPACE::blob_db {

BlobFile::BlobFile(const BlobDBImpl* p, const std::string& bdir, uint64_t fn,
                   Logger* info_log)
    : parent_(p), path_to_dir_(bdir), file_number_(fn), info_log_(info_log) {}

BlobFile::BlobFile(const BlobDBImpl* p, const std::string& bdir, uint64_t fn,
                   Logger* info_log, uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range)
    : parent_(p),
      path_to_dir_(bdir),
      file_number_(fn),
      info_log_(info_log),
      column_family_id_(column_family_id),
      has_ttl_(has_ttl),
      expiration_range_(expiration_range),
      header_(column_family_id, kNoCompression, has_ttl, expiration_range),
      header_valid_(true) {}

BlobFile::~BlobFile() {
  if (obsolete_) {
    std::string pn(PathName());
    Status s = Env::Default()->DeleteFile(PathName());
    if (!s.ok()) {
      // ROCKS_LOG_INFO(db_options_.info_log,
      // "File could not be deleted %s", pn.c_str());
    }
  }
}

std::string BlobFile::PathName() const {
  return BlobFileName(path_to_dir_, file_number_);
}

std::string BlobFile::DumpState() const {
  char str[1000];
  snprintf(
      str, sizeof(str),
      "path: %s fn: %" PRIu64 " blob_count: %" PRIu64 " file_size: %" PRIu64
      " closed: %d obsolete: %d expiration_range: (%" PRIu64 ", %" PRIu64
      "), writer: %d reader: %d",
      path_to_dir_.c_str(), file_number_, blob_count_.load(), file_size_.load(),
      closed_.load(), obsolete_.load(), expiration_range_.first,
      expiration_range_.second, (!!log_writer_), (!!ra_file_reader_));
  return str;
}

void BlobFile::MarkObsolete(SequenceNumber sequence) {
  assert(Immutable());
  obsolete_sequence_ = sequence;
  obsolete_.store(true);
}

Status BlobFile::WriteFooterAndCloseLocked(const WriteOptions& write_options,
                                           SequenceNumber sequence) {
  BlobLogFooter footer;
  footer.blob_count = blob_count_;
  if (HasTTL()) {
    footer.expiration_range = expiration_range_;
  }

  // this will close the file and reset the Writable File Pointer.
  Status s =
      log_writer_->LegacyAppendFooterAndClose(write_options, footer,
                                              /* checksum_method */ nullptr,
                                              /* checksum_value */ nullptr);
  if (s.ok()) {
    closed_ = true;
    immutable_sequence_ = sequence;
    file_size_ += BlobLogFooter::kSize;
  }
  // delete the sequential writer
  log_writer_.reset();
  return s;
}

Status BlobFile::ReadFooter(BlobLogFooter* bf) {
  if (file_size_ < (BlobLogHeader::kSize + BlobLogFooter::kSize)) {
    return Status::IOError("File does not have footer", PathName());
  }

  uint64_t footer_offset = file_size_ - BlobLogFooter::kSize;
  // assume that ra_file_reader_ is valid before we enter this
  assert(ra_file_reader_);

  Slice result;
  std::string buf;
  AlignedBuffer direct_io_buffer;
  Status s;
  FSReadRequest read_req;
  read_req.offset = footer_offset;
  read_req.len = BlobLogFooter::kSize;
  // TODO: rate limit reading footers from blob files.
  if (ra_file_reader_->use_direct_io()) {
    read_req.scratch = nullptr;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    ra_file_reader_->Read(IOOptions(), &read_req, &direct_io_context);
  } else {
    buf.reserve(BlobLogFooter::kSize + 10);
    read_req.scratch = buf.data();
    ra_file_reader_->Read(IOOptions(), &read_req);
  }
  s = std::move(read_req.status);
  if (!s.ok()) {
    return s;
  }
  result = read_req.result;
  if (result.size() != BlobLogFooter::kSize) {
    // should not happen
    return Status::IOError("EOF reached before footer");
  }

  s = bf->DecodeFrom(result);
  return s;
}

Status BlobFile::SetFromFooterLocked(const BlobLogFooter& footer) {
  blob_count_ = footer.blob_count;
  expiration_range_ = footer.expiration_range;
  closed_ = true;
  return Status::OK();
}

Status BlobFile::Fsync(const WriteOptions& write_options) {
  Status s;
  if (log_writer_.get()) {
    s = log_writer_->Sync(write_options);
  }
  return s;
}

void BlobFile::CloseRandomAccessLocked() {
  ra_file_reader_.reset();
  last_access_ = -1;
}

Status BlobFile::GetReader(Env* env, const FileOptions& file_options,
                           std::shared_ptr<RandomAccessFileReader>* reader,
                           bool* fresh_open) {
  assert(reader != nullptr);
  assert(fresh_open != nullptr);
  *fresh_open = false;
  int64_t current_time = 0;
  if (env->GetCurrentTime(&current_time).ok()) {
    last_access_.store(current_time);
  }
  Status s;

  {
    ReadLock lockbfile_r(&mutex_);
    if (ra_file_reader_) {
      *reader = ra_file_reader_;
      return s;
    }
  }

  WriteLock lockbfile_w(&mutex_);
  // Double check.
  if (ra_file_reader_) {
    *reader = ra_file_reader_;
    return s;
  }

  std::unique_ptr<FSRandomAccessFile> rfile;
  s = env->GetFileSystem()->NewRandomAccessFile(PathName(), file_options,
                                                &rfile, nullptr);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to open blob file for random-read: %s status: '%s'"
                    " exists: '%s'",
                    PathName().c_str(), s.ToString().c_str(),
                    env->FileExists(PathName()).ToString().c_str());
    return s;
  }

  ra_file_reader_ =
      std::make_shared<RandomAccessFileReader>(std::move(rfile), PathName());
  *reader = ra_file_reader_;
  *fresh_open = true;
  return s;
}

Status BlobFile::ReadMetadata(const std::shared_ptr<FileSystem>& fs,
                              const FileOptions& file_options) {
  assert(Immutable());
  // Get file size.
  uint64_t file_size = 0;
  Status s =
      fs->GetFileSize(PathName(), file_options.io_options, &file_size, nullptr);
  if (s.ok()) {
    file_size_ = file_size;
  } else {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to get size of blob file %" PRIu64 ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  if (file_size < BlobLogHeader::kSize) {
    ROCKS_LOG_ERROR(
        info_log_, "Incomplete blob file blob file %" PRIu64 ", size: %" PRIu64,
        file_number_, file_size);
    return Status::Corruption("Incomplete blob file header.");
  }

  // Create file reader.
  std::unique_ptr<RandomAccessFileReader> file_reader;
  s = RandomAccessFileReader::Create(fs, PathName(), file_options, &file_reader,
                                     nullptr);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to open blob file %" PRIu64 ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }

  // Read file header.
  std::string header_buf;
  AlignedBuffer direct_io_buffer;
  Slice header_slice;
  FSReadRequest header_req;
  header_req.offset = 0;
  header_req.len = BlobLogHeader::kSize;
  // TODO: rate limit reading headers from blob files.
  if (file_reader->use_direct_io()) {
    header_req.scratch = nullptr;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    file_reader->Read(IOOptions(), &header_req, &direct_io_context);
  } else {
    header_buf.reserve(BlobLogHeader::kSize);
    header_req.scratch = header_buf.data();
    file_reader->Read(IOOptions(), &header_req);
  }
  s = std::move(header_req.status);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        info_log_, "Failed to read header of blob file %" PRIu64 ", status: %s",
        file_number_, s.ToString().c_str());
    return s;
  }
  header_slice = header_req.result;
  BlobLogHeader header;
  s = header.DecodeFrom(header_slice);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to decode header of blob file %" PRIu64
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  column_family_id_ = header.column_family_id;
  has_ttl_ = header.has_ttl;
  if (has_ttl_) {
    expiration_range_ = header.expiration_range;
  }
  header_valid_ = true;

  // Read file footer.
  if (file_size_ < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    // OK not to have footer.
    assert(!footer_valid_);
    return Status::OK();
  }
  std::string footer_buf;
  Slice footer_slice;
  FSReadRequest footer_req;
  footer_req.offset = file_size - BlobLogFooter::kSize;
  footer_req.len = BlobLogFooter::kSize;
  // TODO: rate limit reading footers from blob files.
  if (file_reader->use_direct_io()) {
    footer_req.scratch = nullptr;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    file_reader->Read(IOOptions(), &footer_req, &direct_io_context);
  } else {
    footer_buf.reserve(BlobLogFooter::kSize);
    footer_req.scratch = footer_buf.data();
    file_reader->Read(IOOptions(), &footer_req);
  }
  s = std::move(footer_req.status);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        info_log_, "Failed to read footer of blob file %" PRIu64 ", status: %s",
        file_number_, s.ToString().c_str());
    return s;
  }
  footer_slice = footer_req.result;
  BlobLogFooter footer;
  s = footer.DecodeFrom(footer_slice);
  if (!s.ok()) {
    // OK not to have footer.
    assert(!footer_valid_);
    return Status::OK();
  }
  blob_count_ = footer.blob_count;
  if (has_ttl_) {
    assert(header.expiration_range.first <= footer.expiration_range.first);
    assert(header.expiration_range.second >= footer.expiration_range.second);
    expiration_range_ = footer.expiration_range;
  }
  footer_valid_ = true;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE::blob_db
