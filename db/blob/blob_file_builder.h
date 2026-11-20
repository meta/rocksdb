//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cinttypes>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/advanced_compression.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

class VersionSet;
class FileSystem;
class SystemClock;
struct ImmutableOptions;
struct MutableCFOptions;
struct FileOptions;
class BlobFileAddition;
class Status;
class Slice;
class BlobLogWriter;
class IOTracer;
class BlobFileCompletionCallback;

class BlobFileBuilder {
 public:
  BlobFileBuilder(VersionSet* versions, FileSystem* fs,
                  const ImmutableOptions* immutable_options,
                  const MutableCFOptions* mutable_cf_options,
                  const FileOptions* file_options,
                  const WriteOptions* write_options, std::string db_id,
                  std::string db_session_id, int job_id,
                  uint32_t column_family_id,
                  const std::string& column_family_name,
                  Env::WriteLifeTimeHint write_hint,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  BlobFileCompletionCallback* blob_callback,
                  BlobFileCreationReason creation_reason,
                  std::string blob_output_path,
                  std::vector<std::string>* output_file_paths,
                  std::vector<BlobFileAddition>* blob_file_additions);

  BlobFileBuilder(std::function<uint64_t()> file_number_generator,
                  FileSystem* fs, const ImmutableOptions* immutable_options,
                  const MutableCFOptions* mutable_cf_options,
                  const FileOptions* file_options,
                  const WriteOptions* write_options, std::string db_id,
                  std::string db_session_id, int job_id,
                  uint32_t column_family_id,
                  const std::string& column_family_name,
                  Env::WriteLifeTimeHint write_hint,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  BlobFileCompletionCallback* blob_callback,
                  BlobFileCreationReason creation_reason,
                  std::string blob_output_path,
                  std::vector<std::string>* output_file_paths,
                  std::vector<BlobFileAddition>* blob_file_additions);

  BlobFileBuilder(const BlobFileBuilder&) = delete;
  BlobFileBuilder& operator=(const BlobFileBuilder&) = delete;
  BlobFileBuilder(BlobFileBuilder&&) = delete;
  BlobFileBuilder& operator=(BlobFileBuilder&&) = delete;

  ~BlobFileBuilder();

  Status Add(const Slice& key, const Slice& value, std::string* blob_index);
  Status Finish();
  void Abandon(const Status& s);

 private:
  std::string GetBlobFilePath(uint64_t blob_file_number) const;
  bool IsBlobFileOpen() const;
  Status OpenBlobFileIfNeeded();
  Status CompressBlobIfNeeded(Slice* blob,
                              GrowableBuffer* compressed_blob) const;
  Status WriteBlobToFile(const Slice& key, const Slice& blob,
                         uint64_t* blob_file_number, uint64_t* blob_offset);
  Status CloseBlobFile();
  Status CloseBlobFileIfNeeded();

  Status PutBlobIntoCacheIfNeeded(const Slice& blob, uint64_t blob_file_number,
                                  uint64_t blob_offset) const;

  std::function<uint64_t()> file_number_generator_;
  FileSystem* fs_;
  const ImmutableOptions* immutable_options_;
  uint64_t min_blob_size_;
  uint64_t blob_file_size_;
  uint64_t blob_file_writable_file_max_buffer_size_;
  CompressionType blob_compression_type_;
  std::unique_ptr<Compressor> blob_compressor_;
  mutable Compressor::ManagedWorkingArea blob_compressor_wa_;
  PrepopulateBlobCache prepopulate_blob_cache_;
  const FileOptions* file_options_;
  const WriteOptions* write_options_;
  const std::string db_id_;
  const std::string db_session_id_;
  int job_id_;
  uint32_t column_family_id_;
  std::string column_family_name_;
  Env::WriteLifeTimeHint write_hint_;
  std::shared_ptr<IOTracer> io_tracer_;
  BlobFileCompletionCallback* blob_callback_;
  BlobFileCreationReason creation_reason_;
  const std::string blob_output_path_;
  std::vector<std::string>* output_file_paths_;
  std::vector<BlobFileAddition>* blob_file_additions_;
  std::unique_ptr<BlobLogWriter> writer_;
  uint64_t blob_count_;
  uint64_t blob_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE
