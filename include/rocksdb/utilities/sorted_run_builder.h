//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Comparator;
class Iterator;
class TableFactory;
class WriteBatch;

struct SortedRunBuilderOptions {
  // Directory for temporary DB storage. Required.
  std::string temp_dir;

  // Comparator for key ordering. Default: BytewiseComparator.
  const Comparator* comparator = nullptr;

  // Target SST file size for output files.
  // Default: 64MB (same as default target_file_size_base).
  uint64_t target_file_size_bytes = 64 * 1024 * 1024;

  // Compression type for output SST files.
  // Default: kNoCompression (always available). Set to kLZ4Compression,
  // kSnappyCompression, kZSTD, etc. if the desired library is linked with
  // your binary.
  CompressionType compression = kNoCompression;

  // Max number of background compaction threads.
  // Higher = faster sort for large datasets.
  int max_compaction_threads = 4;

  // Memory budget for memtables (write_buffer_size).
  // Controls how much data is sorted in-memory before flushing.
  size_t write_buffer_size = 64 * 1024 * 1024;

  // Max number of write buffers (memtables) before stalling writes.
  int max_write_buffer_number = 4;

  // Block-based table options for output files (optional).
  // Caller can set block_size, filter_policy, etc.
  std::shared_ptr<TableFactory> table_factory = nullptr;

  // If true, keep the temporary DB after Finish() so files can be
  // linked/moved rather than copied. Caller must call Cleanup() explicitly.
  bool keep_temp_db = false;
};

// SortedRunBuilder is a utility that uses RocksDB internally to sort
// arbitrary key-value data and produce sorted SST files. This allows
// callers to use RocksDB as an external sort engine without managing
// a DB instance directly.
//
// Usage:
//   SortedRunBuilderOptions opts;
//   opts.temp_dir = "/tmp/sort_work";
//   std::unique_ptr<SortedRunBuilder> builder;
//   SortedRunBuilder::Create(opts, &builder);
//
//   builder->Add("zebra", "v1");
//   builder->Add("apple", "v2");
//   builder->Finish();
//
//   // Get sorted SST files suitable for IngestExternalFile
//   auto files = builder->GetOutputFiles();
//
// The output SST files have seqno=0 and can be ingested into another
// DB via IngestExternalFile. Required ingestion settings:
//   ingest_opts.allow_db_generated_files = true;
//   ingest_opts.snapshot_consistency = false;
//   ingest_opts.allow_blocking_flush = false;
//
// Thread safety: Add() and AddBatch() are thread-safe. All other methods
// must not be called concurrently with any other method.
class SortedRunBuilder {
 public:
  static Status Create(const SortedRunBuilderOptions& options,
                       std::unique_ptr<SortedRunBuilder>* result);

  virtual ~SortedRunBuilder();

  // Add a key-value pair. Keys can be added in ANY order.
  // Thread-safe: multiple threads may call Add() concurrently.
  virtual Status Add(const Slice& key, const Slice& value) = 0;

  // Add a batch of key-value pairs for efficiency. Only Put operations
  // are expected; Delete/Merge in the batch will be written but may
  // produce unexpected results after compaction.
  // Thread-safe: multiple threads may call AddBatch() concurrently.
  virtual Status AddBatch(WriteBatch* batch) = 0;

  // Finalize: flush remaining memtables, run compaction to produce
  // sorted SST files with seqno=0.
  //
  // After Finish():
  // - GetOutputFiles() returns paths to sorted, non-overlapping SST files
  // - These files can be ingested via IngestExternalFile (see class
  //   comment for required ingestion settings)
  // - NewIterator() can be used to read sorted output
  virtual Status Finish() = 0;

  // Get paths to the sorted output SST files.
  // Only valid after Finish() succeeds.
  virtual const std::vector<std::string>& GetOutputFiles() const = 0;

  // Get total number of unique entries.
  // Before Finish(): approximate count of Add()/AddBatch() calls (may
  //   overcount if duplicate keys were added).
  // After Finish(): exact number of entries in the output SST files
  //   (duplicates resolved).
  virtual uint64_t GetNumEntries() const = 0;

  // Get total data size.
  // Before Finish(): approximate (Add() tracks raw key+value sizes,
  //   AddBatch() tracks serialized batch data size; may overcount
  //   due to duplicate keys).
  // After Finish(): total size of the output SST files on disk.
  virtual uint64_t GetDataSize() const = 0;

  // Create an iterator over all sorted output.
  // Only valid after Finish() succeeds.
  // Caller owns the returned iterator.
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;

  // Clean up temporary DB and files.
  // Called automatically by destructor unless keep_temp_db=true.
  // WARNING: if keep_temp_db=true, caller MUST call Cleanup() explicitly
  // to avoid leaking temporary files.
  virtual Status Cleanup() = 0;

 protected:
  SortedRunBuilder() = default;
};

}  // namespace ROCKSDB_NAMESPACE
