//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <functional>
#include <limits>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/stackable_db.h"

namespace ROCKSDB_NAMESPACE {

namespace blob_db {

// A wrapped database which puts values of KV pairs in a separate log
// and store location to the log in the underlying DB.
//
// The factory needs to be moved to include/rocksdb/utilities to allow
// users to use blob DB.

constexpr uint64_t kNoExpiration = std::numeric_limits<uint64_t>::max();
// Name of the directory under the base DB where blobs will be stored.
constexpr const char* kBlobDirName = "blob_dir";

// Allows OS to incrementally sync blob files to disk for every
// kBytesPerSync bytes written.
constexpr uint64_t kBytesPerSync = 512 * 1024;

struct BlobDBOptions {
  // Maximum size of the database (including SST files and blob files).
  //
  // Default: 0 (no limits)
  uint64_t max_db_size = 0;

  // a new bucket is opened, for ttl_range. So if ttl_range is 600seconds
  // (10 minutes), and the first bucket starts at 1471542000
  // then the blob buckets will be
  // first bucket is 1471542000 - 1471542600
  // second bucket is 1471542600 - 1471543200
  // and so on
  uint64_t ttl_range_secs = 3600;

  // the target size of each blob file. File will become immutable
  // after it exceeds that size
  uint64_t blob_file_size = 256 * 1024 * 1024;

  // If enabled, BlobDB cleans up stale blobs in non-TTL files during compaction
  // by rewriting the remaining live blobs to new files.
  bool enable_garbage_collection = false;

  // Disable all background job. Used for test only.
  bool disable_background_tasks = false;

  void Dump(Logger* log) const;
};

class BlobDB : public StackableDB {
 public:
  using ROCKSDB_NAMESPACE::StackableDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override = 0;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& value) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    return Put(options, key, value);
  }

  using ROCKSDB_NAMESPACE::StackableDB::Delete;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    assert(db_ != nullptr);
    return db_->Delete(options, column_family, key);
  }

  virtual Status PutWithTTL(const WriteOptions& options, const Slice& key,
                            const Slice& value, uint64_t ttl) = 0;
  virtual Status PutWithTTL(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value, uint64_t ttl) {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    return PutWithTTL(options, key, value, ttl);
  }

  using ROCKSDB_NAMESPACE::StackableDB::Get;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value,
             std::string* timestamp) override = 0;

  // Get value and expiration.
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value, uint64_t* expiration) = 0;
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     PinnableSlice* value, uint64_t* expiration) {
    return Get(options, DefaultColumnFamily(), key, value, expiration);
  }

  using ROCKSDB_NAMESPACE::StackableDB::SingleDelete;
  Status SingleDelete(const WriteOptions& /*wopts*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  using ROCKSDB_NAMESPACE::StackableDB::Merge;
  Status Merge(const WriteOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  Status Write(const WriteOptions& opts, WriteBatch* updates) override = 0;

  using ROCKSDB_NAMESPACE::StackableDB::NewIterator;
  Iterator* NewIterator(const ReadOptions& options) override = 0;
  Iterator* NewIterator(const ReadOptions& options,
                        ColumnFamilyHandle* column_family) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      // Blob DB doesn't support non-default column family.
      return nullptr;
    }
    return NewIterator(options);
  }

  Status CompactFiles(
      const CompactionOptions& compact_options,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override = 0;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }

    return CompactFiles(compact_options, input_file_names, output_level,
                        output_path_id, output_file_names, compaction_job_info);
  }

  using ROCKSDB_NAMESPACE::StackableDB::Close;
  Status Close() override = 0;

  // Opening blob db.
  static Status Open(const Options& options, const BlobDBOptions& bdb_options,
                     const std::string& dbname, BlobDB** blob_db);

  static Status Open(const DBOptions& db_options,
                     const BlobDBOptions& bdb_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     BlobDB** blob_db);

  ~BlobDB() override = default;

 protected:
  explicit BlobDB();
};

// Destroy the content of the database.
Status DestroyBlobDB(const std::string& dbname, const Options& options,
                     const BlobDBOptions& bdb_options);

}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
