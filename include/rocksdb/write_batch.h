// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#pragma once

#include <stdint.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "rocksdb/status.h"
#include "rocksdb/write_batch_base.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class ColumnFamilyHandle;
struct SavePoints;
struct SliceParts;

struct SavePoint {
  size_t size;     // size of rep_
  uint32_t count;  // count of elements in rep_
  uint32_t content_flags;

  SavePoint() : size(0), count(0), content_flags(0) {}

  SavePoint(size_t _size, uint32_t _count, uint32_t _flags)
      : size(_size), count(_count), content_flags(_flags) {}

  void clear() {
    size = 0;
    count = 0;
    content_flags = 0;
  }

  bool is_cleared() const { return (size | count | content_flags) == 0; }
};

class WriteBatch : public WriteBatchBase {
 public:
  explicit WriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : WriteBatch(reserved_bytes, max_bytes, 0, 0) {}

  // `protection_bytes_per_key` is the number of bytes used to store
  // protection information for each key entry. Currently supported values are
  // zero (disabled) and eight.
  explicit WriteBatch(size_t reserved_bytes, size_t max_bytes,
                      size_t protection_bytes_per_key, size_t default_cf_ts_sz);
  ~WriteBatch() override;

  using WriteBatchBase::Put;
  // Store the mapping "key->value" in the database.
  // The following Put(..., const Slice& key, ...) API can also be used when
  // user-defined timestamp is enabled as long as `key` points to a contiguous
  // buffer with timestamp appended after user key. The caller is responsible
  // for setting up the memory buffer pointed to by `key`.
  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;
  Status Put(const Slice& key, const Slice& value) override {
    return Put(nullptr, key, value);
  }
  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& ts, const Slice& value) override;

  // Variant of Put() that gathers output like writev(2).  The key and value
  // that will be written to the database are concatenations of arrays of
  // slices.
  // The following Put(..., const SliceParts& key, ...) API can be used when
  // user-defined timestamp is enabled as long as the timestamp is the last
  // Slice in `key`, a SliceParts (array of Slices). The caller is responsible
  // for setting up the `key` SliceParts object.
  Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value) override;
  Status Put(const SliceParts& key, const SliceParts& value) override {
    return Put(nullptr, key, value);
  }

  using WriteBatchBase::TimedPut;
  // EXPERIMENTAL
  // Stores the mapping "key->value" in the database with the specified write
  // time in the column family. Also see documentation in
  // `WriteBatchBase::TimedPut` for the API's usage and limitations.
  Status TimedPut(ColumnFamilyHandle* column_family, const Slice& key,
                  const Slice& value, uint64_t write_unix_time) override;

  // Store the mapping "key->{column1:value1, column2:value2, ...}" in the
  // column family specified by "column_family". See
  // WriteBatchBase::PutEntity() for the lifetime requirements on `columns`.
  using WriteBatchBase::PutEntity;
  Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns) override;

  // Split and store wide column entities in multiple column families (a.k.a.
  // AttributeGroups)
  Status PutEntity(const Slice& key,
                   const AttributeGroups& attribute_groups) override;

  using WriteBatchBase::Delete;
  // If the database contains a mapping for "key", erase it.  Else do nothing.
  // The following Delete(..., const Slice& key) can be used when user-defined
  // timestamp is enabled as long as `key` points to a contiguous buffer with
  // timestamp appended after user key. The caller is responsible for setting
  // up the memory buffer pointed to by `key`.
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key) override;
  Status Delete(const Slice& key) override { return Delete(nullptr, key); }
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                const Slice& ts) override;

  // variant that takes SliceParts
  // These two variants of Delete(..., const SliceParts& key) can be used when
  // user-defined timestamp is enabled as long as the timestamp is the last
  // Slice in `key`, a SliceParts (array of Slices). The caller is responsible
  // for setting up the `key` SliceParts object.
  Status Delete(ColumnFamilyHandle* column_family,
                const SliceParts& key) override;
  Status Delete(const SliceParts& key) override { return Delete(nullptr, key); }

  using WriteBatchBase::SingleDelete;
  // WriteBatch implementation of DB::SingleDelete().  See db.h.
  Status SingleDelete(ColumnFamilyHandle* column_family,
                      const Slice& key) override;
  Status SingleDelete(const Slice& key) override {
    return SingleDelete(nullptr, key);
  }
  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts) override;

  // variant that takes SliceParts
  Status SingleDelete(ColumnFamilyHandle* column_family,
                      const SliceParts& key) override;
  Status SingleDelete(const SliceParts& key) override {
    return SingleDelete(nullptr, key);
  }

  using WriteBatchBase::DeleteRange;
  // WriteBatch implementation of DB::DeleteRange().  See db.h.
  Status DeleteRange(ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key) override;
  Status DeleteRange(const Slice& begin_key, const Slice& end_key) override {
    return DeleteRange(nullptr, begin_key, end_key);
  }
  // begin_key and end_key should be user keys without timestamp.
  Status DeleteRange(ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key, const Slice& ts) override;

  // variant that takes SliceParts
  Status DeleteRange(ColumnFamilyHandle* column_family,
                     const SliceParts& begin_key,
                     const SliceParts& end_key) override;
  Status DeleteRange(const SliceParts& begin_key,
                     const SliceParts& end_key) override {
    return DeleteRange(nullptr, begin_key, end_key);
  }

  using WriteBatchBase::Merge;
  // Merge "value" with the existing value of "key" in the database.
  // "key->merge(existing, value)"
  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value) override;
  Status Merge(const Slice& key, const Slice& value) override {
    return Merge(nullptr, key, value);
  }
  Status Merge(ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*ts*/, const Slice& /*value*/) override;

  // variant that takes SliceParts
  Status Merge(ColumnFamilyHandle* column_family, const SliceParts& key,
               const SliceParts& value) override;
  Status Merge(const SliceParts& key, const SliceParts& value) override {
    return Merge(nullptr, key, value);
  }

  using WriteBatchBase::PutLogData;
  // Append a blob of arbitrary size to the records in this batch. The blob will
  // be stored in the transaction log but not in any other file. In particular,
  // it will not be persisted to the SST files. When iterating over this
  // WriteBatch, WriteBatch::Handler::LogData will be called with the contents
  // of the blob as it is encountered. Blobs, puts, deletes, and merges will be
  // encountered in the same order in which they were inserted. The blob will
  // NOT consume sequence number(s) and will NOT increase the count of the batch
  //
  // Example application: add timestamps to the transaction log for use in
  // replication.
  Status PutLogData(const Slice& blob) override;

  using WriteBatchBase::Clear;
  // Clear all updates buffered in this batch.
  // Internally, it calls resize() on the string buffer. So allocated memory
  // capacity may not be freed.
  void Clear() override;

  // Records the state of the batch for future calls to RollbackToSavePoint().
  // May be called multiple times to set multiple save points.
  void SetSavePoint() override;

  // Remove all entries in this batch (Put, Merge, Delete, PutLogData) since the
  // most recent call to SetSavePoint() and removes the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  Status RollbackToSavePoint() override;

  // Pop the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  Status PopSavePoint() override;

  // Support for iterating over the contents of a batch.
  // Objects of subclasses of Handler will be used by WriteBatch::Iterate().
  class Handler {
   public:
    virtual ~Handler();
    // All handler functions in this class provide default implementations so
    // we won't break existing clients of Handler on a source code level when
    // adding a new member function.

    // default implementation will just call Put without column family for
    // backwards compatibility. If the column family is not default,
    // the function is noop
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
      if (column_family_id == 0) {
        // Put() historically doesn't return status. We didn't want to be
        // backwards incompatible so we didn't change the return status
        // (this is a public API). We do an ordinary get and return Status::OK()
        Put(key, value);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and PutCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void Put(const Slice& /*key*/, const Slice& /*value*/) {}

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status TimedPutCF(uint32_t /*column_family_id*/,
                              const Slice& /*key*/, const Slice& /*value*/,
                              uint64_t /*write_time*/) {
      return Status::InvalidArgument("TimedPutCF not implemented");
    }

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status PutEntityCF(uint32_t /* column_family_id */,
                               const Slice& /* key */,
                               const Slice& /* entity */) {
      return Status::NotSupported("PutEntityCF not implemented");
    }

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
      if (column_family_id == 0) {
        Delete(key);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and DeleteCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void Delete(const Slice& /*key*/) {}

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) {
      if (column_family_id == 0) {
        SingleDelete(key);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and SingleDeleteCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void SingleDelete(const Slice& /*key*/) {}

    // If user-defined timestamp is enabled, then `begin_key` and `end_key`
    // both include timestamp.
    virtual Status DeleteRangeCF(uint32_t /*column_family_id*/,
                                 const Slice& /*begin_key*/,
                                 const Slice& /*end_key*/) {
      return Status::InvalidArgument("DeleteRangeCF not implemented");
    }

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) {
      if (column_family_id == 0) {
        Merge(key, value);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and MergeCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void Merge(const Slice& /*key*/, const Slice& /*value*/) {}

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status PutBlobIndexCF(uint32_t /*column_family_id*/,
                                  const Slice& /*key*/,
                                  const Slice& /*value*/) {
      return Status::InvalidArgument("PutBlobIndexCF not implemented");
    }

    // The default implementation of LogData does nothing.
    virtual void LogData(const Slice& blob);

    virtual Status MarkBeginPrepare(bool = false) {
      return Status::InvalidArgument("MarkBeginPrepare() handler not defined.");
    }

    virtual Status MarkEndPrepare(const Slice& /*xid*/) {
      return Status::InvalidArgument("MarkEndPrepare() handler not defined.");
    }

    virtual Status MarkNoop(bool /*empty_batch*/) {
      return Status::InvalidArgument("MarkNoop() handler not defined.");
    }

    virtual Status MarkRollback(const Slice& /*xid*/) {
      return Status::InvalidArgument(
          "MarkRollbackPrepare() handler not defined.");
    }

    virtual Status MarkCommit(const Slice& /*xid*/) {
      return Status::InvalidArgument("MarkCommit() handler not defined.");
    }

    virtual Status MarkCommitWithTimestamp(const Slice& /*xid*/,
                                           const Slice& /*commit_ts*/) {
      return Status::InvalidArgument(
          "MarkCommitWithTimestamp() handler not defined.");
    }

    // Continue is called by WriteBatch::Iterate. If it returns false,
    // iteration is halted. Otherwise, it continues iterating. The default
    // implementation always returns true.
    virtual bool Continue();

   protected:
    friend class WriteBatchInternal;
    enum class OptionState {
      kUnknown,
      kDisabled,
      kEnabled,
    };
    virtual OptionState WriteAfterCommit() const {
      return OptionState::kUnknown;
    }
    virtual OptionState WriteBeforePrepare() const {
      return OptionState::kUnknown;
    }
  };
  Status Iterate(Handler* handler) const;

  // Retrieve the serialized version of this batch.
  const std::string& Data() const { return rep_; }

  // Release the serialized data and clear this batch.
  std::string Release();

  // Retrieve data size of the batch.
  size_t GetDataSize() const { return rep_.size(); }

  // Returns the number of updates in the batch
  uint32_t Count() const;

  // Returns true if PutCF will be called during Iterate
  bool HasPut() const;

  // Returns true if TimedPutCF will be called during Iterate
  bool HasTimedPut() const;

  // Returns true if PutEntityCF will be called during Iterate
  bool HasPutEntity() const;

  // Returns true if DeleteCF will be called during Iterate
  bool HasDelete() const;

  // Returns true if SingleDeleteCF will be called during Iterate
  bool HasSingleDelete() const;

  // Returns true if DeleteRangeCF will be called during Iterate
  bool HasDeleteRange() const;

  // Returns true if MergeCF will be called during Iterate
  bool HasMerge() const;

  // Returns true if MarkBeginPrepare will be called during Iterate
  bool HasBeginPrepare() const;

  // Returns true if MarkEndPrepare will be called during Iterate
  bool HasEndPrepare() const;

  // Returns true if MarkCommit will be called during Iterate
  bool HasCommit() const;

  // Returns true if MarkRollback will be called during Iterate
  bool HasRollback() const;

  // Update timestamps of existing entries in the write batch if
  // applicable. If a key is intended for a column family that disables
  // timestamp, then this API won't set the timestamp for this key.
  // This requires that all keys, if enable timestamp, (possibly from multiple
  // column families) in the write batch have timestamps of the same format.
  //
  // ts_sz_func: callable object to obtain the timestamp sizes of column
  // families. If ts_sz_func() accesses data structures, then the caller of this
  // API must guarantee thread-safety. Like other parts of RocksDB, this API is
  // not exception-safe. Therefore, ts_sz_func() must not throw.
  //
  // in: cf, the column family id.
  // ret: timestamp size of the given column family. Return
  //      std::numeric_limits<size_t>::max() indicating "don't know or column
  //      family info not found", this will cause UpdateTimestamps() to fail.
  // size_t ts_sz_func(uint32_t cf);
  Status UpdateTimestamps(const Slice& ts,
                          std::function<size_t(uint32_t /*cf*/)> ts_sz_func);

  // TODO: remove these internal APIs after MyRocks refactor to not directly
  // write to a `WriteBatch` retrieved from `Transaction` via
  // `Transaction::GetWriteBatch`.

  void SetTrackTimestampSize(bool track_timestamp_size) {
    track_timestamp_size_ = track_timestamp_size;
  }

  inline void MaybeTrackTimestampSize(uint32_t column_family_id, size_t ts_sz) {
    if (!track_timestamp_size_) {
      return;
    }
    auto iter = cf_id_to_ts_sz_.find(column_family_id);
    if (iter == cf_id_to_ts_sz_.end()) {
      cf_id_to_ts_sz_.emplace(column_family_id, ts_sz);
    }
  }

  // Return a mapping from column family id to timestamp size of all the column
  // families involved in this WriteBatch.
  const std::unordered_map<uint32_t, size_t>& GetColumnFamilyToTimestampSize() {
    return cf_id_to_ts_sz_;
  }

  // Verify the per-key-value checksums of this write batch.
  // Corruption status will be returned if the verification fails.
  // If this write batch does not have per-key-value checksum,
  // OK status will be returned.
  Status VerifyChecksum() const;

  using WriteBatchBase::GetWriteBatch;
  WriteBatch* GetWriteBatch() override { return this; }

  // Constructor with a serialized string object
  explicit WriteBatch(const std::string& rep);
  explicit WriteBatch(std::string&& rep);

  WriteBatch(const WriteBatch& src);
  WriteBatch(WriteBatch&& src) noexcept;
  WriteBatch& operator=(const WriteBatch& src);
  WriteBatch& operator=(WriteBatch&& src) noexcept;

  // marks this point in the WriteBatch as the last record to
  // be inserted into the WAL, provided the WAL is enabled
  void MarkWalTerminationPoint();
  const SavePoint& GetWalTerminationPoint() const { return wal_term_point_; }

  void SetMaxBytes(size_t max_bytes) override { max_bytes_ = max_bytes; }

  struct ProtectionInfo;
  size_t GetProtectionBytesPerKey() const;

 private:
  friend class WriteBatchInternal;
  friend class LocalSavePoint;
  // TODO(myabandeh): this is needed for a hack to collapse the write batch and
  // remove duplicate keys. Remove it when the hack is replaced with a proper
  // solution.
  friend class WriteBatchWithIndex;
  std::unique_ptr<SavePoints> save_points_;

  // When sending a WriteBatch through WriteImpl we might want to
  // specify that only the first x records of the batch be written to
  // the WAL.
  SavePoint wal_term_point_;

  // Is the content of the batch the application's latest state that meant only
  // to be used for recovery? Refer to
  // TransactionOptions::use_only_the_last_commit_time_batch_for_recovery for
  // more details.
  bool is_latest_persistent_state_ = false;

  // False if all keys are from column families that disable user-defined
  // timestamp OR UpdateTimestamps() has been called at least once.
  // This flag will be set to true if any of the above Put(), Delete(),
  // SingleDelete(), etc. APIs are called at least once.
  // Calling Put(ts), Delete(ts), SingleDelete(ts), etc. will not set this flag
  // to true because the assumption is that these APIs have already set the
  // timestamps to desired values.
  bool needs_in_place_update_ts_ = false;

  // True if the write batch contains at least one key from a column family
  // that enables user-defined timestamp.
  bool has_key_with_ts_ = false;

  // For HasXYZ.  Mutable to allow lazy computation of results
  mutable std::atomic<uint32_t> content_flags_;

  // Performs deferred computation of content_flags if necessary
  uint32_t ComputeContentFlags() const;

  // Maximum size of rep_.
  size_t max_bytes_;

  std::unique_ptr<ProtectionInfo> prot_info_;

  size_t default_cf_ts_sz_ = 0;

  bool track_timestamp_size_ = false;

  std::unordered_map<uint32_t, size_t> cf_id_to_ts_sz_;

 protected:
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};

}  // namespace ROCKSDB_NAMESPACE
