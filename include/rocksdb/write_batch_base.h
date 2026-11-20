// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstddef>

#include "rocksdb/attribute_groups.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class Status;
class ColumnFamilyHandle;
class WriteBatch;
struct SliceParts;

// Abstract base class that defines the basic interface for a write batch.
// See WriteBatch for a basic implementation and WrithBatchWithIndex for an
// indexed implementation.
class WriteBatchBase {
 public:
  WriteBatchBase() = default;
  WriteBatchBase(const WriteBatchBase&) = default;
  WriteBatchBase& operator=(const WriteBatchBase&) = default;
  WriteBatchBase(WriteBatchBase&&) = default;
  WriteBatchBase& operator=(WriteBatchBase&&) = default;
  virtual ~WriteBatchBase() = default;

  // Store the mapping "key->value" in the database.
  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) = 0;
  virtual Status Put(const Slice& key, const Slice& value) = 0;
  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& ts, const Slice& value) = 0;

  // Variant of Put() that gathers output like writev(2).  The key and value
  // that will be written to the database are concatenations of arrays of
  // slices.
  virtual Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                     const SliceParts& value);
  virtual Status Put(const SliceParts& key, const SliceParts& value);

  // EXPERIMENTAL
  // Store the mapping "key->value" in the database with the specified write
  // time in the column family. Using some write time that is in the past to
  // fast track data to their correct placement and preservation is the intended
  // usage of this API. The DB makes a reasonable best effort to treat the data
  // as having the given write time for this purpose but doesn't currently make
  // any guarantees.
  //
  // This feature is experimental and one known side effect is that it can break
  // snapshot immutability. Reading from a snapshot created before
  // TimedPut(k, v, t) may or may not see that k->v.
  // Note: this feature is currently not compatible with user-defined timestamps
  // and wide columns.
  virtual Status TimedPut(ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& value, uint64_t write_unix_time) = 0;

  // Store the mapping "key->{column1:value1, column2:value2, ...}" in the
  // column family specified by "column_family". `columns` is a non-owning
  // view, so the backing storage for each column name and value must remain
  // valid until this method returns.
  virtual Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                           const WideColumns& columns) = 0;

  // Split and store wide column entities in multiple column families (a.k.a.
  // AttributeGroups)
  virtual Status PutEntity(const Slice& key,
                           const AttributeGroups& attribute_groups) = 0;

  // Merge "value" with the existing value of "key" in the database.
  // "key->merge(existing, value)"
  virtual Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) = 0;
  virtual Status Merge(const Slice& key, const Slice& value) = 0;
  virtual Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& ts, const Slice& value) = 0;

  // variant that takes SliceParts
  virtual Status Merge(ColumnFamilyHandle* column_family, const SliceParts& key,
                       const SliceParts& value);
  virtual Status Merge(const SliceParts& key, const SliceParts& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  virtual Status Delete(ColumnFamilyHandle* column_family,
                        const Slice& key) = 0;
  virtual Status Delete(const Slice& key) = 0;
  virtual Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& ts) = 0;

  // variant that takes SliceParts
  virtual Status Delete(ColumnFamilyHandle* column_family,
                        const SliceParts& key);
  virtual Status Delete(const SliceParts& key);

  // If the database contains a mapping for "key", erase it. Expects that the
  // key was not overwritten. Else do nothing.
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const Slice& key) = 0;
  virtual Status SingleDelete(const Slice& key) = 0;
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& ts) = 0;

  // variant that takes SliceParts
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const SliceParts& key);
  virtual Status SingleDelete(const SliceParts& key);

  // If the database contains mappings in the range ["begin_key", "end_key"),
  // erase them. Else do nothing.
  virtual Status DeleteRange(ColumnFamilyHandle* column_family,
                             const Slice& begin_key, const Slice& end_key) = 0;
  virtual Status DeleteRange(const Slice& begin_key, const Slice& end_key) = 0;
  virtual Status DeleteRange(ColumnFamilyHandle* column_family,
                             const Slice& begin_key, const Slice& end_key,
                             const Slice& ts) = 0;

  // variant that takes SliceParts
  virtual Status DeleteRange(ColumnFamilyHandle* column_family,
                             const SliceParts& begin_key,
                             const SliceParts& end_key);
  virtual Status DeleteRange(const SliceParts& begin_key,
                             const SliceParts& end_key);

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
  virtual Status PutLogData(const Slice& blob) = 0;

  // Clear all updates buffered in this batch.
  virtual void Clear() = 0;

  // Covert this batch into a WriteBatch.  This is an abstracted way of
  // converting any WriteBatchBase(eg WriteBatchWithIndex) into a basic
  // WriteBatch.
  virtual WriteBatch* GetWriteBatch() = 0;

  // Records the state of the batch for future calls to RollbackToSavePoint().
  // May be called multiple times to set multiple save points.
  virtual void SetSavePoint() = 0;

  // Remove all entries in this batch (Put, Merge, Delete, PutLogData) since the
  // most recent call to SetSavePoint() and removes the most recent save point.
  // If there is no previous call to SetSavePoint(), behaves the same as
  // Clear().
  virtual Status RollbackToSavePoint() = 0;

  // Pop the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  virtual Status PopSavePoint() = 0;

  // Sets the maximum size of the write batch in bytes. 0 means no limit.
  virtual void SetMaxBytes(size_t max_bytes) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
