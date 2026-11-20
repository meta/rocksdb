//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/write_batch_base.h"

#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Simple implementation of SlicePart variants of Put().  Child classes
// can override these method with more performant solutions if they choose.
Status WriteBatchBase::Put(ColumnFamilyHandle* column_family,
                           const SliceParts& key, const SliceParts& value) {
  std::string key_buf;
  std::string value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  return Put(column_family, key_slice, value_slice);
}

Status WriteBatchBase::Put(const SliceParts& key, const SliceParts& value) {
  std::string key_buf;
  std::string value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  return Put(key_slice, value_slice);
}

Status WriteBatchBase::Put(ColumnFamilyHandle* column_family,
                           const SliceParts& key, const Slice& ts,
                           const SliceParts& value) {
  const int key_with_ts_num_parts = key.num_parts + 1;
  std::vector<Slice> key_with_ts(key_with_ts_num_parts);
  std::copy(key.parts, key.parts + key.num_parts, key_with_ts.begin());
  key_with_ts[key.num_parts] = ts;
  return Put(column_family,
             SliceParts(key_with_ts.data(), key_with_ts_num_parts), value);
}

Status WriteBatchBase::Delete(ColumnFamilyHandle* column_family,
                              const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  return Delete(column_family, key_slice);
}

Status WriteBatchBase::Delete(const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  return Delete(key_slice);
}

Status WriteBatchBase::Delete(ColumnFamilyHandle* column_family,
                              const SliceParts& key, const Slice& ts) {
  const int key_with_ts_num_parts = key.num_parts + 1;
  std::vector<Slice> key_with_ts(key_with_ts_num_parts);
  std::copy(key.parts, key.parts + key.num_parts, key_with_ts.begin());
  key_with_ts[key.num_parts] = ts;
  return Delete(column_family,
                SliceParts(key_with_ts.data(), key_with_ts_num_parts));
}

Status WriteBatchBase::SingleDelete(ColumnFamilyHandle* column_family,
                                    const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  return SingleDelete(column_family, key_slice);
}

Status WriteBatchBase::SingleDelete(const SliceParts& key) {
  std::string key_buf;
  Slice key_slice(key, &key_buf);
  return SingleDelete(key_slice);
}

Status WriteBatchBase::SingleDelete(ColumnFamilyHandle* column_family,
                                    const SliceParts& key, const Slice& ts) {
  const int key_with_ts_num_parts = key.num_parts + 1;
  std::vector<Slice> key_with_ts(key_with_ts_num_parts);
  std::copy(key.parts, key.parts + key.num_parts, key_with_ts.begin());
  key_with_ts[key.num_parts] = ts;
  return SingleDelete(column_family,
                      SliceParts(key_with_ts.data(), key_with_ts_num_parts));
}

Status WriteBatchBase::DeleteRange(ColumnFamilyHandle* column_family,
                                   const SliceParts& begin_key,
                                   const SliceParts& end_key) {
  std::string begin_key_buf;
  std::string end_key_buf;
  Slice begin_key_slice(begin_key, &begin_key_buf);
  Slice end_key_slice(end_key, &end_key_buf);
  return DeleteRange(column_family, begin_key_slice, end_key_slice);
}

Status WriteBatchBase::DeleteRange(const SliceParts& begin_key,
                                   const SliceParts& end_key) {
  std::string begin_key_buf;
  std::string end_key_buf;
  Slice begin_key_slice(begin_key, &begin_key_buf);
  Slice end_key_slice(end_key, &end_key_buf);
  return DeleteRange(begin_key_slice, end_key_slice);
}

Status WriteBatchBase::DeleteRange(ColumnFamilyHandle* column_family,
                                   const SliceParts& begin_key,
                                   const SliceParts& end_key, const Slice& ts) {
  const int begin_key_with_ts_num_parts = begin_key.num_parts + 1;
  std::vector<Slice> begin_key_with_ts(begin_key_with_ts_num_parts);
  std::copy(begin_key.parts, begin_key.parts + begin_key.num_parts,
            begin_key_with_ts.begin());
  begin_key_with_ts[begin_key.num_parts] = ts;

  const int end_key_with_ts_num_parts = end_key.num_parts + 1;
  std::vector<Slice> end_key_with_ts(end_key_with_ts_num_parts);
  std::copy(end_key.parts, end_key.parts + end_key.num_parts,
            end_key_with_ts.begin());
  end_key_with_ts[end_key.num_parts] = ts;

  return DeleteRange(
      column_family,
      SliceParts(begin_key_with_ts.data(), begin_key_with_ts_num_parts),
      SliceParts(end_key_with_ts.data(), end_key_with_ts_num_parts));
}

Status WriteBatchBase::Merge(ColumnFamilyHandle* column_family,
                             const SliceParts& key, const SliceParts& value) {
  std::string key_buf;
  std::string value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  return Merge(column_family, key_slice, value_slice);
}

Status WriteBatchBase::Merge(const SliceParts& key, const SliceParts& value) {
  std::string key_buf;
  std::string value_buf;
  Slice key_slice(key, &key_buf);
  Slice value_slice(value, &value_buf);

  return Merge(key_slice, value_slice);
}

Status WriteBatchBase::Merge(ColumnFamilyHandle* column_family,
                             const SliceParts& key, const Slice& ts,
                             const SliceParts& value) {
  const int key_with_ts_num_parts = key.num_parts + 1;
  std::vector<Slice> key_with_ts(key_with_ts_num_parts);
  std::copy(key.parts, key.parts + key.num_parts, key_with_ts.begin());
  key_with_ts[key.num_parts] = ts;
  return Merge(column_family,
               SliceParts(key_with_ts.data(), key_with_ts_num_parts), value);
}

}  // namespace ROCKSDB_NAMESPACE
