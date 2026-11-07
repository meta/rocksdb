// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_with_timestamp_test_util.h"

namespace ROCKSDB_NAMESPACE {
std::string DBBasicTestWithTimestampBase::Key1(uint64_t k) {
  std::string ret;
  PutFixed64(&ret, k);
  std::reverse(ret.begin(), ret.end());
  return ret;
}

std::string DBBasicTestWithTimestampBase::KeyWithPrefix(
    const std::string& prefix, uint64_t k) {
  std::string ret;
  PutFixed64(&ret, k);
  std::reverse(ret.begin(), ret.end());
  return prefix + ret;
}

std::vector<Slice> DBBasicTestWithTimestampBase::ConvertStrToSlice(
    std::vector<std::string>& strings) {
  std::vector<Slice> ret;
  for (const auto& s : strings) {
    ret.emplace_back(s);
  }
  return ret;
}

std::string DBBasicTestWithTimestampBase::Timestamp(uint64_t low,
                                                    uint64_t high) {
  std::string ts;
  PutFixed64(&ts, low);
  PutFixed64(&ts, high);
  return ts;
}

void DBBasicTestWithTimestampBase::CheckIterUserEntry(
    const Iterator* it, const Slice& expected_key,
    ValueType expected_value_type, const Slice& expected_value,
    const Slice& expected_ts) const {
  ASSERT_TRUE(it->Valid());
  ASSERT_OK(it->status());
  ASSERT_EQ(expected_key, it->key());
  if (kTypeValue == expected_value_type) {
    ASSERT_EQ(expected_value, it->value());
  }
  ASSERT_EQ(expected_ts, it->timestamp());
}

void DBBasicTestWithTimestampBase::CheckIterEntry(
    const Iterator* it, const Slice& expected_ukey, SequenceNumber expected_seq,
    ValueType expected_val_type, const Slice& expected_value,
    const Slice& expected_ts) const {
  ASSERT_TRUE(it->Valid());
  ASSERT_OK(it->status());
  std::string ukey_and_ts;
  ukey_and_ts.assign(expected_ukey.data(), expected_ukey.size());
  ukey_and_ts.append(expected_ts.data(), expected_ts.size());
  ParsedInternalKey parsed_ikey;
  ASSERT_OK(ParseInternalKey(it->key(), &parsed_ikey, true /* log_err_key */));
  ASSERT_EQ(ukey_and_ts, parsed_ikey.user_key);
  ASSERT_EQ(expected_val_type, parsed_ikey.type);
  ASSERT_EQ(expected_seq, parsed_ikey.sequence);
  if (expected_val_type == kTypeValue) {
    ASSERT_EQ(expected_value, it->value());
  }
  ASSERT_EQ(expected_ts, it->timestamp());
}

void DBBasicTestWithTimestampBase::CheckIterEntry(
    const Iterator* it, const Slice& expected_ukey, ValueType expected_val_type,
    const Slice& expected_value, const Slice& expected_ts) const {
  ASSERT_TRUE(it->Valid());
  ASSERT_OK(it->status());
  std::string ukey_and_ts;
  ukey_and_ts.assign(expected_ukey.data(), expected_ukey.size());
  ukey_and_ts.append(expected_ts.data(), expected_ts.size());

  ParsedInternalKey parsed_ikey;
  ASSERT_OK(ParseInternalKey(it->key(), &parsed_ikey, true /* log_err_key */));
  ASSERT_EQ(expected_val_type, parsed_ikey.type);
  ASSERT_EQ(Slice(ukey_and_ts), parsed_ikey.user_key);
  if (expected_val_type == kTypeValue) {
    ASSERT_EQ(expected_value, it->value());
  }
  ASSERT_EQ(expected_ts, it->timestamp());
}
}  // namespace ROCKSDB_NAMESPACE
