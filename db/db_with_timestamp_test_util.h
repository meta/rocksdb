// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
class DBBasicTestWithTimestampBase : public DBTestBase {
 public:
  explicit DBBasicTestWithTimestampBase(const std::string& dbname)
      : DBTestBase(dbname, /*env_do_fsync=*/true) {}

 protected:
  static std::string Key1(uint64_t k);

  static std::string KeyWithPrefix(const std::string& prefix, uint64_t k);

  static std::vector<Slice> ConvertStrToSlice(
      std::vector<std::string>& strings);

  class TestComparator : public Comparator {
   private:
    const Comparator* cmp_without_ts_;

   public:
    explicit TestComparator(size_t ts_sz)
        : Comparator(ts_sz), cmp_without_ts_(nullptr) {
      cmp_without_ts_ = BytewiseComparator();
    }

    const char* Name() const override { return "TestComparator"; }

    void FindShortSuccessor(std::string*) const override {}

    void FindShortestSeparator(std::string*, const Slice&) const override {}

    int Compare(const Slice& a, const Slice& b) const override {
      int r = CompareWithoutTimestamp(a, b);
      if (r != 0 || 0 == timestamp_size()) {
        return r;
      }
      return -CompareTimestamp(
          Slice(a.data() + a.size() - timestamp_size(), timestamp_size()),
          Slice(b.data() + b.size() - timestamp_size(), timestamp_size()));
    }

    using Comparator::CompareWithoutTimestamp;
    int CompareWithoutTimestamp(const Slice& a, bool a_has_ts, const Slice& b,
                                bool b_has_ts) const override {
      if (a_has_ts) {
        assert(a.size() >= timestamp_size());
      }
      if (b_has_ts) {
        assert(b.size() >= timestamp_size());
      }
      Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, timestamp_size()) : a;
      Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, timestamp_size()) : b;
      return cmp_without_ts_->Compare(lhs, rhs);
    }

    int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
      if (!ts1.data() && !ts2.data()) {
        return 0;
      } else if (ts1.data() && !ts2.data()) {
        return 1;
      } else if (!ts1.data() && ts2.data()) {
        return -1;
      }
      assert(ts1.size() == ts2.size());
      uint64_t low1 = 0;
      uint64_t low2 = 0;
      uint64_t high1 = 0;
      uint64_t high2 = 0;
      const size_t kSize = ts1.size();
      std::unique_ptr<char[]> ts1_buf(new char[kSize]);
      memcpy(ts1_buf.get(), ts1.data(), ts1.size());
      std::unique_ptr<char[]> ts2_buf(new char[kSize]);
      memcpy(ts2_buf.get(), ts2.data(), ts2.size());
      Slice ts1_copy = Slice(ts1_buf.get(), kSize);
      Slice ts2_copy = Slice(ts2_buf.get(), kSize);
      auto* ptr1 = const_cast<Slice*>(&ts1_copy);
      auto* ptr2 = const_cast<Slice*>(&ts2_copy);
      if (!GetFixed64(ptr1, &low1) || !GetFixed64(ptr1, &high1) ||
          !GetFixed64(ptr2, &low2) || !GetFixed64(ptr2, &high2)) {
        assert(false);
      }
      if (high1 < high2) {
        return -1;
      } else if (high1 > high2) {
        return 1;
      }
      if (low1 < low2) {
        return -1;
      } else if (low1 > low2) {
        return 1;
      }
      return 0;
    }
  };

  std::string Timestamp(uint64_t low, uint64_t high);

  void CheckIterUserEntry(const Iterator* it, const Slice& expected_key,
                          ValueType expected_value_type,
                          const Slice& expected_value,
                          const Slice& expected_ts) const;

  void CheckIterEntry(const Iterator* it, const Slice& expected_ukey,
                      SequenceNumber expected_seq, ValueType expected_val_type,
                      const Slice& expected_value,
                      const Slice& expected_ts) const;

  void CheckIterEntry(const Iterator* it, const Slice& expected_ukey,
                      ValueType expected_val_type, const Slice& expected_value,
                      const Slice& expected_ts) const;
};
}  // namespace ROCKSDB_NAMESPACE
