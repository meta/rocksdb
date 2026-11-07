// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <map>
#include <memory>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/testharness.h"
#include "util/string_util.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/ttl/db_ttl_impl.h"
#ifndef OS_WIN
#include <unistd.h>
#endif

#if USE_COROUTINES
#include "folly/coro/BlockingWait.h"
#include "rocksdb/coro_db.h"
#endif

namespace ROCKSDB_NAMESPACE {

namespace {

using KVMap = std::map<std::string, std::string>;

enum BatchOperation { OP_PUT = 0, OP_DELETE = 1 };
}  // namespace

class SpecialTimeEnv : public EnvWrapper {
 public:
  explicit SpecialTimeEnv(Env* base) : EnvWrapper(base) {
    EXPECT_OK(base->GetCurrentTime(&current_time_));
  }
  const char* Name() const override { return "SpecialTimeEnv"; }
  void Sleep(int64_t sleep_time) { current_time_ += sleep_time; }
  Status GetCurrentTime(int64_t* current_time) override {
    *current_time = current_time_;
    return Status::OK();
  }

 private:
  int64_t current_time_ = 0;
};

class TtlTest : public testing::Test {
 public:
  TtlTest() {
    env_.reset(new SpecialTimeEnv(Env::Default()));
    dbname_ = test::PerThreadDBPath("db_ttl");
    options_.create_if_missing = true;
    options_.env = env_.get();
    // ensure that compaction is kicked in to always strip timestamp from kvs
    options_.max_compaction_bytes = 1;
    // compaction should take place always from level0 for determinism
    db_ttl_ = nullptr;
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  ~TtlTest() override {
    CloseTtl();
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  // Open database with TTL support when TTL not provided with db_ttl_ pointer
  void OpenTtl() {
    ASSERT_TRUE(db_ttl_ ==
                nullptr);  //  db should be closed before opening again
    ASSERT_OK(DBWithTTL::Open(options_, dbname_, &db_ttl_));
  }

  // Open database with TTL support when TTL provided with db_ttl_ pointer
  void OpenTtl(int32_t ttl) {
    ASSERT_TRUE(db_ttl_ == nullptr);
    ASSERT_OK(DBWithTTL::Open(options_, dbname_, &db_ttl_, ttl));
  }

  // Open with TestFilter compaction filter
  void OpenTtlWithTestCompaction(int32_t ttl) {
    options_.compaction_filter_factory =
        std::shared_ptr<CompactionFilterFactory>(
            new TestFilterFactory(kSampleSize_, kNewValue_));
    OpenTtl(ttl);
  }

  // Open database with TTL support in read_only mode
  void OpenReadOnlyTtl(int32_t ttl) {
    ASSERT_TRUE(db_ttl_ == nullptr);
    ASSERT_OK(DBWithTTL::Open(options_, dbname_, &db_ttl_, ttl, true));
  }

  // Call db_ttl_->Close() before delete db_ttl_
  void CloseTtl() { CloseTtlHelper(true); }

  // No db_ttl_->Close() before delete db_ttl_
  void CloseTtlNoDBClose() { CloseTtlHelper(false); }

  void CloseTtlHelper(bool close_db) {
    if (db_ttl_ != nullptr) {
      if (close_db) {
        EXPECT_OK(db_ttl_->Close());
      }
      delete db_ttl_;
      db_ttl_ = nullptr;
    }
  }

  // Populates and returns a kv-map
  void MakeKVMap(int64_t num_entries) {
    kvmap_.clear();
    int digits = 1;
    for (int64_t dummy = num_entries; dummy /= 10; ++digits) {
    }
    int digits_in_i = 1;
    for (int64_t i = 0; i < num_entries; i++) {
      std::string key = "key";
      std::string value = "value";
      if (i % 10 == 0) {
        digits_in_i++;
      }
      for (int j = digits_in_i; j < digits; j++) {
        key.append("0");
        value.append("0");
      }
      AppendNumberTo(&key, i);
      AppendNumberTo(&value, i);
      kvmap_[key] = value;
    }
    ASSERT_EQ(static_cast<int64_t>(kvmap_.size()),
              num_entries);  // check all insertions done
  }

  // Makes a write-batch with key-vals from kvmap_ and 'Write''s it
  void MakePutWriteBatch(const BatchOperation* batch_ops, int64_t num_ops) {
    ASSERT_LE(num_ops, static_cast<int64_t>(kvmap_.size()));
    static WriteOptions wopts;
    static FlushOptions flush_opts;
    WriteBatch batch;
    kv_it_ = kvmap_.begin();
    for (int64_t i = 0; i < num_ops && kv_it_ != kvmap_.end(); i++, ++kv_it_) {
      switch (batch_ops[i]) {
        case OP_PUT:
          ASSERT_OK(batch.Put(kv_it_->first, kv_it_->second));
          break;
        case OP_DELETE:
          ASSERT_OK(batch.Delete(kv_it_->first));
          break;
        default:
          FAIL();
      }
    }
    ASSERT_OK(db_ttl_->Write(wopts, &batch));
    ASSERT_OK(db_ttl_->Flush(flush_opts));
  }

  // Puts num_entries starting from start_pos_map from kvmap_ into the database
  void PutValues(int64_t start_pos_map, int64_t num_entries, bool flush = true,
                 ColumnFamilyHandle* cf = nullptr) {
    ASSERT_TRUE(db_ttl_);
    ASSERT_LE(start_pos_map + num_entries, static_cast<int64_t>(kvmap_.size()));
    static WriteOptions wopts;
    static FlushOptions flush_opts;
    kv_it_ = kvmap_.begin();
    advance(kv_it_, start_pos_map);
    for (int64_t i = 0; kv_it_ != kvmap_.end() && i < num_entries;
         i++, ++kv_it_) {
      ASSERT_OK(cf == nullptr
                    ? db_ttl_->Put(wopts, kv_it_->first, kv_it_->second)
                    : db_ttl_->Put(wopts, cf, kv_it_->first, kv_it_->second));
    }
    // Put a mock kv at the end because CompactionFilter doesn't delete last key
    ASSERT_OK(cf == nullptr ? db_ttl_->Put(wopts, "keymock", "valuemock")
                            : db_ttl_->Put(wopts, cf, "keymock", "valuemock"));
    if (flush) {
      if (cf == nullptr) {
        ASSERT_OK(db_ttl_->Flush(flush_opts));
      } else {
        ASSERT_OK(db_ttl_->Flush(flush_opts, cf));
      }
    }
  }

  // Runs a manual compaction
  Status ManualCompact(ColumnFamilyHandle* cf = nullptr) {
    assert(db_ttl_);
    if (cf == nullptr) {
      return db_ttl_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    } else {
      return db_ttl_->CompactRange(CompactRangeOptions(), cf, nullptr, nullptr);
    }
  }

  // Runs a DeleteRange
  void MakeDeleteRange(const std::string& start, const std::string& end,
                       ColumnFamilyHandle* cf = nullptr) {
    ASSERT_TRUE(db_ttl_);
    static WriteOptions wops;
    WriteBatch wb;
    ASSERT_OK(cf == nullptr
                  ? wb.DeleteRange(db_ttl_->DefaultColumnFamily(), start, end)
                  : wb.DeleteRange(cf, start, end));
    ASSERT_OK(db_ttl_->Write(wops, &wb));
  }

  // checks the whole kvmap_ to return correct values using KeyMayExist
  void SimpleKeyMayExistCheck() {
    static ReadOptions ropts;
    bool value_found;
    std::string val;
    for (auto& kv : kvmap_) {
      bool ret = db_ttl_->KeyMayExist(ropts, kv.first, &val, &value_found);
      if (ret == false || value_found == false) {
        fprintf(stderr,
                "KeyMayExist could not find key=%s in the database but"
                " should have\n",
                kv.first.c_str());
        FAIL();
      } else if (val.compare(kv.second) != 0) {
        fprintf(stderr,
                " value for key=%s present in database is %s but"
                " should be %s\n",
                kv.first.c_str(), val.c_str(), kv.second.c_str());
        FAIL();
      }
    }
  }

  // checks the whole kvmap_ to return correct values using MultiGet
  void SimpleMultiGetTest() {
    static ReadOptions ropts;
    std::vector<Slice> keys;
    std::vector<std::string> values;

    for (auto& kv : kvmap_) {
      keys.emplace_back(kv.first);
    }

    auto statuses = db_ttl_->MultiGet(ropts, keys, &values);
    size_t i = 0;
    for (auto& kv : kvmap_) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], kv.second);
      ++i;
    }
  }

  void CompactCheck(int64_t st_pos, int64_t span, bool check = true,
                    bool test_compaction_change = false,
                    ColumnFamilyHandle* cf = nullptr) {
    static ReadOptions ropts;
    kv_it_ = kvmap_.begin();
    advance(kv_it_, st_pos);
    std::string v;
    for (int64_t i = 0; kv_it_ != kvmap_.end() && i < span; i++, ++kv_it_) {
      Status s = (cf == nullptr) ? db_ttl_->Get(ropts, kv_it_->first, &v)
                                 : db_ttl_->Get(ropts, cf, kv_it_->first, &v);
      if (s.ok() != check) {
        fprintf(stderr, "key=%s ", kv_it_->first.c_str());
        if (!s.ok()) {
          fprintf(stderr, "is absent from db but was expected to be present\n");
        } else {
          fprintf(stderr, "is present in db but was expected to be absent\n");
        }
        FAIL();
      } else if (s.ok()) {
        if (test_compaction_change && v.compare(kNewValue_) != 0) {
          fprintf(stderr,
                  " value for key=%s present in database is %s but "
                  " should be %s\n",
                  kv_it_->first.c_str(), v.c_str(), kNewValue_.c_str());
          FAIL();
        } else if (!test_compaction_change && v.compare(kv_it_->second) != 0) {
          fprintf(stderr,
                  " value for key=%s present in database is %s but "
                  " should be %s\n",
                  kv_it_->first.c_str(), v.c_str(), kv_it_->second.c_str());
          FAIL();
        }
      }
    }
  }
  // Sleeps for slp_tim then runs a manual compaction
  // Checks span starting from st_pos from kvmap_ in the db and
  // Gets should return true if check is true and false otherwise
  // Also checks that value that we got is the same as inserted; and =kNewValue
  //   if test_compaction_change is true
  void SleepCompactCheck(int slp_tim, int64_t st_pos, int64_t span,
                         bool check = true, bool test_compaction_change = false,
                         ColumnFamilyHandle* cf = nullptr) {
    ASSERT_TRUE(db_ttl_);

    env_->Sleep(slp_tim);
    ASSERT_OK(ManualCompact(cf));
    CompactCheck(st_pos, span, check, test_compaction_change, cf);
  }

  // Similar as SleepCompactCheck but uses TtlIterator to read from db
  void SleepCompactCheckIter(int slp, int st_pos, int64_t span,
                             bool check = true) {
    ASSERT_TRUE(db_ttl_);
    env_->Sleep(slp);
    ASSERT_OK(ManualCompact());
    static ReadOptions ropts;
    Iterator* dbiter = db_ttl_->NewIterator(ropts);
    kv_it_ = kvmap_.begin();
    advance(kv_it_, st_pos);

    dbiter->Seek(kv_it_->first);
    if (!check) {
      if (dbiter->Valid()) {
        ASSERT_NE(dbiter->value().compare(kv_it_->second), 0);
      }
    } else {  // dbiter should have found out kvmap_[st_pos]
      for (int64_t i = st_pos; kv_it_ != kvmap_.end() && i < st_pos + span;
           i++, ++kv_it_) {
        ASSERT_TRUE(dbiter->Valid());
        ASSERT_EQ(dbiter->value().compare(kv_it_->second), 0);
        dbiter->Next();
      }
    }
    ASSERT_OK(dbiter->status());
    delete dbiter;
  }

  // Set ttl on open db
  void SetTtl(int32_t ttl, ColumnFamilyHandle* cf = nullptr) {
    ASSERT_TRUE(db_ttl_);
    cf == nullptr ? db_ttl_->SetTtl(ttl) : db_ttl_->SetTtl(cf, ttl);
  }

  class TestFilter : public CompactionFilter {
   public:
    TestFilter(const int64_t kSampleSize, const std::string& kNewValue)
        : kSampleSize_(kSampleSize), kNewValue_(kNewValue) {}

    // Works on keys of the form "key<number>"
    // Drops key if number at the end of key is in [0, kSampleSize_/3),
    // Keeps key if it is in [kSampleSize_/3, 2*kSampleSize_/3),
    // Change value if it is in [2*kSampleSize_/3, kSampleSize_)
    // Eg. kSampleSize_=6. Drop:key0-1...Keep:key2-3...Change:key4-5...
    bool Filter(int /*level*/, const Slice& key, const Slice& /*value*/,
                std::string* new_value, bool* value_changed) const override {
      assert(new_value != nullptr);

      std::string search_str = "0123456789";
      std::string key_string = key.ToString();
      size_t pos = key_string.find_first_of(search_str);
      int num_key_end;
      if (pos != std::string::npos) {
        auto key_substr = key_string.substr(pos, key.size() - pos);
#ifndef CYGWIN
        num_key_end = std::stoi(key_substr);
#else
        num_key_end = std::strtol(key_substr.c_str(), 0, 10);
#endif

      } else {
        return false;  // Keep keys not matching the format "key<NUMBER>"
      }

      int64_t partition = kSampleSize_ / 3;
      if (num_key_end < partition) {
        return true;
      } else if (num_key_end < partition * 2) {
        return false;
      } else {
        *new_value = kNewValue_;
        *value_changed = true;
        return false;
      }
    }

    const char* Name() const override { return "TestFilter"; }

   private:
    const int64_t kSampleSize_;
    const std::string kNewValue_;
  };

  class TestFilterFactory : public CompactionFilterFactory {
   public:
    TestFilterFactory(const int64_t kSampleSize, const std::string& kNewValue)
        : kSampleSize_(kSampleSize), kNewValue_(kNewValue) {}

    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context& /*context*/) override {
      return std::unique_ptr<CompactionFilter>(
          new TestFilter(kSampleSize_, kNewValue_));
    }

    const char* Name() const override { return "TestFilterFactory"; }

   private:
    const int64_t kSampleSize_;
    const std::string kNewValue_;
  };

  // Choose carefully so that Put, Gets & Compaction complete in 1 second buffer
  static const int64_t kSampleSize_ = 100;
  std::string dbname_;
  DBWithTTL* db_ttl_;
  std::unique_ptr<SpecialTimeEnv> env_;

 protected:
  Options options_;

 private:
  KVMap kvmap_;
  KVMap::iterator kv_it_;
  const std::string kNewValue_ = "new_value";
  std::unique_ptr<CompactionFilter> test_comp_filter_;
};  // class TtlTest

#if USE_COROUTINES
TEST_F(TtlTest, GetCoroutine) {
  options_.env = Env::Default();
  OpenTtl();
  ASSERT_OK(db_ttl_->Put(WriteOptions(), "key", "value"));

  PinnableSlice value;
  CoroDB* coro_db = db_ttl_->GetCoroDB();
  ASSERT_NE(nullptr, coro_db);
  std::string timestamp;
  ASSERT_TRUE(
      folly::coro::blockingWait(CoroDB::CoGet(db_ttl_, ReadOptions(),
                                              db_ttl_->DefaultColumnFamily(),
                                              "key", &value, &timestamp))
          .IsNotSupported());

  Status status = folly::coro::blockingWait(CoroDB::CoGet(
      db_ttl_, ReadOptions(), db_ttl_->DefaultColumnFamily(), "key", &value,
      /*timestamp=*/nullptr));

  ASSERT_OK(status);
  ASSERT_EQ("value", value.ToString());
}

TEST_F(TtlTest, MultiGetCoroutine) {
  options_.env = Env::Default();
  OpenTtl();
  ASSERT_OK(db_ttl_->Put(WriteOptions(), "key", "value"));

  ColumnFamilyHandle* column_families[] = {db_ttl_->DefaultColumnFamily()};
  const Slice keys[] = {Slice("key")};
  PinnableSlice values[1];
  Status statuses[1];
  CoroDB* coro_db = db_ttl_->GetCoroDB();
  ASSERT_NE(nullptr, coro_db);
  std::string timestamps[1];
  folly::coro::blockingWait(
      CoroDB::CoMultiGet(db_ttl_, ReadOptions(), 1, column_families, keys,
                         values, timestamps, statuses, /*sorted_input=*/false));
  ASSERT_TRUE(statuses[0].IsNotSupported());

  folly::coro::blockingWait(CoroDB::CoMultiGet(
      db_ttl_, ReadOptions(), 1, column_families, keys, values,
      /*timestamps=*/nullptr, statuses, /*sorted_input=*/false));

  ASSERT_OK(statuses[0]);
  ASSERT_EQ("value", values[0].ToString());
}
#endif  // USE_COROUTINES

TEST_F(TtlTest, SnapshotNewerVersionMetadata) {
  OpenTtl();
  assert(db_ttl_);
  ASSERT_OK(db_ttl_->Put(WriteOptions(), "key", "old"));
  const Snapshot* snapshot = db_ttl_->GetSnapshot();
  ASSERT_OK(db_ttl_->Put(WriteOptions(), "key", "new"));

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  OutputMetadata output_metadata;
  output_metadata.WantNewerVersionPresent();
  std::string value;
  ASSERT_OK(
      db_ttl_->GetWithMetadata(read_options, "key", &value, &output_metadata));
  ASSERT_EQ("old", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  MultiGetOutputMetadata multiget_output_metadata;
  multiget_output_metadata.WantNewerVersionPresent();
  std::vector<Slice> keys{"key"};
  std::vector<std::string> values;
  const std::vector<Status> statuses = db_ttl_->MultiGetWithMetadata(
      read_options, keys, &values, &multiget_output_metadata);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);
  ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[0]);

  db_ttl_->ReleaseSnapshot(snapshot);
}

// If TTL is non positive or not provided, the behaviour is TTL = infinity
// This test opens the db 3 times with such default behavior and inserts a
// bunch of kvs each time. All kvs should accumulate in the db till the end
// Partitions the sample-size provided into 3 sets over boundary1 and boundary2
TEST_F(TtlTest, NoEffect) {
  MakeKVMap(kSampleSize_);
  int64_t boundary1 = kSampleSize_ / 3;
  int64_t boundary2 = 2 * boundary1;

  OpenTtl();
  PutValues(0, boundary1);             // T=0: Set1 never deleted
  SleepCompactCheck(1, 0, boundary1);  // T=1: Set1 still there
  CloseTtl();

  OpenTtl(0);
  PutValues(boundary1, boundary2 - boundary1);  // T=1: Set2 never deleted
  SleepCompactCheck(1, 0, boundary2);           // T=2: Sets1 & 2 still there
  CloseTtl();

  OpenTtl(-1);
  PutValues(boundary2, kSampleSize_ - boundary2);  // T=3: Set3 never deleted
  SleepCompactCheck(1, 0, kSampleSize_, true);  // T=4: Sets 1,2,3 still there
  CloseTtl();
}

// Rerun the NoEffect test with a different version of CloseTtl
// function, where db is directly deleted without close.
TEST_F(TtlTest, DestructWithoutClose) {
  MakeKVMap(kSampleSize_);
  int64_t boundary1 = kSampleSize_ / 3;
  int64_t boundary2 = 2 * boundary1;

  OpenTtl();
  PutValues(0, boundary1);             // T=0: Set1 never deleted
  SleepCompactCheck(1, 0, boundary1);  // T=1: Set1 still there
  CloseTtlNoDBClose();

  OpenTtl(0);
  PutValues(boundary1, boundary2 - boundary1);  // T=1: Set2 never deleted
  SleepCompactCheck(1, 0, boundary2);           // T=2: Sets1 & 2 still there
  CloseTtlNoDBClose();

  OpenTtl(-1);
  PutValues(boundary2, kSampleSize_ - boundary2);  // T=3: Set3 never deleted
  SleepCompactCheck(1, 0, kSampleSize_, true);  // T=4: Sets 1,2,3 still there
  CloseTtlNoDBClose();
}

// Puts a set of values and checks its presence using Get during ttl
TEST_F(TtlTest, PresentDuringTTL) {
  MakeKVMap(kSampleSize_);

  OpenTtl(2);                  // T=0:Open the db with ttl = 2
  PutValues(0, kSampleSize_);  // T=0:Insert Set1. Delete at t=2
  SleepCompactCheck(1, 0, kSampleSize_,
                    true);  // T=1:Set1 should still be there
  CloseTtl();
}

// Puts a set of values and checks its absence using Get after ttl
TEST_F(TtlTest, AbsentAfterTTL) {
  MakeKVMap(kSampleSize_);

  OpenTtl(1);                  // T=0:Open the db with ttl = 2
  PutValues(0, kSampleSize_);  // T=0:Insert Set1. Delete at t=2
  SleepCompactCheck(2, 0, kSampleSize_, false);  // T=2:Set1 should not be there
  CloseTtl();
}

// Resets the timestamp of a set of kvs by updating them and checks that they
// are not deleted according to the old timestamp
TEST_F(TtlTest, ResetTimestamp) {
  MakeKVMap(kSampleSize_);

  OpenTtl(3);
  PutValues(0, kSampleSize_);             // T=0: Insert Set1. Delete at t=3
  env_->Sleep(2);                         // T=2
  PutValues(0, kSampleSize_);             // T=2: Insert Set1. Delete at t=5
  SleepCompactCheck(2, 0, kSampleSize_);  // T=4: Set1 should still be there
  CloseTtl();
}

// Similar to PresentDuringTTL but uses Iterator
TEST_F(TtlTest, IterPresentDuringTTL) {
  MakeKVMap(kSampleSize_);

  OpenTtl(2);
  PutValues(0, kSampleSize_);                 // T=0: Insert. Delete at t=2
  SleepCompactCheckIter(1, 0, kSampleSize_);  // T=1: Set should be there
  CloseTtl();
}

// Similar to AbsentAfterTTL but uses Iterator
TEST_F(TtlTest, IterAbsentAfterTTL) {
  MakeKVMap(kSampleSize_);

  OpenTtl(1);
  PutValues(0, kSampleSize_);  // T=0: Insert. Delete at t=1
  SleepCompactCheckIter(2, 0, kSampleSize_, false);  // T=2: Should not be there
  CloseTtl();
}

// Checks presence while opening the same db more than once with the same ttl
// Note: The second open will open the same db
TEST_F(TtlTest, MultiOpenSamePresent) {
  MakeKVMap(kSampleSize_);

  OpenTtl(2);
  PutValues(0, kSampleSize_);  // T=0: Insert. Delete at t=2
  CloseTtl();

  OpenTtl(2);                             // T=0. Delete at t=2
  SleepCompactCheck(1, 0, kSampleSize_);  // T=1: Set should be there
  CloseTtl();
}

// Checks absence while opening the same db more than once with the same ttl
// Note: The second open will open the same db
TEST_F(TtlTest, MultiOpenSameAbsent) {
  MakeKVMap(kSampleSize_);

  OpenTtl(1);
  PutValues(0, kSampleSize_);  // T=0: Insert. Delete at t=1
  CloseTtl();

  OpenTtl(1);                                    // T=0.Delete at t=1
  SleepCompactCheck(2, 0, kSampleSize_, false);  // T=2: Set should not be there
  CloseTtl();
}

// Checks presence while opening the same db more than once with bigger ttl
TEST_F(TtlTest, MultiOpenDifferent) {
  MakeKVMap(kSampleSize_);

  OpenTtl(1);
  PutValues(0, kSampleSize_);  // T=0: Insert. Delete at t=1
  CloseTtl();

  OpenTtl(3);                             // T=0: Set deleted at t=3
  SleepCompactCheck(2, 0, kSampleSize_);  // T=2: Set should be there
  CloseTtl();
}

// Checks presence during ttl in read_only mode
TEST_F(TtlTest, ReadOnlyPresentForever) {
  MakeKVMap(kSampleSize_);

  OpenTtl(1);                  // T=0:Open the db normally
  PutValues(0, kSampleSize_);  // T=0:Insert Set1. Delete at t=1
  CloseTtl();

  OpenReadOnlyTtl(1);
  ASSERT_TRUE(db_ttl_);

  env_->Sleep(2);
  Status s = ManualCompact();  // T=2:Set1 should still be there
  ASSERT_TRUE(s.IsNotSupported());
  CompactCheck(0, kSampleSize_);
  CloseTtl();
}

// Checks whether WriteBatch works well with TTL
// Puts all kvs in kvmap_ in a batch and writes first, then deletes first half
TEST_F(TtlTest, WriteBatchTest) {
  MakeKVMap(kSampleSize_);
  BatchOperation batch_ops[kSampleSize_];
  for (int i = 0; i < kSampleSize_; i++) {
    batch_ops[i] = OP_PUT;
  }

  OpenTtl(2);
  MakePutWriteBatch(batch_ops, kSampleSize_);
  for (int i = 0; i < kSampleSize_ / 2; i++) {
    batch_ops[i] = OP_DELETE;
  }
  MakePutWriteBatch(batch_ops, kSampleSize_ / 2);
  SleepCompactCheck(0, 0, kSampleSize_ / 2, false);
  SleepCompactCheck(0, kSampleSize_ / 2, kSampleSize_ - kSampleSize_ / 2);
  CloseTtl();
}

// Checks user's compaction filter for correctness with TTL logic
TEST_F(TtlTest, CompactionFilter) {
  MakeKVMap(kSampleSize_);

  OpenTtlWithTestCompaction(1);
  PutValues(0, kSampleSize_);  // T=0:Insert Set1. Delete at t=1
  // T=2: TTL logic takes precedence over TestFilter:-Set1 should not be there
  SleepCompactCheck(2, 0, kSampleSize_, false);
  CloseTtl();

  OpenTtlWithTestCompaction(3);
  PutValues(0, kSampleSize_);  // T=0:Insert Set1.
  int64_t partition = kSampleSize_ / 3;
  SleepCompactCheck(1, 0, partition, false);                   // Part dropped
  SleepCompactCheck(0, partition, partition);                  // Part kept
  SleepCompactCheck(0, 2 * partition, partition, true, true);  // Part changed
  CloseTtl();
}

TEST_F(TtlTest, UnregisteredMergeOperator) {
  class UnregisteredMergeOperator : public MergeOperator {
   public:
    const char* Name() const override { return "UnregisteredMergeOperator"; }
  };
  options_.merge_operator = std::make_shared<UnregisteredMergeOperator>();
  OpenTtl();
  CloseTtl();
}

// Insert some key-values which KeyMayExist should be able to get and check that
// values returned are fine
TEST_F(TtlTest, KeyMayExist) {
  MakeKVMap(kSampleSize_);

  OpenTtl();
  PutValues(0, kSampleSize_, false);

  SimpleKeyMayExistCheck();

  CloseTtl();
}

TEST_F(TtlTest, MultiGetTest) {
  MakeKVMap(kSampleSize_);

  OpenTtl();
  PutValues(0, kSampleSize_, false);

  SimpleMultiGetTest();

  CloseTtl();
}

TEST_F(TtlTest, NewIteratorsStripTimestamps) {
  OpenTtl();
  ColumnFamilyHandle* second_cf;
  ASSERT_OK(
      db_ttl_->CreateColumnFamilyWithTtl(options_, "second", &second_cf, 0));

  const std::vector<ColumnFamilyHandle*> column_families = {
      db_ttl_->DefaultColumnFamily(), second_cf};
  const std::vector<std::string> values = {"default-value", "second-value"};
  for (size_t i = 0; i < column_families.size(); ++i) {
    ASSERT_OK(
        db_ttl_->Put(WriteOptions(), column_families[i], "key", values[i]));
  }

  const auto verify_iterators = [&](const Status& status,
                                    std::vector<Iterator*>* iterators) {
    ASSERT_OK(status);
    ASSERT_EQ(iterators->size(), column_families.size());
    for (size_t i = 0; i < iterators->size(); ++i) {
      std::unique_ptr<Iterator> iterator((*iterators)[i]);
      iterator->SeekToFirst();
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(iterator->value().ToString(), values[i]);
      ASSERT_OK(iterator->status());
    }
  };

  std::vector<Iterator*> iterators;
  verify_iterators(
      db_ttl_->NewIterators(ReadOptions(), column_families, &iterators),
      &iterators);
  verify_iterators(
      db_ttl_->NewIterators(std::vector<ReadOptions>(column_families.size()),
                            column_families, &iterators),
      &iterators);

  delete second_cf;
}

TEST_F(TtlTest, TtlFiftenYears) {
  MakeKVMap(kSampleSize_);
  // 15 year will lead int32_t overflow from now
  const int kFifteenYearSeconds = 86400 * 365 * 15;
  OpenTtl(kFifteenYearSeconds);
  PutValues(0, kSampleSize_, true);
  // trigger the compaction
  SleepCompactCheck(1, 0, kSampleSize_);
  CloseTtl();
}

TEST_F(TtlTest, ColumnFamiliesTest) {
  std::unique_ptr<DB> db;
  Options options;
  options.create_if_missing = true;
  options.env = env_.get();

  ASSERT_OK(DB::Open(options, dbname_, &db));
  ColumnFamilyHandle* handle;
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(options),
                                   "ttl_column_family", &handle));

  delete handle;
  db.reset();

  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName,
                               ColumnFamilyOptions(options));
  column_families.emplace_back("ttl_column_family",
                               ColumnFamilyOptions(options));

  std::vector<ColumnFamilyHandle*> handles;

  ASSERT_OK(DBWithTTL::Open(DBOptions(options), dbname_, column_families,
                            &handles, &db_ttl_, {3, 5}, false));
  ASSERT_EQ(handles.size(), 2U);
  ColumnFamilyHandle* new_handle;
  ASSERT_OK(db_ttl_->CreateColumnFamilyWithTtl(options, "ttl_column_family_2",
                                               &new_handle, 2));
  handles.push_back(new_handle);

  MakeKVMap(kSampleSize_);
  PutValues(0, kSampleSize_, false, handles[0]);
  PutValues(0, kSampleSize_, false, handles[1]);
  PutValues(0, kSampleSize_, false, handles[2]);

  // everything should be there after 1 second
  SleepCompactCheck(1, 0, kSampleSize_, true, false, handles[0]);
  SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[1]);
  SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[2]);

  // only column family 1 should be alive after 4 seconds
  SleepCompactCheck(3, 0, kSampleSize_, false, false, handles[0]);
  SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[1]);
  SleepCompactCheck(0, 0, kSampleSize_, false, false, handles[2]);

  // nothing should be there after 6 seconds
  SleepCompactCheck(2, 0, kSampleSize_, false, false, handles[0]);
  SleepCompactCheck(0, 0, kSampleSize_, false, false, handles[1]);
  SleepCompactCheck(0, 0, kSampleSize_, false, false, handles[2]);

  for (auto h : handles) {
    delete h;
  }
  delete db_ttl_;
  db_ttl_ = nullptr;
}

// Puts a set of values and checks its absence using Get after ttl
TEST_F(TtlTest, ChangeTtlOnOpenDb) {
  MakeKVMap(kSampleSize_);

  OpenTtl(1);  // T=0:Open the db with ttl = 2
  SetTtl(3);
  int32_t ttl = 0;
  ASSERT_OK(db_ttl_->GetTtl(db_ttl_->DefaultColumnFamily(), &ttl));
  ASSERT_EQ(ttl, 3);
  PutValues(0, kSampleSize_);  // T=0:Insert Set1. Delete at t=2
  SleepCompactCheck(2, 0, kSampleSize_, true);  // T=2:Set1 should be there
  CloseTtl();
}

// Test DeleteRange for DBWithTtl
TEST_F(TtlTest, DeleteRangeTest) {
  OpenTtl();
  ASSERT_OK(db_ttl_->Put(WriteOptions(), "a", "val"));
  MakeDeleteRange("a", "b");
  ASSERT_OK(db_ttl_->Put(WriteOptions(), "c", "val"));
  MakeDeleteRange("b", "d");
  ASSERT_OK(db_ttl_->Put(WriteOptions(), "e", "val"));
  MakeDeleteRange("d", "e");
  // first iteration verifies query correctness in memtable, second verifies
  // query correctness for a single SST file
  for (int i = 0; i < 2; i++) {
    if (i > 0) {
      ASSERT_OK(db_ttl_->Flush(FlushOptions()));
    }
    std::string value;
    ASSERT_TRUE(db_ttl_->Get(ReadOptions(), "a", &value).IsNotFound());
    ASSERT_TRUE(db_ttl_->Get(ReadOptions(), "c", &value).IsNotFound());
    ASSERT_OK(db_ttl_->Get(ReadOptions(), "e", &value));
  }
  CloseTtl();
}

class DummyFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return false;
  }

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "DummyFilter"; }
};

class DummyFilterFactory : public CompactionFilterFactory {
 public:
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "DummyFilterFactory"; }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context&) override {
    std::unique_ptr<CompactionFilter> f(new DummyFilter());
    return f;
  }
};

static int RegisterTestObjects(ObjectLibrary& library,
                               const std::string& /*arg*/) {
  library.AddFactory<CompactionFilter>(
      "DummyFilter", [](const std::string& /*uri*/,
                        std::unique_ptr<CompactionFilter>* /*guard*/,
                        std::string* /* errmsg */) {
        static DummyFilter dummy;
        return &dummy;
      });
  library.AddFactory<CompactionFilterFactory>(
      "DummyFilterFactory", [](const std::string& /*uri*/,
                               std::unique_ptr<CompactionFilterFactory>* guard,
                               std::string* /* errmsg */) {
        guard->reset(new DummyFilterFactory());
        return guard->get();
      });
  return 2;
}

class TtlOptionsTest : public testing::Test {
 public:
  TtlOptionsTest() {
    config_options_.registry->AddLibrary("RegisterTtlObjects",
                                         RegisterTtlObjects, "");
    config_options_.registry->AddLibrary("RegisterTtlTestObjects",
                                         RegisterTestObjects, "");
  }
  ConfigOptions config_options_;
};

TEST_F(TtlOptionsTest, LoadTtlCompactionFilter) {
  const CompactionFilter* filter = nullptr;

  ASSERT_OK(CompactionFilter::CreateFromString(
      config_options_, TtlCompactionFilter::kClassName(), &filter));
  ASSERT_NE(filter, nullptr);
  ASSERT_STREQ(filter->Name(), TtlCompactionFilter::kClassName());
  auto ttl = filter->GetOptions<int32_t>("TTL");
  ASSERT_NE(ttl, nullptr);
  ASSERT_EQ(*ttl, 0);
  ASSERT_OK(filter->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  delete filter;
  filter = nullptr;

  ASSERT_OK(CompactionFilter::CreateFromString(
      config_options_, "id=TtlCompactionFilter; ttl=123", &filter));
  ASSERT_NE(filter, nullptr);
  ttl = filter->GetOptions<int32_t>("TTL");
  ASSERT_NE(ttl, nullptr);
  ASSERT_EQ(*ttl, 123);
  ASSERT_OK(filter->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  delete filter;
  filter = nullptr;

  ASSERT_OK(CompactionFilter::CreateFromString(
      config_options_,
      "id=TtlCompactionFilter; ttl=456; user_filter=DummyFilter;", &filter));
  ASSERT_NE(filter, nullptr);
  auto inner = filter->CheckedCast<DummyFilter>();
  ASSERT_NE(inner, nullptr);
  ASSERT_OK(filter->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  std::string mismatch;
  std::string opts_str = filter->ToString(config_options_);
  const CompactionFilter* copy = nullptr;
  ASSERT_OK(
      CompactionFilter::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(filter->AreEquivalent(config_options_, copy, &mismatch));
  delete filter;
  delete copy;
}

TEST_F(TtlOptionsTest, LoadTtlCompactionFilterFactory) {
  std::shared_ptr<CompactionFilterFactory> cff;

  ASSERT_OK(CompactionFilterFactory::CreateFromString(
      config_options_, TtlCompactionFilterFactory::kClassName(), &cff));
  ASSERT_NE(cff.get(), nullptr);
  ASSERT_STREQ(cff->Name(), TtlCompactionFilterFactory::kClassName());
  auto ttl = cff->GetOptions<int32_t>("TTL");
  ASSERT_NE(ttl, nullptr);
  ASSERT_EQ(*ttl, 0);
  ASSERT_OK(cff->ValidateOptions(DBOptions(), ColumnFamilyOptions()));

  ASSERT_OK(CompactionFilterFactory::CreateFromString(
      config_options_, "id=TtlCompactionFilterFactory; ttl=123", &cff));
  ASSERT_NE(cff.get(), nullptr);
  ASSERT_STREQ(cff->Name(), TtlCompactionFilterFactory::kClassName());
  ttl = cff->GetOptions<int32_t>("TTL");
  ASSERT_NE(ttl, nullptr);
  ASSERT_EQ(*ttl, 123);
  ASSERT_OK(cff->ValidateOptions(DBOptions(), ColumnFamilyOptions()));

  ASSERT_OK(CompactionFilterFactory::CreateFromString(
      config_options_,
      "id=TtlCompactionFilterFactory; ttl=456; "
      "user_filter_factory=DummyFilterFactory;",
      &cff));
  ASSERT_NE(cff.get(), nullptr);
  auto filter = cff->CreateCompactionFilter(CompactionFilter::Context());
  ASSERT_NE(filter.get(), nullptr);
  auto ttlf = filter->CheckedCast<TtlCompactionFilter>();
  ASSERT_EQ(filter.get(), ttlf);
  auto user = filter->CheckedCast<DummyFilter>();
  ASSERT_NE(user, nullptr);
  ASSERT_OK(cff->ValidateOptions(DBOptions(), ColumnFamilyOptions()));

  std::string opts_str = cff->ToString(config_options_);
  std::string mismatch;
  std::shared_ptr<CompactionFilterFactory> copy;
  ASSERT_OK(CompactionFilterFactory::CreateFromString(config_options_, opts_str,
                                                      &copy));
  ASSERT_TRUE(cff->AreEquivalent(config_options_, copy.get(), &mismatch));
}

TEST_F(TtlOptionsTest, LoadTtlMergeOperator) {
  std::shared_ptr<MergeOperator> mo;

  config_options_.invoke_prepare_options = false;
  ASSERT_OK(MergeOperator::CreateFromString(
      config_options_, TtlMergeOperator::kClassName(), &mo));
  ASSERT_NE(mo.get(), nullptr);
  ASSERT_STREQ(mo->Name(), TtlMergeOperator::kClassName());
  ASSERT_NOK(mo->ValidateOptions(DBOptions(), ColumnFamilyOptions()));

  config_options_.invoke_prepare_options = true;
  ASSERT_OK(MergeOperator::CreateFromString(
      config_options_, "id=TtlMergeOperator; user_operator=bytesxor", &mo));
  ASSERT_NE(mo.get(), nullptr);
  ASSERT_STREQ(mo->Name(), TtlMergeOperator::kClassName());
  ASSERT_OK(mo->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  auto ttl_mo = mo->CheckedCast<TtlMergeOperator>();
  ASSERT_EQ(mo.get(), ttl_mo);
  auto user = ttl_mo->CheckedCast<BytesXOROperator>();
  ASSERT_NE(user, nullptr);

  std::string mismatch;
  std::string opts_str = mo->ToString(config_options_);
  std::shared_ptr<MergeOperator> copy;
  ASSERT_OK(MergeOperator::CreateFromString(config_options_, opts_str, &copy));
  ASSERT_TRUE(mo->AreEquivalent(config_options_, copy.get(), &mismatch));

  // An unregistered user_operator will be null, which is not supported by the
  // `TtlMergeOperator` implementation.
  ASSERT_OK(MergeOperator::CreateFromString(
      config_options_, "id=TtlMergeOperator; user_operator=unknown", &mo));
  ASSERT_NE(mo.get(), nullptr);
  ASSERT_STREQ(mo->Name(), TtlMergeOperator::kClassName());
  ASSERT_NOK(mo->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
}
}  // namespace ROCKSDB_NAMESPACE

// A black-box test for the ttl wrapper around rocksdb
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
