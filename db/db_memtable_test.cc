//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/db_test_util.h"
#include "db/memtable.h"
#include "db/range_del_aggregator.h"
#include "memory/arena.h"
#include "port/stack_trace.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/defer.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class DBMemTableTest : public DBTestBase {
 public:
  DBMemTableTest() : DBTestBase("db_memtable_test", /*env_do_fsync=*/true) {}
};

class MockMemTableRep : public MemTableRep {
 public:
  explicit MockMemTableRep(Allocator* allocator, MemTableRep* rep)
      : MemTableRep(allocator), rep_(rep), num_insert_with_hint_(0) {}

  KeyHandle Allocate(const size_t len, char** buf) override {
    return rep_->Allocate(len, buf);
  }

  void Insert(KeyHandle handle) override { rep_->Insert(handle); }

  void InsertWithHint(KeyHandle handle, void** hint) override {
    num_insert_with_hint_++;
    EXPECT_NE(nullptr, hint);
    last_hint_in_ = *hint;
    rep_->InsertWithHint(handle, hint);
    last_hint_out_ = *hint;
  }

  bool Contains(const char* key) const override { return rep_->Contains(key); }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    rep_->Get(k, callback_args, callback_func);
  }

  size_t ApproximateMemoryUsage() override {
    return rep_->ApproximateMemoryUsage();
  }

  Iterator* GetIterator(Arena* arena) override {
    return rep_->GetIterator(arena);
  }

  void* last_hint_in() { return last_hint_in_; }
  void* last_hint_out() { return last_hint_out_; }
  int num_insert_with_hint() { return num_insert_with_hint_; }

 private:
  std::unique_ptr<MemTableRep> rep_;
  void* last_hint_in_ = nullptr;
  void* last_hint_out_ = nullptr;
  int num_insert_with_hint_;
};

class MockMemTableRepFactory : public MemTableRepFactory {
 public:
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override {
    SkipListFactory factory;
    MemTableRep* skiplist_rep =
        factory.CreateMemTableRep(cmp, allocator, transform, logger);
    mock_rep_ = new MockMemTableRep(allocator, skiplist_rep);
    return mock_rep_;
  }

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger,
                                 uint32_t column_family_id) override {
    last_column_family_id_ = column_family_id;
    return CreateMemTableRep(cmp, allocator, transform, logger);
  }

  const char* Name() const override { return "MockMemTableRepFactory"; }

  MockMemTableRep* rep() { return mock_rep_; }

  bool IsInsertConcurrentlySupported() const override { return false; }

  uint32_t GetLastColumnFamilyId() { return last_column_family_id_; }

 private:
  MockMemTableRep* mock_rep_ = nullptr;
  // workaround since there's no std::numeric_limits<uint32_t>::max() yet.
  uint32_t last_column_family_id_ = static_cast<uint32_t>(-1);
};

class TestPrefixExtractor : public SliceTransform {
 public:
  const char* Name() const override { return "TestPrefixExtractor"; }

  Slice Transform(const Slice& key) const override {
    const char* p = separator(key);
    if (p == nullptr) {
      return Slice();
    }
    return Slice(key.data(), p - key.data() + 1);
  }

  bool InDomain(const Slice& key) const override {
    return separator(key) != nullptr;
  }

 private:
  const char* separator(const Slice& key) const {
    return static_cast<const char*>(memchr(key.data(), '_', key.size()));
  }
};

// Test that ::Add properly returns false when inserting duplicate keys
TEST_F(DBMemTableTest, DuplicateSeq) {
  SequenceNumber seq = 123;
  std::string value;
  MergeContext merge_context;
  Options options;
  InternalKeyComparator ikey_cmp(options.comparator);
  ReadRangeDelAggregator range_del_agg(&ikey_cmp,
                                       kMaxSequenceNumber /* upper_bound */);

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                               kMaxSequenceNumber, 0 /* column_family_id */,
                               nullptr /* flush_initiator */);

  // Write some keys and make sure it returns false on duplicates
  ASSERT_OK(
      mem->Add(seq, kTypeValue, "key", "value2", nullptr /* kv_prot_info */));
  ASSERT_TRUE(
      mem->Add(seq, kTypeValue, "key", "value2", nullptr /* kv_prot_info */)
          .IsTryAgain());
  // Changing the type should still cause the duplicatae key
  ASSERT_TRUE(
      mem->Add(seq, kTypeMerge, "key", "value2", nullptr /* kv_prot_info */)
          .IsTryAgain());
  // Changing the seq number will make the key fresh
  ASSERT_OK(mem->Add(seq + 1, kTypeMerge, "key", "value2",
                     nullptr /* kv_prot_info */));
  // Test with different types for duplicate keys
  ASSERT_TRUE(
      mem->Add(seq, kTypeDeletion, "key", "", nullptr /* kv_prot_info */)
          .IsTryAgain());
  ASSERT_TRUE(
      mem->Add(seq, kTypeSingleDeletion, "key", "", nullptr /* kv_prot_info */)
          .IsTryAgain());

  ASSERT_EQ(mem->NumEntries(), 2u);
  ASSERT_EQ(mem->NumDeletion(), 0u);
  ASSERT_EQ(mem->NumRangeDeletion(), 0u);

  // Test the duplicate keys under stress
  for (int i = 0; i < 10000; i++) {
    bool insert_dup = i % 10 == 1;
    if (!insert_dup) {
      seq++;
    }
    Status s = mem->Add(seq, kTypeValue, "foo", "value" + std::to_string(seq),
                        nullptr /* kv_prot_info */);
    if (insert_dup) {
      ASSERT_TRUE(s.IsTryAgain());
    } else {
      ASSERT_OK(s);
    }
  }
  delete mem;

  // Test with InsertWithHint
  // TestPrefixExtractor uses _ to extract the prefix
  options.memtable_insert_with_hint_prefix_extractor =
      std::make_shared<TestPrefixExtractor>();
  ioptions = ImmutableOptions(options);
  mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                     kMaxSequenceNumber, 0 /* column_family_id */,
                     nullptr /* flush_initiator */);
  // Insert a duplicate key with _ in it
  ASSERT_OK(
      mem->Add(seq, kTypeValue, "key_1", "value", nullptr /* kv_prot_info */));
  ASSERT_TRUE(
      mem->Add(seq, kTypeValue, "key_1", "value", nullptr /* kv_prot_info */)
          .IsTryAgain());
  delete mem;

  // Test when InsertConcurrently will be invoked
  options.allow_concurrent_memtable_write = true;
  ioptions = ImmutableOptions(options);
  mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                     kMaxSequenceNumber, 0 /* column_family_id */,
                     nullptr /* flush_initiator */);
  MemTablePostProcessInfo post_process_info;
  ASSERT_OK(mem->Add(seq, kTypeValue, "key", "value",
                     nullptr /* kv_prot_info */, true, &post_process_info));
  ASSERT_TRUE(mem->Add(seq, kTypeValue, "key", "value",
                       nullptr /* kv_prot_info */, true, &post_process_info)
                  .IsTryAgain());

  mem->BatchPostProcess(post_process_info);
  ASSERT_EQ(mem->NumEntries(), 1u);
  ASSERT_EQ(mem->NumDeletion(), 0u);
  ASSERT_EQ(mem->NumRangeDeletion(), 0u);
  delete mem;
}

// A simple test to verify that the concurrent merge writes is functional
TEST_F(DBMemTableTest, ConcurrentMergeWrite) {
  int num_ops = 1000;
  std::string value;
  MergeContext merge_context;
  Options options;
  // A merge operator that is not sensitive to concurrent writes since in this
  // test we don't order the writes.
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  options.allow_concurrent_memtable_write = true;
  ImmutableOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                               kMaxSequenceNumber, 0 /* column_family_id */,
                               nullptr /* flush_initiator */);

  // Put 0 as the base
  PutFixed64(&value, static_cast<uint64_t>(0));
  ASSERT_OK(mem->Add(0, kTypeValue, "key", value, nullptr /* kv_prot_info */));
  value.clear();

  // Write Merge concurrently
  ROCKSDB_NAMESPACE::port::Thread write_thread1([&]() {
    MemTablePostProcessInfo post_process_info1;
    std::string v1;
    for (int seq = 1; seq < num_ops / 2; seq++) {
      PutFixed64(&v1, seq);
      ASSERT_OK(mem->Add(seq, kTypeMerge, "key", v1, nullptr /* kv_prot_info */,
                         true, &post_process_info1));
      v1.clear();
    }
    mem->BatchPostProcess(post_process_info1);
  });
  ROCKSDB_NAMESPACE::port::Thread write_thread2([&]() {
    MemTablePostProcessInfo post_process_info2;
    std::string v2;
    for (int seq = num_ops / 2; seq < num_ops; seq++) {
      PutFixed64(&v2, seq);
      ASSERT_OK(mem->Add(seq, kTypeMerge, "key", v2, nullptr /* kv_prot_info */,
                         true, &post_process_info2));
      v2.clear();
    }
    mem->BatchPostProcess(post_process_info2);
  });
  write_thread1.join();
  write_thread2.join();
  // 1 non-concurrent Put + (num_ops - 1) concurrent Merges
  ASSERT_EQ(mem->NumEntries(), static_cast<uint64_t>(num_ops));
  ASSERT_EQ(mem->NumDeletion(), 0u);
  ASSERT_EQ(mem->NumRangeDeletion(), 0u);

  Status status;
  ReadOptions roptions;
  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey("key", kMaxSequenceNumber);
  bool res = mem->Get(lkey, &value, /*columns=*/nullptr, /*timestamp=*/nullptr,
                      &status, &merge_context, &max_covering_tombstone_seq,
                      roptions, false /* immutable_memtable */);
  ASSERT_OK(status);
  ASSERT_TRUE(res);
  uint64_t ivalue = DecodeFixed64(Slice(value).data());
  uint64_t sum = 0;
  for (int seq = 0; seq < num_ops; seq++) {
    sum += seq;
  }
  ASSERT_EQ(ivalue, sum);

  delete mem;
}

TEST_F(DBMemTableTest, InsertWithHint) {
  Options options;
  options.allow_concurrent_memtable_write = false;
  options.create_if_missing = true;
  options.memtable_factory = std::make_shared<MockMemTableRepFactory>();
  options.memtable_insert_with_hint_prefix_extractor =
      std::make_shared<TestPrefixExtractor>();
  options.env = env_;
  Reopen(options);
  MockMemTableRep* rep =
      static_cast<MockMemTableRepFactory*>(options.memtable_factory.get())
          ->rep();
  ASSERT_OK(Put("foo_k1", "foo_v1"));
  ASSERT_EQ(nullptr, rep->last_hint_in());
  void* hint_foo = rep->last_hint_out();
  ASSERT_OK(Put("foo_k2", "foo_v2"));
  ASSERT_EQ(hint_foo, rep->last_hint_in());
  ASSERT_EQ(hint_foo, rep->last_hint_out());
  ASSERT_OK(Put("foo_k3", "foo_v3"));
  ASSERT_EQ(hint_foo, rep->last_hint_in());
  ASSERT_EQ(hint_foo, rep->last_hint_out());
  ASSERT_OK(Put("bar_k1", "bar_v1"));
  ASSERT_EQ(nullptr, rep->last_hint_in());
  void* hint_bar = rep->last_hint_out();
  ASSERT_NE(hint_foo, hint_bar);
  ASSERT_OK(Put("bar_k2", "bar_v2"));
  ASSERT_EQ(hint_bar, rep->last_hint_in());
  ASSERT_EQ(hint_bar, rep->last_hint_out());
  ASSERT_EQ(5, rep->num_insert_with_hint());
  ASSERT_OK(Put("NotInPrefixDomain", "vvv"));
  ASSERT_EQ(5, rep->num_insert_with_hint());
  ASSERT_EQ("foo_v1", Get("foo_k1"));
  ASSERT_EQ("foo_v2", Get("foo_k2"));
  ASSERT_EQ("foo_v3", Get("foo_k3"));
  ASSERT_EQ("bar_v1", Get("bar_k1"));
  ASSERT_EQ("bar_v2", Get("bar_k2"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "foo_k1", "foo_k4"));
  ASSERT_EQ(hint_bar, rep->last_hint_in());
  ASSERT_EQ(hint_bar, rep->last_hint_out());
  ASSERT_EQ(5, rep->num_insert_with_hint());
  ASSERT_EQ("vvv", Get("NotInPrefixDomain"));
}

TEST_F(DBMemTableTest, ColumnFamilyId) {
  // Verifies MemTableRepFactory is told the right column family id.
  Options options;
  options.env = CurrentOptions().env;
  options.allow_concurrent_memtable_write = false;
  options.create_if_missing = true;
  options.memtable_factory = std::make_shared<MockMemTableRepFactory>();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  for (uint32_t cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Flush(cf));
    ASSERT_EQ(
        cf, static_cast<MockMemTableRepFactory*>(options.memtable_factory.get())
                ->GetLastColumnFamilyId());
  }
}

class DBMemTableTestForSeek : public DBMemTableTest,
                              virtual public ::testing::WithParamInterface<
                                  std::tuple<bool, bool, bool>> {};

TEST_P(DBMemTableTestForSeek, IntegrityChecks) {
  // Validate key corruption could be detected during seek.
  // We insert many keys into skiplist. Then we corrupt the each key one at a
  // time. With memtable_verify_per_key_checksum_on_seek enabled, when the
  // corrupted key is searched, the checksum of every key visited during the
  // seek is validated. It will report data corruption. Otherwise seek returns
  // not found.
  auto allow_data_in_error = std::get<0>(GetParam());
  Options options = CurrentOptions();
  options.allow_data_in_errors = allow_data_in_error;
  options.paranoid_memory_checks = std::get<1>(GetParam());
  options.memtable_verify_per_key_checksum_on_seek = std::get<2>(GetParam());
  options.memtable_protection_bytes_per_key = 8;
  DestroyAndReopen(options);

  // capture the data pointer of all of the keys
  std::vector<char*> raw_data_pointer;

  // Insert enough keys, so memtable would create multiple levels.
  auto key_count = 100;
  for (int i = 0; i < key_count; i++) {
    // The last digit of the key will be corrupted from value 0 to value 5
    ASSERT_OK(Put(Key(i * 10), "val0"));
  }

  ReadOptions rops;

  // Iterate all the keys to get key pointers
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->SetCallBack("InlineSkipList::Iterator::Next::key",
                                        [&raw_data_pointer](void* key) {
                                          auto p = static_cast<char*>(key);
                                          raw_data_pointer.push_back(p);
                                        });
  SyncPoint::GetInstance()->EnableProcessing();

  {
    std::unique_ptr<Iterator> iter{db_->NewIterator(rops)};
    iter->Seek(Key(0));
    while (iter->Valid()) {
      ASSERT_OK(iter->status());
      iter->Next();
    }
    // check status after valid returned false.
    auto status = iter->status();
    ASSERT_TRUE(status.ok());
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(raw_data_pointer.size(), key_count);

  bool enable_key_validation_on_seek =
      options.memtable_verify_per_key_checksum_on_seek;

  // For each key, corrupt it, validate corruption is detected correctly, then
  // revert it.
  for (int i = 0; i < key_count; i++) {
    std::string key_to_corrupt = Key(i * 10);
    raw_data_pointer[i][key_to_corrupt.size()] = '5';

    auto corrupted_key = key_to_corrupt;
    corrupted_key.data()[key_to_corrupt.size() - 1] = '5';
    auto corrupted_key_slice =
        Slice(corrupted_key.data(), corrupted_key.length());
    auto corrupted_key_hex = corrupted_key_slice.ToString(/*hex=*/true);

    {
      // Test Get API
      std::string val;
      auto status = db_->Get(rops, key_to_corrupt, &val);
      if (enable_key_validation_on_seek) {
        ASSERT_TRUE(status.IsCorruption()) << key_to_corrupt;
        ASSERT_EQ(
            status.ToString().find(corrupted_key_hex) != std::string::npos,
            allow_data_in_error)
            << status.ToString() << "\n"
            << corrupted_key_hex;
      } else {
        ASSERT_TRUE(status.IsNotFound());
      }
    }

    {
      // Test MultiGet API
      std::vector<std::string> vals;
      std::vector<Status> statuses = db_->MultiGet(
          rops, {db_->DefaultColumnFamily()}, {key_to_corrupt}, &vals, nullptr);
      if (enable_key_validation_on_seek) {
        ASSERT_TRUE(statuses[0].IsCorruption());
        ASSERT_EQ(
            statuses[0].ToString().find(corrupted_key_hex) != std::string::npos,
            allow_data_in_error);
      } else {
        ASSERT_TRUE(statuses[0].IsNotFound());
      }
    }

    {
      // Test Iterator Seek API
      std::unique_ptr<Iterator> iter{db_->NewIterator(rops)};
      ASSERT_OK(iter->status());
      iter->Seek(key_to_corrupt);
      auto status = iter->status();
      if (enable_key_validation_on_seek) {
        ASSERT_TRUE(status.IsCorruption());
        ASSERT_EQ(
            status.ToString().find(corrupted_key_hex) != std::string::npos,
            allow_data_in_error);
      } else {
        ASSERT_FALSE(iter->Valid());
        ASSERT_FALSE(status.ok());
      }
    }

    // revert the key corruption.
    raw_data_pointer[i][key_to_corrupt.size()] = '0';
  }
}

INSTANTIATE_TEST_CASE_P(DBMemTableTestForSeek, DBMemTableTestForSeek,
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool()));

TEST_F(DBMemTableTest, IntegrityChecks) {
  // We insert keys key000000, key000001 and key000002 into skiplist at fixed
  // height 1 (smallest height). Then we corrupt the second key to aey000001 to
  // make it smaller. With `paranoid_memory_checks` set to true, if the
  // skip list sees key000000 and then aey000001, then it will report out of
  // order keys with corruption status. With `paranoid_memory_checks` set
  // to false, read/scan may return wrong results.
  for (bool allow_data_in_error : {false, true}) {
    Options options = CurrentOptions();
    options.allow_data_in_errors = allow_data_in_error;
    options.paranoid_memory_checks = true;
    DestroyAndReopen(options);
    SyncPoint::GetInstance()->SetCallBack(
        "InlineSkipList::RandomHeight::height", [](void* h) {
          auto height_ptr = static_cast<int*>(h);
          *height_ptr = 1;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(Put(Key(0), "val0"));
    ASSERT_OK(Put(Key(2), "val2"));
    // p will point to the buffer for encoded key000001
    char* p = nullptr;
    SyncPoint::GetInstance()->SetCallBack(
        "MemTable::Add:BeforeReturn:Encoded", [&](void* encoded) {
          p = const_cast<char*>(static_cast<Slice*>(encoded)->data());
        });
    ASSERT_OK(Put(Key(1), "val1"));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_TRUE(p);
    // Offset 0 is key size, key bytes start at offset 1.
    // "key000001 -> aey000001"
    p[1] = 'a';

    ReadOptions rops;
    std::string val;
    Status s = db_->Get(rops, Key(1), &val);
    ASSERT_TRUE(s.IsCorruption());
    std::string key0 = Slice(Key(0)).ToString(true);
    ASSERT_EQ(s.ToString().find(key0) != std::string::npos,
              allow_data_in_error);
    // Without `paranoid_memory_checks`, NotFound will be returned.
    // This would fail an assertion in InlineSkipList::FindGreaterOrEqual().
    // If we remove the assertion, this passes.
    // ASSERT_TRUE(db_->Get(ReadOptions(), Key(1), &val).IsNotFound());

    std::vector<std::string> vals;
    std::vector<Status> statuses = db_->MultiGet(
        rops, {db_->DefaultColumnFamily()}, {Key(1)}, &vals, nullptr);
    ASSERT_TRUE(statuses[0].IsCorruption());
    ASSERT_EQ(statuses[0].ToString().find(key0) != std::string::npos,
              allow_data_in_error);

    std::unique_ptr<Iterator> iter{db_->NewIterator(rops)};
    ASSERT_OK(iter->status());
    iter->Seek(Key(1));
    ASSERT_TRUE(iter->status().IsCorruption());
    ASSERT_EQ(iter->status().ToString().find(key0) != std::string::npos,
              allow_data_in_error);

    iter->Seek(Key(0));
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    // iterating through skip list at height at 1 should catch out-of-order keys
    iter->Next();
    ASSERT_TRUE(iter->status().IsCorruption());
    ASSERT_EQ(iter->status().ToString().find(key0) != std::string::npos,
              allow_data_in_error);
    ASSERT_FALSE(iter->Valid());

    iter->SeekForPrev(Key(2));
    ASSERT_TRUE(iter->status().IsCorruption());
    ASSERT_EQ(iter->status().ToString().find(key0) != std::string::npos,
              allow_data_in_error);

    // Internally DB Iter will iterate backwards (call Prev()) after
    // SeekToLast() to find the correct internal key with the last user key.
    // Prev() will do integrity checks and catch corruption.
    iter->SeekToLast();
    ASSERT_TRUE(iter->status().IsCorruption());
    ASSERT_EQ(iter->status().ToString().find(key0) != std::string::npos,
              allow_data_in_error);
    ASSERT_FALSE(iter->Valid());
  }
}

namespace {
constexpr size_t kSortedHashBucketCount = 8192;

Options SortedHashOptions(Options options) {
  options.create_if_missing = true;
  options.memtable_factory.reset(
      NewSortedHashRepFactory(kSortedHashBucketCount));
  return options;
}

// Bytewise ordering that keeps the default
// CanKeysWithDifferentByteContentsBeEqual() == true.
class ByteDifferentKeysMayBeEqualComparator : public Comparator {
 public:
  const char* Name() const override {
    return "ByteDifferentKeysMayBeEqualComparator";
  }
  int Compare(const Slice& a, const Slice& b) const override {
    return BytewiseComparator()->Compare(a, b);
  }
  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}
  void FindShortSuccessor(std::string* /*key*/) const override {}
};

std::vector<std::pair<std::string, std::string>> ReadAll(Iterator* iter) {
  std::vector<std::pair<std::string, std::string>> contents;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    contents.emplace_back(iter->key().ToString(), iter->value().ToString());
  }
  EXPECT_OK(iter->status());
  return contents;
}
}  // namespace

TEST_F(DBMemTableTest, SortedHashConcurrentInsertAndFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.allow_concurrent_memtable_write = true;
  options.memtable_batch_lookup_optimization = true;
  // One bucket forces all writers through the same collision chain.
  options.memtable_factory.reset(NewSortedHashRepFactory(1));
  DestroyAndReopen(options);

  constexpr int kNumThreads = 8;
  constexpr int kEntriesPerThread = 128;
  std::vector<Status> write_statuses(kNumThreads);
  std::vector<port::Thread> threads;
  threads.reserve(kNumThreads);
  for (int thread = 0; thread < kNumThreads; ++thread) {
    threads.emplace_back([&, thread]() {
      const int begin = thread * kEntriesPerThread;
      WriteBatch batch;
      for (int key = begin; key < begin + kEntriesPerThread; ++key) {
        Status s = batch.Put(Key(key), "value" + std::to_string(key));
        if (!s.ok()) {
          write_statuses[thread] = s;
          return;
        }
      }
      write_statuses[thread] = db_->Write(WriteOptions(), &batch);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (const Status& s : write_statuses) {
    ASSERT_OK(s);
  }

  std::vector<std::string> key_storage;
  std::vector<Slice> keys;
  key_storage.reserve(kNumThreads);
  keys.reserve(kNumThreads);
  for (int thread = 0; thread < kNumThreads; ++thread) {
    key_storage.push_back(Key(thread * kEntriesPerThread));
    keys.emplace_back(key_storage.back());
  }
  std::vector<std::string> values;
  const std::vector<Status> statuses =
      db_->MultiGet(ReadOptions(), keys, &values);
  for (int thread = 0; thread < kNumThreads; ++thread) {
    ASSERT_OK(statuses[thread]);
    ASSERT_EQ(values[thread],
              "value" + std::to_string(thread * kEntriesPerThread));
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  for (int key = 0; key < kNumThreads * kEntriesPerThread; ++key) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(key));
    ASSERT_EQ(iter->value().ToString(), "value" + std::to_string(key));
    iter->Next();
  }
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  iter->SeekToLast();
  for (int key = kNumThreads * kEntriesPerThread - 1; key >= 0; --key) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(key));
    iter->Prev();
  }
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();

  constexpr int kLastKey = kNumThreads * kEntriesPerThread;
  ASSERT_OK(Put(Key(kLastKey), "last-value"));
  iter.reset(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(kLastKey));
  ASSERT_EQ(iter->value().ToString(), "last-value");
  iter.reset();

  ASSERT_OK(Flush());
  Reopen(options);
  ASSERT_EQ(Get(Key(0)), "value0");
  ASSERT_EQ(Get(Key(kLastKey)), "last-value");
}

TEST_F(DBMemTableTest, SortedHashRejectsUnsupportedOptions) {
  ByteDifferentKeysMayBeEqualComparator byte_different_keys_cmp;
  struct Case {
    const char* name;
    std::function<void(Options*)> mutate;
    Status::Code expected;
  };
  const std::vector<Case> cases = {
      {"user-defined timestamps",
       [](Options* o) {
         o->comparator = test::BytewiseComparatorWithU64TsWrapper();
       },
       Status::kNotSupported},
      {"byte-different keys may compare equal",
       [&](Options* o) { o->comparator = &byte_different_keys_cmp; },
       Status::kNotSupported},
      {"paranoid_memory_checks",
       [](Options* o) { o->paranoid_memory_checks = true; },
       Status::kNotSupported},
      {"memtable_verify_per_key_checksum_on_seek",
       [](Options* o) { o->memtable_verify_per_key_checksum_on_seek = true; },
       Status::kNotSupported},
      {"inplace_update_support",
       [](Options* o) { o->inplace_update_support = true; },
       Status::kNotSupported},
      {"max_successive_merges",
       [](Options* o) { o->max_successive_merges = 2; }, Status::kNotSupported},
      {"zero bucket_count",
       [](Options* o) {
         o->memtable_factory.reset(NewSortedHashRepFactory(0));
       },
       Status::kInvalidArgument},
      {"bucket array as large as write_buffer_size",
       [](Options* o) {
         o->write_buffer_size = 64 << 10;
         o->memtable_factory.reset(NewSortedHashRepFactory(4096));
       },
       Status::kInvalidArgument},
  };

  Options base = SortedHashOptions(CurrentOptions());
  // inplace_update_support is rejected earlier with concurrent writes.
  base.allow_concurrent_memtable_write = false;
  DestroyAndReopen(base);
  for (const Case& c : cases) {
    SCOPED_TRACE(c.name);
    Options options = base;
    c.mutate(&options);
    ASSERT_EQ(TryReopen(options).code(), c.expected);

    Reopen(base);
    ColumnFamilyHandle* handle = nullptr;
    ASSERT_EQ(db_->CreateColumnFamily(ColumnFamilyOptions(options),
                                      "unsupported", &handle)
                  .code(),
              c.expected);
    ASSERT_EQ(handle, nullptr);
  }
}

TEST_F(DBMemTableTest, SortedHashRejectsUnsupportedSetOptions) {
  const Options options = SortedHashOptions(CurrentOptions());
  DestroyAndReopen(options);
  const std::string array_sized_write_buffer =
      std::to_string(kSortedHashBucketCount * 16);
  const std::vector<
      std::pair<std::unordered_map<std::string, std::string>, Status::Code>>
      cases = {
          {{{"paranoid_memory_checks", "true"}}, Status::kNotSupported},
          {{{"memtable_verify_per_key_checksum_on_seek", "true"}},
           Status::kNotSupported},
          {{{"max_successive_merges", "2"}}, Status::kNotSupported},
          {{{"write_buffer_size", array_sized_write_buffer}},
           Status::kInvalidArgument},
      };
  for (const auto& [new_options, expected] : cases) {
    SCOPED_TRACE(new_options.begin()->first);
    ASSERT_EQ(db_->SetOptions(new_options).code(), expected);
  }

  const Options current = db_->GetOptions();
  ASSERT_FALSE(current.paranoid_memory_checks);
  ASSERT_FALSE(current.memtable_verify_per_key_checksum_on_seek);
  ASSERT_EQ(current.max_successive_merges, size_t{0});
  ASSERT_EQ(current.write_buffer_size, options.write_buffer_size);
}

TEST_F(DBMemTableTest, SortedHashRejectsTailingIterators) {
  DestroyAndReopen(SortedHashOptions(CurrentOptions()));
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  ASSERT_EQ(iter->status().code(), Status::kNotSupported);

  std::vector<Iterator*> iterators;
  ASSERT_EQ(
      db_->NewIterators(read_options, {db_->DefaultColumnFamily()}, &iterators)
          .code(),
      Status::kNotSupported);
  ASSERT_TRUE(iterators.empty());
}

TEST_F(DBMemTableTest, SortedHashIteratorRefreshSeesNewWrites) {
  DestroyAndReopen(SortedHashOptions(CurrentOptions()));
  using Contents = std::vector<std::pair<std::string, std::string>>;

  ASSERT_OK(Put("a", "v1"));
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  ASSERT_EQ(ReadAll(iter.get()), (Contents{{"a", "v1"}}));

  ASSERT_OK(Put("a", "v2"));
  ASSERT_OK(Put("b", "x"));
  ASSERT_OK(iter->Refresh());
  ASSERT_EQ(ReadAll(iter.get()), (Contents{{"a", "v2"}, {"b", "x"}}));

  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "c"));
  ASSERT_OK(iter->Refresh());
  ASSERT_EQ(ReadAll(iter.get()), (Contents{{"a", "v2"}}));

  // With range tombstones already in the memtable, a refresh that kept the
  // memtable iterator would apply the tombstones but miss the new put.
  ASSERT_OK(Put("b", "y"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("c", "z"));
  ASSERT_OK(iter->Refresh(snapshot));
  ASSERT_EQ(ReadAll(iter.get()), (Contents{{"a", "v2"}, {"b", "y"}}));
  ASSERT_OK(iter->status());
  iter.reset();
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBMemTableTest, SortedHashMemoryUsageIndependentOfReads) {
  std::unique_ptr<MemTableRepFactory> factory(NewSortedHashRepFactory(16));
  const InternalKeyComparator icmp(BytewiseComparator());
  const MemTable::KeyComparator cmp(icmp);
  Arena arena;
  std::unique_ptr<MemTableRep> rep(
      factory->CreateMemTableRep(cmp, &arena, nullptr, nullptr));
  constexpr int kNumEntries = 100;
  for (int i = 0; i < kNumEntries; ++i) {
    std::string entry;
    PutLengthPrefixedSlice(&entry,
                           InternalKey(Key(i), i + 1, kTypeValue).Encode());
    PutLengthPrefixedSlice(&entry, "value");
    char* buf = nullptr;
    const KeyHandle handle = rep->Allocate(entry.size(), &buf);
    memcpy(buf, entry.data(), entry.size());
    ASSERT_TRUE(rep->InsertKey(handle));
  }
  const size_t usage = rep->ApproximateMemoryUsage();
  ASSERT_GT(usage, size_t{0});

  std::unique_ptr<MemTableRep::Iterator> iter(rep->GetIterator());
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(rep->ApproximateMemoryUsage(), usage);
  iter.reset();

  rep->MarkReadOnly();
  ASSERT_EQ(rep->ApproximateMemoryUsage(), usage);
}

TEST_F(DBMemTableTest, SortedHashReadSemantics) {
  Options options = SortedHashOptions(CurrentOptions());
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "a1"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("a", "a2"));
  ASSERT_OK(Put("b", "b1"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Put("c", "c1"));
  ASSERT_OK(SingleDelete("c"));
  ASSERT_OK(Put("d", "d1"));
  ASSERT_OK(Merge("d", "d2"));
  ASSERT_OK(Put("e", "e1"));
  ASSERT_OK(Put("f", "f1"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "e", "f"));
  ASSERT_OK(Put("g", "g1"));

  const std::vector<std::pair<std::string, std::string>> expected = {
      {"a", "a2"}, {"d", "d1,d2"}, {"f", "f1"}, {"g", "g1"}};
  const std::vector<std::pair<std::string, std::string>> expected_at_snapshot =
      {{"a", "a1"}};

  // Run once against the memtable and once after flushing it.
  for (bool flushed : {false, true}) {
    SCOPED_TRACE(flushed ? "after flush" : "memtable");
    if (flushed) {
      ASSERT_OK(Flush());
    }
    for (const auto& [key, value] : expected) {
      ASSERT_EQ(Get(key), value);
    }
    for (const char* key : {"b", "c", "e"}) {
      ASSERT_EQ(Get(key), "NOT_FOUND");
    }
    ASSERT_EQ(Get("a", snapshot), "a1");

    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    ASSERT_EQ(ReadAll(iter.get()), expected);
    ASSERT_OK(iter->status());

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "d");
    iter->Seek("h");
    ASSERT_FALSE(iter->Valid());
    iter->SeekForPrev("e");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "d");
    iter->SeekForPrev("0");
    ASSERT_FALSE(iter->Valid());
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "g");
    iter->SeekToFirst();
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    ReadOptions snapshot_read;
    snapshot_read.snapshot = snapshot;
    iter.reset(db_->NewIterator(snapshot_read));
    ASSERT_EQ(ReadAll(iter.get()), expected_at_snapshot);
    ASSERT_OK(iter->status());
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBMemTableTest, SortedHashIteratorsDuringConcurrentWrites) {
  Options options = SortedHashOptions(CurrentOptions());
  options.allow_concurrent_memtable_write = true;
  DestroyAndReopen(options);

  constexpr int kNumWriters = 4;
  constexpr int kKeysPerWriter = 500;
  std::array<std::atomic<int>, kNumWriters> completed{};
  std::atomic<int> finished_writers{0};
  std::vector<Status> write_statuses(kNumWriters);
  std::vector<port::Thread> writers;
  writers.reserve(kNumWriters);
  for (int writer = 0; writer < kNumWriters; ++writer) {
    writers.emplace_back([&, writer]() {
      for (int i = 0; i < kKeysPerWriter; ++i) {
        Status s = Put(Key(writer * kKeysPerWriter + i), "v");
        if (!s.ok()) {
          write_statuses[writer] = s;
          break;
        }
        completed[writer].store(i + 1, std::memory_order_release);
      }
      finished_writers.fetch_add(1, std::memory_order_release);
    });
  }
  Defer join_writers([&writers]() {
    for (auto& writer : writers) {
      writer.join();
    }
  });

  bool writers_done = false;
  while (!writers_done) {
    // Read before the progress counters so the last pass checks final counts.
    writers_done =
        finished_writers.load(std::memory_order_acquire) == kNumWriters;
    std::array<int, kNumWriters> completed_before{};
    for (int writer = 0; writer < kNumWriters; ++writer) {
      completed_before[writer] =
          completed[writer].load(std::memory_order_acquire);
    }

    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    std::set<std::string> seen;
    std::string previous;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      const std::string key = iter->key().ToString();
      ASSERT_LT(previous, key);
      seen.insert(key);
      previous = key;
    }
    ASSERT_OK(iter->status());
    for (int writer = 0; writer < kNumWriters; ++writer) {
      for (int i = 0; i < completed_before[writer]; ++i) {
        ASSERT_EQ(seen.count(Key(writer * kKeysPerWriter + i)), 1u);
      }
    }
  }
  for (const Status& s : write_statuses) {
    ASSERT_OK(s);
  }
}

TEST_F(DBMemTableTest, SortedHashDuplicateSeq) {
  Options options;
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory.reset(NewSortedHashRepFactory(16));
  InternalKeyComparator cmp(BytewiseComparator());
  ImmutableOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(new MemTable(
      cmp, ioptions, MutableCFOptions(options), &wb, kMaxSequenceNumber,
      0 /* column_family_id */, nullptr /* flush_initiator */));

  ASSERT_OK(mem->Add(100, kTypeValue, "key", "v1", nullptr /* kv_prot_info */));
  ASSERT_EQ(
      mem->Add(100, kTypeValue, "key", "v2", nullptr /* kv_prot_info */).code(),
      Status::kTryAgain);
  ASSERT_OK(mem->Add(101, kTypeValue, "key", "v3", nullptr /* kv_prot_info */));

  MemTablePostProcessInfo post_process_info;
  ASSERT_OK(mem->Add(102, kTypeValue, "key", "v4", nullptr /* kv_prot_info */,
                     true /* allow_concurrent */, &post_process_info));
  ASSERT_EQ(mem->Add(102, kTypeValue, "key", "v5", nullptr /* kv_prot_info */,
                     true /* allow_concurrent */, &post_process_info)
                .code(),
            Status::kTryAgain);
  mem->BatchPostProcess(post_process_info);
  ASSERT_EQ(mem->NumEntries(), 3u);
}

TEST_F(DBMemTableTest, SortedHashFactoryFromString) {
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;
  std::unique_ptr<MemTableRepFactory> factory;
  std::string bucket_count;

  ASSERT_OK(MemTableRepFactory::CreateFromString(config_options,
                                                 "sorted_hash:32", &factory));
  ASSERT_STREQ(factory->Name(), "SortedHashRepFactory");
  ASSERT_TRUE(factory->IsInsertConcurrentlySupported());
  ASSERT_FALSE(factory->IsTailingIteratorSupported());
  ASSERT_OK(factory->GetOption(config_options, "bucket_count", &bucket_count));
  ASSERT_EQ(bucket_count, "32");

  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options, "id=SortedHashRepFactory;bucket_count=64", &factory));
  ASSERT_OK(factory->GetOption(config_options, "bucket_count", &bucket_count));
  ASSERT_EQ(bucket_count, "64");
}

TEST_F(DBMemTableTest, SortedHashOptionsFileRoundTrip) {
  DestroyAndReopen(SortedHashOptions(CurrentOptions()));

  ConfigOptions config_options;
  config_options.env = env_;
  DBOptions db_options;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ASSERT_OK(LoadLatestOptions(config_options, dbname_, &db_options, &cf_descs));
  ASSERT_EQ(cf_descs.size(), 1u);
  const std::shared_ptr<MemTableRepFactory>& factory =
      cf_descs[0].options.memtable_factory;
  ASSERT_STREQ(factory->Name(), "SortedHashRepFactory");
  std::string bucket_count;
  ASSERT_OK(factory->GetOption(config_options, "bucket_count", &bucket_count));
  ASSERT_EQ(bucket_count, std::to_string(kSortedHashBucketCount));
}

TEST_F(DBMemTableTest, VectorConcurrentInsert) {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory = std::make_shared<VectorRepFactory>();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1"}, options);

  // Multi-threaded writes
  {
    WriteOptions write_options;
    std::vector<port::Thread> threads;
    threads.reserve(10);
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&, i]() {
        int start = i * 100;
        int end = start + 100;
        WriteBatch batch;
        for (int j = start; j < end; ++j) {
          ASSERT_OK(
              batch.Put(handles_[0], Key(j), "value" + std::to_string(j)));
        }
        ASSERT_OK(db_->Write(write_options, &batch));
      });
    }
    for (auto& t : threads) {
      t.join();
    }

    std::unique_ptr<Iterator> iter(
        db_->NewIterator(ReadOptions(), handles_[0]));
    iter->SeekToFirst();
    for (int i = 0; i < 1000; ++i) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), Key(i));
      ASSERT_EQ(iter->value().ToString(), "value" + std::to_string(i));
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Multi-threaded writes, multi CF
  {
    WriteOptions write_options;
    std::vector<port::Thread> threads;
    threads.reserve(10);
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&, i]() {
        int start = i * 100;
        int end = start + 100;
        WriteBatch batch;
        for (int j = start; j < end; ++j) {
          ASSERT_OK(batch.Put(handles_[0], Key(j), "CF0" + std::to_string(j)));
          ASSERT_OK(batch.Put(handles_[1], Key(j), "CF1" + std::to_string(j)));
        }
        ASSERT_OK(db_->Write(write_options, &batch));
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    std::unique_ptr<Iterator> iter0(
        db_->NewIterator(ReadOptions(), handles_[0]));
    std::unique_ptr<Iterator> iter1(
        db_->NewIterator(ReadOptions(), handles_[1]));
    iter0->SeekToFirst();
    iter1->SeekToFirst();
    for (int i = 0; i < 1000; ++i) {
      ASSERT_TRUE(iter0->Valid());
      ASSERT_EQ(iter0->key().ToString(), Key(i));
      ASSERT_EQ(iter0->value().ToString(), "CF0" + std::to_string(i));
      iter0->Next();

      ASSERT_TRUE(iter1->Valid());
      ASSERT_EQ(iter1->key().ToString(), Key(i));
      ASSERT_EQ(iter1->value().ToString(), "CF1" + std::to_string(i));
      iter1->Next();
    }
    ASSERT_FALSE(iter0->Valid());
    ASSERT_OK(iter0->status());
    ASSERT_FALSE(iter1->Valid());
    ASSERT_OK(iter1->status());
  }

  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
}
// A minimal key-owning MemTableRep, backed by a std::map keyed by internal
// key, to exercise the OwnsKeys() interface (AllocateWithKey / GetEntries /
// EntryView / Iterator::entry / IsKeyPinned) and the MemTable plumbing for
// key-owning reps without depending on a production rep.
namespace {
// Test-only counter: how many times the owning read path (GetEntries) ran.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<int> g_owning_get_entries_calls{0};
// Test-only counter: how many entries the owning write path (AllocateWithKey)
// allocated. Stays 0 when the memtable falls back to a skip list.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<int> g_owning_allocate_with_key_calls{0};

class OwningMapRep : public MemTableRep {
 public:
  // The test always uses the default bytewise comparator, so order internal
  // keys with it directly rather than through the passed-in comparator.
  OwningMapRep(const MemTableRep::KeyComparator& /*cmp*/, Allocator* allocator)
      : MemTableRep(allocator),
        ikc_(BytewiseComparator()),
        entries_(KeyCmp{&ikc_}) {}

  bool OwnsKeys() const override { return true; }

  KeyHandle AllocateWithKey(const Slice& user_key, uint64_t packed_seq_type,
                            size_t value_part_len, char** value_part) override {
    g_owning_allocate_with_key_calls.fetch_add(1);
    pending_key_.assign(user_key.data(), user_key.size());
    PutFixed64(&pending_key_, packed_seq_type);
    char* buf = allocator_->Allocate(value_part_len);
    pending_value_part_ = buf;
    pending_value_part_len_ = value_part_len;
    *value_part = buf;
    return static_cast<KeyHandle>(buf);
  }

  void Insert(KeyHandle handle) override { InsertKey(handle); }

  bool InsertKey(KeyHandle handle) override {
    assert(static_cast<char*>(handle) == pending_value_part_);
    (void)handle;
    entries_[pending_key_] =
        std::string(pending_value_part_, pending_value_part_len_);
    return true;
  }

  bool Contains(const char* key) const override {
    return entries_.count(GetLengthPrefixedSlice(key).ToString()) > 0;
  }

  void Get(const LookupKey& /*k*/, void* /*callback_args*/,
           bool (* /*callback_func*/)(void* arg, const char* entry)) override {
    assert(false);  // key-owning reps are read through GetEntries().
  }

  void GetEntries(const LookupKey& k, void* callback_args,
                  bool (*callback_func)(void* arg,
                                        const EntryView& entry)) override {
    g_owning_get_entries_calls.fetch_add(1);
    const Slice user_key = k.user_key();
    for (auto it = entries_.lower_bound(k.internal_key().ToString());
         it != entries_.end() && ExtractUserKey(it->first) == user_key; ++it) {
      if (!callback_func(callback_args,
                         EntryView{Slice(it->first), it->second.data()})) {
        break;
      }
    }
  }

  size_t ApproximateMemoryUsage() override { return 0; }

  MemTableRep::Iterator* GetIterator(Arena* arena) override {
    if (arena != nullptr) {
      void* mem = arena->AllocateAligned(sizeof(Iter));
      return new (mem) Iter(&entries_);
    }
    return new Iter(&entries_);
  }

 private:
  struct KeyCmp {
    const InternalKeyComparator* ikc;
    bool operator()(const std::string& a, const std::string& b) const {
      return ikc->Compare(a, b) < 0;
    }
  };
  using Map = std::map<std::string, std::string, KeyCmp>;

  class Iter : public MemTableRep::Iterator {
   public:
    explicit Iter(const Map* m) : map_(m), it_(m->end()) {}
    bool Valid() const override { return it_ != map_->end(); }
    const char* key() const override { return it_->first.data(); }
    EntryView entry() const override {
      return EntryView{Slice(it_->first), it_->second.data()};
    }
    bool IsKeyPinned() const override { return false; }
    void Next() override { ++it_; }
    void Prev() override {
      if (it_ == map_->begin()) {
        it_ = map_->end();
      } else {
        --it_;
      }
    }
    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const Slice target = memtable_key != nullptr
                               ? GetLengthPrefixedSlice(memtable_key)
                               : internal_key;
      it_ = map_->lower_bound(target.ToString());
    }
    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const Slice target = memtable_key != nullptr
                               ? GetLengthPrefixedSlice(memtable_key)
                               : internal_key;
      it_ = map_->lower_bound(target.ToString());
      if (!Valid()) {
        SeekToLast();
      } else if (it_->first != target.ToString()) {
        Prev();
      }
    }
    void SeekToFirst() override { it_ = map_->begin(); }
    void SeekToLast() override {
      it_ = map_->empty() ? map_->end() : std::prev(map_->end());
    }

   private:
    const Map* map_;
    Map::const_iterator it_;
  };

  InternalKeyComparator ikc_;
  Map entries_;
  std::string pending_key_;
  char* pending_value_part_ = nullptr;
  size_t pending_value_part_len_ = 0;
};

class OwningMapRepFactory : public MemTableRepFactory {
 public:
  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* allocator,
                                 const SliceTransform* /*transform*/,
                                 Logger* /*logger*/) override {
    return new OwningMapRep(cmp, allocator);
  }
  const char* Name() const override { return "OwningMapRepFactory"; }
  bool IsInsertConcurrentlySupported() const override { return false; }
  bool OwnsKeys() const override { return true; }
};
}  // namespace

TEST_F(DBMemTableTest, OwnsKeysRoundTrip) {
  Options options = CurrentOptions();
  options.memtable_factory = std::make_shared<OwningMapRepFactory>();
  options.allow_concurrent_memtable_write = false;
  // Exercise VerifyEntryChecksum(EntryView) on the owning read path.
  options.memtable_protection_bytes_per_key = 8;
  g_owning_get_entries_calls.store(0);
  g_owning_allocate_with_key_calls.store(0);
  DestroyAndReopen(options);

  ASSERT_OK(Put("b", "vb1"));
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put("b", "vb2"));  // overwrite; newest version wins

  ASSERT_EQ("va", Get("a"));
  ASSERT_EQ("vb2", Get("b"));
  ASSERT_EQ("vc", Get("c"));
  // The key-owning write and read paths were actually used.
  ASSERT_GT(g_owning_allocate_with_key_calls.load(), 0);
  ASSERT_GT(g_owning_get_entries_calls.load(), 0);

  // Forward iteration returns keys in order through the EntryView path.
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  std::string order;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    order += it->key().ToString() + "=" + it->value().ToString() + ";";
  }
  ASSERT_OK(it->status());
  ASSERT_EQ("a=va;b=vb2;c=vc;", order);

  // Flush reads the memtable through unpinned keys (IsKeyPinned()==false).
  ASSERT_OK(Flush());
  ASSERT_EQ("vb2", Get("b"));
}

TEST_F(DBMemTableTest, OwnsKeysFallsBackForIncompatibleOptions) {
  // Each option needs the encoded-entry layout, so a key-owning rep must fall
  // back to a skip list at creation (see CreateMemTableRepWithFallback).
  struct Case {
    const char* name;
    void (*set)(Options&);
  };
  static const Case kCases[] = {
      {"inplace_update_support",
       [](Options& o) { o.inplace_update_support = true; }},
      {"paranoid_memory_checks",
       [](Options& o) {
         o.memtable_protection_bytes_per_key = 8;
         o.paranoid_memory_checks = true;
       }},
      {"memtable_verify_per_key_checksum_on_seek",
       [](Options& o) {
         o.memtable_protection_bytes_per_key = 8;
         o.memtable_verify_per_key_checksum_on_seek = true;
       }},
      {"experimental_mempurge_threshold",
       [](Options& o) { o.experimental_mempurge_threshold = 1.0; }},
  };
  for (const auto& c : kCases) {
    SCOPED_TRACE(c.name);
    Options options = CurrentOptions();
    options.memtable_factory = std::make_shared<OwningMapRepFactory>();
    options.allow_concurrent_memtable_write = false;
    c.set(options);
    g_owning_get_entries_calls.store(0);
    g_owning_allocate_with_key_calls.store(0);
    DestroyAndReopen(options);

    ASSERT_OK(Put("k", "v1"));
    ASSERT_OK(Put("k", "v2"));
    ASSERT_EQ("v2", Get("k"));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get("k"));
    // Fallback: the owning rep's write and read paths are never exercised (a
    // positive signal, not just the absence of an ASan report).
    ASSERT_EQ(0, g_owning_allocate_with_key_calls.load());
    ASSERT_EQ(0, g_owning_get_entries_calls.load());
  }
}

TEST_F(DBMemTableTest, OwnsKeysFallsBackForUserTimestamp) {
  Options options = CurrentOptions();
  options.memtable_factory = std::make_shared<OwningMapRepFactory>();
  options.allow_concurrent_memtable_write = false;
  // A user comparator with timestamps forces the skip-list fallback. Without
  // it, the key-owning write path makes MaybeUpdateNewestUDT store a pointer
  // into the caller's WriteBatch key buffer, which is freed when the write
  // returns; the next write and GetNewestUDT() then read freed memory
  // (heap-use-after-free under ASan).
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  g_owning_get_entries_calls.store(0);
  g_owning_allocate_with_key_calls.store(0);
  DestroyAndReopen(options);

  auto encode_ts = [](uint64_t v) {
    std::string ts;
    PutFixed64(&ts, v);
    return ts;
  };
  // Two separate writes, so the first WriteBatch buffer is freed before the
  // second write reads the newest-UDT pointer.
  ASSERT_OK(db_->Put(WriteOptions(), "k1", encode_ts(10), "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "k2", encode_ts(11), "v2"));
  ASSERT_OK(Flush());

  // The rep owns keys, but the timestamp comparator forced a skip-list
  // fallback, so the owning write and read paths are never used.
  ASSERT_EQ(0, g_owning_allocate_with_key_calls.load());
  ASSERT_EQ(0, g_owning_get_entries_calls.load());
}

TEST_F(DBMemTableTest, OwnsKeysSkipsMemPurgeAfterSetOptions) {
  Options options = CurrentOptions();
  options.memtable_factory = std::make_shared<OwningMapRepFactory>();
  options.allow_concurrent_memtable_write = false;
  // MemPurge is off at creation, so the active memtable is created key-owning.
  options.experimental_mempurge_threshold = 0.0;
  g_owning_allocate_with_key_calls.store(0);
  DestroyAndReopen(options);

  ASSERT_OK(Put("k1", "v1"));
  // The active memtable really does own its keys.
  ASSERT_GT(g_owning_allocate_with_key_calls.load(), 0);

  // MemPurge reads entries through the encoded layout, so it must never run on
  // a key-owning memtable (see FlushJob::Run's any_memtable_owns_keys guard).
  std::atomic<int> mempurge_runs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void*) { mempurge_runs.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeUnsuccessful",
      [&](void*) { mempurge_runs.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  // Raise the threshold on the live owning memtable, then flush it. A threshold
  // greater than the number of flushed memtables makes MemPurgeDecider return
  // true unconditionally (see FlushJob::MemPurgeDecider), so without the guard
  // FlushJob would attempt MemPurge and read the owning rep's entries through
  // the encoded layout (heap-use-after-free under ASan).
  ASSERT_OK(db_->SetOptions({{"experimental_mempurge_threshold", "100.0"}}));
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(0, mempurge_runs.load());  // the guard skipped MemPurge
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));
}

// The MemTableRep key comparator exposes the wrapped user comparator so a rep
// can check it was given a comparator it supports.
TEST_F(DBMemTableTest, KeyComparatorExposesUserComparator) {
  InternalKeyComparator ikc(BytewiseComparator());
  MemTable::KeyComparator cmp(ikc);
  const MemTableRep::KeyComparator& base = cmp;
  ASSERT_EQ(base.user_comparator(), BytewiseComparator());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
