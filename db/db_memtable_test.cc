//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <limits>
#include <memory>
#include <string>

#include "db/db_test_util.h"
#include "db/memtable.h"
#include "db/range_del_aggregator.h"
#include "port/stack_trace.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"

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
  void* last_hint_in_;
  void* last_hint_out_;
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
  MockMemTableRep* mock_rep_;
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

TEST_F(DBMemTableTest, RejectsOversizedEncodedEntry) {
  Options options;
  InternalKeyComparator cmp(options.comparator);
  ImmutableOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem =
      std::make_unique<MemTable>(cmp, ioptions, MutableCFOptions(options), &wb,
                                 kMaxSequenceNumber, 0 /* column_family_id */);

  constexpr size_t kMaxUint32 = std::numeric_limits<uint32_t>::max();
  const char dummy = 'x';

  // Slice does not own or access the described storage. These artificial
  // lengths exercise validation without allocating or copying gigabytes.
  Status s = mem->Add(1, kTypeValue, Slice(&dummy, 1),
                      Slice(&dummy, kMaxUint32), nullptr /* kv_prot_info */);
  ASSERT_TRUE(s.IsInvalidArgument());

  s = mem->Add(2, kTypeValue, Slice(&dummy, kMaxUint32 - kNumInternalBytes + 1),
               Slice(), nullptr /* kv_prot_info */);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_EQ(0U, mem->NumEntries());
  ASSERT_EQ(0U, mem->GetDataSize());
}

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
  options.memtable_insert_with_hint_prefix_extractor.reset(
      new TestPrefixExtractor());  // which uses _ to extract the prefix
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
  options.memtable_factory.reset(new MockMemTableRepFactory());
  options.memtable_insert_with_hint_prefix_extractor.reset(
      new TestPrefixExtractor());
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
  options.memtable_factory.reset(new MockMemTableRepFactory());
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

TEST_F(DBMemTableTest, VectorConcurrentInsert) {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory.reset(new VectorRepFactory());
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1"}, options);

  // Multi-threaded writes
  {
    WriteOptions write_options;
    std::vector<port::Thread> threads;
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
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
