//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/memtable_list.h"

#include <algorithm>
#include <string>
#include <vector>

#include "db/merge_context.h"
#include "db/version_set.h"
#include "db/write_controller.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
std::string ValueWithWriteTime(std::string value, uint64_t write_time) {
  std::string result;
  result = value;
  PutFixed64(&result, write_time);
  return result;
}
}  // namespace

class MemTableListTest : public testing::Test {
 public:
  std::string dbname;
  std::unique_ptr<DB> db;
  Options options;
  std::vector<ColumnFamilyHandle*> handles;
  std::atomic<uint64_t> file_number;

  MemTableListTest() : file_number(1) {
    dbname = test::PerThreadDBPath("memtable_list_test");
    options.create_if_missing = true;
    EXPECT_OK(DestroyDB(dbname, options));
  }

  // Create a test db if not yet created
  void CreateDB() {
    if (db == nullptr) {
      options.create_if_missing = true;
      EXPECT_OK(DestroyDB(dbname, options));
      // Open DB only with default column family
      ColumnFamilyOptions cf_options;
      std::vector<ColumnFamilyDescriptor> cf_descs;
      if (udt_enabled_) {
        cf_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
      }
      cf_descs.emplace_back(kDefaultColumnFamilyName, cf_options);
      Status s = DB::Open(options, dbname, cf_descs, &handles, &db);
      EXPECT_OK(s);

      ColumnFamilyOptions cf_opt1;
      ColumnFamilyOptions cf_opt2;
      cf_opt1.cf_paths.emplace_back(dbname + "_one_1",
                                    std::numeric_limits<uint64_t>::max());
      cf_opt2.cf_paths.emplace_back(dbname + "_two_1",
                                    std::numeric_limits<uint64_t>::max());
      int sz = static_cast<int>(handles.size());
      handles.resize(sz + 2);
      s = db->CreateColumnFamily(cf_opt1, "one", &handles[1]);
      EXPECT_OK(s);
      s = db->CreateColumnFamily(cf_opt2, "two", &handles[2]);
      EXPECT_OK(s);

      cf_descs.emplace_back("one", cf_options);
      cf_descs.emplace_back("two", cf_options);
    }
  }

  ~MemTableListTest() override {
    if (db) {
      std::vector<ColumnFamilyDescriptor> cf_descs(handles.size());
      for (int i = 0; i != static_cast<int>(handles.size()); ++i) {
        EXPECT_OK(handles[i]->GetDescriptor(&cf_descs[i]));
      }
      for (auto h : handles) {
        if (h) {
          EXPECT_OK(db->DestroyColumnFamilyHandle(h));
        }
      }
      handles.clear();
      db.reset();
      EXPECT_OK(DestroyDB(dbname, options, cf_descs));
    }
  }

  // Calls MemTableList::TryInstallMemtableFlushResults() and sets up all
  // structures needed to call this function.
  Status Mock_InstallMemtableFlushResults(
      MemTableList* list, const autovector<ReadOnlyMemTable*>& m,
      autovector<ReadOnlyMemTable*>* to_delete) {
    // Create a mock Logger
    test::NullLogger logger;
    LogBuffer log_buffer(DEBUG_LEVEL, &logger);

    CreateDB();
    // Create a mock VersionSet
    DBOptions db_options;
    ImmutableDBOptions immutable_db_options(db_options);
    EnvOptions env_options;
    std::shared_ptr<Cache> table_cache(NewLRUCache(50000, 16));
    WriteBufferManager write_buffer_manager(db_options.db_write_buffer_size);
    WriteController write_controller(10000000u);

    VersionSet versions(dbname, &immutable_db_options,
                        MutableDBOptions{db_options}, env_options,
                        table_cache.get(), &write_buffer_manager,
                        &write_controller, /*block_cache_tracer=*/nullptr,
                        /*io_tracer=*/nullptr, /*db_id=*/"",
                        /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
                        /*error_handler=*/nullptr, /*read_only=*/false);
    std::vector<ColumnFamilyDescriptor> cf_descs;
    cf_descs.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
    cf_descs.emplace_back("one", ColumnFamilyOptions());
    cf_descs.emplace_back("two", ColumnFamilyOptions());

    EXPECT_OK(versions.Recover(cf_descs, false));

    // Create mock default ColumnFamilyData
    auto column_family_set = versions.GetColumnFamilySet();
    LogsWithPrepTracker dummy_prep_tracker;
    auto cfd = column_family_set->GetDefault();
    EXPECT_TRUE(nullptr != cfd);
    uint64_t file_num = file_number.fetch_add(1);
    IOStatus io_s;
    // Create dummy mutex.
    InstrumentedMutex mutex;
    InstrumentedMutexLock l(&mutex);
    std::list<std::unique_ptr<FlushJobInfo>> flush_jobs_info;
    Status s = list->TryInstallMemtableFlushResults(
        cfd, m, &dummy_prep_tracker, &versions, &mutex, file_num, to_delete,
        nullptr, &log_buffer, &flush_jobs_info);
    EXPECT_OK(io_s);
    return s;
  }

  // Wrappers giving TEST_F bodies access to state/methods that require
  // friend access this fixture class has (via `friend class
  // MemTableListTest` in ReadOnlyMemTable and MemTableList) but a TEST_F
  // body -- a separate, gtest-generated class deriving from this one --
  // does not have directly. See MempurgeLeaderDoesNotDropSiblingEdit below.
  static void SetMockFlushCompleted(ReadOnlyMemTable* m, uint64_t file_num,
                                    bool should_write_edit) {
    m->flush_completed_ = true;
    m->file_number_ = file_num;
    m->edit_should_be_written_ = should_write_edit;
  }

  static void CallCollectEditsToWrite(
      const std::list<ReadOnlyMemTable*>& memlist, const std::string& cfd_name,
      LogBuffer* log_buffer, autovector<VersionEdit*>* edit_list,
      autovector<ReadOnlyMemTable*>* memtables_to_flush, bool* any_write_edits,
      std::list<std::unique_ptr<FlushJobInfo>>* committed_flush_jobs_info) {
    MemTableList::CollectEditsToWrite(memlist, cfd_name, log_buffer, edit_list,
                                      memtables_to_flush, any_write_edits,
                                      committed_flush_jobs_info);
  }

  // Calls MemTableList::InstallMemtableFlushResults() and sets up all
  // structures needed to call this function.
  Status Mock_InstallMemtableAtomicFlushResults(
      autovector<MemTableList*>& lists, const autovector<uint32_t>& cf_ids,
      const autovector<const autovector<ReadOnlyMemTable*>*>& mems_list,
      autovector<ReadOnlyMemTable*>* to_delete) {
    // Create a mock Logger
    test::NullLogger logger;
    LogBuffer log_buffer(DEBUG_LEVEL, &logger);

    CreateDB();
    // Create a mock VersionSet
    DBOptions db_options;

    ImmutableDBOptions immutable_db_options(db_options);
    EnvOptions env_options;
    std::shared_ptr<Cache> table_cache(NewLRUCache(50000, 16));
    WriteBufferManager write_buffer_manager(db_options.db_write_buffer_size);
    WriteController write_controller(10000000u);

    VersionSet versions(dbname, &immutable_db_options,
                        MutableDBOptions{db_options}, env_options,
                        table_cache.get(), &write_buffer_manager,
                        &write_controller, /*block_cache_tracer=*/nullptr,
                        /*io_tracer=*/nullptr, /*db_id=*/"",
                        /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
                        /*error_handler=*/nullptr, /*read_only=*/false);
    std::vector<ColumnFamilyDescriptor> cf_descs;
    cf_descs.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
    cf_descs.emplace_back("one", ColumnFamilyOptions());
    cf_descs.emplace_back("two", ColumnFamilyOptions());
    EXPECT_OK(versions.Recover(cf_descs, false));

    // Create mock default ColumnFamilyData

    auto column_family_set = versions.GetColumnFamilySet();

    LogsWithPrepTracker dummy_prep_tracker;
    autovector<ColumnFamilyData*> cfds;
    for (int i = 0; i != static_cast<int>(cf_ids.size()); ++i) {
      cfds.emplace_back(column_family_set->GetColumnFamily(cf_ids[i]));
      EXPECT_NE(nullptr, cfds[i]);
    }
    std::vector<FileMetaData> file_metas;
    file_metas.reserve(cf_ids.size());
    for (size_t i = 0; i != cf_ids.size(); ++i) {
      FileMetaData meta;
      uint64_t file_num = file_number.fetch_add(1);
      meta.fd = FileDescriptor(file_num, 0, 0);
      file_metas.emplace_back(meta);
    }
    autovector<FileMetaData*> file_meta_ptrs;
    for (auto& meta : file_metas) {
      file_meta_ptrs.push_back(&meta);
    }
    std::vector<std::list<std::unique_ptr<FlushJobInfo>>>
        committed_flush_jobs_info_storage(cf_ids.size());
    autovector<std::list<std::unique_ptr<FlushJobInfo>>*>
        committed_flush_jobs_info;
    for (int i = 0; i < static_cast<int>(cf_ids.size()); ++i) {
      committed_flush_jobs_info.push_back(
          &committed_flush_jobs_info_storage[i]);
    }

    InstrumentedMutex mutex;
    InstrumentedMutexLock l(&mutex);
    return InstallMemtableAtomicFlushResults(
        &lists, cfds, mems_list, &versions, nullptr /* prep_tracker */, &mutex,
        file_meta_ptrs, committed_flush_jobs_info, to_delete, nullptr,
        &log_buffer);
  }

 protected:
  bool udt_enabled_ = false;
};

TEST_F(MemTableListTest, Empty) {
  // Create an empty MemTableList and validate basic functions.
  MemTableList list(1, 0);

  ASSERT_EQ(0, list.NumNotFlushed());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_FALSE(list.IsFlushPending());

  autovector<ReadOnlyMemTable*> mems;
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &mems);
  ASSERT_EQ(0, mems.size());

  autovector<ReadOnlyMemTable*> to_delete;
  list.current()->Unref(&to_delete);
  ASSERT_EQ(0, to_delete.size());
}

TEST_F(MemTableListTest, GetTest) {
  // Create MemTableList
  int min_write_buffer_number_to_merge = 2;
  int64_t max_write_buffer_size_to_maintain = 0;
  MemTableList list(min_write_buffer_number_to_merge,
                    max_write_buffer_size_to_maintain);

  SequenceNumber seq = 1;
  std::string value;
  Status s;
  MergeContext merge_context;
  InternalKeyComparator ikey_cmp(options.comparator);
  SequenceNumber max_covering_tombstone_seq = 0;
  autovector<ReadOnlyMemTable*> to_delete;

  LookupKey lkey("key1", seq);
  bool found = list.current()->Get(lkey, &value, /*columns=*/nullptr,
                                   /*timestamp=*/nullptr, &s, &merge_context,
                                   &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  ImmutableOptions ioptions(options);

  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                               kMaxSequenceNumber, 0 /* column_family_id */,
                               nullptr /* flush_initiator */);
  mem->Ref();

  // Write some keys to this memtable.
  ASSERT_OK(
      mem->Add(++seq, kTypeDeletion, "key1", "", nullptr /* kv_prot_info */));
  ASSERT_OK(mem->Add(++seq, kTypeValue, "key2", "value2",
                     nullptr /* kv_prot_info */));
  ASSERT_OK(mem->Add(++seq, kTypeValue, "key1", "value1",
                     nullptr /* kv_prot_info */));
  ASSERT_OK(mem->Add(++seq, kTypeValue, "key2", "value2.2",
                     nullptr /* kv_prot_info */));
  ASSERT_OK(mem->Add(++seq, kTypeValuePreferredSeqno, "key3",
                     ValueWithWriteTime("value3.1", 20),
                     nullptr /* kv_prot_info */));

  // Fetch the newly written keys
  merge_context.Clear();
  s = Status::OK();
  found = mem->Get(LookupKey("key1", seq), &value, /*columns*/ nullptr,
                   /*timestamp*/ nullptr, &s, &merge_context,
                   &max_covering_tombstone_seq, ReadOptions(),
                   false /* immutable_memtable */);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value1");

  merge_context.Clear();
  s = Status::OK();
  found = mem->Get(LookupKey("key1", 2), &value, /*columns*/ nullptr,
                   /*timestamp*/ nullptr, &s, &merge_context,
                   &max_covering_tombstone_seq, ReadOptions(),
                   false /* immutable_memtable */);
  // MemTable found out that this key is *not* found (at this sequence#)
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  s = Status::OK();
  found = mem->Get(LookupKey("key2", seq), &value, /*columns*/ nullptr,
                   /*timestamp*/ nullptr, &s, &merge_context,
                   &max_covering_tombstone_seq, ReadOptions(),
                   false /* immutable_memtable */);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.2");

  merge_context.Clear();
  s = Status::OK();
  found = mem->Get(LookupKey("key3", seq), &value, /*columns*/ nullptr,
                   /*timestamp*/ nullptr, &s, &merge_context,
                   &max_covering_tombstone_seq, ReadOptions(),
                   false /* immutable_memtable */);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value3.1");

  ASSERT_EQ(5, mem->NumEntries());
  ASSERT_EQ(1, mem->NumDeletion());

  // Add memtable to list
  // This is to make assert(memtable->IsFragmentedRangeTombstonesConstructed())
  // in MemTableListVersion::GetFromList work.
  mem->ConstructFragmentedRangeTombstones();
  mem->SetID(1);
  list.Add(mem, &to_delete);

  SequenceNumber saved_seq = seq;

  // Create another memtable and write some keys to it
  WriteBufferManager wb2(options.db_write_buffer_size);
  MemTable* mem2 = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb2,
                                kMaxSequenceNumber, 0 /* column_family_id */,
                                nullptr /* flush_initiator */);
  mem2->SetID(2);
  mem2->Ref();

  ASSERT_OK(
      mem2->Add(++seq, kTypeDeletion, "key1", "", nullptr /* kv_prot_info */));
  ASSERT_OK(mem2->Add(++seq, kTypeValue, "key2", "value2.3",
                      nullptr /* kv_prot_info */));
  ASSERT_OK(mem2->Add(++seq, kTypeMerge, "key3", "value3.2",
                      nullptr /* kv_prot_info */));

  // Add second memtable to list
  // This is to make assert(memtable->IsFragmentedRangeTombstonesConstructed())
  // in MemTableListVersion::GetFromList work.
  mem2->ConstructFragmentedRangeTombstones();
  list.Add(mem2, &to_delete);

  // Fetch keys via MemTableList
  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key1", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  s = Status::OK();
  found = list.current()->Get(LookupKey("key1", saved_seq), &value,
                              /*columns=*/nullptr, /*timestamp=*/nullptr, &s,
                              &merge_context, &max_covering_tombstone_seq,
                              ReadOptions());
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ("value1", value);

  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key2", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.3");

  merge_context.Clear();
  s = Status::OK();
  found = list.current()->Get(LookupKey("key2", 1), &value, /*columns=*/nullptr,
                              /*timestamp=*/nullptr, &s, &merge_context,
                              &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key3", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value3.1,value3.2");

  ASSERT_EQ(2, list.NumNotFlushed());

  list.current()->Unref(&to_delete);
  for (ReadOnlyMemTable* m : to_delete) {
    delete m;
  }
}

TEST_F(MemTableListTest, GetFromHistoryTest) {
  // Create MemTableList
  int min_write_buffer_number_to_merge = 2;
  int64_t max_write_buffer_size_to_maintain = 2 * Arena::kInlineSize;
  MemTableList list(min_write_buffer_number_to_merge,
                    max_write_buffer_size_to_maintain);

  SequenceNumber seq = 1;
  std::string value;
  Status s;
  MergeContext merge_context;
  InternalKeyComparator ikey_cmp(options.comparator);
  SequenceNumber max_covering_tombstone_seq = 0;
  autovector<ReadOnlyMemTable*> to_delete;

  LookupKey lkey("key1", seq);
  bool found = list.current()->Get(lkey, &value, /*columns=*/nullptr,
                                   /*timestamp=*/nullptr, &s, &merge_context,
                                   &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableOptions ioptions(options);

  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                               kMaxSequenceNumber, 0 /* column_family_id */,
                               nullptr /* flush_initiator */);
  mem->Ref();

  // Write some keys to this memtable.
  ASSERT_OK(
      mem->Add(++seq, kTypeDeletion, "key1", "", nullptr /* kv_prot_info */));
  ASSERT_OK(mem->Add(++seq, kTypeValue, "key2", "value2",
                     nullptr /* kv_prot_info */));
  ASSERT_OK(mem->Add(++seq, kTypeValue, "key2", "value2.2",
                     nullptr /* kv_prot_info */));

  // Fetch the newly written keys
  merge_context.Clear();
  s = Status::OK();
  found = mem->Get(LookupKey("key1", seq), &value, /*columns*/ nullptr,
                   /*timestamp*/ nullptr, &s, &merge_context,
                   &max_covering_tombstone_seq, ReadOptions(),
                   false /* immutable_memtable */);
  // MemTable found out that this key is *not* found (at this sequence#)
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  s = Status::OK();
  found = mem->Get(LookupKey("key2", seq), &value, /*columns*/ nullptr,
                   /*timestamp*/ nullptr, &s, &merge_context,
                   &max_covering_tombstone_seq, ReadOptions(),
                   false /* immutable_memtable */);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.2");

  // Add memtable to list
  // This is to make assert(memtable->IsFragmentedRangeTombstonesConstructed())
  // in MemTableListVersion::GetFromList work.
  mem->ConstructFragmentedRangeTombstones();
  list.Add(mem, &to_delete);
  ASSERT_EQ(0, to_delete.size());

  // Fetch keys via MemTableList
  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key1", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key2", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ("value2.2", value);

  // Flush this memtable from the list.
  // (It will then be a part of the memtable history).
  autovector<ReadOnlyMemTable*> to_flush;
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush);
  ASSERT_EQ(1, to_flush.size());

  s = Mock_InstallMemtableFlushResults(&list, to_flush, &to_delete);
  ASSERT_OK(s);
  ASSERT_EQ(0, list.NumNotFlushed());
  ASSERT_EQ(1, list.NumFlushed());
  ASSERT_EQ(0, to_delete.size());

  // Verify keys are no longer in MemTableList
  merge_context.Clear();
  found =
      list.current()->Get(LookupKey("key1", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  merge_context.Clear();
  found =
      list.current()->Get(LookupKey("key2", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  // Verify keys are present in history
  merge_context.Clear();
  s = Status::OK();
  found = list.current()->GetFromHistory(
      LookupKey("key1", seq), &value, /*columns=*/nullptr,
      /*timestamp=*/nullptr, &s, &merge_context, &max_covering_tombstone_seq,
      ReadOptions());
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  s = Status::OK();
  found = list.current()->GetFromHistory(
      LookupKey("key2", seq), &value, /*columns=*/nullptr,
      /*timestamp=*/nullptr, &s, &merge_context, &max_covering_tombstone_seq,
      ReadOptions());
  ASSERT_TRUE(found);
  ASSERT_EQ("value2.2", value);

  // Create another memtable and write some keys to it
  WriteBufferManager wb2(options.db_write_buffer_size);
  MemTable* mem2 = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb2,
                                kMaxSequenceNumber, 0 /* column_family_id */,
                                nullptr /* flush_initiator */);
  mem2->Ref();

  ASSERT_OK(
      mem2->Add(++seq, kTypeDeletion, "key1", "", nullptr /* kv_prot_info */));
  ASSERT_OK(mem2->Add(++seq, kTypeValue, "key3", "value3",
                      nullptr /* kv_prot_info */));

  // Add second memtable to list
  // This is to make assert(memtable->IsFragmentedRangeTombstonesConstructed())
  // in MemTableListVersion::GetFromList work.
  mem2->ConstructFragmentedRangeTombstones();
  list.Add(mem2, &to_delete);
  ASSERT_EQ(0, to_delete.size());

  to_flush.clear();
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush);
  ASSERT_EQ(1, to_flush.size());

  // Flush second memtable
  s = Mock_InstallMemtableFlushResults(&list, to_flush, &to_delete);
  ASSERT_OK(s);
  ASSERT_EQ(0, list.NumNotFlushed());
  ASSERT_EQ(2, list.NumFlushed());
  ASSERT_EQ(0, to_delete.size());

  // Add a third memtable to push the first memtable out of the history
  WriteBufferManager wb3(options.db_write_buffer_size);
  MemTable* mem3 = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb3,
                                kMaxSequenceNumber, 0 /* column_family_id */,
                                nullptr /* flush_initiator */);
  mem3->Ref();
  // This is to make assert(memtable->IsFragmentedRangeTombstonesConstructed())
  // in MemTableListVersion::GetFromList work.
  mem3->ConstructFragmentedRangeTombstones();
  list.Add(mem3, &to_delete);
  ASSERT_EQ(1, list.NumNotFlushed());
  ASSERT_EQ(1, list.NumFlushed());
  ASSERT_EQ(1, to_delete.size());

  // Verify keys are no longer in MemTableList
  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key1", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key2", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key3", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  // Verify that the second memtable's keys are in the history
  merge_context.Clear();
  s = Status::OK();
  found = list.current()->GetFromHistory(
      LookupKey("key1", seq), &value, /*columns=*/nullptr,
      /*timestamp=*/nullptr, &s, &merge_context, &max_covering_tombstone_seq,
      ReadOptions());
  ASSERT_TRUE(found && s.IsNotFound());

  merge_context.Clear();
  s = Status::OK();
  found = list.current()->GetFromHistory(
      LookupKey("key3", seq), &value, /*columns=*/nullptr,
      /*timestamp=*/nullptr, &s, &merge_context, &max_covering_tombstone_seq,
      ReadOptions());
  ASSERT_TRUE(found);
  ASSERT_EQ("value3", value);

  // Verify that key2 from the first memtable is no longer in the history
  merge_context.Clear();
  s = Status::OK();
  found =
      list.current()->Get(LookupKey("key2", seq), &value, /*columns=*/nullptr,
                          /*timestamp=*/nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, ReadOptions());
  ASSERT_FALSE(found);

  // Cleanup
  list.current()->Unref(&to_delete);
  ASSERT_EQ(3, to_delete.size());
  for (ReadOnlyMemTable* m : to_delete) {
    delete m;
  }
}

TEST_F(MemTableListTest, FlushPendingTest) {
  const int num_tables = 6;
  SequenceNumber seq = 1;
  Status s;

  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableOptions ioptions(options);
  InternalKeyComparator cmp(BytewiseComparator());
  WriteBufferManager wb(options.db_write_buffer_size);
  autovector<ReadOnlyMemTable*> to_delete;

  // Create MemTableList
  int min_write_buffer_number_to_merge = 3;
  int64_t max_write_buffer_size_to_maintain =
      7 * static_cast<int>(options.write_buffer_size);
  MemTableList list(min_write_buffer_number_to_merge,
                    max_write_buffer_size_to_maintain);

  // Create some MemTables
  uint64_t memtable_id = 0;
  std::vector<MemTable*> tables;
  MutableCFOptions mutable_cf_options(options);
  for (int i = 0; i < num_tables; i++) {
    MemTable* mem =
        new MemTable(cmp, ioptions, mutable_cf_options, &wb, kMaxSequenceNumber,
                     0 /* column_family_id */, nullptr /* flush_initiator */);
    mem->SetID(memtable_id++);
    mem->Ref();

    std::string value;
    MergeContext merge_context;

    ASSERT_OK(mem->Add(++seq, kTypeValue, "key1", std::to_string(i),
                       nullptr /* kv_prot_info */));
    ASSERT_OK(mem->Add(++seq, kTypeValue, "keyN" + std::to_string(i), "valueN",
                       nullptr /* kv_prot_info */));
    ASSERT_OK(mem->Add(++seq, kTypeValue, "keyX" + std::to_string(i), "value",
                       nullptr /* kv_prot_info */));
    ASSERT_OK(mem->Add(++seq, kTypeValue, "keyM" + std::to_string(i), "valueM",
                       nullptr /* kv_prot_info */));
    ASSERT_OK(mem->Add(++seq, kTypeDeletion, "keyX" + std::to_string(i), "",
                       nullptr /* kv_prot_info */));

    tables.push_back(mem);
  }

  // Nothing to flush
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));
  autovector<ReadOnlyMemTable*> to_flush;
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush);
  ASSERT_EQ(0, to_flush.size());

  // Request a flush even though there is nothing to flush
  list.FlushRequested();
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Attempt to 'flush' to clear request for flush
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush);
  ASSERT_EQ(0, to_flush.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Request a flush again
  list.FlushRequested();
  // No flush pending since the list is empty.
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Add 2 tables
  list.Add(tables[0], &to_delete);
  list.Add(tables[1], &to_delete);
  ASSERT_EQ(2, list.NumNotFlushed());
  ASSERT_EQ(0, to_delete.size());

  // Even though we have less than the minimum to flush, a flush is
  // pending since we had previously requested a flush and never called
  // PickMemtablesToFlush() to clear the flush.
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush);
  ASSERT_EQ(2, to_flush.size());
  ASSERT_EQ(2, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Revert flush
  list.RollbackMemtableFlush(to_flush, false);
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  to_flush.clear();

  // Add another table
  list.Add(tables[2], &to_delete);
  // We now have the minimum to flush regardles of whether FlushRequested()
  // was called.
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_EQ(0, to_delete.size());

  // Pick tables to flush
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush);
  ASSERT_EQ(3, to_flush.size());
  ASSERT_EQ(3, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  autovector<ReadOnlyMemTable*> to_flush2;
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush2);
  ASSERT_EQ(0, to_flush2.size());
  ASSERT_EQ(3, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Add another table
  list.Add(tables[3], &to_delete);
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_EQ(0, to_delete.size());

  // Request a flush again
  list.FlushRequested();
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush2);
  ASSERT_EQ(1, to_flush2.size());
  ASSERT_EQ(4, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Rollback first pick of tables
  list.RollbackMemtableFlush(to_flush, false);
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  to_flush.clear();

  // Add another tables
  list.Add(tables[4], &to_delete);
  ASSERT_EQ(5, list.NumNotFlushed());
  // We now have the minimum to flush regardles of whether FlushRequested()
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_EQ(0, to_delete.size());

  // Pick tables to flush
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush);
  // Picks three oldest memtables. The fourth oldest is picked in `to_flush2` so
  // must be excluded. The newest (fifth oldest) is non-consecutive with the
  // three oldest due to omitting the fourth oldest so must not be picked.
  ASSERT_EQ(3, to_flush.size());
  ASSERT_EQ(5, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  autovector<ReadOnlyMemTable*> to_flush3;
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush3);
  // Picks newest (fifth oldest)
  ASSERT_EQ(1, to_flush3.size());
  ASSERT_EQ(5, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Nothing left to flush
  autovector<ReadOnlyMemTable*> to_flush4;
  list.PickMemtablesToFlush(
      std::numeric_limits<uint64_t>::max() /* memtable_id */, &to_flush4);
  ASSERT_EQ(0, to_flush4.size());
  ASSERT_EQ(5, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Flush the 3 memtables that were picked in to_flush
  s = Mock_InstallMemtableFlushResults(&list, to_flush, &to_delete);
  ASSERT_OK(s);

  // Note:  now to_flush contains tables[0,1,2].  to_flush2 contains
  // tables[3]. to_flush3 contains tables[4].
  // Current implementation will only commit memtables in the order they were
  // created. So TryInstallMemtableFlushResults will install the first 3 tables
  // in to_flush and stop when it encounters a table not yet flushed.
  ASSERT_EQ(2, list.NumNotFlushed());
  int num_in_history =
      std::min(3, static_cast<int>(max_write_buffer_size_to_maintain) /
                      static_cast<int>(options.write_buffer_size));
  ASSERT_EQ(num_in_history, list.NumFlushed());
  ASSERT_EQ(5 - list.NumNotFlushed() - num_in_history, to_delete.size());

  // Request a flush again. Should be nothing to flush
  list.FlushRequested();
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Flush the 1 memtable (tables[4]) that was picked in to_flush3
  s = MemTableListTest::Mock_InstallMemtableFlushResults(&list, to_flush3,
                                                         &to_delete);
  ASSERT_OK(s);

  // This will install 0 tables since tables[4] flushed while tables[3] has not
  // yet flushed.
  ASSERT_EQ(2, list.NumNotFlushed());
  ASSERT_EQ(0, to_delete.size());

  // Flush the 1 memtable (tables[3]) that was picked in to_flush2
  s = MemTableListTest::Mock_InstallMemtableFlushResults(&list, to_flush2,
                                                         &to_delete);
  ASSERT_OK(s);

  // This will actually install 2 tables.  The 1 we told it to flush, and also
  // tables[4] which has been waiting for tables[3] to commit.
  ASSERT_EQ(0, list.NumNotFlushed());
  num_in_history =
      std::min(5, static_cast<int>(max_write_buffer_size_to_maintain) /
                      static_cast<int>(options.write_buffer_size));
  ASSERT_EQ(num_in_history, list.NumFlushed());
  ASSERT_EQ(5 - list.NumNotFlushed() - num_in_history, to_delete.size());

  for (const auto& m : to_delete) {
    // Refcount should be 0 after calling TryInstallMemtableFlushResults.
    // Verify this, by Ref'ing then UnRef'ing:
    m->Ref();
    ASSERT_EQ(m, m->Unref());
    delete m;
  }
  to_delete.clear();

  // Add another table
  list.Add(tables[5], &to_delete);
  ASSERT_EQ(1, list.NumNotFlushed());
  ASSERT_EQ(5, list.GetLatestMemTableID(false /* for_atomic_flush */));
  memtable_id = 4;
  // Pick tables to flush. The tables to pick must have ID smaller than or
  // equal to 4. Therefore, no table will be selected in this case.
  autovector<ReadOnlyMemTable*> to_flush5;
  list.FlushRequested();
  ASSERT_TRUE(list.HasFlushRequested());
  list.PickMemtablesToFlush(memtable_id, &to_flush5);
  ASSERT_TRUE(to_flush5.empty());
  ASSERT_EQ(1, list.NumNotFlushed());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.HasFlushRequested());

  // Pick tables to flush. The tables to pick must have ID smaller than or
  // equal to 5. Therefore, only tables[5] will be selected.
  memtable_id = 5;
  list.FlushRequested();
  list.PickMemtablesToFlush(memtable_id, &to_flush5);
  ASSERT_EQ(1, static_cast<int>(to_flush5.size()));
  ASSERT_EQ(1, list.NumNotFlushed());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_FALSE(list.IsFlushPending());
  to_delete.clear();

  list.current()->Unref(&to_delete);
  int to_delete_size =
      std::min(num_tables, static_cast<int>(max_write_buffer_size_to_maintain) /
                               static_cast<int>(options.write_buffer_size));
  ASSERT_EQ(to_delete_size, to_delete.size());

  for (const auto& m : to_delete) {
    // Refcount should be 0 after calling TryInstallMemtableFlushResults.
    // Verify this, by Ref'ing then UnRef'ing:
    m->Ref();
    ASSERT_EQ(m, m->Unref());
    delete m;
  }
  to_delete.clear();
}

// Regression test for https://github.com/facebook/rocksdb/issues/9022.
//
// `MemTableList::TryInstallMemtableFlushResults()` can batch multiple
// concurrently-completed flush jobs' VersionEdits into a single MANIFEST
// write. Each flush job passes its OWN write_edits bool (stashed per
// memtable as `edit_should_be_written_`), which is false only for a
// successful mempurge (that job has no new SST file and its own log
// number is stale, so its own edit must not be written). Before the fix,
// that bool was applied to the WHOLE batch, so a mempurge-successful job
// sharing a batch with a sibling (normal) flush job would cause the
// sibling's real edit to be silently dropped along with the mempurge job's
// (correctly-skipped) edit.
//
// `CollectEditsToWrite()` is the extracted, pure in-memory bookkeeping that
// decides which edits go into the batch's edit_list -- it never touches
// VersionSet/LogAndApply or any real file, so it can be tested directly
// and fast, without a real MANIFEST or SST files.
TEST_F(MemTableListTest, MempurgeLeaderDoesNotDropSiblingEdit) {
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableOptions ioptions(options);
  InternalKeyComparator cmp(BytewiseComparator());
  WriteBufferManager wb(options.db_write_buffer_size);
  MutableCFOptions mutable_cf_options(options);

  // The older memtable: as if its flush "succeeded" via mempurge, so it
  // needs no edit written (edit_should_be_written_ == false).
  MemTable* mempurge_mem = new MemTable(cmp, ioptions, mutable_cf_options, &wb,
                                        kMaxSequenceNumber,
                                        0 /* column_family_id */);
  mempurge_mem->Ref();
  SetMockFlushCompleted(mempurge_mem, /*file_num=*/100,
                        /*should_write_edit=*/false);

  // The newer memtable: as if a normal (non-mempurge) flush completed, so
  // its edit needs to be written (edit_should_be_written_ == true).
  MemTable* normal_mem = new MemTable(cmp, ioptions, mutable_cf_options, &wb,
                                      kMaxSequenceNumber,
                                      0 /* column_family_id */);
  normal_mem->Ref();
  SetMockFlushCompleted(normal_mem, /*file_num=*/200,
                        /*should_write_edit=*/true);
  normal_mem->GetEdits()->SetLogNumber(42);

  // New memtables are inserted at the front of the list, so the list is
  // {newest, ..., oldest}: `normal_mem` (newer) then `mempurge_mem`
  // (older), matching how `MemTableList::Add()` orders `memlist_`.
  std::list<ReadOnlyMemTable*> memlist{normal_mem, mempurge_mem};

  test::NullLogger logger;
  LogBuffer log_buffer(DEBUG_LEVEL, &logger);
  autovector<VersionEdit*> edit_list;
  autovector<ReadOnlyMemTable*> memtables_to_flush;
  bool any_write_edits = false;
  std::list<std::unique_ptr<FlushJobInfo>> committed_flush_jobs_info;

  CallCollectEditsToWrite(memlist, "default", &log_buffer, &edit_list,
                          &memtables_to_flush, &any_write_edits,
                          &committed_flush_jobs_info);

  // Both memtables (mempurge and normal) are gathered for retirement from
  // the immutable list...
  ASSERT_EQ(2u, memtables_to_flush.size());

  // ...but, crucially, only the sibling normal flush's own edit is
  // collected for the MANIFEST write: the mempurge job's (correctly
  // skipped) edit must not suppress it. Before the fix for #9022, this
  // whole batch would have been treated as write_edits=false (since the
  // OLDEST/leading memtable, mempurge_mem, opted out), and `edit_list`
  // would have ended up empty here.
  ASSERT_TRUE(any_write_edits);
  ASSERT_EQ(1u, edit_list.size());
  ASSERT_EQ(normal_mem->GetEdits(), edit_list[0]);
  ASSERT_EQ(42u, edit_list[0]->GetLogNumber());

  ASSERT_EQ(mempurge_mem, mempurge_mem->Unref());
  delete mempurge_mem;
  ASSERT_EQ(normal_mem, normal_mem->Unref());
  delete normal_mem;
}

TEST_F(MemTableListTest, FlushRequestPersistsAfterPartialPick) {
  SequenceNumber seq = 1;

  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableOptions ioptions(options);
  InternalKeyComparator cmp(BytewiseComparator());
  WriteBufferManager wb(options.db_write_buffer_size);
  MutableCFOptions mutable_cf_options(options);
  autovector<ReadOnlyMemTable*> to_delete;

  MemTableList list(10 /* min_write_buffer_number_to_merge */,
                    0 /* max_write_buffer_size_to_maintain */);

  for (uint64_t memtable_id = 0; memtable_id < 2; ++memtable_id) {
    MemTable* mem =
        new MemTable(cmp, ioptions, mutable_cf_options, &wb, kMaxSequenceNumber,
                     0 /* column_family_id */, nullptr /* flush_initiator */);
    mem->SetID(memtable_id);
    mem->Ref();
    ASSERT_OK(mem->Add(++seq, kTypeValue, "key" + std::to_string(memtable_id),
                       "value", nullptr /* kv_prot_info */));
    list.Add(mem, &to_delete);
  }

  list.FlushRequested();
  ASSERT_TRUE(list.IsFlushPending());

  autovector<ReadOnlyMemTable*> to_flush;
  list.PickMemtablesToFlush(0 /* max_memtable_id */, &to_flush);
  ASSERT_EQ(1, to_flush.size());
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.HasFlushRequested());

  autovector<ReadOnlyMemTable*> to_flush2;
  list.PickMemtablesToFlush(1 /* max_memtable_id */, &to_flush2);
  ASSERT_EQ(1, to_flush2.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.HasFlushRequested());

  list.current()->Unref(&to_delete);
  ASSERT_EQ(2, to_delete.size());
  for (ReadOnlyMemTable* m : to_delete) {
    delete m;
  }
}

TEST_F(MemTableListTest, RemoveOldMemTablesTest) {
  // Covers the predicate that lets a caller skip RemoveOldMemTables(), and
  // checks that it agrees with what the removal itself does. Both are used only
  // by the secondary instance, which collects memtables by the log number that
  // follows their contents rather than by flushing them.
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableOptions ioptions(options);
  InternalKeyComparator cmp(BytewiseComparator());
  WriteBufferManager wb(options.db_write_buffer_size);
  MutableCFOptions mutable_cf_options(options);
  autovector<ReadOnlyMemTable*> to_delete;

  MemTableList list(/*min_write_buffer_number_to_merge=*/1,
                    /*max_write_buffer_size_to_maintain=*/0);

  // An empty list has nothing to remove, whatever the log number.
  ASSERT_FALSE(list.HasOldMemTablesToRemove(0));
  ASSERT_FALSE(
      list.HasOldMemTablesToRemove(std::numeric_limits<uint64_t>::max()));

  // Two sealed memtables, the older one followed by WAL 10 and the newer one by
  // WAL 20.
  SequenceNumber seq = 1;
  for (uint64_t next_log_number : {10, 20}) {
    MemTable* mem =
        new MemTable(cmp, ioptions, mutable_cf_options, &wb, kMaxSequenceNumber,
                     0 /* column_family_id */, nullptr /* flush_initiator */);
    mem->Ref();
    ASSERT_OK(mem->Add(++seq, kTypeValue, "key", "value",
                       nullptr /* kv_prot_info */));
    mem->SetNextLogNumber(next_log_number);
    mem->ConstructFragmentedRangeTombstones();
    list.Add(mem, &to_delete);
  }
  ASSERT_EQ(2, list.NumNotFlushed());

  // Neither memtable is superseded by a WAL older than the one following it.
  ASSERT_FALSE(list.HasOldMemTablesToRemove(9));
  list.RemoveOldMemTables(9, &to_delete);
  ASSERT_EQ(2, list.NumNotFlushed());

  // The older memtable is collected at exactly its own next log number.
  ASSERT_TRUE(list.HasOldMemTablesToRemove(10));
  list.RemoveOldMemTables(10, &to_delete);
  ASSERT_EQ(1, list.NumNotFlushed());

  ASSERT_FALSE(list.HasOldMemTablesToRemove(19));
  ASSERT_TRUE(list.HasOldMemTablesToRemove(20));
  list.RemoveOldMemTables(20, &to_delete);
  ASSERT_EQ(0, list.NumNotFlushed());
  ASSERT_FALSE(
      list.HasOldMemTablesToRemove(std::numeric_limits<uint64_t>::max()));

  list.current()->Unref(&to_delete);
  for (ReadOnlyMemTable* m : to_delete) {
    delete m;
  }
}

TEST_F(MemTableListTest, EmptyAtomicFlushTest) {
  autovector<MemTableList*> lists;
  autovector<uint32_t> cf_ids;
  autovector<const autovector<ReadOnlyMemTable*>*> to_flush;
  autovector<ReadOnlyMemTable*> to_delete;
  Status s = Mock_InstallMemtableAtomicFlushResults(lists, cf_ids, to_flush,
                                                    &to_delete);
  ASSERT_OK(s);
  ASSERT_TRUE(to_delete.empty());
}

TEST_F(MemTableListTest, AtomicFlushTest) {
  const int num_cfs = 3;
  const int num_tables_per_cf = 2;
  SequenceNumber seq = 1;

  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableOptions ioptions(options);
  InternalKeyComparator cmp(BytewiseComparator());
  WriteBufferManager wb(options.db_write_buffer_size);

  // Create MemTableLists
  int min_write_buffer_number_to_merge = 3;
  int64_t max_write_buffer_size_to_maintain =
      7 * static_cast<int64_t>(options.write_buffer_size);
  autovector<MemTableList*> lists;
  for (int i = 0; i != num_cfs; ++i) {
    lists.emplace_back(new MemTableList(min_write_buffer_number_to_merge,
                                        max_write_buffer_size_to_maintain));
  }

  autovector<uint32_t> cf_ids;
  std::vector<std::vector<MemTable*>> tables(num_cfs);
  autovector<const MutableCFOptions*> mutable_cf_options_list;
  uint32_t cf_id = 0;
  for (auto& elem : tables) {
    mutable_cf_options_list.emplace_back(new MutableCFOptions(options));
    uint64_t memtable_id = 0;
    for (int i = 0; i != num_tables_per_cf; ++i) {
      MemTable* mem = new MemTable(
          cmp, ioptions, *(mutable_cf_options_list.back()), &wb,
          kMaxSequenceNumber, cf_id, nullptr /* flush_initiator */);
      mem->SetID(memtable_id++);
      mem->Ref();

      std::string value;

      ASSERT_OK(mem->Add(++seq, kTypeValue, "key1", std::to_string(i),
                         nullptr /* kv_prot_info */));
      ASSERT_OK(mem->Add(++seq, kTypeValue, "keyN" + std::to_string(i),
                         "valueN", nullptr /* kv_prot_info */));
      ASSERT_OK(mem->Add(++seq, kTypeValue, "keyX" + std::to_string(i), "value",
                         nullptr /* kv_prot_info */));
      ASSERT_OK(mem->Add(++seq, kTypeValue, "keyM" + std::to_string(i),
                         "valueM", nullptr /* kv_prot_info */));
      ASSERT_OK(mem->Add(++seq, kTypeDeletion, "keyX" + std::to_string(i), "",
                         nullptr /* kv_prot_info */));

      elem.push_back(mem);
    }
    cf_ids.push_back(cf_id++);
  }

  std::vector<autovector<ReadOnlyMemTable*>> flush_candidates(num_cfs);

  // Nothing to flush
  for (auto i = 0; i != num_cfs; ++i) {
    auto* list = lists[i];
    ASSERT_FALSE(list->IsFlushPending());
    ASSERT_FALSE(list->imm_flush_needed.load(std::memory_order_acquire));
    list->PickMemtablesToFlush(
        std::numeric_limits<uint64_t>::max() /* memtable_id */,
        &flush_candidates[i]);
    ASSERT_EQ(0, flush_candidates[i].size());
  }
  // Request flush even though there is nothing to flush
  for (auto i = 0; i != num_cfs; ++i) {
    auto* list = lists[i];
    list->FlushRequested();
    ASSERT_FALSE(list->IsFlushPending());
    ASSERT_FALSE(list->imm_flush_needed.load(std::memory_order_acquire));
  }
  autovector<ReadOnlyMemTable*> to_delete;
  // Add tables to the immutable memtalbe lists associated with column families
  for (auto i = 0; i != num_cfs; ++i) {
    for (auto j = 0; j != num_tables_per_cf; ++j) {
      lists[i]->Add(tables[i][j], &to_delete);
    }
    ASSERT_EQ(num_tables_per_cf, lists[i]->NumNotFlushed());
    ASSERT_TRUE(lists[i]->IsFlushPending());
    ASSERT_TRUE(lists[i]->imm_flush_needed.load(std::memory_order_acquire));
  }
  std::vector<uint64_t> flush_memtable_ids = {1, 1, 0};
  //          +----+
  // list[0]: |0  1|
  // list[1]: |0  1|
  //          | +--+
  // list[2]: |0| 1
  //          +-+
  // Pick memtables to flush
  for (auto i = 0; i != num_cfs; ++i) {
    flush_candidates[i].clear();
    lists[i]->PickMemtablesToFlush(flush_memtable_ids[i], &flush_candidates[i]);
    ASSERT_EQ(flush_memtable_ids[i] - 0 + 1,
              static_cast<uint64_t>(flush_candidates[i].size()));
  }
  autovector<MemTableList*> tmp_lists;
  autovector<uint32_t> tmp_cf_ids;
  autovector<const autovector<ReadOnlyMemTable*>*> to_flush;
  for (auto i = 0; i != num_cfs; ++i) {
    if (!flush_candidates[i].empty()) {
      to_flush.push_back(&flush_candidates[i]);
      tmp_lists.push_back(lists[i]);
      tmp_cf_ids.push_back(i);
    }
  }
  Status s = Mock_InstallMemtableAtomicFlushResults(tmp_lists, tmp_cf_ids,
                                                    to_flush, &to_delete);
  ASSERT_OK(s);

  for (auto i = 0; i != num_cfs; ++i) {
    for (auto j = 0; j != num_tables_per_cf; ++j) {
      if (static_cast<uint64_t>(j) <= flush_memtable_ids[i]) {
        ASSERT_LT(0, tables[i][j]->GetFileNumber());
      }
    }
    ASSERT_EQ(
        static_cast<size_t>(num_tables_per_cf) - flush_candidates[i].size(),
        lists[i]->NumNotFlushed());
  }

  to_delete.clear();
  for (auto list : lists) {
    list->current()->Unref(&to_delete);
    delete list;
  }
  for (auto& mutable_cf_options : mutable_cf_options_list) {
    if (mutable_cf_options != nullptr) {
      delete mutable_cf_options;
      mutable_cf_options = nullptr;
    }
  }
  // All memtables in tables array must have been flushed, thus ready to be
  // deleted.
  ASSERT_EQ(to_delete.size(), tables.size() * tables.front().size());
  for (const auto& m : to_delete) {
    // Refcount should be 0 after calling InstallMemtableFlushResults.
    // Verify this by Ref'ing and then Unref'ing.
    m->Ref();
    ASSERT_EQ(m, m->Unref());
    delete m;
  }
}

class MemTableListWithTimestampTest : public MemTableListTest {
 public:
  MemTableListWithTimestampTest() : MemTableListTest() {}

  void SetUp() override { udt_enabled_ = true; }
};

TEST_F(MemTableListWithTimestampTest, GetTableNewestUDT) {
  const int num_tables = 3;
  const int num_entries = 5;
  SequenceNumber seq = 1;

  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  options.persist_user_defined_timestamps = false;
  ImmutableOptions ioptions(options);
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  InternalKeyComparator cmp(ucmp);
  WriteBufferManager wb(options.db_write_buffer_size);

  // Create MemTableList
  int min_write_buffer_number_to_merge = 1;
  int64_t max_write_buffer_size_to_maintain =
      4 * static_cast<int>(options.write_buffer_size);
  MemTableList list(min_write_buffer_number_to_merge,
                    max_write_buffer_size_to_maintain);

  // Create some MemTables
  uint64_t memtable_id = 0;
  std::vector<MemTable*> tables;
  MutableCFOptions mutable_cf_options(options);
  uint64_t current_ts = 0;
  autovector<ReadOnlyMemTable*> to_delete;
  std::vector<std::string> newest_udts;

  std::string key;
  std::string write_ts;
  for (int i = 0; i < num_tables; i++) {
    MemTable* mem =
        new MemTable(cmp, ioptions, mutable_cf_options, &wb, kMaxSequenceNumber,
                     0 /* column_family_id */, nullptr /* flush_initiator */);
    mem->SetID(memtable_id++);
    mem->Ref();

    std::string value;
    MergeContext merge_context;

    for (int j = 0; j < num_entries; j++) {
      key = "key1";
      write_ts.clear();
      PutFixed64(&write_ts, current_ts);
      key.append(write_ts);
      ASSERT_OK(mem->Add(++seq, kTypeValue, key, std::to_string(i),
                         nullptr /* kv_prot_info */));
      current_ts++;
    }

    tables.push_back(mem);
    list.Add(tables.back(), &to_delete);
    newest_udts.push_back(write_ts);
  }

  ASSERT_EQ(num_tables, list.NumNotFlushed());
  ASSERT_TRUE(list.IsFlushPending());
  std::vector<Slice> tables_newest_udts = list.GetTablesNewestUDT(num_tables);
  ASSERT_EQ(newest_udts.size(), tables_newest_udts.size());
  for (size_t i = 0; i < tables_newest_udts.size(); i++) {
    const Slice& table_newest_udt = tables_newest_udts[i];
    const Slice expected_newest_udt = newest_udts[i];
    ASSERT_EQ(expected_newest_udt, table_newest_udt);
  }

  list.current()->Unref(&to_delete);
  for (ReadOnlyMemTable* m : to_delete) {
    delete m;
  }
  to_delete.clear();
}

TEST_F(MemTableListWithTimestampTest, ConcurrentGetTableNewestUDT) {
  const int num_threads = 4;
  const int num_entries_per_thread = 100;

  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  options.persist_user_defined_timestamps = false;
  ImmutableOptions ioptions(options);
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  InternalKeyComparator cmp(ucmp);
  WriteBufferManager wb(options.db_write_buffer_size);
  MutableCFOptions mutable_cf_options(options);

  MemTable* mem =
      new MemTable(cmp, ioptions, mutable_cf_options, &wb, kMaxSequenceNumber,
                   0 /* column_family_id */, nullptr /* flush_initiator */);
  mem->Ref();

  std::atomic<uint64_t> next_seq{1};
  uint64_t max_ts = num_threads * num_entries_per_thread - 1;

  std::vector<port::Thread> threads;
  threads.reserve(num_threads);

  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([&, t]() {
      MemTablePostProcessInfo post_process_info;
      for (int j = 0; j < num_entries_per_thread; j++) {
        uint64_t ts = t * num_entries_per_thread + j;
        std::string key = "key" + std::to_string(ts);
        std::string write_ts;
        PutFixed64(&write_ts, ts);
        key.append(write_ts);

        SequenceNumber seq = next_seq.fetch_add(1);
        ASSERT_OK(mem->Add(seq, kTypeValue, key, "val",
                           nullptr /* kv_prot_info */,
                           true /* allow_concurrent */, &post_process_info));
      }
      mem->BatchPostProcess(post_process_info);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_EQ(num_threads * num_entries_per_thread, mem->NumEntries());

  Slice newest_udt = mem->GetNewestUDT();
  ASSERT_EQ(sizeof(uint64_t), newest_udt.size());
  uint64_t got_ts = DecodeFixed64(newest_udt.data());
  ASSERT_EQ(max_ts, got_ts);

  ReadOnlyMemTable* m = mem->Unref();
  delete m;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
