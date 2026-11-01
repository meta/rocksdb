//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_dispatcher.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/persistent_cache.h"
#include "table/block_based/flush_block_policy_impl.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "util/random.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

#ifdef OS_LINUX
namespace {
struct QueryAbortState {
  int check_count = 0;
  int abort_after_check_count = -1;
};

QueryAbortState& GetQueryAbortState() {
  static thread_local QueryAbortState state;
  return state;
}

class ScopedQueryAbort {
 public:
  explicit ScopedQueryAbort(int abort_after_check_count) {
    QueryAbortState& state = GetQueryAbortState();
    state.check_count = 0;
    state.abort_after_check_count = abort_after_check_count;
  }

  ~ScopedQueryAbort() { GetQueryAbortState().abort_after_check_count = -1; }

  int check_count() const { return GetQueryAbortState().check_count; }
};
}  // namespace

extern "C" bool RocksDbThreadYieldAndCheckAbort() {
  QueryAbortState& state = GetQueryAbortState();
  if (state.abort_after_check_count < 0) {
    return false;
  }
  ++state.check_count;
  return state.abort_after_check_count > 0 &&
         state.check_count >= state.abort_after_check_count;
}
#endif  // OS_LINUX

namespace ROCKSDB_NAMESPACE {

// A dumb ReadCallback which saying every key is committed.
class DummyReadCallback : public ReadCallback {
 public:
  DummyReadCallback() : ReadCallback(kMaxSequenceNumber) {}
  bool IsVisibleFullCheck(SequenceNumber /*seq*/) override { return true; }
  void SetSnapshot(SequenceNumber seq) { max_visible_seq_ = seq; }
};

class DBIteratorBaseTest : public DBTestBase {
 public:
  DBIteratorBaseTest()
      : DBTestBase("db_iterator_test", /*env_do_fsync=*/true) {}
};

#ifdef OS_LINUX
TEST_F(DBIteratorBaseTest, PrevChecksQueryAbort) {
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));

  for (int abort_after_check_count = 1; abort_after_check_count <= 2;
       ++abort_after_check_count) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek("c");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    ScopedQueryAbort abort(abort_after_check_count);
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsAborted());
  }
}

TEST_F(DBIteratorBaseTest, ForwardMergeChecksQueryAbort) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  ASSERT_OK(Put("b", "base"));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(db_->Merge(WriteOptions(), "b", "operand"));
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  {
    ScopedQueryAbort abort(/*abort_after_check_count=*/0);
    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "b");
    ASSERT_EQ(iter->value(), "base,operand,operand,operand,operand");
    ASSERT_EQ(abort.check_count(), 4);
  }

  iter.reset(db_->NewIterator(ReadOptions()));
  {
    ScopedQueryAbort abort(/*abort_after_check_count=*/3);
    iter->Seek("b");
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsAborted());
    ASSERT_EQ(abort.check_count(), 3);
  }
}

TEST_F(DBIteratorBaseTest, PrevReseekMergeChecksQueryAbort) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 2;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "base"));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(db_->Merge(WriteOptions(), "b", "operand"));
  }
  ASSERT_OK(Put("c", "vc"));

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("c");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());

  {
    ScopedQueryAbort abort(/*abort_after_check_count=*/0);
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "b");
    // One check reaches "b", two trigger the reseek, four scan merge
    // operands, and three finish positioning before "b".
    ASSERT_EQ(abort.check_count(), 10);
  }

  iter.reset(db_->NewIterator(ReadOptions()));
  iter->Seek("c");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  {
    ScopedQueryAbort abort(/*abort_after_check_count=*/4);
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsAborted());
    ASSERT_EQ(abort.check_count(), 4);
  }
}
#endif  // OS_LINUX

TEST_F(DBIteratorBaseTest, APICallsWithPerfContext) {
  // Set up the DB
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 1; i <= 3; i++) {
    ASSERT_OK(Put(std::to_string(i), std::to_string(i)));
  }

  // Setup iterator and PerfContext
  Iterator* iter = db_->NewIterator(ReadOptions());
  std::string key_str = std::to_string(2);
  Slice key(key_str);
  SetPerfLevel(kEnableCount);
  get_perf_context()->Reset();

  // Initial PerfContext counters
  ASSERT_EQ(0, get_perf_context()->iter_seek_count);
  ASSERT_EQ(0, get_perf_context()->iter_next_count);
  ASSERT_EQ(0, get_perf_context()->iter_prev_count);

  // Test Seek-related API calls PerfContext counter
  iter->Seek(key);
  iter->SeekToFirst();
  iter->SeekToLast();
  iter->SeekForPrev(key);
  ASSERT_EQ(4, get_perf_context()->iter_seek_count);
  ASSERT_EQ(0, get_perf_context()->iter_next_count);
  ASSERT_EQ(0, get_perf_context()->iter_prev_count);

  // Test Next() calls PerfContext counter
  iter->Next();
  ASSERT_EQ(4, get_perf_context()->iter_seek_count);
  ASSERT_EQ(1, get_perf_context()->iter_next_count);
  ASSERT_EQ(0, get_perf_context()->iter_prev_count);

  // Test Prev() calls PerfContext counter
  iter->Prev();
  ASSERT_EQ(4, get_perf_context()->iter_seek_count);
  ASSERT_EQ(1, get_perf_context()->iter_next_count);
  ASSERT_EQ(1, get_perf_context()->iter_prev_count);

  delete iter;
}

TEST_F(DBIteratorBaseTest, PrepareWithMultiScanPrunesNonIntersectingFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("z", "vz"));
  ASSERT_OK(Flush());
  ASSERT_EQ(2, NumTableFilesAtLevel(0));

  int table_iterators_created = 0;
  int files_added = 0;
  int block_based_iterators = 0;
  int level_iterators = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::NewIterator::BeforeFindTable",
      [&](void* /*arg*/) { ++table_iterators_created; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:AddedFile",
      [&](void* /*arg*/) { ++files_added; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:IteratorType", [&](void* arg) {
        auto* iterator_type = static_cast<std::pair<bool, bool>*>(arg);
        if (iterator_type->first) {
          ++block_based_iterators;
        }
        if (iterator_type->second) {
          ++level_iterators;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  MultiScanArgs scan_opts(BytewiseComparator());
  scan_opts.insert(Slice("a"), Slice("b"));
  Slice upper_bound("b");
  ReadOptions read_options;
  read_options.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_options, db_->DefaultColumnFamily()));

  ASSERT_EQ(0, table_iterators_created);
  ASSERT_EQ(0, files_added);
  ASSERT_EQ(0, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  iter->Prepare(scan_opts);
  ASSERT_EQ(1, table_iterators_created);
  ASSERT_EQ(1, files_added);
  ASSERT_EQ(1, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  std::vector<std::string> keys;
  for (iter->Seek("a"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_EQ(keys, std::vector<std::string>({"a"}));
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBIteratorBaseTest, PrepareWithMultiScanPrunesNonIntersectingLevels) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("z", "vz"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Put("m", "vm"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  int table_iterators_created = 0;
  int files_added = 0;
  int block_based_iterators = 0;
  int level_iterators = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::NewIterator::BeforeFindTable",
      [&](void* /*arg*/) { ++table_iterators_created; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:AddedFile",
      [&](void* /*arg*/) { ++files_added; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:IteratorType", [&](void* arg) {
        auto* iterator_type = static_cast<std::pair<bool, bool>*>(arg);
        if (iterator_type->first) {
          ++block_based_iterators;
        }
        if (iterator_type->second) {
          ++level_iterators;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  MultiScanArgs scan_opts(BytewiseComparator());
  scan_opts.insert(Slice("a"), Slice("n"));
  Slice upper_bound("n");
  ReadOptions read_options;
  read_options.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_options, db_->DefaultColumnFamily()));

  ASSERT_EQ(0, table_iterators_created);
  ASSERT_EQ(0, files_added);
  ASSERT_EQ(0, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  iter->Prepare(scan_opts);
  ASSERT_EQ(0, table_iterators_created);
  ASSERT_EQ(2, files_added);
  ASSERT_EQ(0, block_based_iterators);
  ASSERT_EQ(1, level_iterators);
  std::vector<std::string> keys;
  for (iter->Seek("a"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_EQ(keys, std::vector<std::string>({"a", "m"}));
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBIteratorBaseTest, PrepareWithMultiScanAllowsSingleUnboundedRange) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  // L0 table iterators receive the unbounded Prepare() directly.
  ASSERT_OK(Put("d", "vd"));
  ASSERT_OK(Flush());

  MultiScanArgs scan_opts(BytewiseComparator());
  scan_opts.insert(Slice("b"));
  ReadOptions read_options;
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_options, db_->DefaultColumnFamily()));

  iter->Prepare(scan_opts);
  std::vector<std::string> keys;
  for (iter->Seek("b"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_EQ(keys, std::vector<std::string>({"b", "c", "d"}));
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBIteratorBaseTest, PrepareWithMultiScanRejectsRepeatedPrepare) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  MultiScanArgs scan_opts(BytewiseComparator());
  scan_opts.insert(Slice("a"), Slice("b"));
  Slice upper_bound("b");
  ReadOptions read_options;
  read_options.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_options, db_->DefaultColumnFamily()));

  iter->Prepare(scan_opts);
  ASSERT_OK(iter->status());
  iter->Prepare(scan_opts);
  ASSERT_NOK(iter->status());
}

TEST_F(DBIteratorBaseTest, PrepareWithMultiScanDedupsMultipleRangesInSameFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("m", "vm"));
  ASSERT_OK(Put("z", "vz"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  int table_iterators_created = 0;
  int files_added = 0;
  int block_based_iterators = 0;
  int level_iterators = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::NewIterator::BeforeFindTable",
      [&](void* /*arg*/) { ++table_iterators_created; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:AddedFile",
      [&](void* /*arg*/) { ++files_added; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:IteratorType", [&](void* arg) {
        auto* iterator_type = static_cast<std::pair<bool, bool>*>(arg);
        if (iterator_type->first) {
          ++block_based_iterators;
        }
        if (iterator_type->second) {
          ++level_iterators;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  MultiScanArgs scan_opts(BytewiseComparator());
  scan_opts.insert(Slice("a"), Slice("b"));
  scan_opts.insert(Slice("m"), Slice("n"));
  Slice upper_bound("b");
  ReadOptions read_options;
  read_options.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_options, db_->DefaultColumnFamily()));

  ASSERT_EQ(0, table_iterators_created);
  ASSERT_EQ(0, files_added);
  ASSERT_EQ(0, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  iter->Prepare(scan_opts);
  ASSERT_EQ(1, table_iterators_created);
  ASSERT_EQ(1, files_added);
  ASSERT_EQ(1, block_based_iterators);
  ASSERT_EQ(0, level_iterators);

  std::vector<std::string> keys;
  for (iter->Seek("a"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(keys, std::vector<std::string>({"a"}));

  upper_bound = "n";
  for (iter->Seek("m"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_EQ(keys, std::vector<std::string>({"a", "m"}));
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBIteratorBaseTest, PrepareWithMultiScanPrunesOverlappingL0Files) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("m", "vm"));
  ASSERT_OK(Flush());
  ASSERT_EQ(2, NumTableFilesAtLevel(0));

  int table_iterators_created = 0;
  int files_added = 0;
  int block_based_iterators = 0;
  int level_iterators = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::NewIterator::BeforeFindTable",
      [&](void* /*arg*/) { ++table_iterators_created; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:AddedFile",
      [&](void* /*arg*/) { ++files_added; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:IteratorType", [&](void* arg) {
        auto* iterator_type = static_cast<std::pair<bool, bool>*>(arg);
        if (iterator_type->first) {
          ++block_based_iterators;
        }
        if (iterator_type->second) {
          ++level_iterators;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  MultiScanArgs scan_opts(BytewiseComparator());
  scan_opts.insert(Slice("a"), Slice("n"));
  Slice upper_bound("n");
  ReadOptions read_options;
  read_options.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_options, db_->DefaultColumnFamily()));

  ASSERT_EQ(0, table_iterators_created);
  ASSERT_EQ(0, files_added);
  ASSERT_EQ(0, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  iter->Prepare(scan_opts);
  ASSERT_EQ(2, table_iterators_created);
  ASSERT_EQ(2, files_added);
  ASSERT_EQ(2, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  std::vector<std::string> keys;
  for (iter->Seek("a"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_EQ(keys, std::vector<std::string>({"a", "m"}));
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBIteratorBaseTest, PrepareWithMultiScanPrunesNonIntersectingMemTables) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("z_imm", "vz"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("z_mem", "vz"));

  int table_iterators_created = 0;
  int files_added = 0;
  int block_based_iterators = 0;
  int level_iterators = 0;
  int merging_iterators = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::NewIterator::BeforeFindTable",
      [&](void* /*arg*/) { ++table_iterators_created; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:AddedFile",
      [&](void* /*arg*/) { ++files_added; });
  SyncPoint::GetInstance()->SetCallBack(
      "Version::AddIteratorsForLevel:IteratorType", [&](void* arg) {
        auto* iterator_type = static_cast<std::pair<bool, bool>*>(arg);
        if (iterator_type->first) {
          ++block_based_iterators;
        }
        if (iterator_type->second) {
          ++level_iterators;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIteratorBuilder::Finish:UseMergingIterator", [&](void* arg) {
        if (*static_cast<bool*>(arg)) {
          ++merging_iterators;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  MultiScanArgs scan_opts(BytewiseComparator());
  scan_opts.insert(Slice("a"), Slice("b"));
  Slice upper_bound("b");
  ReadOptions read_options;
  read_options.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_options, db_->DefaultColumnFamily()));

  ASSERT_EQ(0, table_iterators_created);
  ASSERT_EQ(0, files_added);
  ASSERT_EQ(0, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  ASSERT_EQ(0, merging_iterators);
  iter->Prepare(scan_opts);
  ASSERT_EQ(1, table_iterators_created);
  ASSERT_EQ(1, files_added);
  ASSERT_EQ(1, block_based_iterators);
  ASSERT_EQ(0, level_iterators);
  ASSERT_EQ(0, merging_iterators);
  std::vector<std::string> keys;
  for (iter->Seek("a"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_EQ(keys, std::vector<std::string>({"a"}));
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(db_->ContinueBackgroundWork());
}

// Prepare() on an iterator that is already positioned must discard that
// position and yield the same results as on a fresh iterator. The test needs
// an L0 file, because L0 table iterators receive Prepare() directly from the
// merging iterator rather than through LevelIterator, so they are positioned
// on a data block.
TEST_F(DBIteratorBaseTest, PrepareWithMultiScanAfterPositioning) {
  // Other tests can leave counting disabled on this thread.
  const PerfLevel prev_perf_level = GetPerfLevel();
  SetPerfLevel(kEnableCount);
  Defer restore_perf_level(
      [prev_perf_level]() { SetPerfLevel(prev_perf_level); });

  constexpr int kNumKeys = 200;
  std::vector<std::string> keys;
  for (int i = 0; i <= kNumKeys; ++i) {
    keys.push_back(Key(i));
  }
  const std::string l1_value(50, '1');
  const std::string l0_value(50, '0');
  const std::vector<std::pair<int, int>> ranges = {{10, 20}, {120, 140}};

  struct Case {
    const char* name;
    std::function<void(Iterator*)> position;
    bool reverse;
  };
  const std::vector<Case> cases = {
      {"Seek inside a range", [&](Iterator* it) { it->Seek(keys[15]); }, false},
      {"Seek outside the ranges", [&](Iterator* it) { it->Seek(keys[190]); },
       false},
      {"Seek then Next",
       [&](Iterator* it) {
         it->Seek(keys[5]);
         for (int i = 0; i < 10; ++i) {
           it->Next();
         }
       },
       false},
      {"SeekToFirst", [](Iterator* it) { it->SeekToFirst(); }, false},
      {"SeekToLast", [](Iterator* it) { it->SeekToLast(); }, false},
      {"SeekForPrev", [&](Iterator* it) { it->SeekForPrev(keys[130]); }, false},
      {"Seek then reverse MultiScan", [&](Iterator* it) { it->Seek(keys[15]); },
       true},
  };

  for (bool first_key_index : {false, true}) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.compression = kNoCompression;
    BlockBasedTableOptions table_options;
    table_options.block_size = 256;
    if (first_key_index) {
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
    }
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);
    for (int i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(Put(keys[i], l1_value));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(1);
    for (int i = 0; i < kNumKeys; i += 7) {
      ASSERT_OK(Put(keys[i], l0_value));
    }
    ASSERT_OK(Flush());
    ASSERT_EQ(1, NumTableFilesAtLevel(0));
    ASSERT_EQ(1, NumTableFilesAtLevel(1));

    for (bool pin_data : {false, true}) {
      for (const Case& c : cases) {
        SCOPED_TRACE(std::string(c.name) +
                     " first_key_index=" + std::to_string(first_key_index) +
                     " pin_data=" + std::to_string(pin_data));
        Slice lower_bound = keys[0];
        Slice upper_bound = keys[kNumKeys];
        ReadOptions read_options;
        read_options.pin_data = pin_data;
        read_options.iterate_lower_bound = &lower_bound;
        read_options.iterate_upper_bound = &upper_bound;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        c.position(iter.get());
        ASSERT_TRUE(iter->Valid());
        ASSERT_OK(iter->status());

        MultiScanArgs scan_opts(BytewiseComparator());
        scan_opts.reverse = c.reverse;
        for (const auto& range : ranges) {
          scan_opts.insert(keys[range.first], keys[range.second]);
        }
        get_perf_context()->Reset();
        iter->Prepare(scan_opts);
        ASSERT_OK(iter->status());
        ASSERT_FALSE(iter->Valid());
        // The L0 table iterator took the MultiScan path.
        ASSERT_GT(get_perf_context()->multiscan_prepare_count, 0);

        std::vector<std::pair<std::string, std::string>> expected;
        std::vector<std::pair<std::string, std::string>> actual;
        for (const auto& range : ranges) {
          lower_bound = keys[range.first];
          upper_bound = keys[range.second];
          if (c.reverse) {
            for (int i = range.second - 1; i >= range.first; --i) {
              expected.emplace_back(keys[i], i % 7 == 0 ? l0_value : l1_value);
            }
            for (iter->SeekForPrev(upper_bound); iter->Valid(); iter->Prev()) {
              actual.emplace_back(iter->key().ToString(),
                                  iter->value().ToString());
            }
          } else {
            for (int i = range.first; i < range.second; ++i) {
              expected.emplace_back(keys[i], i % 7 == 0 ? l0_value : l1_value);
            }
            for (iter->Seek(lower_bound); iter->Valid(); iter->Next()) {
              actual.emplace_back(iter->key().ToString(),
                                  iter->value().ToString());
            }
          }
          ASSERT_OK(iter->status());
        }
        ASSERT_EQ(expected, actual);
      }
    }
  }
}

// Test param:
//   bool: whether to pass read_callback to NewIterator().
class DBIteratorTest : public DBIteratorBaseTest,
                       public testing::WithParamInterface<bool> {
 public:
  DBIteratorTest() = default;

  Iterator* NewIterator(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family = nullptr) {
    if (column_family == nullptr) {
      column_family = db_->DefaultColumnFamily();
    }
    auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    auto* cfd = cfh->cfd();
    SequenceNumber seq = read_options.snapshot != nullptr
                             ? read_options.snapshot->GetSequenceNumber()
                             : db_->GetLatestSequenceNumber();
    bool use_read_callback = GetParam();
    DummyReadCallback* read_callback = nullptr;
    if (use_read_callback) {
      read_callback = new DummyReadCallback();
      read_callback->SetSnapshot(seq);
      InstrumentedMutexLock lock(&mutex_);
      read_callbacks_.push_back(
          std::unique_ptr<DummyReadCallback>(read_callback));
    }
    DBImpl* db_impl = dbfull();
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(db_impl);
    return db_impl->NewIteratorImpl(read_options, cfh, super_version, seq,
                                    read_callback);
  }

 private:
  InstrumentedMutex mutex_;
  std::vector<std::unique_ptr<DummyReadCallback>> read_callbacks_;
};

TEST_P(DBIteratorTest, IteratorProperty) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, "1", "2"));
  ASSERT_OK(Delete(1, "2"));
  ReadOptions ropt;
  ropt.pin_data = false;
  {
    std::unique_ptr<Iterator> iter(NewIterator(ropt, handles_[1]));
    iter->SeekToFirst();
    std::string prop_value;
    ASSERT_NOK(iter->GetProperty("non_existing.value", &prop_value));
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("0", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("0", prop_value);
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
    ASSERT_EQ("1", prop_value);
    iter->Next();
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("Iterator is not valid.", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("Iterator is not valid.", prop_value);

    // Get internal key at which the iteration stopped (tombstone in this case).
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
    ASSERT_EQ("2", prop_value);

    prop_value.clear();
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.write-time", &prop_value));
    uint64_t write_time;
    Slice prop_slice = prop_value;
    ASSERT_TRUE(GetFixed64(&prop_slice, &write_time));
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), write_time);
  }
  Close();
}

TEST_P(DBIteratorTest, PersistedTierOnIterator) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions ropt;
  ropt.read_tier = kPersistedTier;

  auto* iter = db_->NewIterator(ropt, handles_[1]);
  ASSERT_TRUE(iter->status().IsNotSupported());
  delete iter;

  std::vector<Iterator*> iters;
  ASSERT_TRUE(db_->NewIterators(ropt, {handles_[1]}, &iters).IsNotSupported());
  Close();
}

TEST_P(DBIteratorTest, NonBlockingIteration) {
  do {
    ReadOptions non_blocking_opts;
    ReadOptions regular_opts;
    anon::OptionsOverride options_override;
    options_override.full_block_cache = true;
    Options options = CurrentOptions(options_override);
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;

    CreateAndReopenWithCF({"pikachu"}, options);
    // write one kv to the database.
    ASSERT_OK(Put(1, "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = NewIterator(non_blocking_opts, handles_[1]);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 1);
    delete iter;

    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    ASSERT_OK(Flush(1));

    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    uint64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    uint64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_EQ(count, 0);
    ASSERT_TRUE(iter->status().IsIncomplete());
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // read in the specified block via a regular get
    ASSERT_EQ(Get(1, "a"), "b");

    // verify that we can find it via a non-blocking scan
    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 1);
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // This test verifies block cache behaviors, which is not used by plain
    // table format.
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast | kSkipMmapReads));
}

TEST_P(DBIteratorTest, IterSeekBeforePrev) {
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("2", "j"));
  auto iter = NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter->Prev();
  iter->Seek(Slice("a"));
  iter->Prev();
  delete iter;
}

TEST_P(DBIteratorTest, IterReseekNewUpperBound) {
  Random rnd(301);
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.block_size_deviation = 50;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.compression = kNoCompression;
  Reopen(options);

  ASSERT_OK(Put("a", rnd.RandomString(400)));
  ASSERT_OK(Put("aabb", rnd.RandomString(400)));
  ASSERT_OK(Put("aaef", rnd.RandomString(400)));
  ASSERT_OK(Put("b", rnd.RandomString(400)));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ReadOptions opts;
  Slice ub = Slice("aa");
  opts.iterate_upper_bound = &ub;
  auto iter = NewIterator(opts);
  iter->Seek(Slice("a"));
  ub = Slice("b");
  iter->Seek(Slice("aabc"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "aaef");
  delete iter;
}

TEST_P(DBIteratorTest, IterSeekForPrevBeforeNext) {
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("2", "j"));
  auto iter = NewIterator(ReadOptions());
  iter->SeekForPrev(Slice("0"));
  iter->Next();
  iter->SeekForPrev(Slice("1"));
  iter->Next();
  delete iter;
}

namespace {
std::string MakeLongKey(size_t length, char c) {
  return std::string(length, c);
}
}  // anonymous namespace

TEST_P(DBIteratorTest, IterLongKeys) {
  ASSERT_OK(Put(MakeLongKey(20, 0), "0"));
  ASSERT_OK(Put(MakeLongKey(32, 2), "2"));
  ASSERT_OK(Put("a", "b"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put(MakeLongKey(50, 1), "1"));
  ASSERT_OK(Put(MakeLongKey(127, 3), "3"));
  ASSERT_OK(Put(MakeLongKey(64, 4), "4"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  iter->Seek(MakeLongKey(20, 0));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(20, 0) + "->0");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(64, 4) + "->4");

  iter->SeekForPrev(MakeLongKey(127, 3));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  delete iter;

  iter = NewIterator(ReadOptions());
  iter->Seek(MakeLongKey(50, 1));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  delete iter;
}

TEST_P(DBIteratorTest, IterNextWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("a"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->SeekForPrev(Slice("b"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");

  delete iter;
}

TEST_P(DBIteratorTest, IterPrevWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  delete iter;
}

TEST_P(DBIteratorTest, IterPrevWithNewerSeq2) {
  ASSERT_OK(Put("0", "0"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("e", "f"));
  auto iter = NewIterator(ReadOptions());
  auto iter2 = NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter2->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "c->d");
  ASSERT_EQ(IterStatus(iter2), "c->d");

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter2->Prev();
  ASSERT_EQ(IterStatus(iter2), "a->b");
  iter2->Prev();
  delete iter;
  delete iter2;
}

TEST_P(DBIteratorTest, IterEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekForPrev("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    ASSERT_OK(iter->status());

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterSingle) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterMulti) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", "vb"));
    ASSERT_OK(Put(1, "c", "vc"));
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("ax");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("d");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("c");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("bx");
    ASSERT_EQ(IterStatus(iter), "b->vb");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Seek("z");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    // Switch from reverse to forward
    iter->SeekToLast();
    iter->Prev();
    iter->Prev();
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Switch from forward to reverse
    iter->SeekToFirst();
    iter->Next();
    iter->Next();
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Make sure iter stays at snapshot
    ASSERT_OK(Put(1, "a", "va2"));
    ASSERT_OK(Put(1, "a2", "va3"));
    ASSERT_OK(Put(1, "b", "vb2"));
    ASSERT_OK(Put(1, "c", "vc2"));
    ASSERT_OK(Delete(1, "b"));
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

// Check that we can skip over a run of user keys
// by using reseek rather than sequential scan
TEST_P(DBIteratorTest, IterReseek) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  options.max_sequential_skip_in_iterations = 3;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // insert three keys with same userkey and verify that
  // reseek is not invoked. For each of these test cases,
  // verify that we can find the next key "b".
  ASSERT_OK(Put(1, "a", "zero"));
  ASSERT_OK(Put(1, "a", "one"));
  ASSERT_OK(Put(1, "a", "two"));
  ASSERT_OK(Put(1, "b", "bone"));
  Iterator* iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->two");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of three keys with same userkey and verify
  // that reseek is still not invoked.
  ASSERT_OK(Put(1, "a", "three"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->three");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of four keys with same userkey and verify
  // that reseek is invoked.
  ASSERT_OK(Put(1, "a", "four"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->four");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // Testing reverse iterator
  // At this point, we have three versions of "a" and one version of "b".
  // The reseek statistics is already at 1.
  int num_reseeks = static_cast<int>(
      TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION));

  // Insert another version of b and assert that reseek is not invoked
  ASSERT_OK(Put(1, "b", "btwo"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->btwo");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 1);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;

  // insert two more versions of b. This makes a total of 4 versions
  // of b and 4 versions of a.
  ASSERT_OK(Put(1, "b", "bthree"));
  ASSERT_OK(Put(1, "b", "bfour"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->bfour");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 2);
  iter->Prev();

  // the previous Prev call should have invoked reseek
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 3);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;
}

TEST_F(DBIteratorTest, ReseekUponDirectionChange) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.merge_operator.reset(
      new StringAppendTESTOperator(/*delim_char=*/' '));
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Put("bar", "value"));
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToLast();
    it->Prev();
    it->Next();
  }
  ASSERT_EQ(1,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  const std::string merge_key("good");
  ASSERT_OK(Put(merge_key, "orig"));
  ASSERT_OK(Merge(merge_key, "suffix"));
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->Seek(merge_key);
    ASSERT_TRUE(it->Valid());
    const uint64_t prev_reseek_count =
        options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
    it->Prev();
    ASSERT_EQ(prev_reseek_count + 1, options.statistics->getTickerCount(
                                         NUMBER_OF_RESEEKS_IN_ITERATION));
  }
}

TEST_P(DBIteratorTest, IterSmallAndLargeMix) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", std::string(100000, 'b')));
    ASSERT_OK(Put(1, "c", "vc"));
    ASSERT_OK(Put(1, "d", std::string(100000, 'd')));
    ASSERT_OK(Put(1, "e", std::string(100000, 'e')));

    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterMultiWithDelete) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);
    iter->Seek("kc");
    ASSERT_EQ(IterStatus(iter), "kc->vc");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ &&
          kHashSkipList != option_config_) {  // doesn't support SeekToLast
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "ka->va");
      }
    }
    delete iter;
  } while (ChangeOptions());
}

TEST_P(DBIteratorTest, IterPrevMaxSkip) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    for (int i = 0; i < 2; i++) {
      ASSERT_OK(Put(1, "key1", "v1"));
      ASSERT_OK(Put(1, "key2", "v2"));
      ASSERT_OK(Put(1, "key3", "v3"));
      ASSERT_OK(Put(1, "key4", "v4"));
      ASSERT_OK(Put(1, "key5", "v5"));
    }

    VerifyIterLast("key5->v5", 1);

    ASSERT_OK(Delete(1, "key5"));
    VerifyIterLast("key4->v4", 1);

    ASSERT_OK(Delete(1, "key4"));
    VerifyIterLast("key3->v3", 1);

    ASSERT_OK(Delete(1, "key3"));
    VerifyIterLast("key2->v2", 1);

    ASSERT_OK(Delete(1, "key2"));
    VerifyIterLast("key1->v1", 1);

    ASSERT_OK(Delete(1, "key1"));
    VerifyIterLast("(invalid)", 1);
  } while (ChangeOptions(kSkipMergePut | kSkipNoSeekToLast));
}

TEST_P(DBIteratorTest, IterWithSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    ASSERT_OK(Put(1, "key1", "val1"));
    ASSERT_OK(Put(1, "key2", "val2"));
    ASSERT_OK(Put(1, "key3", "val3"));
    ASSERT_OK(Put(1, "key4", "val4"));
    ASSERT_OK(Put(1, "key5", "val5"));

    const Snapshot* snapshot = db_->GetSnapshot();
    ReadOptions options;
    options.snapshot = snapshot;
    Iterator* iter = NewIterator(options, handles_[1]);

    ASSERT_OK(Put(1, "key0", "val0"));
    // Put more values after the snapshot
    ASSERT_OK(Put(1, "key100", "val100"));
    ASSERT_OK(Put(1, "key101", "val101"));

    iter->Seek("key5");
    ASSERT_EQ(IterStatus(iter), "key5->val5");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key3->val3");

        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key5->val5");
      }
      iter->Next();
      ASSERT_TRUE(!iter->Valid());
    }

    if (!CurrentOptions().merge_operator) {
      // TODO(gzh): merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->SeekForPrev("key1");
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key3->val3");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Prev();
        ASSERT_TRUE(!iter->Valid());
      }
    }
    db_->ReleaseSnapshot(snapshot);
    ASSERT_OK(iter->status());
    delete iter;
  } while (ChangeOptions());
}

TEST_P(DBIteratorTest, IteratorPinsRef) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "hello"));

    // Get iterator that will yield the current contents of the DB.
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    // Write to force compactions
    ASSERT_OK(Put(1, "foo", "newvalue1"));
    for (int i = 0; i < 100; i++) {
      // 100K values
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(100000, 'v')));
    }
    ASSERT_OK(Put(1, "foo", "newvalue2"));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ("hello", iter->value().ToString());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IteratorDeleteAfterCfDelete) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  ASSERT_OK(Put(1, "foo", "delete-cf-then-delete-iter"));
  ASSERT_OK(Put(1, "hello", "value2"));

  ColumnFamilyHandle* cf = handles_[1];
  ReadOptions ro;

  auto* iter = db_->NewIterator(ro, cf);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "foo->delete-cf-then-delete-iter");

  // delete CF handle
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  handles_.erase(std::begin(handles_) + 1);

  // delete Iterator after CF handle is deleted
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "hello->value2");
  delete iter;
}

TEST_P(DBIteratorTest, IteratorSeekAfterCfDelete) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  ASSERT_OK(Put(1, "foo", "delete-cf-then-seek-iter"));
  ASSERT_OK(Put(1, "hello", "value2"));

  ColumnFamilyHandle* cf = handles_[1];
  ReadOptions ro;

  auto* iter = db_->NewIterator(ro, cf);

  // Delete the CF handle before the lazy iterator tree is materialized.
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  handles_.erase(std::begin(handles_) + 1);

  iter->Seek("foo");
  ASSERT_EQ(IterStatus(iter), "foo->delete-cf-then-seek-iter");
  iter->SeekForPrev("hello");
  ASSERT_EQ(IterStatus(iter), "hello->value2");
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "foo->delete-cf-then-seek-iter");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "hello->value2");
  delete iter;
}

TEST_P(DBIteratorTest, IteratorAutoRefreshAfterCfDeleteBeforeLazyInit) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  ASSERT_OK(Put(1, "foo", "delete-cf-then-auto-refresh"));
  ASSERT_OK(Flush(1));

  ColumnFamilyHandle* cf = handles_[1];
  const Snapshot* snapshot = db_->GetSnapshot();

  ReadOptions ro;
  ro.snapshot = snapshot;
  ro.auto_refresh_iterator_with_snapshot = true;

  auto* iter = db_->NewIterator(ro, cf);

  ASSERT_OK(Put(1, "zzz", "after-snapshot"));
  ASSERT_OK(Flush(1));

  // Delete the CF handle before the lazy iterator tree is materialized. The
  // following operations force auto-refresh to acquire a newer SuperVersion.
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  handles_.erase(std::begin(handles_) + 1);

  iter->Seek("foo");
  ASSERT_EQ(IterStatus(iter), "foo->delete-cf-then-auto-refresh");
  ASSERT_OK(iter->status());
  iter->SeekForPrev("foo");
  ASSERT_EQ(IterStatus(iter), "foo->delete-cf-then-auto-refresh");
  ASSERT_OK(iter->status());
  iter->Next();
  ASSERT_OK(iter->status());

  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_P(DBIteratorTest, IteratorDeleteAfterCfDrop) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  ASSERT_OK(Put(1, "foo", "drop-cf-then-delete-iter"));

  ReadOptions ro;
  ColumnFamilyHandle* cf = handles_[1];

  auto* iter = db_->NewIterator(ro, cf);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "foo->drop-cf-then-delete-iter");

  // drop and delete CF
  EXPECT_OK(db_->DropColumnFamily(cf));
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  handles_.erase(std::begin(handles_) + 1);

  // delete Iterator after CF handle is dropped
  delete iter;
}

TEST_P(DBIteratorTest, IteratorSeekAfterCfDrop) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  ASSERT_OK(Put(1, "foo", "drop-cf-then-seek-iter"));

  ReadOptions ro;
  ColumnFamilyHandle* cf = handles_[1];

  auto* iter = db_->NewIterator(ro, cf);

  // Drop and delete the CF before the lazy iterator tree is materialized.
  EXPECT_OK(db_->DropColumnFamily(cf));
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  handles_.erase(std::begin(handles_) + 1);

  iter->Seek("foo");
  ASSERT_EQ(IterStatus(iter), "foo->drop-cf-then-seek-iter");
  iter->SeekForPrev("foo");
  ASSERT_EQ(IterStatus(iter), "foo->drop-cf-then-seek-iter");
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "foo->drop-cf-then-seek-iter");
  delete iter;
}

// SetOptions not defined in ROCKSDB LITE
TEST_P(DBIteratorTest, DBIteratorBoundTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;

  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing basic case with no iterate_upper_bound and no prefix_extractor
  {
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->SeekForPrev("g1");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);
  }

  // testing iterate_upper_bound and forward iterator
  // to make sure it stops at bound
  {
    ReadOptions ro;
    // iterate_upper_bound points beyond the last expected entry
    Slice prefix("foo2");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("foo1")), 0);

    iter->Next();
    // should stop here...
    ASSERT_TRUE(!iter->Valid());
  }
  // Testing SeekToLast with iterate_upper_bound set
  {
    ReadOptions ro;

    Slice prefix("foo");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("a")), 0);
  }

  // prefix is the first letter of the key
  ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:1"}}));
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing with iterate_upper_bound and prefix_extractor
  // Seek target and iterate_upper_bound are not is same prefix
  // This should be an error
  {
    ReadOptions ro;
    Slice upper_bound("g");
    ro.iterate_upper_bound = &upper_bound;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo1", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(!iter->Valid());
  }

  // testing that iterate_upper_bound prevents iterating over deleted items
  // if the bound has already reached
  {
    options.prefix_extractor = nullptr;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_OK(Put("b", "0"));
    ASSERT_OK(Put("b1", "0"));
    ASSERT_OK(Put("c", "0"));
    ASSERT_OK(Put("d", "0"));
    ASSERT_OK(Put("e", "0"));
    ASSERT_OK(Delete("c"));
    ASSERT_OK(Delete("d"));

    // base case with no bound
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    get_perf_context()->Reset();
    iter->Next();

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(
        static_cast<int>(get_perf_context()->internal_delete_skipped_count), 2);

    // now testing with iterate_bound
    Slice prefix("c");
    ro.iterate_upper_bound = &prefix;

    iter.reset(NewIterator(ro));

    get_perf_context()->Reset();

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    iter->Next();
    // the iteration should stop as soon as the bound key is reached
    // even though the key is deleted
    // hence internal_delete_skipped_count should be 0
    ASSERT_TRUE(!iter->Valid());
    ASSERT_EQ(
        static_cast<int>(get_perf_context()->internal_delete_skipped_count), 0);
  }
}

TEST_P(DBIteratorTest, DBIteratorBoundMultiSeek) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("z", "0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Put("foo3", "bar3"));
  ASSERT_OK(Put("foo4", "bar4"));

  {
    std::string up_str = "foo5";
    Slice up(up_str);
    ReadOptions ro;
    ro.iterate_upper_bound = &up;
    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    uint64_t prev_block_cache_hit =
        TestGetTickerCount(options, BLOCK_CACHE_HIT);
    uint64_t prev_block_cache_miss =
        TestGetTickerCount(options, BLOCK_CACHE_MISS);

    ASSERT_GT(prev_block_cache_hit + prev_block_cache_miss, 0);

    iter->Seek("foo4");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo4")), 0);
    ASSERT_EQ(prev_block_cache_hit,
              TestGetTickerCount(options, BLOCK_CACHE_HIT));
    ASSERT_EQ(prev_block_cache_miss,
              TestGetTickerCount(options, BLOCK_CACHE_MISS));

    iter->Seek("foo2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo2")), 0);
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo3")), 0);
    ASSERT_EQ(prev_block_cache_hit,
              TestGetTickerCount(options, BLOCK_CACHE_HIT));
    ASSERT_EQ(prev_block_cache_miss,
              TestGetTickerCount(options, BLOCK_CACHE_MISS));
  }
}

TEST_P(DBIteratorTest, DBIteratorBoundOptimizationTest) {
  for (auto format_version : {2, 3, 4}) {
    int upper_bound_hits = 0;
    Options options = CurrentOptions();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableIterator:out_of_bound",
        [&upper_bound_hits](void*) { upper_bound_hits++; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    options.env = env_;
    options.create_if_missing = true;
    options.prefix_extractor = nullptr;
    BlockBasedTableOptions table_options;
    table_options.format_version = format_version;
    table_options.flush_block_policy_factory =
        std::make_shared<FlushBlockEveryKeyPolicyFactory>();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    DestroyAndReopen(options);
    ASSERT_OK(Put("foo1", "bar1"));
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Flush());

    Slice ub("foo3");
    ReadOptions ro;
    ro.iterate_upper_bound = &ub;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);
    ASSERT_EQ(upper_bound_hits, 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo2")), 0);
    ASSERT_EQ(upper_bound_hits, 0);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(upper_bound_hits, 1);
  }
}

// Enable kBinarySearchWithFirstKey, do some iterator operations and check that
// they don't do unnecessary block reads.
TEST_P(DBIteratorTest, IndexWithFirstKey) {
  for (int tailing = 0; tailing < 2; ++tailing) {
    SCOPED_TRACE("tailing = " + std::to_string(tailing));
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.prefix_extractor = nullptr;
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    Statistics* stats = options.statistics.get();
    BlockBasedTableOptions table_options;
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
    table_options.index_shortening =
        BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
    table_options.flush_block_policy_factory =
        std::make_shared<FlushBlockEveryKeyPolicyFactory>();
    table_options.block_cache =
        NewLRUCache(8000);  // fits all blocks and their cache metadata overhead
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    DestroyAndReopen(options);
    ASSERT_OK(Merge("a1", "x1"));
    ASSERT_OK(Merge("b1", "y1"));
    ASSERT_OK(Merge("c0", "z1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Merge("a2", "x2"));
    ASSERT_OK(Merge("b2", "y2"));
    ASSERT_OK(Merge("c0", "z2"));
    ASSERT_OK(Flush());
    ASSERT_OK(Merge("a3", "x3"));
    ASSERT_OK(Merge("b3", "y3"));
    ASSERT_OK(Merge("c3", "z3"));
    ASSERT_OK(Flush());

    // Block cache is not important for this test.
    // We use BLOCK_CACHE_DATA_* counters just because they're the most readily
    // available way of counting block accesses.

    ReadOptions ropt;
    ropt.tailing = tailing;
    std::unique_ptr<Iterator> iter(NewIterator(ropt));

    ropt.read_tier = ReadTier::kBlockCacheTier;
    std::unique_ptr<Iterator> nonblocking_iter(NewIterator(ropt));

    iter->Seek("b10");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b2", iter->key().ToString());
    EXPECT_EQ("y2", iter->value().ToString());
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    // The cache-only iterator should succeed too, using the blocks pulled into
    // the cache by the previous iterator.
    nonblocking_iter->Seek("b10");
    ASSERT_TRUE(nonblocking_iter->Valid());
    EXPECT_EQ("b2", nonblocking_iter->key().ToString());
    EXPECT_EQ("y2", nonblocking_iter->value().ToString());
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // ... but it shouldn't be able to step forward since the next block is
    // not in cache yet.
    nonblocking_iter->Next();
    ASSERT_FALSE(nonblocking_iter->Valid());
    ASSERT_TRUE(nonblocking_iter->status().IsIncomplete());

    // ... nor should a seek to the next key succeed.
    nonblocking_iter->Seek("b20");
    ASSERT_FALSE(nonblocking_iter->Valid());
    ASSERT_TRUE(nonblocking_iter->status().IsIncomplete());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b3", iter->key().ToString());
    EXPECT_EQ("y3", iter->value().ToString());
    EXPECT_EQ(4, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // After the blocking iterator loaded the next block, the nonblocking
    // iterator's seek should succeed.
    nonblocking_iter->Seek("b20");
    ASSERT_TRUE(nonblocking_iter->Valid());
    EXPECT_EQ("b3", nonblocking_iter->key().ToString());
    EXPECT_EQ("y3", nonblocking_iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->Seek("c0");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("c0", iter->key().ToString());
    EXPECT_EQ("z1,z2", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(6, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("c3", iter->key().ToString());
    EXPECT_EQ("z3", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(7, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter.reset();

    // Enable iterate_upper_bound and check that iterator is not trying to read
    // blocks that are fully above upper bound.
    std::string ub = "b3";
    Slice ub_slice(ub);
    ropt.iterate_upper_bound = &ub_slice;
    iter.reset(NewIterator(ropt));

    iter->Seek("b2");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b2", iter->key().ToString());
    EXPECT_EQ("y2", iter->value().ToString());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(7, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(7, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  }
}

TEST_P(DBIteratorTest, IndexWithFirstKeyGet) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor = nullptr;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  Statistics* stats = options.statistics.get();
  BlockBasedTableOptions table_options;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
  table_options.index_shortening =
      BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  table_options.block_cache = NewLRUCache(1000);  // fits all blocks
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);
  ASSERT_OK(Merge("a", "x1"));
  ASSERT_OK(Merge("c", "y1"));
  ASSERT_OK(Merge("e", "z1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("c", "y2"));
  ASSERT_OK(Merge("e", "z2"));
  ASSERT_OK(Flush());

  // Get() between blocks shouldn't read any blocks.
  ASSERT_EQ("NOT_FOUND", Get("b"));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

  // Get() of an existing key shouldn't read any unnecessary blocks when there's
  // only one key per block.

  ASSERT_EQ("y1,y2", Get("c"));
  EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

  ASSERT_EQ("x1", Get("a"));
  EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

  EXPECT_EQ(std::vector<std::string>({"NOT_FOUND", "z1,z2"}),
            MultiGet({"b", "e"}));
}

// TODO(3.13): fix the issue of Seek() + Prev() which might not necessary
//             return the biggest key which is smaller than the seek key.
TEST_P(DBIteratorTest, PrevAfterAndNextAfterMerge) {
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.env = env_;
  DestroyAndReopen(options);

  // write three entries with different keys using Merge()
  WriteOptions wopts;
  ASSERT_OK(db_->Merge(wopts, "1", "data1"));
  ASSERT_OK(db_->Merge(wopts, "2", "data2"));
  ASSERT_OK(db_->Merge(wopts, "3", "data3"));

  std::unique_ptr<Iterator> it(NewIterator(ReadOptions()));

  it->Seek("2");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());

  it->Prev();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->SeekForPrev("1");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());
}

class DBIteratorTestForPinnedData : public DBIteratorTest {
 public:
  enum TestConfig {
    NORMAL,
    CLOSE_AND_OPEN,
    COMPACT_BEFORE_READ,
    FLUSH_EVERY_1000,
    MAX
  };
  DBIteratorTestForPinnedData() : DBIteratorTest() {}
  void PinnedDataIteratorRandomized(TestConfig run_config) {
    // Generate Random data
    Random rnd(301);

    int puts = 100000;
    int key_pool = static_cast<int>(puts * 0.7);
    int key_size = 100;
    int val_size = 1000;
    int seeks_percentage = 20;   // 20% of keys will be used to test seek()
    int delete_percentage = 20;  // 20% of keys will be deleted
    int merge_percentage = 20;   // 20% of keys will be added using Merge()

    Options options = CurrentOptions();
    BlockBasedTableOptions table_options;
    table_options.use_delta_encoding = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.merge_operator = MergeOperators::CreatePutOperator();
    DestroyAndReopen(options);

    std::vector<std::string> generated_keys(key_pool);
    for (int i = 0; i < key_pool; i++) {
      generated_keys[i] = rnd.RandomString(key_size);
    }

    std::map<std::string, std::string> true_data;
    std::vector<std::string> random_keys;
    std::vector<std::string> deleted_keys;
    for (int i = 0; i < puts; i++) {
      auto& k = generated_keys[rnd.Next() % key_pool];
      auto v = rnd.RandomString(val_size);

      // Insert data to true_data map and to DB
      true_data[k] = v;
      if (rnd.PercentTrue(merge_percentage)) {
        ASSERT_OK(db_->Merge(WriteOptions(), k, v));
      } else {
        ASSERT_OK(Put(k, v));
      }

      // Pick random keys to be used to test Seek()
      if (rnd.PercentTrue(seeks_percentage)) {
        random_keys.push_back(k);
      }

      // Delete some random keys
      if (rnd.PercentTrue(delete_percentage)) {
        deleted_keys.push_back(k);
        true_data.erase(k);
        ASSERT_OK(Delete(k));
      }

      if (run_config == TestConfig::FLUSH_EVERY_1000) {
        if (i && i % 1000 == 0) {
          ASSERT_OK(Flush());
        }
      }
    }

    if (run_config == TestConfig::CLOSE_AND_OPEN) {
      Close();
      Reopen(options);
    } else if (run_config == TestConfig::COMPACT_BEFORE_READ) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    ReadOptions ro;
    ro.pin_data = true;
    auto iter = NewIterator(ro);

    {
      // Test Seek to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->Seek(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.lower_bound(k), true_data.end());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back(true_data.lower_bound(k)->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }

    {
      // Test SeekForPrev to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->SeekForPrev(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.upper_bound(k), true_data.begin());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back((--true_data.upper_bound(k))->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }

    {
      // Test iterating all data forward
      std::vector<Slice> all_keys;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid
      auto data_iter = true_data.begin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }

    {
      // Test iterating all data backward
      std::vector<Slice> all_keys;
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid (backward)
      auto data_iter = true_data.rbegin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }

    delete iter;
  }
};

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedNormal) {
  PinnedDataIteratorRandomized(TestConfig::NORMAL);
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedCLoseAndOpen) {
  PinnedDataIteratorRandomized(TestConfig::CLOSE_AND_OPEN);
}

TEST_P(DBIteratorTestForPinnedData,
       PinnedDataIteratorRandomizedCompactBeforeRead) {
  PinnedDataIteratorRandomized(TestConfig::COMPACT_BEFORE_READ);
}

TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedFlush) {
  PinnedDataIteratorRandomized(TestConfig::FLUSH_EVERY_1000);
}

INSTANTIATE_TEST_CASE_P(DBIteratorTestForPinnedDataInstance,
                        DBIteratorTestForPinnedData,
                        testing::Values(true, false));

TEST_P(DBIteratorTest, PinnedDataIteratorMultipleFiles) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.disable_auto_compactions = true;
  options.write_buffer_size = 1024 * 1024 * 10;  // 10 Mb
  DestroyAndReopen(options);

  std::map<std::string, std::string> true_data;

  // Generate 4 sst files in L2
  Random rnd(301);
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i * 3);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
    if (i % 250 == 0) {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(FilesPerLevel(0), "4");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(FilesPerLevel(0), "0,4");

  // Generate 4 sst files in L0
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i * 2);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
    if (i % 250 == 0) {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(FilesPerLevel(0), "4,4");

  // Add some keys/values in memtables
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }
  ASSERT_EQ(FilesPerLevel(0), "4,4");

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  std::vector<std::pair<Slice, Slice>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value());
  }

  ASSERT_EQ(results.size(), true_data.size());
  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }
  ASSERT_OK(iter->status());
  delete iter;
}

// The PinnedDataIteratorImmortalMmap* tests below are heavy (thousands of
// seeks plus full scans over a multi-level DB), so they are skipped under
// Valgrind like the neighbouring PinnedDataIteratorRandomized tests.
#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
namespace {
// Forces TableReaderOptions::immortal, like a wrapping factory may do for a
// DB whose table files never go away.
class ImmortalTableFactory : public TableFactory {
 public:
  explicit ImmortalTableFactory(std::shared_ptr<TableFactory> inner)
      : inner_(std::move(inner)) {}
  const char* Name() const override { return "ImmortalTableFactory"; }
  using TableFactory::NewTableReader;
  Status NewTableReader(const ReadOptions& ro, const TableReaderOptions& tro,
                        std::unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size,
                        std::unique_ptr<TableReader>* table_reader,
                        bool prefetch_index_and_filter) const override {
    TableReaderOptions immortal_tro(tro);
    immortal_tro.immortal = true;
    return inner_->NewTableReader(ro, immortal_tro, std::move(file), file_size,
                                  table_reader, prefetch_index_and_filter);
  }
  TableBuilder* NewTableBuilder(const TableBuilderOptions& tbo,
                                WritableFileWriter* file) const override {
    return inner_->NewTableBuilder(tbo, file);
  }
  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<ImmortalTableFactory>(inner_->Clone());
  }

 private:
  std::shared_ptr<TableFactory> inner_;
};

// Uncompressed persistent cache keeping about half of the blocks. Blocks
// read back from it are heap-owned; the others keep coming from the file.
class UncompressedPersistentCache : public PersistentCache {
 public:
  Status Insert(const Slice& key, const char* data, size_t size) override {
    MutexLock l(&mu_);
    if (std::hash<std::string>()(key.ToString()) % 2 == 0) {
      data_[key.ToString()].assign(data, size);
    }
    return Status::OK();
  }
  Status Lookup(const Slice& key, std::unique_ptr<char[]>* data,
                size_t* size) override {
    MutexLock l(&mu_);
    auto it = data_.find(key.ToString());
    if (it == data_.end()) {
      return Status::NotFound();
    }
    data->reset(new char[it->second.size()]);
    memcpy(data->get(), it->second.data(), it->second.size());
    *size = it->second.size();
    return Status::OK();
  }
  bool IsCompressed() override { return false; }
  StatsType Stats() override { return StatsType(); }
  std::string GetPrintableOptions() const override { return ""; }
  uint64_t NewId() override { return next_id_++; }

 private:
  port::Mutex mu_;
  std::map<std::string, std::string> data_;
  std::atomic<uint64_t> next_id_{1};
};

// Shared setup of the PinnedDataIteratorImmortalMmap* tests.
constexpr int kImmortalKeys = 3000;
constexpr int kImmortalKeysPerFile = 100;
constexpr int kImmortalSeeks = 2000;
constexpr int kImmortalRangeDelBegin = 1500;
constexpr int kImmortalRangeDelEnd = 1560;

// Counts how LevelIterator disposes of the file iterators it leaves and how
// many data blocks BlockBasedTableIterator delegates to the
// PinnedIteratorsManager.
struct PinCounters {
  std::atomic<int> pinned_file_iters{0};
  std::atomic<int> dropped_file_iters{0};
  std::atomic<int> delegated_blocks{0};
  // Dropped iterators must never hold a cleanup (table cache handle).
  std::atomic<int> dropped_with_cleanups{0};

  PinCounters() {
    SyncPoint::GetInstance()->SetCallBack(
        "LevelIterator::SetFileIterator:PinIterator",
        [this](void*) { pinned_file_iters++; });
    SyncPoint::GetInstance()->SetCallBack(
        "LevelIterator::SetFileIterator:DeleteIterator", [this](void* arg) {
          if (arg != nullptr) {
            dropped_file_iters++;
            if (static_cast<InternalIterator*>(arg)->HasCleanups()) {
              dropped_with_cleanups++;
            }
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableIterator::ResetDataIter:DelegateBlock",
        [this](void*) { delegated_blocks++; });
    SyncPoint::GetInstance()->EnableProcessing();
  }
  ~PinCounters() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  void Reset() {
    pinned_file_iters = 0;
    dropped_file_iters = 0;
    delegated_blocks = 0;
    dropped_with_cleanups = 0;
  }
};

// Writes Key(0) .. Key(kImmortalKeys - 1) interleaved into L3, L2 and L1
// (key i goes to level (i % 3) + 1), several files per level, calling
// before_level(level) first, e.g. to reopen with other table options. One L1
// file also holds a range tombstone (within its own key range, so the files
// still move down without being compacted) that deletes keys of all three
// levels. true_data receives the surviving keys.
void WriteInterleavedLevels(DBTestBase* t, Random* rnd,
                            const std::function<void(int level)>& before_level,
                            std::map<std::string, std::string>* true_data) {
  for (int level = 3; level >= 1; level--) {
    before_level(level);
    for (int i = level - 1; i < kImmortalKeys; i += 3) {
      if (level == 1 && i == kImmortalRangeDelEnd) {
        ASSERT_OK(t->db_->DeleteRange(WriteOptions(),
                                      t->db_->DefaultColumnFamily(),
                                      DBTestBase::Key(kImmortalRangeDelBegin),
                                      DBTestBase::Key(kImmortalRangeDelEnd)));
        true_data->erase(
            true_data->lower_bound(DBTestBase::Key(kImmortalRangeDelBegin)),
            true_data->lower_bound(DBTestBase::Key(kImmortalRangeDelEnd)));
      }
      std::string k = DBTestBase::Key(i);
      std::string v = test::CompressibleString(rnd, 0.5, 100);
      ASSERT_OK(t->Put(k, v));
      (*true_data)[k] = v;
      if ((i / 3) % kImmortalKeysPerFile == kImmortalKeysPerFile - 1) {
        ASSERT_OK(t->Flush());
      }
    }
    ASSERT_OK(t->Flush());
    t->MoveFilesToLevel(level);
  }
  for (int level = 1; level <= 3; level++) {
    ASSERT_GT(t->NumTableFilesAtLevel(level), 4);
  }
  ASSERT_EQ(kImmortalKeys - (kImmortalRangeDelEnd - kImmortalRangeDelBegin),
            true_data->size());
}

// A key/value pair as the iterator returned it, plus copies to compare
// against after the iterator moved on (and, under ASan, to detect slices
// into freed memory). Unpinned keys live in DBIter's buffer and are not
// saved; values are always pinned.
struct SavedEntry {
  Slice key, value;
  std::string key_copy, value_copy;
};

// Checks the iterator's current entry against true_data and the expected
// is-key-pinned property (nullptr: varies by block), then saves it.
// is-value-pinned is only reported in the forward direction.
void CheckAndSave(Iterator* iter,
                  const std::map<std::string, std::string>& true_data,
                  const char* expected_keys_pinned, bool forward,
                  std::vector<SavedEntry>* saved) {
  ASSERT_TRUE(iter->Valid());
  std::string prop;
  ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop));
  if (expected_keys_pinned != nullptr) {
    ASSERT_EQ(expected_keys_pinned, prop);
  }
  const bool key_pinned = prop == "1";
  if (forward) {
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop));
    ASSERT_EQ("1", prop);
  }
  auto it = true_data.find(iter->key().ToString());
  ASSERT_TRUE(it != true_data.end());
  ASSERT_EQ(it->second, iter->value().ToString());
  saved->push_back({key_pinned ? iter->key() : Slice(), iter->value(),
                    key_pinned ? iter->key().ToString() : "",
                    iter->value().ToString()});
}

// Random seeks with Next/Prev/Prev around each, then a full forward and a
// full backward scan, saving every entry.
void SeekAndScanAll(Iterator* iter, Random* rnd,
                    const std::map<std::string, std::string>& true_data,
                    const char* expected_keys_pinned,
                    std::vector<SavedEntry>* saved) {
  for (int i = 0; i < kImmortalSeeks; i++) {
    // kImmortalKeys - 1 so that Next() stays valid.
    iter->Seek(
        DBTestBase::Key(static_cast<int>(rnd->Uniform(kImmortalKeys - 1))));
    ASSERT_NO_FATAL_FAILURE(
        CheckAndSave(iter, true_data, expected_keys_pinned, true, saved));
    iter->Next();
    ASSERT_NO_FATAL_FAILURE(
        CheckAndSave(iter, true_data, expected_keys_pinned, true, saved));
    iter->Prev();
    iter->Prev();
    if (iter->Valid()) {
      ASSERT_NO_FATAL_FAILURE(
          CheckAndSave(iter, true_data, expected_keys_pinned, false, saved));
    }
  }
  size_t n = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), n++) {
    ASSERT_NO_FATAL_FAILURE(
        CheckAndSave(iter, true_data, expected_keys_pinned, true, saved));
  }
  ASSERT_EQ(true_data.size(), n);
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    ASSERT_NO_FATAL_FAILURE(
        CheckAndSave(iter, true_data, expected_keys_pinned, false, saved));
  }
  ASSERT_OK(iter->status());
}

void VerifySaved(const std::vector<SavedEntry>& saved) {
  ASSERT_GT(saved.size(), 0u);
  for (const SavedEntry& s : saved) {
    ASSERT_EQ(s.key_copy, s.key.ToString());
    ASSERT_EQ(s.value_copy, s.value.ToString());
  }
}
}  // namespace

// LevelIterator drops old file iterators instead of pinning them only for
// immortal tables (ReadOnly with max_open_files=-1, or an immortal factory)
// without table cache handles or first-key indexes, while positioned on no
// block or on a block in the immortal source (mmap, uncompressed, no cache
// handle). Owned blocks it left earlier (here: persistent cache hits, mixed
// with mmap blocks in the same iterator) are delegated to the
// PinnedIteratorsManager. Every other setup must keep pinning. Run under ASan
// to check that saved slices stay valid.
//
// These tests use TEST_F(DBIteratorBaseTest) rather than the parameterized
// DBIteratorTest fixture of their neighbours: the read_callback parameter is
// irrelevant here and would only double an already heavy test's runtime.
TEST_F(DBIteratorBaseTest, PinnedDataIteratorImmortalMmapNoPin) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires mmap reads");
    return;
  }
  PinCounters counters;

  // kPinNone: all dropped. kPinSome: pinned while on an owned block.
  // kPinMost: most pinned, some dropped (e.g. exhausted ones). kPinAll: never
  // dropped.
  enum Pins { kPinNone, kPinSome, kPinMost, kPinAll };
  struct Mode {
    bool read_only, mmap, block_cache, persistent_cache, immortal_factory,
        fill_cache;
    int max_open_files;
    Pins pins;
    // Expected is-key-pinned, or nullptr if it varies by block.
    const char* keys_pinned;
  };
  // {read_only, mmap, block_cache, persistent_cache, immortal_factory,
  //  fill_cache, max_open_files, pins, keys_pinned}
  const std::vector<Mode> kModes = {
      // Immortal, mmap: dropped. The reader does not insert unowned (mmap,
      // uncompressed) blocks into the block cache, so also with a cache.
      {true, true, false, false, false, true, -1, kPinNone, "1"},
      {true, true, true, false, false, true, -1, kPinNone, "1"},
      {false, true, false, false, true, true, -1, kPinNone, "1"},
      // fill_cache=false charges a placeholder for each block to the block
      // cache through a cleanup on the block iterator; dropping the block
      // releases it right away instead of keeping it until the iterator dies.
      {true, true, true, false, false, false, -1, kPinNone, "1"},
      // Immortal, mmap blocks mixed with owned persistent cache hits.
      {true, true, false, true, false, true, -1, kPinSome, nullptr},
      // Immortal, cached or heap-owned blocks.
      {true, false, true, false, false, true, -1, kPinMost, "1"},
      {true, false, false, false, false, true, -1, kPinMost, "0"},
      // Not immortal: cached blocks pin through the handle; unowned mmap
      // blocks are not pinned at all.
      {false, false, true, false, false, true, -1, kPinAll, "1"},
      {false, true, false, false, false, true, -1, kPinAll, "0"},
      // Immortal with table cache handles (only files whose readers were
      // preloaded into FileMetaData have none and are dropped).
      {false, true, false, false, true, true, 20, kPinMost, "1"},
  };
  // Keys taken from index values: always pin.
  const std::vector<Mode> kFirstKeyModes = {
      {true, true, false, false, false, true, -1, kPinAll, "1"}};
  for (bool first_key_index : {false, true}) {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    options.disable_auto_compactions = true;
    options.level_compaction_dynamic_level_bytes = false;
    BlockBasedTableOptions table_options;
    table_options.use_delta_encoding = false;
    table_options.block_size = 1024;
    if (first_key_index) {
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
    }
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    std::map<std::string, std::string> true_data;
    Random rnd(301);
    ASSERT_NO_FATAL_FAILURE(
        WriteInterleavedLevels(this, &rnd, [](int /*level*/) {}, &true_data));

    const std::vector<Mode>& modes = first_key_index ? kFirstKeyModes : kModes;
    for (const Mode& mode : modes) {
      SCOPED_TRACE(
          "first_key_index=" + std::to_string(first_key_index) +
          " read_only=" + std::to_string(mode.read_only) +
          " mmap=" + std::to_string(mode.mmap) +
          " block_cache=" + std::to_string(mode.block_cache) +
          " persistent_cache=" + std::to_string(mode.persistent_cache) +
          " immortal_factory=" + std::to_string(mode.immortal_factory) +
          " fill_cache=" + std::to_string(mode.fill_cache) +
          " max_open_files=" + std::to_string(mode.max_open_files));
      options.allow_mmap_reads = mode.mmap;
      options.max_open_files = mode.max_open_files;
      std::shared_ptr<Cache> block_cache =
          mode.block_cache ? NewLRUCache(32 << 20) : nullptr;
      table_options.no_block_cache = !mode.block_cache;
      table_options.block_cache = block_cache;
      table_options.persistent_cache =
          mode.persistent_cache
              ? std::make_shared<UncompressedPersistentCache>()
              : nullptr;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      if (mode.immortal_factory) {
        options.table_factory =
            std::make_shared<ImmortalTableFactory>(options.table_factory);
      }
      if (mode.read_only) {
        ASSERT_OK(ReadOnlyReopen(options));
      } else {
        Reopen(options);
      }
      counters.Reset();

      ReadOptions ro;
      ro.pin_data = true;
      ro.fill_cache = mode.fill_cache;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      std::vector<SavedEntry> saved;
      ASSERT_NO_FATAL_FAILURE(SeekAndScanAll(iter.get(), &rnd, true_data,
                                             mode.keys_pinned, &saved));
#ifndef NDEBUG
      ASSERT_EQ(0, counters.dropped_with_cleanups.load());
#endif
      if (!mode.fill_cache) {
        // Placeholder charges of dropped blocks were released; only the
        // blocks the live file iterators are on (one per level) may be
        // charged, far below what pinning every block visited would cost.
        ASSERT_NE(nullptr, block_cache);
        ASSERT_LT(block_cache->GetUsage(), 16u << 10);
      }
      ASSERT_NO_FATAL_FAILURE(VerifySaved(saved));
#ifndef NDEBUG
      if (mode.pins == kPinAll || mode.pins == kPinMost) {
        // Most seeks switch files at least once.
        ASSERT_GT(counters.pinned_file_iters.load(), kImmortalSeeks / 2);
        if (mode.pins == kPinAll) {
          ASSERT_EQ(0, counters.dropped_file_iters.load());
        } else {
          ASSERT_GT(counters.dropped_file_iters.load(), 0);
        }
      } else if (mode.pins == kPinSome) {
        // Owned blocks were delegated; iterators on an owned block were
        // pinned, the others dropped.
        ASSERT_GT(counters.delegated_blocks.load(), 0);
        ASSERT_GT(counters.pinned_file_iters.load(), 0);
        ASSERT_GT(counters.dropped_file_iters.load(), 0);
      } else {
        ASSERT_EQ(0, counters.pinned_file_iters.load());
        ASSERT_EQ(0, counters.delegated_blocks.load());
        ASSERT_GT(counters.dropped_file_iters.load(), kImmortalSeeks / 2);
      }
#endif
    }
  }
}

// The same column family with table options that changed between flushes:
// L3 is plain (dropped), L2 has a first-key index (pinned), L1 has delta
// encoding, a hash index and compression (its decompressed blocks are
// heap-owned and delegated; keys are not pinned). Each file iterator is
// judged by its own table's properties.
TEST_F(DBIteratorBaseTest, PinnedDataIteratorImmortalMmapMixedTableOptions) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires mmap reads");
    return;
  }
  PinCounters counters;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.no_block_cache = true;
  DestroyAndReopen(options);

  CompressionType l1_compression = kNoCompression;
  for (CompressionType type : GetSupportedCompressions()) {
    if (type != kNoCompression) {
      l1_compression = type;
      break;
    }
  }
  std::map<std::string, std::string> true_data;
  Random rnd(301);
  ASSERT_NO_FATAL_FAILURE(WriteInterleavedLevels(
      this, &rnd,
      [&](int level) {
        switch (level) {
          case 3:
            options.compression = kNoCompression;
            table_options.use_delta_encoding = false;
            break;
          case 2:
            table_options.index_type =
                BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
            break;
          case 1:
            options.compression = l1_compression;
            table_options.index_type =
                BlockBasedTableOptions::IndexType::kBinarySearch;
            table_options.use_delta_encoding = true;
            table_options.data_block_index_type =
                BlockBasedTableOptions::kDataBlockBinaryAndHash;
            break;
        }
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        Reopen(options);
      },
      &true_data));

  options.allow_mmap_reads = true;
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  counters.Reset();

  ReadOptions ro;
  ro.pin_data = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  std::vector<SavedEntry> saved;
  ASSERT_NO_FATAL_FAILURE(SeekAndScanAll(iter.get(), &rnd, true_data,
                                         /*expected_keys_pinned=*/nullptr,
                                         &saved));
#ifndef NDEBUG
  ASSERT_EQ(0, counters.dropped_with_cleanups.load());
  ASSERT_GT(counters.pinned_file_iters.load(), 0);
  ASSERT_GT(counters.dropped_file_iters.load(), 0);
  if (l1_compression != kNoCompression) {
    ASSERT_GT(counters.delegated_blocks.load(), 0);
  }
#endif
  ASSERT_NO_FATAL_FAILURE(VerifySaved(saved));
}

// A MultiScan iterator owns the ReadSet its blocks were read into, so it is
// pinned like before even over an immortal mmap table.
TEST_F(DBIteratorBaseTest, PinnedDataIteratorImmortalMmapMultiScan) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires mmap reads");
    return;
  }
  PinCounters counters;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  table_options.block_size = 1024;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  std::map<std::string, std::string> true_data;
  Random rnd(301);
  ASSERT_NO_FATAL_FAILURE(
      WriteInterleavedLevels(this, &rnd, [](int /*level*/) {}, &true_data));

  options.allow_mmap_reads = true;
  options.max_open_files = -1;
  options.statistics = CreateDBStatistics();
  ASSERT_OK(ReadOnlyReopen(options));
  counters.Reset();

  // Disjoint ranges, each spanning several files of every level; the second
  // one covers the range tombstone. MultiScanArgs keeps Slices of the bounds.
  const std::vector<std::pair<std::string, std::string>> kRanges = {
      {Key(10), Key(400)}, {Key(1450), Key(1700)}, {Key(2200), Key(2990)}};
  MultiScanArgs scan_opts(BytewiseComparator());
  for (const auto& range : kRanges) {
    scan_opts.insert(range.first, range.second);
  }
  ReadOptions ro;
  ro.pin_data = true;
  // A prepared iterator expects the upper bound to follow the ranges.
  Slice upper_bound;
  ro.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->Prepare(scan_opts);
  ASSERT_OK(iter->status());
  std::vector<SavedEntry> saved;
  for (const auto& range : kRanges) {
    const std::string& limit = range.second;
    upper_bound = limit;
    auto expected = true_data.lower_bound(range.first);
    for (iter->Seek(range.first); iter->Valid(); iter->Next(), ++expected) {
      ASSERT_LT(iter->key().compare(limit), 0);
      ASSERT_TRUE(expected != true_data.end());
      ASSERT_EQ(expected->first, iter->key().ToString());
      ASSERT_NO_FATAL_FAILURE(CheckAndSave(iter.get(), true_data,
                                           /*expected_keys_pinned=*/nullptr,
                                           /*forward=*/true, &saved));
    }
    ASSERT_OK(iter->status());
    ASSERT_TRUE(expected == true_data.end() || expected->first >= limit);
  }
  // LevelIterator prepares its file iterators as it opens them.
  ASSERT_GT(options.statistics->getTickerCount(MULTISCAN_PREPARE_CALLS), 0u);
  ASSERT_EQ(0u, options.statistics->getTickerCount(MULTISCAN_PREPARE_ERRORS));
#ifndef NDEBUG
  ASSERT_EQ(0, counters.dropped_with_cleanups.load());
  // Every file iterator the scan left was prepared and is pinned.
  ASSERT_GT(counters.pinned_file_iters.load(), 0);
  ASSERT_EQ(0, counters.dropped_file_iters.load());
#endif
  ASSERT_NO_FATAL_FAILURE(VerifySaved(saved));
}

// An immortal mmap table whose data-block keys are delta encoded: the values
// still point into the mapping (so the file iterators are dropped), but keys
// that are not at a restart point are reconstructed into the DBIter's buffer
// and are not pinned. Checks that dropping the iterators while pinning leaves
// both the saved values (pinned, in the mapping) and keys (copied out) valid.
TEST_F(DBIteratorBaseTest, PinnedDataIteratorImmortalMmapDeltaEncodedKeys) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires mmap reads");
    return;
  }
  PinCounters counters;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = true;
  table_options.index_type = BlockBasedTableOptions::IndexType::kBinarySearch;
  table_options.block_size = 1024;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  std::map<std::string, std::string> true_data;
  Random rnd(301);
  ASSERT_NO_FATAL_FAILURE(
      WriteInterleavedLevels(this, &rnd, [](int /*level*/) {}, &true_data));

  options.allow_mmap_reads = true;
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  counters.Reset();

  ReadOptions ro;
  ro.pin_data = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  std::vector<SavedEntry> saved;
  // is-key-pinned varies: restart-point keys are stored in full (pinned),
  // delta-encoded keys are not.
  ASSERT_NO_FATAL_FAILURE(SeekAndScanAll(iter.get(), &rnd, true_data,
                                         /*expected_keys_pinned=*/nullptr,
                                         &saved));
#ifndef NDEBUG
  ASSERT_EQ(0, counters.dropped_with_cleanups.load());
  ASSERT_GT(counters.dropped_file_iters.load(), 0);
#endif
  ASSERT_NO_FATAL_FAILURE(VerifySaved(saved));
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_P(DBIteratorTest, PinnedDataIteratorMergeOperator) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);

  std::string numbers[7];
  for (int val = 0; val <= 6; val++) {
    PutFixed64(numbers + val, val);
  }

  // +1 all keys in range [ 0 => 999]
  for (int i = 0; i < 1000; i++) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[1]));
  }

  // +2 all keys divisible by 2 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 2) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[2]));
  }

  // +3 all keys divisible by 5 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 5) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[3]));
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  std::vector<std::pair<Slice, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("0", prop_value);
    results.emplace_back(iter->key(), iter->value().ToString());
  }
  ASSERT_OK(iter->status());

  ASSERT_EQ(results.size(), 1000);
  for (size_t i = 0; i < results.size(); i++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, Key(static_cast<int>(i)));
    int expected_val = 1;
    if (i % 2 == 0) {
      expected_val += 2;
    }
    if (i % 5 == 0) {
      expected_val += 3;
    }
    ASSERT_EQ(kv.second, numbers[expected_val]);
  }

  delete iter;
}

TEST_P(DBIteratorTest, PinnedDataIteratorReadAfterUpdate) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.write_buffer_size = 100000;
  DestroyAndReopen(options);

  Random rnd(301);

  std::map<std::string, std::string> true_data;
  for (int i = 0; i < 1000; i++) {
    std::string k = rnd.RandomString(10);
    std::string v = rnd.RandomString(1000);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  // Delete 50% of the keys and update the other 50%
  for (auto& kv : true_data) {
    if (rnd.OneIn(2)) {
      ASSERT_OK(Delete(kv.first));
    } else {
      std::string new_val = rnd.RandomString(1000);
      ASSERT_OK(Put(kv.first, new_val));
    }
  }

  std::vector<std::pair<Slice, Slice>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    ASSERT_OK(
        iter->GetProperty("rocksdb.iterator.is-value-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value());
  }
  ASSERT_OK(iter->status());

  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }

  delete iter;
}

class SliceTransformLimitedDomainGeneric : public SliceTransform {
  const char* Name() const override {
    return "SliceTransformLimitedDomainGeneric";
  }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 1);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 1;
  }
};

TEST_P(DBIteratorTest, IterSeekForPrevCrossingFiles) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.disable_auto_compactions = true;
  // Enable prefix bloom for SST files
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "va1"));
  ASSERT_OK(Put("a2", "va2"));
  ASSERT_OK(Put("a3", "va3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b1", "vb1"));
  ASSERT_OK(Put("b2", "vb2"));
  ASSERT_OK(Put("b3", "vb3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b4", "vb4"));
  ASSERT_OK(Put("d1", "vd1"));
  ASSERT_OK(Put("d2", "vd2"));
  ASSERT_OK(Put("d4", "vd4"));
  ASSERT_OK(Flush());

  MoveFilesToLevel(1);
  {
    ReadOptions ro;
    Iterator* iter = NewIterator(ro);

    iter->SeekForPrev("a4");
    ASSERT_EQ(iter->key().ToString(), "a3");
    ASSERT_EQ(iter->value().ToString(), "va3");

    iter->SeekForPrev("c2");
    ASSERT_EQ(iter->key().ToString(), "b3");
    iter->SeekForPrev("d3");
    ASSERT_EQ(iter->key().ToString(), "d2");
    iter->SeekForPrev("b5");
    ASSERT_EQ(iter->key().ToString(), "b4");
    delete iter;
  }

  {
    ReadOptions ro;
    ro.prefix_same_as_start = true;
    Iterator* iter = NewIterator(ro);
    iter->SeekForPrev("c2");
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }
}

TEST_P(DBIteratorTest, IterSeekForPrevCrossingFilesCustomPrefixExtractor) {
  Options options = CurrentOptions();
  options.prefix_extractor =
      std::make_shared<SliceTransformLimitedDomainGeneric>();
  options.disable_auto_compactions = true;
  // Enable prefix bloom for SST files
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "va1"));
  ASSERT_OK(Put("a2", "va2"));
  ASSERT_OK(Put("a3", "va3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b1", "vb1"));
  ASSERT_OK(Put("b2", "vb2"));
  ASSERT_OK(Put("b3", "vb3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b4", "vb4"));
  ASSERT_OK(Put("d1", "vd1"));
  ASSERT_OK(Put("d2", "vd2"));
  ASSERT_OK(Put("d4", "vd4"));
  ASSERT_OK(Flush());

  MoveFilesToLevel(1);
  {
    ReadOptions ro;
    Iterator* iter = NewIterator(ro);

    iter->SeekForPrev("a4");
    ASSERT_EQ(iter->key().ToString(), "a3");
    ASSERT_EQ(iter->value().ToString(), "va3");

    iter->SeekForPrev("c2");
    ASSERT_EQ(iter->key().ToString(), "b3");
    iter->SeekForPrev("d3");
    ASSERT_EQ(iter->key().ToString(), "d2");
    iter->SeekForPrev("b5");
    ASSERT_EQ(iter->key().ToString(), "b4");
    delete iter;
  }

  {
    ReadOptions ro;
    ro.prefix_same_as_start = true;
    Iterator* iter = NewIterator(ro);
    iter->SeekForPrev("c2");
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }
}

TEST_P(DBIteratorTest, IterPrevKeyCrossingBlocks) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;  // every block will contain one entry
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.disable_auto_compactions = true;
  options.max_sequential_skip_in_iterations = 8;

  DestroyAndReopen(options);

  // Putting such deletes will force DBIter::Prev() to fallback to a Seek
  for (int file_num = 0; file_num < 10; file_num++) {
    ASSERT_OK(Delete("key4"));
    ASSERT_OK(Flush());
  }

  // First File containing 5 blocks of puts
  ASSERT_OK(Put("key1", "val1.0"));
  ASSERT_OK(Put("key2", "val2.0"));
  ASSERT_OK(Put("key3", "val3.0"));
  ASSERT_OK(Put("key4", "val4.0"));
  ASSERT_OK(Put("key5", "val5.0"));
  ASSERT_OK(Flush());

  // Second file containing 9 blocks of merge operands
  ASSERT_OK(db_->Merge(WriteOptions(), "key1", "val1.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key1", "val1.2"));

  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.3"));

  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.3"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.4"));
  ASSERT_OK(Flush());

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);

    iter->SeekToLast();
    ASSERT_EQ(iter->key().ToString(), "key5");
    ASSERT_EQ(iter->value().ToString(), "val5.0");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key4");
    ASSERT_EQ(iter->value().ToString(), "val4.0");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key3");
    ASSERT_EQ(iter->value().ToString(), "val3.0,val3.1,val3.2,val3.3,val3.4");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key2");
    ASSERT_EQ(iter->value().ToString(), "val2.0,val2.1,val2.2,val2.3");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key1");
    ASSERT_EQ(iter->value().ToString(), "val1.0,val1.1,val1.2");

    delete iter;
  }
}

TEST_P(DBIteratorTest, IterPrevKeyCrossingBlocksRandomized) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.max_sequential_skip_in_iterations = 8;
  DestroyAndReopen(options);

  const int kNumKeys = 500;
  // Small number of merge operands to make sure that DBIter::Prev() don't
  // fall back to Seek()
  const int kNumMergeOperands = 3;
  // Use value size that will make sure that every block contain 1 key
  const int kValSize =
      static_cast<int>(BlockBasedTableOptions().block_size) * 4;
  // Percentage of keys that wont get merge operations
  const int kNoMergeOpPercentage = 20;
  // Percentage of keys that will be deleted
  const int kDeletePercentage = 10;

  // For half of the key range we will write multiple deletes first to
  // force DBIter::Prev() to fall back to Seek()
  for (int file_num = 0; file_num < 10; file_num++) {
    for (int i = 0; i < kNumKeys; i += 2) {
      ASSERT_OK(Delete(Key(i)));
    }
    ASSERT_OK(Flush());
  }

  Random rnd(301);
  std::map<std::string, std::string> true_data;
  std::string gen_key;
  std::string gen_val;

  for (int i = 0; i < kNumKeys; i++) {
    gen_key = Key(i);
    gen_val = rnd.RandomString(kValSize);

    ASSERT_OK(Put(gen_key, gen_val));
    true_data[gen_key] = gen_val;
  }
  ASSERT_OK(Flush());

  // Separate values and merge operands in different file so that we
  // make sure that we don't merge them while flushing but actually
  // merge them in the read path
  for (int i = 0; i < kNumKeys; i++) {
    if (rnd.PercentTrue(kNoMergeOpPercentage)) {
      // Dont give merge operations for some keys
      continue;
    }

    for (int j = 0; j < kNumMergeOperands; j++) {
      gen_key = Key(i);
      gen_val = rnd.RandomString(kValSize);

      ASSERT_OK(db_->Merge(WriteOptions(), gen_key, gen_val));
      true_data[gen_key] += "," + gen_val;
    }
  }
  ASSERT_OK(Flush());

  for (int i = 0; i < kNumKeys; i++) {
    if (rnd.PercentTrue(kDeletePercentage)) {
      gen_key = Key(i);

      ASSERT_OK(Delete(gen_key));
      true_data.erase(gen_key);
    }
  }
  ASSERT_OK(Flush());

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);
    auto data_iter = true_data.rbegin();

    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);
      data_iter++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(data_iter, true_data.rend());

    delete iter;
  }

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);
    auto data_iter = true_data.rbegin();

    int entries_right = 0;
    std::string seek_key;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      // Verify key/value of current position
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);

      bool restore_position_with_seek = rnd.Uniform(2);
      if (restore_position_with_seek) {
        seek_key = iter->key().ToString();
      }

      // Do some Next() operations the restore the iterator to orignal position
      int next_count =
          entries_right > 0 ? rnd.Uniform(std::min(entries_right, 10)) : 0;
      for (int i = 0; i < next_count; i++) {
        iter->Next();
        data_iter--;

        ASSERT_EQ(iter->key().ToString(), data_iter->first);
        ASSERT_EQ(iter->value().ToString(), data_iter->second);
      }

      if (restore_position_with_seek) {
        // Restore orignal position using Seek()
        iter->Seek(seek_key);
        for (int i = 0; i < next_count; i++) {
          data_iter++;
        }

        ASSERT_EQ(iter->key().ToString(), data_iter->first);
        ASSERT_EQ(iter->value().ToString(), data_iter->second);
      } else {
        // Restore original position using Prev()
        for (int i = 0; i < next_count; i++) {
          iter->Prev();
          data_iter++;

          ASSERT_EQ(iter->key().ToString(), data_iter->first);
          ASSERT_EQ(iter->value().ToString(), data_iter->second);
        }
      }

      entries_right++;
      data_iter++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(data_iter, true_data.rend());

    delete iter;
  }
}

TEST_P(DBIteratorTest, IteratorWithLocalStatistics) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 1000; i++) {
    // Key 10 bytes / Value 10 bytes
    ASSERT_OK(Put(rnd.RandomString(10), rnd.RandomString(10)));
  }

  std::atomic<uint64_t> total_next(0);
  std::atomic<uint64_t> total_next_found(0);
  std::atomic<uint64_t> total_prev(0);
  std::atomic<uint64_t> total_prev_found(0);
  std::atomic<uint64_t> total_bytes(0);

  std::vector<port::Thread> threads;
  std::function<void()> reader_func_next = [&]() {
    SetPerfLevel(kEnableCount);
    get_perf_context()->Reset();
    Iterator* iter = NewIterator(ReadOptions());

    iter->SeekToFirst();
    // Seek will bump ITER_BYTES_READ
    uint64_t bytes = 0;
    bytes += iter->key().size();
    bytes += iter->value().size();
    while (true) {
      iter->Next();
      total_next++;

      if (!iter->Valid()) {
        EXPECT_OK(iter->status());
        break;
      }
      total_next_found++;
      bytes += iter->key().size();
      bytes += iter->value().size();
    }

    delete iter;
    ASSERT_EQ(bytes, get_perf_context()->iter_read_bytes);
    SetPerfLevel(kDisable);
    total_bytes += bytes;
  };

  std::function<void()> reader_func_prev = [&]() {
    SetPerfLevel(kEnableCount);
    Iterator* iter = NewIterator(ReadOptions());

    iter->SeekToLast();
    // Seek will bump ITER_BYTES_READ
    uint64_t bytes = 0;
    bytes += iter->key().size();
    bytes += iter->value().size();
    while (true) {
      iter->Prev();
      total_prev++;

      if (!iter->Valid()) {
        EXPECT_OK(iter->status());
        break;
      }
      total_prev_found++;
      bytes += iter->key().size();
      bytes += iter->value().size();
    }

    delete iter;
    ASSERT_EQ(bytes, get_perf_context()->iter_read_bytes);
    SetPerfLevel(kDisable);
    total_bytes += bytes;
  };

  for (int i = 0; i < 10; i++) {
    threads.emplace_back(reader_func_next);
  }
  for (int i = 0; i < 15; i++) {
    threads.emplace_back(reader_func_prev);
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT), (uint64_t)total_next);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT_FOUND),
            (uint64_t)total_next_found);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV), (uint64_t)total_prev);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV_FOUND),
            (uint64_t)total_prev_found);
  ASSERT_EQ(TestGetTickerCount(options, ITER_BYTES_READ),
            (uint64_t)total_bytes);
}

TEST_P(DBIteratorTest, ReadAhead) {
  Options options;
  env_->count_random_reads_ = true;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 4 << 20;
  // Pin compression so the readahead byte thresholds below don't depend on the
  // default compression type. kNoCompression keeps the SST files large enough
  // to exercise readahead and is always available.
  options.compression = kNoCompression;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  std::string value(1024, 'a');
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ("1,1,1", FilesPerLevel());

  env_->random_read_bytes_counter_ = 0;
  options.statistics->setTickerCount(NO_FILE_OPENS, 0);
  ReadOptions read_options;
  auto* iter = NewIterator(read_options);
  iter->SeekToFirst();
  int64_t num_file_opens = TestGetTickerCount(options, NO_FILE_OPENS);
  size_t bytes_read = env_->random_read_bytes_counter_;
  delete iter;

  env_->random_read_bytes_counter_ = 0;
  options.statistics->setTickerCount(NO_FILE_OPENS, 0);
  read_options.readahead_size = 1024 * 10;
  iter = NewIterator(read_options);
  iter->SeekToFirst();
  int64_t num_file_opens_readahead = TestGetTickerCount(options, NO_FILE_OPENS);
  size_t bytes_read_readahead = env_->random_read_bytes_counter_;
  delete iter;
  ASSERT_EQ(num_file_opens, num_file_opens_readahead);
  ASSERT_GT(bytes_read_readahead, bytes_read);
  ASSERT_GT(bytes_read_readahead, read_options.readahead_size * 3);

  // Verify correctness.
  iter = NewIterator(read_options);
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(value, iter->value());
    count++;
  }
  ASSERT_EQ(100, count);
  for (int i = 0; i < 100; i++) {
    iter->Seek(Key(i));
    ASSERT_EQ(value, iter->value());
  }
  delete iter;
}

// Insert a key, create a snapshot iterator, overwrite key lots of times,
// seek to a smaller key. Expect DBIter to fall back to a seek instead of
// going through all the overwrites linearly.
TEST_P(DBIteratorTest, DBIteratorSkipRecentDuplicatesTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_sequential_skip_in_iterations = 3;
  options.prefix_extractor = nullptr;
  options.write_buffer_size = 1 << 27;  // big enough to avoid flush
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  // Insert.
  ASSERT_OK(Put("b", "0"));

  // Create iterator.
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(NewIterator(ro));

  // Insert a lot.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("b", std::to_string(i + 1).c_str()));
  }

  // Check that memtable wasn't flushed.
  std::string val;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &val));
  EXPECT_EQ("0", val);

  // Seek iterator to a smaller key.
  get_perf_context()->Reset();
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("b", iter->key().ToString());
  EXPECT_EQ("0", iter->value().ToString());

  // Check that the seek didn't do too much work.
  // Checks are not tight, just make sure that everything is well below 100.
  EXPECT_LT(get_perf_context()->internal_key_skipped_count, 4);
  EXPECT_LT(get_perf_context()->internal_recent_skipped_count, 8);
  EXPECT_LT(get_perf_context()->seek_on_memtable_count, 10);
  EXPECT_LT(get_perf_context()->next_on_memtable_count, 10);
  EXPECT_LT(get_perf_context()->prev_on_memtable_count, 10);

  // Check that iterator did something like what we expect.
  EXPECT_EQ(get_perf_context()->internal_delete_skipped_count, 0);
  EXPECT_EQ(get_perf_context()->internal_merge_count, 0);
  EXPECT_GE(get_perf_context()->internal_recent_skipped_count, 2);
  EXPECT_GE(get_perf_context()->seek_on_memtable_count, 2);
  EXPECT_EQ(1,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
}

TEST_P(DBIteratorTest, Refresh) {
  ASSERT_OK(Put("x", "y"));

  std::unique_ptr<Iterator> iter(NewIterator(ReadOptions()));
  ASSERT_OK(iter->status());
  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(Put("c", "d"));

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(iter->status());
  ASSERT_OK(iter->Refresh());

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  EXPECT_OK(dbfull()->Flush(FlushOptions()));

  ASSERT_OK(Put("m", "n"));

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(iter->status());
  ASSERT_OK(iter->Refresh());

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("m")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  iter.reset();
}

TEST_P(DBIteratorTest, RefreshWithSnapshot) {
  // L1 file, uses LevelIterator internally
  ASSERT_OK(Put(Key(0), "val0"));
  ASSERT_OK(Put(Key(5), "val5"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  // L0 file, uses table iterator internally
  ASSERT_OK(Put(Key(1), "val1"));
  ASSERT_OK(Put(Key(4), "val4"));
  ASSERT_OK(Flush());

  // Memtable
  ASSERT_OK(Put(Key(2), "val2"));
  ASSERT_OK(Put(Key(3), "val3"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(Key(2), "new val"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(7)));
  const Snapshot* snapshot2 = db_->GetSnapshot();

  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  ReadOptions options;
  options.snapshot = snapshot;
  Iterator* iter = NewIterator(options);
  ASSERT_OK(Put(Key(6), "val6"));
  ASSERT_OK(iter->status());

  auto verify_iter = [&](int start, int end, bool new_key2 = false) {
    for (int i = start; i < end; ++i) {
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), Key(i));
      if (i == 2 && new_key2) {
        ASSERT_EQ(iter->value(), "new val");
      } else {
        ASSERT_EQ(iter->value(), "val" + std::to_string(i));
      }
      iter->Next();
    }
  };

  for (int j = 0; j < 2; j++) {
    iter->Seek(Key(1));
    verify_iter(1, 3);
    // Refresh to same snapshot
    ASSERT_OK(iter->Refresh(snapshot));
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->Seek(Key(3));
    verify_iter(3, 6);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Refresh to a newer snapshot
    ASSERT_OK(iter->Refresh(snapshot2));
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->SeekToFirst();
    verify_iter(0, 4, /*new_key2=*/true);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Refresh to an older snapshot
    ASSERT_OK(iter->Refresh(snapshot));
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->Seek(Key(3));
    verify_iter(3, 6);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Refresh to no snapshot
    ASSERT_OK(iter->Refresh());
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());
    iter->Seek(Key(2));
    verify_iter(2, 4, /*new_key2=*/true);
    verify_iter(6, 7);
    ASSERT_TRUE(!iter->Valid() && iter->status().ok());

    // Change LSM shape, new SuperVersion is created.
    ASSERT_OK(Flush());

    // Refresh back to original snapshot
    ASSERT_OK(iter->Refresh(snapshot));
  }

  delete iter;
  db_->ReleaseSnapshot(snapshot);
  db_->ReleaseSnapshot(snapshot2);
  ASSERT_OK(db_->Close());
}

TEST_P(DBIteratorTest, AutoRefreshIterator) {
  constexpr int kNumKeys = 1000;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (const DBIter::Direction direction :
       {DBIter::kForward, DBIter::kReverse}) {
    for (const bool auto_refresh_enabled : {false, true}) {
      for (const bool explicit_snapshot : {false, true}) {
        DestroyAndReopen(options);
        // Multi dimensional iterator:
        //
        // L0 (level iterator): [key000000]
        // L1 (table iterator): [key000001]
        // Memtable           : [key000000, key000999]
        for (int i = 0; i < kNumKeys + 2; i++) {
          ASSERT_OK(Put(Key(i % kNumKeys), "val" + std::to_string(i)));
          if (i <= 1) {
            ASSERT_OK(Flush());
          }
          if (i == 0) {
            MoveFilesToLevel(1);
          }
        }

        ReadOptions read_options;
        std::unique_ptr<ManagedSnapshot> snapshot = nullptr;
        if (explicit_snapshot) {
          snapshot = std::make_unique<ManagedSnapshot>(db_.get());
        }
        read_options.snapshot =
            explicit_snapshot ? snapshot->snapshot() : nullptr;
        read_options.auto_refresh_iterator_with_snapshot = auto_refresh_enabled;
        std::unique_ptr<Iterator> iter(NewIterator(read_options));

        int trigger_compact_on_it = kNumKeys / 2;

        // This update should NOT be visible from the iterator.
        ASSERT_OK(Put(Key(trigger_compact_on_it + 1), "new val"));

        ASSERT_EQ(1, NumTableFilesAtLevel(1));
        ASSERT_EQ(1, NumTableFilesAtLevel(0));

        uint64_t all_memtables_size_before_refresh;
        uint64_t all_memtables_size_after_refresh;

        std::string prop_value;
        ASSERT_OK(iter->GetProperty("rocksdb.iterator.super-version-number",
                                    &prop_value));
        int superversion_number = std::stoi(prop_value);

        std::vector<LiveFileMetaData> old_files;
        db_->GetLiveFilesMetaData(&old_files);

        int expected_next_key_int;
        if (direction == DBIter::kForward) {
          expected_next_key_int = 0;
          iter->SeekToFirst();
        } else {  // DBIter::kReverse
          expected_next_key_int = kNumKeys - 1;
          iter->SeekToLast();
        }

        int it_num = 0;
        std::unordered_map<std::string, std::string> kvs;
        while (iter->Valid()) {
          ASSERT_OK(iter->status());
          it_num++;
          if (it_num == trigger_compact_on_it) {
            // Bump the superversion by manually scheduling flush + compaction.
            ASSERT_OK(Flush());
            ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr,
                                             nullptr));
            ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

            // For accuracy, capture the memtables size right before consecutive
            // iterator call to Next() will update its' stale superversion ref.
            dbfull()->GetIntProperty("rocksdb.size-all-mem-tables",
                                     &all_memtables_size_before_refresh);
          }

          if (it_num == trigger_compact_on_it + 1) {
            dbfull()->GetIntProperty("rocksdb.size-all-mem-tables",
                                     &all_memtables_size_after_refresh);
            ASSERT_OK(iter->GetProperty("rocksdb.iterator.super-version-number",
                                        &prop_value));
            uint64_t new_superversion_number = std::stoi(prop_value);
            Status expected_status_for_preexisting_files;
            if (auto_refresh_enabled && explicit_snapshot) {
              // Iterator is expected to detect its' superversion staleness.
              ASSERT_LT(superversion_number, new_superversion_number);
              // ... and since our iterator was the only reference to that very
              // superversion, we expect most of the active memory to be
              // returned upon automatical iterator refresh.
              ASSERT_GT(all_memtables_size_before_refresh,
                        all_memtables_size_after_refresh);
              expected_status_for_preexisting_files = Status::NotFound();
            } else {
              ASSERT_EQ(superversion_number, new_superversion_number);
              ASSERT_EQ(all_memtables_size_after_refresh,
                        all_memtables_size_before_refresh);
              expected_status_for_preexisting_files = Status::OK();
            }

            for (const auto& file : old_files) {
              ASSERT_EQ(env_->FileExists(file.db_path + "/" + file.name),
                        expected_status_for_preexisting_files);
            }
          }

          // Ensure we're visiting the keys in desired order and at most once!
          ASSERT_EQ(IdFromKey(iter->key().ToString()), expected_next_key_int);
          kvs[iter->key().ToString()] = iter->value().ToString();

          if (direction == DBIter::kForward) {
            iter->Next();
            expected_next_key_int++;
          } else {
            iter->Prev();
            expected_next_key_int--;
          }
        }
        ASSERT_OK(iter->status());

        // Data validation.
        ASSERT_EQ(kvs.size(), kNumKeys);
        for (int i = 0; i < kNumKeys; i++) {
          auto kv = kvs.find(Key(i));
          ASSERT_TRUE(kv != kvs.end());
          int val = i;
          if (i <= 1) {
            val += kNumKeys;
          }
          ASSERT_EQ(kv->second, "val" + std::to_string(val));
        }
      }
    }
  }
}

TEST_P(DBIteratorTest, CreationFailure) {
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::NewInternalIterator:StatusCallback", [](void* arg) {
        *(static_cast<Status*>(arg)) = Status::Corruption("test status");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Iterator* iter = NewIterator(ReadOptions());
  ASSERT_FALSE(iter->Valid());
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
}

TEST_P(DBIteratorTest, UpperBoundWithChangeDirection) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  DestroyAndReopen(options);

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("y", "1"));
  ASSERT_OK(Put("y1", "1"));
  ASSERT_OK(Put("y2", "1"));
  ASSERT_OK(Put("y3", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Put("bar", "1"));
  ASSERT_OK(Put("foo", "1"));

  std::string upper_bound = "x";
  Slice ub_slice(upper_bound);
  ReadOptions ro;
  ro.iterate_upper_bound = &ub_slice;
  ro.max_skippable_internal_keys = 1000;

  Iterator* iter = NewIterator(ro);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bar", iter->key().ToString());

  delete iter;
}

TEST_P(DBIteratorTest, TableFilter) {
  ASSERT_OK(Put("a", "1"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Put("c", "3"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("d", "4"));
  ASSERT_OK(Put("e", "5"));
  ASSERT_OK(Put("f", "6"));
  EXPECT_OK(dbfull()->Flush(FlushOptions()));

  // Ensure the table_filter callback is called once for each table.
  {
    std::set<uint64_t> unseen{1, 2, 3};
    ReadOptions opts;
    std::function<bool(const TableProperties&)> filter =
        [&](const TableProperties& props) {
          auto it = unseen.find(props.num_entries);
          if (it == unseen.end()) {
            ADD_FAILURE() << "saw table properties with an unexpected "
                          << props.num_entries << " entries";
          } else {
            unseen.erase(it);
          }
          return true;
        };
    opts.table_filter = &filter;
    auto iter = NewIterator(opts);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->1");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->2");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->3");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->4");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->5");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "f->6");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(unseen.empty());
    delete iter;
  }

  // Ensure returning false in the table_filter hides the keys from that table
  // during iteration.
  {
    ReadOptions opts;
    std::function<bool(const TableProperties&)> filter =
        [](const TableProperties& props) { return props.num_entries != 2; };
    opts.table_filter = &filter;
    auto iter = NewIterator(opts);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->1");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->4");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->5");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "f->6");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }
}

TEST_P(DBIteratorTest, UpperBoundWithPrevReseek) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  DestroyAndReopen(options);

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("y", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Put("bar", "1"));
  ASSERT_OK(Put("foo", "1"));
  ASSERT_OK(Put("foo", "2"));

  ASSERT_OK(Put("foo", "3"));
  ASSERT_OK(Put("foo", "4"));
  ASSERT_OK(Put("foo", "5"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("foo", "6"));

  std::string upper_bound = "x";
  Slice ub_slice(upper_bound);
  ReadOptions ro;
  ro.snapshot = snapshot;
  ro.iterate_upper_bound = &ub_slice;

  Iterator* iter = NewIterator(ro);
  iter->SeekForPrev("goo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  iter->Prev();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key().ToString());

  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_P(DBIteratorTest, SkipStatistics) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  int skip_count = 0;

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("b", "1"));
  ASSERT_OK(Put("c", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("d", "1"));
  ASSERT_OK(Put("e", "1"));
  ASSERT_OK(Put("f", "1"));
  ASSERT_OK(Put("a", "2"));
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Delete("e"));
  ASSERT_OK(Delete("f"));

  Iterator* iter = NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 3);
  delete iter;
  skip_count += 8;  // 3 deletes + 3 original keys + 2 lower in sequence
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  iter = NewIterator(ReadOptions());
  count = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 3);
  delete iter;
  skip_count += 8;  // Same as above, but in reverse order
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  ASSERT_OK(Put("aa", "1"));
  ASSERT_OK(Put("ab", "1"));
  ASSERT_OK(Put("ac", "1"));
  ASSERT_OK(Put("ad", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("ab"));
  ASSERT_OK(Delete("ac"));
  ASSERT_OK(Delete("ad"));

  ReadOptions ro;
  Slice prefix("b");
  ro.iterate_upper_bound = &prefix;

  iter = NewIterator(ro);
  count = 0;
  for (iter->Seek("aa"); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 1);
  delete iter;
  skip_count += 6;  // 3 deletes + 3 original keys
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  iter = NewIterator(ro);
  count = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 2);
  delete iter;
  // 3 deletes + 3 original keys + lower sequence of "a"
  skip_count += 7;
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));
}

TEST_P(DBIteratorTest, SeekAfterHittingManyInternalKeys) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ReadOptions ropts;
  ropts.max_skippable_internal_keys = 2;

  ASSERT_OK(Put("1", "val_1"));
  // Add more tombstones than max_skippable_internal_keys so that Next() fails.
  ASSERT_OK(Delete("2"));
  ASSERT_OK(Delete("3"));
  ASSERT_OK(Delete("4"));
  ASSERT_OK(Delete("5"));
  ASSERT_OK(Put("6", "val_6"));

  std::unique_ptr<Iterator> iter(NewIterator(ropts));
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "1");
  ASSERT_EQ(iter->value().ToString(), "val_1");

  // This should fail as incomplete due to too many non-visible internal keys on
  // the way to the next valid user key.
  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_TRUE(iter->status().IsIncomplete());

  // Get the internal key at which Next() failed.
  std::string prop_value;
  ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
  ASSERT_EQ("4", prop_value);

  // Create a new iterator to seek to the internal key.
  std::unique_ptr<Iterator> iter2(NewIterator(ropts));
  iter2->Seek(prop_value);
  ASSERT_TRUE(iter2->Valid());
  ASSERT_OK(iter2->status());

  ASSERT_EQ(iter2->key().ToString(), "6");
  ASSERT_EQ(iter2->value().ToString(), "val_6");
}

// Reproduces a former bug where iterator would skip some records when DBIter
// re-seeks subiterator with Incomplete status.
TEST_P(DBIteratorTest, NonBlockingIterationBugRepro) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  // Make sure the sst file has more than one block.
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Two records in sst file, each in its own block.
  ASSERT_OK(Put("b", ""));
  ASSERT_OK(Put("d", ""));
  ASSERT_OK(Flush());

  // Create a nonblocking iterator before writing to memtable.
  ReadOptions ropt;
  ropt.read_tier = kBlockCacheTier;
  std::unique_ptr<Iterator> iter(NewIterator(ropt));

  // Overwrite a key in memtable many times to hit
  // max_sequential_skip_in_iterations (which is 8 by default).
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Put("c", ""));
  }

  // Load the second block in sst file into the block cache.
  {
    std::unique_ptr<Iterator> iter2(NewIterator(ReadOptions()));
    iter2->Seek("d");
  }

  // Finally seek the nonblocking iterator.
  iter->Seek("a");
  // With the bug, the status used to be OK, and the iterator used to point to
  // "d".
  EXPECT_TRUE(iter->status().IsIncomplete());
}

TEST_P(DBIteratorTest, SeekBackwardAfterOutOfUpperBound) {
  ASSERT_OK(Put("a", ""));
  ASSERT_OK(Put("b", ""));
  ASSERT_OK(Flush());

  ReadOptions ropt;
  Slice ub = "b";
  ropt.iterate_upper_bound = &ub;

  std::unique_ptr<Iterator> it(dbfull()->NewIterator(ropt));
  it->SeekForPrev("a");
  ASSERT_TRUE(it->Valid());
  ASSERT_OK(it->status());
  ASSERT_EQ("a", it->key().ToString());
  it->Next();
  ASSERT_FALSE(it->Valid());
  ASSERT_OK(it->status());
  it->SeekForPrev("a");
  ASSERT_OK(it->status());

  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("a", it->key().ToString());
}

TEST_P(DBIteratorTest, AvoidReseekLevelIterator) {
  Options options = CurrentOptions();
  options.compression = CompressionType::kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 800;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  Random rnd(301);
  std::string random_str = rnd.RandomString(180);

  ASSERT_OK(Put("1", random_str));
  ASSERT_OK(Put("2", random_str));
  ASSERT_OK(Put("3", random_str));
  ASSERT_OK(Put("4", random_str));
  // A new block
  ASSERT_OK(Put("5", random_str));
  ASSERT_OK(Put("6", random_str));
  ASSERT_OK(Put("7", random_str));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("8", random_str));
  ASSERT_OK(Put("9", random_str));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  int num_find_file_in_level = 0;
  int num_idx_blk_seek = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "LevelIterator::Seek:BeforeFindFile",
      [&](void* /*arg*/) { num_find_file_in_level++; });
  SyncPoint::GetInstance()->SetCallBack(
      "IndexBlockIter::Seek:0", [&](void* /*arg*/) { num_idx_blk_seek++; });
  SyncPoint::GetInstance()->EnableProcessing();

  {
    std::unique_ptr<Iterator> iter(NewIterator(ReadOptions()));
    iter->Seek("1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Seek("2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Seek("3");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(1, num_idx_blk_seek);

    iter->Seek("5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(2, num_idx_blk_seek);

    iter->Seek("6");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(2, num_idx_blk_seek);

    iter->Seek("7");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1, num_find_file_in_level);
    ASSERT_EQ(3, num_idx_blk_seek);

    iter->Seek("8");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(2, num_find_file_in_level);
    // Still re-seek because "8" is the boundary key, which has
    // the same user key as the seek key.
    ASSERT_EQ(4, num_idx_blk_seek);

    iter->Seek("5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(3, num_find_file_in_level);
    ASSERT_EQ(5, num_idx_blk_seek);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(3, num_find_file_in_level);
    ASSERT_EQ(5, num_idx_blk_seek);

    // Seek backward never triggers the index block seek to be skipped
    iter->Seek("5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(3, num_find_file_in_level);
    ASSERT_EQ(6, num_idx_blk_seek);
  }

  SyncPoint::GetInstance()->DisableProcessing();
}

// MyRocks may change iterate bounds before seek. Simply test to make sure such
// usage doesn't break iterator.
TEST_P(DBIteratorTest, IterateBoundChangedBeforeSeek) {
  Options options = CurrentOptions();
  options.compression = CompressionType::kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 100;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::string value(50, 'v');
  Reopen(options);
  ASSERT_OK(Put("aaa", value));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("bbb", "v"));
  ASSERT_OK(Put("ccc", "v"));
  ASSERT_OK(Put("ddd", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("eee", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::string ub1 = "e";
  std::string ub2 = "c";
  Slice ub(ub1);
  ReadOptions read_opts1;
  read_opts1.iterate_upper_bound = &ub;
  Iterator* iter = NewIterator(read_opts1);
  // Seek and iterate accross block boundary.
  iter->Seek("b");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bbb", iter->key());
  ub = Slice(ub2);
  iter->Seek("b");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bbb", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  delete iter;

  std::string lb1 = "a";
  std::string lb2 = "c";
  Slice lb(lb1);
  ReadOptions read_opts2;
  read_opts2.iterate_lower_bound = &lb;
  iter = NewIterator(read_opts2);
  iter->SeekForPrev("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("ccc", iter->key());
  lb = Slice(lb2);
  iter->SeekForPrev("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("ccc", iter->key());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  delete iter;
}

TEST_P(DBIteratorTest, IterateWithLowerBoundAcrossFileBoundary) {
  ASSERT_OK(Put("aaa", "v"));
  ASSERT_OK(Put("bbb", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("ccc", "v"));
  ASSERT_OK(Put("ddd", "v"));
  ASSERT_OK(Flush());
  // Move both files to bottom level.
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Slice lower_bound("b");
  ReadOptions read_opts;
  read_opts.iterate_lower_bound = &lower_bound;
  std::unique_ptr<Iterator> iter(NewIterator(read_opts));
  iter->SeekForPrev("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("ccc", iter->key());
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bbb", iter->key());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_P(DBIteratorTest, Blob) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.max_sequential_skip_in_iterations = 2;
  options.statistics = CreateDBStatistics();

  Reopen(options);

  // Note: we have 4 KVs (3 of which are hidden) for key "b" and
  // max_sequential_skip_in_iterations is set to 2. Thus, we need to do a reseek
  // anytime we move from "b" to "c" or vice versa.
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Flush());

  std::unique_ptr<Iterator> iter_guard(NewIterator(ReadOptions()));
  Iterator* const iter = iter_guard.get();

  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("a");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("ax");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "b->vb3");

  iter->SeekForPrev("d");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->SeekForPrev("c");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 2);
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->SeekForPrev("bx");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 3);
  ASSERT_EQ(IterStatus(iter), "b->vb3");

  iter->Seek("b");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 3);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->Seek("z");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 3);
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekForPrev("b");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 4);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
  iter->SeekForPrev("");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 4);
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  // Switch from reverse to forward
  iter->SeekToLast();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 4);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 5);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 5);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 6);
  ASSERT_EQ(IterStatus(iter), "b->vb3");

  // Switch from forward to reverse
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 6);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 6);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 7);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 8);
  ASSERT_EQ(IterStatus(iter), "b->vb3");
}

INSTANTIATE_TEST_CASE_P(DBIteratorTestInstance, DBIteratorTest,
                        testing::Values(true, false));

// Tests how DBIter work with ReadCallback
class DBIteratorWithReadCallbackTest : public DBIteratorTest {};

#ifdef OS_LINUX
TEST_F(DBIteratorWithReadCallbackTest,
       PrevReseekVisibilityScanChecksQueryAbort) {
  class TestReadCallback : public ReadCallback {
   public:
    explicit TestReadCallback(SequenceNumber max_visible_seq)
        : ReadCallback(max_visible_seq) {}

    bool IsVisibleFullCheck(SequenceNumber seq) override {
      return seq <= max_visible_seq_;
    }
  };

  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 2;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("b", "visible"));
  }
  ASSERT_OK(Put("c", "vc"));
  TestReadCallback callback(db_->GetLatestSequenceNumber());
  ASSERT_OK(Put("b", "hidden"));

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* cfd = cfh->cfd();
  DBImpl* db_impl = dbfull();
  SuperVersion* super_version = cfd->GetReferencedSuperVersion(db_impl);
  std::unique_ptr<Iterator> iter(
      db_impl->NewIteratorImpl(ReadOptions(), cfh, super_version,
                               db_->GetLatestSequenceNumber(), &callback));
  iter->Seek("c");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());

  {
    ScopedQueryAbort abort(/*abort_after_check_count=*/0);
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "b");
    // One check reaches "b", two trigger the reseek, one skips the invisible
    // value, and two finish positioning before "b".
    ASSERT_EQ(abort.check_count(), 6);
  }

  super_version = cfd->GetReferencedSuperVersion(db_impl);
  iter.reset(db_impl->NewIteratorImpl(ReadOptions(), cfh, super_version,
                                      db_->GetLatestSequenceNumber(),
                                      &callback));
  iter->Seek("c");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  {
    ScopedQueryAbort abort(/*abort_after_check_count=*/4);
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsAborted());
    ASSERT_EQ(abort.check_count(), 4);
  }
}
#endif  // OS_LINUX

TEST_F(DBIteratorWithReadCallbackTest, ReadCallback) {
  class TestReadCallback : public ReadCallback {
   public:
    explicit TestReadCallback(SequenceNumber _max_visible_seq)
        : ReadCallback(_max_visible_seq) {}

    bool IsVisibleFullCheck(SequenceNumber seq) override {
      return seq <= max_visible_seq_;
    }
  };

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("z", "vz"));
  SequenceNumber seq1 = db_->GetLatestSequenceNumber();
  TestReadCallback callback1(seq1);
  ASSERT_OK(Put("foo", "v4"));
  ASSERT_OK(Put("foo", "v5"));
  ASSERT_OK(Put("bar", "v7"));

  SequenceNumber seq2 = db_->GetLatestSequenceNumber();
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* cfd = cfh->cfd();
  // The iterator are suppose to see data before seq1.
  DBImpl* db_impl = dbfull();
  SuperVersion* super_version = cfd->GetReferencedSuperVersion(db_impl);
  Iterator* iter = db_impl->NewIteratorImpl(ReadOptions(), cfh, super_version,
                                            seq2, &callback1);

  // Seek
  // The latest value of "foo" before seq1 is "v3"
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key
  // "foo".
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());

  // Next
  // Seek to "a"
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("va", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key
  // "foo".
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());

  // Prev
  // Seek to "z"
  iter->Seek("z");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("vz", iter->value());
  // The previous key is "foo", which is visible to the iterator.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key "a".
  iter->Prev();  // skipping "bar"
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("a", iter->key());
  ASSERT_EQ("va", iter->value());

  // SeekForPrev
  // The previous key is "foo", which is visible to the iterator.
  iter->SeekForPrev("y");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key "a".
  iter->SeekForPrev("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("a", iter->key());
  ASSERT_EQ("va", iter->value());

  delete iter;

  // Prev beyond max_sequential_skip_in_iterations
  uint64_t num_versions =
      CurrentOptions().max_sequential_skip_in_iterations + 10;
  for (uint64_t i = 0; i < num_versions; i++) {
    ASSERT_OK(Put("bar", std::to_string(i)));
  }
  SequenceNumber seq3 = db_->GetLatestSequenceNumber();
  TestReadCallback callback2(seq3);
  ASSERT_OK(Put("bar", "v8"));
  SequenceNumber seq4 = db_->GetLatestSequenceNumber();

  // The iterator is suppose to see data before seq3.
  super_version = cfd->GetReferencedSuperVersion(db_impl);
  iter = db_impl->NewIteratorImpl(ReadOptions(), cfh, super_version, seq4,
                                  &callback2);
  // Seek to "z", which is visible.
  iter->Seek("z");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("vz", iter->value());
  // Previous key is "foo" and the last value "v5" is visible.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v5", iter->value());
  // Since the number of values of "bar" is more than
  // max_sequential_skip_in_iterations, Prev() will ultimately fallback to
  // seek in forward direction. Here we test the fallback seek is correct.
  // The last visible value should be (num_versions - 1), as "v8" is not
  // visible.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bar", iter->key());
  ASSERT_EQ(std::to_string(num_versions - 1), iter->value());

  delete iter;
}

TEST_F(DBIteratorTest, BackwardIterationOnInplaceUpdateMemtable) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.inplace_update_support = false;
  options.env = env_;
  DestroyAndReopen(options);
  constexpr int kNumKeys = 10;

  // Write kNumKeys to WAL.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "val"));
  }
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    int count = 0;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ++count;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kNumKeys, count);
  }

  // Reopen and rebuild the memtable from WAL.
  options.create_if_missing = false;
  options.avoid_flush_during_recovery = true;
  options.inplace_update_support = true;
  options.allow_concurrent_memtable_write = false;
  Reopen(options);
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToLast();
    // Backward iteration not supported due to inplace_update_support = true.
    ASSERT_TRUE(iter->status().IsNotSupported());
    ASSERT_FALSE(iter->Valid());
  }
}

TEST_F(DBIteratorTest, IteratorRefreshReturnSV) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  SyncPoint::GetInstance()->SetCallBack(
      "ArenaWrappedDBIter::Refresh:SV", [&](void*) {
        ASSERT_OK(db_->Put(WriteOptions(), "dummy", "new SV"));
        // This makes the local SV obselete.
        ASSERT_OK(Flush());
        SyncPoint::GetInstance()->DisableProcessing();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(iter->Refresh());
  iter.reset();
  // iter used to not cleanup SV, so the Close() below would hit an assertion
  // error.
  Close();
}

TEST_F(DBIteratorTest, ErrorWhenReadFile) {
  // This is to test a bug that is fixed in
  // https://github.com/meta/rocksdb/pull/11782.
  //
  // Ingest error when reading from a file, and
  // see if Iterator handles it correctly.
  Options opts = CurrentOptions();
  opts.num_levels = 7;
  opts.compression = kNoCompression;
  BlockBasedTableOptions bbto;
  // Always do I/O
  bbto.no_block_cache = true;
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(opts);

  // Set up LSM
  // L5: F1 [key0, key99], F2 [key100, key199]
  // L6:        F3 [key50, key149]
  Random rnd(301);
  const int kValLen = 100;
  for (int i = 50; i < 150; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValLen)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  std::vector<std::string> values;
  for (int i = 0; i < 100; ++i) {
    values.emplace_back(rnd.RandomString(kValLen));
    ASSERT_OK(Put(Key(i), values.back()));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);

  for (int i = 100; i < 200; ++i) {
    values.emplace_back(rnd.RandomString(kValLen));
    ASSERT_OK(Put(Key(i), values.back()));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);

  ASSERT_EQ(2, NumTableFilesAtLevel(5));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));

  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  // Get file names for F1, F2 and F3.
  // These are file names, not full paths.
  std::string f1;
  std::string f2;
  std::string f3;
  for (auto& file_meta : files) {
    if (file_meta.level == 6) {
      f3 = file_meta.name;
    } else {
      if (file_meta.smallestkey == Key(0)) {
        f1 = file_meta.name;
      } else {
        f2 = file_meta.name;
      }
    }
  }
  ASSERT_TRUE(!f1.empty());
  ASSERT_TRUE(!f2.empty());
  ASSERT_TRUE(!f3.empty());

  std::string error_file;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::Read::BeforeReturn",
      [&error_file](void* io_s_ptr) {
        auto p = static_cast<std::pair<std::string*, IOStatus*>*>(io_s_ptr);
        if (p->first->find(error_file) != std::string::npos) {
          *p->second = IOStatus::IOError();
          p->second->SetRetryable(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // Error reading F1
  error_file = f1;
  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  iter->SeekToFirst();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  // This does not require reading the first block.
  iter->Seek(Key(90));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[90]);
  // iter has ok status before this Seek.
  iter->Seek(Key(1));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());

  // Error reading F2
  error_file = f2;
  iter.reset(db_->NewIterator(ReadOptions()));
  iter->Seek(Key(99));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[99]);
  // Need to read from F2.
  iter->Next();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->Seek(Key(190));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[190]);
  // Seek for first key of F2.
  iter->Seek(Key(100));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->SeekToLast();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[199]);
  // SeekForPrev for first key of F2.
  iter->SeekForPrev(Key(100));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  // Does not read first block (offset 0).
  iter->SeekForPrev(Key(98));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[98]);

  // Error reading F3
  error_file = f3;
  iter.reset(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->Seek(Key(50));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter->SeekForPrev(Key(50));
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  // Does not read file 3
  iter->Seek(Key(150));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[150]);

  // Test when file read error occurs during Prev().
  // This requires returning an error when reading near the end of a file
  // instead of offset 0.
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::Read::AnyOffset", [&f1](void* pair_ptr) {
        auto p = static_cast<std::pair<std::string*, IOStatus*>*>(pair_ptr);
        if (p->first->find(f1) != std::string::npos) {
          *p->second = IOStatus::IOError();
          p->second->SetRetryable(true);
        }
      });
  iter->SeekForPrev(Key(101));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), values[101]);
  // DBIter will not stop at Key(100) since it needs
  // to make sure the key it returns has the max sequence number for Key(100).
  // So it will call MergingIterator::Prev() which will read F1.
  iter->Prev();
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  iter->Reset();
}

TEST_F(DBIteratorTest, IteratorsConsistentViewImplicitSnapshot) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2"}, options);

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "DBImpl::MultiCFSnapshot::BeforeCheckingSnapshot"}});

  bool flushed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (!flushed) {
          for (int i = 0; i < 3; ++i) {
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val_new"));
          }
          // After SV is obtained for the first CF, flush for the second CF
          ASSERT_OK(Flush(1));
          flushed = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions read_options;
  std::vector<Iterator*> iters;
  ASSERT_OK(db_->NewIterators(read_options, handles_, &iters));

  for (int i = 0; i < 3; ++i) {
    auto iter = iters[i];
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "cf" + std::to_string(i) + "_key->cf" +
                                    std::to_string(i) + "_val_new");
  }
  for (auto* iter : iters) {
    delete iter;
  }

  // Thread-local SVs are no longer obsolete nor in use
  for (int i = 0; i < 3; ++i) {
    auto* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(handles_[i])->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_F(DBIteratorTest, IteratorsConsistentViewExplicitSnapshot) {
  Options options = GetDefaultOptions();
  options.atomic_flush = true;
  CreateAndReopenWithCF({"cf_1", "cf_2"}, options);

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "DBImpl::MultiCFSnapshot::BeforeCheckingSnapshot"}});

  bool flushed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (!flushed) {
          for (int i = 0; i < 3; ++i) {
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val_new"));
          }
          // After SV is obtained for the first CF, do the atomic flush()
          ASSERT_OK(Flush());
          flushed = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Explicit snapshot wouldn't force reloading all svs. We should expect old
  // values
  const Snapshot* snapshot = db_->GetSnapshot();
  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::vector<Iterator*> iters;
  ASSERT_OK(db_->NewIterators(read_options, handles_, &iters));

  for (int i = 0; i < 3; ++i) {
    auto iter = iters[i];
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "cf" + std::to_string(i) + "_key->cf" +
                                    std::to_string(i) + "_val");
  }
  db_->ReleaseSnapshot(snapshot);
  for (auto* iter : iters) {
    delete iter;
  }

  // Thread-local SV for cf_0 is obsolete (atomic flush happened after the first
  // SV Ref)
  auto* cfd0 =
      static_cast_with_check<ColumnFamilyHandleImpl>(handles_[0])->cfd();
  ASSERT_EQ(cfd0->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
  ASSERT_NE(cfd0->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);

  // Rest are not InUse nor Obsolete
  for (int i = 1; i < 3; ++i) {
    auto* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(handles_[i])->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_F(DBIteratorTest, NewIteratorsWithPerColumnFamilyReadOptions) {
  Options options = GetDefaultOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"cf_1"}, options);

  ASSERT_OK(Put(0, "b1", "default_value"));
  ASSERT_OK(Put(1, "b1", "cf_1_value"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  ReadOptions prefix_read_options;
  prefix_read_options.total_order_seek = false;
  ReadOptions total_order_read_options;
  total_order_read_options.total_order_seek = true;
  std::vector<ReadOptions> read_options = {prefix_read_options,
                                           total_order_read_options};
  std::vector<Iterator*> iterators;
  ASSERT_OK(db_->NewIterators(read_options, handles_, &iterators));
  ASSERT_EQ(2U, iterators.size());

  iterators[0]->Seek("a9");
  ASSERT_FALSE(iterators[0]->Valid());
  ASSERT_OK(iterators[0]->status());
  iterators[1]->Seek("a9");
  ASSERT_TRUE(iterators[1]->Valid());
  ASSERT_EQ("b1", iterators[1]->key());
  ASSERT_OK(iterators[1]->status());

  for (auto* iterator : iterators) {
    delete iterator;
  }
}

TEST_F(DBIteratorTest, NewIteratorsPerColumnFamilyOptionsValidation) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1"}, options);

  std::vector<Iterator*> iterators;
  ASSERT_TRUE(
      db_->NewIterators(std::vector<ReadOptions>(1), handles_, &iterators)
          .IsInvalidArgument());

  std::vector<ReadOptions> read_options(2);
  read_options[1].tailing = true;
  ASSERT_TRUE(db_->NewIterators(read_options, handles_, &iterators)
                  .IsInvalidArgument());

  read_options[1].tailing = false;
  const Snapshot* first_snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(0, "key", "value"));
  const Snapshot* second_snapshot = db_->GetSnapshot();
  read_options[0].snapshot = second_snapshot;
  read_options[1].snapshot = second_snapshot;
  ASSERT_OK(db_->NewIterators(read_options, handles_, &iterators));
  for (auto* iterator : iterators) {
    delete iterator;
  }

  read_options[0].snapshot = first_snapshot;
  read_options[1].snapshot = second_snapshot;
  ASSERT_TRUE(db_->NewIterators(read_options, handles_, &iterators)
                  .IsInvalidArgument());
  db_->ReleaseSnapshot(first_snapshot);
  db_->ReleaseSnapshot(second_snapshot);
}

TEST_F(DBIteratorTest,
       ReadOnlyNewIteratorsAllowsTableFilterWithRangeConversion) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(ReadOnlyReopen(options));

  std::function<bool(const TableProperties&)> table_filter =
      [](const TableProperties&) { return true; };
  ReadOptions read_options;
  read_options.table_filter = &table_filter;
  const std::vector<ReadOptions> per_cf_read_options = {read_options};
  const std::vector<ColumnFamilyHandle*> column_families = {
      db_->DefaultColumnFamily()};
  for (bool use_per_cf_read_options : {false, true}) {
    std::vector<Iterator*> iterators;
    const Status s =
        use_per_cf_read_options
            ? db_->NewIterators(per_cf_read_options, column_families,
                                &iterators)
            : db_->NewIterators(read_options, column_families, &iterators);
    ASSERT_OK(s);
    ASSERT_EQ(1U, iterators.size());
    iterators[0]->SeekToFirst();
    ASSERT_OK(iterators[0]->status());
    ASSERT_TRUE(iterators[0]->Valid());
    ASSERT_EQ("key", iterators[0]->key());
    delete iterators[0];
  }
}

TEST_P(DBIteratorTest, MemtableOpsScanFlushTriggerWithSeek) {
  // Tests that option memtable_op_scan_flush_trigger works when the limit
  // is reached during a Seek() operation.
  const int kTrigger = 10;
  Random* r = Random::GetTLSInstance();

  for (int trigger : {kTrigger, kTrigger + 1}) {
    for (bool delete_only : {false, true}) {
      Options options;
      options.create_if_missing = true;
      options.memtable_op_scan_flush_trigger = trigger;
      options.level_compaction_dynamic_level_bytes = true;
      DestroyAndReopen(options);

      // Base data that will be covered by a consecutive sequence of tombstones.
      int kNumKeys = delete_only ? kTrigger : kTrigger / 2;
      for (int i = 0; i < kNumKeys; ++i) {
        ASSERT_OK(Put(Key(i), r->RandomString(100)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
      ASSERT_EQ(1, NumTableFilesAtLevel(6));

      if (delete_only) {
        for (int i = 0; i < kNumKeys; ++i) {
          ASSERT_OK(SingleDelete(Key(i)));
        }
      } else {
        for (int i = 0; i < kNumKeys; ++i) {
          ASSERT_OK(Put(Key(i), r->RandomString(100)));
        }
        for (int i = 0; i < kNumKeys; ++i) {
          ASSERT_OK(Delete(Key(i)));
        }
      }

      SetPerfLevel(PerfLevel::kEnableCount);
      get_perf_context()->Reset();
      ReadOptions ro;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

      // Seek to the first key, this will scan through all the tombstones and
      // hidden puts
      iter->Seek(Key(0));
      ASSERT_FALSE(
          iter->Valid());  // All keys are deleted, so iterator is not valid
      ASSERT_OK(iter->status());
      ASSERT_EQ(get_perf_context()->next_on_memtable_count, kTrigger);

      // Skipping kNumTrigger memtable entries in a single iterator operation
      // should mark the memtable for flush.
      //
      // At the end of a write, we check and update memtable to request a flush
      ASSERT_OK(Put(Key(11), "val"));
      // Before a write, we schedule memtables for flush if requested.
      ASSERT_OK(Put(Key(12), "val"));
      ASSERT_OK(db_->WaitForCompact({}));

      if (trigger <= kTrigger) {
        // Check if memtable was flushed due to scan trigger
        ASSERT_EQ(1, NumTableFilesAtLevel(0));
        uint64_t val = 0;
        ASSERT_TRUE(
            db_->GetIntProperty("rocksdb.num-deletes-active-mem-table", &val));
        ASSERT_EQ(0, val);
      } else {
        ASSERT_EQ(0, NumTableFilesAtLevel(0));
        uint64_t val = 0;
        ASSERT_TRUE(
            db_->GetIntProperty("rocksdb.num-deletes-active-mem-table", &val));
        ASSERT_EQ(kNumKeys, val);
      }
    }
  }
}

TEST_P(DBIteratorTest, MemtableOpsScanFlushTriggerWithNext) {
  // Tests that option memtable_op_scan_flush_trigger works when the limit
  // is reached during a Next() operation, and not trigger a flush when
  // the limit is reached across multiple Next() operations.
  const int kTrigger = 10;
  Random* r = Random::GetTLSInstance();

  for (int trigger : {kTrigger, kTrigger + 1}) {
    for (bool delete_only : {false, true}) {
      Options options;
      options.create_if_missing = true;
      options.memtable_op_scan_flush_trigger = trigger;
      options.level_compaction_dynamic_level_bytes = true;
      DestroyAndReopen(options);

      // Base data that will be covered by a consecutive sequence of tombstones.
      int kNumKeys = delete_only ? kTrigger : kTrigger / 2;
      for (int i = 0; i <= kNumKeys; ++i) {
        ASSERT_OK(Put(Key(i), r->RandomString(100)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
      ASSERT_EQ(1, NumTableFilesAtLevel(6));

      ASSERT_OK(Put(Key(0), "val"));
      if (delete_only) {
        for (int i = 1; i <= kNumKeys; ++i) {
          ASSERT_OK(SingleDelete(Key(i)));
        }
      } else {
        for (int i = 1; i <= kNumKeys; ++i) {
          ASSERT_OK(Put(Key(i), r->RandomString(100)));
        }
        for (int i = 1; i <= kNumKeys; ++i) {
          ASSERT_OK(Delete(Key(i)));
        }
      }

      // Total number of tombstones and hidden puts scanned across multiple
      // Next() operations below will be kTrigger, and it should not trigger a
      // flush when the limit is kTrigger + 1.
      ASSERT_OK(Put(Key(kNumKeys + 1), "v1"));
      ASSERT_OK(Delete(Key(kNumKeys + 2)));
      ASSERT_OK(Put(Key(kNumKeys + 3), "v3"));

      SetPerfLevel(PerfLevel::kEnableCount);
      get_perf_context()->Reset();
      ReadOptions ro;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      iter->Seek(Key(0));
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->value(), "val");
      ASSERT_OK(iter->status());
      ASSERT_EQ(get_perf_context()->next_on_memtable_count, 0);
      iter->Next();
      // kTrigger tombstones and invisible puts and 1 for the visible put
      ASSERT_EQ(get_perf_context()->next_on_memtable_count, kTrigger + 1);
      iter->Next();
      ASSERT_EQ(get_perf_context()->next_on_memtable_count, kTrigger + 3);

      // Skipping kNumTrigger memtable entries in a single iterator operation
      // should mark the memtable for flush.
      //
      // At the end of a write, we check and update memtable to request a flush
      ASSERT_OK(Put(Key(11), "val"));
      // Before a write, we schedule memtables for flush if requested.
      ASSERT_OK(Put(Key(12), "val"));
      ASSERT_OK(db_->WaitForCompact({}));

      if (trigger <= kTrigger) {
        // Check if memtable was flushed due to scan trigger
        ASSERT_EQ(1, NumTableFilesAtLevel(0));
        uint64_t val = 0;
        ASSERT_TRUE(
            db_->GetIntProperty("rocksdb.num-deletes-active-mem-table", &val));
        ASSERT_EQ(0, val);
      } else {
        uint64_t val = 0;
        ASSERT_TRUE(
            db_->GetIntProperty("rocksdb.num-deletes-active-mem-table", &val));
        ASSERT_EQ(kNumKeys + 1, val);
      }
    }
  }
}

TEST_P(DBIteratorTest, AverageMemtableOpsScanFlushTrigger) {
  // Tests option memtable_avg_op_scan_flush_trigger with
  // long tombstone sequences.
  Random* r = Random::GetTLSInstance();

  const int kAvgTrigger = 10;
  const int kMaxTrigger = 500;
  Options options;
  options.create_if_missing = true;
  options.memtable_op_scan_flush_trigger = kMaxTrigger;
  options.memtable_avg_op_scan_flush_trigger = kAvgTrigger;
  options.level_compaction_dynamic_level_bytes = true;
  DestroyAndReopen(options);

  const int kNumKeys = 1000;
  // Base data that will be covered by a consecutive sequence of tombstones.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), r->RandomString(50)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));

  for (int i = 0; i < kNumKeys; ++i) {
    // We issue slightly more deletions than kAvgTrigger between visible keys
    // to ensure avg skipped entries exceed kAvgTrigger.
    if (i % (kAvgTrigger + 2) != 0) {
      ASSERT_OK(SingleDelete(Key(i)));
    }
  }

  // Each operation, except the first Seek, is expected to see kAvgTrigger + 1
  // tombstones (from the active memtable) before it finds the next visible key.
  SetPerfLevel(PerfLevel::kEnableCount);
  get_perf_context()->Reset();
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek(Key(1));
  ASSERT_EQ(get_perf_context()->next_on_memtable_count, kAvgTrigger + 1);
  iter.reset();
  // Should not flush since total entries skipped is below
  // memtable_op_scan_flush_trigger
  ASSERT_OK(Put(Key(0), "dummy write"));
  ASSERT_OK(Put(Key(0), "dummy write"));
  ASSERT_OK(db_->WaitForCompact({}));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  get_perf_context()->Reset();
  iter.reset(db_->NewIterator(ReadOptions()));
  int num_ops = 1;
  uint64_t num_skipped = 0;
  iter->Seek(Key(0));
  ASSERT_EQ(iter->key(), Key(0));
  uint64_t last_memtable_next_count =
      get_perf_context()->next_on_memtable_count;
  iter->Next();
  num_ops++;
  while (iter->Valid()) {
    ASSERT_OK(iter->status());
    uint64_t num_skipped_in_op =
        get_perf_context()->next_on_memtable_count - last_memtable_next_count;
    ASSERT_GE(num_skipped_in_op, kAvgTrigger + 1);
    last_memtable_next_count = get_perf_context()->next_on_memtable_count;
    num_skipped += num_skipped_in_op;
    iter->Next();
    num_ops++;
  }
  // During iterator destruction we mark memtable for flush
  iter.reset();

  // avg trigger
  ASSERT_GE(num_skipped, kAvgTrigger * num_ops);
  // memtable_op_scan_flush_trigger
  ASSERT_GE(num_skipped, kMaxTrigger);
  // Average hidden entries scanned from memtable per operation is more than
  // kAvgTrigger and the total skipped is more than
  // memtable_op_scan_flush_trigger, the current memtable should be marked for
  // flush. The following two writes will trigger the flush.
  ASSERT_OK(Put(Key(0), "dummy write"));
  // Before a write, we schedule memtables for flush if requested.
  ASSERT_OK(Put(Key(0), "dummy write"));
  ASSERT_OK(db_->WaitForCompact({}));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
}

TEST_P(DBIteratorTest, AverageMemtableOpsScanFlushTriggerByOverwrites) {
  // Tests option memtable_avg_op_scan_flush_trigger with overwrites to keys.
  Random* r = Random::GetTLSInstance();

  const int kAvgTrigger = 25;
  Options options;
  options.create_if_missing = true;
  options.memtable_op_scan_flush_trigger = 250;
  options.memtable_avg_op_scan_flush_trigger = kAvgTrigger;
  options.level_compaction_dynamic_level_bytes = true;
  DestroyAndReopen(options);

  const int kNumKeys = 100;
  // Base data that will be covered by a consecutive sequence of tombstones.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), r->RandomString(50)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));

  // One visible key every 10 keys.
  // Each non-visible user key has 3 non-visible entries in the active memtable.
  for (int i = 0; i < kNumKeys; ++i) {
    if (i % 10 != 0) {
      ASSERT_OK(Put(Key(i), r->RandomString(50)));
      ASSERT_OK(Put(Key(i), r->RandomString(50)));
      ASSERT_OK(Delete(Key(i)));
    }
  }

  SetPerfLevel(PerfLevel::kEnableCount);
  get_perf_context()->Reset();
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->Seek(Key(1));
  ASSERT_GT(get_perf_context()->next_on_memtable_count, kAvgTrigger);
  // Re-seek to trigger check for flush trigger
  iter->Seek(Key(1));
  // Should not flush since total entries skipped is below
  // memtable_op_scan_flush_trigger
  ASSERT_FALSE(static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
                   ->cfd()
                   ->mem()
                   ->IsMarkedForFlush());
  ASSERT_OK(Put(Key(0), "dummy write"));
  ASSERT_OK(Put(Key(0), "dummy write"));
  ASSERT_OK(db_->WaitForCompact({}));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  get_perf_context()->Reset();

  int num_ops = 1;
  iter->Seek(Key(1));
  while (iter->Valid()) {
    num_ops++;
    iter->Next();
  }
  ASSERT_GT(get_perf_context()->next_on_memtable_count, num_ops * kAvgTrigger);

  // Re-seek should check conditions for marking memtable for flush
  iter->Seek(Key(80));

  // Average hidden entries scanned from memtable per operation is 2.
  ASSERT_OK(Put(Key(0), "dummy write"));
  // Before a write, we schedule memtables for flush if requested.
  ASSERT_OK(Put(Key(0), "dummy write"));
  ASSERT_OK(db_->WaitForCompact({}));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
}

TEST_P(DBIteratorTest, PrefixSameAsStartSeekToNonInDomainKey) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  DestroyAndReopen(options);

  ASSERT_OK(Put("abc1", "v1"));
  ASSERT_OK(Put("abc2", "v2"));
  ASSERT_OK(Put("abc3", "v3"));

  // Seek to "ab" (2 bytes) -- out-of-domain for FixedPrefixTransform(3).
  // ShouldSetPrefix returns false for out-of-domain targets, so no prefix
  // constraint is set. The seek should find "abc1" and iteration should
  // proceed without prefix_same_as_start enforcement.
  ReadOptions ro;
  ro.prefix_same_as_start = true;
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  iter->Seek("ab");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "abc1");
  ASSERT_EQ(iter->value().ToString(), "v1");

  // Since prefix_ was not set (out-of-domain seek target), Next should
  // continue without prefix boundary checking.
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "abc2");

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "abc3");
}

TEST_P(DBIteratorTest, PrefixSameAsStartIteratePastOutOfDomainKey) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  // FixedPrefixTransform(3): keys shorter than 3 bytes are out-of-domain.
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  DestroyAndReopen(options);

  ASSERT_OK(Put("abc1", "v1"));
  ASSERT_OK(Put("abc2", "v2"));
  ASSERT_OK(Put("zz", "short"));  // out-of-domain: only 2 bytes

  ReadOptions ro;
  ro.prefix_same_as_start = true;
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  iter->Seek("abc1");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "abc1");

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "abc2");

  // Next encounters "zz" (out-of-domain for FixedPrefixTransform(3)).
  // PrefixCheck returns false for out-of-domain keys, so the iterator
  // invalidates cleanly instead of calling Transform() on the short key.
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

class DBMultiScanIteratorTest : public DBTestBase,
                                public ::testing::WithParamInterface<bool> {
 public:
  DBMultiScanIteratorTest()
      : DBTestBase("db_multi_scan_iterator_test", /*env_do_fsync=*/true) {}
};

// Param 0: ReadOptions::fill_cache
INSTANTIATE_TEST_CASE_P(DBMultiScanIteratorTest, DBMultiScanIteratorTest,
                        ::testing::Bool());

TEST_P(DBMultiScanIteratorTest, BasicTest) {
  auto options = CurrentOptions();
  DestroyAndReopen(options);

  // Create a file
  for (int i = 0; i < 100; ++i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    ASSERT_OK(Put("k" + ss.str(), "val" + ss.str()));
  }
  ASSERT_OK(Flush());

  std::vector<std::string> key_ranges({"k03", "k10", "k25", "k50"});
  ReadOptions ro;
  ro.fill_cache = GetParam();
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    int idx = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString().compare(key_ranges[idx]), 0);
        ASSERT_LT(it.first.ToString().compare(key_ranges[idx + 1]), 0);
        count++;
      }
      idx += 2;
    }
    ASSERT_EQ(count, 32);
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, MixedBoundsTest) {
  auto options = CurrentOptions();
  DestroyAndReopen(options);
  // Create a file
  for (int i = 0; i < 100; ++i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    ASSERT_OK(Put("k" + ss.str(), "val" + ss.str()));
  }
  ASSERT_OK(Flush());

  std::vector<std::string> key_ranges(
      {"k03", "k10", "k25", "k50", "k75", "k90"});
  ReadOptions ro;
  ro.fill_cache = GetParam();
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2]);
  scan_options.insert(key_ranges[4], key_ranges[5]);
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    int idx = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(
            it.first.ToString().compare(
                scan_options.GetScanRanges()[idx].range.start->ToString()),
            0);
        if (scan_options.GetScanRanges()[idx].range.limit) {
          ASSERT_LT(
              it.first.ToString().compare(
                  scan_options.GetScanRanges()[idx].range.limit->ToString()),
              0);
        }
        count++;
      }
      idx++;
    }
    ASSERT_EQ(count, 97);
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();
  scan_options = MultiScanArgs(BytewiseComparator());
  scan_options.insert(key_ranges[0]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  scan_options.insert(key_ranges[4]);
  iter = dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    int idx = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(
            it.first.ToString().compare(
                scan_options.GetScanRanges()[idx].range.start->ToString()),
            0);
        if (scan_options.GetScanRanges()[idx].range.limit) {
          ASSERT_LT(
              it.first.ToString().compare(
                  scan_options.GetScanRanges()[idx].range.limit->ToString()),
              0);
        }
        count++;
      }
      idx++;
    }
    ASSERT_EQ(count, 147);
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, RangeAcrossFiles) {
  auto options = CurrentOptions();
  options.target_file_size_base = 100 << 10;  // 20KB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  auto rnd = Random::GetTLSInstance();
  // Write ~200KB data
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(Key(i), rnd->RandomString(2 << 10)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_EQ(2, NumTableFilesAtLevel(49));
  std::vector<std::string> key_ranges({Key(10), Key(90)});
  ReadOptions ro;
  ro.fill_cache = GetParam();
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    int i = 10;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_EQ(it.first.ToString(), Key(i));
        ++i;
      }
    }
    ASSERT_EQ(i, 90);
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, SortedRangesSkipIODispatcherSort) {
  auto options = CurrentOptions();
  options.target_file_size_base = 100 << 10;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  auto key = [](int i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    return ss.str();
  };

  auto rnd = Random::GetTLSInstance();
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(key(i), rnd->RandomString(2 << 10)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_EQ(2, NumTableFilesAtLevel(49));

  int sort_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "IODispatcherImpl::SubmitJob:SortBlockHandles",
      [&](void* /*arg*/) { ++sort_count; });
  SyncPoint::GetInstance()->EnableProcessing();

  auto tracking_dispatcher = std::make_shared<TrackingIODispatcher>();
  std::vector<std::string> key_ranges({key(10), key(90)});
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.io_dispatcher = tracking_dispatcher;
  scan_options.use_async_io = false;
  scan_options.insert(key_ranges[0], key_ranges[1]);

  ReadOptions ro;
  ro.fill_cache = GetParam();
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);

  try {
    int i = 10;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_EQ(it.first.ToString(), key(i));
        ++i;
      }
    }
    ASSERT_EQ(i, 90);
  } catch (MultiScanException& ex) {
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }

  ASSERT_GT(tracking_dispatcher->GetReadSets().size(), 0);
  EXPECT_EQ(sort_count, 0);
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, FailureTest) {
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  Random rnd(301);
  // Create a file
  for (int i = 0; i < 100; ++i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    ASSERT_OK(Put("k" + ss.str(), rnd.RandomString(1024)));
  }
  ASSERT_OK(Flush());

  std::vector<std::string> key_ranges({"k04", "k06", "k12", "k14"});
  ReadOptions ro;
  Slice ub;
  ro.iterate_upper_bound = &ub;
  ro.fill_cache = GetParam();
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  scan_options.max_prefetch_size = 4500;
  scan_options.rolling_prefetch = true;
  scan_options.rolling_prefetch_window_size = 4500;
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Prepare(scan_options);
  int count = 0;
  ub = key_ranges[1];
  iter->Seek(key_ranges[0]);
  while (iter->status().ok() && iter->Valid()) {
    ASSERT_GE(iter->key().compare(key_ranges[0]), 0);
    ASSERT_LT(iter->key().compare(key_ranges[1]), 0);
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status()) << iter->status().ToString();
  ASSERT_EQ(count, 2);

  // Second seek should hit the max_prefetch_size limit
  ub = key_ranges[3];
  iter->Seek(key_ranges[2]);
  ASSERT_NOK(iter->status());
  iter.reset();

  // Test the case of unexpected Seek key
  iter.reset(dbfull()->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  scan_options.max_prefetch_size = 0;
  iter->Prepare(scan_options);
  ub = key_ranges[3];
  iter->Seek(key_ranges[2]);
  ASSERT_NOK(iter->status());
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, OutOfL0FileRange) {
  // Test that prepare does not fail scan when a scan range
  // is outside of a L0 file's key range.
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  Random rnd(301);
  // Create a Lmax file
  // key01 ~ key99
  for (int i = 0; i < 100; ++i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    ASSERT_OK(Put("k" + ss.str(), rnd.RandomString(1024)));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // Create a L0 file
  // key00 ~ key09
  for (int i = 0; i < 10; ++i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    ASSERT_OK(Put("k" + ss.str(), rnd.RandomString(1024)));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  // The second range is outside of L0 file's key range
  std::vector<std::string> key_ranges({"k04", "k06", "k12", "k14"});
  ReadOptions ro;
  Slice ub;
  ro.iterate_upper_bound = &ub;
  ro.fill_cache = GetParam();
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = true;
  scan_options.max_prefetch_size = 64 << 10;
  scan_options.rolling_prefetch = true;
  scan_options.rolling_prefetch_window_size = 128 << 10;
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Prepare(scan_options);
  int count = 0;
  ub = key_ranges[1];
  iter->Seek(key_ranges[0]);
  while (iter->status().ok() && iter->Valid()) {
    ASSERT_GE(iter->key().compare(key_ranges[0]), 0);
    ASSERT_LT(iter->key().compare(key_ranges[1]), 0);
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status()) << iter->status().ToString();
  ASSERT_EQ(count, 2);

  ub = key_ranges[3];
  count = 0;
  iter->Seek(key_ranges[2]);
  while (iter->status().ok() && iter->Valid()) {
    ASSERT_GE(iter->key().compare(key_ranges[2]), 0);
    ASSERT_LT(iter->key().compare(key_ranges[3]), 0);
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status()) << iter->status().ToString();
  ASSERT_EQ(count, 2);
}

TEST_P(DBMultiScanIteratorTest, RangeBetweenFiles) {
  auto options = CurrentOptions();
  options.target_file_size_base = 100 << 10;  // 20KB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  auto rnd = Random::GetTLSInstance();
  // Write ~200KB data
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(Key(i), rnd->RandomString(2 << 10)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_EQ(2, NumTableFilesAtLevel(49));

  // Test with a scan range that overlaps an entire file, with upper bound
  // between 2 files
  std::vector<LiveFileMetaData> file_meta;
  dbfull()->GetLiveFilesMetaData(&file_meta);
  ASSERT_EQ(file_meta.size(), 2);
  std::vector<std::string> key_ranges(4);
  key_ranges[0] = file_meta[0].smallestkey;
  key_ranges[1] = file_meta[0].largestkey + "0";
  key_ranges[2] = file_meta[1].smallestkey + "0";
  key_ranges[3] = file_meta[1].largestkey;
  ReadOptions ro;
  ro.fill_cache = GetParam();
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString(), key_ranges[0]);
      }
    }
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();

  // Test multiscan with a range entirely between adjacent files
  key_ranges[0] = file_meta[0].largestkey + "0";
  key_ranges[1] = file_meta[0].largestkey + "1";
  key_ranges[2] = file_meta[1].smallestkey + "0";
  key_ranges[3] = file_meta[1].largestkey;
  (*scan_options).clear();
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  iter = dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString(), key_ranges[0]);
      }
    }
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();
}

// This test case tests multiscan in the presence of fragmented range
// tombstones in the LSM.
TEST_P(DBMultiScanIteratorTest, FragmentedRangeTombstones) {
  auto options = CurrentOptions();
  // Compaction may create files 2x the target_file_size_base,
  // so set this to 50KB so we atleast end up with 2 files of
  // 100KB
  options.target_file_size_base = 50 << 10;  // 50KB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  // Setup the LSM as follows -
  // 1. Ingest a file with 100 keys
  // 2. Ingest a file with one overlapping key
  // 3. Do a Put and flush a file to L0 with one overlapping key
  // 4. Ingest a standalone delete range file that covers the full key space
  //    and a file with the same 100 keys with new values. This will ingest
  //    into L0 due to the presence of an existing file in L0
  // The final LSM will have an SST in Lmax with 100 keys, and 2 SST files
  // in Lmax-1 with half the keys each and completely overlapping delete ranges
  std::unordered_map<std::string, std::string> kvs;
  auto rnd = Random::GetTLSInstance();
  auto create_ingestion_data_file_and_update_key_value =
      [&](const std::string& filename, int start_key, int end_key) {
        std::unique_ptr<SstFileWriter> writer;
        writer.reset(new SstFileWriter(EnvOptions(), options));
        ASSERT_OK(writer->Open(filename));
        for (int i = start_key; i < end_key; ++i) {
          auto kiter = kvs.find(Key(i));
          if (kiter != kvs.end()) {
            kvs.erase(kiter);
          }
          auto res =
              kvs.emplace(std::make_pair(Key(i), rnd->RandomString(2 << 10)));
          ASSERT_OK(writer->Put(res.first->first, res.first->second));
        }
        ASSERT_OK(writer->Finish());
        writer.reset();
      };

  CreateColumnFamilies({"new_cf"}, options);
  std::string ingest_file = dbname_ + "test.sst";
  // Write ~200KB data
  create_ingestion_data_file_and_update_key_value(ingest_file + "_0", 0, 100);
  create_ingestion_data_file_and_update_key_value(ingest_file + "_1", 50, 51);
  ColumnFamilyHandle* cfh = handles_[0];
  IngestExternalFileOptions ifo;
  Status s = dbfull()->IngestExternalFile(
      cfh, {ingest_file + "_0", ingest_file + "_1"}, ifo);
  ASSERT_OK(s);

  ASSERT_OK(Put(0, Key(50), rnd->RandomString(2 << 10)));
  ASSERT_OK(Flush());

  {
    std::unique_ptr<SstFileWriter> writer;
    writer.reset(new SstFileWriter(EnvOptions(), options));
    ASSERT_OK(writer->Open(ingest_file + "_2"));
    ASSERT_OK(writer->DeleteRange("a", "z"));
    ASSERT_OK(writer->Finish());
    writer.reset();
  }
  create_ingestion_data_file_and_update_key_value(ingest_file + "_3", 0, 100);
  s = dbfull()->IngestExternalFile(
      cfh, {ingest_file + "_2", ingest_file + "_3"}, ifo);
  ASSERT_OK(s);

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(cfh, &cf_meta);
  // Only the L0 with range deletion is compacted.
  ASSERT_EQ(1, cf_meta.levels[0].files.size());
  ASSERT_EQ(0, cf_meta.levels[0].files[0].num_deletions);

  // The first scan range overlaps the DB key range, while the second extends
  // beyond but overlaps the delete range
  std::vector<std::string> key_ranges({"key000085", "key000090", "l", "n"});
  ReadOptions ro;
  ro.fill_cache = GetParam();
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    int i = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString(), key_ranges[i]);
        ASSERT_LT(it.first.ToString(), key_ranges[i + 1]);
        auto kiter = kvs.find(it.first.ToString());
        ASSERT_NE(kiter, kvs.end());
        ASSERT_EQ(kiter->second, it.second.ToString());
        count++;
      }
      i += 2;
    }
    ASSERT_EQ(i, 4);
    ASSERT_EQ(count, 5);
  } catch (MultiScanException& ex) {
    ASSERT_OK(ex.status());
  }
  iter.reset();

  // The second scan range start overlaps the delete range in the first file
  // in Lmax-1, while the end overlaps the keys in the second file
  (*scan_options).clear();
  key_ranges[0] = "key000010";
  key_ranges[1] = "key000020";
  key_ranges[2] = "key0000500";
  key_ranges[3] = "key000060";
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  iter = dbfull()->NewMultiScan(ro, cfh, scan_options);
  try {
    int i = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString(), key_ranges[i]);
        ASSERT_LT(it.first.ToString(), key_ranges[i + 1]);
        auto kiter = kvs.find(it.first.ToString());
        ASSERT_NE(kiter, kvs.end());
        ASSERT_EQ(kiter->second, it.second.ToString());
        count++;
      }
      i += 2;
    }
    ASSERT_EQ(i, 4);
    ASSERT_EQ(count, 19);
  } catch (MultiScanException& ex) {
    ASSERT_OK(ex.status());
  }
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, ReseekAcrossBlocksSameUserKey) {
  // This test exposes a bug where multiscan reseeks backwards when
  // max_sequential_skip_in_iterations is triggered with the same user key
  // spanning multiple data blocks.

  auto options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  options.compression = kNoCompression;

  // Force each internal key into its own block
  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Taking a snapshot after each Put to preserve all versions during flush.
  std::vector<const Snapshot*> snapshots;
  for (int i = 0; i < 7; ++i) {
    ASSERT_OK(Put("key_a", "value_" + std::to_string(i)));
    snapshots.push_back(db_->GetSnapshot());
  }
  ASSERT_OK(Put("key_b", "value_b"));

  ASSERT_OK(Flush());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // Setup multiscan range covering both keys
  std::vector<std::string> key_ranges({"key_a", "key_c"});
  ReadOptions ro;
  Slice ub = key_ranges[1];
  ro.iterate_upper_bound = &ub;
  ro.fill_cache = GetParam();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Prepare(scan_options);

  std::vector<std::string> seen_keys;
  std::vector<std::string> seen_values;
  iter->Seek(key_ranges[0]);
  while (iter->status().ok() && iter->Valid()) {
    seen_keys.push_back(iter->key().ToString());
    seen_values.push_back(iter->value().ToString());
    iter->Next();
  }
  ASSERT_OK(iter->status()) << iter->status().ToString();

  ASSERT_EQ(seen_keys.size(), 2) << "Should see key_a and key_b";
  ASSERT_EQ(seen_keys[0], "key_a");
  ASSERT_EQ(seen_keys[1], "key_b");
  ASSERT_EQ(seen_values[0], "value_6");
  ASSERT_EQ(seen_values[1], "value_b");

  for (auto* snapshot : snapshots) {
    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_P(DBMultiScanIteratorTest, AsyncPrefetchAcrossMultipleFiles) {
  // Test async prefetch with multiple ranges within a single file
  auto options = CurrentOptions();
  options.target_file_size_base = 1 << 15;  // 32KiB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  Random rnd(303);

  // Create a single large file with many keys
  // ~1MiB of data
  // Should be lots of files now
  for (int i = 0; i < 1000; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    // 1KiB values
    ASSERT_OK(Put(ss.str(), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  ASSERT_GT(NumTableFilesAtLevel(49), 3);

  // Set up multiple non-overlapping ranges in the same file
  // Every 32 values should be a file or so
  std::vector<std::string> key_ranges(
      {"k00000", "k00100", "k00500", "k00600", "k00800", "k00900"});
  ReadOptions ro;
  ro.fill_cache = GetParam();

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = true;
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  scan_options.insert(key_ranges[4], key_ranges[5]);

  auto read_count_before =
      options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);
  auto read_count_after =
      options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
  ASSERT_EQ(read_count_after, read_count_before);

  // Verify all three ranges can be scanned successfully
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        it.first.ToString();
      }
    }
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }

  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, ReversePrefetchAcrossMultipleRanges) {
  auto options = CurrentOptions();
  options.target_file_size_base = 1 << 15;  // 32KiB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  Random rnd(303);
  for (int i = 0; i < 1000; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_GT(NumTableFilesAtLevel(49), 3);

  ReadOptions ro;
  ro.fill_cache = GetParam();
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  auto tracking_dispatcher = std::make_shared<TrackingIODispatcher>();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = false;
  scan_options.reverse = true;
  scan_options.rolling_prefetch = true;
  scan_options.rolling_prefetch_window_size = 64 << 10;
  scan_options.io_dispatcher = tracking_dispatcher;
  std::vector<std::string> key_ranges(
      {Key(100), Key(200), Key(500), Key(600), Key(800), Key(900)});
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  scan_options.insert(key_ranges[4], key_ranges[5]);

  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);

  std::vector<std::string> actual_keys;
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        actual_keys.push_back(it.first.ToString());
      }
    }
  } catch (MultiScanException& ex) {
    FAIL() << "Iterator returned status " << ex.what();
  } catch (std::logic_error& ex) {
    FAIL() << "Iterator returned logic error " << ex.what();
  }

  std::vector<std::string> expected_keys;
  for (int i = 199; i >= 100; --i) {
    expected_keys.push_back(Key(i));
  }
  for (int i = 599; i >= 500; --i) {
    expected_keys.push_back(Key(i));
  }
  for (int i = 899; i >= 800; --i) {
    expected_keys.push_back(Key(i));
  }
  ASSERT_EQ(actual_keys, expected_keys);
  ASSERT_GT(tracking_dispatcher->GetReadSets().size(), 0);
}

TEST_P(DBMultiScanIteratorTest, ReverseUnreleasedPrefetchBlocksCountAsWasted) {
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  table_options.block_cache = NewLRUCache(10 * 1024 * 1024);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  constexpr int kNumKeys = 40;
  const std::string value(100, 'v');
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  ReadOptions ro;
  ro.fill_cache = GetParam();
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  auto tracking_dispatcher = std::make_shared<TrackingIODispatcher>();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = false;
  scan_options.reverse = true;
  scan_options.io_dispatcher = tracking_dispatcher;
  const std::string start_key = Key(0);
  const std::string limit_key = Key(kNumKeys);
  scan_options.insert(start_key, limit_key);

  {
    std::unique_ptr<MultiScan> iter =
        dbfull()->NewMultiScan(ro, cfh, scan_options);
    ASSERT_NE(iter, nullptr);

    try {
      auto range_it = iter->begin();
      ASSERT_NE(range_it, iter->end());
      auto range = *range_it;
      auto row_it = range.begin();
      ASSERT_NE(row_it, range.end());
      ASSERT_EQ((*row_it).first.ToString(), Key(kNumKeys - 1));
    } catch (MultiScanException& ex) {
      FAIL() << "Iterator returned status " << ex.what();
    } catch (std::logic_error& ex) {
      FAIL() << "Iterator returned logic error " << ex.what();
    }

    ASSERT_GT(tracking_dispatcher->GetReadSets().size(), 0);
  }

  ASSERT_GT(
      options.statistics->getTickerCount(MULTISCAN_PREFETCH_BLOCKS_WASTED), 0);
}

TEST_P(DBMultiScanIteratorTest, ReverseRequiresLimits) {
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Flush());

  ReadOptions ro;
  ro.fill_cache = GetParam();
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.reverse = true;
  scan_options.insert("a");
  scan_options.insert("b", "c");

  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);
  try {
    (void)iter->begin();
    FAIL() << "Expected reverse MultiScan without an upper bound to fail";
  } catch (MultiScanException& ex) {
    ASSERT_NOK(ex.status());
    ASSERT_TRUE(ex.status().IsInvalidArgument());
  }
}

// Wrapper filesystem that does not support async IO.
// Used to verify that MultiScan gracefully falls back to sync IO.
class NoAsyncIOFS : public FileSystemWrapper {
 public:
  explicit NoAsyncIOFS(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}
  static const char* kClassName() { return "NoAsyncIOFS"; }
  const char* Name() const override { return kClassName(); }

  void SupportedOps(int64_t& supported_ops) override {
    target()->SupportedOps(supported_ops);
    supported_ops &= ~(1 << FSSupportedOps::kAsyncIO);
  }

  IOStatus Poll(std::vector<void*>& /*io_handles*/,
                size_t /*min_completions*/) override {
    ADD_FAILURE() << "Poll should not be called when kAsyncIO is unsupported";
    return IOStatus::NotSupported();
  }

  IOStatus AbortIO(std::vector<void*>& /*io_handles*/) override {
    ADD_FAILURE()
        << "AbortIO should not be called when kAsyncIO is unsupported";
    return IOStatus::NotSupported();
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    IOStatus s = target()->NewRandomAccessFile(fname, opts, result, dbg);
    if (s.ok()) {
      *result = std::make_unique<NoAsyncIOFile>(std::move(*result));
    }
    return s;
  }

 private:
  class NoAsyncIOFile : public FSRandomAccessFileOwnerWrapper {
   public:
    using FSRandomAccessFileOwnerWrapper::FSRandomAccessFileOwnerWrapper;
    IOStatus ReadAsync(FSReadRequest& /*req*/, const IOOptions& /*opts*/,
                       std::function<void(FSReadRequest&, void*)> /*cb*/,
                       void* /*cb_arg*/, void** /*io_handle*/,
                       IOHandleDeleter* /*del_fn*/,
                       IODebugContext* /*dbg*/) override {
      ADD_FAILURE()
          << "ReadAsync should not be called when kAsyncIO is unsupported";
      return IOStatus::NotSupported();
    }
  };
};

TEST_P(DBMultiScanIteratorTest, AsyncIOFallbackWithoutFSSupport) {
  // Verify that MultiScan works correctly when use_async_io = true but the
  // filesystem does NOT support kAsyncIO. The constructor should silently
  // disable async IO, and no async operations should be attempted.
  auto options = CurrentOptions();
  options.target_file_size_base = 1 << 15;  // 32KiB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;

  auto no_async_fs = std::make_shared<NoAsyncIOFS>(env_->GetFileSystem());
  std::unique_ptr<Env> wrapped_env(new CompositeEnvWrapper(env_, no_async_fs));
  options.env = wrapped_env.get();

  DestroyAndReopen(options);

  Random rnd(305);

  // Create data across multiple files
  for (int i = 0; i < 1000; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  // Set up multiple non-overlapping ranges
  std::vector<std::string> key_ranges(
      {"k00000", "k00100", "k00500", "k00600", "k00800", "k00900"});
  ReadOptions ro;
  ro.fill_cache = GetParam();

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = true;  // Deliberately request async IO
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  scan_options.insert(key_ranges[4], key_ranges[5]);

  // The MultiScan constructor should disable async IO since the FS
  // doesn't support it. If it doesn't, the NoAsyncIOFS wrapper will
  // cause ADD_FAILURE() when ReadAsync/Poll/AbortIO are called.
  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);

  // Iterate all ranges and verify keys are returned correctly
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        it.first.ToString();
      }
    }
  } catch (MultiScanException& ex) {
    FAIL() << "Iterator returned status " << ex.what();
  } catch (std::logic_error& ex) {
    FAIL() << "Iterator returned logic error " << ex.what();
  }

  iter.reset();
  Close();
}

TEST_P(DBMultiScanIteratorTest, AsyncPrefetchMultipleLevels) {
  // Test async prefetch with files in L0 and non-L0 levels
  // Similar setup to AsyncPrefetchAcrossMultipleFiles but with L0 files
  auto options = CurrentOptions();
  options.target_file_size_base = 1 << 15;  // 32KiB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  Random rnd(304);

  // Create base files and compact to bottom level - ~500KiB of data
  for (int i = 0; i < 500; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  // Verify we have files at bottom level
  ASSERT_GT(NumTableFilesAtLevel(49), 0);

  // Create additional L0 files with overlapping key ranges
  for (int i = 100; i < 150; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());

  // Verify we now have files in both L0 and bottom level
  ASSERT_GT(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(49), 0);

  // Set up multiple non-overlapping ranges
  std::vector<std::string> key_ranges(
      {"k00000", "k00100", "k00200", "k00300", "k00400", "k00500"});
  ReadOptions ro;
  ro.fill_cache = GetParam();

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = true;
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  scan_options.insert(key_ranges[4], key_ranges[5]);

  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);

  // Verify all three ranges can be scanned successfully
  int total_keys = 0;
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        it.first.ToString();
        total_keys++;
      }
    }
  } catch (MultiScanException& ex) {
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }

  // Should have keys from all three ranges
  ASSERT_GT(total_keys, 0);
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, AsyncPrefetchWithDeleteRange) {
  // Test async prefetch with delete ranges
  auto options = CurrentOptions();
  options.target_file_size_base = 1 << 15;  // 32KiB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  Random rnd(305);

  // Create base data - ~500KiB
  for (int i = 0; i < 500; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());

  // Add delete ranges
  ASSERT_OK(db_->DeleteRange(WriteOptions(), dbfull()->DefaultColumnFamily(),
                             "k00100", "k00200"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_GT(NumTableFilesAtLevel(49), 0);

  // Set up scan ranges that interact with delete ranges
  std::vector<std::string> key_ranges({"k00000", "k00500"});
  ReadOptions ro;
  ro.fill_cache = GetParam();

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = true;
  scan_options.insert(key_ranges[0], key_ranges[1]);

  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);

  // Verify ranges can be scanned successfully
  int total_keys = 0;
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        std::string key = it.first.ToString();
        // Verify deleted keys are not returned
        ASSERT_TRUE((key < "k00100" || key >= "k00200"));
        total_keys++;
      }
    }
  } catch (MultiScanException& ex) {
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }

  // Should have keys excluding deleted ranges
  ASSERT_EQ(total_keys, 400);
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, AsyncPrefetchWithExternalFileIngestion) {
  // Test async prefetch with externally ingested files
  auto options = CurrentOptions();
  options.target_file_size_base = 1 << 15;  // 32KiB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  Random rnd(306);

  // Create base data - ~200KiB
  for (int i = 0; i < 200; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  // Create and ingest external SST file with new data
  std::string ingest_file = dbname_ + "/test_ingest.sst";
  {
    std::unique_ptr<SstFileWriter> writer;
    writer.reset(new SstFileWriter(EnvOptions(), options));
    ASSERT_OK(writer->Open(ingest_file));
    for (int i = 300; i < 500; ++i) {
      std::stringstream ss;
      ss << "k" << std::setw(5) << std::setfill('0') << i;
      ASSERT_OK(writer->Put(ss.str(), rnd.RandomString(1 << 10)));
    }
    ASSERT_OK(writer->Finish());
  }

  IngestExternalFileOptions ifo;
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  ASSERT_OK(dbfull()->IngestExternalFile(cfh, {ingest_file}, ifo));

  // Set up scan ranges that span both regular and ingested files
  std::vector<std::string> key_ranges({"k00000", "k00500"});
  ReadOptions ro;
  ro.fill_cache = GetParam();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = true;
  scan_options.insert(key_ranges[0], key_ranges[1]);

  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);

  // Verify all ranges can be scanned successfully
  int total_keys = 0;
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        it.first.ToString();
        total_keys++;
      }
    }
  } catch (MultiScanException& ex) {
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }

  ASSERT_EQ(total_keys, 400);
  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, IODispatcherStatsVerification) {
  // Test that verifies all IOs go through the IODispatcher by checking stats
  auto options = CurrentOptions();
  options.target_file_size_base = 1 << 15;  // 32KiB
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 50;
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  Random rnd(307);

  // Create data - enough to create multiple data blocks
  for (int i = 0; i < 500; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(5) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), rnd.RandomString(1 << 10)));  // 1KiB values
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  // Set up scan ranges
  std::vector<std::string> key_ranges({"k00000", "k00200", "k00300", "k00400"});
  ReadOptions ro;
  ro.fill_cache = GetParam();

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  // Create a tracking IODispatcher to verify IO statistics
  auto tracking_dispatcher = std::make_shared<TrackingIODispatcher>();

  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = false;  // Use sync IO for predictable stats
  scan_options.io_dispatcher = tracking_dispatcher;
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);

  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);

  // Scan through all data
  int total_keys = 0;
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        it.first.ToString();
        total_keys++;
      }
    }
  } catch (MultiScanException& ex) {
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }

  // We scanned ~200 keys in range 1 and ~100 keys in range 2
  ASSERT_EQ(total_keys, 300);

  // Verify that IO operations went through the IODispatcher
  // The total IO operations should be > 0 (either sync reads, async reads, or
  // cache hits)
  uint64_t total_ops = tracking_dispatcher->GetTotalIOOperations();
  ASSERT_GT(total_ops, 0) << "Expected some IO operations through IODispatcher";

  // Verify that we have at least one ReadSet created
  ASSERT_GT(tracking_dispatcher->GetReadSets().size(), 0)
      << "Expected at least one ReadSet to be created";

  // Since we used sync IO, we should have sync reads (or cache hits if cached)
  uint64_t sync_reads = tracking_dispatcher->GetTotalSyncReads();
  uint64_t cache_hits = tracking_dispatcher->GetTotalCacheHits();
  ASSERT_GT(sync_reads + cache_hits, 0)
      << "Expected sync reads or cache hits for sync IO mode";

  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, IODispatcherPrefetchKnownBlocks) {
  // Test that verifies we prefetch a known/expected number of blocks.
  // Uses FlushBlockEveryKeyPolicyFactory to create exactly one block per key,
  // making the block count predictable and verifiable.
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  // Configure to create exactly one block per key
  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  // Use a block cache (required by IODispatcher), but use a fresh one
  // that won't have any cached data
  table_options.block_cache = NewLRUCache(10 * 1024 * 1024);  // 10MB cache
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Create exactly 100 keys, each in its own block
  const int kNumKeys = 100;
  const int kValueSize = 100;  // Fixed value size for predictability
  std::string value(kValueSize, 'v');

  for (int i = 0; i < kNumKeys; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(3) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), value));
  }
  ASSERT_OK(Flush());

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  // Create a tracking IODispatcher to verify IO statistics
  auto tracking_dispatcher = std::make_shared<TrackingIODispatcher>();

  // Define scan ranges with known block counts:
  // Range 1: k000 to k020 (20 keys = 20 blocks)
  // Range 2: k050 to k060 (10 keys = 10 blocks)
  // Total expected blocks to read: 30
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = false;  // Use sync IO for predictable stats
  scan_options.io_dispatcher = tracking_dispatcher;
  scan_options.insert("k000", "k020");
  scan_options.insert("k050", "k060");

  ReadOptions ro;
  ro.fill_cache = false;  // Don't fill cache, ensure fresh reads

  std::unique_ptr<MultiScan> iter =
      dbfull()->NewMultiScan(ro, cfh, scan_options);
  ASSERT_NE(iter, nullptr);

  // Scan through all data and count keys
  int total_keys = 0;
  try {
    for (auto range : *iter) {
      for (auto it : range) {
        it.first.ToString();
        total_keys++;
      }
    }
  } catch (MultiScanException& ex) {
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }

  // Verify we scanned the expected number of keys
  // Range 1: k000-k019 = 20 keys, Range 2: k050-k059 = 10 keys
  ASSERT_EQ(total_keys, 30) << "Expected 30 keys from two ranges";

  // Verify IODispatcher statistics
  uint64_t total_ops = tracking_dispatcher->GetTotalIOOperations();
  uint64_t sync_reads = tracking_dispatcher->GetTotalSyncReads();

  // We should have at least as many IO operations as blocks we need to read
  // (could be more due to index/filter blocks)
  ASSERT_GE(total_ops, 30)
      << "Expected at least 30 IO operations for 30 data blocks";

  // Since cache is fresh and fill_cache=false, all should be sync reads
  ASSERT_GE(sync_reads, 30)
      << "Expected at least 30 sync reads for 30 data blocks";

  // Verify we created ReadSets (one per range)
  size_t num_readsets = tracking_dispatcher->GetReadSets().size();
  ASSERT_GE(num_readsets, 1) << "Expected at least one ReadSet";

  // Log the stats for debugging
  std::cout << "IODispatcher Stats: total_ops=" << total_ops
            << ", sync_reads=" << sync_reads
            << ", async_reads=" << tracking_dispatcher->GetTotalAsyncReads()
            << ", cache_hits=" << tracking_dispatcher->GetTotalCacheHits()
            << ", readsets=" << num_readsets << std::endl;

  iter.reset();
}

TEST_P(DBMultiScanIteratorTest, IODispatcherCacheHitVerification) {
  // Test that verifies cache hits are properly tracked through IODispatcher.
  // First scan populates cache, second scan should show cache hits.
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  // Enable block cache with enough space for all blocks
  table_options.block_cache = NewLRUCache(10 * 1024 * 1024);  // 10MB cache
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Create 50 keys, each in its own block
  const int kNumKeys = 50;
  std::string value(100, 'v');

  for (int i = 0; i < kNumKeys; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(3) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), value));
  }
  ASSERT_OK(Flush());

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  // First scan: populate the cache
  {
    auto dispatcher1 = std::make_shared<TrackingIODispatcher>();
    MultiScanArgs scan_options(BytewiseComparator());
    scan_options.use_async_io = false;
    scan_options.io_dispatcher = dispatcher1;
    scan_options.insert("k000", "k025");  // 25 keys

    ReadOptions ro;
    ro.fill_cache = true;  // Fill cache on first scan

    std::unique_ptr<MultiScan> iter =
        dbfull()->NewMultiScan(ro, cfh, scan_options);
    ASSERT_NE(iter, nullptr);

    int count = 0;
    try {
      for (auto range : *iter) {
        for (auto it : range) {
          it.first.ToString();
          count++;
        }
      }
    } catch (MultiScanException& ex) {
      FAIL() << "First scan failed: " << ex.what();
    }
    ASSERT_EQ(count, 25);

    // First scan should have sync reads (cache was empty)
    uint64_t first_sync = dispatcher1->GetTotalSyncReads();
    ASSERT_GE(first_sync, 25) << "First scan should have sync reads";

    std::cout << "First scan stats: sync_reads=" << first_sync
              << ", cache_hits=" << dispatcher1->GetTotalCacheHits()
              << std::endl;
  }

  // Second scan: should get cache hits
  {
    auto dispatcher2 = std::make_shared<TrackingIODispatcher>();
    MultiScanArgs scan_options(BytewiseComparator());
    scan_options.use_async_io = false;
    scan_options.io_dispatcher = dispatcher2;
    scan_options.insert("k000", "k025");  // Same range as before

    ReadOptions ro;
    ro.fill_cache = true;

    std::unique_ptr<MultiScan> iter =
        dbfull()->NewMultiScan(ro, cfh, scan_options);
    ASSERT_NE(iter, nullptr);

    int count = 0;
    try {
      for (auto range : *iter) {
        for (auto it : range) {
          it.first.ToString();
          count++;
        }
      }
    } catch (MultiScanException& ex) {
      FAIL() << "Second scan failed: " << ex.what();
    }
    ASSERT_EQ(count, 25);

    // Second scan should have cache hits (blocks were cached in first scan)
    uint64_t second_cache_hits = dispatcher2->GetTotalCacheHits();
    uint64_t second_sync = dispatcher2->GetTotalSyncReads();

    std::cout << "Second scan stats: sync_reads=" << second_sync
              << ", cache_hits=" << second_cache_hits << std::endl;

    // We expect cache hits on the second scan for data blocks
    // Note: Some blocks might still need sync reads (e.g., if cache was
    // evicted)
    ASSERT_GE(second_cache_hits, 20)
        << "Second scan should have cache hits for most blocks";
  }
}

TEST_P(DBMultiScanIteratorTest, WastedBlocksTracking) {
  // Test that verifies wasted prefetch blocks are properly tracked.
  // When blocks are prefetched but skipped (e.g., due to seek), they should
  // be counted as wasted and recorded to MULTISCAN_PREFETCH_BLOCKS_WASTED.
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  table_options.block_cache = NewLRUCache(10 * 1024 * 1024);  // 10MB cache
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Create 100 keys, each in its own block
  const int kNumKeys = 100;
  std::string value(100, 'v');

  for (int i = 0; i < kNumKeys; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(3) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), value));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  // Reset the wasted blocks counter before test
  options.statistics->setTickerCount(MULTISCAN_PREFETCH_BLOCKS_WASTED, 0);

  // Set up MultiScan with two non-contiguous ranges:
  // Range 1: k000-k020 (20 keys/blocks)
  // Range 2: k050-k070 (20 keys/blocks)
  // The blocks between k020-k050 (30 blocks) should be wasted if prefetched
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.use_async_io = false;
  scan_options.insert("k000", "k020");
  scan_options.insert("k050", "k070");

  ReadOptions ro;
  ro.fill_cache = GetParam();

  {
    std::unique_ptr<MultiScan> iter =
        dbfull()->NewMultiScan(ro, cfh, scan_options);
    ASSERT_NE(iter, nullptr);

    int count = 0;
    try {
      for (auto range : *iter) {
        for (auto it : range) {
          it.first.ToString();
          count++;
        }
      }
    } catch (MultiScanException& ex) {
      FAIL() << "Scan failed: " << ex.what();
    }

    // We should have scanned 40 keys total (20 + 20)
    ASSERT_EQ(count, 40);
  }  // Iterator destroyed here, wasted blocks recorded

  // Check that wasted blocks were recorded
  // The exact count depends on how many blocks were prefetched between ranges
  uint64_t wasted =
      options.statistics->getTickerCount(MULTISCAN_PREFETCH_BLOCKS_WASTED);

  ASSERT_GT(wasted, 0);
}

TEST_P(DBMultiScanIteratorTest, PrefetchStatsRecorded) {
  // Regression test: the MultiScan prefetch stats must still be published after
  // the prefetch path moved into the IODispatcher. A cold scan populates the
  // prefetch counters; a warm re-scan registers cache hits.
  auto options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  table_options.block_cache = NewLRUCache(10 * 1024 * 1024);  // 10MB cache
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  const int kNumKeys = 50;
  std::string value(100, 'v');
  for (int i = 0; i < kNumKeys; ++i) {
    std::stringstream ss;
    ss << "k" << std::setw(3) << std::setfill('0') << i;
    ASSERT_OK(Put(ss.str(), value));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();

  auto run_scan = [&]() {
    MultiScanArgs scan_options(BytewiseComparator());
    scan_options.use_async_io = false;  // deterministic sync path
    scan_options.insert("k000", "k049");
    ReadOptions ro;
    ro.fill_cache = true;
    std::unique_ptr<MultiScan> iter =
        dbfull()->NewMultiScan(ro, cfh, scan_options);
    ASSERT_NE(iter, nullptr);
    if (iter == nullptr) {
      return;
    }
    int count = 0;
    try {
      for (auto range : *iter) {
        for (auto it : range) {
          it.first.ToString();
          count++;
        }
      }
    } catch (MultiScanException& ex) {
      FAIL() << "Scan failed: " << ex.what();
    }
    ASSERT_GT(count, 0);
  };

  // Cold scan: force blocks to be read from disk so the prefetch path runs.
  table_options.block_cache->EraseUnRefEntries();
  SetPerfLevel(kEnableCount);
  PerfContext* perf = get_perf_context();
  ASSERT_NE(perf, nullptr);
  if (perf == nullptr) {
    return;
  }
  perf->Reset();
  run_scan();  // MultiScanIndexIterator destroyed here -> stats recorded

  ASSERT_GT(TestGetTickerCount(options, MULTISCAN_PREPARE_CALLS), 0);
  ASSERT_GT(TestGetTickerCount(options, MULTISCAN_BLOCKS_PREFETCHED), 0);
  ASSERT_GT(TestGetTickerCount(options, MULTISCAN_PREFETCH_BYTES), 0);
  ASSERT_GT(TestGetTickerCount(options, MULTISCAN_IO_REQUESTS), 0);
  HistogramData blocks_per_prepare;
  options.statistics->histogramData(MULTISCAN_BLOCKS_PER_PREPARE,
                                    &blocks_per_prepare);
  ASSERT_GT(blocks_per_prepare.count, 0);

  // PerfContext mirrors the same metrics scoped to this operation/thread.
  ASSERT_GT(perf->multiscan_prepare_count, 0);
  ASSERT_GT(perf->multiscan_blocks_prefetched, 0);
  ASSERT_GT(perf->multiscan_prefetch_bytes, 0);
  ASSERT_GT(perf->multiscan_io_requests, 0);
  SetPerfLevel(kDisable);

  // Warm scan: the just-cached blocks now count as cache hits.
  uint64_t cache_hits_before =
      TestGetTickerCount(options, MULTISCAN_BLOCKS_FROM_CACHE);
  run_scan();
  ASSERT_GT(TestGetTickerCount(options, MULTISCAN_BLOCKS_FROM_CACHE),
            cache_hits_before);
}

class ReadPathRangeTombstoneTest : public DBIteratorBaseTest,
                                   public ::testing::WithParamInterface<bool> {
 protected:
  bool Forward() const { return GetParam(); }

  void SetUp() override {
    attempted_insert_ranges_.clear();
    SyncPoint::GetInstance()->SetCallBack(
        "MemTable::AddLogicallyRedundantRangeTombstone:AddRange",
        [this](void* arg) {
          auto* range = static_cast<std::pair<Slice, Slice>*>(arg);
          attempted_insert_ranges_.emplace_back(range->first.ToString(),
                                                range->second.ToString());
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  void SetupTestData(char first_key, char last_key,
                     const std::vector<std::string>& flushed_point_dels,
                     const std::vector<std::string>& memtable_point_dels,
                     const Slice* ts = nullptr) {
    for (char c = first_key; c <= last_key; c++) {
      if (ts) {
        ASSERT_OK(db_->Put(WriteOptions(), std::string(1, c), *ts,
                           std::string("v") + c));
      } else {
        ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
      }
    }
    ASSERT_OK(Flush());
    for (const auto& key : flushed_point_dels) {
      if (ts) {
        ASSERT_OK(db_->Delete(WriteOptions(), key, *ts));
      } else {
        ASSERT_OK(Delete(key));
      }
    }
    if (!flushed_point_dels.empty()) {
      ASSERT_OK(Flush());
    }
    for (const auto& key : memtable_point_dels) {
      if (ts) {
        ASSERT_OK(db_->Delete(WriteOptions(), key, *ts));
      } else {
        ASSERT_OK(Delete(key));
      }
    }
  }

  void AssertRange(size_t idx, const std::string& start,
                   const std::string& end) {
    ASSERT_LT(idx, attempted_insert_ranges_.size());
    ASSERT_EQ(attempted_insert_ranges_[idx].first, start);
    ASSERT_EQ(attempted_insert_ranges_[idx].second, end);
  }

  Slice MaxTimestamp(std::string* storage) const {
    storage->assign(sizeof(uint64_t), static_cast<char>(0xff));
    return Slice(*storage);
  }

  void VerifyIteration(const std::vector<std::string>& expected_keys,
                       const ReadOptions& ro = ReadOptions()) {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    if (Forward()) {
      iter->SeekToFirst();
      for (const auto& key : expected_keys) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(key, iter->key().ToString());
        iter->Next();
      }
    } else {
      iter->SeekToLast();
      for (auto it = expected_keys.rbegin(); it != expected_keys.rend(); ++it) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(*it, iter->key().ToString());
        iter->Prev();
      }
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  void AssertTableFilterRangeConversionRejected(
      const ReadOptions& ro, ColumnFamilyHandle* cfh = nullptr) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(
        ro, cfh != nullptr ? cfh : db_->DefaultColumnFamily()));
    ASSERT_TRUE(iter->status().IsInvalidArgument());
    ASSERT_TRUE(iter->status().ToString().find(
                    "table_filter is not supported") != std::string::npos)
        << iter->status().ToString();
  }

  std::vector<std::pair<std::string, std::string>> attempted_insert_ranges_;
};

INSTANTIATE_TEST_CASE_P(ReadPathRangeTombstoneTest, ReadPathRangeTombstoneTest,
                        ::testing::Bool());

TEST_P(ReadPathRangeTombstoneTest, BasicInsertion) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;

  bool forward = Forward();
  for (bool flush_before_read : {false, true}) {
    SCOPED_TRACE(flush_before_read ? "flush before read"
                                   : "no flush before read");
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);

    SetupTestData(/*first_key=*/'a', /*last_key=*/'n',
                  /*flushed_point_dels=*/{"b", "c", "d", "e", "f"},
                  /*memtable_point_dels=*/{"i", "j", "k", "l", "m"});

    if (flush_before_read) {
      ASSERT_OK(Flush());
      // After dropping the IsEmpty() fast-path in conversion, the now-empty
      // active memtable is a valid conversion target; the per-CF
      // ingest_sst_lock (held shared by ingestion) is the mechanism that
      // gates conversion vs ingestion, not memtable emptiness.
      attempted_insert_ranges_.clear();
      VerifyIteration({"a", "g", "h", "n"});
      ASSERT_EQ(attempted_insert_ranges_.size(), 2);
      if (forward) {
        AssertRange(0, "b", "g");
        AssertRange(1, "i", "n");
      } else {
        AssertRange(0, "i", "n");
        AssertRange(1, "b", "g");
      }
      break;
    }

    attempted_insert_ranges_.clear();

    VerifyIteration({"a", "g", "h", "n"});

    ASSERT_EQ(attempted_insert_ranges_.size(), 2);
    if (forward) {
      AssertRange(0, "b", "g");
      AssertRange(1, "i", "n");
    } else {
      AssertRange(0, "i", "n");
      AssertRange(1, "b", "g");
    }
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        2);

    attempted_insert_ranges_.clear();
    VerifyIteration({"a", "g", "h", "n"});
    ASSERT_EQ(attempted_insert_ranges_.size(), 0);
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        2);

    ASSERT_OK(Put("b", "b_new"));
    ASSERT_EQ(Get("b"), "b_new");
  }
}

TEST_P(ReadPathRangeTombstoneTest, NonContiguous) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  SetupTestData(/*first_key=*/'a', /*last_key=*/'h',
                /*flushed_point_dels=*/{"b", "c"},
                /*memtable_point_dels=*/{"e", "f"});

  VerifyIteration({"a", "d", "g", "h"});

  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);
}

TEST_P(ReadPathRangeTombstoneTest, MemtableSwitch) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  SetupTestData(/*first_key=*/'a', /*last_key=*/'h',
                /*flushed_point_dels=*/{},
                /*memtable_point_dels=*/{"b", "c", "d", "e", "f"});

  attempted_insert_ranges_.clear();
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::AddLogicallyRedundantRangeTombstone:AddRange",
      [this](void* arg) {
        auto* range = static_cast<std::pair<Slice, Slice>*>(arg);
        attempted_insert_ranges_.emplace_back(range->first.ToString(),
                                              range->second.ToString());
        auto* cfh =
            static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
        cfh->cfd()->mem()->MarkImmutable();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  VerifyIteration({"a", "g", "h"});

  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "g");
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_DISCARDED),
      1);

  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "NOT_FOUND");
  ASSERT_EQ(Get("g"), "vg");
}

TEST_P(ReadPathRangeTombstoneTest, ExhaustedIteratorWithBounds) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Keys a-j with tombstones at both ends: a-d deleted, g-j deleted.
  // Live keys: e, f.
  SetupTestData(/*first_key=*/'a', /*last_key=*/'j',
                /*flushed_point_dels=*/{"a", "b", "c", "d"},
                /*memtable_point_dels=*/{"g", "h", "i", "j"});

  ReadOptions ro;
  Slice lower("a");
  Slice upper("z");
  ro.iterate_lower_bound = &lower;
  ro.iterate_upper_bound = &upper;

  VerifyIteration({"e", "f"}, ro);

  // Both directions encounter two tombstone runs (a-d and g-j).
  ASSERT_EQ(attempted_insert_ranges_.size(), 2);
  if (Forward()) {
    // Forward: sees a-d tombstones first -> live e terminates -> [a, e).
    // Then g-j -> exhaustion past j, saved_key_="j" fallback -> [g, j),
    // covering g,h,i. j remains a point tombstone.
    AssertRange(0, "a", "e");
    AssertRange(1, "g", "j");
  } else {
    // Reverse: sees j-g tombstones first, then f,e live, then d-a -> exhausts.
    AssertRange(0, "g", "z");
    AssertRange(1, "a", "e");
  }
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      2);
  ASSERT_EQ(Get("e"), "ve");
  ASSERT_EQ(Get("f"), "vf");
  ASSERT_EQ(Get("a"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "NOT_FOUND");
  ASSERT_EQ(Get("g"), "NOT_FOUND");
  ASSERT_EQ(Get("j"), "NOT_FOUND");

  // Second read: range tombstones already in memtable, no new insertion.
  attempted_insert_ranges_.clear();
  VerifyIteration({"e", "f"}, ro);
  ASSERT_EQ(attempted_insert_ranges_.size(), 0);
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      2);
}

TEST_P(ReadPathRangeTombstoneTest, ExhaustedIteratorNoBounds) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Same data as ExhaustedIteratorWithBounds but no bounds set.
  SetupTestData(/*first_key=*/'a', /*last_key=*/'j',
                /*flushed_point_dels=*/{"a", "b", "c", "d"},
                /*memtable_point_dels=*/{"g", "h", "i", "j"});

  VerifyIteration({"e", "f"});

  if (Forward()) {
    // Forward: a-d run terminates at live e -> [a, e). g-j run exhausts
    // past j -> saved_key_ fallback -> [g, j), covering g,h,i with j as a
    // point tombstone.
    ASSERT_EQ(attempted_insert_ranges_.size(), 2);
    AssertRange(0, "a", "e");
    AssertRange(1, "g", "j");
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        2);
  } else {
    // Reverse fallback path remains prefix-gated.
    ASSERT_EQ(attempted_insert_ranges_.size(), 1);
    AssertRange(0, "a", "e");
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        1);
  }
}

TEST_P(ReadPathRangeTombstoneTest, DirectionChange) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  bool forward_first = Forward();
  SetupTestData(/*first_key=*/'a', /*last_key=*/'p',
                /*flushed_point_dels=*/{"c", "d", "e"},
                /*memtable_point_dels=*/{"f", "g", "j", "k", "l", "m", "n"});

  attempted_insert_ranges_.clear();

  {
    ReadOptions ro;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    if (forward_first) {
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("a", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("b", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("h", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("i", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("o", iter->key().ToString());
      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("i", iter->key().ToString());
    } else {
      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("p", iter->key().ToString());
      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("o", iter->key().ToString());
      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("i", iter->key().ToString());
      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("h", iter->key().ToString());
      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("b", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("h", iter->key().ToString());
    }
    ASSERT_OK(iter->status());
  }

  ASSERT_EQ(attempted_insert_ranges_.size(), 2);
  if (forward_first) {
    AssertRange(0, "c", "h");
    AssertRange(1, "j", "o");
  } else {
    AssertRange(0, "j", "o");
    AssertRange(1, "c", "h");
  }

  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      2);
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_DISCARDED),
      1);
}

TEST_P(ReadPathRangeTombstoneTest, MixedDeleteAndSingleDelete) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  SetupTestData(/*first_key=*/'a', /*last_key=*/'h',
                /*flushed_point_dels=*/{},
                /*memtable_point_dels=*/{"b", "d", "f"});

  ASSERT_OK(SingleDelete("c"));
  ASSERT_OK(SingleDelete("e"));

  attempted_insert_ranges_.clear();

  VerifyIteration({"a", "g", "h"});

  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "g");
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      1);

  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  ASSERT_EQ(Get("c"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "NOT_FOUND");
  ASSERT_EQ(Get("e"), "NOT_FOUND");
  ASSERT_EQ(Get("f"), "NOT_FOUND");
  ASSERT_EQ(Get("g"), "vg");
}

TEST_P(ReadPathRangeTombstoneTest, SingleDeleteOnlyRun) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  SetupTestData(/*first_key=*/'a', /*last_key=*/'h',
                /*flushed_point_dels=*/{}, /*memtable_point_dels=*/{});

  ASSERT_OK(SingleDelete("b"));
  ASSERT_OK(SingleDelete("c"));
  ASSERT_OK(SingleDelete("d"));
  ASSERT_OK(SingleDelete("e"));
  ASSERT_OK(SingleDelete("f"));

  attempted_insert_ranges_.clear();

  VerifyIteration({"a", "g", "h"});

  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "g");
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      1);
}

TEST_P(ReadPathRangeTombstoneTest, PrefixFilterDefaultReadOptions) {
  // total_order_seek=false (default) with prefix extractor. Even when the
  // visible scan contains a valid in-prefix tombstone run [ba, bc), read-path
  // range conversion is disabled in this legacy prefix mode, so no
  // converted memtable tombstone is inserted.
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // One live key is enough to terminate the same-prefix run [ba, bc).
  ASSERT_OK(Put("bc", "live_b"));
  ASSERT_OK(Flush());

  // Two point tombstones make [ba, bc) a valid same-prefix conversion
  // candidate, but default legacy prefix reads must still not materialize it.
  ASSERT_OK(Delete("ba"));
  ASSERT_OK(Delete("bb"));

  attempted_insert_ranges_.clear();
  auto it = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  if (Forward()) {
    it->Seek("b");
    while (it->Valid()) {
      it->Next();
    }
  } else {
    it->Seek("bc");
    ASSERT_TRUE(it->Valid());
    while (it->Valid()) {
      it->Prev();
    }
  }
  ASSERT_OK(it->status());
  ASSERT_EQ(attempted_insert_ranges_.size(), 0u);
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0u);
}

TEST_P(ReadPathRangeTombstoneTest, PrefixFilterTotalOrderSeek) {
  // total_order_seek=true disables prefix filtering -- all files visible.
  // The delete run spans from prefix 'b' into prefix 'c', terminated by
  // live key "cb" from L0. The tombstone [ba, cb) crosses prefix boundaries,
  // which is safe because total_order_seek makes all files visible.
  for (bool use_udt : {false, true}) {
    SCOPED_TRACE(use_udt ? "with UDT" : "without UDT");
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 2;
    options.statistics = CreateDBStatistics();
    options.disable_auto_compactions = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    if (use_udt) {
      options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    }
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    table_options.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    std::string ts;
    Slice ts_slice;
    std::string read_ts;
    Slice read_ts_slice;
    if (use_udt) {
      PutFixed64(&ts, 1);
      ts_slice = Slice(ts);
      read_ts_slice = MaxTimestamp(&read_ts);
    }

    auto put = [&](const std::string& k, const std::string& v) {
      return use_udt ? db_->Put(WriteOptions(), k, ts_slice, v) : Put(k, v);
    };
    auto del = [&](const std::string& k) {
      return use_udt ? db_->Delete(WriteOptions(), k, ts_slice) : Delete(k);
    };

    // L0: live key "cb" (prefix 'c').
    ASSERT_OK(put("cb", "live_value"));
    ASSERT_OK(Flush());

    // Memtable: contiguous deletes spanning prefixes 'b' and 'c'.
    // No live key between "bb" and "cb" -- the run crosses prefix boundaries.
    ASSERT_OK(put("aa", "below"));
    ASSERT_OK(del("ba"));
    ASSERT_OK(del("bb"));
    ASSERT_OK(del("ca"));
    ASSERT_OK(put("fa", "above"));

    attempted_insert_ranges_.clear();
    ReadOptions ro;
    ro.total_order_seek = true;
    if (use_udt) {
      ro.timestamp = &read_ts_slice;
    }
    auto it = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    if (Forward()) {
      it->Seek("b");
      while (it->Valid()) {
        it->Next();
      }
    } else {
      it->Seek("fa");
      ASSERT_TRUE(it->Valid());
      while (it->Valid()) {
        it->Prev();
      }
    }
    ASSERT_OK(it->status());
    // Tombstone crosses from prefix 'b' into 'c', terminated by live "cb".
    ASSERT_EQ(attempted_insert_ranges_.size(), 1u);
    if (use_udt) {
      AssertRange(0, std::string("ba") + ts, std::string("cb") + ts);
    } else {
      AssertRange(0, "ba", "cb");
    }
  }
}

TEST_P(ReadPathRangeTombstoneTest, PrefixFilterPrefixSameAsStart) {
  // prefix_same_as_start=true: prefix filtering active, DBIter bounds the
  // scan to the seek prefix. An out-of-prefix tombstone ends the visible run,
  // but the converted range still stays within prefix by flushing to the
  // last tracked in-prefix tombstone.
  // total_order_seek should not matter as we are guaranteed a total order view
  // within the prefix bounds.
  for (bool use_udt : {false, true}) {
    for (bool total_order : {false, true}) {
      SCOPED_TRACE(std::string(use_udt ? "UDT" : "no-UDT") + ", " +
                   (total_order ? "total_order" : "prefix_seek"));
      Options options = CurrentOptions();
      options.min_tombstones_for_range_conversion = 2;
      options.statistics = CreateDBStatistics();
      options.disable_auto_compactions = true;
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      if (use_udt) {
        options.comparator = test::BytewiseComparatorWithU64TsWrapper();
      }
      BlockBasedTableOptions table_options;
      table_options.filter_policy.reset(NewBloomFilterPolicy(10));
      table_options.whole_key_filtering = false;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      DestroyAndReopen(options);

      std::string ts;
      Slice ts_slice;
      std::string read_ts;
      Slice read_ts_slice;
      if (use_udt) {
        PutFixed64(&ts, 1);
        ts_slice = Slice(ts);
        read_ts_slice = MaxTimestamp(&read_ts);
      }

      auto put = [&](const std::string& k, const std::string& v) {
        return use_udt ? db_->Put(WriteOptions(), k, ts_slice, v) : Put(k, v);
      };
      auto del = [&](const std::string& k) {
        return use_udt ? db_->Delete(WriteOptions(), k, ts_slice) : Delete(k);
      };

      ASSERT_OK(put("cb", "live_value"));
      ASSERT_OK(Flush());

      ASSERT_OK(put("aa", "below"));
      ASSERT_OK(del("ba"));
      ASSERT_OK(del("bb"));
      ASSERT_OK(del("ca"));
      ASSERT_OK(put("fa", "above"));

      attempted_insert_ranges_.clear();
      ReadOptions ro;
      ro.prefix_same_as_start = true;
      ro.total_order_seek = total_order;
      if (use_udt) {
        ro.timestamp = &read_ts_slice;
      }
      auto it = std::unique_ptr<Iterator>(db_->NewIterator(ro));
      if (Forward()) {
        it->Seek("ba");
        while (it->Valid()) {
          it->Next();
        }
      } else {
        it->SeekForPrev("bb");
        ASSERT_FALSE(it->Valid());
      }
      ASSERT_OK(it->status());
      ASSERT_EQ(attempted_insert_ranges_.size(), 1u);
      ASSERT_EQ(options.statistics->getTickerCount(
                    READ_PATH_RANGE_TOMBSTONES_INSERTED),
                1u);
      if (use_udt) {
        const std::string end_ts =
            Forward() ? ts : std::string(sizeof(uint64_t), '\0');
        AssertRange(0, std::string("ba") + ts, std::string("bb") + end_ts);
      } else {
        AssertRange(0, "ba", "bb");
      }
      if (use_udt) {
        std::string val;
        ASSERT_OK(db_->Get(ro, db_->DefaultColumnFamily(), "cb", &val));
        ASSERT_EQ(val, "live_value");
      } else {
        ASSERT_EQ(Get("cb"), "live_value");
      }
    }
  }
}

TEST_P(ReadPathRangeTombstoneTest,
       PrefixFilterUpperBoundDoesNotCoverSkippedLiveKey) {
  if (!Forward()) {
    return;
  }

  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 3;
  options.statistics = CreateDBStatistics();
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Keep a live in-prefix key above the user upper bound in an SST so the
  // bounded prefix scan cannot return it, then leave a later same-prefix
  // memtable key available as the internal stop key. If cleanup uses that
  // late memtable key as the synthetic range end, it will cover "be".
  ASSERT_OK(Put("be", "live_b"));
  ASSERT_OK(Flush());

  ASSERT_OK(Delete("ba"));
  ASSERT_OK(Delete("bb"));
  ASSERT_OK(Delete("bc"));
  ASSERT_OK(Put("bz", "live_z"));

  attempted_insert_ranges_.clear();
  ReadOptions ro;
  ro.prefix_same_as_start = true;
  std::string upper_str = "bd";
  Slice upper(upper_str);
  ro.iterate_upper_bound = &upper;
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));

  std::vector<std::string> keys;
  for (iter->Seek("ba"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_TRUE(keys.empty());

  ASSERT_EQ(attempted_insert_ranges_.size(), 1u);

  ASSERT_EQ(Get("be"), "live_b");
}

TEST_P(ReadPathRangeTombstoneTest, PrefixFilterOutOfDomainSeek) {
  // With an out-of-domain seek target, prefix_same_as_start cannot establish
  // a seek-prefix bound. The iterator therefore behaves like an unrestricted
  // scan while still avoiding Transform() on the short seek target, so the
  // valid run [bbbb, bbdd) should be converted even though it spans multiple
  // distinct prefixes.
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  options.disable_auto_compactions = true;
  // FixedPrefixTransform(4): keys shorter than 4 bytes are out-of-domain.
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // All keys are in-domain (>= 4 bytes).
  ASSERT_OK(Put("aaaa", "v1"));
  ASSERT_OK(Delete("bbbb"));
  ASSERT_OK(Delete("bbcc"));
  ASSERT_OK(Delete("bbcd"));
  ASSERT_OK(Put("bbdd", "v2"));
  ASSERT_OK(Put("cccc", "v3"));

  attempted_insert_ranges_.clear();
  ReadOptions ro;
  ro.prefix_same_as_start = true;
  auto it = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  if (Forward()) {
    // Seek with a 1-byte key -- out-of-domain for FixedPrefixTransform(4).
    // prefix_same_as_start cannot set a prefix bound for this target.
    it->Seek("b");
    while (it->Valid()) {
      it->Next();
    }
  } else {
    it->Seek("c");
    ASSERT_TRUE(it->Valid());
    while (it->Valid()) {
      it->Prev();
    }
  }
  ASSERT_OK(it->status());
  ASSERT_EQ(attempted_insert_ranges_.size(), 1u);
  AssertRange(0, "bbbb", "bbdd");
}

TEST_P(ReadPathRangeTombstoneTest, TableFilterNotAllowed) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Base data lives in one SST.
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put("d", "vd"));
  ASSERT_OK(Flush());

  // Two point tombstones in a second SST are enough to trigger range
  // conversion during a subsequent unfiltered scan.
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  // Keep the active memtable non-empty so the read path has somewhere to store
  // the converted memtable range tombstone.
  ASSERT_OK(Put("zz", "tail_mem"));

  attempted_insert_ranges_.clear();
  {
    // First iterator sees the full SST set, so converting [a, c) into a
    // memtable range tombstone is safe.
    ReadOptions ro;
    auto it = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    it->SeekToFirst();
    while (it->Valid()) {
      it->Next();
    }
    ASSERT_OK(it->status());
  }

  ASSERT_GE(attempted_insert_ranges_.size(), 1u);
  AssertRange(0, "a", "c");

  {
    ReadOptions filtered_ro;
    std::function<bool(const TableProperties&)> filter =
        [](const TableProperties& props) { return props.num_entries != 2; };
    filtered_ro.table_filter = &filter;
    // Hiding the two-delete SST would otherwise leave this iterator with a
    // partial SST view plus the previously converted memtable tombstone,
    // allowing hidden SST state to affect the filtered read result.
    AssertTableFilterRangeConversionRejected(filtered_ro);
  }

  // Rejecting the filtered iterator preserves the correct full-DB view.
  VerifyIteration({"c", "d", "zz"});
}

TEST_P(ReadPathRangeTombstoneTest, SnapshotPredatesMemtable) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  SetupTestData(/*first_key=*/'a', /*last_key=*/'h',
                /*flushed_point_dels=*/{"b", "c", "d", "e", "f"},
                /*memtable_point_dels=*/{});

  const Snapshot* snap = db_->GetSnapshot();

  ASSERT_OK(Put("x", "vx"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("y", "vy"));

  attempted_insert_ranges_.clear();

  ReadOptions ro;
  ro.snapshot = snap;
  VerifyIteration({"a", "g", "h"}, ro);

  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);
  ASSERT_GT(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_DISCARDED),
      0);

  db_->ReleaseSnapshot(snap);
}

TEST_P(ReadPathRangeTombstoneTest, NoInsertionOnBlockCacheTierIncomplete) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  table_options.block_cache = NewLRUCache(1 << 20);
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  for (char c = 'a'; c <= 'h'; c++) {
    ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
  }
  ASSERT_OK(Flush());

  if (Forward()) {
    // Warm cache for a-g, leave h uncached.
    for (char c = 'a'; c <= 'g'; c++) {
      ASSERT_EQ(Get(std::string(1, c)), std::string("v") + c);
    }
    // Delete d-g (4 contiguous tombstones).
    for (char c = 'd'; c <= 'g'; c++) {
      ASSERT_OK(Delete(std::string(1, c)));
    }
  } else {
    // Warm cache for b-h, leave a uncached.
    for (char c = 'b'; c <= 'h'; c++) {
      ASSERT_EQ(Get(std::string(1, c)), std::string("v") + c);
    }
    // Delete b-e (4 contiguous tombstones).
    for (char c = 'b'; c <= 'e'; c++) {
      ASSERT_OK(Delete(std::string(1, c)));
    }
  }

  {
    ReadOptions ro;
    ro.read_tier = kBlockCacheTier;
    Slice upper("z");
    ro.iterate_upper_bound = &upper;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    if (Forward()) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      }
    } else {
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      }
    }
    ASSERT_TRUE(iter->status().IsIncomplete());
  }

  // No range tombstone should have been inserted despite meeting threshold:
  // conversion is disabled for kBlockCacheTier, and the iterator terminated
  // with Incomplete (cache miss).
  ASSERT_EQ(attempted_insert_ranges_.size(), 0);
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);

  // All keys must still be readable via normal Get.
  if (Forward()) {
    ASSERT_EQ(Get("h"), "vh");
  } else {
    ASSERT_EQ(Get("a"), "va");
  }
}

TEST_P(ReadPathRangeTombstoneTest, NoInsertionFromMemtableTierIterator) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // "c" is live and only exists in an SST file, which a kMemtableTier
  // iterator does not read.
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Put("e", "ve"));

  {
    ReadOptions ro;
    ro.read_tier = kMemtableTier;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    std::vector<std::string> seen;
    if (Forward()) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        seen.push_back(iter->key().ToString());
      }
    } else {
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        seen.insert(seen.begin(), iter->key().ToString());
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(seen, std::vector<std::string>({"a", "e"}));
  }

  ASSERT_EQ(Get("c"), "vc");
  ASSERT_EQ(attempted_insert_ranges_.size(), 0);
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(Get("c"), "vc");
}

TEST_P(ReadPathRangeTombstoneTest, PartialTierReadsAfterConversion) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  std::shared_ptr<Cache> block_cache = NewLRUCache(1 << 20);
  table_options.block_cache = block_cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  // Conversion starts disabled so the reads recorded before it do not insert.
  DestroyAndReopen(options);

  for (char c = 'a'; c <= 'h'; c++) {
    ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", "va2"));
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Put("e", "ve2"));
  ASSERT_OK(Delete("e"));

  auto scan = [&](ReadTier tier) {
    ReadOptions ro;
    ro.read_tier = tier;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    std::vector<std::string> out;
    if (Forward()) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        out.push_back(iter->key().ToString() + "=" + iter->value().ToString());
      }
    } else {
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        out.insert(out.begin(),
                   iter->key().ToString() + "=" + iter->value().ToString());
      }
    }
    out.push_back(iter->status().ToString());
    return out;
  };
  // kMemtableTier only affects iterators, so point reads compare
  // kBlockCacheTier with kReadAllTier.
  auto gets = [&](ReadTier tier) {
    ReadOptions ro;
    ro.read_tier = tier;
    std::vector<std::string> out;
    for (char c = 'a'; c <= 'h'; c++) {
      std::string value;
      Status s = db_->Get(ro, std::string(1, c), &value);
      out.push_back(std::string(1, c) + ":" + (s.ok() ? value : s.ToString()));
    }
    return out;
  };

  const auto cold_cache_scan_before = scan(kBlockCacheTier);
  const auto cold_cache_gets_before = gets(kBlockCacheTier);
  const std::string incomplete =
      Status::Incomplete("no blocking io").ToString();
  ASSERT_EQ(cold_cache_scan_before.back(), incomplete);
  ASSERT_EQ(cold_cache_gets_before[1], "b:" + incomplete);
  ASSERT_EQ(cold_cache_gets_before[2], "c:" + incomplete);

  // A full scan with conversion disabled reads every block into the cache.
  const std::vector<std::string> full_view = {"a=va2", "f=vf", "g=vg", "h=vh",
                                              "OK"};
  ASSERT_EQ(scan(kReadAllTier), full_view);
  ASSERT_EQ(attempted_insert_ranges_.size(), 0);

  const auto mem_scan_before = scan(kMemtableTier);
  ASSERT_EQ(mem_scan_before, std::vector<std::string>({"a=va2", "OK"}));
  const auto warm_cache_scan_before = scan(kBlockCacheTier);
  ASSERT_EQ(warm_cache_scan_before, full_view);
  const auto warm_cache_gets_before = gets(kBlockCacheTier);
  const auto full_gets = gets(kReadAllTier);
  ASSERT_EQ(warm_cache_gets_before, full_gets);

  ASSERT_OK(db_->SetOptions({{"min_tombstones_for_range_conversion", "2"}}));
  // A complete kBlockCacheTier scan does not convert. Only kReadAllTier does.
  ASSERT_EQ(scan(kBlockCacheTier), warm_cache_scan_before);
  ASSERT_EQ(attempted_insert_ranges_.size(), 0);
  ASSERT_EQ(scan(kReadAllTier), full_view);
  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "f");
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      1);

  ASSERT_EQ(scan(kMemtableTier), mem_scan_before);
  ASSERT_EQ(scan(kBlockCacheTier), warm_cache_scan_before);
  ASSERT_EQ(gets(kBlockCacheTier), warm_cache_gets_before);

  // With a cold cache, Gets of the covered keys "b" and "c" now stop at the
  // memtable range tombstone and return the full-tier NotFound instead of
  // Incomplete. No read returns a different value.
  block_cache->EraseUnRefEntries();
  ASSERT_EQ(scan(kBlockCacheTier), cold_cache_scan_before);
  auto expected_cold_cache_gets = cold_cache_gets_before;
  expected_cold_cache_gets[1] = full_gets[1];
  expected_cold_cache_gets[2] = full_gets[2];
  ASSERT_EQ(gets(kBlockCacheTier), expected_cold_cache_gets);
}

TEST_P(ReadPathRangeTombstoneTest, SkipInsertionWhenCoveredByExistingRange) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  for (char c = 'a'; c <= 'h'; c++) {
    ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "c", "h"));

  for (char c = 'd'; c <= 'g'; c++) {
    ASSERT_OK(Delete(std::string(1, c)));
  }

  attempted_insert_ranges_.clear();
  ReadOptions ro;
  Slice upper("z");
  ro.iterate_upper_bound = &upper;
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  if (Forward()) {
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    }
  } else {
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    }
  }
  ASSERT_OK(iter->status());

  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);
  ASSERT_GT(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_DISCARDED),
      0);
}

// Verifies that range tombstone insertion works correctly with user-defined
// timestamps (UDT) when the read timestamp has full visibility. With UDT, keys
// include an 8-byte timestamp suffix, so the comparator, Put/Delete APIs, and
// ReadOptions all require timestamps. This test keeps the optimization enabled
// by reading at the max timestamp.
TEST_P(ReadPathRangeTombstoneTest, UDTBasicScan) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  std::string ts;
  PutFixed64(&ts, 1);
  Slice ts_slice(ts);
  std::string read_ts;
  Slice read_ts_slice = MaxTimestamp(&read_ts);
  SetupTestData('a', 'h', /*flushed_point_dels=*/{},
                /*memtable_point_dels=*/{"b", "c", "d", "e", "f"}, &ts_slice);

  ReadOptions ro;
  ro.timestamp = &read_ts_slice;
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  std::vector<std::string> keys;
  if (Forward()) {
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
  } else {
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      keys.push_back(iter->key().ToString());
    }
    std::reverse(keys.begin(), keys.end());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(keys, (std::vector<std::string>{"a", "g", "h"}));

  // Range covers [b+ts, g+ts).
  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, std::string("b") + ts, std::string("g") + ts);
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      1);
}

// Regression test: an older UDT read timestamp can hide newer live versions
// inside a delete run. Range conversion must stay disabled in that case, or
// the converted range tombstone will incorrectly hide those newer versions
// for later max-timestamp reads.
TEST_P(ReadPathRangeTombstoneTest, UDTOlderTimestampDisablesInsertion) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  std::string ts1;
  std::string ts2;
  std::string ts3;
  PutFixed64(&ts1, 1);
  PutFixed64(&ts2, 2);
  PutFixed64(&ts3, 3);
  Slice ts1_slice(ts1);
  Slice ts2_slice(ts2);
  Slice ts3_slice(ts3);
  std::string max_ts;
  Slice max_ts_slice = MaxTimestamp(&max_ts);

  ASSERT_OK(db_->Put(WriteOptions(), "a", ts1_slice, "va1"));
  ASSERT_OK(db_->Put(WriteOptions(), "c", ts1_slice, "vc1"));
  ASSERT_OK(db_->Put(WriteOptions(), "d", ts1_slice, "vd1"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Delete(WriteOptions(), "a", ts2_slice));
  ASSERT_OK(db_->Delete(WriteOptions(), "c", ts2_slice));

  ASSERT_OK(db_->Put(WriteOptions(), "a", ts3_slice, "va3"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", ts3_slice, "vb3"));
  ASSERT_OK(db_->Put(WriteOptions(), "c", ts3_slice, "vc3"));

  attempted_insert_ranges_.clear();
  ReadOptions old_ro;
  old_ro.timestamp = &ts2_slice;
  VerifyIteration({"d"}, old_ro);

  ASSERT_EQ(attempted_insert_ranges_.size(), 0u);
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);

  ReadOptions latest_ro;
  latest_ro.timestamp = &max_ts_slice;
  VerifyIteration({"a", "b", "c", "d"}, latest_ro);
}

// When UDT is enabled and iteration exhausts with tombstones at the boundary,
// range tombstone insertion should still work if the read sees all timestamps.
// For forward exhaustion, the iterate_upper_bound is padded with the min
// timestamp to form a valid end key. For reverse exhaustion, the end key comes
// from the next live key which already has the timestamp suffix.
TEST_P(ReadPathRangeTombstoneTest, ExhaustedWithUDT) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 4;
  options.statistics = CreateDBStatistics();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  std::string ts;
  PutFixed64(&ts, 1);
  Slice ts_slice(ts);
  std::string read_ts;
  Slice read_ts_slice = MaxTimestamp(&read_ts);
  std::string min_ts(sizeof(uint64_t), '\0');

  // Forward: tombstones at end (e-h), needs upper bound for end key.
  // Reverse: tombstones at start (a-d), end key from live key "e".
  if (Forward()) {
    SetupTestData('a', 'h', /*flushed_point_dels=*/{},
                  /*memtable_point_dels=*/{"e", "f", "g", "h"}, &ts_slice);
  } else {
    SetupTestData('a', 'h', /*flushed_point_dels=*/{},
                  /*memtable_point_dels=*/{"a", "b", "c", "d"}, &ts_slice);
  }

  ReadOptions ro;
  ro.timestamp = &read_ts_slice;
  std::string upper_str = "z";
  Slice upper(upper_str);
  if (Forward()) {
    ro.iterate_upper_bound = &upper;
  }
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  std::vector<std::string> keys;
  if (Forward()) {
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
  } else {
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      keys.push_back(iter->key().ToString());
    }
    std::reverse(keys.begin(), keys.end());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(keys.size(), 4);

  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  if (Forward()) {
    // Forward exhaustion past h: saved_key_="h"+ts fallback -> [e+ts, h+ts).
    AssertRange(0, std::string("e") + ts, std::string("h") + ts);
  } else {
    // Reverse exhaustion: range is [a+ts, e+ts).
    AssertRange(0, std::string("a") + ts, std::string("e") + ts);
  }
  ASSERT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      1);
}

TEST_P(ReadPathRangeTombstoneTest, SeekForPrevTombstone) {
  // SeekForPrev lands directly on tombstones. No upper bound. Forward
  // exhaustion past h falls back to saved_key_="h" -> [e, h), covering
  // e,f,g with h as a point tombstone.
  // Reverse: SeekForPrev("h") -> Delete(h,g,f,e) -> live "d" -> range [e, h).
  for (bool use_udt : {false, true}) {
    SCOPED_TRACE(use_udt ? "with UDT" : "without UDT");
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 4;
    options.statistics = CreateDBStatistics();
    if (use_udt) {
      options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    }
    DestroyAndReopen(options);

    std::string ts;
    Slice ts_slice;
    std::string read_ts;
    Slice read_ts_slice;
    Slice* ts_ptr = nullptr;
    if (use_udt) {
      PutFixed64(&ts, 1);
      ts_slice = Slice(ts);
      read_ts_slice = MaxTimestamp(&read_ts);
      ts_ptr = &ts_slice;
    }
    SetupTestData('a', 'h', /*flushed_point_dels=*/{},
                  /*memtable_point_dels=*/{"e", "f", "g", "h"}, ts_ptr);
    attempted_insert_ranges_.clear();

    ReadOptions ro;
    if (use_udt) {
      ro.timestamp = &read_ts_slice;
    }
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));

    std::vector<std::string> keys;
    if (Forward()) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        keys.push_back(iter->key().ToString());
      }
    } else {
      // SeekForPrev("h") lands on Delete(h), traverses g,f,e -> finds "d".
      for (iter->SeekForPrev("h"); iter->Valid(); iter->Prev()) {
        keys.push_back(iter->key().ToString());
      }
      std::reverse(keys.begin(), keys.end());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys, (std::vector<std::string>{"a", "b", "c", "d"}));

    ASSERT_EQ(attempted_insert_ranges_.size(), 1);
    if (Forward()) {
      // Forward exhausts past h -> saved_key_="h" fallback -> [e, h),
      // covering e,f,g with h as a point tombstone.
      if (use_udt) {
        AssertRange(0, std::string("e") + ts, std::string("h") + ts);
      } else {
        AssertRange(0, "e", "h");
      }
    } else {
      if (use_udt) {
        std::string min_ts(sizeof(uint64_t), '\0');
        AssertRange(0, std::string("e") + ts, std::string("h") + min_ts);
      } else {
        AssertRange(0, "e", "h");
      }
    }
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        1);
  }
}

TEST_P(ReadPathRangeTombstoneTest, UpperBoundTombstone) {
  // iterate_upper_bound lands past the data. Both directions see tombstones
  // e-h. Forward exhausts naturally past h (no key in DB >= upper) so the
  // forward path falls back to saved_key_ ("h") as the exclusive end_key,
  // covering n-1 of n deletes -- [e, h). The remaining tombstone for h
  // stays as a point delete. Reverse captures the live key "d" before the
  // run and uses it via range_tomb_end_key_ -- [e, i) when SeekToLast
  // delegates to SeekForPrev("i").
  for (bool use_udt : {false, true}) {
    SCOPED_TRACE(use_udt ? "with UDT" : "without UDT");
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 4;
    options.statistics = CreateDBStatistics();
    if (use_udt) {
      options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    }
    DestroyAndReopen(options);

    std::string ts;
    Slice ts_slice;
    std::string read_ts;
    Slice read_ts_slice;
    Slice* ts_ptr = nullptr;
    if (use_udt) {
      PutFixed64(&ts, 1);
      ts_slice = Slice(ts);
      read_ts_slice = MaxTimestamp(&read_ts);
      ts_ptr = &ts_slice;
    }
    SetupTestData('a', 'h', /*flushed_point_dels=*/{},
                  /*memtable_point_dels=*/{"e", "f", "g", "h"}, ts_ptr);
    attempted_insert_ranges_.clear();

    ReadOptions ro;
    if (use_udt) {
      ro.timestamp = &read_ts_slice;
    }
    std::string upper_str = "i";
    Slice upper(upper_str);
    ro.iterate_upper_bound = &upper;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));

    std::vector<std::string> keys;
    if (Forward()) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        keys.push_back(iter->key().ToString());
      }
    } else {
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        keys.push_back(iter->key().ToString());
      }
      std::reverse(keys.begin(), keys.end());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys, (std::vector<std::string>{"a", "b", "c", "d"}));

    ASSERT_EQ(attempted_insert_ranges_.size(), 1);
    if (use_udt) {
      if (Forward()) {
        // Forward end key: saved_key_ ("h") with the tombstone's own ts.
        AssertRange(0, std::string("e") + ts, std::string("h") + ts);
      } else {
        // Reverse end key: upper bound padded with max_ts (via
        // SetSavedKeyToSeekForPrevTarget).
        std::string end_ts(sizeof(uint64_t), '\xff');
        AssertRange(0, std::string("e") + ts, std::string("i") + end_ts);
      }
    } else {
      AssertRange(0, "e", Forward() ? "h" : "i");
    }
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        1);
  }
}

TEST_P(ReadPathRangeTombstoneTest, LowerBoundTruncatesReverse) {
  // Keys a-j, delete a-h. Lower bound "e" truncates reverse iteration
  // mid-tombstone-run. Forward: tombstones e-h ended by live key i -> [e, i).
  // Reverse: tombstones h,g,f,e then lower_bound hit -> flush [e, i).
  for (bool use_udt : {false, true}) {
    SCOPED_TRACE(use_udt ? "with UDT" : "without UDT");
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 4;
    options.statistics = CreateDBStatistics();
    if (use_udt) {
      options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    }
    DestroyAndReopen(options);

    std::string ts;
    Slice ts_slice;
    std::string read_ts;
    Slice read_ts_slice;
    Slice* ts_ptr = nullptr;
    if (use_udt) {
      PutFixed64(&ts, 1);
      ts_slice = Slice(ts);
      read_ts_slice = MaxTimestamp(&read_ts);
      ts_ptr = &ts_slice;
    }
    SetupTestData(
        'a', 'j', /*flushed_point_dels=*/{},
        /*memtable_point_dels=*/{"a", "b", "c", "d", "e", "f", "g", "h"},
        ts_ptr);
    attempted_insert_ranges_.clear();

    ReadOptions ro;
    if (use_udt) {
      ro.timestamp = &read_ts_slice;
    }
    std::string lower_str = "e";
    Slice lower(lower_str);
    ro.iterate_lower_bound = &lower;
    std::string upper_str = "z";
    Slice upper(upper_str);
    ro.iterate_upper_bound = &upper;

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    std::vector<std::string> keys;
    if (Forward()) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        keys.push_back(iter->key().ToString());
      }
    } else {
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        keys.push_back(iter->key().ToString());
      }
      std::reverse(keys.begin(), keys.end());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys, (std::vector<std::string>{"i", "j"}));

    // Both directions produce one range covering tombstones e-h.
    ASSERT_EQ(attempted_insert_ranges_.size(), 1);
    if (use_udt) {
      AssertRange(0, std::string("e") + ts, std::string("i") + ts);
    } else {
      AssertRange(0, "e", "i");
    }
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        1);
  }
}

// Regression test: a delete run discovered through the reseek path must be
// materialized at the reader's sequence so older snapshots keep seeing the
// pre-delete values.
TEST_P(ReadPathRangeTombstoneTest,
       ReseekDiscoveredDeleteRunPreservesOlderSnapshots) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  // Low skip threshold forces FindValueForCurrentKeyUsingSeek for keys with
  // many versions.
  options.max_sequential_skip_in_iterations = 2;
  DestroyAndReopen(options);

  // Put keys a-d, flush so they get low sequence numbers.
  //   seq 1: Put(a)  seq 2: Put(b)  seq 3: Put(c)  seq 4: Put(d)
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put("d", "vd"));
  ASSERT_OK(Flush());

  // Give both b and c extra versions so FindValueForCurrentKey triggers the
  // reseek path for each.
  //   seq 5-7: Put(b) x3     seq 8-10: Put(c) x3
  ASSERT_OK(Put("b", "b_v2"));
  ASSERT_OK(Put("b", "b_v3"));
  ASSERT_OK(Put("b", "b_v4"));
  ASSERT_OK(Put("c", "c_v2"));
  ASSERT_OK(Put("c", "c_v3"));
  ASSERT_OK(Put("c", "c_v4"));

  // Snapshot BEFORE the deletes. At this snapshot b and c are live.
  const Snapshot* snap = db_->GetSnapshot();

  // Delete b and c (2 tombstones = threshold).
  //   seq 11: Del(b)   seq 12: Del(c)
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Delete("c"));

  // Iterate at the latest sequence. Both b and c hit the reseek path and
  // convert a range tombstone [b, d) for later readers at the same seq.
  attempted_insert_ranges_.clear();
  VerifyIteration({"a", "d"});

  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "d");

  // Read with the earlier snapshot. The point deletes (seq 11-12) are not
  // visible, so b and c must remain live after the latest iterator ran.
  ReadOptions ro;
  ro.snapshot = snap;
  VerifyIteration({"a", "b", "c", "d"}, ro);

  db_->ReleaseSnapshot(snap);
}

// Regression test: keys written entirely after the snapshot do not break a
// tombstone run discovered by reverse iteration.
TEST_P(ReadPathRangeTombstoneTest, InvisibleKeysDontBreakTombstoneRun) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Base keys: a, b, d, f.
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("d", "vd"));
  ASSERT_OK(Put("f", "vf"));
  ASSERT_OK(Flush());

  // Delete b, d -- visible tombstones.
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Delete("d"));

  // Snapshot S.  At S: a(live), b(del), d(del), f(live).
  const Snapshot* snap = db_->GetSnapshot();

  // Write c, e AFTER the snapshot -- invisible at S.
  // At S the iterator sees: a(live), b(del), c(invis), d(del),
  //   e(invis), f(live).
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put("e", "ve"));

  ReadOptions ro;
  ro.snapshot = snap;
  attempted_insert_ranges_.clear();
  VerifyIteration({"a", "f"}, ro);

  // Invisible keys c and e should not break the tombstone run.
  // 2 tombstones (b, d) >= threshold -> range [b, f).
  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "f");

  db_->ReleaseSnapshot(snap);
}

// Regression test: a converted tombstone can land in the current memtable at
// an older snapshot sequence than a live point already ingested into an older
// L0 file. Latest point lookups must continue searching that older file when
// its sequence range can still contain a newer point version.
TEST_P(ReadPathRangeTombstoneTest, NewerPointInOlderFileStillVisible) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put("d", "vd"));
  ASSERT_OK(Flush());

  ASSERT_OK(Delete("b"));
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Flush());

  // Keep the active memtable older than the snapshot so the read path is
  // allowed to convert a tombstone into it later.
  ASSERT_OK(Put("z", "vz_anchor"));
  const Snapshot* snap = db_->GetSnapshot();

  const std::string ingest_file = dbname_ + "_live_c.sst";
  {
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(ingest_file));
    ASSERT_OK(writer.Put("c", "vc_live"));
    ASSERT_OK(writer.Finish());
  }

  IngestExternalFileOptions ifo;
  ifo.allow_blocking_flush = false;
  ASSERT_OK(db_->IngestExternalFile({ingest_file}, ifo));
  ASSERT_EQ(Get("c"), "vc_live");

  attempted_insert_ranges_.clear();
  ReadOptions snap_ro;
  snap_ro.snapshot = snap;
  VerifyIteration({"d", "z"}, snap_ro);

  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "d");

  const Snapshot* latest = db_->GetSnapshot();
  ASSERT_EQ(Get("c", latest), "vc_live");
  ASSERT_EQ(MultiGet({"c"}, latest), (std::vector<std::string>{"vc_live"}));

  db_->ReleaseSnapshot(latest);
  db_->ReleaseSnapshot(snap);
}

// Regression test: SeekToLast() after Seek() must clear stale saved_key_ to
// avoid corrupting range tombstone tracking bounds.
TEST_P(ReadPathRangeTombstoneTest, SeekToLastStaleSavedKey) {
  if (Forward()) {
    return;
  }

  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  for (char c = 'a'; c <= 'z'; c++) {
    ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(Delete("x"));
  ASSERT_OK(Delete("y"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

  // Seek populates saved_key_ with "a".
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->key().ToString());

  // SeekToLast must not carry the stale saved_key_ into range tombstone bounds.
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("z", iter->key().ToString());

  // Reverse iteration skips deleted x and y.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("w", iter->key().ToString());
}

TEST_P(ReadPathRangeTombstoneTest, SeekToLastTombstones) {
  if (Forward()) {
    ROCKSDB_GTEST_BYPASS(
        "SeekToLast tombstone materialization is reverse-only.");
  }

  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  for (char c = 'a'; c <= 'z'; c++) {
    ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(Delete("x"));
  ASSERT_OK(Delete("y"));
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  attempted_insert_ranges_.clear();

  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->key().ToString());

  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("z", iter->key().ToString());
  ASSERT_EQ(attempted_insert_ranges_.size(), 0u);

  // Reverse iteration skips deleted x and y.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("w", iter->key().ToString());
  ASSERT_EQ(attempted_insert_ranges_.size(), 1u);
  AssertRange(0, "x", "z");
}

// Regression test for a crash-test pattern where the interior between two
// point tombstones is hidden by a later DeleteRange. A latest iterator may
// still convert a redundant range tombstone, but an older snapshot must
// continue to see the pre-DeleteRange live key after iteration.
TEST_P(ReadPathRangeTombstoneTest,
       RangeDeletedInteriorPreservesOlderSnapshots) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  for (char c = 'a'; c <= 'n'; ++c) {
    ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
  }
  ASSERT_OK(Flush());

  // Point tombstones at the boundaries of the future range-deleted interior.
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Delete("m"));

  const Snapshot* snap = db_->GetSnapshot();
  ReadOptions snap_ro;
  snap_ro.snapshot = snap;
  std::string snap_value;
  ASSERT_OK(db_->Get(snap_ro, "c", &snap_value));
  ASSERT_EQ(snap_value, "vc");

  // Latest readers now see c-l deleted by range tombstone, but the older
  // snapshot above must still see them live.
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "c", "m"));

  attempted_insert_ranges_.clear();
  VerifyIteration({"a", "n"});

  // Materialize the redundant tombstone at the latest read sequence only.
  ASSERT_EQ(attempted_insert_ranges_.size(), 1);
  AssertRange(0, "b", "n");

  snap_value.clear();
  ASSERT_OK(db_->Get(snap_ro, "c", &snap_value));
  ASSERT_EQ(snap_value, "vc");

  db_->ReleaseSnapshot(snap);
}

// An ingested file that gets no new sequence number is invisible to an
// iterator created before the ingestion. That iterator must not convert the
// deleted keys around the file into a range tombstone that covers it.
TEST_P(ReadPathRangeTombstoneTest, IngestedFileSurvivesOlderIterator) {
  enum class IngestMode { kSeqnoZero, kIngestBehind, kDbGenerated };
  for (IngestMode mode : {IngestMode::kSeqnoZero, IngestMode::kIngestBehind,
                          IngestMode::kDbGenerated}) {
    SCOPED_TRACE("ingest mode " + std::to_string(static_cast<int>(mode)));
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 2;
    options.disable_auto_compactions = true;
    options.cf_allow_ingest_behind = mode == IngestMode::kIngestBehind;
    DestroyAndReopen(options);

    std::string file = dbname_ + "/ingest_c.sst";
    SequenceNumber expected_file_seqno = 0;
    if (mode == IngestMode::kDbGenerated) {
      const std::string src_path = dbname_ + "_src";
      ASSERT_OK(DestroyDB(src_path, options));
      std::unique_ptr<DB> src;
      ASSERT_OK(DB::Open(options, src_path, &src));
      ASSERT_OK(src->Put(WriteOptions(), "c", "vc"));
      ASSERT_OK(src->Flush(FlushOptions()));
      std::vector<LiveFileMetaData> src_files;
      src->GetLiveFilesMetaData(&src_files);
      ASSERT_EQ(src_files.size(), 1);
      expected_file_seqno = src_files[0].largest_seqno;
      ASSERT_GT(expected_file_seqno, 0);
      file = src_files[0].directory + "/" + src_files[0].relative_filename;
      ASSERT_OK(src->Close());
    } else {
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(file));
      ASSERT_OK(writer.Put("c", "vc"));
      ASSERT_OK(writer.Finish());
    }

    for (const char* key : {"a", "b", "d", "e", "f", "g"}) {
      ASSERT_OK(Put(key, std::string("v") + key));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(Delete("b"));
    ASSERT_OK(Delete("d"));
    // The iterator sequence number must be above the DB-generated file's.
    ASSERT_OK(Delete("f"));
    ASSERT_GT(db_->GetLatestSequenceNumber(), expected_file_seqno);

    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

    // "c" falls between the deleted keys and overlaps no key in the DB.
    IngestExternalFileOptions ifo;
    ifo.ingest_behind = mode == IngestMode::kIngestBehind;
    ifo.allow_db_generated_files = mode == IngestMode::kDbGenerated;
    ASSERT_OK(db_->IngestExternalFile({file}, ifo));
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    bool found = false;
    for (const auto& f : files) {
      if (f.smallestkey == "c") {
        found = true;
        ASSERT_EQ(f.level, options.num_levels - 1);
        ASSERT_EQ(f.largest_seqno, expected_file_seqno);
      }
    }
    ASSERT_TRUE(found);
    ASSERT_EQ(Get("c"), "vc");

    // The iterator predates the ingestion, so it sees b and d as one run of
    // deleted keys.
    attempted_insert_ranges_.clear();
    if (Forward()) {
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "a");
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "e");
    } else {
      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "g");
      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "e");
      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "a");
    }
    ASSERT_OK(iter->status());
    iter.reset();
    ASSERT_EQ(attempted_insert_ranges_.size(), 1);
    AssertRange(0, "b", "e");

    EXPECT_EQ(Get("c"), "vc");
    VerifyIteration({"a", "c", "e", "g"});
    ASSERT_OK(Flush());
    Reopen(options);
    EXPECT_EQ(Get("c"), "vc");
  }
}

// NewIterator() references the SuperVersion before it reads the sequence
// number. An ingestion plus a write in between gives the iterator a sequence
// number above the ingested files' while its SuperVersion lacks them. It must
// not convert the deleted keys around those files into a range tombstone.
TEST_P(ReadPathRangeTombstoneTest,
       IngestedFileSurvivesIteratorWithOlderSuperVersion) {
  for (bool new_seqno : {false, true}) {
    SCOPED_TRACE(new_seqno ? "ingested with a new seqno"
                           : "ingested with seqno zero");
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 2;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);

    for (const char* key : {"a", "b", "d", "e"}) {
      ASSERT_OK(Put(key, std::string("v") + key));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(Delete("b"));
    ASSERT_OK(Delete("d"));
    // The memtable stays empty, so the ingestion does not switch it.
    ASSERT_OK(Flush());

    // "c" alone overlaps no key, so it gets seqno zero. Adding "b" overlaps
    // the deleted "b" in L0, so the file gets a new seqno.
    const std::string file = dbname_ + "/ingest.sst";
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(file));
    if (new_seqno) {
      ASSERT_OK(writer.Put("b", "vb2"));
    }
    ASSERT_OK(writer.Put("c", "vc"));
    ASSERT_OK(writer.Finish());

    SyncPoint::GetInstance()->LoadDependency(
        {{"DBImpl::NewIterator:1",
          "ReadPathRangeTombstoneTest::OlderSuperVersion:Ingest"},
         {"ReadPathRangeTombstoneTest::OlderSuperVersion:Written",
          "DBImpl::NewIterator:2"}});
    std::unique_ptr<Iterator> iter;
    port::Thread reader([&]() { iter.reset(db_->NewIterator(ReadOptions())); });
    TEST_SYNC_POINT("ReadPathRangeTombstoneTest::OlderSuperVersion:Ingest");
    ASSERT_OK(db_->IngestExternalFile({file}, IngestExternalFileOptions()));
    ASSERT_OK(Put("z", "vz"));
    TEST_SYNC_POINT("ReadPathRangeTombstoneTest::OlderSuperVersion:Written");
    reader.join();
    SyncPoint::GetInstance()->ClearTrace();
    SyncPoint::GetInstance()->LoadDependency({});

    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    bool found = false;
    for (const auto& f : files) {
      if (f.largestkey == "c") {
        found = true;
        ASSERT_EQ(f.largest_seqno > 0, new_seqno);
      }
    }
    ASSERT_TRUE(found);

    // The iterator reads past the write of "z" but not the ingested file.
    attempted_insert_ranges_.clear();
    std::vector<std::string> seen;
    if (Forward()) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        seen.push_back(iter->key().ToString());
      }
    } else {
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        seen.insert(seen.begin(), iter->key().ToString());
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(seen, std::vector<std::string>({"a", "e", "z"}));
    iter.reset();
    ASSERT_EQ(attempted_insert_ranges_.size(), 1);
    AssertRange(0, "b", "e");

    EXPECT_EQ(Get("c"), "vc");
    EXPECT_EQ(Get("b"), new_seqno ? "vb2" : "NOT_FOUND");
    ASSERT_OK(Flush());
    Reopen(options);
    EXPECT_EQ(Get("c"), "vc");
  }
}

// One IngestExternalFiles() call installs files in two column families. Each
// column family's older iterator must refuse its conversion.
TEST_P(ReadPathRangeTombstoneTest, AtomicIngestionSurvivesOlderIterators) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<std::string> files;
  for (int cf = 0; cf < 2; cf++) {
    files.push_back(dbname_ + "/ingest_c" + std::to_string(cf) + ".sst");
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(files.back()));
    ASSERT_OK(writer.Put("c", "vc"));
    ASSERT_OK(writer.Finish());
    for (const char* key : {"a", "b", "d", "e"}) {
      ASSERT_OK(Put(cf, key, std::string("v") + key));
    }
    ASSERT_OK(Flush(cf));
    ASSERT_OK(Delete(cf, "b"));
    ASSERT_OK(Delete(cf, "d"));
  }
  std::unique_ptr<Iterator> it0(db_->NewIterator(ReadOptions(), handles_[0]));
  std::unique_ptr<Iterator> it1(db_->NewIterator(ReadOptions(), handles_[1]));
  std::vector<IngestExternalFileArg> args(2);
  for (int cf = 0; cf < 2; cf++) {
    args[cf].column_family = handles_[cf];
    args[cf].external_files = {files[cf]};
  }
  ASSERT_OK(db_->IngestExternalFiles(args));

  attempted_insert_ranges_.clear();
  for (auto* it : {it0.get(), it1.get()}) {
    if (Forward()) {
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
      }
    } else {
      for (it->SeekToLast(); it->Valid(); it->Prev()) {
      }
    }
    ASSERT_OK(it->status());
  }
  it0.reset();
  it1.reset();
  EXPECT_EQ(attempted_insert_ranges_.size(), 2);
  EXPECT_EQ(Get(0, "c"), "vc");
  EXPECT_EQ(Get(1, "c"), "vc");
}

// An iterator created after an ingestion sees the ingested files, so the
// barrier must still let it convert.
TEST_P(ReadPathRangeTombstoneTest, IteratorCreatedAfterIngestionConverts) {
  for (bool seqno_zero : {true, false}) {
    SCOPED_TRACE(seqno_zero ? "ingested with seqno zero"
                            : "ingested with a new seqno");
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 2;
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);

    // "x" alone overlaps no key, so it gets seqno zero. Adding "e" overlaps
    // the flushed "e", so the file gets a new seqno.
    const std::string file = dbname_ + "/ingest_x.sst";
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(file));
    if (!seqno_zero) {
      ASSERT_OK(writer.Put("e", "ve2"));
    }
    ASSERT_OK(writer.Put("x", "vx"));
    ASSERT_OK(writer.Finish());
    for (const char* key : {"a", "b", "c", "d", "e"}) {
      ASSERT_OK(Put(key, std::string("v") + key));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(Delete("b"));
    ASSERT_OK(Delete("c"));
    ASSERT_OK(Delete("d"));
    IngestExternalFileOptions ifo;
    ifo.allow_blocking_flush = false;
    ASSERT_OK(db_->IngestExternalFile({file}, ifo));

    {
      std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
      }
      ASSERT_OK(it->status());
    }
    EXPECT_EQ(TestGetTickerCount(options, READ_PATH_RANGE_TOMBSTONES_INSERTED),
              1);
    VerifyIteration({"a", "e", "x"});
    EXPECT_EQ(Get("e"), seqno_zero ? "ve" : "ve2");
  }
}

// A conversion that runs before an ingestion leaves a range tombstone in the
// memtable over the gap where the file lands. The ingestion must either fail
// or make the file visible, whatever the ingestion options.
TEST_P(ReadPathRangeTombstoneTest, IngestionAfterConversionIsNeverHidden) {
  enum class IngestMode {
    kNoGlobalSeqnoNoFlush,
    kNoGlobalSeqno,
    kGlobalSeqno,
    kDbGenerated
  };
  for (IngestMode mode :
       {IngestMode::kNoGlobalSeqnoNoFlush, IngestMode::kNoGlobalSeqno,
        IngestMode::kGlobalSeqno, IngestMode::kDbGenerated}) {
    SCOPED_TRACE("ingest mode " + std::to_string(static_cast<int>(mode)));
    Options options = CurrentOptions();
    options.min_tombstones_for_range_conversion = 2;
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);

    std::string file = dbname_ + "/ingest_c.sst";
    if (mode == IngestMode::kDbGenerated) {
      const std::string src_path = dbname_ + "_src";
      ASSERT_OK(DestroyDB(src_path, options));
      std::unique_ptr<DB> src;
      ASSERT_OK(DB::Open(options, src_path, &src));
      ASSERT_OK(src->Put(WriteOptions(), "c", "vc"));
      ASSERT_OK(src->Flush(FlushOptions()));
      std::vector<LiveFileMetaData> src_files;
      src->GetLiveFilesMetaData(&src_files);
      ASSERT_EQ(src_files.size(), 1);
      file = src_files[0].directory + "/" + src_files[0].relative_filename;
      ASSERT_OK(src->Close());
    } else {
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(file));
      ASSERT_OK(writer.Put("c", "vc"));
      ASSERT_OK(writer.Finish());
    }

    for (const char* key : {"a", "b", "d", "e"}) {
      ASSERT_OK(Put(key, std::string("v") + key));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(Delete("b"));
    ASSERT_OK(Delete("d"));
    attempted_insert_ranges_.clear();
    VerifyIteration({"a", "e"});
    ASSERT_EQ(TestGetTickerCount(options, READ_PATH_RANGE_TOMBSTONES_INSERTED),
              1);
    AssertRange(0, "b", "e");

    IngestExternalFileOptions ifo;
    ifo.allow_global_seqno = mode == IngestMode::kGlobalSeqno;
    ifo.allow_blocking_flush = mode != IngestMode::kNoGlobalSeqnoNoFlush;
    ifo.allow_db_generated_files = mode == IngestMode::kDbGenerated;
    Status s = db_->IngestExternalFile({file}, ifo);
    if (mode == IngestMode::kGlobalSeqno) {
      ASSERT_OK(s);
    } else {
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    }

    const std::string expected_c = s.ok() ? "vc" : "NOT_FOUND";
    EXPECT_EQ(Get("c"), expected_c);
    VerifyIteration(s.ok() ? std::vector<std::string>{"a", "c", "e"}
                           : std::vector<std::string>{"a", "e"});
    ASSERT_OK(Flush());
    Reopen(options);
    EXPECT_EQ(Get("c"), expected_c);
  }
}

// SeekForPrev uses the reverse path: PrevInternal -> FindValueForCurrentKey.
// When a key has more versions than max_sequential_skip_in_iterations,
// FindValueForCurrentKey delegates to FindValueForCurrentKeyUsingSeek which
// re-seeks forward to the newest visible version. That function must preserve
// key pinning by passing the correct `copy` parameter to
// saved_key_.SetUserKey().
TEST_P(DBIteratorTest, SeekForPrevKeyPinnedWithManyVersions) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 8;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // 20 versions > max_sequential_skip (8), forcing the reseek path.
  // No flush: keeps all versions alive in the memtable.
  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put("key1", "value" + std::to_string(i)));
  }

  ReadOptions ro;
  ro.pin_data = true;

  std::unique_ptr<Iterator> iter(NewIterator(ro));
  std::string prop_value;

  // Seek (forward) correctly reports key as pinned.
  iter->Seek("key1");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
  ASSERT_EQ("1", prop_value);

  uint64_t reseeks_before =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);

  iter->SeekForPrev("key1");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("key1", iter->key().ToString());

  // Confirm FindValueForCurrentKeyUsingSeek was actually taken.
  uint64_t reseeks_after =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
  ASSERT_GT(reseeks_after, reseeks_before);

  ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
  ASSERT_EQ("1", prop_value);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
