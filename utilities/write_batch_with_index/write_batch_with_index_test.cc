//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/utilities/write_batch_with_index.h"

#include <db/db_test_util.h>

#include <map>
#include <memory>

#include "db/column_family.h"
#include "memtable/wbwi_memtable.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {

namespace {
class ColumnFamilyHandleImplDummy : public ColumnFamilyHandleImpl {
 public:
  explicit ColumnFamilyHandleImplDummy(int id, const Comparator* comparator)
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr),
        id_(id),
        comparator_(comparator) {}
  uint32_t GetID() const override { return id_; }
  const Comparator* GetComparator() const override { return comparator_; }

 private:
  uint32_t id_;
  const Comparator* comparator_;
};

struct Entry {
  std::string key;
  std::string value;
  WriteType type;
};

struct TestHandler : public WriteBatch::Handler {
  std::map<uint32_t, std::vector<Entry>> seen;
  std::map<uint32_t, std::map<std::string, uint32_t>> cf_key_count;
  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    Entry e;
    e.key = key.ToString();
    e.value = value.ToString();
    e.type = kPutRecord;
    seen[column_family_id].push_back(e);
    cf_key_count[column_family_id][e.key]++;
    return Status::OK();
  }
  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
    Entry e;
    e.key = key.ToString();
    e.value = value.ToString();
    e.type = kMergeRecord;
    seen[column_family_id].push_back(e);
    cf_key_count[column_family_id][e.key]++;
    return Status::OK();
  }
  void LogData(const Slice& /*blob*/) override {}
  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    Entry e;
    e.key = key.ToString();
    e.value = "";
    e.type = kDeleteRecord;
    seen[column_family_id].push_back(e);
    cf_key_count[column_family_id][e.key]++;
    return Status::OK();
  }
  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override {
    Entry e;
    e.key = key.ToString();
    e.value = "";
    e.type = kDeleteRecord;
    seen[column_family_id].push_back(e);
    cf_key_count[column_family_id][e.key]++;
    return Status::OK();
  }
};

using KVMap = std::map<std::string, std::string>;

class KVIter : public Iterator {
 public:
  explicit KVIter(const KVMap* map, bool allow_unprepared_value = false,
                  bool fail_prepare_value = false)
      : map_(map),
        iter_(map_->end()),
        allow_unprepared_value_(allow_unprepared_value),
        fail_prepare_value_(fail_prepare_value) {}

  bool Valid() const override { return status_.ok() && iter_ != map_->end(); }

  void SeekToFirst() override {
    status_ = Status::OK();
    Reset();

    iter_ = map_->begin();

    if (Valid() && !allow_unprepared_value_) {
      Update();
    }
  }

  void SeekToLast() override {
    status_ = Status::OK();
    Reset();

    if (map_->empty()) {
      iter_ = map_->end();
    } else {
      iter_ = map_->find(map_->rbegin()->first);
    }

    if (Valid() && !allow_unprepared_value_) {
      Update();
    }
  }

  void Seek(const Slice& k) override {
    status_ = Status::OK();
    Reset();

    iter_ = map_->lower_bound(k.ToString());

    if (Valid() && !allow_unprepared_value_) {
      Update();
    }
  }

  void SeekForPrev(const Slice& k) override {
    status_ = Status::OK();
    Reset();

    iter_ = map_->upper_bound(k.ToString());
    Prev();

    if (Valid() && !allow_unprepared_value_) {
      Update();
    }
  }

  void Next() override {
    Reset();

    ++iter_;

    if (Valid() && !allow_unprepared_value_) {
      Update();
    }
  }

  void Prev() override {
    Reset();

    if (iter_ == map_->begin()) {
      iter_ = map_->end();
      return;
    }
    --iter_;

    if (Valid() && !allow_unprepared_value_) {
      Update();
    }
  }

  bool PrepareValue() override {
    assert(Valid());

    if (!allow_unprepared_value_) {
      return true;
    }

    if (fail_prepare_value_) {
      status_ = Status::Corruption("PrepareValue() failed");
      return false;
    }

    Update();

    return true;
  }

  Slice key() const override { return iter_->first; }
  Slice value() const override { return value_; }
  const WideColumns& columns() const override { return columns_; }
  Status status() const override { return status_; }

 private:
  void Reset() {
    value_.clear();
    columns_.clear();
  }

  void Update() {
    assert(Valid());

    value_ = iter_->second;
    columns_ = WideColumns{{kDefaultWideColumnName, value_}};
  }

  const KVMap* const map_;
  KVMap::const_iterator iter_;
  Status status_;
  Slice value_;
  WideColumns columns_;
  bool allow_unprepared_value_;
  bool fail_prepare_value_;
};

static std::string PrintContents(WriteBatchWithIndex* batch,
                                 ColumnFamilyHandle* column_family,
                                 bool hex = false) {
  std::string result;

  WBWIIterator* iter;
  if (column_family == nullptr) {
    iter = batch->NewIterator();
  } else {
    iter = batch->NewIterator(column_family);
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    WriteEntry e = iter->Entry();

    if (e.type == kPutRecord) {
      result.append("PUT(");
      result.append(e.key.ToString(hex));
      result.append("):");
      result.append(e.value.ToString(hex));
    } else if (e.type == kMergeRecord) {
      result.append("MERGE(");
      result.append(e.key.ToString(hex));
      result.append("):");
      result.append(e.value.ToString(hex));
    } else if (e.type == kSingleDeleteRecord) {
      result.append("SINGLE-DEL(");
      result.append(e.key.ToString(hex));
      result.append(")");
    } else {
      assert(e.type == kDeleteRecord);
      result.append("DEL(");
      result.append(e.key.ToString(hex));
      result.append(")");
    }

    result.append(",");
    iter->Next();
  }

  delete iter;
  return result;
}

static std::string PrintContents(WriteBatchWithIndex* batch, KVMap* base_map,
                                 ColumnFamilyHandle* column_family) {
  std::string result;

  Iterator* iter;
  if (column_family == nullptr) {
    iter = batch->NewIteratorWithBase(new KVIter(base_map));
  } else {
    iter = batch->NewIteratorWithBase(column_family, new KVIter(base_map));
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    assert(iter->status().ok());

    Slice key = iter->key();
    Slice value = iter->value();

    result.append(key.ToString());
    result.append(":");
    result.append(value.ToString());
    result.append(",");

    iter->Next();
  }

  delete iter;
  return result;
}

void AssertIter(Iterator* iter, const std::string& key,
                const std::string& value) {
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->key().ToString());
  ASSERT_EQ(value, iter->value().ToString());
}

void AssertItersMatch(Iterator* iter1, Iterator* iter2) {
  ASSERT_EQ(iter1->Valid(), iter2->Valid());
  if (iter1->Valid()) {
    ASSERT_EQ(iter1->key().ToString(), iter2->key().ToString());
    ASSERT_EQ(iter1->value().ToString(), iter2->value().ToString());
  }
}

void AssertItersEqual(Iterator* iter1, Iterator* iter2) {
  iter1->SeekToFirst();
  iter2->SeekToFirst();
  while (iter1->Valid()) {
    ASSERT_EQ(iter1->Valid(), iter2->Valid());
    ASSERT_EQ(iter1->key().ToString(), iter2->key().ToString());
    ASSERT_EQ(iter1->value().ToString(), iter2->value().ToString());
    iter1->Next();
    iter2->Next();
  }
  ASSERT_EQ(iter1->Valid(), iter2->Valid());
}

void AssertIterEqual(WBWIIteratorImpl* wbwii,
                     const std::vector<std::string>& keys) {
  wbwii->SeekToFirst();
  for (const auto& k : keys) {
    ASSERT_TRUE(wbwii->Valid());
    ASSERT_EQ(wbwii->Entry().key, k);
    wbwii->NextKey();
  }
  ASSERT_FALSE(wbwii->Valid());
  wbwii->SeekToLast();
  for (auto kit = keys.rbegin(); kit != keys.rend(); ++kit) {
    ASSERT_TRUE(wbwii->Valid());
    ASSERT_EQ(wbwii->Entry().key, *kit);
    wbwii->PrevKey();
  }
  ASSERT_FALSE(wbwii->Valid());
}

void AssertWBWICountEQWBCount(WriteBatchWithIndex& wbwi) {
  ASSERT_EQ(wbwi.GetWBWIOpCount(), wbwi.GetWriteBatch()->Count());
}
}  // namespace

class WBWIBaseTest : public testing::Test {
 public:
  explicit WBWIBaseTest(bool overwrite) : db_(nullptr) {
    options_.merge_operator =
        MergeOperators::CreateFromStringId("stringappend");
    options_.create_if_missing = true;
    dbname_ = test::PerThreadDBPath("write_batch_with_index_test");
    EXPECT_OK(DestroyDB(dbname_, options_));
    batch_.reset(new WriteBatchWithIndex(BytewiseComparator(), 20, overwrite));
  }

  ~WBWIBaseTest() override {
    AssertWBWICountEQWBCount(*batch_);

    if (db_ != nullptr) {
      ReleaseSnapshot();
      db_.reset();
      EXPECT_OK(DestroyDB(dbname_, options_));
    }
  }

  std::string AddToBatch(ColumnFamilyHandle* cf, const std::string& key) {
    std::string result;
    for (size_t i = 0; i < key.size(); i++) {
      if (key[i] == 'd') {
        EXPECT_OK(batch_->Delete(cf, key));
        result = "";
      } else if (key[i] == 'p') {
        result = key + std::to_string(i);
        EXPECT_OK(batch_->Put(cf, key, result));
      } else if (key[i] == 'e') {
        const std::string suffix = std::to_string(i);
        result = key + suffix;
        const WideColumns columns{{kDefaultWideColumnName, result},
                                  {key, suffix}};
        EXPECT_OK(batch_->PutEntity(cf, key, columns));
      } else if (key[i] == 'm') {
        std::string value = key + std::to_string(i);
        EXPECT_OK(batch_->Merge(cf, key, value));
        if (result.empty()) {
          result = value;
        } else {
          result = result + "," + value;
        }
      }
    }
    return result;
  }

  virtual Status OpenDB() { return DB::Open(options_, dbname_, &db_); }

  void ReleaseSnapshot() {
    if (read_opts_.snapshot != nullptr) {
      EXPECT_NE(db_, nullptr);
      db_->ReleaseSnapshot(read_opts_.snapshot);
      read_opts_.snapshot = nullptr;
    }
  }

  void VerifyWBWIIterUpdateCount(ColumnFamilyHandle* cfh = nullptr) {
    TestHandler handler;
    ASSERT_OK(batch_->GetWriteBatch()->Iterate(&handler));
    auto& count_map = handler.cf_key_count[GetColumnFamilyID(cfh)];
    // handler for going through write batch
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(cfh));
    iter->SeekToFirst();
    size_t key_seen = 0;
    while (iter->Valid()) {
      ASSERT_OK(iter->status());
      std::string key = iter->Entry().key.ToString();
      auto count = count_map[key];
      ASSERT_EQ(iter->GetUpdateCount(), count);
      iter->Next();
      // Merge is not overwritten when WBWI is created with overrwite_key
      // We can only check for decreasing update count since the update before
      // Merge can be overwritten multiple times.
      while (iter->Valid() && key == iter->Entry().key.ToString()) {
        ASSERT_GT(count, iter->GetUpdateCount());
        count = iter->GetUpdateCount();
        iter->Next();
      }
      ++key_seen;
    }
    ASSERT_EQ(key_seen, count_map.size());
  }

 public:
  std::unique_ptr<DB> db_;
  std::string dbname_;
  Options options_;
  WriteOptions write_opts_;
  ReadOptions read_opts_;
  std::unique_ptr<WriteBatchWithIndex> batch_;
};

class WBWIKeepTest : public WBWIBaseTest {
 public:
  WBWIKeepTest() : WBWIBaseTest(false) {}
};

class WBWIOverwriteTest : public WBWIBaseTest {
 public:
  WBWIOverwriteTest() : WBWIBaseTest(true) {}
};
class WriteBatchWithIndexTest : public testing::WithParamInterface<bool>,
                                public WBWIBaseTest {
 public:
  WriteBatchWithIndexTest() : WBWIBaseTest(GetParam()) {}
};

void TestValueAsSecondaryIndexHelper(std::vector<Entry> entries,
                                     WriteBatchWithIndex* batch,
                                     bool overwrite) {
  // In this test, we insert <key, value> to column family `data`, and
  // <value, key> to column family `index`. Then iterator them in order
  // and seek them by key.

  // Sort entries by key
  std::map<std::string, std::vector<Entry*>> data_map;
  // Sort entries by value
  std::map<std::string, std::vector<Entry*>> index_map;
  for (auto& e : entries) {
    if (overwrite && e.type != kMergeRecord && data_map[e.key].size() > 0) {
      data_map[e.key].back() = &e;
    } else {
      data_map[e.key].push_back(&e);
    }

    // index does not use Merge, so we overwrite the expected value
    if (overwrite && index_map[e.value].size() > 0) {
      index_map[e.value].back() = &e;
    } else {
      index_map[e.value].push_back(&e);
    }
  }
  // Most recent update for the same key is ordered first.
  for (auto& [_, v] : data_map) {
    std::reverse(v.begin(), v.end());
  }
  for (auto& [_, v] : index_map) {
    std::reverse(v.begin(), v.end());
  }

  ColumnFamilyHandleImplDummy data(6, BytewiseComparator());
  ColumnFamilyHandleImplDummy index(8, BytewiseComparator());
  for (auto& e : entries) {
    if (e.type == kPutRecord) {
      ASSERT_OK(batch->Put(&data, e.key, e.value));
      ASSERT_OK(batch->Put(&index, e.value, e.key));
    } else if (e.type == kMergeRecord) {
      ASSERT_OK(batch->Merge(&data, e.key, e.value));
      ASSERT_OK(batch->Put(&index, e.value, e.key));
    } else {
      assert(e.type == kDeleteRecord);
      std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&data));
      iter->Seek(e.key);
      ASSERT_OK(iter->status());
      auto write_entry = iter->Entry();
      ASSERT_EQ(e.key, write_entry.key.ToString());
      ASSERT_EQ(e.value, write_entry.value.ToString());
      ASSERT_OK(batch->Delete(&data, e.key));
      ASSERT_OK(batch->Put(&index, e.value, ""));
    }
  }

  // Iterator all keys
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&data));
    for (int seek_to_first : {0, 1}) {
      if (seek_to_first) {
        iter->SeekToFirst();
      } else {
        iter->Seek("");
      }
      for (const auto& pair : data_map) {
        for (auto v : pair.second) {
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          auto write_entry = iter->Entry();
          ASSERT_EQ(pair.first, write_entry.key.ToString());
          ASSERT_EQ(v->type, write_entry.type);
          if (write_entry.type != kDeleteRecord) {
            ASSERT_EQ(v->value, write_entry.value.ToString());
          }
          iter->Next();
        }
      }
      ASSERT_TRUE(!iter->Valid());
    }
    iter->SeekToLast();
    for (auto pair = data_map.rbegin(); pair != data_map.rend(); ++pair) {
      for (auto v = pair->second.rbegin(); v != pair->second.rend(); v++) {
        ASSERT_OK(iter->status());
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        ASSERT_EQ((*v)->type, write_entry.type);
        if (write_entry.type != kDeleteRecord) {
          ASSERT_EQ((*v)->value, write_entry.value.ToString());
        }
        iter->Prev();
      }
    }
    ASSERT_TRUE(!iter->Valid());
  }

  // Iterator all indexes
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&index));
    for (int seek_to_first : {0, 1}) {
      if (seek_to_first) {
        iter->SeekToFirst();
      } else {
        iter->Seek("");
      }
      for (const auto& pair : index_map) {
        for (auto v : pair.second) {
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          auto write_entry = iter->Entry();
          ASSERT_EQ(pair.first, write_entry.key.ToString());
          if (v->type != kDeleteRecord) {
            ASSERT_EQ(v->key, write_entry.value.ToString());
            ASSERT_EQ(v->value, write_entry.key.ToString());
          }
          iter->Next();
        }
      }
      ASSERT_TRUE(!iter->Valid());
    }

    iter->SeekToLast();
    for (auto pair = index_map.rbegin(); pair != index_map.rend(); ++pair) {
      for (auto v = pair->second.rbegin(); v != pair->second.rend(); v++) {
        ASSERT_OK(iter->status());
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        if ((*v)->type != kDeleteRecord) {
          ASSERT_EQ((*v)->key, write_entry.value.ToString());
          ASSERT_EQ((*v)->value, write_entry.key.ToString());
        }
        iter->Prev();
      }
    }
    ASSERT_TRUE(!iter->Valid());
  }

  // Seek to every key
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&data));

    // Seek the keys one by one in reverse order
    for (auto pair = data_map.rbegin(); pair != data_map.rend(); ++pair) {
      iter->Seek(pair->first);
      ASSERT_OK(iter->status());
      for (auto v : pair->second) {
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        ASSERT_EQ(v->type, write_entry.type);
        if (write_entry.type != kDeleteRecord) {
          ASSERT_EQ(v->value, write_entry.value.ToString());
        }
        iter->Next();
        ASSERT_OK(iter->status());
      }
    }

    // SeekForPrev
    for (auto pair = data_map.begin(); pair != data_map.end(); ++pair) {
      iter->SeekForPrev(pair->first);
      ASSERT_OK(iter->status());
      // SeekForPrev positions iter at the last entry <= target.
      // So we iterate updates from oldest to most recent.
      for (auto v = pair->second.rbegin(); v != pair->second.rend(); ++v) {
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        ASSERT_EQ((*v)->type, write_entry.type);
        if (write_entry.type != kDeleteRecord) {
          ASSERT_EQ((*v)->value, write_entry.value.ToString());
        }
        iter->Prev();
        ASSERT_OK(iter->status());
      }
    }
  }

  // Seek to every index
  {
    std::unique_ptr<WBWIIterator> iter(batch->NewIterator(&index));

    // Seek the keys one by one in reverse order
    for (auto pair = index_map.rbegin(); pair != index_map.rend(); ++pair) {
      iter->Seek(pair->first);
      ASSERT_OK(iter->status());
      for (auto v : pair->second) {
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        ASSERT_EQ(v->value, write_entry.key.ToString());
        if (v->type != kDeleteRecord) {
          ASSERT_EQ(v->key, write_entry.value.ToString());
        }
        iter->Next();
        ASSERT_OK(iter->status());
      }
    }

    // SeekForPrev
    for (auto pair = index_map.begin(); pair != index_map.end(); ++pair) {
      iter->SeekForPrev(pair->first);
      ASSERT_OK(iter->status());
      // SeekForPrev positions iter at the last entry <= target.
      // So we iterate updates from oldest to most recent.
      for (auto v = pair->second.rbegin(); v != pair->second.rend(); ++v) {
        ASSERT_TRUE(iter->Valid());
        auto write_entry = iter->Entry();
        ASSERT_EQ(pair->first, write_entry.key.ToString());
        if ((*v)->type != kDeleteRecord) {
          ASSERT_EQ((*v)->key, write_entry.value.ToString());
        }
        iter->Prev();
        ASSERT_OK(iter->status());
      }
    }
  }

  // Verify WriteBatch can be iterated
  TestHandler handler;
  ASSERT_OK(batch->GetWriteBatch()->Iterate(&handler));

  // Verify data column family
  {
    ASSERT_EQ(entries.size(), handler.seen[data.GetID()].size());
    size_t i = 0;
    for (const auto& e : handler.seen[data.GetID()]) {
      auto write_entry = entries[i++];
      ASSERT_EQ(e.type, write_entry.type);
      ASSERT_EQ(e.key, write_entry.key);
      if (e.type != kDeleteRecord) {
        ASSERT_EQ(e.value, write_entry.value);
      }
    }
  }

  // Verify index column family
  {
    ASSERT_EQ(entries.size(), handler.seen[index.GetID()].size());
    size_t i = 0;
    for (const auto& e : handler.seen[index.GetID()]) {
      auto write_entry = entries[i++];
      ASSERT_EQ(e.key, write_entry.value);
      if (write_entry.type != kDeleteRecord) {
        ASSERT_EQ(e.value, write_entry.key);
      }
    }
  }
}

TEST_P(WriteBatchWithIndexTest, TestValueAsSecondaryIndex) {
  Entry entries[] = {
      {"aaa", "0005", kPutRecord},   {"b", "0002", kPutRecord},
      {"cdd", "0002", kMergeRecord}, {"aab", "00001", kPutRecord},
      {"cc", "00005", kPutRecord},   {"cdd", "0002", kPutRecord},
      {"aab", "0003", kPutRecord},   {"cc", "00005", kDeleteRecord},
  };
  std::vector<Entry> entries_list(entries, entries + 8);

  batch_.reset(new WriteBatchWithIndex(nullptr, 20, GetParam()));

  TestValueAsSecondaryIndexHelper(entries_list, batch_.get(), GetParam());
  AssertWBWICountEQWBCount(*batch_);

  // Clear batch and re-run test with new values
  batch_->Clear();

  Entry new_entries[] = {
      {"aaa", "0005", kPutRecord}, {"e", "0002", kPutRecord},
      {"add", "0002", kPutRecord}, {"aab", "00001", kPutRecord},
      {"zz", "00005", kPutRecord}, {"add", "0002", kMergeRecord},
      {"aab", "0003", kPutRecord}, {"zz", "00005", kDeleteRecord},
  };

  entries_list = std::vector<Entry>(new_entries, new_entries + 8);

  TestValueAsSecondaryIndexHelper(entries_list, batch_.get(), GetParam());
  AssertWBWICountEQWBCount(*batch_);
}

TEST_P(WriteBatchWithIndexTest, WBWIIteratorImpl) {
  // Tests methods of WBWIIteratorImpl, with some overwrites and merges.
  ASSERT_OK(batch_->Merge("k0", "k0m0"));
  ASSERT_OK(batch_->Put("k0", "k0p1"));

  // a merge and a non-merge
  ASSERT_OK(batch_->Merge("k1", "k1m0"));
  ASSERT_OK(batch_->Merge("k1", "k1m1"));

  ASSERT_OK(batch_->SingleDelete("k2"));

  // put then merge
  ASSERT_OK(batch_->Put("k3", "k3p0"));
  ASSERT_OK(batch_->Merge("k3", "k3m1"));
  if (GetParam()) {
    VerifyWBWIIterUpdateCount();
  }

  std::unique_ptr<WBWIIteratorImpl> iter(
      static_cast<WBWIIteratorImpl*>(batch_->NewIterator()));

  auto verify_iter = [&iter](const std::string& k, const std::string& v,
                             WriteType t, int line) {
    SCOPED_TRACE("Called from line " + std::to_string(line));
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    const auto entry = iter->Entry();
    ASSERT_EQ(entry.key, k);
    if (t != kDeleteRecord && t != kSingleDeleteRecord) {
      ASSERT_EQ(entry.value, v);
    }
    ASSERT_EQ(entry.type, t);
  };

  // Should land on first key >= k0, recent update is ordered first
  iter->Seek("k0");
  verify_iter("k0", "k0p1", kPutRecord, __LINE__);
  // Should land on the first update of the next key
  iter->NextKey();
  verify_iter("k1", "k1m1", kMergeRecord, __LINE__);
  iter->NextKey();
  verify_iter("k2", "", kSingleDeleteRecord, __LINE__);
  iter->NextKey();
  verify_iter("k3", "k3m1", kMergeRecord, __LINE__);
  iter->PrevKey();
  verify_iter("k2", "", kSingleDeleteRecord, __LINE__);

  // Should land on last key <= k0, recent update is ordered first
  iter->SeekForPrev("k3");
  verify_iter("k3", "k3p0", kPutRecord, __LINE__);
  iter->PrevKey();
  verify_iter("k2", "", kSingleDeleteRecord, __LINE__);
  iter->PrevKey();
  verify_iter("k1", "k1m1", kMergeRecord, __LINE__);
  iter->PrevKey();
  verify_iter("k0", "k0p1", kPutRecord, __LINE__);
  iter->NextKey();
  verify_iter("k1", "k1m1", kMergeRecord, __LINE__);

  // test FindLatestUpdate
  iter->SeekToFirst();
  verify_iter("k0", "k0p1", kPutRecord, __LINE__);
  MergeContext merge_context;
  // iterator is not at k1
  ASSERT_EQ(iter->FindLatestUpdate("k1", &merge_context),
            WBWIIteratorImpl::Result::kNotFound);
  ASSERT_EQ(merge_context.GetNumOperands(), 0);
  // iterator was not moved
  verify_iter("k0", "k0p1", kPutRecord, __LINE__);

  // k0's most recent update is a Put
  ASSERT_EQ(iter->FindLatestUpdate("k0", &merge_context),
            WBWIIteratorImpl::Result::kFound);
  ASSERT_EQ(merge_context.GetNumOperands(), 0);
  verify_iter("k0", "k0p1", kPutRecord, __LINE__);

  iter->NextKey();
  verify_iter("k1", "k1m1", kMergeRecord, __LINE__);
  ASSERT_EQ(iter->FindLatestUpdate("k1", &merge_context),
            WBWIIteratorImpl::Result::kMergeInProgress);
  ASSERT_EQ(merge_context.GetNumOperands(), 2);
  auto operands = merge_context.GetOperands();
  // oldest merge is ordered first
  ASSERT_EQ(operands[0], "k1m0");
  ASSERT_EQ(operands[1], "k1m1");
  // iterator moved to last merge
  verify_iter("k1", "k1m0", kMergeRecord, __LINE__);
  merge_context.Clear();

  iter->Next();
  verify_iter("k2", "", kSingleDeleteRecord, __LINE__);
  ASSERT_EQ(iter->FindLatestUpdate("k2", &merge_context),
            WBWIIteratorImpl::Result::kDeleted);
  ASSERT_EQ(merge_context.GetNumOperands(), 0);
  verify_iter("k2", "", kSingleDeleteRecord, __LINE__);

  iter->Next();
  verify_iter("k3", "k3m1", kMergeRecord, __LINE__);
  // This will find latest updated of the current key
  ASSERT_EQ(iter->FindLatestUpdate(&merge_context),
            WBWIIteratorImpl::Result::kFound);
  ASSERT_EQ(merge_context.GetNumOperands(), 1);
  operands = merge_context.GetOperands();
  ASSERT_EQ(operands[0], "k3m1");
  // iterator moved to last merge
  verify_iter("k3", "k3p0", kPutRecord, __LINE__);

  // SeekToLast
  iter->SeekToLast();
  verify_iter("k3", "k3p0", kPutRecord, __LINE__);
  // FindLatestUpdate() while we are at the older update of k3
  ASSERT_EQ(iter->FindLatestUpdate(&merge_context),
            WBWIIteratorImpl::Result::kFound);
  ASSERT_EQ(merge_context.GetNumOperands(), 1);
  operands = merge_context.GetOperands();
  ASSERT_EQ(operands[0], "k3m1");
}

TEST_P(WriteBatchWithIndexTest, TestComparatorForCF) {
  ColumnFamilyHandleImplDummy cf1(6, nullptr);
  ColumnFamilyHandleImplDummy reverse_cf(66, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(88, BytewiseComparator());

  ASSERT_OK(batch_->Put(&cf1, "ddd", ""));
  ASSERT_OK(batch_->Put(&cf2, "aaa", ""));
  ASSERT_OK(batch_->Put(&cf2, "eee", ""));
  ASSERT_OK(batch_->Put(&cf1, "ccc", ""));
  ASSERT_OK(batch_->Put(&reverse_cf, "a11", ""));
  ASSERT_OK(batch_->Put(&cf1, "bbb", ""));

  Slice key_slices[] = {"a", "3", "3"};
  Slice value_slice = "";
  ASSERT_OK(batch_->Put(&reverse_cf, SliceParts(key_slices, 3),
                        SliceParts(&value_slice, 1)));
  ASSERT_OK(batch_->Put(&reverse_cf, "a22", ""));

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf1));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbb", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ccc", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ddd", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf2));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("aaa", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("eee", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&reverse_cf));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("z");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a33", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a22", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("a22");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a22", iter->Entry().key.ToString());

    iter->Seek("a13");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
  }
}

TEST_F(WBWIOverwriteTest, TestOverwriteKey) {
  ColumnFamilyHandleImplDummy cf1(6, nullptr);
  ColumnFamilyHandleImplDummy reverse_cf(66, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(88, BytewiseComparator());

  ASSERT_OK(batch_->Merge(&cf1, "ddd", ""));
  ASSERT_OK(batch_->Put(&cf1, "ddd", ""));
  ASSERT_OK(batch_->Delete(&cf1, "ddd"));
  ASSERT_OK(batch_->Put(&cf2, "aaa", ""));
  ASSERT_OK(batch_->Delete(&cf2, "aaa"));
  ASSERT_OK(batch_->Put(&cf2, "aaa", "aaa"));
  ASSERT_OK(batch_->Put(&cf2, "eee", "eee"));
  ASSERT_OK(batch_->Put(&cf1, "ccc", ""));
  ASSERT_OK(batch_->Put(&reverse_cf, "a11", ""));
  ASSERT_OK(batch_->Delete(&cf1, "ccc"));
  ASSERT_OK(batch_->Put(&reverse_cf, "a33", "a33"));
  ASSERT_OK(batch_->Put(&reverse_cf, "a11", "a11"));
  Slice slices[] = {"a", "3", "3"};
  ASSERT_OK(batch_->Delete(&reverse_cf, SliceParts(slices, 3)));

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf1));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ccc", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ddd", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&cf2));
    iter->SeekToLast();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("eee", iter->Entry().key.ToString());
    ASSERT_EQ("eee", iter->Entry().value.ToString());
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("aaa", iter->Entry().key.ToString());
    ASSERT_EQ("aaa", iter->Entry().value.ToString());
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("aaa", iter->Entry().key.ToString());
    ASSERT_EQ("aaa", iter->Entry().value.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("eee", iter->Entry().key.ToString());
    ASSERT_EQ("eee", iter->Entry().value.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  {
    std::unique_ptr<WBWIIterator> iter(batch_->NewIterator(&reverse_cf));
    iter->Seek("");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("z");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a33", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
    ASSERT_EQ("a11", iter->Entry().value.ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a11", iter->Entry().key.ToString());
    ASSERT_EQ("a11", iter->Entry().value.ToString());
    iter->Prev();

    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a33", iter->Entry().key.ToString());
    ASSERT_TRUE(iter->Entry().type == WriteType::kDeleteRecord);
    iter->Prev();
    ASSERT_TRUE(!iter->Valid());
  }
}

TEST_P(WriteBatchWithIndexTest, TestWBWIIterator) {
  ColumnFamilyHandleImplDummy cf1(1, BytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
  ASSERT_OK(batch_->Put(&cf1, "a", "a1"));
  ASSERT_OK(batch_->Put(&cf1, "c", "c1"));
  ASSERT_OK(batch_->Put(&cf1, "c", "c2"));
  ASSERT_OK(batch_->Put(&cf1, "e", "e1"));
  ASSERT_OK(batch_->Put(&cf1, "e", "e2"));
  ASSERT_OK(batch_->Put(&cf1, "e", "e3"));
  std::unique_ptr<WBWIIteratorImpl> iter1(
      static_cast<WBWIIteratorImpl*>(batch_->NewIterator(&cf1)));
  std::unique_ptr<WBWIIteratorImpl> iter2(
      static_cast<WBWIIteratorImpl*>(batch_->NewIterator(&cf2)));
  AssertIterEqual(iter1.get(), {"a", "c", "e"});
  AssertIterEqual(iter2.get(), {});
  ASSERT_OK(batch_->Put(&cf2, "a", "a2"));
  ASSERT_OK(batch_->Merge(&cf2, "b", "b1"));
  ASSERT_OK(batch_->Merge(&cf2, "b", "b2"));
  ASSERT_OK(batch_->Delete(&cf2, "d"));
  ASSERT_OK(batch_->Merge(&cf2, "d", "d2"));
  ASSERT_OK(batch_->Merge(&cf2, "d", "d3"));
  ASSERT_OK(batch_->Delete(&cf2, "f"));
  AssertIterEqual(iter1.get(), {"a", "c", "e"});
  AssertIterEqual(iter2.get(), {"a", "b", "d", "f"});
  if (GetParam()) {
    VerifyWBWIIterUpdateCount();
  }
}

TEST_P(WriteBatchWithIndexTest, TestRandomIteraratorWithBase) {
  std::vector<std::string> source_strings = {"a", "b", "c", "d", "e",
                                             "f", "g", "h", "i", "j"};
  for (int rand_seed = 301; rand_seed < 366; rand_seed++) {
    Random rnd(rand_seed);

    ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
    ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
    ColumnFamilyHandleImplDummy cf3(8, BytewiseComparator());
    batch_->Clear();

    if (rand_seed % 2 == 0) {
      ASSERT_OK(batch_->Put(&cf2, "zoo", "bar"));
    }
    if (rand_seed % 4 == 1) {
      ASSERT_OK(batch_->Put(&cf3, "zoo", "bar"));
    }

    KVMap map;
    KVMap merged_map;
    for (const auto& key : source_strings) {
      std::string value = key + key;
      int type = rnd.Uniform(6);
      switch (type) {
        case 0:
          // only base has it
          map[key] = value;
          merged_map[key] = value;
          break;
        case 1:
          // only delta has it
          ASSERT_OK(batch_->Put(&cf1, key, value));
          map[key] = value;
          merged_map[key] = value;
          break;
        case 2:
          // both has it. Delta should win
          ASSERT_OK(batch_->Put(&cf1, key, value));
          map[key] = "wrong_value";
          merged_map[key] = value;
          break;
        case 3:
          // both has it. Delta is delete
          ASSERT_OK(batch_->Delete(&cf1, key));
          map[key] = "wrong_value";
          break;
        case 4:
          // only delta has it. Delta is delete
          ASSERT_OK(batch_->Delete(&cf1, key));
          map[key] = "wrong_value";
          break;
        default:
          // Neither iterator has it.
          break;
      }
    }

    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));
    std::unique_ptr<Iterator> result_iter(new KVIter(&merged_map));

    bool is_valid = false;
    for (int i = 0; i < 128; i++) {
      // Random walk and make sure iter and result_iter returns the
      // same key and value
      int type = rnd.Uniform(6);
      ASSERT_OK(iter->status());
      switch (type) {
        case 0:
          // Seek to First
          iter->SeekToFirst();
          result_iter->SeekToFirst();
          break;
        case 1:
          // Seek to last
          iter->SeekToLast();
          result_iter->SeekToLast();
          break;
        case 2: {
          // Seek to random key
          auto key_idx = rnd.Uniform(static_cast<int>(source_strings.size()));
          auto key = source_strings[key_idx];
          iter->Seek(key);
          result_iter->Seek(key);
          break;
        }
        case 3: {
          // SeekForPrev to random key
          auto key_idx = rnd.Uniform(static_cast<int>(source_strings.size()));
          auto key = source_strings[key_idx];
          iter->SeekForPrev(key);
          result_iter->SeekForPrev(key);
          break;
        }
        case 4:
          // Next
          if (is_valid) {
            iter->Next();
            result_iter->Next();
          } else {
            continue;
          }
          break;
        default:
          assert(type == 5);
          // Prev
          if (is_valid) {
            iter->Prev();
            result_iter->Prev();
          } else {
            continue;
          }
          break;
      }
      AssertItersMatch(iter.get(), result_iter.get());
      is_valid = iter->Valid();
    }

    ASSERT_OK(iter->status());
    if (GetParam()) {
      VerifyWBWIIterUpdateCount(&cf1);
      VerifyWBWIIterUpdateCount(&cf2);
      VerifyWBWIIterUpdateCount(&cf3);
    }
  }
}

TEST_P(WriteBatchWithIndexTest, TestIteraratorWithBase) {
  ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, BytewiseComparator());
  {
    KVMap map;
    map["a"] = "aa";
    map["c"] = "cc";
    map["e"] = "ee";
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "e", "ee");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "e", "ee");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    AssertIter(iter.get(), "a", "aa");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("b");
    AssertIter(iter.get(), "c", "cc");

    iter->Prev();
    AssertIter(iter.get(), "a", "aa");

    iter->Seek("a");
    AssertIter(iter.get(), "a", "aa");
  }

  // Test the case that there is one element in the write batch
  ASSERT_OK(batch_->Put(&cf2, "zoo", "bar"));
  ASSERT_OK(batch_->Put(&cf1, "a", "aa"));
  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  ASSERT_OK(batch_->Delete(&cf1, "b"));
  ASSERT_OK(batch_->Put(&cf1, "c", "cc"));
  ASSERT_OK(batch_->Put(&cf1, "d", "dd"));
  ASSERT_OK(batch_->Delete(&cf1, "e"));

  {
    KVMap map;
    map["b"] = "";
    map["cc"] = "cccc";
    map["f"] = "ff";
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "cc", "cccc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");
    iter->Next();
    AssertIter(iter.get(), "f", "ff");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "f", "ff");
    iter->Prev();
    AssertIter(iter.get(), "d", "dd");
    iter->Prev();
    AssertIter(iter.get(), "cc", "cccc");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "cc", "cccc");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    AssertIter(iter.get(), "a", "aa");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("c");
    AssertIter(iter.get(), "c", "cc");

    iter->Seek("cb");
    AssertIter(iter.get(), "cc", "cccc");

    iter->Seek("cc");
    AssertIter(iter.get(), "cc", "cccc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");

    iter->Seek("e");
    AssertIter(iter.get(), "f", "ff");

    iter->Prev();
    AssertIter(iter.get(), "d", "dd");

    iter->Next();
    AssertIter(iter.get(), "f", "ff");
  }

  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "d", "dd");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    AssertIter(iter.get(), "a", "aa");

    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("aa");
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "d", "dd");

    iter->Seek("ca");
    AssertIter(iter.get(), "d", "dd");

    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
  }
}

TEST_P(WriteBatchWithIndexTest, TestIteraratorWithBaseReverseCmp) {
  ColumnFamilyHandleImplDummy cf1(6, ReverseBytewiseComparator());
  ColumnFamilyHandleImplDummy cf2(2, ReverseBytewiseComparator());

  // Test the case that there is one element in the write batch
  ASSERT_OK(batch_->Put(&cf2, "zoo", "bar"));
  ASSERT_OK(batch_->Put(&cf1, "a", "aa"));
  {
    KVMap empty_map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());
  }

  ASSERT_OK(batch_->Put(&cf1, "c", "cc"));
  {
    KVMap map;
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "c", "cc");
    iter->Next();
    AssertIter(iter.get(), "a", "aa");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "a", "aa");
    iter->Prev();
    AssertIter(iter.get(), "c", "cc");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("b");
    AssertIter(iter.get(), "a", "aa");

    iter->Prev();
    AssertIter(iter.get(), "c", "cc");

    iter->Seek("a");
    AssertIter(iter.get(), "a", "aa");
  }

  // default column family
  ASSERT_OK(batch_->Put("a", "b"));
  {
    KVMap map;
    map["b"] = "";
    std::unique_ptr<Iterator> iter(
        batch_->NewIteratorWithBase(new KVIter(&map)));

    iter->SeekToFirst();
    AssertIter(iter.get(), "a", "b");
    iter->Next();
    AssertIter(iter.get(), "b", "");
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->SeekToLast();
    AssertIter(iter.get(), "b", "");
    iter->Prev();
    AssertIter(iter.get(), "a", "b");
    iter->Prev();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("b");
    AssertIter(iter.get(), "b", "");

    iter->Prev();
    AssertIter(iter.get(), "a", "b");

    iter->Seek("0");
    AssertIter(iter.get(), "a", "b");
  }

  if (GetParam()) {
    VerifyWBWIIterUpdateCount();
    VerifyWBWIIterUpdateCount(&cf1);
    VerifyWBWIIterUpdateCount(&cf2);
  }
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatch) {
  Options options;
  Status s;
  std::string value;

  s = batch_->GetFromBatch(options_, "b", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put("a", "a"));
  ASSERT_OK(batch_->Put("b", "b"));
  ASSERT_OK(batch_->Put("c", "c"));
  ASSERT_OK(batch_->Put("a", "z"));
  ASSERT_OK(batch_->Delete("c"));
  ASSERT_OK(batch_->Delete("d"));
  ASSERT_OK(batch_->Delete("e"));
  ASSERT_OK(batch_->Put("e", "e"));

  s = batch_->GetFromBatch(options_, "b", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  s = batch_->GetFromBatch(options_, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z", value);

  s = batch_->GetFromBatch(options_, "c", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatch(options_, "d", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatch(options_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatch(options_, "e", &value);
  ASSERT_OK(s);
  ASSERT_EQ("e", value);

  ASSERT_OK(batch_->Merge("z", "z"));

  s = batch_->GetFromBatch(options_, "z", &value);
  ASSERT_NOK(s);  // No merge operator specified.

  s = batch_->GetFromBatch(options_, "b", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchMerge) {
  Status s = OpenDB();
  ASSERT_OK(s);

  ColumnFamilyHandle* column_family = db_->DefaultColumnFamily();
  std::string value;

  s = batch_->GetFromBatch(options_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put("x", "X"));
  std::string expected = "X";

  for (int i = 0; i < 5; i++) {
    ASSERT_OK(batch_->Merge("x", std::to_string(i)));
    expected = expected + "," + std::to_string(i);

    if (i % 2 == 0) {
      ASSERT_OK(batch_->Put("y", std::to_string(i / 2)));
    }

    ASSERT_OK(batch_->Merge("z", "z"));

    s = batch_->GetFromBatch(column_family, options_, "x", &value);
    ASSERT_OK(s);
    ASSERT_EQ(expected, value);

    s = batch_->GetFromBatch(column_family, options_, "y", &value);
    ASSERT_OK(s);
    ASSERT_EQ(std::to_string(i / 2), value);

    s = batch_->GetFromBatch(column_family, options_, "z", &value);
    ASSERT_TRUE(s.IsMergeInProgress());
  }
}

TEST_F(WBWIOverwriteTest, TestGetFromBatchMerge2) {
  Status s = OpenDB();
  ASSERT_OK(s);

  ColumnFamilyHandle* column_family = db_->DefaultColumnFamily();
  std::string value;

  s = batch_->GetFromBatch(column_family, options_, "X", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put(column_family, "X", "x"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x", value);

  ASSERT_OK(batch_->Put(column_family, "X", "x2"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x2", value);

  ASSERT_OK(batch_->Merge(column_family, "X", "aaa"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x2,aaa", value);

  ASSERT_OK(batch_->Merge(column_family, "X", "bbb"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x2,aaa,bbb", value);

  ASSERT_OK(batch_->Put(column_family, "X", "x3"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x3", value);

  ASSERT_OK(batch_->Merge(column_family, "X", "ccc"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("x3,ccc", value);

  ASSERT_OK(batch_->Delete(column_family, "X"));
  s = batch_->GetFromBatch(column_family, options_, "X", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Merge(column_family, "X", "ddd"));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "X", &value));
  ASSERT_EQ("ddd", value);
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchAndDB) {
  ASSERT_OK(OpenDB());

  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a"));
  ASSERT_OK(db_->Put(write_opts_, "b", "b"));
  ASSERT_OK(db_->Put(write_opts_, "c", "c"));

  ASSERT_OK(batch_->Put("a", "batch_->a"));
  ASSERT_OK(batch_->Delete("b"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "a", &value));
  ASSERT_EQ("batch_->a", value);

  Status s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "b", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "c", &value));
  ASSERT_EQ("c", value);

  s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(db_->Delete(write_opts_, "x"));

  s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchAndDBMerge) {
  Status s = OpenDB();
  ASSERT_OK(s);

  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a0"));
  ASSERT_OK(db_->Put(write_opts_, "b", "b0"));
  ASSERT_OK(db_->Merge(write_opts_, "b", "b1"));
  ASSERT_OK(db_->Merge(write_opts_, "c", "c0"));
  ASSERT_OK(db_->Merge(write_opts_, "d", "d0"));

  ASSERT_OK(batch_->Merge("a", "a1"));
  ASSERT_OK(batch_->Merge("a", "a2"));
  ASSERT_OK(batch_->Merge("b", "b2"));
  ASSERT_OK(batch_->Merge("d", "d1"));
  ASSERT_OK(batch_->Merge("e", "e0"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "a", &value));
  ASSERT_EQ("a0,a1,a2", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "b", &value));
  ASSERT_EQ("b0,b1,b2", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "c", &value));
  ASSERT_EQ("c0", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "d", &value));
  ASSERT_EQ("d0,d1", value);

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "e", &value));
  ASSERT_EQ("e0", value);

  ASSERT_OK(db_->Delete(write_opts_, "x"));

  s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "x", &value);
  ASSERT_TRUE(s.IsNotFound());

  const Snapshot* snapshot = db_->GetSnapshot();
  ReadOptions snapshot_read_options;
  snapshot_read_options.snapshot = snapshot;

  ASSERT_OK(db_->Delete(write_opts_, "a"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "a", &value));
  ASSERT_EQ("a1,a2", value);

  ASSERT_OK(s = batch_->GetFromBatchAndDB(db_.get(), snapshot_read_options, "a",
                                          &value));
  ASSERT_EQ("a0,a1,a2", value);

  ASSERT_OK(batch_->Delete("a"));

  s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "a", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = batch_->GetFromBatchAndDB(db_.get(), snapshot_read_options, "a", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(s = db_->Merge(write_opts_, "c", "c1"));

  ASSERT_OK(s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "c", &value));
  ASSERT_EQ("c0,c1", value);

  ASSERT_OK(s = batch_->GetFromBatchAndDB(db_.get(), snapshot_read_options, "c",
                                          &value));
  ASSERT_EQ("c0", value);

  ASSERT_OK(db_->Put(write_opts_, "e", "e1"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "e", &value));
  ASSERT_EQ("e1,e0", value);

  ASSERT_OK(
      batch_->GetFromBatchAndDB(db_.get(), snapshot_read_options, "e", &value));
  ASSERT_EQ("e0", value);

  ASSERT_OK(s = db_->Delete(write_opts_, "e"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "e", &value));
  ASSERT_EQ("e0", value);

  ASSERT_OK(
      batch_->GetFromBatchAndDB(db_.get(), snapshot_read_options, "e", &value));
  ASSERT_EQ("e0", value);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(WBWIOverwriteTest, TestGetFromBatchAndDBMerge2) {
  Status s = OpenDB();
  ASSERT_OK(s);

  std::string value;

  s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Merge("A", "xxx"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "A", &value));
  ASSERT_EQ(value, "xxx");

  ASSERT_OK(batch_->Merge("A", "yyy"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "A", &value));
  ASSERT_EQ(value, "xxx,yyy");

  ASSERT_OK(db_->Put(write_opts_, "A", "a0"));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "A", &value));
  ASSERT_EQ(value, "a0,xxx,yyy");

  ASSERT_OK(batch_->Delete("A"));

  s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, TestGetFromBatchAndDBMerge3) {
  Status s = OpenDB();
  ASSERT_OK(s);

  FlushOptions flush_options;
  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "A", "1"));
  ASSERT_OK(db_->Flush(flush_options, db_->DefaultColumnFamily()));
  ASSERT_OK(batch_->Merge("A", "2"));

  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "A", &value));
  ASSERT_EQ(value, "1,2");
}

TEST_P(WriteBatchWithIndexTest, TestPinnedGetFromBatchAndDB) {
  Status s = OpenDB();
  ASSERT_OK(s);

  PinnableSlice value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a0"));
  ASSERT_OK(db_->Put(write_opts_, "b", "b0"));
  ASSERT_OK(db_->Merge(write_opts_, "b", "b1"));
  ASSERT_OK(db_->Merge(write_opts_, "c", "c0"));
  ASSERT_OK(db_->Merge(write_opts_, "d", "d0"));
  ASSERT_OK(batch_->Merge("a", "a1"));
  ASSERT_OK(batch_->Merge("a", "a2"));
  ASSERT_OK(batch_->Merge("b", "b2"));
  ASSERT_OK(batch_->Merge("d", "d1"));
  ASSERT_OK(batch_->Merge("e", "e0"));

  for (int i = 0; i < 2; i++) {
    if (i == 1) {
      // Do it again with a flushed DB...
      ASSERT_OK(db_->Flush(FlushOptions(), db_->DefaultColumnFamily()));
    }
    ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "a", &value));
    ASSERT_EQ("a0,a1,a2", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "b", &value));
    ASSERT_EQ("b0,b1,b2", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "c", &value));
    ASSERT_EQ("c0", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "d", &value));
    ASSERT_EQ("d0,d1", value.ToString());

    ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "e", &value));
    ASSERT_EQ("e0", value.ToString());
    ASSERT_OK(db_->Delete(write_opts_, "x"));

    s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "x", &value);
    ASSERT_TRUE(s.IsNotFound());
  }
}

void AssertKey(std::string key, WBWIIterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->Entry().key.ToString());
}

void AssertValue(std::string value, WBWIIterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value, iter->Entry().value.ToString());
}

// Tests that we can write to the WBWI while we iterate (from a single thread).
// iteration should see the newest writes
TEST_F(WBWIOverwriteTest, MutateWhileIteratingCorrectnessTest) {
  for (char c = 'a'; c <= 'z'; ++c) {
    ASSERT_OK(batch_->Put(std::string(1, c), std::string(1, c)));
  }

  std::unique_ptr<WBWIIterator> iter(batch_->NewIterator());
  iter->Seek("k");
  AssertKey("k", iter.get());
  iter->Next();
  AssertKey("l", iter.get());
  ASSERT_OK(batch_->Put("ab", "cc"));
  iter->Next();
  AssertKey("m", iter.get());
  ASSERT_OK(batch_->Put("mm", "kk"));
  iter->Next();
  AssertKey("mm", iter.get());
  AssertValue("kk", iter.get());
  ASSERT_OK(batch_->Delete("mm"));

  iter->Next();
  AssertKey("n", iter.get());
  iter->Prev();
  AssertKey("mm", iter.get());
  ASSERT_EQ(kDeleteRecord, iter->Entry().type);

  iter->Seek("ab");
  AssertKey("ab", iter.get());
  ASSERT_OK(batch_->Delete("x"));
  iter->Seek("x");
  AssertKey("x", iter.get());
  ASSERT_EQ(kDeleteRecord, iter->Entry().type);
  iter->Prev();
  AssertKey("w", iter.get());
}

void AssertIterKey(std::string key, Iterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(key, iter->key().ToString());
}

void AssertIterValue(std::string value, Iterator* iter) {
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value, iter->value().ToString());
}

// same thing as above, but testing IteratorWithBase
TEST_F(WBWIOverwriteTest, MutateWhileIteratingBaseCorrectnessTest) {
  WriteBatchWithIndex batch(BytewiseComparator(), 0, true);
  for (char c = 'a'; c <= 'z'; ++c) {
    ASSERT_OK(batch_->Put(std::string(1, c), std::string(1, c)));
  }

  KVMap map;
  map["aa"] = "aa";
  map["cc"] = "cc";
  map["ee"] = "ee";
  map["em"] = "me";

  std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(new KVIter(&map)));
  iter->Seek("k");
  AssertIterKey("k", iter.get());
  iter->Next();
  AssertIterKey("l", iter.get());
  ASSERT_OK(batch_->Put("ab", "cc"));
  iter->Next();
  AssertIterKey("m", iter.get());
  ASSERT_OK(batch_->Put("mm", "kk"));
  iter->Next();
  AssertIterKey("mm", iter.get());
  AssertIterValue("kk", iter.get());
  ASSERT_OK(batch_->Delete("mm"));
  iter->Next();
  AssertIterKey("n", iter.get());
  iter->Prev();
  // "mm" is deleted, so we're back at "m"
  AssertIterKey("m", iter.get());

  iter->Seek("ab");
  AssertIterKey("ab", iter.get());
  iter->Prev();
  AssertIterKey("aa", iter.get());
  iter->Prev();
  AssertIterKey("a", iter.get());
  ASSERT_OK(batch_->Delete("aa"));
  iter->Next();
  AssertIterKey("ab", iter.get());
  iter->Prev();
  AssertIterKey("a", iter.get());

  ASSERT_OK(batch_->Delete("x"));
  iter->Seek("x");
  AssertIterKey("y", iter.get());
  iter->Next();
  AssertIterKey("z", iter.get());
  iter->Prev();
  iter->Prev();
  AssertIterKey("w", iter.get());

  ASSERT_OK(batch_->Delete("e"));
  iter->Seek("e");
  AssertIterKey("ee", iter.get());
  AssertIterValue("ee", iter.get());
  ASSERT_OK(batch_->Put("ee", "xx"));
  // still the same value
  AssertIterValue("ee", iter.get());
  iter->Next();
  AssertIterKey("em", iter.get());
  iter->Prev();
  // new value
  AssertIterValue("xx", iter.get());

  ASSERT_OK(iter->status());
}

// stress testing mutations with IteratorWithBase
TEST_F(WBWIOverwriteTest, MutateWhileIteratingBaseStressTest) {
  for (char c = 'a'; c <= 'z'; ++c) {
    ASSERT_OK(batch_->Put(std::string(1, c), std::string(1, c)));
  }

  KVMap map;
  for (char c = 'a'; c <= 'z'; ++c) {
    map[std::string(2, c)] = std::string(2, c);
  }

  std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(new KVIter(&map)));

  Random rnd(301);
  for (int i = 0; i < 1000000; ++i) {
    int random = rnd.Uniform(8);
    char c = static_cast<char>(rnd.Uniform(26) + 'a');
    switch (random) {
      case 0:
        ASSERT_OK(batch_->Put(std::string(1, c), "xxx"));
        break;
      case 1:
        ASSERT_OK(batch_->Put(std::string(2, c), "xxx"));
        break;
      case 2:
        ASSERT_OK(batch_->Delete(std::string(1, c)));
        break;
      case 3:
        ASSERT_OK(batch_->Delete(std::string(2, c)));
        break;
      case 4:
        iter->Seek(std::string(1, c));
        break;
      case 5:
        iter->Seek(std::string(2, c));
        break;
      case 6:
        if (iter->Valid()) {
          iter->Next();
        }
        break;
      case 7:
        if (iter->Valid()) {
          iter->Prev();
        }
        break;
      default:
        assert(false);
    }
  }
  ASSERT_OK(iter->status());
  VerifyWBWIIterUpdateCount();
}

TEST_P(WriteBatchWithIndexTest, TestNewIteratorWithBaseFromWbwi) {
  ColumnFamilyHandleImplDummy cf1(6, BytewiseComparator());
  KVMap map;
  map["a"] = "aa";
  map["c"] = "cc";
  map["e"] = "ee";
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(&cf1, new KVIter(&map)));
  ASSERT_NE(nullptr, iter);
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_P(WriteBatchWithIndexTest,
       NewIteratorWithBasePrefixSameAsStartAppliesOnlyToBase) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(4));
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(write_opts_, "foo:base", "db_value"));
  ASSERT_OK(db_->Put(write_opts_, "goo:base", "db_value"));

  ASSERT_OK(batch_->Put("foo:key1", "v1"));
  ASSERT_OK(batch_->Put("foo:key2", "v2"));
  ASSERT_OK(batch_->Put("goo:key1", "v3"));

  ReadOptions ro;
  ro.prefix_same_as_start = true;

  std::unique_ptr<Iterator> db_iter(db_->NewIterator(ro));
  db_iter->Seek("foo:");
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key(), "foo:base");
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
  ASSERT_OK(db_iter->status());

  // NewIteratorWithBase cannot infer prefix_same_as_start or the prefix
  // extractor from base_iterator, so the write batch portion is not
  // prefix-bounded by the base iterator's ReadOptions.
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(db_->NewIterator(ro)));
  std::vector<std::string> keys;
  for (iter->Seek("foo:"); iter->Valid(); iter->Next()) {
    keys.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(keys,
            (std::vector<std::string>{"foo:base", "foo:key1", "foo:key2",
                                      "goo:key1"}));
}

TEST_P(WriteBatchWithIndexTest, NewIteratorWithBasePrepareValue) {
  // BaseDeltaIterator by default should call PrepareValue if it lands on the
  // base iterator in case it was created with allow_unprepared_value=true.
  ColumnFamilyHandleImplDummy cf1(1, BytewiseComparator());
  KVMap map{{"a", "aa"}, {"c", "cc"}, {"e", "ee"}};

  ASSERT_OK(batch_->Put(&cf1, "c", "cc1"));

  {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        &cf1, new KVIter(&map, /* allow_unprepared_value */ true)));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "a");
    ASSERT_EQ(iter->value(), "aa");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "c");
    ASSERT_EQ(iter->value(), "cc1");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "e");
    ASSERT_EQ(iter->value(), "ee");

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "e");
    ASSERT_EQ(iter->value(), "ee");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "c");
    ASSERT_EQ(iter->value(), "cc1");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "a");
    ASSERT_EQ(iter->value(), "aa");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // PrepareValue failures from the base iterator should be propagated
  {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        &cf1, new KVIter(&map, /* allow_unprepared_value */ true,
                         /* fail_prepare_value */ true)));

    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());

    iter->SeekToLast();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());
  }
}

TEST_P(WriteBatchWithIndexTest, NewIteratorWithBaseAllowUnpreparedValue) {
  ColumnFamilyHandleImplDummy cf1(1, BytewiseComparator());
  KVMap map{{"a", "aa"}, {"c", "cc"}, {"e", "ee"}};

  ASSERT_OK(batch_->Put(&cf1, "c", "cc1"));

  ReadOptions read_options = read_opts_;
  read_options.allow_unprepared_value = true;

  {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        &cf1, new KVIter(&map, /* allow_unprepared_value */ true),
        &read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "a");
    ASSERT_TRUE(iter->value().empty());
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value(), "aa");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "c");
    ASSERT_EQ(iter->value(), "cc1");
    // This key is served out of the delta iterator so this PrepareValue() is a
    // no-op
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value(), "cc1");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "e");
    ASSERT_TRUE(iter->value().empty());
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value(), "ee");

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "e");
    ASSERT_TRUE(iter->value().empty());
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value(), "ee");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "c");
    ASSERT_EQ(iter->value(), "cc1");
    // This key is served out of the delta iterator so this PrepareValue() is a
    // no-op
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value(), "cc1");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "a");
    ASSERT_TRUE(iter->value().empty());
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value(), "aa");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // PrepareValue failures from the base iterator should be propagated
  {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        &cf1,
        new KVIter(&map, /* allow_unprepared_value */ true,
                   /* fail_prepare_value */ true),
        &read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->PrepareValue());
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->PrepareValue());
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());
  }
}

TEST_P(WriteBatchWithIndexTest, NewIteratorWithBaseMergePrepareValue) {
  // When performing a merge across the base and delta iterators,
  // BaseDeltaIterator should call PrepareValue on the base iterator in case it
  // was created with allow_unprepared_value=true. (Note: we use BlobDB here
  // to ensure PrepareValue is not a no-op.)
  options_.enable_blob_files = true;

  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(write_opts_, db_->DefaultColumnFamily(), "a", "aa"));
  ASSERT_OK(db_->Put(write_opts_, db_->DefaultColumnFamily(), "c", "cc"));
  ASSERT_OK(db_->Put(write_opts_, db_->DefaultColumnFamily(), "e", "ee"));
  ASSERT_OK(db_->Flush(FlushOptions(), db_->DefaultColumnFamily()));

  ASSERT_OK(batch_->Merge(db_->DefaultColumnFamily(), "a", "aa1"));
  ASSERT_OK(batch_->Merge(db_->DefaultColumnFamily(), "c", "cc1"));
  ASSERT_OK(batch_->Merge(db_->DefaultColumnFamily(), "e", "ee1"));

  ReadOptions db_read_options = read_opts_;
  db_read_options.allow_unprepared_value = true;

  {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        db_->DefaultColumnFamily(),
        db_->NewIterator(db_read_options, db_->DefaultColumnFamily()),
        &read_opts_));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "a");
    ASSERT_EQ(iter->value(), "aa,aa1");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "c");
    ASSERT_EQ(iter->value(), "cc,cc1");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "e");
    ASSERT_EQ(iter->value(), "ee,ee1");

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "e");
    ASSERT_EQ(iter->value(), "ee,ee1");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "c");
    ASSERT_EQ(iter->value(), "cc,cc1");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), "a");
    ASSERT_EQ(iter->value(), "aa,aa1");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:TamperWithResult", [](void* arg) {
        Slice* const blob_index = static_cast<Slice*>(arg);
        assert(blob_index);
        assert(!blob_index->empty());
        blob_index->remove_prefix(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // PrepareValue failures from the base iterator should be propagated
  {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        db_->DefaultColumnFamily(),
        db_->NewIterator(db_read_options, db_->DefaultColumnFamily()),
        &read_opts_));

    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());

    iter->SeekToLast();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(WriteBatchWithIndexTest, TestBoundsCheckingInDeltaIterator) {
  Status s = OpenDB();
  ASSERT_OK(s);

  KVMap empty_map;

  // writes that should be observed by BaseDeltaIterator::delta_iterator_
  ASSERT_OK(batch_->Put("a", "aa"));
  ASSERT_OK(batch_->Put("b", "bb"));
  ASSERT_OK(batch_->Put("c", "cc"));

  ReadOptions ro;

  auto check_only_b_is_visible = [&]() {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        db_->DefaultColumnFamily(), new KVIter(&empty_map), &ro));

    // move to the lower bound
    iter->SeekToFirst();
    ASSERT_EQ("b", iter->key());
    iter->Prev();
    ASSERT_FALSE(iter->Valid());

    // move to the upper bound
    iter->SeekToLast();
    ASSERT_EQ("b", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());

    // test bounds checking in Seek and SeekForPrev
    iter->Seek(Slice("a"));
    ASSERT_EQ("b", iter->key());
    iter->Seek(Slice("b"));
    ASSERT_EQ("b", iter->key());
    iter->Seek(Slice("c"));
    ASSERT_FALSE(iter->Valid());

    iter->SeekForPrev(Slice("c"));
    ASSERT_EQ("b", iter->key());
    iter->SeekForPrev(Slice("b"));
    ASSERT_EQ("b", iter->key());
    iter->SeekForPrev(Slice("a"));
    ASSERT_FALSE(iter->Valid());

    iter->SeekForPrev(
        Slice("a.1"));  // a non-existent key that is smaller than "b"
    ASSERT_FALSE(iter->Valid());

    iter->Seek(Slice("b.1"));  // a non-existent key that is greater than "b"
    ASSERT_FALSE(iter->Valid());

    delete ro.iterate_lower_bound;
    delete ro.iterate_upper_bound;
  };

  ro.iterate_lower_bound = new Slice("b");
  ro.iterate_upper_bound = new Slice("c");
  check_only_b_is_visible();

  ro.iterate_lower_bound = new Slice("a.1");
  ro.iterate_upper_bound = new Slice("c");
  check_only_b_is_visible();

  ro.iterate_lower_bound = new Slice("b");
  ro.iterate_upper_bound = new Slice("b.2");
  check_only_b_is_visible();
}

TEST_P(WriteBatchWithIndexTest,
       TestBoundsCheckingInSeekToFirstAndLastOfDeltaIterator) {
  Status s = OpenDB();
  ASSERT_OK(s);
  KVMap empty_map;
  // writes that should be observed by BaseDeltaIterator::delta_iterator_
  ASSERT_OK(batch_->Put("c", "cc"));

  ReadOptions ro;
  auto check_nothing_visible = [&]() {
    std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
        db_->DefaultColumnFamily(), new KVIter(&empty_map), &ro));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    iter->SeekToLast();
    ASSERT_FALSE(iter->Valid());

    delete ro.iterate_lower_bound;
    delete ro.iterate_upper_bound;
  };

  ro.iterate_lower_bound = new Slice("b");
  ro.iterate_upper_bound = new Slice("c");
  check_nothing_visible();

  ro.iterate_lower_bound = new Slice("d");
  ro.iterate_upper_bound = new Slice("e");
  check_nothing_visible();
}

TEST_P(WriteBatchWithIndexTest, SavePointTest) {
  ColumnFamilyHandleImplDummy cf1(1, BytewiseComparator());
  KVMap empty_map;
  std::unique_ptr<Iterator> cf0_iter(
      batch_->NewIteratorWithBase(new KVIter(&empty_map)));
  std::unique_ptr<Iterator> cf1_iter(
      batch_->NewIteratorWithBase(&cf1, new KVIter(&empty_map)));
  Status s;
  KVMap kvm_cf0_0 = {{"A", "aa"}, {"B", "b"}};
  KVMap kvm_cf1_0 = {{"A", "a1"}, {"C", "c1"}, {"E", "e1"}};
  KVIter kvi_cf0_0(&kvm_cf0_0);
  KVIter kvi_cf1_0(&kvm_cf1_0);

  ASSERT_OK(batch_->Put("A", "a"));
  ASSERT_OK(batch_->Put("B", "b"));
  ASSERT_OK(batch_->Put("A", "aa"));
  ASSERT_OK(batch_->Put(&cf1, "A", "a1"));
  ASSERT_OK(batch_->Delete(&cf1, "B"));
  ASSERT_OK(batch_->Put(&cf1, "C", "c1"));
  ASSERT_OK(batch_->Put(&cf1, "E", "e1"));

  AssertItersEqual(cf0_iter.get(), &kvi_cf0_0);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_0);
  batch_->SetSavePoint();  // 1

  KVMap kvm_cf0_1 = {{"B", "bb"}, {"C", "cc"}};
  KVMap kvm_cf1_1 = {{"B", "b1"}, {"C", "c1"}};
  KVIter kvi_cf0_1(&kvm_cf0_1);
  KVIter kvi_cf1_1(&kvm_cf1_1);

  ASSERT_OK(batch_->Put("C", "cc"));
  ASSERT_OK(batch_->Put("B", "bb"));
  ASSERT_OK(batch_->Delete("A"));
  ASSERT_OK(batch_->Put(&cf1, "B", "b1"));
  ASSERT_OK(batch_->Delete(&cf1, "A"));
  ASSERT_OK(batch_->SingleDelete(&cf1, "E"));
  batch_->SetSavePoint();  // 2
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_1);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_1);

  KVMap kvm_cf0_2 = {{"A", "xxx"}, {"C", "cc"}};
  KVMap kvm_cf1_2 = {{"B", "b2"}};
  KVIter kvi_cf0_2(&kvm_cf0_2);
  KVIter kvi_cf1_2(&kvm_cf1_2);

  ASSERT_OK(batch_->Put("A", "aaa"));
  ASSERT_OK(batch_->Put("A", "xxx"));
  ASSERT_OK(batch_->Delete("B"));
  ASSERT_OK(batch_->Put(&cf1, "B", "b2"));
  ASSERT_OK(batch_->Delete(&cf1, "C"));
  batch_->SetSavePoint();  // 3
  batch_->SetSavePoint();  // 4
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_2);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_2);

  KVMap kvm_cf0_4 = {{"A", "xxx"}, {"C", "cc"}};
  KVMap kvm_cf1_4 = {{"B", "b2"}};
  KVIter kvi_cf0_4(&kvm_cf0_4);
  KVIter kvi_cf1_4(&kvm_cf1_4);
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->Delete(&cf1, "D"));
  ASSERT_OK(batch_->Delete(&cf1, "E"));
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_4);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_4);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 4
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_2);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_2);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 3
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_2);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_2);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 2
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_1);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_1);

  batch_->SetSavePoint();  // 5
  ASSERT_OK(batch_->Put("X", "x"));

  KVMap kvm_cf0_5 = {{"B", "bb"}, {"C", "cc"}, {"X", "x"}};
  KVIter kvi_cf0_5(&kvm_cf0_5);
  KVIter kvi_cf1_5(&kvm_cf1_1);
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_5);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_5);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 5
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_1);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_1);

  ASSERT_OK(batch_->RollbackToSavePoint());  // rollback to 1
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_0);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_0);

  s = batch_->RollbackToSavePoint();  // no savepoint found
  ASSERT_TRUE(s.IsNotFound());
  AssertItersEqual(cf0_iter.get(), &kvi_cf0_0);
  AssertItersEqual(cf1_iter.get(), &kvi_cf1_0);

  batch_->SetSavePoint();  // 6

  batch_->Clear();
  ASSERT_EQ("", PrintContents(batch_.get(), nullptr));
  ASSERT_EQ("", PrintContents(batch_.get(), &cf1));

  s = batch_->RollbackToSavePoint();  // rollback to 6
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, SingleDeleteTest) {
  Status s;
  std::string value;

  ASSERT_OK(batch_->SingleDelete("A"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  batch_->Clear();
  ASSERT_OK(batch_->Put("A", "a"));
  ASSERT_OK(batch_->Put("A", "a2"));
  ASSERT_OK(batch_->Put("B", "b"));
  ASSERT_OK(batch_->SingleDelete("A"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  ASSERT_OK(batch_->Put("C", "c"));
  ASSERT_OK(batch_->Put("A", "a3"));
  ASSERT_OK(batch_->Delete("B"));
  ASSERT_OK(batch_->SingleDelete("B"));
  ASSERT_OK(batch_->SingleDelete("C"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a3", value);
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Put("B", "b4"));
  ASSERT_OK(batch_->Put("C", "c4"));
  ASSERT_OK(batch_->Put("D", "d4"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->Delete("A"));

  s = batch_->GetFromBatch(options_, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatch(options_, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b4", value);
  s = batch_->GetFromBatch(options_, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c4", value);
  s = batch_->GetFromBatch(options_, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(WriteBatchWithIndexTest, SingleDeleteDeltaIterTest) {
  std::string value;
  ASSERT_OK(batch_->Put("A", "a"));
  ASSERT_OK(batch_->Put("A", "a2"));
  ASSERT_OK(batch_->Put("B", "b"));
  ASSERT_OK(batch_->SingleDelete("A"));
  ASSERT_OK(batch_->Delete("B"));

  KVMap map;
  value = PrintContents(batch_.get(), &map, nullptr);
  ASSERT_EQ("", value);

  map["A"] = "aa";
  map["C"] = "cc";
  map["D"] = "dd";

  ASSERT_OK(batch_->SingleDelete("B"));
  ASSERT_OK(batch_->SingleDelete("C"));
  ASSERT_OK(batch_->SingleDelete("Z"));

  value = PrintContents(batch_.get(), &map, nullptr);
  ASSERT_EQ("D:dd,", value);

  ASSERT_OK(batch_->Put("A", "a3"));
  ASSERT_OK(batch_->Put("B", "b3"));
  ASSERT_OK(batch_->SingleDelete("A"));
  ASSERT_OK(batch_->SingleDelete("A"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->SingleDelete("D"));
  ASSERT_OK(batch_->Delete("D"));

  map["E"] = "ee";

  value = PrintContents(batch_.get(), &map, nullptr);
  ASSERT_EQ("B:b3,E:ee,", value);
}

TEST_P(WriteBatchWithIndexTest, MultiGetTest) {
  // MultiGet a lot of keys in order to force std::vector reallocations
  std::vector<std::string> keys;
  for (int i = 0; i < 100; ++i) {
    keys.emplace_back(std::to_string(i));
  }

  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  // Write some data to the db for the even numbered keys
  {
    WriteBatch wb;
    for (size_t i = 0; i < keys.size(); i += 2) {
      std::string val = "val" + std::to_string(i);
      ASSERT_OK(wb.Put(cf0, keys[i], val));
    }
    ASSERT_OK(db_->Write(write_opts_, &wb));
    for (size_t i = 0; i < keys.size(); i += 2) {
      std::string value;
      ASSERT_OK(db_->Get(read_opts_, cf0, keys[i], &value));
    }
  }

  // Write some data to the batch
  for (size_t i = 0; i < keys.size(); ++i) {
    if ((i % 5) == 0) {
      ASSERT_OK(batch_->Delete(cf0, keys[i]));
    } else if ((i % 7) == 0) {
      std::string val = "new" + std::to_string(i);
      ASSERT_OK(batch_->Put(cf0, keys[i], val));
    }
    if (i > 0 && (i % 3) == 0) {
      ASSERT_OK(batch_->Merge(cf0, keys[i], "merge"));
    }
  }

  std::vector<Slice> key_slices;
  for (size_t i = 0; i < keys.size(); ++i) {
    key_slices.emplace_back(keys[i]);
  }
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  batch_->MultiGetFromBatchAndDB(db_.get(), read_opts_, cf0, key_slices.size(),
                                 key_slices.data(), values.data(),
                                 statuses.data(), false);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i == 0) {
      ASSERT_TRUE(statuses[i].IsNotFound());
    } else if ((i % 3) == 0) {
      ASSERT_OK(statuses[i]);
      if ((i % 5) == 0) {  // Merge after Delete
        ASSERT_EQ(values[i], "merge");
      } else if ((i % 7) == 0) {  // Merge after Put
        std::string val = "new" + std::to_string(i);
        ASSERT_EQ(values[i], val + ",merge");
      } else if ((i % 2) == 0) {
        std::string val = "val" + std::to_string(i);
        ASSERT_EQ(values[i], val + ",merge");
      } else {
        ASSERT_EQ(values[i], "merge");
      }
    } else if ((i % 5) == 0) {
      ASSERT_TRUE(statuses[i].IsNotFound());
    } else if ((i % 7) == 0) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], "new" + std::to_string(i));
    } else if ((i % 2) == 0) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], "val" + std::to_string(i));
    } else {
      ASSERT_TRUE(statuses[i].IsNotFound());
    }
  }
}
TEST_P(WriteBatchWithIndexTest, MultiGetTest2) {
  // MultiGet a lot of keys in order to force std::vector reallocations
  const int num_keys = 700;
  const int keys_per_pass = 100;
  std::vector<std::string> keys;
  for (size_t i = 0; i < num_keys; ++i) {
    keys.emplace_back(std::to_string(i));
  }
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  // Keys   0- 99 have a PUT in the batch but not DB
  // Keys 100-199 have a PUT in the DB
  // Keys 200-299 Have a PUT/DELETE
  // Keys 300-399 Have a PUT/DELETE/MERGE
  // Keys 400-499 have a PUT/MERGE
  // Keys 500-599 have a MERGE only
  // Keys 600-699 were never written
  {
    WriteBatch wb;
    for (size_t i = 100; i < 500; i++) {
      std::string val = std::to_string(i);
      ASSERT_OK(wb.Put(cf0, keys[i], val));
    }
    ASSERT_OK(db_->Write(write_opts_, &wb));
  }
  ASSERT_OK(db_->Flush(FlushOptions(), cf0));
  for (size_t i = 0; i < 100; i++) {
    ASSERT_OK(batch_->Put(cf0, keys[i], keys[i]));
  }
  for (size_t i = 200; i < 400; i++) {
    ASSERT_OK(batch_->Delete(cf0, keys[i]));
  }
  for (size_t i = 300; i < 600; i++) {
    std::string val = std::to_string(i) + "m";
    ASSERT_OK(batch_->Merge(cf0, keys[i], val));
  }

  Random rnd(301);
  std::vector<PinnableSlice> values(keys_per_pass);
  std::vector<Status> statuses(keys_per_pass);
  for (int pass = 0; pass < 40; pass++) {
    std::vector<Slice> key_slices;
    for (size_t i = 0; i < keys_per_pass; i++) {
      int random = rnd.Uniform(num_keys);
      key_slices.emplace_back(keys[random]);
    }
    batch_->MultiGetFromBatchAndDB(db_.get(), read_opts_, cf0, keys_per_pass,
                                   key_slices.data(), values.data(),
                                   statuses.data(), false);
    for (size_t i = 0; i < keys_per_pass; i++) {
      int key = ParseInt(key_slices[i].ToString());
      switch (key / 100) {
        case 0:  // 0-99 PUT only
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString());
          break;
        case 1:  // 100-199 PUT only
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString());
          break;
        case 2:  // 200-299 Deleted
          ASSERT_TRUE(statuses[i].IsNotFound());
          break;
        case 3:  // 300-399 Delete+Merge
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString() + "m");
          break;
        case 4:  // 400-400 Put+ Merge
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString() + "," +
                                   key_slices[i].ToString() + "m");
          break;
        case 5:  // Merge only
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], key_slices[i].ToString() + "m");
          break;
        case 6:  // Never written
          ASSERT_TRUE(statuses[i].IsNotFound());
          break;
        default:
          assert(false);
      }  // end switch
    }  // End for each key
  }  // end for passes
}

// This test has merges, but the merge does not play into the final result
TEST_P(WriteBatchWithIndexTest, FakeMergeWithIteratorTest) {
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  // The map we are starting with
  KVMap input = {
      {"odm", "odm0"},
      {"omd", "omd0"},
      {"omp", "omp0"},
  };
  KVMap result = {
      {"odm", "odm2"},  // Orig, Delete, Merge
      {"mp", "mp1"},    // Merge, Put
      {"omp", "omp2"},  // Origi, Merge, Put
      {"mmp", "mmp2"}   // Merge, Merge, Put
  };

  for (auto& iter : result) {
    EXPECT_EQ(AddToBatch(cf0, iter.first), iter.second);
  }
  AddToBatch(cf0, "md");   // Merge, Delete
  AddToBatch(cf0, "mmd");  // Merge, Merge, Delete
  AddToBatch(cf0, "omd");  // Orig, Merge, Delete

  KVIter kvi(&result);
  // First try just the batch
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(cf0, new KVIter(&input)));
  AssertItersEqual(iter.get(), &kvi);
}

TEST_P(WriteBatchWithIndexTest, IteratorMergeTest) {
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  KVMap result = {
      {"m", "m0"},                // Merge
      {"mm", "mm0,mm1"},          // Merge, Merge
      {"dm", "dm1"},              // Delete, Merge
      {"dmm", "dmm1,dmm2"},       // Delete, Merge, Merge
      {"mdm", "mdm2"},            // Merge, Delete, Merge
      {"mpm", "mpm1,mpm2"},       // Merge, Put, Merge
      {"pm", "pm0,pm1"},          // Put, Merge
      {"pmm", "pmm0,pmm1,pmm2"},  // Put, Merge, Merge
  };

  for (auto& iter : result) {
    EXPECT_EQ(AddToBatch(cf0, iter.first), iter.second);
  }

  KVIter kvi(&result);
  // First try just the batch
  KVMap empty_map;
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(cf0, new KVIter(&empty_map)));
  AssertItersEqual(iter.get(), &kvi);
}

TEST_P(WriteBatchWithIndexTest, IteratorMergeTestWithOrig) {
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();
  KVMap original;
  KVMap results = {
      {"m", "om,m0"},             // Merge
      {"mm", "omm,mm0,mm1"},      // Merge, Merge
      {"dm", "dm1"},              // Delete, Merge
      {"dmm", "dmm1,dmm2"},       // Delete, Merge, Merge
      {"mdm", "mdm2"},            // Merge, Delete, Merge
      {"mpm", "mpm1,mpm2"},       // Merge, Put, Merge
      {"pm", "pm0,pm1"},          // Put, Merge
      {"pmm", "pmm0,pmm1,pmm2"},  // Put, Merge, Merge
  };

  for (auto& iter : results) {
    AddToBatch(cf0, iter.first);
    original[iter.first] = "o" + iter.first;
  }

  KVIter kvi(&results);
  // First try just the batch
  std::unique_ptr<Iterator> iter(
      batch_->NewIteratorWithBase(cf0, new KVIter(&original)));
  AssertItersEqual(iter.get(), &kvi);
}

TEST_P(WriteBatchWithIndexTest, GetFromBatchAfterMerge) {
  std::string value;
  Status s;

  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(write_opts_, "o", "aa"));
  ASSERT_OK(batch_->Merge("o", "bb"));  // Merging bb under key "o"
  ASSERT_OK(batch_->Merge("m", "cc"));  // Merging bc under key "m"
  s = batch_->GetFromBatch(options_, "m", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  s = batch_->GetFromBatch(options_, "o", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);

  ASSERT_OK(db_->Write(write_opts_, batch_->GetWriteBatch()));
  ASSERT_OK(db_->Get(read_opts_, "o", &value));
  ASSERT_EQ(value, "aa,bb");
  ASSERT_OK(db_->Get(read_opts_, "m", &value));
  ASSERT_EQ(value, "cc");
}

TEST_P(WriteBatchWithIndexTest, GetFromBatchAndDBAfterMerge) {
  std::string value;

  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(write_opts_, "o", "aa"));
  ASSERT_OK(batch_->Merge("o", "bb"));  // Merging bb under key "o"
  ASSERT_OK(batch_->Merge("m", "cc"));  // Merging bc under key "m"
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "o", &value));
  ASSERT_EQ(value, "aa,bb");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "m", &value));
  ASSERT_EQ(value, "cc");
}

TEST_P(WriteBatchWithIndexTest, GetAfterPut) {
  std::string value;
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  ASSERT_OK(db_->Put(write_opts_, "key", "orig"));

  ASSERT_OK(batch_->Put("key", "aa"));  // Writing aa under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "aa");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "aa");

  ASSERT_OK(batch_->Merge("key", "bb"));  // Merging bb under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "aa,bb");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "aa,bb");

  ASSERT_OK(batch_->Merge("key", "cc"));  // Merging cc under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "aa,bb,cc");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "aa,bb,cc");
}

TEST_P(WriteBatchWithIndexTest, GetAfterMergePut) {
  std::string value;
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();
  ASSERT_OK(db_->Put(write_opts_, "key", "orig"));

  ASSERT_OK(batch_->Merge("key", "aa"));  // Merging aa under key
  Status s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "orig,aa");

  ASSERT_OK(batch_->Merge("key", "bb"));  // Merging bb under key
  s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "orig,aa,bb");

  ASSERT_OK(batch_->Put("key", "cc"));  // Writing cc under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "cc");

  ASSERT_OK(batch_->Merge("key", "dd"));  // Merging dd under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
}

TEST_P(WriteBatchWithIndexTest, GetAfterMergeDelete) {
  std::string value;
  ASSERT_OK(OpenDB());
  ColumnFamilyHandle* cf0 = db_->DefaultColumnFamily();

  ASSERT_OK(batch_->Merge("key", "aa"));  // Merging aa under key
  Status s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "aa");

  ASSERT_OK(batch_->Merge("key", "bb"));  // Merging bb under key
  s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_EQ(s.code(), Status::Code::kMergeInProgress);
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "aa,bb");

  ASSERT_OK(batch_->Delete("key"));  // Delete key from batch
  s = batch_->GetFromBatch(cf0, options_, "key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(batch_->Merge("key", "cc"));  // Merging cc under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "cc");
  ASSERT_OK(batch_->Merge("key", "dd"));  // Merging dd under key
  ASSERT_OK(batch_->GetFromBatch(cf0, options_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "key", &value));
  ASSERT_EQ(value, "cc,dd");
}

TEST_P(WriteBatchWithIndexTest, TestBadMergeOperator) {
  class FailingMergeOperator : public MergeOperator {
   public:
    FailingMergeOperator() = default;

    bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                     MergeOperationOutput* /*merge_out*/) const override {
      return false;
    }

    const char* Name() const override { return "Failing"; }
  };
  options_.merge_operator.reset(new FailingMergeOperator());
  ASSERT_OK(OpenDB());

  ColumnFamilyHandle* column_family = db_->DefaultColumnFamily();
  std::string value;

  ASSERT_OK(db_->Put(write_opts_, "a", "a0"));
  ASSERT_OK(batch_->Put("b", "b0"));

  ASSERT_OK(batch_->Merge("a", "a1"));
  ASSERT_NOK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "a", &value));
  ASSERT_NOK(batch_->GetFromBatch(column_family, options_, "a", &value));
  ASSERT_OK(batch_->GetFromBatchAndDB(db_.get(), read_opts_, "b", &value));
  ASSERT_OK(batch_->GetFromBatch(column_family, options_, "b", &value));
}

TEST_P(WriteBatchWithIndexTest, ColumnFamilyWithTimestamp) {
  ASSERT_OK(OpenDB());

  ColumnFamilyHandleImplDummy cf2(2,
                                  test::BytewiseComparatorWithU64TsWrapper());

  // Sanity checks
  ASSERT_TRUE(batch_->Put(&cf2, "key", "ts", "value").IsNotSupported());
  ASSERT_TRUE(batch_->Put(/*column_family=*/nullptr, "key", "ts", "value")
                  .IsInvalidArgument());
  ASSERT_TRUE(batch_->Delete(&cf2, "key", "ts").IsNotSupported());
  ASSERT_TRUE(batch_->Delete(/*column_family=*/nullptr, "key", "ts")
                  .IsInvalidArgument());
  ASSERT_TRUE(batch_->SingleDelete(&cf2, "key", "ts").IsNotSupported());
  ASSERT_TRUE(batch_->SingleDelete(/*column_family=*/nullptr, "key", "ts")
                  .IsInvalidArgument());
  {
    std::string value;
    ASSERT_TRUE(
        batch_->GetFromBatchAndDB(db_.get(), ReadOptions(), &cf2, "key", &value)
            .IsInvalidArgument());
  }
  {
    constexpr size_t num_keys = 2;
    std::array<Slice, num_keys> keys{{Slice(), Slice()}};
    std::array<PinnableSlice, num_keys> pinnable_vals{
        {PinnableSlice(), PinnableSlice()}};
    std::array<Status, num_keys> statuses{{Status(), Status()}};
    constexpr bool sorted_input = false;
    batch_->MultiGetFromBatchAndDB(db_.get(), ReadOptions(), &cf2, num_keys,
                                   keys.data(), pinnable_vals.data(),
                                   statuses.data(), sorted_input);
    for (const auto& s : statuses) {
      ASSERT_TRUE(s.IsInvalidArgument());
    }
  }

  constexpr uint32_t kMaxKey = 10;

  const auto ts_sz_lookup = [&cf2](uint32_t id) {
    if (cf2.GetID() == id) {
      return sizeof(uint64_t);
    } else {
      return std::numeric_limits<size_t>::max();
    }
  };

  // Put keys
  for (uint32_t i = 0; i < kMaxKey; ++i) {
    std::string key;
    PutFixed32(&key, i);
    Status s = batch_->Put(&cf2, key, "value" + std::to_string(i));
    ASSERT_OK(s);
  }

  WriteBatch* wb = batch_->GetWriteBatch();
  assert(wb);
  ASSERT_OK(
      wb->UpdateTimestamps(std::string(sizeof(uint64_t), '\0'), ts_sz_lookup));

  // Point lookup
  for (uint32_t i = 0; i < kMaxKey; ++i) {
    std::string value;
    std::string key;
    PutFixed32(&key, i);
    Status s = batch_->GetFromBatch(&cf2, Options(), key, &value);
    ASSERT_OK(s);
    ASSERT_EQ("value" + std::to_string(i), value);
  }

  // Iterator
  {
    std::unique_ptr<WBWIIterator> it(batch_->NewIterator(&cf2));
    uint32_t start = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next(), ++start) {
      std::string key;
      PutFixed32(&key, start);
      ASSERT_OK(it->status());
      ASSERT_EQ(key, it->Entry().key);
      ASSERT_EQ("value" + std::to_string(start), it->Entry().value);
      ASSERT_EQ(WriteType::kPutRecord, it->Entry().type);
    }
    ASSERT_EQ(kMaxKey, start);
  }

  // Delete the keys with Delete() or SingleDelete()
  for (uint32_t i = 0; i < kMaxKey; ++i) {
    std::string key;
    PutFixed32(&key, i);
    Status s;
    if (0 == (i % 2)) {
      s = batch_->Delete(&cf2, key);
    } else {
      s = batch_->SingleDelete(&cf2, key);
    }
    ASSERT_OK(s);
  }

  ASSERT_OK(wb->UpdateTimestamps(std::string(sizeof(uint64_t), '\xfe'),
                                 ts_sz_lookup));

  for (uint32_t i = 0; i < kMaxKey; ++i) {
    std::string value;
    std::string key;
    PutFixed32(&key, i);
    Status s = batch_->GetFromBatch(&cf2, Options(), key, &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  // Iterator
  {
    const bool overwrite = GetParam();
    std::unique_ptr<WBWIIterator> it(batch_->NewIterator(&cf2));
    uint32_t start = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next(), ++start) {
      std::string key;
      PutFixed32(&key, start);
      ASSERT_EQ(key, it->Entry().key);
      // For each key, most recent update (Del) is ordered first.
      if (0 == (start % 2)) {
        ASSERT_EQ(WriteType::kDeleteRecord, it->Entry().type);
      } else {
        ASSERT_EQ(WriteType::kSingleDeleteRecord, it->Entry().type);
      }
      if (!overwrite) {
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(WriteType::kPutRecord, it->Entry().type);
      }
    }
  }
}

TEST_P(WriteBatchWithIndexTest, IndexNoTs) {
  const Comparator* const ucmp = test::BytewiseComparatorWithU64TsWrapper();
  ColumnFamilyHandleImplDummy cf(1, ucmp);
  WriteBatchWithIndex wbwi;
  ASSERT_OK(wbwi.Put(&cf, "a", "a0"));
  ASSERT_OK(wbwi.Put(&cf, "a", "a1"));
  {
    std::string ts;
    PutFixed64(&ts, 10000);
    ASSERT_OK(wbwi.GetWriteBatch()->UpdateTimestamps(
        ts, [](uint32_t cf_id) { return cf_id == 1 ? 8 : 0; }));
  }
  {
    std::string value;
    Status s = wbwi.GetFromBatch(&cf, options_, "a", &value);
    ASSERT_OK(s);
    ASSERT_EQ("a1", value);
  }
}

TEST_P(WriteBatchWithIndexTest, WideColumnsBatchOnly) {
  // Tests for the case when there's no need to consult the underlying DB during
  // queries, i.e. when all queries can be answered using the write batch only.

  ASSERT_OK(OpenDB());

  constexpr size_t num_keys = 6;

  constexpr char delete_key[] = "d";
  constexpr char delete_merge_key[] = "dm";
  constexpr char put_entity_key[] = "e";
  constexpr char put_entity_merge_key[] = "em";
  constexpr char put_key[] = "p";
  constexpr char put_merge_key[] = "pm";

  AddToBatch(db_->DefaultColumnFamily(), delete_key);
  AddToBatch(db_->DefaultColumnFamily(), delete_merge_key);
  AddToBatch(db_->DefaultColumnFamily(), put_entity_key);
  AddToBatch(db_->DefaultColumnFamily(), put_entity_merge_key);
  AddToBatch(db_->DefaultColumnFamily(), put_key);
  AddToBatch(db_->DefaultColumnFamily(), put_merge_key);

  std::array<Slice, num_keys> keys{{delete_key, delete_merge_key,
                                    put_entity_key, put_entity_merge_key,
                                    put_key, put_merge_key}};

  std::array<WideColumns, num_keys> expected{
      {{},
       {{kDefaultWideColumnName, "dm1"}},
       {{kDefaultWideColumnName, "e0"}, {"e", "0"}},
       {{kDefaultWideColumnName, "em0,em1"}, {"em", "0"}},
       {{kDefaultWideColumnName, "p0"}},
       {{kDefaultWideColumnName, "pm0,pm1"}}}};

  // GetFromBatchAndDB
  {
    PinnableSlice value;
    ASSERT_TRUE(
        batch_->GetFromBatchAndDB(db_.get(), read_opts_, delete_key, &value)
            .IsNotFound());
  }

  for (size_t i = 1; i < num_keys; ++i) {
    PinnableSlice value;
    ASSERT_OK(
        batch_->GetFromBatchAndDB(db_.get(), read_opts_, keys[i], &value));
    ASSERT_EQ(value, expected[i].front().value());
  }

  // MultiGetFromBatchAndDB
  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetFromBatchAndDB(
        db_.get(), read_opts_, db_->DefaultColumnFamily(), num_keys,
        keys.data(), values.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsNotFound());

    for (size_t i = 1; i < num_keys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], expected[i].front().value());
    }
  }

  // GetEntityFromBatchAndDB
  {
    PinnableWideColumns columns;
    ASSERT_TRUE(batch_
                    ->GetEntityFromBatchAndDB(db_.get(), read_opts_,
                                              db_->DefaultColumnFamily(),
                                              delete_key, &columns)
                    .IsNotFound());
  }

  for (size_t i = 1; i < num_keys; ++i) {
    PinnableWideColumns columns;
    ASSERT_OK(batch_->GetEntityFromBatchAndDB(
        db_.get(), read_opts_, db_->DefaultColumnFamily(), keys[i], &columns));
    ASSERT_EQ(columns.columns(), expected[i]);
  }

  // MultiGetEntityFromBatchAndDB
  {
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetEntityFromBatchAndDB(
        db_.get(), read_opts_, db_->DefaultColumnFamily(), num_keys,
        keys.data(), results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsNotFound());

    for (size_t i = 1; i < num_keys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i].columns(), expected[i]);
    }
  }

  // Iterator
  std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
      db_->DefaultColumnFamily(), db_->NewIterator(read_opts_), &read_opts_));

  iter->SeekToFirst();

  for (size_t i = 1; i < num_keys; ++i) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), keys[i]);
    ASSERT_EQ(iter->value(), expected[i].front().value());
    ASSERT_EQ(iter->columns(), expected[i]);
    iter->Next();
  }

  ASSERT_FALSE(iter->Valid());

  iter->SeekToLast();

  for (size_t i = num_keys - 1; i > 0; --i) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), keys[i]);
    ASSERT_EQ(iter->value(), expected[i].front().value());
    ASSERT_EQ(iter->columns(), expected[i]);
    iter->Prev();
  }

  ASSERT_FALSE(iter->Valid());
}

TEST_P(WriteBatchWithIndexTest, WideColumnsBatchAndDB) {
  // Tests for the case when queries require consulting both the write batch and
  // the underlying DB, either because of merges or because the write batch
  // doesn't contain the key.

  ASSERT_OK(OpenDB());

  constexpr size_t num_keys = 6;

  // Note: for the "merge" keys, we'll have a merge operation in the write batch
  // and the base value (Put/PutEntity/Delete) in the DB. For the "no-merge"
  // keys, we'll have nothing in the write batch and a standalone
  // Put/PutEntity/Delete in the DB.
  constexpr char merge_a_key[] = "ma";
  constexpr char merge_b_key[] = "mb";
  constexpr char merge_c_key[] = "mc";
  constexpr char no_merge_a_key[] = "na";
  constexpr char no_merge_b_key[] = "nb";
  constexpr char no_merge_c_key[] = "nc";

  constexpr char merge_a_value[] = "mao";
  const WideColumns merge_b_columns{{kDefaultWideColumnName, "mbo"},
                                    {"mb", "o"}};
  constexpr char no_merge_a_value[] = "nao";
  const WideColumns no_merge_b_columns{{kDefaultWideColumnName, "nbo"},
                                       {"nb", "o"}};

  ASSERT_OK(db_->Put(write_opts_, db_->DefaultColumnFamily(), merge_a_key,
                     merge_a_value));
  ASSERT_OK(db_->PutEntity(write_opts_, db_->DefaultColumnFamily(), merge_b_key,
                           merge_b_columns));
  ASSERT_OK(db_->Delete(write_opts_, db_->DefaultColumnFamily(), merge_c_key));
  ASSERT_OK(db_->Put(write_opts_, db_->DefaultColumnFamily(), no_merge_a_key,
                     no_merge_a_value));
  ASSERT_OK(db_->PutEntity(write_opts_, db_->DefaultColumnFamily(),
                           no_merge_b_key, no_merge_b_columns));
  ASSERT_OK(
      db_->Delete(write_opts_, db_->DefaultColumnFamily(), no_merge_c_key));

  AddToBatch(db_->DefaultColumnFamily(), merge_a_key);
  AddToBatch(db_->DefaultColumnFamily(), merge_b_key);
  AddToBatch(db_->DefaultColumnFamily(), merge_c_key);

  std::array<Slice, num_keys> keys{{merge_a_key, merge_b_key, merge_c_key,
                                    no_merge_a_key, no_merge_b_key,
                                    no_merge_c_key}};

  std::array<WideColumns, num_keys> expected{
      {{{kDefaultWideColumnName, "mao,ma0"}},
       {{kDefaultWideColumnName, "mbo,mb0"}, {"mb", "o"}},
       {{kDefaultWideColumnName, "mc0"}},
       {{kDefaultWideColumnName, "nao"}},
       {{kDefaultWideColumnName, "nbo"}, {"nb", "o"}},
       {}}};

  // GetFromBatchAndDB
  for (size_t i = 0; i < num_keys - 1; ++i) {
    PinnableSlice value;
    ASSERT_OK(
        batch_->GetFromBatchAndDB(db_.get(), read_opts_, keys[i], &value));
    ASSERT_EQ(value, expected[i].front().value());
  }

  {
    PinnableSlice value;
    ASSERT_TRUE(
        batch_->GetFromBatchAndDB(db_.get(), read_opts_, no_merge_c_key, &value)
            .IsNotFound());
  }

  // MultiGetFromBatchAndDB
  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetFromBatchAndDB(
        db_.get(), read_opts_, db_->DefaultColumnFamily(), num_keys,
        keys.data(), values.data(), statuses.data(), sorted_input);

    for (size_t i = 0; i < num_keys - 1; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], expected[i].front().value());
    }

    ASSERT_TRUE(statuses[num_keys - 1].IsNotFound());
  }

  // GetEntityFromBatchAndDB
  for (size_t i = 0; i < num_keys - 1; ++i) {
    PinnableWideColumns columns;
    ASSERT_OK(batch_->GetEntityFromBatchAndDB(
        db_.get(), read_opts_, db_->DefaultColumnFamily(), keys[i], &columns));
    ASSERT_EQ(columns.columns(), expected[i]);
  }

  {
    PinnableWideColumns columns;
    ASSERT_TRUE(batch_
                    ->GetEntityFromBatchAndDB(db_.get(), read_opts_,
                                              db_->DefaultColumnFamily(),
                                              no_merge_c_key, &columns)
                    .IsNotFound());
  }

  // MultiGetEntityFromBatchAndDB
  {
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetEntityFromBatchAndDB(
        db_.get(), read_opts_, db_->DefaultColumnFamily(), num_keys,
        keys.data(), results.data(), statuses.data(), sorted_input);

    for (size_t i = 0; i < num_keys - 1; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i].columns(), expected[i]);
    }

    ASSERT_TRUE(statuses[num_keys - 1].IsNotFound());
  }

  // Iterator
  std::unique_ptr<Iterator> iter(batch_->NewIteratorWithBase(
      db_->DefaultColumnFamily(), db_->NewIterator(read_opts_), &read_opts_));

  iter->SeekToFirst();

  for (size_t i = 0; i < num_keys - 1; ++i) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), keys[i]);
    ASSERT_EQ(iter->value(), expected[i].front().value());
    ASSERT_EQ(iter->columns(), expected[i]);
    iter->Next();
  }

  ASSERT_FALSE(iter->Valid());

  iter->SeekToLast();

  for (size_t i = 0; i < num_keys - 1; ++i) {
    const size_t idx = num_keys - 2 - i;

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), keys[idx]);
    ASSERT_EQ(iter->value(), expected[idx].front().value());
    ASSERT_EQ(iter->columns(), expected[idx]);
    iter->Prev();
  }

  ASSERT_FALSE(iter->Valid());
}

TEST_P(WriteBatchWithIndexTest, GetEntityFromBatch) {
  ASSERT_OK(OpenDB());

  // No base value, no merges => NotFound
  {
    constexpr char key[] = "a";

    PinnableWideColumns result;
    ASSERT_TRUE(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result)
            .IsNotFound());
  }

  // No base value, with merges => MergeInProgress
  {
    constexpr char key[] = "b";
    constexpr char merge_op1[] = "bv1";
    constexpr char merge_op2[] = "bv2";

    ASSERT_OK(batch_->Merge("b", merge_op1));
    ASSERT_OK(batch_->Merge("b", merge_op2));

    PinnableWideColumns result;
    ASSERT_TRUE(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result)
            .IsMergeInProgress());
  }

  // Plain value, no merges => Found
  {
    constexpr char key[] = "c";
    constexpr char value[] = "cv";

    ASSERT_OK(batch_->Put(key, value));

    PinnableWideColumns result;
    ASSERT_OK(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result));

    const WideColumns expected{{kDefaultWideColumnName, value}};
    ASSERT_EQ(result.columns(), expected);
  }

  // Wide-column value, no merges => Found
  {
    constexpr char key[] = "d";
    const WideColumns columns{
        {kDefaultWideColumnName, "d0v"}, {"1", "d1v"}, {"2", "d2v"}};

    ASSERT_OK(batch_->PutEntity(db_->DefaultColumnFamily(), key, columns));

    PinnableWideColumns result;
    ASSERT_OK(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result));

    ASSERT_EQ(result.columns(), columns);
  }

  // Plain value, with merges => Found
  {
    constexpr char key[] = "e";
    constexpr char base_value[] = "ev0";
    constexpr char merge_op1[] = "ev1";
    constexpr char merge_op2[] = "ev2";

    ASSERT_OK(batch_->Put(key, base_value));
    ASSERT_OK(batch_->Merge(key, merge_op1));
    ASSERT_OK(batch_->Merge(key, merge_op2));

    PinnableWideColumns result;
    ASSERT_OK(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result));

    const WideColumns expected{{kDefaultWideColumnName, "ev0,ev1,ev2"}};
    ASSERT_EQ(result.columns(), expected);
  }

  // Wide-column value, with merges => Found
  {
    constexpr char key[] = "f";
    const WideColumns base_columns{
        {kDefaultWideColumnName, "f0v0"}, {"1", "f1v"}, {"2", "f2v"}};
    constexpr char merge_op1[] = "f0v1";
    constexpr char merge_op2[] = "f0v2";

    ASSERT_OK(batch_->PutEntity(db_->DefaultColumnFamily(), key, base_columns));
    ASSERT_OK(batch_->Merge(key, merge_op1));
    ASSERT_OK(batch_->Merge(key, merge_op2));

    PinnableWideColumns result;
    ASSERT_OK(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result));

    const WideColumns expected{{kDefaultWideColumnName, "f0v0,f0v1,f0v2"},
                               base_columns[1],
                               base_columns[2]};
    ASSERT_EQ(result.columns(), expected);
  }

  // Delete, no merges => NotFound
  {
    constexpr char key[] = "g";

    ASSERT_OK(batch_->Delete(key));

    PinnableWideColumns result;
    ASSERT_TRUE(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result)
            .IsNotFound());
  }

  // Delete, with merges => Found
  {
    constexpr char key[] = "h";
    constexpr char merge_op1[] = "hv1";
    constexpr char merge_op2[] = "hv2";

    ASSERT_OK(batch_->Delete(key));
    ASSERT_OK(batch_->Merge(key, merge_op1));
    ASSERT_OK(batch_->Merge(key, merge_op2));

    PinnableWideColumns result;
    ASSERT_OK(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, &result));

    const WideColumns expected{{kDefaultWideColumnName, "hv1,hv2"}};
    ASSERT_EQ(result.columns(), expected);
  }

  // Validate parameters
  {
    constexpr char key[] = "foo";
    PinnableWideColumns result;

    ASSERT_TRUE(
        batch_->GetEntityFromBatch(nullptr, key, &result).IsInvalidArgument());
    ASSERT_TRUE(
        batch_->GetEntityFromBatch(db_->DefaultColumnFamily(), key, nullptr)
            .IsInvalidArgument());
  }
}

TEST_P(WriteBatchWithIndexTest, EntityReadSanityChecks) {
  ASSERT_OK(OpenDB());

  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";
  constexpr size_t num_keys = 2;

  {
    constexpr DB* db = nullptr;
    PinnableWideColumns columns;
    ASSERT_TRUE(batch_
                    ->GetEntityFromBatchAndDB(db, ReadOptions(),
                                              db_->DefaultColumnFamily(), foo,
                                              &columns)
                    .IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    PinnableWideColumns columns;
    ASSERT_TRUE(batch_
                    ->GetEntityFromBatchAndDB(db_.get(), ReadOptions(),
                                              column_family, foo, &columns)
                    .IsInvalidArgument());
  }

  {
    constexpr PinnableWideColumns* columns = nullptr;
    ASSERT_TRUE(batch_
                    ->GetEntityFromBatchAndDB(db_.get(), ReadOptions(),
                                              db_->DefaultColumnFamily(), foo,
                                              columns)
                    .IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kGet;

    PinnableWideColumns columns;
    ASSERT_TRUE(batch_
                    ->GetEntityFromBatchAndDB(db_.get(), read_options,
                                              db_->DefaultColumnFamily(), foo,
                                              &columns)
                    .IsInvalidArgument());
  }

  {
    constexpr DB* db = nullptr;
    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetEntityFromBatchAndDB(
        db, ReadOptions(), db_->DefaultColumnFamily(), num_keys, keys.data(),
        results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetEntityFromBatchAndDB(
        db_.get(), ReadOptions(), column_family, num_keys, keys.data(),
        results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr Slice* keys = nullptr;
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetEntityFromBatchAndDB(
        db_.get(), ReadOptions(), db_->DefaultColumnFamily(), num_keys, keys,
        results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    std::array<Slice, num_keys> keys{{foo, bar}};
    constexpr PinnableWideColumns* results = nullptr;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetEntityFromBatchAndDB(
        db_.get(), ReadOptions(), db_->DefaultColumnFamily(), num_keys,
        keys.data(), results, statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kMultiGet;

    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    batch_->MultiGetEntityFromBatchAndDB(
        db_.get(), read_options, db_->DefaultColumnFamily(), num_keys,
        keys.data(), results.data(), statuses.data(), sorted_input);
    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }
}

TEST_P(WriteBatchWithIndexTest, TrackAndClearCFStats) {
  std::string value;
  ASSERT_OK(batch_->Put("A", "val"));
  ASSERT_OK(batch_->SingleDelete("B"));

  ColumnFamilyHandleImplDummy cf1(/*id=*/1, BytewiseComparator());
  ASSERT_OK(batch_->Put(&cf1, "bar", "foo"));

  {
    auto& cf_id_to_count = batch_->GetCFStats();
    ASSERT_EQ(2, cf_id_to_count.size());
    for (const auto [cf_id, stat] : cf_id_to_count) {
      if (cf_id == 0) {
        ASSERT_EQ(2, stat.entry_count);
        ASSERT_EQ(0, stat.overwritten_sd_count);
      } else {
        ASSERT_EQ(cf_id, 1);
        ASSERT_EQ(1, stat.entry_count);
        ASSERT_EQ(0, stat.overwritten_sd_count);
      }
    }
  }

  batch_->Clear();
  ASSERT_TRUE(batch_->GetCFStats().empty());

  // Now do a version with overwritten SD
  ASSERT_OK(batch_->Put("A", "val"));
  ASSERT_OK(batch_->SingleDelete("A"));
  bool overwrite = GetParam();
  {
    auto& cf_id_to_count = batch_->GetCFStats();
    ASSERT_EQ(1, cf_id_to_count.size());
    ASSERT_EQ(overwrite ? 1 : 2, cf_id_to_count.at(0).entry_count);
    ASSERT_EQ(0, cf_id_to_count.at(0).overwritten_sd_count);
  }
  ASSERT_OK(batch_->Put("A", "new_val"));
  {
    auto& cf_id_to_count = batch_->GetCFStats();
    ASSERT_EQ(1, cf_id_to_count.size());
    ASSERT_EQ(overwrite ? 1 : 3, cf_id_to_count.at(0).entry_count);
    ASSERT_EQ(overwrite ? 1 : 0, cf_id_to_count.at(0).overwritten_sd_count);
  }
}

INSTANTIATE_TEST_CASE_P(WBWI, WriteBatchWithIndexTest, testing::Bool());

std::string Get(const std::string& k, std::unique_ptr<WBWIMemTable>& wbwi_mem,
                SequenceNumber snapshot_seq, bool* found_final_value,
                MergeContext* merge_context = nullptr) {
  LookupKey lkey(k, snapshot_seq);
  std::string val;
  SequenceNumber max_range_del_seqno = 0;
  SequenceNumber out_seqno = 0;
  bool is_blob_index = false;
  Status s;
  std::unique_ptr<MergeContext> merge_context_guard{new MergeContext};
  if (merge_context == nullptr) {
    merge_context = merge_context_guard.get();
  }
  *found_final_value = wbwi_mem->Get(
      lkey, &val, nullptr, nullptr, &s, merge_context, &max_range_del_seqno,
      &out_seqno, ReadOptions(), true, nullptr, &is_blob_index, true);
  if (s.ok()) {
    if (*found_final_value) {
      EXPECT_FALSE(val.empty());
      return val;
    }
    return "NOT_FOUND";
  } else if (s.IsNotFound()) {
    EXPECT_TRUE(*found_final_value);
    return "NOT_FOUND";
  }
  return s.ToString();
}

class WBWIMemTableTest : public testing::Test {};

TEST_F(WBWIMemTableTest, ReadFromWBWIMemtable) {
  // Mini stress test for read.
  // Do random 10000 put and delete operations then do some overwrite.
  // Keep track of expected state, then verify with Get, MultiGet, and Iterator.
  const Comparator* cmp = BytewiseComparator();
  Options opts;
  ImmutableOptions immutable_opts(opts);
  MutableCFOptions mutable_cf_options(opts);

  Random& rnd = *Random::GetTLSInstance();
  auto wbwi = std::make_shared<WriteBatchWithIndex>(
      cmp, 0, /*overwrite_key=*/true, 0, 0);
  std::vector<std::pair<std::string, std::string>> expected;
  const int kNumUpdate = 10000;
  expected.resize(kNumUpdate);
  for (int i = 0; i < kNumUpdate; ++i) {
    // Leave a non-existing key 9999 in between existing keys to test read.
    std::string key = i < 9999 ? DBTestBase::Key(i) : DBTestBase::Key(i + 1);
    bool del = rnd.OneIn(2);
    std::string val = del ? "NOT_FOUND" : rnd.RandomString(50);
    expected[i] = std::make_pair(key, val);
  }

  std::unique_ptr<WBWIMemTable> wbwi_mem{
      new WBWIMemTable(wbwi, cmp,
                       /*cf_id=*/0, &immutable_opts, &mutable_cf_options,
                       // stats is inaccurate but read path should still work
                       /*stat=*/{})};
  ASSERT_TRUE(wbwi_mem->IsEmpty());
  constexpr SequenceNumber visible_seq = 10002;
  constexpr SequenceNumber non_visible_seq = 1;
  const SequenceNumber seqno_lb = 2;
  constexpr WBWIMemTable::SeqnoRange assigned_seq = {seqno_lb,
                                                     seqno_lb + kNumUpdate - 1};
  wbwi_mem->AssignSequenceNumbers(assigned_seq);

  // Random insertion order
  RandomShuffle(expected.begin(), expected.end());
  bool found_final_value = false;
  for (const auto& [key, val] : expected) {
    if (val == "NOT_FOUND") {
      if (rnd.OneIn(2)) {
        ASSERT_OK(wbwi->SingleDelete(key));
      } else {
        ASSERT_OK(wbwi->Delete(key));
      }
    } else {
      ASSERT_OK(wbwi->Put(key, val));
    }
    found_final_value = false;
    // We are writing to wbwi after WBWIMemtable is created. This won't
    // happen with normal usage, but we just use the hack for testing here.
    ASSERT_TRUE(val == Get(key, wbwi_mem, visible_seq, &found_final_value));
    ASSERT_TRUE(found_final_value);
  }
  ASSERT_FALSE(wbwi_mem->IsEmpty());

  // Some data with same key in another CF
  ColumnFamilyHandleImplDummy meta_cf(/*id=*/1, BytewiseComparator());
  ASSERT_OK(wbwi->Put(&meta_cf, DBTestBase::Key(0), "foo"));

  // Sort keys to compare with iterator
  std::sort(expected.begin(), expected.end(),
            [](const std::pair<std::string, std::string>& a,
               const std::pair<std::string, std::string>& b) {
              return a.first < b.first;
            });

  // overwrites
  std::vector<SequenceNumber> expected_seqno(kNumUpdate, seqno_lb);
  for (size_t i = 0; i < 2000; ++i) {
    // We don't expect to mix SD and DEL, or issue multiple SD consecutively in
    // a DB. Read from WBWI should still work so we do it here to keep the test
    // simple.
    size_t idx = rnd.Uniform(kNumUpdate);
    if (rnd.OneIn(2)) {
      std::string val = rnd.RandomString(100);
      expected[idx].second = val;
      ASSERT_OK(wbwi->Put(expected[idx].first, val));
    } else {
      expected[idx].second = "NOT_FOUND";
      if (rnd.OneIn(2)) {
        ASSERT_OK(wbwi->SingleDelete(expected[idx].first));
      } else {
        ASSERT_OK(wbwi->Delete(expected[idx].first));
      }
    }
    found_final_value = false;
    ASSERT_TRUE(expected[idx].second == Get(expected[idx].first, wbwi_mem,
                                            visible_seq, &found_final_value));
    ASSERT_TRUE(found_final_value);
    // See comment for WBWIMemTable for sequence number assignment method.
    expected_seqno[idx]++;
  }
  AssertWBWICountEQWBCount(*wbwi);
  // Get a non-existing key
  found_final_value = false;
  ASSERT_EQ("NOT_FOUND", Get("foo", wbwi_mem, visible_seq, &found_final_value));
  ASSERT_FALSE(found_final_value);
  ASSERT_EQ("NOT_FOUND", Get(DBTestBase::Key(9999), wbwi_mem, visible_seq,
                             &found_final_value));
  ASSERT_FALSE(found_final_value);
  // Get with a non-visible snapshot
  found_final_value = false;
  ASSERT_EQ("NOT_FOUND", Get(DBTestBase::Key(0), wbwi_mem, non_visible_seq,
                             &found_final_value));
  ASSERT_FALSE(found_final_value);
  // Get existing keys
  for (const auto& [key, val] : expected) {
    found_final_value = false;
    ASSERT_TRUE(val == Get(key, wbwi_mem, visible_seq, &found_final_value));
    ASSERT_TRUE(found_final_value);
  }
  // MultiGet
  int batch_size = 30;
  for (int i = 0; i < 10000; i += batch_size) {
    for (uint64_t read_seq : {non_visible_seq, visible_seq}) {
      autovector<KeyContext> key_context;
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
      sorted_keys.resize(batch_size);
      std::vector<PinnableSlice> values(batch_size);
      std::vector<Status> statuses(batch_size);
      std::vector<Slice> key_slice(batch_size);
      std::vector<std::string> key_str(batch_size);
      for (int j = 0; j < batch_size; ++j) {
        if (i + j >= 10000) {
          // read non-existing keys
          // the last key in expected is 10000
          key_str[i + j - 10000] = DBTestBase::Key(i + j + 1);
          key_slice[j] = key_str[i + j - 10000];
        } else {
          key_slice[j] = expected[i + j].first;
        }
        key_context.emplace_back(/*col_family=*/nullptr, key_slice[j],
                                 &values[j], /*cols=*/nullptr, /*ts=*/nullptr,
                                 &statuses[j]);
      }
      for (int j = 0; j < batch_size; ++j) {
        sorted_keys[j] = &key_context[j];
      }
      std::sort(sorted_keys.begin(), sorted_keys.begin() + batch_size,
                [](const KeyContext* a, const KeyContext* b) {
                  return a->key->compare(*b->key) < 0;
                });

      MultiGetContext ctx(&sorted_keys, 0, batch_size, read_seq, ReadOptions(),
                          immutable_opts.fs.get(),
                          immutable_opts.statistics.get());
      MultiGetRange range = ctx.GetMultiGetRange();
      wbwi_mem->MultiGet(ReadOptions(), &range, /*callback=*/nullptr,
                         /*immutable_memtable=*/true);
      for (int j = 0; j < batch_size; ++j) {
        if (read_seq != visible_seq || i + j >= 10000) {
          // Nothing is found in WBWIMemtable, status and value are not set.
          ASSERT_OK(statuses[j]);
          ASSERT_TRUE(values[j].empty());
        } else if (expected[i + j].second == "NOT_FOUND") {
          ASSERT_TRUE(statuses[j].IsNotFound());
        } else {
          ASSERT_OK(statuses[j]);
          ASSERT_EQ(values[j], expected[i + j].second);
        }
      }
    }
  }

  Arena arena;
  InternalIterator* iter = wbwi_mem->NewIterator(
      ReadOptions(), /*seqno_to_time_mapping=*/nullptr, &arena,
      /*prefix_extractor=*/nullptr, /*for_flush=*/false);
  ASSERT_OK(iter->status());

  auto verify_iter_at = [&](size_t idx) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(ExtractUserKey(iter->key()), expected[idx].first);

    SequenceNumber seq;
    ValueType val_type;
    UnPackSequenceAndType(ExtractInternalKeyFooter(iter->key()), &seq,
                          &val_type);
    ASSERT_EQ(seq, expected_seqno[idx]);
    if (expected[idx].second == "NOT_FOUND") {
      ASSERT_TRUE(val_type == kTypeDeletion || val_type == kTypeSingleDeletion);
    } else {
      ASSERT_EQ(val_type, kTypeValue);
      ASSERT_EQ(iter->value(), expected[idx].second);
    }
  };

  // Seek then next, prev
  IterKey seek_key;
  for (int i = 0; i < 1000; i++) {
    uint32_t key_idx = rnd.Uniform(10000);
    for (bool seek_for_prev : {false, true}) {
      if (seek_for_prev) {
        seek_key.SetInternalKey(expected[key_idx].first, 0,
                                kValueTypeForSeekForPrev);
        iter->SeekForPrev(seek_key.GetInternalKey());
      } else {
        seek_key.SetInternalKey(expected[key_idx].first, visible_seq,
                                kValueTypeForSeek);
        iter->Seek(seek_key.GetInternalKey());
      }
      verify_iter_at(key_idx);

      // verify next/prev 5 times
      for (int j = 0; j < 5; ++j) {
        if (++key_idx >= 10000) {
          --key_idx;
          break;
        }
        iter->Next();
        verify_iter_at(key_idx);
      }
      for (int j = 0; j < 5; ++j) {
        iter->Prev();
        assert(key_idx >= 1);
        verify_iter_at(--key_idx);
      }
    }
  }

  iter->SeekToFirst();
  for (size_t i = 0; i < expected.size(); ++i) {
    verify_iter_at(i);
    iter->Next();
  }
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());
  iter->SeekToLast();
  for (int i = static_cast<int>(expected.size() - 1); i >= 0; --i) {
    verify_iter_at(i);
    iter->Prev();
  }
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());

  // Read from another CF
  std::unique_ptr<WBWIMemTable> meta_wbwi_mem{new WBWIMemTable(
      wbwi, cmp, /*cf_id=*/1, &immutable_opts, &mutable_cf_options,
      /*stat=*/{1, 0})};
  meta_wbwi_mem->AssignSequenceNumbers(assigned_seq);
  found_final_value = false;
  ASSERT_TRUE("foo" == Get(DBTestBase::Key(0), meta_wbwi_mem, visible_seq,
                           &found_final_value));
  ASSERT_TRUE(found_final_value);
  found_final_value = false;
  ASSERT_TRUE("NOT_FOUND" == Get(DBTestBase::Key(1), meta_wbwi_mem, visible_seq,
                                 &found_final_value));
  ASSERT_FALSE(found_final_value);
  // Deleting one memtable should not affect another memtable with the same wbwi
  wbwi_mem.reset();
  // allocated by arena
  iter->~InternalIterator();
  iter = meta_wbwi_mem->NewIterator(ReadOptions(), nullptr, &arena, nullptr,
                                    /*for_flush=*/false);
  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(ExtractUserKey(iter->key()), DBTestBase::Key(0));
  ASSERT_EQ(iter->value(), "foo");
  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());
  iter->~InternalIterator();
}

TEST_F(WBWIMemTableTest, IterEmitSingleDelete) {
  const Comparator* cmp = BytewiseComparator();
  Options opts;
  ImmutableOptions immutable_opts(opts);
  MutableCFOptions mutable_cf_options(opts);

  auto wbwi = std::make_shared<WriteBatchWithIndex>(
      cmp, 0, /*overwrite_key=*/true, 0, 0);

  ASSERT_OK(wbwi->Put(DBTestBase::Key(0), "val0"));
  ASSERT_OK(wbwi->SingleDelete(DBTestBase::Key(0)));
  ASSERT_OK(wbwi->SingleDelete(DBTestBase::Key(1)));
  ASSERT_OK(wbwi->SingleDelete(DBTestBase::Key(2)));
  // SD at key1 overwritten
  ASSERT_OK(wbwi->Put(DBTestBase::Key(1), "val1"));
  // For key3, most recent update is SD, and a SD is overwritten,
  // here we test that these two SD get assigned different seqnos.
  // FIXME: ideally we should only emit at most one SD per key.
  ASSERT_OK(wbwi->SingleDelete(DBTestBase::Key(3)));
  ASSERT_OK(wbwi->Put(DBTestBase::Key(3), "val3"));
  ASSERT_OK(wbwi->SingleDelete(DBTestBase::Key(3)));

  std::unique_ptr<WBWIMemTable> wbwi_mem{
      new WBWIMemTable(wbwi, cmp,
                       /*cf_id=*/0, &immutable_opts, &mutable_cf_options,
                       /*stat=*/wbwi->GetCFStats().at(0))};
  WBWIMemTable::SeqnoRange assigned_seqno = {
      1, 1 + wbwi->GetWriteBatch()->Count()};
  wbwi_mem->AssignSequenceNumbers(assigned_seqno);
  Arena arena;
  InternalIterator* iter_for_flush = wbwi_mem->NewIterator(
      ReadOptions(), /*seqno_to_time_mapping=*/nullptr, &arena,
      /*prefix_extractor=*/nullptr, /*for_flush=*/true);
  InternalIterator* iter = wbwi_mem->NewIterator(
      ReadOptions(), /*seqno_to_time_mapping=*/nullptr, &arena,
      /*prefix_extractor=*/nullptr, /*for_flush=*/false);
  iter->SeekToFirst();
  iter_for_flush->SeekToFirst();
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter_for_flush->Valid());
    ASSERT_EQ(iter->key(), iter_for_flush->key());

    if (i == 1 || i == 3) {
      // Most recent update should have a higher seqno than the overwritten SD
      SequenceNumber current_seq;
      ValueType _;
      UnPackSequenceAndType(ExtractInternalKeyFooter(iter->key()), &current_seq,
                            &_);
      ASSERT_GT(current_seq, assigned_seqno.lower_bound);

      iter_for_flush->Next();
      // overwritten SD at key1 and key3
      // See WBWIMemTableIterator::UpdateSingleDeleteKey() for seqno assignment
      InternalKey ikey(DBTestBase::Key(i), assigned_seqno.lower_bound,
                       kTypeSingleDeletion);
      ASSERT_EQ(ikey.Encode(), iter_for_flush->key());
    }
    iter->Next();
    iter_for_flush->Next();
  }
  ASSERT_FALSE(iter->Valid());
  ASSERT_FALSE(iter_for_flush->Valid());
  ASSERT_OK(iter->status());
  ASSERT_OK(iter_for_flush->status());
  iter->~InternalIterator();
  iter_for_flush->~InternalIteratorBase();
}

void VerifyIterator(
    InternalIterator* iter,
    const std::vector<std::pair<std::string, std::string>>& expected) {
  // Verify SeekToFirst and Next
  iter->SeekToFirst();
  auto k = expected.begin();
  while (iter->Valid()) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    ASSERT_EQ(iter->key(), k->first);
    ASSERT_EQ(iter->value(), k->second);

    iter->Next();
    ++k;
  }
  ASSERT_OK(iter->status());
  ASSERT_TRUE(k == expected.end());

  // Verify SeekToLast and Prev
  iter->SeekToLast();
  k = expected.end();
  while (iter->Valid()) {
    --k;
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    ASSERT_EQ(iter->key(), k->first);
    ASSERT_EQ(iter->value(), k->second);

    iter->Prev();
  }
  ASSERT_OK(iter->status());
  ASSERT_TRUE(k == expected.begin());

  // Verify Seek and SeekForPrev
  for (auto exp = expected.begin(); exp != expected.end(); ++exp) {
    iter->Seek(exp->first);
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), exp->first);
    ASSERT_EQ(iter->value(), exp->second);

    iter->Next();
    if (iter->Valid()) {
      ASSERT_OK(iter->status());
      ++exp;
      ASSERT_EQ(iter->key(), exp->first);
      ASSERT_EQ(iter->value(), exp->second);
      iter->Prev();
      --exp;
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), exp->first);
      ASSERT_EQ(iter->value(), exp->second);
    } else {
      iter->SeekToLast();
    }

    iter->SeekForPrev(exp->first);
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), exp->first);
    ASSERT_EQ(iter->value(), exp->second);

    iter->Prev();
    if (iter->Valid()) {
      ASSERT_OK(iter->status());
      --exp;
      ASSERT_EQ(iter->key(), exp->first);
      ASSERT_EQ(iter->value(), exp->second);
      iter->Next();
      ++exp;
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), exp->first);
      ASSERT_EQ(iter->value(), exp->second);
    } else {
      iter->SeekToFirst();
    }
  }
}

TEST_F(WBWIMemTableTest, WBWIMemTableWithMerge) {
  const Comparator* cmp = BytewiseComparator();
  Options opts;
  opts.merge_operator = MergeOperators::CreateFromStringId("stringappend");
  ImmutableOptions immutable_opts(opts);
  MutableCFOptions mutable_cf_options(opts);

  auto wbwi = std::make_shared<WriteBatchWithIndex>(
      cmp, 0, /*overwrite_key=*/true, 0, 0);
  std::unique_ptr<WBWIMemTable> wbwi_mem{
      new WBWIMemTable(wbwi, cmp,
                       /*cf_id=*/0, &immutable_opts, &mutable_cf_options,
                       // stats is inaccurate but read path should still work
                       /*stat=*/{})};
  ASSERT_TRUE(wbwi_mem->IsEmpty());
  constexpr SequenceNumber seqno_lb = 10;
  constexpr SequenceNumber seqno_ub = 100;
  constexpr WBWIMemTable::SeqnoRange assigned_seq = {seqno_lb, seqno_ub};
  wbwi_mem->AssignSequenceNumbers(assigned_seq);

  // Update then Merge
  ASSERT_OK(wbwi->Put("a", "a1"));
  ASSERT_OK(wbwi->Merge("a", "a2"));
  ASSERT_OK(wbwi->Merge("a", "a3"));
  ASSERT_OK(wbwi->Delete("b"));
  ASSERT_OK(wbwi->Merge("b", "b1"));

  // Merge then Update
  ASSERT_OK(wbwi->Merge("c", "c1"));
  ASSERT_OK(wbwi->Put("c", "c2"));
  ASSERT_OK(wbwi->Merge("d", "d1"));
  ASSERT_OK(wbwi->Merge("d", "d2"));
  ASSERT_OK(wbwi->Delete("d"));

  // Just Merge
  ASSERT_OK(wbwi->Merge("e", "e1"));
  ASSERT_OK(wbwi->Merge("f", "f1"));
  ASSERT_OK(wbwi->Merge("f", "f2"));

  // Just Update
  ASSERT_OK(wbwi->SingleDelete("g"));

  // key <-> val
  // Refer to the sequence number assignment method described in the comments
  // above the WBWIMemTable class.
  std::vector<std::pair<std::string, std::string>> expected = {
      {InternalKey("a", seqno_lb + 2, kTypeMerge).Encode().ToString(), "a3"},
      {InternalKey("a", seqno_lb + 1, kTypeMerge).Encode().ToString(), "a2"},
      {InternalKey("a", seqno_lb, kTypeValue).Encode().ToString(), "a1"},
      {InternalKey("b", seqno_lb + 1, kTypeMerge).Encode().ToString(), "b1"},
      {InternalKey("b", seqno_lb, kTypeDeletion).Encode().ToString(), ""},
      {InternalKey("c", seqno_lb + 1, kTypeValue).Encode().ToString(), "c2"},
      {InternalKey("d", seqno_lb + 2, kTypeDeletion).Encode().ToString(), ""},
      {InternalKey("d", seqno_lb, kTypeMerge).Encode().ToString(), "d1"},
      {InternalKey("e", seqno_lb, kTypeMerge).Encode().ToString(), "e1"},
      {InternalKey("f", seqno_lb + 1, kTypeMerge).Encode().ToString(), "f2"},
      {InternalKey("f", seqno_lb, kTypeMerge).Encode().ToString(), "f1"},
      {InternalKey("g", seqno_lb, kTypeSingleDeletion).Encode().ToString(), ""},
  };

  Arena arena;
  InternalIterator* iter = wbwi_mem->NewIterator(
      ReadOptions(), /*seqno_to_time_mapping=*/nullptr, &arena,
      /*prefix_extractor=*/nullptr, /*for_flush=*/false);
  VerifyIterator(iter, expected);
  iter->~InternalIterator();

  // Test Get
  bool found_final_value = false;
  ASSERT_EQ("a1,a2,a3", Get("a", wbwi_mem, seqno_ub, &found_final_value));
  ASSERT_EQ("b1", Get("b", wbwi_mem, seqno_ub, &found_final_value));
  ASSERT_EQ("c2", Get("c", wbwi_mem, seqno_ub, &found_final_value));
  ASSERT_EQ("NOT_FOUND", Get("d", wbwi_mem, seqno_ub, &found_final_value));
  MergeContext merge_context;
  ASSERT_EQ(Status::MergeInProgress().ToString(),
            Get("e", wbwi_mem, seqno_ub, &found_final_value, &merge_context));
  ASSERT_EQ(merge_context.GetNumOperands(), 1);
  ASSERT_EQ(merge_context.GetOperand(0), "e1");
  merge_context.Clear();
  ASSERT_EQ(Status::MergeInProgress().ToString(),
            Get("f", wbwi_mem, seqno_ub, &found_final_value, &merge_context));
  ASSERT_EQ(merge_context.GetNumOperands(), 2);
  ASSERT_EQ(merge_context.GetOperand(0), "f1");
  ASSERT_EQ(merge_context.GetOperand(1), "f2");
  ASSERT_EQ("NOT_FOUND", Get("g", wbwi_mem, seqno_ub, &found_final_value));
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
