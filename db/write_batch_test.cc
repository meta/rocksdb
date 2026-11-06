//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <memory>
#include <unordered_map>

#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/memtable.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "db/write_batch_internal.h"
#include "dbformat.h"
#include "port/mmap.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

static std::string PrintContents(WriteBatch* b,
                                 bool merge_operator_supported = true) {
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  Options options;
  options.memtable_factory = factory;
  if (merge_operator_supported) {
    options.merge_operator.reset(new TestPutOperator());
  }
  ImmutableOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                               kMaxSequenceNumber, 0 /* column_family_id */,
                               nullptr /* flush_initiator */);
  mem->Ref();
  std::string state;
  ColumnFamilyMemTablesDefault cf_mems_default(mem);
  Status s =
      WriteBatchInternal::InsertInto(b, &cf_mems_default, nullptr, nullptr);
  uint32_t count = 0;
  int put_count = 0;
  int timed_put_count = 0;
  int delete_count = 0;
  int single_delete_count = 0;
  int delete_range_count = 0;
  int merge_count = 0;
  for (int i = 0; i < 2; ++i) {
    Arena arena;
    ScopedArenaPtr<InternalIterator> arena_iter_guard;
    std::unique_ptr<InternalIterator> iter_guard;
    InternalIterator* iter;
    if (i == 0) {
      iter = mem->NewIterator(ReadOptions(), /*seqno_to_time_mapping=*/nullptr,
                              &arena, /*prefix_extractor=*/nullptr,
                              /*for_flush=*/false);
      arena_iter_guard.reset(iter);
    } else {
      iter = mem->NewRangeTombstoneIterator(ReadOptions(),
                                            kMaxSequenceNumber /* read_seq */,
                                            false /* immutable_memtable */);
      iter_guard.reset(iter);
    }
    if (iter == nullptr) {
      continue;
    }
    EXPECT_OK(iter->status());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ParsedInternalKey ikey;
      ikey.clear();
      EXPECT_OK(ParseInternalKey(iter->key(), &ikey, true /* log_err_key */));
      switch (ikey.type) {
        case kTypeValue:
          state.append("Put(");
          state.append(ikey.user_key.ToString());
          state.append(", ");
          state.append(iter->value().ToString());
          state.append(")");
          count++;
          put_count++;
          break;
        case kTypeDeletion:
          state.append("Delete(");
          state.append(ikey.user_key.ToString());
          state.append(")");
          count++;
          delete_count++;
          break;
        case kTypeSingleDeletion:
          state.append("SingleDelete(");
          state.append(ikey.user_key.ToString());
          state.append(")");
          count++;
          single_delete_count++;
          break;
        case kTypeRangeDeletion:
          state.append("DeleteRange(");
          state.append(ikey.user_key.ToString());
          state.append(", ");
          state.append(iter->value().ToString());
          state.append(")");
          count++;
          delete_range_count++;
          break;
        case kTypeMerge:
          state.append("Merge(");
          state.append(ikey.user_key.ToString());
          state.append(", ");
          state.append(iter->value().ToString());
          state.append(")");
          count++;
          merge_count++;
          break;
        case kTypeValuePreferredSeqno: {
          state.append("TimedPut(");
          state.append(ikey.user_key.ToString());
          state.append(", ");
          auto [unpacked_value, unix_write_time] =
              ParsePackedValueWithWriteTime(iter->value());
          state.append(unpacked_value.ToString());
          state.append(", ");
          state.append(std::to_string(unix_write_time));
          state.append(")");
          count++;
          timed_put_count++;
          break;
        }
        default:
          assert(false);
          break;
      }
      state.append("@");
      state.append(std::to_string(ikey.sequence));
    }
    EXPECT_OK(iter->status());
  }
  if (s.ok()) {
    EXPECT_EQ(b->HasPut(), put_count > 0);
    EXPECT_EQ(b->HasTimedPut(), timed_put_count > 0);
    EXPECT_EQ(b->HasDelete(), delete_count > 0);
    EXPECT_EQ(b->HasSingleDelete(), single_delete_count > 0);
    EXPECT_EQ(b->HasDeleteRange(), delete_range_count > 0);
    EXPECT_EQ(b->HasMerge(), merge_count > 0);
    if (count != WriteBatchInternal::Count(b)) {
      state.append("CountMismatch()");
    }
  } else {
    state.append(s.ToString());
  }
  delete mem->Unref();
  return state;
}

class WriteBatchTest : public testing::Test {};

TEST_F(WriteBatchTest, Empty) {
  WriteBatch batch;
  ASSERT_EQ("", PrintContents(&batch));
  ASSERT_EQ(0u, WriteBatchInternal::Count(&batch));
  ASSERT_EQ(0u, batch.Count());
}

TEST_F(WriteBatchTest, Multiple) {
  WriteBatch batch;
  ASSERT_OK(batch.Put(Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Delete(Slice("box")));
  ASSERT_OK(batch.DeleteRange(Slice("bar"), Slice("foo")));
  ASSERT_OK(batch.Put(Slice("baz"), Slice("boo")));
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ(100U, WriteBatchInternal::Sequence(&batch));
  ASSERT_EQ(4u, WriteBatchInternal::Count(&batch));
  ASSERT_EQ(
      "Put(baz, boo)@103"
      "Delete(box)@101"
      "Put(foo, bar)@100"
      "DeleteRange(bar, foo)@102",
      PrintContents(&batch));
  ASSERT_EQ(4u, batch.Count());
}

TEST_F(WriteBatchTest, Corruption) {
  WriteBatch batch;
  ASSERT_OK(batch.Put(Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Delete(Slice("box")));
  WriteBatchInternal::SetSequence(&batch, 200);
  Slice contents = WriteBatchInternal::Contents(&batch);
  ASSERT_OK(WriteBatchInternal::SetContents(
      &batch, Slice(contents.data(), contents.size() - 1)));
  ASSERT_EQ(
      "Put(foo, bar)@200"
      "Corruption: bad WriteBatch Delete",
      PrintContents(&batch));
}

TEST_F(WriteBatchTest, TimedPutCorruption) {
  WriteBatch batch;
  ASSERT_OK(batch.Put(Slice("foo"), Slice("bar")));
  WriteBatchInternal::SetSequence(&batch, 200);
  // Append a TimedPut record whose packed value is shorter than the fixed
  // size write time trailer.
  std::string contents = WriteBatchInternal::Contents(&batch).ToString();
  contents.push_back(static_cast<char>(kTypeValuePreferredSeqno));
  PutLengthPrefixedSlice(&contents, "box");
  PutLengthPrefixedSlice(&contents, "short");
  ASSERT_OK(WriteBatchInternal::SetContents(&batch, contents));
  WriteBatchInternal::SetCount(&batch, 2);
  ASSERT_EQ(
      "Put(foo, bar)@200"
      "Corruption: bad WriteBatch TimedPut",
      PrintContents(&batch));
}

TEST_F(WriteBatchTest, Append) {
  WriteBatch b1;
  WriteBatch b2;
  WriteBatchInternal::SetSequence(&b1, 200);
  WriteBatchInternal::SetSequence(&b2, 300);
  ASSERT_OK(WriteBatchInternal::Append(&b1, &b2));
  ASSERT_EQ("", PrintContents(&b1));
  ASSERT_EQ(0u, b1.Count());
  ASSERT_OK(b2.Put("a", "va"));
  ASSERT_OK(WriteBatchInternal::Append(&b1, &b2));
  ASSERT_EQ("Put(a, va)@200", PrintContents(&b1));
  ASSERT_EQ(1u, b1.Count());
  b2.Clear();
  ASSERT_OK(b2.Put("b", "vb"));
  ASSERT_OK(WriteBatchInternal::Append(&b1, &b2));
  ASSERT_EQ(
      "Put(a, va)@200"
      "Put(b, vb)@201",
      PrintContents(&b1));
  ASSERT_EQ(2u, b1.Count());
  ASSERT_OK(b2.Delete("foo"));
  ASSERT_OK(WriteBatchInternal::Append(&b1, &b2));
  ASSERT_EQ(
      "Put(a, va)@200"
      "Put(b, vb)@202"
      "Put(b, vb)@201"
      "Delete(foo)@203",
      PrintContents(&b1));
  ASSERT_EQ(4u, b1.Count());
  b2.Clear();
  ASSERT_OK(b2.Put("c", "cc"));
  ASSERT_OK(b2.Put("d", "dd"));
  b2.MarkWalTerminationPoint();
  ASSERT_OK(b2.Put("e", "ee"));
  ASSERT_OK(WriteBatchInternal::Append(&b1, &b2, /*wal only*/ true));
  ASSERT_EQ(
      "Put(a, va)@200"
      "Put(b, vb)@202"
      "Put(b, vb)@201"
      "Put(c, cc)@204"
      "Put(d, dd)@205"
      "Delete(foo)@203",
      PrintContents(&b1));
  ASSERT_EQ(6u, b1.Count());
  ASSERT_EQ(
      "Put(c, cc)@0"
      "Put(d, dd)@1"
      "Put(e, ee)@2",
      PrintContents(&b2));
  ASSERT_EQ(3u, b2.Count());
}

TEST_F(WriteBatchTest, SingleDeletion) {
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ("", PrintContents(&batch));
  ASSERT_EQ(0u, batch.Count());
  ASSERT_OK(batch.Put("a", "va"));
  ASSERT_EQ("Put(a, va)@100", PrintContents(&batch));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_OK(batch.SingleDelete("a"));
  ASSERT_EQ(
      "SingleDelete(a)@101"
      "Put(a, va)@100",
      PrintContents(&batch));
  ASSERT_EQ(2u, batch.Count());
}

TEST_F(WriteBatchTest, OwnershipTransfer) {
  Random rnd(301);
  WriteBatch put_batch;
  ASSERT_OK(put_batch.Put(rnd.RandomString(16) /* key */,
                          rnd.RandomString(1024) /* value */));

  // (1) Verify `Release()` transfers string data ownership
  const char* expected_data = put_batch.Data().data();
  std::string batch_str = put_batch.Release();
  ASSERT_EQ(expected_data, batch_str.data());

  // (2) Verify constructor transfers string data ownership
  WriteBatch move_batch(std::move(batch_str));
  ASSERT_EQ(expected_data, move_batch.Data().data());
}

namespace {
struct TestHandler : public WriteBatch::Handler {
  std::string seen;
  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    if (column_family_id == 0) {
      seen += "Put(" + key.ToString() + ", " + value.ToString() + ")";
    } else {
      seen += "PutCF(" + std::to_string(column_family_id) + ", " +
              key.ToString() + ", " + value.ToString() + ")";
    }
    return Status::OK();
  }
  Status TimedPutCF(uint32_t column_family_id, const Slice& key,
                    const Slice& value, uint64_t unix_write_time) override {
    if (column_family_id == 0) {
      seen += "TimedPut(" + key.ToString() + ", " + value.ToString() + ", " +
              std::to_string(unix_write_time) + ")";
    } else {
      seen += "TimedPutCF(" + std::to_string(column_family_id) + ", " +
              key.ToString() + ", " + value.ToString() + ", " +
              std::to_string(unix_write_time) + ")";
    }
    return Status::OK();
  }
  Status PutEntityCF(uint32_t column_family_id, const Slice& key,
                     const Slice& entity) override {
    std::ostringstream oss;
    Status s = WideColumnsHelper::DumpSliceAsWideColumns(entity, oss, false);
    if (!s.ok()) {
      return s;
    }
    if (column_family_id == 0) {
      seen += "PutEntity(" + key.ToString() + ", " + oss.str() + ")";
    } else {
      seen += "PutEntityCF(" + std::to_string(column_family_id) + ", " +
              key.ToString() + ", " + oss.str() + ")";
    }
    return Status::OK();
  }
  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    if (column_family_id == 0) {
      seen += "Delete(" + key.ToString() + ")";
    } else {
      seen += "DeleteCF(" + std::to_string(column_family_id) + ", " +
              key.ToString() + ")";
    }
    return Status::OK();
  }
  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override {
    if (column_family_id == 0) {
      seen += "SingleDelete(" + key.ToString() + ")";
    } else {
      seen += "SingleDeleteCF(" + std::to_string(column_family_id) + ", " +
              key.ToString() + ")";
    }
    return Status::OK();
  }
  Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                       const Slice& end_key) override {
    if (column_family_id == 0) {
      seen += "DeleteRange(" + begin_key.ToString() + ", " +
              end_key.ToString() + ")";
    } else {
      seen += "DeleteRangeCF(" + std::to_string(column_family_id) + ", " +
              begin_key.ToString() + ", " + end_key.ToString() + ")";
    }
    return Status::OK();
  }
  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
    if (column_family_id == 0) {
      seen += "Merge(" + key.ToString() + ", " + value.ToString() + ")";
    } else {
      seen += "MergeCF(" + std::to_string(column_family_id) + ", " +
              key.ToString() + ", " + value.ToString() + ")";
    }
    return Status::OK();
  }
  void LogData(const Slice& blob) override {
    seen += "LogData(" + blob.ToString() + ")";
  }
  Status MarkBeginPrepare(bool unprepare) override {
    seen +=
        "MarkBeginPrepare(" + std::string(unprepare ? "true" : "false") + ")";
    return Status::OK();
  }
  Status MarkEndPrepare(const Slice& xid) override {
    seen += "MarkEndPrepare(" + xid.ToString() + ")";
    return Status::OK();
  }
  Status MarkNoop(bool empty_batch) override {
    seen += "MarkNoop(" + std::string(empty_batch ? "true" : "false") + ")";
    return Status::OK();
  }
  Status MarkCommit(const Slice& xid) override {
    seen += "MarkCommit(" + xid.ToString() + ")";
    return Status::OK();
  }
  Status MarkCommitWithTimestamp(const Slice& xid, const Slice& ts) override {
    seen += "MarkCommitWithTimestamp(" + xid.ToString() + ", " +
            ts.ToString(true) + ")";
    return Status::OK();
  }
  Status MarkRollback(const Slice& xid) override {
    seen += "MarkRollback(" + xid.ToString() + ")";
    return Status::OK();
  }
};

struct ReplayUntilCountHandler : public WriteBatch::Handler {
  explicit ReplayUntilCountHandler(uint32_t max_write_ops)
      : max_write_ops_(max_write_ops) {}

  bool Continue() override { return num_write_ops_ < max_write_ops_; }

  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    if (buffered_writes_) {
      return WriteBatchInternal::Put(buffered_writes_.get(), column_family_id,
                                     key, value);
    }
    seen += "Put(" + key.ToString() + ", " + value.ToString() + ")";
    ++num_write_ops_;
    return Status::OK();
  }

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    if (buffered_writes_) {
      return WriteBatchInternal::Delete(buffered_writes_.get(),
                                        column_family_id, key);
    }
    seen += "Delete(" + key.ToString() + ")";
    ++num_write_ops_;
    return Status::OK();
  }

  Status MarkBeginPrepare(bool /* unprepare */) override {
    assert(!buffered_writes_);
    buffered_writes_ = std::make_unique<WriteBatch>();
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice& xid) override {
    assert(buffered_writes_);
    prepared_writes_[xid.ToString()] = std::move(buffered_writes_);
    return Status::OK();
  }

  Status MarkNoop(bool /* empty_batch */) override { return Status::OK(); }

  Status MarkCommit(const Slice& xid) override {
    auto it = prepared_writes_.find(xid.ToString());
    if (it == prepared_writes_.end() || !it->second) {
      return Status::Corruption("Missing prepared batch for commit");
    }
    Status s = it->second->Iterate(this);
    prepared_writes_.erase(it);
    return s;
  }

  uint32_t NumWriteOps() const { return num_write_ops_; }

  std::string seen;

 private:
  const uint32_t max_write_ops_;
  uint32_t num_write_ops_ = 0;
  std::unordered_map<std::string, std::unique_ptr<WriteBatch>> prepared_writes_;
  std::unique_ptr<WriteBatch> buffered_writes_;
};
}  // anonymous namespace

TEST_F(WriteBatchTest, PutNotImplemented) {
  WriteBatch batch;
  ASSERT_OK(batch.Put(Slice("k1"), Slice("v1")));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_EQ("Put(k1, v1)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, TimedPutNotImplemented) {
  WriteBatch batch;
  ASSERT_OK(
      batch.TimedPut(0, Slice("k1"), Slice("v1"), /*write_unix_time=*/30));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_EQ("TimedPut(k1, v1, 30)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_TRUE(batch.Iterate(&handler).IsInvalidArgument());

  batch.Clear();
  ASSERT_OK(
      batch.TimedPut(0, Slice("k1"), Slice("v1"),
                     /*write_unix_time=*/std::numeric_limits<uint64_t>::max()));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_EQ("Put(k1, v1)@0", PrintContents(&batch));
}

TEST_F(WriteBatchTest, DeleteNotImplemented) {
  WriteBatch batch;
  ASSERT_OK(batch.Delete(Slice("k2")));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_EQ("Delete(k2)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, SingleDeleteNotImplemented) {
  WriteBatch batch;
  ASSERT_OK(batch.SingleDelete(Slice("k2")));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_EQ("SingleDelete(k2)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, MergeNotImplemented) {
  WriteBatch batch;
  ASSERT_OK(batch.Merge(Slice("foo"), Slice("bar")));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_EQ("Merge(foo, bar)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, MergeWithoutOperatorInsertionFailure) {
  WriteBatch batch;
  ASSERT_OK(batch.Merge(Slice("foo"), Slice("bar")));
  ASSERT_EQ(1u, batch.Count());
  ASSERT_EQ(
      "Invalid argument: Merge requires `ColumnFamilyOptions::merge_operator "
      "!= nullptr`",
      PrintContents(&batch, false /* merge_operator_supported */));
}

TEST_F(WriteBatchTest, Blob) {
  WriteBatch batch;
  ASSERT_OK(batch.Put(Slice("k1"), Slice("v1")));
  ASSERT_OK(batch.Put(Slice("k2"), Slice("v2")));
  ASSERT_OK(batch.Put(Slice("k3"), Slice("v3")));
  ASSERT_OK(batch.PutLogData(Slice("blob1")));
  ASSERT_OK(batch.Delete(Slice("k2")));
  ASSERT_OK(batch.SingleDelete(Slice("k3")));
  ASSERT_OK(batch.PutLogData(Slice("blob2")));
  ASSERT_OK(batch.Merge(Slice("foo"), Slice("bar")));
  ASSERT_EQ(6u, batch.Count());
  ASSERT_EQ(
      "Merge(foo, bar)@5"
      "Put(k1, v1)@0"
      "Delete(k2)@3"
      "Put(k2, v2)@1"
      "SingleDelete(k3)@4"
      "Put(k3, v3)@2",
      PrintContents(&batch));

  TestHandler handler;
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(
      "Put(k1, v1)"
      "Put(k2, v2)"
      "Put(k3, v3)"
      "LogData(blob1)"
      "Delete(k2)"
      "SingleDelete(k3)"
      "LogData(blob2)"
      "Merge(foo, bar)",
      handler.seen);
}

TEST_F(WriteBatchTest, PrepareCommit) {
  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::InsertNoop(&batch));
  ASSERT_OK(batch.Put(Slice("k1"), Slice("v1")));
  ASSERT_OK(batch.Put(Slice("k2"), Slice("v2")));
  batch.SetSavePoint();
  ASSERT_OK(WriteBatchInternal::MarkEndPrepare(&batch, Slice("xid1")));
  Status s = batch.RollbackToSavePoint();
  ASSERT_EQ(s, Status::NotFound());
  ASSERT_OK(WriteBatchInternal::MarkCommit(&batch, Slice("xid1")));
  ASSERT_OK(WriteBatchInternal::MarkRollback(&batch, Slice("xid1")));
  ASSERT_EQ(2u, batch.Count());

  TestHandler handler;
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(
      "MarkBeginPrepare(false)"
      "Put(k1, v1)"
      "Put(k2, v2)"
      "MarkEndPrepare(xid1)"
      "MarkCommit(xid1)"
      "MarkRollback(xid1)",
      handler.seen);
}

// Expected-state restore can target a sequence number in the middle of a traced
// multi-op write batch. Verify `Continue()` stops iteration cleanly there.
TEST_F(WriteBatchTest, ContinueStopsMidBatch) {
  WriteBatch batch;
  ASSERT_OK(batch.Put(Slice("k1"), Slice("v1")));
  ASSERT_OK(batch.Delete(Slice("k2")));
  ASSERT_OK(batch.Put(Slice("k3"), Slice("v3")));

  ReplayUntilCountHandler handler(/* max_write_ops */ 2);
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(2u, handler.NumWriteOps());
  ASSERT_EQ("Put(k1, v1)Delete(k2)", handler.seen);
}

// Regression test for restore replay stopping inside a committed prepared
// batch. The handler buffers prepare contents and replays them on commit,
// matching the expected-state restore logic.
TEST_F(WriteBatchTest, ContinueStopsMidPreparedCommitReplay) {
  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::InsertNoop(&batch));
  ASSERT_OK(batch.Put(Slice("k1"), Slice("v1")));
  ASSERT_OK(batch.Put(Slice("k2"), Slice("v2")));
  batch.SetSavePoint();
  ASSERT_OK(WriteBatchInternal::MarkEndPrepare(&batch, Slice("xid1")));
  ASSERT_EQ(Status::NotFound(), batch.RollbackToSavePoint());
  ASSERT_OK(WriteBatchInternal::MarkCommit(&batch, Slice("xid1")));

  ReplayUntilCountHandler handler(/* max_write_ops */ 1);
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(1u, handler.NumWriteOps());
  ASSERT_EQ("Put(k1, v1)", handler.seen);
}

// It requires more than 30GB of memory to run the test. With single memory
// allocation of more than 30GB.
// Not all platform can run it. Also it runs a long time. So disable it.
TEST_F(WriteBatchTest, DISABLED_ManyUpdates) {
  // Insert key and value of 3GB and push total batch size to 12GB.
  static const size_t kKeyValueSize = 4u;
  static const uint32_t kNumUpdates = uint32_t{3} << 30;
  std::string raw(kKeyValueSize, 'A');
  WriteBatch batch(kNumUpdates * (4 + kKeyValueSize * 2) + 1024u);
  char c = 'A';
  for (uint32_t i = 0; i < kNumUpdates; i++) {
    if (c > 'Z') {
      c = 'A';
    }
    raw[0] = c;
    raw[raw.length() - 1] = c;
    c++;
    ASSERT_OK(batch.Put(raw, raw));
  }

  ASSERT_EQ(kNumUpdates, batch.Count());

  struct NoopHandler : public WriteBatch::Handler {
    uint32_t num_seen = 0;
    char expected_char = 'A';
    Status PutCF(uint32_t /*column_family_id*/, const Slice& key,
                 const Slice& value) override {
      EXPECT_EQ(kKeyValueSize, key.size());
      EXPECT_EQ(kKeyValueSize, value.size());
      EXPECT_EQ(expected_char, key[0]);
      EXPECT_EQ(expected_char, value[0]);
      EXPECT_EQ(expected_char, key[kKeyValueSize - 1]);
      EXPECT_EQ(expected_char, value[kKeyValueSize - 1]);
      expected_char++;
      if (expected_char > 'Z') {
        expected_char = 'A';
      }
      ++num_seen;
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*column_family_id*/,
                    const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t /*column_family_id*/,
                          const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status MergeCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                   const Slice& /*value*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    void LogData(const Slice& /*blob*/) override { ADD_FAILURE(); }
    bool Continue() override { return num_seen < kNumUpdates; }
  } handler;

  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(kNumUpdates, handler.num_seen);
}

// The test requires more than 18GB memory to run it, with single memory
// allocation of more than 12GB. Not all the platform can run it. So disable it.
TEST_F(WriteBatchTest, DISABLED_LargeKeyValue) {
  // Insert key and value of 3GB and push total batch size to 12GB.
  static const size_t kKeyValueSize = 3221225472u;
  std::string raw(kKeyValueSize, 'A');
  WriteBatch batch(size_t(12884901888ull + 1024u));
  for (char i = 0; i < 2; i++) {
    raw[0] = 'A' + i;
    raw[raw.length() - 1] = 'A' - i;
    ASSERT_OK(batch.Put(raw, raw));
  }

  ASSERT_EQ(2u, batch.Count());

  struct NoopHandler : public WriteBatch::Handler {
    int num_seen = 0;
    Status PutCF(uint32_t /*column_family_id*/, const Slice& key,
                 const Slice& value) override {
      EXPECT_EQ(kKeyValueSize, key.size());
      EXPECT_EQ(kKeyValueSize, value.size());
      EXPECT_EQ('A' + num_seen, key[0]);
      EXPECT_EQ('A' + num_seen, value[0]);
      EXPECT_EQ('A' - num_seen, key[kKeyValueSize - 1]);
      EXPECT_EQ('A' - num_seen, value[kKeyValueSize - 1]);
      ++num_seen;
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*column_family_id*/,
                    const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t /*column_family_id*/,
                          const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status MergeCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                   const Slice& /*value*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    void LogData(const Slice& /*blob*/) override { ADD_FAILURE(); }
    bool Continue() override { return num_seen < 2; }
  } handler;

  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(2, handler.num_seen);
}

// Uses anonymous mmap (lazy-zeroed) so the large data itself doesn't consume
// physical memory -- only the destination copy does (~4GB peak during the
// acceptance case where batch.Put copies into batch.rep_).
TEST_F(WriteBatchTest, LargeKeyValueSizeLimit) {
  if (!test::HasBigMem()) {
    ROCKSDB_GTEST_BYPASS("insufficient memory for reliable continuous testing");
    return;
  }

  constexpr size_t kMaxKeySize =
      size_t{std::numeric_limits<uint32_t>::max()} - 8;
  constexpr size_t kMaxValueSize = size_t{std::numeric_limits<uint32_t>::max()};

  WriteBatch batch;

  // --- Large key ---
  {
    MemMapping mm = MemMapping::AllocateLazyZeroed(kMaxKeySize + 1);
    ASSERT_NE(nullptr, mm.Get());

    // A key at the limit should be accepted
    ASSERT_OK(batch.Put(Slice(mm.AsSlice().data(), kMaxKeySize), "val"));
    batch.Clear();

    // A key one byte over the limit should be rejected
    ASSERT_TRUE(batch.Put(mm.AsSlice(), "val").IsInvalidArgument());
    ASSERT_TRUE(batch.Merge(mm.AsSlice(), "val").IsInvalidArgument());
    ASSERT_TRUE(batch.Delete(mm.AsSlice()).IsInvalidArgument());
    ASSERT_TRUE(batch.SingleDelete(mm.AsSlice()).IsInvalidArgument());
    ASSERT_TRUE(
        batch.DeleteRange(mm.AsSlice(), mm.AsSlice()).IsInvalidArgument());
  }

  // --- Large value ---
  {
    MemMapping mm = MemMapping::AllocateLazyZeroed(kMaxValueSize + 1);
    ASSERT_NE(nullptr, mm.Get());

    // A value at the limit should be accepted
    ASSERT_OK(batch.Put("key", Slice(mm.AsSlice().data(), kMaxValueSize)));
    batch.Clear();

    // A value one byte over the limit should be rejected
    ASSERT_TRUE(batch.Put("key", mm.AsSlice()).IsInvalidArgument());
    ASSERT_TRUE(batch.Merge("key", mm.AsSlice()).IsInvalidArgument());
  }
}

TEST_F(WriteBatchTest, Continue) {
  WriteBatch batch;

  struct Handler : public TestHandler {
    int num_seen = 0;
    Status PutCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
      ++num_seen;
      return TestHandler::PutCF(column_family_id, key, value);
    }
    Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
      ++num_seen;
      return TestHandler::DeleteCF(column_family_id, key);
    }
    Status SingleDeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
      ++num_seen;
      return TestHandler::SingleDeleteCF(column_family_id, key);
    }
    Status MergeCF(uint32_t column_family_id, const Slice& key,
                   const Slice& value) override {
      ++num_seen;
      return TestHandler::MergeCF(column_family_id, key, value);
    }
    void LogData(const Slice& blob) override {
      ++num_seen;
      TestHandler::LogData(blob);
    }
    bool Continue() override { return num_seen < 5; }
  } handler;

  ASSERT_OK(batch.Put(Slice("k1"), Slice("v1")));
  ASSERT_OK(batch.Put(Slice("k2"), Slice("v2")));
  ASSERT_OK(batch.PutLogData(Slice("blob1")));
  ASSERT_OK(batch.Delete(Slice("k1")));
  ASSERT_OK(batch.SingleDelete(Slice("k2")));
  ASSERT_OK(batch.PutLogData(Slice("blob2")));
  ASSERT_OK(batch.Merge(Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(
      "Put(k1, v1)"
      "Put(k2, v2)"
      "LogData(blob1)"
      "Delete(k1)"
      "SingleDelete(k2)",
      handler.seen);
}

TEST_F(WriteBatchTest, PutGatherSlices) {
  WriteBatch batch;
  ASSERT_OK(batch.Put(Slice("foo"), Slice("bar")));

  {
    // Try a write where the key is one slice but the value is two
    Slice key_slice("baz");
    Slice value_slices[2] = {Slice("header"), Slice("payload")};
    ASSERT_OK(
        batch.Put(SliceParts(&key_slice, 1), SliceParts(value_slices, 2)));
  }

  {
    // One where the key is composite but the value is a single slice
    Slice key_slices[3] = {Slice("key"), Slice("part2"), Slice("part3")};
    Slice value_slice("value");
    ASSERT_OK(
        batch.Put(SliceParts(key_slices, 3), SliceParts(&value_slice, 1)));
  }

  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ(
      "Put(baz, headerpayload)@101"
      "Put(foo, bar)@100"
      "Put(keypart2part3, value)@102",
      PrintContents(&batch));
  ASSERT_EQ(3u, batch.Count());
}

namespace {
class ColumnFamilyHandleImplDummy : public ColumnFamilyHandleImpl {
 public:
  explicit ColumnFamilyHandleImplDummy(int id)
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr), id_(id) {}
  explicit ColumnFamilyHandleImplDummy(int id, const Comparator* ucmp)
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr),
        id_(id),
        ucmp_(ucmp) {}
  uint32_t GetID() const override { return id_; }
  const Comparator* GetComparator() const override { return ucmp_; }

 private:
  uint32_t id_;
  const Comparator* const ucmp_ = BytewiseComparator();
};
}  // anonymous namespace

TEST_F(WriteBatchTest, AttributeGroupTest) {
  WriteBatch batch;
  ColumnFamilyHandleImplDummy zero(0);
  ColumnFamilyHandleImplDummy two(2);
  AttributeGroups foo_ags;
  WideColumn zero_col_1{"0_c_1_n", "0_c_1_v"};
  WideColumn zero_col_2{"0_c_2_n", "0_c_2_v"};
  WideColumns zero_col_1_col_2{zero_col_1, zero_col_2};

  WideColumn two_col_1{"2_c_1_n", "2_c_1_v"};
  WideColumn two_col_2{"2_c_2_n", "2_c_2_v"};
  WideColumns two_col_1_col_2{two_col_1, two_col_2};

  foo_ags.emplace_back(&zero, zero_col_1_col_2);
  foo_ags.emplace_back(&two, two_col_1_col_2);

  ASSERT_OK(batch.PutEntity("foo", foo_ags));

  TestHandler handler;
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(
      "PutEntity(foo, 0_c_1_n:0_c_1_v "
      "0_c_2_n:0_c_2_v)"
      "PutEntityCF(2, foo, 2_c_1_n:2_c_1_v "
      "2_c_2_n:2_c_2_v)",
      handler.seen);
}

TEST_F(WriteBatchTest, AttributeGroupSavePointTest) {
  WriteBatch batch;
  batch.SetSavePoint();

  ColumnFamilyHandleImplDummy zero(0);
  ColumnFamilyHandleImplDummy two(2);
  ColumnFamilyHandleImplDummy three(3);
  AttributeGroups foo_ags;
  WideColumn zero_col_1{"0_c_1_n", "0_c_1_v"};
  WideColumn zero_col_2{"0_c_2_n", "0_c_2_v"};
  WideColumns zero_col_1_col_2{zero_col_1, zero_col_2};

  WideColumn two_col_1{"2_c_1_n", "2_c_1_v"};
  WideColumn two_col_2{"2_c_2_n", "2_c_2_v"};
  WideColumns two_col_1_col_2{two_col_1, two_col_2};

  foo_ags.emplace_back(&zero, zero_col_1_col_2);
  foo_ags.emplace_back(&two, two_col_1_col_2);

  AttributeGroups bar_ags;
  WideColumn three_col_1{"3_c_1_n", "3_c_1_v"};
  WideColumn three_col_2{"3_c_2_n", "3_c_2_v"};
  WideColumns three_col_1_col_2{three_col_1, three_col_2};

  bar_ags.emplace_back(&zero, zero_col_1_col_2);
  bar_ags.emplace_back(&three, three_col_1_col_2);

  ASSERT_OK(batch.PutEntity("foo", foo_ags));
  batch.SetSavePoint();

  ASSERT_OK(batch.PutEntity("bar", bar_ags));

  TestHandler handler;
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(
      "PutEntity(foo, 0_c_1_n:0_c_1_v 0_c_2_n:0_c_2_v)"
      "PutEntityCF(2, foo, 2_c_1_n:2_c_1_v 2_c_2_n:2_c_2_v)"
      "PutEntity(bar, 0_c_1_n:0_c_1_v 0_c_2_n:0_c_2_v)"
      "PutEntityCF(3, bar, 3_c_1_n:3_c_1_v 3_c_2_n:3_c_2_v)",
      handler.seen);

  ASSERT_OK(batch.RollbackToSavePoint());

  handler.seen.clear();
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(
      "PutEntity(foo, 0_c_1_n:0_c_1_v 0_c_2_n:0_c_2_v)"
      "PutEntityCF(2, foo, 2_c_1_n:2_c_1_v 2_c_2_n:2_c_2_v)",
      handler.seen);
}

TEST_F(WriteBatchTest, IterateCanRebuildSerializedV2Entity) {
  WriteBatch batch;

  BlobIndex blob_index;
  std::string encoded_blob_index;
  BlobIndex::EncodeBlob(&encoded_blob_index, 9 /* file_number */,
                        123 /* offset */, 456 /* size */, kNoCompression);
  ASSERT_OK(blob_index.DecodeFrom(encoded_blob_index));

  const std::vector<std::pair<std::string, std::string>> columns = {
      {"", "default_inline"},
      {"ttl", "00000001"},
  };
  std::string serialized_entity;
  ASSERT_OK(WideColumnSerialization::SerializeV2(columns, {{0, blob_index}},
                                                 serialized_entity));
  ASSERT_OK(WriteBatchInternal::PutEntitySerialized(&batch, 7 /* cf_id */,
                                                    "key", serialized_entity));

  struct RebuildHandler : public WriteBatch::Handler {
    WriteBatch rebuilt;

    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      return WriteBatchInternal::Put(&rebuilt, cf, key, value);
    }

    Status PutEntityCF(uint32_t cf, const Slice& key,
                       const Slice& entity) override {
      return WriteBatchInternal::PutEntitySerialized(&rebuilt, cf, key, entity);
    }
  } handler;

  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(handler.rebuilt.Count(), batch.Count());
  ASSERT_EQ(handler.rebuilt.Data(), batch.Data());
}

TEST_F(WriteBatchTest, ColumnFamiliesBatchTest) {
  WriteBatch batch;
  ColumnFamilyHandleImplDummy zero(0);
  ColumnFamilyHandleImplDummy two(2);
  ColumnFamilyHandleImplDummy three(3);
  ColumnFamilyHandleImplDummy eight(8);
  ASSERT_OK(batch.Put(&zero, Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Put(&two, Slice("twofoo"), Slice("bar2")));
  ASSERT_OK(batch.Put(&eight, Slice("eightfoo"), Slice("bar8")));
  ASSERT_OK(batch.Delete(&eight, Slice("eightfoo")));
  ASSERT_OK(batch.SingleDelete(&two, Slice("twofoo")));
  ASSERT_OK(batch.DeleteRange(&two, Slice("3foo"), Slice("4foo")));
  ASSERT_OK(batch.Merge(&three, Slice("threethree"), Slice("3three")));
  ASSERT_OK(batch.Put(&zero, Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Merge(Slice("omom"), Slice("nom")));
  ASSERT_OK(batch.TimedPut(&zero, Slice("foo"), Slice("bar"),
                           /*write_unix_time*/ 0u));

  TestHandler handler;
  ASSERT_OK(batch.Iterate(&handler));
  ASSERT_EQ(
      "Put(foo, bar)"
      "PutCF(2, twofoo, bar2)"
      "PutCF(8, eightfoo, bar8)"
      "DeleteCF(8, eightfoo)"
      "SingleDeleteCF(2, twofoo)"
      "DeleteRangeCF(2, 3foo, 4foo)"
      "MergeCF(3, threethree, 3three)"
      "Put(foo, bar)"
      "Merge(omom, nom)"
      "TimedPut(foo, bar, 0)",
      handler.seen);
}

TEST_F(WriteBatchTest, ColumnFamiliesBatchWithIndexTest) {
  WriteBatchWithIndex batch;
  ColumnFamilyHandleImplDummy zero(0);
  ColumnFamilyHandleImplDummy two(2);
  ColumnFamilyHandleImplDummy three(3);
  ColumnFamilyHandleImplDummy eight(8);
  ASSERT_OK(batch.Put(&zero, Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Put(&two, Slice("twofoo"), Slice("bar2")));
  ASSERT_OK(batch.Put(&eight, Slice("eightfoo"), Slice("bar8")));
  ASSERT_OK(batch.Delete(&eight, Slice("eightfoo")));
  ASSERT_OK(batch.SingleDelete(&two, Slice("twofoo")));
  ASSERT_OK(batch.Merge(&three, Slice("threethree"), Slice("3three")));
  ASSERT_OK(batch.Put(&zero, Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Merge(Slice("omom"), Slice("nom")));
  ASSERT_TRUE(
      batch.TimedPut(&zero, Slice("foo"), Slice("bar"), 0u).IsNotSupported());

  std::unique_ptr<WBWIIterator> iter;

  iter.reset(batch.NewIterator(&eight));
  iter->Seek("eightfoo");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  // For the same key, most recent update is ordered first.
  ASSERT_EQ(WriteType::kDeleteRecord, iter->Entry().type);
  ASSERT_EQ("eightfoo", iter->Entry().key.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("eightfoo", iter->Entry().key.ToString());
  ASSERT_EQ("bar8", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  iter.reset(batch.NewIterator(&two));
  iter->Seek("twofoo");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kSingleDeleteRecord, iter->Entry().type);
  ASSERT_EQ("twofoo", iter->Entry().key.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("twofoo", iter->Entry().key.ToString());
  ASSERT_EQ("bar2", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  iter.reset(batch.NewIterator());
  iter->Seek("gggg");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kMergeRecord, iter->Entry().type);
  ASSERT_EQ("omom", iter->Entry().key.ToString());
  ASSERT_EQ("nom", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  iter.reset(batch.NewIterator(&zero));
  iter->Seek("foo");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("foo", iter->Entry().key.ToString());
  ASSERT_EQ("bar", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("foo", iter->Entry().key.ToString());
  ASSERT_EQ("bar", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kMergeRecord, iter->Entry().type);
  ASSERT_EQ("omom", iter->Entry().key.ToString());
  ASSERT_EQ("nom", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  TestHandler handler;
  ASSERT_OK(batch.GetWriteBatch()->Iterate(&handler));
  ASSERT_EQ(
      "Put(foo, bar)"
      "PutCF(2, twofoo, bar2)"
      "PutCF(8, eightfoo, bar8)"
      "DeleteCF(8, eightfoo)"
      "SingleDeleteCF(2, twofoo)"
      "MergeCF(3, threethree, 3three)"
      "Put(foo, bar)"
      "Merge(omom, nom)",
      handler.seen);
}

TEST_F(WriteBatchTest, SavePointTest) {
  Status s;
  WriteBatch batch;
  batch.SetSavePoint();

  ASSERT_OK(batch.Put("A", "a"));
  ASSERT_OK(batch.Put("B", "b"));
  batch.SetSavePoint();

  ASSERT_OK(batch.Put("C", "c"));
  ASSERT_OK(batch.Delete("A"));
  batch.SetSavePoint();
  batch.SetSavePoint();

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Delete(A)@3"
      "Put(A, a)@0"
      "Put(B, b)@1"
      "Put(C, c)@2",
      PrintContents(&batch));

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Put(A, a)@0"
      "Put(B, b)@1",
      PrintContents(&batch));

  ASSERT_OK(batch.Delete("A"));
  ASSERT_OK(batch.Put("B", "bb"));

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ("", PrintContents(&batch));

  s = batch.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch));

  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Delete("A"));

  batch.SetSavePoint();

  ASSERT_OK(batch.Put("A", "aaa"));

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Delete(A)@1"
      "Put(D, d)@0",
      PrintContents(&batch));

  batch.SetSavePoint();

  ASSERT_OK(batch.Put("D", "d"));
  ASSERT_OK(batch.Delete("A"));

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Delete(A)@1"
      "Put(D, d)@0",
      PrintContents(&batch));

  s = batch.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(
      "Delete(A)@1"
      "Put(D, d)@0",
      PrintContents(&batch));

  WriteBatch batch2;

  s = batch2.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch2));

  ASSERT_OK(batch2.Delete("A"));
  batch2.SetSavePoint();

  s = batch2.RollbackToSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("Delete(A)@0", PrintContents(&batch2));

  batch2.Clear();
  ASSERT_EQ("", PrintContents(&batch2));

  batch2.SetSavePoint();

  ASSERT_OK(batch2.Delete("B"));
  ASSERT_EQ("Delete(B)@0", PrintContents(&batch2));

  batch2.SetSavePoint();
  s = batch2.RollbackToSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("Delete(B)@0", PrintContents(&batch2));

  s = batch2.RollbackToSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("", PrintContents(&batch2));

  s = batch2.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch2));

  WriteBatch batch3;

  s = batch3.PopSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch3));

  batch3.SetSavePoint();
  ASSERT_OK(batch3.Delete("A"));

  s = batch3.PopSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("Delete(A)@0", PrintContents(&batch3));
}

TEST_F(WriteBatchTest, MemoryLimitTest) {
  Status s;
  // The header size is 12 bytes. The two Puts take 8 bytes which gives total
  // of 12 + 8 * 2 = 28 bytes.
  WriteBatch batch(0, 28);

  ASSERT_OK(batch.Put("a", "...."));
  ASSERT_OK(batch.Put("b", "...."));
  s = batch.Put("c", "....");
  ASSERT_TRUE(s.IsMemoryLimit());
}

namespace {
class TimestampChecker : public WriteBatch::Handler {
 public:
  explicit TimestampChecker(
      std::unordered_map<uint32_t, const Comparator*> cf_to_ucmps, Slice ts)
      : cf_to_ucmps_(std::move(cf_to_ucmps)), timestamp_(std::move(ts)) {}
  Status PutCF(uint32_t cf, const Slice& key, const Slice& /*value*/) override {
    auto cf_iter = cf_to_ucmps_.find(cf);
    if (cf_iter == cf_to_ucmps_.end()) {
      return Status::Corruption();
    }
    const Comparator* const ucmp = cf_iter->second;
    assert(ucmp);
    size_t ts_sz = ucmp->timestamp_size();
    if (ts_sz == 0) {
      return Status::OK();
    }
    if (key.size() < ts_sz) {
      return Status::Corruption();
    }
    Slice ts = ExtractTimestampFromUserKey(key, ts_sz);
    if (ts.compare(timestamp_) != 0) {
      return Status::Corruption();
    }
    return Status::OK();
  }

 private:
  std::unordered_map<uint32_t, const Comparator*> cf_to_ucmps_;
  Slice timestamp_;
};

Status CheckTimestampsInWriteBatch(
    WriteBatch& wb, Slice timestamp,
    std::unordered_map<uint32_t, const Comparator*> cf_to_ucmps) {
  TimestampChecker ts_checker(cf_to_ucmps, timestamp);
  return wb.Iterate(&ts_checker);
}
}  // anonymous namespace

TEST_F(WriteBatchTest, SanityChecks) {
  ColumnFamilyHandleImplDummy cf0(0,
                                  test::BytewiseComparatorWithU64TsWrapper());
  ColumnFamilyHandleImplDummy cf4(4);

  WriteBatch wb(0, 0, 0, /*default_cf_ts_sz=*/sizeof(uint64_t));

  // Sanity checks for the new WriteBatch APIs with extra 'ts' arg.
  ASSERT_TRUE(wb.Put(nullptr, "key", "ts", "value").IsInvalidArgument());
  ASSERT_TRUE(wb.Delete(nullptr, "key", "ts").IsInvalidArgument());
  ASSERT_TRUE(wb.SingleDelete(nullptr, "key", "ts").IsInvalidArgument());
  ASSERT_TRUE(wb.Merge(nullptr, "key", "ts", "value").IsInvalidArgument());
  ASSERT_TRUE(wb.DeleteRange(nullptr, "begin_key", "end_key", "ts")
                  .IsInvalidArgument());

  ASSERT_TRUE(wb.Put(&cf4, "key", "ts", "value").IsInvalidArgument());
  ASSERT_TRUE(wb.Delete(&cf4, "key", "ts").IsInvalidArgument());
  ASSERT_TRUE(wb.SingleDelete(&cf4, "key", "ts").IsInvalidArgument());
  ASSERT_TRUE(wb.Merge(&cf4, "key", "ts", "value").IsInvalidArgument());
  ASSERT_TRUE(
      wb.DeleteRange(&cf4, "begin_key", "end_key", "ts").IsInvalidArgument());

  constexpr size_t wrong_ts_sz = 1 + sizeof(uint64_t);
  std::string ts(wrong_ts_sz, '\0');

  ASSERT_TRUE(wb.Put(&cf0, "key", ts, "value").IsInvalidArgument());
  ASSERT_TRUE(wb.Delete(&cf0, "key", ts).IsInvalidArgument());
  ASSERT_TRUE(wb.SingleDelete(&cf0, "key", ts).IsInvalidArgument());
  ASSERT_TRUE(wb.Merge(&cf0, "key", ts, "value").IsInvalidArgument());
  ASSERT_TRUE(
      wb.DeleteRange(&cf0, "begin_key", "end_key", ts).IsInvalidArgument());

  // Sanity checks for the new WriteBatch APIs without extra 'ts' arg.
  WriteBatch wb1(0, 0, 0, wrong_ts_sz);
  ASSERT_TRUE(wb1.Put(&cf0, "key", "value").IsInvalidArgument());
  ASSERT_TRUE(wb1.Delete(&cf0, "key").IsInvalidArgument());
  ASSERT_TRUE(wb1.SingleDelete(&cf0, "key").IsInvalidArgument());
  ASSERT_TRUE(wb1.Merge(&cf0, "key", "value").IsInvalidArgument());
  ASSERT_TRUE(
      wb1.DeleteRange(&cf0, "begin_key", "end_key").IsInvalidArgument());
}

TEST_F(WriteBatchTest, UpdateTimestamps) {
  // We assume the last eight bytes of each key is reserved for timestamps.
  // Therefore, we must make sure each key is longer than eight bytes.
  constexpr size_t key_size = 16;
  constexpr size_t num_of_keys = 10;
  std::vector<std::string> key_strs(num_of_keys, std::string(key_size, '\0'));

  ColumnFamilyHandleImplDummy cf0(0);
  ColumnFamilyHandleImplDummy cf4(4,
                                  test::BytewiseComparatorWithU64TsWrapper());
  ColumnFamilyHandleImplDummy cf5(5,
                                  test::BytewiseComparatorWithU64TsWrapper());

  const std::unordered_map<uint32_t, const Comparator*> cf_to_ucmps = {
      {0, cf0.GetComparator()},
      {4, cf4.GetComparator()},
      {5, cf5.GetComparator()}};

  static constexpr size_t timestamp_size = sizeof(uint64_t);

  {
    WriteBatch wb1;
    WriteBatch wb2;
    WriteBatch wb3;
    WriteBatch wb4;
    WriteBatch wb5;
    WriteBatch wb6;
    WriteBatch wb7;
    ASSERT_OK(wb1.Put(&cf0, "key", "value"));
    ASSERT_FALSE(WriteBatchInternal::HasKeyWithTimestamp(wb1));
    ASSERT_OK(wb2.Put(&cf4, "key", "value"));
    ASSERT_TRUE(WriteBatchInternal::HasKeyWithTimestamp(wb2));
    ASSERT_OK(wb3.Put(&cf4, "key", /*ts=*/std::string(timestamp_size, '\xfe'),
                      "value"));
    ASSERT_TRUE(WriteBatchInternal::HasKeyWithTimestamp(wb3));
    ASSERT_OK(wb4.Delete(&cf4, "key",
                         /*ts=*/std::string(timestamp_size, '\xfe')));
    ASSERT_TRUE(WriteBatchInternal::HasKeyWithTimestamp(wb4));
    ASSERT_OK(wb5.Delete(&cf4, "key"));
    ASSERT_TRUE(WriteBatchInternal::HasKeyWithTimestamp(wb5));
    ASSERT_OK(wb6.SingleDelete(&cf4, "key"));
    ASSERT_TRUE(WriteBatchInternal::HasKeyWithTimestamp(wb6));
    ASSERT_OK(wb7.SingleDelete(&cf4, "key",
                               /*ts=*/std::string(timestamp_size, '\xfe')));
    ASSERT_TRUE(WriteBatchInternal::HasKeyWithTimestamp(wb7));
  }

  WriteBatch batch;
  // Write to the batch. We will assign timestamps later.
  for (const auto& key_str : key_strs) {
    ASSERT_OK(batch.Put(&cf0, key_str, "value"));
    ASSERT_OK(batch.Put(&cf4, key_str, "value"));
    ASSERT_OK(batch.Put(&cf5, key_str, "value"));
  }

  const auto checker1 = [](uint32_t cf) {
    if (cf == 4 || cf == 5) {
      return timestamp_size;
    } else if (cf == 0) {
      return static_cast<size_t>(0);
    } else {
      return std::numeric_limits<size_t>::max();
    }
  };
  ASSERT_OK(
      batch.UpdateTimestamps(std::string(timestamp_size, '\xfe'), checker1));
  ASSERT_OK(CheckTimestampsInWriteBatch(
      batch, std::string(timestamp_size, '\xfe'), cf_to_ucmps));

  // We use indexed_cf_to_ucmps, non_indexed_cfs_with_ts and timestamp_size to
  // simulate the case in which a transaction enables indexing for some writes
  // while disables indexing for other writes. A transaction uses a
  // WriteBatchWithIndex object to buffer writes (we consider Write-committed
  // policy only). If indexing is enabled, then writes go through
  // WriteBatchWithIndex API populating a WBWI internal data structure, i.e. a
  // mapping from cf to user comparators. If indexing is disabled, a transaction
  // writes directly to the underlying raw WriteBatch. We will need to track the
  // comparator information for the column families to which un-indexed writes
  // are performed. When calling UpdateTimestamp API of WriteBatch, we need
  // indexed_cf_to_ucmps, non_indexed_cfs_with_ts, and timestamp_size to perform
  // checking.
  std::unordered_map<uint32_t, const Comparator*> indexed_cf_to_ucmps = {
      {0, cf0.GetComparator()}, {4, cf4.GetComparator()}};
  std::unordered_set<uint32_t> non_indexed_cfs_with_ts = {cf5.GetID()};
  const auto checker2 = [&indexed_cf_to_ucmps,
                         &non_indexed_cfs_with_ts](uint32_t cf) {
    if (non_indexed_cfs_with_ts.count(cf) > 0) {
      return timestamp_size;
    }
    auto cf_iter = indexed_cf_to_ucmps.find(cf);
    if (cf_iter == indexed_cf_to_ucmps.end()) {
      assert(false);
      return std::numeric_limits<size_t>::max();
    }
    const Comparator* const ucmp = cf_iter->second;
    assert(ucmp);
    return ucmp->timestamp_size();
  };
  ASSERT_OK(
      batch.UpdateTimestamps(std::string(timestamp_size, '\xef'), checker2));
  ASSERT_OK(CheckTimestampsInWriteBatch(
      batch, std::string(timestamp_size, '\xef'), cf_to_ucmps));
}

TEST_F(WriteBatchTest, CommitWithTimestamp) {
  WriteBatch wb;
  const std::string txn_name = "xid1";
  std::string ts;
  constexpr uint64_t commit_ts = 23;
  PutFixed64(&ts, commit_ts);
  ASSERT_OK(WriteBatchInternal::MarkCommitWithTimestamp(&wb, txn_name, ts));
  TestHandler handler;
  ASSERT_OK(wb.Iterate(&handler));
  ASSERT_EQ("MarkCommitWithTimestamp(" + txn_name + ", " +
                Slice(ts).ToString(true) + ")",
            handler.seen);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
