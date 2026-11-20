//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <array>
#include <unordered_set>
#include <vector>

#include "db/flush_scheduler.h"
#include "db/kv_checksum.h"
#include "db/trim_history_scheduler.h"
#include "db/write_thread.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/autovector.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

class MemTable;
class FlushScheduler;
class ColumnFamilyData;

class ColumnFamilyMemTables {
 public:
  virtual ~ColumnFamilyMemTables() = default;
  virtual bool Seek(uint32_t column_family_id) = 0;
  // returns true if the update to memtable should be ignored
  // (useful when recovering from log whose updates have already
  // been processed)
  virtual uint64_t GetLogNumber() const = 0;
  virtual MemTable* GetMemTable() const = 0;
  virtual ColumnFamilyHandle* GetColumnFamilyHandle() = 0;
  virtual ColumnFamilyData* current() { return nullptr; }
};

class ColumnFamilyMemTablesDefault : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesDefault(MemTable* mem)
      : ok_(false), mem_(mem) {}

  bool Seek(uint32_t column_family_id) override {
    ok_ = (column_family_id == 0);
    return ok_;
  }

  uint64_t GetLogNumber() const override { return 0; }

  MemTable* GetMemTable() const override {
    assert(ok_);
    return mem_;
  }

  ColumnFamilyHandle* GetColumnFamilyHandle() override { return nullptr; }

 private:
  bool ok_;
  MemTable* mem_;
};

struct WriteBatch::ProtectionInfo {
  // `WriteBatch` usually doesn't contain a huge number of keys so protecting
  // with a fixed, non-configurable eight bytes per key may work well enough.
  autovector<ProtectionInfoKVOC64> entries_;

  size_t GetBytesPerKey() const { return 8; }
};

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
class WriteBatchInternal {
 public:
  // WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
  static constexpr size_t kHeader = 12;

  // WriteBatch methods with column_family_id instead of ColumnFamilyHandle*
  static Status Put(WriteBatch* batch, uint32_t column_family_id,
                    const Slice& key, const Slice& value);

  static Status Put(WriteBatch* batch, uint32_t column_family_id,
                    const SliceParts& key, const SliceParts& value);

  static Status TimedPut(WriteBatch* batch, uint32_t column_family_id,
                         const Slice& key, const Slice& value,
                         uint64_t unix_write_time);

  static Status PutEntity(WriteBatch* batch, uint32_t column_family_id,
                          const Slice& key, const WideColumns& columns);

  // Appends an already-serialized wide-column entity. This is used by
  // WriteBatch handlers that need to preserve the original entity bytes
  // without a deserialize/re-serialize round-trip.
  static Status PutEntitySerialized(WriteBatch* batch,
                                    uint32_t column_family_id, const Slice& key,
                                    const Slice& entity);

  static Status Delete(WriteBatch* batch, uint32_t column_family_id,
                       const SliceParts& key);

  static Status Delete(WriteBatch* batch, uint32_t column_family_id,
                       const Slice& key);

  static Status SingleDelete(WriteBatch* batch, uint32_t column_family_id,
                             const SliceParts& key);

  static Status SingleDelete(WriteBatch* batch, uint32_t column_family_id,
                             const Slice& key);

  static Status DeleteRange(WriteBatch* b, uint32_t column_family_id,
                            const Slice& begin_key, const Slice& end_key);

  static Status DeleteRange(WriteBatch* b, uint32_t column_family_id,
                            const SliceParts& begin_key,
                            const SliceParts& end_key);

  static Status Merge(WriteBatch* batch, uint32_t column_family_id,
                      const Slice& key, const Slice& value);

  static Status Merge(WriteBatch* batch, uint32_t column_family_id,
                      const SliceParts& key, const SliceParts& value);

  static Status PutBlobIndex(WriteBatch* batch, uint32_t column_family_id,
                             const Slice& key, const Slice& value);

  static ValueType GetBeginPrepareType(bool write_after_commit,
                                       bool unprepared_batch);

  static Status InsertBeginPrepare(WriteBatch* batch,
                                   const bool write_after_commit = true,
                                   bool unprepared_batch = false);

  static Status InsertEndPrepare(WriteBatch* batch, const Slice& xid);

  static Status MarkEndPrepare(WriteBatch* batch, const Slice& xid,
                               const bool write_after_commit = true,
                               const bool unprepared_batch = false);

  static Status MarkRollback(WriteBatch* batch, const Slice& xid);

  static Status MarkCommit(WriteBatch* batch, const Slice& xid);

  static Status MarkCommitWithTimestamp(WriteBatch* batch, const Slice& xid,
                                        const Slice& commit_ts);

  static Status InsertNoop(WriteBatch* batch);

  // Return the number of entries in the batch.
  static uint32_t Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, uint32_t n);

  // Return the sequence number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // Returns the offset of the first entry in the batch.
  // This offset is only valid if the batch is not empty.
  static size_t GetFirstOffset(WriteBatch* batch);

  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  static Status SetContents(WriteBatch* batch, const Slice& contents);

  static Status CheckSlicePartsLength(const SliceParts& key,
                                      const SliceParts& value);

  // Inserts batches[i] into memtable, for i in 0..num_batches-1 inclusive.
  //
  // If ignore_missing_column_families == true. WriteBatch
  // referencing non-existing column family will be ignored.
  // If ignore_missing_column_families == false, processing of the
  // batches will be stopped if a reference is found to a non-existing
  // column family and InvalidArgument() will be returned.  The writes
  // in batches may be only partially applied at that point.
  //
  // If log_number is non-zero, the memtable will be updated only if
  // memtables->GetLogNumber() >= log_number.
  //
  // If flush_scheduler is non-null, it will be invoked if the memtable
  // should be flushed.
  //
  // This overload is for non-concurrent insertion only.
  static Status InsertInto(
      WriteThread::WriteGroup& write_group, SequenceNumber sequence,
      ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
      TrimHistoryScheduler* trim_history_scheduler,
      bool ignore_missing_column_families = false, uint64_t log_number = 0,
      DB* db = nullptr, bool seq_per_batch = false, bool batch_per_txn = true);

  // Convenience form of InsertInto when you have only one batch
  // next_seq returns the seq after last sequence number used in MemTable insert
  //
  // `selected_cf_ids`, when not null, receives the id of every column family
  // this batch selected for writing, which matches neither the ids the batch
  // names nor what reached a memtable. Replaying a 2PC commit marker applies
  // the prepared batch's column families while naming none; a column family
  // already past `log_number` is skipped and never added; and an id is added
  // before the write, so a later per-write rejection can leave one recorded
  // for a column family nothing was written to. It accumulates as the batch
  // is applied, so it is meaningful when this returns an error partway.
  //
  // Under concurrent use, the caller is responsible for making sure that the
  // memtables object and `selected_cf_ids` are thread-local.
  static Status InsertInto(
      const WriteBatch* batch, ColumnFamilyMemTables* memtables,
      FlushScheduler* flush_scheduler,
      TrimHistoryScheduler* trim_history_scheduler,
      bool ignore_missing_column_families = false, uint64_t log_number = 0,
      DB* db = nullptr, bool concurrent_memtable_writes = false,
      SequenceNumber* next_seq = nullptr, bool* has_valid_writes = nullptr,
      bool seq_per_batch = false, bool batch_per_txn = true,
      std::unordered_set<uint32_t>* selected_cf_ids = nullptr);

  static Status InsertInto(WriteThread::Writer* writer, SequenceNumber sequence,
                           ColumnFamilyMemTables* memtables,
                           FlushScheduler* flush_scheduler,
                           TrimHistoryScheduler* trim_history_scheduler,
                           bool ignore_missing_column_families = false,
                           uint64_t log_number = 0, DB* db = nullptr,
                           bool concurrent_memtable_writes = false,
                           bool seq_per_batch = false, size_t batch_cnt = 0,
                           bool batch_per_txn = true,
                           bool hint_per_batch = false);

  // Appends src write batch to dst write batch and updates count in dst
  // write batch. Returns OK if the append is successful. Checks number of
  // checksum against count in dst and src write batches, and returns Corruption
  // if the count is inconsistent.
  static Status Append(WriteBatch* dst, const WriteBatch* src,
                       const bool WAL_only = false);

  // Returns the byte size of appending a WriteBatch with ByteSize
  // leftByteSize and a WriteBatch with ByteSize rightByteSize
  static size_t AppendedByteSize(size_t leftByteSize, size_t rightByteSize);

  // Iterate over [begin, end) range of a write batch
  static Status Iterate(const WriteBatch* wb, WriteBatch::Handler* handler,
                        size_t begin, size_t end);

  // This write batch includes the latest state that should be persisted. Such
  // state meant to be used only during recovery.
  static void SetAsLatestPersistentState(WriteBatch* b);
  static bool IsLatestPersistentState(const WriteBatch* b);

  static void SetDefaultColumnFamilyTimestampSize(WriteBatch* wb,
                                                  size_t default_cf_ts_sz);

  static std::tuple<Status, uint32_t, size_t> GetColumnFamilyIdAndTimestampSize(
      WriteBatch* b, ColumnFamilyHandle* column_family);

  static bool TimestampsUpdateNeeded(const WriteBatch& wb) {
    return wb.needs_in_place_update_ts_;
  }

  static bool HasKeyWithTimestamp(const WriteBatch& wb) {
    return wb.has_key_with_ts_;
  }

  // Update per-key value protection information on this write batch.
  // If checksum is provided, the batch content is verfied against the checksum.
  static Status UpdateProtectionInfo(WriteBatch* wb, size_t bytes_per_key,
                                     uint64_t* checksum = nullptr);
};

// LocalSavePoint is similar to a scope guard
class LocalSavePoint {
 public:
  explicit LocalSavePoint(WriteBatch* batch)
      : batch_(batch),
        savepoint_(batch->GetDataSize(), batch->Count(),
                   batch->content_flags_.load(std::memory_order_relaxed))
#ifndef NDEBUG
        ,
        committed_(false)
#endif
  {
  }

  // No copying allowed
  LocalSavePoint(const LocalSavePoint&) = delete;
  LocalSavePoint& operator=(const LocalSavePoint&) = delete;
  LocalSavePoint(LocalSavePoint&&) = delete;
  LocalSavePoint& operator=(LocalSavePoint&&) = delete;

#ifndef NDEBUG
  ~LocalSavePoint() { assert(committed_); }
#endif
  Status commit() {
#ifndef NDEBUG
    committed_ = true;
#endif
    if (batch_->max_bytes_ && batch_->rep_.size() > batch_->max_bytes_) {
      batch_->rep_.resize(savepoint_.size);
      WriteBatchInternal::SetCount(batch_, savepoint_.count);
      if (batch_->prot_info_ != nullptr) {
        batch_->prot_info_->entries_.resize(savepoint_.count);
      }
      batch_->content_flags_.store(savepoint_.content_flags,
                                   std::memory_order_relaxed);
      return Status::MemoryLimit();
    }
    return Status::OK();
  }

 private:
  WriteBatch* batch_;
  SavePoint savepoint_;
#ifndef NDEBUG
  bool committed_;
#endif
};

template <typename TimestampSizeFuncType>
class TimestampUpdater : public WriteBatch::Handler {
 public:
  explicit TimestampUpdater(WriteBatch::ProtectionInfo* prot_info,
                            TimestampSizeFuncType&& ts_sz_func, const Slice& ts)
      : prot_info_(prot_info),
        ts_sz_func_(std::move(ts_sz_func)),
        timestamp_(ts) {
    assert(!timestamp_.empty());
  }

  ~TimestampUpdater() override {}

  Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
    return UpdateTimestamp(cf, key);
  }

  Status DeleteCF(uint32_t cf, const Slice& key) override {
    return UpdateTimestamp(cf, key);
  }

  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    return UpdateTimestamp(cf, key);
  }

  Status DeleteRangeCF(uint32_t cf, const Slice& begin_key,
                       const Slice& end_key) override {
    Status s = UpdateTimestamp(cf, begin_key, true /* is_key */);
    if (s.ok()) {
      s = UpdateTimestamp(cf, end_key, false /* is_key */);
    }
    return s;
  }

  Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
    return UpdateTimestamp(cf, key);
  }

  Status PutBlobIndexCF(uint32_t cf, const Slice& key, const Slice&) override {
    return UpdateTimestamp(cf, key);
  }

  Status MarkBeginPrepare(bool) override { return Status::OK(); }

  Status MarkEndPrepare(const Slice&) override { return Status::OK(); }

  Status MarkCommit(const Slice&) override { return Status::OK(); }

  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
    return Status::OK();
  }

  Status MarkRollback(const Slice&) override { return Status::OK(); }

  Status MarkNoop(bool /*empty_batch*/) override { return Status::OK(); }

 private:
  // @param is_key specifies whether the update is for key or value.
  Status UpdateTimestamp(uint32_t cf, const Slice& buf, bool is_key = true) {
    Status s = UpdateTimestampImpl(cf, buf, idx_, is_key);
    ++idx_;
    return s;
  }

  Status UpdateTimestampImpl(uint32_t cf, const Slice& buf, size_t /*idx*/,
                             bool is_key) {
    if (timestamp_.empty()) {
      return Status::InvalidArgument("Timestamp is empty");
    }
    size_t cf_ts_sz = ts_sz_func_(cf);
    if (0 == cf_ts_sz) {
      // Skip this column family.
      return Status::OK();
    } else if (std::numeric_limits<size_t>::max() == cf_ts_sz) {
      // Column family timestamp info not found.
      return Status::NotFound();
    } else if (cf_ts_sz != timestamp_.size()) {
      return Status::InvalidArgument("timestamp size mismatch");
    }
    UpdateProtectionInformationIfNeeded(buf, timestamp_, is_key);

    char* ptr = const_cast<char*>(buf.data() + buf.size() - cf_ts_sz);
    assert(ptr);
    memcpy(ptr, timestamp_.data(), timestamp_.size());
    return Status::OK();
  }

  void UpdateProtectionInformationIfNeeded(const Slice& buf, const Slice& ts,
                                           bool is_key) {
    if (prot_info_ != nullptr) {
      const size_t ts_sz = ts.size();
      SliceParts old(&buf, 1);
      Slice old_no_ts(buf.data(), buf.size() - ts_sz);
      std::array<Slice, 2> new_key_cmpts{{old_no_ts, ts}};
      SliceParts new_parts(new_key_cmpts.data(), 2);
      if (is_key) {
        prot_info_->entries_[idx_].UpdateK(old, new_parts);
      } else {
        prot_info_->entries_[idx_].UpdateV(old, new_parts);
      }
    }
  }

  // No copy or move.
  TimestampUpdater(const TimestampUpdater&) = delete;
  TimestampUpdater(TimestampUpdater&&) = delete;
  TimestampUpdater& operator=(const TimestampUpdater&) = delete;
  TimestampUpdater& operator=(TimestampUpdater&&) = delete;

  WriteBatch::ProtectionInfo* const prot_info_ = nullptr;
  const TimestampSizeFuncType ts_sz_func_{};
  const Slice timestamp_;
  size_t idx_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
