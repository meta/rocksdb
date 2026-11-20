// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/autovector.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace ROCKSDB_NAMESPACE {

class WritePreparedTxnDB;

// This impl could write to DB also uncommitted data and then later tell apart
// committed data from uncommitted data. Uncommitted data could be after the
// Prepare phase in 2PC (WritePreparedTxn) or before that
// (WriteUnpreparedTxnImpl).
//
// == Concrete example: WritePrepared 2PC transaction ==
//
// User code:
//
//   Transaction* txn = db->BeginTransaction(write_opts, txn_opts);
//   txn->SetName("txn1");
//   txn->Put("key1", "value1");   // buffered in WriteBatch, nothing written
//   yet txn->Prepare();               // Phase 1 txn->Commit(); // Phase 2
//
// -- Phase 1: Prepare (PrepareInternal) --
//
// The Prepare call (write_prepared_txn.cc PrepareInternal) calls:
//
//   db_impl_->WriteImpl(write_options, GetWriteBatch(),
//                       ..., !DISABLE_MEMTABLE, ...);
//
// !DISABLE_MEMTABLE is false -- memtable is enabled. This is the defining
// characteristic of "WritePrepared": the actual data (Put("key1", "value1"))
// is written to the memtable at Prepare time.
//
// Because disable_memtable == false, the routing check at
// db_impl_write.cc:502 is not taken. The write goes through the main write
// queue (write_thread_), which handles both WAL and memtable:
//
//   Destination | What gets written                          | Sequence
//   ------------|--------------------------------------------|-----------
//   WAL         | Put(key1, value1) + EndPrepare(txn1)       | prepare_seq
//   Memtable    | Put(key1, value1)                          | prepare_seq
//
// The data is now durable (WAL) and in the memtable, but not yet visible
// to readers. Readers use GetLastPublishedSequence() which consults a
// commit map -- since prepare_seq is in the PreparedHeap but not yet in the
// CommitCache, readers know this data is uncommitted and skip it.
//
// -- Phase 2: Commit (CommitInternal) --
//
// The Commit call (write_prepared_txn.cc CommitInternal) calls:
//
//   db_impl_->WriteImpl(write_options_, working_batch,
//                       ..., disable_memtable, ...);
//
// In the typical case (do_one_write == true, i.e., the commit-time batch
// is empty or has no data), disable_memtable is true. Now the routing
// check at db_impl_write.cc:502 is taken:
//
//   if (two_write_queues_ && disable_memtable) {
//       return WriteImplWALOnly(&nonmem_write_thread_, ...);
//   }
//
// The commit goes through the second write queue (nonmem_write_thread_),
// WAL only:
//
//   Destination | What gets written   | Sequence
//   ------------|---------------------|-----------
//   WAL         | Commit(txn1) marker | commit_seq
//   Memtable    | Nothing             | --
//
// The PreReleaseCallback (WritePreparedCommitEntryPreReleaseCallback)
// updates the CommitCache to record that prepare_seq was committed at
// commit_seq. After this, readers consulting the commit map will see that
// the data at prepare_seq is committed and therefore visible.
//
// -- Why two queues help --
//
// The Commit phase doesn't touch the memtable -- it only writes a small
// marker to WAL and updates an in-memory commit map. By routing this
// through a separate queue, Commit writes don't have to wait behind other
// transactions' Prepare writes (which do the expensive memtable insertion
// on the main queue). This is the optimization mentioned in the options
// comment about MySQL 2PC where commits are serial.
//
// -- Sequence number flow --
//
//                            last_sequence_ | last_allocated_seq |
//                            last_published_seq
//                            ---------------|--------------------|-------------------
//   Before Prepare:                  9      |         9          |        9
//
//   Prepare (main queue):
//     FetchAdd alloc seq             9      |        10          |        9
//     Write WAL + memtable
//     SetLastSequence               10      |        10          |        9
//     (published_seq not advanced yet -- data is uncommitted)
//
//   Commit (2nd queue):
//     FetchAdd alloc seq            10      |        11          |        9
//     Write WAL only
//     Update CommitCache
//     SetLastPublishedSeq           10      |        11          |       11
//
class WritePreparedTxn : public PessimisticTransaction {
 public:
  WritePreparedTxn(WritePreparedTxnDB* db, const WriteOptions& write_options,
                   const TransactionOptions& txn_options);
  // No copying allowed
  WritePreparedTxn(const WritePreparedTxn&) = delete;
  void operator=(const WritePreparedTxn&) = delete;
  WritePreparedTxn(WritePreparedTxn&&) = delete;
  void operator=(WritePreparedTxn&&) = delete;

  ~WritePreparedTxn() override {}

  // To make WAL commit markers visible, the snapshot will be based on the last
  // seq in the WAL that is also published, LastPublishedSequence, as opposed to
  // the last seq in the memtable.
  using Transaction::Get;
  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             PinnableSlice* value) override;

  using Transaction::MultiGet;
  void MultiGet(const ReadOptions& _read_options,
                ColumnFamilyHandle* column_family, const size_t num_keys,
                const Slice* keys, PinnableSlice* values, Status* statuses,
                const bool sorted_input = false) override;

  // Note: The behavior is undefined in presence of interleaved writes to the
  // same transaction.
  // To make WAL commit markers visible, the snapshot will be
  // based on the last seq in the WAL that is also published,
  // LastPublishedSequence, as opposed to the last seq in the memtable.
  using Transaction::GetIterator;
  Iterator* GetIterator(const ReadOptions& options) override;
  Iterator* GetIterator(const ReadOptions& options,
                        ColumnFamilyHandle* column_family) override;

  std::unique_ptr<Iterator> GetCoalescingIterator(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  std::unique_ptr<AttributeGroupIterator> GetAttributeGroupIterator(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  void SetSnapshot() override;

 protected:
  void Initialize(const TransactionOptions& txn_options) override;
  // Override the protected SetId to make it visible to the friend class
  // WritePreparedTxnDB
  inline void SetId(uint64_t id) override { Transaction::SetId(id); }

 private:
  friend class WritePreparedTransactionTest_BasicRecoveryTest_Test;
  friend class WritePreparedTxnDB;
  friend class WriteUnpreparedTxnDB;
  friend class WriteUnpreparedTxn;

  using Transaction::GetImpl;
  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, PinnableSlice* value) override;

  Status GetEntityImpl(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableWideColumns* columns) override;

  void MultiGetEntityImpl(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, size_t num_keys,
                          const Slice* keys, PinnableWideColumns* results,
                          Status* statuses, bool sorted_input) override;

  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;

  Status CommitBatchInternal(WriteBatch* batch, size_t batch_cnt) override;

  // Since the data is already written to memtables at the Prepare phase, the
  // commit entails writing only a commit marker in the WAL. The sequence number
  // of the commit marker is then the commit timestamp of the transaction. To
  // make WAL commit markers visible, the snapshot will be based on the last seq
  // in the WAL that is also published, LastPublishedSequence, as opposed to the
  // last seq in the memtable.
  Status CommitInternal() override;

  Status RollbackInternal() override;

  Status ValidateSnapshot(ColumnFamilyHandle* column_family, const Slice& key,
                          SequenceNumber* tracked_at_seq) override;

  Status RebuildFromWriteBatch(WriteBatch* src_batch) override;

  WritePreparedTxnDB* wpt_db_;
  // Number of sub-batches in prepare
  size_t prepare_batch_cnt_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
