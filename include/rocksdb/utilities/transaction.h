// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <limits>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Iterator;
class TransactionDB;
class WriteBatchWithIndex;

using TransactionName = std::string;

using TransactionID = uint64_t;

using TxnTimestamp = uint64_t;

constexpr TxnTimestamp kMaxTxnTimestamp =
    std::numeric_limits<TxnTimestamp>::max();

/*
  class Endpoint allows to define prefix ranges.

  Prefix ranges are introduced below.

  == Basic Ranges ==
  Let's start from basic ranges. Key Comparator defines ordering of rowkeys.
  Then, one can specify finite closed ranges by just providing rowkeys of their
  endpoints:

    lower_endpoint <= X <= upper_endpoint

  However our goal is to provide a richer set of endpoints. Read on.

  == Lexicographic ordering ==
  A lexicographic (or dictionary) ordering satisfies these criteria: If there
  are two keys in form
    key_a = {prefix_a, suffix_a}
    key_b = {prefix_b, suffix_b}
  and
    prefix_a < prefix_b
  then
    key_a < key_b.

  == Prefix ranges ==
  With lexicographic ordering, one may want to define ranges in form

     "prefix is $PREFIX"

  which translates to a range in form

    {$PREFIX, -infinity} < X < {$PREFIX, +infinity}

  where -infinity will compare less than any possible suffix, and +infinity
  will compare as greater than any possible suffix.

  class Endpoint allows to define these kind of rangtes.

  == Notes ==
  BytewiseComparator and ReverseBytewiseComparator produce lexicographic
  ordering.

  The row comparison function is able to compare key prefixes. If the data
  domain includes keys A and B, then the comparison function is able to compare
  equal-length prefixes:

    min_len= min(byte_length(A), byte_length(B));
    cmp(Slice(A, min_len), Slice(B, min_len));  // this call is valid

  == Other options ==
  As far as MyRocks is concerned, the alternative to prefix ranges would be to
  support both open (non-inclusive) and closed (inclusive) range endpoints.
*/

class Endpoint {
 public:
  Slice slice;

  /*
    true  : the key has a "+infinity" suffix. A suffix that would compare as
            greater than any other suffix
    false : otherwise
  */
  bool inf_suffix;

  explicit Endpoint(const Slice& slice_arg, bool inf_suffix_arg = false)
      : slice(slice_arg), inf_suffix(inf_suffix_arg) {}

  explicit Endpoint(const char* s, bool inf_suffix_arg = false)
      : slice(s), inf_suffix(inf_suffix_arg) {}

  Endpoint(const char* s, size_t size, bool inf_suffix_arg = false)
      : slice(s, size), inf_suffix(inf_suffix_arg) {}

  Endpoint() : inf_suffix(false) {}
};

// Provides notification to the caller of SetSnapshotOnNextOperation when
// the actual snapshot gets created
class TransactionNotifier {
 public:
  virtual ~TransactionNotifier() = default;

  // Implement this method to receive notification when a snapshot is
  // requested via SetSnapshotOnNextOperation.
  // Do not take exclusive ownership of `newSnapshot` because it is shared with
  // the underlying transaction.
  virtual void SnapshotCreated(const Snapshot* newSnapshot) = 0;
};

// Provides BEGIN/COMMIT/ROLLBACK transactions.
//
// To use transactions, you must first create either an OptimisticTransactionDB
// or a TransactionDB.  See examples/[optimistic_]transaction_example.cc for
// more information.
//
// To create a transaction, use [Optimistic]TransactionDB::BeginTransaction().
//
// It is up to the caller to synchronize access to this object.
//
// See examples/transaction_example.cc for some simple examples.
//
// TODO(agiardullo): Not yet implemented
//  -PerfContext statistics
//  -Support for using Transactions with DBWithTTL
class Transaction {
 public:
  // No copying allowed
  Transaction(const Transaction&) = delete;
  void operator=(const Transaction&) = delete;
  Transaction(Transaction&&) = delete;
  void operator=(Transaction&&) = delete;

  virtual ~Transaction() {}

  // If a transaction has a snapshot set, the transaction will ensure that
  // any keys successfully written(or fetched via GetForUpdate()) have not
  // been modified outside of this transaction since the time the snapshot was
  // set.
  // If a snapshot has not been set, the transaction guarantees that keys have
  // not been modified since the time each key was first written (or fetched via
  // GetForUpdate()).
  //
  // Using SetSnapshot() will provide stricter isolation guarantees at the
  // expense of potentially more transaction failures due to conflicts with
  // other writes.
  //
  // Calling SetSnapshot() has no effect on keys written before this function
  // has been called.
  //
  // SetSnapshot() may be called multiple times if you would like to change
  // the snapshot used for different operations in this transaction.
  //
  // Calling SetSnapshot will not affect the version of Data returned by Get()
  // methods.  See Transaction::Get() for more details.
  virtual void SetSnapshot() = 0;

  // Similar to SetSnapshot(), but will not change the current snapshot
  // until Put/PutEntity/Merge/Delete/GetForUpdate/MultigetForUpdate is called.
  // By calling this function, the transaction will essentially call
  // SetSnapshot() for you right before performing the next write/GetForUpdate.
  //
  // Calling SetSnapshotOnNextOperation() will not affect what snapshot is
  // returned by GetSnapshot() until the next write/GetForUpdate is executed.
  //
  // When the snapshot is created the notifier's SnapshotCreated method will
  // be called so that the caller can get access to the snapshot.
  //
  // This is an optimization to reduce the likelihood of conflicts that
  // could occur in between the time SetSnapshot() is called and the first
  // write/GetForUpdate operation.  Eg, this prevents the following
  // race-condition:
  //
  //   txn1->SetSnapshot();
  //                             txn2->Put("A", ...);
  //                             txn2->Commit();
  //   txn1->GetForUpdate(opts, "A", ...);  // FAIL!
  //
  // WriteCommittedTxn only: a new snapshot will be taken upon next operation,
  // and next operation can be a Commit.
  // TODO(yanqin) remove the "write-committed only" limitation.
  virtual void SetSnapshotOnNextOperation(
      std::shared_ptr<TransactionNotifier> notifier = nullptr) = 0;

  // Returns the Snapshot created by the last call to SetSnapshot().
  //
  // REQUIRED: The returned Snapshot is only valid up until the next time
  // SetSnapshot()/SetSnapshotOnNextSavePoint() is called, ClearSnapshot()
  // is called, or the Transaction is deleted.
  virtual const Snapshot* GetSnapshot() const = 0;

  // Returns the Snapshot created by the last call to SetSnapshot().
  // The returned snapshot can outlive the transaction.
  virtual std::shared_ptr<const Snapshot> GetTimestampedSnapshot() const = 0;

  // Clears the current snapshot (i.e. no snapshot will be 'set')
  //
  // This removes any snapshot that currently exists or is set to be created
  // on the next update operation (SetSnapshotOnNextOperation).
  //
  // Calling ClearSnapshot() has no effect on keys written before this function
  // has been called.
  //
  // If a reference to a snapshot was retrieved via GetSnapshot(), it will no
  // longer be valid and should be discarded after a call to ClearSnapshot().
  virtual void ClearSnapshot() = 0;

  // Prepare the current transaction for 2PC
  virtual Status Prepare() = 0;

  // Write all batched keys to the db atomically.
  //
  // Returns OK on success.
  //
  // May return any error status that could be returned by DB:Write().
  //
  // If this transaction was created by an OptimisticTransactionDB(),
  // Status::Busy() may be returned if the transaction could not guarantee
  // that there are no write conflicts.  Status::TryAgain() may be returned
  // if the memtable history size is not large enough
  // (see max_write_buffer_size_to_maintain). In either case, a Rollback()
  // or new transaction is required to expect a different result.
  //
  // If this transaction was created by a TransactionDB(), Status::Expired()
  // may be returned if this transaction has lived for longer than
  // TransactionOptions.expiration. Status::TxnNotPrepared() may be returned if
  // TransactionOptions.skip_prepare is false and Prepare is not called on this
  // transaction before Commit.
  //
  // For TransactionDB transactions using 2PC, if Commit() returns non-OK after
  // Prepare() succeeds, the transaction may still need to be resolved. Unless
  // the application resolves it another way, call Rollback() before destroying
  // the transaction or closing the DB.
  virtual Status Commit() = 0;

  // In addition to Commit(), also creates a snapshot of the db after all
  // writes by this txn are visible to other readers.
  // Caller is responsible for ensuring that
  // snapshot1.seq < snapshot2.seq iff. snapshot1.ts < snapshot2.ts
  // in which snapshot1 and snapshot2 are created by this API.
  //
  // Currently only supported by WriteCommittedTxn. Calling this method on
  // other types of transactions will return non-ok Status resulting from
  // Commit() or a `NotSupported` error.
  // This method returns OK if and only if the transaction successfully
  // commits. It is possible that transaction commits successfully but fails to
  // create a timestamped snapshot. Therefore, the caller should check that the
  // snapshot is created.
  // notifier will be notified upon next snapshot creation. Nullable.
  // ret non-null output argument storing a shared_ptr to the newly created
  // snapshot.
  Status CommitAndTryCreateSnapshot(
      std::shared_ptr<TransactionNotifier> notifier =
          std::shared_ptr<TransactionNotifier>(),
      TxnTimestamp ts = kMaxTxnTimestamp,
      std::shared_ptr<const Snapshot>* snapshot = nullptr);

  // Discard all batched writes in this transaction.
  //
  // Transactions should normally be completed with Commit() or Rollback()
  // before destruction and before closing the DB. Destroying a Transaction
  // object releases its in-memory resources, but it is not a substitute for
  // resolving a prepared transaction.
  //
  // Rollback() can return write errors when it needs to write rollback state,
  // such as for prepared transactions. If it returns a retryable I/O error,
  // recover the DB (for example by calling DB::Resume() when applicable) and
  // retry Rollback() before closing the DB.
  virtual Status Rollback() = 0;

  // Records the state of the transaction for future calls to
  // RollbackToSavePoint().  May be called multiple times to set multiple save
  // points.
  virtual void SetSavePoint() = 0;

  // Undo all operations in this transaction (Put, PutEntity, Merge, Delete,
  // PutLogData) since the most recent call to SetSavePoint() and removes the
  // most recent SetSavePoint(). If there is no previous call to SetSavePoint(),
  // returns Status::NotFound()
  virtual Status RollbackToSavePoint() = 0;

  // Pop the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  virtual Status PopSavePoint() = 0;

  // This function is similar to DB::Get() except it will also read pending
  // changes in this transaction.  Currently, this function will return
  // Status::MergeInProgress if the most recent write to the queried key in
  // this batch is a Merge.
  //
  // If read_options.snapshot is not set, the current version of the key will
  // be read.  Calling SetSnapshot() does not affect the version of the data
  // returned.
  //
  // Note that setting read_options.snapshot will affect what is read from the
  // DB but will NOT change which keys are read from this transaction (the keys
  // in this transaction do not yet belong to any snapshot and will be fetched
  // regardless).
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) = 0;

  // An overload of the above method that receives a PinnableSlice
  // For backward compatibility a default implementation is provided
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* pinnable_val) {
    assert(pinnable_val != nullptr);
    auto s = Get(options, column_family, key, pinnable_val->GetSelf());
    pinnable_val->PinSelf();
    return s;
  }

  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     PinnableSlice* pinnable_val) {
    assert(pinnable_val != nullptr);
    auto s = Get(options, key, pinnable_val->GetSelf());
    pinnable_val->PinSelf();
    return s;
  }

  virtual Status GetEntity(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableWideColumns* columns) = 0;

  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values) = 0;

  virtual std::vector<Status> MultiGet(const ReadOptions& options,
                                       const std::vector<Slice>& keys,
                                       std::vector<std::string>* values) = 0;

  // Batched version of MultiGet - see DBImpl::MultiGet(). Sub-classes are
  // expected to override this with an implementation that calls
  // DBImpl::MultiGet()
  virtual void MultiGet(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        const size_t num_keys, const Slice* keys,
                        PinnableSlice* values, Status* statuses,
                        const bool /*sorted_input*/ = false) {
    if (options.io_activity != Env::IOActivity::kUnknown &&
        options.io_activity != Env::IOActivity::kMultiGet) {
      Status s = Status::InvalidArgument(
          "Can only call MultiGet with `ReadOptions::io_activity` is "
          "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGet`");

      for (size_t i = 0; i < num_keys; ++i) {
        if (statuses[i].ok()) {
          statuses[i] = s;
        }
      }
      return;
    }

    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = GetImpl(options, column_family, keys[i], &values[i]);
    }
  }

  virtual void MultiGetEntity(const ReadOptions& options,
                              ColumnFamilyHandle* column_family,
                              size_t num_keys, const Slice* keys,
                              PinnableWideColumns* results, Status* statuses,
                              bool sorted_input = false) = 0;

  // Read this key and ensure that this transaction will only
  // be able to be committed if this key is not written outside this
  // transaction after it has first been read (or after the snapshot if a
  // snapshot is set in this transaction and do_validate is true). If
  // do_validate is false, ReadOptions::snapshot is expected to be nullptr so
  // that GetForUpdate returns the latest committed value. The transaction
  // behavior is the same regardless of whether the key exists or not.
  //
  // Note: Currently, this function will return Status::MergeInProgress
  // if the most recent write to the queried key in this batch is a Merge.
  //
  // The values returned by this function are similar to Transaction::Get().
  // If value==nullptr, then this function will not read any data, but will
  // still ensure that this key cannot be written to by outside of this
  // transaction.
  //
  // If this transaction was created by an OptimisticTransaction, GetForUpdate()
  // could cause commit() to fail.  Otherwise, it could return any error
  // that could be returned by DB::Get().
  //
  // If this transaction was created by a TransactionDB, it can return
  // Status::OK() on success,
  // Status::Busy() if there is a write conflict,
  // Status::TimedOut() if a lock could not be acquired,
  // Status::TryAgain() if the memtable history size is not large enough
  //  (See max_write_buffer_size_to_maintain)
  // Status::MergeInProgress() if merge operations cannot be resolved.
  // or other errors if this key could not be read.
  virtual Status GetForUpdate(const ReadOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key, std::string* value,
                              bool exclusive = true,
                              const bool do_validate = true) = 0;

  // An overload of the above method that receives a PinnableSlice
  // For backward compatibility a default implementation is provided
  virtual Status GetForUpdate(const ReadOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key, PinnableSlice* pinnable_val,
                              bool exclusive = true,
                              const bool do_validate = true) {
    if (pinnable_val == nullptr) {
      std::string* null_str = nullptr;
      return GetForUpdate(options, column_family, key, null_str, exclusive,
                          do_validate);
    } else {
      auto s = GetForUpdate(options, column_family, key,
                            pinnable_val->GetSelf(), exclusive, do_validate);
      pinnable_val->PinSelf();
      return s;
    }
  }

  // Get a range lock on [start_endpoint; end_endpoint].
  virtual Status GetRangeLock(ColumnFamilyHandle*, const Endpoint&,
                              const Endpoint&) {
    return Status::NotSupported();
  }

  virtual Status GetForUpdate(const ReadOptions& options, const Slice& key,
                              std::string* value, bool exclusive = true,
                              const bool do_validate = true) = 0;

  // An overload of the above method that receives a PinnableSlice
  // For backward compatibility a default implementation is provided
  virtual Status GetForUpdate(const ReadOptions& options, const Slice& key,
                              PinnableSlice* pinnable_val,
                              bool exclusive = true,
                              const bool do_validate = true) {
    if (pinnable_val == nullptr) {
      std::string* null_str = nullptr;
      return GetForUpdate(options, key, null_str, exclusive, do_validate);
    } else {
      auto s = GetForUpdate(options, key, pinnable_val->GetSelf(), exclusive,
                            do_validate);
      pinnable_val->PinSelf();
      return s;
    }
  }

  virtual Status GetEntityForUpdate(const ReadOptions& read_options,
                                    ColumnFamilyHandle* column_family,
                                    const Slice& key,
                                    PinnableWideColumns* columns,
                                    bool exclusive = true,
                                    bool do_validate = true) = 0;

  virtual std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values) = 0;

  virtual std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options, const std::vector<Slice>& keys,
      std::vector<std::string>* values) = 0;

  // Returns an iterator that will iterate on all keys in the default
  // column family including both keys in the DB and uncommitted keys in this
  // transaction.
  //
  // Setting read_options.snapshot will affect what is read from the
  // DB but will NOT change which keys are read from this transaction (the keys
  // in this transaction do not yet belong to any snapshot and will be fetched
  // regardless).
  //
  // Caller is responsible for deleting the returned Iterator.
  //
  // The returned iterator is only valid until Commit(), Rollback(), or
  // RollbackToSavePoint() is called.
  virtual Iterator* GetIterator(const ReadOptions& read_options) = 0;

  virtual Iterator* GetIterator(const ReadOptions& read_options,
                                ColumnFamilyHandle* column_family) = 0;

  // Returns a multi-column-family coalescing iterator for the given column
  // families that includes both keys in the DB and uncommitted keys in this
  // transaction.
  //
  // Setting read_options.snapshot will affect what is read from the
  // DB but will NOT change which keys are read from this transaction (the keys
  // in this transaction do not yet belong to any snapshot and will be fetched
  // regardless).
  //
  // The returned iterator is only valid until Commit(), Rollback(), or
  // RollbackToSavePoint() is called.
  virtual std::unique_ptr<Iterator> GetCoalescingIterator(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families) = 0;

  // Returns a multi-column-family attribute group iterator for the given column
  // families that includes both keys in the DB and uncommitted keys in this
  // transaction.
  //
  // Setting read_options.snapshot will affect what is read from the
  // DB but will NOT change which keys are read from this transaction (the keys
  // in this transaction do not yet belong to any snapshot and will be fetched
  // regardless).
  //
  // The returned iterator is only valid until Commit(), Rollback(), or
  // RollbackToSavePoint() is called.
  virtual std::unique_ptr<AttributeGroupIterator> GetAttributeGroupIterator(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families) = 0;

  // Put, PutEntity, Merge, Delete, and SingleDelete behave similarly to the
  // corresponding functions in WriteBatch, but will also do conflict checking
  // on the keys being written.
  //
  // assume_tracked=true expects the key be already tracked. More
  // specifically, it means the key was previously tracked in the same
  // savepoint, with the same exclusive flag, and at a lower sequence number.
  // If valid then it skips ValidateSnapshot.  Returns error otherwise.
  //
  // If this Transaction was created on an OptimisticTransactionDB, these
  // functions should always return Status::OK().
  //
  // If this Transaction was created on a TransactionDB, the status returned
  // can be:
  // Status::OK() on success,
  // Status::Busy() if there is a write conflict,
  // Status::TimedOut() if a lock could not be acquired,
  // Status::TryAgain() if the memtable history size is not large enough
  //  (See max_write_buffer_size_to_maintain)
  // or other errors on unexpected failures.
  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value, const bool assume_tracked = false) = 0;
  virtual Status Put(const Slice& key, const Slice& value) = 0;
  virtual Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                     const SliceParts& value,
                     const bool assume_tracked = false) = 0;
  virtual Status Put(const SliceParts& key, const SliceParts& value) = 0;

  // `columns` is a non-owning view, so the backing storage for each column
  // name and value must remain valid until this method returns.
  virtual Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                           const WideColumns& columns,
                           bool assume_tracked = false) = 0;

  virtual Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value,
                       const bool assume_tracked = false) = 0;
  virtual Status Merge(const Slice& key, const Slice& value) = 0;

  virtual Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                        const bool assume_tracked = false) = 0;
  virtual Status Delete(const Slice& key) = 0;
  virtual Status Delete(ColumnFamilyHandle* column_family,
                        const SliceParts& key,
                        const bool assume_tracked = false) = 0;
  virtual Status Delete(const SliceParts& key) = 0;

  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const Slice& key,
                              const bool assume_tracked = false) = 0;
  virtual Status SingleDelete(const Slice& key) = 0;
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const SliceParts& key,
                              const bool assume_tracked = false) = 0;
  virtual Status SingleDelete(const SliceParts& key) = 0;

  // PutUntracked() will write a Put to the batch of operations to be committed
  // in this transaction.  This write will only happen if this transaction
  // gets committed successfully.  But unlike Transaction::Put(),
  // no conflict checking will be done for this key.
  //
  // If this Transaction was created on a PessimisticTransactionDB, this
  // function will still acquire locks necessary to make sure this write doesn't
  // cause conflicts in other transactions and may return Status::Busy().
  virtual Status PutUntracked(ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& value) = 0;
  virtual Status PutUntracked(const Slice& key, const Slice& value) = 0;
  virtual Status PutUntracked(ColumnFamilyHandle* column_family,
                              const SliceParts& key,
                              const SliceParts& value) = 0;
  virtual Status PutUntracked(const SliceParts& key,
                              const SliceParts& value) = 0;

  virtual Status PutEntityUntracked(ColumnFamilyHandle* column_family,
                                    const Slice& key,
                                    const WideColumns& columns) = 0;

  virtual Status MergeUntracked(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) = 0;
  virtual Status MergeUntracked(const Slice& key, const Slice& value) = 0;

  virtual Status DeleteUntracked(ColumnFamilyHandle* column_family,
                                 const Slice& key) = 0;

  virtual Status DeleteUntracked(const Slice& key) = 0;
  virtual Status DeleteUntracked(ColumnFamilyHandle* column_family,
                                 const SliceParts& key) = 0;
  virtual Status DeleteUntracked(const SliceParts& key) = 0;
  virtual Status SingleDeleteUntracked(ColumnFamilyHandle* column_family,
                                       const Slice& key) = 0;

  virtual Status SingleDeleteUntracked(const Slice& key) = 0;

  // Collpase the merge chain for the given key. This is can be used by the
  // application to trigger an on-demand collpase to a key that has a long
  // merge chain to reduce read amplification, without waiting for compaction
  // to kick in.
  virtual Status CollapseKey(const ReadOptions&, const Slice&,
                             ColumnFamilyHandle* = nullptr) {
    return Status::NotSupported("collpase not supported");
  }

  // Similar to WriteBatch::PutLogData
  virtual void PutLogData(const Slice& blob) = 0;

  // By default, all Put/PutEntity/Merge/Delete operations will be indexed in
  // the transaction so that Get/GetForUpdate/GetIterator can search for these
  // keys.
  //
  // If the caller does not want to fetch the keys about to be written,
  // they may want to avoid indexing as a performance optimization.
  // Calling DisableIndexing() will turn off indexing for all future
  // Put/PutEntity/Merge/Delete operations until EnableIndexing() is called.
  //
  // If a key is written (using Put/PutEntity/Merge/Delete) after
  // DisableIndexing is called and then is fetched via
  // Get/GetForUpdate/GetIterator, the result of the fetch is undefined.
  virtual void DisableIndexing() = 0;
  virtual void EnableIndexing() = 0;

  // Returns the number of distinct Keys being tracked by this transaction.
  // If this transaction was created by a TransactionDB, this is the number of
  // keys that are currently locked by this transaction.
  // If this transaction was created by an OptimisticTransactionDB, this is the
  // number of keys that need to be checked for conflicts at commit time.
  virtual uint64_t GetNumKeys() const = 0;

  // Returns the number of Put/PutEntity/Delete/Merge operations that have been
  // applied to this transaction so far.
  virtual uint64_t GetNumPuts() const = 0;
  virtual uint64_t GetNumPutEntities() const = 0;
  virtual uint64_t GetNumDeletes() const = 0;
  virtual uint64_t GetNumMerges() const = 0;

  // Returns the elapsed time in milliseconds since this Transaction began.
  virtual uint64_t GetElapsedTime() const = 0;

  // Fetch the underlying write batch that contains all pending changes to be
  // committed.
  //
  // Note:  You should not write or delete anything from the batch directly and
  // should only use the functions in the Transaction class to
  // write to this transaction.
  virtual WriteBatchWithIndex* GetWriteBatch() = 0;

  // Change the value of TransactionOptions.lock_timeout (in milliseconds) for
  // this transaction.
  // Has no effect on OptimisticTransactions.
  virtual void SetLockTimeout(int64_t timeout_ms) = 0;

  // Change the value of deadlock_timeout (in milliseconds) for this
  // transaction.
  // Has no effect on OptimisticTransactions.
  virtual void SetDeadlockTimeout(int64_t timeout_ms) = 0;

  // Return the WriteOptions that will be used during Commit()
  virtual WriteOptions* GetWriteOptions() = 0;

  // Reset the WriteOptions that will be used during Commit().
  virtual void SetWriteOptions(const WriteOptions& write_options) = 0;

  // If this key was previously fetched in this transaction using
  // GetForUpdate/MultigetForUpdate(), calling UndoGetForUpdate will tell
  // the transaction that it no longer needs to do any conflict checking
  // for this key.
  //
  // If a key has been fetched N times via GetForUpdate/MultigetForUpdate(),
  // then UndoGetForUpdate will only have an effect if it is also called N
  // times.  If this key has been written to in this transaction,
  // UndoGetForUpdate() will have no effect.
  //
  // If SetSavePoint() has been called after the GetForUpdate(),
  // UndoGetForUpdate() will not have any effect.
  //
  // If this Transaction was created by an OptimisticTransactionDB,
  // calling UndoGetForUpdate can affect whether this key is conflict checked
  // at commit time.
  // If this Transaction was created by a TransactionDB,
  // calling UndoGetForUpdate may release any held locks for this key.
  virtual void UndoGetForUpdate(ColumnFamilyHandle* column_family,
                                const Slice& key) = 0;
  virtual void UndoGetForUpdate(const Slice& key) = 0;

  virtual Status RebuildFromWriteBatch(WriteBatch* src_batch) = 0;

  // Note: data in the commit-time-write-batch bypasses concurrency control,
  // thus should be used with great caution.
  // For write-prepared/write-unprepared transactions,
  // GetCommitTimeWriteBatch() can be used only if the transaction is started
  // with
  // `TransactionOptions::use_only_the_last_commit_time_batch_for_recovery` set
  // to true. Otherwise, it is possible that two uncommitted versions of the
  // same key exist in the database due to the current implementation (see the
  // explanation in WritePreparedTxn::CommitInternal).
  // During bottommost compaction, RocksDB may
  // set the sequence numbers of both to zero once becoming committed, causing
  // output SST file to have two identical internal keys.
  virtual WriteBatch* GetCommitTimeWriteBatch() = 0;

  virtual void SetLogNumber(uint64_t log) { log_number_ = log; }

  virtual uint64_t GetLogNumber() const { return log_number_; }

  virtual Status SetName(const TransactionName& name) = 0;

  virtual TransactionName GetName() const { return name_; }

  virtual TransactionID GetID() const { return 0; }

  virtual bool IsDeadlockDetect() const { return false; }

  virtual std::vector<TransactionID> GetWaitingTxns(
      uint32_t* /*column_family_id*/, std::string* /*key*/) const {
    assert(false);
    return std::vector<TransactionID>();
  }

  enum TransactionState {
    STARTED = 0,
    AWAITING_PREPARE = 1,
    PREPARED = 2,
    AWAITING_COMMIT = 3,
    COMMITTED = 4,
    COMMITED = COMMITTED,  // old misspelled name
    AWAITING_ROLLBACK = 5,
    ROLLEDBACK = 6,
    LOCKS_STOLEN = 7,
  };

  TransactionState GetState() const { return txn_state_; }
  void SetState(TransactionState state) { txn_state_ = state; }

  // NOTE: Experimental feature
  // The globally unique id with which the transaction is identified. This id
  // might or might not be set depending on the implementation. Similarly the
  // implementation decides the point in lifetime of a transaction at which it
  // assigns the id. Although currently it is the case, the id is not guaranteed
  // to remain the same across restarts.
  uint64_t GetId() { return id_; }

  virtual Status SetReadTimestampForValidation(TxnTimestamp /*ts*/) {
    return Status::NotSupported("timestamp not supported");
  }

  virtual Status SetCommitTimestamp(TxnTimestamp /*ts*/) {
    return Status::NotSupported("timestamp not supported");
  }

  virtual TxnTimestamp GetCommitTimestamp() const { return kMaxTxnTimestamp; }

 protected:
  explicit Transaction(const TransactionDB* /*db*/) {}
  Transaction() : log_number_(0), txn_state_(STARTED) {}

  // the log in which the prepared section for this txn resides
  // (for two phase commit)
  uint64_t log_number_;
  TransactionName name_;

  // Execution status of the transaction.
  std::atomic<TransactionState> txn_state_;

  uint64_t id_ = 0;
  virtual void SetId(uint64_t id) {
    assert(id_ == 0);
    id_ = id;
  }

  virtual Status GetImpl(const ReadOptions& /* options */,
                         ColumnFamilyHandle* /* column_family */,
                         const Slice& /* key */, std::string* /* value */) {
    return Status::NotSupported("Not implemented");
  }

  virtual Status GetImpl(const ReadOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableSlice* pinnable_val) {
    assert(pinnable_val != nullptr);
    auto s = GetImpl(options, column_family, key, pinnable_val->GetSelf());
    pinnable_val->PinSelf();
    return s;
  }

  virtual uint64_t GetLastLogNumber() const { return log_number_; }

 private:
  friend class PessimisticTransactionDB;
  friend class WriteUnpreparedTxnDB;
  friend class TransactionTest_TwoPhaseLogRollingTest_Test;
  friend class TransactionTest_TwoPhaseLogRollingTest2_Test;
};

}  // namespace ROCKSDB_NAMESPACE
