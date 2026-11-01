//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/write_prepared_txn_db.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/arena_wrapped_db_iter.h"
#include "db/db_impl/db_impl.h"
#include "logging/logging.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

// This function is for testing only. If it returns true, then all entries in
// the commit cache will be evicted. Unit and/or stress tests (db_stress)
// can implement this function and customize how frequently commit cache
// eviction occurs.
// TODO: remove this function once we can configure commit cache to be very
// small so that eviction occurs very frequently. This requires the commit
// cache entry to be able to encode prepare and commit sequence numbers so that
// the commit sequence number does not have to be within a certain range of
// prepare sequence number.
extern "C" bool rocksdb_write_prepared_TEST_ShouldClearCommitCache(void)
    __attribute__((__weak__));

namespace ROCKSDB_NAMESPACE {

Status WritePreparedTxnDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  auto dbimpl = static_cast_with_check<DBImpl>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtxns = dbimpl->recovered_transactions();
  std::map<SequenceNumber, SequenceNumber> ordered_seq_cnt;
  for (const auto& rtxn : rtxns) {
    // There should only one batch for WritePrepared policy.
    assert(rtxn.second->batches_.size() == 1);
    const auto& seq = rtxn.second->batches_.begin()->first;
    const auto& batch_info = rtxn.second->batches_.begin()->second;
    auto cnt = batch_info.batch_cnt_ ? batch_info.batch_cnt_ : 1;
    ordered_seq_cnt[seq] = cnt;
  }
  // AddPrepared must be called in order
  for (auto seq_cnt : ordered_seq_cnt) {
    auto seq = seq_cnt.first;
    auto cnt = seq_cnt.second;
    for (size_t i = 0; i < cnt; i++) {
      AddPrepared(seq + i);
    }
  }
  SequenceNumber prev_max = max_evicted_seq_;
  SequenceNumber last_seq = db_impl_->GetLatestSequenceNumber();
  AdvanceMaxEvictedSeq(prev_max, last_seq);
  // Create a gap between max and the next snapshot. This simplifies the logic
  // in IsInSnapshot by not having to consider the special case of max ==
  // snapshot after recovery. This is tested in IsInSnapshotEmptyMapTest.
  if (last_seq) {
    db_impl_->versions_->SetLastAllocatedSequence(last_seq + 1);
    db_impl_->versions_->SetLastSequence(last_seq + 1);
    db_impl_->versions_->SetLastPublishedSequence(last_seq + 1);
  }

  db_impl_->SetSnapshotChecker(new WritePreparedSnapshotChecker(this));
  // A callback to commit a single sub-batch
  class CommitSubBatchPreReleaseCallback : public PreReleaseCallback {
   public:
    explicit CommitSubBatchPreReleaseCallback(WritePreparedTxnDB* db)
        : db_(db) {}
    Status Callback(SequenceNumber commit_seq,
                    bool is_mem_disabled __attribute__((__unused__)), uint64_t,
                    size_t /*index*/, size_t /*total*/) override {
      assert(!is_mem_disabled);
      db_->AddCommitted(commit_seq, commit_seq);
      return Status::OK();
    }

   private:
    WritePreparedTxnDB* db_;
  };
  db_impl_->SetRecoverableStatePreReleaseCallback(
      new CommitSubBatchPreReleaseCallback(this));

  auto s = PessimisticTransactionDB::Initialize(compaction_enabled_cf_indices,
                                                handles);
  return s;
}

Status WritePreparedTxnDB::VerifyCFOptions(
    const ColumnFamilyOptions& cf_options) {
  Status s = PessimisticTransactionDB::VerifyCFOptions(cf_options);
  if (!s.ok()) {
    return s;
  }
  if (!cf_options.memtable_factory->CanHandleDuplicatedKey()) {
    return Status::InvalidArgument(
        "memtable_factory->CanHandleDuplicatedKey() cannot be false with "
        "WritePrepared transactions");
  }
  return Status::OK();
}

Transaction* WritePreparedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WritePreparedTxn(this, write_options, txn_options);
  }
}

Status WritePreparedTxnDB::Write(const WriteOptions& opts,
                                 WriteBatch* updates) {
  if (txn_db_options_.skip_concurrency_control) {
    // Skip locking the rows
    const size_t UNKNOWN_BATCH_CNT = 0;
    WritePreparedTxn* NO_TXN = nullptr;
    return WriteInternal(opts, updates, UNKNOWN_BATCH_CNT, NO_TXN);
  } else {
    return PessimisticTransactionDB::WriteWithConcurrencyControl(opts, updates);
  }
}

Status WritePreparedTxnDB::Write(
    const WriteOptions& opts,
    const TransactionDBWriteOptimizations& optimizations, WriteBatch* updates) {
  if (optimizations.skip_concurrency_control) {
    // Skip locking the rows
    const size_t UNKNOWN_BATCH_CNT = 0;
    const size_t ONE_BATCH_CNT = 1;
    const size_t batch_cnt = optimizations.skip_duplicate_key_check
                                 ? ONE_BATCH_CNT
                                 : UNKNOWN_BATCH_CNT;
    WritePreparedTxn* NO_TXN = nullptr;
    return WriteInternal(opts, updates, batch_cnt, NO_TXN);
  } else {
    // TODO(myabandeh): Make use of skip_duplicate_key_check hint
    // Fall back to unoptimized version
    return PessimisticTransactionDB::WriteWithConcurrencyControl(opts, updates);
  }
}

Status WritePreparedTxnDB::WriteInternal(const WriteOptions& write_options_orig,
                                         WriteBatch* batch, size_t batch_cnt,
                                         WritePreparedTxn* txn) {
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "CommitBatchInternal");
  if (batch->Count() == 0) {
    // Otherwise our 1 seq per batch logic will break since there is no seq
    // increased for this batch.
    return Status::OK();
  }

  if (write_options_orig.protection_bytes_per_key > 0) {
    auto s = WriteBatchInternal::UpdateProtectionInfo(
        batch, write_options_orig.protection_bytes_per_key);
    if (!s.ok()) {
      return s;
    }
  }

  if (batch_cnt == 0) {  // not provided, then compute it
    // TODO(myabandeh): add an option to allow user skipping this cost
    SubBatchCounter counter(*GetCFComparatorMap());
    auto s = batch->Iterate(&counter);
    if (!s.ok()) {
      return s;
    }
    batch_cnt = counter.BatchCount();
    WPRecordTick(TXN_DUPLICATE_KEY_OVERHEAD);
    ROCKS_LOG_DETAILS(info_log_, "Duplicate key overhead: %" PRIu64 " batches",
                      static_cast<uint64_t>(batch_cnt));
  }
  assert(batch_cnt);

  bool do_one_write = !db_impl_->immutable_db_options().two_write_queues;
  WriteOptions write_options(write_options_orig);
  // In the absence of Prepare markers, use Noop as a batch separator
  auto s = WriteBatchInternal::InsertNoop(batch);
  assert(s.ok());
  const bool DISABLE_MEMTABLE = true;
  const uint64_t no_log_ref = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  const size_t ZERO_PREPARES = 0;
  const bool kSeparatePrepareCommitBatches = true;
  // Since this is not 2pc, there is no need for AddPrepared but having it in
  // the PreReleaseCallback enables an optimization. Refer to
  // SmallestUnCommittedSeq for more details.
  AddPreparedCallback add_prepared_callback(
      this, db_impl_, batch_cnt,
      db_impl_->immutable_db_options().two_write_queues,
      !kSeparatePrepareCommitBatches);
  WritePreparedCommitEntryPreReleaseCallback update_commit_map(
      this, db_impl_, kMaxSequenceNumber, ZERO_PREPARES, batch_cnt);
  PreReleaseCallback* pre_release_callback;
  if (do_one_write) {
    pre_release_callback = &update_commit_map;
  } else {
    pre_release_callback = &add_prepared_callback;
  }
  s = db_impl_->WriteImpl(write_options, batch, nullptr, nullptr, nullptr,
                          no_log_ref, !DISABLE_MEMTABLE, &seq_used, batch_cnt,
                          pre_release_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  uint64_t prepare_seq = seq_used;
  if (txn != nullptr) {
    txn->SetId(prepare_seq);
  }
  if (!s.ok()) {
    return s;
  }
  if (do_one_write) {
    return s;
  }  // else do the 2nd write for commit
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "CommitBatchInternal 2nd write prepare_seq: %" PRIu64,
                    prepare_seq);
  // Commit the batch by writing an empty batch to the 2nd queue that will
  // release the commit sequence number to readers.
  const size_t ZERO_COMMITS = 0;
  WritePreparedCommitEntryPreReleaseCallback update_commit_map_with_prepare(
      this, db_impl_, prepare_seq, batch_cnt, ZERO_COMMITS);
  WriteBatch empty_batch;
  write_options.disableWAL = true;
  write_options.sync = false;
  const size_t ONE_BATCH = 1;  // Just to inc the seq
  s = db_impl_->WriteImpl(write_options, &empty_batch, nullptr, nullptr,
                          nullptr, no_log_ref, DISABLE_MEMTABLE, &seq_used,
                          ONE_BATCH, &update_commit_map_with_prepare);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Note: RemovePrepared is called from within PreReleaseCallback
  return s;
}

Status WritePreparedTxnDB::GetWithMetadata(const ReadOptions& options,
                                           ColumnFamilyHandle* column_family,
                                           const Slice& key,
                                           PinnableSlice* value,
                                           OutputMetadata* output_metadata) {
  return DB::GetWithMetadata(options, column_family, key, value,
                             output_metadata);
}

Status WritePreparedTxnDB::GetEntityWithMetadata(
    const ReadOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableWideColumns* columns,
    OutputMetadata* output_metadata) {
  return DB::GetEntityWithMetadata(options, column_family, key, columns,
                                   output_metadata);
}

void WritePreparedTxnDB::UpdateCFComparatorMap(
    const std::vector<ColumnFamilyHandle*>& handles) {
  auto cf_map = new std::map<uint32_t, const Comparator*>();
  auto handle_map = new std::map<uint32_t, ColumnFamilyHandle*>();
  for (auto h : handles) {
    auto id = h->GetID();
    const Comparator* comparator = h->GetComparator();
    (*cf_map)[id] = comparator;
    if (id != 0) {
      (*handle_map)[id] = h;
    } else {
      // The pointer to the default cf handle in the handles will be deleted.
      // Use the pointer maintained by the db instead.
      (*handle_map)[id] = DefaultColumnFamily();
    }
  }
  cf_map_.reset(cf_map);
  handle_map_.reset(handle_map);
}

void WritePreparedTxnDB::UpdateCFComparatorMap(ColumnFamilyHandle* h) {
  auto old_cf_map_ptr = cf_map_.get();
  assert(old_cf_map_ptr);
  auto cf_map = new std::map<uint32_t, const Comparator*>(*old_cf_map_ptr);
  auto old_handle_map_ptr = handle_map_.get();
  assert(old_handle_map_ptr);
  auto handle_map =
      new std::map<uint32_t, ColumnFamilyHandle*>(*old_handle_map_ptr);
  auto id = h->GetID();
  const Comparator* comparator = h->GetComparator();
  (*cf_map)[id] = comparator;
  (*handle_map)[id] = h;
  cf_map_.reset(cf_map);
  handle_map_.reset(handle_map);
}

void WritePreparedTxnDB::MultiGetWithMetadata(
    const ReadOptions& options, const size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableSlice* values, Status* statuses,
    MultiGetOutputMetadata* output_metadata, const bool sorted_input) {
  DB::MultiGetWithMetadata(options, num_keys, column_families, keys, values,
                           statuses, output_metadata, sorted_input);
}

void WritePreparedTxnDB::MultiGetEntityWithMetadata(
    const ReadOptions& options, size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableWideColumns* results, Status* statuses,
    MultiGetOutputMetadata* output_metadata, bool sorted_input) {
  DB::MultiGetEntityWithMetadata(options, num_keys, column_families, keys,
                                 results, statuses, output_metadata,
                                 sorted_input);
}

void WritePreparedTxnDB::MultiGetEntityWithMetadata(
    const ReadOptions& options, size_t num_keys, const Slice* keys,
    PinnableAttributeGroups* results, MultiGetOutputMetadata* output_metadata) {
  DB::MultiGetEntityWithMetadata(options, num_keys, keys, results,
                                 output_metadata);
}

// Struct to hold ownership of snapshot and read callback for iterator cleanup.
struct WritePreparedTxnDB::IteratorState {
  IteratorState(WritePreparedTxnDB* txn_db, SequenceNumber sequence,
                std::shared_ptr<ManagedSnapshot> s,
                SequenceNumber min_uncommitted)
      : callback(txn_db, sequence, min_uncommitted, kBackedByDBSnapshot),
        snapshot(s) {}

  WritePreparedTxnReadCallback callback;
  std::shared_ptr<ManagedSnapshot> snapshot;
};

namespace {
static void CleanupWritePreparedTxnDBIterator(void* arg1, void* /*arg2*/) {
  delete static_cast<WritePreparedTxnDB::IteratorState*>(arg1);
}
}  // anonymous namespace

Iterator* WritePreparedTxnDB::NewIterator(const ReadOptions& _read_options,
                                          ColumnFamilyHandle* column_family) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kDBIterator) {
    return NewErrorIterator(Status::InvalidArgument(
        "Can only call NewIterator with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`"));
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kDBIterator;
  }
  constexpr bool expose_blob_index = false;
  constexpr bool allow_refresh = false;
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
  SequenceNumber min_uncommitted = 0;
  if (read_options.snapshot != nullptr) {
    snapshot_seq = read_options.snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl>(read_options.snapshot)
            ->min_uncommitted_;
  } else {
    auto* snapshot = GetSnapshot();
    // We take a snapshot to make sure that the related data in the commit map
    // are not deleted.
    snapshot_seq = snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl>(snapshot)->min_uncommitted_;
    own_snapshot = std::make_shared<ManagedSnapshot>(db_impl_, snapshot);
  }
  assert(snapshot_seq != kMaxSequenceNumber);
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto* cfd = cfh->cfd();
  auto* state =
      new IteratorState(this, snapshot_seq, own_snapshot, min_uncommitted);
  SuperVersion* super_version = cfd->GetReferencedSuperVersion(db_impl_);
  auto* db_iter = db_impl_->NewIteratorImpl(read_options, cfh, super_version,
                                            snapshot_seq, &state->callback,
                                            expose_blob_index, allow_refresh);
  db_iter->RegisterCleanup(CleanupWritePreparedTxnDBIterator, state, nullptr);
  return db_iter;
}

Status WritePreparedTxnDB::NewIterators(
    const ReadOptions& _read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (column_families.empty() &&
      _read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kDBIterator) {
    return Status::InvalidArgument(
        "Can only call NewIterator with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
  }
  return NewIterators(
      std::vector<ReadOptions>(column_families.size(), _read_options),
      column_families, iterators);
}

Status WritePreparedTxnDB::NewIterators(
    const std::vector<ReadOptions>& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (read_options.size() != column_families.size()) {
    return Status::InvalidArgument(
        "read_options and column_families must have the same size");
  }
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }

  if (column_families.empty()) {
    iterators->clear();
    return Status::OK();
  }

  const Snapshot* const snapshot = read_options.front().snapshot;
  const bool tailing = read_options.front().tailing;
  std::vector<ReadOptions> normalized_read_options;
  normalized_read_options.reserve(read_options.size());
  for (const ReadOptions& options : read_options) {
    if (options.io_activity != Env::IOActivity::kUnknown &&
        options.io_activity != Env::IOActivity::kDBIterator) {
      return Status::InvalidArgument(
          "Can only call NewIterators with `ReadOptions::io_activity` is "
          "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
    }
    if (options.read_tier == kPersistedTier) {
      return Status::NotSupported(
          "ReadTier::kPersistedData is not yet supported in iterators.");
    }
    if (options.snapshot != snapshot) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same snapshot");
    }
    if (options.tailing != tailing) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same tailing setting");
    }

    normalized_read_options.emplace_back(options);
    if (normalized_read_options.back().io_activity ==
        Env::IOActivity::kUnknown) {
      normalized_read_options.back().io_activity = Env::IOActivity::kDBIterator;
    }
  }

  if (tailing) {
    return Status::NotSupported(
        "tailing iterator not supported in write-prepared mode");
  }

  iterators->clear();
  iterators->reserve(column_families.size());

  std::shared_ptr<ManagedSnapshot> own_snapshot;
  SequenceNumber snapshot_seq;
  SequenceNumber min_uncommitted;
  if (snapshot != nullptr) {
    snapshot_seq = snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl>(snapshot)->min_uncommitted_;
  } else {
    const Snapshot* managed_snapshot = GetSnapshot();
    snapshot_seq = managed_snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl>(managed_snapshot)
            ->min_uncommitted_;
    own_snapshot =
        std::make_shared<ManagedSnapshot>(db_impl_, managed_snapshot);
  }
  assert(snapshot_seq != kMaxSequenceNumber);

  autovector<SuperVersion*> super_versions;
  for (auto* column_family : column_families) {
    auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    super_versions.emplace_back(
        cfh->cfd()->GetReferencedSuperVersion(db_impl_));
  }
  const auto cleanup_super_versions = [&]() {
    for (auto* super_version : super_versions) {
      db_impl_->CleanupSuperVersion(super_version);
    }
  };
  for (size_t i = 0; i < column_families.size(); ++i) {
    const Status s = db_impl_->FailIfTableFilterWithRangeConversion(
        normalized_read_options[i], super_versions[i]->mutable_cf_options);
    if (!s.ok()) {
      cleanup_super_versions();
      return s;
    }
  }

  for (size_t i = 0; i < column_families.size(); ++i) {
    auto* cfh =
        static_cast_with_check<ColumnFamilyHandleImpl>(column_families[i]);
    auto* state =
        new IteratorState(this, snapshot_seq, own_snapshot, min_uncommitted);
    auto* db_iter = db_impl_->NewIteratorImpl(
        normalized_read_options[i], cfh, super_versions[i], snapshot_seq,
        &state->callback, /* expose_blob_index */ false,
        /* allow_refresh */ false);
    db_iter->RegisterCleanup(CleanupWritePreparedTxnDBIterator, state, nullptr);
    iterators->push_back(db_iter);
  }
  return Status::OK();
}

void WritePreparedTxnDB::Init(const TransactionDBOptions& txn_db_opts) {
  // Advance max_evicted_seq_ no more than 100 times before the cache wraps
  // around.
  INC_STEP_FOR_MAX_EVICTED =
      std::max(COMMIT_CACHE_SIZE / 100, static_cast<size_t>(1));
  snapshot_cache_ = std::unique_ptr<std::atomic<SequenceNumber>[]>(
      new std::atomic<SequenceNumber>[SNAPSHOT_CACHE_SIZE] {});
  commit_cache_ = std::unique_ptr<std::atomic<CommitEntry64b>[]>(
      new std::atomic<CommitEntry64b>[COMMIT_CACHE_SIZE] {});
  dummy_max_snapshot_.number_ = kMaxSequenceNumber;
  rollback_deletion_type_callback_ =
      txn_db_opts.rollback_deletion_type_callback;
}

void WritePreparedTxnDB::CheckPreparedAgainstMax(SequenceNumber new_max,
                                                 bool locked) {
  // When max_evicted_seq_ advances, move older entries from prepared_txns_
  // to delayed_prepared_. This guarantees that if a seq is lower than max,
  // then it is not in prepared_txns_ and save an expensive, synchronized
  // lookup from a shared set. delayed_prepared_ is expected to be empty in
  // normal cases.
  ROCKS_LOG_DETAILS(
      info_log_,
      "CheckPreparedAgainstMax prepared_txns_.empty() %d top: %" PRIu64,
      prepared_txns_.empty(),
      prepared_txns_.empty() ? 0 : prepared_txns_.top());
  const SequenceNumber prepared_top = prepared_txns_.top();
  const bool empty = prepared_top == kMaxSequenceNumber;
  // Preliminary check to avoid the synchronization cost
  if (!empty && prepared_top <= new_max) {
    if (locked) {
      // Needed to avoid double locking in pop().
      prepared_txns_.push_pop_mutex()->Unlock();
    }
    WriteLock wl(&prepared_mutex_);
    // Need to fetch fresh values of ::top after mutex is acquired
    while (!prepared_txns_.empty() && prepared_txns_.top() <= new_max) {
      auto to_be_popped = prepared_txns_.top();
      delayed_prepared_.insert(to_be_popped);
      ROCKS_LOG_WARN(info_log_,
                     "prepared_mutex_ overhead %" PRIu64 " (prep=%" PRIu64
                     " new_max=%" PRIu64 ")",
                     static_cast<uint64_t>(delayed_prepared_.size()),
                     to_be_popped, new_max);
      delayed_prepared_empty_.store(false, std::memory_order_release);
      // Update prepared_txns_ after updating delayed_prepared_empty_ otherwise
      // there will be a point in time that the entry is neither in
      // prepared_txns_ nor in delayed_prepared_, which will not be checked if
      // delayed_prepared_empty_ is false.
      prepared_txns_.pop();
    }
    if (locked) {
      prepared_txns_.push_pop_mutex()->Lock();
    }
  }
}

void WritePreparedTxnDB::AddPrepared(uint64_t seq, bool locked) {
  ROCKS_LOG_DETAILS(info_log_, "Txn %" PRIu64 " Preparing with max %" PRIu64,
                    seq, max_evicted_seq_.load());
  TEST_SYNC_POINT("AddPrepared::begin:pause");
  TEST_SYNC_POINT("AddPrepared::begin:resume");
  if (!locked) {
    prepared_txns_.push_pop_mutex()->Lock();
  }
  prepared_txns_.push_pop_mutex()->AssertHeld();
  prepared_txns_.push(seq);
  auto new_max = future_max_evicted_seq_.load();
  if (UNLIKELY(seq <= new_max)) {
    // This should not happen in normal case
    ROCKS_LOG_ERROR(
        info_log_,
        "Added prepare_seq is not larger than max_evicted_seq_: %" PRIu64
        " <= %" PRIu64,
        seq, new_max);
    CheckPreparedAgainstMax(new_max, true /*locked*/);
  }
  if (!locked) {
    prepared_txns_.push_pop_mutex()->Unlock();
  }
  TEST_SYNC_POINT("AddPrepared::end");
}

void WritePreparedTxnDB::AddCommitted(uint64_t prepare_seq, uint64_t commit_seq,
                                      uint8_t loop_cnt) {
  ROCKS_LOG_DETAILS(info_log_, "Txn %" PRIu64 " Committing with %" PRIu64,
                    prepare_seq, commit_seq);
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:start:pause");
  auto indexed_seq = prepare_seq % COMMIT_CACHE_SIZE;
  CommitEntry64b evicted_64b;
  CommitEntry evicted;
  bool to_be_evicted = GetCommitEntry(indexed_seq, &evicted_64b, &evicted);
  if (LIKELY(to_be_evicted)) {
    assert(evicted.prep_seq != prepare_seq);
    auto prev_max = max_evicted_seq_.load(std::memory_order_acquire);
    ROCKS_LOG_DETAILS(info_log_,
                      "Evicting %" PRIu64 ",%" PRIu64 " with max %" PRIu64,
                      evicted.prep_seq, evicted.commit_seq, prev_max);
    if (prev_max < evicted.commit_seq) {
      auto last = db_impl_->GetLastPublishedSequence();  // could be 0
      SequenceNumber max_evicted_seq;
      if (LIKELY(evicted.commit_seq < last)) {
        assert(last > 0);
        // Inc max in larger steps to avoid frequent updates
        max_evicted_seq =
            std::min(evicted.commit_seq + INC_STEP_FOR_MAX_EVICTED, last - 1);
      } else {
        // legit when a commit entry in a write batch overwrite the previous one
        max_evicted_seq = evicted.commit_seq;
      }
#ifdef OS_LINUX
      if (rocksdb_write_prepared_TEST_ShouldClearCommitCache &&
          rocksdb_write_prepared_TEST_ShouldClearCommitCache()) {
        max_evicted_seq = last;
      }
#endif  // OS_LINUX
      ROCKS_LOG_DETAILS(info_log_,
                        "%lu Evicting %" PRIu64 ",%" PRIu64 " with max %" PRIu64
                        " => %lu",
                        prepare_seq, evicted.prep_seq, evicted.commit_seq,
                        prev_max, max_evicted_seq);
      AdvanceMaxEvictedSeq(prev_max, max_evicted_seq);
    }
    if (UNLIKELY(!delayed_prepared_empty_.load(std::memory_order_acquire))) {
      WriteLock wl(&prepared_mutex_);
      auto dp_iter = delayed_prepared_.find(evicted.prep_seq);
      if (dp_iter != delayed_prepared_.end()) {
        // This is a rare case that txn is committed but prepared_txns_ is not
        // cleaned up yet. Refer to delayed_prepared_commits_ definition for
        // why it should be kept updated.
        delayed_prepared_commits_[evicted.prep_seq] = evicted.commit_seq;
        ROCKS_LOG_DEBUG(info_log_,
                        "delayed_prepared_commits_[%" PRIu64 "]=%" PRIu64,
                        evicted.prep_seq, evicted.commit_seq);
      }
    }
    // After each eviction from commit cache, check if the commit entry should
    // be kept around because it overlaps with a live snapshot.
    CheckAgainstSnapshots(evicted);
  }
  bool succ =
      ExchangeCommitEntry(indexed_seq, evicted_64b, {prepare_seq, commit_seq});
  if (UNLIKELY(!succ)) {
    ROCKS_LOG_ERROR(info_log_,
                    "ExchangeCommitEntry failed on [%" PRIu64 "] %" PRIu64
                    ",%" PRIu64 " retrying...",
                    indexed_seq, prepare_seq, commit_seq);
    // A very rare event, in which the commit entry is updated before we do.
    // Here we apply a very simple solution of retrying.
    if (loop_cnt > 100) {
      throw std::runtime_error("Infinite loop in AddCommitted!");
    }
    AddCommitted(prepare_seq, commit_seq, ++loop_cnt);
    return;
  }
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:end:pause");
}

void WritePreparedTxnDB::RemovePrepared(const uint64_t prepare_seq,
                                        const size_t batch_cnt) {
  TEST_SYNC_POINT_CALLBACK(
      "RemovePrepared:Start",
      const_cast<void*>(static_cast<const void*>(&prepare_seq)));
  TEST_SYNC_POINT("WritePreparedTxnDB::RemovePrepared:pause");
  TEST_SYNC_POINT("WritePreparedTxnDB::RemovePrepared:resume");
  ROCKS_LOG_DETAILS(info_log_,
                    "RemovePrepared %" PRIu64 " cnt: %" ROCKSDB_PRIszt,
                    prepare_seq, batch_cnt);
  WriteLock wl(&prepared_mutex_);
  for (size_t i = 0; i < batch_cnt; i++) {
    prepared_txns_.erase(prepare_seq + i);
    bool was_empty = delayed_prepared_.empty();
    if (!was_empty) {
      delayed_prepared_.erase(prepare_seq + i);
      auto it = delayed_prepared_commits_.find(prepare_seq + i);
      if (it != delayed_prepared_commits_.end()) {
        ROCKS_LOG_DETAILS(info_log_, "delayed_prepared_commits_.erase %" PRIu64,
                          prepare_seq + i);
        delayed_prepared_commits_.erase(it);
      }
      bool is_empty = delayed_prepared_.empty();
      if (was_empty != is_empty) {
        delayed_prepared_empty_.store(is_empty, std::memory_order_release);
      }
    }
  }
}

bool WritePreparedTxnDB::GetCommitEntry(const uint64_t indexed_seq,
                                        CommitEntry64b* entry_64b,
                                        CommitEntry* entry) const {
  *entry_64b = commit_cache_[static_cast<size_t>(indexed_seq)].load(
      std::memory_order_acquire);
  bool valid = entry_64b->Parse(indexed_seq, entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::AddCommitEntry(const uint64_t indexed_seq,
                                        const CommitEntry& new_entry,
                                        CommitEntry* evicted_entry) {
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  CommitEntry64b evicted_entry_64b =
      commit_cache_[static_cast<size_t>(indexed_seq)].exchange(
          new_entry_64b, std::memory_order_acq_rel);
  bool valid = evicted_entry_64b.Parse(indexed_seq, evicted_entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::ExchangeCommitEntry(const uint64_t indexed_seq,
                                             CommitEntry64b& expected_entry_64b,
                                             const CommitEntry& new_entry) {
  auto& atomic_entry = commit_cache_[static_cast<size_t>(indexed_seq)];
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  bool succ = atomic_entry.compare_exchange_strong(
      expected_entry_64b, new_entry_64b, std::memory_order_acq_rel,
      std::memory_order_acquire);
  return succ;
}

void WritePreparedTxnDB::AdvanceMaxEvictedSeq(const SequenceNumber& prev_max,
                                              const SequenceNumber& new_max) {
  ROCKS_LOG_DETAILS(info_log_,
                    "AdvanceMaxEvictedSeq overhead %" PRIu64 " => %" PRIu64,
                    prev_max, new_max);
  // Declare the intention before getting snapshot from the DB. This helps a
  // concurrent GetSnapshot to wait to catch up with future_max_evicted_seq_ if
  // it has not already. Otherwise the new snapshot is when we ask DB for
  // snapshots smaller than future max.
  auto updated_future_max = prev_max;
  while (updated_future_max < new_max &&
         !future_max_evicted_seq_.compare_exchange_weak(
             updated_future_max, new_max, std::memory_order_acq_rel,
             std::memory_order_relaxed)) {
  };

  CheckPreparedAgainstMax(new_max, false /*locked*/);

  // With each change to max_evicted_seq_ fetch the live snapshots behind it.
  // We use max as the version of snapshots to identify how fresh are the
  // snapshot list. This works because the snapshots are between 0 and
  // max, so the larger the max, the more complete they are.
  SequenceNumber new_snapshots_version = new_max;
  std::vector<SequenceNumber> snapshots;
  bool update_snapshots = false;
  if (new_snapshots_version > snapshots_version_) {
    // This is to avoid updating the snapshots_ if it already updated
    // with a more recent version by a concurrent thread
    update_snapshots = true;
    // We only care about snapshots lower then max
    snapshots = GetSnapshotListFromDB(new_max);
  }
  if (update_snapshots) {
    UpdateSnapshots(snapshots, new_snapshots_version);
    if (!snapshots.empty()) {
      WriteLock wl(&old_commit_map_mutex_);
      for (auto snap : snapshots) {
        // This allows IsInSnapshot to tell apart the reads from in valid
        // snapshots from the reads from committed values in valid snapshots.
        old_commit_map_[snap];
      }
      old_commit_map_empty_.store(false, std::memory_order_release);
    }
  }
  auto updated_prev_max = prev_max;
  TEST_SYNC_POINT("AdvanceMaxEvictedSeq::update_max:pause");
  TEST_SYNC_POINT("AdvanceMaxEvictedSeq::update_max:resume");
  while (updated_prev_max < new_max &&
         !max_evicted_seq_.compare_exchange_weak(updated_prev_max, new_max,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
  };
}

const Snapshot* WritePreparedTxnDB::GetSnapshot() {
  const bool kForWWConflictCheck = true;
  return GetSnapshotInternal(!kForWWConflictCheck);
}

SnapshotImpl* WritePreparedTxnDB::GetSnapshotInternal(
    bool for_ww_conflict_check) {
  // Note: for this optimization setting the last sequence number and obtaining
  // the smallest uncommitted seq should be done atomically. However to avoid
  // the mutex overhead, we call SmallestUnCommittedSeq BEFORE taking the
  // snapshot. Since we always updated the list of unprepared seq (via
  // AddPrepared) AFTER the last sequence is updated, this guarantees that the
  // smallest uncommitted seq that we pair with the snapshot is smaller or equal
  // the value that would be obtained otherwise atomically. That is ok since
  // this optimization works as long as min_uncommitted is less than or equal
  // than the smallest uncommitted seq when the snapshot was taken.
  auto min_uncommitted = WritePreparedTxnDB::SmallestUnCommittedSeq();
  SnapshotImpl* snap_impl = db_impl_->GetSnapshotImpl(
      for_ww_conflict_check, DBImpl::SnapshotLockMode::kTryWithoutDBMutex);
  TEST_SYNC_POINT("WritePreparedTxnDB::GetSnapshotInternal:first");
  assert(snap_impl);
  SequenceNumber snap_seq = snap_impl->GetSequenceNumber();
  // Note: Check against future_max_evicted_seq_ (in contrast with
  // max_evicted_seq_) in case there is a concurrent AdvanceMaxEvictedSeq.
  if (UNLIKELY(snap_seq != 0 && snap_seq <= future_max_evicted_seq_)) {
    // There is a very rare case in which the commit entry evicts another commit
    // entry that is not published yet thus advancing max evicted seq beyond the
    // last published seq. This case is not likely in real-world setup so we
    // handle it with a few retries.
    size_t retry = 0;
    SequenceNumber max;
    while ((max = future_max_evicted_seq_.load()) != 0 &&
           snap_impl->GetSequenceNumber() <= max && retry < 100) {
      ROCKS_LOG_WARN(info_log_,
                     "GetSnapshot snap: %" PRIu64 " max: %" PRIu64
                     " retry %" ROCKSDB_PRIszt,
                     snap_impl->GetSequenceNumber(), max, retry);
      ReleaseSnapshot(snap_impl);
      // Wait for last visible seq to catch up with max, and also go beyond it
      // by one.
      AdvanceSeqByOne();
      snap_impl = db_impl_->GetSnapshotImpl(
          for_ww_conflict_check, DBImpl::SnapshotLockMode::kTryWithoutDBMutex);
      assert(snap_impl);
      retry++;
    }
    assert(snap_impl->GetSequenceNumber() > max);
    if (snap_impl->GetSequenceNumber() <= max) {
      throw std::runtime_error(
          "Snapshot seq " + std::to_string(snap_impl->GetSequenceNumber()) +
          " after " + std::to_string(retry) +
          " retries is still less than future_max_evicted_seq_" +
          std::to_string(max));
    }
  }
  EnhanceSnapshot(snap_impl, min_uncommitted);
  ROCKS_LOG_DETAILS(
      db_impl_->immutable_db_options().info_log,
      "GetSnapshot %" PRIu64 " ww:%" PRIi32 " min_uncommitted: %" PRIu64,
      snap_impl->GetSequenceNumber(), for_ww_conflict_check, min_uncommitted);
  TEST_SYNC_POINT("WritePreparedTxnDB::GetSnapshotInternal:end");
  return snap_impl;
}

void WritePreparedTxnDB::AdvanceSeqByOne() {
  // Inserting an empty value will i) let the max evicted entry to be
  // published, i.e., max == last_published, increase the last published to
  // be one beyond max, i.e., max < last_published.
  // TODO: plumb Env::IOActivity, Env::IOPriority
  WriteOptions woptions;
  TransactionOptions txn_options;
  Transaction* txn0 = BeginTransaction(woptions, txn_options, nullptr);
  std::hash<std::thread::id> hasher;
  char name[64];
  snprintf(name, 64, "txn%" ROCKSDB_PRIszt, hasher(std::this_thread::get_id()));
  assert(strlen(name) < 64 - 1);
  Status s = txn0->SetName(name);
  assert(s.ok());
  if (s.ok()) {
    // Without prepare it would simply skip the commit
    s = txn0->Prepare();
  }
  assert(s.ok());
  if (s.ok()) {
    s = txn0->Commit();
  }
  assert(s.ok());
  delete txn0;
}

const std::vector<SequenceNumber> WritePreparedTxnDB::GetSnapshotListFromDB(
    SequenceNumber max) {
  ROCKS_LOG_DETAILS(info_log_, "GetSnapshotListFromDB with max %" PRIu64, max);
  // Hold the DB mutex to serialize with timestamped snapshot creation. Regular
  // snapshots can still be created concurrently, but future_max_evicted_seq_
  // keeps their visibility checks complete if this scan misses them.
  InstrumentedMutexLock dblock(db_impl_->mutex());
  db_impl_->mutex()->AssertHeld();
  std::vector<SequenceNumber> snapshots;
  uint64_t snapshot_release_count = 0;
  const SequenceNumber oldest_snapshot = db_impl_->LoadSnapshots(
      &snapshots, nullptr, max, &snapshot_release_count);
  db_impl_->UpdateOldestSnapshotAndScheduleWork(oldest_snapshot,
                                                snapshot_release_count,
                                                /*installed_cfd=*/nullptr);
  return snapshots;
}

void WritePreparedTxnDB::ReleaseSnapshotInternal(
    const SequenceNumber snap_seq) {
  // TODO(myabandeh): relax should enough since the synchronizatin is already
  // done by snapshots_mutex_ under which this function is called.
  if (snap_seq <= max_evicted_seq_.load(std::memory_order_acquire)) {
    // Then this is a rare case that transaction did not finish before max
    // advances. It is expected for a few read-only backup snapshots. For such
    // snapshots we might have kept around a couple of entries in the
    // old_commit_map_. Check and do garbage collection if that is the case.
    bool need_gc = false;
    {
      WPRecordTick(TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD);
      ROCKS_LOG_WARN(info_log_, "old_commit_map_mutex_ overhead for %" PRIu64,
                     snap_seq);
      ReadLock rl(&old_commit_map_mutex_);
      auto prep_set_entry = old_commit_map_.find(snap_seq);
      need_gc = prep_set_entry != old_commit_map_.end();
    }
    if (need_gc) {
      WPRecordTick(TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD);
      ROCKS_LOG_WARN(info_log_, "old_commit_map_mutex_ overhead for %" PRIu64,
                     snap_seq);
      WriteLock wl(&old_commit_map_mutex_);
      old_commit_map_.erase(snap_seq);
      old_commit_map_empty_.store(old_commit_map_.empty(),
                                  std::memory_order_release);
    }
  }
}

void WritePreparedTxnDB::CleanupReleasedSnapshots(
    const std::vector<SequenceNumber>& new_snapshots,
    const std::vector<SequenceNumber>& old_snapshots) {
  auto newi = new_snapshots.begin();
  auto oldi = old_snapshots.begin();
  for (; newi != new_snapshots.end() && oldi != old_snapshots.end();) {
    assert(*newi >= *oldi);  // cannot have new snapshots with lower seq
    if (*newi == *oldi) {    // still not released
      auto value = *newi;
      while (newi != new_snapshots.end() && *newi == value) {
        newi++;
      }
      while (oldi != old_snapshots.end() && *oldi == value) {
        oldi++;
      }
    } else {
      assert(*newi > *oldi);  // *oldi is released
      ReleaseSnapshotInternal(*oldi);
      oldi++;
    }
  }
  // Everything remained in old_snapshots is released and must be cleaned up
  for (; oldi != old_snapshots.end(); oldi++) {
    ReleaseSnapshotInternal(*oldi);
  }
}

void WritePreparedTxnDB::UpdateSnapshots(
    const std::vector<SequenceNumber>& snapshots,
    const SequenceNumber& version) {
  ROCKS_LOG_DETAILS(info_log_, "UpdateSnapshots with version %" PRIu64,
                    version);
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  ROCKS_LOG_DETAILS(info_log_, "snapshots_mutex_ overhead");
  WriteLock wl(&snapshots_mutex_);
  snapshots_version_ = version;
  // We update the list concurrently with the readers.
  // Both new and old lists are sorted and the new list is subset of the
  // previous list plus some new items. Thus if a snapshot repeats in
  // both new and old lists, it will appear upper in the new list. So if
  // we simply insert the new snapshots in order, if an overwritten item
  // is still valid in the new list is either written to the same place in
  // the array or it is written in a higher place before it gets
  // overwritten by another item. This guarantee a reader that reads the
  // list bottom-up will eventually see a snapshot that repeats in the
  // update, either before it gets overwritten by the writer or
  // afterwards.
  size_t i = 0;
  auto it = snapshots.begin();
  for (; it != snapshots.end() && i < SNAPSHOT_CACHE_SIZE; ++it, ++i) {
    snapshot_cache_[i].store(*it, std::memory_order_release);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#ifndef NDEBUG
  // Release the remaining sync points since they are useless given that the
  // reader would also use lock to access snapshots
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#endif
  snapshots_.clear();
  for (; it != snapshots.end(); ++it) {
    // Insert them to a vector that is less efficient to access
    // concurrently
    snapshots_.push_back(*it);
  }
  // Update the size at the end. Otherwise a parallel reader might read
  // items that are not set yet.
  snapshots_total_.store(snapshots.size(), std::memory_order_release);

  // Note: this must be done after the snapshots data structures are updated
  // with the new list of snapshots.
  CleanupReleasedSnapshots(snapshots, snapshots_all_);
  snapshots_all_ = snapshots;

  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:end");
}

void WritePreparedTxnDB::CheckAgainstSnapshots(const CommitEntry& evicted) {
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  // First check the snapshot cache that is efficient for concurrent access
  auto cnt = snapshots_total_.load(std::memory_order_acquire);
  // The list might get updated concurrently as we are reading from it. The
  // reader should be able to read all the snapshots that are still valid
  // after the update. Since the survived snapshots are written in a higher
  // place before gets overwritten the reader that reads bottom-up will
  // eventually see it.
  const bool next_is_larger = true;
  // We will set to true if the border line snapshot suggests that.
  bool search_larger_list = false;
  size_t ip1 = std::min(cnt, SNAPSHOT_CACHE_SIZE);
  for (; 0 < ip1; ip1--) {
    SequenceNumber snapshot_seq =
        snapshot_cache_[ip1 - 1].load(std::memory_order_acquire);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:",
                        ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
    if (ip1 == SNAPSHOT_CACHE_SIZE) {  // border line snapshot
      // snapshot_seq < commit_seq => larger_snapshot_seq <= commit_seq
      // then later also continue the search to larger snapshots
      search_larger_list = snapshot_seq < evicted.commit_seq;
    }
    if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                 snapshot_seq, !next_is_larger)) {
      break;
    }
  }
#ifndef NDEBUG
  // Release the remaining sync points before acquiring the lock
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
  }
#endif
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:end");
  if (UNLIKELY(SNAPSHOT_CACHE_SIZE < cnt && search_larger_list)) {
    // Then access the less efficient list of snapshots_
    WPRecordTick(TXN_SNAPSHOT_MUTEX_OVERHEAD);
    ROCKS_LOG_WARN(info_log_,
                   "snapshots_mutex_ overhead for <%" PRIu64 ",%" PRIu64
                   "> with %" ROCKSDB_PRIszt " snapshots",
                   evicted.prep_seq, evicted.commit_seq, cnt);
    ReadLock rl(&snapshots_mutex_);
    // Items could have moved from the snapshots_ to snapshot_cache_ before
    // acquiring the lock. To make sure that we do not miss a valid snapshot,
    // read snapshot_cache_ again while holding the lock.
    for (size_t i = 0; i < SNAPSHOT_CACHE_SIZE; i++) {
      SequenceNumber snapshot_seq =
          snapshot_cache_[i].load(std::memory_order_acquire);
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq, next_is_larger)) {
        break;
      }
    }
    for (auto snapshot_seq_2 : snapshots_) {
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq_2, next_is_larger)) {
        break;
      }
    }
  }
}

bool WritePreparedTxnDB::MaybeUpdateOldCommitMap(
    const uint64_t& prep_seq, const uint64_t& commit_seq,
    const uint64_t& snapshot_seq, const bool next_is_larger = true) {
  // If we do not store an entry in old_commit_map_ we assume it is committed in
  // all snapshots. If commit_seq <= snapshot_seq, it is considered already in
  // the snapshot so we need not to keep the entry around for this snapshot.
  if (commit_seq <= snapshot_seq) {
    // continue the search if the next snapshot could be smaller than commit_seq
    return !next_is_larger;
  }
  // then snapshot_seq < commit_seq
  if (prep_seq <= snapshot_seq) {  // overlapping range
    WPRecordTick(TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD);
    ROCKS_LOG_WARN(info_log_,
                   "old_commit_map_mutex_ overhead for %" PRIu64
                   " commit entry: <%" PRIu64 ",%" PRIu64 ">",
                   snapshot_seq, prep_seq, commit_seq);
    WriteLock wl(&old_commit_map_mutex_);
    old_commit_map_empty_.store(false, std::memory_order_release);
    auto& vec = old_commit_map_[snapshot_seq];
    vec.insert(std::upper_bound(vec.begin(), vec.end(), prep_seq), prep_seq);
    // We need to store it once for each overlapping snapshot. Returning true to
    // continue the search if there is more overlapping snapshot.
    return true;
  }
  // continue the search if the next snapshot could be larger than prep_seq
  return next_is_larger;
}

WritePreparedTxnDB::~WritePreparedTxnDB() {
  // At this point there could be running compaction/flush holding a
  // SnapshotChecker, which holds a pointer back to WritePreparedTxnDB.
  // Make sure those jobs finished before destructing WritePreparedTxnDB.
  if (!db_impl_->shutting_down_) {
    db_impl_->CancelAllBackgroundWork(true /*wait*/);
  }
}

void SubBatchCounter::InitWithComp(const uint32_t cf) {
  auto cmp = comparators_[cf];
  keys_[cf] = CFKeys(SetComparator(cmp));
}

void SubBatchCounter::AddKey(const uint32_t cf, const Slice& key) {
  CFKeys& cf_keys = keys_[cf];
  if (cf_keys.size() == 0) {  // just inserted
    InitWithComp(cf);
  }
  auto it = cf_keys.insert(key);
  if (it.second == false) {  // second is false if a element already existed.
    batches_++;
    keys_.clear();
    InitWithComp(cf);
    keys_[cf].insert(key);
  }
}

}  // namespace ROCKSDB_NAMESPACE

// clang-format off
#define WITHOUT_COROUTINES
#include "utilities/transactions/write_prepared_txn_db_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "utilities/transactions/write_prepared_txn_db_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on
