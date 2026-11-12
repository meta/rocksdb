//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#include "db_stress_tool/multi_ops_txns_stress.h"

#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/defer.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/transactions/write_prepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

// The description of A and C can be found in multi_ops_txns_stress.h
DEFINE_int32(lb_a, 0, "(Inclusive) lower bound of A");
DEFINE_int32(ub_a, 1000, "(Exclusive) upper bound of A");
DEFINE_int32(lb_c, 0, "(Inclusive) lower bound of C");
DEFINE_int32(ub_c, 1000, "(Exclusive) upper bound of C");

DEFINE_string(key_spaces_path, "",
              "Path to file describing the lower and upper bounds of A and C");

DEFINE_int32(delay_snapshot_read_one_in, 0,
             "With a chance of 1/N, inject a random delay between taking "
             "snapshot and read.");

DEFINE_int32(rollback_one_in, 0,
             "If non-zero, rollback non-read-only transactions with a "
             "probability of 1/N.");

DEFINE_int32(clear_wp_commit_cache_one_in, 0,
             "If non-zero, evict all commit entries from commit cache with a "
             "probability of 1/N. This options applies to write-prepared and "
             "write-unprepared transactions.");

extern "C" bool rocksdb_write_prepared_TEST_ShouldClearCommitCache(void) {
  static Random rand(static_cast<uint32_t>(raw_env->NowMicros()));
  return FLAGS_clear_wp_commit_cache_one_in > 0 &&
         rand.OneIn(FLAGS_clear_wp_commit_cache_one_in);
}

// MultiOpsTxnsStressTest can either operate on a database with pre-populated
// data (possibly from previous ones), or create a new db and preload it with
// data specified via `-lb_a`, `-ub_a`, `-lb_c`, `-ub_c`, etc. Among these, we
// define the test key spaces as two key ranges: [lb_a, ub_a) and [lb_c, ub_c).
// The key spaces specification is persisted in a file whose absolute path can
// be specified via `-key_spaces_path`.
//
// Whether an existing db is used or a new one is created, key_spaces_path will
// be used. In the former case, the test reads the key spaces specification
// from `-key_spaces_path` and decodes [lb_a, ub_a) and [lb_c, ub_c). In the
// latter case, the test writes a key spaces specification to a file at the
// location, and this file will be used by future runs until a new db is
// created.
//
// Create a fresh new database (-destroy_db_initially=1 or there is no database
// in the location specified by -db). See PreloadDb().
//
// Use an existing, non-empty database. See ScanExistingDb().
//
// This test is multi-threaded, and thread count can be specified via
// `-threads`. For simplicity, we partition the key ranges and each thread
// operates on a subrange independently.
// Within each subrange, a KeyGenerator object is responsible for key
// generation. A KeyGenerator maintains two sets: set of existing keys within
// [low, high), set of non-existing keys within [low, high). [low, high) is the
// subrange. The test initialization makes sure there is at least one
// non-existing key, otherwise the test will return an error and exit before
// any test thread is spawned.

void MultiOpsTxnsStressTest::KeyGenerator::FinishInit() {
  assert(existing_.empty());
  assert(!existing_uniq_.empty());
  assert(low_ < high_);
  for (auto v : existing_uniq_) {
    assert(low_ <= v);
    assert(high_ > v);
    existing_.push_back(v);
  }
  if (non_existing_uniq_.empty()) {
    fprintf(
        stderr,
        "Cannot allocate key in [%u, %u)\nStart with a new DB or try change "
        "the number of threads for testing via -threads=<#threads>\n",
        static_cast<unsigned int>(low_), static_cast<unsigned int>(high_));
    fflush(stdout);
    fflush(stderr);
    assert(false);
  }
  initialized_ = true;
}

std::pair<uint32_t, uint32_t>
MultiOpsTxnsStressTest::KeyGenerator::ChooseExisting() {
  assert(initialized_);
  const size_t N = existing_.size();
  assert(N > 0);
  uint32_t rnd = rand_.Uniform(static_cast<int>(N));
  assert(rnd < N);
  return std::make_pair(existing_[rnd], rnd);
}

uint32_t MultiOpsTxnsStressTest::KeyGenerator::Allocate() {
  assert(initialized_);
  auto it = non_existing_uniq_.begin();
  assert(non_existing_uniq_.end() != it);
  uint32_t ret = *it;
  // Remove this element from non_existing_.
  // Need to call UndoAllocation() if the calling transaction does not commit.
  non_existing_uniq_.erase(it);
  return ret;
}

void MultiOpsTxnsStressTest::KeyGenerator::Replace(uint32_t old_val,
                                                   uint32_t old_pos,
                                                   uint32_t new_val) {
  assert(initialized_);
  {
    auto it = existing_uniq_.find(old_val);
    assert(it != existing_uniq_.end());
    existing_uniq_.erase(it);
  }

  {
    assert(0 == existing_uniq_.count(new_val));
    existing_uniq_.insert(new_val);
    existing_[old_pos] = new_val;
  }

  {
    assert(0 == non_existing_uniq_.count(old_val));
    non_existing_uniq_.insert(old_val);
  }
}

void MultiOpsTxnsStressTest::KeyGenerator::UndoAllocation(uint32_t new_val) {
  assert(initialized_);
  assert(0 == non_existing_uniq_.count(new_val));
  non_existing_uniq_.insert(new_val);
}

std::string MultiOpsTxnsStressTest::Record::EncodePrimaryKey(uint32_t a) {
  std::string ret;
  PutFixed32(&ret, kPrimaryIndexId);
  PutFixed32(&ret, a);

  char* const buf = ret.data();
  std::reverse(buf, buf + sizeof(kPrimaryIndexId));
  std::reverse(buf + sizeof(kPrimaryIndexId),
               buf + sizeof(kPrimaryIndexId) + sizeof(a));
  return ret;
}

std::string MultiOpsTxnsStressTest::Record::EncodeSecondaryKey(uint32_t c) {
  std::string ret;
  PutFixed32(&ret, kSecondaryIndexId);
  PutFixed32(&ret, c);

  char* const buf = ret.data();
  std::reverse(buf, buf + sizeof(kSecondaryIndexId));
  std::reverse(buf + sizeof(kSecondaryIndexId),
               buf + sizeof(kSecondaryIndexId) + sizeof(c));
  return ret;
}

std::string MultiOpsTxnsStressTest::Record::EncodeSecondaryKey(uint32_t c,
                                                               uint32_t a) {
  std::string ret;
  PutFixed32(&ret, kSecondaryIndexId);
  PutFixed32(&ret, c);
  PutFixed32(&ret, a);

  char* const buf = ret.data();
  std::reverse(buf, buf + sizeof(kSecondaryIndexId));
  std::reverse(buf + sizeof(kSecondaryIndexId),
               buf + sizeof(kSecondaryIndexId) + sizeof(c));
  std::reverse(buf + sizeof(kSecondaryIndexId) + sizeof(c),
               buf + sizeof(kSecondaryIndexId) + sizeof(c) + sizeof(a));
  return ret;
}

std::tuple<Status, uint32_t, uint32_t>
MultiOpsTxnsStressTest::Record::DecodePrimaryIndexValue(
    Slice primary_index_value) {
  if (primary_index_value.size() != 8) {
    return std::tuple<Status, uint32_t, uint32_t>{Status::Corruption(""), 0, 0};
  }
  uint32_t b = 0;
  uint32_t c = 0;
  if (!GetFixed32(&primary_index_value, &b) ||
      !GetFixed32(&primary_index_value, &c)) {
    assert(false);
    return std::tuple<Status, uint32_t, uint32_t>{Status::Corruption(""), 0, 0};
  }
  return std::tuple<Status, uint32_t, uint32_t>{Status::OK(), b, c};
}

std::pair<Status, uint32_t>
MultiOpsTxnsStressTest::Record::DecodeSecondaryIndexValue(
    Slice secondary_index_value) {
  if (secondary_index_value.size() != 4) {
    return std::make_pair(Status::Corruption(""), 0);
  }
  uint32_t crc = 0;
  bool result __attribute__((unused)) =
      GetFixed32(&secondary_index_value, &crc);
  assert(result);
  return std::make_pair(Status::OK(), crc);
}

std::pair<std::string, std::string>
MultiOpsTxnsStressTest::Record::EncodePrimaryIndexEntry() const {
  std::string primary_index_key = EncodePrimaryKey();
  std::string primary_index_value = EncodePrimaryIndexValue();
  return std::make_pair(primary_index_key, primary_index_value);
}

std::string MultiOpsTxnsStressTest::Record::EncodePrimaryKey() const {
  return EncodePrimaryKey(a_);
}

std::string MultiOpsTxnsStressTest::Record::EncodePrimaryIndexValue() const {
  std::string ret;
  PutFixed32(&ret, b_);
  PutFixed32(&ret, c_);
  return ret;
}

std::pair<std::string, std::string>
MultiOpsTxnsStressTest::Record::EncodeSecondaryIndexEntry() const {
  std::string secondary_index_key = EncodeSecondaryKey(c_, a_);

  // Secondary index value is always 4-byte crc32 of the secondary key
  std::string secondary_index_value;
  uint32_t crc =
      crc32c::Value(secondary_index_key.data(), secondary_index_key.size());
  PutFixed32(&secondary_index_value, crc);
  return std::make_pair(std::move(secondary_index_key), secondary_index_value);
}

std::string MultiOpsTxnsStressTest::Record::EncodeSecondaryKey() const {
  return EncodeSecondaryKey(c_, a_);
}

Status MultiOpsTxnsStressTest::Record::DecodePrimaryIndexEntry(
    Slice primary_index_key, Slice primary_index_value) {
  if (primary_index_key.size() != 8) {
    assert(false);
    return Status::Corruption("Primary index key length is not 8");
  }

  uint32_t index_id = 0;

  [[maybe_unused]] bool res = GetFixed32(&primary_index_key, &index_id);
  assert(res);
  index_id = EndianSwapValue(index_id);

  if (index_id != kPrimaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected primary index id: " << index_id;
    return Status::Corruption(oss.str());
  }

  res = GetFixed32(&primary_index_key, &a_);
  assert(res);
  a_ = EndianSwapValue(a_);
  assert(primary_index_key.empty());

  if (primary_index_value.size() != 8) {
    return Status::Corruption("Primary index value length is not 8");
  }
  GetFixed32(&primary_index_value, &b_);
  GetFixed32(&primary_index_value, &c_);
  return Status::OK();
}

Status MultiOpsTxnsStressTest::Record::DecodeSecondaryIndexEntry(
    Slice secondary_index_key, Slice secondary_index_value) {
  if (secondary_index_key.size() != 12) {
    return Status::Corruption("Secondary index key length is not 12");
  }
  uint32_t crc =
      crc32c::Value(secondary_index_key.data(), secondary_index_key.size());

  uint32_t index_id = 0;

  [[maybe_unused]] bool res = GetFixed32(&secondary_index_key, &index_id);
  assert(res);
  index_id = EndianSwapValue(index_id);

  if (index_id != kSecondaryIndexId) {
    std::ostringstream oss;
    oss << "Unexpected secondary index id: " << index_id;
    return Status::Corruption(oss.str());
  }

  assert(secondary_index_key.size() == 8);
  res = GetFixed32(&secondary_index_key, &c_);
  assert(res);
  c_ = EndianSwapValue(c_);

  assert(secondary_index_key.size() == 4);
  res = GetFixed32(&secondary_index_key, &a_);
  assert(res);
  a_ = EndianSwapValue(a_);
  assert(secondary_index_key.empty());

  if (secondary_index_value.size() != 4) {
    return Status::Corruption("Secondary index value length is not 4");
  }
  uint32_t val = 0;
  GetFixed32(&secondary_index_value, &val);
  if (val != crc) {
    std::ostringstream oss;
    oss << "Secondary index key checksum mismatch, stored: " << val
        << ", recomputed: " << crc;
    return Status::Corruption(oss.str());
  }
  return Status::OK();
}

void MultiOpsTxnsStressTest::FinishInitDb(SharedState* shared) {
  if (FLAGS_enable_compaction_filter) {
    // TODO (yanqin) enable compaction filter
  }
  ProcessRecoveredPreparedTxns(shared);

  ReopenAndPreloadDbIfNeeded(shared);
  // TODO (yanqin) parallelize if key space is large
  for (auto& key_gen : key_gen_for_a_) {
    assert(key_gen);
    key_gen->FinishInit();
  }
  // TODO (yanqin) parallelize if key space is large
  for (auto& key_gen : key_gen_for_c_) {
    assert(key_gen);
    key_gen->FinishInit();
  }
}

void MultiOpsTxnsStressTest::ReopenAndPreloadDbIfNeeded(SharedState* shared) {
  (void)shared;
  bool db_empty = false;
  {
    ReadOptions ropt;
    std::unique_ptr<ManagedSnapshot> snapshot = nullptr;
    if (FLAGS_auto_refresh_iterator_with_snapshot) {
      snapshot = std::make_unique<ManagedSnapshot>(db_);
      ropt.snapshot = snapshot->snapshot();
      ropt.auto_refresh_iterator_with_snapshot = true;
    }
    std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));
    iter->SeekToFirst();
    if (!iter->Valid()) {
      db_empty = true;
    }
  }

  if (db_empty) {
    PreloadDb(shared, FLAGS_threads, FLAGS_lb_a, FLAGS_ub_a, FLAGS_lb_c,
              FLAGS_ub_c);
  } else {
    fprintf(stdout,
            "Key ranges will be read from %s.\n-lb_a, -ub_a, -lb_c, -ub_c will "
            "be ignored\n",
            FLAGS_key_spaces_path.c_str());
    fflush(stdout);
    ScanExistingDb(shared, FLAGS_threads);
  }
}

// Used for point-lookup transaction
Status MultiOpsTxnsStressTest::TestGet(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  ThreadStatus::OperationType cur_op_type =
      ThreadStatusUtil::GetThreadOperation();
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);
  uint32_t a = 0;
  uint32_t pos = 0;
  std::tie(a, pos) = ChooseExistingA(thread);
  Status s = PointLookupTxn(thread, read_opts, a);
  ThreadStatusUtil::SetThreadOperation(cur_op_type);
  return s;
}

// Not used.
std::vector<Status> MultiOpsTxnsStressTest::TestMultiGet(
    ThreadState* /*thread*/, const ReadOptions& /*read_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  return std::vector<Status>{Status::NotSupported()};
}

// Wide columns are currently not supported by transactions.
void MultiOpsTxnsStressTest::TestGetEntity(
    ThreadState* /* thread */, const ReadOptions& /* read_opts */,
    const std::vector<int>& /* rand_column_families */,
    const std::vector<int64_t>& /* rand_keys */) {}

// Wide columns are currently not supported by transactions.
void MultiOpsTxnsStressTest::TestMultiGetEntity(
    ThreadState* /* thread */, const ReadOptions& /* read_opts */,
    const std::vector<int>& /* rand_column_families */,
    const std::vector<int64_t>& /* rand_keys */) {}

Status MultiOpsTxnsStressTest::TestPrefixScan(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;
  return Status::OK();
}

// Given a key K, this creates an iterator which scans to K and then
// does a random sequence of Next/Prev operations.
Status MultiOpsTxnsStressTest::TestIterate(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  ThreadStatus::OperationType cur_op_type =
      ThreadStatusUtil::GetThreadOperation();
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);
  uint32_t c = 0;
  uint32_t pos = 0;
  std::tie(c, pos) = ChooseExistingC(thread);
  Status s = RangeScanTxn(thread, read_opts, c);
  ThreadStatusUtil::SetThreadOperation(cur_op_type);
  return s;
}

Status MultiOpsTxnsStressTest::TestIterateAttributeGroups(
    ThreadState* /*thread*/, const ReadOptions& /*read_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  return Status::NotSupported();
}

// Not intended for use.
Status MultiOpsTxnsStressTest::TestPut(ThreadState* /*thread*/,
                                       WriteOptions& /*write_opts*/,
                                       const ReadOptions& /*read_opts*/,
                                       const std::vector<int>& /*cf_ids*/,
                                       const std::vector<int64_t>& /*keys*/,
                                       char (&value)[100]) {
  (void)value;
  return Status::NotSupported();
}

// Not intended for use.
Status MultiOpsTxnsStressTest::TestDelete(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  return Status::NotSupported();
}

// Not intended for use.
Status MultiOpsTxnsStressTest::TestDeleteRange(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  return Status::NotSupported();
}

void MultiOpsTxnsStressTest::TestIngestExternalFile(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
}

void MultiOpsTxnsStressTest::TestCompactRange(
    ThreadState* thread, int64_t /*rand_key*/, const Slice& /*start_key*/,
    ColumnFamilyHandle* column_family) {
  // TODO (yanqin).
  // May use GetRangeHash() for validation before and after DB::CompactRange()
  // completes.
  (void)thread;
  (void)column_family;
}

Status MultiOpsTxnsStressTest::TestBackupRestore(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
  return Status::OK();
}

Status MultiOpsTxnsStressTest::TestCheckpoint(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)rand_column_families;
  return Status::OK();
}

Status MultiOpsTxnsStressTest::TestApproximateSize(
    ThreadState* thread, uint64_t iteration,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& /*rand_keys*/) {
  // TODO (yanqin)
  (void)thread;
  (void)iteration;
  (void)rand_column_families;
  return Status::OK();
}

Status MultiOpsTxnsStressTest::TestCustomOperations(
    ThreadState* thread, const std::vector<int>& rand_column_families) {
  (void)rand_column_families;
  // Randomly choose from 0, 1, and 2.
  // TODO (yanqin) allow user to configure probability of each operation.
  uint32_t rand = thread->rand.Uniform(3);
  Status s;
  if (0 == rand) {
    // Update primary key.
    uint32_t old_a = 0;
    uint32_t pos = 0;
    std::tie(old_a, pos) = ChooseExistingA(thread);
    uint32_t new_a = GenerateNextA(thread);
    s = PrimaryKeyUpdateTxn(thread, old_a, pos, new_a);
  } else if (1 == rand) {
    // Update secondary key.
    uint32_t old_c = 0;
    uint32_t pos = 0;
    std::tie(old_c, pos) = ChooseExistingC(thread);
    uint32_t new_c = GenerateNextC(thread);
    s = SecondaryKeyUpdateTxn(thread, old_c, pos, new_c);
  } else if (2 == rand) {
    // Update primary index value.
    uint32_t a = 0;
    uint32_t pos = 0;
    std::tie(a, pos) = ChooseExistingA(thread);
    s = UpdatePrimaryIndexValueTxn(thread, a, /*b_delta=*/1);
  } else {
    // Should never reach here.
    assert(false);
  }
  if (!s.ok()) {
    fprintf(stderr, "Transaction failed %s\n", s.ToString().c_str());
    fflush(stderr);
    thread->shared->SafeTerminate();
  }

  return s;
}

void MultiOpsTxnsStressTest::RegisterAdditionalListeners() {
  options_.listeners.emplace_back(new MultiOpsTxnsStressListener(this));
}

void MultiOpsTxnsStressTest::PrepareTxnDbOptions(
    SharedState* /*shared*/, TransactionDBOptions& txn_db_opts) {
  // MultiOpsTxnStressTest uses SingleDelete to delete secondary keys, thus we
  // register this callback to let TxnDb know that when rolling back
  // a transaction, use only SingleDelete to cancel prior Put from the same
  // transaction if applicable.
  txn_db_opts.rollback_deletion_type_callback =
      [](TransactionDB* /*db*/, ColumnFamilyHandle* /*column_family*/,
         const Slice& key) {
        Slice ks = key;
        uint32_t index_id = 0;
        [[maybe_unused]] bool res = GetFixed32(&ks, &index_id);
        assert(res);
        index_id = EndianSwapValue(index_id);
        assert(index_id <= Record::kSecondaryIndexId);
        return index_id == Record::kSecondaryIndexId;
      };
}

Status MultiOpsTxnsStressTest::PrimaryKeyUpdateTxn(ThreadState* thread,
                                                   uint32_t old_a,
                                                   uint32_t old_a_pos,
                                                   uint32_t new_a) {
  std::string old_pk = Record::EncodePrimaryKey(old_a);
  std::string new_pk = Record::EncodePrimaryKey(new_a);
  std::unique_ptr<Transaction> txn;
  WriteOptions wopts;
  Status s = NewTxn(wopts, thread, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);
  txn->SetSnapshotOnNextOperation(/*notifier=*/nullptr);

  const Defer cleanup([new_a, &s, thread, this, &txn]() {
    if (s.ok()) {
      // Two gets, one for existing pk, one for locking potential new pk.
      thread->stats.AddGets(/*ngets=*/2, /*nfounds=*/1);
      thread->stats.AddDeletes(1);
      thread->stats.AddBytesForWrites(
          /*nwrites=*/2,
          Record::kPrimaryIndexEntrySize + Record::kSecondaryIndexEntrySize);
      thread->stats.AddSingleDeletes(1);
      return;
    }
    if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else if (s.IsBusy() || s.IsIncomplete()) {
      // ignore.
      // Incomplete also means rollback by application. See the transaction
      // implementations.
    } else {
      thread->stats.AddErrors(1);
    }
    auto& key_gen = key_gen_for_a_[thread->tid];
    key_gen->UndoAllocation(new_a);
    s = txn->Rollback();
    if (!s.ok()) {
      fprintf(stderr, "Transaction rollback failed %s\n", s.ToString().c_str());
      fflush(stderr);
      assert(false);
    }
  });

  ReadOptions ropts;
  ropts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  std::string value;
  s = txn->GetForUpdate(ropts, old_pk, &value);
  if (!s.ok()) {
    return s;
  }
  std::string empty_value;
  s = txn->GetForUpdate(ropts, new_pk, &empty_value);
  if (s.ok()) {
    assert(!empty_value.empty());
    s = Status::Busy();
    return s;
  } else if (!s.IsNotFound()) {
    return s;
  }

  auto result = Record::DecodePrimaryIndexValue(value);
  s = std::get<0>(result);
  if (!s.ok()) {
    return s;
  }
  uint32_t b = std::get<1>(result);
  uint32_t c = std::get<2>(result);

  ColumnFamilyHandle* cf = db_->DefaultColumnFamily();
  s = txn->Delete(cf, old_pk, /*assume_tracked=*/true);
  if (!s.ok()) {
    return s;
  }
  s = txn->Put(cf, new_pk, value, /*assume_tracked=*/true);
  if (!s.ok()) {
    return s;
  }

  auto* wb = txn->GetWriteBatch();
  assert(wb);

  std::string old_sk = Record::EncodeSecondaryKey(c, old_a);
  s = wb->SingleDelete(old_sk);
  if (!s.ok()) {
    return s;
  }

  Record record(new_a, b, c);
  std::string new_sk;
  std::string new_crc;
  std::tie(new_sk, new_crc) = record.EncodeSecondaryIndexEntry();
  s = wb->Put(new_sk, new_crc);
  if (!s.ok()) {
    return s;
  }

  s = txn->Prepare();

  if (!s.ok()) {
    return s;
  }

  if (FLAGS_rollback_one_in > 0 && thread->rand.OneIn(FLAGS_rollback_one_in)) {
    s = Status::Incomplete();
    return s;
  }

  s = WriteToCommitTimeWriteBatch(*txn);
  if (!s.ok()) {
    return s;
  }

  s = CommitAndCreateTimestampedSnapshotIfNeeded(thread, *txn);

  auto& key_gen = key_gen_for_a_.at(thread->tid);
  if (s.ok()) {
    key_gen->Replace(old_a, old_a_pos, new_a);
  }
  return s;
}

Status MultiOpsTxnsStressTest::SecondaryKeyUpdateTxn(ThreadState* thread,
                                                     uint32_t old_c,
                                                     uint32_t old_c_pos,
                                                     uint32_t new_c) {
  std::unique_ptr<Transaction> txn;
  WriteOptions wopts;
  Status s = NewTxn(wopts, thread, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  Iterator* it = nullptr;
  long iterations = 0;
  const Defer cleanup([new_c, &s, thread, &txn, &it, this, &iterations]() {
    delete it;
    if (s.ok()) {
      thread->stats.AddIterations(iterations);
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
      thread->stats.AddSingleDeletes(1);
      thread->stats.AddBytesForWrites(
          /*nwrites=*/2,
          Record::kPrimaryIndexEntrySize + Record::kSecondaryIndexEntrySize);
      return;
    } else if (s.IsBusy() || s.IsTimedOut() || s.IsTryAgain() ||
               s.IsMergeInProgress() || s.IsIncomplete()) {
      // ww-conflict detected, or
      // lock cannot be acquired, or
      // memtable history is not large enough for conflict checking, or
      // Merge operation cannot be resolved, or
      // application rollback.
      // TODO (yanqin) add stats for other cases?
    } else if (s.IsNotFound()) {
      // ignore.
    } else {
      thread->stats.AddErrors(1);
    }
    auto& key_gen = key_gen_for_c_[thread->tid];
    key_gen->UndoAllocation(new_c);
    s = txn->Rollback();
    if (!s.ok()) {
      fprintf(stderr, "Transaction rollback failed %s\n", s.ToString().c_str());
      fflush(stderr);
      assert(false);
    }
  });

  // TODO (yanqin) try SetSnapshotOnNextOperation(). We currently need to take
  // a snapshot here because we will later verify that point lookup in the
  // primary index using GetForUpdate() returns the same value for 'c' as the
  // iterator. The iterator does not need a snapshot though, because it will be
  // assigned the current latest (published) sequence in the db, which will be
  // no smaller than the snapshot created here. The GetForUpdate will perform
  // ww conflict checking to ensure GetForUpdate() (using the snapshot) sees
  // the same data as this iterator.
  txn->SetSnapshot();
  std::string old_sk_prefix = Record::EncodeSecondaryKey(old_c);
  std::string iter_ub_str = Record::EncodeSecondaryKey(old_c + 1);
  Slice iter_ub = iter_ub_str;
  std::function<bool(const TableProperties&)> table_filter;
  ReadOptions ropts;
  ropts.snapshot = txn->GetSnapshot();
  ropts.auto_refresh_iterator_with_snapshot =
      FLAGS_auto_refresh_iterator_with_snapshot;
  ropts.total_order_seek = true;
  ropts.iterate_upper_bound = &iter_ub;
  ropts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  if (FLAGS_use_sqfc_for_range_queries) {
    table_filter =
        sqfc_factory_->GetTableFilterForRangeQuery(old_sk_prefix, iter_ub);
    ropts.table_filter = &table_filter;
  }
  it = txn->GetIterator(ropts);

  assert(it);
  it->Seek(old_sk_prefix);
  if (!it->Valid()) {
    s = Status::NotFound();
    return s;
  }
  auto* wb = txn->GetWriteBatch();
  assert(wb);

  do {
    ++iterations;
    Record record;
    s = record.DecodeSecondaryIndexEntry(it->key(), it->value());
    if (!s.ok()) {
      fprintf(stderr, "Cannot decode secondary key (%s => %s): %s\n",
              it->key().ToString(true).c_str(),
              it->value().ToString(true).c_str(), s.ToString().c_str());
      assert(false);
      break;
    }
    // At this point, record.b is not known yet, thus we need to access
    // primary index.
    std::string pk = Record::EncodePrimaryKey(record.a_value());
    std::string value;
    ReadOptions read_opts;
    read_opts.rate_limiter_priority =
        FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
    read_opts.snapshot = txn->GetSnapshot();
    s = txn->GetForUpdate(read_opts, pk, &value);
    if (s.IsBusy() || s.IsTimedOut() || s.IsTryAgain() ||
        s.IsMergeInProgress()) {
      // Write conflict, or cannot acquire lock, or memtable size is not large
      // enough, or merge cannot be resolved.
      break;
    } else if (s.IsNotFound()) {
      // We can also fail verification here.
      std::ostringstream oss;
      auto* dbimpl = static_cast_with_check<DBImpl>(db_->GetRootDB());
      assert(dbimpl);
      oss << "snap " << read_opts.snapshot->GetSequenceNumber()
          << " (published " << dbimpl->GetLastPublishedSequence()
          << "), pk should exist: " << Slice(pk).ToString(true);
      fprintf(stderr, "%s\n", oss.str().c_str());
      assert(false);
      break;
    }
    if (!s.ok()) {
      std::ostringstream oss;
      auto* dbimpl = static_cast_with_check<DBImpl>(db_->GetRootDB());
      assert(dbimpl);
      oss << "snap " << read_opts.snapshot->GetSequenceNumber()
          << " (published " << dbimpl->GetLastPublishedSequence() << "), "
          << s.ToString();
      fprintf(stderr, "%s\n", oss.str().c_str());
      assert(false);
      break;
    }
    auto result = Record::DecodePrimaryIndexValue(value);
    s = std::get<0>(result);
    if (!s.ok()) {
      fprintf(stderr, "Cannot decode primary index value %s: %s\n",
              Slice(value).ToString(true).c_str(), s.ToString().c_str());
      assert(false);
      break;
    }
    uint32_t b = std::get<1>(result);
    uint32_t c = std::get<2>(result);
    if (c != old_c) {
      std::ostringstream oss;
      auto* dbimpl = static_cast_with_check<DBImpl>(db_->GetRootDB());
      assert(dbimpl);
      oss << "snap " << read_opts.snapshot->GetSequenceNumber()
          << " (published " << dbimpl->GetLastPublishedSequence()
          << "), pk/sk mismatch. pk: (a=" << record.a_value() << ", "
          << "c=" << c << "), sk: (c=" << old_c << ")";
      s = Status::Corruption();
      fprintf(stderr, "%s\n", oss.str().c_str());
      assert(false);
      break;
    }
    Record new_rec(record.a_value(), b, new_c);
    std::string new_primary_index_value = new_rec.EncodePrimaryIndexValue();
    ColumnFamilyHandle* cf = db_->DefaultColumnFamily();
    s = txn->Put(cf, pk, new_primary_index_value, /*assume_tracked=*/true);
    if (!s.ok()) {
      break;
    }
    std::string old_sk = it->key().ToString(/*hex=*/false);
    std::string new_sk;
    std::string new_crc;
    std::tie(new_sk, new_crc) = new_rec.EncodeSecondaryIndexEntry();
    s = wb->SingleDelete(old_sk);
    if (!s.ok()) {
      break;
    }
    s = wb->Put(new_sk, new_crc);
    if (!s.ok()) {
      break;
    }

    it->Next();
  } while (it->Valid());

  if (!s.ok()) {
    return s;
  }

  s = txn->Prepare();

  if (!s.ok()) {
    return s;
  }

  if (FLAGS_rollback_one_in > 0 && thread->rand.OneIn(FLAGS_rollback_one_in)) {
    s = Status::Incomplete();
    return s;
  }

  s = WriteToCommitTimeWriteBatch(*txn);
  if (!s.ok()) {
    return s;
  }

  s = CommitAndCreateTimestampedSnapshotIfNeeded(thread, *txn);

  if (s.ok()) {
    auto& key_gen = key_gen_for_c_.at(thread->tid);
    key_gen->Replace(old_c, old_c_pos, new_c);
  }

  return s;
}

Status MultiOpsTxnsStressTest::UpdatePrimaryIndexValueTxn(ThreadState* thread,
                                                          uint32_t a,
                                                          uint32_t b_delta) {
  std::string pk_str = Record::EncodePrimaryKey(a);
  std::unique_ptr<Transaction> txn;
  WriteOptions wopts;
  Status s = NewTxn(wopts, thread, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  const Defer cleanup([&s, thread, &txn]() {
    if (s.ok()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
      thread->stats.AddBytesForWrites(
          /*nwrites=*/1, /*nbytes=*/Record::kPrimaryIndexEntrySize);
      return;
    }
    if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else if (s.IsInvalidArgument()) {
      // ignored.
    } else if (s.IsBusy() || s.IsTimedOut() || s.IsTryAgain() ||
               s.IsMergeInProgress() || s.IsIncomplete()) {
      // ignored.
    } else {
      thread->stats.AddErrors(1);
    }
    s = txn->Rollback();
    if (!s.ok()) {
      fprintf(stderr, "Transaction rollback failed %s\n", s.ToString().c_str());
      fflush(stderr);
      assert(false);
    }
  });
  ReadOptions ropts;
  ropts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  std::string value;
  s = txn->GetForUpdate(ropts, pk_str, &value);
  if (!s.ok()) {
    return s;
  }
  auto result = Record::DecodePrimaryIndexValue(value);
  if (!std::get<0>(result).ok()) {
    s = std::get<0>(result);
    fprintf(stderr, "Cannot decode primary index value %s: %s\n",
            Slice(value).ToString(true).c_str(), s.ToString().c_str());
    assert(false);
    return s;
  }
  uint32_t b = std::get<1>(result) + b_delta;
  uint32_t c = std::get<2>(result);
  Record record(a, b, c);
  std::string primary_index_value = record.EncodePrimaryIndexValue();
  ColumnFamilyHandle* cf = db_->DefaultColumnFamily();
  s = txn->Put(cf, pk_str, primary_index_value, /*assume_tracked=*/true);
  if (!s.ok()) {
    return s;
  }
  s = txn->Prepare();
  if (!s.ok()) {
    return s;
  }

  if (FLAGS_rollback_one_in > 0 && thread->rand.OneIn(FLAGS_rollback_one_in)) {
    s = Status::Incomplete();
    return s;
  }

  s = WriteToCommitTimeWriteBatch(*txn);
  if (!s.ok()) {
    return s;
  }

  s = CommitAndCreateTimestampedSnapshotIfNeeded(thread, *txn);

  return s;
}

Status MultiOpsTxnsStressTest::PointLookupTxn(ThreadState* thread,
                                              ReadOptions ropts, uint32_t a) {
  std::string pk_str = Record::EncodePrimaryKey(a);
  // pk may or may not exist
  PinnableSlice value;

  std::unique_ptr<Transaction> txn;
  WriteOptions wopts;
  Status s = NewTxn(wopts, thread, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  const Defer cleanup([&s, thread, &txn]() {
    if (s.ok()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/1);
      return;
    } else if (s.IsNotFound()) {
      thread->stats.AddGets(/*ngets=*/1, /*nfounds=*/0);
    } else {
      thread->stats.AddErrors(1);
    }
    txn->Rollback().PermitUncheckedError();
  });

  std::shared_ptr<const Snapshot> snapshot;
  SetupSnapshot(thread, ropts, *txn, snapshot);

  if (FLAGS_delay_snapshot_read_one_in > 0 &&
      thread->rand.OneIn(FLAGS_delay_snapshot_read_one_in)) {
    uint64_t delay_ms = thread->rand.Uniform(100) + 1;
    db_->GetDBOptions().env->SleepForMicroseconds(
        static_cast<int>(delay_ms * 1000));
  }

  s = txn->Get(ropts, db_->DefaultColumnFamily(), pk_str, &value);
  if (s.ok()) {
    s = txn->Commit();
  }
  return s;
}

Status MultiOpsTxnsStressTest::RangeScanTxn(ThreadState* thread,
                                            ReadOptions ropts, uint32_t c) {
  std::string sk = Record::EncodeSecondaryKey(c);

  std::unique_ptr<Transaction> txn;
  WriteOptions wopts;
  Status s = NewTxn(wopts, thread, &txn);
  if (!s.ok()) {
    assert(!txn);
    thread->stats.AddErrors(1);
    return s;
  }

  assert(txn);

  const Defer cleanup([&s, thread, &txn]() {
    if (s.ok()) {
      thread->stats.AddIterations(1);
      return;
    }
    thread->stats.AddErrors(1);
    txn->Rollback().PermitUncheckedError();
  });

  std::shared_ptr<const Snapshot> snapshot;
  SetupSnapshot(thread, ropts, *txn, snapshot);

  if (FLAGS_delay_snapshot_read_one_in > 0 &&
      thread->rand.OneIn(FLAGS_delay_snapshot_read_one_in)) {
    uint64_t delay_ms = thread->rand.Uniform(100) + 1;
    db_->GetDBOptions().env->SleepForMicroseconds(
        static_cast<int>(delay_ms * 1000));
  }

  std::unique_ptr<Iterator> iter(txn->GetIterator(ropts));

  constexpr size_t total_nexts = 10;
  size_t nexts = 0;
  for (iter->Seek(sk);
       iter->Valid() && nexts < total_nexts && iter->status().ok();
       iter->Next(), ++nexts) {
  }

  if (iter->status().ok()) {
    s = txn->Commit();
  } else {
    s = iter->status();
  }

  return s;
}

void MultiOpsTxnsStressTest::VerifyDb(ThreadState* thread) const {
  if (thread->shared->HasVerificationFailedYet()) {
    return;
  }
  const Snapshot* const snapshot = db_->GetSnapshot();
  assert(snapshot);
  ManagedSnapshot snapshot_guard(db_, snapshot);

  std::ostringstream oss;
  oss << "[snap=" << snapshot->GetSequenceNumber() << ",";

  auto* dbimpl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  assert(dbimpl);

  oss << " last_published=" << dbimpl->GetLastPublishedSequence() << "] ";

  if (FLAGS_delay_snapshot_read_one_in > 0 &&
      thread->rand.OneIn(FLAGS_delay_snapshot_read_one_in)) {
    uint64_t delay_ms = thread->rand.Uniform(100) + 1;
    db_->GetDBOptions().env->SleepForMicroseconds(
        static_cast<int>(delay_ms * 1000));
  }

  // TODO (yanqin) with a probability, we can use either forward or backward
  // iterator in subsequent checks. We can also use more advanced features in
  // range scan. For now, let's just use simple forward iteration with
  // total_order_seek = true.

  // First, iterate primary index.
  size_t primary_index_entries_count = 0;
  {
    std::string iter_ub_str;
    PutFixed32(&iter_ub_str, Record::kPrimaryIndexId + 1);
    std::reverse(iter_ub_str.begin(), iter_ub_str.end());
    Slice iter_ub = iter_ub_str;

    std::string start_key;
    PutFixed32(&start_key, Record::kPrimaryIndexId);
    std::reverse(start_key.begin(), start_key.end());

    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions ropts;
    ropts.snapshot = snapshot;
    ropts.auto_refresh_iterator_with_snapshot =
        FLAGS_auto_refresh_iterator_with_snapshot;
    ropts.total_order_seek = true;
    ropts.iterate_upper_bound = &iter_ub;
    std::function<bool(const TableProperties&)> table_filter;
    if (FLAGS_use_sqfc_for_range_queries) {
      table_filter =
          sqfc_factory_->GetTableFilterForRangeQuery(start_key, iter_ub);
      ropts.table_filter = &table_filter;
    }

    std::unique_ptr<Iterator> it(db_->NewIterator(ropts));
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      Record record;
      Status s = record.DecodePrimaryIndexEntry(it->key(), it->value());
      if (!s.ok()) {
        oss << "Cannot decode primary index entry " << it->key().ToString(true)
            << "=>" << it->value().ToString(true) << ". Status is "
            << s.ToString();
        VerificationAbort(thread->shared, oss.str());
        assert(false);
        return;
      }
      ++primary_index_entries_count;

      // Search secondary index.
      uint32_t a = record.a_value();
      uint32_t c = record.c_value();
      char sk_buf[12];
      EncodeFixed32(sk_buf, Record::kSecondaryIndexId);
      std::reverse(sk_buf, sk_buf + sizeof(uint32_t));
      EncodeFixed32(sk_buf + sizeof(uint32_t), c);
      std::reverse(sk_buf + sizeof(uint32_t), sk_buf + 2 * sizeof(uint32_t));
      EncodeFixed32(sk_buf + 2 * sizeof(uint32_t), a);
      std::reverse(sk_buf + 2 * sizeof(uint32_t), sk_buf + sizeof(sk_buf));
      Slice sk(sk_buf, sizeof(sk_buf));
      std::string value;
      s = DbStressGet(db_, ropts, sk, &value);
      if (!s.ok()) {
        oss << "Cannot find secondary index entry " << sk.ToString(true)
            << ". Status is " << s.ToString();
        VerificationAbort(thread->shared, oss.str());
        assert(false);
        return;
      }
    }
  }

  // Second, iterate secondary index.
  size_t secondary_index_entries_count = 0;
  {
    std::string start_key;
    PutFixed32(&start_key, Record::kSecondaryIndexId);
    std::reverse(start_key.begin(), start_key.end());

    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions ropts;
    ropts.snapshot = snapshot;
    ropts.auto_refresh_iterator_with_snapshot =
        FLAGS_auto_refresh_iterator_with_snapshot;
    ropts.total_order_seek = true;

    std::unique_ptr<Iterator> it(db_->NewIterator(ropts));
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      ++secondary_index_entries_count;
      Record record;
      Status s = record.DecodeSecondaryIndexEntry(it->key(), it->value());
      if (!s.ok()) {
        oss << "Cannot decode secondary index entry "
            << it->key().ToString(true) << "=>" << it->value().ToString(true)
            << ". Status is " << s.ToString();
        VerificationAbort(thread->shared, oss.str());
        assert(false);
        return;
      }
      // After decoding secondary index entry, we know a and c. Crc is verified
      // in decoding phase.
      //
      // Form a primary key and search in the primary index.
      std::string pk = Record::EncodePrimaryKey(record.a_value());
      std::string value;
      s = DbStressGet(db_, ropts, pk, &value);
      if (!s.ok()) {
        oss << "Error searching pk " << Slice(pk).ToString(true) << ". "
            << s.ToString() << ". sk " << it->key().ToString(true);
        VerificationAbort(thread->shared, oss.str());
        assert(false);
        return;
      }
      auto result = Record::DecodePrimaryIndexValue(value);
      s = std::get<0>(result);
      if (!s.ok()) {
        oss << "Error decoding primary index value "
            << Slice(value).ToString(true) << ". Status is " << s.ToString();
        VerificationAbort(thread->shared, oss.str());
        assert(false);
        return;
      }
      uint32_t c_in_primary = std::get<2>(result);
      if (c_in_primary != record.c_value()) {
        oss << "Pk/sk mismatch. pk: " << Slice(pk).ToString(true) << "=>"
            << Slice(value).ToString(true) << " (a=" << record.a_value()
            << ", c=" << c_in_primary << "), sk: " << it->key().ToString(true)
            << " (c=" << record.c_value() << ")";
        VerificationAbort(thread->shared, oss.str());
        assert(false);
        return;
      }
    }
  }

  if (secondary_index_entries_count != primary_index_entries_count) {
    oss << "Pk/sk mismatch: primary index has " << primary_index_entries_count
        << " entries. Secondary index has " << secondary_index_entries_count
        << " entries.";
    VerificationAbort(thread->shared, oss.str());
    assert(false);
    return;
  }
}

// VerifyPkSkFast() can be called by MultiOpsTxnsStressListener's callbacks
// which can be called before TransactionDB::Open() returns to caller.
// Therefore, at that time, db_ and txn_db_  may still be nullptr.
// Caller has to make sure that the race condition does not happen.
// We use the atomically loaded `db` pointer throughout this function to avoid
// racing with destruction of the underlying DB.
void MultiOpsTxnsStressTest::VerifyPkSkFast(const ReadOptions& read_options,
                                            int job_id) {
  DB* const db = db_aptr_.load(std::memory_order_acquire);
  if (db == nullptr) {
    return;
  }

  ThreadStatus::OperationType cur_op_type =
      ThreadStatusUtil::GetThreadOperation();
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);
  const Snapshot* const snapshot = db->GetSnapshot();
  ThreadStatusUtil::SetThreadOperation(cur_op_type);
  assert(snapshot);
  ManagedSnapshot snapshot_guard(db, snapshot);

  std::ostringstream oss;
  auto* dbimpl = static_cast_with_check<DBImpl>(db->GetRootDB());
  assert(dbimpl);

  oss << "Job " << job_id << ": [" << snapshot->GetSequenceNumber() << ","
      << dbimpl->GetLastPublishedSequence() << "] ";

  std::string start_key;
  PutFixed32(&start_key, Record::kSecondaryIndexId);
  std::reverse(start_key.begin(), start_key.end());

  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ropts;
  ropts.snapshot = snapshot;
  ropts.auto_refresh_iterator_with_snapshot =
      FLAGS_auto_refresh_iterator_with_snapshot;
  ropts.total_order_seek = true;
  ropts.io_activity = read_options.io_activity;

  std::unique_ptr<Iterator> it(db->NewIterator(ropts));
  for (it->Seek(start_key); it->Valid(); it->Next()) {
    Record record;
    Status s = record.DecodeSecondaryIndexEntry(it->key(), it->value());
    if (!s.ok()) {
      oss << "Cannot decode secondary index entry " << it->key().ToString(true)
          << "=>" << it->value().ToString(true);
      fprintf(stderr, "%s\n", oss.str().c_str());
      fflush(stderr);
      assert(false);
    }
    // After decoding secondary index entry, we know a and c. Crc is verified
    // in decoding phase.
    //
    // Form a primary key and search in the primary index.
    std::string pk = Record::EncodePrimaryKey(record.a_value());
    std::string value;
    s = DbStressGet(db, ropts, pk, &value);
    if (!s.ok()) {
      oss << "Error searching pk " << Slice(pk).ToString(true) << ". "
          << s.ToString() << ". sk " << it->key().ToString(true);
      fprintf(stderr, "%s\n", oss.str().c_str());
      fflush(stderr);
      assert(false);
    }
    auto result = Record::DecodePrimaryIndexValue(value);
    s = std::get<0>(result);
    if (!s.ok()) {
      oss << "Error decoding primary index value "
          << Slice(value).ToString(true) << ". " << s.ToString();
      fprintf(stderr, "%s\n", oss.str().c_str());
      fflush(stderr);
      assert(false);
    }
    uint32_t c_in_primary = std::get<2>(result);
    if (c_in_primary != record.c_value()) {
      oss << "Pk/sk mismatch. pk: " << Slice(pk).ToString(true) << "=>"
          << Slice(value).ToString(true) << " (a=" << record.a_value()
          << ", c=" << c_in_primary << "), sk: " << it->key().ToString(true)
          << " (c=" << record.c_value() << ")";
      fprintf(stderr, "%s\n", oss.str().c_str());
      fflush(stderr);
      assert(false);
    }
  }
}

std::pair<uint32_t, uint32_t> MultiOpsTxnsStressTest::ChooseExistingA(
    ThreadState* thread) {
  uint32_t tid = thread->tid;
  auto& key_gen = key_gen_for_a_.at(tid);
  return key_gen->ChooseExisting();
}

uint32_t MultiOpsTxnsStressTest::GenerateNextA(ThreadState* thread) {
  uint32_t tid = thread->tid;
  auto& key_gen = key_gen_for_a_.at(tid);
  return key_gen->Allocate();
}

std::pair<uint32_t, uint32_t> MultiOpsTxnsStressTest::ChooseExistingC(
    ThreadState* thread) {
  uint32_t tid = thread->tid;
  auto& key_gen = key_gen_for_c_.at(tid);
  return key_gen->ChooseExisting();
}

uint32_t MultiOpsTxnsStressTest::GenerateNextC(ThreadState* thread) {
  uint32_t tid = thread->tid;
  auto& key_gen = key_gen_for_c_.at(tid);
  return key_gen->Allocate();
}

void MultiOpsTxnsStressTest::ProcessRecoveredPreparedTxnsHelper(
    Transaction* txn, SharedState* shared) {
  thread_local Random rand(static_cast<uint32_t>(FLAGS_seed));
  if (rand.OneIn(2)) {
    Status s = txn->Commit();
    ProcessStatus(shared, "ProcessRecoveredPreparedTxnsHelper", s,
                  /*ignore_injected_error=*/false);
  } else {
    Status s = txn->Rollback();
    ProcessStatus(shared, "ProcessRecoveredPreparedTxnsHelper", s,
                  /*ignore_injected_error=*/false);
  }
}

Status MultiOpsTxnsStressTest::WriteToCommitTimeWriteBatch(Transaction& txn) {
  WriteBatch* ctwb = txn.GetCommitTimeWriteBatch();
  assert(ctwb);
  // Do not change the content in key_buf.
  static constexpr char key_buf[sizeof(Record::kMetadataPrefix) + 4] = {
      '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\xff'};

  uint64_t counter_val = counter_.Next();
  char val_buf[sizeof(counter_val)];
  EncodeFixed64(val_buf, counter_val);
  return ctwb->Put(Slice(key_buf, sizeof(key_buf)),
                   Slice(val_buf, sizeof(val_buf)));
}

Status MultiOpsTxnsStressTest::CommitAndCreateTimestampedSnapshotIfNeeded(
    ThreadState* thread, Transaction& txn) {
  Status s;
  if (FLAGS_create_timestamped_snapshot_one_in > 0 &&
      thread->rand.OneInOpt(FLAGS_create_timestamped_snapshot_one_in)) {
    uint64_t ts = raw_env->NowNanos();
    std::shared_ptr<const Snapshot> snapshot;
    s = txn.CommitAndTryCreateSnapshot(/*notifier=*/nullptr, ts, &snapshot);
  } else {
    s = txn.Commit();
  }
  if (!s.ok()) {
    fprintf(stderr, "Txn %s commit failed with %s\n", txn.GetName().c_str(),
            s.ToString().c_str());
    fflush(stderr);
  }

  assert(txn_db_);
  if (FLAGS_create_timestamped_snapshot_one_in > 0 &&
      thread->rand.OneInOpt(50000)) {
    uint64_t now = raw_env->NowNanos();
    constexpr uint64_t time_diff = static_cast<uint64_t>(1000) * 1000 * 1000;
    txn_db_->ReleaseTimestampedSnapshotsOlderThan(now - time_diff);
  }
  return s;
}

void MultiOpsTxnsStressTest::SetupSnapshot(
    ThreadState* thread, ReadOptions& read_opts, Transaction& txn,
    std::shared_ptr<const Snapshot>& snapshot) {
  if (thread->rand.OneInOpt(2)) {
    snapshot = txn_db_->GetLatestTimestampedSnapshot();
  }

  if (snapshot) {
    read_opts.snapshot = snapshot.get();
  } else {
    txn.SetSnapshot();
    read_opts.snapshot = txn.GetSnapshot();
  }
}

std::string MultiOpsTxnsStressTest::KeySpaces::EncodeTo() const {
  std::string result;
  PutFixed32(&result, lb_a);
  PutFixed32(&result, ub_a);
  PutFixed32(&result, lb_c);
  PutFixed32(&result, ub_c);
  return result;
}

bool MultiOpsTxnsStressTest::KeySpaces::DecodeFrom(Slice data) {
  if (!GetFixed32(&data, &lb_a) || !GetFixed32(&data, &ub_a) ||
      !GetFixed32(&data, &lb_c) || !GetFixed32(&data, &ub_c)) {
    return false;
  }
  return true;
}

void MultiOpsTxnsStressTest::PersistKeySpacesDesc(
    const std::string& key_spaces_path, uint32_t lb_a, uint32_t ub_a,
    uint32_t lb_c, uint32_t ub_c) {
  KeySpaces key_spaces(lb_a, ub_a, lb_c, ub_c);
  std::string key_spaces_rep = key_spaces.EncodeTo();

  std::unique_ptr<WritableFile> wfile;
  Status s1 =
      Env::Default()->NewWritableFile(key_spaces_path, &wfile, EnvOptions());
  DB_STRESS_ASSERT_OK(s1);
  assert(wfile);
  s1 = wfile->Append(key_spaces_rep);
  DB_STRESS_ASSERT_OK(s1);
}

MultiOpsTxnsStressTest::KeySpaces MultiOpsTxnsStressTest::ReadKeySpacesDesc(
    const std::string& key_spaces_path) {
  KeySpaces key_spaces;
  std::unique_ptr<SequentialFile> sfile;
  Status s1 =
      Env::Default()->NewSequentialFile(key_spaces_path, &sfile, EnvOptions());
  DB_STRESS_ASSERT_OK(s1);
  assert(sfile);
  char buf[16];
  Slice result;
  s1 = sfile->Read(sizeof(buf), &result, buf);
  DB_STRESS_ASSERT_OK(s1);
  if (!key_spaces.DecodeFrom(result)) {
    assert(false);
  }
  return key_spaces;
}

// Create an empty database if necessary and preload it with initial test data.
// Key range [lb_a, ub_a), [lb_c, ub_c). The key ranges will be shared by
// 'threads' threads.
// PreloadDb() also sets up KeyGenerator objects for each sub key range
// operated on by each thread.
// Both [lb_a, ub_a) and [lb_c, ub_c) are partitioned. Each thread operates on
// one sub range, using KeyGenerators to generate keys.
// For example, we choose a from [0, 10000) and c from [0, 100). Number of
// threads is 32, their tids range from 0 to 31.
// Thread k chooses a from [312*k,312*(k+1)) and c from [3*k,3*(k+1)) if k<31.
// Thread 31 chooses a from [9672, 10000) and c from [93, 100).
// Within each subrange: a from [low1, high1), c from [low2, high2).
// high1 - low1 > high2 - low2
// We reserve {high1 - 1} and {high2 - 1} as unallocated.
// The records are <low1,low2>, <low1+1,low2+1>, ...,
// <low1+k,low2+k%(high2-low2-1), <low1+k+1,low2+(k+1)%(high2-low2-1)>, ...
void MultiOpsTxnsStressTest::PreloadDb(SharedState* shared, int threads,
                                       uint32_t lb_a, uint32_t ub_a,
                                       uint32_t lb_c, uint32_t ub_c) {
  key_gen_for_a_.resize(threads);
  key_gen_for_c_.resize(threads);

  assert(ub_a > lb_a && ub_a > lb_a + threads);
  assert(ub_c > lb_c && ub_c > lb_c + threads);

  PersistKeySpacesDesc(FLAGS_key_spaces_path, lb_a, ub_a, lb_c, ub_c);

  fprintf(stdout, "a from [%u, %u), c from [%u, %u)\n",
          static_cast<unsigned int>(lb_a), static_cast<unsigned int>(ub_a),
          static_cast<unsigned int>(lb_c), static_cast<unsigned int>(ub_c));

  const uint32_t num_c = ub_c - lb_c;
  const uint32_t num_c_per_thread = num_c / threads;
  const uint32_t num_a = ub_a - lb_a;
  const uint32_t num_a_per_thread = num_a / threads;

  WriteOptions wopts;
  wopts.disableWAL = FLAGS_disable_wal;
  Random rnd(shared->GetSeed());
  assert(txn_db_);

  std::vector<KeySet> existing_a_uniqs(threads);
  std::vector<KeySet> non_existing_a_uniqs(threads);
  std::vector<KeySet> existing_c_uniqs(threads);
  std::vector<KeySet> non_existing_c_uniqs(threads);

  for (uint32_t a = lb_a; a < ub_a; ++a) {
    uint32_t tid = (a - lb_a) / num_a_per_thread;
    if (tid >= static_cast<uint32_t>(threads)) {
      tid = threads - 1;
    }

    uint32_t a_base = lb_a + tid * num_a_per_thread;
    uint32_t a_hi = (tid < static_cast<uint32_t>(threads - 1))
                        ? (a_base + num_a_per_thread)
                        : ub_a;
    uint32_t a_delta = a - a_base;

    if (a == a_hi - 1) {
      non_existing_a_uniqs[tid].insert(a);
      continue;
    }

    uint32_t c_base = lb_c + tid * num_c_per_thread;
    uint32_t c_hi = (tid < static_cast<uint32_t>(threads - 1))
                        ? (c_base + num_c_per_thread)
                        : ub_c;
    uint32_t c_delta = a_delta % (c_hi - c_base - 1);
    uint32_t c = c_base + c_delta;

    uint32_t b = rnd.Next();
    Record record(a, b, c);
    WriteBatch wb;
    const auto primary_index_entry = record.EncodePrimaryIndexEntry();
    Status s = wb.Put(primary_index_entry.first, primary_index_entry.second);
    ProcessStatus(shared, "PreloadDB", s, /*ignore_injected_error=*/false);

    const auto secondary_index_entry = record.EncodeSecondaryIndexEntry();
    s = wb.Put(secondary_index_entry.first, secondary_index_entry.second);
    ProcessStatus(shared, "PreloadDB", s, /*ignore_injected_error=*/false);

    s = txn_db_->Write(wopts, &wb);
    DB_STRESS_ASSERT_OK(s);
    ProcessStatus(shared, "PreloadDB", s, /*ignore_injected_error=*/false);

    // TODO (yanqin): make the following check optional, especially when data
    // size is large.
    Record tmp_rec;
    tmp_rec.SetB(record.b_value());
    s = tmp_rec.DecodeSecondaryIndexEntry(secondary_index_entry.first,
                                          secondary_index_entry.second);
    ProcessStatus(shared, "PreloadDB", s, /*ignore_injected_error=*/false);
    assert(tmp_rec == record);

    existing_a_uniqs[tid].insert(a);
    existing_c_uniqs[tid].insert(c);
  }

  for (int i = 0; i < threads; ++i) {
    uint32_t my_seed = i + shared->GetSeed();

    auto& key_gen_for_a = key_gen_for_a_[i];
    assert(!key_gen_for_a);
    uint32_t low = lb_a + i * num_a_per_thread;
    uint32_t high = (i < threads - 1) ? (low + num_a_per_thread) : ub_a;
    assert(existing_a_uniqs[i].size() == high - low - 1);
    assert(non_existing_a_uniqs[i].size() == 1);
    key_gen_for_a = std::make_unique<KeyGenerator>(
        my_seed, low, high, std::move(existing_a_uniqs[i]),
        std::move(non_existing_a_uniqs[i]));

    auto& key_gen_for_c = key_gen_for_c_[i];
    assert(!key_gen_for_c);
    low = lb_c + i * num_c_per_thread;
    high = (i < threads - 1) ? (low + num_c_per_thread) : ub_c;
    non_existing_c_uniqs[i].insert(high - 1);
    assert(existing_c_uniqs[i].size() == high - low - 1);
    assert(non_existing_c_uniqs[i].size() == 1);
    key_gen_for_c = std::make_unique<KeyGenerator>(
        my_seed, low, high, std::move(existing_c_uniqs[i]),
        std::move(non_existing_c_uniqs[i]));
  }
}

// Scan an existing, non-empty database.
// Set up [lb_a, ub_a) and [lb_c, ub_c) as test key ranges.
// Set up KeyGenerator objects for each sub key range operated on by each
// thread.
// Scan the entire database and for each subrange, populate the existing keys
// and non-existing keys. We currently require the non-existing keys be
// non-empty after initialization.
void MultiOpsTxnsStressTest::ScanExistingDb(SharedState* shared, int threads) {
  key_gen_for_a_.resize(threads);
  key_gen_for_c_.resize(threads);

  KeySpaces key_spaces = ReadKeySpacesDesc(FLAGS_key_spaces_path);

  const uint32_t lb_a = key_spaces.lb_a;
  const uint32_t ub_a = key_spaces.ub_a;
  const uint32_t lb_c = key_spaces.lb_c;
  const uint32_t ub_c = key_spaces.ub_c;

  assert(lb_a < ub_a && lb_c < ub_c);

  fprintf(stdout, "a from [%u, %u), c from [%u, %u)\n",
          static_cast<unsigned int>(lb_a), static_cast<unsigned int>(ub_a),
          static_cast<unsigned int>(lb_c), static_cast<unsigned int>(ub_c));

  assert(ub_a > lb_a && ub_a > lb_a + threads);
  assert(ub_c > lb_c && ub_c > lb_c + threads);

  const uint32_t num_c = ub_c - lb_c;
  const uint32_t num_c_per_thread = num_c / threads;
  const uint32_t num_a = ub_a - lb_a;
  const uint32_t num_a_per_thread = num_a / threads;

  assert(db_);
  ReadOptions ropts;
  std::unique_ptr<ManagedSnapshot> snapshot = nullptr;
  if (FLAGS_auto_refresh_iterator_with_snapshot) {
    snapshot = std::make_unique<ManagedSnapshot>(db_);
    ropts.snapshot = snapshot->snapshot();
    ropts.auto_refresh_iterator_with_snapshot = true;
  }

  std::vector<KeySet> existing_a_uniqs(threads);
  std::vector<KeySet> non_existing_a_uniqs(threads);
  std::vector<KeySet> existing_c_uniqs(threads);
  std::vector<KeySet> non_existing_c_uniqs(threads);
  {
    std::string pk_lb_str = Record::EncodePrimaryKey(0);
    std::string pk_ub_str =
        Record::EncodePrimaryKey(std::numeric_limits<uint32_t>::max());
    Slice pk_lb = pk_lb_str;
    Slice pk_ub = pk_ub_str;
    std::function<bool(const TableProperties&)> table_filter;
    ropts.iterate_lower_bound = &pk_lb;
    ropts.iterate_upper_bound = &pk_ub;
    ropts.total_order_seek = true;
    if (FLAGS_use_sqfc_for_range_queries) {
      table_filter = sqfc_factory_->GetTableFilterForRangeQuery(pk_lb, pk_ub);
      ropts.table_filter = &table_filter;
    }
    std::unique_ptr<Iterator> it(db_->NewIterator(ropts));

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Record record;
      Status s = record.DecodePrimaryIndexEntry(it->key(), it->value());
      DB_STRESS_ASSERT_OK_MSG(s, "Cannot decode primary index entry (%s => %s)",
                              it->key().ToString(true).c_str(),
                              it->value().ToString(true).c_str());
      uint32_t a = record.a_value();
      assert(a >= lb_a);
      assert(a < ub_a);
      uint32_t tid = (a - lb_a) / num_a_per_thread;
      if (tid >= static_cast<uint32_t>(threads)) {
        tid = threads - 1;
      }

      existing_a_uniqs[tid].insert(a);

      uint32_t c = record.c_value();
      assert(c >= lb_c);
      assert(c < ub_c);
      tid = (c - lb_c) / num_c_per_thread;
      if (tid >= static_cast<uint32_t>(threads)) {
        tid = threads - 1;
      }
      auto& existing_c_uniq = existing_c_uniqs[tid];
      existing_c_uniq.insert(c);
    }

    for (uint32_t a = lb_a; a < ub_a; ++a) {
      uint32_t tid = (a - lb_a) / num_a_per_thread;
      if (tid >= static_cast<uint32_t>(threads)) {
        tid = threads - 1;
      }
      if (0 == existing_a_uniqs[tid].count(a)) {
        non_existing_a_uniqs[tid].insert(a);
      }
    }

    for (uint32_t c = lb_c; c < ub_c; ++c) {
      uint32_t tid = (c - lb_c) / num_c_per_thread;
      if (tid >= static_cast<uint32_t>(threads)) {
        tid = threads - 1;
      }
      if (0 == existing_c_uniqs[tid].count(c)) {
        non_existing_c_uniqs[tid].insert(c);
      }
    }

    for (int i = 0; i < threads; ++i) {
      uint32_t my_seed = i + shared->GetSeed();
      auto& key_gen_for_a = key_gen_for_a_[i];
      assert(!key_gen_for_a);
      uint32_t low = lb_a + i * num_a_per_thread;
      uint32_t high = (i < threads - 1) ? (low + num_a_per_thread) : ub_a;

      // The following two assertions assume the test thread count and key
      // space remain the same across different runs. Will need to relax.
      assert(existing_a_uniqs[i].size() == high - low - 1);
      assert(non_existing_a_uniqs[i].size() == 1);

      key_gen_for_a = std::make_unique<KeyGenerator>(
          my_seed, low, high, std::move(existing_a_uniqs[i]),
          std::move(non_existing_a_uniqs[i]));

      auto& key_gen_for_c = key_gen_for_c_[i];
      assert(!key_gen_for_c);
      low = lb_c + i * num_c_per_thread;
      high = (i < threads - 1) ? (low + num_c_per_thread) : ub_c;

      // The following two assertions assume the test thread count and key
      // space remain the same across different runs. Will need to relax.
      assert(existing_c_uniqs[i].size() == high - low - 1);
      assert(non_existing_c_uniqs[i].size() == 1);

      key_gen_for_c = std::make_unique<KeyGenerator>(
          my_seed, low, high, std::move(existing_c_uniqs[i]),
          std::move(non_existing_c_uniqs[i]));
    }
  }
}

StressTest* CreateMultiOpsTxnsStressTest(int db_index,
                                         const std::string& db_path,
                                         const std::string& ev_path,
                                         const std::string& sec_path) {
  return new MultiOpsTxnsStressTest(db_index, db_path, ev_path, sec_path);
}

void CheckAndSetOptionsForMultiOpsTxnStressTest() {
  if (FLAGS_test_batches_snapshots || FLAGS_test_cf_consistency) {
    fprintf(stderr,
            "-test_multi_ops_txns is not compatible with "
            "-test_bathces_snapshots and -test_cf_consistency\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
  if (!FLAGS_use_txn) {
    fprintf(stderr, "-use_txn must be true if -test_multi_ops_txns\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  } else if (FLAGS_test_secondary > 0) {
    fprintf(
        stderr,
        "secondary instance does not support replaying logs (MANIFEST + WAL) "
        "of TransactionDB with write-prepared/write-unprepared policy\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
  if (FLAGS_clear_column_family_one_in > 0) {
    fprintf(stderr,
            "-test_multi_ops_txns is not compatible with clearing column "
            "families\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
  if (FLAGS_column_families > 1) {
    // TODO (yanqin) support separating primary index and secondary index in
    // different column families.
    fprintf(stderr,
            "-test_multi_ops_txns currently does not use more than one column "
            "family\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
  if (FLAGS_writepercent > 0 || FLAGS_delpercent > 0 ||
      FLAGS_delrangepercent > 0) {
    fprintf(stderr,
            "-test_multi_ops_txns requires that -writepercent, -delpercent and "
            "-delrangepercent be 0\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
  if (FLAGS_key_spaces_path.empty()) {
    fprintf(stderr,
            "Must specify a file to store ranges of A and C via "
            "-key_spaces_path\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
  if (FLAGS_create_timestamped_snapshot_one_in > 0) {
    if (FLAGS_txn_write_policy !=
        static_cast<uint64_t>(TxnDBWritePolicy::WRITE_COMMITTED)) {
      fprintf(stderr,
              "Timestamped snapshot is not yet supported by "
              "write-prepared/write-unprepared transactions\n");
      exit(1);  // NOLINT(concurrency-mt-unsafe)
    }
  }
  if (FLAGS_sync_fault_injection == 1) {
    fprintf(stderr,
            "Sync fault injection is currently not supported in "
            "-test_multi_ops_txns\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
