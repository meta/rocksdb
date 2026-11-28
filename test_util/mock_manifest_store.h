//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "port/port.h"
#include "rocksdb/manifest_store.h"

namespace ROCKSDB_NAMESPACE {

// Background: unit tests of the ManifestStore contract need a ManifestStore
// that keeps its rows in memory and can return the results that the
// ManifestStore comments in include/rocksdb/manifest_store.h allow.
// MockManifestStore is that store. A test can make Open(), Commit() and
// WriteGeneration() fail on purpose (see Fault). ReadDbRow(), Scan() and
// Destroy() fail only for the reasons that the ManifestStore comments give.
//
// Layout: MockManifestStoreData holds the rows of one DB. Each
// MockManifestStore object holds one epoch, or no epoch for a read-only object,
// and points to the shared data. So a test can create two objects with
// different epochs to play two DB instances that take over the same DB.
//
// Behavior that differs from a real store:
// - Scan() copies all rows under a lock, then calls the visitor, so every
//   scan returns the rows of one commit, whatever ManifestScanOptions says.
// - A test chooses the next failure with MockManifestStoreData::SetNextFault()
//   and SetReadDbRowFailures(). Without them, no call fails except for the
//   reasons that the ManifestStore comments give: fenced, Busy (also for a
//   failed expected_commit_sequence of Open() and Destroy()), NotFound,
//   NotSupported for a read-only object, TryAgain for a failed generation
//   check, and the status of the visitor. Open() also returns Corruption when
//   db_field_updates or the stored DB row fields cannot be parsed.
//
// The tests of this store are MockManifestStoreTest in db/version_set_test.cc.

class MockManifestStoreData {
 public:
  // A failure for the next call of Open(), Commit() or WriteGeneration().
  enum class Fault {
    kNone,
    // Returns TryAgain. Not applied.
    kTryAgain,
    // Returns TimedOut. Not applied.
    kTimedOutNotApplied,
    // Open() and Commit(): apply the call, then return TimedOut, whatever the
    // apply returned (HiddenApplyCode() gives that code). WriteGeneration(): if
    // the switch applies, the call returns TimedOut (the caller cannot know
    // that it applied). If a chunk write or the switch fails, the call returns
    // that failure (IOFenced, Busy or TryAgain), as without a fault. In the
    // TimedOut case the mock still sets `*generation`.
    kTimedOutApplied,
    // Open() and Commit(): return TimedOut, and keep the call pending. The next
    // call of any of the six store methods (ReadDbRow(), Scan(), Open(),
    // Commit(), WriteGeneration(), Destroy()) on an object that shares this
    // data runs the pending call first, like a request that a real store
    // finishes late. The pending call runs its usual checks, so it can fail.
    // After the pending call runs, HiddenApplyCode() returns its status code;
    // before that, it returns no value. The pending call keeps the epoch of
    // the object that made it, so it runs with that epoch also after that
    // object is destroyed. WriteGeneration() treats it as kTimedOutApplied.
    kTimedOutLateApply,
    // WriteGeneration() only: the store loses one written row before the
    // switch, so the row count check of the switch fails.
    kLoseGenerationRow,
    // WriteGeneration() only: the store changes the bytes of one written row
    // before the switch, so the row count matches and only the checksum check
    // of the switch fails.
    kCorruptGenerationRow,
  };

  // Sets the failure of the next Open(), Commit() or WriteGeneration() call.
  // That call uses it once, then the fault is kNone again. Open() and Commit()
  // take the fault before any check, also for a read-only object.
  // WriteGeneration() takes the fault only after its epoch check passes, so a
  // read-only or fenced object leaves the fault for the next call.
  void SetNextFault(Fault fault);

  // Makes the next `count` ReadDbRow() calls return IOError, separately from
  // SetNextFault(), so that a test can fail the reads that follow a commit
  // with an unknown outcome.
  void SetReadDbRowFailures(int count);

  // Sets a function that WriteGeneration() calls after it writes each chunk
  // of rows, without holding the lock. A test can claim the DB with a newer
  // epoch there, to check that the remaining chunks are refused.
  // WriteGeneration() copies the function when it starts.
  void SetAfterChunkCallback(std::function<void()> callback);

  // The newest epoch that claimed the DB.
  uint64_t StoredEpoch() const;

  // The fault that the next call will use, so a test can check that no call
  // used it.
  Fault PendingFault() const;

  // The status code that the hidden apply returned for the last Open() or
  // Commit() call that failed with kTimedOutApplied or kTimedOutLateApply.
  // The caller of that call saw TimedOut. For kTimedOutLateApply, the value
  // is std::nullopt until the next call runs the late apply. WriteGeneration()
  // applies its switch itself and does not set it.
  std::optional<Status::Code> HiddenApplyCode() const;

 private:
  friend class MockManifestStore;

  // Rows other than the DB row, by key, ordered by (type, cf_id, number).
  using RowKey = std::tuple<uint8_t, uint32_t, uint64_t>;
  using RowMap = std::map<RowKey, std::string>;

  mutable port::Mutex mu_;
  uint64_t stored_epoch_ = 0;
  bool has_db_row_ = false;
  ManifestStoreDbState state_;
  std::string db_row_fields_;
  // The rows of the current generation.
  RowMap rows_;
  // Rows of generations that WriteGeneration() is writing, by generation.
  std::map<uint64_t, RowMap> pending_generations_;
  // One counter for all objects of the data, so each attempt gets a new
  // generation number.
  uint64_t generation_counter_ = 0;
  Fault next_fault_ = Fault::kNone;
  int read_db_row_failures_ = 0;
  // A call that returned TimedOut with kTimedOutLateApply, applied at the
  // start of the next call. It gets this data as its argument.
  std::function<void(MockManifestStoreData*)> late_apply_;
  std::optional<Status::Code> hidden_apply_code_;
  std::function<void()> after_chunk_callback_;
};

class MockManifestStore : public ManifestStore {
 public:
  // WriteGeneration() writes rows in chunks of this many rows. The value is
  // small, so that tests with a few rows get several chunks.
  static constexpr size_t kGenerationChunkRows = 2;

  // `epoch` is std::nullopt for a read-only object, for example for a secondary
  // instance. Such an object is never fenced in ReadDbRow() and Scan(), and
  // gets NotSupported from the methods that change rows. Open() returns
  // NotFound first when there is no DB row.
  MockManifestStore(std::shared_ptr<MockManifestStoreData> data,
                    std::optional<uint64_t> epoch);

  static const char* kClassName() { return "MockManifestStore"; }
  const char* Name() const override { return kClassName(); }
  IOStatus Open(const ManifestStoreOpenOptions& opts,
                ManifestDbRowResult* result) override;
  IOStatus Commit(const ManifestCommit& commit) override;
  IOStatus ReadDbRow(ManifestDbRowResult* result) override;
  IOStatus Scan(const ManifestScanOptions& opts,
                ManifestRowVisitor* visitor) override;
  IOStatus WriteGeneration(const ManifestGeneration& request,
                           uint64_t* generation) override;
  IOStatus Destroy(const ManifestStoreDestroyOptions& opts) override;

 private:
  using Fault = MockManifestStoreData::Fault;
  // An Open() or Commit() request with its epoch, applied to the data that it
  // gets as its argument (see ApplyWithFault()).
  using ApplyFunction = std::function<IOStatus(MockManifestStoreData*)>;

  // Runs a late apply, if one is waiting. Requires the lock.
  void RunLateApply();

  // Takes the next fault. Requires the lock.
  Fault TakeFault();

  // The static functions below take `data`, and some also take `epoch`,
  // instead of reading this object. So a pending call (kTimedOutLateApply) can
  // run them after the object that made it is destroyed. They all require the
  // lock of `data`.

  // Returns IOFenced if `epoch` is set and the stored epoch is newer.
  static IOStatus CheckEpoch(const MockManifestStoreData& data,
                             const std::optional<uint64_t>& epoch);

  // Returns NotSupported when `epoch` is not set (a read-only object), then as
  // CheckEpoch().
  static IOStatus CheckWriter(const MockManifestStoreData& data,
                              const std::optional<uint64_t>& epoch);

  // Returns Busy unless `expected` matches the stored DB row: 0 when there is
  // no DB row, or the stored commit sequence.
  static IOStatus CheckExpected(const MockManifestStoreData& data,
                                uint64_t expected);

  // The precondition of Open() and Destroy(): as CheckExpected() when
  // `expected` is set, and Busy unless the DB row exists with last_commit_id
  // `expected_id` when `expected_id` is not empty.
  static IOStatus CheckPrecondition(const MockManifestStoreData& data,
                                    const std::optional<uint64_t>& expected,
                                    const std::string& expected_id);

  // Sets the DB row fields and has_db_row_, raises the commit sequence by 1,
  // and sets last_commit_id.
  static void ApplyDbRow(MockManifestStoreData* data,
                         const std::string& commit_id,
                         const ManifestRow& db_row);

  // Copies the stored DB row into `result`. Requires the lock.
  void FillResult(ManifestDbRowResult* result) const;

  // Applies a claim or a commit of an object with `epoch`.
  static IOStatus ApplyOpen(MockManifestStoreData* data,
                            const std::optional<uint64_t>& epoch,
                            const ManifestStoreOpenOptions& opts);
  static IOStatus ApplyCommit(MockManifestStoreData* data,
                              const std::optional<uint64_t>& epoch,
                              const ManifestCommit& commit);

  // Runs `apply` (ApplyOpen() or ApplyCommit() with its epoch and request) as
  // the next fault says. Returns the result of `apply` when there is no fault.
  // Requires the lock.
  IOStatus ApplyWithFault(const ApplyFunction& apply);

  std::shared_ptr<MockManifestStoreData> data_;
  const std::optional<uint64_t> epoch_;
};

}  // namespace ROCKSDB_NAMESPACE
