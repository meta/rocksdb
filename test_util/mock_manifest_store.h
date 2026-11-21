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
// that keeps its rows in memory and can return each result that the
// ManifestStore comments in include/rocksdb/manifest_store.h allow, including
// the failures. MockManifestStore is that store.
//
// Layout: MockManifestStoreData holds the rows of one DB. Each
// MockManifestStore object holds one epoch, or no epoch for a reader, and
// points to the shared data. So a test can create two objects with different
// epochs to play two DB instances that take over the same DB.
//
// Behavior that differs from a real store:
// - Scan() copies all rows under a lock, then calls the visitor, so every
//   scan returns the rows of one commit, whatever ManifestScanOptions says.
// - A test chooses the next failure with MockManifestStoreData::SetNextFault().
//   Without it, no call fails except for the reasons that the
//   ManifestStore comments give: fenced, Busy, NotFound, NotSupported for a
//   reader, TryAgain for a failed generation check, and the status of the
//   visitor. Open() also returns Corruption when db_field_updates or the
//   stored DB row fields cannot be parsed.
//
// The tests of the store contract are ManifestStoreContractTest in
// db/version_set_test.cc.

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
    // apply returned (HiddenApplyCode() gives that code). WriteGeneration():
    // returns TimedOut only when its switch applies. It returns the failure of
    // a chunk write or of the switch (IOFenced, Busy or TryAgain).
    kTimedOutApplied,
    // Open() and Commit(): return TimedOut, and apply the call at the start of
    // the next call of any method on the same data, like a request that a real
    // store finishes late. The MockManifestStore object that made the call must
    // still exist then, because the late apply runs ApplyOpen() or
    // ApplyCommit() of that object. WriteGeneration() treats it as
    // kTimedOutApplied.
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
  // That call uses it once, then the fault is kNone again. WriteGeneration()
  // takes the fault only after its epoch check passes, so a reader or a fenced
  // object leaves the fault for the next call.
  void SetNextFault(Fault fault);

  // Sets a function that WriteGeneration() calls after it writes each chunk
  // of rows, without holding the lock. A test can claim the DB with a newer
  // epoch there, to check that the remaining chunks are refused.
  // WriteGeneration() copies the function when it starts.
  void SetAfterChunkCallback(std::function<void()> callback);

  // The newest epoch that claimed the DB.
  uint64_t StoredEpoch() const;

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
  // A call that returned TimedOut with kTimedOutLateApply, applied at the
  // start of the next call.
  std::function<void()> late_apply_;
  std::optional<Status::Code> hidden_apply_code_;
  std::function<void()> after_chunk_callback_;
};

class MockManifestStore : public ManifestStore {
 public:
  // WriteGeneration() writes rows in chunks of this many rows. The value is
  // small, so that tests with a few rows get several chunks.
  static constexpr size_t kGenerationChunkRows = 2;

  // `epoch` is std::nullopt for a reader, for example for a secondary
  // instance. A reader is never fenced in ReadDbRow() and Scan(), and gets
  // NotSupported from the methods that change rows. Open() returns NotFound
  // first when there is no DB row.
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
  IOStatus Destroy() override;

 private:
  using Fault = MockManifestStoreData::Fault;

  // Runs a late apply, if one is waiting. Requires the lock.
  void RunLateApply();

  // Takes the next fault. Requires the lock.
  Fault TakeFault();

  // Returns IOFenced if this object has an epoch and the stored epoch is
  // newer. Requires the lock.
  IOStatus CheckEpoch() const;

  // Returns NotSupported for a reader, then as CheckEpoch(). Requires the
  // lock.
  IOStatus CheckWriter() const;

  // Returns Busy unless `expected` matches the stored DB row: 0 when there is
  // no DB row, or the stored commit sequence. Requires the lock.
  IOStatus CheckExpected(uint64_t expected) const;

  // Sets the DB row fields and has_db_row_, raises the commit sequence by 1,
  // and sets last_commit_id. Requires the lock.
  void ApplyDbRow(const std::string& commit_id, const ManifestRow& db_row);

  // Copies the stored DB row into `result`. Requires the lock.
  void FillResult(ManifestDbRowResult* result) const;

  // Applies a claim or a commit. Requires the lock.
  IOStatus ApplyOpen(const ManifestStoreOpenOptions& opts);
  IOStatus ApplyCommit(const ManifestCommit& commit);

  // Runs `apply` (ApplyOpen() or ApplyCommit() with its request) as the next
  // fault says. Returns the result of `apply` when there is no fault.
  // Requires the lock.
  IOStatus ApplyWithFault(const std::function<IOStatus()>& apply);

  std::shared_ptr<MockManifestStoreData> data_;
  const std::optional<uint64_t> epoch_;
};

}  // namespace ROCKSDB_NAMESPACE
