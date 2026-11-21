//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/manifest_store.h"
#include "test_util/mock_manifest_store.h"

namespace ROCKSDB_NAMESPACE {

// The state of one store dir that the objects of one process share.
struct FileManifestStoreState;

// Background: db_stress runs DBs in store mode (--use_manifest_store), where
// the DB keeps its MANIFEST state in a ManifestStore, or in shadow mode
// (--manifest_store_shadow), where the DB copies its MANIFEST state to the
// store. The crash test kills db_stress and starts it again on the same DB, so
// the store must keep its state across a process kill, as the external database
// behind a real store does. FileManifestStore is that store. It is test code.
//
// Store dir: all state of one store is in one file, <store dir>/STORE. Only one
// db_stress process uses a store dir at a time.
//
// Persistence: each FileManifestStore call that changes the state writes the
// whole state to <store dir>/STORE.tmp, syncs it, renames it over STORE and
// syncs the store dir. So a process kill keeps either the old or the new state.
// The files go through the Env that the caller passes in. db_stress passes its
// raw Env, not the fault injection file system, because the store stands for an
// external database that a crash of the DB host does not touch. Each change
// rewrites the whole file, syncs it and syncs the store dir, which costs time
// linear in the number of rows. This was not measured; the estimate is that it
// stays small for the DB sizes of db_stress.
//
// Sharing: the objects of one process share one in-memory copy of the state per
// store dir (MockManifestStoreData, in a process-wide registry), so they behave
// as clients of one store. The registry loads the state from the file when the
// first object of a store dir is created.
//
// Epochs: each writer object gets a new epoch, larger than every epoch that the
// store saw, as a new primary would. So the claim of each new DB open fences
// the objects of the earlier opens. A reader object has no epoch.
//
// Results: the same as MockManifestStore (test_util/mock_manifest_store.h),
// including that Scan() always returns the rows of one commit.
//
// Faults: with fault_one_in N > 0 and faults enabled (SetFaultsEnabled()), each
// Open(), Commit() and WriteGeneration() call fails with a chance of 1 in N,
// except for the commits below. The fault is one of the
// MockManifestStoreData::Fault values, chosen evenly: TryAgain, TimedOut not
// applied, TimedOut applied, TimedOut with a late apply, and for
// WriteGeneration() also a lost or a changed row. A late apply runs at the
// start of the next call on the same store dir. A process kill before that
// call, or the destruction of any object of the store dir (also a short-lived
// reader, not only the object that made the call), loses it, which equals
// "not applied". WriteGeneration() has no late apply: for this fault it
// applies the switch, then returns TimedOut, as for "TimedOut applied".
// ReadDbRow() and Scan() never get an injected fault, so RocksDB can always
// read the real state after an unknown commit outcome, unless a newer writer
// has fenced it. The status of every injected fault has the text "injected",
// which db_stress uses to tell injected errors from real ones.
//
// Commits without faults: a Commit() that may come from a WAL edit never gets
// an injected fault. Such a commit changes a WAL row, or changes no row other
// than the DB row.
// - Why: with WAL tracking in the MANIFEST and synced writes (--sync=1),
//   RocksDB records the synced WALs with a WAL edit from inside the user
//   write, after the write is in the WAL. When that commit fails, the write
//   returns an error although its records are in the WAL, and the next write
//   reuses its sequence numbers. RocksDB does the same in file mode when the
//   MANIFEST write of a WAL edit fails. The db_stress expected state cannot
//   follow it.
// - Shapes: the WAL edit can share one commit with other edits (for example a
//   trivial move of a compaction). When the WAL rows already hold the synced
//   state, the WAL edit changes only the DB row (for example the last sequence
//   number).
// - Gap: a WAL edit that changes no WAL row can share a commit with an edit
//   that changes file rows. Nothing in that commit marks the WAL edit, so it
//   can still get a fault, and then a synced write fails with its records in
//   the WAL. The estimate (not measured) is that this is rare: it needs a
//   synced write whose WAL rows already hold the synced state, in the same
//   commit as a flush or compaction edit.
// - Cost: flushes and compactions whose commit carries a WAL row change never
//   get a store fault.
class FileManifestStore : public ManifestStore {
 public:
  // The name of the state file in the store dir.
  static constexpr const char* kStateFileName = "STORE";

  // Creates a writer object with a new epoch when `writer` is true, otherwise a
  // reader object. `env` writes the state file. `fault_one_in` (0 = never) sets
  // the faults of this object, which start disabled. `seed` seeds the random
  // choices of all objects of the store dir; only the first object of a store
  // dir in this process uses it. Creates the store dir if it is missing. The
  // process aborts when the store dir cannot be created, when the state file
  // exists and cannot be read, and when a call cannot write the state file,
  // because db_stress cannot go on without its store.
  FileManifestStore(Env* env, const std::string& dir, bool writer,
                    uint64_t fault_one_in, uint32_t seed);
  ~FileManifestStore() override;
  static const char* kClassName() { return "FileManifestStore"; }
  const char* Name() const override { return kClassName(); }
  IOStatus Open(const ManifestStoreOpenOptions& opts,
                ManifestDbRowResult* db_row) override;
  IOStatus Commit(const ManifestCommit& commit) override;
  IOStatus ReadDbRow(ManifestDbRowResult* db_row) override;
  IOStatus Scan(const ManifestScanOptions& opts,
                ManifestRowVisitor* visitor) override;
  IOStatus WriteGeneration(const ManifestGeneration& request,
                           uint64_t* generation) override;
  IOStatus Destroy() override;

  // Turns the faults on or off for this object. db_stress turns them off while
  // DB::Open() runs, and from the start of each reopen until its open ends.
  void SetFaultsEnabled(bool enabled) { faults_enabled_.store(enabled); }

  // The epoch of this object, or std::nullopt for a reader.
  std::optional<uint64_t> epoch() const { return epoch_; }

  // The newest epoch that claimed the DB.
  uint64_t StoredEpoch() const;

  // Removes the store dir with its files and forgets the in-memory state of the
  // store dir. No object of the store dir may exist.
  static Status DestroyStore(Env* env, const std::string& dir);

  // Forgets the in-memory state of the store dir, as a process restart does, so
  // the next object loads the state from the file. No object of the store dir
  // may exist. DestroyStore() calls it, and unit tests call it to simulate a
  // restart.
  static void ForgetState(const std::string& dir);

 private:
  friend class FileManifestStoreTest;

  // Makes the next Open(), Commit() or WriteGeneration() call of this object
  // that can get a fault (see "Faults" above) fail with `fault`, as an injected
  // fault, whether the faults are enabled or not. For unit tests.
  void SetNextFaultForTesting(MockManifestStoreData::Fault fault) {
    forced_fault_ = fault;
  }

  // Runs `call` on the inner store with the lock of the store dir, then writes
  // the state file if the state changed. When `may_fault` is true (Open(),
  // WriteGeneration(), and a Commit() that cannot come from a WAL edit, see
  // "Commits without faults"), it first may set a fault: the fault from
  // SetNextFaultForTesting(), or, when the faults are enabled, a random fault
  // with a chance of 1 in `fault_one_in_`. `is_write_generation` is true for a
  // WriteGeneration() call, which can also get the faults that lose or change
  // a row. Run() marks the returned status as injected when the call used
  // that fault.
  IOStatus Run(bool may_fault, bool is_write_generation,
               const std::function<IOStatus()>& call);

  std::shared_ptr<FileManifestStoreState> state_;
  std::optional<uint64_t> epoch_;
  std::unique_ptr<MockManifestStore> inner_;
  const uint64_t fault_one_in_;
  std::atomic<bool> faults_enabled_{false};
  std::optional<MockManifestStoreData::Fault> forced_fault_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
