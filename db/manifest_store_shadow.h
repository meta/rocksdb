//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/manifest_rows.h"
#include "db/manifest_store_state.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/manifest_store.h"

namespace ROCKSDB_NAMESPACE {

class Logger;
class SystemClock;

// Shadow mode (DBOptions::manifest_store_shadow): the MANIFEST file is the
// authority, and this class copies the MANIFEST state to the ManifestStore in
// the background, so that the store can be checked before a DB moves into it.
// The option comment holds the user-facing rules; this comment holds the
// design.
//
// What the store gets: the rows (db/manifest_rows.h) of the state that
// recovery from the MANIFEST file gives, in the canonical public encoding of
// include/rocksdb/manifest_store.h. The DB row has authority kShadow, copy mode
// kOff, copy file number 0 and in-flight file number 0 (keep every file).
//
// Write side: VersionSet::ProcessManifestWrites() calls ReplaceWithSnapshot()
// and Apply() after each successful MANIFEST write, without the DB mutex, from
// one thread at a time (see VersionSet::HandOffToManifestStoreShadow()):
// - A new MANIFEST file starts with a snapshot of the in-memory state.
//   ReplaceWithSnapshot() takes the rows of that snapshot, so the rows follow
//   the file exactly. Only the rows that differ are committed. It also logs
//   each difference that is not a known in-memory difference (see
//   ReplaceWithSnapshot()).
// - Apply() takes the row changes of each MANIFEST write.
//
// Store side: the store calls run as jobs in the Env LOW priority pool. Each
// job makes one round of store calls, a full copy or a commit (see below). The
// jobs use the LOW pool, because store calls are background I/O like
// compactions. The HIGH pool runs flushes, which unblock writes. The BOTTOM
// pool often has no threads. After a successful round, a job asks for the
// next job when work is left (see "Job slots"). The two kinds of round are:
// - Full copy: the first round after Start() and the first round after a
//   store error. It writes all rows as a new store generation.
//   1. ReadDbRow(). If the DB row does not parse, has authority kStore (a DB
//      in store mode) or an authority value above kStore (unknown here, so a
//      newer binary owns the DB), or has another DB id (another DB uses the
//      same store key), shadow mode stops and writes nothing.
//   2. Open() claims the DB, with no DB row field updates. If the store has no
//      DB row, a Commit() with expected commit sequence 0 creates it instead.
//   3. If the claimed DB row meets any condition of step 1, shadow mode stops
//      and writes nothing more.
//   4. WriteGeneration() with the commit sequence of the claim. A store-mode
//      instance that claims the DB after step 3 raises the commit sequence, so
//      this step gets Busy and writes nothing; the next full copy stops at
//      step 1.
// - Commit: the rows that changed since the last round (see `changed_`), as
//   one Commit(). Many MANIFEST writes that happen during one store call
//   become one commit.
//
// In sync: the store is in sync when shadow mode started and did not stop,
// its last round succeeded, and no full copy is needed. The store then holds
// the rows as of that round, and the changed keys since that round are
// exactly what the next commit, or the close commit, must send. The info LOG
// at close and TEST_InSync() use this meaning.
//
// Job slots: a DB has at most one job at a time, and all shadow DBs of the
// process together run at most kMaxJobsPerProcess (1) jobs at a time. The
// limit is per process, not per Env, because Env objects often share one
// thread pool (for example, every Env from NewCompositeEnv() uses the pools of
// Env::Default()). Each DBImpl makes sure that the LOW pool of its Env has
// kMaxJobsPerProcess threads more than its compaction limit, so a slow store
// call does not take a compaction thread. Shadow mode is a step to check a
// store before a DB moves into it, so one job at a time is enough. A DB that
// needs a job while the slot is taken joins a first-in, first-out queue of
// waiting DBs. A job that ends gives the slot to the first DB in the queue
// (see ShadowJobSlots in the .cc file for the states and the hand-off). When
// its round succeeded and work is left, its DB joins the back of the queue,
// so every waiting DB gets a turn. After a failed round, the DB asks again at
// the first Apply() after the retry wait (see "Errors").
//
// State: the rows are a map from row key to a shared pointer to the encoded
// row. A full copy keeps a reference to the current row map by copying one
// shared pointer. The thread that calls ReplaceWithSnapshot() and Apply() (the
// writer) then copies the map of pointers (not the row bytes) once, outside
// the mutex, and changes its own copy in place until the next full copy. A
// commit round copies only the pointers of the changed rows.
//
// Lifecycle:
// 1. DBImpl creates the object before recovery. No store call runs yet.
// 2. The first MANIFEST write calls ReplaceWithSnapshot() and Apply(). From
//    here on the rows are complete (`has_rows_`).
// 3. Start(), after DB::Open() succeeds, allows jobs. The first job is a full
//    copy.
// 4. Commit rounds follow while `need_full_copy_` is false. A store error sets
//    `need_full_copy_` and a wait before the next full copy.
// 5. Shadow mode stops for good when the store says that another instance
//    owns it, or when the DB row is one it must not write (see "Errors").
//    No store call runs after that.
// 6. Close() sets `closing_`, waits for the job, and sends the close commit
//    only when the store is in sync.
//
// Errors: no store error fails a user write or a MANIFEST write, and shadow
// mode never sets a background error. The MANIFEST file is the authority, so a
// store error says nothing about the DB files.
// - IOFenced: another DB instance claimed the store with a newer epoch, for
//   example a copy of this DB folder (which has the same DB id) opened with
//   the same store key. Shadow mode stops and logs an error.
// - The DB row does not parse, has authority kStore or an unknown authority,
//   or has another DB id: shadow mode stops. Writes go on.
// - Every other error: Busy and TryAgain mean that the call changed nothing,
//   and for other errors the outcome is not known. Either way the store may
//   not hold the rows that this DB expects, so the next round is a full copy.
//   It runs after a wait of `retry_interval_us`
//   (DBOptions::bgerror_resume_retry_interval), doubled for each failure in a
//   row, up to 64 times `retry_interval_us` (kMaxRetryWaitShift, 6
//   doublings). The cap keeps the wait about a minute with the default
//   interval of one second. The full copy starts at the first Apply() after
//   the wait.
//
// Close ids and the enter: the close commit has a close commit id
// (ManifestCommitIdGenerator::NextCloseId()). An enter into store mode takes
// over kShadow rows only when the last commit has such an id. A close id marks
// the last close of a shadow session that reached the store. A shadow DB that
// is open again makes its first store call only after its first MANIFEST
// write, the end of DB::Open() and its turn at the job slot. Until then the
// store still holds the close id of the previous session. So the enter cannot
// see every open shadow DB, and users must close every shadow DB of the DB
// (also copies of its folder) before an enter.
//
// Threads and locks: `mutex_` of a DB is taken before the mutex of the job
// slots, never after it. The DB mutex is never taken while `mutex_` is held.
// Close() must be called without the DB mutex.
class ManifestStoreShadow {
 public:
  // The most shadow jobs that run at the same time for all DBs of the process,
  // and the number of LOW pool threads that DBImpl adds for them.
  static constexpr int kMaxJobsPerProcess = 1;

  ManifestStoreShadow(std::shared_ptr<ManifestStore> store, Env* env,
                      SystemClock* clock, std::shared_ptr<Logger> info_log,
                      uint32_t feature_version, uint64_t retry_interval_us,
                      const std::string& db_session_id);

  // REQUIRES: Close() was called, or Start() was never called.
  ~ManifestStoreShadow();

  ManifestStoreShadow(const ManifestStoreShadow&) = delete;
  ManifestStoreShadow& operator=(const ManifestStoreShadow&) = delete;

  // True after shadow mode stopped (see "Errors" above). The write side then
  // skips all shadow work. Once true, it stays true.
  bool Stopped() const { return stopped_.load(std::memory_order_acquire); }

  // The DB row fields of shadow mode, and the row feature version.
  const ManifestDbRowExtraFields& extra_fields() const { return extra_fields_; }
  uint32_t feature_version() const { return feature_version_; }

  // Replaces all rows other than the DB row with `rows`, the rows of the
  // snapshot at the start of a new MANIFEST file (from
  // ManifestRowsToPublicRows(), so the DB row is first). The keys whose rows
  // differ become changed keys. The DB row of the snapshot is not used: it
  // lacks the values that the edits of the same MANIFEST write record (for
  // example the next file number), and the Apply() that follows every
  // ReplaceWithSnapshot() for the same MANIFEST write sets the whole DB row.
  //
  // Before it replaces the rows, it compares them with the old rows, which
  // describe the same state as recorded by the old MANIFEST file. These known
  // differences are expected: the log number of a column family and
  // marked_for_compaction of a file (see "In-memory differences" at
  // BuildManifestRowChanges()), the rows of a dropped column family (a new
  // MANIFEST file leaves out dropped column families), and WAL rows that only
  // the old rows have (a new MANIFEST file drops the WALs below the min log
  // number to keep). Each other difference is logged as a warning with its row
  // key, because it means that the row changes of some earlier MANIFEST write
  // were wrong. The DB row is not compared.
  void ReplaceWithSnapshot(std::vector<ManifestRow>&& rows);

  // Applies the row changes of one MANIFEST write: the DB row, the upserts
  // and the deletes of `changes` (from ManifestRowChangesToCommit(); the commit
  // sequence and the commit id are not used). Then asks for a job if none is
  // scheduled.
  void Apply(ManifestCommit&& changes);

  // Allows store calls. DB::Open() calls it after the open succeeds, so a
  // failed open never calls the store. `db_id` is the id of the DB; a store
  // whose DB row has another DB id is never claimed. Asks for a job if rows
  // are waiting.
  void Start(const std::string& db_id);

  // Stops shadow mode for close: leaves the queue of the job slots, removes a
  // queued job, waits for a running job (which stops after its current store
  // call and starts no new full copy), then sends the remaining changes as one
  // Commit() with a close commit id (ManifestCommitIdGenerator::NextCloseId()),
  // but only when the store is in sync (see "In sync" above). This final
  // commit runs in the calling thread. After a successful Start(), it logs
  // whether the store was in sync at close; it logs nothing when shadow mode
  // never started or already stopped. It must be called after the last MANIFEST
  // write, without the DB mutex.
  void Close();

  // Test helpers.
  // Waits until no job is scheduled or running, and this DB does not wait for
  // the job slot.
  void TEST_WaitForIdle();

  // True when the last round succeeded and no full copy is needed (see "In
  // sync" above, without the started check).
  bool TEST_InSync();

  // The number of entries in `changed_`.
  size_t TEST_NumChangedKeys();

 private:
  using EncodedRow = std::shared_ptr<const std::string>;
  using RowMap = std::map<ManifestRowKey, EncodedRow, ManifestRowKeyLess>;

  // One changed row of a commit round: the encoded row, or null when the row
  // is deleted, and whether the store has the row.
  struct ChangedRow {
    ManifestRowKey key;
    EncodedRow row;
    bool store_has_row = false;
  };

  static void BGWork(void* arg);

  // Delivers the job slot that a job gave to `shadow`, the first waiting DB
  // (see "Job slots"), and then to the next DB when `shadow` has no work.
  // Called without any shadow mutex held.
  static void GiveSlotTo(ManifestStoreShadow* shadow);

  // Runs one round. Called by BGWork().
  void RunJob();

  // True when store calls are allowed, there is work, no job is scheduled and
  // the retry wait has passed. REQUIRES: mutex_ held.
  bool NeedsJobLocked() const;

  // Schedules a job when NeedsJobLocked() and the job slot is free. Otherwise
  // a DB that needs a job joins the queue of the job slots. REQUIRES: mutex_
  // held.
  void MaybeScheduleLocked();

  // Takes the job slot that GiveSlotTo() delivers: schedules a job when
  // NeedsJobLocked(), otherwise gives the slot back. Returns the DB that gets
  // the slot next, or null. REQUIRES: mutex_ not held.
  ManifestStoreShadow* TakeGivenSlot();

  // Clears `job_scheduled_` and gives back the job slot. Returns the waiting
  // DB that now holds the slot, or null; the caller passes it to GiveSlotTo()
  // after it releases mutex_. REQUIRES: mutex_ held.
  ManifestStoreShadow* ReleaseJobSlotLocked();

  // Makes `rows_` a map that only the writer uses, so the writer can change it
  // in place. Returns with mutex_ held. REQUIRES: mutex_ not held.
  void OwnRowsAndLock();

  // Records a change of `key` in `changed_` (see the rules there). `had_row`
  // says whether the rows had the key before the change, and `has_row`
  // whether they have it after the change. REQUIRES: mutex_ held.
  void MarkChangedLocked(const ManifestRowKey& key, bool had_row, bool has_row);

  // Logs each unexpected difference between `old_rows` and `new_rows` (see
  // ReplaceWithSnapshot()).
  void LogUnexpectedDifferences(const RowMap& old_rows, const RowMap& new_rows);

  // Steps 1 to 4 of a full copy (see the class comment). On OK with an empty
  // `*stop_reason`, sets `*commit_sequence` to the commit sequence after the
  // generation switch. Sets `*stop_reason` and returns OK without writing rows
  // when shadow mode must not write the store (steps 1 and 3); a claim in step
  // 2 may have raised the commit sequence.
  IOStatus FullCopy(const RowMap& rows, const std::string& db_row,
                    uint64_t* commit_sequence, std::string* stop_reason);

  // Returns true if mutex_-protected `closing_` is set. Takes the mutex.
  bool IsClosing();

  // Moves the changed keys out of `changed_`, each with its current row.
  // REQUIRES: mutex_ held.
  std::vector<ChangedRow> TakeChangedRowsLocked();

  // Builds the commit of one round. `changed` holds the changed rows.
  ManifestCommit MakeCommit(uint64_t expected_commit_sequence,
                            std::string commit_id, const std::string& db_row,
                            const std::vector<ChangedRow>& changed) const;

  // Records a failed round: sets need_full_copy_, counts the failure, sets the
  // time of the next full copy and logs it. REQUIRES: mutex_ held.
  void RecordFailureLocked(const char* step, const IOStatus& s);

  const std::shared_ptr<ManifestStore> store_;
  Env* const env_;
  SystemClock* const clock_;
  const std::shared_ptr<Logger> info_log_;
  const uint32_t feature_version_;
  const uint64_t retry_interval_us_;
  ManifestDbRowExtraFields extra_fields_;
  ManifestCommitIdGenerator commit_ids_;

  std::atomic<bool> stopped_{false};
  // The DB id, set by Start() before the first job is scheduled.
  std::string db_id_;

  port::Mutex mutex_;
  port::CondVar cv_;

  // Guarded by mutex_. The writer changes the map only while `writer_owns_`
  // is true; a full copy sets it to false when it takes a reference to the map
  // (see OwnRowsAndLock()).
  std::shared_ptr<RowMap> rows_;
  bool writer_owns_ = true;
  std::string db_row_;
  bool db_row_changed_ = false;
  // The changed keys since the last round. The value says whether the store
  // has the row as of the last round, so that a commit sends a delete only for
  // a row that the store has. MarkChangedLocked() keeps these rules:
  // - A key without an entry has the same row in `rows_` and in the store.
  //   So its first change adds an entry whose value is whether `rows_` had
  //   the row before the change.
  // - Later changes keep the value, because it describes the store, which
  //   does not change before the next round.
  // - An entry with value false whose row is deleted again is erased: the
  //   store never had the row, so there is nothing to send.
  // Example: file 12 is added (entry false) and compacted away (entry erased)
  // before the next round, so the commit sends nothing for it. File 7, which
  // the store has, is deleted (entry true), so the commit deletes it. Hence
  // the map has at most one entry for each row of `rows_` and each row of the
  // store, also when the DB waits long for the job slot.
  std::map<ManifestRowKey, bool, ManifestRowKeyLess> changed_;
  // True after the first Apply(), which follows the first
  // ReplaceWithSnapshot().
  bool has_rows_ = false;

  bool started_ = false;
  bool closing_ = false;
  // True while a job is queued in the Env or running. The DB holds the job
  // slot while this is true, and also for a short time while a slot is in
  // delivery to it (see ShadowJobSlots). Close() and TEST_WaitForIdle() wait
  // until it is false.
  bool job_scheduled_ = false;
  // True until a full copy succeeds, and again after each store error.
  bool need_full_copy_ = true;
  // The commit sequence that the next commit expects.
  uint64_t commit_sequence_ = 0;
  // Store errors in a row, and the time before which no full copy starts.
  uint32_t failures_ = 0;
  uint64_t retry_after_us_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
