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
// design. Epoch, claim, commit sequence and generation are defined in "Terms"
// in include/rocksdb/manifest_store.h.
//
// Data flow. VersionSet::ProcessManifestWrites() hooks into shadow mode in
// three places; the comment before its `shadow_active` variable in
// db/version_set.cc lists them. The last one hands the write over:
//
//   VersionSet::HandOffToManifestStoreShadow(), called by
//   ProcessManifestWrites() after the MANIFEST write (one MANIFEST writer at
//   a time, with the DB mutex released)
//     |                          |
//     | only when the write      | every successful write:
//     | starts a new MANIFEST    | the row changes of the write
//     | file: the snapshot rows  |
//     v                          |
//   ReplaceWithSnapshot() (1)    |
//     |                          |
//     v                          |
//   Apply() (2) <----------------+
//     |
//     v
//   MaybeScheduleLocked()  -->  ShadowJobSlots (one job slot per process)
//     |
//     v
//   RunJob() in the LOW pool:  full copy (WriteGeneration()) or commit
//                              (Commit())
//   Close():                   close commit (Commit() with a close id),
//                              only when the store is in sync (see "In sync")
//
//   (1) Replaces the rows other than the DB row in `rows_` and marks the rows
//       that differ in `changed_`. `db_row_` stays as it was.
//   (2) Applies the row changes to `rows_` and `changed_`, and sets `db_row_`.
//
// After the DB is closed, CompareManifestStoreWithManifest() (the ldb command
// compare_manifest_store) recovers the rows from the MANIFEST file and lists
// the differences between the store rows and the recovered rows. To make the
// two sides comparable, it lowers the recovered next file number by 1, skips
// the store rows of dropped column families, and takes the extra DB row fields
// (such as the authority) from the store. Its comment in db/manifest_rows.h
// says why.
//
// What the store gets: the rows (db/manifest_rows.h) of the state that
// recovery from the MANIFEST file gives, in the canonical public encoding of
// include/rocksdb/manifest_store.h. The DB row has authority kShadow, copy mode
// kOff, copy file number 0 and in-flight file number 0 (keep every file).
//
// Write side: after each successful MANIFEST write,
// VersionSet::ProcessManifestWrites() hands the write over without the DB
// mutex, from one thread at a time (see
// VersionSet::HandOffToManifestStoreShadow()). It builds the row changes of
// the write from its decoded base rows (VersionSet::shadow_base_rows_) and
// calls the methods of this class below. It also applies the row changes to
// the base rows, which it keeps for the next write. The methods:
// - Only when the write starts a new MANIFEST file: the new file starts with a
//   snapshot of the state before the edits of the write.
//   ReplaceWithSnapshot() takes the rows of that snapshot, so the rows follow
//   the file exactly. Only the rows that differ are committed. It also logs
//   each difference that is not a known in-memory difference (see
//   ReplaceWithSnapshot()).
// - For every successful write: Apply() takes the row changes of the write
//   and the new DB row. An ordinary append calls only Apply().
// The two calls take the mutex one after the other, so a job that is already
// scheduled can run between them. For example (example file numbers), a
// compaction replaces file 15 with file 16, and its MANIFEST write starts a new
// MANIFEST file:
//
//   step                   rows other than the DB row    DB row
//   ReplaceWithSnapshot()  the snapshot, with file 15    of the previous write
//   a job runs here        commits the snapshot rows     commits the previous
//                                                        DB row
//   Apply()                file 16 replaces file 15      the new DB row
//   next round             commits the delete of file    commits the new DB
//                          15 and the add of file 16     row
//
// The snapshot is the state before the edits of the write, so it still has
// file 15. A job that runs between the two calls commits that state with the
// DB row of the previous MANIFEST write. The next round, or the close commit
// when the store is in sync, sends the changes of Apply().
//
// The row changes of a MANIFEST write are built from the rows before the
// write (the base rows, VersionSet::shadow_base_rows_). Only the snapshot of a
// new MANIFEST file gives the full state, so the first MANIFEST write in
// shadow mode starts a new MANIFEST file, and so does the first MANIFEST write
// after a failed one (see VersionSet::shadow_base_rows_).
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
//   2. Open() claims the DB, with no DB row field updates, and with the
//      commit sequence and the last commit id that step 1 read as the
//      precondition. If another instance claimed the DB after step 1, the
//      claim changes nothing. It gets IOFenced when that instance has a newer
//      epoch, and shadow mode stops (see "Errors"). Else it gets Busy, and the
//      round fails (the next round is a full copy again). "Order of the
//      checks" in include/rocksdb/manifest_store.h has the order of these
//      checks. If the store has no DB row, a Commit() with expected commit
//      sequence 0 creates it instead.
//   3. If the claimed DB row meets any condition of step 1, shadow mode stops
//      and writes nothing more.
//   4. WriteGeneration() with the commit sequence of the claim. A store-mode
//      instance that claims the DB after step 3 raises the commit sequence, so
//      this step writes nothing. It gets IOFenced when that instance has a
//      newer epoch, and shadow mode stops. Else it gets Busy, and the round
//      fails. The next full copy (after the retry wait, at a later MANIFEST
//      write) runs the checks of step 1 again: it stops when the DB row has
//      kStore (an enter switched), and it can claim again when the row still
//      has kFile or kShadow.
//   Where a store-mode instance (for example an enter, see "Close ids and the
//   enter") can step in:
//
//     ReadDbRow() ---------> Open() (claim) ---------> WriteGeneration()
//                  window 1                  window 2
//
//   - Window 1: the instance changes the DB row, so the claim of step 2
//     changes nothing and never fences that instance. With a newer epoch, the
//     claim gets IOFenced and shadow mode stops. Else it gets Busy: if the row
//     then has kStore (the switch of an enter), the next full copy stops at
//     step 1, and if it has kFile (an enter that did not switch yet), see the
//     last bullet.
//   - Window 2: step 4 gets Busy, or IOFenced for a newer epoch, and writes
//     nothing.
//   - An enter that claimed the DB but did not switch yet leaves authority
//     kFile, so step 3 does not stop. The full copy writes kShadow, and the
//     enter fails (see "Close ids and the enter").
// - Commit: the rows that changed since the last round (see `changed_`), as
//   one Commit(). Many MANIFEST writes that happen during one store call
//   become one commit.
//
// In sync: "in sync" names the condition under which Close() can send the
// remaining changes with a close id. It does not mean that the store has no
// pending changes. The store is in sync when Start() has run, the first
// MANIFEST write has happened (`has_rows_`), shadow mode did not stop, the last
// round succeeded, no full copy is needed, and no failed MANIFEST write may be
// in the MANIFEST file without being in the rows (`manifest_ahead_`)
// (InSyncLocked()). The store then holds the rows as of that round, and the
// changed keys since that round are exactly what the next commit, or the close
// commit, must send. Close() and TEST_InSync() use this check.
//
// A successful close commit marks the state that its shadow instance sent. It
// does not show that the store matches the MANIFEST file later: a later
// instance can write the MANIFEST file while the close id stays in the store
// (see "Close ids and the enter"). So an enter checks only that the last commit
// has a close id, and then builds all rows from the MANIFEST file.
//
// Job slots: a DB has at most one job at a time, and all shadow DBs of the
// process together run at most kMaxJobsPerProcess (1) jobs at a time in the Env
// LOW pool. Shadow mode is a step to check a store before a DB moves into it,
// so one job at a time is enough. The slot limits only these jobs. Close()
// removes a queued job of its DB or waits for a running one, so its DB then
// holds no slot. Then, when the store is in sync, it sends the close commit on
// the calling thread, so that call can run at the same time as a job of another
// DB. ShadowJobSlots in the .cc file holds the full design: why the limit is
// per process, the queue of waiting DBs, the states, the hand-off and the
// methods that change who holds a slot.
//
// State: the rows are a map from row key to a shared pointer to the encoded
// row. A full copy keeps a reference to the current row map by copying one
// shared pointer. The thread that calls ReplaceWithSnapshot() and Apply() (the
// writer) then copies the map of pointers (not the row bytes) once, outside the
// mutex, and changes its own copy in place until the next full copy. When the
// full copy builds ManifestGeneration::rows for WriteGeneration(), it copies
// the encoded row bytes into the request. A commit round copies only the
// pointers of the changed rows. A MANIFEST write that starts a new MANIFEST
// file also holds its snapshot rows for a short time, in decoded form (a copy
// of the file metadata, including the smallest and largest keys) and in encoded
// form.
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
//    owns it, when the store does not support a call, or when the DB row is
//    one it must not write (see "Errors"). No store call runs after that.
// 6. Close() sets `closing_`, waits for the job, and sends the close commit
//    only when the store is in sync.
//
// Errors: no store error fails a user write or a MANIFEST write, and shadow
// mode never sets a background error. The MANIFEST file is the authority, so a
// store error says nothing about the DB files.
// - IOFenced: another DB instance claimed the store with a newer epoch, for
//   example a copy of this DB folder (which has the same DB id) opened with
//   the same store key. Shadow mode stops and logs an error.
// - NotSupported: the store object cannot do the call, for example because
//   it has no epoch, so a retry gets the same answer. Shadow mode stops and
//   logs an error.
// - The DB row does not parse, has authority kStore or an unknown authority,
//   or has another DB id: shadow mode stops. Writes go on.
// - Every other error: Busy and TryAgain mean that the call changed nothing,
//   and for other errors the outcome is not known. Either way the store may not
//   hold the rows that this DB expects, so the next round is a full copy. It
//   runs after a wait. The wait is `retry_interval_us`
//   (DBOptions::bgerror_resume_retry_interval) after the first failure. It
//   doubles after each further failure in a row, up to 64 times
//   `retry_interval_us` (kMaxRetryWaitShift, 6 doublings, reached at the 7th
//   failure in a row). The cap keeps the wait about a minute with the default
//   interval of one second. The full copy starts at the first Apply() after the
//   wait. These retries have no limit, so a store that keeps failing gets a
//   full copy about once a minute, once the wait reaches its cap, for as long
//   as the DB writes. Each full copy claims the store (which raises the commit
//   sequence), sends all rows, and holds the job slot while it runs.
//
// Close ids and the enter: an enter is DB::Open() with
// DBOptions::manifest_store_migrate, which moves a DB in file mode into store
// mode (see "Mode changes" in db/manifest_store_state.h). The close commit has
// a close id (ManifestCommitIdGenerator::NextCloseId()). An enter takes over
// kShadow rows only when the last commit has such an id. A close id marks the
// last close of a shadow instance (one DB::Open() to close of a DB in shadow
// mode; its commit ids start with its DB session id) that reached the store. It
// does not show a later shadow instance that never changed the store. A shadow
// DB that is open again makes its first store call only after its first
// MANIFEST write, the end of DB::Open() and its turn at the job slot. Until
// then the store still holds the close id of the previous shadow instance. For
// example (example values):
//
//   Instance S1: opens, writes, closes in sync  store: last id "S1-7-close"
//   Instance S2: opens and writes the MANIFEST  store: still "S1-7-close",
//     file, then closes or crashes before its     but behind the MANIFEST
//     first claim, or its first claim fails       file
//     without applying
//   Enter: accepts the rows. This is safe, because the enter writes all rows
//     from the MANIFEST file.
//   CompareManifestStoreWithManifest(): lists the differences, with a warning
//     that the next file number of the store is below that of the MANIFEST.
//
// So the enter cannot see every open shadow DB, and users must close every
// shadow DB of the DB (also copies of its folder) before an enter.
//
// A shadow instance that reaches the store during an enter fails that enter.
// The claim of the enter writes authority kFile, and only its switch
// (WriteGeneration()) writes kStore. A full copy accepts a kFile DB row, so it
// can claim the DB and write kShadow between the claim and the switch of the
// enter, or after an enter that stopped before its switch:
//
//   Enter: Open() (claim, kFile) ------------------> WriteGeneration() (kStore)
//   Full copy:              Open() (claim), WriteGeneration() (kShadow)
//
// The switch then writes nothing. It gets Busy, because the claim of the full
// copy raised the commit sequence, or IOFenced when the full copy has a newer
// epoch. CURRENT keeps the "entering" stub. The next store-mode open
// (DBImpl::FinishEnterManifestStore()) and RollBackManifestStore() then act on
// the DB row next to that stub:
// - kStore: the switch happened. The open finishes the enter, and
//   RollBackManifestStore() returns InvalidArgument (open the DB first).
// - kFile: the switch did not happen, so CURRENT points back to the MANIFEST
//   file.
// - kShadow: the same as kFile. A switch always writes kStore, and shadow mode
//   never overwrites kStore, so kShadow means that the switch did not happen.
//   After that, a new enter returns InvalidArgument while the last commit has
//   no close id.
//
// For a kShadow row, both paths point CURRENT back to the MANIFEST file, but
// only the rollback claims the DB row. A read-write open
// (FinishEnterManifestStore()) does not claim a kShadow row and restores
// CURRENT on local files only, and the new enter that follows in the same open
// makes the claim. So the close id of a shadow instance that closed in sync
// stays, and that enter accepts the rows. RollBackManifestStore() claims the
// row first, to fence the earlier writer, also when it only restores CURRENT.
// The claim replaces the close id, so an enter after a rollback needs a shadow
// instance that closes in sync again.
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
  // skips all shadow work. Once true, it stays true. A stop frees the rows, the
  // changed keys and the DB row, because nothing uses them again.
  bool Stopped() const { return stopped_.load(std::memory_order_acquire); }

  // The DB row fields of shadow mode, and the row feature version.
  const ManifestDbRowExtraFields& extra_fields() const { return extra_fields_; }
  uint32_t feature_version() const { return feature_version_; }

  // Replaces all rows other than the DB row with `rows`, the rows of the
  // snapshot at the start of a new MANIFEST file (from
  // ManifestRowsToPublicRows(), so the DB row is first). The keys whose rows
  // differ become changed keys. The DB row of `rows` is not compared or stored
  // here: it lacks the values that the edits of the same MANIFEST write record
  // (for example the next file number), and the Apply() that follows every
  // ReplaceWithSnapshot() for the same MANIFEST write sets the whole DB row.
  //
  // Before it replaces the rows, it compares them with the old rows, which
  // describe the same state as recorded by the old MANIFEST file. The compare
  // skips these cases (see KnownDifferentFields() and
  // LogUnexpectedDifferences()):
  // - the log number field of every column family row, and the
  //   marked_for_compaction field of every file row. These can differ from
  //   memory (see "In-memory differences" in the overview of
  //   db/manifest_rows.h; for the log number, an empty column family is the
  //   known case);
  // - the rows of a dropped column family that only the old rows have (a new
  //   MANIFEST file leaves out dropped column families);
  // - every WAL row that only the old rows have. A new MANIFEST file drops the
  //   WALs below the min log number to keep, and the compare does not check
  //   that a missing WAL row is below it.
  // Each other difference is logged as a warning with its row key. It usually
  // means that the row changes of some earlier MANIFEST write were wrong. A
  // wrong value in a skipped field gives no warning.
  void ReplaceWithSnapshot(std::vector<ManifestRow>&& rows);

  // Applies the row changes of one MANIFEST write: the DB row, the upserts
  // and the deletes of `changes` (from ManifestRowChangesToCommit(); the commit
  // sequence and the commit id are not used). Then asks for a job if none is
  // scheduled.
  void Apply(ManifestCommit&& changes);

  // Marks that a MANIFEST write failed, or that its row changes could not be
  // built. The MANIFEST file may hold that write while the rows do not, so the
  // store is not in sync (see "In sync" above) until the next
  // ReplaceWithSnapshot(). The next successful MANIFEST write starts a new
  // MANIFEST file and calls ReplaceWithSnapshot().
  void MarkManifestAhead();

  // Allows store calls. DB::Open() calls it after the open succeeds, so a
  // failed open never calls the store. `db_id` is the id of the DB; a store
  // whose DB row has another DB id is never claimed. Asks for a job if rows
  // are waiting.
  void Start(const std::string& db_id);

  // Stops shadow mode for close: leaves the queue of the job slots, removes a
  // queued job, and waits for a running job. A running job starts no new full
  // copy after close starts, but it can make more than one store call: a full
  // copy that reads no DB row creates it with a Commit() before its next
  // closing check. When that check sees the close, the full copy stops before
  // WriteGeneration(). The store then has a kShadow DB row without the other
  // rows, and a full copy is still needed, so Close() sends no close commit. A
  // later enter that finds this kShadow row without a close id returns
  // InvalidArgument (see "Close ids and the enter"). Otherwise Close() sends
  // the remaining changes as one Commit() with a close id
  // (ManifestCommitIdGenerator::NextCloseId()), but only when the store is in
  // sync (see "In sync" above). This final commit runs in the calling thread.
  // After Start(), it logs whether the store was in sync at close; it logs
  // nothing when shadow mode never started or already stopped. It must be
  // called after the last MANIFEST write, without the DB mutex.
  void Close();

 private:
  // Test helpers, for the wrappers in DBManifestStoreShadowTest.
  friend class DBManifestStoreShadowTest;

  // Waits until no job is scheduled or running, and this DB does not wait for
  // the job slot.
  void TEST_WaitForIdle();

  // Returns InSyncLocked() (see "In sync" above).
  bool TEST_InSync();

  // The number of entries in `changed_`.
  size_t TEST_NumChangedKeys();

  // The number of rows in `rows_`, without the DB row.
  size_t TEST_NumRows();

  using EncodedRow = std::shared_ptr<const std::string>;
  using RowMap = std::map<ManifestRowKey, EncodedRow, ManifestRowKeyLess>;
  // See `changed_`.
  using ChangedMap = std::map<ManifestRowKey, bool, ManifestRowKeyLess>;

  // One changed row of a commit round: the encoded row, or null when the row is
  // deleted, and whether the state that the previous round sent has the row.
  // That previous round can still be in its store call. It is never a failed
  // round: after a failed round that does not stop shadow mode, the next round
  // is a full copy, not a commit. IOFenced, NotSupported and a DB row that
  // shadow mode must not write (ReasonNotToWrite()) stop shadow mode.
  struct ChangedRow {
    ManifestRowKey key;
    EncodedRow row;
    bool previous_round_had_row = false;
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

  // True when the store is in sync (see "In sync" above). REQUIRES: mutex_
  // held.
  bool InSyncLocked() const;

  // Replaces rows_ with an empty map and clears `changed_` and `db_row_`, so a
  // stopped shadow mode keeps no rows. Returns the old map, which the caller
  // frees after it releases mutex_. REQUIRES: mutex_ held.
  std::shared_ptr<RowMap> FreeRowsLocked();

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
  // ReplaceWithSnapshot()). `changed` holds the keys whose rows differ, so
  // only these keys are compared.
  void LogUnexpectedDifferences(const RowMap& old_rows, const RowMap& new_rows,
                                const std::vector<ManifestRowKey>& changed);

  // Steps 1 to 4 of a full copy (see "Store side" above). On OK with an empty
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
  // The changed keys since the last round. A job clears the map when it takes
  // its rows for a store call (TakeChangedRowsLocked(), or the full copy
  // branch of RunJob()). From then on, MANIFEST writes record their changes
  // against the state that this job sends, also while its store call runs.
  // The value says whether that state has the row, so that a commit sends a
  // delete only for a row that the store has. For example, if a job in
  // progress sends file 15 and a later MANIFEST write deletes it, the next
  // commit sends the delete. If the store call fails and shadow mode goes on,
  // the next round is a full copy (see RecordFailureLocked()), so the map does
  // not need to describe a failed round. MarkChangedLocked() keeps these
  // rules:
  // - A key without an entry has the same row in `rows_` and in the state
  //   that the last round sent. So its first change adds an entry whose value
  //   is whether `rows_` had the row before the change.
  // - Later changes keep the value, because it describes the state that the
  //   last round sent, which does not change before the next round.
  // - An entry with value false whose row is deleted again is erased: the
  //   store never had the row, so there is nothing to send.
  // Example: file 12 is added (entry false) and compacted away (entry erased)
  // before the next round, so the commit sends nothing for it. File 7, which
  // the store has, is deleted (entry true), so the commit deletes it. Hence
  // the map has at most one entry for each row of `rows_` and each row of the
  // store, also when the DB waits long for the job slot.
  ChangedMap changed_;
  // True after the first Apply(), which follows the first
  // ReplaceWithSnapshot().
  bool has_rows_ = false;
  // True after a failed MANIFEST write (MarkManifestAhead()), until the next
  // ReplaceWithSnapshot().
  bool manifest_ahead_ = false;

  bool started_ = false;
  bool closing_ = false;
  // True while a job is queued in the Env or running. The DB holds the job
  // slot while this is true, and also for a short time while a slot is in
  // delivery to it (see "Who holds a slot" at ShadowJobSlots). Close() and
  // TEST_WaitForIdle() wait until it is false.
  bool job_scheduled_ = false;
  // True until a full copy succeeds. A failed round and a full copy stopped by
  // Close() set it again, so !need_full_copy_ also means that the last round
  // succeeded (see InSyncLocked()).
  bool need_full_copy_ = true;
  // The commit sequence that the next commit expects.
  uint64_t commit_sequence_ = 0;
  // Store errors in a row, and the time before which no full copy starts.
  uint32_t failures_ = 0;
  uint64_t retry_after_us_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
