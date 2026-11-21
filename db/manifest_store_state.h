//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/manifest_rows.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/manifest_store.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

class SystemClock;
struct ImmutableDBOptions;

// Store mode: overview
//
// Background: in store mode the rows in a ManifestStore
// (include/rocksdb/manifest_store.h, DBOptions::manifest_store) are the
// authority for the metadata of a DB, instead of the MANIFEST file. The rows
// are the internal rows of db/manifest_rows.h in their public form.
//
// Files in the DB folder in store mode:
// - CURRENT names a stub MANIFEST (`MANIFEST-<number>`). The stub has one
//   record, which starts with the tag kManifestStoreStub. That tag is not
//   safe to ignore, so a reader that does not know store mode (an older
//   binary, or ldb) fails instead of reading an empty DB. New code decodes it
//   as NotSupported (VersionEdit::DecodeFrom()). RepairDB() and DestroyDB()
//   check for a stub and return NotSupported.
// - The stub records the db id and a state (ManifestStubState): done, or a
//   mode change that is not finished yet (creating a new DB in the store, or
//   entering store mode from file mode).
// - RocksDB never rewrites a stub in place. It writes a new stub with a new
//   number, points CURRENT to it, then deletes the old one. This is a stub
//   replacement.
//
// Stub layout (EncodeManifestStub()): the stub file holds one log record:
//
//   +------------------------+------------------------------------------+
//   | tag kManifestStoreStub | length-prefixed payload: payload version,|
//   | (varint32, 500)        | state, db id, and for kEntering also the |
//   |                        | MANIFEST number and size and the number  |
//   |                        | of the "done" stub                       |
//   +------------------------+------------------------------------------+
//
// Stub states: each arrow below is one change of what CURRENT names. The
// text on the right is the step that comes before the change.
//
//   Create:    no CURRENT    --> creating stub  (create_if_missing)
//              creating stub --> done stub      (first DB row committed)
//   Enter:     MANIFEST file --> entering stub  (claim, authority kFile)
//              entering stub --> done stub      (switch, MANIFEST deleted)
//              entering stub --> MANIFEST file  (Open finds the DB row still
//                                                kFile; the enter then starts
//                                                again)
//   Roll back: done stub     --> MANIFEST file  (new MANIFEST from the rows
//                                                or from the copy)
//              entering stub --> MANIFEST file  (only before the switch)
//
// The switch is the ManifestStore::WriteGeneration() call of an enter. It
// writes all rows with authority kStore. Before it the MANIFEST file holds
// the metadata, after it the rows do (DBImpl::EnterManifestStore()).
//
// Open (DBImpl::Recover()):
// 1. Read CURRENT and the stub. Without CURRENT, create a new DB
//    (DBImpl::CreateDbInManifestStore()). A normal MANIFEST needs an enter
//    (DBImpl::EnterManifestStore()). A creating or entering stub first
//    finishes that mode change.
// 2. Check that the DB row belongs to this DB, then claim the DB with
//    ManifestStore::Open() (CheckOwnerAndClaim()). The claim fences an older
//    instance.
// 3. Scan all rows.
// 4. Check the DB row of the scan: its authority must be
//    ManifestAuthority::kStore, and its db id must match the stub. When
//    DBOptions::write_identity_file is true and the IDENTITY file exists, the
//    db id must also match that file.
// 5. Convert the rows to full-state VersionEdit records
//    (ManifestRowsToRecords()), and replay the records with the normal
//    recovery code from memory (VersionSet::RecoverFromStore()).
//
// Read-only: DB::OpenForReadOnly() needs a done stub. It scans without a
// claim, with a consistent scan, and never commits. DB::ListColumnFamilies()
// with a store scans the same way. DB::OpenAsSecondary(), DB::OpenAsFollower()
// and the remote compaction worker open the same way. A secondary and a
// follower scan again to catch up (see "Store mode in a secondary or
// follower" at ReactiveVersionSet::ReadStoreForCatchUp()).
//
// Close: when the DB starts to close, a commit with an unknown outcome gives
// up at the first failed retry, so a close does not wait for a store that
// does not answer (see DBImpl::closing_).
//
// Placeholder DB row: an enter into a store without a DB row first creates a
// DB row with authority kFile, no db id and zero counters
// (DBImpl::ClaimForEnter()). When the enter fails before its switch, that row
// stays. It belongs to no DB, so a later create or enter may take it over.
//
// Unsupported paths: see "Not supported in store mode" in
// include/rocksdb/manifest_store.h. They return NotSupported.
//
// Copies of the DB: DB::GetLiveFilesStorageInfo() builds a MANIFEST from the
// in-memory state (ManifestRowsToRecords()), so checkpoints and backups are
// DBs in file mode.
//
// Remote compaction: the primary sends its applied commit sequence
// (ManifestStoreState::applied_commit_sequence()) as
// CompactionServiceInput::min_store_commit_sequence when
// DBOptions::remote_compaction_manifest_floor is true (otherwise it sends 0,
// which means no floor). The worker refuses a scan with a smaller commit
// sequence and returns Incomplete, so the compaction service can run the job
// locally.
//
// DestroyDB(): with the store in the options, it checks that the DB row has
// the db id of the stub, calls ManifestStore::Destroy(), and then deletes the
// files. After a rollback, it also removes the rows that the rollback left
// when they have the db id of the DB (see DestroyDB() in
// db/db_impl/db_impl.cc).
//
// Write path (VersionSet::ProcessManifestWrites()): RocksDB turns each batch
// of VersionEdits into row changes and sends them to the store as one
// ManifestCommit (ManifestStoreState::CommitBatch()). The commit takes the
// place of the MANIFEST append of file mode. The DB mutex is not held during
// the commit. The stub MANIFEST is not written, and it never rolls over.
//
// Two rules on the write path keep the files of a commit safe:
// - In-flight file number: each commit writes the in-flight file number
//   (defined at ManifestDbField::kInFlightFileNumber in
//   include/rocksdb/manifest_store.h; computed by
//   DBImpl::MinFileNumberInUse()). A service that deletes unused files must
//   keep every file at or above it.
// - Quarantine: in store mode, the new files of every failed batch go to the
//   quarantine of the error handler, because a failed commit may still apply
//   in the store. The next batch that the store applies and that then
//   succeeds (including the sync copy update) clears the quarantine. A batch
//   with no store commit (for example a batch that skips the MANIFEST write)
//   does not clear it. The quarantine only stops PurgeObsoleteFiles(). An
//   external file ingestion or an import deletes its own files directly on
//   failure, so after an IOError in store mode it keeps them instead
//   (DBImpl::KeepFilesAfterStoreError()).
//
// Errors: see ManifestStoreState::CommitBatch().
//
// Mode changes: DBImpl::CreateDbInManifestStore() creates a new DB in the
// store, DBImpl::EnterManifestStore() moves a file-mode DB into the store,
// and RollBackManifestStore() moves a DB back to file mode. The MANIFEST file
// copy (below) makes a rollback without the store possible.

// Returns true when `options` (DBOptions or ImmutableDBOptions) give a store,
// so that a reader (a read-only, secondary or follower instance, or a remote
// compaction worker) reads the metadata from the store instead of the
// MANIFEST file. Three checks answer "is the metadata in the store?" for
// different callers:
//
//   Check                        Used by
//   ---------------------------  --------------------------------------------
//   VersionSet::IsStoreMode()    a primary, whose VersionSet commits to the
//                                store
//   StoreModeOptions(options)    the readers above, which have no
//                                ManifestStoreState
//   DBImpl::MetadataInStore()    the copy paths (checkpoints, backups), which
//                                run on a primary or a reader
template <typename DbOptions>
bool StoreModeOptions(const DbOptions& options) {
  return options.manifest_store != nullptr;
}

// Makes the commit ids of one DB instance. A commit id is the DB session id
// (new for each DB::Open()), a dash and a counter, so the ids of two instances
// never match, also when both use the same store epoch. Thread-safe.
class ManifestCommitIdGenerator {
 public:
  explicit ManifestCommitIdGenerator(std::string db_session_id);
  std::string Next();

 private:
  const std::string db_session_id_;
  std::atomic<uint64_t> counter_{0};
};

// The state that a stub MANIFEST records.
enum class ManifestStubState : uint8_t {
  // The rows in the store are the authority.
  kDone = 0,
  // A new DB is being created in the store. The DB row may not exist yet.
  kCreating = 1,
  // A DB whose MANIFEST file holds the metadata is being moved into the
  // store. The stub records that MANIFEST file and its size.
  kEntering = 2,
};

// The content of a stub MANIFEST.
struct ManifestStub {
  ManifestStubState state = ManifestStubState::kDone;
  // The RocksDB db id, as in the DB row and the IDENTITY file.
  std::string db_id;
  // kEntering only: the number and the size of the MANIFEST file that holds
  // the metadata until the switch, and the number of the "done" stub that
  // replaces this stub after the switch.
  uint64_t from_manifest_number = 0;
  uint64_t from_manifest_size = 0;
  uint64_t done_stub_number = 0;
};

// Appends the record of `stub` to `record`: the tag kManifestStoreStub, then
// a length-prefixed payload with a payload version (1), the state, the db id,
// and for kEntering the three numbers of ManifestStub.
void EncodeManifestStub(const ManifestStub& stub, std::string* record);

// Decodes a stub record. Sets `*is_stub` to false and returns OK when the
// record does not start with kManifestStoreStub. Otherwise sets `*is_stub` to
// true, also when it returns Corruption for a bad stub record or NotSupported
// for an unknown payload version or state (a stub of a newer binary).
Status DecodeManifestStub(const Slice& record, bool* is_stub,
                          ManifestStub* stub);

// Reads the first record of the MANIFEST file `path` and decodes it as a stub
// (DecodeManifestStub()). An empty MANIFEST is not a stub.
Status ReadManifestStub(FileSystem* fs, const std::string& path, bool* is_stub,
                        ManifestStub* stub);

// What CURRENT names.
struct CurrentManifestInfo {
  // False when there is no CURRENT file. The other fields are then not set.
  bool exists = false;
  uint64_t number = 0;
  // True when the MANIFEST that CURRENT names is a stub.
  bool is_stub = false;
  ManifestStub stub;
};

// Reads CURRENT and the first record of the MANIFEST that it names. A
// missing CURRENT file is not an error (`info->exists` is false).
Status ReadCurrentManifestInfo(FileSystem* fs, const std::string& dbname,
                               CurrentManifestInfo* info);

// Writes the stub file `MANIFEST-<number>`, syncs it, and points CURRENT to
// it (SetCurrentFile()).
IOStatus WriteManifestStub(const ImmutableDBOptions& db_options,
                           const std::string& dbname, uint64_t number,
                           const ManifestStub& stub,
                           const WriteOptions& write_options,
                           FSDirectory* dir_contains_current_file);

// The result of one scan of the store.
struct ManifestStoreScan {
  // The DB row with the values that the store owns.
  ManifestDbRowResult db_row;
  // All rows, including the rows of dropped column families.
  ManifestRows rows;
  ManifestDbRowExtraFields extra_fields;
};

// Scans all rows of the store and converts them. Returns the error of the
// scan, or the error of PublicRowsToManifestRows().
Status ScanManifestStore(ManifestStore* store, bool consistent,
                         ManifestStoreScan* scan);

// Decodes `db_row` into `rows` and `extra` (each may be null) and checks that
// the row belongs to the DB with `db_id`, not to another DB whose store key
// the caller passed by mistake (for example two DBs configured with the same
// store). Returns InvalidArgument when the row has another db id. A row
// without a db id is a placeholder DB row (see the overview), so it belongs
// to no DB. An empty `db_id` skips the check. Returns the error of
// PublicRowsToManifestRows() for a bad row.
Status CheckManifestDbRowOwner(const ManifestRow& db_row,
                               const std::string& db_id,
                               ManifestRows* rows = nullptr,
                               ManifestDbRowExtraFields* extra = nullptr);

// Claims the DB for this instance (ManifestStore::Open() with `opts`) when
// the DB row belongs to the DB with `expected_db_id`
// (CheckManifestDbRowOwner()). The owner check runs before the claim, so that
// a store object of another DB does not fence the primary of that DB, and
// again after the claim, because another instance can write the DB row in
// between. DB::Open() and RollBackManifestStore() use it.
//
// Results: NotFound when the store has no DB row (each caller maps it to its
// own error), IOFenced when a newer instance claimed the DB, IOError for any
// other store error, and the error of CheckManifestDbRowOwner(). The store's
// own status is never returned, so its flags have no effect. On success,
// `claimed` holds the DB row after the claim, and `extra` (if not null) its
// extra fields.
Status CheckOwnerAndClaim(ManifestStore* store,
                          const ManifestStoreOpenOptions& opts,
                          const std::string& expected_db_id,
                          ManifestDbRowResult* claimed,
                          ManifestDbRowExtraFields* extra = nullptr);

// What a secondary or follower reads from the store for its open or one
// catch-up (see "Store mode in a secondary or follower" at
// ReactiveVersionSet::ReadStoreForCatchUp()).
struct ManifestStoreRead {
  // The number of the stub MANIFEST that CURRENT names.
  uint64_t stub_number = 0;
  // A consistent scan of all rows.
  ManifestStoreScan scan;
  // The ids of the live column families of the instance (in increasing
  // order) that `bytes` was built for.
  std::vector<uint32_t> live_cf_ids;
  // The rows as the records of a MANIFEST file.
  std::string bytes;
};

// MANIFEST file copy
//
// Background: in store mode RocksDB can also write the metadata as a MANIFEST
// file, the copy, so that an operator can roll the DB back to file mode
// without the store (RollBackManifestStore()). The DB row holds the copy mode
// (ManifestCopyMode): off, background or sync.
//
// Files: `MANIFEST_COPY-<number>` is a copy (see ManifestCopyFileName()). The
// pointer file `MANIFEST_COPY_CURRENT` names the active copy, like CURRENT
// names the MANIFEST. A copy starts with a marker record (tag
// kManifestCopyMarker, safe to ignore), then the full state, then the edits of
// each later commit. The marker says whether the copy was written in sync
// mode ("sync-marked").
//
// Copy modes:
//
//   Mode        Written at Open        Each commit             Copy error
//   ----------  ---------------------  ----------------------  ---------------
//   off         no copy; old copies    nothing                 -
//               are deleted
//   background  unmarked copy, synced  edits appended after    copy dropped;
//                                      the commit, not synced  next commit
//                                                              starts a new one
//   sync        sync-marked copy,      edits appended and      DB stops
//               synced                 synced after the
//                                      commit, before install
//
// IOFenced from a copy operation stops the DB in every mode. Only a
// sync-marked copy can be used for a rollback without the store.
//
// Rule: a sync-marked copy that the pointer names holds every store commit
// except at most the last one. The last commit can be missing only when
// RocksDB crashed or its copy append failed after the store commit. RocksDB
// did not acknowledge that commit, and after a failed append the DB stopped.

// Appends the marker record of a copy to `record`: the tag
// kManifestCopyMarker, then a length-prefixed payload with a payload version
// (1) and the sync flag.
void EncodeManifestCopyMarker(bool sync_marked, std::string* record);

// Decodes a marker record. Sets `*is_marker` to false and returns OK when the
// record is not a marker.
Status DecodeManifestCopyMarker(const Slice& record, bool* is_marker,
                                bool* sync_marked);

// Writes the pointer file so that it names the copy `number`: a temporary
// file (ManifestCopyPointerTempFileName()), then a rename, then a sync of
// `dir`, as SetCurrentFile() does for CURRENT. Sets `*renamed` when the rename
// succeeded, so that the caller keeps the copy that the pointer may name even
// when the directory sync fails.
IOStatus WriteManifestCopyPointer(FileSystem* fs, const std::string& dbname,
                                  uint64_t number,
                                  const WriteOptions& write_options,
                                  FSDirectory* dir, bool* renamed);

// Reads the pointer file. Sets `*number` to 0 when there is no pointer file.
Status ReadManifestCopyPointer(FileSystem* fs, const std::string& dbname,
                               uint64_t* number);

// Deletes the copy files other than `keep_number` (0 keeps none). Deletes the
// pointer file too when `delete_pointer` is true. Returns the first error.
IOStatus DeleteManifestCopies(FileSystem* fs, const std::string& dbname,
                              uint64_t keep_number, bool delete_pointer);

// Returns the records as the bytes of a MANIFEST file (the log format of
// db/log_writer.h), so that the normal recovery code can read them from
// memory.
Status RecordsToManifestBytes(const std::vector<std::string>& records,
                              std::string* bytes);

// Returns a sequential file that reads `data`. It frees `data` when a read
// reaches the end.
std::unique_ptr<FSSequentialFile> NewStringSequentialFile(std::string data);

// The store mode state of one DB instance that opened a DB for writing.
// VersionSet owns one in store mode (VersionSet::IsStoreMode()).
//
// Threads: only the thread that runs VersionSet::ProcessManifestWrites()
// calls CommitBatch() and changes the state, one batch at a time, without the
// DB mutex. Open sets the state before any write.
class ManifestStoreState {
 public:
  // The most rows of dropped column families (file, blob file and column
  // family rows) that one commit deletes. So the cleanup makes a commit at
  // most 1000 rows larger than its batch. The value is a guess, because the
  // limit of real stores per commit is not known yet.
  static constexpr size_t kDroppedCfRowsPerCommit = 1000;

  // `db_session_id` makes the commit ids unique (ManifestCommitIdGenerator).
  // `feature_version` is DBOptions::manifest_row_feature_version.
  ManifestStoreState(std::shared_ptr<ManifestStore> store,
                     const std::string& db_session_id,
                     uint32_t feature_version);

  // The stop error is state, not a result for a caller to check, so a state
  // that is destroyed without a check (for example after a failed open) is
  // fine.
  ~ManifestStoreState() { stop_error_.PermitUncheckedError(); }

  // Sets the state from `scan`, the scan after the claim of this instance.
  // The rows of dropped column families are queued for deletion. Must be
  // called once, before CommitBatch().
  void Init(const ManifestStoreScan& scan);

  // A new commit id, for the claim and the commits of DB::Open().
  std::string NextCommitId() { return commit_ids_.Next(); }

  ManifestStore* store() const { return store_.get(); }

  // The DB row and the column family rows of the last commit. File rows are
  // not kept.
  const ManifestRows& committed_rows() const { return committed_rows_; }

  // The commit sequence of the last commit that applied: the claim of
  // DB::Open() or a later CommitBatch(). Every Version installed so far was
  // committed at or below it, so a consistent scan with at least this commit
  // sequence holds them. A remote compaction worker uses it as its floor
  // (CompactionServiceInput::min_store_commit_sequence). Any thread can read
  // it without a data race. The guarantee above holds only for a caller that
  // holds the DB mutex.
  uint64_t applied_commit_sequence() const {
    return expected_commit_sequence_.load(std::memory_order_relaxed);
  }

  // Stops the DB with `error`, for an error after a commit applied, such as a
  // failed copy append in sync mode. Returns the stop error.
  IOStatus Stop(const IOStatus& error);

  // The copy mode of the DB row.
  ManifestCopyMode copy_mode() const { return extra_fields_.copy_mode; }

  // Sets the copy file number that the next commit writes into the DB row (0
  // when there is no copy). The value is information for operators: RocksDB
  // never reads it, and uses the pointer file instead. It is one commit behind
  // after a new copy, because the copy is written after the commit it follows.
  void SetCopyFileNumber(uint64_t number) {
    extra_fields_.copy_file_number = number;
  }

  // Queues the deletion of the file and blob file rows, then of the column
  // family row, of a column family that the last commit dropped.
  void QueueDroppedCfRows(uint32_t cf_id, const std::vector<uint64_t>& files,
                          const std::vector<uint64_t>& blob_files);

  // Timestamp size changes: a column family can turn user-defined timestamps
  // on or off, or change persist_user_defined_timestamps, only across a
  // reopen. The rows always hold the file boundaries in the form that the
  // recovered Version holds. So when DB::Open() finds a column family whose
  // timestamp size, flag or comparator name differs from its row, it gives
  // the rows of that column family from the recovered Version here: the
  // column family row and all its file and blob file rows. The next commit
  // writes them (PrepareRewrite() and MergeRewrite()). A crash before that
  // commit leaves the old rows, and the next DB::Open() finds the same
  // difference again.
  void SetRewrite(std::map<uint32_t, ManifestCfRows> rewrite) {
    rewrite_ = std::move(rewrite);
  }

  // Puts the rewritten column family rows into the committed rows, so that
  // BuildManifestRowChanges() starts from them. Call before building the row
  // changes of a batch. This is safe also when the commit then fails: the
  // committed rows differ from the store until a commit applies, but every
  // batch until then upserts the rewritten rows again (MergeRewrite()),
  // because only an applied commit clears the rewrite.
  void PrepareRewrite();

  // Adds the rewritten column family rows (also when the batch writes none)
  // and the rewritten file and blob file rows to `changes` as upserts. A file
  // or blob file row that the batch deletes stays deleted. Call after
  // building the row changes of a batch.
  void MergeRewrite(ManifestRowChanges* changes) const;

  // How CommitBatch() waits and when it gives up while the outcome of a
  // commit is unknown. DBImpl sets it during DB::Open().
  struct RetryPolicy {
    SystemClock* clock = nullptr;
    // The most retries after the first send, to learn the outcome: resends,
    // and ReadDbRow() calls that repeat a failed one
    // (DBOptions::max_bgerror_resume_count). With 0, RocksDB still sends once
    // and reads the DB row once.
    int max_retries = 0;
    // The wait between two attempts (DBOptions::bgerror_resume_retry_interval).
    uint64_t retry_interval_us = 0;
    // The LOG of the DB, for the resend and stop messages. May be null.
    Logger* info_log = nullptr;
    // Returns true when the DB starts to close: from the start of
    // DBImpl::CloseHelper() (before its shutdown flush), or from the point in
    // CancelAllBackgroundWork() after its own shutdown flush. CommitBatch()
    // checks it after each failed ReadDbRow() or resend, so during a close a
    // commit with an unknown outcome stops at the first such failure (see
    // DBImpl::closing_).
    std::function<bool()> is_closing;
  };
  void SetRetryPolicy(RetryPolicy policy) { retry_policy_ = std::move(policy); }

  // Commits the row changes of one batch to the store. `in_flight_file_number`
  // is the value of DBImpl::MinFileNumberInUse(), without the cap by the next
  // file number of the commit (this function applies it).
  //
  // Results (the store's own status is never returned as it is; the store's
  // retryable flag and scope have no effect, and only the text of the last
  // failed store call is added to the message):
  //
  //   Commit() returns    RocksDB does                    CommitBatch() returns
  //   ------------------  ------------------------------  ---------------------
  //   OK                  updates the committed rows      OK
  //   IOFenced            stops the DB                    IOFenced
  //   TryAgain            nothing (not applied)           retryable IOError
  //   Busy, other error   learns the outcome (below)      OK, IOFenced, or a
  //                                                       stop error
  //
  // - IOFenced: a newer epoch claimed the DB.
  // - The retryable IOError: the commit was not applied, and a retry is safe.
  //   The error handler starts auto-resume, which commits an empty batch
  //   (DBImpl::ResumeImpl()).
  // - A stop error is an IOError without the retryable flag (see below).
  //
  // Unknown outcome: when Commit() returns a status other than OK, IOFenced,
  // Busy or TryAgain, the commit may still apply. RocksDB then reads the DB
  // row: if the commit sequence is the expected one plus 1 and the last
  // commit id is the id of this commit, the commit applied. If the commit
  // sequence is still the expected one, RocksDB sends the same commit again.
  // Each resend, and each ReadDbRow() that repeats a failed one, is one retry.
  // RocksDB makes at most RetryPolicy::max_retries retries. It waits
  // RetryPolicy::retry_interval_us after a failed ReadDbRow() and after a
  // resend that fails with a status other than Busy. Busy from the first send
  // also leads to a ReadDbRow(), but never to a resend.
  //
  // Stopped: when the outcome cannot be learned (the retry limit is reached,
  // the DB starts to close, a DB row with another commit, no DB row, or Busy
  // from the first send while the commit sequence is unchanged), the DB is
  // stopped. Every later call returns the same error at once without a
  // store call. The error is a plain IOError: not retryable and without a
  // file scope, so the error handler makes it a fatal error and does not
  // resume. This holds also after an earlier TryAgain: VersionSet keeps the
  // stop error in place of the retryable one (VersionSet::io_status()), and
  // DBImpl::ResumeImpl() reports it. The next DB::Open() claims the DB, which
  // raises the commit sequence, so the old commit can never apply after
  // that. During a close (RetryPolicy::is_closing), a short store failure can
  // stop the DB, because RocksDB gives up at the first failed ReadDbRow() or
  // resend.
  IOStatus CommitBatch(const ManifestRowChanges& changes,
                       uint64_t in_flight_file_number);

 private:
  // Updates the state after the commit of `changes` applied.
  // `cleanup_deletes` is the number of queued rows of dropped column families
  // that the commit deleted.
  void OnApplied(const ManifestRowChanges& changes, size_t cleanup_deletes);

  const std::shared_ptr<ManifestStore> store_;
  ManifestCommitIdGenerator commit_ids_;
  const uint32_t feature_version_;
  // The commit sequence that the next commit expects, which is also the
  // commit sequence of the last commit that applied: ManifestStore::Commit()
  // takes the commit sequence that the DB row has now, and the store adds 1 on
  // success. So after a commit applies, the DB row has the value that the next
  // commit expects. Only the commit thread changes it. It is atomic because
  // other threads read it with applied_commit_sequence(). Relaxed order is
  // enough there: a reader that needs the guarantee of
  // applied_commit_sequence() holds the DB mutex, and the Versions it uses were
  // installed under the DB mutex after their commit applied.
  std::atomic<uint64_t> expected_commit_sequence_{0};
  ManifestRows committed_rows_;
  ManifestDbRowExtraFields extra_fields_;
  IOStatus stop_error_;
  RetryPolicy retry_policy_;
  // Keys of rows of dropped column families to delete, in order: for each
  // column family its file and blob file rows, then its row.
  std::deque<ManifestRowKey> dropped_cf_rows_;
  // See SetRewrite(). Empty after the commit that writes it.
  std::map<uint32_t, ManifestCfRows> rewrite_;
};

}  // namespace ROCKSDB_NAMESPACE
