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
// A stub is written and synced before CURRENT names it (WriteManifestStub()).
// So only media damage, or a file system that does not honor fsync, can leave
// an empty or unreadable stub. The salvage tools (best-efforts recovery,
// RepairDB() and DestroyDB()) then can treat the DB as file mode.
//
// "Enter" is the move of a file-mode DB into the store, done by DB::Open()
// with DBOptions::manifest_store_migrate. The enter writes all rows of a new
// generation with ManifestStore::WriteGeneration(). The switch is the moment
// when the store makes that generation visible: in the same commit it sets
// the authority in the DB row to kStore. Before the switch the MANIFEST file
// holds the metadata, after it the rows do (DBImpl::EnterManifestStore()).
// Claim, epoch and commit sequence are defined in "Terms" in
// include/rocksdb/manifest_store.h. The two cases below learn in different
// ways whether the switch happened:
// - The same enter, after a WriteGeneration() call with an unknown outcome:
//   RocksDB reads the DB row. The switch applied only if the row decodes,
//   holds authority kStore, and holds the commit id of this switch as its last
//   commit id. Otherwise the open fails with an IOError, and the entering stub
//   stays.
// - A later read-write open that finds an entering stub
//   (DBImpl::FinishEnterManifestStore()): it checks that the DB row belongs to
//   this DB (the db id of the stub) and claims that row (CheckOwnerAndClaim()).
//   Then the authority of the claimed row decides:
//   - kStore: the switch happened, and the open finishes the enter.
//   - kFile: the switch did not happen. The open points CURRENT back to the
//     MANIFEST file and starts the enter again.
//
// Stub states: each arrow below is one change of what CURRENT names. The
// text on the right says when the change happens.
//
//   Create:    no CURRENT    --> creating stub  (create_if_missing)
//              creating stub --> done stub      (first DB row committed)
//   Enter:     MANIFEST file --> entering stub  (after the claim; authority
//                                                kFile, the MANIFEST file
//                                                holds the metadata)
//              entering stub --> done stub      (after the switch wrote
//                                                kStore and the MANIFEST
//                                                file was deleted)
//              entering stub --> MANIFEST file  (Open finds the DB row still
//                                                kFile; the enter then starts
//                                                again)
//   Roll back: done stub     --> MANIFEST file  (new MANIFEST from the rows
//                                                or from the copy)
//              entering stub --> MANIFEST file  (only before the switch)
//
// Between the switch and the done stub, CURRENT still names the entering
// stub while the rows hold the metadata. An open in that window finds a
// kStore DB row next to the entering stub and finishes the enter: it deletes
// the MANIFEST file, writes the done stub, points CURRENT to it, and removes
// the entering stub (DBImpl::FinishEnterManifestStore()).
//
// Open (DBImpl::Recover()):
// 1. Read CURRENT and the stub. Without CURRENT, and with create_if_missing,
//    DBImpl::Recover() first creates a new DB with
//    DBImpl::CreateDbInManifestStore() (in file mode it calls NewDB() there),
//    and then opens it. Before that, it looks at the MANIFEST files in the
//    folder (DBImpl::CheckNoSurvivingStubBeforeCreate()): a stub there gives
//    NotSupported, and with a store, a normal MANIFEST file gives
//    InvalidArgument, because CURRENT of an existing DB was lost. The open
//    creates the DB again in one case only: the only stub is a creating stub
//    whose record decodes, no normal MANIFEST file is there, and the store has
//    no DB row. A normal MANIFEST that CURRENT names: with
//    DBOptions::manifest_store_migrate, Open moves the DB into the store
//    (DBImpl::EnterManifestStore()); without it, Open returns InvalidArgument.
//    A creating or entering stub first finishes that mode change.
// 2. Check that the DB row belongs to this DB, then claim the DB with
//    ManifestStore::Open() (CheckOwnerAndClaim()). Every read-write open claims
//    here once CURRENT names a done stub. This includes an open that already
//    claimed the DB during an enter, or while it finished an enter or a create.
//    A writer that takes over the DB uses a newer epoch, so the claim fences
//    store objects with lower epochs. Each claim raises the commit sequence and
//    records a new commit id, so requests that expect the previous commit
//    sequence get Busy. With manifest_store_migrate, the claim also writes the
//    requested copy mode.
// 3. Scan all rows, without asking for a consistent scan. This instance
//    sends no commit during its scan. A writer with a newer epoch can make the
//    scan return IOFenced or a mix of rows from several commits, but then this
//    instance cannot commit any more (see ManifestScanOptions::consistent).
//    Read-only opens scan differently (see "Read-only" below).
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
// with a store scans the same way.
//
// Open and close: during DB::Open(), and when the DB starts to close, a
// commit with an unknown outcome gives up at the first failed ReadDbRow() or
// resend. So DB::Open() returns an error, as a failed MANIFEST write fails
// DB::Open() in file mode, and a close does not start more retries (see
// DBImpl::opening_ and DBImpl::closing_). A close still waits for a store
// call that is already running, so the store must end each call in bounded
// time (see "Threading and timeouts" in include/rocksdb/manifest_store.h).
//
// Placeholder DB row: an enter into a store without a DB row first creates a
// DB row with authority kFile, no db id and zero counters
// (DBImpl::ClaimForEnter()). When the enter fails before its switch, that row
// stays. It belongs to no DB, so a later create or enter may take it over.
//
// Unsupported paths: see "Not supported in store mode yet" in
// include/rocksdb/manifest_store.h. They return NotSupported.
//
// Write path (VersionSet::ProcessManifestWrites()): RocksDB turns each batch
// of VersionEdits into row changes and sends them to the store as one
// ManifestCommit (ManifestStoreState::CommitBatch()). The commit takes the
// place of the MANIFEST append of file mode. The DB mutex is not held during
// the commit. The stub MANIFEST is not written, and it never rolls over.
//
// Dropped column families: a drop edit does not list the files of the column
// family, so the drop commit only marks the column family row as dropped and
// keeps its file and blob file rows. Later commits delete these rows, at most
// ManifestStoreState::kDroppedCfRowsPerCommit (1000) rows per commit, and the
// column family row last. When a crash stops this cleanup, the next DB::Open()
// queues the rows that are left (ManifestStoreState::Init()). These deletes
// are optional: RocksDB can move them to a later commit. When the store
// rejects a commit with them for good (NotSupported on the first send),
// CommitBatch() sends the batch once more without them, and they stay queued.
//
// Three rules on the write path keep the files of a commit safe:
// - In-flight file number: the field ManifestDbField::kInFlightFileNumber in
//   include/rocksdb/manifest_store.h. That comment also describes the limits
//   for older Versions and blob direct writes. A service that deletes unused
//   SST and blob files must keep every file at or above this number, and every
//   file that a row names. The number protects the files that RocksDB is
//   writing but that no row names yet. In sync copy mode it can be lower than
//   those files, to protect the files that the copy still names. Each commit
//   writes the smallest of three values:
//   1. the next file number of the batch;
//   2. DBImpl::MinFileNumberInUse(), computed under the DB mutex (its comment
//      in db/db_impl/db_impl.h lists the files it covers, for example the
//      pending outputs of flushes and compactions and the tracked blob direct
//      write files);
//   3. in sync copy mode, the smallest file number whose row the commit
//      deletes (see "Sync copy retention" below).
//   ManifestStoreState::CommitBatch() applies the caps 1 and 3. Order:
//   ProcessManifestWrites() reads the next file number of the batch, then calls
//   MinFileNumberInUse() with the DB mutex held. A blob direct write takes its
//   number without the DB mutex, and tracks the file in the same step under one
//   small lock (BlobFilePartitionManager::NewTrackedBlobFileNumber()).
//   MinFileNumberInUse() reads the tracked files under the same lock. For
//   example, a batch reads 50 as its next file number. A blob file with a
//   number below 50 was taken before that read, and was tracked in the same
//   step, so MinFileNumberInUse() sees it while it is still tracked. A blob
//   file taken after the read has a number of 50 or more. The commit writes an
//   in-flight file number of at most 50, so the service keeps both files.
// - Sync copy retention: in ManifestCopyMode::kSync, the copy gets a batch
//   only after its store commit. Until then, the copy still names the files
//   whose rows the edits of the commit delete, and a rollback without the
//   store needs them. So the commit also caps the in-flight file number by the
//   smallest of these file numbers (ManifestStoreState::CommitBatch()). The
//   deletes of leftover rows of a column family that an earlier commit dropped
//   do not lower the cap, because the copy already holds the drop. For
//   example, a compaction replaces SST 12 with SST 48: the commit deletes the
//   row of 12 and writes an in-flight file number of at most 12, so the service
//   keeps 12. The next commit is sent only after the copy holds this batch, and
//   it writes its own number, which releases 12. When the copy update fails,
//   the DB stops, and the next DB::Open() writes a new copy from the rows
//   before its first commit. A background copy needs no such cap, because a
//   rollback without the store never uses it.
// - Quarantine: in store mode, when the store commit or the copy update returns
//   an I/O error to a batch, the new files of the batch go to the quarantine of
//   the error handler, because a failed commit may still apply in the store.
//   The files go there also in these cases:
//   - The store says that it did not apply the commit (TryAgain or
//     NotSupported).
//   - CommitBatch() returns the saved stop error without a new store call.
//   A batch whose row changes reject a WAL edit fails before CommitBatch() and
//   adds no files. The next batch that the store applies and that then succeeds
//   (including the sync copy update) clears the quarantine. A batch with no
//   store commit (for example a batch that skips the MANIFEST write) does not
//   clear it. The quarantine only stops PurgeObsoleteFiles(). An external file
//   ingestion or an import deletes its own files directly on failure, so after
//   an IOError in store mode it keeps them instead
//   (DBImpl::KeepFilesAfterStoreError()).
//
// Errors: a failed store commit either did not apply (TryAgain or
// NotSupported), or RocksDB learns its outcome, or the store state stops. A
// stop saves an error in ManifestStoreState: every later MANIFEST batch that
// reaches CommitBatch() gets that error at once, without a store call, until
// the DB is closed and opened again. The background error of the DB is
// separate from this stop. NotSupported from the first send returns a
// non-retryable IOError and does not stop the store state. A flush or a
// compaction that records this IOError as its background error makes the DB
// error fatal.
// ManifestStoreState::CommitBatch() has the full result table and the retry
// loop. In short, with E the expected commit sequence of the commit:
//
//   Commit() --+-- OK -------------------------------> applied
//              +-- TryAgain (temporary rejection) ---> retryable IOError
//              +-- NotSupported (rejected for good) -> IOError (*)
//              +-- IOFenced -------------------------> stop
//              +-- Busy or unknown outcome
//                     |
//                     v
//                  ReadDbRow() --+-- E+1 and this commit id --> applied
//                                +-- E, last send was Busy ---> stop
//                                +-- E, last send had an -----> resend, then
//                                |   unknown outcome, a         ReadDbRow()
//                                |   retry left                 again
//                                +-- anything else -----------> stop
//
// Busy never leads to a resend. Busy means that the stored commit sequence was
// not E. A DB row that still holds E after Busy does not match that, so RocksDB
// cannot learn the outcome and stops the DB. ManifestStoreState::CommitBatch()
// describes a failed ReadDbRow() and has the full result table.
//
// (*) No resend of the same commit. When the commit carries deletes of
// dropped column family rows, CommitBatch() first sends the batch once more
// without them (see "Dropped column families" above).
//
// 1. TryAgain: CommitBatch() returns a retryable IOError, which can start
//    auto-resume.
// 2. NotSupported: the store rejects the commit for good and did not apply
//    it. CommitBatch() returns a non-retryable IOError at once. A caller that
//    sets it as the background error (for example a flush or a compaction)
//    gets a fatal error, because the error handler makes a MANIFEST write
//    IOError without the retryable flag fatal. DB::Resume() returns that
//    error without a store call, so the DB must be closed and opened again.
//    The store state itself does not stop (see "Stopped" in
//    ManifestStoreState::CommitBatch()). A resend that gets NotSupported
//    stops the DB instead, because the first send can still apply.
// 3. Busy or an unknown outcome: CommitBatch() reads the DB row, and after an
//    unknown outcome it resends, within ManifestStoreState::RetryPolicy. Then
//    it either applies the batch or stops the DB.
// 4. A stop or IOFenced: VersionSet::io_status() keeps the error in place of
//    an earlier retryable error, so the error handler makes it fatal.
//
// Mode changes: DBImpl::CreateDbInManifestStore() creates a new DB in the
// store, DBImpl::EnterManifestStore() moves a file-mode DB into the store,
// and RollBackManifestStore() moves a DB back to file mode. The MANIFEST file
// copy (below) makes a rollback without the store possible.
//
// Limits:
// - RocksDB sends each batch as one commit, and does not split a batch that
//   is larger than the transaction limit of the store.
// - A timestamp rewrite puts all file rows of a column family into one
//   commit. A read-write open queues one when a column family turned
//   user-defined timestamps on or off, or changed its comparator name or
//   persist_user_defined_timestamps, since its row was written
//   (DBImpl::RowsToRewriteAfterOpen()). The file rows hold boundary keys in
//   the form of the recovered Version, so the open queues the column family
//   row and all its file and blob file rows again. The next MANIFEST batch
//   carries them, and the rows that the batch itself writes or deletes win.
//   The rewrite stays queued until a store commit applies (see
//   ManifestStoreState::SetRewrite()).
// - A rollback without the store cannot claim the DB, so it cannot fence the
//   old primary. It relies on the folder lock and on the application having
//   stopped the old primary.
// - RocksDB treats every Commit() status other than OK, IOFenced, Busy,
//   TryAgain and NotSupported as an unknown outcome. This includes a status
//   that means the commit can never apply, for example InvalidArgument for a
//   commit that is too large; a store should return NotSupported for it.
//   After DB::Open(), RocksDB resends such a commit until the outcome is
//   known, the retries run out, or the DB starts to close. With the default
//   DBOptions::max_bgerror_resume_count (INT_MAX), the retries do not run
//   out. Later MANIFEST writes wait during that time.
// - Blob direct write files after a crash: a blob direct write file that only
//   the WAL referred to at the crash is not named by any row after the next
//   DB::Open(), and its number is below the in-flight file number. Base
//   RocksDB does not add such a file to the Version (see
//   enable_blob_direct_write in include/rocksdb/advanced_options.h: "WAL
//   replay of active direct-write blob files is not currently supported").
//   RocksDB's own purge keeps the file, but a service that deletes unused
//   files by the rows and the in-flight file number can delete it, and the
//   value is then lost. This holds for an enter and for a store-mode reopen.
// - A close gives up at the first failed ReadDbRow() or resend (see "Open and
//   close"). So a short store failure during the shutdown flush can stop the
//   DB, and data written without the WAL is then lost.

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
  // store. The stub records that MANIFEST file and its size. While the DB row
  // has authority kFile, that MANIFEST file is still the authority. When the
  // DB row has authority kStore, the switch happened, and the MANIFEST file may
  // already be deleted.
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

// Returns true when the caller must search the folder for a surviving stub
// (FindSurvivingManifestStub()), because CURRENT cannot tell where the
// metadata is (a MANIFEST file or the store). `current_s` and `current` are
// the results of ReadCurrentManifestInfo(). This is the case when CURRENT
// does not name a stub, and either CURRENT is missing (`current_s` OK and
// `current.exists` false) or `current_s` is NotFound, PathNotFound or
// Corruption (CURRENT is damaged, or names a missing or damaged MANIFEST
// file). It returns false for any other error, for example a timeout, which
// the caller returns. A missing or damaged CURRENT is what the file-mode
// salvage tools (best-efforts recovery, RepairDB() and DestroyDB()) handle,
// but a stub can still be in the folder then.
bool ShouldSearchForSurvivingManifestStub(const Status& current_s,
                                          const CurrentManifestInfo& current);

// Background: a stub can survive in the DB folder when CURRENT is lost or
// damaged. For example, the DB is in store mode, and a rollback to file mode
// (RollBackManifestStore()) wrote a newer normal MANIFEST file but stopped
// before it pointed CURRENT to that file. If CURRENT is then lost, the newer
// file misses the commits in the store, and only the stub shows that the store
// holds the metadata.
//
// This function reads the first record of each MANIFEST file among
// `filenames` (the names in the folder `dbname`) and sets `*found` to true
// when one of them is a stub, also a stub that this binary cannot decode. A
// missing or damaged MANIFEST file is not a stub. Any other read error (for
// example an I/O error or a timeout) leaves open whether the file is a stub,
// and is returned.
Status FindSurvivingManifestStub(FileSystem* fs, const std::string& dbname,
                                 const std::vector<std::string>& filenames,
                                 bool* found);

// Checks that the DB in `dbname` is not in store mode, for a tool that rebuilds
// the metadata from the files in the folder (best-efforts recovery and
// RepairDB()). `filenames` are the names in the folder. Returns:
// - NotSupported, with `what` in the message, when CURRENT names a stub, or
//   when CURRENT names no authority (ShouldSearchForSurvivingManifestStub())
//   and a stub survives in the folder (FindSurvivingManifestStub()).
// - Another error that leaves open whether there is a stub, for example a
//   timeout while it reads CURRENT or a MANIFEST file.
// - OK otherwise. A valid CURRENT that names a normal MANIFEST file decides
//   alone, so an older stub that a finished rollback could not delete does not
//   matter then. That case also sets `*current_names_manifest_file` to true
//   (otherwise false), so that best-efforts recovery can skip a stub that
//   CURRENT does not name.
Status CheckNoManifestStub(FileSystem* fs, const std::string& dbname,
                           const std::vector<std::string>& filenames,
                           const std::string& what,
                           bool* current_names_manifest_file);

// Writes the stub file `MANIFEST-<number>`, syncs it, and points CURRENT to
// it (SetCurrentFile()).
IOStatus WriteManifestStub(const ImmutableDBOptions& db_options,
                           const std::string& dbname, uint64_t number,
                           const ManifestStub& stub,
                           const WriteOptions& write_options,
                           FSDirectory* dir_contains_current_file);

// Sets `*largest` to the largest file number in the DB folder, including the
// numbers of the copy files, which ParseFileName() does not parse. Returns
// the error of listing the folder.
Status LargestFileNumber(FileSystem* fs, const std::string& dbname,
                         uint64_t* largest);

// Reads the db id from the IDENTITY file of the DB folder `dbname`, for a
// caller that needs the db id of a DB in file mode whose MANIFEST file has
// none. Removes a trailing newline, which older binaries wrote. A missing
// IDENTITY file is not an error: `*db_id` is then empty. Returns any other
// read error.
Status ReadDbIdFromIdentityFile(FileSystem* fs, const std::string& dbname,
                                std::string* db_id);

// Deletes the stub file `number` in the DB folder `dbname`, after CURRENT
// stopped naming it. The stub is then only a leftover file, which the obsolete
// file purge of a later DB::Open() also deletes, so a failed delete only
// writes a WARN line to `info_log` (which may be null).
void DeleteStubFile(FileSystem* fs, const std::string& dbname, uint64_t number,
                    Logger* info_log);

// Ends an enter that did not reach its switch: CURRENT names the "entering"
// stub `current`, and the MANIFEST file still holds the metadata. Checks that
// the MANIFEST file of the stub still has the size that the stub recorded,
// points CURRENT back to it, and deletes the stub with DeleteStubFile(). Used
// by DBImpl::FinishEnterManifestStore() and RollBackManifestStore().
// Errors: Corruption when the size of that MANIFEST file cannot be read (for
// example, the file is missing) or differs from the stub, or the error of
// SetCurrentFile().
Status PointCurrentBackFromEnteringStub(const WriteOptions& write_options,
                                        FileSystem* fs,
                                        const std::string& dbname,
                                        const CurrentManifestInfo& current,
                                        Temperature temperature,
                                        FSDirectory* dir, Logger* info_log);

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

// Checks that the scanned rows are the rows of the DB in store mode whose
// stub MANIFEST has `stub_db_id`. DB::Open(), DB::OpenForReadOnly() and
// DB::ListColumnFamilies() run this check after their scan. Returns
// InvalidArgument when the DB row names the MANIFEST file as the authority
// (after a rollback), NotSupported for authority kShadow, and Corruption when
// the db id of the DB row differs from `stub_db_id` (for example a store object
// of another DB). A read-write open checks the owner before its claim
// (CheckManifestDbRowOwner()) and returns InvalidArgument there, because
// nothing has changed yet. This check runs after the scan, so a different db
// id here means that the stub and the rows disagree.
Status CheckScannedStoreRows(const ManifestStoreScan& scan,
                             const std::string& stub_db_id);

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

// CheckOwnerAndClaim() makes at most this many claims, then returns TryAgain.
inline constexpr int kMaxOwnerClaimAttempts = 10;

// Claims the DB for this instance (ManifestStore::Open() with `opts`) when
// the DB row belongs to the DB with `expected_db_id`
// (CheckManifestDbRowOwner()). DB::Open() and RollBackManifestStore() use
// it. Steps:
// 1. Read the DB row and check its owner. A store object of another DB (a
//    configuration error) fails here, before any change.
// 2. Claim with ManifestStoreOpenOptions::expected_commit_sequence and
//    expected_last_commit_id set to the values that step 1 read. So the
//    claim applies only when the row has not changed since step 1, and never
//    fences a DB that took over the store key after the read (a new row
//    never has the same last commit id). When the row changed, the store
//    returns Busy and changes nothing, and the function starts again at
//    step 1.
// A primary that is still running commits during the open of a new instance, so
// a claim can get Busy. After kMaxOwnerClaimAttempts (10) claims that got Busy,
// the function returns TryAgain. Each attempt is two store calls, and a primary
// that commits in each such window is not expected.
//
// Results: NotFound when the store has no DB row (each caller maps it to its
// own error), IOFenced when a newer instance claimed the DB, TryAgain (see
// above), IOError for any other store error, and the error of
// CheckManifestDbRowOwner(). The store's own status is never returned, so its
// flags have no effect. On success, `claimed` holds the DB row after the
// claim, and `extra` (if not null) its extra fields.
Status CheckOwnerAndClaim(ManifestStore* store,
                          const ManifestStoreOpenOptions& opts,
                          const std::string& expected_db_id,
                          ManifestDbRowResult* claimed,
                          ManifestDbRowExtraFields* extra = nullptr);

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
// Copy layout (VersionSet::WriteNewManifestCopy()). Each box is one or more
// log records. In this example, commit 7 starts the copy at a rollover. The
// store has already applied commit 7 when RocksDB writes the copy. A copy
// that DB::Open() starts has no edits of its own commit.
//
//   +--------------+--------------+-----------+----------+-----------------+
//   | marker       | installed    | DB fields | edits of | edits of commit |
//   |              | state before | record    | commit 7 | 8, then 9, ...  |
//   |              | commit 7     |           |          |                 |
//   +--------------+--------------+-----------+----------+-----------------+
//
// - marker: the tag kManifestCopyMarker and the sync flag
//   (EncodeManifestCopyMarker()).
// - installed state before commit 7: the records of
//   VersionSet::WriteCurrentStateToManifest() for the installed Versions,
//   which do not have the edits of commit 7 yet, as at the start of a new
//   MANIFEST file.
// - DB fields record: one VersionEdit with the next file number, the last
//   sequence, the prev log number and the max column family
//   (VersionSet::ManifestCopyDbFields()). A new MANIFEST file gets these
//   fields from the edits that follow its installed state. A copy that
//   DB::Open() writes has no commit after the installed state, so this record
//   makes the copy replayable on its own.
// - edits: the VersionEdits of each commit, in commit order. The edits of an
//   atomic group stay together.
//
// Copy modes:
//
//   Mode        Written at Open        Each commit             Copy error
//   ----------  ---------------------  ----------------------  ---------------
//   off         no copy; old copies    nothing                 -
//               are deleted
//   background  copy with the sync     edits appended after    writer dropped
//               flag clear in its      the commit, not synced  (files stay);
//               marker, synced                                 next commit
//                                                              starts a new one
//   sync        sync-marked copy,      edits appended and      DB stops
//               synced                 synced after the
//                                      commit, before install
//
// In both modes the thread that commits to the store also appends the edits
// to the copy. Only sync mode syncs each append, so a crash can lose the last
// appends of a background copy.
//
// IOFenced from a copy operation stops the DB in every mode. After another
// error in background mode, RocksDB stops writing to the copy, but its files
// stay (a rename of the pointer may already have taken effect). The next
// commit that the store applies starts a new copy, and a later rollover or
// DB::Open() deletes the old files. At DB::Open() in background mode, a failed
// copy instead deletes the pointer and the copies, and can return the error
// of that delete (VersionSet::StartManifestCopyAtOpen()). Only a sync-marked
// copy can be used for a rollback without the store.
//
// Lifecycle of a copy (modes background and sync):
// 1. DB::Open() writes a new copy with the full state and points
//    MANIFEST_COPY_CURRENT to it (VersionSet::StartManifestCopyAtOpen()).
// 2. Each commit appends its edits to the copy after the store commit
//    (VersionSet::UpdateManifestCopy()).
// 3. The copy has a size limit, which max_manifest_file_size and
//    max_manifest_space_amp_pct tune as for a MANIFEST file. The copy uses
//    that limit for every commit. It does not get the larger limit that a
//    MANIFEST file gets for a foreground write. When there is no copy, or the
//    copy has reached the limit, the next commit writes a new copy with the
//    full state (VersionSet::WriteNewManifestCopy()).
// 4. After the pointer names the new copy, the older copies are deleted. A
//    copy that is left over is deleted by a later rollover or DB::Open(). A
//    failed delete writes a WARN line.
// 5. DB::Close() syncs the copy, which a copy in background mode needs
//    (VersionSet::Close()).
//
// Rule: a sync-marked copy that the pointer names holds every acknowledged
// store commit. A crash or a failed copy append after the store commit can
// leave all or part of the last batch missing. RocksDB did not acknowledge
// that batch, and after a failed append the DB stopped. A rollback without the
// store reads the copy up to its end (ReadSyncCopyRecords()): it keeps every
// complete record and every complete atomic group, and drops a torn last
// record or an atomic group that the end cuts short. An atomic group with a
// wrong number of records or with a normal record inside it before the end,
// or a damaged record, fails the rollback with Corruption.

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
// `dir`, as SetCurrentFile() does for CURRENT. Sets `*rename_attempted` when
// it calls the rename. A rename that returns an error can still have
// happened, so after an attempt the caller keeps the copy that the pointer may
// name, whatever the result. On an error it deletes the temporary file, and a
// failed delete writes a WARN line to `info_log`.
IOStatus WriteManifestCopyPointer(FileSystem* fs, const std::string& dbname,
                                  uint64_t number,
                                  const WriteOptions& write_options,
                                  FSDirectory* dir, Logger* info_log,
                                  bool* rename_attempted);

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

// Returns a sequential file that reads `data`.
std::unique_ptr<FSSequentialFile> NewStringSequentialFile(std::string data);

// The store mode state of one DB instance that opened a DB for writing.
// VersionSet owns one in store mode (VersionSet::HasManifestStoreWriteState()).
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

  // The base rows: the DB row and the column family rows that the next batch
  // starts from (BuildManifestRowChanges()). File rows are not kept. They are
  // usually the rows of the last applied commit. When DB::Open() schedules a
  // timestamp rewrite (see SetRewrite()), PrepareRewrite() puts the recovered
  // column family rows here before the next commit, so these rows can be
  // ahead of the store until a commit with the timestamp rewrite applies.
  const ManifestRows& batch_base_rows() const { return batch_base_rows_; }

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

  // Puts the rewritten column family rows into the base rows
  // (batch_base_rows()), so that BuildManifestRowChanges() starts from them.
  // Call before building the row changes of a batch. This is safe also when
  // the commit then fails: the base rows differ from the store until a commit
  // applies, but every batch until then upserts the rewritten rows again
  // (MergeRewrite()), because only an applied commit clears the rewrite.
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
    // and reads the DB row once. With the default max_bgerror_resume_count
    // (INT_MAX), CommitBatch() retries after DB::Open() until the outcome is
    // known or the DB starts to close, and later MANIFEST writes wait in the
    // queue during that time. The auto-resume options are reused because they
    // already say how long the user lets RocksDB retry a failed background
    // write.
    int max_retries = 0;
    // The wait between two attempts (DBOptions::bgerror_resume_retry_interval).
    uint64_t retry_interval_us = 0;
    // The LOG of the DB, for the resend and stop messages. May be null.
    Logger* info_log = nullptr;
    // Returns true until DB::Open() succeeds (DBImpl::opening_), and when the
    // DB starts to close: from the start of DBImpl::CloseHelper() (before its
    // shutdown flush), or from the point in CancelAllBackgroundWork() after
    // its own shutdown flush (DBImpl::closing_). CommitBatch() checks it after
    // each failed ReadDbRow() or resend, so during an open or a close a commit
    // with an unknown outcome stops at the first such failure.
    std::function<bool()> opening_or_closing;
  };
  void SetRetryPolicy(RetryPolicy policy) { retry_policy_ = std::move(policy); }

  // Commits the row changes of one batch to the store. `in_flight_file_number`
  // is the value of DBImpl::MinFileNumberInUse(), without the caps by the
  // next file number of the commit and, in sync copy mode, by the deleted
  // file numbers (this function applies them; see "Sync copy retention" in
  // the overview above).
  //
  // Results (the store's own status is never returned as it is; the store's
  // retryable flag and scope have no effect, and only the text of the last
  // failed store call is added to the message):
  //
  //   First Commit()      RocksDB does                    CommitBatch() returns
  //   returns
  //   ------------------  ------------------------------  ---------------------
  //   OK                  updates the base rows           OK
  //   IOFenced            stops the DB                    IOFenced
  //   TryAgain            nothing (not applied)           retryable IOError
  //   NotSupported        nothing (not applied, never     IOError
  //                       will be), or one smaller send
  //                       (see below)
  //   Busy                reads the DB row, never         OK, IOFenced, or a
  //                       resends                         stop error
  //   other error         learns the outcome (below)      OK, IOFenced, or a
  //                                                       stop error
  //
  // - IOFenced: a newer epoch claimed the DB.
  // - The retryable IOError: the commit was not applied, and a retry is safe.
  //   When the caller of the MANIFEST write sets it as the background error
  //   (for example a flush or a compaction does), the error handler starts
  //   auto-resume, which commits an empty batch (DBImpl::ResumeImpl()).
  // - NotSupported with deletes of dropped column family rows: those deletes
  //   are optional, so RocksDB sends the batch once more without them, with a
  //   new commit id and the same expected commit sequence (see "Dropped
  //   column families" in the overview above). The result of that send is
  //   handled as the result of a first Commit() in this table.
  // - The IOError after NotSupported: the store rejects the commit for good,
  //   so a resend would get the same result. The store state does not stop
  //   (see "Stopped" below), because the stored commit sequence did not
  //   change. The DB background error is kept apart from the store state.
  //   When a flush or a compaction sets this IOError as the background error,
  //   the error handler makes it fatal, and DB::Resume() returns it without a
  //   store call. The DB must then be closed and opened again.
  // - A stop error is an IOError without the retryable flag (see below).
  //
  // Unknown outcome: any status other than OK, IOFenced, Busy, TryAgain or
  // NotSupported. The commit may still apply. RocksDB then reads the DB row:
  // if the commit sequence is the expected one plus 1 and the last commit id
  // is the id of this commit, the commit applied. If the commit sequence is
  // still the expected one, the commit had not applied when RocksDB read the
  // row, but it can still apply later. RocksDB then sends the same commit
  // again, with the same commit id and expected commit sequence. Whichever
  // of the two requests applies first raises the commit sequence, so the
  // other can never apply to the row. A claim also raises the commit
  // sequence, so after a claim neither request can apply.
  // Each resend, and each ReadDbRow() that repeats a failed one, is one retry.
  // RocksDB makes at most RetryPolicy::max_retries retries. It waits
  // RetryPolicy::retry_interval_us after a failed ReadDbRow() and after a
  // resend that fails with a status other than Busy. Busy from the first send
  // also leads to a ReadDbRow(), but never to a resend. After any send that
  // got Busy, a DB row that still holds the expected commit sequence stops
  // the DB.
  //
  // The loop, with E the expected commit sequence and "this" the commit id of
  // this commit:
  //
  //   send --- OK -----------------------------> applied
  //        --- IOFenced -----------------------> stop (IOFenced)
  //        --- TryAgain -----------------------> retryable IOError
  //        --- NotSupported -------------------> IOError
  //        --- Busy or other error ------------> read the DB row
  //
  //   read the DB row
  //     --- OK, seq == E+1 and id == this -----> applied
  //     --- OK, seq != E ----------------------> stop
  //     --- OK, seq == E, last send was Busy --> stop
  //     --- OK, seq == E, retries left --------> resend:
  //                                                OK: applied
  //                                                IOFenced: stop
  //                                                NotSupported: stop
  //                                                Busy: read again
  //                                                other (also TryAgain):
  //                                                  wait, read again
  //     --- IOFenced --------------------------> stop (IOFenced)
  //     --- NotFound --------------------------> stop
  //     --- other error, retries left ---------> wait, read again
  //
  // No retries left, or the DB is opening or starts to close after a failed
  // read or resend: stop.
  //
  // TryAgain and NotSupported end the loop only for the first send. A resend
  // that returns one of them did not apply, but the first send may still
  // apply. So after TryAgain RocksDB keeps reading the DB row within the
  // retry limit, and after NotSupported (which a further resend would get
  // again) it stops the DB.
  //
  // Stopped: when the outcome cannot be learned (the retry limit is reached,
  // the DB is opening or closing, a DB row with another commit, no DB row, or
  // Busy from a send while the commit sequence is unchanged), the DB is
  // stopped. Every later call returns the same error at once without a store
  // call. The error is a plain IOError: not retryable and without a file scope,
  // so the error handler makes it a fatal error and does not resume. This holds
  // also after an earlier TryAgain: VersionSet keeps the stop error in place of
  // the retryable one (VersionSet::io_status()), and DBImpl::ResumeImpl()
  // reports it. The next DB::Open() claims the DB, which raises the commit
  // sequence, so the old commit can never apply after that. During an open or a
  // close (RetryPolicy::opening_or_closing), a short store failure can stop the
  // DB, because RocksDB gives up at the first failed ReadDbRow() or resend. A
  // failed DB::Open() is also safe: the next DB::Open() claims the DB and
  // raises the commit sequence, so the old commit can never apply.
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
  // The commit sequence that the DB row holds after the last claim or applied
  // commit of this instance. The next commit sends this value as its expected
  // commit sequence, because the store adds 1 on success.
  uint64_t applied_commit_sequence_ = 0;
  ManifestRows batch_base_rows_;
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
