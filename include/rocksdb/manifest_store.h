//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// EXPERIMENTAL: the format and the interface in this header may still change.
//
// Background: RocksDB keeps its metadata (the live files of each level, the
// column families, the WAL state and the counters such as the next file number)
// in the MANIFEST file. An external metadata store can hold the same state as
// structured rows instead. Each row has a key and a set of fields, and each
// field has a stable small integer, its field ID. The rows hold resulting
// values, not changes, so a reader needs no history. A DB whose metadata lives
// in such a store is in store mode (DBOptions::manifest_store).
//
// This header defines the public form of the rows: the row types and keys,
// the field IDs of each row type, and the canonical byte encoding of the
// fields of a row. A store keeps the encoded fields as they are (a store
// without a schema), or maps the known field IDs to columns (a store with a
// fixed schema).
//
// Rows: one DB row per DB, one row per column family, one row per SST file,
// one row per blob file, and one row per WAL when the MANIFEST tracks WALs
// (DBOptions::track_and_verify_wals_in_manifest).
//
// Schema evolution:
// - A field ID is never reused, and its "must understand" (MU) status never
//   changes. A reader (the RocksDB code that reads rows) drops an unknown
//   field without the MU bit. For an unknown field with the MU bit it returns
//   NotSupported, because a newer binary wrote the row and a reader must not
//   misread it.
// - A store keeps every field it receives, also fields it does not know, so
//   that the fields of a newer binary are not lost.
// - The DB row holds a row format version. It changes only for a change of
//   the layout, such as a new row type or a new key layout.
// - New MU fields belong to a feature version. RocksDB writes them only when a
//   DB option allows the feature version, so that a binary rollback stays safe
//   until the option is raised. New fields without the MU bit need no feature
//   version, because an older reader drops them.
// - Why a per-field MU bit and not a version bump for each new field: the rows
//   are independent records, like the records of a MANIFEST file, and a row
//   holds a field only when its value is not the default. An older reader then
//   fails only when a row really holds a field that it must understand. When
//   no row holds such a field, it reads all rows. This is the MANIFEST rule for
//   new SST file fields (kCustomTagNonSafeIgnoreMask, the same bit).
// - An older reader drops unknown fields without the MU bit. If it later
//   writes such a row again, those fields are lost, even though the store kept
//   them.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/io_status.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct DBOptions;

// The type of a row. Stores keep these values, so they never change.
enum class ManifestRowType : uint8_t {
  // The row with the fields of the whole DB. Its key has no other part.
  kDb = 1,
  // The row of one column family. Key: cf_id.
  kColumnFamily = 2,
  // The row of one SST file. Key: cf_id, and the file number in `number`.
  kFile = 3,
  // The row of one blob file. Key: cf_id, and the blob file number in
  // `number`.
  kBlobFile = 4,
  // The row of one WAL. Key: the WAL number in `number`.
  kWal = 5,
};

// The key of a row inside the current set of rows of one DB. The store adds
// two parts that RocksDB does not send: the app's key of the DB (chosen by the
// app; it is not the RocksDB db id that the DB row holds) and the generation
// (see "Terms" in the store interface part below). Each store maps the key to
// its own schema. Parts that a row type does not use are 0.
struct ManifestRowKey {
  ManifestRowType type = ManifestRowType::kDb;
  uint32_t cf_id = 0;
  uint64_t number = 0;

  bool operator==(const ManifestRowKey& other) const = default;
};

// One row: its key, and its fields in the canonical encoding described below.
struct ManifestRow {
  ManifestRowKey key;
  std::string fields;
};

// Which source holds the authoritative metadata of a DB. The DB row stores
// it in ManifestDbField::kAuthority. These values and the ManifestCopyMode
// values are fixed because stores keep them. RocksDB writes kShadow in shadow
// mode (DBOptions::manifest_store_shadow). A reader returns NotSupported for an
// authority value that it does not know, because a newer binary wrote it.
enum class ManifestAuthority : uint8_t {
  // The MANIFEST file, as without a store.
  kFile = 0,
  // The MANIFEST file. The value means that RocksDB also writes the rows to
  // the store in the background, so that the two can be compared.
  kShadow = 1,
  // The rows in the store.
  kStore = 2,
};

// Whether RocksDB also writes a copy of the metadata as a MANIFEST file when
// the store is the authority, so that RollBackManifestStore() can roll the DB
// back without the store. The DB row stores it, in ManifestDbField::kCopyMode.
// DBOptions::manifest_store_copy_mode sets it when a DB is created in the
// store, enters store mode, or opens with manifest_store_migrate. A reader
// returns NotSupported for a copy mode value that it does not know, because a
// newer binary wrote it.
enum class ManifestCopyMode : uint8_t {
  // No copy.
  kOff = 0,
  // The value means that RocksDB writes the copy after the store commit, so
  // the copy may be behind.
  kBackground = 1,
  // The value means that RocksDB appends and syncs the copy after each store
  // commit, and installs the change only after both writes succeed. The copy
  // is equal to the store or one store commit behind.
  kSync = 2,
};

// Bit 6 of a field ID is the "must understand" (MU) bit. So the IDs 64 to 127,
// 192 to 255 and so on are MU fields, and the other IDs are not. A MANIFEST
// file uses the same rule and the same bit for the custom fields of a new SST
// file record.
constexpr uint32_t kManifestFieldMustUnderstand = 1u << 6;

// The field IDs of each row type. An ID is unique inside its row type, and
// field ID 0 is never used. All IDs below belong to feature version 1 and have
// no MU bit: every reader of row format version 1 knows them, so the bit would
// change nothing for them.
//
// Each comment gives the value encoding and the default, or the meaning of a
// missing field. "Canonical encoding" below defines the value encodings
// (varint32, varint64, bool, enum, bytes, ikey and lists). A writer leaves out
// a field that holds its default. "Always present" means the field has no
// default: a writer always writes it, and a reader returns Corruption if it is
// missing. A field is always present when the matching MANIFEST record always
// writes it, also when its value can be 0 ("0 means unknown"). A comment that
// says "when absent, X" marks an optional field: a writer leaves it out when it
// has no value, and X is the meaning of a missing field.

// The fields of the DB row that RocksDB owns. The store owns other values of
// the DB row, which are not fields:
//
//   DB row = RocksDB-owned fields   ManifestDbField (encoded in
//                                   ManifestRow::fields)
//          + store-owned values     ManifestStoreDbState (commit sequence,
//                                   last commit id, generation)
//
// Who writes each part, and in which type:
// - RocksDB writes all of its fields in ManifestCommit::db_row and
//   ManifestGeneration::db_row, and some of them at a claim in
//   ManifestStoreOpenOptions::db_field_updates.
// - The store sets its values at each commit and claim.
// - ReadDbRow(), Open() and Scan() return both parts in a
//   ManifestDbRowResult.
enum class ManifestDbField : uint32_t {
  // varint32, always present. The row format version of the rows. This
  // release always writes kManifestRowFormatVersionLatest (1). A reader
  // returns NotSupported when the value is larger than the version it knows.
  kRowFormatVersion = 1,
  // enum ManifestAuthority, always present.
  kAuthority = 2,
  // enum ManifestCopyMode, always present.
  kCopyMode = 3,
  // varint64, default 0 (no copy file). The number of the current MANIFEST
  // file copy, as information for operators. RocksDB does not read it (it
  // reads the copy pointer file), and it can be one commit behind.
  kCopyFileNumber = 4,
  // bytes, default empty. The RocksDB db id, as in the IDENTITY file.
  kDbId = 5,
  // varint64, always present. The next file number.
  kNextFileNumber = 6,
  // varint64, always present. The smallest file number that RocksDB may still
  // use for a file that the rows do not name yet (the output of a running
  // flush, compaction or external file ingestion, or a blob file of blob
  // direct write), or the next file number when there is none. It can be
  // lower than the smallest such file, because it also keeps files that event
  // listeners are still told about. It is never larger than the next file
  // number. A service that deletes unused files must keep every file whose
  // number is at or above this number. The value 0 means that the number is
  // not known, and the service must keep every file.
  kInFlightFileNumber = 7,
  // varint64, always present. The last sequence number in the metadata.
  kLastSequence = 8,
  // varint64, default 0. A legacy WAL number that recovery still reads.
  kPrevLogNumber = 9,
  // varint64, default 0. WALs with a smaller number are not needed.
  kMinLogNumberToKeep = 10,
  // varint32, default 0. The largest column family id ever used.
  kMaxColumnFamily = 11,
  // varint64; when absent, no OPTIONS file number is recorded. The OPTIONS
  // file that describes the DB state.
  kCommittedOptionsFileNumber = 12,
};

// The fields of a column family row.
enum class ManifestCfField : uint32_t {
  // bytes, always present. The column family name.
  kName = 1,
  // bytes, always present. The name of the user comparator.
  kComparatorName = 2,
  // varint32, default 0. The size of the user-defined timestamp in each user
  // key. A reader needs it to find the timestamp at the end of the user key
  // in each ikey of this column family.
  kTimestampSize = 3,
  // bool, default true. ColumnFamilyOptions::persist_user_defined_timestamps.
  kPersistUserDefinedTimestamps = 4,
  // varint64, default 0. WALs with a smaller number hold no data of this
  // column family that is not in an SST file yet.
  kLogNumber = 5,
  // bytes; when absent, it is not set. The full_history_ts_low timestamp.
  kFullHistoryTsLow = 6,
  // List; when absent, no level has a cursor. The round-robin compaction
  // cursors: repeated { varint32 level; varint32 size of the ikey, including
  // its 8-byte trailer; ikey }, in increasing level order.
  kCompactCursors = 7,
  // List; when absent, there are none. The blob files that a best-efforts
  // BlobDB repair left out: repeated varint64, strictly increasing.
  kDroppedBlobFiles = 8,
  // bool, default false. True after the column family is dropped.
  kDropped = 9,
};

// The fields of an SST file row.
enum class ManifestFileField : uint32_t {
  // varint32, default 0.
  kLevel = 1,
  // varint32, default 0. The index into the SST paths of the column family:
  // ColumnFamilyOptions::cf_paths, or DBOptions::db_paths when cf_paths is
  // empty. RocksDB keeps the path id of a file in 2 bits, so the value is 0
  // to 3.
  kPathId = 2,
  // varint64, always present.
  kFileSize = 3,
  // ikey, always present.
  kSmallest = 4,
  // ikey, always present.
  kLargest = 5,
  // varint64, always present.
  kSmallestSeqno = 6,
  // varint64, always present.
  kLargestSeqno = 7,
  // bool, default false.
  kMarkedForCompaction = 8,
  // varint64, default 0 (no blob file).
  kOldestBlobFileNumber = 9,
  // varint64, always present (0 means unknown).
  kOldestAncestorTime = 10,
  // varint64, always present (0 means unknown).
  kFileCreationTime = 11,
  // varint64, always present (0 means unknown). The L0 epoch number, which
  // orders the L0 files. It is not related to the store epoch below.
  kEpochNumber = 12,
  // bytes, default empty.
  kFileChecksum = 13,
  // bytes, default "Unknown" (kUnknownFileChecksumFuncName).
  kFileChecksumFuncName = 14,
  // enum Temperature (include/rocksdb/types.h), default Temperature::kUnknown.
  // Like a MANIFEST file reader, a reader reads a value that is not below
  // Temperature::kLastTemperature as kUnknown.
  kTemperature = 15,
  // bytes, default empty.
  kMinTimestamp = 16,
  // bytes, default empty.
  kMaxTimestamp = 17,
  // 16 bytes that a store keeps as opaque bytes: two little-endian 64-bit
  // integers. Default all zero (no unique id).
  kUniqueId = 18,
  // varint64, default 0.
  kCompensatedRangeDeletionSize = 19,
  // varint64, default 0.
  kTailSize = 20,
  // bool, default true.
  kUserDefinedTimestampsPersisted = 21,
  // bytes, default empty. Opaque metadata that a file system can use to open
  // the file faster.
  kFileOpenMetadata = 22,
  // varint64, default 0 (unknown).
  kDbOwnershipStartTime = 23,
  // varint32, default 0. The packed DataFileProvenance
  // (include/rocksdb/metadata.h, DataFileProvenance::DecodeFrom()), kept as
  // it is.
  kDataFileProvenance = 24,
};

// The fields of a blob file row.
enum class ManifestBlobFileField : uint32_t {
  // varint64, always present.
  kTotalBlobCount = 1,
  // varint64, always present.
  kTotalBlobBytes = 2,
  // bytes, default empty.
  kChecksumMethod = 3,
  // bytes, default empty.
  kChecksumValue = 4,
  // varint64, default 0. The total garbage in the file.
  kGarbageBlobCount = 5,
  // varint64, default 0.
  kGarbageBlobBytes = 6,
};

// The fields of a WAL row.
enum class ManifestWalField : uint32_t {
  // varint64; when absent, the synced size is unknown. 0 is a valid size.
  kSyncedSize = 1,
};

// The row format version that this release reads. A reader returns
// NotSupported for a DB row whose row format version is larger.
constexpr uint32_t kManifestRowFormatVersionLatest = 1;

// The newest feature version that this release can write. Each MU field that
// a later release adds belongs to one feature version, and RocksDB writes it
// only when the feature version that a DB option allows is at least that
// version. All fields in this header belong to feature version 1.
constexpr uint32_t kManifestRowFeatureVersionLatest = 1;

// Canonical encoding: the fields of a row are encoded as
//   repeated { varint32 field_id; varint32 value_size; value bytes }
//
//   +----------+------------+-------+     +----------+------------+-------+
//   | field id | value size | value | ... | field id | value size | value |
//   | varint32 | varint32   | bytes |     | varint32 | varint32   | bytes |
//   +----------+------------+-------+     +----------+------------+-------+
//
// For example, field 6 with the varint64 value 10 is the 3 bytes 0x06 0x01
// 0x0A.
// - Fields are in increasing ID order, and each ID appears at most once.
// - A field that holds its default is left out. A parser accepts a field that
//   holds its default.
// - A writer uses the fewest bytes for every varint, also inside values.
//   ParseManifestRowFields() rejects a longer form of a field ID or a value
//   size, and the RocksDB reader rejects a longer form inside a value.
// So equal rows give equal bytes, and a store can compare or checksum the
// bytes. A store with a fixed schema keeps an absent field as NULL, and
// rebuilds the bytes in ID order.
//
// The value encodings are defined byte by byte, so that a store adapter can
// decode them without RocksDB code:
// - varint32 and varint64: unsigned LEB128. Each byte holds 7 bits of the
//   value, the lowest bits first, and its high bit is 1 when another byte
//   follows. A varint32 takes at most 5 bytes, and a varint64 at most 10.
// - bool: one byte, 0 or 1.
// - enum: one byte that holds the enum value.
// - bytes: the raw bytes. The value size gives the length.
// - ikey: an internal key: the user key bytes (with the user-defined
//   timestamp, if the column family has one), then 8 bytes that hold
//   (sequence number << 8 | value type) as a little-endian 64-bit integer.
// - Lists: as described at each field.

// One field of an encoded row. `value` points into the encoded bytes.
struct ManifestRowField {
  uint32_t id = 0;
  Slice value;
};

// Parses the canonical encoding in `encoded` into `fields`. It clears `fields`
// first. Each value points into `encoded`, which must outlive `fields`.
// Returns Corruption if the bytes end inside a field, if a field ID or a value
// size is not a varint32 in its shortest form, if a field ID is 0, or if the
// IDs are not in increasing order (which includes a repeated ID). On
// Corruption, `fields` may hold the fields before the bad one. It does not
// check whether it knows the IDs or whether the values are valid.
Status ParseManifestRowFields(const Slice& encoded,
                              std::vector<ManifestRowField>* fields);

// Returns the canonical encoding of `fields`. The IDs must be larger than 0
// and in increasing order. Each value must be smaller than 4 GiB, because the
// value size is a varint32. The caller leaves out fields that hold their
// default.
std::string EncodeManifestRowFields(
    const std::vector<ManifestRowField>& fields);

// ======== The store interface ========
//
// Background: an application implements ManifestStore on top of its own
// database (for example a replicated key-value store) and gives one object to
// RocksDB in DBOptions::manifest_store. The DB is then in store mode: the
// rows in the store hold its metadata. With DBOptions::manifest_store_shadow
// the DB stays in file mode instead: the MANIFEST file stays the authority, and
// RocksDB copies each MANIFEST write to the store in the background, as
// described at that option. The types below define what a store must do. The
// application creates a store object for one DB, with its own key for the DB
// and the epoch.
//
// Terms:
// - Epoch (store epoch): a number that the application gives to a store
//   object when it creates it, one per DB instance that takes over the DB.
//   The store keeps the newest epoch that opened or created the DB, and
//   refuses requests of an object with an older epoch ("fenced"). RocksDB
//   never sees the epoch. An object made only for reading, for example for a
//   secondary instance, may have no epoch.
// - Claim: Open(), and the Commit() that creates the DB row. A claim makes
//   the epoch of this object the stored epoch and raises the commit sequence.
//   A claim with the same epoch as the stored one is allowed.
// - Commit sequence: a number in the DB row that grows by 1 with each
//   successful commit or claim. A commit names the commit sequence it
//   expects, so two writers cannot both apply a change on the same state.
// - Commit id: a unique string that RocksDB gives each commit and claim. The
//   store writes the id of the last applied one into the DB row, so after an
//   unknown outcome RocksDB can read the DB row and learn whether its request
//   was applied.
// - Generation: a full copy of all rows other than the DB row. A store keeps
//   rows of one current generation. A new generation replaces all rows at
//   once, for example when RocksDB copies the rows of an existing DB into the
//   store. WriteGeneration() writes a new generation.
//
// Call order: RocksDB uses a store in store mode (DBOptions::manifest_store)
// as follows, and a store must support it:
// 1. Create a DB: Commit() with expected_commit_sequence 0 writes the DB row
//    and the first rows.
// 2. Open a DB: Open() claims the DB, then Scan() reads all rows.
//    DB::OpenForReadOnly() and DB::ListColumnFamilies() use only Scan(), with
//    a consistent scan, so they work with an object without an epoch.
// 3. Each metadata change (what RocksDB writes to the MANIFEST file when there
//    is no store): one Commit() with the expected commit sequence.
// 4. Readers that follow the primary: DB::OpenAsSecondary(),
//    DB::OpenAsFollower() and the remote compaction worker, which gets the
//    store from CompactionServiceOptionsOverride::manifest_store in
//    DB::OpenAndCompact(), use a consistent Scan() at open. To catch up, a
//    secondary and a follower call ReadDbRow(), and Scan() again only when the
//    commit sequence changed. They never claim the DB, so they work with an
//    object without an epoch.
// Other cases:
// - After Busy or an unknown outcome: ReadDbRow(), and compare
//   last_commit_id with the commit id of the request.
// - Move an existing DB into the store: WriteGeneration().
// - DestroyDB(): Destroy(), then RocksDB deletes the files. After a rollback,
//   DestroyDB() of the DB in file mode also calls Destroy() when the rows have
//   the db id of the DB and authority kFile. DestroyDB() keeps the rows that
//   shadow mode wrote (authority kShadow); call Destroy() once no DB uses
//   them.
//
// Not supported in store mode: DB::GetLiveFiles() (the MANIFEST file is only
// a stub, so DB::GetLiveFilesStorageInfo() gives the MANIFEST content in
// memory instead), RepairDB(), and the tools that read or rewrite the
// MANIFEST file offline. They return NotSupported.

// The DB row values that the store owns. The store sets them in each
// successful commit and claim, and RocksDB never writes them.
struct ManifestStoreDbState {
  // Grows by 1 with each successful commit or claim. A stored DB row has a
  // commit sequence of at least 1.
  uint64_t commit_sequence = 0;
  // The commit id of the last successful commit or claim.
  std::string last_commit_id;
  // The current generation. The store chooses its value.
  uint64_t generation = 0;
};

// The store-owned values and the DB row as RocksDB wrote it.
struct ManifestDbRowResult {
  ManifestStoreDbState state;
  // key.type is ManifestRowType::kDb; the fields are the RocksDB-owned ones.
  ManifestRow db_row;
};

// The input of ManifestStore::Open().
struct ManifestStoreOpenOptions {
  // The commit id of the claim.
  std::string commit_id;
  // DB row fields that the claim writes, in the canonical encoding. They let a
  // change that must happen together with the claim, for example a new
  // ManifestDbField::kAuthority value, apply in the same atomic step. The store
  // sets each of these fields in the stored DB row. If the row has a field with
  // the same ID, the store replaces it. If not, the store adds the field. An
  // empty value removes the field, which means the default. This merge is the
  // only place where a store must parse fields.
  std::string db_field_updates;
};

// One atomic change of the rows.
struct ManifestCommit {
  // The commit applies only if the stored commit sequence equals this value.
  // 0 means "create the DB row": it applies only if no DB row exists.
  uint64_t expected_commit_sequence = 0;
  // Written as last_commit_id when the commit applies.
  std::string commit_id;
  // The whole new DB row (the RocksDB-owned fields).
  ManifestRow db_row;
  // Rows to write in the current generation, and rows to delete. A commit
  // applies all of them, or none. A key appears at most once across
  // `upserts` and `deletes`.
  std::vector<ManifestRow> upserts;
  std::vector<ManifestRowKey> deletes;
};

// The input of ManifestStore::Scan().
struct ManifestScanOptions {
  // True for a reader that is not the DB instance that claimed the DB, such
  // as a secondary instance. The store then returns the rows of one commit,
  // not a mix of rows from before and after a commit that runs during the
  // scan. The instance that claimed the DB is its only writer, so no commit
  // runs during its own scan, and it can use false to let the store skip the
  // cost of a consistent read.
  bool consistent = false;
};

// Receives the rows of ManifestStore::Scan().
class ManifestRowVisitor {
 public:
  virtual ~ManifestRowVisitor() = default;

  // Called once, before any other row.
  virtual Status OnDbRow(const ManifestDbRowResult& db_row) = 0;

  // Called for each column family, file, blob file and WAL row of the current
  // generation, in any order. A non-OK status stops the scan, and Scan()
  // returns it.
  virtual Status OnRow(const ManifestRow& row) = 0;
};

// The input of ManifestStore::WriteGeneration().
struct ManifestGeneration {
  // As in ManifestCommit, for the commit that switches to the new generation.
  uint64_t expected_commit_sequence = 0;
  std::string commit_id;
  ManifestRow db_row;
  // All column family, file, blob file and WAL rows of the new generation.
  // Each key appears at most once.
  std::vector<ManifestRow> rows;
};

// Results of Commit() and WriteGeneration(). The status gives the meaning.
// Check it with IsIOFenced(), IsBusy() and IsTryAgain(). IOFenced is code
// kIOError with subcode kIOFenced.
// - OK: applied.
// - IOFenced: the stored epoch is newer than the epoch of this object. Not
//   applied, and never will be.
// - Busy: the stored commit sequence is not the expected one. Not applied,
//   and never will be.
// - TryAgain: not applied, never will be, and safe to retry. IOStatus has no
//   TryAgain() factory, so a store builds it with
//   status_to_io_status(Status::TryAgain()).
// - Any other status (for example TimedOut or IOError): the outcome is
//   unknown. The change may still be applied later.
// An object without an epoch only reads. Its Commit(), WriteGeneration(),
// Open() and Destroy() calls return NotSupported (Open() returns NotFound
// first when there is no DB row) and change nothing.
//
// Calls: RocksDB makes at most one call at a time on the store object of one
// DB instance. In shadow mode these calls come from a background thread, and
// the last commit at close from the thread that closes the DB.
// DB::ListColumnFamilies(), RollBackManifestStore() and DestroyDB() call the
// store from the caller's thread, so do not pass them a store object that an
// open DB uses. Every call must return in a bounded time (the store applies its
// own timeout), because DB::Open() and DB::Close() wait for calls in flight. A
// method added to this class has a default implementation that
// returns NotSupported, so that existing stores still
// build.
class ManifestStore : public Customizable {
 public:
  ~ManifestStore() override = default;
  static const char* Type() { return "ManifestStore"; }

  // Creates a store from `value`, the name or the ID of a registered store
  // class (see ObjectRegistry). RocksDB has no built-in store.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<ManifestStore>* result);

  // Open() does not open a connection. It claims the DB for the epoch of this
  // object. Results:
  // - OK: claimed. `result` holds the state after the claim: the commit
  //   sequence is 1 larger, last_commit_id is `opts.commit_id`, and
  //   `opts.db_field_updates` are merged.
  // - NotFound: the store has no DB row. Nothing is claimed.
  // - IOFenced: the stored epoch is newer. Nothing is claimed.
  // - Any other status: the outcome is unknown. Calling Open() again is safe.
  // A claim with the same epoch as the stored one is allowed. Like every
  // claim, it raises the commit sequence, so requests that expect the old
  // commit sequence get Busy.
  virtual IOStatus Open(const ManifestStoreOpenOptions& opts,
                        ManifestDbRowResult* result) = 0;

  // Applies `commit` atomically. Results as listed above the class. A commit
  // with expected_commit_sequence 0 creates the DB row: it returns Busy when
  // a DB row exists. A commit with a larger expected value returns Busy when
  // no DB row exists. The creating commit sets the commit sequence to 1.
  virtual IOStatus Commit(const ManifestCommit& commit) = 0;

  // Reads the latest committed DB row, without a claim and without reading
  // other rows. A caller uses it after Busy or an unknown outcome (see "Call
  // order" above). Results: OK; IOFenced, when this object has an epoch and the
  // stored epoch is newer; NotFound (no DB row); any other status (nothing is
  // known; calling it again is safe).
  virtual IOStatus ReadDbRow(ManifestDbRowResult* result) = 0;

  // Calls `visitor` for the DB row, then for each other row of the current
  // generation. Results: OK; NotFound (no DB row); IOFenced, when this object
  // has an epoch and the stored epoch is newer; the non-OK status of the
  // visitor; any other status (the scan failed; calling it again is safe).
  virtual IOStatus Scan(const ManifestScanOptions& opts,
                        ManifestRowVisitor* visitor) = 0;

  // Writes the rows of a new generation, then switches to it with a commit
  // that checks the row count and a checksum of the rows, so that a partly
  // written generation never becomes current. The store computes the row
  // count and the checksum of the rows in the request. At the switch it
  // compares them with the rows it wrote. When they differ, the switch is not
  // applied and WriteGeneration() returns TryAgain. The store writes in chunks,
  // and refuses each chunk of an object with an older epoch. The store chooses
  // the generation number, unique for each attempt, and returns it in
  // `generation` on OK. Other results as for Commit().
  virtual IOStatus WriteGeneration(const ManifestGeneration& request,
                                   uint64_t* generation) = 0;

  // Removes all rows of this DB. DestroyDB() calls it for a DB in store mode,
  // and for a DB in file mode whose rows a rollback left in the store.
  // Results: OK, also when there is no DB row, so that a caller can repeat a
  // DestroyDB() that stopped after this call; NotSupported for an object
  // without an epoch, also when there is no DB row; IOFenced; any other status
  // (calling it again is safe).
  virtual IOStatus Destroy() = 0;
};

// Moves a DB in store mode back to file mode: afterwards the MANIFEST file
// holds the metadata, and DB::Open() works without `manifest_store`. Stop
// every instance of the DB before the call. Pass a store object with a new
// epoch in `db_options.manifest_store`, or null when the store cannot be
// reached. The DB folder lock is held during the call.
//
// What it does, by what CURRENT names:
//
//   CURRENT names     With the store                Without the store
//   ----------------  ----------------------------  -------------------------
//   "done" stub       new MANIFEST file from the    new MANIFEST file from the
//                     rows; DB row set to kFile     sync-marked copy
//   "entering" stub   CURRENT back to the MANIFEST  InvalidArgument
//                     file if the switch did not
//                     happen; else InvalidArgument
//                     (open the DB read-write to
//                     finish)
//   "creating" stub   InvalidArgument (open the DB read-write to finish)
//   MANIFEST file     DB row set to kFile; copies   OK, nothing to do
//                     deleted
//
// With the store, the function first checks that the DB row holds the db id
// of this DB, so that a store object of another DB returns InvalidArgument.
// The db id comes from the stub, or for a MANIFEST file from its records,
// else from the IDENTITY file. Without a db id it returns InvalidArgument.
// Then it claims the DB (which fences an older primary in the store), writes
// a new MANIFEST file from the rows, points CURRENT to it, sets the authority
// of the DB row to ManifestAuthority::kFile, and deletes the stub and the
// MANIFEST file copies.
//
// Without the store, it writes the new MANIFEST file from the MANIFEST file
// copy, which must have been written in ManifestCopyMode::kSync. It cannot
// claim the DB, so it relies only on the folder lock and on the application
// having stopped the old primary. It leaves the DB row, the stub and the
// copies; a later call with the store updates the DB row and deletes the
// copies, and keeps the MANIFEST file. The next file-mode DB::Open() deletes
// the old stub, as it deletes any obsolete MANIFEST file. Until that later
// call, the DB row still says that the store is the authority, so DB::Open()
// with manifest_store_migrate refuses to move the DB into the store again.
//
// The end of the copy can be torn by a crash or a failed append. The function
// cuts the copy at the last complete atomic group: it drops a record that the
// crash cut short, and the records of an atomic group that is not complete.
// Such a group belongs to a commit that was never acknowledged, because in
// sync mode a commit is acknowledged only after its copy append. Any other
// damage (a record with a bad checksum, or a record that does not decode)
// returns Corruption, because the damaged part may hold acknowledged commits.
//
// It also undoes a DB::Open() that stopped while it moved a DB into the
// store, when the store shows that the switch did not happen.
//
// Results: OK; InvalidArgument when the DB is not in a state that this
// function handles (for example a background copy without the store);
// Corruption when the stub, the DB row and the files do not agree, or the
// copy is damaged; or the error of a file or store operation.
Status RollBackManifestStore(const DBOptions& db_options,
                             const std::string& dbname);

}  // namespace ROCKSDB_NAMESPACE
