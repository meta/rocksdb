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
// values, not changes, so a row reader needs no history. A DB whose metadata
// lives in such a store is in store mode (DBOptions::manifest_store).
//
// Which source recovery trusts: recovery starts from the file that CURRENT
// names in the DB folder.
// - A normal MANIFEST file: file mode. Recovery reads that file.
// - A done stub: store mode. Recovery reads the rows in the store.
// - A creating or entering stub: a create or a move into the store that has
//   not finished. A read-write open with the store finishes it. For an
//   entering stub, the authority in the DB row says whether the switch
//   happened.
// The authority field of the DB row (ManifestAuthority) records which source
// is the authority for the metadata of a DB. Its values are listed below.
// This release supports kFile and kStore; the value of kShadow (1) is
// reserved, so that stores can keep it. A rollback without the store
// (RollBackManifestStore()) can leave a kStore row while CURRENT names the
// restored MANIFEST file. Recovery then reads that file, and a later rollback
// with the store sets the row to kFile.
// - kFile: the MANIFEST file is the authority, and recovery reads that file.
//   CURRENT names it, or, during a move into the store that has not finished,
//   CURRENT names an entering stub, and that stub names the MANIFEST file.
// - kShadow: the MANIFEST file is the authority, and RocksDB also writes the
//   rows to the store, so that the two can be compared.
// - kStore: the rows in the store are the authority. CURRENT names a short
//   stub MANIFEST file that identifies the DB, and recovery reads the metadata
//   from the store. ManifestCopyMode says whether RocksDB also keeps a copy of
//   the metadata as a MANIFEST file.
//
// Who uses this header: an application implements the ManifestStore class
// (see "The store interface" below) to read and update the metadata rows of
// one DB in its own database. This header defines what such a store needs:
// the public form of the rows (the row types and keys, the field IDs of each
// row type, and the canonical byte encoding of the fields of a row), the
// ownership protocol (epochs, claims and commit sequences), and the store
// methods. "Call order" below gives the order of reads, claims and commits. A
// store keeps the encoded fields as they are (a store without a schema), or
// maps the known field IDs to columns (a store with a fixed schema).
//
// Rows: one DB row per DB, one row per column family, one row per SST file,
// one row per blob file, and one row per WAL when the MANIFEST tracks WALs
// (DBOptions::track_and_verify_wals_in_manifest).
//
// Schema evolution:
// - A field ID is never reused, and its "must understand" (MU) status never
//   changes. A row reader (the RocksDB code that decodes rows) drops an unknown
//   field without the MU bit. For an unknown field with the MU bit it returns
//   NotSupported, because a newer binary wrote the row and a row reader must
//   not misread it.
// - A store keeps every field it receives, also fields it does not know.
//   This keeps the fields of a newer binary as long as no older binary writes
//   the row. An older row reader drops unknown fields without the MU bit, so
//   when an older binary writes the row again, those fields are lost.
// - The DB row holds a row format version. It changes only for a change of
//   the layout, such as a new row type or a new key layout.
// - New MU fields belong to a feature version. RocksDB writes them only when a
//   DB option allows the feature version, so that a binary rollback stays safe
//   until the option is raised. New fields without the MU bit need no feature
//   version, because an older row reader drops them.
// - Why a per-field MU bit and not a version bump for each new field: the rows
//   are independent records, like the records of a MANIFEST file, and a row
//   holds a field only when its value is not the default. An older row reader
//   then fails only when a row really holds a field that it must understand.
//   When no row holds such a field, it reads all rows. This is the MANIFEST
//   rule for new SST file fields (kCustomTagNonSafeIgnoreMask, the same bit).

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
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

// The key of a row inside the current set of rows of one DB. The store
// identifies the DB by the app's key of the DB, which RocksDB does not send
// (the app chooses it; it is not the RocksDB db id that the DB row holds). The
// store keeps one DB row under that key, outside of any generation. It keeps
// the column family, file, blob file and WAL rows by generation (see "Terms" in
// the store interface part below), and a generation switch changes the DB row
// and its current generation together. Each store maps the key to its own
// schema. Parts that a row type does not use are 0.
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

// Which source holds the authoritative metadata of a DB. The DB row stores it
// in ManifestDbField::kAuthority. These values and the ManifestCopyMode values
// are fixed because stores keep them. RocksDB writes kFile and kStore, not
// kShadow. A row reader returns NotSupported for an authority value that it
// does not know, because a newer binary wrote it.
enum class ManifestAuthority : uint8_t {
  // The MANIFEST file, as without a store.
  kFile = 0,
  // The MANIFEST file is the authority. RocksDB also writes the rows to the
  // store in the background, so that the two can be compared.
  kShadow = 1,
  // The rows in the store.
  kStore = 2,
};

// Whether RocksDB also writes a copy of the metadata as a MANIFEST file when
// the store is the authority, so that RollBackManifestStore() can roll the DB
// back without the store (only a kSync copy can be used for that). The DB row
// stores it, in ManifestDbField::kCopyMode.
// DBOptions::manifest_store_copy_mode sets it when a DB is created in the
// store, enters store mode, or opens with manifest_store_migrate. A row reader
// returns NotSupported for a copy mode value that it does not know, because a
// newer binary wrote it.
//
// The authority and the copy mode are two separate fields of the DB row:
//
//   authority  copy mode                 who holds the metadata
//   kFile      any value, not used       the MANIFEST file only
//   kShadow    any value, not used       the MANIFEST file; the rows are also
//                                        written to the store
//   kStore     kOff, kBackground, kSync  the store; the copy mode says whether
//                                        RocksDB also writes a MANIFEST copy
//
// The copy mode and ManifestDbField::kCopyFileNumber describe the MANIFEST
// copy, so they have a meaning only when the authority is kStore. With kFile
// or kShadow, a DB row may hold any copy mode and copy file number, and
// RocksDB does not use them. The row reader accepts every pair of known values.
enum class ManifestCopyMode : uint8_t {
  // No copy.
  kOff = 0,
  // The thread that commits to the store appends the edits of each commit to
  // the copy after the store commit, without a sync, so a crash can lose the
  // last appends. RocksDB syncs each new copy when it writes it, and syncs the
  // copy when the DB closes. Such a copy cannot take the DB back to the
  // MANIFEST file as the authority without the store.
  kBackground = 1,
  // The thread that commits to the store appends and syncs the copy after each
  // store commit, and installs the change only after both writes succeed. So
  // the synced copy holds all metadata that the DB installed. It can lack one
  // store commit: for example, the next store commit can apply, and the process
  // can stop before it appends that batch to the copy. The copy then lacks a
  // batch that the DB never installed. Such a copy can take a closed DB back to
  // the MANIFEST file as the authority without the store, after the application
  // has stopped the DB instance that wrote to the store.
  kSync = 2,
};

// Bit 6 of a field ID is the "must understand" (MU) bit. So the IDs 64 to 127,
// 192 to 255 and so on are MU fields, and the other IDs are not. A MANIFEST
// file uses the same rule and the same bit for the custom fields of a new SST
// file record.
constexpr uint32_t kManifestFieldMustUnderstand = 1u << 6;

// The field IDs of each row type. An ID is unique inside its row type, and
// field ID 0 is never used. All IDs below belong to feature version 1 and have
// no MU bit: every row reader of row format version 1 knows them, so the bit
// would change nothing for them.
//
// Each comment gives the value encoding and the default, or the meaning of a
// missing field. "Canonical encoding" below defines the value encodings
// (varint32, varint64, bool, enum, bytes, ikey and lists). A writer leaves out
// a field that holds its default. "Always present" means the field has no
// default: a writer always writes it, and a row reader returns Corruption if it
// is missing. Some fields are always present even when their value can be 0,
// because 0 has a meaning there (for example kOldestAncestorTime, where 0 means
// unknown, as in a MANIFEST record). The comment of each field says whether it
// is always present. A comment that says "when absent, X" marks an optional
// field: a writer leaves it out when it has no value, and X is the meaning of a
// missing field.

// The fields of the DB row that RocksDB owns. The DB row also has store-owned
// values; see ManifestStoreDbState in the store interface part below.
enum class ManifestDbField : uint32_t {
  // varint32, always present. The row format version of the rows. This release
  // always writes kManifestRowFormatVersionLatest (1). A row reader returns
  // NotSupported when the value is larger than the version it knows.
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
  // number. An outside service of the application that deletes unused SST and
  // blob files (not RocksDB itself) must keep every file whose number is at or
  // above this number. The value 0 means that the number is not known, and the
  // service must keep every file.
  //
  // The rows and this number describe only the newest Version. An older
  // Version can still be in use, for example by an open iterator, a read-only,
  // secondary or follower instance, or a backup or checkpoint in progress, and
  // it can name files that no row names and whose numbers are below this
  // number. The DB instance that uses the older Version keeps such files until
  // it releases that Version, but other instances and the store do not know
  // about this use. So the service may delete a file that no row names and
  // whose number is below this number only when no instance of the DB still
  // uses an older Version.
  //
  // In ManifestCopyMode::kSync it is also at most the smallest number of the
  // files that the edits of the same commit remove. The sync copy gets a
  // commit only after the store applied it, so until then the copy still names
  // these files, and RollBackManifestStore() without the store needs them.
  // The next commit comes after the copy holds the previous one, and its own
  // value releases these files. A commit can also delete the leftover rows of
  // a column family that an earlier commit dropped. Those deletes do not lower
  // this value, because the copy already holds the drop and no longer names
  // these files.
  //
  // Limitation: after a crash, a blob direct write file that only the WAL
  // referred to is not named by any row, and its number is below this value.
  // RocksDB does not add such a file to its metadata (see
  // ColumnFamilyOptions::enable_blob_direct_write: WAL replay of active
  // direct-write blob files is not supported). RocksDB itself keeps the file,
  // but a service that follows the rows and this value can delete it, and the
  // value that the WAL holds is then lost.
  kInFlightFileNumber = 7,
  // varint64, always present. The last sequence number of the DB data (key
  // writes) that the metadata records, as in the MANIFEST file. It is not the
  // commit sequence: the store counts commits and claims separately in
  // ManifestStoreDbState::commit_sequence.
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
  // key. A row reader needs it to find the timestamp at the end of the user key
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
  // its 8-byte trailer; ikey }, in strictly increasing level order (each level
  // appears at most once). A level is at most 65536. This is a sanity bound of
  // the row reader, so that a corrupt level cannot make it allocate a huge
  // vector; real DBs have far fewer levels.
  kCompactCursors = 7,
  // List; when absent, there are none. The blob files that a best-efforts
  // BlobDB repair left out: repeated varint64, strictly increasing.
  kDroppedBlobFiles = 8,
  // bool, default false. True after the column family is dropped. The SST
  // file and blob file rows of the column family can stay in the store after
  // this flag is set. The store does not remove rows because of this flag. The
  // rows go away when a commit deletes them, when a new generation replaces
  // them, or when Destroy() runs. RocksDB accepts such rows when it reads them,
  // and leaves them out of recovery.
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
  // Like a MANIFEST file reader, a row reader reads a value that is not below
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
  // bytes, default empty. The checksum method and the checksum value are both
  // empty or both not empty. An absent field has the empty default.
  kChecksumMethod = 3,
  // bytes, default empty. See kChecksumMethod.
  kChecksumValue = 4,
  // varint64, default 0. The total garbage in the file.
  kGarbageBlobCount = 5,
  // varint64, default 0.
  kGarbageBlobBytes = 6,
};

// The fields of a WAL row.
enum class ManifestWalField : uint32_t {
  // varint64; when absent, the synced size is unknown. 0 is a valid size. The
  // largest 64-bit value (2^64 - 1) is not valid, because RocksDB uses it in
  // memory for an unknown size; a writer leaves the field out instead.
  kSyncedSize = 1,
};

// The row format version that this release reads. A row reader returns
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
// - RocksDB leaves out a field that holds its default. A parser also accepts
//   a field that holds its default.
// - A writer uses the fewest bytes for every varint, also inside values.
//   ParseManifestRowFields() rejects a longer form of a field ID or a value
//   size, and the RocksDB row reader rejects a longer form inside a value.
// So rows that RocksDB writes with equal values give equal bytes, and a store
// can compare or checksum the bytes. Two encodings can still decode to the
// same values when one of them holds a field with its default and the other
// leaves it out. A store compares only the bytes it was given, so this does
// not matter to it. A store with a fixed schema keeps an absent field as NULL,
// and rebuilds the bytes in ID order.
//
// The value encodings are defined byte by byte, so that a store adapter can
// decode them without RocksDB code:
// - varint32 and varint64: unsigned LEB128. Each byte holds 7 bits of the
//   value, the lowest bits first, and its high bit is 1 when another byte
//   follows. A varint32 takes at most 5 bytes, and a varint64 at most 10.
// - bool: one byte, 0 or 1.
// - enum: one byte that holds the enum value.
// - bytes: the raw bytes. The value size gives the length.
// - ikey: an internal key. It starts with the user key bytes as RocksDB holds
//   them in memory. In a column family with user-defined timestamps, the user
//   key ends with the timestamp, which has ManifestCfField::kTimestampSize
//   bytes. ManifestCfField::kPersistUserDefinedTimestamps and
//   ManifestFileField::kUserDefinedTimestampsPersisted do not change this
//   encoding: a row keeps the timestamp bytes also when the SST file does not
//   store them. The next 8 bytes hold (sequence number << 8 | value type) as a
//   little-endian 64-bit integer.
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
// value size is a varint32. It encodes every given field, also a field with
// an empty value. RocksDB leaves out fields that hold their default when it
// builds a whole row. A store that writes a row again keeps all of its fields,
// also unknown ones. An empty value removes a field only in
// ManifestStoreOpenOptions::db_field_updates.
std::string EncodeManifestRowFields(
    const std::vector<ManifestRowField>& fields);

// ======== The store interface ========
//
// Background: an application implements ManifestStore on top of its own
// database (for example a replicated key-value store) and gives one object to
// RocksDB in DBOptions::manifest_store. A new DB that DB::Open() creates with
// this option is in store mode: the rows in the store hold its metadata. An
// existing file-mode DB starts to move into the store only in a read-write
// DB::Open() with DBOptions::manifest_store_migrate. A later read-write
// DB::Open() that finds an unfinished move finishes it, also without that
// option. The types below define what a store must do. The application creates
// a store object for one DB, with its own key for the DB and the epoch.
//
// Terms:
// - Epoch (store epoch): a number that the application gives to a store
//   object when it creates it, one per DB instance that takes over the DB.
//   Each DB instance that takes over the DB must get a newer epoch than all
//   earlier instances. The store keeps the newest epoch that opened or created
//   the DB, and refuses requests of an object with an older epoch ("fenced").
//   RocksDB never sees the epoch. An object made only for reading, for example
//   for a secondary instance, may have no epoch.
// - Claim: Open(), and the Commit() that creates the DB row. A claim makes
//   the epoch of this object the stored epoch and raises the commit sequence.
//   A claim with the same epoch as the stored one is allowed. It fences no
//   object: a request of another object with that epoch fails only when its
//   expected commit sequence no longer matches, and then gets Busy.
// - Commit sequence: a number in the DB row that grows by 1 with each
//   successful commit or claim. A commit names the commit sequence it
//   expects, so two writers cannot both apply a change on the same state.
//   For example, store object A has epoch 1, and store object B of the same
//   DB has epoch 2:
//
//     call                              result    stored epoch  commit sequence
//     A: Commit(expected 0) creates     OK        1             1
//     A: Commit(expected 1)             OK        1             2
//     B: Open() claims the DB           OK        2             3
//     A: Commit(expected 2)             IOFenced  2             3
//     B: Commit(expected 2)             Busy      2             3
//     B: Commit(expected 3)             OK        2             4
//
//   A is fenced, because B has a newer epoch. B gets Busy for expected
//   sequence 2, because its own claim raised the commit sequence to 3.
// - Commit id: a unique string that RocksDB gives each commit and claim. The
//   store writes the id of the last applied one into the DB row. After an
//   unknown outcome, RocksDB reads the DB row: a last_commit_id equal to the
//   id of the request means that the request applied. A different id alone
//   does not mean that it failed. A request with an expected commit sequence
//   may still apply later, while the stored commit sequence equals that value.
//   A claim without one may still apply later at any time (see Open()).
// - Generation: a full copy of all rows other than the DB row. A store keeps
//   rows of one current generation. A new generation replaces all rows at
//   once, for example when RocksDB copies the rows of an existing DB into the
//   store. WriteGeneration() writes a new generation; its comment lists the
//   steps and checks. For example, with generation 4 current, the store goes
//   through these states:
//
//     before:   DB row (generation 4)  rows of gen 4 (visible)
//     writing:  DB row (generation 4)  rows of gen 4 (visible)
//                                      rows of gen 5 (hidden, in chunks)
//     switched: DB row (generation 5)  rows of gen 5 (visible)
//                                      rows of gen 4 (hidden, removed later)
//
//   A store keeps the rows of each generation apart (for example, it adds the
//   generation to each row key). Only the current generation is visible to
//   Scan(). The store picks a new number for each WriteGeneration() attempt.
//   After a successful switch it returns the number in `generation` and
//   stores it in ManifestStoreDbState::generation. The number is for logs and
//   debugging; RocksDB does not depend on its value.
//
// Call order: RocksDB uses a store in store mode (DBOptions::manifest_store)
// as follows, and a store must support it:
// 1. Create a DB: Commit() with expected_commit_sequence 0 writes the DB row
//    and the first rows. When it returns Busy or an unknown outcome,
//    ReadDbRow() reads the DB row. A row without a db id (left by a failed
//    move into the store) is claimed with Open() and written over with one
//    Commit().
// 2. Open a DB: the caller reads the DB row with ReadDbRow() and checks that
//    it belongs to its DB. Open() then claims the DB with the commit sequence
//    and the last commit id of that read as preconditions. After Busy, the
//    caller reads and checks the DB row again before it retries. After the
//    claim succeeds, Scan() reads all rows.
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
//    object without an epoch. When the open replays a scan but a missing or
//    damaged file leaves no valid Version, it calls ReadDbRow() again. A
//    failed read returns IOError. An unchanged commit sequence, or a file that
//    exists but fails its check, returns Corruption. A changed commit
//    sequence with only missing files returns TryAgain (the primary may have
//    deleted them), and the caller then opens again.
// Other cases:
// - After Busy or an unknown outcome: ReadDbRow(). After an unknown outcome,
//   compare last_commit_id with the commit id of the request. An equal id
//   means that the request applied, and a different id leaves its outcome
//   unknown. Busy means that this try did not apply, but an earlier try with
//   the same commit id and an unknown outcome may have applied. Check it in
//   the same way. Open() describes how to retry a claim, with an example.
// - Claim or destroy the DB only if the DB row is still the one the caller
//   read and checked: set expected_commit_sequence and
//   expected_last_commit_id in ManifestStoreOpenOptions or
//   ManifestStoreDestroyOptions to the commit sequence and the last commit id
//   that ReadDbRow() returned. If another writer changed the DB row after
//   that read, the call changes nothing, so it does not fence that writer. It
//   returns IOFenced when that writer has a newer epoch, and Busy otherwise
//   (see "Order of the checks"). Both values are needed, because the commit
//   sequence alone can match a different row. For example (example values), a
//   caller reads commit sequence 3 with last commit id "A-3"; another writer
//   destroys and creates the DB again, and its row reaches commit sequence 3
//   with last commit id "B-3". The sequence matches, but the id does not, so
//   the call changes nothing (Busy, or IOFenced if that writer has a newer
//   epoch).
// - Move an existing DB into the store: ReadDbRow(), then a claim. Open()
//   claims an existing DB row, and a Commit() with expected_commit_sequence 0
//   creates and claims a missing one. Then WriteGeneration() writes all other
//   rows as a new generation. After the switch, the same read-write open
//   claims the DB again as in step 2 (ReadDbRow(), a check of the db id, and
//   Open() with both preconditions), then calls Scan().
// - Roll back to the MANIFEST file (RollBackManifestStore()): ReadDbRow(),
//   then Open(). For a done stub it then calls Scan() and one Commit() that
//   sets the authority to kFile. For an entering stub whose switch did not
//   happen, the DB row still has kFile, so it makes no Commit(); its claim
//   only replaces the last commit id and raises the commit sequence.
// - DestroyDB(): ReadDbRow() checks that the DB row belongs to this DB, then
//   Destroy(), then RocksDB deletes the files. After a rollback, DestroyDB()
//   of the DB in file mode also calls Destroy() when the rows have the db id
//   of the DB and authority kFile.
// - The readers that do not claim, DB::OpenForReadOnly(),
//   DB::ListColumnFamilies(), and secondary and follower instances
//   (DB::OpenAsSecondary(), DB::OpenAsFollower()), use only Scan() and
//   ReadDbRow().
// The ReadDbRow() before a claim comes from an object whose epoch can be
// newer than the stored epoch. It must not return IOFenced then.
//
// Not supported in store mode: DB::GetLiveFiles() (the MANIFEST file is only
// a stub, so DB::GetLiveFilesStorageInfo() gives the MANIFEST content in
// memory instead), RepairDB(), and the tools that read or rewrite the
// MANIFEST file offline. They return NotSupported.

// The DB row values that the store owns. This is the one place that describes
// the whole DB row. The store keeps this logical state under the app's key of
// one DB (see ManifestRowKey); each store chooses its own physical layout:
//
//   app's key of the DB
//   +-- stored epoch          private to the store; RocksDB never sees it
//   +-- DB row (outside of any generation)
//   |   +-- RocksDB-owned fields   ManifestDbField, encoded in
//   |   |                          ManifestRow::fields
//   |   +-- store-owned values     ManifestStoreDbState (this struct)
//   +-- generation G (the current one; others are being written or removed)
//       +-- column family, file, blob file and WAL rows
//
// Destroy() removes the DB row and all other rows, and keeps the stored epoch.
//
// Who writes each part:
// - RocksDB writes all of its fields in ManifestCommit::db_row and
//   ManifestGeneration::db_row.
// - At a claim, RocksDB can also change some of its fields through
//   ManifestStoreOpenOptions::db_field_updates.
// - The store sets commit_sequence and last_commit_id in each successful
//   commit and claim. It sets generation only when WriteGeneration() switches
//   to a new generation. RocksDB never writes these values.
// ReadDbRow(), Open() and Scan() return both parts in a ManifestDbRowResult.
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
  // An optional precondition of the claim. When set, the claim applies only if
  // the stored commit sequence equals this value. Else Open() returns Busy and
  // changes nothing: not the stored epoch, the commit sequence, last_commit_id
  // or the DB row fields. Open() checks this value last (see "Order of the
  // checks" above ManifestStore). So with no DB row it returns NotFound,
  // whatever this value is. A caller sets it to the commit sequence of a DB
  // row that it read and checked, so that a change by another writer after
  // that read makes the claim fail, instead of fencing that writer. When not
  // set, the claim does not check the commit sequence. The claim of
  // DB::Open() sets both preconditions to the DB row that it read and checked
  // (see step 2 of "Call order").
  std::optional<uint64_t> expected_commit_sequence;
  // A second part of the precondition. When it is not empty, the claim also
  // needs a DB row whose last_commit_id equals this value, else Open() returns
  // Busy and changes nothing. A caller sets it together with
  // `expected_commit_sequence`, to the last_commit_id of the DB row that it
  // read. The reason: the commit sequence alone cannot tell the DB row that
  // the caller read from a DB row that was destroyed and created again after
  // that read, because the new row can reach the same commit sequence. The
  // new row never has the same last_commit_id, because each commit id is
  // unique.
  std::string expected_last_commit_id;
  // DB row fields that the claim writes, in the canonical encoding. They let a
  // change that must happen together with the claim, for example a new
  // ManifestDbField::kAuthority value, apply in the same atomic step. The store
  // sets each of these fields in the stored DB row. If the row has a field with
  // the same ID, the store replaces it. If not, the store adds the field. An
  // empty value removes the field, which means the default. A field that the
  // updates do not name keeps its stored value. The caller must not remove a
  // field that the row format needs, such as the DB row format version. This
  // merge is the only place where a store must parse fields.
  std::string db_field_updates;
};

// The input of ManifestStore::Destroy().
struct ManifestStoreDestroyOptions {
  // An optional precondition, as in ManifestStoreOpenOptions: when set,
  // Destroy() removes the rows only if the stored commit sequence equals this
  // value. 0 means that there must be no DB row. Else Destroy() returns Busy
  // and removes nothing. When not set, Destroy() does not check the commit
  // sequence.
  std::optional<uint64_t> expected_commit_sequence;
  // As in ManifestStoreOpenOptions: when not empty, Destroy() also needs a DB
  // row whose last_commit_id equals this value, else it returns Busy and
  // removes nothing.
  std::string expected_last_commit_id;
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
  // `upserts`: rows to add, or to replace when a row with the same key exists.
  // `deletes`: keys of rows to delete. Both are in the current generation. A
  // commit applies all of them, or none. A key appears at most once across
  // `upserts` and `deletes`.
  std::vector<ManifestRow> upserts;
  std::vector<ManifestRowKey> deletes;
};

// The input of ManifestStore::Scan().
struct ManifestScanOptions {
  // True for a read-only instance that is not the DB instance that claimed the
  // DB, such as a secondary instance. The store then returns the DB row and all
  // other rows of one commit, not a mix of rows from before and after a commit
  // that runs during the scan. RocksDB makes one call at a time on each store
  // object. So the instance that claimed the DB makes no commit during its own
  // scan, and it can use false to let the store skip the cost of a consistent
  // read.
  //
  // With false, another object can still claim the DB during the scan, and
  // commit after it. A DB instance that takes over the DB claims with a newer
  // epoch (see "Epoch"). The store then either returns IOFenced, or finishes
  // the scan with rows that can mix the states before and after those commits.
  // The mix is safe: the next Commit() of this object gets IOFenced, because
  // its epoch is older, so the instance stops before it writes anything that
  // depends on those rows. This guarantee needs the newer epoch. A claim with
  // the same epoch fences nothing: a later Commit() of this object that
  // expects the commit sequence from before that claim gets Busy, because the
  // claim raised the commit sequence, but a Commit() that expects the current
  // commit sequence can still apply.
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
  // The commit sequence that the caller expects in the stored DB row: the value
  // after its claim and its latest commit. It is 1 or more, because a claimed
  // DB row exists. The store checks it at the switch to the new generation.
  // `commit_id` and `db_row` are as in ManifestCommit, for the commit that
  // switches to the new generation.
  uint64_t expected_commit_sequence = 0;
  std::string commit_id;
  ManifestRow db_row;
  // All column family, file, blob file and WAL rows of the new generation.
  // Each key appears at most once.
  std::vector<ManifestRow> rows;
};

// Results of Commit() and WriteGeneration(): the status gives the meaning.
// Check it with IsIOFenced(), IsBusy() and IsTryAgain(). IOFenced is code
// kIOError with subcode kIOFenced. The table below uses an example commit with
// expected commit sequence E and commit id "c". Each result describes this try
// only. An earlier try of the same request with an unknown outcome may still
// have applied; "After an unknown outcome" below says how to tell.
//
//   OK ......................... this try applied
//   IOFenced ................... this try did not apply and never will (a
//                                newer epoch claimed the DB)
//   Busy ....................... this try did not apply and never will
//                                (the stored commit sequence is not E)
//   TryAgain ................... this try did not apply and never will (a
//                                temporary refusal; see below)
//   NotSupported ............... this try did not apply and never will
//                                (this object has no epoch, see "Read-only
//                                objects" below; or the store rejects this
//                                request for good, for example because it is
//                                too large)
//   other (TimedOut, IOError) .. unknown; this try may still apply later.
//                                Then ReadDbRow(): last_commit_id == "c"
//                                means applied
//
// TryAgain: the store did not apply this try and never will. This try did not
// change the stored commit sequence, so the caller can send the same request
// again, with the same expected commit sequence and commit id. Another writer
// may have changed the commit sequence in the meantime; the retry then gets
// Busy. A store returns TryAgain for a temporary refusal that it knows did not
// apply, for example a lost race inside the store. WriteGeneration() also
// returns it when the row count or checksum check fails. Busy differs: the
// stored commit sequence is not the expected one. When it is already larger
// than E (the usual case, because the caller read E from the store), a retry
// with the same E also gets Busy (see "After an unknown outcome"). IOStatus has
// no TryAgain() factory, so a store builds it with
// status_to_io_status(Status::TryAgain()).
//
// After an unknown outcome: a successful request raises the commit sequence of
// the current DB row by one, from E to E + 1. So while that DB row stays in
// place, at most one request with expected commit sequence E can apply, and a
// retry with the same E after a success gets Busy (or IOFenced, if a newer
// epoch claimed the DB). So a retry that gets Busy does not tell whether an
// earlier try applied. ReadDbRow() tells that the request applied when its
// commit id is still last_commit_id ("After Busy or an unknown outcome" in
// "Call order" has the steps). A DB row that is destroyed and created again can
// reach commit sequence E again; see the example in "Call order".
//
// A store returns NotSupported for a commit that it rejects for good and did
// not apply, for example a commit that is larger than the store allows. RocksDB
// then sends nothing more for that batch: the batch fails with a non-retryable
// IOError, as after a failed MANIFEST write in file mode, and no auto-resume
// starts. A flush or a compaction then sets a fatal background error, so the
// DB must be opened again. When a resend gets NotSupported, the first send can
// still apply, so the DB stops all later MANIFEST writes until it is opened
// again.
//
// There is one exception to "nothing more". RocksDB deletes the rows of a
// dropped column family in parts, as part of later commits. Those deletes are
// optional. When the store rejects a commit that carries them, RocksDB sends
// the batch once more without them, with a new commit id and the same
// expected commit sequence. It handles the result as the result of a first
// send, and a later commit retries the deletes.
//
// RocksDB treats every other status as an unknown outcome. This includes a
// status that means the commit can never apply, for example InvalidArgument for
// a commit that is too large, so a store should return NotSupported for that.
// RocksDB reads the DB row and sends the commit again until the outcome is
// known, its retries run out, or the DB starts to close. With the default
// DBOptions::max_bgerror_resume_count (INT_MAX), the retries do not run out.
// Later MANIFEST writes of the DB wait during that time. A commit inside
// DB::Open() makes no retry after a failed ReadDbRow() or resend: DB::Open()
// returns the error.
//
// Read-only objects: an object without an epoch only reads. Its Commit(),
// WriteGeneration(), Open() and Destroy() calls return NotSupported (Open()
// returns NotFound first when there is no DB row) and change nothing.
//
// Order of the checks: when an Open(), Commit(), WriteGeneration() or
// Destroy() call fails more than one check, it returns the result of the first
// failed check in this order: NotFound (only Open() checks for a missing DB
// row here), NotSupported, IOFenced, Busy. For example, a fenced object gets
// IOFenced also when its expected commit sequence is wrong.
//
// Threading and timeouts: RocksDB makes at most one call at a time on the
// store object of one DB instance. DB::ListColumnFamilies(),
// RollBackManifestStore() and DestroyDB() call the store from the caller's
// thread, so do not pass them a store object that an open DB uses. Every call
// must return in a bounded time (the store applies its own timeout), because
// DB::Open() and DB::Close() wait for calls in flight. A method added to this
// class has a default implementation that returns NotSupported, so that
// existing stores still build.
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
  // - NotSupported: this object has no epoch, so it cannot claim.
  // - IOFenced: the stored epoch is newer. Nothing is claimed.
  // - Busy: `opts.expected_commit_sequence` is set and the stored commit
  //   sequence differs from it, or `opts.expected_last_commit_id` is not empty
  //   and the stored last_commit_id differs from it. Nothing is claimed.
  // - Any other status: the outcome is unknown. See "Unknown outcome" below.
  // A claim with the same epoch as the stored one is allowed. Like every
  // claim, it raises the commit sequence, so requests that expect the old
  // commit sequence get Busy.
  //
  // Unknown outcome: a claim with an unknown outcome (for example TimedOut)
  // may have applied, or may still apply later, also after later requests of
  // this object. When it applies, it raises the commit sequence, so a later
  // commit that expects the old commit sequence gets Busy. A retry without
  // either precondition claims again with the same epoch, raises the commit
  // sequence once more, and returns the state after its own claim. A retry
  // with the same preconditions gets Busy when a precondition no longer
  // holds, for example because the first try applied, or IOFenced when a newer
  // epoch claimed the DB (see "Order of the checks").
  // How to retry: after Busy, call ReadDbRow() and check the DB row before a
  // new claim. When an earlier claim had an unknown outcome, compare the row's
  // last_commit_id with the commit id of that earlier claim. An equal id means
  // that the earlier claim applied. A different id leaves the outcome unknown:
  // the claim may not have applied yet, or a later claim or commit may have
  // replaced the id. A guarded claim is a claim that sets both preconditions
  // (expected_commit_sequence and expected_last_commit_id). To send the same
  // guarded claim again, keep its commit id and both preconditions. For example
  // (example values), a guarded claim expects commit sequence 3 and times out.
  // The caller retries with the same commit id and both preconditions. If the
  // first try applied and no newer epoch claimed the DB, the retry gets Busy
  // (or IOFenced after a claim with a newer epoch). This Busy says only that
  // the retry did not apply. The caller reads the DB row again: a
  // last_commit_id equal to the claim's commit id means that the first try
  // applied, and a different id leaves its outcome unknown.
  virtual IOStatus Open(const ManifestStoreOpenOptions& opts,
                        ManifestDbRowResult* result) = 0;

  // Applies `commit` atomically. Results as listed above the class. A commit
  // with expected_commit_sequence 0 creates the DB row: it returns Busy when
  // a DB row exists. A commit with a larger expected value returns Busy when
  // no DB row exists. The creating commit sets the commit sequence to 1.
  //
  // Who may commit: the creating commit is a claim, and stores the epoch of
  // this object. For an existing DB, RocksDB first claims it with Open(), then
  // commits (see "Call order" above). A store checks only that the epoch of
  // this object is not older than the stored epoch. So a store may apply a
  // commit of a newer object that has not claimed the DB yet, when the
  // expected commit sequence matches; such a commit does not change the stored
  // epoch. RocksDB never sends one.
  virtual IOStatus Commit(const ManifestCommit& commit) = 0;

  // Reads the latest committed DB row, without a claim and without reading
  // other rows. A caller uses it after Busy or an unknown outcome, and before
  // each claim to check that the row belongs to its DB (see "Call order"
  // above). Results: OK; IOFenced, when this object has an epoch and the
  // stored epoch is newer; NotFound (no DB row); any other status (nothing is
  // known; calling it again is safe).
  virtual IOStatus ReadDbRow(ManifestDbRowResult* result) = 0;

  // Calls `visitor` for the DB row, then for each other row of the current
  // generation. Results: OK; NotFound (no DB row); IOFenced, when this object
  // has an epoch and the stored epoch is newer; the non-OK status of the
  // visitor; any other status (the scan failed; calling it again is safe).
  virtual IOStatus Scan(const ManifestScanOptions& opts,
                        ManifestRowVisitor* visitor) = 0;

  // Writes the rows of a new generation, then switches to it with a commit that
  // checks the row count and a checksum of the rows, so that a partly written
  // generation never becomes current:
  // 1. The store checks the epoch and picks a new generation number G, unique
  //    for each attempt.
  // 2. It writes the rows to G in chunks, and checks the epoch for each chunk
  //    (IOFenced for an object with an older epoch).
  // 3. The switch checks the epoch, and that the stored commit sequence equals
  //    `expected_commit_sequence` (else Busy). It compares the count and a
  //    checksum of the rows it wrote to G with `request.rows` (else TryAgain).
  //    The checksum covers the key and the encoded fields of each row. The
  //    store picks the checksum algorithm.
  // 4. In one atomic step, G becomes current, the RocksDB-owned DB fields are
  //    replaced with `request.db_row.fields`, last_commit_id becomes
  //    `request.commit_id`, and the commit sequence grows by 1.
  // The store returns G in `generation` on OK. The store removes the rows of
  // the replaced generation and of a failed attempt, at a time it chooses;
  // Scan() never returns them. RocksDB calls WriteGeneration() only after it
  // has claimed the DB, so the DB row exists. WriteGeneration() is not a claim:
  // it does not change the stored epoch. Other results as for Commit().
  virtual IOStatus WriteGeneration(const ManifestGeneration& request,
                                   uint64_t* generation) = 0;

  // Removes all rows of this DB. Results:
  // - OK, also when there is no DB row (and `opts.expected_commit_sequence` is
  //   not set or 0), so that a caller can repeat a cleanup that stopped after
  //   this call.
  // - NotSupported for an object without an epoch, also when there is no DB
  //   row.
  // - IOFenced.
  // - Busy: `opts.expected_commit_sequence` is set and the stored commit
  //   sequence (0 when there is no DB row) differs from it, or
  //   `opts.expected_last_commit_id` is not empty and there is no DB row or
  //   its last_commit_id differs. Nothing is removed.
  // - Any other status: unknown; the call may still apply later.
  // Retry after an unknown result: to remove only the DB row that the caller
  // read and checked, set both `opts.expected_commit_sequence` and
  // `opts.expected_last_commit_id` on the first call, and send the same values
  // again on each retry. Then a retry gets Busy if an earlier try applied (and
  // ReadDbRow() returns NotFound, unless another writer created a DB row
  // since), or if another writer changed or created the DB row; it never
  // removes another DB row. A call without these values can remove a DB row
  // that another writer created after the first call. Adding the values only on
  // the retry does not protect against the earlier call without them, because
  // that call may still apply later. A retry of a call with only
  // `opts.expected_commit_sequence` 0 (no DB row expected) returns OK while
  // there is no DB row.
  // The store keeps the stored epoch, so an object with an older epoch stays
  // fenced.
  virtual IOStatus Destroy(const ManifestStoreDestroyOptions& opts) = 0;
};

// Moves a DB in store mode back to file mode: afterwards the MANIFEST file
// holds the metadata, and DB::Open() works without `manifest_store`. Stop
// every instance of the DB before the call. Pass a store object with a new
// epoch in `db_options.manifest_store`, or null when the store cannot be
// reached. The DB folder lock is held during the call.
//
// With the store, the function first checks that the DB row belongs to this
// DB and claims it (see below), and only then applies the rules of the table.
// A successful claim replaces the last commit id and raises the commit
// sequence, also when the function then refuses a "creating" stub, or an
// "entering" stub whose switch happened.
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
// Then it claims the DB, which fences an older primary in the store. For a
// "done" stub, it writes and syncs a new MANIFEST file from the rows, points
// CURRENT to it, and then sets the authority of the DB row to
// ManifestAuthority::kFile. When that last update fails, CURRENT already
// names the new MANIFEST file; call the function again with the store. When
// CURRENT names a normal MANIFEST file, the function sets the authority to
// kFile and keeps that MANIFEST file, so file-mode writes in it stay. It
// deletes the old stub (for a "done" stub) and the MANIFEST file copies only
// after the DB row update succeeded.
//
// Without the store, it writes the new MANIFEST file from the MANIFEST file
// copy, which must have been written in ManifestCopyMode::kSync. The files that
// the copy names are still in the folder, also when an outside service deletes
// unused files by the rows: a commit that deletes rows keeps their files at or
// above ManifestDbField::kInFlightFileNumber until the next commit, which
// RocksDB sends only after the copy holds the first one. It cannot claim the
// DB, so it relies only on the folder lock and on the application having
// stopped the old primary. It leaves the DB row, the stub and the copies; a
// later call with the store updates the DB row and deletes the copies, and
// keeps the MANIFEST file. The next file-mode DB::Open() deletes the old stub,
// as it deletes any obsolete MANIFEST file. Until that later call, the DB row
// still says that the store is the authority, so DB::Open() with
// manifest_store_migrate refuses to move the DB into the store again.
//
// The end of the copy can be torn by a crash or a failed append. The function
// keeps every complete record that is not in an atomic group, and every
// complete atomic group. It drops a last record that the crash cut short, and
// all records of an atomic group that is still open at the end. Such a group
// belongs to a commit that was never acknowledged, because in sync mode a
// commit is acknowledged only after its copy append. A store commit can also
// hold several records that are not in an atomic group. So the kept records can
// end inside such a commit, whose copy append did not finish. That commit was
// never acknowledged either, and each kept record is a complete VersionEdit, as
// in a MANIFEST file. Any other damage returns Corruption, because the damaged
// part may hold acknowledged commits: a record with a bad checksum, a record
// that does not decode, or an atomic group with a wrong number of records or
// with a normal record inside it.
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
