//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/version_edit.h"
#include "db/wal_edit.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;
class VersionStorageInfo;
struct ColumnFamilyOptions;

// Background
//
// The MANIFEST file holds the metadata of a DB as a log of VersionEdit records:
// each record is a change, and recovery replays all of them. The rows in this
// file are a second form of the same metadata, meant for storing it in an
// external metadata store instead of a file. Each row holds the resulting
// values of one object (the DB, a column family, an SST file, a blob file or a
// WAL), not the changes that led to them. A reader gets the whole state by
// reading all rows, with no history.
//
// This file defines the in-memory form of the rows and these conversions. A
// batch is the VersionEdits of one MANIFEST write, as
// VersionSet::ProcessManifestWrites() groups them. The comments below use
// "batch" for these edits and "MANIFEST write" for writing them.
// - VersionSet::GetManifestRows() reads the current state into rows.
// - BuildManifestRowChanges() turns one batch into the rows to write and the
//   rows to delete, and ApplyManifestRowChanges() applies them to rows. Only a
//   debug check in VersionSet::ProcessManifestWrites() uses them. It compares
//   the result with the new Versions (see VersionSet::debug_manifest_rows_).
// - ManifestRowsToRecords() turns rows into encoded "full-state" VersionEdit
//   records, like the ones that VersionSet::WriteCurrentStateToManifest()
//   writes at the start of a new MANIFEST file. The existing recovery code
//   replays them.
//
// The rows hold every field that recovery reads from a MANIFEST. Three kinds
// of VersionEdit content have no row field:
// - kInAtomicGroup: an atomic group is always complete inside one MANIFEST
//   write. The rows have no atomic group marker, so a user of the row changes
//   must apply all row changes of one MANIFEST write together
//   (ApplyManifestRowChanges() does).
// - kLastCompactedManifestFileSize: it only controls when a MANIFEST file is
//   rewritten.
// - kSubcompactionProgress: only remote compaction workers write it, to their
//   own progress file, never to the DB MANIFEST.
// Deprecated tags (kNewFile, kNewFile2, kNewFile3, the *_DEPRECATED tags) are
// only read from old MANIFEST files, and their content is held in the rows
// below in its current form. When you add a VersionEdit tag, follow the
// checklist at ManifestRowFieldOfTag() in db/version_set_test.cc. When you add
// a persisted FileMetaData field, add its tag to ManifestRowFieldOfNewFileTag()
// there.
//
// In-memory differences
//
// Rows built from the edits of MANIFEST writes (BuildManifestRowChanges()) keep
// the values of those edits. Some values change in memory without a MANIFEST
// write, so these rows can differ from the in-memory state:
// - the log number of an empty column family (DBImpl::SwitchMemtable());
// - files marked for compaction (DB::SuggestCompactRange());
// - the committed OPTIONS file number, which a new OPTIONS file drops from
//   memory when DBOptions::track_options_file_number_in_manifest is false;
// - the max column family, which DB::CreateColumnFamily() raises
//   (ColumnFamilySet::GetNextColumnFamilyID()) before its add edit is
//   written;
// - the next file number: each edit records it when the batch is built, and
//   other threads allocate more file numbers later.
// A new MANIFEST file starts with a snapshot of the in-memory state, so it can
// hold the newer in-memory values. Both describe a state that recovery accepts.
//
// The DB id is not an in-memory difference. An edit can set it, and the row
// takes the value from the edit. DBImpl sets the id in memory before it writes
// the edit, so the row and memory differ only when a caller writes the edit
// alone, as tests do. The debug check accepts such a difference only in the
// MANIFEST write that sets the id.

// The row with the fields that belong to the whole DB.
struct ManifestDbRow {
  // The DB id, also kept in the IDENTITY file when
  // DBOptions::write_identity_file is true. Empty when no MANIFEST record has
  // set it, for example when DBOptions::write_dbid_to_manifest is false.
  std::string db_id;
  // The next file number. VersionSet::GetManifestRows() takes the in-memory
  // value, which is the number that the next new file gets. Rows built by
  // BuildManifestRowChanges() hold the value that the last MANIFEST write
  // recorded, which can be smaller (see "In-memory differences" above).
  uint64_t next_file_number = 0;
  // The last sequence number written to the MANIFEST
  // (VersionSet::descriptor_last_sequence_).
  SequenceNumber last_sequence = 0;
  // A legacy WAL number that recovery still reads. It is usually 0.
  uint64_t prev_log_number = 0;
  // WALs with a smaller number are no longer needed by any column family.
  uint64_t min_log_number_to_keep = 0;
  // The largest column family id ever used, including dropped ones.
  uint32_t max_column_family = 0;
  // The OPTIONS file that describes the DB state, if the MANIFEST records it
  // (VersionEdit::SetCommittedOptionsFileNumber()).
  std::optional<uint64_t> committed_options_file_number;

  bool operator==(const ManifestDbRow&) const = default;
};

// The row of one column family.
struct ManifestCfRow {
  // The column family name.
  std::string name;
  // The name of the user comparator (Comparator::Name()).
  std::string comparator_name;
  // The size of the user-defined timestamp in each user key, from the
  // comparator. 0 when the column family has no timestamps. A MANIFEST does
  // not store this value. Recovery gets it from the comparator.
  // ManifestRowsToRecords() needs it to encode the file boundaries.
  size_t timestamp_size = 0;
  // ColumnFamilyOptions::persist_user_defined_timestamps.
  bool persist_user_defined_timestamps = true;
  // WALs with a smaller number hold no data of this column family that is not
  // in an SST file yet.
  uint64_t log_number = 0;
  // History below this timestamp may be collapsed by compaction, and reads
  // with an older timestamp are rejected
  // (DBImpl::FailIfReadCollapsedHistory()). Empty means it is not set.
  std::string full_history_ts_low;
  // The round-robin compaction cursor of each level, indexed by level. An
  // unset (empty) key means the level has no cursor. Trailing levels with no
  // cursor are left out.
  std::vector<InternalKey> compact_cursors;
  // Not related to `dropped`. Blob files that best-efforts BlobDB repair left
  // out of the column family (VersionStorageInfo::GetDroppedBlobFiles()).
  // Sorted, with no duplicates.
  std::vector<uint64_t> dropped_blob_files;
  // True after the column family is dropped. VersionSet::GetManifestRows()
  // leaves out dropped column families, so it never sets this flag.
  // BuildManifestRowChanges() sets it for a column family drop.
  // ManifestRowsToRecords() leaves out the column family and its file and blob
  // file rows (see "Column family row" at BuildManifestRowChanges()).
  bool dropped = false;
};

// The row of one SST file. `meta` holds the fields of the file's FileMetaData
// that a MANIFEST stores. The in-memory fields (reference count, table reader,
// compaction state, statistics) are reset when the row is made, so a row does
// not depend on the Version it came from.
struct ManifestFileRow {
  // The LSM level of the file. FileMetaData has no level field.
  int level = 0;
  FileMetaData meta;
};

// The row of one blob file: the values that a blob file addition records,
// plus the total garbage in the file.
struct ManifestBlobFileRow {
  BlobFileAddition addition;
  uint64_t garbage_blob_count = 0;
  uint64_t garbage_blob_bytes = 0;

  bool operator==(const ManifestBlobFileRow&) const = default;
};

// All rows of one column family.
struct ManifestCfRows {
  ManifestCfRow cf;
  // Keyed by file number.
  std::map<uint64_t, ManifestFileRow> files;
  // Keyed by blob file number.
  std::map<uint64_t, ManifestBlobFileRow> blob_files;
};

// All rows of a DB. The maps are ordered so that ManifestRowsToRecords() gives
// the same output for the same rows.
struct ManifestRows {
  ManifestDbRow db;
  // Keyed by column family id.
  std::map<uint32_t, ManifestCfRows> cfs;
  // The live WALs that the MANIFEST tracks
  // (DBOptions::track_and_verify_wals_in_manifest), keyed by WAL number.
  std::map<WalNumber, WalMetadata> wals;
};

// The changes that one MANIFEST write makes to the rows: the rows to write,
// with their whole new values, and the keys of the rows to delete. An upsert
// ("update or insert") writes a row: it adds the row, or replaces the row with
// the same key. File and blob file rows are keyed by (column family id, file
// number). A key is never in both the upserts and the deletes of the same row
// type.
struct ManifestRowChanges {
  // The DB row after the write. Every write sets it, because each edit carries
  // the next file number and the last sequence.
  ManifestDbRow db;
  // Keyed by column family id.
  std::map<uint32_t, ManifestCfRow> cf_upserts;
  std::map<std::pair<uint32_t, uint64_t>, ManifestFileRow> file_upserts;
  std::set<std::pair<uint32_t, uint64_t>> file_deletes;
  std::map<std::pair<uint32_t, uint64_t>, ManifestBlobFileRow>
      blob_file_upserts;
  std::set<std::pair<uint32_t, uint64_t>> blob_file_deletes;
  std::map<WalNumber, WalMetadata> wal_upserts;
  std::set<WalNumber> wal_deletes;
};

// Describes one column family that a MANIFEST write changes: a column family
// that gets a new Version, or the column family that the write drops. A column
// family add has no entry, because its first Version is created when the write
// is installed. For a column family drop, no new Version is installed, so
// `base` and `new_version` are both its current Version.
struct ManifestBatchVersion {
  const ColumnFamilyData* cfd = nullptr;
  // The Version that the edits of the write were applied to.
  const VersionStorageInfo* base = nullptr;
  // The new Version that the write installs.
  const VersionStorageInfo* new_version = nullptr;
};

// Builds the row changes of one batch, as VersionSet::ProcessManifestWrites()
// groups it, before the write is installed.
//
// Parameters:
// - `edits` are the edits of the batch, in order. Edits of dropped column
//   families are already left out.
// - `batch_versions` has one entry for each column family that gets a new
//   Version, plus one for the dropped column family of a column family drop
//   (see ManifestBatchVersion).
// - `new_cf_options` holds the options of the new column family for a column
//   family add, and is null otherwise.
// - `old_rows` holds the rows before the write. Only its DB row and column
//   family rows are used, not its file rows. `wals` is the WAL set before the
//   write.
//
// Key choices: the file rows and the DB row come from the edits, in order, the
// same way recovery replays them. The column family row and the blob file rows
// are built again from the new Version, because the new Version already holds
// the result of their edits. Only the log number, full_history_ts_low and the
// dropped flag of the column family row are merged from the edits, because the
// new Version does not hold them. Rules:
// - DB row: the next file number, the DB id and the committed OPTIONS file
//   number come from the last edit that sets them. The last sequence, the min
//   log number to keep and the max column family take the largest value. A
//   column family add also raises the max column family to the new id.
// - DB row, prev log number: the row takes the value of the first edit in
//   `edits`. ProcessManifestWrites() installs the value of the first edit of
//   the first writer. These two edits differ only when the first writer
//   belongs to a dropped column family, because `edits` leaves out its edits.
//   Such edits skip LogAndApplyHelper() and keep their caller's value,
//   normally 0, and the earlier drop already set the in-memory value to 0. So
//   both values are 0, unless a caller sets a non-zero value explicitly, as
//   only tests do.
// - Files: within one edit, deleted files are applied before new files, so a
//   file that moves to another level becomes one upsert.
// - Column family row: the log number is the largest of the row and the
//   edits. full_history_ts_low only increases. A column family drop writes the
//   row with `dropped` set. Its file and blob file rows stay, because the drop
//   edit lists no deleted files. The files themselves are deleted later, when
//   the last reference to the dropped column family is released, and no
//   MANIFEST write records that. ManifestRowsToRecords() skips these rows, so
//   recovery does not see them.
// - Blob file rows: a row is written when the blob file is new or its garbage
//   changed, and deleted when the new Version no longer has the blob file,
//   because no VersionEdit says that a blob file is gone.
// - WAL rows: the WAL edits are applied to a copy of `wals`, and the result is
//   compared with `wals`.
//
// The rows can differ from the in-memory state after the write (see "In-memory
// differences" at the top of this file).
//
// Cost: the work is linear in the number of edits and the files in them. It
// also scans the edits once for each column family with a new Version, and
// reads the blob files of those column families and all live WALs.
//
// Errors: returns the error of a WAL edit that the WAL set rejects.
Status BuildManifestRowChanges(
    const autovector<VersionEdit*>& edits,
    const std::vector<ManifestBatchVersion>& batch_versions,
    const ColumnFamilyOptions* new_cf_options, const ManifestRows& old_rows,
    const WalSet& wals, ManifestRowChanges* changes);

// Applies `changes` to `rows`. The column family row comes from `changes`,
// where BuildManifestRowChanges() set its dropped flag.
void ApplyManifestRowChanges(const ManifestRowChanges& changes,
                             ManifestRows* rows);

// Converts `rows` into encoded full-state VersionEdit records and appends them
// to `records`. Writing the records to a new MANIFEST file and recovering from
// it gives the state that the rows describe.
//
// Record order:
// 1. The DB id, when it is not empty. It is written even when
//    DBOptions::write_dbid_to_manifest is false; it is the same value as in
//    the IDENTITY file.
// 2. The WAL additions, then the deletion of WALs below the minimum log
//    number to keep.
// 3. For each column family that is not dropped: one record that adds the
//    column family with its comparator and persist_user_defined_timestamps (the
//    default column family is never added, so its record has only these two
//    fields), then one record with all of its files, blob files, compaction
//    cursors, dropped blob files, log number, full_history_ts_low and the last
//    sequence.
// 4. One record with the DB fields: next file number, last sequence, prev log
//    number, max column family, min log number to keep and the committed
//    OPTIONS file number.
//
// Compared with WriteCurrentStateToManifest(): WriteCurrentStateToManifest()
// writes the same content in a different layout:
// 1. It puts the min log number to keep in the default column family record.
// 2. It puts the committed OPTIONS file number in its own record.
// 3. It writes no next file number, prev log number or max column family.
// ProcessManifestWrites() writes the edits of the current MANIFEST write after
// those records, and these edits hold the three values.
//
// Timestamps: each record with files is encoded with the timestamp size of its
// column family, so that the timestamps are removed from the file boundaries
// when persist_user_defined_timestamps is false.
//
// Next file number after recovery: recovery from the records sets the next file
// number to the recorded value plus one
// (VersionEditHandler::CheckIterationResult()), as after every reopen. This
// only skips one file number, so a round trip gives a next file number that is
// larger by one.
//
// Errors: returns Corruption if a record cannot be encoded.
Status ManifestRowsToRecords(const ManifestRows& rows,
                             std::vector<std::string>* records);

// Returns true if `a` and `b` hold the same rows with the same values. A file
// row that cannot be encoded as a new file record (invalid boundary keys or an
// unknown epoch number) always compares as different; a Version never holds
// such a file. If the rows differ and `diff` is not null, sets `diff` to the
// name of the first row or group of rows that differs.
// - DB, WAL, column family and blob file rows are compared field by field,
//   without ManifestRowsToRecords(). So a round-trip test detects a field that
//   ManifestRowsToRecords() does not write.
// - File rows are compared through their encoding as a new file record, which
//   ManifestRowsToRecords() also uses. So a round-trip test cannot detect a
//   file field that this encoding drops. The switch in
//   ManifestRowFieldOfNewFileTag() in db/version_set_test.cc makes the build
//   fail when a new file field tag is added.
bool ManifestRowsEqual(const ManifestRows& a, const ManifestRows& b,
                       std::string* diff);

}  // namespace ROCKSDB_NAMESPACE
