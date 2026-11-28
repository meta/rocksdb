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
#include "rocksdb/manifest_store.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;
class VersionStorageInfo;
struct ColumnFamilyOptions;

// Background
//
// The MANIFEST file holds the metadata of a DB as a log of VersionEdit records:
// each record is a change, and recovery replays all of them. The rows in this
// file, called the structured MANIFEST rows, are a second form of the same
// metadata, meant for storing it in an external metadata store instead of a
// file. Each row holds the resulting values of one object (the DB, a column
// family, an SST file, a blob file or a WAL), not the changes that led to them.
// The rows hold all the metadata that recovery needs: ManifestRowsToRecords()
// turns them into MANIFEST records that recovery replays. So a reader gets the
// whole state by reading all rows, with no history. One in-memory value is not
// in the rows: the minimum of the live WAL set, which BuildManifestRowChanges()
// also needs to turn a batch into row changes. The comment of
// ManifestDbRow::min_log_number_to_keep explains the two WAL minimums.
//
// This file defines the in-memory form of the rows and these conversions. A
// batch is the list of VersionEdits that VersionSet::ProcessManifestWrites()
// groups for one update. When that update creates a new MANIFEST file,
// WriteCurrentStateToManifest() first writes a full copy of the current state
// (the snapshot), and the batch follows it. BuildManifestRowChanges()
// translates the batch only. The snapshot can record a few values from memory
// that differ from the rows; "In-memory differences" below lists them. The
// comments below use "batch" for these edits and "MANIFEST write" for writing
// them.
// - VersionSet::GetManifestRows() reads the current state into rows.
// - BuildManifestRowChanges() turns one batch into the rows to write and the
//   rows to delete, and ApplyManifestRowChanges() applies them to rows. Only a
//   debug check in VersionSet::ProcessManifestWrites() uses them. It compares
//   the result with the new Versions (see VersionSet::debug_manifest_rows_).
// - ManifestRowsToRecords() turns rows into encoded "full-state" VersionEdit
//   records, like the ones that VersionSet::WriteCurrentStateToManifest()
//   writes at the start of a new MANIFEST file. The existing recovery code
//   replays them.
// - ManifestRowsToPublicRows() and PublicRowsToManifestRows() convert rows to
//   and from the public rows of include/rocksdb/manifest_store.h, which an
//   external metadata store keeps.
//
// The rows hold every field that recovery reads from a MANIFEST. Three kinds
// of VersionEdit content have no row field:
// - kInAtomicGroup: an atomic group is always complete inside one MANIFEST
//   write. The rows have no atomic group marker. ApplyManifestRowChanges()
//   applies all changes of one MANIFEST write to the caller's rows. A caller
//   that shares those rows with other readers must make the whole write
//   visible to them at once.
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
// - the committed OPTIONS file number: when
//   DBOptions::track_options_file_number_in_manifest is false, writing a new
//   OPTIONS file clears this value in memory
//   (VersionSet::ApplyLegacyOptionsFileNumber()), but the row keeps the last
//   committed value;
// - the max column family, which DB::CreateColumnFamily() raises
//   (ColumnFamilySet::GetNextColumnFamilyID()) before its add edit is
//   written;
// - the next file number: each edit records it when the batch is built, and
//   other threads allocate more file numbers later.
// A new MANIFEST file starts with a full copy of the in-memory state, so it can
// hold the newer in-memory values. Both describe a state that recovery accepts.

// The row with the fields that belong to the whole DB.
struct ManifestDbRow {
  // The DB id from the last MANIFEST record that set it (VersionSet::db_id_).
  // DBImpl::NewDB() always records it in the first MANIFEST file. When
  // DBOptions::write_dbid_to_manifest is false, a new MANIFEST file does not
  // record it, so the row is empty after the DB reopens from such a file. The
  // IDENTITY file also keeps the id when DBOptions::write_identity_file is
  // true.
  std::string db_id;
  // The next file number. VersionSet::GetManifestRows() takes the in-memory
  // value, which is the number that the next new file gets. Rows built by
  // BuildManifestRowChanges() hold the value that the last MANIFEST write
  // recorded, which can be smaller (see "In-memory differences" above).
  uint64_t next_file_number = 0;
  // The last sequence number written to the MANIFEST
  // (VersionSet::descriptor_last_sequence_).
  SequenceNumber last_sequence = 0;
  // A legacy WAL number. In the original LevelDB design it named the WAL of the
  // memtable that was being flushed. It is usually 0. GetManifestRows() copies
  // the installed value (VersionSet::prev_log_number_).
  // BuildManifestRowChanges() takes the value of the first edit written to the
  // MANIFEST (0 when that edit does not set it). Recovery keeps the last value
  // that an edit sets. The rules at BuildManifestRowChanges() say when these
  // can differ.
  uint64_t prev_log_number = 0;
  // WALs with a smaller number are no longer needed by any column family
  // (VersionSet::min_log_number_to_keep()). This is the one place that explains
  // the two WAL minimums:
  // - The DB minimum (this field): WALs below it are not needed for recovery.
  // - The live WAL set minimum (WalSet::GetMinWalNumberToKeep()): a WAL
  //   deletion edit raises it, and the set ignores a WAL addition below it.
  // The rows hold the DB minimum and the tracked WALs.
  // BuildManifestRowChanges() applies the WAL edits to a copy of the live WAL
  // set, so the live minimum decides which WAL additions become rows.
  // ManifestRowsToRecords() writes the DB minimum in a WAL deletion record, and
  // recovery sets the minimum of the recovered WAL set from it. For example
  // (example numbers), with a DB minimum of 40 and a live minimum of 50, the
  // builder ignores an addition of WAL 45. Recovery from a MANIFEST written
  // from these rows sets the recovered minimum to 40, and its WAL list still
  // has no WAL 45, because the records hold only the tracked WAL rows.
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
  // in an SST file yet. Rows built by BuildManifestRowChanges() take the
  // largest value of the old row and the edits. Memory can be larger, because
  // DBImpl::SwitchMemtable() raises it for an empty column family without a
  // MANIFEST write. The debug check accepts a row value at most the memory
  // value.
  uint64_t log_number = 0;
  // History below this timestamp may be collapsed by compaction, and reads
  // with an older timestamp are rejected
  // (DBImpl::FailIfReadCollapsedHistory()). Empty means it is not set.
  std::string full_history_ts_low;
  // The round-robin compaction cursor of each level, indexed by level. An
  // unset (empty) key means the level has no cursor. Trailing levels with no
  // cursor are left out.
  std::vector<InternalKey> compact_cursors;
  // Blob files that best-efforts BlobDB repair left out of the column family
  // (VersionStorageInfo::GetDroppedBlobFiles()). Sorted, with no duplicates.
  // Not related to the `dropped` flag below.
  std::vector<uint64_t> dropped_blob_files;
  // True after the column family is dropped. This is the one place that
  // describes the rows of a dropped column family:
  // 1. BuildManifestRowChanges() writes the column family row with `dropped`
  //    set. The drop edit lists no deleted files, so the file rows stay. A
  //    drop has a ManifestBatchVersion whose `base` and `new_version` are both
  //    the current Version. So the blob file diff finds no change, and the
  //    blob file rows stay too.
  //    The files themselves are deleted later, when the last reference to the
  //    column family is released, and no MANIFEST write records that.
  // 2. ManifestRowsToRecords() leaves out the column family and all of these
  //    rows, so recovery does not see them.
  // 3. VersionSet::GetManifestRows() leaves out dropped column families, so it
  //    never sets this flag.
  // 4. The debug check (see VersionSet::debug_manifest_rows_ for its two
  //    phases) removes these rows from its own copy. Phase 1, which runs
  //    without the DB mutex, removes the file and blob file rows. Phase 2
  //    removes the column family row.
  bool dropped = false;
};

// The row of one SST file. `meta` holds the fields of the file's FileMetaData
// that a MANIFEST stores, including marked_for_compaction. The in-memory fields
// (reference count, table reader, being_compacted, statistics) are reset when
// the row is made, so a row does not depend on the Version it came from. The
// smallest and largest keys keep their in-memory form, with the user-defined
// timestamp bytes when the column family has them. When the file does not
// persist user-defined timestamps, ManifestRowsToRecords() writes the
// boundaries without those bytes, as a MANIFEST does.
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
//
//   ManifestRows
//   +-- db                      ManifestDbRow, one row
//   +-- cfs[cf id]              ManifestCfRows, one per column family
//   |   +-- cf                  ManifestCfRow
//   |   +-- files[number]       ManifestFileRow, one per SST file
//   |   +-- blob_files[number]  ManifestBlobFileRow, one per blob file
//   +-- wals[number]            WalMetadata, one per tracked WAL
//
// Memory: each file row holds a copy of the persisted fields of one
// FileMetaData plus a std::map node, so a full row set takes about as much
// memory as the FileMetaData objects of all live SST files (estimate, not
// measured).
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
// type. Column family rows are never deleted. A column family drop writes its
// row with `dropped` set, and its file and blob file rows stay (see
// ManifestCfRow::dropped).
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

// The column family and its Versions before and after one MANIFEST write: a
// column family that gets a new Version, or the column family that the write
// drops. A column family add has no entry, because its first Version is created
// when the write is installed. For a column family drop, no new Version is
// installed, so `base` and `new_version` are both its current Version.
struct ManifestBatchVersion {
  const ColumnFamilyData* cfd = nullptr;
  // The Version that the edits of the write were applied to.
  const VersionStorageInfo* base = nullptr;
  // The Version after the write. For a column family drop, this is the current
  // Version (see above).
  const VersionStorageInfo* new_version = nullptr;
};

// Builds the row changes of one batch, as VersionSet::ProcessManifestWrites()
// groups it, before the write is installed.
//
// Parameters:
// - `edits` are the edits of the batch, in order, including the edit that
//   drops a column family. ProcessManifestWrites() leaves out a queued edit
//   whose column family was dropped before the edit's turn.
// - `batch_versions` has one entry for each column family that gets a new
//   Version, plus one for the dropped column family of a column family drop
//   (see ManifestBatchVersion).
// - `new_cf_options` holds the options of the new column family for a column
//   family add, and is null otherwise.
// - `old_rows` holds the rows before the write. Only its DB row and column
//   family rows are used, not its file rows. Each caller keeps its own copy of
//   these rows:
//   - The debug check passes the rows that it tracks for the installed
//     metadata (VersionSet::debug_manifest_rows_).
// - `wals` is the live WAL set (VersionSet::wals_) before the write. The
//   builder copies it and applies the WAL edits to the copy, so that the edits
//   use its cutoff for obsolete WALs: WalSet::AddWal() ignores a WAL below the
//   cutoff. `old_rows.wals` has the stored WAL rows only, without the cutoff.
//
// Key choices: the file rows come from the edits, in order, the same way
// recovery replays them. The DB row fields follow the rules below. They give
// the values that recovery gives, except the prev log number, which follows
// ProcessManifestWrites(). The other rows have these sources:
// - Existing column family: the name, the comparator and the timestamp
//   options come from ColumnFamilyData. The compaction cursors and the dropped
//   blob file list come from the new Version, because it already holds the
//   result of the edits. The log number and full_history_ts_low start from
//   the row in `old_rows` and advance with the edits, and a drop edit sets
//   the dropped flag, because the new Version does not hold these values.
// - New column family: the row comes from the add edit and `new_cf_options`.
// - Blob file rows: they come from a comparison of the base Version and the
//   new Version. When no SST file links to a blob file and all its blobs are
//   garbage, VersionBuilder leaves the file out of the new Version, and no
//   edit records this removal. So the comparison is the way to find removed
//   blob files.
// Rules:
// - DB row: the next file number, the DB id and the committed OPTIONS file
//   number come from the last edit that sets them. The last sequence, the min
//   log number to keep and the max column family take the largest value. A
//   column family add also raises the max column family to the new id.
// - DB row, prev log number: three places hold this value. Only tests set a
//   value other than 0.
//   - The DB row takes the value of the first edit in `edits`, which is the
//     first edit written to the MANIFEST.
//   - The installed state: ProcessManifestWrites() sets prev_log_number_ in
//     memory from the first edit of the first queued writer.
//   - Recovery keeps the last value that any edit sets. Column family add and
//     drop edits set none, so after recovery the value can differ from the
//     other two.
//   The first two use different edits when the first queued writer's column
//   family was dropped while it waited. Both values are then 0, so they still
//   match:
//   1. That writer's edits are left out of `edits`, and they keep the default
//      0, because edits of a dropped column family do not go through
//      LogAndApplyHelper(). So prev_log_number_ becomes 0.
//   2. The first edit in `edits` got prev_log_number_ from
//      LogAndApplyHelper(). That value is 0, because the drop write set
//      prev_log_number_ from the drop edit, which has none.
// - Files: within one edit, deleted files are applied before new files, so a
//   file that moves to another level becomes one upsert.
// - Column family row: the log number is the largest of the row and the
//   edits. full_history_ts_low only increases. A column family drop writes the
//   row with `dropped` set (see ManifestCfRow::dropped).
// - Blob file rows: a row is written when the blob file is new or its garbage
//   changed, and deleted when the new Version no longer has the blob file,
//   because no VersionEdit says that a blob file is gone.
// - WAL rows: the WAL edits are applied to a copy of `wals`, and the result is
//   compared with `wals`.
//
// The rows can differ from the in-memory state after the write (see "In-memory
// differences" at the top of this file).
//
// Cost: for a batch that succeeds, the function scans the edits once for the
// DB, SST file and WAL changes, and once more for each column family in
// `batch_versions`. Each SST file change is an insert or erase in an ordered
// map or set, so it costs logarithmic time in the number of changes. For each
// column family in `batch_versions`, it scans the blob files of the base and
// the new Version and looks up each blob file by number in the other Version.
// It copies the live WAL set at the start and compares the two WAL sets at the
// end, also for a batch with no WAL edit.
//
// Errors: returns the error of a WAL edit that the WAL set rejects.
Status BuildManifestRowChanges(
    const autovector<VersionEdit*>& edits,
    const std::vector<ManifestBatchVersion>& batch_versions,
    const ColumnFamilyOptions* new_cf_options, const ManifestRows& old_rows,
    const WalSet& wals, ManifestRowChanges* changes);

// Applies all changes of one MANIFEST write to `rows`. Apply them together,
// because the rows have no atomic group marker. Deletes are applied before
// upserts. For the rows of a dropped column family, see ManifestCfRow::dropped.
void ApplyManifestRowChanges(const ManifestRowChanges& changes,
                             ManifestRows* rows);

// Converts `rows` into encoded full-state VersionEdit records and appends them
// to `records`. The records hold the rows of the column families that are not
// dropped, with their files. Writing the records to a new MANIFEST file and
// recovering from it gives the state that the rows describe, with these
// results:
// - Recovery drops every tracked WAL below the DB row's min_log_number_to_keep,
//   because the records include a WAL deletion with that minimum (step 2
//   below). A MANIFEST that WriteCurrentStateToManifest() writes behaves the
//   same way. For example, when the rows hold WAL 41 and the minimum is 42,
//   recovery drops WAL 41.
// - Recovery sets the next file number to the recorded value plus one (see
//   "Next file number after recovery" below).
//
// Record order:
// 1. The DB id, when it is not empty. It is written even when
//    DBOptions::write_dbid_to_manifest is false; it is the same value as in
//    the IDENTITY file.
// 2. The WAL additions, then one WAL deletion record with the DB row's
//    min_log_number_to_keep, as WriteCurrentStateToManifest() does with
//    VersionSet::min_log_number_to_keep(). The comment of
//    ManifestDbRow::min_log_number_to_keep explains the two WAL minimums.
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
// writes the same state in a different layout, with these differences:
// 1. It puts the min log number to keep in the default column family record.
// 2. It puts the committed OPTIONS file number in its own record.
// 3. It writes no next file number, prev log number or max column family.
//    The edits of the current MANIFEST write follow those records and usually
//    hold these values. A column family add or drop edit holds no prev log
//    number (recovery then uses 0). The max column family is not written when
//    the first writer's column family was dropped while it waited, because
//    ProcessManifestWrites() skips the edits of that writer. So a new MANIFEST
//    file can miss the prev log number or the max column family. The records
//    of this function always write them.
// 4. It ends with a kLastCompactedManifestFileSize record, which the rows do
//    not keep. After recovery from the records of this function,
//    TuneMaxManifestFileSize() starts from DBOptions::max_manifest_file_size.
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

// Public rows and internal rows: ManifestRows (this header) is the internal
// model of all rows of a DB. ManifestRow (include/rocksdb/manifest_store.h,
// without the final "s") is one public row: a key and its encoded fields. The
// two functions below convert between them. Their parameters say which form
// they hold: `internal_rows` or `public_rows`.

// The DB row fields of the public rows (include/rocksdb/manifest_store.h) that
// ManifestDbRow does not hold. RocksDB owns and writes these fields. They are
// not the store-owned values of the DB row (ManifestStoreDbState). The defaults
// are those of a DB whose MANIFEST file is the authority
// (ManifestAuthority::kFile). No RocksDB code path computes
// in_flight_file_number, so callers leave it at its default 0. The value 0
// means that every file must be kept, which is always safe. The row format
// version is not a member: the writer always writes
// kManifestRowFormatVersionLatest, and the reader only checks it.
struct ManifestDbRowExtraFields {
  ManifestAuthority authority = ManifestAuthority::kFile;
  ManifestCopyMode copy_mode = ManifestCopyMode::kOff;
  uint64_t copy_file_number = 0;
  uint64_t in_flight_file_number = 0;

  bool operator==(const ManifestDbRowExtraFields&) const = default;
};

// Converts `internal_rows` to public rows, including the rows of dropped column
// families. On OK, `*public_rows` is replaced: the DB row (with `extra_fields`)
// is first, then for each column family in id order its row, its file rows and
// its blob file rows (each in number order), then the WAL rows in number order.
// Each row holds its fields in the canonical encoding, without the fields that
// hold their default. `feature_version` is the newest feature version that the
// caller allows, from 1 to kManifestRowFeatureVersionLatest. All fields of this
// release belong to feature version 1, so every valid value gives the same
// rows. `extra_fields` must hold known authority and copy mode values; this
// function writes them without a check. It always writes
// kManifestRowFormatVersionLatest as the row format version. Every file row
// must have non-empty smallest and largest keys, as the rows from
// VersionSet::GetManifestRows() do.
//
// Errors: returns InvalidArgument for a `feature_version` out of range.
Status ManifestRowsToPublicRows(const ManifestRows& internal_rows,
                                const ManifestDbRowExtraFields& extra_fields,
                                uint32_t feature_version,
                                std::vector<ManifestRow>* public_rows);

// Converts public rows back to `internal_rows` and `extra_fields`.
// `public_db_row` is the DB row, and `other_public_rows` holds the other rows
// in any order. To decode the output of ManifestRowsToPublicRows(), pass its
// first element as `public_db_row` and the rest as `other_public_rows`. For a
// store scan, pass the `db_row` field of the ManifestDbRowResult that
// ManifestRowVisitor::OnDbRow() receives, and the rows of OnRow(). It drops an
// unknown field without the "must understand" bit, and reads a temperature
// value that is not below Temperature::kLastTemperature as
// Temperature::kUnknown. On error, `*internal_rows` and `*extra_fields` hold
// partly decoded data and must not be used.
//
// Error order: the DB row is decoded first, in these stages:
// 1. The reader checks the DB row key, and parses the field IDs and lengths
//    before it reads any field value. A bad key, a field encoding that cannot
//    be parsed, or a field ID that is 0 or not larger than the one before it
//    gives Corruption.
// 2. It reads the field values in ID order. The row format version has field
//    ID 1, so when it is present it is read first, and a version from a newer
//    binary gives NotSupported before a later field value can give an error.
// 3. After it has read the present fields, it checks for missing "always
//    present" fields.
// The other rows are decoded only after the DB row.
//
// Errors:
// - NotSupported if a newer binary wrote the rows: a row format version above
//   kManifestRowFormatVersionLatest, an unknown "must understand" field, or an
//   unknown authority or copy mode value.
// - Corruption for a bad encoding, a bad value (for example a path id above 3,
//   a key with an unknown value type, a blob file with only one of checksum
//   method and value, a compaction cursor level above 65536 or not in
//   increasing order, or a WAL synced size of 2^64 - 1), a file number above
//   kFileNumberMask (2^62 - 1; the top 2 bits of a packed file number hold the
//   path id), a key that is too short for the timestamp size of its column
//   family, a missing "always present" field, a repeated key, a key with parts
//   that its row type does not use, a DB row inside `other_public_rows`, an
//   unknown row type, or a file or blob file row whose column family has no
//   row.
Status PublicRowsToManifestRows(
    const ManifestRow& public_db_row,
    const std::vector<ManifestRow>& other_public_rows,
    ManifestRows* internal_rows, ManifestDbRowExtraFields* extra_fields);

}  // namespace ROCKSDB_NAMESPACE
