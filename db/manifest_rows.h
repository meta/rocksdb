//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/version_edit.h"
#include "db/wal_edit.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

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
// This file defines the in-memory form of the rows and two conversions:
// - VersionSet::GetManifestRows() reads the current state into rows.
// - ManifestRowsToRecords() turns rows into encoded "full-state" VersionEdit
//   records, like the ones that VersionSet::WriteCurrentStateToManifest()
//   writes at the start of a new MANIFEST file. The existing recovery code
//   replays them.
//
// The rows hold every field that recovery reads from a MANIFEST. Three kinds
// of VersionEdit content have no row field:
// - kInAtomicGroup: an atomic group is always complete inside one MANIFEST
//   write. The rows have no atomic group marker. If a store updates the rows
//   after each MANIFEST write, it must commit all row changes of that write
//   together.
// - kLastCompactedManifestFileSize: it only controls when a MANIFEST file is
//   rewritten.
// - kSubcompactionProgress: only remote compaction workers write it, to their
//   own progress file, never to the DB MANIFEST.
// Deprecated tags (kNewFile, kNewFile2, kNewFile3, the *_DEPRECATED tags) are
// only read from old MANIFEST files, and their content is held in the rows
// below in its current form.

// The row with the fields that belong to the whole DB.
struct ManifestDbRow {
  // The DB id, also kept in the IDENTITY file.
  std::string db_id;
  // The smallest file number that is not used yet.
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
  // unset (empty) key means the level has no cursor.
  std::vector<InternalKey> compact_cursors;
  // Blob files that best-efforts BlobDB repair left out of the column family
  // (VersionStorageInfo::GetDroppedBlobFiles()). Sorted, with no duplicates.
  std::vector<uint64_t> dropped_blob_files;
  // True after the column family is dropped. VersionSet::GetManifestRows()
  // leaves out dropped column families, so it never sets this flag; code that
  // keeps the rows up to date across a column family drop sets it.
  // ManifestRowsToRecords() leaves out the column family and its file and blob
  // file rows.
  bool dropped = false;
};

// The row of one SST file. `meta` holds the fields of the file's FileMetaData
// that a MANIFEST stores. The in-memory fields (reference count, table reader,
// compaction state, statistics) are reset when the row is made, so a row does
// not depend on the Version it came from.
struct ManifestFileRow {
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
// Compared with WriteCurrentStateToManifest(): that function writes the same
// content in a different layout. It puts the min log number to keep in the
// default column family record and the committed OPTIONS file number in its
// own record. The next file number, the prev log number and the max column
// family come from the edits of the same MANIFEST write, which
// VersionSet::ProcessManifestWrites() appends after the records of that
// function.
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
// name of the first row or group of rows that differs. It does not use
// ManifestRowsToRecords(), so that a test that compares rows before and after
// a round trip finds a field that the converter drops. It compares file rows
// through their encoding as a new file record, which ManifestRowsToRecords()
// also uses, so a file field that this encoding drops is not found. It
// compares all other rows field by field.
bool ManifestRowsEqual(const ManifestRows& a, const ManifestRows& b,
                       std::string* diff);

}  // namespace ROCKSDB_NAMESPACE
