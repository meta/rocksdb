//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/manifest_rows.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <tuple>

#include "db/column_family.h"
#include "db/version_set.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Returns the column family row of `cfd`, with the compaction cursors and the
// dropped blob files of `vstorage`, which must be a Version of `cfd`.
// `log_number` and `full_history_ts_low` are passed in because a MANIFEST write
// records the values of its edits, which can differ from the values in `cfd` at
// the time of the call.
// REQUIRES: the fields read from `cfd` and `vstorage` do not change during the
// call. Holding the DB mutex is enough. The debug check also calls this
// without the mutex (see "Threads" at VersionSet::debug_manifest_rows_).
ManifestCfRow MakeManifestCfRow(const ColumnFamilyData& cfd,
                                const VersionStorageInfo& vstorage,
                                uint64_t log_number,
                                const std::string& full_history_ts_low) {
  ManifestCfRow cf;
  cf.name = cfd.GetName();
  cf.comparator_name = cfd.user_comparator()->Name();
  cf.timestamp_size = cfd.user_comparator()->timestamp_size();
  cf.persist_user_defined_timestamps =
      cfd.ioptions().persist_user_defined_timestamps;
  cf.log_number = log_number;
  cf.full_history_ts_low = full_history_ts_low;
  cf.compact_cursors = vstorage.GetCompactCursors();
  // VersionStorageInfo keeps one compaction cursor per level, and most are
  // unset. The row of a new column family has no cursors. Drop the unset
  // cursors at the end, so that both give the same row.
  while (!cf.compact_cursors.empty() && cf.compact_cursors.back().unset()) {
    cf.compact_cursors.pop_back();
  }
  cf.dropped_blob_files = vstorage.GetDroppedBlobFiles();
  cf.dropped = cfd.IsDropped();
  return cf;
}

ManifestBlobFileRow MakeBlobFileRow(const BlobFileMetaData& meta) {
  ManifestBlobFileRow row;
  row.addition =
      BlobFileAddition(meta.GetBlobFileNumber(), meta.GetTotalBlobCount(),
                       meta.GetTotalBlobBytes(), meta.GetChecksumMethod(),
                       meta.GetChecksumValue());
  row.garbage_blob_count = meta.GetGarbageBlobCount();
  row.garbage_blob_bytes = meta.GetGarbageBlobBytes();
  return row;
}

// Returns the row of file `f` on `level`. AddExistingFile() copies only the
// fields that a MANIFEST stores, so the row does not keep the table reader or
// other in-memory state of the file.
ManifestFileRow MakeFileRow(int level, const FileMetaData& f) {
  VersionEdit edit;
  edit.AddExistingFile(level, f, /*provenance_flags_to_set=*/0);
  return ManifestFileRow{level, std::move(edit.GetMutableNewFiles()[0].second)};
}

// Sets the column family row and the file and blob file rows of one column
// family in `rows`, from `cfd` and `vstorage`, the storage of its current
// Version.
// REQUIRES: DB mutex held.
void SetManifestCfRows(const ColumnFamilyData& cfd,
                       const VersionStorageInfo& vstorage, ManifestRows* rows) {
  assert(rows != nullptr);
  ManifestCfRows& cf_rows = rows->cfs[cfd.GetID()];
  cf_rows = ManifestCfRows();
  cf_rows.cf = MakeManifestCfRow(cfd, vstorage, cfd.GetLogNumber(),
                                 cfd.GetFullHistoryTsLow());
  for (int level = 0; level < vstorage.num_levels(); ++level) {
    for (const FileMetaData* f : vstorage.LevelFiles(level)) {
      cf_rows.files[f->fd.GetNumber()] = MakeFileRow(level, *f);
    }
  }
  for (const auto& meta : vstorage.GetBlobFiles()) {
    cf_rows.blob_files[meta->GetBlobFileNumber()] = MakeBlobFileRow(*meta);
  }
}

}  // namespace

void VersionSet::SetManifestDbRowAndWals(ManifestRows* rows) const {
  ManifestDbRow& db = rows->db;
  db = ManifestDbRow();
  db.db_id = db_id_;
  db.next_file_number = current_next_file_number();
  db.last_sequence = descriptor_last_sequence_;
  db.prev_log_number = prev_log_number_;
  db.min_log_number_to_keep = min_log_number_to_keep();
  db.max_column_family = column_family_set_->GetMaxColumnFamily();
  const OptionsFileManifestState options_state = options_file_manifest_state();
  if (options_state.has_committed_options_file_number) {
    db.committed_options_file_number =
        options_state.committed_options_file_number;
  }
  rows->wals = wals_.GetWals();
}

void VersionSet::GetManifestRows(ManifestRows* rows) const {
  assert(rows != nullptr);
  *rows = ManifestRows();
  SetManifestDbRowAndWals(rows);

  for (ColumnFamilyData* cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    SetManifestCfRows(*cfd, *cfd->current()->storage_info(), rows);
  }
}

Status BuildManifestRowChanges(
    const autovector<VersionEdit*>& edits,
    const std::vector<ManifestBatchVersion>& batch_versions,
    const ColumnFamilyOptions* new_cf_options, const ManifestRows& old_rows,
    const WalSet& wals, ManifestRowChanges* changes) {
  assert(!edits.empty());
  assert(changes != nullptr);
  *changes = ManifestRowChanges();
  ManifestDbRow& db = changes->db;
  db = old_rows.db;
  // See "prev log number" in the rules at the declaration.
  db.prev_log_number = edits.front()->GetPrevLogNumber();
  WalSet new_wals = wals;

  // 1. Edit by edit: the DB row, the WAL set, the file rows and the row of an
  //    added column family.
  for (const VersionEdit* e : edits) {
    if (e->HasDbId()) {
      db.db_id = e->GetDbId();
    }
    if (e->HasNextFile()) {
      db.next_file_number = e->GetNextFile();
    }
    if (e->HasLastSequence()) {
      db.last_sequence = std::max(db.last_sequence, e->GetLastSequence());
    }
    if (e->HasMinLogNumberToKeep()) {
      db.min_log_number_to_keep =
          std::max(db.min_log_number_to_keep, e->GetMinLogNumberToKeep());
    }
    if (e->HasMaxColumnFamily()) {
      db.max_column_family =
          std::max(db.max_column_family, e->GetMaxColumnFamily());
    }
    if (e->HasCommittedOptionsFileNumber()) {
      db.committed_options_file_number = e->GetCommittedOptionsFileNumber();
    }
    Status s;
    if (e->IsWalAddition()) {
      s = new_wals.AddWals(e->GetWalAdditions());
    } else if (e->IsWalDeletion()) {
      s = new_wals.DeleteWalsBefore(e->GetWalDeletion().GetLogNumber());
    }
    if (!s.ok()) {
      return s;
    }

    const uint32_t cf_id = e->GetColumnFamily();
    for (const auto& deleted : e->GetDeletedFiles()) {
      const std::pair<uint32_t, uint64_t> key(cf_id, deleted.second);
      changes->file_upserts.erase(key);
      changes->file_deletes.insert(key);
    }
    for (const auto& [level, meta] : e->GetNewFiles()) {
      const std::pair<uint32_t, uint64_t> key(cf_id, meta.fd.GetNumber());
      changes->file_deletes.erase(key);
      changes->file_upserts[key] = MakeFileRow(level, meta);
    }
    if (e->IsColumnFamilyAdd()) {
      assert(new_cf_options != nullptr);
      // The edit can carry a max column family that is lower than the new id,
      // or none; creating the column family raises it to the new id, also
      // during recovery.
      db.max_column_family = std::max(db.max_column_family, cf_id);
      ManifestCfRow& cf = changes->cf_upserts[cf_id];
      cf.name = e->GetColumnFamilyName();
      cf.comparator_name = new_cf_options->comparator->Name();
      cf.timestamp_size = new_cf_options->comparator->timestamp_size();
      cf.persist_user_defined_timestamps =
          new_cf_options->persist_user_defined_timestamps;
      cf.log_number = e->GetLogNumber();
    }
  }

  // 2. The column family row and the blob file rows of each new Version.
  for (const ManifestBatchVersion& v : batch_versions) {
    const uint32_t cf_id = v.cfd->GetID();
    const Comparator* ucmp = v.cfd->user_comparator();
    const std::map<uint32_t, ManifestCfRows>::const_iterator base_cf =
        old_rows.cfs.find(cf_id);
    // Every column family in `batch_versions` existed before the batch, because
    // a column family add has no entry. So `old_rows` has its row. The check
    // below only keeps a release build from reading past the end.
    assert(base_cf != old_rows.cfs.end());
    uint64_t log_number = 0;
    std::string ts_low;
    if (base_cf != old_rows.cfs.end()) {
      log_number = base_cf->second.cf.log_number;
      ts_low = base_cf->second.cf.full_history_ts_low;
    }
    bool dropped = false;
    for (const VersionEdit* e : edits) {
      if (e->GetColumnFamily() != cf_id) {
        continue;
      }
      dropped = dropped || e->IsColumnFamilyDrop();
      if (e->HasLogNumber()) {
        log_number = std::max(log_number, e->GetLogNumber());
      }
      // Same rule as ColumnFamilyData::SetFullHistoryTsLow().
      if (e->HasFullHistoryTsLow() && ucmp->timestamp_size() > 0 &&
          (ts_low.empty() ||
           ucmp->CompareTimestamp(e->GetFullHistoryTsLow(), ts_low) > 0)) {
        ts_low = e->GetFullHistoryTsLow();
      }
    }
    ManifestCfRow& cf = changes->cf_upserts[cf_id];
    cf = MakeManifestCfRow(*v.cfd, *v.new_version, log_number, ts_low);
    // The drop is not installed yet, so cfd->IsDropped() is still false and
    // the flag comes from the edits.
    cf.dropped = cf.dropped || dropped;

    for (const auto& meta : v.new_version->GetBlobFiles()) {
      const uint64_t number = meta->GetBlobFileNumber();
      ManifestBlobFileRow row = MakeBlobFileRow(*meta);
      const std::shared_ptr<BlobFileMetaData> base_meta =
          v.base->GetBlobFileMetaData(number);
      if (base_meta == nullptr || MakeBlobFileRow(*base_meta) != row) {
        changes->blob_file_upserts[{cf_id, number}] = std::move(row);
      }
    }
    for (const auto& meta : v.base->GetBlobFiles()) {
      const uint64_t number = meta->GetBlobFileNumber();
      if (v.new_version->GetBlobFileMetaData(number) == nullptr) {
        changes->blob_file_deletes.insert({cf_id, number});
      }
    }
  }

  // 3. The WAL rows: compare the new WAL set with the old one.
  const std::map<WalNumber, WalMetadata>& old_wals = wals.GetWals();
  for (const auto& [number, meta] : new_wals.GetWals()) {
    const std::map<WalNumber, WalMetadata>::const_iterator it =
        old_wals.find(number);
    if (it == old_wals.end() || it->second != meta) {
      changes->wal_upserts[number] = meta;
    }
  }
  for (const auto& wal : old_wals) {
    if (new_wals.GetWals().count(wal.first) == 0) {
      changes->wal_deletes.insert(wal.first);
    }
  }
  return Status::OK();
}

void ApplyManifestRowChanges(const ManifestRowChanges& changes,
                             ManifestRows* rows) {
  assert(rows != nullptr);
  rows->db = changes.db;
  for (const auto& [cf_id, cf] : changes.cf_upserts) {
    rows->cfs[cf_id].cf = cf;
  }
  for (const auto& [cf_id, number] : changes.file_deletes) {
    // A row is either written or deleted, never both.
    assert(changes.file_upserts.count({cf_id, number}) == 0);
    rows->cfs[cf_id].files.erase(number);
  }
  for (const auto& [key, row] : changes.file_upserts) {
    rows->cfs[key.first].files[key.second] = row;
  }
  for (const auto& [cf_id, number] : changes.blob_file_deletes) {
    rows->cfs[cf_id].blob_files.erase(number);
  }
  for (const auto& [key, row] : changes.blob_file_upserts) {
    rows->cfs[key.first].blob_files[key.second] = row;
  }
  for (const WalNumber number : changes.wal_deletes) {
    rows->wals.erase(number);
  }
  for (const auto& [number, meta] : changes.wal_upserts) {
    rows->wals[number] = meta;
  }
}

#ifndef NDEBUG
namespace {

// Stops a debug build when the rows that the row changes give differ from the
// rows of a new Version or of the installed state.
void AssertManifestRowsEqual(const ManifestRows& expected,
                             const ManifestRows& actual) {
  std::string diff;
  if (!ManifestRowsEqual(expected, actual, &diff)) {
    fprintf(stderr, "Structured MANIFEST row changes are wrong: %s\n",
            diff.c_str());
    assert(false);
  }
}

// Checks the rows of one column family against its new Version `v.new_version`
// in phase 1 of the debug check, without the DB mutex (see "Threads" at
// VersionSet::debug_manifest_rows_ for why each object read here is safe).
//
// The log number, full_history_ts_low and the dropped flag are taken from
// `cf_rows`. VersionSet::DebugCheckRowsAfterInstall() checks them after
// install.
//
// It compares:
// - the file number and the level of every file, which finds a lost file add,
//   a lost file delete and a wrong level;
// - all fields of the files that the write adds (`changes.file_upserts`);
// - the column family row and the blob file rows.
void AssertCfRowsMatchVersion(const ManifestBatchVersion& v,
                              const ManifestRowChanges& changes,
                              const ManifestCfRows& cf_rows) {
  const uint32_t cf_id = v.cfd->GetID();
  ManifestRows expected;
  ManifestCfRows& expected_cf = expected.cfs[cf_id];
  expected_cf.cf =
      MakeManifestCfRow(*v.cfd, *v.new_version, cf_rows.cf.log_number,
                        cf_rows.cf.full_history_ts_low);
  expected_cf.cf.dropped = cf_rows.cf.dropped;
  for (const auto& meta : v.new_version->GetBlobFiles()) {
    expected_cf.blob_files[meta->GetBlobFileNumber()] = MakeBlobFileRow(*meta);
  }
  ManifestRows actual;
  ManifestCfRows& actual_cf = actual.cfs[cf_id];
  actual_cf.cf = cf_rows.cf;
  actual_cf.blob_files = cf_rows.blob_files;

  size_t num_files = 0;
  for (int level = 0; level < v.new_version->num_levels(); ++level) {
    for (const FileMetaData* f : v.new_version->LevelFiles(level)) {
      ++num_files;
      const uint64_t number = f->fd.GetNumber();
      const std::map<uint64_t, ManifestFileRow>::const_iterator it =
          cf_rows.files.find(number);
      if (it == cf_rows.files.end() || it->second.level != level) {
        fprintf(stderr,
                "Structured MANIFEST row changes are wrong: column family "
                "%" PRIu32 " has no row for file %" PRIu64 " at level %d\n",
                cf_id, number, level);
        assert(false);
        continue;
      }
      if (changes.file_upserts.count({cf_id, number}) > 0) {
        expected_cf.files[number] = MakeFileRow(level, *f);
        actual_cf.files[number] = it->second;
      }
    }
  }
  // Every file of the Version has a row, so equal counts mean that no row is
  // left over.
  assert(num_files == cf_rows.files.size());
  AssertManifestRowsEqual(expected, actual);
}

}  // namespace

void VersionSet::DebugCheckRowsBeforeInstall(
    const autovector<VersionEdit*>& batch_edits,
    const autovector<Version*>& new_versions,
    const std::vector<std::unique_ptr<BaseReferencedVersionBuilder>>&
        builder_guards,
    ColumnFamilyData* first_cfd, const ColumnFamilyOptions* new_cf_options) {
  assert(debug_manifest_rows_ != nullptr);
  // This runs without the DB mutex (see "Threads" at debug_manifest_rows_).
  std::vector<ManifestBatchVersion> batch_versions;
  batch_versions.reserve(new_versions.size() + 1);
  for (size_t i = 0; i < new_versions.size(); ++i) {
    // builder_guards[i] holds the base Version of new_versions[i].
    batch_versions.push_back({new_versions[i]->cfd(),
                              builder_guards[i]->version()->storage_info(),
                              new_versions[i]->storage_info()});
  }
  // A column family add or drop is always the only edit of its batch (see
  // ProcessManifestWrites()), so checking the first edit is enough.
  if (batch_edits.front()->IsColumnFamilyDrop()) {
    const VersionStorageInfo* current = first_cfd->current()->storage_info();
    batch_versions.push_back({first_cfd, current, current});
  }
  ManifestRowChanges changes;
  if (!BuildManifestRowChanges(batch_edits, batch_versions, new_cf_options,
                               *debug_manifest_rows_, wals_, &changes)
           .ok()) {
    // ProcessManifestWrites() rejects the same WAL edit when it installs the
    // write, so the write is not installed. Fill the rows again from the
    // installed state on the next call.
    debug_manifest_rows_.reset();
    return;
  }
  // The next file number in the rows only grows. Each batch records
  // next_file_number_ under the DB mutex (LogAndApplyHelper()),
  // next_file_number_ only grows, and batches are written one at a time.
  assert(changes.db.next_file_number >=
         debug_manifest_rows_->db.next_file_number);
  // After a successful MANIFEST write, installing it fails only for a WAL
  // edit that BuildManifestRowChanges() already rejected, so the changes can
  // be applied in place.
  ApplyManifestRowChanges(changes, debug_manifest_rows_.get());

  for (const ManifestBatchVersion& v : batch_versions) {
    AssertCfRowsMatchVersion(v, changes,
                             debug_manifest_rows_->cfs[v.cfd->GetID()]);
  }
}

void VersionSet::DebugCheckRowsAfterInstall(
    const autovector<VersionEdit*>& batch_edits) {
  // Phase 1 resets the rows when BuildManifestRowChanges() rejects a WAL edit.
  // Installing such a write fails, so this is only a guard.
  if (debug_manifest_rows_ == nullptr) {
    return;
  }
  // Phase 2 of the debug check, see VersionSet::debug_manifest_rows_.
  ManifestRows expected;
  ManifestRows actual;
  actual.db = debug_manifest_rows_->db;
  actual.wals = debug_manifest_rows_->wals;
  SetManifestDbRowAndWals(&expected);
  // Allowed differences between the rows and memory (see "In-memory
  // differences" in db/manifest_rows.h):
  // - Next file number: the row can be smaller than memory, never larger.
  // - Max column family: the row can be smaller than memory, never larger.
  //   The loop below checks that it is at least the id of each added column
  //   family.
  // - Committed OPTIONS file number: memory can have none while the row keeps
  //   one. The row's value is then accepted.
  // - Log number: the loop below checks it with bounds.
  assert(actual.db.next_file_number <= expected.db.next_file_number);
  expected.db.next_file_number = actual.db.next_file_number;
  assert(actual.db.max_column_family <= expected.db.max_column_family);
  expected.db.max_column_family = actual.db.max_column_family;
  if (!expected.db.committed_options_file_number.has_value()) {
    expected.db.committed_options_file_number =
        actual.db.committed_options_file_number;
  }
  for (const VersionEdit* e : batch_edits) {
    // Recovery takes the DB id from the MANIFEST. DBImpl also sets it in
    // memory before it writes it, but a test can write only the edit.
    if (e->HasDbId()) {
      expected.db.db_id = e->GetDbId();
    }
    if (e->IsWalManipulation() || e->IsOptionsFileManipulation()) {
      // These edits change no column family.
      continue;
    }
    const uint32_t cf_id = e->GetColumnFamily();
    const std::map<uint32_t, ManifestCfRows>::const_iterator it =
        debug_manifest_rows_->cfs.find(cf_id);
    assert(it != debug_manifest_rows_->cfs.end());
    const ManifestCfRow& cf = it->second.cf;
    ColumnFamilyData* cfd = column_family_set_->GetColumnFamily(cf_id);
    if (cfd == nullptr || cfd->IsDropped()) {
      assert(cf.dropped);
      continue;
    }
    assert(!cf.dropped);
    assert(cf.log_number <= cfd->GetLogNumber());
    assert(!e->HasLogNumber() || cf.log_number >= e->GetLogNumber());
    assert(cf.full_history_ts_low == cfd->GetFullHistoryTsLow());
    if (e->IsColumnFamilyAdd()) {
      assert(actual.db.max_column_family >= cf_id);
      // A new column family has no Version in DebugCheckRowsBeforeInstall(),
      // so its whole row is compared here. It has no files yet.
      expected.cfs[cf_id].cf =
          MakeManifestCfRow(*cfd, *cfd->current()->storage_info(),
                            cf.log_number, cfd->GetFullHistoryTsLow());
      actual.cfs[cf_id].cf = cf;
    }
  }
  AssertManifestRowsEqual(expected, actual);
  TEST_SYNC_POINT("VersionSet::DebugCheckRowsAfterInstall:Done");
}
#endif  // !NDEBUG

Status ManifestRowsToRecords(const ManifestRows& rows,
                             std::vector<std::string>* records) {
  assert(records != nullptr);
  const ManifestDbRow& db = rows.db;

  // Each record with files must be encoded with the timestamp size of its
  // column family; the other records have no file boundaries to change.
  auto add_record = [records](const VersionEdit& edit, size_t ts_sz) {
    std::string record;
    if (!edit.EncodeTo(&record, ts_sz)) {
      return Status::Corruption("Unable to encode VersionEdit: " +
                                edit.DebugString(true));
    }
    records->push_back(std::move(record));
    return Status::OK();
  };

  if (!db.db_id.empty()) {
    VersionEdit edit;
    edit.SetDBId(db.db_id);
    Status s = add_record(edit, /*ts_sz=*/0);
    if (!s.ok()) {
      return s;
    }
  }

  // AddWal() and DeleteWalsBefore() cannot be in the same VersionEdit.
  if (!rows.wals.empty()) {
    VersionEdit edit;
    for (const auto& [wal_number, wal_meta] : rows.wals) {
      edit.AddWal(wal_number, wal_meta);
    }
    Status s = add_record(edit, /*ts_sz=*/0);
    if (!s.ok()) {
      return s;
    }
  }
  {
    VersionEdit edit;
    edit.DeleteWalsBefore(db.min_log_number_to_keep);
    Status s = add_record(edit, /*ts_sz=*/0);
    if (!s.ok()) {
      return s;
    }
  }

  for (const auto& [cf_id, cf_rows] : rows.cfs) {
    const ManifestCfRow& cf = cf_rows.cf;
    if (cf.dropped) {
      continue;
    }

    VersionEdit cf_edit;
    if (cf_id != 0) {
      // The default column family always exists and is never added.
      cf_edit.AddColumnFamily(cf.name);
      cf_edit.SetColumnFamily(cf_id);
    }
    cf_edit.SetComparatorName(cf.comparator_name);
    cf_edit.SetPersistUserDefinedTimestamps(cf.persist_user_defined_timestamps);
    Status s = add_record(cf_edit, /*ts_sz=*/0);
    if (!s.ok()) {
      return s;
    }

    VersionEdit files_edit;
    files_edit.SetColumnFamily(cf_id);
    for (const auto& file : cf_rows.files) {
      files_edit.AddExistingFile(file.second.level, file.second.meta,
                                 /*provenance_flags_to_set=*/0);
    }
    files_edit.SetCompactCursors(cf.compact_cursors);
    for (const auto& blob_file : cf_rows.blob_files) {
      const BlobFileAddition& addition = blob_file.second.addition;
      files_edit.AddBlobFile(
          addition.GetBlobFileNumber(), addition.GetTotalBlobCount(),
          addition.GetTotalBlobBytes(), addition.GetChecksumMethod(),
          addition.GetChecksumValue());
      if (blob_file.second.garbage_blob_count > 0) {
        files_edit.AddBlobFileGarbage(addition.GetBlobFileNumber(),
                                      blob_file.second.garbage_blob_count,
                                      blob_file.second.garbage_blob_bytes);
      }
    }
    for (const uint64_t blob_file_number : cf.dropped_blob_files) {
      files_edit.AddDroppedBlobFile(blob_file_number);
    }
    files_edit.SetLogNumber(cf.log_number);
    if (!cf.full_history_ts_low.empty()) {
      files_edit.SetFullHistoryTsLow(cf.full_history_ts_low);
    }
    // AddExistingFile() raises the last sequence of the edit to the largest
    // sequence number of its files. Recovery requires the last sequence to
    // never go down from one record to the next, so every record uses the DB
    // value, as in WriteCurrentStateToManifest().
    files_edit.SetLastSequence(db.last_sequence);
    s = add_record(files_edit, cf.timestamp_size);
    if (!s.ok()) {
      return s;
    }
  }

  VersionEdit db_edit;
  db_edit.SetNextFile(db.next_file_number);
  db_edit.SetLastSequence(db.last_sequence);
  db_edit.SetPrevLogNumber(db.prev_log_number);
  db_edit.SetMaxColumnFamily(db.max_column_family);
  if (db.min_log_number_to_keep != 0) {
    db_edit.SetMinLogNumberToKeep(db.min_log_number_to_keep);
  }
  if (db.committed_options_file_number.has_value()) {
    db_edit.SetCommittedOptionsFileNumber(*db.committed_options_file_number);
  }
  return add_record(db_edit, /*ts_sz=*/0);
}

namespace {

bool SameCfRow(const ManifestCfRow& a, const ManifestCfRow& b) {
  auto fields = [](const ManifestCfRow& r) {
    return std::tie(r.name, r.comparator_name, r.timestamp_size,
                    r.persist_user_defined_timestamps, r.log_number,
                    r.full_history_ts_low, r.dropped_blob_files, r.dropped);
  };
  return fields(a) == fields(b) &&
         std::equal(a.compact_cursors.begin(), a.compact_cursors.end(),
                    b.compact_cursors.begin(), b.compact_cursors.end(),
                    // Two empty cursors are equal. Other cursors are
                    // compared by their encoding.
                    [](const InternalKey& x, const InternalKey& y) {
                      return x.size() == y.size() &&
                             (x.unset() || x.Encode() == y.Encode());
                    });
}

// Compares the persisted fields of two file rows through their encoding as a
// new file record, so that a new persisted FileMetaData field needs no change
// here. A timestamp size of 0 keeps the file boundaries as they are in memory.
bool SameFileRow(const ManifestFileRow& a, const ManifestFileRow& b) {
  VersionEdit edit_a;
  VersionEdit edit_b;
  edit_a.AddExistingFile(a.level, a.meta, /*provenance_flags_to_set=*/0);
  edit_b.AddExistingFile(b.level, b.meta, /*provenance_flags_to_set=*/0);
  std::string record_a;
  std::string record_b;
  return edit_a.EncodeTo(&record_a, /*ts_sz=*/0) &&
         edit_b.EncodeTo(&record_b, /*ts_sz=*/0) && record_a == record_b;
}

}  // namespace

bool ManifestRowsEqual(const ManifestRows& a, const ManifestRows& b,
                       std::string* diff) {
  std::string where;
  if (a.db != b.db) {
    where = "DB row";
  } else if (a.wals != b.wals) {
    where = "WAL rows";
  } else if (a.cfs.size() != b.cfs.size()) {
    where = "number of column families";
  }
  for (std::map<uint32_t, ManifestCfRows>::const_iterator it_a = a.cfs.begin(),
                                                          it_b = b.cfs.begin();
       where.empty() && it_a != a.cfs.end(); ++it_a, ++it_b) {
    const std::string cf = "column family " + std::to_string(it_a->first);
    if (it_a->first != it_b->first) {
      where = "column family ids";
    } else if (!SameCfRow(it_a->second.cf, it_b->second.cf)) {
      where = cf + " row";
    } else if (!std::equal(it_a->second.files.begin(), it_a->second.files.end(),
                           it_b->second.files.begin(), it_b->second.files.end(),
                           [](const auto& x, const auto& y) {
                             return x.first == y.first &&
                                    SameFileRow(x.second, y.second);
                           })) {
      where = cf + " file rows";
    } else if (it_a->second.blob_files != it_b->second.blob_files) {
      where = cf + " blob file rows";
    }
  }
  if (where.empty()) {
    return true;
  }
  if (diff != nullptr) {
    *diff = where;
  }
  return false;
}

}  // namespace ROCKSDB_NAMESPACE
