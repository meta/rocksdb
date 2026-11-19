//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/manifest_rows.h"

#include <algorithm>
#include <tuple>

#include "db/column_family.h"
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Sets the column family row and the file and blob file rows of one column
// family in `rows`, from `cfd` and `vstorage`, the storage of its current
// Version.
// REQUIRES: DB mutex held.
void SetManifestCfRows(const ColumnFamilyData& cfd,
                       const VersionStorageInfo& vstorage, ManifestRows* rows) {
  assert(rows != nullptr);
  ManifestCfRows& cf_rows = rows->cfs[cfd.GetID()];
  cf_rows = ManifestCfRows();

  ManifestCfRow& cf = cf_rows.cf;
  cf.name = cfd.GetName();
  cf.comparator_name = cfd.user_comparator()->Name();
  cf.timestamp_size = cfd.user_comparator()->timestamp_size();
  cf.persist_user_defined_timestamps =
      cfd.ioptions().persist_user_defined_timestamps;
  cf.log_number = cfd.GetLogNumber();
  cf.full_history_ts_low = cfd.GetFullHistoryTsLow();
  cf.compact_cursors = vstorage.GetCompactCursors();
  cf.dropped_blob_files = vstorage.GetDroppedBlobFiles();
  cf.dropped = cfd.IsDropped();

  // AddExistingFile() copies only the fields that a MANIFEST stores, so the
  // rows do not keep the table readers or other in-memory state of the files.
  VersionEdit files_edit;
  for (int level = 0; level < vstorage.num_levels(); ++level) {
    for (const FileMetaData* f : vstorage.LevelFiles(level)) {
      files_edit.AddExistingFile(level, *f, /*provenance_flags_to_set=*/0);
    }
  }
  for (auto& [level, meta] : files_edit.GetMutableNewFiles()) {
    const uint64_t file_number = meta.fd.GetNumber();
    cf_rows.files[file_number] = ManifestFileRow{level, std::move(meta)};
  }

  for (const auto& meta : vstorage.GetBlobFiles()) {
    ManifestBlobFileRow& row = cf_rows.blob_files[meta->GetBlobFileNumber()];
    row.addition =
        BlobFileAddition(meta->GetBlobFileNumber(), meta->GetTotalBlobCount(),
                         meta->GetTotalBlobBytes(), meta->GetChecksumMethod(),
                         meta->GetChecksumValue());
    row.garbage_blob_count = meta->GetGarbageBlobCount();
    row.garbage_blob_bytes = meta->GetGarbageBlobBytes();
  }
}

}  // namespace

void VersionSet::GetManifestRows(ManifestRows* rows) const {
  assert(rows != nullptr);
  *rows = ManifestRows();

  ManifestDbRow& db = rows->db;
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

  for (ColumnFamilyData* cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    SetManifestCfRows(*cfd, *cfd->current()->storage_info(), rows);
  }

  rows->wals = wals_.GetWals();
}

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
