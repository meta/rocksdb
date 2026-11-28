//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/manifest_rows.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <initializer_list>
#include <limits>
#include <set>
#include <tuple>

#include "db/column_family.h"
#include "db/manifest_store_state.h"
#include "db/version_set.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/manifest_store.h"
#include "rocksdb/utilities/customizable_util.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"

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
// other in-memory state of the file. When `marked_files` is not null, it gives
// the files that are marked for compaction, so that
// FileMetaData::marked_for_compaction, which changes under the DB mutex, is not
// read (see VersionSet::CheckpointRowsRefs).
ManifestFileRow MakeFileRow(int level, const FileMetaData& f,
                            const UnorderedSet<uint64_t>* marked_files) {
  VersionEdit edit;
  edit.AddExistingFile(level, f, /*provenance_flags_to_set=*/0,
                       marked_files == nullptr
                           ? f.marked_for_compaction
                           : marked_files->count(f.fd.GetNumber()) > 0);
  return ManifestFileRow{level, std::move(edit.GetMutableNewFiles()[0].second)};
}

// Sets the file and blob file rows of `cf_rows` from `vstorage`.
// `marked_files` is as in MakeFileRow().
void SetManifestFileRows(const VersionStorageInfo& vstorage,
                         const UnorderedSet<uint64_t>* marked_files,
                         ManifestCfRows* cf_rows) {
  for (int level = 0; level < vstorage.num_levels(); ++level) {
    for (const FileMetaData* f : vstorage.LevelFiles(level)) {
      cf_rows->files[f->fd.GetNumber()] = MakeFileRow(level, *f, marked_files);
    }
  }
  for (const auto& meta : vstorage.GetBlobFiles()) {
    cf_rows->blob_files[meta->GetBlobFileNumber()] = MakeBlobFileRow(*meta);
  }
}

// Sets the column family row of `cfd` in `rows`, from `cfd` and `vstorage`,
// the storage of its current Version, without its file rows. The compaction
// cursors of `vstorage` change under the DB mutex.
// REQUIRES: DB mutex held.
ManifestCfRows& SetManifestCfRow(const ColumnFamilyData& cfd,
                                 const VersionStorageInfo& vstorage,
                                 ManifestRows* rows) {
  assert(rows != nullptr);
  ManifestCfRows& cf_rows = rows->cfs[cfd.GetID()];
  cf_rows = ManifestCfRows();
  cf_rows.cf = MakeManifestCfRow(cfd, vstorage, cfd.GetLogNumber(),
                                 cfd.GetFullHistoryTsLow());
  return cf_rows;
}

// Sets the column family row and the file and blob file rows of one column
// family in `rows`, from `cfd` and `vstorage`, the storage of its current
// Version.
// REQUIRES: DB mutex held.
void SetManifestCfRows(const ColumnFamilyData& cfd,
                       const VersionStorageInfo& vstorage, ManifestRows* rows) {
  SetManifestFileRows(vstorage, /*marked_files=*/nullptr,
                      &SetManifestCfRow(cfd, vstorage, rows));
}

}  // namespace

void VersionSet::SetManifestDbRow(ManifestDbRow* db_row) const {
  ManifestDbRow& db = *db_row;
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
}

void VersionSet::SetManifestDbRowAndWals(ManifestRows* rows) const {
  SetManifestDbRow(&rows->db);
  rows->wals = wals_.GetWals();
}

uint64_t VersionSet::StoreAppliedCommitSequence() const {
  return HasManifestStoreWriteState()
             ? manifest_store_state_->applied_commit_sequence()
             : 0;
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

void VersionSet::GetManifestRowsWithoutFiles(
    ManifestRows* rows, const UnorderedSet<uint32_t>* include_cf_ids,
    CheckpointRowsRefs* refs) const {
  assert(rows != nullptr);
  assert(refs != nullptr);
  *rows = ManifestRows();
  SetManifestDbRowAndWals(rows);
  for (ColumnFamilyData* cfd : *column_family_set_) {
    if (cfd->IsDropped() || (include_cf_ids != nullptr &&
                             include_cf_ids->count(cfd->GetID()) == 0)) {
      continue;
    }
    Version* current = cfd->current();
    const VersionStorageInfo& vstorage = *current->storage_info();
    SetManifestCfRow(*cfd, vstorage, rows);
    cfd->Ref();
    current->Ref();
    refs->versions.push_back(current);
  }
}

void VersionSet::AddManifestFileRows(const CheckpointRowsRefs& refs,
                                     ManifestRows* rows) {
  assert(rows != nullptr);
  for (Version* version : refs.versions) {
    SetManifestFileRows(*version->storage_info(), &refs.marked_files,
                        &rows->cfs[version->cfd()->GetID()]);
  }
}

void VersionSet::ReleaseCheckpointRowsRefs(CheckpointRowsRefs* refs) {
  for (Version* version : refs->versions) {
    ColumnFamilyData* cfd = version->cfd();
    version->Unref();
    cfd->UnrefAndTryDelete();
  }
  refs->versions.clear();
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
      changes->file_upserts[key] =
          MakeFileRow(level, meta, /*marked_files=*/nullptr);
    }
    if (e->IsColumnFamilyAdd()) {
      assert(new_cf_options != nullptr);
      // A new column family has no Version yet, so its row is built from the
      // edit and the options. It must match MakeManifestCfRow() for an empty
      // Version; DebugCheckRowsAfterInstall() checks this. The edit can carry a
      // max column family that is lower than the new id, or none; creating the
      // column family raises it to the new id, also during recovery.
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

namespace {
// Pairs each new Version of a batch with its base Version (builder_guards[i]
// holds the base Version of new_versions[i]). A column family add or drop is
// always the only edit of its batch: ProcessManifestWrites() does not group a
// writer whose first edit is IsColumnFamilyManipulation() with other writers.
// So checking the first edit is enough. For a drop, the current Version of
// `first_cfd` is added as both the base Version and the new Version. Only the
// thread that runs ProcessManifestWrites() installs Versions, so the current
// Version cannot change here.
std::vector<ManifestBatchVersion> MakeManifestBatchVersions(
    const autovector<VersionEdit*>& batch_edits,
    const autovector<Version*>& new_versions,
    const std::vector<std::unique_ptr<BaseReferencedVersionBuilder>>&
        builder_guards,
    ColumnFamilyData* first_cfd) {
  std::vector<ManifestBatchVersion> batch_versions;
  batch_versions.reserve(new_versions.size() + 1);
  for (size_t i = 0; i < new_versions.size(); ++i) {
    batch_versions.push_back({new_versions[i]->cfd(),
                              builder_guards[i]->version()->storage_info(),
                              new_versions[i]->storage_info()});
  }
  if (batch_edits.front()->IsColumnFamilyDrop()) {
    const VersionStorageInfo* current = first_cfd->current()->storage_info();
    batch_versions.push_back({first_cfd, current, current});
  }
  return batch_versions;
}
}  // namespace

Status VersionSet::BuildManifestBatchRowChanges(
    const autovector<VersionEdit*>& batch_edits,
    const autovector<Version*>& new_versions,
    const std::vector<std::unique_ptr<BaseReferencedVersionBuilder>>&
        builder_guards,
    ColumnFamilyData* first_cfd, const ColumnFamilyOptions* new_cf_options,
    const ManifestRows& old_rows, ManifestRowChanges* changes) const {
  // This reads wals_ without the DB mutex. Only this thread changes wals_,
  // when it installs a write.
  return BuildManifestRowChanges(
      batch_edits,
      MakeManifestBatchVersions(batch_edits, new_versions, builder_guards,
                                first_cfd),
      new_cf_options, old_rows, wals_, changes);
}

Status VersionSet::CommitBatchToStore(
    const autovector<VersionEdit*>& batch_edits,
    const autovector<Version*>& new_versions,
    const std::vector<std::unique_ptr<BaseReferencedVersionBuilder>>&
        builder_guards,
    ColumnFamilyData* first_cfd, const ColumnFamilyOptions* new_cf_options,
    uint64_t min_file_number_in_use, IOStatus* io_s) {
  ManifestStoreState* state = manifest_store_state_.get();
  assert(state != nullptr);
  ManifestRowChanges changes;
  state->PrepareRewrite();
  Status s = BuildManifestBatchRowChanges(
      batch_edits, new_versions, builder_guards, first_cfd, new_cf_options,
      state->batch_base_rows(), &changes);
  if (!s.ok()) {
    // A WAL edit that the WAL set rejects. Installing the batch would fail
    // for the same reason, so nothing is committed.
    return s;
  }
  state->MergeRewrite(&changes);
  *io_s = state->CommitBatch(changes, min_file_number_in_use);
  if (io_s->ok() && batch_edits.front()->IsColumnFamilyDrop()) {
    // The rows of the dropped column family stay in the store until later
    // commits delete them (ManifestStoreState::QueueDroppedCfRows()).
    const VersionStorageInfo* vstorage = first_cfd->current()->storage_info();
    std::vector<uint64_t> files;
    std::vector<uint64_t> blob_files;
    for (int level = 0; level < vstorage->num_levels(); ++level) {
      for (const FileMetaData* f : vstorage->LevelFiles(level)) {
        files.push_back(f->fd.GetNumber());
      }
    }
    for (const auto& blob_file : vstorage->GetBlobFiles()) {
      blob_files.push_back(blob_file->GetBlobFileNumber());
    }
    state->QueueDroppedCfRows(first_cfd->GetID(), files, blob_files);
  }
  return *io_s;
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
// - the MANIFEST encoding of the files that the write adds or updates
//   (`changes.file_upserts`), with a timestamp size of 0, so the key
//   boundaries keep their in-memory form, including any timestamp bytes.
//   ManifestRowsRoundTrip covers the encoding of boundaries without timestamp
//   bytes, as ManifestRowsToRecords() writes them for a column family that
//   does not persist timestamps. Both sides copy the file with MakeFileRow()
//   and compare the same MANIFEST record encoding (SameFileRow()), so a field
//   that both leave out is not found here. When someone adds a new
//   NewFileCustomTag, the switch in ManifestRowFieldOfNewFileTag()
//   (db/version_set_test.cc) does not build until they name the row field
//   that holds the tag, because the switch has no default case;
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
        expected_cf.files[number] =
            MakeFileRow(level, *f, /*marked_files=*/nullptr);
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
  const std::vector<ManifestBatchVersion> batch_versions =
      MakeManifestBatchVersions(batch_edits, new_versions, builder_guards,
                                first_cfd);
  ManifestRowChanges changes;
  // This reads wals_ without the DB mutex. Only the thread that runs
  // ProcessManifestWrites() changes wals_ (see wals_ in db/version_set.h).
  if (!BuildManifestRowChanges(batch_edits, batch_versions, new_cf_options,
                               *debug_manifest_rows_, wals_, &changes)
           .ok()) {
    // ProcessManifestWrites() rejects the same WAL edit when it installs the
    // write, so the batch installs no new Version. By then the live WAL set
    // may already hold the earlier WAL edits of the batch. So the debug rows
    // are cleared, and the next call fills them again from the live state.
    debug_manifest_rows_.reset();
    return;
  }
  // The next file number in the rows never goes down. Each batch records
  // next_file_number_ under the DB mutex (LogAndApplyHelper(),
  // LogAndApplyCFHelper(), or the new MANIFEST step of
  // ProcessManifestWrites()). next_file_number_ never goes down. Batches are
  // written one at a time.
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
  if (batch_edits.front()->IsColumnFamilyDrop()) {
    // See ManifestCfRow::dropped. The file and blob file rows of a dropped
    // column family are freed here, without the DB mutex, so that phase 2
    // erases only the small column family row.
    ManifestCfRows& dropped = debug_manifest_rows_->cfs[first_cfd->GetID()];
    dropped.files.clear();
    dropped.blob_files.clear();
  }
  TEST_SYNC_POINT_CALLBACK("VersionSet::DebugCheckRowsBeforeInstall:Done",
                           debug_manifest_rows_.get());
}

void VersionSet::DebugCheckRowsAfterInstall(
    const autovector<VersionEdit*>& batch_edits) {
  // Phase 1 resets the rows when BuildManifestRowChanges() rejects a WAL edit.
  // Installing such a write fails, so this is only a guard.
  if (debug_manifest_rows_ == nullptr) {
    return;
  }
  // Phase 2 of the debug check, see VersionSet::debug_manifest_rows_.
  // The WAL rows are compared with the live WAL set in place. A copy would
  // allocate one map entry for each tracked WAL with the DB mutex held, also
  // for a write that changes no WAL.
  if (debug_manifest_rows_->wals != wals_.GetWals()) {
    fprintf(stderr, "Structured MANIFEST row changes are wrong: WAL rows\n");
    assert(false);
  }
  // `expected` and `actual` hold the DB row and the rows of the added column
  // families. Their WAL rows stay empty.
  ManifestRows expected;
  ManifestRows actual;
  actual.db = debug_manifest_rows_->db;
  SetManifestDbRow(&expected.db);
  // Allowed differences between the rows and memory (see "In-memory
  // differences" in db/manifest_rows.h):
  // - Next file number: the row can be smaller than memory, never larger.
  // - Max column family: the row can be smaller than memory, never larger.
  //   The loop below checks that it is at least the id of each added column
  //   family.
  // - Committed OPTIONS file number: memory can have none while the row keeps
  //   one. The row's value is then accepted.
  // - Log number: the loop below checks it with bounds.
  // To accept an allowed difference, the check first asserts its bound, then
  // copies the row value into the memory side (`expected`), so the final
  // compare ignores it.
  assert(actual.db.next_file_number <= expected.db.next_file_number);
  expected.db.next_file_number = actual.db.next_file_number;
  assert(actual.db.max_column_family <= expected.db.max_column_family);
  expected.db.max_column_family = actual.db.max_column_family;
  if (!expected.db.committed_options_file_number.has_value()) {
    expected.db.committed_options_file_number =
        actual.db.committed_options_file_number;
  }
  for (const VersionEdit* e : batch_edits) {
    // The row takes the DB id from the edit. DBImpl::SetDBId() sets
    // VersionSet::db_id_ before it writes such an edit, so the two match. Some
    // tests write a DB id edit without setting VersionSet::db_id_. To support
    // them, the check takes the expected id from the edit in this write.
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
      // See ManifestCfRow::dropped. Phase 1 already cleared the file and blob
      // file rows, so this erases only the column family row.
      assert(it->second.files.empty() && it->second.blob_files.empty());
      debug_manifest_rows_->cfs.erase(it);
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
  TEST_SYNC_POINT_CALLBACK("VersionSet::DebugCheckRowsAfterInstall:Done",
                           debug_manifest_rows_.get());
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

// ======== Public rows ========
//
// The code below encodes and decodes the public rows of
// include/rocksdb/manifest_store.h: ManifestRowsToPublicRows(),
// PublicRowsToManifestRows() and their helpers, and the canonical field
// encoding (ParseManifestRowFields(), EncodeManifestRowFields()). The code
// above works only on the internal rows.

namespace {

// Reads a varint that uses the fewest bytes for its value. GetVarint32() and
// GetVarint64() also accept longer forms and ignore the bits of the last byte
// that do not fit the type, so this checks the number of bytes read and the
// last byte. A varint32 has 32 bits, so its fifth byte may use only its low 4
// bits; a varint64 has 64 bits, so its tenth byte may use only its lowest bit.
template <typename T>
bool GetShortestVarint(Slice* input, T* value) {
  constexpr ptrdiff_t kMaxLength = sizeof(T) == 4 ? 5 : 10;
  constexpr char kUnusedBits = sizeof(T) == 4 ? '\xF0' : '\xFE';
  const char* start = input->data();
  bool ok = false;
  if constexpr (sizeof(T) == 4) {
    ok = GetVarint32(input, value);
  } else {
    ok = GetVarint64(input, value);
  }
  if (!ok) {
    return false;
  }
  const ptrdiff_t length = input->data() - start;
  if (length == kMaxLength && (start[kMaxLength - 1] & kUnusedBits) != 0) {
    return false;
  }
  return length == VarintLength(*value);
}

// Reads a value size in its shortest form, then that many bytes.
bool GetShortestSlice(Slice* input, Slice* result) {
  uint32_t size = 0;
  if (!GetShortestVarint(input, &size) || input->size() < size) {
    return false;
  }
  *result = Slice(input->data(), size);
  input->remove_prefix(size);
  return true;
}

// Builds the canonical encoding of one row. Callers add the fields in
// increasing ID order and leave out the fields that hold their default.
class FieldWriter {
 public:
  template <typename Id>
  void Bytes(Id id, const Slice& value) {
    const uint32_t raw_id = lossless_cast<uint32_t>(id);
    assert(raw_id > last_id_);
    assert(value.size() <= std::numeric_limits<uint32_t>::max());
    last_id_ = raw_id;
    PutVarint32(&encoded_, raw_id);
    PutLengthPrefixedSlice(&encoded_, value);
  }

  // varint32 and varint64 give the same bytes for the same value, so this
  // writes both.
  template <typename Id>
  void Varint(Id id, uint64_t value) {
    std::string buf;
    PutVarint64(&buf, value);
    Bytes(id, buf);
  }

  // Writes `value` unless it equals `default_value`, because a row leaves out
  // the fields that hold their default.
  template <typename Id>
  void VarintUnlessDefault(Id id, uint64_t value, uint64_t default_value) {
    if (value != default_value) {
      Varint(id, value);
    }
  }

  // The same for a bytes value.
  template <typename Id>
  void BytesUnlessDefault(Id id, const Slice& value,
                          const Slice& default_value) {
    if (value != default_value) {
      Bytes(id, value);
    }
  }

  // Writes a bool or an enum value as one byte.
  template <typename Id>
  void Byte(Id id, uint8_t value) {
    const char c = lossless_cast<char>(value);
    Bytes(id, Slice(&c, 1));
  }

  std::string Release() { return std::move(encoded_); }

 private:
  std::string encoded_;
  uint32_t last_id_ = 0;
};

std::string EncodeDbRow(const ManifestDbRow& db,
                        const ManifestDbRowExtraFields& extra_fields) {
  using F = ManifestDbField;
  FieldWriter w;
  w.Varint(F::kRowFormatVersion, kManifestRowFormatVersionLatest);
  w.Byte(F::kAuthority, lossless_cast<uint8_t>(extra_fields.authority));
  w.Byte(F::kCopyMode, lossless_cast<uint8_t>(extra_fields.copy_mode));
  w.VarintUnlessDefault(F::kCopyFileNumber, extra_fields.copy_file_number, 0);
  w.BytesUnlessDefault(F::kDbId, db.db_id, "");
  w.Varint(F::kNextFileNumber, db.next_file_number);
  w.Varint(F::kInFlightFileNumber, extra_fields.in_flight_file_number);
  w.Varint(F::kLastSequence, db.last_sequence);
  w.VarintUnlessDefault(F::kPrevLogNumber, db.prev_log_number, 0);
  w.VarintUnlessDefault(F::kMinLogNumberToKeep, db.min_log_number_to_keep, 0);
  w.VarintUnlessDefault(F::kMaxColumnFamily, db.max_column_family, 0);
  if (db.committed_options_file_number.has_value()) {
    w.Varint(F::kCommittedOptionsFileNumber, *db.committed_options_file_number);
  }
  return w.Release();
}

std::string EncodeCfRow(const ManifestCfRow& cf) {
  using F = ManifestCfField;
  FieldWriter w;
  w.Bytes(F::kName, cf.name);
  w.Bytes(F::kComparatorName, cf.comparator_name);
  w.VarintUnlessDefault(F::kTimestampSize, cf.timestamp_size, 0);
  if (!cf.persist_user_defined_timestamps) {
    w.Byte(F::kPersistUserDefinedTimestamps, 0);
  }
  w.VarintUnlessDefault(F::kLogNumber, cf.log_number, 0);
  w.BytesUnlessDefault(F::kFullHistoryTsLow, cf.full_history_ts_low, "");
  std::string cursors;
  for (uint32_t level = 0; level < cf.compact_cursors.size(); ++level) {
    const InternalKey& cursor = cf.compact_cursors[level];
    if (cursor.size() != 0) {
      PutVarint32(&cursors, level);
      PutLengthPrefixedSlice(&cursors, cursor.Encode());
    }
  }
  w.BytesUnlessDefault(F::kCompactCursors, cursors, "");
  std::string dropped_blob_files;
  for (uint64_t number : cf.dropped_blob_files) {
    PutVarint64(&dropped_blob_files, number);
  }
  w.BytesUnlessDefault(F::kDroppedBlobFiles, dropped_blob_files, "");
  if (cf.dropped) {
    w.Byte(F::kDropped, 1);
  }
  return w.Release();
}

std::string EncodeFileRow(const ManifestFileRow& row) {
  using F = ManifestFileField;
  const FileMetaData& f = row.meta;
  FieldWriter w;
  w.VarintUnlessDefault(F::kLevel, lossless_cast<uint32_t>(row.level), 0);
  w.VarintUnlessDefault(F::kPathId, f.fd.GetPathId(), 0);
  w.Varint(F::kFileSize, f.fd.GetFileSize());
  w.Bytes(F::kSmallest, f.smallest.Encode());
  w.Bytes(F::kLargest, f.largest.Encode());
  w.Varint(F::kSmallestSeqno, f.fd.smallest_seqno);
  w.Varint(F::kLargestSeqno, f.fd.largest_seqno);
  if (f.marked_for_compaction) {
    w.Byte(F::kMarkedForCompaction, 1);
  }
  w.VarintUnlessDefault(F::kOldestBlobFileNumber, f.oldest_blob_file_number,
                        kInvalidBlobFileNumber);
  w.Varint(F::kOldestAncestorTime, f.oldest_ancestor_time);
  w.Varint(F::kFileCreationTime, f.file_creation_time);
  w.Varint(F::kEpochNumber, f.epoch_number);
  w.BytesUnlessDefault(F::kFileChecksum, f.file_checksum, kUnknownFileChecksum);
  w.BytesUnlessDefault(F::kFileChecksumFuncName, f.file_checksum_func_name,
                       kUnknownFileChecksumFuncName);
  if (f.temperature != Temperature::kUnknown) {
    w.Byte(F::kTemperature, lossless_cast<uint8_t>(f.temperature));
  }
  w.BytesUnlessDefault(F::kMinTimestamp, f.min_timestamp, "");
  w.BytesUnlessDefault(F::kMaxTimestamp, f.max_timestamp, "");
  if (f.unique_id != kNullUniqueId64x2) {
    UniqueId64x2 unique_id = f.unique_id;
    w.Bytes(F::kUniqueId, EncodeUniqueIdBytes(&unique_id));
  }
  w.VarintUnlessDefault(F::kCompensatedRangeDeletionSize,
                        f.compensated_range_deletion_size, 0);
  w.VarintUnlessDefault(F::kTailSize, f.tail_size, 0);
  if (!f.user_defined_timestamps_persisted) {
    w.Byte(F::kUserDefinedTimestampsPersisted, 0);
  }
  w.BytesUnlessDefault(F::kFileOpenMetadata, f.file_open_metadata, "");
  w.VarintUnlessDefault(F::kDbOwnershipStartTime, f.db_ownership_start_time,
                        kUnknownDbOwnershipStartTime);
  w.VarintUnlessDefault(F::kDataFileProvenance, f.data_file_provenance, 0);
  return w.Release();
}

std::string EncodeBlobFileRow(const ManifestBlobFileRow& row) {
  using F = ManifestBlobFileField;
  const BlobFileAddition& addition = row.addition;
  FieldWriter w;
  w.Varint(F::kTotalBlobCount, addition.GetTotalBlobCount());
  w.Varint(F::kTotalBlobBytes, addition.GetTotalBlobBytes());
  w.BytesUnlessDefault(F::kChecksumMethod, addition.GetChecksumMethod(), "");
  w.BytesUnlessDefault(F::kChecksumValue, addition.GetChecksumValue(), "");
  w.VarintUnlessDefault(F::kGarbageBlobCount, row.garbage_blob_count, 0);
  w.VarintUnlessDefault(F::kGarbageBlobBytes, row.garbage_blob_bytes, 0);
  return w.Release();
}

std::string EncodeWalRow(const WalMetadata& wal) {
  FieldWriter w;
  if (wal.HasSyncedSize()) {
    w.Varint(ManifestWalField::kSyncedSize, wal.GetSyncedSizeInBytes());
  }
  return w.Release();
}

// The reader of public rows. The functions below are in bottom-up order. The
// calls go:
//   PublicRowsToManifestRows()
//     -> DecodeDbRow() for the DB row
//     -> DecodeRow() for each other row. It reads column family and WAL rows
//        itself, and calls DecodeFileRow() and DecodeBlobFileRow().
//   Each of these calls ReadRowFields(), which calls a Read*Field() function
//   for each field. The Read*Field() functions use the Get*Value() helpers,
//   GetEnumByte(), ReadCompactCursors() and ReadDroppedBlobFiles().
//   PublicRowsToManifestRows() runs CheckTimestampKeySizes() last.

// The largest compaction cursor level that a reader accepts. A column family
// row keeps its cursors in a vector indexed by level, so a corrupt level must
// not make the reader allocate a huge vector. Real DBs have far fewer levels
// (ColumnFamilyOptions::num_levels is 7 by default); the bound is a sanity
// limit, not a RocksDB limit. At this bound the vector holds at most 65537
// InternalKey objects, almost all of them empty, about 2 MB (an estimate from a
// 32-byte InternalKey), which a reader can afford for one corrupt row.
constexpr uint32_t kMaxCompactCursorLevel = 1 << 16;

Status BadValue(uint32_t id) {
  return Status::Corruption("Bad value in manifest row field " +
                            std::to_string(id));
}

// Handles a field ID that the reader does not know.
Status UnknownField(uint32_t id) {
  if ((id & kManifestFieldMustUnderstand) != 0) {
    return Status::NotSupported(
        "Manifest row has an unknown must-understand field " +
        std::to_string(id));
  }
  return Status::OK();
}

// Each Get*Value() function returns true if `value` holds exactly one value of
// its type. GetVarintValue() reads a varint32 for a uint32_t and a varint64 for
// a uint64_t.
template <typename T>
bool GetVarintValue(Slice value, T* out) {
  return GetShortestVarint(&value, out) && value.empty();
}

bool GetBoolValue(const Slice& value, bool* out) {
  if (value.size() != 1 || lossless_cast<uint8_t>(value[0]) > 1) {
    return false;
  }
  *out = value[0] == 1;
  return true;
}

// Returns true if `key` is long enough to be an internal key and its value
// type is one that a key in RocksDB metadata can have.
bool IsInternalKey(const Slice& key) {
  return key.size() >= kNumInternalBytes &&
         IsExtendedValueType(ExtractValueType(key));
}

bool GetInternalKeyValue(const Slice& value, InternalKey* out) {
  if (!IsInternalKey(value)) {
    return false;
  }
  out->DecodeFrom(value);
  return true;
}

Status CheckValue(bool ok, uint32_t id) {
  return ok ? Status::OK() : BadValue(id);
}

// Reads a one-byte enum value whose largest known value is `max`. A larger
// value is NotSupported, because a newer release may have added it.
template <typename Enum>
Status GetEnumByte(const Slice& value, uint32_t id, Enum max, Enum* out) {
  if (value.size() != 1) {
    return BadValue(id);
  }
  if (lossless_cast<uint8_t>(value[0]) > lossless_cast<uint8_t>(max)) {
    return Status::NotSupported("Unknown value of manifest row field " +
                                std::to_string(id));
  }
  *out = lossless_cast<Enum>(value[0]);
  return Status::OK();
}

// Parses the fields of `row`, calls `read_field` for each field, then checks
// that the row has each field ID in `required`.
template <typename ReadField>
Status ReadRowFields(const ManifestRow& row,
                     std::initializer_list<uint32_t> required,
                     const ReadField& read_field) {
  std::vector<ManifestRowField> fields;
  Status s = ParseManifestRowFields(row.fields, &fields);
  for (size_t i = 0; s.ok() && i < fields.size(); ++i) {
    s = read_field(fields[i]);
  }
  if (!s.ok()) {
    return s;
  }
  for (uint32_t id : required) {
    std::vector<ManifestRowField>::const_iterator it = std::lower_bound(
        fields.begin(), fields.end(), id,
        [](const ManifestRowField& f, uint32_t v) { return f.id < v; });
    if (it == fields.end() || it->id != id) {
      return Status::Corruption("Manifest row misses field " +
                                std::to_string(id));
    }
  }
  return Status::OK();
}

// Field id casts: each Read*Field() function below switches on its field enum
// with no default case. So the build fails (-Wswitch with -Werror) when someone
// adds a field ID without a case for it. An ID that the enum does not name
// falls through to UnknownField(). The cast of such an ID is defined, because
// each field enum has a fixed underlying type. The NOLINT silences a static
// analyzer that reports it as out of range.

Status ReadDbField(const ManifestRowField& field, ManifestDbRow* db,
                   ManifestDbRowExtraFields* extra_fields) {
  using F = ManifestDbField;
  const Slice& v = field.value;
  uint64_t u64 = 0;
  uint32_t u32 = 0;
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  switch (lossless_cast<F>(field.id)) {
    case F::kRowFormatVersion:
      // ParseManifestRowFields() accepts only fields in increasing ID order,
      // and ReadRowFields() stops at the first error. So this field (ID 1) is
      // read first, and a newer version gives NotSupported before any other
      // field can give Corruption. A row whose field encoding cannot be parsed
      // is still Corruption.
      if (!GetVarintValue(v, &u32) || u32 == 0) {
        return BadValue(field.id);
      }
      if (u32 > kManifestRowFormatVersionLatest) {
        return Status::NotSupported("Manifest row format version " +
                                    std::to_string(u32) +
                                    " is newer than this release");
      }
      return Status::OK();
    case F::kAuthority:
      return GetEnumByte(v, field.id, ManifestAuthority::kStore,
                         &extra_fields->authority);
    case F::kCopyMode:
      return GetEnumByte(v, field.id, ManifestCopyMode::kSync,
                         &extra_fields->copy_mode);
    case F::kCopyFileNumber:
      return CheckValue(GetVarintValue(v, &extra_fields->copy_file_number),
                        field.id);
    case F::kDbId:
      db->db_id = v.ToString();
      return Status::OK();
    case F::kNextFileNumber:
      return CheckValue(GetVarintValue(v, &db->next_file_number), field.id);
    case F::kInFlightFileNumber:
      return CheckValue(GetVarintValue(v, &extra_fields->in_flight_file_number),
                        field.id);
    case F::kLastSequence:
      return CheckValue(GetVarintValue(v, &db->last_sequence), field.id);
    case F::kPrevLogNumber:
      return CheckValue(GetVarintValue(v, &db->prev_log_number), field.id);
    case F::kMinLogNumberToKeep:
      return CheckValue(GetVarintValue(v, &db->min_log_number_to_keep),
                        field.id);
    case F::kMaxColumnFamily:
      return CheckValue(GetVarintValue(v, &db->max_column_family), field.id);
    case F::kCommittedOptionsFileNumber:
      if (!GetVarintValue(v, &u64)) {
        return BadValue(field.id);
      }
      db->committed_options_file_number = u64;
      return Status::OK();
  }
  return UnknownField(field.id);
}

Status ReadCompactCursors(Slice v, uint32_t id,
                          std::vector<InternalKey>* cursors) {
  while (!v.empty()) {
    uint32_t level = 0;
    Slice key;
    if (!GetShortestVarint(&v, &level) || !GetShortestSlice(&v, &key) ||
        level > kMaxCompactCursorLevel || level < cursors->size() ||
        !IsInternalKey(key)) {
      return BadValue(id);
    }
    cursors->resize(level + 1);
    cursors->back().DecodeFrom(key);
  }
  return Status::OK();
}

Status ReadDroppedBlobFiles(Slice v, uint32_t id,
                            std::vector<uint64_t>* numbers) {
  while (!v.empty()) {
    uint64_t number = 0;
    if (!GetShortestVarint(&v, &number) ||
        (!numbers->empty() && number <= numbers->back())) {
      return BadValue(id);
    }
    numbers->push_back(number);
  }
  return Status::OK();
}

Status ReadCfField(const ManifestRowField& field, ManifestCfRow* cf) {
  using F = ManifestCfField;
  const Slice& v = field.value;
  uint32_t u32 = 0;
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  switch (lossless_cast<F>(field.id)) {
    case F::kName:
      cf->name = v.ToString();
      return Status::OK();
    case F::kComparatorName:
      cf->comparator_name = v.ToString();
      return Status::OK();
    case F::kTimestampSize:
      if (!GetVarintValue(v, &u32)) {
        return BadValue(field.id);
      }
      cf->timestamp_size = u32;
      return Status::OK();
    case F::kPersistUserDefinedTimestamps:
      return CheckValue(GetBoolValue(v, &cf->persist_user_defined_timestamps),
                        field.id);
    case F::kLogNumber:
      return CheckValue(GetVarintValue(v, &cf->log_number), field.id);
    case F::kFullHistoryTsLow:
      cf->full_history_ts_low = v.ToString();
      return Status::OK();
    case F::kCompactCursors:
      return ReadCompactCursors(v, field.id, &cf->compact_cursors);
    case F::kDroppedBlobFiles:
      return ReadDroppedBlobFiles(v, field.id, &cf->dropped_blob_files);
    case F::kDropped:
      return CheckValue(GetBoolValue(v, &cf->dropped), field.id);
  }
  return UnknownField(field.id);
}

// FileDescriptor keeps the path id in the 2 high bits of the packed file
// number, so a path id is at most 3, as in a MANIFEST file.
constexpr uint32_t kMaxPathId = 3;

// Reads one field of a file row. FileDescriptor packs the file number and the
// path id, so the path id goes to `path_id`, and DecodeFileRow() rebuilds the
// FileDescriptor after all fields are read.
Status ReadFileField(const ManifestRowField& field, ManifestFileRow* row,
                     uint32_t* path_id) {
  using F = ManifestFileField;
  const Slice& v = field.value;
  FileMetaData& f = row->meta;
  FileDescriptor& fd = f.fd;
  uint32_t u32 = 0;
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  switch (lossless_cast<F>(field.id)) {
    case F::kLevel:
      if (!GetVarintValue(v, &u32) ||
          u32 > lossless_cast<uint32_t>(std::numeric_limits<int>::max())) {
        return BadValue(field.id);
      }
      row->level = lossless_cast<int>(u32);
      return Status::OK();
    case F::kPathId:
      return CheckValue(GetVarintValue(v, path_id) && *path_id <= kMaxPathId,
                        field.id);
    case F::kFileSize:
      return CheckValue(GetVarintValue(v, &fd.file_size), field.id);
    case F::kSmallest:
      return CheckValue(GetInternalKeyValue(v, &f.smallest), field.id);
    case F::kLargest:
      return CheckValue(GetInternalKeyValue(v, &f.largest), field.id);
    case F::kSmallestSeqno:
      return CheckValue(GetVarintValue(v, &fd.smallest_seqno), field.id);
    case F::kLargestSeqno:
      return CheckValue(GetVarintValue(v, &fd.largest_seqno), field.id);
    case F::kMarkedForCompaction:
      return CheckValue(GetBoolValue(v, &f.marked_for_compaction), field.id);
    case F::kOldestBlobFileNumber:
      return CheckValue(GetVarintValue(v, &f.oldest_blob_file_number),
                        field.id);
    case F::kOldestAncestorTime:
      return CheckValue(GetVarintValue(v, &f.oldest_ancestor_time), field.id);
    case F::kFileCreationTime:
      return CheckValue(GetVarintValue(v, &f.file_creation_time), field.id);
    case F::kEpochNumber:
      return CheckValue(GetVarintValue(v, &f.epoch_number), field.id);
    case F::kFileChecksum:
      f.file_checksum = v.ToString();
      return Status::OK();
    case F::kFileChecksumFuncName:
      f.file_checksum_func_name = v.ToString();
      return Status::OK();
    case F::kTemperature: {
      if (v.size() != 1) {
        return BadValue(field.id);
      }
      // Like a MANIFEST file reader, keep kUnknown for a temperature value
      // that is not below Temperature::kLastTemperature. The raw byte is
      // compared before the cast, so that no out of range enum value is made.
      const uint8_t raw = lossless_cast<uint8_t>(v[0]);
      if (raw < lossless_cast<uint8_t>(Temperature::kLastTemperature)) {
        f.temperature = lossless_cast<Temperature>(raw);
      }
      return Status::OK();
    }
    case F::kMinTimestamp:
      f.min_timestamp = v.ToString();
      return Status::OK();
    case F::kMaxTimestamp:
      f.max_timestamp = v.ToString();
      return Status::OK();
    case F::kUniqueId:
      return CheckValue(DecodeUniqueIdBytes(v.ToString(), &f.unique_id).ok(),
                        field.id);
    case F::kCompensatedRangeDeletionSize:
      return CheckValue(GetVarintValue(v, &f.compensated_range_deletion_size),
                        field.id);
    case F::kTailSize:
      return CheckValue(GetVarintValue(v, &f.tail_size), field.id);
    case F::kUserDefinedTimestampsPersisted:
      return CheckValue(GetBoolValue(v, &f.user_defined_timestamps_persisted),
                        field.id);
    case F::kFileOpenMetadata:
      f.file_open_metadata = v.ToString();
      return Status::OK();
    case F::kDbOwnershipStartTime:
      return CheckValue(GetVarintValue(v, &f.db_ownership_start_time),
                        field.id);
    case F::kDataFileProvenance:
      return CheckValue(GetVarintValue(v, &f.data_file_provenance), field.id);
  }
  return UnknownField(field.id);
}

struct BlobFileFields {
  uint64_t total_blob_count = 0;
  uint64_t total_blob_bytes = 0;
  std::string checksum_method;
  std::string checksum_value;
};

Status ReadBlobFileField(const ManifestRowField& field, BlobFileFields* blob,
                         ManifestBlobFileRow* row) {
  using F = ManifestBlobFileField;
  const Slice& v = field.value;
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  switch (lossless_cast<F>(field.id)) {
    case F::kTotalBlobCount:
      return CheckValue(GetVarintValue(v, &blob->total_blob_count), field.id);
    case F::kTotalBlobBytes:
      return CheckValue(GetVarintValue(v, &blob->total_blob_bytes), field.id);
    case F::kChecksumMethod:
      blob->checksum_method = v.ToString();
      return Status::OK();
    case F::kChecksumValue:
      blob->checksum_value = v.ToString();
      return Status::OK();
    case F::kGarbageBlobCount:
      return CheckValue(GetVarintValue(v, &row->garbage_blob_count), field.id);
    case F::kGarbageBlobBytes:
      return CheckValue(GetVarintValue(v, &row->garbage_blob_bytes), field.id);
  }
  return UnknownField(field.id);
}

Status ReadWalField(const ManifestRowField& field, WalMetadata* wal) {
  uint64_t size = 0;
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  switch (lossless_cast<ManifestWalField>(field.id)) {
    case ManifestWalField::kSyncedSize:
      if (!GetVarintValue(field.value, &size)) {
        return BadValue(field.id);
      }
      wal->SetSyncedSizeInBytes(size);
      // The largest 64-bit value means an unknown size, which a writer
      // leaves out instead.
      return CheckValue(wal->HasSyncedSize(), field.id);
  }
  return UnknownField(field.id);
}

template <typename Field>
uint32_t Id(Field f) {
  return lossless_cast<uint32_t>(f);
}

Status DecodeDbRow(const ManifestRow& row, ManifestDbRow* db,
                   ManifestDbRowExtraFields* extra_fields) {
  const ManifestRowKey& key = row.key;
  if (key.type != ManifestRowType::kDb || key.cf_id != 0 || key.number != 0) {
    return Status::Corruption("Bad manifest DB row key");
  }
  using F = ManifestDbField;
  return ReadRowFields(row,
                       {Id(F::kRowFormatVersion), Id(F::kAuthority),
                        Id(F::kCopyMode), Id(F::kNextFileNumber),
                        Id(F::kInFlightFileNumber), Id(F::kLastSequence)},
                       [&](const ManifestRowField& field) {
                         return ReadDbField(field, db, extra_fields);
                       });
}

Status DecodeFileRow(const ManifestRow& row, ManifestFileRow* out) {
  if (row.key.number > kFileNumberMask) {
    return Status::Corruption("Manifest file row number is too large");
  }
  using F = ManifestFileField;
  uint32_t path_id = 0;
  Status s = ReadRowFields(
      row,
      {Id(F::kFileSize), Id(F::kSmallest), Id(F::kLargest),
       Id(F::kSmallestSeqno), Id(F::kLargestSeqno), Id(F::kOldestAncestorTime),
       Id(F::kFileCreationTime), Id(F::kEpochNumber)},
      [&](const ManifestRowField& field) {
        return ReadFileField(field, out, &path_id);
      });
  if (s.ok()) {
    const FileDescriptor& fd = out->meta.fd;
    out->meta.fd = FileDescriptor(row.key.number, path_id, fd.file_size,
                                  fd.smallest_seqno, fd.largest_seqno);
  }
  return s;
}

Status DecodeBlobFileRow(const ManifestRow& row, ManifestBlobFileRow* out) {
  using F = ManifestBlobFileField;
  BlobFileFields blob;
  Status s =
      ReadRowFields(row, {Id(F::kTotalBlobCount), Id(F::kTotalBlobBytes)},
                    [&](const ManifestRowField& field) {
                      return ReadBlobFileField(field, &blob, out);
                    });
  if (s.ok() && blob.checksum_method.empty() != blob.checksum_value.empty()) {
    s = Status::Corruption(
        "Manifest blob file row has only one of checksum method and value");
  }
  if (s.ok()) {
    out->addition = BlobFileAddition(
        row.key.number, blob.total_blob_count, blob.total_blob_bytes,
        std::move(blob.checksum_method), std::move(blob.checksum_value));
  }
  return s;
}

// Checks that each key of a column family that uses user-defined timestamps
// is long enough to hold a timestamp. The check runs after all rows are
// decoded, because the column family row may come after its file rows.
Status CheckTimestampKeySizes(const ManifestCfRows& cf_rows) {
  const size_t min_size = kNumInternalBytes + cf_rows.cf.timestamp_size;
  for (const auto& [number, file] : cf_rows.files) {
    if (file.meta.smallest.size() < min_size ||
        file.meta.largest.size() < min_size) {
      return Status::Corruption(
          "Manifest file row key is shorter than the timestamp size");
    }
  }
  for (const InternalKey& cursor : cf_rows.cf.compact_cursors) {
    if (cursor.size() != 0 && cursor.size() < min_size) {
      return Status::Corruption(
          "Manifest compaction cursor is shorter than the timestamp size");
    }
  }
  return Status::OK();
}

Status RepeatedKey() { return Status::Corruption("Repeated manifest row key"); }

// Decodes one row other than the DB row into `out`. `cfs_with_row` collects
// the ids of the column families whose row it decoded.
Status DecodeRow(const ManifestRow& row, ManifestRows* out,
                 std::set<uint32_t>* cfs_with_row) {
  const ManifestRowKey& key = row.key;
  switch (key.type) {
    case ManifestRowType::kDb:
      return Status::Corruption("More than one manifest DB row");
    case ManifestRowType::kColumnFamily: {
      if (key.number != 0) {
        return Status::Corruption("Bad manifest column family row key");
      }
      if (!cfs_with_row->insert(key.cf_id).second) {
        return RepeatedKey();
      }
      ManifestCfRow* cf = &out->cfs[key.cf_id].cf;
      using F = ManifestCfField;
      return ReadRowFields(row, {Id(F::kName), Id(F::kComparatorName)},
                           [&](const ManifestRowField& field) {
                             return ReadCfField(field, cf);
                           });
    }
    case ManifestRowType::kFile: {
      std::pair<std::map<uint64_t, ManifestFileRow>::iterator, bool> inserted =
          out->cfs[key.cf_id].files.try_emplace(key.number);
      if (!inserted.second) {
        return RepeatedKey();
      }
      return DecodeFileRow(row, &inserted.first->second);
    }
    case ManifestRowType::kBlobFile: {
      std::pair<std::map<uint64_t, ManifestBlobFileRow>::iterator, bool>
          inserted = out->cfs[key.cf_id].blob_files.try_emplace(key.number);
      if (!inserted.second) {
        return RepeatedKey();
      }
      return DecodeBlobFileRow(row, &inserted.first->second);
    }
    case ManifestRowType::kWal: {
      if (key.cf_id != 0) {
        return Status::Corruption("Bad manifest WAL row key");
      }
      std::pair<std::map<WalNumber, WalMetadata>::iterator, bool> inserted =
          out->wals.try_emplace(key.number);
      if (!inserted.second) {
        return RepeatedKey();
      }
      return ReadRowFields(row, {}, [&](const ManifestRowField& field) {
        return ReadWalField(field, &inserted.first->second);
      });
    }
  }
  return Status::Corruption("Unknown manifest row type");
}

}  // namespace

Status ManifestRowsToPublicRows(const ManifestRows& internal_rows,
                                const ManifestDbRowExtraFields& extra_fields,
                                uint32_t feature_version,
                                std::vector<ManifestRow>* public_rows) {
  if (feature_version < 1 ||
      feature_version > kManifestRowFeatureVersionLatest) {
    return Status::InvalidArgument("Bad manifest row feature version " +
                                   std::to_string(feature_version));
  }
  public_rows->clear();
  public_rows->push_back(
      ManifestRow{ManifestRowKey{ManifestRowType::kDb, 0, 0},
                  EncodeDbRow(internal_rows.db, extra_fields)});
  for (const auto& [cf_id, cf_rows] : internal_rows.cfs) {
    public_rows->push_back(
        ManifestRow{ManifestRowKey{ManifestRowType::kColumnFamily, cf_id, 0},
                    EncodeCfRow(cf_rows.cf)});
    for (const auto& [number, file] : cf_rows.files) {
      public_rows->push_back(
          ManifestRow{ManifestRowKey{ManifestRowType::kFile, cf_id, number},
                      EncodeFileRow(file)});
    }
    for (const auto& [number, blob_file] : cf_rows.blob_files) {
      public_rows->push_back(
          ManifestRow{ManifestRowKey{ManifestRowType::kBlobFile, cf_id, number},
                      EncodeBlobFileRow(blob_file)});
    }
  }
  for (const auto& [number, wal] : internal_rows.wals) {
    public_rows->push_back(ManifestRow{
        ManifestRowKey{ManifestRowType::kWal, 0, number}, EncodeWalRow(wal)});
  }
  return Status::OK();
}

Status ManifestRowChangesToCommit(const ManifestRowChanges& changes,
                                  const ManifestDbRowExtraFields& extra_fields,
                                  uint32_t feature_version,
                                  uint64_t expected_commit_sequence,
                                  std::string commit_id, ManifestCommit* out) {
  if (feature_version < 1 ||
      feature_version > kManifestRowFeatureVersionLatest) {
    return Status::InvalidArgument("Bad manifest row feature version " +
                                   std::to_string(feature_version));
  }
  *out = ManifestCommit();
  out->expected_commit_sequence = expected_commit_sequence;
  out->commit_id = std::move(commit_id);
  out->db_row = ManifestRow{ManifestRowKey{ManifestRowType::kDb, 0, 0},
                            EncodeDbRow(changes.db, extra_fields)};
  for (const auto& [cf_id, cf] : changes.cf_upserts) {
    out->upserts.push_back(
        ManifestRow{ManifestRowKey{ManifestRowType::kColumnFamily, cf_id, 0},
                    EncodeCfRow(cf)});
  }
  for (const auto& [key, file] : changes.file_upserts) {
    out->upserts.push_back(ManifestRow{
        ManifestRowKey{ManifestRowType::kFile, key.first, key.second},
        EncodeFileRow(file)});
  }
  for (const auto& [key, blob_file] : changes.blob_file_upserts) {
    out->upserts.push_back(ManifestRow{
        ManifestRowKey{ManifestRowType::kBlobFile, key.first, key.second},
        EncodeBlobFileRow(blob_file)});
  }
  for (const auto& [number, wal] : changes.wal_upserts) {
    out->upserts.push_back(ManifestRow{
        ManifestRowKey{ManifestRowType::kWal, 0, number}, EncodeWalRow(wal)});
  }
  for (const auto& key : changes.file_deletes) {
    out->deletes.push_back(
        ManifestRowKey{ManifestRowType::kFile, key.first, key.second});
  }
  for (const auto& key : changes.blob_file_deletes) {
    out->deletes.push_back(
        ManifestRowKey{ManifestRowType::kBlobFile, key.first, key.second});
  }
  for (WalNumber number : changes.wal_deletes) {
    out->deletes.push_back(ManifestRowKey{ManifestRowType::kWal, 0, number});
  }
  return Status::OK();
}

Status PublicRowsToManifestRows(
    const ManifestRow& public_db_row,
    const std::vector<ManifestRow>& other_public_rows,
    ManifestRows* internal_rows, ManifestDbRowExtraFields* extra_fields) {
  *internal_rows = ManifestRows();
  *extra_fields = ManifestDbRowExtraFields();
  Status s = DecodeDbRow(public_db_row, &internal_rows->db, extra_fields);
  std::set<uint32_t> cfs_with_row;
  for (size_t i = 0; s.ok() && i < other_public_rows.size(); ++i) {
    s = DecodeRow(other_public_rows[i], internal_rows, &cfs_with_row);
  }
  // A file or blob file row creates the entry of its column family in `cfs`
  // (DecodeRow()). So `cfs` has an entry without a column family row exactly
  // when `cfs_with_row` is smaller.
  if (s.ok() && cfs_with_row.size() != internal_rows->cfs.size()) {
    s = Status::Corruption(
        "Manifest file or blob file row without a column family row");
  }
  for (std::map<uint32_t, ManifestCfRows>::const_iterator it =
           internal_rows->cfs.begin();
       s.ok() && it != internal_rows->cfs.end(); ++it) {
    s = CheckTimestampKeySizes(it->second);
  }
  return s;
}

Status ParseManifestRowFields(const Slice& encoded,
                              std::vector<ManifestRowField>* fields) {
  fields->clear();
  Slice input = encoded;
  while (!input.empty()) {
    ManifestRowField field;
    if (!GetShortestVarint(&input, &field.id) ||
        !GetShortestSlice(&input, &field.value)) {
      return Status::Corruption("Bad manifest row field encoding");
    }
    if (field.id == 0 || (!fields->empty() && field.id <= fields->back().id)) {
      return Status::Corruption("Bad manifest row field ID order");
    }
    fields->push_back(field);
  }
  return Status::OK();
}

std::string EncodeManifestRowFields(
    const std::vector<ManifestRowField>& fields) {
  FieldWriter w;
  for (const ManifestRowField& field : fields) {
    w.Bytes(field.id, field.value);
  }
  return w.Release();
}

// ManifestStore has no .cc file of its own; its only out-of-line function lives
// here, next to the public row code.
Status ManifestStore::CreateFromString(const ConfigOptions& config_options,
                                       const std::string& value,
                                       std::shared_ptr<ManifestStore>* result) {
  return LoadSharedObject<ManifestStore>(config_options, value, result);
}

}  // namespace ROCKSDB_NAMESPACE
