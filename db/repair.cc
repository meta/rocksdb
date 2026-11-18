//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Repairer does best effort recovery to recover as much data as possible after
// a disaster without compromising consistency. It does not guarantee bringing
// the database to a time consistent state.
//
// Repair process is broken into 4 phases:
// (a) Find files
// (b) Convert logs to tables
// (c) Extract metadata
// (d) Write Descriptor
//
// (a) Find files
//
// The repairer goes through all the files in the directory, and classifies them
// based on their file name. Any file that cannot be identified by name will be
// ignored.
//
// (b) Convert logs to table
//
// Legacy repair (the DB has no persisted blob indirection evidence) replays
// every log file and skips parts whose checksum does not match. Feature repair
// (the DB has persisted blob indirection evidence) converts the WAL files in
// order, one whole WAL at a time, and also skips damaged records. After each
// WAL, it writes the next WAL number to the repair catalog. This number is the
// certified floor: every WAL below it is converted. If a WAL has a record that
// needs transaction recovery (for example prepare/commit markers), that WAL and
// all later WALs are not converted. With two_write_queues or unordered_write,
// no WAL is converted. These WALs stay for the normal open to replay.
//
// (c) Extract metadata
//
// We scan every table to compute
// (1) smallest/largest for the table
// (2) largest sequence number in the table
// (3) oldest blob file referred to by the table (if applicable)
//
// If we are unable to scan the file, then we ignore the table.
//
// (d) Write Descriptor
//
// We generate descriptor contents:
//  - legacy repair sets log number to zero. Feature repair sets each CF's log
//    number to the larger of two floors. The certified floor is described in
//    (b). The original floor is the CF's log number in the original MANIFEST.
//    Repair uses the original floor only when it finds every SST that the
//    original MANIFEST lists for the CF; otherwise the original floor is 0. The
//    certified floor stops the open from replaying a WAL that repair already
//    converted. The original floor stops the open from replaying WAL records
//    that the original SSTs already hold. See "WAL coverage" above
//    PrepareOriginalReplayCoverage() for the catalog records.
//  - next-file-number is set to 1 + largest file number we found
//  - legacy repair sets last-sequence-number to the largest sequence found
//    across all tables (see 2c). Feature repair uses the larger of that and
//    certified_max_sequence_ (see "WAL coverage" above
//    PrepareOriginalReplayCoverage()).
//  - compaction pointers are cleared
//  - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
//   (d) We can provide options for time consistent recovery and unsafe recovery
//       (ignore checksum failure when applicable)
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.
//
// Feature repair
//
// Background: a DB that uses blob indirection keeps routes and relocation
// files in its MANIFEST. When the MANIFEST is lost or damaged, the phases
// above would lose that state, and the indirect references in the SSTs could
// not find their values. So when the DB has persisted blob indirection
// evidence (see IsBlobIndirectionManifestEvidence() and the other evidence
// rules in file/filename.h), RepairDB runs feature repair instead. Feature
// repair is a crash-safe transaction: it never publishes an incomplete image.
//
// Terms:
// - Staged MANIFEST: the new MANIFEST while repair writes it
//   (REPAIR-<number>-<session>.dbtmp). It gets its MANIFEST name just before
//   CURRENT changes.
// - Image: one MANIFEST and the files it names. The original image is the one
//   that CURRENT names when repair starts. The new image is the MANIFEST that
//   repair writes. The image that CURRENT names decides which files belong to
//   the DB.
// - Repair catalog: the file REPAIR-IN-PROGRESS. It lists the original files,
//   the outputs that repair may create, and the new MANIFEST, so that a crash
//   or a retry can finish or roll back the attempt. The record format, the
//   reader and the resolver are in file/filename.h (RepairCatalogRecordType).
// - Source: one reference group key (encoding class, generation number,
//   origin), as in db/blob/blob_reference_metadata.h. The encoding class is
//   the kind of reference (BlobReferenceEncodingClass): kDirect for a legacy
//   reference, kIndirectUnflattened or kIndirectFlattened.
// - Lost file: a physical file that a loss record (kBlobPhysicalFileLoss,
//   "Accepted loss" in db/blob/blob_relocation_manifest.h) marks lost. Its
//   values read as deleted. See "Lost files" below.
// - Reference run: the blob references of one (SST, source), written to a
//   scratch file (see RepairReferenceRun). It is not the metadata run of
//   blob_reference_metadata.h.
// - Repair group: all references of one (column family, generation number).
//   A legacy reference cannot be routed, so it is grouped by its blob file,
//   which repair must keep. It is not the reference group of
//   blob_reference_metadata.h.
// - Visit: one merge of all reference runs of a repair group, in key order
//   (origin, original offset).
// - Donor: a normal blob file that repair only copies value bytes from into
//   a rebuilt relocation file (NormalBlobValidationMode::kPayloadDonor).
// - Feature CF: a column family that has blob indirection state: the original
//   MANIFEST gives it the format fence or relocation files, or one of its
//   SSTs has exact metadata, a coarse frontier (the oldest indirect-origin file
//   number a coarse SST may reference) or a classification with frontier 0 (see
//   feature_format_column_families_). The new MANIFEST gives it the format
//   fence. RepairBlobState() rebuilds the blob state only of feature CFs.
//
// Steps (Repairer::Run()):
// 1. If a catalog exists, ResolveRepairCatalog() finishes the cleanup of a
//    committed attempt (then Run() stops) or rolls back an uncommitted one.
// 2. FindFiles() and FindFeatureColumnFamilyBlobFiles() find the files.
//    DetectBlobIndirection() chooses feature repair. Without evidence,
//    RunLegacyRepair() runs phases (b) to (d) above on the files that step 2
//    found. Run() calls FindFeatureColumnFamilyBlobFiles() three times: first
//    as a probe for detection, then before the catalog starts (these files
//    become input records), and after the SST scan of step 5 for the column
//    families that the scan found (it adds input records).
// 3. ReadOriginalManifest() replays the original MANIFEST. It keeps the loss
//    records, the routes that point to a lost relocation file, the dropped-file
//    records, and the per-CF log floors and SST lists (see
//    RepairOriginalManifestHandler). Repair rebuilds all other routes in
//    step 7.
// 4. StartRepairCatalog() writes the catalog: a header and one input record
//    per original file. Then repair registers the staged MANIFEST
//    REPAIR-<number>-<session>.dbtmp and starts it with
//    VersionSet::RecoverForRepair().
// 5. ExtractMetaData() scans each SST once. It writes the blob references of
//    the SST as reference runs.
// 6. PrepareOriginalReplayCoverage() proves the log floors of the original
//    MANIFEST and writes policy records. ConvertLogFilesToTables() converts
//    the WALs as (b) says and writes coverage records (see "WAL coverage"
//    above PrepareOriginalReplayCoverage()).
// 7. RepairBlobState() rebuilds the routes and relocation files (see the
//    comment there).
// 8. AddTables() writes one edit per CF to the staged MANIFEST: the SSTs, the
//    log number and the last sequence, and for a feature CF the format fence
//    and the blob files, relocation files, loss records, dropped-file records
//    and routes that step 7 chose. PublishManifest() renames it to its MANIFEST
//    name, writes the candidate record, syncs the catalog and writes CURRENT.
//    Then ResolveRepairCatalog() cleans up.
//
// Lost files: the original MANIFEST can mark a relocation file lost (a loss
// record). Repair keeps that loss:
// 1. ReadOriginalManifest() (step 3) keeps the lost files and the routes whose
//    target is a lost relocation file.
// 2. RepairBlobState() step 1 does not group a reference that reads from a
//    lost file. It copies the file record, the loss record and the route into
//    the new MANIFEST. A lost file that no kept SST uses is dropped.
// 3. RepairBlobState() step 4 never picks a lost file as a live generation
//    number or as the target of a new route (usable()). Only a route copied
//    from the original MANIFEST can point to a lost file. So a restored copy of
//    the file on disk is not used.
// 4. If a CF has a lost file, repair cannot prove the CF's original floor, and
//    WAL files exist, repair returns Corruption (see "WAL coverage" above
//    PrepareOriginalReplayCoverage()).
//
// Dropped blob files: an open with best_efforts_blob_db_repair records a
// missing normal or origin blob file as dropped (kDroppedBlobFile), and reads
// treat its values as deleted. In a column family with the format fence,
// repair keeps that record:
// 1. ReadOriginalManifest() (step 3) keeps the dropped-file records per CF.
// 2. The record wins over any copy of the file on disk, for example a restored
//    copy, or a copy that a crash left before the purge. Its values are
//    already accepted as lost, and compactions may have turned some of them
//    into deletions. So DiscoverBlobFiles() skips such a copy, and
//    RepairBlobState() records the file as dropped again.
// 3. RepairBlobState() step 3 records the blob file of a legacy group as
//    dropped again when a dropped-file record names it. It also records a
//    legacy blob file that is not on disk and has no record as dropped, the
//    same record that an open with best_efforts_blob_db_repair writes. The
//    reason: a feature CF rejects skip_missing_blob_files, so without the
//    record a later compaction of such a value fails with "garbage for an
//    unknown blob file" and stops writes. So in a feature CF a read of such a
//    value returns NotFound, where base RepairDB keeps the reference and the
//    read fails with Corruption.
// 4. Step 4 records an unflattened generation number as dropped again when a
//    dropped-file record names it.
// 5. Any other missing value makes repair fail with Corruption. This includes
//    a missing origin without a dropped-file record. When the MANIFEST is
//    lost, there are no dropped-file records, so such a missing origin always
//    makes repair fail.
// A column family without the format fence keeps no dropped-file record in a
// feature repair, and it keeps base behavior for a missing blob file, as in
// base RepairDB.

#include <cinttypes>
#include <functional>
#include <map>
#include <queue>
#include <set>
#include <tuple>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_sequential_reader.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_file.h"
#include "db/blob/blob_relocation_manifest.h"
#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_edit_handler.h"
#include "db/wide/wide_column_serialization.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/adaptive/adaptive_table_factory.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/embedded_blob_sst.h"
#include "table/meta_blocks.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/defer.h"
#include "util/hash_containers.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class RepairLossLogReporter : public log::Reader::Reporter {
 public:
  explicit RepairLossLogReporter(Status* status) : status_(status) {}

  void Corruption(size_t, const Status& status, uint64_t) override {
    if (status_->ok()) {
      *status_ = status;
    }
  }

 private:
  Status* const status_;
};

// A relocation file addition seen in a MANIFEST and the column family whose
// edit added it.
struct RepairRelocationFile {
  uint32_t column_family_id;
  BlobRelocationFileAddition addition;
};

// The identity of an SST the original MANIFEST lists, enough to check that
// repair found the same file.
struct RepairOriginalTable {
  uint64_t file_size = 0;
  UniqueId64x2 unique_id = kNullUniqueId64x2;
};

// Replays the original MANIFEST (the one CURRENT names) for RepairDB without
// opening any SST. It keeps:
// - the accepted losses, with their relocation files and routes. A restored
// copy of a lost file must not bring its keys back.
// - per CF: the name, the log floor and the SST list. RepairDB uses the log
//   floors and SST lists, with the last sequence, to prove which WAL records
//   the MANIFEST already covers.
// - the CFs that have blob indirection state (a format version record or a
//   relocation file addition). Repair gives these CFs the format fence.
// - per CF: the dropped-file records (see "Dropped blob files" at the top of
//   this file).
// Only fully decoded edits count. The caller undoes an incomplete atomic
// group (DiscardIncompleteAtomicGroup()).
class RepairOriginalManifestHandler : public VersionEditHandlerBase {
 public:
  explicit RepairOriginalManifestHandler(const ReadOptions& read_options)
      : VersionEditHandlerBase(read_options) {}

  // Every relocation file that the replayed MANIFEST adds and does not delete,
  // with its owner CF.
  std::map<uint64_t, RepairRelocationFile> files;

  // The relocation files in `files` that a loss record marks lost (lost
  // files, not files missing on disk).
  std::set<uint64_t> lost_relocation_files;

  // Per column family: route[old file name] = target relocation file.
  std::map<uint32_t, std::map<uint64_t, uint64_t>> routes;

  // Per CF: the last log number in the MANIFEST (see "WAL coverage" above
  // Repairer::PrepareOriginalReplayCoverage()).
  std::map<uint32_t, uint64_t> cf_floors;

  // Per CF: the SSTs of the MANIFEST, by file number.
  std::map<uint32_t, std::map<uint64_t, RepairOriginalTable>> cf_tables;
  std::map<uint32_t, std::string> cf_names{{0, kDefaultColumnFamilyName}};

  // CFs with blob indirection state (see the class comment).
  std::set<uint32_t> feature_cfs;

  // Per CF: the blob files that a dropped-file record (kDroppedBlobFile)
  // names. An open with best_efforts_blob_db_repair writes these records.
  std::map<uint32_t, std::set<uint64_t>> dropped_blob_files;
  SequenceNumber last_sequence = 0;

  void DiscardIncompleteAtomicGroup() {
    if (in_atomic_group_) {
      for (auto undo = undo_log_.rbegin(); undo != undo_log_.rend(); ++undo) {
        (*undo)();
      }
      undo_log_.clear();
      in_atomic_group_ = false;
    }
  }

 protected:
  // An atomic group keeps an undo log of the entries it changes instead of a
  // copy of every map, so its cost is linear in the group's own edits. Only a
  // CF drop saves the dropped CF's whole route and SST maps.
  Status OnAtomicGroupReplayBegin() override {
    undo_log_.clear();
    in_atomic_group_ = true;
    return Status::OK();
  }

  Status OnAtomicGroupReplayEnd() override {
    in_atomic_group_ = false;
    undo_log_.clear();
    return Status::OK();
  }

  // Each Save* records how to restore one entry of a member map; the restore
  // looks the map up again by key, so it stays valid if an outer entry is
  // erased and re-created within the group.
  template <typename GetMap, typename Key>
  void SaveMapEntry(GetMap get_map, const Key& key) {
    if (!in_atomic_group_) {
      return;
    }
    auto& map = get_map();
    const auto current = map.find(key);
    if (current == map.end()) {
      undo_log_.emplace_back([get_map, key]() { get_map().erase(key); });
    } else {
      undo_log_.emplace_back([get_map, key, value = current->second]() {
        get_map().insert_or_assign(key, value);
      });
    }
  }

  // For an outer per-CF map that an edit changes entry by entry: records only
  // that the CF entry must be erased again if the edit creates it. The inner
  // entries are saved one by one, so the inner map is never copied.
  template <typename GetMap, typename Key>
  void SaveOuterEntryExistence(GetMap get_map, const Key& key) {
    if (!in_atomic_group_ || get_map().count(key) != 0) {
      return;
    }
    undo_log_.emplace_back([get_map, key]() { get_map().erase(key); });
  }

  template <typename GetSet, typename Key>
  void SaveSetEntry(GetSet get_set, const Key& key) {
    if (!in_atomic_group_) {
      return;
    }
    if (get_set().count(key) == 0) {
      undo_log_.emplace_back([get_set, key]() { get_set().erase(key); });
    } else {
      undo_log_.emplace_back([get_set, key]() { get_set().insert(key); });
    }
  }

  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData**) override {
    const uint32_t cf_id = edit.GetColumnFamily();
    // Inside an atomic group, each change is saved in undo_log_ so that it can
    // be undone. An undo step calls one of these getters to find the map or
    // set again, then restores the entry by key (see SaveMapEntry() and
    // SaveSetEntry()).
    const auto get_files = [this]() -> auto& { return files; };
    const auto get_lost = [this]() -> auto& { return lost_relocation_files; };
    const auto get_routes = [this]() -> auto& { return routes; };
    const auto get_cf_routes = [this, cf_id]() -> auto& {
      return routes[cf_id];
    };
    const auto get_floors = [this]() -> auto& { return cf_floors; };
    const auto get_tables = [this]() -> auto& { return cf_tables; };
    const auto get_cf_tables = [this, cf_id]() -> auto& {
      return cf_tables[cf_id];
    };
    const auto get_names = [this]() -> auto& { return cf_names; };
    const auto get_feature_cfs = [this]() -> auto& { return feature_cfs; };
    const auto get_dropped = [this]() -> auto& { return dropped_blob_files; };
    const auto get_cf_dropped = [this, cf_id]() -> auto& {
      return dropped_blob_files[cf_id];
    };
    if (edit.IsColumnFamilyDrop()) {
      for (std::map<uint64_t, RepairRelocationFile>::iterator file =
               files.begin();
           file != files.end();) {
        if (file->second.column_family_id == cf_id) {
          SaveSetEntry(get_lost, file->first);
          lost_relocation_files.erase(file->first);
          SaveMapEntry(get_files, file->first);
          file = files.erase(file);
        } else {
          ++file;
        }
      }
      SaveMapEntry(get_routes, cf_id);
      routes.erase(cf_id);
      SaveMapEntry(get_floors, cf_id);
      cf_floors.erase(cf_id);
      SaveMapEntry(get_tables, cf_id);
      cf_tables.erase(cf_id);
      SaveMapEntry(get_names, cf_id);
      cf_names.erase(cf_id);
      SaveSetEntry(get_feature_cfs, cf_id);
      feature_cfs.erase(cf_id);
      SaveMapEntry(get_dropped, cf_id);
      dropped_blob_files.erase(cf_id);
      return Status::OK();
    }
    // Check every part of the edit before changing any map. A bad edit then
    // leaves the maps as they were after the previous edit. An incomplete
    // atomic group is undone as a whole (DiscardIncompleteAtomicGroup()).
    std::set<uint64_t> additions;
    for (const BlobRelocationFileAddition& addition :
         edit.GetBlobRelocationFileAdditions()) {
      Status status = addition.Validate();
      if (!status.ok()) {
        return status;
      }
      const auto current = files.find(addition.GetFileNumber());
      if (current != files.end() && current->second.column_family_id != cf_id) {
        return Status::Corruption("Repair loss file has conflicting ownership");
      }
      if (!additions.insert(addition.GetFileNumber()).second) {
        return Status::Corruption("Duplicate repair relocation file addition");
      }
    }
    // The owner of a live or just-added relocation file, or nullopt.
    const auto file_owner = [&](uint64_t number) -> std::optional<uint32_t> {
      if (additions.count(number) != 0) {
        return cf_id;
      }
      const auto current = files.find(number);
      return current == files.end()
                 ? std::nullopt
                 : std::optional<uint32_t>(current->second.column_family_id);
    };
    for (uint64_t number : edit.GetBlobPhysicalFileLosses()) {
      if (file_owner(number) != cf_id ||
          std::find(edit.GetBlobRelocationFileDeletions().begin(),
                    edit.GetBlobRelocationFileDeletions().end(),
                    number) != edit.GetBlobRelocationFileDeletions().end()) {
        return Status::Corruption("Repair loss record names no live file");
      }
    }
    for (uint64_t number : edit.GetBlobRelocationFileDeletions()) {
      const std::optional<uint32_t> owner = file_owner(number);
      if (additions.count(number) != 0 || (owner && *owner != cf_id)) {
        return Status::Corruption(
            "Repair loss deletion crosses owners/additions");
      }
    }
    for (const BlobRelocationRoute& route : edit.GetBlobRelocationRoutes()) {
      if (file_owner(route.GetTargetFileNumber()) != cf_id) {
        return Status::Corruption(
            "Repair route names no CF-owned relocation file");
      }
    }
    for (const BlobRelocationFileAddition& addition :
         edit.GetBlobRelocationFileAdditions()) {
      SaveMapEntry(get_files, addition.GetFileNumber());
      files.insert_or_assign(addition.GetFileNumber(),
                             RepairRelocationFile{cf_id, addition});
    }
    for (uint64_t number : edit.GetBlobPhysicalFileLosses()) {
      SaveSetEntry(get_lost, number);
      lost_relocation_files.insert(number);
    }
    const bool touches_routes = !edit.GetBlobRelocationRoutes().empty() ||
                                !edit.GetBlobRelocationRouteDeletions().empty();
    if (touches_routes) {
      // routes[cf_id] may be created below.
      SaveOuterEntryExistence(get_routes, cf_id);
    }
    for (const BlobRelocationRoute& route : edit.GetBlobRelocationRoutes()) {
      SaveMapEntry(get_cf_routes, route.GetOldFileNumber());
      routes[cf_id].insert_or_assign(route.GetOldFileNumber(),
                                     route.GetTargetFileNumber());
    }
    for (uint64_t source : edit.GetBlobRelocationRouteDeletions()) {
      SaveMapEntry(get_cf_routes, source);
      routes[cf_id].erase(source);
    }
    for (uint64_t number : edit.GetBlobRelocationFileDeletions()) {
      SaveMapEntry(get_files, number);
      files.erase(number);
      SaveSetEntry(get_lost, number);
      lost_relocation_files.erase(number);
    }
    if (edit.HasLogNumber()) {
      SaveMapEntry(get_floors, cf_id);
      cf_floors[cf_id] = edit.GetLogNumber();
    }
    if (!edit.GetDroppedBlobFiles().empty()) {
      // dropped_blob_files[cf_id] may be created below.
      SaveOuterEntryExistence(get_dropped, cf_id);
    }
    for (uint64_t number : edit.GetDroppedBlobFiles()) {
      SaveSetEntry(get_cf_dropped, number);
      dropped_blob_files[cf_id].insert(number);
    }
    if (edit.HasBlobIndirectionFormatVersion() ||
        !edit.GetBlobRelocationFileAdditions().empty()) {
      SaveSetEntry(get_feature_cfs, cf_id);
      feature_cfs.insert(cf_id);
    }
    if (edit.IsColumnFamilyAdd()) {
      SaveMapEntry(get_names, cf_id);
      cf_names[cf_id] = edit.GetColumnFamilyName();
    }
    if (!edit.GetDeletedFiles().empty() || !edit.GetNewFiles().empty()) {
      // cf_tables[cf_id] may be created below.
      SaveOuterEntryExistence(get_tables, cf_id);
    }
    for (const auto& deleted : edit.GetDeletedFiles()) {
      SaveMapEntry(get_cf_tables, deleted.second);
      cf_tables[cf_id].erase(deleted.second);
    }
    for (const auto& added : edit.GetNewFiles()) {
      SaveMapEntry(get_cf_tables, added.second.fd.GetNumber());
      cf_tables[cf_id].insert_or_assign(
          added.second.fd.GetNumber(),
          RepairOriginalTable{added.second.fd.GetFileSize(),
                              added.second.unique_id});
    }
    if (edit.HasLastSequence()) {
      if (in_atomic_group_) {
        undo_log_.emplace_back(
            [this, previous = last_sequence]() { last_sequence = previous; });
      }
      last_sequence = std::max(last_sequence, edit.GetLastSequence());
    }
    return Status::OK();
  }

 private:
  bool in_atomic_group_ = false;
  std::vector<std::function<void()>> undo_log_;
};

// Decodes a whole WAL batch before repair inserts any part of it, so that a
// bad batch changes no CF. Transaction markers (prepare, commit, rollback,
// noop) set requires_context, because repair has no transaction state to
// replay them; the normal open replays that WAL instead. An operation on an
// unknown CF is counted in ignored_cf_operations. Its key is not checked
// against a timestamp size, but its entity or blob index is still decoded.
class RepairBatchValidator : public WriteBatch::Handler {
 public:
  explicit RepairBatchValidator(ColumnFamilySet* families)
      : families_(families) {}
  bool requires_context = false;
  size_t ignored_cf_operations = 0;
  Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
    return Check(cf, key);
  }
  Status TimedPutCF(uint32_t cf, const Slice& key, const Slice&,
                    uint64_t) override {
    return Check(cf, key);
  }
  Status DeleteCF(uint32_t cf, const Slice& key) override {
    return Check(cf, key);
  }
  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    return Check(cf, key);
  }
  Status DeleteRangeCF(uint32_t cf, const Slice& begin,
                       const Slice& end) override {
    Status status = Check(cf, begin);
    return status.ok() ? Check(cf, end) : status;
  }
  Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
    return Check(cf, key);
  }
  Status PutEntityCF(uint32_t cf, const Slice& key,
                     const Slice& entity) override {
    Status status = Check(cf, key);
    if (!status.ok()) {
      return status;
    }
    // ForEachBlobIndex() skips the framing checks for entities without blob
    // columns, but a batch must decode fully before insertion. Deserialize()
    // checks the framing; the name order and blob indexes are checked here.
    WideColumns columns;
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    status =
        WideColumnSerialization::Deserialize(entity, columns, &blob_columns);
    if (!status.ok()) {
      return status;
    }
    for (size_t i = 1; i < columns.size(); ++i) {
      if (columns[i - 1].name().compare(columns[i].name()) >= 0) {
        return Status::Corruption("Wide column names are not strictly ordered");
      }
    }
    for (const auto& blob_column : blob_columns) {
      if (!blob_column.second.Validate().ok()) {
        return Status::Corruption("Error decoding blob index in wide column");
      }
    }
    return Status::OK();
  }
  Status PutBlobIndexCF(uint32_t cf, const Slice& key,
                        const Slice& value) override {
    Status status = Check(cf, key);
    BlobIndex index;
    return status.ok() ? index.DecodeFrom(value) : status;
  }
  Status MarkBeginPrepare(bool) override { return Context(); }
  Status MarkEndPrepare(const Slice&) override { return Context(); }
  Status MarkNoop(bool) override { return Context(); }
  Status MarkRollback(const Slice&) override { return Context(); }
  Status MarkCommit(const Slice&) override { return Context(); }
  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
    return Context();
  }

 private:
  Status Context() {
    requires_context = true;
    return Status::OK();
  }
  Status Check(uint32_t cf, const Slice& key) {
    const ColumnFamilyData* cfd = families_->GetColumnFamily(cf);
    if (cfd == nullptr) {
      ++ignored_cf_operations;
      return Status::OK();
    }
    return key.size() >= cfd->user_comparator()->timestamp_size()
               ? Status::OK()
               : Status::Corruption("Repair WAL key lacks timestamp");
  }
  ColumnFamilySet* const families_;
};

// Runs one RepairDB call. Both modes start in Run(), which finds the files
// (phase (a) at the top of this file). Legacy repair then runs phases (b) to
// (d). Feature repair runs the steps of "Feature repair" there.
class Repairer {
 public:
  Repairer(const std::string& dbname, const DBOptions& db_options,
           const std::vector<ColumnFamilyDescriptor>& column_families,
           const ColumnFamilyOptions& default_cf_opts,
           const ColumnFamilyOptions& unknown_cf_opts, bool create_unknown_cfs)
      : dbname_(dbname),
        db_session_id_(DBImpl::GenerateDbSessionId(db_options.env)),
        env_(db_options.env),
        file_options_(),
        db_options_(SanitizeOptions(dbname_, db_options)),
        immutable_db_options_(ImmutableDBOptions(db_options_)),
        icmp_(default_cf_opts.comparator),
        default_cf_opts_(SanitizeCfOptions(immutable_db_options_,
                                           /*read_only*/ false,
                                           default_cf_opts)),
        default_iopts_(
            ImmutableOptions(immutable_db_options_, default_cf_opts_)),
        default_mopts_(MutableCFOptions(default_cf_opts_)),
        unknown_cf_opts_(SanitizeCfOptions(immutable_db_options_,
                                           /*read_only*/ false,
                                           unknown_cf_opts)),
        create_unknown_cfs_(create_unknown_cfs),
        raw_table_cache_(
            // TableCache can be small since we expect each table to be opened
            // once.
            NewLRUCache(10, db_options_.table_cache_numshardbits)),
        table_cache_(new TableCache(default_iopts_, &file_options_,
                                    raw_table_cache_.get(),
                                    /*block_cache_tracer=*/nullptr,
                                    /*io_tracer=*/nullptr, db_session_id_)),
        wb_(db_options_.db_write_buffer_size),
        wc_(db_options_.delayed_write_rate),
        vset_(dbname_, &immutable_db_options_, MutableDBOptions{db_options_},
              file_options_, raw_table_cache_.get(), &wb_, &wc_,
              /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
              /*db_id=*/"", db_session_id_, db_options.daily_offpeak_time_utc,
              /*error_handler=*/nullptr, /*read_only=*/false),
        next_file_number_(1),
        db_lock_(nullptr),
        closed_(false) {
    for (const auto& cfd : column_families) {
      cf_name_to_opts_[cfd.name] = SanitizeCfOptions(
          immutable_db_options_, /*read_only=*/false, cfd.options);
    }
  }

  // No copying allowed
  Repairer(const Repairer&) = delete;
  Repairer& operator=(const Repairer&) = delete;
  Repairer(Repairer&&) = delete;
  Repairer& operator=(Repairer&&) = delete;

  const ColumnFamilyOptions* GetColumnFamilyOptions(
      const std::string& cf_name) {
    if (cf_name_to_opts_.find(cf_name) == cf_name_to_opts_.end()) {
      if (create_unknown_cfs_) {
        return &unknown_cf_opts_;
      }
      return nullptr;
    }
    return &cf_name_to_opts_[cf_name];
  }

  // Adds a column family to the VersionSet with cf_options_ and updates
  // manifest.
  Status AddColumnFamily(const std::string& cf_name, uint32_t cf_id) {
    // TODO: plumb Env::IOActivity, Env::IOPriority;
    const ReadOptions read_options;
    const WriteOptions write_options;

    const auto* cf_opts = GetColumnFamilyOptions(cf_name);
    if (cf_opts == nullptr) {
      return Status::Corruption("Encountered unknown column family with name=" +
                                cf_name + ", id=" + std::to_string(cf_id));
    }
    Options opts(db_options_, *cf_opts);

    VersionEdit edit;
    edit.SetComparatorName(opts.comparator->Name());
    edit.SetPersistUserDefinedTimestamps(opts.persist_user_defined_timestamps);
    edit.SetLogNumber(0);
    edit.SetColumnFamily(cf_id);
    ColumnFamilyData* cfd;
    cfd = nullptr;
    edit.AddColumnFamily(cf_name);

    mutex_.Lock();
    std::unique_ptr<FSDirectory> db_dir;
    Status status = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(),
                                                        &db_dir, nullptr);
    if (status.ok()) {
      status = vset_.LogAndApply(cfd, read_options, write_options, &edit,
                                 &mutex_, db_dir.get(),
                                 false /* new_descriptor_log */, cf_opts);
    }
    mutex_.Unlock();
    return status;
  }

  Status Close() {
    Status s = Status::OK();
    if (!closed_) {
      if (repair_catalog_log_ != nullptr) {
        s = repair_catalog_log_->Close(WriteOptions());
        repair_catalog_log_.reset();
      }
      if (db_lock_ != nullptr) {
        Status unlocked = env_->UnlockFile(db_lock_);
        if (s.ok()) {
          s = unlocked;
        } else {
          unlocked.PermitUncheckedError();
        }
        db_lock_ = nullptr;
      }
      closed_ = true;
    }
    return s;
  }

  ~Repairer() { Close().PermitUncheckedError(); }

  Status Run() {
    Status status = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!status.ok()) {
      return status;
    }
    bool recognized = false;
    Status catalog_status =
        ReadRepairCatalog(env_, dbname_, &repair_catalog_, &recognized);
    if (!catalog_status.ok() && recognized) {
      return catalog_status;
    }
    if (recognized) {
      bool committed = false;
      status = ResolveRepairCatalog(env_, dbname_, repair_catalog_,
                                    /*read_only=*/false,
                                    /*repair_retry=*/true, &committed,
                                    /*seals_verified=*/false);
      if (!status.ok() || committed) {
        return status;
      }
      repair_catalog_ = {};
    }
    status = FindFiles();
    const size_t original_blob_count = blob_file_paths_.size();
    if (status.ok()) {
      // Options are not evidence. Probe the blob files in every CF path before
      // repair chooses its mode, also when enable_blob_indirection is now
      // off.
      status = FindFeatureColumnFamilyBlobFiles(true);
    }
    if (status.ok()) {
      status = DetectBlobIndirection();
    }
    if (status.ok() && !feature_repair_) {
      // Legacy repair does not use the blob files that the probe found in
      // the CF paths, so drop them. Their file numbers stay tracked, so the
      // SSTs that legacy repair writes from WALs never reuse those numbers.
      blob_file_paths_.resize(original_blob_count);
      return RunLegacyRepair();
    }
    // An unreadable or unrecognized REPAIR-IN-PROGRESS file blocks only
    // feature repair. Legacy repair never writes a catalog, so the file cannot
    // belong to it.
    if (status.ok() && !catalog_status.ok()) {
      return catalog_status;
    }
    if (status.ok()) {
      // Feature repair needs every blob file of the feature CF paths: it may
      // copy values from them, and new file numbers must be above theirs. So
      // a directory that cannot be listed now fails the repair. This scan
      // runs before the catalog starts, so these files become input records.
      status = FindFeatureColumnFamilyBlobFiles(false);
    }
    if (status.ok()) {
      status = ReadOriginalManifest();
    }
    if (status.ok()) {
      status = StartRepairCatalog();
    }
    if (status.ok()) {
      status = AllocateRepairFileNumber(&repair_manifest_file_number_);
      if (status.ok()) {
        // The staged MANIFEST is named REPAIR-<number>-<session>.dbtmp. A blob
        // file found later in another cf_path can have the same number, and
        // copying it to the canonical path uses <number>.dbtmp. The REPAIR-
        // prefix keeps the two names apart, and open never takes a
        // REPAIR-*.dbtmp file for a MANIFEST.
        repair_manifest_path_ = dbname_ + "/REPAIR-" +
                                std::to_string(repair_manifest_file_number_) +
                                "-" + db_session_id_ + ".dbtmp";
        status = RegisterRepairOutput(repair_manifest_path_,
                                      repair_manifest_file_number_, 0, 0,
                                      /*sync=*/false);
        if (status.ok()) {
          status = RegisterRepairOutput(
              DescriptorFileName(dbname_, repair_manifest_file_number_),
              repair_manifest_file_number_, 0, 0);
        }
        if (status.ok()) {
          status = vset_.RecoverForRepair(
              repair_manifest_path_, repair_manifest_file_number_,
              {{kDefaultColumnFamilyName, default_cf_opts_}});
        }
      }
    }
    if (status.ok()) {
      // Need to scan existing SST files first so the column families are
      // created before we process WAL files
      status = ExtractMetaData(/*tables_built_from_wals=*/false);

      // ExtractMetaData() uses table_fds_ to know which SST files' metadata to
      // extract -- we need to clear it here since metadata for existing SST
      // files has been extracted already
      if (status.ok()) {
        // ExtractMetaData() created the column families that it found in the
        // SSTs, and it may have marked more of them as feature CFs. Scan the
        // cf_paths of these CFs for blob files too. The catalog has started,
        // so this call appends an input record for each new blob file. These
        // files existed before repair, so they are inputs even though some
        // outputs already exist.
        status = FindFeatureColumnFamilyBlobFiles(false);
      }
      if (status.ok()) {
        table_fds_.clear();
        status = PrepareOriginalReplayCoverage();
        if (status.ok()) {
          status = ConvertLogFilesToTables();
        }
        if (status.ok()) {
          status = ExtractMetaData(/*tables_built_from_wals=*/true);
        }
      }
    }
    if (status.ok()) {
      status = RepairBlobState();
      if (status.ok()) {
        status = AddTables();
      }
    }
    if (!status.ok()) {
      // The catalog cleanup of a retry would also remove it.
      DeleteRepairScratchFile(&reference_scratch_).PermitUncheckedError();
    }
    if (status.ok()) {
      status = PublishManifest();
    }
    if (status.ok()) {
      uint64_t bytes = 0;
      for (size_t i = 0; i < tables_.size(); i++) {
        bytes += tables_[i].meta.fd.GetFileSize();
      }
      ROCKS_LOG_WARN(db_options_.info_log,
                     "**** Repaired rocksdb %s; "
                     "recovered %" ROCKSDB_PRIszt " files; %" PRIu64
                     " bytes. "
                     "Some data may have been lost. "
                     "****",
                     dbname_.c_str(), tables_.size(), bytes);
    }
    return status;
  }

 private:
  using BlobIdentity = std::pair<uint64_t, uint64_t>;
  using RepairReferenceSource = BlobReferenceGroupKey;
  static RepairReferenceSource ReferenceSource(const BlobReference& reference) {
    return reference.group_key();
  }
  // Returns the key (origin, original offset) of a reference. Visits and merges
  // order references by this key. One repair group can hold references of
  // several origins (all references of one generation number). A run that the
  // SST scan writes holds the references of one SST and one source, so of one
  // origin. A run that a visit reads is in key order, and a run that a merge
  // pass writes can hold several origins.
  static BlobIdentity ReferenceIdentity(const BlobReference& reference) {
    return {reference.origin_file_number, reference.original_value_offset};
  }

  // A repair scratch file: holds reference runs only while RepairDB runs.
  // It is registered as an output intent, so the catalog cleanup removes it
  // after a failure or a crash.
  struct RepairScratchFile {
    std::string path;
    std::unique_ptr<WritableFileWriter> writer;
    std::unique_ptr<RandomAccessFileReader> reader;
    uint64_t size = 0;
  };

  // The blob references of one (SST, source) in a scratch file: chunks of
  // encoded records, each a BlobReference plus the SST user key of a legacy
  // reference. Visits read runs instead of SSTs, so they read only reference
  // bytes, hold no table reader, and check legacy keys without an SST read.
  struct RepairReferenceRun {
    RepairScratchFile* file = nullptr;
    // (offset, size) of each chunk in `file`.
    std::vector<std::pair<uint64_t, uint32_t>> chunks;
    uint64_t count = 0;
    // False once a reference has a smaller offset than the one before it.
    bool ordered = true;
    uint64_t last_offset = 0;
    // The chunk being filled.
    std::string pending;
    // Set by PrepareRepairRun().
    bool prepared = false;
    // Prepared to read the exact metadata instead (see PrepareRepairRun()).
    bool fallback = false;
  };
  static constexpr size_t kRepairRunChunkSize = 4096;
  // At most this many runs (64) are merged at once, so a merge holds at most
  // 64 run readers. Each reader buffers one chunk of about kRepairRunChunkSize
  // (4096 bytes); a chunk is larger only when one record is larger. So one
  // merge buffers about 256 KB (64 x 4096 bytes, an estimate). 64 is a chosen
  // limit, not a measured best value. A larger group is first merged in passes
  // into intermediate runs. The limit counts only runs. PrepareGroupCursors()
  // adds each metadata fallback after the capped runs, and each one opens one
  // more SST reader.
  static constexpr size_t kMaxRepairCursorFanIn = 64;

  struct TableInfo {
    FileMetaData meta;
    uint32_t column_family_id;
    std::string column_family_name;
    std::set<RepairReferenceSource> reference_sources;
    std::map<RepairReferenceSource, RepairReferenceRun> runs;
  };

  static void EncodeRepairRunRecord(const BlobReference& reference,
                                    const Slice& user_key,
                                    std::string* output) {
    PutVarint32(output, static_cast<uint32_t>(reference.encoding_class));
    PutVarint64(output, reference.generation_file_number);
    PutVarint64(output, reference.origin_file_number);
    PutVarint64(output, reference.original_value_offset);
    PutVarint64(output, reference.stored_value_size);
    PutVarint64(output, reference.record_bytes);
    output->push_back(static_cast<char>(reference.compression));
    PutFixed32(output, reference.stable_checksum);
    PutFixed32(output, reference.stored_checksum);
    PutLengthPrefixedSlice(output, user_key);
  }

  static bool DecodeRepairRunRecord(Slice* input, BlobReference* reference,
                                    Slice* user_key) {
    uint32_t encoding_class = 0;
    if (!GetVarint32(input, &encoding_class) ||
        !GetVarint64(input, &reference->generation_file_number) ||
        !GetVarint64(input, &reference->origin_file_number) ||
        !GetVarint64(input, &reference->original_value_offset) ||
        !GetVarint64(input, &reference->stored_value_size) ||
        !GetVarint64(input, &reference->record_bytes) || input->empty()) {
      return false;
    }
    reference->encoding_class =
        static_cast<BlobReferenceEncodingClass>(encoding_class);
    reference->compression = static_cast<CompressionType>((*input)[0]);
    input->remove_prefix(1);
    return GetFixed32(input, &reference->stable_checksum) &&
           GetFixed32(input, &reference->stored_checksum) &&
           GetLengthPrefixedSlice(input, user_key);
  }

  Status OpenRepairScratchFile(std::unique_ptr<RepairScratchFile>* file) {
    uint64_t number = 0;
    Status status = AllocateRepairFileNumber(&number);
    if (!status.ok()) {
      return status;
    }
    auto scratch = std::make_unique<RepairScratchFile>();
    // Like the staged MANIFEST, this name is not discoverable as a DB file.
    scratch->path = dbname_ + "/REPAIR-" + std::to_string(number) + "-" +
                    db_session_id_ + "-refs.dbtmp";
    status = RegisterRepairOutput(scratch->path, number, 0, 0);
    std::unique_ptr<FSWritableFile> writable;
    if (status.ok()) {
      status = NewWritableFile(env_->GetFileSystem().get(), scratch->path,
                               &writable, FileOptions());
    }
    if (status.ok()) {
      status = RandomAccessFileReader::Create(env_->GetFileSystem(),
                                              scratch->path, FileOptions(),
                                              &scratch->reader, nullptr);
      if (!status.ok()) {
        writable.reset();
        env_->DeleteFile(scratch->path).PermitUncheckedError();
      }
    }
    if (!status.ok()) {
      return status;
    }
    scratch->writer = std::make_unique<WritableFileWriter>(
        std::move(writable), scratch->path, FileOptions());
    *file = std::move(scratch);
    return Status::OK();
  }

  Status DeleteRepairScratchFile(std::unique_ptr<RepairScratchFile>* file) {
    if (*file == nullptr) {
      return Status::OK();
    }
    Status status = (*file)->writer->Close(IOOptions());
    (*file)->reader.reset();
    Status deleted = env_->DeleteFile((*file)->path);
    file->reset();
    return status.ok() ? deleted : status;
  }

  // Points `run` at the main scratch file, creating it on first use.
  Status OpenReferenceScratch(RepairReferenceRun* run) {
    if (reference_scratch_ == nullptr) {
      Status status = OpenRepairScratchFile(&reference_scratch_);
      if (!status.ok()) {
        return status;
      }
    }
    run->file = reference_scratch_.get();
    return Status::OK();
  }

  Status AppendRepairRunChunk(RepairReferenceRun* run) {
    if (run->pending.empty()) {
      return Status::OK();
    }
    RepairScratchFile* const file = run->file;
    Status status = file->writer->Append(run->pending, IOOptions());
    if (status.ok()) {
      // Flushed so the reader sees the chunk; the file is never synced.
      status = file->writer->Flush(IOOptions());
    }
    TEST_SYNC_POINT_CALLBACK("Repairer::AppendRepairRunChunk", &status);
    if (!status.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Repair scratch write to %s failed: %s",
                      file->path.c_str(), status.ToString().c_str());
      return status;
    }
    run->chunks.emplace_back(file->size,
                             static_cast<uint32_t>(run->pending.size()));
    file->size += run->pending.size();
    run->pending.clear();
    return Status::OK();
  }

  // Writes the last chunk and frees the chunk buffer; the run keeps only its
  // chunk list.
  Status FinishRepairRun(RepairReferenceRun* run) {
    Status status = AppendRepairRunChunk(run);
    std::string().swap(run->pending);
    return status;
  }

  Status AppendRepairRunRecord(RepairReferenceRun* run,
                               const BlobReference& reference,
                               const Slice& user_key) {
    if (run->count != 0 && reference.original_value_offset < run->last_offset) {
      run->ordered = false;
    }
    run->last_offset = reference.original_value_offset;
    ++run->count;
    std::string record;
    EncodeRepairRunRecord(reference, user_key, &record);
    if (!run->pending.empty() &&
        run->pending.size() + record.size() > kRepairRunChunkSize) {
      Status status = AppendRepairRunChunk(run);
      if (!status.ok()) {
        return status;
      }
    }
    run->pending.append(record);
    return Status::OK();
  }

  // Adds one reference found by the scan of `table` to its run.
  Status AppendScannedReference(TableInfo* table, BlobReference reference,
                                const Slice& user_key) {
    // A normal blob file record holds a header and the user key, so its record
    // bytes are the stored size plus that overhead. A flattened reference
    // points into a relocation file, which has no such header.
    if (reference.encoding_class !=
        BlobReferenceEncodingClass::kIndirectFlattened) {
      const uint64_t header_bytes =
          BlobLogRecord::CalculateAdjustmentForRecordHeader(user_key.size());
      if (reference.stored_value_size >
          std::numeric_limits<uint64_t>::max() - header_bytes) {
        return Status::Corruption("Repair physical record size overflows");
      }
      reference.record_bytes = reference.stored_value_size + header_bytes;
    }
    RepairReferenceRun& run = table->runs[ReferenceSource(reference)];
    if (run.file == nullptr) {
      Status status = OpenReferenceScratch(&run);
      if (!status.ok()) {
        return status;
      }
    }
    if (reference.HasStableIdentity()) {
      return AppendRepairRunRecord(&run, reference, Slice());
    }
#ifndef NDEBUG
    // Test only: lets a test give the run a different user key, to check that
    // repair compares the run key with the blob record key.
    std::string test_key = user_key.ToString();
    TEST_SYNC_POINT_CALLBACK("Repairer::AppendScannedReference:LegacyKey",
                             &test_key);
    if (test_key != user_key) {
      const uint64_t header_bytes =
          BlobLogRecord::CalculateAdjustmentForRecordHeader(test_key.size());
      reference.record_bytes = reference.stored_value_size + header_bytes;
      return AppendRepairRunRecord(&run, reference, test_key);
    }
#endif  // NDEBUG
    return AppendRepairRunRecord(&run, reference, user_key);
  }

  struct BlobFilePath {
    uint64_t file_number = 0;
    std::string path;
  };

  struct NormalBlobFileInfo {
    BlobFileAddition addition;
    uint32_t column_family_id = 0;
    uint64_t file_size = 0;
    CompressionType compression = kNoCompression;
    std::string path;
    bool is_canonical = false;
  };

  struct RelocationBlobFileInfo {
    uint32_t column_family_id = 0;
    uint64_t file_size = 0;
    std::string path;
    bool is_canonical = false;
    BlobRelocationFileAddition addition;
    BlobRelocationDirectory directory;
  };

  using NormalBlobFileCandidates = std::multimap<uint64_t, NormalBlobFileInfo>;
  using RelocationBlobFileCandidates =
      std::multimap<uint64_t, RelocationBlobFileInfo>;
  // (column family, origin) -> every relocation candidate whose directory
  // lists that origin, in file-number order. Built once after discovery so
  // that lookups do not scan all relocation files.
  using RelocationOriginIndex =
      std::multimap<std::pair<uint32_t, uint64_t>,
                    const RelocationBlobFileCandidates::value_type*>;

  static RelocationOriginIndex IndexRelocationOrigins(
      const RelocationBlobFileCandidates& relocation_files) {
    RelocationOriginIndex index;
    for (const auto& file : relocation_files) {
      for (const BlobRelocationOrigin& origin :
           file.second.directory.origins()) {
        index.emplace(std::make_pair(file.second.column_family_id,
                                     origin.origin_file_number),
                      &file);
      }
    }
    return index;
  }

  // (column family, served file) -> every relocation candidate whose
  // directory lists that file in its served list, in file-number order. The
  // route of a generation number can point only to a file that serves it.
  static RelocationOriginIndex IndexRelocationServedFiles(
      const RelocationBlobFileCandidates& relocation_files) {
    RelocationOriginIndex index;
    for (const auto& file : relocation_files) {
      for (uint64_t served : file.second.directory.served_file_numbers()) {
        index.emplace(std::make_pair(file.second.column_family_id, served),
                      &file);
      }
    }
    return index;
  }

  struct BlobReaderCache {
    uint64_t file_number = kInvalidBlobFileNumber;
    std::string path;
    std::unique_ptr<BlobFileReader> reader;
    std::string normal_record_path;
    std::unique_ptr<BlobLogSequentialReader> normal_records;
    uint64_t checked_offset = 0;
    uint64_t checked_value_size = 0;
    uint64_t checked_record_bytes = 0;
    // crc32c of the stored value bytes of the checked record.
    uint32_t checked_value_checksum = 0;
    std::string checked_user_key;
  };

  // One (SST, source) input of a repair group. It names one reference run,
  // not a whole repair group.
  struct RepairRunInput {
    TableInfo* table;
    RepairReferenceSource source;
  };

  // Reads the records of one run, chunk by chunk.
  class RepairRunReader {
   public:
    explicit RepairRunReader(const RepairReferenceRun* run) : run_(run) {}

    // Loads the next record. `*valid` is false at the end of the run.
    Status Next(bool* valid) {
      *valid = false;
      while (remaining_.empty()) {
        if (chunk_index_ == run_->chunks.size()) {
          return Status::OK();
        }
        const std::pair<uint64_t, uint32_t>& chunk =
            run_->chunks[chunk_index_++];
        buffer_.resize(chunk.second);
        Slice result;
        Status status = run_->file->reader->Read(
            chunk.first, chunk.second, IOOptions(), &result, buffer_.data(),
            /*dbg=*/nullptr);
        if (!status.ok()) {
          return status;
        }
        if (result.size() != chunk.second) {
          return Status::Corruption("Truncated repair reference run");
        }
        if (result.data() != buffer_.data()) {
          std::memmove(buffer_.data(), result.data(), result.size());
        }
        remaining_ = Slice(buffer_.data(), buffer_.size());
      }
      Slice key;
      if (!DecodeRepairRunRecord(&remaining_, &reference_, &key)) {
        return Status::Corruption("Malformed repair reference run");
      }
      key_.assign(key.data(), key.size());
      *valid = true;
      return Status::OK();
    }

    const BlobReference& reference() const { return reference_; }
    // The user key of a legacy reference. It is empty for an indirect
    // reference, which has a stable identity
    // (BlobReference::HasStableIdentity()).
    const std::string& key() const { return key_; }

   private:
    const RepairReferenceRun* const run_;
    size_t chunk_index_ = 0;
    std::string buffer_;
    Slice remaining_;
    BlobReference reference_;
    std::string key_;
  };

  // One input of a group visit: a run, or (`run` null) the exact metadata of
  // an SST whose legacy data run is not in offset order.
  struct RepairCursorSpec {
    RepairRunInput input;
    const RepairReferenceRun* run = nullptr;
  };

  struct RepairReferenceCursor {
    Repairer* repairer;
    RepairCursorSpec spec;
    std::unique_ptr<RepairRunReader> run_reader;
    // Metadata fallback only.
    ReadOptions read_options;
    TableReader* table_reader = nullptr;
    std::unique_ptr<InternalIterator> data_iterator;
    std::unique_ptr<BlobReferenceIterator> reference_iterator;
    std::unique_ptr<InternalIterator> legacy_lookup;
    std::shared_ptr<CacheReservationManager> reservation_manager;
    bool valid = false;

    Status Open() {
      if (spec.run != nullptr) {
        run_reader = std::make_unique<RepairRunReader>(spec.run);
        return run_reader->Next(&valid);
      }
      TEST_SYNC_POINT("Repairer::RepairReferenceCursor:MetadataFallback");
      Status status = repairer->OpenRepairMetadataIterator(
          spec.input, &read_options, &table_reader, &data_iterator,
          &reservation_manager, &reference_iterator);
      valid = status.ok() && reference_iterator->Valid();
      return status;
    }

    const BlobReference& reference() const {
      assert(valid);
      return run_reader != nullptr ? run_reader->reference()
                                   : reference_iterator->reference();
    }

    Status Next() {
      assert(valid);
      if (run_reader != nullptr) {
        return run_reader->Next(&valid);
      }
      Status status = reference_iterator->Next();
      valid = status.ok() && reference_iterator->Valid();
      return status;
    }

    Status VerifyLegacyUserKey(const BlobReference& expected,
                               const std::string& blob_user_key) {
      if (run_reader != nullptr) {
        // The run kept the referencing SST key; no SST read is needed.
        std::string sst_key = run_reader->key();
        TEST_SYNC_POINT_CALLBACK(
            "Repairer::ScanTable:LegacyBlobReferenceUserKey", &sst_key);
        return sst_key == blob_user_key
                   ? Status::OK()
                   : Status::Corruption(
                         "Legacy blob key does not match SST contents");
      }
      ColumnFamilyData* const cfd =
          repairer->vset_.GetColumnFamilySet()->GetColumnFamily(
              spec.input.table->column_family_id);
      const InternalKey target(blob_user_key, kMaxSequenceNumber,
                               kValueTypeForSeek);
      if (legacy_lookup == nullptr) {
        legacy_lookup.reset(table_reader->NewIterator(
            read_options, nullptr, nullptr, true, TableReaderCaller::kRepair));
      }
      legacy_lookup->Seek(target.Encode());
      for (; legacy_lookup->Valid(); legacy_lookup->Next()) {
        ParsedInternalKey key;
        Status status = ParseInternalKey(legacy_lookup->key(), &key, false);
        if (!status.ok()) {
          return status;
        }
        if (!cfd->user_comparator()->Equal(key.user_key, blob_user_key)) {
          break;
        }
        bool found = false;
        const auto check = [&](const BlobIndex& index) -> Status {
          BlobReference actual;
          bool included = false;
          Status decoded =
              BlobReference::FromBlobIndex(index, &actual, &included);
          found |=
              decoded.ok() && included && !actual.HasStableIdentity() &&
              actual.generation_file_number ==
                  expected.generation_file_number &&
              actual.original_value_offset == expected.original_value_offset &&
              actual.stored_value_size == expected.stored_value_size &&
              actual.compression == expected.compression;
          return decoded;
        };
        if (key.type == kTypeBlobIndex) {
          BlobIndex index;
          status = index.DecodeFrom(legacy_lookup->value());
          if (status.ok()) {
            status = check(index);
          }
        } else if (key.type == kTypeWideColumnEntity) {
          status = WideColumnSerialization::ForEachBlobIndex(
              legacy_lookup->value(), check);
        }
        if (!status.ok()) {
          return status;
        }
        if (found) {
          std::string copied_key = key.user_key.ToString();
          TEST_SYNC_POINT_CALLBACK(
              "Repairer::ScanTable:LegacyBlobReferenceUserKey", &copied_key);
          return copied_key == blob_user_key
                     ? Status::OK()
                     : Status::Corruption(
                           "Legacy blob key does not match SST contents");
        }
      }
      const Status status = legacy_lookup->status();
      return status.ok()
                 ? Status::Corruption(
                       "Legacy blob reference is absent from SST user key")
                 : status;
    }

    ~RepairReferenceCursor() {
      if (reference_iterator == nullptr && reservation_manager != nullptr) {
        reservation_manager->UpdateCacheReservation(0).PermitUncheckedError();
      }
    }
  };

  // Opens the exact metadata group of `input` for reading. The data iterator
  // keeps the table reader alive.
  Status OpenRepairMetadataIterator(
      const RepairRunInput& input, ReadOptions* read_options,
      TableReader** table_reader,
      std::unique_ptr<InternalIterator>* data_iterator,
      std::shared_ptr<CacheReservationManager>* reservation_manager,
      std::unique_ptr<BlobReferenceIterator>* reference_iterator) {
    read_options->fill_cache = false;
    read_options->verify_checksums = true;
    read_options->total_order_seek = true;
    ColumnFamilyData* const cfd = vset_.GetColumnFamilySet()->GetColumnFamily(
        input.table->column_family_id);
    if (cfd == nullptr) {
      return Status::Corruption("Missing repair cursor column family");
    }
    data_iterator->reset(table_cache_->NewIterator(
        *read_options, file_options_, cfd->internal_comparator(),
        input.table->meta, nullptr, cfd->GetLatestMutableCFOptions(),
        table_reader, nullptr, TableReaderCaller::kRepair, nullptr, true, -1, 0,
        nullptr, nullptr, false, /*seek_target=*/nullptr,
        /*prefix_filtered_table=*/nullptr, nullptr, nullptr, false, nullptr,
        RepairTableOpenOptions()));
    Status status = (*data_iterator)->status();
    if (!status.ok() || *table_reader == nullptr) {
      return status.ok() ? Status::Corruption("Missing repair cursor reader")
                         : status;
    }
    const BlockBasedTableOptions* const options =
        GetBlockBasedTableWriterOptions(
            cfd->GetLatestMutableCFOptions().table_factory.get());
    if (options != nullptr && options->block_cache != nullptr) {
      *reservation_manager = std::make_shared<CacheReservationManagerImpl<
          CacheEntryRole::kBlobReferenceMetadataBuildingBuffer>>(
          options->block_cache);
    }
    status = (*table_reader)
                 ->NewBlobReferenceIterator(
                     *read_options, input.table->meta.blob_reference_descriptor,
                     *reservation_manager, reference_iterator);
    if (status.ok()) {
      status = (*reference_iterator)->SeekGroup(input.source);
    }
    return status;
  }

  // Prepares the runs of an exact SST while its scan still has the table open,
  // so building the runs does not open the SST again (a visit that falls back
  // opens it again). An indirect source copies its metadata group into a run. A
  // legacy source keeps its scanned run, which has the user keys, after
  // checking that it equals the metadata group; if the run is not in offset
  // order, it is marked to fall back: its visits read the metadata and look up
  // keys in the SST.
  Status PrepareExactTableRuns(TableReader* reader,
                               const ReadOptions& read_options,
                               TableInfo* table) {
    for (const RepairReferenceSource& source : table->reference_sources) {
      std::unique_ptr<BlobReferenceIterator> metadata;
      Status status = reader->NewBlobReferenceIterator(
          read_options, table->meta.blob_reference_descriptor,
          /*reservation_manager=*/nullptr, &metadata);
      if (status.ok()) {
        status = metadata->SeekGroup(source);
      }
      const bool stable =
          source.encoding_class != BlobReferenceEncodingClass::kDirect;
      if (status.ok() && stable) {
        RepairReferenceRun& copied = table->runs[source];
        copied = RepairReferenceRun();
        status = OpenReferenceScratch(&copied);
        while (status.ok() && metadata->Valid()) {
          status =
              AppendRepairRunRecord(&copied, metadata->reference(), Slice());
          if (status.ok()) {
            status = metadata->Next();
          }
        }
        if (status.ok()) {
          status = FinishRepairRun(&copied);
        }
        if (status.ok() && (copied.count == 0 || !copied.ordered)) {
          status = Status::Corruption(
              "Repair SST metadata group is empty or not ordered");
        }
        copied.prepared = status.ok();
      } else if (status.ok()) {
        const auto found = table->runs.find(source);
        if (found == table->runs.end() || found->second.count == 0) {
          return Status::Corruption(
              "Repair SST is missing a required reference source");
        }
        RepairReferenceRun& scanned = found->second;
        bool unordered = !scanned.ordered;
        TEST_SYNC_POINT_CALLBACK("Repairer::PrepareRepairRun:Unordered",
                                 &unordered);
        if (unordered) {
          scanned.fallback = true;
        } else {
          RepairRunReader run_reader(&scanned);
          bool valid = false;
          status = run_reader.Next(&valid);
          while (status.ok() && (valid || metadata->Valid())) {
            if (!valid || !metadata->Valid() ||
                !SameBlobReference(run_reader.reference(),
                                   metadata->reference())) {
              status = Status::Corruption(
                  "Repair SST blob reference metadata does not match its "
                  "data");
              break;
            }
            status = run_reader.Next(&valid);
            if (status.ok()) {
              status = metadata->Next();
            }
          }
        }
        scanned.prepared = status.ok();
      }
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }

  // The run that the visits of `input` read. An SST without exact reference
  // metadata (empty blob_reference_descriptor) uses the run written by its
  // scan, which must be in offset order (as flush output is). An exact SST's
  // runs were prepared by its scan; `*run` is null for a legacy run that
  // falls back to the metadata.
  Status PrepareRepairRun(const RepairRunInput& input,
                          const RepairReferenceRun** run) {
    *run = nullptr;
    TableInfo* const table = input.table;
    const auto found = table->runs.find(input.source);
    if (found == table->runs.end() || found->second.count == 0) {
      return Status::Corruption(
          "Repair SST is missing a required reference source");
    }
    RepairReferenceRun& prepared = found->second;
    if (table->meta.blob_reference_descriptor.empty()) {
      if (!prepared.ordered) {
        return Status::Corruption(
            "Repair physical references are not in (origin, original offset) "
            "order");
      }
      *run = &prepared;
      return Status::OK();
    }
    if (!prepared.prepared) {
      return Status::Corruption("Repair SST reference run was not prepared");
    }
    *run = prepared.fallback ? nullptr : &prepared;
    return Status::OK();
  }

  // The prepared inputs of one group: at most the fan-in limit of runs, some
  // of them merged, plus metadata fallbacks. Owns the intermediate runs.
  struct RepairGroupCursors {
    std::vector<RepairCursorSpec> specs;
    std::vector<std::unique_ptr<RepairReferenceRun>> merged_runs;
    std::unique_ptr<RepairScratchFile> scratch;
  };

  // Merges `inputs` (all runs) into `*merged`, written to merged->file. Equal
  // identities (origin, original offset) must have the same metadata. For a
  // legacy reference (no stable identity), the whole reference and the user
  // key must also be equal. Each identity is written once.
  Status MergeRepairRuns(const std::vector<RepairCursorSpec>& inputs,
                         RepairReferenceRun* merged) {
    std::vector<std::unique_ptr<RepairRunReader>> readers;
    const auto later = [](const RepairRunReader* lhs,
                          const RepairRunReader* rhs) {
      return ReferenceIdentity(lhs->reference()) >
             ReferenceIdentity(rhs->reference());
    };
    std::priority_queue<RepairRunReader*, std::vector<RepairRunReader*>,
                        decltype(later)>
        heap(later);
    for (const RepairCursorSpec& input : inputs) {
      assert(input.run != nullptr);
      readers.push_back(std::make_unique<RepairRunReader>(input.run));
      bool valid = false;
      Status status = readers.back()->Next(&valid);
      if (!status.ok()) {
        return status;
      }
      if (valid) {
        heap.push(readers.back().get());
      }
    }
    std::vector<RepairRunReader*> owners;
    while (!heap.empty()) {
      const BlobReference reference = heap.top()->reference();
      const std::string key = heap.top()->key();
      owners.clear();
      while (!heap.empty() && ReferenceIdentity(heap.top()->reference()) ==
                                  ReferenceIdentity(reference)) {
        RepairRunReader* const reader = heap.top();
        heap.pop();
        if (reference.HasStableIdentity()
                ? !SameStableBlobMetadata(reference, reader->reference())
                : (!SameBlobReference(reference, reader->reference()) ||
                   reader->key() != key)) {
          return Status::Corruption(
              "Conflicting immutable metadata for one blob identity");
        }
        owners.push_back(reader);
      }
      Status status = AppendRepairRunRecord(merged, reference, key);
      if (!status.ok()) {
        return status;
      }
      for (RepairRunReader* reader : owners) {
        bool valid = false;
        status = reader->Next(&valid);
        if (!status.ok()) {
          return status;
        }
        if (valid) {
          if (ReferenceIdentity(reader->reference()) <
              ReferenceIdentity(reference)) {
            return Status::Corruption(
                "Repair physical references are not in (origin, original "
                "offset) order");
          }
          heap.push(reader);
        }
      }
    }
    return FinishRepairRun(merged);
  }

  Status PrepareGroupCursors(const std::vector<RepairRunInput>& inputs,
                             RepairGroupCursors* cursors) {
    std::vector<RepairCursorSpec> runs;
    std::vector<RepairCursorSpec> fallbacks;
    for (const RepairRunInput& input : inputs) {
      RepairCursorSpec spec{input, nullptr};
      Status status = PrepareRepairRun(input, &spec.run);
      if (!status.ok()) {
        return status;
      }
      (spec.run != nullptr ? runs : fallbacks).push_back(spec);
    }
    size_t fan_in = kMaxRepairCursorFanIn;
    TEST_SYNC_POINT_CALLBACK("Repairer::PrepareGroupCursors:FanIn", &fan_in);
    fan_in = std::max<size_t>(fan_in, 2);
    // All passes of a group append to one scratch file, so a group costs one
    // catalog intent; the file is deleted when the group is done.
    while (runs.size() > fan_in) {
      Status status;
      if (cursors->scratch == nullptr) {
        status = OpenRepairScratchFile(&cursors->scratch);
        if (!status.ok()) {
          return status;
        }
      }
      std::vector<RepairCursorSpec> next;
      for (size_t begin = 0; begin < runs.size(); begin += fan_in) {
        const size_t end = std::min(runs.size(), begin + fan_in);
        const std::vector<RepairCursorSpec> batch(runs.begin() + begin,
                                                  runs.begin() + end);
        cursors->merged_runs.push_back(std::make_unique<RepairReferenceRun>());
        RepairReferenceRun* const merged = cursors->merged_runs.back().get();
        merged->file = cursors->scratch.get();
        status = MergeRepairRuns(batch, merged);
        if (!status.ok()) {
          return status;
        }
        // A merged run holds references of many SSTs. Its `input` is only a
        // label: a merged spec always has `run` set, so no code opens this SST
        // through it.
        next.push_back({batch.front().input, merged});
      }
      TEST_SYNC_POINT("Repairer::PrepareGroupCursors:MergePass");
      runs = std::move(next);
    }
    cursors->specs = std::move(runs);
    cursors->specs.insert(cursors->specs.end(), fallbacks.begin(),
                          fallbacks.end());
    return Status::OK();
  }

  using RepairReferenceVisitor = std::function<Status(
      const BlobReference&, const std::vector<RepairReferenceCursor*>&)>;

  // One visit: merges the references of `inputs` in key order (origin,
  // original offset) and calls `visitor` once per key, with the cursors that
  // hold that key (`owners`). Returns Corruption when an input is missing,
  // when a cursor goes back in key order, or when two references with the
  // same key differ: an indirect reference must have the same stable
  // metadata, and a legacy reference must be the same reference. Inside one
  // run, a duplicate must also have the same user key.
  //
  // This is the same heap merge as MergeRepairRuns(), with two differences.
  // MergeRepairRuns() writes each identity (origin, original offset) once to a
  // new run, and it compares user keys across runs only for legacy
  // references. This function calls `visitor` once per identity, and it
  // compares the user keys of duplicates only inside one run, for all
  // references.
  Status VisitRepairReferences(const std::vector<RepairCursorSpec>& inputs,
                               const RepairReferenceVisitor& visitor) {
    std::vector<std::unique_ptr<RepairReferenceCursor>> cursors;
    const auto later = [](const RepairReferenceCursor* lhs,
                          const RepairReferenceCursor* rhs) {
      return ReferenceIdentity(lhs->reference()) >
             ReferenceIdentity(rhs->reference());
    };
    std::priority_queue<RepairReferenceCursor*,
                        std::vector<RepairReferenceCursor*>, decltype(later)>
        heap(later);
    for (const RepairCursorSpec& input : inputs) {
      std::unique_ptr<RepairReferenceCursor> cursor =
          std::make_unique<RepairReferenceCursor>();
      cursor->repairer = this;
      cursor->spec = input;
      Status status = cursor->Open();
      if (!status.ok()) {
        return status;
      }
      if (!cursor->valid) {
        return Status::Corruption(
            "Repair SST is missing a required reference source");
      }
      heap.push(cursor.get());
      cursors.push_back(std::move(cursor));
    }
    std::vector<RepairReferenceCursor*> owners;
    while (!heap.empty()) {
      const BlobReference reference = heap.top()->reference();
      owners.clear();
      while (!heap.empty() && ReferenceIdentity(heap.top()->reference()) ==
                                  ReferenceIdentity(reference)) {
        RepairReferenceCursor* const cursor = heap.top();
        heap.pop();
        const BlobReference& other = cursor->reference();
        if (reference.HasStableIdentity()
                ? !SameStableBlobMetadata(reference, other)
                : !SameBlobReference(reference, other)) {
          return Status::Corruption(
              "Conflicting immutable metadata for one blob identity");
        }
        owners.push_back(cursor);
      }
      Status status = visitor(reference, owners);
      if (!status.ok()) {
        return status;
      }
      for (RepairReferenceCursor* cursor : owners) {
        const BlobIdentity previous = ReferenceIdentity(cursor->reference());
        // A run also has the key; a duplicate within it must have the same
        // key (the visitor checked the first one).
        const std::string previous_key =
            cursor->run_reader != nullptr ? cursor->run_reader->key() : "";
        do {
          status = cursor->Next();
          if (!status.ok()) {
            return status;
          }
          if (cursor->valid &&
              ReferenceIdentity(cursor->reference()) == previous &&
              ((reference.HasStableIdentity()
                    ? !SameStableBlobMetadata(reference, cursor->reference())
                    : !SameBlobReference(reference, cursor->reference())) ||
               (cursor->run_reader != nullptr &&
                cursor->run_reader->key() != previous_key))) {
            return Status::Corruption("Conflicting duplicate repair reference");
          }
        } while (cursor->valid &&
                 ReferenceIdentity(cursor->reference()) == previous);
        if (cursor->valid) {
          if (ReferenceIdentity(cursor->reference()) < previous) {
            return Status::Corruption(
                "Repair physical references are not in (origin, original "
                "offset) order");
          }
          heap.push(cursor);
        }
      }
    }
    return Status::OK();
  }

  // How repair uses a normal blob file whose values it checks.
  enum class NormalBlobValidationMode {
    // The file stays in the new MANIFEST, so its classification (indirect
    // origin or not) must match the reference.
    kPublishedFile,

    // A donor: repair only copies value bytes from this file into a rebuilt
    // relocation file. The file may stay or be dropped.
    kPayloadDonor,
  };

  static TableCacheOpenOptions RepairTableOpenOptions() {
    TableCacheOpenOptions options;
    options.open_ephemeral_table_reader = true;
    options.avoid_shared_metadata_cache = true;
    options.isolate_repair_caches = true;
    options.skip_filters = true;
    return options;
  }

  struct ColumnFamilyBlobRepairPlan {
    std::map<uint64_t, BlobFileAddition> normal_files;
    std::map<uint64_t, BlobRelocationFileAddition> relocation_files;

    // route[old file name] = target relocation file.
    std::map<uint64_t, uint64_t> routes;

    // The files that the original MANIFEST marks lost and that a reference in
    // a kept SST still uses (directly or through a route). The new MANIFEST
    // keeps their file records and marks them lost again (lost files, not
    // files missing on disk). A lost file that no SST uses is dropped.
    std::set<uint64_t> lost_files;

    // The missing blob files that a dropped-file record of the original
    // MANIFEST names and that a kept SST still uses. The new MANIFEST records
    // them as dropped again (see "Dropped blob files" at the top of this
    // file).
    std::set<uint64_t> dropped_files;
  };

  std::string const dbname_;
  std::string db_session_id_;
  Env* const env_;
  const FileOptions file_options_;
  const DBOptions db_options_;
  const ImmutableDBOptions immutable_db_options_;
  const InternalKeyComparator icmp_;
  const ColumnFamilyOptions default_cf_opts_;
  const ImmutableOptions default_iopts_;  // table_cache_ holds reference
  const MutableCFOptions default_mopts_;
  const ColumnFamilyOptions unknown_cf_opts_;
  const bool create_unknown_cfs_;
  std::shared_ptr<Cache> raw_table_cache_;
  std::unique_ptr<TableCache> table_cache_;
  WriteBufferManager wb_;
  WriteController wc_;
  VersionSet vset_;
  std::unordered_map<std::string, ColumnFamilyOptions> cf_name_to_opts_;
  InstrumentedMutex mutex_;

  std::vector<std::string> manifests_;

  // Feature repair: input files to move to the lost/ directory only after
  // CURRENT names the new MANIFEST (converted WALs, and SSTs that repair
  // could not scan). Until then the original image must stay complete.
  std::vector<std::string> archive_after_publication_;
  std::vector<FileDescriptor> table_fds_;
  std::vector<uint64_t> logs_;
  std::vector<TableInfo> tables_;
  std::vector<BlobFilePath> blob_file_paths_;
  // The numbers of the blob files that DiscoverBlobFiles() found on disk,
  // valid or not. A blob file whose number is not in this set is missing.
  UnorderedSet<uint64_t> blob_file_numbers_on_disk_;

  // Column families that have blob indirection state: from the original
  // MANIFEST, or from an SST with exact metadata, a coarse frontier or a
  // classification with frontier 0. The new MANIFEST gives them the format
  // fence.
  std::set<uint32_t> feature_format_column_families_;
  UnorderedMap<uint32_t, ColumnFamilyBlobRepairPlan> blob_repair_plans_;

  // The lost_relocation_, durable_dropped_ and original_ members below hold
  // state that ReadOriginalManifest() reads from the original MANIFEST.
  // PrepareOriginalReplayCoverage() then sets proven_original_cf_floors_ from
  // it.

  // Relocation files that the original MANIFEST marks lost.
  std::map<uint64_t, RepairRelocationFile> lost_relocation_files_;
  // Per CF: the blob files that a dropped-file record of the original MANIFEST
  // names (RepairOriginalManifestHandler::dropped_blob_files).
  std::map<uint32_t, std::set<uint64_t>> durable_dropped_files_;
  // The file numbers of durable_dropped_files_ in all CFs. File numbers are
  // unique in a DB, so DiscoverBlobFiles() can skip these files without
  // knowing their CF.
  UnorderedSet<uint64_t> durable_dropped_numbers_;

  // Per column family: the routes of the original MANIFEST whose target is a
  // lost relocation file.
  std::map<uint32_t, std::map<uint64_t, uint64_t>> lost_relocation_routes_;
  uint64_t next_file_number_;
  uint64_t repair_manifest_file_number_ = 0;
  std::string repair_manifest_path_;
  bool feature_repair_ = false;
  // True when a catalog record was appended after the last catalog sync.
  bool repair_catalog_unsynced_ = false;
  // Scratch file of the reference runs; see RepairReferenceRun.
  std::unique_ptr<RepairScratchFile> reference_scratch_;
  RepairCatalog repair_catalog_;
  std::unique_ptr<log::Writer> repair_catalog_log_;
  std::string original_current_;
  bool original_current_absent_ = false;

  // The input record of the MANIFEST that the original CURRENT names (see the
  // table at RepairCatalogRecord).
  RepairCatalogRecord original_manifest_authority_;

  // The certified floor and the last sequence (see "WAL coverage" above
  // PrepareOriginalReplayCoverage()). certified_max_sequence_ starts at the
  // original MANIFEST's last sequence. Each converted WAL raises it.
  uint64_t certified_wal_floor_ = 0;
  SequenceNumber certified_max_sequence_ = 0;

  // True when some WALs are left for the normal open to replay: a WAL needs
  // transaction recovery, or two_write_queues or unordered_write is set.
  bool deferred_wal_ = false;

  // True when the original MANIFEST replayed with no corruption, so its log
  // floors can be used.
  bool original_manifest_complete_ = false;
  std::map<uint32_t, uint64_t> original_cf_floors_;
  std::map<uint32_t, std::string> original_cf_names_;
  std::map<uint32_t, std::map<uint64_t, RepairOriginalTable>>
      original_cf_tables_;

  // Per CF: the original floor (see "WAL coverage" above
  // PrepareOriginalReplayCoverage()).
  std::map<uint32_t, uint64_t> proven_original_cf_floors_;
  // Lock over the persistent DB state. Non-nullptr iff successfully
  // acquired.
  FileLock* db_lock_;
  bool closed_;

  Status RunLegacyRepair() {
    // No persisted evidence was found, so repair keeps the base behavior: it
    // moves the MANIFESTs to lost/ first, and it publishes and archives files
    // as it goes, with no catalog.
    for (const std::string& manifest : manifests_) {
      ArchiveFile(dbname_ + "/" + manifest);
    }
    DBImpl db(db_options_, dbname_);
    Status status = db.NewDB(nullptr);
    if (status.ok()) {
      status =
          vset_.Recover({{kDefaultColumnFamilyName, default_cf_opts_}}, false);
    }
    if (status.ok()) {
      ExtractMetaData(/*tables_built_from_wals=*/false).PermitUncheckedError();
      table_fds_.clear();
      ConvertLogFilesToTables().PermitUncheckedError();
      ExtractMetaData(/*tables_built_from_wals=*/true).PermitUncheckedError();
      status = AddTables();
    }
    return status;
  }

  Status DetectBlobIndirection() {
    // Detection reads every MANIFEST file, including ones CURRENT does not
    // name: a blob indirection record (IsBlobIndirectionManifestEvidence()) in
    // any of them turns on feature repair. The file numbers they name are
    // tracked, so the files that repair creates never reuse such a number.
    // Accepted losses, routes and relocation files come only from the MANIFEST
    // that CURRENT names (ReadOriginalManifest()). They are never combined with
    // records of other MANIFESTs.
    for (const std::string& manifest : manifests_) {
      std::unique_ptr<SequentialFileReader> file;
      Status status = SequentialFileReader::Create(
          env_->GetFileSystem(), dbname_ + "/" + manifest, file_options_, &file,
          nullptr, nullptr);
      if (!status.ok()) {
        continue;
      }
      RepairLossLogReporter reporter(&status);
      log::Reader reader(nullptr, std::move(file), &reporter, true, 0);
      Slice record;
      std::string scratch;
      while (reader.ReadRecord(&record, &scratch)) {
        VersionEdit edit;
        Status decoded = edit.DecodeFrom(record);
        if (!decoded.ok()) {
          continue;
        }
        feature_repair_ |= IsBlobIndirectionManifestEvidence(edit);
        if (edit.HasNextFile()) {
          status = TrackDiscoveredFileNumber(edit.GetNextFile());
          if (!status.ok()) {
            return status;
          }
        }
        for (const BlobRelocationRoute& route :
             edit.GetBlobRelocationRoutes()) {
          status = TrackDiscoveredFileNumber(route.GetOldFileNumber());
          if (status.ok()) {
            status = TrackDiscoveredFileNumber(route.GetTargetFileNumber());
          }
          if (!status.ok()) {
            return status;
          }
        }
      }
      // A read error here is ignored: detection uses these reads only to find
      // evidence and file numbers. ReadOriginalManifest() reads the MANIFEST
      // that CURRENT names again.
      status.PermitUncheckedError();
    }
    for (const FileDescriptor& fd : table_fds_) {
      FileMetaData meta;
      meta.fd = fd;
      uint64_t size = 0;
      Status size_status = env_->GetFileSize(
          TableFileName(db_options_.db_paths, fd.GetNumber(), fd.GetPathId()),
          &size);
      if (!size_status.ok()) {
        continue;
      }
      meta.fd = FileDescriptor(fd.GetNumber(), fd.GetPathId(), size);
      std::shared_ptr<const TableProperties> properties;
      ReadOptions read_options;
      read_options.fill_cache = false;
      Status status = table_cache_->GetTableProperties(
          file_options_, read_options, icmp_, meta, &properties, default_mopts_,
          false, RepairTableOpenOptions());
      TEST_SYNC_POINT_CALLBACK(
          "Repairer::DetectBlobIndirection:TableProperties", &status);
      if (!status.ok()) {
        continue;
      }
      ExtractBlobReferenceDescriptorFromTableProperties(*properties, &meta);
      status = ExtractBlobReferenceClassificationFromTableProperties(
          *properties, &meta);
      if (!status.ok()) {
        if (feature_repair_) {
          return status;
        }
        continue;
      }
      // Runtime options and failed reads are not evidence.
      const bool feature = IsBlobIndirectionTableEvidence(meta);
      feature_repair_ |= feature;
      if (feature) {
        const ColumnFamilyOptions* options =
            GetColumnFamilyOptions(properties->column_family_name);
        if (options != nullptr) {
          status = CheckBlobIndirectionCompactionStyle(
              options->compaction_style, true);
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
    for (const BlobFilePath& file : blob_file_paths_) {
      std::unique_ptr<RandomAccessFileReader> reader;
      Status status = RandomAccessFileReader::Create(
          env_->GetFileSystem(), file.path, file_options_, &reader, nullptr);
      if (!status.ok()) {
        continue;
      }
      char scratch[BlobLogHeader::kSize];
      Slice prefix;
      status = reader->Read(0, sizeof(scratch), IOOptions(), &prefix, scratch,
                            nullptr);
      if (!status.ok()) {
        continue;
      }
      feature_repair_ |= IsBlobIndirectionBlobFileEvidence(prefix);
    }
    return Status::OK();
  }

  // Appends one record without syncing it. A record must be durable before the
  // action it allows, so callers sync (SyncRepairCatalog) only right before
  // such an action: the catalog rename, an output file creation and the
  // candidate. One sync makes every earlier append durable. Inputs, seals,
  // policies, coverage, unused marks and archives allow no action before the
  // next sync, so a crash that loses them only makes a retry redo work.
  Status AppendRepairCatalog(RepairCatalogRecord record) {
    if (repair_catalog_.last_ordinal == std::numeric_limits<uint64_t>::max()) {
      return Status::Corruption("Repair catalog ordinal overflow");
    }
    record.ordinal = repair_catalog_.last_ordinal + 1;
    std::string encoded;
    record.EncodeTo(&encoded);
    if (encoded.size() > log::kBlockSize) {
      return Status::NotSupported(
          "Repair catalog record exceeds log block bound");
    }
    Status status = repair_catalog_log_->AddRecord(WriteOptions(), encoded);
    if (status.ok()) {
      repair_catalog_unsynced_ = true;
      repair_catalog_.last_ordinal = record.ordinal;
      if (record.type == RepairCatalogRecordType::kCoverage) {
        repair_catalog_.coverage_floor = record.number;
        repair_catalog_.coverage_max_sequence = record.size;
      }
    }
    return status;
  }

  Status SyncRepairCatalog() {
    if (!repair_catalog_unsynced_) {
      return Status::OK();
    }
    // The marker tells a reader that every record before it is durable, so
    // damage after the last marker is a torn unsynced tail.
    RepairCatalogRecord marker;
    marker.type = RepairCatalogRecordType::kSyncMarker;
    Status status = AppendRepairCatalog(marker);
    if (!status.ok()) {
      return status;
    }
    bool previously_failed = false;
    status = repair_catalog_log_->file()->Sync(
        IOOptions(), immutable_db_options_.use_fsync, &previously_failed);
    TEST_SYNC_POINT_CALLBACK("Repairer::SyncRepairCatalog", &status);
    if (status.ok()) {
      repair_catalog_unsynced_ = false;
    }
    return status;
  }

  Status StartRepairCatalog() {
    const std::string path = RepairInProgressFileName(dbname_);
    Status status = env_->FileExists(path);
    if (!status.IsNotFound() && !status.IsPathNotFound()) {
      return status.ok()
                 ? Status::Corruption("Repair catalog ownership conflict", path)
                 : status;
    }
    // Rename the file to REPAIR-IN-PROGRESS only after its header and input
    // records are durable. A crash before the rename leaves an ordinary temp
    // file that open purges.
    // next_file_number_ is above every discovered number, including temps.
    const std::string temporary_path = TempFileName(dbname_, next_file_number_);
    std::unique_ptr<FSWritableFile> file;
    status = NewWritableFile(env_->GetFileSystem().get(), temporary_path, &file,
                             file_options_);
    if (!status.ok()) {
      return status;
    }
    repair_catalog_log_ =
        std::make_unique<log::Writer>(std::make_unique<WritableFileWriter>(
                                          std::move(file), path, file_options_),
                                      0, false);
    repair_catalog_.header.path = original_current_;
    repair_catalog_.header.identity = db_session_id_;
    repair_catalog_.header.column_family =
        original_current_absent_ ? RepairCatalogRecord::kOriginalCurrentAbsent
                                 : 0;
    repair_catalog_.header.number = next_file_number_;
    repair_catalog_.header.source_wal =
        (db_options_.two_write_queues
             ? RepairCatalogRecord::kHeaderTwoWriteQueues
             : 0) |
        (db_options_.unordered_write
             ? RepairCatalogRecord::kHeaderUnorderedWrite
             : 0) |
        (create_unknown_cfs_ ? RepairCatalogRecord::kHeaderCreateUnknownCfs
                             : 0);
    status = AppendRepairCatalog(repair_catalog_.header);
    if (!status.ok()) {
      return status;
    }
    if (!original_manifest_authority_.path.empty()) {
      status = AppendRepairCatalog(original_manifest_authority_);
      if (!status.ok()) {
        return status;
      }
      repair_catalog_.inputs.emplace(original_manifest_authority_.path,
                                     original_manifest_authority_);
    }
    // Record every original file found so far (SST, blob file, MANIFEST,
    // WAL) as an input before repair creates any output. A retry then knows
    // that these files belong to the original image and must not be deleted.
    // The input records go into the temp file, so the one sync before the
    // rename makes all of them durable. A later
    // FindFeatureColumnFamilyBlobFiles() call adds the blob files it finds.
    std::vector<std::string> inputs;
    for (const FileDescriptor& fd : table_fds_) {
      inputs.push_back(
          TableFileName(db_options_.db_paths, fd.GetNumber(), fd.GetPathId()));
    }
    for (const BlobFilePath& blob : blob_file_paths_) {
      inputs.push_back(blob.path);
    }
    for (const std::string& manifest : manifests_) {
      inputs.push_back(dbname_ + "/" + manifest);
    }
    for (uint64_t wal : logs_) {
      inputs.push_back(LogFileName(immutable_db_options_.GetWalDir(), wal));
    }
    std::sort(inputs.begin(), inputs.end());
    inputs.erase(std::unique(inputs.begin(), inputs.end()), inputs.end());
    for (const std::string& input : inputs) {
      if (repair_catalog_.inputs.count(input) != 0) {
        continue;
      }
      RepairCatalogRecord record;
      record.type = RepairCatalogRecordType::kInput;
      record.path = input;
      const size_t slash = input.find_last_of("/\\");
      FileType type;
      if (ParseFileName(
              input.substr(slash == std::string::npos ? 0 : slash + 1),
              &record.number, &type) &&
          type == kWalFile) {
        record.source_wal = record.number;
      }
      status = env_->GetFileSize(input, &record.size);
      if (!status.ok()) {
        return status;
      }
      status = AppendRepairCatalog(record);
      if (!status.ok()) {
        return status;
      }
      repair_catalog_.inputs.emplace(input, std::move(record));
    }
    status = SyncRepairCatalog();
    if (!status.ok()) {
      return status;
    }
    TEST_SYNC_POINT_CALLBACK("Repairer::StartRepairCatalog:BeforeRename",
                             &status);
    if (!status.ok()) {
      return status;
    }
    status = env_->RenameFile(temporary_path, path);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<FSDirectory> directory;
    status = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(),
                                                 &directory, nullptr);
    if (status.ok()) {
      status = directory->FsyncWithDirOptions(IOOptions(), nullptr,
                                              DirFsyncOptions(path));
    }
    return status;
  }

  // `sync` is false only when the caller registers another intent for the
  // same output right after this one; that intent's sync covers both.
  Status RegisterRepairOutput(const std::string& path, uint64_t number,
                              uint32_t cf, uint64_t wal, bool sync = true) {
    if (!feature_repair_) {
      return Status::OK();
    }
    Status status = env_->FileExists(path);
    if (!status.IsNotFound() && !status.IsPathNotFound()) {
      return status.ok()
                 ? Status::Corruption("Repair output already exists", path)
                 : status;
    }
    RepairCatalogRecord record;
    record.type = RepairCatalogRecordType::kIntent;
    record.path = path;
    record.number = number;
    record.column_family = cf;
    record.source_wal = wal;
    status = AppendRepairCatalog(record);
    if (status.ok()) {
      repair_catalog_.outputs.emplace(path, std::move(record));
    }
    if (status.ok() && sync) {
      status = SyncRepairCatalog();
    }
    return status;
  }

  Status SealRepairOutput(const std::string& path) {
    if (!feature_repair_) {
      return Status::OK();
    }
    const std::map<std::string, RepairCatalogRecord>::iterator intent =
        repair_catalog_.outputs.find(path);
    if (intent == repair_catalog_.outputs.end()) {
      return Status::Corruption(
          "Repair output has no durable allocation intent", path);
    }
    RepairCatalogRecord record = intent->second;
    record.type = RepairCatalogRecordType::kSeal;
    Status status = env_->GetFileSize(path, &record.size);
    if (status.ok()) {
      status = GetRepairFileChecksum(env_, path, record.size, &record.checksum);
    }
    if (status.ok()) {
      status = AppendRepairCatalog(record);
    }
    if (status.ok()) {
      intent->second = std::move(record);
    }
    return status;
  }

  Status RecordRepairArchive(const std::string& path) {
    if (repair_catalog_.archives.count(path) != 0) {
      return Status::OK();
    }
    RepairCatalogRecord record;
    record.type = RepairCatalogRecordType::kArchive;
    record.path = path;
    // An earlier repair, or a DB created again later, can have a file with the
    // same number in lost/. The archive name starts with this attempt's
    // session ID, so it never overwrites those bytes, and a retry can finish
    // the move again safely.
    record.identity = repair_catalog_.header.identity;
    Status status = env_->GetFileSize(path, &record.size);
    if (status.ok()) {
      status = GetRepairFileChecksum(env_, path, record.size, &record.checksum);
    }
    if (status.ok()) {
      status = AppendRepairCatalog(record);
    }
    if (status.ok()) {
      repair_catalog_.archives.emplace(path, std::move(record));
    }
    return status;
  }

  // WAL coverage
  //
  // Background: after repair, the next open replays every WAL whose number is
  // at least the log number of a CF. A WAL record that an SST already holds
  // must not be replayed again, and a WAL record of a lost value must not
  // bring the value back. So repair gives each CF a safe log number.
  //
  // Terms:
  // - Certified floor (certified_wal_floor_): the WAL number after the last
  //   WAL that ConvertLogFilesToTables() converted. Every WAL below it is
  //   converted.
  // - Original floor (proven_original_cf_floors_): the CF's log number in the
  //   original MANIFEST. It is used only when the original MANIFEST replayed
  //   with no corruption and repair found every SST that it lists for the CF,
  //   with the same size and, when the MANIFEST records one, the same unique
  //   ID. Otherwise it is 0.
  //
  // Each CF's log number in the new MANIFEST is max(certified floor, original
  // floor) (see AddTables()). Its last sequence is the larger of
  // certified_max_sequence_ (the original MANIFEST's last sequence, raised by
  // each converted WAL) and the largest sequence number in any SST.
  //
  // Catalog records (see the table at RepairCatalogRecord in file/filename.h):
  // 1. This function computes the original floor and writes one kPolicy
  //    record per CF.
  // 2. ConvertLogFilesToTables() writes one kCoverage record after each WAL:
  //    the certified floor and the last sequence so far.
  // 3. PublishManifest() writes the kCandidate record with the certified
  //    floor. ResolveRepairCatalog() checks that each CF of the candidate
  //    MANIFEST has a log number of at least max(certified floor, original
  //    floor), and a last sequence of at least the kCoverage sequence.
  //
  // Corruption: when repair cannot prove the original floor of a CF, the CF
  // has a lost file in the original MANIFEST, and WAL files exist, this
  // function returns Corruption. Replaying those WALs could bring lost values
  // back.
  //
  // This guard covers only lost relocation files, not dropped blob files (see
  // "Dropped blob files" at the top of this file). A WAL that is converted
  // again can bring back a value whose blob file has a dropped-file record.
  // Base RepairDB does the same, and the WAL record is the original write of
  // the value, so repair keeps the base behavior there.
  Status PrepareOriginalReplayCoverage() {
    std::map<std::pair<uint32_t, uint64_t>, const TableInfo*> selected;
    for (const TableInfo& table : tables_) {
      if (!selected
               .emplace(std::make_pair(table.column_family_id,
                                       table.meta.fd.GetNumber()),
                        &table)
               .second) {
        return Status::Corruption("Duplicate original repair SST identity");
      }
    }
    for (const auto& name : original_cf_names_) {
      if (vset_.GetColumnFamilySet()->GetColumnFamily(name.first) == nullptr &&
          GetColumnFamilyOptions(name.second) != nullptr) {
        Status status = AddColumnFamily(name.second, name.first);
        if (!status.ok()) {
          return status;
        }
      }
    }
    for (ColumnFamilyData* cfd : *vset_.GetColumnFamilySet()) {
      bool complete = original_manifest_complete_;
      const std::map<uint64_t, RepairOriginalTable>& expected =
          original_cf_tables_[cfd->GetID()];
      for (const auto& file : expected) {
        const std::map<std::pair<uint32_t, uint64_t>,
                       const TableInfo*>::const_iterator actual =
            selected.find({cfd->GetID(), file.first});
        if (actual == selected.end() ||
            actual->second->meta.fd.GetFileSize() != file.second.file_size ||
            (file.second.unique_id != kNullUniqueId64x2 &&
             actual->second->meta.unique_id != file.second.unique_id)) {
          complete = false;
          break;
        }
      }
      const uint64_t floor = complete ? original_cf_floors_[cfd->GetID()] : 0;
      proven_original_cf_floors_[cfd->GetID()] = floor;
      cfd->SetLogNumber(floor);
      RepairCatalogRecord policy;
      policy.type = RepairCatalogRecordType::kPolicy;
      policy.column_family = cfd->GetID();
      policy.path = cfd->GetName();
      policy.identity = cfd->user_comparator()->Name();
      policy.number = cfd->user_comparator()->timestamp_size();
      policy.source_wal = floor;
      Status policy_status = AppendRepairCatalog(policy);
      if (!policy_status.ok()) {
        return policy_status;
      }
      repair_catalog_.policies.emplace(cfd->GetID(), std::move(policy));
      if (floor != 0) {
        Status status = TrackDiscoveredFileNumber(floor);
        if (!status.ok()) {
          return status;
        }
      }
      if (!complete && !logs_.empty() && HasLostRelocationFile(cfd->GetID())) {
        return Status::Corruption(
            "Feature repair cannot certify WAL coverage of a lost relocation "
            "file");
      }
    }
    return Status::OK();
  }

  // Replays the MANIFEST that the original CURRENT names. It keeps the loss
  // records and the routes that point to a lost relocation file. It also keeps
  // the dropped-file records, the per-CF log floors, SST lists and CF names,
  // the feature CFs and the last sequence (see RepairOriginalManifestHandler).
  Status ReadOriginalManifest() {
    std::string current;
    Status status = ReadFileToString(env_, CurrentFileName(dbname_), &current);
    if (status.IsNotFound() || status.IsPathNotFound()) {
      original_current_absent_ = true;
      return Status::OK();  // No CURRENT: there is no original MANIFEST.
    }
    if (!status.ok()) {
      return status;
    }
    original_current_ = current;
    uint64_t manifest_number = 0;
    FileType type;
    if (current.empty() || current.back() != '\n') {
      return Status::OK();
    }
    current.pop_back();
    if (!ParseFileName(current, &manifest_number, &type) ||
        type != kDescriptorFile || manifest_number == 0 ||
        DescriptorFileName("", manifest_number) != "/" + current) {
      return Status::OK();
    }
    const std::string path = DescriptorFileName(dbname_, manifest_number);
    original_manifest_authority_.type = RepairCatalogRecordType::kInput;
    original_manifest_authority_.path = path;
    original_manifest_authority_.number = manifest_number;
    original_manifest_authority_.identity =
        RepairCatalogRecord::kOriginalManifest;
    status = env_->GetFileSize(path, &original_manifest_authority_.size);
    if (status.IsNotFound() || status.IsPathNotFound()) {
      original_manifest_authority_.identity =
          RepairCatalogRecord::kOriginalManifestAbsent;
      return Status::OK();
    }
    if (!status.ok()) {
      return status;
    }
    status =
        GetRepairFileChecksum(env_, path, original_manifest_authority_.size,
                              &original_manifest_authority_.checksum);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<FSSequentialFile> file;
    status = immutable_db_options_.fs->NewSequentialFile(
        path, immutable_db_options_.fs->OptimizeForManifestRead(file_options_),
        &file, nullptr);
    if (status.IsNotFound() || status.IsPathNotFound()) {
      return Status::OK();
    }
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<SequentialFileReader> file_reader =
        std::make_unique<SequentialFileReader>(std::move(file), path,
                                               /*io_tracer=*/nullptr);
    Status log_status;
    RepairLossLogReporter reporter(&log_status);
    log::Reader reader(nullptr, std::move(file_reader), &reporter,
                       /*checksum=*/true, /*log_number=*/0);
    const ReadOptions read_options;
    RepairOriginalManifestHandler handler(read_options);
    handler.Iterate(reader, &log_status);
    handler.DiscardIncompleteAtomicGroup();
    if (!handler.status().ok() && !handler.status().IsCorruption()) {
      return handler.status();
    }
    if (!log_status.ok() && !log_status.IsCorruption()) {
      return log_status;
    }
    original_manifest_complete_ = handler.status().ok() && log_status.ok();
    original_cf_floors_ = handler.cf_floors;
    original_cf_tables_ = handler.cf_tables;
    original_cf_names_ = handler.cf_names;
    feature_format_column_families_.insert(handler.feature_cfs.begin(),
                                           handler.feature_cfs.end());
    for (uint32_t cf : handler.feature_cfs) {
      const std::map<uint32_t, std::string>::const_iterator name =
          original_cf_names_.find(cf);
      if (name != original_cf_names_.end()) {
        const ColumnFamilyOptions* options =
            GetColumnFamilyOptions(name->second);
        if (options != nullptr) {
          status = CheckBlobIndirectionCompactionStyle(
              options->compaction_style, true);
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
    certified_max_sequence_ = handler.last_sequence;
    // Keep the losses of every fully decoded edit, also when the MANIFEST has
    // a damaged tail. The handler already dropped an incomplete atomic group.
    for (uint64_t number : handler.lost_relocation_files) {
      const auto file_info = handler.files.find(number);
      if (file_info != handler.files.end()) {
        lost_relocation_files_.emplace(number, file_info->second);
      }
    }
    for (const auto& cf : handler.routes) {
      for (const auto& route : cf.second) {
        if (lost_relocation_files_.count(route.second) != 0) {
          lost_relocation_routes_[cf.first].emplace(route);
        }
      }
    }
    durable_dropped_files_ = std::move(handler.dropped_blob_files);
    for (const auto& cf : durable_dropped_files_) {
      durable_dropped_numbers_.insert(cf.second.begin(), cf.second.end());
    }
    return Status::OK();
  }

  // True if a dropped-file record of the original MANIFEST names blob file
  // `number` in column family `cf_id`.
  bool IsDurablyDropped(uint32_t cf_id, uint64_t number) const {
    const auto cf = durable_dropped_files_.find(cf_id);
    return cf != durable_dropped_files_.end() && cf->second.count(number) != 0;
  }

  // Returns the lost physical file that `reference` reads from in column
  // family `cf_id`: the file of its generation number, or the target of its
  // route. Returns kInvalidBlobFileNumber if that file is not lost or the
  // reference is legacy. See "Lost files" at the top of this file.
  uint64_t GetLostRelocationFile(uint32_t cf_id,
                                 const BlobReference& reference) const {
    if (!reference.HasStableIdentity()) {
      return kInvalidBlobFileNumber;
    }
    uint64_t physical = reference.generation_file_number;
    const auto routes = lost_relocation_routes_.find(cf_id);
    if (routes != lost_relocation_routes_.end()) {
      const auto route = routes->second.find(reference.generation_file_number);
      if (route != routes->second.end()) {
        physical = route->second;
      }
    }
    const auto file = lost_relocation_files_.find(physical);
    if (file != lost_relocation_files_.end()) {
      return file->second.column_family_id == cf_id ? physical
                                                    : kInvalidBlobFileNumber;
    }
    return kInvalidBlobFileNumber;
  }

  // True if the original MANIFEST marks a relocation file of column family
  // `cf_id` lost. Only relocation files can be lost.
  bool HasLostRelocationFile(uint32_t cf_id) const {
    for (const auto& file : lost_relocation_files_) {
      if (file.second.column_family_id == cf_id) {
        return true;
      }
    }
    return false;
  }

  Status PublishManifest() {
    Status status = vset_.SyncAndCloseRepairManifest();
    if (!status.ok()) {
      return status;
    }
    const std::string final_path =
        DescriptorFileName(dbname_, repair_manifest_file_number_);
    TEST_SYNC_POINT("Repairer::PublishManifest:BeforeRename");
    status = env_->RenameFile(repair_manifest_path_, final_path);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<FSDirectory> directory;
    status = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(),
                                                 &directory, /*dbg=*/nullptr);
    if (!status.ok()) {
      return status;
    }
    status = directory->FsyncWithDirOptions(IOOptions(), /*dbg=*/nullptr,
                                            DirFsyncOptions(final_path));
    TEST_SYNC_POINT_CALLBACK("Repairer::PublishManifest:AfterDirectorySync",
                             &status);
    if (!status.ok()) {
      return status;
    }
    const WriteOptions write_options(Env::IOActivity::kDBOpen);
    status = SealRepairOutput(final_path);
    if (!status.ok()) {
      return status;
    }
    // A sealed output is not always in the new MANIFEST. Mark the unused
    // ones. When the attempt commits, the cleanup then deletes them and keeps
    // the sealed outputs that the new MANIFEST uses.
    UnorderedSet<std::string> table_paths;
    table_paths.reserve(tables_.size());
    for (const TableInfo& table : tables_) {
      table_paths.insert(TableFileName(db_options_.db_paths,
                                       table.meta.fd.GetNumber(),
                                       table.meta.fd.GetPathId()));
    }
    UnorderedSet<uint64_t> blob_numbers;
    for (const auto& plan : blob_repair_plans_) {
      for (const auto& file : plan.second.normal_files) {
        blob_numbers.insert(file.first);
      }
      for (const auto& file : plan.second.relocation_files) {
        blob_numbers.insert(file.first);
      }
    }
    for (auto& output : repair_catalog_.outputs) {
      if (output.second.type != RepairCatalogRecordType::kSeal ||
          output.first == final_path) {
        continue;
      }
      const bool used = table_paths.count(output.first) != 0 ||
                        blob_numbers.count(output.second.number) != 0;
      if (!used) {
        output.second.identity = RepairCatalogRecord::kUnusedOutput;
        status = AppendRepairCatalog(output.second);
        if (!status.ok()) {
          return status;
        }
      }
    }
    for (const std::string& manifest : manifests_) {
      status = RecordRepairArchive(dbname_ + "/" + manifest);
      if (!status.ok()) {
        return status;
      }
    }
    for (const std::string& path : archive_after_publication_) {
      if (repair_catalog_.outputs.count(path) == 0) {
        status = RecordRepairArchive(path);
        if (!status.ok()) {
          return status;
        }
      }
    }
    repair_catalog_.candidate = repair_catalog_.outputs.at(final_path);
    repair_catalog_.candidate.type = RepairCatalogRecordType::kCandidate;
    repair_catalog_.candidate.source_wal = certified_wal_floor_;
    status = AppendRepairCatalog(repair_catalog_.candidate);
    if (status.ok()) {
      status = SyncRepairCatalog();
    }
    if (!status.ok()) {
      return status;
    }
    status = repair_catalog_log_->Close(WriteOptions());
    repair_catalog_log_.reset();
    if (!status.ok()) {
      return status;
    }
    status = SetCurrentFile(write_options, env_->GetFileSystem().get(), dbname_,
                            repair_manifest_file_number_,
                            immutable_db_options_.metadata_write_temperature,
                            directory.get());
    if (status.ok()) {
      bool committed = false;
      status = ResolveRepairCatalog(env_, dbname_, repair_catalog_,
                                    /*read_only=*/false,
                                    /*repair_retry=*/false, &committed,
                                    /*seals_verified=*/true);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Repair image committed; cleanup incomplete: %s",
                       status.ToString().c_str());
      }
    }
    return status;
  }

  Status TrackDiscoveredFileNumber(uint64_t file_number) {
    if (file_number == std::numeric_limits<uint64_t>::max()) {
      return Status::Corruption("Invalid maximum repair file number");
    }
    next_file_number_ = std::max(next_file_number_, file_number + 1);
    return Status::OK();
  }

  Status AllocateRepairFileNumber(uint64_t* file_number) {
    if (file_number == nullptr) {
      return Status::InvalidArgument("Null repair file number output");
    }
    if (next_file_number_ == 0 ||
        next_file_number_ >= std::numeric_limits<uint64_t>::max() - 1) {
      return Status::Corruption("Repair file number space is exhausted");
    }
    *file_number = next_file_number_++;
    return Status::OK();
  }

  Status FindFiles() {
    std::vector<std::string> filenames;
    bool found_file = false;
    std::vector<std::string> to_search_paths;

    to_search_paths.reserve(db_options_.db_paths.size() + 1);
    for (size_t path_id = 0; path_id < db_options_.db_paths.size(); path_id++) {
      to_search_paths.push_back(db_options_.db_paths[path_id].path);
    }

    // search wal_dir if user uses a customize wal_dir
    bool same = immutable_db_options_.IsWalDirSameAsDBPath(dbname_);
    if (!same) {
      to_search_paths.push_back(immutable_db_options_.wal_dir);
    }

    for (size_t path_id = 0; path_id < to_search_paths.size(); path_id++) {
      ROCKS_LOG_INFO(db_options_.info_log, "Searching path %s\n",
                     to_search_paths[path_id].c_str());
      Status status = env_->GetChildren(to_search_paths[path_id], &filenames);
      if (!status.ok()) {
        return status;
      }
      if (!filenames.empty()) {
        found_file = true;
      }

      uint64_t number;
      FileType type;
      for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
          status = TrackDiscoveredFileNumber(number);
          if (!status.ok()) {
            return status;
          }
          if (type == kDescriptorFile) {
            manifests_.push_back(filenames[i]);
          } else {
            if (type == kWalFile) {
              logs_.push_back(number);
            } else if (type == kTableFile) {
              table_fds_.emplace_back(number, static_cast<uint32_t>(path_id),
                                      0);
            } else if (type == kBlobFile) {
              blob_file_paths_.push_back(BlobFilePath{
                  number, BlobFileName(to_search_paths[path_id], number)});
            } else {
              // Ignore other files
            }
          }
        }
      }
    }
    if (!found_file) {
      return Status::Corruption(dbname_, "repair found no files");
    }
    return Status::OK();
  }

  // Finds the blob files in the cf_paths of the default CF, of the configured
  // CFs (and of unknown CFs with create_unknown_cfs), and of every CF that is
  // a feature CF or has enable_blob_indirection on. It tracks their file
  // numbers, so new files never reuse them. With `detection_only`, a
  // directory that cannot be listed is skipped; otherwise it fails the
  // repair. When the catalog has started, each new file also gets an input
  // record.
  Status FindFeatureColumnFamilyBlobFiles(bool detection_only) {
    std::set<std::string> known_paths;
    for (const BlobFilePath& file : blob_file_paths_) {
      known_paths.insert(file.path);
    }
    std::set<std::string> feature_directories;
    for (const DbPath& path : default_cf_opts_.cf_paths) {
      feature_directories.insert(path.path);
    }
    if (create_unknown_cfs_) {
      for (const DbPath& path : unknown_cf_opts_.cf_paths) {
        feature_directories.insert(path.path);
      }
    }
    for (const auto& configured_cf : cf_name_to_opts_) {
      for (const DbPath& path : configured_cf.second.cf_paths) {
        feature_directories.insert(path.path);
      }
    }
    for (ColumnFamilyData* cfd : *vset_.GetColumnFamilySet()) {
      if (feature_format_column_families_.count(cfd->GetID()) == 0 &&
          !cfd->GetLatestMutableCFOptions().enable_blob_indirection) {
        continue;
      }
      if (cfd->ioptions().cf_paths.empty()) {
        return Status::Corruption(
            "Blob-indirection column family has no canonical path");
      }
      for (const DbPath& path : cfd->ioptions().cf_paths) {
        feature_directories.insert(path.path);
      }
    }

    for (const std::string& directory : feature_directories) {
      std::vector<std::string> filenames;
      Status status = env_->GetChildren(directory, &filenames);
      if (!status.ok()) {
        if (detection_only) {
          continue;
        }
        return status;
      }
      for (const std::string& filename : filenames) {
        uint64_t number = 0;
        FileType type = kTempFile;
        if (!ParseFileName(filename, &number, &type) || type != kBlobFile) {
          continue;
        }
        status = TrackDiscoveredFileNumber(number);
        if (!status.ok()) {
          return status;
        }
        TEST_SYNC_POINT_CALLBACK(
            "Repairer::FindFeatureColumnFamilyBlobFiles:BlobFileNumber",
            &number);
        const std::string path = BlobFileName(directory, number);
        if (known_paths.insert(path).second) {
          if (repair_catalog_log_ != nullptr) {
            RepairCatalogRecord input;
            input.type = RepairCatalogRecordType::kInput;
            input.path = path;
            input.number = number;
            status = env_->GetFileSize(path, &input.size);
            if (status.ok()) {
              status = AppendRepairCatalog(input);
            }
            if (!status.ok()) {
              return status;
            }
            repair_catalog_.inputs.emplace(path, std::move(input));
          }
          blob_file_paths_.push_back(BlobFilePath{number, path});
        }
      }
    }
    return Status::OK();
  }

  Status ConvertLogFilesToTables() {
    const auto& wal_dir = immutable_db_options_.GetWalDir();
    if (feature_repair_) {
      std::sort(logs_.begin(), logs_.end());
      logs_.erase(std::unique(logs_.begin(), logs_.end()), logs_.end());
      // With these options, WAL replay needs the transaction and sequence
      // state that only the normal open has. Leave all WALs for the normal
      // open.
      if (db_options_.two_write_queues || db_options_.unordered_write) {
        deferred_wal_ = !logs_.empty();
        return Status::OK();
      }
    }
    for (size_t i = 0; i < logs_.size(); i++) {
      // we should use LogFileName(wal_dir, logs_[i]) here. user might uses
      // wal_dir option.
      std::string logname = LogFileName(wal_dir, logs_[i]);
      Status status = ConvertLogToTable(wal_dir, logs_[i]);
      if (feature_repair_) {
        if (!status.ok()) {
          return status;
        }
        if (deferred_wal_) {
          break;
        }
        if (logs_[i] == std::numeric_limits<uint64_t>::max()) {
          return Status::Corruption("Repair WAL floor overflow");
        }
        certified_wal_floor_ = logs_[i] + 1;
        RepairCatalogRecord coverage;
        coverage.type = RepairCatalogRecordType::kCoverage;
        coverage.number = certified_wal_floor_;
        coverage.size = certified_max_sequence_;
        status = AppendRepairCatalog(coverage);
        if (!status.ok()) {
          return status;
        }
        archive_after_publication_.push_back(logname);
        continue;
      }
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Log #%" PRIu64 ": ignoring conversion error: %s",
                       logs_[i], status.ToString().c_str());
      }
      ArchiveFile(logname);
    }
    return Status::OK();
  }

  Status ConvertLogToTable(const std::string& wal_dir, uint64_t log) {
    struct LogReporter : public log::Reader::Reporter {
      Env* env;
      std::shared_ptr<Logger> info_log;
      uint64_t lognum;
      Status read_error;
      ~LogReporter() override { read_error.PermitUncheckedError(); }
      void Corruption(size_t bytes, const Status& s,
                      uint64_t log_number = kMaxSequenceNumber) override {
        // We print error messages for corruption, but continue repairing.
        ROCKS_LOG_ERROR(info_log, "Log #%" PRIu64 ": dropping %d bytes; %s",
                        log_number == kMaxSequenceNumber ? lognum : log_number,
                        static_cast<int>(bytes), s.ToString().c_str());
        if (!s.IsCorruption()) {
          read_error = s;
        }
      }
    };

    // Open the log file
    std::string logname = LogFileName(wal_dir, log);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<SequentialFileReader> lfile_reader;
    Status status = SequentialFileReader::Create(
        fs, logname, fs->OptimizeForLogRead(file_options_), &lfile_reader,
        nullptr /* dbg */, nullptr /* rate limiter */);
    if (!status.ok()) {
      return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = db_options_.info_log;
    reporter.lognum = log;
    // We intentionally make log::Reader do checksumming so that
    // corruptions cause entire commits to be skipped instead of
    // propagating bad information (like overly large sequence
    // numbers).
    log::Reader reader(db_options_.info_log, std::move(lfile_reader), &reporter,
                       true /*enable checksum*/, log);

    // Initialize per-column family memtables
    for (auto* cfd : *vset_.GetColumnFamilySet()) {
      cfd->CreateNewMemtable(kMaxSequenceNumber);
    }
    std::unique_ptr<ColumnFamilyMemTablesImpl> cf_mems =
        std::make_unique<ColumnFamilyMemTablesImpl>(vset_.GetColumnFamilySet());

    // Read all the records and add to a memtable
    const UnorderedMap<uint32_t, size_t>& running_ts_sz =
        vset_.GetRunningColumnFamiliesTimestampSize();
    std::string scratch;
    Slice record;
    WriteBatch batch;

    int counter = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
        continue;
      }
      Status record_status = WriteBatchInternal::SetContents(&batch, record);
      if (feature_repair_ && record_status.ok()) {
        RepairBatchValidator validator(vset_.GetColumnFamilySet());
        record_status = batch.Iterate(&validator);
        if (record_status.ok() && validator.requires_context) {
          deferred_wal_ = true;
          // No output from this WAL exists yet. Its ordinary records and all
          // later WALs remain intact for the existing transaction recovery.
          return Status::OK();
        }
        if (!record_status.ok()) {
          if (!record_status.IsCorruption()) {
            return record_status;
          }
          reporter.Corruption(record.size(), record_status);
          continue;  // Complete decode precedes any memtable mutation.
        }
        if (validator.ignored_cf_operations != 0) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Repair discarded %" ROCKSDB_PRIszt
                         " WAL operations for excluded CFs",
                         validator.ignored_cf_operations);
        }
        const SequenceNumber sequence = WriteBatchInternal::Sequence(&batch);
        const uint64_t count = WriteBatchInternal::Count(&batch);
        if (sequence > kMaxSequenceNumber ||
            (count != 0 && count - 1 > kMaxSequenceNumber - sequence)) {
          return Status::Corruption("Repair batch sequence range overflow");
        }
        certified_max_sequence_ = std::max(
            certified_max_sequence_, sequence + (count != 0 ? count - 1 : 0));
      }
      if (record_status.ok()) {
        const UnorderedMap<uint32_t, size_t>& record_ts_sz =
            reader.GetRecordedTimestampSize();
        // Use same value for `seq_per_batch` and `batch_per_txn` as
        // WriteBatchInternal::InsertInto does below.
        record_status = HandleWriteBatchTimestampSizeDifference(
            &batch, running_ts_sz, record_ts_sz,
            TimestampSizeConsistencyMode::kVerifyConsistency,
            /* seq_per_batch */ false, /* batch_per_txn */ true);
        if (record_status.ok()) {
          record_status = WriteBatchInternal::InsertInto(
              &batch, cf_mems.get(), nullptr, nullptr,
              /*ignore_missing_column_families=*/feature_repair_,
              /*log_number=*/feature_repair_ ? log : 0);
        }
      }
      if (record_status.ok()) {
        counter += WriteBatchInternal::Count(&batch);
      } else {
        if (feature_repair_) {
          return record_status;
        }
        ROCKS_LOG_WARN(db_options_.info_log, "Log #%" PRIu64 ": ignoring %s",
                       log, record_status.ToString().c_str());
      }
    }
    if (feature_repair_ &&
        (reader.hasReadError() || !reporter.read_error.ok())) {
      return reporter.read_error.ok()
                 ? Status::IOError("Repair WAL read failed")
                 : reporter.read_error;
    }

    // Dump a table for each column family with entries in this log file.
    for (auto* cfd : *vset_.GetColumnFamilySet()) {
      // Do not record a version edit for this conversion to a Table
      // since ExtractMetaData() will also generate edits.
      MemTable* mem = cfd->mem();
      if (mem->IsEmpty()) {
        continue;
      }

      FileMetaData meta;
      uint64_t file_number = 0;
      status = AllocateRepairFileNumber(&file_number);
      if (!status.ok()) {
        break;
      }
      TEST_SYNC_POINT_CALLBACK("Repairer::ConvertLogToTable:TableFileNumber",
                               &file_number);
      meta.fd = FileDescriptor(file_number, 0, 0);
      if (feature_repair_) {
        status = RegisterRepairOutput(
            TableFileName(db_options_.db_paths, file_number, 0), file_number,
            cfd->GetID(), log);
        if (!status.ok()) {
          break;
        }
      }
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions ro;
      ro.total_order_seek = true;
      Arena arena;
      ScopedArenaPtr<InternalIterator> iter(
          mem->NewIterator(ro, /*seqno_to_time_mapping=*/nullptr, &arena,
                           /*prefix_extractor=*/nullptr, /*for_flush=*/true));
      int64_t _current_time = 0;
      immutable_db_options_.clock->GetCurrentTime(&_current_time)
          .PermitUncheckedError();  // ignore error
      const uint64_t current_time = static_cast<uint64_t>(_current_time);
      meta.file_creation_time = current_time;
      SnapshotChecker* snapshot_checker = DisableGCSnapshotChecker::Instance();

      auto write_hint = cfd->current()->storage_info()->CalculateSSTWriteHint(
          /*level=*/0, db_options_.calculate_sst_write_lifetime_hint_set);

      std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
          range_del_iters;
      auto range_del_iter = mem->NewRangeTombstoneIterator(
          ro, kMaxSequenceNumber, false /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }

      IOStatus io_s;
      CompressionOptions default_compression;
      // TODO: plumb Env::IOActivity, Env::IOPriority
      const ReadOptions read_options;
      const WriteOptions write_option(Env::IO_HIGH);
      TableBuilderOptions tboptions(
          cfd->ioptions(), cfd->GetLatestMutableCFOptions(), read_options,
          write_option, cfd->internal_comparator(),
          cfd->internal_tbl_prop_coll_factories(), kNoCompression,
          default_compression, cfd->GetID(), cfd->GetName(), -1 /* level */,
          current_time /* newest_key_time */, false /* is_bottommost */,
          TableFileCreationReason::kRecovery, 0 /* oldest_key_time */,
          "DB Repairer" /* db_id */, db_session_id_, 0 /*target_file_size*/,
          meta.fd.GetNumber(),
          kMaxSequenceNumber /*last_level_inclusive_max_seqno_threshold*/,
          dbname_ /*db_name*/);
      // Legacy salvage output stays legacy. It has no format fence, so exact
      // metadata would fail publication and become false feature evidence.
      tboptions.collect_blob_reference_metadata =
          feature_repair_ &&
          (cfd->GetLatestMutableCFOptions().enable_blob_indirection ||
           feature_format_column_families_.find(cfd->GetID()) !=
               feature_format_column_families_.end());

      SeqnoToTimeMapping empty_seqno_to_time_mapping;
      status = BuildTable(
          dbname_, /* versions */ nullptr, immutable_db_options_, tboptions,
          file_options_, table_cache_.get(), iter.get(),
          std::move(range_del_iters), &meta, nullptr /* blob_file_additions */,
          {}, kMaxSequenceNumber, kMaxSequenceNumber, kMaxSequenceNumber,
          snapshot_checker, false /* paranoid_file_checks*/,
          nullptr /* internal_stats */, &io_s, nullptr /*IOTracer*/,
          BlobFileCreationReason::kRecovery,
          nullptr /* seqno_to_time_mapping */, nullptr /* event_logger */,
          0 /* job_id */, nullptr /* table_properties */, write_hint);
      if (!io_s.ok() && status.ok()) {
        status = io_s;
      }
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Log #%" PRIu64 ": %d ops saved to Table #%" PRIu64 " %s",
                     log, counter, meta.fd.GetNumber(),
                     status.ToString().c_str());
      if (status.ok()) {
        if (meta.fd.GetFileSize() > 0) {
          status = SealRepairOutput(
              TableFileName(db_options_.db_paths, file_number, 0));
          if (!status.ok()) {
            break;
          }
          table_fds_.push_back(meta.fd);
        }
      } else {
        break;
      }
    }
    return status;
  }

  // `tables_built_from_wals`: whether table_fds_ are tables this repair built
  // from WAL files, rather than tables that existed before the repair.
  Status ExtractMetaData(bool tables_built_from_wals) {
    if (feature_repair_) {
      // The log line below tells the user how much temporary disk space the
      // repair scratch file (the file that holds the reference runs) may need.
      // A run record holds about the same bytes as its BlobIndex, plus the user
      // key for a legacy reference. So the total SST size is a rough estimate
      // of the scratch space. The scratch can be larger when the SSTs are
      // compressed.
      uint64_t sst_bytes = 0;
      for (const FileDescriptor& fd : table_fds_) {
        uint64_t size = 0;
        if (env_->GetFileSize(TableFileName(db_options_.db_paths,
                                            fd.GetNumber(), fd.GetPathId()),
                              &size)
                .ok()) {
          sst_bytes += size;
        }
      }
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Repair blob reference runs may use about %" PRIu64
                     " bytes of temporary space in %s",
                     sst_bytes, dbname_.c_str());
    }
    for (size_t i = 0; i < table_fds_.size(); i++) {
      TableInfo t;
      t.meta.fd = table_fds_[i];
      bool repair_metadata_error = false;
      Status status = ScanTable(&t, &repair_metadata_error);
      if (!status.ok()) {
        // Legacy salvage keeps base behavior: set one bad table aside and
        // continue. Its caller cannot stop after inputs were archived.
        if (repair_metadata_error && feature_repair_) {
          return status;
        }
        std::string fname = TableFileName(
            db_options_.db_paths, t.meta.fd.GetNumber(), t.meta.fd.GetPathId());
        if (feature_repair_ && (!status.IsCorruption() ||
                                repair_catalog_.outputs.count(fname) != 0)) {
          // A certified WAL output cannot be dropped after its source WAL's
          // coverage was advanced. Ambiguous original-file I/O is not absence.
          return status;
        }
        char file_num_buf[kFormatFileNumberBufSize];
        FormatFileNumber(t.meta.fd.GetNumber(), t.meta.fd.GetPathId(),
                         file_num_buf, sizeof(file_num_buf));
        ROCKS_LOG_WARN(db_options_.info_log, "Table #%s: ignoring %s",
                       file_num_buf, status.ToString().c_str());
        // Keep the pre-repair image discoverable until CURRENT commits the
        // complete replacement, even for inputs repair elects not to salvage.
        if (feature_repair_) {
          archive_after_publication_.push_back(fname);
        } else {
          ArchiveFile(fname);
        }
      } else {
        // The MANIFEST being rebuilt held the ownership start time and origin
        // of tables that existed before the repair, so those are unknown.
        t.meta.data_file_provenance = kProvenanceRecordedByRepair;
        if (tables_built_from_wals) {
          t.meta.data_file_provenance |=
              DataFileProvenanceForOrigin(DataFileProvenance::Origin::kFlush);
          t.meta.db_ownership_start_time = t.meta.file_creation_time;
        }
        // A classification with frontier 0 is feature evidence for its CF, so
        // the repaired MANIFEST must fence that CF.
        if (!t.meta.blob_reference_descriptor.empty() ||
            t.meta.blob_reference_legacy_metadata) {
          feature_format_column_families_.insert(t.column_family_id);
        }
        tables_.push_back(t);
      }
    }
    return Status::OK();
  }

  Status CollectLegacyBlobReferences(const ParsedInternalKey& parsed,
                                     const Slice& value,
                                     bool has_exact_blob_references,
                                     bool* repair_metadata_error,
                                     TableInfo* table) {
    assert(repair_metadata_error != nullptr);
    assert(table != nullptr);
    if (parsed.type != kTypeBlobIndex && parsed.type != kTypeWideColumnEntity) {
      return Status::OK();
    }

    const auto collect_reference = [&](const BlobIndex& blob_index) -> Status {
      BlobReference reference;
      bool included = false;
      Status status =
          BlobReference::FromBlobIndex(blob_index, &reference, &included);
      if (!status.ok() || !included) {
        return status;
      }
      // Sources feed only the feature blob repair plan. A legacy repair keeps
      // no per-SST state for them.
      if (feature_repair_) {
        table->reference_sources.insert(ReferenceSource(reference));
        // Exact metadata serves stable references; the scan keeps the rest,
        // with the user keys of legacy references.
        if (!has_exact_blob_references || !reference.HasStableIdentity()) {
          status = AppendScannedReference(table, reference, parsed.user_key);
          if (!status.ok()) {
            // A scratch error is never a reason to set the SST aside.
            *repair_metadata_error = true;
            return status;
          }
        }
      }
      if (reference.HasStableIdentity()) {
        if (has_exact_blob_references) {
          return Status::OK();
        }
        const uint64_t frontier =
            table->meta.oldest_coarse_indirect_origin_file_number;
        if (frontier == kInvalidBlobFileNumber ||
            reference.encoding_class !=
                BlobReferenceEncodingClass::kIndirectUnflattened ||
            reference.origin_file_number < frontier) {
          *repair_metadata_error = true;
          return Status::Corruption(
              "Feature BlobIndex is missing exact/coarse SST metadata");
        }
      }
      return Status::OK();
    };

    if (parsed.type == kTypeBlobIndex) {
      BlobIndex blob_index;
      Status status = blob_index.DecodeFrom(value);
      return status.ok() ? collect_reference(blob_index) : status;
    }
    return WideColumnSerialization::ForEachBlobIndex(value, collect_reference);
  }

  Status ScanTable(TableInfo* t, bool* repair_metadata_error) {
    assert(t != nullptr);
    assert(repair_metadata_error != nullptr);
    *repair_metadata_error = false;
    std::string fname = TableFileName(
        db_options_.db_paths, t->meta.fd.GetNumber(), t->meta.fd.GetPathId());
    int counter = 0;
    bool has_exact_blob_references = false;
    uint64_t file_size;
    Status status = env_->GetFileSize(fname, &file_size);
    ColumnFamilyData* cfd = nullptr;
    TableReader* scan_table_reader = nullptr;
    std::unique_ptr<InternalIterator> data_iterator;
    t->meta.fd = FileDescriptor(t->meta.fd.GetNumber(), t->meta.fd.GetPathId(),
                                file_size);
    std::shared_ptr<const TableProperties> props;
    if (status.ok()) {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions read_options;
      read_options.fill_cache = false;
      read_options.verify_checksums = true;
      status = table_cache_->GetTableProperties(
          file_options_, read_options, icmp_, t->meta, &props, default_mopts_,
          /*no_io=*/false, RepairTableOpenOptions());
    }
    if (status.ok()) {
      auto s =
          GetSstInternalUniqueId(props->db_id, props->db_session_id,
                                 props->orig_file_number, &t->meta.unique_id);
      if (!s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Table #%" PRIu64
                       ": unable to get unique id, default to Unknown.",
                       t->meta.fd.GetNumber());
      }
      ExtractBlobReferenceDescriptorFromTableProperties(*props, &t->meta);
      status = ExtractBlobReferenceClassificationFromTableProperties(*props,
                                                                     &t->meta);
      if (!status.ok()) {
        *repair_metadata_error = true;
      }
      has_exact_blob_references = !t->meta.blob_reference_descriptor.empty();
      uint64_t raw_column_family_id = props->column_family_id;
      TEST_SYNC_POINT_CALLBACK("Repairer::ScanTable:ColumnFamilyId",
                               &raw_column_family_id);
      if ((has_exact_blob_references ||
           props->user_collected_properties.count(
               kBlobReferenceClassificationPropertyName) != 0) &&
          (raw_column_family_id > std::numeric_limits<uint32_t>::max() ||
           raw_column_family_id == TablePropertiesCollectorFactory::Context::
                                       kUnknownColumnFamily)) {
        *repair_metadata_error = true;
        status = Status::Corruption(
            "SST blob repair metadata has an invalid column family");
      }
      if (status.ok()) {
        t->column_family_id = static_cast<uint32_t>(raw_column_family_id);
        if (t->column_family_id ==
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) {
          ROCKS_LOG_WARN(
              db_options_.info_log,
              "Table #%" PRIu64
              ": column family unknown (probably due to legacy format); "
              "adding to default column family id 0.",
              t->meta.fd.GetNumber());
          t->column_family_id = 0;
        }

        if (vset_.GetColumnFamilySet()->GetColumnFamily(t->column_family_id) ==
            nullptr) {
          status =
              AddColumnFamily(props->column_family_name, t->column_family_id);
          if (!status.ok() &&
              GetColumnFamilyOptions(props->column_family_name) != nullptr) {
            // An unknown CF retains normal salvage behavior. Failure to
            // commit a known CF is fatal: the staged writer may be unusable.
            *repair_metadata_error = true;
          }
        }
      }
      t->meta.oldest_ancestor_time = props->oldest_ancestor_time;
      t->meta.file_creation_time = props->file_creation_time;
      t->meta.user_defined_timestamps_persisted =
          static_cast<bool>(props->user_defined_timestamps_persisted);
    }
    if (status.ok() && !feature_repair_ &&
        (has_exact_blob_references || t->meta.blob_reference_legacy_metadata ||
         t->meta.oldest_coarse_indirect_origin_file_number !=
             kInvalidBlobFileNumber)) {
      // Detection missed this feature table. Legacy output has no format
      // fence, so archive it like any other table repair cannot use.
      status = Status::Corruption("Feature SST found during legacy repair");
    }
    if (status.ok()) {
      if (t->meta.oldest_coarse_indirect_origin_file_number !=
          kInvalidBlobFileNumber) {
        feature_format_column_families_.insert(t->column_family_id);
      }
      if (has_exact_blob_references) {
        BlobReferenceMetadataDescriptor descriptor;
        status = BlobReferenceMetadataDescriptor::DecodeFrom(
            t->meta.blob_reference_descriptor, &descriptor);
        if (!status.ok()) {
          *repair_metadata_error = true;
        } else {
          TEST_SYNC_POINT_CALLBACK(
              "Repairer::ScanTable:BlobReferenceMetadataDescriptor",
              &descriptor);
        }
        if (status.ok() &&
            (!descriptor.HasRepairMetadata() ||
             descriptor.column_family_id != t->column_family_id ||
             descriptor.sst_file_number != props->orig_file_number)) {
          *repair_metadata_error = true;
          status = Status::Corruption(
              "SST has incomplete or mismatched blob repair metadata");
        }
      } else {
        // An SST without a descriptor is either a coarse SST (it has a coarse
        // classification, for example flush output of a feature CF) or an SST
        // written before the feature was turned on (no classification). The
        // scan below collects its BlobIndexes. After every SST is scanned,
        // RepairBlobState() marks each such SST of a feature CF that has no
        // coarse classification as legacy metadata, so the repaired format
        // fence accepts it. A CF without feature evidence does not get the
        // fence.
      }
    }
    if (status.ok()) {
      uint64_t tail_size = FileMetaData::CalculateTailSize(file_size, *props);
      t->meta.tail_size = tail_size;
    }
    if (status.ok()) {
      cfd = vset_.GetColumnFamilySet()->GetColumnFamily(t->column_family_id);
      if (cfd->GetName() != props->column_family_name) {
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "Table #%" PRIu64
            ": inconsistent column family name '%s'; expected '%s' for column "
            "family id %" PRIu32 ".",
            t->meta.fd.GetNumber(), props->column_family_name.c_str(),
            cfd->GetName().c_str(), t->column_family_id);
        status = Status::Corruption(dbname_, "inconsistent column family name");
      }
    }
    if (status.ok()) {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions ropts;
      ropts.total_order_seek = true;
      ropts.fill_cache = false;
      ropts.verify_checksums = true;
      data_iterator.reset(table_cache_->NewIterator(
          ropts, file_options_, cfd->internal_comparator(), t->meta,
          nullptr /* range_del_agg */, cfd->GetLatestMutableCFOptions(),
          &scan_table_reader, /*file_read_hist=*/nullptr,
          TableReaderCaller::kRepair, /*arena=*/nullptr, /*skip_filters=*/false,
          /*level=*/-1, /*max_file_size_for_l0_meta_pin=*/0,
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key=*/nullptr,
          /*allow_unprepared_value=*/false, /*seek_target=*/nullptr,
          /*prefix_filtered_table=*/nullptr,
          /*range_del_read_seqno=*/nullptr, /*range_del_iter=*/nullptr,
          /*maybe_pin_table_handle=*/false, /*file_open_metadata=*/nullptr,
          RepairTableOpenOptions()));
      status = data_iterator->status();
      if (status.ok() && scan_table_reader == nullptr) {
        status = Status::Corruption("Missing repair table reader");
      }
      if (status.ok() && has_exact_blob_references) {
        status = scan_table_reader->VerifyBlobReferenceMetadata(
            ropts, t->meta.blob_reference_descriptor);
        if (status.ok()) {
          status = cfd->table_cache()->ReadBlobReferenceSstSummary(
              ropts, *scan_table_reader, t->meta,
              cfd->GetLatestMutableCFOptions(), &t->meta.blob_reference_summary,
              /*reservation_manager=*/nullptr);
        }
        if (!status.ok()) {
          *repair_metadata_error = true;
        }
      }
      ParsedInternalKey parsed;
      for (data_iterator->SeekToFirst(); status.ok() && data_iterator->Valid();
           data_iterator->Next()) {
        Slice key = data_iterator->key();
        Status pik_status =
            ParseInternalKey(key, &parsed, db_options_.allow_data_in_errors);
        if (!pik_status.ok()) {
          ROCKS_LOG_ERROR(db_options_.info_log,
                          "Table #%" PRIu64 ": unparsable key - %s",
                          t->meta.fd.GetNumber(), pik_status.getState());
          continue;
        }

        counter++;

        status = CollectLegacyBlobReferences(parsed, data_iterator->value(),
                                             has_exact_blob_references,
                                             repair_metadata_error, t);
        if (!status.ok()) {
          break;
        }

        status = t->meta.UpdateBoundaries(key, data_iterator->value(),
                                          parsed.sequence, parsed.type);
        if (!status.ok()) {
          break;
        }
      }
      if (status.ok() && !data_iterator->status().ok()) {
        status = data_iterator->status();
      }
      if (status.ok()) {
        for (auto& run : t->runs) {
          status = FinishRepairRun(&run.second);
          if (!status.ok()) {
            break;
          }
        }
        if (status.ok() && feature_repair_ && has_exact_blob_references) {
          status = PrepareExactTableRuns(scan_table_reader, ropts, t);
        }
        if (!status.ok()) {
          // A scratch or run error is never a reason to set the SST aside.
          *repair_metadata_error = true;
        }
      }
      ROCKS_LOG_INFO(db_options_.info_log, "Table #%" PRIu64 ": %d entries %s",
                     t->meta.fd.GetNumber(), counter,
                     status.ToString().c_str());
    }
    if (status.ok()) {
      // XXX/FIXME: This is just basic, naive handling of range tombstones,
      // like call to UpdateBoundariesForRange in builder.cc where we assume
      // an SST file is a full sorted run. This probably needs the extra logic
      // from compaction_job.cc around call to UpdateBoundariesForRange (to
      // handle range tombstones extendingg beyond range of other entries).
      // TODO: plumb Env::IOActivity, Env::IOPriority
      ReadOptions ropts;
      ropts.fill_cache = false;
      ropts.verify_checksums = true;
      std::unique_ptr<FragmentedRangeTombstoneIterator> r_iter;
      assert(scan_table_reader != nullptr);
      r_iter.reset(static_cast<FragmentedRangeTombstoneIterator*>(
          scan_table_reader->NewRangeTombstoneIterator(ropts)));

      if (r_iter) {
        status = r_iter->status();
        r_iter->SeekToFirst();

        while (status.ok() && r_iter->Valid()) {
          auto tombstone = r_iter->Tombstone();
          auto kv = tombstone.Serialize();
          t->meta.UpdateBoundariesForRange(
              kv.first, tombstone.SerializeEndKey(), tombstone.seq_,
              cfd->internal_comparator());
          r_iter->Next();
        }
      }
    }
    return status;
  }

  Status GetRelocationTableOptions(
      const ColumnFamilyData& cfd,
      BlockBasedTableOptions* table_options) const {
    assert(table_options != nullptr);
    const TableFactory* const table_factory =
        cfd.GetLatestMutableCFOptions().table_factory.get();
    if (table_factory == nullptr) {
      return Status::NotSupported(
          "Blob relocation repair requires a block-based table factory");
    }
    const BlockBasedTableOptions* options =
        table_factory->GetOptions<BlockBasedTableOptions>();
    const AdaptiveTableFactory* const adaptive =
        table_factory->CheckedCast<AdaptiveTableFactory>();
    if (adaptive != nullptr) {
      options = adaptive->GetBlockBasedTableWriterOptions();
    }
    if (options == nullptr) {
      return Status::NotSupported(
          "Blob relocation repair requires a block-based table factory");
    }
    *table_options = *options;
    return Status::OK();
  }

  Status ScanNormalBlobFile(const BlobFilePath& file,
                            const std::set<uint64_t>& required_origins,
                            NormalBlobFileInfo* info,
                            bool* belongs_to_active_column_family) {
    *belongs_to_active_column_family = false;
    uint64_t file_size = 0;
    Status status = env_->GetFileSize(file.path, &file_size);
    if (!status.ok()) {
      return status;
    }
    if (file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
      return Status::Corruption("Blob file is too small");
    }
    std::unique_ptr<RandomAccessFileReader> file_reader;
    status = RandomAccessFileReader::Create(
        env_->GetFileSystem(), file.path, file_options_, &file_reader, nullptr);
    if (!status.ok()) {
      return status;
    }
    BlobLogSequentialReader reader(std::move(file_reader),
                                   immutable_db_options_.clock,
                                   immutable_db_options_.stats);
    BlobLogHeader header;
    status = reader.ReadHeader(&header);
    if (!status.ok()) {
      return status;
    }
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(header.column_family_id);
    if (header.has_ttl || header.expiration_range != ExpirationRange{} ||
        cfd == nullptr || cfd->ioptions().cf_paths.empty() ||
        (header.HasRepairMetadata() &&
         header.repair_file_number != file.file_number)) {
      return Status::Corruption("Invalid normal blob repair header");
    }
    if (feature_format_column_families_.count(header.column_family_id) == 0 ||
        required_origins.count(file.file_number) == 0) {
      return Status::OK();
    }
    *belongs_to_active_column_family = true;
    uint64_t blob_count = 0;
    uint64_t blob_bytes = 0;
    while (reader.GetNextByte() < file_size - BlobLogFooter::kSize) {
      BlobLogRecord record;
      uint64_t offset = 0;
      TEST_SYNC_POINT("Repairer::ScanNormalBlobFile:ReadRecord");
      status = reader.ReadRecordHeader(&record, &offset,
                                       file_size - BlobLogFooter::kSize);
      if (status.ok()) {
        // Inventory only framing. Selected requirements are validated by the
        // ordered record cursor, without retaining a map of every offset/key.
        status = reader.ReadRecordPayload(&record,
                                          BlobLogSequentialReader::kReadHeader);
      }
      if (!status.ok()) {
        return status;
      }
      if (record.record_size() >
          std::numeric_limits<uint64_t>::max() - blob_bytes) {
        return Status::Corruption("Blob record byte total overflow");
      }
      blob_bytes += record.record_size();
      ++blob_count;
    }
    if (reader.GetNextByte() != file_size - BlobLogFooter::kSize) {
      return Status::Corruption("Blob records do not align with footer");
    }
    BlobLogFooter footer;
    status = reader.ReadFooter(&footer);
    if (!status.ok()) {
      return status;
    }
    const ExpirationRange expected =
        header.HasRepairMetadata()
            ? ExpirationRange{blob_bytes, header.GetRepairMetadataFooterValue()}
            : ExpirationRange{};
    if (reader.GetNextByte() != file_size || footer.blob_count != blob_count ||
        footer.expiration_range != expected) {
      return Status::Corruption("Blob footer does not match file contents");
    }
    info->addition = BlobFileAddition(file.file_number, blob_count, blob_bytes,
                                      "", "", header.is_indirect_origin);
    info->column_family_id = header.column_family_id;
    info->file_size = file_size;
    info->compression = header.compression;
    info->path = file.path;
    info->is_canonical =
        file.path ==
        BlobFileName(cfd->ioptions().cf_paths.front().path, file.file_number);
    return Status::OK();
  }

  Status CopyNormalBlobFileToCanonical(uint64_t file_number,
                                       const NormalBlobFileInfo& file) {
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(file.column_family_id);
    if (cfd == nullptr || cfd->ioptions().cf_paths.empty()) {
      return Status::Corruption("Missing canonical blob file path");
    }
    const std::string& output_directory = cfd->ioptions().cf_paths.front().path;
    const std::string output_path = BlobFileName(output_directory, file_number);
    const std::string temporary_path =
        TempFileName(output_directory, file_number);
    if (file.path == output_path) {
      return Status::InvalidArgument("Blob file is already canonical");
    }

    // Repair holds the DB lock, but rename can still replace an existing
    // destination. Never overwrite pre-repair bytes before CURRENT commits.
    // Restoring an absent canonical file is safe; replacing an existing one
    // would require a separate crash-safe backup/restore protocol.
    Status status = env_->FileExists(output_path);
    if (status.ok()) {
      return Status::Corruption(
          "Repair refuses to replace an existing canonical blob file; move "
          "it away and run RepairDB again",
          output_path);
    }
    if (!status.IsNotFound()) {
      return status;
    }
    status = RegisterRepairOutput(temporary_path, file_number, cfd->GetID(), 0,
                                  /*sync=*/false);
    if (status.ok()) {
      status = RegisterRepairOutput(output_path, file_number, cfd->GetID(), 0);
    }
    if (!status.ok()) {
      return status;
    }
    status = CopyFile(env_->GetFileSystem(), file.path, Temperature::kUnknown,
                      temporary_path, Temperature::kUnknown, file.file_size,
                      db_options_.use_fsync,
                      /*io_tracer=*/nullptr);
    if (status.ok()) {
      status = env_->RenameFile(temporary_path, output_path);
    }
    if (status.ok()) {
      std::unique_ptr<FSDirectory> directory;
      status = env_->GetFileSystem()->NewDirectory(
          output_directory, IOOptions(), &directory, /*dbg=*/nullptr);
      if (status.ok()) {
        status = directory->FsyncWithDirOptions(
            IOOptions(), /*dbg=*/nullptr,
            DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      }
    }
    if (!status.ok()) {
      env_->DeleteFile(temporary_path).PermitUncheckedError();
    }
    if (status.ok()) {
      status = SealRepairOutput(output_path);
    }
    return status;
  }

  Status DiscoverBlobFiles(const std::set<uint64_t>& required_normal_records,
                           NormalBlobFileCandidates* normal_files,
                           RelocationBlobFileCandidates* relocation_files) {
    assert(normal_files != nullptr);
    assert(relocation_files != nullptr);
    for (const BlobFilePath& file : blob_file_paths_) {
      blob_file_numbers_on_disk_.insert(file.file_number);
      if (lost_relocation_files_.count(file.file_number) != 0 ||
          durable_dropped_numbers_.count(file.file_number) != 0) {
        // A restored copy cannot undo a committed loss or a committed drop.
        // RepairBlobState() steps 3 and 4 check the dropped-file record before
        // they look for a file. Step 5 does not: SelectRepairBlobReader()
        // looks for donor bytes by origin. A flattened reference can have a
        // dropped origin, because best_efforts_blob_db_repair records a missing
        // indirect origin as dropped even when it has a route (see
        // AddBlobFileIfNeeded() in db/version_builder.cc). Skipping the copy
        // here keeps the record authoritative there too, so such a rebuild
        // fails with Corruption instead of reading a file that the MANIFEST
        // declared lost.
        continue;
      }
      uint64_t file_size = 0;
      Status status = env_->GetFileSize(file.path, &file_size);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Blob file #%" PRIu64 ": ignoring %s", file.file_number,
                       status.ToString().c_str());
        continue;
      }
      bool is_relocation = false;
      std::unique_ptr<RandomAccessFileReader> prefix_reader;
      if (file_size >= kBlobRelocationFilePrefixSize) {
        status = RandomAccessFileReader::Create(
            env_->GetFileSystem(), file.path, file_options_, &prefix_reader,
            /*dbg=*/nullptr);
        if (status.ok()) {
          char scratch[kBlobRelocationFilePrefixSize];
          Slice prefix;
          status = prefix_reader->Read(0, kBlobRelocationFilePrefixSize,
                                       IOOptions(), &prefix, scratch,
                                       /*dbg=*/nullptr);
          is_relocation = status.ok() &&
                          prefix.size() == kBlobRelocationFilePrefixSize &&
                          prefix == Slice(kBlobRelocationFilePrefix,
                                          kBlobRelocationFilePrefixSize);
        }
      }

      if (!is_relocation) {
        NormalBlobFileInfo info;
        bool belongs_to_active_column_family = false;
        status = ScanNormalBlobFile(file, required_normal_records, &info,
                                    &belongs_to_active_column_family);
        if (status.ok() && belongs_to_active_column_family) {
          normal_files->emplace(file.file_number, std::move(info));
        }
      } else {
        // Read the owner column family from the table properties, then open
        // the file once, with that column family's table options.
        std::unique_ptr<BlobRelocationFileReader> reader;
        BlobRelocationDirectory directory;
        std::unique_ptr<TableProperties> properties;
        ReadOptions read_options;
        read_options.fill_cache = false;
        status = ReadTableProperties(prefix_reader.get(), file_size,
                                     kBlockBasedTableMagicNumber,
                                     default_iopts_, read_options, &properties);
        ColumnFamilyData* cfd = nullptr;
        if (status.ok()) {
          if (properties->column_family_id <=
              std::numeric_limits<uint32_t>::max()) {
            cfd = vset_.GetColumnFamilySet()->GetColumnFamily(
                static_cast<uint32_t>(properties->column_family_id));
          }
          if (cfd == nullptr ||
              feature_format_column_families_.count(cfd->GetID()) == 0) {
            status = Status::Corruption(
                "Relocation file does not match a recovered column family");
          }
        }
        BlockBasedTableOptions table_options;
        if (status.ok()) {
          status = GetRelocationTableOptions(*cfd, &table_options);
        }
        if (status.ok()) {
          const MutableCFOptions& mutable_options =
              cfd->GetLatestMutableCFOptions();
          status = BlobRelocationFileReader::OpenForRepair(
              cfd->ioptions(), ReadOptions(), file_options_,
              /*blob_file_read_hist=*/nullptr, file.file_number, file_size,
              file.path, db_session_id_, table_options,
              mutable_options.compression_manager.get(),
              mutable_options.block_protection_bytes_per_key,
              /*io_tracer=*/nullptr, &reader, &directory);
        }
        if (status.ok() && reader->column_family_id() != cfd->GetID()) {
          status = Status::Corruption(
              "Relocation file does not match a recovered column family");
        }
        if (status.ok()) {
          RelocationBlobFileInfo info;
          info.column_family_id = reader->column_family_id();
          info.file_size = file_size;
          info.path = file.path;
          ColumnFamilyData* const owner =
              vset_.GetColumnFamilySet()->GetColumnFamily(
                  info.column_family_id);
          info.is_canonical =
              owner != nullptr && !owner->ioptions().cf_paths.empty() &&
              file.path == BlobFileName(owner->ioptions().cf_paths.front().path,
                                        file.file_number);
          std::string encoded_directory;
          directory.EncodeTo(&encoded_directory);
          info.addition = BlobRelocationFileAddition(
              file.file_number, file_size,
              /*checksum_method=*/"", /*checksum_value=*/"",
              std::move(encoded_directory), reader->tail_size());
          info.directory = std::move(directory);
          relocation_files->emplace(file.file_number, std::move(info));
        }
      }
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Blob file #%" PRIu64 ": ignoring invalid candidate: %s",
                       file.file_number, status.ToString().c_str());
        if (!status.IsCorruption() && !status.IsNotSupported()) {
          return status;
        }
      }
    }
    return Status::OK();
  }

  static bool SameBlobReference(const BlobReference& lhs,
                                const BlobReference& rhs) {
    return lhs.encoding_class == rhs.encoding_class &&
           lhs.generation_file_number == rhs.generation_file_number &&
           lhs.origin_file_number == rhs.origin_file_number &&
           lhs.original_value_offset == rhs.original_value_offset &&
           lhs.stored_value_size == rhs.stored_value_size &&
           lhs.record_bytes == rhs.record_bytes &&
           lhs.compression == rhs.compression &&
           lhs.stable_checksum == rhs.stable_checksum &&
           lhs.stored_checksum == rhs.stored_checksum;
  }

  static bool SameStableBlobMetadata(const BlobReference& lhs,
                                     const BlobReference& rhs) {
    return lhs.HasStableIdentity() && rhs.HasStableIdentity() &&
           lhs.origin_file_number == rhs.origin_file_number &&
           lhs.original_value_offset == rhs.original_value_offset &&
           lhs.stored_value_size == rhs.stored_value_size &&
           lhs.compression == rhs.compression &&
           lhs.stable_checksum == rhs.stable_checksum &&
           lhs.stored_checksum == rhs.stored_checksum;
  }

  Status ReadRequiredNormalRecord(const BlobReference& reference,
                                  const NormalBlobFileInfo& file,
                                  BlobReaderCache* cache) {
    if (cache->normal_records == nullptr ||
        cache->normal_record_path != file.path ||
        reference.original_value_offset < cache->checked_offset) {
      cache->normal_records.reset();
      cache->normal_record_path.clear();
      cache->checked_offset = 0;
      std::unique_ptr<RandomAccessFileReader> reader;
      Status status = RandomAccessFileReader::Create(
          env_->GetFileSystem(), file.path, file_options_, &reader, nullptr);
      if (!status.ok()) {
        return status;
      }
      cache->normal_records = std::make_unique<BlobLogSequentialReader>(
          std::move(reader), immutable_db_options_.clock,
          immutable_db_options_.stats);
      BlobLogHeader header;
      status = cache->normal_records->ReadHeader(&header);
      if (!status.ok()) {
        cache->normal_records.reset();
        return status;
      }
      if (header.column_family_id != file.column_family_id ||
          header.compression != file.compression) {
        cache->normal_records.reset();
        return Status::Corruption("Normal blob donor changed during repair");
      }
      cache->normal_record_path = file.path;
    }
    if (cache->checked_offset != 0 &&
        reference.original_value_offset == cache->checked_offset) {
      return Status::OK();
    }
    while (cache->normal_records->GetNextByte() <
           file.file_size - BlobLogFooter::kSize) {
      BlobLogRecord record;
      uint64_t offset = 0;
      Status status = cache->normal_records->ReadRecordHeader(
          &record, &offset, file.file_size - BlobLogFooter::kSize);
      if (!status.ok()) {
        return status;
      }
      if (offset > reference.original_value_offset) {
        return Status::Corruption(
            "Blob reference does not name a record boundary");
      }
      if (offset != reference.original_value_offset) {
        status = cache->normal_records->ReadRecordPayload(
            &record, BlobLogSequentialReader::kReadHeader);
        if (!status.ok()) {
          return status;
        }
        continue;
      }
      cache->checked_user_key.clear();
      // One read verifies the record CRC and gives the value checksum that
      // stable references are checked against.
      status =
          cache->normal_records->ReadAndVerifyRecordPayloadAndGetValueChecksum(
              record,
              reference.HasStableIdentity() ? nullptr
                                            : &cache->checked_user_key,
              &cache->checked_value_checksum);
      if (!status.ok()) {
        cache->normal_records.reset();
        return status;
      }
      cache->checked_offset = offset;
      cache->checked_value_size = record.value_size;
      cache->checked_record_bytes = record.record_size();
      TEST_SYNC_POINT("Repairer::ScanNormalBlobFile:RetainedRecord");
      return Status::OK();
    }
    return Status::Corruption("Blob file does not contain a required record");
  }

  // Streams the stored bytes of `index` from a blob file or a relocation file.
  static Status StreamStoredBlob(const BlobFileReader& reader,
                                 const BlobIndex& index,
                                 const BlobChunkConsumer& consumer) {
    return reader.relocation() != nullptr
               ? reader.relocation()->StreamStoredBlob(ReadOptions(), index,
                                                       consumer)
               : reader.StreamStoredBlobForRelocation(ReadOptions(), index,
                                                      consumer);
  }

  static Status MakeRepairBlobIndex(const BlobReference& reference,
                                    BlobIndex* index) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, reference.generation_file_number,
        {.origin_file_number = reference.origin_file_number,
         .original_value_offset = reference.original_value_offset,
         .stored_value_size = reference.stored_value_size,
         .compression = reference.compression,
         .stable_checksum = reference.stable_checksum,
         .stored_checksum = reference.stored_checksum});
    return index->DecodeFrom(encoded);
  }

  Status ValidateNormalBlobReference(
      uint32_t column_family_id, const BlobReference& reference,
      const NormalBlobFileInfo& file,
      const std::string* expected_legacy_user_key,
      NormalBlobValidationMode validation_mode, BlobReaderCache* reader_cache) {
    assert(reader_cache != nullptr);
    if (file.column_family_id != column_family_id ||
        file.compression != reference.compression) {
      return Status::Corruption("Blob reference owner or compression mismatch");
    }
    Status record_status =
        ReadRequiredNormalRecord(reference, file, reader_cache);
    if (!record_status.ok()) {
      return record_status;
    }
    if (reader_cache->checked_value_size != reference.stored_value_size ||
        (reference.encoding_class !=
             BlobReferenceEncodingClass::kIndirectFlattened &&
         reference.record_bytes != 0 &&
         reader_cache->checked_record_bytes != reference.record_bytes)) {
      return Status::Corruption("Blob reference does not name an exact record");
    }
    if (validation_mode == NormalBlobValidationMode::kPublishedFile &&
        reference.HasStableIdentity() != file.addition.IsIndirectOrigin()) {
      return Status::Corruption(
          "Blob reference encoding does not match file classification");
    }
    if (expected_legacy_user_key != nullptr &&
        reader_cache->checked_user_key != *expected_legacy_user_key) {
      return Status::Corruption(
          "Legacy blob reference does not match its SST user key");
    }
    if (!reference.HasStableIdentity()) {
      return Status::OK();
    }
    // The stored bytes were already read once above; check them here instead
    // of reading them again.
    if (BlobIndex::ExtendChecksumWithCompression(
            reader_cache->checked_value_checksum, reference.compression) !=
        reference.stored_checksum) {
      return Status::Corruption("Stored blob checksum mismatch");
    }
    if (validation_mode != NormalBlobValidationMode::kPayloadDonor) {
      return Status::OK();
    }
    // A payload donor also needs a reader to copy the bytes from.
    const uint64_t file_number = file.addition.GetBlobFileNumber();
    if (reader_cache->file_number != file_number ||
        reader_cache->path != file.path) {
      reader_cache->reader.reset();
      reader_cache->file_number = kInvalidBlobFileNumber;
      reader_cache->path.clear();
      ColumnFamilyData* const cfd =
          vset_.GetColumnFamilySet()->GetColumnFamily(column_family_id);
      if (cfd == nullptr) {
        return Status::Corruption("Missing blob reference column family");
      }
      Status status = BlobFileReader::CreateFromPathForRepair(
          cfd->ioptions(), ReadOptions(), file_options_, column_family_id,
          /*blob_file_read_hist=*/nullptr, file_number, file.path,
          /*io_tracer=*/nullptr, &reader_cache->reader);
      if (!status.ok()) {
        return status;
      }
      reader_cache->file_number = file_number;
      reader_cache->path = file.path;
    }
    return Status::OK();
  }

  Status GetRelocationBlobReader(uint64_t file_number,
                                 const RelocationBlobFileInfo& file,
                                 BlobReaderCache* reader_cache,
                                 BlobFileReader** reader) {
    assert(reader_cache != nullptr);
    assert(reader != nullptr);
    *reader = nullptr;
    if (reader_cache->file_number == file_number &&
        reader_cache->path == file.path) {
      assert(reader_cache->reader != nullptr);
      *reader = reader_cache->reader.get();
      return Status::OK();
    }

    reader_cache->reader.reset();
    reader_cache->file_number = kInvalidBlobFileNumber;
    reader_cache->path.clear();
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(file.column_family_id);
    if (cfd == nullptr) {
      return Status::Corruption("Missing blob relocation column family");
    }
    BlockBasedTableOptions table_options;
    Status status = GetRelocationTableOptions(*cfd, &table_options);
    if (!status.ok()) {
      return status;
    }
    const MutableCFOptions& mutable_options = cfd->GetLatestMutableCFOptions();
    std::unique_ptr<BlobRelocationFileReader> relocation;
    status = BlobRelocationFileReader::OpenForRepair(
        cfd->ioptions(), ReadOptions(), file_options_,
        /*blob_file_read_hist=*/nullptr, file_number, file.file_size, file.path,
        db_session_id_, table_options,
        mutable_options.compression_manager.get(),
        mutable_options.block_protection_bytes_per_key,
        /*io_tracer=*/nullptr, &relocation);
    if (!status.ok()) {
      return status;
    }
    if (relocation->column_family_id() != file.column_family_id ||
        relocation->directory_digest() != file.directory.directory_digest() ||
        relocation->total_entries() != file.directory.total_entries() ||
        relocation->total_record_bytes() !=
            file.directory.total_record_bytes()) {
      return Status::Corruption("Blob relocation file changed during repair");
    }
    reader_cache->reader.reset(new BlobFileReader(std::move(relocation)));
    reader_cache->file_number = file_number;
    reader_cache->path = file.path;
    *reader = reader_cache->reader.get();
    return Status::OK();
  }

  static Status ValidateRelocationBlobReference(const BlobReference& reference,
                                                BlobFileReader* reader) {
    assert(reader != nullptr);
    BlobIndex index;
    Status status = MakeRepairBlobIndex(reference, &index);
    return status.ok()
               ? StreamStoredBlob(*reader, index,
                                  [](const Slice&) { return Status::OK(); })
               : status;
  }

  Status SelectRepairBlobReader(uint32_t cf_id, const BlobReference& reference,
                                const NormalBlobFileCandidates& normal_files,
                                const RelocationOriginIndex& relocation_origins,
                                BlobReaderCache* normal_cache,
                                BlobReaderCache* relocation_cache,
                                BlobFileReader** reader) {
    const auto range = normal_files.equal_range(reference.origin_file_number);
    for (auto file = range.first; file != range.second; ++file) {
      Status status = ValidateNormalBlobReference(
          cf_id, reference, file->second, nullptr,
          NormalBlobValidationMode::kPayloadDonor, normal_cache);
      if (status.ok()) {
        *reader = normal_cache->reader.get();
        return status;
      }
      if (!status.IsCorruption() && !status.IsNotSupported()) {
        return status;
      }
    }
    const auto relocation_range = relocation_origins.equal_range(
        std::make_pair(cf_id, reference.origin_file_number));
    for (auto entry = relocation_range.first; entry != relocation_range.second;
         ++entry) {
      const auto& file = *entry->second;
      Status status = GetRelocationBlobReader(file.first, file.second,
                                              relocation_cache, reader);
      if (status.ok()) {
        status = ValidateRelocationBlobReference(reference, *reader);
      }
      if (status.ok()) {
        return status;
      }
      if (!status.IsCorruption() && !status.IsNotSupported()) {
        return status;
      }
    }
    if (range.first == range.second &&
        relocation_range.first == relocation_range.second &&
        blob_file_numbers_on_disk_.count(reference.origin_file_number) == 0) {
      return Status::Corruption(
          "Repair cannot find missing blob file #" +
          std::to_string(reference.origin_file_number) +
          ", and the MANIFEST has no dropped-file record for it");
    }
    return Status::Corruption(
        "Repair cannot find a valid copy of required blob bytes");
  }

  // One relocation file that RepairDB writes for the generation numbers that no
  // existing file can serve. There is one such file per column family, and it
  // serves all of these generation numbers. Its records are in key order
  // (origin, original offset). The values of one generation number can come
  // from several origins and interleave with the values of others, and one
  // route points to one file. So these generation numbers are not split over
  // several files.
  struct RepairRelocationPack {
    uint32_t column_family_id = 0;
    uint64_t file_number = 0;
    std::string output_directory;
    std::string output_path;
    std::string temporary_path;
    // The table builder keeps this comparator by reference through Finish.
    std::unique_ptr<InternalKeyComparator> comparator;
    std::unique_ptr<WritableFileWriter> file_writer;
    std::unique_ptr<BlockBasedTableBuilder> table_builder;

    bool IsOpen() const { return table_builder != nullptr; }
  };

  Status OpenRepairRelocationPack(uint32_t column_family_id,
                                  RepairRelocationPack* pack) {
    assert(pack != nullptr && !pack->IsOpen());
    ColumnFamilyData* const cfd =
        vset_.GetColumnFamilySet()->GetColumnFamily(column_family_id);
    if (cfd == nullptr) {
      return Status::Corruption("Missing blob repair column family");
    }
    BlockBasedTableOptions source_table_options;
    Status status = GetRelocationTableOptions(*cfd, &source_table_options);
    if (!status.ok()) {
      return status;
    }
    *pack = RepairRelocationPack();
    pack->column_family_id = column_family_id;
    status = AllocateRepairFileNumber(&pack->file_number);
    if (!status.ok()) {
      return status;
    }
    // A relocation file number must be at most kMaxBlobRelocationFileNumber
    // (2^62 - 1). This is kFileNumberMask, the limit for all table and blob
    // file numbers.
    if (pack->file_number > kMaxBlobRelocationFileNumber) {
      return Status::Corruption(
          "Repair relocation file number is above the route limit");
    }
    pack->output_directory = cfd->ioptions().cf_paths.front().path;
    pack->output_path = BlobFileName(pack->output_directory, pack->file_number);
    pack->temporary_path =
        TempFileName(pack->output_directory, pack->file_number);
    FileOptions file_options = file_options_;
    file_options.open_contract = FileOpenContract::kNoReopenForWrite |
                                 FileOpenContract::kNoReadersWhileOpenForWrite;
    std::unique_ptr<FSWritableFile> file;
    status = RegisterRepairOutput(pack->temporary_path, pack->file_number,
                                  column_family_id, 0, /*sync=*/false);
    if (status.ok()) {
      status = RegisterRepairOutput(pack->output_path, pack->file_number,
                                    column_family_id, 0);
    }
    if (status.ok()) {
      status = NewWritableFile(env_->GetFileSystem().get(),
                               pack->temporary_path, &file, file_options);
    }
    if (status.ok() && file == nullptr) {
      status =
          Status::Corruption("File system returned a null repair blob file");
    }
    if (!status.ok()) {
      return status;
    }
    FileTypeSet checksum_types =
        immutable_db_options_.checksum_handoff_file_types;
    pack->file_writer = std::make_unique<WritableFileWriter>(
        std::move(file), pack->temporary_path, file_options,
        immutable_db_options_.clock, /*io_tracer=*/nullptr,
        immutable_db_options_.stats, Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS,
        immutable_db_options_.listeners,
        immutable_db_options_.file_checksum_gen_factory.get(),
        checksum_types.Contains(FileType::kBlobFile), false);
    pack->comparator =
        std::make_unique<InternalKeyComparator>(BytewiseComparator());
    BlobRelocationTableFileInfo file_info;
    file_info.column_family_id = column_family_id;
    file_info.column_family_name = cfd->GetName();
    file_info.reason = TableFileCreationReason::kRecovery;
    file_info.db_id = "DB Repairer";
    file_info.db_session_id = db_session_id_;
    file_info.db_name = dbname_;
    file_info.file_number = pack->file_number;
    status = NewBlobRelocationTableBuilder(
        cfd->ioptions(), cfd->GetLatestMutableCFOptions(), source_table_options,
        ReadOptions(), WriteOptions(), *pack->comparator, file_info,
        pack->file_writer.get(), &pack->table_builder);
    if (!status.ok()) {
      AbandonRepairRelocationPack(pack);
    }
    return status;
  }

  // Writes the generation numbers `generations` into the served list of `pack`,
  // then copies the value of every reference in `references` into `pack`, in
  // key order (origin, original offset). A value that several generation
  // numbers or SSTs reference is written once.
  Status AddRepairRelocationGenerations(
      const std::vector<uint64_t>& generations,
      const std::vector<RepairCursorSpec>& references,
      const NormalBlobFileCandidates& normal_files,
      const RelocationOriginIndex& relocation_origins,
      BlobReaderCache* normal_reader_cache,
      BlobReaderCache* relocation_reader_cache, RepairRelocationPack* pack) {
    assert(normal_reader_cache != nullptr);
    assert(relocation_reader_cache != nullptr);
    assert(pack != nullptr && pack->IsOpen());
    assert(!generations.empty());
    BlockBasedTableBuilder* const table_builder = pack->table_builder.get();
    Status status;
    for (uint64_t generation : generations) {
      status = table_builder->AddBlobRelocationServedFile(generation);
      if (!status.ok()) {
        return status;
      }
    }
    return VisitRepairReferences(
        references,
        [&](const BlobReference& reference,
            const std::vector<RepairReferenceCursor*>&) -> Status {
          BlobFileReader* reader = nullptr;
          Status selected = SelectRepairBlobReader(
              pack->column_family_id, reference, normal_files,
              relocation_origins, normal_reader_cache, relocation_reader_cache,
              &reader);
          if (!selected.ok()) {
            return selected;
          }
          BlobIndex index;
          selected = MakeRepairBlobIndex(reference, &index);
          if (!selected.ok()) {
            return selected;
          }
          return table_builder->AddBlobRelocation(
              reference.origin_file_number, reference.original_value_offset,
              reference.stored_value_size, reference.compression,
              reference.stored_checksum,
              [&](const BlobChunkConsumer& consumer) {
                return StreamStoredBlob(*reader, index, consumer);
              });
        });
  }

  void AbandonRepairRelocationPack(RepairRelocationPack* pack) {
    if (pack->table_builder != nullptr) {
      pack->table_builder->Abandon();
      pack->table_builder.reset();
    }
    if (pack->file_writer != nullptr) {
      pack->file_writer->Close(IOOptions()).PermitUncheckedError();
      pack->file_writer.reset();
    }
    if (!pack->temporary_path.empty()) {
      env_->DeleteFile(pack->temporary_path).PermitUncheckedError();
    }
    *pack = RepairRelocationPack();
  }

  Status FinishRepairRelocationPack(RepairRelocationPack* pack,
                                    BlobRelocationFileAddition* addition) {
    assert(pack != nullptr && pack->IsOpen());
    assert(addition != nullptr);
    BlobRelocationFileMetadata metadata;
    Status status = pack->table_builder->Finish();
    const uint64_t file_size = pack->table_builder->FileSize();
    if (status.ok()) {
      status = pack->table_builder->GetBlobRelocationFileMetadata(&metadata);
    }
    const IOStatus builder_status = pack->table_builder->io_status();
    if (!builder_status.ok() && status.ok()) {
      status = builder_status;
    }
    pack->table_builder.reset();

    std::string checksum_method;
    std::string checksum_value;
    IOOptions io_options;
    if (status.ok()) {
      status = WritableFileWriter::PrepareIOOptions(WriteOptions(), io_options);
    }
    if (status.ok()) {
      status = immutable_db_options_.use_fsync
                   ? pack->file_writer->Fsync(io_options)
                   : pack->file_writer->Sync(io_options);
    }
    const IOStatus close_status = pack->file_writer->Close(io_options);
    if (!close_status.ok() && status.ok()) {
      status = close_status;
    }
    if (status.ok()) {
      checksum_value = pack->file_writer->GetFileChecksum();
      checksum_method = pack->file_writer->GetFileChecksumFuncName();
      if (checksum_value == kUnknownFileChecksum) {
        checksum_value.clear();
      }
      if (checksum_method == kUnknownFileChecksumFuncName) {
        checksum_method.clear();
      }
      if (checksum_method.empty() != checksum_value.empty()) {
        status =
            Status::Corruption("Incomplete repair blob file checksum metadata");
      }
    }
    pack->file_writer.reset();
    if (status.ok()) {
      status = env_->RenameFile(pack->temporary_path, pack->output_path);
    }
    if (status.ok()) {
      std::unique_ptr<FSDirectory> directory;
      status = env_->GetFileSystem()->NewDirectory(
          pack->output_directory, IOOptions(), &directory, /*dbg=*/nullptr);
      if (status.ok()) {
        status = directory->FsyncWithDirOptions(
            IOOptions(), /*dbg=*/nullptr,
            DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      }
    }
    if (!status.ok()) {
      env_->DeleteFile(pack->temporary_path).PermitUncheckedError();
      *pack = RepairRelocationPack();
      return status;
    }
    *addition = BlobRelocationFileAddition(
        pack->file_number, file_size, std::move(checksum_method),
        std::move(checksum_value), std::move(metadata.encoded_directory),
        metadata.tail_size);
    const std::string output_path = pack->output_path;
    *pack = RepairRelocationPack();
    status = addition->Validate();
    return status.ok() ? SealRepairOutput(output_path) : status;
  }

  // Rebuilds the blob state of each feature column family from the files that
  // RepairDB found. Background: every indirect reference stores a generation
  // number, the file number it uses to find its value. After repair, each
  // generation number must be readable in one of two ways:
  // - live: the generation number is a normal blob file or a relocation file
  //   that holds every value that the references with this generation number
  //   point to;
  // - routed: route[generation number] points to a live relocation file that
  //   serves the generation number (its served list has it) and holds all
  //   these values.
  //
  // Steps:
  // 1. Group the references of all SSTs by (column family, generation number)
  //    into repair groups. A reference whose generation number is a relocation
  //    file that the original MANIFEST marks lost, or routes to such a file,
  //    is not grouped: repair keeps the file record, the loss record and the
  //    route. Legacy references are grouped by their blob file.
  // 2. Find the candidate files: normal blob files and relocation files.
  // 3. Keep the blob file of each legacy group. A file with a dropped-file
  //    record, and a missing file, are recorded as dropped instead (see
  //    "Dropped blob files" at the top of this file).
  // 4. Decide each generation number, from the largest to the smallest. A
  //    relocation file serves only older (smaller) file numbers, so every
  //    possible target of a generation number is decided before it. A file
  //    number is usable when it is not a routed generation number and not a
  //    lost file. For generation number G, a reference is unflattened when
  //    its origin is G. The canonical path is the CF's first cf_path. Repair
  //    takes the first row that applies:
  //
  //    Row  Condition                                 Result
  //    1    every reference of G is unflattened, and  keep the record (G is
  //         G has a dropped-file record               dropped)
  //    2    every reference of G is unflattened, G    live: normal blob file G
  //         is usable, and a copy of normal blob
  //         file G passes the checks (see below)
  //    3    G is usable, and relocation file G is in  live: relocation file G
  //         the same CF and in the canonical path
  //         and holds all values of G
  //    4    the newest (highest-numbered) usable      route G -> that file
  //         relocation file in the canonical path
  //         that serves G and holds all its values
  //    5    no row above applies                      route G -> the rebuilt
  //                                                   relocation file (step 5)
  //
  //    Row 1 follows "Dropped blob files" at the top of this file. In row 2,
  //    ScanNormalBlobFile() checks the header and footer, and
  //    ValidateNormalBlobReference() checks every reference of the group.
  //    Repair prefers the copy in the canonical path. It uses a copy in
  //    another path of the CF only if the canonical path has no file named G,
  //    and then copies it to the canonical path. If the canonical path holds
  //    a file G that fails the checks, row 2 does not apply, because repair
  //    never replaces that file before CURRENT commits. Row 5 first adds a
  //    placeholder route (target kInvalidBlobFileNumber), so that G is not
  //    usable as a target of smaller generation numbers. Step 5 replaces it.
  //    A reference that reads from a lost file is not in any group (step 1).
  // 5. Copy the values of the generation numbers that no file can serve into
  //    one rebuilt relocation file per column family, and route these
  //    generation numbers to it.
  Status RepairBlobState() {
    if (feature_format_column_families_.empty()) {
      return Status::OK();
    }
    for (uint32_t cf : feature_format_column_families_) {
      if (vset_.GetColumnFamilySet()->GetColumnFamily(cf) != nullptr) {
        blob_repair_plans_.try_emplace(cf);
      }
    }
    // Step 1. Each repair group holds only (SST, source) pairs, where a source
    // is (encoding class, generation number, origin). The references stay in
    // the runs of the scratch file. A visit reads the runs of a group in
    // sorted order, and reads a reference that several SSTs share only once.
    using GroupKey = std::pair<uint32_t, uint64_t>;
    std::map<GroupKey, std::vector<RepairRunInput>> legacy_groups;
    std::map<GroupKey, std::vector<RepairRunInput>> generation_groups;
    std::set<uint64_t> required_origins;
    for (TableInfo& table : tables_) {
      if (feature_format_column_families_.count(table.column_family_id) == 0) {
        continue;
      }
      table.meta.oldest_blob_file_number = kInvalidBlobFileNumber;
      if (table.meta.blob_reference_descriptor.empty()) {
        table.meta.blob_reference_legacy_metadata =
            table.meta.oldest_coarse_indirect_origin_file_number ==
            kInvalidBlobFileNumber;
      }
      ColumnFamilyBlobRepairPlan& plan =
          blob_repair_plans_[table.column_family_id];
      for (const RepairReferenceSource& source : table.reference_sources) {
        BlobReference reference;
        reference.encoding_class = source.encoding_class;
        reference.generation_file_number = source.generation_file_number;
        reference.origin_file_number = source.origin_file_number;
        const uint64_t lost =
            GetLostRelocationFile(table.column_family_id, reference);
        if (lost != kInvalidBlobFileNumber) {
          plan.relocation_files.emplace(
              lost, lost_relocation_files_.at(lost).addition);
          plan.lost_files.insert(lost);
          if (reference.generation_file_number != lost) {
            plan.routes.emplace(reference.generation_file_number, lost);
          }
          continue;
        }
        if (reference.HasStableIdentity()) {
          generation_groups[{table.column_family_id,
                             reference.generation_file_number}]
              .push_back({&table, source});
        } else {
          legacy_groups[{table.column_family_id, reference.origin_file_number}]
              .push_back({&table, source});
        }
        required_origins.insert(reference.origin_file_number);
      }
    }
    // Step 2.
    NormalBlobFileCandidates normal_files;
    RelocationBlobFileCandidates relocation_files;
    Status status =
        DiscoverBlobFiles(required_origins, &normal_files, &relocation_files);
    if (!status.ok()) {
      return status;
    }
    const RelocationOriginIndex relocation_origins =
        IndexRelocationOrigins(relocation_files);
    const RelocationOriginIndex relocation_served =
        IndexRelocationServedFiles(relocation_files);
    BlobReaderCache normal_cache;
    BlobReaderCache relocation_cache;
    // Keeps normal blob file `selected` for the SSTs of `inputs`.
    const auto keep_normal_file =
        [&](ColumnFamilyBlobRepairPlan* plan, uint64_t selected,
            const NormalBlobFileInfo& chosen,
            const std::vector<RepairRunInput>& inputs) -> Status {
      if (!chosen.is_canonical) {
        Status copied = CopyNormalBlobFileToCanonical(selected, chosen);
        if (!copied.ok()) {
          return copied;
        }
      }
      plan->normal_files.emplace(selected, chosen.addition);
      for (const RepairRunInput& input : inputs) {
        uint64_t& oldest = input.table->meta.oldest_blob_file_number;
        oldest = oldest == kInvalidBlobFileNumber ? selected
                                                  : std::min(oldest, selected);
      }
      return Status::OK();
    };
    // Records missing blob file `dropped` as dropped again for the SSTs of
    // `inputs`. The SSTs link it through oldest_blob_file_number, as they do
    // for a kept file, so that VersionBuilder keeps the record while an SST can
    // use the file.
    const auto keep_dropped_file =
        [&](ColumnFamilyBlobRepairPlan* plan, uint64_t dropped,
            const std::vector<RepairRunInput>& inputs) {
          plan->dropped_files.insert(dropped);
          for (const RepairRunInput& input : inputs) {
            uint64_t& oldest = input.table->meta.oldest_blob_file_number;
            oldest = oldest == kInvalidBlobFileNumber
                         ? dropped
                         : std::min(oldest, dropped);
          }
        };
    // Picks a copy of normal blob file `number` that holds every reference of
    // `cursors`. Each record that a reference names is read with its record CRC
    // checked. An indirect reference is also checked against its stored CRC,
    // and a legacy reference against its SST user key. It prefers the copy in
    // the canonical path (the CF's first cf_path), then the copy with the
    // smallest path. `*chosen` stays null if no copy holds all references.
    const auto find_normal_file =
        [&](uint32_t cf_id, uint64_t number, bool stable,
            const RepairGroupCursors& cursors,
            const NormalBlobFileInfo** chosen) -> Status {
      *chosen = nullptr;
      const auto range = normal_files.equal_range(number);
      for (auto file = range.first; file != range.second; ++file) {
        if (file->second.column_family_id != cf_id) {
          continue;
        }
        Status checked = VisitRepairReferences(
            cursors.specs,
            [&](const BlobReference& reference,
                const std::vector<RepairReferenceCursor*>& owners) -> Status {
              Status valid = ValidateNormalBlobReference(
                  cf_id, reference, file->second, nullptr,
                  NormalBlobValidationMode::kPublishedFile, &normal_cache);
              if (valid.ok() && !stable) {
                for (RepairReferenceCursor* cursor : owners) {
                  valid = cursor->VerifyLegacyUserKey(
                      reference, normal_cache.checked_user_key);
                  if (!valid.ok()) {
                    break;
                  }
                }
              }
              return valid;
            });
        if (checked.ok()) {
          if (*chosen == nullptr || file->second.is_canonical ||
              (!(*chosen)->is_canonical &&
               file->second.path < (*chosen)->path)) {
            *chosen = &file->second;
          }
          if ((*chosen)->is_canonical) {
            break;
          }
        } else if (!checked.IsCorruption() && !checked.IsNotSupported()) {
          return checked;
        }
      }
      return Status::OK();
    };
    // Frees every run when this function returns. This also frees the runs of
    // column families without a repair plan, which no step reads.
    const auto free_all_runs = Defer([&]() {
      // Only a test sync point reads this, so it is unused in release builds.
      [[maybe_unused]] size_t retained_run_bytes = 0;
      for (TableInfo& table : tables_) {
        for (const auto& run : table.runs) {
          retained_run_bytes +=
              run.second.pending.capacity() +
              run.second.chunks.capacity() * sizeof(run.second.chunks.front());
        }
        table.runs.clear();
      }
      TEST_SYNC_POINT_CALLBACK("Repairer::RepairBlobState:RetainedRunBytes",
                               &retained_run_bytes);
    });

    // Step 3. A legacy reference has no route: its blob file must be kept.
    for (const auto& group : legacy_groups) {
      const uint32_t cf_id = group.first.first;
      const uint64_t file_number = group.first.second;
      RepairGroupCursors cursors;
      const auto free_group_runs = Defer([&]() {
        DeleteRepairScratchFile(&cursors.scratch).PermitUncheckedError();
        for (const RepairRunInput& input : group.second) {
          input.table->runs.erase(input.source);
        }
      });
      if (IsDurablyDropped(cf_id, file_number)) {
        // The dropped-file record wins over any copy on disk (see "Dropped
        // blob files" at the top of this file).
        keep_dropped_file(&blob_repair_plans_[cf_id], file_number,
                          group.second);
        continue;
      }
      status = PrepareGroupCursors(group.second, &cursors);
      if (!status.ok()) {
        return status;
      }
      const NormalBlobFileInfo* chosen = nullptr;
      status = find_normal_file(cf_id, file_number, /*stable=*/false, cursors,
                                &chosen);
      if (!status.ok()) {
        return status;
      }
      if (chosen == nullptr &&
          blob_file_numbers_on_disk_.count(file_number) == 0) {
        // The blob file is missing and has no dropped-file record. Repair
        // records it as dropped (see "Dropped blob files" at the top of this
        // file).
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Repair records missing legacy blob file #%" PRIu64
                       " as dropped",
                       file_number);
        keep_dropped_file(&blob_repair_plans_[cf_id], file_number,
                          group.second);
        continue;
      }
      if (chosen == nullptr) {
        return Status::Corruption(
            "Repair cannot find a valid required legacy blob file");
      }
      status = keep_normal_file(&blob_repair_plans_[cf_id], file_number,
                                *chosen, group.second);
      if (!status.ok()) {
        return status;
      }
    }

    // Step 4. Generation numbers that no file can serve wait for step 5, per
    // CF.
    std::map<uint32_t, std::vector<uint64_t>> rebuilt_generations;
    std::map<uint32_t, std::vector<RepairRunInput>> rebuilt_inputs;
    for (auto group = generation_groups.rbegin();
         group != generation_groups.rend(); ++group) {
      const uint32_t cf_id = group->first.first;
      const uint64_t generation = group->first.second;
      ColumnFamilyBlobRepairPlan& plan = blob_repair_plans_[cf_id];
      // True if every reference of generation number `generation` is
      // unflattened (origin == generation), so normal blob file `generation`
      // can hold all of them.
      bool unflattened = true;
      for (const RepairRunInput& input : group->second) {
        unflattened &= input.source.origin_file_number == generation;
      }
      RepairGroupCursors cursors;
      bool keep_runs = false;
      const auto free_group_runs = Defer([&]() {
        DeleteRepairScratchFile(&cursors.scratch).PermitUncheckedError();
        if (keep_runs) {
          return;
        }
        for (const RepairRunInput& input : group->second) {
          input.table->runs.erase(input.source);
        }
      });
      if (unflattened && IsDurablyDropped(cf_id, generation)) {
        // The dropped-file record wins over any copy of the origin and over
        // any relocation file on disk (see "Dropped blob files" at the top of
        // this file).
        keep_dropped_file(&plan, generation, group->second);
        continue;
      }
      status = PrepareGroupCursors(group->second, &cursors);
      if (!status.ok()) {
        return status;
      }
      // A file can be a live generation number or a route target only if it is
      // not a routed generation number and not lost.
      const auto usable = [&](uint64_t file_number) {
        return plan.routes.count(file_number) == 0 &&
               plan.lost_files.count(file_number) == 0;
      };
      uint64_t selected = kInvalidBlobFileNumber;
      if (unflattened && usable(generation)) {
        const NormalBlobFileInfo* chosen = nullptr;
        // A verified copy at another path is copied to the canonical path, as
        // for a legacy blob file in step 3. find_normal_file() checks every
        // reference of the group against the copy.
        status = find_normal_file(cf_id, generation, /*stable=*/true, cursors,
                                  &chosen);
        if (!status.ok()) {
          return status;
        }
        if (chosen != nullptr && !chosen->is_canonical) {
          // A file in the canonical path that is not a valid copy is never
          // replaced before CURRENT commits (see
          // CopyNormalBlobFileToCanonical()). The generation number then
          // routes to a relocation file, which can copy the values from the
          // good copy.
          ColumnFamilyData* const cfd =
              vset_.GetColumnFamilySet()->GetColumnFamily(cf_id);
          if (cfd == nullptr || cfd->ioptions().cf_paths.empty()) {
            return Status::Corruption("Missing canonical blob file path");
          }
          const Status exists = env_->FileExists(
              BlobFileName(cfd->ioptions().cf_paths.front().path, generation));
          if (exists.ok()) {
            chosen = nullptr;
          } else if (!exists.IsNotFound()) {
            return exists;
          }
        }
        if (chosen != nullptr) {
          status = keep_normal_file(&plan, generation, *chosen, group->second);
          if (!status.ok()) {
            return status;
          }
          selected = generation;
        }
      }
      // The relocation file `generation` itself, then the files that serve
      // generation number `generation`.
      std::vector<const RelocationBlobFileCandidates::value_type*> candidates;
      if (selected == kInvalidBlobFileNumber && usable(generation)) {
        const auto own = relocation_files.equal_range(generation);
        for (auto file = own.first; file != own.second; ++file) {
          if (file->second.column_family_id == cf_id) {
            candidates.push_back(&*file);
          }
        }
      }
      if (selected == kInvalidBlobFileNumber) {
        // Newest first: a newer file that serves the generation number replaced
        // the older ones, which hold more garbage.
        const auto serving =
            relocation_served.equal_range(std::make_pair(cf_id, generation));
        for (auto entry = std::make_reverse_iterator(serving.second);
             entry != std::make_reverse_iterator(serving.first); ++entry) {
          if (usable(entry->second->first)) {
            candidates.push_back(entry->second);
          }
        }
      }
      for (const auto* file : candidates) {
        if (selected != kInvalidBlobFileNumber) {
          break;
        }
        if (!file->second.is_canonical) {
          continue;
        }
        BlobFileReader* reader = nullptr;
        status = GetRelocationBlobReader(file->first, file->second,
                                         &relocation_cache, &reader);
        if (status.ok()) {
          status = VisitRepairReferences(
              cursors.specs, [&](const BlobReference& reference,
                                 const std::vector<RepairReferenceCursor*>&) {
                return ValidateRelocationBlobReference(reference, reader);
              });
        }
        if (status.ok()) {
          selected = file->first;
          plan.relocation_files.emplace(selected, file->second.addition);
          if (selected != generation) {
            plan.routes.emplace(generation, selected);
          }
        } else if (!status.IsCorruption() && !status.IsNotSupported()) {
          return status;
        }
      }
      if (selected == kInvalidBlobFileNumber) {
        // Step 5 reads these runs again. Add a placeholder route (target
        // kInvalidBlobFileNumber) now, so that usable() rejects this
        // generation number as a target of smaller ones. Step 5 replaces the
        // placeholder with the rebuilt file.
        keep_runs = true;
        rebuilt_generations[cf_id].push_back(generation);
        rebuilt_inputs[cf_id].insert(rebuilt_inputs[cf_id].end(),
                                     group->second.begin(),
                                     group->second.end());
        plan.routes.emplace(generation, kInvalidBlobFileNumber);
      }
    }

    // Step 5.
    for (auto& cf : rebuilt_generations) {
      const uint32_t cf_id = cf.first;
      ColumnFamilyBlobRepairPlan& plan = blob_repair_plans_[cf_id];
      const std::vector<RepairRunInput>& inputs = rebuilt_inputs[cf_id];
      RepairGroupCursors cursors;
      const auto free_group_runs = Defer([&]() {
        DeleteRepairScratchFile(&cursors.scratch).PermitUncheckedError();
      });
      status = PrepareGroupCursors(inputs, &cursors);
      if (!status.ok()) {
        return status;
      }
      RepairRelocationPack pack;
      const auto abandon_open_pack = Defer([&]() {
        if (pack.IsOpen()) {
          AbandonRepairRelocationPack(&pack);
        }
      });
      status = OpenRepairRelocationPack(cf_id, &pack);
      if (status.ok()) {
        status = AddRepairRelocationGenerations(
            cf.second, cursors.specs, normal_files, relocation_origins,
            &normal_cache, &relocation_cache, &pack);
      }
      BlobRelocationFileAddition rebuilt;
      if (status.ok()) {
        status = FinishRepairRelocationPack(&pack, &rebuilt);
      }
      if (!status.ok()) {
        return status;
      }
      const uint64_t number = rebuilt.GetFileNumber();
      plan.relocation_files.emplace(number, std::move(rebuilt));
      for (uint64_t generation : cf.second) {
        plan.routes[generation] = number;
      }
    }
    // One summary line per column family, so an operator can see what the
    // repair kept, rebuilt and gave up.
    for (const auto& entry : blob_repair_plans_) {
      const ColumnFamilyData* const cfd =
          vset_.GetColumnFamilySet()->GetColumnFamily(entry.first);
      const ColumnFamilyBlobRepairPlan& summary_plan = entry.second;
      ROCKS_LOG_WARN(db_options_.info_log,
                     "[%s] RepairDB blob state: %" ROCKSDB_PRIszt
                     " blob files kept, %" ROCKSDB_PRIszt
                     " relocation files (%" ROCKSDB_PRIszt
                     " rebuilt), %" ROCKSDB_PRIszt " routes, %" ROCKSDB_PRIszt
                     " lost files, %" ROCKSDB_PRIszt " dropped blob files",
                     cfd != nullptr ? cfd->GetName().c_str() : "?",
                     summary_plan.normal_files.size(),
                     summary_plan.relocation_files.size(),
                     rebuilt_generations.count(entry.first),
                     summary_plan.routes.size(), summary_plan.lost_files.size(),
                     summary_plan.dropped_files.size());
    }
    if (reference_scratch_ != nullptr) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Repair blob reference runs used %" PRIu64
                     " bytes of temporary space",
                     reference_scratch_->size);
      status = DeleteRepairScratchFile(&reference_scratch_);
    }
    return status;
  }

  Status AddTables() {
    if (next_file_number_ == 0 ||
        next_file_number_ >= std::numeric_limits<uint64_t>::max() - 1) {
      return Status::Corruption("Repair next file number is exhausted");
    }
    // TODO: plumb Env::IOActivity, Env::IOPriority;
    const ReadOptions read_options;
    const WriteOptions write_options;

    std::unordered_map<uint32_t, std::vector<const TableInfo*>> cf_id_to_tables;
    SequenceNumber max_sequence = feature_repair_ ? certified_max_sequence_ : 0;
    if (feature_repair_) {
      for (ColumnFamilyData* cfd : *vset_.GetColumnFamilySet()) {
        cf_id_to_tables.try_emplace(cfd->GetID());
      }
      if (certified_wal_floor_ != 0) {
        Status status = TrackDiscoveredFileNumber(certified_wal_floor_);
        if (!status.ok()) {
          return status;
        }
      }
    }
    for (size_t i = 0; i < tables_.size(); i++) {
      cf_id_to_tables[tables_[i].column_family_id].push_back(&tables_[i]);
      if (max_sequence < tables_[i].meta.fd.largest_seqno) {
        max_sequence = tables_[i].meta.fd.largest_seqno;
      }
    }
    vset_.SetLastAllocatedSequence(max_sequence);
    vset_.SetLastPublishedSequence(max_sequence);
    vset_.SetLastSequence(max_sequence);

    for (const auto& cf_id_and_tables : cf_id_to_tables) {
      auto* cfd =
          vset_.GetColumnFamilySet()->GetColumnFamily(cf_id_and_tables.first);

      // Recover files' epoch number using dummy VersionStorageInfo
      VersionBuilder dummy_version_builder(
          cfd->current()->version_set()->file_options(), &cfd->ioptions(),
          cfd->table_cache(), cfd->current()->storage_info(),
          cfd->current()->version_set(),
          cfd->GetFileMetadataCacheReservationManager());
      VersionStorageInfo dummy_vstorage(
          &cfd->internal_comparator(), cfd->user_comparator(),
          cfd->NumberLevels(), cfd->ioptions().compaction_style,
          nullptr /* src_vstorage */, cfd->ioptions().force_consistency_checks,
          EpochNumberRequirement::kMightMissing, cfd->ioptions().clock,
          /*bottommost_file_compaction_delay=*/0,
          cfd->current()->version_set()->offpeak_time_option(),
          PeriodicCompactionPhaseParams{});
      Status s;
      VersionEdit dummy_edit;
      const auto repair_plan = blob_repair_plans_.find(cfd->GetID());
      if (repair_plan != blob_repair_plans_.end()) {
        dummy_edit.SetBlobIndirectionFormatVersion(
            kBlobIndirectionManifestFormatVersion);
      }
      for (const auto* table : cf_id_and_tables.second) {
        // TODO(opt): separate out into multiple levels
        dummy_edit.AddFile(0, table->meta);
      }
      s = dummy_version_builder.Apply(&dummy_edit);
      if (s.ok()) {
        s = dummy_version_builder.SaveTo(&dummy_vstorage);
      }
      if (s.ok()) {
        dummy_vstorage.RecoverEpochNumbers(cfd);
      }
      if (s.ok()) {
        // Record changes from this repair in VersionEdit, including files with
        // recovered epoch numbers
        VersionEdit edit;
        edit.SetComparatorName(cfd->user_comparator()->Name());
        edit.SetPersistUserDefinedTimestamps(
            cfd->ioptions().persist_user_defined_timestamps);
        edit.SetLogNumber(
            feature_repair_ ? std::max(certified_wal_floor_,
                                       proven_original_cf_floors_[cfd->GetID()])
                            : 0);
        edit.SetNextFile(next_file_number_);
        edit.SetColumnFamily(cfd->GetID());
        if (feature_repair_) {
          // The staged MANIFEST must record the certified last sequence, which
          // can be larger than every SST sequence (for example after a
          // bottommost compaction set them to 0). The repair catalog checks it
          // before CURRENT changes.
          edit.SetLastSequence(max_sequence);
        }
        // Versions drop blob reference summaries; the census install needs
        // the scanned ones back on the edit's new files.
        std::map<uint64_t, std::shared_ptr<const BlobReferenceSstSummary>>
            summaries;
        for (const auto* table : cf_id_and_tables.second) {
          if (table->meta.blob_reference_summary != nullptr) {
            summaries.emplace(table->meta.fd.GetNumber(),
                              table->meta.blob_reference_summary);
          }
        }
        for (int level = 0; level < dummy_vstorage.num_levels(); ++level) {
          for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
            edit.AddFile(level, *file_meta);
            const auto summary = summaries.find(file_meta->fd.GetNumber());
            if (summary != summaries.end()) {
              edit.GetMutableNewFiles().back().second.blob_reference_summary =
                  summary->second;
            }
          }
        }
        if (repair_plan != blob_repair_plans_.end()) {
          edit.SetBlobIndirectionFormatVersion(
              kBlobIndirectionManifestFormatVersion);
          for (const auto& addition : repair_plan->second.normal_files) {
            edit.AddBlobFile(addition.second);
          }
          for (const auto& addition : repair_plan->second.relocation_files) {
            edit.AddBlobRelocationFile(addition.second);
          }
          for (uint64_t file : repair_plan->second.lost_files) {
            edit.MarkBlobPhysicalFileLost(file);
          }
          for (uint64_t file : repair_plan->second.dropped_files) {
            edit.AddDroppedBlobFile(file);
          }
          for (const auto& route : repair_plan->second.routes) {
            edit.SetBlobRelocationRoute(
                BlobRelocationRoute(route.first, route.second));
          }
        }

        // Release resources occupied by the dummy VersionStorageInfo
        for (int level = 0; level < dummy_vstorage.num_levels(); ++level) {
          for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
            file_meta->refs--;
            if (file_meta->refs <= 0) {
              delete file_meta;
            }
          }
        }

        // Persist record of changes
        assert(next_file_number_ > 0);
        vset_.MarkFileNumberUsed(next_file_number_ - 1);
        mutex_.Lock();
        std::unique_ptr<FSDirectory> db_dir;
        s = env_->GetFileSystem()->NewDirectory(dbname_, IOOptions(), &db_dir,
                                                nullptr);
        if (s.ok()) {
          s = vset_.LogAndApply(cfd, read_options, write_options, &edit,
                                &mutex_, db_dir.get(),
                                false /* new_descriptor_log */);
        }
        mutex_.Unlock();
      }
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  }

  void ArchiveFile(const std::string& fname) {
    // Move into another directory.  E.g., for
    //    dir/foo
    // rename to
    //    dir/lost/foo
    const char* slash = strrchr(fname.c_str(), '/');
    std::string new_dir;
    if (slash != nullptr) {
      new_dir.assign(fname.data(), slash - fname.data());
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir).PermitUncheckedError();  // Ignore error
    std::string new_file = new_dir;
    new_file.append("/");
    new_file.append((slash == nullptr) ? fname.c_str() : slash + 1);
    Status s = env_->RenameFile(fname, new_file);
    ROCKS_LOG_INFO(db_options_.info_log, "Archiving %s: %s\n", fname.c_str(),
                   s.ToString().c_str());
  }
};

Status GetDefaultCFOptions(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    ColumnFamilyOptions* res) {
  assert(res != nullptr);
  auto iter = std::find_if(column_families.begin(), column_families.end(),
                           [](const ColumnFamilyDescriptor& cfd) {
                             return cfd.name == kDefaultColumnFamilyName;
                           });
  if (iter == column_families.end()) {
    return Status::InvalidArgument(
        "column_families", "Must contain entry for default column family");
  }
  *res = iter->options;
  return Status::OK();
}
}  // anonymous namespace

Status RepairDB(const std::string& dbname, const DBOptions& db_options,
                const std::vector<ColumnFamilyDescriptor>& column_families) {
  ColumnFamilyOptions default_cf_opts;
  Status status = GetDefaultCFOptions(column_families, &default_cf_opts);
  if (!status.ok()) {
    return status;
  }

  Repairer repairer(dbname, db_options, column_families, default_cf_opts,
                    ColumnFamilyOptions() /* unknown_cf_opts */,
                    false /* create_unknown_cfs */);
  status = repairer.Run();
  if (status.ok()) {
    status = repairer.Close();
  }
  return status;
}

Status RepairDB(const std::string& dbname, const DBOptions& db_options,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                const ColumnFamilyOptions& unknown_cf_opts) {
  ColumnFamilyOptions default_cf_opts;
  Status status = GetDefaultCFOptions(column_families, &default_cf_opts);
  if (!status.ok()) {
    return status;
  }

  Repairer repairer(dbname, db_options, column_families, default_cf_opts,
                    unknown_cf_opts, true /* create_unknown_cfs */);
  status = repairer.Run();
  if (status.ok()) {
    status = repairer.Close();
  }
  return status;
}

Status RepairDB(const std::string& dbname, const Options& options) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);

  Repairer repairer(dbname, db_options, {}, cf_options /* default_cf_opts */,
                    cf_options /* unknown_cf_opts */,
                    true /* create_unknown_cfs */);
  Status status = repairer.Run();
  if (status.ok()) {
    status = repairer.Close();
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
