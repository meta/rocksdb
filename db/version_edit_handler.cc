//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit_handler.h"

#include <cinttypes>
#include <sstream>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_source.h"
#include "db/version_edit.h"
#include "logging/logging.h"
#include "monitoring/persistent_stats_history.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

void VersionEditHandlerBase::Iterate(log::Reader& reader,
                                     Status* log_read_status) {
  Slice record;
  std::string scratch;
  assert(log_read_status);
  assert(log_read_status->ok());

  [[maybe_unused]] size_t recovered_edits = 0;
  Status s = Initialize();
  while (reader.LastRecordEnd() < max_manifest_read_size_ && s.ok() &&
         reader.ReadRecord(&record, &scratch) && log_read_status->ok()) {
    VersionEdit edit;
    s = DecodeVersionEdit(record, &edit);
    if (s.ok()) {
      s = read_buffer_.AddEdit(&edit);
    }
    if (s.ok()) {
      ColumnFamilyData* cfd = nullptr;
      if (edit.IsInAtomicGroup()) {
        if (read_buffer_.IsFull()) {
          last_valid_record_end_ = reader.LastRecordEnd();
          s = OnAtomicGroupReplayBegin();
          for (size_t i = 0; s.ok() && i < read_buffer_.replay_buffer().size();
               i++) {
            // SyncPoint's singleton has process lifetime.
            // @lint-ignore NULLSAFECLANG nullable-dereference
            TEST_SYNC_POINT_CALLBACK(
                "VersionEditHandlerBase::Iterate:BeforeApplyAtomicGroupEdit",
                &s);
            if (!s.ok()) {
              break;
            }
            auto& e = read_buffer_.replay_buffer()[i];
            s = ApplyVersionEdit(e, &cfd);
            if (s.ok()) {
              recovered_edits++;
            }
          }
          if (s.ok()) {
            read_buffer_.Clear();
            s = OnAtomicGroupReplayEnd();
          }
        }
      } else {
        last_valid_record_end_ = reader.LastRecordEnd();
        s = ApplyVersionEdit(edit, &cfd);
        if (s.ok()) {
          recovered_edits++;
        }
      }
    }
  }
  if (s.ok() && !log_read_status->ok()) {
    s = *log_read_status;
  }

  CheckIterationResult(reader, &s);

  if (!s.ok()) {
    if (s.IsCorruption()) {
      // when we find a Corruption error, something is
      // wrong with the underlying file. in this case we
      // want to report the filename, so in here we append
      // the filename to the Corruption message
      assert(reader.file());

      // build a new error message
      std::stringstream message;
      // append previous dynamic state message
      const char* state = s.getState();
      if (state != nullptr) {
        message << state;
        message << ' ';
      }
      // append the filename to the corruption message
      message << " The file " << reader.file()->file_name()
              << " may be corrupted.";
      // overwrite the status with the extended status
      s = Status(s.code(), s.subcode(), s.severity(), message.str());
    }
    status_ = s;
  }
  TEST_SYNC_POINT_CALLBACK("VersionEditHandlerBase::Iterate:Finish",
                           &recovered_edits);
}

Status VersionEditHandlerBase::DecodeVersionEdit(const Slice& record,
                                                 VersionEdit* edit) {
  if (edit == nullptr) {
    return Status::InvalidArgument("null VersionEdit output");
  }
  return edit->DecodeFrom(record);
}

Status ListColumnFamiliesHandler::ApplyVersionEdit(
    VersionEdit& edit, ColumnFamilyData** /*unused*/) {
  Status s;
  uint32_t cf_id = edit.GetColumnFamily();
  if (edit.IsColumnFamilyAdd()) {
    if (column_family_names_.find(cf_id) != column_family_names_.end()) {
      s = Status::Corruption("Manifest adding the same column family twice");
    } else {
      column_family_names_.insert({cf_id, edit.GetColumnFamilyName()});
    }
  } else if (edit.IsColumnFamilyDrop()) {
    if (column_family_names_.find(cf_id) == column_family_names_.end()) {
      s = Status::Corruption("Manifest - dropping non-existing column family");
    } else {
      column_family_names_.erase(cf_id);
    }
  }
  return s;
}

Status FileChecksumRetriever::FetchFileChecksumList(
    FileChecksumList& file_checksum_list) {
  Status s = Status::OK();
  for (const auto& [cf, file_checksums] : cf_file_checksums_) {
    [[maybe_unused]] const auto& _ = cf;
    for (const auto& [file_number, info] : file_checksums) {
      if (!(s = file_checksum_list.InsertOneFileChecksum(
                file_number, info.first, info.second))
               .ok()) {
        break;
      }
    }
  }
  return s;
}

Status FileChecksumRetriever::ApplyVersionEdit(VersionEdit& edit,
                                               ColumnFamilyData** /*unused*/) {
  uint32_t column_family_id = edit.GetColumnFamily();
  if (edit.IsColumnFamilyDrop()) {
    cf_file_checksums_.erase(column_family_id);
  }
  for (const auto& deleted_file : edit.GetDeletedFiles()) {
    if (cf_file_checksums_.find(column_family_id) == cf_file_checksums_.end()) {
      return Status::NotFound();
    }
    if (cf_file_checksums_[column_family_id].find(deleted_file.second) ==
        cf_file_checksums_[column_family_id].end()) {
      return Status::NotFound();
    }
    cf_file_checksums_[column_family_id].erase(deleted_file.second);
  }
  for (const auto& new_file : edit.GetNewFiles()) {
    cf_file_checksums_[column_family_id].emplace(
        new_file.second.fd.GetNumber(),
        std::make_pair(new_file.second.file_checksum,
                       new_file.second.file_checksum_func_name));
  }
  for (const auto& new_blob_file : edit.GetBlobFileAdditions()) {
    std::string checksum_value = new_blob_file.GetChecksumValue();
    std::string checksum_method = new_blob_file.GetChecksumMethod();
    assert(checksum_value.empty() == checksum_method.empty());
    if (checksum_method.empty()) {
      checksum_value = kUnknownFileChecksum;
      checksum_method = kUnknownFileChecksumFuncName;
    }
    cf_file_checksums_[column_family_id].emplace(
        new_blob_file.GetBlobFileNumber(),
        std::make_pair(checksum_value, checksum_method));
  }
  for (uint64_t file_number : edit.GetBlobRelocationFileDeletions()) {
    const auto cf = cf_file_checksums_.find(column_family_id);
    if (cf != cf_file_checksums_.end()) {
      cf->second.erase(file_number);
    }
  }
  for (const BlobRelocationFileAddition& addition :
       edit.GetBlobRelocationFileAdditions()) {
    std::string checksum_value = addition.GetChecksumValue();
    std::string checksum_method = addition.GetChecksumMethod();
    if (checksum_method.empty()) {
      checksum_value = kUnknownFileChecksum;
      checksum_method = kUnknownFileChecksumFuncName;
    }
    cf_file_checksums_[column_family_id].emplace(
        addition.GetFileNumber(),
        std::make_pair(std::move(checksum_value), std::move(checksum_method)));
  }
  return Status::OK();
}

VersionEditHandler::VersionEditHandler(
    bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
    VersionSet* version_set, bool track_found_and_missing_files,
    bool no_error_if_files_missing, const std::shared_ptr<IOTracer>& io_tracer,
    const ReadOptions& read_options, bool skip_load_table_files,
    bool allow_incomplete_valid_version,
    EpochNumberRequirement epoch_number_requirement)
    : VersionEditHandlerBase(read_options),
      read_only_(read_only),
      column_families_(std::move(column_families)),
      version_set_(version_set),
      track_found_and_missing_files_(track_found_and_missing_files),
      no_error_if_files_missing_(no_error_if_files_missing),
      io_tracer_(io_tracer),
      skip_load_table_files_(skip_load_table_files),
      initialized_(false),
      allow_incomplete_valid_version_(allow_incomplete_valid_version),
      epoch_number_requirement_(epoch_number_requirement) {
  assert(version_set_ != nullptr);
}

Status VersionEditHandler::Initialize() {
  Status s;
  if (!initialized_) {
    for (const auto& cf_desc : column_families_) {
      name_to_options_.emplace(cf_desc.name, cf_desc.options);
    }
    auto default_cf_iter = name_to_options_.find(kDefaultColumnFamilyName);
    if (default_cf_iter == name_to_options_.end()) {
      s = Status::InvalidArgument("Default column family not specified");
    }
    if (s.ok()) {
      VersionEdit default_cf_edit;
      default_cf_edit.AddColumnFamily(kDefaultColumnFamilyName);
      default_cf_edit.SetColumnFamily(0);
      ColumnFamilyData* cfd =
          CreateCfAndInit(default_cf_iter->second, default_cf_edit);
      assert(cfd != nullptr);
#ifdef NDEBUG
      (void)cfd;
#endif
      initialized_ = true;
    }
  }
  return s;
}

Status VersionEditHandler::ApplyVersionEdit(VersionEdit& edit,
                                            ColumnFamilyData** cfd) {
  Status s = TrackUnopenedPhysicalFileOwnership(edit);
  if (!s.ok()) {
    return s;
  }
  if (edit.IsColumnFamilyAdd()) {
    s = OnColumnFamilyAdd(edit, cfd);
  } else if (edit.IsColumnFamilyDrop()) {
    s = OnColumnFamilyDrop(edit, cfd);
  } else if (edit.IsWalAddition()) {
    s = OnWalAddition(edit);
  } else if (edit.IsWalDeletion()) {
    s = OnWalDeletion(edit);
  } else {
    s = OnNonCfOperation(edit, cfd);
  }
  if (s.ok()) {
    assert(cfd != nullptr);
    s = ExtractInfoFromVersionEdit(*cfd, edit);
  }
  return s;
}

Status VersionEditHandler::TrackUnopenedPhysicalFileOwnership(
    const VersionEdit& edit) {
  if (edit.IsColumnFamilyAdd() || edit.IsWalAddition() ||
      edit.IsWalDeletion()) {
    return Status::OK();
  }

  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);
  if (!do_not_open_cf) {
    return Status::OK();
  }

  const uint32_t column_family_id = edit.GetColumnFamily();
  if (edit.IsColumnFamilyDrop()) {
    unopened_physical_file_owners_.erase(column_family_id);
    return Status::OK();
  }

  UnopenedPhysicalFileKinds& files =
      unopened_physical_file_owners_[column_family_id];
  const auto erase = [&](uint64_t file_number,
                         VersionSet::PhysicalFileOwnerKind kind) {
    const auto file = files.find(file_number);
    if (file == files.end()) {
      return;
    }
    file->second.erase(kind);
    if (file->second.empty()) {
      files.erase(file);
    }
  };
  const auto record = [&](uint64_t file_number,
                          VersionSet::PhysicalFileOwnerKind kind) {
    files[file_number].insert(kind);
  };

  for (const auto& deleted_file : edit.GetDeletedFiles()) {
    erase(deleted_file.second, VersionSet::PhysicalFileOwnerKind::kTable);
  }
  for (const BlobFileGarbage& garbage : edit.GetBlobFileGarbages()) {
    // A garbage record does not prove that the physical blob file is gone.
    record(garbage.GetBlobFileNumber(),
           VersionSet::PhysicalFileOwnerKind::kBlob);
  }
  for (uint64_t file_number : edit.GetBlobRelocationFileDeletions()) {
    // Relocation IDs remain owned after deletion because retirement is
    // durable and prevents identity reuse.
    record(file_number, VersionSet::PhysicalFileOwnerKind::kBlobRelocation);
  }
  for (const auto& new_file : edit.GetNewFiles()) {
    record(new_file.second.fd.GetNumber(),
           VersionSet::PhysicalFileOwnerKind::kTable);
  }
  for (const BlobFileAddition& addition : edit.GetBlobFileAdditions()) {
    record(addition.GetBlobFileNumber(),
           VersionSet::PhysicalFileOwnerKind::kBlob);
  }
  for (const BlobRelocationFileAddition& addition :
       edit.GetBlobRelocationFileAdditions()) {
    if (addition.GetColumnFamilyId() != column_family_id) {
      return Status::Corruption(
          "Blob relocation file belongs to another column family");
    }
    record(addition.GetFileNumber(),
           VersionSet::PhysicalFileOwnerKind::kBlobRelocation);
  }
  return Status::OK();
}

Status VersionEditHandler::AddUnopenedPhysicalFileOwnership(
    VersionSet::PhysicalFileOwnershipIndex* index) const {
  if (index == nullptr) {
    return Status::InvalidArgument("null physical ownership output");
  }
  assert(version_set_ != nullptr);
  // VersionEditHandler's constructor requires a non-null VersionSet.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  const bool enforce_uniqueness =
      version_set_->blob_indirection_format_version_ != 0;
  for (const auto& cf_entry : unopened_physical_file_owners_) {
    const uint32_t column_family_id = cf_entry.first;
    for (const auto& file_entry : cf_entry.second) {
      const uint64_t file_number = file_entry.first;
      for (VersionSet::PhysicalFileOwnerKind kind : file_entry.second) {
        const VersionSet::PhysicalFileOwner owner{column_family_id, kind};
        const auto result = index->emplace(file_number, owner);
        if (result.second ||
            (result.first->second.column_family_id == column_family_id &&
             result.first->second.kind == kind)) {
          continue;
        }
        if (enforce_uniqueness) {
          return Status::Corruption(
              "Physical file number has conflicting ownership in an "
              "unopened column family");
        }
      }
    }
  }
  return Status::OK();
}

Status VersionEditHandler::RebuildPhysicalFileOwnershipIndex() {
  assert(version_set_ != nullptr);
  // VersionEditHandler's constructor requires a non-null VersionSet.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  VersionSet& version_set = *version_set_;
  VersionSet::PhysicalFileOwnershipIndex rebuilt;
  Status status = version_set.BuildPhysicalFileOwnershipIndex(
      {}, /*db_mutex=*/nullptr, &rebuilt);
  if (status.ok()) {
    status = AddUnopenedPhysicalFileOwnership(&rebuilt);
  }
  if (status.ok()) {
    version_set.physical_file_owners_.swap(rebuilt);
  }
  return status;
}

Status VersionEditHandler::OnColumnFamilyAdd(VersionEdit& edit,
                                             ColumnFamilyData** cfd) {
  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  const std::string& cf_name = edit.GetColumnFamilyName();
  Status s;
  if (cf_in_builders || do_not_open_cf) {
    s = Status::Corruption("MANIFEST adding the same column family twice: " +
                           cf_name);
  }
  if (s.ok()) {
    auto cf_options = name_to_options_.find(cf_name);
    // implicitly add persistent_stats column family without requiring user
    // to specify
    ColumnFamilyData* tmp_cfd = nullptr;
    bool is_persistent_stats_column_family =
        cf_name.compare(kPersistentStatsColumnFamilyName) == 0;
    if (cf_options == name_to_options_.end() &&
        !is_persistent_stats_column_family) {
      do_not_open_column_families_.emplace(edit.GetColumnFamily(), cf_name);
    } else {
      if (is_persistent_stats_column_family) {
        ColumnFamilyOptions cfo;
        OptimizeForPersistentStats(&cfo);
        tmp_cfd = CreateCfAndInit(cfo, edit);
      } else {
        tmp_cfd = CreateCfAndInit(cf_options->second, edit);
      }
      *cfd = tmp_cfd;
    }
  }
  return s;
}

Status VersionEditHandler::OnColumnFamilyDrop(VersionEdit& edit,
                                              ColumnFamilyData** cfd) {
  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  ColumnFamilyData* tmp_cfd = nullptr;
  Status s;
  if (cf_in_builders) {
    auto builder = builders_.find(edit.GetColumnFamily());
    assert(builder != builders_.end());
    s = ValidateVersionBuilderBeforeDrop(builder->second->version_builder());
    if (s.ok()) {
      tmp_cfd = DestroyCfAndCleanup(edit);
    }
  } else if (do_not_open_cf) {
    do_not_open_column_families_.erase(edit.GetColumnFamily());
  } else {
    s = Status::Corruption("MANIFEST - dropping non-existing column family");
  }
  *cfd = tmp_cfd;
  return s;
}

Status VersionEditHandler::OnWalAddition(VersionEdit& edit) {
  assert(edit.IsWalAddition());
  return version_set_->wals_.AddWals(edit.GetWalAdditions());
}

Status VersionEditHandler::OnWalDeletion(VersionEdit& edit) {
  assert(edit.IsWalDeletion());
  return version_set_->wals_.DeleteWalsBefore(
      edit.GetWalDeletion().GetLogNumber());
}

Status VersionEditHandler::OnNonCfOperation(VersionEdit& edit,
                                            ColumnFamilyData** cfd) {
  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  Status s;
  if (!do_not_open_cf) {
    if (!cf_in_builders) {
      s = Status::Corruption(
          "MANIFEST record referencing unknown column family");
    }
    ColumnFamilyData* tmp_cfd = nullptr;
    if (s.ok()) {
      tmp_cfd = version_set_->GetColumnFamilySet()->GetColumnFamily(
          edit.GetColumnFamily());
      assert(tmp_cfd != nullptr);
      // It's important to handle file boundaries before `MaybeCreateVersion`
      // because `VersionEditHandlerPointInTime::MaybeCreateVersion` does
      // `FileMetaData` verification that involves the file boundaries.
      // All `VersionEditHandlerBase` subclasses that need to deal with
      // `FileMetaData` for new files are also subclasses of
      // `VersionEditHandler`, so it's sufficient to do the file boundaries
      // handling in this method.
      s = MaybeHandleFileBoundariesForNewFiles(edit, tmp_cfd);
      if (!s.ok()) {
        return s;
      }
      s = MaybeCreateVersionBeforeApplyEdit(edit, tmp_cfd,
                                            /*force_create_version=*/false);
    }
    *cfd = tmp_cfd;
  }
  return s;
}

void VersionEditHandler::CheckColumnFamilyId(const VersionEdit& edit,
                                             bool* do_not_open_cf,
                                             bool* cf_in_builders) const {
  assert(do_not_open_cf != nullptr);
  assert(cf_in_builders != nullptr);
  // Not found means that user didn't supply that column
  // family option AND we encountered column family add
  // record. Once we encounter column family drop record,
  // we will delete the column family from
  // do_not_open_column_families_.
  uint32_t cf_id = edit.GetColumnFamily();
  bool in_do_not_open = do_not_open_column_families_.find(cf_id) !=
                        do_not_open_column_families_.end();
  // in builders means that user supplied that column family
  // option AND that we encountered column family add record
  bool in_builders = builders_.find(cf_id) != builders_.end();
  // They cannot both be true
  assert(!(in_do_not_open && in_builders));
  *do_not_open_cf = in_do_not_open;
  *cf_in_builders = in_builders;
}

void VersionEditHandler::CheckIterationResult(const log::Reader& reader,
                                              Status* s) {
  assert(s != nullptr);
  assert(version_set_ != nullptr);
  if (!s->ok()) {
    // Do nothing here.
  } else if (!version_edit_params_.HasLogNumber() ||
             !version_edit_params_.HasNextFile() ||
             !version_edit_params_.HasLastSequence()) {
    std::string msg("no ");
    if (!version_edit_params_.HasLogNumber()) {
      msg.append("log_file_number, ");
    }
    if (!version_edit_params_.HasNextFile()) {
      msg.append("next_file_number, ");
    }
    if (!version_edit_params_.HasLastSequence()) {
      msg.append("last_sequence, ");
    }
    msg = msg.substr(0, msg.size() - 2);
    msg.append(" entry in MANIFEST");
    *s = Status::Corruption(msg);
  }
  // There were some column families in the MANIFEST that weren't specified
  // in the argument. This is OK in read_only mode
  if (s->ok() && MustOpenAllColumnFamilies() &&
      !do_not_open_column_families_.empty()) {
    std::string msg;
    for (const auto& cf : do_not_open_column_families_) {
      msg.append(", ");
      msg.append(cf.second);
    }
    msg = msg.substr(2);
    *s = Status::InvalidArgument("Column families not opened: " + msg);
  }
  if (s->ok()) {
    version_set_->GetColumnFamilySet()->UpdateMaxColumnFamily(
        version_edit_params_.GetMaxColumnFamily());
    version_set_->MarkMinLogNumberToKeep(
        version_edit_params_.GetMinLogNumberToKeep());
    version_set_->MarkFileNumberUsed(version_edit_params_.GetPrevLogNumber());
    version_set_->MarkFileNumberUsed(version_edit_params_.GetLogNumber());
    for (auto* cfd : *(version_set_->GetColumnFamilySet())) {
      if (cfd->IsDropped()) {
        continue;
      }
      auto builder_iter = builders_.find(cfd->GetID());
      assert(builder_iter != builders_.end());
      auto* builder = builder_iter->second->version_builder();
      if (!builder->CheckConsistencyForNumLevels()) {
        *s = Status::InvalidArgument(
            "db has more levels than options.num_levels");
        break;
      }
    }
  }
  if (s->ok()) {
    for (auto* cfd : *(version_set_->GetColumnFamilySet())) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (version_set_->unchanging()) {
        cfd->table_cache()->SetTablesAreImmortal();
      }
      *s = LoadTables(cfd, /*prefetch_index_and_filter_in_cache=*/false,
                      /*is_initial_load=*/true);
      if (!s->ok()) {
        // If s is IOError::PathNotFound, then we mark the db as corrupted.
        if (s->IsPathNotFound()) {
          *s = Status::Corruption("Corruption: " + s->ToString());
        }
        break;
      }
    }
  }

  if (s->ok()) {
    for (auto* cfd : *(version_set_->column_family_set_)) {
      if (cfd->IsDropped()) {
        continue;
      }
      assert(cfd->initialized());
      VersionEdit edit;
      *s = MaybeCreateVersionBeforeApplyEdit(edit, cfd,
                                             /*force_create_version=*/true);
      if (!s->ok()) {
        break;
      }
    }
  }
  if (s->ok()) {
    version_set_->manifest_file_size_ = reader.GetReadOffset();
    assert(version_set_->manifest_file_size_ > 0);
    version_set_->manifest_recovery_last_valid_record_end_ =
        last_valid_record_end_;
    version_set_->manifest_last_valid_record_end_ = last_valid_record_end_;
    version_set_->manifest_last_valid_record_end_file_number_ =
        version_set_->manifest_file_number_;
    version_set_->next_file_number_.store(version_edit_params_.GetNextFile() +
                                          1);
    SequenceNumber last_seq = version_edit_params_.GetLastSequence();
    assert(last_seq != kMaxSequenceNumber);
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_allocated_sequence_.load()) {
      version_set_->last_allocated_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_published_sequence_.load()) {
      version_set_->last_published_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_sequence_.load()) {
      version_set_->last_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->descriptor_last_sequence_) {
      // This is the maximum last sequence of all `VersionEdit`s iterated. It
      // may be greater than the maximum `largest_seqno` of all files in case
      // the newest data referred to by the MANIFEST has been dropped or had its
      // sequence number zeroed through compaction.
      version_set_->descriptor_last_sequence_ = last_seq;
    }
    version_set_->prev_log_number_ = version_edit_params_.GetPrevLogNumber();
  }
}

ColumnFamilyData* VersionEditHandler::CreateCfAndInit(
    const ColumnFamilyOptions& cf_options, const VersionEdit& edit) {
  uint32_t cf_id = edit.GetColumnFamily();
  ColumnFamilyData* cfd = version_set_->CreateColumnFamily(
      cf_options, read_options_, &edit, read_only_,
      /*options_are_sanitized=*/false);
  assert(cfd != nullptr);
  cfd->set_initialized();
  assert(builders_.find(cf_id) == builders_.end());
  builders_.emplace(
      cf_id, VersionBuilderUPtr(new BaseReferencedVersionBuilder(
                 cfd, cfd->current(), this, track_found_and_missing_files_,
                 allow_incomplete_valid_version_)));
  return cfd;
}

ColumnFamilyData* VersionEditHandler::DestroyCfAndCleanup(
    const VersionEdit& edit) {
  uint32_t cf_id = edit.GetColumnFamily();
  DestroyVersionBuilder(cf_id);
  ColumnFamilyData* ret =
      version_set_->GetColumnFamilySet()->GetColumnFamily(cf_id);
  assert(ret != nullptr);
  ret->SetDropped();
  ret->UnrefAndTryDelete();
  ret = nullptr;
  return ret;
}

void VersionEditHandler::DestroyVersionBuilder(uint32_t column_family_id) {
  const auto builder = builders_.find(column_family_id);
  assert(builder != builders_.end());
  builders_.erase(builder);
}

Status VersionEditHandler::MaybeCreateVersionBeforeApplyEdit(
    const VersionEdit& edit, ColumnFamilyData* cfd, bool force_create_version) {
  assert(cfd->initialized());
  Status s;
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  auto* builder = builder_iter->second->version_builder();
  if (force_create_version) {
    auto* v = new Version(cfd, version_set_, version_set_->file_options_,
                          cfd->GetLatestMutableCFOptions(), io_tracer_,
                          /*periodic_compaction_phase_params=*/std::nullopt,
                          version_set_->current_version_number_++,
                          epoch_number_requirement_);
    s = builder->SaveTo(v->storage_info());
    if (s.ok()) {
      // Install new version
      v->PrepareAppend(
          read_options_,
          !(version_set_->db_options_->skip_stats_update_on_db_open));
      version_set_->AppendVersion(cfd, v);
    } else {
      delete v;
    }
  }
  s = ApplyVersionBuilderEdit(builder, edit);
  return s;
}

Status VersionEditHandler::ApplyVersionBuilderEdit(VersionBuilder* builder,
                                                   const VersionEdit& edit) {
  assert(builder != nullptr);
  return builder->Apply(&edit);
}

Status VersionEditHandler::ValidateVersionBuilderBeforeDrop(
    VersionBuilder* builder) {
  if (builder == nullptr) {
    return Status::InvalidArgument("null VersionBuilder before CF drop");
  }
  return builder->ValidatePendingBlobRelocationReplays();
}

Status VersionEditHandler::LoadTables(ColumnFamilyData* cfd,
                                      bool prefetch_index_and_filter_in_cache,
                                      bool is_initial_load) {
  bool skip_load_table_files = skip_load_table_files_;
  TEST_SYNC_POINT_CALLBACK(
      "VersionEditHandler::LoadTables:skip_load_table_files",
      &skip_load_table_files);
  if (skip_load_table_files) {
    return Status::OK();
  }
  assert(cfd != nullptr);
  assert(!cfd->IsDropped());
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  assert(builder_iter->second != nullptr);
  VersionBuilder* builder = builder_iter->second->version_builder();
  assert(builder);
  const auto& moptions = cfd->GetLatestMutableCFOptions();
  Status s = builder->LoadTableHandlers(
      cfd->internal_stats(),
      version_set_->db_options_->max_file_opening_threads,
      prefetch_index_and_filter_in_cache, is_initial_load, moptions,
      MaxFileSizeForL0MetaPin(moptions), read_options_);
  if ((s.IsPathNotFound() || s.IsCorruption()) && no_error_if_files_missing_) {
    s = Status::OK();
  }
  if (!s.ok() && !version_set_->db_options_->paranoid_checks) {
    s = Status::OK();
  }
  return s;
}

Status VersionEditHandler::ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                                      const VersionEdit& edit) {
  if (version_set_ == nullptr) {
    return Status::Corruption("null VersionSet while replaying MANIFEST");
  }
  Status s;
  if (edit.HasBlobIndirectionFormatVersion()) {
    const uint32_t format_version = edit.GetBlobIndirectionFormatVersion();
    if (version_set_->blob_indirection_format_version_ != 0 &&
        version_set_->blob_indirection_format_version_ != format_version) {
      return Status::Corruption(
          "Conflicting DB-global blob indirection format fence");
    }
    version_set_->blob_indirection_format_version_ = format_version;
  }
  if (edit.HasDbId()) {
    version_set_->db_id_ = edit.GetDbId();
    version_edit_params_.SetDBId(edit.GetDbId());
  }
  if (cfd != nullptr) {
    if (edit.HasLogNumber()) {
      if (cfd->GetLogNumber() > edit.GetLogNumber()) {
        ROCKS_LOG_WARN(
            version_set_->db_options()->info_log,
            "MANIFEST corruption detected, but ignored - Log numbers in "
            "records NOT monotonically increasing");
      } else {
        cfd->SetLogNumber(edit.GetLogNumber());
        version_edit_params_.SetLogNumber(edit.GetLogNumber());
      }
    }
    if (edit.HasComparatorName()) {
      bool mark_sst_files_has_no_udt = false;
      // If `persist_user_defined_timestamps` flag is recorded in manifest, it
      // is guaranteed to be in the same VersionEdit as comparator. Otherwise,
      // it's not recorded and it should have default value true.
      s = ValidateUserDefinedTimestampsOptions(
          cfd->user_comparator(), edit.GetComparatorName(),
          cfd->ioptions().persist_user_defined_timestamps,
          edit.GetPersistUserDefinedTimestamps(), &mark_sst_files_has_no_udt);
      if (!s.ok() && cf_to_cmp_names_) {
        cf_to_cmp_names_->emplace(cfd->GetID(), edit.GetComparatorName());
      }
      if (mark_sst_files_has_no_udt) {
        cfds_to_mark_no_udt_.insert(cfd->GetID());
      }
    }
    if (edit.HasFullHistoryTsLow()) {
      const std::string& new_ts = edit.GetFullHistoryTsLow();
      cfd->SetFullHistoryTsLow(new_ts);
    }
  }

  if (s.ok()) {
    if (edit.HasPrevLogNumber()) {
      version_edit_params_.SetPrevLogNumber(edit.GetPrevLogNumber());
    }
    if (edit.HasNextFile()) {
      version_edit_params_.SetNextFile(edit.GetNextFile());
    }
    if (edit.HasMaxColumnFamily()) {
      version_edit_params_.SetMaxColumnFamily(edit.GetMaxColumnFamily());
    }
    if (edit.HasMinLogNumberToKeep()) {
      version_edit_params_.SetMinLogNumberToKeep(
          std::max(version_edit_params_.GetMinLogNumberToKeep(),
                   edit.GetMinLogNumberToKeep()));
    }
    if (edit.HasLastSequence()) {
      // `VersionEdit::last_sequence_`s are assumed to be non-decreasing. This
      // is legacy behavior that cannot change without breaking downgrade
      // compatibility.
      assert(!version_edit_params_.HasLastSequence() ||
             version_edit_params_.GetLastSequence() <= edit.GetLastSequence());
      version_edit_params_.SetLastSequence(edit.GetLastSequence());
    }
    if (edit.HasLastCompactedManifestFileSize()) {
      version_set_->last_compacted_manifest_file_size_ =
          edit.GetLastCompactedManifestFileSize();
      version_set_->TuneMaxManifestFileSize();
    }
    if (edit.HasCommittedOptionsFileNumber()) {
      assert(version_set_ != nullptr);
      version_set_->ApplyCommittedOptionsFileNumber(
          edit.GetCommittedOptionsFileNumber());
    }
    if (!version_edit_params_.HasPrevLogNumber()) {
      version_edit_params_.SetPrevLogNumber(0);
    }
  }
  return s;
}

Status VersionEditHandler::MaybeHandleFileBoundariesForNewFiles(
    VersionEdit& edit, const ColumnFamilyData* cfd) {
  if (edit.GetNewFiles().empty()) {
    return Status::OK();
  }
  auto ucmp = cfd->user_comparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0) {
    return Status::OK();
  }

  VersionEdit::NewFiles& new_files = edit.GetMutableNewFiles();
  assert(!new_files.empty());
  // If true, enabling user-defined timestamp is detected for this column
  // family. All its existing SST files need to have the file boundaries handled
  // and their `persist_user_defined_timestamps` flag set to false regardless of
  // its existing value.
  bool mark_existing_ssts_with_no_udt =
      cfds_to_mark_no_udt_.find(cfd->GetID()) != cfds_to_mark_no_udt_.end();
  bool file_boundaries_need_handling = false;
  for (auto& new_file : new_files) {
    FileMetaData& meta = new_file.second;
    if (meta.user_defined_timestamps_persisted &&
        !mark_existing_ssts_with_no_udt) {
      // `FileMetaData.user_defined_timestamps_persisted` field is the value of
      // the flag `AdvancedColumnFamilyOptions.persist_user_defined_timestamps`
      // at the time when the SST file was created. As a result, all added SST
      // files in one `VersionEdit` should have the same value for it.
      if (file_boundaries_need_handling) {
        return Status::Corruption(
            "New files in one VersionEdit has different "
            "user_defined_timestamps_persisted value.");
      }
      break;
    }
    file_boundaries_need_handling = true;
    assert(!meta.user_defined_timestamps_persisted ||
           mark_existing_ssts_with_no_udt);
    if (mark_existing_ssts_with_no_udt) {
      meta.user_defined_timestamps_persisted = false;
    }
    std::string smallest_buf;
    std::string largest_buf;
    Slice largest_slice = meta.largest.Encode();
    PadInternalKeyWithMinTimestamp(&smallest_buf, meta.smallest.Encode(),
                                   ts_sz);
    auto largest_footer = ExtractInternalKeyFooter(largest_slice);
    if (largest_footer == kRangeTombstoneSentinel) {
      PadInternalKeyWithMaxTimestamp(&largest_buf, largest_slice, ts_sz);
    } else {
      PadInternalKeyWithMinTimestamp(&largest_buf, largest_slice, ts_sz);
    }
    meta.smallest.DecodeFrom(smallest_buf);
    meta.largest.DecodeFrom(largest_buf);
  }
  return Status::OK();
}

VersionEditHandlerPointInTime::VersionEditHandlerPointInTime(
    bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
    VersionSet* version_set, const std::shared_ptr<IOTracer>& io_tracer,
    const ReadOptions& read_options, bool allow_incomplete_valid_version,
    bool trust_manifest_recovery, bool defer_sst_file_opening,
    EpochNumberRequirement epoch_number_requirement)
    : VersionEditHandler(read_only, column_families, version_set,
                         /*track_found_and_missing_files=*/true,
                         /*no_error_if_files_missing=*/true, io_tracer,
                         read_options, allow_incomplete_valid_version,
                         epoch_number_requirement),
      atomic_update_versions_missing_(0),
      trust_manifest_recovery_(trust_manifest_recovery),
      defer_sst_file_opening_(defer_sst_file_opening) {}

VersionEditHandlerPointInTime::~VersionEditHandlerPointInTime() {
  VersionEditHandlerPointInTime::DiscardVersions(&atomic_update_versions_);
  VersionEditHandlerPointInTime::DiscardVersions(&versions_);
}

void VersionEditHandlerPointInTime::DeleteVersion(Version* version) {
  delete version;
}

void VersionEditHandlerPointInTime::DiscardVersions(
    PointInTimeVersions* versions) {
  assert(versions != nullptr);
  if (versions == nullptr) {
    return;
  }
  for (const auto& elem : *versions) {
    delete elem.second.version;
  }
  versions->clear();
}

Status VersionEditHandlerPointInTime::OnAtomicGroupReplayBegin() {
  if (in_atomic_group_) {
    return Status::Corruption("unexpected AtomicGroup start");
  }

  // The AtomicGroup that is about to begin may block column families in a valid
  // state from saving any more updates. So we should save any valid states
  // before proceeding.
  for (const auto& cfid_and_builder : builders_) {
    ColumnFamilyData* cfd = version_set_->GetColumnFamilySet()->GetColumnFamily(
        cfid_and_builder.first);
    assert(!cfd->IsDropped());
    assert(cfd->initialized());
    VersionEdit edit;
    Status s = MaybeCreateVersionBeforeApplyEdit(
        edit, cfd, true /* force_create_version */);
    if (!s.ok()) {
      return s;
    }
  }

  // An old AtomicGroup is incomplete. Throw away the versions that failed to
  // complete it. They must not be used for completing the upcoming
  // AtomicGroup since they are too old.
  ResetAtomicUpdateState();

  in_atomic_group_ = true;
  // We lazily assume the column families that exist at this point are all
  // involved in the AtomicGroup. Overestimating the scope of the AtomicGroup
  // will sometimes cause less data to be recovered, which is fine for
  // best-effort recovery.
  atomic_update_versions_.clear();
  for (const auto& cfid_and_builder : builders_) {
    atomic_update_versions_[cfid_and_builder.first] = PointInTimeVersion();
  }
  atomic_update_versions_missing_ = atomic_update_versions_.size();
  return Status::OK();
}

Status VersionEditHandlerPointInTime::OnAtomicGroupReplayEnd() {
  if (!in_atomic_group_) {
    return Status::Corruption("unexpected AtomicGroup end");
  }
  in_atomic_group_ = false;

  // The AtomicGroup must not have changed the column families. We don't support
  // CF adds or drops in an AtomicGroup.
  if (builders_.size() != atomic_update_versions_.size()) {
    return Status::Corruption("unexpected CF change in AtomicGroup");
  }
  for (const auto& cfid_and_builder : builders_) {
    if (atomic_update_versions_.find(cfid_and_builder.first) ==
        atomic_update_versions_.end()) {
      return Status::Corruption("unexpected CF add in AtomicGroup");
    }
  }
  for (const auto& cfid_and_version : atomic_update_versions_) {
    if (builders_.find(cfid_and_version.first) == builders_.end()) {
      return Status::Corruption("unexpected CF drop in AtomicGroup");
    }
  }
  return Status::OK();
}

void VersionEditHandlerPointInTime::ResetAtomicUpdateState() {
  DiscardVersions(&atomic_update_versions_);
  atomic_update_versions_missing_ = 0;
  in_atomic_group_ = false;
}

void VersionEditHandlerPointInTime::CheckIterationResult(
    const log::Reader& reader, Status* s) {
  VersionEditHandler::CheckIterationResult(reader, s);
  assert(s != nullptr);
  if (s->ok()) {
    *s = PrepareToInstallVersions();
  }
  if (s->ok()) {
    for (auto* cfd : *(version_set_->column_family_set_)) {
      if (cfd->IsDropped()) {
        continue;
      }
      assert(cfd->initialized());
      auto v_iter = versions_.find(cfd->GetID());
      auto builder_iter = builders_.find(cfd->GetID());
      if (v_iter != versions_.end()) {
        assert(v_iter->second.version != nullptr);
        assert(builder_iter != builders_.end());

        version_set_->AppendVersion(cfd, v_iter->second.version);
        installed_version_log_numbers_[cfd->GetID()] =
            v_iter->second.log_number;
        versions_.erase(v_iter);
        // Let's clear found_files, since any files in that are part of the
        // installed Version. Any files that got obsoleted would have already
        // been moved to intermediate_files_
        builder_iter->second->version_builder()->ClearFoundFiles();
      }
    }
  } else {
    DiscardVersions(&versions_);
  }
}

ColumnFamilyData* VersionEditHandlerPointInTime::DestroyCfAndCleanup(
    const VersionEdit& edit) {
  uint32_t cfid = edit.GetColumnFamily();
  // Release uninstalled candidates while the builder still owns every added
  // FileMetaData and the current Version still owns every base file. This
  // keeps unlocked ManifestTailer destruction from becoming the last physical
  // file reference and publishing obsolete-file state without the DB mutex.
  if (AtomicUpdateVersionsContains(cfid)) {
    AtomicUpdateVersionsDropCf(cfid);
    if (AtomicUpdateVersionsCompleted()) {
      AtomicUpdateVersionsApply();
    }
  }
  auto v_iter = versions_.find(cfid);
  if (v_iter != versions_.end()) {
    DeleteVersion(v_iter->second.version);
    versions_.erase(v_iter);
  }
  ColumnFamilyData* cfd = VersionEditHandler::DestroyCfAndCleanup(edit);
  installed_version_log_numbers_.erase(cfid);
  return cfd;
}

Status VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit(
    const VersionEdit& edit, ColumnFamilyData* cfd, bool force_create_version) {
  TEST_SYNC_POINT(
      "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
      "Begin1");
  TEST_SYNC_POINT(
      "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
      "Begin2");
  assert(cfd != nullptr);
  if (!force_create_version) {
    assert(edit.GetColumnFamily() == cfd->GetID());
  }

  bool missing_info = !version_edit_params_.HasLogNumber() ||
                      !version_edit_params_.HasNextFile() ||
                      !version_edit_params_.HasLastSequence();

  Status s;
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  VersionBuilder* builder = builder_iter->second->version_builder();
  const bool valid_pit_before_edit = builder->ValidVersionAvailable();
  s = ApplyVersionBuilderEdit(builder, edit);
  const bool valid_pit_after_edit = builder->ValidVersionAvailable();

  // A new version will be created if:
  // 1) no error has occurred so far, and
  // 2) log_number_, next_file_number_ and last_sequence_ are known, and
  // 3) not in an AtomicGroup
  // 4) any of the following:
  //   a) a valid Version is available before applying the edit
  //      and a valid Version is not available after the edit.
  //   b) a valid Version is available after the edit and the
  //      caller explicitly request that a new version be created.
  if (s.ok() && !missing_info && !in_atomic_group_ &&
      ((!valid_pit_after_edit && valid_pit_before_edit) ||
       (valid_pit_after_edit && force_create_version))) {
    // The Version to be created reflects the state *before* this edit. On a
    // negative edge (valid before, invalid after), roll the just-applied edit
    // back so the builder reflects the pre-edit state, build the Version from
    // it, then redo to restore the post-edit state and continue replay. In the
    // force case the edit is empty, so pre-edit == post-edit and no rollback is
    // needed.
    const bool negative_edge = valid_pit_before_edit && !valid_pit_after_edit;
    if (negative_edge) {
      builder->RollbackLastApply();
    }
    // `cfd->GetLogNumber()` describes exactly the state the Version about to be
    // built reflects: VersionEditHandler::ExtractInfoFromVersionEdit() applies
    // `edit`'s own log number only after this function returns, so on a
    // negative edge it is still the pre-edit value.
    const uint64_t log_number = cfd->GetLogNumber();
    const auto& mopts = cfd->GetLatestMutableCFOptions();
    auto* version = new Version(
        cfd, version_set_, version_set_->file_options_, mopts, io_tracer_,
        /*periodic_compaction_phase_params=*/std::nullopt,
        version_set_->current_version_number_++, epoch_number_requirement_);
    if (!trust_manifest_recovery_ && !defer_sst_file_opening_) {
      s = builder->LoadTableHandlers(
          cfd->internal_stats(),
          version_set_->db_options_->max_file_opening_threads, false, true,
          mopts, MaxFileSizeForL0MetaPin(mopts), read_options_);
      if (!s.ok()) {
        DeleteVersion(version);
        if (negative_edge) {
          builder->RedoLastApply();
        }
        if (s.IsCorruption() ||
            (s.IsPathNotFound() && no_error_if_files_missing_)) {
          // This point in time cannot be recovered; skip it and continue. A
          // PathNotFound is a file the primary compacted away before this
          // handler opened it, which open_files_async makes reachable during a
          // secondary's catch-up by leaving recovery-time files unpinned until
          // the background opener reaches them. Treat it as missing rather than
          // latching the error, the same tolerance LoadTables() applies.
          s = Status::OK();
        }
        CommitVersionBuilderApply(builder);
        return s;
      }
    }
    s = SaveVersionTo(builder, version->storage_info());
    if (negative_edge) {
      builder->RedoLastApply();
    }
    if (s.ok()) {
      if (AtomicUpdateVersionsContains(cfd->GetID())) {
        AtomicUpdateVersionsPut(PointInTimeVersion{version, log_number});
        if (AtomicUpdateVersionsCompleted()) {
          AtomicUpdateVersionsApply();
        }
      } else {
        version->PrepareAppend(
            read_options_,
            !version_set_->db_options_->skip_stats_update_on_db_open);
        auto v_iter = versions_.find(cfd->GetID());
        if (v_iter != versions_.end()) {
          DeleteVersion(v_iter->second.version);
          v_iter->second = PointInTimeVersion{version, log_number};
        } else {
          versions_.emplace(cfd->GetID(),
                            PointInTimeVersion{version, log_number});
        }
      }
    } else {
      DeleteVersion(version);
    }
  }

  CommitVersionBuilderApply(builder);
  return s;
}

Status VersionEditHandlerPointInTime::VerifyFile(ColumnFamilyData* cfd,
                                                 const std::string& fpath,
                                                 int level,
                                                 const FileMetaData& fmeta) {
  if (trust_manifest_recovery_ || defer_sst_file_opening_) {
    // Trust the MANIFEST: do not stat/open the file to classify it. The
    // compaction or asynchronous opener validates files later.
    return Status::OK();
  }
  return version_set_->VerifyFileMetadata(read_options_, cfd, fpath, level,
                                          fmeta);
}

Status VersionEditHandlerPointInTime::VerifyBlobFile(
    ColumnFamilyData* cfd, uint64_t blob_file_num,
    const BlobFileAddition& blob_addition) {
  if (trust_manifest_recovery_) {
    // Trust the MANIFEST: do not open the blob file to classify it.
    return Status::OK();
  }
  BlobSource* blob_source = cfd->blob_source();
  assert(blob_source);
  CacheHandleGuard<BlobFileReader> blob_file_reader;

  const BlobFileOpenInfo blob_file{blob_file_num,
                                   blob_addition.GetChecksumValue(),
                                   blob_addition.GetChecksumMethod()};
  Status s = blob_source->GetBlobFileReader(read_options_, blob_file,
                                            &blob_file_reader);
  if (!s.ok()) {
    return s;
  }
  // TODO: verify checksum
  return s;
}

Status VersionEditHandlerPointInTime::VerifyBlobRelocationFile(
    ColumnFamilyData* cfd, const BlobRelocationFileAddition& addition) {
  if (trust_manifest_recovery_) {
    return Status::OK();
  }
  assert(cfd != nullptr);
  std::shared_ptr<const BlobRelocationDirectory> directory;
  Status status = addition.GetValidatedDirectory(&directory);
  if (!status.ok()) {
    return status;
  }
  CacheHandleGuard<BlobFileReader> reader;
  const MutableCFOptions& mutable_options = cfd->GetLatestMutableCFOptions();
  BlobFileCache* const blob_file_cache = cfd->blob_file_cache();
  if (blob_file_cache == nullptr || version_set_ == nullptr) {
    return Status::Corruption("Missing blob relocation recovery dependency");
  }
  return blob_file_cache->GetRelocationFileReader(
      read_options_,
      BlobFileOpenInfo{addition.GetFileNumber(), addition.GetChecksumValue(),
                       addition.GetChecksumMethod(), addition.GetTailSize()},
      addition.GetFileSize(), directory->directory_digest(),
      version_set_->DbSessionId(), mutable_options.compression_manager.get(),
      mutable_options.block_protection_bytes_per_key, &reader);
}

Status VersionEditHandlerPointInTime::LoadTables(
    ColumnFamilyData* /*cfd*/, bool /*prefetch_index_and_filter_in_cache*/,
    bool /*is_initial_load*/) {
  return Status::OK();
}

Status VersionEditHandlerPointInTime::SaveVersionTo(
    VersionBuilder* builder, VersionStorageInfo* storage) {
  if (builder == nullptr || storage == nullptr) {
    return Status::InvalidArgument("null point-in-time Version output");
  }
  return builder->SaveTo(storage);
}

void VersionEditHandlerPointInTime::CommitVersionBuilderApply(
    VersionBuilder* builder) {
  assert(builder != nullptr);
  builder->CommitLastApply();
}

Status VersionEditHandlerPointInTime::PrepareToInstallVersions() {
  return Status::OK();
}

bool VersionEditHandlerPointInTime::HasMissingFiles() const {
  for (const auto& builder : builders_) {
    if (builder.second->version_builder()->HasMissingFiles()) {
      return true;
    }
  }
  return false;
}

uint64_t VersionEditHandlerPointInTime::GetInstalledVersionLogNumber(
    uint32_t cf_id) const {
  auto iter = installed_version_log_numbers_.find(cf_id);
  if (iter == installed_version_log_numbers_.end()) {
    return 0;
  }
  return iter->second;
}

bool VersionEditHandlerPointInTime::AtomicUpdateVersionsCompleted() {
  return atomic_update_versions_missing_ == 0;
}

bool VersionEditHandlerPointInTime::AtomicUpdateVersionsContains(
    uint32_t cfid) {
  return atomic_update_versions_.find(cfid) != atomic_update_versions_.end();
}

void VersionEditHandlerPointInTime::AtomicUpdateVersionsDropCf(uint32_t cfid) {
  assert(!AtomicUpdateVersionsCompleted());
  auto atomic_update_versions_iter = atomic_update_versions_.find(cfid);
  assert(atomic_update_versions_iter != atomic_update_versions_.end());
  if (atomic_update_versions_iter->second.version == nullptr) {
    atomic_update_versions_missing_--;
  } else {
    DeleteVersion(atomic_update_versions_iter->second.version);
  }
  atomic_update_versions_.erase(atomic_update_versions_iter);
}

void VersionEditHandlerPointInTime::AtomicUpdateVersionsPut(
    PointInTimeVersion pit_version) {
  assert(!AtomicUpdateVersionsCompleted());
  assert(pit_version.version != nullptr);
  auto atomic_update_versions_iter =
      atomic_update_versions_.find(pit_version.version->cfd()->GetID());
  assert(atomic_update_versions_iter != atomic_update_versions_.end());
  if (atomic_update_versions_iter->second.version == nullptr) {
    atomic_update_versions_missing_--;
  } else {
    DeleteVersion(atomic_update_versions_iter->second.version);
  }
  atomic_update_versions_iter->second = pit_version;
}

void VersionEditHandlerPointInTime::AtomicUpdateVersionsApply() {
  assert(AtomicUpdateVersionsCompleted());
  for (const auto& cfid_and_version : atomic_update_versions_) {
    uint32_t cfid = cfid_and_version.first;
    const PointInTimeVersion& pit_version = cfid_and_version.second;
    assert(pit_version.version != nullptr);
    pit_version.version->PrepareAppend(
        read_options_,
        !version_set_->db_options_->skip_stats_update_on_db_open);
    auto versions_iter = versions_.find(cfid);
    if (versions_iter != versions_.end()) {
      DeleteVersion(versions_iter->second.version);
      versions_iter->second = pit_version;
    } else {
      versions_.emplace(cfid, pit_version);
    }
  }
  atomic_update_versions_.clear();
}

void ManifestTailer::PrepareToReadNewManifest(InstrumentedMutex* db_mutex) {
  if (db_mutex != nullptr) {
    assert(db_mutex_ == nullptr);
    db_mutex_ = db_mutex;
    ResetAtomicUpdateState();
    db_mutex_ = nullptr;
  } else {
    ResetAtomicUpdateState();
  }
  const auto reset_replay_state = [&] {
    ResetReadState();
    ResetUnopenedPhysicalFileOwnership();
    physical_file_ownership_deltas_.clear();
    prepared_column_family_drop_owners_.reset();
    prepared_physical_file_owners_.reset();
  };
  if (db_mutex != nullptr) {
    InstrumentedMutexUnlock unlock(db_mutex);
    reset_replay_state();
  } else {
    reset_replay_state();
  }
  initialized_ = false;
  status_ = Status::OK();
  replace_physical_file_ownership_on_success_ = true;
}

Status ManifestTailer::Initialize() {
  physical_file_ownership_deltas_.clear();
  prepared_column_family_drop_owners_.reset();
  deferred_physical_file_ownership_conflict_ = false;
  format_activation_requires_full_ownership_census_ = false;
  if (Mode::kRecovery == mode_) {
    return VersionEditHandler::Initialize();
  }
  assert(Mode::kCatchUp == mode_);
  Status s;
  if (!initialized_) {
    ColumnFamilySet* cfd_set = version_set_->GetColumnFamilySet();
    assert(cfd_set);
    ColumnFamilyData* default_cfd = cfd_set->GetDefault();
    assert(default_cfd);
    assert(builders_.find(default_cfd->GetID()) != builders_.end());

    Version* dummy_version = default_cfd->dummy_versions();
    assert(dummy_version);
    Version* base_version = dummy_version->Next();
    assert(base_version);
    VersionBuilderUPtr new_builder(new BaseReferencedVersionBuilder(
        default_cfd, base_version, this, track_found_and_missing_files_,
        allow_incomplete_valid_version_));
    ReplaceBuilder(default_cfd->GetID(), std::move(new_builder));

    initialized_ = true;
  }
  return s;
}

Status ManifestTailer::ApplyVersionEdit(VersionEdit& edit,
                                        ColumnFamilyData** cfd) {
  assert(version_set_ != nullptr);
  // VersionEditHandler's constructor requires a non-null VersionSet.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  VersionSet& version_set = *version_set_;
  if (edit.HasBlobIndirectionFormatVersion() &&
      version_set.blob_indirection_format_version_ == 0 &&
      edit.GetBlobIndirectionFormatVersion() != 0) {
    format_activation_requires_full_ownership_census_ = true;
  }
  const auto validate_ownership = [&] {
    return version_set.ValidateAndTrackPhysicalFileOwnership(
        edit, version_set.blob_indirection_format_version_,
        physical_file_ownership_deltas_,
        /*validate_current_owners=*/
        !replace_physical_file_ownership_on_success_,
        &deferred_physical_file_ownership_conflict_);
  };
  Status s;
  if (db_mutex_ != nullptr) {
    InstrumentedMutexUnlock unlock(db_mutex_);
    // SyncPoint's singleton has process lifetime.
    // @lint-ignore NULLSAFECLANG nullable-dereference
    TEST_SYNC_POINT_CALLBACK(
        "ManifestTailer::ValidatePhysicalFileOwnership:Unlocked", db_mutex_);
    s = validate_ownership();
  } else {
    s = validate_ownership();
  }
  if (s.ok() && edit.IsColumnFamilyDrop()) {
    bool do_not_open_cf = false;
    bool cf_in_builders = false;
    CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);
    if (do_not_open_cf) {
      PrepareColumnFamilyDropOwnership(edit.GetColumnFamily());
    }
  }
  if (s.ok()) {
    s = VersionEditHandler::ApplyVersionEdit(edit, cfd);
  }
  if (s.ok()) {
    assert(cfd);
    if (*cfd) {
      cfds_changed_.insert(*cfd);
    }
  }
  return s;
}

Status ManifestTailer::TrackUnopenedPhysicalFileOwnership(
    const VersionEdit& edit) {
  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  if (!edit.IsColumnFamilyAdd() && !edit.IsWalAddition() &&
      !edit.IsWalDeletion()) {
    CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);
  }
  if (db_mutex_ == nullptr || !do_not_open_cf) {
    return VersionEditHandler::TrackUnopenedPhysicalFileOwnership(edit);
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK(
      "ManifestTailer::TrackUnopenedPhysicalFileOwnership:Unlocked", db_mutex_);
  return VersionEditHandler::TrackUnopenedPhysicalFileOwnership(edit);
}

Status ManifestTailer::DecodeVersionEdit(const Slice& record,
                                         VersionEdit* edit) {
  if (db_mutex_ == nullptr) {
    return VersionEditHandlerBase::DecodeVersionEdit(record, edit);
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK("ManifestTailer::DecodeVersionEdit:Unlocked",
                           db_mutex_);
  return VersionEditHandlerBase::DecodeVersionEdit(record, edit);
}

Status ManifestTailer::VerifyBlobRelocationFile(
    ColumnFamilyData* cfd, const BlobRelocationFileAddition& addition) {
  if (db_mutex_ == nullptr) {
    return VersionEditHandlerPointInTime::VerifyBlobRelocationFile(cfd,
                                                                   addition);
  }
  if (version_builder_apply_unlocked_) {
    // SyncPoint's singleton has process lifetime.
    // @lint-ignore NULLSAFECLANG nullable-dereference
    TEST_SYNC_POINT_CALLBACK(
        "ManifestTailer::VerifyBlobRelocationFile:Unlocked", db_mutex_);
    return VersionEditHandlerPointInTime::VerifyBlobRelocationFile(cfd,
                                                                   addition);
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK("ManifestTailer::VerifyBlobRelocationFile:Unlocked",
                           db_mutex_);
  return VersionEditHandlerPointInTime::VerifyBlobRelocationFile(cfd, addition);
}

Status ManifestTailer::ApplyVersionBuilderEdit(VersionBuilder* builder,
                                               const VersionEdit& edit) {
  if (db_mutex_ == nullptr) {
    return VersionEditHandler::ApplyVersionBuilderEdit(builder, edit);
  }
  assert(!version_builder_apply_unlocked_);
  Status status;
  {
    InstrumentedMutexUnlock unlock(db_mutex_);
    version_builder_apply_unlocked_ = true;
    // SyncPoint's singleton has process lifetime.
    // @lint-ignore NULLSAFECLANG nullable-dereference
    TEST_SYNC_POINT_CALLBACK("ManifestTailer::ApplyVersionBuilderEdit:Unlocked",
                             db_mutex_);
    status = VersionEditHandler::ApplyVersionBuilderEdit(builder, edit);
    version_builder_apply_unlocked_ = false;
  }
  return status;
}

Status ManifestTailer::SaveVersionTo(VersionBuilder* builder,
                                     VersionStorageInfo* storage) {
  if (db_mutex_ == nullptr) {
    return VersionEditHandlerPointInTime::SaveVersionTo(builder, storage);
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK("ManifestTailer::SaveVersionTo:Unlocked", db_mutex_);
  return VersionEditHandlerPointInTime::SaveVersionTo(builder, storage);
}

void ManifestTailer::CommitVersionBuilderApply(VersionBuilder* builder) {
  if (db_mutex_ == nullptr) {
    VersionEditHandlerPointInTime::CommitVersionBuilderApply(builder);
    return;
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // Commit can release the final relocation-metadata reference, evict the
  // corresponding reader, and destroy a bounded but large directory.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK("ManifestTailer::CommitVersionBuilderApply:Unlocked",
                           db_mutex_);
  VersionEditHandlerPointInTime::CommitVersionBuilderApply(builder);
}

Status ManifestTailer::ValidateVersionBuilderBeforeDrop(
    VersionBuilder* builder) {
  if (db_mutex_ == nullptr) {
    return VersionEditHandler::ValidateVersionBuilderBeforeDrop(builder);
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK(
      "ManifestTailer::ValidateVersionBuilderBeforeDrop:Unlocked", db_mutex_);
  return VersionEditHandler::ValidateVersionBuilderBeforeDrop(builder);
}

void ManifestTailer::DeleteVersion(Version* version) {
  if (db_mutex_ == nullptr) {
    VersionEditHandlerPointInTime::DeleteVersion(version);
    return;
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK("ManifestTailer::DeleteVersion:Unlocked", db_mutex_);
  // An older atomic candidate can be the final physical-metadata owner after
  // the builder advances past it. VersionSet serializes obsolete publication
  // independently, allowing the large candidate state and cache callbacks to
  // remain outside the DB mutex even in that case.
  VersionEditHandlerPointInTime::DeleteVersion(version);
}

void ManifestTailer::DiscardVersions(PointInTimeVersions* versions) {
  if (db_mutex_ == nullptr) {
    VersionEditHandlerPointInTime::DiscardVersions(versions);
    return;
  }
  InstrumentedMutexUnlock unlock(db_mutex_);
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK("ManifestTailer::DiscardVersions:Unlocked",
                           db_mutex_);
  VersionEditHandlerPointInTime::DiscardVersions(versions);
}

Status ManifestTailer::PrepareToInstallVersions() {
  assert(version_set_ != nullptr);
  // VersionEditHandler's constructor requires a non-null VersionSet.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  VersionSet& version_set = *version_set_;
  Status status;
  const bool rebuild_all_owners =
      replace_physical_file_ownership_on_success_ ||
      format_activation_requires_full_ownership_census_;
  const auto prepare_ownership = [&] {
    VersionSet::PhysicalFileOwnershipIndex* replacement_owners = nullptr;
    if (rebuild_all_owners) {
      // SyncPoint's singleton has process lifetime.
      // @lint-ignore NULLSAFECLANG nullable-dereference
      TEST_SYNC_POINT(
          "ManifestTailer::PrepareToInstallVersions:FullOwnershipCensus");
      VersionSet::PhysicalFileOwnershipIndex next_owners;
      autovector<Version*> candidate_versions;
      candidate_versions.reserve(versions_.size());
      for (const auto& entry : versions_) {
        if (entry.second.version != nullptr) {
          candidate_versions.push_back(entry.second.version);
        }
      }
      status = version_set.BuildPhysicalFileOwnershipIndex(
          candidate_versions, db_mutex_, &next_owners);
      if (!status.ok()) {
        return;
      }
      status = AddUnopenedPhysicalFileOwnership(&next_owners);
      if (!status.ok()) {
        return;
      }
      prepared_physical_file_owners_.emplace(std::move(next_owners));
      replacement_owners = &*prepared_physical_file_owners_;
    } else if (prepared_column_family_drop_owners_.has_value()) {
      // A CF drop already needed one bulk copy to remove all of that CF's
      // owners. Reconcile this attempt's remaining deltas into that copy.
      replacement_owners = &*prepared_column_family_drop_owners_;
    }

    const bool enforce_uniqueness =
        version_set.blob_indirection_format_version_ != 0;
    const auto same_owner = [](const VersionSet::PhysicalFileOwner& lhs,
                               const VersionSet::PhysicalFileOwner& rhs) {
      return lhs.column_family_id == rhs.column_family_id &&
             lhs.kind == rhs.kind;
    };
    const auto record = [&](VersionSet::PhysicalFileOwnershipIndex* owners,
                            uint64_t file_number,
                            VersionSet::PhysicalFileOwner owner,
                            bool present) -> Status {
      assert(owners != nullptr);
      const auto current = owners->find(file_number);
      if (!present) {
        if (current != owners->end() && same_owner(current->second, owner)) {
          owners->erase(current);
        }
        return Status::OK();
      }
      if (current == owners->end()) {
        owners->emplace(file_number, owner);
        return Status::OK();
      }
      if (same_owner(current->second, owner)) {
        return Status::OK();
      }
      if (!enforce_uniqueness) {
        deferred_physical_file_ownership_conflict_ = true;
        return Status::OK();
      }
      return Status::Corruption(
          "Physical file number is reused across column families or file "
          "kinds in installed Versions");
    };

    for (auto& entry : physical_file_ownership_deltas_) {
      const uint64_t file_number = entry.first;
      VersionSet::PhysicalFileOwnershipDelta& delta = entry.second;
      bool present = delta.present;
      const VersionStorageInfo* storage = nullptr;
      const auto candidate = versions_.find(delta.owner.column_family_id);
      if (candidate != versions_.end() &&
          candidate->second.version != nullptr) {
        storage = candidate->second.version->storage_info();
      } else {
        ColumnFamilySet* cfd_set = version_set.GetColumnFamilySet();
        assert(cfd_set != nullptr);
        // VersionSet always owns a ColumnFamilySet.
        // @lint-ignore NULLSAFECLANG nullable-dereference
        ColumnFamilyData* cfd =
            cfd_set->GetColumnFamily(delta.owner.column_family_id);
        Version* current =
            cfd != nullptr && !cfd->IsDropped() ? cfd->current() : nullptr;
        if (current != nullptr) {
          storage = current->storage_info();
        }
      }
      if (storage != nullptr) {
        switch (delta.owner.kind) {
          case VersionSet::PhysicalFileOwnerKind::kTable:
            present = storage->GetFileLocation(file_number).IsValid();
            break;
          case VersionSet::PhysicalFileOwnerKind::kBlob:
            present = storage->GetBlobFileMetaData(file_number) != nullptr;
            break;
          case VersionSet::PhysicalFileOwnerKind::kBlobRelocation:
            present = storage->GetBlobRelocationFileMetaData(file_number) !=
                          nullptr ||
                      storage->IsBlobRelocationFileRetired(file_number);
            break;
        }
      } else if (rebuild_all_owners && !delta.present) {
        // A complete snapshot starts from an empty index, so absent raw claims
        // for unopened or dropped CFs require no work.
        continue;
      }
      delta.present = present;
      if (replacement_owners != nullptr) {
        status = record(replacement_owners, file_number, delta.owner, present);
      } else if (present) {
        // Ordinary catch-up is serialized by manifest_tail_in_progress_, so
        // the published index cannot change while the mutex is released. Keep
        // the update proportional to this attempt's deltas and apply it only
        // after the candidate Versions are installed.
        const auto current =
            version_set.physical_file_owners_.find(file_number);
        if (current != version_set.physical_file_owners_.end() &&
            !same_owner(current->second, delta.owner)) {
          if (enforce_uniqueness) {
            status = Status::Corruption(
                "Physical file number is reused across column families or "
                "file kinds in installed Versions");
          } else {
            deferred_physical_file_ownership_conflict_ = true;
          }
        }
      }
      if (!status.ok()) {
        return;
      }
    }
  };

  if (status.ok()) {
    if (db_mutex_ != nullptr) {
      InstrumentedMutexUnlock unlock(db_mutex_);
      // SyncPoint's singleton has process lifetime.
      // @lint-ignore NULLSAFECLANG nullable-dereference
      TEST_SYNC_POINT_CALLBACK(
          "ManifestTailer::PrepareToInstallVersions:Unlocked", db_mutex_);
      prepare_ownership();
    } else {
      prepare_ownership();
    }
  }

  if (!status.ok() && prepared_physical_file_owners_.has_value()) {
    if (db_mutex_ != nullptr) {
      InstrumentedMutexUnlock unlock(db_mutex_);
      prepared_physical_file_owners_.reset();
    } else {
      prepared_physical_file_owners_.reset();
    }
  }
  return status;
}

ColumnFamilyData* ManifestTailer::DestroyCfAndCleanup(const VersionEdit& edit) {
  assert(version_set_ != nullptr);
  // VersionEditHandler's constructor requires a non-null VersionSet.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  VersionSet& version_set = *version_set_;
  PrepareColumnFamilyDropOwnership(edit.GetColumnFamily());
  ColumnFamilySet* cfd_set = version_set.GetColumnFamilySet();
  assert(cfd_set != nullptr);
  // Every drop reaching this override passed CheckColumnFamilyId.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  ColumnFamilyData* cfd = cfd_set->GetColumnFamily(edit.GetColumnFamily());
  assert(cfd != nullptr);
  // The successful ID check above guarantees a live CFD until the base drop.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  ColumnFamilyData& dropped_cfd = *cfd;
  cfds_changed_.erase(cfd);
  (void)dropped_cfd;

  ColumnFamilyData* result =
      VersionEditHandlerPointInTime::DestroyCfAndCleanup(edit);
  assert(result == nullptr);
  return result;
}

void ManifestTailer::DestroyVersionBuilder(uint32_t column_family_id) {
  const auto existing = builders_.find(column_family_id);
  assert(existing != builders_.end());
  assert(existing->second != nullptr);
  VersionBuilderUPtr guard = std::move(existing->second);
  builders_.erase(existing);
  // The assertions above establish that the moved guard is non-null.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  std::unique_ptr<VersionBuilder> builder = guard->ReleaseVersionBuilder();
  if (db_mutex_ != nullptr) {
    InstrumentedMutexUnlock unlock(db_mutex_);
    // SyncPoint's singleton has process lifetime.
    // @lint-ignore NULLSAFECLANG nullable-dereference
    TEST_SYNC_POINT_CALLBACK("ManifestTailer::DestroyVersionBuilder:Unlocked",
                             db_mutex_);
    builder.reset();
  } else {
    builder.reset();
  }
  // Release the guard's non-atomic base-Version reference after the DB mutex
  // has been reacquired.
  guard.reset();
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK(
      "ManifestTailer::DestroyVersionBuilder:GuardReleasedLocked", db_mutex_);
}

void ManifestTailer::PrepareColumnFamilyDropOwnership(
    uint32_t column_family_id) {
  assert(version_set_ != nullptr);
  // VersionEditHandler's constructor requires a non-null VersionSet.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  VersionSet& version_set = *version_set_;
  const auto prepare_ownership_index = [&] {
    if (!prepared_column_family_drop_owners_.has_value()) {
      prepared_column_family_drop_owners_.emplace();
      version_set.PrepareColumnFamilyDropOwnershipIndex(
          column_family_id, &*prepared_column_family_drop_owners_);
    } else {
      VersionSet::RemoveColumnFamilyPhysicalFileOwners(
          column_family_id, &*prepared_column_family_drop_owners_);
    }
    VersionSet::MarkColumnFamilyDroppedOwnershipDeltas(
        column_family_id, physical_file_ownership_deltas_);
  };
  if (db_mutex_ != nullptr) {
    InstrumentedMutexUnlock unlock(db_mutex_);
    // SyncPoint's singleton has process lifetime.
    // @lint-ignore NULLSAFECLANG nullable-dereference
    TEST_SYNC_POINT_CALLBACK(
        "ManifestTailer::PrepareColumnFamilyDropOwnershipDeltas:Unlocked",
        db_mutex_);
    prepare_ownership_index();
  } else {
    prepare_ownership_index();
  }
}

void ManifestTailer::ReplaceBuilder(uint32_t column_family_id,
                                    VersionBuilderUPtr replacement) {
  assert(replacement != nullptr);
  const auto existing = builders_.find(column_family_id);
  assert(existing != builders_.end());
  assert(existing->second != nullptr);
  VersionBuilderUPtr old = std::move(existing->second);
  existing->second = std::move(replacement);
  // The assertions above establish that the moved guard is non-null.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  std::unique_ptr<VersionBuilder> old_builder = old->ReleaseVersionBuilder();
  if (db_mutex_ != nullptr) {
    InstrumentedMutexUnlock unlock(db_mutex_);
    // SyncPoint's singleton has process lifetime.
    // @lint-ignore NULLSAFECLANG nullable-dereference
    TEST_SYNC_POINT_CALLBACK("ManifestTailer::ReplaceBuilder:Unlocked",
                             db_mutex_);
    old_builder.reset();
  } else {
    old_builder.reset();
  }
  // BaseReferencedVersionBuilder releases the non-atomic Version reference
  // here, after InstrumentedMutexUnlock has reacquired the DB mutex.
  old.reset();
  // SyncPoint's singleton has process lifetime.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT_CALLBACK("ManifestTailer::ReplaceBuilder:GuardReleasedLocked",
                           db_mutex_);
}

Status ManifestTailer::OnColumnFamilyAdd(VersionEdit& edit,
                                         ColumnFamilyData** cfd) {
  if (Mode::kRecovery == mode_) {
    return VersionEditHandler::OnColumnFamilyAdd(edit, cfd);
  }
  assert(Mode::kCatchUp == mode_);
  ColumnFamilySet* cfd_set = version_set_->GetColumnFamilySet();
  assert(cfd_set);
  ColumnFamilyData* tmp_cfd = cfd_set->GetColumnFamily(edit.GetColumnFamily());
  assert(cfd);
  *cfd = tmp_cfd;
  if (!tmp_cfd) {
    // For now, ignore new column families created after Recover() succeeds.
    return Status::OK();
  }
  assert(builders_.find(edit.GetColumnFamily()) != builders_.end());

  Version* dummy_version = tmp_cfd->dummy_versions();
  assert(dummy_version);
  Version* base_version = dummy_version->Next();
  assert(base_version);
  VersionBuilderUPtr new_builder(new BaseReferencedVersionBuilder(
      tmp_cfd, base_version, this, track_found_and_missing_files_));
  ReplaceBuilder(edit.GetColumnFamily(), std::move(new_builder));

#ifndef NDEBUG
  auto version_iter = versions_.find(edit.GetColumnFamily());
  assert(version_iter == versions_.end());
#endif  // !NDEBUG
  return Status::OK();
}

void ManifestTailer::CheckIterationResult(const log::Reader& reader,
                                          Status* s) {
  VersionEditHandlerPointInTime::CheckIterationResult(reader, s);
  assert(s);
  assert(version_set_ != nullptr);
  // VersionEditHandler's constructor requires a non-null VersionSet.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  VersionSet& version_set = *version_set_;
  std::optional<VersionSet::PhysicalFileOwnershipIndex> owners_to_destroy;
  if (s->ok()) {
    if (prepared_physical_file_owners_.has_value()) {
      version_set.physical_file_owners_.swap(*prepared_physical_file_owners_);
      owners_to_destroy = std::move(prepared_physical_file_owners_);
      prepared_physical_file_owners_.reset();
    } else if (prepared_column_family_drop_owners_.has_value()) {
      version_set.physical_file_owners_.swap(
          *prepared_column_family_drop_owners_);
      owners_to_destroy = std::move(prepared_column_family_drop_owners_);
      prepared_column_family_drop_owners_.reset();
    } else {
      version_set.ApplyPhysicalFileOwnershipDeltas(
          physical_file_ownership_deltas_);
    }
    replace_physical_file_ownership_on_success_ = false;
    if (Mode::kRecovery == mode_) {
      if (defer_sst_file_opening_) {
        // Deferred verification treats each added SST as found. Discard files
        // also deleted during initial replay so they are not reported as
        // obsolete files from a later catch-up.
        GetAndClearIntermediateFiles();
      }
      defer_sst_file_opening_ = false;
      mode_ = Mode::kCatchUp;
    } else {
      assert(Mode::kCatchUp == mode_);
    }
  } else if (prepared_column_family_drop_owners_.has_value()) {
    // Column-family destruction is immediate during replay even if a later
    // record fails. Keep current ownership aligned with that actual state,
    // without applying unrelated deltas from the failed tail attempt.
    version_set.physical_file_owners_.swap(
        *prepared_column_family_drop_owners_);
    owners_to_destroy = std::move(prepared_column_family_drop_owners_);
    prepared_column_family_drop_owners_.reset();
  }
  VersionSet::PhysicalFileOwnershipDeltas deltas_to_destroy;
  deltas_to_destroy.swap(physical_file_ownership_deltas_);
  const auto release_staged_ownership = [&] {
    deltas_to_destroy.clear();
    owners_to_destroy.reset();
    prepared_column_family_drop_owners_.reset();
    prepared_physical_file_owners_.reset();
  };
  if (deltas_to_destroy.empty() && !owners_to_destroy.has_value() &&
      !prepared_column_family_drop_owners_.has_value() &&
      !prepared_physical_file_owners_.has_value()) {
    return;
  }
  if (db_mutex_ != nullptr) {
    InstrumentedMutexUnlock unlock(db_mutex_);
    release_staged_ownership();
  } else {
    release_staged_ownership();
  }
}

std::vector<std::string> ManifestTailer::GetAndClearIntermediateFiles() {
  std::vector<std::string> res;
  for (const auto& builder : builders_) {
    std::vector<std::string> files =
        builder.second->version_builder()->GetAndClearIntermediateFiles();
    res.insert(res.end(), std::make_move_iterator(files.begin()),
               std::make_move_iterator(files.end()));
  }
  return res;
}

Status ManifestTailer::VerifyFile(ColumnFamilyData* cfd,
                                  const std::string& fpath, int level,
                                  const FileMetaData& fmeta) {
  Status s =
      VersionEditHandlerPointInTime::VerifyFile(cfd, fpath, level, fmeta);
  // TODO: Open file or create hard link to prevent the file from being
  // deleted.
  return s;
}

void DumpManifestHandler::CheckIterationResult(const log::Reader& reader,
                                               Status* s) {
  VersionEditHandler::CheckIterationResult(reader, s);
  if (!s->ok()) {
    fprintf(stdout, "%s\n", s->ToString().c_str());
    return;
  }
  assert(cf_to_cmp_names_);
  for (auto* cfd : *(version_set_->column_family_set_)) {
    fprintf(stdout,
            "--------------- Column family \"%s\"  (ID %" PRIu32
            ") --------------\n",
            cfd->GetName().c_str(), cfd->GetID());
    fprintf(stdout, "log number: %" PRIu64 "\n", cfd->GetLogNumber());
    auto it = cf_to_cmp_names_->find(cfd->GetID());
    if (it != cf_to_cmp_names_->end()) {
      fprintf(stdout,
              "comparator: <%s>, but the comparator object is not available.\n",
              it->second.c_str());
    } else {
      fprintf(stdout, "comparator: %s\n", cfd->user_comparator()->Name());
    }
    assert(cfd->current());

    // Print out DebugStrings. Can include non-terminating null characters.
    fwrite(cfd->current()->DebugString(hex_).data(), sizeof(char),
           cfd->current()->DebugString(hex_).size(), stdout);

    fprintf(stdout,
            "By default, manifest file dump prints LSM trees as if %d levels "
            "were configured, "
            "which is not necessarily true for the column family (CF) this "
            "manifest is associated with. "
            "Please consult other DB files, such as the OPTIONS file, to "
            "confirm.\n",
            cfd->ioptions().num_levels);
  }
  fprintf(stdout,
          "next_file_number %" PRIu64 " last_sequence %" PRIu64
          "  prev_log_number %" PRIu64 " max_column_family %" PRIu32
          " min_log_number_to_keep %" PRIu64 "\n",
          version_set_->current_next_file_number(),
          version_set_->LastSequence(), version_set_->prev_log_number(),
          version_set_->column_family_set_->GetMaxColumnFamily(),
          version_set_->min_log_number_to_keep());
}

}  // namespace ROCKSDB_NAMESPACE
