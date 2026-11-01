//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/import_column_family_job.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/builder.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/wide/wide_column_serialization.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "table/merging_iterator.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status ImportColumnFamilyJob::Prepare(uint64_t next_file_number,
                                      SuperVersion* sv) {
  Status status;
  std::vector<ColumnFamilyIngestFileInfo> cf_ingest_infos;
  for (const auto& metadata_per_cf : metadatas_) {
    // Read the information of files we are importing
    ColumnFamilyIngestFileInfo cf_file_info;
    InternalKey smallest;
    InternalKey largest;
    int num_files = 0;
    std::vector<IngestedFileInfo> files_to_import_per_cf;
    for (size_t i = 0; i < metadata_per_cf.size(); i++) {
      auto file_metadata = *metadata_per_cf[i];
      const auto file_path = file_metadata.db_path + "/" + file_metadata.name;
      IngestedFileInfo file_to_import;
      status = GetIngestedFileInfo(file_path, next_file_number++, sv,
                                   file_metadata, &file_to_import);
      if (!status.ok()) {
        return status;
      }

      if (file_to_import.num_entries == 0) {
        status = Status::InvalidArgument("File contain no entries");
        return status;
      }

      if (!file_to_import.smallest_internal_key.Valid() ||
          !file_to_import.largest_internal_key.Valid()) {
        status = Status::Corruption("File has corrupted keys");
        return status;
      }

      files_to_import_per_cf.push_back(file_to_import);
      num_files++;

      // Calculate the smallest and largest keys of all files in this CF
      if (i == 0) {
        smallest = file_to_import.smallest_internal_key;
        largest = file_to_import.largest_internal_key;
      } else {
        if (cfd_->internal_comparator().Compare(
                smallest, file_to_import.smallest_internal_key) > 0) {
          smallest = file_to_import.smallest_internal_key;
        }
        if (cfd_->internal_comparator().Compare(
                largest, file_to_import.largest_internal_key) < 0) {
          largest = file_to_import.largest_internal_key;
        }
      }
    }

    if (num_files == 0) {
      status = Status::InvalidArgument("The list of files is empty");
      return status;
    }
    files_to_import_.push_back(files_to_import_per_cf);
    cf_file_info.smallest_internal_key = smallest;
    cf_file_info.largest_internal_key = largest;
    cf_ingest_infos.push_back(cf_file_info);
  }

  std::sort(cf_ingest_infos.begin(), cf_ingest_infos.end(),
            [this](const ColumnFamilyIngestFileInfo& info1,
                   const ColumnFamilyIngestFileInfo& info2) {
              return cfd_->user_comparator()->Compare(
                         info1.smallest_internal_key.user_key(),
                         info2.smallest_internal_key.user_key()) < 0;
            });

  for (size_t i = 0; i + 1 < cf_ingest_infos.size(); i++) {
    if (cfd_->user_comparator()->Compare(
            cf_ingest_infos[i].largest_internal_key.user_key(),
            cf_ingest_infos[i + 1].smallest_internal_key.user_key()) >= 0) {
      status = Status::InvalidArgument("CFs have overlapping ranges");
      return status;
    }
  }

  // Copy/Move external files into DB
  auto hardlink_files = import_options_.move_files;

  for (auto& files_to_import_per_cf : files_to_import_) {
    for (auto& f : files_to_import_per_cf) {
      const auto path_outside_db = f.external_file_path;
      const auto path_inside_db = TableFileName(
          cfd_->ioptions().cf_paths, f.fd.GetNumber(), f.fd.GetPathId());

      if (hardlink_files) {
        status = fs_->LinkFile(path_outside_db, path_inside_db, IOOptions(),
                               nullptr);
        if (status.IsNotSupported()) {
          // Original file is on a different FS, use copy instead of hard
          // linking
          hardlink_files = false;
          ROCKS_LOG_INFO(db_options_.info_log,
                         "Try to link file %s but it's not supported : %s",
                         f.internal_file_path.c_str(),
                         status.ToString().c_str());
        }
      }
      if (!hardlink_files) {
        // FIXME: temperature handling (like ExternalSstFileIngestionJob)
        status = CopyFile(fs_.get(), path_outside_db, Temperature::kUnknown,
                          path_inside_db, Temperature::kUnknown, 0,
                          db_options_.use_fsync, io_tracer_);
      }
      if (!status.ok()) {
        break;
      }
      f.copy_file = !hardlink_files;
      f.internal_file_path = path_inside_db;
    }
    if (!status.ok()) {
      break;
    }
  }

  if (!status.ok()) {
    // We failed, remove all files that we copied into the db
    for (auto& files_to_import_per_cf : files_to_import_) {
      for (auto& f : files_to_import_per_cf) {
        if (f.internal_file_path.empty()) {
          break;
        }
        const auto s =
            fs_->DeleteFile(f.internal_file_path, IOOptions(), nullptr);
        if (!s.ok()) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "AddFile() clean up for file %s failed : %s",
                         f.internal_file_path.c_str(), s.ToString().c_str());
        }
      }
    }
  }

  return status;
}

// REQUIRES: we have become the only writer by entering both write_thread_ and
// nonmem_write_thread_
Status ImportColumnFamilyJob::Run() {
  // We use the import time as the ancestor time. This is the time the data
  // is written to the database.
  int64_t temp_current_time = 0;
  uint64_t oldest_ancestor_time = kUnknownOldestAncestorTime;
  uint64_t current_time = kUnknownOldestAncestorTime;
  if (clock_->GetCurrentTime(&temp_current_time).ok()) {
    current_time = oldest_ancestor_time =
        static_cast<uint64_t>(temp_current_time);
  }

  Status s;
  // When importing multiple CFs, we should not reuse epoch number from ingested
  // files. Since these epoch numbers were assigned by different CFs, there may
  // be different files from different CFs with the same epoch number. With a
  // subsequent intra-L0 compaction we may end up with files with overlapping
  // key range but the same epoch number. Here we will create a dummy
  // VersionStorageInfo per CF being imported. Each CF's files will be assigned
  // increasing epoch numbers to avoid duplicated epoch number. This is done by
  // only resetting epoch number of the new CF in the first call to
  // RecoverEpochNumbers() below.
  for (size_t i = 0; s.ok() && i < files_to_import_.size(); ++i) {
    VersionBuilder dummy_version_builder(
        cfd_->current()->version_set()->file_options(), &cfd_->ioptions(),
        cfd_->table_cache(), cfd_->current()->storage_info(),
        cfd_->current()->version_set(),
        cfd_->GetFileMetadataCacheReservationManager());
    VersionStorageInfo dummy_vstorage(
        &cfd_->internal_comparator(), cfd_->user_comparator(),
        cfd_->NumberLevels(), cfd_->ioptions().compaction_style,
        nullptr /* src_vstorage */, cfd_->ioptions().force_consistency_checks,
        EpochNumberRequirement::kMightMissing, cfd_->ioptions().clock,
        cfd_->GetLatestMutableCFOptions().bottommost_file_compaction_delay,
        cfd_->current()->version_set()->offpeak_time_option(),
        PeriodicCompactionPhaseParams{});
    for (size_t j = 0; s.ok() && j < files_to_import_[i].size(); ++j) {
      const auto& f = files_to_import_[i][j];
      const auto& file_metadata = *metadatas_[i][j];

      uint64_t tail_size = FileMetaData::CalculateTailSize(f.fd.GetFileSize(),
                                                           f.table_properties);

      VersionEdit dummy_version_edit;
      dummy_version_edit.AddFile(
          file_metadata.level, f.fd.GetNumber(), f.fd.GetPathId(),
          f.fd.GetFileSize(), f.smallest_internal_key, f.largest_internal_key,
          file_metadata.smallest_seqno, file_metadata.largest_seqno, false,
          file_metadata.temperature, kInvalidBlobFileNumber,
          oldest_ancestor_time, current_time, file_metadata.epoch_number,
          kUnknownFileChecksum, kUnknownFileChecksumFuncName, f.unique_id, 0,
          tail_size,
          static_cast<bool>(
              f.table_properties.user_defined_timestamps_persisted));
      s = dummy_version_builder.Apply(&dummy_version_edit);
    }
    if (s.ok()) {
      s = dummy_version_builder.SaveTo(&dummy_vstorage);
    }
    if (s.ok()) {
      // force resetting epoch number for each file
      dummy_vstorage.RecoverEpochNumbers(cfd_, /*restart_epoch=*/i == 0,
                                         /*force=*/true);
      edit_.SetColumnFamily(cfd_->GetID());

      for (int level = 0; level < dummy_vstorage.num_levels(); level++) {
        for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
          edit_.AddFile(level, *file_meta);
          // If incoming sequence number is higher, update local sequence
          // number.
          if (file_meta->fd.largest_seqno > versions_->LastSequence()) {
            versions_->SetLastAllocatedSequence(file_meta->fd.largest_seqno);
            versions_->SetLastPublishedSequence(file_meta->fd.largest_seqno);
            versions_->SetLastSequence(file_meta->fd.largest_seqno);
          }
        }
      }
    }
    // Release resources occupied by the dummy VersionStorageInfo
    for (int level = 0; level < dummy_vstorage.num_levels(); level++) {
      for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
        if (file_meta->Unref()) {
          delete file_meta;
        }
      }
    }
  }

  return s;
}

void ImportColumnFamilyJob::Cleanup(const Status& status) {
  if (!status.ok()) {
    // We failed to add files to the database remove all the files we copied.
    for (auto& files_to_import_per_cf : files_to_import_) {
      for (auto& f : files_to_import_per_cf) {
        const auto s =
            fs_->DeleteFile(f.internal_file_path, IOOptions(), nullptr);
        if (!s.ok()) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "AddFile() clean up for file %s failed : %s",
                         f.internal_file_path.c_str(), s.ToString().c_str());
        }
      }
    }
  } else if (status.ok() && import_options_.move_files) {
    // The files were moved and added successfully, remove original file links
    for (auto& files_to_import_per_cf : files_to_import_) {
      for (auto& f : files_to_import_per_cf) {
        const auto s =
            fs_->DeleteFile(f.external_file_path, IOOptions(), nullptr);
        if (!s.ok()) {
          ROCKS_LOG_WARN(
              db_options_.info_log,
              "%s was added to DB successfully but failed to remove original "
              "file link : %s",
              f.external_file_path.c_str(), s.ToString().c_str());
        }
      }
    }
  }
}

Status ImportColumnFamilyJob::GetIngestedFileInfo(
    const std::string& external_file, uint64_t new_file_number,
    SuperVersion* sv, const LiveFileMetaData& file_meta,
    IngestedFileInfo* file_to_import) {
  file_to_import->external_file_path = external_file;
  Status status;
  if (file_meta.size > 0) {
    file_to_import->file_size = file_meta.size;
  } else {
    // Get external file size
    status = fs_->GetFileSize(external_file, IOOptions(),
                              &file_to_import->file_size, nullptr);
    if (!status.ok()) {
      return status;
    }
  }
  // Assign FD with number
  file_to_import->fd =
      FileDescriptor(new_file_number, 0, file_to_import->file_size);

  // Create TableReader for external file
  std::unique_ptr<TableReader> table_reader;
  std::unique_ptr<FSRandomAccessFile> sst_file;
  std::unique_ptr<RandomAccessFileReader> sst_file_reader;

  FileOptions fo{env_options_};
  fo.file_checksum = file_meta.file_checksum;
  fo.file_checksum_func_name = file_meta.file_checksum_func_name;
  status = fs_->NewRandomAccessFile(external_file, fo, &sst_file, nullptr);
  if (!status.ok()) {
    return status;
  }
  sst_file_reader.reset(new RandomAccessFileReader(
      std::move(sst_file), external_file, nullptr /*Env*/, io_tracer_));

  // TODO(yuzhangyu): User-defined timestamps doesn't support importing column
  //  family. Pass in the correct `user_defined_timestamps_persisted` flag for
  //  creating `TableReaderOptions` when the support is there.
  status = sv->mutable_cf_options.table_factory->NewTableReader(
      TableReaderOptions(
          cfd_->ioptions(), sv->mutable_cf_options.prefix_extractor,
          sv->mutable_cf_options.compression_manager.get(), env_options_,
          cfd_->internal_comparator(),
          sv->mutable_cf_options.block_protection_bytes_per_key,
          /*skip_filters*/ false, /*immortal*/ false,
          /*force_direct_prefetch*/ false, /*level*/ -1,
          /*block_cache_tracer*/ nullptr,
          /*max_file_size_for_l0_meta_pin*/ 0, versions_->DbSessionId(),
          /*cur_file_num*/ new_file_number),
      std::move(sst_file_reader), file_to_import->file_size, &table_reader);
  if (!status.ok()) {
    return status;
  }

  // Get the external file properties
  auto props = table_reader->GetTableProperties();
  FileMetaData blob_metadata;
  ExtractBlobReferenceDescriptorFromTableProperties(*props, &blob_metadata);
  status = ExtractBlobReferenceClassificationFromTableProperties(
      *props, &blob_metadata);
  if (!status.ok()) {
    return status;
  }
  if (!blob_metadata.blob_reference_descriptor.empty() ||
      blob_metadata.oldest_coarse_identity_blob_file_number !=
          kInvalidBlobFileNumber) {
    return Status::NotSupported(
        "Import does not carry blob indirection ownership/route metadata");
  }
  // A missing/malformed property is not proof that internal entries are
  // feature-free. Validate hidden versions and every blob-backed column,
  // before Prepare copies/links anything. Legacy/self-contained cases remain.
  const auto validate_blob = [](const BlobIndex& index) -> Status {
    return index.IsIndirect() ? Status::NotSupported(
                                    "Import does not support stable or "
                                    "relocated blob references")
                              : Status::OK();
  };
  status = ValidateExternalTableBlobIndexes(
      table_reader.get(), sv->mutable_cf_options.prefix_extractor.get(), false,
      validate_blob);
  if (!status.ok()) {
    return status;
  }

  // Set original_seqno to 0.
  file_to_import->original_seqno = 0;

  // Get number of entries in table
  file_to_import->num_entries = props->num_entries;

  // If the importing files were exported with Checkpoint::ExportColumnFamily(),
  // we cannot simply recompute smallest and largest used to truncate range
  // tombstones from file content, and we expect smallest and largest populated
  // in file_meta.
  if (file_meta.smallest.empty()) {
    assert(file_meta.largest.empty());
    // TODO: plumb Env::IOActivity, Env::IOPriority
    ReadOptions ro;
    std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
        ro, sv->mutable_cf_options.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));

    // Get first (smallest) key from file
    iter->SeekToFirst();
    bool bound_set = false;
    if (iter->Valid()) {
      file_to_import->smallest_internal_key.DecodeFrom(iter->key());
      Slice largest;
      if (strcmp(sv->mutable_cf_options.table_factory->Name(), "PlainTable") ==
          0) {
        // PlainTable iterator does not support SeekToLast().
        largest = iter->key();
        for (; iter->Valid(); iter->Next()) {
          if (cfd_->internal_comparator().Compare(iter->key(), largest) > 0) {
            largest = iter->key();
          }
        }
        if (!iter->status().ok()) {
          return iter->status();
        }
      } else {
        iter->SeekToLast();
        if (!iter->Valid()) {
          if (iter->status().ok()) {
            // The file contains at least 1 key since iter is valid after
            // SeekToFirst().
            return Status::Corruption("Can not find largest key in sst file");
          } else {
            return iter->status();
          }
        }
        largest = iter->key();
      }
      file_to_import->largest_internal_key.DecodeFrom(largest);
      bound_set = true;
    } else if (!iter->status().ok()) {
      return iter->status();
    }

    std::unique_ptr<InternalIterator> range_del_iter{
        table_reader->NewRangeTombstoneIterator(ro)};
    if (range_del_iter != nullptr) {
      range_del_iter->SeekToFirst();
      if (range_del_iter->Valid()) {
        ParsedInternalKey key;
        Status pik_status = ParseInternalKey(range_del_iter->key(), &key,
                                             db_options_.allow_data_in_errors);
        if (!pik_status.ok()) {
          return Status::Corruption("Corrupted key in external file. ",
                                    pik_status.getState());
        }
        RangeTombstone first_tombstone(key, range_del_iter->value());
        InternalKey start_key = first_tombstone.SerializeKey();
        const InternalKeyComparator* icmp = &cfd_->internal_comparator();
        if (!bound_set ||
            icmp->Compare(start_key, file_to_import->smallest_internal_key) <
                0) {
          file_to_import->smallest_internal_key = start_key;
        }

        range_del_iter->SeekToLast();
        pik_status = ParseInternalKey(range_del_iter->key(), &key,
                                      db_options_.allow_data_in_errors);
        if (!pik_status.ok()) {
          return Status::Corruption("Corrupted key in external file. ",
                                    pik_status.getState());
        }
        RangeTombstone last_tombstone(key, range_del_iter->value());
        InternalKey end_key = last_tombstone.SerializeEndKey();
        if (!bound_set ||
            icmp->Compare(end_key, file_to_import->largest_internal_key) > 0) {
          file_to_import->largest_internal_key = end_key;
        }
        bound_set = true;
      }
    }
    assert(bound_set);
  } else {
    assert(!file_meta.largest.empty());
    file_to_import->smallest_internal_key.DecodeFrom(file_meta.smallest);
    file_to_import->largest_internal_key.DecodeFrom(file_meta.largest);
  }

  file_to_import->cf_id = static_cast<uint32_t>(props->column_family_id);

  file_to_import->table_properties = *props;

  auto s = GetSstInternalUniqueId(props->db_id, props->db_session_id,
                                  props->orig_file_number,
                                  &(file_to_import->unique_id));
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get SST unique id for file %s",
                   file_to_import->internal_file_path.c_str());
  }

  return status;
}
}  // namespace ROCKSDB_NAMESPACE
