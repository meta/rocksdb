//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/external_sst_file_ingestion_job.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "options/options_helper.h"
#include "table/merging_iterator.h"
#include "table/prepared_file_info.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

bool ExternalSstFileIngestionJob::SupportsAtomicReplaceRangeTombstone() const {
  return cfd_->is_delete_range_supported() &&
         cfd_->ioptions().compaction_style == kCompactionStyleUniversal &&
         ingestion_options_.allow_global_seqno &&
         !ingestion_options_.allow_db_generated_files &&
         !ingestion_options_.snapshot_consistency &&
         ucmp_->timestamp_size() == 0;
}

bool ExternalSstFileIngestionJob::CanUseAtomicReplaceRangeTombstone(
    const std::optional<RangeOpt>& atomic_replace_range) const {
  return atomic_replace_range.has_value() &&
         atomic_replace_range->start.has_value() &&
         atomic_replace_range->limit.has_value() &&
         SupportsAtomicReplaceRangeTombstone() &&
         !ingestion_options_.fail_if_not_bottommost_level;
}

bool ExternalSstFileIngestionJob::HasPartialOverlap(
    const VersionStorageInfo* vstorage) const {
  assert(vstorage != nullptr);
  assert(atomic_replace_range_.has_value());
  assert(!atomic_replace_range_->unset());
  for (int level = 0; level < cfd_->NumberLevels(); ++level) {
    for (const auto* file : vstorage->LevelFiles(level)) {
      if (file_range_checker_.Overlaps(*atomic_replace_range_, file->smallest,
                                       file->largest) &&
          !file_range_checker_.Contains(*atomic_replace_range_, file->smallest,
                                        file->largest)) {
        return true;
      }
    }
  }
  return false;
}

size_t ExternalSstFileIngestionJob::NumFilesToPrepare(
    size_t num_external_files,
    const std::optional<RangeOpt>& atomic_replace_range) const {
  return num_external_files +
         static_cast<size_t>(
             CanUseAtomicReplaceRangeTombstone(atomic_replace_range));
}

Status ExternalSstFileIngestionJob::PrepareAtomicReplaceRangeTombstone(
    const Slice& start, const Slice& limit, uint64_t file_number,
    SuperVersion* super_version) {
  assert(cfd_->is_delete_range_supported());
  assert(cfd_->ioptions().compaction_style == kCompactionStyleUniversal);
  assert(ingestion_options_.allow_global_seqno);
  assert(!ingestion_options_.allow_db_generated_files);
  assert(!ingestion_options_.fail_if_not_bottommost_level);
  assert(!ingestion_options_.snapshot_consistency);
  assert(ucmp_->timestamp_size() == 0);

  const uint32_t path_id = 0;
  const std::string file_path =
      TableFileName(cfd_->ioptions().cf_paths, file_number, path_id);
  ExternalSstFileInfo external_file_info;
  Status status;
  {
    Options options(
        BuildDBOptions(db_options_, mutable_db_options_),
        BuildColumnFamilyOptions(cfd_->initial_cf_options(),
                                 super_version->mutable_cf_options));
    SstFileWriter writer(env_options_, options, ucmp_);
    status = writer.Open(
        file_path, super_version->mutable_cf_options.default_write_temperature);
    if (status.ok()) {
      status = writer.DeleteRange(start, limit);
    }
    if (status.ok()) {
      status = writer.Finish(&external_file_info);
    }
  }
  if (!status.ok()) {
    fs_->DeleteFile(file_path, IOOptions(), nullptr).PermitUncheckedError();
    return status;
  }

  IngestedFileInfo tombstone;
  tombstone.file_temperature =
      super_version->mutable_cf_options.default_write_temperature;
  tombstone.prefetch_lmax_index_and_filter_blocks =
      ingestion_options_.prefetch_lmax_index_and_filter_blocks;
  status = GetIngestedFileInfo(file_path, file_number,
                               external_file_info.prepared_file_info.get(),
                               &tombstone, super_version);
  if (!status.ok()) {
    fs_->DeleteFile(file_path, IOOptions(), nullptr).PermitUncheckedError();
    return status;
  }

  tombstone.internal_file_path = file_path;
  tombstone.copy_file = true;
  tombstone.generated_for_ingestion = true;
  tombstone.file_checksum = external_file_info.file_checksum;
  tombstone.file_checksum_func_name =
      external_file_info.file_checksum_func_name;
  atomic_replace_range_tombstone_.emplace(std::move(tombstone));
  return Status::OK();
}

void ExternalSstFileIngestionJob::ActivateAtomicReplaceRangeTombstone() {
  assert(atomic_replace_range_tombstone_.has_value());
  // Prepare rejects an empty external file list and verifies every input file
  // is contained in the replacement range. The covering tombstone must
  // therefore overlap at least one input file.
  assert(!files_to_ingest_.empty());
  file_batches_to_ingest_.clear();
  files_to_ingest_.emplace_back(std::move(*atomic_replace_range_tombstone_));
  std::rotate(files_to_ingest_.begin(), files_to_ingest_.end() - 1,
              files_to_ingest_.end());
  atomic_replace_range_tombstone_.reset();
  atomic_replace_range_tombstone_active_ = true;
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);
  assert(files_overlap_);
  DivideInputFilesIntoBatches();
}

Status ExternalSstFileIngestionJob::Prepare(
    const std::vector<std::string>& external_files_paths,
    const std::vector<std::string>& files_checksums,
    const std::vector<std::string>& files_checksum_func_names,
    const std::vector<const PreparedFileInfo*>& file_infos,
    const std::optional<RangeOpt>& atomic_replace_range,
    const Temperature& file_temperature, uint64_t next_file_number,
    SuperVersion* sv) {
  Status status;

  // Read the information of files we are ingesting.
  for (size_t i = 0; i < external_files_paths.size(); i++) {
    const std::string& file_path = external_files_paths[i];
    IngestedFileInfo file_to_ingest;
    // For temperature, first assume it matches provided hint
    file_to_ingest.file_temperature = file_temperature;
    file_to_ingest.prefetch_lmax_index_and_filter_blocks =
        ingestion_options_.prefetch_lmax_index_and_filter_blocks;
    const PreparedFileInfo* prepared_file_info =
        file_infos.empty() ? nullptr : file_infos[i];
    status = GetIngestedFileInfo(file_path, next_file_number++,
                                 prepared_file_info, &file_to_ingest, sv);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to get ingested file info: %s: %s",
                     file_path.c_str(), status.ToString().c_str());
      return status;
    }

    // Files generated in another DB or CF may have a different column family
    // ID, so we let it pass here.
    if (file_to_ingest.cf_id !=
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily &&
        file_to_ingest.cf_id != cfd_->GetID() &&
        !ingestion_options_.allow_db_generated_files) {
      return Status::InvalidArgument(
          "External file column family id don't match");
    }

    if (file_to_ingest.num_entries == 0 &&
        file_to_ingest.num_range_deletions == 0) {
      return Status::InvalidArgument("File contain no entries");
    }

    if (!file_to_ingest.smallest_internal_key.Valid() ||
        !file_to_ingest.largest_internal_key.Valid()) {
      return Status::Corruption("Generated table have corrupted keys");
    }

    files_to_ingest_.emplace_back(std::move(file_to_ingest));
  }

  auto num_files = files_to_ingest_.size();
  if (num_files == 0) {
    return Status::InvalidArgument("The list of files is empty");
  }
  // Detect whether the input files overlap one another; this drives how they
  // are divided into batches below.
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);

  if (atomic_replace_range.has_value()) {
    atomic_replace_range_.emplace();

    if (atomic_replace_range->start && atomic_replace_range->limit) {
      if (ucmp_->CompareWithoutTimestamp(*atomic_replace_range->start,
                                         *atomic_replace_range->limit) >= 0) {
        return Status::InvalidArgument(
            "Atomic replace range limit must be greater than start");
      }
      // User keys to internal keys (with timestamps)
      const size_t ts_sz = ucmp_->timestamp_size();
      std::string start_with_ts;
      std::string limit_with_ts;
      auto [start, limit] = MaybeAddTimestampsToRange(
          atomic_replace_range->start, atomic_replace_range->limit, ts_sz,
          &start_with_ts, &limit_with_ts);
      assert(start.has_value());
      assert(limit.has_value());
      atomic_replace_range_->smallest_internal_key.Set(
          *start, kMaxSequenceNumber, kValueTypeForSeek);
      atomic_replace_range_->largest_internal_key =
          RangeTombstone(*start, *limit, kMaxSequenceNumber).SerializeEndKey();
      // Check files to ingest against replace range
      for (size_t i = 0; i < num_files; i++) {
        if (!file_range_checker_.Contains(*atomic_replace_range_,
                                          files_to_ingest_[i])) {
          return Status::InvalidArgument(
              "Atomic replace range does not contain all files");
        }
      }
    } else {
      // Currently if either bound is not present, both must be
      assert(atomic_replace_range->start.has_value() == false);
      assert(atomic_replace_range->limit.has_value() == false);
      assert(atomic_replace_range_->smallest_internal_key.unset());
      assert(atomic_replace_range_->largest_internal_key.unset());
    }
  }

  if (files_overlap_) {
    if (ingestion_options_.ingest_behind) {
      return Status::NotSupported(
          "Files with overlapping ranges cannot be ingested with ingestion "
          "behind mode.");
    }

    // Overlapping files need at least two different sequence numbers. If
    // settings disables global seqno, ingestion will fail anyway, so fail
    // fast in prepare.
    if (!ingestion_options_.allow_global_seqno &&
        !ingestion_options_.allow_db_generated_files) {
      return Status::InvalidArgument(
          "Global seqno is required, but disabled (because external files key "
          "range overlaps).");
    }

    if (ucmp_->timestamp_size() > 0) {
      return Status::NotSupported(
          "Files with overlapping ranges cannot be ingested to column "
          "family with user-defined timestamp enabled.");
    }
  }

  // Copy/Move external files into DB
  std::unordered_set<size_t> ingestion_path_ids;
  for (IngestedFileInfo& f : files_to_ingest_) {
    f.copy_file = false;
    const std::string path_outside_db = f.external_file_path;
    const std::string path_inside_db = TableFileName(
        cfd_->ioptions().cf_paths, f.fd.GetNumber(), f.fd.GetPathId());
    if (ingestion_options_.move_files || ingestion_options_.link_files) {
      status =
          fs_->LinkFile(path_outside_db, path_inside_db, IOOptions(), nullptr);
      if (status.ok()) {
        // It is unsafe to assume application had sync the file and file
        // directory before ingest the file. For integrity of RocksDB we need
        // to sync the file.
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncIngestedFile");
        Status s = fs_->SyncFile(path_inside_db, env_options_, IOOptions(),
                                 db_options_.use_fsync, nullptr);
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncIngestedFile");
        TEST_SYNC_POINT_CALLBACK(
            "ExternalSstFileIngestionJob::CheckSyncReturnCode", &s);
        // Some file systems (especially remote/distributed) don't support
        // explicitly syncing the file and don't require it. Ignore the
        // NotSupported error in that case.
        if (!s.IsNotSupported()) {
          status = s;
          if (!status.ok()) {
            ROCKS_LOG_WARN(db_options_.info_log,
                           "Failed to sync ingested file %s: %s",
                           path_inside_db.c_str(), status.ToString().c_str());
          }
        }
      } else if (status.IsNotSupported() &&
                 ingestion_options_.failed_move_fall_back_to_copy) {
        // Original file is on a different FS, use copy instead of hard linking.
        f.copy_file = true;
        ROCKS_LOG_INFO(db_options_.info_log,
                       "Tried to link file %s but it's not supported : %s",
                       path_outside_db.c_str(), status.ToString().c_str());
      } else {
        ROCKS_LOG_WARN(db_options_.info_log, "Failed to link file %s to %s: %s",
                       path_outside_db.c_str(), path_inside_db.c_str(),
                       status.ToString().c_str());
      }
    } else {
      f.copy_file = true;
    }

    if (f.copy_file) {
      TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Prepare:CopyFile",
                               nullptr);
      // Always determining the destination temperature from the ingested-to
      // level would be difficult because in general we only find out the level
      // ingested to later, during Run().
      // However, we can guarantee "last level" temperature for when the user
      // requires ingestion to the last level.
      Temperature dst_temp =
          (ingestion_options_.ingest_behind ||
           ingestion_options_.fail_if_not_bottommost_level)
              ? sv->mutable_cf_options.last_level_temperature
              : sv->mutable_cf_options.default_write_temperature;
      // Note: CopyFile also syncs the new file.
      status = CopyFile(fs_.get(), path_outside_db, f.file_temperature,
                        path_inside_db, dst_temp, 0, db_options_.use_fsync,
                        io_tracer_);
      // The destination of the copy will be ingested
      f.file_temperature = dst_temp;

      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log, "Failed to copy file %s to %s: %s",
                       path_outside_db.c_str(), path_inside_db.c_str(),
                       status.ToString().c_str());
      }
    } else {
      // Note: we currently assume that linking files does not cross
      // temperatures, so no need to change f.file_temperature
    }
    TEST_SYNC_POINT("ExternalSstFileIngestionJob::Prepare:FileAdded");
    if (!status.ok()) {
      break;
    }
    f.internal_file_path = path_inside_db;
    // Initialize the checksum information of ingested files.
    f.file_checksum = kUnknownFileChecksum;
    f.file_checksum_func_name = kUnknownFileChecksumFuncName;
    ingestion_path_ids.insert(f.fd.GetPathId());
  }

  if (status.ok() && CanUseAtomicReplaceRangeTombstone(atomic_replace_range) &&
      HasPartialOverlap(sv->current->storage_info())) {
    assert(atomic_replace_range->start.has_value());
    assert(atomic_replace_range->limit.has_value());
    status = PrepareAtomicReplaceRangeTombstone(*atomic_replace_range->start,
                                                *atomic_replace_range->limit,
                                                next_file_number, sv);
    if (status.ok()) {
      assert(atomic_replace_range_tombstone_.has_value());
      ingestion_path_ids.insert(
          atomic_replace_range_tombstone_->fd.GetPathId());
    }
  }

  TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncDir");
  if (status.ok()) {
    for (auto path_id : ingestion_path_ids) {
      status = directories_->GetDataDir(path_id)->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to sync directory %" ROCKSDB_PRIszt
                       " while ingest file: %s",
                       path_id, status.ToString().c_str());
        break;
      }
    }
  }
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncDir");

  // Generate and check the SST file checksum.
  if (status.ok() && db_options_.file_checksum_gen_factory != nullptr) {
    bool need_generate_file_checksum;
    if (ingestion_options_.verify_file_checksum == false &&
        files_checksums.size() == files_to_ingest_.size() &&
        files_checksum_func_names.size() == files_to_ingest_.size()) {
      // Only when verify_file_checksum == false and the checksum for ingested
      // files are provided, DB will use the provided checksum and does not
      // generate the checksum for ingested files.
      need_generate_file_checksum = false;
    } else {
      need_generate_file_checksum = true;
    }
    std::vector<std::string> generated_checksums;
    std::vector<std::string> generated_checksum_func_names;
    // Step 1: generate the checksum for ingested sst file.
    if (need_generate_file_checksum) {
      for (size_t i = 0; i < files_to_ingest_.size(); i++) {
        std::string generated_checksum;
        std::string generated_checksum_func_name;
        std::string requested_checksum_func_name =
            i < files_checksum_func_names.size() ? files_checksum_func_names[i]
                                                 : "";
        // TODO: rate limit file reads for checksum calculation during file
        // ingestion.
        // TODO: plumb Env::IOActivity
        ReadOptions ro;
        // Pass user-provided checksums through FileOptions when available.
        // The caller may not have provided checksums at all (empty vectors),
        // so we guard with a bounds check.
        FileOptions fopts;
        if (i < files_checksums.size()) {
          fopts.file_checksum = files_checksums[i];
        }
        if (i < files_checksum_func_names.size()) {
          fopts.file_checksum_func_name = files_checksum_func_names[i];
        } else {
          fopts.file_checksum_func_name = kNoFileChecksumFuncName;
        }
        IOStatus io_s = GenerateOneFileChecksum(
            fs_.get(), files_to_ingest_[i].internal_file_path,
            db_options_.file_checksum_gen_factory.get(),
            requested_checksum_func_name, &generated_checksum,
            &generated_checksum_func_name,
            ingestion_options_.verify_checksums_readahead_size,
            db_options_.allow_mmap_reads, io_tracer_,
            db_options_.rate_limiter.get(), ro, db_options_.stats,
            db_options_.clock, fopts);
        if (!io_s.ok()) {
          status = io_s;
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Sst file checksum generation of file: %s failed: %s",
                         files_to_ingest_[i].internal_file_path.c_str(),
                         status.ToString().c_str());
          break;
        }
        files_to_ingest_[i].file_checksum = generated_checksum;
        files_to_ingest_[i].file_checksum_func_name =
            generated_checksum_func_name;
        generated_checksums.push_back(generated_checksum);
        generated_checksum_func_names.push_back(generated_checksum_func_name);
      }
    }

    // Step 2: based on the verify_file_checksum and ingested checksum
    // information, do the verification.
    if (status.ok()) {
      if (files_checksums.size() == files_to_ingest_.size() &&
          files_checksum_func_names.size() == files_to_ingest_.size()) {
        // Verify the checksum and checksum function name.
        if (ingestion_options_.verify_file_checksum) {
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            if (files_checksum_func_names[i] !=
                generated_checksum_func_names[i]) {
              status = Status::InvalidArgument(
                  "DB file checksum gen factory " +
                  std::string(db_options_.file_checksum_gen_factory->Name()) +
                  " generated checksum function name " +
                  generated_checksum_func_names[i] + " for file " +
                  external_files_paths[i] +
                  " which does not match requested/provided " +
                  files_checksum_func_names[i]);
              break;
            }
            if (files_checksums[i] != generated_checksums[i]) {
              status = Status::Corruption(
                  "Checksum verification mismatch for ingestion file " +
                  external_files_paths[i] + " using function " +
                  generated_checksum_func_names[i] + ". Expected: " +
                  Slice(files_checksums[i]).ToString(/*hex=*/true) +
                  " Computed: " +
                  Slice(generated_checksums[i]).ToString(/*hex=*/true));
              break;
            }
          }
        } else {
          // If verify_file_checksum is not enabled, we only verify the factory
          // recognizes the checksum function name. If it does not match, fail
          // the ingestion. If matches, we trust the ingested checksum
          // information and store in the Manifest.
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            FileChecksumGenContext gen_context;
            gen_context.file_name = files_to_ingest_[i].internal_file_path;
            gen_context.requested_checksum_func_name =
                files_checksum_func_names[i];
            auto file_checksum_gen =
                db_options_.file_checksum_gen_factory
                    ->CreateFileChecksumGenerator(gen_context);

            if (file_checksum_gen == nullptr ||
                files_checksum_func_names[i] != file_checksum_gen->Name()) {
              status = Status::InvalidArgument(
                  "Checksum function name " + files_checksum_func_names[i] +
                  " for file " + external_files_paths[i] +
                  " not recognized by DB checksum gen factory" +
                  db_options_.file_checksum_gen_factory->Name() +
                  (file_checksum_gen ? (" Returned function " +
                                        std::string(file_checksum_gen->Name()))
                                     : ""));
              break;
            }
            files_to_ingest_[i].file_checksum = files_checksums[i];
            files_to_ingest_[i].file_checksum_func_name =
                files_checksum_func_names[i];
          }
        }
      } else if (files_checksums.size() != files_checksum_func_names.size() ||
                 files_checksums.size() != 0) {
        // The checksum or checksum function name vector are not both empty
        // and they are incomplete.
        status = Status::InvalidArgument(
            "The checksum information of ingested sst files are nonempty and "
            "the size of checksums or the size of the checksum function "
            "names does not match with the number of ingested sst files");
      }
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log, "Ingestion failed: %s",
                       status.ToString().c_str());
      }
    }
  }

  if (status.ok()) {
    DivideInputFilesIntoBatches();
  }

  return status;
}

void ExternalSstFileIngestionJob::DivideInputFilesIntoBatches() {
  if (!files_overlap_) {
    // No overlap, treat as one batch without the need of tracking overall batch
    // range.
    file_batches_to_ingest_.emplace_back(/* _track_batch_range= */ false);
    for (auto& file : files_to_ingest_) {
      file_batches_to_ingest_.back().AddFile(&file, file_range_checker_);
    }
    return;
  }

  file_batches_to_ingest_.emplace_back(/* _track_batch_range= */ true);
  for (auto& file : files_to_ingest_) {
    if (!file_batches_to_ingest_.back().unset() &&
        file_range_checker_.Overlaps(file_batches_to_ingest_.back(), file,
                                     /* known_sorted= */ false)) {
      file_batches_to_ingest_.emplace_back(/* _track_batch_range= */ true);
    }
    file_batches_to_ingest_.back().AddFile(&file, file_range_checker_);
  }
}

bool ExternalSstFileIngestionJob::ComputeFilesOverlap(
    const autovector<IngestedFileInfo>& files) const {
  const size_t num_files = files.size();
  if (num_files <= 1) {
    return false;
  }
  // Verify whether the files have overlapping ranges by sorting copies of the
  // file ranges and checking adjacent pairs.
  autovector<const IngestedFileInfo*> sorted_files;
  for (size_t i = 0; i < num_files; i++) {
    sorted_files.push_back(&files[i]);
  }
  std::sort(sorted_files.begin(), sorted_files.end(), file_range_checker_);
  for (size_t i = 0; i + 1 < num_files; i++) {
    if (file_range_checker_.Overlaps(*sorted_files[i], *sorted_files[i + 1],
                                     /* known_sorted= */ true)) {
      return true;
    }
  }
  return false;
}

Status ExternalSstFileIngestionJob::MergeForSameColumnFamily(
    ExternalSstFileIngestionJob* other) {
  assert(other != nullptr);
  assert(other != this);
  assert(cfd_ == other->cfd_);
  if (atomic_replace_range_.has_value() ||
      other->atomic_replace_range_.has_value()) {
    return Status::NotSupported(
        "cannot merge file ingestion handles for the same column family when "
        "atomic_replace_range is used");
  }
  if (!(ingestion_options_ == other->ingestion_options_)) {
    return Status::InvalidArgument(
        "file ingestion handles for the same column family must be prepared "
        "with the same IngestExternalFileOptions");
  }
  // Append the other job's prepared files after this job's so that, for any
  // overlapping keys, the other job's data wins via a higher assigned sequence
  // number -- the same semantics as passing all the files to a single ingestion
  // call in this order. Recompute overlap and rebuild the batches over the
  // union.
  for (IngestedFileInfo& file : other->files_to_ingest_) {
    files_to_ingest_.push_back(std::move(file));
  }
  other->files_to_ingest_.clear();
  other->file_batches_to_ingest_.clear();
  files_overlap_ = ComputeFilesOverlap(files_to_ingest_);
  file_batches_to_ingest_.clear();
  DivideInputFilesIntoBatches();
  return Status::OK();
}

Status ExternalSstFileIngestionJob::NeedsFlush(bool* flush_needed,
                                               SuperVersion* super_version) {
  Status status;
  if (atomic_replace_range_.has_value() && atomic_replace_range_->unset()) {
    // For replacing whole CF, we can simply check whether memtable is empty
    *flush_needed = !super_version->mem->IsEmpty();
  } else {
    autovector<UserKeyRange> ranges;
    if (atomic_replace_range_.has_value()) {
      assert(!atomic_replace_range_->smallest_internal_key.unset());
      assert(!atomic_replace_range_->largest_internal_key.unset());
      // NOTE: we already checked in Prepare() that the atomic_replace_range
      // covers all the files_to_ingest.
      ranges.emplace_back(
          atomic_replace_range_->smallest_internal_key.user_key(),
          atomic_replace_range_->largest_internal_key.user_key());
    } else {
      ranges.reserve(files_to_ingest_.size());
      for (const IngestedFileInfo& file_to_ingest : files_to_ingest_) {
        ranges.emplace_back(file_to_ingest.start_ukey,
                            file_to_ingest.limit_ukey);
      }
    }
    status = cfd_->RangesOverlapWithMemtables(
        ranges, super_version, db_options_.allow_data_in_errors, flush_needed,
        atomic_replace_range_.has_value() && !atomic_replace_range_->unset());
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to check ranges overlap with memtables: %s",
                     status.ToString().c_str());
    }
  }
  if (status.ok() && *flush_needed) {
    if (!ingestion_options_.allow_blocking_flush) {
      status = Status::InvalidArgument("External file requires flush");
    }
    if (ucmp_->timestamp_size() > 0) {
      status = Status::InvalidArgument(
          "Column family enables user-defined timestamps, please make "
          "sure the key range (without timestamp) of external file does not "
          "overlap with key range in the memtables.");
    }
  }
  return status;
}

// REQUIRES: we have become the only writer by entering both write_thread_ and
// nonmem_write_thread_
Status ExternalSstFileIngestionJob::Run() {
  SuperVersion* super_version = cfd_->GetSuperVersion();
  // If column family is flushed after Prepare and before Run, we should have a
  // specific state of Memtables. The mutable Memtable should be empty, and the
  // immutable Memtable list should be empty.
  if (flushed_before_run_ && (super_version->imm->NumNotFlushed() != 0 ||
                              !super_version->mem->IsEmpty())) {
    return Status::TryAgain(
        "Inconsistent memtable state detected when flushed before run.");
  }
  Status status;
#ifndef NDEBUG
  // We should never run the job with a memtable that is overlapping
  // with the files we are ingesting
  bool need_flush = false;
  status = NeedsFlush(&need_flush, super_version);
  if (!status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to check if flush is needed: %s",
                   status.ToString().c_str());
    return status;
  }
  if (need_flush) {
    return Status::TryAgain("need_flush");
  }
  assert(status.ok() && need_flush == false);
#endif

  bool force_global_seqno = false;

  if (ingestion_options_.snapshot_consistency) {
    // We need to assign a global sequence number to all the files even
    // if the don't overlap with any ranges since we have snapshots
    force_global_seqno = db_snapshots_->AnySnapshotWhileNewBlocked();
  }
  // It is safe to use this instead of LastAllocatedSequence since we are
  // the only active writer, and hence they are equal
  SequenceNumber last_seqno = versions_->LastSequence();
  edit_.SetColumnFamily(cfd_->GetID());

  if (atomic_replace_range_.has_value()) {
    // A grouped MANIFEST write can advance current before publishing its SV.
    files_to_replace_version_ = cfd_->current();
    files_to_replace_version_->Ref();
    auto* vstorage = files_to_replace_version_->storage_info();
    files_to_replace_.clear();
    if (atomic_replace_range_->unset()) {
      if (cfd_->compaction_picker()->IsCompactionInProgress()) {
        return Status::InvalidArgument(
            "Atomic replace range (full) overlaps with pending compaction");
      }
      for (int lvl = 0; lvl < cfd_->NumberLevels(); lvl++) {
        for (auto file : vstorage->LevelFiles(lvl)) {
          status = AddFileToReplace(lvl, file);
          if (!status.ok()) {
            return status;
          }
        }
      }
    } else {
      assert(!atomic_replace_range_->smallest_internal_key.unset());
      assert(!atomic_replace_range_->largest_internal_key.unset());
      bool has_partial_overlap = false;
      for (int lvl = 0; lvl < cfd_->NumberLevels(); lvl++) {
        if (cfd_->RangeOverlapWithCompaction(
                atomic_replace_range_->smallest_internal_key.user_key(),
                atomic_replace_range_->largest_internal_key.user_key(), lvl,
                /*range_limit_exclusive=*/true)) {
          return Status::InvalidArgument(
              "Atomic replace range overlaps with pending compaction");
        }
        for (auto file : vstorage->LevelFiles(lvl)) {
          if (file_range_checker_.Overlaps(*atomic_replace_range_,
                                           file->smallest, file->largest)) {
            if (file_range_checker_.Contains(*atomic_replace_range_,
                                             file->smallest, file->largest)) {
              status = AddFileToReplace(lvl, file);
              if (!status.ok()) {
                return status;
              }
            } else {
              has_partial_overlap = true;
            }
          }
        }
      }
      if (has_partial_overlap) {
        if (ingestion_options_.fail_if_not_bottommost_level &&
            SupportsAtomicReplaceRangeTombstone()) {
          return Status::TryAgain(
              "Atomic replace range partially overlaps with an existing file, "
              "so replacement files cannot all be ingested to Lmax");
        }
        if (!atomic_replace_range_tombstone_.has_value()) {
          if (SupportsAtomicReplaceRangeTombstone()) {
            return Status::TryAgain(
                "Atomic replace range acquired a partial overlap after "
                "preparation; retry ingestion");
          }
          return Status::InvalidArgument(
              "Atomic replace range partially overlaps with existing file");
        }
        ActivateAtomicReplaceRangeTombstone();
      }
    }
  }

  // Find levels to ingest into
  std::optional<int> prev_batch_uppermost_level;
  // Batches at the front contain older updates and are placed deeper in the
  // LSM tree than later overlapping batches.
  for (auto& batch : file_batches_to_ingest_) {
    int batch_uppermost_level = 0;
    status = AssignLevelsForOneBatch(batch, force_global_seqno, &last_seqno,
                                     &batch_uppermost_level,
                                     prev_batch_uppermost_level);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to assign levels for one batch: %s",
                     status.ToString().c_str());
      return status;
    }

    prev_batch_uppermost_level = batch_uppermost_level;
  }

  CreateEquivalentFileIngestingCompactions();
  return status;
}

Status ExternalSstFileIngestionJob::AssignLevelsForOneBatch(
    FileBatchInfo& batch, bool force_global_seqno, SequenceNumber* last_seqno,
    int* batch_uppermost_level, std::optional<int> prev_batch_uppermost_level) {
  Status status;
  assert(batch_uppermost_level);
  *batch_uppermost_level = std::numeric_limits<int>::max();
  for (IngestedFileInfo* file : batch.files) {
    assert(file);
    SequenceNumber assigned_seqno = 0;
    if (ingestion_options_.ingest_behind) {
      status = CheckLevelForIngestedBehindFile(file);
    } else {
      status = AssignLevelAndSeqnoForIngestedFile(
          force_global_seqno, cfd_->ioptions().compaction_style, *last_seqno,
          file, &assigned_seqno, prev_batch_uppermost_level);
    }

    // Modify the smallest/largest internal key to include the sequence number
    // that we just learned. Only overwrite sequence number zero. There could
    // be a nonzero sequence number already to indicate a range tombstone's
    // exclusive endpoint.
    ParsedInternalKey smallest_parsed;
    ParsedInternalKey largest_parsed;
    if (status.ok()) {
      status = ParseInternalKey(*(file->smallest_internal_key.rep()),
                                &smallest_parsed, false /* log_err_key */);
    }
    if (status.ok()) {
      status = ParseInternalKey(*(file->largest_internal_key.rep()),
                                &largest_parsed, false /* log_err_key */);
    }
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log, "Failed to parse internal key: %s",
                     status.ToString().c_str());
      return status;
    }

    // If any ingested file overlaps with the DB, it will fail here.
    if (ingestion_options_.allow_db_generated_files && assigned_seqno != 0) {
      return Status::InvalidArgument(
          "An ingested file overlaps with existing data in the DB and has been "
          "assigned a non-zero sequence number, which is not allowed when "
          "'allow_db_generated_files' is enabled.");
    }

    if (smallest_parsed.sequence == 0 && assigned_seqno != 0) {
      UpdateInternalKey(file->smallest_internal_key.rep(), assigned_seqno,
                        smallest_parsed.type);
    }
    if (largest_parsed.sequence == 0 && assigned_seqno != 0) {
      UpdateInternalKey(file->largest_internal_key.rep(), assigned_seqno,
                        largest_parsed.type);
    }

    status = AssignGlobalSeqnoForIngestedFile(file, assigned_seqno);
    if (!status.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Failed to assign global sequence number for ingested file: %s",
          status.ToString().c_str());
      return status;
    }
    TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Run",
                             &assigned_seqno);
    assert(assigned_seqno == 0 || assigned_seqno == *last_seqno + 1);
    if (assigned_seqno > *last_seqno) {
      *last_seqno = assigned_seqno;
    }
    max_assigned_seqno_ = std::max(max_assigned_seqno_, assigned_seqno);

    // We use the import time as the ancestor time. This is the time the data
    // is written to the database.
    int64_t temp_current_time = 0;
    uint64_t current_time = kUnknownFileCreationTime;
    uint64_t oldest_ancestor_time = kUnknownOldestAncestorTime;
    if (clock_->GetCurrentTime(&temp_current_time).ok()) {
      current_time = oldest_ancestor_time =
          static_cast<uint64_t>(temp_current_time);
    }
    uint64_t tail_size = FileMetaData::CalculateTailSize(
        file->fd.GetFileSize(), file->table_properties);

    bool marked_for_compaction =
        file->table_properties.num_range_deletions == 1 &&
        (file->table_properties.num_entries ==
         file->table_properties.num_range_deletions);
    SequenceNumber smallest_seqno = file->assigned_seqno;
    SequenceNumber largest_seqno = file->assigned_seqno;
    if (ingestion_options_.allow_db_generated_files) {
      assert(file->assigned_seqno == 0);
      assert(file->smallest_seqno != kMaxSequenceNumber);
      assert(file->largest_seqno != kMaxSequenceNumber);
      smallest_seqno = file->smallest_seqno;
      largest_seqno = file->largest_seqno;
      max_assigned_seqno_ = std::max(max_assigned_seqno_, file->largest_seqno);
    }
    FileMetaData f_metadata(
        file->fd.GetNumber(), file->fd.GetPathId(), file->fd.GetFileSize(),
        file->smallest_internal_key, file->largest_internal_key, smallest_seqno,
        largest_seqno, false, file->file_temperature, kInvalidBlobFileNumber,
        oldest_ancestor_time, current_time,
        ingestion_options_.ingest_behind
            ? kReservedEpochNumberForFileIngestedBehind
            : cfd_->NewEpochNumber(),  // orders files ingested to L0
        file->file_checksum, file->file_checksum_func_name, file->unique_id, 0,
        tail_size, file->user_defined_timestamps_persisted, "", "");
    f_metadata.temperature = file->file_temperature;
    f_metadata.marked_for_compaction = marked_for_compaction;
    f_metadata.skip_index_and_filter_blocks_prefetch =
        !file->prefetch_lmax_index_and_filter_blocks &&
        file->picked_level == cfd_->NumberLevels() - 1;
    // Extract min/max timestamps from table properties for UDT support.
    // This ensures ingested files have proper timestamp ranges in FileMetaData,
    // similar to files created by flush and compaction.
    ExtractTimestampFromTableProperties(file->table_properties, &f_metadata);
    // Retrieve file open metadata for fast SST open
    if (mutable_db_options_.fast_sst_open) {
      std::unique_ptr<FSRandomAccessFile> readable_file;
      FileOptions fopts{env_options_};
      fopts.file_checksum = f_metadata.file_checksum;
      fopts.file_checksum_func_name = f_metadata.file_checksum_func_name;
      IOStatus io_s = fs_->NewRandomAccessFile(file->internal_file_path, fopts,
                                               &readable_file, nullptr);
      if (io_s.ok()) {
        io_s =
            readable_file->GetFileOpenMetadata(&f_metadata.file_open_metadata);
        if (io_s.ok() && !f_metadata.file_open_metadata.empty() &&
            f_metadata.file_open_metadata.size() <=
                FSRandomAccessFile::kMaxFileOpenMetadataSize) {
          RecordTick(db_options_.stats, FILE_OPEN_METADATA_RETRIEVED);
        } else {
          if (io_s.ok() && f_metadata.file_open_metadata.size() >
                               FSRandomAccessFile::kMaxFileOpenMetadataSize) {
            ROCKS_LOG_WARN(db_options_.info_log,
                           "File open metadata for %s too large (%zu bytes), "
                           "ignoring",
                           file->internal_file_path.c_str(),
                           f_metadata.file_open_metadata.size());
          }
          f_metadata.file_open_metadata.clear();
        }
      }
    }
    edit_.AddFile(file->picked_level, f_metadata);

    *batch_uppermost_level =
        std::min(*batch_uppermost_level, file->picked_level);
  }

  return Status::OK();
}

void ExternalSstFileIngestionJob::CreateEquivalentFileIngestingCompactions() {
  // A map from output level to input of compactions equivalent to this
  // ingestion job.
  // TODO: simplify below logic to creating compaction per ingested file
  // instead of per output level, once we figure out how to treat ingested files
  // with adjacent range deletion tombstones to same output level in the same
  // job as non-overlapping compactions.
  std::map<int, CompactionInputFiles>
      output_level_to_file_ingesting_compaction_input;

  for (const auto& pair : edit_.GetNewFiles()) {
    int output_level = pair.first;
    const FileMetaData& f_metadata = pair.second;

    CompactionInputFiles& input =
        output_level_to_file_ingesting_compaction_input[output_level];
    if (input.files.empty()) {
      // Treat the source level of ingested files to be level 0
      input.level = 0;
    }

    compaction_input_metdatas_.push_back(new FileMetaData(f_metadata));
    input.files.push_back(compaction_input_metdatas_.back());
  }

  for (const auto& pair : output_level_to_file_ingesting_compaction_input) {
    int output_level = pair.first;
    const CompactionInputFiles& input = pair.second;

    const auto& mutable_cf_options = cfd_->GetLatestMutableCFOptions();
    file_ingesting_compactions_.push_back(new Compaction(
        cfd_->current()->storage_info(), cfd_->ioptions(), mutable_cf_options,
        mutable_db_options_, {input}, output_level,
        /* output file size limit not applicable */
        MaxFileSizeForLevel(mutable_cf_options, output_level,
                            cfd_->ioptions().compaction_style),
        LLONG_MAX /* max compaction bytes, not applicable */,
        0 /* output path ID, not applicable */, mutable_cf_options.compression,
        mutable_cf_options.compression_opts, Temperature::kUnknown,
        0 /* max_subcompaction, not applicable */,
        {} /* grandparents, not applicable */,
        std::nullopt /* earliest_snapshot */, nullptr /* snapshot_checker */,
        CompactionReason::kExternalSstIngestion, "" /* trim_ts */,
        -1 /* score, not applicable */,
        files_overlap_ /* l0_files_might_overlap, not applicable */));
  }
}

Status ExternalSstFileIngestionJob::AddFileToReplace(int level,
                                                     FileMetaData* file) {
  // Range checks against registered compactions do not cover every input
  // file, e.g. inputs of a compaction outputting to a different level or
  // files marked by DeleteFilesInRanges().
  if (file->being_compacted) {
    return Status::InvalidArgument(
        "Atomic replace range overlaps with pending compaction");
  }
  edit_.DeleteFile(level, file->fd.GetNumber());
  files_to_replace_.push_back(file);
  return Status::OK();
}

void ExternalSstFileIngestionJob::RegisterRange() {
  for (const auto& c : file_ingesting_compactions_) {
    cfd_->compaction_picker()->RegisterCompaction(c);
  }
  // Compaction pickers only skip files marked as being compacted. Universal
  // compaction does not check range overlap for L0 outputs, so without this a
  // compaction picked while LogAndApply() has released the DB mutex can take
  // a replaced file as input and later fail to delete it from the LSM tree.
  for (FileMetaData* f : files_to_replace_) {
    assert(!f->being_compacted);
    f->being_compacted = true;
  }
  files_to_replace_marked_ = true;
  if (!files_to_replace_.empty()) {
    cfd_->current()
        ->storage_info()
        ->RemoveFilesBeingCompactedFromCompactionCandidates();
  }
}

void ExternalSstFileIngestionJob::UnregisterRange(
    bool restore_compaction_candidates) {
  for (const auto& c : file_ingesting_compactions_) {
    cfd_->compaction_picker()->UnregisterCompaction(c);
    delete c;
  }
  file_ingesting_compactions_.clear();

  for (const auto& f : compaction_input_metdatas_) {
    delete f;
  }
  compaction_input_metdatas_.clear();

  if (files_to_replace_marked_) {
    for (FileMetaData* f : files_to_replace_) {
      assert(f->being_compacted);
      f->being_compacted = false;
    }
    files_to_replace_marked_ = false;
    if (restore_compaction_candidates && !files_to_replace_.empty()) {
      cfd_->current()->storage_info()->ComputeCompactionScore(
          cfd_->ioptions(), cfd_->GetLatestMutableCFOptions(),
          cfd_->GetFullHistoryTsLow());
    }
  }
  files_to_replace_.clear();
  if (files_to_replace_version_ != nullptr) {
    files_to_replace_version_->Unref();
    files_to_replace_version_ = nullptr;
  }
}

void ExternalSstFileIngestionJob::UpdateStats() {
  // Update internal stats for new ingested files
  uint64_t total_keys = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_time = clock_->NowMicros() - job_start_time_;

  EventLoggerStream stream = event_logger_->Log();
  stream << "event" << "ingest_finished";
  stream << "files_ingested";
  stream.StartArray();

  for (IngestedFileInfo& f : files_to_ingest_) {
    InternalStats::CompactionStats stats(
        CompactionReason::kExternalSstIngestion, 1);
    stats.micros = total_time;
    // If actual copy occurred for this file, then we need to count the file
    // size as the actual bytes written. If the file was linked, then we ignore
    // the bytes written for file metadata.
    // TODO (yanqin) maybe account for file metadata bytes for exact accuracy?
    if (f.copy_file) {
      stats.bytes_written = f.fd.GetFileSize();
    } else {
      stats.bytes_moved = f.fd.GetFileSize();
    }
    stats.num_output_files = 1;
    cfd_->internal_stats()->AddCompactionStats(f.picked_level,
                                               Env::Priority::USER, stats);
    cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_INGESTED_ADD_FILE,
                                       f.fd.GetFileSize());
    total_keys += f.num_entries;
    if (f.picked_level == 0) {
      total_l0_files += 1;
    }
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[AddFile] External SST file %s was ingested in L%d with path %s "
        "(global_seqno=%" PRIu64 ")\n",
        f.external_file_path.c_str(), f.picked_level,
        f.internal_file_path.c_str(), f.assigned_seqno);
    stream << "file" << f.internal_file_path << "level" << f.picked_level;
  }
  stream.EndArray();

  stream << "lsm_state";
  stream.StartArray();
  auto vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_KEYS_TOTAL,
                                     total_keys);
  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_FILES_TOTAL,
                                     files_to_ingest_.size());
  cfd_->internal_stats()->AddCFStats(
      InternalStats::INGESTED_LEVEL0_NUM_FILES_TOTAL, total_l0_files);
}

void ExternalSstFileIngestionJob::Cleanup(const Status& status) {
  IOOptions io_opts;
  if (!status.ok()) {
    // We failed to add the files to the database
    // remove all the files we copied
    DeleteInternalFiles();
    files_overlap_ = false;
  } else {
    if (atomic_replace_range_tombstone_.has_value()) {
      Status s =
          fs_->DeleteFile(atomic_replace_range_tombstone_->internal_file_path,
                          io_opts, nullptr);
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Failed to remove unused atomic replace range tombstone %s: %s",
            atomic_replace_range_tombstone_->internal_file_path.c_str(),
            s.ToString().c_str());
      }
      atomic_replace_range_tombstone_.reset();
    }
  }
  if (status.ok() && ingestion_options_.move_files) {
    // The files were moved and added successfully, remove original file links
    for (IngestedFileInfo& f : files_to_ingest_) {
      if (f.generated_for_ingestion) {
        continue;
      }
      Status s = fs_->DeleteFile(f.external_file_path, io_opts, nullptr);
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

void ExternalSstFileIngestionJob::DeleteInternalFiles() {
  IOOptions io_opts;
  for (IngestedFileInfo& f : files_to_ingest_) {
    if (f.internal_file_path.empty()) {
      continue;
    }
    Status s = fs_->DeleteFile(f.internal_file_path, io_opts, nullptr);
    if (!s.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "AddFile() clean up for file %s failed : %s",
                     f.internal_file_path.c_str(), s.ToString().c_str());
    }
  }
  if (atomic_replace_range_tombstone_.has_value() &&
      !atomic_replace_range_tombstone_->internal_file_path.empty()) {
    Status s = fs_->DeleteFile(
        atomic_replace_range_tombstone_->internal_file_path, io_opts, nullptr);
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "AddFile() clean up for generated tombstone file %s failed : %s",
          atomic_replace_range_tombstone_->internal_file_path.c_str(),
          s.ToString().c_str());
    }
    atomic_replace_range_tombstone_.reset();
  }
}

Status ExternalSstFileIngestionJob::ResetTableReader(
    const std::string& external_file, uint64_t new_file_number,
    bool user_defined_timestamps_persisted, SuperVersion* sv,
    IngestedFileInfo* file_to_ingest,
    std::unique_ptr<TableReader>* table_reader) {
  std::unique_ptr<FSRandomAccessFile> sst_file;
  FileOptions fo{env_options_};
  fo.temperature = file_to_ingest->file_temperature;
  fo.file_checksum_func_name = kNoFileChecksumFuncName;
  Status status =
      fs_->NewRandomAccessFile(external_file, fo, &sst_file, nullptr);
  if (!status.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "Failed to create random access file for external file %s: %s",
        external_file.c_str(), status.ToString().c_str());
    return status;
  }
  Temperature updated_temp = sst_file->GetTemperature();
  if (updated_temp != Temperature::kUnknown &&
      updated_temp != file_to_ingest->file_temperature) {
    // The hint was missing or wrong. Track temperature reported by storage.
    file_to_ingest->file_temperature = updated_temp;
  }
  std::unique_ptr<RandomAccessFileReader> sst_file_reader(
      new RandomAccessFileReader(std::move(sst_file), external_file,
                                 nullptr /*Env*/, io_tracer_));
  table_reader->reset();
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  status = sv->mutable_cf_options.table_factory->NewTableReader(
      ro,
      TableReaderOptions(
          cfd_->ioptions(), sv->mutable_cf_options.prefix_extractor,
          sv->mutable_cf_options.compression_manager.get(), env_options_,
          cfd_->internal_comparator(),
          sv->mutable_cf_options.block_protection_bytes_per_key,
          /*skip_filters*/ false, /*immortal*/ false,
          /*force_direct_prefetch*/ false, /*level*/ -1,
          /*block_cache_tracer*/ nullptr,
          /*max_file_size_for_l0_meta_pin*/ 0, versions_->DbSessionId(),
          /*cur_file_num*/ new_file_number,
          /* unique_id */ {}, /* largest_seqno */ 0,
          /* tail_size */ 0, user_defined_timestamps_persisted),
      std::move(sst_file_reader), file_to_ingest->file_size, table_reader,
      // No need to prefetch index/filter if caching is not needed.
      /*prefetch_index_and_filter_in_cache=*/ingestion_options_.fill_cache);
  return status;
}

Status ExternalSstFileIngestionJob::SanityCheckTableProperties(
    const std::string& external_file, const TableProperties& props,
    IngestedFileInfo* file_to_ingest) {
  const auto& uprops = props.user_collected_properties;

  // Get table version
  auto version_iter = uprops.find(ExternalSstFilePropertyNames::kVersion);
  if (version_iter == uprops.end()) {
    assert(!SstFileWriter::CreatedBySstFileWriter(props));
    if (!ingestion_options_.allow_db_generated_files) {
      return Status::Corruption("External file version not found");
    } else {
      // 0 is special version for when a file from live DB does not have the
      // version table property
      file_to_ingest->version = 0;
    }
  } else {
    assert(SstFileWriter::CreatedBySstFileWriter(props));
    file_to_ingest->version = DecodeFixed32(version_iter->second.c_str());
  }

  auto seqno_iter = uprops.find(ExternalSstFilePropertyNames::kGlobalSeqno);
  if (file_to_ingest->version == 2) {
    // version 2 imply that we have global sequence number
    if (seqno_iter == uprops.end()) {
      return Status::Corruption(
          "External file global sequence number not found");
    }

    // Set the global sequence number
    file_to_ingest->original_seqno = DecodeFixed64(seqno_iter->second.c_str());
  } else if (file_to_ingest->version == 1) {
    // SST file V1 should not have global seqno field
    assert(seqno_iter == uprops.end());
    file_to_ingest->original_seqno = 0;
    if (ingestion_options_.allow_blocking_flush ||
        ingestion_options_.allow_global_seqno) {
      return Status::InvalidArgument(
          "External SST file V1 does not support global seqno");
    }
  } else if (file_to_ingest->version == 0) {
    // allow_db_generated_files is true
    assert(seqno_iter == uprops.end());
    file_to_ingest->original_seqno = 0;
  } else {
    return Status::InvalidArgument("External file version " +
                                   std::to_string(file_to_ingest->version) +
                                   " is not supported");
  }

  file_to_ingest->cf_id = static_cast<uint32_t>(props.column_family_id);
  file_to_ingest->table_properties = props;

  // Get number of entries in table
  file_to_ingest->num_entries = props.num_entries;
  file_to_ingest->num_range_deletions = props.num_range_deletions;

  // Validate table properties related to comparator name and user defined
  // timestamps persisted flag.
  file_to_ingest->user_defined_timestamps_persisted =
      static_cast<bool>(props.user_defined_timestamps_persisted);
  bool mark_sst_file_has_no_udt = false;
  Status s = ValidateUserDefinedTimestampsOptions(
      cfd_->user_comparator(), props.comparator_name,
      cfd_->ioptions().persist_user_defined_timestamps,
      file_to_ingest->user_defined_timestamps_persisted,
      &mark_sst_file_has_no_udt);
  if (s.ok() && mark_sst_file_has_no_udt) {
    // A column family that enables user-defined timestamps in Memtable only
    // feature can also ingest external files created by a setting that disables
    // user-defined timestamps. In that case, we need to re-mark the
    // user_defined_timestamps_persisted flag for the file. The open-and-scan
    // caller is then responsible for reopening its `TableReader` with the
    // updated flag.
    file_to_ingest->user_defined_timestamps_persisted = false;
  } else if (!s.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "ValidateUserDefinedTimestampsOptions failed for external file %s: %s",
        external_file.c_str(), s.ToString().c_str());
    return s;
  }

  return s;
}

Status ExternalSstFileIngestionJob::GetIngestedFileInfoFromFileInfo(
    const std::string& external_file,
    const PreparedFileInfo& prepared_file_info,
    IngestedFileInfo* file_to_ingest) {
  // Boundaries, size, and table properties were obtained without opening the
  // file (e.g. produced by SstFileWriter::Finish). We reuse them directly.
  file_to_ingest->file_size = prepared_file_info.file_size;
  Status status = SanityCheckTableProperties(
      external_file, prepared_file_info.table_properties, file_to_ingest);
  if (!status.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "Failed to sanity check table properties for external file %s: %s",
        external_file.c_str(), status.ToString().c_str());
    return status;
  }

  const size_t ts_sz = ucmp_->timestamp_size();
  if (ts_sz > 0 && !file_to_ingest->user_defined_timestamps_persisted) {
    auto pad_timestamp = [ts_sz](std::string* result, const Slice& key) {
      assert(result->empty());
      if (ExtractValueType(key) == kTypeRangeDeletion) {
        PadInternalKeyWithMaxTimestamp(result, key, ts_sz);
      } else {
        PadInternalKeyWithMinTimestamp(result, key, ts_sz);
      }
    };
    pad_timestamp(file_to_ingest->smallest_internal_key.rep(),
                  prepared_file_info.smallest.Encode());
    pad_timestamp(file_to_ingest->largest_internal_key.rep(),
                  prepared_file_info.largest.Encode());
  } else {
    file_to_ingest->smallest_internal_key = prepared_file_info.smallest;
    file_to_ingest->largest_internal_key = prepared_file_info.largest;
  }

  if (ingestion_options_.allow_db_generated_files) {
    // Sequence numbers are preserved (not reassigned), so the bounds must be
    // known before we skip the GetSeqnoBoundaryForFile scan.
    if (!file_to_ingest->table_properties.HasKeyLargestSeqno()) {
      return Status::Corruption("Unknown largest seqno for db generated file.");
    }
    file_to_ingest->largest_seqno =
        file_to_ingest->table_properties.key_largest_seqno;
    if (file_to_ingest->largest_seqno == 0) {
      file_to_ingest->smallest_seqno = 0;
    } else {
      if (!file_to_ingest->table_properties.HasKeySmallestSeqno()) {
        return Status::Corruption(
            "Unknown smallest seqno for db generated file.");
      }
      file_to_ingest->smallest_seqno =
          file_to_ingest->table_properties.key_smallest_seqno;
    }
  } else {
    // Normal ingestion reassigns a global sequence number later, so the file's
    // keys must currently be at seqno 0 (mirror of the open-and-scan check).
    SequenceNumber largest_seqno =
        file_to_ingest->table_properties.key_largest_seqno;
    // UINT64_MAX means unknown and the file is generated before table property
    // `key_largest_seqno` is introduced.
    if (largest_seqno != UINT64_MAX && largest_seqno > 0) {
      return Status::Corruption(
          "External file has non zero largest sequence number " +
          std::to_string(largest_seqno));
    }
  }
  return Status::OK();
}

Status ExternalSstFileIngestionJob::GetIngestedFileInfoFromFile(
    const std::string& external_file, uint64_t new_file_number,
    IngestedFileInfo* file_to_ingest, SuperVersion* sv,
    std::unique_ptr<TableReader>* out_table_reader) {
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::GetIngestedFileInfo:ReadPath");

  // Get external file size
  Status status = fs_->GetFileSize(external_file, IOOptions(),
                                   &file_to_ingest->file_size, nullptr);
  if (!status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get file size for external file %s: %s",
                   external_file.c_str(), status.ToString().c_str());
    return status;
  }

  // Create TableReader for external file.
  std::unique_ptr<TableReader> table_reader;
  // Initially create the `TableReader` with flag
  // `user_defined_timestamps_persisted` to be true since that's the most
  // common case
  status = ResetTableReader(external_file, new_file_number,
                            /*user_defined_timestamps_persisted=*/true, sv,
                            file_to_ingest, &table_reader);
  if (!status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to reset table reader for external file %s: %s",
                   external_file.c_str(), status.ToString().c_str());
    return status;
  }

  status = SanityCheckTableProperties(
      external_file, *table_reader->GetTableProperties(), file_to_ingest);
  if (!status.ok()) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "Failed to sanity check table properties for external file %s: %s",
        external_file.c_str(), status.ToString().c_str());
    return status;
  }

  // The `TableReader` above was opened with `user_defined_timestamps_persisted`
  // assumed true. If the sanity check determined the file has no persisted
  // timestamps (UDT-in-Memtable-only feature), reopen it with the corrected
  // flag so keys are parsed properly by the scan below.
  if (ucmp_->timestamp_size() > 0 &&
      !file_to_ingest->user_defined_timestamps_persisted) {
    status = ResetTableReader(external_file, new_file_number,
                              file_to_ingest->user_defined_timestamps_persisted,
                              sv, file_to_ingest, &table_reader);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to reset table reader for external file %s: %s",
                     external_file.c_str(), status.ToString().c_str());
      return status;
    }
  }

  const bool allow_data_in_errors = db_options_.allow_data_in_errors;
  ParsedInternalKey key;
  if (ingestion_options_.allow_db_generated_files) {
    // We are ingesting a DB generated SST file for which we don't reassign
    // sequence numbers. We need its smallest sequence number and largest
    // sequence number for FileMetaData.
    Status seqno_status = GetSeqnoBoundaryForFile(
        table_reader.get(), sv, file_to_ingest, allow_data_in_errors);

    if (!seqno_status.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Failed to get sequence number boundary for external file %s: %s",
          external_file.c_str(), seqno_status.ToString().c_str());
      return seqno_status;
    }
    assert(file_to_ingest->smallest_seqno <= file_to_ingest->largest_seqno);
    assert(file_to_ingest->largest_seqno < kMaxSequenceNumber);
  } else {
    SequenceNumber largest_seqno =
        table_reader.get()->GetTableProperties()->key_largest_seqno;
    // UINT64_MAX means unknown and the file is generated before table
    // property `key_largest_seqno` is introduced.
    if (largest_seqno != UINT64_MAX && largest_seqno > 0) {
      return Status::Corruption(
          "External file has non zero largest sequence number " +
          std::to_string(largest_seqno));
    }
  }

  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ro, sv->mutable_cf_options.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));

  // Get first (smallest) and last (largest) key from file.
  iter->SeekToFirst();
  if (iter->Valid()) {
    Status pik_status =
        ParseInternalKey(iter->key(), &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    if (key.sequence != 0 && !ingestion_options_.allow_db_generated_files) {
      return Status::Corruption("External file has non zero sequence number");
    }
    file_to_ingest->smallest_internal_key.SetFrom(key);

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

    pik_status = ParseInternalKey(largest, &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    if (key.sequence != 0 && !ingestion_options_.allow_db_generated_files) {
      return Status::Corruption("External file has non zero sequence number");
    }
    file_to_ingest->largest_internal_key.SetFrom(key);
  } else if (!iter->status().ok()) {
    return iter->status();
  }

  std::unique_ptr<InternalIterator> range_del_iter(
      table_reader->NewRangeTombstoneIterator(ro));
  // We may need to adjust these key bounds, depending on whether any range
  // deletion tombstones extend past them.
  if (range_del_iter != nullptr) {
    for (range_del_iter->SeekToFirst(); range_del_iter->Valid();
         range_del_iter->Next()) {
      Status pik_status =
          ParseInternalKey(range_del_iter->key(), &key, allow_data_in_errors);
      if (!pik_status.ok()) {
        return Status::Corruption("Corrupted key in external file. ",
                                  pik_status.getState());
      }
      if (key.sequence != 0 && !ingestion_options_.allow_db_generated_files) {
        return Status::Corruption(
            "External file has a range deletion with non zero sequence "
            "number.");
      }
#ifndef NDEBUG
      // To keep aligned with the fast path that expects range deletion keys to
      // always use max ts.
      const size_t ts_sz = ucmp_->timestamp_size();
      if (ts_sz > 0) {
        const std::string max_ts(ts_sz, '\xff');
        assert(key.user_key.size() >= ts_sz);
        assert(ucmp_->CompareTimestamp(
                   ExtractTimestampFromUserKey(key.user_key, ts_sz), max_ts) ==
               0);
        assert(range_del_iter->value().size() >= ts_sz);
        assert(ucmp_->CompareTimestamp(
                   ExtractTimestampFromUserKey(range_del_iter->value(), ts_sz),
                   max_ts) == 0);
      }
#endif
      RangeTombstone tombstone(key, range_del_iter->value());
      file_range_checker_.MaybeUpdateRange(tombstone.SerializeKey(),
                                           tombstone.SerializeEndKey(),
                                           file_to_ingest);
    }
  }

  *out_table_reader = std::move(table_reader);
  return Status::OK();
}

Status ExternalSstFileIngestionJob::GetIngestedFileInfo(
    const std::string& external_file, uint64_t new_file_number,
    const PreparedFileInfo* prepared_file_info,
    IngestedFileInfo* file_to_ingest, SuperVersion* sv) {
  file_to_ingest->external_file_path = external_file;

  std::unique_ptr<TableReader> table_reader;
  Status status =
      prepared_file_info != nullptr
          ? GetIngestedFileInfoFromFileInfo(external_file, *prepared_file_info,
                                            file_to_ingest)
          : GetIngestedFileInfoFromFile(external_file, new_file_number,
                                        file_to_ingest, sv, &table_reader);
  if (!status.ok()) {
    return status;
  }

  assert(file_to_ingest->file_size > 0);
  assert(!file_to_ingest->unset());
  assert(file_to_ingest->table_properties.num_entries > 0 ||
         file_to_ingest->table_properties.num_range_deletions > 0);
  if (ingestion_options_.allow_db_generated_files) {
    // These files keep their original sequence numbers (derived, not
    // reassigned), so the bounds must be valid here.
    assert(file_to_ingest->smallest_seqno <= file_to_ingest->largest_seqno);
    assert(file_to_ingest->largest_seqno < kMaxSequenceNumber);
  }

  // Verify the file checksum if requested. The open-and-scan path already has a
  // `TableReader` open; the fast-path opens one here only when verification is
  // requested -- otherwise it performs no file I/O.
  if (ingestion_options_.verify_checksums_before_ingest) {
    if (table_reader == nullptr) {
      status =
          ResetTableReader(external_file, new_file_number,
                           file_to_ingest->user_defined_timestamps_persisted,
                           sv, file_to_ingest, &table_reader);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to reset table reader for external file %s: %s",
                       external_file.c_str(), status.ToString().c_str());
        return status;
      }
    }
    // If customized readahead size is needed, we can pass a user option all the
    // way to here. Right now we just rely on the default readahead to keep
    // things simple.
    // TODO: plumb Env::IOActivity, Env::IOPriority
    ReadOptions ro;
    ro.readahead_size = ingestion_options_.verify_checksums_readahead_size;
    ro.fill_cache = ingestion_options_.fill_cache;
    status = table_reader->VerifyChecksum(
        ro, TableReaderCaller::kExternalSSTIngestion);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to verify checksum for external file %s: %s",
                     external_file.c_str(), status.ToString().c_str());
      return status;
    }
  }

  // Assign FD with number.
  file_to_ingest->fd =
      FileDescriptor(new_file_number, 0, file_to_ingest->file_size);

  const size_t ts_sz = ucmp_->timestamp_size();
  Slice smallest = file_to_ingest->smallest_internal_key.user_key();
  Slice largest = file_to_ingest->largest_internal_key.user_key();
  if (ts_sz > 0) {
    AppendUserKeyWithMaxTimestamp(&file_to_ingest->start_ukey, smallest, ts_sz);
    AppendUserKeyWithMinTimestamp(&file_to_ingest->limit_ukey, largest, ts_sz);
  } else {
    file_to_ingest->start_ukey.assign(smallest.data(), smallest.size());
    file_to_ingest->limit_ukey.assign(largest.data(), largest.size());
  }

  auto s =
      GetSstInternalUniqueId(file_to_ingest->table_properties.db_id,
                             file_to_ingest->table_properties.db_session_id,
                             file_to_ingest->table_properties.orig_file_number,
                             &(file_to_ingest->unique_id));
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get SST unique id for file %s",
                   file_to_ingest->internal_file_path.c_str());
    file_to_ingest->unique_id = kNullUniqueId64x2;
  }

  return status;
}

Status ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile(
    bool force_global_seqno, CompactionStyle compaction_style,
    SequenceNumber last_seqno, IngestedFileInfo* file_to_ingest,
    SequenceNumber* assigned_seqno,
    std::optional<int> prev_batch_uppermost_level) {
  Status status;
  *assigned_seqno = 0;
  const size_t ts_sz = ucmp_->timestamp_size();
  assert(!prev_batch_uppermost_level.has_value() ||
         prev_batch_uppermost_level.value() < cfd_->NumberLevels());
  bool must_assign_to_l0 = (prev_batch_uppermost_level.has_value() &&
                            prev_batch_uppermost_level.value() == 0) ||
                           compaction_style == kCompactionStyleFIFO;

  if (force_global_seqno || (!ingestion_options_.allow_db_generated_files &&
                             (files_overlap_ || must_assign_to_l0))) {
    *assigned_seqno = last_seqno + 1;
    if (must_assign_to_l0) {
      assert(ts_sz == 0);
      file_to_ingest->picked_level = 0;
      if (ingestion_options_.fail_if_not_bottommost_level &&
          cfd_->NumberLevels() > 1) {
        status = Status::TryAgain(
            "Files cannot be ingested to Lmax. Please make sure key range of "
            "Lmax does not overlap with files to ingest.");
      }
      return status;
    }
  }

  bool overlap_with_db = false;
  Arena arena;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  ro.total_order_seek = true;
  int target_level = 0;
  auto* vstorage = cfd_->current()->storage_info();
  assert(!must_assign_to_l0 || ingestion_options_.allow_db_generated_files);
  int assigned_level_exclusive_end = cfd_->NumberLevels();
  if (must_assign_to_l0) {
    assigned_level_exclusive_end = 0;
  } else if (prev_batch_uppermost_level.has_value()) {
    assigned_level_exclusive_end = prev_batch_uppermost_level.value();
  }

  // When ingesting db generated files, we require that ingested files do not
  // overlap with any file in the DB. So we need to check all levels.
  int overlap_checking_exclusive_end =
      ingestion_options_.allow_db_generated_files
          ? cfd_->NumberLevels()
          : assigned_level_exclusive_end;
  const bool is_generated_range_tombstone =
      atomic_replace_range_tombstone_active_ &&
      file_to_ingest->generated_for_ingestion;
  for (int lvl = 0; lvl < overlap_checking_exclusive_end; lvl++) {
    if (lvl > 0 && lvl < vstorage->base_level()) {
      continue;
    }
    if (lvl < assigned_level_exclusive_end &&
        atomic_replace_range_.has_value() &&
        !atomic_replace_range_tombstone_active_) {
      target_level = lvl;
      continue;
    }
    if (cfd_->RangeOverlapWithCompaction(file_to_ingest->start_ukey,
                                         file_to_ingest->limit_ukey, lvl,
                                         /*range_limit_exclusive=*/
                                         is_generated_range_tombstone)) {
      // We must use L0 or any level higher than `lvl` to be able to overwrite
      // the compaction output keys that we overlap with in this level, We also
      // need to assign this file a seqno to overwrite the compaction output
      // keys in level `lvl`
      overlap_with_db = true;
      break;
    } else if (vstorage->NumLevelFiles(lvl) > 0) {
      bool overlap_with_level = false;
      status = cfd_->current()->OverlapWithLevelIterator(
          ro, env_options_, file_to_ingest->start_ukey,
          file_to_ingest->limit_ukey, lvl, &overlap_with_level);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to check overlap with level iterator: %s",
                       status.ToString().c_str());
        return status;
      }
      if (overlap_with_level) {
        // We must use L0 or any level higher than `lvl` to be able to overwrite
        // the keys that we overlap with in this level, We also need to assign
        // this file a seqno to overwrite the existing keys in level `lvl`
        overlap_with_db = true;
        break;
      }
    }

    // We don't overlap with any keys in this level, but we still need to check
    // if our file can fit in it
    if (lvl < assigned_level_exclusive_end &&
        IngestedFileFitInLevel(file_to_ingest, lvl)) {
      target_level = lvl;
    }
  }

  if (ingestion_options_.fail_if_not_bottommost_level &&
      target_level < cfd_->NumberLevels() - 1) {
    status = Status::TryAgain(
        "Files cannot be ingested to Lmax. Please make sure key range of Lmax "
        "and ongoing compaction's output to Lmax does not overlap with files "
        "to ingest. Input files overlapping with each other can cause some "
        "file to be assigned to non Lmax level.");
    return status;
  }

  TEST_SYNC_POINT_CALLBACK(
      "ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile",
      &overlap_with_db);
  file_to_ingest->picked_level = target_level;
  if (overlap_with_db) {
    if (ts_sz > 0) {
      status = Status::InvalidArgument(
          "Column family enables user-defined timestamps, please make sure the "
          "key range (without timestamp) of external file does not overlap "
          "with key range (without timestamp) in the db");
      return status;
    }
    if (*assigned_seqno == 0) {
      *assigned_seqno = last_seqno + 1;
    }
  }

  return status;
}

Status ExternalSstFileIngestionJob::CheckLevelForIngestedBehindFile(
    IngestedFileInfo* file_to_ingest) {
  assert(!atomic_replace_range_.has_value());

  auto* vstorage = cfd_->current()->storage_info();
  // First, check if new files fit in the last level
  int last_lvl = cfd_->NumberLevels() - 1;
  if (!IngestedFileFitInLevel(file_to_ingest, last_lvl)) {
    return Status::InvalidArgument(
        "Can't ingest_behind file as it doesn't fit "
        "at the last level!");
  }

  // Second, check if despite cf_allow_ingest_behind=true we still have 0
  // seqnums at some upper level
  for (int lvl = 0; lvl < cfd_->NumberLevels() - 1; lvl++) {
    for (auto file : vstorage->LevelFiles(lvl)) {
      if (file->fd.smallest_seqno == 0) {
        return Status::InvalidArgument(
            "Can't ingest_behind file as despite cf_allow_ingest_behind=true "
            "there are files with 0 seqno in database at upper levels!");
      }
    }
  }

  file_to_ingest->picked_level = last_lvl;
  return Status::OK();
}

Status ExternalSstFileIngestionJob::AssignGlobalSeqnoForIngestedFile(
    IngestedFileInfo* file_to_ingest, SequenceNumber seqno) {
  if (ingestion_options_.allow_db_generated_files) {
    assert(seqno == 0);
    assert(file_to_ingest->original_seqno == 0);
  }
  if (file_to_ingest->original_seqno != seqno) {
    if (!ingestion_options_.allow_global_seqno) {
      return Status::InvalidArgument("Global seqno is required, but disabled");
    }
    if (file_to_ingest->original_seqno != 0) {
      return Status::InvalidArgument(
          "External file global seqno does not match assigned seqno");
    }
  }

  file_to_ingest->assigned_seqno = seqno;
  return Status::OK();
}

bool ExternalSstFileIngestionJob::IngestedFileFitInLevel(
    const IngestedFileInfo* file_to_ingest, int level) {
  if (level == 0) {
    // Files can always fit in L0
    return true;
  }

  auto* vstorage = cfd_->current()->storage_info();
  Slice file_smallest_user_key(file_to_ingest->start_ukey);
  Slice file_largest_user_key(file_to_ingest->limit_ukey);

  if (vstorage->OverlapInLevel(level, &file_smallest_user_key,
                               &file_largest_user_key)) {
    // File overlap with another files in this level, we cannot
    // add it to this level
    return false;
  }

  // File did not overlap with level files, nor compaction output
  return true;
}

Status ExternalSstFileIngestionJob::GetSeqnoBoundaryForFile(
    TableReader* table_reader, SuperVersion* sv,
    IngestedFileInfo* file_to_ingest, bool allow_data_in_errors) {
  const auto tp = table_reader->GetTableProperties();
  const bool has_largest_seqno = tp->HasKeyLargestSeqno();
  SequenceNumber largest_seqno = tp->key_largest_seqno;
  if (has_largest_seqno) {
    file_to_ingest->largest_seqno = largest_seqno;
    if (largest_seqno == 0) {
      file_to_ingest->smallest_seqno = 0;
      return Status::OK();
    }
    if (tp->HasKeySmallestSeqno()) {
      file_to_ingest->smallest_seqno = tp->key_smallest_seqno;
      return Status::OK();
    }
  }

  // For older SST files they may not be recorded in table properties, so
  // we scan the file to find out.
  TEST_SYNC_POINT(
      "ExternalSstFileIngestionJob::GetSeqnoBoundaryForFile:FileScan");
  SequenceNumber smallest_seqno = kMaxSequenceNumber;
  SequenceNumber largest_seqno_from_iter = 0;
  ReadOptions ro;
  ro.fill_cache = ingestion_options_.fill_cache;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ro, sv->mutable_cf_options.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));
  ParsedInternalKey key;
  iter->SeekToFirst();
  while (iter->Valid()) {
    Status pik_status =
        ParseInternalKey(iter->key(), &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    smallest_seqno = std::min(smallest_seqno, key.sequence);
    largest_seqno_from_iter = std::max(largest_seqno_from_iter, key.sequence);
    iter->Next();
  }
  if (!iter->status().ok()) {
    return iter->status();
  }

  if (table_reader->GetTableProperties()->num_range_deletions > 0) {
    std::unique_ptr<InternalIterator> range_del_iter(
        table_reader->NewRangeTombstoneIterator(ro));
    if (range_del_iter != nullptr) {
      for (range_del_iter->SeekToFirst(); range_del_iter->Valid();
           range_del_iter->Next()) {
        Status pik_status =
            ParseInternalKey(range_del_iter->key(), &key, allow_data_in_errors);
        if (!pik_status.ok()) {
          return Status::Corruption("Corrupted key in external file. ",
                                    pik_status.getState());
        }
        smallest_seqno = std::min(smallest_seqno, key.sequence);
        largest_seqno_from_iter =
            std::max(largest_seqno_from_iter, key.sequence);
      }
      if (!range_del_iter->status().ok()) {
        return range_del_iter->status();
      }
    }
  }

  file_to_ingest->smallest_seqno = smallest_seqno;
  if (!has_largest_seqno) {
    file_to_ingest->largest_seqno = largest_seqno_from_iter;
  } else {
    assert(largest_seqno == largest_seqno_from_iter);
    file_to_ingest->largest_seqno = largest_seqno;
  }

  if (file_to_ingest->largest_seqno == kMaxSequenceNumber) {
    return Status::InvalidArgument(
        "Unknown smallest seqno for db generated file.");
  }
  if (file_to_ingest->smallest_seqno == kMaxSequenceNumber) {
    return Status::InvalidArgument(
        "Unknown largest seqno for db generated file.");
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
