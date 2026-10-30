//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <unordered_map>

#include "db/blob/blob_log_format.h"
#include "db/compaction/compaction_job.h"
#include "db/compaction/compaction_state.h"
#include "file/file_util.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/utilities/options_type.h"
#include "util/defer.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
class SubcompactionState;

namespace {

void SaturatingAdd(uint64_t value, uint64_t* total) {
  assert(total);
  if (*total > std::numeric_limits<uint64_t>::max() - value) {
    *total = std::numeric_limits<uint64_t>::max();
  } else {
    *total += value;
  }
}

uint64_t DivideAndRoundUp(uint64_t value, uint64_t divisor) {
  assert(divisor > 0);
  return value / divisor + (value % divisor != 0 ? 1 : 0);
}

Status ValidateRemoteBlobFile(FileSystem* fs, const FileOptions& file_options,
                              const std::string& file_name,
                              const BlobFileAddition& addition) {
  assert(fs != nullptr);
  IOOptions io_options;
  io_options.io_activity = Env::IOActivity::kCompaction;
  constexpr IODebugContext* dbg = nullptr;

  uint64_t file_size = 0;
  IOStatus io_status = fs->GetFileSize(file_name, io_options, &file_size, dbg);
  TEST_SYNC_POINT_CALLBACK("ValidateRemoteBlobFile:AfterGetFileSize",
                           &io_status);
  if (!io_status.ok()) {
    return Status::IOError("Failed to get remote blob file size",
                           io_status.ToString());
  }

  constexpr uint64_t kBlobFileOverhead =
      BlobLogHeader::kSize + BlobLogFooter::kSize;
  const uint64_t total_blob_bytes = addition.GetTotalBlobBytes();
  if (total_blob_bytes >
          std::numeric_limits<uint64_t>::max() - kBlobFileOverhead ||
      file_size != total_blob_bytes + kBlobFileOverhead) {
    return Status::Corruption(
        "Remote blob file size does not match addition metadata");
  }

  FileOptions footer_file_options = file_options;
  footer_file_options.use_direct_reads = false;
  std::unique_ptr<FSRandomAccessFile> file;
  io_status =
      fs->NewRandomAccessFile(file_name, footer_file_options, &file, dbg);
  if (!io_status.ok()) {
    return Status::IOError("Failed to open remote blob file",
                           io_status.ToString());
  }

  std::array<char, BlobLogFooter::kSize> scratch{};
  Slice footer_slice;
  io_status = file->Read(file_size - BlobLogFooter::kSize, BlobLogFooter::kSize,
                         io_options, &footer_slice, scratch.data(), dbg);
  if (!io_status.ok()) {
    return Status::IOError("Failed to read remote blob file footer",
                           io_status.ToString());
  }
  if (footer_slice.size() != BlobLogFooter::kSize) {
    return Status::Corruption("Truncated remote blob file footer");
  }

  BlobLogFooter footer;
  Status status = footer.DecodeFrom(footer_slice);
  if (!status.ok()) {
    return status;
  }
  if (footer.blob_count != addition.GetTotalBlobCount()) {
    return Status::Corruption(
        "Remote blob file count does not match addition metadata");
  }
  return Status::OK();
}

}  // namespace

CompactionServiceJobStatus
CompactionJob::ProcessKeyValueCompactionWithCompactionService(
    SubcompactionState* sub_compact, CompactionFilter::MergeOrder merge_order) {
  assert(sub_compact);
  assert(sub_compact->compaction);
  assert(db_options_.compaction_service);

  const Compaction* const compaction = sub_compact->compaction;
  CompactionServiceInput compaction_input;
  compaction_input.merge_before_filter =
      merge_order == CompactionFilter::MergeOrder::kMergeBeforeFilter;
  compaction_input.output_level = compaction->output_level();
  compaction_input.db_id = db_id_;

  uint64_t oldest_input_blob_file_number = kInvalidBlobFileNumber;
  uint64_t input_raw_value_bytes = 0;
  uint64_t input_file_count = 0;
  uint64_t input_sst_bytes = 0;
  const Comparator* const user_comparator =
      compaction->column_family_data()->user_comparator();
  const std::vector<CompactionInputFiles>& inputs =
      *compact_->compaction->inputs();
  for (const auto& files_per_level : inputs) {
    for (const auto& file : files_per_level.files) {
      compaction_input.input_files.emplace_back(
          MakeTableFileName(file->fd.GetNumber()));
      const bool overlaps_start =
          !sub_compact->start.has_value() ||
          user_comparator->Compare(file->largest.user_key(),
                                   *sub_compact->start) >= 0;
      const bool overlaps_end =
          !sub_compact->end.has_value() ||
          user_comparator->Compare(file->smallest.user_key(),
                                   *sub_compact->end) < 0;
      if (!overlaps_start || !overlaps_end) {
        continue;
      }
      SaturatingAdd(1, &input_file_count);
      SaturatingAdd(file->raw_value_size, &input_raw_value_bytes);
      SaturatingAdd(file->fd.GetFileSize(), &input_sst_bytes);
      if (file->oldest_blob_file_number != kInvalidBlobFileNumber &&
          (oldest_input_blob_file_number == kInvalidBlobFileNumber ||
           file->oldest_blob_file_number < oldest_input_blob_file_number)) {
        oldest_input_blob_file_number = file->oldest_blob_file_number;
      }
    }
  }
  const MutableCFOptions& mutable_cf_options = compaction->mutable_cf_options();
  const VersionStorageInfo* const storage_info =
      compaction->input_version()->storage_info();
  if (mutable_cf_options.enable_blob_garbage_collection) {
    const CompactionIterator::RealCompaction compaction_proxy(compaction);
    compaction_input.has_blob_gc_cutoff_file_number = true;
    compaction_input.blob_gc_cutoff_file_number =
        CompactionIterator::ComputeBlobGarbageCollectionCutoffFileNumber(
            &compaction_proxy);
  }
  uint64_t referenced_blob_bytes = 0;
  uint64_t referenced_blob_count = 0;
  std::set<uint64_t> input_blob_file_numbers;
  if (oldest_input_blob_file_number != kInvalidBlobFileNumber) {
    // An SST only records its oldest blob dependency. It can reference every
    // later blob file in the Version, even though only the oldest file lists
    // the SST in GetLinkedSsts(). Send the full implied suffix to the worker.
    for (const auto& blob_file : storage_info->GetBlobFiles()) {
      assert(blob_file);
      const uint64_t blob_file_number = blob_file->GetBlobFileNumber();
      if (blob_file_number < oldest_input_blob_file_number ||
          !input_blob_file_numbers.insert(blob_file_number).second) {
        continue;
      }
      compaction_input.input_blob_files.emplace_back(
          BlobFileName(blob_file_number));
      assert(blob_file->GetGarbageBlobCount() <=
             blob_file->GetTotalBlobCount());
      SaturatingAdd(
          blob_file->GetTotalBlobCount() - blob_file->GetGarbageBlobCount(),
          &referenced_blob_count);
      assert(blob_file->GetGarbageBlobBytes() <=
             blob_file->GetTotalBlobBytes());
      SaturatingAdd(
          blob_file->GetTotalBlobBytes() - blob_file->GetGarbageBlobBytes(),
          &referenced_blob_bytes);
    }
  }
  const bool blob_eligible_compaction =
      mutable_cf_options.enable_blob_files &&
      compaction->output_level() >= mutable_cf_options.blob_file_starting_level;
  if (blob_eligible_compaction) {
    // Estimate output from the actual live blob payload and count, plus raw
    // values that can be converted into blobs. Resumption closes a blob file
    // at each checkpointed output SST boundary, so the physical output-file
    // estimate and input SST count are additional lower bounds.
    const uint64_t blob_file_size =
        std::max<uint64_t>(1, mutable_cf_options.blob_file_size);
    const uint64_t estimated_rewritten_blob_files =
        std::min(referenced_blob_count,
                 DivideAndRoundUp(referenced_blob_bytes, blob_file_size));
    uint64_t estimated_data_file_count = estimated_rewritten_blob_files;
    SaturatingAdd(DivideAndRoundUp(input_raw_value_bytes, blob_file_size),
                  &estimated_data_file_count);
    const uint64_t estimated_output_file_count = DivideAndRoundUp(
        input_sst_bytes,
        std::max<uint64_t>(1, compaction->max_output_file_size()));
    // Size-based rotation and resumable-checkpoint tail closure are
    // independent, so their bounds are additive.
    SaturatingAdd(estimated_output_file_count, &estimated_data_file_count);
    const uint64_t estimated_file_count =
        std::max(input_file_count, estimated_data_file_count);
    // Allow one partially filled tail file and one file of expansion from a
    // compaction filter. If the estimate is still too small, the worker
    // returns Incomplete and the primary safely retries locally.
    uint64_t range_size =
        estimated_file_count <= std::numeric_limits<uint64_t>::max() - 2
            ? estimated_file_count + 2
            : std::numeric_limits<uint64_t>::max();
    TEST_SYNC_POINT_CALLBACK(
        "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
        "BlobFileNumberRangeSize",
        &range_size);
    if (!versions_->TryAllocateFileNumberRange(
            range_size, &compaction_input.blob_file_number_start)) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "[%s] [JOB %d] Unable to reserve blob file numbers; "
                     "falling back to local compaction",
                     compaction->column_family_data()->GetName().c_str(),
                     job_id_);
      return CompactionServiceJobStatus::kUseLocal;
    }
    compaction_input.blob_file_number_limit =
        compaction_input.blob_file_number_start + range_size;
  }

  compaction_input.cf_name = compaction->column_family_data()->GetName();
  compaction_input.snapshots = job_context_->snapshot_seqs;
  compaction_input.has_begin = sub_compact->start.has_value();
  compaction_input.begin =
      compaction_input.has_begin ? sub_compact->start->ToString() : "";
  compaction_input.has_end = sub_compact->end.has_value();
  compaction_input.end =
      compaction_input.has_end ? sub_compact->end->ToString() : "";
  compaction_input.options_file_number = options_file_number_;
  compaction_input.min_manifest_file_number = min_manifest_file_number_;
  compaction_input.min_manifest_file_size = min_manifest_file_size_;
  compaction_input.preserve_internal_time_seconds =
      compaction->mutable_cf_options().preserve_internal_time_seconds;
  compaction_input.preclude_last_level_data_seconds =
      compaction->mutable_cf_options().preclude_last_level_data_seconds;
  compaction_input.preserve_time_min_seqno_floor =
      preserve_time_min_seqno_floor_;
  compaction_input.proximal_level = compaction->GetProximalLevel();
  compaction_input.proximal_output_range_type =
      static_cast<int>(compaction->GetProximalOutputRangeType());
  compaction_input.keep_in_last_level_through_seqno =
      compaction->GetKeepInLastLevelThroughSeqno();

  TEST_SYNC_POINT_CALLBACK(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      &compaction_input);

  std::string compaction_input_binary;
  Status s = compaction_input.Write(&compaction_input_binary);
  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  std::ostringstream input_files_oss;
  bool is_first_one = true;
  for (const auto& file : compaction_input.input_files) {
    input_files_oss << (is_first_one ? "" : ", ") << file;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Starting remote compaction (output level: %d): %s",
      compaction->column_family_data()->GetName().c_str(), job_id_,
      compaction_input.output_level, input_files_oss.str().c_str());
  CompactionServiceJobInfo info(
      dbname_, db_id_, db_session_id_,
      compaction->column_family_data()->GetID(),
      compaction->column_family_data()->GetName(), GetCompactionId(sub_compact),
      thread_pri_, compaction->compaction_reason(),
      compaction->is_full_compaction(), compaction->is_manual_compaction(),
      compaction->bottommost_level(), compaction->start_level(),
      compaction->output_level());
  info.merge_before_filter = compaction_input.merge_before_filter;
  CompactionServiceScheduleResponse response =
      db_options_.compaction_service->Schedule(info, compaction_input_binary);
  switch (response.status) {
    case CompactionServiceJobStatus::kSuccess:
      if (oldest_input_blob_file_number != kInvalidBlobFileNumber) {
        uint64_t current = remote_compaction_min_blob_file_number_.load(
            std::memory_order_relaxed);
        while (oldest_input_blob_file_number < current &&
               !remote_compaction_min_blob_file_number_.compare_exchange_weak(
                   current, oldest_input_blob_file_number,
                   std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
      }
      break;
    case CompactionServiceJobStatus::kAborted:
      sub_compact->status =
          Status::Aborted("Scheduling a remote compaction job was aborted");
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "[%s] [JOB %d] Remote compaction was aborted at Schedule()",
          compaction->column_family_data()->GetName().c_str(), job_id_);
      return response.status;
    case CompactionServiceJobStatus::kFailure:
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to schedule a remote compaction job.");
      ROCKS_LOG_WARN(db_options_.info_log,
                     "[%s] [JOB %d] Remote compaction failed to start.",
                     compaction->column_family_data()->GetName().c_str(),
                     job_id_);
      return response.status;
    case CompactionServiceJobStatus::kUseLocal:
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[%s] [JOB %d] Remote compaction fallback to local by API (Schedule)",
          compaction->column_family_data()->GetName().c_str(), job_id_);
      return response.status;
    default:
      assert(false);  // unknown status
      break;
  }

  // TODO: Update CompactionService API to support abort and resume
  // functionality. Currently, remote compaction jobs cannot be aborted via
  // AbortAllCompactions() because the CompactionService interface lacks methods
  // to signal abort to remote workers and to properly resume after an abort.
  // The API needs to be extended with:
  // - A method to signal abort to running remote compaction jobs
  // - A method to resume/re-enable scheduling after an abort is lifted

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Waiting for remote compaction...",
                 compaction->column_family_data()->GetName().c_str(), job_id_);
  std::string compaction_result_binary;
  CompactionServiceJobStatus compaction_status;
  {
    // Increment rocksdb.num-running-remote-compactions while this compaction
    // service job is waiting in CompactionService::Wait().
    std::atomic<int>* const num_running = num_running_remote_compactions_;
    if (num_running != nullptr) {
      num_running->fetch_add(1, std::memory_order_relaxed);
    }
    Defer decrement_num_running([num_running]() {
      if (num_running != nullptr) {
        num_running->fetch_sub(1, std::memory_order_relaxed);
      }
    });
    compaction_status = db_options_.compaction_service->Wait(
        response.scheduled_job_id, &compaction_result_binary);
  }

  if (compaction_status != CompactionServiceJobStatus::kSuccess) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] Wait() status is not kSuccess. "
        "\nDebugString After Wait():\n%s",
        compaction->column_family_data()->GetName().c_str(), job_id_,
        compaction->input_version()->DebugString(/*hex=*/true).c_str());
  }

  if (compaction_status == CompactionServiceJobStatus::kUseLocal) {
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[%s] [JOB %d] Remote compaction fallback to local by API (Wait)",
        compaction->column_family_data()->GetName().c_str(), job_id_);
    return compaction_status;
  }

  if (compaction_status == CompactionServiceJobStatus::kAborted) {
    sub_compact->status =
        Status::Aborted("Waiting a remote compaction job was aborted");
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction was aborted during Wait()",
                   compaction->column_family_data()->GetName().c_str(),
                   job_id_);
    return compaction_status;
  }

  CompactionServiceResult compaction_result;
  s = CompactionServiceResult::Read(compaction_result_binary,
                                    &compaction_result);

  if (s.ok() && compaction_result.merge_order_mismatch &&
      (compaction_status == CompactionServiceJobStatus::kSuccess ||
       compaction_status == CompactionServiceJobStatus::kFailure)) {
    if (!compaction_result.status.IsNotSupported() ||
        !compaction_result.output_files.empty() ||
        !compaction_result.blob_file_additions.empty() ||
        !compaction_result.blob_file_garbages.empty() ||
        compaction_result.stats.num_input_records != 0 ||
        compaction_result.num_purge_normalized != 0 ||
        compaction_result.merge_before_filter_version != 0) {
      sub_compact->status =
          Status::Corruption("Invalid remote compaction filter order decline");
      db_options_.compaction_service->OnInstallation(
          response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
      return CompactionServiceJobStatus::kFailure;
    }
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Worker declined compaction filter order; "
                   "falling back to local compaction",
                   compaction->column_family_data()->GetName().c_str(),
                   job_id_);
    db_options_.compaction_service->OnInstallation(
        response.scheduled_job_id, CompactionServiceJobStatus::kUseLocal);
    return CompactionServiceJobStatus::kUseLocal;
  }

  if (compaction_status == CompactionServiceJobStatus::kFailure) {
    if (s.ok()) {
      if (compaction_result.status.ok()) {
        sub_compact->status = Status::Incomplete(
            "CompactionService failed to run the compaction job (even though "
            "the internal status is okay).");
      } else {
        // set the current sub compaction status with the status returned from
        // remote
        sub_compact->status = compaction_result.status;
      }
    } else {
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to run the compaction job (and no valid "
          "result is returned).");
      compaction_result.status.PermitUncheckedError();
    }
    ROCKS_LOG_WARN(
        db_options_.info_log, "[%s] [JOB %d] Remote compaction failed.",
        compaction->column_family_data()->GetName().c_str(), job_id_);
    return compaction_status;
  }

  if (!s.ok()) {
    // Wait() returned kSuccess, but the primary host could not deserialize the
    // remote result before importing any output files into this DB. The remote
    // output is still isolated in the service-managed staging directory.
    // Surface a recoverable failure so the CompactionService implementation
    // can apply its own retry or local-fallback policy.
    assert(sub_compact->status.ok());
    assert(sub_compact->GetMutableCompactionOutputs().empty());
    assert(sub_compact->GetMutableProximalOutputs().empty());
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Declining malformed remote compaction "
                   "result (%s)",
                   compaction->column_family_data()->GetName().c_str(), job_id_,
                   s.ToString().c_str());
    compaction_result.status.PermitUncheckedError();
    db_options_.compaction_service->OnInstallation(
        response.scheduled_job_id, CompactionServiceJobStatus::kUseLocal);
    return CompactionServiceJobStatus::kUseLocal;
  }

  // The worker checks that its per-key placement matches this compaction's.
  // As a backstop, never install a proximal-level output when this compaction
  // has no proximal level. Nothing has been imported yet, so fall back to
  // local compaction as above.
  if (!compaction->SupportsPerKeyPlacement() &&
      std::any_of(compaction_result.output_files.begin(),
                  compaction_result.output_files.end(),
                  [](const CompactionServiceOutputFile& file) {
                    return file.is_proximal_level_output;
                  })) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction returned proximal-level "
                   "output, but this compaction has no proximal level; "
                   "falling back to local compaction",
                   compaction->column_family_data()->GetName().c_str(),
                   job_id_);
    compaction_result.status.PermitUncheckedError();
    db_options_.compaction_service->OnInstallation(
        response.scheduled_job_id, CompactionServiceJobStatus::kUseLocal);
    return CompactionServiceJobStatus::kUseLocal;
  }
  sub_compact->status = compaction_result.status;
  if (!sub_compact->status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction returned a non-OK result: "
                   "%s",
                   compaction->column_family_data()->GetName().c_str(), job_id_,
                   sub_compact->status.ToString().c_str());
    db_options_.compaction_service->OnInstallation(
        response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
    return CompactionServiceJobStatus::kFailure;
  }

  const uint64_t expected_merge_version =
      compaction_input.merge_before_filter
          ? kCompactionServiceMergeBeforeFilterVersion
          : 0;
  if (compaction_result.merge_before_filter_version != expected_merge_version) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Declining remote compaction filter order "
                   "acknowledgment; falling back to local compaction",
                   compaction->column_family_data()->GetName().c_str(),
                   job_id_);
    db_options_.compaction_service->OnInstallation(
        response.scheduled_job_id, CompactionServiceJobStatus::kUseLocal);
    return CompactionServiceJobStatus::kUseLocal;
  }

  auto decline_blob_result = [&](const std::string& reason) {
    RecordTick(stats_, REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT);
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Cannot install remote BlobDB compaction "
                   "result: %s; falling back to local compaction",
                   compaction->column_family_data()->GetName().c_str(), job_id_,
                   reason.c_str());
    db_options_.compaction_service->OnInstallation(
        response.scheduled_job_id, CompactionServiceJobStatus::kUseLocal);
    return CompactionServiceJobStatus::kUseLocal;
  };

  if ((oldest_input_blob_file_number != kInvalidBlobFileNumber ||
       blob_eligible_compaction) &&
      compaction_result.blob_compaction_version !=
          kCompactionServiceBlobVersion) {
    return decline_blob_result(
        "worker does not support BlobDB remote compaction");
  }
  if (compaction_input.has_blob_gc_cutoff_file_number &&
      compaction_result.blob_gc_cutoff_version !=
          kCompactionServiceBlobGCCutoffVersion) {
    return decline_blob_result(
        "worker does not support a primary-provided blob GC cutoff");
  }

  const VerifyOutputFlags verify_output_flags =
      compaction->mutable_cf_options().verify_output_flags;
  const bool should_verify_blob_file_checksums =
      !!(verify_output_flags & VerifyOutputFlags::kEnableForRemoteCompaction) &&
      !!(verify_output_flags & VerifyOutputFlags::kVerifyFileChecksum) &&
      db_options_.file_checksum_gen_factory != nullptr;

  std::set<uint64_t> blob_file_numbers;
  struct BlobFileState {
    uint64_t total_blob_count;
    uint64_t total_blob_bytes;
    uint64_t garbage_blob_count;
    uint64_t garbage_blob_bytes;
  };
  std::unordered_map<uint64_t, BlobFileState> known_blob_files;
  uint64_t expected_blob_file_number = compaction_input.blob_file_number_start;
  for (const auto& addition : compaction_result.blob_file_additions) {
    const uint64_t file_number = addition.GetBlobFileNumber();
    if (!blob_file_numbers.insert(file_number).second) {
      return decline_blob_result("duplicate blob file number");
    }
    if (compaction_input.blob_file_number_start == kInvalidBlobFileNumber ||
        compaction_input.blob_file_number_limit <=
            compaction_input.blob_file_number_start ||
        file_number != expected_blob_file_number ||
        file_number >= compaction_input.blob_file_number_limit) {
      return decline_blob_result(
          "blob file number is not the next number in the reserved range");
    }
    if (addition.GetTotalBlobCount() == 0 ||
        addition.GetTotalBlobBytes() == 0) {
      return decline_blob_result("invalid blob file addition metadata");
    }
    if (addition.GetChecksumMethod().empty() !=
        addition.GetChecksumValue().empty()) {
      if (should_verify_blob_file_checksums) {
        return decline_blob_result(
            "worker returned incomplete blob file checksum metadata");
      }
      return decline_blob_result("invalid blob file addition metadata");
    }
    if (should_verify_blob_file_checksums) {
      if (addition.GetChecksumMethod().empty()) {
        return decline_blob_result(
            "worker did not provide a blob file checksum");
      }
      FileChecksumGenContext checksum_context;
      checksum_context.file_name = BlobFileName(compaction_result.output_path,
                                                addition.GetBlobFileNumber());
      checksum_context.requested_checksum_func_name =
          addition.GetChecksumMethod();
      const std::unique_ptr<FileChecksumGenerator> checksum_generator =
          db_options_.file_checksum_gen_factory->CreateFileChecksumGenerator(
              checksum_context);
      if (checksum_generator == nullptr ||
          checksum_generator->Name() != addition.GetChecksumMethod()) {
        return decline_blob_result(
            "worker blob checksum method is unsupported by the primary");
      }
    }
    known_blob_files.emplace(file_number,
                             BlobFileState{addition.GetTotalBlobCount(),
                                           addition.GetTotalBlobBytes(), 0, 0});
    ++expected_blob_file_number;
  }

  for (const auto& blob_file : storage_info->GetBlobFiles()) {
    if (!known_blob_files
             .emplace(blob_file->GetBlobFileNumber(),
                      BlobFileState{blob_file->GetTotalBlobCount(),
                                    blob_file->GetTotalBlobBytes(),
                                    blob_file->GetGarbageBlobCount(),
                                    blob_file->GetGarbageBlobBytes()})
             .second) {
      return decline_blob_result(
          "new blob file number collides with an existing blob file");
    }
  }
  std::set<uint64_t> garbage_file_numbers;
  for (const auto& garbage : compaction_result.blob_file_garbages) {
    const uint64_t file_number = garbage.GetBlobFileNumber();
    if (blob_file_numbers.count(file_number) != 0) {
      return decline_blob_result(
          "worker returned garbage for a newly added blob file");
    }
    const std::unordered_map<uint64_t, BlobFileState>::const_iterator
        blob_file = known_blob_files.find(file_number);
    if (file_number == kInvalidBlobFileNumber ||
        input_blob_file_numbers.count(file_number) == 0 ||
        blob_file == known_blob_files.end() ||
        garbage.GetGarbageBlobCount() == 0 ||
        garbage.GetGarbageBlobBytes() == 0) {
      return decline_blob_result("invalid blob file garbage metadata");
    }
    if (!garbage_file_numbers.insert(file_number).second) {
      return decline_blob_result("duplicate blob file garbage metadata");
    }
    const BlobFileState& state = blob_file->second;
    if (state.garbage_blob_count > state.total_blob_count ||
        state.garbage_blob_bytes > state.total_blob_bytes ||
        garbage.GetGarbageBlobCount() >
            state.total_blob_count - state.garbage_blob_count ||
        garbage.GetGarbageBlobBytes() >
            state.total_blob_bytes - state.garbage_blob_bytes) {
      return decline_blob_result(
          "blob file garbage exceeds file totals for file " +
          std::to_string(file_number) + ": count " +
          std::to_string(state.garbage_blob_count) + "+" +
          std::to_string(garbage.GetGarbageBlobCount()) + "/" +
          std::to_string(state.total_blob_count) + ", bytes " +
          std::to_string(state.garbage_blob_bytes) + "+" +
          std::to_string(garbage.GetGarbageBlobBytes()) + "/" +
          std::to_string(state.total_blob_bytes));
    }
  }
  for (const auto& file : compaction_result.output_files) {
    if (file.oldest_blob_file_number != kInvalidBlobFileNumber &&
        known_blob_files.count(file.oldest_blob_file_number) == 0) {
      return decline_blob_result(
          "output SST refers to an unknown oldest blob file");
    }
  }

  for (const auto& addition : compaction_result.blob_file_additions) {
    const uint64_t file_number = addition.GetBlobFileNumber();
    const std::string src_file =
        BlobFileName(compaction_result.output_path, file_number);
    const IOStatus source_exists =
        fs_->FileExists(src_file, IOOptions(), /*dbg=*/nullptr);
    if (!source_exists.ok()) {
      if (source_exists.IsNotFound()) {
        return decline_blob_result("blob file is missing from worker output");
      }
      sub_compact->status = source_exists;
      db_options_.compaction_service->OnInstallation(
          response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
      return CompactionServiceJobStatus::kFailure;
    }
    const Status validation_status = ValidateRemoteBlobFile(
        fs_.get(), file_options_for_read_, src_file, addition);
    if (!validation_status.ok()) {
      if (validation_status.IsIOError()) {
        sub_compact->status = validation_status;
        db_options_.compaction_service->OnInstallation(
            response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
        return CompactionServiceJobStatus::kFailure;
      }
      return decline_blob_result(validation_status.ToString());
    }
    const std::string tgt_file = BlobFileName(
        compaction->immutable_options().cf_paths.front().path, file_number);
    const IOStatus exists =
        fs_->FileExists(tgt_file, IOOptions(), /*dbg=*/nullptr);
    if (exists.ok()) {
      return decline_blob_result("blob file already exists on the primary");
    }
    if (!exists.IsNotFound()) {
      sub_compact->status = exists;
      db_options_.compaction_service->OnInstallation(
          response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
      return CompactionServiceJobStatus::kFailure;
    }
  }

  auto validate_remote_blob_garbage = [&](bool commit) {
    std::string error;
    MutexLock lock(&remote_blob_garbage_mutex_);
    for (const auto& garbage : compaction_result.blob_file_garbages) {
      const BlobFileState& state =
          known_blob_files.at(garbage.GetBlobFileNumber());
      const auto aggregate_it =
          remote_blob_garbage_.find(garbage.GetBlobFileNumber());
      const uint64_t aggregate_count =
          aggregate_it == remote_blob_garbage_.end()
              ? 0
              : aggregate_it->second.GetCount();
      const uint64_t aggregate_bytes =
          aggregate_it == remote_blob_garbage_.end()
              ? 0
              : aggregate_it->second.GetBytes();
      const uint64_t available_blob_count =
          state.total_blob_count - state.garbage_blob_count;
      const uint64_t available_blob_bytes =
          state.total_blob_bytes - state.garbage_blob_bytes;
      if (aggregate_count > available_blob_count ||
          aggregate_bytes > available_blob_bytes ||
          garbage.GetGarbageBlobCount() >
              available_blob_count - aggregate_count ||
          garbage.GetGarbageBlobBytes() >
              available_blob_bytes - aggregate_bytes) {
        error =
            "aggregate blob garbage across subcompactions exceeds file "
            "totals";
        break;
      }
    }
    if (error.empty() && commit) {
      for (const auto& garbage : compaction_result.blob_file_garbages) {
        remote_blob_garbage_[garbage.GetBlobFileNumber()].Add(
            garbage.GetGarbageBlobCount(), garbage.GetGarbageBlobBytes());
      }
    }
    if (!commit) {
      [[maybe_unused]] bool aggregate_is_empty = remote_blob_garbage_.empty();
      TEST_SYNC_POINT_CALLBACK(
          "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
          "AfterRemoteBlobGarbagePrevalidation",
          &aggregate_is_empty);
    }
    return error;
  };
  std::string aggregate_garbage_error =
      validate_remote_blob_garbage(/*commit=*/false);
  if (!aggregate_garbage_error.empty()) {
    return decline_blob_result(aggregate_garbage_error);
  }
  std::ostringstream output_files_oss;
  is_first_one = true;
  for (const auto& file : compaction_result.output_files) {
    output_files_oss << (is_first_one ? "" : ", ") << file.file_name;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Received remote compaction result, output path: "
      "%s, files: %s",
      compaction->column_family_data()->GetName().c_str(), job_id_,
      compaction_result.output_path.c_str(), output_files_oss.str().c_str());

  // Installation Starts
  CompactionOutputs* const current_outputs = sub_compact->Outputs(false);
  assert(current_outputs);
  std::vector<std::string> installed_blob_files;
  std::vector<std::string> registered_blob_files;
  auto* const sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  const size_t initial_blob_addition_count =
      current_outputs->GetBlobFileAdditions().size();
  const size_t initial_output_path_count =
      current_outputs->GetOutputFilePaths().size();
  auto cleanup_installed_blob_files = [&]() {
    Status first_cleanup_error;
    for (auto it = installed_blob_files.rbegin();
         it != installed_blob_files.rend(); ++it) {
      IOStatus cleanup_status;
      TEST_SYNC_POINT_CALLBACK(
          "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
          "BeforeBlobFileDelete",
          &cleanup_status);
      if (cleanup_status.ok()) {
        cleanup_status = fs_->DeleteFile(*it, IOOptions(), /*dbg=*/nullptr);
      }
      if (!cleanup_status.ok() && !cleanup_status.IsNotFound()) {
        if (first_cleanup_error.ok()) {
          first_cleanup_error = cleanup_status;
        }
        ROCKS_LOG_WARN(db_options_.info_log,
                       "[%s] [JOB %d] Failed to clean up remote blob file %s: "
                       "%s",
                       compaction->column_family_data()->GetName().c_str(),
                       job_id_, it->c_str(), cleanup_status.ToString().c_str());
        continue;
      }
      if (sfm != nullptr &&
          std::find(registered_blob_files.begin(), registered_blob_files.end(),
                    *it) != registered_blob_files.end()) {
        const Status unregister_status = sfm->OnDeleteFile(*it);
        if (!unregister_status.ok() && first_cleanup_error.ok()) {
          first_cleanup_error = unregister_status;
        }
      }
    }
    registered_blob_files.clear();
    installed_blob_files.clear();
    current_outputs->GetBlobFileAdditionsPtr()->resize(
        initial_blob_addition_count);
    current_outputs->GetOutputFilePathsPtr()->resize(initial_output_path_count);
    TEST_SYNC_POINT_CALLBACK(
        "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
        "AfterBlobFileRollback",
        current_outputs);
    return first_cleanup_error;
  };
  for (const auto& addition : compaction_result.blob_file_additions) {
    const uint64_t file_number = addition.GetBlobFileNumber();
    const std::string src_file =
        BlobFileName(compaction_result.output_path, file_number);
    const std::string tgt_file = BlobFileName(
        compaction->immutable_options().cf_paths.front().path, file_number);
    s = fs_->RenameFile(src_file, tgt_file, IOOptions(), nullptr);
    if (s.ok()) {
      installed_blob_files.push_back(tgt_file);
      if (blob_callback_ != nullptr) {
        TEST_SYNC_POINT(
            "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
            "BeforeBlobFileRegistration");
        s = blob_callback_->OnBlobFileCompleted(
            tgt_file, compaction->column_family_data()->GetName(), job_id_,
            file_number, BlobFileCreationReason::kCompaction, s,
            addition.GetChecksumValue(), addition.GetChecksumMethod(),
            addition.GetTotalBlobCount(), addition.GetTotalBlobBytes(),
            /*set_bg_error_on_space_limit=*/false);
        if (sfm != nullptr) {
          registered_blob_files.push_back(tgt_file);
        }
      }
      if (s.ok()) {
        current_outputs->AddOutputFilePath(tgt_file);
        current_outputs->GetBlobFileAdditionsPtr()->push_back(addition);
      }
    }
    TEST_SYNC_POINT_CALLBACK(
        "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
        "AfterBlobFileRename",
        &s);
    if (!s.ok()) {
      const Status cleanup_status = cleanup_installed_blob_files();
      if (!cleanup_status.ok()) {
        sub_compact->status = cleanup_status;
        db_options_.compaction_service->OnInstallation(
            response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
        return CompactionServiceJobStatus::kFailure;
      }
      if (s.subcode() == Status::SubCode::kSpaceLimit) {
        return decline_blob_result(s.ToString());
      }
      sub_compact->status = s;
      db_options_.compaction_service->OnInstallation(
          response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
      return CompactionServiceJobStatus::kFailure;
    }
  }

  if (should_verify_blob_file_checksums) {
    ReadOptions verification_read_options(Env::IOActivity::kCompaction);
    verification_read_options.readahead_size =
        file_options_for_read_.compaction_readahead_size;
    verification_read_options.rate_limiter_priority = GetRateLimiterPriority();
    for (const auto& addition : compaction_result.blob_file_additions) {
      assert(!addition.GetChecksumValue().empty());
      const std::string blob_file =
          BlobFileName(compaction->immutable_options().cf_paths.front().path,
                       addition.GetBlobFileNumber());
      std::string actual_checksum;
      std::string actual_checksum_method;
      s = GenerateOneFileChecksum(
          fs_.get(), blob_file, db_options_.file_checksum_gen_factory.get(),
          addition.GetChecksumMethod(), &actual_checksum,
          &actual_checksum_method, verification_read_options.readahead_size,
          db_options_.allow_mmap_reads, io_tracer_,
          db_options_.rate_limiter.get(), verification_read_options, stats_,
          db_options_.clock, file_options_for_read_);
      if (s.ok() && actual_checksum != addition.GetChecksumValue()) {
        const Status cleanup_status = cleanup_installed_blob_files();
        if (!cleanup_status.ok()) {
          sub_compact->status = cleanup_status;
          db_options_.compaction_service->OnInstallation(
              response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
          return CompactionServiceJobStatus::kFailure;
        }
        return decline_blob_result(
            "file checksum mismatch for remote compaction blob file " +
            blob_file);
      }
      if (!s.ok()) {
        const Status cleanup_status = cleanup_installed_blob_files();
        if (!cleanup_status.ok()) {
          s = cleanup_status;
        }
        sub_compact->status = s;
        db_options_.compaction_service->OnInstallation(
            response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
        return CompactionServiceJobStatus::kFailure;
      }
    }
  }

  std::vector<std::string> installed_sst_files;
  std::vector<CompactionOutputs::Output>& normal_outputs =
      sub_compact->GetMutableCompactionOutputs();
  std::vector<CompactionOutputs::Output>& proximal_outputs =
      sub_compact->GetMutableProximalOutputs();
  const size_t initial_output_count = normal_outputs.size();
  const size_t initial_proximal_output_count = proximal_outputs.size();
  auto cleanup_installed_sst_files = [&]() {
    for (auto it = installed_sst_files.rbegin();
         it != installed_sst_files.rend(); ++it) {
      fs_->DeleteFile(*it, IOOptions(), /*dbg=*/nullptr).PermitUncheckedError();
    }
    installed_sst_files.clear();
    while (normal_outputs.size() > initial_output_count) {
      normal_outputs.pop_back();
    }
    while (proximal_outputs.size() > initial_proximal_output_count) {
      proximal_outputs.pop_back();
    }
  };
  for (const auto& file : compaction_result.output_files) {
    uint64_t file_num = versions_->NewFileNumber();
    auto src_file = compaction_result.output_path + "/" + file.file_name;
    auto tgt_file = TableFileName(compaction->immutable_options().cf_paths,
                                  file_num, compaction->output_path_id());
    s = fs_->RenameFile(src_file, tgt_file, IOOptions(), nullptr);
    if (!s.ok()) {
      cleanup_installed_sst_files();
      const Status cleanup_status = cleanup_installed_blob_files();
      if (!cleanup_status.ok()) {
        s = cleanup_status;
      }
      sub_compact->status = s;
      db_options_.compaction_service->OnInstallation(
          response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
      return CompactionServiceJobStatus::kFailure;
    }

    FileMetaData meta;
    uint64_t file_size = file.file_size;

    // TODO - Clean this up in the next release.
    // For backward compatibility - in case the remote worker does not populate
    // the file_size yet. If missing, continue to populate this from the file
    // system.
    if (file_size == 0) {
      s = fs_->GetFileSize(tgt_file, IOOptions(), &file_size, nullptr);
    }

    if (!s.ok()) {
      fs_->DeleteFile(tgt_file, IOOptions(), /*dbg=*/nullptr)
          .PermitUncheckedError();
      cleanup_installed_sst_files();
      const Status cleanup_status = cleanup_installed_blob_files();
      if (!cleanup_status.ok()) {
        s = cleanup_status;
      }
      sub_compact->status = s;
      db_options_.compaction_service->OnInstallation(
          response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
      return CompactionServiceJobStatus::kFailure;
    }
    assert(file_size > 0);

    meta.fd = FileDescriptor(file_num, compaction->output_path_id(), file_size,
                             file.smallest_seqno, file.largest_seqno);
    meta.smallest.DecodeFrom(file.smallest_internal_key);
    meta.largest.DecodeFrom(file.largest_internal_key);
    meta.oldest_blob_file_number = file.oldest_blob_file_number;
    meta.oldest_ancestor_time = file.oldest_ancestor_time;
    meta.file_creation_time = file.file_creation_time;
    meta.epoch_number = file.epoch_number;
    meta.file_checksum = file.file_checksum;
    meta.file_checksum_func_name = file.file_checksum_func_name;
    meta.marked_for_compaction = file.marked_for_compaction;
    meta.unique_id = file.unique_id;
    meta.temperature = file.file_temperature;
    meta.tail_size =
        FileMetaData::CalculateTailSize(file_size, file.table_properties);
    auto cfd = compaction->column_family_data();
    CompactionOutputs* compaction_outputs =
        sub_compact->Outputs(file.is_proximal_level_output);
    assert(compaction_outputs);
    compaction_outputs->AddOutput(std::move(meta), cfd->internal_comparator(),
                                  false, true, file.paranoid_hash);
    compaction_outputs->UpdateTableProperties(file.table_properties);
    installed_sst_files.push_back(tgt_file);
  }

  aggregate_garbage_error = validate_remote_blob_garbage(/*commit=*/true);
  if (!aggregate_garbage_error.empty()) {
    cleanup_installed_sst_files();
    const Status cleanup_status = cleanup_installed_blob_files();
    if (!cleanup_status.ok()) {
      sub_compact->status = cleanup_status;
      db_options_.compaction_service->OnInstallation(
          response.scheduled_job_id, CompactionServiceJobStatus::kFailure);
      return CompactionServiceJobStatus::kFailure;
    }
    return decline_blob_result(aggregate_garbage_error);
  }

  if (!compaction_result.blob_file_garbages.empty()) {
    BlobGarbageMeter* const garbage_meter =
        current_outputs->CreateBlobGarbageMeter();
    for (const auto& garbage : compaction_result.blob_file_garbages) {
      garbage_meter->AddGarbage(garbage.GetBlobFileNumber(),
                                garbage.GetGarbageBlobCount(),
                                garbage.GetGarbageBlobBytes());
    }
  }

  // Set per-level stats
  auto compaction_output_stats =
      sub_compact->OutputStats(false /* is_proximal_level */);
  assert(compaction_output_stats);
  compaction_output_stats->Add(
      compaction_result.internal_stats.output_level_stats);
  if (compaction->SupportsPerKeyPlacement()) {
    compaction_output_stats =
        sub_compact->OutputStats(true /* is_proximal_level */);
    assert(compaction_output_stats);
    compaction_output_stats->Add(
        compaction_result.internal_stats.proximal_level_stats);
  }

  // Set job stats
  sub_compact->compaction_job_stats = compaction_result.stats;
  sub_compact->num_purge_normalized = compaction_result.num_purge_normalized;
  RecordTick(stats_, COMPACTION_FILTER_PURGE_NORMALIZED,
             sub_compact->num_purge_normalized);

  RecordTick(stats_, REMOTE_COMPACT_READ_BYTES, compaction_result.bytes_read);
  RecordTick(stats_, REMOTE_COMPACT_WRITE_BYTES,
             compaction_result.bytes_written);
  db_options_.compaction_service->OnInstallation(
      response.scheduled_job_id, CompactionServiceJobStatus::kSuccess);
  return CompactionServiceJobStatus::kSuccess;
}

std::string CompactionServiceCompactionJob::GetTableFileName(
    uint64_t file_number) {
  return MakeTableFileName(output_path_, file_number);
}

const std::string& CompactionServiceCompactionJob::GetBlobOutputPath() const {
  return output_path_;
}

uint64_t CompactionServiceCompactionJob::NewBlobFileNumber() {
  if (next_blob_file_number_ >= compaction_input_.blob_file_number_limit) {
    return kInvalidBlobFileNumber;
  }
  uint64_t file_number = next_blob_file_number_++;
  TEST_SYNC_POINT_CALLBACK("CompactionServiceCompactionJob::NewBlobFileNumber",
                           &file_number);
  return file_number;
}

uint64_t CompactionServiceCompactionJob::GetNextBlobFileNumber() const {
  return next_blob_file_number_;
}

std::optional<uint64_t>
CompactionServiceCompactionJob::GetBlobGarbageCollectionCutoffFileNumber()
    const {
  if (!compaction_input_.has_blob_gc_cutoff_file_number) {
    return 0;
  }
  return compaction_input_.blob_gc_cutoff_file_number;
}

Status CompactionServiceCompactionJob::RestoreNextBlobFileNumber(
    const SubcompactionProgress& subcompaction_progress) {
  const bool has_blob_work =
      compaction_input_.blob_file_number_start != kInvalidBlobFileNumber ||
      !compaction_input_.input_blob_files.empty();
  if (!has_blob_work) {
    return Status::OK();
  }
  if (!subcompaction_progress.has_blob_compaction_progress) {
    return Status::Incomplete(
        "Cannot safely resume BlobDB compaction from legacy progress");
  }

  const uint64_t range_start = compaction_input_.blob_file_number_start;
  const uint64_t range_limit = compaction_input_.blob_file_number_limit;
  const uint64_t saved_next = subcompaction_progress.next_blob_file_number;
  if (range_start == kInvalidBlobFileNumber) {
    if (!subcompaction_progress.blob_file_additions.empty() ||
        saved_next != kInvalidBlobFileNumber) {
      return Status::Incomplete(
          "Blob compaction progress does not match the reserved range");
    }
    next_blob_file_number_ = kInvalidBlobFileNumber;
    return Status::OK();
  }
  if (range_limit <= range_start || saved_next < range_start ||
      saved_next > range_limit) {
    return Status::Incomplete(
        "Blob compaction progress is outside the reserved range");
  }

  uint64_t expected = range_start;
  for (const auto& addition : subcompaction_progress.blob_file_additions) {
    if (addition.GetBlobFileNumber() != expected || expected >= range_limit) {
      return Status::Incomplete(
          "Blob compaction progress has non-consecutive file numbers");
    }
    ++expected;
  }
  if (saved_next != expected) {
    return Status::Incomplete(
        "Blob compaction progress has an invalid next file number");
  }

  next_blob_file_number_ = saved_next;
  return Status::OK();
}

void CompactionServiceCompactionJob::RecordCompactionIOStats() {
  compaction_result_->bytes_read += IOSTATS(bytes_read);
  compaction_result_->bytes_written += IOSTATS(bytes_written);
  CompactionJob::RecordCompactionIOStats();
}

CompactionServiceCompactionJob::CompactionServiceCompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    LogBuffer* log_buffer, FSDirectory* output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    JobContext* job_context, std::shared_ptr<Cache> table_cache,
    EventLogger* event_logger, const std::string& dbname,
    const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<bool>& manual_compaction_canceled,
    const std::string& db_id, const std::string& db_session_id,
    std::string output_path,
    const CompactionServiceInput& compaction_service_input,
    CompactionServiceResult* compaction_service_result)
    : CompactionJob(
          job_id, compaction, db_options, mutable_db_options, file_options,
          versions, shutting_down, log_buffer, nullptr, output_directory,
          nullptr, stats, db_mutex, db_error_handler, job_context,
          std::move(table_cache), event_logger,
          compaction->mutable_cf_options().paranoid_file_checks,
          compaction->mutable_cf_options().report_bg_io_stats, dbname,
          &(compaction_service_result->stats), Env::Priority::USER, io_tracer,
          manual_compaction_canceled, CompactionJob::kCompactionAbortedFalse,
          db_id, db_session_id,
          compaction->column_family_data()->GetFullHistoryTsLow()),
      output_path_(std::move(output_path)),
      compaction_input_(compaction_service_input),
      next_blob_file_number_(compaction_service_input.blob_file_number_start),
      compaction_result_(compaction_service_result) {}

void CompactionServiceCompactionJob::Prepare(
    const CompactionProgress& compaction_progress,
    log::Writer* compaction_progress_writer) {
  std::optional<Slice> begin;
  if (compaction_input_.has_begin) {
    begin = compaction_input_.begin;
  }
  std::optional<Slice> end;
  if (compaction_input_.has_end) {
    end = compaction_input_.end;
  }
  // CompactionJob::Prepare() replaces the floor for a kBottommostFiles
  // compaction. The worker always rebuilds the compaction as a manual one, so
  // the primary's floor is kept.
  assert(compact_->compaction->compaction_reason() !=
         CompactionReason::kBottommostFiles);
  preserve_time_min_seqno_floor_ =
      compaction_input_.preserve_time_min_seqno_floor;
  CompactionJob::Prepare(std::make_pair(begin, end), compaction_progress,
                         compaction_progress_writer);
}

Status CompactionServiceCompactionJob::ValidateCompactionFilterMergeOrder(
    CompactionFilter::MergeOrder merge_order) {
  const bool actual_merge_before_filter =
      merge_order == CompactionFilter::MergeOrder::kMergeBeforeFilter;
  if (actual_merge_before_filter != compaction_input_.merge_before_filter) {
    compaction_result_->merge_order_mismatch = true;
    return Status::NotSupported(
        "Remote compaction filter order does not match the primary request");
  }
  compaction_result_->merge_before_filter_version =
      actual_merge_before_filter ? kCompactionServiceMergeBeforeFilterVersion
                                 : 0;
  return Status::OK();
}

Status CompactionServiceCompactionJob::Run() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);

  auto* c = compact_->compaction;

  log_buffer_->FlushBufferToLog();
  LogCompaction();

  compaction_result_->stats.Reset();

  const uint64_t start_micros = db_options_.clock->NowMicros();
  c->GetOrInitInputTableProperties();

  // Pick the only sub-compaction we should have
  assert(compact_->sub_compact_states.size() == 1);
  SubcompactionState* sub_compact = compact_->sub_compact_states.data();

  ProcessKeyValueCompaction(sub_compact);
  compaction_result_->num_purge_normalized = sub_compact->num_purge_normalized;

  CompactionOutputs* const outputs = sub_compact->Outputs(false);
  assert(outputs);
  compaction_result_->blob_compaction_version = kCompactionServiceBlobVersion;
  compaction_result_->blob_gc_cutoff_version =
      kCompactionServiceBlobGCCutoffVersion;
  compaction_result_->blob_file_additions = outputs->GetBlobFileAdditions();
  compaction_result_->blob_file_garbages.clear();
  const BlobGarbageMeter* const garbage_meter = outputs->GetBlobGarbageMeter();
  if (garbage_meter != nullptr) {
    for (const auto& [file_number, flow] : garbage_meter->flows()) {
      if (!flow.IsValid()) {
        sub_compact->status = Status::Corruption(
            "Remote compaction produced invalid blob garbage flow");
        break;
      }
      if (flow.HasGarbage()) {
        compaction_result_->blob_file_garbages.emplace_back(
            file_number, flow.GetGarbageCount(), flow.GetGarbageBytes());
      }
    }
  }

  uint64_t elapsed_micros = db_options_.clock->NowMicros() - start_micros;
  internal_stats_.SetMicros(elapsed_micros);
  internal_stats_.AddCpuMicros(elapsed_micros);

  RecordTimeToHistogram(stats_, COMPACTION_TIME,
                        internal_stats_.output_level_stats.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        internal_stats_.output_level_stats.cpu_micros);

  Status status = sub_compact->status;
  IOStatus io_s = sub_compact->io_status;

  if (io_status_.ok()) {
    io_status_ = io_s;
  }

  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (output_directory_) {
      io_s = output_directory_->FsyncWithDirOptions(IOOptions(), dbg,
                                                    DirFsyncOptions());
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }

  LogFlush(db_options_.info_log);
  compact_->status = status;
  compact_->status.PermitUncheckedError();

  // Build Compaction Job Stats

  // 1. Aggregate internal stats and job stats for all subcompactions
  // internal stats: sub_compact.proximal_level_outputs_.stats and
  //                 sub_compact.compaction_outputs_.stats into
  //                 internal_stats_.output_level_stats and
  //                 internal_stats_.proximal_level_stats
  // job-level stats: sub_compact.compaction_job_stats into compact.job_stats_
  //
  // For remote compaction, there's only one subcompaction.
  compact_->AggregateCompactionStats(internal_stats_, *job_stats_);

  // 2. Update job-level output stats with the aggregated internal_stats_
  // Please note that input stats will be updated by primary host when all
  // subcompactions are finished
  UpdateCompactionJobOutputStatsFromInternalStats(status, internal_stats_);
  // Whole-file filtering changes which keys are fed to the iterator, so the
  // iterator-based input record count should not be treated as exact.
  for (size_t level = 1; level < c->num_input_levels(); ++level) {
    if (!c->filtered_input_levels(level).empty()) {
      compaction_result_->stats.has_accurate_num_input_records = false;
      break;
    }
  }
  // and set fields that are not propagated as part of the update
  compaction_result_->stats.is_manual_compaction = c->is_manual_compaction();
  compaction_result_->stats.is_full_compaction = c->is_full_compaction();
  compaction_result_->stats.is_remote_compaction = true;

  // 3. Update IO Stats that are not part of the the update above
  // (bytes_read, bytes_written)
  RecordCompactionIOStats();

  // Build Output
  compaction_result_->internal_stats = internal_stats_;
  compaction_result_->output_level = compact_->compaction->output_level();
  compaction_result_->output_path = output_path_;
  if (status.ok()) {
    for (const auto& output_file : sub_compact->GetOutputs()) {
      auto& meta = output_file.meta;
      compaction_result_->output_files.emplace_back(
          MakeTableFileName(meta.fd.GetNumber()), meta.fd.GetFileSize(),
          meta.fd.smallest_seqno, meta.fd.largest_seqno,
          meta.smallest.Encode().ToString(), meta.largest.Encode().ToString(),
          meta.oldest_blob_file_number, meta.oldest_ancestor_time,
          meta.file_creation_time, meta.epoch_number, meta.file_checksum,
          meta.file_checksum_func_name, output_file.validator.GetHash(),
          meta.marked_for_compaction, meta.unique_id,
          *output_file.table_properties, output_file.is_proximal_level,
          meta.temperature);
    }
  }

  TEST_SYNC_POINT_CALLBACK("CompactionServiceCompactionJob::Run:0",
                           &compaction_result_);
  return status;
}

void CompactionServiceCompactionJob::CleanupCompaction() {
  CompactionJob::CleanupCompaction();
}

// Internal binary format for the input and result data
namespace {

enum BinaryFormatVersion : uint32_t {
  kOptionsString = 1,  // Use string format similar to Option string format
};

template <typename T>
OptionTypeInfo EncodedBlobMetadataType() {
  return OptionTypeInfo(
      0, OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions&, const std::string& name,
         const std::string& value, void* addr) {
        std::string decoded;
        if (!Slice(value).DecodeHex(&decoded)) {
          return Status::InvalidArgument("Invalid hex encoding for ", name);
        }
        Slice input(decoded);
        Status s = static_cast<T*>(addr)->DecodeFrom(&input);
        if (s.ok() && !input.empty()) {
          return Status::Corruption("Trailing bytes in ", name);
        }
        return s;
      },
      [](const ConfigOptions&, const std::string&, const void* addr,
         std::string* value) {
        std::string encoded;
        static_cast<const T*>(addr)->EncodeTo(&encoded);
        // Hex protects binary metadata from options-string delimiters.
        *value = Slice(encoded).ToString(/*hex=*/true);
        return Status::OK();
      },
      [](const ConfigOptions&, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        if (*static_cast<const T*>(addr1) == *static_cast<const T*>(addr2)) {
          return true;
        }
        *mismatch = name;
        return false;
      });
}

}  // namespace

static std::unordered_map<std::string, OptionTypeInfo> cfd_type_info = {
    {"name",
     {offsetof(struct ColumnFamilyDescriptor, name), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"options",
     {offsetof(struct ColumnFamilyDescriptor, options),
      OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto cf_options = static_cast<ColumnFamilyOptions*>(addr);
        return GetColumnFamilyOptionsFromString(opts, ColumnFamilyOptions(),
                                                value, cf_options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto cf_options = static_cast<const ColumnFamilyOptions*>(addr);
        std::string result;
        auto status =
            GetStringFromColumnFamilyOptions(opts, *cf_options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const ColumnFamilyOptions*>(addr1);
        const auto that_one = static_cast<const ColumnFamilyOptions*>(addr2);
        auto this_conf = CFOptionsAsConfigurable(*this_one);
        auto that_conf = CFOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_input_type_info = {
    {"merge_before_filter",
     {offsetof(struct CompactionServiceInput, merge_before_filter),
      OptionType::kBoolean}},
    {"cf_name",
     {offsetof(struct CompactionServiceInput, cf_name),
      OptionType::kEncodedString}},
    {"snapshots", OptionTypeInfo::Vector<uint64_t>(
                      offsetof(struct CompactionServiceInput, snapshots),
                      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                      {0, OptionType::kUInt64T})},
    {"input_files", OptionTypeInfo::Vector<std::string>(
                        offsetof(struct CompactionServiceInput, input_files),
                        OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                        {0, OptionType::kEncodedString})},
    {"input_blob_files",
     OptionTypeInfo::Vector<std::string>(
         offsetof(struct CompactionServiceInput, input_blob_files),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         {0, OptionType::kEncodedString})},
    {"has_blob_gc_cutoff_file_number",
     {offsetof(struct CompactionServiceInput, has_blob_gc_cutoff_file_number),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"blob_gc_cutoff_file_number",
     {offsetof(struct CompactionServiceInput, blob_gc_cutoff_file_number),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"blob_file_number_start",
     {offsetof(struct CompactionServiceInput, blob_file_number_start),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"blob_file_number_limit",
     {offsetof(struct CompactionServiceInput, blob_file_number_limit),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"output_level",
     {offsetof(struct CompactionServiceInput, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"db_id",
     {offsetof(struct CompactionServiceInput, db_id),
      OptionType::kEncodedString}},
    {"has_begin",
     {offsetof(struct CompactionServiceInput, has_begin), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"begin",
     {offsetof(struct CompactionServiceInput, begin),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"has_end",
     {offsetof(struct CompactionServiceInput, has_end), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"end",
     {offsetof(struct CompactionServiceInput, end), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"options_file_number",
     {offsetof(struct CompactionServiceInput, options_file_number),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"min_manifest_file_number",
     {offsetof(struct CompactionServiceInput, min_manifest_file_number),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"min_manifest_file_size",
     {offsetof(struct CompactionServiceInput, min_manifest_file_size),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"preserve_internal_time_seconds",
     {offsetof(struct CompactionServiceInput, preserve_internal_time_seconds),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"preclude_last_level_data_seconds",
     {offsetof(struct CompactionServiceInput, preclude_last_level_data_seconds),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"preserve_time_min_seqno_floor",
     {offsetof(struct CompactionServiceInput, preserve_time_min_seqno_floor),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"proximal_level",
     {offsetof(struct CompactionServiceInput, proximal_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"proximal_output_range_type",
     {offsetof(struct CompactionServiceInput, proximal_output_range_type),
      OptionType::kInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"keep_in_last_level_through_seqno",
     {offsetof(struct CompactionServiceInput, keep_in_last_level_through_seqno),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    cs_output_file_type_info = {
        {"file_name",
         {offsetof(struct CompactionServiceOutputFile, file_name),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_size",
         {offsetof(struct CompactionServiceOutputFile, file_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_seqno",
         {offsetof(struct CompactionServiceOutputFile, smallest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_seqno",
         {offsetof(struct CompactionServiceOutputFile, largest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, smallest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, largest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"oldest_blob_file_number",
         {offsetof(struct CompactionServiceOutputFile, oldest_blob_file_number),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        // Misspelled key kept for compatibility with other versions
        {"oldest_ancester_time",
         {offsetof(struct CompactionServiceOutputFile, oldest_ancestor_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_creation_time",
         {offsetof(struct CompactionServiceOutputFile, file_creation_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"epoch_number",
         {offsetof(struct CompactionServiceOutputFile, epoch_number),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_checksum",
         {offsetof(struct CompactionServiceOutputFile, file_checksum),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_checksum_func_name",
         {offsetof(struct CompactionServiceOutputFile, file_checksum_func_name),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"paranoid_hash",
         {offsetof(struct CompactionServiceOutputFile, paranoid_hash),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"marked_for_compaction",
         {offsetof(struct CompactionServiceOutputFile, marked_for_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"unique_id",
         OptionTypeInfo::Array<uint64_t, 2>(
             offsetof(struct CompactionServiceOutputFile, unique_id),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone,
             {0, OptionType::kUInt64T})},
        {"table_properties",
         {offsetof(struct CompactionServiceOutputFile, table_properties),
          OptionType::kStruct, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto table_properties = static_cast<TableProperties*>(addr);
            return TableProperties::Parse(opts, value, table_properties);
          },
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const void* addr, std::string* value) {
            const auto table_properties =
                static_cast<const TableProperties*>(addr);
            std::string result;
            auto status = table_properties->Serialize(opts, &result);
            *value = "{" + result + "}";
            return status;
          },
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const void* addr1, const void* addr2, std::string* mismatch) {
            const auto this_one = static_cast<const TableProperties*>(addr1);
            const auto that_one = static_cast<const TableProperties*>(addr2);
            return this_one->AreEqual(opts, that_one, mismatch);
          }}},
        {"is_proximal_level_output",
         {offsetof(struct CompactionServiceOutputFile,
                   is_proximal_level_output),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_temperature",
         {offsetof(struct CompactionServiceOutputFile, file_temperature),
          OptionType::kTemperature, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}}};

static std::unordered_map<std::string, OptionTypeInfo>
    compaction_job_stats_type_info = {
        {"elapsed_micros",
         {offsetof(struct CompactionJobStats, elapsed_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cpu_micros",
         {offsetof(struct CompactionJobStats, cpu_micros), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"has_accurate_num_input_records",
         {offsetof(struct CompactionJobStats, has_accurate_num_input_records),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_records",
         {offsetof(struct CompactionJobStats, num_input_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_blobs_read",
         {offsetof(struct CompactionJobStats, num_blobs_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files",
         {offsetof(struct CompactionJobStats, num_input_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files_at_output_level",
         {offsetof(struct CompactionJobStats, num_input_files_at_output_level),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_records",
         {offsetof(struct CompactionJobStats, num_output_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files",
         {offsetof(struct CompactionJobStats, num_output_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files_blob",
         {offsetof(struct CompactionJobStats, num_output_files_blob),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_full_compaction",
         {offsetof(struct CompactionJobStats, is_full_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_manual_compaction",
         {offsetof(struct CompactionJobStats, is_manual_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_remote_compaction",
         {offsetof(struct CompactionJobStats, is_remote_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_bytes",
         {offsetof(struct CompactionJobStats, total_input_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_blob_bytes_read",
         {offsetof(struct CompactionJobStats, total_blob_bytes_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes",
         {offsetof(struct CompactionJobStats, total_output_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes_blob",
         {offsetof(struct CompactionJobStats, total_output_bytes_blob),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_records_replaced",
         {offsetof(struct CompactionJobStats, num_records_replaced),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_key_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_key_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_value_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_value_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_deletion_records",
         {offsetof(struct CompactionJobStats, num_input_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_expired_deletion_records",
         {offsetof(struct CompactionJobStats, num_expired_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_corrupt_keys",
         {offsetof(struct CompactionJobStats, num_corrupt_keys),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_write_nanos",
         {offsetof(struct CompactionJobStats, file_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_range_sync_nanos",
         {offsetof(struct CompactionJobStats, file_range_sync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_fsync_nanos",
         {offsetof(struct CompactionJobStats, file_fsync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_prepare_write_nanos",
         {offsetof(struct CompactionJobStats, file_prepare_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_output_key_prefix",
         {offsetof(struct CompactionJobStats, smallest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_output_key_prefix",
         {offsetof(struct CompactionJobStats, largest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_fallthru",
         {offsetof(struct CompactionJobStats, num_single_del_fallthru),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_mismatch",
         {offsetof(struct CompactionJobStats, num_single_del_mismatch),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

namespace {

// Like OptionTypeInfo::Array<T, kSize> but deserialization tolerates
// size mismatches: extra elements beyond kSize are silently ignored,
// missing elements are zero-filled. Serialization and comparison are
// inherited from Array<T, kSize> unchanged; only the parse
// (deserialize) callback is overridden.
template <typename T, size_t kSize>
OptionTypeInfo SizeTolerantDeserializeArray(int offset,
                                            OptionVerificationType verification,
                                            OptionTypeFlags flags,
                                            const OptionTypeInfo& elem_info,
                                            char separator = ':') {
  return OptionTypeInfo::Array<T, kSize>(offset, verification, flags, elem_info,
                                         separator)
      .SetParseFunc([elem_info, separator](
                        const ConfigOptions& opts, const std::string& name,
                        const std::string& value, void* addr) {
        std::vector<T> parsed;
        Status s =
            ParseVector<T>(opts, elem_info, separator, name, value, &parsed);
        if (!s.ok()) {
          return Status(std::move(s));
        }
        auto* arr = static_cast<std::array<T, kSize>*>(addr);
        for (size_t i = 0; i < kSize; ++i) {
          (*arr)[i] = (i < parsed.size()) ? parsed[i] : T{};
        }
        return Status::OK();
      });
}

}  // namespace

static std::unordered_map<std::string, OptionTypeInfo>
    compaction_stats_type_info = {
        {"micros",
         {offsetof(struct InternalStats::CompactionStats, micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cpu_micros",
         {offsetof(struct InternalStats::CompactionStats, cpu_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_read_non_output_levels",
         {offsetof(struct InternalStats::CompactionStats,
                   bytes_read_non_output_levels),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_read_output_level",
         {offsetof(struct InternalStats::CompactionStats,
                   bytes_read_output_level),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_skipped_non_output_levels",
         {offsetof(struct InternalStats::CompactionStats,
                   bytes_skipped_non_output_levels),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_skipped_output_level",
         {offsetof(struct InternalStats::CompactionStats,
                   bytes_skipped_output_level),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_read_blob",
         {offsetof(struct InternalStats::CompactionStats, bytes_read_blob),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_written",
         {offsetof(struct InternalStats::CompactionStats, bytes_written),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_written_blob",
         {offsetof(struct InternalStats::CompactionStats, bytes_written_blob),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bytes_moved",
         {offsetof(struct InternalStats::CompactionStats, bytes_moved),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files_in_non_output_levels",
         {offsetof(struct InternalStats::CompactionStats,
                   num_input_files_in_non_output_levels),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files_in_output_level",
         {offsetof(struct InternalStats::CompactionStats,
                   num_input_files_in_output_level),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_filtered_input_files_in_non_output_levels",
         {offsetof(struct InternalStats::CompactionStats,
                   num_filtered_input_files_in_non_output_levels),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_filtered_input_files_in_output_level",
         {offsetof(struct InternalStats::CompactionStats,
                   num_filtered_input_files_in_output_level),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files",
         {offsetof(struct InternalStats::CompactionStats, num_output_files),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files_blob",
         {offsetof(struct InternalStats::CompactionStats,
                   num_output_files_blob),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_records",
         {offsetof(struct InternalStats::CompactionStats, num_input_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_dropped_records",
         {offsetof(struct InternalStats::CompactionStats, num_dropped_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_records",
         {offsetof(struct InternalStats::CompactionStats, num_output_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"count",
         {offsetof(struct InternalStats::CompactionStats, count),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"counts", SizeTolerantDeserializeArray<
                       int, static_cast<int>(CompactionReason::kNumOfReasons)>(
                       offsetof(struct InternalStats::CompactionStats, counts),
                       OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                       {0, OptionType::kInt})},
};

static std::unordered_map<std::string, OptionTypeInfo>
    compaction_internal_stats_type_info = {
        {"output_level_stats",
         OptionTypeInfo::Struct(
             "output_level_stats", &compaction_stats_type_info,
             offsetof(struct InternalStats::CompactionStatsFull,
                      output_level_stats),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
        {"has_proximal_level_output",
         {offsetof(struct InternalStats::CompactionStatsFull,
                   has_proximal_level_output),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"proximal_level_stats",
         OptionTypeInfo::Struct(
             "proximal_level_stats", &compaction_stats_type_info,
             offsetof(struct InternalStats::CompactionStatsFull,
                      proximal_level_stats),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
};

namespace {
// this is a helper struct to serialize and deserialize class Status, because
// Status's members are not public.
struct StatusSerializationAdapter {
  uint8_t code;
  uint8_t subcode;
  uint8_t severity;
  std::string message;

  StatusSerializationAdapter() = default;
  explicit StatusSerializationAdapter(const Status& s) {
    code = s.code();
    subcode = s.subcode();
    severity = s.severity();
    auto msg = s.getState();
    message = msg ? msg : "";
  }

  Status GetStatus() const {
    return Status{static_cast<Status::Code>(code),
                  static_cast<Status::SubCode>(subcode),
                  static_cast<Status::Severity>(severity), message};
  }
};
}  // namespace

static std::unordered_map<std::string, OptionTypeInfo>
    status_adapter_type_info = {
        {"code",
         {offsetof(struct StatusSerializationAdapter, code),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"subcode",
         {offsetof(struct StatusSerializationAdapter, subcode),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"severity",
         {offsetof(struct StatusSerializationAdapter, severity),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"message",
         {offsetof(struct StatusSerializationAdapter, message),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_result_type_info = {
    {"merge_before_filter_version",
     {offsetof(struct CompactionServiceResult, merge_before_filter_version),
      OptionType::kUInt64T}},
    {"merge_order_mismatch",
     {offsetof(struct CompactionServiceResult, merge_order_mismatch),
      OptionType::kBoolean}},
    {"num_purge_normalized",
     {offsetof(struct CompactionServiceResult, num_purge_normalized),
      OptionType::kUInt64T}},
    {"status",
     {offsetof(struct CompactionServiceResult, status),
      OptionType::kCustomizable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto status_obj = static_cast<Status*>(addr);
        StatusSerializationAdapter adapter;
        Status s = OptionTypeInfo::ParseType(
            opts, value, status_adapter_type_info, &adapter);
        *status_obj = adapter.GetStatus();
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto status_obj = static_cast<const Status*>(addr);
        StatusSerializationAdapter adapter(*status_obj);
        std::string result;
        Status s = OptionTypeInfo::SerializeType(opts, status_adapter_type_info,
                                                 &adapter, &result);
        *value = "{" + result + "}";
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr1, const void* addr2, std::string* mismatch) {
        const auto status1 = static_cast<const Status*>(addr1);
        const auto status2 = static_cast<const Status*>(addr2);

        StatusSerializationAdapter adatper1(*status1);
        StatusSerializationAdapter adapter2(*status2);
        return OptionTypeInfo::TypesAreEqual(opts, status_adapter_type_info,
                                             &adatper1, &adapter2, mismatch);
      }}},
    {"output_files",
     OptionTypeInfo::Vector<CompactionServiceOutputFile>(
         offsetof(struct CompactionServiceResult, output_files),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         OptionTypeInfo::Struct("output_files", &cs_output_file_type_info, 0,
                                OptionVerificationType::kNormal,
                                OptionTypeFlags::kNone))},
    {"blob_file_additions",
     OptionTypeInfo::Vector<BlobFileAddition>(
         offsetof(struct CompactionServiceResult, blob_file_additions),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         EncodedBlobMetadataType<BlobFileAddition>())},
    {"blob_file_garbages",
     OptionTypeInfo::Vector<BlobFileGarbage>(
         offsetof(struct CompactionServiceResult, blob_file_garbages),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         EncodedBlobMetadataType<BlobFileGarbage>())},
    {"blob_compaction_version",
     {offsetof(struct CompactionServiceResult, blob_compaction_version),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"blob_gc_cutoff_version",
     {offsetof(struct CompactionServiceResult, blob_gc_cutoff_version),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"output_level",
     {offsetof(struct CompactionServiceResult, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"output_path",
     {offsetof(struct CompactionServiceResult, output_path),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_read",
     {offsetof(struct CompactionServiceResult, bytes_read),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_written",
     {offsetof(struct CompactionServiceResult, bytes_written),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"stats", OptionTypeInfo::Struct(
                  "stats", &compaction_job_stats_type_info,
                  offsetof(struct CompactionServiceResult, stats),
                  OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"internal_stats",
     OptionTypeInfo::Struct(
         "internal_stats", &compaction_internal_stats_type_info,
         offsetof(struct CompactionServiceResult, internal_stats),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
};

Status CompactionServiceInput::Read(const std::string& data_str,
                                    CompactionServiceInput* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceInput string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    obj->merge_before_filter = false;
    obj->input_blob_files.clear();
    obj->has_blob_gc_cutoff_file_number = false;
    obj->blob_gc_cutoff_file_number = 0;
    obj->blob_file_number_start = kInvalidBlobFileNumber;
    obj->blob_file_number_limit = kInvalidBlobFileNumber;
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    Status s = OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_input_type_info,
        obj);
    if (!s.ok()) {
      return s;
    }
    if ((obj->blob_file_number_start == kInvalidBlobFileNumber) !=
            (obj->blob_file_number_limit == kInvalidBlobFileNumber) ||
        (obj->blob_file_number_start != kInvalidBlobFileNumber &&
         obj->blob_file_number_start >= obj->blob_file_number_limit)) {
      return Status::InvalidArgument(
          "CompactionServiceInput has an invalid blob file number range");
    }
    for (size_t i = 1; i < obj->snapshots.size(); ++i) {
      if (obj->snapshots[i - 1] >= obj->snapshots[i]) {
        return Status::InvalidArgument(
            "CompactionServiceInput snapshots must be strictly increasing");
      }
    }
    return Status::OK();
  } else {
    return Status::NotSupported(
        "Compaction Service Input data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceInput::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_input_type_info, this, output);
}

Status CompactionServiceResult::Read(const std::string& data_str,
                                     CompactionServiceResult* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceResult string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    obj->merge_before_filter_version = 0;
    obj->merge_order_mismatch = false;
    obj->num_purge_normalized = 0;
    obj->blob_file_additions.clear();
    obj->blob_file_garbages.clear();
    obj->blob_compaction_version = 0;
    obj->blob_gc_cutoff_version = 0;
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_result_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Result data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceResult::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_result_type_info, this, output);
}

#ifndef NDEBUG
bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other,
                                          std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_result_type_info, this, other,
                                       mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other,
                                         std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_input_type_info, this, other,
                                       mismatch);
}
#endif  // NDEBUG
}  // namespace ROCKSDB_NAMESPACE
