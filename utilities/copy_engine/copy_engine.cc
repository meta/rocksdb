//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/copy_engine/copy_engine.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <string>

#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/backup_engine.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"
#include "util/file_checksum_helper.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {

CopyEngine::CopyEngine(CopyEngineOptions options)
    : options_(std::move(options)),
      threads_cpu_priority_(CpuPriority::kNormal) {
  threads_.reserve(options_.max_background_operations);
  for (int t = 0; t < options_.max_background_operations; t++) {
    threads_.emplace_back([this]() { ThreadBody(); });
  }
}

CopyEngine::~CopyEngine() {
  work_items_.sendEof();
  for (auto& t : threads_) {
    t.join();
  }
}

void CopyEngine::Submit(WorkItem&& item) { work_items_.write(std::move(item)); }

void CopyEngine::MaybeDecreaseCpuPriority(CpuPriority priority) {
  if (priority < threads_cpu_priority_) {
    threads_cpu_priority_.store(priority);
  }
}

void CopyEngine::ThreadBody() {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  pthread_setname_np(pthread_self(), options_.thread_name.c_str());
#endif
#endif
  CpuPriority current_priority = CpuPriority::kNormal;
  WorkItem work_item;
  uint64_t bytes_toward_next_callback = 0;
  while (work_items_.read(work_item)) {
    CpuPriority priority = threads_cpu_priority_;
    if (current_priority != priority) {
      TEST_SYNC_POINT_CALLBACK("BackupEngineImpl::Initialize:SetCpuPriority",
                               &priority);
      port::SetCpuPriority(0, priority);
      current_priority = priority;
    }
    // `bytes_read` and `bytes_written` stats are enabled based on compile-time
    // support and cannot be dynamically toggled. So we do not need to worry
    // about `PerfLevel` here, unlike many other `IOStatsContext` /
    // `PerfContext` stats.
    uint64_t prev_bytes_read = IOSTATS(bytes_read);
    uint64_t prev_bytes_written = IOSTATS(bytes_written);

    WorkItemResult result;
    if (work_item.type == WorkItemType::CopyOrCreate) {
      Temperature temp = work_item.src_temperature;
      result.io_status = CopyOrCreateFile(
          work_item.src_path, work_item.dst_path, work_item.contents,
          work_item.size_limit, work_item.src_env, work_item.dst_env,
          work_item.src_env_options, work_item.sync, work_item.use_fsync,
          work_item.rate_limiter, work_item.progress_callback, &temp,
          work_item.dst_temperature, &bytes_toward_next_callback, &result.size,
          &result.checksum_hex);

      RecordTick(work_item.stats, options_.read_bytes_ticker,
                 IOSTATS(bytes_read) - prev_bytes_read);
      RecordTick(work_item.stats, options_.write_bytes_ticker,
                 IOSTATS(bytes_written) - prev_bytes_written);

      result.db_id = work_item.db_id;
      result.db_session_id = work_item.db_session_id;
      result.expected_src_temperature = work_item.src_temperature;
      result.current_src_temperature = temp;
      if (result.io_status.ok() && !work_item.src_checksum_hex.empty()) {
        // unknown checksum function name implies no db table file checksum in
        // db manifest; work_item.src_checksum_hex not empty means backup engine
        // has calculated its crc32c checksum for the table file; therefore, we
        // are able to compare the checksums.
        if (work_item.src_checksum_func_name == kUnknownFileChecksumFuncName ||
            work_item.src_checksum_func_name == kDbFileChecksumFuncName) {
          if (work_item.src_checksum_hex != result.checksum_hex) {
            std::string checksum_info(
                "Expected checksum is " + work_item.src_checksum_hex +
                " while computed checksum is " + result.checksum_hex);
            result.io_status =
                IOStatus::Corruption("Checksum mismatch after copying to " +
                                     work_item.dst_path + ": " + checksum_info);
          }
        } else {
          // FIXME(peterd): dead code?
          std::string checksum_function_info(
              "Existing checksum function is " +
              work_item.src_checksum_func_name +
              " while provided checksum function is " +
              kBackupFileChecksumFuncName);
          ROCKS_LOG_INFO(options_.info_log,
                         "Unable to verify checksum after copying to %s: %s\n",
                         work_item.dst_path.c_str(),
                         checksum_function_info.c_str());
        }
      }
      if (result.io_status.ok() && !work_item.final_dst_path.empty()) {
        result.io_status = work_item.dst_env->GetFileSystem()->RenameFile(
            work_item.dst_path, work_item.final_dst_path, IOOptions(), nullptr);
      }
    } else if (work_item.type == ComputeChecksum) {
      result.io_status = ReadFileAndComputeChecksum(
          work_item.src_path, work_item.src_env->GetFileSystem(),
          work_item.src_env_options, work_item.size_limit, &result.checksum_hex,
          work_item.src_temperature);
      result.db_id = work_item.db_id;
      result.db_session_id = work_item.db_session_id;
    } else if (work_item.type == Link) {
      result.io_status =
          LinkFile(work_item.src_path, work_item.dst_path, work_item.dst_env);
    } else {
      result.io_status = IOStatus::InvalidArgument(
          "Unknown work item type: " + std::to_string(work_item.type));
    }
    work_item.result.set_value(std::move(result));
  }
}

IOStatus CopyEngine::CopyOrCreateFile(
    const std::string& src, const std::string& dst, const std::string& contents,
    uint64_t size_limit, Env* src_env, Env* dst_env,
    const EnvOptions& src_env_options, bool sync, bool use_fsync,
    RateLimiter* rate_limiter, const std::function<void()>& progress_callback,
    Temperature* src_temperature, Temperature dst_temperature,
    uint64_t* bytes_toward_next_callback, uint64_t* size,
    std::string* checksum_hex) {
  assert(src.empty() != contents.empty());
  if (ShouldAbort()) {
    return status_to_io_status(
        Status::Incomplete(options_.abort_status_message));
  }

  IOStatus io_s;
  std::unique_ptr<FSWritableFile> dst_file;
  std::unique_ptr<FSSequentialFile> src_file;
  FileOptions dst_file_options;
  dst_file_options.use_mmap_writes = false;
  dst_file_options.temperature = dst_temperature;
  // TODO:(gzh) maybe use direct reads/writes here if possible
  if (size != nullptr) {
    *size = 0;
  }
  uint32_t checksum_value = 0;

  // Check if size limit is set. if not, set it to very big number
  if (size_limit == 0) {
    size_limit = std::numeric_limits<uint64_t>::max();
  }

  io_s = dst_env->GetFileSystem()->NewWritableFile(dst, dst_file_options,
                                                   &dst_file, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }

  auto src_file_options = FileOptions(src_env_options);
  if (!src.empty()) {
    src_file_options.temperature = *src_temperature;
    io_s = src_env->GetFileSystem()->NewSequentialFile(src, src_file_options,
                                                       &src_file, nullptr);
  }
  if (io_s.IsPathNotFound() && *src_temperature != Temperature::kUnknown) {
    // Retry without temperature hint in case the FileSystem is strict with
    // non-kUnknown temperature option
    src_file_options.temperature = Temperature::kUnknown;
    io_s = src_env->GetFileSystem()->NewSequentialFile(src, src_file_options,
                                                       &src_file, nullptr);
  }
  if (!io_s.ok()) {
    return io_s;
  }

  size_t buf_size = CalculateIOBufferSize(rate_limiter);
  TEST_SYNC_POINT_CALLBACK(
      "BackupEngineImpl::CopyOrCreateFile:CalculateIOBufferSize", &buf_size);

  // TODO: pass in Histograms if the destination file is sst or blob
  std::unique_ptr<WritableFileWriter> dest_writer(
      new WritableFileWriter(std::move(dst_file), dst, dst_file_options));
  std::unique_ptr<SequentialFileReader> src_reader;
  std::unique_ptr<char[]> buf;
  if (!src.empty()) {
    // Return back current temperature in FileSystem
    *src_temperature = src_file->GetTemperature();

    src_reader.reset(new SequentialFileReader(
        std::move(src_file), src, nullptr /* io_tracer */, {}, rate_limiter));
    buf.reset(new char[buf_size]);
  }

  Slice data;
  const IOOptions opts;
  do {
    if (ShouldAbort()) {
      return status_to_io_status(
          Status::Incomplete(options_.abort_status_message));
    }
    if (!src.empty()) {
      size_t buffer_to_read =
          (buf_size < size_limit) ? buf_size : static_cast<size_t>(size_limit);
      io_s = src_reader->Read(buffer_to_read, &data, buf.get(),
                              Env::IO_LOW /* rate_limiter_priority */);
      *bytes_toward_next_callback += data.size();
    } else {
      data = contents;
    }
    size_limit -= data.size();
    TEST_SYNC_POINT_CALLBACK(
        "BackupEngineImpl::CopyOrCreateFile:CorruptionDuringBackup",
        (src.length() > 4 && src.rfind(".sst") == src.length() - 4) ? &data
                                                                    : nullptr);

    if (!io_s.ok()) {
      return io_s;
    }

    if (size != nullptr) {
      *size += data.size();
    }
    if (checksum_hex != nullptr) {
      checksum_value = crc32c::Extend(checksum_value, data.data(), data.size());
    }

    io_s = dest_writer->Append(data, opts);

    if (rate_limiter != nullptr) {
      if (!src.empty()) {
        rate_limiter->Request(data.size(), Env::IO_LOW, nullptr /* stats */,
                              RateLimiter::OpType::kWrite);
      } else {
        LoopRateLimitRequestHelper(data.size(), rate_limiter, Env::IO_LOW,
                                   nullptr /* stats */,
                                   RateLimiter::OpType::kWrite);
      }
    }
    while (*bytes_toward_next_callback >=
           options_.callback_trigger_interval_size) {
      *bytes_toward_next_callback -= options_.callback_trigger_interval_size;
      if (progress_callback) {
        std::lock_guard<std::mutex> lock(byte_report_mutex_);
        try {
          progress_callback();
        } catch (const std::exception& exn) {
          io_s = IOStatus::Aborted("Exception in progress_callback: " +
                                   std::string(exn.what()));
          break;
        } catch (...) {
          io_s = IOStatus::Aborted("Unknown exception in progress_callback");
          break;
        }
      }
    }
  } while (io_s.ok() && contents.empty() && data.size() > 0 && size_limit > 0);

  // Convert uint32_t checksum to hex checksum
  if (checksum_hex != nullptr) {
    checksum_hex->assign(ChecksumInt32ToHex(checksum_value));
  }

  if (io_s.ok() && sync) {
    io_s = use_fsync ? dest_writer->Fsync(opts) : dest_writer->Sync(opts);
  }
  if (io_s.ok()) {
    io_s = dest_writer->Close(opts);
  }
  return io_s;
}

IOStatus CopyEngine::LinkFile(const std::string& src, const std::string& dst,
                              Env* dst_env) {
  if (ShouldAbort()) {
    return status_to_io_status(
        Status::Incomplete(options_.abort_status_message));
  }
  return dst_env->GetFileSystem()->LinkFile(src, dst, IOOptions(), nullptr);
}

uint64_t CopyEngine::CalculateIOBufferSize(RateLimiter* rate_limiter) const {
  if (options_.io_buffer_size > 0) {
    return options_.io_buffer_size;
  }
  return rate_limiter != nullptr
             ? static_cast<size_t>(rate_limiter->GetSingleBurstBytes())
             : kDefaultCopyFileBufferSize;
}

IOStatus CopyEngine::ReadFileAndComputeChecksum(
    const std::string& src, const std::shared_ptr<FileSystem>& src_fs,
    const EnvOptions& src_env_options, uint64_t size_limit,
    std::string* checksum_hex, const Temperature src_temperature) const {
  if (checksum_hex == nullptr) {
    return status_to_io_status(Status::Aborted("Checksum pointer is null"));
  }
  if (ShouldAbort()) {
    return status_to_io_status(
        Status::Incomplete(options_.abort_status_message));
  }
  uint32_t checksum_value = 0;
  if (size_limit == 0) {
    size_limit = std::numeric_limits<uint64_t>::max();
  }

  std::unique_ptr<SequentialFileReader> src_reader;
  auto file_options = FileOptions(src_env_options);
  file_options.temperature = src_temperature;
  RateLimiter* rate_limiter = options_.checksum_rate_limiter;
  IOStatus io_s = SequentialFileReader::Create(
      src_fs, src, file_options, &src_reader, nullptr /* dbg */, rate_limiter);
  if (io_s.IsPathNotFound() && src_temperature != Temperature::kUnknown) {
    // Retry without temperature hint in case the FileSystem is strict with
    // non-kUnknown temperature option
    file_options.temperature = Temperature::kUnknown;
    io_s = SequentialFileReader::Create(src_fs, src, file_options, &src_reader,
                                        nullptr /* dbg */, rate_limiter);
  }
  if (!io_s.ok()) {
    return io_s;
  }

  size_t buf_size = CalculateIOBufferSize(rate_limiter);
  std::unique_ptr<char[]> buf(new char[buf_size]);
  Slice data;

  do {
    if (ShouldAbort()) {
      return status_to_io_status(
          Status::Incomplete(options_.abort_status_message));
    }
    size_t buffer_to_read =
        (buf_size < size_limit) ? buf_size : static_cast<size_t>(size_limit);
    io_s = src_reader->Read(buffer_to_read, &data, buf.get(),
                            Env::IO_LOW /* rate_limiter_priority */);
    if (!io_s.ok()) {
      return io_s;
    }

    size_limit -= data.size();
    checksum_value = crc32c::Extend(checksum_value, data.data(), data.size());
  } while (data.size() > 0 && size_limit > 0);

  checksum_hex->assign(ChecksumInt32ToHex(checksum_value));

  return io_s;
}

}  // namespace ROCKSDB_NAMESPACE
