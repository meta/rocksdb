//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/io_status.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/statistics.h"
#include "util/channel.h"

namespace ROCKSDB_NAMESPACE {

class FileSystem;

struct WorkItemResult {
  WorkItemResult() = default;

  WorkItemResult(const WorkItemResult& other) = delete;
  WorkItemResult& operator=(const WorkItemResult& other) = delete;

  WorkItemResult(WorkItemResult&& o) noexcept { *this = std::move(o); }

  WorkItemResult& operator=(WorkItemResult&& o) noexcept {
    size = o.size;
    checksum_hex = std::move(o.checksum_hex);
    db_id = std::move(o.db_id);
    db_session_id = std::move(o.db_session_id);
    io_status = std::move(o.io_status);
    expected_src_temperature = o.expected_src_temperature;
    current_src_temperature = o.current_src_temperature;
    return *this;
  }

  ~WorkItemResult() {
    // The Status needs to be ignored here for two reasons.
    // First, if the CopyEngine shuts down with jobs outstanding, then
    // it is possible that the Status in the future/promise is never read,
    // resulting in an unchecked Status. Second, if there are items in the
    // channel when the CopyEngine is shutdown, these will also have
    // Status that have not been checked.  This
    // TODO: Fix those issues so that the Status
    io_status.PermitUncheckedError();
  }
  uint64_t size = 0;
  std::string checksum_hex;
  std::string db_id;
  std::string db_session_id;
  IOStatus io_status;
  Temperature expected_src_temperature = Temperature::kUnknown;
  Temperature current_src_temperature = Temperature::kUnknown;
};

enum WorkItemType : uint64_t {
  CopyOrCreate = 1U,
  ComputeChecksum = 2U,
  Link = 3U,
};

// For CopyOrCreate, exactly one of src_path and contents must be non-empty.
// dst_path is the initial destination for the copy or the supplied contents.
struct WorkItem {
  std::string src_path;
  std::string dst_path;
  // CopyOrCreate only. If non-empty, rename dst_path to final_dst_path after
  // successful copy or creation, close, and any checksum verification. The
  // rename completes before result is fulfilled, and any rename error is
  // reported in result. An OK result means the file is at final_dst_path.
  std::string final_dst_path;
  Temperature src_temperature = Temperature::kUnknown;
  Temperature dst_temperature = Temperature::kUnknown;
  std::string contents;
  Env* src_env = nullptr;
  Env* dst_env = nullptr;
  EnvOptions src_env_options;
  bool sync = false;
  bool use_fsync = false;
  RateLimiter* rate_limiter = nullptr;
  uint64_t size_limit = 0;
  Statistics* stats = nullptr;
  std::promise<WorkItemResult> result;
  std::function<void()> progress_callback;
  std::string src_checksum_func_name = kUnknownFileChecksumFuncName;
  std::string src_checksum_hex;
  std::string db_id;
  std::string db_session_id;
  WorkItemType type = WorkItemType::CopyOrCreate;

  WorkItem() = default;

  WorkItem(const WorkItem&) = delete;
  WorkItem& operator=(const WorkItem&) = delete;

  WorkItem(WorkItem&& o) noexcept { *this = std::move(o); }

  WorkItem& operator=(WorkItem&& o) noexcept {
    src_path = std::move(o.src_path);
    dst_path = std::move(o.dst_path);
    final_dst_path = std::move(o.final_dst_path);
    src_temperature = o.src_temperature;
    dst_temperature = o.dst_temperature;
    contents = std::move(o.contents);
    src_env = o.src_env;
    dst_env = o.dst_env;
    src_env_options = std::move(o.src_env_options);
    sync = o.sync;
    use_fsync = o.use_fsync;
    rate_limiter = o.rate_limiter;
    size_limit = o.size_limit;
    stats = o.stats;
    result = std::move(o.result);
    progress_callback = std::move(o.progress_callback);
    src_checksum_func_name = std::move(o.src_checksum_func_name);
    src_checksum_hex = std::move(o.src_checksum_hex);
    db_id = std::move(o.db_id);
    db_session_id = std::move(o.db_session_id);
    type = o.type;
    return *this;
  }

  WorkItem(
      std::string _src_path, std::string _dst_path,
      const Temperature _src_temperature, const Temperature _dst_temperature,
      std::string _contents, Env* _src_env, Env* _dst_env,
      EnvOptions _src_env_options, bool _sync, bool _use_fsync,
      RateLimiter* _rate_limiter, uint64_t _size_limit, Statistics* _stats,
      std::function<void()> _progress_callback = {},
      const std::string& _src_checksum_func_name = kUnknownFileChecksumFuncName,
      const std::string& _src_checksum_hex = "", const std::string& _db_id = "",
      const std::string& _db_session_id = "",
      WorkItemType _type = WorkItemType::CopyOrCreate)
      : src_path(std::move(_src_path)),
        dst_path(std::move(_dst_path)),
        src_temperature(_src_temperature),
        dst_temperature(_dst_temperature),
        contents(std::move(_contents)),
        src_env(_src_env),
        dst_env(_dst_env),
        src_env_options(std::move(_src_env_options)),
        sync(_sync),
        use_fsync(_use_fsync),
        rate_limiter(_rate_limiter),
        size_limit(_size_limit),
        stats(_stats),
        progress_callback(std::move(_progress_callback)),
        src_checksum_func_name(_src_checksum_func_name),
        src_checksum_hex(_src_checksum_hex),
        db_id(_db_id),
        db_session_id(_db_session_id),
        type(_type) {}

  ~WorkItem() = default;
};

struct CopyEngineOptions {
  int max_background_operations = 1;
  uint64_t io_buffer_size = 0;
  uint64_t callback_trigger_interval_size = 4 * 1024 * 1024;
  // Rate limiter for standalone ComputeChecksum reads; copies use the
  // per-WorkItem rate_limiter instead. Not owned; must outlive the CopyEngine.
  RateLimiter* checksum_rate_limiter = nullptr;
  // Tickers recorded for bytes read/written by copies. Defaults are the backup
  // tickers; other callers (e.g. checkpoint) should set their own.
  Tickers read_bytes_ticker = BACKUP_READ_BYTES;
  Tickers write_bytes_ticker = BACKUP_WRITE_BYTES;
  Logger* info_log = nullptr;
  // Pool thread name; must be <= 15 chars on Linux or it is left unnamed.
  std::string thread_name = "copy_engine";
  // If set, polled to abort work early (e.g. BackupEngine::StopBackup).
  std::function<bool()> should_abort = {};
  std::string abort_status_message = "Operation stopped";
};

// A pool of background threads that copy, hard-link, or checksum files pulled
// from a shared work queue, so BackupEngine and CheckpointEngine share one
// implementation.
class CopyEngine {
 public:
  explicit CopyEngine(CopyEngineOptions options);
  ~CopyEngine();

  CopyEngine(const CopyEngine&) = delete;
  CopyEngine& operator=(const CopyEngine&) = delete;
  CopyEngine(CopyEngine&&) = delete;
  CopyEngine& operator=(CopyEngine&&) = delete;

  // Enqueues item (moved in); extract item.result's future before calling.
  void Submit(WorkItem&& item);

  void MaybeDecreaseCpuPriority(CpuPriority priority);

  // Synchronous primitives, usable inline or from the pool.
  IOStatus CopyOrCreateFile(const std::string& src, const std::string& dst,
                            const std::string& contents, uint64_t size_limit,
                            Env* src_env, Env* dst_env,
                            const EnvOptions& src_env_options, bool sync,
                            bool use_fsync, RateLimiter* rate_limiter,
                            const std::function<void()>& progress_callback,
                            Temperature* src_temperature,
                            Temperature dst_temperature,
                            uint64_t* bytes_toward_next_callback,
                            uint64_t* size, std::string* checksum_hex);

  IOStatus ReadFileAndComputeChecksum(const std::string& src,
                                      const std::shared_ptr<FileSystem>& src_fs,
                                      const EnvOptions& src_env_options,
                                      uint64_t size_limit,
                                      std::string* checksum_hex,
                                      Temperature src_temperature) const;

  // Returns the FileSystem's NotSupported when linking is unavailable, so
  // callers can fall back to copying.
  IOStatus LinkFile(const std::string& src, const std::string& dst,
                    Env* dst_env);

  uint64_t CalculateIOBufferSize(RateLimiter* rate_limiter) const;

 private:
  static constexpr size_t kDefaultCopyFileBufferSize =
      5 * 1024 * 1024LL;  // 5MB

  void ThreadBody();
  bool ShouldAbort() const {
    return options_.should_abort && options_.should_abort();
  }

  CopyEngineOptions options_;
  std::mutex byte_report_mutex_;
  mutable channel<WorkItem> work_items_;
  std::vector<port::Thread> threads_;
  std::atomic<CpuPriority> threads_cpu_priority_;
};

}  // namespace ROCKSDB_NAMESPACE
