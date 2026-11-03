//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom FileSystem to keep track of the state of a file
// system the last "Sync". The data being written is cached in a "buffer".
// Only when "Sync" is called, the data will be persistent. It can similate
// file data loss (or entire files) not protected by a "Sync". For any of the
// FileSystem related operations, by specify the "IOStatus Error", a specific
// error can be returned when file system is not activated.

#pragma once

#include <algorithm>
#include <atomic>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>

#ifndef OS_WIN
#include <fcntl.h>
#include <unistd.h>
#endif

#include "file/filename.h"
#include "port/lang.h"
#include "rocksdb/file_system.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

// A binary log that records injected errors directly to a file.
// Thread-safe for concurrent writes. The log file can be flushed on exit or
// from a signal handler so records survive clean exits and crash paths.
// Disabled on big-endian platforms because the decoder expects little-endian
// fields in the native Entry layout.
class InjectedErrorLog {
 public:
  static constexpr size_t kMaxOpNameLen = 32;
  static constexpr size_t kMaxFileNameLen = 72;
  static constexpr size_t kMaxStatusMessageLen = 56;
  static constexpr size_t kMaxDetailPayloadLen = 48;
  static constexpr uint8_t kDetailNone = 0;
  static constexpr uint8_t kDetailTwoFiles = 1;
  static constexpr uint8_t kDetailSizeAndHead = 2;
  static constexpr uint8_t kDetailOffsetSizeAndHead = 3;
  static constexpr uint8_t kDetailOffsetAndSize = 4;
  static constexpr uint8_t kDetailSize = 5;
  static constexpr uint8_t kDetailCount = 6;
  static constexpr uint8_t kDetailReqOffsetAndSize = 7;

  struct EntryDetail {
    uint64_t offset = 0;
    uint64_t size = 0;
    uint32_t count = 0;
    uint32_t req_idx = 0;
    char payload[kMaxDetailPayloadLen] = {};
  };

  static_assert(sizeof(EntryDetail) == 72,
                "Injected error log detail size must stay stable");

  struct TaggedEntryDetail {
    uint8_t kind = kDetailNone;
    EntryDetail entry;
  };

  struct Entry {
    uint64_t timestamp_us;
    uint64_t thread_id;
    EntryDetail detail;
    uint8_t detail_kind;
    uint8_t detail_payload_size;
    uint8_t retryable;
    uint8_t data_loss;
    char op_name[kMaxOpNameLen];
    char file_name[kMaxFileNameLen];
    char status_message[kMaxStatusMessageLen];
    // Keep Entry exactly 256 bytes on both 32 bit and 64 bit architectures.
    uint8_t padding[4];
  };

  static_assert(sizeof(Entry) == 256,
                "Injected error log entry size must stay stable");

  explicit InjectedErrorLog(const std::string& path);
  ~InjectedErrorLog();
  InjectedErrorLog(const InjectedErrorLog&) = delete;
  InjectedErrorLog& operator=(const InjectedErrorLog&) = delete;

  void Record(const Slice& op_name, const Slice& file_name,
              const TaggedEntryDetail& detail, const IOStatus& status);

  // Format the first few bytes of a buffer as hex for logging.
  // Returns a string like "ab cd ef 01 02 ..."
  static std::string HexHead(const char* data, size_t size,
                             size_t max_bytes = 8) {
    std::string result;
    size_t n = std::min(size, max_bytes);
    static const char kHexDigits[] = "0123456789abcdef";
    result.reserve(n * 3 + ((size > max_bytes) ? 4 : 0));
    for (size_t i = 0; i < n; i++) {
      if (i > 0) {
        result.push_back(' ');
      }
      uint8_t byte = static_cast<uint8_t>(data[i]);
      result.push_back(kHexDigits[byte >> 4]);
      result.push_back(kHexDigits[byte & 0x0f]);
    }
    if (size > max_bytes) {
      result.append(" ...");
    }
    return result;
  }

  // Flush the already-appended log file so crash/termination paths preserve
  // the most recent records.
  void Flush() const;

 private:
  std::atomic<uint64_t> next_write_offset_;
  const int log_fd_;
};

class TestFSWritableFile;
class FaultInjectionTestFS;

// Fixed-size detail builders for injected error logging.
// These avoid hot-path string formatting while keeping the detail payload owned
// and reusable by the on-disk entry layout.
namespace fault_injection_detail {

using TaggedEntryDetail = InjectedErrorLog::TaggedEntryDetail;

inline void CopyPayloadBytes(const Slice& src, char* dst, size_t dst_len) {
  const size_t copied = std::min(src.size(), dst_len);
  if (copied > 0) {
    std::memcpy(dst, src.data(), copied);
  }
}

inline TaggedEntryDetail NoDetail() { return TaggedEntryDetail(); }

inline TaggedEntryDetail TwoFiles(const std::string& /*f1*/,
                                  const std::string& f2) {
  TaggedEntryDetail detail;
  detail.kind = InjectedErrorLog::kDetailTwoFiles;
  detail.entry.size = static_cast<uint64_t>(f2.size());
  CopyPayloadBytes(Slice(f2), detail.entry.payload,
                   sizeof(detail.entry.payload));
  return detail;
}

inline TaggedEntryDetail SizeAndHead(const Slice& data) {
  TaggedEntryDetail detail;
  detail.kind = InjectedErrorLog::kDetailSizeAndHead;
  detail.entry.size = static_cast<uint64_t>(data.size());
  CopyPayloadBytes(data, detail.entry.payload, sizeof(detail.entry.payload));
  return detail;
}

inline TaggedEntryDetail OffsetSizeAndHead(uint64_t offset, const Slice& data) {
  TaggedEntryDetail detail;
  detail.kind = InjectedErrorLog::kDetailOffsetSizeAndHead;
  detail.entry.offset = offset;
  detail.entry.size = static_cast<uint64_t>(data.size());
  CopyPayloadBytes(data, detail.entry.payload, sizeof(detail.entry.payload));
  return detail;
}

inline TaggedEntryDetail OffsetAndSize(uint64_t offset, size_t n) {
  TaggedEntryDetail detail;
  detail.kind = InjectedErrorLog::kDetailOffsetAndSize;
  detail.entry.offset = offset;
  detail.entry.size = static_cast<uint64_t>(n);
  return detail;
}

inline TaggedEntryDetail Size(uint64_t size) {
  TaggedEntryDetail detail;
  detail.kind = InjectedErrorLog::kDetailSize;
  detail.entry.size = size;
  return detail;
}

inline TaggedEntryDetail Count(size_t count) {
  assert(count <= std::numeric_limits<uint32_t>::max());
  TaggedEntryDetail detail;
  detail.kind = InjectedErrorLog::kDetailCount;
  detail.entry.count = static_cast<uint32_t>(count);
  return detail;
}

inline TaggedEntryDetail ReqOffsetAndSize(size_t req_idx, uint64_t offset,
                                          size_t n) {
  assert(req_idx <= std::numeric_limits<uint32_t>::max());
  TaggedEntryDetail detail;
  detail.kind = InjectedErrorLog::kDetailReqOffsetAndSize;
  detail.entry.req_idx = static_cast<uint32_t>(req_idx);
  detail.entry.offset = offset;
  detail.entry.size = static_cast<uint64_t>(n);
  return detail;
}

}  // namespace fault_injection_detail

enum class FaultInjectionIOType {
  kRead = 0,
  kWrite,
  kMetadataRead,
  kMetadataWrite,
};

struct FSFileState {
  std::string filename_;
  uint64_t pos_at_last_append_ = 0;
  uint64_t pos_at_last_sync_ = 0;
  std::string buffer_;

  explicit FSFileState(const std::string& filename = {})
      : filename_(filename) {}

  bool IsFullySynced() const {
    return pos_at_last_append_ == pos_at_last_sync_;
  }

  IOStatus DropUnsyncedData();

  IOStatus DropRandomUnsyncedData(Random* rand);
};

// A per-file fault-injection wrapper around FSWritableFile. It injects
// write/metadata faults while reporting path-level state transitions back to
// FaultInjectionTestFS.
class TestFSWritableFile : public FSWritableFile {
 public:
  explicit TestFSWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>&& f,
                              FaultInjectionTestFS* fs,
                              bool reopened_for_write = false);
  virtual ~TestFSWritableFile();
  IOStatus Append(const Slice& data, const IOOptions&,
                  IODebugContext*) override;
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override;
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Flush(const IOOptions&, IODebugContext*) override;
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                     const IOOptions& options, IODebugContext* dbg) override;
  bool IsSyncThreadSafe() const override { return true; }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    MutexLock l(&mutex_);
    return target_->GetFileSize(options, dbg);
  }

 private:
  IOStatus CloseImpl(const IOOptions& options, IODebugContext* dbg);
  IOStatus ValidateWrite(const char* op_name);

  FSFileState state_;  // Need protection by mutex_
  FileOptions file_opts_;
  std::unique_ptr<FSWritableFile> target_;
  // Whether this wrapper may still forward Close() to target_. The
  // FSWritableFile contract treats the file as closed after the first Close()
  // attempt regardless of returned status, so later Close() calls are
  // wrapper-level no-ops here.
  bool should_forward_close_;
  FaultInjectionTestFS* fs_;
  port::Mutex mutex_;
  const bool unsync_data_loss_;
  const bool reopened_for_write_;
};

// A per-file fault-injection wrapper around FSRandomRWFile that reports
// path-level close transitions back to FaultInjectionTestFS.
class TestFSRandomRWFile : public FSRandomRWFile {
 public:
  explicit TestFSRandomRWFile(const std::string& fname,
                              std::unique_ptr<FSRandomRWFile>&& f,
                              FaultInjectionTestFS* fs);
  virtual ~TestFSRandomRWFile();
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override;
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

 private:
  // keep a copy of file name, so we can untrack it in File system, when it is
  // closed
  std::string fname_;
  std::unique_ptr<FSRandomRWFile> target_;
  bool file_opened_;
  FaultInjectionTestFS* fs_;
};

class TestFSRandomAccessFile : public FSRandomAccessFile {
 public:
  explicit TestFSRandomAccessFile(const std::string& fname,
                                  std::unique_ptr<FSRandomAccessFile>&& f,
                                  FaultInjectionTestFS* fs);
  ~TestFSRandomAccessFile() override {}
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;
  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;
  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetUniqueId(char* id, size_t max_size) const override;

  IOStatus GetFileSize(uint64_t* file_size) override;

 private:
  std::string fname_;
  std::unique_ptr<FSRandomAccessFile> target_;
  FaultInjectionTestFS* fs_;
  const bool is_sst_;
};

// A per-file fault-injection wrapper around FSSequentialFile that can surface
// injected read faults and simulated unsynced data through
// FaultInjectionTestFS.
class TestFSSequentialFile : public FSSequentialFileOwnerWrapper {
 public:
  explicit TestFSSequentialFile(std::unique_ptr<FSSequentialFile>&& f,
                                FaultInjectionTestFS* fs, std::string fname)
      : FSSequentialFileOwnerWrapper(std::move(f)),
        fs_(fs),
        fname_(std::move(fname)) {}
  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;

 private:
  FaultInjectionTestFS* fs_;
  std::string fname_;
  uint64_t read_pos_ = 0;
  uint64_t target_read_pos_ = 0;
};

// A fault-injection wrapper around FSDirectory that surfaces metadata faults
// through FaultInjectionTestFS.
class TestFSDirectory : public FSDirectory {
 public:
  explicit TestFSDirectory(FaultInjectionTestFS* fs, std::string dirname,
                           FSDirectory* dir)
      : fs_(fs), dirname_(std::move(dirname)), dir_(dir) {}
  ~TestFSDirectory() {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& dir_fsync_options) override;

 private:
  FaultInjectionTestFS* fs_;
  std::string dirname_;
  std::unique_ptr<FSDirectory> dir_;
};

class FaultInjectionTestFS : public FileSystemWrapper {
 public:
  explicit FaultInjectionTestFS(
      const std::shared_ptr<FileSystem>& base,
      const std::string& injected_error_log_path = std::string())
      : FileSystemWrapper(base),
        filesystem_active_(true),
        filesystem_writable_(false),
        inject_unsynced_data_loss_(false),
        read_unsynced_data_(true),
        allow_link_open_file_(false),
        injected_thread_local_read_error_(DeleteThreadLocalErrorContext),
        injected_thread_local_write_error_(DeleteThreadLocalErrorContext),
        injected_thread_local_metadata_read_error_(
            DeleteThreadLocalErrorContext),
        injected_thread_local_metadata_write_error_(
            DeleteThreadLocalErrorContext),
        ingest_data_corruption_before_write_(false),
        checksum_handoff_func_type_(kCRC32c),
        injected_error_log_(injected_error_log_path) {}
  virtual ~FaultInjectionTestFS() override { fs_error_.PermitUncheckedError(); }

  static const char* kClassName() { return "FaultInjectionTestFS"; }
  const char* Name() const override { return kClassName(); }

  static bool IsInjectedError(const Status& s,
                              const std::string& specific_error_marker = "") {
    if (s.ok()) {
      return false;
    }
    const char* state = s.getState();
    if (state == nullptr) {
      return false;
    }
    bool is_injected_error = std::strstr(state, kInjected.c_str()) != nullptr;
    bool is_specific_error =
        specific_error_marker.empty() ||
        std::strstr(state, specific_error_marker.c_str()) != nullptr;

    return is_injected_error && is_specific_error;
  }

  static bool IsFailedToWriteToWALError(const Status& s) {
    return IsInjectedError(s, kFailedToWriteToWAL);
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& f, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& options, IODebugContext* dbg) override;

  IOStatus SyncFile(const std::string& fname, const FileOptions& file_opts,
                    const IOOptions& io_opts, bool use_fsync,
                    IODebugContext* dbg) override;

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& options,
                        uint64_t* count, IODebugContext* dbg) override;

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& options, bool* res,
                        IODebugContext* dbg) override;
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override;

// Undef to eliminate clash on Windows
#undef GetFreeSpace
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& options,
                        uint64_t* disk_free, IODebugContext* dbg) override {
    IOStatus io_s;
    bool inactive_no_space = false;
    {
      MutexLock l(&mutex_);
      inactive_no_space = !filesystem_active_ &&
                          fs_error_.subcode() == IOStatus::SubCode::kNoSpace;
    }
    if (inactive_no_space) {
      *disk_free = 0;
    } else {
      io_s = MaybeInjectThreadLocalError(FaultInjectionIOType::kMetadataRead,
                                         options, "GetFreeSpace", path);
      if (io_s.ok()) {
        io_s = target()->GetFreeSpace(path, options, disk_free, dbg);
      }
    }
    return io_s;
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dgb) override;

  IOStatus Poll(std::vector<void*>& io_handles,
                size_t min_completions) override;

  IOStatus AbortIO(std::vector<void*>& io_handles) override;

  void WritableFileClosed(const FSFileState& state);

  void WritableFileSynced(const FSFileState& state);

  void WritableFileAppended(const FSFileState& state);

  void RandomRWFileClosed(const std::string& fname);

  void WritableFileOpened(const std::string& fname,
                          FileOpenContract open_contract);

  IOStatus ValidateReadOpen(const std::string& fname, const char* op_name);

  IOStatus ValidateWriteOpen(const std::string& fname, const char* op_name);

  IOStatus ValidateReopenedWrite(const std::string& fname, const char* op_name);

  IOStatus DropUnsyncedFileData();

  IOStatus DropRandomUnsyncedFileData(Random* rnd);

  IOStatus DeleteFilesCreatedAfterLastDirSync(const IOOptions& options,
                                              IODebugContext* dbg);

  void ResetState();

  void UntrackFile(const std::string& f);

  void SyncDir(const std::string& dirname) {
    MutexLock l(&mutex_);
    dir_to_new_files_since_last_sync_.erase(dirname);
  }

  // Setting the filesystem to inactive is the test equivalent to simulating a
  // system reset. Setting to inactive will freeze our saved filesystem state so
  // that it will stop being recorded. It can then be reset back to the state at
  // the time of the reset.
  bool IsFilesystemActive() {
    MutexLock l(&mutex_);
    return filesystem_active_;
  }

  // Setting filesystem_writable_ makes NewWritableFile. ReopenWritableFile,
  // and NewRandomRWFile bypass FaultInjectionTestFS and go directly to the
  // target FS
  bool IsFilesystemDirectWritable() {
    MutexLock l(&mutex_);
    return filesystem_writable_;
  }
  void SetFilesystemActiveNoLock(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    error.PermitUncheckedError();
    filesystem_active_ = active;
    if (!active) {
      fs_error_ = error;
    }
  }
  void SetFilesystemActive(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    MutexLock l(&mutex_);
    error.PermitUncheckedError();
    SetFilesystemActiveNoLock(active, error);
  }
  void SetFilesystemDirectWritable(bool writable) {
    MutexLock l(&mutex_);
    filesystem_writable_ = writable;
  }

  // If true, we buffer write data in memory to simulate data loss upon system
  // crash by only having process crashes
  void SetInjectUnsyncedDataLoss(bool inject) {
    MutexLock l(&mutex_);
    inject_unsynced_data_loss_ = inject;
  }

  bool InjectUnsyncedDataLoss() {
    MutexLock l(&mutex_);
    return inject_unsynced_data_loss_;
  }

  // In places (e.g. GetSortedWals()) RocksDB relies on querying the file size
  // or even reading the contents of files currently open for writing, and
  // as in POSIX semantics, expects to see the flushed size and contents
  // regardless of what has been synced. FaultInjectionTestFS historically
  // did not emulate this behavior, only showing synced data from such read
  // operations. (Different from FaultInjectionTestEnv--sigh.) Calling this
  // function with false restores this historical behavior for testing
  // stability, but use of this semantics must be phased out as it is
  // inconsistent with expected FileSystem semantics. In other words, this
  // functionality is DEPRECATED. Intended to be set after construction and
  // unchanged (not thread safe).
  void SetReadUnsyncedData(bool read_unsynced_data) {
    read_unsynced_data_ = read_unsynced_data;
  }
  bool ReadUnsyncedData() const { return read_unsynced_data_; }

  // FaultInjectionTestFS normally includes a hygiene check for FileSystem
  // implementations that only support LinkFile() on closed files (not open
  // for write). Setting this to true bypasses the check.
  void SetAllowLinkOpenFile(bool allow_link_open_file = true) {
    allow_link_open_file_ = allow_link_open_file;
  }

  bool ShouldIOActivitiesExcludedFromFaultInjection(
      Env::IOActivity io_activity) {
    MutexLock l(&mutex_);
    return io_activities_excluded_from_fault_injection.find(io_activity) !=
           io_activities_excluded_from_fault_injection.end();
  }

  void AssertNoOpenFile() { assert(open_managed_files_.empty()); }

  IOStatus GetError() {
    MutexLock l(&mutex_);
    return fs_error_;
  }

  void SetFileSystemIOError(IOStatus io_error) {
    MutexLock l(&mutex_);
    io_error.PermitUncheckedError();
    fs_error_ = io_error;
  }

  // To simulate the data corruption before data is written in FS
  void IngestDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    ingest_data_corruption_before_write_ = true;
  }

  void NoDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    ingest_data_corruption_before_write_ = false;
  }

  bool ShouldDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    return ingest_data_corruption_before_write_;
  }

  void SetChecksumHandoffFuncType(const ChecksumType& func_type) {
    MutexLock l(&mutex_);
    checksum_handoff_func_type_ = func_type;
  }

  const ChecksumType& GetChecksumHandoffFuncType() {
    MutexLock l(&mutex_);
    return checksum_handoff_func_type_;
  }

  void SetFailGetUniqueId(bool flag) {
    MutexLock l(&mutex_);
    fail_get_file_unique_id_ = flag;
  }

  bool ShouldFailGetUniqueId() {
    MutexLock l(&mutex_);
    return fail_get_file_unique_id_;
  }

  void SetFailRandomAccessGetFileSizeSst(bool flag) {
    MutexLock l(&mutex_);
    fail_random_access_get_file_size_sst_ = flag;
  }

  bool ShouldFailRandomAccessGetFileSizeSst() {
    MutexLock l(&mutex_);
    return fail_random_access_get_file_size_sst_;
  }

  void SetFailFilesystemGetFileSizeSst(bool flag) {
    MutexLock l(&mutex_);
    fail_fs_get_file_size_sst_ = flag;
  }

  bool ShouldFailFilesystemGetFileSizeSst() {
    MutexLock l(&mutex_);
    return fail_fs_get_file_size_sst_;
  }

  // Specify what the operation, so we can inject the right type of error
  enum ErrorOperation : char {
    kRead = 0,
    kMultiReadSingleReq = 1,
    kMultiRead = 2,
    kOpen,
    kAppend,
    kPositionedAppend,
    kUnknown,
  };

  void SetThreadLocalErrorContext(FaultInjectionIOType type, uint32_t seed,
                                  int one_in, bool retryable,
                                  bool has_data_loss) {
    struct ErrorContext* new_ctx = new ErrorContext(seed);
    new_ctx->one_in = one_in;
    new_ctx->count = 0;
    new_ctx->retryable = retryable;
    new_ctx->has_data_loss = has_data_loss;

    SetErrorContextOfFaultInjectionIOType(type, new_ctx);
  }

  static void DeleteThreadLocalErrorContext(void* p) {
    ErrorContext* ctx = static_cast<ErrorContext*>(p);
    delete ctx;
  }

  IOStatus MaybeInjectThreadLocalError(
      FaultInjectionIOType type, const IOOptions& io_options,
      const char* op_name, const std::string& file_name,
      const InjectedErrorLog::TaggedEntryDetail& detail =
          InjectedErrorLog::TaggedEntryDetail(),
      ErrorOperation op = kUnknown, Slice* slice = nullptr,
      bool direct_io = false, char* scratch = nullptr,
      bool need_count_increase = false, bool* fault_injected = nullptr);

  int GetAndResetInjectedThreadLocalErrorCount(FaultInjectionIOType type) {
    ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
    int count = 0;
    if (ctx) {
      count = ctx->count;
      ctx->count = 0;
    }
    return count;
  }

  void SetIOActivitiesExcludedFromFaultInjection(
      const std::set<Env::IOActivity>& io_activities) {
    MutexLock l(&mutex_);
    io_activities_excluded_from_fault_injection = io_activities;
  }

  void SetFileTypesExcludedFromWriteFaultInjection(
      const std::set<FileType>& types) {
    MutexLock l(&mutex_);
    file_types_excluded_from_write_fault_injection_ = types;
  }

  void SetFileTypesExcludedFromFaultInjection(const std::set<FileType>& types) {
    MutexLock l(&mutex_);
    file_types_excluded_from_fault_injection_ = types;
  }

  void EnableThreadLocalErrorInjection(FaultInjectionIOType type) {
    ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
    if (ctx) {
      ctx->enable_error_injection = true;
    }
  }

  void EnableAllThreadLocalErrorInjection() {
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kRead);
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataRead);
    EnableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataWrite);
  }

  void DisableThreadLocalErrorInjection(FaultInjectionIOType type) {
    ErrorContext* ctx = GetErrorContextFromFaultInjectionIOType(type);
    if (ctx) {
      ctx->enable_error_injection = false;
    }
  }

  void DisableAllThreadLocalErrorInjection() {
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kRead);
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataRead);
    DisableThreadLocalErrorInjection(FaultInjectionIOType::kMetadataWrite);
  }

  void PrintInjectedThreadLocalErrorBacktrace(FaultInjectionIOType type);

  // If there is unsynced data in the specified file within the specified
  // range [offset, offset + n), return the unsynced data overlapping with
  // that range, in a corresponding range of scratch. When known, also return
  // the position of the last sync, so that the caller can determine whether
  // more data is available from the target file when not available from
  // unsynced.
  void ReadUnsynced(const std::string& fname, uint64_t offset, size_t n,
                    Slice* result, char* scratch, int64_t* pos_at_last_sync);

  // Flush recently injected errors to the configured binary log file and sync
  // it to storage.
  void FlushRecentInjectedErrors() const { injected_error_log_.Flush(); }

  inline static const std::string kInjected = "injected";

 private:
  inline static const std::string kFailedToWriteToWAL =
      "failed to write to WAL";

  IOStatus ValidateNoReopenForWrite(const std::string& fname,
                                    const char* op_name,
                                    bool allow_missing_file);

  inline static const std::string kInjectedReadError = "injected read error";
  inline static const std::string kInjectedEmptyResult =
      "injected empty result";
  inline static const std::string kInjectedCorruptLastByte =
      "injected corrupt last byte";
  inline static const std::string kInjectedErrorResultMultiGetSingle =
      "injected error result multiget single";
  inline static const std::string kInjectedWriteError = "injected write error";
  inline static const std::string kInjectedWriteErrorFailedToWriteToWAL =
      "injected write error failed to write to WAL";
  inline static const std::string kInjectedMetadataReadError =
      "injected metadata read error";
  inline static const std::string kInjectedMetadataWriteError =
      "injected metadata write error";
  port::Mutex mutex_;
  std::map<std::string, FSFileState> db_file_state_;
  std::map<std::string, FileOpenContract> open_managed_files_;
  std::map<std::string, FileOpenContract> file_open_contracts_;
  // directory -> (file name -> file contents to recover)
  // When data is recovered from unsyned parent directory, the files with
  // empty file contents to recover is deleted. Those with non-empty ones
  // will be recovered to content accordingly.
  std::unordered_map<std::string, std::map<std::string, std::string>>
      dir_to_new_files_since_last_sync_;
  bool filesystem_active_;    // Record flushes, syncs, writes
  bool filesystem_writable_;  // Bypass FaultInjectionTestFS and go directly
                              // to underlying FS for writable files
  bool inject_unsynced_data_loss_;  // See InjectUnsyncedDataLoss()
  bool read_unsynced_data_;         // See SetReadUnsyncedData()
  bool allow_link_open_file_;       // See SetAllowLinkOpenFile()
  IOStatus fs_error_;

  enum ErrorType : int {
    kErrorTypeStatus = 0,
    kErrorTypeCorruption,
    kErrorTypeTruncated,
    kErrorTypeMax
  };

  struct ErrorContext {
    Random rand;
    int one_in;
    int count;
    bool enable_error_injection;
    void* callstack;
    std::string message;
    int frames;
    ErrorType type;
    bool retryable;
    bool has_data_loss;

    explicit ErrorContext(uint32_t seed)
        : rand(seed),
          enable_error_injection(false),
          callstack(nullptr),
          frames(0),
          retryable(false),
          has_data_loss(false) {}
    ~ErrorContext() {
      if (callstack) {
        free(callstack);
      }
    }
  };

  std::set<FileType> file_types_excluded_from_fault_injection_;
  std::set<FileType> file_types_excluded_from_write_fault_injection_;
  std::set<Env::IOActivity> io_activities_excluded_from_fault_injection;
  ThreadLocalPtr injected_thread_local_read_error_;
  ThreadLocalPtr injected_thread_local_write_error_;
  ThreadLocalPtr injected_thread_local_metadata_read_error_;
  ThreadLocalPtr injected_thread_local_metadata_write_error_;
  bool ingest_data_corruption_before_write_;
  ChecksumType checksum_handoff_func_type_;
  bool fail_get_file_unique_id_ = false;
  bool fail_random_access_get_file_size_sst_ = false;
  bool fail_fs_get_file_size_sst_ = false;
  InjectedErrorLog injected_error_log_;

  // Inject an error. For a READ operation, a status of IOError(), a
  // corruption in the contents of scratch, or truncation of slice
  // are the types of error with equal probability. For OPEN,
  // its always an IOError.
  // fault_injected returns whether a fault is injected. It is needed
  // because some fault is inected with IOStatus to be OK.
  IOStatus MaybeInjectThreadLocalReadError(
      const IOOptions& io_options, const char* op_name,
      const std::string& file_name,
      const InjectedErrorLog::TaggedEntryDetail& detail, ErrorOperation op,
      Slice* slice, bool direct_io, char* scratch, bool need_count_increase,
      bool* fault_injected);

  bool ShouldExcludeFromFaultInjection(const std::string& file_name,
                                       FaultInjectionIOType type) {
    MutexLock l(&mutex_);
    FileType file_type = kTempFile;
    uint64_t file_number = 0;
    if (!TryParseFileName(file_name, &file_number, &file_type)) {
      return false;
    }
    if (file_types_excluded_from_fault_injection_.count(file_type) > 0) {
      return true;
    }
    switch (type) {
      case FaultInjectionIOType::kWrite:
        return file_types_excluded_from_write_fault_injection_.count(
                   file_type) > 0;
      case FaultInjectionIOType::kRead:
      case FaultInjectionIOType::kMetadataRead:
      case FaultInjectionIOType::kMetadataWrite:
        return false;
    }
    return false;
  }

  // Extract number of type from file name. Return false if failing to fine
  // them.
  bool TryParseFileName(const std::string& file_name, uint64_t* number,
                        FileType* type);

  ErrorContext* GetErrorContextFromFaultInjectionIOType(
      FaultInjectionIOType type) {
    ErrorContext* ctx = nullptr;
    switch (type) {
      case FaultInjectionIOType::kRead:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_read_error_.Get());
        break;
      case FaultInjectionIOType::kWrite:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_write_error_.Get());
        break;
      case FaultInjectionIOType::kMetadataRead:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_read_error_.Get());
        break;
      case FaultInjectionIOType::kMetadataWrite:
        ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_write_error_.Get());
        break;
      default:
        assert(false);
        break;
    }
    return ctx;
  }

  void SetErrorContextOfFaultInjectionIOType(FaultInjectionIOType type,
                                             ErrorContext* new_ctx) {
    ErrorContext* old_ctx = nullptr;
    switch (type) {
      case FaultInjectionIOType::kRead:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_read_error_.Swap(new_ctx));
        break;
      case FaultInjectionIOType::kWrite:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_write_error_.Swap(new_ctx));
        break;
      case FaultInjectionIOType::kMetadataRead:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_read_error_.Swap(new_ctx));
        break;
      case FaultInjectionIOType::kMetadataWrite:
        old_ctx = static_cast<struct ErrorContext*>(
            injected_thread_local_metadata_write_error_.Swap(new_ctx));
        break;
      default:
        assert(false);
        break;
    }

    if (old_ctx) {
      DeleteThreadLocalErrorContext(old_ctx);
    }
  }

  std::string GetErrorMessage(FaultInjectionIOType type,
                              const std::string& file_name, ErrorOperation op) {
    switch (type) {
      case FaultInjectionIOType::kRead:
        return kInjectedReadError;
      case FaultInjectionIOType::kWrite: {
        if (op == ErrorOperation::kOpen || op == ErrorOperation::kAppend ||
            op == ErrorOperation::kPositionedAppend) {
          FileType file_type = kTempFile;
          uint64_t ignore = 0;
          if (TryParseFileName(file_name, &ignore, &file_type) &&
              file_type == FileType::kWalFile) {
            return kInjectedWriteErrorFailedToWriteToWAL;
          }
        }
        return kInjectedWriteError;
      }
      case FaultInjectionIOType::kMetadataRead:
        return kInjectedMetadataReadError;
      case FaultInjectionIOType::kMetadataWrite:
        return kInjectedMetadataWriteError;
    }
    assert(false);
    return kInjectedReadError;
  }
};

}  // namespace ROCKSDB_NAMESPACE
